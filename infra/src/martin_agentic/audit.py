"""Local append-only, hash-linked audit and deterministic session exports.

Integrity checks expose accidental edits. They do not authenticate broker
receipts or defend against someone rewriting the entire database and hashes.
"""

from collections import Counter
from datetime import datetime, timezone, date
import hashlib
import json
from pathlib import Path
import re
import sqlite3
from zoneinfo import ZoneInfo

from .ledger import timestamp


KINDS = frozenset({'AGENT_ACTION_INTENT','AGENT_ACTION_RESULT','AGENT_ACTION_UNKNOWN',
    'BROKER_EXECUTION_OBSERVED','BROKER_ORDER_OBSERVED','ACCOUNT_OBSERVED',
    'BROKER_SOURCE_STATE','RECONCILIATION_ISSUE','SHADOW_TRANSITION','PROTECTION_REHEARSAL','ENGINEERING_TEST_PLAN',
    'ENGINEERING_EXIT_REQUESTED'})
TOP_KEYS = frozenset({'event_id','occurred_at','event_type','actor','synthetic','account',
    'trade_id','intent_id','ref_id','broker_order_id','symbol','side','quantity','price',
    'stop','take_profit','reason','details'})
FORBIDDEN = frozenset({'account_number','rhs_account_number','authorization','access_token',
                      'refresh_token','api_key','password','cookie','cookies'})


def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def validate(event):
    if not isinstance(event,dict) or set(event)-TOP_KEYS:
        raise ValueError('Invalid audit fields')
    if not isinstance(event.get('event_id'),str) or not event['event_id'] or len(event['event_id'])>256:
        raise ValueError('Require bounded audit identity')
    timestamp(event['occurred_at'])
    if event.get('event_type') not in KINDS or event.get('actor') not in {'agent','script','broker','simulation'} or type(event.get('synthetic')) is not bool:
        raise ValueError('Require explicit audit kind, actor and synthetic provenance')
    if event.get('actor')=='simulation' and not event['synthetic']:
        raise ValueError('Simulation must remain explicitly synthetic')
    if 'account' in event and not re.fullmatch(r'••••\d{4}',event['account']):
        raise ValueError('Audit account must be masked')
    if event['event_type'].startswith('AGENT_ACTION_'):
        if event['actor']!='agent' or not isinstance(event.get('intent_id'),str) or not event['intent_id']:
            raise ValueError('Agent actions require a linked intent identity')
        if event['event_type']!='AGENT_ACTION_UNKNOWN' and not isinstance(event.get('details'),dict):
            raise ValueError('Agent action parameters/outcome are required')
        if event['event_type']=='AGENT_ACTION_UNKNOWN' and not isinstance(event.get('reason'),str):
            raise ValueError('Unknown result requires an explicit reason')
        if event['event_type']=='AGENT_ACTION_INTENT' and event['details'].get('action') not in {
                'entry','stop_loss','take_profit','raise_stop','cancel','exit','review'}:
            raise ValueError('Require explicit trading action')
        if event['event_type']=='AGENT_ACTION_INTENT' and not isinstance(event['details'].get('parameters'),dict):
            raise ValueError('Require masked action parameters before dispatch')
        if event['event_type']=='AGENT_ACTION_RESULT' and event['details'].get('outcome') not in {
                'submitted','pending_approval','pending_cancel','rejected','cancelled','observed_fill','failed'}:
            raise ValueError('Require explicit non-inferred tool outcome')
    def check(value,depth=0):
        if depth>12:
            raise ValueError('Audit nesting limit exceeded')
        if isinstance(value,dict):
            for key,v in value.items():
                if not isinstance(key,str) or key.lower() in FORBIDDEN:
                    raise ValueError('Credential/account fields are forbidden in audit records')
                check(v,depth+1)
        elif isinstance(value,list):
            for v in value:check(v,depth+1)
        elif value is not None and not isinstance(value,(str,int,bool)):
            raise ValueError('Audit numbers must use decimal strings or integers')
    check(event)
    if len(canonical(event).encode())>131072:
        raise ValueError('Audit event size limit exceeded')


class AuditTrail:
    def __init__(self,path=None,*,connection=None,clock=None):
        self.owned = connection is None
        self.db = connection if connection is not None else sqlite3.connect(Path(path),timeout=10)
        if self.db.in_transaction:
            raise ValueError('Initialize audit before starting a shared transaction')
        self.clock = clock or (lambda:datetime.now(timezone.utc))
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('''CREATE TABLE IF NOT EXISTS audit_events(
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,event_id TEXT UNIQUE NOT NULL,
            recorded_at TEXT NOT NULL,payload TEXT NOT NULL,fingerprint TEXT NOT NULL,
            previous_hash TEXT NOT NULL,entry_hash TEXT NOT NULL);
            CREATE TRIGGER IF NOT EXISTS audit_no_update BEFORE UPDATE ON audit_events
            BEGIN SELECT RAISE(ABORT,'audit is append-only'); END;
            CREATE TRIGGER IF NOT EXISTS audit_no_delete BEFORE DELETE ON audit_events
            BEGIN SELECT RAISE(ABORT,'audit is append-only'); END;''')

    def close(self):
        if self.owned:self.db.close()

    def append(self,event):
        self.db.execute('BEGIN IMMEDIATE')
        try:
            self.verify()
            result=self.append_in_transaction(event)
            self.db.commit()
            return result
        except BaseException:
            self.db.rollback()
            raise

    def append_in_transaction(self,event):
        if not self.db.in_transaction:
            raise ValueError('Shared audit write requires an existing transaction')
        validate(event)
        fingerprint=digest(event)
        prior=self.db.execute('SELECT fingerprint,entry_hash,sequence FROM audit_events WHERE event_id=?',(event['event_id'],)).fetchone()
        if prior:
            if prior[0]!=fingerprint:raise ValueError('Conflicting audit event identity')
            return {'sequence':prior[2],'entry_hash':prior[1],'duplicate':True}
        if event['event_type'].startswith('AGENT_ACTION_'):
            intents=[json.loads(r[0]) for r in self.db.execute('SELECT payload FROM audit_events')]
            matching=[e for e in intents if e['event_type']=='AGENT_ACTION_INTENT' and e.get('intent_id')==event['intent_id']]
            if event['event_type']=='AGENT_ACTION_INTENT' and matching:
                raise ValueError('Agent intent identity already exists')
            if event['event_type']!='AGENT_ACTION_INTENT' and not matching:
                raise ValueError('Agent response has no durable pre-dispatch intent')
        last=self.db.execute('SELECT entry_hash FROM audit_events ORDER BY sequence DESC LIMIT 1').fetchone()
        previous=last[0] if last else '0'*64
        recorded=self.clock().astimezone(timezone.utc).isoformat()
        entry_hash=digest({'previous_hash':previous,'recorded_at':recorded,'event':event})
        cursor=self.db.execute('INSERT INTO audit_events(event_id,recorded_at,payload,fingerprint,previous_hash,entry_hash) VALUES(?,?,?,?,?,?)',
            (event['event_id'],recorded,canonical(event),fingerprint,previous,entry_hash))
        return {'sequence':cursor.lastrowid,'entry_hash':entry_hash,'duplicate':False}

    def verify(self):
        previous='0'*64
        count=0
        for seq,recorded,payload,fp,prev,entry in self.db.execute('SELECT sequence,recorded_at,payload,fingerprint,previous_hash,entry_hash FROM audit_events ORDER BY sequence'):
            event=json.loads(payload)
            validate(event)
            if prev!=previous or digest(event)!=fp or digest({'previous_hash':prev,'recorded_at':recorded,'event':event})!=entry:
                raise ValueError('AUDIT_INTEGRITY_FAILED at sequence '+str(seq))
            previous=entry
            count+=1
        return {'integrity_valid':True,'events_verified':count,'head_hash':previous}

    def report(self,session_date,*,tz='America/New_York'):
        # Pin one read snapshot while the background observer keeps appending.
        own_transaction=not self.db.in_transaction
        if own_transaction:self.db.execute('BEGIN')
        try:return self._report_snapshot(session_date,tz=tz)
        finally:
            if own_transaction:self.db.rollback()

    def _report_snapshot(self,session_date,*,tz):
        day=date.fromisoformat(session_date)
        zone=ZoneInfo(tz)
        integrity=self.verify()
        events=[]
        for seq,recorded,payload,entry in self.db.execute('SELECT sequence,recorded_at,payload,entry_hash FROM audit_events ORDER BY sequence'):
            if timestamp(recorded).astimezone(zone).date()!=day:continue
            e=json.loads(payload)
            events.append({'sequence':seq,'recorded_at':recorded,
                'recorded_local':timestamp(recorded).astimezone(zone).isoformat(),
                'occurred_local':timestamp(e['occurred_at']).astimezone(zone).isoformat(),
                'entry_hash':entry,'event':e})
        kinds=Counter(e['event']['event_type'] for e in events)
        real_fills=[e for e in events if e['event']['event_type']=='BROKER_EXECUTION_OBSERVED' and e['event']['synthetic'] is False]
        all_agent=[json.loads(row[0]) for row in self.db.execute('SELECT payload,recorded_at FROM audit_events ORDER BY sequence')
                   if timestamp(row[1]).astimezone(zone).date()<=day]
        finished={e.get('intent_id') for e in all_agent if e['event_type']=='AGENT_ACTION_RESULT'}
        pending=[e.get('intent_id') for e in all_agent if e['event_type']=='AGENT_ACTION_INTENT' and e.get('intent_id') not in finished]
        return {'session_date':session_date,'timezone':tz,'date_basis':'recorded_at (late observations retain occurred_at)',
            **integrity,'event_count':len(events),'event_types':dict(kinds),'real_execution_observations':len(real_fills),
            'real_executions_occurred_in_session':sum(timestamp(e['event']['occurred_at']).astimezone(zone).date()==day for e in real_fills),
            'unresolved_agent_intents':sorted(set(x for x in pending if x)),
            'broker_end_of_day_reconciliation_performed':False,
            'provenance_cryptographically_authenticated':False,'events':events}

    def export(self,session_date,output_dir):
        report=self.report(session_date)
        directory=Path(output_dir)
        directory.mkdir(parents=True,exist_ok=True)
        stem=directory/('session-'+session_date)
        paths={'json':str(stem.with_suffix('.json')),'jsonl':str(stem.with_suffix('.jsonl')),'markdown':str(stem.with_suffix('.md'))}
        lines=['# 本地交易与操作审计：'+session_date,'',
            f"事件数：{report['event_count']}；真实成交观察：{report['real_execution_observations']}；完整性检查：通过。",'',
            '按落盘日期归档；实际发生时间单独保留。该记录不代替券商收盘对账，哈希链不认证调用来源。','',
            '| 落盘时间（美东） | 发生时间（美东） | 类型 | 来源 | 标的 | 数量 | 价格/止损 | 关联 |',
            '|---|---|---|---|---|---|---|---|']
        def cell(v):return str(v if v is not None else '').replace('|','\\|').replace('\n',' ')
        for row in report['events']:
            e=row['event']
            label=e['actor']+(' / synthetic' if e['synthetic'] else '')
            lines.append('| '+' | '.join(cell(v) for v in (row['recorded_local'],row['occurred_local'],e['event_type'],label,
                e.get('symbol'),e.get('quantity'),e.get('price',e.get('stop')),e.get('intent_id',e.get('broker_order_id',e['event_id']))))+' |')
        lines+=['','未完成/结果未知的 Agent 意图：'+str(report['unresolved_agent_intents']),
            '','详细参数、止盈止损与证据关联保存在 JSON/JSONL 的 event.details 中；没有日志不能证明没有交易。','']
        outputs={'json':json.dumps(report,ensure_ascii=False,indent=2)+'\n',
                 'jsonl':''.join(canonical(e)+'\n' for e in report['events']),
                 'markdown':'\n'.join(lines)}
        for key,value in outputs.items():
            final=Path(paths[key]);temp=final.with_suffix(final.suffix+'.tmp')
            temp.write_text(value);temp.replace(final)
        return {k:v for k,v in report.items() if k!='events'} | {'artifacts':paths}
