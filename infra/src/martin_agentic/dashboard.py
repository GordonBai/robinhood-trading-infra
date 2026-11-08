"""Loopback-only, read-only dashboard. No model calls or broker calls per refresh."""
from datetime import datetime,timezone
from decimal import Decimal
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import json
import hashlib
import os
from pathlib import Path
import re
import sqlite3
from urllib.parse import urlsplit

from .broker import TERMINAL
from .ledger import timestamp
from .market import inspect_recording
from .qualification import source_fingerprint
from .session_control import process_matches
from .runtime_health import health_scope, observation_healthy

WEB=Path(__file__).with_name('web')
PRIVATE={'account_number','rhs_account_number','rhc_account_number','account_key','password','authorization',
         'access_token','refresh_token','api_key','headers','cookies','cookie','token'}


def clean(value):
    if isinstance(value,dict):return {k:clean(v) for k,v in value.items() if k.lower() not in PRIVATE}
    if isinstance(value,list):return [clean(v) for v in value]
    return value


def read_json(path):
    p=Path(path)
    if not p.is_file():return {}
    if p.stat().st_size>4_000_000:raise ValueError('DASHBOARD_INPUT_TOO_LARGE')
    result=json.loads(p.read_text())
    if not isinstance(result,dict):raise ValueError('DASHBOARD_INPUT_NOT_OBJECT')
    return result


def purpose(trade):
    if trade.get('purpose') in {'strategy','engineering'}:return trade['purpose']
    evidence=trade.get('evidence',{})
    if evidence.get('strategy_version') and evidence.get('setup'):return 'strategy'
    return 'unclassified'


class DashboardData:
    def __init__(self,db_path,status_file,capture_dir,release_file,*,clock=None):
        self.db=Path(db_path).resolve();self.status=Path(status_file).resolve();self.capture=Path(capture_dir).resolve()
        self.release=Path(release_file).resolve();self.clock=clock or (lambda:datetime.now(timezone.utc))

    def snapshot(self):
        now=self.clock();errors=[]
        try:status=read_json(self.status)
        except (ValueError,OSError):status={};errors.append('STATUS_UNAVAILABLE')
        def age(value):
            if not isinstance(value,str):return None
            try:return round((now-timestamp(value)).total_seconds(),2)
            except (ValueError,TypeError):return None
        observed_age=age(status.get('observed_at'))
        alive=process_matches(status.get('pid'),self.db)
        healthy=bool(alive and status.get('running') and observation_healthy(status) and observed_age is not None and 0<=observed_age<=30)
        state={};strategy={};actions=[];audit=[];fills=[];drafts=[];broker={};alias_refs={};exact_order_ids={};recovery_cases=[];fills_truncated=False;closed_broker={};archive_quotes=[]
        if self.db.is_file():
            connection=sqlite3.connect(self.db.as_uri()+'?mode=ro',uri=True,timeout=2)
            try:
                connection.execute('PRAGMA query_only=ON');connection.execute('BEGIN')
                tables={r[0] for r in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                def one(table):
                    if table not in tables:return {}
                    row=connection.execute('SELECT payload FROM '+table+' WHERE id=1').fetchone()
                    return json.loads(row[0]) if row else {}
                state=one('live_state');strategy=one('live_strategy');broker=one('broker_current')
                if {'live_event_archive','live_decision_archive'}<=tables:
                    rows=connection.execute("""SELECT e.payload FROM live_event_archive e
                        WHERE json_extract(e.payload,'$.kind')='quote' AND EXISTS(
                            SELECT 1 FROM live_decision_archive d WHERE d.event_id=e.id
                            AND json_extract(d.payload,'$.status')='CONSUMED')
                        ORDER BY e.rowid DESC LIMIT 30""").fetchall()
                    seen=set()
                    for raw, in rows:
                        event=json.loads(raw);quote=event['data'];symbol=quote['symbol']
                        if symbol in seen:continue
                        seen.add(symbol)
                        archive_quotes.append({**quote,'source_sha256':event.get('source_sha256'),
                            'source_type':'owner_consumed_quote_archive','owner_consumed':True})
                closed_broker=broker
                if 'broker_observations' in tables and 'audit_events' in tables and status.get('observed_at'):
                    rows=connection.execute("SELECT digest,payload FROM broker_observations WHERE json_extract(payload,'$.observed_at')=? LIMIT 2",(status['observed_at'],)).fetchall()
                    if len(rows)==1:
                        source_hash,raw=rows[0]
                        event_row=connection.execute('SELECT payload FROM audit_events WHERE event_id=?',('account-'+source_hash,)).fetchone()
                        event=json.loads(event_row[0]) if event_row else {}
                        if (hashlib.sha256(raw.encode()).hexdigest()==source_hash
                                and event.get('event_type')=='ACCOUNT_OBSERVED'
                                and event.get('occurred_at')==status['observed_at']
                                and event.get('details',{}).get('source_sha256')==source_hash
                                and event.get('details',{}).get('reconciliation_issues')==[]):
                            closed_broker=json.loads(raw)
                if 'live_actions' in tables:
                    for row in connection.execute('SELECT id,trade_id,kind,state,payload,created_at,updated_at,report FROM live_actions ORDER BY created_at DESC LIMIT 150'):
                        a=dict(zip(('id','trade_id','kind','state','payload','created_at','updated_at','report'),row))
                        a['payload']=json.loads(a['payload']);a['report']=json.loads(a['report']) if a['report'] else None
                        a['purpose']=purpose(state.get('trades',{}).get(a['trade_id'],{}));actions.append(a)
                if 'external_broker_refs' in tables and 'live_actions' in tables:
                    alias_refs=dict(connection.execute('SELECT r.ref_id,a.trade_id FROM external_broker_refs r JOIN live_actions a ON a.id=r.action_id'))
                if 'external_broker_order_ids' in tables and 'live_actions' in tables and state.get('account_key'):
                    exact_order_ids=dict(connection.execute('SELECT r.order_id,a.trade_id FROM external_broker_order_ids r JOIN live_actions a ON a.id=r.action_id WHERE r.account_key=?', (state['account_key'],)))
                if 'audit_events' in tables:
                    for seq,recorded,payload in connection.execute('SELECT sequence,recorded_at,payload FROM audit_events ORDER BY sequence DESC LIMIT 80'):
                        audit.append({'sequence':seq,'recorded_at':recorded,'event':json.loads(payload)})
                if 'broker_executions' in tables:
                    fills=[json.loads(row[0]) for row in connection.execute('SELECT payload FROM broker_executions ORDER BY rowid DESC LIMIT 1000')]
                    fills_truncated=connection.execute('SELECT COUNT(*) FROM broker_executions').fetchone()[0]>1000
                if 'engineering_plans' in tables:
                    drafts=[json.loads(row[0]) for row in connection.execute('SELECT payload FROM engineering_plans ORDER BY rowid DESC LIMIT 30')]
                if 'recovery_cases' in tables:
                    recovery_cases=[json.loads(row[0]) for row in connection.execute('SELECT payload FROM recovery_cases ORDER BY rowid DESC LIMIT 30')]
            except (sqlite3.Error,ValueError):errors.append('DATABASE_READ_FAILED')
            finally:connection.rollback();connection.close()
        else:errors.append('DATABASE_MISSING')
        # A closed incident keeps immutable WAITING_USER history, but an
        # audited recovery must not appear as currently actionable approval.
        # Closed displays may inspect the last broker observation historically;
        # live admission still requires the ordinary fresh recovery proof.
        retirement={'acknowledged_action_ids':[], 'historical_only':False,
                    'live_admission_authorized':False, 'blockers':[]}
        if recovery_cases:
            policy=next((a['payload'].get('policy') for a in actions
                         if isinstance(a['payload'].get('policy'),dict)),None)
            if policy:
                from .recovery import readonly_recovery_view
                closed=not alive and status.get('running') is False
                retirement=readonly_recovery_view(self.db,policy,
                    historical_as_of=broker.get('observed_at') if closed else None,
                    clock=self.clock)
        retired_ids=set(retirement['acknowledged_action_ids'])
        for action in actions:action['effective_retired']=action['id'] in retired_ids
        quotes=[];bars=[];quality={};capture_hash=status.get('data',{}).get('source_sha256')
        path=None
        if isinstance(capture_hash,str) and re.fullmatch('[0-9a-f]{64}',capture_hash):path=self.capture/('intraday-'+capture_hash+'.json')
        elif self.capture.is_dir():
            files=list(self.capture.glob('intraday-*.json'))
            if files:path=max(files,key=lambda p:p.stat().st_mtime_ns)
        try:
            if path and path.is_file():
                raw=read_json(path)
                at=status.get('data',{}).get('at') or max(r['observed_at'] for r in raw['records'])
                normalized=inspect_recording(raw,as_of=at)
                quotes=[{**q,'age_seconds':max(age(q['bid_at']) or 0,age(q['ask_at']) or 0),
                    'fresh_now':all(age(q[k]) is not None and 0<=age(q[k])<=15 for k in ('bid_at','ask_at','observed_at'))} for q in normalized['quotes']]
                by_symbol={}
                for bar in normalized['bars']:by_symbol.setdefault(bar['symbol'],[]).append(bar)
                bars=[bar for group in by_symbol.values() for bar in group[-24:]]
                quality={'checked_at':at,'checked_at_capture':normalized['data_usable'],
                    'errors':normalized['errors']+normalized['freshness_errors'],'warnings':normalized['warnings'],
                    'note':'Capture-time validation retained; current freshness is separately calculated.'}
        except (ValueError,OSError,KeyError,TypeError):errors.append('CAPTURE_NOT_READABLE')
        if not quotes and archive_quotes:
            # Current collectors preserve content-addressed raw responses rather
            # than legacy intraday-*.json. The owner archive is also available
            # during the next background read; never invent a new quote time.
            quotes=[{**q,'age_seconds':max(age(q['bid_at']) or 0,age(q['ask_at']) or 0),
                'fresh_now':all(age(q[k]) is not None and 0<=age(q[k])<=15 for k in ('bid_at','ask_at','observed_at'))}
                for q in archive_quotes]
            quality={'source':'owner_consumed_quote_archive','current_freshness_checked':True,
                'provenance_cryptographically_authenticated':False,
                'note':'Last owner-consumed quotes; original timestamps retained. Not a new broker request or execution authorization.'}
        # Classify real executions by exact known broker order/ref; never use a draft as a fill.
        action_refs={a['payload']['ref_id']:a['trade_id'] for a in actions}
        action_refs.update(alias_refs)
        order_trades={oid:action_refs[o['ref_id']] for oid,o in broker.get('orders',{}).items() if o.get('ref_id') in action_refs}
        for oid,tid in exact_order_ids.items():
            if oid not in broker.get('orders',{}) or broker.get('account_key')!=state.get('account_key'):continue
            if oid in order_trades and order_trades[oid]!=tid:
                errors.append('BROKER_ORDER_OWNER_CONFLICT');order_trades.pop(oid)
            else:order_trades[oid]=tid
        by_trade={}
        for fill in fills:
            tid=order_trades.get(fill['order_id']);trade=state.get('trades',{}).get(tid,{})
            fill['purpose']=purpose(trade);fill['trade_id']=tid
            for case in recovery_cases:
                if fill['order_id'] in case.get('manual_order_ids',[]):
                    fill['purpose']='manual_recovery';fill['recovery_case_id']=case['id']
            if tid:by_trade.setdefault(tid,[]).append(fill)
        pnl={'strategy':{'closed_trades':0,'realized_pnl':'0'},'engineering':{'closed_trades':0,'realized_pnl':'0'}}
        trades=[]
        for tid,trade in state.get('trades',{}).items():
            value={**trade,'purpose':purpose(trade),'realized_pnl':None};own=by_trade.get(tid,[])
            buy=sum((Decimal(e['quantity']) for e in own if e['side']=='buy'),Decimal(0))
            sell=sum((Decimal(e['quantity']) for e in own if e['side']=='sell'),Decimal(0))
            if not fills_truncated and not state.get('simulation') and buy>0 and buy==sell and trade['phase']=='FLAT_VERIFIED':
                amount=sum((Decimal(e['quantity'])*Decimal(e['price'])*(1 if e['side']=='sell' else -1)-Decimal(e['fees']) for e in own),Decimal(0))
                value['realized_pnl']=str(amount)
                if value['purpose'] in pnl:
                    summary=pnl[value['purpose']];summary['closed_trades']+=1
                    summary['realized_pnl']=str(Decimal(summary['realized_pnl'])+amount)
            trades.append(value)
        for draft in drafts:
            tid=draft['id'];trade=state.get('trades',{}).get(tid)
            if not trade:continue
            related=[a for a in actions if a['trade_id']==tid]
            observed_orders=[oid for oid,owner in order_trades.items() if owner==tid]
            draft['parameters_user_specified']=trade.get('evidence',{}).get('parameters_user_specified',False)
            draft['submitted']=bool(observed_orders)
            draft['actual_trade_phase']=trade['phase']
            if by_trade.get(tid):draft['status']='ACTUAL_FILL_OBSERVED'
            elif observed_orders:draft['status']='BROKER_ORDER_OBSERVED'
            elif any(a['state']=='WAITING_USER' for a in related):draft['status']='AWAITING_NATIVE_CONFIRMATION'
            else:draft['status']='QUEUED_NOT_BROKER_VERIFIED'
        try:release=read_json(self.release)
        except (ValueError,OSError):release={};errors.append('RELEASE_REPORT_UNAVAILABLE')
        # A closed status file alone cannot establish safety for another ledger.
        # Bind the historical claim to this database and its exact final snapshot.
        try:
            closed_safe=bool(not alive and not errors and status.get('db')==str(self.db)
                and status.get('running') is False and status.get('reason')=='AUTHORIZED_SESSION_CLOSED'
                and status.get('final_flat_verified') is True and status.get('attention_required') is False
                and observed_age is not None and observed_age>=0
                and closed_broker.get('observed_at')==status.get('observed_at')
                and isinstance(closed_broker.get('account_mask'),str)
                and closed_broker.get('account_mask','').startswith('••••')
                and len(closed_broker.get('account_mask',''))==8
                and closed_broker.get('pending_approvals')==0
                and not closed_broker.get('missing_approval_ids') and not closed_broker.get('reconciliation_issues')
                and all(Decimal(p['quantity'])==0 for p in closed_broker['positions'].values())
                and all(o['state'] in TERMINAL for o in closed_broker['orders'].values())
                and all(g['state'] in TERMINAL for g in closed_broker.get('groups',[])))
        except (KeyError,TypeError,ValueError,ArithmeticError):closed_safe=False
        return clean({'schema_version':1,'server_time':now.isoformat(),'read_only':True,'account':closed_broker.get('account_mask') or '••••----',
            'recovery_cases':recovery_cases,'recovery_disposition':retirement,
            'runtime':{'healthy_now':healthy,'process_alive':alive,'reported_running':status.get('running',False),
                'health_scope':health_scope(status),
                'new_entries_blocked':status.get('new_entries_blocked',True),
                'pid':status.get('pid'),'observed_at':status.get('observed_at'),'observation_age_seconds':observed_age,
                'cycle':status.get('cycle'),'agent_mode':status.get('agent_mode'),'reason':status.get('reason'),
                'attention_required':status.get('attention_required',False),'execution_gate':status.get('execution_gate'),
                'final_flat_verified':status.get('final_flat_verified') is True,
                'historical_manual_recovery':status.get('historical_manual_recovery') is True,
                'session_closed_safe':closed_safe,
                'build_matches':status.get('source_fingerprint')==source_fingerprint()},
            'portfolio':status.get('broker',{}).get('portfolio',{}),'broker_summary':status.get('broker',{}),
            'data':status.get('data',{}),'quotes':quotes,'bars':bars,'data_quality':quality,
            'strategy':strategy,'actions':actions,'trades':trades,'engineering_plans':drafts,
            'fills':fills,'fills_truncated':fills_truncated,'performance':pnl,'audit_events':audit,'issues':state.get('issues',[]),
            'synthetic_book':state.get('simulation'),'release':release,'errors':errors,
            'note':'Dashboard refresh reads local files/SQLite only; no Agent or broker request.'})


def make_server(data,port=8765):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def do_GET(self):
            host=self.headers.get('Host','');allowed={f'127.0.0.1:{self.server.server_port}',f'localhost:{self.server.server_port}'}
            origin=self.headers.get('Origin')
            if host not in allowed or origin and origin not in {'http://'+h for h in allowed}:
                self.send_error(403);return
            path=urlsplit(self.path).path
            if path=='/api/state':
                try:body=json.dumps(data.snapshot(),ensure_ascii=False,allow_nan=False).encode();mime='application/json; charset=utf-8'
                except (ValueError,OSError,sqlite3.Error):
                    self.send_error(503,'Dashboard snapshot unavailable');return
            elif path in {'/','/app.js','/style.css'}:
                name={'/':'index.html','/app.js':'app.js','/style.css':'style.css'}[path]
                body=(WEB/name).read_bytes();mime={'/':'text/html; charset=utf-8','/app.js':'text/javascript; charset=utf-8','/style.css':'text/css; charset=utf-8'}[path]
            else:self.send_error(404);return
            self.send_response(200);self.send_header('Content-Type',mime);self.send_header('Content-Length',str(len(body)))
            self.send_header('Cache-Control','no-store');self.send_header('X-Content-Type-Options','nosniff')
            self.send_header('Content-Security-Policy',"default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'")
            self.end_headers();self.wfile.write(body)
        def do_POST(self):self.send_error(405,'Read-only dashboard')
    return ThreadingHTTPServer(('127.0.0.1',port),Handler)


def serve_dashboard(db_path,status_file,capture_dir,release_file,*,port=8765):
    if not 1024<=port<=65535:raise ValueError('DASHBOARD_PORT_OUT_OF_RANGE')
    server=make_server(DashboardData(db_path,status_file,capture_dir,release_file),port)
    print(json.dumps({'url':f'http://127.0.0.1:{port}','read_only':True}),flush=True)
    try:server.serve_forever(poll_interval=.5)
    finally:server.server_close()
