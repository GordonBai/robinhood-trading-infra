"""Independent script-only observation. Never submits, cancels or approves orders."""
from contextlib import closing
from datetime import datetime,timezone
import json
import hashlib
from types import SimpleNamespace
import os
import math
from pathlib import Path
import sqlite3
import time
import uuid

from .agent_worker import notify_local
from .session_control import process_matches,watchdog_process_matches
from .audit import digest,AuditTrail,canonical
from .broker import TERMINAL,APPROVAL_NO_ORDER_TERMINAL
from .risk import number


def _write(path,value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    temp=path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp');temp.write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n');temp.chmod(0o600);temp.replace(path)


def watchdog_entry_admission(db_path,status_file,policy,*,policy_path,build_fingerprint,control_pid,
                             clock=None,maximum_age=15,process_checker=watchdog_process_matches):
    """Read-only guardian proof for new risk; never gates protective management.

    A formerly healthy file is insufficient: the actual independent process must
    still match this DB, policy and status path, and its current healthy heartbeat
    must bind this control owner/build. This is local admission, not a global MCP
    authorization or a broker observation.
    """
    now=(clock or (lambda:datetime.now(timezone.utc)))();db=Path(db_path).resolve()
    issues=[];value={}
    try:
        value=json.loads(Path(str(db)+'.watchdog.json').read_text())
        if not isinstance(value,dict):raise ValueError('WATCHDOG_STATUS_INVALID')
        if (value.get('db')!=str(db) or value.get('status_file')!=str(Path(status_file).resolve())
                or value.get('control_pid')!=control_pid):issues.append('WATCHDOG_STATUS_BINDING_INVALID')
        if value.get('policy_hash')!=digest(policy) or value.get('source_fingerprint')!=build_fingerprint:
            issues.append('WATCHDOG_POLICY_OR_BUILD_MISMATCH')
        stamp=datetime.fromisoformat(value['checked_at'].replace('Z','+00:00'))
        age=(now-stamp).total_seconds()
        if not 0<=age<=maximum_age:issues.append('WATCHDOG_HEARTBEAT_STALE')
        pid=value.get('pid')
        if (type(pid) is not int or pid==control_pid
                or not process_checker(pid,db,Path(policy_path).resolve(),Path(status_file).resolve())):
            issues.append('WATCHDOG_PROCESS_NOT_VERIFIED')
        if (value.get('healthy') is not True or value.get('issues')!=[]
                or value.get('process_verified') is not True or value.get('closed_safe') is not False):
            issues.append('WATCHDOG_NOT_READY')
        # Its prior copy of the sticky gate is not observer health. The control
        # owner reads the current audited gate separately, so a release is not
        # immediately re-locked merely while the observer's old copy catches up.
    except (OSError,ValueError,TypeError,KeyError,AttributeError):issues.append('WATCHDOG_STATUS_MISSING_OR_INVALID')
    return {'required':True,'ready':not issues,'issues':sorted(set(issues)),
        'watchdog_pid':value.get('pid') if isinstance(value,dict) else None,
        'checked_at':value.get('checked_at') if isinstance(value,dict) else None,
        'broker_writes':0,'model_calls':0,'automatic_execution_enabled':False}


def inhibit_new_entries(db_path,issues,*,inhibit_file=None,clock=None):
    db=Path(db_path).resolve();now=(clock or (lambda:datetime.now(timezone.utc)))()
    if not issues:raise ValueError('ENTRY_INHIBIT_REASON_REQUIRED')
    _write(inhibit_file or str(db)+'.entry-inhibit.json',{'db':str(db),'blocked':True,
        'at':now.isoformat(),'issues':sorted(set(issues)),
        'clearing_requires':'EXPLICIT_RECONCILED_OPERATOR_DISPOSITION'})


def entry_inhibit(path,db_path):
    """Missing optional inhibit is clear; an unreadable or foreign gate fails closed."""
    target=Path(path)
    if not target.exists():return {'blocked':False,'reason':'NO_WATCHDOG_INHIBIT'}
    try:
        value=json.loads(target.read_text())
        if not isinstance(value,dict) or value.get('db')!=str(Path(db_path).resolve()):
            raise ValueError('GATE_BINDING_INVALID')
        if value.get('blocked') is False:
            with closing(sqlite3.connect(Path(db_path).resolve().as_uri()+'?mode=ro',uri=True,timeout=1)) as db:
                AuditTrail.verify(SimpleNamespace(db=db))
                row=db.execute('SELECT payload FROM audit_events WHERE event_id=?',(value.get('release_id'),)).fetchone()
                event=json.loads(row[0]) if row else {}
                if (event.get('event_type')!='RECONCILIATION_ISSUE' or event.get('details',{}).get('released_gate_sha256')!=digest(value)):
                    raise ValueError('GATE_RELEASE_AUDIT_REQUIRED')
            return {'blocked':False,'reason':'AUDITED_OPERATOR_RELEASE','evidence':value}
        if value.get('blocked') is not True:raise ValueError('GATE_BINDING_INVALID')
        return {'blocked':True,'reason':'WATCHDOG_STICKY_INHIBIT','evidence':value}
    except (OSError,ValueError,TypeError,sqlite3.Error):return {'blocked':True,'reason':'WATCHDOG_GATE_UNREADABLE_OR_BINDING_INVALID'}


def release_entry_inhibit(book,snapshot,*,reason,inhibit_file=None):
    """Fresh, audited flat-account disposition; never resets risk or enables writes."""
    if not isinstance(reason,str) or not reason.strip():raise ValueError('WATCHDOG_RELEASE_REASON_REQUIRED')
    book._snapshot_check(snapshot,book.clock());book.audit.verify()
    if book.simulation or snapshot.get('provider')!='robinhood':raise ValueError('WATCHDOG_RELEASE_REAL_BROKER_REQUIRED')
    row=book.db.execute('SELECT payload FROM broker_current WHERE id=1').fetchone()
    if not row:raise ValueError('WATCHDOG_RELEASE_JOURNALED_SNAPSHOT_REQUIRED')
    health_row=book.db.execute('SELECT payload FROM broker_health WHERE id=1').fetchone()
    health=json.loads(health_row[0]) if health_row else {}
    source_sha=hashlib.sha256(json.dumps(snapshot,sort_keys=True).encode()).hexdigest()
    source=book.db.execute('SELECT payload FROM broker_observations WHERE digest=?',(source_sha,)).fetchone()
    if health.get('healthy') is not True or health.get('at')!=snapshot['observed_at'] or not source or canonical(json.loads(source[0]))!=canonical(snapshot):
        raise ValueError('WATCHDOG_RELEASE_CURRENT_HEALTHY_SNAPSHOT_REQUIRED')
    persisted=json.loads(row[0]);journal_only={'reconciliation_issues','new_executions_seen','baseline_established'}
    if digest({k:v for k,v in persisted.items() if k not in journal_only})!=digest(snapshot) or persisted.get('reconciliation_issues'):
        raise ValueError('WATCHDOG_RELEASE_CURRENT_HEALTHY_SNAPSHOT_REQUIRED')
    tracked={r[0] for r in book.db.execute('SELECT ref_id FROM external_broker_refs')}
    if not tracked<=set(snapshot.get('requested_approval_ids',[])):raise ValueError('WATCHDOG_RELEASE_TRACKED_APPROVAL_NOT_QUERIED')
    for identity in tracked:
        approval=snapshot.get('approval_states',{}).get(identity,{})
        if (approval.get('approval_id')!=identity or approval.get('asset_class')!='ASSET_CLASS_EQUITY'
                or approval.get('is_edited') is not False
                or approval.get('state') not in APPROVAL_NO_ORDER_TERMINAL|{'APPROVAL_STATE_APPROVED','APPROVAL_STATE_AUTO_APPROVED'}
                or identity in snapshot.get('missing_approval_ids',[])):
            raise ValueError('WATCHDOG_RELEASE_APPROVAL_UNRESOLVED')
        if approval['state'] not in APPROVAL_NO_ORDER_TERMINAL and not any(o.get('ref_id')==identity for o in snapshot['orders'].values()):
            raise ValueError('WATCHDOG_RELEASE_APPROVED_WRITE_NOT_OBSERVED')
    recovery=book.recovery_status(snapshot)
    if (book.active_issues(snapshot) or snapshot['pending_approvals'] or snapshot.get('missing_approval_ids')
            or any(number(p['quantity'])!=0 for p in snapshot['positions'].values())
            or any(o['state'] not in TERMINAL for o in snapshot['orders'].values())
            or any(g['state'] not in TERMINAL for g in snapshot.get('groups',[]))
            or any(a['state'] in {'READY','RUNNING','UNKNOWN','WAITING_USER','REPORTED'} and a['id'] not in recovery['acknowledged_action_ids'] for a in book.actions())
            or any(t['phase']!='FLAT_VERIFIED' and tid not in recovery['acknowledged_trade_ids'] for tid,t in book.state()['trades'].items())):
        raise ValueError('WATCHDOG_RELEASE_RECONCILED_FLAT_REQUIRED')
    from .recovery import _result_order_id
    events=[json.loads(r[0]) for r in book.db.execute('SELECT payload FROM audit_events ORDER BY sequence')]
    module_intents={a['id'] for a in book.actions()}
    def proven_no_order(operation_id):
        row=book.db.execute('SELECT operation FROM external_operations WHERE operation_id=?',(operation_id,)).fetchone()
        if not row or row[0]!='place':return False
        aliases=[r[0] for r in book.db.execute('SELECT ref_id FROM external_broker_refs WHERE operation_id=?',(operation_id,))]
        if not aliases or not set(aliases)<=set(snapshot.get('requested_approval_ids',[])):return False
        if book.db.execute('SELECT 1 FROM external_broker_order_ids WHERE operation_id=?',(operation_id,)).fetchone():return False
        return all(snapshot.get('approval_states',{}).get(identity,{}).get('state') in APPROVAL_NO_ORDER_TERMINAL
            and not any(o.get('ref_id')==identity for o in snapshot['orders'].values()) for identity in aliases)
    for operation_id,kind in book.db.execute('SELECT operation_id,operation FROM external_operations'):
        if kind=='review':continue
        results=[r for r in events if r['event_type']=='AGENT_ACTION_RESULT' and r.get('intent_id')==operation_id]
        identities=[identity for r in results if (identity:=_result_order_id(r))]
        if identities and all(i in snapshot['orders'] and snapshot['orders'][i]['state'] in TERMINAL for i in identities):continue
        if not proven_no_order(operation_id):raise ValueError('WATCHDOG_RELEASE_UNKNOWN_EXTERNAL_WRITE:'+operation_id)
    for event in events:
        if event['event_type']!='AGENT_ACTION_INTENT' or event['details']['action']=='review' or event['intent_id'] in module_intents:continue
        results=[r for r in events if r['event_type']=='AGENT_ACTION_RESULT' and r.get('intent_id')==event['intent_id']]
        identities=[identity for r in results if (identity:=_result_order_id(r))]
        if not identities and proven_no_order(event['intent_id']):continue
        if not identities or any(i not in snapshot['orders'] or snapshot['orders'][i]['state'] not in TERMINAL for i in identities):
            raise ValueError('WATCHDOG_RELEASE_MANUAL_WRITE_UNRESOLVED:'+event['intent_id'])
    db=book.db.execute('PRAGMA database_list').fetchone()[2]
    target=inhibit_file or db+'.entry-inhibit.json'
    if Path(target).resolve()!=Path(db+'.entry-inhibit.json').resolve():raise ValueError('WATCHDOG_RELEASE_DEFAULT_GATE_ONLY')
    now=book.clock().isoformat();identity='watchdog-release-'+digest([str(Path(db).resolve()),now,reason])[:32]
    value={'db':str(Path(db).resolve()),'blocked':False,'released_at':now,'release_id':identity,
        'snapshot_sha256':digest(snapshot),'reason':reason,'execution_enabled':False}
    book.audit.append({'event_id':identity,'occurred_at':now,'event_type':'RECONCILIATION_ISSUE',
        'actor':'script','synthetic':book.simulation,'account':snapshot['account_mask'],
        'details':{'status':'watchdog_entry_inhibit_released','released_gate_sha256':digest(value),
            'snapshot_sha256':digest(snapshot),'reason':reason,'execution_enabled':False,'risk_locks_unchanged':True}})
    _write(target,value)
    return {'released':True,'release_id':identity,'execution_enabled':False,'risk_locks_unchanged':True}


def watchdog_check(db_path,status_file,*,state_file=None,inhibit_file=None,clock=None,
                   heartbeat_limit=35,broker_limit=30,worker_limit=135,protection_limit=30,
                   process_checker=process_matches,notifier=notify_local,policy=None,build_fingerprint=None,
                   expected_control_pid=None):
    """One bounded check. Sticky risk inhibit needs explicit operator disposition.

    Historical CLOSED/SAFE_FLAT is quiet only with no active process, no pending
    actions and no nonterminal trade in the durable book. No restart is attempted.
    """
    if any(type(v) not in (int,float) or not math.isfinite(v) or v<=0 for v in (heartbeat_limit,broker_limit,worker_limit,protection_limit)):
        raise ValueError('INVALID_WATCHDOG_THRESHOLDS')
    now=(clock or (lambda:datetime.now(timezone.utc)))();db=Path(db_path).resolve()
    state_file=state_file or str(db)+'.watchdog.json';inhibit_file=inhibit_file or str(db)+'.entry-inhibit.json'
    issues=[];status={};pending=[];trades={};prior={};retirement={'acknowledged_action_ids':[],'acknowledged_trade_ids':[],'blockers':[]}
    try:prior=json.loads(Path(state_file).read_text())
    except (OSError,ValueError,TypeError):pass
    if not isinstance(prior,dict):prior={}
    try:
        status=json.loads(Path(status_file).read_text())
        if not isinstance(status,dict) or status.get('db')!=str(db):raise ValueError('STATUS_DATABASE_BINDING_INVALID')
    except (OSError,ValueError,TypeError):
        status={};issues.append('CONTROL_STATUS_MISSING_OR_INVALID')
    running=process_checker(status.get('pid'),db)
    owner_matches=expected_control_pid is None or status.get('pid')==expected_control_pid
    if not owner_matches:issues.append('CONTROL_OWNER_STATUS_BINDING_INVALID')
    historical_close=owner_matches and not running and status.get('reason')=='AUTHORIZED_SESSION_CLOSED' and status.get('final_flat_verified') is True
    try:
        with closing(sqlite3.connect(db.as_uri()+'?mode=ro',uri=True,timeout=1)) as connection:
            connection.execute('BEGIN')
            pending=connection.execute("SELECT id,kind,state,created_at FROM live_actions WHERE state IN ('READY','RUNNING','UNKNOWN','WAITING_USER','REPORTED')").fetchall()
            row=connection.execute('SELECT payload FROM live_state WHERE id=1').fetchone()
            state=json.loads(row[0]) if row else {};trades=state.get('trades',{})
            from .recovery import unresolved_write_intents
            has_snapshot=connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='broker_current'").fetchone()
            current=connection.execute('SELECT payload FROM broker_current WHERE id=1').fetchone() if has_snapshot else None
            snapshot=json.loads(current[0]) if current else {}
            if historical_close and current and snapshot.get('observed_at')!=status.get('observed_at'):
                issues.append('CLOSED_BROKER_SNAPSHOT_BINDING_INVALID')
            unresolved_external=unresolved_write_intents(connection,snapshot,
                account_mask='••••'+policy['account_suffix'] if policy else snapshot.get('account_mask'),
                simulation=state.get('simulation') is True)
            if unresolved_external:issues.append('UNRESOLVED_EXTERNAL_WRITE')
            has_retirement=connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='recovery_cases'").fetchone()
            if has_retirement and connection.execute("SELECT 1 FROM recovery_cases WHERE status='ACKNOWLEDGED'").fetchone():
                if policy is None:raise ValueError('WATCHDOG_RETIREMENT_POLICY_REQUIRED')
                from .recovery import readonly_recovery_view
                retirement=readonly_recovery_view(db,policy,historical_as_of=status.get('observed_at') if historical_close else None,clock=lambda:now)
                if retirement['blockers']:raise ValueError('WATCHDOG_RETIREMENT_EVIDENCE_INVALID')
                if historical_close and retirement.get('observed_at')!=status.get('observed_at'):
                    raise ValueError('WATCHDOG_RETIREMENT_FINAL_SNAPSHOT_MISMATCH')
            connection.rollback()
        pending=[a for a in pending if a[0] not in retirement['acknowledged_action_ids']]
        trades={tid:t for tid,t in trades.items() if tid not in retirement['acknowledged_trade_ids']}
    except (sqlite3.Error,ValueError,TypeError,KeyError):issues.append('DURABLE_BOOK_UNREADABLE_OR_RECOVERY_INVALID')
    def age(value):
        try:
            stamp=datetime.fromisoformat(value.replace('Z','+00:00'))
            elapsed=(now-stamp).total_seconds()
            return elapsed if elapsed>=0 else float('inf')
        except (ValueError,TypeError,AttributeError):return float('inf')
    settled=not pending and all(t.get('phase')=='FLAT_VERIFIED' for t in trades.values())
    closed=not issues and not running and settled and status.get('reason')=='AUTHORIZED_SESSION_CLOSED' and status.get('final_flat_verified') is True
    if not closed:
        if not running:issues.append('CONTROL_PROCESS_NOT_VERIFIED')
        if age(status.get('control_heartbeat_at'))>heartbeat_limit:issues.append('CONTROL_HEARTBEAT_STALE')
        if age(status.get('observed_at'))>broker_limit:issues.append('BROKER_OBSERVATION_STALE')
        task=status.get('worker_task',{})
        if not isinstance(task,dict):issues.append('WORKER_STATUS_INVALID')
        elif task.get('running'):
            worker_age=task.get('age_seconds')
            if type(worker_age) not in (int,float) or not math.isfinite(worker_age) or worker_age<0:issues.append('WORKER_STATUS_INVALID')
            elif worker_age+age(status.get('control_heartbeat_at'))>worker_limit:issues.append('WORKER_OVERDUE')
        if any(t.get('unprotected_since') and age(t['unprotected_since'])>protection_limit for t in trades.values()):issues.append('ACTUAL_POSITION_UNPROTECTED_OVERDUE')
        if any(kind in {'protect','raise_stop'} and age(created)>protection_limit for _,kind,_,created in pending):issues.append('PROTECTION_ACTION_OVERDUE')
    issues=sorted(set(issues));changed=issues!=prior.get('issues',[])
    result={'db':str(db),'pid':os.getpid(),'status_file':str(Path(status_file).resolve()),
        'control_pid':status.get('pid'),'expected_control_pid':expected_control_pid,
        'policy_hash':digest(policy) if policy is not None else None,
        'source_fingerprint':build_fingerprint,
        'control_status_observed_at':status.get('observed_at'),'control_status_reason':status.get('reason'),
        'checked_at':now.isoformat(),'issues':issues,'healthy':not issues,
        'closed_safe':closed,'closed_safety_is_historical':closed,'historical_manual_recovery':bool(retirement['acknowledged_trade_ids']),'process_verified':running,'new_entries_blocked':bool(issues) or entry_inhibit(inhibit_file,db)['blocked'],
        'pending_actions':len(pending),'notifications_sent':int(bool(issues and changed)),
        'broker_writes':0,'model_calls':0,'automatic_restart':False}
    if issues:
        inhibit_new_entries(db,issues,inhibit_file=inhibit_file,clock=lambda:now)
        if changed:notifier('脚本 watchdog 发现控制或持仓待办异常；已阻止新增风险，请核对。')
    _write(state_file,result)
    return result


def run_watchdog(db_path,status_file,*,cycles=1,poll_seconds=5,
                 stop_on_safe_close=False,expected_control_pid=None,**kwargs):
    if not 1<=cycles<=100000 or not 1<=poll_seconds<=60:raise ValueError('INVALID_WATCHDOG_PARAMETERS')
    if type(stop_on_safe_close) is not bool or (stop_on_safe_close and
            (type(expected_control_pid) is not int or expected_control_pid<=1)):
        raise ValueError('SAFE_CLOSE_STOP_REQUIRES_BOUND_CONTROL_PID')
    result={}
    from .shadow import single_instance
    with single_instance(str(Path(db_path).resolve())+'.watchdog'):
        from .qualification import source_fingerprint
        build=source_fingerprint()
        for index in range(cycles):
            result=watchdog_check(db_path,status_file,build_fingerprint=build,
                expected_control_pid=expected_control_pid,**kwargs)
            # Historical flatness from another run must never retire the
            # observer of a newly launched controller. Keep observing failures,
            # live owners, UNKNOWN writes and unverified closes.
            if (stop_on_safe_close and result.get('control_pid')==expected_control_pid
                    and result.get('closed_safe') is True and result.get('healthy') is True
                    and result.get('issues')==[] and result.get('process_verified') is False):
                result['stopped_after_verified_safe_close']=True
                break
            if index+1<cycles:time.sleep(poll_seconds)
    return result
