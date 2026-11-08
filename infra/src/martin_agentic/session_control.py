"""Manage the bounded session in its verified local environment, without launchd.

No credential extraction, account settings changes or automatic write enabling.
"""
from datetime import datetime,timezone
import fcntl
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import uuid

from .qualification import candidate_root, source_fingerprint
from .local_worker_wake import LOCAL_WORKER_ADAPTER, worker_adapter_selection_report
from .shadow import single_instance


def process_matches(pid,db_path,command_marker=None,worker_adapter=None,strategy=None,strategy_plugins=None):
    if type(pid) is not int or pid<=1:return False
    try:
        result=subprocess.run(['ps','-p',str(pid),'-o','command='],capture_output=True,text=True,timeout=3)
    except (OSError,subprocess.TimeoutExpired):return False
    command=result.stdout.strip()+' '
    db_token='--db '+str(Path(db_path).resolve())+' '
    if worker_adapter is not None:
        if worker_adapter == 'none':
            # The signed-off 083 CLI predates this flag; absence means the
            # legacy default none. An explicit non-none selection is never
            # mistaken for none.
            if '--worker-adapter local-worker-review ' in command:
                return False
            if '--worker-adapter ' in command:
                if '--worker-adapter none ' not in command:
                    return False
        elif '--worker-adapter '+str(worker_adapter)+' ' not in command:
            return False
    if strategy is not None:
        if '--strategy ' + str(strategy) + ' ' not in command:
            return False
        for plugin in tuple(strategy_plugins or ()):
            if '--strategy-plugin ' + str(plugin) + ' ' not in command:
                return False
    if command_marker is not None:
        # A caller-supplied marker is the exact launch handle this controller
        # created. It is accepted only as a whole command line; a prefix would
        # let an unrelated process with the same leading words and database be
        # signalled. Metadata is never an authority for this value.
        if not isinstance(command_marker, str) or not command_marker.strip():
            return False
        return (result.returncode==0 and command.strip()==command_marker.strip()
                and db_token in command)
    return result.returncode==0 and ' -m martin_agentic live-session ' in command and db_token in command


def watchdog_process_matches(pid,db_path,policy_path,status_file=None):
    if type(pid) is not int or pid<=1:return False
    try:
        result=subprocess.run(['ps','-p',str(pid),'-o','command='],capture_output=True,text=True,timeout=3)
    except (OSError,subprocess.TimeoutExpired):return False
    command=result.stdout.strip()+' '
    return (result.returncode==0 and ' -m martin_agentic watchdog ' in command
        and '--db '+str(Path(db_path).resolve())+' ' in command
        and '--policy '+str(Path(policy_path).resolve())+' ' in command
        and (status_file is None or '--status-file '+str(Path(status_file).resolve())+' ' in command))


def _inhibit_failed_start(db,reason):
    target=Path(str(db)+'.entry-inhibit.json')
    value={'db':str(db),'blocked':True,'at':datetime.now(timezone.utc).isoformat(),
        'issues':[reason],'clearing_requires':'EXPLICIT_RECONCILED_OPERATOR_DISPOSITION'}
    temp=target.with_name(target.name+'.'+uuid.uuid4().hex+'.tmp')
    temp.write_text(json.dumps(value,indent=2)+'\n');temp.chmod(0o600);temp.replace(target)


def _stop_created_control(process):
    """Only the Popen handle created by this start; never signal a borrowed PID."""
    if process.poll() is not None:return True
    try:process.send_signal(signal.SIGINT)
    except ProcessLookupError:return True
    try:process.wait(timeout=3);return True
    except subprocess.TimeoutExpired:return False


def _stop_watchdog(pid,db,policy,status_file):
    if not watchdog_process_matches(pid,db,policy,status_file):return {'stopped':False,'signals_sent':0,'reason':'WATCHDOG_OWNER_UNVERIFIED'}
    try:os.kill(pid,signal.SIGTERM)
    except ProcessLookupError:return {'stopped':True,'signals_sent':0}
    for _ in range(25):
        if not watchdog_process_matches(pid,db,policy,status_file):return {'stopped':True,'signals_sent':1}
        time.sleep(.2)
    return {'stopped':False,'signals_sent':1,'reason':'WATCHDOG_STILL_RUNNING'}


def runtime_lock_held(db_path):
    """Inspect the supervisor's real flock independently of disposable metadata.

    A held lock proves another owner exists, not that a particular PID or broker
    session is healthy. Missing/unreadable owner metadata cannot permit a start.
    """
    with open(str(db_path)+'.lock','a',encoding='utf-8') as handle:
        try:fcntl.flock(handle,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:return True
        fcntl.flock(handle,fcntl.LOCK_UN)
        return False


def session_control(mode,db_path,policy_path,*,symbols=None,status_file='runtime/live-status.json',
                    metadata_file='runtime/live-daemon-control.json',agent_mode='review',
                    worker_adapter='none',strategy='martin',strategy_plugins=None,child_command=None):
    db=Path(db_path).resolve();policy=Path(policy_path).resolve();status_path=Path(status_file).resolve()
    metadata=Path(metadata_file).resolve();metadata.parent.mkdir(parents=True,exist_ok=True)
    if mode not in {'start','status','stop'} or agent_mode not in {'disabled','review','script-review','supervised-host'}:
        raise ValueError('INVALID_SESSION_CONTROL_MODE')
    if worker_adapter not in {'none',LOCAL_WORKER_ADAPTER}:
        raise ValueError('INVALID_WORKER_ADAPTER_SELECTION')
    if worker_adapter==LOCAL_WORKER_ADAPTER and agent_mode!='supervised-host':
        raise ValueError('LOCAL_WORKER_ADAPTER_REQUIRES_SUPERVISED_HOST')
    with single_instance(str(db)+'.control'):
        record=json.loads(metadata.read_text()) if metadata.exists() else {}
        command_marker=(' '.join(str(part) for part in child_command)
                        if child_command is not None else None)
        status=json.loads(status_path.read_text()) if status_path.exists() else {}
        watchdog_file=Path(str(db)+'.watchdog.json')
        try:watchdog_status=json.loads(watchdog_file.read_text())
        except (OSError,ValueError):watchdog_status={}
        if not isinstance(watchdog_status,dict):watchdog_status={}
        watchdog_candidates=list(dict.fromkeys(p for p in (record.get('watchdog_pid'),watchdog_status.get('pid')) if type(p) is int))
        watchdog_matches=[p for p in watchdog_candidates if watchdog_process_matches(p,db,policy,status_path)]
        if len(watchdog_matches)>1:raise ValueError('MULTIPLE_WATCHDOG_PROCESSES_REQUIRE_RECONCILIATION')
        watchdog_pid=watchdog_matches[0] if watchdog_matches else None
        watchdog_running=bool(watchdog_matches)
        watchdog_lock=runtime_lock_held(str(db)+'.watchdog')
        candidates=list(dict.fromkeys(p for p in (record.get('pid'),status.get('pid')) if type(p) is int))
        recorded_strategy=record.get('strategy')
        recorded_plugins=record.get('strategy_plugins', [])
        match_kwargs={'worker_adapter':worker_adapter}
        if recorded_strategy is not None:
            match_kwargs.update(strategy=recorded_strategy,
                                strategy_plugins=recorded_plugins)
        matches=[p for p in candidates if process_matches(
            p,db,command_marker,**match_kwargs)]
        if len(matches)>1:raise ValueError('MULTIPLE_BOUND_PROCESSES_REQUIRE_RECONCILIATION')
        candidate=matches[0] if matches else record.get('pid') or status.get('pid')
        running=bool(matches)
        control_guarded=running and (record.get('require_watchdog') is True or
            status.get('pid')==candidate and status.get('watchdog_required') is True)
        lock_held=runtime_lock_held(db)
        if record and record.get('db')!=str(db):raise ValueError('CONTROLLER_DATABASE_MISMATCH')
        if record.get('strategy') is not None:
            if record.get('strategy') != strategy:
                raise ValueError('CONTROLLER_COMPOSITION_MISMATCH')
            if tuple(record.get('strategy_plugins') or ()) != tuple(strategy_plugins or ()):
                raise ValueError('CONTROLLER_COMPOSITION_MISMATCH')
        if lock_held and not running:
            # The process still owns the database even if status/control files
            # vanished or their PIDs are stale. Never overwrite its ownership
            # record, spawn a competing child, or claim a successful stop.
            # If a live module/db owner exists but its adapter token differs,
            # return that specific refusal instead of a generic unknown owner.
            untyped_matches=[p for p in candidates
                             if process_matches(p,db,command_marker,worker_adapter=None)]
            adapter_mismatch=bool(untyped_matches)
            result={'process_running':None,'runtime_lock_held':True,'owner_verified':False,
                'pid':untyped_matches[0] if adapter_mismatch else None,'safe_to_start':False,
                'attention_required':True,
                'reason':('WORKER_ADAPTER_MISMATCH' if adapter_mismatch
                          else 'RUNTIME_LOCK_HELD_OWNER_UNVERIFIED'),'runtime':status,
                'worker_adapter':worker_adapter,
                'worker_adapter_capability':worker_adapter_selection_report(worker_adapter),
                'worker_adapter_recorded':record.get('worker_adapter'),
                'strategy':strategy,'strategy_plugins':tuple(strategy_plugins or ()),
                'worker_adapter_matches':False,
                'worker_adapter_mismatch':adapter_mismatch,
                'launchd_qualified':False,'live_order_submission_enabled':False}
            if mode=='start':result.update(started=False,already_running=None)
            elif mode=='stop':result.update(stopped=False,signals_sent=0)
            return result
        if mode=='status':
            return {'process_running':running,'pid':candidate,'runtime':status,
                'watchdog_running':watchdog_running,'watchdog_pid':watchdog_pid,
                'watchdog_runtime':watchdog_status,'watchdog_lock_held':watchdog_lock,
                'paired_processes_verified':running and watchdog_running,
                'control_requires_watchdog':bool(control_guarded),
                'health_verified':False,
                'runtime_lock_held':lock_held,'owner_verified':running,
                'safe_to_start':not running and not lock_held and (not watchdog_lock or watchdog_running),
                'attention_required':bool(watchdog_lock and not watchdog_running),
                'current_build_matches':status.get('source_fingerprint')==source_fingerprint(),
                'worker_adapter':worker_adapter,
                'worker_adapter_capability':worker_adapter_selection_report(worker_adapter),
                'worker_adapter_recorded':record.get('worker_adapter',status.get('worker_adapter')),
                'strategy':recorded_strategy,'strategy_plugins':tuple(recorded_plugins),
                'worker_adapter_matches':running,
                'launchd_qualified':False,'live_order_submission_enabled':False}
        if mode=='stop':
            signals_sent=0
            if running:
                os.kill(candidate,signal.SIGINT);signals_sent=1
                for _ in range(50):
                    if not process_matches(candidate,db,command_marker):break
                    time.sleep(.2)
                else:return {'stopped':False,'pid':candidate,'reason':'STILL_RUNNING_DO_NOT_START_SECOND_WORKER',
                    'watchdog_preserved':True,'signals_sent':signals_sent}
            # Keep independent observation when stopped management has not proven
            # final flatness. Explicit stop never removes the last observer of an
            # unresolved position, order, native intent or unknown worker outcome.
            try:final_status=json.loads(status_path.read_text()) if status_path.exists() else {}
            except (OSError,ValueError):final_status={}
            try:
                final_watchdog=json.loads(watchdog_file.read_text())
                close_age=(datetime.now(timezone.utc)-datetime.fromisoformat(final_watchdog['checked_at'].replace('Z','+00:00'))).total_seconds()
            except (OSError,ValueError,TypeError,KeyError,AttributeError):final_watchdog={};close_age=float('inf')
            final_safe=(isinstance(final_status,dict) and final_status.get('db')==str(db)
                and final_status.get('reason')=='AUTHORIZED_SESSION_CLOSED' and final_status.get('final_flat_verified') is True
                and final_watchdog.get('db')==str(db) and final_watchdog.get('pid')==watchdog_pid
                and final_watchdog.get('status_file')==str(status_path)
                and final_watchdog.get('closed_safe') is True and final_watchdog.get('issues')==[]
                and final_watchdog.get('control_status_observed_at')==final_status.get('observed_at')
                and final_watchdog.get('control_status_reason')==final_status.get('reason') and 0<=close_age<=15)
            if watchdog_running and not final_safe:
                return {'stopped':True,'pid':candidate,'signals_sent':signals_sent,'watchdog_stopped':False,
                    'watchdog_pid':watchdog_pid,'watchdog_preserved':True,'attention_required':True,
                    'reason':'WATCHDOG_PRESERVED_UNTIL_FINAL_FLAT_VERIFIED'}
            watchdog_stop=_stop_watchdog(watchdog_pid,db,policy,status_path) if watchdog_running else {'stopped':not watchdog_lock,'signals_sent':0}
            return {'stopped':True,'pid':candidate,'reason':'CONTROL_STOPPED' if running else 'NO_MATCHING_PROCESS',
                'signals_sent':signals_sent,'watchdog_stopped':watchdog_stop['stopped'],
                'watchdog_signals_sent':watchdog_stop['signals_sent'],
                'attention_required':not watchdog_stop['stopped']}
        if watchdog_lock and not watchdog_running:
            _inhibit_failed_start(db,'WATCHDOG_LOCK_HELD_OWNER_UNVERIFIED')
            return {'started':False,'already_running':running,'pid':candidate,
                'worker_adapter':worker_adapter,'health_verified':False,
                'watchdog_running':None,'attention_required':True,'reason':'WATCHDOG_LOCK_HELD_OWNER_UNVERIFIED'}
        if running and watchdog_running:
            if not control_guarded:_inhibit_failed_start(db,'UNMANAGED_CONTROL_RESTART_REQUIRED_FOR_WATCHDOG_GUARD')
            return {'started':False,'already_running':True,'pid':candidate,
                'worker_adapter':worker_adapter,'watchdog_running':True,
                'watchdog_pid':watchdog_pid,'paired_processes_verified':True,'health_verified':False,
                'requires_control_restart_for_guard':not control_guarded,'attention_required':not control_guarded,
                'current_build_matches':status.get('source_fingerprint')==source_fingerprint()}
        if not running and (symbols is None or not 10<=len(symbols)<=30 or len(symbols)!=len(set(symbols))):
            raise ValueError('EXPLICIT_UNIQUE_STOCK_POOL_REQUIRED')
        root=candidate_root()
        log=metadata.parent/'live-daemon.log';error=metadata.parent/'live-daemon.stderr'
        created_control=None
        if not running:
            # A 15-second idle sleep plus broker/market collection yielded
            # 22-second quote gaps in real supervised-host observation. Keep
            # model-free readers on the supported five-second control tick;
            # freshness checks and all execution authority remain unchanged.
            control_poll='5' if agent_mode in {'disabled','script-review','supervised-host'} else '15'
            args=list(child_command) if child_command is not None else [
                sys.executable,'-m','martin_agentic','live-session','--db',str(db),'--policy',str(policy),
                '--symbols',*symbols,'--cycles','10000','--until-close','--poll-seconds',control_poll,'--agent-mode',agent_mode,
                '--worker-adapter',worker_adapter,'--strategy',strategy,
                *([item for plugin in (strategy_plugins or ())
                   for item in ('--strategy-plugin', plugin)]),
                '--status-file',str(status_path),'--output-dir',str(metadata.parent/'live-agent'),'--require-watchdog']
            if (child_command is None and worker_adapter=='none'
                    and agent_mode in {'disabled','script-review','supervised-host'}):
                # These modes create no model worker. Preserve a zero budget
                # established by bounded preflight instead of requesting the
                # CLI's larger model defaults on restart.
                args+=['--budget-entry-limit','0','--budget-total-limit','0']
            try:
                with log.open('a') as stdout,error.open('a') as stderr:
                    created_control=subprocess.Popen(args,stdin=subprocess.DEVNULL,stdout=stdout,stderr=stderr,
                        cwd=root,start_new_session=True)
            except OSError:
                _inhibit_failed_start(db,'CONTROL_CHILD_SPAWN_FAILED')
                return {'started':False,'health_verified':False,
                    'worker_adapter':worker_adapter,
                    'reason':'CONTROL_CHILD_SPAWN_FAILED',
                    'new_entries_blocked':True,'attention_required':True}
            exit_code=created_control.poll();candidate=created_control.pid
            if exit_code is not None:
                _inhibit_failed_start(db,'CONTROL_CHILD_EXITED_BEFORE_HEALTH_VERIFICATION')
                return {'started':False,'pid':candidate,'exit_code':exit_code,
                    'worker_adapter':worker_adapter,'health_verified':False,
                    'reason':'CHILD_EXITED_BEFORE_HEALTH_VERIFICATION','attention_required':True}
        record={'pid':candidate,'db':str(db),'started_at':datetime.now(timezone.utc).isoformat(),
            'source_fingerprint':source_fingerprint(),'agent_mode':record.get('agent_mode',agent_mode) if running else agent_mode,
            'worker_adapter':record.get('worker_adapter',worker_adapter) if running else worker_adapter,
            'strategy':record.get('strategy',strategy) if running else strategy,
            'strategy_plugins':tuple(record.get('strategy_plugins',strategy_plugins or ()) if running else (strategy_plugins or ())),
            'run_until_authorized_close':created_control is not None or status.get('session_run_mode')=='until_authorized_close',
            'launchd_qualified':False,'watchdog_pid':watchdog_pid,'watchdog_policy':str(policy),
            'require_watchdog':created_control is not None or bool(control_guarded)}
        failure=None
        if not watchdog_running:
            args=[sys.executable,'-m','martin_agentic','watchdog','--db',str(db),'--policy',str(policy),
                '--status-file',str(status_path),'--cycles','100000','--poll-seconds','5',
                '--stop-on-safe-close','--expected-control-pid',str(candidate)]
            try:
                with (metadata.parent/'live-watchdog.log').open('a') as stdout,(metadata.parent/'live-watchdog.stderr').open('a') as stderr:
                    watchdog=subprocess.Popen(args,stdin=subprocess.DEVNULL,stdout=stdout,stderr=stderr,
                        cwd=root,start_new_session=True)
                watchdog_pid=watchdog.pid
                if watchdog.poll() is not None:failure='WATCHDOG_CHILD_EXITED_BEFORE_HEALTH_VERIFICATION'
                else:record['watchdog_pid']=watchdog_pid
            except OSError:failure='WATCHDOG_CHILD_SPAWN_FAILED'
        if failure:
            try:_inhibit_failed_start(db,failure);gate_written=True
            except OSError:gate_written=False
            stopped=_stop_created_control(created_control) if created_control is not None else False
            record.update(pairing_failed=True,watchdog_failure=failure,created_control_stopped=stopped)
        temp=metadata.with_suffix('.tmp');temp.write_text(json.dumps(record,indent=2)+'\n');temp.chmod(0o600);temp.replace(metadata)
        (metadata.parent/'live-daemon.pid').write_text(str(candidate)+'\n')
        if failure:return {'started':False,'already_running':running,'pid':candidate,
            'worker_adapter':worker_adapter,'health_verified':False,
            'reason':failure,'attention_required':True,'new_entries_blocked':gate_written or stopped,
            'entry_inhibit_written':gate_written,
            'created_control_stopped':record['created_control_stopped'],'watchdog_running':False}
        if running and not control_guarded:
            _inhibit_failed_start(db,'UNMANAGED_CONTROL_RESTART_REQUIRED_FOR_WATCHDOG_GUARD')
        return {'started':created_control is not None,'already_running':running,'pid':candidate,
                'worker_adapter':worker_adapter,
                'worker_adapter_capability':worker_adapter_selection_report(worker_adapter),
                'watchdog_pid':watchdog_pid,'watchdog_start_requested':not watchdog_running,
                'requires_control_restart_for_guard':running and not control_guarded,
                'attention_required':running and not control_guarded,
                'health_verified':False,'mode':record['agent_mode'],
                'note':'Process start requests only. Verify fresh control and independent watchdog status; sticky inhibits need audited disposition. No broker write mode enabled.'}
