"""Owner-thread reservations/imports, background formal review I/O, zero models."""
from copy import deepcopy
from datetime import datetime,timezone,timedelta
import json
from pathlib import Path
import threading
import time

from .audit import digest
from .control_tasks import BackgroundTask,ReadTransportOwner,owned_read_transport,worker_lease
from .ledger import timestamp
from .native_operations import NativeOperations
from .native_review import EquityReviewTransport,persist_review_receipt
from .qualification import source_fingerprint
from .transport import safe_read_failure


def _review_io(factory,packet,binding,directory,cancellation,deadline,_lease_keepalive):
    """Detached data only. The owner retains the lease through receipt import."""
    try:
        with owned_read_transport(factory,cancellation,deadline_monotonic=deadline) as transport:
            if cancellation.stopped:raise RuntimeError('READ_ONLY_COLLECTION_STOPPED')
            if time.monotonic()>=deadline:raise RuntimeError('REVIEW_INPUT_EXPIRED_BEFORE_CALL')
            response=transport.execute_review(packet)
    except Exception as exc:
        code=str(exc) if str(exc) in {'READ_ONLY_COLLECTION_STOPPED','REVIEW_INPUT_EXPIRED_BEFORE_CALL'} else safe_read_failure(exc)['code']
        response={'isError':True,'content':[{'type':'text','text':code}]}
    path=persist_review_receipt(packet,response,binding,directory,datetime.now(timezone.utc).isoformat())
    return {'receipt_file':path}


class AsyncNativeReviewDispatcher:
    requires_observation=True

    def __init__(self,book,directory,*,transport_factory=None):
        self.book=book;self.directory=Path(directory).resolve()
        self.factory=transport_factory or EquityReviewTransport
        self.task=BackgroundTask('deterministic-native-review');self.pending=None
        self.owner_thread=threading.get_ident();self.stopped=False;self.recovery_failure=None;self.reconciliation_notice=None
        self.build=source_fingerprint()

    def _owner(self):
        if threading.get_ident()!=self.owner_thread:raise ValueError('NATIVE_REVIEW_OWNER_THREAD_REQUIRED')

    def _import(self,ready):
        pending=self.pending;packet=pending['packet'];ok,value=ready
        failure=None
        try:
            if not ok:raise ValueError('REVIEW_RECEIPT_PERSISTENCE_FAILED')
            expected=self.directory/(packet['operation_id']+'.json')
            if Path(value['receipt_file'])!=expected:raise ValueError('REVIEW_RECEIPT_PATH_MISMATCH')
            with expected.open('rb') as handle:raw=handle.read(2_000_001)
            if len(raw)>2_000_000:raise ValueError('REVIEW_RECEIPT_TOO_LARGE')
            receipt=json.loads(raw)
            if (receipt['binding']!=pending['binding'] or receipt['packet']!=packet
                    or receipt['packet_sha256']!=digest(packet)
                    or receipt['response_sha256']!=digest(receipt['response'])):
                raise ValueError('REVIEW_RECEIPT_BINDING_INVALID')
            response=receipt['response']
        except (ValueError,KeyError,TypeError,OSError):
            failure='REVIEW_RECEIPT_UNAVAILABLE_OR_INVALID'
            response={'isError':True,'content':[{'type':'text','text':failure}]}
        try:
            result=NativeOperations(self.book).record(packet['operation_id'],response)
        except Exception:
            # Leave the exact native intent unresolved; never claim recovery.
            self.stopped=True
            self.recovery_failure={'agent_called':False,'status':'REVIEW_IMPORT_REQUIRES_RECOVERY',
                'operation_id':packet['operation_id'],'attention_required':True,'model_calls':0}
            return dict(self.recovery_failure)
        finally:
            pending['lease'].close();self.pending=None
        return {'agent_called':False,'status':'WAITING_USER' if result.get('review_verified') else 'REVIEW_FAILED',
            'action_id':packet['action_id'],'operation_id':packet['operation_id'],'parameters':packet['parameters'],
            'receipt_file':str(expected) if ok else None,
            'native_result':result,'receipt_failure':failure,'model_calls':0,'broker_writes':0,
            'attention_required':True,'execution_authorized':False}

    def _priority(self,action):
        return ({'protect':0,'cancel':1,'exit':2,'entry':3}.get(action['kind'],4),
            action['created_at'],action['id'])

    def _next_waiting(self,actions):
        return next(iter(sorted((a for a in actions if a['state']=='WAITING_USER'),
            key=self._priority)),None)

    def protection_deadline(self,action):
        if action['kind']!='protect':return None
        trade=self.book.state()['trades'][action['trade_id']]
        anchor=trade.get('unprotected_since') or trade.get('first_fill_at')
        return timestamp(anchor)+timedelta(seconds=30) if anchor else None

    def _waiting(self,action):
        payload=json.loads(action['payload']);now=self.book.clock()
        result={'agent_called':False,'status':'WAITING_USER','action_id':action['id'],
            'parameters':payload['parameters'],'attention_required':True,
            'execution_authorized':False,'model_calls':0,'broker_writes':0}
        expires=timestamp(payload['expires_at'])
        deadline=self.protection_deadline(action)
        if deadline:
            expires=min(expires,deadline)
            result.update(protection_deadline=deadline.isoformat(),
                protection_seconds_remaining=max(0,(deadline-now).total_seconds()))
        if self.book.db.execute("SELECT 1 FROM sqlite_master WHERE name='native_operation_records' AND type='table'").fetchone():
            # The latest attempt invalidates every older preview, including when
            # that latest attempt was abandoned without any broker response.
            row=self.book.db.execute("SELECT id,result,state,created_at FROM native_operation_records WHERE action_id=? AND operation='review' ORDER BY rowid DESC LIMIT 1",(action['id'],)).fetchone()
            if row:
                result.update(operation_id=row[0],review_state=row[2])
                if row[2]=='RECORDED':
                    native_result=json.loads(row[1]);result['native_result']=native_result
                    expires=min(expires,timestamp(row[3])+timedelta(seconds=30))
                    if not native_result.get('review_verified'):result['status']='REVIEW_FAILED'
                else:
                    result.update(status='FRESH_REVIEW_REQUIRED',review_verified=False)
        result['review_valid_until']=expires.isoformat()
        if now>=expires:result.update(status='FRESH_REVIEW_REQUIRED',review_verified=False)
        return result

    def _core_reconciliation(self,actions):
        unresolved=[a['id'] for a in actions if a['state'] in {'RUNNING','UNKNOWN'}]
        self.reconciliation_notice=({'agent_called':False,'status':'NATIVE_RECONCILIATION_REQUIRED',
            'unresolved_action_ids':unresolved,'attention_required':True,'execution_authorized':False,
            'model_calls':0,'broker_writes':0} if unresolved else None)
        return self.reconciliation_notice

    def __call__(self,*,snapshot=None,quotes=None,allow_entries=False,allow_dispatch=True):
        self._owner()
        ready=self.task.take()
        if ready is not None:return self._import(ready)
        if self.pending:return {'agent_called':False,'status':'REVIEW_IN_FLIGHT','model_calls':0}
        if self.stopped:
            return dict(self.recovery_failure) if self.recovery_failure else {'agent_called':False,'status':'NO_REVIEW_DISPATCH','model_calls':0}
        if not allow_dispatch or snapshot is None:
            retired=set(self.book.recovery_status(snapshot)['acknowledged_action_ids'])
            active=[a for a in self.book.actions() if a['id'] not in retired]
            reconciliation=self._core_reconciliation(active)
            if reconciliation:return dict(reconciliation)
            waiting=self._next_waiting(active)
            return self._waiting(waiting) if waiting else {'agent_called':False,'status':'NO_REVIEW_DISPATCH','model_calls':0}
        if (type(getattr(self.factory,'simulation_only',None)) is not bool
                or self.book.simulation is not self.factory.simulation_only
                or snapshot.get('synthetic') is not self.book.simulation
                or not (str(self.book.policy.get('account_suffix', '')).isdigit() and len(str(self.book.policy.get('account_suffix', ''))) == 4) or self.book.policy.get('capture_only') is True):
            return {'agent_called':False,'status':'REVIEW_SCOPE_DENIED','model_calls':0}
        retired=set(self.book.recovery_status(snapshot)['acknowledged_action_ids'])
        active=[a for a in self.book.actions() if a['id'] not in retired]
        reconciliation=self._core_reconciliation(active)
        if reconciliation:return dict(reconciliation)
        actions=[a for a in active if a['state']=='READY' and (allow_entries or a['kind']!='entry')]
        actions.sort(key=self._priority)
        if not actions:
            waiting=self._next_waiting(active)
            return self._waiting(waiting) if waiting else {'agent_called':False,'status':'NO_READY_REVIEW_ACTION','model_calls':0}
        action=actions[0]
        if action['kind']=='cancel':return {'agent_called':False,'status':'CANCEL_REQUIRES_NATIVE_CONFIRMATION',
            'action_id':action['id'],'attention_required':True,'model_calls':0}
        path=self.book.db.execute('PRAGMA database_list').fetchone()[2]
        lease=worker_lease(path) if path else None
        if lease is None:return {'agent_called':False,'status':'WORKER_OWNER_STILL_ACTIVE','model_calls':0}
        packet=None
        try:
            payload=json.loads(action['payload']);quote=(quotes or {}).get(payload['plan']['symbol'])
            now=self.book.clock()
            expires=min(timestamp(payload['expires_at']),timestamp(snapshot['observed_at'])+timedelta(seconds=30),
                *(timestamp(quote[key])+timedelta(seconds=15) for key in ('bid_at','ask_at','observed_at'))) if quote else now
            protection_deadline=self.protection_deadline(action)
            if protection_deadline:expires=min(expires,protection_deadline)
            ttl=min(10,(expires-now).total_seconds())
            if ttl<=0:raise ValueError('NATIVE_REVIEW_FRESH_INPUT_REQUIRED')
            deadline=time.monotonic()+ttl
            packet=NativeOperations(self.book).begin(action['id'],'review',snapshot,quote=quote)
            binding={'db':str(Path(path).resolve()),'account_key':snapshot['account_key'],
                'policy_sha256':digest(self.book.policy),'synthetic':self.book.simulation,'source_fingerprint':self.build}
            cancellation=ReadTransportOwner()
            self.pending={'packet':packet,'binding':binding,'lease':lease,'cancellation':cancellation}
            self.task.start(_review_io,self.factory,deepcopy(packet),deepcopy(binding),self.directory,cancellation,deadline,lease)
        except Exception:
            if packet is not None:
                try:NativeOperations(self.book).record(packet['operation_id'],{'isError':True,'content':[
                    {'type':'text','text':'REVIEW_TASK_START_FAILED_NO_ORDER_SUBMITTED'}]})
                except Exception:
                    self.stopped=True
                    self.recovery_failure={'agent_called':False,'status':'REVIEW_IMPORT_REQUIRES_RECOVERY',
                        'operation_id':packet['operation_id'],'attention_required':True,'model_calls':0}
            self.pending=None;lease.close()
            return {'agent_called':False,'status':'CORE_REVIEW_NOT_DISPATCHED','model_calls':0,
                'operation_id':packet['operation_id'] if packet else None,'attention_required':True}
        return {'agent_called':False,'status':'SCRIPT_REVIEW_STARTED','action_id':action['id'],
            'operation_id':packet['operation_id'],'model_calls':0,'broker_writes':0,'execution_authorized':False}

    def close(self):
        self._owner();self.stopped=True;failure=False
        if self.pending:
            try:self.pending['cancellation'].cancel()
            except Exception:failure=True
            if self.task.thread is not None:self.task.thread.join(timeout=.25)
            ready=self.task.take()
            imported=self._import(ready) if ready is not None else None
        else:imported=None
        return {**self.task.status(),'receipt_import':imported,'model_calls':0,
            'pending_operation_id':self.pending['packet']['operation_id'] if self.pending else None,
            'recovery_failure':self.recovery_failure,
            'reconciliation_notice':self.reconciliation_notice,
            'attention_required':failure or self.pending is not None or bool(self.recovery_failure) or bool(self.reconciliation_notice) or bool(imported and imported.get('attention_required'))}
