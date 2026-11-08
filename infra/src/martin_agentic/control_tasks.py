"""Bounded read/compute tasks. SQLite is exclusively owned by the control loop."""
import copy
import hashlib
import fcntl
import inspect
import json
import os
from pathlib import Path
import queue
import threading
import time
import uuid
from contextlib import contextmanager


class ReadTransportOwner:
    """Shutdown ownership of a read-only RPC process, including startup races.

    The owner is the identity anchor for the RPC process, its pipes and its
    reader threads.  Cancellation records the absolute deadline before any
    close begins, and late registration is closed under the same deadline.
    A legacy close without the deadline contract runs in a named, tracked
    thread; if it does not finish in budget the owner reports incomplete
    instead of silently blocking the control loop.
    """
    def __init__(self):
        self.lock=threading.Lock();self.stopped=False;self.rpc=None
        self.deadline_monotonic=None;self.rpc_cleanup=None
        self.late_cleanup_status=None;self.cancel_thread_ids=[]

    def _close_rpc(self,rpc,deadline_monotonic):
        try:
            parameters=inspect.signature(rpc.close).parameters
        except (TypeError,ValueError):
            parameters={}
        if 'deadline_monotonic' in parameters:
            return rpc.close(deadline_monotonic=deadline_monotonic)
        deadline=(deadline_monotonic if deadline_monotonic is not None
                  else time.monotonic()+3)
        holder={}
        def legacy_close():
            try:holder['result']=rpc.close()
            except BaseException as exc:holder['error']=type(exc).__name__
            finally:holder['done']=True
        thread=threading.Thread(target=legacy_close,name='legacy-rpc-close',
                                daemon=True)
        thread.start()
        with self.lock:self.cancel_thread_ids.append(thread.ident)
        thread.join(max(0.0,deadline-time.monotonic()))
        if thread.is_alive():
            return {'complete':False,'legacy_close_thread_alive':True,
                    'cancel_thread_id':thread.ident,
                    'deadline_exhausted':True}
        if holder.get('error'):
            return {'complete':False,'legacy_close_error':holder['error']}
        return holder.get('result') or {'complete':True,'legacy_close':True}

    def attach(self,rpc):
        with self.lock:
            stopped=self.stopped;deadline=self.deadline_monotonic
            if not stopped:self.rpc=rpc
        if stopped:
            try:status=self._close_rpc(rpc,deadline)
            except BaseException as exc:
                status={'complete':False,
                        'late_registration_error':type(exc).__name__}
            with self.lock:self.late_cleanup_status=status
            raise RuntimeError('READ_ONLY_COLLECTION_STOPPED')

    def detach(self,rpc):
        with self.lock:
            if self.rpc is rpc:self.rpc=None

    def cancel(self, *, deadline_monotonic=None):
        with self.lock:
            self.stopped=True
            if deadline_monotonic is not None:
                self.deadline_monotonic=deadline_monotonic
            deadline=self.deadline_monotonic
            rpc=self.rpc
        result=None
        if rpc is not None:
            result=self._close_rpc(rpc,deadline)
            with self.lock:self.rpc_cleanup=result
        with self.lock:late=self.late_cleanup_status
        return {'rpc_cleanup':result,
                'deadline_monotonic':deadline,
                'late_registration':late}

    def cleanup_status(self):
        with self.lock:
            return {'rpc_cleanup':self.rpc_cleanup,
                    'late_registration':self.late_cleanup_status,
                    'cancel_thread_ids':list(self.cancel_thread_ids),
                    'deadline_monotonic':self.deadline_monotonic}


@contextmanager
def owned_read_transport(factory,owner=None,*,close_deadline_monotonic=None,
                         **kwargs):
    """Register a read process before initialization; unregister after cleanup.

    Passing ``close_deadline_monotonic`` makes the existing
    ``ReadTransportOwner`` perform the one close with the same absolute
    deadline, retaining its verifiable cleanup status.  Callers that need
    owner cleanup results must inspect ``owner.cleanup_status()`` after exit.
    """
    if owner is not None and owner.stopped:
        raise RuntimeError('READ_ONLY_COLLECTION_STOPPED')
    transport=factory(**kwargs)
    try:
        if owner is not None:owner.attach(transport.rpc)
        if owner is not None and close_deadline_monotonic is not None:
            # Establish the shutdown contract before the first possible
            # __exit__/close; subsequent idempotent closes may repeat but must
            # not reset the absolute deadline.
            try:transport.shutdown_deadline_monotonic=close_deadline_monotonic
            except (AttributeError,TypeError):pass
            try:transport.rpc.cleanup_deadline_monotonic=close_deadline_monotonic
            except (AttributeError,TypeError):pass
        with transport:
            yield transport
    finally:
        # attach hooks and __enter__ may fail before Python invokes __exit__.
        # StdioRpc.close is idempotent; retain ownership until cleanup finishes.
        try:
            if owner is not None and close_deadline_monotonic is not None:
                owner.cancel(deadline_monotonic=close_deadline_monotonic)
            else:
                transport.rpc.close()
        finally:
            if owner is not None:owner.detach(transport.rpc)


class BackgroundTask:
    """One in-flight task, one result; no SQLite connection enters the thread."""
    def __init__(self,name):
        self.name=name;self.thread=None;self.started=None;self.mailbox=queue.Queue(maxsize=1)

    def start(self,fn,*args,**kwargs):
        if self.thread is not None:raise ValueError('BACKGROUND_TASK_ALREADY_OWNED')
        self.started=time.monotonic()
        def run():
            try:result=(True,fn(*args,**kwargs))
            except Exception as exc:result=(False,exc)
            self.mailbox.put_nowait(result)
        self.thread=threading.Thread(target=run,name=self.name,daemon=True)
        try:self.thread.start()
        except BaseException:
            self.thread=None;self.started=None
            raise

    def take(self):
        try:result=self.mailbox.get_nowait()
        except queue.Empty:return None
        self.thread.join();self.thread=None;self.started=None
        return result

    def status(self):
        return {'running':self.thread is not None,'thread_alive':bool(self.thread and self.thread.is_alive()),
            'age_seconds':None if self.started is None else time.monotonic()-self.started}


class FrozenSignals:
    """Detached collection input. Strategies are consumed only by the main owner.

    ``view`` carries the per-consumer collection frontier when the session runs
    more than one instance. It is built on the owner thread and is pure data;
    the background task never sees a connection, a strategy object or a clock.
    """
    def __init__(self,state,view=None,state_view=None):
        self.value=copy.deepcopy(state);self.view=view;self.state_view=state_view
    def state(self):return copy.deepcopy(self.value)
    def collection_view(self):
        if self.view is not None:return self.view
        from .collection_view import CollectionView
        return CollectionView.legacy_view(self.value)
    def collection_state(self):
        """Day-preparedness metadata for the collector, detached like the rest."""
        if self.state_view is not None:return self.state_view
        from .collection_view import CollectionState
        return CollectionState.legacy_state_view(self.value)


def persist_worker_result(directory,action,worker,*,binding=None):
    """Durably preserve the actual worker response before the owner consumes it.

    On crash, RUNNING/UNKNOWN stays fail-closed; this receipt is evidence for explicit
    reconciliation, never automatic replay authorization.
    """
    try:report=worker(action)
    except Exception:report={'status':'unknown','message':'WORKER_FAILURE_RECONCILE_BEFORE_RETRY'}
    folder=Path(directory)/'mailbox';folder.mkdir(parents=True,exist_ok=True);folder.chmod(0o700)
    path=folder/(uuid.uuid4().hex+'.json');temp=path.with_suffix('.tmp')
    with temp.open('x') as handle:
        os.chmod(temp,0o600);json.dump({'binding':binding,'action_id':action['action_id'],'action_sha256':hashlib.sha256(json.dumps(action,sort_keys=True,separators=(',',':')).encode()).hexdigest(),'report':report},handle)
        handle.flush();os.fsync(handle.fileno())
    temp.replace(path)
    descriptor=os.open(folder,os.O_RDONLY)
    try:os.fsync(descriptor)
    finally:os.close(descriptor)
    return {'binding':binding,'action_id':action['action_id'],'report':report,'receipt_file':str(path),'action_sha256':hashlib.sha256(json.dumps(action,sort_keys=True,separators=(',',':')).encode()).hexdigest()}


def worker_lease(path):
    """Nonblocking lease held through worker termination and receipt fsync."""
    handle=open(str(path)+'.worker.lock','a',encoding='utf-8')
    os.chmod(handle.name,0o600)
    try:fcntl.flock(handle,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError:handle.close();return None
    return handle


def run_leased_worker(lease,directory,action,worker,binding):
    try:return persist_worker_result(directory,action,worker,binding=binding)
    finally:lease.close()


def inspect_worker_receipts(book,directory,*,limit=1000):
    """Read-only receipt inventory for explicit UNKNOWN reconciliation.

    A valid model receipt cannot prove placement/fill or clear an action. No
    action, risk, budget or audit history is rewritten by this inspection.
    """
    if type(limit) is not int or not 1<=limit<=10000:raise ValueError('INVALID_RECEIPT_INSPECTION_LIMIT')
    folder=Path(directory)/'mailbox';paths=sorted(folder.glob('*.json')) if folder.exists() else []
    if len(paths)>limit:raise ValueError('WORKER_RECEIPT_INSPECTION_LIMIT_EXCEEDED')
    actions={a['id']:a for a in book.actions()};state=book.state()
    db_path=book.db.execute('PRAGMA database_list').fetchone()[2]
    binding={'db':str(Path(db_path).resolve()),'account_key':state.get('account_key')}
    rows=[]
    for path in paths:
        row={'receipt_file':str(path),'valid_binding':False,'broker_verified':False,'automatic_replay_allowed':False}
        try:
            value=json.loads(path.read_text());action=actions.get(value.get('action_id'))
            if not action or value.get('binding')!=binding:raise ValueError('RECEIPT_BINDING_INVALID')
            fingerprint=hashlib.sha256(json.dumps(json.loads(action['payload']),sort_keys=True,separators=(',',':')).encode()).hexdigest()
            if value.get('action_sha256')!=fingerprint:raise ValueError('RECEIPT_ACTION_HASH_INVALID')
            row.update(valid_binding=True,action_id=action['id'],action_state=action['state'],reported_status=value['report']['status'])
        except (OSError,ValueError,TypeError,KeyError,AttributeError):row['reason']='RECEIPT_UNBOUND_OR_INVALID'
        rows.append(row)
    return {'receipts':rows,'automatic_replay_allowed':False,'broker_writes':0,'book_mutations':0}
