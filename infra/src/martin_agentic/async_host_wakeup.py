"""Detached queue I/O with owner-enforced deadlines and durable exact receipts.

No official adapter is configured here. An injected reviewed adapter must expose
cooperative cancellation. A dishonest/hung callback cannot block the owner: its
attempt becomes UNKNOWN, cancellation is requested and no second callback can
start until the old thread terminates. Python cannot force-kill that thread;
shutdown reports the retained task instead of claiming transport termination.
"""
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
import threading
import time

from .audit import digest
from .control_tasks import BackgroundTask
from .host_mailbox import write_private_json, read_private_json
from .host_wakeup import HostWakeOutbox
from .ledger import timestamp


def _queue_io(adapter, reservation, cancellation, deadline, directory):
    # Detached primitives only; neither outbox nor SQLite connection enters I/O.
    receipt = None
    try:
        if not cancellation.is_set() and time.monotonic() < deadline:
            value = adapter.enqueue(deepcopy(reservation['event']),
                idempotency_key=reservation['event_id'], attempt_id=reservation['attempt_id'],
                timeout_seconds=max(0, deadline-time.monotonic()), cancellation=cancellation)
            if HostWakeOutbox.valid_receipt(reservation, value):
                receipt = value
    except BaseException:
        # A failing callback is still an ambiguous queue submission.
        pass
    finished = time.monotonic()
    value = {'reservation': reservation, 'receipt': receipt,
             'receipt_sha256': digest(receipt), 'finished_monotonic': finished,
             'recorded_at': datetime.now(timezone.utc).isoformat()}
    path = Path(directory)/(reservation['attempt_id']+'.json')
    write_private_json(path, value)
    return {'receipt_path': str(path), 'finished_monotonic': finished}


class AsyncHostWakeDispatcher:
    """At most one retained callback, with all database work on its owner."""
    def __init__(self, outbox, directory, *, adapter=None, timeout_seconds=2):
        if isinstance(timeout_seconds, bool) or not 0 < timeout_seconds <= 2:
            raise ValueError('HOST_WAKE_DEADLINE_INVALID')
        self.outbox = outbox
        self.directory = Path(directory).resolve()
        self.adapter = adapter
        self.timeout = timeout_seconds
        self.monotonic = time.monotonic
        self.owner = threading.get_ident()
        self.task = BackgroundTask('official-host-wake-queue')
        self.pending = None
        self.stopped = False

    def _owner(self):
        if threading.get_ident() != self.owner:
            raise ValueError('HOST_WAKE_DISPATCH_OWNER_REQUIRED')

    def _unknown(self, reason):
        self.pending['cancellation'].set()
        self.pending['timer'].cancel()
        # A retained, uncooperative callback can outlive many owner ticks.
        # Persist ambiguity once; repeatedly opening audit-verifying SQLite
        # transactions cannot add evidence and consumes the control budget.
        # Set the flag only after durable success so a failed commit is retried.
        if not self.pending['timeout_recorded']:
            result = self.outbox.mark_attempt_unknown(self.pending['reservation'], reason=reason)
            self.pending['unknown_result'] = deepcopy(result)
            self.pending['timeout_recorded'] = True
        return deepcopy(self.pending['unknown_result'])

    def _take(self):
        ready = self.task.take()
        if ready is None:
            return None
        pending = self.pending;reservation = pending['reservation']
        pending['timer'].cancel()
        ok, value = ready
        receipt = None
        try:
            if not ok:
                raise ValueError('HOST_WAKE_RECEIPT_PERSISTENCE_FAILED')
            expected = self.directory/(reservation['attempt_id']+'.json')
            if Path(value['receipt_path']) != expected:
                raise ValueError('HOST_WAKE_RECEIPT_PATH_MISMATCH')
            persisted = read_private_json(expected)
            if (persisted['reservation'] != reservation
                    or persisted['receipt_sha256'] != digest(persisted['receipt'])
                    or persisted['finished_monotonic'] != value['finished_monotonic']):
                raise ValueError('HOST_WAKE_RECEIPT_BINDING_INVALID')
            if value['finished_monotonic'] >= pending['deadline']:
                self._unknown('QUEUE_DEADLINE_EXCEEDED_NO_AUTOMATIC_REPLAY')
            receipt = persisted['receipt']
        except (OSError, ValueError, KeyError, TypeError):
            self._unknown('QUEUE_RECEIPT_UNAVAILABLE_OR_INVALID')
        result = self.outbox.record_attempt(reservation, receipt)
        self.pending = None
        return result

    @property
    def next_deadline_monotonic(self):
        self._owner()
        return (self.pending['deadline'] if self.pending and not self.pending['timeout_recorded']
                else None)

    def tick(self, *, binding, allow_dispatch=True, current_event_ids=None):
        self._owner()
        if self.pending:
            expired = (self.monotonic() >= self.pending['deadline']
                       or self.outbox.clock() >= timestamp(self.pending['reservation']['event']['expires_at']))
            changed = self.pending['reservation']['event']['binding'] != binding
            superseded = (current_event_ids is not None
                and self.pending['reservation']['event_id'] not in current_event_ids)
            if expired or changed or superseded:
                result = self._unknown('QUEUE_SESSION_CHANGED_NO_AUTOMATIC_REPLAY' if changed
                    else 'QUEUE_ACTION_SUPERSEDED_NO_AUTOMATIC_REPLAY' if superseded
                    else 'QUEUE_DEADLINE_EXCEEDED_NO_AUTOMATIC_REPLAY')
            ready = self._take()
            if ready is not None:
                return ready
            if expired or changed or superseded:
                return {**result, 'callback_retained': True,
                        'cancellation_requested': True, 'attention_required': True}
            return {'dispatched': False, 'status': 'WAKE_QUEUE_IO_IN_FLIGHT',
                    'execution_authorized': False}
        if self.stopped or not allow_dispatch:
            return {'dispatched': False, 'reason': 'WAKE_DISPATCH_STOPPED',
                    'execution_authorized': False}
        capability = getattr(self.adapter, 'capability', {})
        eligible = (HostWakeOutbox.adapter_available(self.adapter)
                    and capability.get('cooperative_cancellation') is True)
        reservation = self.outbox.reserve_one(binding=binding,
            adapter=self.adapter if eligible else None)
        if not reservation['reserved']:
            return reservation
        remaining = (timestamp(reservation['event']['expires_at'])-self.outbox.clock()).total_seconds()
        budget = min(self.timeout, max(0, remaining))
        cancellation = threading.Event()
        timer = threading.Timer(budget, cancellation.set)
        timer.daemon = True
        self.pending = {'reservation': reservation, 'cancellation': cancellation,
                        'deadline': self.monotonic()+budget, 'timer': timer, 'timeout_recorded': False}
        try:
            timer.start()
            self.task.start(_queue_io, self.adapter, deepcopy(reservation), cancellation,
                            self.pending['deadline'], str(self.directory))
        except Exception:
            result = self._unknown('QUEUE_TASK_START_FAILED_NO_AUTOMATIC_REPLAY')
            self.pending = None
            return result
        return {'dispatched': False, 'status': 'WAKE_QUEUE_IO_STARTED',
                'event_id': reservation['event_id'], 'attempt_id': reservation['attempt_id'],
                'execution_authorized': False}

    def close(self):
        """Nonblocking shutdown: unknown is durable before the owner can close."""
        self._owner();self.stopped = True
        adapter_close = getattr(self.adapter, 'close', None)
        adapter_shutdown = None
        if callable(adapter_close):
            try:
                adapter_shutdown = adapter_close()
            except Exception as exc:
                adapter_shutdown = {'status': 'WAKE_ADAPTER_CLOSE_FAILED',
                                    'error': type(exc).__name__ + ':' + str(exc)}
        result = None
        if self.pending:
            result = self._unknown('QUEUE_OWNER_STOPPED_NO_AUTOMATIC_REPLAY')
            imported = self._take()
            if imported is not None:result = imported
        adapter_unresolved = bool(
            adapter_shutdown and (
                adapter_shutdown.get('status') == 'WAKE_ADAPTER_CLOSE_FAILED'
                or adapter_shutdown.get('status') ==
                'LOCAL_WORKER_WAKE_ADAPTER_RESOURCES_UNRESOLVED'
                or adapter_shutdown.get('threads_alive')
                or adapter_shutdown.get('worker_stop_retained')
                or adapter_shutdown.get('worker_stop_error')))
        return {'status': 'WAKE_DISPATCH_STOPPED', 'pending_result': result,
                'adapter_shutdown': adapter_shutdown,
                'callback_retained': self.pending is not None,
                'callback_alive': bool(self.task.thread and self.task.thread.is_alive()),
                'attention_required': adapter_unresolved,
                'execution_authorized': False}
