"""Explicit local worker-event adapter for the existing Host wake outbox.

Not an official broker/Host queue. A durable claim file is created with
O_EXCL before any worker starts, so two adapter objects/processes sharing the
same directory cannot execute the same idempotency key/attempt twice. The
worker runs detached; close/timeout/cancellation call its existing stop and
record the bound attempt as UNKNOWN rather than replaying it.
"""
from __future__ import annotations

from copy import deepcopy
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
import threading
import time
import uuid
import weakref
from pathlib import Path

from .host_mailbox import write_private_json


_LIVE_OWNERS = weakref.WeakValueDictionary()


LOCAL_WORKER_ADAPTER = 'local-worker-review'


def worker_adapter_selection_report(selection):
    """Static capability facts for a configured selection before construction."""
    if selection == LOCAL_WORKER_ADAPTER:
        return {'selection': LOCAL_WORKER_ADAPTER, 'configured': True,
                'construction_required': True,
                'official_queue': False, 'local_worker_event': True,
                'review_only': True, 'formal_adapter_qualified': False,
                'requires_native_confirmation': True,
                'execution_authorized': False, 'broker_effect_verified': False,
                'reason': None}
    return worker_adapter_report(None)


def worker_adapter_report(adapter=None):
    """One capability fact shared by status, manifest and session output."""
    capability = getattr(adapter, 'capability', {}) or {}
    if not isinstance(capability, dict):
        capability = {}
    local = capability.get('local_worker_event') is True
    official = capability.get('official_queue') is True
    available = bool(adapter is not None and callable(
        getattr(adapter, 'enqueue', None)) and (local or official))
    if local:
        selection = LOCAL_WORKER_ADAPTER
        reason = None
        review_only = True
    elif official:
        selection = 'official-host-queue'
        reason = None
        review_only = None
    else:
        selection = 'none'
        reason = ('HOST_WAKE_ADAPTER_UNCONFIGURED' if adapter is None
                  else 'HOST_WAKE_ADAPTER_UNAVAILABLE')
        review_only = None
    return {'selection': selection, 'configured': adapter is not None,
            'available': available, 'official_queue': official,
            'local_worker_event': local, 'review_only': review_only,
            'formal_adapter_qualified': False,
            'requires_native_confirmation': True,
            'execution_authorized': False, 'broker_effect_verified': False,
            'reason': reason}


class LocalWorkerWakeAdapter:
    capability = {
        'name': LOCAL_WORKER_ADAPTER,
        'official_queue': False,
        'local_worker_event': True,
        'review_only': True,
        'formal_adapter_qualified': False,
        'durable_idempotency': True,
        'cooperative_cancellation': True,
    }
    needs_action = True
    JOIN_SECONDS = 2.0
    MAX_RESULTS_PER_TAKE = 32
    INTAKE_LOCK_SECONDS = 0.5

    def __init__(self, dispatch, directory):
        if not callable(dispatch):
            raise ValueError('HOST_LOCAL_WORKER_DISPATCH_REQUIRED')
        self.dispatch = dispatch
        self.directory = Path(directory).resolve()
        self.directory.mkdir(parents=True, exist_ok=True)
        self.directory.chmod(0o700)
        self.stopped = threading.Event()
        self._lock = threading.Lock()
        self._active = {}
        self._threads = {}
        self._watchers = {}
        self.failures = []
        self.owner_id = uuid.uuid4().hex
        _LIVE_OWNERS[self.owner_id] = self
        self._worker_stopped = False
        self._enqueueing = False
        self._stop_thread = None
        self._stop_lock = threading.Lock()
        self._stop_error = None
        self._processing = {}
        self._processing_lock = threading.Lock()

    def _claim_path(self, key, attempt_id):
        name = hashlib.sha256(('%s\0%s' % (key, attempt_id)).encode()).hexdigest()
        return self.directory / ('claim-' + name + '.json')

    def _result_path(self, key, attempt_id):
        name = hashlib.sha256(('%s\0%s' % (key, attempt_id)).encode()).hexdigest()
        return self.directory / ('result-' + name + '.json')

    def _owner_path(self, processing_path):
        # Deliberately not a ``result-*.json`` name: owner metadata is not a
        # worker result and must never be handed to a consumer.
        return Path(processing_path).with_name(
            Path(processing_path).name + '.owner')

    @staticmethod
    def _pid_alive(pid):
        try:
            pid = int(pid)
        except (TypeError, ValueError):
            return False
        if pid <= 0:
            return False
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        return True

    def _processing_owner_active(self, processing_path):
        marker = self._read_claim(self._owner_path(processing_path))
        if not marker or marker.get('state') == 'RELEASED':
            return False
        if not self._pid_alive(marker.get('pid')):
            return False
        try:
            owner_pid = int(marker.get('pid'))
        except (TypeError, ValueError):
            return False
        if owner_pid == os.getpid():
            if marker.get('owner_id') == self.owner_id:
                # Same adapter: active only while its consumer holds the
                # in-memory processing handoff. A failed finish that removes it
                # from this set is recoverable and can be finalized without a
                # second Book report.
                with self._processing_lock:
                    return Path(processing_path) in self._processing
            # Another adapter in this process owns it only while that adapter
            # object is still live.
            return marker.get('owner_id') in _LIVE_OWNERS
        return True

    def _write_processing_owner(self, processing_path):
        marker = {'schema_version': 1, 'owner_id': self.owner_id,
                  'pid': os.getpid(), 'state': 'ACTIVE',
                  'acquired_at': datetime.now(timezone.utc).isoformat(),
                  'processing_file': Path(processing_path).name}
        path = self._owner_path(processing_path)
        if not self._private_json(path, marker):
            return False
        with self._processing_lock:
            self._processing[Path(processing_path)] = path
        return True

    def _release_processing_locked(self, processing_path, state, *, force=False):
        path = Path(processing_path)
        marker_path = self._owner_path(path)
        marker = self._read_claim(marker_path)
        if (marker is not None and not force
                and marker.get('owner_id') != self.owner_id):
            with self._processing_lock:
                self._processing.pop(path, None)
            return False
        try:
            marker_path.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            if marker is not None:
                marker['state'] = 'RELEASED'
                marker['release_reason'] = state
                marker['released_at'] = datetime.now(timezone.utc).isoformat()
                if not self._write_claim(marker_path, marker):
                    with self._lock:
                        self.failures.append({
                            'attempt_id': path.name,
                            'reason': 'PROCESSING_LEASE_RELEASE_FAILED'})
        with self._processing_lock:
            self._processing.pop(path, None)
        return True

    def _release_processing(self, processing_path, state, *, force=False):
        try:
            with self._intake_guard():
                return self._release_processing_locked(
                    processing_path, state, force=force)
        except TimeoutError:
            with self._lock:
                self.failures.append({
                    'attempt_id': Path(processing_path).name,
                    'reason': 'PROCESSING_RELEASE_LOCK_BUSY'})
            return False

    def _recover_abandoned_processing_locked(self):
        for path in sorted(self.directory.glob('result-*.processing.json')):
            marker_path = self._owner_path(path)
            marker = self._read_claim(marker_path)
            if marker_path.exists() and marker is None:
                # Damaged ownership evidence is quarantine, never a claim.
                with self._lock:
                    self.failures.append({
                        'attempt_id': path.name,
                        'reason': 'PROCESSING_OWNER_UNREADABLE_QUARANTINED'})
                continue
            if marker is not None and self._processing_owner_active(path):
                continue
            target = path.with_name(path.name.replace('.processing', ''))
            try:
                os.replace(path, target)
            except OSError:
                continue
            self._release_processing_locked(
                path, 'RELEASED_FOR_RECOVERY', force=True)
            return True
        return False

    def _failure_path(self, key, attempt_id):
        name = hashlib.sha256(('%s\0%s' % (key, attempt_id)).encode()).hexdigest()
        return self.directory / ('failure-' + name + '.json')

    @staticmethod
    def _private_json(path, value):
        try:
            write_private_json(path, value)
        except (OSError, ValueError):
            return False
        return True

    def _claim(self, key, attempt_id, event, action):
        path = self._claim_path(key, attempt_id)
        value = {'schema_version': 1,
                 'idempotency_key': key,
                 'attempt_id': attempt_id,
                 'event_id': event.get('event_id'),
                 'binding': event.get('binding'),
                 'action_id': event.get('action_id'),
                 'revision': event.get('revision'),
                 'action': deepcopy(action),
                 'state': 'CLAIMED',
                 'owner': hashlib.sha256(str(self.directory).encode()).hexdigest()[:16],
                 'claimed_at': datetime.now(timezone.utc).isoformat()}
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            prior = None
            for _ in range(5):
                try:
                    prior = json.loads(path.read_text())
                    break
                except (OSError, ValueError):
                    time.sleep(.005)
            if not isinstance(prior, dict):
                return None, 'CLAIM_UNREADABLE'
            if (prior.get('idempotency_key') != key
                    or prior.get('attempt_id') != attempt_id):
                return None, 'CLAIM_CONFLICT'
            return prior, 'CLAIM_EXISTS'
        with os.fdopen(fd, 'w') as handle:
            handle.write(json.dumps(value, sort_keys=True) + '\n')
            handle.flush();os.fsync(handle.fileno())
        return value, 'CLAIMED'

    def _claim_lock_path(self, key, attempt_id):
        name = hashlib.sha256(('%s\0%s' % (key, attempt_id)).encode()).hexdigest()
        return self.directory / ('claim-' + name + '.lock')

    @contextmanager
    def _claim_guard(self, key, attempt_id):
        """Serialize read/validate/write across adapters and processes."""
        path = self._claim_lock_path(key, attempt_id)
        fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            try:
                os.chmod(path, 0o600)
            except OSError:
                pass
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            except OSError:
                pass
            os.close(fd)

    def _intake_lock_path(self):
        return self.directory / 'intake.lock'

    @contextmanager
    def _intake_guard(self, timeout=None):
        """Short cross-process mutex for result/owner path transitions.

        This never encloses worker execution or Book writes. A failed
        acquisition returns/raises before any path change, so an owner can
        never steal a result merely because another owner is in the tiny
        rename-to-lease window.
        """
        budget = self.INTAKE_LOCK_SECONDS if timeout is None else timeout
        deadline = time.monotonic() + max(0.0, float(budget))
        path = self._intake_lock_path()
        fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            try:
                os.chmod(path, 0o600)
            except OSError:
                pass
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except (BlockingIOError, OSError):
                    if time.monotonic() >= deadline:
                        raise TimeoutError('LOCAL_WORKER_INTAKE_LOCK_BUSY')
                    time.sleep(.005)
            yield
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            except OSError:
                pass
            os.close(fd)

    @staticmethod
    def _read_claim(path):
        try:
            value = json.loads(Path(path).read_text())
        except (OSError, ValueError):
            return None
        return value if isinstance(value, dict) else None

    @staticmethod
    def _write_claim(path, value):
        temporary = Path(path).with_name(Path(path).name + '.tmp')
        try:
            temporary.write_text(json.dumps(value, sort_keys=True) + '\n')
            os.chmod(temporary, 0o600)
            os.replace(temporary, Path(path))
        except OSError:
            try:
                temporary.unlink()
            except OSError:
                pass
            return False
        return True

    @staticmethod
    def _replace_json(path, value):
        """Atomic replace for a durable result whose state was re-evaluated."""
        path = Path(path)
        temporary = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
        try:
            temporary.write_text(json.dumps(value, sort_keys=True) + '\n')
            os.chmod(temporary, 0o600)
            os.replace(temporary, path)
        except OSError:
            try:
                temporary.unlink()
            except OSError:
                pass
            return False
        return True

    def _transition_claim(self, key, attempt_id, allowed, state, **fields):
        """CAS one claim state, returning ``(previous_state, changed)``.

        The file lock is the cross-process mutex. A terminal state is never in
        ``allowed`` for a later transition, so UNKNOWN cannot regress.
        """
        path = self._claim_path(key, attempt_id)
        with self._claim_guard(key, attempt_id):
            value = self._read_claim(path)
            if value is None:
                return None, False
            current = value.get('state')
            if current not in allowed:
                return current, False
            value.update(fields)
            value['state'] = state
            if not self._write_claim(path, value):
                with self._lock:
                    self.failures.append({
                        'attempt_id': attempt_id,
                        'reason': 'CLAIM_STATE_WRITE_FAILED:' + str(state)})
                return current, False
            return current, True

    def _record_failure(self, key, attempt_id, reason):
        failure = {'schema_version': 1, 'idempotency_key': key,
                   'attempt_id': attempt_id, 'reason': reason,
                   'recorded_at': datetime.now(timezone.utc).isoformat()}
        path = self._failure_path(key, attempt_id)
        if not self._private_json(path, failure):
            with self._lock:
                self.failures.append(failure)

    def _result_value(self, key, attempt_id, event, result, *, late,
                      claim_state):
        return {'schema_version': 1,
                'event_id': event.get('event_id'),
                'attempt_id': attempt_id,
                'idempotency_key': key,
                'binding': event.get('binding'),
                'action_id': event.get('action_id'),
                'revision': event.get('revision'),
                'finished_at': datetime.now(timezone.utc).isoformat(),
                'worker_result': result,
                'late_after_timeout': bool(late),
                'claim_state': claim_state,
                'execution_authorized': False,
                'broker_effect_verified': False}

    def _record_result(self, key, attempt_id, event, result, *, late,
                       claim_state):
        path = self._result_path(key, attempt_id)
        return path, self._private_json(
            path, self._result_value(key, attempt_id, event, result,
                                     late=late, claim_state=claim_state))

    def _run(self, key, attempt_id, event, action):
        previous, running = self._transition_claim(
            key, attempt_id, {'CLAIMED'}, 'RUNNING')
        if not running:
            if previous in {'UNKNOWN', 'LATE_RESULT'}:
                self._record_result(
                    key, attempt_id, event,
                    {'status': 'unknown',
                     'message': 'LOCAL_WORKER_CANCELLED_BEFORE_START'},
                    late=True, claim_state=previous)
                self._record_failure(key, attempt_id,
                                     'LOCAL_WORKER_CANCELLED_BEFORE_START')
            else:
                self._record_failure(
                    key, attempt_id,
                    'LOCAL_WORKER_START_STATE_REJECTED:' + str(previous))
            with self._lock:
                self._active.pop(attempt_id, None)
            return
        with self._lock:
            active = self._active.get(attempt_id) or {}
            cancellation = active.get('cancellation')
        if cancellation is not None and cancellation.is_set():
            self._transition_claim(
                key, attempt_id, {'RUNNING'}, 'UNKNOWN',
                reason='LOCAL_WORKER_CANCELLED_BEFORE_DISPATCH')
            self._record_result(
                key, attempt_id, event,
                {'status': 'unknown',
                 'message': 'LOCAL_WORKER_CANCELLED_BEFORE_DISPATCH'},
                late=True, claim_state='UNKNOWN')
            self._record_failure(key, attempt_id,
                                 'LOCAL_WORKER_CANCELLED_BEFORE_DISPATCH')
            with self._lock:
                self._active.pop(attempt_id, None)
            return
        result = None
        try:
            result = self.dispatch(deepcopy(action))
        except BaseException as exc:
            result = {'status': 'unknown',
                      'message': 'LOCAL_WORKER_EXCEPTION:' + type(exc).__name__}
        finally:
            path, written = self._record_result(
                key, attempt_id, event, result, late=False,
                claim_state='RUNNING')
            if not written:
                self._transition_claim(
                    key, attempt_id, {'RUNNING'}, 'UNKNOWN',
                    reason='LOCAL_WORKER_RESULT_WRITE_FAILED')
                self._record_failure(key, attempt_id,
                                     'RESULT_WRITE_FAILED')
            else:
                # The result exists before the terminal state is decided, so a
                # cancellation can persist UNKNOWN while this owner is blocked.
                # Re-read and finalize under the cross-process claim mutex.
                with self._claim_guard(key, attempt_id):
                    claim = self._read_claim(self._claim_path(key, attempt_id))
                    current = (claim or {}).get('state')
                    late = current in {'UNKNOWN', 'LATE_RESULT'}
                    if current == 'RUNNING':
                        claim['state'] = 'RESULT_RECORDED'
                        claim['late_result'] = False
                    elif current in {'UNKNOWN', 'LATE_RESULT'}:
                        claim['late_result'] = True
                    value = self._result_value(
                        key, attempt_id, event, result, late=late,
                        claim_state=current)
                    rewritten = self._replace_json(path, value)
                    if claim is not None:
                        persisted = self._write_claim(
                            self._claim_path(key, attempt_id), claim)
                    else:
                        persisted = False
                    if not rewritten:
                        self._record_failure(
                            key, attempt_id, 'RESULT_FINALIZE_WRITE_FAILED')
                    if not persisted:
                        self._record_failure(
                            key, attempt_id, 'CLAIM_FINALIZE_WRITE_FAILED')
            with self._lock:
                self._active.pop(attempt_id, None)

    def _stop_worker(self, timeout=None):
        """Call the worker's stop once; never let a hung stop own the close."""
        with self._stop_lock:
            if self._stop_thread is None:
                if self._worker_stopped:
                    return True
                stop = getattr(self.dispatch, 'stop', None)
                if not callable(stop):
                    self._worker_stopped = True
                    return True

                def invoke():
                    try:
                        stop()
                    except BaseException as exc:
                        self._stop_error = (type(exc).__name__ + ':' +
                                            str(exc)[:256])
                self._stop_thread = threading.Thread(
                    target=invoke, daemon=True, name='local-worker-stop')
                self._stop_thread.start()
            thread = self._stop_thread
        if timeout is not None:
            thread.join(max(0.0, timeout))
        else:
            thread.join()
        complete = not thread.is_alive()
        self._worker_stopped = complete
        return complete

    def _cancel(self, key, attempt_id, reason):
        with self._lock:
            active = self._active.get(attempt_id)
        if active is None:
            return
        active['cancellation'].set()
        # CAS only an in-flight claim. RESULT_RECORDED is already terminal and
        # a late cancel must never regress it; UNKNOWN never regresses either.
        previous, changed = self._transition_claim(
            key, attempt_id, {'CLAIMED', 'RUNNING'}, 'UNKNOWN',
            reason=reason)
        if not changed:
            if previous == 'RESULT_RECORDED':
                self._record_failure(
                    key, attempt_id, 'LOCAL_WORKER_CANCEL_TOO_LATE_RESULT_RECORDED')
            elif previous is not None:
                self._record_failure(
                    key, attempt_id,
                    'LOCAL_WORKER_CANCEL_STATE_REJECTED:' + str(previous))
        else:
            self._record_failure(key, attempt_id, reason)
        self._stop_worker(timeout=self.JOIN_SECONDS)
        if self._stop_error is not None:
            self._record_failure(key, attempt_id,
                                 'LOCAL_WORKER_STOP_FAILED:' + self._stop_error)

    def _watch(self, key, attempt_id, cancellation, timeout_seconds):
        deadline = time.monotonic() + max(0.0, float(timeout_seconds))
        while time.monotonic() < deadline:
            with self._lock:
                if attempt_id not in self._active:
                    return
            if cancellation.wait(min(0.01, deadline - time.monotonic())):
                self._cancel(key, attempt_id,
                             'LOCAL_WORKER_CANCELLED_BEFORE_RESULT')
                return
        self._cancel(key, attempt_id, 'LOCAL_WORKER_TIMEOUT_NO_RETRY')

    def enqueue(self, event, *, idempotency_key, attempt_id, timeout_seconds,
                cancellation):
        if (not isinstance(event, dict) or not isinstance(idempotency_key, str)
                or not idempotency_key or not isinstance(attempt_id, str)
                or not attempt_id):
            return {'event_id': event.get('event_id') if isinstance(event, dict)
                    else None, 'attempt_id': attempt_id, 'accepted': False,
                    'retryable': False}
        action = event.get('worker_action')
        if (not isinstance(action, dict)
                or action.get('action_id') != event.get('action_id')
                or not isinstance(action.get('kind'), str)):
            return {'event_id': event.get('event_id'),
                    'attempt_id': attempt_id, 'accepted': False,
                    'retryable': False}
        if self.stopped.is_set() or cancellation.is_set():
            return {'event_id': event.get('event_id'),
                    'attempt_id': attempt_id, 'accepted': False,
                    'retryable': False}
        with self._lock:
            if self._active or self._enqueueing:
                # One CodexOrderWorker has one process field. This reservation
                # covers the claim-file I/O gap, so different attempts cannot
                # both pass an empty-active check and start two workers.
                return {'event_id': event.get('event_id'),
                        'attempt_id': attempt_id, 'accepted': False,
                        'retryable': True, 'reason': 'LOCAL_WORKER_BUSY'}
            self._enqueueing = True
        try:
            claim, state = self._claim(idempotency_key, attempt_id, event, action)
            if state == 'CLAIM_UNREADABLE':
                return {'event_id': event.get('event_id'),
                        'attempt_id': attempt_id, 'accepted': True,
                        'receipt_id': 'local-worker:' + attempt_id,
                        'retryable': False}
            if claim is None:
                return {'event_id': event.get('event_id'),
                        'attempt_id': attempt_id, 'accepted': False,
                        'retryable': False, 'reason': state}
            if state == 'CLAIM_EXISTS':
                # No second execution. An existing claim without a result is
                # UNKNOWN/abandoned, never a retryable duplicate. The claim
                # path is already derived from this exact key/attempt pair.
                return {'event_id': event.get('event_id'),
                        'attempt_id': attempt_id, 'accepted': True,
                        'receipt_id': 'local-worker:' + attempt_id,
                        'retryable': False}
            with self._lock:
                closed = self.stopped.is_set() or cancellation.is_set()
                if not closed:
                    self._active[attempt_id] = {
                        'cancellation': cancellation, 'key': idempotency_key}
            if closed:
                self._transition_claim(
                    idempotency_key, attempt_id, {'CLAIMED'}, 'UNKNOWN',
                    reason='LOCAL_WORKER_ADAPTER_CLOSED_BEFORE_START')
                return {'event_id': event.get('event_id'),
                        'attempt_id': attempt_id, 'accepted': False,
                        'retryable': False,
                        'reason': 'LOCAL_WORKER_ADAPTER_CLOSED_BEFORE_START'}
        finally:
            with self._lock:
                self._enqueueing = False
        thread = threading.Thread(target=self._run, args=(idempotency_key,
                                  attempt_id, event, action), daemon=True,
                                  name='local-worker-' + attempt_id[:12])
        self._threads[attempt_id] = thread
        try:
            thread.start()
            watcher = threading.Thread(target=self._watch,
                args=(idempotency_key, attempt_id, cancellation,
                      timeout_seconds), daemon=True,
                name='local-worker-watch-' + attempt_id[:12])
            self._watchers[attempt_id] = watcher
            watcher.start()
        except RuntimeError:
            with self._lock:
                self._active.pop(attempt_id, None)
            self._record_failure(idempotency_key, attempt_id,
                                 'WORKER_THREAD_START_FAILED')
            return {'event_id': event.get('event_id'),
                    'attempt_id': attempt_id, 'accepted': False,
                    'retryable': True}
        return {'event_id': event.get('event_id'), 'attempt_id': attempt_id,
                'accepted': True, 'receipt_id': 'local-worker:' + attempt_id,
                'retryable': False}

    def _take_normal_result_locked(self):
        for path in sorted(self.directory.glob('result-*.json')):
            if '.processing.' in path.name:
                continue
            processing = path.with_name(path.name[:-5] + '.processing.json')
            try:
                os.replace(path, processing)
            except OSError:
                continue
            if not self._write_processing_owner(processing):
                try:
                    os.replace(processing, path)
                except OSError:
                    self._record_failure(processing.name, processing.name,
                                         'PROCESSING_ROLLBACK_FAILED')
                self._record_failure(processing.name, processing.name,
                                     'PROCESSING_OWNER_WRITE_FAILED')
                return None
            try:
                value = json.loads(processing.read_text())
            except (OSError, ValueError):
                self._finish_result_locked(processing, 'REJECTED_UNREADABLE')
                continue
            if not isinstance(value, dict):
                self._finish_result_locked(processing, 'REJECTED_NOT_OBJECT')
                continue
            return processing, value
        return None

    def take_result(self):
        """Atomically hand one completed worker result to the owner consumer.

        The rename-to-processing and owner-marker publish happen under one
        short cross-process intake mutex. A concurrent owner therefore cannot
        observe an unowned processing file and reclaim it in the gap. If the
        mutex is busy, this returns None; it never steals an active result.
        """
        item = None
        try:
            with self._intake_guard():
                item = self._take_normal_result_locked()
                if item is None and self._recover_abandoned_processing_locked():
                    item = self._take_normal_result_locked()
        except TimeoutError:
            with self._lock:
                self.failures.append({'attempt_id': 'INTAKE',
                                      'reason': 'INTAKE_LOCK_BUSY_NO_STEAL'})
            return None
        if item is None:
            return None
        return str(item[0]), item[1]

    def take_results(self):
        """Return at most one bounded page of completed worker results."""
        results = []
        for _ in range(self.MAX_RESULTS_PER_TAKE):
            item = self.take_result()
            if item is None:
                break
            results.append(item)
        return results

    def requeue_result(self, path, reason):
        """Return a processing result to the queue for a later owner retry.

        The caller must own the durable processing lease. Missing or foreign
        ownership is a refused transition, not permission to move the file.
        """
        path = Path(path)
        target = path.with_name(path.name.replace('.processing', ''))
        try:
            with self._intake_guard():
                marker = self._read_claim(self._owner_path(path))
                if marker is None or marker.get('owner_id') != self.owner_id:
                    with self._lock:
                        self.failures.append({
                            'attempt_id': path.name,
                            'reason': 'RESULT_REQUEUE_OWNER_MISMATCH'})
                    return None
                try:
                    os.replace(path, target)
                except OSError as exc:
                    self._release_processing_locked(path, 'REQUEUE_FAILED')
                    with self._lock:
                        self.failures.append({'attempt_id': path.name,
                                              'reason': 'RESULT_REQUEUE_FAILED:' + str(exc)})
                    return None
                self._release_processing_locked(path, 'REQUEUED')
                return target
        except TimeoutError:
            with self._processing_lock:
                self._processing.pop(path, None)
            with self._lock:
                self.failures.append({'attempt_id': path.name,
                                      'reason': 'RESULT_REQUEUE_LOCK_BUSY'})
            return None

    def processing_results(self):
        return sorted(path.name for path in self.directory.glob(
            'result-*.processing.json'))

    def _finish_result_locked(self, path, disposition):
        path = Path(path)
        consumed = disposition in {
            'BOOK_REPORT_IMPORTED', 'WAITING_CONFIRMATION_RECORDED',
            'UNKNOWN_REQUIRES_RECONCILIATION',
            'ALREADY_APPLIED_RECOVERY_FINALIZED'}
        base = path.name.replace('.processing', '').replace('result-', '')
        target = path.with_name(('consumed-' if consumed else 'rejected-') + base)
        marker = self._read_claim(self._owner_path(path))
        if marker is None or marker.get('owner_id') != self.owner_id:
            with self._lock:
                self.failures.append({
                    'attempt_id': path.name,
                    'reason': 'RESULT_FINISH_OWNER_MISMATCH'})
            return None
        try:
            os.replace(path, target)
        except OSError as exc:
            self._release_processing_locked(path, 'FINISH_FAILED')
            with self._lock:
                self.failures.append({'attempt_id': path.name,
                                      'reason': 'RESULT_FINISH_FAILED:' + str(exc)})
            return None
        self._release_processing_locked(path, 'FINISHED:' + disposition)
        return target

    def finish_result(self, path, disposition):
        try:
            with self._intake_guard():
                return self._finish_result_locked(path, disposition)
        except TimeoutError:
            with self._processing_lock:
                self._processing.pop(Path(path), None)
            with self._lock:
                self.failures.append({
                    'attempt_id': Path(path).name,
                    'reason': 'RESULT_FINISH_LOCK_BUSY'})
            return None

    def claim_state(self, idempotency_key, attempt_id):
        try:
            return json.loads(self._claim_path(idempotency_key, attempt_id).read_text())
        except (OSError, ValueError):
            return None

    def close(self):
        deadline = time.monotonic() + self.JOIN_SECONDS
        self.stopped.set()
        with self._lock:
            active = list(self._active.items())
        for attempt_id, value in active:
            value['cancellation'].set()
            previous, changed = self._transition_claim(
                value['key'], attempt_id, {'CLAIMED', 'RUNNING'}, 'UNKNOWN',
                reason='LOCAL_WORKER_ADAPTER_CLOSED')
            if changed:
                self._record_failure(value['key'], attempt_id,
                                     'LOCAL_WORKER_ADAPTER_CLOSED_NO_REPLAY')
            elif previous not in (None, 'RESULT_RECORDED'):
                self._record_failure(
                    value['key'], attempt_id,
                    'LOCAL_WORKER_CLOSE_STATE_REJECTED:' + str(previous))
        stop_complete = self._stop_worker(
            timeout=max(0.0, deadline - time.monotonic()))
        for attempt_id, thread in list(self._threads.items()):
            thread.join(max(0.0, deadline - time.monotonic()))
        for attempt_id, watcher in list(self._watchers.items()):
            watcher.join(max(0.0, deadline - time.monotonic()))
        with self._lock:
            alive = [attempt_id for attempt_id, thread in self._threads.items()
                     if thread.is_alive()]
            alive += [attempt_id for attempt_id, watcher in self._watchers.items()
                      if watcher.is_alive()]
            stop_thread = self._stop_thread
        if stop_thread is not None and stop_thread.is_alive():
            alive.append('WORKER_STOP')
        processing = self.processing_results()
        with self._processing_lock:
            owned_processing = list(self._processing)
        for path in owned_processing:
            self._release_processing(path, 'ADAPTER_CLOSED')
        _LIVE_OWNERS.pop(self.owner_id, None)
        stop_ok = stop_complete and self._stop_error is None
        unresolved = bool(alive) or not stop_ok
        return {'status': ('LOCAL_WORKER_WAKE_ADAPTER_RESOURCES_UNRESOLVED'
                           if unresolved else
                           'LOCAL_WORKER_WAKE_ADAPTER_STOPPED'),
                'worker_stop_called': bool(self._stop_thread is not None
                                           or self._worker_stopped),
                'worker_stop_retained': not stop_complete,
                'worker_stop_ok': stop_ok,
                'worker_stop_error': self._stop_error,
                'threads_alive': alive,
                'processing_results': processing,
                'active_attempts': [attempt_id for attempt_id, _ in active],
                'failures': list(self.failures),
                'execution_authorized': False}


def local_worker_wake_adapter(book, *, output_dir, policy=None,
                              worker_factory=None, review_only=True):
    """Public factory: explicit local worker adapter for a real session.

    Construction starts no worker and grants no execution authority. A caller
    must opt in by passing this factory to the real-session seam; disabled and
    capture-only sessions never construct a bridge/worker.
    """
    from .agent_worker import CodexOrderWorker
    policy = getattr(book, 'policy', None)
    if not isinstance(policy, dict):
        raise ValueError('HOST_LOCAL_WORKER_POLICY_REQUIRED')
    suffix = str(policy.get('account_suffix', ''))
    if not (suffix.isdigit() and len(suffix) == 4) or policy.get('capture_only') is True:
        raise ValueError('HOST_LOCAL_WORKER_SCOPE_DENIED')
    directory = Path(output_dir).resolve() / 'local-worker'
    factory = worker_factory or CodexOrderWorker
    import inspect
    if 'policy' in inspect.signature(factory).parameters:
        worker = factory(Path.cwd(), directory, review_only=review_only,
                         policy=policy)
    else:
        worker = factory(Path.cwd(), directory, review_only=review_only)
    return LocalWorkerWakeAdapter(worker, directory)
