"""Owner-only durable event wake queue. Waking a Host grants no trade authority.

No transport is enabled by this module. An adapter must implement a supported
official queue contract, including durable idempotency keys and explicit
acceptance receipts. A tool listing or accessible thread is not that contract.
There is deliberately no thread resume, polling model, broker API or fallback.
"""
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
import re
import threading
import uuid

from .audit import canonical, digest
from .host_mailbox import _binding
from .ledger import timestamp


PRIORITY = {'protect': 0, 'cancel': 1, 'exit': 2, 'incident': 3, 'entry': 4}
TERMINAL = {'ACCEPTED', 'EXPIRED', 'FAILED', 'UNKNOWN'}


class UnavailableWakeAdapter:
    """Default: no supported, configured Host queue exists."""
    capability = {'official_queue': False, 'durable_idempotency': False}


class HostWakeOutbox:
    """Use only on the execution owner's thread/connection, never a second DB.

    Acceptance means an event was queued, not that a Host ran or a broker acted.
    STARTED surviving a process crash becomes UNKNOWN, never an automatic retry.
    Only a receiver's explicit *not accepted* result is eligible for retry.
    """
    def __init__(self, connection, *, clock=None, max_attempts=3):
        if type(max_attempts) is not int or not 1 <= max_attempts <= 5:
            raise ValueError('HOST_WAKE_ATTEMPTS_INVALID')
        self.db = connection
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.owner = threading.get_ident()
        self.max_attempts = max_attempts
        with self._transaction():
            self.db.execute('''CREATE TABLE IF NOT EXISTS host_wake_events(
                event_id TEXT PRIMARY KEY, payload TEXT NOT NULL, state TEXT NOT NULL,
                attempts INTEGER NOT NULL, next_at TEXT NOT NULL, detail TEXT NOT NULL)''')
            self.db.execute('''CREATE TABLE IF NOT EXISTS host_wake_history(
                sequence INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL,
                recorded_at TEXT NOT NULL, state TEXT NOT NULL, detail TEXT NOT NULL)''')
            self.db.execute('''CREATE TRIGGER IF NOT EXISTS host_wake_history_no_update
                BEFORE UPDATE ON host_wake_history BEGIN
                SELECT RAISE(ABORT,'wake history is append-only'); END''')
            self.db.execute('''CREATE TRIGGER IF NOT EXISTS host_wake_history_no_delete
                BEFORE DELETE ON host_wake_history BEGIN
                SELECT RAISE(ABORT,'wake history is append-only'); END''')

    @contextmanager
    def _transaction(self):
        if threading.get_ident() != self.owner:
            raise ValueError('HOST_WAKE_OWNER_THREAD_REQUIRED')
        if self.db.in_transaction:
            raise ValueError('HOST_WAKE_SEPARATE_OWNER_TRANSACTION_REQUIRED')
        self.db.execute('BEGIN IMMEDIATE')
        try:
            yield
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def _change(self, event_id, state, detail, *, attempts=None, next_at=None):
        prior = self.db.execute('SELECT attempts,next_at,state,detail FROM host_wake_events WHERE event_id=?',
                                (event_id,)).fetchone()
        if prior is None:
            raise ValueError('HOST_WAKE_EVENT_NOT_FOUND')
        if (prior[2] == state and prior[3] == canonical(detail)
                and (attempts is None or attempts == prior[0])
                and (next_at is None or next_at == prior[1])):
            return
        self.db.execute('UPDATE host_wake_events SET state=?,attempts=?,next_at=?,detail=? WHERE event_id=?',
                        (state, prior[0] if attempts is None else attempts,
                         prior[1] if next_at is None else next_at, canonical(detail), event_id))
        self.db.execute('INSERT INTO host_wake_history(event_id,recorded_at,state,detail) VALUES(?,?,?,?)',
                        (event_id, self.clock().isoformat(), state, canonical(detail)))

    def enqueue(self, *, binding, action_id, kind, revision, expires_at, worker_action=None):
        """Same session/action/revision is immutable and never rearms old work."""
        _binding(binding)
        if (kind not in PRIORITY or not isinstance(action_id, str) or not 0 < len(action_id) <= 256
                or not isinstance(revision, str) or not re.fullmatch('[0-9a-f]{64}', revision)):
            raise ValueError('HOST_WAKE_EVENT_INVALID')
        expiry = timestamp(expires_at)
        identity = digest({'binding': binding, 'action_id': action_id, 'kind': kind, 'revision': revision})
        event_id = 'host-wake-' + identity
        value = {'schema_version': 1, 'event_id': event_id, 'binding': binding,
                 'action_id': action_id, 'kind': kind, 'revision': revision,
                 'expires_at': expiry.isoformat(), 'execution_authorized': False,
                 'broker_effect_verified': False}
        if worker_action is not None:
            if not isinstance(worker_action, dict):
                raise ValueError('HOST_WAKE_WORKER_ACTION_INVALID')
            value['worker_action'] = worker_action
        with self._transaction():
            previous = self.db.execute('SELECT payload FROM host_wake_events WHERE event_id=?', (event_id,)).fetchone()
            if previous:
                if canonical(value) != previous[0]:
                    raise ValueError('HOST_WAKE_EVENT_CONFLICT')
                return event_id
            now = self.clock()
            self.db.execute('INSERT INTO host_wake_events VALUES(?,?,?,?,?,?)',
                            (event_id, canonical(value), 'PENDING', 0, now.isoformat(), '{}'))
            self._change(event_id, 'PENDING' if expiry > now else 'EXPIRED', {'reason': 'OWNER_EVENT_RECORDED'})
        return event_id

    def get(self, event_id):
        if threading.get_ident() != self.owner:
            raise ValueError('HOST_WAKE_OWNER_THREAD_REQUIRED')
        row = self.db.execute('SELECT payload,state,attempts,next_at,detail FROM host_wake_events WHERE event_id=?',
                              (event_id,)).fetchone()
        if row is None:
            raise ValueError('HOST_WAKE_EVENT_NOT_FOUND')
        return {**json.loads(row[0]), 'state': row[1], 'attempts': row[2],
                'next_at': row[3], 'detail': json.loads(row[4]),
                'host_execution_verified': False}

    def recover_interrupted(self):
        """Call once after acquiring exclusive controller ownership on restart."""
        with self._transaction():
            rows = self.db.execute("SELECT event_id FROM host_wake_events WHERE state='STARTED'").fetchall()
            for (event_id,) in rows:
                self._change(event_id, 'UNKNOWN', {**self.get(event_id)['detail'],
                             'reason': 'DISPATCH_INTERRUPTED_NO_AUTOMATIC_REPLAY'})
        return len(rows)

    def retire_superseded(self, *, binding, current_event_ids):
        """Owner projection invalidates unclaimed events whose action changed.

        UNKNOWN/STARTED are never cleared by projection changes. A sent event
        remains in history even when the action is no longer current.
        """
        _binding(binding)
        if (not isinstance(current_event_ids, (list, set, tuple))
                or len(current_event_ids) > 1000
                or any(not isinstance(value, str) or not re.fullmatch('host-wake-[0-9a-f]{64}', value)
                       for value in current_event_ids)):
            raise ValueError('HOST_WAKE_CURRENT_EVENTS_INVALID')
        current = set(current_event_ids)
        retired = 0
        with self._transaction():
            for event_id, raw in self.db.execute(
                    "SELECT event_id,payload FROM host_wake_events WHERE state IN ('PENDING','BLOCKED','RETRY')").fetchall():
                if json.loads(raw)['binding'] == binding and event_id not in current:
                    self._change(event_id, 'EXPIRED', {'reason': 'OWNER_ACTION_SUPERSEDED'})
                    retired += 1
        return retired

    @staticmethod
    def adapter_available(adapter):
        capability = getattr(adapter, 'capability', {})
        if not isinstance(capability, dict):
            return False
        # An official Host queue and a supported local worker-event adapter are
        # distinct delivery classes. Neither may claim broker execution.
        supported_class = (capability.get('official_queue') is True
                           or capability.get('local_worker_event') is True)
        return (supported_class
                and capability.get('durable_idempotency') is True
                and callable(getattr(adapter, 'enqueue', None)))

    def reserve_one(self, *, binding, adapter=None):
        """Durably reserve an exact attempt. This method never calls an adapter."""
        _binding(binding)
        selected = None
        with self._transaction():
            now = self.clock()
            candidates = []
            for event_id, raw, state, attempts, next_at in self.db.execute(
                    "SELECT event_id,payload,state,attempts,next_at FROM host_wake_events WHERE state IN ('PENDING','BLOCKED','RETRY')").fetchall():
                value = json.loads(raw)
                if value['binding'] != binding or timestamp(value['expires_at']) <= now:
                    self._change(event_id, 'EXPIRED', {'reason': 'SESSION_CHANGED_OR_EVENT_EXPIRED'})
                elif timestamp(next_at) <= now:
                    candidates.append((PRIORITY[value['kind']], event_id, value, attempts))
            if candidates:
                _, event_id, value, attempts = min(candidates)
                if not self.adapter_available(adapter):
                    for _, blocked_id, _, _ in candidates:
                        self._change(blocked_id, 'BLOCKED', {'reason': 'OFFICIAL_HOST_QUEUE_UNAVAILABLE',
                             'adapter_status': ('HOST_WAKE_ADAPTER_UNCONFIGURED'
                                 if adapter is None else
                                 'HOST_WAKE_ADAPTER_UNSUPPORTED')})
                else:
                    attempt_id = str(uuid.uuid4())
                    self._change(event_id, 'STARTED', {'reason': 'DISPATCH_RESERVED',
                                 'attempt_id': attempt_id}, attempts=attempts + 1)
                    selected = {'event': value, 'attempt_id': attempt_id, 'event_id': event_id,
                                'reserved': True, 'execution_authorized': False}
        if not candidates:
            return {'reserved': False, 'dispatched': False, 'reason': 'NO_ELIGIBLE_WAKE_EVENT',
                    'execution_authorized': False}
        if selected is None:
            return {'reserved': False, 'dispatched': False, **self.get(event_id)}
        return selected

    def _attempt(self, reservation):
        event = reservation['event'];event_id = reservation['event_id']
        current = self.get(event_id)
        if (event.get('event_id') != event_id or current['detail'].get('attempt_id') != reservation['attempt_id']
                or any(current.get(key) != value for key, value in event.items())
                or current['state'] not in {'STARTED', 'UNKNOWN'}):
            raise ValueError('HOST_WAKE_ATTEMPT_BINDING_INVALID')
        return current

    @staticmethod
    def valid_receipt(reservation, receipt):
        return (isinstance(receipt, dict) and set(receipt) <= {
                'event_id', 'attempt_id', 'accepted', 'receipt_id', 'retryable'}
                and receipt.get('event_id') == reservation['event_id']
                and receipt.get('attempt_id') == reservation['attempt_id']
                and type(receipt.get('accepted')) is bool
                and (receipt['accepted'] is False or isinstance(receipt.get('receipt_id'), str)
                     and 0 < len(receipt['receipt_id']) <= 256))

    def mark_attempt_unknown(self, reservation, *, reason):
        with self._transaction():
            current = self._attempt(reservation)
            if current['state'] == 'STARTED':
                self._change(reservation['event_id'], 'UNKNOWN', {'reason': reason,
                             'attempt_id': reservation['attempt_id']})
        return {'dispatched': True, **self.get(reservation['event_id'])}

    def record_attempt(self, reservation, receipt):
        """Import only this attempt; late receipts never clear timed-out UNKNOWN."""
        with self._transaction():
            current = self._attempt(reservation)
            event_id = reservation['event_id'];attempt_id = reservation['attempt_id']
            valid = self.valid_receipt(reservation, receipt)
            if current['state'] == 'UNKNOWN':
                # Keep the original ambiguity/failure state while preserving
                # independently returned exact queue evidence for review.
                self._change(event_id, 'UNKNOWN', {**current['detail'],
                    'late_receipt_received': True, 'late_receipt_valid': valid,
                    'late_receipt_sha256': digest(receipt) if valid else None,
                    'late_queue_accepted': receipt['accepted'] if valid else None,
                    'queue_acceptance_only': True, 'host_execution_verified': False})
            elif valid and receipt['accepted'] is True:
                self._change(event_id, 'ACCEPTED', {'queue_receipt_id': receipt['receipt_id'],
                             'attempt_id': attempt_id, 'queue_acceptance_only': True,
                             'host_execution_verified': False})
            elif valid and receipt['accepted'] is False:
                attempt = current['attempts']
                retry = receipt.get('retryable') is True and attempt < self.max_attempts
                self._change(event_id, 'RETRY' if retry else 'FAILED',
                             {'reason': 'QUEUE_EXPLICITLY_NOT_ACCEPTED', 'attempt_id': attempt_id},
                             next_at=(self.clock() + timedelta(seconds=min(2 ** attempt, 30))).isoformat())
            else:
                self._change(event_id, 'UNKNOWN', {'reason': 'QUEUE_ACCEPTANCE_UNKNOWN_NO_AUTOMATIC_REPLAY',
                             'attempt_id': attempt_id})
        return {'dispatched': True, **self.get(reservation['event_id'])}

    def dispatch_one(self, *, binding, adapter=None):
        """Offline synchronous compatibility helper; never use on a live owner.

        Controller integration uses AsyncHostWakeDispatcher. Arbitrary callback
        deadlines cannot be enforced by this synchronous testing helper.
        """
        reservation = self.reserve_one(binding=binding, adapter=adapter)
        if not reservation['reserved']:
            return reservation
        try:
            receipt = adapter.enqueue(reservation['event'], idempotency_key=reservation['event_id'],
                                      attempt_id=reservation['attempt_id'], timeout_seconds=2)
        except Exception:
            receipt = None
        return self.record_attempt(reservation, receipt)
