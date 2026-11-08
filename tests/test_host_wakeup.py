from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
import tempfile
from unittest import TestCase
import uuid

from martin_agentic.host_wakeup import HostWakeOutbox


class Queue:
    capability = {'official_queue': True, 'durable_idempotency': True}

    def __init__(self, outcome=True):
        self.outcome = outcome
        self.calls = []

    def enqueue(self, event, **kwargs):
        self.calls.append((event, kwargs))
        if isinstance(self.outcome, BaseException):
            raise self.outcome
        return {'event_id': event['event_id'], 'attempt_id': kwargs['attempt_id'],
                'accepted': self.outcome, 'receipt_id': 'official-queue-receipt', 'retryable': True}


class HostWakeTests(TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'owner.sqlite'
        self.db = sqlite3.connect(self.path)
        self.addCleanup(lambda: self.db.close())
        self.now = datetime(2026, 10, 27, 14, tzinfo=timezone.utc)
        self.binding = {'db': str(self.path), 'account': '••••0000', 'account_key': 'a' * 64,
                        'policy_sha256': 'b' * 64, 'source_fingerprint': 'c' * 64,
                        'epoch': str(uuid.uuid4())}
        self.outbox = HostWakeOutbox(self.db, clock=lambda: self.now)

    def event(self, kind='protect', action='action-1', revision='d' * 64):
        return self.outbox.enqueue(binding=self.binding, action_id=action, kind=kind,
                                   revision=revision, expires_at=(self.now + timedelta(seconds=30)).isoformat())

    def dispatch(self, adapter=None):
        return self.outbox.dispatch_one(binding=self.binding, adapter=adapter)

    def test_default_unavailable_is_blocked_never_accepted(self):
        identity = self.event()
        result = self.dispatch()
        self.assertEqual(result['state'], 'BLOCKED')
        self.assertEqual(result['detail']['reason'], 'OFFICIAL_HOST_QUEUE_UNAVAILABLE')
        self.assertFalse(result['dispatched'])
        self.assertEqual(result['attempts'], 0)
        self.assertFalse(result['execution_authorized'])
        self.assertFalse(result['broker_effect_verified'])
        self.assertEqual(self.outbox.get(identity)['state'], 'BLOCKED')

    def test_repeated_blocked_dispatch_does_not_spam_history(self):
        self.event()
        self.dispatch()
        for _ in range(20):
            self.dispatch()
        self.assertEqual(self.db.execute('SELECT count(*) FROM host_wake_history').fetchone()[0], 2)
        self.assertEqual(self.dispatch(Queue())['state'], 'ACCEPTED')

    def test_unavailable_transport_exposes_all_actions_without_priority_starvation(self):
        identities = [self.event(kind=kind, action=kind) for kind in ('entry', 'exit', 'protect')]
        self.dispatch()
        self.assertEqual([self.outbox.get(identity)['state'] for identity in identities], ['BLOCKED'] * 3)

    def test_owner_projection_retires_superseded_but_preserves_unknown(self):
        old = self.event()
        self.dispatch()
        self.outbox.retire_superseded(binding=self.binding, current_event_ids=[])
        unknown = self.event(action='unknown-action')
        self.dispatch(Queue(TimeoutError()))
        current = self.event(action='current-action')
        self.outbox.retire_superseded(binding=self.binding, current_event_ids=[current])
        self.assertEqual(self.outbox.get(old)['state'], 'EXPIRED')
        self.assertEqual(self.outbox.get(unknown)['state'], 'UNKNOWN')
        self.outbox.retire_superseded(binding=self.binding, current_event_ids=[])
        self.assertIn('UNKNOWN', [r[0] for r in self.db.execute('SELECT state FROM host_wake_events')])

    def test_stable_event_dedup_does_not_rearm_or_extend(self):
        identity = self.event()
        self.assertEqual(self.event(), identity)
        self.dispatch(Queue())
        self.assertEqual(self.event(), identity)
        self.assertEqual(self.outbox.get(identity)['state'], 'ACCEPTED')
        self.now += timedelta(seconds=1)
        with self.assertRaisesRegex(ValueError, 'EVENT_CONFLICT'):
            self.event()

    def test_priority_protection_before_cancel_exit_entry(self):
        for kind in ('entry', 'exit', 'cancel', 'protect'):
            self.event(kind, kind)
        queue = Queue()
        for _ in range(4):
            self.dispatch(queue)
        self.assertEqual([e[0]['kind'] for e in queue.calls], ['protect', 'cancel', 'exit', 'entry'])
        self.assertFalse(self.dispatch(queue)['dispatched'])

    def test_durable_acceptance_is_not_execution_or_broker_effect(self):
        identity = self.event()
        result = self.dispatch(Queue())
        self.assertEqual(result['state'], 'ACCEPTED')
        self.assertFalse(result['execution_authorized'])
        self.assertFalse(result['host_execution_verified'])
        self.assertFalse(result['broker_effect_verified'])
        other = sqlite3.connect(self.path)
        self.addCleanup(other.close)
        self.assertEqual(HostWakeOutbox(other).get(identity)['state'], 'ACCEPTED')

    def test_expired_or_previous_epoch_work_never_dispatches(self):
        first = self.event()
        self.now += timedelta(seconds=31)
        second = self.event(action='action-2')
        self.binding = {**self.binding, 'epoch': str(uuid.uuid4())}
        queue = Queue()
        self.assertFalse(self.dispatch(queue)['dispatched'])
        self.assertEqual(queue.calls, [])
        for identity in (first, second):
            self.assertEqual(self.outbox.get(identity)['state'], 'EXPIRED')

    def test_explicit_reject_retries_bounded_with_same_idempotency_key(self):
        identity = self.event()
        queue = Queue(False)
        self.assertEqual(self.dispatch(queue)['state'], 'RETRY')
        self.assertFalse(self.dispatch(queue)['dispatched'])
        self.now += timedelta(seconds=2)
        self.assertEqual(self.dispatch(queue)['state'], 'RETRY')
        self.now += timedelta(seconds=4)
        self.assertEqual(self.dispatch(queue)['state'], 'FAILED')
        self.now += timedelta(seconds=8)
        self.assertFalse(self.dispatch(queue)['dispatched'])
        self.assertEqual({c[1]['idempotency_key'] for c in queue.calls}, {identity})
        self.assertEqual(len({c[1]['attempt_id'] for c in queue.calls}), 3)

    def test_timeout_is_unknown_no_automatic_retry(self):
        identity = self.event()
        queue = Queue(TimeoutError())
        self.assertEqual(self.dispatch(queue)['state'], 'UNKNOWN')
        self.now += timedelta(seconds=5)
        self.assertFalse(self.dispatch(queue)['dispatched'])
        self.assertEqual(len(queue.calls), 1)
        self.assertEqual(self.outbox.get(identity)['attempts'], 1)

    def test_crash_after_send_recovers_unknown_without_replaying(self):
        identity = self.event()

        class Crash(Queue):
            def enqueue(self, event, **kwargs):
                self.calls.append((event, kwargs))
                raise KeyboardInterrupt()

        queue = Crash()
        with self.assertRaises(KeyboardInterrupt):
            self.dispatch(queue)
        self.assertEqual(self.outbox.get(identity)['state'], 'STARTED')
        self.assertEqual(self.outbox.recover_interrupted(), 1)
        self.assertEqual(self.outbox.get(identity)['state'], 'UNKNOWN')
        self.assertEqual(self.outbox.recover_interrupted(), 0)
        self.assertFalse(self.dispatch(Queue())['dispatched'])

    def test_mismatched_receipt_or_missing_attempt_is_unknown(self):
        class Forged(Queue):
            def enqueue(self, event, **kwargs):
                return {'event_id': 'another-event', 'accepted': True, 'receipt_id': 'fake'}

        self.event()
        self.assertEqual(self.dispatch(Forged())['state'], 'UNKNOWN')

    def test_unqualified_adapter_not_called(self):
        for capability in ({}, {'official_queue': True}, {'official_queue': 1, 'durable_idempotency': True}):
            queue = Queue()
            queue.capability = capability
            self.event(action=uuid.uuid4().hex)
            self.assertEqual(self.dispatch(queue)['state'], 'BLOCKED')
            self.assertEqual(queue.calls, [])

    def test_owner_transaction_cannot_be_silently_committed(self):
        self.db.execute('CREATE TABLE sentinel(value TEXT)')
        self.db.execute("INSERT INTO sentinel VALUES('pending')")
        with self.assertRaisesRegex(ValueError, 'SEPARATE_OWNER_TRANSACTION'):
            self.event()
        self.assertTrue(self.db.in_transaction)
        self.db.rollback()
        self.assertEqual(self.db.execute('SELECT * FROM sentinel').fetchall(), [])

    def test_history_is_append_only_and_retained_after_delivery(self):
        self.event()
        self.dispatch(Queue())
        self.assertEqual([r[0] for r in self.db.execute('SELECT state FROM host_wake_history ORDER BY sequence')],
                         ['PENDING', 'STARTED', 'ACCEPTED'])
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute('DELETE FROM host_wake_history')
        self.db.rollback()

    def test_invalid_identity_or_credential_payload_cannot_enter_event(self):
        with self.assertRaises(ValueError):
            self.outbox.enqueue(binding={**self.binding, 'access_token': 'secret'}, action_id='x',
                                kind='protect', revision='d' * 64, expires_at=self.now.isoformat())
        for kind, revision in [('place', 'd' * 64), ('protect', 'not-a-hash')]:
            with self.assertRaises(ValueError):
                self.event(kind=kind, revision=revision)
