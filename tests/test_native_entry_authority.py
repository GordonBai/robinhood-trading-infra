"""Native entry review/place must use the same durable/current authority."""
import json
from unittest import TestCase

from martin_agentic.entry_authority import DurableEntryAuthority
from martin_agentic.native_operations import NativeOperations
import test_execution as execution_module


class DenyAuthority:
    def __init__(self):
        self.calls = 0

    def check(self, **kwargs):
        self.calls += 1
        return {'allowed': False, 'reason': 'PROBE_AUTHORITY_DENIED'}

    def require_entry(self, **kwargs):
        raise ValueError('ENTRY_AUTHORITY_BLOCKED:PROBE_AUTHORITY_DENIED')


class DenyPreflight:
    def __init__(self):
        self.calls = 0

    def check_claim(self, book, action):
        self.calls += 1
        return {'allowed': False, 'reasons': ['PROBE_PREFLIGHT_DENIED']}

    def check_proposal(self, book, snapshot, quote, plan, *, purpose, evidence,
                       candidate_action_id=None):
        self.calls += 1
        return {'allowed': False, 'reasons': ['PROBE_PREFLIGHT_DENIED']}


class NativeEntryAuthorityTests(TestCase):
    def setUp(self):
        self.f = execution_module.ExecutionTests('test_empty_queue_has_zero_agent_calls')
        self.f.setUp()
        self.addCleanup(self.f.tearDown)
        self.book = self.f.book
        self.plan = dict(self.f.plan)
        self.quote = dict(self.f.quote)
        self.snapshot = self.f.snap()
        self.action = self.book.propose(
            'engineering-native-entry-authority', self.plan, self.quote, self.snapshot,
            purpose='engineering',
            evidence={'synthetic': True, 'source_sha256': 'engineering-native-entry-authority',
                      'engineering_scope_authorized': True,
                      'parameters_user_specified': True})['action_id']
        self.native = NativeOperations(self.book)

    def review(self):
        packet = self.native.begin(self.action, 'review', self.snapshot,
                                   quote=self.quote)
        response = {'data': {**packet['parameters'], 'order_checks': {},
                             'customer_approval_required': False,
                             'market_data_disclosure': 'fixture',
                             'quote_data': {}}}
        self.native.record(packet['operation_id'], response)
        return packet

    def counts(self):
        return {
            'place_intents': self.book.db.execute(
                "SELECT count(*) FROM external_operations WHERE operation='place'"
            ).fetchone()[0],
            'place_started': self.book.db.execute(
                "SELECT count(*) FROM native_operation_records"
                " WHERE operation='place' AND state='STARTED'"
            ).fetchone()[0],
            'action_state': next(a['state'] for a in self.book.actions()
                                 if a['id'] == self.action),
        }

    def test_valid_synthetic_review_then_denied_authority_blocks_place(self):
        self.review()
        before = self.counts()
        guard = DenyAuthority()
        self.book._entry_authority = guard
        with self.assertRaisesRegex(ValueError, 'ENTRY_AUTHORITY_BLOCKED'):
            self.native.begin(self.action, 'place', self.snapshot, quote=self.quote)
        self.assertGreaterEqual(guard.calls, 1)
        self.assertEqual(self.counts(), before)
        self.assertEqual(before['place_intents'], 0)
        self.assertEqual(before['place_started'], 0)

    def test_review_is_refused_when_durable_authority_is_denied(self):
        self.book._entry_authority = DenyAuthority()
        with self.assertRaisesRegex(ValueError, 'ENTRY_AUTHORITY_BLOCKED'):
            self.native.begin(self.action, 'review', self.snapshot, quote=self.quote)
        self.assertEqual(self.counts()['place_intents'], 0)
        self.assertEqual(self.book.db.execute(
            "SELECT count(*) FROM external_operations WHERE operation='review'"
        ).fetchone()[0], 0)
        self.assertEqual(self.book.db.execute(
            "SELECT count(*) FROM native_operation_records WHERE operation='review'"
        ).fetchone()[0], 0)

    def test_current_preflight_denial_blocks_place_after_valid_review(self):
        self.review()
        before = self.counts()
        preflight = DenyPreflight()
        self.book._current_preflight = preflight
        with self.assertRaisesRegex(ValueError, 'CURRENT_ENGINEERING_PREFLIGHT_BLOCKED'):
            self.native.begin(self.action, 'place', self.snapshot, quote=self.quote)
        self.assertGreaterEqual(preflight.calls, 1)
        self.assertEqual(self.counts(), before)

    def test_final_receipt_binding_drift_blocks_place(self):
        self.review()
        before = self.counts()
        self.book._preflight_binding_ok = lambda *args, **kwargs: False
        with self.assertRaisesRegex(ValueError, 'CURRENT_ENGINEERING_PREFLIGHT_BINDING_CHANGED'):
            self.native.begin(self.action, 'place', self.snapshot, quote=self.quote)
        self.assertEqual(self.counts(), before)

    def test_identity_only_and_revoked_markers_block_native_place(self):
        self.review()
        table = 'entry_authority_marker'
        self.book.db.execute(
            'CREATE TABLE IF NOT EXISTS ' + table +
            '(required INTEGER,reason TEXT,baseline_id TEXT,baseline_scope TEXT,created_at TEXT)')
        for reason, scope, label in (
                ('BASELINE_REVOKED', 'terminal_equity_order_identity_v1', 'IDENTITY_ONLY_REVOKED'),
                ('IDENTITY_ONLY', 'terminal_equity_order_identity_v1', 'IDENTITY_ONLY')):
            with self.subTest(scope=scope, reason=reason):
                self.book.db.execute('DELETE FROM ' + table)
                self.book.db.execute('INSERT INTO ' + table + ' VALUES(1,?,?,?,?)',
                                     (reason, 'logical-baseline', scope,
                                      '2026-10-16T14:00:00+00:00'))
                self.book.db.commit()
                self.book._entry_authority = DurableEntryAuthority(self.book.db)
                before = self.counts()
                with self.assertRaisesRegex(ValueError, 'ENTRY_AUTHORITY_BLOCKED'):
                    self.native.begin(self.action, 'place', self.snapshot,
                                      quote=self.quote)
                self.assertEqual(self.counts(), before)

    def test_missing_namespace_real_durable_blocks_native_place(self):
        self.review()
        before = self.counts()
        self.book._entry_authority = DurableEntryAuthority(self.book.db)
        with self.assertRaisesRegex(ValueError, 'ENTRY_AUTHORITY_NAMESPACE_ABSENT'):
            self.native.begin(self.action, 'place', self.snapshot, quote=self.quote)
        self.assertEqual(self.counts(), before)

    def test_protection_and_cancel_are_not_blocked_by_entry_authority(self):
        self.review()
        self.book._entry_authority = DenyAuthority()
        # This fixture has no filled position yet; a direct protection action
        # remains a management path and must reach the ordinary quantity/state
        # checks rather than the entry authority.
        trade = self.book.state()['trades']['engineering-native-entry-authority']
        protect = self.book._queue(trade, 'protect',
                                   dict(symbol='DEMO', side='sell', quantity='1'),
                                   self.f.now, 'fixture safety')
        self.book.db.commit()
        self.assertIsNotNone(protect)
        self.assertEqual(self.book.db.execute(
            "SELECT count(*) FROM external_operations WHERE operation='place'"
        ).fetchone()[0], 0)
