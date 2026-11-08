"""Synthetic tests for the immutable historical baseline core."""
import json
import sqlite3
import tempfile
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase

from entry_authority_fixture import TestExecutionBook as ExecutionBook
from martin_luk.live_session import LiveSupervisor
from test_broker import FakeBroker, order, snapshot
from trading_contracts.values import digest
from trading_infra.attribution import ExecutionAttribution
from trading_infra.historical_baseline import (
    BaselineError, HistoricalBaselineIndex, apply_baseline, plan_baseline,
    status_baseline, validate_baseline)
from trading_infra.instance_accounting import InstanceAccounting
from trading_infra.receipts import ExecutionReceiptImporter


AT = '2026-10-16T14:00:00+00:00'
NOW = datetime.fromisoformat(AT)
EXPIRES = '2026-10-16T14:05:00+00:00'
POLICY = json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())


def evidence(account_key):
    return {
        'source': {'execution_db_sha256': 'a' * 64, 'account_key': account_key,
                   'policy_sha256': 'b' * 64, 'source_fingerprint': 'c' * 64},
        'risk': {'sidecar_sha256': 'd' * 64, 'account_key': account_key,
                 'terms_sha256': 'e' * 64, 'simulation': False},
        'host': {'manifest_sha256': 'f' * 64, 'account_key': account_key,
                 'generation': '1', 'epoch': 'epoch-1'},
        'lineage': {'head_sha256': '1' * 64, 'account_key': account_key,
                    'policy_sha256': 'b' * 64, 'source_fingerprint': 'c' * 64},
    }


def historical_snapshot():
    fake = FakeBroker()
    def raw(*args, **kwargs):
        value = order(*args, **kwargs)
        value['ref_id'] = 'ref-' + value['id']
        return value
    fake.orders = [
        raw('buy-a', 'buy', filled='1', state='filled'),
        raw('sell-a', 'sell', filled='1', state='filled'),
        raw('buy-b', 'buy', filled='1', state='filled'),
        raw('sell-b', 'sell', filled='1', state='filled'),
        raw('buy-c', 'buy', filled='1', state='filled'),
        raw('sell-c', 'sell', filled='1', state='filled'),
        raw('cancel-a', 'sell', filled='0', state='cancelled'),
    ]
    return complete(snapshot(fake, at=AT))


def complete(snap):
    """Add the explicit collections and provenance the baseline requires."""
    scopes = ('options', 'crypto', 'futures', 'event_contracts',
              'fixed_income', 'mutual_funds')
    snap['other_asset_positions'] = {asset: [] for asset in scopes}
    snap['other_asset_orders'] = {asset: [] for asset in scopes}
    snap['other_asset_pending_settlements'] = {asset: [] for asset in scopes}
    def collection(records):
        return {
            'pagination': {'complete': True, 'pages': 1,
                           'source': 'synthetic_test_broker_normalized_v1',
                           'observed_at': AT},
            'record_sha256': digest(records),
            'record_count': len(records),
        }
    snap['provenance'] = {
        'collection_source': 'synthetic_test_broker_normalized_v1',
        'positions_complete': True,
        'positions_pagination_complete': True,
        'orders_complete': True,
        'executions_complete': True,
        'approvals_complete': True,
        'asset_scopes': {
            asset: {
                'positions': collection(snap['other_asset_positions'][asset]),
                'orders': collection(snap['other_asset_orders'][asset]),
                'pending_settlements': collection(
                    snap['other_asset_pending_settlements'][asset]),
            }
            for asset in scopes},
        'snapshot_time_source': 'collector_completed_at_utc',
        'execution_time_source': 'broker_execution_timestamp_utc',
    }
    return snap


class HistoricalBaselineTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db_path = Path(self.temp.name) / 'baseline.sqlite'
        self.db = sqlite3.connect(self.db_path, isolation_level=None)
        self.addCleanup(self.db.close)
        self.snapshot = historical_snapshot()
        self.account_key = self.snapshot['account_key']
        self.evidence = evidence(self.account_key)

    def plan(self, snapshot=None, **changes):
        kwargs = dict(account_key=self.account_key, cutoff_utc=AT,
                      policy_sha256='b' * 64, source_fingerprint='c' * 64,
                      evidence=self.evidence, expires_at=EXPIRES, now=NOW)
        kwargs.update(changes)
        return plan_baseline(snapshot or self.snapshot, **kwargs)

    def tables(self):
        return {row[0] for row in self.db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}

    def test_plan_and_validate_do_not_create_tables_or_write(self):
        manifest = self.plan()
        self.assertNotIn('historical_baseline_schema', self.tables())
        result = validate_baseline(self.db, manifest,
                                   manifest_pin=manifest['manifest_sha256'], now=NOW,
                                   evidence=self.evidence, snapshot=self.snapshot)
        self.assertEqual(result['status'], 'VALIDATED')
        self.assertNotIn('historical_baseline_schema', self.tables())
        self.assertEqual(manifest['counts'], {'orders': 7, 'executions': 6})
        self.assertEqual(len(manifest['orders']), 7)
        self.assertEqual(len(manifest['executions']), 6)

    def test_apply_is_atomic_idempotent_and_writes_no_live_ownership(self):
        manifest = self.plan()
        result = apply_baseline(self.db, manifest,
                                manifest_pin=manifest['manifest_sha256'],
                                approval_scope='test-scope', now=NOW,
                       evidence=self.evidence, snapshot=self.snapshot)
        self.assertEqual(result['status'], 'APPLIED')
        self.assertEqual(status_baseline(self.db)['status'], 'PRESENT')
        duplicate = apply_baseline(self.db, manifest,
                                   manifest_pin=manifest['manifest_sha256'],
                                   approval_scope='test-scope', now=NOW,
                       evidence=self.evidence, snapshot=self.snapshot)
        self.assertTrue(duplicate['duplicate'])
        for table in ('instance_fills', 'execution_fill_identity',
                      'execution_attribution', 'execution_identity'):
            if table in self.tables():
                self.assertEqual(self.db.execute('SELECT count(*) FROM '+table).fetchone()[0], 0)
        self.assertFalse(result['live_enabled'])
        self.assertFalse(result['production_ready'])

    def test_match_is_exact_and_revision_or_new_execution_is_conflict(self):
        manifest = self.plan()
        apply_baseline(self.db, manifest, manifest_pin=manifest['manifest_sha256'],
                       approval_scope='test-scope', now=NOW,
                       evidence=self.evidence, snapshot=self.snapshot)
        index = HistoricalBaselineIndex(self.db)
        first = self.snapshot['orders']['buy-a']
        self.assertEqual(index.match_order(first, self.account_key)['status'], 'matched')
        revised = deepcopy(first)
        revised['filled'] = '0'
        revised['executions'] = []
        self.assertEqual(index.match_order(revised, self.account_key)['status'], 'conflict')
        extra = deepcopy(first)
        extra['id'] = 'live-owner'
        extra['ref_id'] = first.get('ref_id')
        self.assertEqual(index.match_order(extra, self.account_key)['status'], 'conflict')

    def test_receipt_importer_baseline_match_does_not_create_owner_or_fill(self):
        manifest = self.plan()
        apply_baseline(self.db, manifest, manifest_pin=manifest['manifest_sha256'],
                       approval_scope='test-scope', now=NOW,
                       evidence=self.evidence, snapshot=self.snapshot)
        attribution = ExecutionAttribution(self.db, clock=lambda: NOW)
        accounting = InstanceAccounting(self.db, clock=lambda: NOW)
        importer = ExecutionReceiptImporter(
            self.db, attribution=attribution, accounting=accounting,
            clock=lambda: NOW, baseline=HistoricalBaselineIndex(self.db))
        result = importer.import_snapshot(self.snapshot)
        self.assertEqual(len(result['baseline_matched']), 7)
        self.assertEqual(result['quarantined'], [])
        self.assertEqual(result['imported'], [])
        self.assertEqual(self.db.execute('SELECT count(*) FROM instance_fills').fetchone()[0], 0)
        self.assertEqual(self.db.execute('SELECT count(*) FROM execution_attribution').fetchone()[0], 0)

        with_new = deepcopy(self.snapshot)
        with_new['orders']['new-unknown'] = order('new-unknown', 'buy', filled='1',
                                                   state='filled')
        result = importer.import_snapshot(with_new)
        self.assertEqual(len(result['baseline_matched']), 7)
        self.assertEqual([item['reason'] for item in result['quarantined']],
                         ['RECEIPT_OWNER_UNKNOWN'])

    def test_live_supervisor_default_importer_accepts_existing_baseline(self):
        manifest = self.plan()
        apply_baseline(self.db, manifest, manifest_pin=manifest['manifest_sha256'],
                       approval_scope='test-scope', now=NOW,
                       evidence=self.evidence, snapshot=self.snapshot)
        # Avoid a second connection: the supervisor's owner uses this database
        # through an ExecutionBook with the same policy.
        book = ExecutionBook(self.db_path, POLICY, simulation=True, clock=lambda: NOW)
        self.addCleanup(book.close)
        attribution = ExecutionAttribution(book.db, clock=lambda: NOW)
        accounting = InstanceAccounting(book.db, clock=lambda: NOW)
        supervisor = LiveSupervisor(
            book, admission=SimpleNamespace(attribution=attribution),
            accounting=accounting)
        errors = []
        result = supervisor._import_receipts(self.snapshot, errors)
        self.assertEqual(result['baseline_matched'], 7)
        self.assertEqual(result['quarantined'], 0)
        self.assertEqual(errors, [])
        self.assertFalse(supervisor._receipts_incomplete)

        with_new = deepcopy(self.snapshot)
        with_new['orders']['new-unknown'] = order('new-unknown', 'buy', filled='1',
                                                   state='filled')
        errors = []
        result = supervisor._import_receipts(with_new, errors)
        self.assertEqual(result['baseline_matched'], 7)
        self.assertEqual(result['quarantined'], 1)
        self.assertIn('RECEIPT_OWNER_UNKNOWN',
                      [item.get('reason') for item in errors])
        self.assertTrue(supervisor._receipts_incomplete)

    def test_exact_apply_replay_after_plan_expiry_is_idempotent(self):
        manifest = self.plan()
        apply_baseline(self.db, manifest, manifest_pin=manifest['manifest_sha256'],
                       approval_scope='test-scope', now=NOW,
                       evidence=self.evidence, snapshot=self.snapshot)
        later = datetime.fromisoformat(EXPIRES) + timedelta(hours=1)
        result = apply_baseline(self.db, manifest, manifest_pin=manifest['manifest_sha256'],
                                approval_scope='test-scope', now=later)
        self.assertTrue(result['duplicate'])

    def test_wrong_pin_or_expired_plan_is_rejected(self):
        manifest = self.plan()
        with self.assertRaisesRegex(BaselineError, 'BASELINE_MANIFEST_PIN_MISMATCH'):
            validate_baseline(self.db, manifest, manifest_pin='0' * 64, now=NOW)
        with self.assertRaisesRegex(BaselineError, 'BASELINE_PLAN_EXPIRED'):
            self.plan(expires_at='2026-10-16T13:59:00+00:00')

    def test_invalid_inputs_are_explicitly_rejected(self):
        cases = []
        wrong_account = deepcopy(self.snapshot)
        wrong_account['account_key'] = 'other-account'
        cases.append(('account', wrong_account, {}))
        cases.append(('provider', self.snapshot, {'provider': 'other-provider'}))
        cases.append(('stale', self.snapshot, {
            'now': datetime.fromisoformat(AT) + timedelta(seconds=31)}))
        missing = deepcopy(self.snapshot)
        missing.pop('other_asset_values')
        cases.append(('cross-asset', missing, {}))
        open_order = deepcopy(self.snapshot)
        open_order['orders']['buy-a']['state'] = 'confirmed'
        cases.append(('open', open_order, {}))
        position = deepcopy(self.snapshot)
        position['positions'] = {'DEMO': {'quantity': '1'}}
        cases.append(('position', position, {}))
        approval = deepcopy(self.snapshot)
        approval['pending_approvals'] = 1
        cases.append(('approval', approval, {}))
        for label, snap, changes in cases:
            with self.subTest(label=label), self.assertRaises(BaselineError):
                self.plan(snapshot=snap, **changes)

    def test_risk_or_host_plan_change_is_rejected_before_apply(self):
        manifest = self.plan()
        changed = deepcopy(self.evidence)
        changed['risk']['sidecar_sha256'] = '2' * 64
        with self.assertRaisesRegex(BaselineError, 'BASELINE_EVIDENCE_CHANGED'):
            validate_baseline(self.db, manifest,
                              manifest_pin=manifest['manifest_sha256'], now=NOW,
                              evidence=changed, snapshot=self.snapshot)

    def test_revision_and_late_execution_are_baseline_conflicts(self):
        manifest = self.plan()
        apply_baseline(self.db, manifest, manifest_pin=manifest['manifest_sha256'],
                       approval_scope='test-scope', now=NOW,
                       evidence=self.evidence, snapshot=self.snapshot)
        attribution = ExecutionAttribution(self.db, clock=lambda: NOW)
        accounting = InstanceAccounting(self.db, clock=lambda: NOW)
        importer = ExecutionReceiptImporter(
            self.db, attribution=attribution, accounting=accounting,
            clock=lambda: NOW, baseline=HistoricalBaselineIndex(self.db))
        revised = deepcopy(self.snapshot)
        revised['orders']['buy-a']['executions'][0]['price'] = '41'
        result = importer.import_snapshot(revised)
        self.assertIn('BASELINE_ORDER_REVISED',
                      [item['reason'] for item in result['quarantined']])
        late = deepcopy(self.snapshot)
        late['orders']['buy-a']['executions'].append(
            {'id': 'late-exec', 'order_id': 'buy-a', 'side': 'buy',
             'quantity': '1', 'price': '40', 'fees': '0',
             'executed_at': AT})
        result = importer.import_snapshot(late)
        self.assertIn('BASELINE_ORDER_REVISED',
                      [item['reason'] for item in result['quarantined']])

    def test_accepted_baseline_survives_unrelated_risk_table_change(self):
        manifest = self.plan()
        apply_baseline(self.db, manifest, manifest_pin=manifest['manifest_sha256'],
                       approval_scope='test-scope', now=NOW,
                       evidence=self.evidence, snapshot=self.snapshot)
        self.db.execute(
            'CREATE TABLE IF NOT EXISTS account_risk(id INTEGER PRIMARY KEY, payload TEXT)')
        self.db.execute('INSERT OR REPLACE INTO account_risk VALUES(1,?)', ('changed',))
        index = HistoricalBaselineIndex(self.db)
        self.assertEqual(index.match_order(self.snapshot['orders']['buy-a'],
                                           self.account_key)['status'], 'matched')
        self.assertEqual(status_baseline(self.db)['status'], 'PRESENT')

    def test_live_owner_takeover_after_baseline_is_a_conflict(self):
        manifest = self.plan()
        apply_baseline(self.db, manifest, manifest_pin=manifest['manifest_sha256'],
                       approval_scope='test-scope', now=NOW,
                       evidence=self.evidence, snapshot=self.snapshot)
        book = ExecutionBook(self.db_path, POLICY, simulation=True, clock=lambda: NOW)
        self.addCleanup(book.close)
        attribution = ExecutionAttribution(book.db, clock=lambda: NOW)
        attribution.bind('ref-buy-a', 'alpha', 'trade-a')
        accounting = InstanceAccounting(book.db, clock=lambda: NOW)
        supervisor = LiveSupervisor(
            book, admission=SimpleNamespace(attribution=attribution),
            accounting=accounting)
        errors = []
        result = supervisor._import_receipts(self.snapshot, errors)
        self.assertEqual(result['baseline_matched'], 6)
        self.assertIn('BASELINE_LIVE_OWNER_CONFLICT',
                      [item['reason'] for item in errors])

    def test_existing_identity_conflict_still_blocks_with_baseline(self):
        manifest = self.plan()
        apply_baseline(self.db, manifest, manifest_pin=manifest['manifest_sha256'],
                       approval_scope='test-scope', now=NOW,
                       evidence=self.evidence, snapshot=self.snapshot)
        book = ExecutionBook(self.db_path, POLICY, simulation=True, clock=lambda: NOW)
        self.addCleanup(book.close)
        attribution = ExecutionAttribution(book.db, clock=lambda: NOW)
        accounting = InstanceAccounting(book.db, clock=lambda: NOW)
        supervisor = LiveSupervisor(
            book, admission=SimpleNamespace(attribution=attribution),
            accounting=accounting)
        book.db.execute(
            'INSERT INTO execution_identity_conflict(conflict_id,identity_key,provider,'
            'account_key,execution_id,first_seen_at) VALUES(?,?,?,?,?,?)',
            ('conflict-test', 'key-test', 'robinhood', self.account_key,
             'exec-conflict', AT))
        book.db.commit()
        errors = []
        result = supervisor._import_receipts(self.snapshot, errors)
        self.assertEqual(result['baseline_matched'], 7)
        self.assertIn('RECEIPT_IDENTITY_CONFLICT',
                      [item['reason'] for item in errors])
