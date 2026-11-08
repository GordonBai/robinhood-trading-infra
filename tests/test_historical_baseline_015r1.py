"""015r1 regression boundaries for the historical baseline."""
import json
import sqlite3
import tempfile
from copy import deepcopy
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from entry_authority_fixture import TestExecutionBook as ExecutionBook
from martin_luk.exits import exit_decision
from martin_luk.live_session import LiveSupervisor
from test_broker import FakeBroker, order, position, snapshot
from test_historical_baseline import (
    AT, EXPIRES, NOW, POLICY, evidence, historical_snapshot)
from trading_contracts.values import canonical, digest
from trading_infra import historical_baseline as baseline_module
from trading_infra.attribution import ExecutionAttribution
from trading_infra.historical_baseline import (
    BaselineError, HistoricalBaselineIndex, apply_baseline, plan_baseline,
    status_baseline, validate_baseline)
from trading_infra.instance_accounting import InstanceAccounting
from trading_infra.receipts import ExecutionReceiptImporter


class BoundaryTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db_path = Path(self.temp.name) / 'baseline.sqlite'
        self.db = sqlite3.connect(self.db_path, isolation_level=None)
        self.addCleanup(self.db.close)
        self.snapshot = historical_snapshot()
        self.account_key = self.snapshot['account_key']
        self.evidence = evidence(self.account_key)

    def new_db(self, name='other.sqlite'):
        db = sqlite3.connect(Path(self.temp.name) / name, isolation_level=None)
        self.addCleanup(db.close)
        return db

    def plan(self, snapshot=None, **changes):
        kwargs = dict(account_key=self.account_key, cutoff_utc=AT,
                      policy_sha256='b' * 64, source_fingerprint='c' * 64,
                      evidence=self.evidence, expires_at=EXPIRES, now=NOW)
        kwargs.update(changes)
        return plan_baseline(snapshot or self.snapshot, **kwargs)

    def apply(self, manifest=None, *, db=None, snapshot=None, **changes):
        manifest = manifest or self.plan()
        db = db or self.db
        kwargs = dict(manifest_pin=manifest['manifest_sha256'],
                      approval_scope='test-scope', now=NOW,
                      evidence=self.evidence, snapshot=snapshot or self.snapshot)
        kwargs.update(changes)
        return apply_baseline(db, manifest, **kwargs)

    def tables(self, db=None):
        db = db or self.db
        return {row[0] for row in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}

    def test_new_validate_and_apply_require_current_snapshot_and_evidence(self):
        manifest = self.plan()
        with self.assertRaisesRegex(BaselineError, 'BASELINE_CURRENT_INPUTS_REQUIRED'):
            validate_baseline(self.db, manifest,
                              manifest_pin=manifest['manifest_sha256'], now=NOW)
        with self.assertRaisesRegex(BaselineError, 'BASELINE_CURRENT_INPUTS_REQUIRED'):
            apply_baseline(self.db, manifest,
                           manifest_pin=manifest['manifest_sha256'],
                           approval_scope='test-scope', now=NOW)
        with self.assertRaisesRegex(BaselineError, 'BASELINE_CURRENT_INPUTS_REQUIRED'):
            apply_baseline(self.db, manifest,
                           manifest_pin=manifest['manifest_sha256'],
                           approval_scope='test-scope', now=NOW,
                           evidence=self.evidence)
        self.assertNotIn('historical_baseline_schema', self.tables())

        # An old plan cannot use its still-future expiry to bypass freshness.
        with self.assertRaisesRegex(BaselineError, 'BASELINE_SNAPSHOT_STALE_OR_CUTOFF'):
            self.apply(manifest, now=NOW + timedelta(seconds=31))
        self.assertNotIn('historical_baseline_schema', self.tables())

    def test_future_observation_is_rejected(self):
        snapshot = deepcopy(self.snapshot)
        snapshot['observed_at'] = (NOW + timedelta(seconds=1)).isoformat()
        with self.assertRaisesRegex(BaselineError, 'BASELINE_SNAPSHOT_TIME_INVALID'):
            self.plan(snapshot=snapshot)

    def test_missing_position_quantity_or_held_is_rejected(self):
        for positions, code, expected_code in (
                ({'SOFI': {}}, 'BASELINE_POSITION_QUANTITY_MISSING',
                 'CONDITIONS_MISSING'),
                ({'SOFI': {'quantity': '0'}}, 'BASELINE_POSITION_HELD_MISSING',
                 'CONDITIONS_MISSING'),
                ({'SOFI': {'quantity': '0', 'shares_held_for_sells': '1'}},
                 'BASELINE_POSITION_HELD', 'BASELINE_POSITION_HELD')):
            snapshot = deepcopy(self.snapshot)
            snapshot['positions'] = positions
            with self.subTest(code=code), self.assertRaisesRegex(BaselineError, code) as raised:
                self.plan(snapshot=snapshot)
            self.assertEqual(raised.exception.code, expected_code)

    def test_missing_collection_provenance_is_rejected(self):
        for mutate, code in (
                (lambda snap: snap.pop('provenance'), 'BASELINE_SNAPSHOT_MISSING:provenance'),
                (lambda snap: snap['provenance'].pop('execution_time_source'),
                 'BASELINE_PROVENANCE_INCOMPLETE'),
                (lambda snap: snap['provenance'].__setitem__(
                    'asset_scopes', {'equity': {}}), 'BASELINE_CROSS_ASSET_INCOMPLETE')):
            snapshot = deepcopy(self.snapshot)
            mutate(snapshot)
            with self.subTest(code=code), self.assertRaisesRegex(BaselineError, code) as raised:
                self.plan(snapshot=snapshot)
            self.assertEqual(raised.exception.code, 'CONDITIONS_MISSING')

    def test_schema_version_partial_and_multiple_active_fail_closed(self):
        manifest = self.plan()
        self.apply(manifest)
        index = HistoricalBaselineIndex(self.db)
        self.assertEqual(index.match_order(self.snapshot['orders']['buy-a'],
                                           self.account_key)['status'], 'matched')
        self.db.execute('UPDATE historical_baseline_schema SET version=99')
        with self.assertRaisesRegex(BaselineError,
                                    'BASELINE_SCHEMA_VERSION_UNSUPPORTED'):
            self.apply(manifest)
        self.assertEqual(index.match_order(self.snapshot['orders']['buy-a'],
                                           self.account_key),
                         {'status': 'conflict',
                          'reason': 'BASELINE_SCHEMA_VERSION_UNSUPPORTED'})
        self.assertEqual(status_baseline(self.db)['status'], 'INVALID')
        importer = ExecutionReceiptImporter(
            self.db, attribution=ExecutionAttribution(self.db, clock=lambda: NOW),
            accounting=InstanceAccounting(self.db, clock=lambda: NOW),
            clock=lambda: NOW, baseline=HistoricalBaselineIndex(self.db))
        result = importer.import_snapshot(self.snapshot)
        self.assertEqual(result['baseline_matched'], [])
        self.assertEqual({item['reason'] for item in result['quarantined']},
                         {'BASELINE_SCHEMA_VERSION_UNSUPPORTED'})

        partial = self.new_db('partial.sqlite')
        self.apply(db=partial)
        partial.execute('DROP TABLE historical_baseline_order')
        self.assertEqual(status_baseline(partial)['status'], 'INVALID')
        self.assertEqual(HistoricalBaselineIndex(partial).match_order(
            self.snapshot['orders']['buy-a'], self.account_key),
            {'status': 'conflict', 'reason': 'BASELINE_SCHEMA_PARTIAL'})

        multi = self.new_db('multi.sqlite')
        self.apply(db=multi)
        row = multi.execute('SELECT * FROM historical_baseline_manifest').fetchone()
        values = list(row)
        values[0] = 'second-active'
        values[1] = row[1] + 1
        multi.execute(
            'INSERT INTO historical_baseline_manifest VALUES('
            + ','.join('?' for _ in values) + ')', values)
        self.assertEqual(HistoricalBaselineIndex(multi).match_order(
            self.snapshot['orders']['buy-a'], self.account_key),
            {'status': 'conflict', 'reason': 'BASELINE_MULTIPLE_ACTIVE'})

        corrupt = self.new_db('corrupt.sqlite')
        self.apply(db=corrupt)
        corrupt.execute(
            "UPDATE historical_baseline_manifest SET manifest_json='{}'")
        self.assertEqual(status_baseline(corrupt)['status'], 'INVALID')
        self.assertEqual(HistoricalBaselineIndex(corrupt).match_order(
            self.snapshot['orders']['buy-a'], self.account_key),
            {'status': 'conflict', 'reason': 'BASELINE_INDEX_CORRUPT'})

    def test_future_and_partial_schema_block_real_supervisor(self):
        for name, mutate, reason in (
                ('future', lambda db: db.execute(
                    'UPDATE historical_baseline_schema SET version=99'),
                 'BASELINE_SCHEMA_VERSION_UNSUPPORTED'),
                ('partial', lambda db: db.execute(
                    'DROP TABLE historical_baseline_order'),
                 'BASELINE_SCHEMA_PARTIAL'),
                ('materialized', lambda db: db.execute(
                    "UPDATE historical_baseline_order SET order_json='{}',"
                    "canonical_order_sha256='" + '0' * 64 + "'"),
                 'BASELINE_INDEX_CORRUPT')):
            db = self.new_db(name + '.sqlite')
            self.apply(db=db)
            book_path = Path(db.execute('PRAGMA database_list').fetchone()[2])
            book = ExecutionBook(book_path, POLICY, simulation=True, clock=lambda: NOW)
            self.addCleanup(book.close)
            supervisor = LiveSupervisor(
                book,
                admission=SimpleNamespace(
                    attribution=ExecutionAttribution(book.db, clock=lambda: NOW)),
                accounting=InstanceAccounting(book.db, clock=lambda: NOW))
            mutate(book.db)
            errors = []
            result = supervisor._import_receipts(self.snapshot, errors)
            self.assertEqual(result['baseline_matched'], 0, name)
            self.assertEqual(result['quarantined'], 7, name)
            self.assertIn(reason, [item['reason'] for item in errors], name)
            self.assertTrue(supervisor._receipts_incomplete, name)

    def test_lock_wait_rechecks_freshness_under_write_lock(self):
        manifest = self.plan()
        times = iter([NOW, NOW + timedelta(seconds=31)])
        with self.assertRaisesRegex(BaselineError, 'BASELINE_SNAPSHOT_STALE_OR_CUTOFF'):
            apply_baseline(self.db, manifest,
                           manifest_pin=manifest['manifest_sha256'],
                           approval_scope='test-scope', clock=lambda: next(times),
                           evidence=self.evidence, snapshot=self.snapshot)
        self.assertNotIn('historical_baseline_schema', self.tables())

    def test_caller_rollback_and_crash_before_store_leave_no_baseline(self):
        manifest = self.plan()
        self.db.execute('BEGIN IMMEDIATE')
        self.apply(manifest)
        self.db.execute('ROLLBACK')
        self.assertNotIn('historical_baseline_schema', self.tables())
        self.assertEqual(status_baseline(self.db)['status'], 'ABSENT')

        with patch('trading_infra.historical_baseline._store_manifest',
                   side_effect=RuntimeError('simulated crash before commit')):
            with self.assertRaisesRegex(RuntimeError, 'simulated crash'):
                self.apply(manifest)
        self.assertNotIn('historical_baseline_schema', self.tables())
        self.assertEqual(status_baseline(self.db)['status'], 'ABSENT')

        original_store = baseline_module._store_manifest
        def crash_after_store(db, manifest, **kwargs):
            original_store(db, manifest, **kwargs)
            raise RuntimeError('simulated crash after store')
        with patch('trading_infra.historical_baseline._store_manifest',
                   side_effect=crash_after_store):
            with self.assertRaisesRegex(RuntimeError, 'simulated crash after store'):
                self.apply(manifest)
        self.assertNotIn('historical_baseline_schema', self.tables())
        self.assertEqual(status_baseline(self.db)['status'], 'ABSENT')

    def test_sqlite_schema_read_error_is_not_reported_absent(self):
        self.apply()
        self.db.close()
        self.assertEqual(status_baseline(self.db)['status'], 'INVALID')
        self.assertEqual(
            HistoricalBaselineIndex(self.db).match_order(
                self.snapshot['orders']['buy-a'], self.account_key),
            {'status': 'conflict', 'reason': 'BASELINE_SCHEMA_READ_FAILED'})

    def test_global_live_state_blocks_new_apply(self):
        cases = (
            ('live_actions', "CREATE TABLE live_actions(state TEXT)",
             "INSERT INTO live_actions VALUES('UNKNOWN')",
             'BASELINE_GLOBAL_ACTION_UNRESOLVED'),
            ('execution_intents', 'CREATE TABLE execution_intents(state TEXT)',
             "INSERT INTO execution_intents VALUES('PENDING')",
             'BASELINE_GLOBAL_EXECUTION_PENDING'),
            ('receipt_pending_executions',
             'CREATE TABLE receipt_pending_executions(order_id TEXT)',
             "INSERT INTO receipt_pending_executions VALUES('o')",
             'BASELINE_GLOBAL_EXECUTION_PENDING'),
            ('external_operations',
             'CREATE TABLE external_operations(report TEXT)',
             'INSERT INTO external_operations VALUES(NULL)',
             'BASELINE_GLOBAL_OPERATION_PENDING'),
            ('native_operation_records',
             'CREATE TABLE native_operation_records(state TEXT)',
             "INSERT INTO native_operation_records VALUES('STARTED')",
             'BASELINE_GLOBAL_OPERATION_PENDING'),
            ('symbol_leases', 'CREATE TABLE symbol_leases(symbol TEXT)',
             "INSERT INTO symbol_leases VALUES('SOFI')",
             'BASELINE_GLOBAL_LEASE_ACTIVE'),
            ('receipt_order_progress',
             'CREATE TABLE receipt_order_progress(terminal INTEGER)',
             'INSERT INTO receipt_order_progress VALUES(0)',
             'BASELINE_GLOBAL_TRADE_ACTIVE'),
            ('execution_identity_conflict',
             'CREATE TABLE execution_identity_conflict(id TEXT)',
             "INSERT INTO execution_identity_conflict VALUES('c')",
             'BASELINE_GLOBAL_IDENTITY_CONFLICT'),
            ('receipt_identity_quarantine',
             'CREATE TABLE receipt_identity_quarantine(id TEXT)',
             "INSERT INTO receipt_identity_quarantine VALUES('q')",
             'BASELINE_GLOBAL_IDENTITY_QUARANTINE'),
            ('execution_integrity_faults',
             'CREATE TABLE execution_integrity_faults(id TEXT)',
             "INSERT INTO execution_integrity_faults VALUES('f')",
             'BASELINE_GLOBAL_INTEGRITY_FAULT'),
            ('receipt_identity_migration',
             'CREATE TABLE receipt_identity_migration(version INTEGER)',
             'INSERT INTO receipt_identity_migration VALUES(2)',
             'BASELINE_GLOBAL_IDENTITY_MIGRATION'),
        )
        for table, schema, insert, code in cases:
            with self.subTest(table=table):
                db = self.new_db(table + '.sqlite')
                db.execute(schema)
                db.execute(insert)
                with self.assertRaisesRegex(BaselineError, code):
                    self.apply(db=db)
                self.assertNotIn('historical_baseline_schema', self.tables(db))

        # receipt_quarantine is an audit queue awaiting baseline classification,
        # not a reason to delete or block an otherwise safe baseline.
        db = self.new_db('quarantine.sqlite')
        db.execute('CREATE TABLE receipt_quarantine(reason TEXT)')
        db.execute("INSERT INTO receipt_quarantine VALUES('RECEIPT_REFERENCE_UNBOUND')")
        self.assertTrue(self.apply(db=db)['status'] == 'APPLIED')
        self.assertEqual(db.execute(
            'SELECT count(*) FROM receipt_quarantine').fetchone()[0], 1)

    def test_materialized_rows_are_bound_to_pinned_manifest(self):
        manifest = self.plan()
        self.apply(manifest)
        row = self.db.execute(
            "SELECT order_key,order_json FROM historical_baseline_order "
            "WHERE order_id='buy-a'").fetchone()
        body = json.loads(row[1])
        body['executions'][0]['price'] = '1'
        self.db.execute(
            'UPDATE historical_baseline_order SET order_json=?,'
            'canonical_order_sha256=? WHERE order_key=?',
            (canonical(body), digest(body), row[0]))
        self.assertEqual(status_baseline(self.db)['status'], 'INVALID')
        self.assertEqual(HistoricalBaselineIndex(self.db).match_order(
            self.snapshot['orders']['buy-a'], self.account_key),
            {'status': 'conflict', 'reason': 'BASELINE_INDEX_CORRUPT'})
        importer = ExecutionReceiptImporter(
            self.db, attribution=ExecutionAttribution(self.db, clock=lambda: NOW),
            accounting=InstanceAccounting(self.db, clock=lambda: NOW),
            clock=lambda: NOW, baseline=HistoricalBaselineIndex(self.db))
        result = importer.import_snapshot(self.snapshot)
        self.assertEqual(result['baseline_matched'], [])
        self.assertEqual({item['reason'] for item in result['quarantined']},
                         {'BASELINE_INDEX_CORRUPT'})

    def test_materialized_missing_extra_orphan_and_manifest_column_fail_closed(self):
        for name, mutate in (
                ('missing-order', lambda db: db.execute(
                    "DELETE FROM historical_baseline_order WHERE order_id='buy-a'")),
                ('extra-order', lambda db: db.execute(
                    "INSERT INTO historical_baseline_order SELECT baseline_id,"
                    "'extra', 'extra', NULL, account_key, provider,"
                    "canonical_order_sha256, executions_sha256, order_json "
                    "FROM historical_baseline_order WHERE order_id='buy-a'")),
                ('orphan-order', lambda db: db.execute(
                    "INSERT INTO historical_baseline_order SELECT 'orphan',"
                    "'orphan', 'orphan', NULL, account_key, provider,"
                    "canonical_order_sha256, executions_sha256, order_json "
                    "FROM historical_baseline_order WHERE order_id='buy-a'")),
                ('manifest-column', lambda db: db.execute(
                    "UPDATE historical_baseline_manifest SET account_key='other'")),
                ('missing-event', lambda db: db.execute(
                    'DELETE FROM historical_baseline_event')),
                ('missing-balance', lambda db: db.execute(
                    "DELETE FROM historical_baseline_balance_component "
                    "WHERE component='CASH'"))):
            with self.subTest(name=name):
                db = self.new_db(name + '.sqlite')
                self.apply(db=db)
                mutate(db)
                self.assertEqual(status_baseline(db)['status'], 'INVALID')
                self.assertEqual(HistoricalBaselineIndex(db).match_order(
                    self.snapshot['orders']['buy-a'], self.account_key),
                    {'status': 'conflict', 'reason': 'BASELINE_INDEX_CORRUPT'})

    def test_cross_asset_actual_collections_reject_zero_value_positions(self):
        for asset, record in (
                ('options', {'quantity': '1', 'held': '0', 'value': '0'}),
                ('crypto', {'quantity': '2', 'held': '0', 'value': '0'})):
            snapshot = deepcopy(self.snapshot)
            snapshot['other_asset_positions'][asset] = [record]
            self._refresh_cross(snapshot, asset, 'positions', record)
            with self.subTest(asset=asset), self.assertRaisesRegex(
                    BaselineError, 'BASELINE_CROSS_ASSET_POSITION_OPEN'):
                self.plan(snapshot=snapshot)

    def test_cross_asset_requires_actual_collections_and_pagination(self):
        missing = deepcopy(self.snapshot)
        missing.pop('other_asset_positions')
        with self.assertRaisesRegex(BaselineError, 'CONDITIONS_MISSING'):
            self.plan(snapshot=missing)

        no_pages = deepcopy(self.snapshot)
        no_pages['provenance']['asset_scopes']['options']['orders'][
            'pagination'].pop('pages')
        with self.assertRaisesRegex(BaselineError, 'CONDITIONS_MISSING'):
            self.plan(snapshot=no_pages)

        mismatch = deepcopy(self.snapshot)
        mismatch['other_asset_orders']['options'] = [
            {'id': 'option-order', 'state': 'filled'}]
        mismatch['provenance']['asset_scopes']['options']['orders'][
            'record_count'] = 1
        with self.assertRaisesRegex(BaselineError,
                                    'BASELINE_CROSS_ASSET_DIGEST_INVALID'):
            self.plan(snapshot=mismatch)

        unknown = deepcopy(self.snapshot)
        unknown['other_asset_held'] = {'options': [{'quantity': '1'}]}
        with self.assertRaisesRegex(BaselineError,
                                    'BASELINE_CROSS_ASSET_UNKNOWN_FIELD'):
            self.plan(snapshot=unknown)

    def test_malformed_trade_and_null_global_states_fail_closed(self):
        for payload, code in (
                ({'trades': {'corrupt-trade': None}}, 'BASELINE_GLOBAL_TRADE_INVALID'),
                ({'trades': {'t': {'id': 't', 'phase': 'UNKNOWN', 'quantity': '0'}}},
                 'BASELINE_GLOBAL_TRADE_ACTIVE'),
                ({'trades': {'t': {'id': 't', 'phase': 'FLAT_VERIFIED'}}},
                 'BASELINE_GLOBAL_TRADE_QUANTITY_MISSING'),
                ({'trades': {'t': {'id': 't', 'phase': 'FLAT_VERIFIED',
                                   'quantity': None}}},
                 'BASELINE_GLOBAL_TRADE_INVALID')):
            with self.subTest(payload=payload):
                db = self.new_db('trade-' + str(abs(hash(str(payload)))) + '.sqlite')
                db.execute('CREATE TABLE live_state(id INTEGER PRIMARY KEY, payload TEXT)')
                db.execute('INSERT INTO live_state VALUES(1,?)',
                           (json.dumps({**payload, 'issues': []}),))
                with self.assertRaisesRegex(BaselineError, code):
                    self.apply(db=db)

        cases = (
            ('live_actions', 'CREATE TABLE live_actions(state TEXT)',
             'INSERT INTO live_actions VALUES(NULL)',
             'BASELINE_GLOBAL_ACTION_UNRESOLVED'),
            ('execution_intents', 'CREATE TABLE execution_intents(state TEXT)',
             'INSERT INTO execution_intents VALUES(NULL)',
             'BASELINE_GLOBAL_EXECUTION_PENDING'),
            ('native_operation_records',
             'CREATE TABLE native_operation_records(state TEXT)',
             'INSERT INTO native_operation_records VALUES(NULL)',
             'BASELINE_GLOBAL_OPERATION_PENDING'),
            ('receipt_order_progress',
             'CREATE TABLE receipt_order_progress(terminal INTEGER)',
             'INSERT INTO receipt_order_progress VALUES(NULL)',
             'BASELINE_GLOBAL_TRADE_ACTIVE'),
        )
        for table, schema, insert, code in cases:
            with self.subTest(table=table):
                db = self.new_db('null-' + table + '.sqlite')
                db.execute(schema)
                db.execute(insert)
                with self.assertRaisesRegex(BaselineError, code):
                    self.apply(db=db)

    def _refresh_cross(self, snapshot, asset, collection, record):
        key = {'positions': 'other_asset_positions',
               'orders': 'other_asset_orders',
               'pending_settlements': 'other_asset_pending_settlements'}[collection]
        records = snapshot[key][asset]
        evidence = snapshot['provenance']['asset_scopes'][asset][collection]
        evidence['record_count'] = len(records)
        evidence['record_sha256'] = digest(records)
        evidence['pagination']['observed_at'] = snapshot['observed_at']

    def test_import_exception_clears_view_and_validates_once(self):
        manifest = self.plan()
        self.apply(manifest)
        importer = ExecutionReceiptImporter(
            self.db, attribution=ExecutionAttribution(self.db, clock=lambda: NOW),
            accounting=InstanceAccounting(self.db, clock=lambda: NOW),
            clock=lambda: NOW, baseline=HistoricalBaselineIndex(self.db))
        original_resolve = importer.resolve
        def fail_resolve(order, account_key):
            if order.get('id') == 'sell-a':
                raise ValueError('SIMULATED_IMPORT_FAILURE')
            return original_resolve(order, account_key)
        importer.resolve = fail_resolve
        with self.assertRaisesRegex(ValueError, 'SIMULATED_IMPORT_FAILURE'):
            importer.import_snapshot(self.snapshot)
        importer.resolve = original_resolve
        result = importer.import_snapshot(self.snapshot)
        self.assertEqual(len(result['baseline_matched']), 7)

        calls = []
        original_state = baseline_module._schema_state
        def counted(db):
            calls.append(1)
            return original_state(db)
        with patch('trading_infra.historical_baseline._schema_state',
                   side_effect=counted):
            result = importer.import_snapshot(self.snapshot)
        self.assertEqual(len(result['baseline_matched']), 7)
        self.assertEqual(len(calls), 1)

    def test_import_uses_one_validated_view_across_second_connection_write(self):
        manifest = self.plan()
        self.apply(manifest)
        writer = sqlite3.connect(self.db_path, isolation_level=None)
        self.addCleanup(writer.close)
        importer = ExecutionReceiptImporter(
            self.db, attribution=ExecutionAttribution(self.db, clock=lambda: NOW),
            accounting=InstanceAccounting(self.db, clock=lambda: NOW),
            clock=lambda: NOW, baseline=HistoricalBaselineIndex(self.db))
        original_resolve = importer.resolve
        fired = {'value': False}
        revised = deepcopy(self.snapshot['orders']['sell-a'])
        revised['executions'][0]['price'] = '1'
        def resolve(order, account_key):
            if order.get('id') == 'sell-a' and not fired['value']:
                fired['value'] = True
                writer.execute(
                    'UPDATE historical_baseline_order SET order_json=?,'
                    'canonical_order_sha256=?, executions_sha256=? '
                    'WHERE order_id=?',
                    (canonical(revised), digest(revised),
                     digest(revised['executions']), 'sell-a'))
                writer.commit()
            return original_resolve(order, account_key)
        importer.resolve = resolve
        incoming = deepcopy(self.snapshot)
        incoming['orders']['sell-a'] = revised
        result = importer.import_snapshot(incoming)
        self.assertTrue(fired['value'])
        self.assertEqual(len(result['baseline_matched']), 6)
        self.assertIn('BASELINE_ORDER_REVISED',
                      [item['reason'] for item in result['quarantined']])

        # The cache must not survive the import; the next import sees the
        # materialized corruption and blocks rather than matching the old row.
        result = importer.import_snapshot(self.snapshot)
        self.assertEqual(result['baseline_matched'], [])
        self.assertEqual({item['reason'] for item in result['quarantined']},
                         {'BASELINE_INDEX_CORRUPT'})

    def test_residual_broker_order_or_approval_blocks_flat_apply(self):
        for payload, code in (
                ({'trades': {'t': {'id': 't', 'phase': 'FLAT_VERIFIED',
                                    'quantity': '0'}},
                  'broker_orders': {'o': {'state': 'confirmed'}}, 'issues': []},
                 'BASELINE_GLOBAL_ORDER_ACTIVE'),
                ({'trades': {'t': {'id': 't', 'phase': 'FLAT_VERIFIED',
                                    'quantity': '0'}},
                  'broker_approvals': {'count': 1, 'ids': ['a'],
                                        'missing': [], 'states': {}},
                  'issues': []},
                 'BASELINE_GLOBAL_APPROVAL_PENDING')):
            with self.subTest(code=code):
                db = self.new_db('residual-' + code + '.sqlite')
                db.execute('CREATE TABLE live_state(id INTEGER PRIMARY KEY, payload TEXT)')
                db.execute('INSERT INTO live_state VALUES(1,?)',
                           (json.dumps(payload),))
                with self.assertRaisesRegex(BaselineError, code):
                    self.apply(db=db)

    def test_real_normal_flat_exit_flag_allows_unrelated_baseline(self):
        book = ExecutionBook(self.db_path, POLICY, simulation=True,
                             clock=lambda: NOW, exit_decider=exit_decision)
        self.addCleanup(book.close)
        quote = {'symbol': 'DEMO', 'bid': '39.99', 'ask': '40',
                 'bid_at': AT, 'ask_at': AT, 'observed_at': AT}
        plan = {'symbol': 'DEMO', 'quantity': 2, 'entry_ceiling': '40',
                'stop': '39', 'cost_buffer_per_share': '0.05'}
        def snap(orders=None, qty='0', held='0'):
            fake = FakeBroker()
            fake.orders = orders or []
            fake.positions = [] if qty == '0' else [position(qty, held)]
            value = snapshot(fake, NOW.isoformat())
            value['synthetic'] = True
            return value
        def bound(action, *, filled='0', state='confirmed'):
            params = action['parameters']
            value = order(action['action_id'], params['side'], params['quantity'],
                          filled, state, params.get('stop_price'))
            value['ref_id'] = action['ref_id']
            value['price'] = params.get('limit_price')
            value['type'] = 'limit' if params['type'] == 'limit' else 'market'
            value['market_hours'] = params['market_hours']
            value['time_in_force'] = params['time_in_force']
            return value
        book.propose('signal-1', plan, quote, snap(),
                     evidence={'source_sha256': 'synthetic-fixture',
                               'synthetic': True})
        entry = book.claim()
        book.report(entry['action_id'], {'status': 'submitted'})
        buy = bound(entry, filled='2', state='filled')
        book.reconcile(snap([buy], qty='2'))
        protect = book.claim()
        book.report(protect['action_id'], {'status': 'submitted'})
        stop = bound(protect)
        quote = {**quote, 'bid': '38.99', 'ask': '39'}
        book.reconcile(snap([buy, stop], qty='2', held='2'),
                       quotes={'DEMO': quote})
        cancel = book.claim()
        book.report(cancel['action_id'], {'status': 'cancel_requested'})
        stop = bound(protect, filled='0', state='cancelled')
        book.reconcile(snap([buy, stop], qty='2'), quotes={'DEMO': quote})
        exit_action = book.claim()
        self.assertEqual(exit_action['kind'], 'exit')
        book.report(exit_action['action_id'], {'status': 'submitted'})
        sell = bound(exit_action, filled='2', state='filled')
        book.reconcile(snap([buy, stop, sell]))
        trade = book.state()['trades']['signal-1']
        self.assertEqual(trade['phase'], 'FLAT_VERIFIED')
        self.assertEqual(trade['quantity'], '0')
        self.assertTrue(trade['exit_requested'])
        self.assertTrue(book.state()['issues'])
        self.assertEqual(self.apply()['status'], 'APPLIED')

    def test_live_state_active_trade_blocks_new_apply(self):
        db = self.new_db('live-state.sqlite')
        db.execute('CREATE TABLE live_state(id INTEGER PRIMARY KEY, payload TEXT)')
        db.execute('INSERT INTO live_state VALUES(1,?)',
                   (json.dumps({'trades': {'t': {
                       'id': 't', 'phase': 'ENTRY_OPEN', 'quantity': '1'}},
                       'issues': []}),))
        with self.assertRaisesRegex(BaselineError, 'BASELINE_GLOBAL_TRADE_ACTIVE'):
            self.apply(db=db)

    def test_accepted_baseline_remains_matched_after_risk_and_live_rows(self):
        manifest = self.plan()
        self.apply(manifest)
        self.db.execute('CREATE TABLE account_risk(payload TEXT)')
        self.db.execute("INSERT INTO account_risk VALUES('changed')")
        self.db.execute('CREATE TABLE live_actions(state TEXT)')
        self.db.execute("INSERT INTO live_actions VALUES('UNKNOWN')")
        index = HistoricalBaselineIndex(self.db)
        self.assertEqual(index.match_order(self.snapshot['orders']['buy-a'],
                                           self.account_key)['status'], 'matched')
