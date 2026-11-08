"""Cross-session recovery for every durable write boundary (003r6a).

Each fault is injected on the real write path with a SQLite trigger, the old
connection and importer are closed, and a NEW connection replays the same
formal receipt or settlement. The point is that a same-connection rollback is
not evidence: only the new session proves the durable state is consistent.

Boundaries covered: instance fills (with fees), attribution feedback, execution
identity, receipt order progress, lease release, budget release, and the window
between ref binding and the DISPATCHED intent mark.
"""
import json
import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

import test_admission_safety as safety
from entry_authority_fixture import TestExecutionBook as ExecutionBook
from martin_luk.live_session import run_live_session
from test_partial_lifecycle import FailingPlugin  # noqa: F401  (fixture import side)
from test_approval_unknown_lifecycle import ApprovalFlow
from test_runner_result_merge import NOW, snapshot
from trading_infra.attribution import ExecutionAttribution
from trading_infra.instance_accounting import InstanceAccounting
from trading_infra.integrity_faults import ExecutionIntegrityFaults
from trading_infra.receipts import ExecutionReceiptImporter


AT = datetime.fromisoformat('2026-10-16T14:00:00+00:00')


def execution(*, execution_id='E1', order_id='ord-1', quantity='1',
              price='40.00', fees='0'):
    return {'id': execution_id, 'order_id': order_id, 'symbol': 'DEMO',
            'side': 'buy', 'quantity': quantity, 'price': price, 'fees': fees,
            'executed_at': (AT - timedelta(seconds=5)).isoformat()}


def receipt_snapshot(fills, *, at=None):
    order_id = fills[0]['order_id']
    return {'account_key': 'acct-0000', 'observed_at': (at or AT).isoformat(),
            'collection_complete': True,
            'orders': {order_id: {
                'id': order_id, 'ref_id': 'ref-a', 'symbol': 'DEMO',
                'side': 'buy', 'state': 'filled', 'quantity': '1',
                'cumulative_quantity': str(sum(int(e['quantity']) for e in fills)),
                'executions': fills}}}


def abort_on(table):
    return ('CREATE TRIGGER abort_write BEFORE INSERT ON %s BEGIN '
            "SELECT RAISE(ABORT, 'injected %s write failure'); END" % (table, table))


def abort_delete_on(table):
    return ('CREATE TRIGGER abort_write BEFORE DELETE ON %s BEGIN '
            "SELECT RAISE(ABORT, 'injected %s release failure'); END" % (table, table))


class ReceiptWriteBoundaries(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'receipts.sqlite'

    def connections(self):
        db = sqlite3.connect(self.path, isolation_level='')
        self.addCleanup(db.close)
        attribution = ExecutionAttribution(db, clock=lambda: AT)
        accounting = InstanceAccounting(db, clock=lambda: AT)
        importer = ExecutionReceiptImporter(
            db, attribution=attribution, accounting=accounting, clock=lambda: AT)
        return db, attribution, accounting, importer

    def seed(self):
        db, attribution, accounting, importer = self.connections()
        attribution.bind('ref-a', 'alpha', 'trade-a')
        attribution.record  # table exists after first call; bind creates schema
        return db, attribution, accounting, importer

    def assert_replay_is_consistent(self, *, fees):
        db, attribution, accounting, importer = self.connections()
        snapshot_value = receipt_snapshot([execution(fees=fees)])
        importer.import_snapshot(snapshot_value)
        fills = accounting.fills('alpha')
        self.assertEqual([(item['fill_id'], item['quantity'], item['fees'])
                          for item in fills], [('E1', '1', fees)])
        self.assertEqual(len([event for event in attribution.events('alpha')
                              if event['payload'].get('execution_id')]), 1)
        self.assertEqual(db.execute('SELECT count(*) FROM execution_identity')
                         .fetchone()[0], 1)
        self.assertEqual(db.execute('SELECT count(*) FROM receipt_order_progress')
                         .fetchone()[0], 1)
        # A replay of the same receipt changes nothing.
        importer.import_snapshot(snapshot_value)
        self.assertEqual(len(accounting.fills('alpha')), 1)
        self.assertEqual(db.execute('SELECT count(*) FROM execution_identity')
                         .fetchone()[0], 1)
        self.assertFalse(db.in_transaction)

    def test_fill_write_failure_recovers_on_a_new_connection(self):
        self.assert_boundary('instance_fills', expect_fill_before_crash=False)

    def test_fee_is_part_of_the_fill_row_and_lands_exactly_once(self):
        self.assert_boundary('instance_fills', expect_fill_before_crash=False,
                             fees='0.07')

    def test_feedback_write_failure_recovers_on_a_new_connection(self):
        self.assert_boundary('execution_feedback', expect_fill_before_crash=False)

    def test_identity_write_failure_recovers_on_a_new_connection(self):
        self.assert_boundary('execution_identity', expect_fill_before_crash=False)

    def test_order_progress_failure_recovers_without_a_duplicate_fill(self):
        self.assert_boundary('receipt_order_progress',
                             expect_fill_before_crash=True)

    def assert_boundary(self, table, *, expect_fill_before_crash, fees='0'):
        db, attribution, accounting, importer = self.seed()
        db.execute(abort_on(table))
        db.commit()
        with self.assertRaises(sqlite3.Error):
            importer.import_snapshot(receipt_snapshot([execution(fees=fees)]))
        self.assertFalse(db.in_transaction, 'no transaction may be left open')
        fills_before = accounting.fills('alpha')
        if expect_fill_before_crash:
            self.assertEqual(len(fills_before), 1,
                             'the later progress step failed after the fill')
        else:
            self.assertEqual(fills_before, [])
        db.execute('DROP TRIGGER abort_write')
        db.commit()
        db.close()
        self.assert_replay_is_consistent(fees=fees)


class SettlementReleaseBoundaries(TestCase):
    def setUp(self):
        self.fixture = safety.Base('runTest')
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.path = Path(self.fixture.temp.name) / 'safety.sqlite'

    def new_service(self):
        db = sqlite3.connect(self.path, isolation_level=None)
        self.addCleanup(db.close)
        from trading_infra.budget import AccountBudgetAuthority
        from trading_infra.instance_registry import InstanceRegistry
        from trading_infra.symbol_lease import SymbolLease
        from trading_infra.admission import AdmissionService
        money = AccountBudgetAuthority(db, safety.ACCOUNT, clock=lambda: safety.AT)
        self.addCleanup(money.close)
        leases = SymbolLease(db, clock=lambda: safety.AT)
        self.addCleanup(leases.close)
        attribution = ExecutionAttribution(db, clock=lambda: safety.AT)
        registry = InstanceRegistry(db, clock=lambda: safety.AT)
        service = AdmissionService(budget=money, leases=leases,
                                   attribution=attribution, policy=safety.POLICY,
                                   registry=registry, clock=lambda: safety.AT)
        return db, service, money, leases

    def assert_boundary(self, table):
        self.assertTrue(self.fixture.admit()['admitted'])
        db = self.fixture.db
        db.execute(abort_delete_on(table))
        db.execute('COMMIT') if db.in_transaction else None
        with self.assertRaises(sqlite3.Error):
            self.fixture.service.settle('alpha', 'p-SPY', authority=safety.flat())
        self.assertEqual(self.fixture.leases.owner('SPY'), 'alpha',
                         'a refused release must not free the symbol')
        self.assertEqual(self.fixture.money.status()['open_cash'], '80.00',
                         'a refused release must not free the cash')
        self.assertFalse(db.in_transaction)
        db.execute('DROP TRIGGER abort_write')
        db.execute('COMMIT') if db.in_transaction else None
        db.close()

        _, service, money, leases = self.new_service()
        first = service.settle('alpha', 'p-SPY', authority=safety.flat())
        self.assertTrue(first['settled'])
        self.assertIsNone(leases.owner('SPY'))
        self.assertEqual(money.status()['open_cash'], '0')
        again = service.settle('alpha', 'p-SPY', authority=safety.flat())
        self.assertFalse(again['settled'])
        self.assertIsNone(leases.owner('SPY'))
        self.assertEqual(money.status()['open_cash'], '0')

    def test_lease_release_failure(self):
        self.assert_boundary('symbol_leases')

    def test_budget_release_failure(self):
        self.assert_boundary('account_budget_reservations')


class IntentCrashMixin:
    """Close the old owner completely, then reopen the ledger from disk."""

    def hard_restart(self):
        runtime, book = self.runtime, self.book
        runtime.close()
        # Its registered cleanup will call close() again; keep that a no-op on
        # the closed connection without weakening the close we just proved.
        runtime.queue.close = lambda: None
        book.close()
        self.old_db = book.db
        self.rebuild()

    def force_dispatched_mark_failure(self):
        db = sqlite3.connect(Path(self.temp.name) / 'partial.sqlite',
                            isolation_level='')
        db.execute("CREATE TRIGGER abort_write BEFORE UPDATE ON execution_intents"
                   " WHEN NEW.state='DISPATCHED' BEGIN"
                   " SELECT RAISE(ABORT, 'injected dispatched mark failure'); END")
        db.commit()
        db.close()
        raised = False
        for _ in range(5):
            try:
                self.step()
            except sqlite3.Error:
                raised = True
                break
        self.assertTrue(raised, 'the injected failure must reach the caller')
        db = sqlite3.connect(Path(self.temp.name) / 'partial.sqlite',
                            isolation_level='')
        db.execute('DROP TRIGGER abort_write')
        db.commit()
        db.close()
        self.assertEqual(self.book.db.execute(
            'SELECT state FROM execution_intents').fetchone()[0], 'DISPATCHING')
        entries = [item for item in self.book.actions() if item['kind'] == 'entry']
        self.assertEqual(len(entries), 1, 'the action landed before the failure')
        self.assertFalse(self.book.db.in_transaction)


class IntentDispatchWindow(IntentCrashMixin, ApprovalFlow):
    """A failure between ref binding and the DISPATCHED mark is recoverable."""

    def test_a_stuck_dispatch_adopts_the_one_existing_action(self):
        self.force_dispatched_mark_failure()
        self.hard_restart()
        with self.assertRaises(sqlite3.ProgrammingError):
            self.old_db.execute('SELECT 1')
        self.assertIsNot(self.book.db, self.old_db,
                         'the new owner must open the ledger from disk')
        for _ in range(3):
            self.now += timedelta(seconds=1)
            self.step()
        db = self.book.db
        self.assertEqual(db.execute(
            'SELECT state FROM execution_intents').fetchone()[0], 'DISPATCHED')
        entries = [item for item in self.book.actions() if item['kind'] == 'entry']
        self.assertEqual(len(entries), 1, 'recovery must never create a second entry')
        ref = json.loads(entries[0]['payload'])['ref_id']
        owner = self.feedback.owner(ref)
        self.assertEqual(owner['instance_id'], 'alpha')
        self.assertEqual(self.leases.owner('DEMO'), 'alpha')


class IntentRecoveryIsolation(IntentCrashMixin, ApprovalFlow):
    """An unreadable action authority never becomes "no action"."""

    def test_an_unreadable_action_authority_isolates_and_never_abandons(self):
        self.force_dispatched_mark_failure()
        self.hard_restart()
        with self.assertRaises(sqlite3.ProgrammingError):
            self.old_db.execute('SELECT 1')
        db = self.book.db
        with patch.object(type(self.supervisor), '_entry_action_for',
                          side_effect=sqlite3.OperationalError('injected action read')):
            for _ in range(3):
                self.now += timedelta(seconds=1)
                result = self.step()
                reasons = [item.get('reason') for item in
                           (result.get('errors') or [])]
                self.assertIn('RECOVERY_ACTION_AUTHORITY_UNAVAILABLE', reasons)
                self.assertEqual(db.execute(
                    'SELECT state FROM execution_intents').fetchone()[0],
                    'DISPATCHING')
                self.assertEqual(self.leases.owner('DEMO'), 'alpha')
                self.assertEqual(self.money.status()['open_cash'], '80.00')
        entries = [item for item in self.book.actions() if item['kind'] == 'entry']
        self.assertEqual(len(entries), 1)

        # Fault removed: the same owner session only adopts the existing action.
        for _ in range(3):
            self.now += timedelta(seconds=1)
            self.step()
        self.assertEqual(db.execute(
            'SELECT state FROM execution_intents').fetchone()[0], 'DISPATCHED')
        entries = [item for item in self.book.actions() if item['kind'] == 'entry']
        self.assertEqual(len(entries), 1, 'no second entry may be created')
        ref = json.loads(entries[0]['payload'])['ref_id']
        self.assertEqual(self.feedback.owner(ref)['instance_id'], 'alpha')
        self.assertEqual(self.leases.owner('DEMO'), 'alpha')


class CloseAndStatusFailures(TestCase):
    def run_session(self, directory, *, seed=None, close_time=False,
                    status=None, extra_patches=()):
        from contextlib import ExitStack
        from unittest.mock import patch
        policy = json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        policy['capture_only'] = True
        path = Path(directory) / 'book.sqlite'
        if seed is not None:
            seed(path)
        at = (datetime(2026, 10, 16, 20, 0, 1, tzinfo=timezone.utc)
              if close_time else NOW)
        value = snapshot(base=at)
        target = status if status is not None else Path(directory) / 'status.json'
        with ExitStack() as stack:
            stack.enter_context(patch('martin_agentic.live_session.single_instance'))
            stack.enter_context(patch('martin_agentic.live_session._broker_read',
                                      return_value=value))
            stack.enter_context(patch('martin_agentic.live_session.collect_live_events',
                                      return_value=([], {'quotes': 0})))
            stack.enter_context(patch('martin_agentic.live_session.notify_local'))
            stack.enter_context(patch('martin_agentic.live_session.print'))
            for item in extra_patches:
                stack.enter_context(item)
            result = run_live_session(
                path, policy, symbols=[str(index) for index in range(10)],
                cycles=1, clock=lambda: at, status_file=target,
                output_dir=directory, budget_entry_limit=0,
                budget_total_limit=0)
        return result

    def test_close_with_unreadable_integrity_ledger_is_manual_handoff(self):
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_session(directory, close_time=True,
                                      extra_patches=[patch(
                                          'trading_infra.integrity_gate.unresolved_faults',
                                          side_effect=sqlite3.OperationalError('injected read'))])
            self.assertTrue(result['final_broker_check_performed'])
            self.assertTrue(result['supported_scope_flat'])
            self.assertFalse(result['broker_physically_flat'])
            self.assertFalse(result['final_flat_verified'])
            self.assertTrue(result['attention_required'])
            self.assertFalse(result['healthy'])
            self.assertEqual(result['close_disposition'],
                             'INTEGRITY_UNAVAILABLE_MANUAL_HANDOFF')
            self.assertTrue(result['integrity_gate_unavailable'])

    def test_a_status_write_failure_is_reported_and_never_masks_the_reason(self):
        with tempfile.TemporaryDirectory() as directory:
            blocked = Path(directory) / 'status.json'
            blocked.mkdir()
            result = self.run_session(directory, status=blocked)
            self.assertTrue(result['status_persistence_failed'])
            self.assertEqual(result['status_persistence_reason'],
                             'STATUS_FILE_WRITE_OR_READBACK_FAILED')
            self.assertFalse(result['healthy'])
            self.assertTrue(result['attention_required'])
            self.assertEqual(result['stop_disposition'],
                             'STATUS_PERSISTENCE_FAILED_MANUAL_HANDOFF_REQUIRED')
            self.assertIn('reason', result)
            self.assertNotEqual(result.get('reason'),
                                'STATUS_FILE_WRITE_OR_READBACK_FAILED')
