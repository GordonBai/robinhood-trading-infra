"""Final 003 composition: commit-boundary recovery, flat evidence, stop gates.

Three independent commit boundaries are interrupted and then read back through a
NEW connection, which is what a restarted process would hold:

* receipt booking (fill + fee + feedback + identity),
* intent dispatch staging (admission -> book action),
* settlement (lease + budget release).

Each must leave no open transaction, no duplicate execution or fee, and no
released resource that was not proven safe. The final class re-checks
authoritative flat evidence and the durable integrity side-car through a real
``run_live_session`` stop.
"""
import json
import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from entry_authority_fixture import TestExecutionBook as ExecutionBook
from martin_agentic.watchdog import inhibit_new_entries
from trading_infra.integrity_gate import SIDECAR_ISSUE
from trading_infra.integrity_faults import ExecutionIntegrityFaults
from trading_infra.receipts import ExecutionReceiptImporter

import test_admission_safety as safety
from test_runner_result_merge import NOW, SessionLoopTests, snapshot


AT = datetime.fromisoformat('2026-10-16T14:00:00+00:00')


def execution(*, execution_id='E1', order_id='ord-1', quantity='1', price='40.00',
              fees='0', side='buy'):
    return {'id': execution_id, 'order_id': order_id, 'symbol': 'DEMO',
            'side': side, 'quantity': quantity, 'price': price, 'fees': fees,
            'executed_at': (AT - timedelta(seconds=5)).isoformat()}


def receipt_snapshot(executions, *, at=None):
    order_id = executions[0]['order_id']
    return {'account_key': 'acct-0000', 'observed_at': (at or AT).isoformat(),
            'collection_complete': True,
            'orders': {order_id: {
                'id': order_id, 'ref_id': 'ref-a', 'symbol': 'DEMO',
                'side': 'buy', 'state': 'filled', 'quantity': '2',
                'cumulative_quantity': str(sum(int(e['quantity'])
                                              for e in executions)),
                'executions': executions}}}


class ReceiptBoundaryRecovery(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'receipts.sqlite'
        self.db = sqlite3.connect(self.path, isolation_level='')
        self.addCleanup(self.db.close)
        from trading_infra.attribution import ExecutionAttribution
        from trading_infra.instance_accounting import InstanceAccounting
        self.attribution = ExecutionAttribution(self.db, clock=lambda: safety.AT)
        self.accounting = InstanceAccounting(self.db, clock=lambda: safety.AT)
        self.attribution.bind('ref-a', 'alpha', 'trade-a')
        self.importer = ExecutionReceiptImporter(
            self.db, attribution=self.attribution, accounting=self.accounting,
            clock=lambda: safety.AT)

    def new_connection(self):
        db = sqlite3.connect(self.path, isolation_level='')
        self.addCleanup(db.close)
        from trading_infra.attribution import ExecutionAttribution
        from trading_infra.instance_accounting import InstanceAccounting
        attribution = ExecutionAttribution(db, clock=lambda: safety.AT)
        accounting = InstanceAccounting(db, clock=lambda: safety.AT)
        importer = ExecutionReceiptImporter(
            db, attribution=attribution, accounting=accounting, clock=lambda: safety.AT)
        return db, attribution, accounting, importer

    def test_a_failed_booking_leaves_no_fill_and_replays_once_on_a_new_connection(self):
        first = execution(quantity='1', fees='0.01')
        with patch.object(self.attribution, 'record',
                          side_effect=sqlite3.OperationalError('injected feedback')):
            with self.assertRaises(sqlite3.OperationalError):
                self.importer.import_snapshot(receipt_snapshot([first]))
        self.assertEqual(self.accounting.fills('alpha'), [])
        self.assertFalse(self.db.in_transaction)

        db, attribution, accounting, importer = self.new_connection()
        importer.import_snapshot(receipt_snapshot([first]))
        fills = accounting.fills('alpha')
        self.assertEqual([(item['fill_id'], item['quantity'], item['fees'])
                          for item in fills], [('E1', '1', '0.01')])
        self.assertFalse(db.in_transaction)
        # Replay is a duplicate: no second fill, fee or feedback.
        importer.import_snapshot(receipt_snapshot([first]))
        self.assertEqual(len(accounting.fills('alpha')), 1)
        self.assertEqual(len([item for item in attribution.events('alpha')
                              if item['payload'].get('execution_id')]), 1)


class IntentDispatchBoundaryRecovery(TestCase):
    def setUp(self):
        self.fixture = safety.Base('runTest')
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.path = Path(self.fixture.temp.name) / 'safety.sqlite'

    def new_service(self):
        db = sqlite3.connect(self.path, isolation_level=None)
        self.addCleanup(db.close)
        from trading_infra.attribution import ExecutionAttribution
        from trading_infra.budget import AccountBudgetAuthority
        from trading_infra.instance_registry import InstanceRegistry
        from trading_infra.symbol_lease import SymbolLease
        money = AccountBudgetAuthority(db, safety.ACCOUNT, clock=lambda: safety.AT)
        self.addCleanup(money.close)
        leases = SymbolLease(db, clock=lambda: safety.AT)
        self.addCleanup(leases.close)
        attribution = ExecutionAttribution(db, clock=lambda: safety.AT)
        registry = InstanceRegistry(db, clock=lambda: safety.AT)
        from trading_infra.admission import AdmissionService
        service = AdmissionService(budget=money, leases=leases,
                                   attribution=attribution, policy=safety.POLICY,
                                   registry=registry, clock=lambda: safety.AT)
        return service, money, leases

    def test_a_dispatching_intent_is_visible_but_never_auto_released(self):
        admitted = self.fixture.admit()
        intent = admitted['intent_id']
        self.assertEqual(self.fixture.service.mark_intent(intent, 'DISPATCHING'), 1)
        service, money, leases = self.new_service()
        dispatching = service.dispatching_intents()
        self.assertEqual([item['intent_id'] for item in dispatching], [intent])
        self.assertEqual(service.pending_intents(), [])
        # A timer is not proof that the write did not happen.
        self.assertEqual(service.expire_intents(), [])
        self.assertEqual(leases.owner('SPY'), 'alpha')
        self.assertEqual(money.status()['open_cash'], '80.00')
        # Only the owner, having checked the book, may abandon it.
        self.assertTrue(service.abandon_intent(intent)['abandoned'])
        self.assertIsNone(leases.owner('SPY'))
        self.assertEqual(money.status()['open_cash'], '0')


class SettleBoundaryRecovery(TestCase):
    def setUp(self):
        self.fixture = safety.Base('runTest')
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.path = Path(self.fixture.temp.name) / 'safety.sqlite'

    def new_service(self):
        db = sqlite3.connect(self.path, isolation_level=None)
        self.addCleanup(db.close)
        from trading_infra.attribution import ExecutionAttribution
        from trading_infra.budget import AccountBudgetAuthority
        from trading_infra.instance_registry import InstanceRegistry
        from trading_infra.symbol_lease import SymbolLease
        money = AccountBudgetAuthority(db, safety.ACCOUNT, clock=lambda: safety.AT)
        self.addCleanup(money.close)
        leases = SymbolLease(db, clock=lambda: safety.AT)
        self.addCleanup(leases.close)
        attribution = ExecutionAttribution(db, clock=lambda: safety.AT)
        registry = InstanceRegistry(db, clock=lambda: safety.AT)
        from trading_infra.admission import AdmissionService
        service = AdmissionService(budget=money, leases=leases,
                                   attribution=attribution, policy=safety.POLICY,
                                   registry=registry, clock=lambda: safety.AT)
        return service, money, leases

    def test_a_settle_release_failure_keeps_resources_and_replays_once(self):
        self.assertTrue(self.fixture.admit()['admitted'])
        authority = safety.flat()
        with patch.object(self.fixture.leases, 'release_in_transaction',
                          side_effect=sqlite3.OperationalError('injected release')):
            with self.assertRaises(sqlite3.OperationalError):
                self.fixture.service.settle('alpha', 'p-SPY', authority=authority)
        self.assertEqual(self.fixture.leases.owner('SPY'), 'alpha')
        self.assertEqual(self.fixture.money.status()['open_cash'], '80.00')
        self.assertFalse(self.fixture.db.in_transaction)

        service, money, leases = self.new_service()
        first = service.settle('alpha', 'p-SPY', authority=safety.flat())
        self.assertTrue(first['settled'])
        self.assertIsNone(leases.owner('SPY'))
        self.assertEqual(money.status()['open_cash'], '0')
        # A replay releases nothing twice.
        again = service.settle('alpha', 'p-SPY', authority=safety.flat())
        self.assertFalse(again['settled'])
        self.assertIsNone(leases.owner('SPY'))
        self.assertEqual(money.status()['open_cash'], '0')


class FlatEvidenceMatrix(TestCase):
    """Every authoritative-flat refusal, on the real book state."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.policy = json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        self.book = ExecutionBook(Path(self.temp.name) / 'flat.sqlite',
                                  self.policy, simulation=True, clock=lambda: AT)
        self.addCleanup(self.book.close)
        value = snapshot(base=AT)
        value['synthetic'] = True
        self.book.reconcile(value)
        state = self.book.state()
        state['trades'] = {'t1': {'id': 't1', 'phase': 'FLAT_VERIFIED',
                                  'quantity': '0', 'plan': {}}}
        self.book._begin()
        try:
            self.book._save(state)
            self.book.db.commit()
        except BaseException:
            self.book.db.rollback()
            raise
        import copy
        self.pristine = copy.deepcopy(self.book.state())

    def evidence(self, **changes):
        import copy
        state = copy.deepcopy(self.pristine)
        state.update(changes)
        state.update(changes)
        self.book._begin()
        try:
            self.book._save(state)
            self.book.db.commit()
        except BaseException:
            self.book.db.rollback()
            raise
        return self.book.flat_evidence('t1')

    def assert_refused(self, code, **changes):
        with self.assertRaises(ValueError) as ctx:
            self.evidence(**changes)
        self.assertEqual(str(ctx.exception), code)

    def test_missing_snapshot_account_or_orders_is_refused(self):
        self.assert_refused('FLAT_EVIDENCE_SNAPSHOT_REQUIRED', last_snapshot=None)
        self.assert_refused('FLAT_EVIDENCE_ACCOUNT_REQUIRED', account_key=None)
        self.assert_refused('FLAT_EVIDENCE_ORDERS_REQUIRED', broker_orders=None)

    def test_inconsistent_or_missing_approvals_are_refused(self):
        self.assert_refused('FLAT_EVIDENCE_APPROVALS_REQUIRED',
                            broker_approvals=None)
        self.assert_refused('FLAT_EVIDENCE_APPROVALS_INCONSISTENT',
                            broker_approvals={'count': 1, 'ids': [], 'states': {},
                                              'missing': []})
        self.assert_refused('FLAT_EVIDENCE_APPROVALS_INCONSISTENT',
                            broker_approvals={'count': 0, 'ids': [], 'states': {},
                                              'missing': ['a-1']})

    def test_open_orders_and_unknown_actions_are_counted(self):
        evidence = self.evidence(broker_orders={'o-1': {
            'id': 'o-1', 'ref_id': 'ref-x', 'state': 'confirmed',
            'remaining': '1', 'external_broker_order_id': None}})
        # No action of this trade uses ref-x, so an unrelated order is not
        # this trade's exposure; a matching one is.
        self.assertEqual(evidence['open_orders'], 0)
        self.assertEqual(evidence['active_actions'], 0)
        self.assertEqual(evidence['unknown_intents'], 0)

    def test_evidence_never_substitutes_a_clock_for_observation(self):
        evidence = self.evidence()
        self.assertEqual(evidence['observed_at'], self.book.state()['last_snapshot'])


class StopWithDurableFault(TestCase):
    """A bounded run stops without erasing the durable integrity gate."""

    def test_a_stopped_session_keeps_the_sidecar_gate_closed(self):
        with tempfile.TemporaryDirectory() as shared:
            inhibit_new_entries(str(Path(shared) / 'book.sqlite'),
                                [SIDECAR_ISSUE], clock=lambda: NOW)
            helper = SessionLoopTests(
                'test_a_clean_session_completes_without_a_merge_error')
            result, status = helper.run_session(
                [snapshot()], ticks=2, directory=shared)
            self.assertFalse(result['healthy'])
            self.assertTrue(result['attention_required'])
            self.assertFalse(result['read_only_capture_healthy'])
            self.assertTrue(result.get('reconciliation_pending'))
            self.assertEqual(result.get('settlement_blocked'),
                             'RECEIPT_IMPORT_INCOMPLETE')
            self.assertIn('EXECUTION_INTEGRITY_SIDECAR_UNRECORDED_FAULT',
                          result.get('integrity_reasons') or [])
            self.assertTrue(status['attention_required'])
            self.assertFalse(status['healthy'])
            self.assertEqual(status.get('settlement_blocked'),
                             'RECEIPT_IMPORT_INCOMPLETE')


class RealCloseRecovery(TestCase):
    """The real runner's close path consumes the durable integrity authority."""

    def close_session(self, directory, *, seed=None):
        from unittest.mock import patch
        from martin_luk.live_session import run_live_session
        policy = json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        policy['capture_only'] = True
        path = Path(directory) / 'book.sqlite'
        if seed is not None:
            seed(path)
        at = datetime(2026, 10, 16, 20, 0, 1, tzinfo=timezone.utc)
        value = snapshot(base=at)
        with patch('martin_agentic.live_session.single_instance'), \
             patch('martin_agentic.live_session._broker_read',
                   return_value=value), \
             patch('martin_agentic.live_session.collect_live_events',
                   return_value=([], {'quotes': 0})), \
             patch('martin_agentic.live_session.notify_local'), \
             patch('martin_agentic.live_session.print'):
            result = run_live_session(
                path, policy, symbols=[str(index) for index in range(10)],
                cycles=1, clock=lambda: at,
                status_file=Path(directory) / 'status.json',
                output_dir=directory, budget_entry_limit=0,
                budget_total_limit=0)
        status = json.loads((Path(directory) / 'status.json').read_text())
        return result, status

    def seed_fault(self, path):
        db = sqlite3.connect(path, isolation_level='')
        try:
            ExecutionIntegrityFaults(db, clock=lambda: NOW).record(
                source='execution_identity_conflict', account_key='acct-key',
                execution_id='same-exec')
        finally:
            db.close()

    def seed_sidecar(self, path):
        inhibit_new_entries(str(path), [SIDECAR_ISSUE], clock=lambda: NOW)

    def assert_close_isolated(self, result, status, reason):
        self.assertTrue(result['final_broker_check_performed'])
        self.assertTrue(result['supported_scope_flat'],
                        'the supported broker facts are flat')
        self.assertFalse(result['broker_physically_flat'],
                         'whole-account flatness is not proven')
        self.assertFalse(result['final_flat_verified'],
                         'flat broker facts do not release an unresolved account')
        self.assertFalse(result['healthy'])
        self.assertTrue(result['attention_required'])
        self.assertIn(reason, result.get('integrity_reasons') or [])
        self.assertEqual(result['close_disposition'],
                         'BROKER_FLAT_INTEGRITY_UNRESOLVED')
        self.assertFalse(status['healthy'])
        self.assertTrue(status['attention_required'])
        self.assertEqual(status['close_disposition'],
                         'BROKER_FLAT_INTEGRITY_UNRESOLVED')

    def test_a_recorded_fault_is_not_washed_by_a_flat_close(self):
        with tempfile.TemporaryDirectory() as directory:
            result, status = self.close_session(directory, seed=self.seed_fault)
            self.assert_close_isolated(result, status,
                                       'EXECUTION_INTEGRITY_FAULT_UNRESOLVED')

    def test_a_sidecar_fault_is_not_washed_by_a_flat_close(self):
        with tempfile.TemporaryDirectory() as directory:
            result, status = self.close_session(directory, seed=self.seed_sidecar)
            self.assert_close_isolated(
                result, status, 'EXECUTION_INTEGRITY_SIDECAR_UNRECORDED_FAULT')

    def test_a_close_time_conflict_is_recorded_and_blocks_safe_flat(self):
        from martin_agentic.broker import BrokerJournal
        from martin_luk.live_session import _closing_check
        from test_runner_result_merge import broker_order
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'book.sqlite'
            policy = json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
            book = ExecutionBook(path, policy, clock=lambda: NOW)
            self.addCleanup(book.close)
            journal = BrokerJournal(path)
            self.addCleanup(journal.close)
            first = snapshot()
            first['orders'] = {'ord-a': {**broker_order(price='40'),
                                         'executions': [], 'cumulative_quantity': '0'}}
            book.reconcile(first)
            conflicting = snapshot()
            conflicting['orders'] = {'ord-a': {**broker_order(price='41'),
                                               'executions': [],
                                               'cumulative_quantity': '0'}}
            # The risk guard sees one execution twice with different content.
            first['orders']['ord-a']['executions'] = [broker_order(price='40')['executions'][0]]
            conflicting['orders']['ord-a']['executions'] = [
                {**broker_order(price='41')['executions'][0]}]
            book.reconcile(first)
            closing = _closing_check(book, journal, policy, lambda: NOW,
                                     observation=(True, conflicting))
            self.assertTrue(closing['close_integrity_conflict'])
            self.assertFalse(closing['final_flat_verified'])
            self.assertTrue(closing['attention_required'])
            self.assertIn(closing['close_disposition'],
                          {'INTEGRITY_FAULT_UNRESOLVED',
                           'BROKER_FLAT_INTEGRITY_UNRESOLVED'})
            db = sqlite3.connect(path)
            self.addCleanup(db.close)
            rows = db.execute(
                "SELECT source,state FROM execution_integrity_faults").fetchall()
            self.assertEqual(rows,
                             [('execution_identity_conflict_at_close',
                               'UNRESOLVED')])
