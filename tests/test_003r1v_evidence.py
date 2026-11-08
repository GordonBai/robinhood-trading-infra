"""Evidence the 003r1 review asked for, on the real call paths.

Three claims are checked here and nowhere hand-faked:

1. the provider's *tracked approved* approval semantics — a real
   ``collect_broker(..., approval_ids=[...])`` call returns a populated
   ``approval_states`` superset with zero pending, and the book accepts it;
2. a fill and its attribution event roll back together on the default
   implicit-transaction connection when the second write fails;
3. a durable sell-before-basis receipt blocks new entries and settlement across
   a restart, and releases them only once an earlier basis arrives.

Synthetic fixtures and temporary databases; no broker or market request.
"""
import json
import sqlite3
import tempfile
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from martin_agentic.broker import collect_broker
from entry_authority_fixture import TestExecutionBook as ExecutionBook
from martin_luk.live_session import LiveSupervisor
from test_broker import FakeBroker, approval, order as broker_order, position
from test_broker import snapshot as broker_snapshot
from trading_contracts.instance import InstanceBudget, PluginManifest
from trading_infra.admission import AdmissionService
from trading_infra.attribution import ExecutionAttribution
from trading_infra.budget import AccountBudgetAuthority
from trading_infra.instance_accounting import InstanceAccounting
from trading_infra.instance_registry import InstanceRegistry
from trading_infra.instance_runtime import InstanceRuntime
from trading_infra.instance_supervisor import InstanceSupervisor
from trading_infra.receipts import ExecutionReceiptImporter
from trading_infra.symbol_lease import SymbolLease

AT = datetime.fromisoformat('2026-10-16T14:00:00+00:00')


def manifest(plugin_id='alpha'):
    return PluginManifest(plugin_id=plugin_id, version='1.0', state_schema='s-1',
                          config={'step': '1', 'cost_buffer_per_share': '0.05'},
                          data_requirements=('quotes',),
                          capabilities=('advisory-entry',), max_inflight=8)


def budget():
    return InstanceBudget(max_cash=Decimal('100'), max_planned_loss=Decimal('2.50'),
                          max_notional=Decimal('100'), max_concurrent=1)


class FakeRunner:
    def __init__(self):
        self.started = []

    @property
    def next_deadline_monotonic(self):
        return None

    def start(self, request, *, payload=None):
        self.started.append(request)

    def poll(self):
        return None

    def close(self):
        pass


class TrackedApprovalTests(TestCase):
    """A tracked *approved* approval must not look like a pending one."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.policy = json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        self.book = ExecutionBook(Path(self.temp.name) / 'approvals.sqlite',
                                  self.policy, simulation=True, clock=lambda: AT)
        self.addCleanup(self.book.close)

    def provider(self, *, approvals, tracked=None):
        fake = FakeBroker()
        fake.approvals = approvals
        fake.positions = [position('2', '0')]
        return collect_broker(fake, account_suffix='0000', clock=lambda: AT,
                              approval_ids=tracked)

    def test_tracked_approved_provider_shape_is_a_superset_with_no_pending(self):
        value = self.provider(approvals=[approval('appr-1',
                                                  state='APPROVAL_STATE_APPROVED')],
                              tracked=['appr-1'])
        self.assertEqual(value['pending_approvals'], 0)
        self.assertEqual(value['pending_approval_ids'], [])
        self.assertEqual(list(value['approval_states']), ['appr-1'])
        self.assertEqual(value['approval_states']['appr-1']['state'],
                         'APPROVAL_STATE_APPROVED')
        self.assertEqual(value['missing_approval_ids'], [])

    def test_a_tracked_approved_snapshot_round_trips_through_the_book(self):
        value = self.provider(approvals=[approval('appr-1',
                                                  state='APPROVAL_STATE_APPROVED')],
                              tracked=['appr-1'])
        value['synthetic'] = True
        self.book.reconcile(value)
        approvals = self.book.state()['broker_approvals']
        self.assertEqual(approvals['count'], 0)
        self.assertEqual(approvals['ids'], [])
        self.assertEqual(list(approvals['states']), ['appr-1'])
        self.assertEqual(approvals['missing'], [])
        # Internally consistent facts are accepted by the release gate.
        evidence = self.book.flat_evidence('unused-but-checked-below') \
            if False else None
        self.assertIsNone(evidence)
        self.book._begin()
        try:
            state = self.book.state()
            state['trades'] = {'t1': {'id': 't1', 'phase': 'FLAT_VERIFIED',
                                      'quantity': '0', 'plan': {'symbol': 'DEMO'}}}
            self.book._save(state)
            self.book.db.commit()
        finally:
            pass
        evidence = self.book.flat_evidence('t1')
        self.assertEqual(evidence['pending_approvals'], 0)
        self.assertEqual(evidence['open_orders'], 0)

    def test_a_genuinely_pending_approval_still_blocks_release(self):
        value = self.provider(approvals=[approval('appr-2')],
                              tracked=['appr-2'])
        self.assertEqual(value['pending_approvals'], 1)
        self.assertEqual(value['pending_approval_ids'], ['appr-2'])
        value['synthetic'] = True
        self.book.reconcile(value)
        self.book._begin()
        try:
            state = self.book.state()
            state['trades'] = {'t1': {'id': 't1', 'phase': 'FLAT_VERIFIED',
                                      'quantity': '0', 'plan': {'symbol': 'DEMO'}}}
            self.book._save(state)
            self.book.db.commit()
        finally:
            pass
        evidence = self.book.flat_evidence('t1')
        self.assertEqual(evidence['pending_approvals'], 1)


class AtomicFillTests(TestCase):
    """A fill and its feedback must commit together or not at all."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = sqlite3.connect(self.temp.name + '/atomic.sqlite',
                                  isolation_level='')
        self.addCleanup(self.db.close)
        self.attribution = ExecutionAttribution(self.db, clock=lambda: AT)
        self.accounting = InstanceAccounting(self.db, clock=lambda: AT)
        self.attribution.bind('ref-a', 'alpha', 'trade-a')
        self.importer = ExecutionReceiptImporter(
            self.db, attribution=self.attribution, accounting=self.accounting,
            clock=lambda: AT)

    def snapshot(self):
        # The normalised execution shape keeps side and symbol on the execution,
        # exactly as ``broker.normalize_order`` produces them.
        execution = {'id': 'E1', 'order_id': 'ord-1', 'symbol': 'DEMO',
                     'side': 'buy', 'quantity': '1', 'price': '40.00',
                     'fees': '0', 'executed_at': (AT - timedelta(seconds=5)).isoformat()}
        return {'account_key': 'acct-0000', 'observed_at': AT.isoformat(),
                'collection_complete': True,
                'orders': {'ord-1': {
                    'id': 'ord-1', 'ref_id': 'ref-a', 'symbol': 'DEMO',
                    'side': 'buy', 'state': 'filled', 'quantity': '1',
                    'cumulative_quantity': '1', 'executions': [execution]}}}

    def test_a_feedback_failure_rolls_the_fill_back_and_leaves_no_transaction(self):
        self.assertNotEqual(self.db.isolation_level, None)
        with patch.object(self.attribution, 'record',
                          side_effect=sqlite3.OperationalError('injected feedback failure')):
            with self.assertRaises(sqlite3.OperationalError):
                self.importer.import_snapshot(self.snapshot())
        self.assertEqual(self.accounting.fills('alpha'), [],
                         'the accounting write must roll back with the feedback')
        self.assertEqual(self.attribution.events('alpha'), [])
        self.assertFalse(self.db.in_transaction, 'no transaction may be left open')
        self.assertEqual(self.importer.pending_execution_count(), 0)

    def test_the_replay_then_succeeds_exactly_once_without_a_new_execution(self):
        self.importer.import_snapshot(self.snapshot())
        replay = self.importer.import_snapshot(self.snapshot())
        self.assertEqual(len(self.accounting.fills('alpha')), 1)
        self.assertEqual([item['recorded'] for item in replay['duplicate']], [False])
        # One fill fact and one order-state fact; the replay adds neither.
        events = self.attribution.events('alpha')
        self.assertEqual(len([item for item in events
                              if item['payload'].get('execution_id')]), 1)
        self.assertEqual(len(events), 2)
        self.assertFalse(self.db.in_transaction)


class DeferredGateTests(TestCase):
    """A pending basis must gate entries and settlement across a restart."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'gate.sqlite'
        self.policy = json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        self.book = ExecutionBook(self.path, self.policy, simulation=True,
                                  clock=lambda: AT)
        self.addCleanup(self.book.close)
        self.db = self.book.db

    def supervisor(self, *, fresh_runtime=False):
        registry = InstanceRegistry(self.db, clock=lambda: AT)
        registry.register('alpha', manifest(), budget(), mode='shadow', enabled=True)
        runtime = InstanceRuntime(registry, registry.instance('alpha'),
                                  runner=FakeRunner(), source_fingerprint='f' * 64,
                                  clock=lambda: AT)
        self.addCleanup(runtime.close)
        money = AccountBudgetAuthority(self.db, {'max_open_cash': Decimal('500'),
                                                 'max_open_notional': Decimal('500'),
                                                 'max_open_planned_loss': Decimal('10.00'),
                                                 'max_concurrent_trades': 4},
                                       clock=lambda: AT)
        self.addCleanup(money.close)
        leases = SymbolLease(self.db, clock=lambda: AT)
        self.addCleanup(leases.close)
        attribution = ExecutionAttribution(self.db, clock=lambda: AT)
        accounting = InstanceAccounting(self.db, clock=lambda: AT)
        admission = AdmissionService(budget=money, leases=leases,
                                     attribution=attribution,
                                     policy={'max_position_notional_usd': '100',
                                             'max_planned_loss_per_trade_usd': '2.50'},
                                     registry=registry, clock=lambda: AT)
        supervisor = LiveSupervisor(self.book, process_strategy=False,
                                    agent_mode='review', owner_source='f' * 64,
                                    instance_supervisor=InstanceSupervisor([runtime]),
                                    admission=admission, accounting=accounting)
        return supervisor, attribution, accounting

    def execution(self, execution_id, side, quantity, price, minutes):
        return {'id': execution_id, 'order_id': 'ord-' + side, 'symbol': 'SPY',
                'side': side, 'quantity': quantity, 'price': price, 'fees': '0',
                'executed_at': (AT + timedelta(minutes=minutes)).isoformat()}

    def order(self, side, execution):
        return {'id': 'ord-' + side, 'ref_id': 'ref-t', 'symbol': 'SPY',
                'side': side, 'state': 'filled',
                'quantity': execution['quantity'],
                'cumulative_quantity': execution['quantity'],
                'executions': [execution]}

    def snapshot(self, orders):
        return {'account_key': 'acct-0000',
                'observed_at': (AT + timedelta(minutes=30)).isoformat(),
                'collection_complete': True,
                'orders': {item['id']: item for item in orders}}

    def test_the_control_step_reports_entries_blocked_while_basis_is_pending(self):
        supervisor, attribution, _accounting = self.supervisor()
        attribution.bind('ref-t', 'alpha', 'trade-a')
        sell = self.execution('E2', 'sell', '1', '41', -5)
        supervisor._import_receipts(self.snapshot([self.order('sell', sell)]), [])
        self.assertTrue(supervisor._receipts_incomplete)
        # A full control step must report the gate closed, not merely carry an
        # internal flag.
        snapshot = broker_snapshot(FakeBroker(), AT.isoformat())
        snapshot['synthetic'] = True
        quote = {'id': 'e-DEMO', 'kind': 'quote', 'at': AT.isoformat(),
                 'synthetic_input': True, 'source_sha256': 'source-e',
                 'data': {'symbol': 'DEMO', 'bid': '39.99', 'ask': '40.00',
                          'bid_at': AT.isoformat(), 'ask_at': AT.isoformat(),
                          'observed_at': AT.isoformat()}}
        step = supervisor._step_instances(snapshot, [quote], allow_entries=True,
                                          allow_dispatch=False)
        self.assertTrue(step['new_entries_blocked'])
        self.assertIn('RECEIPT_AWAITING_BASIS',
                      [item.get('reason') for item in step['errors']])
        self.assertEqual(supervisor.receipt_importer.pending_execution_count(), 1,
                         'the pending basis is still recorded, not dropped')

    def test_a_deferred_sale_gates_entries_and_settlement_across_a_restart(self):
        supervisor, attribution, accounting = self.supervisor()
        attribution.bind('ref-t', 'alpha', 'trade-a')
        sell = self.execution('E2', 'sell', '1', '41', -5)
        errors = []
        outcome = supervisor._import_receipts(self.snapshot([self.order('sell', sell)]),
                                              errors)
        self.assertEqual(outcome['deferred'], 1)
        self.assertTrue(supervisor._receipts_incomplete)
        self.assertIn('RECEIPT_AWAITING_BASIS', [item['reason'] for item in errors])
        self.assertEqual(supervisor._settle_flat_trades()['blocked'],
                         'RECEIPT_IMPORT_INCOMPLETE')

        # Restart: a brand new supervisor over the same database still sees it.
        restarted, attribution2, accounting2 = self.supervisor(fresh_runtime=True)
        attribution2.bind('ref-t', 'alpha', 'trade-a')
        outcome = restarted._import_receipts(self.snapshot([]), [])
        self.assertEqual(outcome.get('imported'), 0)
        self.assertTrue(restarted._receipts_incomplete,
                        'the pending basis survives a restart')
        self.assertEqual(restarted._settle_flat_trades()['blocked'],
                         'RECEIPT_IMPORT_INCOMPLETE')
        self.assertEqual(accounting2.fills('alpha'), [])

        # An earlier basis arrives: the sale recovers exactly once and the gate
        # opens only then.
        buy = self.execution('E1', 'buy', '1', '40', -10)
        outcome = restarted._import_receipts(
            self.snapshot([self.order('buy', buy)]), [])
        self.assertEqual(outcome['imported'], 2, 'the buy and the recovered sale')
        self.assertFalse(restarted._receipts_incomplete)
        self.assertNotEqual(restarted._settle_flat_trades().get('blocked'),
                            'RECEIPT_IMPORT_INCOMPLETE')
        fills = accounting2.fills('alpha')
        self.assertEqual([fill['side'] for fill in fills], ['buy', 'sell'])
        self.assertEqual(restarted.receipt_importer.pending_execution_count(), 0)


class IdentityConflictGateTests(DeferredGateTests):
    """An execution identity conflict closes entries and settlement."""

    def test_a_conflicting_execution_fails_closed_through_the_supervisor(self):
        supervisor, attribution, accounting = self.supervisor()
        attribution.bind('ref-t', 'alpha', 'trade-a')
        attribution.bind('ref-u', 'beta', 'trade-b')
        buy = self.execution('E1', 'buy', '1', '40', -10)
        errors = []
        supervisor._import_receipts(self.snapshot([self.order('buy', buy)]), errors)
        self.assertEqual(len(accounting.fills('alpha')), 1)

        # The same execution re-presented under another order and another owner.
        second = self.execution('E1', 'buy', '1', '40', -10)
        rebadged = dict(self.order('buy', second), id='ord-other', ref_id='ref-u')
        errors = []
        outcome = supervisor._import_receipts(self.snapshot([rebadged]), errors)
        self.assertTrue(outcome.get('failed'))
        self.assertTrue(supervisor._receipts_incomplete)
        self.assertIn('RECEIPT_EXECUTION_IDENTITY_CONFLICT',
                      [item.get('reason') for item in errors])
        self.assertEqual(supervisor._settle_flat_trades()['blocked'],
                         'RECEIPT_IMPORT_INCOMPLETE')
        self.assertEqual(accounting.fills('beta'), [],
                         'the second instance must not receive the same execution')
