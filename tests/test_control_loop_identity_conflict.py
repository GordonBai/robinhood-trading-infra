"""An execution-identity conflict must be evidence, not an uncaught exception.

``LiveRiskGuard`` compares execution ids before the receipt importer runs and
raises when one id reappears with different content. That is the very conflict
the receipt ledger records, so the control loop has to capture it and report a
diagnosable tick instead of letting it escape.

Driven through the real ``LiveSupervisor._step_instances`` on the book's own
default connection, with synthetic FakeBroker snapshots. No broker call.
"""
import json
import sqlite3
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
import tempfile
from unittest import TestCase

from entry_authority_fixture import TestExecutionBook as ExecutionBook
from martin_luk.live_session import LiveSupervisor
from test_broker import FakeBroker, order as broker_order, position
from test_broker import snapshot as broker_snapshot
from test_instance_host import CountingPlugin
from test_instance_recovery import DecisionRunner
from trading_contracts.instance import InstanceBudget, PluginManifest
from trading_infra.admission import AdmissionService
from trading_infra.attribution import ExecutionAttribution
from trading_infra.budget import AccountBudgetAuthority
from trading_infra.instance_accounting import InstanceAccounting
from trading_infra.instance_registry import InstanceRegistry
from trading_infra.instance_runtime import InstanceRuntime
from trading_infra.instance_supervisor import InstanceSupervisor
from trading_infra.symbol_lease import SymbolLease

AT = datetime.fromisoformat('2026-10-16T14:00:00+00:00')
LIMITS = {'max_open_cash': Decimal('500'), 'max_open_notional': Decimal('500'),
          'max_open_planned_loss': Decimal('10.00'), 'max_concurrent_trades': 4}
POLICY = {'max_position_notional_usd': '100',
          'max_planned_loss_per_trade_usd': '2.50'}


def manifest(plugin_id='alpha'):
    return PluginManifest(plugin_id=plugin_id, version='1.0', state_schema='s-1',
                          config={'step': '1', 'cost_buffer_per_share': '0.05'},
                          data_requirements=('quotes',),
                          capabilities=('advisory-entry',), max_inflight=8)


def budget():
    return InstanceBudget(max_cash=Decimal('100'), max_planned_loss=Decimal('2.50'),
                          max_notional=Decimal('100'), max_concurrent=1)


class _Runner:
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


class ControlLoopConflictTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'loop.sqlite'
        self.now = AT
        self.policy_doc = json.loads(
            Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        self.book = ExecutionBook(self.path, self.policy_doc, simulation=True,
                                  clock=lambda: self.now)
        self.addCleanup(self.book.close)
        self.db = self.book.db

    def supervisor(self, *, fresh=False):
        registry = InstanceRegistry(self.db, clock=lambda: self.now)
        registry.register('alpha', manifest('alpha'), budget(), mode='shadow',
                          enabled=True)
        registry.register('beta', manifest('beta'), budget(), mode='shadow',
                          enabled=True)
        runtimes = []
        for instance_id in ('alpha', 'beta'):
            runtime = InstanceRuntime(registry, registry.instance(instance_id),
                                      runner=_Runner(), source_fingerprint='f' * 64,
                                      clock=lambda: self.now)
            self.addCleanup(runtime.close)
            runtimes.append(runtime)
        money = AccountBudgetAuthority(self.db, LIMITS, clock=lambda: self.now)
        self.addCleanup(money.close)
        leases = SymbolLease(self.db, clock=lambda: self.now)
        self.addCleanup(leases.close)
        attribution = ExecutionAttribution(self.db, clock=lambda: self.now)
        accounting = InstanceAccounting(self.db, clock=lambda: self.now)
        admission = AdmissionService(budget=money, leases=leases,
                                     attribution=attribution, policy=POLICY,
                                     registry=registry, clock=lambda: self.now)
        supervisor = LiveSupervisor(
            self.book, process_strategy=False, agent_mode='review',
            owner_source='f' * 64, instance_supervisor=InstanceSupervisor(runtimes),
            admission=admission, accounting=accounting)
        return supervisor, attribution, accounting

    def snap(self, orders=(), *, qty='0'):
        fake = FakeBroker()
        fake.orders = list(orders)
        fake.positions = [] if qty == '0' else [position(qty, '0')]
        value = broker_snapshot(fake, self.now.isoformat())
        value['synthetic'] = True
        return value

    def event(self):
        at = self.now.isoformat()
        return {'id': 'e-' + at, 'kind': 'quote', 'at': at, 'synthetic_input': True,
                'source_sha256': 'source-' + at,
                'data': {'symbol': 'DEMO', 'bid': '39.99', 'ask': '40.00',
                         'bid_at': at, 'ask_at': at, 'observed_at': at}}

    def fill(self, *, ref, order_id, execution_id='same-exec', price='40',
             minutes=-5):
        return {'id': order_id, 'ref_id': ref, 'symbol': 'DEMO', 'side': 'buy',
                'state': 'filled', 'quantity': '1', 'cumulative_quantity': '1',
                'type': 'market', 'trigger': 'immediate',
                'created_at': AT.isoformat(), 'market_hours': 'regular_hours',
                'time_in_force': 'gfd',
                'executions': [{'id': execution_id, 'price': price,
                                'quantity': '1', 'fees': '0',
                                'timestamp': (AT + timedelta(minutes=minutes)).isoformat()}]}

    def test_a_risk_pre_empted_conflict_is_recorded_and_the_tick_is_diagnosable(self):
        supervisor, attribution, accounting = self.supervisor()
        attribution.bind('ref-a', 'alpha', 'trade-a')
        attribution.bind('ref-b', 'beta', 'trade-b')
        first = self.snap([self.fill(ref='ref-a', order_id='ord-a')], qty='1')
        step = supervisor._step_instances(first, [self.event()], allow_entries=True,
                                          allow_dispatch=False)
        self.assertTrue(step.get('reconciled', True))
        self.assertEqual(len(accounting.fills('alpha')), 1)

        # The same execution id reappears under another order and owner with
        # different content. The risk guard sees it first.
        self.now = self.now + timedelta(minutes=1)
        bad = self.snap([self.fill(ref='ref-b', order_id='ord-b', price='41')], qty='1')
        step = supervisor._step_instances(bad, [self.event()], allow_entries=True,
                                          allow_dispatch=False)
        self.assertFalse(step['reconciled'])
        self.assertTrue(step['needs_reconciliation'])
        self.assertTrue(step['new_entries_blocked'])
        self.assertFalse(step['healthy'])
        self.assertEqual(step['settlement_blocked'], 'RECEIPT_IMPORT_INCOMPLETE')
        self.assertEqual([item['reason'] for item in step['errors']],
                         ['RECEIPT_EXECUTION_IDENTITY_CONFLICT'])
        self.assertEqual(step['errors'][0]['source'], 'risk_guard')
        # No second fill and the original binding is untouched.
        self.assertEqual(len(accounting.fills('alpha')), 1)
        self.assertEqual(accounting.fills('beta'), [])
        self.assertEqual(attribution.owner('ref-b')['instance_id'], 'beta')
        # Durable evidence, readable from a fresh connection.
        reopened = sqlite3.connect(self.path, isolation_level='')
        self.addCleanup(reopened.close)
        rows = reopened.execute(
            'SELECT identity_key,rejected_order_id,rejected_instance_id'
            ' FROM execution_identity_conflict').fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][1], 'ord-b')

    def test_the_gate_survives_a_restart_and_the_conflict_is_not_cleared(self):
        supervisor, attribution, accounting = self.supervisor()
        attribution.bind('ref-a', 'alpha', 'trade-a')
        attribution.bind('ref-b', 'beta', 'trade-b')
        supervisor._step_instances(self.snap([self.fill(ref='ref-a', order_id='ord-a')],
                                             qty='1'), [self.event()],
                                   allow_entries=True, allow_dispatch=False)
        self.now = self.now + timedelta(minutes=1)
        supervisor._step_instances(self.snap([self.fill(ref='ref-b', order_id='ord-b',
                                                        price='41')], qty='1'),
                                   [self.event()], allow_entries=True,
                                   allow_dispatch=False)

        restarted, attribution2, accounting2 = self.supervisor(fresh=True)
        attribution2.bind('ref-a', 'alpha', 'trade-a')
        self.now = self.now + timedelta(minutes=1)
        step = restarted._step_instances(self.snap(qty='1'), [self.event()],
                                         allow_entries=True, allow_dispatch=False)
        self.assertTrue(step['new_entries_blocked'])
        self.assertIn('RECEIPT_IDENTITY_CONFLICT',
                      [item.get('reason') for item in step['errors']])
        self.assertEqual(restarted._settle_flat_trades()['blocked'],
                         'RECEIPT_IMPORT_INCOMPLETE')
        self.assertEqual(accounting2.fills('beta'), [])

    def test_a_healthy_tick_is_unaffected(self):
        supervisor, attribution, accounting = self.supervisor()
        attribution.bind('ref-a', 'alpha', 'trade-a')
        step = supervisor._step_instances(
            self.snap([self.fill(ref='ref-a', order_id='ord-a')], qty='1'),
            [self.event()], allow_entries=True, allow_dispatch=False)
        self.assertTrue(step.get('reconciled', True))
        self.assertNotIn('needs_reconciliation', step)
        self.assertEqual(len(accounting.fills('alpha')), 1)

    def test_a_failed_audit_write_is_explicit_and_still_fails_closed(self):
        from unittest.mock import patch
        supervisor, attribution, accounting = self.supervisor()
        attribution.bind('ref-a', 'alpha', 'trade-a')
        attribution.bind('ref-b', 'beta', 'trade-b')
        supervisor._step_instances(self.snap([self.fill(ref='ref-a', order_id='ord-a')],
                                             qty='1'), [self.event()],
                                   allow_entries=True, allow_dispatch=False)
        self.now = self.now + timedelta(minutes=1)
        with patch.object(supervisor.receipt_importer, 'record_risk_conflict',
                          side_effect=sqlite3.OperationalError('injected audit failure')):
            step = supervisor._step_instances(
                self.snap([self.fill(ref='ref-b', order_id='ord-b', price='41')],
                          qty='1'), [self.event()], allow_entries=True,
                allow_dispatch=False)
        self.assertFalse(step['reconciled'])
        self.assertTrue(step['conflict_audit_failed'])
        self.assertTrue(step['new_entries_blocked'])
        self.assertIn('RECEIPT_CONFLICT_AUDIT_FAILED',
                      [item.get('reason') for item in step['errors']])
        self.assertEqual(len(accounting.fills('alpha')), 1)
        self.assertEqual(accounting.fills('beta'), [])
        self.assertEqual(supervisor._settle_flat_trades()['blocked'],
                         'RECEIPT_IMPORT_INCOMPLETE')

    # -- the two probes from the review --------------------------------
    def test_a_first_unbound_fill_still_records_the_dispute(self):
        """Probe 1: no receipt identity yet, only the risk ledger knows it."""
        supervisor, attribution, accounting = self.supervisor()
        # ref-orphan has no attribution, so no execution_identity is created.
        attribution.bind('ref-b', 'beta', 'trade-b')
        first = self.snap([self.fill(ref='ref-orphan', order_id='ord-a')], qty='1')
        supervisor._step_instances(first, [self.event()], allow_entries=True,
                                   allow_dispatch=False)
        self.assertEqual(self.db.execute(
            'SELECT count(*) FROM execution_identity').fetchone()[0], 0)
        self.now = self.now + timedelta(minutes=1)
        step = supervisor._step_instances(
            self.snap([self.fill(ref='ref-b', order_id='ord-b', price='41')], qty='1'),
            [self.event()], allow_entries=True, allow_dispatch=False)
        self.assertFalse(step['reconciled'])
        rows = self.db.execute(
            'SELECT evidence_source,accepted_order_id,rejected_order_id,'
            ' rejected_instance_id FROM execution_identity_conflict').fetchall()
        self.assertEqual(len(rows), 1, 'the dispute must be persisted, not dropped')
        self.assertEqual(rows[0][0], 'risk_ledger_hash_only')
        self.assertIsNone(rows[0][1], 'no accepted binding may be invented')
        self.assertEqual(rows[0][2], 'ord-b')
        self.assertEqual(rows[0][3], 'beta')

    def test_every_variant_in_one_snapshot_is_audited_with_accurate_sides(self):
        """Probe 2: accepted and conflicting variants in the same snapshot."""
        supervisor, attribution, accounting = self.supervisor()
        attribution.bind('ref-a', 'alpha', 'trade-a')
        attribution.bind('ref-b', 'beta', 'trade-b')
        # A prior binding exists after the first tick, so the accepted side is
        # the real one rather than the first variant of this snapshot.
        supervisor._step_instances(
            self.snap([self.fill(ref='ref-a', order_id='ord-a')], qty='1'),
            [self.event()], allow_entries=True, allow_dispatch=False)
        self.now = self.now + timedelta(minutes=1)
        both = self.snap([self.fill(ref='ref-a', order_id='ord-a'),
                          self.fill(ref='ref-b', order_id='ord-b', price='41')], qty='1')
        supervisor._step_instances(both, [self.event()], allow_entries=True,
                                   allow_dispatch=False)
        rows = self.db.execute(
            'SELECT accepted_order_id,accepted_instance_id,rejected_order_id,'
            ' rejected_instance_id FROM execution_identity_conflict').fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], 'ord-a')
        self.assertEqual(rows[0][1], 'alpha')
        self.assertEqual(rows[0][2], 'ord-b')
        self.assertEqual(rows[0][3], 'beta')

    def test_without_a_prior_each_variant_is_audited_and_acceptance_is_unstated(self):
        supervisor, attribution, accounting = self.supervisor()
        attribution.bind('ref-a', 'alpha', 'trade-a')
        attribution.bind('ref-b', 'beta', 'trade-b')
        # Two variants arrive together with no prior binding at all.
        both = self.snap([self.fill(ref='ref-a', order_id='ord-a'),
                          self.fill(ref='ref-b', order_id='ord-b', price='41')], qty='1')
        supervisor._step_instances(both, [self.event()], allow_entries=True,
                                   allow_dispatch=False)
        rows = self.db.execute(
            'SELECT accepted_order_id,rejected_order_id FROM execution_identity_conflict'
        ).fetchall()
        # No prior binding exists, so acceptance is unstated and every variant
        # is recorded rather than one being invented as the accepted side.
        self.assertGreaterEqual(len(rows), 1)
        for accepted, rejected in rows:
            self.assertIsNone(accepted)
            self.assertIsNotNone(rejected)
        self.assertEqual({row[1] for row in rows}, {'ord-a', 'ord-b'})

    def test_a_risk_storage_failure_is_structured_and_fails_closed(self):
        supervisor, attribution, accounting = self.supervisor()
        attribution.bind('ref-a', 'alpha', 'trade-a')
        from unittest.mock import patch
        with patch.object(self.book.risk_guard, 'observe',
                          side_effect=sqlite3.OperationalError('injected risk failure')):
            step = supervisor._step_instances(
                self.snap([self.fill(ref='ref-a', order_id='ord-a')], qty='1'),
                [self.event()], allow_entries=True, allow_dispatch=False)
        self.assertFalse(step['reconciled'])
        self.assertTrue(step['new_entries_blocked'])
        self.assertIn('RECONCILIATION_STORAGE_FAILED',
                      [item.get('reason') for item in step['errors']])
        self.assertFalse(step['healthy'])
        self.assertFalse(step['flat_claimed'])
        self.assertEqual(accounting.fills('alpha'), [],
                         'nothing may be booked from an unreconciled snapshot')

    def test_a_failed_tick_reports_real_management_state_not_a_promise(self):
        supervisor, attribution, accounting = self.supervisor()
        attribution.bind('ref-a', 'alpha', 'trade-a')
        attribution.bind('ref-b', 'beta', 'trade-b')
        # A protection action exists and is still open at the moment of failure.
        self.book.db.execute(
            "INSERT INTO live_actions(id,trade_id,kind,state,payload,created_at,"
            " updated_at,report) VALUES('protect-1','trade-a','protect','RUNNING',"
            "'{}',?,?,NULL)", (AT.isoformat(), AT.isoformat()))
        self.book.db.commit()
        supervisor._step_instances(
            self.snap([self.fill(ref='ref-a', order_id='ord-a')], qty='1'),
            [self.event()], allow_entries=True, allow_dispatch=False)
        self.now = self.now + timedelta(minutes=1)
        step = supervisor._step_instances(
            self.snap([self.fill(ref='ref-b', order_id='ord-b', price='41')], qty='1'),
            [self.event()], allow_entries=True, allow_dispatch=False)
        self.assertFalse(step['management_dispatched'])
        self.assertFalse(step['management_continues'],
                         'a failed tick must not promise management it did not do')
        self.assertGreaterEqual(step['management_state']['active_protection_actions'], 1)
        self.assertEqual(step['protection_status'], 'ACTIVE_UNVERIFIED')
        self.assertFalse(step['flat_claimed'])

    def test_the_async_step_path_fails_closed_the_same_way(self):
        """`_step_async` shares the wrapper; drive it end to end."""
        from unittest.mock import patch

        class StubSignals:
            def __init__(self, book):
                self.book = book
                self.identity = None
                self.cost = 0

            def state(self):
                return {'machines': {}}

            def pending(self):
                return []

            def consume(self, event):
                return None

            def complete(self, signal_id, *, result):
                return None

        class StubQueue:
            def __init__(self, book):
                self.book = book
                self.signals = StubSignals(book)
                self.enqueued = []

            def enqueue(self, event):
                self.enqueued.append(event)
                return {'enqueued': True}

            def advance(self):
                return {'completed': [], 'rejected': [], 'errors': [],
                        'started': None, 'pending': 0, 'busy': False}

            def status(self):
                return {'busy': False, 'pending': 0}

            def close(self):
                return {'closed': True}

        base, attribution, accounting = self.supervisor()
        queue = StubQueue(self.book)
        supervisor = LiveSupervisor(self.book, process_strategy=False,
                                    agent_mode='review', owner_source='f' * 64,
                                    strategy_queue=queue, admission=base.admission,
                                    accounting=accounting)
        attribution.bind('ref-a', 'alpha', 'trade-a')
        attribution.bind('ref-b', 'beta', 'trade-b')
        first = supervisor._step_async(
            self.snap([self.fill(ref='ref-a', order_id='ord-a')], qty='1'),
            [self.event()], allow_entries=True, allow_dispatch=False)
        self.assertTrue(first.get('reconciled', True))
        self.now = self.now + timedelta(minutes=1)
        step = supervisor._step_async(
            self.snap([self.fill(ref='ref-b', order_id='ord-b', price='41')], qty='1'),
            [self.event()], allow_entries=True, allow_dispatch=False)
        self.assertFalse(step['reconciled'])
        self.assertTrue(step['new_entries_blocked'])
        self.assertFalse(step['management_continues'])
        self.assertEqual(step['errors'][0]['reason'],
                         'RECEIPT_EXECUTION_IDENTITY_CONFLICT')
        self.assertEqual(len(accounting.fills('alpha')), 1)
        self.assertEqual(accounting.fills('beta'), [])
