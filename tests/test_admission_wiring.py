"""Instance advice reaches the account authority through the real control step.

The review's complaint was that the budget, lease, attribution and accounting
components existed but no execution path called them. This drives the real
``_step_instances`` path and checks that a proposal becomes an execution intent
only after the account authority has agreed to the cash and the symbol.
"""
import sqlite3
import tempfile
from datetime import datetime, timezone
from decimal import Decimal
from unittest import TestCase

from martin_luk.live_session import LiveSupervisor
from trading_contracts.instance import InstanceBudget, PluginManifest
from trading_infra.admission import AdmissionService
from trading_infra.attribution import ExecutionAttribution
from trading_infra.budget import AccountBudgetAuthority
from trading_infra.instance_registry import InstanceRegistry
from trading_infra.instance_runtime import InstanceRuntime
from trading_infra.instance_supervisor import InstanceSupervisor
from trading_infra.symbol_lease import SymbolLease
from test_instance_host import CountingPlugin
from test_instance_recovery import DecisionRunner

NOW = datetime(2026, 10, 29, 14, 0, tzinfo=timezone.utc)
ACCOUNT = {'max_open_cash': Decimal('200'), 'max_open_notional': Decimal('200'),
           'max_open_planned_loss': Decimal('5.00'), 'max_concurrent_trades': 4}
POLICY = {'max_position_notional_usd': '100', 'max_planned_loss_per_trade_usd': '2.50'}


class Owner:
    def __init__(self, db):
        self.db = db
        self.clock = lambda: NOW
        self.simulation = True
        self.policy = {}

    def _begin(self):
        self.db.execute('BEGIN IMMEDIATE')


def manifest(plugin_id, step='1'):
    return PluginManifest(plugin_id=plugin_id, version='1.0', state_schema='counter-1',
                          config={'step': step, 'cost_buffer_per_share': '0.05'},
                          data_requirements=('quotes',),
                          capabilities=('advisory-entry',), max_inflight=64)


def budget():
    return InstanceBudget(max_cash=Decimal('100'), max_planned_loss=Decimal('2.50'),
                          max_notional=Decimal('100'), max_concurrent=2)


class AdmissionWiringTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = sqlite3.connect(self.temp.name + '/wiring.sqlite', isolation_level=None)
        self.addCleanup(self.db.close)
        self.registry = InstanceRegistry(self.db, clock=lambda: NOW)
        self.registry.register('alpha', manifest('alpha'), budget(), mode='shadow', enabled=True)
        self.registry.register('beta', manifest('beta', '2'), budget(), mode='shadow', enabled=True)
        self.runtimes = []
        for instance_id in ('alpha', 'beta'):
            runtime = InstanceRuntime(self.registry, self.registry.instance(instance_id),
                                      runner=DecisionRunner(CountingPlugin(proposal_at=1)),
                                      source_fingerprint='f' * 64, clock=lambda: NOW)
            self.addCleanup(runtime.close)
            self.runtimes.append(runtime)
        self.money = AccountBudgetAuthority(self.db, ACCOUNT, clock=lambda: NOW)
        self.addCleanup(self.money.close)
        self.leases = SymbolLease(self.db, clock=lambda: NOW)
        self.addCleanup(self.leases.close)
        self.feedback = ExecutionAttribution(self.db, clock=lambda: NOW)
        self.admission = AdmissionService(budget=self.money, leases=self.leases,
                                          attribution=self.feedback, policy=POLICY,
                                          registry=self.registry, clock=lambda: NOW)
        self.supervisor = LiveSupervisor(Owner(self.db), process_strategy=False,
                                         agent_mode='review', owner_source='f' * 64,
                                         instance_supervisor=InstanceSupervisor(self.runtimes),
                                         admission=self.admission)
        self.supervisor.book.reconcile = lambda *a, **k: None
        self.supervisor.book.actions = lambda: []
        self.supervisor._dispatch = lambda *a, **k: None
        self.supervisor._finish_step = lambda *a, **k: {'reconciled': True}

    def event(self, event_id, symbol):
        at = NOW.isoformat()
        return {'id': event_id, 'kind': 'quote', 'at': at, 'synthetic_input': True,
                'source_sha256': 'source-' + event_id,
                'data': {'symbol': symbol, 'bid': '40.00', 'ask': '40.01',
                         'bid_at': at, 'ask_at': at, 'observed_at': at}}

    def step(self, symbol='SPY'):
        snapshot = {'account_key': 'acct', 'observed_at': NOW.isoformat(), 'orders': {}}
        return self.supervisor._step_instances(snapshot, [self.event('e-' + symbol, symbol)],
                                               allow_entries=True, allow_dispatch=False)

    def drain_admission(self, symbol='SPY'):
        """Run several control ticks, as a daemon would, and gather the outcomes."""
        outcomes = []
        for _ in range(6):
            result = self.step(symbol)
            outcomes.extend(result['strategy_admissions']['outcomes'])
        return outcomes

    def test_advice_becomes_an_intent_through_the_real_step(self):
        outcomes = self.drain_admission()
        admitted = [item for item in outcomes if item['admitted']]
        self.assertEqual(len(admitted), 1)
        # Exactly one instance ends up owning the contested symbol.
        self.assertEqual(self.leases.owner('SPY'), admitted[0]['instance_id'])
        refused = [item for item in outcomes if not item['admitted']]
        self.assertTrue(any(item['reason'] == 'ADMISSION_SYMBOL_HELD' for item in refused))
        self.assertEqual(admitted[0]['symbol'], 'SPY')
        self.assertTrue(admitted[0]['confirmation_required'])

    def test_an_admitted_proposal_is_claimed_and_not_admitted_twice(self):
        outcomes = self.drain_admission()
        admitted = [item for item in outcomes if item['admitted']]
        self.assertEqual(len(admitted), 1)
        owner = admitted[0]['instance_id']
        claimed = [runtime for runtime in self.runtimes
                   if runtime.instance_id == owner][0]
        self.assertEqual(claimed.host.pending_proposals(), [])
        # The refused instance keeps its advice pending: it was never consumed.
        other = [runtime for runtime in self.runtimes if runtime.instance_id != owner][0]
        self.assertEqual(len(other.host.pending_proposals()), 1)
        self.assertEqual(len([item for item in outcomes if item['admitted']]), 1)

    def test_a_refused_proposal_leaves_no_reservation_behind(self):
        outcomes = self.drain_admission()
        refused = [item for item in outcomes if not item['admitted']]
        self.assertTrue(refused)
        for item in refused:
            self.assertFalse(self.money.instance_status(item['instance_id'])['open_trades']
                             if item['instance_id'] in self.money.status()['per_instance']
                             else 0)

    def test_without_an_admission_service_advice_stays_advisory(self):
        supervisor = LiveSupervisor(Owner(self.db), process_strategy=False,
                                    agent_mode='review', owner_source='f' * 64,
                                    instance_supervisor=InstanceSupervisor(self.runtimes))
        supervisor.book.reconcile = lambda *a, **k: None
        supervisor.book.actions = lambda: []
        supervisor._dispatch = lambda *a, **k: None
        supervisor._finish_step = lambda *a, **k: {'reconciled': True}
        snapshot = {'account_key': 'acct', 'observed_at': NOW.isoformat(), 'orders': {}}
        result = supervisor._step_instances(snapshot, [self.event('z-1', 'QQQ')],
                                            allow_entries=True, allow_dispatch=False)
        self.assertEqual(result['strategy_admissions'], {'enabled': False, 'outcomes': []})
        self.assertIsNone(self.leases.owner('QQQ'))
