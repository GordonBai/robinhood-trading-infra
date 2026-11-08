"""One owner admits an instance proposal against account-wide limits.

The properties under test are the ones the review asked for: nothing is
half-reserved, two instances cannot both claim the same cash or ticker, and the
final quantity comes from infrastructure limits rather than from the proposal.
"""
import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest import TestCase

from trading_contracts.instance import InstanceBudget, PluginManifest
from trading_contracts.strategy import (ProposalAction, StrategyDecision,
                                        StrategyIdentity, StrategyProposal)
from trading_infra.admission import AdmissionService
from trading_infra.attribution import ExecutionAttribution
from trading_infra.budget import AccountBudgetAuthority
from trading_infra.instance_registry import InstanceRegistry
from trading_infra.symbol_lease import SymbolLease

AT = datetime(2026, 10, 29, 14, 0, tzinfo=timezone.utc)
ACCOUNT = {'max_open_cash': Decimal('200'), 'max_open_notional': Decimal('200'),
           'max_open_planned_loss': Decimal('5.00'), 'max_concurrent_trades': 4}
POLICY = {'max_position_notional_usd': '100', 'max_planned_loss_per_trade_usd': '2.50'}


def manifest(plugin_id='counter'):
    return PluginManifest(plugin_id=plugin_id, version='1.0', state_schema='counter-1',
                          config={'step': '1', 'cost_buffer_per_share': '0.05'},
                          data_requirements=('quotes',),
                          capabilities=('advisory-entry',), max_inflight=64)


def budget(cash='100', loss='2.50', notional='100', concurrent=2):
    return InstanceBudget(max_cash=Decimal(cash), max_planned_loss=Decimal(loss),
                          max_notional=Decimal(notional), max_concurrent=concurrent)


class AdmissionTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = sqlite3.connect(self.temp.name + '/admission.sqlite', isolation_level=None)
        self.addCleanup(self.db.close)
        self.clock = lambda: AT
        self.registry = InstanceRegistry(self.db, clock=self.clock)
        self.registry.register('alpha', manifest('alpha'), budget(), mode='shadow', enabled=True)
        self.registry.register('beta', manifest('beta'), budget(), mode='shadow', enabled=True)
        self.money = AccountBudgetAuthority(self.db, ACCOUNT, clock=self.clock)
        self.addCleanup(self.money.close)
        self.leases = SymbolLease(self.db, clock=self.clock)
        self.addCleanup(self.leases.close)
        self.feedback = ExecutionAttribution(self.db, clock=self.clock)
        self.service = AdmissionService(budget=self.money, leases=self.leases,
                                        attribution=self.feedback, policy=POLICY,
                                        registry=self.registry, clock=self.clock)

    def proposal(self, instance='alpha', *, proposal_id='p-1', symbol='SPY',
                 valid_for=20, action=ProposalAction.ENTER,
                 ceiling='40.00', stop='39.60', quantity_limit=10):
        return StrategyProposal(
            identity=StrategyIdentity(instance, 'alpha' if instance == 'alpha' else 'beta', '1.0'),
            proposal_id=proposal_id, decision_id='d-' + proposal_id,
            setup_id='s-1', data_snapshot_id='snap', source_sha256='a' * 64,
            generated_at=AT, valid_until=AT + timedelta(seconds=valid_for),
            action=action, symbol=symbol, reason='FIXTURE_ENTRY', synthetic=True,
            quantity_limit=(quantity_limit
                            if action in (ProposalAction.ENTER, ProposalAction.REDUCE)
                            else None),
            entry_ceiling=Decimal(ceiling) if action is ProposalAction.ENTER else None,
            stop_price=Decimal(stop) if action is ProposalAction.ENTER else None)

    def flat(self, trade_id):
        class Authority:
            def flat_evidence(self, requested):
                return {'source': 'execution_book', 'account_key': 'acct',
                        'trade_id': requested, 'phase': 'FLAT_VERIFIED',
                        'remaining_quantity': '0', 'open_orders': 0,
                        'pending_approvals': 0, 'unknown_intents': 0,
                        'active_actions': 0,
                        'observed_at': AT.isoformat()}
        return Authority()

    def test_a_live_proposal_is_admitted_with_an_intent(self):
        instance = self.registry.instance('alpha')
        result = self.service.admit(instance, self.proposal(), allow_entries=True)
        self.assertTrue(result['admitted'])
        self.assertEqual(result['symbol'], 'SPY')
        self.assertEqual(result['quantity'], 2)  # 100 notional / 40 ceiling
        self.assertEqual(result['trade_id'], 'p-1')
        self.assertTrue(result['confirmation_required'])
        self.assertEqual(self.leases.owner('SPY'), 'alpha')
        self.assertEqual(self.money.status()['open_cash'], '80.00')

    def test_the_same_symbol_cannot_be_admitted_twice(self):
        alpha = self.registry.instance('alpha')
        beta = self.registry.instance('beta')
        self.assertTrue(self.service.admit(alpha, self.proposal('alpha'), allow_entries=True)['admitted'])
        refused = self.service.admit(beta, self.proposal('beta', proposal_id='p-2'),
                                     allow_entries=True)
        self.assertFalse(refused['admitted'])
        self.assertEqual(refused['reason'], 'ADMISSION_SYMBOL_HELD')
        self.assertEqual(self.leases.owner('SPY'), 'alpha')

    def test_a_budget_refusal_gives_the_lease_back(self):
        tight = self.registry.register('gamma', manifest('gamma'),
                                       budget(cash='1', notional='1', loss='0.01'),
                                       mode='shadow', enabled=True)
        refused = self.service.admit(tight, self.proposal(
            'gamma', proposal_id='p-g', symbol='QQQ'), allow_entries=True)
        # The instance ceiling refuses on cash, after the lease was taken.
        self.assertFalse(refused['admitted'])
        self.assertEqual(refused['reason'], 'ADMISSION_BUDGET_REFUSED')
        self.assertIsNone(self.leases.owner('QQQ'))
        self.assertEqual(self.money.status()['open_cash'], '0')

    def test_an_expired_proposal_is_refused_and_writes_nothing(self):
        instance = self.registry.instance('alpha')
        stale = self.proposal(valid_for=1)
        self.service.clock = lambda: AT + timedelta(seconds=2)
        result = self.service.admit(instance, stale, allow_entries=True)
        self.assertFalse(result['admitted'])
        self.assertEqual(result['reason'], 'ADMISSION_PROPOSAL_EXPIRED')
        self.assertIsNone(self.leases.owner('SPY'))
        self.assertEqual(self.money.status()['open_cash'], '0')

    def test_a_disabled_instance_cannot_be_admitted(self):
        self.registry.set_mode('beta', 'disabled', enabled=False)
        instance = self.registry.instance('beta')
        result = self.service.admit(instance, self.proposal('beta'), allow_entries=True)
        self.assertEqual(result['reason'], 'ADMISSION_INSTANCE_DISABLED')
        self.assertIsNone(self.leases.owner('SPY'))

    def test_a_proposal_from_another_instance_is_refused(self):
        instance = self.registry.instance('beta')
        result = self.service.admit(instance, self.proposal('alpha'), allow_entries=True)
        self.assertEqual(result['reason'], 'ADMISSION_INSTANCE_MISMATCH')

    def test_only_entries_are_admitted_in_v1(self):
        instance = self.registry.instance('alpha')
        for action in (ProposalAction.EXIT, ProposalAction.HOLD,
                       ProposalAction.REDUCE):
            result = self.service.admit(instance, self.proposal(
                action=action, proposal_id='p-' + action.value), allow_entries=True)
            self.assertEqual(result['reason'], 'ADMISSION_ACTION_UNSUPPORTED')

    def test_an_unaffordable_proposal_is_refused_rather_than_resized_up(self):
        instance = self.registry.instance('alpha')
        result = self.service.admit(instance, self.proposal(
            ceiling='500.00', stop='499.00', proposal_id='p-big'), allow_entries=True)
        self.assertEqual(result['reason'], 'ADMISSION_ZERO_AFFORDABLE_QUANTITY')
        self.assertIsNone(self.leases.owner('SPY'))

    def test_the_proposal_cannot_raise_its_own_quantity_ceiling(self):
        instance = self.registry.instance('alpha')
        result = self.service.admit(instance, self.proposal(quantity_limit=99),
                                    allow_entries=True)
        self.assertEqual(result['quantity'], 2)

    def test_feedback_is_bound_to_the_admission(self):
        instance = self.registry.instance('alpha')
        result = self.service.admit(instance, self.proposal(), allow_entries=True)
        self.feedback.record({'event_id': 'f1', 'ref_id': result['ref_id'],
                              'kind': 'filled',
                              'occurred_at': AT.isoformat()})
        self.assertEqual(len(self.feedback.events('alpha')), 1)
        self.assertEqual(self.feedback.events('beta'), [])

    def test_settlement_releases_both_ownerships_together(self):
        instance = self.registry.instance('alpha')
        result = self.service.admit(instance, self.proposal(), allow_entries=True)
        settled = self.service.settle('alpha', result['trade_id'],
                                      authority=self.flat(result['trade_id']))
        self.assertTrue(settled['settled'])
        self.assertIsNone(self.leases.owner('SPY'))
        self.assertEqual(self.money.status()['open_cash'], '0')

    def test_an_unsettled_trade_is_not_released(self):
        instance = self.registry.instance('alpha')
        result = self.service.admit(instance, self.proposal(), allow_entries=True)
        self.leases.mark_unsettled('alpha', result['trade_id'], ['SPY'])
        settled = self.service.settle('alpha', result['trade_id'],
                                      authority=self.flat(result['trade_id']))
        self.assertFalse(settled['settled'])
        self.assertEqual(settled['reason'], 'SYMBOL_LEASE_UNSETTLED')
        self.assertEqual(self.leases.owner('SPY'), 'alpha')
        self.assertEqual(self.money.status()['open_cash'], '80.00')

    def test_the_account_ceiling_binds_across_instances(self):
        # Each instance ceiling is generous; the account only has room for two.
        wide_manifest = manifest('wide')
        for name in ('wide', 'wider', 'widest'):
            self.registry.register(name, wide_manifest,
                                   budget(cash='900', notional='900', loss='9', concurrent=4),
                                   mode='shadow', enabled=True)
        symbols = ['SPY', 'QQQ', 'IWM']
        results = []
        for name, symbol in zip(('wide', 'wider', 'widest'), symbols):
            results.append(self.service.admit(
                self.registry.instance(name),
                self.proposal(name, proposal_id='p-' + name, symbol=symbol),
                trade_id=name, allow_entries=True))
        self.assertTrue(results[0]['admitted'])
        self.assertTrue(results[1]['admitted'])
        self.assertFalse(results[2]['admitted'])
        self.assertEqual(results[2]['reason'], 'ADMISSION_BUDGET_REFUSED')
        self.assertIsNone(self.leases.owner('IWM'))
        self.assertEqual(self.money.status()['open_cash'], '160.00')
