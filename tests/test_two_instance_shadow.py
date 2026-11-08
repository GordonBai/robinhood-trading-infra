"""Two independent strategy instances on one account, in shadow, end to end.

This is the V1 multi-instance acceptance slice: one durable registry, one
account budget authority, one symbol-lease table, two deterministic fixture
plugins. It proves state, money and symbol ownership stay separated, that
over-allocation is refused by the account authority rather than by a plugin,
and that one instance's fault never silently advances another.

Synthetic only. No broker, no order, no real ledger.
"""
import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest import TestCase

from trading_contracts.instance import InstanceBudget, PluginManifest, StrategyInstance
from trading_contracts.strategy import ProposalAction, StrategyDecision, StrategyProposal
from trading_infra.attribution import ExecutionAttribution
from trading_infra.budget import AccountBudgetAuthority
from trading_infra.instance_host import InstanceStrategyHost
from trading_infra.instance_registry import InstanceRegistry
from trading_infra.symbol_lease import SymbolLease

ACCOUNT_LIMITS = {'max_open_cash': Decimal('200'), 'max_open_notional': Decimal('200'),
                  'max_open_planned_loss': Decimal('5.00'), 'max_concurrent_trades': 2}


class FixturePlugin:
    """Counts per symbol in the checkpoint and proposes one entry per new symbol."""

    def __init__(self, *, step=1, symbols=()):
        self.step = step
        self.symbols = set(symbols)

    def decide(self, request):
        event = request.market['event']
        symbol = event['data']['symbol']
        seen = dict(request.checkpoint.get('seen') or {})
        seen[symbol] = seen.get(symbol, 0) + self.step
        proposals = ()
        if symbol in self.symbols:
            proposals = (StrategyProposal(
                identity=request.identity,
                proposal_id='p-' + request.identity.instance_id + '-' + request.cursor,
                decision_id='d-' + request.identity.instance_id + '-' + request.cursor,
                setup_id='s-' + request.cursor,
                data_snapshot_id=request.data_snapshot_id,
                source_sha256=request.source_sha256,
                generated_at=request.as_of,
                valid_until=request.as_of + timedelta(seconds=20),
                action=ProposalAction.ENTER, symbol=symbol, reason='FIXTURE_ENTRY',
                synthetic=request.synthetic, quantity_limit=1,
                entry_ceiling=Decimal('40.00'), stop_price=Decimal('39.60')),)
        return StrategyDecision(identity=request.identity,
                                decision_id='d-' + request.identity.instance_id + '-' + request.cursor,
                                as_of=request.as_of, data_snapshot_id=request.data_snapshot_id,
                                consumed_cursor=request.cursor, synthetic=request.synthetic,
                                proposals=proposals, next_checkpoint={'seen': seen})


def manifest(plugin_id, version='1.0'):
    return PluginManifest(plugin_id=plugin_id, version=version, state_schema='fixture-1',
                          config={'step': '1'}, data_requirements=('quotes',),
                          capabilities=('advisory-entry',), max_inflight=64)


def budget(cash='100', loss='2.50', notional='100', concurrent=1):
    return InstanceBudget(max_cash=Decimal(cash), max_planned_loss=Decimal(loss),
                          max_notional=Decimal(notional), max_concurrent=concurrent)


class TwoInstanceShadowTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = self.temp.name + '/account.sqlite'
        self.db = sqlite3.connect(self.path, isolation_level=None)
        self.addCleanup(self.db.close)
        self.now = datetime(2026, 10, 29, 14, 0, tzinfo=timezone.utc)
        self.clock = lambda: self.now
        self.registry = InstanceRegistry(self.db, clock=self.clock)
        self.money = AccountBudgetAuthority(self.db, ACCOUNT_LIMITS, clock=self.clock)
        self.leases = SymbolLease(self.db, clock=self.clock)
        self.feedback = ExecutionAttribution(self.db, clock=self.clock)
        self.addCleanup(self.money.close)
        self.addCleanup(self.leases.close)
        self.registry.register('alpha', manifest('alpha-plugin'), budget(), mode='shadow', enabled=True)
        self.registry.register('beta', manifest('beta-plugin'), budget(), mode='shadow', enabled=True)
        self.alpha = InstanceStrategyHost(self.registry, self.registry.instance('alpha'), clock=self.clock)
        self.beta = InstanceStrategyHost(self.registry, self.registry.instance('beta'), clock=self.clock)

    def event(self, event_id, symbol):
        return {'id': event_id, 'kind': 'quote', 'at': self.now.isoformat(),
                'synthetic_input': True, 'source_sha256': 'source-' + event_id,
                'data': {'symbol': symbol, 'bid': '40.00', 'ask': '40.01'}}

    def run_instance(self, host, plugin, events):
        outcomes = []
        for event in events:
            prepared = host.prepare(event)
            if prepared is None:
                continue
            outcomes.append(host.commit(prepared, plugin.decide(prepared.request)))
        return outcomes

    # -- parallel replay -------------------------------------------------
    def test_both_instances_replay_the_same_stream_with_separate_state(self):
        events = [self.event('e1', 'SPY'), self.event('e2', 'QQQ')]
        self.run_instance(self.alpha, FixturePlugin(step=1), events)
        self.run_instance(self.beta, FixturePlugin(step=2), events)
        self.assertEqual(self.alpha.state(), {'seen': {'SPY': 1, 'QQQ': 1}})
        self.assertEqual(self.beta.state(), {'seen': {'SPY': 2, 'QQQ': 2}})

    def test_one_instance_failing_does_not_advance_the_other(self):
        events = [self.event('e1', 'SPY')]
        prepared = self.beta.prepare(events[0])
        class Exploding:
            def decide(self, request):
                raise ValueError('FIXTURE_PLUGIN_FAULT')
        with self.assertRaises(ValueError):
            self.beta.commit(prepared, Exploding().decide(prepared.request))
        self.assertEqual(self.beta.state(), {})
        self.run_instance(self.alpha, FixturePlugin(step=1), events)
        self.assertEqual(self.alpha.state(), {'seen': {'SPY': 1}})
        self.assertEqual(self.beta.state(), {})

    def test_a_disabled_instance_stops_advancing_without_touching_the_other(self):
        self.run_instance(self.alpha, FixturePlugin(step=1), [self.event('e1', 'SPY')])
        self.registry.set_mode('beta', 'disabled', enabled=False)
        disabled = InstanceStrategyHost(self.registry, self.registry.instance('beta'), clock=self.clock)
        with self.assertRaises(ValueError) as ctx:
            disabled.prepare(self.event('e1', 'SPY'))
        self.assertEqual(str(ctx.exception), 'INSTANCE_DISABLED')
        self.run_instance(self.alpha, FixturePlugin(step=1), [self.event('e2', 'QQQ')])
        self.assertEqual(self.alpha.state(), {'seen': {'SPY': 1, 'QQQ': 1}})
        self.assertEqual(self.registry.state('beta'), None)

    # -- symbol ownership ------------------------------------------------
    def test_contested_symbol_belongs_to_exactly_one_instance(self):
        first = self.leases.acquire('alpha', ['SPY'], trade_id='alpha-1')
        self.assertTrue(first['acquired'])
        second = self.leases.acquire('beta', ['SPY'], trade_id='beta-1')
        self.assertFalse(second['acquired'])
        self.assertEqual(second['reason'], 'SYMBOL_LEASE_HELD')
        self.assertEqual(second['holder']['instance_id'], 'alpha')
        self.assertEqual(self.leases.owner('SPY'), 'alpha')
        self.assertEqual(self.leases.holding('beta'), [])

    def test_partial_contention_leases_nothing(self):
        self.leases.acquire('alpha', ['SPY'], trade_id='alpha-1')
        result = self.leases.acquire('beta', ['SPY', 'QQQ'], trade_id='beta-1')
        self.assertFalse(result['acquired'])
        self.assertIsNone(self.leases.owner('QQQ'))
        self.assertEqual(self.leases.held(), [{'symbol': 'SPY', 'instance_id': 'alpha',
                                               'trade_id': 'alpha-1',
                                               'acquired_at': self.now.isoformat()}])

    def test_an_unsettled_lease_cannot_be_released(self):
        self.leases.acquire('alpha', ['SPY'], trade_id='alpha-1')
        self.leases.mark_unsettled('alpha', 'alpha-1', ['SPY'])
        guarded = self.leases.assert_releasable('alpha', 'alpha-1', reason='flat_confirmed')
        self.assertFalse(guarded['releasable'])
        self.assertEqual(guarded['reason'], 'SYMBOL_LEASE_UNSETTLED')
        self.assertEqual(self.leases.owner('SPY'), 'alpha')

    # -- account money ---------------------------------------------------
    def test_two_instances_cannot_exceed_the_account_ceiling(self):
        alpha = self.registry.instance('alpha')
        beta = self.registry.instance('beta')
        first = self.money.reserve(alpha, trade_id='alpha-1', cash=Decimal('100'),
                                   planned_loss=Decimal('2.50'), notional=Decimal('100'))
        self.assertTrue(first['reserved'])
        # Beta is inside its own 100 cash ceiling, but the account only has 200
        # and the notional ceiling is also 200, so a second full-size entry fits;
        # a third of the same size must not.
        second = self.money.reserve(beta, trade_id='beta-1', cash=Decimal('100'),
                                    planned_loss=Decimal('2.50'), notional=Decimal('100'))
        self.assertTrue(second['reserved'])
        third = self.money.reserve(alpha, trade_id='alpha-2', cash=Decimal('1'),
                                   planned_loss=Decimal('0'), notional=Decimal('0'))
        self.assertFalse(third['reserved'])
        self.assertIn(third['reason'], {'INSTANCE_CASH_LIMIT', 'INSTANCE_CONCURRENCY_LIMIT',
                                        'ACCOUNT_CONCURRENCY_LIMIT'})

    def test_account_ceiling_binds_even_when_each_instance_budget_is_generous(self):
        self.registry.set_mode('alpha', 'disabled', enabled=False)
        self.registry.set_mode('beta', 'disabled', enabled=False)
        wide = budget(cash='1000', loss='100', notional='1000', concurrent=5)
        self.registry.register('wide-a', manifest('wide-a'), wide, enabled=True)
        self.registry.register('wide-b', manifest('wide-b'), wide, enabled=True)
        wide_a = self.registry.instance('wide-a')
        wide_b = self.registry.instance('wide-b')
        self.assertTrue(self.money.reserve(wide_a, trade_id='a1', cash=Decimal('150'),
                                           planned_loss=Decimal('2'), notional=Decimal('150'))['reserved'])
        blocked = self.money.reserve(wide_b, trade_id='b1', cash=Decimal('100'),
                                     planned_loss=Decimal('2'), notional=Decimal('100'))
        self.assertFalse(blocked['reserved'])
        self.assertIn(blocked['reason'], {'ACCOUNT_CASH_LIMIT', 'ACCOUNT_NOTIONAL_LIMIT',
                                          'ACCOUNT_CONCURRENCY_LIMIT'})
        state = self.money.status()
        self.assertEqual(Decimal(state['open_cash']), Decimal('150'))
        self.assertEqual(Decimal(state['per_instance']['wide-a']['open_cash']), Decimal('150'))
        # A refused reservation writes nothing at all, not even a zero row.
        self.assertNotIn('wide-b', state['per_instance'])

    def test_releasing_one_instance_never_frees_another(self):
        alpha = self.registry.instance('alpha')
        beta = self.registry.instance('beta')
        self.money.reserve(alpha, trade_id='a1', cash=Decimal('90'),
                           planned_loss=Decimal('1'), notional=Decimal('90'))
        self.money.reserve(beta, trade_id='b1', cash=Decimal('90'),
                           planned_loss=Decimal('1'), notional=Decimal('90'))
        self.assertTrue(self.money.release('alpha', 'a1'))
        state = self.money.status()
        self.assertEqual(Decimal(state['open_cash']), Decimal('90'))
        self.assertEqual(Decimal(state['per_instance']['alpha']['open_cash']), Decimal('0'))
        self.assertEqual(Decimal(state['per_instance']['beta']['open_cash']), Decimal('90'))

    def test_full_round_trip_attaches_ownership_to_the_instance(self):
        event = self.event('e1', 'SPY')
        prepared = self.alpha.prepare(event)
        outcome = self.alpha.commit(prepared, FixturePlugin(symbols={'SPY'}).decide(prepared.request))
        proposal = outcome['proposals'][0]
        # The acknowledgement is a JSON-safe summary of instance-owned advice.
        self.assertEqual(proposal['action'], 'ENTER')
        self.assertEqual(proposal['symbol'], 'SPY')
        lease = self.leases.acquire('alpha', [proposal['symbol']], trade_id='alpha-1')
        self.assertTrue(lease['acquired'])
        reservation = self.money.reserve(self.registry.instance('alpha'), trade_id='alpha-1',
                                         cash=Decimal('40.00'),
                                         planned_loss=Decimal('0.40'), notional=Decimal('40.00'))
        self.assertTrue(reservation['reserved'])
        self.assertEqual(self.leases.owner('SPY'), 'alpha')
        self.assertEqual(self.money.instance_status('alpha')['open_trades'], 1)
        # No reservation record is not the same fact as "zero open trades";
        # the authority refuses to imply one from the absence of the other.
        with self.assertRaises(ValueError) as ctx:
            self.money.instance_status('beta')
        self.assertEqual(str(ctx.exception), 'BUDGET_INSTANCE_UNKNOWN')

    # -- execution feedback routing --------------------------------------
    def test_feedback_follows_the_reference_not_the_symbol(self):
        self.feedback.bind('ref-alpha-1', 'alpha', 'alpha-1')
        self.feedback.record({'event_id': 'f1', 'ref_id': 'ref-alpha-1', 'kind': 'filled',
                              'occurred_at': self.now.isoformat(), 'symbol': 'SPY'})
        self.assertEqual(len(self.feedback.events('alpha')), 1)
        self.assertEqual(self.feedback.events('beta'), [])

    def test_an_unbound_reference_from_another_instance_is_refused(self):
        self.feedback.bind('ref-alpha-1', 'alpha', 'alpha-1')
        with self.assertRaises(ValueError) as ctx:
            self.feedback.record({'event_id': 'f2', 'ref_id': 'ref-beta-9', 'kind': 'filled',
                                  'occurred_at': self.now.isoformat(), 'symbol': 'SPY'})
        self.assertEqual(str(ctx.exception), 'ATTRIBUTION_REF_UNKNOWN')
