"""The whole trading lifecycle driven by the control loop and a fake broker.

Nothing here calls ``book.propose``, ``record_fill`` or ``admission.settle`` to
drive the business: the loop admits, dispatches, reconciles, imports receipts,
attributes them and releases on authoritative evidence. The only test-side
actors are the two the daemon owns — a dispatcher that claims and reports one
action, and a broker that answers with snapshots.

Synthetic fixtures and a temporary database; no broker or market request.
"""
import json
import tempfile
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest import TestCase

from entry_authority_fixture import TestExecutionBook as ExecutionBook
from martin_luk.live_session import LiveSupervisor
from test_broker import FakeBroker, position
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
ACCOUNT_LIMITS = {'max_open_cash': Decimal('500'), 'max_open_notional': Decimal('500'),
                  'max_open_planned_loss': Decimal('10.00'),
                  'max_concurrent_trades': 4}
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


class SymbolPlugin(CountingPlugin):
    """One active instance only proposes for its configured symbol."""
    def __init__(self, symbol, *, step=1, proposal_at=None, quantity_limit=1):
        super().__init__(step=step, proposal_at=proposal_at,
                         quantity_limit=quantity_limit)
        self.symbol = symbol

    def decide(self, request):
        decision = super().decide(request)
        if request.market['event'].get('data', {}).get('symbol') != self.symbol:
            decision = type(decision)(**{**decision.__dict__, 'proposals': ()})
        return decision


class ReportingDispatcher:
    """Stands in for the daemon's worker: claim one action, report it submitted."""

    def __init__(self, book):
        self.book = book
        self.reported = []

    def __call__(self, *, allow_entries=True, allow_dispatch=True):
        if not allow_dispatch:
            return {'agent_called': False, 'reason': 'FRESH_BROKER_OBSERVATION_REQUIRED'}
        action = self.book.claim(allow_entries=allow_entries)
        if not action:
            return {'agent_called': False, 'reason': 'NO_DISPATCHABLE_ACTION'}
        self.book.report(action['action_id'], {'status': 'submitted',
                                               'message': 'synthetic submission'})
        self.reported.append(action['action_id'])
        return {'agent_called': True, 'action_id': action['action_id']}


class FullLifecycleTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.now = AT
        self.policy = json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        self.book = ExecutionBook(Path(self.temp.name) / 'life.sqlite', self.policy,
                                  simulation=True, clock=lambda: self.now)
        self.addCleanup(self.book.close)
        self.db = self.book.db
        registry = InstanceRegistry(self.db, clock=lambda: self.now)
        registry.register('alpha', manifest(), budget(), mode='shadow', enabled=True)
        self.registry = registry
        runtime = InstanceRuntime(registry, registry.instance('alpha'),
                                  runner=DecisionRunner(CountingPlugin(proposal_at=1)),
                                  source_fingerprint='f' * 64, clock=lambda: self.now)
        self.addCleanup(runtime.close)
        self.runtime = runtime
        self.money = AccountBudgetAuthority(self.db, ACCOUNT_LIMITS, clock=lambda: self.now)
        self.addCleanup(self.money.close)
        self.leases = SymbolLease(self.db, clock=lambda: self.now)
        self.addCleanup(self.leases.close)
        self.feedback = ExecutionAttribution(self.db, clock=lambda: self.now)
        self.accounting = InstanceAccounting(self.db, clock=lambda: self.now)
        self.admission = AdmissionService(budget=self.money, leases=self.leases,
                                          attribution=self.feedback, policy=POLICY,
                                          registry=registry, clock=lambda: self.now)
        self.dispatcher = ReportingDispatcher(self.book)
        self.supervisor = LiveSupervisor(self.book, dispatcher=self.dispatcher,
                                         process_strategy=False, agent_mode='review',
                                         owner_source='f' * 64,
                                         instance_supervisor=InstanceSupervisor([runtime]),
                                         admission=self.admission,
                                         accounting=self.accounting)

    # -- fake broker ------------------------------------------------------
    def snap(self, orders=(), *, qty='0', held='0'):
        fake = FakeBroker()
        fake.orders = list(orders)
        fake.positions = [] if qty == '0' else [position(qty, held)]
        value = broker_snapshot(fake, self.now.isoformat())
        value['synthetic'] = True
        return value

    def snap_multi(self, orders=(), *, positions=()):
        fake = FakeBroker()
        fake.orders = list(orders)
        fake.positions = list(positions)
        value = broker_snapshot(fake, self.now.isoformat())
        value['synthetic'] = True
        return value

    def event(self, *, symbol='DEMO', bid='39.99', ask='40.00'):
        at = self.now.isoformat()
        return {'id': 'e-' + at, 'kind': 'quote', 'at': at, 'synthetic_input': True,
                'source_sha256': 'source-' + at,
                'data': {'symbol': symbol, 'bid': bid, 'ask': ask,
                         'bid_at': at, 'ask_at': at, 'observed_at': at}}

    def broker_order(self, action, *, filled, state='filled', sides=None):
        payload = json.loads(action['payload'])
        parameters = payload['parameters']
        # The book speaks the provider's vocabulary: a stop is a market order
        # with a stop trigger, not a "stop_market" order type.
        kind, trigger = {'limit': ('limit', 'immediate'),
                         'market': ('market', 'immediate'),
                         'stop_market': ('market', 'stop'),
                         'stop_limit': ('limit', 'stop')}[parameters['type']]
        raw = {'id': 'broker-' + action['id'], 'symbol': parameters['symbol'],
               'side': parameters['side'], 'quantity': parameters['quantity'],
               'cumulative_quantity': filled, 'type': kind,
               'trigger': trigger, 'price': parameters.get('limit_price'),
               'stop_price': parameters.get('stop_price'),
               'created_at': self.now.isoformat(), 'state': state,
               'market_hours': parameters['market_hours'],
               'time_in_force': parameters['time_in_force'],
               'executions': [] if filled == '0' else [{
                   'id': 'exec-' + action['id'], 'price': '40',
                   'quantity': filled, 'fees': '0',
                   'timestamp': (self.now - timedelta(seconds=5)).isoformat()}]}
        raw['ref_id'] = payload['ref_id']
        return raw

    def step(self, orders=(), *, qty='0', held='0', bid='39.99', ask='40.00',
             ticks=1, entries=True, dispatch=True):
        last = None
        for _ in range(ticks):
            last = self.supervisor.step(self.snap(orders, qty=qty, held=held),
                                        [self.event(bid=bid, ask=ask)],
                                        allow_entries=entries, allow_dispatch=dispatch)
        return last

    def action(self, kind):
        found = [item for item in self.book.actions() if item['kind'] == kind]
        return found[-1] if found else None

    def test_two_active_instances_each_consume_independent_broker_fills(self):
        self.registry.register('beta', manifest('beta'), budget(),
                               mode='shadow', enabled=True)
        beta = InstanceRuntime(
            self.registry, self.registry.instance('beta'),
            runner=DecisionRunner(SymbolPlugin('SPY', proposal_at=1)),
            source_fingerprint='f' * 64, clock=lambda: self.now)
        self.addCleanup(beta.close)
        alpha = self.runtime
        supervisor = LiveSupervisor(
            self.book, dispatcher=self.dispatcher, process_strategy=False,
            agent_mode='review', owner_source='f' * 64,
            instance_supervisor=InstanceSupervisor([alpha, beta]),
            admission=self.admission, accounting=self.accounting)

        def event(symbol):
            at = self.now.isoformat()
            return {'id': 'e-' + symbol + '-' + at, 'kind': 'quote', 'at': at,
                    'synthetic_input': True, 'source_sha256': 'source-' + symbol,
                    'data': {'symbol': symbol, 'bid': '39.99', 'ask': '40.00',
                             'bid_at': at, 'ask_at': at, 'observed_at': at}}

        def step(orders=(), positions=(), ticks=1):
            for _ in range(ticks):
                supervisor.step(
                    self.snap_multi(orders, positions=positions),
                    [event('DEMO'), event('SPY')], allow_entries=True)

        step(ticks=8)
        entries = {json.loads(a['payload'])['parameters']['symbol']: a
                   for a in self.book.actions() if a['kind'] == 'entry'}
        self.assertEqual(set(entries), {'DEMO', 'SPY'}, entries)
        submitted = [self.broker_order(entries[s], filled='0',
                                       state='confirmed')
                     for s in ('DEMO', 'SPY')]
        step(submitted, ticks=2)
        filled = [self.broker_order(entries[s],
                                    filled=json.loads(entries[s]['payload'])[
                                        'parameters']['quantity'])
                  for s in ('DEMO', 'SPY')]
        positions = [
            {'symbol': 'DEMO', 'quantity': '1',
             'shares_available_for_sells': '1',
             'shares_held_for_sells': '0', 'average_buy_price': '40'},
            {'symbol': 'SPY', 'quantity': '1',
             'shares_available_for_sells': '1',
             'shares_held_for_sells': '0', 'average_buy_price': '40'}]
        step(filled, positions=positions, ticks=3)
        self.assertEqual(len(self.accounting.fills('alpha')), 1,
                         self.accounting.fills('alpha'))
        self.assertEqual(len(self.accounting.fills('beta')), 1,
                         self.accounting.fills('beta'))
        self.assertEqual(self.accounting.instance_summary('alpha')['open_quantity'],
                         '1')
        self.assertEqual(self.accounting.instance_summary('beta')['open_quantity'],
                         '1')
        owners = {ref: self.feedback.owner_for_trade(trade)
                  for ref, trade in ()}
        refs = [json.loads(a['payload'])['ref_id']
                for a in self.book.actions() if a['kind'] == 'entry']
        self.assertEqual({self.feedback.owner(ref)['instance_id'] for ref in refs},
                         {'alpha', 'beta'})

    def test_entry_fill_protection_exit_and_release_run_through_the_loop(self):
        # 1. Advice admitted and dispatched into a real entry action.
        step = self.step(ticks=4)
        entry = self.action('entry')
        self.assertIsNotNone(entry, 'the loop must create an entry action')
        ref = json.loads(entry['payload'])['ref_id']
        self.assertTrue(self.feedback.owner(ref), 'the entry reference is attributed')

        # 2. Broker confirms and fills it.
        quantity = json.loads(entry['payload'])['parameters']['quantity']
        submitted = self.broker_order(entry, filled='0', state='confirmed')
        self.step([submitted], ticks=1)
        filled = self.broker_order(entry, filled=quantity)
        self.step([filled], qty=quantity, ticks=2)
        self.assertTrue([item for item in self.accounting.fills('alpha')],
                        'the fill is booked from the broker receipt')

        # 3. The book creates native protection; the broker confirms it covers
        #    the whole quantity.
        protection = self.action('protect')
        self.assertIsNotNone(protection, 'a filled entry must be protected')
        self.assertTrue(self.feedback.owner(json.loads(protection['payload'])['ref_id']),
                        'the protection reference belongs to the same instance')
        protective = self.broker_order(protection, filled='0', state='confirmed')
        self.step([filled, protective], qty=quantity, ticks=2)
        kinds = [item['kind'] for item in self.feedback.events('alpha')]
        self.assertIn('protection_covered', kinds)

        # 4. The market breaks the stop. The book must first cancel the resting
        #    stop; an accepted cancel is not terminal, so the exit only appears
        #    once the broker reports the stop cancelled.
        self.now = self.now + timedelta(minutes=5)
        self.step([filled, protective], qty=quantity, bid='38.90', ask='38.95', ticks=1)
        cancel = self.action('cancel')
        self.assertIsNotNone(cancel, 'a breaching quote must retire the resting stop')
        self.assertIsNone(self.action('exit'),
                          'no exit may be placed while the cancel is only accepted')
        cancelled_stop = self.broker_order(protection, filled='0', state='cancelled')
        self.step([filled, cancelled_stop], qty=quantity, bid='38.90', ask='38.95',
                  ticks=3)
        exit_ready = self.action('exit')
        self.assertIsNotNone(exit_ready, 'the exit follows the terminal cancel')

        # 5. The exit fills, the account goes flat and settlement releases.
        exit_order = self.broker_order(exit_ready, filled=quantity)
        # The account is flat only once the broker shows no position and both
        # the entry and the exit executions; the loop must see the same facts.
        self.step([filled, exit_order], qty='0', ticks=3)
        self.assertEqual(self.book.state()['trades'][entry['trade_id']]['phase'],
                         'FLAT_VERIFIED')
        kinds = [item['kind'] for item in self.feedback.events('alpha')]
        self.assertIn('exit', kinds)
        self.assertIn('flat', kinds)
        self.assertIsNone(self.leases.owner('DEMO'), 'ownership released after flat')
        self.assertEqual(self.money.status()['open_cash'], '0')
        summary = self.accounting.instance_summary('alpha')
        self.assertEqual(Decimal(summary['open_quantity']), Decimal('0'))
        self.assertFalse(self.db.in_transaction, 'no transaction left open')
