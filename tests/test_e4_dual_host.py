"""078r2 offline probe: two symbols through the default Host bridge.

Test-only seed/entry authority is used only to create already-owned trades.
After that the production ExecutionBook/BrokerJournal/default ports/
HostExecutionBridge/HostDeliveryDispatcher/LiveSupervisor path manages both
symbols.  Local fake Host/broker only; no actual tool or broker call.
"""
import json
import tempfile
import time
from decimal import Decimal
from pathlib import Path
from unittest import TestCase

from entry_authority_fixture import TestExecutionBook
from martin_agentic.broker import BrokerJournal
from martin_agentic.execution import ExecutionBook
from martin_agentic.host_execution import HostExecutionBridge
from martin_luk.live_session import (
    HostDeliveryDispatcher, LiveSupervisor, default_live_ports)
from test_broker import FakeBroker, snapshot as broker_snapshot
from test_default_lifecycle import Host, MarketPort, NOW, TradingClock
from test_full_lifecycle import (manifest, budget, ACCOUNT_LIMITS, POLICY,
                                ReportingDispatcher, SymbolPlugin)
from test_instance_host import CountingPlugin
from test_instance_recovery import DecisionRunner
from trading_infra.instance_registry import InstanceRegistry
from trading_infra.instance_runtime import InstanceRuntime
from trading_infra.instance_supervisor import InstanceSupervisor
from trading_infra.admission import AdmissionService
from trading_infra.attribution import ExecutionAttribution
from trading_infra.budget import AccountBudgetAuthority
from trading_infra.instance_accounting import InstanceAccounting
from trading_infra.symbol_lease import SymbolLease
from test_supervised_engineering import plan, quote
from test_supervised_host_default_loop import (
    SimulationReviewTransport, SimulationWake)


class MultiBroker:
    def __init__(self, clock):
        self.clock = clock
        self.orders = []
        self.cash = Decimal('500')
        self.positions = {}
        self.mark_prices = {'DEMO': Decimal('40'), 'SPY': Decimal('40')}
        self.unconfirmed_protections = set()
        self.fill_queue = {}
        self.ignore_cancel_orders = set()
        self.snapshot_calls = 0

    def set_mark(self, symbol, price):
        self.mark_prices[symbol] = Decimal(str(price))

    def equity(self):
        return self.cash + sum(
            qty * self.mark_prices.get(symbol, Decimal('40'))
            for symbol, qty in self.positions.items())

    def add(self, order):
        self.orders.append(order)

    def cancel(self, order_id):
        if order_id in self.ignore_cancel_orders:
            return True
        for order in self.orders:
            if order['id'] == order_id and order['state'] in {
                    'submitted', 'confirmed', 'partially_filled',
                    'pending_cancelled'}:
                order['state'] = 'pending_cancelled'
                return True
        return False

    def held(self, symbol):
        return sum(Decimal(str(order['quantity'])) for order in self.orders
                   if order['symbol'] == symbol and order['side'] == 'sell'
                   and order['trigger'] == 'stop'
                   and order['state'] in {'submitted', 'confirmed',
                                          'partially_filled',
                                          'pending_cancelled'})

    def snapshot(self):
        self.snapshot_calls += 1
        for order in self.orders:
            if order['state'] in {'submitted', 'partially_filled'}:
                if (order['state'] == 'submitted' and order['side'] == 'sell'
                        and order['trigger'] == 'stop'):
                    if order['symbol'] in self.unconfirmed_protections:
                        continue
                    if not order.get('submitted_seen'):
                        order['submitted_seen'] = True
                        continue
                    order['state'] = 'confirmed'
                    continue
                if order['side'] == 'buy':
                    qty = Decimal(str(order['quantity']))
                    price = Decimal(str(order.get('price') or '40'))
                    configured = order['id'] in self.fill_queue
                    queue = (list(self.fill_queue[order['id']])
                             if configured else [qty])
                    if configured and not queue:
                        continue
                    fill_qty = Decimal(str(queue.pop(0)))
                    if fill_qty <= 0 or fill_qty > qty:
                        raise ValueError('FIXTURE_FILL_QUANTITY_INVALID')
                    cumulative = Decimal(str(order.get(
                        'cumulative_quantity') or '0')) + fill_qty
                    order['cumulative_quantity'] = str(cumulative)
                    fill_number = len(order.get('executions') or ()) + 1
                    order.setdefault('executions', []).append({
                        'id': order['id'] + '-exec' + (
                            '' if fill_number == 1 else '-' + str(fill_number)),
                        'symbol': order['symbol'], 'side': 'buy',
                        'quantity': str(fill_qty), 'price': str(price),
                        'fees': '0.05',
                        'timestamp': self.clock().isoformat(),
                        'order_id': order['id']})
                    self.positions[order['symbol']] = (
                        self.positions.get(order['symbol'], Decimal(0)) + fill_qty)
                    self.cash -= fill_qty * price + Decimal('0.05')
                    if cumulative < qty:
                        order['state'] = 'partially_filled'
                        if configured:
                            self.fill_queue[order['id']] = queue
                    else:
                        order['state'] = 'filled'
                        self.fill_queue.pop(order['id'], None)
            elif order['state'] == 'pending_cancelled':
                if not order.get('pending_cancel_seen'):
                    order['pending_cancel_seen'] = True
                    continue
                order['state'] = 'cancelled'
        fake = FakeBroker()
        fake.orders = list(self.orders)
        fake.cash = str(self.cash)
        fake.positions = []
        for symbol, qty in self.positions.items():
            held = self.held(symbol)
            fake.positions.append({
                'symbol': symbol, 'quantity': str(qty),
                'shares_available_for_sells': str(qty - held),
                'shares_held_for_sells': str(held),
                'average_buy_price': '40'})
        def transport(tool, args):
            response = fake(tool, args)
            if tool == 'get_portfolio':
                data = response['data']
                data['cash'] = str(self.cash)
                data['total_value'] = str(self.equity())
                data['buying_power'] = {
                    'display_currency': 'USD',
                    'buying_power': str(self.cash),
                    'unleveraged_buying_power': str(self.cash),
                }
            return response

        transport.bind = fake.bind
        value = broker_snapshot(transport, self.clock().isoformat())
        value['synthetic'] = True
        return value


class DualHostTests(TestCase):
    def _seed(self, db, policy, clock, state, *, demo_quantity=1):
        # This seed intentionally uses the production admission/control loop
        # (not book.propose plus feedback.bind) so reservations, leases,
        # execution intents and attribution are durable before management.
        book = TestExecutionBook(db, policy, simulation=True, clock=clock)
        registry = InstanceRegistry(book.db, clock=clock)
        registry.register('alpha', manifest('alpha'), budget(),
                          mode='shadow', enabled=True)
        registry.register('beta', manifest('beta'), budget(),
                          mode='shadow', enabled=True)
        money = AccountBudgetAuthority(book.db, ACCOUNT_LIMITS, clock=clock)
        leases = SymbolLease(book.db, clock=clock)
        feedback = ExecutionAttribution(book.db, clock=clock)
        accounting = InstanceAccounting(book.db, clock=clock)
        admission = AdmissionService(
            budget=money, leases=leases, attribution=feedback,
            policy=POLICY, registry=registry, clock=clock)
        runtimes = [
            InstanceRuntime(registry, registry.instance('alpha'),
                            runner=DecisionRunner(SymbolPlugin(
                                'DEMO', proposal_at=1,
                                quantity_limit=demo_quantity)),
                            source_fingerprint='f' * 64, clock=clock),
            InstanceRuntime(registry, registry.instance('beta'),
                            runner=DecisionRunner(SymbolPlugin('SPY', proposal_at=1)),
                            source_fingerprint='f' * 64, clock=clock),
        ]
        dispatcher = ReportingDispatcher(book)
        supervisor = LiveSupervisor(
            book, dispatcher=dispatcher, process_strategy=False,
            agent_mode='review', owner_source='f' * 64,
            instance_supervisor=InstanceSupervisor(runtimes),
            admission=admission, accounting=accounting)
        try:
            for _ in range(12):
                at = clock().isoformat()
                events = []
                for symbol in ('DEMO', 'SPY'):
                    events.append({
                        'id': 'seed-' + symbol + '-' + at, 'kind': 'quote',
                        'at': at, 'synthetic_input': True,
                        'source_sha256': 'fixture-' + symbol,
                        'data': {'symbol': symbol, 'bid': '39.99',
                                 'ask': '40.00', 'bid_at': at,
                                 'ask_at': at, 'observed_at': at}})
                supervisor.step(state.snapshot(), events, allow_entries=True)
                entries = [action for action in book.actions()
                           if action['kind'] == 'entry'
                           and action['state'] == 'REPORTED']
                if len(entries) == 2:
                    break
            payloads = {}
            for action in book.actions():
                if action['kind'] != 'entry':
                    continue
                payload = json.loads(action['payload'])
                payloads[payload['parameters']['symbol']] = payload
            intents = book.db.execute(
                'SELECT state FROM execution_intents ORDER BY created_at').fetchall()
            self.assertEqual(len(payloads), 2, book.state())
            self.assertTrue(intents)
            self.assertEqual({r[0] for r in intents}, {'DISPATCHED'},
                             intents)
            return payloads
        finally:
            supervisor.close()
            for runtime in runtimes:
                runtime.close()
            book.close()

    def test_two_symbols_through_default_host_bridge(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name) / 'workspace'
        root.mkdir()
        clock = TradingClock(NOW)
        state = MultiBroker(clock)
        policy = json.loads(
            Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        policy.update(capture_only=False, live_enabled=False,
                      strategy_instances=[
                          {'instance_id': 'alpha', 'plugin': 'vwap-fixture'},
                          {'instance_id': 'beta', 'plugin': 'martin'}])
        db = root / 'book.sqlite'
        payloads = self._seed(db, policy, clock, state)
        for payload in payloads.values():
            params = payload['parameters']
            state.add({
                'id': 'seed-' + payload['parameters']['symbol'], 'symbol':
                params['symbol'], 'side': 'buy',
                'quantity': params['quantity'],
                'cumulative_quantity': '0', 'type': 'limit',
                'trigger': 'immediate', 'price': params['limit_price'],
                'stop_price': None, 'created_at': clock().isoformat(),
                'state': 'submitted', 'market_hours': params['market_hours'],
                'time_in_force': params['time_in_force'],
                'ref_id': payload['ref_id'], 'executions': []})
        book = ExecutionBook(db, policy, simulation=True, clock=clock)
        self.addCleanup(book.close)
        journal = BrokerJournal(db, allow_synthetic=True)
        self.addCleanup(journal.close)
        registry = InstanceRegistry(book.db, clock=clock)
        registry.register('alpha', manifest('alpha'), budget(),
                          mode='shadow', enabled=True)
        registry.register('beta', manifest('beta'), budget(),
                          mode='shadow', enabled=True)
        money = AccountBudgetAuthority(book.db, ACCOUNT_LIMITS, clock=clock)
        leases = SymbolLease(book.db, clock=clock)
        feedback = ExecutionAttribution(book.db, clock=clock)
        accounting = InstanceAccounting(book.db, clock=clock)
        admission = AdmissionService(
            budget=money, leases=leases, attribution=feedback,
            policy=POLICY, registry=registry, clock=clock)
        runtimes = []
        for instance_id in ('alpha', 'beta'):
            runtimes.append(InstanceRuntime(
                registry, registry.instance(instance_id),
                runner=DecisionRunner(CountingPlugin(proposal_at=None)),
                source_fingerprint='f' * 64, clock=clock))
            self.addCleanup(runtimes[-1].close)
        before_budget = money.status()
        self.assertEqual(before_budget['open_cash'], '80.00')
        self.assertEqual(before_budget['open_notional'], '80.00')
        self.assertEqual(before_budget['open_planned_loss'], '0.90')
        self.assertEqual(before_budget['open_trades'], 2)
        self.assertEqual(leases.owner('DEMO'), 'alpha')
        self.assertEqual(leases.owner('SPY'), 'beta')
        intents = book.db.execute(
            'SELECT instance_id,trade_id,ref_id,symbol,state '
            'FROM execution_intents ORDER BY symbol').fetchall()
        self.assertEqual(len(intents), 2, intents)
        self.assertEqual({row[4] for row in intents}, {'DISPATCHED'})
        self.assertEqual({row[0] for row in intents}, {'alpha', 'beta'})
        for payload in payloads.values():
            owner = 'alpha' if payload['parameters']['symbol'] == 'DEMO' else 'beta'
            self.assertEqual(feedback.owner(payload['ref_id'])['instance_id'],
                             owner)
        host_dir = root / 'output' / 'host-execution'
        bridge = HostExecutionBridge(
            book, host_dir, review_factory=SimulationReviewTransport,
            wake_adapter=SimulationWake())
        self.addCleanup(bridge.close)
        host = Host(host_dir, state, clock=clock)
        host.start()
        self.addCleanup(host.join, 1)
        self.addCleanup(host.stop.set)
        ports = default_live_ports(
            book, policy, clock, dispatcher=bridge, host_surface=bridge,
            broker_surface=state.snapshot, market_surface=MarketPort(clock))
        dispatcher = HostDeliveryDispatcher(book, ports.host, bridge)
        supervisor = LiveSupervisor(
            book, dispatcher=dispatcher, agent_mode='supervised-host',
            owner_source='f' * 64, ports=ports, process_strategy=False,
            instance_supervisor=InstanceSupervisor(runtimes),
            admission=admission, accounting=accounting)
        self.addCleanup(supervisor.close)

        def event(at, symbol):
            return {'id': 'q-' + symbol + '-' + at, 'kind': 'quote',
                    'at': at, 'synthetic_input': True,
                    'source_sha256': 'fixture-' + symbol,
                    'data': {'symbol': symbol, 'bid': '40.00',
                             'ask': '40.00', 'bid_at': at, 'ask_at': at,
                             'observed_at': at}}
        for _ in range(20):
            at = clock().isoformat()
            snapshot = state.snapshot()
            journal.ingest(snapshot)
            supervisor.step(snapshot,
                            [event(at, 'DEMO'), event(at, 'SPY')],
                            allow_entries=False)
            time.sleep(.02)
        trades = book.state()['trades']
        self.assertEqual(len(trades), 2, trades)
        self.assertEqual({trade['plan']['symbol'] for trade in trades.values()},
                         {'DEMO', 'SPY'})
        for symbol, trade in trades.items():
            self.assertEqual(trade['phase'], 'PROTECTED', trade)
            self.assertEqual(trade['first_protection_observed_quantity'],
                             '1')
        protects = [action for action in book.actions()
                    if action['kind'] == 'protect']
        self.assertEqual(len(protects), 2)
        self.assertEqual(len(state.orders), 4)
        self.assertEqual({order['symbol'] for order in state.orders},
                         {'DEMO', 'SPY'})
        self.assertEqual(book.db.execute(
            'SELECT count(*) FROM broker_executions').fetchone()[0], 2)
        self.assertEqual(len(accounting.fills('alpha')), 1)
        self.assertEqual(len(accounting.fills('beta')), 1)
        self.assertEqual(accounting.instance_summary('alpha')['open_quantity'],
                         '1')
        self.assertEqual(accounting.instance_summary('beta')['open_quantity'],
                         '1')
        self.assertEqual(accounting.instance_summary('alpha')['fees'], '0.05')
        self.assertEqual(accounting.instance_summary('beta')['fees'], '0.05')
        self.assertEqual(str(state.cash), '419.90')
        after_budget = money.status()
        self.assertEqual(after_budget['open_cash'], before_budget['open_cash'])
        self.assertEqual(after_budget['open_notional'],
                         before_budget['open_notional'])
        self.assertEqual(after_budget['open_planned_loss'],
                         before_budget['open_planned_loss'])
        self.assertEqual(after_budget['open_trades'], 2)
        self.assertEqual(leases.owner('DEMO'), 'alpha')
        self.assertEqual(leases.owner('SPY'), 'beta')
