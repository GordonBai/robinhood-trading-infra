"""C2: default run_live_session proposal -> broker facts -> flat.

The scenario uses only the production composition: explicit synthetic source
ports, two configured plugin ids, the default Host delivery bridge and its
mailbox protocol, FakeBroker economics, and the ordinary Book reconciliation.
No test helper proposes, fills or settles an execution directly.
"""
import json
import sqlite3
import tempfile
import threading
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from martin_agentic.audit import digest
from martin_luk.live_session import LiveSupervisor, run_live_session
from martin_agentic.execution import ExecutionBook
from martin_agentic.broker import BrokerJournal
from martin_agentic.simulation import SimulationSources
from test_broker import FakeBroker, position
from test_broker import snapshot as broker_snapshot
from test_supervised_host_default_loop import (SimulationReviewTransport,
                                                SimulationWake)
from trading_infra.instance_accounting import InstanceAccounting

NOW = datetime(2026, 10, 16, 14, 0, tzinfo=timezone.utc)
TERMINAL = {'filled', 'cancelled', 'canceled', 'rejected', 'expired'}
FEE = '0.05'
OPEN_STOP_STATES = {'submitted', 'confirmed', 'partially_filled', 'pending_cancelled'}


def review_response(packet):
    return {'data': {**packet['parameters'], 'order_checks': {'alert_type': 'SYNTHETIC'},
                     'customer_approval_required': False,
                     'market_data_disclosure': 'SYNTHETIC_DISCLOSURE',
                     'quote_data': {}}}


def order_for_packet(packet, quantity=None):
    p = packet['parameters']
    return {'id': 'order-' + packet['operation_id'][-12:], 'symbol': p['symbol'],
            'side': p['side'], 'quantity': str(quantity or p['quantity']),
            'cumulative_quantity': '0',
            'type': 'market' if p['type'].startswith('stop_') else p['type'],
            'trigger': 'stop' if p.get('stop_price') else 'immediate',
            'price': p.get('limit_price'), 'stop_price': p.get('stop_price'),
            'created_at': NOW.isoformat(), 'state': 'submitted',
            'market_hours': p['market_hours'], 'time_in_force': p['time_in_force'],
            'ref_id': packet.get('logical_ref_id'), 'executions': []}


class BrokerState:
    """Small broker with real cash, holds, fees and order-state transitions."""

    def __init__(self, clock=None, *, protection_mode='confirmed',
                 protection_quantity=None):
        self.clock = clock or (lambda: NOW)
        self.protection_mode = protection_mode
        self.protection_quantity = protection_quantity
        self.fake = FakeBroker()
        self.fake.cash = '500'
        self.orders = []
        self.position_qty = Decimal(0)
        self.transitions = []
        self.samples = []

    def add(self, order):
        self.orders.append(order)
        self.transitions.append(('ORDER_ADDED', order['id'], order['side'],
                                 order['trigger'], order['quantity']))

    def _fill(self, order):
        price = Decimal(str(order['price'] or order.get('stop_price') or '40'))
        quantity = Decimal(str(order['quantity']))
        fee = Decimal(FEE)
        order['state'] = 'filled'
        order['cumulative_quantity'] = order['quantity']
        order['executions'] = [{'id': order['id'] + '-exec',
            'symbol': order['symbol'], 'side': order['side'],
            'quantity': order['quantity'], 'price': str(price),
            'fees': FEE, 'timestamp': self.clock().isoformat(),
            'order_id': order['id']}]
        if order['side'] == 'buy':
            self.position_qty += quantity
            self.fake.cash = str(Decimal(self.fake.cash) - quantity * price - fee)
        else:
            self.position_qty -= quantity
            self.fake.cash = str(Decimal(self.fake.cash) + quantity * price - fee)
        self.transitions.append(('FILLED', order['id'], str(quantity),
                                 str(price), FEE))

    def advance(self):
        for order in self.orders:
            if order['state'] == 'submitted':
                if order['side'] == 'sell' and order['trigger'] == 'stop':
                    if not order.get('submitted_seen'):
                        order['submitted_seen'] = True
                        self.transitions.append(('STOP_SUBMITTED_OBSERVED',
                                                 order['id']))
                        continue
                    if self.protection_mode == 'unconfirmed':
                        continue
                    if self.protection_mode == 'rejected':
                        order['state'] = 'rejected'
                        self.transitions.append(('STOP_REJECTED', order['id']))
                    else:
                        order['state'] = 'confirmed'
                        self.transitions.append(('STOP_CONFIRMED', order['id'],
                                                 order['quantity']))
                    continue
                self._fill(order)
            elif order['state'] == 'pending_cancelled':
                if not order.get('pending_cancel_seen'):
                    order['pending_cancel_seen'] = True
                    self.transitions.append(('CANCEL_PENDING_OBSERVED',
                                             order['id']))
                    continue
                order['state'] = 'cancelled'
                self.transitions.append(('CANCELLED', order['id']))

    def cancel(self, order_id):
        for order in self.orders:
            if order['id'] == order_id and order['state'] in OPEN_STOP_STATES:
                order['state'] = 'pending_cancelled'
                self.transitions.append(('CANCEL_ACCEPTED', order_id))
                return True
        return False

    def held_quantity(self):
        held = sum((Decimal(str(order['quantity'])) for order in self.orders
                    if order['side'] == 'sell' and order['trigger'] == 'stop'
                    and order['state'] in OPEN_STOP_STATES), Decimal(0))
        return min(self.position_qty, held)

    def snapshot(self):
        self.advance()
        self.fake.orders = list(self.orders)
        held = self.held_quantity()
        available = self.position_qty - held
        self.fake.positions = ([] if self.position_qty <= 0 else [{
            'symbol': 'DEMO', 'quantity': str(self.position_qty),
            'shares_available_for_sells': str(available),
            'shares_held_for_sells': str(held),
            'average_buy_price': '40'}])
        value = broker_snapshot(self.fake, self.clock().isoformat())
        value['synthetic'] = True
        cash = Decimal(self.fake.cash)
        equity = cash + self.position_qty * Decimal('40')
        value['portfolio']['cash'] = str(cash)
        value['portfolio']['total_value'] = str(equity)
        self.samples.append({
            'at': self.clock().isoformat(),
            'position_qty': str(self.position_qty), 'held': str(held),
            'available': str(available), 'cash': str(cash),
            'equity': str(equity),
            'orders': [(order['id'], order['side'], order['trigger'],
                        order['quantity'], order['state'])
                       for order in self.orders]})
        return value

    def open_orders(self):
        return [order for order in self.orders if order['state'] not in TERMINAL]

class BrokerPort:
    simulation_only = True

    def __init__(self, state):
        self.state = state
        self.calls = 0

    def __call__(self, approval_ids=()):
        self.calls += 1
        return self.state.snapshot()


class MarketPort:
    """Quote above session VWAP first, then below it to request the exit."""

    simulation_only = True

    def __init__(self, clock=None, *, exit_after=45, exit_gate=None):
        self.clock = clock or (lambda: NOW)
        self.calls = 0
        self.exit_after = exit_after
        self.exit_gate = exit_gate

    def __call__(self, *args, **kwargs):
        self.calls += 1
        at = self.clock().isoformat()
        low = (self.calls >= self.exit_after
               and (self.exit_gate is None or self.exit_gate()))
        bid = '39.99' if low else '40.00'
        return ([{'id': 'e-' + str(self.calls), 'kind': 'quote', 'at': at,
                  'synthetic_input': True, 'source_sha256': 'm-' + str(self.calls),
                  'data': {'symbol': 'DEMO', 'bid': bid, 'ask': '40.00',
                           'bid_at': at, 'ask_at': at, 'observed_at': at}}],
                {'quotes': 1})


class Host(threading.Thread):
    """Synthetic HostDelivery peer using the real mailbox protocol."""

    def __init__(self, directory, state, *, cancel_effect=True,
                 replay_receipts=False, clock=None, late_first_place=False,
                 protection_quantity=None, withhold_place=False,
                 withhold_protection_receipt=False,
                 withhold_protection_symbols=None):
        super().__init__(daemon=True)
        self.directory = Path(directory)
        self.state = state
        self.cancel_effect = cancel_effect
        self.replay_receipts = replay_receipts
        self.clock = clock or (lambda: NOW)
        self.late_first_place = late_first_place
        self.protection_quantity = protection_quantity
        self.withhold_place = withhold_place
        self.withhold_protection_receipt = withhold_protection_receipt
        self.withhold_protection_symbols = withhold_protection_symbols
        self.withheld_operation = None
        self.withheld_receipt = None
        self.place_packets = []
        self.late_used = False
        self.held_receipt = None
        self.held_operation = None
        self.write_after = 0
        self.stop = threading.Event()

    def run(self):
        while not self.stop.is_set():
            self.step()
            time.sleep(.01)

    def step(self):
        if (self.held_receipt is not None
                and time.monotonic() >= self.write_after):
            self.held_receipt.parent.mkdir(parents=True, exist_ok=True)
            self.held_receipt.write_text(self.held_text)
            self.held_receipt = None
        folder = self.directory / 'packets'
        if not folder.exists():
            return
        for path in sorted(folder.glob('*.json')):
            env = json.loads(path.read_text())
            packet = env['packet']
            # The Host peer uses the production mailbox freshness/epoch
            # validator, not a test-side ignore set or packet read bypass.
            from martin_agentic.host_mailbox import read_request_result
            current = read_request_result(
                self.directory, env['request_id'], clock=self.clock)
            if current.get('status') != 'PREPARED' \
                    or current.get('packet') != packet:
                continue
            op = packet['operation']
            late = False
            if (self.held_receipt is not None
                    and packet['operation_id'] == self.held_operation):
                continue
            if op == 'place' and packet['operation_id'] not in self.place_packets:
                self.place_packets.append(packet['operation_id'])
            if (op == 'place' and self.withheld_operation is not None
                    and packet['operation_id'] == self.withheld_operation):
                continue
            receipt = self.directory / 'receipts' / (packet['operation_id'] + '.json')
            if receipt.exists():
                if self.replay_receipts:
                    # Byte-identical replay must be idempotent at the importer.
                    receipt.write_text(receipt.read_text())
                continue
            request = json.loads(
                (self.directory / 'requests' / (env['request_id'] + '.json')).read_text())
            if op == 'review':
                response = review_response(packet)
            elif (op == 'place' and self.withhold_place
                  and packet['parameters'].get('side') == 'sell'
                  and packet['parameters'].get('type') == 'market'
                  and self.withheld_operation is None):
                # The Host accepted work but never returned a receipt or a
                # broker order. This is the conservative "may have been sent"
                # case for cross-restart UNKNOWN evidence.
                self.withheld_operation = packet['operation_id']
                self.place_packets.append(packet['operation_id'])
                self.clock.jump(16)
                continue
            elif op == 'cancel':
                if self.cancel_effect:
                    self.state.cancel(packet['parameters'].get('order_id', ''))
                # The Host report itself is never terminal proof.
                response = {'data': {'accepted': True}}
            else:
                quantity = (self.protection_quantity
                            if packet['parameters'].get('stop_price') else None)
                order = order_for_packet(packet, quantity=quantity)
                late = self.late_first_place and not self.late_used
                if late:
                    # Withhold the Host result long enough for the bridge to
                    # mark the handoff UNKNOWN. No broker order exists yet.
                    self.late_used = True
                    self.clock.jump(16)
                else:
                    self.state.add(order)
                response = {'structuredContent': {
                    'data': {'order': order, 'approval': None},
                    'guide': 'SYNTHETIC'}}
                if (self.withhold_protection_receipt
                        and packet['parameters'].get('stop_price')
                        and (self.withhold_protection_symbols is None
                             or packet['parameters'].get('symbol')
                             in self.withhold_protection_symbols)):
                    # The broker accepted the protection, but the local
                    # receipt is lost. Fresh broker reconciliation must recover
                    # the order without re-sending the place. Preserve the
                    # exact response for the later late-receipt test; it is not
                    # written into the normal receipts directory.
                    self.withheld_operation = packet['operation_id']
                    self.withheld_receipt = {'response': response}
                    self.place_packets.append(packet['operation_id'])
                    continue
            value = {'schema_version': 1, 'binding': env['binding'],
                     'request_id': env['request_id'],
                     'action_id': packet['action_id'],
                     'trade_id': request.get('trade_id'),
                     'operation_id': packet['operation_id'],
                     'packet_sha256': env['packet_sha256'],
                     'received_at': self.clock().isoformat(),
                     'execution_authorized': False,
                     'broker_effect_verified': False,
                     'response_sha256': digest(response),
                     'response': response}
            if late and op != 'review':
                self.held_receipt = receipt
                self.held_operation = packet['operation_id']
                self.held_text = json.dumps(value)
                self.write_after = time.monotonic() + .2
            else:
                receipt.parent.mkdir(parents=True, exist_ok=True)
                receipt.write_text(json.dumps(value))


class TradingClock:
    """Normal session time follows monotonic elapsed time, not a frozen value."""

    def __init__(self, start=NOW):
        self.start = start
        self.monotonic = time.monotonic()
        self.offset = 0.0

    def __call__(self):
        return self.start + timedelta(
            seconds=(time.monotonic() - self.monotonic) + self.offset)

    def jump(self, seconds):
        self.offset += seconds


class DefaultLifecycleTests(TestCase):
    def run_default(self, *, cancel_effect=True, replay_receipts=False,
                    cycles=120, late_first_place=False, starts_at=None,
                    protection_mode='confirmed', protection_quantity=None,
                    strategy_instances=None, market_exit_after=45,
                    require_protection_before_exit=False):
        clock = TradingClock(starts_at or NOW)
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        workspace = Path(temp.name) / 'workspace'
        state = BrokerState(clock, protection_mode=protection_mode,
                             protection_quantity=protection_quantity)
        exit_gate = None
        if require_protection_before_exit:
            exit_gate = lambda: any(
                order.get('side') == 'sell' and order.get('trigger') == 'stop'
                and order.get('state') == 'confirmed' for order in state.orders)
        market = MarketPort(clock, exit_after=market_exit_after,
                            exit_gate=exit_gate)
        host = Host(workspace / 'output' / 'host-execution', state,
                    cancel_effect=cancel_effect,
                    replay_receipts=replay_receipts, clock=clock,
                    late_first_place=late_first_place,
                    protection_quantity=protection_quantity)
        host.start()
        self.addCleanup(host.join, 1)
        self.addCleanup(host.stop.set)
        policy = json.loads(
            Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        policy.update(capture_only=False, live_enabled=False, strategy_instances=(
            strategy_instances if strategy_instances is not None else [
                {'instance_id': 'vwap-live', 'plugin': 'vwap-fixture'},
                {'instance_id': 'martin-observe', 'plugin': 'martin'}]))
        self.policy = policy
        sources = SimulationSources(broker=BrokerPort(state), market=market,
                                    review=SimulationReviewTransport,
                                    wake=SimulationWake())
        trace = []
        original_step = LiveSupervisor.step

        def observe(supervisor, *args, **kwargs):
            value = original_step(supervisor, *args, **kwargs)
            trades_now = supervisor.book.state().get('trades') or {}
            trace.append({trade_id: {
                'phase': item.get('phase'),
                'first_protection_observed_at': item.get(
                    'first_protection_observed_at'),
                'last_full_coverage_observed_at': item.get(
                    'last_full_coverage_observed_at'),
                'exit_requested': item.get('exit_requested')}
                for trade_id, item in trades_now.items()})
            return value

        with patch.object(LiveSupervisor, 'step', observe), \
                patch('martin_agentic.live_session.single_instance'), \
                patch('martin_agentic.live_session.wait_for_result_or_tick',
                      side_effect=lambda *a, **k: time.sleep(.05)), \
                patch('martin_agentic.live_session.notify_local'), \
                patch('martin_agentic.live_session.print'):
            result = run_live_session(
                workspace / 'book.sqlite', policy,
                symbols=['DEMO'] + [str(index) for index in range(9)],
                cycles=cycles, poll_seconds=5, agent_mode='supervised-host',
                simulation=True, simulation_workspace=workspace,
                simulation_sources=sources, clock=clock,
                status_file=workspace / 'status.json',
                output_dir=workspace / 'output')
        con = sqlite3.connect(workspace / 'book.sqlite')
        self.addCleanup(con.close)
        actions = con.execute(
            'SELECT kind,state,report,payload FROM live_actions').fetchall()
        trades = json.loads(con.execute(
            'SELECT payload FROM live_state WHERE id=1').fetchone()[0])['trades']
        return result, state, con, actions, trades, trace

    def test_default_runner_reaches_authoritative_flat(self):
        result, state, con, actions, trades, trace = self.run_default(
            replay_receipts=True)
        self.assertTrue(trades, (result.get('reason'), result.get('errors'),
                                actions, result.get('strategy_admissions')))
        trade = next(iter(trades.values()))
        self.assertEqual(trade['phase'], 'FLAT_VERIFIED',
                         (actions, result.get('errors'), trade))
        self.assertEqual(state.position_qty, 0)
        self.assertEqual(state.open_orders(), [])
        self.assertEqual(state.fake.positions, [])
        self.assertEqual(state.fake.approvals, [])
        self.assertEqual(float(state.fake.cash), 499.90)
        self.assertEqual({a[0] for a in actions},
                         {'entry', 'protect', 'cancel', 'exit'})
        self.assertNotIn('UNKNOWN', {a[1] for a in actions})
        self.assertNotIn('UNKNOWN', {row[0] for row in con.execute(
            'SELECT state FROM execution_intents').fetchall()})
        # One actual entry fill and one actual exit fill. Repeated broker
        # snapshots and repeated Host receipts are observations, not new fills.
        self.assertEqual(con.execute(
            'SELECT count(*) FROM broker_executions').fetchone()[0], 2)
        self.assertEqual(con.execute(
            'SELECT count(*) FROM execution_intents').fetchone()[0], 1)
        self.assertEqual(con.execute(
            'SELECT state FROM execution_intents').fetchone()[0], 'SETTLED')
        self.assertEqual(con.execute(
            'SELECT count(*) FROM account_budget_reservations').fetchone()[0], 0)
        self.assertEqual(con.execute(
            'SELECT count(*) FROM symbol_leases').fetchone()[0], 0)
        owners = {row[0] for row in con.execute(
            'SELECT instance_id FROM execution_attribution').fetchall()}
        self.assertEqual(owners, {'vwap-live'})
        accounting = InstanceAccounting(con, clock=lambda: NOW)
        summary = accounting.instance_summary('vwap-live')
        self.assertEqual(summary['fills'], 2, summary)
        self.assertEqual(summary['quantity_bought'], '1')
        self.assertEqual(summary['quantity_sold'], '1')
        self.assertEqual(summary['open_quantity'], '0')
        self.assertEqual(summary['fees'], '0.10')
        self.assertEqual(summary['realised_pnl'], '0')
        self.assertEqual(summary['net_cash_flow'], '-0.10')
        self.assertEqual(summary['trades'], ['e-1'])
        self.assertEqual(accounting.instance_summary('martin-observe')['fills'], 0)
        fill_ids = {row[0] for row in con.execute(
            'SELECT fill_id FROM instance_fills').fetchall()}
        broker_fill_ids = {row[0] for row in con.execute(
            'SELECT id FROM broker_executions').fetchall()}
        self.assertEqual(fill_ids, broker_fill_ids)
        self.assertEqual(len(fill_ids), 2)
        self.assertEqual(con.execute(
            'SELECT count(*) FROM execution_fill_identity').fetchone()[0], 2)
        self.assertEqual({row[0] for row in con.execute(
            'SELECT instance_id FROM execution_fill_identity').fetchall()},
            {'vwap-live'})
        execution_fees = sum((Decimal(json.loads(row[0])['fees'])
                              for row in con.execute(
                                  'SELECT payload FROM broker_executions').fetchall()),
                             Decimal(0))
        self.assertEqual(execution_fees, Decimal('0.10'))
        self.assertEqual(len(state.orders), 3)
        feedback = con.execute(
            'SELECT event_id,instance_id,kind FROM execution_feedback').fetchall()
        self.assertEqual(len({row[0] for row in feedback}), len(feedback))
        self.assertEqual({row[1] for row in feedback}, {'vwap-live'})
        kinds = Counter(row[2] for row in feedback)
        self.assertEqual(kinds, Counter({'filled': 4, 'protection_covered': 1,
                                         'cancel_terminal': 1, 'exit': 1,
                                         'flat': 1}), feedback)
        self.assertEqual(trade['first_protection_observed_quantity'], '1')
        self.assertEqual(trade['last_full_coverage_observed_quantity'], '1')
        self.assertTrue(trade['first_protection_observed_at'])
        self.assertTrue(trade['last_full_coverage_observed_at'])
        self.assertGreater(Decimal(trade['protection_observation_delay_seconds']),
                           Decimal(0))
        phases = [next(iter(row.values()))['phase'] for row in trace if row]
        self.assertIn('PROTECTED', phases, phases)
        self.assertLess(phases.index('PROTECTED'), phases.index('EXIT_REQUESTED'))
        self.assertLess(phases.index('EXIT_REQUESTED'), phases.index('FLAT_VERIFIED'))
        transitions = state.transitions
        confirmed = next(index for index, item in enumerate(transitions)
                         if item[0] == 'STOP_CONFIRMED')
        accepted = next(index for index, item in enumerate(transitions)
                        if item[0] == 'CANCEL_ACCEPTED')
        pending = next(index for index, item in enumerate(transitions)
                       if item[0] == 'CANCEL_PENDING_OBSERVED')
        cancelled = next(index for index, item in enumerate(transitions)
                         if item[0] == 'CANCELLED')
        exit_fill = next(index for index, item in enumerate(transitions)
                         if item[0] == 'FILLED' and item[1] !=
                         next(order['id'] for order in state.orders
                              if order['side'] == 'buy'))
        self.assertLess(confirmed, accepted)
        self.assertLess(accepted, pending)
        self.assertLess(pending, cancelled)
        self.assertLess(cancelled, exit_fill)
        self.assertTrue(any(sample['held'] == '1' and sample['available'] == '0'
                            and any(order[4] == 'confirmed' for order in sample['orders'])
                            for sample in state.samples), state.samples)
        self.assertTrue(any(sample['held'] == '1' and sample['available'] == '0'
                            and any(order[4] == 'pending_cancelled'
                                    for order in sample['orders'])
                            for sample in state.samples), state.samples)
        self.assertTrue(any(sample['position_qty'] == '1' and sample['held'] == '0'
                            and sample['available'] == '1'
                            and any(order[4] == 'cancelled'
                                    for order in sample['orders'])
                            for sample in state.samples), state.samples)
        self.assertTrue(all(Decimal(sample['position_qty']) >= 0
                            and Decimal(sample['available']) >= 0
                            for sample in state.samples), state.samples)
        self.assertEqual(state.samples[-1]['cash'], '499.90')
        self.assertEqual(state.samples[-1]['equity'], '499.90')
        self.assertEqual(state.samples[-1]['position_qty'], '0')
        protect = json.loads(next(a for a in actions if a[0] == 'protect')[3])
        entry = json.loads(next(a for a in actions if a[0] == 'entry')[3])
        exit_action = json.loads(next(a for a in actions if a[0] == 'exit')[3])
        self.assertEqual(protect['parameters']['quantity'],
                         entry['parameters']['quantity'])
        self.assertEqual(exit_action['parameters']['quantity'],
                         entry['parameters']['quantity'])
        self.assertEqual(result['synthetic_session'], True)
        self.assertFalse(result['production_authorized'])

    def test_restart_replays_accounting_and_audit_without_duplicate_fills(self):
        result, state, con, actions, trades, trace = self.run_default(
            replay_receipts=True)
        self.assertEqual(float(state.fake.cash), 499.90)
        path = con.execute('PRAGMA database_list').fetchone()[2]
        before_fills = con.execute(
            'SELECT count(*) FROM broker_executions').fetchone()[0]
        before_events = con.execute(
            'SELECT event_id,payload FROM audit_events ORDER BY rowid').fetchall()
        policy = json.loads(
            Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        policy.update(capture_only=False, live_enabled=False, strategy_instances=[
            {'instance_id': 'vwap-live', 'plugin': 'vwap-fixture'},
            {'instance_id': 'martin-observe', 'plugin': 'martin'}])
        book = ExecutionBook(path, policy, simulation=True, clock=state.clock)
        self.addCleanup(book.close)
        journal = BrokerJournal(path, allow_synthetic=True)
        self.addCleanup(journal.close)
        final = state.snapshot()
        journal.ingest(final)
        book.reconcile(final)
        accounting = InstanceAccounting(book.db, clock=lambda: NOW)
        self.assertEqual(accounting.instance_summary('vwap-live')['fills'], 2)
        self.assertEqual(accounting.instance_summary('vwap-live')['fees'],
                         '0.10')
        self.assertEqual(accounting.instance_summary('vwap-live')[
            'open_quantity'], '0')
        self.assertEqual(con.execute(
            'SELECT count(*) FROM broker_executions').fetchone()[0],
            before_fills)
        after_events = con.execute(
            'SELECT event_id,payload FROM audit_events ORDER BY rowid').fetchall()
        self.assertEqual(after_events[:len(before_events)], before_events,
                         'historical audit prefix changed')
        self.assertTrue(book.audit.verify()['integrity_valid'])

    def test_nonterminal_cancel_never_claims_flat(self):
        result, state, con, actions, trades, trace = self.run_default(
            cancel_effect=False, cycles=90)
        self.assertTrue(trades, (result.get('reason'), result.get('errors'),
                                actions))
        trade = next(iter(trades.values()))
        self.assertNotEqual(trade['phase'], 'FLAT_VERIFIED', trade)
        self.assertEqual(state.position_qty, 1)
        self.assertEqual(state.held_quantity(), 1)
        self.assertEqual(len(state.open_orders()), 1, state.orders)
        self.assertEqual(state.open_orders()[0]['trigger'], 'stop')
        self.assertEqual(con.execute(
            'SELECT count(*) FROM broker_executions').fetchone()[0], 1)
        self.assertEqual(trade['phase'], 'EXIT_REQUESTED')
        self.assertNotIn('exit', {kind for kind, state_name, _, _ in actions
                                  if state_name == 'OBSERVED'})
        self.assertIn('cancel', {kind for kind, _, _, _ in actions})
        cancel = next(a for a in actions if a[0] == 'cancel')
        self.assertEqual(cancel[1], 'REPORTED', (cancel, result.get('errors')))
        self.assertGreaterEqual(con.execute(
            'SELECT count(*) FROM account_budget_reservations').fetchone()[0], 1)
        self.assertEqual(con.execute(
            'SELECT count(*) FROM symbol_leases').fetchone()[0], 1)

    def test_late_host_result_never_becomes_a_fill(self):
        result, state, con, actions, trades, trace = self.run_default(
            late_first_place=True, cycles=40)
        self.assertTrue(trades, (result.get('reason'), result.get('errors'), actions))
        trade = next(iter(trades.values()))
        self.assertEqual(trade['phase'], 'ENTRY_AWAITING_BROKER', trade)
        self.assertEqual(trade['quantity'], '0')
        self.assertIsNone(trade['entry_price'])
        self.assertEqual(state.orders, [])
        self.assertEqual(state.position_qty, 0)
        self.assertEqual(float(state.fake.cash), 500.0)
        self.assertEqual(con.execute(
            'SELECT count(*) FROM broker_executions').fetchone()[0], 0)
        self.assertNotEqual(next(a for a in actions if a[0] == 'entry')[1],
                            'OBSERVED')
        self.assertEqual(con.execute(
            'SELECT count(*) FROM symbol_leases').fetchone()[0], 1)
        self.assertGreaterEqual(con.execute(
            'SELECT count(*) FROM account_budget_reservations').fetchone()[0], 1)

    def test_unconfirmed_stop_never_reports_protected(self):
        result, state, con, actions, trades, trace = self.run_default(
            protection_mode='unconfirmed', cycles=25)
        trade = next(iter(trades.values()))
        phases = [next(iter(row.values()))['phase'] for row in trace if row]
        self.assertNotIn('PROTECTED', phases, phases)
        self.assertNotIn('first_protection_observed_at', trade)
        self.assertEqual(con.execute(
            "SELECT count(*) FROM execution_feedback WHERE kind='protection_covered'"
        ).fetchone()[0], 0)
        self.assertIn('unprotected_since', trade)
        self.assertFalse(any(order[4] == 'confirmed'
                             for sample in state.samples
                             for order in sample['orders']), state.samples)
        self.assertTrue(any(sample['held'] == '1' and sample['available'] == '0'
                            and any(order[4] == 'submitted'
                                    for order in sample['orders'])
                            for sample in state.samples), state.samples)

    def test_insufficient_stop_coverage_never_reports_protected(self):
        result, state, con, actions, trades, trace = self.run_default(
            protection_mode='confirmed', protection_quantity='0.5', cycles=25)
        trade = next(iter(trades.values()))
        self.assertNotIn(trade['phase'], {'PROTECTED', 'FLAT_VERIFIED'}, trade)
        self.assertNotIn('first_protection_observed_at', trade)
        self.assertEqual(con.execute(
            "SELECT count(*) FROM execution_feedback WHERE kind='protection_covered'"
        ).fetchone()[0], 0)
        self.assertEqual(state.position_qty, 1)
        self.assertEqual(state.held_quantity(), Decimal('0.5'))
        self.assertTrue(any(order['trigger'] == 'stop' and
                            order['quantity'] == '0.5'
                            for order in state.orders), state.orders)
        self.assertEqual(con.execute(
            'SELECT count(*) FROM instance_fills').fetchone()[0], 1)

    def test_rejected_stop_never_reports_covered(self):
        result, state, con, actions, trades, trace = self.run_default(
            protection_mode='rejected', cycles=25)
        trade = next(iter(trades.values()))
        self.assertNotIn('first_protection_observed_at', trade)
        self.assertNotIn('PROTECTED', [next(iter(row.values()))['phase']
                                       for row in trace if row])
        self.assertEqual(con.execute(
            "SELECT count(*) FROM execution_feedback WHERE kind='protection_covered'"
        ).fetchone()[0], 0)
        self.assertTrue(any(order['trigger'] == 'stop' and
                            order['state'] == 'rejected'
                            for order in state.orders), state.orders)
        self.assertTrue(trade.get('exit_requested') or
                        trade['phase'] in {'EXIT_REQUESTED', 'FLAT_VERIFIED'},
                        trade)
