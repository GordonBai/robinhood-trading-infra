"""E3: manage an already-owned position through the default Host/owner loop.

The entry below is a test-only, explicitly synthetic, already-owned trade used
only to reach the management lifecycle; it is not an entry-source or permit
proof.  From that point the production ExecutionBook, BrokerJournal,
reconcile, HostExecutionBridge, native receipt importer and LiveSupervisor are
used.  The Host peer, broker snapshots and executions are local fixtures.
"""
import json
import sqlite3
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
from test_default_lifecycle import (
    BrokerState, Host, MarketPort, NOW, TradingClock)
from test_supervised_engineering import plan, quote
from test_supervised_host_default_loop import (
    SimulationReviewTransport, SimulationWake)

TERMINAL = {'filled', 'cancelled', 'canceled', 'rejected', 'expired'}


class PreownedManagementTests(TestCase):
    def _policy(self):
        policy = json.loads(
            Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        policy.update(capture_only=False, live_enabled=False)
        return policy

    def _seed(self, db, policy, clock, state, *, partial_fill=False):
        """Create one owned trade and a broker-shaped submitted entry order."""
        test_book = TestExecutionBook(db, policy, simulation=True, clock=clock)
        try:
            snapshot = state.snapshot()
            proposal = test_book.propose(
                'preowned-signal', plan(), quote(), snapshot,
                evidence={'synthetic': True, 'source_sha256': 'fixture'})
            action_id = proposal['action_id']
            test_book.claim()
            test_book.report(action_id, {'status': 'submitted'})
            action = next(item for item in test_book.actions()
                          if item['id'] == action_id)
            payload = json.loads(action['payload'])
            parameters = payload['parameters']
            order = {
                'id': 'seed-entry-order', 'symbol': parameters['symbol'],
                'side': 'buy', 'quantity': parameters['quantity'],
                'cumulative_quantity': '0', 'type': 'limit',
                'trigger': 'immediate',
                'price': parameters['limit_price'], 'stop_price': None,
                'created_at': clock().isoformat(), 'state': 'submitted',
                'market_hours': parameters['market_hours'],
                'time_in_force': parameters['time_in_force'],
                'ref_id': payload['ref_id'], 'executions': []}
            if partial_fill:
                # One of two ordered shares executed; the remainder is still
                # an open buy and must be cancelled separately by the book.
                order.update(
                    state='partially_filled', cumulative_quantity='1',
                    executions=[{'id': 'seed-entry-exec', 'symbol': 'DEMO',
                                 'side': 'buy', 'quantity': '1',
                                 'price': parameters['limit_price'],
                                 'fees': '0.05',
                                 'timestamp': clock().isoformat(),
                                 'order_id': order['id']}])
                state.position_qty = Decimal('1')
                state.fake.cash = '459.95'
            state.add(order)
            return payload
        finally:
            test_book.close()

    def _start(self, *, protection_mode='confirmed',
               protection_quantity=None, partial_fill=False,
               withhold_protection_receipt=False):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        workspace = root / 'workspace'
        workspace.mkdir()
        clock = TradingClock(NOW)
        state = BrokerState(clock, protection_mode=protection_mode,
                            protection_quantity=protection_quantity)
        policy = self._policy()
        db = workspace / 'book.sqlite'
        payload = self._seed(db, policy, clock, state,
                             partial_fill=partial_fill)
        book = ExecutionBook(db, policy, simulation=True, clock=clock)
        self.addCleanup(book.close)
        journal = BrokerJournal(db, allow_synthetic=True)
        self.addCleanup(journal.close)
        bridge = HostExecutionBridge(
            book, workspace / 'output' / 'host-execution',
            review_factory=SimulationReviewTransport,
            wake_adapter=SimulationWake())
        self.addCleanup(bridge.close)
        port_host = workspace / 'output' / 'host-execution'
        host = Host(port_host, state, clock=clock,
                    protection_quantity=protection_quantity,
                    withhold_protection_receipt=withhold_protection_receipt)
        host.start()
        self.addCleanup(host.join, 1)
        self.addCleanup(host.stop.set)
        ports = default_live_ports(
            book, policy, clock, dispatcher=bridge, host_surface=bridge,
            broker_surface=state.snapshot, market_surface=MarketPort(clock))
        dispatcher = HostDeliveryDispatcher(book, ports.host, bridge)
        supervisor = LiveSupervisor(
            book, dispatcher=dispatcher, agent_mode='supervised-host',
            owner_source='f' * 64, ports=ports, process_strategy=False)
        self.addCleanup(supervisor.close)
        return {
            'book': book, 'journal': journal, 'bridge': bridge,
            'supervisor': supervisor, 'state': state, 'clock': clock,
            'entry_payload': payload, 'db': db,
        }

    @staticmethod
    def _event(at, bid='40.00', ask='40.00'):
        return {'id': 'quote-' + at, 'kind': 'quote', 'at': at,
                'synthetic_input': True, 'source_sha256': 'fixture',
                'data': {'symbol': 'DEMO', 'bid': bid, 'ask': ask,
                         'bid_at': at, 'ask_at': at, 'observed_at': at}}

    def _step(self, context, *, bid='40.00', ask='40.00'):
        at = context['clock']().isoformat()
        snapshot = context['state'].snapshot()
        context['journal'].ingest(snapshot)
        result = context['supervisor'].step(
            snapshot, [self._event(at, bid=bid, ask=ask)],
            allow_entries=False)
        time.sleep(.02)
        return result

    def _trade(self, context):
        return next(iter(context['book'].state()['trades'].values()))

    def test_owned_fill_protects_only_after_confirmation_then_exits_flat(self):
        context = self._start()
        # The first observations are deliberately before the broker confirms
        # the submitted stop.  A submitted stop is not terminal and must not be
        # cancelled merely because confirmation is one snapshot late.
        context['supervisor'].step(
            context['state'].snapshot(),
            [self._event(context['clock']().isoformat())],
            allow_entries=False)
        context['journal'].ingest(context['state'].snapshot())
        for _ in range(10):
            self._step(context)
            action = self._trade(context)
            if action['phase'] == 'PROTECTED':
                break
        trade = self._trade(context)
        self.assertEqual(trade['phase'], 'PROTECTED', context['book'].state())
        self.assertFalse(any(item['kind'] == 'cancel'
                             for item in context['book'].actions()),
                         'a submitted stop was cancelled before confirmation')
        self.assertIn('first_protection_observed_at', trade)
        self.assertEqual(trade['first_protection_observed_quantity'], '2')
        self.assertLessEqual(
            Decimal(trade['protection_observation_delay_seconds']),
            Decimal('30'))

        # Breach the stop.  The accepted cancel is not terminal proof: the exit
        # may only appear after the broker reports the stop cancelled.
        for _ in range(20):
            self._step(context, bid='38.99', ask='39.00')
            trade = self._trade(context)
            if trade['phase'] == 'FLAT_VERIFIED':
                break
        trade = self._trade(context)
        self.assertEqual(trade['phase'], 'FLAT_VERIFIED', context['book'].state())
        self.assertEqual(context['state'].position_qty, 0)
        self.assertEqual(context['state'].open_orders(), [])
        kinds = [item['kind'] for item in context['book'].actions()]
        self.assertIn('protect', kinds)
        self.assertIn('cancel', kinds)
        self.assertIn('exit', kinds)
        transitions = context['state'].transitions
        confirmed = next(index for index, item in enumerate(transitions)
                         if item[0] == 'STOP_CONFIRMED')
        accepted = next(index for index, item in enumerate(transitions)
                        if item[0] == 'CANCEL_ACCEPTED')
        cancelled = next(index for index, item in enumerate(transitions)
                         if item[0] == 'CANCELLED')
        exit_fill = next(index for index, item in enumerate(transitions)
                         if item[0] == 'FILLED' and item[1] !=
                         next(order['id'] for order in context['state'].orders
                              if order['side'] == 'buy'))
        self.assertLess(confirmed, accepted)
        self.assertLess(accepted, cancelled)
        self.assertLess(cancelled, exit_fill)
        con = sqlite3.connect(context['db'])
        try:
            self.assertEqual(con.execute(
                'SELECT count(*) FROM broker_executions').fetchone()[0], 2)
            self.assertGreaterEqual(con.execute(
                "SELECT count(*) FROM native_operation_records "
                "WHERE operation='place' AND state='RECORDED'").fetchone()[0],
                2)
        finally:
            con.close()

    def test_additional_fill_after_partial_protection_is_not_covered(self):
        context = self._start(partial_fill=True)
        for _ in range(12):
            self._step(context)
            if self._trade(context)['phase'] == 'PROTECTED':
                break
        self.assertEqual(self._trade(context)['phase'], 'PROTECTED',
                         context['book'].state())
        entry_order = next(order for order in context['state'].orders
                           if order['side'] == 'buy')
        entry_order['state'] = 'filled'
        entry_order['cumulative_quantity'] = '2'
        entry_order['executions'].append({
            'id': 'seed-entry-exec-2', 'symbol': 'DEMO', 'side': 'buy',
            'quantity': '1', 'price': '40', 'fees': '0.05',
            'timestamp': context['clock']().isoformat(),
            'order_id': entry_order['id']})
        context['state'].position_qty = Decimal('2')
        context['state'].fake.cash = '419.90'
        self._step(context)
        trade = self._trade(context)
        self.assertNotEqual(trade['phase'], 'PROTECTED', context['book'].state())
        self.assertIn('unprotected_since', trade)
        context['clock'].jump(31)
        for _ in range(40):
            self._step(context)
            trade = self._trade(context)
            if trade['phase'] == 'FLAT_VERIFIED':
                break
        self.assertEqual(self._trade(context)['phase'],
                         'FLAT_VERIFIED', context['book'].state())
        self.assertTrue(any(issue.startswith('PROTECTION_WAIT_EXCEEDED')
                            for issue in context['book'].state()['issues']))
        self.assertEqual(context['state'].position_qty, 0)

    def _wait_submitted_stop(self, context):
        for _ in range(20):
            self._step(context)
            stops = [order for order in context['state'].orders
                     if order['side'] == 'sell'
                     and order['trigger'] == 'stop'
                     and order['state'] == 'submitted']
            if stops:
                return stops
        self.fail('submitted stop not observed')

    @staticmethod
    def _cancel_ids(context):
        return {action['id'] for action in context['book'].actions()
                if action['kind'] == 'cancel'}

    def test_submitted_stop_numeric_equivalence_does_not_cancel(self):
        context = self._start(protection_mode='unconfirmed')
        try:
            stop = self._wait_submitted_stop(context)[0]
            before = self._cancel_ids(context)
            stop['quantity'] = '2.000000'
            stop['remaining'] = '2.000000'
            self._step(context)
            self.assertEqual(self._cancel_ids(context), before,
                             'numeric-equivalent submitted stop was cancelled')
            self.assertEqual(self._trade(context)['phase'], 'NEEDS_PROTECTION')
        finally:
            self.doCleanups()

    def test_submitted_stop_total_and_terms_decision(self):
        context = self._start(protection_mode='unconfirmed')
        try:
            stop = self._wait_submitted_stop(context)[0]
            trade = self._trade(context)
            qty = Decimal(trade['quantity'])
            now = context['clock']()
            base = {
                'type': 'stop_market', 'state': 'submitted',
                'stop': stop.get('stop', stop.get('stop_price')),
                'quantity': stop.get('quantity', '2'),
                'remaining': stop.get('remaining', '2'),
                'market_hours': stop.get('market_hours', 'regular_hours'),
                'time_in_force': stop.get('time_in_force', 'gtc'),
                'created_at': stop.get('created_at')}
            split = [{**base, 'id': 'a', 'quantity': '1', 'remaining': '1'},
                     {**base, 'id': 'b', 'quantity': '1.000000',
                      'remaining': '1.000000'}]
            self.assertTrue(context['book']._submitted_stop_covers(
                split, trade, qty, now))
            self.assertFalse(context['book']._submitted_stop_covers(
                [split[0]], trade, qty, now), 'under-coverage waited')
            self.assertFalse(context['book']._submitted_stop_covers(
                split + [{**base, 'id': 'c', 'quantity': '1',
                          'remaining': '1'}], trade, qty, now),
                'over-coverage waited')
            wrong_stop = [{**split[0], 'stop': '38'}]
            self.assertFalse(context['book']._submitted_stop_covers(
                wrong_stop, trade, qty, now))
            wrong_type = [{**split[0], 'type': 'market'}]
            self.assertFalse(context['book']._submitted_stop_covers(
                wrong_type, trade, qty, now))
            out_of_session = [{**split[0], 'time_in_force': 'gfd',
                               'created_at': '2026-10-15T14:00:00+00:00'}]
            self.assertFalse(context['book']._submitted_stop_covers(
                out_of_session, trade, qty, now))
        finally:
            self.doCleanups()

    def test_cancel_accepted_then_stop_fill_does_not_send_old_exit(self):
        context = self._start()
        try:
            for _ in range(15):
                self._step(context)
                if self._trade(context)['phase'] == 'PROTECTED':
                    break
            self.assertEqual(self._trade(context)['phase'], 'PROTECTED',
                             context['book'].state())
            for _ in range(15):
                self._step(context, bid='38.99', ask='39.00')
                if any(order['state'] == 'pending_cancelled'
                       for order in context['state'].orders):
                    break
            stop = next(order for order in context['state'].orders
                        if order['side'] == 'sell'
                        and order['trigger'] == 'stop')
            self.assertEqual(stop['state'], 'pending_cancelled')
            before = self._cancel_ids(context)
            # The stop fills while the cancel is accepted/pending. The next
            # default reconcile must read fresh position/order facts and go
            # flat without sending the old full-size exit.
            stop['state'] = 'filled'
            stop['cumulative_quantity'] = stop['quantity']
            stop['executions'] = [{
                'id': 'race-stop-exec', 'symbol': 'DEMO', 'side': 'sell',
                'quantity': stop['quantity'], 'price': '39', 'fees': '0.05',
                'timestamp': context['clock']().isoformat(),
                'order_id': stop['id']}]
            context['state'].position_qty = Decimal('0')
            context['state'].fake.cash = '497.90'
            self._step(context, bid='38.99', ask='39.00')
            self.assertEqual(self._trade(context)['phase'], 'FLAT_VERIFIED',
                             context['book'].state())
            self.assertEqual(self._cancel_ids(context), before)
            self.assertEqual(context['state'].position_qty, 0)
            self.assertEqual(context['state'].fake.cash, '497.90',
                             'buy 2@40 + sell 2@39 with $0.05 fees')
            self.assertFalse(any(action['kind'] == 'exit'
                                 for action in context['book'].actions()),
                             'old full-size exit was sent after stop filled')
            self.assertFalse(any(order['side'] == 'sell'
                                 and order['state'] not in TERMINAL
                                 for order in context['state'].orders))
        finally:
            self.doCleanups()

    def test_lost_protection_receipt_recovers_from_fresh_broker_order(self):
        context = self._start(withhold_protection_receipt=True)
        try:
            for _ in range(20):
                self._step(context)
                if any(order['side'] == 'sell'
                       and order['trigger'] == 'stop'
                       for order in context['state'].orders):
                    break
            self.assertTrue(any(order['side'] == 'sell'
                                and order['trigger'] == 'stop'
                                for order in context['state'].orders))
            # The Host peer accepted and created the stop, but deliberately did
            # not write a local receipt. Fresh broker facts must recover it.
            for _ in range(10):
                self._step(context)
                if self._trade(context)['phase'] == 'PROTECTED':
                    break
            self.assertFalse(context['bridge'].pending)
            self.assertEqual(self._trade(context)['phase'], 'PROTECTED',
                             context['book'].state())
            protects = [action for action in context['book'].actions()
                        if action['kind'] == 'protect']
            self.assertEqual(len(protects), 1)
            self.assertEqual(protects[0]['state'], 'OBSERVED')
            self.assertTrue(context['bridge'].retired)
            self.assertEqual(context['state'].position_qty, 2)
            self.assertFalse(any(action['kind'] == 'exit'
                                 for action in context['book'].actions()))
        finally:
            self.doCleanups()

    def test_unconfirmed_protection_times_out_and_exits(self):
        context = self._start(protection_mode='unconfirmed')
        for _ in range(12):
            self._step(context)
            if any(order['side'] == 'sell' and order['trigger'] == 'stop'
                   and order['state'] == 'submitted'
                   for order in context['state'].orders):
                break
        self.assertTrue(any(order['side'] == 'sell'
                            and order['trigger'] == 'stop'
                            and order['state'] == 'submitted'
                            for order in context['state'].orders))
        context['clock'].jump(31)
        for _ in range(30):
            self._step(context)
            if self._trade(context)['phase'] == 'FLAT_VERIFIED':
                break
        trade = self._trade(context)
        self.assertEqual(trade['phase'], 'FLAT_VERIFIED', context['book'].state())
        self.assertNotIn('first_protection_observed_at', trade)
        self.assertTrue(any(issue.startswith('PROTECTION_WAIT_EXCEEDED')
                            for issue in context['book'].state()['issues']))
        self.assertEqual(context['state'].position_qty, 0)
