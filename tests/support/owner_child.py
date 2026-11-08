#!/usr/bin/env python3
"""Isolated child owner used by the 077r1 crash-recovery test.

Production ExecutionBook/BrokerJournal/LiveSupervisor/default ports/Host bridge
only.  Broker facts live in a separate fsynced JSON file; the test-only seed and
Host peer are local fixtures.  This process never calls a real broker/tool/DB.
"""
import argparse
import json
import os
import sys
import time
import uuid
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'tests'))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'src'))

from entry_authority_fixture import TestExecutionBook
from martin_agentic.broker import BrokerJournal
from martin_agentic.execution import ExecutionBook
from martin_agentic.host_execution import HostExecutionBridge
from martin_agentic.shadow import single_instance
from martin_luk.live_session import (
    HostDeliveryDispatcher, LiveSupervisor, default_live_ports)
from test_default_lifecycle import (
    BrokerState, Host, MarketPort, NOW, TradingClock)
from test_supervised_engineering import plan, quote
from test_supervised_host_default_loop import (
    SimulationReviewTransport, SimulationWake)

TERMINAL = {'filled', 'cancelled', 'canceled', 'rejected', 'expired'}


def save_broker(path, state):
    value = {
        'orders': state.orders,
        'position_qty': str(state.position_qty),
        'cash': state.fake.cash,
        'protection_mode': state.protection_mode,
        'protection_quantity': state.protection_quantity,
        'transitions': state.transitions,
        'saved_at': state.clock().isoformat(),
    }
    tmp = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    with tmp.open('w') as handle:
        json.dump(value, handle, sort_keys=True)
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(tmp, 0o600)
    tmp.replace(path)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def load_broker(path, clock):
    if not path.exists():
        return BrokerState(clock)
    value = json.loads(path.read_text())
    saved = value.get('saved_at')
    if saved:
        clock.start = __import__('datetime').datetime.fromisoformat(saved)
        clock.monotonic = time.monotonic()
        clock.offset = 0.0
    state = BrokerState(clock,
                        protection_mode=value.get('protection_mode', 'confirmed'),
                        protection_quantity=value.get('protection_quantity'))
    state.orders = value.get('orders', [])
    state.position_qty = Decimal(value.get('position_qty', '0'))
    state.fake.cash = value.get('cash', '500')
    state.transitions = value.get('transitions', [])
    return state


def seed(db, policy, clock, state):
    book = TestExecutionBook(db, policy, simulation=True, clock=clock)
    try:
        proposal = book.propose(
            'preowned-signal', plan(), quote(), state.snapshot(),
            evidence={'synthetic': True, 'source_sha256': 'fixture'})
        action_id = proposal['action_id']
        book.claim()
        book.report(action_id, {'status': 'submitted'})
        action = next(item for item in book.actions() if item['id'] == action_id)
        payload = json.loads(action['payload'])
        parameters = payload['parameters']
        state.add({
            'id': 'seed-entry-order', 'symbol': parameters['symbol'],
            'side': 'buy', 'quantity': parameters['quantity'],
            'cumulative_quantity': '0', 'type': 'limit',
            'trigger': 'immediate', 'price': parameters['limit_price'],
            'stop_price': None, 'created_at': clock().isoformat(),
            'state': 'submitted', 'market_hours': parameters['market_hours'],
            'time_in_force': parameters['time_in_force'],
            'ref_id': payload['ref_id'], 'executions': []})
    finally:
        book.close()


class BarrierHost(Host):
    def __init__(self, *args, marker=None, persist=None, mode=None, token=None, workspace=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.marker = Path(marker)
        self.persist = persist
        self.mode = mode
        self.token = token
        self.workspace = workspace

    def step(self):
        super().step()
        if self.persist is not None:
            self.persist()
        if self.marker.exists():
            return
        stop = [order for order in self.state.orders
                if order['side'] == 'sell' and order['trigger'] == 'stop']
        if self.mode == 'protect-lost' and stop:
            self.marker.write_text(json.dumps({'barrier': 'protect-accepted',
                                               'order': stop[-1]['id'],
                                               'pid': os.getpid(),
                                               'token': self.token,
                                               'workspace': str(self.workspace)}))
            self.stop.set()
        elif self.mode == 'cancel-fill' and any(
                order['state'] == 'pending_cancelled'
                for order in self.state.orders):
            self.marker.write_text(json.dumps({'barrier': 'cancel-accepted',
                                               'pid': os.getpid(),
                                               'token': self.token,
                                               'workspace': str(self.workspace)}))
            self.stop.set()


def event(at, bid='40.00'):
    return {'id': 'quote-' + at, 'kind': 'quote', 'at': at,
            'synthetic_input': True, 'source_sha256': 'fixture',
            'data': {'symbol': 'DEMO', 'bid': bid, 'ask': '40.00',
                     'bid_at': at, 'ask_at': at, 'observed_at': at}}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--workspace', required=True)
    parser.add_argument('--mode', required=True,
                        choices=('protect-lost', 'cancel-fill',
                                 'resume-protect', 'resume-cancel'))
    parser.add_argument('--result', required=True)
    parser.add_argument('--token', default='')
    args = parser.parse_args()
    workspace = Path(args.workspace).resolve()
    workspace.mkdir(parents=True, exist_ok=True)
    db = workspace / 'book.sqlite'
    broker_path = workspace / 'broker.json'
    marker = workspace / 'barrier.json'
    policy = json.loads(
        Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
    policy.update(capture_only=False, live_enabled=False)
    clock = TradingClock(NOW)
    owner_lock = single_instance(db)
    owner_lock.__enter__()
    try:
        state = load_broker(broker_path, clock)
        if not db.exists():
            seed(db, policy, clock, state)
            save_broker(broker_path, state)
        return _run(args, workspace, db, broker_path, marker, policy, clock, state)
    finally:
        owner_lock.__exit__(None, None, None)

def _run(args, workspace, db, broker_path, marker, policy, clock, state):
    book = ExecutionBook(db, policy, simulation=True, clock=clock)
    journal = BrokerJournal(db, allow_synthetic=True)
    host_dir = workspace / 'output' / 'host-execution'
    bridge = HostExecutionBridge(
        book, host_dir, review_factory=SimulationReviewTransport,
        wake_adapter=SimulationWake())
    host = BarrierHost(
        host_dir, state, clock=clock,
        withhold_protection_receipt=args.mode == 'protect-lost',
        marker=marker,
        persist=lambda: save_broker(broker_path, state),
        mode=args.mode, token=args.token, workspace=workspace)
    host.start()
    ports = default_live_ports(
        book, policy, clock, dispatcher=bridge, host_surface=bridge,
        broker_surface=state.snapshot, market_surface=MarketPort(clock))
    dispatcher = HostDeliveryDispatcher(book, ports.host, bridge)
    supervisor = LiveSupervisor(
        book, dispatcher=dispatcher, agent_mode='supervised-host',
        owner_source='f' * 64, ports=ports, process_strategy=False)
    result = {'mode': args.mode, 'pid': os.getpid(), 'actual_calls': 0}
    try:
        for index in range(160):
            if marker.exists() and args.mode in {'protect-lost', 'cancel-fill'}:
                # Keep the crashed owner alive at the durable barrier until
                # the parent sends SIGKILL.  No further DB action is taken.
                time.sleep(.05)
                continue
            snapshot = state.snapshot()
            journal.ingest(snapshot)
            trade = next(iter(book.state()['trades'].values()))
            bid = ('38.99' if args.mode in {'cancel-fill', 'resume-cancel'}
                   and trade['phase'] == 'PROTECTED' else '40.00')
            supervisor.step(snapshot, [event(clock().isoformat(), bid=bid)],
                            allow_entries=False)
            save_broker(broker_path, state)
            if args.mode.startswith('resume-protect') \
                    and trade['phase'] == 'PROTECTED':
                result['phase'] = trade['phase']
                break
            if args.mode.startswith('resume-cancel') \
                    and trade['phase'] == 'FLAT_VERIFIED':
                result['phase'] = trade['phase']
                break
            time.sleep(.02)
        else:
            result['phase'] = 'TIMEOUT'
        if 'phase' not in result:
            result['phase'] = 'BARRIER_ALIVE'
        Path(args.result).write_text(json.dumps(result, indent=2) + '\n')
    finally:
        host.stop.set()
        host.join(.5)
        supervisor.close()
        bridge.close()
        book.close()
        journal.close()
    return 0 if result.get('phase') in {'PROTECTED', 'FLAT_VERIFIED',
                                         'BARRIER_ALIVE'} else 2


if __name__ == '__main__':
    raise SystemExit(main())
