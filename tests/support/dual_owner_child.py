#!/usr/bin/env python3
"""078r3c isolated dual-symbol owner child; test-only, no actual tools."""
import argparse
from datetime import datetime, timedelta
from decimal import Decimal
import hashlib
import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tests'))
sys.path.insert(0, str(ROOT / 'src'))

from dual_broker import PersistentBroker
from martin_agentic.broker import BrokerJournal
from martin_agentic.execution import ExecutionBook
from martin_agentic.host_execution import HostExecutionBridge
from martin_agentic.live_risk import LiveRiskGuard
from martin_luk.live_session import (
    HostDeliveryDispatcher, LiveSupervisor, default_live_ports)
from martin_agentic.shadow import single_instance
from test_default_lifecycle import Host, MarketPort, NOW, TradingClock
from test_e4_dual_host import DualHostTests
from test_full_lifecycle import (
    ACCOUNT_LIMITS, POLICY, budget, manifest)
from test_instance_host import CountingPlugin
from test_instance_recovery import DecisionRunner
from test_supervised_host_default_loop import (
    SimulationReviewTransport, SimulationWake)
from trading_infra.admission import AdmissionService
from trading_infra.attribution import ExecutionAttribution
from trading_infra.budget import AccountBudgetAuthority
from trading_infra.instance_accounting import InstanceAccounting
from trading_infra.instance_registry import InstanceRegistry
from trading_infra.instance_runtime import InstanceRuntime
from trading_infra.instance_supervisor import InstanceSupervisor
from trading_infra.symbol_lease import SymbolLease


def _prefix_digest(rows):
    body = json.dumps([row[1] for row in rows],
                      separators=(',', ':'), sort_keys=True)
    return hashlib.sha256(body.encode()).hexdigest()


def summary(book, money, leases, accounting, state, *, risk_before=None):
    refs = sorted(book.db.execute(
        'SELECT ref_id,instance_id,trade_id FROM execution_attribution'
    ).fetchall())
    intents = sorted(book.db.execute(
        'SELECT ref_id,instance_id,trade_id,symbol,state FROM execution_intents'
    ).fetchall())
    audit_rows = book.db.execute(
        'SELECT sequence,entry_hash FROM audit_events ORDER BY sequence'
    ).fetchall()
    protections = sorted(
        (order['symbol'], order['id'], order['state'],
         order.get('stop_price'), order.get('quantity'))
        for order in state.orders
        if order['side'] == 'sell' and order['trigger'] == 'stop')
    fill_owners = sorted(book.db.execute(
        'SELECT execution_key,instance_id,trade_id,fill_id '
        'FROM execution_fill_identity').fetchall())
    accounts = {iid: accounting.instance_summary(iid)
                for iid in ('alpha', 'beta')}
    risk_db = [list(row) for row in
               book.risk_guard.db.execute('PRAGMA database_list').fetchall()]
    try:
        from trading_infra.integrity_gate import read_integrity
        integrity = read_integrity(
            book.db, db_path=getattr(book, 'db_path', None),
            clock=book.clock)
        entry_gate = book.entry_gate()
    except Exception as exc:
        integrity = {'error': type(exc).__name__ + ':' + str(exc)}
        entry_gate = {'error': type(exc).__name__ + ':' + str(exc)}
    result = {
        'integrity': integrity,
        'entry_gate': entry_gate,
        'actions': [(action['id'], action['kind'], action['state'],
                     action['trade_id'])
                    for action in book.actions()],
        'risk_db': risk_db,
        'risk_rows': book.risk_guard.db.execute(
            'SELECT count(*) FROM account_risk').fetchone()[0],
        'refs': refs,
        'intents': intents,
        'fill_owners': fill_owners,
        'accounting': accounts,
        'protections': protections,
        'audit_prefix_sha256': _prefix_digest(audit_rows),
        'cash': str(state.cash),
        'equity': str(state.equity()),
        'marks': {k: str(v) for k, v in state.mark_prices.items()},
        'positions': {k: str(v) for k, v in state.positions.items()},
        'orders': len(state.orders),
        'budget': money.status(),
        'leases': {s: leases.owner(s) for s in ('DEMO', 'SPY')},
        'trades': {tid: {'phase': t.get('phase'),
                          'quantity': t.get('quantity'),
                          'first_fill_at': t.get('first_fill_at'),
                          'unprotected_since': t.get('unprotected_since')}
                   for tid, t in book.state().get('trades', {}).items()},
        'fills': {iid: len(accounting.fills(iid))
                  for iid in ('alpha', 'beta')},
        'risk': (lambda state: {
            'day': state.get('day'),
            'units': state.get('units'),
            'day_start_equity': state.get('day_start_equity'),
            'nav_high': state.get('nav_high'),
            'equity': state.get('equity'),
            'cash': state.get('cash'),
            'nav': state.get('nav'),
            'drawdown': state.get('drawdown'),
            'daily_halted': state.get('daily_halted'),
            'project_halted': state.get('project_halted'),
            'blocked': state.get('blocked'),
            'reasons': state.get('reasons'),
            'executions': sorted(state.get('executions', {})),
        })(book.risk_guard.state() or {}),
        'audit': book.db.execute(
            'SELECT count(*) FROM audit_events').fetchone()[0],
        'issues': list(book.state().get('issues', [])),
        'sla': {
            'unconfirmed_protections': sorted(state.unconfirmed_protections),
            'state_time': state.clock().isoformat(),
            'snapshot_calls': state.snapshot_calls,
            'unprotected': {
                trade_id: {
                    'first_fill_at': trade.get('first_fill_at'),
                    'unprotected_since': trade.get('unprotected_since'),
                    'deadline': (datetime.fromisoformat(
                        trade['unprotected_since']) + timedelta(seconds=30)
                    ).isoformat() if trade.get('unprotected_since') else None,
                }
                for trade_id, trade in book.state().get('trades', {}).items()
                if trade.get('unprotected_since')
            },
        },
    }
    if risk_before is not None:
        result['risk_before'] = risk_before
    return result




def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--workspace', required=True)
    parser.add_argument('--mode', choices=('seed', 'resume', 'sla-seed',
                                            'sla-resume', 'partial-seed',
                                            'partial-resume', 'epoch-seed',
                                            'epoch-resume', 'facts',
                                            'identity'), required=True)
    parser.add_argument('--fact', choices=('stale', 'incomplete', 'transport',
                                           'missing-history'), default='stale')
    parser.add_argument('--late-case', choices=('valid', 'hash', 'time',
                                                'flags', 'json', 'gate',
                                                'conflict', 'result-fail'),
                        default='valid')
    parser.add_argument('--token', default='')
    parser.add_argument('--result', required=True)
    args = parser.parse_args()
    workspace = Path(args.workspace).resolve()
    workspace.mkdir(parents=True, exist_ok=True)
    db = workspace / 'book.sqlite'
    broker_path = workspace / 'broker.json'
    risk_path = workspace / 'risk.sqlite'
    snapshot_path = workspace / 'last_snapshot.json'
    marker = workspace / 'barrier.json'
    result_path = Path(args.result)
    seed_mode = args.mode in {'seed', 'sla-seed', 'partial-seed',
                              'epoch-seed'}
    sla = args.mode in {'sla-seed', 'sla-resume'}
    partial = args.mode in {'partial-seed', 'partial-resume'}
    epoch = args.mode in {'epoch-seed', 'epoch-resume'}
    policy = json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
    policy.update(capture_only=False, live_enabled=False, strategy_instances=[
        {'instance_id': 'alpha', 'plugin': 'vwap-fixture'},
        {'instance_id': 'beta', 'plugin': 'martin'}])
    clock = TradingClock(NOW)
    lock = single_instance(db)
    lock.__enter__()
    try:
        risk_before = None
        if seed_mode and not db.exists():
            # The test-only storage choice is a real production risk guard on
            # an isolated workspace file, initialized from the flat original
            # broker facts before admission changes anything.
            state = PersistentBroker(clock)
            risk_guard = LiveRiskGuard(risk_path, policy, simulation=True)
            risk_guard.observe(state.snapshot())
            seed = DualHostTests('test_two_symbols_through_default_host_bridge')
            payloads = seed._seed(
                db, policy, clock, state,
                demo_quantity=2 if partial else 1)
            for payload in payloads.values():
                params = payload['parameters']
                state.add({
                    'id': 'seed-' + params['symbol'], 'symbol': params['symbol'],
                    'side': 'buy', 'quantity': params['quantity'],
                    'cumulative_quantity': '0', 'type': 'limit',
                    'trigger': 'immediate', 'price': params['limit_price'],
                    'stop_price': None, 'created_at': clock().isoformat(),
                    'state': 'submitted',
                    'market_hours': params['market_hours'],
                    'time_in_force': params['time_in_force'],
                    'ref_id': payload['ref_id'], 'executions': []})
            if sla:
                state.unconfirmed_protections = {'DEMO'}
            if partial:
                state.fill_queue = {'seed-DEMO': ['1']}
                state.ignore_cancel_orders = {'seed-DEMO'}
            if epoch:
                # Allow SPY to complete its ordinary protection before the
                # DEMO tranche is released; only DEMO's receipt is withheld.
                state.fill_queue = {'seed-DEMO': []}
            state.save(broker_path)
        else:
            if not risk_path.is_file() or risk_path.stat().st_size == 0:
                result_path.write_text(json.dumps(
                    {'error': 'RISK_STATE_MISSING'}) + '\n')
                return 2
            risk_guard = LiveRiskGuard(risk_path, policy, simulation=True)
            risk_before = risk_guard.state()
            if not risk_before:
                risk_guard.close()
                result_path.write_text(json.dumps(
                    {'error': 'RISK_STATE_MISSING'}) + '\n')
                return 2
            try:
                state, saved = PersistentBroker.load(broker_path, clock)
            except (OSError, ValueError) as exc:
                risk_guard.close()
                result_path.write_text(json.dumps(
                    {'error': str(exc)}) + '\n')
                return 2
            if sla:
                # Persisted current facts end before the outage.  Advance the
                # synthetic clock explicitly; a fresh snapshot is not treated
                # as if no outage occurred.
                clock.jump(31)
            if partial:
                # The first stage deliberately leaves the second fill for the
                # resumed child; the broker file holds only the remaining fact.
                state.fill_queue = {'seed-DEMO': ['1']}
        book = ExecutionBook(db, policy, simulation=True, clock=clock)
        # The ordinary simulation book's default guard is memory-only.  Close
        # it normally, then bind the same production guard backed by the
        # workspace risk file.  This is storage selection, not a gate bypass.
        book.risk_guard.close()
        book.risk_guard = risk_guard
        journal = BrokerJournal(db, allow_synthetic=True)
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
        runtimes = [InstanceRuntime(
            registry, registry.instance(iid),
            runner=DecisionRunner(CountingPlugin(proposal_at=None)),
            source_fingerprint='f' * 64, clock=clock)
            for iid in ('alpha', 'beta')]
        host_dir = workspace / 'output' / (
            'facts-host' if args.mode == 'facts'
            else 'identity-host' if args.mode == 'identity'
            else 'host-execution')
        bridge = HostExecutionBridge(
            book, host_dir, review_factory=SimulationReviewTransport,
            wake_adapter=SimulationWake())
        host = Host(host_dir, state, clock=clock,
                    withhold_protection_receipt=epoch,
                    withhold_protection_symbols={'DEMO'} if epoch else None)
        host.start()
        ports = default_live_ports(
            book, policy, clock, dispatcher=bridge, host_surface=bridge,
            broker_surface=state.snapshot, market_surface=MarketPort(clock))
        dispatcher = HostDeliveryDispatcher(book, ports.host, bridge)
        supervisor = LiveSupervisor(
            book, dispatcher=dispatcher, agent_mode='supervised-host',
            owner_source='f' * 64, ports=ports, process_strategy=False,
            instance_supervisor=InstanceSupervisor(runtimes),
            admission=admission, accounting=accounting)
        def write_snapshot(snapshot):
            tmp = snapshot_path.with_name(snapshot_path.name + '.tmp')
            tmp.write_text(json.dumps(snapshot, sort_keys=True) + '\n')
            with tmp.open('rb') as handle:
                os.fsync(handle.fileno())
            tmp.replace(snapshot_path)
            directory = os.open(workspace, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)

        def event(at, symbol, price):
            return {'id': 'q-' + symbol + '-' + at, 'kind': 'quote',
                    'at': at, 'synthetic_input': True,
                    'source_sha256': 'fixture-' + symbol,
                    'data': {'symbol': symbol, 'bid': price, 'ask': price,
                             'bid_at': at, 'ask_at': at, 'observed_at': at}}

        def step(price, spy_price=None):
            spy_price = price if spy_price is None else spy_price
            state.set_mark('DEMO', price)
            state.set_mark('SPY', spy_price)
            at = clock().isoformat()
            snapshot = state.snapshot()
            journal.ingest(snapshot)
            supervisor.step(snapshot,
                            [event(at, 'DEMO', price),
                             event(at, 'SPY', spy_price)],
                            allow_entries=False)
            state.save(broker_path)
            write_snapshot(snapshot)
            return book.state().get('trades', {})

        def by_symbol(trades):
            return {trade.get('plan', {}).get('symbol'): trade
                    for trade in trades.values()}

        def partial_ready(trades, second=False):
            items = by_symbol(trades)
            demo, spy = items.get('DEMO'), items.get('SPY')
            wanted = '2' if second else '1'
            confirmed = any(
                order['symbol'] == 'DEMO' and order['side'] == 'sell'
                and order['trigger'] == 'stop'
                and order['state'] == 'confirmed'
                and Decimal(str(order['quantity'])) == Decimal(wanted)
                for order in state.orders)
            return (demo is not None and spy is not None
                    and demo.get('phase') == 'PROTECTED'
                    and demo.get('quantity') == wanted
                    and spy.get('phase') == 'PROTECTED'
                    and confirmed)

        def epoch_ready(trades):
            items = by_symbol(trades)
            demo, spy = items.get('DEMO'), items.get('SPY')
            stop = any(
                order['symbol'] == 'DEMO' and order['side'] == 'sell'
                and order['trigger'] == 'stop'
                and order['state'] in {'submitted', 'confirmed'}
                for order in state.orders)
            return (demo is not None and spy is not None
                    and demo.get('quantity') == '1'
                    and demo.get('phase') == 'PROTECTED'
                    and spy.get('phase') == 'PROTECTED'
                    and stop and host.withheld_receipt is not None)

        def sla_ready(trades):
            items = by_symbol(trades)
            demo, spy = items.get('DEMO'), items.get('SPY')
            submitted = any(
                order['symbol'] == 'DEMO' and order['side'] == 'sell'
                and order['trigger'] == 'stop' and order['state'] == 'submitted'
                for order in state.orders)
            return (demo is not None and spy is not None
                    and demo.get('first_fill_at') is not None
                    and demo.get('unprotected_since') is not None
                    and spy.get('phase') == 'PROTECTED' and submitted)

        try:
            if args.mode == 'epoch-resume':
                from copy import deepcopy
                from martin_agentic.host_mailbox import read_request_result, submit_receipt
                epoch_data = json.loads((workspace / 'epoch.json').read_text())
                old_envelope = epoch_data['old_envelope']
                old_binding = old_envelope['binding']
                old_request = old_envelope['request_id']
                validator_now = clock().isoformat()
                validator = read_request_result(host_dir, old_request, clock=clock)
                for _ in range(30):
                    trades = step('40.00')
                    items = by_symbol(trades)
                    if (len(items) == 2
                            and all(trade.get('phase') == 'PROTECTED'
                                    for trade in items.values())):
                        break
                claimed = summary(book, money, leases, accounting, state,
                                  risk_before=risk_before)
                late = epoch_data['late_receipt']
                submit_receipt(host_dir, old_request, late['response'], clock=clock)
                receipt_path = (host_dir / 'receipts' /
                                (old_envelope['packet']['operation_id'] + '.json'))
                original_receipt = receipt_path.read_text()
                if args.late_case == 'result-fail':
                    from martin_agentic import host_execution as host_module
                    original_writer = host_module.write_private_json
                    def failing_writer(path, value):
                        if 'results' in str(path):
                            raise OSError('INJECTED_RESULT_PUBLISH_FAILURE')
                        return original_writer(path, value)
                    host_module.write_private_json = failing_writer
                    try:
                        step('40.00')
                    finally:
                        host_module.write_private_json = original_writer
                    native_state = book.db.execute(
                        'SELECT state FROM native_operation_records WHERE id=?',
                        (old_envelope['packet']['operation_id'],)).fetchone()
                    result_path.write_text(json.dumps({
                        'case': 'result-fail',
                        'late_results': list(bridge.late_receipt_results),
                        'native_state': native_state[0] if native_state else None,
                        'result_path': str(host_dir / 'results' /
                                           (old_request + '.json')),
                    }, indent=2) + '\n')
                    return 0
                bad_late_results = []
                if args.late_case != 'valid':
                    wrong_receipt = json.loads(original_receipt)
                    if args.late_case == 'hash':
                        wrong_receipt['response_sha256'] = '0' * 64
                    elif args.late_case == 'time':
                        wrong_receipt['received_at'] = '2099-01-01T00:00:00+00:00'
                    elif args.late_case == 'flags':
                        wrong_receipt['execution_authorized'] = True
                    elif args.late_case == 'json':
                        receipt_path.write_text('{')
                    elif args.late_case == 'gate':
                        bridge.receipt_gate = lambda *_: False
                    if args.late_case != 'json':
                        receipt_path.write_text(json.dumps(
                            wrong_receipt, sort_keys=True) + '\n')
                    for _ in range(5):
                        step('40.00')
                    bad_late_results = list(bridge.late_receipt_results)
                    receipt_path.write_text(original_receipt)
                    bridge.receipt_gate = None
                # The ordinary default bridge/supervisor is the only late-receipt
                # consumer; no private pending/lease injection is used.
                for _ in range(5):
                    step('40.00')
                late_results = list(bridge.late_receipt_results)
                conflict_late_results = []
                if args.late_case == 'conflict':
                    from martin_agentic.audit import digest
                    wrong = json.loads(receipt_path.read_text())
                    wrong['response']['structuredContent']['data'][
                        'order']['id'] = 'CONFLICTING-ORDER-ID'
                    wrong['response_sha256'] = digest(wrong['response'])
                    receipt_path.write_text(json.dumps(
                        wrong, sort_keys=True) + '\n')
                    for _ in range(5):
                        step('40.00')
                    conflict_late_results = list(
                        bridge.late_receipt_results)
                    receipt_path.write_text(original_receipt)
                port_receipt = None
                try:
                    from martin_agentic.adapters import BridgeHostDeliveryPort
                    for delivery_path in (host_dir / 'deliveries').glob('*.json'):
                        delivery = json.loads(delivery_path.read_text())
                        if delivery.get('request_id') == old_request:
                            port_receipt = BridgeHostDeliveryPort(bridge).receipt(
                                delivery['intent_id'])
                            break
                except Exception as exc:
                    port_receipt = {'error': type(exc).__name__ + ':' + str(exc)}
                native_state = book.db.execute(
                    'SELECT state FROM native_operation_records WHERE id=?',
                    (old_envelope['packet']['operation_id'],)).fetchone()
                # A complete fresh observation after the late import must not
                # recreate the old write or add a second DEMO protection.
                step('40.00')
                spy_exit = False
                for _ in range(30):
                    trades = step('40.00', spy_price='39.00')
                    items = by_symbol(trades)
                    spy = items.get('SPY') or {}
                    if any(action['kind'] == 'exit' and action['trade_id'] ==
                           spy.get('id') for action in book.actions()):
                        spy_exit = True
                        break
                after = summary(book, money, leases, accounting, state,
                                risk_before=risk_before)
                result_path.write_text(json.dumps({
                    'case': 'epoch-late-new',
                    'old_binding_epoch': old_binding.get('epoch'),
                    'new_binding_epoch': bridge.binding.get('epoch'),
                    'old_prepared_at': old_envelope.get('prepared_at'),
                    'old_expires_at': old_envelope.get('expires_at'),
                    'validator_now': validator_now,
                    'validator': validator,
                    'claim': claimed,
                    'late_case': args.late_case,
                    'bad_late_results': bad_late_results,
                    'native_state': native_state[0] if native_state else None,
                    'late_results': late_results,
                    'conflict_late_results': conflict_late_results,
                    'port_receipt': port_receipt,
                    'spy_exit': spy_exit,
                    'after': after,
                }, indent=2) + '\n')
                return 0

            if args.mode == 'identity':
                from copy import deepcopy
                at = clock().isoformat()
                events = [event(at, 'DEMO', '40.00'),
                          event(at, 'SPY', '40.00')]
                before = summary(book, money, leases, accounting, state,
                                 risk_before=risk_before)
                clean = state.snapshot()
                conflict = deepcopy(clean)
                demo_order = next(
                    order for order in clean['orders'].values()
                    if order.get('symbol') == 'DEMO' and order.get('state') == 'filled')
                spy_order = next(
                    order for order in clean['orders'].values()
                    if order.get('symbol') == 'SPY' and order.get('state') == 'filled')
                duplicate = deepcopy(demo_order['executions'][0])
                duplicate.update({
                    'order_id': 'conflict-order', 'symbol': 'SPY',
                    'side': 'buy', 'price': '41', 'fees': '0.06',
                    'executed_at': at,
                })
                conflicting_order = deepcopy(spy_order)
                conflicting_order.update({
                    'id': 'conflict-order', 'ref_id': spy_order.get('ref_id'),
                    'symbol': 'SPY', 'executions': [duplicate],
                    'filled': '1', 'remaining': '0', 'state': 'filled',
                })
                conflict['orders']['conflict-order'] = conflicting_order
                try:
                    result = supervisor.step(conflict, events,
                                             allow_entries=False)
                    outcome = {
                        'error': None,
                        'reconciled': result.get('reconciled'),
                        'new_entries_blocked': result.get('new_entries_blocked'),
                        'integrity_faults_unresolved': result.get(
                            'integrity_faults_unresolved'),
                        'integrity_reasons': result.get('integrity_reasons'),
                        'errors': result.get('errors'),
                        'management_dispatched': result.get(
                            'management_dispatched'),
                        'management_continues': result.get(
                            'management_continues'),
                        'dispatch': result.get('dispatch'),
                    }
                except Exception as exc:
                    outcome = {'error': type(exc).__name__ + ':' + str(exc)}
                after = summary(book, money, leases, accounting, state,
                                risk_before=risk_before)
                result_path.write_text(json.dumps({
                    'case': 'identity-conflict',
                    'before': before,
                    'outcome': outcome,
                    'after': after,
                }, indent=2) + '\n')
                return 0

            if args.mode == 'facts':
                from copy import deepcopy
                from martin_agentic.adapters import ProviderBrokerReadPort
                from martin_agentic.transport import TransportError
                at = clock().isoformat()
                events = [event(at, 'DEMO', '40.00'),
                          event(at, 'SPY', '40.00')]
                before = summary(book, money, leases, accounting, state,
                                 risk_before=risk_before)
                stale_snapshot = json.loads(snapshot_path.read_text())
                if args.fact == 'transport':
                    def unavailable():
                        raise TransportError('BROKER_READ_UNAVAILABLE')
                    outcomes = []
                    for read in (unavailable, lambda: None):
                        port = ProviderBrokerReadPort(read)
                        try:
                            port.snapshot()
                            outcomes.append({'error': None})
                        except (TransportError, ValueError) as exc:
                            outcomes.append({'error': str(exc)})
                    outcome = {'errors': outcomes}
                else:
                    if args.fact == 'stale':
                        clock.jump(31)
                        bad = stale_snapshot
                    elif args.fact == 'incomplete':
                        bad = deepcopy(stale_snapshot)
                        bad['collection_complete'] = False
                    else:
                        clock.jump(1)
                        empty = PersistentBroker(clock)
                        empty.cash = state.cash
                        empty.mark_prices = dict(state.mark_prices)
                        bad = empty.snapshot()
                    try:
                        result = supervisor.step(bad, events,
                                                 allow_entries=False)
                        outcome = {'error': None, 'result': result}
                    except Exception as exc:
                        outcome = {'error': type(exc).__name__ + ':' + str(exc)}
                blocked = summary(book, money, leases, accounting, state,
                                  risk_before=risk_before)
                # A deliberately bad observation must not consume or release
                # anything.  Then a complete fresh observation must still manage
                # the original facts.
                recovery = None
                try:
                    recovery = supervisor.step(state.snapshot(), events,
                                               allow_entries=False)
                except Exception as exc:
                    recovery = {'error': type(exc).__name__ + ':' + str(exc)}
                final = summary(book, money, leases, accounting, state,
                                risk_before=risk_before)
                result_path.write_text(json.dumps({
                    'case': args.fact,
                    'before': before,
                    'blocked': blocked,
                    'outcome': outcome,
                    'recovery': recovery,
                    'after': final,
                }, indent=2) + '\n')
                return 0

            highwater_done = not seed_mode or partial or epoch
            partial_trace = []
            for _ in range(120):
                trades = step('40.00')
                if partial and not seed_mode and not partial_trace:
                    demo = by_symbol(trades).get('DEMO')
                    if (demo is not None and demo.get('quantity') == '2'
                            and demo.get('unprotected_since')):
                        partial_trace.append({
                            'phase': demo.get('phase'),
                            'quantity': demo.get('quantity'),
                            'first_fill_at': demo.get('first_fill_at'),
                            'unprotected_since': demo.get('unprotected_since'),
                        })
                if epoch and seed_mode:
                    items = by_symbol(trades)
                    if (items.get('SPY', {}).get('phase') == 'PROTECTED'
                            and state.fill_queue.get('seed-DEMO') == []):
                        state.fill_queue['seed-DEMO'] = ['1']
                        state.save(broker_path)
                if partial:
                    ready = partial_ready(
                        trades, second=args.mode == 'partial-resume')
                    if not ready:
                        time.sleep(.02)
                        continue
                elif epoch and seed_mode:
                    ready = epoch_ready(trades)
                    if not ready:
                        time.sleep(.02)
                        continue
                elif sla and not seed_mode:
                    exits = [action for action in book.actions()
                             if action['kind'] == 'exit'
                             and action['state'] in {'REPORTED', 'OBSERVED'}]
                    if not exits:
                        time.sleep(.02)
                        continue
                else:
                    ready = sla_ready(trades) if sla else (
                        len(trades) == 2 and all(
                            trade.get('phase') == 'PROTECTED'
                            for trade in trades.values()))
                    if not ready:
                        time.sleep(.02)
                        continue
                if partial and not seed_mode:
                    # Replay the complete stage-2 facts.  The broker and the
                    # consumer must be idempotent for this snapshot.
                    for _ in range(3):
                        step('40.00')
                if not sla and seed_mode and not highwater_done:
                    # Same synthetic quote and mark source: first mark up,
                    # then return to the baseline mark.  The risk guard,
                    # not the test, records the high-water NAV.
                    for price in ('40.10', '40.10', '40.00', '40.00'):
                        trades = step(price)
                    highwater_done = True
                    if not all(trade.get('phase') == 'PROTECTED'
                               for trade in trades.values()):
                        continue
                if epoch and seed_mode:
                    from martin_agentic.host_mailbox import read_private_json
                    demo_action = next(
                        action for action in book.actions()
                        if action['kind'] == 'protect'
                        and action['trade_id'] in {
                            trade_id for trade_id, trade in
                            book.state().get('trades', {}).items()
                            if trade.get('plan', {}).get('symbol') == 'DEMO'})
                    old_envelope = None
                    for packet_path in sorted(
                            (host_dir / 'packets').glob('*.json')):
                        candidate = read_private_json(packet_path)
                        packet = candidate.get('packet') or {}
                        if (packet.get('action_id') == demo_action['id']
                                and packet.get('operation') == 'place'):
                            old_envelope = candidate
                            break
                    if old_envelope is None:
                        result_path.write_text(json.dumps(
                            {'phase': 'EPOCH_PACKET_MISSING'}) + '\n')
                        return 2
                    request_id = old_envelope['request_id']
                    operation_id = old_envelope['packet']['operation_id']
                    receipt_path = host_dir / 'receipts' / (operation_id + '.json')
                    # The Host accepted the DEMO place and withheld the local
                    # receipt; no normal receipts/results file may exist.
                    if receipt_path.exists():
                        result_path.write_text(json.dumps(
                            {'phase': 'EPOCH_RECEIPT_NOT_WITHHELD'}) + '\n')
                        return 2
                    late_receipt = {'response': host.withheld_receipt['response']}
                    epoch_path = workspace / 'epoch.json'
                    tmp = epoch_path.with_name(epoch_path.name + '.tmp')
                    tmp.write_text(json.dumps({
                        'old_envelope': old_envelope,
                        'late_receipt': late_receipt,
                    }, indent=2) + '\n')
                    with tmp.open('rb') as handle:
                        os.fsync(handle.fileno())
                    tmp.replace(epoch_path)
                    directory = os.open(workspace, os.O_RDONLY)
                    try:
                        os.fsync(directory)
                    finally:
                        os.close(directory)
                here = summary(book, money, leases, accounting, state,
                               risk_before=risk_before)
                if partial_trace:
                    here['partial_trace'] = partial_trace
                if seed_mode:
                    barrier = {'pid': os.getpid(), 'token': args.token,
                               'workspace': str(workspace),
                               'checkpoint': here}
                    state.save(broker_path, barrier=barrier, checkpoint=here)
                    tmp = marker.with_name(marker.name + '.tmp')
                    tmp.write_text(json.dumps(barrier, indent=2) + '\n')
                    with tmp.open('rb') as handle:
                        os.fsync(handle.fileno())
                    tmp.replace(marker)
                    directory = os.open(workspace, os.O_RDONLY)
                    try:
                        os.fsync(directory)
                    finally:
                        os.close(directory)
                    Path(result_path).write_text(json.dumps(here, indent=2) + '\n')
                    while True:
                        time.sleep(.05)
                result_path.write_text(json.dumps(here, indent=2) + '\n')
                break
            else:
                Path(result_path).write_text(json.dumps(
                    {'phase': 'TIMEOUT'}) + '\n')
                return 2
            return 0
        finally:
            host.stop.set()
            host.join(.5)
            supervisor.close()
            bridge.close()
            book.close()
            journal.close()
            for runtime in runtimes:
                try:
                    runtime.close()
                except Exception:
                    pass
    finally:
        lock.__exit__(None, None, None)


if __name__ == '__main__':
    raise SystemExit(main())
