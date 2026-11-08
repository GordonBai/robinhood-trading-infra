"""Real default owner/read-port/journal/supervisor failure regression.

Only the external broker read, clock, fake broker, local Host peer and a
test-only storage wrapper for the simulation risk sidecar are supplied.  The
real LiveRiskGuard, ExecutionBook, BrokerJournal, default live ports,
LiveSupervisor, admission/accounting and HostExecutionBridge/
HostDeliveryDispatcher are production objects.
"""
import json
import shutil
import sqlite3
import threading
from decimal import Decimal
import tempfile
import time
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from martin_luk.live_session import run_live_session
from martin_agentic.live_risk import LiveRiskGuard
from martin_agentic.qualification import source_fingerprint
from martin_agentic.simulation import SimulationSources
from martin_agentic.transport import TransportError
from test_default_lifecycle import Host, NOW, TradingClock
from test_full_lifecycle import SymbolPlugin
from test_instance_recovery import DecisionRunner
from test_supervised_host_default_loop import (
    SimulationReviewTransport, SimulationWake)


class _Market:
    simulation_only = True

    def __init__(self, clock):
        self.clock = clock

    def __call__(self, *args, **kwargs):
        at = self.clock().isoformat()
        events = []
        for symbol in ('DEMO', 'SPY'):
            events.append({
                'id': 'q-' + symbol + '-' + at, 'kind': 'quote', 'at': at,
                'synthetic_input': True, 'source_sha256': 'fixture-' + symbol,
                'data': {'symbol': symbol, 'bid': '40.00', 'ask': '40.00',
                         'bid_at': at, 'ask_at': at, 'observed_at': at}})
        return events, {'quotes': 2}


_UNSET = object()


class _Broker:
    simulation_only = True

    def __init__(self, state, *, failures=(), value=_UNSET, mutate=None,
                 success_mutator=None, post_success=None):
        self.state = state
        self.failures = list(failures)
        self.value = value
        self.mutate = mutate
        self.success_mutator = success_mutator
        self.post_success = post_success
        self.successes = 0
        self.calls = 0
        self.records = []

    def __call__(self, approval_ids=()):
        self.calls += 1
        started = time.monotonic()
        if self.failures:
            failure = self.failures.pop(0)
            self.records.append({'started': started, 'ended': time.monotonic(),
                                 'ok': False, 'error': str(failure)})
            raise failure
        if self.value is not _UNSET:
            value = self.value
        else:
            if self.success_mutator is not None and self.successes == 0:
                self.successes = 1
                self.success_mutator(self.state)
            value = self.state.snapshot()
            if self.mutate is not None:
                self.mutate(value)
        if self.post_success is not None:
            self.post_success(self.state)
        self.records.append({'started': started, 'ended': time.monotonic(),
                             'ok': True, 'error': None})
        return value


class _BlockingBroker:
    simulation_only = True

    def __init__(self, state):
        self.state = state
        self.calls = 0
        self.started = threading.Event()
        self.release = threading.Event()
        self.thread = None
        self.returned = False

    def __call__(self, approval_ids=()):
        self.calls += 1
        self.thread = threading.current_thread()
        self.started.set()
        self.release.wait(timeout=10)
        self.returned = True
        return self.state.snapshot()


class LiveOwnerReadFailureTests(TestCase):
    def _risk_storage(self, path):
        # Production simulation deliberately selects :memory: and has no public
        # risk-object injection.  This test-only constructor wrapper maps that
        # storage choice to a private sidecar; observe/state/risk algorithms are
        # the real LiveRiskGuard with the same policy and simulation mode.
        real = LiveRiskGuard

        def construct(storage, policy, *, simulation=False):
            if storage == ':memory:':
                storage = path
            return real(storage, policy, simulation=simulation)
        return patch('martin_agentic.execution.LiveRiskGuard', construct)

    def _workspace(self, root, policy):
        """Create a production-admission two-instance checkpoint in-process.

        The seed uses the same composition helpers (build_instance_group /
        build_admission_stack) as the default owner.  Only the local broker,
        quote source, clock and Host peer are fixtures.
        """
        from support.dual_broker import PersistentBroker
        from entry_authority_fixture import TestExecutionBook
        from martin_agentic.broker import BrokerJournal
        from martin_agentic.host_execution import HostExecutionBridge
        from martin_luk.instance_composition import (
            build_admission_stack, build_instance_group)
        from martin_luk.live_session import (
            HostDeliveryDispatcher, LiveSupervisor, default_live_ports)
        from test_default_lifecycle import MarketPort
        from test_full_lifecycle import ReportingDispatcher
        source = Path(root) / 'source'
        source.mkdir()
        build = source_fingerprint()
        clock = TradingClock(NOW)
        state = PersistentBroker(clock)
        db = source / 'book.sqlite'
        with self._risk_storage(source / 'risk.sqlite'):
            book = TestExecutionBook(db, policy, simulation=True, clock=clock)
        journal = BrokerJournal(db, allow_synthetic=True)
        self.addCleanup(book.close)
        self.addCleanup(journal.close)
        # The seed runner is a deterministic local plugin fixture, but the
        # manifests, admission, leases, attribution and accounting are the
        # production composition built from the same policy.
        def runner_factory(cost, source_fingerprint, *, plugin_id='martin'):
            symbol = 'DEMO' if plugin_id == 'vwap-fixture' else 'SPY'
            return DecisionRunner(SymbolPlugin(symbol, proposal_at=1))
        group = build_instance_group(
            book, policy, source_fingerprint=build, clock=clock,
            runner_factory=runner_factory)
        admission = build_admission_stack(book, policy, clock=clock)
        def event(at, symbol):
            return {'id': 'seed-' + symbol + '-' + at, 'kind': 'quote',
                    'at': at, 'synthetic_input': True,
                    'source_sha256': 'fixture-' + symbol,
                    'data': {'symbol': symbol, 'bid': '40.00',
                             'ask': '40.00', 'bid_at': at, 'ask_at': at,
                             'observed_at': at}}
        seed_supervisor = LiveSupervisor(
            book, dispatcher=ReportingDispatcher(book),
            agent_mode='review', owner_source=build,
            process_strategy=False, instance_supervisor=group,
            admission=admission, accounting=admission.accounting)
        try:
            for _ in range(20):
                at = clock().isoformat()
                seed_supervisor.step(
                    state.snapshot(), [event(at, 'DEMO'), event(at, 'SPY')],
                    allow_entries=True)
                entries = [action for action in book.actions()
                           if action['kind'] == 'entry'
                           and action['state'] == 'REPORTED']
                if len(entries) == 2:
                    break
            else:
                self.fail('production seed did not admit two entries: '
                          + repr(book.actions()))
            for action in entries:
                payload = json.loads(action['payload'])
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
        finally:
            seed_supervisor.close()
        bridge = HostExecutionBridge(
            book, source / 'output' / 'host-execution',
            review_factory=SimulationReviewTransport,
            wake_adapter=SimulationWake())
        host = Host(source / 'output' / 'host-execution', state, clock=clock)
        host.start()
        self.addCleanup(host.join, 1)
        self.addCleanup(host.stop.set)
        self.addCleanup(bridge.close)
        ports = default_live_ports(
            book, policy, clock, dispatcher=bridge, host_surface=bridge,
            broker_surface=state.snapshot, market_surface=MarketPort(clock))
        dispatcher = HostDeliveryDispatcher(book, ports.host, bridge)
        supervisor = LiveSupervisor(
            book, dispatcher=dispatcher, agent_mode='supervised-host',
            owner_source=build, ports=ports, process_strategy=False,
            instance_supervisor=group, admission=admission,
            accounting=admission.accounting)
        try:
            for _ in range(30):
                at = clock().isoformat()
                snapshot = state.snapshot()
                journal.ingest(snapshot)
                supervisor.step(snapshot,
                                [event(at, 'DEMO'), event(at, 'SPY')],
                                allow_entries=False)
                trades = book.state().get('trades') or {}
                if len(trades) == 2 and all(
                        trade.get('phase') == 'PROTECTED'
                        for trade in trades.values()):
                    break
                time.sleep(.01)
            else:
                self.fail('production seed did not reach two protected trades: '
                          + repr(book.state()) + ' actions=' + repr(book.actions())
                          + ' orders=' + repr(state.orders))
            state.save(source / 'broker.json')
        finally:
            supervisor.close()
            group.close()
            host.stop.set()
            host.join(1)
        return source, clock

    def _policy(self):
        policy = json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        policy.update(capture_only=False, live_enabled=False, strategy_instances=[
            {'instance_id': 'alpha', 'plugin': 'vwap-fixture'},
            {'instance_id': 'beta', 'plugin': 'martin'}],
            account_limits={'max_open_cash': '500', 'max_open_notional': '500',
                            'max_open_planned_loss': '10.00',
                            'max_concurrent_trades': 4})
        return policy


    def _snapshot_counts(self, db):
        con = sqlite3.connect(db)
        try:
            state = json.loads(con.execute(
                'SELECT payload FROM live_state WHERE id=1').fetchone()[0])
            return {
                'actions': {row[0] for row in con.execute(
                    'SELECT id FROM live_actions')},
                'reservations': con.execute(
                    'SELECT instance_id, trade_id, cash, planned_loss, notional '
                    'FROM account_budget_reservations ORDER BY instance_id').fetchall(),
                'leases': con.execute(
                    'SELECT symbol, instance_id FROM symbol_leases '
                    'ORDER BY symbol').fetchall(),
                'observations': con.execute(
                    'SELECT count(*) FROM broker_observations').fetchone()[0],
                'trades': state['trades'],
            }
        finally:
            con.close()

    def _run_target(self, root, name, source, source_clock, policy, *,
                    failures=(), value=_UNSET, mutate=None, cycles=2,
                    broker_factory=None, state_mutator=None,
                    success_mutator=None, post_success=None):
        """Run the production owner against a copied admitted checkpoint."""
        from martin_agentic.simulation import validate_simulation_workspace
        from support.dual_broker import PersistentBroker
        target = Path(root) / name
        target.mkdir()
        db = target / 'book.sqlite'
        output = target / 'output'
        paths = validate_simulation_workspace(
            target, db_path=db, output_dir=output,
            status_file=target / 'status.json')
        shutil.copy2(source / 'book.sqlite', db)
        shutil.copy2(source / 'risk.sqlite', target / 'risk.sqlite')
        output.mkdir()
        clock = TradingClock(source_clock())
        state, _ = PersistentBroker.load(source / 'broker.json', clock)
        if state_mutator is not None:
            state_mutator(state)
        broker = (broker_factory(state) if broker_factory is not None
                  else _Broker(state, failures=failures, value=value,
                               mutate=mutate,
                               success_mutator=success_mutator,
                               post_success=post_success))
        sources = SimulationSources(broker=broker, market=_Market(clock),
                                    review=SimulationReviewTransport,
                                    wake=SimulationWake())
        host = Host(output / 'host-execution', state, clock=clock)
        host.start()
        try:
            with self._risk_storage(target / 'risk.sqlite'), \
                 patch('martin_agentic.simulation.validate_simulation_workspace',
                       return_value=paths), \
                 patch('martin_agentic.simulation.create_simulation_workspace',
                       side_effect=lambda value: value), \
                 patch('martin_agentic.live_session.wait_for_result_or_tick',
                       side_effect=lambda *a, **k: time.sleep(.01)), \
                 patch('martin_agentic.live_session.notify_local'), \
                 patch('martin_agentic.live_session.print'):
                result = run_live_session(
                    db, policy,
                    symbols=['DEMO', 'SPY'] + [str(i) for i in range(8)],
                    cycles=cycles, poll_seconds=5, agent_mode='supervised-host',
                    simulation=True, simulation_workspace=target,
                    simulation_sources=sources, clock=clock,
                    output_dir=output,
                    status_file=target / 'status.json',
                    budget_entry_limit=0, budget_total_limit=0)
        finally:
            host.stop.set()
            host.join(1)
        return result, db, state, broker, target, paths, clock

    def _run_existing(self, target, paths, state, policy, *, cycles=1):
        """Run a second default owner over the same admitted checkpoint."""
        from support.dual_broker import PersistentBroker
        clock = TradingClock(state.clock())
        broker = _Broker(state)
        sources = SimulationSources(broker=broker, market=_Market(clock),
                                    review=SimulationReviewTransport,
                                    wake=SimulationWake())
        host = Host(target / 'output' / 'host-execution', state, clock=clock)
        host.start()
        try:
            with self._risk_storage(target / 'risk.sqlite'), \
                 patch('martin_agentic.simulation.validate_simulation_workspace',
                       return_value=paths), \
                 patch('martin_agentic.simulation.create_simulation_workspace',
                       side_effect=lambda value: value), \
                 patch('martin_agentic.live_session.wait_for_result_or_tick',
                       side_effect=lambda *a, **k: time.sleep(.01)), \
                 patch('martin_agentic.live_session.notify_local'), \
                 patch('martin_agentic.live_session.print'):
                result = run_live_session(
                    target / 'book.sqlite', policy,
                    symbols=['DEMO', 'SPY'] + [str(i) for i in range(8)],
                    cycles=cycles, poll_seconds=5, agent_mode='supervised-host',
                    simulation=True, simulation_workspace=target,
                    simulation_sources=sources, clock=clock,
                    output_dir=target / 'output',
                    status_file=target / 'status.json',
                    budget_entry_limit=0, budget_total_limit=0)
        finally:
            host.stop.set()
            host.join(1)
        return result, broker

    def test_recovery_after_failure_manages_cancelled_protection_and_exit(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        policy = self._policy()
        source, source_clock = self._workspace(temp.name, policy)
        source_db = sqlite3.connect(source / 'book.sqlite')
        try:
            source_actions = {row[0] for row in source_db.execute(
                'SELECT id FROM live_actions')}
            source_refs = {row[0] for row in source_db.execute(
                'SELECT ref_id FROM external_broker_refs')}
        finally:
            source_db.close()

        def cancel_demo_stop(state):
            stop = next(order for order in state.orders
                        if order['symbol'] == 'DEMO'
                        and order['side'] == 'sell'
                        and order['trigger'] == 'stop')
            stop['state'] = 'cancelled'
            stop['cumulative_quantity'] = '0'

        def fill_demo_exit(state):
            exit_order = next((order for order in state.orders
                               if order['symbol'] == 'DEMO'
                               and order['side'] == 'sell'
                               and order['trigger'] == 'immediate'
                               and order['state'] in {'submitted', 'confirmed'}),
                              None)
            if exit_order is None:
                return
            exit_order['state'] = 'filled'
            exit_order['cumulative_quantity'] = '1'
            exit_order['executions'] = [{
                'id': 'exit-DEMO-exec', 'symbol': 'DEMO', 'side': 'sell',
                'quantity': '1', 'price': '40.00', 'fees': '0.05',
                'timestamp': state.clock().isoformat(),
                'order_id': exit_order['id']}]
            state.positions['DEMO'] = 0
            state.cash += Decimal('39.95')

        result, db, state, broker, target, paths, clock = self._run_target(
                temp.name, 'active', source, source_clock, policy,
                failures=[TransportError('APP_SERVER_DISCONNECTED')] * 2,
                cycles=20, success_mutator=cancel_demo_stop,
                post_success=fill_demo_exit)
        self.assertGreaterEqual(broker.calls, 3)
        self.assertTrue(result.get('broker', {}).get('read_healthy'), (result.get('reason'), result.get('failure'), broker.calls, result.get('errors'), result.get('strategy_receipts'), result.get('strategy_admissions')))
        self.assertEqual(state.positions.get('DEMO'), 0)
        self.assertEqual(state.positions.get('SPY'), 1)
        con = sqlite3.connect(db)
        try:
            trades = json.loads(con.execute(
                'SELECT payload FROM live_state WHERE id=1').fetchone()[0])['trades']
            actions = [json.loads(row[0]) for row in con.execute(
                'SELECT payload FROM live_actions ORDER BY created_at')]
            fills = con.execute(
                'SELECT instance_id, fill_id, quantity FROM instance_fills '
                'ORDER BY instance_id').fetchall()
        finally:
            con.close()
        demo = next(trade for trade in trades.values()
                    if trade['plan']['symbol'] == 'DEMO')
        spy = next(trade for trade in trades.values()
                   if trade['plan']['symbol'] == 'SPY')
        self.assertEqual(demo['quantity'], '0', demo)
        self.assertEqual(demo['phase'], 'FLAT_VERIFIED', demo)
        self.assertIn('first_protection_observed_at', demo)
        self.assertEqual(demo['first_protection_observed_quantity'], '1')
        self.assertLessEqual(Decimal(demo['protection_observation_delay_seconds']),
                             Decimal('30'))
        self.assertEqual(spy['quantity'], '1', spy)
        self.assertEqual(len(fills), 3, fills)
        self.assertEqual({row[1] for row in fills},
                         {'seed-DEMO-exec', 'seed-SPY-exec',
                          'exit-DEMO-exec'})
        self.assertEqual(sum(Decimal(row[2]) for row in fills),
                         Decimal('3'))
        self.assertEqual(state.cash, Decimal('459.85'))
        new_actions = [action for action in actions
                       if action['action_id'] not in source_actions]
        new_exit = [action for action in new_actions
                    if action['kind'] == 'exit'
                    and action['trade_id'] == demo['id']]
        self.assertEqual(len(new_exit), 1, new_actions)
        self.assertEqual(new_exit[0]['parameters']['quantity'], '1')
        self.assertNotIn(new_exit[0]['ref_id'], source_refs)
        # This scenario exercises active exit after protection termination, not
        # a successful protection replacement; it does not borrow the seed protect.
        self.assertFalse(any(action['kind'] == 'protect'
                             for action in new_actions))
        self.assertFalse(any(order['side'] == 'sell'
                             and order['state'] not in {
                                 'confirmed', 'filled', 'cancelled'}
                             for order in state.orders))

    def test_blocking_getter_cancels_without_consuming_late_generation(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        policy = self._policy()
        source, source_clock = self._workspace(temp.name, policy)
        before = self._snapshot_counts(source / 'book.sqlite')
        def factory(state):
            return _BlockingBroker(state)

        begin = time.monotonic()
        result, db, state, broker, target, paths, clock = self._run_target(
            temp.name, 'blocked', source, source_clock, policy, cycles=1,
            broker_factory=factory)
        elapsed = time.monotonic() - begin
        self.assertLessEqual(elapsed, 20)
        self.assertTrue(broker.started.is_set(), (result.get('reason'), result.get('failure'), broker.calls))
        self.assertGreaterEqual(broker.calls, 1)
        # The read task did not drain inside the bounded shutdown budget. Do
        # not claim the underlying call ended merely because the owner stopped.
        self.assertFalse(result['broker_read_cleanup']['drained'],
                         result['broker_read_cleanup'])
        self.assertTrue(result['broker_read_shutdown']['thread_alive'],
                        result['broker_read_shutdown'])
        self.assertTrue(result['new_entries_blocked'])
        after = self._snapshot_counts(db)
        self.assertEqual(after['actions'], before['actions'])
        self.assertEqual(after['observations'], before['observations'])
        self.assertEqual(after['reservations'], before['reservations'])
        self.assertEqual(after['leases'], before['leases'])
        self.assertEqual(state.positions.get('DEMO'), 1)
        self.assertEqual(state.positions.get('SPY'), 1)
        self.assertEqual(
            {trade['phase'] for trade in after['trades'].values()},
            {'PROTECTED'})
        # A second default owner publishes a newer complete observation and
        # healthy status over the same checkpoint while the first read is still
        # blocked.  Only then release the old callable: its late result must not
        # overwrite the newer fact or health.
        fresh, fresh_broker = self._run_existing(
            target, paths, state, policy, cycles=1)
        self.assertTrue(fresh['broker']['read_healthy'], fresh)
        fresh_counts = self._snapshot_counts(db)
        self.assertEqual(fresh_counts['observations'],
                         before['observations'] + 1)
        broker.release.set()
        broker.thread.join(5)
        self.assertFalse(broker.thread.is_alive())
        self.assertTrue(broker.returned)
        time.sleep(.05)
        after_late = self._snapshot_counts(db)
        self.assertEqual(after_late['actions'], fresh_counts['actions'])
        self.assertEqual(after_late['observations'], fresh_counts['observations'])
        self.assertEqual(after_late['trades'], fresh_counts['trades'])
        latest = sqlite3.connect(db)
        try:
            health = json.loads(latest.execute(
                'SELECT payload FROM broker_health WHERE id=1').fetchone()[0])
            current = json.loads(latest.execute(
                'SELECT payload FROM broker_current WHERE id=1').fetchone()[0])
        finally:
            latest.close()
        # The bounded session's normal shutdown records a healthy=false
        # supervisor-stop marker after the fresh read.  The late old result is
        # not allowed to replace that marker, the current payload, or health.
        self.assertEqual(health['reason'], 'LIVE_SUPERVISOR_STOPPED', health)
        self.assertEqual(current['observed_at'],
                         fresh['broker']['last_observed_at'])

    def test_nonretryable_read_failures_fail_closed_without_consumption(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        policy = self._policy()
        source, source_clock = self._workspace(temp.name, policy)
        before = self._snapshot_counts(source / 'book.sqlite')

        def wrong_account(snapshot):
            snapshot['account_key'] = 'wrong-account'
            snapshot['account_mask'] = '••••9999'

        def wrong_schema(snapshot):
            snapshot['portfolio'] = None

        cases = [
            ('none', dict(value=None), 'BROKER_READ_UNCLASSIFIED'),
            ('incomplete', dict(value={}), 'BROKER_READ_UNCLASSIFIED'),
            ('auth', dict(failures=[
                TransportError('INTERACTIVE_AUTH_OR_APPROVAL_REQUIRED')]),
             'INTERACTIVE_AUTH_OR_APPROVAL_REQUIRED'),
            ('account', dict(mutate=wrong_account),
             'BROKER_READ_UNCLASSIFIED'),
            ('schema', dict(mutate=wrong_schema),
             'BROKER_READ_UNCLASSIFIED'),
        ]
        for name, kwargs, code in cases:
            with self.subTest(name=name):
                result, db, state, broker, target, paths, clock = self._run_target(
                    temp.name, name, source, source_clock, policy, **kwargs)
                self.assertEqual(result['cycle'], 1, result.get('reason'))
                self.assertFalse(result['healthy'])
                self.assertTrue(result['attention_required'])
                self.assertTrue(result['new_entries_blocked'])
                self.assertEqual(result['failure']['code'], code)
                self.assertFalse(result['failure']['retryable'])
                after = self._snapshot_counts(db)
                self.assertEqual(after['actions'], before['actions'])
                self.assertEqual(after['reservations'], before['reservations'])
                self.assertEqual(after['leases'], before['leases'])
                self.assertEqual(after['observations'], before['observations'])
                self.assertEqual(
                    {trade['phase'] for trade in after['trades'].values()},
                    {'PROTECTED'})
                self.assertEqual(state.positions.get('DEMO'), 1)
                self.assertEqual(state.positions.get('SPY'), 1)
                self.assertEqual(broker.calls, 1)

    def test_transient_read_failure_real_owner_recovers_without_flat_or_release(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        policy = self._policy()
        source, source_clock = self._workspace(temp.name, policy)
        target = Path(temp.name) / 'session'
        target.mkdir()
        from martin_agentic.simulation import validate_simulation_workspace
        db = target / 'book.sqlite'
        output = target / 'output'
        # Capture the validated empty paths before copying the already-admitted
        # checkpoint; only the setup seam is replaced, not supervisor/read/
        # reconcile/authority objects.
        simulation_paths = validate_simulation_workspace(
            target, db_path=db, output_dir=output,
            status_file=target / 'status.json')
        shutil.copy2(source / 'book.sqlite', db)
        con = sqlite3.connect(source / 'book.sqlite')
        try:
            source_actions = {row[0] for row in con.execute(
                'SELECT id FROM live_actions')}
            source_reservations = con.execute(
                'SELECT instance_id, trade_id, cash, planned_loss, notional '
                'FROM account_budget_reservations ORDER BY instance_id').fetchall()
            source_observations = con.execute(
                'SELECT count(*) FROM broker_observations').fetchone()[0]
            source_leases = con.execute(
                'SELECT symbol, instance_id FROM symbol_leases ORDER BY symbol').fetchall()
            source_intents = con.execute(
                'SELECT intent_id, state FROM execution_intents ORDER BY intent_id').fetchall()
            source_refs = con.execute(
                'SELECT ref_id FROM external_broker_refs ORDER BY ref_id').fetchall()
            source_native = con.execute(
                'SELECT id, state FROM native_operation_records ORDER BY id').fetchall()
        finally:
            con.close()
        output.mkdir()
        from support.dual_broker import PersistentBroker
        clock = TradingClock(source_clock())
        state, _ = PersistentBroker.load(source / 'broker.json', clock)
        shutil.copy2(source / 'risk.sqlite', target / 'risk.sqlite')
        def seed_workspace(paths):
            return paths
        broker = _Broker(state, failures=[TransportError('APP_SERVER_DISCONNECTED')] * 2)
        sources = SimulationSources(broker=broker, market=_Market(clock),
                                    review=SimulationReviewTransport,
                                    wake=SimulationWake())
        host = Host(output / 'host-execution', state, clock=clock)
        host.start()
        self.addCleanup(host.join, 1)
        self.addCleanup(host.stop.set)
        with self._risk_storage(target / 'risk.sqlite'), \
             patch('martin_agentic.simulation.validate_simulation_workspace',
                   return_value=simulation_paths), \
             patch('martin_agentic.simulation.create_simulation_workspace',
                   side_effect=seed_workspace), \
             patch('martin_agentic.live_session.wait_for_result_or_tick',
                   side_effect=lambda *a, **k: time.sleep(.01)), \
             patch('martin_agentic.live_session.notify_local'), \
             patch('martin_agentic.live_session.print'):
            result = run_live_session(
                db, policy, symbols=['DEMO', 'SPY'] + [str(i) for i in range(8)],
                cycles=3, poll_seconds=5, agent_mode='supervised-host',
                simulation=True, simulation_workspace=target,
                simulation_sources=sources, clock=clock,
                output_dir=output, status_file=target / 'status.json',
                budget_entry_limit=0, budget_total_limit=0)
        self.assertEqual(result['cycle'], 3)
        self.assertTrue(result['attention_required'])
        self.assertTrue(result['broker']['read_healthy'])
        self.assertGreaterEqual(broker.calls, 3)
        self.assertEqual(host.state.positions.get('DEMO'), 1)
        self.assertEqual(host.state.positions.get('SPY'), 1)
        con = sqlite3.connect(db)
        try:
            trades = json.loads(con.execute(
                'SELECT payload FROM live_state WHERE id=1').fetchone()[0])['trades']
            self.assertEqual({trade['phase'] for trade in trades.values()},
                             {'PROTECTED'})
            self.assertEqual(
                con.execute('SELECT count(*) FROM account_budget_reservations')
                .fetchone()[0], 2)
            self.assertEqual(
                con.execute('SELECT count(*) FROM symbol_leases').fetchone()[0], 2)
            self.assertEqual(
                con.execute('SELECT instance_id, trade_id, cash, planned_loss, '
                            'notional FROM account_budget_reservations '
                            'ORDER BY instance_id').fetchall(),
                source_reservations)
            self.assertEqual(
                con.execute('SELECT symbol, instance_id FROM symbol_leases '
                            'ORDER BY symbol').fetchall(), source_leases)
            self.assertEqual(
                con.execute('SELECT intent_id, state FROM execution_intents '
                            'ORDER BY intent_id').fetchall(), source_intents)
            self.assertEqual(
                con.execute('SELECT ref_id FROM external_broker_refs '
                            'ORDER BY ref_id').fetchall(), source_refs)
            self.assertEqual(
                con.execute('SELECT id, state FROM native_operation_records '
                            'ORDER BY id').fetchall(), source_native)
            self.assertEqual(con.execute(
                'SELECT count(*) FROM broker_executions').fetchone()[0], 2)
            fills = con.execute(
                'SELECT instance_id, fill_id, side, quantity, fees '
                'FROM instance_fills ORDER BY instance_id').fetchall()
            self.assertEqual(len(fills), 2, fills)
            self.assertEqual({row[0] for row in fills}, {'alpha', 'beta'})
            self.assertEqual({row[1] for row in fills},
                             {'seed-DEMO-exec', 'seed-SPY-exec'})
            self.assertEqual({row[4] for row in fills}, {'0.05'})
            self.assertEqual(
                {row[0] for row in con.execute('SELECT id FROM live_actions')},
                source_actions, 'read failure created or replaced an action')
            events = []
            for (payload,) in con.execute(
                    'SELECT payload FROM audit_events ORDER BY sequence'):
                event = json.loads(payload)
                if event['event_type'] == 'BROKER_SOURCE_STATE':
                    events.append(event)
            reasons = [event.get('reason') for event in events]
            self.assertEqual(
                [reason for reason in reasons
                 if reason == 'APP_SERVER_DISCONNECTED'],
                ['APP_SERVER_DISCONNECTED', 'APP_SERVER_DISCONNECTED'],
                (reasons, result.get('failure'), result.get('reason'),
                 broker.calls, result.get('cycle')))
            observations = con.execute(
                'SELECT count(*) FROM broker_observations').fetchone()[0]
            # The two failed reads wrote health/audit failures but no snapshot;
            # exactly one later complete observation was consumed.
            self.assertEqual(observations, source_observations + 1)
            self.assertTrue(result['current_observation_evidence_fresh'])
            self.assertEqual(result['broker']['positions']['DEMO']['quantity'], '1')
            self.assertEqual(result['broker']['positions']['SPY']['quantity'], '1')
            risk_db = sqlite3.connect(target / 'risk.sqlite')
            try:
                risk = json.loads(risk_db.execute(
                    'SELECT payload FROM account_risk WHERE id=1').fetchone()[0])
            finally:
                risk_db.close()
            self.assertFalse(risk['blocked'], risk)
            self.assertEqual(risk['executions'].keys(),
                             {'seed-DEMO-exec', 'seed-SPY-exec'})
            self.assertFalse(any(item.get('admitted') is True
                                 for item in result['strategy_admissions']['outcomes']))
            self.assertEqual(result['strategy_bindings']['lifecycle'], 2)
            self.assertEqual(result['strategy_bindings']['bound'], 4)
            self.assertEqual(result['strategy_receipts']['duplicate'], 2)
            self.assertEqual(result['strategy_receipts']['imported'], 0)
            self.assertLessEqual(
                events[-2]['occurred_at'],
                result['broker']['last_observed_at'])
            self.assertEqual([record['ok'] for record in broker.records],
                             [False, False, True])
            self.assertLess(broker.records[-1]['started'],
                            broker.records[-1]['ended'])
            self.assertLessEqual(broker.records[-1]['ended'],
                                 time.monotonic())
        finally:
            con.close()
