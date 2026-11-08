"""Second plugin owner feature -> default exit advisor -> Book exit action."""
import json
import os
import tempfile
import time
from datetime import timedelta
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from martin_luk.live_session import run_live_session
from martin_agentic.simulation import SimulationSources
import test_execution
from test_supervised_host_default_loop import (SimulationReviewTransport,
                                               SimulationWake)
from trading_contracts.instance import InstanceBudget, PluginManifest
from trading_infra.attribution import ExecutionAttribution
from trading_infra.instance_registry import InstanceRegistry
from trading_strategies.registry import contract


def slow_vwap_rule(view):
    time.sleep(1)
    return {'action': 'EXIT', 'reason': 'SLOW_RULE_MUST_NOT_APPLY'}


class SeedBroker:
    simulation_only = True

    def __init__(self, snapshot):
        self.value = snapshot
        self.calls = 0

    def __call__(self, approval_ids=()):
        self.calls += 1
        return self.value


class SeedMarket:
    simulation_only = True

    def __init__(self, quote, at, *, age_seconds=0, empty=False):
        self.quote = quote
        self.at = at
        self.age_seconds = age_seconds
        self.empty = empty
        self.calls = 0

    def __call__(self, *args, **kwargs):
        self.calls += 1
        if self.empty:
            return [], {'quotes': 0}
        at = self.at.isoformat()
        observed = (self.at - timedelta(
            seconds=self.age_seconds)).isoformat()
        return ([{'id': 'e-' + at, 'kind': 'quote', 'at': at,
                  'synthetic_input': True, 'source_sha256': 'market-' + at,
                  'data': {**self.quote, 'bid_at': observed,
                           'ask_at': observed, 'observed_at': observed}}],
                {'quotes': 1})


class VwapDefaultExitTests(TestCase):
    def setUp(self):
        self.fixture = test_execution.ExecutionTests(
            'test_empty_queue_has_zero_agent_calls')
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.book = self.fixture.book
        self.now = self.fixture.now
        self.policy = json.loads(
            Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        self.policy.update(capture_only=False, live_enabled=False,
                           strategy_instances=[{'instance_id': 'vwap-e2e',
                                                'plugin': 'vwap-fixture'}])
        self.book.policy = self.policy
        self.book.propose(
            'signal-vwap',
            {'symbol': 'DEMO', 'quantity': 2, 'entry_ceiling': '40',
             'stop': '39', 'cost_buffer_per_share': '0.05'},
            self.fixture.quote, self.fixture.snap(),
            evidence={'source_sha256': 'seed', 'synthetic': True})
        claimed = self.book.claim()
        self.book.report(claimed['action_id'], {'status': 'submitted'})
        raw = self.fixture.bound(claimed, filled='2', state='filled')
        self.snapshot = self.fixture.snap([raw], qty='2')
        self.book.reconcile(self.snapshot)
        self.entry_order = raw
        self.trade_id = claimed['trade_id']
        self.ref_id = claimed['ref_id']
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)

    def register_owner(self, *, state, instance_id='vwap-e2e',
                       plugin_id='vwap-fixture'):
        family = contract(plugin_id)
        manifest = PluginManifest(
            plugin_id=plugin_id, version=family['version'],
            state_schema=family['state_schema'],
            config={'cost_buffer_per_share': '0.05'},
            data_requirements=tuple(family['data_requirements']),
            capabilities=tuple(family['capabilities']),
            max_inflight=256)
        budget = InstanceBudget(max_cash='100', max_planned_loss='2.50',
                                max_notional='100', max_concurrent=1)
        registry = InstanceRegistry(self.book.db, clock=lambda: self.now)
        registry.register(instance_id, manifest, budget,
                          mode='shadow', enabled=True)
        if state is not None:
            registry.put_state(instance_id, state, expected=None)
        attribution = ExecutionAttribution(self.book.db, clock=lambda: self.now)
        attribution.bind(self.ref_id, instance_id, self.trade_id)

    def run_session(self, *, state, owner_instance='vwap-e2e', cycles=8,
                    quote_age_seconds=0, empty_market=False,
                    quote_bid='39.99', plugin_id='vwap-fixture',
                    snapshot=None, generic=False):
        self.register_owner(state=state, instance_id=owner_instance,
                            plugin_id=plugin_id)
        quote = {'symbol': 'DEMO', 'bid': quote_bid, 'ask': '40.00'}
        sources = SimulationSources(
            broker=SeedBroker(snapshot or self.snapshot),
            market=SeedMarket(quote, self.now,
                              age_seconds=quote_age_seconds,
                              empty=empty_market),
            review=SimulationReviewTransport, wake=SimulationWake())
        workspace = Path(self.temp.name) / 'workspace'
        output = workspace / 'output'
        with patch('martin_agentic.live_session.ExecutionBook',
                   return_value=self.book), \
             patch.object(self.book, 'close'), \
             patch('martin_agentic.live_session.single_instance'), \
             patch('martin_agentic.live_session._broker_read',
                   side_effect=AssertionError('formal broker read')), \
             patch('martin_agentic.live_session.collect_live_events',
                   side_effect=AssertionError('formal market read')), \
             patch('martin_agentic.live_session.wait_for_result_or_tick',
                   side_effect=lambda *a, **k: time.sleep(0.1)), \
             patch('martin_agentic.live_session.notify_local'), \
             patch('martin_agentic.live_session.print'):
            if generic:
                from martin_agentic.live_session import run_live_session as generic_run
                from trading_example_app.composition import COMPOSITION
                return generic_run(
                    workspace / 'book.sqlite', self.policy,
                    symbols=['DEMO'] + [str(index) for index in range(9)],
                    cycles=cycles, poll_seconds=5, agent_mode='disabled',
                    simulation=True, simulation_workspace=workspace,
                    simulation_sources=sources, clock=lambda: self.now,
                    status_file=workspace / 'status.json', output_dir=output,
                    budget_entry_limit=0, budget_total_limit=0,
                    composition=COMPOSITION)
            return run_live_session(
                workspace / 'book.sqlite', self.policy,
                symbols=['DEMO'] + [str(index) for index in range(9)],
                cycles=cycles, poll_seconds=5, agent_mode='disabled',
                simulation=True, simulation_workspace=workspace,
                simulation_sources=sources, clock=lambda: self.now,
                status_file=workspace / 'status.json', output_dir=output,
                budget_entry_limit=0, budget_total_limit=0)

    def exit_actions(self):
        return [item for item in self.book.actions() if item['kind'] == 'exit']

    def test_owned_vwap_position_drives_default_book_exit_action(self):
        result = self.run_session(state={'session_vwap': '99'})
        exits = self.exit_actions()
        self.assertEqual(len(exits), 1, (result.get('errors'),
                                          result.get('strategy_exit_advice_errors'),
                                          [(a['kind'], a['state'])
                                           for a in self.book.actions()]))
        trade = self.book.state()['trades'][self.trade_id]
        self.assertTrue(trade['exit_requested'])
        advice = trade['last_exit_advice']
        self.assertEqual(advice['action'], 'EXIT', advice)
        self.assertEqual(advice['reason'], 'BELOW_SESSION_VWAP')
        self.assertNotEqual(advice['advisor_pid'], os.getpid())
        owner_state = InstanceRegistry(self.book.db,
                                       clock=lambda: self.now).state('vwap-e2e')
        self.assertIn('session_vwap', owner_state)
        self.assertGreater(float(owner_state['session_vwap']), 39.99)
        self.assertNotIn('ema9', owner_state)
        payload = json.loads(exits[0]['payload'])
        self.assertEqual(payload['trade_id'], self.trade_id)
        self.assertLessEqual(float(payload['parameters']['quantity']),
                             float(trade['quantity']))
        self.assertEqual(result['synthetic_session'], True)
        self.assertFalse(result['production_authorized'])

    def test_generic_session_runs_example_plugin_without_martin_composition(self):
        result = self.run_session(state={'session_vwap': '99'}, generic=True)
        exits = self.exit_actions()
        self.assertEqual(len(exits), 1, (result.get('errors'),
                                          result.get('strategy_exit_advice_errors')))
        advice = self.book.state()['trades'][self.trade_id]['last_exit_advice']
        self.assertEqual(advice['reason'], 'BELOW_SESSION_VWAP')
        self.assertNotIn('ema9', InstanceRegistry(
            self.book.db, clock=lambda: self.now).state('vwap-e2e'))

    def test_unknown_or_wrong_owner_does_not_borrow_vwap_feature(self):
        # The attribution points at an instance the supervisor did not build.
        # Its checkpoint has a vwap value, but that value must not be borrowed.
        result = self.run_session(state={'session_vwap': '99'},
                                  owner_instance='ghost-instance')
        self.assertEqual(self.exit_actions(), [])
        trade = self.book.state()['trades'][self.trade_id]
        self.assertFalse(trade['exit_requested'])
        advice = trade.get('last_exit_advice') or {}
        if advice:
            self.assertEqual(advice['action'], 'HOLD')
            self.assertEqual(advice['reason'], 'NO_SESSION_VWAP')
        self.assertFalse(any('EXIT' == item.get('action')
                             for item in (result.get('strategy_exit_advice_errors') or [])))

    def test_default_runner_advisor_timeout_keeps_reconcile_and_protection(self):
        self.policy['strategy_instances'] = [
            {'instance_id': 'slow-e2e', 'plugin': 'slow-exit-fixture'},
            {'instance_id': 'vwap-observer', 'plugin': 'vwap-fixture'}]
        self.book.policy = self.policy
        protection = next(action for action in self.book.actions()
                          if action['kind'] == 'protect')
        protection_order = self.fixture.bound(
            json.loads(protection['payload']), filled='0', state='confirmed',
            oid='slow-stop')
        protected_snapshot = self.fixture.snap(
            [self.entry_order, protection_order], qty='2')
        result = self.run_session(state=None, owner_instance='slow-e2e',
                                  plugin_id='slow-exit-fixture', cycles=22,
                                  snapshot=protected_snapshot)
        self.assertEqual(self.exit_actions(), [], result.get('errors'))
        # The new broker observation must be consumed by reconcile while the
        # advisor is still running past its deadline; a no-op reconcile leaves
        # this protection action at READY and fails.
        current = next(action for action in self.book.actions()
                       if action['id'] == protection['id'])
        self.assertNotEqual(current['state'], 'READY',
                            (current, result.get('errors')))
        observer = InstanceRegistry(self.book.db,
                                    clock=lambda: self.now).state('vwap-observer')
        self.assertIn('session_vwap', observer)
        self.assertGreater(len(observer), 0)
        errors = (result.get('strategy_exit_advice_history') or
                  result.get('strategy_exit_advice_errors') or [])
        self.assertTrue(any(item.get('reason') == 'EXIT_ADVISOR_TIMEOUT'
                            for item in errors), errors)
        trade = self.book.state()['trades'][self.trade_id]
        self.assertFalse(trade['exit_requested'])
        self.assertNotEqual((trade.get('last_exit_advice') or {}).get('reason'),
                            'SLOW_FIXTURE_MUST_TIMEOUT')

    def test_vwap_advisor_timeout_does_not_apply_or_fall_back(self):
        from trading_infra.exit_advisory import ExitAdvisoryService
        view = {'instance_id': 'vwap-e2e', 'plugin_id': 'vwap-fixture',
                'trade_id': self.trade_id, 'symbol': 'DEMO',
                'quantity': '2', 'entry_price': '40', 'stop': '39',
                'initial_stop': '39',
                'quote': {'symbol': 'DEMO', 'bid': '39.99', 'ask': '40.00',
                          'bid_at': self.now.isoformat(),
                          'ask_at': self.now.isoformat(),
                          'observed_at': self.now.isoformat()},
                'features': {'session_vwap': '99'},
                'as_of': self.now.isoformat(),
                'position_version': self.book.exit_position_version(
                    self.book.state()['trades'][self.trade_id])}
        service = ExitAdvisoryService(
            {'vwap-fixture': 'test_vwap_default_exit:slow_vwap_rule'},
            timeout_seconds=0.2, advice_ttl_seconds=20)
        self.assertEqual(service.submit([view]), 1)
        self.assertEqual(service.collect(), {})
        time.sleep(0.3)
        results = service.collect()
        self.assertEqual(set(results), {self.trade_id})
        self.assertEqual(results[self.trade_id]['error'],
                         'EXIT_ADVISOR_TIMEOUT')
        self.assertNotIn('action', results[self.trade_id])

    def test_hard_stop_creates_exit_even_without_owner_feature(self):
        # The unknown owner has no vwap feature, but the structural stop is
        # infrastructure policy and must still produce the exit action.
        result = self.run_session(state={'session_vwap': '99'},
                                  owner_instance='ghost-instance',
                                  quote_bid='38.90')
        exits = self.exit_actions()
        self.assertEqual(len(exits), 1, (result.get('errors'),
                                          result.get('strategy_exit_advice_errors'),
                                          [(a['kind'], a['state'])
                                           for a in self.book.actions()]))
        trade = self.book.state()['trades'][self.trade_id]
        self.assertTrue(trade['exit_requested'])
        self.assertNotEqual((trade.get('last_exit_advice') or {}).get('reason'),
                            'BELOW_SESSION_VWAP')

    def test_missing_quote_does_not_create_exit(self):
        result = self.run_session(state={'session_vwap': '99'},
                                  empty_market=True)
        self.assertEqual(self.exit_actions(), [])
        trade = self.book.state()['trades'][self.trade_id]
        self.assertFalse(trade['exit_requested'])
        self.assertNotEqual((trade.get('last_exit_advice') or {}).get('reason'),
                            'BELOW_SESSION_VWAP')

    def test_stale_quote_feature_does_not_create_exit(self):
        result = self.run_session(state={'session_vwap': '99'},
                                  quote_age_seconds=20)
        self.assertEqual(self.exit_actions(), [])
        trade = self.book.state()['trades'][self.trade_id]
        self.assertFalse(trade['exit_requested'])
        self.assertNotEqual((trade.get('last_exit_advice') or {}).get('reason'),
                            'BELOW_SESSION_VWAP')
