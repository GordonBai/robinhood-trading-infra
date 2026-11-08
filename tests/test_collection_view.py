"""Per-consumer collection frontiers and owner-routed exit features.

Collection used to ask one strategy state whether a bar was already known. With
several instances that answer starves whichever instance is behind, so the
frontier is now per consumer and a bar is omitted only when everyone holds the
identical content.

Synthetic data only; no broker, no order.
"""
import sqlite3
import tempfile
from datetime import datetime, timezone
from decimal import Decimal
from unittest import TestCase

from martin_agentic.collection_view import (CollectionView, build_collection_state,
                                            build_collection_view,
                                            filter_bars_for_view)
from martin_agentic.control_tasks import FrozenSignals
from martin_luk.live_session import LiveSupervisor
from martin_luk.live_signals import filter_unchanged_bars
from trading_contracts.instance import InstanceBudget, PluginManifest
from trading_contracts.market import bar_observation_identity
from trading_infra.attribution import ExecutionAttribution
from trading_infra.instance_registry import InstanceRegistry
from trading_infra.instance_runtime import InstanceRuntime
from trading_infra.instance_supervisor import InstanceSupervisor

NOW = datetime(2026, 10, 30, 14, 0, tzinfo=timezone.utc)
SOURCE = 'a' * 64


def bar(symbol='SPY', close='40.00', begins='2026-10-30T13:30:00+00:00'):
    return {'symbol': symbol, 'begins_at': begins, 'available_at': begins,
            'observed_at': begins, 'interval': '5minute', 'bounds': 'regular',
            'adjustment': 'split', 'open': '40', 'high': '40.1', 'low': '39.9',
            'close': close, 'volume': 10000}


def manifest(plugin_id, cost='0.05', requirements=('quotes',)):
    return PluginManifest(plugin_id=plugin_id, version='1.0', state_schema='s-1',
                          config={'step': '1', 'cost_buffer_per_share': cost},
                          data_requirements=requirements,
                          capabilities=('advisory-entry',), max_inflight=8)


def budget():
    return InstanceBudget(max_cash=Decimal('100'), max_planned_loss=Decimal('2.50'),
                          max_notional=Decimal('100'), max_concurrent=1)


class Owner:
    """Minimal owner-shaped object for the supervisor's legacy state path."""

    def __init__(self, db):
        self.db = db
        self.clock = lambda: NOW
        self.simulation = True
        self.policy = {}

    def _begin(self):
        self.db.execute('BEGIN IMMEDIATE')


class FakeRunner:
    def __init__(self):
        self.started = []

    @property
    def next_deadline_monotonic(self):
        return None

    def start(self, request, *, payload=None):
        self.started.append(request)

    def poll(self):
        return None

    def close(self):
        pass


class ViewTests(TestCase):
    def key(self, data=None):
        return bar_observation_identity(data or bar())

    def test_an_instance_that_is_behind_still_receives_the_bar(self):
        key, fingerprint = self.key()
        ahead = {key: fingerprint}
        behind = {}
        view = CollectionView(frontiers=[ahead, behind], instance_ids=['a', 'b'])
        self.assertTrue(view.should_emit(bar()))

    def test_a_bar_every_consumer_holds_is_omitted(self):
        key, fingerprint = self.key()
        view = CollectionView(frontiers=[{key: fingerprint}, {key: fingerprint}],
                              instance_ids=['a', 'b'])
        self.assertFalse(view.should_emit(bar()))

    def test_a_revision_reaches_every_consumer(self):
        key, fingerprint = self.key()
        revised = bar(close='41.00')
        revised_key, revised_fingerprint = self.key(revised)
        self.assertEqual(key, revised_key, 'same bar identity')
        self.assertNotEqual(fingerprint, revised_fingerprint)
        view = CollectionView(frontiers=[{key: fingerprint}, {key: fingerprint}],
                              instance_ids=['a', 'b'])
        self.assertTrue(view.should_emit(revised))

    def test_the_legacy_shape_keeps_its_old_behaviour(self):
        key, fingerprint = self.key()
        view = CollectionView.legacy_view({'bar_observations': {key: fingerprint}})
        self.assertFalse(view.should_emit(bar()))
        self.assertEqual(view.state(), {'bar_observations': {key: fingerprint}})
        # The plain mapping path is unchanged for callers that still use it.
        changed, omitted = filter_unchanged_bars([bar()],
                                                 {'bar_observations': {key: fingerprint}})
        self.assertEqual((changed, omitted), ([], 1))

    def test_filter_bars_for_view_matches_the_view(self):
        key, fingerprint = self.key()
        view = CollectionView(frontiers=[{key: fingerprint}], instance_ids=['a'])
        changed, omitted = filter_bars_for_view([bar(), bar(close='42.00')], view)
        self.assertEqual((len(changed), omitted), (1, 1))

    def test_a_frozen_view_does_not_follow_later_owner_changes(self):
        key, fingerprint = self.key()
        owner_state = {key: fingerprint}
        view = CollectionView(frontiers=[dict(owner_state)], instance_ids=['a'])
        owner_state[key] = 'changed-after-build'
        self.assertFalse(view.should_emit(bar()))

    def test_a_frozen_signals_object_carries_the_view(self):
        key, fingerprint = self.key()
        view = CollectionView(frontiers=[{key: fingerprint}], instance_ids=['a'])
        frozen = FrozenSignals({'revision': 2}, view=view)
        self.assertEqual(frozen.state(), {'revision': 2})
        self.assertIs(frozen.collection_view(), view)

    def test_a_frozen_signals_without_a_view_falls_back_to_the_legacy_map(self):
        key, fingerprint = self.key()
        frozen = FrozenSignals({'bar_observations': {key: fingerprint}})
        self.assertFalse(frozen.collection_view().should_emit(bar()))


class OwnerRoutingTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = sqlite3.connect(self.temp.name + '/routing.sqlite',
                                  isolation_level=None)
        self.addCleanup(self.db.close)
        self.registry = InstanceRegistry(self.db, clock=lambda: NOW)
        # Use plugins that explicitly declare their exit feature contract.
        # ``alpha``/``beta`` remain the instance identities; plugin ids must
        # not be invented fallback names with an implicit EMA map.
        self.registry.register('alpha', manifest(
            'martin', '0.05', ('quotes', '5minute-bars', 'universe-screen')),
            budget(), mode='shadow', enabled=True)
        self.registry.register('beta', manifest('vwap-fixture', '0.10'), budget(),
                               mode='shadow', enabled=True)
        self.runtimes = []
        for instance_id in ('alpha', 'beta'):
            runtime = InstanceRuntime(self.registry,
                                      self.registry.instance(instance_id),
                                      runner=FakeRunner(), source_fingerprint=SOURCE,
                                      clock=lambda: NOW)
            self.addCleanup(runtime.close)
            self.runtimes.append(runtime)
        self.feedback = ExecutionAttribution(self.db, clock=lambda: NOW)
        self.supervisor = LiveSupervisor(Owner(self.db), process_strategy=False,
                                         agent_mode='review', owner_source=SOURCE,
                                         instance_supervisor=InstanceSupervisor(self.runtimes))

    def test_the_view_is_built_from_each_instance_frontier(self):
        self.runtimes[0].host.record_observation('k1', 'f1', at=NOW)
        view = build_collection_view(self.supervisor)
        self.assertEqual(view.source, 'instances')
        self.assertEqual(list(view.instance_ids), ['alpha', 'beta'])
        self.assertEqual(view.describe()['frontier_sizes'], [1, 0])

    def test_a_quotes_only_peer_does_not_block_bar_or_daily_readiness(self):
        key, fingerprint = bar_observation_identity(bar())
        self.runtimes[0].host.record_observation(key, fingerprint, at=NOW)
        self.registry.put_state('alpha', {
            'universe_date': '2026-10-30',
            'screening_summary': {},
            'indicator_values': {},
        }, expected=None)
        view = build_collection_view(self.supervisor)
        self.assertFalse(view.should_emit(bar()),
                         'the quotes-only peer has no bar frontier')
        state = build_collection_state(self.supervisor)
        self.assertFalse(state.needs_daily('2026-10-30'),
                         'the quotes-only peer does not need a universe bootstrap')

    def test_a_lagging_bar_consumer_still_receives_the_bar(self):
        view = CollectionView.from_instances(self.runtimes)
        self.assertTrue(view.should_emit(bar()))

    def test_a_revision_still_reaches_the_bar_consumer(self):
        key, fingerprint = bar_observation_identity(bar())
        self.runtimes[0].host.record_observation(key, fingerprint, at=NOW)
        view = CollectionView.from_instances(self.runtimes)
        self.assertTrue(view.should_emit(bar(close='41.00')))

    def test_quotes_first_order_does_not_restore_an_empty_bar_frontier(self):
        key, fingerprint = bar_observation_identity(bar())
        self.runtimes[0].host.record_observation(key, fingerprint, at=NOW)
        view = CollectionView.from_instances(list(reversed(self.runtimes)))
        self.assertFalse(view.should_emit(bar()))

    def test_an_unprepared_universe_consumer_still_bootstraps(self):
        state = build_collection_state(self.supervisor)
        self.assertTrue(state.needs_daily('2026-10-30'))

    def test_a_missing_owner_yields_no_features_and_is_reported(self):
        class Book:
            def state(self):
                return {'trades': {'t1': {'phase': 'ENTRY_OPEN',
                                          'plan': {'symbol': 'SPY'}}}}

        self.supervisor.book = Book()
        self.supervisor.admission = type('Admission', (), {
            'attribution': self.feedback})()
        features = self.supervisor.exit_features()
        self.assertEqual(features, {})
        diagnostics = self.supervisor.exit_feature_diagnostics
        self.assertEqual(diagnostics['unresolved_trades'], ['t1'])
        self.assertEqual(diagnostics['symbols_without_owner'], ['SPY'])

    def test_each_trade_reads_only_its_own_instance_features(self):
        self.registry.put_state('alpha', {'ema9': {'SPY': '40.50'}},
                                expected=None)
        self.registry.put_state('beta', {'session_vwap': {'SPY': '99.99'}},
                                expected=None)
        self.feedback.bind('ref-alpha', 'alpha', 'trade-a')
        self.feedback.bind('ref-beta', 'beta', 'trade-b')

        class Book:
            def state(self):
                return {'trades': {
                    'trade-a': {'phase': 'ENTRY_OPEN', 'plan': {'symbol': 'SPY'}},
                    'trade-b': {'phase': 'FLAT_VERIFIED', 'plan': {'symbol': 'QQQ'}}}}

        self.supervisor.book = Book()
        self.supervisor.admission = type('Admission', (), {
            'attribution': self.feedback})()
        features = self.supervisor.exit_features()
        self.assertEqual(features, {'SPY': {'ema9': '40.50'}},
                         'the closed trade must not contribute a feature')

    def test_each_open_trade_uses_its_own_plugin_feature_keys(self):
        self.registry.put_state('alpha', {'ema9': {'SPY': '40.50'}},
                                expected=None)
        self.registry.put_state('beta', {'session_vwap': {'QQQ': '40.10'}},
                                expected=None)
        self.feedback.bind('ref-alpha', 'alpha', 'trade-a')
        self.feedback.bind('ref-beta', 'beta', 'trade-b')

        class Book:
            def state(self):
                return {'trades': {
                    'trade-a': {'phase': 'ENTRY_OPEN',
                                'plan': {'symbol': 'SPY'}},
                    'trade-b': {'phase': 'ENTRY_OPEN',
                                'plan': {'symbol': 'QQQ'}}}}

        self.supervisor.book = Book()
        self.supervisor.admission = type('Admission', (), {
            'attribution': self.feedback})()
        self.assertEqual(self.supervisor.exit_features(), {
            'SPY': {'ema9': '40.50'},
            'QQQ': {'session_vwap': '40.10'},
        })

    def test_two_owners_for_one_symbol_yield_no_feature_and_report_it(self):
        self.registry.put_state('alpha', {'ema9': {'SPY': '40.50'}}, expected=None)
        self.registry.put_state('beta', {'session_vwap': {'SPY': '99.99'}}, expected=None)
        self.feedback.bind('ref-a', 'alpha', 'trade-a')
        self.feedback.bind('ref-b', 'beta', 'trade-b')

        class Book:
            def state(self):
                return {'trades': {
                    'trade-a': {'phase': 'ENTRY_OPEN', 'plan': {'symbol': 'SPY'}},
                    'trade-b': {'phase': 'ENTRY_OPEN', 'plan': {'symbol': 'SPY'}}}}

        self.supervisor.book = Book()
        self.supervisor.admission = type('Admission', (), {
            'attribution': self.feedback})()
        self.assertEqual(self.supervisor.exit_features(), {})
        diagnostics = self.supervisor.exit_feature_diagnostics
        self.assertEqual(diagnostics['symbols_without_owner'], ['SPY'])
        self.assertEqual(diagnostics['ownership_conflicts'][0]['symbol'], 'SPY')

    def test_the_legacy_session_still_uses_the_legacy_feature_source(self):
        supervisor = LiveSupervisor(Owner(self.db), process_strategy=False,
                                    agent_mode='review', owner_source=SOURCE)
        features = supervisor.exit_features()
        self.assertEqual(features, {})
