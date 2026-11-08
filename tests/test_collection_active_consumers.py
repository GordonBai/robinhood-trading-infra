"""Collection must follow the consumers that are enabled *now*.

Reading the runtime list captured at start-up let a disabled instance block
readiness and dedupe forever: alpha was prepared, beta was disabled with an
empty state, and every pass still bootstrapped the daily universe.

These tests drive the real registry, the real supervisor, the real builders and
the real ``collect_live_events``, with the provider call stopped at a spy.

Synthetic fixtures only; no market request, no broker.
"""
import json
import sqlite3
import tempfile
from datetime import datetime, timezone
from decimal import Decimal
from unittest import TestCase
from unittest.mock import patch

from martin_agentic.collection_view import build_collection_state, build_collection_view
from martin_agentic.control_tasks import FrozenSignals
from martin_luk.live_session import LiveSupervisor, collect_live_events
from trading_contracts.instance import InstanceBudget, PluginManifest
from trading_contracts.market import bar_observation_identity
from trading_infra.instance_registry import InstanceRegistry
from trading_infra.instance_runtime import InstanceRuntime
from trading_infra.instance_supervisor import InstanceSupervisor

SYMBOLS = ['AAPL', 'AMD', 'HOOD', 'IONQ', 'META', 'NVDA', 'PLTR', 'RKLB',
           'SOFI', 'TSLA']
NOW = datetime.fromisoformat('2026-10-16T14:00:00+00:00')
DAY = '2026-10-16'


def manifest(plugin_id):
    return PluginManifest(plugin_id=plugin_id, version='1.0', state_schema='s-1',
                          config={'step': '1', 'cost_buffer_per_share': '0.05'},
                          data_requirements=('quotes', '5minute-bars',
                                             'universe-screen'),
                          capabilities=('advisory-entry',), max_inflight=8)


def budget():
    return InstanceBudget(max_cash=Decimal('100'), max_planned_loss=Decimal('2.50'),
                          max_notional=Decimal('100'), max_concurrent=1)


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


def prepared_state(**overrides):
    state = {'universe_date': DAY, 'screening_summary': {'n': 1},
             'indicator_values': {'SOFI': {}}, 'universe_complete': True,
             'machines': {}}
    state.update(overrides)
    return state


class ActiveConsumerTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = sqlite3.connect(self.temp.name + '/active.sqlite',
                                  isolation_level=None)
        self.addCleanup(self.db.close)
        self.registry = InstanceRegistry(self.db, clock=lambda: NOW)
        self.registry.register('alpha', manifest('alpha'), budget(),
                               mode='shadow', enabled=True)
        self.registry.register('beta', manifest('beta'), budget(),
                               mode='shadow', enabled=True)
        self.runtimes = []
        for instance_id in ('alpha', 'beta'):
            runtime = InstanceRuntime(self.registry,
                                      self.registry.instance(instance_id),
                                      runner=FakeRunner(),
                                      source_fingerprint='f' * 64, clock=lambda: NOW)
            self.addCleanup(runtime.close)
            self.runtimes.append(runtime)
        self.bar = {'symbol': 'SOFI', 'begins_at': '2026-10-16T13:30:00+00:00',
                    'available_at': '2026-10-16T13:35:00+00:00',
                    'observed_at': NOW.isoformat(), 'interval': '5minute',
                    'bounds': 'regular', 'adjustment': 'split', 'open': '14',
                    'high': '14.1', 'low': '13.9', 'close': '14.05', 'volume': 10000}

    def supervisor(self):
        class Book:
            db = self.db
            clock = lambda self: NOW          # noqa: E731
            simulation = True
            policy = {}

            def _begin(self):
                self.db.execute('BEGIN IMMEDIATE')

        return LiveSupervisor(Book(), process_strategy=False, agent_mode='review',
                              owner_source='f' * 64,
                              instance_supervisor=InstanceSupervisor(self.runtimes))

    def collect(self, supervisor, *, daily_calls=None):
        view = build_collection_view(supervisor)
        state = build_collection_state(supervisor)
        normalized = {'data_usable': True, 'bars': [self.bar],
                      'quotes': [{'symbol': 'SOFI', 'bid': '14', 'ask': '14.01',
                                  'observed_at': NOW.isoformat()}]}
        calls = daily_calls if daily_calls is not None else []

        def fake_daily(reader, *, symbols, start_time, end_time):
            calls.append(1)
            return {'universe_id': 'u'}

        with patch('martin_agentic.live_session.CodexMarketTransport'), \
             patch('martin_agentic.live_session.RobinhoodReader') as reader, \
             patch('martin_agentic.live_session.inspect_recording',
                   return_value=normalized), \
             patch('martin_agentic.live_session.persist_raw_record'), \
             patch('martin_agentic.live_session.collect_daily',
                   side_effect=fake_daily), \
             patch('martin_agentic.live_session.universe_event',
                   side_effect=lambda daily, **kw: {'id': 'u-1', 'kind': 'universe',
                                                    'at': NOW.isoformat(), 'data': {},
                                                    'synthetic_input': False,
                                                    'source_sha256': 'u'}):
            reader.return_value.fetch.return_value = {'fixture': 'raw'}
            events, summary = collect_live_events(
                FrozenSignals({}, view=view, state_view=state),
                symbols=SYMBOLS, clock=lambda: NOW)
        return events, summary, calls

    def disable(self, instance_id):
        self.registry.set_mode(instance_id, 'disabled', enabled=False)

    def enable(self, instance_id):
        self.registry.set_mode(instance_id, 'shadow', enabled=True)

    def test_a_disabled_instance_does_not_hold_readiness_back(self):
        self.registry.put_state('alpha', prepared_state(), expected=None)
        self.disable('beta')
        supervisor = self.supervisor()
        first_events, first, first_calls = self.collect(supervisor)
        second_events, second, second_calls = self.collect(supervisor)
        self.assertEqual(first_calls, [])
        self.assertEqual(second_calls, [], 'no repeated bootstrap for a disabled peer')
        self.assertFalse(first['daily_bootstrap_performed'])
        self.assertEqual(first['collection_state']['instance_ids'], ['alpha'])

    def test_a_disabled_instance_does_not_hold_the_dedupe_frontier_back(self):
        key, fingerprint = bar_observation_identity(self.bar)
        self.registry.put_state('alpha', prepared_state(), expected=None)
        self.runtimes[0].host.record_observation(key, fingerprint, at=NOW)
        self.disable('beta')
        supervisor = self.supervisor()
        events, summary, _ = self.collect(supervisor)
        self.assertEqual(summary['unchanged_bar_observations_omitted'], 1)
        self.assertEqual([event['kind'] for event in events], ['quote'])

    def test_a_re_enabled_instance_catches_up_with_one_bootstrap(self):
        self.registry.put_state('alpha', prepared_state(), expected=None)
        self.disable('beta')
        supervisor = self.supervisor()
        self.collect(supervisor)
        # Beta is re-enabled but genuinely behind: one bootstrap, then silence.
        self.enable('beta')
        events, summary, calls = self.collect(supervisor)
        self.assertEqual(calls, [1])
        self.assertTrue(summary['daily_bootstrap_performed'])
        self.assertEqual([event['kind'] for event in events][0], 'universe')

    def test_once_every_active_consumer_is_ready_the_bootstrap_stops(self):
        self.registry.put_state('alpha', prepared_state(), expected=None)
        self.registry.put_state('beta', prepared_state(), expected=None)
        supervisor = self.supervisor()
        _, _, calls = self.collect(supervisor)
        self.assertEqual(calls, [])

    def test_all_consumers_disabled_skips_the_bootstrap_entirely(self):
        self.disable('alpha')
        self.disable('beta')
        supervisor = self.supervisor()
        events, summary, calls = self.collect(supervisor)
        self.assertEqual(calls, [])
        self.assertFalse(summary['daily_bootstrap_performed'])
        self.assertTrue(summary['collection_state']['no_active_consumers'])

    def test_disabling_never_removes_existing_position_management(self):
        self.disable('alpha')
        supervisor = self.supervisor()
        # The instance group still exists and still advances; only collection
        # readiness ignores it. Management is not withdrawn by a mode change.
        self.assertEqual(supervisor.instance_supervisor.instance_ids(),
                         ['alpha', 'beta'])
        # Only collection readiness drops the disabled one; the other consumer
        # is unaffected and management of any position is untouched.
        self.assertEqual(list(build_collection_state(supervisor).instance_ids),
                         ['beta'])

    def test_the_legacy_session_is_unchanged(self):
        class LegacyBook:
            db = self.db
            clock = lambda self: NOW          # noqa: E731
            simulation = True
            policy = {}

            def _begin(self):
                self.db.execute('BEGIN IMMEDIATE')

        supervisor = LiveSupervisor(LegacyBook(), process_strategy=False,
                                    agent_mode='review', owner_source='f' * 64)
        self.assertFalse(supervisor.instance_supervisor_enabled)
        state = build_collection_state(supervisor)
        self.assertEqual(state.source, 'legacy')
        self.assertTrue(state.needs_daily(DAY), 'an empty legacy state bootstraps')
