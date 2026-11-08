"""The real collector must decide daily bootstrap from the consumer it runs.

The previous slice gave collection a per-instance bar frontier but still fed it
the legacy strategy state for day metadata, which the instance path never
updates. A fully prepared instance group therefore bootstrapped the daily
universe again on every pass.

These tests call the real ``collect_live_events`` with the provider call stopped
at a spy, as the review allowed: transport, reader and raw persistence are
fakes, but the bootstrap decision, bar filtering and event shaping are real.

Synthetic fixtures only; no market request, no broker.
"""
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from martin_agentic.collection_view import CollectionState
from martin_agentic.control_tasks import FrozenSignals
from martin_luk.live_session import collect_live_events

SYMBOLS = ['AAPL', 'AMD', 'HOOD', 'IONQ', 'META', 'NVDA', 'PLTR', 'RKLB',
           'SOFI', 'TSLA']
DAY = '2026-10-16'


class CollectorBootstrapTests(TestCase):
    def setUp(self):
        self.now = datetime.fromisoformat('2026-10-16T14:00:00+00:00')
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.bar = {'symbol': 'SOFI', 'begins_at': '2026-10-16T13:30:00+00:00',
                    'available_at': '2026-10-16T13:35:00+00:00',
                    'observed_at': self.now.isoformat(), 'interval': '5minute',
                    'bounds': 'regular', 'adjustment': 'split', 'open': '14',
                    'high': '14.1', 'low': '13.9', 'close': '14.05', 'volume': 10000}
        self.quote = {'symbol': 'SOFI', 'bid': '14', 'ask': '14.01',
                      'observed_at': self.now.isoformat()}

    def prepared(self, *, universe_date=DAY, extra=None):
        state = {'universe_date': universe_date, 'screening_summary': {'n': 1},
                 'indicator_values': {'SOFI': {}}, 'universe_complete': True,
                 'machines': {}}
        state.update(extra or {})
        return state

    def run_collector(self, collection_state, *, daily=None):
        normalized = {'data_usable': True, 'bars': [self.bar], 'quotes': [self.quote]}
        calls = []

        def fake_daily(reader, *, symbols, start_time, end_time):
            calls.append({'symbols': list(symbols), 'start': start_time,
                          'end': end_time})
            return {'universe_id': 'u', 'histories': {}, 'indicators': {},
                    'rejections': {}, 'as_of': self.now.isoformat()}

        with patch('martin_agentic.live_session.CodexMarketTransport'), \
             patch('martin_agentic.live_session.RobinhoodReader') as reader, \
             patch('martin_agentic.live_session.inspect_recording',
                   return_value=normalized), \
             patch('martin_agentic.live_session.persist_raw_record'), \
             patch('martin_agentic.live_session.collect_daily',
                   side_effect=fake_daily), \
             patch('martin_agentic.live_session.universe_event',
                   side_effect=lambda daily, **kw: {'id': 'universe-1',
                                                    'kind': 'universe',
                                                    'at': self.now.isoformat(),
                                                    'data': {}, 'synthetic_input': False,
                                                    'source_sha256': 'u'}):
            reader.return_value.fetch.return_value = {'fixture': 'raw'}
            from martin_agentic.collection_view import CollectionView
            signals = SimpleNamespace(state=lambda: {},
                                      collection_view=lambda: CollectionView(
                                          frontiers=[], instance_ids=[]),
                                      collection_state=lambda: collection_state)
            events, summary = collect_live_events(signals, symbols=SYMBOLS,
                                                  clock=lambda: self.now)
        return events, summary, calls

    def test_a_fully_prepared_group_does_not_bootstrap_again(self):
        state = CollectionState(states=[self.prepared(), self.prepared()],
                                instance_ids=['alpha', 'beta'])
        events, summary, calls = self.run_collector(state)
        self.assertEqual(calls, [], 'every consumer was ready; no daily call')
        self.assertFalse(summary['daily_bootstrap_performed'])
        self.assertEqual([event['kind'] for event in events], ['bar', 'quote'])
        self.assertEqual(summary['collection_state']['source'], 'instances')

    def test_one_lagging_instance_triggers_exactly_one_bootstrap(self):
        state = CollectionState(states=[self.prepared(), {}],
                                instance_ids=['alpha', 'beta'])
        events, summary, calls = self.run_collector(state)
        self.assertEqual(len(calls), 1)
        self.assertTrue(summary['daily_bootstrap_performed'])
        self.assertEqual([event['kind'] for event in events],
                         ['universe', 'bar', 'quote'])
        self.assertEqual(state.lagging(DAY), ['beta'])

    def test_a_new_session_day_triggers_a_bootstrap(self):
        state = CollectionState(states=[self.prepared(universe_date='2026-10-15')],
                                instance_ids=['alpha'])
        events, summary, calls = self.run_collector(state)
        self.assertEqual(len(calls), 1)
        self.assertEqual(events[0]['kind'], 'universe')

    def test_a_legacy_frozen_signals_object_keeps_its_old_behaviour(self):
        legacy = FrozenSignals(dict(self.prepared()))
        normalized = {'data_usable': True, 'bars': [self.bar], 'quotes': [self.quote]}
        calls = []

        def fake_daily(reader, *, symbols, start_time, end_time):
            calls.append(1)
            return {'universe_id': 'u', 'histories': {}, 'indicators': {},
                    'rejections': {}, 'as_of': self.now.isoformat()}

        with patch('martin_agentic.live_session.CodexMarketTransport'), \
             patch('martin_agentic.live_session.RobinhoodReader') as reader, \
             patch('martin_agentic.live_session.inspect_recording',
                   return_value=normalized), \
             patch('martin_agentic.live_session.persist_raw_record'), \
             patch('martin_agentic.live_session.collect_daily', side_effect=fake_daily), \
             patch('martin_agentic.live_session.universe_event',
                   side_effect=lambda daily, **kw: {'id': 'universe-1',
                                                    'kind': 'universe',
                                                    'at': self.now.isoformat(),
                                                    'data': {}, 'synthetic_input': False,
                                                    'source_sha256': 'u'}):
            reader.return_value.fetch.return_value = {'fixture': 'raw'}
            events, summary = collect_live_events(legacy, symbols=SYMBOLS,
                                                  clock=lambda: self.now)
        self.assertEqual(calls, [], 'a prepared legacy state still skips bootstrap')
        self.assertEqual(summary['collection_state']['source'], 'legacy')


class CollectionStateUnitTests(TestCase):
    def test_without_optional_instance_ids_unprepared_state_still_bootstraps(self):
        view = CollectionState(states=[{}])
        self.assertTrue(view.needs_daily(DAY))

    def test_without_optional_instance_ids_prepared_state_does_not_bootstrap(self):
        view = CollectionState(states=[{
            'universe_date': DAY,
            'screening_summary': {},
            'indicator_values': {},
        }])
        self.assertFalse(view.needs_daily(DAY))

    def test_an_explicit_empty_subset_means_no_universe_consumer(self):
        view = CollectionState(states=[{}], instance_ids=['alpha'],
                               universe_instance_ids=[])
        self.assertFalse(view.needs_daily(DAY))

    def test_identity_length_mismatch_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'COLLECTION_STATE_IDENTITY_MISMATCH'):
            CollectionState(states=[{}, {}], instance_ids=['alpha'])

    def test_unknown_universe_consumer_is_rejected(self):
        with self.assertRaisesRegex(ValueError,
                                    'COLLECTION_STATE_UNKNOWN_UNIVERSE_INSTANCE'):
            CollectionState(states=[{}], instance_ids=['alpha'],
                            universe_instance_ids=['beta'])

    def test_readiness_requires_all_three_fields_on_the_same_day(self):
        for missing in ('universe_date', 'screening_summary', 'indicator_values'):
            state = {'universe_date': DAY, 'screening_summary': {},
                     'indicator_values': {}}
            state.pop(missing)
            view = CollectionState(states=[state], instance_ids=['a'])
            self.assertTrue(view.needs_daily(DAY), missing)

    def test_a_group_with_no_active_consumer_does_not_bootstrap(self):
        view = CollectionState(states=[], instance_ids=[])
        self.assertTrue(view.no_active_consumers)
        self.assertFalse(view.needs_daily(DAY),
                         'preparing a universe for nobody is not work worth doing')
        self.assertTrue(view.describe()['no_active_consumers'])

    def test_a_single_lagging_consumer_is_named(self):
        view = CollectionState(states=[{'universe_date': DAY},
                                       {'universe_date': '2026-10-15'}],
                               instance_ids=['alpha', 'beta'])
        self.assertEqual(view.lagging(DAY), ['alpha', 'beta'])

    def test_the_state_is_detached_from_the_owner_mapping(self):
        owner_state = {'universe_date': DAY, 'screening_summary': {},
                       'indicator_values': {}, 'machines': {'x': 1}}
        view = CollectionState(states=[owner_state], instance_ids=['a'])
        owner_state['machines']['y'] = 2
        self.assertEqual(view.state()['machines'], {'x': 1})
        json.dumps(view.state())
