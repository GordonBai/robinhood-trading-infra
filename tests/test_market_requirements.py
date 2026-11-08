"""Market collection follows plugin requirements instead of a Martin profile."""
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, patch

from martin_luk.live_session import collect_live_events
from martin_agentic.market import unwrap_response

AT = datetime(2026, 10, 16, 14, 0, tzinfo=timezone.utc)


class MarketRequirementTests(TestCase):
    def signals(self):
        signals = MagicMock()
        signals.state.return_value = {}
        signals.collection_view.return_value = {}
        del signals.collection_state
        return signals

    def run_collect(self, requirements, now=AT):
        reader = MagicMock()
        reader.fetch.return_value = {'data': {}}
        normalized = {'data_usable': True, 'errors': [], 'freshness_errors': [],
                      'bars': [], 'quotes': []}
        with patch('martin_agentic.control_tasks.owned_read_transport',
                   MagicMock()), \
             patch('martin_agentic.live_session.CodexMarketTransport',
                   MagicMock()), \
             patch('martin_agentic.live_session.RobinhoodReader',
                   return_value=reader), \
             patch('martin_agentic.live_session.persist_raw_record'), \
             patch('martin_agentic.live_session.collect_daily') as daily, \
             patch('martin_agentic.live_session.inspect_recording',
                   return_value=normalized), \
             patch('martin_agentic.live_session.filter_unchanged_bars',
                   return_value=([], 0)):
            events, meta = collect_live_events(
                self.signals(), symbols=[str(index) for index in range(10)],
                clock=lambda: now, requirements=requirements)
        return reader, daily, events, meta

    def test_a_quotes_only_plugin_skips_universe_and_bar_bootstrap(self):
        reader, daily, events, meta = self.run_collect(('quotes',))
        daily.assert_not_called()
        calls = [call.args[0] for call in reader.fetch.call_args_list]
        self.assertNotIn('get_equity_historicals', calls)
        self.assertIn('get_equity_quotes', calls)
        self.assertFalse(meta['daily_bootstrap_performed'])

    def test_requiring_bars_requests_them_without_daily_screening(self):
        reader, daily, events, meta = self.run_collect(
            ('quotes', '5minute-bars'), now=AT + timedelta(minutes=6))
        daily.assert_not_called()
        calls = [call.args[0] for call in reader.fetch.call_args_list]
        self.assertIn('get_equity_historicals', calls)
        self.assertFalse(meta['bars_waiting_for_first_completion'])

    def test_bars_wait_for_first_completed_5minute_bar(self):
        reader, daily, events, meta = self.run_collect(
            ('quotes', '5minute-bars'), now=AT - timedelta(minutes=30))
        daily.assert_not_called()
        calls = [call.args[0] for call in reader.fetch.call_args_list]
        self.assertNotIn('get_equity_historicals', calls)
        self.assertIn('get_equity_quotes', calls)
        self.assertTrue(meta['bars_waiting_for_first_completion'])
        self.assertTrue(meta['first_bar_available_at'])

    def test_actual_pre_first_bar_error_is_not_treated_as_a_series(self):
        import json
        from pathlib import Path
        fixture = json.loads((Path(__file__).parent / 'fixtures' /
                              'robinhood_historicals_before_first_bar.json').read_text())
        with self.assertRaises(ValueError):
            unwrap_response(fixture)

    def test_an_unsupported_requirement_is_refused_before_any_read(self):
        with self.assertRaisesRegex(ValueError, 'MARKET_REQUIREMENT_UNSUPPORTED'):
            collect_live_events(self.signals(),
                                symbols=[str(index) for index in range(10)],
                                clock=lambda: AT,
                                requirements=('quotes', 'options-chain'))
