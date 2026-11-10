"""Generic owner schema for capture dispositions and cold-start observations.

These tests use the real ExecutionBook owner (not the test entry-authority
subclass), a fresh synthetic capture-only policy with no strategy instance, and
synthetic broker/market transports. No provider, order or live write occurs.
"""
import json
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
import tempfile
import time
from unittest import TestCase
from unittest.mock import patch

from martin_agentic.capture_archive import ensure_archive_schema, read_decision_context
from martin_agentic.execution import ExecutionBook
from martin_agentic.live_session import (LiveSupervisor, record_capture_disposition,
                                         run_live_session)
from martin_agentic.simulation import SimulationSources
from test_live_session import full_snapshot
from test_supervised_host_default_loop import (SimulationReviewTransport,
                                               SimulationWake)

NOW = datetime(2026, 10, 16, 14, 0, tzinfo=timezone.utc)


def _policy():
    policy = json.loads(Path(
        'tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
    policy['session_date'] = '2026-10-16'
    policy['capture_only'] = True
    # Deliberately no strategy_instances / no strategy initializer.
    return policy


class _SlowBroker:
    simulation_only = True

    def __init__(self, delay=0.2):
        self.delay = delay

    def __call__(self, approval_ids=()):
        time.sleep(self.delay)
        return full_snapshot(NOW.isoformat())


class _EmptyMarket:
    simulation_only = True

    def __call__(self, *args, **kwargs):
        return [], {'quotes': 0}


class _SequenceMarket:
    simulation_only = True

    def __init__(self, outcomes):
        self.outcomes = list(outcomes)

    def __call__(self, *args, **kwargs):
        if self.outcomes:
            outcome = self.outcomes.pop(0)
        else:
            outcome = ([], {'quotes': 0})
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class CaptureArchiveOwnerTests(TestCase):
    def book(self, directory):
        book = ExecutionBook(Path(directory) / 'book.sqlite', _policy(),
                             simulation=True, clock=lambda: NOW)
        self.addCleanup(book.close)
        return book

    def test_fresh_owner_without_strategy_records_rejected_and_unconsumed(self):
        with tempfile.TemporaryDirectory() as directory:
            book = self.book(directory)
            tables = {row[0] for row in book.db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertIn('live_event_archive', tables)
            self.assertIn('live_decision_archive', tables)
            self.assertNotIn('strategy_instances', tables)

            rejected = {'id': 'stale-quote', 'kind': 'quote',
                        'at': NOW.isoformat(), 'source_sha256': 'fixture',
                        'data': {'symbol': 'DEMO'}}
            unconsumed = {'id': 'unconsumed-quote', 'kind': 'quote',
                          'at': NOW.isoformat(), 'source_sha256': 'fixture',
                          'data': {'symbol': 'DEMO'}}
            record_capture_disposition(book, rejected, reason='MARKET_QUOTE_STALE')
            record_capture_disposition(
                book, unconsumed,
                reason='SESSION_STOPPED_BEFORE_CONSUMPTION')

            rows = list(book.db.execute(
                'SELECT event_id,result_id FROM live_decision_archive'
                ' ORDER BY rowid'))
            self.assertEqual(len(rows), 2)
            contexts = [read_decision_context(book.db, event_id, result_id)
                        for event_id, result_id in rows]
            self.assertEqual([c['reason'] for c in contexts],
                             ['MARKET_QUOTE_STALE',
                              'SESSION_STOPPED_BEFORE_CONSUMPTION'])
            self.assertTrue(all(c['status'] == 'NOT_CONSUMED' for c in contexts))
            self.assertEqual(book.db.execute(
                'SELECT COUNT(*) FROM live_event_archive').fetchone()[0], 2)
            self.assertEqual(book.actions(), [])
            tables = {row[0] for row in book.db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            if 'execution_intents' in tables:
                self.assertEqual(book.db.execute(
                    'SELECT COUNT(*) FROM execution_intents').fetchone()[0], 0)
            self.assertEqual(book.db.execute(
                "SELECT COUNT(*) FROM live_actions WHERE kind='entry'").fetchone()[0], 0)


    def test_existing_archive_survives_owner_reopen(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'book.sqlite'
            first = ExecutionBook(path, _policy(), simulation=True,
                                  clock=lambda: NOW)
            event = {'id': 'persisted-quote', 'kind': 'quote',
                     'at': NOW.isoformat(), 'source_sha256': 'fixture'}
            record_capture_disposition(
                first, event, reason='SESSION_STOPPED_BEFORE_CONSUMPTION')
            first.close()
            second = ExecutionBook(path, _policy(), simulation=True,
                                   clock=lambda: NOW)
            self.addCleanup(second.close)
            self.assertEqual(second.db.execute(
                'SELECT COUNT(*) FROM live_event_archive').fetchone()[0], 1)
            context = read_decision_context(
                second.db, 'persisted-quote', 'admission')
            self.assertEqual(context['status'], 'NOT_CONSUMED')
            self.assertEqual(context['reason'],
                             'SESSION_STOPPED_BEFORE_CONSUMPTION')

    def test_schema_initializer_does_not_commit_an_active_transaction(self):
        with tempfile.TemporaryDirectory() as directory:
            book = self.book(directory)
            book._begin()
            ensure_archive_schema(book.db)
            self.assertTrue(book.db.in_transaction)
            book.db.rollback()
            self.assertFalse(book.db.in_transaction)

    def test_archive_disposition_rolls_back_on_write_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            book = self.book(directory)
            event = {'id': 'rollback-quote', 'kind': 'quote',
                     'at': NOW.isoformat(), 'source_sha256': 'fixture'}
            with patch('martin_agentic.live_session.archive_decision',
                       side_effect=OSError('fixture disk')):
                with self.assertRaises(OSError):
                    record_capture_disposition(
                        book, event, reason='MARKET_QUOTE_STALE')
            self.assertFalse(book.db.in_transaction)
            self.assertEqual(book.db.execute(
                'SELECT COUNT(*) FROM live_event_archive').fetchone()[0], 0)
            self.assertEqual(book.db.execute(
                'SELECT COUNT(*) FROM live_decision_archive').fetchone()[0], 0)

    def _run(self, workspace, *, observation_wait_seconds):
        sources = SimulationSources(
            broker=_SlowBroker(), market=_EmptyMarket(),
            review=SimulationReviewTransport, wake=SimulationWake())
        with patch('martin_agentic.live_session.single_instance'), \
             patch('martin_agentic.live_session.wait_for_result_or_tick',
                   side_effect=lambda *args, **kwargs: time.sleep(0.05)), \
             patch('martin_agentic.live_session.notify_local'), \
             patch('martin_agentic.live_session.print'):
            return run_live_session(
                workspace / 'book.sqlite', _policy(),
                symbols=['DEMO'] + [str(index) for index in range(9)],
                cycles=3, poll_seconds=5, agent_mode='disabled',
                simulation=True, simulation_workspace=workspace,
                simulation_sources=sources, source_mode='real_read_only',
                clock=lambda: NOW, status_file=workspace / 'status.json',
                output_dir=workspace / 'output',
                observation_wait_seconds=observation_wait_seconds)

    def test_early_market_failure_does_not_count_as_a_healthy_cycle(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory) / 'workspace'
            market = _SequenceMarket([
                ValueError('fixture market failure'), ([], {'quotes': 0}),
                ([], {'quotes': 0})])
            sources = SimulationSources(
                broker=_SlowBroker(delay=0.0), market=market,
                review=SimulationReviewTransport, wake=SimulationWake())
            with patch('martin_agentic.live_session.single_instance'), \
                 patch('martin_agentic.live_session.wait_for_result_or_tick',
                       side_effect=lambda *args, **kwargs: time.sleep(0.01)), \
                 patch('martin_agentic.live_session.notify_local'), \
                 patch('martin_agentic.live_session.print'):
                result = run_live_session(
                    workspace / 'book.sqlite', _policy(),
                    symbols=['DEMO'] + [str(index) for index in range(9)],
                    cycles=3, poll_seconds=5, agent_mode='disabled',
                    simulation=True, simulation_workspace=workspace,
                    simulation_sources=sources, source_mode='real_read_only',
                    clock=lambda: NOW, status_file=workspace / 'status.json',
                    output_dir=workspace / 'output',
                    observation_wait_seconds=0.2)
            self.assertEqual(result.get('completed_observation_cycles'), 2)
            self.assertFalse(result.get('bounded_capture_completed'))
            # The final cycle may recover, but the earlier failed cycle is not
            # retroactively counted as a healthy observation.
            self.assertLess(result.get('completed_observation_cycles'), 3)

    def test_early_in_flight_market_does_not_make_three_healthy_cycles(self):
        class SlowFirstMarket:
            simulation_only = True

            def __init__(self):
                self.calls = 0

            def __call__(self, *args, **kwargs):
                self.calls += 1
                if self.calls == 1:
                    time.sleep(0.3)
                return [], {'quotes': 0}

        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory) / 'workspace'
            sources = SimulationSources(
                broker=_SlowBroker(delay=0.0), market=SlowFirstMarket(),
                review=SimulationReviewTransport, wake=SimulationWake())
            with patch('martin_agentic.live_session.single_instance'), \
                 patch('martin_agentic.live_session.wait_for_result_or_tick',
                       side_effect=lambda *args, **kwargs: time.sleep(0.01)), \
                 patch('martin_agentic.live_session.notify_local'), \
                 patch('martin_agentic.live_session.print'):
                result = run_live_session(
                    workspace / 'book.sqlite', _policy(),
                    symbols=['DEMO'] + [str(index) for index in range(9)],
                    cycles=3, poll_seconds=5, agent_mode='disabled',
                    simulation=True, simulation_workspace=workspace,
                    simulation_sources=sources, source_mode='real_read_only',
                    clock=lambda: NOW, status_file=workspace / 'status.json',
                    output_dir=workspace / 'output',
                    observation_wait_seconds=0.05)
            self.assertLess(result.get('completed_observation_cycles'), 3)
            self.assertFalse(result.get('bounded_capture_completed'))

    def test_capture_only_nonempty_quote_and_bar_are_archived_not_consumed(self):
        class NonEmptyMarket:
            simulation_only = True

            def __init__(self):
                self.calls = 0

            def __call__(self, *args, **kwargs):
                self.calls += 1
                at = NOW.isoformat()
                base = self.calls * 10
                events = [
                    {'id': 'quote-%d' % self.calls, 'kind': 'quote', 'at': at,
                     'synthetic_input': False, 'source_sha256': 'fixture-quote-%d' % self.calls,
                     'source_type': 'desensitized_fixture',
                     'data': {'symbol': 'DEMO', 'bid': '39.99', 'ask': '40.00',
                              'bid_at': at, 'ask_at': at, 'observed_at': at}},
                    {'id': 'bar-%d' % self.calls, 'kind': 'bar', 'at': at,
                     'synthetic_input': False, 'source_sha256': 'fixture-bar-%d' % self.calls,
                     'source_type': 'desensitized_fixture',
                     'data': {'symbol': 'DEMO', 'start': at, 'end': at,
                              'open': str(40 + base), 'high': str(41 + base),
                              'low': str(39 + base), 'close': str(40.5 + base),
                              'volume': '100', 'observed_at': at}}]
                return events, {'quotes': 1, 'bars': 1}

        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory) / 'workspace'
            sources = SimulationSources(
                broker=_SlowBroker(delay=0.0), market=NonEmptyMarket(),
                review=SimulationReviewTransport, wake=SimulationWake())
            with patch('martin_agentic.live_session.single_instance'), \
                 patch('martin_agentic.live_session.wait_for_result_or_tick',
                       side_effect=lambda *args, **kwargs: time.sleep(0.01)), \
                 patch('martin_agentic.live_session.notify_local'), \
                 patch('martin_agentic.live_session.print'):
                result = run_live_session(
                    workspace / 'book.sqlite', _policy(),
                    symbols=['DEMO'] + [str(index) for index in range(9)],
                    cycles=3, poll_seconds=5, agent_mode='disabled',
                    simulation=True, simulation_workspace=workspace,
                    simulation_sources=sources, source_mode='real_read_only',
                    clock=lambda: NOW, status_file=workspace / 'status.json',
                    output_dir=workspace / 'output',
                    observation_wait_seconds=0.2)
            self.assertEqual(result.get('errors'), [])
            self.assertEqual(result.get('completed_observation_cycles'), 3)
            self.assertTrue(result.get('bounded_capture_completed'))
            con = sqlite3.connect(workspace / 'book.sqlite')
            self.addCleanup(con.close)
            self.assertEqual(con.execute(
                'SELECT COUNT(*) FROM live_event_archive').fetchone()[0], 6)
            contexts = [json.loads(row[0]) for row in con.execute(
                'SELECT payload FROM live_decision_archive ORDER BY rowid')]
            self.assertEqual(len(contexts), 6)
            self.assertTrue(all(c['status'] == 'NOT_CONSUMED' for c in contexts))
            self.assertTrue(all(c['reason'] == 'NO_STRATEGY_CONSUMER_CAPTURE_ONLY'
                                for c in contexts))
            self.assertEqual(con.execute(
                'SELECT COUNT(*) FROM live_actions').fetchone()[0], 0)
            self.assertEqual(con.execute(
                "SELECT COUNT(*) FROM live_actions WHERE kind='entry'").fetchone()[0], 0)
            tables = {row[0] for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertNotIn('strategy_instances', tables)

    def test_non_capture_missing_strategy_still_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            policy = {**_policy(), 'capture_only': False}
            book = ExecutionBook(Path(directory) / 'book.sqlite', policy,
                                 simulation=True, clock=lambda: NOW)
            self.addCleanup(book.close)
            supervisor = LiveSupervisor(book, source_mode='synthetic')
            event = {'id': 'noncapture-q', 'kind': 'quote',
                     'at': NOW.isoformat(), 'source_sha256': 'fixture',
                     'data': {'symbol': 'DEMO', 'bid': '10', 'ask': '10.01'}}
            result = supervisor.step(full_snapshot(NOW.isoformat()), [event],
                                     allow_entries=False, allow_dispatch=False)
            reasons = [error.get('reason') for error in result.get('errors') or []]
            self.assertIn('STRATEGY_RUNTIME_NOT_CONFIGURED', reasons)

    def test_capture_only_archive_failure_is_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            book = self.book(directory)
            supervisor = LiveSupervisor(book, source_mode='synthetic')
            event = {'id': 'archive-fail-q', 'kind': 'quote',
                     'at': NOW.isoformat(), 'source_sha256': 'fixture',
                     'data': {'symbol': 'DEMO', 'bid': '10', 'ask': '10.01'}}
            with patch('martin_agentic.live_session.archive_event',
                       side_effect=OSError('fixture disk')):
                result = supervisor.step(full_snapshot(NOW.isoformat()), [event],
                                         allow_entries=False, allow_dispatch=False)
            reasons = [error.get('reason') for error in result.get('errors') or []]
            self.assertIn('REJECTION_ARCHIVE_FAILED', reasons)

    def test_observation_wait_is_bounded_and_capture_only(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory) / 'workspace'
            for value in (float('inf'), float('nan'), 20.1):
                with self.assertRaises(ValueError):
                    run_live_session(
                        workspace / 'book.sqlite', _policy(),
                        symbols=['DEMO'] + [str(index) for index in range(9)],
                        cycles=1, poll_seconds=5, agent_mode='disabled',
                        simulation=True, simulation_workspace=workspace,
                        simulation_sources=SimulationSources(
                            broker=_SlowBroker(0), market=_EmptyMarket(),
                            review=SimulationReviewTransport, wake=SimulationWake()),
                        source_mode='real_read_only', clock=lambda: NOW,
                        status_file=workspace / 'status.json',
                        output_dir=workspace / 'output',
                        observation_wait_seconds=value)
            write_policy = {**_policy(), 'capture_only': False}
            with self.assertRaisesRegex(ValueError,
                                        'OBSERVATION_WAIT_REQUIRES_CAPTURE_ONLY'):
                run_live_session(
                    workspace / 'write.sqlite', write_policy,
                    symbols=['DEMO'] + [str(index) for index in range(9)],
                    cycles=1, poll_seconds=5, agent_mode='disabled',
                    simulation=True, simulation_workspace=workspace,
                    simulation_sources=SimulationSources(
                        broker=_SlowBroker(0), market=_EmptyMarket(),
                        review=SimulationReviewTransport, wake=SimulationWake()),
                    source_mode='real_read_only', clock=lambda: NOW,
                    status_file=workspace / 'status.json',
                    output_dir=workspace / 'output',
                    observation_wait_seconds=1.0)

    def test_cold_start_cycle_never_counts_an_empty_in_flight_tick(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory) / 'workspace'
            result = self._run(workspace, observation_wait_seconds=0)
            self.assertEqual(result.get('completed_observation_cycles'), 0)
            self.assertFalse(result.get('bounded_capture_completed'))
            self.assertFalse(result.get('current_observation_healthy'))
            self.assertEqual(result.get('reason'), 'BROKER_COLLECTION_IN_FLIGHT')
            con = sqlite3.connect(workspace / 'book.sqlite')
            self.addCleanup(con.close)
            self.assertEqual(con.execute(
                "SELECT COUNT(*) FROM live_actions WHERE kind='entry'"
            ).fetchone()[0], 0)
            tables = {row[0] for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            if 'execution_intents' in tables:
                self.assertEqual(con.execute(
                    'SELECT COUNT(*) FROM execution_intents').fetchone()[0], 0)

    def test_bounded_owner_waits_for_one_real_observation_per_cycle(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory) / 'workspace'
            result = self._run(workspace, observation_wait_seconds=0.5)
            self.assertEqual(result.get('cycle'), 3)
            self.assertEqual(result.get('completed_observation_cycles'), 3)
            self.assertTrue(result.get('bounded_capture_completed'))
            self.assertTrue(result.get('current_observation_healthy'))
            self.assertTrue(result.get('read_only_capture_healthy'))
            self.assertEqual(result.get('errors'), [])
            con = sqlite3.connect(workspace / 'book.sqlite')
            self.addCleanup(con.close)
            self.assertEqual(con.execute(
                "SELECT COUNT(*) FROM live_actions WHERE kind='entry'"
            ).fetchone()[0], 0)
            tables = {row[0] for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            if 'execution_intents' in tables:
                self.assertEqual(con.execute(
                    'SELECT COUNT(*) FROM execution_intents').fetchone()[0], 0)
