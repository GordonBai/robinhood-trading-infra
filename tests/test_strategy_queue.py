"""Durable queue recovery over temporary synthetic books; no broker calls."""
from datetime import datetime, timedelta
import json
import tempfile
from unittest import TestCase
from unittest.mock import patch

from martin_agentic.strategy_queue import DurableStrategyQueue
from trading_contracts.codec import encode_input
import test_signal_host_isolation as fixture
from shadow_fixture import events


class FakeRunner:
    def __init__(self, plugin, *, warm=False):
        self.plugin = plugin; self.warm = warm; self.process = object() if warm else None
        self.request = None; self.starts = []; self.poll_calls = 0; self.close_calls = 0
        self.hold = False; self.failure = None

    @property
    def next_deadline_monotonic(self):
        return 999999 if self.request is not None else None

    def start(self, request, *, payload=None):
        if self.request is not None:
            raise AssertionError('duplicate runner request')
        if payload is not None and payload != encode_input(request):
            raise AssertionError('runner payload does not match request')
        self.starts.append(request); self.request = request; self.process = object()

    def poll(self):
        self.poll_calls += 1
        if self.request is None or self.hold:
            return None
        request = self.request; self.request = None
        if not self.warm:
            self.process = None
        if self.failure is not None:
            failure = self.failure; self.failure = None; raise failure
        return self.plugin.decide(request)

    def close(self):
        self.close_calls += 1; self.request = None; self.process = None


class SimulatedCrash(BaseException):
    pass


class StrategyQueueTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.fixture = fixture.SignalHostIsolationTests()
        _, self.signals = self.fixture.books(self.temp.name)
        self.addCleanup(self.fixture.doCleanups)

    def event(self, identity='tick-a'):
        at = self.fixture.now.isoformat()
        return dict(id=identity, kind='quote', at=at, synthetic_input=True,
            source_sha256='fixture-'+identity,
            data=dict(symbol='DEMO', bid='40', ask='40.01', bid_at=at, ask_at=at, observed_at=at))

    def queue(self, *, source='a'*64, warm=False, bound=2048):
        runner = FakeRunner(self.signals.plugin, warm=warm)
        queue = DurableStrategyQueue(self.signals, runner=runner, source_fingerprint=source, max_pending=bound)
        self.addCleanup(queue.close)
        return queue, runner

    def test_fifo_untouched_until_commit_and_one_poll_one_start_per_advance(self):
        queue, runner = self.queue()
        first, second = self.event('first'), self.event('second')
        queue.enqueue(first); queue.enqueue(second)
        self.assertEqual(self.signals.book.db.execute('SELECT count(*) FROM live_inputs').fetchone()[0], 0)
        self.assertTrue(queue.status()['busy'])
        self.assertEqual(queue.advance()['started'], 'first')
        self.assertEqual(runner.poll_calls, 0)
        result = queue.advance()
        self.assertEqual([row['event_id'] for row in result['completed']], ['first'])
        self.assertEqual(result['started'], 'second'); self.assertEqual(runner.poll_calls, 1)
        queue.advance()
        self.assertEqual(queue.status()['committed'], 2)
        self.assertEqual(runner.poll_calls, 2)
        self.assertFalse(queue.status()['busy'])

    def test_duplicate_conflict_and_backpressure_do_not_consume_or_overwrite(self):
        queue, _ = self.queue(bound=1)
        event = self.event(); queue.enqueue(event)
        self.assertTrue(queue.enqueue(event)['duplicate'])
        with self.assertRaisesRegex(ValueError, 'STRATEGY_EVENT_ID_CONFLICT'):
            queue.enqueue({**event, 'source_sha256': 'changed'})
        with self.assertRaisesRegex(ValueError, 'CAPACITY_EXCEEDED'):
            queue.enqueue(self.event('over-bound'))
        self.assertEqual(queue.status()['pending'], 1)
        self.assertEqual(self.signals.book.db.execute('SELECT count(*) FROM live_inputs').fetchone()[0], 0)

    def test_restart_reuses_nested_exact_request_without_extending_asof(self):
        queue, runner = self.queue(); event = self.event(); queue.enqueue(event); queue.advance()
        stored = queue._row(event['id'])['request_payload']; old_time = runner.starts[0].as_of
        runner.close()  # Simulate lost owner after its child has been reaped.
        self.fixture.now += timedelta(seconds=1)
        restarted, next_runner = self.queue()
        self.assertEqual(restarted.recovered_running, 1)
        self.assertEqual(restarted.advance()['started'], event['id'])
        self.assertEqual(next_runner.starts[0].as_of, old_time)
        self.assertEqual(encode_input(next_runner.starts[0]), stored)
        self.assertIn('machines', next_runner.starts[0].checkpoint)
        self.assertEqual(restarted.advance()['completed'][0]['event_id'], event['id'])

    def test_warm_idle_process_is_reused_instead_of_treated_as_busy(self):
        queue, runner = self.queue(warm=True)
        queue.enqueue(self.event('one')); queue.enqueue(self.event('two'))
        self.assertIsNotNone(runner.process)
        self.assertEqual(queue.advance()['started'], 'one')
        self.assertEqual(queue.advance()['started'], 'two')
        queue.advance()
        self.assertEqual(len(runner.starts), 2)
        self.assertIsNotNone(runner.process)
        self.assertFalse(queue.status()['busy'])

    def test_crash_after_real_signal_commit_repairs_ack_without_duplicate_outbox(self):
        for raw in events()[1:4]:
            event = self.fixture.event(raw); self.fixture.now = datetime.fromisoformat(event['at'])
            self.signals.consume(event)
        event = self.fixture.event(events()[4]); self.fixture.now = datetime.fromisoformat(event['at'])
        queue, runner = self.queue(); queue.enqueue(event); queue.advance()
        with patch.object(queue, '_ack', side_effect=SimulatedCrash()):
            with self.assertRaises(SimulatedCrash): queue.advance()
        pending = self.signals.pending(); self.assertEqual(len(pending), 1)
        count = self.signals.book.db.execute('SELECT count(*) FROM live_inputs').fetchone()[0]
        self.assertEqual(queue._row(event['id'])['state'], 'RUNNING')
        restarted, next_runner = self.queue()
        result = restarted.advance()
        self.assertTrue(result['completed'][0]['duplicate'])
        self.assertEqual(next_runner.starts, [])
        self.assertEqual(self.signals.pending(), pending)
        self.assertEqual(self.signals.book.db.execute('SELECT count(*) FROM live_inputs').fetchone()[0], count)

    def test_expiry_is_durable_rejection_and_next_fresh_event_still_runs(self):
        queue, runner = self.queue(); event = self.event(); queue.enqueue(event)
        self.fixture.now += timedelta(seconds=31)
        result = queue.advance()
        self.assertEqual(result['rejected'][0]['reason'], 'STALE_STRATEGY_EVENT')
        self.assertEqual(result['errors'], result['rejected']); self.assertEqual(runner.starts, [])
        self.assertEqual(queue._row(event['id'])['state'], 'REJECTED')
        queue.enqueue(self.event('fresh')); queue.advance(); queue.advance()
        self.assertEqual(queue.status()['committed'], 1)

    def test_expired_restarted_task_is_not_reprepared_with_new_clock(self):
        queue, runner = self.queue(); queue.enqueue(self.event()); queue.advance(); runner.close()
        self.fixture.now += timedelta(seconds=31)
        restarted, next_runner = self.queue()
        result = restarted.advance()
        self.assertEqual(result['rejected'][0]['reason'], 'STALE_STRATEGY_EVENT')
        self.assertEqual(next_runner.starts, [])

    def test_plugin_crash_is_generic_persistent_and_fifo_continues(self):
        queue, runner = self.queue(); queue.enqueue(self.event('bad')); queue.enqueue(self.event('good'))
        queue.advance(); runner.failure = ValueError('private upstream contents must not leak')
        result = queue.advance()
        self.assertEqual(result['rejected'][0]['reason'], 'STRATEGY_TASK_RUNNER_FAILED')
        self.assertNotIn('private', json.dumps(result))
        self.assertEqual(result['started'], 'good')
        queue.advance(); self.assertEqual(queue.status()['committed'], 1)

    def test_cas_failure_keeps_current_checkpoint_and_rejects_task(self):
        queue, _ = self.queue(); queue.enqueue(self.event()); queue.advance()
        state = self.signals.state(); state['revision'] = 'another-owner-update'
        self.signals.book.db.execute('UPDATE live_strategy SET payload=?', (json.dumps(state),)); self.signals.book.db.commit()
        result = queue.advance()
        self.assertEqual(result['rejected'][0]['reason'], 'STRATEGY_CHECKPOINT_CHANGED_RETRY_REQUIRED')
        self.assertEqual(self.signals.state(), state)
        self.assertEqual(self.signals.book.db.execute('SELECT count(*) FROM live_inputs').fetchone()[0], 0)

    def test_source_and_config_mismatch_are_visible_durable_errors(self):
        queue, _ = self.queue(); queue.enqueue(self.event()); queue.close()
        restarted, _ = self.queue(source='b'*64)
        result = restarted.advance()
        self.assertEqual(result['errors'][0]['reason'], 'STRATEGY_QUEUE_SOURCE_CHANGED')
        self.assertEqual(result['rejected'], result['errors'])
        restarted.enqueue(self.event('configuration')); restarted.close()
        self.signals.cost += 1
        changed, _ = self.queue(source='b'*64)
        result = changed.advance()
        self.assertEqual(result['errors'][0]['reason'], 'STRATEGY_QUEUE_CONFIG_CHANGED')

    def test_active_conflicting_inbox_discards_worker_result_without_commit(self):
        queue, runner = self.queue(); event = self.event(); queue.enqueue(event); queue.advance()
        before = self.signals.state()
        self.signals.book.db.execute('INSERT INTO live_inputs VALUES(?,?)', (event['id'], '0'*64)); self.signals.book.db.commit()
        with patch.object(self.signals, 'commit', side_effect=AssertionError('conflicted output cannot commit')):
            result = queue.advance()
        self.assertEqual(result['errors'][0]['reason'], 'STRATEGY_EVENT_ID_CONFLICT')
        self.assertEqual(queue._row(event['id'])['state'], 'REJECTED')
        self.assertEqual(self.signals.state(), before); self.assertIsNone(runner.request)

    def test_corrupt_persisted_request_is_rejected_without_starting_worker(self):
        queue, runner = self.queue(); queue.enqueue(self.event()); queue.advance(); runner.close()
        self.signals.book.db.execute("UPDATE strategy_tasks SET request_hash=?", ('0'*64,)); self.signals.book.db.commit()
        restarted, next_runner = self.queue(); result = restarted.advance()
        self.assertEqual(result['errors'][0]['reason'], 'STRATEGY_QUEUE_REQUEST_CORRUPT')
        self.assertEqual(next_runner.starts, [])

    def test_close_reaps_runner_preserves_pending_and_is_idempotent(self):
        queue, runner = self.queue(); queue.enqueue(self.event()); queue.advance()
        first = queue.close(); second = queue.close()
        self.assertTrue(first['closed']); self.assertEqual(second['pending'], 1)
        self.assertEqual(runner.close_calls, 1)
        self.assertEqual(self.signals.book.db.execute('SELECT count(*) FROM live_inputs').fetchone()[0], 0)
        self.assertEqual(queue._row('tick-a')['state'], 'QUEUED')
        with self.assertRaisesRegex(ValueError, 'CLOSED'): queue.enqueue(self.event('new'))
