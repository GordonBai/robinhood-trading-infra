"""Controller scheduling and freshness tests; all broker inputs are synthetic."""
from contextlib import ExitStack
from copy import deepcopy
from datetime import datetime, timedelta
import json
from pathlib import Path
import tempfile
import threading
from unittest import TestCase
from unittest.mock import MagicMock, patch

from test_live_session import full_snapshot
from martin_agentic.control_tasks import BackgroundTask, FrozenSignals
from martin_luk.live_session import run_live_session
from martin_agentic.transport import TransportError


class IdleDispatcher:
    def __init__(self, *args, **kwargs):
        self.task = BackgroundTask('fixture-dispatch')
    def __call__(self, **kwargs):
        return {'agent_called': False, 'status': 'NO_READY_ACTION'}
    def close(self):
        return {'attention_required': False}


class MarketPrefetchTests(TestCase):
    def setUp(self):
        self.now = datetime.fromisoformat('2026-10-16T14:00:00+00:00')
        self.state = {'revision': 0}
        self.consumed = []
        self.state_reader_threads = []
        self.owner = threading.get_ident()

    def event(self, identity):
        return {'id': identity, 'kind': 'data_status', 'at': self.now.isoformat(), 'data': {}}

    def session(self, collect, *, read=None, cycles=3, mode='disabled', on_step=None):
        policy = json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        policy['capture_only'] = mode == 'disabled'
        book = MagicMock()
        book.simulation = True
        book.policy = policy
        book.active_issues.return_value = []
        book.pending_approval_ids.return_value = []
        book.recover.return_value = {}
        book.recovery_status.return_value = {
            'acknowledged_trade_ids': [], 'acknowledged_action_ids': [], 'blockers': []}
        book.summary.return_value = {'state': {'trades': {}}}
        book.actions.return_value = []
        journal = MagicMock()
        journal.summary.return_value = {
            'read_healthy': True, 'reconciliation_issues': [], 'blockers': [],
            'positions': {}, 'open_equity_orders': 0, 'other_asset_open_orders': 0,
            'pending_approvals': 0}
        supervisor = MagicMock()
        def state():
            self.state_reader_threads.append(threading.get_ident())
            return deepcopy(self.state)
        supervisor.signals.state.side_effect = state
        def step(snapshot, events, **kwargs):
            self.consumed.extend(deepcopy(events))
            if events:
                self.state['revision'] += 1
            if on_step:
                on_step(events)
            return {'lifecycle': {}, 'new_entries_blocked': not kwargs.get('allow_entries', True)}
        supervisor.step.side_effect = step
        def wait(task, dispatcher, seconds):
            if task.thread is not None:
                task.thread.join(timeout=.05)
        with tempfile.TemporaryDirectory() as folder, ExitStack() as stack:
            prefix = 'martin_agentic.live_session.'
            for name, value in [('single_instance', MagicMock()), ('ExecutionBook', MagicMock(return_value=book)),
                                ('BrokerJournal', MagicMock(return_value=journal)),
                                ('CodexOrderWorker', MagicMock()), ('AsyncDispatcher', IdleDispatcher)]:
                stack.enter_context(patch(prefix+name, value))
            # ``run_live_session`` is the Martin compatibility entry and injects
            # its own supervisor class. Patch that class, not the generic base
            # the wrapper would ignore.
            stack.enter_context(patch('martin_luk.live_session.LiveSupervisor',
                                      MagicMock(return_value=supervisor)))
            stack.enter_context(patch('martin_agentic.async_native_review.AsyncNativeReviewDispatcher', IdleDispatcher))
            stack.enter_context(patch('martin_agentic.host_execution.HostExecutionBridge', IdleDispatcher))
            self.read = stack.enter_context(patch(prefix+'_broker_read', side_effect=read or (
                lambda *a, **kw: full_snapshot(self.now.isoformat()))))
            self.market = stack.enter_context(patch(prefix+'collect_live_events', side_effect=collect))
            self.disposition = stack.enter_context(patch(prefix+'record_capture_disposition'))
            stack.enter_context(patch(prefix+'wait_for_result_or_tick', side_effect=wait))
            stack.enter_context(patch(prefix+'notify_local'))
            stack.enter_context(patch(prefix+'print'))
            self.supervisor = supervisor
            return run_live_session(Path(folder)/'book.sqlite', policy,
                symbols=[str(i) for i in range(10)], cycles=cycles, poll_seconds=5,
                agent_mode=mode, status_file=Path(folder)/'status.json',
                output_dir=Path(folder)/'output', clock=lambda: self.now,
                budget_entry_limit=0, budget_total_limit=0)

    def test_real_book_queue_initialization_requires_and_leaves_idle_transaction(self):
        from martin_agentic.execution import ExecutionBook
        from martin_luk.live_session import LiveSupervisor
        policy = json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        with tempfile.TemporaryDirectory() as folder:
            book = ExecutionBook(Path(folder) / 'book.sqlite', policy,
                                 simulation=True, clock=lambda: self.now)
            try:
                self.assertFalse(book.db.in_transaction)
                supervisor = LiveSupervisor(book, process_strategy=True)
                try:
                    self.assertFalse(book.db.in_transaction)
                    self.assertTrue(supervisor.strategy_process_enabled)
                finally:
                    supervisor.close()
            finally:
                book.close()

    def test_next_market_read_overlaps_next_account_read_with_detached_committed_state(self):
        for mode in ('disabled', 'script-review', 'supervised-host'):
            with self.subTest(mode=mode):
                self.state = {'revision': 0}; self.consumed = []
                market_started = threading.Event(); account_started = threading.Event()
                read_count = []; market_states = []; overlapped = []
                def read(*args, **kwargs):
                    read_count.append(1)
                    if len(read_count) == 2:
                        overlapped.append(market_started.wait(1))
                        account_started.set()
                    return full_snapshot(self.now.isoformat())
                def collect(signals, **kwargs):
                    self.assertIsInstance(signals, FrozenSignals)
                    market_states.append(signals.state())
                    if len(market_states) == 2:
                        market_started.set()
                        self.assertTrue(account_started.wait(2))
                    return [self.event('batch-'+str(len(market_states)))], {'quotes': 10}
                result = self.session(collect, read=read, mode=mode)
                self.assertEqual(overlapped, [True])
                self.assertEqual(market_states[1]['revision'], 1)
                self.assertEqual(set(self.state_reader_threads), {self.owner})
                self.assertEqual(result['model_budget']['total_calls_reserved'], 0)

    def test_completed_prefetch_waits_for_fresh_account_and_is_archived_on_bounded_stop(self):
        release = threading.Event(); blocked = threading.Event(); threads = []; reads = []
        def read(*args, **kwargs):
            reads.append(1)
            if len(reads) == 2:
                threads.append(threading.current_thread()); blocked.set(); release.wait(2)
            return full_snapshot(self.now.isoformat())
        batches = []
        def collect(*args, **kwargs):
            batches.append(1)
            return [self.event('batch-'+str(len(batches)))], {'quotes': 10}
        try:
            result = self.session(collect, read=read, cycles=4)
            self.assertTrue(blocked.is_set())
            self.assertEqual(len(batches), 2)
            self.assertEqual([e['id'] for e in self.consumed], ['batch-1'])
            self.assertEqual(result['reason'], 'BROKER_COLLECTION_IN_FLIGHT')
            self.assertEqual(result['market_shutdown_events_archived'], 1)
            self.assertTrue(any(c.kwargs.get('reason') == 'SESSION_STOPPED_BEFORE_CONSUMPTION'
                                for c in self.disposition.call_args_list))
        finally:
            release.set()
            for thread in threads:
                thread.join(timeout=1)

    def test_delayed_account_does_not_make_a_prefetched_quote_fresh(self):
        original = self.now.isoformat()
        quote = {'id': 'prefetched-quote', 'kind': 'quote', 'at': original,
                 'data': {'symbol': 'DEMO', 'bid_at': original, 'ask_at': original,
                          'observed_at': original, 'bid': '10', 'ask': '10.01'}}
        reads = []; batches = []
        def read(*args, **kwargs):
            reads.append(1)
            if len(reads) == 2:
                self.now += timedelta(seconds=20)
            return full_snapshot(self.now.isoformat())
        def collect(*args, **kwargs):
            batches.append(1)
            return ([self.event('first')] if len(batches) == 1 else [quote]), {'quotes': 10}
        result = self.session(collect, read=read, cycles=2)
        self.assertEqual([e['id'] for e in self.consumed], ['first'])
        self.assertEqual(result['market_failure']['code'], 'MARKET_QUOTE_STALE')
        self.assertTrue(any(c.args[1]['id'] == 'prefetched-quote'
                            and c.kwargs.get('reason') == 'MARKET_QUOTE_STALE'
                            for c in self.disposition.call_args_list))

    def test_prefetched_auth_failure_is_not_retried_and_account_management_continues(self):
        batches = []
        def collect(*args, **kwargs):
            batches.append(1)
            if len(batches) == 2:
                raise TransportError('INTERACTIVE_AUTH_OR_APPROVAL_REQUIRED')
            return [self.event('first')], {'quotes': 10}
        result = self.session(collect, cycles=5)
        self.assertEqual(len(batches), 2)
        self.assertEqual(self.read.call_count, 5)
        self.assertEqual(self.supervisor.step.call_count, 5)
        self.assertEqual(result['market_failure']['code'], 'INTERACTIVE_AUTH_OR_APPROVAL_REQUIRED')
        self.assertEqual([e['id'] for e in self.consumed], ['first'])

    def test_step_crossing_close_does_not_start_another_market_read(self):
        def step(events):
            if events:
                self.now = datetime.fromisoformat('2026-10-16T20:00:00+00:00')
        result = self.session(lambda *a, **k: ([self.event('first')], {'quotes': 10}),
                              on_step=step, cycles=2)
        self.assertEqual(self.market.call_count, 1)
        self.assertEqual(result['reason'], 'AUTHORIZED_SESSION_CLOSED')
        self.assertFalse(result['final_flat_verified'])
        self.assertTrue(result['supported_scope_flat'])

    def test_last_bounded_tick_does_not_start_unused_prefetch(self):
        self.session(lambda *a, **k: ([self.event('last')], {'quotes': 10}), cycles=1)
        self.assertEqual(self.market.call_count, 1)

    def test_shutdown_reaps_the_actual_read_process_started_by_prefetch(self):
        import sys
        from martin_agentic.control_tasks import owned_read_transport
        from martin_agentic.transport import StdioRpc
        children = []; batches = []
        class Transport:
            def __init__(self):
                self.rpc = StdioRpc([sys.executable, '-u', '-c', 'import time;time.sleep(30)'], timeout=5)
                children.append(self.rpc)
            def __enter__(self):
                return self
            def __exit__(self, *args):
                self.rpc.close()
        def collect(*args, **kwargs):
            batches.append(1)
            if len(batches) > 1:
                with owned_read_transport(Transport, kwargs['cancellation']) as transport:
                    transport.rpc.request('blocked', {})
            return [self.event('first')], {'quotes': 10}
        try:
            result = self.session(collect, cycles=2)
            self.assertEqual(len(children), 1)
            self.assertIsNotNone(children[0].process.poll())
            self.assertFalse(result['market_read_cleanup']['thread_alive'])
            self.assertFalse(result.get('market_read_cleanup_incomplete', False))
            self.assertEqual(result['model_budget']['total_calls_reserved'], 0)
        finally:
            for child in children:
                child.close()

    def test_failed_initial_account_read_never_starts_market_collection(self):
        def unavailable(*args, **kwargs):
            raise TransportError('INTERACTIVE_AUTH_OR_APPROVAL_REQUIRED')
        self.session(lambda *a, **k: ([], {}), read=unavailable)
        self.market.assert_not_called()

    def test_model_worker_mode_keeps_original_serial_read_schedule(self):
        states = []; counts = []
        def read(*args, **kwargs):
            counts.append(len(states))
            return full_snapshot(self.now.isoformat())
        def collect(*args, **kwargs):
            states.append(1)
            return [self.event(str(len(states)))], {'quotes': 10}
        self.session(collect, read=read, mode='review', cycles=2)
        self.assertEqual(counts, [0, 1])
