"""Owner-loop protection continues while isolated alpha work is pending/faulted."""
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

import test_execution as execution_fixture
from martin_luk.live_session import LiveSupervisor
from martin_luk.live_signals import LiveSignals


class FakeStrategyQueue:
    def __init__(self, signals, *, advance=None):
        self.signals = signals
        self.events = []
        self.advance_callback = advance

    def enqueue(self, event):
        self.events.append(event)

    def advance(self):
        if self.advance_callback is not None:
            return self.advance_callback()
        return {'errors': [], 'busy': True, 'completed': 0}

    def status(self):
        return {'busy': True, 'pending': len(self.events)}

    def close(self):
        return {'closed': True}


class AsyncStrategySupervisorTests(TestCase):
    def setUp(self):
        self.fixture = execution_fixture.ExecutionTests(methodName='runTest')
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.book = self.fixture.book
        self.signals = LiveSignals(self.book)
        self.trace = []
        self.dispatches = []

    def event(self):
        return {'id': 'async-quote', 'kind': 'quote',
                'at': self.fixture.now.isoformat(), 'data': self.fixture.quote,
                'source_sha256': 'synthetic-async-fixture', 'synthetic_input': True}

    def dispatcher(self):
        owner = self

        class Dispatcher:
            requires_observation = True

            def __call__(self, *, snapshot, quotes, allow_entries):
                owner.trace.append('management_dispatch')
                owner.dispatches.append((snapshot, quotes, allow_entries))
                return {'agent_called': False, 'broker_writes': 0}

        return Dispatcher()

    def reconcile(self, *args, **kwargs):
        self.trace.append('reconcile')
        self.observed_quotes = kwargs['quotes']
        return self.original_reconcile(*args, **kwargs)

    def test_pending_worker_is_polled_after_reconcile_and_protection_dispatch(self):
        buy = self.fixture.entry_filled()
        snapshot = self.fixture.snap([buy], qty='2')

        def advance():
            self.assertEqual(self.trace, ['reconcile', 'management_dispatch'])
            self.assertEqual(self.dispatches[0][1]['DEMO'], self.fixture.quote)
            self.assertFalse(self.dispatches[0][2])
            self.assertTrue(any(a['kind'] == 'protect' and a['state'] == 'READY'
                                for a in self.book.actions()))
            self.trace.append('strategy_poll')
            return {'errors': [], 'busy': True, 'completed': 0}

        queue = FakeStrategyQueue(self.signals, advance=advance)
        supervisor = LiveSupervisor(self.book, strategy_queue=queue, dispatcher=self.dispatcher())
        self.original_reconcile = self.book.reconcile
        with patch.object(self.book, 'reconcile', side_effect=self.reconcile), \
                patch.object(self.signals, 'consume', side_effect=AssertionError('inline alpha evaluation')):
            result = supervisor.step(snapshot, [self.event()])
        self.assertEqual(self.trace, ['reconcile', 'management_dispatch', 'strategy_poll'])
        self.assertTrue(result['strategy_queue']['busy'])
        self.assertEqual(len(self.dispatches), 1)
        self.assertEqual(queue.events, [self.event()])
        self.assertEqual(result['live_orders_by_script'], 0)

    def test_queue_failure_preserves_fresh_management_quotes_and_blocks_entries(self):
        buy = self.fixture.entry_filled()
        snapshot = self.fixture.snap([buy], qty='2')

        def fail():
            self.assertEqual(self.trace, ['reconcile', 'management_dispatch'])
            raise RuntimeError('untrusted worker details must not escape')

        queue = FakeStrategyQueue(self.signals, advance=fail)
        supervisor = LiveSupervisor(self.book, strategy_queue=queue, dispatcher=self.dispatcher())
        self.original_reconcile = self.book.reconcile
        with patch.object(self.book, 'reconcile', side_effect=self.reconcile), \
                patch.object(self.book, 'propose', side_effect=AssertionError('new entry on queue failure')):
            result = supervisor.step(snapshot, [self.event()])
        self.assertEqual(self.observed_quotes, {'DEMO': self.fixture.quote})
        self.assertEqual(self.dispatches[0][1], self.observed_quotes)
        self.assertEqual(len(self.dispatches), 1)
        self.assertTrue(result['new_entries_blocked'])
        self.assertEqual(result['errors'], [{'reason': 'STRATEGY_QUEUE_ADVANCE_FAILED', 'scope': 'STRATEGY_INPUT'}])
        self.assertNotIn('untrusted worker details', str(result))

    def test_reported_worker_timeout_blocks_entries_without_suppressing_management(self):
        buy = self.fixture.entry_filled()
        queue = FakeStrategyQueue(self.signals, advance=lambda: {
            'errors': [{'reason': 'STRATEGY_PROCESS_TIMEOUT'}], 'busy': False, 'completed': 0})
        supervisor = LiveSupervisor(self.book, strategy_queue=queue, dispatcher=self.dispatcher())
        with patch.object(self.book, 'propose', side_effect=AssertionError('new entry after timeout')):
            result = supervisor.step(self.fixture.snap([buy], qty='2'), [self.event()])
        self.assertTrue(result['new_entries_blocked'])
        self.assertEqual(result['errors'][0]['reason'], 'STRATEGY_PROCESS_TIMEOUT')
        self.assertEqual(self.dispatches[0][1], {'DEMO': self.fixture.quote})
        self.assertFalse(self.dispatches[0][2])

    def test_dispatch_disabled_reconciles_and_polls_without_calling_dispatcher(self):
        buy = self.fixture.entry_filled()
        queue = FakeStrategyQueue(self.signals)
        supervisor = LiveSupervisor(self.book, strategy_queue=queue, dispatcher=self.dispatcher())
        self.original_reconcile = self.book.reconcile
        with patch.object(self.book, 'reconcile', side_effect=self.reconcile):
            result = supervisor.step(self.fixture.snap([buy], qty='2'), [self.event()],
                                     allow_entries=False, allow_dispatch=False)
        self.assertEqual(self.trace, ['reconcile'])
        self.assertEqual(self.observed_quotes, {'DEMO': self.fixture.quote})
        self.assertEqual(self.dispatches, [])
        self.assertTrue(result['strategy_queue']['busy'])
        self.assertEqual(result['dispatch']['reason'], 'OUTSIDE_REGULAR_SESSION_READ_ONLY_MANAGEMENT')

    def test_queue_bound_to_another_book_is_rejected(self):
        queue = FakeStrategyQueue(SimpleNamespace(book=object()))
        with self.assertRaisesRegex(ValueError, '^STRATEGY_QUEUE_OWNER_BOOK_MISMATCH$'):
            LiveSupervisor(self.book, strategy_queue=queue)

    def test_integrity_fault_during_advisory_poll_inhibits_immediately_and_survives_next_step(self):
        reports = iter([
            {'errors': [{'reason': 'CONSUMED_BAR_REVISED'}], 'busy': False},
            {'errors': [], 'busy': False},
        ])
        queue = FakeStrategyQueue(self.signals, advance=lambda: next(reports))
        supervisor = LiveSupervisor(self.book, strategy_queue=queue, dispatcher=self.dispatcher())
        with patch('martin_agentic.watchdog.inhibit_new_entries') as inhibit, \
                patch.object(self.book, 'propose', side_effect=AssertionError('entry after wait integrity fault')):
            report = supervisor.advance_strategy()
            self.assertEqual(inhibit.call_count, 1)
            self.assertEqual(report['undelivered_error_count'], 1)
            self.assertEqual(self.dispatches, [])
            result = supervisor.step(self.fixture.snap(), [self.event()])
        self.assertTrue(result['new_entries_blocked'])
        self.assertEqual([e['reason'] for e in result['errors']], ['CONSUMED_BAR_REVISED'])
        self.assertEqual(result['strategy_queue']['errors'], [])
        self.assertFalse(self.dispatches[0][2])
        self.assertEqual(supervisor._strategy_pending_errors, [])

    def test_nonintegrity_fault_during_advisory_poll_blocks_next_fresh_step(self):
        reports = iter([
            {'errors': [{'reason': 'STRATEGY_PROCESS_TIMEOUT'}], 'busy': False},
            {'errors': [], 'busy': False},
        ])
        queue = FakeStrategyQueue(self.signals, advance=lambda: next(reports))
        supervisor = LiveSupervisor(self.book, strategy_queue=queue, dispatcher=self.dispatcher())
        with patch('martin_agentic.watchdog.inhibit_new_entries') as inhibit, \
                patch.object(self.book, 'propose', side_effect=AssertionError('entry after wait timeout')):
            supervisor.advance_strategy()
            inhibit.assert_not_called()
            result = supervisor.step(self.fixture.snap(), [self.event()])
            inhibit.assert_not_called()
        self.assertTrue(result['new_entries_blocked'])
        self.assertEqual([e['reason'] for e in result['errors']], ['STRATEGY_PROCESS_TIMEOUT'])
        self.assertEqual(self.dispatches[0][1], {'DEMO': self.fixture.quote})
        self.assertFalse(self.dispatches[0][2])
