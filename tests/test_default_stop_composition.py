"""Default run_live_session stop while the real collector/bridge read is in flight."""
from copy import deepcopy
from datetime import timedelta
import io
import json
import os
from pathlib import Path
import tempfile
import threading
from unittest import TestCase
from unittest.mock import patch
from contextlib import redirect_stdout

from martin_agentic.account_facts_worker import AccountFactsWorker
from martin_agentic.audit import digest
from martin_agentic.capture_publisher import CapturePublisher
from martin_agentic.current_context import CurrentContextRegistry
from martin_agentic.engineering_preflight import CurrentEngineeringPreflight
from martin_luk.live_session import run_live_session
from martin_agentic.qualification import source_fingerprint
import martin_agentic.live_session as live_session
from test_broker_bundle import FakeReadTransport
from test_capture_publisher import ACCOUNT_KEY, T as CAPTURE_T
from test_current_evidence import PublicFixture
from test_engineering_preflight import POLICY
from test_live_capture_bridge import FakeCodexBrokerRead
import sys


class DefaultStopCompositionTests(TestCase):
    def test_stop_during_default_collector_read_does_not_publish_or_enter(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        public = PublicFixture(root / 'public')
        policy = deepcopy(POLICY)
        now = [CAPTURE_T]
        publisher = CapturePublisher(
            root / 'captures', public.manifest, policy=policy,
            account_key=ACCOUNT_KEY, account_suffix='0000',
            source_fingerprint_value=source_fingerprint(),
            clock=lambda: now[0])
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   public.policy):
            published = publisher.produce_and_publish(
                FakeReadTransport(), timeout_seconds=5)
        self.assertEqual(published['status'], 'PUBLISHED')
        policy['current_capture'] = {
            'root': str(root / 'captures'),
            'public_manifest': str(public.manifest)}
        preflight = CurrentEngineeringPreflight(
            published['capture_root'], public.manifest,
            account_key=ACCOUNT_KEY, account_suffix='0000',
            policy_sha256=digest(policy),
            source_fingerprint_value=source_fingerprint(),
            clock=lambda: now[0],
            expected_capture_generation=published['generation_id'])
        entered = threading.Event()
        release = threading.Event()
        stopped = threading.Event()
        calls = []
        def handler(transport, tool, args, index):
            calls.append(tool)
            if not entered.is_set():
                now[0] = CAPTURE_T + timedelta(hours=6, seconds=31)
                entered.set()
                release.wait(timeout=5)
        class BlockingBroker(FakeCodexBrokerRead):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.inner.handler = handler
        result_box = {}
        def run():
            with patch.object(live_session, 'CodexBrokerReadTransport',
                              BlockingBroker), \
                 patch.object(live_session, '_drain_read_task',
                              side_effect=lambda *a, **k: (
                                  stopped.set(),
                                  original_drain(*a, **k))[1]), \
                 patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                       public.policy), \
                 redirect_stdout(io.StringIO()):
                result_box['result'] = run_live_session(
                    root / 'book.sqlite', policy,
                    symbols=['DEMO'] + [str(index) for index in range(9)],
                    cycles=1, poll_seconds=5, agent_mode='disabled',
                    simulation=False, clock=lambda: now[0],
                    status_file=root / 'status.json',
                    output_dir=root / 'output', budget_entry_limit=0,
                    budget_total_limit=0, current_preflight=preflight,
                    until_close=True)
        original_drain = live_session._drain_read_task
        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        try:
            self.assertTrue(entered.wait(timeout=5))
            self.assertTrue(stopped.wait(timeout=10))
        finally:
            release.set()
            thread.join(timeout=10)
        self.assertFalse(thread.is_alive())
        self.assertIn('get_accounts', calls)
        result = result_box['result']
        self.assertEqual(result['stopped'], True)
        self.assertEqual(result['reason'], 'AUTHORIZED_SESSION_CLOSED')
        self.assertIs(result['broker_read_cleanup']['drained'], True)
        self.assertIsNot(result.get('account_facts', {}).get(
            'entry_authorized'), True)
        registry = CurrentContextRegistry(
            Path(policy['current_capture']['root']) / 'registry.json',
            allowed_capture_roots=(
                Path(policy['current_capture']['root']),),
            allowed_public_roots=(public.root,), clock=lambda: CAPTURE_T)
        self.assertEqual(registry.read()['generation_id'],
                         published['generation_id'])

    def test_default_runner_stops_real_facts_child_in_finally(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        public = PublicFixture(root / 'public')
        policy = deepcopy(POLICY)
        now = [CAPTURE_T]
        policy['current_capture'] = {
            'root': str(root / 'captures'),
            'public_manifest': str(public.manifest)}
        publisher = CapturePublisher(
            root / 'captures', public.manifest, policy=policy,
            account_key=ACCOUNT_KEY, account_suffix='0000',
            source_fingerprint_value=source_fingerprint(),
            clock=lambda: now[0])
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   public.policy):
            published = publisher.produce_and_publish(
                FakeReadTransport(), timeout_seconds=5)
        self.assertEqual(published['status'], 'PUBLISHED')
        preflight = CurrentEngineeringPreflight(
            published['capture_root'], public.manifest,
            account_key=ACCOUNT_KEY, account_suffix='0000',
            policy_sha256=digest(policy),
            source_fingerprint_value=source_fingerprint(),
            clock=lambda: now[0],
            expected_capture_generation=published['generation_id'])
        class DifferentBroker(FakeCodexBrokerRead):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                def marker(transport, tool, args, index):
                    if tool == 'get_equity_orders' and index == 0:
                        return {'data': {'orders': [],
                                         'runner_generation_marker': 'b'}}
                self.inner.handler = marker
        started = threading.Event()
        proceed = threading.Event()
        observed = []
        class SlowWorker(AccountFactsWorker):
            def __init__(self, *args, **kwargs):
                super().__init__(
                    command=[sys.executable, '-c',
                             'import time; time.sleep(30)'],
                    timeout_seconds=30.0)
                observed.append(self)
            def start(self, request):
                result = super().start(request)
                self.process_ref = self._process
                started.set()
                proceed.wait(timeout=10)
                return result
        result_box = {}
        def fake_market(*args, **kwargs):
            return ([], {'quotes': 0, 'source': 'stop-facts-test'})
        failure = []
        def run():
            try:
                with (
                    patch.object(live_session, 'CodexBrokerReadTransport',
                                 DifferentBroker),
                    patch.object(live_session, 'AccountFactsWorker', SlowWorker),
                    patch.object(live_session, 'collect_live_events',
                                 fake_market),
                    patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                          public.policy),
                    redirect_stdout(io.StringIO()),
                ):
                    result_box['result'] = run_live_session(
                        root / 'book.sqlite', policy,
                        symbols=['DEMO'] + [str(index) for index in range(9)],
                        cycles=1, poll_seconds=5, agent_mode='disabled',
                        simulation=False, clock=lambda: now[0],
                        status_file=root / 'status.json',
                        output_dir=root / 'output', budget_entry_limit=0,
                        budget_total_limit=0, current_preflight=preflight,
                        until_close=True)
            except BaseException:
                import traceback
                failure.append(traceback.format_exc())
        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        try:
            self.assertTrue(started.wait(timeout=5), failure)
            self.assertEqual(len(observed), 1)
            worker = observed[0]
            self.assertIs(worker.status()['inflight'], True)
            self.assertIs(worker.status()['cleanup_pending'], False)
            self.assertIsNotNone(getattr(worker, 'process_ref', None))
            self.assertNotEqual(worker.process_ref.pid, os.getpid())
            self.assertIsNone(worker.process_ref.poll())
            # Owner-supported close handoff; no manual supervisor.close().
            now[0] = CAPTURE_T + timedelta(hours=6)
        finally:
            proceed.set()
        thread.join(timeout=20)
        self.assertFalse(thread.is_alive())
        self.assertEqual(failure, [])
        result = result_box['result']
        self.assertEqual(result.get('stopped'), True)
        self.assertEqual(result['reason'], 'AUTHORIZED_SESSION_CLOSED')
        self.assertIs(worker.status()['inflight'], False)
        self.assertIs(worker.status()['cleanup_pending'], False)
        self.assertIsNotNone(worker.process_ref.poll())
        self.assertIsNone(worker.process_ref.stdin)
        self.assertIsNone(worker.process_ref.stdout)
        self.assertIsNone(worker.process_ref.stderr)
        self.assertIsNone(worker._process)
        self.assertIsNone(worker._directory)
        self.assertIsNone(worker._request_path)
        self.assertIsNone(worker._response_path)
        self.assertIn(worker.status()['state'], {'cancelled', 'closed'})
        self.assertIsNot(result.get('account_facts', {}).get(
            'entry_authorized'), True)
        self.assertIs(worker.status()['broker_writes'], 0)
        self.assertIs(worker.status()['book_mutations'], 0)
        lifecycle = result.get('lifecycle') or {}
        lifecycle_state = lifecycle.get('state') or {}
        self.assertEqual(result.get('actions', []), [])
        self.assertEqual(lifecycle_state.get('trades', {}), {})
        self.assertEqual(lifecycle.get('effective_ready_actions', 0), 0)
        self.assertEqual(result.get('live_orders_by_script', 0), 0)
        self.assertIsNot(result.get('dispatch', {}).get('agent_called'), True)
        self.assertIsNot(result.get('host_delivery_status', {}).get(
            'available'), True)
