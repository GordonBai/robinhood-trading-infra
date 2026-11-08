"""Default read-only capture chain across two owner-published generations."""
from copy import deepcopy
from datetime import timedelta
import hashlib
from pathlib import Path
import shutil
import tempfile
import time
from unittest import TestCase
from unittest.mock import patch

from martin_agentic.broker import BrokerJournal
from martin_agentic.capture_publisher import CapturePublisher
from martin_agentic.current_context import CurrentContextRegistry
from martin_agentic.current_evidence import capture_generation
from martin_agentic.execution import ExecutionBook
from martin_agentic.live_capture_bridge import (
    SavedBundleObservationBridge, journal_association_projection)
from martin_luk.live_session import (
    LiveSupervisor, publish_owner_observation)
from test_broker_bundle import FakeReadTransport
from test_capture_publisher import T as CAPTURE_T
from test_current_evidence import PublicFixture
from test_engineering_preflight import POLICY


class ChangingApprovalTransport(FakeReadTransport):
    def __init__(self, human_must_approve):
        super().__init__()
        self.human_must_approve = human_must_approve

    def __call__(self, tool, args):
        response = deepcopy(super().__call__(tool, args))
        if tool == 'get_trade_approval_setting':
            response['data']['setting']['human_must_approve_trades'] = (
                self.human_must_approve)
        return response


class MultiGenerationDefaultCaptureTests(TestCase):
    def test_two_generations_owner_ingest_factory_and_real_facts_worker(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        public = PublicFixture(root / 'public')
        now = [CAPTURE_T]
        policy = deepcopy(POLICY)
        policy['current_capture'] = {
            'root': str(root / 'captures'),
            'public_manifest': str(public.manifest)}
        book = ExecutionBook(root / 'book.sqlite', policy,
                             simulation=False, clock=lambda: now[0])
        self.addCleanup(book.close)
        journal = BrokerJournal(book.db_path)
        self.addCleanup(journal.close)
        bridge = SavedBundleObservationBridge(policy, lambda: now[0])
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   public.policy):
            first = bridge.read(ChangingApprovalTransport(False),
                                deadline_monotonic=time.monotonic() + 10,
                                timeout_seconds=10)
            first_capture = capture_generation(
                first['live_capture']['bundle_root'])
            journal.ingest(first)
            published_first = publish_owner_observation(
                book, journal, policy, lambda: now[0], first)
            self.assertEqual(published_first['status'], 'PUBLISHED')
            first_generation = published_first['generation']
            self.assertEqual(first_generation,
                             first_capture['generation'])
            supervisor = LiveSupervisor(book)
            self.addCleanup(supervisor.close)

            def step_until(observed):
                for _ in range(300):
                    result = supervisor.step(
                        observed, [], allow_entries=False)
                    if result['account_facts']['status'] != 'inflight':
                        return result
                    time.sleep(.01)
                raise AssertionError('facts worker did not settle')

            first_facts = step_until(first)
            first_facts_value = first_facts['account_facts']
            self.assertEqual(first_facts_value['generation'],
                             first_generation)
            self.assertIs(first_facts_value['layers']['observation']['valid'],
                          True, first_facts_value)
            self.assertFalse(any(
                str(reason).startswith('CURRENT_JOURNAL_')
                for reason in first_facts_value['reasons']))
            self.assertGreaterEqual(
                first_facts_value['refresh']['consumed_count'], 1)
            self.assertGreaterEqual(
                first_facts_value['refresh']['worker_starts'], 1)
            self.assertEqual(first_facts_value['worker']['state'], 'completed')
            self.assertIs(first_facts_value['worker']['read_only_child'], True)
            self.assertIs(first_facts_value['entry_authorized'], False)
            first_consumed = first_facts_value['refresh']['consumed_count']

            now[0] = CAPTURE_T + timedelta(seconds=1)
            second = bridge.read(ChangingApprovalTransport(True),
                                 deadline_monotonic=time.monotonic() + 10,
                                 timeout_seconds=10)
            second_capture = capture_generation(
                second['live_capture']['bundle_root'])
            self.assertNotEqual(second_capture['raw_chain_sha256'],
                                first_capture['raw_chain_sha256'])
            self.assertNotEqual(second['live_capture']['generation'],
                                first_generation)
            journal.ingest(second)
            published_second = publish_owner_observation(
                book, journal, policy, lambda: now[0], second)
            self.assertEqual(published_second['status'], 'PUBLISHED')
            second_generation = published_second['generation']
            self.assertEqual(second_generation,
                             second_capture['generation'])
            self.assertNotEqual(second_generation, first_generation)
            second_facts = step_until(second)
            second_facts_value = second_facts['account_facts']
            self.assertEqual(second_facts_value['generation'],
                             second_generation)
            self.assertIs(second_facts_value['layers']['observation']['valid'],
                          True, second_facts_value)
            self.assertFalse(any(
                str(reason).startswith('CURRENT_JOURNAL_')
                for reason in second_facts_value['reasons']))
            self.assertGreaterEqual(
                second_facts_value['refresh']['consumed_count'],
                first_consumed + 1)
            self.assertGreaterEqual(
                second_facts_value['refresh']['worker_starts'], 2)
            self.assertEqual(second_facts_value['worker']['state'], 'completed')
            self.assertIs(second_facts_value['worker']['read_only_child'], True)
            self.assertIs(second_facts_value['entry_authorized'], False)
            registry = CurrentContextRegistry(
                Path(policy['current_capture']['root']) / 'registry.json',
                allowed_capture_roots=(
                    Path(policy['current_capture']['root']),),
                allowed_public_roots=(public.root,), clock=lambda: now[0])
            self.assertEqual(registry.read()['generation_id'],
                             second_generation)

    def test_other_snapshot_in_journal_blocks_observation_then_restores(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        public = PublicFixture(root / 'public')
        now = [CAPTURE_T]
        policy = deepcopy(POLICY)
        policy['current_capture'] = {
            'root': str(root / 'captures'),
            'public_manifest': str(public.manifest)}
        book = ExecutionBook(root / 'book.sqlite', policy,
                             simulation=False, clock=lambda: now[0])
        self.addCleanup(book.close)
        journal = BrokerJournal(book.db_path)
        self.addCleanup(journal.close)
        bridge = SavedBundleObservationBridge(policy, lambda: now[0])
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   public.policy):
            good = bridge.read(ChangingApprovalTransport(False),
                               deadline_monotonic=time.monotonic() + 10,
                               timeout_seconds=10)
            journal.ingest(good)
            published = publish_owner_observation(
                book, journal, policy, lambda: now[0], good)
            self.assertEqual(published['status'], 'PUBLISHED')
            generation = published['generation']
            supervisor = LiveSupervisor(book)
            self.addCleanup(supervisor.close)

            def step_until(snapshot_value):
                for _ in range(300):
                    result = supervisor.step(
                        snapshot_value, [], allow_entries=False)
                    if result['account_facts']['status'] != 'inflight':
                        return result
                    time.sleep(.01)
                raise AssertionError('facts worker did not settle')

            first = step_until(good)['account_facts']
            self.assertIs(first['layers']['observation']['valid'], True,
                          first)
            other = bridge.read(ChangingApprovalTransport(True),
                                deadline_monotonic=time.monotonic() + 10,
                                timeout_seconds=10)
            journal.ingest(other)
            blocked = step_until(good)['account_facts']
            self.assertIs(blocked['layers']['observation']['valid'], False,
                          blocked)
            self.assertTrue(any(
                str(reason).startswith('CURRENT_JOURNAL_')
                for reason in blocked['reasons']), blocked)
            self.assertIs(blocked['entry_authorized'], False)
            self.assertEqual(blocked['generation'], generation)
            journal.ingest(good)
            restored = step_until(good)['account_facts']
            self.assertIs(restored['layers']['observation']['valid'], True,
                          restored)
            self.assertFalse(any(
                str(reason).startswith('CURRENT_JOURNAL_')
                for reason in restored['reasons']))
            self.assertEqual(restored['generation'], generation)
            registry = CurrentContextRegistry(
                Path(policy['current_capture']['root']) / 'registry.json',
                allowed_capture_roots=(
                    Path(policy['current_capture']['root']),),
                allowed_public_roots=(public.root,), clock=lambda: now[0])
            self.assertEqual(registry.read()['generation_id'], generation)

    def test_old_generation_publish_cannot_replace_current_head(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        public = PublicFixture(root / 'public')
        now = [CAPTURE_T]
        policy = deepcopy(POLICY)
        policy['current_capture'] = {
            'root': str(root / 'captures'),
            'public_manifest': str(public.manifest)}
        book = ExecutionBook(root / 'book.sqlite', policy,
                             simulation=False, clock=lambda: now[0])
        self.addCleanup(book.close)
        journal = BrokerJournal(book.db_path)
        self.addCleanup(journal.close)
        bridge = SavedBundleObservationBridge(policy, lambda: now[0])
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   public.policy):
            first = bridge.read(ChangingApprovalTransport(False),
                                deadline_monotonic=time.monotonic() + 10,
                                timeout_seconds=10)
            retained = root / 'retained-first'
            shutil.copytree(first['live_capture']['bundle_root'], retained)
            before_bytes = {
                str(path.relative_to(retained)): hashlib.sha256(
                    path.read_bytes()).hexdigest()
                for path in retained.rglob('*') if path.is_file()}
            journal.ingest(first)
            first_projection = journal_association_projection(
                journal.current_observation())
            first_published = publish_owner_observation(
                book, journal, policy, lambda: now[0], first)
            self.assertEqual(first_published['status'], 'PUBLISHED')
            first_generation = first_published['generation']
            now[0] = CAPTURE_T + timedelta(seconds=1)
            second = bridge.read(ChangingApprovalTransport(True),
                                 deadline_monotonic=time.monotonic() + 10,
                                 timeout_seconds=10)
            journal.ingest(second)
            second_published = publish_owner_observation(
                book, journal, policy, lambda: now[0], second)
            self.assertEqual(second_published['status'], 'PUBLISHED')
            second_generation = second_published['generation']
            self.assertNotEqual(second_generation, first_generation)
            registry = CurrentContextRegistry(
                Path(policy['current_capture']['root']) / 'registry.json',
                allowed_capture_roots=(
                    Path(policy['current_capture']['root']),),
                allowed_public_roots=(public.root,), clock=lambda: now[0])
            self.assertEqual(registry.read()['generation_id'],
                             second_generation)
            old_snapshot = deepcopy(first)
            old_snapshot['live_capture']['bundle_root'] = str(retained)
            old = bridge.publish(
                old_snapshot, journal_snapshot=first_projection)
            self.assertEqual(old['status'], 'REJECTED')
            self.assertEqual(old['error'], 'PUBLICATION_STALE_ORPHAN')
            self.assertFalse(old['current_evidence_valid'])
            self.assertFalse(old['published'])
            self.assertEqual(registry.read()['generation_id'],
                             second_generation)
            after_bytes = {
                str(path.relative_to(retained)): hashlib.sha256(
                    path.read_bytes()).hexdigest()
                for path in retained.rglob('*') if path.is_file()}
            self.assertEqual(after_bytes, before_bytes)
