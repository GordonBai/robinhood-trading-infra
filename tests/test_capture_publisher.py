"""Owner producer, atomic publication, and factory refresh lifecycle."""
from copy import deepcopy
from datetime import datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import tempfile
import threading
from unittest import TestCase
from unittest.mock import patch

from martin_agentic.assessment_receipt import build_receipt
from martin_agentic.audit import canonical
from martin_agentic.broker import BrokerJournal
from martin_agentic.capture_publisher import CapturePublisher
from martin_agentic.current_context import (
    CurrentContextFactory, CurrentContextRegistry, MissingCurrentContext,
    ensure_default_preflight)
from martin_agentic.current_evidence import capture_generation
from martin_agentic.engineering_preflight import CurrentEngineeringPreflight
from martin_agentic.execution import ExecutionBook
from martin_agentic.host_execution import HostExecutionBridge
from martin_luk.live_session import LiveSupervisor
from martin_agentic.qualification import source_fingerprint
from test_baseline_evidence import AT
from test_broker import FakeBroker, snapshot
from test_broker_bundle import FakeReadTransport
from test_current_evidence import PublicFixture
from test_engineering_preflight import POLICY


T = datetime.fromisoformat(AT)
ACCOUNT = 'SIM-0000'
ACCOUNT_KEY = hashlib.sha256(ACCOUNT.encode()).hexdigest()


class CashTransport(FakeReadTransport):
    def __init__(self, cash='500', **kwargs):
        self.cash = cash
        super().__init__(**kwargs)

    def __call__(self, tool, args):
        response = deepcopy(super().__call__(tool, args))
        if tool == 'get_portfolio':
            response['data']['cash'] = self.cash
        return response


class TwoPassMismatchTransport(FakeReadTransport):
    def __call__(self, tool, args):
        response = super().__call__(tool, args)
        if tool == 'get_equity_positions' and self.counts[tool] == 2:
            response = {'data': {'positions': [{
                'symbol': 'DEMO', 'quantity': '1',
                'shares_available_for_sells': '1',
                'shares_held_for_sells': '0',
                'shares_held_for_asset_transfer': '0',
                'shares_held_for_options_events': '0',
                'shares_held_for_stock_grants': '0',
                'shares_pending_from_options_events': '0',
            }]}}
        return response


class CursorLoopTransport(FakeReadTransport):
    def __call__(self, tool, args):
        if tool == 'get_equity_orders':
            return {'data': {'orders': [], 'next': 'loop'}}
        return super().__call__(tool, args)


class OwnerCapturePublisherTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.public = PublicFixture(self.root / 'public')
        self.now = [T]
        self.policy = deepcopy(POLICY)
        self.publisher = CapturePublisher(
            self.root / 'captures', self.public.manifest, policy=self.policy,
            account_key=ACCOUNT_KEY, account_suffix='0000',
            source_fingerprint_value=source_fingerprint(),
            clock=lambda: self.now[0])

    def compile_patch(self):
        return patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                     self.public.policy)

    def publish(self, transport=None, *, journal_snapshot=None,
                referenced_generations=()):
        with self.compile_patch():
            return self.publisher.produce_and_publish(
                transport or FakeReadTransport(), timeout_seconds=5,
                journal_snapshot=journal_snapshot,
                referenced_generations=referenced_generations)

    def registry(self):
        return CurrentContextRegistry(
            self.publisher.registry_path,
            allowed_capture_roots=(self.publisher.root,),
            allowed_public_roots=(self.public.root,),
            clock=lambda: self.now[0])

    def test_collector_publish_and_atomic_registry_are_verified(self):
        result = self.publish()
        self.assertEqual(result['status'], 'PUBLISHED', result)
        self.assertFalse(result['historical_orders_complete'])
        self.assertFalse(result['settlement_ledger_complete'])
        self.assertFalse(result['all_asset_flat_proven'])
        record = self.registry().read()
        self.assertEqual(record['generation_id'], result['generation_id'])
        self.assertEqual(record['account_key_hash'], ACCOUNT_KEY)
        self.assertEqual(record['policy_sha256'], self.publisher._policy_sha256())
        self.assertTrue((Path(result['capture_root']) / 'manifest.json').is_file())
        self.assertTrue((Path(result['capture_root']) / 'raw').is_dir())
        self.assertTrue((self.publisher.root / 'registry.json').is_file())
        (self.publisher.root / 'registry.json').read_bytes()

    def test_invalid_observations_and_source_accounts_are_not_published(self):
        before = self.publish()
        old_registry = self.publisher.registry_path.read_bytes()
        for cash_transport, expected in (
                (TwoPassMismatchTransport(), 'PUBLICATION_OBSERVATION_INVALID'),
                (CursorLoopTransport(), 'COLLECTOR_CURSOR_LOOP')):
            result = self.publish(cash_transport)
            self.assertEqual(result['status'], 'FAILED')
            self.assertEqual(self.publisher.registry_path.read_bytes(),
                             old_registry)
        wrong_source = CapturePublisher(
            self.root / 'wrong-source', self.public.manifest, policy=self.policy,
            account_key=ACCOUNT_KEY, account_suffix='0000',
            source_fingerprint_value='0' * 64, clock=lambda: self.now[0])
        with self.compile_patch():
            result = wrong_source.produce_and_publish(
                FakeReadTransport(), timeout_seconds=5)
        self.assertEqual(result['status'], 'FAILED')
        wrong_account = CapturePublisher(
            self.root / 'wrong-account', self.public.manifest, policy=self.policy,
            account_key='0' * 64, account_suffix='0000',
            source_fingerprint_value=source_fingerprint(),
            clock=lambda: self.now[0])
        with self.compile_patch():
            result = wrong_account.produce_and_publish(
                FakeReadTransport(), timeout_seconds=5)
        self.assertEqual(result['status'], 'FAILED')
        missing_public = CapturePublisher(
            self.root / 'missing-public', self.root / 'missing.json',
            policy=self.policy, account_key=ACCOUNT_KEY, account_suffix='0000',
            source_fingerprint_value=source_fingerprint(),
            clock=lambda: self.now[0])
        with self.compile_patch():
            result = missing_public.produce_and_publish(
                FakeReadTransport(), timeout_seconds=5)
        self.assertEqual(result['status'], 'FAILED')
        self.assertEqual(self.publisher.registry_path.read_bytes(), old_registry)

    def test_same_generation_retry_reports_current_validity_separately(self):
        first = self.publish()
        registry_before = self.publisher.registry_path.read_bytes()
        with self.compile_patch():
            fresh = self.publisher.publish_bundle(first['capture_root'])
        self.assertEqual(fresh['status'], 'ALREADY_PUBLISHED')
        self.assertTrue(fresh['observation_valid'])
        self.assertTrue(fresh['current_evidence_valid'])
        self.assertTrue(fresh['committed_at_valid'])
        self.assertEqual(fresh['published_at'], first['published_at'])

        self.now[0] = T + timedelta(seconds=29)
        original = self.publisher._publish_record
        def wait_then_record(commit):
            self.now[0] = T + timedelta(seconds=31)
            return original(commit)
        with self.compile_patch():
            with patch.object(self.publisher, '_publish_record',
                              side_effect=wait_then_record):
                stale = self.publisher.publish_bundle(first['capture_root'])
        self.assertEqual(stale['status'], 'ALREADY_PUBLISHED')
        self.assertTrue(stale['committed'])
        self.assertTrue(stale['committed_at_valid'])
        self.assertFalse(stale['observation_valid'])
        self.assertFalse(stale['current_evidence_valid'])
        self.assertIn('PUBLICATION_COMMIT_STALE', stale['current_reasons'])
        self.assertEqual(stale['published_at'], first['published_at'])
        self.assertEqual(self.publisher.registry_path.read_bytes(),
                         registry_before)
        with self.compile_patch():
            direct = self.publisher.publish_bundle(first['capture_root'])
        self.assertEqual(direct['status'], 'ALREADY_PUBLISHED')
        self.assertTrue(direct['committed'])
        self.assertTrue(direct['committed_at_valid'])
        self.assertFalse(direct['observation_valid'])
        self.assertEqual(direct['published_at'], first['published_at'])
        self.assertEqual(self.publisher.registry_path.read_bytes(),
                         registry_before)

    def test_final_validation_after_preparation_rejects_crossing(self):
        output = self.root / 'final-crossing'
        with self.compile_patch():
            from martin_agentic.broker_bundle import collect_saved_bundle
            collect_saved_bundle(
                output, account_suffix='0000', transport=FakeReadTransport(),
                timeout_seconds=5, clock=lambda: T)
            self.now[0] = T + timedelta(seconds=29)
            original = self.publisher._validate_commit
            calls = [0]
            def delayed(*args, **kwargs):
                value = original(*args, **kwargs)
                calls[0] += 1
                if calls[0] == 2:
                    self.now[0] = T + timedelta(seconds=31)
                return value
            with patch.object(self.publisher, '_validate_commit',
                              side_effect=delayed):
                result = self.publisher.publish_bundle(output)
        self.assertEqual(result['status'], 'REJECTED')
        self.assertFalse(result['committed'])
        self.assertFalse(result['observation_valid'])
        self.assertFalse(result['current_evidence_valid'])
        self.assertFalse(result['committed_at_valid'])
        self.assertIn('PUBLICATION_CURRENT_STALE',
                      result['current_reasons'])
        self.assertFalse(self.publisher.registry_path.exists())

    def test_exact_30_boundary_publishes_and_future_rejects(self):
        output = self.root / 'exact-30'
        with self.compile_patch():
            from martin_agentic.broker_bundle import collect_saved_bundle
            collect_saved_bundle(
                output, account_suffix='0000', transport=FakeReadTransport(),
                timeout_seconds=5, clock=lambda: T)
            self.now[0] = T + timedelta(seconds=30)
            result = self.publisher.publish_bundle(output)
        self.assertEqual(result['status'], 'PUBLISHED')
        self.assertTrue(result['observation_valid'])
        self.assertEqual(result['validation']['checked_at'],
                         (T + timedelta(seconds=30)).isoformat())
        future = CapturePublisher(
            self.root / 'future', self.public.manifest,
            policy=self.policy, account_key=ACCOUNT_KEY,
            account_suffix='0000', source_fingerprint_value=source_fingerprint(),
            clock=lambda: T - timedelta(seconds=1))
        early_public = PublicFixture(
            self.root / 'future-public',
            retrieved_at=(T - timedelta(seconds=2)).isoformat())
        future.public_manifest = early_public.manifest
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   early_public.policy):
            rejected = future.publish_bundle(output)
        self.assertEqual(rejected['status'], 'REJECTED')
        self.assertIn('PUBLICATION_CAPTURE_FUTURE', rejected['error'])

    def test_cleanup_crossing_expiry_is_committed_but_current_invalid(self):
        output = self.root / 'cleanup-crossing'
        with self.compile_patch():
            from martin_agentic.broker_bundle import collect_saved_bundle
            collect_saved_bundle(
                output, account_suffix='0000', transport=FakeReadTransport(),
                timeout_seconds=5, clock=lambda: T)
            self.now[0] = T + timedelta(seconds=29)
            original_prune = self.publisher._prune
            def slow_prune(*args, **kwargs):
                self.now[0] = T + timedelta(seconds=31)
                return original_prune(*args, **kwargs)
            with patch.object(self.publisher, '_prune',
                              side_effect=slow_prune):
                result = self.publisher.publish_bundle(output)
        self.assertEqual(result['status'], 'PUBLISHED')
        self.assertTrue(result['committed'])
        self.assertTrue(result['committed_at_valid'])
        self.assertFalse(result['observation_valid'])
        self.assertFalse(result['current_evidence_valid'])
        self.assertIn('PUBLICATION_CURRENT_STALE',
                      result['current_reasons'])
        self.assertTrue(self.publisher.registry_path.is_file())
        self.assertTrue(Path(result['capture_root']).is_dir())

    def test_published_at_uses_commit_time_not_initial_validation_time(self):
        output = self.root / 'commit-time'
        with self.compile_patch():
            from martin_agentic.broker_bundle import collect_saved_bundle
            collect_saved_bundle(
                output, account_suffix='0000', transport=FakeReadTransport(),
                timeout_seconds=5, clock=lambda: T)
        original = self.publisher._copy_bundle
        def advance(*args, **kwargs):
            value = original(*args, **kwargs)
            self.now[0] = T + timedelta(seconds=1)
            return value
        with self.compile_patch():
            with patch.object(self.publisher, '_copy_bundle',
                              side_effect=advance):
                result = self.publisher.publish_bundle(output)
        self.assertEqual(result['status'], 'PUBLISHED')
        self.assertEqual(result['published_at'],
                         (T + timedelta(seconds=1)).isoformat())
        record = self.registry().read()
        self.assertEqual(record['published_at'], result['published_at'])

    def test_source_policy_and_public_change_after_copy_are_rejected(self):
        output = self.root / 'change-after-copy'
        with self.compile_patch():
            from martin_agentic.broker_bundle import collect_saved_bundle
            collect_saved_bundle(
                output, account_suffix='0000', transport=FakeReadTransport(),
                timeout_seconds=5, clock=lambda: T)

        policy_publisher = CapturePublisher(
            self.root / 'policy-change', self.public.manifest,
            policy=deepcopy(self.policy), account_key=ACCOUNT_KEY,
            account_suffix='0000',
            source_fingerprint_value=source_fingerprint(), clock=lambda: T)
        original = policy_publisher._copy_bundle
        def mutate_policy(*args, **kwargs):
            value = original(*args, **kwargs)
            policy_publisher.policy['account_capital_usd'] = '501'
            return value
        with self.compile_patch():
            with patch.object(policy_publisher, '_copy_bundle',
                              side_effect=mutate_policy):
                result = policy_publisher.publish_bundle(output)
        self.assertEqual(result['status'], 'REJECTED')
        self.assertIn('PUBLICATION_POLICY_CHANGED', result['error'])
        self.assertFalse(policy_publisher.registry_path.exists())

        source_publisher = CapturePublisher(
            self.root / 'source-change', self.public.manifest,
            policy=deepcopy(self.policy), account_key=ACCOUNT_KEY,
            account_suffix='0000',
            source_fingerprint_value=source_fingerprint(), clock=lambda: T)
        calls = [0]
        real_source = source_fingerprint
        def changing_source():
            calls[0] += 1
            return real_source() if calls[0] == 1 else '0' * 64
        with self.compile_patch():
            with patch('martin_agentic.capture_publisher.source_fingerprint',
                       side_effect=changing_source):
                result = source_publisher.publish_bundle(output)
        self.assertEqual(result['status'], 'REJECTED')
        self.assertIn('PUBLICATION_SOURCE_CHANGED', result['error'])
        self.assertFalse(source_publisher.registry_path.exists())

        public_publisher = CapturePublisher(
            self.root / 'public-change', self.public.manifest,
            policy=deepcopy(self.policy), account_key=ACCOUNT_KEY,
            account_suffix='0000',
            source_fingerprint_value=source_fingerprint(), clock=lambda: T)
        original_public = public_publisher._copy_bundle
        def mutate_public(*args, **kwargs):
            value = original_public(*args, **kwargs)
            self.public.manifest.write_text(
                self.public.manifest.read_text() + ' ')
            return value
        with self.compile_patch():
            with patch.object(public_publisher, '_copy_bundle',
                              side_effect=mutate_public):
                result = public_publisher.publish_bundle(output)
        self.assertEqual(result['status'], 'REJECTED')
        self.assertIn('PUBLICATION_PUBLIC_MANIFEST_CHANGED', result['error'])
        self.assertFalse(public_publisher.registry_path.exists())

    def test_same_generation_association_change_is_rejected(self):
        observed = T.isoformat()
        good = snapshot(FakeBroker(), observed)
        good['human_must_approve_trades'] = False
        good['portfolio']['cash'] = '501'
        first = self.publish(CashTransport('501'), journal_snapshot=good)
        self.assertEqual(first['status'], 'PUBLISHED')
        other = deepcopy(good)
        # Same semantic journal observation, different persisted association
        # metadata (mask is not part of the binding fields).
        other['account_mask'] = '••••9999'
        self.assertNotEqual(other['account_mask'], good['account_mask'])
        # The same capture retried with a different association cannot rewrite
        # the immutable generation or return the new unpersisted association.
        with self.compile_patch():
            result = self.publisher.publish_bundle(
                first['capture_root'], journal_snapshot=other)
        self.assertEqual(result['status'], 'REJECTED')
        self.assertIn('PUBLICATION_ASSOCIATION_CONFLICT', result['error'])

    def test_orphan_retry_recovers_complete_generation(self):
        output = self.root / 'orphan-output'
        with self.compile_patch():
            from martin_agentic.broker_bundle import collect_saved_bundle
            collect_saved_bundle(
                output, account_suffix='0000',
                transport=CashTransport('501'), timeout_seconds=5,
                clock=lambda: T)
            with patch.object(self.publisher, '_publish_record',
                              side_effect=OSError('no pointer')):
                with self.assertRaises(OSError):
                    self.publisher.publish_bundle(output)
            self.assertFalse(self.publisher.registry_path.exists())
            result = self.publisher.publish_bundle(output)
        self.assertEqual(result['status'], 'RECOVERED')
        self.assertEqual(self.registry().read()['generation_id'],
                         result['generation_id'])
        final_capture = capture_generation(result['capture_root'])
        self.assertEqual(final_capture['generation'], result['generation_id'])

    def test_committed_cleanup_retry_is_idempotent_not_fake_failure(self):
        first = self.publish()
        second_output = self.root / 'cleanup-retry'
        with self.compile_patch():
            from martin_agentic.broker_bundle import collect_saved_bundle
            collect_saved_bundle(
                second_output, account_suffix='0000',
                transport=CashTransport('501'), timeout_seconds=5,
                clock=lambda: T)
            with patch.object(self.publisher, '_prune',
                              side_effect=OSError('cleanup crashed')):
                first_attempt = self.publisher.publish_bundle(second_output)
        self.assertEqual(first_attempt['status'], 'PUBLISHED')
        self.assertEqual(first_attempt['cleanup']['status'], 'pending')
        # The pointer was already committed; retrying reports idempotent
        # state, not a fake publication failure or a new observation.
        with self.compile_patch():
            retried = self.publisher.publish_bundle(second_output)
        self.assertIn(retried['status'], {'ALREADY_PUBLISHED', 'RECOVERED'})
        self.assertEqual(self.registry().read()['generation_id'],
                         retried['generation_id'])
        self.assertNotEqual(first['generation_id'], retried['generation_id'])

    def test_cleanup_limit_reports_pending_and_keeps_current(self):
        first = self.publish()
        second = self.publish(CashTransport('501'))
        with patch('martin_agentic.capture_publisher.PRUNE_MAX_ENTRIES', 1):
            third = self.publish(CashTransport('502'))
        self.assertEqual(third['cleanup']['status'], 'pending')
        self.assertEqual(third['cleanup']['reason'], 'ENTRY_LIMIT')
        self.assertTrue(Path(third['capture_root']).is_dir())
        self.assertEqual(self.registry().read()['generation_id'],
                         third['generation_id'])
        status = self.publisher.publication_status()
        self.assertEqual(status['status'], 'COMMITTED')
        self.assertEqual(status['generation_id'], third['generation_id'])

    def test_stale_and_future_capture_are_not_published(self):
        output = self.root / 'collect-now'
        with self.compile_patch():
            from martin_agentic.broker_bundle import collect_saved_bundle
            result = collect_saved_bundle(
                output, account_suffix='0000', transport=FakeReadTransport(),
                timeout_seconds=5, clock=lambda: T)
        self.assertNotEqual(result['status'], 'FAILED')
        self.now[0] = T + timedelta(seconds=31)
        with self.compile_patch():
            rejected = self.publisher.publish_bundle(output)
            self.assertEqual(rejected['status'], 'REJECTED')
            self.assertIn('PUBLICATION_CAPTURE_STALE', rejected['error'])
        early_public = PublicFixture(
            self.root / 'public-early',
            retrieved_at=(T - timedelta(seconds=2)).isoformat())
        early = CapturePublisher(
            self.root / 'early-captures', early_public.manifest, policy=self.policy,
            account_key=ACCOUNT_KEY, account_suffix='0000',
            source_fingerprint_value=source_fingerprint(),
            clock=lambda: T - timedelta(seconds=1))
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   early_public.policy):
            rejected = early.publish_bundle(output)
            self.assertEqual(rejected['status'], 'REJECTED')
            self.assertIn('PUBLICATION_CAPTURE_FUTURE', rejected['error'])

    def test_hash_mismatch_bundle_is_not_published(self):
        output = self.root / 'hash-mismatch'
        with self.compile_patch():
            from martin_agentic.broker_bundle import collect_saved_bundle
            collect_saved_bundle(
                output, account_suffix='0000', transport=FakeReadTransport(),
                timeout_seconds=5, clock=lambda: T)
        manifest = json.loads((output / 'manifest.json').read_text())
        manifest['pages'][0]['raw_sha256'] = '0' * 64
        (output / 'manifest.json').write_text(json.dumps(manifest))
        os.chmod(output / 'manifest.json', 0o600)
        with self.compile_patch():
            with self.assertRaisesRegex(Exception, 'BUNDLE_RAW_HASH_MISMATCH'):
                self.publisher.publish_bundle(output)
        self.assertFalse(self.publisher.registry_path.exists())

    def test_crash_after_registry_replace_leaves_complete_new_generation(self):
        first = self.publish()
        second_output = self.root / 'after-output'
        with self.compile_patch():
            from martin_agentic.broker_bundle import collect_saved_bundle
            collect_saved_bundle(
                second_output, account_suffix='0000',
                transport=CashTransport('501'), timeout_seconds=5,
                clock=lambda: T)
            real_fsync = __import__('martin_agentic.capture_publisher',
                                    fromlist=['_fsync_dir'])._fsync_dir
            def fail_root(path):
                if Path(path).resolve() == self.publisher.root.resolve():
                    raise OSError('CRASH_AFTER_REPLACE')
                return real_fsync(path)
            with patch('martin_agentic.capture_publisher._fsync_dir',
                       side_effect=fail_root):
                result = self.publisher.publish_bundle(second_output)
        self.assertEqual(result['status'], 'PUBLISHED')
        record = self.registry().read()
        self.assertNotEqual(record['generation_id'], first['generation_id'])
        capture = capture_generation(record['capture_root'])
        self.assertEqual(capture['generation'], record['generation_id'])

    def test_crash_before_registry_replace_keeps_previous_generation(self):
        first = self.publish()
        registry_before = self.publisher.registry_path.read_bytes()
        # Build a second collector output and force the atomic pointer write to
        # fail after the immutable generation directory is renamed.
        second_output = self.root / 'second-output'
        with self.compile_patch():
            from martin_agentic.broker_bundle import collect_saved_bundle
            collect_saved_bundle(
                second_output, account_suffix='0000',
                transport=CashTransport('501'), timeout_seconds=5,
                clock=lambda: T)
            original = self.publisher._publish_record
            with patch.object(self.publisher, '_publish_record',
                              side_effect=OSError('CRASH_BEFORE_REPLACE')):
                with self.assertRaises(OSError):
                    self.publisher.publish_bundle(second_output)
            self.publisher._publish_record = original
        self.assertEqual(self.publisher.registry_path.read_bytes(),
                         registry_before)
        self.assertEqual(self.registry().read()['generation_id'],
                         first['generation_id'])

    def test_concurrent_readers_never_observe_a_partial_registry(self):
        first = self.publish()
        errors = []
        stop = threading.Event()

        def reader():
            registry = self.registry()
            while not stop.is_set():
                try:
                    registry.read()
                except Exception as exc:
                    errors.append(repr(exc))

        thread = threading.Thread(target=reader)
        thread.start()
        try:
            second = self.publish(CashTransport('501'))
            self.assertEqual(second['status'], 'PUBLISHED')
        finally:
            stop.set()
            thread.join()
        self.assertEqual(errors, [])
        self.assertEqual(self.registry().read()['generation_id'],
                         second['generation_id'])
        self.assertNotEqual(first['generation_id'], second['generation_id'])

    def test_generation_retention_keeps_referenced_and_current(self):
        first = self.publish()
        second = self.publish(CashTransport('501'))
        third = self.publish(CashTransport('502'))
        self.publisher.retention = 1
        fourth = self.publish(
            CashTransport('503'),
            referenced_generations=(Path(first['capture_root']).name,))
        generations = self.publisher.root / 'generations'
        names = {entry.name for entry in os.scandir(generations)
                 if entry.is_dir(follow_symlinks=False)}
        self.assertIn(Path(first['capture_root']).name, names)
        self.assertIn(Path(fourth['capture_root']).name, names)
        self.assertNotIn(Path(second['capture_root']).name, names)
        self.assertNotIn(Path(third['capture_root']).name, names)

    def test_journal_association_requires_same_semantic_observation(self):
        observed = T.isoformat()
        good = snapshot(FakeBroker(), observed)
        good['human_must_approve_trades'] = False
        good['portfolio']['cash'] = '501'
        result = self.publish(CashTransport('501'), journal_snapshot=good)
        self.assertEqual(result['status'], 'PUBLISHED', result)
        self.assertEqual(result['association']['generation_id'],
                         result['generation_id'])
        self.assertTrue((Path(result['capture_root'])
                         / 'journal-generation.json').is_file())

        bad = deepcopy(good)
        bad['portfolio']['cash'] = '502'
        registry_before = self.publisher.registry_path.read_bytes()
        result = self.publish(journal_snapshot=bad)
        self.assertEqual(result['status'], 'FAILED')
        # No new current registry is installed for the mismatched snapshot.
        self.assertEqual(self.publisher.registry_path.read_bytes(),
                         registry_before)

    def test_refresh_missing_to_g1_g2_expiry_and_old_receipt_binding(self):
        book = ExecutionBook(self.root / 'book.sqlite', self.policy,
                             simulation=False, clock=lambda: self.now[0])
        self.addCleanup(book.close)
        factory = CurrentContextFactory(self.registry(),
                                        clock=lambda: self.now[0])
        with self.compile_patch():
            context, status = ensure_default_preflight(
                book, factory=factory, clock=lambda: self.now[0])
            self.assertIsInstance(context, MissingCurrentContext)
            self.assertEqual(status['status'], 'missing')
            first = self.publish()
            refresher = book._current_context_refresher
            status = refresher.refresh()
            self.assertEqual(status['status'], 'bound')
            g1 = book._current_preflight
            self.assertIsInstance(g1, CurrentEngineeringPreflight)
            self.assertEqual(g1.expected_capture_generation,
                             first['generation_id'])
            g1_binding = g1.binding_id
            # Exercise the actual factory -> book assessment consumer with a
            # matching journal observation, not only an allowed=false constant.
            observed = capture_generation(first['capture_root'])['observed_at']
            snap = snapshot(FakeBroker(), observed)
            snap['human_must_approve_trades'] = False
            book.reconcile(snap)
            journal = BrokerJournal(book.db_path)
            try:
                journal.ingest(snap)
            finally:
                journal.close()
            plan = {'symbol': 'DEMO', 'quantity': 2,
                    'entry_ceiling': '40', 'stop': '39',
                    'cost_buffer_per_share': '0.05'}
            quote = {'symbol': 'DEMO', 'bid': '39.99', 'ask': '40',
                     'bid_at': observed, 'ask_at': observed,
                     'observed_at': observed}
            trade = {'id': 'engineering-publisher', 'purpose': 'engineering',
                     'plan': plan, 'phase': 'ENTRY_PLANNED',
                     'stop': plan['stop'], 'initial_stop': plan['stop'],
                     'quantity': '0', 'entry_price': None,
                     'snapshot_at': observed, 'exit_requested': False,
                     'cancel_for_replace': None,
                     'evidence': {'synthetic': False,
                                  'source_sha256': 'fixture',
                                  'engineering_scope_authorized': True,
                                  'parameters_user_specified': True,
                                  'parameters_valid_until':
                                      (T + timedelta(minutes=30)).isoformat()},
                     'preflight_quote': quote}
            state = book.state()
            state['trades'][trade['id']] = trade
            book.db.execute('UPDATE live_state SET payload=? WHERE id=1',
                            (canonical(state),))
            book.db.commit()
            book._queue(trade, 'entry',
                        {'symbol': 'DEMO', 'side': 'buy', 'type': 'limit',
                         'quantity': '2', 'limit_price': '40',
                         'market_hours': 'regular_hours',
                         'time_in_force': 'gfd'}, T, 'TEST')
            book.db.commit()
            action = next(a for a in book.actions() if a['kind'] == 'entry')
            decision = book._current_preflight_decision(action=action)
            self.assertFalse(decision['allowed'])
            self.assertIn('FORMAL_ENGINEERING_CHANNEL_UNPROVEN',
                          decision['reasons'])
            self.assertIsNotNone(decision.get('receipt'))
            self.assertEqual(
                decision['receipt']['components']['journal_current']['status'],
                'evaluated')
            self.assertTrue(book.current_preflight_binding(
                decision, action=action)['bound'])
            second = self.publish(CashTransport('501'))
            status = refresher.refresh()
            self.assertEqual(status['status'], 'bound')
            g2 = book._current_preflight
            self.assertNotEqual(g1_binding, g2.binding_id)
            self.assertEqual(g2.expected_capture_generation,
                             second['generation_id'])
            # A receipt issued under G1 cannot be presented as the current G2.
            receipt = build_receipt(
                mode='claim', simulation=False, owner_binding_id=g1_binding,
                account={'key_hash': ACCOUNT_KEY, 'mask': '••••0000',
                         'suffix': '0000',
                         'configured_key_hash': ACCOUNT_KEY},
                policy_sha256=self.publisher._policy_sha256(),
                source={'expected_fingerprint': source_fingerprint(),
                        'observed_fingerprint': source_fingerprint(),
                        'pin': {}, 'public_manifest_sha256': '0' * 64,
                        'public_files': []},
                capture={'status': 'evaluated'}, journal={},
                journal_health={}, action={}, quote={}, components={},
                reasons=[], assessed_at=self.now[0].isoformat())
            binding = book.current_preflight_binding({'receipt': receipt})
            self.assertFalse(binding['bound'])
            self.assertIn('RECEIPT_OWNER_BINDING_MISMATCH',
                          binding['reasons'])
            self.now[0] = T + timedelta(seconds=31)
            status = refresher.refresh()
            self.assertEqual(status['status'], 'missing')
            self.assertIn('CURRENT_CONTEXT_CAPTURE_STALE', status['reason'])
            # The stale pointer is not renewed. A fresh observation can publish
            # and recover the default context.
            stale_registry = self.publisher.registry_path.read_bytes()
            rejected = self.publisher.publish_bundle(first['capture_root'])
            self.assertEqual(rejected['status'], 'REJECTED')
            self.assertIn('PUBLICATION_CAPTURE_STALE', rejected['error'])
            self.assertEqual(self.publisher.registry_path.read_bytes(),
                             stale_registry)
            third = self.publish(CashTransport('502'))
            status = refresher.refresh()
            self.assertEqual(status['status'], 'bound')
            self.assertEqual(book._current_preflight.expected_capture_generation,
                             third['generation_id'])

    def test_live_supervisor_and_host_adopt_factory_after_publication(self):
        book = ExecutionBook(self.root / 'adopt.sqlite', self.policy,
                             simulation=False, clock=lambda: self.now[0])
        self.addCleanup(book.close)
        factory = CurrentContextFactory(self.registry(),
                                        clock=lambda: self.now[0])
        with self.compile_patch():
            context, status = ensure_default_preflight(
                book, factory=factory, clock=lambda: self.now[0])
            self.assertIsInstance(context, MissingCurrentContext)
            self.publish()
            supervisor = LiveSupervisor(book, source_mode='real')
            self.assertIsInstance(book._current_preflight,
                                  CurrentEngineeringPreflight)
            self.assertEqual(supervisor.current_context_status['status'],
                             'bound')
            bridge = HostExecutionBridge(book, self.root / 'host-adopt')
            self.addCleanup(bridge.close)
            self.assertEqual(bridge.current_context_status['status'],
                             'bound')
            self.assertIsNotNone(bridge.current_context_refresher)

    def test_registry_pointer_swap_between_assessment_and_claim_is_detected(self):
        self.publish()
        book = ExecutionBook(self.root / 'swap.sqlite', self.policy,
                             simulation=False, clock=lambda: self.now[0])
        self.addCleanup(book.close)
        factory = CurrentContextFactory(self.registry(),
                                        clock=lambda: self.now[0])
        with self.compile_patch():
            ensure_default_preflight(book, factory=factory,
                                     clock=lambda: self.now[0])
            refresher = book._current_context_refresher
            self.assertEqual(refresher.refresh()['status'], 'bound')
            g1 = book._current_preflight
            self.publish(CashTransport('501'))
            receipt = build_receipt(
                mode='claim', simulation=False,
                owner_binding_id=g1.binding_id,
                account={'key_hash': ACCOUNT_KEY, 'mask': '••••0000',
                         'suffix': '0000',
                         'configured_key_hash': ACCOUNT_KEY},
                policy_sha256=g1.policy_sha256,
                source={'expected_fingerprint': g1.source_fingerprint_value,
                        'observed_fingerprint': g1.source_fingerprint_value,
                        'pin': {}, 'public_manifest_sha256': '0' * 64,
                        'public_files': []},
                capture={'status': 'evaluated'}, journal={},
                journal_health={}, action={}, quote={}, components={},
                reasons=[], assessed_at=self.now[0].isoformat())
            binding = book.current_preflight_binding({'receipt': receipt})
            self.assertFalse(binding['bound'])
            self.assertIn('CURRENT_REGISTRY_POINTER_CHANGED',
                          binding['reasons'])
            self.assertEqual(refresher.refresh()['status'], 'bound')
            self.assertNotEqual(book._current_preflight.binding_id,
                                g1.binding_id)

    def test_refresh_blocks_source_pin_and_capture_changes(self):
        source_root = self.root / 'source-captures'
        source_publisher = CapturePublisher(
            source_root, self.public.manifest, policy=self.policy,
            account_key=ACCOUNT_KEY, account_suffix='0000',
            source_fingerprint_value=source_fingerprint(),
            clock=lambda: self.now[0])
        with self.compile_patch():
            source_publisher.produce_and_publish(
                FakeReadTransport(), timeout_seconds=5)
        source_book = ExecutionBook(
            self.root / 'source-book.sqlite', self.policy,
            simulation=False, clock=lambda: self.now[0])
        self.addCleanup(source_book.close)
        source_registry = CurrentContextRegistry(
            source_publisher.registry_path,
            allowed_capture_roots=(source_root,),
            allowed_public_roots=(self.public.root,),
            clock=lambda: self.now[0])
        source_factory = CurrentContextFactory(
            source_registry, clock=lambda: self.now[0])
        with self.compile_patch():
            ensure_default_preflight(source_book, factory=source_factory,
                                     clock=lambda: self.now[0])
            source_refresher = source_book._current_context_refresher
            self.assertEqual(source_refresher.refresh()['status'], 'bound')
            current_pin = source_book._current_preflight.context_source_pin
            with patch('martin_agentic.current_context.source_pin',
                       return_value=dict(current_pin, digest='0' * 64)):
                status = source_refresher.refresh()
            self.assertEqual(status['status'], 'missing')
            self.assertIn('CURRENT_CONTEXT_SOURCE_CHANGED',
                          status['reason'])

        capture_root = self.root / 'capture-captures'
        capture_publisher = CapturePublisher(
            capture_root, self.public.manifest, policy=self.policy,
            account_key=ACCOUNT_KEY, account_suffix='0000',
            source_fingerprint_value=source_fingerprint(),
            clock=lambda: self.now[0])
        with self.compile_patch():
            capture_publisher.produce_and_publish(
                FakeReadTransport(), timeout_seconds=5)
        capture_book = ExecutionBook(
            self.root / 'capture-book.sqlite', self.policy,
            simulation=False, clock=lambda: self.now[0])
        self.addCleanup(capture_book.close)
        capture_registry = CurrentContextRegistry(
            capture_publisher.registry_path,
            allowed_capture_roots=(capture_root,),
            allowed_public_roots=(self.public.root,),
            clock=lambda: self.now[0])
        with self.compile_patch():
            ensure_default_preflight(
                capture_book,
                factory=CurrentContextFactory(
                    capture_registry, clock=lambda: self.now[0]),
                clock=lambda: self.now[0])
            capture_refresher = capture_book._current_context_refresher
            self.assertEqual(capture_refresher.refresh()['status'], 'bound')
            victim = next(
                Path(capture_book._current_preflight.capture_root, 'raw').glob('*.raw'))
            victim.unlink()
            status = capture_refresher.refresh()
            self.assertEqual(status['status'], 'missing')
            self.assertIn('RECEIPT_CAPTURE_GENERATION_CHANGED',
                          status['reason'])

    def test_refresh_blocks_policy_and_source_pin_changes(self):
        self.publish()
        book = ExecutionBook(self.root / 'book2.sqlite', self.policy,
                             simulation=False, clock=lambda: self.now[0])
        self.addCleanup(book.close)
        factory = CurrentContextFactory(self.registry(),
                                        clock=lambda: self.now[0])
        with self.compile_patch():
            ensure_default_preflight(book, factory=factory,
                                     clock=lambda: self.now[0])
            refresher = book._current_context_refresher
            self.assertEqual(refresher.refresh()['status'], 'bound')
            book.policy['account_capital_usd'] = '501'
            self.assertIn('CURRENT_CONTEXT_POLICY_CHANGED',
                          refresher.refresh()['reason'])
