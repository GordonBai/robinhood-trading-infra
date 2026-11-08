"""Same-observation saved-bundle bridge tests; fake transport, temp files only."""
from copy import deepcopy
from datetime import datetime
import json
from pathlib import Path
import tempfile
import time
from unittest import TestCase
from unittest.mock import MagicMock, patch

from martin_agentic.broker import BrokerJournal
from martin_agentic.capture_publisher import CapturePublisher
from martin_agentic.live_capture_bridge import (
    LiveCaptureBridgeError, SavedBundleObservationBridge,
    journal_association_projection, prune_staging, release_staging_owner)
from martin_agentic.current_evidence import capture_generation
from martin_luk.live_session import (
    full_account_flat_authority, run_live_session)
from martin_agentic.transport import TransportError
from test_broker import approval
from test_broker_bundle import AT, FakeReadTransport
from test_current_evidence import PublicFixture


class FakeCodexBrokerRead:
    """A read-only transport with the ownership surface owned_read_transport needs."""
    instances = []

    def __init__(self, *args, **kwargs):
        self.inner = FakeReadTransport()
        self.rpc = MagicMock()
        self.rpc.close.return_value = {'complete': True}
        self.__class__.instances.append(self)

    def __call__(self, *args, **kwargs):
        return self.inner(*args, **kwargs)

    def bind(self, *args, **kwargs):
        return self.inner.bind(*args, **kwargs)

    def arm_deadline(self, deadline):
        return self.inner.arm_deadline(deadline)

    def __enter__(self):
        self.inner.__enter__()
        return self

    def __exit__(self, *args):
        return self.inner.__exit__(*args)


class BridgeCase(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.public = PublicFixture(self.root / 'public')
        self.now = datetime.fromisoformat(AT)
        self.policy = json.loads(
            Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        self.policy.update(capture_only=True, session_date=AT[:10])
        self.policy['current_capture'] = {
            'root': str(self.root / 'captures'),
            'public_manifest': str(self.public.manifest),
        }

    def bridge(self):
        return SavedBundleObservationBridge(self.policy, lambda: self.now)

    def read(self, transport=None, **kwargs):
        return self.bridge().read(
            transport or FakeReadTransport(),
            deadline_monotonic=time.monotonic() + 10,
            timeout_seconds=10, **kwargs)

    def publish(self, snapshot, journal_snapshot=None):
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            return self.bridge().publish(
                snapshot, journal_snapshot=journal_snapshot)

    def test_one_saved_observation_is_stable_and_supported_only(self):
        transport = FakeReadTransport()
        snapshot = self.read(transport)
        self.assertEqual(snapshot['observed_at'], AT)
        self.assertEqual(snapshot['buying_power'], '500')
        self.assertEqual(snapshot['unleveraged_buying_power'], '500')
        self.assertTrue(snapshot['collection_complete'])
        self.assertTrue(snapshot['supported_coverage_complete'])
        self.assertFalse(snapshot['full_baseline_complete'])
        self.assertFalse(snapshot['settlement_ledger_complete'])
        self.assertFalse(snapshot['account_flat_proven'])
        self.assertEqual(snapshot['unsupported_scopes'],
                         ['futures', 'event_contracts', 'fixed_income',
                          'mutual_funds', 'settlements'])
        self.assertEqual(snapshot['live_capture']['generation'],
                         capture_generation(snapshot['live_capture']['bundle_root'])['generation'])
        # 1 account + 2 single-pass + 8 tools x 2 passes = 19 calls.
        self.assertEqual(len(transport.calls), 19)
        second = self.read(FakeReadTransport())
        self.assertEqual(second['live_capture']['generation'],
                         snapshot['live_capture']['generation'])

    def test_publication_uses_actual_journal_payload_and_is_idempotent(self):
        snapshot = self.read()
        journal = BrokerJournal(self.root / 'journal.sqlite')
        self.addCleanup(journal.close)
        journal.ingest(snapshot)
        payload = journal.current_observation()
        projection = journal_association_projection(payload)
        first = self.publish(snapshot, projection)
        self.assertEqual(first['status'], 'PUBLISHED', first)
        self.assertTrue(first['current_evidence_valid'])
        self.assertEqual(first['association']['journal_sha256'],
                         __import__('trading_contracts.values', fromlist=['digest']).digest(projection))
        # A repeated owner retry reads the same observation into a new local
        # bundle and must not conflict on journal bookkeeping.
        again = self.read()
        journal.ingest(again)
        second = self.publish(
            again, journal_association_projection(
                journal.current_observation()))
        self.assertEqual(second['status'], 'ALREADY_PUBLISHED', second)
        self.assertTrue(second['committed'])
        self.assertTrue(second['current_evidence_valid'])

    def test_missing_public_manifest_and_bad_approval_cannot_publish(self):
        snapshot = self.read()
        missing = SavedBundleObservationBridge(
            {**self.policy, 'current_capture': {
                **self.policy['current_capture'],
                'public_manifest': str(self.root / 'missing.json')}},
            lambda: self.now)
        rejected = missing.publish(snapshot)
        self.assertEqual(rejected['status'], 'REJECTED', rejected)
        self.assertFalse((self.root / 'captures' / 'registry.json').exists())

        def handler(transport, tool, args, index):
            if tool == 'get_trade_approvals':
                return {'data': {'approvals': [approval('approval-1')]}}

        approved = self.read(FakeReadTransport(handler=handler))
        self.assertEqual(approved['pending_approvals'], 1)
        self.assertEqual(approved['pending_approval_ids'], ['approval-1'])
        self.assertIn('approval-1', approved['approval_states'])
        journal = BrokerJournal(self.root / 'approval.sqlite')
        self.addCleanup(journal.close)
        journal.ingest(approved)
        result = self.publish(
            approved, journal_association_projection(
                journal.current_observation()))
        self.assertEqual(result['status'], 'REJECTED', result)
        self.assertFalse(result['committed'])

    def test_association_must_match_actual_journal_and_account(self):
        snapshot = self.read()
        journal = BrokerJournal(self.root / 'mismatch.sqlite')
        self.addCleanup(journal.close)
        journal.ingest(snapshot)
        projection = journal_association_projection(
            journal.current_observation())
        wrong = deepcopy(projection)
        wrong['positions'] = {'DEMO': {'quantity': '1'}}
        result = self.publish(snapshot, wrong)
        self.assertEqual(result['status'], 'REJECTED', result)
        wrong_account = deepcopy(snapshot)
        wrong_account['account_key'] = '0' * 64
        result = self.publish(wrong_account, projection)
        self.assertEqual(result['status'], 'REJECTED', result)
        self.assertFalse((self.root / 'captures' / 'registry.json').exists())

    def test_two_pass_and_crypto_anomalies_fail_closed(self):
        def two_pass(transport, tool, args, index):
            if tool == 'get_equity_positions' and index >= 1:
                return {'data': {'positions': [{
                    'symbol': 'DEMO', 'quantity': '1',
                    'shares_available_for_sells': '1',
                    'shares_held_for_sells': '0',
                    'shares_held_for_asset_transfer': '0',
                    'shares_held_for_options_events': '0',
                    'shares_held_for_stock_grants': '0',
                    'shares_pending_from_options_events': '0',
                }]}}
        with self.assertRaises(LiveCaptureBridgeError) as raised:
            self.read(FakeReadTransport(handler=two_pass))
        self.assertIn('SUPPORTED_COVERAGE', str(raised.exception))

        def crypto(transport, tool, args, index):
            if tool == 'get_crypto_orders':
                return {'data': {
                    'crypto_account_number': 'RHC-0000',
                    'rhs_account_number': 'RHS-0000',
                    'results': [{'id': 'crypto-order'}]}}
        with self.assertRaises(LiveCaptureBridgeError) as raised:
            self.read(FakeReadTransport(handler=crypto))
        self.assertIn('SUPPORTED_COVERAGE', str(raised.exception))

    def test_failed_observation_does_not_refresh_previous_journal(self):
        healthy = self.read()
        journal = BrokerJournal(self.root / 'previous.sqlite')
        self.addCleanup(journal.close)
        journal.ingest(healthy)
        before = journal.current_observation()
        def conflict(transport, tool, args, index):
            if tool == 'get_equity_positions':
                return {'data': {'positions': [
                    {'symbol': 'DEMO', 'quantity': '0',
                     'shares_available_for_sells': '0',
                     'shares_held_for_sells': '0',
                     'shares_held_for_asset_transfer': '0',
                     'shares_held_for_options_events': '0',
                     'shares_held_for_stock_grants': '0',
                     'shares_pending_from_options_events': '0'},
                    {'symbol': 'DEMO', 'quantity': '1',
                     'shares_available_for_sells': '1',
                     'shares_held_for_sells': '0',
                     'shares_held_for_asset_transfer': '0',
                     'shares_held_for_options_events': '0',
                     'shares_held_for_stock_grants': '0',
                     'shares_pending_from_options_events': '0'}]}}
        with self.assertRaises(LiveCaptureBridgeError):
            self.read(FakeReadTransport(handler=conflict))
        after = journal.current_observation()
        self.assertEqual(before, after)
        self.assertEqual(after['observed_at'], healthy['observed_at'])

    def test_supported_fact_blocker_classification_covers_all_scopes(self):
        from martin_agentic.live_capture_bridge import _blocker_is_scope_unknown_or_management
        supported_errors = [
            'IDENTITY_CONFLICT:get_equity_positions:DEMO',
            'IDENTITY_CONFLICT:get_option_positions:OPT',
            'IDENTITY_CONFLICT:get_crypto_positions:BTC',
            'IDENTITY_CONFLICT:get_advanced_orders:GROUP',
            'IDENTITY_CONFLICT:equity_order:order-1',
            'TWO_PASS_MISMATCH:get_equity_positions',
            'TWO_PASS_REQUIRED:get_option_orders',
            'QUERY_SCOPE_PARTIAL:get_equity_orders',
            'SCHEMA_UNVERIFIED:get_crypto_positions',
            'UNPROVEN_ORDER_QUANTITY:order-1',
            'EXECUTION_ID_DUPLICATE:exec-1',
            'ORDER_REF_DUPLICATE:order-1',
            'UNKNOWN_FUTURE_FACT',
        ]
        for blocker in supported_errors:
            self.assertFalse(
                _blocker_is_scope_unknown_or_management(blocker), blocker)
        for blocker in [
            'CONDITIONS_MISSING:futures_positions',
            'GETTER_MISSING:fixed_income_orders',
            'SCOPE_UNAVAILABLE:mutual_funds',
            'SETTLEMENT_SOURCE_MISSING',
            'CONDITIONS_MISSING:SETTLEMENT_SOURCE_MISSING',
            'NONFLAT_POSITION:equity:DEMO',
            'OPEN_ORDER:equity:order-1',
            'AGGREGATE_NONZERO:options_value',
            'APPROVALS_UNVERIFIED',
            'BUNDLE_SCHEMA_UNVERIFIED:get_trade_approvals:pass1',
        ]:
            self.assertTrue(
                _blocker_is_scope_unknown_or_management(blocker), blocker)

    def test_supported_identity_and_hold_errors_reject_before_journal(self):
        def duplicate(transport, tool, args, index):
            if tool == 'get_equity_positions':
                return {'data': {'positions': [{
                    'symbol': 'DEMO', 'quantity': str(q),
                    'shares_available_for_sells': str(q),
                    'shares_held_for_sells': '0',
                    'shares_held_for_asset_transfer': '0',
                    'shares_held_for_options_events': '0',
                    'shares_held_for_stock_grants': '0',
                    'shares_pending_from_options_events': '0',
                } for q in (0, 1)]}}
        with self.assertRaises(LiveCaptureBridgeError) as raised:
            self.read(FakeReadTransport(handler=duplicate))
        self.assertEqual(raised.exception.code,
                         'LIVE_CAPTURE_SUPPORTED_FACT_INVALID')
        journal = BrokerJournal(self.root / 'untouched.sqlite')
        self.addCleanup(journal.close)
        self.assertIsNone(journal.current_observation())

        def bad_hold(transport, tool, args, index):
            if tool == 'get_equity_positions':
                return {'data': {'positions': [{
                    'symbol': 'DEMO', 'quantity': '1',
                    'shares_available_for_sells': '2',
                    'shares_held_for_sells': '0',
                    'shares_held_for_asset_transfer': '0',
                    'shares_held_for_options_events': '0',
                    'shares_held_for_stock_grants': '0',
                    'shares_pending_from_options_events': '0',
                }]}}
        with self.assertRaises(LiveCaptureBridgeError) as raised:
            self.read(FakeReadTransport(handler=bad_hold))
        self.assertIn('BROKER_POSITION_HOLDS_INVALID', str(raised.exception))

    def test_normal_nonflat_support_facts_still_reach_management(self):
        position = {
            'symbol': 'DEMO', 'quantity': '1',
            'shares_available_for_sells': '1', 'shares_held_for_sells': '0',
            'shares_held_for_asset_transfer': '0',
            'shares_held_for_options_events': '0',
            'shares_held_for_stock_grants': '0',
            'shares_pending_from_options_events': '0',
            'average_buy_price': '40',
        }
        def handler(transport, tool, args, index):
            if tool == 'get_equity_positions':
                # Duplicate identical records are semantically one fact.
                return {'data': {'positions': [position, deepcopy(position)]}}
        snapshot = self.read(FakeReadTransport(handler=handler))
        self.assertEqual(snapshot['positions']['DEMO']['quantity'], '1')
        journal = BrokerJournal(self.root / 'nonflat.sqlite')
        self.addCleanup(journal.close)
        summary = journal.ingest(snapshot)
        self.assertTrue(summary['read_healthy'])

    def test_close_keeps_supported_scope_flat_separate_from_whole_account(self):
        from martin_luk.live_session import _closing_check
        from martin_agentic.execution import ExecutionBook
        snapshot = self.read()
        book = ExecutionBook(self.root / 'close-book.sqlite',
                             self.policy, clock=lambda: self.now)
        journal = BrokerJournal(self.root / 'close-journal.sqlite')
        self.addCleanup(journal.close)
        self.addCleanup(book.close)
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            result = _closing_check(
                book, journal, self.policy, lambda: self.now,
                observation=(True, snapshot))
        self.assertFalse(result['final_flat_verified'])
        self.assertFalse(result['broker_physically_flat'])
        self.assertTrue(result['supported_scope_flat'])
        self.assertFalse(result['full_account_flat_proven'])
        self.assertNotEqual(result['close_disposition'], 'SAFE_FLAT')
        self.assertTrue(result['attention_required'])

    def test_failed_staging_is_bounded_and_owner_recovery_uses_journal(self):
        missing = SavedBundleObservationBridge(
            {**self.policy, 'current_capture': {
                **self.policy['current_capture'],
                'public_manifest': str(self.root / 'missing-public.json'),
                'retention': 3}}, lambda: self.now)
        statuses = []
        for _ in range(8):
            statuses.append(missing.publish(self.read())['status'])
        self.assertEqual(statuses, ['REJECTED'] * 8)
        staging = self.root / 'captures' / 'staging'
        self.assertLessEqual(len([p for p in staging.iterdir() if p.is_dir()]), 3)

        # Leave one journaled, unpublished observation for the default owner.
        recovery_snapshot = self.read()
        journal = BrokerJournal(self.root / 'recovery.sqlite')
        journal.ingest(recovery_snapshot)
        journal.close()
        import martin_agentic.live_session as live_session
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy), \
             patch.object(live_session, '_broker_read',
                          side_effect=RuntimeError('no network for recovery')), \
             patch.object(live_session, 'collect_live_events',
                          return_value=([], {'quotes': 10})), \
             patch.object(live_session, 'notify_local'), \
             patch.object(live_session.time, 'sleep'), \
             patch.object(live_session, 'print'):
            result = run_live_session(
                self.root / 'recovery.sqlite', self.policy,
                symbols=[f'S{i}' for i in range(10)], cycles=1,
                status_file=self.root / 'recovery-status.json',
                clock=lambda: self.now)
        recovery = result.get('capture_recovery') or {}
        self.assertIn(recovery.get('status'), {'PUBLISHED', 'ALREADY_PUBLISHED'},
                      recovery)
        self.assertFalse(recovery.get('network_read_performed', True))
        self.assertTrue((self.root / 'captures' / 'registry.json').is_file())

    def test_pointer_io_failure_is_recoverable_from_durable_journal(self):
        snapshot = self.read()
        journal = BrokerJournal(self.root / 'pointer.sqlite')
        self.addCleanup(journal.close)
        journal.ingest(snapshot)
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy), \
             patch.object(CapturePublisher, '_publish_record',
                          side_effect=OSError('pointer write failed')):
            with self.assertRaises(OSError):
                self.bridge().publish(snapshot)
        bundle = Path(snapshot['live_capture']['bundle_root'])
        marker = __import__(
            'martin_agentic.live_capture_bridge', fromlist=['_staging_marker']
        )._staging_marker(bundle)
        self.assertEqual(marker['state'], 'COMMIT_PENDING')
        self.assertFalse((self.root / 'captures' / 'registry.json').exists())
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            recovered = self.bridge().recover(journal)
        self.assertIn(recovered['status'], {'RECOVERED', 'PUBLISHED'}, recovered)
        self.assertTrue(recovered['network_read_performed'] is False)
        self.assertEqual(recovered['generation_id'],
                         snapshot['live_capture']['generation'])
        self.assertEqual(recovered['association']['journal_observed_at'],
                         snapshot['observed_at'])
        self.assertFalse(bundle.exists())
        self.assertTrue((self.root / 'captures' / 'registry.json').is_file())

    def test_bounded_prune_makes_progress_and_preserves_owner_pending(self):
        stage = self.root / 'captures' / 'staging'
        stage.mkdir(parents=True)
        stage.chmod(0o700)
        stage.parent.chmod(0o700)
        for index in range(129):
            path = stage / f'bridge-orphan-{index}'
            path.mkdir()
            old = time.time() - 600
            import os
            os.utime(path, (old, old))
        first = prune_staging(self.root / 'captures', retention=3)
        self.assertGreater(len(first['deleted']), 0, first)
        second = prune_staging(self.root / 'captures', retention=3)
        self.assertLessEqual(len([p for p in stage.iterdir() if p.is_dir()]), 3)

        self.policy['current_capture']['retention'] = 1
        snapshots = [self.read() for _ in range(5)]
        self.assertTrue(Path(snapshots[0]['live_capture']['bundle_root']).exists())
        self.assertGreaterEqual(len([p for p in stage.iterdir()
                                     if p.is_dir()]), 5)
        for _ in range(3):
            self.read()
        with self.assertRaises(LiveCaptureBridgeError) as capacity:
            self.read()
        self.assertEqual(capacity.exception.code,
                         'LIVE_CAPTURE_STAGING_CAPACITY')
        self.assertTrue(Path(snapshots[0]['live_capture']['bundle_root']).exists())

    def test_missing_scope_fields_cannot_claim_whole_account_flat(self):
        self.assertFalse(full_account_flat_authority({
            'provider': 'robinhood', 'synthetic': False,
            'collection_complete': True}))
        self.assertFalse(full_account_flat_authority({
            'account_flat_proven': True,
            'full_baseline_complete': True,
            'settlement_ledger_complete': True,
            'unsupported_scopes': ['settlements']}))

    def test_abandoned_pending_releases_capacity_after_owner_release(self):
        snapshots = [self.read() for _ in range(8)]
        release_staging_owner(self.root / 'captures')
        self.now += __import__('datetime').timedelta(seconds=601)
        import os
        for snapshot in snapshots:
            path = Path(snapshot['live_capture']['bundle_root'])
            old = time.time() - 601
            os.utime(path, (old, old))
        journal = BrokerJournal(self.root / 'fresh-owner.sqlite')
        self.addCleanup(journal.close)
        recovered = self.bridge().recover(journal)
        self.assertEqual(recovered['status'],
                         'NO_CURRENT_JOURNAL_OBSERVATION')
        transport = FakeReadTransport()
        self.read(transport)
        self.assertEqual(len(transport.calls), 19)
        staged = self.root / 'captures' / 'staging'
        self.assertLessEqual(len([p for p in staged.iterdir() if p.is_dir()]), 4)
        self.assertIsNone(journal.current_observation())

    def test_live_owner_pending_is_not_ttl_deleted(self):
        snapshots = [self.read() for _ in range(8)]
        self.now += __import__('datetime').timedelta(seconds=601)
        import os
        for snapshot in snapshots:
            path = Path(snapshot['live_capture']['bundle_root'])
            old = time.time() - 601
            os.utime(path, (old, old))
        result = self.bridge()._prune()
        self.assertTrue(all(Path(s['live_capture']['bundle_root']).exists()
                            for s in snapshots), result)
        with self.assertRaises(LiveCaptureBridgeError) as capacity:
            self.read()
        self.assertEqual(capacity.exception.code,
                         'LIVE_CAPTURE_STAGING_CAPACITY')

    def test_unreadable_journal_preserves_pending_evidence(self):
        class UnreadableJournal:
            def current_observation(self):
                raise __import__('sqlite3').OperationalError('journal unreadable')
        snapshot = self.read()
        recovered = self.bridge().recover(UnreadableJournal())
        self.assertEqual(recovered['status'], 'JOURNAL_UNREADABLE')
        self.assertTrue(Path(snapshot['live_capture']['bundle_root']).exists())
        with self.assertRaises(LiveCaptureBridgeError) as blocked:
            self.read()
        self.assertEqual(blocked.exception.code,
                         'LIVE_CAPTURE_JOURNAL_RECOVERY_BLOCKED')
        self.assertTrue(Path(snapshot['live_capture']['bundle_root']).exists())

    def test_actual_default_live_runner_uses_one_bridge_and_denies_entry(self):
        FakeCodexBrokerRead.instances.clear()
        import martin_agentic.live_session as live_session
        publications = []
        original = live_session.publish_owner_observation
        def record_publication(*args, **kwargs):
            value = original(*args, **kwargs)
            if isinstance(value, dict):
                publications.append(value)
            return value
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy), \
             patch('martin_agentic.live_session.CodexBrokerReadTransport',
                   FakeCodexBrokerRead), \
             patch('martin_agentic.live_session.collect_broker',
                   side_effect=AssertionError('parallel collector forbidden')), \
             patch('martin_agentic.live_session.collect_live_events',
                   return_value=([], {'quotes': 10})), \
             patch('martin_agentic.live_session.notify_local'), \
             patch('martin_agentic.live_session.time.sleep'), \
             patch('martin_agentic.live_session.print'), \
             patch.object(live_session, 'publish_owner_observation',
                          side_effect=record_publication):
            result = run_live_session(
                self.root / 'live.sqlite', self.policy,
                symbols=[f'S{i}' for i in range(10)], cycles=40,
                status_file=self.root / 'status.json',
                clock=lambda: self.now)
        self.assertTrue(publications, result)
        publication = publications[-1]
        self.assertIn(publication['status'],
                      {'PUBLISHED', 'ALREADY_PUBLISHED'}, publication)
        self.assertTrue(publication['committed'])
        self.assertTrue(publication['current_evidence_valid'])
        self.assertFalse(publication['entry_allowed'])
        self.assertEqual(publication['current_context']['status'], 'bound')
        self.assertTrue(result['new_entries_blocked'])
        self.assertTrue(result.get('last_completed_observation_healthy'))
        self.assertTrue(FakeCodexBrokerRead.instances)
        self.assertTrue(any(len(observed.inner.calls) == 19
                            for observed in FakeCodexBrokerRead.instances))
        self.assertTrue(all(len(observed.inner.calls) <= 19
                            for observed in FakeCodexBrokerRead.instances))

    def test_missing_bridge_config_is_structured_not_a_legacy_fallback(self):
        from martin_luk.live_session import _broker_read
        with patch('martin_agentic.live_session.CodexBrokerReadTransport'):
            with self.assertRaises(LiveCaptureBridgeError) as raised:
                _broker_read({'account_suffix': '0000'}, lambda: self.now)
        self.assertEqual(raised.exception.code, 'LIVE_CAPTURE_BRIDGE_NOT_CONFIGURED')
