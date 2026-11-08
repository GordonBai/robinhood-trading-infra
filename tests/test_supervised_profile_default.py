"""Default supervised-engineering profile composition tests.

All broker/account/quote data is a local synthetic fixture.  The real book is
used only to exercise the actual owner/Host validators; no formal tool, MCP,
broker or native write is called.
"""
from datetime import datetime, timedelta
import copy
import json
import os
from pathlib import Path
import tempfile
from unittest import TestCase
from unittest.mock import patch

from martin_agentic import engineering_permit_producer, engineering_profile
from martin_agentic.audit import digest
from martin_agentic.execution import ExecutionBook
from martin_agentic.host_execution import HostExecutionBridge
from martin_agentic.host_mailbox import (
    read_private_json, submit_engineering_intent, submit_request,
    write_private_json)
from martin_agentic.native_operations import NativeOperations
from martin_agentic.engineering_permit_producer import (
    EngineeringPermitError, produce_supervised_engineering_permit)
from martin_agentic.native_receipt_source import NativeReceiptEvidenceReader
from martin_agentic.qualification import source_fingerprint
from martin_luk.live_session import run_live_session
from test_broker import snapshot
from test_supervised_engineering import (
    FakePreflight, SupervisedBook, assessment, plan, policy, quote, request)
from martin_agentic.supervised_engineering import (
    SupervisedEngineeringAuthority)
from test_formal_review_ingress import (
    FailingEntryTransport, ScriptedEntryTransport)
from test_engineering_source import RealDryFixture
from test_historical_baseline import (
    AT as BASELINE_AT, EXPIRES as BASELINE_EXPIRES,
    evidence as baseline_evidence, historical_snapshot)
from trading_infra.historical_baseline import (
    BASELINE_SCOPE_FULL, apply_baseline, plan_baseline)

AT = '2026-10-16T14:00:00+00:00'
NOW = datetime.fromisoformat(AT)


def _write_private(path, value):
    path.write_text(json.dumps(value, sort_keys=True))
    os.chmod(path, 0o600)


class ProfileFixture:
    def __init__(self, *, enabled=True, owner_exists=True, write_request=True,
                 review_enabled=False, review_only=True, permit_enabled=False,
                 promotion_enabled=False):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.root.chmod(0o700)
        self.owner = self.root / 'owner'
        if owner_exists:
            self.owner.mkdir(mode=0o700)
        self.policy = policy()
        self.policy['supervised_engineering'] = {
            'enabled': enabled,
            'owner_root': str(self.owner),
        }
        if review_enabled:
            self.policy['supervised_engineering']['review'] = {
                'enabled': True, 'review_only': review_only,
                'deadline_seconds': 10}
        if permit_enabled:
            self.policy['supervised_engineering']['permit'] = {
                'enabled': True}
        if promotion_enabled:
            self.policy['supervised_engineering']['promotion'] = {
                'enabled': True}
        self.snap = snapshot()
        self.snap['synthetic'] = False
        self.account_key = self.snap['account_key']
        self.facts = assessment(self.policy, self.account_key)
        self.request = request(
            self.policy, self.account_key, synthetic=False,
            signal_id='engineering-default-profile')
        self.request['source']['source_fingerprint'] = source_fingerprint()
        self.request['source']['policy_sha256'] = digest(self.policy)
        if owner_exists and write_request:
            _write_private(self.owner / 'supervised-engineering-request.json',
                           self.request)
        self.book = ExecutionBook(
            self.root / 'book.sqlite', self.policy, simulation=False,
            clock=lambda: NOW, current_preflight=FakePreflight(self.facts))
        state = self.book.state()
        state['account_key'] = self.account_key
        self.book._save(state)
        self.book.db.commit()
        self.bridge = None

    def close(self):
        if self.bridge is not None:
            self.bridge.close()
        self.book.close()
        self.temp.cleanup()


class SupervisedProfileDefaultTests(TestCase):
    def test_default_host_bridge_consumes_profile_and_reaches_review_pending(self):
        fixture = ProfileFixture()
        self.addCleanup(fixture.close)
        fixture.bridge = HostExecutionBridge(
            fixture.book, fixture.root / 'host')
        authority = fixture.book.supervised_engineering_authority
        self.assertIsNotNone(authority)
        self.assertIs(fixture.book.formal_authority, authority)
        self.assertIs(fixture.book._current_preflight.formal_authority,
                      authority)
        self.assertFalse(authority.real_activation_enabled)
        self.assertEqual(
            fixture.book.supervised_engineering_profile['real_activation_enabled'],
            False)
        submit_engineering_intent(
            fixture.bridge.directory, 'engineering-default-profile',
            plan(), {
                'synthetic': False, 'source_sha256': 'fixture',
                'engineering_scope_authorized': True,
                'parameters_user_specified': True,
                'parameters_valid_until': (
                    NOW.replace(year=NOW.year + 1)).isoformat()},
            clock=lambda: NOW)
        output = fixture.bridge(
            snapshot=fixture.snap, quotes={'DEMO': quote()},
            allow_entries=False)
        self.assertEqual(output['status'], 'HOST_ENGINEERING_INTENT_RECORDED',
                         output)
        self.assertEqual(output['state'], 'REVIEW_PENDING')
        self.assertFalse(output['entry_authorized'])
        self.assertFalse(output['execution_authorized'])
        action_id = output['action_id']
        action = next(a for a in fixture.book.actions()
                      if a['id'] == action_id)
        self.assertEqual(action['state'], 'REVIEW_PENDING')
        with self.assertRaisesRegex(
                ValueError,
                'SUPERVISED_ENGINEERING_REAL_ACTIVATION_DISABLED'):
            fixture.book.promote_supervised_entry(action_id)
        self.assertEqual(next(a for a in fixture.book.actions()
                              if a['id'] == action_id)['state'],
                         'REVIEW_PENDING')

    def test_default_profile_runs_one_review_only_and_stops_at_native_confirmation(self):
        ScriptedEntryTransport.constructed = 0
        fixture = ProfileFixture(review_enabled=True)
        self.addCleanup(fixture.close)
        fixture.bridge = HostExecutionBridge(
            fixture.book, fixture.root / 'host',
            review_factory=ScriptedEntryTransport)
        self.assertIsNotNone(fixture.bridge.engineering_review)
        submit_engineering_intent(
            fixture.bridge.directory, 'engineering-default-profile',
            plan(), {
                'synthetic': False, 'source_sha256': 'fixture',
                'engineering_scope_authorized': True,
                'parameters_user_specified': True,
                'parameters_valid_until': (
                    NOW.replace(year=NOW.year + 1)).isoformat()},
            clock=lambda: NOW)
        first = fixture.bridge(
            snapshot=fixture.snap, quotes={'DEMO': quote()},
            allow_entries=False)
        self.assertEqual(first['status'], 'HOST_ENGINEERING_INTENT_RECORDED')
        action_id = first['action_id']
        self.assertEqual(next(a for a in fixture.book.actions()
                              if a['id'] == action_id)['state'],
                         'REVIEW_PENDING')
        request = submit_request(
            fixture.bridge.directory, action_id, 'review', clock=lambda: NOW)
        output = fixture.bridge(
            snapshot=fixture.snap, quotes={'DEMO': quote()},
            allow_entries=False)
        self.assertEqual(output['status'], 'WAITING_NATIVE_CONFIRMATION',
                         output)
        self.assertEqual(output['review_status'], 'WAITING_USER')
        self.assertTrue(output['native_confirmation_required'])
        self.assertFalse(output['entry_authorized'])
        self.assertFalse(output['execution_authorized'])
        self.assertFalse(output['broker_effect_verified'])
        self.assertEqual(ScriptedEntryTransport.constructed, 1)
        review = output['review_result']
        self.assertEqual(review['status'], 'WAITING_USER')
        self.assertTrue(review['review_verified'])
        self.assertFalse(review['execution_authorized'])
        operation_id = review['operation_id']
        action = next(a for a in fixture.book.actions()
                      if a['id'] == action_id)
        self.assertEqual(action['state'], 'WAITING_USER')
        row = fixture.book.db.execute(
            'SELECT state,result FROM native_operation_records WHERE id=?',
            (operation_id,)).fetchone()
        self.assertEqual(row[0], 'RECORDED')
        self.assertTrue(json.loads(row[1])['review_verified'])
        self.assertTrue((fixture.bridge.directory / 'receipts' /
                         (operation_id + '.json')).is_file())
        self.assertTrue((fixture.bridge.directory / 'review-ingress' /
                         (operation_id + '.json')).is_file())
        binding = review['host_import']['binding']
        packet_row = fixture.book.db.execute(
            'SELECT payload FROM native_operation_records WHERE id=?',
            (operation_id,)).fetchone()
        packet_parameters = json.loads(packet_row[0])['parameters']
        permit = {
            'action_id': action_id,
            'account_key': binding['account_key'],
            'account_suffix': '0000',
            'source': {
                'policy_sha256': binding['policy_sha256'],
                'source_fingerprint': binding['source_fingerprint']},
            'formal': {
                'review_operation_id': operation_id,
                'review_parameters_sha256': digest(packet_parameters)}}
        # No ingress_root: this is the raw Host receipt reader path.  The
        # scripted transport is deliberately non-formal, so it must be labelled
        # host_submitted_response rather than formal_runtime_transport.
        evidence = NativeReceiptEvidenceReader(
            fixture.bridge.directory,
            clock=lambda: NOW).read(fixture.book, {
                'permit': permit,
                'review': {'operation_id': operation_id}})
        self.assertEqual(evidence['origin'], 'host_submitted_response')
        self.assertEqual(evidence['native_status'], 'needs_confirmation')
        self.assertFalse(evidence['broker_effect_verified'])
        result_file = fixture.bridge.directory / 'results' / (
            request['request_id'] + '.json')
        self.assertTrue(result_file.is_file())
        self.assertEqual(read_private_json(result_file)['status'],
                         'HOST_RECEIPT_IMPORTED')
        # Repeating the tick must not dispatch another review.
        again = fixture.bridge(
            snapshot=fixture.snap, quotes={'DEMO': quote()},
            allow_entries=False)
        self.assertNotEqual(again.get('status'), 'WAITING_NATIVE_CONFIRMATION')
        self.assertEqual(ScriptedEntryTransport.constructed, 1)
        self.assertEqual(next(a for a in fixture.book.actions()
                              if a['id'] == action_id)['state'],
                         'WAITING_USER')

    def test_post_import_final_summary_failure_keeps_imported_stage(self):
        ScriptedEntryTransport.constructed = 0
        fixture = ProfileFixture(review_enabled=True)
        self.addCleanup(fixture.close)
        fixture.bridge = HostExecutionBridge(
            fixture.book, fixture.root / 'host',
            review_factory=ScriptedEntryTransport)
        submit_engineering_intent(
            fixture.bridge.directory, 'engineering-default-profile',
            plan(), {
                'synthetic': False, 'source_sha256': 'fixture',
                'engineering_scope_authorized': True,
                'parameters_user_specified': True,
                'parameters_valid_until': (
                    NOW.replace(year=NOW.year + 1)).isoformat()},
            clock=lambda: NOW)
        first = fixture.bridge(
            snapshot=fixture.snap, quotes={'DEMO': quote()},
            allow_entries=False)
        action_id = first['action_id']
        request = submit_request(
            fixture.bridge.directory, action_id, 'review', clock=lambda: NOW)
        with patch.object(fixture.bridge, '_finalize_review_run',
                          side_effect=OSError('SIMULATED_POST_IMPORT_RESULT_FAILURE')):
            output = fixture.bridge(
                snapshot=fixture.snap, quotes={'DEMO': quote()},
                allow_entries=False)
        self.assertEqual(output['status'], 'REVIEW_RESULT_RECOVERY_REQUIRED',
                         output)
        self.assertEqual(output['stage'], 'IMPORTED_FINAL_RESULT_FAILED')
        self.assertEqual(output['review_status'], 'RECORDED')
        self.assertTrue(output['recovery_required'])
        self.assertEqual(ScriptedEntryTransport.constructed, 1)
        self.assertEqual(next(a for a in fixture.book.actions()
                              if a['id'] == action_id)['state'],
                         'WAITING_USER')
        rows = fixture.book.db.execute(
            "SELECT operation,state FROM native_operation_records "
            "WHERE operation='review'").fetchall()
        self.assertEqual(rows, [('review', 'RECORDED')])
        self.assertTrue((fixture.bridge.directory / 'results' /
                         (request['request_id'] + '.json')).is_file())
        again = fixture.bridge(
            snapshot=fixture.snap, quotes={'DEMO': quote()},
            allow_entries=False)
        self.assertEqual(ScriptedEntryTransport.constructed, 1)
        self.assertNotEqual(again.get('status'), 'WAITING_NATIVE_CONFIRMATION')
        self.assertEqual(next(a for a in fixture.book.actions()
                              if a['id'] == action_id)['state'],
                         'WAITING_USER')

    def test_started_native_reservation_is_recovery_not_not_dispatched(self):
        fixture = ProfileFixture(review_enabled=True)
        self.addCleanup(fixture.close)
        fixture.bridge = HostExecutionBridge(
            fixture.book, fixture.root / 'host',
            review_factory=ScriptedEntryTransport)
        submit_engineering_intent(
            fixture.bridge.directory, 'engineering-default-profile',
            plan(), {
                'synthetic': False, 'source_sha256': 'fixture',
                'engineering_scope_authorized': True,
                'parameters_user_specified': True,
                'parameters_valid_until': (
                    NOW.replace(year=NOW.year + 1)).isoformat()},
            clock=lambda: NOW)
        first = fixture.bridge(
            snapshot=fixture.snap, quotes={'DEMO': quote()},
            allow_entries=False)
        submit_request(
            fixture.bridge.directory, first['action_id'], 'review',
            clock=lambda: NOW)
        with patch.object(fixture.bridge, 'run_prepared_review',
                          side_effect=ValueError('OWNED_REVIEW_DEADLINE_EXCEEDED')):
            output = fixture.bridge(
                snapshot=fixture.snap, quotes={'DEMO': quote()},
                allow_entries=False)
        self.assertEqual(output['status'], 'REVIEW_RECOVERY_REQUIRED', output)
        self.assertEqual(output['stage'], 'PROVIDER_MAY_HAVE_BEEN_CALLED')
        self.assertEqual(output['review_status'], 'UNKNOWN')
        self.assertTrue(output['recovery_required'])
        self.assertNotEqual(output['review_status'], 'NOT_DISPATCHED')

    def test_absent_evidence_is_unknown_not_pre_dispatch(self):
        fixture = ProfileFixture(review_enabled=True)
        self.addCleanup(fixture.close)
        fixture.bridge = HostExecutionBridge(
            fixture.book, fixture.root / 'host',
            review_factory=ScriptedEntryTransport)
        prepared = {'operation_id': 'missing-operation',
                    'action_id': 'missing-action',
                    'request_id': 'missing-request'}
        NativeOperations(fixture.book)
        stage = fixture.bridge._review_failure_stage(prepared)
        self.assertEqual(stage['status'], 'REVIEW_RECOVERY_REQUIRED')
        self.assertEqual(stage['stage'], 'DISPATCHED_OR_UNKNOWN')
        self.assertEqual(stage['review_status'], 'UNKNOWN')
        self.assertTrue(stage['recovery_required'])
        self.assertNotEqual(stage['review_status'], 'NOT_DISPATCHED')
        proven = fixture.bridge._review_failure_stage(
            prepared, pre_dispatch_proven=True)
        self.assertEqual(proven['status'], 'ENGINEERING_REVIEW_REJECTED')
        self.assertEqual(proven['stage'], 'PRE_DISPATCH_PROVEN')
        self.assertEqual(proven['review_status'], 'NOT_DISPATCHED')
        self.assertFalse(proven['recovery_required'])

    def test_unreadable_store_and_result_only_are_recovery(self):
        fixture = ProfileFixture(review_enabled=True)
        self.addCleanup(fixture.close)
        fixture.bridge = HostExecutionBridge(
            fixture.book, fixture.root / 'host',
            review_factory=ScriptedEntryTransport)
        prepared = {'operation_id': 'missing-operation',
                    'action_id': 'missing-action',
                    'request_id': 'missing-request'}
        real_db = fixture.book.db
        class Unreadable:
            def execute(self, *args, **kwargs):
                raise OSError('SIMULATED_DB_READ_FAILURE')
        fixture.book.db = Unreadable()
        try:
            stage = fixture.bridge._review_failure_stage(prepared)
        finally:
            fixture.book.db = real_db
        self.assertEqual(stage['status'], 'REVIEW_RECOVERY_REQUIRED')
        self.assertEqual(stage['stage'], 'DURABLE_EVIDENCE_UNREADABLE')
        self.assertEqual(stage['review_status'], 'UNKNOWN')
        self.assertTrue(stage['recovery_required'])
        NativeOperations(fixture.book)
        result_path = (fixture.bridge.directory / 'results' /
                       'missing-request.json')
        result_path.parent.mkdir(parents=True, exist_ok=True)
        result_path.write_text('{}')
        stage = fixture.bridge._review_failure_stage(prepared)
        self.assertEqual(stage['status'], 'REVIEW_RECOVERY_REQUIRED')
        self.assertEqual(stage['stage'], 'DISPATCHED_OR_UNKNOWN')
        self.assertTrue(stage['recovery_required'])
        self.assertNotEqual(stage['review_status'], 'NOT_DISPATCHED')

    def test_classifier_exception_does_not_mask_original_review_error(self):
        fixture = ProfileFixture(review_enabled=True)
        self.addCleanup(fixture.close)
        fixture.bridge = HostExecutionBridge(
            fixture.book, fixture.root / 'host',
            review_factory=ScriptedEntryTransport)
        prepared = {'operation_id': 'missing-operation',
                    'action_id': 'missing-action',
                    'request_id': 'missing-request'}
        request = {'request_id': 'missing-request'}
        with patch.object(fixture.bridge, 'run_prepared_review',
                          side_effect=ValueError('OWNED_REVIEW_DEADLINE_EXCEEDED')), \
                patch.object(fixture.bridge, '_review_failure_stage',
                             side_effect=RuntimeError('SIMULATED_CLASSIFIER_FAILURE')):
            output = fixture.bridge._run_engineering_review(request, prepared)
        self.assertEqual(output['status'], 'REVIEW_RECOVERY_REQUIRED')
        self.assertEqual(output['stage'], 'CLASSIFIER_FAILED')
        self.assertEqual(output['review_status'], 'UNKNOWN')
        self.assertTrue(output['recovery_required'])
        self.assertEqual(output['reason'], 'OWNED_REVIEW_DEADLINE_EXCEEDED')

    def test_default_host_issues_permit_from_real_dry_source_fixture(self):
        fixture = RealDryFixture(prebuilt_intent=True, activate=True)
        bridge = None
        try:
            permit_path = fixture.owner / 'supervised-engineering-permit.json'
            permit_path.unlink()
            fixture.book.supervised_engineering_profile = {
                'kind': 'supervised_engineering_profile_v1',
                'enabled': True,
                'owner_root': str(fixture.owner),
                'review': None,
                'permit': {'enabled': True},
                'real_activation_enabled': True,
            }
            bridge = HostExecutionBridge(
                fixture.book, fixture.root / 'host',
                review_factory=ScriptedEntryTransport)
            output = bridge(
                snapshot=fixture.snap, quotes={'DEMO': fixture._quote()},
                allow_entries=False)
            self.assertEqual(output['status'], 'ENGINEERING_PERMIT_ISSUED',
                             output)
            self.assertTrue(output['permits_issued'])
            self.assertFalse(output['entry_authorized'])
            self.assertFalse(output['execution_authorized'])
            self.assertIsNotNone(output['permit_id'])
            self.assertTrue(permit_path.is_file())
            permit = json.loads(permit_path.read_text())
            self.assertEqual(permit['permit_id'], output['permit_id'])
            self.assertEqual(permit['action_id'], fixture.action_id)
            action = next(a for a in fixture.book.actions()
                          if a['id'] == fixture.action_id)
            self.assertEqual(action['state'], 'WAITING_USER')
            first = permit_path.read_bytes()
            again = bridge(
                snapshot=fixture.snap, quotes={'DEMO': fixture._quote()},
                allow_entries=False)
            self.assertNotEqual(again.get('status'), 'ENGINEERING_PERMIT_ISSUED')
            self.assertEqual(permit_path.read_bytes(), first)
        finally:
            if bridge is not None:
                bridge.close()
            fixture.close()

    def _full_entry_baseline_fixture(self):
        """Build a legal APPLIED full local baseline, then one reviewed intent.

        This is a test-only local fixture.  It uses the production historical
        baseline planner/applier and a complete local snapshot/evidence shape;
        it is not a formal broker or release proof.
        """
        fixture = RealDryFixture(prebuilt_intent=False, activate=True)
        try:
            policy_value = fixture.book.policy
            account_key = fixture.account_key
            state = fixture.book.state()
            state['account_key'] = account_key
            fixture.book._save(state)
            fixture.book.db.commit()
            evidence_value = baseline_evidence(account_key)
            for group in ('source', 'lineage'):
                evidence_value[group]['policy_sha256'] = digest(policy_value)
                evidence_value[group]['source_fingerprint'] = source_fingerprint()
            snapshot_value = historical_snapshot()
            manifest = plan_baseline(
                snapshot_value, account_key=account_key,
                cutoff_utc=BASELINE_AT,
                policy_sha256=digest(policy_value),
                source_fingerprint=source_fingerprint(),
                evidence=evidence_value, expires_at=BASELINE_EXPIRES,
                now=NOW)
            manifest['baseline_scope'] = BASELINE_SCOPE_FULL
            manifest['scope_id'] = digest([
                'historical-baseline-scope-1', manifest['account_key'],
                manifest['provider'], manifest['policy_sha256'],
                manifest['source_fingerprint'], manifest['cutoff_utc'],
                manifest['evidence']['source']['execution_db_sha256'],
                manifest['evidence']['risk']['sidecar_sha256'],
                manifest['evidence']['host']['manifest_sha256'],
                manifest['evidence']['lineage']['head_sha256'],
                manifest['baseline_scope']])
            manifest['baseline_id'] = digest([
                'historical-baseline-1', manifest['account_key'],
                manifest['provider'], manifest['scope_id'],
                manifest['cutoff_utc'], manifest['snapshot_sha256']])
            manifest['manifest_sha256'] = digest({
                key: value for key, value in manifest.items()
                if key != 'manifest_sha256'})
            applied = apply_baseline(
                fixture.book.db, manifest,
                manifest_pin=manifest['manifest_sha256'],
                approval_scope='fixture-full-baseline', now=NOW,
                evidence=evidence_value, snapshot=snapshot_value)
            self.assertEqual(applied['status'], 'APPLIED')
            result = fixture.book.record_supervised_engineering_intent(
                'engineering-real-dry', plan(), quote(), fixture.snap,
                evidence={
                    'synthetic': False, 'source_sha256': 'fixture',
                    'engineering_scope_authorized': True,
                    'parameters_user_specified': True,
                    'parameters_valid_until': (
                        NOW.replace(year=NOW.year + 1)).isoformat()})
            action_id = result['action_id']
            ops = NativeOperations(fixture.book)
            packet = ops.begin(action_id, 'review', fixture.snap,
                               quote=fixture._quote())
            ops.record(packet['operation_id'], {'data': {
                **packet['parameters'], 'order_checks': {},
                'customer_approval_required': False,
                'market_data_disclosure': 'Exact disclosure',
                'quote_data': {}}})
            fixture.action_id = action_id
            fixture.packet = packet
            fixture.book.refresh_current_account_assessment(
                purpose='engineering')
            return fixture
        except Exception:
            fixture.close()
            raise

    def _full_entry_marker(self, fixture):
        fixture.book.db.execute(
            "CREATE TABLE IF NOT EXISTS entry_authority_marker("
            "required INTEGER,reason TEXT,baseline_id TEXT,"
            "baseline_scope TEXT,created_at TEXT)")
        fixture.book.db.execute(
            'INSERT INTO entry_authority_marker VALUES(1,?,?,?,?)',
            ('BASELINE_APPLIED', 'fixture-full',
             'full_account_baseline_v1', fixture._clock().isoformat()))
        fixture.book.db.commit()

    def test_marker_only_full_facts_is_not_a_baseline_grant(self):
        fixture = RealDryFixture(prebuilt_intent=True, activate=True)
        try:
            self._full_entry_marker(fixture)
            action = next(a for a in fixture.book.actions()
                          if a['id'] == fixture.action_id)
            decision = fixture.book.entry_authority.check(
                source='073-marker', action=action, purpose='engineering')
            self.assertFalse(decision['allowed'], decision)
            self.assertEqual(decision['reason'],
                             'ENTRY_AUTHORITY_BASELINE_MISSING')
            for reason in ('BASELINE_REVOKED', 'UNKNOWN_REASON'):
                with self.subTest(reason=reason):
                    fixture.book.db.execute(
                        'UPDATE entry_authority_marker SET reason=?',
                        (reason,))
                    fixture.book.db.commit()
                    denied = fixture.book.entry_authority.check(
                        source='073-marker', action=action,
                        purpose='engineering')
                    self.assertFalse(denied['allowed'], denied)
                    self.assertIn('BASELINE_', denied['reason'])
        finally:
            fixture.close()

    def test_full_facts_ready_without_supervised_permit_still_blocked(self):
        fixture = self._full_entry_baseline_fixture()
        try:
            action = next(a for a in fixture.book.actions()
                          if a['id'] == fixture.action_id)
            decision = fixture.book.entry_authority.check(
                source='073-test', action=action, purpose='engineering')
            self.assertFalse(decision['allowed'], decision)
            self.assertTrue(decision['facts_ready'])
            self.assertTrue(decision['permit_missing'])
            self.assertFalse(decision['entry_authorized'])
        finally:
            fixture.close()

    def test_full_facts_ready_with_exact_engineering_permit_crosses_outer_gate(self):
        fixture = self._full_entry_baseline_fixture()
        try:
            issued = produce_supervised_engineering_permit(
                fixture.book, fixture.action_id)
            self.assertEqual(issued['status'], 'ISSUED', issued)
            action = next(a for a in fixture.book.actions()
                          if a['id'] == fixture.action_id)
            decision = fixture.book.entry_authority.check(
                source='073-test', action=action, purpose='engineering')
            self.assertTrue(decision['allowed'], decision)
            self.assertTrue(decision['facts_ready'])
            self.assertTrue(decision['permit_verified'])
            self.assertFalse(decision['permit_missing'])
            self.assertEqual(decision['reason'],
                             'SUPERVISED_ENGINEERING_PERMIT_VERIFIED')
            self.assertFalse(decision['entry_authorized'])
            with self.assertRaisesRegex(
                    ValueError,
                    'CURRENT_ENGINEERING_PREFLIGHT_BLOCKED:'
                    'FORMAL_ENGINEERING_CHANNEL_UNPROVEN'):
                fixture.book.promote_supervised_entry(fixture.action_id)
            self.assertEqual(next(a for a in fixture.book.actions()
                                  if a['id'] == fixture.action_id)['state'],
                             'WAITING_USER')
            self.assertEqual(self.consumption_count(fixture.book), 0)
        finally:
            fixture.close()

    def test_full_facts_ready_rejects_changed_or_revoked_permit(self):
        for invalidation in ('wrong_ref', 'revoked'):
            with self.subTest(invalidation=invalidation):
                fixture = self._full_entry_baseline_fixture()
                try:
                    issued = produce_supervised_engineering_permit(
                        fixture.book, fixture.action_id)
                    self.assertEqual(issued['status'], 'ISSUED', issued)
                    permit_path = (fixture.owner /
                                   'supervised-engineering-permit.json')
                    permit = json.loads(permit_path.read_text())
                    if invalidation == 'wrong_ref':
                        permit['ref_id'] = 'wrong-ref'
                    else:
                        permit['revoked_at'] = fixture._clock().isoformat()
                    permit['permit_id'] = digest({
                        **permit, 'permit_id': None})
                    _write_private(permit_path, permit)
                    action = next(a for a in fixture.book.actions()
                                  if a['id'] == fixture.action_id)
                    decision = fixture.book.entry_authority.check(
                        source='073-test', action=action,
                        purpose='engineering')
                    self.assertFalse(decision['allowed'], decision)
                    self.assertTrue(decision['facts_ready'])
                    self.assertFalse(decision['entry_authorized'])
                finally:
                    fixture.close()

    def test_final_entry_recheck_uses_prevalidated_cache_without_external_read(self):
        fixture = self._full_entry_baseline_fixture()
        try:
            issued = produce_supervised_engineering_permit(
                fixture.book, fixture.action_id)
            self.assertEqual(issued['status'], 'ISSUED', issued)
            action = next(a for a in fixture.book.actions()
                          if a['id'] == fixture.action_id)
            fixture.book._begin_supervised_entry_prevalidation(action)
            first = fixture.book.entry_authority.check(
                source='ExecutionBook.promote_supervised_entry',
                action=action, purpose='engineering')
            self.assertTrue(first['allowed'], first)
            with patch.object(
                    fixture.book.supervised_engineering_authority,
                    'validate_current_permit',
                    side_effect=AssertionError('external source read')):
                final = fixture.book.entry_authority.check(
                    source='ExecutionBook.promote_supervised_entry.final',
                    action=action, purpose='engineering')
            self.assertTrue(final['allowed'], final)
            self.assertTrue(final['permit_cached'])
            self.assertEqual(final['reason'],
                             'SUPERVISED_ENGINEERING_PERMIT_VERIFIED')
        finally:
            fixture.book._clear_supervised_entry_context()
            fixture.close()

    def test_claim_source_never_runs_supervised_source_validation_in_transaction(self):
        fixture = self._full_entry_baseline_fixture()
        try:
            issued = produce_supervised_engineering_permit(
                fixture.book, fixture.action_id)
            self.assertEqual(issued['status'], 'ISSUED', issued)
            action = next(a for a in fixture.book.actions()
                          if a['id'] == fixture.action_id)
            calls = []
            with patch.object(
                    fixture.book.supervised_engineering_authority,
                    'validate_current_permit',
                    side_effect=lambda **kwargs: calls.append(kwargs)):
                fixture.book._begin()
                try:
                    decision = fixture.book.entry_authority.check(
                        source='ExecutionBook.claim', action=action,
                        purpose='engineering')
                finally:
                    fixture.book.db.rollback()
            self.assertFalse(decision['allowed'], decision)
            self.assertEqual(decision['reason'],
                             'ENTRY_AUTHORITY_SUPERVISED_PERMIT_NOT_PREVALIDATED')
            self.assertEqual(calls, [])
        finally:
            fixture.close()

    def test_final_without_trusted_context_is_not_prevalidated(self):
        fixture = self._full_entry_baseline_fixture()
        try:
            issued = produce_supervised_engineering_permit(
                fixture.book, fixture.action_id)
            self.assertEqual(issued['status'], 'ISSUED', issued)
            action = next(a for a in fixture.book.actions()
                          if a['id'] == fixture.action_id)
            decision = fixture.book.entry_authority.check(
                source='ExecutionBook.promote_supervised_entry.final',
                action=action, purpose='engineering')
            self.assertFalse(decision['allowed'], decision)
            self.assertEqual(decision['reason'],
                             'ENTRY_AUTHORITY_SUPERVISED_PERMIT_NOT_PREVALIDATED')
        finally:
            fixture.close()

    def test_expired_or_wrong_context_is_not_reused(self):
        fixture = self._full_entry_baseline_fixture()
        try:
            issued = produce_supervised_engineering_permit(
                fixture.book, fixture.action_id)
            self.assertEqual(issued['status'], 'ISSUED', issued)
            action = next(a for a in fixture.book.actions()
                          if a['id'] == fixture.action_id)
            fixture.book._begin_supervised_entry_prevalidation(action)
            fixture.book.entry_authority.check(
                source='ExecutionBook.promote_supervised_entry',
                action=action, purpose='engineering')
            fixture.advance(11)
            expired = fixture.book.entry_authority.check(
                source='ExecutionBook.promote_supervised_entry.final',
                action=action, purpose='engineering')
            self.assertFalse(expired['allowed'], expired)
            self.assertEqual(expired['reason'],
                             'ENTRY_AUTHORITY_SUPERVISED_PERMIT_CONTEXT_INVALID')
            other = dict(action)
            other['payload'] = json.dumps({**json.loads(action['payload']),
                                           'ref': 'changed'})
            changed = fixture.book.entry_authority.check(
                source='ExecutionBook.promote_supervised_entry.final',
                action=other, purpose='engineering')
            self.assertFalse(changed['allowed'], changed)
        finally:
            fixture.book._clear_supervised_entry_context()
            fixture.close()

    def test_failed_promotion_clears_prevalidation_context(self):
        fixture = self._full_entry_baseline_fixture()
        try:
            with self.assertRaises(ValueError):
                fixture.book.promote_supervised_entry(fixture.action_id)
            self.assertIsNone(fixture.book._supervised_entry_context)
        finally:
            fixture.close()

    def test_legal_baseline_marker_anomalies_stay_blocked(self):
        for mutation in ('revoked', 'unknown', 'identity', 'drop_schema',
                         'corrupt_manifest'):
            with self.subTest(mutation=mutation):
                fixture = self._full_entry_baseline_fixture()
                try:
                    issued = produce_supervised_engineering_permit(
                        fixture.book, fixture.action_id)
                    self.assertEqual(issued['status'], 'ISSUED', issued)
                    if mutation == 'revoked':
                        fixture.book.db.execute(
                            "UPDATE entry_authority_marker SET reason='BASELINE_REVOKED'")
                    elif mutation == 'unknown':
                        fixture.book.db.execute(
                            "UPDATE entry_authority_marker SET reason='UNKNOWN_REASON'")
                    elif mutation == 'identity':
                        fixture.book.db.execute(
                            "UPDATE entry_authority_marker SET baseline_scope='terminal_equity_order_identity_v1'")
                    elif mutation == 'drop_schema':
                        fixture.book.db.execute(
                            'DROP TABLE historical_baseline_manifest')
                    else:
                        fixture.book.db.execute(
                            "UPDATE historical_baseline_manifest SET manifest_json='{}'")
                    fixture.book.db.commit()
                    action = next(a for a in fixture.book.actions()
                                  if a['id'] == fixture.action_id)
                    decision = fixture.book.entry_authority.check(
                        source='073-test', action=action,
                        purpose='engineering')
                    self.assertFalse(decision['allowed'], decision)
                finally:
                    fixture.close()

    def test_claim_and_native_operation_contexts_are_one_shot_and_transaction_bounded(self):
        for operation, pre_source, final_source in (
                ('claim', 'ExecutionBook.claim.prevalidate',
                 'ExecutionBook.claim'),
                ('review', 'ExecutionBook.prepare_external_action.review',
                 'ExecutionBook.prepare_external_action.review'),
                ('place', 'ExecutionBook.prepare_external_action.place',
                 'ExecutionBook.prepare_external_action.place')):
            with self.subTest(operation=operation):
                fixture = self._full_entry_baseline_fixture()
                try:
                    issued = produce_supervised_engineering_permit(
                        fixture.book, fixture.action_id)
                    self.assertEqual(issued['status'], 'ISSUED', issued)
                    action = next(a for a in fixture.book.actions()
                                  if a['id'] == fixture.action_id)
                    fixture.book._begin_entry_operation_context(action, operation)
                    first = fixture.book.entry_authority.check(
                        source=pre_source, action=action,
                        purpose='engineering')
                    self.assertTrue(first['allowed'], first)
                    with patch.object(
                            fixture.book.supervised_engineering_authority,
                            'validate_current_permit',
                            side_effect=AssertionError('external source read')):
                        fixture.book._begin()
                        try:
                            final = fixture.book.entry_authority.check(
                                source=final_source, action=action,
                                purpose='engineering')
                        finally:
                            fixture.book.db.rollback()
                    self.assertTrue(final['allowed'], final)
                    self.assertTrue(final['permit_cached'])
                    fixture.advance(11)
                    expired = fixture.book.entry_authority.check(
                        source=final_source, action=action,
                        purpose='engineering')
                    self.assertFalse(expired['allowed'], expired)
                    self.assertEqual(
                        expired['reason'],
                        'ENTRY_AUTHORITY_SUPERVISED_PERMIT_CONTEXT_INVALID')
                finally:
                    fixture.book._clear_entry_operation_contexts()
                    fixture.close()

    def _short_request(self, fixture, seconds=2):
        path = fixture.owner / 'supervised-engineering-request.json'
        value = json.loads(path.read_text())
        value['valid_until'] = (
            fixture._clock() + timedelta(seconds=seconds)).isoformat()
        _write_private(path, value)

    def test_context_never_extends_two_second_owner_window(self):
        fixture = self._full_entry_baseline_fixture()
        try:
            self._short_request(fixture)
            issued = produce_supervised_engineering_permit(
                fixture.book, fixture.action_id)
            self.assertEqual(issued['status'], 'ISSUED', issued)
            action = next(a for a in fixture.book.actions()
                          if a['id'] == fixture.action_id)
            fixture.book._begin_entry_operation_context(action, 'claim')
            first = fixture.book.entry_authority.check(
                source='ExecutionBook.claim.prevalidate', action=action,
                purpose='engineering')
            self.assertTrue(first['allowed'], first)
            context = fixture.book._entry_operation_contexts[
                fixture.action_id]['claim']
            permit_until = datetime.fromisoformat(
                issued['permit']['valid_until'])
            self.assertEqual(context['expires_at'], permit_until)
            self.assertLessEqual(
                context['expires_at'], context['created_at'] +
                timedelta(seconds=10))
            fixture.advance(3)
            fixture.book._begin()
            try:
                final = fixture.book.entry_authority.check(
                    source='ExecutionBook.claim', action=action,
                    purpose='engineering')
            finally:
                fixture.book.db.rollback()
            self.assertFalse(final['allowed'], final)
            self.assertEqual(
                final['reason'],
                'ENTRY_AUTHORITY_SUPERVISED_PERMIT_CONTEXT_INVALID')
            with self.assertRaisesRegex(
                    Exception, 'SUPERVISED_ENGINEERING_REQUEST_WINDOW'):
                fixture.book.supervised_engineering_authority.validate_current_permit(
                    fixture.book, action=action, purpose='engineering')
        finally:
            fixture.book._clear_entry_operation_contexts()
            fixture.close()

    def test_context_window_uses_each_shortest_original_right(self):
        fixture = self._full_entry_baseline_fixture()
        try:
            created = fixture._clock()
            long = created + timedelta(seconds=9)
            base_result = {
                'permit': {'valid_until': long,
                           'not_before': created - timedelta(seconds=1)},
                'request': {'valid_until': long,
                            'not_before': created - timedelta(seconds=1)},
                'facts': {'observation_valid_until': long,
                          'observed_at': created - timedelta(seconds=1)},
                'review': {'recorded_at': created - timedelta(seconds=1)},
                'real_source_evidence': {
                    'valid_until': long, 'risk_valid_until': long},
            }
            action = {'payload': json.dumps({
                'expires_at': long.isoformat()})}
            expected = created + timedelta(seconds=2)
            for field in ('permit', 'request', 'facts', 'review',
                          'evidence', 'risk', 'action', 'receipt'):
                with self.subTest(field=field):
                    result = copy.deepcopy(base_result)
                    action_value = {'payload': action['payload']}
                    if field == 'permit':
                        result['permit']['valid_until'] = expected
                    elif field == 'request':
                        result['request']['valid_until'] = expected
                    elif field == 'facts':
                        result['facts']['observation_valid_until'] = expected
                    elif field == 'review':
                        result['review']['recorded_at'] = (
                            expected - timedelta(seconds=30))
                    elif field == 'evidence':
                        result['real_source_evidence']['valid_until'] = expected
                    elif field == 'risk':
                        result['real_source_evidence'][
                            'risk_valid_until'] = expected
                    elif field == 'action':
                        action_value = {'payload': json.dumps({
                            'expires_at': expected.isoformat()})}
                    else:
                        result['assessment_receipt'] = {
                            'valid_until': expected}
                    window = fixture.book._entry_context_window(
                        result, action_value, created)
                    self.assertIsNotNone(window)
                    self.assertEqual(window['expires_at'], expected)
                    self.assertLessEqual(window['not_before'], expected)

            bad = dict(base_result)
            bad['permit'] = {'valid_until': '2026-10-16T14:00:09',
                             'not_before': created}
            self.assertIsNone(
                fixture.book._entry_context_window(bad, action, created))
        finally:
            fixture.close()

    def test_context_exact_boundary_and_clock_rollback_fail_closed(self):
        fixture = self._full_entry_baseline_fixture()
        try:
            self._short_request(fixture, seconds=2)
            issued = produce_supervised_engineering_permit(
                fixture.book, fixture.action_id)
            self.assertEqual(issued['status'], 'ISSUED', issued)
            action = next(a for a in fixture.book.actions()
                          if a['id'] == fixture.action_id)
            fixture.book._begin_entry_operation_context(action, 'claim')
            first = fixture.book.entry_authority.check(
                source='ExecutionBook.claim.prevalidate', action=action,
                purpose='engineering')
            self.assertTrue(first['allowed'], first)
            fixture.advance(2)
            fixture.book._begin()
            try:
                boundary = fixture.book.entry_authority.check(
                    source='ExecutionBook.claim', action=action,
                    purpose='engineering')
            finally:
                fixture.book.db.rollback()
            self.assertFalse(boundary['allowed'], boundary)
            self.assertEqual(
                boundary['reason'],
                'ENTRY_AUTHORITY_SUPERVISED_PERMIT_CONTEXT_INVALID')
            fixture._clock_value[0] = fixture._clock() - timedelta(seconds=1)
            rolled = fixture.book.entry_authority.check(
                source='ExecutionBook.claim', action=action,
                purpose='engineering')
            self.assertFalse(rolled['allowed'], rolled)
            self.assertEqual(
                rolled['reason'],
                'ENTRY_AUTHORITY_SUPERVISED_PERMIT_CONTEXT_INVALID')
        finally:
            fixture.book._clear_entry_operation_contexts()
            fixture.close()

    def test_slow_validation_does_not_refresh_context_window(self):
        fixture = self._full_entry_baseline_fixture()
        try:
            self._short_request(fixture)
            issued = produce_supervised_engineering_permit(
                fixture.book, fixture.action_id)
            self.assertEqual(issued['status'], 'ISSUED', issued)
            action = next(a for a in fixture.book.actions()
                          if a['id'] == fixture.action_id)
            fixture.book._begin_entry_operation_context(action, 'claim')
            real = fixture.book.supervised_engineering_authority.validate_current_permit

            def slow(*args, **kwargs):
                result = real(*args, **kwargs)
                fixture.advance(1)
                return result

            with patch.object(
                    fixture.book.supervised_engineering_authority,
                    'validate_current_permit', side_effect=slow):
                first = fixture.book.entry_authority.check(
                    source='ExecutionBook.claim.prevalidate', action=action,
                    purpose='engineering')
            self.assertTrue(first['allowed'], first)
            context = fixture.book._entry_operation_contexts[
                fixture.action_id]['claim']
            self.assertEqual(context['expires_at'],
                             datetime.fromisoformat(
                                 issued['permit']['valid_until']))
            fixture.advance(2)
            fixture.book._begin()
            try:
                final = fixture.book.entry_authority.check(
                    source='ExecutionBook.claim', action=action,
                    purpose='engineering')
            finally:
                fixture.book.db.rollback()
            self.assertFalse(final['allowed'], final)
            self.assertEqual(
                final['reason'],
                'ENTRY_AUTHORITY_SUPERVISED_PERMIT_CONTEXT_INVALID')
        finally:
            fixture.book._clear_entry_operation_contexts()
            fixture.close()

    def test_promotion_context_uses_shortest_original_window(self):
        fixture = self._full_entry_baseline_fixture()
        try:
            self._short_request(fixture)
            issued = produce_supervised_engineering_permit(
                fixture.book, fixture.action_id)
            self.assertEqual(issued['status'], 'ISSUED', issued)
            action = next(a for a in fixture.book.actions()
                          if a['id'] == fixture.action_id)
            fixture.book._begin_supervised_entry_prevalidation(action)
            first = fixture.book.entry_authority.check(
                source='ExecutionBook.promote_supervised_entry',
                action=action, purpose='engineering')
            self.assertTrue(first['allowed'], first)
            context = fixture.book._supervised_entry_context
            self.assertEqual(context['expires_at'],
                             datetime.fromisoformat(
                                 issued['permit']['valid_until']))
            fixture.advance(3)
            final = fixture.book.entry_authority.check(
                source='ExecutionBook.promote_supervised_entry.final',
                action=action, purpose='engineering')
            self.assertFalse(final['allowed'], final)
            self.assertEqual(
                final['reason'],
                'ENTRY_AUTHORITY_SUPERVISED_PERMIT_CONTEXT_INVALID')
        finally:
            fixture.book._clear_supervised_entry_context()
            fixture.close()

    def test_public_claim_and_native_entry_reject_without_residue(self):
        fixture = self._full_entry_baseline_fixture()
        try:
            action = next(a for a in fixture.book.actions()
                          if a['id'] == fixture.action_id)
            before_state = action['state']
            before_ops = fixture.book.db.execute(
                'SELECT count(*) FROM external_operations').fetchone()[0]
            claimed = fixture.book.claim()
            self.assertIsNone(claimed)
            self.assertEqual(next(a for a in fixture.book.actions()
                                  if a['id'] == fixture.action_id)['state'],
                             before_state)
            self.assertEqual(fixture.book._entry_operation_contexts, {})
            with self.assertRaisesRegex(
                    ValueError, 'SUPERVISED_ENGINEERING_PROMOTION_REQUIRED'):
                fixture.ops.begin(fixture.action_id, 'place',
                                  fixture.snap, quote=fixture._quote())
            self.assertEqual(next(a for a in fixture.book.actions()
                                  if a['id'] == fixture.action_id)['state'],
                             before_state)
            self.assertEqual(fixture.book.db.execute(
                'SELECT count(*) FROM external_operations').fetchone()[0],
                before_ops)
            self.assertEqual(fixture.book._entry_operation_contexts, {})
        finally:
            fixture.book._clear_entry_operation_contexts()
            fixture.close()

    def test_profile_promotion_is_default_off_and_enabled_only_explicitly(self):
        off = ProfileFixture()
        self.addCleanup(off.close)
        off.bridge = HostExecutionBridge(off.book, off.root / 'host')
        self.assertFalse(off.book.supervised_engineering_authority.real_activation_enabled)
        self.assertIsNone(off.bridge.engineering_promotion)
        self.assertIsNone(off.book.supervised_engineering_profile.get('promotion'))
        on = ProfileFixture(promotion_enabled=True)
        self.addCleanup(on.close)
        on.bridge = HostExecutionBridge(on.book, on.root / 'host')
        self.assertTrue(on.book.supervised_engineering_authority.real_activation_enabled)
        self.assertEqual(on.bridge.engineering_promotion, {'enabled': True})
        self.assertEqual(on.book.supervised_engineering_profile['promotion'],
                         {'enabled': True})
        self.assertTrue(on.book.supervised_engineering_profile['real_activation_enabled'])

    def test_default_profile_issuer_and_auto_promotion_reach_ready_synthetic(self):
        # Test-only synthetic book: no formal source, no broker, no real
        # capability.  The default Host/profile caller must still go through
        # the existing promote_supervised_entry consume/READY transaction.
        root = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: __import__('shutil').rmtree(root))
        policy_value = policy()
        snap = snapshot()
        snap['synthetic'] = True
        account_key = snap['account_key']
        owner = root / 'owner'
        owner.mkdir(mode=0o700)
        clock_value = [NOW]
        _write_private(owner / 'supervised-engineering-request.json',
                       request(policy_value, account_key))
        authority = SupervisedEngineeringAuthority(
            owner, clock=lambda: clock_value[0], allow_synthetic=True)
        book = SupervisedBook(
            root / 'book.sqlite', policy_value, simulation=True,
            clock=lambda: clock_value[0],
            preflight=FakePreflight(assessment(policy_value, account_key)),
            supervised_engineering_authority=authority)
        bridge = None
        try:
            state = book.state()
            state['account_key'] = account_key
            book._save(state)
            book.db.commit()
            result = book.record_supervised_engineering_intent(
                'engineering-1', plan(), quote(), snap,
                evidence={
                    'synthetic': True, 'source_sha256': 'fixture',
                    'engineering_scope_authorized': True,
                    'parameters_user_specified': True,
                    'parameters_valid_until': (
                        NOW.replace(year=NOW.year + 1)).isoformat()})
            action_id = result['action_id']
            ops = NativeOperations(book)
            packet = ops.begin(action_id, 'review', snap, quote=quote())
            response = {'data': {
                **packet['parameters'], 'order_checks': {},
                'customer_approval_required': False,
                'market_data_disclosure': 'Exact disclosure',
                'quote_data': {}}}
            ops.record(packet['operation_id'], response)
            book.supervised_engineering_profile = {
                'kind': 'supervised_engineering_profile_v1',
                'enabled': True, 'owner_root': str(owner),
                'review': None, 'permit': {'enabled': True},
                'promotion': {'enabled': True},
                'real_activation_enabled': True}
            bridge = HostExecutionBridge(book, root / 'host')
            issued = bridge(snapshot=snap, quotes={'DEMO': quote()},
                            allow_entries=False)
            self.assertEqual(issued['status'], 'ENGINEERING_PERMIT_ISSUED',
                             issued)
            promoted = bridge(snapshot=snap, quotes={'DEMO': quote()},
                              allow_entries=True)
            self.assertEqual(promoted['status'], 'ENGINEERING_PROMOTION_READY',
                             promoted)
            self.assertEqual(promoted['state'], 'READY')
            self.assertFalse(promoted['claimable'])
            self.assertTrue(promoted['claim_check_required'])
            self.assertIn(promoted['claim_recheck_status'],
                          {'ALLOWED_FOR_FULL_CLAIM_CHECK', 'BLOCKED',
                           'UNKNOWN'})
            self.assertFalse(promoted['placeable'])
            self.assertFalse(promoted['entry_authorized'])
            self.assertFalse(promoted['execution_authorized'])
            self.assertFalse(promoted['broker_effect_verified'])
            self.assertEqual(self.consumption_count(book), 1)
            self.assertEqual(
                next(a for a in book.actions()
                     if a['id'] == action_id)['state'], 'READY')
            bridge(snapshot=snap, quotes={'DEMO': quote()},
                   allow_entries=True)
            self.assertEqual(self.consumption_count(book), 1)
            bridge.close()
            clock_value[0] = clock_value[0] + timedelta(seconds=60)
            changed = json.loads(
                (owner / 'supervised-engineering-request.json').read_text())
            changed['authority_basis']['ref'] = 'CHANGED'
            _write_private(owner / 'supervised-engineering-request.json',
                           changed)
            bridge = HostExecutionBridge(book, root / 'host2')
            restarted = bridge(snapshot=snap, quotes={'DEMO': quote()},
                               allow_entries=True)
            self.assertEqual(restarted['status'],
                             'ENGINEERING_PROMOTION_ALREADY_COMPLETED',
                             restarted)
            self.assertTrue(restarted['recovered'])
            self.assertFalse(restarted['claimable'])
            self.assertTrue(restarted['claim_check_required'])
            self.assertEqual(restarted['claim_recheck_status'], 'BLOCKED')
            self.assertTrue(restarted['attention_required'])
            manifest = read_private_json(bridge.directory / 'manifest.json')
            item = next(a for a in manifest['actions']
                        if a['id'] == action_id)
            self.assertFalse(item['claimable'])
            self.assertTrue(item['claim_check_required'])
            self.assertEqual(item['claim_recheck_status'],
                             'REQUIRES_CURRENT_CLAIM_CHECK')
            self.assertEqual(self.consumption_count(book), 1)
        finally:
            if bridge is not None:
                bridge.close()
            book.close()

    def test_recovered_promotion_is_conservative_for_revoked_permit_and_missing_facts(self):
        for invalidation in ('revoked', 'facts'):
            with self.subTest(invalidation=invalidation):
                root = Path(tempfile.mkdtemp())
                self.addCleanup(lambda: __import__('shutil').rmtree(root, ignore_errors=True))
                policy_value = policy()
                snap = snapshot()
                snap['synthetic'] = True
                account_key = snap['account_key']
                owner = root / 'owner'
                owner.mkdir(mode=0o700)
                clock_value = [NOW]
                _write_private(owner / 'supervised-engineering-request.json',
                               request(policy_value, account_key))
                book = SupervisedBook(
                    root / 'book.sqlite', policy_value, simulation=True,
                    clock=lambda: clock_value[0],
                    preflight=FakePreflight(
                        assessment(policy_value, account_key)),
                    supervised_engineering_authority=(
                        SupervisedEngineeringAuthority(
                            owner, clock=lambda: clock_value[0],
                            allow_synthetic=True)))
                bridge = None
                try:
                    state = book.state()
                    state['account_key'] = account_key
                    book._save(state)
                    book.db.commit()
                    result = book.record_supervised_engineering_intent(
                        'engineering-1', plan(), quote(), snap,
                        evidence={
                            'synthetic': True, 'source_sha256': 'fixture',
                            'engineering_scope_authorized': True,
                            'parameters_user_specified': True,
                            'parameters_valid_until': (
                                NOW.replace(year=NOW.year + 1)).isoformat()})
                    action_id = result['action_id']
                    ops = NativeOperations(book)
                    packet = ops.begin(action_id, 'review', snap, quote=quote())
                    ops.record(packet['operation_id'], {'data': {
                        **packet['parameters'], 'order_checks': {},
                        'customer_approval_required': False,
                        'market_data_disclosure': 'Exact disclosure',
                        'quote_data': {}}})
                    book.supervised_engineering_profile = {
                        'kind': 'supervised_engineering_profile_v1',
                        'enabled': True, 'owner_root': str(owner),
                        'review': None, 'permit': {'enabled': True},
                        'promotion': {'enabled': True},
                        'real_activation_enabled': True}
                    bridge = HostExecutionBridge(book, root / 'host')
                    bridge(snapshot=snap, quotes={'DEMO': quote()},
                           allow_entries=False)
                    promoted = bridge(snapshot=snap, quotes={'DEMO': quote()},
                                      allow_entries=True)
                    self.assertEqual(promoted['status'],
                                     'ENGINEERING_PROMOTION_READY')
                    bridge.close()
                    bridge = None
                    if invalidation == 'revoked':
                        permit_path = (owner /
                                       'supervised-engineering-permit.json')
                        permit = json.loads(permit_path.read_text())
                        permit['revoked_at'] = NOW.isoformat()
                        permit['permit_id'] = digest({
                            **permit, 'permit_id': None})
                        _write_private(permit_path, permit)
                    else:
                        book._current_account_facts['engineering'] = None
                    bridge = HostExecutionBridge(book, root / 'host2')
                    recovered = bridge(snapshot=snap,
                                       quotes={'DEMO': quote()},
                                       allow_entries=True)
                    if invalidation == 'revoked':
                        self.assertIn(recovered['status'], {
                            'ENGINEERING_PROMOTION_ALREADY_COMPLETED',
                            'ENGINEERING_PROMOTION_RECOVERY_REQUIRED'},
                            recovered)
                    else:
                        self.assertEqual(
                            recovered['status'],
                            'ENGINEERING_PROMOTION_ALREADY_COMPLETED',
                            recovered)
                        self.assertTrue(recovered['claim_check_required'])
                        self.assertEqual(recovered['claim_recheck_status'],
                                         'BLOCKED')
                    self.assertFalse(recovered['claimable'])
                    self.assertTrue(recovered['attention_required'])
                    self.assertEqual(self.consumption_count(book), 1)
                finally:
                    if bridge is not None:
                        bridge.close()
                    book.close()

    def test_auto_promotion_recovers_after_publish_failure_without_reconsume(self):
        root = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: __import__('shutil').rmtree(root))
        policy_value = policy()
        snap = snapshot()
        snap['synthetic'] = True
        account_key = snap['account_key']
        owner = root / 'owner'
        owner.mkdir(mode=0o700)
        _write_private(owner / 'supervised-engineering-request.json',
                       request(policy_value, account_key))
        book = SupervisedBook(
            root / 'book.sqlite', policy_value, simulation=True,
            clock=lambda: NOW,
            preflight=FakePreflight(assessment(policy_value, account_key)),
            supervised_engineering_authority=SupervisedEngineeringAuthority(
                owner, clock=lambda: NOW, allow_synthetic=True))
        bridge = None
        try:
            state = book.state()
            state['account_key'] = account_key
            book._save(state)
            book.db.commit()
            result = book.record_supervised_engineering_intent(
                'engineering-1', plan(), quote(), snap,
                evidence={
                    'synthetic': True, 'source_sha256': 'fixture',
                    'engineering_scope_authorized': True,
                    'parameters_user_specified': True,
                    'parameters_valid_until': (
                        NOW.replace(year=NOW.year + 1)).isoformat()})
            action_id = result['action_id']
            ops = NativeOperations(book)
            packet = ops.begin(action_id, 'review', snap, quote=quote())
            ops.record(packet['operation_id'], {'data': {
                **packet['parameters'], 'order_checks': {},
                'customer_approval_required': False,
                'market_data_disclosure': 'Exact disclosure',
                'quote_data': {}}})
            book.supervised_engineering_profile = {
                'kind': 'supervised_engineering_profile_v1',
                'enabled': True, 'owner_root': str(owner),
                'review': None, 'permit': {'enabled': True},
                'promotion': {'enabled': True},
                'real_activation_enabled': True}
            bridge = HostExecutionBridge(book, root / 'host')
            issued = bridge(snapshot=snap, quotes={'DEMO': quote()},
                            allow_entries=False)
            self.assertEqual(issued['status'], 'ENGINEERING_PERMIT_ISSUED')
            with patch.object(bridge, '_publish',
                              side_effect=RuntimeError('PUBLISH_FAILED')):
                with self.assertRaisesRegex(RuntimeError, 'PUBLISH_FAILED'):
                    bridge(snapshot=snap, quotes={'DEMO': quote()},
                           allow_entries=True)
            self.assertEqual(self.consumption_count(book), 1)
            self.assertEqual(next(a for a in book.actions()
                                  if a['id'] == action_id)['state'], 'READY')
            recovered = bridge(snapshot=snap, quotes={'DEMO': quote()},
                               allow_entries=True)
            self.assertEqual(recovered['status'],
                             'ENGINEERING_PROMOTION_ALREADY_COMPLETED',
                             recovered)
            self.assertTrue(recovered['recovered'])
            self.assertEqual(recovered['current_state'], 'READY')
            self.assertEqual(self.consumption_count(book), 1)
        finally:
            if bridge is not None:
                bridge.close()
            book.close()

    def test_auto_promotion_is_blocked_by_outer_entry_gate_without_ready(self):
        fixture = RealDryFixture(prebuilt_intent=True, activate=True)
        try:
            permit_path = fixture.owner / 'supervised-engineering-permit.json'
            permit_path.unlink()
            fixture.book.supervised_engineering_profile = {
                'kind': 'supervised_engineering_profile_v1',
                'enabled': True, 'owner_root': str(fixture.owner),
                'review': None, 'permit': {'enabled': True},
                'promotion': {'enabled': True},
                'real_activation_enabled': True}
            bridge = HostExecutionBridge(
                fixture.book, fixture.root / 'host',
                review_factory=ScriptedEntryTransport)
            try:
                issued = bridge(
                    snapshot=fixture.snap, quotes={'DEMO': fixture._quote()},
                    allow_entries=False)
                self.assertEqual(issued['status'], 'ENGINEERING_PERMIT_ISSUED',
                                 issued)
                self.assertTrue(permit_path.is_file())
                blocked = bridge(
                    snapshot=fixture.snap, quotes={'DEMO': fixture._quote()},
                    allow_entries=True)
                self.assertEqual(blocked['status'],
                                 'ENGINEERING_PROMOTION_BLOCKED', blocked)
                self.assertIn('ENTRY_AUTHORITY_BLOCKED',
                              blocked['promotion_error'])
                self.assertFalse(blocked['claimable'])
                self.assertFalse(blocked['placeable'])
                action = next(a for a in fixture.book.actions()
                              if a['id'] == fixture.action_id)
                self.assertEqual(action['state'], 'WAITING_USER')
                self.assertEqual(self.consumption_count(fixture.book), 0)
            finally:
                bridge.close()
        finally:
            fixture.close()

    def test_auto_promotion_blocks_changed_permit_action_without_consume(self):
        fixture = RealDryFixture(prebuilt_intent=True, activate=True)
        try:
            permit_path = fixture.owner / 'supervised-engineering-permit.json'
            permit_path.unlink()
            fixture.book.supervised_engineering_profile = {
                'kind': 'supervised_engineering_profile_v1',
                'enabled': True, 'owner_root': str(fixture.owner),
                'review': None, 'permit': {'enabled': True},
                'promotion': {'enabled': True},
                'real_activation_enabled': True}
            bridge = HostExecutionBridge(fixture.book, fixture.root / 'host')
            try:
                issued = bridge(
                    snapshot=fixture.snap, quotes={'DEMO': fixture._quote()},
                    allow_entries=False)
                self.assertEqual(issued['status'], 'ENGINEERING_PERMIT_ISSUED')
                permit = json.loads(permit_path.read_text())
                permit['action_id'] = 'live-missing-action'
                _write_private(permit_path, permit)
                out = bridge(
                    snapshot=fixture.snap, quotes={'DEMO': fixture._quote()},
                    allow_entries=True)
                self.assertEqual(out['status'],
                                 'ENGINEERING_PERMIT_BLOCKED', out)
                self.assertEqual(out['permit_status'], 'BLOCKED')
                self.assertEqual(self.consumption_count(fixture.book), 0)
                self.assertEqual(next(a for a in fixture.book.actions()
                                      if a['id'] == fixture.action_id)['state'],
                                 'WAITING_USER')
            finally:
                bridge.close()
        finally:
            fixture.close()

    def test_auto_promotion_after_clock_advance_blocks_without_consume(self):
        fixture = RealDryFixture(prebuilt_intent=True, activate=True)
        try:
            permit_path = fixture.owner / 'supervised-engineering-permit.json'
            permit_path.unlink()
            fixture.book.supervised_engineering_profile = {
                'kind': 'supervised_engineering_profile_v1',
                'enabled': True, 'owner_root': str(fixture.owner),
                'review': None, 'permit': {'enabled': True},
                'promotion': {'enabled': True},
                'real_activation_enabled': True}
            bridge = HostExecutionBridge(fixture.book, fixture.root / 'host')
            try:
                bridge(snapshot=fixture.snap, quotes={'DEMO': fixture._quote()},
                       allow_entries=False)
                fixture.advance(31)
                out = bridge(
                    snapshot=fixture.snap, quotes={'DEMO': fixture._quote()},
                    allow_entries=True)
                self.assertEqual(out['status'],
                                 'ENGINEERING_PERMIT_BLOCKED', out)
                self.assertIn('PERMIT_WINDOW',
                              out['permit_error'])
                self.assertEqual(self.consumption_count(fixture.book), 0)
                self.assertEqual(next(a for a in fixture.book.actions()
                                      if a['id'] == fixture.action_id)['state'],
                                 'WAITING_USER')
            finally:
                bridge.close()
        finally:
            fixture.close()

    def test_auto_promotion_requires_allow_entries_and_dispatch(self):
        fixture = RealDryFixture(prebuilt_intent=True, activate=True)
        try:
            fixture.book.supervised_engineering_profile = {
                'kind': 'supervised_engineering_profile_v1',
                'enabled': True, 'owner_root': str(fixture.owner),
                'review': None, 'permit': {'enabled': True},
                'promotion': {'enabled': True},
                'real_activation_enabled': True}
            bridge = HostExecutionBridge(
                fixture.book, fixture.root / 'host',
                review_factory=ScriptedEntryTransport)
            try:
                bridge(snapshot=fixture.snap, quotes={'DEMO': fixture._quote()},
                       allow_entries=False)
                for allow_entries, allow_dispatch in ((False, True),
                                                      (True, False)):
                    out = bridge(
                        snapshot=fixture.snap,
                        quotes={'DEMO': fixture._quote()},
                        allow_entries=allow_entries,
                        allow_dispatch=allow_dispatch)
                    self.assertNotEqual(out.get('status'),
                                        'ENGINEERING_PROMOTION_READY', out)
                    action = next(a for a in fixture.book.actions()
                                  if a['id'] == fixture.action_id)
                    self.assertEqual(action['state'], 'WAITING_USER')
                    self.assertEqual(self.consumption_count(fixture.book), 0)
            finally:
                bridge.close()
        finally:
            fixture.close()

    def consumption_count(self, book):
        exists = book.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            ('formal_authorization_consumptions',)).fetchone()
        if not exists:
            return 0
        return book.db.execute(
            'SELECT count(*) FROM formal_authorization_consumptions').fetchone()[0]

    def test_permit_producer_is_idempotent_and_never_overwrites_conflict(self):
        fixture = RealDryFixture(prebuilt_intent=True, activate=True)
        try:
            permit_path = fixture.owner / 'supervised-engineering-permit.json'
            permit_path.unlink()
            first = produce_supervised_engineering_permit(
                fixture.book, fixture.action_id)
            self.assertEqual(first['status'], 'ISSUED')
            raw = permit_path.read_bytes()
            second = produce_supervised_engineering_permit(
                fixture.book, fixture.action_id)
            self.assertEqual(second['status'], 'EXISTING')
            self.assertEqual(second['permit_id'], first['permit_id'])
            self.assertEqual(permit_path.read_bytes(), raw)
            permit_path.write_text(json.dumps({'schema_version': 1}))
            os.chmod(permit_path, 0o600)
            with self.assertRaisesRegex(
                    EngineeringPermitError,
                    'SUPERVISED_ENGINEERING_PERMIT_EXISTING_CONFLICT'):
                produce_supervised_engineering_permit(
                    fixture.book, fixture.action_id)
            self.assertEqual(json.loads(permit_path.read_text()),
                             {'schema_version': 1})
        finally:
            fixture.close()

    def test_permit_producer_blocks_unverified_source_without_writing(self):
        fixture = RealDryFixture(prebuilt_intent=True, activate=True)
        try:
            permit_path = fixture.owner / 'supervised-engineering-permit.json'
            permit_path.unlink()
            fixture.provenance_fault = 'exception'
            with self.assertRaisesRegex(
                    EngineeringPermitError,
                    'SUPERVISED_ENGINEERING_PERMIT_SOURCE_UNVERIFIED'):
                produce_supervised_engineering_permit(
                    fixture.book, fixture.action_id)
            self.assertFalse(permit_path.exists())
        finally:
            fixture.close()

    def test_permit_producer_refuses_stale_existing_permit(self):
        fixture = RealDryFixture(prebuilt_intent=True, activate=True)
        try:
            permit_path = fixture.owner / 'supervised-engineering-permit.json'
            permit_path.unlink()
            first = produce_supervised_engineering_permit(
                fixture.book, fixture.action_id)
            before = permit_path.read_bytes()
            fixture.mutate_facts({'status': 'mutated'})
            with self.assertRaisesRegex(
                    EngineeringPermitError,
                    'SUPERVISED_ENGINEERING_PERMIT_EXISTING_CONFLICT'):
                produce_supervised_engineering_permit(
                    fixture.book, fixture.action_id)
            self.assertEqual(permit_path.read_bytes(), before)
            self.assertEqual(json.loads(before)['permit_id'],
                             first['permit_id'])
        finally:
            fixture.close()

    def test_permit_producer_blocks_wrong_review_order_before_publish(self):
        fixture = RealDryFixture(prebuilt_intent=True, activate=True)
        try:
            permit_path = fixture.owner / 'supervised-engineering-permit.json'
            permit_path.unlink()
            row = fixture.book.db.execute(
                'SELECT payload FROM native_operation_records WHERE id=?',
                (fixture.packet['operation_id'],)).fetchone()
            value = json.loads(row[0])
            value['parameters']['limit_price'] = '99.00'
            fixture.book.db.execute(
                'UPDATE native_operation_records SET payload=? WHERE id=?',
                (json.dumps(value), fixture.packet['operation_id']))
            fixture.book.db.commit()
            with self.assertRaisesRegex(
                    EngineeringPermitError, 'REVIEW_ORDER'):
                produce_supervised_engineering_permit(
                    fixture.book, fixture.action_id)
            self.assertFalse(permit_path.exists())
            action = next(a for a in fixture.book.actions()
                          if a['id'] == fixture.action_id)
            self.assertEqual(action['state'], 'WAITING_USER')
        finally:
            fixture.close()

    def test_post_source_owner_request_change_blocks_publication(self):
        fixture = RealDryFixture(prebuilt_intent=True, activate=True)
        try:
            permit_path = fixture.owner / 'supervised-engineering-permit.json'
            permit_path.unlink()
            original = engineering_permit_producer._verified

            def late_change(*args, **kwargs):
                result = original(*args, **kwargs)
                value = json.loads(fixture.request_bytes())
                value['authority_basis']['ref'] = 'changed-owner-authorization'
                fixture.write_request(value)
                return result

            with patch.object(engineering_permit_producer, '_verified',
                              side_effect=late_change):
                with self.assertRaisesRegex(
                        EngineeringPermitError,
                        'POST_READ:.*REQUEST_CHANGED'):
                    produce_supervised_engineering_permit(
                        fixture.book, fixture.action_id)
            self.assertFalse(permit_path.exists())
        finally:
            fixture.close()

    def test_slow_source_reader_crossing_facts_window_blocks(self):
        fixture = RealDryFixture(
            prebuilt_intent=True, activate=True, facts_valid_seconds=10)
        try:
            permit_path = fixture.owner / 'supervised-engineering-permit.json'
            permit_path.unlink()
            called = [False]

            def slow_once(book, result):
                value = fixture._default_provenance(book, result)
                if not called[0]:
                    called[0] = True
                    fixture.advance(15)
                return value

            fixture.configure(provenance=slow_once)
            with self.assertRaisesRegex(
                    EngineeringPermitError,
                    'SOURCE_UNVERIFIED:.*WINDOW_EXPIRED'):
                produce_supervised_engineering_permit(
                    fixture.book, fixture.action_id)
            self.assertFalse(permit_path.exists())
        finally:
            fixture.close()

    def test_post_source_clock_advance_blocks_without_refreshing_ttl(self):
        fixture = RealDryFixture(prebuilt_intent=True, activate=True)
        try:
            permit_path = fixture.owner / 'supervised-engineering-permit.json'
            permit_path.unlink()
            original = engineering_permit_producer._verified

            def late_clock(*args, **kwargs):
                result = original(*args, **kwargs)
                fixture.advance(31)
                return result

            with patch.object(engineering_permit_producer, '_verified',
                              side_effect=late_clock):
                with self.assertRaisesRegex(
                        EngineeringPermitError,
                        'POST_READ:.*PERMIT_WINDOW'):
                    produce_supervised_engineering_permit(
                        fixture.book, fixture.action_id)
            self.assertFalse(permit_path.exists())
        finally:
            fixture.close()

    def test_post_source_action_ref_and_facts_changes_block(self):
        cases = ('action-payload', 'facts-body', 'ref')
        for case in cases:
            with self.subTest(case=case):
                fixture = RealDryFixture(prebuilt_intent=True, activate=True)
                try:
                    permit_path = (fixture.owner /
                                   'supervised-engineering-permit.json')
                    permit_path.unlink()
                    if case == 'ref':
                        original = engineering_permit_producer._verified

                        def late_ref(*args, **kwargs):
                            result = original(*args, **kwargs)
                            row = fixture.book.db.execute(
                                'SELECT payload FROM live_actions WHERE id=?',
                                (fixture.action_id,)).fetchone()
                            value = json.loads(row[0])
                            value['ref_id'] = 'changed-ref'
                            fixture.book.db.execute(
                                'UPDATE live_actions SET payload=? WHERE id=?',
                                (json.dumps(value), fixture.action_id))
                            fixture.book.db.commit()
                            return result

                        context = patch.object(
                            engineering_permit_producer, '_verified',
                            side_effect=late_ref)
                    else:
                        fixture.provenance_fault = case
                        context = patch.object(
                            engineering_permit_producer, '_verified',
                            wraps=engineering_permit_producer._verified)
                    with context:
                        with self.assertRaises(EngineeringPermitError) as raised:
                            produce_supervised_engineering_permit(
                                fixture.book, fixture.action_id)
                    self.assertIn('POST_READ', raised.exception.code)
                    self.assertFalse(permit_path.exists())
                finally:
                    fixture.close()

    def test_permit_restart_and_report_failure_return_existing(self):
        fixture = RealDryFixture(prebuilt_intent=True, activate=True)
        try:
            permit_path = fixture.owner / 'supervised-engineering-permit.json'
            permit_path.unlink()
            first = produce_supervised_engineering_permit(
                fixture.book, fixture.action_id)
            self.assertEqual(first['status'], 'ISSUED')
            raw = permit_path.read_bytes()
            fixture.reopen()
            second = produce_supervised_engineering_permit(
                fixture.book, fixture.action_id)
            self.assertEqual(second['status'], 'EXISTING')
            self.assertEqual(second['permit_id'], first['permit_id'])
            self.assertEqual(permit_path.read_bytes(), raw)
        finally:
            fixture.close()

    def test_concurrent_same_permit_is_idempotent_and_conflict_never_overwrites(self):
        for conflict in (False, True):
            with self.subTest(conflict=conflict):
                fixture = RealDryFixture(prebuilt_intent=True, activate=True)
                try:
                    permit_path = (fixture.owner /
                                   'supervised-engineering-permit.json')
                    permit_path.unlink()
                    original = engineering_permit_producer._verified

                    def racing_issuer(*args, **kwargs):
                        result = original(*args, **kwargs)
                        if conflict:
                            write_private_json(
                                permit_path, {'schema_version': 1})
                        else:
                            write_private_json(permit_path, args[2])
                        return result

                    with patch.object(engineering_permit_producer, '_verified',
                                      side_effect=racing_issuer):
                        if conflict:
                            with self.assertRaisesRegex(
                                    EngineeringPermitError,
                                    'WRITE_CONFLICT'):
                                produce_supervised_engineering_permit(
                                    fixture.book, fixture.action_id)
                        else:
                            output = produce_supervised_engineering_permit(
                                fixture.book, fixture.action_id)
                            self.assertEqual(output['status'], 'EXISTING')
                    if conflict:
                        self.assertEqual(
                            json.loads(permit_path.read_text()),
                            {'schema_version': 1})
                    else:
                        permit = json.loads(permit_path.read_text())
                        self.assertEqual(permit['action_id'],
                                         fixture.action_id)
                finally:
                    fixture.close()

    def test_profile_review_failure_is_named_and_not_retried(self):
        FailingEntryTransport.constructed = 0
        fixture = ProfileFixture(review_enabled=True)
        self.addCleanup(fixture.close)
        fixture.bridge = HostExecutionBridge(
            fixture.book, fixture.root / 'host',
            review_factory=FailingEntryTransport)
        submit_engineering_intent(
            fixture.bridge.directory, 'engineering-default-profile',
            plan(), {
                'synthetic': False, 'source_sha256': 'fixture',
                'engineering_scope_authorized': True,
                'parameters_user_specified': True,
                'parameters_valid_until': (
                    NOW.replace(year=NOW.year + 1)).isoformat()},
            clock=lambda: NOW)
        first = fixture.bridge(
            snapshot=fixture.snap, quotes={'DEMO': quote()},
            allow_entries=False)
        action_id = first['action_id']
        submit_request(
            fixture.bridge.directory, action_id, 'review', clock=lambda: NOW)
        output = fixture.bridge(
            snapshot=fixture.snap, quotes={'DEMO': quote()},
            allow_entries=False)
        self.assertEqual(output['status'], 'OWNED_REVIEW_FAILED', output)
        self.assertEqual(FailingEntryTransport.constructed, 1)
        self.assertFalse(output['execution_authorized'])
        action = next(a for a in fixture.book.actions()
                      if a['id'] == action_id)
        self.assertNotEqual(action['state'], 'READY')
        receipt = fixture.bridge.directory / 'receipts' / (
            output['operation_id'] + '.json')
        self.assertFalse(receipt.exists())
        again = fixture.bridge(
            snapshot=fixture.snap, quotes={'DEMO': quote()},
            allow_entries=False)
        self.assertEqual(FailingEntryTransport.constructed, 1)
        self.assertNotEqual(again.get('status'), 'WAITING_NATIVE_CONFIRMATION')

    def test_profile_review_disabled_prepares_packet_without_transport(self):
        ScriptedEntryTransport.constructed = 0
        fixture = ProfileFixture()
        self.addCleanup(fixture.close)
        fixture.bridge = HostExecutionBridge(
            fixture.book, fixture.root / 'host',
            review_factory=ScriptedEntryTransport)
        self.assertIsNone(fixture.bridge.engineering_review)
        submit_engineering_intent(
            fixture.bridge.directory, 'engineering-default-profile',
            plan(), {
                'synthetic': False, 'source_sha256': 'fixture',
                'engineering_scope_authorized': True,
                'parameters_user_specified': True,
                'parameters_valid_until': (
                    NOW.replace(year=NOW.year + 1)).isoformat()},
            clock=lambda: NOW)
        first = fixture.bridge(
            snapshot=fixture.snap, quotes={'DEMO': quote()},
            allow_entries=False)
        action_id = first['action_id']
        submit_request(
            fixture.bridge.directory, action_id, 'review', clock=lambda: NOW)
        output = fixture.bridge(
            snapshot=fixture.snap, quotes={'DEMO': quote()},
            allow_entries=False)
        self.assertEqual(output['status'], 'HOST_PACKET_PREPARED')
        self.assertEqual(ScriptedEntryTransport.constructed, 0)
        self.assertIsNotNone(fixture.bridge.pending)

    def test_profile_review_requires_review_only_and_ten_second_max(self):
        fixture = ProfileFixture(review_enabled=True, review_only=False)
        self.addCleanup(fixture.close)
        with self.assertRaisesRegex(
                ValueError, 'SUPERVISED_ENGINEERING_REVIEW_ONLY_REQUIRED'):
            HostExecutionBridge(
                fixture.book, fixture.root / 'host',
                review_factory=ScriptedEntryTransport)
        fixture.policy['supervised_engineering']['review']['review_only'] = True
        fixture.policy['supervised_engineering']['review']['deadline_seconds'] = 11
        with self.assertRaisesRegex(
                ValueError, 'SUPERVISED_ENGINEERING_REVIEW_DEADLINE_INVALID'):
            HostExecutionBridge(
                fixture.book, fixture.root / 'host-2',
                review_factory=ScriptedEntryTransport)

    def test_profile_disabled_leaves_default_authority_unavailable(self):
        fixture = ProfileFixture(enabled=False)
        self.addCleanup(fixture.close)
        fixture.bridge = HostExecutionBridge(
            fixture.book, fixture.root / 'host')
        self.assertIsNone(fixture.book.supervised_engineering_authority)
        submit_engineering_intent(
            fixture.bridge.directory, 'engineering-default-profile',
            plan(), {
                'synthetic': False, 'source_sha256': 'fixture',
                'engineering_scope_authorized': True,
                'parameters_user_specified': True,
                'parameters_valid_until': (
                    NOW.replace(year=NOW.year + 1)).isoformat()},
            clock=lambda: NOW)
        output = fixture.bridge(
            snapshot=fixture.snap, quotes={'DEMO': quote()},
            allow_entries=False)
        self.assertEqual(output['status'], 'HOST_REQUEST_REJECTED')
        self.assertEqual(output['reason'],
                         'SUPERVISED_ENGINEERING_AUTHORITY_UNAVAILABLE')
        self.assertEqual(fixture.book.actions(), [])

    def test_profile_without_owner_request_is_named_and_not_claimed(self):
        fixture = ProfileFixture(write_request=False)
        self.addCleanup(fixture.close)
        fixture.bridge = HostExecutionBridge(
            fixture.book, fixture.root / 'host')
        self.assertIsNotNone(fixture.book.supervised_engineering_authority)
        submit_engineering_intent(
            fixture.bridge.directory, 'engineering-default-profile',
            plan(), {
                'synthetic': False, 'source_sha256': 'fixture',
                'engineering_scope_authorized': True,
                'parameters_user_specified': True,
                'parameters_valid_until': (
                    NOW.replace(year=NOW.year + 1)).isoformat()},
            clock=lambda: NOW)
        output = fixture.bridge(
            snapshot=fixture.snap, quotes={'DEMO': quote()},
            allow_entries=False)
        self.assertEqual(output['status'], 'HOST_REQUEST_REJECTED')
        self.assertEqual(output['reason'], 'OWNER_RECEIPT_REQUEST_MISSING')
        self.assertEqual(fixture.book.actions(), [])

    def test_profile_requires_existing_owner_root(self):
        fixture = ProfileFixture(owner_exists=False)
        self.addCleanup(fixture.close)
        with self.assertRaisesRegex(
                ValueError,
                'SUPERVISED_ENGINEERING_PROFILE_OWNER_ROOT_INVALID'):
            HostExecutionBridge(fixture.book, fixture.root / 'host')

    def test_profile_is_not_enabled_by_a_non_supervised_composition(self):
        with tempfile.TemporaryDirectory() as directory:
            p = policy()
            owner = Path(directory) / 'owner'
            owner.mkdir(mode=0o700)
            p['supervised_engineering'] = {
                'enabled': True, 'owner_root': str(owner)}
            with self.assertRaisesRegex(
                    ValueError,
                    'SUPERVISED_ENGINEERING_PROFILE_REQUIRES_SUPERVISED_HOST'):
                run_live_session(
                    Path(directory) / 'db', p,
                    symbols=[str(i) for i in range(10)],
                    agent_mode='disabled', cycles=1)

    def test_profile_config_is_default_off_and_malformed_values_fail_closed(self):
        self.assertIsNone(engineering_profile.profile_config({}))
        self.assertIsNone(engineering_profile.profile_config({
            'supervised_engineering': {'enabled': False}}))
        with self.assertRaisesRegex(
                ValueError, 'SUPERVISED_ENGINEERING_PROFILE_INVALID'):
            engineering_profile.profile_config({
                'supervised_engineering': []})
