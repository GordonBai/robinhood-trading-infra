from datetime import timedelta
import json
from pathlib import Path
import sqlite3
import tempfile
from unittest import TestCase
from unittest.mock import patch

from capability_contract_fixture import CONTRACT
from martin_agentic.capabilities import capability_blockers, capability_report
from martin_agentic.execution_preflight import script_execution_preflight
from martin_agentic.rehearsal import _Fixture


class ScriptPreflightTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'fixture.sqlite'
        self.policy = json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        self.fixture = _Fixture(self.path, self.policy)
        self.inventory = json.loads(Path('reports/community_tool_contract_evidence.json').read_text())
        # This fixture executes as_of an older session date.  Anchor its local
        # metadata snapshot to that same observation so the shared future-time
        # guard is exercised by its own capability test, not by this historical
        # preflight fixture.
        self.inventory['captured_at'] = self.fixture.at.isoformat()

    def tearDown(self):
        self.fixture.book.close()
        self.temp.cleanup()

    def inspect(self, **kwargs):
        return script_execution_preflight(self.path, self.policy, self.inventory,
            as_of=self.fixture.at.isoformat(), **kwargs)

    def test_shadow_does_not_claim_or_modify_execution_or_open_risk_sidecar(self):
        self.fixture.propose()
        before = self.path.read_bytes()
        files = set(Path(self.temp.name).iterdir())
        with patch('martin_agentic.execution.ExecutionBook.__init__', side_effect=AssertionError('Must not construct book')):
            result = self.inspect()
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual(set(Path(self.temp.name).iterdir()), files)
        self.assertEqual(result['queued_action_count'], 1)
        self.assertEqual(result['actions'][0]['state'], 'READY')
        self.assertFalse(result['actions'][0]['dispatch_allowed'])
        self.assertEqual(result['book_mutations'], 0)
        self.assertEqual(result['broker_calls'], 0)
        self.assertEqual(result['model_calls'], 0)

    def test_local_policy_flags_cannot_establish_unattended_capability(self):
        result = self.inspect()
        self.assertEqual(result['status'], 'BLOCKED_CAPABILITY')
        self.assertIn('NO_FORMALLY_QUALIFIED_UNATTENDED_ADAPTER', result['blockers'])
        self.assertIn('CURRENT_NATIVE_CANCEL_REQUIRES_SPECIFIC_CONFIRMATION', result['blockers'])
        self.assertIn('CURRENT_NATIVE_PLACE_REQUIRES_SPECIFIC_CONFIRMATION', result['blockers'])
        self.assertFalse(result['execution_authorized'])

    def test_preflight_uses_the_same_capability_blockers_as_the_report(self):
        report = capability_report(
            self.inventory, clock=lambda: self.fixture.at)
        result = self.inspect()
        self.assertTrue(set(capability_blockers(report)) <=
                        set(result['blockers']))
        self.assertEqual(result['contract_evidence_sha256'],
                         report['metadata_sha256'])
        self.assertFalse(result['execution_authorized'])

    def test_synthetic_source_is_rejected_by_preflight(self):
        inventory = json.loads(json.dumps(CONTRACT))
        inventory['source'] = 'synthetic_contract_fixture'
        inventory['synthetic'] = True
        inventory['account_data'] = False
        inventory['captured_at'] = self.fixture.at.isoformat()
        self.inventory = inventory
        with self.assertRaisesRegex(ValueError, 'OFFICIAL_TOOL_METADATA_REQUIRED'):
            self.inspect()

    def test_unknown_confirmation_blocks_preflight_with_named_unknowns(self):
        inventory = json.loads(json.dumps(CONTRACT))
        inventory['captured_at'] = self.fixture.at.isoformat()
        inventory['tools'] = [{'name': 'get_equity_quotes',
                               'description': 'Read quotes only.'}]
        self.inventory = inventory
        result = self.inspect()
        self.assertIn('CURRENT_NATIVE_PLACE_CONFIRMATION_UNKNOWN',
                      result['blockers'])
        self.assertIn('CURRENT_NATIVE_CANCEL_CONFIRMATION_UNKNOWN',
                      result['blockers'])
        self.assertIn('CURRENT_ADVANCED_CANCEL_CONFIRMATION_UNKNOWN',
                      result['blockers'])
        self.assertNotIn('CURRENT_NATIVE_PLACE_REQUIRES_SPECIFIC_CONFIRMATION',
                         result['blockers'])
        self.assertFalse(result['execution_authorized'])

    def test_unknown_actions_are_only_reconciled_and_never_replayed(self):
        self.fixture.propose()
        self.fixture.book.claim()
        self.fixture.book.recover()
        result = self.inspect()
        self.assertEqual(result['actions'][0]['state'], 'UNKNOWN')
        self.assertEqual(result['actions'][0]['disposition'], 'RECONCILE_ONLY_NO_REPLAY')

    def test_expired_actions_are_not_dispatched(self):
        self.fixture.propose()
        self.fixture.at += timedelta(seconds=16)
        result = self.inspect()
        self.assertTrue(result['actions'][0]['expired'])
        self.assertEqual(result['actions'][0]['disposition'], 'EXPIRED_NO_DISPATCH')
        self.assertEqual(self.fixture.book.actions()[0]['state'], 'READY')

    def test_missing_path_is_not_created(self):
        missing = self.path.with_name('missing.sqlite')
        with self.assertRaises(FileNotFoundError):
            script_execution_preflight(missing, self.policy, self.inventory)
        self.assertFalse(missing.exists())

    def test_policy_rebinding_fails_without_modifying_source(self):
        before = self.path.read_bytes()
        with self.assertRaisesRegex(ValueError, 'POLICY_OR_MODE_CHANGED'):
            script_execution_preflight(self.path, {**self.policy, 'live_enabled': True}, self.inventory)
        self.assertEqual(self.path.read_bytes(), before)

    def test_limit_requires_bounded_integer(self):
        for value in [True, 0, 1001, '1']:
            with self.assertRaisesRegex(ValueError, 'PREFLIGHT_LIMIT'):
                self.inspect(limit=value)

    def test_audit_corruption_fails_instead_of_publishing_valid_snapshot(self):
        self.fixture.propose()
        self.fixture.book.claim()
        db = self.fixture.book.db
        db.execute('DROP TRIGGER audit_no_update')
        db.execute("UPDATE audit_events SET entry_hash='bad'")
        db.commit()
        with self.assertRaisesRegex(ValueError, 'AUDIT_INTEGRITY_FAILED'):
            self.inspect()

    def test_full_audit_verification_does_not_hold_source_read_transaction(self):
        from martin_agentic.audit import AuditTrail
        verify = AuditTrail.verify
        def verify_while_writing_snapshot_source(audit):
            self.fixture.book.db.execute('CREATE TABLE source_writer_unblocked(id INTEGER)')
            self.fixture.book.db.commit()
            return verify(audit)
        with patch.object(AuditTrail, 'verify', verify_while_writing_snapshot_source):
            result = self.inspect()
        self.assertFalse(result['private_snapshot']['source_transaction_held_during_export'])
