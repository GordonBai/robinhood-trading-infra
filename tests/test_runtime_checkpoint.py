"""C5-06 local runtime checkpoint preflight, tamper refusal and isolated restore."""
import hashlib
import json
import sqlite3
import tempfile
from pathlib import Path
from unittest import TestCase

from martin_agentic.audit import canonical, digest
from martin_agentic.qualification import source_fingerprint
from martin_agentic.runtime_checkpoint import _full_generation, _hash
from martin_agentic.runtime_checkpoint import (
    create_runtime_checkpoint, inspect_runtime_checkpoint,
    preflight_runtime_checkpoint, restore_runtime_checkpoint,
    runtime_generation, verify_runtime_checkpoint)


class RuntimeCheckpointTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.db = self.root / 'execution.sqlite'
        self.risk = self.root / 'account-risk-0000.sqlite'
        self.host = self.root / 'host'
        self.host.mkdir()
        (self.host / 'requests').mkdir()
        (self.host / 'results').mkdir()
        (self.host / 'requests' / 'old-request.json').write_text('{"request_id":"old"}')
        (self.host / 'deliveries').mkdir()
        (self.host / 'deliveries' / 'old-index.json').write_text('{}')
        binding = {'db': str(self.db), 'account': '••••0000',
                   'account_key': 'acct', 'policy_sha256': 'policy',
                   'source_fingerprint': source_fingerprint(), 'epoch': 'epoch-1'}
        (self.host / 'manifest.json').write_text(json.dumps(
            {'schema_version': 1, 'running': True, 'binding': binding}))
        connection = sqlite3.connect(self.db)
        for table, columns in {
            'live_state': 'id INTEGER PRIMARY KEY,payload TEXT',
            'live_actions': 'id TEXT',
            'execution_intents': 'intent_id TEXT',
            'native_operation_records': 'id TEXT',
            'external_operations': 'operation_id TEXT',
            'instance_fills': 'id TEXT',
            'execution_fill_identity': 'id TEXT'}.items():
            connection.execute('CREATE TABLE ' + table + '(' + columns + ')')
        connection.execute("INSERT INTO live_state VALUES(1,'{}')")
        connection.commit()
        connection.close()
        risk = sqlite3.connect(self.risk)
        risk.execute('CREATE TABLE account_risk('
                     'id INTEGER PRIMARY KEY,payload TEXT)')
        risk.execute('INSERT INTO account_risk VALUES(1,?)', (json.dumps({
            'account_key': 'acct', 'terms': 'terms', 'blocked': True,
            'day': '2026-10-16', 'day_start_equity': '500',
            'nav_high': '1', 'daily_halted': True, 'project_halted': True,
            'equity': '490', 'cash': '490'}),))
        risk.commit()
        risk.close()

    def create(self, name='checkpoint'):
        return create_runtime_checkpoint(
            self.db, self.host, self.risk, self.root / name,
            account_suffix='0000')

    def _set_risk_locks(self, *, blocked, daily, project):
        connection = sqlite3.connect(self.risk)
        try:
            state = json.loads(connection.execute(
                'SELECT payload FROM account_risk WHERE id=1').fetchone()[0])
            state.update(blocked=blocked, daily_halted=daily,
                         project_halted=project)
            connection.execute('UPDATE account_risk SET payload=? WHERE id=1',
                               (json.dumps(state),))
            connection.commit()
        finally:
            connection.close()

    def test_create_verify_and_restore_preserves_nonempty_risk(self):
        created = self.create()
        self.assertTrue(created['trading_runtime_restore_qualified'])
        self.assertTrue(created['risk_blocked'])
        checked = verify_runtime_checkpoint(
            self.root / 'checkpoint',
            expected_manifest_sha256=created['manifest_sha256'],
            expected_state_generation=created['state_generation'])
        self.assertEqual(checked['state_generation'], created['state_generation'])
        self.assertFalse(checked['formal_production_restore_qualified'])
        restored = restore_runtime_checkpoint(
            self.root / 'checkpoint', self.root / 'restored',
            expected_manifest_sha256=created['manifest_sha256'],
            expected_state_generation=created['state_generation'])
        self.assertTrue(restored['requires_reconciliation'])
        self.assertFalse(restored['writes_authorized'])
        self.assertTrue((self.root / 'restored' / 'host' / 'manifest.json').is_file())
        risk = sqlite3.connect(self.root / 'restored' /
                               'account-risk-0000.sqlite')
        try:
            state = json.loads(risk.execute(
                'SELECT payload FROM account_risk WHERE id=1').fetchone()[0])
        finally:
            risk.close()
        original = sqlite3.connect(self.risk)
        try:
            original_state = json.loads(original.execute(
                'SELECT payload FROM account_risk WHERE id=1').fetchone()[0])
        finally:
            original.close()
        self.assertEqual(state, original_state)
        self.assertTrue(state['blocked'])
        self.assertTrue(state['daily_halted'])
        self.assertTrue(state['project_halted'])

    def test_historical_baseline_tables_are_included_in_generation_and_restore(self):
        connection = sqlite3.connect(self.db)
        connection.execute('CREATE TABLE historical_baseline_schema(version INTEGER PRIMARY KEY)')
        connection.execute('INSERT INTO historical_baseline_schema VALUES(1)')
        connection.execute('CREATE TABLE historical_baseline_manifest(baseline_id TEXT)')
        connection.execute("INSERT INTO historical_baseline_manifest VALUES('baseline-test')")
        connection.commit(); connection.close()
        created = self.create('baseline-checkpoint')
        restored = restore_runtime_checkpoint(
            self.root / 'baseline-checkpoint', self.root / 'baseline-restored',
            expected_manifest_sha256=created['manifest_sha256'],
            expected_state_generation=created['state_generation'])
        self.assertTrue(restored['requires_reconciliation'])
        restored_db = sqlite3.connect(self.root / 'baseline-restored' / 'execution.sqlite')
        try:
            self.assertEqual(restored_db.execute(
                'SELECT baseline_id FROM historical_baseline_manifest').fetchone()[0],
                'baseline-test')
        finally:
            restored_db.close()

    def test_empty_or_tampered_checkpoint_is_refused(self):
        empty = self.root / 'empty'
        empty.mkdir()
        (empty / 'manifest.json').write_text('{}')
        (empty / 'COMPLETE').write_text('{}')
        with self.assertRaises(ValueError):
            verify_runtime_checkpoint(empty, expected_manifest_sha256='0' * 64,
                                      expected_state_generation='head')
        created = self.create()
        checkpoint = self.root / 'checkpoint'
        (checkpoint / 'host' / 'requests' / 'old-request.json').write_text('{}')
        with self.assertRaises(ValueError) as raised:
            verify_runtime_checkpoint(
                checkpoint, expected_manifest_sha256=created['manifest_sha256'],
                expected_state_generation=created['state_generation'])
        self.assertIn('HASH_MISMATCH', str(raised.exception))
        self.assertEqual(created['risk_blocked'], True)

    def test_missing_risk_and_missing_host_file_are_refused(self):
        empty_risk = self.root / 'empty-sidecar' / 'account-risk-0000.sqlite'
        empty_risk.parent.mkdir()
        connection = sqlite3.connect(empty_risk)
        connection.execute('CREATE TABLE account_risk('
                           'id INTEGER PRIMARY KEY,payload TEXT)')
        connection.commit()
        connection.close()
        with self.assertRaises(ValueError) as raised:
            create_runtime_checkpoint(self.db, self.host, empty_risk,
                                      self.root / 'no-risk', account_suffix='0000')
        self.assertEqual(str(raised.exception),
                         'RUNTIME_CHECKPOINT_RISK_STATE_REQUIRED')
        created = self.create()
        checkpoint = self.root / 'checkpoint'
        (checkpoint / 'host' / 'requests' / 'old-request.json').unlink()
        with self.assertRaises(ValueError) as raised:
            verify_runtime_checkpoint(
                checkpoint, expected_manifest_sha256=created['manifest_sha256'],
                expected_state_generation=created['state_generation'])
        self.assertIn('FILE_SET_MISMATCH', str(raised.exception))

    def test_newer_generation_requires_a_new_checkpoint(self):
        created = self.create()
        connection = sqlite3.connect(self.db)
        connection.execute("UPDATE live_state SET payload='{\"newer\":true}'")
        connection.execute("INSERT INTO live_actions VALUES('late-unknown')")
        connection.execute("INSERT INTO instance_fills VALUES('late-fill')")
        connection.commit()
        connection.close()
        newer = self.create('newer-checkpoint')
        self.assertNotEqual(created['state_generation'], newer['state_generation'])
        with self.assertRaises(ValueError) as raised:
            verify_runtime_checkpoint(
                self.root / 'checkpoint',
                expected_manifest_sha256=created['manifest_sha256'],
                expected_state_generation=newer['state_generation'])
        self.assertEqual(str(raised.exception),
                         'RUNTIME_CHECKPOINT_NEWER_GENERATION_REQUIRED')
        self.assertTrue(created['state_generation'])

    def test_unconverged_wal_is_refused(self):
        Path(str(self.db) + '-wal').write_bytes(b'not-checkpointed')
        with self.assertRaises(ValueError) as raised:
            self.create()
        self.assertEqual(str(raised.exception),
                         'RUNTIME_CHECKPOINT_WAL_NOT_CONVERGED:-wal')

    def test_trusted_pin_and_latest_head_are_both_required(self):
        created = self.create()
        with self.assertRaises(ValueError) as raised:
            verify_runtime_checkpoint(
                self.root / 'checkpoint',
                expected_manifest_sha256=created['manifest_sha256'])
        self.assertEqual(str(raised.exception),
                         'RUNTIME_CHECKPOINT_LATEST_HEAD_REQUIRED')
        with self.assertRaises(ValueError) as raised:
            verify_runtime_checkpoint(
                self.root / 'checkpoint',
                expected_state_generation=created['state_generation'])
        self.assertEqual(str(raised.exception),
                         'RUNTIME_CHECKPOINT_TRUSTED_MANIFEST_REQUIRED')

    def test_risk_only_new_locks_change_full_generation_and_reject_old(self):
        self._set_risk_locks(blocked=False, daily=False, project=False)
        old = self.create('old-risk')
        self._set_risk_locks(blocked=True, daily=True, project=True)
        new = self.create('new-risk')
        self.assertNotEqual(old['state_generation'], new['state_generation'])
        with self.assertRaises(ValueError) as raised:
            verify_runtime_checkpoint(
                self.root / 'old-risk',
                expected_manifest_sha256=old['manifest_sha256'],
                expected_state_generation=new['state_generation'])
        self.assertEqual(str(raised.exception),
                         'RUNTIME_CHECKPOINT_NEWER_GENERATION_REQUIRED')
        target = self.root / 'old-restored'
        with self.assertRaises(ValueError):
            restore_runtime_checkpoint(
                self.root / 'old-risk', target,
                expected_manifest_sha256=old['manifest_sha256'],
                expected_state_generation=new['state_generation'])
        self.assertFalse(target.exists())

    def test_host_and_execution_updates_change_full_generation(self):
        old = self.create('old-host')
        (self.host / 'requests' / 'late-unknown.json').write_text('{"UNKNOWN":true}')
        host_new = self.create('new-host')
        self.assertNotEqual(old['state_generation'], host_new['state_generation'])
        connection = sqlite3.connect(self.db)
        connection.execute('CREATE TABLE account_budget_reservations('
                           'instance_id TEXT, trade_id TEXT)')
        connection.execute("INSERT INTO account_budget_reservations VALUES"
                           "('acct','late')")
        connection.commit()
        connection.close()
        budget_new = self.create('new-budget')
        self.assertNotEqual(host_new['state_generation'],
                            budget_new['state_generation'])
        with self.assertRaises(ValueError):
            verify_runtime_checkpoint(
                self.root / 'old-host',
                expected_manifest_sha256=old['manifest_sha256'],
                expected_state_generation=budget_new['state_generation'])

    def test_coherent_rewrite_without_original_pin_is_rejected(self):
        created = self.create()
        checkpoint = self.root / 'checkpoint'
        risk = checkpoint / 'account-risk-0000.sqlite'
        connection = sqlite3.connect(risk)
        state = json.loads(connection.execute(
            'SELECT payload FROM account_risk WHERE id=1').fetchone()[0])
        state.update(blocked=False, daily_halted=False, project_halted=False)
        connection.execute('UPDATE account_risk SET payload=? WHERE id=1',
                           (json.dumps(state),))
        connection.commit()
        connection.close()
        manifest_path = checkpoint / 'manifest.json'
        manifest = json.loads(manifest_path.read_text())
        manifest['risk'].update(state_sha256=digest(state), blocked=False,
                                daily_halted=False, project_halted=False)
        manifest['risk']['state_sha256'] = digest(state)
        manifest['files'][manifest['account_risk_sidecar']] = _hash(risk)
        manifest['state_generation'] = _full_generation(
            checkpoint / manifest['execution_sqlite'],
            risk, checkpoint / manifest['host_mailbox'],
            account_suffix=manifest['account_suffix'],
            source_fingerprint=manifest['source_fingerprint'],
            policy_sha256=manifest['host_binding']['policy_sha256'])
        raw = (canonical(manifest) + '\n').encode()
        manifest_path.write_bytes(raw)
        new_pin = hashlib.sha256(raw).hexdigest()
        (checkpoint / 'COMPLETE').write_text(canonical(
            {'schema_version': 1, 'manifest_sha256': new_pin}) + '\n')
        self.assertEqual(verify_runtime_checkpoint(
            checkpoint, expected_manifest_sha256=new_pin,
            expected_state_generation=manifest['state_generation']
        )['status'], 'RUNTIME_CHECKPOINT_VERIFIED')
        with self.assertRaises(ValueError) as raised:
            verify_runtime_checkpoint(
                checkpoint, expected_manifest_sha256=created['manifest_sha256'],
                expected_state_generation=manifest['state_generation'])
        self.assertEqual(str(raised.exception),
                         'RUNTIME_CHECKPOINT_TRUSTED_MANIFEST_MISMATCH')
        self.assertNotEqual(created['manifest_sha256'], new_pin)
        target = self.root / 'rewritten-restored'
        with self.assertRaises(ValueError):
            restore_runtime_checkpoint(
                checkpoint, target,
                expected_manifest_sha256=created['manifest_sha256'],
                expected_state_generation=manifest['state_generation'])
        self.assertFalse(target.exists())

    def test_inspection_is_never_qualified_or_write_authorized(self):
        created = self.create()
        inspected = inspect_runtime_checkpoint(self.root / 'checkpoint')
        self.assertEqual(inspected['status'], 'RUNTIME_CHECKPOINT_INSPECTED')
        self.assertFalse(inspected['trading_runtime_restore_qualified'])
        self.assertTrue(inspected['offline_inspection_only'])
        self.assertFalse(inspected['writes_authorized'])
        self.assertEqual(inspected['manifest_sha256'],
                         created['manifest_sha256'])

    def test_real_old_artifact_metadata_is_refused_before_restore(self):
        created = self.create()
        target = self.root / 'restored'
        artifact = (Path(__file__).resolve().parent /
                    'fixtures' / 'legacy-artifact-579e')
        with self.assertRaises(ValueError) as raised:
            preflight_runtime_checkpoint(
                self.root / 'checkpoint',
                expected_manifest_sha256=created['manifest_sha256'],
                expected_state_generation=created['state_generation'],
                artifact_directory=artifact)
        self.assertIn('ARTIFACT_SOURCE_MISMATCH', str(raised.exception))
        self.assertFalse(target.exists())
        self.assertTrue((self.root / 'checkpoint' / 'manifest.json').is_file())
        self.assertTrue(created['state_generation'])

    def test_artifact_compatible_requires_trusted_build_and_input_pins(self):
        created = self.create()
        artifact = self.root / 'artifact'
        artifact.mkdir()
        manifest = {'source_fingerprint': source_fingerprint(),
                    'admission_schema_version': 2,
                    'input_set_sha256': 'a' * 64}
        raw = (json.dumps(manifest, sort_keys=True) + '\n').encode()
        (artifact / 'manifest.json').write_bytes(raw)
        manifest_sha = hashlib.sha256(raw).hexdigest()
        with self.assertRaises(ValueError) as raised:
            preflight_runtime_checkpoint(
                self.root / 'checkpoint',
                expected_manifest_sha256=created['manifest_sha256'],
                expected_state_generation=created['state_generation'],
                artifact_directory=artifact)
        self.assertEqual(str(raised.exception),
                         'RUNTIME_CHECKPOINT_ARTIFACT_TRUSTED_PIN_REQUIRED')
        result = preflight_runtime_checkpoint(
            self.root / 'checkpoint',
            expected_manifest_sha256=created['manifest_sha256'],
            expected_state_generation=created['state_generation'],
            artifact_directory=artifact,
            expected_artifact_manifest_sha256=manifest_sha,
            expected_artifact_input_set_sha256='a' * 64)
        self.assertTrue(result['artifact_compatible'])
        self.assertEqual(result['artifact_manifest_sha256'], manifest_sha)

    def test_artifact_schema_must_be_declared(self):
        created = self.create()
        artifact = self.root / 'artifact'
        artifact.mkdir()
        (artifact / 'manifest.json').write_text(json.dumps({
            'source_fingerprint': source_fingerprint()}))
        with self.assertRaises(ValueError) as raised:
            preflight_runtime_checkpoint(
                self.root / 'checkpoint',
                expected_manifest_sha256=created['manifest_sha256'],
                expected_state_generation=created['state_generation'],
                artifact_directory=artifact)
        self.assertEqual(str(raised.exception),
                         'RUNTIME_CHECKPOINT_ARTIFACT_SCHEMA_UNKNOWN')
