from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
from unittest import TestCase
from unittest.mock import patch

from martin_agentic.audit import AuditTrail,canonical
from martin_agentic.local_audit_storage import create_local_backup,verify_local_backup,restore_local_backup


class LocalAuditStorageTests(TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.db=self.root/'audit.sqlite'
        self.audit=AuditTrail(self.db);self.addCleanup(self.audit.close)
        self.now=datetime(2026,10,27,16,tzinfo=timezone.utc)
        self.append('one');self.bundle=self.root/'backup';self.target=self.root/'restored.sqlite'

    def append(self,identity,audit=None):
        (audit or self.audit).append({'event_id':identity,'occurred_at':self.now.isoformat(),
            'event_type':'ACCOUNT_OBSERVED','actor':'simulation','synthetic':True,'details':{'fixture':True}})

    def test_backup_verify_restore_and_append_are_private_and_local_only(self):
        source_before=self.db.read_bytes();result=create_local_backup(self.db,self.bundle)
        self.assertEqual(result['status'],'LOCAL_AUDIT_BACKUP_CREATED')
        self.assertEqual(self.db.read_bytes(),source_before)
        checked=verify_local_backup(self.bundle,expected_manifest_sha256=result['manifest_sha256'])
        self.assertEqual(checked['audit']['events_verified'],1)
        self.assertEqual(self.bundle.stat().st_mode&0o777,0o700)
        for file in self.bundle.iterdir():self.assertEqual(file.stat().st_mode&0o777,0o600)
        self.append('two')
        restored=restore_local_backup(self.bundle,self.target,expected_manifest_sha256=result['manifest_sha256'])
        self.assertTrue(restored['local_only']);self.assertFalse(restored['independent_immutable_storage_verified'])
        self.assertFalse(restored['trading_runtime_restore_qualified']);self.assertFalse(restored['account_risk_sidecar_included'])
        self.assertEqual(self.target.stat().st_mode&0o777,0o600)
        audit=AuditTrail(self.target)
        try:
            self.assertEqual(audit.verify()['events_verified'],1)
            self.append('restored-two',audit);self.assertEqual(audit.verify()['events_verified'],2)
        finally:audit.close()
        self.assertEqual(self.audit.verify()['events_verified'],2)

    def test_wal_committed_data_and_unknown_state_survive_consistent_backup(self):
        self.audit.db.execute('PRAGMA journal_mode=WAL')
        self.audit.db.execute('CREATE TABLE unknown_intents(id TEXT,state TEXT)')
        self.audit.db.execute("INSERT INTO unknown_intents VALUES('original','UNKNOWN')");self.audit.db.commit()
        self.append('committed-wal')
        create_local_backup(self.db,self.bundle);restore_local_backup(self.bundle,self.target)
        with sqlite3.connect(self.target) as db:
            self.assertEqual(db.execute('SELECT * FROM unknown_intents').fetchall(),[('original','UNKNOWN')])
            self.assertEqual(db.execute('SELECT count(*) FROM audit_events').fetchone()[0],2)

    def test_tampered_db_is_never_restored(self):
        create_local_backup(self.db,self.bundle)
        with (self.bundle/'audit.sqlite').open('ab') as f:f.write(b'changed')
        with self.assertRaisesRegex(ValueError,'HASH_MISMATCH'):restore_local_backup(self.bundle,self.target)
        self.assertFalse(self.target.exists())

    def test_missing_commit_or_extra_files_are_not_complete_backups(self):
        create_local_backup(self.db,self.bundle)
        (self.bundle/'COMPLETE').unlink()
        with self.assertRaisesRegex(ValueError,'INCOMPLETE'):verify_local_backup(self.bundle)
        self.assertFalse(self.target.exists())

    def test_local_owner_rewriting_manifest_is_detected_by_separately_held_hash(self):
        result=create_local_backup(self.db,self.bundle)
        manifest=json.loads((self.bundle/'manifest.json').read_text());manifest['created_at']='2026-10-27T17:00:00+00:00'
        raw=(canonical(manifest)+'\n').encode();(self.bundle/'manifest.json').write_bytes(raw)
        new_hash=hashlib.sha256(raw).hexdigest()
        (self.bundle/'COMPLETE').write_text(canonical({'schema_version':1,'manifest_sha256':new_hash}))
        self.assertFalse(verify_local_backup(self.bundle)['independent_immutable_storage_verified'])
        with self.assertRaisesRegex(ValueError,'TRUSTED_MANIFEST_MISMATCH'):
            restore_local_backup(self.bundle,self.target,expected_manifest_sha256=result['manifest_sha256'])

    def test_invalid_audit_chain_cannot_be_backed_up(self):
        self.audit.db.execute('DROP TRIGGER audit_no_update')
        self.audit.db.execute("UPDATE audit_events SET payload='{}'");self.audit.db.commit()
        with self.assertRaises(ValueError):create_local_backup(self.db,self.bundle)
        self.assertFalse(self.bundle.exists())

    def test_existing_backup_and_restore_destination_are_never_overwritten(self):
        create_local_backup(self.db,self.bundle);original=(self.bundle/'manifest.json').read_bytes()
        with self.assertRaises(FileExistsError):create_local_backup(self.db,self.bundle)
        self.target.write_bytes(b'existing')
        with self.assertRaises(FileExistsError):restore_local_backup(self.bundle,self.target)
        self.assertEqual(self.target.read_bytes(),b'existing');self.assertEqual((self.bundle/'manifest.json').read_bytes(),original)

    def test_destination_sidecars_or_lock_require_a_new_target(self):
        create_local_backup(self.db,self.bundle)
        for suffix in ('-wal','-shm','-journal','.lock'):
            sidecar=Path(str(self.target)+suffix);sidecar.write_bytes(b'original')
            with self.assertRaises(FileExistsError):restore_local_backup(self.bundle,self.target)
            self.assertFalse(self.target.exists());self.assertEqual(sidecar.read_bytes(),b'original');sidecar.unlink()

    def test_symlinks_and_path_substitution_metadata_are_rejected(self):
        create_local_backup(self.db,self.bundle)
        snapshot=self.bundle/'audit.sqlite';snapshot.unlink();snapshot.symlink_to(self.db)
        with self.assertRaisesRegex(ValueError,'SYMLINK'):verify_local_backup(self.bundle)
        linked=self.root/'linked.sqlite';linked.symlink_to(self.db)
        with self.assertRaisesRegex(ValueError,'SOURCE_INVALID'):create_local_backup(linked,self.root/'other')

    def test_failure_before_complete_marker_never_publishes_a_valid_backup(self):
        link=os.link
        def fail_marker(source,target,**kwargs):
            if Path(target).name=='COMPLETE':raise OSError('injected marker publication failure')
            return link(source,target,**kwargs)
        with patch('martin_agentic.local_audit_storage.os.link',side_effect=fail_marker):
            with self.assertRaises(OSError):create_local_backup(self.db,self.bundle)
        self.assertTrue(self.bundle.exists());self.assertFalse((self.bundle/'COMPLETE').exists())
        with self.assertRaisesRegex(ValueError,'INCOMPLETE'):verify_local_backup(self.bundle)

    def test_manifest_cannot_claim_external_or_runtime_restore_qualification(self):
        create_local_backup(self.db,self.bundle)
        manifest=json.loads((self.bundle/'manifest.json').read_text());manifest['trading_runtime_restore_qualified']=True
        raw=canonical(manifest).encode();(self.bundle/'manifest.json').write_bytes(raw)
        (self.bundle/'COMPLETE').write_text(canonical({'schema_version':1,'manifest_sha256':hashlib.sha256(raw).hexdigest()}))
        with self.assertRaisesRegex(ValueError,'MANIFEST_INVALID'):verify_local_backup(self.bundle)
