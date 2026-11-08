from copy import deepcopy
from datetime import timedelta
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
from unittest import TestCase

from martin_agentic.audit import digest
from martin_agentic.control_tasks import worker_lease
from entry_authority_fixture import TestExecutionBook as ExecutionBook
from martin_agentic.native_review import persist_review_receipt
from martin_agentic.native_review_recovery import recover_native_review
import test_native_operations


class NativeReviewRecoveryTests(TestCase):
    def setUp(self):
        self.fixture=test_native_operations.NativeOperationTests();self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.f=self.fixture.f;self.book=self.fixture.book
        self.packet,self.response=self.fixture.review()
        self.directory=Path(self.f.tmp.name)/'receipts'
        self.binding={'db':str(self.f.path.resolve()),'account_key':self.book.state()['account_key'],
            'policy_sha256':digest(self.book.policy),'synthetic':True}
        self.path=Path(persist_review_receipt(self.packet,self.response,self.binding,self.directory,self.f.now.isoformat()))

    def recover(self):return recover_native_review(self.book,self.packet['operation_id'],self.directory)

    def test_process_exits_after_fsync_then_reopened_book_recovers_without_replaying(self):
        self.path.unlink()
        payload=Path(self.f.tmp.name)/'input.json'
        payload.write_text(json.dumps([self.packet,self.response,self.binding,str(self.directory),self.f.now.isoformat()]))
        code=('import json,os,sys; from martin_agentic.native_review import persist_review_receipt; '
            'persist_review_receipt(*json.load(open(sys.argv[1]))); os._exit(73)')
        child=subprocess.run([sys.executable,'-c',code,str(payload)],capture_output=True,text=True,timeout=10)
        self.assertEqual(child.returncode,73,child.stderr);self.assertTrue(self.path.is_file())
        self.book.close()
        self.book=ExecutionBook(self.f.path,self.f.policy,simulation=True,clock=lambda:self.f.now)
        self.f.book=self.book
        self.book.recover()
        self.assertEqual(next(a for a in self.book.actions() if a['id']==self.packet['action_id'])['state'],'RUNNING')
        result=self.recover()
        self.assertEqual(result['status'],'REVIEW_METADATA_RECOVERED')
        self.assertEqual(result['action_state'],'WAITING_USER')
        self.assertEqual(result['broker_calls'],0);self.assertFalse(result['execution_authorized'])
        self.assertTrue(result['native_result']['requires_native_confirmation'])
        self.assertEqual(result['native_result']['market_data_disclosure'],'Exact disclosure text')
        after=self.book.audit.verify()
        self.assertEqual(self.recover()['native_result'],result['native_result'])
        self.assertEqual(self.book.audit.verify(),after)

    def test_expired_receipt_recovers_metadata_without_refreshing_original_review(self):
        self.f.now+=timedelta(seconds=31)
        result=self.recover()
        self.assertEqual(result['original_review_created_at'],self.packet_created_at())
        self.f.quote.update({k:self.f.now.isoformat() for k in ('observed_at','bid_at','ask_at')})
        with self.assertRaises(ValueError):
            self.fixture.ops.begin(self.fixture.action,'place',self.fixture.snapshot(),quote=self.f.quote)
        self.assertEqual(self.book.db.execute("SELECT count(*) FROM native_operation_records WHERE operation='place'").fetchone()[0],0)

    def test_late_review_receipt_cannot_regress_broker_observed_protection(self):
        stop=self.fixture.official_order(self.packet)['structuredContent']['data']['order']
        self.book.reconcile(self.f.snap([self.fixture.buy,stop],qty='2',held='2'))
        action=next(a for a in self.book.actions() if a['id']==self.packet['action_id'])
        self.assertEqual(action['state'],'OBSERVED')
        result=self.recover()
        self.assertEqual(result['action_state'],'OBSERVED')
        self.assertEqual(next(iter(self.book.state()['trades'].values()))['phase'],'PROTECTED')
        self.assertFalse(result['broker_effect_verified'])

    def packet_created_at(self):
        return self.book.db.execute('SELECT created_at FROM native_operation_records WHERE id=?',(self.packet['operation_id'],)).fetchone()[0]

    def test_mismatched_receipts_fail_without_changing_ledger_or_audit(self):
        original=json.loads(self.path.read_text())
        changes=[('binding','db','another.sqlite'),('binding','account_key','another'),
            ('binding','policy_sha256','another'),('binding','synthetic',1),
            ('binding','source_fingerprint','malformed'),('binding','extra',True),
            ('packet','tool','place_equity_order'),('packet','action_id','another')]
        malformed=[]
        for section,key,value in changes:
            r=deepcopy(original);r[section][key]=value;malformed.append(r)
        for key,value in [('packet_sha256','bad'),('response_sha256','bad'),
                ('broker_effect_verified',True),('received_at',(self.f.now+timedelta(seconds=1)).isoformat()),
                ('received_at',(self.f.now-timedelta(seconds=1)).isoformat())]:
            r=deepcopy(original);r[key]=value;malformed.append(r)
        for receipt in malformed:
            with self.subTest(receipt=receipt):
                self.path.write_text(json.dumps(receipt));before=self.book.audit.verify()
                with self.assertRaises(ValueError):self.recover()
                self.assertEqual(self.book.audit.verify(),before)
                self.assertEqual(self.book.db.execute('SELECT state FROM native_operation_records WHERE id=?',(self.packet['operation_id'],)).fetchone()[0],'STARTED')

    def test_async_receipt_from_previous_build_is_metadata_not_current_authority(self):
        receipt=json.loads(self.path.read_text());receipt['binding']['source_fingerprint']='a'*64
        self.path.write_text(json.dumps(receipt))
        self.assertFalse(self.recover()['execution_authorized'])

    def test_active_worker_blocks_recovery_and_releases_no_foreign_lease(self):
        lease=worker_lease(self.f.path)
        try:
            with self.assertRaisesRegex(ValueError,'WORKER_OWNER_STILL_ACTIVE'):self.recover()
            self.assertIsNone(worker_lease(self.f.path))
        finally:lease.close()
        self.assertEqual(self.recover()['status'],'REVIEW_METADATA_RECOVERED')

    def test_import_failure_preserves_receipt_and_unresolved_intent_then_can_retry(self):
        raw=self.path.read_bytes()
        self.book.db.execute("""CREATE TRIGGER fail_recovery BEFORE UPDATE ON native_operation_records
            BEGIN SELECT RAISE(ABORT,'synthetic disk failure'); END""");self.book.db.commit()
        before=self.book.audit.verify()
        with self.assertRaises(sqlite3.IntegrityError):self.recover()
        self.assertEqual(self.book.audit.verify(),before);self.assertEqual(self.path.read_bytes(),raw)
        self.assertEqual(self.book.db.execute('SELECT state FROM native_operation_records WHERE id=?',(self.packet['operation_id'],)).fetchone()[0],'STARTED')
        self.book.db.execute('DROP TRIGGER fail_recovery');self.book.db.commit()
        self.assertEqual(self.recover()['status'],'REVIEW_METADATA_RECOVERED')

    def test_place_and_cancel_operations_cannot_use_review_recovery(self):
        for operation in ('place','cancel'):
            self.book.db.execute('UPDATE native_operation_records SET operation=? WHERE id=?',(operation,self.packet['operation_id']));self.book.db.commit()
            with self.assertRaisesRegex(ValueError,'REVIEW_ONLY'):self.recover()

    def test_symlink_oversize_and_fifo_are_rejected_without_hanging(self):
        target=self.path.with_suffix('.original');self.path.rename(target)
        self.path.symlink_to(target)
        with self.assertRaises(OSError):self.recover()
        self.path.unlink();self.path.write_bytes(b' '*2_000_001)
        with self.assertRaisesRegex(ValueError,'TOO_LARGE'):self.recover()
        self.path.unlink();os.mkfifo(self.path)
        with self.assertRaisesRegex(ValueError,'REGULAR_FILE_REQUIRED'):self.recover()

    def test_invalid_operation_cannot_escape_receipt_directory(self):
        with self.assertRaisesRegex(ValueError,'OPERATION_ID_INVALID'):
            recover_native_review(self.book,'../../other',self.directory)
