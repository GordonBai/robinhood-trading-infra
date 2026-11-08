from copy import deepcopy
from datetime import timedelta
import json
import sqlite3
from unittest import TestCase
from unittest.mock import patch

from entry_authority_fixture import TestExecutionBook as ExecutionBook
from martin_agentic.native_operations import NativeOperations
import test_native_operations


class NativeReceiptAtomicityTests(TestCase):
    def setUp(self):
        self.fixture=test_native_operations.NativeOperationTests();self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.book=self.fixture.book;self.ops=self.fixture.ops

    def fail_native_result(self):
        self.book.db.execute("""CREATE TRIGGER fail_native_result BEFORE UPDATE ON native_operation_records
            WHEN NEW.state='RECORDED' BEGIN SELECT RAISE(ABORT,'synthetic receipt write failure'); END""")
        self.book.db.commit()

    def check_atomic_failure(self,packet,response):
        before=self.book.audit.verify()
        mappings=self.book.db.execute('SELECT * FROM external_broker_order_ids ORDER BY order_id').fetchall()
        aliases=self.book.db.execute('SELECT * FROM external_broker_refs ORDER BY ref_id').fetchall()
        self.fail_native_result()
        with self.assertRaises(sqlite3.IntegrityError):self.ops.record(packet['operation_id'],response)
        self.assertFalse(self.book.db.in_transaction)
        action=next(a for a in self.book.actions() if a['id']==packet['action_id'])
        self.assertEqual(action['state'],'RUNNING')
        self.assertEqual(self.book.db.execute('SELECT report FROM external_operations WHERE operation_id=?',(packet['operation_id'],)).fetchone(),(None,))
        self.assertEqual(self.book.db.execute('SELECT state,result FROM native_operation_records WHERE id=?',(packet['operation_id'],)).fetchone(),('STARTED',None))
        self.assertEqual(self.book.audit.verify(),before)
        self.assertEqual(self.book.db.execute('SELECT * FROM external_broker_order_ids ORDER BY order_id').fetchall(),mappings)
        self.assertEqual(self.book.db.execute('SELECT * FROM external_broker_refs ORDER BY ref_id').fetchall(),aliases)
        self.book.db.execute('DROP TRIGGER fail_native_result');self.book.db.commit()
        result=self.ops.record(packet['operation_id'],response)
        after=self.book.audit.verify()
        self.assertEqual(self.ops.record(packet['operation_id'],response),result)
        self.assertEqual(self.book.audit.verify(),after)
        self.assertFalse(result['broker_effect_verified'])
        return result

    def test_review_result_action_report_and_audit_commit_together(self):
        packet,response=self.fixture.review()
        result=self.check_atomic_failure(packet,response)
        self.assertTrue(result['requires_native_confirmation'])
        self.assertEqual(next(a for a in self.book.actions() if a['id']==packet['action_id'])['state'],'WAITING_USER')

    def test_order_identity_and_submitted_report_roll_back_with_native_result(self):
        packet=self.fixture.place_packet();response=self.fixture.official_order(packet)
        result=self.check_atomic_failure(packet,response)
        self.assertEqual(result['report']['status'],'submitted')
        self.assertEqual(self.book.db.execute('SELECT order_id FROM external_broker_order_ids').fetchone(),('official-order',))
        self.assertNotEqual(next(iter(self.book.state()['trades'].values()))['phase'],'PROTECTED')

    def test_pending_approval_alias_and_report_roll_back_with_native_result(self):
        packet=self.fixture.place_packet();response=self.fixture.official_approval()
        result=self.check_atomic_failure(packet,response)
        self.assertEqual(result['report']['status'],'pending_approval')
        self.assertEqual(self.book.db.execute('SELECT ref_id FROM external_broker_refs').fetchone(),('official-approval',))
        self.assertNotEqual(next(iter(self.book.state()['trades'].values()))['phase'],'PROTECTED')

    def test_cancel_acceptance_is_atomic_but_not_a_cancel_terminal(self):
        place=self.fixture.place_packet();response=self.fixture.official_order(place)
        self.ops.record(place['operation_id'],response)
        stop=response['structuredContent']['data']['order']
        snapshot=self.fixture.f.snap([self.fixture.buy,stop],qty='2',held='2')
        self.book.reconcile(snapshot)
        quote={**self.fixture.f.quote,'bid':'38.99','ask':'39'}
        self.book.reconcile(snapshot,quotes={'DEMO':quote})
        action=next(a for a in self.book.actions() if a['kind']=='cancel')
        packet=self.ops.begin(action['id'],'cancel',snapshot)
        self.assertTrue(packet['requires_native_confirmation'])
        result=self.check_atomic_failure(packet,{'data':{'accepted':True}})
        self.assertEqual(result['report']['status'],'cancel_requested')
        self.assertFalse(result['cancel_terminal_verified'])

    def test_unknown_write_remains_unknown_after_atomic_import(self):
        packet=self.fixture.place_packet()
        result=self.check_atomic_failure(packet,{'isError':True,'content':[{'type':'text','text':'timeout'}]})
        self.assertEqual(result['report']['status'],'unknown')
        self.assertEqual(next(a for a in self.book.actions() if a['id']==packet['action_id'])['state'],'UNKNOWN')

    def test_legacy_partial_core_report_can_complete_without_duplicate_audit(self):
        packet,response=self.fixture.review()
        report={'status':'needs_confirmation','message':'NATIVE_REVIEW_RECORDED_FOLLOW_CURRENT_TOOL_CONFIRMATION',
            'market_data_disclosure':response['data']['market_data_disclosure']}
        self.book.record_external_report(packet['action_id'],report,operation_id=packet['operation_id'])
        before=self.book.audit.verify()
        self.fail_native_result()
        with self.assertRaises(sqlite3.IntegrityError):self.ops.record(packet['operation_id'],response)
        self.assertFalse(self.book.db.in_transaction);self.assertEqual(self.book.audit.verify(),before)
        self.book.db.execute('DROP TRIGGER fail_native_result');self.book.db.commit()
        result=self.ops.record(packet['operation_id'],response)
        self.assertTrue(result['review_verified']);self.assertTrue(result['requires_native_confirmation'])
        self.assertEqual(self.book.audit.verify(),before)
        self.assertEqual(self.book.db.execute('SELECT state FROM native_operation_records WHERE id=?',(packet['operation_id'],)).fetchone(),('RECORDED',))

    def interleaved_import(self,*,conflicting=False):
        packet,response=self.fixture.review();other_response=deepcopy(response)
        if conflicting:other_response['guide']='different retained guide'
        other=ExecutionBook(self.fixture.f.path,self.fixture.f.policy,simulation=True,clock=lambda:self.fixture.f.now)
        self.addCleanup(other.close);other_ops=NativeOperations(other)
        original=self.book.record_external_report;committed=[]
        def interleave(*args,**kwargs):
            self.fixture.f.now+=timedelta(milliseconds=1)
            committed.append(other_ops.record(packet['operation_id'],other_response))
            return original(*args,**kwargs)
        with patch.object(self.book,'record_external_report',side_effect=interleave):
            if conflicting:
                with self.assertRaisesRegex(ValueError,'NATIVE_RESPONSE_ID_CONFLICT'):self.ops.record(packet['operation_id'],response)
            else:self.assertEqual(self.ops.record(packet['operation_id'],response),committed[0])
        stored=json.loads(self.book.db.execute('SELECT result FROM native_operation_records WHERE id=?',(packet['operation_id'],)).fetchone()[0])
        self.assertEqual(stored,committed[0]);self.assertTrue(self.book.audit.verify()['integrity_valid'])

    def test_duplicate_import_after_interleaved_commit_keeps_original_result(self):
        self.interleaved_import()

    def test_conflicting_import_after_interleaved_commit_cannot_overwrite_response(self):
        self.interleaved_import(conflicting=True)
