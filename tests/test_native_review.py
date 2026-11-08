import hashlib
import json
from pathlib import Path
from unittest import TestCase
from unittest.mock import MagicMock,patch

from martin_agentic.control_tasks import worker_lease
from martin_agentic.native_review import EquityReviewTransport,NativeReviewFailure,run_native_review
import test_execution


class NativeReviewTests(TestCase):
    def setUp(self):
        self.f=test_execution.ExecutionTests();self.f.setUp();self.addCleanup(self.f.tearDown)
        self.buy=self.f.entry_filled();self.action=next(a['id'] for a in self.f.book.actions() if a['kind']=='protect')
        self.snapshot=self.f.snap([self.buy],qty='2')
        self.calls=[]
        outer=self
        class Factory:
            simulation_only=True
            def __init__(self,**kwargs):self.rpc=MagicMock();outer.calls.append(kwargs)
            def __enter__(self):return self
            def __exit__(self,*args):self.rpc.close()
            def execute_review(self,packet):
                outer.calls.append(packet)
                return {'data':{**packet['parameters'],'order_checks':{},'customer_approval_required':False,
                    'market_data_disclosure':'Exact formal disclosure'}}
        self.factory=Factory

    def run_review(self):
        return run_native_review(self.f.book,self.action,self.snapshot,self.f.quote,
            Path(self.f.tmp.name)/'receipts',transport_factory=self.factory)

    def test_core_review_receipt_preserves_confirmation_without_placing_or_model(self):
        result=self.run_review();receipt=json.loads(Path(result['receipt_file']).read_text())
        self.assertEqual(result['status'],'WAITING_USER');self.assertEqual(result['model_calls'],0)
        self.assertEqual(result['broker_writes'],0);self.assertFalse(result['execution_authorized'])
        self.assertTrue(result['native_result']['requires_native_confirmation'])
        self.assertEqual(receipt['packet']['operation'],'review')
        self.assertEqual(next(a for a in self.f.book.actions() if a['id']==self.action)['state'],'WAITING_USER')
        self.assertEqual(receipt['binding']['account_key'],self.snapshot['account_key'])
        self.assertTrue(self.f.book.audit.verify()['integrity_valid'])

    def test_synthetic_book_cannot_reach_real_transport(self):
        with patch.object(EquityReviewTransport,'__init__',side_effect=AssertionError('must not connect')):
            with self.assertRaisesRegex(ValueError,'PROVENANCE_MISMATCH'):
                run_native_review(self.f.book,self.action,self.snapshot,self.f.quote,self.f.tmp.name)

    def test_held_worker_lease_prevents_reservation_or_transport(self):
        held=worker_lease(self.f.path)
        try:
            with self.assertRaisesRegex(ValueError,'WORKER_OWNER_STILL_ACTIVE'):self.run_review()
            self.assertEqual(self.calls,[])
            self.assertEqual(next(a for a in self.f.book.actions() if a['id']==self.action)['state'],'READY')
        finally:held.close()

    def test_connect_delay_revalidates_freshness_before_reservation(self):
        from datetime import timedelta
        original=self.factory.__enter__
        def delayed(instance):self.f.now+=timedelta(seconds=40);return original(instance)
        with patch.object(self.factory,'__enter__',delayed):
            with self.assertRaises(ValueError):self.run_review()
        self.assertEqual(len(self.calls),1)
        self.assertEqual(next(a for a in self.f.book.actions() if a['id']==self.action)['state'],'READY')

    def test_receipt_failure_cannot_leave_order_or_confirmation_success(self):
        with patch('martin_agentic.native_review.os.fsync',side_effect=OSError('synthetic disk failure')):
            with self.assertRaises(NativeReviewFailure) as error:self.run_review()
        self.assertTrue(error.exception.details['failed_review_recorded'])
        action=next(a for a in self.f.book.actions() if a['id']==self.action)
        self.assertEqual(action['state'],'WAITING_USER')
        self.assertEqual(json.loads(action['report'])['status'],'review_only')

    def test_transport_error_is_a_failed_review_without_unknown_write(self):
        with patch.object(self.factory,'execute_review',side_effect=RuntimeError('private upstream text')):
            result=self.run_review()
        self.assertEqual(result['status'],'REVIEW_FAILED')
        self.assertNotIn('private upstream text',json.dumps(result))
        self.assertEqual(next(a for a in self.f.book.actions() if a['id']==self.action)['state'],'WAITING_USER')

    def test_directory_cleanup_and_serialization_failures_release_review_core_state(self):
        failures=(('mkdir',lambda:patch('martin_agentic.native_review.Path.mkdir',side_effect=OSError('private detail'))),
            ('chmod',lambda:patch('martin_agentic.native_review.Path.chmod',side_effect=OSError('private detail'))),
            ('serialize',lambda:patch('martin_agentic.native_review.canonical',side_effect=TypeError('private detail'))))
        for label,patcher in failures:
            with self.subTest(label=label),patcher():
                with self.assertRaises(NativeReviewFailure) as error:self.run_review()
                self.assertTrue(error.exception.details['failed_review_recorded'])
                self.assertNotIn('private detail',str(error.exception))
            self.assertEqual(next(a for a in self.f.book.actions() if a['id']==self.action)['state'],'WAITING_USER')
        with patch.object(self.factory,'__exit__',side_effect=OSError('cleanup failed')):
            with self.assertRaises(NativeReviewFailure) as error:self.run_review()
        self.assertTrue(error.exception.details['failed_review_recorded'])
        self.assertEqual(next(a for a in self.f.book.actions() if a['id']==self.action)['state'],'WAITING_USER')

    def test_failed_core_failure_record_preserves_explicit_recovery_identity(self):
        with (patch('martin_agentic.native_review.Path.mkdir',side_effect=OSError('disk unavailable')),
                patch('martin_agentic.native_review.NativeOperations.record',side_effect=OSError('ledger unavailable'))):
            with self.assertRaises(NativeReviewFailure) as error:self.run_review()
        details=error.exception.details
        self.assertTrue(details['pending_operation_requires_recovery'])
        self.assertFalse(details['failed_review_recorded']);self.assertTrue(details['operation_id'])
        self.assertEqual(next(a for a in self.f.book.actions() if a['id']==self.action)['state'],'RUNNING')

    def test_native_reservation_insert_failure_rolls_back_action_intent_and_audit_together(self):
        from martin_agentic.native_operations import NativeOperations
        NativeOperations(self.f.book)
        self.f.book.db.execute("""CREATE TRIGGER fail_native_review_insert BEFORE INSERT ON native_operation_records
            BEGIN SELECT RAISE(ABORT,'synthetic native insert failure'); END""")
        self.f.book.db.commit()
        before=self.f.book.audit.verify()
        count=self.f.book.db.execute('SELECT count(*) FROM external_operations').fetchone()[0]
        with self.assertRaises(NativeReviewFailure) as error:self.run_review()
        self.assertFalse(error.exception.details['pending_operation_requires_recovery'])
        self.assertEqual(next(a for a in self.f.book.actions() if a['id']==self.action)['state'],'READY')
        self.assertEqual(self.f.book.db.execute('SELECT count(*) FROM external_operations').fetchone()[0],count)
        self.assertEqual(self.f.book.audit.verify(),before)

    def test_transport_denies_writes_and_changed_account_before_any_review(self):
        transport=object.__new__(EquityReviewTransport)
        account={'account_number':'TEST0000','agentic_allowed':True,'state':'active',
            'deactivated':False,'permanently_deactivated':False}
        transport._read_call=MagicMock(return_value={'data':{'accounts':[account]}})
        packet={'operation':'review','tool':'review_equity_order','account':'••••0000',
            'account_key':hashlib.sha256(b'TEST0000').hexdigest(),
            'parameters':{'symbol':'SOFI','side':'buy','type':'limit','quantity':'1',
                'limit_price':'1','time_in_force':'gfd','market_hours':'regular_hours'}}
        for tool in ('place_equity_order','cancel_equity_order','review_equity_order','get_equity_quotes'):
            with self.assertRaisesRegex(ValueError,'TOOL_DENIED'):transport(tool,{})
        transport._read_call.assert_not_called()
        bad={**packet,'account_key':'different'}
        with self.assertRaisesRegex(ValueError,'BINDING_CHANGED'):transport.execute_review(bad)
        self.assertEqual([c.args[0] for c in transport._read_call.call_args_list],['get_accounts'])
        transport._read_call.reset_mock();transport.execute_review(packet)
        self.assertEqual([c.args[0] for c in transport._read_call.call_args_list],['get_accounts','review_equity_order'])
