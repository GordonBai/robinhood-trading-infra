"""Synthetic owner-only engineering exit, preserving native cancellation gates."""
from datetime import timedelta
import json
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from martin_agentic.host_execution import HostExecutionBridge
from martin_agentic.host_mailbox import submit_engineering_exit,read_request_result
import test_native_lifecycle


class HostEngineeringExitTests(TestCase):
    def setUp(self):
        self.f=test_native_lifecycle.NativeLifecycleTests();self.f.setUp()
        self.addCleanup(self.f.tearDown);self.book=self.f.book
        entry=self.f.engineering_entry()
        payload=self.f.place(entry['id'],self.f.snap())
        self.f.orders=[self.f.raw(payload,state='filled',filled='2')]
        self.f.now+=timedelta(seconds=1);self.book.reconcile(self.f.snap('2'))
        protect=self.f.action('protect');payload=self.f.place(protect['id'],self.f.snap('2'))
        self.f.orders.append(self.f.raw(payload))
        self.f.now+=timedelta(seconds=1);self.book.reconcile(self.f.snap('2','2'))
        self.assertEqual(self.book.state()['trades']['engineering-native-fixture']['phase'],'PROTECTED')
        self.directory=Path(self.f.temp.name)/'host';self.bridge=HostExecutionBridge(self.book,self.directory)
        self.addCleanup(self.bridge.close)
        deny=patch('martin_agentic.native_review.EquityReviewTransport.__init__',side_effect=AssertionError('No broker calls'))
        deny.start();self.addCleanup(deny.stop)

    def request(self,trade_id='engineering-native-fixture'):
        return submit_engineering_exit(self.directory,trade_id,clock=lambda:self.f.now)

    def test_owner_requests_normal_exit_and_waits_for_native_cancel_terminal(self):
        request=self.request()
        result=self.bridge(snapshot=self.f.snap('2','2'),allow_entries=False)
        self.assertEqual(result['status'],'HOST_ENGINEERING_EXIT_REQUESTED')
        self.assertFalse(result['execution_authorized']);self.assertEqual(result['broker_writes_by_bridge'],0)
        self.assertTrue(self.book.state()['trades']['engineering-native-fixture']['exit_requested'])
        cancel=self.f.action('cancel')
        self.assertFalse(any(a['kind']=='exit' and a['state']=='READY' for a in self.book.actions()))
        packet=self.f.native.begin(cancel['id'],'cancel',self.f.snap('2','2'))
        self.assertEqual(packet['tool'],'cancel_equity_order')
        self.assertTrue(packet['requires_native_confirmation'])
        self.f.native.record(packet['operation_id'],{'data':{'accepted':True}})
        self.f.orders[-1]['state']='cancelled'
        self.f.now+=timedelta(seconds=1);self.book.reconcile(self.f.snap('2'))
        self.assertEqual(self.f.action('exit')['kind'],'exit')
        response=read_request_result(self.directory,request['request_id'],clock=lambda:self.f.now)
        self.assertEqual(response['status'],'HOST_ENGINEERING_EXIT_REQUESTED')
        self.assertTrue(self.book.audit.verify()['integrity_valid'])

    def test_same_request_after_result_publication_loss_does_not_duplicate_exit_audit(self):
        request=self.request();snapshot=self.f.snap('2','2')
        result=self.bridge._prepare(request,snapshot,{},False)
        count=self.book.audit.verify()['events_verified']
        self.assertEqual(self.bridge._prepare(request,snapshot,{},False),result)
        self.assertEqual(self.book.audit.verify()['events_verified'],count)

    def test_wrong_trade_rejected_without_exit_or_native_write(self):
        self.request('engineering-missing')
        result=self.bridge(snapshot=self.f.snap('2','2'),allow_entries=False)
        self.assertEqual(result['status'],'HOST_REQUEST_REJECTED')
        self.assertEqual(result['reason'],'ENGINEERING_TRADE_REQUIRED')
        self.assertFalse(self.book.state()['trades']['engineering-native-fixture'].get('exit_requested',False))

    def test_expired_request_and_old_snapshot_cannot_request_exit(self):
        request=self.request();snapshot=self.f.snap('2','2')
        self.f.now+=timedelta(seconds=31)
        with self.assertRaisesRegex(ValueError,'HOST_REQUEST_EXPIRED'):
            self.bridge._prepare(request,snapshot,{},False)
        request=self.request()
        with self.assertRaises(ValueError):self.bridge._prepare(request,snapshot,{},False)
        self.assertFalse(self.book.state()['trades']['engineering-native-fixture'].get('exit_requested',False))
