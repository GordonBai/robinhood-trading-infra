"""Synthetic entry fills before the original Host receipt; zero broker I/O."""
from datetime import timedelta
import json
import os
from pathlib import Path
import stat
from unittest import TestCase
from unittest.mock import patch

from martin_agentic.host_execution import HostExecutionBridge
from martin_agentic.host_mailbox import submit_request,submit_receipt,read_private_json
import test_native_lifecycle


class HostEntryReceiptRecoveryTests(TestCase):
    def setUp(self):
        self.fixture=test_native_lifecycle.NativeLifecycleTests();self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.book=self.fixture.book
        self.entry=self.fixture.engineering_entry(
            parameters_valid_until=(self.fixture.now+timedelta(minutes=1)).isoformat())
        self.directory=Path(self.fixture.temp.name)/'host'
        self.bridge=HostExecutionBridge(self.book,self.directory)
        self.addCleanup(self.bridge.close)
        deny=patch('martin_agentic.native_review.EquityReviewTransport.__init__',
            side_effect=AssertionError('Synthetic recovery test must not connect to a broker'))
        deny.start();self.addCleanup(deny.stop)

    def action(self,identity):
        return next(action for action in self.book.actions() if action['id']==identity)

    def prepare(self,action_id,operation,*,quantity='0'):
        request=submit_request(self.directory,action_id,operation,clock=lambda:self.fixture.now)
        result=self.bridge(snapshot=self.fixture.snap(quantity),quotes={'DEMO':self.fixture.quote()},allow_entries=True)
        self.assertEqual(result['status'],'HOST_PACKET_PREPARED',result)
        prepared=read_private_json(self.directory/'packets'/(request['request_id']+'.json'))
        return request,prepared

    def review(self,action_id,*,quantity='0'):
        request,prepared=self.prepare(action_id,'review',quantity=quantity)
        packet=prepared['packet']
        submit_receipt(self.directory,request['request_id'],{'data':{**packet['parameters'],
            'order_checks':{},'customer_approval_required':False,
            'market_data_disclosure':'SYNTHETIC exact disclosure','quote_data':{}}},clock=lambda:self.fixture.now)
        result=self.bridge(allow_dispatch=False)
        self.assertEqual(result['status'],'HOST_RECEIPT_IMPORTED')
        self.assertTrue(result['native_result']['requires_native_confirmation'])

    def entry_fill_without_receipt(self,*,quantity='2',state='filled'):
        self.review(self.entry['id'])
        request,prepared=self.prepare(self.entry['id'],'place')
        payload=json.loads(self.action(self.entry['id'])['payload'])
        raw=self.fixture.raw(payload,state=state,filled=quantity)
        self.fixture.orders=[raw];self.fixture.now+=timedelta(seconds=1)
        snapshot=self.fixture.snap(quantity);self.book.reconcile(snapshot)
        self.assertEqual(self.action(self.entry['id'])['state'],'OBSERVED')
        protection=self.fixture.action('protect')
        return request,prepared,raw,snapshot,protection

    def test_exact_entry_fill_releases_handoff_and_prepares_protection_before_entry_receipt(self):
        old,prepared,raw,snapshot,protection=self.entry_fill_without_receipt()
        request=submit_request(self.directory,protection['id'],'review',clock=lambda:self.fixture.now)
        result=self.bridge(snapshot=snapshot,quotes={'DEMO':self.fixture.quote()},allow_entries=False)
        self.assertEqual(result['status'],'HOST_PACKET_PREPARED',result)
        self.assertEqual(result['request_id'],request['request_id'])
        self.assertIn(prepared['packet']['operation_id'],result['observed_receipts_pending'])
        self.assertEqual(self.bridge.pending['packet']['action_id'],protection['id'])
        active_operation=self.bridge.pending['packet']['operation_id']
        # The original actual-shaped response arrives while protection review
        # remains pending. Import only its metadata; do not clear the new owner.
        submit_receipt(self.directory,old['request_id'],{'data':{'order':raw,'approval':None}},clock=lambda:self.fixture.now)
        imported=self.bridge(allow_dispatch=False)
        self.assertIn(prepared['packet']['operation_id'],imported['retired_receipts_imported'])
        self.assertEqual(self.bridge.pending['packet']['operation_id'],active_operation)
        self.assertEqual(self.action(self.entry['id'])['state'],'OBSERVED')
        self.assertEqual(self.action(protection['id'])['state'],'RUNNING')
        self.assertFalse(imported['execution_authorized'])
        self.assertEqual(self.book.state()['trades']['engineering-native-fixture']['phase'],'NEEDS_PROTECTION')
        self.assertTrue(self.book.audit.verify()['integrity_valid'])

    def test_bad_retired_entry_receipt_does_not_poison_current_protection_place(self):
        old,prepared,raw,snapshot,protection=self.entry_fill_without_receipt()
        # Retire by exact fresh broker observation, without manufacturing a tool
        # response, then advance the independent protection review/place path.
        result=self.bridge(snapshot=snapshot,allow_dispatch=False)
        self.assertIn(prepared['packet']['operation_id'],result['observed_receipts_pending'])
        self.review(protection['id'],quantity='2')
        current,current_prepared=self.prepare(protection['id'],'place',quantity='2')
        submit_receipt(self.directory,old['request_id'],{'data':{'order':raw,'approval':None}},clock=lambda:self.fixture.now)
        path=self.directory/'receipts'/(prepared['packet']['operation_id']+'.json')
        invalid=read_private_json(path);invalid['binding']['account_key']='invalid-synthetic-binding'
        path.write_text(json.dumps(invalid))
        result=self.bridge(allow_dispatch=False)
        self.assertEqual(self.action(protection['id'])['state'],'RUNNING')
        self.assertEqual(self.bridge.pending['request_id'],current['request_id'])
        self.assertEqual(self.bridge.pending['packet']['operation_id'],current_prepared['packet']['operation_id'])
        self.assertEqual(self.action(self.entry['id'])['state'],'OBSERVED')
        self.assertIn(prepared['packet']['operation_id'],result['observed_receipts_pending'])
        self.assertTrue(result['attention_required'])
        self.assertTrue(result['observed_receipt_errors'])
        self.assertEqual(self.book.db.execute('SELECT state FROM native_operation_records WHERE id=?',
            (current_prepared['packet']['operation_id'],)).fetchone()[0],'STARTED')

    def test_missing_fresh_broker_input_does_not_retire_entry_from_cached_state(self):
        _,prepared,_,_,protection=self.entry_fill_without_receipt()
        submit_request(self.directory,protection['id'],'review',clock=lambda:self.fixture.now)
        result=self.bridge(allow_dispatch=False)
        self.assertEqual(result['status'],'HOST_TOOL_RESULT_PENDING')
        self.assertEqual(self.bridge.pending['packet']['operation_id'],prepared['packet']['operation_id'])
        self.assertFalse(self.bridge.retired)
        self.assertEqual(self.action(protection['id'])['state'],'READY')

    def test_partial_entry_fill_can_prepare_exact_quantity_protection_before_cancel_remainder(self):
        _,prepared,_,snapshot,protection=self.entry_fill_without_receipt(quantity='1',state='partially_filled')
        self.assertEqual(json.loads(protection['payload'])['parameters']['quantity'],'1')
        cancels=[action for action in self.book.actions() if action['kind']=='cancel' and action['state']=='READY']
        self.assertTrue(cancels)
        submit_request(self.directory,cancels[0]['id'],'cancel',
            request_id='00000000-0000-0000-0000-000000000001',clock=lambda:self.fixture.now)
        request=submit_request(self.directory,protection['id'],'review',
            request_id='ffffffff-ffff-ffff-ffff-ffffffffffff',clock=lambda:self.fixture.now)
        result=self.bridge(snapshot=snapshot,quotes={'DEMO':self.fixture.quote()},allow_entries=False)
        self.assertEqual(result['status'],'HOST_PACKET_PREPARED',result)
        self.assertEqual(result['request_id'],request['request_id'])
        self.assertEqual(self.bridge.pending['packet']['parameters']['quantity'],'1')
        self.assertIn(prepared['packet']['operation_id'],result['observed_receipts_pending'])

    def test_observed_marker_directory_fsync_failure_retries_with_new_snapshot_idempotently(self):
        _,prepared,_,snapshot,_=self.entry_fill_without_receipt()
        original=os.fsync;failed=[]
        def fail_first_directory(descriptor):
            if stat.S_ISDIR(os.fstat(descriptor).st_mode) and not failed:
                failed.append(True)
                raise OSError('synthetic directory fsync failure after exclusive publication')
            return original(descriptor)
        with patch('martin_agentic.host_mailbox.os.fsync',side_effect=fail_first_directory):
            with self.assertRaises(OSError):self.bridge(snapshot=snapshot,allow_dispatch=False)
        marker=self.directory/'observed'/(prepared['packet']['operation_id']+'.json')
        self.assertTrue(marker.exists());original_marker=read_private_json(marker)
        self.assertIsNotNone(self.bridge.pending)
        self.fixture.now+=timedelta(seconds=1)
        newer=self.fixture.snap('2');self.book.reconcile(newer)
        self.assertNotEqual(newer['observed_at'],snapshot['observed_at'])
        result=self.bridge(snapshot=newer,allow_dispatch=False)
        self.assertIsNone(self.bridge.pending)
        self.assertIn(prepared['packet']['operation_id'],result['observed_receipts_pending'])
        self.assertEqual(read_private_json(marker),original_marker)
        self.assertTrue(self.book.audit.verify()['integrity_valid'])

    def test_owner_mailbox_complete_protected_roundtrip_requires_each_broker_effect(self):
        from martin_agentic.host_mailbox import submit_engineering_exit
        def import_response(request,response):
            submit_receipt(self.directory,request['request_id'],response,clock=lambda:self.fixture.now)
            result=self.bridge(allow_dispatch=False)
            self.assertEqual(result['status'],'HOST_RECEIPT_IMPORTED',result)
            self.assertFalse(result['broker_effect_verified'])
            return result
        def place(action,quantity):
            self.review(action['id'],quantity=quantity)
            request,envelope=self.prepare(action['id'],'place',quantity=quantity)
            payload=json.loads(self.action(action['id'])['payload'])
            raw=self.fixture.raw(payload)
            import_response(request,{'data':{'order':raw,'approval':None}})
            return raw
        entry=place(self.entry,'0')
        self.assertEqual(self.book.state()['trades']['engineering-native-fixture']['phase'],'ENTRY_AWAITING_BROKER')
        # Use the fixture's full execution evidence rather than inferring a fill
        # from a successful place return or a changed position alone.
        entry=self.fixture.raw(json.loads(self.action(self.entry['id'])['payload']),state='filled',filled='2')
        self.fixture.orders=[entry];self.fixture.now+=timedelta(seconds=1)
        self.book.reconcile(self.fixture.snap('2'))
        protection=self.fixture.action('protect');stop=place(protection,'2')
        self.assertEqual(self.book.state()['trades']['engineering-native-fixture']['phase'],'NEEDS_PROTECTION')
        self.fixture.orders.append(stop);self.fixture.now+=timedelta(seconds=1)
        self.book.reconcile(self.fixture.snap('2','2'))
        self.assertEqual(self.book.state()['trades']['engineering-native-fixture']['phase'],'PROTECTED')
        exit_request=submit_engineering_exit(self.directory,'engineering-native-fixture',clock=lambda:self.fixture.now)
        result=self.bridge(snapshot=self.fixture.snap('2','2'),allow_entries=False)
        self.assertEqual(result['status'],'HOST_ENGINEERING_EXIT_REQUESTED')
        cancel=self.fixture.action('cancel')
        request,envelope=self.prepare(cancel['id'],'cancel',quantity='2')
        self.assertTrue(envelope['packet']['requires_native_confirmation'])
        import_response(request,{'data':{'accepted':True}})
        self.assertFalse(any(a['kind']=='exit' and a['state']=='READY' for a in self.book.actions()))
        self.fixture.orders[-1]['state']='cancelled';self.fixture.now+=timedelta(seconds=1)
        self.book.reconcile(self.fixture.snap('2'))
        exit_action=self.fixture.action('exit');sell=place(exit_action,'2')
        self.assertNotEqual(self.book.state()['trades']['engineering-native-fixture']['phase'],'FLAT_VERIFIED')
        sell=self.fixture.raw(json.loads(self.action(exit_action['id'])['payload']),state='filled',filled='2')
        self.fixture.orders.append(sell);self.fixture.now+=timedelta(seconds=1)
        self.book.reconcile(self.fixture.snap())
        self.assertEqual(self.book.state()['trades']['engineering-native-fixture']['phase'],'FLAT_VERIFIED')
        self.assertFalse(any(a['state'] in {'READY','WAITING_USER','RUNNING','UNKNOWN','REPORTED'} for a in self.book.actions()))
        self.assertFalse(self.book.state()['issues'])
        self.assertTrue(self.book.audit.verify()['integrity_valid'])
        self.assertEqual(self.book.db.execute('SELECT count(*) FROM native_operation_records').fetchone()[0],7)
