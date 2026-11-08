from datetime import timedelta
import json
from unittest import TestCase

from martin_agentic.native_operations import NativeOperations
import test_execution

class NativeOperationTests(TestCase):
    def setUp(self):
        self.f=test_execution.ExecutionTests('test_empty_queue_has_zero_agent_calls');self.f.setUp();self.addCleanup(self.f.tearDown)
        self.buy=self.f.entry_filled();self.book=self.f.book;self.ops=NativeOperations(self.book)
        self.action=next(a for a in self.book.actions() if a['kind']=='protect')['id']
    def snapshot(self):return self.f.snap([self.buy],qty='2')
    def review(self):
        packet=self.ops.begin(self.action,'review',self.snapshot(),quote=self.f.quote)
        response={'data':{**packet['parameters'],'order_checks':{},'customer_approval_required':False,
            'market_data_disclosure':'Exact disclosure text','quote_data':{}}}
        return packet,response
    def test_preview_does_not_reserve_approve_or_submit(self):
        p=self.ops.preview(self.action,self.snapshot(),quote=self.f.quote)
        self.assertFalse(p['specific_order_confirmed']);self.assertEqual(p['next_operation'],'review')
        self.assertEqual(next(a for a in self.book.actions() if a['id']==self.action)['state'],'READY')
    def test_place_without_native_review_fails_before_reservation(self):
        with self.assertRaisesRegex(ValueError,'FRESH_NATIVE_REVIEW_REQUIRED'):
            self.ops.begin(self.action,'place',self.snapshot(),quote=self.f.quote)
    def test_review_preserves_disclosure_confirmation_and_idempotence(self):
        p,r=self.review();result=self.ops.record(p['operation_id'],r)
        self.assertTrue(result['requires_native_confirmation']);self.assertEqual(result['market_data_disclosure'],'Exact disclosure text')
        self.assertEqual(result,self.ops.record(p['operation_id'],r))
        self.assertEqual(next(a for a in self.book.actions() if a['id']==self.action)['state'],'WAITING_USER')
        with self.assertRaisesRegex(ValueError,'ID_CONFLICT'):
            self.ops.record(p['operation_id'],{'data':{}})
    def test_invalid_review_does_not_create_unknown_write_or_enable_place(self):
        p,r=self.review();del r['data']['quantity']
        result=self.ops.record(p['operation_id'],r)
        self.assertFalse(result['response_usable'])
        self.assertEqual(next(a for a in self.book.actions() if a['id']==self.action)['state'],'WAITING_USER')
        with self.assertRaisesRegex(ValueError,'FRESH_NATIVE_REVIEW_REQUIRED'):
            self.ops.begin(self.action,'place',self.snapshot(),quote=self.f.quote)
    def test_unknown_place_is_locked_not_assumed_rejected(self):
        p,r=self.review();self.ops.record(p['operation_id'],r)
        place=self.ops.begin(self.action,'place',self.snapshot(),quote=self.f.quote)
        self.assertFalse(place['execution_authorized_by_packet']);self.assertTrue(place['requires_native_confirmation'])
        result=self.ops.record(place['operation_id'],{'isError':True,'content':[{'type':'text','text':'timeout'}]})
        self.assertEqual(result['report']['status'],'unknown');self.assertIsNone(self.book.claim())
    def test_generic_id_does_not_become_approval_alias(self):
        p,r=self.review();self.ops.record(p['operation_id'],r)
        place=self.ops.begin(self.action,'place',self.snapshot(),quote=self.f.quote)
        result=self.ops.record(place['operation_id'],{'data':{'id':'not-an-explicit-approval'}})
        self.assertFalse(result['response_usable']);self.assertNotIn('approval_id',result)
    def test_expired_review_cannot_dispatch_place(self):
        p,r=self.review();self.ops.record(p['operation_id'],r)
        self.f.now+=timedelta(seconds=31)
        self.f.quote.update(bid_at=self.f.now.isoformat(),ask_at=self.f.now.isoformat(),observed_at=self.f.now.isoformat())
        with self.assertRaisesRegex(ValueError,'FRESH_NATIVE_REVIEW_REQUIRED|EXTERNAL_PROTECTION_NO_LONGER_CURRENT|EXTERNAL_ACTION_NOT_AVAILABLE'):
            self.ops.begin(self.action,'place',self.snapshot(),quote=self.f.quote)
    def test_stored_response_never_contains_full_account_or_credentials(self):
        p,r=self.review();r['data'].update(account_number='PRIVATE_ACCOUNT',access_token='PRIVATE_TOKEN')
        self.ops.record(p['operation_id'],r)
        raw=self.book.db.execute('SELECT payload,result FROM native_operation_records').fetchall()
        self.assertNotIn('PRIVATE_ACCOUNT',json.dumps(raw));self.assertNotIn('PRIVATE_TOKEN',json.dumps(raw))

    def test_app_approval_reason_and_alert_confirmation_are_preserved(self):
        p,r=self.review()
        r['data'].update(customer_approval_required=True,customer_approval_required_reason='mandatory_model',order_checks={'alert_type':'EXACT_NATIVE_ALERT'})
        result=self.ops.record(p['operation_id'],r)
        self.assertTrue(result['requires_native_confirmation'])
        packet=self.ops.begin(self.action,'place',self.snapshot(),quote=self.f.quote)
        self.assertEqual(packet['customer_approval_required_reason'],'mandatory_model')
        self.assertEqual(packet['order_checks'],{'alert_type':'EXACT_NATIVE_ALERT'})
        self.assertFalse(packet['execution_authorized_by_packet'])

    def place_packet(self):
        p,r=self.review();self.ops.record(p['operation_id'],r)
        return self.ops.begin(self.action,'place',self.snapshot(),quote=self.f.quote)

    def official_approval(self,state='APPROVAL_STATE_PENDING',**extra):
        approval={'approval_id':'official-approval','account_number':'PRIVATE_ACCOUNT',
            'asset_class':'ASSET_CLASS_EQUITY','state':state,'is_edited':False}
        approval.update(extra)
        return {'structuredContent':{'data':{'order':None,'approval':approval},
            'guide':'The order was NOT placed. Approve or decline in the Robinhood app.'}}

    def official_order(self,packet,*,state='confirmed'):
        from test_broker import order
        value=order('official-order','sell','2','0',state,'39')
        value.update(time_in_force=packet['parameters']['time_in_force'],ref_id=packet['logical_ref_id'],instrument_id='instrument',price=None,average_price=None,fees='0',
            dollar_based_amount=None,placed_agent='agentic',agent_initiated=True,last_transaction_at=None)
        return {'structuredContent':{'data':{'approval':None,'order':value},
            'guide':'Submitted does not mean filled.'}}

    def test_official_nullable_order_nested_pending_approval_is_not_a_fill(self):
        packet=self.place_packet();response=self.official_approval()
        result=self.ops.record(packet['operation_id'],response)
        self.assertTrue(result['response_usable'])
        self.assertEqual(result['report']['status'],'pending_approval')
        self.assertFalse(result['broker_effect_verified'])
        self.assertEqual(result['native_guide'],response['structuredContent']['guide'])
        self.assertIn('official-approval',self.book._order_refs(next(a for a in self.book.actions() if a['id']==self.action)))
        stored=json.dumps(self.book.db.execute('SELECT result FROM native_operation_records').fetchall())
        self.assertNotIn('PRIVATE_ACCOUNT',stored)

    def test_official_approved_states_keep_alias_but_do_not_prove_order_or_protection(self):
        packet=self.place_packet()
        result=self.ops.record(packet['operation_id'],self.official_approval('APPROVAL_STATE_AUTO_APPROVED'))
        self.assertEqual(result['report']['status'],'submitted')
        self.assertFalse(result['broker_effect_verified'])
        self.assertEqual(result['approval_id'],'official-approval')
        self.assertIn('official-approval',self.book._order_refs(next(a for a in self.book.actions() if a['id']==self.action)))
        self.assertNotEqual(next(iter(self.book.state()['trades'].values()))['phase'],'PROTECTED')

    def test_official_declined_approval_keeps_tracking_alias_without_proving_terminal_effect(self):
        packet=self.place_packet()
        result=self.ops.record(packet['operation_id'],self.official_approval('APPROVAL_STATE_DECLINED'))
        self.assertEqual(result['report']['status'],'rejected')
        self.assertFalse(result['broker_effect_verified'])
        self.assertIn('official-approval',self.book._order_refs(next(a for a in self.book.actions() if a['id']==self.action)))

    def test_edited_approval_is_unknown_and_cannot_retry(self):
        packet=self.place_packet()
        result=self.ops.record(packet['operation_id'],self.official_approval(is_edited=True))
        self.assertEqual(result['report']['status'],'unknown')
        self.assertIsNone(self.book.claim())
        self.assertIn('official-approval',self.book._order_refs(next(a for a in self.book.actions() if a['id']==self.action)))

    def test_unknown_approval_state_is_not_accepted_as_pending(self):
        packet=self.place_packet()
        result=self.ops.record(packet['operation_id'],self.official_approval('APPROVAL_STATE_SOMETHING_NEW'))
        self.assertEqual(result['report']['status'],'unknown')
        self.assertFalse(result['response_usable'])
        self.assertIn('official-approval',self.book._order_refs(next(a for a in self.book.actions() if a['id']==self.action)))

    def test_conflicting_top_level_and_official_nested_approval_identity_is_unknown(self):
        packet=self.place_packet();response=self.official_approval()
        response['structuredContent']['data']['approval_id']='different-approval'
        result=self.ops.record(packet['operation_id'],response)
        self.assertEqual(result['report']['status'],'unknown')
        self.assertFalse(result['response_usable'])
        self.assertNotIn('official-approval',self.book._order_refs(next(a for a in self.book.actions() if a['id']==self.action)))

    def test_real_book_rejects_legacy_compact_receipts(self):
        packet=self.place_packet()
        self.book.simulation=False
        try:result=self.ops.record(packet['operation_id'],{'data':{'approval_id':'compact'}})
        finally:self.book.simulation=True
        self.assertEqual(result['report']['status'],'unknown')
        self.assertFalse(result['response_usable'])

    def test_official_order_response_exact_stop_terms_are_checked_and_only_advisory(self):
        packet=self.place_packet();response=self.official_order(packet)
        result=self.ops.record(packet['operation_id'],response)
        self.assertTrue(result['response_usable']);self.assertEqual(result['report']['status'],'submitted')
        self.assertEqual(result['order_id'],'official-order');self.assertFalse(result['broker_effect_verified'])

    def test_official_order_changed_stop_price_is_unknown(self):
        packet=self.place_packet();response=self.official_order(packet)
        response['structuredContent']['data']['order']['stop_price']='38'
        result=self.ops.record(packet['operation_id'],response)
        self.assertEqual(result['report']['status'],'unknown');self.assertFalse(result['response_usable'])

    def test_official_rejected_order_is_not_submitted_or_protection(self):
        packet=self.place_packet();response=self.official_order(packet,state='rejected')
        result=self.ops.record(packet['operation_id'],response)
        self.assertEqual(result['report']['status'],'rejected');self.assertFalse(result['broker_effect_verified'])

    def test_official_response_with_both_order_and_approval_is_unknown(self):
        packet=self.place_packet();response=self.official_approval()
        response['structuredContent']['data']['order']=self.official_order(packet)['structuredContent']['data']['order']
        result=self.ops.record(packet['operation_id'],response)
        self.assertEqual(result['report']['status'],'unknown');self.assertFalse(result['response_usable'])
