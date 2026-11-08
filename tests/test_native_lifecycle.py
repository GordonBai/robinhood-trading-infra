"""Synthetic supervised native handoffs; no test invokes a brokerage tool."""
from copy import deepcopy
from datetime import datetime,timedelta
import json
from pathlib import Path
import tempfile
from unittest import TestCase

from martin_luk.exits import exit_decision
from entry_authority_fixture import TestExecutionBook as ExecutionBook
from martin_agentic.qualification import release_check,source_fingerprint
from test_broker import FakeBroker,snapshot,position,order


class NativeLifecycleTests(TestCase):
    def setUp(self):
        from martin_agentic.native_operations import NativeOperations
        self.temp=tempfile.TemporaryDirectory()
        self.now=datetime.fromisoformat('2026-10-16T14:00:00+00:00')
        self.policy=json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        self.book=ExecutionBook(Path(self.temp.name)/'native.sqlite',self.policy,simulation=True,clock=lambda:self.now,
            exit_decider=exit_decision)
        self.native=NativeOperations(self.book)
        self.plan={'symbol':'DEMO','quantity':2,'entry_ceiling':'40','stop':'39','cost_buffer_per_share':'0.05'}
        self.orders=[]

    def tearDown(self):self.book.close();self.temp.cleanup()

    def quote(self,bid='39.99',ask='40'):
        return {'symbol':'DEMO','bid':bid,'ask':ask,**{k:self.now.isoformat() for k in ('bid_at','ask_at','observed_at')}}

    def snap(self,qty='0',held='0'):
        broker=FakeBroker();broker.orders=deepcopy(self.orders)
        broker.positions=[] if qty=='0' else [position(qty,held)]
        # All synthetic executions below use $40, preserving exact cash bookkeeping.
        broker.cash=str(500+sum(int(o['cumulative_quantity'])*40*(1 if o['side']=='sell' else -1) for o in self.orders))
        result=snapshot(broker,self.now.isoformat());result['synthetic']=True
        return result

    def action(self,kind):
        return next(a for a in reversed(self.book.actions()) if a['kind']==kind and a['state']=='READY')

    def raw(self,payload,*,state='confirmed',filled='0',ref_id=None):
        p=payload['parameters']
        value=order(payload['action_id'],p['side'],p['quantity'],filled,state,p.get('stop_price'))
        value['ref_id']=ref_id or payload['ref_id'];value['price']=p.get('limit_price')
        value['type']='limit' if p['type']=='limit' else 'market'
        value['time_in_force']=p.get('time_in_force','gfd')
        return value

    def filled_entry(self):
        self.book.propose('native-fixture',self.plan,self.quote(),self.snap(),
            evidence={'synthetic':True,'source_sha256':'native-lifecycle-fixture'})
        entry=self.book.claim();self.book.report(entry['action_id'],{'status':'submitted'})
        self.orders=[self.raw(entry,state='filled',filled='2')]
        self.now+=timedelta(seconds=1);self.book.reconcile(self.snap('2'))
        return entry

    def response(self,data):return {'structuredContent':{'data':data}}

    def review(self,action_id,snap,quote=None,**changes):
        payload=json.loads(next(a['payload'] for a in self.book.actions() if a['id']==action_id))
        parameters=payload['parameters']
        quote=quote or (self.quote('43','43.01') if float(parameters.get('stop_price','0'))>=40 else self.quote())
        packet=self.native.begin(action_id,'review',snap,quote=quote)
        data={k:v for k,v in parameters.items() if k not in {'market_hours','time_in_force'}}
        data.update(customer_approval_required=True,order_checks={},market_data_disclosure='SYNTHETIC NBBO fixture',
            quote_data={'symbol':'DEMO','bid_price':quote['bid'],'ask_price':quote['ask'],
                'venue_bid_time':self.now.isoformat(),'venue_ask_time':self.now.isoformat()})
        data.update(changes)
        return self.native.record(packet['operation_id'],self.response(data))

    def place(self,action_id,snap,*,approval_id=None):
        self.review(action_id,snap)
        saved=json.loads(next(a['payload'] for a in self.book.actions() if a['id']==action_id))
        quote=self.quote('43','43.01') if float(saved['parameters'].get('stop_price','0'))>=40 else self.quote()
        packet=self.native.begin(action_id,'place',snap,quote=quote)
        response={'order':None,'approval':{'approval_id':approval_id,'state':'APPROVAL_STATE_PENDING',
            'asset_class':'ASSET_CLASS_EQUITY','is_edited':False}} if approval_id else {
                'order':self.raw(saved),'approval':None}
        self.native.record(packet['operation_id'],self.response(response))
        return json.loads(next(a['payload'] for a in self.book.actions() if a['id']==action_id))

    def test_supervised_protect_deferred_tightening_exit_round_trip_stays_synthetic(self):
        self.filled_entry();protection=self.action('protect')
        approval='11111111-1111-4111-8111-111111111111'
        payload=self.place(protection['id'],self.snap('2'),approval_id=approval)
        self.assertNotEqual(self.book.state()['trades']['native-fixture']['phase'],'PROTECTED')
        self.assertEqual(json.loads(next(a['payload'] for a in self.book.actions() if a['id']==protection['id']))['ref_id'],payload['ref_id'])
        # Official order ref switches only via the recorded exact approval id.
        self.orders.append(self.raw(payload,ref_id=approval))
        self.now+=timedelta(seconds=1);self.book.reconcile(self.snap('2','2'))
        self.assertEqual(self.book.state()['trades']['native-fixture']['phase'],'PROTECTED')
        timing=deepcopy(self.book.state()['trades']['native-fixture'])
        self.assertEqual(timing['protection_observation_delay_seconds'],'2.0')
        self.assertEqual(timing['protection_timing_basis'],'first_fill_to_first_full_coverage_observation')
        self.assertTrue(timing['protection_timing_synthetic'])
        self.assertEqual(timing['first_protection_observed_quantity'],'2')
        # At +3R, unknown replacement capability must preserve the valid stop.
        self.book.reconcile(self.snap('2','2'),quotes={'DEMO':self.quote('43','43.01')})
        trade=self.book.state()['trades']['native-fixture']
        self.assertEqual(trade['phase'],'PROTECTED');self.assertEqual(trade['stop'],'39')
        self.assertEqual(trade['suggested_stop'],'40')
        self.assertFalse(any(a['kind']=='cancel' and a['state']=='READY' for a in self.book.actions()))
        self.now=datetime.fromisoformat('2026-10-16T19:55:00+00:00')
        self.book.reconcile(self.snap('2','2'))
        cancel=self.action('cancel');packet=self.native.begin(cancel['id'],'cancel',self.snap('2','2'))
        self.native.record(packet['operation_id'],self.response({'accepted':True}))
        self.assertFalse(any(a['kind']=='exit' and a['state']=='READY' for a in self.book.actions()))
        self.orders[-1]['state']='cancelled';self.book.reconcile(self.snap('2'))
        exit_action=self.action('exit');payload=self.place(exit_action['id'],self.snap('2'))
        self.orders.append(self.raw(payload,state='filled',filled='2'));self.book.reconcile(self.snap())
        self.assertEqual(self.book.state()['trades']['native-fixture']['phase'],'FLAT_VERIFIED')
        final_trade=self.book.state()['trades']['native-fixture']
        self.assertEqual(final_trade['first_protection_observed_at'],timing['first_protection_observed_at'])
        self.assertEqual(final_trade['protection_observation_delay_seconds'],'2.0')
        self.assertTrue(self.book.audit.verify()['integrity_valid'])
        events=[json.loads(r[0]) for r in self.book.db.execute('SELECT payload FROM audit_events')]
        self.assertTrue(events);self.assertTrue(all(e['synthetic'] for e in events))
        status={'running':True,'healthy':True,'observed_at':self.now.isoformat(),
            'source_fingerprint':source_fingerprint(),'broker':{'account':'••••0000'}}
        release=release_check(self.book,status,{'passed':True,'source_fingerprint':source_fingerprint()})
        self.assertFalse(release['production_ready']);self.assertFalse(release['supervised_live_smoke_passed'])

    def test_deferred_tightening_does_not_mask_actual_protection_loss(self):
        self.filled_entry();a=self.action('protect');payload=self.place(a['id'],self.snap('2'))
        self.orders.append(self.raw(payload));self.book.reconcile(self.snap('2','2'))
        self.book.reconcile(self.snap('2','2'),quotes={'DEMO':self.quote('43','43.01')})
        self.orders[-1]['state']='cancelled';self.now+=timedelta(seconds=1)
        self.book.reconcile(self.snap('2'))
        trade=self.book.state()['trades']['native-fixture']
        self.assertEqual(trade['stop'],'39');self.assertEqual(trade['suggested_stop'],'40')
        self.assertNotEqual(trade['phase'],'PROTECTED')
        self.assertTrue(trade['exit_requested'])
        self.assertIn('PROTECTION_TERMINATED_UNCOVERED:DEMO',self.book.state()['issues'])
        self.assertEqual(json.loads(self.action('exit')['payload'])['parameters']['quantity'],'2')

    def test_unprotected_fill_does_not_wait_for_unverified_tighter_stop(self):
        self.filled_entry()
        self.book.reconcile(self.snap('2'),quotes={'DEMO':self.quote('43','43.01')})
        trade=self.book.state()['trades']['native-fixture']
        self.assertEqual(trade['phase'],'NEEDS_PROTECTION')
        self.assertEqual(trade['stop'],'39');self.assertEqual(trade['suggested_stop'],'40')
        self.assertEqual(json.loads(self.action('protect')['payload'])['parameters']['stop_price'],'39')
        self.assertFalse(any(a['kind']=='cancel' and a['state']=='READY' for a in self.book.actions()))

    def test_unknown_native_result_cannot_be_resubmitted(self):
        self.filled_entry();action=self.action('protect');self.review(action['id'],self.snap('2'))
        packet=self.native.begin(action['id'],'place',self.snap('2'),quote=self.quote())
        self.native.record(packet['operation_id'],{'isError':True,'content':[{'type':'text','text':'ambiguous transport result'}]})
        self.assertEqual(next(a['state'] for a in self.book.actions() if a['id']==action['id']),'UNKNOWN')
        with self.assertRaises(ValueError):self.native.begin(action['id'],'place',self.snap('2'),quote=self.quote())
        self.assertIsNone(self.book.claim())

    def test_unbound_approval_ref_is_not_guessed_from_same_parameters(self):
        self.filled_entry();action=self.action('protect');payload=json.loads(action['payload'])
        self.orders.append(self.raw(payload,ref_id='22222222-2222-4222-8222-222222222222'))
        self.book.reconcile(self.snap('2','2'))
        self.assertNotEqual(self.book.state()['trades']['native-fixture']['phase'],'PROTECTED')
        self.assertIn('UNMANAGED_POSITION_OR_ORDER',self.book.state()['issues'])

    def test_partial_fill_protects_only_observed_quantity_then_cancel_remainder(self):
        self.book.propose('native-fixture',self.plan,self.quote(),self.snap(),
            evidence={'synthetic':True,'source_sha256':'partial-native-fixture'})
        entry=self.book.claim();self.book.report(entry['action_id'],{'status':'submitted'})
        self.orders=[self.raw(entry,state='partially_filled',filled='1')]
        self.now+=timedelta(seconds=1);self.book.reconcile(self.snap('1'))
        protection=self.action('protect');payload=self.place(protection['id'],self.snap('1'))
        self.assertEqual(payload['parameters']['quantity'],'1')
        self.orders.append(self.raw(payload));self.book.reconcile(self.snap('1','1'))
        self.assertEqual(self.book.state()['trades']['native-fixture']['phase'],'PROTECTED')
        cancel=self.action('cancel');packet=self.native.begin(cancel['id'],'cancel',self.snap('1','1'))
        self.assertTrue(packet['requires_native_confirmation'])
        self.native.record(packet['operation_id'],self.response({'accepted':True}))
        self.orders[0]['state']='partially_filled_rest_cancelled';self.book.reconcile(self.snap('1','1'))
        self.assertEqual(self.book.state()['trades']['native-fixture']['quantity'],'1')
        self.assertEqual(self.book.state()['trades']['native-fixture']['phase'],'PROTECTED')

    def test_stop_fill_racing_native_cancel_never_queues_extra_sell(self):
        self.filled_entry();action=self.action('protect');payload=self.place(action['id'],self.snap('2'))
        self.orders.append(self.raw(payload));self.book.reconcile(self.snap('2','2'))
        self.book.reconcile(self.snap('2','2'),quotes={'DEMO':self.quote('38.99','39')})
        cancel=self.action('cancel');packet=self.native.begin(cancel['id'],'cancel',self.snap('2','2'))
        self.native.record(packet['operation_id'],self.response({'accepted':True}))
        self.orders[-1]=self.raw(payload,state='filled',filled='2');self.book.reconcile(self.snap())
        self.assertEqual(self.book.state()['trades']['native-fixture']['phase'],'FLAT_VERIFIED')
        self.assertFalse(any(a['kind']=='exit' and a['state']=='READY' for a in self.book.actions()))
        self.assertEqual(next(a['state'] for a in self.book.actions() if a['id']==cancel['id']),'OBSERVED')

    def test_native_receipt_replay_is_idempotent_and_conflicting_replay_rejected(self):
        self.filled_entry();action=self.action('protect')
        packet=self.native.begin(action['id'],'review',self.snap('2'),quote=self.quote())
        with self.assertRaises(ValueError):self.native.begin(action['id'],'review',self.snap('2'),quote=self.quote())
        data={'symbol':'DEMO','side':'sell','type':'stop_market','quantity':'2','stop_price':'39',
            'customer_approval_required':True,'order_checks':{},'market_data_disclosure':'SYNTHETIC'}
        result=self.native.record(packet['operation_id'],self.response(data))
        count=self.book.audit.verify()['events_verified']
        self.assertEqual(self.native.record(packet['operation_id'],self.response(data)),result)
        self.assertEqual(self.book.audit.verify()['events_verified'],count)
        with self.assertRaises(ValueError):self.native.record(packet['operation_id'],self.response({**data,'quantity':'3'}))

    def test_stale_native_review_does_not_enable_place(self):
        self.filled_entry();action=self.action('protect');self.review(action['id'],self.snap('2'))
        self.now+=timedelta(seconds=31)
        with self.assertRaisesRegex(ValueError,'^(FRESH_NATIVE_REVIEW_REQUIRED|EXTERNAL_PROTECTION_NO_LONGER_CURRENT|EXTERNAL_ACTION_NOT_AVAILABLE)$'):
            self.native.begin(action['id'],'place',self.snap('2'),quote=self.quote())

    def test_explicit_approval_alias_still_requires_exact_broker_terms(self):
        self.filled_entry();action=self.action('protect');approval='33333333-3333-4333-8333-333333333333'
        payload=self.place(action['id'],self.snap('2'),approval_id=approval)
        raw=self.raw(payload,ref_id=approval);raw['stop_price']='38'
        self.orders.append(raw)
        with self.assertRaisesRegex(ValueError,'BROKER_ORDER_TERMS_MISMATCH'):self.book.reconcile(self.snap('2','2'))
        self.assertNotEqual(self.book.state()['trades']['native-fixture']['phase'],'PROTECTED')

    def test_unknown_native_submit_resolves_only_via_matching_official_observation(self):
        self.filled_entry();action=self.action('protect');self.review(action['id'],self.snap('2'))
        packet=self.native.begin(action['id'],'place',self.snap('2'),quote=self.quote())
        self.native.record(packet['operation_id'],{'isError':True})
        payload=json.loads(next(a['payload'] for a in self.book.actions() if a['id']==action['id']))
        self.orders.append(self.raw(payload));self.now+=timedelta(seconds=1)
        self.book.reconcile(self.snap('2','2'))
        self.assertEqual(self.book.state()['trades']['native-fixture']['phase'],'PROTECTED')
        self.assertEqual(next(a['state'] for a in self.book.actions() if a['id']==action['id']),'OBSERVED')
        self.assertEqual(self.book.audit.report('2026-10-16')['unresolved_agent_intents'],[])
        with self.assertRaises(ValueError):self.native.begin(action['id'],'place',self.snap('2','2'),quote=self.quote())

    def test_cancel_unknown_resolves_on_target_terminal_without_duplicate_cancel(self):
        self.filled_entry();action=self.action('protect');payload=self.place(action['id'],self.snap('2'))
        self.orders.append(self.raw(payload));self.book.reconcile(self.snap('2','2'))
        self.book.reconcile(self.snap('2','2'),quotes={'DEMO':self.quote('38.99','39')})
        cancel=self.action('cancel');packet=self.native.begin(cancel['id'],'cancel',self.snap('2','2'))
        self.native.record(packet['operation_id'],{'isError':True})
        self.assertIsNone(self.book.claim())
        self.orders[-1]['state']='cancelled';self.book.reconcile(self.snap('2'))
        self.assertEqual(next(a['state'] for a in self.book.actions() if a['id']==cancel['id']),'OBSERVED')
        self.assertEqual(self.book.audit.report('2026-10-16')['unresolved_agent_intents'],[])
        self.assertEqual(self.action('exit')['kind'],'exit')


    def test_zero_observation_delay_is_explicitly_synthetic_not_broker_acceptance(self):
        self.book.propose('native-fixture',self.plan,self.quote(),self.snap(),
            evidence={'synthetic':True,'source_sha256':'zero-delay-fixture'})
        entry=self.book.claim();self.book.report(entry['action_id'],{'status':'submitted'})
        self.orders=[self.raw(entry,state='filled',filled='2')]
        self.book.reconcile(self.snap('2'))
        action=self.action('protect');payload=self.place(action['id'],self.snap('2'))
        self.orders.append(self.raw(payload));self.book.reconcile(self.snap('2','2'))
        trade=self.book.state()['trades']['native-fixture']
        self.assertEqual(trade['protection_observation_delay_seconds'],'0.0')
        self.assertEqual(trade['protection_timing_basis'],'first_fill_to_first_full_coverage_observation')
        self.assertTrue(trade['protection_timing_synthetic'])
        status={'running':True,'healthy':True,'observed_at':self.now.isoformat(),
            'source_fingerprint':source_fingerprint(),'broker':{'account':'••••0000'}}
        result=release_check(self.book,status,{'passed':True,'source_fingerprint':source_fingerprint()})
        self.assertFalse(result['actual_protection_acceptance_validated'])
        self.assertFalse(result['supervised_live_smoke_passed'])

    def test_definitive_cancel_rejection_allows_bounded_new_confirmed_attempt(self):
        self.filled_entry();a=self.action('protect');payload=self.place(a['id'],self.snap('2'))
        self.orders.append(self.raw(payload));self.book.reconcile(self.snap('2','2'))
        self.book.reconcile(self.snap('2','2'),quotes={'DEMO':self.quote('38.99','39')})
        ids=[]
        for attempt in range(1,4):
            cancel=self.action('cancel');ids.append(cancel['id'])
            packet=self.native.begin(cancel['id'],'cancel',self.snap('2','2'))
            self.assertTrue(packet['requires_native_confirmation'])
            self.assertEqual(json.loads(cancel['payload'])['cancel_attempt'],attempt)
            self.native.record(packet['operation_id'],self.response({'accepted':False}))
            self.book.reconcile(self.snap('2','2'))
        self.assertEqual(len(set(ids)),3)
        self.assertFalse(any(a['kind']=='cancel' and a['state']=='READY' for a in self.book.actions()))
        self.assertTrue(self.book.state()['trades']['native-fixture']['manual_intervention_required'].startswith('CANCEL_RETRY_LIMIT_EXCEEDED:'))
        self.assertEqual([a['state'] for a in self.book.actions() if a['id'] in ids].count('REJECTED'),2)

    def test_readonly_partial_fill_review_is_replaced_when_more_shares_fill(self):
        self.book.propose('native-fixture',self.plan,self.quote(),self.snap(),
            evidence={'synthetic':True,'source_sha256':'partial-review-fixture'})
        entry=self.book.claim();self.book.report(entry['action_id'],{'status':'submitted'})
        self.orders=[self.raw(entry,state='partially_filled',filled='1')]
        self.now+=timedelta(seconds=1);self.book.reconcile(self.snap('1'))
        first=self.action('protect');self.review(first['id'],self.snap('1'))
        # Broker execution identities are immutable: the second share is a
        # separate execution, rather than editing the first fill's quantity.
        self.orders[0]['state']='filled';self.orders[0]['cumulative_quantity']='2'
        second=deepcopy(self.orders[0]['executions'][0]);second['id']+='-second'
        self.orders[0]['executions'].append(second)
        self.now+=timedelta(seconds=1);self.book.reconcile(self.snap('2'))
        self.assertEqual(next(a['state'] for a in self.book.actions() if a['id']==first['id']),'SUPERSEDED')
        replacement=self.action('protect')
        self.assertEqual(json.loads(replacement['payload'])['parameters']['quantity'],'2')
        self.assertNotEqual(first['id'],replacement['id'])
        self.assertEqual(self.native.preview(replacement['id'],self.snap('2'),quote=self.quote())['parameters']['quantity'],'2')

    def engineering_entry(self,**evidence):
        self.book.propose('engineering-native-fixture',self.plan,self.quote(),self.snap(),purpose='engineering',
            evidence={'synthetic':True,'source_sha256':'engineering-native-fixture',
                'engineering_scope_authorized':True,'parameters_user_specified':True,**evidence})
        return self.action('entry')

    def test_engineering_entry_native_alias_connects_actual_fill_to_protection(self):
        action=self.engineering_entry(parameters_valid_until=(self.now+timedelta(minutes=1)).isoformat())
        approval='44444444-4444-4444-8444-444444444444'
        payload=self.place(action['id'],self.snap(),approval_id=approval)
        self.assertEqual(next(a['state'] for a in self.book.actions() if a['id']==action['id']),'REPORTED')
        self.orders=[self.raw(payload,state='filled',filled='2',ref_id=approval)]
        self.now+=timedelta(seconds=1);self.book.reconcile(self.snap('2'))
        protect=self.action('protect')
        self.assertEqual(json.loads(protect['payload'])['parameters']['quantity'],'2')
        self.assertEqual(self.book.state()['trades']['engineering-native-fixture']['phase'],'NEEDS_PROTECTION')
        self.assertNotIn('UNMANAGED_POSITION_OR_ORDER',self.book.state()['issues'])

    def test_strategy_entry_still_cannot_use_native_handoff(self):
        self.book.propose('strategy-fixture',self.plan,self.quote(),self.snap(),
            evidence={'synthetic':True,'source_sha256':'strategy-fixture'})
        # Purpose alone opens nothing: qualification is a durable admission
        # fact, and this trade does not carry it.
        with self.assertRaisesRegex(ValueError,'STRATEGY_ENTRY_NOT_QUALIFIED'):
            self.native.preview(self.action('entry')['id'],self.snap(),quote=self.quote())

    def test_engineering_entry_parameters_deadline_is_enforced(self):
        action=self.engineering_entry(parameters_valid_until=(self.now+timedelta(seconds=1)).isoformat())
        self.now+=timedelta(seconds=2)
        with self.assertRaisesRegex(ValueError,'ENGINEERING_PARAMETERS_EXPIRED|EXTERNAL_ACTION_NOT_AVAILABLE'):
            self.native.begin(action['id'],'review',self.snap(),quote=self.quote())
        self.assertEqual(next(a['state'] for a in self.book.actions() if a['id']==action['id']),'EXPIRED')

    def test_engineering_entry_revalidates_quote_cash_and_foreign_orders(self):
        action=self.engineering_entry()
        for gate in ('quote','order','approval','cash'):
            with self.subTest(gate=gate):
                snap=self.snap();quote=self.quote()
                if gate=='quote':quote['ask_at']=(self.now-timedelta(seconds=16)).isoformat()
                elif gate=='cash':snap['portfolio']['cash']='1'
                elif gate=='order':snap['orders']['foreign']={'id':'foreign','symbol':'OTHER','side':'buy','state':'confirmed','ref_id':'foreign','executions':[]}
                else:snap['pending_approvals']=[{'id':'foreign-approval'}]
                with self.assertRaises(ValueError):self.native.preview(action['id'],snap,quote=quote)
                self.assertEqual(next(a['state'] for a in self.book.actions() if a['id']==action['id']),'READY')

    def test_pending_protection_write_is_not_superseded_when_more_shares_fill(self):
        self.book.propose('native-fixture',self.plan,self.quote(),self.snap(),
            evidence={'synthetic':True,'source_sha256':'partial-pending-fixture'})
        entry=self.book.claim();self.book.report(entry['action_id'],{'status':'submitted'})
        self.orders=[self.raw(entry,state='partially_filled',filled='1')]
        self.now+=timedelta(seconds=1);self.book.reconcile(self.snap('1'))
        first=self.action('protect');self.place(first['id'],self.snap('1'),approval_id='pending-one-share')
        self.orders[0]['state']='filled';self.orders[0]['cumulative_quantity']='2'
        second=deepcopy(self.orders[0]['executions'][0]);second['id']+='-second'
        self.orders[0]['executions'].append(second)
        self.now+=timedelta(seconds=1);self.book.reconcile(self.snap('2'))
        self.assertEqual(next(a['state'] for a in self.book.actions() if a['id']==first['id']),'REPORTED')
        self.assertFalse(any(a['kind']=='protect' and a['state']=='READY' for a in self.book.actions()))

    def test_engineering_entry_review_does_not_authorize_after_parameter_deadline(self):
        action=self.engineering_entry(parameters_valid_until=(self.now+timedelta(seconds=1)).isoformat())
        self.review(action['id'],self.snap())
        self.now+=timedelta(seconds=2)
        with self.assertRaisesRegex(ValueError,'ENGINEERING_PARAMETERS_EXPIRED|EXTERNAL_ACTION_NOT_AVAILABLE'):
            self.native.begin(action['id'],'place',self.snap(),quote=self.quote())
        self.assertEqual(next(a['state'] for a in self.book.actions() if a['id']==action['id']),'EXPIRED')

    def test_cancel_rejection_retry_can_complete_terminal_then_exit(self):
        self.filled_entry();a=self.action('protect');payload=self.place(a['id'],self.snap('2'))
        self.orders.append(self.raw(payload));self.book.reconcile(self.snap('2','2'))
        self.book.reconcile(self.snap('2','2'),quotes={'DEMO':self.quote('38.99','39')})
        first=self.action('cancel');packet=self.native.begin(first['id'],'cancel',self.snap('2','2'))
        self.native.record(packet['operation_id'],self.response({'accepted':False}))
        self.book.reconcile(self.snap('2','2'))
        retry=self.action('cancel');packet=self.native.begin(retry['id'],'cancel',self.snap('2','2'))
        self.native.record(packet['operation_id'],self.response({'accepted':True}))
        self.assertNotEqual(first['id'],retry['id'])
        self.orders[-1]['state']='cancelled';self.book.reconcile(self.snap('2'))
        self.assertEqual(next(a['state'] for a in self.book.actions() if a['id']==retry['id']),'OBSERVED')
        self.assertEqual(json.loads(self.action('exit')['payload'])['parameters']['quantity'],'2')

    def engineering_filled(self):
        entry=self.engineering_entry();payload=self.place(entry['id'],self.snap(),approval_id='engineering-entry-approval')
        self.orders=[self.raw(payload,state='filled',filled='2',ref_id='engineering-entry-approval')]
        self.now+=timedelta(seconds=1);self.book.reconcile(self.snap('2'))

    def test_engineering_exit_request_uses_normal_cancel_terminal_and_flat_path(self):
        self.engineering_filled();a=self.action('protect');payload=self.place(a['id'],self.snap('2'))
        self.orders.append(self.raw(payload));self.book.reconcile(self.snap('2','2'))
        result=self.book.request_engineering_exit('engineering-native-fixture',self.snap('2','2'))
        self.assertFalse(result['broker_effect_verified']);self.assertFalse(result['duplicate'])
        self.now+=timedelta(seconds=1)
        self.assertTrue(self.book.request_engineering_exit('engineering-native-fixture',self.snap('2','2'))['duplicate'])
        self.assertFalse(any(a['kind']=='exit' and a['state']=='READY' for a in self.book.actions()))
        cancel=self.action('cancel');packet=self.native.begin(cancel['id'],'cancel',self.snap('2','2'))
        self.native.record(packet['operation_id'],self.response({'accepted':True}))
        self.orders[-1]['state']='cancelled';self.book.reconcile(self.snap('2'))
        exit_action=self.action('exit');payload=self.place(exit_action['id'],self.snap('2'))
        self.orders.append(self.raw(payload,state='filled',filled='2'));self.book.reconcile(self.snap())
        self.assertEqual(self.book.state()['trades']['engineering-native-fixture']['phase'],'FLAT_VERIFIED')
        events=[json.loads(r[0]) for r in self.book.db.execute('SELECT payload FROM audit_events')]
        self.assertEqual(sum(e['event_type']=='ENGINEERING_EXIT_REQUESTED' for e in events),1)

    def test_engineering_exit_cannot_bypass_unknown_or_unfilled_or_strategy(self):
        action=self.engineering_entry()
        with self.assertRaisesRegex(ValueError,'ENGINEERING_EXIT_REQUIRES_OBSERVED_POSITION_AND_FILL'):
            self.book.request_engineering_exit('engineering-native-fixture',self.snap())
        self.review(action['id'],self.snap())
        packet=self.native.begin(action['id'],'place',self.snap(),quote=self.quote())
        self.native.record(packet['operation_id'],{'isError':True})
        with self.assertRaisesRegex(ValueError,'UNRESOLVED_ACTION_BLOCKS_ENGINEERING_EXIT_REQUEST'):
            self.book.request_engineering_exit('engineering-native-fixture',self.snap())
        with self.assertRaisesRegex(ValueError,'ENGINEERING_TRADE_REQUIRED'):
            self.book.request_engineering_exit('strategy-or-absent',self.snap())

    def test_readonly_entry_expiry_reconciles_flat_without_lingering_review(self):
        action=self.engineering_entry();self.review(action['id'],self.snap())
        self.now+=timedelta(seconds=16);self.book.reconcile(self.snap())
        self.assertEqual(next(a['state'] for a in self.book.actions() if a['id']==action['id']),'EXPIRED')
        self.assertEqual(self.book.state()['trades']['engineering-native-fixture']['phase'],'FLAT_VERIFIED')
        self.assertEqual(self.book.summary()['unresolved_actions'],0)

    def test_entry_pending_approval_is_not_assumed_flat_after_local_expiry(self):
        action=self.engineering_entry();self.place(action['id'],self.snap(),approval_id='entry-pending-after-expiry')
        self.now+=timedelta(seconds=16);self.book.reconcile(self.snap())
        self.assertEqual(next(a['state'] for a in self.book.actions() if a['id']==action['id']),'REPORTED')
        self.assertEqual(self.book.state()['trades']['engineering-native-fixture']['phase'],'ENTRY_AWAITING_BROKER')

    def delayed_alias_fill(self):
        action=self.engineering_entry();self.review(action['id'],self.snap())
        packet=self.native.begin(action['id'],'place',self.snap(),quote=self.quote())
        payload=json.loads(next(a['payload'] for a in self.book.actions() if a['id']==action['id']))
        self.orders=[self.raw(payload,state='filled',filled='2',ref_id='late-official-approval')]
        self.now+=timedelta(seconds=1);self.book.reconcile(self.snap('2'))
        self.assertIn('POSITION_NOT_EXPLAINED_BY_OWN_EXECUTIONS:DEMO',self.book.state()['issues'])
        self.assertIn('UNMANAGED_POSITION_OR_ORDER',self.book.state()['issues'])
        return packet

    def test_exact_late_alias_and_execution_reconciliation_resolve_only_current_gap(self):
        packet=self.delayed_alias_fill()
        self.native.record(packet['operation_id'],self.response({'approval_id':'late-official-approval'}))
        self.now+=timedelta(seconds=1);self.book.reconcile(self.snap('2'))
        self.assertNotIn('POSITION_NOT_EXPLAINED_BY_OWN_EXECUTIONS:DEMO',self.book.state()['issues'])
        self.assertNotIn('UNMANAGED_POSITION_OR_ORDER',self.book.state()['issues'])
        self.assertEqual(json.loads(self.action('protect')['payload'])['parameters']['quantity'],'2')
        events=[json.loads(r[0]) for r in self.book.db.execute('SELECT payload FROM audit_events')]
        resolved=[e for e in events if e['event_type']=='RECONCILIATION_ISSUE' and e.get('details',{}).get('status')=='resolved']
        self.assertEqual(len(resolved),2)
        self.assertTrue(all(e['details']['broker_verified'] for e in resolved))

    def test_late_alias_never_clears_historical_or_unrelated_sticky_issues(self):
        action=self.engineering_entry();self.review(action['id'],self.snap())
        packet=self.native.begin(action['id'],'place',self.snap(),quote=self.quote())
        state=self.book.state();state['issues']=['UNMANAGED_POSITION_OR_ORDER','PROTECTION_WAIT_EXCEEDED:OTHER']
        self.book._save(state);self.book.db.commit()
        payload=json.loads(action['payload']);self.orders=[self.raw(payload,state='filled',filled='2',ref_id='late-official-approval')]
        self.now+=timedelta(seconds=1);self.book.reconcile(self.snap('2'))
        self.native.record(packet['operation_id'],self.response({'approval_id':'late-official-approval'}))
        self.now+=timedelta(seconds=1);self.book.reconcile(self.snap('2'))
        self.assertIn('UNMANAGED_POSITION_OR_ORDER',self.book.state()['issues'])
        self.assertIn('PROTECTION_WAIT_EXCEEDED:OTHER',self.book.state()['issues'])

    def test_unmatched_alias_and_unknown_write_keep_explanation_issues_frozen(self):
        packet=self.delayed_alias_fill()
        self.native.record(packet['operation_id'],{'isError':True})
        self.now+=timedelta(seconds=1);self.book.reconcile(self.snap('2'))
        self.assertEqual(next(a['state'] for a in self.book.actions() if a['kind']=='entry'),'UNKNOWN')
        self.assertIn('POSITION_NOT_EXPLAINED_BY_OWN_EXECUTIONS:DEMO',self.book.state()['issues'])
        self.assertIn('UNMANAGED_POSITION_OR_ORDER',self.book.state()['issues'])
        self.assertIsNone(self.book.claim())

    def test_wrong_official_alias_cannot_clear_gap_from_same_symbol_and_terms(self):
        packet=self.delayed_alias_fill()
        self.native.record(packet['operation_id'],self.response({'approval_id':'different-explicit-approval'}))
        self.now+=timedelta(seconds=1);self.book.reconcile(self.snap('2'))
        self.assertIn('POSITION_NOT_EXPLAINED_BY_OWN_EXECUTIONS:DEMO',self.book.state()['issues'])
        self.assertIn('UNMANAGED_POSITION_OR_ORDER',self.book.state()['issues'])

    def test_resolved_alias_gap_preserves_risk_and_protection_sticky_issues(self):
        packet=self.delayed_alias_fill()
        state=self.book.state();state['issues']+=['PROTECTION_WAIT_EXCEEDED:OTHER','DAILY_EQUITY_LOSS_LOCKED']
        self.book._save(state);self.book.db.commit()
        self.native.record(packet['operation_id'],self.response({'approval_id':'late-official-approval'}))
        self.now+=timedelta(seconds=1);self.book.reconcile(self.snap('2'))
        self.assertEqual(self.book.state()['issues'],['DAILY_EQUITY_LOSS_LOCKED','PROTECTION_WAIT_EXCEEDED:OTHER'])

    def approval_snapshot(self,identity,state,qty='0',*,edited=False,asset_class='ASSET_CLASS_EQUITY'):
        value=self.snap(qty)
        value.update(requested_approval_ids=[identity],missing_approval_ids=[],approval_states={identity:{
            'approval_id':identity,'asset_class':asset_class,'state':state,'is_edited':edited}})
        return value

    def test_formal_entry_decline_proves_flat_without_rearming_reference(self):
        action=self.engineering_entry();self.place(action['id'],self.snap(),approval_id='formal-decline')
        self.assertEqual(self.book.pending_approval_ids(),['formal-decline'])
        self.book.reconcile(self.approval_snapshot('formal-decline','APPROVAL_STATE_DECLINED'))
        self.assertEqual(next(a['state'] for a in self.book.actions() if a['id']==action['id']),'APPROVAL_DECLINED')
        self.assertEqual(self.book.state()['trades']['engineering-native-fixture']['phase'],'FLAT_VERIFIED')
        self.assertEqual(self.book.pending_approval_ids(),[])
        self.assertEqual(self.book.summary()['unresolved_actions'],0)
        self.assertEqual(self.book.audit.report('2026-10-16')['unresolved_agent_intents'],[])

    def test_formal_entry_expiry_proves_flat_but_local_clock_alone_does_not(self):
        action=self.engineering_entry();self.place(action['id'],self.snap(),approval_id='formal-expire')
        self.now+=timedelta(seconds=121);self.book.reconcile(self.snap())
        self.assertEqual(self.book.state()['trades']['engineering-native-fixture']['phase'],'ENTRY_AWAITING_BROKER')
        self.book.reconcile(self.approval_snapshot('formal-expire','APPROVAL_STATE_EXPIRED'))
        self.assertEqual(next(a['state'] for a in self.book.actions() if a['id']==action['id']),'APPROVAL_EXPIRED')
        self.assertEqual(self.book.state()['trades']['engineering-native-fixture']['phase'],'FLAT_VERIFIED')

    def test_approved_is_not_fill_and_missing_id_is_not_a_terminal_approval(self):
        action=self.engineering_entry();self.place(action['id'],self.snap(),approval_id='await-order')
        for state in ('APPROVAL_STATE_APPROVED','APPROVAL_STATE_AUTO_APPROVED'):
            self.book.reconcile(self.approval_snapshot('await-order',state))
            self.assertEqual(next(a['state'] for a in self.book.actions() if a['id']==action['id']),'REPORTED')
            self.assertEqual(self.book.state()['trades']['engineering-native-fixture']['phase'],'ENTRY_AWAITING_BROKER')
        missing=self.snap();missing.update(requested_approval_ids=['await-order'],missing_approval_ids=['await-order'],approval_states={})
        self.book.reconcile(missing)
        self.assertEqual(self.book.pending_approval_ids(),['await-order'])

    def test_pending_protection_disappearing_from_list_cannot_queue_or_dispatch_second_sell(self):
        self.filled_entry();protection=self.action('protect');self.place(protection['id'],self.snap('2'),approval_id='latent-stop')
        self.now+=timedelta(seconds=31);self.book.reconcile(self.snap('2'))
        self.assertFalse(any(a['kind']=='exit' and a['state']=='READY' for a in self.book.actions()))
        # A pre-upgrade READY outbox row must also fail fresh native validation.
        state=self.book.state();trade=state['trades']['native-fixture'];self.book._begin()
        exit_id=self.book._queue(trade,'exit',{'symbol':'DEMO','side':'sell','type':'market','quantity':'2',
            'market_hours':'regular_hours','time_in_force':'gfd'},self.now,'OLD_OUTBOX_FIXTURE')
        self.book._save(state);self.book.db.commit()
        with self.assertRaisesRegex(ValueError,'UNRESOLVED_SELL_PLACE_BLOCKS_EXTERNAL_DISPATCH'):
            self.native.preview(exit_id,self.snap('2'),quote=self.quote())

    def test_formal_protection_expiry_unblocks_exit_without_rearming_old_stop(self):
        self.filled_entry();protection=self.action('protect');self.place(protection['id'],self.snap('2'),approval_id='expired-stop')
        self.book.reconcile(self.approval_snapshot('expired-stop','APPROVAL_STATE_EXPIRED','2'))
        self.assertEqual(next(a['state'] for a in self.book.actions() if a['id']==protection['id']),'APPROVAL_EXPIRED')
        self.assertEqual(self.action('exit')['kind'],'exit')
        self.book.reconcile(self.approval_snapshot('expired-stop','APPROVAL_STATE_EXPIRED','2'))
        self.assertFalse(any(a['kind']=='protect' and a['state']=='READY' for a in self.book.actions()))
        events=[json.loads(r[0]) for r in self.book.db.execute('SELECT payload FROM audit_events')]
        self.assertTrue(any(e.get('details',{}).get('evidence')=='fresh_bound_exact_approval_ids_and_complete_order_snapshot' for e in events))

    def test_edited_unknown_wrong_asset_or_unrequested_terminal_do_not_release_protection(self):
        self.filled_entry();protection=self.action('protect');self.place(protection['id'],self.snap('2'),approval_id='ambiguous-stop')
        for mode in ('edited','unknown','asset','unrequested'):
            snap=self.approval_snapshot('ambiguous-stop','APPROVAL_STATE_DECLINED','2',
                edited=mode=='edited',asset_class='ASSET_CLASS_OPTION' if mode=='asset' else 'ASSET_CLASS_EQUITY')
            if mode=='unknown':snap['approval_states']['ambiguous-stop']['state']='UNDOCUMENTED_STATE'
            if mode=='unrequested':snap['requested_approval_ids']=[]
            self.book.reconcile(snap)
            self.assertEqual(next(a['state'] for a in self.book.actions() if a['id']==protection['id']),'REPORTED')
            self.assertFalse(any(a['kind']=='exit' and a['state']=='READY' for a in self.book.actions()))

    def test_terminal_approval_with_corresponding_order_is_a_contradiction(self):
        action=self.engineering_entry();payload=self.place(action['id'],self.snap(),approval_id='contradiction')
        self.orders=[self.raw(payload,ref_id='contradiction')]
        with self.assertRaisesRegex(ValueError,'BROKER_APPROVAL_TERMINAL_ORDER_CONTRADICTION'):
            self.book.reconcile(self.approval_snapshot('contradiction','APPROVAL_STATE_DECLINED'))

    def test_exit_approval_decline_requires_manual_intervention_without_automatic_retry(self):
        self.engineering_filled();self.book.request_engineering_exit('engineering-native-fixture',self.snap('2'))
        exit_action=self.action('exit');self.place(exit_action['id'],self.snap('2'),approval_id='declined-exit')
        snap=self.approval_snapshot('declined-exit','APPROVAL_STATE_DECLINED','2');self.book.reconcile(snap)
        self.assertTrue(self.book.state()['trades']['engineering-native-fixture']['manual_intervention_required'].startswith('EXIT_APPROVAL_DECLINED:'))
        self.assertFalse(any(a['kind']=='exit' and a['state']=='READY' for a in self.book.actions()))
        self.book.reconcile(snap)
        self.assertEqual(next(a['state'] for a in self.book.actions() if a['id']==exit_action['id']),'APPROVAL_DECLINED')

    def test_direct_native_declined_approval_is_tracked_but_not_final_until_fresh_read(self):
        action=self.engineering_entry();self.review(action['id'],self.snap())
        packet=self.native.begin(action['id'],'place',self.snap(),quote=self.quote())
        response=self.response({'order':None,'approval':{'approval_id':'direct-decline','state':'APPROVAL_STATE_DECLINED',
            'asset_class':'ASSET_CLASS_EQUITY','is_edited':False}})
        self.native.record(packet['operation_id'],response)
        self.assertEqual(self.book.pending_approval_ids(),['direct-decline'])
        self.book.reconcile(self.snap())
        self.assertEqual(next(a['state'] for a in self.book.actions() if a['id']==action['id']),'REPORTED')
        self.book.reconcile(self.approval_snapshot('direct-decline','APPROVAL_STATE_DECLINED'))
        self.assertEqual(self.book.state()['trades']['engineering-native-fixture']['phase'],'FLAT_VERIFIED')

    def test_unknown_native_state_retains_exact_id_for_readonly_resolution(self):
        action=self.engineering_entry();self.review(action['id'],self.snap())
        packet=self.native.begin(action['id'],'place',self.snap(),quote=self.quote())
        response=self.response({'order':None,'approval':{'approval_id':'tracked-unknown','state':'UNKNOWN_NEW_STATE',
            'asset_class':'ASSET_CLASS_EQUITY','is_edited':False}})
        self.native.record(packet['operation_id'],response)
        self.assertEqual(self.book.pending_approval_ids(),['tracked-unknown'])
        self.assertIsNone(self.book.claim())
        self.book.reconcile(self.approval_snapshot('tracked-unknown','APPROVAL_STATE_EXPIRED'))
        self.assertEqual(next(a['state'] for a in self.book.actions() if a['id']==action['id']),'APPROVAL_EXPIRED')
        self.assertEqual(self.book.audit.report('2026-10-16')['unresolved_agent_intents'],[])

    def test_new_engineering_intent_is_grant_bounded_without_refreshing_identity(self):
        deadline=(self.now+timedelta(minutes=10)).isoformat()
        action=self.engineering_entry(parameters_valid_until=deadline)
        original=json.loads(action['payload'])
        self.assertEqual(original['expires_at'],deadline)
        self.now+=timedelta(seconds=20)
        state=self.book.state();trade=state['trades']['engineering-native-fixture']
        self.book._begin()
        same=self.book._queue(trade,'entry',original['parameters'],self.now,'SAME_AUTHORIZED_ACTION')
        self.book.db.commit()
        current=json.loads(next(a['payload'] for a in self.book.actions() if a['id']==same))
        self.assertEqual(current['ref_id'],original['ref_id'])
        self.assertEqual(current['expires_at'],original['expires_at'])
        self.assertEqual(self.native.preview(same,self.snap(),quote=self.quote())['next_operation'],'review')

    def test_new_engineering_handoff_ttl_never_exceeds_explicit_parameter_deadline(self):
        deadline=(self.now+timedelta(seconds=45)).isoformat()
        action=self.engineering_entry(parameters_valid_until=deadline)
        self.assertEqual(json.loads(action['payload'])['expires_at'],deadline)

    def test_longer_engineering_handoff_keeps_quote_and_review_freshness(self):
        action=self.engineering_entry(parameters_valid_until=(self.now+timedelta(minutes=10)).isoformat())
        oldquote=self.quote();self.review(action['id'],self.snap())
        self.now+=timedelta(seconds=20)
        with self.assertRaisesRegex(ValueError,'ENTRY_GATES:QUOTE_STALE_OR_FUTURE'):
            self.native.preview(action['id'],self.snap(),quote=oldquote)
        self.now+=timedelta(seconds=11)
        with self.assertRaisesRegex(ValueError,'FRESH_NATIVE_REVIEW_REQUIRED'):
            self.native.begin(action['id'],'place',self.snap(),quote=self.quote())

    def test_strategy_handoff_stays_fifteen_seconds_even_with_parameter_deadline(self):
        self.book.propose('strategy-ttl',self.plan,self.quote(),self.snap(),
            evidence={'synthetic':True,'source_sha256':'strategy-ttl','parameters_valid_until':(self.now+timedelta(minutes=10)).isoformat()})
        self.assertEqual(json.loads(self.action('entry')['payload'])['expires_at'],(self.now+timedelta(seconds=15)).isoformat())

    def direct_order_with_rewritten_ref(self,action_id,snap,identity,ref,**changes):
        self.review(action_id,snap)
        payload=json.loads(next(a['payload'] for a in self.book.actions() if a['id']==action_id))
        packet=self.native.begin(action_id,'place',snap,quote=self.quote())
        raw=self.raw(payload);raw.update(id=identity,ref_id=ref,**changes)
        result=self.native.record(packet['operation_id'],self.response({'order':raw,'approval':None}))
        return payload,result,raw

    def test_native_order_id_connects_rewritten_ref_entry_protection_and_exit(self):
        entry=self.engineering_entry()
        payload,result,buy=self.direct_order_with_rewritten_ref(entry['id'],self.snap(),'actual-buy-id','broker-rewritten-buy-ref')
        self.assertTrue(result['response_usable']);self.assertFalse(result['broker_effect_verified'])
        self.assertEqual(self.book.pending_approval_ids(),[])
        self.assertNotIn('broker-rewritten-buy-ref',self.book._order_refs(entry))
        filled=self.raw(payload,state='filled',filled='2');filled.update(id=buy['id'],ref_id=buy['ref_id'])
        self.orders=[filled];self.now+=timedelta(seconds=1);self.book.reconcile(self.snap('2'))
        protection=self.action('protect')
        _,result,stop=self.direct_order_with_rewritten_ref(protection['id'],self.snap('2'),'actual-stop-id','broker-rewritten-stop-ref')
        self.assertTrue(result['response_usable']);self.orders.append(stop)
        self.book.reconcile(self.snap('2','2'))
        self.assertEqual(self.book.state()['trades']['engineering-native-fixture']['phase'],'PROTECTED')
        self.assertEqual(self.book.state()['issues'],[])
        self.book.request_engineering_exit('engineering-native-fixture',self.snap('2','2'))
        cancel=self.action('cancel');packet=self.native.begin(cancel['id'],'cancel',self.snap('2','2'))
        self.assertEqual(packet['parameters']['order_id'],'actual-stop-id')
        self.native.record(packet['operation_id'],self.response({'accepted':True}))
        self.orders[-1]['state']='cancelled';self.book.reconcile(self.snap('2'))
        exit_action=self.action('exit')
        payload,result,exit_order=self.direct_order_with_rewritten_ref(exit_action['id'],self.snap('2'),'actual-exit-id','broker-rewritten-exit-ref')
        raw=self.raw(payload,state='filled',filled='2');raw.update(id=exit_order['id'],ref_id=exit_order['ref_id'])
        self.orders.append(raw);self.book.reconcile(self.snap())
        self.assertEqual(self.book.state()['trades']['engineering-native-fixture']['phase'],'FLAT_VERIFIED')
        self.assertEqual(self.book.state()['issues'],[])
        self.assertEqual(self.book.pending_approval_ids(),[])
        self.assertEqual(self.book.db.execute('SELECT COUNT(*) FROM external_broker_order_ids').fetchone()[0],3)

    def test_rewritten_ref_same_symbol_terms_without_returned_order_id_is_not_guessed(self):
        action=self.engineering_entry();self.review(action['id'],self.snap())
        packet=self.native.begin(action['id'],'place',self.snap(),quote=self.quote())
        self.native.record(packet['operation_id'],{'isError':True})
        payload=json.loads(action['payload']);raw=self.raw(payload,state='filled',filled='2')
        raw.update(id='unknown-order-id',ref_id='unknown-ref');self.orders=[raw]
        self.now+=timedelta(seconds=1);self.book.reconcile(self.snap('2'))
        self.assertIn('POSITION_NOT_EXPLAINED_BY_OWN_EXECUTIONS:DEMO',self.book.state()['issues'])
        self.assertEqual(next(a['state'] for a in self.book.actions() if a['id']==action['id']),'UNKNOWN')

    def test_returned_exact_order_id_still_requires_exact_later_order_terms(self):
        action=self.engineering_entry()
        _,result,raw=self.direct_order_with_rewritten_ref(action['id'],self.snap(),'exact-id','rewritten-ref')
        self.assertTrue(result['response_usable'])
        raw['quantity']='3';self.orders=[raw]
        with self.assertRaisesRegex(ValueError,'BROKER_ORDER_DOES_NOT_MATCH_INTENT'):
            self.book.reconcile(self.snap())

    def test_native_order_account_mismatch_is_unknown_without_identity_registration(self):
        action=self.engineering_entry()
        _,result,_=self.direct_order_with_rewritten_ref(action['id'],self.snap(),'foreign-id','foreign-ref',account_number='FOREIGN-ACCOUNT')
        self.assertFalse(result['response_usable'])
        self.assertEqual(result['report']['status'],'unknown')
        self.assertEqual(self.book.db.execute('SELECT COUNT(*) FROM external_broker_order_ids').fetchone()[0],0)
        self.assertNotIn('FOREIGN-ACCOUNT',json.dumps(result))

    def test_delayed_native_order_id_receipt_resolves_only_its_original_explanation_gap(self):
        action=self.engineering_entry();self.review(action['id'],self.snap())
        packet=self.native.begin(action['id'],'place',self.snap(),quote=self.quote())
        payload=json.loads(action['payload']);raw=self.raw(payload,state='filled',filled='2')
        raw.update(id='late-exact-id',ref_id='late-rewritten-ref');self.orders=[raw]
        self.now+=timedelta(seconds=1);self.book.reconcile(self.snap('2'))
        self.assertIn('POSITION_NOT_EXPLAINED_BY_OWN_EXECUTIONS:DEMO',self.book.state()['issues'])
        self.native.record(packet['operation_id'],self.response({'order':raw,'approval':None}))
        self.book.reconcile(self.snap('2'))
        self.assertEqual(self.book.state()['issues'],[])
        self.assertEqual(json.loads(self.action('protect')['payload'])['parameters']['quantity'],'2')

    def test_manual_stop_is_never_adopted_from_matching_symbol_quantity_and_price(self):
        action=self.engineering_entry()
        payload,_,buy=self.direct_order_with_rewritten_ref(action['id'],self.snap(),'actual-buy','actual-buy-ref')
        raw=self.raw(payload,state='filled',filled='2');raw.update(id=buy['id'],ref_id=buy['ref_id'])
        self.orders=[raw];self.now+=timedelta(seconds=1);self.book.reconcile(self.snap('2'))
        protection=self.action('protect');payload=json.loads(protection['payload'])
        manual=self.raw(payload);manual.update(id='manual-stop-id',ref_id='manual-stop-ref')
        self.orders.append(manual);self.book.reconcile(self.snap('2','2'))
        self.assertNotEqual(self.book.state()['trades']['engineering-native-fixture']['phase'],'PROTECTED')
        self.assertIn('UNMANAGED_POSITION_OR_ORDER',self.book.state()['issues'])
        self.assertFalse(self.book.db.execute('SELECT 1 FROM external_broker_order_ids WHERE order_id=?',('manual-stop-id',)).fetchone())

    def test_order_identity_account_scope_is_immutable_across_receipt_replay(self):
        action=self.engineering_entry();self.review(action['id'],self.snap())
        packet=self.native.begin(action['id'],'place',self.snap(),quote=self.quote())
        raw=self.raw(json.loads(action['payload']));raw['account_number']='SIM-0000'
        response=self.response({'order':raw,'approval':None});result=self.native.record(packet['operation_id'],response)
        self.assertTrue(result['response_usable'])
        self.assertEqual(result,self.native.record(packet['operation_id'],response))
        raw['account_number']='FOREIGN-ACCOUNT'
        with self.assertRaisesRegex(ValueError,'NATIVE_RESPONSE_ID_CONFLICT'):
            self.native.record(packet['operation_id'],self.response({'order':raw,'approval':None}))

    def test_order_id_cannot_be_claimed_by_two_actions(self):
        action=self.engineering_entry()
        payload,_,buy=self.direct_order_with_rewritten_ref(action['id'],self.snap(),'unique-actual-id','actual-buy-ref')
        raw=self.raw(payload,state='filled',filled='2');raw.update(id=buy['id'],ref_id=buy['ref_id'])
        self.orders=[raw];self.now+=timedelta(seconds=1);self.book.reconcile(self.snap('2'))
        protection=self.action('protect');self.review(protection['id'],self.snap('2'))
        packet=self.native.begin(protection['id'],'place',self.snap('2'),quote=self.quote())
        raw=self.raw(json.loads(protection['payload']));raw.update(id='unique-actual-id',ref_id='other-ref')
        with self.assertRaisesRegex(ValueError,'BROKER_ORDER_ID_OWNER_CONFLICT'):
            self.native.record(packet['operation_id'],self.response({'order':raw,'approval':None}))
        self.assertIsNone(self.book.claim())

    def test_exact_stored_legacy_receipt_reconnects_identity_without_transition_or_issue_clear(self):
        from martin_agentic.audit import canonical,digest
        from martin_agentic.native_operations import sanitized
        action=self.engineering_entry();self.review(action['id'],self.snap())
        packet=self.native.begin(action['id'],'place',self.snap(),quote=self.quote())
        raw=self.raw(json.loads(action['payload']));raw.update(id='recorded-actual-id',ref_id='recorded-rewritten-ref')
        response=self.response({'order':raw,'approval':None})
        result=self.native.record(packet['operation_id'],response)
        # Simulate the old recorded schema that saved the exact receipt hash
        # and order ID but did not maintain an order-identity association table.
        self.book.db.execute('DELETE FROM external_broker_order_ids')
        result['response_hash']=digest(sanitized(response))
        self.book.db.execute('UPDATE native_operation_records SET result=? WHERE id=?',(canonical(result),packet['operation_id']))
        state=self.book.state();state['issues']=['UNMANAGED_POSITION_OR_ORDER'];self.book._save(state);self.book.db.commit()
        before_state=self.book.state();before_actions=self.book.actions()
        connected=self.native.reconnect_recorded_order(packet['operation_id'],response)
        self.assertFalse(connected['broker_effect_verified']);self.assertFalse(connected['duplicate'])
        self.assertEqual(self.book.state(),before_state);self.assertEqual(self.book.actions(),before_actions)
        self.assertEqual(self.book.pending_approval_ids(),[])
        self.assertTrue(self.native.reconnect_recorded_order(packet['operation_id'],response)['duplicate'])
        changed=deepcopy(response);changed['structuredContent']['data']['order']['id']='different-manual-id'
        with self.assertRaisesRegex(ValueError,'NATIVE_RESPONSE_ID_CONFLICT'):
            self.native.reconnect_recorded_order(packet['operation_id'],changed)

    def test_only_place_receipt_can_reconnect_order_and_unknown_receipts_stay_unknown(self):
        action=self.engineering_entry()
        packet=self.native.begin(action['id'],'review',self.snap(),quote=self.quote())
        with self.assertRaisesRegex(ValueError,'RECORDED_NATIVE_PLACE_REQUIRED'):
            self.native.reconnect_recorded_order(packet['operation_id'],{'data':{}})

    def protected_engineering_order(self):
        self.engineering_filled();action=self.action('protect');payload=self.place(action['id'],self.snap('2'))
        raw=self.raw(payload);self.orders.append(raw);self.book.reconcile(self.snap('2','2'))
        return action,payload

    def test_planned_exit_stop_cancel_has_no_uncovered_failure_but_unauthorized_cancel_does(self):
        self.protected_engineering_order()
        # A cancellation request without an exit decision is not the planned
        # close exemption, even though the native broker accepted it.
        state=self.book.state();trade=state['trades']['engineering-native-fixture'];self.book._begin()
        cancel_id=self.book._cancel(trade,self.snap('2','2')['orders'][self.orders[-1]['id']],self.now,'UNPLANNED_CANCEL_FIXTURE')
        self.book.db.commit()
        packet=self.native.begin(cancel_id,'cancel',self.snap('2','2'))
        self.native.record(packet['operation_id'],self.response({'accepted':True}))
        self.orders[-1]['state']='cancelled';self.book.reconcile(self.snap('2'))
        self.assertIn('PROTECTION_TERMINATED_UNCOVERED:DEMO',self.book.state()['issues'])

    def test_exit_request_does_not_excuse_unaccepted_cancel_or_failed_stop(self):
        self.protected_engineering_order();self.book.request_engineering_exit('engineering-native-fixture',self.snap('2','2'))
        cancel=self.action('cancel');packet=self.native.begin(cancel['id'],'cancel',self.snap('2','2'))
        self.native.record(packet['operation_id'],self.response({'accepted':False}))
        self.orders[-1]['state']='cancelled';self.book.reconcile(self.snap('2'))
        self.assertIn('PROTECTION_TERMINATED_UNCOVERED:DEMO',self.book.state()['issues'])

    def test_partial_stop_fill_racing_planned_cancel_exits_only_actual_remainder(self):
        _,payload=self.protected_engineering_order()
        self.book.request_engineering_exit('engineering-native-fixture',self.snap('2','2'))
        cancel=self.action('cancel');packet=self.native.begin(cancel['id'],'cancel',self.snap('2','2'))
        self.native.record(packet['operation_id'],self.response({'accepted':True}))
        self.orders[-1]=self.raw(payload,state='partially_filled_rest_cancelled',filled='1')
        self.book.reconcile(self.snap('1'))
        self.assertEqual(json.loads(self.action('exit')['payload'])['parameters']['quantity'],'1')
        self.assertNotIn('PROTECTION_TERMINATED_UNCOVERED:DEMO',self.book.state()['issues'])

    def test_exit_request_with_accepted_cancel_does_not_excuse_failed_stop(self):
        self.protected_engineering_order();self.book.request_engineering_exit('engineering-native-fixture',self.snap('2','2'))
        cancel=self.action('cancel');packet=self.native.begin(cancel['id'],'cancel',self.snap('2','2'))
        self.native.record(packet['operation_id'],self.response({'accepted':True}))
        self.orders[-1]['state']='failed';self.book.reconcile(self.snap('2'))
        self.assertIn('PROTECTION_TERMINATED_UNCOVERED:DEMO',self.book.state()['issues'])
