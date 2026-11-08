from copy import deepcopy
from datetime import datetime,timedelta
import json
from pathlib import Path
import tempfile
from unittest import TestCase
from unittest.mock import patch

from martin_luk.exits import exit_decision
from entry_authority_fixture import TestExecutionBook as ExecutionBook
from martin_agentic.agent_worker import dispatch_one, CodexOrderWorker, execution_prompt
from martin_luk.live_signals import LiveSignals
from martin_agentic.broker import normalize_order
from martin_agentic.transport import TransportError
from test_broker import snapshot,order,FakeBroker,position
from shadow_fixture import events as shadow_events

AT='2026-10-16T14:00:00+00:00'


class ExecutionTests(TestCase):
    def test_protection_deadline_audits_once_per_exposure_for_recurrence(self):
        raw=self.entry_filled()
        self.now+=timedelta(seconds=31)
        self.book.reconcile(self.snap([raw],qty='2'))
        self.now+=timedelta(seconds=1)
        self.book.reconcile(self.snap([raw],qty='2'))
        incidents=[json.loads(row[0]) for row in self.book.db.execute(
            "SELECT payload FROM audit_events WHERE event_id LIKE 'protection-deadline-%'")]
        self.assertEqual(len(incidents),1)
        self.assertEqual(incidents[0]['trade_id'],'signal-1')
        self.assertEqual(incidents[0]['details']['issues'],['PROTECTION_WAIT_EXCEEDED:DEMO'])

    def test_engineering_grant_survives_confirmation_wait_without_waiving_native_freshness(self):
        from martin_agentic.native_operations import NativeOperations
        deadline=self.now+timedelta(minutes=30)
        self.book.propose('engineering-grant',self.plan,self.quote,self.snap(),purpose='engineering',
            evidence={'synthetic':True,'source_sha256':'fixture','engineering_scope_authorized':True,
                'parameters_user_specified':True,'parameters_valid_until':deadline.isoformat()})
        action=self.book.actions()[0]
        self.assertEqual(json.loads(action['payload'])['expires_at'],deadline.isoformat())
        self.now+=timedelta(seconds=121)
        fresh_quote={**self.quote,**{k:self.now.isoformat() for k in ('bid_at','ask_at','observed_at')}}
        native=NativeOperations(self.book)
        with self.assertRaisesRegex(ValueError,'QUOTE_STALE'):
            native.preview(action['id'],self.snap(),quote=self.quote)
        self.assertEqual(native.preview(action['id'],self.snap(),quote=fresh_quote)['next_operation'],'review')
        with self.assertRaisesRegex(ValueError,'FRESH_NATIVE_REVIEW_REQUIRED'):
            native.begin(action['id'],'place',self.snap(),quote=fresh_quote)
        self.assertEqual(self.book.db.execute('SELECT count(*) FROM external_operations').fetchone()[0],0)

    def test_expired_engineering_parameters_cannot_create_new_intent(self):
        with self.assertRaisesRegex(ValueError,'ENGINEERING_PARAMETERS_EXPIRED'):
            self.book.propose('engineering-expired',self.plan,self.quote,self.snap(),purpose='engineering',
                evidence={'synthetic':True,'source_sha256':'fixture','engineering_scope_authorized':True,
                    'parameters_user_specified':True,'parameters_valid_until':(self.now-timedelta(seconds=1)).isoformat()})
        self.assertEqual(self.book.actions(),[])

    def test_engineering_only_policy_rejects_strategy_before_state_or_risk_changes(self):
        self.book.close();self.policy['engineering_only']=True
        self.book=ExecutionBook(Path(self.tmp.name)/'engineering.sqlite',self.policy,simulation=True,clock=lambda:self.now,
                exit_decider=exit_decision)
        with self.assertRaisesRegex(ValueError,'ENGINEERING_ACCEPTANCE_ONLY'):self.propose()
        self.assertEqual(self.book.actions(),[])
        self.book.propose('engineering-fixture',self.plan,self.quote,self.snap(),purpose='engineering',
            evidence={'synthetic':True,'source_sha256':'fixture','engineering_scope_authorized':True,'parameters_user_specified':True})
        self.assertEqual(len(self.book.actions()),1)

    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.path=Path(self.tmp.name)/'live.sqlite'
        self.now=datetime.fromisoformat(AT)
        self.policy=json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        self.book=ExecutionBook(self.path,self.policy,simulation=True,clock=lambda:self.now,
                exit_decider=exit_decision)
        self.plan={'symbol':'DEMO','quantity':2,'entry_ceiling':'40','stop':'39','cost_buffer_per_share':'0.05'}
        self.quote={'symbol':'DEMO','bid':'39.99','ask':'40','bid_at':AT,'ask_at':AT,'observed_at':AT}

    def tearDown(self):self.book.close();self.tmp.cleanup()
    def snap(self,orders=None,qty='0',held='0'):
        f=FakeBroker();f.orders=orders or [];f.positions=[] if qty=='0' else [position(qty,held)]
        s=snapshot(f,self.now.isoformat());s['synthetic']=True;return s
    def propose(self):
        return self.book.propose('signal-1',self.plan,self.quote,self.snap(),evidence={'source_sha256':'synthetic-fixture','synthetic':True})
    def bound(self,action,*,filled='0',state='confirmed',oid=None):
        p=action['parameters'];r=order(oid or action['action_id'],p['side'],p['quantity'],filled,state,p.get('stop_price'))
        r['ref_id']=action['ref_id'];r['price']=p.get('limit_price');r['type']='limit' if p['type']=='limit' else 'market'
        r['market_hours']=p['market_hours'];r['time_in_force']=p['time_in_force']
        return r
    def entry_filled(self):
        self.propose();a=self.book.claim();self.book.report(a['action_id'],{'status':'submitted','message':'not broker proof'})
        raw=self.bound(a,filled='2',state='filled')
        self.book.reconcile(self.snap([raw],qty='2'))
        return raw

    def test_durable_signal_and_ref_survive_restart(self):
        r=self.propose();self.assertTrue(self.propose()['duplicate']);action=self.book.actions()[0]
        ref=json.loads(action['payload'])['ref_id'];self.book.close()
        self.book=ExecutionBook(self.path,self.policy,simulation=True,clock=lambda:self.now,
                exit_decider=exit_decision)
        self.assertEqual(json.loads(self.book.actions()[0]['payload'])['ref_id'],ref)
        self.assertEqual(r['action_id'],action['id'])
    def test_concurrent_worker_cannot_claim_running_action(self):
        self.propose();first=self.book.claim();self.assertIsNotNone(first)
        self.assertIsNone(self.book.claim());self.assertEqual(self.book.audit.verify()['events_verified'],1)
    def test_stale_signal_expires_without_model_call(self):
        self.propose();self.now+=timedelta(seconds=16)
        r=dispatch_one(self.book,lambda action:self.fail('expired action must not wake model'))
        self.assertFalse(r['agent_called']);self.assertEqual(self.book.actions()[0]['state'],'EXPIRED')
    def test_empty_queue_has_zero_agent_calls(self):
        self.assertFalse(dispatch_one(self.book,lambda a:self.fail('idle'))['agent_called'])
    def test_unknown_is_not_automatically_retried(self):
        self.propose();dispatch_one(self.book,lambda a:{'status':'unknown','message':'timeout'})
        self.assertIsNone(self.book.claim());self.assertEqual(self.book.summary()['unresolved_actions'],1)
    def test_restart_marks_interrupted_running_unknown(self):
        self.propose();self.book.claim();self.book.recover()
        self.assertEqual(self.book.actions()[0]['state'],'UNKNOWN')
    def test_model_report_cannot_create_fill_or_protection(self):
        self.propose();a=self.book.claim();self.book.report(a['action_id'],{'status':'submitted','message':'filled and protected'})
        self.book.reconcile(self.snap());t=self.book.state()['trades']['signal-1']
        self.assertEqual(t['quantity'],'0');self.assertEqual(t['phase'],'ENTRY_AWAITING_BROKER')
        self.assertEqual(len(self.book.actions()),1)
    def test_actual_entry_fill_creates_exact_quantity_protection(self):
        self.entry_filled();protection=self.book.claim()
        self.assertEqual(protection['kind'],'protect');self.assertEqual(protection['parameters']['quantity'],'2')
        self.assertEqual(protection['parameters']['stop_price'],'39')
    def test_actual_confirmed_stop_establishes_protected_state(self):
        buy=self.entry_filled();protect=self.book.claim();self.book.report(protect['action_id'],{'status':'submitted'})
        stop=self.bound(protect)
        self.book.reconcile(self.snap([buy,stop],qty='2',held='2'))
        self.assertEqual(self.book.state()['trades']['signal-1']['phase'],'PROTECTED')
    def native_protection(self,*,waiting=True):
        buy=self.entry_filled()
        action=json.loads(next(a['payload'] for a in self.book.actions() if a['kind']=='protect'))
        if waiting:
            action=self.book.claim();self.book.report(action['action_id'],{'status':'needs_confirmation','message':'native preview'})
        return buy,action

    def test_native_waiting_action_report_is_advisory_until_broker_confirmation(self):
        buy,action=self.native_protection();ref=action['ref_id']
        prepared=self.book.prepare_external_action(action['action_id'],self.snap([buy],qty='2'),
            quote=self.quote,operation_id='native-place-1',operation='place')
        self.assertEqual(prepared['ref_id'],ref)
        self.assertIsNone(self.book.claim())
        report={'status':'pending_approval','message':'formal tool reports pending approval'}
        result=self.book.record_external_report(action['action_id'],report,operation_id='native-place-1',broker_ref_id='approval-1')
        self.assertFalse(result['broker_verified'])
        self.assertNotEqual(self.book.state()['trades']['signal-1']['phase'],'PROTECTED')
        stop=self.bound(action);stop['ref_id']='approval-1'
        self.book.reconcile(self.snap([buy,stop],qty='2',held='2'))
        self.assertEqual(self.book.state()['trades']['signal-1']['phase'],'PROTECTED')
        self.assertEqual(json.loads(next(a['payload'] for a in self.book.actions() if a['id']==action['action_id']))['ref_id'],ref)

    def test_native_ready_review_reserves_then_returns_to_waiting_user(self):
        buy,action=self.native_protection(waiting=False)
        self.book.prepare_external_action(action['action_id'],self.snap([buy],qty='2'),quote=self.quote,
            operation_id='native-review-1',operation='review')
        self.assertIsNone(self.book.claim())
        self.book.record_external_report(action['action_id'],{'status':'review_only','message':'preview, not confirmation'},operation_id='native-review-1')
        self.assertEqual(next(a['state'] for a in self.book.actions() if a['id']==action['action_id']),'WAITING_USER')
        self.assertEqual(self.book.audit.report('2026-10-16')['unresolved_agent_intents'],[])

    def test_native_report_replay_is_idempotent_and_conflicts_are_rejected(self):
        buy,action=self.native_protection()
        self.book.prepare_external_action(action['action_id'],self.snap([buy],qty='2'),quote=self.quote,
            operation_id='native-place-1',operation='place')
        report={'status':'submitted','message':'formal tool response','order_id':'stop-1'}
        self.book.record_external_report(action['action_id'],report,operation_id='native-place-1')
        self.assertTrue(self.book.record_external_report(action['action_id'],report,operation_id='native-place-1')['duplicate'])
        with self.assertRaisesRegex(ValueError,'EXTERNAL_REPORT_CONFLICT'):
            self.book.record_external_report(action['action_id'],{'status':'rejected'},operation_id='native-place-1')

    def test_native_unknown_cannot_be_overwritten_and_recovers_from_exact_order(self):
        buy,action=self.native_protection(waiting=False)
        self.book.prepare_external_action(action['action_id'],self.snap([buy],qty='2'),quote=self.quote,
            operation_id='native-place-1',operation='place')
        self.book.record_external_report(action['action_id'],{'status':'unknown','message':'native timeout'},operation_id='native-place-1')
        with self.assertRaisesRegex(ValueError,'EXTERNAL_REPORT_CONFLICT'):
            self.book.record_external_report(action['action_id'],{'status':'submitted'},operation_id='native-place-1')
        self.assertIsNone(self.book.claim())
        self.book.reconcile(self.snap([buy,self.bound(action)],qty='2',held='2'))
        self.assertEqual(self.book.state()['trades']['signal-1']['phase'],'PROTECTED')
        self.assertEqual(self.book.audit.report('2026-10-16')['unresolved_agent_intents'],[])

    def test_native_restart_preserves_original_operation_for_genuine_late_receipt(self):
        buy,action=self.native_protection(waiting=False)
        self.book.prepare_external_action(action['action_id'],self.snap([buy],qty='2'),quote=self.quote,
            operation_id='native-place-1',operation='place')
        recovery=self.book.recover()
        self.assertEqual(next(a['state'] for a in self.book.actions() if a['id']==action['action_id']),'RUNNING')
        self.assertEqual(recovery['native_operations_pending'][0]['operation_id'],'native-place-1')
        self.assertFalse(recovery['automatic_resubmission_permitted'])
        self.assertIsNone(self.book.db.execute('SELECT report FROM external_operations WHERE operation_id=?',('native-place-1',)).fetchone()[0])
        self.assertIsNone(self.book.claim())
        self.book.record_external_report(action['action_id'],{'status':'pending_approval'},
            operation_id='native-place-1',broker_ref_id='genuine-approval-after-recovery')
        stop=self.bound(action);stop['ref_id']='genuine-approval-after-recovery'
        self.book.reconcile(self.snap([buy,stop],qty='2',held='2'))
        self.assertEqual(self.book.state()['trades']['signal-1']['phase'],'PROTECTED')

    def test_native_review_timeout_does_not_create_unknown_submission(self):
        buy,action=self.native_protection(waiting=False)
        self.book.prepare_external_action(action['action_id'],self.snap([buy],qty='2'),quote=self.quote,
            operation_id='native-review-1',operation='review')
        self.book.recover()
        # A daemon restart does not establish that a still-active root review
        # timed out. Its actual read failure may subsequently release the slot.
        self.assertEqual(next(a['state'] for a in self.book.actions() if a['id']==action['action_id']),'RUNNING')
        self.book.record_external_report(action['action_id'],{'status':'unknown','message':'ACTUAL_NATIVE_READ_TIMEOUT'},
            operation_id='native-review-1')
        self.assertEqual(next(a['state'] for a in self.book.actions() if a['id']==action['action_id']),'WAITING_USER')
        self.assertEqual(self.book.audit.report('2026-10-16')['unresolved_agent_intents'],[])
        self.book.prepare_external_action(action['action_id'],self.snap([buy],qty='2'),quote=self.quote,
            operation_id='native-review-2',operation='review')

    def test_second_book_recovery_does_not_invalidate_live_root_native_reservation(self):
        buy,action=self.native_protection(waiting=False)
        self.book.prepare_external_action(action['action_id'],self.snap([buy],qty='2'),quote=self.quote,
            operation_id='native-live-owner',operation='place')
        other=ExecutionBook(self.path,self.policy,simulation=True,clock=lambda:self.now,
                exit_decider=exit_decision)
        try:
            recovery=other.recover()
            self.assertEqual(recovery['native_operations_pending'][0]['owner_liveness'],
                'NOT_INFERRED_FROM_SUPERVISOR_LOCK')
            self.assertIsNone(other.claim())
            self.book.record_external_report(action['action_id'],{'status':'submitted','order_id':'exact-native-order'},
                operation_id='native-live-owner')
            other.reconcile(self.snap([buy,self.bound(action)],qty='2',held='2'))
            self.assertEqual(other.state()['trades']['signal-1']['phase'],'PROTECTED')
        finally:other.close()

    def test_native_unknown_write_remains_unknown_across_recovery(self):
        buy,action=self.native_protection(waiting=False)
        self.book.prepare_external_action(action['action_id'],self.snap([buy],qty='2'),quote=self.quote,
            operation_id='native-genuine-unknown',operation='place')
        self.book.record_external_report(action['action_id'],{'status':'unknown'},operation_id='native-genuine-unknown')
        self.book.recover()
        self.assertEqual(next(a['state'] for a in self.book.actions() if a['id']==action['action_id']),'UNKNOWN')
        self.assertIsNone(self.book.claim())

    def test_native_exit_handoff_rejects_any_open_sell_order(self):
        buy=self.entry_filled();quote={**self.quote,'bid':'38.99','ask':'39'}
        self.book.reconcile(self.snap([buy],qty='2'),quotes={'DEMO':quote})
        action=next(a for a in self.book.actions() if a['kind']=='exit')
        sell=order('foreign-sell',side='sell',qty='2',filled='0',state='confirmed')
        with self.assertRaisesRegex(ValueError,'EXTERNAL_ACTION_OPEN_SELL_ORDER'):
            self.book.validate_external_action(action['id'],self.snap([buy,sell],qty='2'),quote=quote)

    def test_native_handoff_rejects_stale_quantity_quote_and_expired_action(self):
        buy,action=self.native_protection()
        with self.assertRaisesRegex(ValueError,'QUANTITY_NO_LONGER_VALID'):
            self.book.validate_external_action(action['action_id'],self.snap([buy],qty='1'),quote=self.quote)
        self.now+=timedelta(seconds=16)
        with self.assertRaisesRegex(ValueError,'QUOTE_STALE_OR_FUTURE'):
            self.book.validate_external_action(action['action_id'],self.snap([buy],qty='2'),quote=self.quote)
        self.now+=timedelta(seconds=105)
        with self.assertRaisesRegex(ValueError,'EXTERNAL_ACTION_EXPIRED'):
            self.book.validate_external_action(action['action_id'],self.snap([buy],qty='2'),quote=self.quote)

    def test_superseded_broker_snapshot_cannot_roll_back_trade_or_reach_risk_guard(self):
        self.propose();older=self.snap()
        entry=self.book.claim();self.book.report(entry['action_id'],{'status':'submitted'})
        self.now+=timedelta(seconds=1)
        self.book.reconcile(self.snap([self.bound(entry,filled='2',state='filled')],qty='2'))
        before=self.book.state();actions=self.book.actions()
        with patch.object(self.book.risk_guard,'observe',side_effect=AssertionError('old snapshot reached risk guard')):
            with self.assertRaisesRegex(TransportError,'BROKER_SNAPSHOT_SUPERSEDED'):
                self.book.reconcile(older)
        self.assertEqual(self.book.state(),before)
        self.assertEqual(self.book.actions(),actions)

    def test_external_prepare_rejects_snapshot_superseded_by_native_observer(self):
        buy,action=self.native_protection(waiting=False);older=self.snap([buy],qty='2')
        self.now+=timedelta(seconds=1);self.book.reconcile(self.snap([buy],qty='2'))
        before=self.book.state();actions=self.book.actions()
        with self.assertRaisesRegex(TransportError,'BROKER_SNAPSHOT_SUPERSEDED'):
            self.book.prepare_external_action(action['action_id'],older,quote=self.quote,
                operation_id='superseded-native-place',operation='place')
        self.assertEqual(self.book.state(),before)
        self.assertEqual(self.book.actions(),actions)
        self.assertIsNone(self.book.db.execute('SELECT 1 FROM external_operations WHERE operation_id=?',('superseded-native-place',)).fetchone())

    def test_newer_observation_between_precheck_and_transaction_cannot_be_overwritten(self):
        self.propose();older=self.snap()
        entry=self.book.claim();self.book.report(entry['action_id'],{'status':'submitted'})
        self.now+=timedelta(seconds=1)
        newer=self.snap([self.bound(entry,filled='2',state='filled')],qty='2')
        begin=self.book._begin
        def competing_observer_then_begin():
            other=ExecutionBook(self.path,self.policy,simulation=True,clock=lambda:self.now,
                exit_decider=exit_decision)
            try:other.reconcile(newer)
            finally:other.close()
            begin()
        with patch.object(self.book,'_begin',side_effect=competing_observer_then_begin):
            with self.assertRaisesRegex(TransportError,'BROKER_SNAPSHOT_SUPERSEDED'):
                self.book.reconcile(older)
        trade=self.book.state()['trades']['signal-1']
        self.assertEqual(trade['quantity'],'2')
        self.assertEqual(trade['snapshot_at'],newer['observed_at'])

    def test_native_alias_does_not_accept_wrong_order_terms_or_infer_unknown_ref(self):
        buy,action=self.native_protection()
        stop=self.bound(action);stop['ref_id']='unmapped-approval'
        self.book.reconcile(self.snap([buy,stop],qty='2',held='2'))
        self.assertNotEqual(self.book.state()['trades']['signal-1']['phase'],'PROTECTED')
        self.book.prepare_external_action(action['action_id'],self.snap([buy],qty='2'),quote=self.quote,
            operation_id='native-place-1',operation='place')
        self.book.record_external_report(action['action_id'],{'status':'submitted'},operation_id='native-place-1',broker_ref_id='explicit-approval')
        stop['ref_id']='explicit-approval';stop['stop_price']='38'
        with self.assertRaisesRegex(ValueError,'TERMS_MISMATCH'):
            self.book.reconcile(self.snap([buy,stop],qty='2',held='2'))

    def test_native_entry_gate_is_never_opened_by_external_handoff(self):
        self.propose();action=self.book.actions()[0]
        # An unqualified strategy entry still cannot reach the native channel.
        with self.assertRaisesRegex(ValueError,'STRATEGY_ENTRY_NOT_QUALIFIED'):
            self.book.prepare_external_action(action['id'],self.snap(),quote=self.quote,
                operation_id='native-entry-1',operation='place')

    def test_protection_timing_is_first_full_coverage_observation_not_acceptance(self):
        buy=self.entry_filled();protect=self.book.claim();self.book.report(protect['action_id'],{'status':'submitted'})
        stop=self.bound(protect);self.now+=timedelta(seconds=2)
        self.book.reconcile(self.snap([buy,stop],qty='2',held='2'))
        first=self.book.state()['trades']['signal-1']
        self.assertEqual(first['first_protection_observed_at'],self.now.isoformat())
        self.assertEqual(first['protection_observation_delay_seconds'],'2.0')
        self.assertEqual(first['protection_timing_basis'],'first_fill_to_first_full_coverage_observation')
        self.assertTrue(first['protection_timing_synthetic'])
        self.now+=timedelta(seconds=2);self.book.reconcile(self.snap([buy,stop],qty='2',held='2'))
        self.assertEqual(self.book.state()['trades']['signal-1']['first_protection_observed_at'],first['first_protection_observed_at'])
        self.assertFalse(self.book.summary()['broker_execution_verified'])

    def test_partial_fill_protection_timing_records_only_observed_position_quantity(self):
        self.propose();entry=self.book.claim();self.book.report(entry['action_id'],{'status':'submitted'})
        buy=self.bound(entry,filled='1',state='partially_filled')
        self.book.reconcile(self.snap([buy],qty='1'))
        protect=self.book.claim();self.book.report(protect['action_id'],{'status':'submitted'})
        self.now+=timedelta(seconds=3)
        self.book.reconcile(self.snap([buy,self.bound(protect)],qty='1',held='1'))
        trade=self.book.state()['trades']['signal-1']
        self.assertEqual(trade['first_protection_observed_quantity'],'1')
        self.assertEqual(trade['protection_observation_delay_seconds'],'3.0')
        self.assertTrue(trade['protection_timing_synthetic'])
        self.assertFalse(self.book.summary()['broker_execution_verified'])

    def test_regular_hours_stop_after_close_does_not_establish_first_coverage_time(self):
        buy=self.entry_filled();protect=self.book.claim();self.book.report(protect['action_id'],{'status':'submitted'})
        self.now=self.now.replace(hour=20,minute=1)
        self.book.reconcile(self.snap([buy,self.bound(protect)],qty='2',held='2'))
        trade=self.book.state()['trades']['signal-1']
        self.assertEqual(trade['phase'],'OUTSIDE_PROTECTION_SESSION')
        self.assertNotIn('first_protection_observed_at',trade)
    def test_pending_approval_does_not_protect(self):
        buy=self.entry_filled();a=self.book.claim();self.book.report(a['action_id'],{'status':'pending_approval'})
        self.book.reconcile(self.snap([buy],qty='2'))
        self.assertEqual(self.book.state()['trades']['signal-1']['phase'],'NEEDS_PROTECTION')
        self.assertIsNone(self.book.claim())
    def test_partial_entry_queues_protection_and_cancel_remainder(self):
        self.propose();a=self.book.claim();self.book.report(a['action_id'],{'status':'submitted'})
        raw=self.bound(a,filled='1',state='partially_filled')
        self.book.reconcile(self.snap([raw],qty='1'))
        pending=[json.loads(x['payload']) for x in self.book.actions() if x['state']=='READY']
        self.assertEqual({x['kind'] for x in pending},{'cancel','protect'})
        self.assertEqual(next(x for x in pending if x['kind']=='protect')['parameters']['quantity'],'1')
    def test_partial_fill_completion_supersedes_undispatched_protection(self):
        self.propose();entry=self.book.claim();self.book.report(entry['action_id'],{'status':'submitted'})
        partial=self.bound(entry,filled='1',state='partially_filled')
        self.book.reconcile(self.snap([partial],qty='1'))
        original=next(a for a in self.book.actions() if a['kind']=='protect')
        original_ref=json.loads(original['payload'])['ref_id']
        completed=self.bound(entry,filled='2',state='filled')
        completed['executions']=[*partial['executions'],{**partial['executions'][0],'id':'second-fill'}]
        self.book.reconcile(self.snap([completed],qty='2'))
        protection=self.book.claim()
        self.assertEqual(protection['kind'],'protect')
        self.assertEqual(protection['parameters']['quantity'],'2')
        self.assertNotEqual(protection['ref_id'],original_ref)
        self.assertEqual(next(a for a in self.book.actions() if a['id']==original['id'])['state'],'SUPERSEDED')
    def test_unknown_protection_keeps_original_ref_and_blocks_new_dispatch(self):
        buy=self.entry_filled();protection=self.book.claim()
        self.book.report(protection['action_id'],{'status':'unknown'})
        self.book.reconcile(self.snap([buy],qty='2'))
        self.assertIsNone(self.book.claim())
        actions=[a for a in self.book.actions() if a['kind']=='protect']
        self.assertEqual(len(actions),1)
        self.assertEqual(json.loads(actions[0]['payload'])['ref_id'],protection['ref_id'])
    def test_rejected_protection_escalates_to_exit_without_retry(self):
        buy=self.entry_filled();protection=self.book.claim()
        self.book.report(protection['action_id'],{'status':'rejected','message':'quantity changed before review'})
        self.book.reconcile(self.snap([buy],qty='2'))
        self.assertTrue(self.book.state()['trades']['signal-1']['exit_requested'])
        self.assertIn('PROTECTION_REJECTED:DEMO',self.book.state()['issues'])
        action=self.book.claim();self.assertEqual(action['kind'],'exit')
        self.assertEqual(len([a for a in self.book.actions() if a['kind']=='protect']),1)
    def test_close_without_quote_requests_exit_and_supersedes_unsubmitted_stop(self):
        buy=self.entry_filled();self.now=self.now.replace(hour=19,minute=56)
        self.book.reconcile(self.snap([buy],qty='2'))
        action=self.book.claim();self.assertEqual(action['kind'],'exit')
        self.assertEqual(action['reason'],'INTRADAY_CLOSE_POLICY')
    def test_disabled_entries_do_not_dispatch_queued_entry(self):
        self.propose()
        self.assertIsNone(self.book.claim(allow_entries=False))
        self.assertEqual(self.book.actions()[0]['state'],'READY')
        self.assertEqual(self.book.claim()['kind'],'entry')
    def test_risk_lock_blocks_already_queued_entry_at_dispatch_boundary(self):
        self.propose();snapshot=self.snap()
        snapshot['portfolio'].update(total_value='490',cash='490')
        self.book.reconcile(snapshot)
        self.assertTrue(self.book.risk_guard.state()['blocked'])
        self.assertIsNone(self.book.claim())
        self.assertEqual(self.book.actions()[0]['state'],'READY')
        self.assertEqual(self.book.audit.verify()['events_verified'],0)
    def test_risk_lock_immediately_requests_cancel_of_unfilled_confirmed_entry(self):
        self.propose();entry=self.book.claim();self.book.report(entry['action_id'],{'status':'submitted'})
        buy=self.bound(entry,state='confirmed');buy['created_at']=self.now.isoformat()
        snapshot=self.snap([buy]);snapshot['portfolio'].update(total_value='490',cash='490')
        self.book.reconcile(snapshot)
        cancel=self.book.claim();self.assertIsNotNone(cancel)
        self.assertEqual(cancel['kind'],'cancel')
        self.assertEqual(cancel['reason'],'ACCOUNT_RISK_LOCK_CANCEL_ENTRY')
        self.assertEqual(cancel['target_order_id'],buy['id'])
        self.book.report(cancel['action_id'],{'status':'needs_confirmation'})
        self.book.reconcile(snapshot)
        self.assertIsNone(self.book.claim())
        self.assertEqual(len([a for a in self.book.actions() if a['kind']=='entry']),1)
        self.assertEqual(len([a for a in self.book.actions() if a['kind']=='cancel']),1)
        self.assertFalse(any(a['kind'] in {'protect','exit'} for a in self.book.actions()))
    def test_exit_waits_for_partial_entry_remainder_to_be_terminal(self):
        self.propose();entry=self.book.claim();self.book.report(entry['action_id'],{'status':'submitted'})
        buy=self.bound(entry,filled='1',state='partially_filled')
        quote={**self.quote,'bid':'38.99','ask':'39'}
        self.book.reconcile(self.snap([buy],qty='1'),quotes={'DEMO':quote})
        self.assertFalse(any(a['kind']=='exit' for a in self.book.actions()))
        cancel=self.book.claim();self.assertEqual(cancel['kind'],'cancel')
        self.book.report(cancel['action_id'],{'status':'cancel_requested'})
        buy['state']='partially_filled_rest_cancelled'
        self.book.reconcile(self.snap([buy],qty='1'),quotes={'DEMO':quote})
        self.assertEqual(self.book.claim()['kind'],'exit')
    def test_exit_waits_for_cancel_terminal_and_rechecks_actual_quantity(self):
        buy=self.entry_filled();a=self.book.claim();self.book.report(a['action_id'],{'status':'submitted'});stop=self.bound(a)
        quote={**self.quote,'bid':'38.99','ask':'39'}
        self.book.reconcile(self.snap([buy,stop],qty='2',held='2'),quotes={'DEMO':quote})
        cancel=self.book.claim();self.assertEqual(cancel['kind'],'cancel')
        self.book.report(cancel['action_id'],{'status':'cancel_requested'})
        self.book.reconcile(self.snap([buy,stop],qty='2',held='2'),quotes={'DEMO':quote})
        self.assertFalse(any(a['kind']=='exit' for a in self.book.actions()))
        stop['state']='cancelled'
        self.book.reconcile(self.snap([buy,stop],qty='2'),quotes={'DEMO':quote})
        exit_action=self.book.claim();self.assertEqual(exit_action['kind'],'exit')
        self.assertEqual(exit_action['parameters']['quantity'],'2')
    def test_stop_fill_racing_cancel_never_sends_second_sell(self):
        buy=self.entry_filled();a=self.book.claim();self.book.report(a['action_id'],{'status':'submitted'});stop=self.bound(a)
        quote={**self.quote,'bid':'38.99','ask':'39'}
        self.book.reconcile(self.snap([buy,stop],qty='2',held='2'),quotes={'DEMO':quote})
        cancel=self.book.claim();self.book.report(cancel['action_id'],{'status':'needs_confirmation'})
        stop=self.bound(a,filled='2',state='filled')
        self.book.reconcile(self.snap([buy,stop]))
        self.assertEqual(self.book.state()['trades']['signal-1']['phase'],'FLAT_VERIFIED')
        self.assertFalse(any(a['kind']=='exit' for a in self.book.actions()))
    def test_foreign_position_blocks_entries(self):
        self.book.reconcile(self.snap(qty='1'))
        self.assertTrue(self.book.state()['issues'])
        with self.assertRaisesRegex(ValueError,'ACTIVE_TRADE_OR_ISSUE'):self.propose()
    def test_source_mode_and_wrong_account_rejected(self):
        for patch in ({'synthetic':False},{'account_mask':'••••9999'}):
            with self.assertRaises(ValueError):self.book.reconcile({**self.snap(),**patch})
    def test_wrong_broker_ref_terms_cannot_verify_protection(self):
        buy=self.entry_filled();a=self.book.claim();stop=self.bound(a);stop['quantity']='1'
        with self.assertRaisesRegex(ValueError,'MATCH_INTENT'):self.book.reconcile(self.snap([buy,stop],qty='2',held='1'))
    def test_headless_agent_must_respect_native_confirmation_contract(self):
        self.propose();a=self.book.claim();prompt=execution_prompt(a,review_only=False)
        self.assertIn('specific user conversation confirmation is mandatory',prompt)
        self.assertIn('ENTRY IS REVIEW-ONLY',prompt)
        self.assertIn('do NOT place',prompt)
    def test_actual_exit_fill_completes_round_trip(self):
        buy=self.entry_filled();a=self.book.claim();self.book.report(a['action_id'],{'status':'submitted'});stop=self.bound(a)
        quote={**self.quote,'bid':'38.99','ask':'39'}
        self.book.reconcile(self.snap([buy,stop],qty='2',held='2'),quotes={'DEMO':quote})
        cancel=self.book.claim();self.book.report(cancel['action_id'],{'status':'cancel_requested'});stop['state']='cancelled'
        self.book.reconcile(self.snap([buy,stop],qty='2'),quotes={'DEMO':quote})
        out=self.book.claim();self.book.report(out['action_id'],{'status':'submitted'})
        sell=self.bound(out,filled='2',state='filled')
        self.book.reconcile(self.snap([buy,stop,sell]))
        self.assertEqual(self.book.state()['trades']['signal-1']['phase'],'FLAT_VERIFIED')
    def test_unknown_submit_recovered_only_from_matching_broker_order(self):
        self.propose();a=self.book.claim();self.book.report(a['action_id'],{'status':'unknown'})
        self.book.reconcile(self.snap([self.bound(a,filled='2',state='filled')],qty='2'))
        self.assertEqual(self.book.actions()[0]['state'],'OBSERVED')
        self.assertEqual(self.book.audit.report('2026-10-16')['unresolved_agent_intents'],[])
        self.assertEqual(self.book.claim()['kind'],'protect')
    def test_unknown_cancel_resolves_audit_only_after_target_terminal(self):
        buy=self.entry_filled();protect=self.book.claim();self.book.report(protect['action_id'],{'status':'submitted'})
        stop=self.bound(protect)
        quote={**self.quote,'bid':'38.99','ask':'39'}
        self.book.reconcile(self.snap([buy,stop],qty='2',held='2'),quotes={'DEMO':quote})
        cancel=self.book.claim();self.book.report(cancel['action_id'],{'status':'unknown'})
        self.book.reconcile(self.snap([buy,stop],qty='2',held='2'),quotes={'DEMO':quote})
        self.assertIsNone(self.book.claim())
        stop['state']='cancelled'
        self.book.reconcile(self.snap([buy,stop],qty='2'),quotes={'DEMO':quote})
        self.assertEqual(self.book.audit.report('2026-10-16')['unresolved_agent_intents'],[])
        self.assertEqual(self.book.claim()['kind'],'exit')
    def test_profit_signal_defers_tightening_without_cancelling_effective_stop(self):
        buy=self.entry_filled();a=self.book.claim();self.book.report(a['action_id'],{'status':'submitted'});stop=self.bound(a)
        quote={**self.quote,'bid':'43','ask':'43.01'}
        self.book.reconcile(self.snap([buy,stop],qty='2',held='2'),quotes={'DEMO':quote})
        self.assertIsNone(self.book.claim())
        trade=self.book.state()['trades']['signal-1']
        self.assertEqual(trade['phase'],'PROTECTED')
        self.assertEqual(trade['stop'],'39');self.assertEqual(trade['suggested_stop'],'40')
        self.assertNotIn('unprotected_since',trade)
        self.assertEqual(trade['stop_adjustment']['deferred_reason'],'SAFE_NATIVE_REPLACEMENT_NOT_VERIFIED')
        self.now+=timedelta(seconds=31)
        quote={**quote,**{key:self.now.isoformat() for key in ('bid_at','ask_at','observed_at')}}
        self.book.reconcile(self.snap([buy,stop],qty='2',held='2'),quotes={'DEMO':quote})
        self.assertNotIn('PROTECTION_WAIT_EXCEEDED:DEMO',self.book.state()['issues'])
        self.assertIsNone(self.book.claim())
        events=[json.loads(r[0]) for r in self.book.db.execute("SELECT payload FROM audit_events WHERE json_extract(payload,'$.event_type')='SHADOW_TRANSITION'")]
        self.assertEqual(sum(e.get('details',{}).get('deferred_reason')=='SAFE_NATIVE_REPLACEMENT_NOT_VERIFIED' for e in events),1)
    def test_protection_wait_exceeded_is_persistently_visible(self):
        buy=self.entry_filled();a=self.book.claim();self.book.report(a['action_id'],{'status':'needs_confirmation'})
        self.now+=timedelta(seconds=31);self.book.reconcile(self.snap([buy],qty='2'))
        self.assertIn('PROTECTION_WAIT_EXCEEDED:DEMO',self.book.state()['issues'])
        self.assertTrue(self.book.state()['trades']['signal-1']['exit_requested'])
        self.assertEqual(self.book.claim()['kind'],'exit')

    def test_first_fill_observed_after_ninety_seconds_does_not_restart_protection_budget(self):
        self.propose();entry=self.book.claim();self.book.report(entry['action_id'],{'status':'submitted'})
        buy=self.bound(entry,filled='2',state='filled')
        self.now+=timedelta(seconds=90)
        self.book.reconcile(self.snap([buy],qty='2'))
        trade=self.book.state()['trades']['signal-1']
        self.assertEqual(trade['unprotected_since'],AT)
        self.assertEqual(trade['unprotected_timing_basis'],'first_actual_entry_execution')
        self.assertEqual(trade['unprotected_detection_delay_seconds'],'90.0')
        self.assertTrue(trade['unprotected_timing_synthetic'])
        self.assertTrue(trade['exit_requested'])
        self.assertIn('PROTECTION_WAIT_EXCEEDED:DEMO',self.book.state()['issues'])
        self.assertEqual(self.book.claim()['kind'],'exit')

    def test_unprotected_deadline_is_thirty_seconds_without_an_extra_poll_grace(self):
        buy=self.entry_filled();self.now+=timedelta(seconds=30)
        self.book.reconcile(self.snap([buy],qty='2'))
        self.assertTrue(self.book.state()['trades']['signal-1']['exit_requested'])
        self.assertEqual(self.book.claim()['kind'],'exit')

    def test_late_partial_fill_observation_uses_actual_new_execution_when_old_shares_covered(self):
        self.propose();entry=self.book.claim();self.book.report(entry['action_id'],{'status':'submitted'})
        buy=self.bound(entry,filled='1',state='partially_filled')
        self.book.reconcile(self.snap([buy],qty='1'))
        protection=self.book.claim();self.book.report(protection['action_id'],{'status':'submitted'})
        stop=self.bound(protection)
        self.now+=timedelta(seconds=1)
        self.book.reconcile(self.snap([buy,stop],qty='1',held='1'))
        coverage_at=self.now.isoformat()
        buy['state']='filled';buy['cumulative_quantity']='2'
        second=deepcopy(buy['executions'][0]);second['id']+='-second'
        second['timestamp']=(datetime.fromisoformat(AT)+timedelta(seconds=10)).isoformat()
        buy['executions'].append(second)
        self.now=datetime.fromisoformat(AT)+timedelta(seconds=45)
        self.book.reconcile(self.snap([buy,stop],qty='2',held='1'))
        trade=self.book.state()['trades']['signal-1']
        self.assertEqual(trade['unprotected_since'],second['timestamp'])
        self.assertEqual(trade['unprotected_new_entry_execution_at'],second['timestamp'])
        self.assertEqual(trade['unprotected_timing_basis'],'additional_actual_entry_execution_old_tranche_still_covered')
        self.assertEqual(trade['unprotected_detection_delay_seconds'],'35.0')
        self.assertTrue(trade['exit_requested'])
        self.assertIn('PROTECTION_WAIT_EXCEEDED:DEMO',self.book.state()['issues'])

    def test_new_fill_does_not_time_out_using_old_still_valid_protection_age(self):
        self.propose();entry=self.book.claim();self.book.report(entry['action_id'],{'status':'submitted'})
        buy=self.bound(entry,filled='1',state='partially_filled')
        self.book.reconcile(self.snap([buy],qty='1'))
        protection=self.book.claim();self.book.report(protection['action_id'],{'status':'submitted'})
        stop=self.bound(protection)
        self.now+=timedelta(seconds=1)
        self.book.reconcile(self.snap([buy,stop],qty='1',held='1'))
        buy['state']='filled';buy['cumulative_quantity']='2'
        second=deepcopy(buy['executions'][0]);second['id']+='-second'
        second['timestamp']=(datetime.fromisoformat(AT)+timedelta(seconds=94)).isoformat()
        buy['executions'].append(second)
        self.now=datetime.fromisoformat(AT)+timedelta(seconds=95)
        self.book.reconcile(self.snap([buy,stop],qty='2',held='1'))
        trade=self.book.state()['trades']['signal-1']
        self.assertEqual(trade['unprotected_since'],second['timestamp'])
        self.assertEqual(trade['unprotected_detection_delay_seconds'],'1.0')
        self.assertFalse(trade['exit_requested'])
        self.assertNotIn('PROTECTION_WAIT_EXCEEDED:DEMO',self.book.state()['issues'])
        self.now+=timedelta(seconds=30)
        self.book.reconcile(self.snap([buy,stop],qty='2',held='1'))
        trade=self.book.state()['trades']['signal-1']
        self.assertEqual(trade['unprotected_since'],second['timestamp'])
        self.assertTrue(trade['exit_requested'])

    def test_added_shares_with_old_protection_also_lost_use_last_coverage_bound(self):
        self.propose();entry=self.book.claim();self.book.report(entry['action_id'],{'status':'submitted'})
        buy=self.bound(entry,filled='1',state='partially_filled')
        self.book.reconcile(self.snap([buy],qty='1'))
        protection=self.book.claim();self.book.report(protection['action_id'],{'status':'submitted'})
        stop=self.bound(protection)
        self.now+=timedelta(seconds=1)
        self.book.reconcile(self.snap([buy,stop],qty='1',held='1'))
        coverage_at=self.now.isoformat()
        buy['state']='filled';buy['cumulative_quantity']='2'
        second=deepcopy(buy['executions'][0]);second['id']+='-second'
        second['timestamp']=(datetime.fromisoformat(AT)+timedelta(seconds=94)).isoformat()
        buy['executions'].append(second);stop['state']='cancelled'
        self.now=datetime.fromisoformat(AT)+timedelta(seconds=95)
        self.book.reconcile(self.snap([buy,stop],qty='2'))
        trade=self.book.state()['trades']['signal-1']
        self.assertEqual(trade['unprotected_since'],coverage_at)
        self.assertEqual(trade['unprotected_timing_basis'],'last_full_coverage_observation_conservative_bound')
        self.assertEqual(trade['unprotected_detection_delay_seconds'],'94.0')
        self.assertTrue(trade['exit_requested'])

    def test_late_reprotection_observation_retains_last_known_coverage_time(self):
        buy=self.entry_filled();protection=self.book.claim()
        self.book.report(protection['action_id'],{'status':'submitted'})
        stop=self.bound(protection)
        self.now+=timedelta(seconds=5)
        self.book.reconcile(self.snap([buy,stop],qty='2',held='2'))
        coverage_at=self.now.isoformat()
        stop['state']='cancelled'
        self.now+=timedelta(seconds=90)
        self.book.reconcile(self.snap([buy,stop],qty='2'))
        trade=self.book.state()['trades']['signal-1']
        self.assertEqual(trade['unprotected_since'],coverage_at)
        self.assertEqual(trade['unprotected_timing_basis'],'last_full_coverage_observation_conservative_bound')
        self.assertEqual(trade['unprotected_detection_delay_seconds'],'90.0')
        self.assertEqual(trade['last_full_coverage_observed_quantity'],'2')
        self.assertTrue(trade['exit_requested'])
        self.assertIn('PROTECTION_WAIT_EXCEEDED:DEMO',self.book.state()['issues'])
    def test_active_exit_is_not_cancelled_on_next_poll(self):
        buy=self.entry_filled();a=self.book.claim();self.book.report(a['action_id'],{'status':'submitted'});stop=self.bound(a)
        quote={**self.quote,'bid':'38.99','ask':'39'}
        self.book.reconcile(self.snap([buy,stop],qty='2',held='2'),quotes={'DEMO':quote})
        cancel=self.book.claim();self.book.report(cancel['action_id'],{'status':'cancel_requested'});stop['state']='cancelled'
        self.book.reconcile(self.snap([buy,stop],qty='2'),quotes={'DEMO':quote})
        out=self.book.claim();self.book.report(out['action_id'],{'status':'submitted'});sell=self.bound(out)
        self.book.reconcile(self.snap([buy,stop,sell],qty='2',held='2'),quotes={'DEMO':quote})
        self.assertFalse(any(x['kind']=='cancel' and json.loads(x['payload'])['target_order_id']==sell['id'] for x in self.book.actions()))
    def test_unfilled_entry_timeout_requests_cancel(self):
        self.propose();a=self.book.claim();self.book.report(a['action_id'],{'status':'submitted'})
        raw=self.bound(a,state='confirmed')
        self.now+=timedelta(seconds=61)
        self.book.reconcile(self.snap([raw]))
        pending=self.book.claim()
        self.assertIsNotNone(pending)
        self.assertEqual(pending['kind'],'cancel')
        self.assertEqual(pending['target_order_id'],raw['id'])

    def test_expired_undispatched_stop_exits_after_protection_deadline(self):
        buy=self.entry_filled()
        self.now+=timedelta(seconds=121)
        self.assertIsNone(self.book.claim())
        expired=next(a for a in self.book.actions() if a['kind']=='protect')
        old_ref=json.loads(expired['payload'])['ref_id']
        self.book.reconcile(self.snap([buy],qty='2'))
        refreshed=self.book.claim()
        self.assertIsNotNone(refreshed)
        self.assertEqual(refreshed['kind'],'exit')
        self.assertEqual(next(a for a in self.book.actions() if a['kind']=='protect')['state'],'SUPERSEDED')
        self.assertEqual(json.loads(next(a for a in self.book.actions() if a['kind']=='protect')['payload'])['ref_id'],old_ref)
        self.assertEqual(len([a for a in self.book.actions() if a['kind']=='protect']),1)

    def test_rejected_stop_is_visible_without_unbounded_retry(self):
        buy=self.entry_filled();a=self.book.claim();self.book.report(a['action_id'],{'status':'submitted'})
        stop=self.bound(a,state='rejected')
        self.book.reconcile(self.snap([buy,stop],qty='2'))
        self.assertIn('PROTECTION_TERMINATED_UNCOVERED:DEMO',self.book.state()['issues'])
        self.assertEqual(self.book.claim()['kind'],'exit')
        self.assertEqual(len([a for a in self.book.actions() if a['kind']=='protect']),1)

    def test_worker_refuses_synthetic_action_before_process(self):
        self.propose();a=self.book.claim()
        worker=CodexOrderWorker('.',self.tmp.name,runner=lambda cmd,prompt:self.fail('synthetic action'))
        with self.assertRaisesRegex(ValueError,'SYNTHETIC'):worker(a)


    def _unknown_pending(self, *, phase=None, orphan=False, malformed=False):
        self.propose()
        action = self.book.actions()[0]
        self.book.claim()
        self.book.report(action['id'], {'status': 'unknown'})
        state = self.book.state()
        if phase is not None:
            state['trades']['signal-1']['phase'] = phase
        if orphan:
            state['trades'] = {}
        self.book._save(state)
        if malformed:
            self.book.db.execute(
                "UPDATE live_actions SET payload='{' WHERE id=?", (action['id'],))
        self.book.db.commit()
        return action

    def test_unknown_or_orphan_unresolved_actions_fail_closed(self):
        cases = ('flat_unknown_same_symbol', 'orphan_unknown',
                 'different_symbol_unknown', 'malformed_payload',
                 'terminal_trade_unresolved')
        for case in cases:
            with self.subTest(case=case):
                self.setUp()
                try:
                    if case == 'flat_unknown_same_symbol':
                        self._unknown_pending(phase='FLAT_VERIFIED')
                        plan, quote = self.plan, self.quote
                    elif case == 'orphan_unknown':
                        self._unknown_pending(orphan=True)
                        plan = {**self.plan, 'symbol': 'SPY'}
                        quote = {**self.quote, 'symbol': 'SPY'}
                    elif case == 'different_symbol_unknown':
                        self._unknown_pending(phase='ENTRY_PLANNED')
                        plan = {**self.plan, 'symbol': 'SPY'}
                        quote = {**self.quote, 'symbol': 'SPY'}
                    elif case == 'malformed_payload':
                        self._unknown_pending(malformed=True)
                        plan, quote = self.plan, self.quote
                    else:
                        self._unknown_pending(phase='FLAT_VERIFIED')
                        plan, quote = self.plan, self.quote
                    before = len(self.book.actions())
                    with self.assertRaisesRegex(
                            ValueError, 'UNRESOLVED_ACTION_BLOCKS_ENTRY'):
                        self.book.propose(
                            'signal-2', plan, quote, self.snap(),
                            evidence={'source_sha256': 'fixture', 'synthetic': True})
                    self.assertEqual(len(self.book.actions()), before)
                    self.assertFalse(self.book.db.in_transaction)
                finally:
                    self.tearDown()

    def test_known_different_symbol_reported_action_allows_new_proposal(self):
        self.propose()
        action = self.book.actions()[0]
        self.book.claim()
        self.book.report(action['id'], {'status': 'submitted'})
        plan = {**self.plan, 'symbol': 'SPY'}
        quote = {**self.quote, 'symbol': 'SPY'}
        result = self.book.propose(
            'signal-2', plan, quote, self.snap(),
            evidence={'source_sha256': 'fixture', 'synthetic': True})
        self.assertFalse(result['duplicate'])
        self.assertEqual(len(self.book.actions()), 2)

    def test_same_symbol_active_action_still_rejected(self):
        self.propose()
        action = self.book.actions()[0]
        self.book.claim()
        self.book.report(action['id'], {'status': 'submitted'})
        with self.assertRaisesRegex(
                ValueError, 'EXECUTION_HAS_ACTIVE_TRADE_OR_ISSUE|'
                'UNRESOLVED_ACTION_BLOCKS_ENTRY'):
            self.book.propose(
                'signal-2', self.plan, self.quote, self.snap(),
                evidence={'source_sha256': 'fixture', 'synthetic': True})
        self.assertEqual(len(self.book.actions()), 1)



class LiveSignalTests(TestCase):
    def test_complete_strategy_events_create_durable_signal_without_paper_orders(self):
        with tempfile.TemporaryDirectory() as directory:
            policy=json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
            now=[datetime.fromisoformat('2026-10-15T09:00:00-04:00')]
            b=ExecutionBook(Path(directory)/'signals.sqlite',policy,simulation=True,clock=lambda:now[0])
            try:
                signals=LiveSignals(b)
                for e in shadow_events()[1:5]:
                    now[0]=datetime.fromisoformat(e['at']);e={**e,'synthetic_input':True,'source_sha256':'fixture-'+e['id']}
                    signals.consume(e)
                pending=signals.pending();self.assertEqual(len(pending),1)
                self.assertEqual(pending[0]['plan']['symbol'],'DEMO')
                self.assertFalse(b.actions())
                self.assertIsNone(b.db.execute("SELECT name FROM sqlite_master WHERE name='paper_orders'").fetchone())
                self.assertEqual(signals.pending()[0]['id'],pending[0]['id'])
            finally:b.close()
