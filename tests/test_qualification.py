from datetime import datetime,timedelta
import json
from pathlib import Path
import tempfile
from unittest import TestCase

from entry_authority_fixture import TestExecutionBook as ExecutionBook
from martin_agentic.qualification import release_check,source_fingerprint,protection_references


class QualificationTests(TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.now=datetime.fromisoformat('2026-10-16T14:00:00Z')
        self.policy=json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        self.book=ExecutionBook(Path(self.tmp.name)/'qualify.sqlite',self.policy,simulation=True,clock=lambda:self.now)
        self.status={'healthy':True,'running':True,'observed_at':self.now.isoformat(),
            'source_fingerprint':source_fingerprint(),'broker':{'account':'••••0000'}}
        self.evidence={'passed':True,'source_fingerprint':source_fingerprint()}
    def tearDown(self):self.book.close();self.tmp.cleanup()
    def test_green_software_cannot_be_presented_as_production(self):
        r=release_check(self.book,self.status,self.evidence)
        self.assertTrue(r['software_test_evidence_current'])
        self.assertFalse(r['production_ready']);self.assertFalse(r['supervised_live_smoke_passed'])
        self.assertIn('NO_REAL_PROTECTION_ACCEPTANCE_VALIDATED',r['blockers'])
    def test_changing_code_invalidates_prior_test_and_running_evidence(self):
        self.evidence['source_fingerprint']='old';self.status['source_fingerprint']='old'
        r=release_check(self.book,self.status,self.evidence)
        self.assertIn('CURRENT_SOURCE_TEST_EVIDENCE_MISSING',r['blockers'])
        self.assertIn('RUNNING_PROCESS_BUILD_NOT_VERIFIED',r['blockers'])
    def test_local_production_flag_cannot_waive_formal_confirmation(self):
        self.evidence['production_ready']=True;self.evidence['native_confirmation_waived']=True
        r=release_check(self.book,self.status,self.evidence)
        self.assertFalse(r['production_ready'])
        self.assertIn('NATIVE_SPECIFIC_ORDER_CONFIRMATION_STILL_REQUIRED',r['blockers'])
    def test_old_account_snapshot_does_not_pass_release(self):
        self.status['observed_at']='2026-10-16T13:00:00Z'
        r=release_check(self.book,self.status,self.evidence)
        self.assertIn('BROKER_OBSERVATION_NOT_CURRENT',r['blockers'])

    def test_configuration_dependencies_web_and_worker_change_release_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            prior=source_fingerprint(root)
            for name in ['config/project.json','uv.lock','infra/src/martin_agentic/web/app.js',
                         'strategy/martin_luk/src/martin_luk/strategy.py','tests/test_x.py']:
                p=root/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_text('changed')
                new=source_fingerprint(root);self.assertNotEqual(prior,new);prior=new
            p=root/'runtime/noise.json';p.parent.mkdir();p.write_text('temporary')
            self.assertEqual(prior,source_fingerprint(root))

    def test_protection_reference_index_preserves_explicit_approval_alias(self):
        # Pure reference resolution test; this remains a synthetic book, not real evidence.
        self.book.db.execute('INSERT INTO live_actions VALUES(?,?,?,?,?,?,?,NULL)',
            ('protect-action','trade','protect','REPORTED',json.dumps({'ref_id':'logical-ref'}),self.now.isoformat(),self.now.isoformat()))
        self.book.db.execute('INSERT INTO external_broker_refs VALUES(?,?,?)',('explicit-approval-id','protect-action','native-operation'))
        self.book.db.commit()
        refs=protection_references(self.book)
        self.assertEqual(set(refs),{'logical-ref','explicit-approval-id'})
        self.assertTrue(self.book.simulation)
        self.assertFalse(release_check(self.book,self.status,self.evidence)['actual_protection_acceptance_validated'])

class LiveLifecycleEvidenceTests(TestCase):
    """Fabricated normalized observations exercise verifier logic, never live trades."""
    def setUp(self):
        from copy import deepcopy
        self.copy=deepcopy;self.tmp=tempfile.TemporaryDirectory()
        self.now=datetime.fromisoformat('2026-10-16T14:01:00Z')
        self.policy=json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        self.book=ExecutionBook(Path(self.tmp.name)/'live.sqlite',self.policy,clock=lambda:self.now)
        self.book.db.execute('CREATE TABLE broker_observations(digest TEXT PRIMARY KEY,payload TEXT NOT NULL)')
        self.base='2026-10-16T14:00:'
        self.snapshots=[]
        self.add_trade('one')

    def tearDown(self):self.book.close();self.tmp.cleanup()

    def add_trade(self,trade_id):
        from test_broker import snapshot
        state=self.book.state();state['account_key']=snapshot()['account_key']
        state['trades'][trade_id]={'id':trade_id,'plan':{'symbol':'DEMO'},'phase':'FLAT_VERIFIED','entry_price':'40'}
        self.book._save(state)
        for kind,side,typ in [('entry','buy','market'),('protect','sell','stop_market'),('exit','sell','market')]:
            params={'symbol':'DEMO','side':side,'type':typ,'quantity':'1','market_hours':'regular_hours','time_in_force':'gfd'}
            if kind=='protect':params['stop_price']='39'
            payload={'ref_id':trade_id+'-'+kind,'parameters':params}
            self.book.db.execute('INSERT INTO live_actions VALUES(?,?,?,?,?,?,?,NULL)',
                (trade_id+'-'+kind,trade_id,kind,'OBSERVED',json.dumps(payload),self.now.isoformat(),self.now.isoformat()))
        self.book.db.commit()

    def snap(self,trade_id,stage):
        from test_broker import FakeBroker,snapshot,order,position
        fake=FakeBroker()
        entry=order(trade_id+'-entry');entry['ref_id']=trade_id+'-entry'
        fake.orders=[entry]
        if stage!='entry':
            stop=order(trade_id+'-protect','sell','1','0','confirmed','39');stop['ref_id']=trade_id+'-protect'
            fake.orders.append(stop)
        if stage=='flat':
            fake.orders[-1]['state']='cancelled'
            exit_order=order(trade_id+'-exit','sell');exit_order['ref_id']=trade_id+'-exit'
            exit_order['executions'][0]['timestamp']=self.base+'03Z';fake.orders.append(exit_order)
        else:fake.positions=[position('1')]
        second={'entry':'00','protected':'01','flat':'04'}[stage]
        return snapshot(fake,self.base+second+'Z')

    def store(self,*snaps):
        for snap in snaps:
            self.snapshots.append(snap)
            self.book.db.execute('INSERT INTO broker_observations VALUES(?,?)',(str(len(self.snapshots)),json.dumps(snap)))
        self.book.db.commit()

    def check(self):
        fingerprint=source_fingerprint()
        return release_check(self.book,{'healthy':True,'running':True,'observed_at':self.now.isoformat(),
            'source_fingerprint':fingerprint,'broker':{'account':'••••0000'}},
            {'passed':True,'source_fingerprint':fingerprint})

    def test_same_trade_complete_ordered_broker_chain_is_smoke_only(self):
        self.store(self.snap('one','entry'),self.snap('one','protected'),self.snap('one','flat'))
        report=self.check()
        self.assertTrue(report['supervised_live_smoke_passed'])
        self.assertFalse(report['production_ready'])
        self.assertEqual(report['live_lifecycle_evidence'][0]['entry_execution_ids'],['one-entry-exec'])
        self.assertEqual(report['live_lifecycle_evidence'][0]['exit_execution_ids'],['one-exit-exec'])
        self.assertIn('NATIVE_SPECIFIC_ORDER_CONFIRMATION_STILL_REQUIRED',report['blockers'])

    def test_protection_of_different_trade_cannot_complete_another_round_trip(self):
        self.add_trade('two')
        self.store(self.snap('one','entry'),self.snap('two','protected'),self.snap('one','flat'))
        result=self.check()
        self.assertTrue(result['actual_entry_fill_validated'])
        self.assertTrue(result['actual_protection_acceptance_validated'])
        self.assertTrue(result['actual_exit_flat_validated'])
        self.assertFalse(result['supervised_live_smoke_passed'])

    def test_rewritten_refs_need_exact_registered_order_ids_for_each_stage(self):
        snaps=[self.snap('one',stage) for stage in ('entry','protected','flat')]
        for snap in snaps:
            for order in snap['orders'].values():order['ref_id']='rewritten-'+order['id']
        self.store(*snaps)
        self.assertFalse(self.check()['actual_entry_fill_validated'])
        for kind in ('entry','protect','exit'):
            self.book.db.execute('INSERT INTO external_broker_order_ids VALUES(?,?,?,?)',
                ('one-'+kind,'one-'+kind,'receipt-'+kind,self.book.state()['account_key']))
        self.book.db.commit()
        self.assertTrue(self.check()['supervised_live_smoke_passed'])
        self.book.db.execute("UPDATE external_broker_order_ids SET account_key='wrong-account'")
        self.book.db.commit()
        self.assertFalse(self.check()['supervised_live_smoke_passed'])

    def test_cached_trade_entry_price_and_flat_phase_do_not_prove_any_execution(self):
        result=self.check()
        self.assertFalse(result['actual_entry_fill_validated'])
        self.assertFalse(result['actual_exit_flat_validated'])

    def test_synthetic_wrong_account_incomplete_or_future_protection_is_rejected(self):
        for mutation in ({'synthetic':True},{'account_key':'other'},{'account_mask':'••••9999'},
                         {'collection_complete':False},{'observed_at':'2026-10-16T15:00:00Z'}):
            with self.subTest(mutation=mutation):
                self.book.db.execute('DELETE FROM broker_observations');self.book.db.commit()
                stop=self.snap('one','protected');stop.update(mutation)
                self.store(self.snap('one','entry'),stop,self.snap('one','flat'))
                self.assertFalse(self.check()['supervised_live_smoke_passed'])

    def test_cancelled_unfilled_entry_and_zero_position_is_not_exit_evidence(self):
        snap=self.snap('one','entry');snap['positions']={}
        entry=next(iter(snap['orders'].values()));entry.update(state='cancelled',filled='0',remaining='1',executions=[])
        self.store(snap)
        result=self.check()
        self.assertFalse(result['actual_entry_fill_validated']);self.assertFalse(result['actual_exit_flat_validated'])

    def test_broker_fill_details_and_terminal_flat_account_are_required(self):
        for mutation in ['missing_execution','active_exit','residual_position','pending_approval','other_active_order']:
            with self.subTest(mutation=mutation):
                self.book.db.execute('DELETE FROM broker_observations');self.book.db.commit()
                final=self.snap('one','flat')
                if mutation=='missing_execution':final['orders']['one-exit']['executions']=[]
                if mutation=='active_exit':final['orders']['one-exit']['state']='pending_cancelled'
                if mutation=='residual_position':final['positions']={'OTHER':{'quantity':'1'}}
                if mutation=='pending_approval':final['pending_approvals']=1
                if mutation=='other_active_order':
                    final['orders']['unrelated']=self.copy(final['orders']['one-protect']);final['orders']['unrelated']['state']='confirmed'
                self.store(self.snap('one','entry'),self.snap('one','protected'),final)
                self.assertFalse(self.check()['actual_exit_flat_validated'])
                self.assertFalse(self.check()['supervised_live_smoke_passed'])

    def test_exit_execution_before_protection_observation_cannot_complete_chain(self):
        final=self.snap('one','flat');final['orders']['one-exit']['executions'][0]['executed_at']=self.base+'00Z'
        self.store(self.snap('one','entry'),self.snap('one','protected'),final)
        self.assertTrue(self.check()['actual_exit_flat_validated'])
        self.assertFalse(self.check()['supervised_live_smoke_passed'])

    def test_unmatched_terms_and_unbound_reference_cannot_prove_protection(self):
        for key,value in [('stop','38'),('ref_id','unknown-alias')]:
            with self.subTest(key=key):
                self.book.db.execute('DELETE FROM broker_observations');self.book.db.commit()
                stop=self.snap('one','protected');stop['orders']['one-protect'][key]=value
                self.store(self.snap('one','entry'),stop,self.snap('one','flat'))
                self.assertFalse(self.check()['actual_protection_acceptance_validated'])

    def test_explicit_broker_approval_alias_still_proves_same_trade(self):
        self.book.db.execute('INSERT INTO external_broker_refs VALUES(?,?,?)',('broker-approval','one-protect','operation'))
        self.book.db.commit()
        stop=self.snap('one','protected');stop['orders']['one-protect']['ref_id']='broker-approval'
        final=self.snap('one','flat');final['orders']['one-protect']['ref_id']='broker-approval'
        self.store(self.snap('one','entry'),stop,final)
        self.assertTrue(self.check()['supervised_live_smoke_passed'])

    def test_historical_smoke_does_not_prove_current_operational_health(self):
        self.store(self.snap('one','entry'),self.snap('one','protected'),self.snap('one','flat'))
        result=self.check()
        self.assertTrue(result['supervised_live_smoke_passed'])
        self.assertFalse(result['process_identity_verified'])
        self.assertFalse(result['current_operational_healthy'])
        self.assertIn('LIVE_PROCESS_IDENTITY_NOT_VERIFIED',result['blockers'])

    def test_failed_audit_verification_blocks_evidence_and_release(self):
        from unittest.mock import patch
        self.store(self.snap('one','entry'),self.snap('one','protected'),self.snap('one','flat'))
        for result in ({'integrity_valid':False},ValueError('AUDIT_INTEGRITY_FAILED')):
            with self.subTest(result=type(result).__name__):
                kwargs={'side_effect':result} if isinstance(result,Exception) else {'return_value':result}
                with patch.object(self.book.audit,'verify',**kwargs):report=self.check()
                self.assertFalse(report['supervised_live_smoke_passed'])
                self.assertFalse(report['current_operational_healthy'])
                self.assertIn('AUDIT_INTEGRITY_NOT_VERIFIED',report['blockers'])

    def test_confirmed_stop_execution_can_supply_the_real_exit(self):
        final=self.snap('one','flat');exit_order=final['orders'].pop('one-exit')
        stop=final['orders']['one-protect'];stop.update(state='filled',filled='1',remaining='0',executions=exit_order['executions'])
        stop['executions'][0].update(id='one-stop-exec',order_id=stop['id'])
        self.store(self.snap('one','entry'),self.snap('one','protected'),final)
        result=self.check()
        self.assertTrue(result['supervised_live_smoke_passed'])
        self.assertEqual(result['live_lifecycle_evidence'][0]['exit_execution_ids'],['one-stop-exec'])

    def test_later_entry_fill_requires_full_coverage_of_final_entry_quantity(self):
        for action_id in ['one-entry','one-exit']:
            raw=self.book.db.execute('SELECT payload FROM live_actions WHERE id=?',(action_id,)).fetchone()[0]
            payload=json.loads(raw);payload['parameters']['quantity']='2'
            self.book.db.execute('UPDATE live_actions SET payload=? WHERE id=?',(json.dumps(payload),action_id))
        self.book.db.commit()
        entry=self.snap('one','entry');protected=self.snap('one','protected');final=self.snap('one','flat')
        for snap in [entry,protected,final]:
            snap['orders']['one-entry']['quantity']='2'
        for snap in [entry,protected]:snap['orders']['one-entry'].update(state='partially_filled',remaining='1')
        final_entry=final['orders']['one-entry'];final_entry.update(filled='2',remaining='0')
        later=self.copy(final_entry['executions'][0]);later.update(id='late-entry-exec',executed_at=self.base+'02Z')
        final_entry['executions'].append(later)
        final_exit=final['orders']['one-exit'];final_exit.update(quantity='2',filled='2')
        final_exit['executions'][0]['quantity']='2'
        self.store(entry,protected,final)
        result=self.check()
        self.assertTrue(result['actual_exit_flat_validated'])
        self.assertFalse(result['supervised_live_smoke_passed'])

    def test_31_second_protection_preserves_real_roundtrip_but_fails_acceptance(self):
        protected=self.snap('one','protected');protected['observed_at']=self.base+'31Z'
        final=self.snap('one','flat');final['observed_at']=self.base+'35Z'
        final['orders']['one-exit']['executions'][0]['executed_at']=self.base+'34Z'
        self.store(self.snap('one','entry'),protected,final)
        report=self.check();trade=report['live_lifecycle_evidence'][0]
        self.assertTrue(trade['complete_protected_round_trip'])
        self.assertTrue(report['actual_exit_flat_validated'])
        self.assertTrue(report['module_functional_closed_loop_validated'])
        self.assertFalse(report['supervised_live_smoke_passed'])
        self.assertFalse(report['protection_timeliness_validated'])
        self.assertEqual(trade['protection_tranches'][0]['unprotected_seconds_upper_bound'],31)
        self.assertIn('PROTECTION_30_SECOND_DEADLINE_NOT_VALIDATED',report['blockers'])

    def test_exactly_30_seconds_is_within_deadline(self):
        protected=self.snap('one','protected');protected['observed_at']=self.base+'30Z'
        final=self.snap('one','flat');final['observed_at']=self.base+'35Z'
        final['orders']['one-exit']['executions'][0]['executed_at']=self.base+'34Z'
        self.store(self.snap('one','entry'),protected,final)
        self.assertTrue(self.check()['supervised_live_smoke_passed'])

    def test_late_first_observation_does_not_restart_fill_clock(self):
        entry=self.snap('one','entry');entry['observed_at']=self.base+'29Z'
        protected=self.snap('one','protected');protected['observed_at']=self.base+'31Z'
        final=self.snap('one','flat');final['observed_at']=self.base+'35Z'
        final['orders']['one-exit']['executions'][0]['executed_at']=self.base+'34Z'
        self.store(entry,protected,final)
        tranche=self.check()['live_lifecycle_evidence'][0]['protection_tranches'][0]
        self.assertEqual(tranche['first_observed_at'],self.base+'29Z')
        self.assertEqual(tranche['unprotected_seconds_upper_bound'],31)
        self.assertFalse(tranche['within_30_seconds'])

    def test_permanent_recovery_case_manual_marker_fails_module_not_broker_facts(self):
        self.store(self.snap('one','entry'),self.snap('one','protected'),self.snap('one','flat'))
        self.book.db.execute('CREATE TABLE recovery_cases(id TEXT PRIMARY KEY,trade_id TEXT NOT NULL,status TEXT NOT NULL,payload TEXT NOT NULL)')
        self.book.db.execute('INSERT INTO recovery_cases VALUES(?,?,?,?)',
            ('case-one','one','ACKNOWLEDGED',json.dumps({'manual_takeover':True})))
        self.book.db.commit()
        report=self.check();trade=report['live_lifecycle_evidence'][0]
        self.assertTrue(trade['complete_protected_round_trip'])
        self.assertTrue(trade['protection_timeliness_passed'])
        self.assertTrue(trade['manual_takeover_detected'])
        self.assertFalse(report['module_functional_closed_loop_validated'])
        self.assertFalse(report['supervised_live_smoke_passed'])
        self.assertIn('MANUAL_TAKEOVER_REQUIRES_NEW_MODULE_ACCEPTANCE',report['blockers'])

    def test_normal_user_confirmation_audit_does_not_mean_manual_takeover(self):
        event={'event_id':'normal-review','occurred_at':self.base+'00Z',
            'event_type':'AGENT_ACTION_INTENT','actor':'agent','synthetic':False,
            'account':'••••0000','trade_id':'one','intent_id':'normal-review',
            'ref_id':'one-protect','details':{'action':'review','parameters':{}}}
        self.book.audit.append(event)
        self.book.audit.append({'event_id':'normal-result','occurred_at':self.base+'00Z',
            'event_type':'AGENT_ACTION_RESULT','actor':'agent','synthetic':False,
            'account':'••••0000','trade_id':'one','intent_id':'normal-review',
            'details':{'outcome':'pending_approval','broker_verified':False}})
        self.store(self.snap('one','entry'),self.snap('one','protected'),self.snap('one','flat'))
        report=self.check();timeline=report['live_lifecycle_evidence'][0]['protection_action_timeline'][0]
        self.assertTrue(report['supervised_live_smoke_passed'])
        self.assertFalse(report['manual_takeover_detected'])
        self.assertEqual(timeline['review_started_at'],self.base+'00Z')
        self.assertEqual(timeline['confirmation_wait_started_at'],self.base+'00Z')
        self.assertIsNone(timeline['user_confirmation_at'])
        self.assertIsNone(timeline['broker_accepted_at'])

    def test_agent_claim_of_confirmed_stop_does_not_prove_timing(self):
        self.book.audit.append({'event_id':'claim-confirmed','occurred_at':self.base+'01Z',
            'event_type':'RECONCILIATION_ISSUE','actor':'agent','synthetic':False,
            'account':'••••0000','trade_id':'one','details':{'broker_verified':True,
                'protection_accepted_at':self.base+'01Z'}})
        self.store(self.snap('one','entry'),self.snap('one','flat'))
        report=self.check();trade=report['live_lifecycle_evidence'][0]
        self.assertTrue(report['actual_exit_flat_validated'])
        self.assertFalse(report['protection_timeliness_validated'])
        self.assertIsNone(trade['protection_tranches'][0]['protection_confirmed_observed_at'])
        self.assertFalse(report['supervised_live_smoke_passed'])

    def test_each_entry_tranche_has_its_own_clock_and_full_coverage(self):
        for action in self.book.actions():
            payload=json.loads(action['payload']);payload['parameters']['quantity']='2'
            self.book.db.execute('UPDATE live_actions SET payload=? WHERE id=?',(json.dumps(payload),action['id']))
        self.book.db.commit()
        entry=self.snap('one','entry');protected=self.snap('one','protected');final=self.snap('one','flat')
        entry['orders']['one-entry'].update(quantity='2',remaining='1',state='partially_filled')
        protected['observed_at']=self.base+'31Z';protected['positions']['DEMO']['quantity']='2'
        final['observed_at']=self.base+'35Z'
        for snap in (protected,final):
            buy=snap['orders']['one-entry'];buy.update(quantity='2',filled='2')
            second=self.copy(buy['executions'][0]);second.update(id='second-fill',executed_at=self.base+'25Z')
            buy['executions'].append(second)
            snap['orders']['one-protect'].update(quantity='2',remaining='2')
        final['orders']['one-exit'].update(quantity='2',filled='2')
        final['orders']['one-exit']['executions'][0].update(quantity='2',executed_at=self.base+'34Z')
        self.store(entry,protected,final)
        trade=self.check()['live_lifecycle_evidence'][0]
        self.assertTrue(trade['complete_protected_round_trip'])
        tranches=trade['protection_tranches']
        self.assertEqual([t['unprotected_seconds_upper_bound'] for t in tranches],[31,6])
        self.assertEqual([t['within_30_seconds'] for t in tranches],[False,True])
        self.assertEqual(tranches[0]['latency_segments']['fill_to_observation']['seconds'],0)
        self.assertIsNone(tranches[0]['latency_segments']['confirmation_wait']['seconds'])
        self.assertFalse(trade['supervised_module_acceptance_passed'])

    def test_explicit_manual_audit_marker_cannot_be_cleared_by_flat_trade(self):
        self.book.audit.append({'event_id':'takeover','occurred_at':self.base+'01Z',
            'event_type':'RECONCILIATION_ISSUE','actor':'script','synthetic':False,
            'account':'••••0000','trade_id':'one','details':{'manual_takeover':True}})
        self.store(self.snap('one','entry'),self.snap('one','protected'),self.snap('one','flat'))
        report=self.check()
        self.assertTrue(report['actual_exit_flat_validated'])
        self.assertTrue(report['manual_takeover_detected'])
        self.assertFalse(report['supervised_live_smoke_passed'])

    def add_manual_incident(self,second='10'):
        self.book.audit.append({'event_id':'manual-incident-'+second,'occurred_at':self.base+second+'Z',
            'event_type':'RECONCILIATION_ISSUE','actor':'script','synthetic':False,
            'account':'••••0000','trade_id':'one','details':{'manual_takeover':True}})

    def later_trade_chain(self,start=12):
        self.add_trade('two')
        snaps=[self.snap('two',stage) for stage in ('entry','protected','flat')]
        for snap in snaps:
            snap['observed_at']=(datetime.fromisoformat(snap['observed_at'])+timedelta(seconds=start)).isoformat()
            for order in snap['orders'].values():
                for fill in order['executions']:
                    fill['executed_at']=(datetime.fromisoformat(fill['executed_at'])+timedelta(seconds=start)).isoformat()
        self.store(*snaps)

    def test_later_module_acceptance_clears_current_manual_blocker_preserves_history(self):
        self.store(self.snap('one','entry'),self.snap('one','protected'),self.snap('one','flat'))
        self.add_manual_incident();self.later_trade_chain()
        report=self.check()
        self.assertTrue(report['supervised_live_smoke_passed'])
        self.assertTrue(report['historical_manual_takeover_detected'])
        self.assertFalse(report['manual_takeover_current_blocker'])
        self.assertTrue(report['post_takeover_supervised_acceptance_validated'])
        self.assertNotIn('MANUAL_TAKEOVER_REQUIRES_NEW_MODULE_ACCEPTANCE',report['blockers'])
        failed=next(t for t in report['live_lifecycle_evidence'] if t['trade_id']=='one')
        self.assertFalse(failed['supervised_module_acceptance_passed'])

    def test_preincident_success_and_equal_time_entry_do_not_clear_current_blocker(self):
        self.add_manual_incident();self.later_trade_chain(start=10)
        report=self.check();self.assertFalse(report['supervised_live_smoke_passed'])
        self.assertTrue(report['manual_takeover_current_blocker'])

    def test_missing_incident_time_does_not_guess_later_success(self):
        state=self.book.state();state['trades']['one']['manual_takeover']=True;self.book._save(state);self.book.db.commit()
        self.later_trade_chain()
        report=self.check()
        self.assertTrue(report['manual_takeover_current_blocker'])
        self.assertFalse(report['manual_takeover_boundary']['known'])
        self.assertFalse(report['supervised_live_smoke_passed'])

    def test_recovery_acknowledgement_after_success_requires_a_newer_acceptance(self):
        self.add_manual_incident();self.later_trade_chain()
        self.book.db.execute('CREATE TABLE recovery_cases(id TEXT PRIMARY KEY,trade_id TEXT NOT NULL,status TEXT NOT NULL,payload TEXT NOT NULL)')
        self.book.db.execute('INSERT INTO recovery_cases VALUES(?,?,?,?)',('recovered','one','ACKNOWLEDGED',json.dumps({
            'manual_takeover':True,'opened_at':self.base+'10Z','acknowledged_at':self.base+'20Z'})))
        self.book.db.commit()
        report=self.check();self.assertTrue(report['manual_takeover_current_blocker'])
        self.assertFalse(report['supervised_live_smoke_passed'])
        self.assertEqual(datetime.fromisoformat(report['manual_takeover_boundary']['latest_at']),datetime.fromisoformat(self.base+'20Z'))

    def test_acceptance_after_recovery_ack_can_clear_only_current_blocker(self):
        self.add_manual_incident();self.later_trade_chain()
        self.book.db.execute('CREATE TABLE recovery_cases(id TEXT PRIMARY KEY,trade_id TEXT NOT NULL,status TEXT NOT NULL,payload TEXT NOT NULL)')
        self.book.db.execute('INSERT INTO recovery_cases VALUES(?,?,?,?)',('recovered','one','ACKNOWLEDGED',json.dumps({
            'manual_takeover':True,'opened_at':self.base+'10Z','acknowledged_at':self.base+'11Z'})))
        self.book.db.commit()
        report=self.check()
        self.assertTrue(report['supervised_live_smoke_passed'])
        self.assertTrue(report['historical_manual_takeover_detected'])
        self.assertFalse(report['manual_takeover_current_blocker'])
        self.assertFalse(next(t for t in report['live_lifecycle_evidence'] if t['trade_id']=='one')['supervised_module_acceptance_passed'])
