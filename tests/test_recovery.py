from copy import deepcopy
from datetime import datetime, timedelta
import json
from pathlib import Path
import tempfile
from unittest import TestCase

from martin_agentic.audit import canonical
from martin_agentic.broker import BrokerJournal
from entry_authority_fixture import TestExecutionBook as ExecutionBook
from martin_agentic.recovery import RecoveryCases, recovery_disposition, readonly_recovery_view
from martin_agentic.recovery import _result_order_id
from test_broker import FakeBroker, order, snapshot, AT


class RecoveryTests(TestCase):
    """All broker records here are deterministic fixtures, never live reads."""
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name)/'fixture.sqlite'
        self.now = datetime.fromisoformat(AT)
        policy = json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        self.book = ExecutionBook(self.path,policy,clock=lambda:self.now)
        self.journal = BrokerJournal(self.path)
        self.recovery = RecoveryCases(self.book)
        broker = FakeBroker()
        broker.orders = [order('buy','buy'), order('stop','sell',filled='0',state='cancelled',stop='39'),
                         order('exit','sell')]
        self.snapshot = snapshot(broker,AT)
        self.journal.ingest(self.snapshot)
        state = self.book.state()
        state.update(account_key=self.snapshot['account_key'],issues=['UNMANAGED_POSITION_OR_ORDER',
            'POSITION_NOT_EXPLAINED_BY_OWN_EXECUTIONS:DEMO','PROTECTION_WAIT_EXCEEDED:DEMO'],
            trades={'old':{'id':'old','plan':{'symbol':'DEMO'},'phase':'MANUAL_INTERVENTION_REQUIRED'}})
        self.book._save(state)
        self.book.db.commit()
        for name,action in [('buy','entry'),('stop','stop_loss'),('exit','exit')]:
            normalized = self.snapshot['orders'][name]
            params = {k:normalized[k] for k in ('symbol','side','type','quantity','time_in_force','market_hours')}
            if normalized.get('stop') is not None:
                params['stop_price'] = normalized['stop']
            self.book.audit.append({'event_id':name+'-intent','occurred_at':AT,'event_type':'AGENT_ACTION_INTENT',
                'actor':'agent','synthetic':False,'account':'••••0000','intent_id':name,'trade_id':'old',
                'details':{'action':action,'parameters':params}})
            # Match actual legacy audit layouts: the native buy result omitted
            # account while its unique durable intent carried the account bind.
            result = {'event_id':name+'-result','occurred_at':AT,'event_type':'AGENT_ACTION_RESULT',
                      'actor':'agent','synthetic':False,'intent_id':name,'trade_id':'old',
                      'details':{'outcome':'submitted'}}
            if name == 'buy':
                result['details']['external_report'] = {'order_id':name}
            else:
                result['account'] = '••••0000'
                result['details']['broker_order_id' if name=='stop' else 'order_id'] = name
            self.book.audit.append(result)
        self.parameters = dict(intent_ids=['buy','stop','exit'],incident_order_ids=['buy'],
                               manual_order_ids=['stop','exit'],execution_ids=['buy-exec','exit-exec'],
                               cancelled_order_ids=['stop'],reason='Fixture manual recovery after module fault')

    def tearDown(self):
        self.journal.close()
        self.book.close()
        self.tmp.cleanup()

    def opened(self):
        return self.recovery.open('old',**self.parameters)

    def acknowledged(self):
        case = self.opened()
        self.recovery.verify(case['id'],self.snapshot)
        return self.recovery.acknowledge(case['id'],self.snapshot,reason='Reviewed exact fixture terminal receipts')

    def persist(self, changed):
        self.now += timedelta(seconds=1)
        changed['observed_at'] = self.now.isoformat()
        self.journal.ingest(changed)
        return changed

    def test_ack_keeps_failure_state_issues_risk_and_manual_provenance(self):
        before = self.book.state()
        risk = self.book.risk_guard.state()
        case = self.acknowledged()
        self.assertEqual(case['status'],'ACKNOWLEDGED')
        self.assertTrue(case['manual_takeover'])
        self.assertFalse(case['automatic_execution_enabled'])
        self.assertEqual(self.book.state(),before)
        self.assertEqual(self.book.risk_guard.state(),risk)
        d = recovery_disposition(self.book,self.snapshot)
        self.assertEqual(d['acknowledged_trade_ids'],['old'])
        self.assertEqual(d['covered_issue_ids'],['POSITION_NOT_EXPLAINED_BY_OWN_EXECUTIONS:DEMO','UNMANAGED_POSITION_OR_ORDER'])
        events = [json.loads(r[0]) for r in self.book.db.execute('SELECT payload FROM audit_events')]
        recovery_events = [e for e in events if e['details'].get('recovery_case_id')]
        self.assertEqual([e['details']['status'] for e in recovery_events],['OPEN','BROKER_SAFE_VERIFIED','ACKNOWLEDGED'])
        self.assertTrue(all(e['details']['manual_takeover'] for e in recovery_events))
        self.assertTrue(self.book.audit.verify()['integrity_valid'])

    def test_open_requires_durable_original_intent_and_exact_execution_manifest(self):
        self.parameters['intent_ids'] = ['not-durable']
        with self.assertRaisesRegex(ValueError,'INTENT_NOT_DURABLE'):
            self.opened()
        self.parameters['intent_ids'] = ['buy','stop','exit']
        self.parameters['execution_ids'] = ['buy-exec']
        case = self.opened()
        with self.assertRaisesRegex(ValueError,'EXECUTION_IDENTITY_MISMATCH'):
            self.recovery.verify(case['id'],self.snapshot)
        self.assertEqual(self.recovery.get(case['id'])['status'],'OPEN')

    def test_unpersisted_or_tampered_snapshot_cannot_prove_safety(self):
        case = self.opened()
        changed = deepcopy(self.snapshot)
        changed['portfolio']['cash'] = '10000'
        with self.assertRaisesRegex(ValueError,'DIFFERS_FROM_JOURNAL'):
            self.recovery.verify(case['id'],changed)

    def test_wrong_account_stale_and_synthetic_rejected(self):
        case = self.opened()
        changed = deepcopy(self.snapshot)
        changed['account_key'] = 'other'
        with self.assertRaises(ValueError):
            self.recovery.verify(case['id'],changed)
        changed = deepcopy(self.snapshot)
        changed['synthetic'] = True
        with self.assertRaises(ValueError):
            self.recovery.verify(case['id'],changed)
        self.now += timedelta(seconds=31)
        with self.assertRaises(ValueError):
            self.recovery.verify(case['id'],self.snapshot)

    def test_cancel_acceptance_or_flat_missing_order_is_insufficient(self):
        case = self.opened()
        changed = deepcopy(self.snapshot)
        changed['orders']['stop']['state'] = 'pending_cancelled'
        self.persist(changed)
        with self.assertRaisesRegex(ValueError,'OPEN_OR_UNKNOWN_ORDER|BROKER_RECONCILIATION_UNRESOLVED'):
            self.recovery.verify(case['id'],changed)

    def test_open_assets_and_pending_approval_block_ack_recheck(self):
        case = self.opened()
        self.recovery.verify(case['id'],self.snapshot)
        changed = deepcopy(self.snapshot)
        changed['pending_approvals'] = 1
        changed['pending_approval_ids'] = ['pending']
        self.persist(changed)
        with self.assertRaisesRegex(ValueError,'APPROVAL_UNRESOLVED'):
            self.recovery.acknowledge(case['id'],changed,reason='Must not acknowledge')
        self.assertEqual(self.recovery.get(case['id'])['status'],'BROKER_SAFE_VERIFIED')

    def test_unknown_write_blocks_recovery_despite_flat_snapshot(self):
        case = self.opened()
        self.book.db.execute('INSERT INTO external_operations VALUES(?,?,?,?,NULL,NULL)',('unknown','a','place',AT))
        self.book.db.commit()
        with self.assertRaisesRegex(ValueError,'UNKNOWN_OR_UNRESOLVED_WRITE'):
            self.recovery.verify(case['id'],self.snapshot)

    def test_manual_unknown_intent_blocks_recovery(self):
        case = self.opened()
        self.book.audit.append({'event_id':'unknown-intent','occurred_at':AT,'event_type':'AGENT_ACTION_INTENT',
            'actor':'agent','synthetic':False,'account':'••••0000','intent_id':'manual-unknown',
            'details':{'action':'exit','parameters':{'symbol':'DEMO','quantity':'1'}}})
        with self.assertRaisesRegex(ValueError,'MANUAL_WRITE_UNRESOLVED'):
            self.recovery.verify(case['id'],self.snapshot)

    def test_explicit_protection_disposition_preserves_incident_and_risk(self):
        case=self.acknowledged()
        self.book.risk_guard.observe(self.snapshot)
        before_state=self.book.state();before_risk=self.book.risk_guard.state()
        original=self.recovery.get(case['id'])
        addition=self.recovery.dispose_protection_incident(case['id'],self.snapshot,reason='Explicit incident closed after manual flat verification')
        self.assertFalse(addition['normal_lifecycle_accepted']);self.assertFalse(addition['production_ready'])
        self.assertEqual(self.book.state(),before_state);self.assertEqual(self.book.risk_guard.state(),before_risk)
        self.assertEqual(self.recovery.get(case['id']),original)
        self.assertIn('PROTECTION_WAIT_EXCEEDED:DEMO',recovery_disposition(self.book,self.snapshot)['covered_issue_ids'])
        self.book.audit.append({'event_id':'new-protection-failure','occurred_at':AT,
            'event_type':'RECONCILIATION_ISSUE','actor':'script','synthetic':False,'account':'••••0000',
            'details':{'issues':['PROTECTION_WAIT_EXCEEDED:DEMO']}})
        proof=recovery_disposition(self.book,self.snapshot)
        self.assertNotIn('PROTECTION_WAIT_EXCEEDED:DEMO',proof['covered_issue_ids'])
        self.assertTrue(any('RECURRED' in b for b in proof['blockers']))
        with self.assertRaisesRegex(ValueError,'VALID_ACK_REQUIRED|ISSUE_RECURRED'):
            self.recovery.dispose_protection_incident(case['id'],self.snapshot,reason='Cannot forgive new incident')

    def test_protection_disposition_rejects_unacknowledged_and_unknown_write(self):
        case=self.opened()
        with self.assertRaisesRegex(ValueError,'ACK_AND_REASON_REQUIRED'):
            self.recovery.dispose_protection_incident(case['id'],self.snapshot,reason='No ACK')
        self.recovery.verify(case['id'],self.snapshot)
        self.recovery.acknowledge(case['id'],self.snapshot,reason='Fixture ACK')
        self.book.risk_guard.observe(self.snapshot)
        self.book.db.execute('INSERT INTO external_operations VALUES(?,?,?,?,NULL,NULL)',('new-unknown','missing','place',AT));self.book.db.commit()
        with self.assertRaisesRegex(ValueError,'UNKNOWN_OR_UNRESOLVED_WRITE'):
            self.recovery.dispose_protection_incident(case['id'],self.snapshot,reason='Cannot mask pending write')

    def test_protection_disposition_rejects_tamper_and_stale_observation(self):
        case=self.acknowledged();self.book.risk_guard.observe(self.snapshot)
        self.now+=timedelta(seconds=31)
        with self.assertRaisesRegex(ValueError,'VALID_ACK_REQUIRED'):
            self.recovery.dispose_protection_incident(case['id'],self.snapshot,reason='Stale proof prohibited')
        self.now-=timedelta(seconds=31)
        addition=self.recovery.dispose_protection_incident(case['id'],self.snapshot,reason='Fresh proof')
        addition['issue_id']='PROTECTION_WAIT_EXCEEDED:OTHER'
        with self.book.db:self.book.db.execute('UPDATE protection_incident_dispositions SET payload=?',(json.dumps(addition),))
        proof=recovery_disposition(self.book,self.snapshot)
        self.assertTrue(any('PROTECTION_DISPOSITION_AUDIT_BINDING_INVALID' in b for b in proof['blockers']))
        self.assertNotIn('PROTECTION_WAIT_EXCEEDED:DEMO',proof['covered_issue_ids'])

    def test_protection_disposition_cannot_cover_later_issue(self):
        state=self.book.state();state['issues'].remove('PROTECTION_WAIT_EXCEEDED:DEMO')
        self.book._save(state);self.book.db.commit()
        case=self.acknowledged();self.book.risk_guard.observe(self.snapshot)
        state=self.book.state();state['issues'].append('PROTECTION_WAIT_EXCEEDED:DEMO')
        self.book._save(state);self.book.db.commit()
        with self.assertRaisesRegex(ValueError,'ORIGINAL_HISTORICAL_ISSUE_REQUIRED'):
            self.recovery.dispose_protection_incident(case['id'],self.snapshot,reason='Cannot dispose newly introduced issue')

    def test_protection_disposition_rejects_current_open_position(self):
        case=self.acknowledged();self.book.risk_guard.observe(self.snapshot)
        self.now+=timedelta(seconds=1);self.snapshot['observed_at']=self.now.isoformat()
        self.snapshot['positions']['DEMO']={'quantity':'1','available_for_sells':'1','held_for_sells':'0'}
        self.journal.ingest(self.snapshot)
        with self.assertRaisesRegex(ValueError,'ACCOUNT_NOT_FLAT|BROKER_RECONCILIATION_UNRESOLVED'):
            self.recovery.dispose_protection_incident(case['id'],self.snapshot,reason='Must reject exposure')

    def test_write_intent_view_preserves_ack_and_requires_exact_terminal_approval(self):
        from martin_agentic.recovery import unresolved_write_intents
        self.acknowledged()
        check=lambda:unresolved_write_intents(self.book.db,self.snapshot,account_mask='••••0000')
        self.assertEqual(check(),[])
        with self.book.db:
            self.book.db.execute('INSERT INTO external_operations VALUES(?,?,?,?,NULL,NULL)',
                ('expired-write','not-an-action','place',AT))
            self.book.db.execute('INSERT INTO external_broker_refs VALUES(?,?,?)',
                ('exact-approval','not-an-action','expired-write'))
        self.assertEqual(check(),['expired-write'])
        self.snapshot['requested_approval_ids']=['exact-approval']
        self.snapshot['approval_states']={'exact-approval':{'approval_id':'exact-approval',
            'asset_class':'ASSET_CLASS_EQUITY','is_edited':False,'state':'APPROVAL_STATE_EXPIRED'}}
        self.assertEqual(check(),[])
        self.snapshot['approval_states']['exact-approval']['is_edited']=True
        self.assertEqual(check(),['expired-write'])

    def test_write_intent_view_rejects_synthetic_receipt_and_wrong_cancel_target(self):
        from martin_agentic.recovery import unresolved_write_intents
        for identity,action,parameters in [('new-write','entry',{'symbol':'DEMO','quantity':'1'}),
                                            ('new-cancel','cancel',{'order_id':'stop'})]:
            self.book.audit.append({'event_id':identity+'-intent','occurred_at':AT,
                'event_type':'AGENT_ACTION_INTENT','actor':'agent','synthetic':False,
                'account':'••••0000','intent_id':identity,'details':{'action':action,'parameters':parameters}})
            self.book.audit.append({'event_id':identity+'-result','occurred_at':AT,
                'event_type':'AGENT_ACTION_RESULT','actor':'agent','synthetic':identity=='new-write',
                'account':'••••0000','intent_id':identity,'broker_order_id':'buy',
                'details':{'outcome':'submitted'}})
        self.assertEqual(unresolved_write_intents(self.book.db,self.snapshot,account_mask='••••0000'),
            ['new-cancel','new-write'])

    def test_ack_proof_survives_later_trade_but_not_recovered_receipt_change(self):
        self.acknowledged()
        changed = deepcopy(self.snapshot)
        changed['positions']['NEW'] = {'quantity':'1','available_for_sells':'1','held_for_sells':'0'}
        changed['observed_at'] = (self.now+timedelta(seconds=1)).isoformat()
        self.now += timedelta(seconds=1)
        self.assertEqual(recovery_disposition(self.book,changed)['acknowledged_trade_ids'],['old'])
        changed['orders']['buy']['executions'][0]['price'] = '41'
        d = recovery_disposition(self.book,changed)
        self.assertEqual(d['acknowledged_trade_ids'],[])
        self.assertTrue(d['blockers'])

    def test_repeated_issue_not_forgiven_by_old_ack(self):
        self.acknowledged()
        self.book.audit.append({'event_id':'new-issue','occurred_at':AT,'event_type':'RECONCILIATION_ISSUE',
            'actor':'script','synthetic':False,'account':'••••0000',
            'details':{'issues':['UNMANAGED_POSITION_OR_ORDER']}})
        d = recovery_disposition(self.book,self.snapshot)
        self.assertNotIn('UNMANAGED_POSITION_OR_ORDER',d['covered_issue_ids'])
        self.assertTrue(any('RECURRED' in b for b in d['blockers']))

    def test_ack_cannot_skip_verification_and_reopen_does_not_remove_manual_marker(self):
        case = self.opened()
        with self.assertRaises(ValueError):
            self.recovery.acknowledge(case['id'],self.snapshot,reason='Skip')
        with self.assertRaisesRegex(ValueError,'ALREADY_OPEN'):
            self.opened()

    def test_order_terms_must_match_durable_placement_intent(self):
        case = self.opened()
        changed = deepcopy(self.snapshot)
        changed['orders']['stop']['stop'] = '38'
        self.persist(changed)
        with self.assertRaisesRegex(ValueError,'INTENT_TERMS_MISMATCH'):
            self.recovery.verify(case['id'],changed)

    def test_exact_execution_id_does_not_forgive_quantity_mismatch(self):
        case = self.opened()
        changed = deepcopy(self.snapshot)
        changed['orders']['buy']['filled'] = '2'
        self.persist(changed)
        with self.assertRaisesRegex(ValueError,'EXECUTION_QUANTITY_MISMATCH'):
            self.recovery.verify(case['id'],changed)

    def test_mutable_case_row_cannot_forge_audited_acknowledgement(self):
        case = self.acknowledged()
        case['covered_issue_ids'].append('PROTECTION_WAIT_EXCEEDED:DEMO')
        self.book.db.execute('UPDATE recovery_cases SET payload=? WHERE id=?',(canonical(case),case['id']))
        self.book.db.commit()
        with self.assertRaisesRegex(ValueError,'AUDIT_BINDING_INVALID'):
            self.recovery.get(case['id'])
        disposition = recovery_disposition(self.book,self.snapshot)
        self.assertEqual(disposition['acknowledged_trade_ids'],[])
        self.assertTrue(any('AUDIT_BINDING_INVALID' in b for b in disposition['blockers']))

    def test_readonly_view_writes_neither_book_nor_risk_and_labels_history(self):
        self.acknowledged()
        before = self.path.read_bytes()
        risk_path = self.path.with_name('account-risk-0000.sqlite')
        risk_before = risk_path.read_bytes()
        historical = readonly_recovery_view(self.path,self.book.policy,historical_as_of=AT)
        self.assertEqual(historical['acknowledged_trade_ids'],['old'])
        self.assertTrue(historical['historical_only'])
        self.assertFalse(historical['live_admission_authorized'])
        stale = readonly_recovery_view(self.path,self.book.policy,clock=lambda:self.now+timedelta(hours=1))
        self.assertEqual(stale['acknowledged_trade_ids'],[])
        self.assertFalse(stale['historical_only'])
        self.assertTrue(stale['blockers'])
        live = readonly_recovery_view(self.path,self.book.policy,clock=lambda:self.now)
        self.assertEqual(live['acknowledged_trade_ids'],['old'])
        self.assertFalse(live['live_admission_authorized'])
        self.assertEqual(self.path.read_bytes(),before)
        self.assertEqual(risk_path.read_bytes(),risk_before)

    def test_readonly_view_cannot_override_policy_binding_or_create_database(self):
        self.acknowledged()
        wrong = {**self.book.policy,'account_suffix':'9999'}
        self.assertTrue(readonly_recovery_view(self.path,wrong,historical_as_of=AT)['blockers'])
        missing = self.path.with_name('must-not-create.sqlite')
        self.assertTrue(readonly_recovery_view(missing,self.book.policy,historical_as_of=AT)['blockers'])
        self.assertFalse(missing.exists())

    def test_only_explicit_receipt_order_ids_are_supported_and_conflicts_rejected(self):
        for event in ({'broker_order_id':'order'}, {'details':{'broker_order_id':'order'}},
                      {'details':{'order_id':'order'}}, {'details':{'external_report':{'order_id':'order'}}}):
            self.assertEqual(_result_order_id(event),'order')
        self.assertIsNone(_result_order_id({'details':{'id':'generic','ref_id':'ref','approval_id':'approval'}}))
        with self.assertRaisesRegex(ValueError,'ORDER_ID_CONFLICT'):
            _result_order_id({'broker_order_id':'a','details':{'order_id':'b'}})

    def test_old_ack_cannot_be_reopened_to_cover_new_incident(self):
        self.acknowledged()
        with self.assertRaisesRegex(ValueError,'ALREADY_OPEN'):
            self.opened()

    def test_historical_readonly_accepts_only_audited_bounded_stop_not_live(self):
        self.acknowledged()
        self.journal.failure(at=(self.now+timedelta(seconds=1)).isoformat(),reason='BOUNDED_OBSERVER_STOPPED')
        view = readonly_recovery_view(self.path,self.book.policy,historical_as_of=AT)
        self.assertEqual(view['acknowledged_trade_ids'],['old'])
        self.assertTrue(view['historical_only'])
        self.assertTrue(view['source_stopped_after_completed_snapshot'])
        self.assertFalse(view['live_admission_authorized'])
        live = readonly_recovery_view(self.path,self.book.policy,clock=lambda:self.now+timedelta(seconds=1))
        self.assertEqual(live['acknowledged_trade_ids'],[])
        self.assertTrue(live['blockers'])
        self.journal.failure(at=(self.now+timedelta(seconds=2)).isoformat(),reason='AUTHENTICATION_REQUIRED')
        self.assertTrue(readonly_recovery_view(self.path,self.book.policy,historical_as_of=AT)['blockers'])

    def test_historical_stop_requires_exact_audit_and_bounded_timestamp(self):
        self.acknowledged()
        self.journal.failure(at=(self.now+timedelta(seconds=31)).isoformat(),reason='LIVE_SUPERVISOR_STOPPED')
        self.assertTrue(readonly_recovery_view(self.path,self.book.policy,historical_as_of=AT)['blockers'])
        self.book.db.execute('UPDATE broker_health SET payload=?',(json.dumps({
            'healthy':False,'at':(self.now+timedelta(seconds=1)).isoformat(),'reason':'BOUNDED_OBSERVER_STOPPED'}),))
        self.book.db.commit()
        self.assertTrue(readonly_recovery_view(self.path,self.book.policy,historical_as_of=AT)['blockers'])

    def terminal_approval(self, state):
        self.book.db.execute('INSERT INTO live_actions VALUES(?,?,?,?,?,?,?,NULL)',
            ('no-order-action','old','entry','APPROVAL_DECLINED',canonical({'plan':{'symbol':'DEMO'}}),AT,AT))
        self.book.db.execute('INSERT INTO external_operations VALUES(?,?,?,?,?,NULL)',
            ('no-order-place','no-order-action','place',AT,canonical({'status':'rejected'})))
        self.book.db.execute('INSERT INTO external_broker_refs VALUES(?,?,?)',
            ('exact-approval','no-order-action','no-order-place'))
        self.book.db.commit()
        self.book.audit.append({'event_id':'no-order-intent','occurred_at':AT,'event_type':'AGENT_ACTION_INTENT',
            'actor':'agent','synthetic':False,'account':'••••0000','intent_id':'no-order-place','trade_id':'old',
            'details':{'action':'entry','parameters':{'symbol':'DEMO','side':'buy','type':'market','quantity':'1'}}})
        self.book.audit.append({'event_id':'no-order-result','occurred_at':AT,'event_type':'AGENT_ACTION_RESULT',
            'actor':'agent','synthetic':False,'intent_id':'no-order-place','details':{'outcome':'rejected'}})
        changed = deepcopy(self.snapshot)
        changed['requested_approval_ids'] = ['exact-approval']
        changed['approval_states'] = {'exact-approval':{'approval_id':'exact-approval',
            'asset_class':'ASSET_CLASS_EQUITY','state':state,'is_edited':False}}
        return self.persist(changed)

    def test_formal_declined_no_order_is_terminal_for_global_intent_check(self):
        changed = self.terminal_approval('APPROVAL_STATE_DECLINED')
        case = self.opened()
        self.assertEqual(self.recovery.verify(case['id'],changed)['status'],'BROKER_SAFE_VERIFIED')

    def test_formal_expired_no_order_is_terminal_for_global_intent_check(self):
        changed = self.terminal_approval('APPROVAL_STATE_EXPIRED')
        case = self.opened()
        self.assertEqual(self.recovery.verify(case['id'],changed)['status'],'BROKER_SAFE_VERIFIED')

    def test_terminal_no_order_requires_exact_requested_unedited_formal_state(self):
        baseline = self.terminal_approval('APPROVAL_STATE_DECLINED')
        case = self.opened()
        variants = []
        missing = deepcopy(baseline); missing['requested_approval_ids'] = []; variants.append(missing)
        edited = deepcopy(baseline); edited['approval_states']['exact-approval']['is_edited'] = True; variants.append(edited)
        for state in ('APPROVAL_STATE_PENDING','APPROVAL_STATE_APPROVED','APPROVAL_STATE_AUTO_APPROVED'):
            changed = deepcopy(baseline); changed['approval_states']['exact-approval']['state'] = state; variants.append(changed)
        for changed in variants:
            with self.subTest(approval=changed['approval_states']['exact-approval']):
                self.persist(changed)
                with self.assertRaises(ValueError):
                    self.recovery.verify(case['id'],changed)

