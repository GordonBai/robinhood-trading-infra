"""Deterministic original-tool fixtures; no actual broker calls or orders."""
from copy import deepcopy
from datetime import datetime, timedelta
import json
from pathlib import Path
import tempfile
from unittest import TestCase

from martin_agentic.audit import canonical
from martin_agentic.broker import BrokerJournal
from entry_authority_fixture import TestExecutionBook as ExecutionBook
from martin_agentic.manual_takeover import ManualTakeoverCases
from martin_agentic.recovery import recovery_disposition, RecoveryCases
from test_broker import FakeBroker, order, snapshot, AT, ACCOUNT


class ManualTakeoverTests(TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name)/'fixture.sqlite'
        self.now = datetime.fromisoformat(AT)
        policy = json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        self.book = ExecutionBook(self.path, policy, clock=lambda:self.now)
        self.journal = BrokerJournal(self.path)
        broker = FakeBroker()
        broker.orders = [order('buy','buy'), order('manual-exit','sell')]
        self.snapshot = snapshot(broker,AT)
        self.journal.ingest(self.snapshot)
        state = self.book.state()
        state.update(account_key=self.snapshot['account_key'], issues=['PROTECTION_WAIT_EXCEEDED:DEMO'],
                     trades={'old':{'id':'old','plan':{'symbol':'DEMO'},'phase':'MANUAL_INTERVENTION_REQUIRED'}})
        self.book._save(state)
        self.book.db.execute('INSERT INTO live_actions VALUES(?,?,?,?,?,?,?,?)',
            ('old-exit','old','exit','WAITING_USER','{}',AT,AT,canonical({'status':'review_only'})))
        self.book.db.commit()
        params = {k:self.snapshot['orders']['buy'][k] for k in ('symbol','side','type','quantity','time_in_force','market_hours')}
        self.book.audit.append({'event_id':'buy-intent','occurred_at':AT,'event_type':'AGENT_ACTION_INTENT',
            'actor':'agent','synthetic':False,'account':'••••0000','intent_id':'buy','trade_id':'old',
            'details':{'action':'entry','parameters':params}})
        self.book.audit.append({'event_id':'buy-result','occurred_at':AT,'event_type':'AGENT_ACTION_RESULT',
            'actor':'agent','synthetic':False,'account':'••••0000','intent_id':'buy','trade_id':'old',
            'details':{'outcome':'submitted','order_id':'buy'}})
        self.response = {'data':{'order':deepcopy(broker.orders[1]),'account_number':ACCOUNT}}
        self.cases = ManualTakeoverCases(self.book)
        self.params = dict(intent_ids=['buy'],incident_order_ids=['buy'],manual_order_ids=['manual-exit'],
            execution_ids=['buy-exec','manual-exit-exec'],cancelled_order_ids=[],reason='Observed manual flat after protection fault')

    def tearDown(self):
        self.journal.close()
        self.book.close()
        self.tmp.cleanup()

    def imported(self,response=None,**kwargs):
        return self.cases.import_response('old',response or self.response,
            **{'account_number':ACCOUNT,'tool_call_id':'formal-call-123','reason':'Original response imported retrospectively',**kwargs})

    def opened(self):
        self.imported()
        return self.cases.open('old',**self.params)

    def persist(self, changed):
        self.now += timedelta(seconds=1)
        changed['observed_at'] = self.now.isoformat()
        self.journal.ingest(changed)
        return changed

    def test_manual_receipt_closes_effective_intent_without_fabrication_or_risk_reset(self):
        before, risk = self.book.state(), self.book.risk_guard.state()
        case = self.opened()
        self.cases.verify(case['id'],self.snapshot)
        result = self.cases.acknowledge(case['id'],self.snapshot,reason='Exact original receipt and fresh journal checked')
        self.assertFalse(result['normal_lifecycle_acceptance_passed'])
        self.assertFalse(result['automatic_execution_enabled'])
        self.assertEqual(self.book.state(),before)
        self.assertEqual(self.book.risk_guard.state(),risk)
        self.assertEqual(self.book.actions()[0]['state'],'WAITING_USER')
        self.assertEqual(recovery_disposition(self.book,self.snapshot)['acknowledged_action_ids'],['old-exit'])
        events = [json.loads(r[0]) for r in self.book.db.execute('SELECT payload FROM audit_events')]
        self.assertEqual([e['intent_id'] for e in events if e['event_type']=='AGENT_ACTION_INTENT'],['buy'])
        self.assertTrue(self.book.audit.verify()['integrity_valid'])

    def test_default_recovery_still_requires_manual_preintent(self):
        case = RecoveryCases(self.book).open('old',**self.params)
        with self.assertRaisesRegex(ValueError,'ORDER_NOT_LINKED_TO_DURABLE_INTENT'):
            RecoveryCases(self.book).verify(case['id'],self.snapshot)

    def test_missing_raw_receipt_not_replaced_by_flat_account(self):
        with self.assertRaisesRegex(ValueError,'RECEIPT_MISSING'):
            self.cases.open('old',**self.params)
        self.assertEqual(self.book.db.execute('SELECT count(*) FROM recovery_cases').fetchone()[0],0)

    def test_wrong_request_or_response_account_rejected(self):
        with self.assertRaisesRegex(ValueError,'ACCOUNT_MISMATCH'):
            self.imported(account_number='other0000')
        changed=deepcopy(self.response);changed['data']['order']['account_number']='other0000'
        with self.assertRaisesRegex(ValueError,'ACCOUNT_MISMATCH'):
            self.imported(changed)

    def test_caller_report_or_approval_is_not_an_original_order_receipt(self):
        for response in ({'status':'submitted','order_id':'manual-exit'},
                         {'data':{'order':self.response['data']['order'],'approval_id':'pending'}},
                         {'isError':True,'data':self.response['data']}):
            with self.subTest(response=response), self.assertRaises(ValueError):
                self.imported(response)

    def test_import_duplicate_is_idempotent_and_conflicting_response_rejected(self):
        original=self.imported()
        self.now += timedelta(seconds=1)
        self.assertEqual(self.imported(),original)
        with self.assertRaisesRegex(ValueError,'RECEIPT_CONFLICT'):
            self.imported(tool_call_id='different-call')
        self.assertEqual(self.book.db.execute('SELECT count(*) FROM manual_takeover_receipts').fetchone()[0],1)

    def test_receipt_store_tamper_fails_audit_binding(self):
        self.imported()
        row=json.loads(self.book.db.execute('SELECT payload FROM manual_takeover_receipts').fetchone()[0])
        row['reason']='tampered'
        self.book.db.execute('UPDATE manual_takeover_receipts SET payload=?',(canonical(row),));self.book.db.commit()
        with self.assertRaisesRegex(ValueError,'AUDIT_INVALID'):
            self.cases.open('old',**self.params)

    def test_observed_terms_conflict_cannot_be_covered_by_matching_symbol_and_flatness(self):
        case=self.opened();changed=deepcopy(self.snapshot)
        changed['orders']['manual-exit']['market_hours']='extended_hours'
        self.persist(changed)
        with self.assertRaisesRegex(ValueError,'OBSERVED_ORDER_TERMS_MISMATCH'):
            self.cases.verify(case['id'],changed)

    def test_observed_execution_cannot_rewrite_original_receipt(self):
        case=self.opened();changed=deepcopy(self.snapshot)
        changed['orders']['manual-exit']['executions'][0]['price']='41'
        self.persist(changed)
        with self.assertRaisesRegex(ValueError,'OBSERVED_EXECUTION_MISMATCH|BROKER_RECONCILIATION_UNRESOLVED'):
            self.cases.verify(case['id'],changed)

    def test_queued_original_response_can_be_correlated_with_later_actual_fill(self):
        queued=deepcopy(self.response);queued['data']['order'].update(state='queued',cumulative_quantity='0',executions=[])
        self.imported(queued)
        case=self.cases.open('old',**self.params)
        self.assertEqual(self.cases.verify(case['id'],self.snapshot)['status'],'BROKER_SAFE_VERIFIED')

    def test_unknown_write_blocks_even_after_exact_manual_fill(self):
        case=self.opened()
        self.book.db.execute('INSERT INTO external_operations VALUES(?,?,?,?,NULL,NULL)',('unknown','other','place',AT));self.book.db.commit()
        with self.assertRaisesRegex(ValueError,'UNKNOWN_OR_UNRESOLVED_WRITE'):
            self.cases.verify(case['id'],self.snapshot)

    def test_unjournaled_or_stale_observation_cannot_retire(self):
        case=self.opened();changed=deepcopy(self.snapshot);changed['portfolio']['cash']='700'
        with self.assertRaisesRegex(ValueError,'DIFFERS_FROM_JOURNAL'):
            self.cases.verify(case['id'],changed)
        self.now += timedelta(seconds=31)
        with self.assertRaises(ValueError):self.cases.verify(case['id'],self.snapshot)

    def test_import_is_private_and_never_persists_full_account(self):
        result=self.imported()
        payload=canonical(result)
        self.assertNotIn(ACCOUNT,payload)
        self.assertNotIn('account_number',payload)

    def test_original_call_id_may_be_absent_with_explicit_saved_source_pin(self):
        result=self.imported(tool_call_id=None,origin_reference='sha256:'+'a'*64)
        self.assertIsNone(result['tool_call_id'])
        self.assertFalse(result['original_tool_call_id_recorded'])
        self.assertEqual(result['origin_reference'],'sha256:'+'a'*64)
        case=self.cases.open('old',**self.params)
        self.assertEqual(self.cases.verify(case['id'],self.snapshot)['status'],'BROKER_SAFE_VERIFIED')

    def test_missing_call_and_missing_or_malformed_origin_is_rejected(self):
        for origin in (None,'original-response.json','sha256:bad', {'sha256':'a'*64}):
            with self.subTest(origin=origin), self.assertRaisesRegex(ValueError,'CALL_OR_ORIGIN'):
                self.imported(tool_call_id=None,origin_reference=origin)

    def test_old_same_symbol_manual_exit_cannot_retire_later_incident(self):
        earlier=deepcopy(self.response)
        earlier['data']['order']['created_at']='2026-10-15T13:59:00Z'
        earlier['data']['order']['executions'][0]['timestamp']='2026-10-15T14:00:00Z'
        self.imported(earlier)
        changed=deepcopy(self.snapshot)
        changed['orders']['manual-exit']['created_at']='2026-10-15T13:59:00+00:00'
        changed['orders']['manual-exit']['executions'][0]['executed_at']='2026-10-15T14:00:00+00:00'
        self.persist(changed)
        case=self.cases.open('old',**self.params)
        events=self.cases._events()
        intents={e['intent_id']:e for e in events if e['event_type']=='AGENT_ACTION_INTENT'}
        results=[{**e,'broker_order_id':'buy'} for e in events if e['event_type']=='AGENT_ACTION_RESULT']
        with self.assertRaisesRegex(ValueError,'EXIT_PREDATES_INCIDENT'):
            self.cases._validate_order_linkage(case,changed['orders'],events,intents,results)
        with self.assertRaisesRegex(ValueError,'EXIT_PREDATES_INCIDENT|BROKER_RECONCILIATION_UNRESOLVED'):
            self.cases.verify(case['id'],changed)

    def test_import_immediately_disqualifies_normal_lifecycle_evidence(self):
        self.imported()
        events=self.cases._events()
        event=next(e for e in events if e['event_id']=='manual-takeover-import-manual-exit')
        self.assertTrue(event['details']['manual_takeover'])
        self.assertFalse(event['details']['pre_submission_intent_recorded'])
