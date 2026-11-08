"""Fail-closed automatic strategy admission; fixtures never call a broker."""
from datetime import datetime, timedelta
import json
from pathlib import Path
import tempfile
from unittest import TestCase
from unittest.mock import Mock, patch

from capability_contract_fixture import CONTRACT
from martin_agentic.audit import canonical
from martin_agentic.broker import BrokerJournal
from entry_authority_fixture import TestExecutionBook as ExecutionBook
from martin_luk.live_session import LiveSupervisor
from martin_agentic.production_admission import ProductionAdmission
from martin_agentic.qualification import source_fingerprint
from test_broker import snapshot


class ProductionAdmissionTests(TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.now=datetime.fromisoformat('2026-10-16T14:00:00Z')
        self.policy=json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text());self.policy['live_enabled']=True
        self.book=ExecutionBook(self.root/'book.sqlite',self.policy,clock=lambda:self.now)
        self.addCleanup(self.book.close)
        self.observed=snapshot(at=self.now.isoformat())
        journal=BrokerJournal(self.root/'book.sqlite');journal.ingest(self.observed);journal.close()
        self.book.reconcile(self.observed)
        self.source=source_fingerprint();self.tests=self.root/'tests.json';self.research=self.root/'research.json'
        self.tests.write_text(json.dumps({'passed':True,'source_fingerprint':self.source,
            'source_unchanged_during_tests':True,'tests':1010,'unattended_authorized':True}))
        self.research.write_text(json.dumps({'production_ready':True,'profitability_validated':True,
            'unattended_authorized':True,'status':'PASS'}))
        self.gate=ProductionAdmission(self.book,agent_mode='execute',owner_source=self.source,
            test_evidence_path=self.tests,research_evidence_path=self.research)

    def test_capability_consumer_uses_shared_nine_tool_evidence(self):
        inventory = json.loads(json.dumps(CONTRACT))
        inventory['captured_at'] = self.now.isoformat()
        path = self.root / 'capability-inventory.json'
        path.write_text(json.dumps(inventory))
        gate = ProductionAdmission(
            self.book, agent_mode='execute', owner_source=self.source,
            test_evidence_path=self.tests, research_evidence_path=self.research,
            capability_inventory_path=path)
        result = gate.assess(self.observed)
        self.assertTrue(result['capability_evidence']['available'])
        self.assertFalse(result['capability_evidence']['unattended_ready'])
        self.assertIn('FORMALLY_QUALIFIED_UNATTENDED_ADAPTER_UNAVAILABLE',
                      result['blockers'])
        self.assertIn('CURRENT_NATIVE_CANCEL_REQUIRES_SPECIFIC_CONFIRMATION',
                      result['blockers'])
        self.assertIn('CURRENT_ADVANCED_CANCEL_UNATTENDED_NOT_PROVEN',
                      result['blockers'])
        self.assertIn('ACCOUNT_TRADE_APPROVAL_SETTING_UNKNOWN',
                      result['blockers'])
        self.assertFalse(result['automatic_strategy_entry_allowed'])
        self.assertFalse(result['execution_authorized'])

    def test_synthetic_source_is_unavailable_not_eligible(self):
        inventory = json.loads(json.dumps(CONTRACT))
        inventory['source'] = 'synthetic_contract_fixture'
        inventory['synthetic'] = True
        inventory['account_data'] = False
        inventory['captured_at'] = self.now.isoformat()
        path = self.root / 'synthetic-source.json'
        path.write_text(json.dumps(inventory))
        gate = ProductionAdmission(
            self.book, agent_mode='execute', owner_source=self.source,
            test_evidence_path=self.tests, research_evidence_path=self.research,
            capability_inventory_path=path)
        result = gate.assess(self.observed)
        self.assertFalse(result['capability_evidence']['available'])
        self.assertIn('CAPABILITY_EVIDENCE_UNAVAILABLE', result['blockers'])
        self.assertFalse(result['automatic_strategy_entry_allowed'])
        self.assertFalse(result['execution_authorized'])

    def test_unknown_confirmation_tools_remain_unknown_not_false(self):
        inventory = json.loads(json.dumps(CONTRACT))
        inventory['captured_at'] = self.now.isoformat()
        inventory['tools'] = [{'name': 'get_equity_quotes',
                               'description': 'Read quotes only.'}]
        path = self.root / 'read-only-capability.json'
        path.write_text(json.dumps(inventory))
        gate = ProductionAdmission(
            self.book, agent_mode='execute', owner_source=self.source,
            test_evidence_path=self.tests, research_evidence_path=self.research,
            capability_inventory_path=path)
        result = gate.assess(self.observed)
        self.assertTrue(result['capability_evidence']['available'])
        self.assertIn('CURRENT_NATIVE_PLACE_CONFIRMATION_UNKNOWN',
                      result['blockers'])
        self.assertIn('CURRENT_NATIVE_CANCEL_CONFIRMATION_UNKNOWN',
                      result['blockers'])
        self.assertIn('CURRENT_ADVANCED_CANCEL_CONFIRMATION_UNKNOWN',
                      result['blockers'])
        self.assertNotIn('CURRENT_NATIVE_PLACE_REQUIRES_SPECIFIC_CONFIRMATION',
                         result['blockers'])
        self.assertFalse(result['automatic_strategy_entry_allowed'])
        self.assertFalse(result['execution_authorized'])

    def test_green_or_forged_local_evidence_cannot_create_formal_capability(self):
        with patch.object(self.book,'entry_gate',return_value={'blocked':False}):
            result=self.gate.assess(self.observed)
        self.assertTrue(result['software_test_evidence_current'])
        self.assertTrue(result['current_account_observation_verified'])
        self.assertTrue(result['current_risk_verified'])
        self.assertFalse(result['automatic_strategy_entry_allowed'])
        self.assertFalse(result['execution_authorized']);self.assertFalse(result['research_admitted'])
        self.assertIn('FORMALLY_QUALIFIED_UNATTENDED_ADAPTER_UNAVAILABLE',result['blockers'])
        self.assertIn('STRATEGY_RESEARCH_ADMISSION_NOT_ESTABLISHED',result['blockers'])

    def test_source_change_invalidates_current_owner_and_test_diagnostics(self):
        with patch('martin_agentic.production_admission._bounded_source_fingerprint',return_value='f'*64):
            result=self.gate.assess(self.observed)
        self.assertIn('PRODUCTION_OWNER_SOURCE_CHANGED',result['blockers'])
        self.assertIn('PRODUCTION_CURRENT_SOURCE_TEST_EVIDENCE_MISSING',result['blockers'])

    def test_tampered_or_oversized_diagnostic_file_does_not_interrupt_reconciliation(self):
        self.tests.write_text('{invalid')
        result=self.gate.assess(self.observed)
        self.assertIn('PRODUCTION_CURRENT_SOURCE_TEST_EVIDENCE_MISSING',result['blockers'])
        self.tests.write_text('x'*2_000_001)
        self.assertFalse(self.gate.assess(self.observed)['software_test_evidence_current'])

    def test_stale_or_caller_modified_snapshot_is_not_admission(self):
        self.now+=timedelta(seconds=31)
        result=self.gate.assess(self.observed)
        self.assertIn('PRODUCTION_CURRENT_BOUND_ACCOUNT_NOT_VERIFIED',result['blockers'])
        self.now-=timedelta(seconds=31)
        changed={**self.observed,'buying_power':'999'}
        self.assertFalse(self.gate.assess(changed)['current_account_observation_verified'])

    def test_source_inventory_is_bounded_before_hashing(self):
        with patch('martin_agentic.production_admission.MAX_SOURCE_BYTES',1):
            result=self.gate.assess(self.observed)
        self.assertIn('PRODUCTION_SOURCE_IDENTITY_UNAVAILABLE',result['blockers'])
        self.assertFalse(result['owner_source_matches'])

    def test_runtime_keeps_candidate_but_does_not_propose_real_automatic_strategy(self):
        dispatcher=Mock(return_value={'agent_called':False});dispatcher.requires_observation=True
        supervisor=LiveSupervisor(self.book,dispatcher=dispatcher,agent_mode='execute',owner_source=self.source)
        signal={'id':'candidate','plan':{},'quote':{},'evidence':{}}
        state=supervisor.signals.state();state['pending_signals']={'candidate':signal}
        self.book.db.execute('UPDATE live_strategy SET payload=?',(canonical(state),));self.book.db.commit()
        with patch.object(self.book,'propose',side_effect=AssertionError('Unqualified strategy must stay a candidate')) as propose:
            result=supervisor.step(self.observed,[],allow_entries=True)
        propose.assert_not_called();self.assertEqual(supervisor.signals.pending(),[signal])
        self.assertTrue(result['automatic_strategy_entries_blocked'])
        self.assertTrue(result['new_entries_blocked'])
        self.assertTrue(result['explicit_engineering_requests_allowed'])
        self.assertEqual(result['entry_dispatch_scope'],'ENGINEERING_ONLY')
        dispatcher.assert_called_once()  # Management/explicit engineering still runs.

    def queue(self,kind,purpose='strategy'):
        return self.book._queue({'id':'queued-'+kind,'purpose':purpose,'plan':{'symbol':'DEMO'}},kind,
            {'symbol':'DEMO','quantity':'1','side':'buy' if kind=='entry' else 'sell','type':'market'},self.now,'fixture')

    def test_existing_strategy_entry_is_not_dispatched_but_protection_is(self):
        self.queue('entry');protect=self.queue('protect')
        calls=[]
        def dispatch(**kwargs):
            calls.append(kwargs)
            action=self.book.claim(allow_entries=kwargs['allow_entries'])
            return {'agent_called':False,'action_id':action['action_id'] if action else None}
        dispatch.requires_observation=True
        supervisor=LiveSupervisor(self.book,dispatcher=dispatch,agent_mode='execute',owner_source=self.source)
        result=supervisor.step(self.observed,[],allow_entries=True)
        self.assertFalse(calls[0]['allow_entries']);self.assertEqual(result['dispatch']['action_id'],protect)
        self.assertTrue(result['new_entries_blocked'])
        self.assertFalse(result['explicit_engineering_requests_allowed'])
        self.assertEqual(next(a for a in self.book.actions() if a['kind']=='entry')['state'],'READY')

    def test_supervised_engineering_entry_stays_in_explicit_pathway(self):
        self.queue('entry',purpose='engineering')
        admission=self.gate.assess(self.observed)
        self.assertTrue(self.gate.permits_entry_dispatch(admission=admission,allow_entries=True))
        self.assertFalse(self.gate.permits_entry_dispatch(admission=admission,allow_entries=False))
        self.assertFalse(admission['automatic_strategy_entry_allowed'])

    def test_review_scope_keeps_candidate_preview_workflow_without_claiming_production(self):
        supervisor=LiveSupervisor(self.book,agent_mode='script-review',owner_source=self.source)
        signal={'id':'review-candidate','plan':{},'quote':{},'evidence':{}}
        state=supervisor.signals.state();state['pending_signals']={'review-candidate':signal}
        self.book.db.execute('UPDATE live_strategy SET payload=?',(canonical(state),));self.book.db.commit()
        with patch.object(self.book,'propose',return_value={'action_id':'review-only'}) as propose:
            result=supervisor.step(self.observed,[],allow_entries=True)
        propose.assert_called_once();self.assertTrue(result['review_only_strategy_proposals_allowed'])
        self.assertFalse(result['production_admission']['automatic_strategy_entry_allowed'])

    def test_mutating_diagnostic_booleans_cannot_dispatch_strategy_entry(self):
        self.queue('entry',purpose='strategy')
        forged={'required_for_current_dispatch':False,'automatic_strategy_entry_allowed':True}
        self.assertFalse(self.gate.permits_entry_dispatch(admission=forged,allow_entries=True))

    def test_engineering_dispatch_retained_while_automatic_risk_status_stays_locked(self):
        self.queue('entry',purpose='engineering')
        dispatcher=Mock(return_value={'agent_called':False});dispatcher.requires_observation=True
        supervisor=LiveSupervisor(self.book,dispatcher=dispatcher,agent_mode='supervised-host',owner_source=self.source)
        result=supervisor.step(self.observed,[],allow_entries=True)
        self.assertTrue(result['new_entries_blocked'])
        self.assertTrue(result['explicit_engineering_requests_allowed'])
        self.assertTrue(dispatcher.call_args.kwargs['allow_entries'])
        self.assertFalse(result['production_admission']['automatic_strategy_entry_allowed'])
