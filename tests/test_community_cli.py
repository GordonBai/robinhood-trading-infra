from contextlib import redirect_stdout,redirect_stderr
import io
import json
from pathlib import Path
import tempfile
from unittest import TestCase
from unittest.mock import patch
from martin_agentic.cli import main


class CommunityCliTests(TestCase):
    def invoke(self,args):
        out=io.StringIO();err=io.StringIO()
        with redirect_stdout(out),redirect_stderr(err):code=main(args)
        return code,json.loads(out.getvalue()) if out.getvalue() else None,err.getvalue()

    def test_healthy_capture_exits_successfully_without_admitting_entries(self):
        status={'healthy':False,'read_only_capture':True,'read_only_capture_healthy':True,
            'agent_mode':'disabled','new_entries_blocked':True,'attention_required':False}
        with patch('martin_agentic.live_session.run_live_session',return_value=status):
            code,result,error=self.invoke(['live-session','--db','not-created.sqlite',
                '--policy','tests/fixtures/policies/read_only_capture_2026-10-27.json',
                '--symbols',*[str(i) for i in range(10)],'--agent-mode','disabled'])
        self.assertEqual(code,0,error);self.assertFalse(result['healthy'])
        self.assertTrue(result['new_entries_blocked'])

    def test_capture_fault_or_invalid_mode_is_not_hidden_by_capture_health(self):
        status={'healthy':False,'read_only_capture':True,'read_only_capture_healthy':True,
            'agent_mode':'disabled','new_entries_blocked':True,'attention_required':False}
        for bad in ({'attention_required':True},{'read_only_capture_healthy':False},
                    {'market_failure':{'code':'MARKET_QUOTE_STALE'}},{'agent_mode':'execute'}):
            with self.subTest(bad=bad),patch('martin_agentic.live_session.run_live_session',return_value=status|bad):
                code,_,_=self.invoke(['live-session','--db','not-created.sqlite',
                    '--policy','tests/fixtures/policies/read_only_capture_2026-10-27.json',
                    '--symbols',*[str(i) for i in range(10)],'--agent-mode','disabled'])
            self.assertEqual(code,2)

    def test_capability_requirement_is_an_exit_code_gate_without_order_authority(self):
        with tempfile.TemporaryDirectory() as directory:
            p=Path(directory)/'requirements.json';p.write_text(json.dumps({'atomic_entry_bracket':True}))
            code,r,_=self.invoke(['capabilities','--requirements',str(p)])
            self.assertEqual(code,2);self.assertFalse(r['admission']['eligible']);self.assertFalse(r['admission']['authorization_granted'])

    def test_registered_missing_research_dataset_is_actionable_cli_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            code,r,_=self.invoke(['research-replay','--manifest',str(Path(directory)/'missing.json'),'--output-dir',directory])
            self.assertEqual(code,2);self.assertEqual(r['status'],'FAILED')
            self.assertTrue(Path(r['report_path']).is_file())

    def test_fault_matrix_command_cannot_claim_live_acceptance(self):
        with tempfile.TemporaryDirectory() as directory:
            code,r,error=self.invoke(['fault-rehearsal','--output-dir',directory])
            self.assertEqual(code,0,error);self.assertTrue(r['passed'])
            self.assertTrue(r['synthetic']);self.assertFalse(r['robinhood_live_acceptance'])

    def test_recovery_does_not_create_a_missing_database(self):
        with tempfile.TemporaryDirectory() as directory:
            p=Path(directory)/'missing.sqlite'
            code,_,error=self.invoke(['recovery-case','status','--db',str(p),'--policy','tests/fixtures/policies/live_trial_2026-10-19.json','--case-id','unknown'])
            self.assertEqual(code,2);self.assertIn('does not exist',error);self.assertFalse(p.exists())

    def test_required_watchdog_flag_forwards_exact_policy_path_without_running_a_session(self):
        policy='tests/fixtures/policies/live_trial_2026-10-19.json'
        with patch('martin_agentic.live_session.run_live_session',return_value={'diagnostic':True}) as run:
            code,_,error=self.invoke(['live-session','--db','not-created.sqlite','--policy',policy,
                '--symbols','AAPL','AMD','HOOD','NVDA','TSLA','META','PLTR','SOFI','IONQ','RKLB',
                '--require-watchdog'])
        self.assertEqual(code,0,error);self.assertTrue(run.call_args.kwargs['require_watchdog'])
        self.assertEqual(run.call_args.kwargs['watchdog_policy_path'],policy)

    def test_script_driver_rehearsal_is_offline_and_model_free(self):
        with tempfile.TemporaryDirectory() as directory:
            code,r,error=self.invoke(['script-execution','rehearse',
                '--policy','tests/fixtures/policies/live_trial_2026-10-21.json','--output-dir',directory])
            self.assertEqual(code,0,error)
            self.assertTrue(r['passed']);self.assertTrue(r['synthetic'])
            self.assertEqual(r['final_phase'],'FLAT_VERIFIED')
            self.assertEqual(r['model_calls'],0);self.assertEqual(r['broker_calls'],0)
            self.assertEqual(r['synthetic_operations'],7)
            self.assertFalse(r['unattended_ready'])
            self.assertTrue(Path(r['report_path']).is_file())

    def test_script_rehearsal_cannot_accept_a_real_database_argument(self):
        with tempfile.TemporaryDirectory() as directory:
            target=Path(directory)/'must-not-create.sqlite'
            code,_,error=self.invoke(['script-execution','rehearse',
                '--policy','tests/fixtures/policies/live_trial_2026-10-21.json','--db',str(target),
                '--output-dir',directory])
            self.assertEqual(code,2);self.assertIn('NO_REAL_DB',error)
            self.assertFalse(target.exists())

    def test_script_shadow_blocks_capability_without_mutating_book(self):
        from martin_agentic.rehearsal import _Fixture
        with tempfile.TemporaryDirectory() as directory:
            db=Path(directory)/'fixture.sqlite'
            policy='tests/fixtures/policies/live_trial_2026-10-21.json'
            fixture=_Fixture(db,json.loads(Path(policy).read_text()))
            try:
                fixture.propose();before=db.read_bytes()
                code,r,error=self.invoke(['script-execution','shadow','--db',str(db),'--policy',policy])
                self.assertEqual(code,2,error)
                self.assertEqual(r['status'],'BLOCKED_CAPABILITY')
                self.assertEqual(r['book_mutations'],0)
                self.assertFalse(r['execution_authorized'])
                self.assertEqual(db.read_bytes(),before)
            finally:fixture.book.close()

    def test_bounded_rehearsal_records_real_child_timeout_and_synthetic_protection(self):
        with tempfile.TemporaryDirectory() as directory:
            code,r,error=self.invoke(['script-execution','rehearse-bounded',
                '--policy','tests/fixtures/policies/live_trial_2026-10-21.json','--output-dir',directory,
                '--io-timeout-seconds','1'])
            self.assertEqual(code,0,error)
            self.assertTrue(r['passed']);self.assertTrue(r['synthetic'])
            self.assertEqual(r['entry_child_started_evidence']['delay_seconds'],120)
            self.assertTrue(r['entry_child_terminal_evidence']['reaped'])
            self.assertFalse(r['unattended_ready'])
            self.assertGreater(float(r['synthetic_observed_protection_delay_seconds']),.2)
            self.assertLess(float(r['synthetic_observed_protection_delay_seconds']),30)
            self.assertEqual(r['real_broker_writes'],0)
