"""Public Host mailbox CLI: real files, no trading ledger or broker I/O."""
from contextlib import redirect_stdout,redirect_stderr
from datetime import datetime,timedelta,timezone
import io
import json
from pathlib import Path
import tempfile
from unittest import TestCase
from unittest.mock import patch
import uuid

from martin_agentic.audit import digest
from martin_agentic.cli import main
from martin_agentic.host_mailbox import read_private_json,write_private_json


class HostExecutionCliTests(TestCase):
    def test_engineering_exit_command_only_queues_mailbox_intent(self):
        code,output,error=self.invoke('engineering-exit','--trade-id','engineering-fixture','--request-id',self.rid)
        self.assertEqual(code,0,error)
        request=json.loads(output)
        self.assertEqual(request['operation'],'engineering-exit')
        self.assertEqual(request['trade_id'],'engineering-fixture')
        self.assertFalse(Path(self.binding['db']).exists())

    def test_standalone_engineering_exit_cannot_open_db_owned_by_daemon(self):
        from martin_agentic.shadow import single_instance
        db=self.root/'owned.sqlite';db.touch()
        with (single_instance(db),patch('martin_agentic.cli.ExecutionBook',side_effect=AssertionError('second writer')),
                redirect_stdout(io.StringIO()),redirect_stderr(io.StringIO())):
            code=main(['engineering-exit-request','--db',str(db),
                '--policy','tests/fixtures/policies/live_trial_2026-10-19.json','--trade-id','engineering-fixture'])
        self.assertEqual(code,2)

    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)/'mailbox'
        self.now=datetime.now(timezone.utc)
        self.binding={'db':str(self.root/'execution-never-opened.sqlite'),'account':'••••0000',
            'account_key':'a'*64,'policy_sha256':'b'*64,'source_fingerprint':'c'*64,'epoch':str(uuid.uuid4())}
        self.manifest={'schema_version':1,'running':True,'binding':self.binding,'published_at':self.now.isoformat()}
        write_private_json(self.root/'manifest.json',self.manifest)
        self.rid=str(uuid.uuid4());self.oid='native-'+str(uuid.uuid4())

    def invoke(self,mode,*arguments):
        output=io.StringIO();error=io.StringIO()
        with (patch('martin_agentic.cli.ExecutionBook',side_effect=AssertionError('no execution DB')),
                patch('martin_agentic.cli.BrokerJournal',side_effect=AssertionError('no broker journal')),
                patch('sqlite3.connect',side_effect=AssertionError('no SQLite')),
                redirect_stdout(output),redirect_stderr(error)):
            result=main(['host-execution',mode,'--directory',str(self.root),*arguments])
        return result,output.getvalue(),error.getvalue()

    def prepare_packet(self):
        packet={'operation_id':self.oid,'action_id':'protect-action','operation':'place',
            'tool':'place_equity_order','account':'••••0000','account_key':'a'*64,
            'parameters':{'symbol':'DEMO','side':'sell','quantity':'1','type':'stop_market','stop_price':'39'},
            'execution_authorized_by_packet':False,'specific_order_confirmed':False}
        prepared={'schema_version':1,'status':'PREPARED','request_id':self.rid,'binding':self.binding,
            'packet':packet,'packet_sha256':digest(packet),'prepared_at':self.now.isoformat(),
            'expires_at':(self.now+timedelta(seconds=15)).isoformat(),
            'execution_authorized':False,'broker_effect_verified':False}
        write_private_json(self.root/'packets'/(self.rid+'.json'),prepared)
        manifest={**self.manifest,'pending_operation_id':self.oid,
            'actions':[{'id':'protect-action','state':'RUNNING','kind':'protect','parameters':packet['parameters']}]}
        (self.root/'manifest.json').write_text(json.dumps(manifest))
        return prepared

    def test_status_reads_manifest_without_constructing_execution_book(self):
        code,output,error=self.invoke('status')
        self.assertEqual(code,0,error);self.assertEqual(json.loads(output),self.manifest)
        self.assertFalse(Path(self.binding['db']).exists())

    def test_request_and_result_use_exact_request_action_and_operation(self):
        code,output,error=self.invoke('request','--request-id',self.rid,'--action-id','protect-action','--operation','place')
        self.assertEqual(code,0,error);request=json.loads(output)
        self.assertEqual(request['request_id'],self.rid)
        self.assertEqual(request['action_id'],'protect-action');self.assertEqual(request['operation'],'place')
        self.assertEqual(request,read_private_json(self.root/'requests'/(self.rid+'.json')))
        code,output,error=self.invoke('result','--request-id',self.rid)
        self.assertEqual(code,0,error);self.assertEqual(json.loads(output)['status'],'PENDING_OWNER')
        prepared=self.prepare_packet()
        code,output,error=self.invoke('result','--request-id',self.rid)
        self.assertEqual(code,0,error);self.assertEqual(json.loads(output),prepared)
        self.assertFalse(json.loads(output)['execution_authorized'])
        self.assertFalse(Path(self.binding['db']).exists())

    def test_receipt_stdout_is_hash_only_while_private_receipt_retains_original_account_scope(self):
        self.prepare_packet()
        response={'data':{'order':{'id':'native-returned-order','account_number':'fixture-account-0000'}}}
        source=self.root/'original-response.json';write_private_json(source,response)
        code,output,error=self.invoke('receipt','--request-id',self.rid,'--response',str(source))
        self.assertEqual(code,0,error);result=json.loads(output)
        self.assertEqual(result['status'],'HOST_RECEIPT_PERSISTED')
        self.assertEqual(result['request_id'],self.rid);self.assertEqual(result['response_sha256'],digest(response))
        self.assertNotIn('account_number',output);self.assertNotIn('fixture-account-0000',output)
        self.assertNotIn('response',result);self.assertFalse(result['execution_authorized'])
        receipt=self.root/'receipts'/(self.oid+'.json')
        self.assertEqual(read_private_json(receipt)['response'],response)
        self.assertEqual(receipt.stat().st_mode&0o777,0o600)
        self.assertFalse(Path(self.binding['db']).exists())

    def test_missing_required_handoff_fields_fail_without_any_ledger(self):
        for mode,args in [('request',()),('result',()),('receipt',('--request-id',self.rid))]:
            with self.subTest(mode=mode):
                code,output,error=self.invoke(mode,*args)
                self.assertEqual(code,2);self.assertFalse(output);self.assertIn('HOST_',json.loads(error)['error'])
        self.assertFalse(Path(self.binding['db']).exists())

    def test_session_control_parser_accepts_supervised_host_without_starting_process(self):
        with (patch('martin_agentic.cli.session_control',return_value={'started':True}) as control,
                patch('martin_agentic.cli.ExecutionBook',side_effect=AssertionError('no execution DB')),
                redirect_stdout(io.StringIO()),redirect_stderr(io.StringIO())):
            code=main(['session-control','start','--db',str(self.root/'fixture.sqlite'),
                '--policy','tests/fixtures/policies/live_trial_2026-10-19.json','--agent-mode','supervised-host'])
        self.assertEqual(code,0);self.assertEqual(control.call_args.kwargs['agent_mode'],'supervised-host')

    def test_live_session_selects_explicit_local_worker_factory_only(self):
        from types import SimpleNamespace
        symbols=['DEMO']+['S'+str(i) for i in range(9)]
        with tempfile.TemporaryDirectory() as directory:
            with (patch('martin_agentic.live_session.run_live_session',
                        return_value={'healthy':True,'stopped':True}) as run,
                    patch('martin_agentic.cli.ExecutionBook',side_effect=AssertionError('no execution DB')),
                    redirect_stdout(io.StringIO()),redirect_stderr(io.StringIO())):
                code=main(['live-session','--db',str(self.root/'fixture.sqlite'),
                    '--policy','tests/fixtures/policies/live_trial_2026-10-19.json',
                    '--agent-mode','supervised-host','--worker-adapter','local-worker-review',
                    '--cycles','1','--symbols',*symbols])
            self.assertEqual(code,0)
            factory=run.call_args.kwargs['host_wake_adapter_factory']
            self.assertTrue(callable(factory))
            book=SimpleNamespace(policy={'account_suffix':'0000','capture_only':False})
            adapter=factory(book, output_dir=Path(directory), policy=book.policy)
            try:
                self.assertTrue(adapter.capability['local_worker_event'])
                self.assertTrue(adapter.capability['review_only'])
                self.assertFalse(adapter.capability['official_queue'])
            finally:
                adapter.close()
        with (patch('martin_agentic.live_session.run_live_session') as run,
              redirect_stdout(io.StringIO()),redirect_stderr(io.StringIO())):
            code=main(['live-session','--db','not-created.sqlite',
                '--policy','tests/fixtures/policies/live_trial_2026-10-19.json',
                '--agent-mode','disabled','--worker-adapter','local-worker-review',
                '--symbols',*symbols])
        self.assertEqual(code,2);run.assert_not_called()

    def test_session_control_parser_forwards_worker_adapter_selection(self):
        with (patch('martin_agentic.cli.session_control',return_value={'started':True}) as control,
                redirect_stdout(io.StringIO()),redirect_stderr(io.StringIO())):
            code=main(['session-control','start','--db',str(self.root/'fixture.sqlite'),
                '--policy','tests/fixtures/policies/live_trial_2026-10-19.json',
                '--agent-mode','supervised-host','--worker-adapter','local-worker-review'])
        self.assertEqual(code,0)
        self.assertEqual(control.call_args.kwargs['worker_adapter'],'local-worker-review')

    def test_live_session_parser_accepts_supervised_host_without_real_session(self):
        symbols=['DEMO']+['S'+str(i) for i in range(9)]
        with (patch('martin_agentic.live_session.run_live_session',return_value={'healthy':True,'stopped':True}) as run,
                patch('martin_agentic.cli.ExecutionBook',side_effect=AssertionError('no execution DB')),
                redirect_stdout(io.StringIO()),redirect_stderr(io.StringIO())):
            code=main(['live-session','--db',str(self.root/'fixture.sqlite'),
                '--policy','tests/fixtures/policies/live_trial_2026-10-19.json','--agent-mode','supervised-host',
                '--cycles','1','--symbols',*symbols])
        self.assertEqual(code,0);self.assertEqual(run.call_args.kwargs['agent_mode'],'supervised-host')
        self.assertEqual(run.call_args.kwargs['symbols'],symbols)
