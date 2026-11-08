"""Legacy direct commands must not open a second writer beside the daemon."""
from contextlib import ExitStack, redirect_stdout, redirect_stderr
import io
import json
from pathlib import Path
import tempfile
from unittest import TestCase
from unittest.mock import patch

from martin_agentic.cli import main
from martin_agentic.shadow import single_instance


class CliOwnerExclusionTests(TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.db=Path(self.temp.name)/'book.sqlite';self.db.touch()
        self.policy='tests/fixtures/policies/live_trial_2026-10-19.json'

    def invoke(self, arguments):
        out,err=io.StringIO(),io.StringIO()
        with redirect_stdout(out),redirect_stderr(err):code=main(arguments)
        return code,out.getvalue(),err.getvalue()

    def test_all_direct_book_paths_reject_owner_before_any_book_or_rpc(self):
        commands=[
            ['native-preview','--action-id','exact'],
            ['native-begin','--action-id','exact','--operation','place'],
            ['native-record','--operation-id','exact','--response','not-read.json'],
            ['native-review-run','--action-id','exact','--receipt-dir',self.temp.name],
            ['native-review-recover','--operation-id','exact','--receipt-dir',self.temp.name],
            ['execution-status'],['dispatch-action'],['operator-packet'],
            ['release-check','--status-file','not-read.json','--test-evidence','not-read.json'],
            ['watchdog-release','--reason','operator requested local recovery'],
            ['session-lineage','status'],
        ]
        with single_instance(self.db), ExitStack() as stack:
            book=stack.enter_context(patch('martin_agentic.cli.ExecutionBook',side_effect=AssertionError('second owner')))
            transport=stack.enter_context(patch('martin_agentic.transport.CodexBrokerReadTransport',side_effect=AssertionError('no RPC before owner')))
            for args in commands:
                with self.subTest(command=args[0]):
                    code,out,err=self.invoke(args+['--db',str(self.db),'--policy',self.policy])
                    self.assertEqual(code,2);self.assertFalse(out);self.assertIn('already owns',err)
            book.assert_not_called();transport.assert_not_called()

    def test_mutating_journal_and_review_paths_reject_before_network_or_init(self):
        commands=[['audit-record','--event','not-read.json'],['audit-verify'],
            ['audit-report','--date','2026-10-27','--output-dir',self.temp.name],
            ['observe-broker','--account-suffix','0000'],['broker-status'],
            ['session-review','--date','2026-10-27','--output-dir',self.temp.name,'--verify-broker'],
            ['session-review','--date','2026-10-27','--output-dir',self.temp.name]]
        with single_instance(self.db), ExitStack() as stack:
            audit=stack.enter_context(patch('martin_agentic.cli.AuditTrail',side_effect=AssertionError('no constructor')))
            journal=stack.enter_context(patch('martin_agentic.cli.BrokerJournal',side_effect=AssertionError('no constructor')))
            observe=stack.enter_context(patch('martin_agentic.cli.observe_broker',side_effect=AssertionError('no RPC')))
            review=stack.enter_context(patch('martin_agentic.cli.session_review',side_effect=AssertionError('no RPC')))
            for args in commands:
                with self.subTest(command=args[0]):
                    code,out,err=self.invoke(args+['--db',str(self.db)])
                    self.assertEqual(code,2);self.assertFalse(out);self.assertIn('already owns',err)
            audit.assert_not_called();journal.assert_not_called();observe.assert_not_called();review.assert_not_called()

    def test_direct_record_holds_owner_lock_through_final_close_and_releases_after_exception(self):
        with ExitStack() as stack:
            book=stack.enter_context(patch('martin_agentic.cli.ExecutionBook')).return_value
            native=stack.enter_context(patch('martin_agentic.cli.NativeOperations')).return_value
            stack.enter_context(patch('martin_agentic.cli.load_json',return_value={}))
            def check_locked():
                with self.assertRaisesRegex(ValueError,'already owns'):
                    with single_instance(self.db):pass
            def record(*args):check_locked();raise ValueError('invalid original receipt')
            book.close.side_effect=check_locked;native.record.side_effect=record
            code,_,err=self.invoke(['native-record','--operation-id','exact','--response','file.json','--db',str(self.db),'--policy',self.policy])
            self.assertEqual(code,2);self.assertIn('invalid original receipt',err)
            book.close.assert_called_once()
        with single_instance(self.db):pass

    def test_dispatch_one_has_one_lock_reserved_before_constructor_not_nested_locks(self):
        with ExitStack() as stack:
            stack.enter_context(patch('martin_agentic.cli.ExecutionBook'))
            stack.enter_context(patch('martin_agentic.cli.CodexOrderWorker'))
            dispatch=stack.enter_context(patch('martin_agentic.cli.dispatch_one',return_value={'agent_called':False}))
            code,_,err=self.invoke(['dispatch-action','--db',str(self.db),'--policy',self.policy])
            self.assertEqual(code,0,err);dispatch.assert_called_once()

    def test_paper_and_shadow_status_cannot_mutate_active_execution_database(self):
        commands=[['status'],['paper-status'],['shadow-status'],
                  ['protection-status','--as-of','2026-10-27T14:00:00Z'],
                  ['replay-paper','--events','never-read.json'],
                  ['replay-ledger','--events','never-read.json'],
                  ['shadow-replay','--events','never-read.json'],
                  ['replay-protection','--events','never-read.json']]
        with single_instance(self.db):
            for args in commands:
                with self.subTest(command=args[0]):
                    code,out,err=self.invoke(args+['--db',str(self.db)])
                    self.assertEqual(code,2);self.assertFalse(out);self.assertIn('already owns',err)
        self.assertEqual(self.db.read_bytes(),b'')

    def test_research_modes_reject_stopped_execution_book_without_schema_changes(self):
        import sqlite3
        connection=sqlite3.connect(self.db)
        connection.execute('CREATE TABLE live_state(id INTEGER,payload TEXT)')
        connection.commit();connection.close();before=self.db.read_bytes()
        for args in [['status'],['shadow-status'],['paper-status'],
                     ['protection-status','--as-of','2026-10-27T14:00:00Z'],
                     ['watch-data','--inbox',self.temp.name,'--once'],
                     ['shadow-daemon','--inbox',self.temp.name,'--once']]:
            with self.subTest(command=args[0]):
                code,out,err=self.invoke(args+['--db',str(self.db)])
                self.assertEqual(code,2);self.assertFalse(out)
                self.assertIn('RESEARCH_COMMAND_REQUIRES_SEPARATE_DATABASE',err)
        self.assertEqual(self.db.read_bytes(),before)

    def test_explicit_execute_legacy_path_does_not_bypass_production_strategy_gate(self):
        with ExitStack() as stack:
            book=stack.enter_context(patch('martin_agentic.cli.ExecutionBook')).return_value
            book.policy={'live_enabled':True};book.simulation=False
            book.actions.return_value=[{'kind':'entry','state':'READY','payload':json.dumps({'purpose':'strategy'})}]
            stack.enter_context(patch('martin_agentic.cli.CodexOrderWorker'))
            stack.enter_context(patch('martin_agentic.production_admission.ProductionAdmission.assess',return_value={
                'automatic_strategy_entry_allowed':True,'execution_authorized':True}))
            dispatch=stack.enter_context(patch('martin_agentic.cli.dispatch_one',return_value={'agent_called':False}))
            # Empty fresh account diagnostic cannot promote an unqualified entry.
            book.db.execute.return_value.fetchone.return_value=None
            code,out,err=self.invoke(['dispatch-action','--execute','--db',str(self.db),'--policy',self.policy])
            self.assertEqual(code,0,err)
            self.assertFalse(dispatch.call_args.kwargs['allow_entries'])
            self.assertTrue(json.loads(out)['automatic_strategy_entries_blocked'])
