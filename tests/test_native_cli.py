"""Exercise the public native handoff command's real quote-normalization path."""
from contextlib import ExitStack,redirect_stdout,redirect_stderr
from datetime import datetime,timezone
import io
import json
from pathlib import Path
import tempfile
from unittest import TestCase
from unittest.mock import MagicMock,patch

from martin_agentic.cli import main


class NativeCliTests(TestCase):
    def test_review_recovery_uses_explicit_receipt_without_broker_reads_or_write_dispatch(self):
        with tempfile.TemporaryDirectory() as directory,ExitStack() as stack:
            db=Path(directory)/'book.sqlite';db.touch()
            book=stack.enter_context(patch('martin_agentic.cli.ExecutionBook')).return_value
            stack.enter_context(patch('martin_agentic.cli.NativeOperations'))
            recover=stack.enter_context(patch('martin_agentic.native_review_recovery.recover_native_review',
                return_value={'status':'REVIEW_METADATA_RECOVERED','execution_authorized':False}))
            collect=stack.enter_context(patch('martin_agentic.broker.collect_broker',side_effect=AssertionError('no broker I/O')))
            journal=stack.enter_context(patch('martin_agentic.cli.BrokerJournal',side_effect=AssertionError('no journal mutation')))
            stack.enter_context(redirect_stdout(io.StringIO()));stack.enter_context(redirect_stderr(io.StringIO()))
            code=main(['native-review-recover','--db',str(db),'--policy','tests/fixtures/policies/live_trial_2026-10-19.json',
                '--operation-id','exact-operation','--receipt-dir',directory])
            self.assertEqual(code,0);recover.assert_called_once_with(book,'exact-operation',directory)
            collect.assert_not_called();journal.assert_not_called();book.close.assert_called_once()

    def test_deterministic_review_command_uses_existing_core_action_and_receipt_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);db=root/'book.sqlite';db.touch()
            snap=root/'snapshot.json';snap.write_text('{}')
            quote=root/'quote.json';quote.write_text('{}')
            with (patch('martin_agentic.cli.ExecutionBook') as book,
                    patch('martin_agentic.cli.NativeOperations'),patch('martin_agentic.cli.BrokerJournal'),
                    patch('martin_agentic.native_review.run_native_review',return_value={'status':'WAITING_USER'}) as run,
                    redirect_stdout(io.StringIO()),redirect_stderr(io.StringIO())):
                book.return_value.actions.return_value=[{'id':'queued','kind':'protect','payload':'{}'}]
                code=main(['native-review-run','--db',str(db),'--policy','tests/fixtures/policies/live_trial_2026-10-19.json',
                    '--action-id','queued','--snapshot',str(snap),'--quote',str(quote),'--receipt-dir',str(root/'receipts')])
            self.assertEqual(code,0)
            run.assert_called_once_with(book.return_value,'queued',{},{},str(root/'receipts'))
    def test_controller_command_exposes_management_without_a_model_worker(self):
        with patch('martin_agentic.cli.session_control',return_value={'started':True,'mode':'disabled'}) as control:
            with redirect_stdout(io.StringIO()):
                result=main(['session-control','start','--db','runtime/fixture.sqlite',
                    '--policy','tests/fixtures/policies/live_trial_2026-10-19.json','--agent-mode','disabled'])
            self.assertEqual(result,0)
            self.assertEqual(control.call_args.kwargs['agent_mode'],'disabled')

    def test_controller_command_exposes_explicit_deterministic_review_mode(self):
        with patch('martin_agentic.cli.session_control',return_value={'started':True}) as control:
            with redirect_stdout(io.StringIO()):
                result=main(['session-control','start','--db','runtime/fixture.sqlite',
                    '--policy','tests/fixtures/policies/live_trial_2026-10-19.json','--agent-mode','script-review'])
        self.assertEqual(result,0);self.assertEqual(control.call_args.kwargs['agent_mode'],'script-review')

    def test_controller_command_never_exposes_execute(self):
        with patch('martin_agentic.cli.session_control') as control,redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as error:
                main(['session-control','start','--db','runtime/fixture.sqlite',
                    '--policy','tests/fixtures/policies/live_trial_2026-10-19.json','--agent-mode','execute'])
            self.assertEqual(error.exception.code,2)
            control.assert_not_called()

    def test_native_preview_reads_current_quote_with_actual_inspector_and_journals_snapshot(self):
        now=datetime.now(timezone.utc).isoformat()
        snapshot={'fixture_only':True}
        record={'tool':'get_equity_quotes','request':{'symbols':['DEMO']},'observed_at':now,
            'response':{'data':{'results':[{'quote':{'symbol':'DEMO','bid_price':'39.99','ask_price':'40',
                'venue_bid_time':now,'venue_ask_time':now,'has_traded':True,'state':'active'}}]}}}
        book=MagicMock();book.policy={'account_suffix':'0000'}
        book.actions.return_value=[{'id':'test-action','kind':'protect','payload':json.dumps({'plan':{'symbol':'DEMO'}})}]
        with tempfile.TemporaryDirectory() as directory,ExitStack() as stack:
            db=Path(directory)/'book.sqlite';db.touch()
            stack.enter_context(patch('martin_agentic.cli.ExecutionBook',return_value=book))
            journal=stack.enter_context(patch('martin_agentic.cli.BrokerJournal')).return_value
            native=stack.enter_context(patch('martin_agentic.cli.NativeOperations')).return_value
            native.preview.return_value={'fixture_only':True,'specific_order_confirmed':False}
            stack.enter_context(patch('martin_agentic.broker.collect_broker',return_value=snapshot))
            stack.enter_context(patch('martin_agentic.transport.CodexBrokerReadTransport'))
            stack.enter_context(patch('martin_agentic.transport.CodexMarketTransport'))
            reader=stack.enter_context(patch('martin_agentic.market.RobinhoodReader')).return_value
            reader.fetch.return_value=record
            output=io.StringIO();error=io.StringIO()
            with redirect_stdout(output),redirect_stderr(error):
                result=main(['native-preview','--db',str(db),'--policy','tests/fixtures/policies/live_trial_2026-10-19.json',
                    '--action-id','test-action'])
            self.assertEqual(result,0,error.getvalue())
            journal.ingest.assert_called_once_with(snapshot)
            journal.close.assert_called_once()
            quote=native.preview.call_args.kwargs['quote']
            self.assertEqual((quote['symbol'],quote['bid'],quote['ask']),('DEMO','39.99','40'))
            self.assertFalse(json.loads(output.getvalue())['specific_order_confirmed'])
            book.close.assert_called_once()
