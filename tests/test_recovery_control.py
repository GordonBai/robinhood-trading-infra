"""Recovery must never open a second writer beside the trading supervisor."""
from contextlib import ExitStack, contextmanager, redirect_stderr, redirect_stdout
import io
import json
from pathlib import Path
import tempfile
from unittest import TestCase
from unittest.mock import MagicMock, patch

from martin_agentic.recovery_control import run_recovery_case
from martin_agentic.shadow import single_instance
from martin_agentic.cli import main


class RecoveryControlTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.source = Path(self.temp.name) / 'book.sqlite'
        self.source.touch()
        self.policy = {'account_suffix': '0000'}
        self.risk_check = patch('martin_agentic.recovery_control._validate_existing_risk')
        self.risk_check.start(); self.addCleanup(self.risk_check.stop)

    def test_held_owner_lock_blocks_before_book_or_risk_sidecar_is_opened(self):
        with single_instance(self.source), patch('martin_agentic.recovery_control.ExecutionBook') as factory:
            with self.assertRaisesRegex(ValueError, 'already owns'):
                run_recovery_case('status', self.source, self.policy, case_id='case')
        factory.assert_not_called()
        self.assertEqual(self.source.read_bytes(), b'')

    def test_symlink_alias_owner_and_canonical_recovery_share_exclusion(self):
        alias = self.source.with_name('alias.sqlite')
        alias.symlink_to(self.source)
        with single_instance(alias), patch('martin_agentic.recovery_control.ExecutionBook') as book:
            with self.assertRaisesRegex(ValueError, 'already owns'):
                run_recovery_case('status', self.source, self.policy, case_id='case')
            book.assert_not_called()

    def test_hardlinked_book_is_rejected_before_any_writer(self):
        import os
        alias = self.source.with_name('hardlink.sqlite')
        os.link(self.source, alias)
        with patch('martin_agentic.recovery_control.ExecutionBook') as book:
            with self.assertRaisesRegex(ValueError, 'hardlinks'):
                run_recovery_case('status', alias, self.policy, case_id='case')
            book.assert_not_called()

    def test_standalone_audit_restore_cannot_initialize_missing_risk_history(self):
        self.risk_check.stop()
        with patch('martin_agentic.recovery_control.ExecutionBook') as book:
            with self.assertRaisesRegex(ValueError, 'EXISTING_RISK_SIDECAR_REQUIRED'):
                run_recovery_case('status', self.source, self.policy, case_id='case')
            book.assert_not_called()
        self.assertFalse(self.source.with_name('account-risk-0000.sqlite').exists())

    def test_wrong_account_risk_sidecar_rejected_without_rewriting_it(self):
        import sqlite3
        from martin_agentic.audit import digest
        self.risk_check.stop()
        policy = {'account_suffix':'0000', 'account_capital_usd':'500',
            'stop_new_entries_at_daily_equity_loss_usd':'10', 'project_max_drawdown_fraction':'0.30'}
        connection = sqlite3.connect(self.source)
        connection.execute('CREATE TABLE live_state(id INTEGER,payload TEXT)')
        connection.execute('INSERT INTO live_state VALUES(1,?)', (json.dumps({
            'policy_hash':digest(policy),'simulation':False,'account_key':'expected-account'}),))
        connection.commit(); connection.close()
        risk = self.source.with_name('account-risk-0000.sqlite')
        connection = sqlite3.connect(risk)
        connection.execute('CREATE TABLE account_risk(id INTEGER,payload TEXT)')
        connection.execute('INSERT INTO account_risk VALUES(1,?)', (json.dumps({
            'terms':digest(policy),'simulation':False,'account_key':'different-account'}),))
        connection.commit(); connection.close()
        before = risk.read_bytes()
        with patch('martin_agentic.recovery_control.ExecutionBook') as book:
            with self.assertRaisesRegex(ValueError, 'EXISTING_RISK_BINDING_INVALID'):
                run_recovery_case('status', self.source, policy, case_id='case')
            book.assert_not_called()
        self.assertEqual(risk.read_bytes(), before)

    def test_lock_lasts_through_case_operation_and_book_close(self):
        events = []
        @contextmanager
        def guard(path):
            events.append('lock')
            yield
            events.append('unlock')
        with ExitStack() as stack:
            stack.enter_context(patch('martin_agentic.recovery_control.single_instance', guard))
            factory = stack.enter_context(patch('martin_agentic.recovery_control.ExecutionBook'))
            factory.side_effect = lambda *a: events.append('book') or MagicMock()
            cases = stack.enter_context(patch('martin_agentic.recovery_control.RecoveryCases')).return_value
            cases.get.side_effect = lambda *a: events.append('get') or {'status': 'OPEN'}
            # A real instance lets close ordering be observed without SQLite setup.
            book = MagicMock(); book.close.side_effect = lambda: events.append('close')
            factory.side_effect = lambda *a: events.append('book') or book
            result = run_recovery_case('status', self.source, self.policy, case_id='exact-case')
        self.assertEqual(events, ['lock', 'book', 'get', 'close', 'unlock'])
        self.assertEqual(result['status'], 'OPEN')

    def test_case_exception_closes_book_and_releases_lock(self):
        with patch('martin_agentic.recovery_control.ExecutionBook') as factory, \
                patch('martin_agentic.recovery_control.RecoveryCases') as cases:
            cases.return_value.get.side_effect = ValueError('invalid audit binding')
            with self.assertRaisesRegex(ValueError, 'invalid audit'):
                run_recovery_case('status', self.source, self.policy, case_id='bad')
            factory.return_value.close.assert_called_once()
        with single_instance(self.source):
            pass

    def test_verify_collects_fresh_full_approval_scope_before_case_verification(self):
        with ExitStack() as stack:
            book = stack.enter_context(patch('martin_agentic.recovery_control.ExecutionBook')).return_value
            book.policy = self.policy; book.pending_approval_ids.return_value = ['pending']
            cases = stack.enter_context(patch('martin_agentic.recovery_control.RecoveryCases')).return_value
            cases.required_approval_ids.return_value = ['old-terminal', 'pending']
            transport = stack.enter_context(patch('martin_agentic.recovery_control.CodexBrokerReadTransport'))
            collect = stack.enter_context(patch('martin_agentic.recovery_control.collect_broker', return_value={'fresh': True}))
            journal = stack.enter_context(patch('martin_agentic.recovery_control.BrokerJournal')).return_value
            cases.verify.return_value = {'status': 'BROKER_SAFE_VERIFIED'}
            run_recovery_case('verify', self.source, self.policy, case_id='case')
            collect.assert_called_once_with(transport.return_value.__enter__.return_value,
                account_suffix='0000', approval_ids=['old-terminal', 'pending'])
            journal.ingest.assert_called_once_with({'fresh': True})
            journal.close.assert_called_once()
            cases.verify.assert_called_once_with('case', {'fresh': True})
            book.close.assert_called_once()

    def test_missing_database_and_invalid_mode_never_create_book(self):
        with patch('martin_agentic.recovery_control.ExecutionBook') as book:
            with self.assertRaises(FileNotFoundError):
                run_recovery_case('status', self.source.with_name('missing'), self.policy)
            with self.assertRaisesRegex(ValueError, 'INVALID_RECOVERY'):
                run_recovery_case('reset-all', self.source, self.policy)
            book.assert_not_called()

    def test_cli_delegates_recovery_without_constructing_unlocked_book(self):
        for command,mode in ((c,m) for c in ('recovery-case','manual-takeover') for m in ('status','dispose-protection-incident')):
            with self.subTest(command=command,mode=mode), ExitStack() as stack:
                book = stack.enter_context(patch('martin_agentic.cli.ExecutionBook', side_effect=AssertionError('unlocked book')))
                call = stack.enter_context(patch('martin_agentic.recovery_control.run_recovery_case', return_value={'status': 'OPEN'}))
                stack.enter_context(redirect_stdout(io.StringIO())); stack.enter_context(redirect_stderr(io.StringIO()))
                code = main([command, mode, '--db', str(self.source), '--policy', 'tests/fixtures/policies/live_trial_2026-10-19.json', '--case-id', 'exact-case'])
                self.assertEqual(code, 0); book.assert_not_called()
                self.assertEqual(call.call_args.kwargs['manual_takeover'], command == 'manual-takeover')
                self.assertEqual(call.call_args.kwargs['case_id'], 'exact-case')
