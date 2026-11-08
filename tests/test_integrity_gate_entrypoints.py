"""The durable account gate is read at the real risk boundaries.

Supervisor flags are a report. The pieces that actually create or release risk
are ``ExecutionBook.propose``/``claim`` (through ``entry_gate``/``integrity_block``)
and ``AdmissionService.admit``/``settle``. Each of them must consult the same
durable ledger, or a caller that bypasses the supervisor could place new risk on
an account whose executions are not reconciled.
"""
import json
import sqlite3
import tempfile
from pathlib import Path
from unittest import TestCase

from entry_authority_fixture import TestExecutionBook as ExecutionBook
from martin_luk.instance_composition import build_admission_stack
from martin_luk.live_session import LiveSupervisor
from trading_infra.integrity_faults import ExecutionIntegrityFaults

from test_runner_result_merge import NOW, broker_order, snapshot


def policy():
    return json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())


def conflict_snapshot(base=None):
    return snapshot(executions=[broker_order(order_id='ord-b', ref='ref-b',
                                             price='41', minutes=1, base=base)],
                    base=base)


class LegacySyncGateTests(TestCase):
    """The synchronous compatibility shape must not skip reconciliation."""

    def test_the_sync_step_writes_the_fault_and_later_steps_read_it(self):
        with tempfile.TemporaryDirectory() as shared:
            path = Path(shared) / 'book.sqlite'
            book = ExecutionBook(path, policy(), clock=lambda: NOW)
            self.addCleanup(book.close)
            supervisor = LiveSupervisor(book, agent_mode='disabled',
                                        process_strategy=False)
            first = supervisor.step(snapshot(), [], allow_entries=False,
                                    allow_dispatch=False)
            self.assertNotEqual(first.get('reconciled'), False)
            self.assertFalse(supervisor._durable_integrity_blocked())

            booked = supervisor.step(
                snapshot(executions=[broker_order(price='40')]), [],
                allow_entries=False, allow_dispatch=False)
            self.assertNotEqual(booked.get('reconciled'), False)
            fault = supervisor.step(conflict_snapshot(), [], allow_entries=False,
                                    allow_dispatch=False)
            self.assertFalse(fault['reconciled'])
            self.assertFalse(fault['healthy'])
            self.assertTrue(fault['new_entries_blocked'])

            reopened = sqlite3.connect(path)
            self.addCleanup(reopened.close)
            rows = reopened.execute(
                'SELECT source,state FROM execution_integrity_faults').fetchall()
            self.assertEqual(rows, [('execution_identity_conflict', 'UNRESOLVED')])

    def test_recording_the_same_conflict_twice_leaves_one_fault(self):
        with tempfile.TemporaryDirectory() as shared:
            book = ExecutionBook(Path(shared) / 'book.sqlite', policy(), clock=lambda: NOW)
            self.addCleanup(book.close)
            supervisor = LiveSupervisor(book, agent_mode='disabled',
                                        process_strategy=False)
            value = snapshot(executions=[broker_order()])
            first = supervisor._record_integrity_fault(value)
            again = supervisor._record_integrity_fault(value)
            self.assertEqual(first, again)
            self.assertEqual(supervisor.integrity_faults.count(), 1)

    def test_the_fallback_row_is_committed_and_owned_by_no_caller(self):
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as shared:
            path = Path(shared) / 'book.sqlite'
            book = ExecutionBook(path, policy(), clock=lambda: NOW)
            self.addCleanup(book.close)
            supervisor = LiveSupervisor(book, agent_mode='disabled',
                                        process_strategy=False)
            with patch.object(ExecutionIntegrityFaults, 'record',
                              side_effect=sqlite3.OperationalError('injected ledger')):
                fault = supervisor._record_integrity_fault(snapshot())
            self.assertIsNotNone(fault)
            self.assertFalse(book.db.in_transaction,
                             'the fallback must not leave the owner transaction open')
            reopened = sqlite3.connect(path)
            self.addCleanup(reopened.close)
            self.assertEqual(
                reopened.execute('SELECT state FROM execution_integrity_faults').fetchall(),
                [('UNRESOLVED',)])
            # The same supervisor keeps reading the same account fact.
            self.assertTrue(supervisor._durable_integrity_blocked())

    def test_the_book_itself_refuses_entries_and_settlement_after_a_fault(self):
        with tempfile.TemporaryDirectory() as shared:
            path = Path(shared) / 'book.sqlite'
            book = ExecutionBook(path, policy(), clock=lambda: NOW)
            self.addCleanup(book.close)
            ExecutionIntegrityFaults(book.db, clock=lambda: NOW).record(
                source='execution_identity_conflict', account_key='acct-key',
                execution_id='same-exec')
            self.assertTrue(book.integrity_block())
            self.assertEqual(book.entry_gate(),
                             {'blocked': True,
                              'reason': 'EXECUTION_INTEGRITY_FAULT_UNRESOLVED'})
            # Claim is the last local step before a broker write. An entry may
            # not be claimed while the account is unreconciled.
            self.assertIsNone(book.claim(allow_entries=True))

    def test_admission_refuses_to_settle_on_an_unreconciled_account(self):
        with tempfile.TemporaryDirectory() as shared:
            path = Path(shared) / 'book.sqlite'
            book = ExecutionBook(path, policy(), clock=lambda: NOW)
            self.addCleanup(book.close)
            spec = policy()
            spec['strategy_instances'] = [{'instance_id': 'i1', 'plugin': 'martin'}]
            admission = build_admission_stack(book, spec, clock=lambda: NOW)
            self.assertIsNone(admission.execution_integrity_block())
            ExecutionIntegrityFaults(book.db, clock=lambda: NOW).record(
                source='execution_identity_conflict', account_key='acct-key',
                execution_id='same-exec')
            self.assertEqual(admission.execution_integrity_block(),
                             'EXECUTION_INTEGRITY_FAULT_UNRESOLVED')
            outcome = admission.settle('i1', 'trade-1', authority=book)
            self.assertFalse(outcome['settled'])
            self.assertEqual(outcome['reason'],
                             'EXECUTION_INTEGRITY_FAULT_UNRESOLVED')

    def test_the_sticky_inhibit_uses_the_known_path_when_pragma_fails(self):
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as shared:
            path = Path(shared) / 'book.sqlite'
            book = ExecutionBook(path, policy(), clock=lambda: NOW)
            self.addCleanup(book.close)
            supervisor = LiveSupervisor(book, agent_mode='disabled',
                                        process_strategy=False)

            class Broken:
                def execute(self, *args, **kwargs):
                    raise sqlite3.OperationalError('injected database failure')

            supervisor.book = SimpleNamespace(db_path=str(path), db=Broken(),
                                              clock=book.clock)
            self.assertTrue(supervisor._persist_gate_inhibit())
            inhibit = json.loads(
                (Path(shared).resolve() / 'book.sqlite.entry-inhibit.json').read_text())
            self.assertTrue(inhibit['blocked'])
            self.assertIn('EXECUTION_INTEGRITY_FAULT_UNRECORDED', inhibit['issues'])

    def test_a_new_book_connection_that_cannot_read_the_ledger_fails_closed(self):
        with tempfile.TemporaryDirectory() as shared:
            path = Path(shared) / 'book.sqlite'
            book = ExecutionBook(path, policy(), clock=lambda: NOW)
            self.addCleanup(book.close)
            broken = sqlite3.connect(path)
            self.addCleanup(broken.close)
            broken.execute('DROP TABLE IF EXISTS execution_integrity_faults')
            broken.commit()

            from unittest.mock import patch
            with patch('trading_infra.integrity_gate.unresolved_faults',
                       side_effect=sqlite3.OperationalError('injected read')):
                self.assertTrue(book.integrity_block())
                self.assertEqual(book.entry_gate()['reason'],
                                 'EXECUTION_INTEGRITY_GATE_UNAVAILABLE')
