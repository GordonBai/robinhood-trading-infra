"""Account-level execution-integrity faults survive every session boundary."""
import sqlite3
import tempfile
from datetime import datetime, timezone
from unittest import TestCase

from trading_infra.integrity_faults import ExecutionIntegrityFaults

AT = datetime(2026, 10, 31, 14, 0, tzinfo=timezone.utc)


class IntegrityFaultTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = self.temp.name + '/faults.sqlite'
        self.db = sqlite3.connect(self.path, isolation_level='')
        self.addCleanup(self.db.close)
        self.ledger = ExecutionIntegrityFaults(self.db, clock=lambda: AT)

    def test_a_fault_blocks_until_it_is_explicitly_reconciled(self):
        self.assertFalse(self.ledger.blocks())
        self.ledger.record(source='execution_identity_conflict', account_key='acct',
                           execution_id='same-exec', evidence={'orders': ['ord-a']})
        self.assertTrue(self.ledger.blocks())
        rows = self.ledger.unresolved()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['state'], 'UNRESOLVED')
        self.assertEqual(rows[0]['evidence'], {'orders': ['ord-a']})

    def test_an_exact_replay_is_idempotent(self):
        for _ in range(3):
            self.ledger.record(source='execution_identity_conflict', account_key='acct',
                               execution_id='same-exec')
        self.assertEqual(self.ledger.count(), 1)

    def test_a_different_execution_is_its_own_fault(self):
        self.ledger.record(source='execution_identity_conflict', account_key='acct',
                           execution_id='exec-1')
        self.ledger.record(source='execution_identity_conflict', account_key='acct',
                           execution_id='exec-2')
        self.assertEqual(self.ledger.count(), 2)

    def test_unknown_identity_is_recorded_as_unknown_not_guessed(self):
        self.ledger.record(source='reconciliation_storage_failed')
        row = self.ledger.unresolved()[0]
        self.assertIsNone(row['account_key'])
        self.assertIsNone(row['execution_id'])

    def test_a_new_connection_reads_the_same_fault(self):
        self.ledger.record(source='execution_identity_conflict', account_key='acct',
                           execution_id='same-exec')
        reopened = sqlite3.connect(self.path, isolation_level='')
        self.addCleanup(reopened.close)
        again = ExecutionIntegrityFaults(reopened, clock=lambda: AT)
        self.assertTrue(again.blocks())
        self.assertEqual(again.count(), 1)
