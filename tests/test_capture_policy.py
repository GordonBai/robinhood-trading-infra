from pathlib import Path
import json
import tempfile
from unittest import TestCase
from entry_authority_fixture import TestExecutionBook as ExecutionBook
from martin_agentic.native_operations import NativeOperations

class CapturePolicyTests(TestCase):
    def test_capture_policy_blocks_all_native_writes_without_creating_intents(self):
        policy=json.loads(Path('tests/fixtures/policies/read_only_capture_2026-10-20.json').read_text())
        with tempfile.TemporaryDirectory() as folder:
            book=ExecutionBook(Path(folder)/'capture.sqlite',policy,simulation=True)
            try:
                operations=NativeOperations(book)
                before=book.audit.verify()['events_verified']
                self.assertTrue(book.entry_gate()['blocked'])
                with self.assertRaisesRegex(ValueError,'CAPTURE_ONLY_POLICY_READ_ONLY'):
                    book._check_entry_gate()
                for op in ('place','cancel'):
                    with self.subTest(operation=op):
                        with self.assertRaisesRegex(ValueError,'CAPTURE_ONLY_POLICY_READ_ONLY'):
                            operations.begin('absent',op,{})
                        with self.assertRaisesRegex(ValueError,'CAPTURE_ONLY_POLICY_READ_ONLY'):
                            book.prepare_external_action('absent',{},operation_id='blocked-'+op,operation=op)
                self.assertEqual(book.db.execute('SELECT count(*) FROM external_operations').fetchone()[0],0)
                self.assertEqual(book.audit.verify()['events_verified'],before)
            finally:book.close()
    def test_capture_mode_is_bound_to_ledger_and_requires_a_boolean(self):
        policy=json.loads(Path('tests/fixtures/policies/read_only_capture_2026-10-20.json').read_text())
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'capture.sqlite';book=ExecutionBook(path,policy,simulation=True);book.close()
            with self.assertRaisesRegex(ValueError,'EXECUTION_POLICY_OR_MODE_CHANGED'):
                ExecutionBook(path,{**policy,'capture_only':False},simulation=True)
            with self.assertRaisesRegex(ValueError,'INVALID_CAPTURE_ONLY_POLICY'):
                ExecutionBook(Path(folder)/'bad.sqlite',{**policy,'capture_only':'true'},simulation=True)
