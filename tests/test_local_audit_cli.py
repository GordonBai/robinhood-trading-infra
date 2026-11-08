from contextlib import redirect_stdout,redirect_stderr
import io
from unittest import TestCase
from unittest.mock import patch

from martin_agentic.cli import main


class LocalAuditCliTests(TestCase):
    def test_restore_cannot_use_unpinned_manifest_or_source_db(self):
        for args in [[],['--destination','new.sqlite'],
                ['--destination','new.sqlite','--expected-manifest','a'*64,'--db','original.sqlite']]:
            with (patch('martin_agentic.cli.ExecutionBook',side_effect=AssertionError('no trading ledger')),
                    patch('martin_agentic.local_audit_storage.restore_local_backup') as restore,
                    redirect_stdout(io.StringIO()),redirect_stderr(io.StringIO())):
                result=main(['local-audit','restore','--directory','backup',*args])
            self.assertEqual(result,2);restore.assert_not_called()

    def test_backup_routes_to_local_snapshot_with_no_execution_book(self):
        with (patch('martin_agentic.cli.ExecutionBook',side_effect=AssertionError('no trading ledger')),
                patch('martin_agentic.local_audit_storage.create_local_backup',return_value={'local_only':True}) as backup,
                redirect_stdout(io.StringIO()),redirect_stderr(io.StringIO())):
            result=main(['local-audit','backup','--db','original.sqlite','--directory','backup','--timeout-seconds','12'])
        self.assertEqual(result,0)
        backup.assert_called_once_with('original.sqlite','backup',timeout_seconds=12)
