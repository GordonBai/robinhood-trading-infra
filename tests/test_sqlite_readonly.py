"""No-write SQLite sidecar behavior for status/source/checkpoint helpers."""
import json
import os
from pathlib import Path
import shutil
import sqlite3
import tempfile
from unittest import TestCase

from martin_agentic import runtime_checkpoint, source_evidence
from martin_agentic.sqlite_readonly import SQLiteReadOnlyError, readonly_uri
from test_baseline_cli import run_cli, tree_state


class SQLiteReadOnlyTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.root.chmod(0o700)

    def _wal_database(self, path):
        db = sqlite3.connect(path)
        self.addCleanup(db.close)
        self.assertEqual(db.execute('PRAGMA journal_mode=WAL').fetchone()[0],
                         'wal')
        db.execute('CREATE TABLE t(x INTEGER)')
        db.execute('INSERT INTO t VALUES(1)')
        db.commit()
        return db

    def test_checkpointed_wal_header_no_sidecars_reads_without_shm(self):
        path = self.root / 'checkpointed.sqlite'
        db = self._wal_database(path)
        db.close()
        self.addCleanup(lambda: db.close())
        self.assertFalse(Path(str(path) + '-wal').exists())
        self.assertFalse(Path(str(path) + '-shm').exists())
        uri = readonly_uri(path)
        self.assertIn('immutable=1', uri)
        plain = self.root / 'plain.sqlite'
        plain_db = sqlite3.connect(plain)
        try:
            plain_db.execute('CREATE TABLE t(x INTEGER)')
            plain_db.commit()
        finally:
            plain_db.close()
        self.assertNotIn('immutable=1', readonly_uri(plain))
        for helper in (
                lambda: runtime_checkpoint._db(path, tables=('t',)),
                lambda: source_evidence._open_ro(path, tables=('t',))):
            connection = helper()
            try:
                self.assertEqual(
                    connection.execute('SELECT x FROM t').fetchone()[0], 1)
            finally:
                connection.close()
            self.assertFalse(Path(str(path) + '-shm').exists())
            self.assertFalse(Path(str(path) + '-wal').exists())
        before = tree_state(self.root)
        result = run_cli(
            'historical-baseline', 'status', '--db', str(path),
            '--root', str(self.root), '--isolated-root')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)['status'], 'ABSENT')
        self.assertFalse(Path(str(path) + '-shm').exists())
        self.assertEqual(tree_state(self.root), before)

    def test_empty_wal_sidecars_are_ignored_without_creation_or_change(self):
        path = self.root / 'empty-sidecars.sqlite'
        db = self._wal_database(path)
        db.close()
        self.addCleanup(lambda: db.close())
        for suffix in ('-wal', '-shm'):
            Path(str(path) + suffix).touch()
        before = tree_state(self.root)
        connection = source_evidence._open_ro(path, tables=('t',))
        try:
            self.assertEqual(
                connection.execute('SELECT count(*) FROM t').fetchone()[0], 1)
        finally:
            connection.close()
        self.assertEqual(tree_state(self.root), before)
        result = run_cli(
            'historical-baseline', 'status', '--db', str(path),
            '--root', str(self.root), '--isolated-root')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)['status'], 'ABSENT')
        self.assertEqual(tree_state(self.root), before)

    def test_nonempty_wal_is_rejected_before_creating_shm(self):
        source = self.root / 'writer.sqlite'
        target = self.root / 'inspect.sqlite'
        db = self._wal_database(source)
        try:
            wal = Path(str(source) + '-wal')
            self.assertGreater(wal.stat().st_size, 0)
            shutil.copy2(source, target)
            shutil.copy2(wal, Path(str(target) + '-wal'))
            before = tree_state(self.root)
            with self.assertRaises(SQLiteReadOnlyError) as raised:
                readonly_uri(target)
            self.assertEqual(raised.exception.code,
                             'SQLITE_SIDECAR_NOT_CONVERGED')
            with self.assertRaises(ValueError):
                runtime_checkpoint._db(target)
            with self.assertRaises(source_evidence.SourceEvidenceError):
                source_evidence._open_ro(target)
            self.assertFalse(Path(str(target) + '-shm').exists())
            self.assertEqual(tree_state(self.root), before)

            result = run_cli(
                'historical-baseline', 'status', '--db', str(target),
                '--root', str(self.root), '--isolated-root')
            self.assertEqual(result.returncode, 4, result.stderr)
            output = json.loads(result.stdout)
            self.assertEqual(output['status'], 'INVALID')
            self.assertIn('CLI_WAL_NOT_CONVERGED', output['blockers'])
            self.assertFalse(Path(str(target) + '-shm').exists())
            self.assertEqual(tree_state(self.root), before)
        finally:
            db.close()

    def test_active_writer_and_journal_sidecars_are_rejected(self):
        active = self.root / 'active.sqlite'
        db = self._wal_database(active)
        try:
            self.assertTrue(Path(str(active) + '-wal').stat().st_size)
            self.assertTrue(Path(str(active) + '-shm').exists())
            before = tree_state(self.root)
            with self.assertRaises(SQLiteReadOnlyError):
                readonly_uri(active)
            result = run_cli(
                'historical-baseline', 'status', '--db', str(active),
                '--root', str(self.root), '--isolated-root')
            self.assertEqual(result.returncode, 4, result.stderr)
            self.assertEqual(json.loads(result.stdout)['status'], 'INVALID')
            self.assertEqual(tree_state(self.root), before)
        finally:
            db.close()

        journal = self.root / 'journal.sqlite'
        plain = sqlite3.connect(journal)
        try:
            plain.execute('CREATE TABLE t(x INTEGER)')
            plain.commit()
        finally:
            plain.close()
        Path(str(journal) + '-journal').write_bytes(b'hot journal')
        before = tree_state(self.root)
        with self.assertRaises(SQLiteReadOnlyError):
            readonly_uri(journal)
        with self.assertRaises(ValueError):
            runtime_checkpoint._db(journal)
        with self.assertRaises(source_evidence.SourceEvidenceError):
            source_evidence._open_ro(journal)
        result = run_cli(
            'historical-baseline', 'status', '--db', str(journal),
            '--root', str(self.root), '--isolated-root')
        self.assertEqual(result.returncode, 4, result.stderr)
        self.assertEqual(json.loads(result.stdout)['status'], 'INVALID')
        self.assertEqual(tree_state(self.root), before)
