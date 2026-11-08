"""Read-only source evidence provider tests. Isolated real-API fixtures only."""
import asyncio
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
import warnings
from unittest import TestCase
from unittest.mock import patch

from martin_agentic.audit import canonical, digest
from martin_agentic.broker import BrokerJournal
from entry_authority_fixture import TestExecutionBook as ExecutionBook
from martin_agentic.host_execution import HostExecutionBridge
from martin_agentic.native_operations import NativeOperations
from martin_agentic.qualification import source_fingerprint
from martin_agentic.runtime_checkpoint import runtime_generation
from martin_agentic.session_lineage import (
    ZERO_HASH, _terms, bootstrap_session_lineage, enroll_session_lineage)
from martin_agentic import source_evidence
from martin_agentic.source_evidence import (
    SourceEvidenceError, SourceEvidenceProvider)
from test_broker import FakeBroker, snapshot
from test_historical_baseline import historical_snapshot
from trading_infra.instance_accounting import InstanceAccounting


AT = '2026-10-19T14:00:00+00:00'
NOW = datetime.fromisoformat(AT)
POLICY = json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
CURRENT_POLICY = {
    **POLICY, 'session_date': '2026-10-19',
    'prior_execution_evidence_db': 'runtime/old.sqlite',
}
LOCK_CHILD = '''
import fcntl,os,sys
fd=os.open(sys.argv[1],os.O_RDWR|os.O_NOFOLLOW)
try:
    fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
except BlockingIOError:
    print("BLOCKED",flush=True);sys.exit(3)
print("ACQUIRED",flush=True)
'''
LOCK_HOLDER = '''
import fcntl,os,sys,time
fd=os.open(sys.argv[1],os.O_RDWR|os.O_NOFOLLOW)
fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
print("HELD",flush=True)
time.sleep(60)
'''


def _acquire_once(lock):
    return subprocess.run(
        [sys.executable, '-c', LOCK_CHILD, str(lock)],
        capture_output=True, text=True, timeout=10)


def _start_holder(lock):
    process = subprocess.Popen(
        [sys.executable, '-c', LOCK_HOLDER, str(lock)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    line = process.stdout.readline().strip()
    if line != 'HELD':
        process.kill()
        process.communicate()
        raise AssertionError('lock holder did not start: ' + line)
    return process


def _stop_holder(process):
    process.terminate()
    process.communicate(timeout=5)


def _open_fd_count():
    fd_root = Path('/dev/fd')
    if not fd_root.is_dir():
        raise unittest.SkipTest('operating-system fd table unavailable')
    return len(os.listdir(fd_root))


class SourceFixture:
    """Actual ExecutionBook, risk, lineage and Host APIs in an isolated root."""

    def __init__(self, root):
        self.parent = Path(root)
        self.root = self.parent / 'runtime'
        self.root.mkdir(parents=True, mode=0o700)
        self.root.chmod(0o700)
        self.prior_policy = dict(POLICY)
        self.policy = dict(CURRENT_POLICY)
        self.prior = self.root / 'old.sqlite'
        self.execution = self.root / 'next.sqlite'
        self.risk = self.root / 'account-risk-0000.sqlite'
        self.host = self.root / 'host'
        self.lock = Path(str(self.execution) + '.lock')
        self.account_key = None
        self.now = NOW
        with patch('martin_agentic.session_lineage.CANONICAL_RUNTIME',
                   self.root):
            self._build()
        self.lock.touch(mode=0o600)
        self._refresh_pins()

    def _fresh(self, book, path):
        current = snapshot(FakeBroker(), self.now.isoformat())
        journal = BrokerJournal(path)
        try:
            journal.ingest(current)
        finally:
            journal.close()
        book.reconcile(current)
        return current

    def _build(self):
        old = ExecutionBook(self.prior, self.prior_policy,
                            clock=lambda: self.now)
        next_book = None
        bridge = None
        try:
            self._fresh(old, self.prior)
            self.account_key = old.state()['account_key']
            bootstrap_session_lineage(
                self.prior, self.prior_policy, current_db=self.prior,
                current_policy=self.prior_policy, clock=lambda: self.now)
            next_book = ExecutionBook(
                self.execution, self.policy, clock=lambda: self.now)
            self._fresh(next_book, self.execution)
            enroll_session_lineage(
                self.execution, self.policy, predecessor_db=self.prior,
                current_db=self.execution, current_policy=self.policy,
                clock=lambda: self.now)
            NativeOperations(next_book)
            InstanceAccounting(next_book.db)
            next_book.db.execute(
                'CREATE TABLE IF NOT EXISTS execution_intents('
                'intent_id TEXT PRIMARY KEY)')
            next_book.db.commit()
            self.host.mkdir(mode=0o700)
            (self.host / 'requests').mkdir()
            (self.host / 'deliveries').mkdir()
            bridge = HostExecutionBridge(next_book, self.host)
            bridge.close()
            bridge = None
            (self.host / 'requests' / 'request.json').write_text(
                '{"request_id":"synthetic"}')
            (self.host / 'deliveries' / 'index.json').write_text('{}')
        finally:
            if bridge is not None:
                bridge.close()
            if next_book is not None:
                next_book.close()
            old.close()

    def _file_pin(self, path):
        return hashlib.sha256(Path(path).read_bytes()).hexdigest()

    def _head(self):
        db = sqlite3.connect(self.risk)
        try:
            return db.execute(
                'SELECT entry_hash FROM session_lineage_head WHERE id=1'
            ).fetchone()[0]
        finally:
            db.close()

    def _refresh_pins(self):
        self.generation = runtime_generation(
            self.execution, self.host, self.risk, account_suffix='0000')
        binding = json.loads(
            (self.host / 'manifest.json').read_text())['binding']
        self.epoch = binding['epoch']
        self.head = self._head()
        self.terms = _terms(self.policy)
        self.expected = {
            'source_fingerprint': source_fingerprint(),
            'policy_sha256': digest(self.policy),
            'account_key': self.account_key, 'account_suffix': '0000',
            'execution_db_sha256': self._file_pin(self.execution),
            'risk_sidecar_sha256': self._file_pin(self.risk),
            'host_manifest_sha256': self._file_pin(
                self.host / 'manifest.json'),
            'lineage_head_sha256': self.head,
            'state_generation': self.generation,
            'terms_sha256': self.terms, 'host_epoch': self.epoch,
        }

    def provider(self, **changes):
        supplied = changes.pop('expected', None)
        expected = dict(self.expected if supplied is None else supplied)
        return SourceEvidenceProvider(
            root=self.root, isolated=True, execution_db=self.execution,
            risk_db=self.risk, host_dir=self.host, expected=expected,
            policy=changes.pop('policy', self.policy), **changes)

    def events(self):
        db = sqlite3.connect(self.risk)
        try:
            return [json.loads(row[0]) for row in db.execute(
                'SELECT payload FROM session_lineage_events ORDER BY sequence')]
        finally:
            db.close()

    def write_lineage(self, events):
        previous = ZERO_HASH
        rows = []
        for sequence, event in enumerate(events, 1):
            encoded = canonical(event)
            entry_hash = digest({'sequence': sequence,
                                 'previous_hash': previous,
                                 'event': event})
            rows.append((sequence, encoded, previous, entry_hash))
            previous = entry_hash
        db = sqlite3.connect(self.risk)
        try:
            db.execute('DROP TRIGGER IF EXISTS session_lineage_no_update')
            db.execute('DROP TRIGGER IF EXISTS session_lineage_no_delete')
            db.execute('DELETE FROM session_lineage_events')
            db.execute('DELETE FROM session_lineage_head')
            db.executemany(
                'INSERT INTO session_lineage_events VALUES(?,?,?,?)', rows)
            db.execute('INSERT INTO session_lineage_head VALUES(1,?,?)',
                       (len(rows), previous))
            db.commit()
        finally:
            db.close()
        self._refresh_pins()

    def change_policy_terms(self, new_policy, *, keep_expected_terms):
        state_db = sqlite3.connect(self.execution)
        try:
            row = state_db.execute(
                'SELECT payload FROM live_state WHERE id=1').fetchone()
            state = json.loads(row[0])
            state['policy_hash'] = digest(new_policy)
            state_db.execute(
                'UPDATE live_state SET payload=? WHERE id=1',
                (canonical(state),))
            state_db.commit()
        finally:
            state_db.close()
        manifest_path = self.host / 'manifest.json'
        manifest = json.loads(manifest_path.read_text())
        manifest['binding']['policy_sha256'] = digest(new_policy)
        manifest_path.write_text(json.dumps(manifest))
        expected = dict(self.expected)
        expected['policy_sha256'] = digest(new_policy)
        expected['terms_sha256'] = (
            self.terms if keep_expected_terms else _terms(new_policy))
        expected['execution_db_sha256'] = self._file_pin(self.execution)
        expected['host_manifest_sha256'] = self._file_pin(manifest_path)
        return expected


class SourceEvidenceTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.fixture = SourceFixture(self.temp.name)

    def test_checkpointed_wal_sources_read_without_creating_sidecars(self):
        for path in (self.fixture.execution, self.fixture.risk):
            db = sqlite3.connect(path)
            try:
                self.assertEqual(
                    db.execute('PRAGMA journal_mode=WAL').fetchone()[0], 'wal')
                db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
            finally:
                db.close()
            self.assertFalse(Path(str(path) + '-wal').exists())
            self.assertFalse(Path(str(path) + '-shm').exists())
        self.fixture._refresh_pins()
        before_files = {p.relative_to(self.fixture.root): p.read_bytes()
                        for p in self.fixture.root.rglob('*') if p.is_file()}
        before_mtimes = {p: p.stat().st_mtime_ns
                         for p in self.fixture.root.rglob('*') if p.is_file()}
        result = self.fixture.provider().read()
        self.assertEqual(result['status'], 'READY', result['blockers'])
        after_files = {p.relative_to(self.fixture.root): p.read_bytes()
                       for p in self.fixture.root.rglob('*') if p.is_file()}
        after_mtimes = {p: p.stat().st_mtime_ns
                        for p in self.fixture.root.rglob('*') if p.is_file()}
        self.assertEqual(before_files, after_files)
        self.assertEqual(before_mtimes, after_mtimes)

        for suffix in ('-wal', '-shm'):
            Path(str(self.fixture.execution) + suffix).touch()
        before_files = {p.relative_to(self.fixture.root): p.read_bytes()
                        for p in self.fixture.root.rglob('*') if p.is_file()}
        before_mtimes = {p: p.stat().st_mtime_ns
                         for p in self.fixture.root.rglob('*') if p.is_file()}
        result = self.fixture.provider().read()
        self.assertEqual(result['status'], 'READY', result['blockers'])
        self.assertEqual(
            before_files,
            {p.relative_to(self.fixture.root): p.read_bytes()
             for p in self.fixture.root.rglob('*') if p.is_file()})
        self.assertEqual(
            before_mtimes,
            {p: p.stat().st_mtime_ns
             for p in self.fixture.root.rglob('*') if p.is_file()})

    def test_positive_two_session_measured_evidence_and_no_writes(self):
        before_files = {p.relative_to(self.fixture.root): p.read_bytes()
                        for p in self.fixture.root.rglob('*') if p.is_file()}
        before_mtimes = {p: p.stat().st_mtime_ns
                         for p in self.fixture.root.rglob('*') if p.is_file()}
        before_tables = {}
        for path in (self.fixture.execution, self.fixture.risk,
                     self.fixture.prior):
            db = sqlite3.connect(path)
            try:
                before_tables[path] = sorted(
                    row[0] for row in db.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"))
            finally:
                db.close()
        result = self.fixture.provider().read()
        self.assertEqual(result['status'], 'READY', result['blockers'])
        evidence = result['evidence']
        self.assertEqual(evidence['source']['execution_db_sha256'],
                         self.fixture.expected['execution_db_sha256'])
        self.assertEqual(evidence['risk']['sidecar_sha256'],
                         self.fixture.expected['risk_sidecar_sha256'])
        self.assertEqual(evidence['host']['manifest_sha256'],
                         self.fixture.expected['host_manifest_sha256'])
        self.assertEqual(evidence['lineage']['head_sha256'],
                         self.fixture.expected['lineage_head_sha256'])
        self.assertEqual(evidence['lineage']['sequence'], 2)
        self.assertEqual(evidence['host']['generation'],
                         self.fixture.generation)
        self.assertTrue(result['provenance']['owner_lock_stopped'])
        self.assertTrue(
            result['provenance']['owner_lock_held_across_read'])
        self.assertFalse(result['execution_authorized'])
        self.assertFalse(result['production_ready'])
        after_files = {p.relative_to(self.fixture.root): p.read_bytes()
                       for p in self.fixture.root.rglob('*') if p.is_file()}
        after_mtimes = {p: p.stat().st_mtime_ns
                        for p in self.fixture.root.rglob('*') if p.is_file()}
        self.assertEqual(before_files, after_files)
        self.assertEqual(before_mtimes, after_mtimes)
        for path, names in before_tables.items():
            db = sqlite3.connect(path)
            try:
                self.assertEqual(names, sorted(
                    row[0] for row in db.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'")))
            finally:
                db.close()
        # A successful read releases the owner lock for the next process.
        acquired = _acquire_once(self.fixture.lock)
        self.assertEqual(acquired.returncode, 0, acquired.stdout)

    def test_external_pins_wrong_or_missing_block(self):
        for field in ('execution_db_sha256', 'risk_sidecar_sha256',
                      'host_manifest_sha256', 'lineage_head_sha256',
                      'state_generation', 'source_fingerprint', 'account_key',
                      'terms_sha256', 'host_epoch'):
            with self.subTest(field=field):
                expected = dict(self.fixture.expected)
                expected[field] = '0' * 64 if field not in {
                    'host_epoch', 'account_key', 'account_suffix'
                } else 'wrong'
                result = self.fixture.provider(expected=expected).read()
                self.assertEqual(result['status'], 'BLOCKED')
                self.assertIsNone(result['evidence'])

        expected = dict(self.fixture.expected)
        expected.pop('state_generation')
        result = self.fixture.provider(expected=expected).read()
        self.assertEqual(result['status'], 'BLOCKED')
        self.assertIn('SOURCE_EXPECTED_MISSING', result['blockers'])

    def test_owner_running_wal_symlink_and_schema_missing(self):
        holder = _start_holder(self.fixture.lock)
        try:
            result = self.fixture.provider().read()
            self.assertIn('SOURCE_OWNER_RUNNING', result['blockers'])
            self.assertIsNone(result['evidence'])
        finally:
            _stop_holder(holder)
        # Restart after the other process exits is clean and does not reuse
        # any reader-held descriptor.
        result = self.fixture.provider().read()
        self.assertEqual(result['status'], 'READY', result['blockers'])

        wal = Path(str(self.fixture.execution) + '-wal')
        wal.write_bytes(b'x')
        result = self.fixture.provider().read()
        self.assertIn('SOURCE_WAL_NOT_CONVERGED', result['blockers'])
        self.assertFalse(Path(str(self.fixture.execution) + '-shm').exists())
        wal.unlink()

        link = Path(str(self.fixture.risk) + '.link')
        link.symlink_to(self.fixture.risk)
        with self.assertRaises(ValueError):
            SourceEvidenceProvider(
                root=self.fixture.root, isolated=True,
                execution_db=self.fixture.execution, risk_db=link,
                host_dir=self.fixture.host, expected=self.fixture.expected,
                policy=self.fixture.policy)

        db = sqlite3.connect(self.fixture.execution)
        try:
            db.execute('DROP TABLE live_state')
            db.commit()
        finally:
            db.close()
        expected = dict(self.fixture.expected)
        expected['execution_db_sha256'] = self.fixture._file_pin(
            self.fixture.execution)
        result = self.fixture.provider(expected=expected).read()
        self.assertIn('SOURCE_TABLE_MISSING', result['blockers'])

    def test_sticky_risk_is_reported_and_not_cleared(self):
        db = sqlite3.connect(self.fixture.risk)
        try:
            state = json.loads(db.execute(
                'SELECT payload FROM account_risk WHERE id=1').fetchone()[0])
            state['blocked'] = True
            state['daily_halted'] = True
            state['reasons'] = ['SYNTHETIC_STICKY']
            db.execute('UPDATE account_risk SET payload=? WHERE id=1',
                       (canonical(state),))
            db.commit()
        finally:
            db.close()
        before = self.fixture.risk.read_bytes()
        self.fixture._refresh_pins()
        result = self.fixture.provider().read()
        self.assertEqual(result['status'], 'READY', result['blockers'])
        self.assertIs(result['evidence']['risk']['blocked'], True)
        self.assertIs(result['provenance']['risk_state']['blocked'], True)
        self.assertTrue(result['evidence']['risk']['daily_halted'])
        self.assertEqual(self.fixture.risk.read_bytes(), before)

    def test_missing_lock_blocks_and_is_not_created(self):
        self.fixture.lock.unlink()
        result = self.fixture.provider().read()
        self.assertEqual(result['status'], 'BLOCKED')
        self.assertIn('SOURCE_OWNER_LOCK_REPLACED', result['blockers'])
        self.assertFalse(self.fixture.lock.exists())

    def test_exclusive_scope_shares_one_lock_across_nested_reads(self):
        provider = self.fixture.provider()
        with provider.exclusive_scope() as owner:
            result = provider.read()
            self.assertEqual(result['status'], 'READY', result['blockers'])
            blocked = _acquire_once(self.fixture.lock)
            self.assertEqual(blocked.returncode, 3, blocked.stdout)
            provider.verify_scope(owner)
        acquired = _acquire_once(self.fixture.lock)
        self.assertEqual(acquired.returncode, 0, acquired.stdout)
        with self.assertRaises(SourceEvidenceError) as raised:
            provider.verify_scope(owner)
        self.assertEqual(raised.exception.code, 'SOURCE_SCOPE_INVALID')

    def test_closed_scope_cannot_be_reused_by_delayed_async_task(self):
        provider = self.fixture.provider()

        async def scenario():
            ready = asyncio.Event()
            with provider.exclusive_scope():
                async def delayed():
                    await ready.wait()
                    return provider.read()
                task = asyncio.create_task(delayed())
            holder = _start_holder(self.fixture.lock)
            try:
                ready.set()
                return await task
            finally:
                _stop_holder(holder)

        result = asyncio.run(scenario())
        self.assertEqual(result['status'], 'BLOCKED')
        self.assertIn('SOURCE_SCOPE_CLOSED', result['blockers'])

    @unittest.skipUnless(hasattr(os, 'fork'), 'fork required')
    def test_forked_child_cannot_reuse_parent_scope(self):
        provider = self.fixture.provider()
        with provider.exclusive_scope():
            read_fd, write_fd = os.pipe()
            with warnings.catch_warnings():
                warnings.simplefilter('ignore', DeprecationWarning)
                pid = os.fork()
            if pid == 0:
                os.close(read_fd)
                try:
                    result = provider.read()
                    payload = {'status': result['status'],
                               'blockers': result['blockers']}
                except BaseException as exc:
                    payload = {'status': 'EXCEPTION',
                               'blockers': [type(exc).__name__]}
                os.write(write_fd, json.dumps(payload).encode())
                os._exit(0)
            os.close(write_fd)
            data = os.read(read_fd, 65536)
            os.close(read_fd)
            os.waitpid(pid, 0)
        payload = json.loads(data)
        self.assertEqual(payload['status'], 'BLOCKED')
        self.assertIn('SOURCE_SCOPE_CROSS_PROCESS', payload['blockers'])

    def test_exposed_fd_close_reopen_same_inode_cannot_take_private_lock(self):
        provider = self.fixture.provider()
        with provider.exclusive_scope() as owner:
            public_fd = owner['fd']
            identity = owner['identity']
            os.close(public_fd)
            reopened = os.open(self.fixture.lock, os.O_RDWR | os.O_NOFOLLOW)
            try:
                self.assertEqual(reopened, public_fd)
                info = os.fstat(reopened)
                self.assertEqual(info.st_dev, identity['dev'])
                self.assertEqual(info.st_ino, identity['ino'])
                blocked = _acquire_once(self.fixture.lock)
                self.assertEqual(blocked.returncode, 3, blocked.stdout)
                provider.verify_scope(owner)
                result = provider.read()
                self.assertEqual(result['status'], 'READY', result['blockers'])
            finally:
                os.close(reopened)
        acquired = _acquire_once(self.fixture.lock)
        self.assertEqual(acquired.returncode, 0, acquired.stdout)

    def test_exposed_duplicate_does_not_keep_lock_after_scope(self):
        provider = self.fixture.provider()
        with provider.exclusive_scope() as owner:
            duplicate = owner['fd']
        try:
            acquired = _acquire_once(self.fixture.lock)
            self.assertEqual(acquired.returncode, 0, acquired.stdout)
        finally:
            os.close(duplicate)

    def test_lost_private_lock_without_other_holder_is_not_reacquired(self):
        provider = self.fixture.provider()
        reopened = None
        with provider.exclusive_scope() as owner:
            os.close(owner._lock_fd)
            reopened = os.open(self.fixture.lock, os.O_RDWR | os.O_NOFOLLOW)
            owner._lock_fd = reopened
            result = provider.read()
            self.assertEqual(result['status'], 'BLOCKED')
            self.assertIn('SOURCE_SCOPE_LOCK_LOST', result['blockers'])
        try:
            os.fstat(reopened)
        except OSError:
            self.fail('cleanup closed a reused foreign fd')
        os.close(reopened)
        acquired = _acquire_once(self.fixture.lock)
        self.assertEqual(acquired.returncode, 0, acquired.stdout)

    def test_lost_private_lock_is_blocked_and_not_closed_as_foreign_fd(self):
        provider = self.fixture.provider()
        reopened = None
        with provider.exclusive_scope() as owner:
            os.close(owner._lock_fd)
            reopened = os.open(self.fixture.lock, os.O_RDWR | os.O_NOFOLLOW)
            owner._lock_fd = reopened
            holder = _start_holder(self.fixture.lock)
            try:
                result = provider.read()
                self.assertEqual(result['status'], 'BLOCKED')
                self.assertIn('SOURCE_SCOPE_LOCK_LOST', result['blockers'])
                with self.assertRaises(SourceEvidenceError) as raised:
                    provider.verify_scope(owner)
                self.assertIn(raised.exception.code,
                              {'SOURCE_SCOPE_LOCK_LOST',
                               'SOURCE_SCOPE_INVALID'})
            finally:
                _stop_holder(holder)
        # Scope cleanup must not close a descriptor that was reopened after its
        # private descriptor was lost.
        try:
            os.fstat(reopened)
        except OSError:
            self.fail('cleanup closed a reused foreign fd')
        os.close(reopened)
        acquired = _acquire_once(self.fixture.lock)
        self.assertEqual(acquired.returncode, 0, acquired.stdout)

    @unittest.skipUnless(hasattr(os, 'fork'), 'fork required')
    def test_fork_child_scope_exit_never_unlocks_parent_ofd(self):
        provider = self.fixture.provider()
        cm = provider.exclusive_scope()
        owner = cm.__enter__()
        try:
            parent_fds = _open_fd_count()
            for mode in ('normal', 'read', 'exception'):
                read_fd, write_fd = os.pipe()
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore', DeprecationWarning)
                    pid = os.fork()
                if pid == 0:
                    os.close(read_fd)
                    payload = {'mode': mode}
                    lock_fd = owner._lock_fd
                    before = _open_fd_count()
                    try:
                        if mode == 'read':
                            result = provider.read()
                            payload['read_status'] = result['status']
                            payload['read_blockers'] = result['blockers']
                        if mode == 'exception':
                            try:
                                raise RuntimeError('child unwind')
                            except BaseException:
                                payload['suppressed'] = cm.__exit__(
                                    *sys.exc_info())
                        else:
                            payload['suppressed'] = cm.__exit__(
                                None, None, None)
                        try:
                            os.fstat(lock_fd)
                            payload['fd_closed'] = False
                        except OSError:
                            payload['fd_closed'] = True
                        payload['fds_after'] = _open_fd_count()
                        payload['fds_before'] = before
                    except BaseException as exc:
                        payload['error'] = type(exc).__name__
                    os.write(write_fd, json.dumps(payload).encode())
                    os._exit(0)
                os.close(write_fd)
                data = os.read(read_fd, 65536)
                os.close(read_fd)
                os.waitpid(pid, 0)
                child = json.loads(data)
                self.assertNotIn('error', child)
                self.assertTrue(child['fd_closed'])
                self.assertFalse(child['suppressed'])
                self.assertLessEqual(child['fds_after'], child['fds_before'])
                if mode == 'read':
                    self.assertEqual(child['read_status'], 'BLOCKED')
                    self.assertIn('SOURCE_SCOPE_CROSS_PROCESS',
                                  child['read_blockers'])
                # Child closing its inherited reference must not release the
                # parent's shared OFD lock or mutate parent scope state.
                self.assertIs(owner['active'], True)
                self.assertEqual(_open_fd_count(), parent_fds)
                competing = _acquire_once(self.fixture.lock)
                self.assertEqual(competing.returncode, 3, competing.stdout)
                parent_result = provider.read()
                self.assertEqual(parent_result['status'], 'READY',
                                 parent_result['blockers'])
        finally:
            cm.__exit__(None, None, None)
        acquired = _acquire_once(self.fixture.lock)
        self.assertEqual(acquired.returncode, 0, acquired.stdout)

    def test_parent_exception_scope_exit_releases_normally(self):
        provider = self.fixture.provider()
        before_fds = _open_fd_count()
        cm = provider.exclusive_scope()
        cm.__enter__()
        try:
            raise RuntimeError('parent unwind')
        except BaseException:
            suppressed = cm.__exit__(*sys.exc_info())
        self.assertFalse(suppressed)
        self.assertLessEqual(_open_fd_count(), before_fds)
        acquired = _acquire_once(self.fixture.lock)
        self.assertEqual(acquired.returncode, 0, acquired.stdout)

    def test_repeated_owner_running_rejections_do_not_leak_fds(self):
        holder = _start_holder(self.fixture.lock)
        try:
            before_fds = _open_fd_count()
            before_tree = {
                p.relative_to(self.fixture.root): (p.read_bytes(),
                                                   p.stat().st_mtime_ns)
                for p in self.fixture.root.rglob('*') if p.is_file()}
            for _ in range(50):
                result = self.fixture.provider().read()
                self.assertEqual(result['status'], 'BLOCKED')
                self.assertIn('SOURCE_OWNER_RUNNING', result['blockers'])
            self.assertEqual(_open_fd_count(), before_fds)
            self.assertEqual(
                before_tree,
                {p.relative_to(self.fixture.root): (p.read_bytes(),
                                                    p.stat().st_mtime_ns)
                 for p in self.fixture.root.rglob('*') if p.is_file()})
        finally:
            _stop_holder(holder)
        result = self.fixture.provider().read()
        self.assertEqual(result['status'], 'READY', result['blockers'])
        acquired = _acquire_once(self.fixture.lock)
        self.assertEqual(acquired.returncode, 0, acquired.stdout)

    def test_post_flock_pid_rejection_closes_opened_fd(self):
        pid_file = Path(str(self.fixture.execution) + '.pid')
        pid_file.write_text(str(os.getpid()))
        before_fds = _open_fd_count()
        with patch('martin_agentic.source_evidence.process_matches',
                   return_value=True):
            result = self.fixture.provider().read()
        self.assertEqual(result['status'], 'BLOCKED')
        self.assertIn('SOURCE_OWNER_RUNNING', result['blockers'])
        self.assertEqual(_open_fd_count(), before_fds)
        acquired = _acquire_once(self.fixture.lock)
        self.assertEqual(acquired.returncode, 0, acquired.stdout)
        pid_file.unlink()

    def test_second_process_cannot_take_lock_during_read(self):
        original = source_evidence._read_source_state
        attempts = []

        def observed(path, expected):
            attempts.append(_acquire_once(self.fixture.lock))
            return original(path, expected)

        with patch.object(
                source_evidence, '_read_source_state', observed):
            result = self.fixture.provider().read()
        self.assertEqual(result['status'], 'READY', result['blockers'])
        self.assertEqual(len(attempts), 1)
        self.assertEqual(attempts[0].returncode, 3,
                         attempts[0].stdout + attempts[0].stderr)

    def test_lock_replacement_releases_private_fd_and_old_lock(self):
        original = source_evidence._read_source_state
        holder = None
        replaced = Path(str(self.fixture.lock) + '.replaced')
        counts = []

        def replace_lock(path, expected):
            nonlocal holder
            self.fixture.lock.rename(replaced)
            self.fixture.lock.touch(mode=0o600)
            holder = _start_holder(self.fixture.lock)
            counts.append(_open_fd_count())
            return original(path, expected)

        try:
            with patch.object(
                    source_evidence, '_read_source_state', replace_lock):
                result = self.fixture.provider().read()
            self.assertIn('SOURCE_OWNER_LOCK_REPLACED', result['blockers'])
            self.assertEqual(result['status'], 'BLOCKED')
            self.assertIsNone(result['evidence'])
            self.assertLessEqual(_open_fd_count(), counts[0])
        finally:
            if holder is not None:
                _stop_holder(holder)
        # The reader still owned the old fd after path replacement, so cleanup
        # must have released its lock/close; both old and current lock are free.
        old = _acquire_once(replaced)
        self.assertEqual(old.returncode, 0, old.stdout)
        current = _acquire_once(self.fixture.lock)
        self.assertEqual(current.returncode, 0, current.stdout)

    def test_lock_rename_without_new_holder_still_releases_old_lock(self):
        original = source_evidence._read_source_state
        replaced = Path(str(self.fixture.lock) + '.renamed')

        def rename_lock(path, expected):
            self.fixture.lock.rename(replaced)
            self.fixture.lock.touch(mode=0o600)
            return original(path, expected)

        before_fds = _open_fd_count()
        with patch.object(source_evidence, '_read_source_state', rename_lock):
            result = self.fixture.provider().read()
        self.assertEqual(result['status'], 'BLOCKED')
        self.assertIn('SOURCE_OWNER_LOCK_REPLACED', result['blockers'])
        self.assertLessEqual(_open_fd_count(), before_fds)
        old = _acquire_once(replaced)
        self.assertEqual(old.returncode, 0, old.stdout)
        current = _acquire_once(self.fixture.lock)
        self.assertEqual(current.returncode, 0, current.stdout)

    def test_lock_unlink_replacement_closes_private_fd(self):
        original = source_evidence._read_source_state
        holder = None
        counts = []

        def unlink_lock(path, expected):
            nonlocal holder
            self.fixture.lock.unlink()
            self.fixture.lock.touch(mode=0o600)
            holder = _start_holder(self.fixture.lock)
            counts.append(_open_fd_count())
            return original(path, expected)

        try:
            with patch.object(
                    source_evidence, '_read_source_state', unlink_lock):
                result = self.fixture.provider().read()
            self.assertEqual(result['status'], 'BLOCKED')
            self.assertIn('SOURCE_OWNER_LOCK_REPLACED', result['blockers'])
            self.assertLessEqual(_open_fd_count(), counts[0])
        finally:
            if holder is not None:
                _stop_holder(holder)
        current = _acquire_once(self.fixture.lock)
        self.assertEqual(current.returncode, 0, current.stdout)

    def test_exception_exit_releases_lock(self):
        before_fds = _open_fd_count()
        with patch.object(
                source_evidence, '_read_source_state',
                side_effect=sqlite3.OperationalError('database is locked')):
            result = self.fixture.provider().read()
        self.assertEqual(result['status'], 'BLOCKED')
        self.assertIn('SOURCE_READ_FAILED', result['blockers'])
        self.assertEqual(_open_fd_count(), before_fds)
        acquired = _acquire_once(self.fixture.lock)
        self.assertEqual(acquired.returncode, 0, acquired.stdout)

    def test_sqlite_timeout_exit_releases_lock(self):
        before_fds = _open_fd_count()
        blocker = sqlite3.connect(self.fixture.execution)
        blocker.execute('BEGIN EXCLUSIVE')
        try:
            result = self.fixture.provider().read()
            self.assertEqual(result['status'], 'BLOCKED')
            self.assertIn('SOURCE_SQLITE_INVALID', result['blockers'])
        finally:
            blocker.rollback()
            blocker.close()
        self.assertLessEqual(_open_fd_count(), before_fds)
        acquired = _acquire_once(self.fixture.lock)
        self.assertEqual(acquired.returncode, 0, acquired.stdout)

    def test_policy_terms_and_account_suffix_are_recomputed(self):
        stale_policy = {**self.fixture.policy, 'account_capital_usd': '1000'}
        stale_expected = self.fixture.change_policy_terms(
            stale_policy, keep_expected_terms=True)
        result = self.fixture.provider(
            expected=stale_expected, policy=stale_policy).read()
        self.assertIn('SOURCE_POLICY_TERMS_MISMATCH', result['blockers'])

    def test_stale_risk_terms_block_even_when_expected_terms_follow_policy(self):
        new_policy = {**self.fixture.policy, 'account_capital_usd': '1000'}
        expected = self.fixture.change_policy_terms(
            new_policy, keep_expected_terms=False)
        result = self.fixture.provider(
            expected=expected, policy=new_policy).read()
        self.assertIn('SOURCE_RISK_BINDING_MISMATCH', result['blockers'])
        self.assertIsNone(result['evidence'])

    def test_policy_account_suffix_mismatch_blocks(self):
        wrong = {**self.fixture.policy, 'account_suffix': '9999'}
        expected = dict(self.fixture.expected)
        expected['policy_sha256'] = digest(wrong)
        expected['terms_sha256'] = _terms(wrong)
        result = self.fixture.provider(
            expected=expected, policy=wrong).read()
        self.assertIn('SOURCE_POLICY_ACCOUNT_SUFFIX_MISMATCH',
                      result['blockers'])

    def test_semantic_mismatch_and_lineage_mismatch(self):
        for changes in (
                {'account_key': 'wrong-account'},
                {'policy_sha256': '1' * 64},
                {'terms_sha256': '2' * 64},
                {'host_epoch': 'wrong-epoch'},
                {'lineage_head_sha256': '3' * 64}):
            expected = dict(self.fixture.expected)
            expected.update(changes)
            result = self.fixture.provider(expected=expected).read()
            self.assertEqual(result['status'], 'BLOCKED', changes)

        db = sqlite3.connect(self.fixture.risk)
        try:
            db.execute('DROP TRIGGER IF EXISTS session_lineage_no_update')
            db.execute("UPDATE session_lineage_events SET payload='{}'")
            db.commit()
        finally:
            db.close()
        expected = dict(self.fixture.expected)
        expected['risk_sidecar_sha256'] = self.fixture._file_pin(
            self.fixture.risk)
        result = self.fixture.provider(expected=expected).read()
        self.assertIn('SOURCE_LINEAGE_CHAIN_INVALID', result['blockers'])

    def test_historical_policy_hash_is_bound_to_its_record(self):
        events = self.fixture.events()
        events[0]['session']['policy_sha256'] = '0' * 64
        self.fixture.write_lineage(events)
        result = self.fixture.provider().read()
        self.assertIn('SOURCE_LINEAGE_RECORD_POLICY_INVALID',
                      result['blockers'])

    def test_historical_session_cannot_change_terms(self):
        events = self.fixture.events()
        historical = {
            **self.fixture.prior_policy, 'account_capital_usd': '999'}
        events[0]['session']['policy'] = historical
        events[0]['session']['policy_sha256'] = digest(historical)
        self.fixture.write_lineage(events)
        result = self.fixture.provider().read()
        self.assertIn('SOURCE_LINEAGE_RECORD_TERMS_MISMATCH',
                      result['blockers'])

    def test_lineage_audit_prefix_head_and_current_registration(self):
        events = self.fixture.events()
        events[0]['session']['audit_head_hash'] = ZERO_HASH
        self.fixture.write_lineage(events)
        result = self.fixture.provider().read()
        self.assertIn('SOURCE_LINEAGE_AUDIT_PREFIX_MISMATCH',
                      result['blockers'])

    def test_lineage_kind_and_predecessor_are_verified(self):
        events = self.fixture.events()
        events[0]['kind'] = 'ENROLL'
        self.fixture.write_lineage(events)
        result = self.fixture.provider().read()
        self.assertIn('SOURCE_LINEAGE_CHAIN_INVALID', result['blockers'])

        events = self.fixture.events()
        events[0]['kind'] = 'BOOTSTRAP'
        events[1]['session']['predecessor_db'] = None
        self.fixture.write_lineage(events)
        result = self.fixture.provider().read()
        self.assertIn('SOURCE_LINEAGE_PREDECESSOR_MISMATCH',
                      result['blockers'])

    def test_duplicate_registered_source_blocks(self):
        events = self.fixture.events()
        events[1]['session'] = dict(events[0]['session'])
        events[1]['current_source_db'] = events[0]['session']['db']
        events[1]['current_source_policy_sha256'] = events[0]['session'][
            'policy_sha256']
        self.fixture.write_lineage(events)
        result = self.fixture.provider().read()
        self.assertIn('SOURCE_LINEAGE_DUPLICATE', result['blockers'])

    def test_historical_source_view_mismatch_blocks(self):
        db = sqlite3.connect(self.fixture.prior)
        try:
            state = json.loads(db.execute(
                'SELECT payload FROM live_state WHERE id=1').fetchone()[0])
            state['policy_hash'] = '0' * 64
            db.execute('UPDATE live_state SET payload=? WHERE id=1',
                       (canonical(state),))
            db.commit()
        finally:
            db.close()
        result = self.fixture.provider().read()
        self.assertIn('SOURCE_LINEAGE_SOURCE_VIEW_INVALID',
                      result['blockers'])

    def test_head_mismatch_blocks(self):
        db = sqlite3.connect(self.fixture.risk)
        try:
            db.execute(
                'UPDATE session_lineage_head SET entry_hash=? WHERE id=1',
                ('4' * 64,))
            db.commit()
        finally:
            db.close()
        self.fixture._refresh_pins()
        result = self.fixture.provider().read()
        self.assertIn('SOURCE_LINEAGE_HEAD_MISMATCH', result['blockers'])

    def test_current_registration_must_name_current_source(self):
        decoy = self.fixture.root / 'decoy.sqlite'
        shutil.copy2(self.fixture.execution, decoy)
        events = self.fixture.events()
        events[1]['session']['db'] = str(decoy.resolve())
        events[1]['current_source_db'] = str(decoy.resolve())
        self.fixture.write_lineage(events)
        result = self.fixture.provider().read()
        self.assertIn('SOURCE_LINEAGE_CURRENT_NOT_REGISTERED',
                      result['blockers'])

    def test_evidence_can_reach_pure_baseline_plan_without_being_empty(self):
        result = self.fixture.provider().read()
        self.assertEqual(result['status'], 'READY')
        evidence = result['evidence']
        self.assertEqual(set(evidence), {'source', 'risk', 'host', 'lineage'})
        snapshot_value = historical_snapshot()
        snapshot_value['account_key'] = self.fixture.account_key
        snapshot_value['account_mask'] = '••••0000'
        observed = datetime.fromisoformat('2026-10-16T14:00:00+00:00')
        from trading_infra.historical_baseline import plan_baseline
        planned = plan_baseline(
            snapshot_value, account_key=self.fixture.account_key,
            cutoff_utc='2026-10-16T14:00:00+00:00',
            policy_sha256=digest(self.fixture.policy),
            source_fingerprint=source_fingerprint(), evidence=evidence,
            expires_at='2026-10-16T14:05:00+00:00', now=observed)
        self.assertFalse(planned['production_ready'])
        self.assertFalse(planned['live_enabled'])
