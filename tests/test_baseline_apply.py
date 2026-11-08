"""Offline tests for the explicit historical-baseline apply CLI."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from martin_agentic import baseline_cli
from martin_agentic.audit import canonical
from martin_agentic.qualification import source_fingerprint
from martin_agentic.source_evidence import SourceEvidenceProvider
from martin_agentic.sqlite_readonly import readonly_uri
from test_baseline_cli import (
    CliFixture, MutableClock, _acquire_once, _start_holder, _stop_holder,
    run_cli, tree_state)
from test_historical_baseline import AT, EXPIRES, NOW, historical_snapshot
from trading_contracts.values import digest
from trading_infra.attribution import ExecutionAttribution
from trading_infra.historical_baseline import (
    HistoricalBaselineIndex, apply_baseline, plan_baseline, status_baseline)
from trading_infra.instance_accounting import InstanceAccounting
from trading_infra.receipts import ExecutionReceiptImporter


def _now():
    return datetime.now(timezone.utc)


SQLITE_WRITER = '''
import os,sqlite3,sys,time
db=sqlite3.connect(sys.argv[1],isolation_level=None,timeout=5)
if len(sys.argv)>3 and sys.argv[3]=='mutate':
    db.execute('PRAGMA journal_mode=OFF')
db.execute('BEGIN IMMEDIATE')
if len(sys.argv)>3 and sys.argv[3]=='mutate':
    db.execute("UPDATE live_state SET payload=payload || ' ' WHERE id=1")
print('LOCKED',flush=True)
release=os.environ.get('SQLITE_WRITER_RELEASE')
if release:
    deadline=time.monotonic()+10
    while not os.path.exists(release) and time.monotonic()<deadline:
        time.sleep(0.01)
else:
    time.sleep(float(sys.argv[2]))
if len(sys.argv)>3 and sys.argv[3]=='mutate':
    db.commit()
else:
    db.rollback()
db.close()
'''


class BaselineApplyTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.fixture = CliFixture(self.temp.name)

    def _snapshot(self):
        snapshot = historical_snapshot()
        snapshot['account_key'] = self.fixture.source.account_key
        snapshot['account_mask'] = '••••0000'
        return snapshot

    def _manifest(self, snapshot=None, *, expires=EXPIRES, now=NOW,
                  evidence=None, name='apply.json'):
        snapshot = snapshot or self._snapshot()
        if isinstance(expires, datetime):
            expires = expires.isoformat()
        evidence = evidence or self.fixture.legal_source_evidence()['evidence']
        manifest = plan_baseline(
            snapshot, account_key=self.fixture.source.account_key,
            cutoff_utc=AT, policy_sha256=digest(self.fixture.source.policy),
            source_fingerprint=source_fingerprint(), evidence=evidence,
            expires_at=expires, now=now)
        path = self.fixture.write_manifest(manifest, name)
        return manifest, path

    def _args(self, manifest, path, **changes):
        args = SimpleNamespace(**self.fixture.plan_args())
        args.db = str(self.fixture.source.execution)
        args.manifest = str(path)
        args.manifest_pin = manifest['manifest_sha256']
        args.approval_scope = 'test-local-apply'
        for key, value in changes.items():
            setattr(args, key, value)
        return args

    def _fake(self, snapshot):
        return {
            'account': {'account_key': self.fixture.source.account_key,
                        'account_mask': '••••0000'},
            'raw_chain_sha256': 'a' * 64, 'observed_at': AT,
            'collection_window_seconds': 3.0, 'blockers': [],
            'candidate_snapshot': snapshot, 'ready': True,
        }

    def _run(self, args, clock=NOW, fake=None):
        clock_arg = clock if callable(clock) else (lambda: clock)
        if fake is None:
            return baseline_cli._run_apply(args, clock=clock_arg)
        with patch('martin_agentic.baseline_cli.normalize_bundle',
                   return_value=fake):
            return baseline_cli._run_apply(args, clock=clock_arg)

    def _risk(self):
        db = sqlite3.connect(self.fixture.source.risk, isolation_level=None)
        try:
            row = db.execute('SELECT payload FROM account_risk WHERE id=1'
                             ).fetchone()
            return json.loads(row[0]) if row else None
        finally:
            db.close()

    def _baseline_tables(self):
        db = sqlite3.connect(self.fixture.source.execution)
        try:
            return {row[0] for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
        finally:
            db.close()

    def _status(self):
        db = sqlite3.connect(self.fixture.source.execution)
        try:
            return status_baseline(db)
        finally:
            db.close()

    def test_apply_then_expired_exact_replay_after_source_change(self):
        snapshot = self._snapshot()
        manifest, path = self._manifest(snapshot)
        args = self._args(manifest, path)
        before_risk = self._risk()
        before_source = Path(self.fixture.source.execution).read_bytes()
        output, code = self._run(args, fake=self._fake(snapshot))
        self.assertEqual(code, 0, output)
        self.assertEqual(output['status'], 'APPLIED')
        self.assertTrue(output['applied'])
        self.assertEqual(output['baseline']['baseline_id'],
                         manifest['baseline_id'])
        self.assertFalse(output['production_ready'])
        self.assertFalse(output['live_enabled'])
        self.assertNotIn('SIM-0000', json.dumps(output))
        self.assertNotEqual(Path(self.fixture.source.execution).read_bytes(),
                            before_source)
        self.assertEqual(self._risk(), before_risk)
        self.assertEqual(self._status()['status'], 'PRESENT')
        self.assertEqual(self._status()['active'][0]['baseline_id'],
                         manifest['baseline_id'])
        db = sqlite3.connect(self.fixture.source.execution)
        try:
            self.assertEqual(db.execute('PRAGMA integrity_check').fetchall(),
                             [('ok',)])
            self.assertEqual(db.execute(
                'SELECT count(*) FROM historical_baseline_order').fetchone()[0],
                7)
        finally:
            db.close()
        self.assertFalse(Path(
            str(self.fixture.source.execution) + '-journal').exists())
        acquired = _acquire_once(self.fixture.source.lock)
        self.assertEqual(acquired.returncode, 0, acquired.stdout)

        # Apply changed the source DB and generation. Exact replay must still
        # work after plan expiry and must not read source or write anything.
        before = tree_state(self.fixture.temp)
        with patch('martin_agentic.baseline_cli.normalize_bundle',
                   return_value=self._fake(snapshot)), \
                patch.object(SourceEvidenceProvider, 'read',
                             side_effect=AssertionError('source must not be read')):
            replay, replay_code = baseline_cli._run_apply(
                args, clock=lambda: NOW + timedelta(days=1))
        self.assertEqual(replay_code, 0, replay)
        self.assertEqual(replay['status'], 'ALREADY_APPLIED')
        self.assertFalse(replay['applied'])
        self.assertTrue(replay['baseline']['duplicate'])
        self.assertEqual(tree_state(self.fixture.temp), before)

    def test_active_second_payload_and_materialized_tamper_rejected(self):
        snapshot = self._snapshot()
        manifest, path = self._manifest(snapshot)
        args = self._args(manifest, path)
        output, code = self._run(args, fake=self._fake(snapshot))
        self.assertEqual(code, 0, output)

        second_snapshot = deepcopy(snapshot)
        second_snapshot['portfolio']['cash'] = '499.94'
        second, second_path = self._manifest(
            second_snapshot, expires=NOW + timedelta(minutes=10),
            evidence=manifest['evidence'], name='second.json')
        second_args = self._args(second, second_path)
        output, code = self._run(second_args, fake=self._fake(snapshot))
        self.assertEqual(code, 4, output)
        self.assertIn('BASELINE_ACTIVE_EXISTS', output['blockers'])
        self.assertEqual(self._status()['active'][0]['baseline_id'],
                         manifest['baseline_id'])

        changed = deepcopy(manifest)
        changed['opening_balance_components'][0]['amount'] = '499.94'
        changed['manifest_sha256'] = digest(
            {key: value for key, value in changed.items()
             if key != 'manifest_sha256'})
        changed_path = self.fixture.write_manifest(changed, 'changed.json')
        changed_args = self._args(changed, changed_path)
        output, code = self._run(changed_args, fake=self._fake(snapshot))
        self.assertEqual(code, 4, output)
        self.assertIn('BASELINE_ID_PAYLOAD_CONFLICT', output['blockers'])

        db = sqlite3.connect(self.fixture.source.execution)
        try:
            row = db.execute(
                'SELECT order_key,order_json FROM historical_baseline_order '
                "WHERE order_id='buy-a'").fetchone()
            body = json.loads(row[1])
            body['executions'][0]['price'] = '1'
            db.execute(
                'UPDATE historical_baseline_order SET order_json=?,'
                'canonical_order_sha256=? WHERE order_key=?',
                (canonical(body), digest(body), row[0]))
            db.commit()
        finally:
            db.close()
        output, code = self._run(args, clock=NOW + timedelta(days=1),
                                 fake=self._fake(snapshot))
        self.assertEqual(code, 4, output)
        self.assertEqual(output['status'], 'VALIDATION_FAILED')
        self.assertIn('BASELINE_INDEX_CORRUPT', output['blockers'])

    def test_missing_symlink_busy_wal_expiry_and_source_mismatch(self):
        snapshot = self._snapshot()
        manifest, path = self._manifest(snapshot)
        args = self._args(manifest, path)

        missing = Path(self.fixture.temp) / 'missing.sqlite'
        output, code = self._run(
            self._args(manifest, path, db=str(missing)),
            fake=self._fake(snapshot))
        self.assertEqual(code, 5, output)
        self.assertFalse(missing.exists())

        link = Path(self.fixture.temp) / 'link.sqlite'
        link.symlink_to(self.fixture.source.execution)
        output, code = self._run(
            self._args(manifest, path, db=str(link)),
            fake=self._fake(snapshot))
        self.assertEqual(code, 5, output)

        other = self.fixture.source.risk
        output, code = self._run(
            self._args(manifest, path, execution_db=str(other)),
            fake=self._fake(snapshot))
        self.assertEqual(code, 5, output)
        self.assertIn('CLI_DB_SOURCE_MISMATCH', output['blockers'])

        holder = _start_holder(self.fixture.source.lock)
        try:
            output, code = self._run(args, fake=self._fake(snapshot))
        finally:
            _stop_holder(holder)
        self.assertEqual(code, 3, output)
        self.assertEqual(output['status'], 'BLOCKED')
        self.assertIn('SOURCE_OWNER_RUNNING',
                      output['source']['blockers'])

        wal = Path(str(self.fixture.source.execution) + '-wal')
        wal.write_bytes(b'not empty')
        try:
            output, code = self._run(args, fake=self._fake(snapshot))
        finally:
            wal.unlink()
        self.assertEqual(code, 5, output)
        self.assertIn('CLI_WAL_NOT_CONVERGED', output['blockers'])

        journal = Path(str(self.fixture.source.execution) + '-journal')
        journal.write_bytes(b'not empty')
        try:
            output, code = self._run(args, fake=self._fake(snapshot))
        finally:
            journal.unlink()
        self.assertEqual(code, 5, output)
        self.assertIn('CLI_WAL_NOT_CONVERGED', output['blockers'])

        short, short_path = self._manifest(
            snapshot, expires=NOW + timedelta(seconds=1), name='short.json')
        output, code = self._run(
            self._args(short, short_path), clock=NOW + timedelta(seconds=10),
            fake=self._fake(snapshot))
        self.assertEqual(code, 4, output)
        self.assertIn('BASELINE_PLAN_EXPIRED', output['blockers'])
        self.assertNotIn('historical_baseline_schema', self._baseline_tables())

    def test_wrong_root_pins_account_and_manifest_pin_reject(self):
        snapshot = self._snapshot()
        manifest, path = self._manifest(snapshot, name='wrong.json')
        args = self._args(manifest, path)
        other_root = Path(self.fixture.temp) / 'other-root'
        other_root.mkdir(mode=0o700)
        output, code = self._run(
            self._args(manifest, path, root=str(other_root)),
            fake=self._fake(snapshot))
        self.assertEqual(code, 5, output)

        bad_pins = dict(self.fixture.source.expected)
        bad_pins['execution_db_sha256'] = '0' * 64
        self.fixture.pins_path.write_text(json.dumps(bad_pins))
        os.chmod(self.fixture.pins_path, 0o600)
        before = tree_state(self.fixture.temp)
        output, code = self._run(args, fake=self._fake(snapshot))
        self.assertEqual(code, 3, output)
        self.assertIn('SOURCE_EXECUTION_HASH_MISMATCH',
                      output['source']['blockers'])
        self.assertNotIn('historical_baseline_schema', self._baseline_tables())
        self.assertEqual(tree_state(self.fixture.temp), before)

        bad_account = dict(self.fixture.source.expected)
        bad_account['account_key'] = '0' * 64
        self.fixture.pins_path.write_text(json.dumps(bad_account))
        os.chmod(self.fixture.pins_path, 0o600)
        output, code = self._run(args, fake=self._fake(snapshot))
        self.assertEqual(code, 5, output)
        self.assertIn('CLI_MANIFEST_PINS_MISMATCH', output['blockers'])

        self.fixture.pins_path.write_text(json.dumps(self.fixture.source.expected))
        os.chmod(self.fixture.pins_path, 0o600)
        output, code = self._run(
            self._args(manifest, path, manifest_pin='0' * 64),
            fake=self._fake(snapshot))
        self.assertEqual(code, 4, output)
        self.assertIn('BASELINE_MANIFEST_PIN_MISMATCH', output['blockers'])
        self.assertNotIn('historical_baseline_schema', self._baseline_tables())

    def test_owner_lock_replaced_after_source_read_rejected(self):
        snapshot = self._snapshot()
        manifest, path = self._manifest(snapshot, name='lock-replaced.json')
        args = self._args(manifest, path)
        real_open = baseline_cli._open_writable
        saved = Path(str(self.fixture.source.lock) + '.saved')

        def replace_lock(db_path):
            db = real_open(db_path)
            self.fixture.source.lock.rename(saved)
            self.fixture.source.lock.touch(mode=0o600)
            return db

        with patch('martin_agentic.baseline_cli._open_writable',
                   side_effect=replace_lock):
            output, code = self._run(args, fake=self._fake(snapshot))
        self.assertEqual(code, 3, output)
        self.assertEqual(output['status'], 'BLOCKED')
        self.assertIn('SOURCE_OWNER_LOCK_REPLACED',
                      output['source']['blockers'])
        self.assertNotIn('historical_baseline_schema', self._baseline_tables())

    def test_risk_changed_after_source_read_rejected_at_write_lock(self):
        snapshot = self._snapshot()
        manifest, path = self._manifest(snapshot, name='risk-drift.json')
        args = self._args(manifest, path)
        real_open = baseline_cli._open_writable

        def change_risk(db_path):
            db = real_open(db_path)
            risk = sqlite3.connect(self.fixture.source.risk,
                                   isolation_level=None)
            try:
                risk.execute('PRAGMA journal_mode=OFF')
                state = json.loads(risk.execute(
                    'SELECT payload FROM account_risk WHERE id=1').fetchone()[0])
                state['blocked'] = True
                state['sticky_reasons'] = ['PROJECT_DRAWDOWN_LOCKED']
                state['reasons'] = ['PROJECT_DRAWDOWN_LOCKED']
                risk.execute('UPDATE account_risk SET payload=? WHERE id=1',
                             (canonical(state),))
                risk.commit()
            finally:
                risk.close()
            return db

        with patch('martin_agentic.baseline_cli._open_writable',
                   side_effect=change_risk):
            output, code = self._run(args, fake=self._fake(snapshot))
        self.assertEqual(code, 3, output)
        self.assertEqual(output['status'], 'BLOCKED')
        self.assertTrue(any('SOURCE_RISK' in item
                            for item in output['source']['blockers']))
        self.assertNotIn('historical_baseline_schema', self._baseline_tables())

    def test_exact_replay_verifies_owner_after_db_read(self):
        snapshot = self._snapshot()
        manifest, path = self._manifest(snapshot, name='replay-lock.json')
        args = self._args(manifest, path)
        output, code = self._run(args, fake=self._fake(snapshot))
        self.assertEqual(code, 0, output)
        real_open = baseline_cli._open_readonly
        saved = Path(str(self.fixture.source.lock) + '.saved')

        def replace_after_read(db_path):
            db = real_open(db_path)
            self.fixture.source.lock.rename(saved)
            self.fixture.source.lock.touch(mode=0o600)
            return db

        with patch('martin_agentic.baseline_cli._open_readonly',
                   side_effect=replace_after_read):
            replay, replay_code = self._run(
                args, clock=NOW + timedelta(days=1),
                fake=self._fake(snapshot))
        self.assertNotEqual(replay_code, 0, replay)
        self.assertEqual(replay['status'], 'BLOCKED')
        self.assertIn('SOURCE_OWNER_LOCK_REPLACED',
                      replay['source']['blockers'])
        self.assertNotEqual(replay.get('baseline', {}).get('status'),
                            'ALREADY_APPLIED')

    def _start_sqlite_writer(self, hold, mutate=False, release=None):
        args = [sys.executable, '-c', SQLITE_WRITER,
                str(self.fixture.source.execution), str(hold)]
        if mutate:
            args.append('mutate')
        env = dict(os.environ)
        if release is not None:
            env['SQLITE_WRITER_RELEASE'] = str(release)
        process = subprocess.Popen(
            args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, env=env)
        line = process.stdout.readline().strip()
        if line != 'LOCKED':
            process.kill()
            self.fail('sqlite writer did not acquire: ' + line)
        self.addCleanup(self._stop_sqlite_writer, process)
        return process

    @staticmethod
    def _stop_sqlite_writer(process):
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=5)

    def _run_in_thread(self, args, clock, fake, ready=None):
        result = {}

        def target():
            try:
                if ready is None:
                    result['out'], result['rc'] = self._run(
                        args, clock=clock, fake=fake)
                else:
                    original = baseline_cli._open_writable

                    def open_with_handshake(path):
                        db = original(path)
                        def trace(statement):
                            if statement.lstrip().upper().startswith(
                                    'BEGIN IMMEDIATE'):
                                ready.set()
                        db.set_trace_callback(trace)
                        return db

                    with patch('martin_agentic.baseline_cli._open_writable',
                               side_effect=open_with_handshake):
                        result['out'], result['rc'] = self._run(
                            args, clock=clock, fake=fake)
            except BaseException as exc:
                result['error'] = exc

        thread = threading.Thread(target=target)
        started = time.monotonic()
        thread.start()
        return thread, result, started

    def test_actual_sqlite_writer_wait_expiry_rejects(self):
        snapshot = self._snapshot()
        manifest, path = self._manifest(
            snapshot, expires=NOW + timedelta(seconds=0.2),
            name='real-wait-expiry.json')
        args = self._args(manifest, path)
        release = Path(self.temp.name) / 'writer-release-expiry'
        ready = threading.Event()
        writer = self._start_sqlite_writer(5, release=release)
        start = time.monotonic()
        clock = lambda: NOW + timedelta(seconds=time.monotonic() - start)
        thread, result, started = self._run_in_thread(
            args, clock, self._fake(snapshot), ready=ready)
        self.assertTrue(ready.wait(2), 'apply did not reach BEGIN IMMEDIATE')
        time.sleep(0.3)
        release.write_text('release')
        thread.join(5)
        self._stop_sqlite_writer(writer)
        self.assertFalse(thread.is_alive())
        self.assertNotIn('error', result, result)
        self.assertGreaterEqual(time.monotonic() - started, 0.25)
        self.assertEqual(result['rc'], 4, result['out'])
        self.assertIn('BASELINE_PLAN_EXPIRED',
                      result['out']['blockers'])
        self.assertNotIn('historical_baseline_schema', self._baseline_tables())

    def test_actual_sqlite_writer_wait_risk_drift_rejects(self):
        snapshot = self._snapshot()
        manifest, path = self._manifest(snapshot, name='real-wait-risk.json')
        args = self._args(manifest, path)
        release = Path(self.temp.name) / 'writer-release-risk'
        ready = threading.Event()
        writer = self._start_sqlite_writer(5, release=release)
        thread, result, started = self._run_in_thread(
            args, lambda: NOW, self._fake(snapshot), ready=ready)
        self.assertTrue(ready.wait(2), 'apply did not reach BEGIN IMMEDIATE')
        risk = sqlite3.connect(self.fixture.source.risk, isolation_level=None)
        try:
            risk.execute('PRAGMA journal_mode=OFF')
            state = json.loads(risk.execute(
                'SELECT payload FROM account_risk WHERE id=1').fetchone()[0])
            state['blocked'] = True
            state['sticky_reasons'] = ['PROJECT_DRAWDOWN_LOCKED']
            state['reasons'] = ['PROJECT_DRAWDOWN_LOCKED']
            risk.execute('UPDATE account_risk SET payload=? WHERE id=1',
                         (canonical(state),))
            risk.commit()
        finally:
            risk.close()
        time.sleep(0.3)
        release.write_text('release')
        thread.join(5)
        self._stop_sqlite_writer(writer)
        self.assertFalse(thread.is_alive())
        self.assertNotIn('error', result, result)
        self.assertGreaterEqual(time.monotonic() - started, 0.25)
        self.assertEqual(result['rc'], 3, result['out'])
        self.assertEqual(result['out']['status'], 'BLOCKED')
        self.assertTrue(any('SOURCE_RISK' in item
                            for item in result['out']['source']['blockers']))
        self.assertNotIn('historical_baseline_schema', self._baseline_tables())

    def test_actual_sqlite_writer_wait_owner_lock_replaced_rejects(self):
        snapshot = self._snapshot()
        manifest, path = self._manifest(snapshot, name='real-wait-owner.json')
        args = self._args(manifest, path)
        release = Path(self.temp.name) / 'writer-release-owner'
        ready = threading.Event()
        writer = self._start_sqlite_writer(5, release=release)
        thread, result, started = self._run_in_thread(
            args, lambda: NOW, self._fake(snapshot), ready=ready)
        self.assertTrue(ready.wait(2), 'apply did not reach BEGIN IMMEDIATE')
        time.sleep(0.3)
        saved = Path(str(self.fixture.source.lock) + '.saved')
        self.fixture.source.lock.rename(saved)
        self.fixture.source.lock.touch(mode=0o600)
        release.write_text('release')
        thread.join(5)
        self._stop_sqlite_writer(writer)
        self.assertFalse(thread.is_alive())
        self.assertNotIn('error', result, result)
        self.assertEqual(result['rc'], 3, result['out'])
        self.assertIn('SOURCE_OWNER_LOCK_REPLACED',
                      result['out']['source']['blockers'])
        self.assertNotIn('historical_baseline_schema', self._baseline_tables())

    def test_actual_sqlite_writer_wait_source_change_rejects(self):
        snapshot = self._snapshot()
        manifest, path = self._manifest(snapshot, name='real-wait-source.json')
        args = self._args(manifest, path)
        writer = self._start_sqlite_writer(0.4, mutate=True)
        thread, result, started = self._run_in_thread(
            args, lambda: NOW, self._fake(snapshot))
        thread.join(5)
        self._stop_sqlite_writer(writer)
        self.assertFalse(thread.is_alive())
        self.assertNotIn('error', result, result)
        self.assertEqual(result['rc'], 3, result['out'])
        self.assertEqual(result['out']['status'], 'BLOCKED')
        self.assertNotIn('historical_baseline_schema', self._baseline_tables())

    def test_sticky_risk_state_is_not_reset_by_apply(self):
        risk = sqlite3.connect(self.fixture.source.risk, isolation_level=None)
        try:
            state = json.loads(risk.execute(
                'SELECT payload FROM account_risk WHERE id=1').fetchone()[0])
            state['sticky_reasons'] = ['INITIAL_CAPITAL_NOT_RECONCILED',
                                       'PROJECT_DRAWDOWN_LOCKED']
            state['blocked'] = True
            state['reasons'] = list(state['sticky_reasons'])
            state['nav_high'] = '1.0'
            state['daily_halted'] = True
            state['day_start_equity'] = '500'
            risk.execute('UPDATE account_risk SET payload=? WHERE id=1',
                         (canonical(state),))
            risk.commit()
            expected = state
        finally:
            risk.close()
        self.fixture.source._refresh_pins()
        self.fixture.pins_path.write_text(json.dumps(self.fixture.source.expected))
        os.chmod(self.fixture.pins_path, 0o600)
        snapshot = self._snapshot()
        manifest, path = self._manifest(snapshot, name='sticky.json')
        args = self._args(manifest, path)
        output, code = self._run(args, fake=self._fake(snapshot))
        self.assertEqual(code, 0, output)
        self.assertEqual(self._risk(), expected)
        self.assertTrue(self._risk()['blocked'])
        self.assertEqual(self._risk()['sticky_reasons'],
                         expected['sticky_reasons'])

    def test_write_lock_wait_expiry_and_source_drift_reject(self):
        snapshot = self._snapshot()
        manifest, path = self._manifest(
            snapshot, expires=NOW + timedelta(seconds=1), name='wait.json')
        args = self._args(manifest, path)
        clock = MutableClock(NOW)

        def slow_normalize(loaded, *, now=None):
            clock.advance(10)
            return self._fake(snapshot)

        with patch('martin_agentic.baseline_cli.normalize_bundle',
                   side_effect=slow_normalize):
            output, code = baseline_cli._run_apply(args, clock=clock)
        self.assertEqual(code, 4, output)
        self.assertIn('BASELINE_PLAN_EXPIRED', output['blockers'])
        self.assertNotIn('historical_baseline_schema', self._baseline_tables())

        manifest, path = self._manifest(snapshot, name='drift.json')
        args = self._args(manifest, path)

        def drift(loaded, *, now=None):
            db = sqlite3.connect(self.fixture.source.execution,
                                 isolation_level=None)
            try:
                db.execute("UPDATE live_state SET payload=payload || ' ' "
                           "WHERE id=1")
                db.commit()
            finally:
                db.close()
            return self._fake(snapshot)

        with patch('martin_agentic.baseline_cli.normalize_bundle',
                   side_effect=drift):
            output, code = baseline_cli._run_apply(args, clock=lambda: NOW)
        self.assertEqual(code, 3, output)
        self.assertEqual(output['status'], 'BLOCKED')
        self.assertNotIn('historical_baseline_schema', self._baseline_tables())

    def test_core_prewrite_hook_runs_before_baseline_schema(self):
        snapshot = self._snapshot()
        evidence = self.fixture.legal_source_evidence()['evidence']
        manifest = plan_baseline(
            snapshot, account_key=self.fixture.source.account_key,
            cutoff_utc=AT, policy_sha256=digest(self.fixture.source.policy),
            source_fingerprint=source_fingerprint(), evidence=evidence,
            expires_at=EXPIRES, now=NOW)
        db = sqlite3.connect(self.fixture.source.execution, isolation_level=None)
        called = []
        try:
            db.execute('BEGIN IMMEDIATE')

            def hook():
                called.append(True)
                self.assertNotIn('historical_baseline_schema', {
                    row[0] for row in db.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'")})
                raise RuntimeError('synthetic-prewrite-stop')

            with self.assertRaises(RuntimeError):
                apply_baseline(
                    db, manifest, manifest_pin=manifest['manifest_sha256'],
                    approval_scope='test-scope', clock=lambda: NOW,
                    evidence=evidence, snapshot=snapshot, prewrite=hook)
        finally:
            if db.in_transaction:
                db.rollback()
            db.close()
        self.assertEqual(called, [True])
        self.assertNotIn('historical_baseline_schema', self._baseline_tables())

    def test_precommit_rollback_and_postcheck_failure(self):
        snapshot = self._snapshot()
        manifest, path = self._manifest(snapshot, name='rollback.json')
        args = self._args(manifest, path)
        with patch('trading_infra.historical_baseline._store_manifest',
                   side_effect=sqlite3.IntegrityError('synthetic-precommit')):
            output, code = self._run(args, fake=self._fake(snapshot))
        self.assertEqual(code, 4, output)
        self.assertEqual(output['status'], 'APPLY_OUTCOME_UNKNOWN')
        self.assertIsNone(output['applied'])
        self.assertNotIn('historical_baseline_schema', self._baseline_tables())
        self.assertEqual(self._status()['status'], 'ABSENT')

        snapshot = self._snapshot()
        manifest, path = self._manifest(snapshot, name='postcheck.json')
        args = self._args(manifest, path)
        with patch('martin_agentic.baseline_cli._post_apply_status',
                   side_effect=baseline_cli.CliError(
                       'CLI_APPLY_POSTCHECK_FAILED')):
            output, code = self._run(args, fake=self._fake(snapshot))
        self.assertEqual(code, 4, output)
        self.assertEqual(output['status'], 'APPLIED_POSTCHECK_FAILED')
        self.assertTrue(output['applied'])
        self.assertEqual(output['baseline']['manifest_pin'],
                         manifest['manifest_sha256'])
        replay, replay_code = self._run(
            args, clock=NOW + timedelta(days=1),
            fake=self._fake(snapshot))
        self.assertEqual(replay_code, 0, replay)
        self.assertEqual(replay['status'], 'ALREADY_APPLIED')

    def test_public_bundle_incomplete_never_writes(self):
        snapshot = self._snapshot()
        manifest, path = self._manifest(
            snapshot, expires=_now() + timedelta(minutes=5),
            name='public.json')
        args = self._args(manifest, path)
        before = tree_state(self.fixture.temp)
        output, code = self._run(args, clock=_now())
        self.assertEqual(code, 3, output)
        self.assertEqual(output['status'], 'CONDITIONS_MISSING')
        self.assertEqual(output['baseline'], {'status': 'NOT_ATTEMPTED'})
        self.assertIn('GETTER_MISSING:futures_positions',
                      output['bundle']['blockers'])
        self.assertNotIn('historical_baseline_schema', self._baseline_tables())
        self.assertEqual(tree_state(self.fixture.temp), before)

    def test_quarantine_preserved_importer_matches_without_owner_or_fill(self):
        snapshot = self._snapshot()
        db = sqlite3.connect(self.fixture.source.execution, isolation_level=None)
        try:
            db.executescript('''
                CREATE TABLE receipt_quarantine(
                    receipt_key TEXT PRIMARY KEY, account_key TEXT,
                    observed_at TEXT NOT NULL, reason TEXT NOT NULL,
                    payload TEXT NOT NULL, recorded_at TEXT NOT NULL);
                INSERT INTO receipt_quarantine VALUES(
                    'audit-row','account','2026-10-19T14:00:00+00:00',
                    'RECEIPT_REFERENCE_UNBOUND','{}',
                    '2026-10-19T14:00:00+00:00');
            ''')
            db.commit()
        finally:
            db.close()
        self.fixture.source._refresh_pins()
        self.fixture.pins_path.write_text(json.dumps(self.fixture.source.expected))
        os.chmod(self.fixture.pins_path, 0o600)
        manifest, path = self._manifest(snapshot, name='quarantine.json')
        args = self._args(manifest, path)
        output, code = self._run(args, fake=self._fake(snapshot))
        self.assertEqual(code, 0, output)

        db = sqlite3.connect(self.fixture.source.execution)
        try:
            before_quarantine = db.execute(
                'SELECT count(*) FROM receipt_quarantine').fetchone()[0]
        finally:
            db.close()
        db = sqlite3.connect(self.fixture.source.execution)
        try:
            importer = ExecutionReceiptImporter(
                db,
                attribution=ExecutionAttribution(db, clock=lambda: NOW),
                accounting=InstanceAccounting(db, clock=lambda: NOW),
                clock=lambda: NOW,
                baseline=HistoricalBaselineIndex(db))
            result = importer.import_snapshot(snapshot)
            self.assertEqual(len(result['baseline_matched']), 7)
            self.assertEqual(result['quarantined'], [])
            self.assertEqual(result['deferred'], [])
            self.assertEqual(db.execute(
                'SELECT count(*) FROM receipt_quarantine').fetchone()[0],
                before_quarantine)
            for table in ('instance_fills', 'execution_fill_identity',
                          'account_budget_reservations'):
                if table in {row[0] for row in db.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'")}:
                    self.assertEqual(db.execute(
                        'SELECT count(*) FROM ' + table).fetchone()[0], 0)
            self.assertEqual(db.execute(
                'SELECT count(*) FROM execution_attribution').fetchone()[0], 0)
        finally:
            db.close()

    def test_prewrite_clock_crossing_expiry_rejects_before_schema(self):
        snapshot = self._snapshot()
        evidence = self.fixture.legal_source_evidence()['evidence']
        manifest = plan_baseline(
            snapshot, account_key=self.fixture.source.account_key,
            cutoff_utc=AT, policy_sha256=digest(self.fixture.source.policy),
            source_fingerprint=source_fingerprint(), evidence=evidence,
            expires_at=(NOW + timedelta(seconds=1)).isoformat(), now=NOW)
        clock = MutableClock(NOW)
        db = sqlite3.connect(self.fixture.source.execution, isolation_level=None)
        try:
            with self.assertRaisesRegex(
                    baseline_cli.BaselineError, 'BASELINE_PLAN_EXPIRED'):
                apply_baseline(
                    db, manifest, manifest_pin=manifest['manifest_sha256'],
                    approval_scope='test-scope', clock=clock,
                    evidence=evidence, snapshot=snapshot,
                    prewrite=lambda: clock.advance(2))
        finally:
            db.close()
        self.assertNotIn('historical_baseline_schema', self._baseline_tables())

    def test_prewrite_clock_crossing_snapshot_freshness_rejects(self):
        snapshot = self._snapshot()
        evidence = self.fixture.legal_source_evidence()['evidence']
        manifest = plan_baseline(
            snapshot, account_key=self.fixture.source.account_key,
            cutoff_utc=AT, policy_sha256=digest(self.fixture.source.policy),
            source_fingerprint=source_fingerprint(), evidence=evidence,
            expires_at='2099-01-01T00:00:00+00:00', now=NOW)
        clock = MutableClock(NOW)
        db = sqlite3.connect(self.fixture.source.execution, isolation_level=None)
        try:
            with self.assertRaisesRegex(
                    baseline_cli.BaselineError,
                    'BASELINE_SNAPSHOT_STALE_OR_CUTOFF'):
                apply_baseline(
                    db, manifest, manifest_pin=manifest['manifest_sha256'],
                    approval_scope='test-scope', clock=clock,
                    evidence=evidence, snapshot=snapshot,
                    prewrite=lambda: clock.advance(31))
        finally:
            db.close()
        self.assertNotIn('historical_baseline_schema', self._baseline_tables())

    def test_third_source_read_crossing_expiry_rejects(self):
        snapshot = self._snapshot()
        manifest, path = self._manifest(
            snapshot, expires=NOW + timedelta(seconds=1),
            name='third-read-expiry.json')
        args = self._args(manifest, path)
        clock = MutableClock(NOW)
        calls = []
        real_read = SourceEvidenceProvider.read

        def timed_read(provider):
            calls.append(None)
            if len(calls) == 3:
                clock.advance(2)
            return real_read(provider)

        with patch.object(SourceEvidenceProvider, 'read', timed_read):
            output, code = self._run(args, clock=clock,
                                     fake=self._fake(snapshot))
        self.assertEqual(len(calls), 3)
        self.assertEqual(code, 4, output)
        self.assertEqual(output['status'], 'VALIDATION_FAILED')
        self.assertIn('BASELINE_PLAN_EXPIRED', output['blockers'])
        self.assertNotIn('historical_baseline_schema', self._baseline_tables())

    def test_third_source_read_control_applies(self):
        snapshot = self._snapshot()
        manifest, path = self._manifest(
            snapshot, expires=NOW + timedelta(seconds=1),
            name='third-read-control.json')
        args = self._args(manifest, path)
        clock = MutableClock(NOW)
        real_read = SourceEvidenceProvider.read
        calls = []

        def timed_read(provider):
            calls.append(None)
            return real_read(provider)

        with patch.object(SourceEvidenceProvider, 'read', timed_read):
            output, code = self._run(args, clock=clock,
                                     fake=self._fake(snapshot))
        self.assertEqual(len(calls), 3)
        self.assertEqual(code, 0, output)
        self.assertEqual(output['status'], 'APPLIED')

    def test_third_source_read_crossing_snapshot_freshness_rejects(self):
        snapshot = self._snapshot()
        manifest, path = self._manifest(
            snapshot, expires='2099-01-01T00:00:00+00:00',
            name='third-read-stale.json')
        args = self._args(manifest, path)
        clock = MutableClock(NOW)
        calls = []
        real_read = SourceEvidenceProvider.read

        def slow_read(provider):
            calls.append(None)
            if len(calls) == 3:
                clock.advance(31)
            return real_read(provider)

        with patch.object(SourceEvidenceProvider, 'read', slow_read):
            output, code = self._run(args, clock=clock,
                                     fake=self._fake(snapshot))
        self.assertEqual(len(calls), 3)
        self.assertEqual(code, 4, output)
        self.assertIn('BASELINE_SNAPSHOT_STALE_OR_CUTOFF',
                      output['blockers'])
        self.assertNotIn('historical_baseline_schema', self._baseline_tables())

    def test_cli_apply_dispatch_public_bundle_is_conditions_missing(self):
        snapshot = self._snapshot()
        manifest, path = self._manifest(
            snapshot, expires=_now() + timedelta(minutes=5),
            name='cli-public.json')
        result = run_cli(
            'historical-baseline', 'apply',
            '--bundle-root', str(self.fixture.bundle.root),
            '--policy', str(self.fixture.policy_path),
            '--pins', str(self.fixture.pins_path),
            '--execution-db', str(self.fixture.source.execution),
            '--risk-db', str(self.fixture.source.risk),
            '--host-dir', str(self.fixture.source.host),
            '--root', str(self.fixture.source.root), '--isolated-root',
            '--db', str(self.fixture.source.execution),
            '--manifest', str(path),
            '--manifest-pin', manifest['manifest_sha256'],
            '--approval-scope', 'cli-test-scope')
        self.assertEqual(result.returncode, 3, result.stderr)
        output = json.loads(result.stdout)
        self.assertEqual(output['status'], 'CONDITIONS_MISSING')
        self.assertEqual(output['baseline'], {'status': 'NOT_ATTEMPTED'})
        self.assertIn('GETTER_MISSING:futures_positions',
                      output['bundle']['blockers'])
        self.assertNotIn('historical_baseline_schema', self._baseline_tables())

    def test_pending_execution_blocks_apply_and_remains(self):
        snapshot = self._snapshot()
        db = sqlite3.connect(self.fixture.source.execution, isolation_level=None)
        try:
            db.executescript('''
                CREATE TABLE receipt_pending_executions(
                    account_key TEXT, order_id TEXT, execution_id TEXT,
                    ref_id TEXT, body TEXT, first_seen_at TEXT,
                    PRIMARY KEY(account_key,order_id,execution_id));
                INSERT INTO receipt_pending_executions VALUES(
                    'account','order','execution','ref','{}',
                    '2026-10-19T14:00:00+00:00');
            ''')
            db.commit()
        finally:
            db.close()
        self.fixture.source._refresh_pins()
        self.fixture.pins_path.write_text(json.dumps(self.fixture.source.expected))
        os.chmod(self.fixture.pins_path, 0o600)
        manifest, path = self._manifest(snapshot, name='pending.json')
        args = self._args(manifest, path)
        output, code = self._run(args, fake=self._fake(snapshot))
        self.assertEqual(code, 4, output)
        self.assertIn('BASELINE_GLOBAL_EXECUTION_PENDING', output['blockers'])
        self.assertNotIn('historical_baseline_schema', self._baseline_tables())
        db = sqlite3.connect(self.fixture.source.execution)
        try:
            self.assertEqual(db.execute(
                'SELECT count(*) FROM receipt_pending_executions').fetchone()[0],
                1)
        finally:
            db.close()
