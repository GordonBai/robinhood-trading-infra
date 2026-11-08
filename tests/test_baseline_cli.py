"""End-to-end and injected tests for the read-only historical-baseline CLI."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from martin_agentic import baseline_cli
from martin_agentic.audit import canonical
from martin_agentic.qualification import source_fingerprint
from martin_agentic.source_evidence import SourceEvidenceProvider
from martin_agentic.sqlite_readonly import SQLiteReadOnlyError, readonly_uri
from test_baseline_evidence import BundleFixture, account_data
from test_broker import order
from test_historical_baseline import (
    AT, EXPIRES, NOW, evidence, historical_snapshot)
from test_source_evidence import (
    SourceFixture, _acquire_once, _start_holder, _stop_holder)
from trading_contracts.values import digest
from trading_infra.historical_baseline import (
    apply_baseline, plan_baseline, validate_baseline)


REPO = Path(__file__).resolve().parents[1]


def _cli_environment():
    env = dict(os.environ)
    paths = [str(REPO / 'src')]
    if env.get('PYTHONPATH'):
        paths.append(env['PYTHONPATH'])
    env['PYTHONPATH'] = os.pathsep.join(paths)
    return env


class MutableClock:
    def __init__(self, start):
        self.current = start

    def __call__(self):
        return self.current

    def advance(self, seconds):
        self.current += timedelta(seconds=seconds)


class SequenceClock:
    def __init__(self, values):
        self.values = list(values)
        self.index = 0

    def __call__(self):
        value = self.values[min(self.index, len(self.values) - 1)]
        self.index += 1
        return value


def run_cli(*args):
    return subprocess.run(
        [sys.executable, '-c',
         'from martin_agentic.cli import main; raise SystemExit(main())',
         *args],
        cwd=REPO, env=_cli_environment(), capture_output=True, text=True,
        timeout=40)


def consistent_evidence(account_key, policy):
    value = evidence(account_key)
    for section in ('source', 'lineage'):
        value[section]['policy_sha256'] = digest(policy)
        value[section]['source_fingerprint'] = source_fingerprint()
    return value


def tree_state(root):
    root = Path(root)
    files = {}
    directories = []
    tables = {}
    for path in sorted(root.rglob('*')):
        relative = path.relative_to(root).as_posix()
        if path.is_dir():
            directories.append(relative)
            continue
        if path.is_file():
            files[relative] = (path.read_bytes(), path.stat().st_mtime_ns)
            if path.suffix == '.sqlite':
                try:
                    db = sqlite3.connect(readonly_uri(path), uri=True)
                    try:
                        tables[relative] = sorted(
                            row[0] for row in db.execute(
                                "SELECT name FROM sqlite_master "
                                "WHERE type='table'"))
                    finally:
                        db.close()
                except (sqlite3.Error, SQLiteReadOnlyError):
                    tables[relative] = ['<invalid>']
    return {'files': files, 'directories': directories, 'tables': tables}


class CliFixture:
    def __init__(self, root):
        self.temp = Path(root)
        self.temp.chmod(0o700)
        self.source = SourceFixture(self.temp)
        db = sqlite3.connect(self.source.execution)
        try:
            db.execute('ALTER TABLE execution_intents ADD COLUMN state TEXT')
            db.commit()
        finally:
            db.close()
        self.source._refresh_pins()
        self.bundle = BundleFixture(self.temp / 'bundle')
        self.refresh_bundle_clock()
        self.policy_path = self.temp / 'policy.json'
        self.policy_path.write_text(json.dumps(self.source.policy))
        os.chmod(self.policy_path, 0o600)
        self.pins_path = self.temp / 'pins.json'
        self.pins_path.write_text(json.dumps(self.source.expected))
        os.chmod(self.pins_path, 0o600)

    def refresh_bundle_clock(self, now=None):
        now = now or datetime.now(timezone.utc)
        base = now - timedelta(seconds=10)
        for page in self.bundle.pages:
            if page['pass'] == 1:
                started = base
                completed = base + timedelta(seconds=1)
            else:
                started = base + timedelta(seconds=2)
                completed = base + timedelta(seconds=3)
            page['started_at'] = started.isoformat()
            page['completed_at'] = completed.isoformat()
        self.bundle.write_manifest()

    def source_args(self):
        return [
            '--bundle-root', str(self.bundle.root),
            '--policy', str(self.policy_path),
            '--pins', str(self.pins_path),
            '--execution-db', str(self.source.execution),
            '--risk-db', str(self.source.risk),
            '--host-dir', str(self.source.host),
            '--root', str(self.source.root),
            '--isolated-root',
        ]

    def plan_args(self):
        now = datetime.now(timezone.utc)
        return {
            'bundle_root': str(self.bundle.root),
            'policy': str(self.policy_path),
            'pins': str(self.pins_path),
            'execution_db': str(self.source.execution),
            'risk_db': str(self.source.risk),
            'host_dir': str(self.source.host),
            'root': str(self.source.root),
            'isolated_root': True,
            'cutoff': (now - timedelta(minutes=1)).isoformat(),
            'expires_at': (now + timedelta(minutes=5)).isoformat(),
        }

    def write_manifest(self, manifest, name='manifest.json'):
        path = self.temp / name
        path.write_text(json.dumps(manifest))
        os.chmod(path, 0o600)
        return path

    def legal_source_evidence(self):
        provider = SourceEvidenceProvider(
            root=self.source.root, isolated=True,
            execution_db=self.source.execution, risk_db=self.source.risk,
            host_dir=self.source.host, expected=self.source.expected,
            policy=self.source.policy, clock=lambda: NOW)
        result = provider.read()
        assert result['status'] == 'READY', result
        return result


class HistoricalBaselineCliTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.fixture = CliFixture(self.temp.name)

    def _assert_no_write(self, before):
        self.assertEqual(tree_state(self.fixture.temp), before)

    def test_help_and_plan_conditions_missing_end_to_end(self):
        help_result = run_cli('historical-baseline', '--help')
        self.assertEqual(help_result.returncode, 0)
        self.assertIn('plan', help_result.stdout)
        self.assertIn('validate', help_result.stdout)
        self.assertIn('status', help_result.stdout)
        self.assertIn('apply', help_result.stdout)

        before = tree_state(self.fixture.temp)
        result = run_cli('historical-baseline', 'plan',
                         *self.fixture.source_args(),
                         '--cutoff', self.fixture.plan_args()['cutoff'],
                         '--expires-at', self.fixture.plan_args()['expires_at'])
        self.assertEqual(result.returncode, 3, result.stderr)
        output = json.loads(result.stdout)
        self.assertEqual(output['status'], 'CONDITIONS_MISSING')
        self.assertEqual(output['source']['status'], 'READY')
        self.assertIn('GETTER_MISSING:futures_positions',
                      output['bundle']['blockers'])
        self.assertIn('SETTLEMENT_SOURCE_MISSING',
                      output['bundle']['blockers'])
        self.assertEqual(output['baseline'], {'status': 'NOT_ATTEMPTED'})
        self.assertFalse(output['production_ready'])
        self.assertFalse(output['live_enabled'])
        self.assertFalse(output['execution_authorized'])
        self.assertNotIn('SIM-0000', result.stdout)
        self._assert_no_write(before)

    def test_missing_conditions_never_promote_candidate_or_validate(self):
        plan_args = SimpleNamespace(**self.fixture.plan_args())
        with patch('martin_agentic.baseline_cli.plan_baseline',
                   side_effect=AssertionError('must not plan incomplete input')):
            output, code = baseline_cli._run_plan(
                plan_args, clock=lambda: datetime.now(timezone.utc))
        self.assertEqual(code, 3, output)
        self.assertEqual(output['status'], 'CONDITIONS_MISSING')
        self.assertIs(output['bundle']['candidate_snapshot_present'], True)
        self.assertEqual(output['baseline'], {'status': 'NOT_ATTEMPTED'})

        snapshot = historical_snapshot()
        snapshot['account_key'] = self.fixture.source.account_key
        snapshot['account_mask'] = '••••0000'
        ev = consistent_evidence(
            self.fixture.source.account_key, self.fixture.source.policy)
        manifest = plan_baseline(
            snapshot, account_key=self.fixture.source.account_key,
            cutoff_utc=AT, policy_sha256=digest(self.fixture.source.policy),
            source_fingerprint=source_fingerprint(), evidence=ev,
            expires_at='2099-01-01T00:00:00+00:00', now=NOW)
        manifest_path = self.fixture.write_manifest(manifest, 'incomplete.json')
        validate_args = SimpleNamespace(
            **{**self.fixture.plan_args(),
               'db': str(self.fixture.source.execution),
               'manifest': str(manifest_path),
               'manifest_pin': manifest['manifest_sha256']})
        with patch('martin_agentic.baseline_cli.validate_baseline',
                   side_effect=AssertionError('must not validate incomplete input')):
            output, code = baseline_cli._run_validate(
                validate_args, clock=lambda: datetime.now(timezone.utc))
        self.assertEqual(code, 3, output)
        self.assertEqual(output['status'], 'CONDITIONS_MISSING')
        self.assertEqual(output['baseline'], {'status': 'NOT_ATTEMPTED'})

    def test_plan_owner_active_blocks_then_restarts_and_releases(self):
        before = tree_state(self.fixture.temp)
        holder = _start_holder(self.fixture.source.lock)
        try:
            result = run_cli('historical-baseline', 'plan',
                             *self.fixture.source_args(),
                             '--cutoff', self.fixture.plan_args()['cutoff'],
                             '--expires-at', self.fixture.plan_args()['expires_at'])
        finally:
            _stop_holder(holder)
        self.assertEqual(result.returncode, 3, result.stderr)
        output = json.loads(result.stdout)
        self.assertEqual(output['status'], 'BLOCKED')
        self.assertIn('SOURCE_OWNER_RUNNING', output['source']['blockers'])
        self._assert_no_write(before)

        retry = run_cli('historical-baseline', 'plan',
                        *self.fixture.source_args(),
                        '--cutoff', self.fixture.plan_args()['cutoff'],
                        '--expires-at', self.fixture.plan_args()['expires_at'])
        self.assertEqual(retry.returncode, 3, retry.stderr)
        acquired = _acquire_once(self.fixture.source.lock)
        self.assertEqual(acquired.returncode, 0, acquired.stdout)

    def test_plan_invalid_raw_and_wrong_source_pins_are_json_errors(self):
        page = self.fixture.bundle.pages[0]
        raw_path = self.fixture.bundle.root / page['raw_path']
        raw_path.write_bytes(b'{"data":{"accounts":[]}}')
        before = tree_state(self.fixture.temp)
        result = run_cli('historical-baseline', 'plan',
                         *self.fixture.source_args(),
                         '--cutoff', self.fixture.plan_args()['cutoff'],
                         '--expires-at', self.fixture.plan_args()['expires_at'])
        self.assertEqual(result.returncode, 5, result.stderr)
        output = json.loads(result.stdout)
        self.assertEqual(output['status'], 'INVALID_INPUT')
        self.assertIn('BUNDLE_RAW_HASH_MISMATCH', output['blockers'])
        self._assert_no_write(before)

        second = tempfile.TemporaryDirectory()
        self.addCleanup(second.cleanup)
        self.fixture = CliFixture(second.name)
        pins = dict(self.fixture.source.expected)
        pins['state_generation'] = '0' * 64
        self.fixture.pins_path.write_text(json.dumps(pins))
        before = tree_state(self.fixture.temp)
        result = run_cli('historical-baseline', 'plan',
                         *self.fixture.source_args(),
                         '--cutoff', self.fixture.plan_args()['cutoff'],
                         '--expires-at', self.fixture.plan_args()['expires_at'])
        self.assertEqual(result.returncode, 3, result.stderr)
        output = json.loads(result.stdout)
        self.assertEqual(output['status'], 'BLOCKED')
        self.assertIn('SOURCE_GENERATION_PIN_MISMATCH',
                      output['source']['blockers'])
        self._assert_no_write(before)

    def test_input_symlink_permissions_and_account_mismatch(self):
        pins_link = self.fixture.temp / 'pins-link.json'
        pins_link.symlink_to(self.fixture.pins_path)
        args = self.fixture.source_args()
        args[args.index('--pins') + 1] = str(pins_link)
        result = run_cli('historical-baseline', 'plan', *args,
                         '--cutoff', self.fixture.plan_args()['cutoff'],
                         '--expires-at', self.fixture.plan_args()['expires_at'])
        self.assertEqual(result.returncode, 5, result.stderr)
        self.assertEqual(json.loads(result.stdout)['status'], 'INVALID_INPUT')

        os.chmod(self.fixture.pins_path, 0o666)
        result = run_cli('historical-baseline', 'plan',
                         *self.fixture.source_args(),
                         '--cutoff', self.fixture.plan_args()['cutoff'],
                         '--expires-at', self.fixture.plan_args()['expires_at'])
        self.assertEqual(result.returncode, 5, result.stderr)
        self.assertEqual(json.loads(result.stdout)['status'], 'INVALID_INPUT')
        os.chmod(self.fixture.pins_path, 0o600)

        link_db = self.fixture.temp / 'status-link.sqlite'
        link_db.symlink_to(self.fixture.source.execution)
        result = run_cli('historical-baseline', 'status', '--db', str(link_db),
                         '--root', str(self.fixture.temp), '--isolated-root')
        self.assertEqual(result.returncode, 5, result.stderr)
        self.assertEqual(json.loads(result.stdout)['status'], 'INVALID_INPUT')

        page = next(page for page in self.fixture.bundle.pages
                    if page['tool'] == 'get_accounts')
        changed = account_data()
        changed['accounts'][0]['account_number'] = 'OTHER-0000'
        self.fixture.bundle.rewrite_raw(page, changed)
        setting = next(page for page in self.fixture.bundle.pages
                       if page['tool'] == 'get_trade_approval_setting')
        self.fixture.bundle.rewrite_raw(setting, {
            'setting': {'account_number': 'OTHER-0000',
                        'human_must_approve_trades': False}})
        for page in self.fixture.bundle.pages:
            if 'account_number' in page['args']:
                page['args']['account_number'] = 'OTHER-0000'
        self.fixture.bundle.write_manifest()
        result = run_cli('historical-baseline', 'plan',
                         *self.fixture.source_args(),
                         '--cutoff', self.fixture.plan_args()['cutoff'],
                         '--expires-at', self.fixture.plan_args()['expires_at'])
        self.assertEqual(result.returncode, 4, result.stderr)
        output = json.loads(result.stdout)
        self.assertEqual(output['status'], 'VALIDATION_FAILED')
        self.assertIn('CLI_ACCOUNT_MISMATCH', output['blockers'])

    def test_stale_and_future_bundle_are_rejected(self):
        self.fixture.refresh_bundle_clock(
            datetime.now(timezone.utc) - timedelta(minutes=2))
        result = run_cli('historical-baseline', 'plan',
                         *self.fixture.source_args(),
                         '--cutoff', self.fixture.plan_args()['cutoff'],
                         '--expires-at', self.fixture.plan_args()['expires_at'])
        self.assertEqual(result.returncode, 5, result.stderr)
        self.assertIn('BUNDLE_STALE_OBSERVATION',
                      json.loads(result.stdout)['blockers'])

        self.fixture.refresh_bundle_clock(
            datetime.now(timezone.utc) + timedelta(minutes=2))
        result = run_cli('historical-baseline', 'plan',
                         *self.fixture.source_args(),
                         '--cutoff', self.fixture.plan_args()['cutoff'],
                         '--expires-at', self.fixture.plan_args()['expires_at'])
        self.assertEqual(result.returncode, 5, result.stderr)
        self.assertIn('BUNDLE_FUTURE_OBSERVATION',
                      json.loads(result.stdout)['blockers'])

    def test_status_absent_present_invalid_missing_and_no_creation(self):
        missing = self.fixture.temp / 'missing.sqlite'
        status_args = ['historical-baseline', 'status', '--db', str(missing),
                       '--root', str(self.fixture.temp), '--isolated-root']
        result = run_cli(*status_args)
        self.assertEqual(result.returncode, 4, result.stderr)
        output = json.loads(result.stdout)
        self.assertEqual(output['status'], 'MISSING')
        self.assertFalse(missing.exists())

        absent = self.fixture.temp / 'absent.sqlite'
        sqlite3.connect(absent).close()
        before = tree_state(self.fixture.temp)
        result = run_cli('historical-baseline', 'status', '--db', str(absent),
                         '--root', str(self.fixture.temp), '--isolated-root')
        self.assertEqual(result.returncode, 0, result.stderr)
        output = json.loads(result.stdout)
        self.assertEqual(output['status'], 'ABSENT')
        self.assertFalse(output['source_verified'])
        self.assertIsNone(output['source_diagnostics'])
        self._assert_no_write(before)

        present = self.fixture.temp / 'present.sqlite'
        snapshot = historical_snapshot()
        snapshot['account_key'] = self.fixture.source.account_key
        snapshot['account_mask'] = '••••0000'
        ev = consistent_evidence(
            self.fixture.source.account_key, self.fixture.source.policy)
        manifest = plan_baseline(
            snapshot, account_key=self.fixture.source.account_key,
            cutoff_utc=AT, policy_sha256=digest(self.fixture.source.policy),
            source_fingerprint=source_fingerprint(), evidence=ev,
            expires_at=EXPIRES, now=NOW)
        db = sqlite3.connect(present, isolation_level=None)
        try:
            apply_baseline(
                db, manifest, manifest_pin=manifest['manifest_sha256'],
                approval_scope='test-only', now=NOW, evidence=ev,
                snapshot=snapshot)
        finally:
            db.close()
        before = tree_state(self.fixture.temp)
        result = run_cli('historical-baseline', 'status', '--db', str(present),
                         '--root', str(self.fixture.temp), '--isolated-root')
        self.assertEqual(result.returncode, 0, result.stderr)
        output = json.loads(result.stdout)
        self.assertEqual(output['status'], 'PRESENT')
        self.assertEqual(len(output['baseline']['active']), 1)
        self.assertNotIn('SIM-0000', result.stdout)
        self._assert_no_write(before)

        partial = self.fixture.temp / 'partial.sqlite'
        db = sqlite3.connect(partial)
        try:
            db.execute(
                'CREATE TABLE historical_baseline_schema(version INTEGER PRIMARY KEY)')
            db.commit()
        finally:
            db.close()
        result = run_cli('historical-baseline', 'status', '--db', str(partial),
                         '--root', str(self.fixture.temp), '--isolated-root')
        self.assertEqual(result.returncode, 4, result.stderr)
        self.assertEqual(json.loads(result.stdout)['status'], 'INVALID')

        future = self.fixture.temp / 'future.sqlite'
        future.write_bytes(present.read_bytes())
        db = sqlite3.connect(future)
        try:
            db.execute('UPDATE historical_baseline_schema SET version=99')
            db.commit()
        finally:
            db.close()
        result = run_cli('historical-baseline', 'status', '--db', str(future),
                         '--root', str(self.fixture.temp), '--isolated-root')
        self.assertEqual(result.returncode, 4, result.stderr)
        output = json.loads(result.stdout)
        self.assertEqual(output['status'], 'INVALID')
        self.assertIn('BASELINE_SCHEMA_VERSION_UNSUPPORTED',
                      output['baseline']['reason'])

        corrupt = self.fixture.temp / 'corrupt.sqlite'
        corrupt.write_bytes(b'not a sqlite database')
        before = tree_state(self.fixture.temp)
        result = run_cli('historical-baseline', 'status', '--db', str(corrupt),
                         '--root', str(self.fixture.temp), '--isolated-root')
        self.assertEqual(result.returncode, 4, result.stderr)
        self.assertEqual(json.loads(result.stdout)['status'], 'INVALID')
        self._assert_no_write(before)

    def test_validate_bundle_conditions_pin_and_expiry_end_to_end(self):
        snapshot = historical_snapshot()
        snapshot['account_key'] = self.fixture.source.account_key
        snapshot['account_mask'] = '••••0000'
        ev = consistent_evidence(
            self.fixture.source.account_key, self.fixture.source.policy)
        manifest = plan_baseline(
            snapshot, account_key=self.fixture.source.account_key,
            cutoff_utc=AT, policy_sha256=digest(self.fixture.source.policy),
            source_fingerprint=source_fingerprint(), evidence=ev,
            expires_at='2099-01-01T00:00:00+00:00', now=NOW)
        manifest_path = self.fixture.write_manifest(manifest)
        expired_manifest = plan_baseline(
            snapshot, account_key=self.fixture.source.account_key,
            cutoff_utc=AT, policy_sha256=digest(self.fixture.source.policy),
            source_fingerprint=source_fingerprint(), evidence=ev,
            expires_at=EXPIRES, now=NOW)
        expired_path = self.fixture.write_manifest(
            expired_manifest, 'expired.json')

        before = tree_state(self.fixture.temp)
        result = run_cli(
            'historical-baseline', 'validate',
            *self.fixture.source_args(),
            '--db', str(self.fixture.source.execution),
            '--manifest', str(manifest_path),
            '--manifest-pin', manifest['manifest_sha256'])
        self.assertEqual(result.returncode, 3, result.stderr)
        output = json.loads(result.stdout)
        self.assertEqual(output['status'], 'CONDITIONS_MISSING')
        self.assertEqual(output['source']['status'], 'READY')
        self.assertIn('GETTER_MISSING:futures_positions',
                      output['bundle']['blockers'])
        self.assertNotIn('SIM-0000', result.stdout)
        self._assert_no_write(before)

        wrong = run_cli(
            'historical-baseline', 'validate',
            *self.fixture.source_args(),
            '--db', str(self.fixture.source.execution),
            '--manifest', str(manifest_path),
            '--manifest-pin', '0' * 64)
        self.assertEqual(wrong.returncode, 4, wrong.stderr)
        wrong_output = json.loads(wrong.stdout)
        self.assertEqual(wrong_output['status'], 'VALIDATION_FAILED')
        self.assertIn('BASELINE_MANIFEST_PIN_MISMATCH',
                      wrong_output['blockers'])

        expired = run_cli(
            'historical-baseline', 'validate',
            *self.fixture.source_args(),
            '--db', str(self.fixture.source.execution),
            '--manifest', str(expired_path),
            '--manifest-pin', expired_manifest['manifest_sha256'])
        self.assertEqual(expired.returncode, 4, expired.stderr)
        self.assertIn('BASELINE_PLAN_EXPIRED',
                      json.loads(expired.stdout)['blockers'])

    def test_legal_plan_and_validate_via_internal_injection(self):
        snapshot = historical_snapshot()
        snapshot['account_key'] = self.fixture.source.account_key
        snapshot['account_mask'] = '••••0000'
        fake = {
            'account': {'account_key': self.fixture.source.account_key,
                        'account_mask': '••••0000'},
            'raw_chain_sha256': 'a' * 64, 'observed_at': AT,
            'collection_window_seconds': 3.0, 'blockers': [],
            'candidate_snapshot': snapshot, 'ready': True,
        }
        source_result = self.fixture.legal_source_evidence()
        manifest = plan_baseline(
            snapshot, account_key=self.fixture.source.account_key,
            cutoff_utc=AT, policy_sha256=digest(self.fixture.source.policy),
            source_fingerprint=source_fingerprint(),
            evidence=source_result['evidence'], expires_at=EXPIRES, now=NOW)
        manifest_path = self.fixture.write_manifest(manifest, 'lease.json')
        args = SimpleNamespace(**self.fixture.plan_args())
        args.cutoff = AT
        args.expires_at = EXPIRES
        validate_args = SimpleNamespace(
            **{**self.fixture.plan_args(),
               'db': str(self.fixture.source.execution),
               'manifest': str(manifest_path),
               'manifest_pin': manifest['manifest_sha256']})
        before = tree_state(self.fixture.temp)
        with patch('martin_agentic.baseline_cli.normalize_bundle',
                   return_value=fake):
            output, code = baseline_cli._run_plan(args, clock=lambda: NOW)
        self.assertEqual(code, 0, output)
        self.assertEqual(output['status'], 'PLANNED')
        self.assertNotIn('SIM-0000', json.dumps(output))

        with patch('martin_agentic.baseline_cli.normalize_bundle',
                   return_value=fake):
            output, code = baseline_cli._run_validate(validate_args, clock=lambda: NOW)
        self.assertEqual(code, 0, output)
        self.assertEqual(output['status'], 'VALIDATED')
        self.assertNotIn('SIM-0000', json.dumps(output))
        self._assert_no_write(before)

    def test_validate_holds_owner_lock_and_detects_replacement(self):
        snapshot = historical_snapshot()
        snapshot['account_key'] = self.fixture.source.account_key
        snapshot['account_mask'] = '••••0000'
        fake = {
            'account': {'account_key': self.fixture.source.account_key,
                        'account_mask': '••••0000'},
            'raw_chain_sha256': 'b' * 64, 'observed_at': AT,
            'collection_window_seconds': 3.0, 'blockers': [],
            'candidate_snapshot': snapshot, 'ready': True,
        }
        source_result = self.fixture.legal_source_evidence()
        manifest = plan_baseline(
            snapshot, account_key=self.fixture.source.account_key,
            cutoff_utc=AT, policy_sha256=digest(self.fixture.source.policy),
            source_fingerprint=source_fingerprint(),
            evidence=source_result['evidence'], expires_at=EXPIRES, now=NOW)
        manifest_path = self.fixture.write_manifest(manifest, 'lock.json')
        args = SimpleNamespace(
            **{**self.fixture.plan_args(),
               'db': str(self.fixture.source.execution),
               'manifest': str(manifest_path),
               'manifest_pin': manifest['manifest_sha256']})
        attempts = []

        def observed(db, current, **kwargs):
            attempts.append(_acquire_once(self.fixture.source.lock))
            return validate_baseline(db, current, **kwargs)

        with patch('martin_agentic.baseline_cli.normalize_bundle',
                   return_value=fake), \
                patch('martin_agentic.baseline_cli.validate_baseline',
                      side_effect=observed):
            output, code = baseline_cli._run_validate(args, clock=lambda: NOW)
        self.assertEqual(code, 0, output)
        self.assertEqual(output['status'], 'VALIDATED')
        self.assertEqual(attempts[0].returncode, 3, attempts[0].stdout)
        acquired = _acquire_once(self.fixture.source.lock)
        self.assertEqual(acquired.returncode, 0, acquired.stdout)

        replaced = Path(str(self.fixture.source.lock) + '.saved')
        holder = None

        def replace_lock(db, current, **kwargs):
            nonlocal holder
            self.fixture.source.lock.rename(replaced)
            self.fixture.source.lock.touch(mode=0o600)
            holder = _start_holder(self.fixture.source.lock)
            return validate_baseline(db, current, **kwargs)

        try:
            with patch('martin_agentic.baseline_cli.normalize_bundle',
                       return_value=fake), \
                    patch('martin_agentic.baseline_cli.validate_baseline',
                          side_effect=replace_lock):
                output, code = baseline_cli._run_validate(args, clock=lambda: NOW)
        finally:
            if holder is not None:
                _stop_holder(holder)
        self.assertEqual(code, 3, output)
        self.assertEqual(output['status'], 'BLOCKED')
        self.assertIn('SOURCE_OWNER_LOCK_REPLACED',
                      output['source']['blockers'])

    def test_validate_rejects_bundle_and_source_changes_during_validation(self):
        snapshot = historical_snapshot()
        snapshot['account_key'] = self.fixture.source.account_key
        snapshot['account_mask'] = '••••0000'
        fake = {
            'account': {'account_key': self.fixture.source.account_key,
                        'account_mask': '••••0000'},
            'raw_chain_sha256': 'd' * 64, 'observed_at': AT,
            'collection_window_seconds': 3.0, 'blockers': [],
            'candidate_snapshot': snapshot, 'ready': True,
        }
        source_result = self.fixture.legal_source_evidence()
        manifest = plan_baseline(
            snapshot, account_key=self.fixture.source.account_key,
            cutoff_utc=AT, policy_sha256=digest(self.fixture.source.policy),
            source_fingerprint=source_fingerprint(),
            evidence=source_result['evidence'], expires_at=EXPIRES, now=NOW)
        manifest_path = self.fixture.write_manifest(
            manifest, 'moving-inputs.json')
        args = SimpleNamespace(
            **{**self.fixture.plan_args(),
               'db': str(self.fixture.source.execution),
               'manifest': str(manifest_path),
               'manifest_pin': manifest['manifest_sha256']})

        changed = deepcopy(fake)
        changed['raw_chain_sha256'] = 'e' * 64
        with patch('martin_agentic.baseline_cli.normalize_bundle',
                   side_effect=[fake, changed]):
            output, code = baseline_cli._run_validate(args, clock=lambda: NOW)
        self.assertEqual(code, 4, output)
        self.assertEqual(output['status'], 'VALIDATION_FAILED')
        self.assertIn('CLI_BUNDLE_CHANGED_DURING_VALIDATION',
                      output['blockers'])

        def change_source(db, current, **kwargs):
            result = validate_baseline(db, current, **kwargs)
            risk = sqlite3.connect(self.fixture.source.risk)
            try:
                state = json.loads(risk.execute(
                    'SELECT payload FROM account_risk WHERE id=1').fetchone()[0])
                state['blocked'] = not state.get('blocked', False)
                risk.execute('UPDATE account_risk SET payload=? WHERE id=1',
                             (canonical(state),))
                risk.commit()
            finally:
                risk.close()
            return result

        with patch('martin_agentic.baseline_cli.normalize_bundle',
                   return_value=fake), \
                patch('martin_agentic.baseline_cli.validate_baseline',
                      side_effect=change_source):
            output, code = baseline_cli._run_validate(args, clock=lambda: NOW)
        self.assertEqual(code, 3, output)
        self.assertEqual(output['status'], 'BLOCKED')
        self.assertEqual(output['baseline'], {'status': 'NOT_VALIDATED'})

    def test_sampled_clock_checks_freshness_expiry_and_final_rereads(self):
        snapshot = historical_snapshot()
        snapshot['account_key'] = self.fixture.source.account_key
        snapshot['account_mask'] = '••••0000'
        fake = {
            'account': {'account_key': self.fixture.source.account_key,
                        'account_mask': '••••0000'},
            'raw_chain_sha256': 'f' * 64, 'observed_at': AT,
            'collection_window_seconds': 3.0, 'blockers': [],
            'candidate_snapshot': snapshot, 'ready': True,
        }
        plan_args = SimpleNamespace(**self.fixture.plan_args())
        plan_args.cutoff = AT
        plan_args.expires_at = EXPIRES

        clock = MutableClock(NOW)

        def slow_normalize(loaded, *, now=None):
            clock.advance(45)
            return fake

        with patch('martin_agentic.baseline_cli.normalize_bundle',
                   side_effect=slow_normalize):
            output, code = baseline_cli._run_plan(plan_args, clock=clock)
        self.assertEqual(code, 4, output)
        self.assertEqual(output['status'], 'VALIDATION_FAILED')
        self.assertIn('BASELINE_SNAPSHOT_STALE_OR_CUTOFF',
                      output['blockers'])

        clock = MutableClock(NOW)
        short_args = SimpleNamespace(**vars(plan_args))
        short_args.expires_at = (NOW + timedelta(seconds=20)).isoformat()
        real_plan = baseline_cli.plan_baseline

        def advance_after_plan(*args, **kwargs):
            value = real_plan(*args, **kwargs)
            clock.advance(25)
            return value

        with patch('martin_agentic.baseline_cli.normalize_bundle',
                   return_value=fake), \
                patch('martin_agentic.baseline_cli.plan_baseline',
                      side_effect=advance_after_plan):
            output, code = baseline_cli._run_plan(short_args, clock=clock)
        self.assertEqual(code, 4, output)
        self.assertIn('BASELINE_PLAN_EXPIRED', output['blockers'])

        source_result = self.fixture.legal_source_evidence()
        short_manifest = plan_baseline(
            snapshot, account_key=self.fixture.source.account_key,
            cutoff_utc=AT, policy_sha256=digest(self.fixture.source.policy),
            source_fingerprint=source_fingerprint(),
            evidence=source_result['evidence'],
            expires_at=(NOW + timedelta(seconds=20)).isoformat(), now=NOW)
        short_path = self.fixture.write_manifest(short_manifest, 'short.json')
        validate_args = SimpleNamespace(
            **{**self.fixture.plan_args(),
               'db': str(self.fixture.source.execution),
               'manifest': str(short_path),
               'manifest_pin': short_manifest['manifest_sha256']})
        clock = MutableClock(NOW)
        real_validate = baseline_cli.validate_baseline

        def advance_after_validate(*args, **kwargs):
            value = real_validate(*args, **kwargs)
            clock.advance(25)
            return value

        with patch('martin_agentic.baseline_cli.normalize_bundle',
                   return_value=fake), \
                patch('martin_agentic.baseline_cli.validate_baseline',
                      side_effect=advance_after_validate):
            output, code = baseline_cli._run_validate(
                validate_args, clock=clock)
        self.assertEqual(code, 4, output)
        self.assertEqual(output['status'], 'VALIDATION_FAILED')
        self.assertIn('BASELINE_PLAN_EXPIRED', output['blockers'])

        long_manifest = plan_baseline(
            snapshot, account_key=self.fixture.source.account_key,
            cutoff_utc=AT, policy_sha256=digest(self.fixture.source.policy),
            source_fingerprint=source_fingerprint(),
            evidence=source_result['evidence'],
            expires_at='2099-01-01T00:00:00+00:00', now=NOW)
        long_path = self.fixture.write_manifest(long_manifest, 'long.json')
        long_args = SimpleNamespace(
            **{**self.fixture.plan_args(),
               'db': str(self.fixture.source.execution),
               'manifest': str(long_path),
               'manifest_pin': long_manifest['manifest_sha256']})
        clock = MutableClock(NOW)

        def checked_fake(loaded, *, now=None):
            observed = datetime.fromisoformat(fake['observed_at'])
            if observed > now:
                raise baseline_cli.BundleError('BUNDLE_FUTURE_OBSERVATION')
            if (now - observed).total_seconds() > 30:
                raise baseline_cli.BundleError('BUNDLE_STALE_OBSERVATION')
            return fake

        def advance_after_long_validate(*args, **kwargs):
            value = real_validate(*args, **kwargs)
            clock.advance(45)
            return value

        with patch('martin_agentic.baseline_cli.normalize_bundle',
                   side_effect=checked_fake), \
                patch('martin_agentic.baseline_cli.validate_baseline',
                      side_effect=advance_after_long_validate):
            output, code = baseline_cli._run_validate(
                long_args, clock=clock)
        self.assertNotEqual(code, 0, output)
        self.assertNotEqual(output['status'], 'VALIDATED')

        bad = baseline_cli._run_plan(
            plan_args, clock=lambda: 'not-a-clock')
        self.assertEqual(bad[1], 5, bad[0])
        self.assertIn('CLI_CLOCK_INVALID', bad[0]['blockers'])
        backwards = baseline_cli._run_plan(
            plan_args, clock=SequenceClock(
                [NOW, NOW - timedelta(seconds=1)]))
        self.assertEqual(backwards[1], 5, backwards[0])
        self.assertIn('CLI_CLOCK_WENT_BACKWARDS',
                      backwards[0]['blockers'])

    def test_validate_global_unknown_gate_blocks(self):
        db = sqlite3.connect(self.fixture.source.execution)
        try:
            state = json.loads(db.execute(
                'SELECT payload FROM live_state WHERE id=1').fetchone()[0])
            state['trades'] = {'active': {
                'id': 'active', 'phase': 'OPEN', 'quantity': '1'}}
            db.execute('UPDATE live_state SET payload=? WHERE id=1',
                       (canonical(state),))
            db.commit()
        finally:
            db.close()
        self.fixture.source._refresh_pins()
        self.fixture.pins_path.write_text(
            json.dumps(self.fixture.source.expected))
        os.chmod(self.fixture.pins_path, 0o600)
        snapshot = historical_snapshot()
        snapshot['account_key'] = self.fixture.source.account_key
        snapshot['account_mask'] = '••••0000'
        fake = {
            'account': {'account_key': self.fixture.source.account_key,
                        'account_mask': '••••0000'},
            'raw_chain_sha256': 'c' * 64, 'observed_at': AT,
            'collection_window_seconds': 3.0, 'blockers': [],
            'candidate_snapshot': snapshot, 'ready': True,
        }
        source_result = self.fixture.legal_source_evidence()
        manifest = plan_baseline(
            snapshot, account_key=self.fixture.source.account_key,
            cutoff_utc=AT, policy_sha256=digest(self.fixture.source.policy),
            source_fingerprint=source_fingerprint(),
            evidence=source_result['evidence'], expires_at=EXPIRES, now=NOW)
        manifest_path = self.fixture.write_manifest(manifest, 'unknown.json')
        args = SimpleNamespace(
            **{**self.fixture.plan_args(),
               'db': str(self.fixture.source.execution),
               'manifest': str(manifest_path),
               'manifest_pin': manifest['manifest_sha256']})
        before = tree_state(self.fixture.temp)
        with patch('martin_agentic.baseline_cli.normalize_bundle',
                   return_value=fake):
            output, code = baseline_cli._run_validate(args, clock=lambda: NOW)
        self.assertEqual(code, 4, output)
        self.assertEqual(output['status'], 'VALIDATION_FAILED')
        self.assertIn('BASELINE_GLOBAL_TRADE_ACTIVE', output['blockers'])
        self._assert_no_write(before)


class IdentityCliTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.fixture = CliFixture(self.temp.name)
        orders = [order('buy-1', 'buy', '1', '1', 'filled'),
                  order('sell-1', 'sell', '1', '0', 'cancelled')]
        for item in orders:
            item['ref_id'] = 'ref-' + item['id']
        for page in self.fixture.bundle.pages:
            if page['tool'] == 'get_equity_orders':
                self.fixture.bundle.rewrite_raw(
                    page, {'orders': deepcopy(orders)})
        self.fixture.refresh_bundle_clock()

    def test_identity_scope_plan_is_explicit_and_narrow(self):
        args = self.fixture.plan_args()
        result = run_cli(
            'historical-baseline', 'plan', *self.fixture.source_args(),
            '--cutoff', args['cutoff'], '--expires-at', args['expires_at'],
            '--scope', 'terminal_equity_order_identity_v1')
        self.assertEqual(result.returncode, 0, result.stderr)
        output = json.loads(result.stdout)
        self.assertEqual(output['status'], 'PLANNED')
        plan = output['baseline']['plan']
        self.assertEqual(plan['baseline_scope'],
                         'terminal_equity_order_identity_v1')
        self.assertEqual(plan['counts'], {'orders': 2, 'executions': 1})
        self.assertEqual(plan['opening_balance_components'], [])
        self.assertFalse(plan['claims']['full_account_flat'])
