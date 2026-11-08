"""Offline tests for append-only historical-baseline revoke."""
from contextlib import contextmanager
from copy import deepcopy
from datetime import timedelta
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
from martin_agentic.source_evidence import SourceEvidenceProvider
from test_baseline_cli import (
    CliFixture, MutableClock, _start_holder, _stop_holder, tree_state)
from test_baseline_apply import SQLITE_WRITER
from test_historical_baseline import AT, EXPIRES, NOW, historical_snapshot
from trading_contracts.values import digest
from trading_infra.attribution import ExecutionAttribution
from trading_infra import historical_baseline as core
from trading_infra.historical_baseline import (
    HistoricalBaselineIndex, apply_baseline, plan_baseline, status_baseline)
from trading_infra.instance_accounting import InstanceAccounting
from trading_infra.receipts import ExecutionReceiptImporter


class BaselineRevokeTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.fixture = CliFixture(self.temp.name)

    def _snapshot(self):
        snapshot = historical_snapshot()
        snapshot['account_key'] = self.fixture.source.account_key
        snapshot['account_mask'] = '••••0000'
        return snapshot

    def _manifest(self, snapshot=None, name='apply.json', cutoff=AT):
        snapshot = snapshot or self._snapshot()
        evidence = self.fixture.legal_source_evidence()['evidence']
        manifest = plan_baseline(
            snapshot, account_key=self.fixture.source.account_key,
            cutoff_utc=cutoff, policy_sha256=digest(self.fixture.source.policy),
            source_fingerprint=__import__(
                'martin_agentic.qualification', fromlist=['source_fingerprint']
            ).source_fingerprint(), evidence=evidence,
            expires_at=EXPIRES, now=NOW)
        path = self.fixture.write_manifest(manifest, name)
        return snapshot, manifest, path

    def _args(self, manifest, path, **changes):
        args = SimpleNamespace(**self.fixture.plan_args())
        args.db = str(self.fixture.source.execution)
        args.manifest = str(path)
        args.manifest_pin = manifest['manifest_sha256']
        args.baseline_id = manifest['baseline_id']
        args.approval_scope = 'apply-scope'
        args.reason = 'test revoke'
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

    def _apply(self, snapshot, manifest, path, args=None):
        args = args or self._args(manifest, path)
        with patch('martin_agentic.baseline_cli.normalize_bundle',
                   return_value=self._fake(snapshot)):
            output, code = baseline_cli._run_apply(args, clock=lambda: NOW)
        self.assertEqual(code, 0, output)
        return output

    def _refresh_pins(self):
        self.fixture.source._refresh_pins()
        self.fixture.pins_path.write_text(json.dumps(self.fixture.source.expected))
        os.chmod(self.fixture.pins_path, 0o600)

    def _revoke(self, manifest, path, *, reason='test revoke',
                approval_scope='revoke-scope', clock=NOW, args=None):
        args = args or self._args(
            manifest, path, reason=reason,
            approval_scope=approval_scope)
        return baseline_cli._run_revoke(args, clock=lambda: clock)

    def _tables(self):
        db = sqlite3.connect(self.fixture.source.execution)
        try:
            return {row[0] for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
        finally:
            db.close()

    def _count(self, table):
        db = sqlite3.connect(self.fixture.source.execution)
        try:
            return db.execute('SELECT count(*) FROM ' + table).fetchone()[0]
        finally:
            db.close()

    def _events(self, baseline_id):
        db = sqlite3.connect(self.fixture.source.execution)
        try:
            return db.execute(
                'SELECT event_id,kind,occurred_at,detail FROM '
                'historical_baseline_event WHERE baseline_id=? ORDER BY kind',
                (baseline_id,)).fetchall()
        finally:
            db.close()

    def _risk(self):
        db = sqlite3.connect(self.fixture.source.risk)
        try:
            row = db.execute('SELECT payload FROM account_risk WHERE id=1').fetchone()
            return row[0] if row else None
        finally:
            db.close()

    def _status(self):
        db = sqlite3.connect(self.fixture.source.execution)
        try:
            return status_baseline(db)
        finally:
            db.close()

    def _prepare_revoked(self):
        snapshot, manifest, path = self._manifest()
        self._apply(snapshot, manifest, path)
        self._refresh_pins()
        output, code = self._revoke(manifest, path)
        self.assertEqual(code, 0, output)
        self.assertEqual(output['status'], 'REVOKED')
        return manifest, path
    def _fresh_revoked(self):
        fixture = CliFixture(tempfile.mkdtemp(dir=self.temp.name))
        snapshot = historical_snapshot()
        snapshot['account_key'] = fixture.source.account_key
        snapshot['account_mask'] = '••••0000'
        evidence = fixture.legal_source_evidence()['evidence']
        manifest = plan_baseline(
            snapshot, account_key=fixture.source.account_key, cutoff_utc=AT,
            policy_sha256=digest(fixture.source.policy),
            source_fingerprint=__import__(
                'martin_agentic.qualification',
                fromlist=['source_fingerprint']).source_fingerprint(),
            evidence=evidence, expires_at=EXPIRES, now=NOW)
        path = fixture.write_manifest(manifest, 'revoked.json')
        args = SimpleNamespace(**fixture.plan_args())
        args.db = str(fixture.source.execution)
        args.manifest = str(path)
        args.manifest_pin = manifest['manifest_sha256']
        args.baseline_id = manifest['baseline_id']
        args.approval_scope = 'apply-scope'
        args.reason = 'tamper'
        fake = {
            'account': {'account_key': fixture.source.account_key,
                        'account_mask': '••••0000'},
            'raw_chain_sha256': 'a' * 64, 'observed_at': AT,
            'collection_window_seconds': 3.0, 'blockers': [],
            'candidate_snapshot': snapshot, 'ready': True}
        with patch('martin_agentic.baseline_cli.normalize_bundle',
                   return_value=fake):
            self.assertEqual(baseline_cli._run_apply(
                args, clock=lambda: NOW)[1], 0)
        fixture.source._refresh_pins()
        fixture.pins_path.write_text(json.dumps(fixture.source.expected))
        os.chmod(fixture.pins_path, 0o600)
        args.approval_scope = 'revoke-scope'
        output, code = baseline_cli._run_revoke(args, clock=lambda: NOW)
        self.assertEqual(code, 0, output)
        return fixture, manifest, args

    def test_revoke_duplicate_and_old_manifest_terminal(self):
        snapshot, manifest, path = self._manifest()
        self._apply(snapshot, manifest, path)
        self._refresh_pins()
        orders = self._count('historical_baseline_order')
        executions = self._count('historical_baseline_execution')
        balances = self._count('historical_baseline_balance_component')
        risk_before = self._risk()
        db = sqlite3.connect(self.fixture.source.execution)
        try:
            manifest_before = db.execute(
                'SELECT manifest_json,approval_scope FROM '
                'historical_baseline_manifest WHERE baseline_id=?',
                (manifest['baseline_id'],)).fetchone()
        finally:
            db.close()
        output, code = self._revoke(manifest, path)
        self.assertEqual(code, 0, output)
        self.assertEqual(output['status'], 'REVOKED')
        self.assertTrue(output['revoked'])
        self.assertEqual(self._status()['active'], [])
        history = self._status()['history']
        self.assertEqual(history[0]['status'], 'REVOKED')
        self.assertEqual(history[0]['revoke_reason'], 'test revoke')
        events = self._events(manifest['baseline_id'])
        self.assertEqual(len(events), 2)
        self.assertEqual({row[1] for row in events},
                         {'BASELINE_APPLIED', 'BASELINE_REVOKED'})
        self.assertEqual(self._count('historical_baseline_order'), orders)
        self.assertEqual(self._count('historical_baseline_execution'), executions)
        self.assertEqual(self._count('historical_baseline_balance_component'),
                         balances)
        self.assertEqual(self._risk(), risk_before)
        db = sqlite3.connect(self.fixture.source.execution)
        try:
            self.assertEqual(db.execute(
                'SELECT manifest_json,approval_scope FROM '
                'historical_baseline_manifest WHERE baseline_id=?',
                (manifest['baseline_id'],)).fetchone(), manifest_before)
        finally:
            db.close()

        # A different reason/scope cannot overwrite the original audit.
        with patch.object(SourceEvidenceProvider, 'read',
                          side_effect=AssertionError('source must not be read')):
            duplicate, duplicate_code = self._revoke(
                manifest, path, reason='different', approval_scope='other')
        self.assertEqual(duplicate_code, 0, duplicate)
        self.assertEqual(duplicate['status'], 'ALREADY_REVOKED')
        self.assertEqual(duplicate['baseline']['revoke_reason'], 'test revoke')
        self.assertEqual(len(self._events(manifest['baseline_id'])), 2)
        self.assertEqual(self._status()['active'], [])

        with patch.object(SourceEvidenceProvider, 'read',
                          side_effect=AssertionError('source must not be read')):
            old, old_code = baseline_cli._run_apply(
                self._args(manifest, path), clock=lambda: NOW + timedelta(days=1))
        self.assertEqual(old_code, 4, old)
        self.assertEqual(old['status'], 'REVOKED')
        self.assertFalse(old['applied'])
        self.assertFalse(old['active'])
        self.assertEqual(old['baseline']['manifest_pin'],
                         manifest['manifest_sha256'])
        self.assertEqual(len(self._events(manifest['baseline_id'])), 2)

    def test_revoke_stale_source_pins_then_refresh(self):
        snapshot, manifest, path = self._manifest()
        self._apply(snapshot, manifest, path)
        # No _refresh_pins yet: old source pins must not be auto-refreshed.
        output, code = self._revoke(manifest, path)
        self.assertEqual(code, 3, output)
        self.assertEqual(output['status'], 'BLOCKED')
        self.assertIn('SOURCE_EXECUTION_HASH_MISMATCH',
                      output['source']['blockers'])
        self.assertEqual(self._status()['active'][0]['status'], 'APPLIED')
        self._refresh_pins()
        output, code = self._revoke(manifest, path)
        self.assertEqual(code, 0, output)
        self.assertEqual(output['status'], 'REVOKED')

    def test_revoke_rejects_unresolved_rows_without_deleting(self):
        snapshot, manifest, path = self._manifest()
        self._apply(snapshot, manifest, path)
        db = sqlite3.connect(self.fixture.source.execution, isolation_level=None)
        try:
            db.executescript('''
                CREATE TABLE receipt_pending_executions(
                    account_key TEXT, order_id TEXT, execution_id TEXT,
                    ref_id TEXT, body TEXT, first_seen_at TEXT,
                    PRIMARY KEY(account_key,order_id,execution_id));
                INSERT INTO receipt_pending_executions VALUES(
                    'account','order','execution','ref','{}',
                    '2026-10-16T14:00:00+00:00');
            ''')
            db.commit()
        finally:
            db.close()
        self._refresh_pins()
        output, code = self._revoke(manifest, path)
        self.assertEqual(code, 4, output)
        self.assertEqual(output['status'], 'REVOKE_REJECTED')
        self.assertIn('BASELINE_GLOBAL_EXECUTION_PENDING', output['blockers'])
        self.assertEqual(self._status()['active'][0]['status'], 'APPLIED')
        self.assertEqual(len(self._events(manifest['baseline_id'])), 1)
        self.assertEqual(self._count('receipt_pending_executions'), 1)

    def test_revoke_owner_wal_and_lock_replacement(self):
        snapshot, manifest, path = self._manifest()
        self._apply(snapshot, manifest, path)
        self._refresh_pins()
        holder = _start_holder(self.fixture.source.lock)
        try:
            output, code = self._revoke(manifest, path)
        finally:
            _stop_holder(holder)
        self.assertEqual(code, 3, output)
        self.assertEqual(output['status'], 'BLOCKED')
        self.assertIn('SOURCE_OWNER_RUNNING', output['source']['blockers'])

        wal = Path(str(self.fixture.source.execution) + '-wal')
        wal.write_bytes(b'not-empty')
        try:
            output, code = self._revoke(manifest, path)
        finally:
            wal.unlink()
        self.assertEqual(code, 5, output)
        self.assertIn('CLI_WAL_NOT_CONVERGED', output['blockers'])

        saved = Path(str(self.fixture.source.lock) + '.saved')
        real_open = baseline_cli._open_writable
        def replace(db_path):
            db = real_open(db_path)
            self.fixture.source.lock.rename(saved)
            self.fixture.source.lock.touch(mode=0o600)
            return db
        with patch('martin_agentic.baseline_cli._open_writable',
                   side_effect=replace):
            output, code = self._revoke(manifest, path)
        self.assertEqual(code, 3, output)
        self.assertEqual(output['status'], 'BLOCKED')
        self.assertIn('SOURCE_OWNER_LOCK_REPLACED',
                      output['source']['blockers'])
        self.assertEqual(self._status()['active'][0]['status'], 'APPLIED')

    def test_lifecycle_tampering_is_invalid(self):
        cases = [
            ('delete_applied', "DELETE FROM historical_baseline_event"
                              " WHERE kind='BASELINE_APPLIED'"),
            ('delete_revoked', "DELETE FROM historical_baseline_event"
                              " WHERE kind='BASELINE_REVOKED'"),
            ('extra_event', "INSERT INTO historical_baseline_event"
                            "(event_id,baseline_id,kind,occurred_at,detail)"
                            " SELECT 'extra',baseline_id,'BASELINE_REVOKED',"
                            "occurred_at,'{}' FROM historical_baseline_event"
                            " WHERE kind='BASELINE_REVOKED'"),
        ]
        for name, statement in cases:
            with self.subTest(name=name):
                fixture = CliFixture(tempfile.mkdtemp(dir=self.temp.name))
                snapshot = historical_snapshot()
                snapshot['account_key'] = fixture.source.account_key
                snapshot['account_mask'] = '••••0000'
                evidence = fixture.legal_source_evidence()['evidence']
                manifest = plan_baseline(
                    snapshot, account_key=fixture.source.account_key,
                    cutoff_utc=AT,
                    policy_sha256=digest(fixture.source.policy),
                    source_fingerprint=__import__(
                        'martin_agentic.qualification',
                        fromlist=['source_fingerprint']).source_fingerprint(),
                    evidence=evidence, expires_at=EXPIRES, now=NOW)
                archive_path = fixture.write_manifest(manifest, 'tamper.json')
                args = SimpleNamespace(**fixture.plan_args())
                args.db = str(fixture.source.execution)
                args.manifest = str(archive_path)
                args.manifest_pin = manifest['manifest_sha256']
                args.baseline_id = manifest['baseline_id']
                args.approval_scope = 'apply-scope'
                args.reason = 'test revoke'
                fake = {
                    'account': {'account_key': fixture.source.account_key,
                                'account_mask': '••••0000'},
                    'raw_chain_sha256': 'a' * 64, 'observed_at': AT,
                    'collection_window_seconds': 3.0, 'blockers': [],
                    'candidate_snapshot': snapshot, 'ready': True}
                with patch('martin_agentic.baseline_cli.normalize_bundle',
                           return_value=fake):
                    self.assertEqual(baseline_cli._run_apply(
                        args, clock=lambda: NOW)[1], 0)
                fixture.source._refresh_pins()
                fixture.pins_path.write_text(json.dumps(fixture.source.expected))
                os.chmod(fixture.pins_path, 0o600)
                args.approval_scope = 'revoke-scope'
                args.reason = 'tamper'
                self.assertEqual(baseline_cli._run_revoke(
                    args, clock=lambda: NOW)[1], 0)
                db = sqlite3.connect(fixture.source.execution,
                                     isolation_level=None)
                try:
                    db.execute(statement)
                    db.commit()
                finally:
                    db.close()
                db = sqlite3.connect(fixture.source.execution)
                try:
                    status = status_baseline(db)
                finally:
                    db.close()
                self.assertEqual(status['status'], 'INVALID', name)

    def test_importer_same_index_rechecks_after_revoke(self):
        snapshot, manifest, path = self._manifest()
        self._apply(snapshot, manifest, path)
        db = sqlite3.connect(self.fixture.source.execution)
        try:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS receipt_quarantine(
                    receipt_key TEXT PRIMARY KEY, account_key TEXT,
                    observed_at TEXT NOT NULL, reason TEXT NOT NULL,
                    payload TEXT NOT NULL, recorded_at TEXT NOT NULL);
                INSERT OR IGNORE INTO receipt_quarantine VALUES(
                    'prior-audit','account','2026-10-16T14:00:00+00:00',
                    'RECEIPT_REFERENCE_UNBOUND','{}',
                    '2026-10-16T14:00:00+00:00');
            ''')
            db.commit()
            index = HistoricalBaselineIndex(db)
            importer = ExecutionReceiptImporter(
                db,
                attribution=ExecutionAttribution(db, clock=lambda: NOW),
                accounting=InstanceAccounting(db, clock=lambda: NOW),
                clock=lambda: NOW, baseline=index)
            first = importer.import_snapshot(snapshot)
            self.assertEqual(len(first['baseline_matched']), 7)
            self.assertEqual(first['quarantined'], [])
            self._refresh_pins()
            output, code = self._revoke(manifest, path)
            self.assertEqual(code, 0, output)
            second = importer.import_snapshot(snapshot)
            self.assertEqual(second['baseline_matched'], [])
            self.assertTrue(second['quarantined'])
            self.assertNotEqual(self._count('historical_baseline_manifest'), 0)
            self.assertEqual(db.execute(
                'SELECT count(*) FROM receipt_quarantine WHERE receipt_key=?',
                ('prior-audit',)).fetchone()[0], 1)
            for table in ('instance_fills', 'execution_attribution',
                          'account_budget_reservations'):
                if table in {row[0] for row in db.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'")}:
                    self.assertEqual(db.execute(
                        'SELECT count(*) FROM ' + table).fetchone()[0], 0)
        finally:
            db.close()

    def test_mid_import_revoke_invalidates_cached_active_view(self):
        snapshot, manifest, path = self._manifest()
        self._apply(snapshot, manifest, path)
        self._refresh_pins()
        order = next(iter(snapshot['orders'].values()))
        db = sqlite3.connect(self.fixture.source.execution)
        try:
            index = HistoricalBaselineIndex(db)
            index.begin_import()
            try:
                first = index.match_order(order, self.fixture.source.account_key)
                self.assertEqual(first['status'], 'matched', first)
                output, code = self._revoke(manifest, path)
                self.assertEqual(code, 0, output)
                second = index.match_order(
                    order, self.fixture.source.account_key)
                self.assertEqual(second['status'], 'none', second)
            finally:
                index.end_import()
        finally:
            db.close()

    def test_new_complete_manifest_can_be_applied_after_revoke(self):
        snapshot, old_manifest, old_path = self._manifest()
        self._apply(snapshot, old_manifest, old_path)
        self._refresh_pins()
        output, code = self._revoke(old_manifest, old_path)
        self.assertEqual(code, 0, output)
        self._refresh_pins()
        old_events = len(self._events(old_manifest['baseline_id']))

        new_snapshot = self._snapshot()
        new_snapshot['account_mask'] = '•••• 0000'
        new_snapshot, new_manifest, new_path = self._manifest(
            new_snapshot, name='replacement.json')
        self.assertNotEqual(new_manifest['baseline_id'],
                            old_manifest['baseline_id'])
        new_args = self._args(new_manifest, new_path)
        new_args.approval_scope = 'replacement-apply'
        self._apply(new_snapshot, new_manifest, new_path, args=new_args)
        status = self._status()
        self.assertEqual([item['baseline_id'] for item in status['active']],
                         [new_manifest['baseline_id']])
        self.assertEqual([item['status'] for item in status['history']],
                         ['REVOKED', 'APPLIED'])
        self.assertEqual(len(self._events(old_manifest['baseline_id'])),
                         old_events)
        self.assertEqual(len(self._events(new_manifest['baseline_id'])), 1)
        old, old_code = baseline_cli._run_apply(
            self._args(old_manifest, old_path), clock=lambda: NOW)
        self.assertEqual(old_code, 4)
        self.assertEqual(old['status'], 'REVOKED')

    def test_revoke_wrong_target_pin_and_account_rejected(self):
        snapshot, manifest, path = self._manifest()
        self._apply(snapshot, manifest, path)
        self._refresh_pins()
        wrong, code = self._revoke(
            manifest, path, args=self._args(
                manifest, path, manifest_pin='0' * 64))
        self.assertEqual(code, 4, wrong)
        self.assertEqual(wrong['status'], 'REVOKE_REJECTED')
        self.assertEqual(wrong['baseline']['manifest_pin'], '0' * 64)
        self.assertEqual(self._status()['active'][0]['status'], 'APPLIED')
        self.assertEqual(len(self._events(manifest['baseline_id'])), 1)

        missing, missing_code = self._revoke(
            manifest, path, args=self._args(
                manifest, path, baseline_id='missing'))
        self.assertEqual(missing_code, 4, missing)
        self.assertEqual(missing['status'], 'REVOKE_REJECTED')
        self.assertEqual(self._status()['active'][0]['status'], 'APPLIED')

        @contextmanager
        def scope():
            yield {'synthetic': True}
        class FakeProvider:
            execution_db = str(self.fixture.source.execution)
            def exclusive_scope(self):
                return scope()
            def verify_scope(self, owner):
                return None
            def read(self):
                return {'status': 'READY',
                        'evidence': {'source': {'account_key': 'other'}}}
        with patch('martin_agentic.baseline_cli._source_provider',
                   return_value=FakeProvider()):
            account, account_code = self._revoke(manifest, path)
        self.assertEqual(account_code, 4, account)
        self.assertEqual(account['status'], 'REVOKE_REJECTED')
        self.assertIn('CLI_REVOKE_ACCOUNT_MISMATCH', account['blockers'])
        self.assertEqual(self._status()['active'][0]['status'], 'APPLIED')
        self.assertEqual(len(self._events(manifest['baseline_id'])), 1)

    def test_lifecycle_event_field_tampering_is_invalid(self):
        cases = [
            ('event_id', lambda db, manifest: db.execute(
                "UPDATE historical_baseline_event SET event_id='0'*64 "
                "WHERE kind='BASELINE_REVOKED'")),
            ('applied_detail', lambda db, manifest: db.execute(
                "UPDATE historical_baseline_event SET detail=? "
                "WHERE kind='BASELINE_APPLIED'",
                (canonical({'manifest_sha256': manifest['manifest_sha256'],
                            'approval_scope': 'changed'}),))),
            ('revoked_detail_extra', lambda db, manifest: db.execute(
                "UPDATE historical_baseline_event SET detail=? "
                "WHERE kind='BASELINE_REVOKED'",
                (canonical({'manifest_sha256': manifest['manifest_sha256'],
                            'old_status': 'APPLIED',
                            'new_status': 'REVOKED',
                            'approval_scope': 'revoke-scope',
                            'reason': 'test revoke',
                            'extra': 'forbidden'}),))),
            ('revoked_scope_empty', lambda db, manifest: db.execute(
                "UPDATE historical_baseline_event SET detail=? "
                "WHERE kind='BASELINE_REVOKED'",
                (canonical({'manifest_sha256': manifest['manifest_sha256'],
                            'old_status': 'APPLIED',
                            'new_status': 'REVOKED',
                            'approval_scope': '',
                            'reason': 'test revoke'}),))),
            ('revoked_reason_empty', lambda db, manifest: db.execute(
                "UPDATE historical_baseline_event SET detail=? "
                "WHERE kind='BASELINE_REVOKED'",
                (canonical({'manifest_sha256': manifest['manifest_sha256'],
                            'old_status': 'APPLIED',
                            'new_status': 'REVOKED',
                            'approval_scope': 'revoke-scope',
                            'reason': ''}),))),
            ('revoked_time_before_applied', lambda db, manifest: db.execute(
                "UPDATE historical_baseline_event SET occurred_at=? "
                "WHERE kind='BASELINE_REVOKED'",
                ('2026-10-16T13:59:59+00:00',))),
            ('status_applied_with_two_events', lambda db, manifest: db.execute(
                "UPDATE historical_baseline_manifest SET status='APPLIED'")),
            ('status_superseded', lambda db, manifest: db.execute(
                "UPDATE historical_baseline_manifest SET status='SUPERSEDED'")),
            ('applied_time_changed', lambda db, manifest: db.execute(
                "UPDATE historical_baseline_event SET occurred_at=?"
                " WHERE kind='BASELINE_APPLIED'",
                ('2026-10-16T14:00:01+00:00',))),
            ('unknown_kind', lambda db, manifest: db.execute(
                "INSERT INTO historical_baseline_event"
                "(event_id,baseline_id,kind,occurred_at,detail) "
                "SELECT 'x',baseline_id,'BASELINE_OTHER',occurred_at,'{}' "
                "FROM historical_baseline_event WHERE kind='BASELINE_REVOKED'")),
            ('orphan_event', lambda db, manifest: db.execute(
                "UPDATE historical_baseline_event SET baseline_id='orphan' "
                "WHERE kind='BASELINE_REVOKED'")),
        ]
        for name, mutate in cases:
            with self.subTest(name=name):
                fixture, manifest, args = self._fresh_revoked()
                db = sqlite3.connect(fixture.source.execution,
                                     isolation_level=None)
                try:
                    mutate(db, manifest)
                    db.commit()
                finally:
                    db.close()
                db = sqlite3.connect(fixture.source.execution)
                try:
                    status = status_baseline(db)
                finally:
                    db.close()
                self.assertEqual(status['status'], 'INVALID', name)

    def test_prewrite_rollback_and_postcheck_failure(self):
        snapshot, manifest, path = self._manifest()
        self._apply(snapshot, manifest, path)
        self._refresh_pins()
        with patch('trading_infra.historical_baseline._revoke_detail',
                   side_effect=sqlite3.IntegrityError('synthetic-prewrite')):
            output, code = self._revoke(manifest, path)
        self.assertEqual(code, 4, output)
        self.assertEqual(output['status'], 'REVOKE_REJECTED')
        self.assertFalse(output['revoked'])
        self.assertEqual(output['baseline']['baseline_id'],
                         manifest['baseline_id'])
        self.assertEqual(self._status()['active'][0]['status'], 'APPLIED')
        self.assertEqual(len(self._events(manifest['baseline_id'])), 1)

        real_entry = baseline_cli._baseline_entry
        calls = []
        def flaky_entry(*a, **k):
            calls.append(True)
            if len(calls) == 3:
                raise baseline_cli.CliError('SYNTHETIC_POSTCHECK')
            return real_entry(*a, **k)
        with patch('martin_agentic.baseline_cli._baseline_entry',
                   side_effect=flaky_entry):
            output, code = self._revoke(manifest, path)
        self.assertEqual(code, 4, output)
        self.assertEqual(output['status'], 'REVOKE_POSTCHECK_FAILED')
        self.assertTrue(output['revoked'])
        self.assertEqual(output['baseline']['baseline_id'],
                         manifest['baseline_id'])
        self.assertEqual(self._status()['active'], [])
        history = self._status()['history']
        self.assertEqual(history[0]['status'], 'REVOKED')
        self.assertEqual(len(self._events(manifest['baseline_id'])), 2)
        with patch.object(SourceEvidenceProvider, 'read',
                          side_effect=AssertionError('source must not be read')):
            duplicate, duplicate_code = self._revoke(
                manifest, path, reason='different', approval_scope='other')
        self.assertEqual(duplicate_code, 0, duplicate)
        self.assertEqual(duplicate['status'], 'ALREADY_REVOKED')
        self.assertEqual(len(self._events(manifest['baseline_id'])), 2)

    def test_commit_failure_is_outcome_unknown_and_rolls_back(self):
        snapshot, manifest, path = self._manifest()
        self._apply(snapshot, manifest, path)
        self._refresh_pins()
        real_open = baseline_cli._open_writable

        class CommitFailure:
            def __init__(self, db):
                self._db = db
            @property
            def in_transaction(self):
                return self._db.in_transaction
            def __getattr__(self, name):
                return getattr(self._db, name)
            def commit(self):
                raise sqlite3.OperationalError('synthetic-commit-unknown')

        def open_proxy(db_path):
            return CommitFailure(real_open(db_path))
        with patch('martin_agentic.baseline_cli._open_writable',
                   side_effect=open_proxy):
            output, code = self._revoke(manifest, path)
        self.assertEqual(code, 4, output)
        self.assertEqual(output['status'], 'REVOKE_OUTCOME_UNKNOWN')
        self.assertIsNone(output['revoked'])
        self.assertEqual(output['baseline']['manifest_pin'],
                         manifest['manifest_sha256'])
        self.assertEqual(self._status()['active'][0]['status'], 'APPLIED')
        self.assertEqual(len(self._events(manifest['baseline_id'])), 1)

    def _start_sqlite_writer(self, hold, mutate=False):
        args = [sys.executable, '-c', SQLITE_WRITER,
                str(self.fixture.source.execution), str(hold)]
        if mutate:
            args.append('mutate')
        process = subprocess.Popen(
            args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
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

    def test_actual_sqlite_wait_source_change_rejects(self):
        snapshot, manifest, path = self._manifest()
        self._apply(snapshot, manifest, path)
        self._refresh_pins()
        writer = self._start_sqlite_writer(0.4, mutate=True)
        result = {}
        def target():
            try:
                result['out'], result['rc'] = self._revoke(manifest, path)
            except BaseException as exc:
                result['error'] = exc
        thread = threading.Thread(target=target)
        started = time.monotonic()
        thread.start()
        time.sleep(0.1)
        thread.join(5)
        self._stop_sqlite_writer(writer)
        self.assertFalse(thread.is_alive())
        self.assertNotIn('error', result, result)
        self.assertGreaterEqual(time.monotonic() - started, 0.25)
        self.assertEqual(result['rc'], 3, result['out'])
        self.assertEqual(result['out']['status'], 'BLOCKED')
        self.assertTrue(any('SOURCE_' in item for item in
                            result['out']['source']['blockers']))
        self.assertEqual(self._status()['active'][0]['status'], 'APPLIED')
        self.assertEqual(len(self._events(manifest['baseline_id'])), 1)

    def test_exact_duplicate_does_not_read_source_or_append(self):
        manifest, path = self._prepare_revoked()
        with patch.object(SourceEvidenceProvider, 'read',
                          side_effect=AssertionError('source must not be read')):
            output, code = self._revoke(
                manifest, path, reason='new reason',
                approval_scope='new scope')
        self.assertEqual(code, 0, output)
        self.assertEqual(output['status'], 'ALREADY_REVOKED')
        self.assertEqual(output['baseline']['revoke_reason'], 'test revoke')
        self.assertEqual(output['baseline']['revoke_approval_scope'],
                         'revoke-scope')
        self.assertEqual(len(self._events(manifest['baseline_id'])), 2)

    def test_revoke_rejects_live_unknown_lease_identity_approval_and_order(self):
        cases = [
            ('live_action', "INSERT INTO live_actions VALUES"
                           "('a','t','order','UNKNOWN','{}',?,?,NULL)",
             (AT, AT)),
            ('execution_intent', "INSERT INTO execution_intents VALUES('i','OPEN')",
             ()),
            ('symbol_lease', "CREATE TABLE symbol_leases(symbol TEXT);"
                             "INSERT INTO symbol_leases VALUES('SPY')", ()),
            ('identity_conflict',
             "CREATE TABLE execution_identity_conflict(execution_key TEXT);"
             "INSERT INTO execution_identity_conflict VALUES('k')", ()),
            ('identity_quarantine',
             "CREATE TABLE receipt_identity_quarantine(id TEXT);"
             "INSERT INTO receipt_identity_quarantine VALUES('q')", ()),
            ('integrity_fault',
             "CREATE TABLE execution_integrity_faults(id TEXT);"
             "INSERT INTO execution_integrity_faults VALUES('f')", ()),
        ]
        for name, statement, params in cases:
            with self.subTest(name=name):
                fixture = CliFixture(tempfile.mkdtemp(dir=self.temp.name))
                snapshot = historical_snapshot()
                snapshot['account_key'] = fixture.source.account_key
                snapshot['account_mask'] = '••••0000'
                evidence = fixture.legal_source_evidence()['evidence']
                manifest = plan_baseline(
                    snapshot, account_key=fixture.source.account_key,
                    cutoff_utc=AT, policy_sha256=digest(fixture.source.policy),
                    source_fingerprint=__import__(
                        'martin_agentic.qualification',
                        fromlist=['source_fingerprint']).source_fingerprint(),
                    evidence=evidence, expires_at=EXPIRES, now=NOW)
                path = fixture.write_manifest(manifest, 'gated.json')
                args = SimpleNamespace(**fixture.plan_args())
                args.db = str(fixture.source.execution)
                args.manifest = str(path)
                args.manifest_pin = manifest['manifest_sha256']
                args.baseline_id = manifest['baseline_id']
                args.approval_scope = 'apply-scope'
                args.reason = 'gated'
                fake = {
                    'account': {'account_key': fixture.source.account_key,
                                'account_mask': '••••0000'},
                    'raw_chain_sha256': 'a' * 64, 'observed_at': AT,
                    'collection_window_seconds': 3.0, 'blockers': [],
                    'candidate_snapshot': snapshot, 'ready': True}
                with patch('martin_agentic.baseline_cli.normalize_bundle',
                           return_value=fake):
                    self.assertEqual(baseline_cli._run_apply(
                        args, clock=lambda: NOW)[1], 0)
                fixture.source._refresh_pins()
                db = sqlite3.connect(fixture.source.execution,
                                     isolation_level=None)
                try:
                    if params:
                        db.execute(statement, params)
                    else:
                        db.executescript(statement)
                    db.commit()
                finally:
                    db.close()
                fixture.source._refresh_pins()
                fixture.pins_path.write_text(json.dumps(fixture.source.expected))
                os.chmod(fixture.pins_path, 0o600)
                args.approval_scope = 'revoke-scope'
                output, code = baseline_cli._run_revoke(
                    args, clock=lambda: NOW)
                self.assertIn(code, (3, 4), output)
                self.assertNotEqual(output['status'], 'REVOKED')
                db = sqlite3.connect(fixture.source.execution)
                try:
                    self.assertEqual(db.execute(
                        'SELECT status FROM historical_baseline_manifest'
                        ' WHERE baseline_id=?',
                        (manifest['baseline_id'],)).fetchone()[0], 'APPLIED')
                    self.assertEqual(db.execute(
                        'SELECT count(*) FROM historical_baseline_event'
                        ' WHERE baseline_id=?',
                        (manifest['baseline_id'],)).fetchone()[0], 1)
                finally:
                    db.close()

    def test_revoke_rejects_approval_and_open_order_in_live_state(self):
        for name, change in (
                ('approval', lambda state: state.update({
                    'broker_approvals': {'count': 1, 'ids': ['a'],
                                         'missing': [], 'states': {}}})),
                ('open_order', lambda state: state.update({
                    'broker_orders': {'o': {'state': 'open'}}})),):
            with self.subTest(name=name):
                fixture = CliFixture(tempfile.mkdtemp(dir=self.temp.name))
                snapshot = historical_snapshot()
                snapshot['account_key'] = fixture.source.account_key
                snapshot['account_mask'] = '••••0000'
                evidence = fixture.legal_source_evidence()['evidence']
                manifest = plan_baseline(
                    snapshot, account_key=fixture.source.account_key,
                    cutoff_utc=AT, policy_sha256=digest(fixture.source.policy),
                    source_fingerprint=__import__(
                        'martin_agentic.qualification',
                        fromlist=['source_fingerprint']).source_fingerprint(),
                    evidence=evidence, expires_at=EXPIRES, now=NOW)
                path = fixture.write_manifest(manifest, 'state-gated.json')
                args = SimpleNamespace(**fixture.plan_args())
                args.db = str(fixture.source.execution)
                args.manifest = str(path)
                args.manifest_pin = manifest['manifest_sha256']
                args.baseline_id = manifest['baseline_id']
                args.approval_scope = 'apply-scope'
                args.reason = 'state-gated'
                fake = {
                    'account': {'account_key': fixture.source.account_key,
                                'account_mask': '••••0000'},
                    'raw_chain_sha256': 'a' * 64, 'observed_at': AT,
                    'collection_window_seconds': 3.0, 'blockers': [],
                    'candidate_snapshot': snapshot, 'ready': True}
                with patch('martin_agentic.baseline_cli.normalize_bundle',
                           return_value=fake):
                    self.assertEqual(baseline_cli._run_apply(
                        args, clock=lambda: NOW)[1], 0)
                db = sqlite3.connect(fixture.source.execution,
                                     isolation_level=None)
                try:
                    state = json.loads(db.execute(
                        'SELECT payload FROM live_state WHERE id=1').fetchone()[0])
                    change(state)
                    db.execute('UPDATE live_state SET payload=? WHERE id=1',
                               (canonical(state),))
                    db.commit()
                finally:
                    db.close()
                fixture.source._refresh_pins()
                fixture.pins_path.write_text(json.dumps(fixture.source.expected))
                os.chmod(fixture.pins_path, 0o600)
                args.approval_scope = 'revoke-scope'
                output, code = baseline_cli._run_revoke(args, clock=lambda: NOW)
                self.assertIn(code, (3, 4), output)
                self.assertNotEqual(output['status'], 'REVOKED')
                db = sqlite3.connect(fixture.source.execution)
                try:
                    self.assertEqual(db.execute(
                        'SELECT status FROM historical_baseline_manifest'
                        ' WHERE baseline_id=?',
                        (manifest['baseline_id'],)).fetchone()[0], 'APPLIED')
                finally:
                    db.close()

    def test_revoke_source_fingerprint_pin_mismatch_blocks_without_write(self):
        snapshot, manifest, path = self._manifest()
        self._apply(snapshot, manifest, path)
        self._refresh_pins()
        pins = dict(self.fixture.source.expected)
        pins['source_fingerprint'] = 'f' * 64
        self.fixture.pins_path.write_text(json.dumps(pins))
        os.chmod(self.fixture.pins_path, 0o600)
        output, code = self._revoke(manifest, path)
        self.assertEqual(code, 3, output)
        self.assertEqual(output['status'], 'BLOCKED')
        self.assertTrue(any(item.startswith('SOURCE_')
                            for item in output['source']['blockers']))
        self.assertEqual(output['baseline']['baseline_id'],
                         manifest['baseline_id'])
        self.assertEqual(self._status()['active'][0]['status'], 'APPLIED')
        self.assertEqual(len(self._events(manifest['baseline_id'])), 1)

    def _cache_fixture(self):
        snapshot, manifest, path = self._manifest()
        self._apply(snapshot, manifest, path)
        self._refresh_pins()
        return snapshot, manifest, path

    def test_same_connection_revoke_invalidates_cached_index(self):
        snapshot, manifest, path = self._cache_fixture()
        db = sqlite3.connect(self.fixture.source.execution, isolation_level=None)
        try:
            index = HistoricalBaselineIndex(db)
            index.begin_import()
            order = manifest['orders'][0]['order']
            account = self.fixture.source.account_key
            self.assertEqual(index.match_order(order, account)['status'],
                             'matched')
            core.revoke_baseline(
                db, manifest['baseline_id'], manifest['manifest_sha256'],
                approval_scope='probe', reason='same-connection', now=NOW)
            self.assertEqual(index.match_order(order, account)['status'],
                             'none')
            self.assertEqual(core.status_baseline(db)['active'], [])
            index.end_import()
        finally:
            db.close()

    def test_multiple_indexes_on_same_connection_see_revoke(self):
        snapshot, manifest, path = self._cache_fixture()
        db = sqlite3.connect(self.fixture.source.execution, isolation_level=None)
        try:
            first = HistoricalBaselineIndex(db)
            second = HistoricalBaselineIndex(db)
            first.begin_import()
            second.begin_import()
            order = manifest['orders'][0]['order']
            account = self.fixture.source.account_key
            self.assertEqual(first.match_order(order, account)['status'],
                             'matched')
            self.assertEqual(second.match_order(order, account)['status'],
                             'matched')
            core.revoke_baseline(
                db, manifest['baseline_id'], manifest['manifest_sha256'],
                approval_scope='probe', reason='multi-index', now=NOW)
            self.assertEqual(first.match_order(order, account)['status'],
                             'none')
            self.assertEqual(second.match_order(order, account)['status'],
                             'none')
            first.end_import()
            second.end_import()
        finally:
            db.close()

    def test_same_connection_unrelated_write_keeps_one_validated_scan(self):
        snapshot, manifest, path = self._cache_fixture()
        db = sqlite3.connect(self.fixture.source.execution, isolation_level=None)
        try:
            index = HistoricalBaselineIndex(db)
            index.begin_import()
            order = manifest['orders'][0]['order']
            account = self.fixture.source.account_key
            calls = []
            revisions = []
            real = core._schema_state
            real_revision = index._revision
            def counted(connection):
                calls.append(True)
                return real(connection)
            def counted_revision():
                revisions.append(True)
                return real_revision()
            with patch.object(core, '_schema_state', side_effect=counted), \
                    patch.object(index, '_revision',
                                 side_effect=counted_revision):
                self.assertEqual(index.match_order(order, account)['status'],
                                 'matched')
                db.execute("UPDATE live_state SET payload=payload || ' ' "
                           "WHERE id=1")
                self.assertEqual(index.match_order(order, account)['status'],
                                 'matched')
                self.assertEqual(index.match_order(order, account)['status'],
                                 'matched')
            self.assertEqual(len(calls), 1)
            self.assertLessEqual(len(revisions), 4)
            index.end_import()
        finally:
            db.close()

    def test_rollback_then_revoke_is_reread_not_stale(self):
        snapshot, manifest, path = self._cache_fixture()
        db = sqlite3.connect(self.fixture.source.execution, isolation_level=None)
        try:
            index = HistoricalBaselineIndex(db)
            index.begin_import()
            order = manifest['orders'][0]['order']
            account = self.fixture.source.account_key
            self.assertEqual(index.match_order(order, account)['status'],
                             'matched')
            db.execute('BEGIN')
            core.revoke_baseline(
                db, manifest['baseline_id'], manifest['manifest_sha256'],
                approval_scope='probe', reason='rollback', now=NOW)
            self.assertEqual(index.match_order(order, account)['status'],
                             'none')
            db.rollback()
            self.assertEqual(index.match_order(order, account)['status'],
                             'matched')
            index.end_import()
        finally:
            if db.in_transaction:
                db.rollback()
            db.close()

    def test_cross_connection_revoke_invalidates_cached_index(self):
        snapshot, manifest, path = self._cache_fixture()
        db = sqlite3.connect(self.fixture.source.execution, isolation_level=None)
        try:
            index = HistoricalBaselineIndex(db)
            index.begin_import()
            order = manifest['orders'][0]['order']
            account = self.fixture.source.account_key
            self.assertEqual(index.match_order(order, account)['status'],
                             'matched')
            other = sqlite3.connect(self.fixture.source.execution,
                                    isolation_level=None)
            try:
                core.revoke_baseline(
                    other, manifest['baseline_id'],
                    manifest['manifest_sha256'], approval_scope='probe',
                    reason='cross-connection', now=NOW)
            finally:
                other.close()
            self.assertEqual(index.match_order(order, account)['status'],
                             'none')
            index.end_import()
        finally:
            db.close()

    def test_schema_state_commit_after_scan_does_not_bind_new_revision(self):
        snapshot, manifest, path = self._cache_fixture()
        db = sqlite3.connect(self.fixture.source.execution, isolation_level=None)
        try:
            index = HistoricalBaselineIndex(db)
            index.begin_import()
            order = manifest['orders'][0]['order']
            account = self.fixture.source.account_key
            real = core._schema_state
            done = []
            def racing(connection):
                state = real(connection)
                if not done:
                    done.append(True)
                    other = sqlite3.connect(
                        self.fixture.source.execution, isolation_level=None)
                    try:
                        core.revoke_baseline(
                            other, manifest['baseline_id'],
                            manifest['manifest_sha256'],
                            approval_scope='probe', reason='after-state',
                            now=NOW)
                    finally:
                        other.close()
                return state
            with patch.object(core, '_schema_state', side_effect=racing):
                self.assertEqual(index.match_order(order, account)['status'],
                                 'none')
                self.assertEqual(index.match_order(order, account)['status'],
                                 'none')
            index.end_import()
        finally:
            db.close()

    def test_revision_commit_between_selects_is_bounded_and_reread(self):
        snapshot, manifest, path = self._cache_fixture()
        db = sqlite3.connect(self.fixture.source.execution, isolation_level=None)
        try:
            index = HistoricalBaselineIndex(db)
            index.begin_import()
            order = manifest['orders'][0]['order']
            account = self.fixture.source.account_key
            done = []
            def racing_revision():
                rows = db.execute(
                    'SELECT baseline_id,status,manifest_sha256 '
                    'FROM historical_baseline_manifest ORDER BY sequence'
                ).fetchall()
                if not done:
                    done.append(True)
                    other = sqlite3.connect(
                        self.fixture.source.execution, isolation_level=None)
                    try:
                        core.revoke_baseline(
                            other, manifest['baseline_id'],
                            manifest['manifest_sha256'],
                            approval_scope='probe', reason='between-selects',
                            now=NOW)
                    finally:
                        other.close()
                events = db.execute(
                    'SELECT event_id,kind FROM historical_baseline_event '
                    'ORDER BY event_id').fetchall()
                version = db.execute(
                    'SELECT version FROM historical_baseline_schema').fetchall()
                return (tuple(rows), tuple(events), tuple(version))
            with patch.object(index, '_revision',
                              side_effect=racing_revision):
                self.assertEqual(index.match_order(order, account)['status'],
                                 'none')
            index.end_import()
        finally:
            db.close()

    def test_continuous_revision_contention_is_bounded_conflict(self):
        snapshot, manifest, path = self._cache_fixture()
        db = sqlite3.connect(self.fixture.source.execution)
        try:
            index = HistoricalBaselineIndex(db)
            index.begin_import()
            order = manifest['orders'][0]['order']
            account = self.fixture.source.account_key
            calls = []
            def unstable():
                calls.append(True)
                return (('revision-' + str(len(calls)),), len(calls))
            with patch.object(index, '_stable_revision',
                              side_effect=unstable):
                result = index.match_order(order, account)
            self.assertEqual(result['status'], 'conflict')
            self.assertEqual(result['reason'], 'BASELINE_INDEX_UNSTABLE')
            self.assertGreater(len(calls), 0)
            self.assertLessEqual(len(calls), 2 * index._CACHE_ATTEMPTS)
            index.end_import()
        finally:
            db.close()

    def test_cached_event_or_materialized_tamper_is_not_bypassed(self):
        snapshot, manifest, path = self._manifest()
        self._apply(snapshot, manifest, path)
        db = sqlite3.connect(self.fixture.source.execution,
                             isolation_level=None)
        try:
            index = HistoricalBaselineIndex(db)
            index.begin_import()
            order = manifest['orders'][0]['order']
            account = self.fixture.source.account_key
            self.assertEqual(index.match_order(order, account)['status'],
                             'matched')
            detail = canonical({
                'manifest_sha256': manifest['manifest_sha256'],
                'approval_scope': 'tampered'})
            db.execute("UPDATE historical_baseline_event SET detail=? "
                       "WHERE kind='BASELINE_APPLIED'", (detail,))
            result = index.match_order(order, account)
            self.assertEqual(result['status'], 'conflict', result)
            index.end_import()
        finally:
            db.close()

    def test_schema_state_commit_before_scan_is_rejected_by_generation(self):
        snapshot, manifest, path = self._cache_fixture()
        db = sqlite3.connect(self.fixture.source.execution, isolation_level=None)
        try:
            index = HistoricalBaselineIndex(db)
            index.begin_import()
            order = manifest['orders'][0]['order']
            account = self.fixture.source.account_key
            real = core._schema_state
            done = []
            def racing(connection):
                if not done:
                    done.append(True)
                    other = sqlite3.connect(
                        self.fixture.source.execution, isolation_level=None)
                    try:
                        core.revoke_baseline(
                            other, manifest['baseline_id'],
                            manifest['manifest_sha256'],
                            approval_scope='probe', reason='before-state',
                            now=NOW)
                    finally:
                        other.close()
                return real(connection)
            with patch.object(core, '_schema_state', side_effect=racing):
                self.assertEqual(index.match_order(order, account)['status'],
                                 'none')
                self.assertEqual(index.match_order(order, account)['status'],
                                 'none')
            index.end_import()
        finally:
            db.close()

    def test_apply_in_caller_transaction_rollback_and_begin(self):
        snapshot, manifest, path = self._manifest()
        db = sqlite3.connect(self.fixture.source.execution,
                             isolation_level=None)
        try:
            db.execute('BEGIN IMMEDIATE')
            core.apply_baseline(
                db, manifest, manifest_pin=manifest['manifest_sha256'],
                approval_scope='caller-txn', now=NOW,
                evidence=manifest['evidence'], snapshot=snapshot)
            index = HistoricalBaselineIndex(db)
            index.begin_import()
            order = manifest['orders'][0]['order']
            account = self.fixture.source.account_key
            self.assertEqual(index.match_order(order, account)['status'],
                             'matched')
            db.rollback()
            db.execute('BEGIN IMMEDIATE')
            self.assertEqual(index.match_order(order, account)['status'],
                             'none')
            self.assertEqual(core.status_baseline(db)['status'], 'ABSENT')
            index.end_import()
        finally:
            if db.in_transaction:
                db.rollback()
            db.close()

    def test_apply_in_caller_transaction_rollback_to_savepoint(self):
        snapshot, manifest, path = self._manifest()
        db = sqlite3.connect(self.fixture.source.execution,
                             isolation_level=None)
        try:
            db.execute('BEGIN IMMEDIATE')
            db.execute('SAVEPOINT before_apply')
            core.apply_baseline(
                db, manifest, manifest_pin=manifest['manifest_sha256'],
                approval_scope='caller-txn', now=NOW,
                evidence=manifest['evidence'], snapshot=snapshot)
            first = HistoricalBaselineIndex(db)
            second = HistoricalBaselineIndex(db)
            first.begin_import()
            second.begin_import()
            order = manifest['orders'][0]['order']
            account = self.fixture.source.account_key
            self.assertEqual(first.match_order(order, account)['status'],
                             'matched')
            self.assertEqual(second.match_order(order, account)['status'],
                             'matched')
            db.execute('ROLLBACK TO before_apply')
            self.assertEqual(first.match_order(order, account)['status'],
                             'none')
            self.assertEqual(second.match_order(order, account)['status'],
                             'none')
            self.assertEqual(core.status_baseline(db)['status'], 'ABSENT')
            first.end_import()
            second.end_import()
        finally:
            if db.in_transaction:
                db.rollback()
            db.close()

    def test_new_manifest_rollback_keeps_old_revoked_history(self):
        snapshot, manifest, path = self._manifest()
        self._apply(snapshot, manifest, path)
        self._refresh_pins()
        output, code = self._revoke(manifest, path)
        self.assertEqual(code, 0, output)
        new_snapshot = self._snapshot()
        new_snapshot['account_mask'] = '•••• 0000'
        new_manifest = plan_baseline(
            new_snapshot, account_key=self.fixture.source.account_key,
            cutoff_utc=AT, policy_sha256=digest(self.fixture.source.policy),
            source_fingerprint=manifest['source_fingerprint'],
            evidence=manifest['evidence'], expires_at=EXPIRES, now=NOW)
        self.assertNotEqual(new_manifest['baseline_id'],
                            manifest['baseline_id'])
        db = sqlite3.connect(self.fixture.source.execution,
                             isolation_level=None)
        try:
            db.execute('BEGIN IMMEDIATE')
            core.apply_baseline(
                db, new_manifest,
                manifest_pin=new_manifest['manifest_sha256'],
                approval_scope='replacement-txn', now=NOW,
                evidence=manifest['evidence'], snapshot=new_snapshot)
            index = HistoricalBaselineIndex(db)
            index.begin_import()
            order = new_manifest['orders'][0]['order']
            account = self.fixture.source.account_key
            self.assertEqual(index.match_order(order, account)['status'],
                             'matched')
            db.rollback()
            self.assertEqual(index.match_order(order, account)['status'],
                             'none')
            status = core.status_baseline(db)
            self.assertEqual(status['status'], 'PRESENT')
            self.assertEqual(status['active'], [])
            self.assertEqual(status['history'][0]['status'], 'REVOKED')
            index.end_import()
        finally:
            if db.in_transaction:
                db.rollback()
            db.close()

    def test_revoke_in_savepoint_rollback_restores_applied_match(self):
        snapshot, manifest, path = self._cache_fixture()
        db = sqlite3.connect(self.fixture.source.execution,
                             isolation_level=None)
        try:
            index = HistoricalBaselineIndex(db)
            index.begin_import()
            order = manifest['orders'][0]['order']
            account = self.fixture.source.account_key
            self.assertEqual(index.match_order(order, account)['status'],
                             'matched')
            db.execute('BEGIN')
            db.execute('SAVEPOINT before_revoke')
            core.revoke_baseline(
                db, manifest['baseline_id'], manifest['manifest_sha256'],
                approval_scope='savepoint', reason='savepoint', now=NOW)
            self.assertEqual(index.match_order(order, account)['status'],
                             'none')
            db.execute('ROLLBACK TO before_revoke')
            self.assertEqual(index.match_order(order, account)['status'],
                             'matched')
            self.assertEqual(core.status_baseline(db)['active'][0]['status'],
                             'APPLIED')
            index.end_import()
        finally:
            if db.in_transaction:
                db.rollback()
            db.close()

    def test_implicit_driver_transaction_rollback_is_rechecked(self):
        snapshot, manifest, path = self._manifest()
        db = sqlite3.connect(self.fixture.source.execution)
        try:
            # The default sqlite3 isolation mode starts an implicit
            # transaction on this DML; apply_baseline joins it.
            db.execute("UPDATE live_state SET payload=payload || ' ' WHERE id=1")
            self.assertTrue(db.in_transaction)
            core.apply_baseline(
                db, manifest, manifest_pin=manifest['manifest_sha256'],
                approval_scope='implicit-txn', now=NOW,
                evidence=manifest['evidence'], snapshot=snapshot)
            index = HistoricalBaselineIndex(db)
            index.begin_import()
            order = manifest['orders'][0]['order']
            account = self.fixture.source.account_key
            self.assertEqual(index.match_order(order, account)['status'],
                             'matched')
            db.rollback()
            self.assertEqual(index.match_order(order, account)['status'],
                             'none')
            self.assertEqual(core.status_baseline(db)['status'], 'ABSENT')
            index.end_import()
        finally:
            if db.in_transaction:
                db.rollback()
            db.close()
