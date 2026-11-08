"""User pause is enforced locally; every fixture is temporary, without broker I/O."""
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import tempfile
from unittest import TestCase
from unittest.mock import patch

from martin_agentic import live_verification_control as control
from entry_authority_fixture import TestExecutionBook as ExecutionBook
from martin_agentic.native_operations import NativeOperations
from test_broker import snapshot
import test_host_mailbox

NOW = datetime(2026, 10, 28, 15, tzinfo=timezone.utc)


class LiveVerificationControlTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.scope = patch.object(control, 'CANONICAL_RUNTIME', self.root)
        self.scope.start(); self.addCleanup(self.scope.stop)

    def write(self, enabled=False, **changes):
        value = dict(schema_version=1, live_trading_enabled=enabled,
            live_engineering_verification_enabled=enabled, recorded_at=NOW.isoformat(),
            grants_not_before=(NOW-timedelta(minutes=1)).isoformat(), reason='Explicit fixture user stop')
        value.update(changes)
        path = self.root/control.CONTROL_NAME
        path.write_text(json.dumps(value)); path.chmod(0o600)

    def gate(self, **changes):
        args = dict(simulation=False, purpose='engineering', clock=lambda: NOW,
            evidence={'parameters_authorized_at': NOW.isoformat()})
        args.update(changes)
        return control.engineering_verification_gate(self.root/'book.sqlite', **args)

    def test_missing_invalid_or_unsafe_control_fails_closed(self):
        self.assertTrue(self.gate()['blocked'])
        self.write(); self.assertEqual(self.gate()['reason'], 'LIVE_TRADING_USER_HOLD')
        self.write(enabled='true'); self.assertTrue(self.gate()['blocked'])
        self.write(enabled=True); (self.root/control.CONTROL_NAME).chmod(0o644)
        self.assertTrue(self.gate()['blocked'])
        path = self.root/control.CONTROL_NAME; path.unlink(); os.mkfifo(path)
        self.assertTrue(self.gate()['blocked']); path.unlink()
        path.symlink_to(self.root/'absent'); self.assertTrue(self.gate()['blocked'])

    def test_malformed_time_is_blocked_without_crashing_owner(self):
        for key in ('recorded_at', 'grants_not_before'):
            for value in (None, 12, {}, [], 'not-a-time'):
                self.write(enabled=True, **{key: value})
                self.assertTrue(self.gate()['blocked'])

    def test_resumption_does_not_reuse_old_grants_or_authorize_execution(self):
        self.write(enabled=True)
        for evidence in ({}, {'parameters_authorized_at': (NOW-timedelta(hours=1)).isoformat()},
                         {'parameters_authorized_at': (NOW+timedelta(seconds=1)).isoformat()}):
            self.assertEqual(self.gate(evidence=evidence)['reason'], 'NEW_ENGINEERING_AUTHORIZATION_REQUIRED')
        allowed = self.gate()
        self.assertFalse(allowed['blocked']); self.assertFalse(allowed['execution_authorized'])
        self.write(); self.assertTrue(self.gate(purpose='strategy')['blocked'])
        self.assertFalse(self.gate(simulation=True)['applicable'])

    def test_pause_blocks_propose_claim_and_native_place_without_blocking_review(self):
        self.write(enabled=True)
        policy = json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        policy['session_date'] = '2026-10-28'
        book = ExecutionBook(self.root/'book.sqlite', policy, clock=lambda: NOW)
        self.addCleanup(book.close)
        observation = snapshot(at=NOW.isoformat())
        quote = dict(symbol='DEMO', bid='9.99', ask='10', bid_at=NOW.isoformat(),
                     ask_at=NOW.isoformat(), observed_at=NOW.isoformat())
        plan = dict(symbol='DEMO', quantity=1, entry_ceiling='10', stop='9', cost_buffer_per_share='0.05')
        evidence = dict(synthetic=False, source_sha256='isolated-wire-shape-fixture',
            engineering_scope_authorized=True, parameters_user_specified=True,
            parameters_authorized_at=NOW.isoformat(), parameters_valid_until=(NOW+timedelta(hours=1)).isoformat())
        with patch.object(book, 'entry_gate', return_value={'blocked': False}):
            entry = book.propose('engineering-pause-fixture', plan, quote, observation,
                purpose='engineering', evidence=evidence)['action_id']
            self.write()
            with self.assertRaisesRegex(ValueError, 'LIVE_TRADING_USER_HOLD'):
                book.propose('engineering-old-grant', plan, quote, observation,
                    purpose='engineering', evidence=evidence)
            self.assertIsNone(book.claim())
            native = NativeOperations(book)
            review = native.begin(entry, 'review', observation, quote=quote)
            native.record(review['operation_id'], {'data': {**review['parameters'],
                'order_checks': {}, 'customer_approval_required': False,
                'market_data_disclosure': 'SYNTHETIC fixture', 'quote_data': {}}})
            with self.assertRaisesRegex(ValueError, 'LIVE_TRADING_USER_HOLD'):
                native.begin(entry, 'place', observation, quote=quote)
            self.assertEqual(book.db.execute("SELECT count(*) FROM external_operations WHERE operation='place'").fetchone()[0], 0)
            trade = book.state()['trades']['engineering-pause-fixture']
            protect = book._queue(trade, 'protect', dict(symbol='DEMO', side='sell', quantity='1'), NOW, 'fixture safety')
            book.db.commit()
            self.assertEqual(book.claim()['action_id'], protect)

    def test_prepared_entry_packet_is_withheld_when_pause_changes(self):
        fixture = test_host_mailbox.HostMailboxTests(); fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        fixture.now = NOW; fixture.manifest['published_at'] = NOW.isoformat()
        with patch.object(control, 'CANONICAL_RUNTIME', fixture.root):
            envelope = fixture.prepared()
            manifest = json.loads((fixture.root/'manifest.json').read_text())
            manifest['actions'][0].update(kind='entry', purpose='engineering')
            (fixture.root/'manifest.json').write_text(json.dumps(manifest))
            # Missing control defaults held even for a packet prepared earlier.
            result = test_host_mailbox.read_request_result(fixture.root, fixture.rid, clock=lambda: NOW)
            self.assertNotEqual(result['status'], 'PREPARED')
            self.assertFalse(result['execution_authorized'])
