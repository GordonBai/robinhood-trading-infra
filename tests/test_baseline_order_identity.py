"""Narrow terminal-order identity and its unbypassable entry fence."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from pathlib import Path
import tempfile
from unittest import TestCase

from martin_agentic.baseline_evidence import (
    BundleError, normalize_order_identity)
from martin_agentic.entry_authority import DurableEntryAuthority
from trading_infra.attribution import ExecutionAttribution
from trading_infra.historical_baseline import HistoricalBaselineIndex
from trading_infra.instance_accounting import InstanceAccounting
from trading_infra.receipts import ExecutionReceiptImporter
from martin_agentic.execution import ExecutionBook
from test_baseline_evidence import (
    ACCOUNT, AT, AT_DT, OBSERVED, NOW_LATER, BundleFixture, envelope)
from test_broker import FakeBroker, order, snapshot
from test_historical_baseline import (
    EXPIRES, NOW, evidence, historical_snapshot)
from trading_contracts.values import digest
from trading_infra.historical_baseline import (
    BASELINE_SCOPE_IDENTITY, BaselineError, apply_baseline,
    SCHEMA_TABLES, plan_baseline, plan_order_identity_baseline,
    revoke_baseline, status_baseline)


POLICY = json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
H = 'a' * 64


def account_evidence(account_key):
    value = deepcopy(evidence(account_key))
    for section in ('source', 'risk', 'host', 'lineage'):
        value[section]['account_key'] = account_key
    return value


class IdentityFixture:
    def __init__(self, root):
        self.bundle = BundleFixture(root)
        self.orders = [
            order('buy-1', 'buy', '1', '1', 'filled'),
            order('sell-1', 'sell', '1', '0', 'cancelled'),
        ]
        for item in self.orders:
            item['ref_id'] = 'ref-' + item['id']
        self.rewrite()

    def rewrite(self):
        for page in self.bundle.pages:
            if page['tool'] == 'get_equity_orders':
                self.bundle.rewrite_raw(page, {'orders': deepcopy(self.orders)})

    def normalize(self, now=NOW_LATER):
        return normalize_order_identity(self.bundle.root, now=now)


class OrderIdentityTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.f = IdentityFixture(self.temp.name)

    def plan(self, identity=None):
        identity = identity or self.f.normalize()
        ev = account_evidence(identity['account_key'])
        manifest = plan_order_identity_baseline(
            identity, account_key=identity['account_key'],
            cutoff_utc=AT, policy_sha256='b' * 64,
            source_fingerprint='c' * 64, evidence=ev,
            expires_at=(NOW_LATER + timedelta(minutes=1)).isoformat(),
            now=NOW_LATER)
        return identity, ev, manifest

    def test_two_pass_exact_identity_and_partial_cancel_quantities(self):
        extra = order('buy-2', 'buy', '2', '1', 'cancelled')
        extra['ref_id'] = 'ref-buy-2'
        self.f.orders.append(extra)
        self.f.rewrite()
        identity = self.f.normalize()
        self.assertEqual(identity['baseline_scope'], BASELINE_SCOPE_IDENTITY)
        self.assertEqual(len(identity['orders']), 3)
        self.assertEqual(len(identity['executions']), 2)
        self.assertEqual(identity['blockers'], [])
        partial = next(o for o in identity['orders'] if o['id'] == 'buy-2')
        self.assertEqual(partial['state'], 'cancelled')
        self.assertEqual(partial['filled'], '1')

    def test_missing_pass_and_nonterminal_are_fail_closed(self):
        page = next(p for p in self.f.bundle.pages
                    if p['tool'] == 'get_equity_orders' and p['pass'] == 2)
        self.f.bundle.pages.remove(page)
        (self.f.bundle.raw / Path(page['raw_path']).name).unlink()
        self.f.bundle.write_manifest()
        identity = self.f.normalize()
        self.assertIn('TWO_PASS_REQUIRED:get_equity_orders',
                      identity['blockers'])

        fixture = IdentityFixture(Path(self.temp.name + '-nonterminal'))
        fixture.orders[0]['state'] = 'queued'
        fixture.rewrite()
        identity = fixture.normalize()
        self.assertIn('ORDER_NOT_TERMINAL:buy-1', identity['blockers'])

    def test_future_order_is_rejected(self):
        page = next(p for p in self.f.bundle.pages
                    if p['tool'] == 'get_equity_orders' and p['pass'] == 1)
        future = order('future', 'buy', '1', '1', 'filled')
        future['ref_id'] = 'ref-future'
        future['created_at'] = (AT_DT + timedelta(days=1)).isoformat()
        self.f.bundle.rewrite_raw(page, {'orders': [future]})
        with self.assertRaises(BundleError) as raised:
            self.f.normalize()
        self.assertEqual(raised.exception.code, 'BUNDLE_EQUITY_ORDER_INVALID')

    def test_plan_apply_revoke_and_durable_fence(self):
        identity, ev, manifest = self.plan()
        db_path = Path(self.temp.name) / 'book.sqlite'
        book = ExecutionBook(db_path, POLICY, simulation=True,
                             clock=lambda: NOW_LATER)
        self.addCleanup(book.close)
        applied = apply_baseline(
            book.db, manifest, manifest_pin=manifest['manifest_sha256'],
            approval_scope='synthetic-test', now=NOW_LATER, evidence=ev,
            snapshot=identity)
        self.assertEqual(applied['status'], 'APPLIED')
        again = apply_baseline(
            book.db, manifest, manifest_pin=manifest['manifest_sha256'],
            approval_scope='synthetic-test', now=NOW_LATER, evidence=ev,
            snapshot=identity)
        self.assertTrue(again['duplicate'])
        self.assertEqual(status_baseline(book.db)['active'][0]['baseline_scope'],
                         BASELINE_SCOPE_IDENTITY)
        check = book.entry_authority.check(source='test')
        self.assertFalse(check['allowed'])
        self.assertEqual(check['reason'], 'ENTRY_AUTHORITY_IDENTITY_ONLY')

        broker = FakeBroker()
        broker.orders, broker.positions = [], []
        snap = snapshot(broker, NOW_LATER.isoformat())
        snap['synthetic'] = True
        quote = {'symbol': 'DEMO', 'bid': '39.99', 'ask': '40',
                 'bid_at': NOW_LATER.isoformat(),
                 'ask_at': NOW_LATER.isoformat(),
                 'observed_at': NOW_LATER.isoformat()}
        plan = {'symbol': 'DEMO', 'quantity': 2, 'entry_ceiling': '40',
                'stop': '39', 'cost_buffer_per_share': '0.05'}
        with self.assertRaisesRegex(
                ValueError,
                'ENTRY_AUTHORITY_BLOCKED:ENTRY_AUTHORITY_IDENTITY_ONLY'):
            book.propose('blocked-entry', plan, quote, snap,
                         evidence={'source_sha256': 'fixture',
                                   'synthetic': True})

        revoked = revoke_baseline(
            book.db, manifest['baseline_id'], manifest['manifest_sha256'],
            approval_scope='synthetic-test', reason='fixture revoke',
            now=NOW_LATER)
        book.db.commit()
        self.assertEqual(revoked['status'], 'REVOKED')
        check = book.entry_authority.check(source='test-after-revoke')
        self.assertFalse(check['allowed'])
        self.assertEqual(check['reason'],
                         'ENTRY_AUTHORITY_IDENTITY_ONLY_REVOKED')

        # A READY entry already in the outbox cannot be claimed either.
        book.db.execute(
            'INSERT INTO live_actions VALUES(?,?,?,?,?,?,?,?)',
            ('old-entry', 't-old', 'entry', 'READY', '{}',
             NOW_LATER.isoformat(), NOW_LATER.isoformat(), None))
        book.db.commit()
        self.assertIsNone(book.claim(allow_entries=True))

    def test_unknown_scope_and_tampered_claims_are_rejected(self):
        identity, ev, manifest = self.plan()
        bad = deepcopy(manifest)
        bad['baseline_scope'] = 'made_up_scope_v1'
        book = ExecutionBook(Path(self.temp.name) / 'bad.sqlite', POLICY,
                             simulation=True, clock=lambda: NOW_LATER)
        try:
            with self.assertRaises(BaselineError) as raised:
                apply_baseline(
                    book.db, bad, manifest_pin=digest(
                        {k: v for k, v in bad.items()
                         if k != 'manifest_sha256'}),
                    approval_scope='x', now=NOW_LATER, evidence=ev,
                    snapshot=identity)
            self.assertEqual(raised.exception.code, 'BASELINE_SCOPE_UNKNOWN')
        finally:
            book.close()

    def test_p1_baseline_matches_receipts_but_never_books_fills(self):
        identity, ev, manifest = self.plan()
        book = ExecutionBook(Path(self.temp.name) / 'receipts.sqlite', POLICY,
                             simulation=True, clock=lambda: NOW_LATER)
        try:
            apply_baseline(
                book.db, manifest, manifest_pin=manifest['manifest_sha256'],
                approval_scope='synthetic-test', now=NOW_LATER, evidence=ev,
                snapshot=identity)
            attribution = ExecutionAttribution(book.db, clock=lambda: NOW_LATER)
            accounting = InstanceAccounting(book.db, clock=lambda: NOW_LATER)
            index = HistoricalBaselineIndex(book.db)
            importer = ExecutionReceiptImporter(
                book.db, attribution=attribution, accounting=accounting,
                clock=lambda: NOW_LATER, baseline=index)
            matched_order = deepcopy(identity['orders'][0])
            outcome = importer.import_snapshot({
                'account_key': identity['account_key'],
                'observed_at': NOW_LATER.isoformat(),
                'collection_complete': True,
                'orders': {matched_order['id']: matched_order},
            })
            self.assertEqual(len(outcome['baseline_matched']), 1)
            self.assertEqual(outcome['imported'], [])
            self.assertEqual(outcome['quarantined'], [])
            self.assertEqual(accounting.fills('alpha'), [])

            revised = deepcopy(matched_order)
            revised['executions'] = [*revised['executions'], {
                'id': 'late-exec', 'order_id': revised['id'],
                'symbol': revised['symbol'], 'side': revised['side'],
                'quantity': '1', 'price': '40', 'fees': '0',
                'executed_at': NOW_LATER.isoformat()}]
            revised['filled'] = '2'
            revised['quantity'] = '2'
            revised['state'] = 'filled'
            outcome = importer.import_snapshot({
                'account_key': identity['account_key'],
                'observed_at': NOW_LATER.isoformat(),
                'collection_complete': True,
                'orders': {revised['id']: revised},
            })
            self.assertEqual(outcome['quarantined'][0]['reason'],
                             'BASELINE_ORDER_REVISED')

            revoke_baseline(
                book.db, manifest['baseline_id'], manifest['manifest_sha256'],
                approval_scope='synthetic-test', reason='fixture revoke',
                now=NOW_LATER)
            book.db.commit()
            outcome = importer.import_snapshot({
                'account_key': identity['account_key'],
                'observed_at': NOW_LATER.isoformat(),
                'collection_complete': True,
                'orders': {matched_order['id']: matched_order},
            })
            self.assertEqual(outcome['baseline_matched'], [])
            self.assertEqual(outcome['quarantined'][0]['reason'],
                             'RECEIPT_REFERENCE_UNBOUND')
        finally:
            book.close()

    def test_missing_namespace_is_never_authority(self):
        db = sqlite3.connect(':memory:')
        try:
            decision = DurableEntryAuthority(db).check(source='missing')
            self.assertFalse(decision['allowed'])
            self.assertEqual(decision['reason'],
                             'ENTRY_AUTHORITY_NAMESPACE_ABSENT')
        finally:
            db.close()

    def test_closed_or_partial_ledger_is_structured_blocked(self):
        db = sqlite3.connect(':memory:')
        authority = DurableEntryAuthority(db)
        db.close()
        decision = authority.check(source='closed')
        self.assertFalse(decision['allowed'])
        self.assertEqual(decision['reason'],
                         'ENTRY_AUTHORITY_LEDGER_UNAVAILABLE')

        db = sqlite3.connect(':memory:')
        db.execute('CREATE TABLE historical_baseline_manifest(baseline_id TEXT)')
        db.commit()
        decision = DurableEntryAuthority(db).check(source='partial')
        self.assertFalse(decision['allowed'])
        self.assertEqual(decision['reason'],
                         'ENTRY_AUTHORITY_LEDGER_INVALID')
        db.close()

    def test_namespace_loss_after_apply_keeps_durable_marker(self):
        identity, ev, manifest = self.plan()
        book = ExecutionBook(Path(self.temp.name) / 'drop.sqlite', POLICY,
                             simulation=True, clock=lambda: NOW_LATER)
        try:
            apply_baseline(
                book.db, manifest, manifest_pin=manifest['manifest_sha256'],
                approval_scope='synthetic-test', now=NOW_LATER, evidence=ev,
                snapshot=identity)
            book.db.commit()
            before = book.entry_authority.check(source='before-drop')
            self.assertFalse(before['allowed'])
            for name in SCHEMA_TABLES:
                book.db.execute('DROP TABLE ' + name)
            book.db.commit()
            after = book.entry_authority.check(source='after-drop')
            self.assertFalse(after['allowed'])
            self.assertEqual(after['reason'],
                             'ENTRY_AUTHORITY_IDENTITY_ONLY')
        finally:
            book.close()

    def test_real_simulation_false_book_blocks_missing_namespace(self):
        book = ExecutionBook(Path(self.temp.name) / 'real.sqlite', POLICY,
                             simulation=False, clock=lambda: NOW_LATER)
        try:
            decision = book.entry_authority.check(source='real-missing')
            self.assertFalse(decision['allowed'])
            self.assertEqual(decision['reason'],
                             'ENTRY_AUTHORITY_NAMESPACE_ABSENT')
        finally:
            book.close()

    def test_full_historical_baseline_does_not_authorize_entry(self):
        snapshot = historical_snapshot()
        account_key = snapshot['account_key']
        ev = account_evidence(account_key)
        manifest = plan_baseline(
            snapshot, account_key=account_key, cutoff_utc=AT,
            policy_sha256='b' * 64, source_fingerprint='c' * 64,
            evidence=ev, expires_at=EXPIRES, now=NOW)
        book = ExecutionBook(Path(self.temp.name) / 'full.sqlite', POLICY,
                             simulation=True, clock=lambda: NOW)
        try:
            applied = apply_baseline(
                book.db, manifest, manifest_pin=manifest['manifest_sha256'],
                approval_scope='synthetic-test', now=NOW, evidence=ev,
                snapshot=snapshot)
            self.assertEqual(applied['status'], 'APPLIED')
            check = book.entry_authority.check(source='full-test')
            self.assertFalse(check['allowed'])
            self.assertEqual(
                check['reason'],
                'ENTRY_AUTHORITY_LEGACY_FULL_PREFLIGHT_MISSING')
        finally:
            book.close()

    def test_full_baseline_strict_contract_unchanged(self):
        # A missing settlement/scope block is still the full path's rejection;
        # the identity scope never supplies an empty six-scope snapshot.
        identity, ev, manifest = self.plan()
        with self.assertRaises((BaselineError, KeyError, TypeError)):
            plan_order_identity_baseline(
                {**identity, 'claims': {**identity['claims'],
                                        'full_account_flat': True}},
                account_key=identity['account_key'], cutoff_utc=AT,
                policy_sha256='b' * 64, source_fingerprint='c' * 64,
                evidence=ev,
                expires_at=(NOW_LATER + timedelta(minutes=1)).isoformat(),
                now=NOW_LATER)
