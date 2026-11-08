"""Current evidence consumed by direct book and old READY entry paths."""
import json
import sqlite3
import tempfile
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from martin_agentic.audit import digest
from martin_agentic.baseline_evidence import normalize_order_identity
from martin_agentic.broker import BrokerJournal
from martin_agentic.current_evidence import compile_current_evidence
from martin_agentic.engineering_preflight import (
    CurrentEngineeringPreflight, NoCurrentPreflight)
import martin_agentic.engineering_preflight as preflight_module
from martin_agentic.entry_authority import DurableEntryAuthority
from martin_agentic.execution import ExecutionBook
from martin_agentic.host_execution import HostExecutionBridge
from martin_agentic.qualification import source_fingerprint
from test_baseline_evidence import AT, NOW_LATER, OBSERVED, BundleFixture, page_data
from test_broker import FakeBroker, position, snapshot
from test_current_evidence import PublicFixture


POLICY = json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())


def facts_assessment(*, account_key='a' * 64, policy_sha256='b' * 64,
                     purpose='engineering', assessed_at=None, **overrides):
    binding = {
        'kind': 'current_account_facts_binding_v1',
        'owner_binding_id': 'owner',
        'account_key': account_key,
        'account_suffix': '0000',
        'policy_sha256': policy_sha256,
        'source_fingerprint': 'c' * 64,
        'capture_generation': 'd' * 64,
        'expected_capture_generation': 'd' * 64,
        'purpose': purpose,
    }
    body = {
        'schema_version': 1,
        'kind': 'current_account_facts_assessment_v1',
        'status': 'facts_ready',
        'assessed_at': assessed_at or NOW_LATER.isoformat(),
        'observed_at': NOW_LATER.isoformat(),
        'observation_valid_from': NOW_LATER.isoformat(),
        'observation_valid_until': (NOW_LATER + timedelta(seconds=30)).isoformat(),
        'purpose': purpose,
        'binding': binding,
        'account': {'key_hash': account_key, 'mask': '••••0000',
                    'suffix': '0000', 'configured_key_hash': account_key},
        'observation_valid': True,
        'facts_qualified': True,
        'account_entry_readiness': True,
        'entry_authorized': False,
        'production_ready': False,
        'facts_are_not_entry_authorization': True,
        'observation_reasons': [],
        'facts_reasons': [],
        'readiness_reasons': [],
        'reasons': [],
    }
    body.update(overrides)
    body['assessment_id'] = digest(body)
    return body


def accept_facts(assessment, *, purpose=None):
    return True, None


def authority_db(*, account_key='a' * 64, policy_hash='b' * 64,
                 scope='full_account_baseline_v1'):
    db = sqlite3.connect(':memory:')
    db.execute('CREATE TABLE entry_authority_marker('
               'required INTEGER,reason TEXT,baseline_id TEXT,'
               'baseline_scope TEXT,created_at TEXT)')
    db.execute('INSERT INTO entry_authority_marker VALUES(1,?,?,?,?)',
               ('BASELINE_APPLIED', 'baseline', scope,
                NOW_LATER.isoformat()))
    db.execute('CREATE TABLE live_state(id INTEGER PRIMARY KEY,payload TEXT NOT NULL)')
    db.execute('INSERT INTO live_state VALUES(1,?)', (json.dumps({
        'account_key': account_key, 'policy_hash': policy_hash}),))
    db.commit()
    return db


class EngineeringPreflightTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.bundle = BundleFixture(Path(self.temp.name) / 'bundle')
        self.public = PublicFixture(Path(self.temp.name) / 'public')
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            compiled = compile_current_evidence(
                self.bundle.root, self.public.manifest, now=NOW_LATER)
        self.account_key = compiled['account']['account_key_hash']
        self.preflight = CurrentEngineeringPreflight(
            self.bundle.root, self.public.manifest,
            account_key=self.account_key, account_suffix='0000',
            policy_sha256=digest(POLICY),
            source_fingerprint_value=source_fingerprint(),
            clock=lambda: NOW_LATER)
        self.book = ExecutionBook(Path(self.temp.name) / 'book.sqlite', POLICY,
                                  simulation=True, clock=lambda: NOW_LATER,
                                  current_preflight=self.preflight)
        self.addCleanup(self.book.close)
        self.plan = {'symbol': 'DEMO', 'quantity': 2,
                     'entry_ceiling': '40', 'stop': '39',
                     'cost_buffer_per_share': '0.05'}
        self.quote = {'symbol': 'DEMO', 'bid': '39.99', 'ask': '40',
                      'bid_at': NOW_LATER.isoformat(),
                      'ask_at': NOW_LATER.isoformat(),
                      'observed_at': NOW_LATER.isoformat()}
        self.evidence = {
            'synthetic': True, 'source_sha256': 'fixture',
            'engineering_scope_authorized': True,
            'parameters_user_specified': True,
            'parameters_valid_until': (NOW_LATER.replace(
                year=2026, month=10, day=16) + __import__('datetime').timedelta(minutes=30)).isoformat()}

    def snap(self):
        value = snapshot(FakeBroker(), NOW_LATER.isoformat())
        value['synthetic'] = True
        return value

    def ingest(self):
        journal = BrokerJournal(self.book.db_path, allow_synthetic=True)
        try:
            journal.ingest(self.snap())
        finally:
            journal.close()

    def propose(self):
        return self.book.propose(
            'engineering-fixture', self.plan, self.quote, self.snap(),
            purpose='engineering', evidence=self.evidence)

    def test_direct_book_consumes_current_evidence_and_blocks_source_gap(self):
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            with self.assertRaisesRegex(
                    ValueError, 'CURRENT_ENGINEERING_PREFLIGHT_BLOCKED'):
                self.propose()
        self.assertEqual(self.book.actions(), [])

    def test_account_policy_source_and_quote_changes_add_reasons(self):
        wrong = CurrentEngineeringPreflight(
            self.bundle.root, self.public.manifest,
            account_key='0' * 64, account_suffix='0000',
            policy_sha256='0' * 64,
            source_fingerprint_value='0' * 64,
            clock=lambda: NOW_LATER)
        self.book._current_preflight = wrong
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            with self.assertRaisesRegex(
                    ValueError, 'CURRENT_ENGINEERING_PREFLIGHT_BLOCKED'):
                self.propose()
        self.assertEqual(self.book.actions(), [])

    def test_host_entry_prepare_uses_same_current_preflight(self):
        self.book._current_preflight = NoCurrentPreflight()
        self.propose()
        action = next(a for a in self.book.actions() if a['kind'] == 'entry')
        self.book.reconcile(self.snap())
        self.ingest()
        self.book._current_preflight = self.preflight
        bridge = HostExecutionBridge(self.book, Path(self.temp.name) / 'host')
        self.addCleanup(bridge.close)
        request = {'schema_version': 1, 'binding': bridge.binding,
                   'operation': 'review', 'action_id': action['id'],
                   'requested_at': NOW_LATER.isoformat()}
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            with self.assertRaisesRegex(
                    ValueError, 'CURRENT_ENGINEERING_PREFLIGHT_BLOCKED'):
                bridge._prepare(request, self.snap(), {'DEMO': self.quote}, True)

    def test_real_book_and_stale_quote_cannot_use_current_preflight(self):
        real = ExecutionBook(Path(self.temp.name) / 'real.sqlite', POLICY,
                             simulation=False, clock=lambda: NOW_LATER,
                             current_preflight=self.preflight)
        try:
            decision = real._current_preflight_decision(
                proposal=True, snapshot=self.snap(), quote=self.quote,
                plan=self.plan, purpose='engineering',
                evidence=self.evidence)
            self.assertFalse(decision['allowed'])
            self.assertIn('FORMAL_ENGINEERING_CHANNEL_UNPROVEN',
                          decision['reasons'])
        finally:
            real.close()
        stale = dict(self.quote, observed_at=(datetime.fromisoformat(AT) - timedelta(seconds=20)).isoformat())
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            decision = self.preflight.check_proposal(
                self.book, self.snap(), stale, self.plan,
                purpose='engineering', evidence=self.evidence)
        self.assertFalse(decision['allowed'])
        self.assertIn('QUOTE_STALE_OR_FUTURE', decision['reasons'])

    def test_old_ready_claim_revalidates_and_does_not_use_propose_green(self):
        # Create the action with the synthetic compatibility preflight.
        self.book._current_preflight = NoCurrentPreflight()
        self.propose()
        action = next(a for a in self.book.actions() if a['kind'] == 'entry')
        self.assertEqual(action['state'], 'READY')
        # The same action must be re-evaluated at claim time using the
        # broker journal/current state, not the last_snapshot timestamp.
        self.book.reconcile(self.snap())
        self.ingest()
        self.book._current_preflight = self.preflight
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            decision = self.preflight.check_claim(self.book, action)
            self.assertIn('BLOCKED_SOURCE:fixed_income_orders',
                          decision['reasons'])
            self.assertNotIn('CURRENT_PREFLIGHT_SNAPSHOT_MISSING',
                             decision['reasons'])
            self.assertIsNone(self.book.claim(allow_entries=True))
        self.assertEqual(next(a for a in self.book.actions()
                              if a['id'] == action['id'])['state'], 'READY')

    def test_verified_public_na_removes_only_the_scope_blockers(self):
        self.public.enable_rhf_product_not_applicable()
        stale = dict(self.quote, observed_at=(
            datetime.fromisoformat(AT) - timedelta(seconds=20)).isoformat())
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            decision = self.preflight.check_proposal(
                self.book, self.snap(), stale, self.plan,
                purpose='engineering', evidence=self.evidence)
        self.assertFalse(decision['allowed'])
        self.assertIn('QUOTE_STALE_OR_FUTURE', decision['reasons'])
        for name in ('fixed_income', 'mutual_funds'):
            self.assertNotIn('BLOCKED_SOURCE:' + name + '_orders',
                             decision['reasons'])
            self.assertNotIn('CURRENT_POSITION_UNPROVEN:' + name,
                             decision['reasons'])
            self.assertNotIn('CURRENT_SCOPE_UNPROVEN:' + name,
                             decision['reasons'])
        self.assertNotIn('CURRENT_SCOPE_SOURCE_UNPROVEN:fixed_income',
                         decision['reasons'])

    def test_preflight_rejects_tampered_public_na_source(self):
        self.public.enable_rhf_product_not_applicable()
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            compiled = compile_current_evidence(
                self.bundle.root, self.public.manifest, now=NOW_LATER)
        tampered = deepcopy(compiled)
        tampered['account_scope_applicability']['scopes']['fixed_income'][
            'applicability']['source_sha256'] = '0' * 64
        with patch('martin_agentic.engineering_preflight.compile_current_evidence',
                   return_value=tampered):
            decision = self.preflight.check_proposal(
                self.book, self.snap(), self.quote, self.plan,
                purpose='engineering', evidence=self.evidence)
        self.assertFalse(decision['allowed'])
        self.assertIn('CURRENT_SCOPE_SOURCE_UNPROVEN:fixed_income',
                      decision['reasons'])

    def test_preflight_uses_broker_buying_power_not_cash(self):
        page = next(p for p in self.bundle.pages
                    if p['tool'] == 'get_portfolio')
        data = page_data(self.bundle, page)
        data.update({'cash': '0', 'total_value': '500',
                     'pending_deposits': '0'})
        data['buying_power'].update({'buying_power': '500',
                                     'unleveraged_buying_power': '500'})
        self.bundle.rewrite_raw(page, data)
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            decision = self.preflight.check_proposal(
                self.book, self.snap(), self.quote, self.plan,
                purpose='engineering', evidence=self.evidence)
        self.assertNotIn('CURRENT_FUNDS_UNPROVEN', decision['reasons'])
        self.assertNotIn('CURRENT_UNLEVERAGED_FUNDS_INSUFFICIENT',
                         decision['reasons'])
        self.assertNotIn('CURRENT_BUYING_POWER_NEGATIVE',
                         decision['reasons'])

    def test_preflight_keeps_pending_and_negative_funds_blocks(self):
        page = next(p for p in self.bundle.pages
                    if p['tool'] == 'get_portfolio')
        data = page_data(self.bundle, page)
        data['pending_deposits'] = '5'
        self.bundle.rewrite_raw(page, data)
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            decision = self.preflight.check_proposal(
                self.book, self.snap(), self.quote, self.plan,
                purpose='engineering', evidence=self.evidence)
        self.assertIn('PENDING_DEPOSIT_REQUIRES_REVIEW',
                      decision['reasons'])

        data['pending_deposits'] = '0'
        data['buying_power']['unleveraged_buying_power'] = '-1'
        self.bundle.rewrite_raw(page, data)
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            decision = self.preflight.check_proposal(
                self.book, self.snap(), self.quote, self.plan,
                purpose='engineering', evidence=self.evidence)
        self.assertIn('CURRENT_BUYING_POWER_NEGATIVE', decision['reasons'])
        self.assertIn('CURRENT_UNLEVERAGED_FUNDS_INSUFFICIENT',
                      decision['reasons'])

        data['buying_power']['unleveraged_buying_power'] = '500'
        data['pending_deposits'] = '-1'
        self.bundle.rewrite_raw(page, data)
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            decision = self.preflight.check_proposal(
                self.book, self.snap(), self.quote, self.plan,
                purpose='engineering', evidence=self.evidence)
        self.assertIn('FUNDS_PENDING_DEPOSITS_NEGATIVE',
                      decision['reasons'])

    def _shared_clock(self, now):
        self.book.clock = lambda: now[0]
        self.preflight.clock = lambda: now[0]
        self.book.entry_authority.clock = lambda: now[0]

    def _full_marker(self):
        self.book.db.execute(
            'CREATE TABLE IF NOT EXISTS entry_authority_marker('
            'required INTEGER,reason TEXT,baseline_id TEXT,'
            'baseline_scope TEXT,created_at TEXT)')
        self.book.db.execute(
            'INSERT INTO entry_authority_marker VALUES(1,?,?,?,?)',
            ('BASELINE_APPLIED', 'fixture-full',
             'full_account_baseline_v1', NOW_LATER.isoformat()))
        state = self.book.state()
        state['account_key'] = self.account_key
        self.book._save(state)
        self.book.db.commit()

    def _align_bundle_and_journal(self):
        page = next(p for p in self.bundle.pages
                    if p['tool'] == 'get_trade_approval_setting')
        data = page_data(self.bundle, page)
        data['setting']['human_must_approve_trades'] = True
        self.bundle.rewrite_raw(page, data)
        snap = snapshot(FakeBroker(), OBSERVED)
        snap['synthetic'] = True
        self.book.reconcile(snap)
        journal = BrokerJournal(self.book.db_path, allow_synthetic=True)
        try:
            journal.ingest(snap)
        finally:
            journal.close()

    def test_default_book_facts_consumer_is_not_always_missing(self):
        self._align_bundle_and_journal()
        self._full_marker()
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            facts = self.book.refresh_current_account_assessment(
                purpose='engineering')
            decision = self.book.entry_authority.check(
                source='facts-consumer', purpose='engineering')
        self.assertIsInstance(facts, dict)
        self.assertTrue(facts['observation_valid'])
        self.assertFalse(facts['facts_qualified'])
        self.assertIn('BLOCKED_SOURCE:futures', facts['facts_reasons'])
        self.assertEqual(decision['reason'],
                         'ENTRY_AUTHORITY_CURRENT_FACTS_BLOCKED')
        self.assertFalse(decision['allowed'])
        self.assertFalse(decision['entry_authorized'])

    def test_default_book_facts_ready_still_has_no_permit(self):
        self.book.reconcile(self.snap())
        self.ingest()
        self._full_marker()
        ready = facts_assessment(account_key=self.account_key,
                                 policy_sha256=digest(POLICY))
        with patch.object(self.preflight, 'current_account_assessment',
                          return_value=ready), patch.object(
                              self.preflight, 'validate_account_assessment',
                              return_value=(True, None)):
            self.book.refresh_current_account_assessment(purpose='engineering')
            decision = self.book.entry_authority.check(
                source='facts-ready', purpose='engineering')
        self.assertEqual(
            decision['reason'],
            'ENTRY_AUTHORITY_CURRENT_FACTS_READY_PERMIT_MISSING')
        self.assertTrue(decision['facts_ready'])
        self.assertTrue(decision['permit_missing'])
        self.assertFalse(decision['allowed'])
        self.assertFalse(decision['entry_authorized'])
        self.assertFalse(decision['production_ready'])

    def test_default_book_requires_owner_binding_for_facts(self):
        self.book.reconcile(self.snap())
        self.ingest()
        self._full_marker()
        wrong = facts_assessment(account_key='e' * 64,
                                 policy_sha256=digest(POLICY))
        with patch.object(self.preflight, 'current_account_assessment',
                          return_value=wrong):
            self.book.refresh_current_account_assessment(purpose='engineering')
            decision = self.book.entry_authority.check(
                source='facts-wrong-account', purpose='engineering')
        self.assertEqual(decision['reason'],
                         'ENTRY_AUTHORITY_CURRENT_FACTS_UNTRUSTED')
        self.assertFalse(decision['allowed'])

    def test_cached_facts_reject_risk_change(self):
        self._align_bundle_and_journal()
        self._full_marker()
        now = [NOW_LATER]
        self._shared_clock(now)
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            self.book.refresh_current_account_assessment(purpose='engineering')
            risk = self.book.risk_guard.state()
            self.assertIsInstance(risk, dict)
            risk['cache_probe'] = 'changed-after-assessment'
            self.book.risk_guard.db.execute(
                'UPDATE account_risk SET payload=? WHERE id=1',
                (json.dumps(risk),))
            self.book.risk_guard.db.commit()
            decision = self.book.entry_authority.check(
                source='risk-changed', purpose='engineering')
        self.assertEqual(decision['reason'],
                         'ENTRY_AUTHORITY_CURRENT_FACTS_UNTRUSTED')
        self.assertFalse(decision['allowed'])

    def test_current_account_facts_require_source_policy_and_generation(self):
        self._align_bundle_and_journal()
        variants = {
            'CURRENT_EVIDENCE_SOURCE_CHANGED': dict(
                source_fingerprint_value='0' * 64),
            'CURRENT_EVIDENCE_POLICY_MISMATCH': dict(
                policy_sha256='0' * 64),
            'CURRENT_CAPTURE_GENERATION_REPLACED': dict(
                expected_capture_generation='0' * 64),
        }
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            for reason, changes in variants.items():
                with self.subTest(reason):
                    preflight = CurrentEngineeringPreflight(
                        self.bundle.root, self.public.manifest,
                        account_key=self.account_key, account_suffix='0000',
                        clock=lambda: NOW_LATER, **{
                            'policy_sha256': self.preflight.policy_sha256,
                            'source_fingerprint_value': (
                                self.preflight.source_fingerprint_value),
                            **changes})
                    facts = preflight.current_account_assessment(
                        self.book, purpose='engineering')
                    self.assertFalse(facts['observation_valid'])
                    self.assertIn(reason, facts['observation_reasons'])

    def test_nonzero_position_is_valid_fact_but_not_ready(self):
        page = next(p for p in self.bundle.pages
                    if p['tool'] == 'get_trade_approval_setting')
        setting = page_data(self.bundle, page)
        setting['setting']['human_must_approve_trades'] = True
        self.bundle.rewrite_raw(page, setting)
        full_position = {
            'symbol': 'DEMO', 'quantity': '1',
            'shares_available_for_sells': '1', 'shares_held_for_sells': '0',
            'shares_held_for_asset_transfer': '0',
            'shares_held_for_options_events': '0',
            'shares_held_for_stock_grants': '0',
            'shares_pending_from_options_events': '0',
        }
        for page in self.bundle.pages:
            if page['tool'] == 'get_equity_positions':
                self.bundle.rewrite_raw(page, {'positions': [full_position]})
        fake = FakeBroker()
        fake.positions = [position()]
        snap = snapshot(fake, OBSERVED)
        snap['synthetic'] = True
        self.book.reconcile(snap)
        journal = BrokerJournal(self.book.db_path, allow_synthetic=True)
        try:
            journal.ingest(snap)
        finally:
            journal.close()
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            facts = self.preflight.current_account_assessment(
                self.book, purpose='engineering')
        self.assertTrue(facts['observation_valid'])
        self.assertFalse(facts['account_entry_readiness'])
        self.assertIn('CURRENT_POSITION_NOT_FLAT:equity',
                      facts['readiness_reasons'])
        self.assertNotIn('CURRENT_POSITION_UNPROVEN:equity',
                         facts['observation_reasons'])

    def test_slow_authority_does_not_extend_original_fact_age(self):
        self._align_bundle_and_journal()
        self._full_marker()
        now = [NOW_LATER]
        self._shared_clock(now)
        original = self.preflight.authority_reader

        def slow(*args, **kwargs):
            value = original(*args, **kwargs)
            now[0] += timedelta(seconds=31)
            return value

        self.preflight.authority_reader = slow
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            facts = self.book.refresh_current_account_assessment(
                purpose='engineering')
            decision = self.book.entry_authority.check(
                source='slow-authority', purpose='engineering')
        self.assertFalse(facts['observation_valid'])
        self.assertTrue(any('STALE' in reason
                            for reason in facts['observation_reasons']))
        self.assertFalse(decision['allowed'])
        self.assertEqual(decision['reason'],
                         'ENTRY_AUTHORITY_CURRENT_FACTS_STALE')

    def test_cached_facts_expire_at_original_validity(self):
        self._align_bundle_and_journal()
        self._full_marker()
        now = [NOW_LATER]
        self._shared_clock(now)
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            facts = self.book.refresh_current_account_assessment(
                purpose='engineering')
            self.assertTrue(facts['observation_valid'])
            now[0] += timedelta(seconds=31)
            decision = self.book.entry_authority.check(
                source='cache-wait', purpose='engineering')
        self.assertFalse(decision['allowed'])
        self.assertEqual(decision['reason'],
                         'ENTRY_AUTHORITY_CURRENT_FACTS_STALE')

    def test_current_account_facts_boundary_and_future(self):
        self._align_bundle_and_journal()
        base = datetime.fromisoformat(OBSERVED)
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            for offset, expected in ((30, True), (30.001, False), (-1, False)):
                with self.subTest(offset=offset):
                    now = [base + timedelta(seconds=offset)]
                    self._shared_clock(now)
                    facts = self.preflight.current_account_assessment(
                        self.book, purpose='engineering')
                    self.assertEqual(facts['observation_valid'], expected)

    def test_generation_final_clock_after_bounded_checks(self):
        self._align_bundle_and_journal()
        now = [NOW_LATER]
        self._shared_clock(now)
        original = preflight_module._public_recheck_reasons

        def slow_public(source, at):
            value = original(source, at)
            now[0] += timedelta(seconds=31)
            return value

        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy), patch.object(
                       preflight_module, '_public_recheck_reasons',
                       side_effect=slow_public):
            facts = self.preflight.current_account_assessment(
                self.book, purpose='engineering')
        self.assertFalse(facts['observation_valid'])
        self.assertTrue(any('STALE' in reason
                            for reason in facts['observation_reasons']))

    def test_slow_validator_boundary_and_stale(self):
        self._align_bundle_and_journal()
        self._full_marker()
        now = [NOW_LATER]
        self._shared_clock(now)
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            self.book.refresh_current_account_assessment(purpose='engineering')
            original = self.preflight.authority_reader
            for advance, expected in ((29, 'ENTRY_AUTHORITY_CURRENT_FACTS_BLOCKED'),
                                      (31, 'ENTRY_AUTHORITY_CURRENT_FACTS_STALE'),
                                      (-2, 'ENTRY_AUTHORITY_CURRENT_FACTS_STALE')):
                with self.subTest(advance=advance):
                    now[0] = NOW_LATER

                    def slow_validate(*args, **kwargs):
                        value = original(*args, **kwargs)
                        now[0] += timedelta(seconds=advance)
                        return value

                    self.preflight.authority_reader = slow_validate
                    decision = self.book.entry_authority.check(
                        source='slow-validator', purpose='engineering')
                    self.assertFalse(decision['allowed'])
                    self.assertEqual(decision['reason'], expected)

    def test_cached_facts_reject_generation_context_and_journal_changes(self):
        self._align_bundle_and_journal()
        self._full_marker()
        now = [NOW_LATER]
        self._shared_clock(now)
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            self.book.refresh_current_account_assessment(purpose='engineering')
            self.preflight.expected_capture_generation = 'replaced-after-assessment'
            generation = self.book.entry_authority.check(
                source='generation-replaced', purpose='engineering')
            self.preflight.expected_capture_generation = None
            original_source = self.preflight.source_fingerprint_value
            self.preflight.source_fingerprint_value = '0' * 64
            source_changed = self.book.entry_authority.check(
                source='source-changed', purpose='engineering')
            self.preflight.source_fingerprint_value = original_source
            original_policy = self.preflight.policy_sha256
            self.preflight.policy_sha256 = '0' * 64
            policy_changed = self.book.entry_authority.check(
                source='policy-changed', purpose='engineering')
            self.preflight.policy_sha256 = original_policy
            self.book._current_preflight = NoCurrentPreflight()
            missing = self.book.entry_authority.check(
                source='context-missing', purpose='engineering')
        self.assertEqual(generation['reason'],
                         'ENTRY_AUTHORITY_CURRENT_FACTS_UNTRUSTED')
        self.assertEqual(source_changed['reason'],
                         'ENTRY_AUTHORITY_CURRENT_FACTS_UNTRUSTED')
        self.assertEqual(policy_changed['reason'],
                         'ENTRY_AUTHORITY_CURRENT_FACTS_UNTRUSTED')
        self.assertEqual(missing['reason'],
                         'ENTRY_AUTHORITY_CURRENT_FACTS_UNTRUSTED')

        # A changed journal row invalidates an otherwise current cache.
        self.book._current_preflight = self.preflight
        self._shared_clock([NOW_LATER])
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            self.book.refresh_current_account_assessment(purpose='engineering')
            row = self.book.db.execute(
                'SELECT payload FROM broker_current WHERE id=1').fetchone()
            payload = json.loads(row[0])
            payload['post_cache_change'] = True
            self.book.db.execute(
                'UPDATE broker_current SET payload=? WHERE id=1',
                (json.dumps(payload),))
            self.book.db.commit()
            changed = self.book.entry_authority.check(
                source='journal-changed', purpose='engineering')
        self.assertEqual(changed['reason'],
                         'ENTRY_AUTHORITY_CURRENT_FACTS_UNTRUSTED')
        self.assertFalse(changed['allowed'])


class EntryAuthorityFactsTests(TestCase):
    def test_full_marker_without_reader_keeps_legacy_missing(self):
        db = authority_db()
        try:
            decision = DurableEntryAuthority(db).check(
                source='no-reader', purpose='engineering')
        finally:
            db.close()
        self.assertFalse(decision['allowed'])
        self.assertEqual(decision['reason'], 'CURRENT_FULL_PREFLIGHT_MISSING')

    def test_reader_absent_and_blocked_are_distinct(self):
        db = authority_db()
        try:
            absent = DurableEntryAuthority(
                db, clock=lambda: NOW_LATER,
                current_facts_reader=lambda **kwargs: None).check(
                    source='absent', purpose='engineering')
            blocked = facts_assessment(
                observation_valid=False, facts_qualified=False,
                account_entry_readiness=False,
                observation_reasons=['CURRENT_EVIDENCE_NOT_VALID'])
            decision = DurableEntryAuthority(
                db, clock=lambda: NOW_LATER,
                current_facts_reader=lambda **kwargs: blocked,
                current_facts_validator=accept_facts).check(
                    source='blocked', purpose='engineering')
        finally:
            db.close()
        self.assertEqual(absent['reason'],
                         'ENTRY_AUTHORITY_CURRENT_FACTS_MISSING')
        self.assertEqual(decision['reason'],
                         'ENTRY_AUTHORITY_CURRENT_FACTS_BLOCKED')
        self.assertIn('CURRENT_EVIDENCE_NOT_VALID', decision['reasons'])

    def test_facts_ready_is_not_entry_authorized(self):
        db = authority_db()
        try:
            ready = facts_assessment()
            decision = DurableEntryAuthority(
                db, clock=lambda: NOW_LATER,
                current_facts_reader=lambda **kwargs: ready,
                current_facts_validator=accept_facts).check(
                    source='ready', purpose='engineering')
        finally:
            db.close()
        self.assertFalse(decision['allowed'])
        self.assertEqual(
            decision['reason'],
            'ENTRY_AUTHORITY_CURRENT_FACTS_READY_PERMIT_MISSING')
        self.assertTrue(decision['facts_ready'])
        self.assertTrue(decision['permit_missing'])
        self.assertFalse(decision['entry_authorized'])

    def test_facts_binding_staleness_and_identity_marker_are_rejected(self):
        db = authority_db()
        try:
            wrong = facts_assessment(account_key='e' * 64)
            untrusted = DurableEntryAuthority(
                db, clock=lambda: NOW_LATER, current_facts_reader=lambda **kwargs: wrong,
                current_facts_validator=accept_facts).check(
                    source='wrong', purpose='engineering')
            stale = facts_assessment(
                assessed_at=(NOW_LATER - timedelta(seconds=31)).isoformat())
            stale_decision = DurableEntryAuthority(
                db, clock=lambda: NOW_LATER, current_facts_reader=lambda **kwargs: stale,
                current_facts_validator=accept_facts).check(
                    source='stale', purpose='engineering')
            future = facts_assessment(
                assessed_at=(NOW_LATER + timedelta(seconds=1)).isoformat())
            future_decision = DurableEntryAuthority(
                db, clock=lambda: NOW_LATER, current_facts_reader=lambda **kwargs: future,
                current_facts_validator=accept_facts).check(
                    source='future', purpose='engineering')
            tampered = facts_assessment()
            tampered['assessment_id'] = '0' * 64
            tampered_decision = DurableEntryAuthority(
                db, clock=lambda: NOW_LATER, current_facts_reader=lambda **kwargs: tampered,
                current_facts_validator=accept_facts).check(
                    source='tampered', purpose='engineering')
        finally:
            db.close()
        self.assertEqual(untrusted['reason'],
                         'ENTRY_AUTHORITY_CURRENT_FACTS_UNTRUSTED')
        self.assertEqual(stale_decision['reason'],
                         'ENTRY_AUTHORITY_CURRENT_FACTS_STALE')
        self.assertEqual(future_decision['reason'],
                         'ENTRY_AUTHORITY_CURRENT_FACTS_STALE')
        self.assertEqual(tampered_decision['reason'],
                         'ENTRY_AUTHORITY_CURRENT_FACTS_UNTRUSTED')

        db = authority_db(scope='terminal_equity_order_identity_v1')
        try:
            ready = facts_assessment()
            identity = DurableEntryAuthority(
                db, clock=lambda: NOW_LATER, current_facts_reader=lambda **kwargs: ready,
                current_facts_validator=accept_facts).check(
                    source='identity', purpose='engineering')
        finally:
            db.close()
        self.assertEqual(identity['reason'],
                         'ENTRY_AUTHORITY_IDENTITY_ONLY')

