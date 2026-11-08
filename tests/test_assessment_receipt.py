"""Owner-bound receipt binding, current consistency, and default factory paths."""
from copy import deepcopy
from datetime import datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time
from unittest import TestCase
from unittest.mock import patch

from martin_agentic.audit import canonical, digest
from martin_agentic.broker import BrokerJournal
from martin_agentic.current_context import (
    CurrentContextFactory, CurrentContextRegistry, MissingCurrentContext,
    ensure_default_preflight)
from martin_agentic.current_evidence import (
    capture_generation, compile_current_evidence,
    load_public_product_evidence)
from martin_agentic.engineering_preflight import (
    CurrentEngineeringPreflight, NoCurrentPreflight)
from martin_agentic.execution import ExecutionBook
from martin_agentic.host_execution import HostExecutionBridge
from martin_luk.live_session import LiveSupervisor
from martin_agentic.qualification import (
    source_fingerprint, source_files, source_pin)
from martin_agentic.assessment_receipt import capture_recheck_reasons
from test_baseline_evidence import NOW_LATER, OBSERVED, BundleFixture
from test_broker import FakeBroker, snapshot
from test_current_evidence import PublicFixture
from test_engineering_preflight import POLICY


class StagedAuthorityReader:
    def __init__(self):
        self.values = {
            'risk': {'status': 'evaluated', 'blocked': False, 'reason': None,
                     'state_sha256': 'a' * 64},
            'lineage': {'status': 'evaluated', 'blocked': False,
                        'reason': 'TEST_LINEAGE', 'result_sha256': 'b' * 64},
            'control': {'status': 'evaluated', 'blocked': False,
                        'reason': 'TEST_CONTROL', 'result_sha256': 'c' * 64},
        }

    def __call__(self, book, *, purpose=None, evidence=None, snapshot=None):
        return deepcopy(self.values)


class ReceiptBindingTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.bundle = BundleFixture(Path(self.temp.name) / 'bundle')
        self.public = PublicFixture(Path(self.temp.name) / 'public')
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            self.compiled = compile_current_evidence(
                self.bundle.root, self.public.manifest, now=NOW_LATER)
        self.capture_at = self.compiled['observed_at']
        self.account_key = self.compiled['account']['account_key_hash']
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
            'parameters_valid_until': (NOW_LATER + timedelta(minutes=30)).isoformat()}
        self.action = self._ready_action()

    def snap(self):
        value = snapshot(FakeBroker(), self.capture_at)
        value['synthetic'] = True
        value['human_must_approve_trades'] = False
        return value

    def _ready_action(self):
        self.book._current_preflight = NoCurrentPreflight()
        self.book.propose('engineering-fixture', self.plan, self.quote,
                          self.snap(), purpose='engineering',
                          evidence=self.evidence)
        self.book.reconcile(self.snap())
        journal = BrokerJournal(self.book.db_path, allow_synthetic=True)
        try:
            journal.ingest(self.snap())
        finally:
            journal.close()
        self.book._current_preflight = self.preflight
        return next(a for a in self.book.actions() if a['kind'] == 'entry')

    def claim(self):
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            return self.preflight.check_claim(self.book, self.action)

    def binding(self, decision, action=None):
        return self.book.current_preflight_binding(
            decision, action=self.action if action is None else action)

    def set_clock(self, when):
        self.book.clock = lambda: when
        self.preflight.clock = lambda: when

    def set_quote(self, when):
        action = next(a for a in self.book.actions()
                      if a['id'] == self.action['id'])
        payload = json.loads(action['payload'])
        for key in ('observed_at', 'bid_at', 'ask_at'):
            payload['preflight_quote'][key] = when.isoformat()
        self.book.db.execute('UPDATE live_actions SET payload=? WHERE id=?',
                             (canonical(payload), self.action['id']))
        self.book.db.commit()
        self.action = next(a for a in self.book.actions()
                           if a['id'] == self.action['id'])

    def test_final_recheck_rejects_capture_age_31_not_quote(self):
        late = (datetime.fromisoformat(self.capture_at)
                + timedelta(seconds=29))
        self.set_clock(late)
        self.set_quote(late)
        decision = self.claim()
        self.assertTrue(self.binding(decision)['bound'])
        later = late + timedelta(seconds=2)
        self.set_clock(later)
        reasons = self.binding(decision)['reasons']
        self.assertIn('RECEIPT_CAPTURE_STALE', reasons)
        self.assertNotIn('RECEIPT_QUOTE_OBSERVED_STALE', reasons)

    def test_exact_capture_30_boundary_and_one_second_past(self):
        boundary = (datetime.fromisoformat(self.capture_at)
                    + timedelta(seconds=30))
        self.set_clock(boundary)
        self.set_quote(boundary)
        decision = self.claim()
        self.assertTrue(self.binding(decision)['bound'])
        self.set_clock(boundary + timedelta(seconds=1))
        self.assertIn('RECEIPT_CAPTURE_STALE',
                      self.binding(decision)['reasons'])

    def test_exact_quote_15_boundary_and_one_second_past(self):
        boundary = datetime.fromisoformat(self.capture_at)
        self.set_clock(boundary)
        self.set_quote(boundary - timedelta(seconds=15))
        decision = self.claim()
        self.assertTrue(self.binding(decision)['bound'])
        self.set_clock(boundary + timedelta(seconds=1))
        reasons = self.binding(decision)['reasons']
        self.assertIn('RECEIPT_QUOTE_OBSERVED_STALE', reasons)
        self.assertNotIn('RECEIPT_CAPTURE_STALE', reasons)

    def test_old_bid_or_ask_cannot_be_masked_by_fresh_observed_at(self):
        boundary = datetime.fromisoformat(self.capture_at)
        self.set_clock(boundary)
        self.set_quote(boundary)
        action = next(a for a in self.book.actions()
                      if a['id'] == self.action['id'])
        payload = json.loads(action['payload'])
        payload['preflight_quote']['bid_at'] = (
            boundary - timedelta(seconds=20)).isoformat()
        payload['preflight_quote']['ask_at'] = (
            boundary - timedelta(seconds=20)).isoformat()
        self.book.db.execute('UPDATE live_actions SET payload=? WHERE id=?',
                             (canonical(payload), self.action['id']))
        self.book.db.commit()
        self.action = next(a for a in self.book.actions()
                           if a['id'] == self.action['id'])
        decision = self.claim()
        reasons = self.binding(decision)['reasons']
        self.assertIn('RECEIPT_QUOTE_BID_STALE', reasons)
        self.assertIn('RECEIPT_QUOTE_ASK_STALE', reasons)
        self.assertNotIn('RECEIPT_QUOTE_OBSERVED_STALE', reasons)
        self.assertNotIn('RECEIPT_CAPTURE_STALE', reasons)

    def test_future_capture_and_public_expiry_are_specific(self):
        decision = self.claim()
        before = datetime.fromisoformat(self.capture_at) - timedelta(seconds=1)
        self.set_clock(before)
        reasons = self.binding(decision)['reasons']
        self.assertIn('RECEIPT_CAPTURE_FUTURE', reasons)

        self.set_clock(datetime.fromisoformat(self.capture_at)
                       + timedelta(days=31))
        reasons = self.binding(decision)['reasons']
        self.assertIn('RECEIPT_PUBLIC_SOURCE_EXPIRED',
                      ' '.join(reasons))

    def test_journal_health_and_assessed_at_are_final_rechecked(self):
        decision = self.claim()
        self.set_clock(datetime.fromisoformat(self.capture_at)
                       + timedelta(seconds=31))
        reasons = self.binding(decision)['reasons']
        self.assertIn('RECEIPT_JOURNAL_STALE', reasons)
        self.assertIn('RECEIPT_HEALTH_STALE', reasons)
        self.assertIn('RECEIPT_CAPTURE_STALE', reasons)

        self.set_clock(NOW_LATER)
        self.preflight.clock = lambda: NOW_LATER + timedelta(seconds=5)
        future = self.claim()
        self.set_clock(NOW_LATER)
        self.assertIn('RECEIPT_ASSESSED_AT_FUTURE',
                      self.binding(future)['reasons'])

    def test_source_pin_limit_fails_closed_and_tail_change_is_not_hidden(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            folder = root / 'infra/src/martin_agentic'
            folder.mkdir(parents=True)
            for index in range(5002):
                (folder / f'f{index:05}.py').write_text('x=1\n')
            pin = source_pin(root)
            self.assertEqual(pin['status'], 'limit_exceeded')
            self.assertTrue(pin['truncated'])
            self.assertIn('FILE_LIMIT', pin['reason'])
            before_fingerprint = source_fingerprint(root)
            (folder / 'f05001.py').write_text('x=2\n')
            self.assertNotEqual(before_fingerprint, source_fingerprint(root))
            self.assertNotEqual(pin['status'], 'bounded')

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            folder = root / 'infra/src/martin_agentic'
            folder.mkdir(parents=True)
            for index in range(5):
                (folder / f'f{index}.py').write_text('x=1\n')
            first = source_pin(root)
            self.assertEqual(first['status'], 'bounded')
            self.assertEqual(first['count'], 5)
            (folder / 'f999.py').write_text('x=1\n')
            second = source_pin(root)
            self.assertEqual(second['status'], 'bounded')
            self.assertNotEqual(first['digest'], second['digest'])

    def test_source_pin_normal_set_matches_full_source_set(self):
        pin = source_pin()
        self.assertEqual(pin['status'], 'bounded')
        self.assertFalse(pin['truncated'])
        self.assertEqual(pin['count'], len(source_files()))


    def test_source_pin_and_source_files_share_exact_qualified_inventory(self):
        from martin_agentic.qualification import _source_pin_scan
        root = Path('.').resolve()
        self.assertEqual(
            set(_source_pin_scan(root)),
            {path.relative_to(root).as_posix() for path in source_files(root)})

    def test_source_pin_tracks_each_new_example_json(self):
        import shutil
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            shutil.copytree('examples/plugins/example_momentum',
                            root / 'examples/plugins/example_momentum')
            names = ('events-expired.json', 'events-mean-reversion.json',
                     'events-momentum.json', 'policy.json')
            for name in names:
                path = root / 'examples/plugins/example_momentum' / name
                first = source_pin(root)
                path.write_text(path.read_text() + '\n')
                second = source_pin(root)
                self.assertEqual(first['status'], 'bounded')
                self.assertEqual(second['status'], 'bounded')
                self.assertNotEqual(first['digest'], second['digest'], name)

    def test_source_pin_limit_breaks_binding_with_specific_reason(self):
        decision = self.claim()
        limited = {'schema_version': 1, 'status': 'limit_exceeded',
                   'reason': 'SOURCE_PIN_FILE_LIMIT', 'truncated': True}
        with patch('martin_agentic.assessment_receipt.source_pin',
                   return_value=limited):
            reasons = self.binding(decision)['reasons']
        self.assertIn('RECEIPT_SOURCE_PIN_LIMIT:SOURCE_PIN_FILE_LIMIT',
                      reasons)

    def test_capture_recheck_bounded_extra_remove_fifo_and_symlink(self):
        capture = capture_generation(self.bundle.root)
        self.assertEqual(capture_recheck_reasons(capture), [])
        raw = self.bundle.raw
        extra = raw / 'zzz-extra.raw'
        extra.write_bytes(b'extra')
        self.assertIn('RECEIPT_CAPTURE_EXTRA_FILES',
                      capture_recheck_reasons(capture))
        extra.unlink()
        victim = next(raw.glob('*.raw'))
        moved = self.bundle.root / 'moved.raw'
        victim.rename(moved)
        self.assertIn('RECEIPT_CAPTURE_GENERATION_CHANGED',
                      capture_recheck_reasons(capture))
        moved.rename(victim)
        fifo = raw / 'fifo.raw'
        os.mkfifo(fifo)
        try:
            self.assertIn('RECEIPT_CAPTURE_EXTRA_FILES',
                          capture_recheck_reasons(capture))
        finally:
            fifo.unlink()
        link = raw / 'link.raw'
        link.symlink_to(victim.name)
        try:
            self.assertIn('RECEIPT_CAPTURE_EXTRA_FILES',
                          capture_recheck_reasons(capture))
        finally:
            link.unlink()
        for index in range(50):
            (raw / f'extra-{index}.raw').write_bytes(b'x')
        started = time.monotonic()
        self.assertIn('RECEIPT_CAPTURE_EXTRA_FILES',
                      capture_recheck_reasons(capture))
        self.assertLess(time.monotonic() - started, 1.0)

    def test_valid_matching_receipt_binding_is_true_while_source_gaps_block(self):
        decision = self.claim()
        self.assertFalse(decision['allowed'])
        self.assertIn('BLOCKED_SOURCE:fixed_income_orders',
                      decision['reasons'])
        self.assertNotIn('FORMAL_ENGINEERING_CHANNEL_UNPROVEN',
                         decision['reasons'])
        receipt = decision['receipt']
        self.assertEqual(receipt['account']['key_hash'], self.account_key)
        self.assertNotIn('account_key_hash', decision['evidence'])
        result = self.binding(decision)
        self.assertTrue(result['bound'], result['reasons'])
        self.assertEqual(result['reasons'], [])
        components = receipt['components']
        for name in ('current_evidence', 'capture_generation', 'journal_current',
                     'journal_health', 'journal_capture_consistency', 'action',
                     'quote', 'risk', 'lineage', 'control'):
            self.assertEqual(components[name]['status'], 'evaluated', name)
        self.assertEqual(components['formal_channel']['status'],
                         'not_applicable')

    def test_expected_capture_generation_replacement_is_rejected(self):
        expected = CurrentEngineeringPreflight(
            self.bundle.root, self.public.manifest,
            account_key=self.account_key, account_suffix='0000',
            policy_sha256=digest(POLICY),
            source_fingerprint_value=source_fingerprint(),
            clock=lambda: NOW_LATER,
            expected_capture_generation='f' * 64)
        self.book._current_preflight = expected
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            decision = expected.check_claim(self.book, self.action)
        self.assertIn('CURRENT_CAPTURE_GENERATION_REPLACED',
                      decision['reasons'])
        reasons = self.binding(decision)['reasons']
        self.assertIn('RECEIPT_CAPTURE_GENERATION_REPLACED', reasons)

    def test_account_policy_source_manifest_and_generation_changes_break_binding(self):
        decision = self.claim()
        self.assertTrue(self.binding(decision)['bound'])

        state = self.book.state()
        original = state['account_key']
        state['account_key'] = 'f' * 64
        self.book.db.execute('UPDATE live_state SET payload=? WHERE id=1',
                             (canonical(state),))
        self.book.db.commit()
        self.assertIn('RECEIPT_ACCOUNT_MISMATCH',
                      self.binding(decision)['reasons'])
        state['account_key'] = original
        self.book.db.execute('UPDATE live_state SET payload=? WHERE id=1',
                             (canonical(state),))
        self.book.db.commit()

        self.book.policy['account_capital_usd'] = '501'
        self.assertIn('RECEIPT_POLICY_MISMATCH',
                      self.binding(decision)['reasons'])
        self.book.policy['account_capital_usd'] = '500'

        with patch('martin_agentic.assessment_receipt.source_pin',
                   return_value=dict(decision['receipt']['source']['pin'],
                                     digest='0' * 64)):
            self.assertIn('RECEIPT_SOURCE_CHANGED',
                          self.binding(decision)['reasons'])

        wrong = CurrentEngineeringPreflight(
            self.bundle.root, self.public.manifest,
            account_key=self.account_key, account_suffix='0000',
            policy_sha256=digest(POLICY),
            source_fingerprint_value='0' * 64, clock=lambda: NOW_LATER)
        self.book._current_preflight = wrong
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            wrong_decision = wrong.check_claim(self.book, self.action)
        self.assertIn('RECEIPT_SOURCE_FINGERPRINT_MISMATCH',
                      self.binding(wrong_decision)['reasons'])
        self.book._current_preflight = self.preflight

        wrong_account = CurrentEngineeringPreflight(
            self.bundle.root, self.public.manifest,
            account_key='0' * 64, account_suffix='0000',
            policy_sha256=digest(POLICY),
            source_fingerprint_value=source_fingerprint(),
            clock=lambda: NOW_LATER)
        self.book._current_preflight = wrong_account
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            wrong_account_decision = wrong_account.check_claim(
                self.book, self.action)
        self.assertIn('RECEIPT_ACCOUNT_BINDING_MISMATCH',
                      self.binding(wrong_account_decision)['reasons'])
        self.book._current_preflight = self.preflight

        self.public.manifest.write_text(
            self.public.manifest.read_text() + ' ')
        self.assertIn('RECEIPT_PUBLIC_MANIFEST_CHANGED',
                      self.binding(decision)['reasons'])
        self.public.write()

        raw = next(self.bundle.raw.glob('*.raw'))
        raw.write_bytes(raw.read_bytes() + b'x')
        self.assertIn('RECEIPT_CAPTURE_GENERATION_CHANGED',
                      self.binding(decision)['reasons'])

    def test_journal_change_breaks_binding(self):
        decision = self.claim()
        self.assertTrue(self.binding(decision)['bound'])

        raw = json.loads(self.book.db.execute(
            'SELECT payload FROM broker_current WHERE id=1').fetchone()[0])
        raw['portfolio']['cash'] = '501'
        self.book.db.execute('UPDATE broker_current SET payload=? WHERE id=1',
                             (json.dumps(raw),))
        self.book.db.commit()
        self.assertIn('RECEIPT_JOURNAL_CHANGED',
                      self.binding(decision)['reasons'])

    def test_health_change_breaks_binding_and_is_reported(self):
        decision = self.claim()
        self.book.db.execute(
            'UPDATE broker_health SET payload=? WHERE id=1',
            (json.dumps({'healthy': True, 'at': '2000-01-01T00:00:00+00:00'}),))
        self.book.db.commit()
        reasons = self.binding(decision)['reasons']
        self.assertIn('RECEIPT_HEALTH_CHANGED', reasons)
        self.assertIn('RECEIPT_HEALTH_SNAPSHOT_MISMATCH', reasons)
        stale = self.claim()
        self.assertIn('CURRENT_PREFLIGHT_HEALTH_STALE', stale['reasons'])

    def test_health_future_and_different_snapshot_are_rejected(self):
        future = (NOW_LATER + timedelta(minutes=2)).isoformat()
        self.book.db.execute(
            'UPDATE broker_health SET payload=? WHERE id=1',
            (json.dumps({'healthy': True, 'at': future}),))
        self.book.db.commit()
        self.assertIn('CURRENT_PREFLIGHT_HEALTH_FUTURE',
                      self.claim()['reasons'])

        different = (NOW_LATER - timedelta(seconds=5)).isoformat()
        self.book.db.execute(
            'UPDATE broker_health SET payload=? WHERE id=1',
            (json.dumps({'healthy': True, 'at': different}),))
        self.book.db.commit()
        self.assertIn('CURRENT_PREFLIGHT_HEALTH_SNAPSHOT_MISMATCH',
                      self.claim()['reasons'])

    def test_action_state_and_quote_changes_break_binding(self):
        decision = self.claim()
        self.book.db.execute(
            "UPDATE live_actions SET state='WAITING_USER' WHERE id=?",
            (self.action['id'],))
        self.book.db.commit()
        changed = next(a for a in self.book.actions()
                       if a['id'] == self.action['id'])
        reasons = self.binding(decision, action=changed)['reasons']
        self.assertIn('RECEIPT_ACTION_CHANGED', reasons)

    def test_quote_stale_breaks_binding(self):
        action = next(a for a in self.book.actions()
                      if a['id'] == self.action['id'])
        payload = json.loads(action['payload'])
        payload['preflight_quote'] = dict(
            payload['preflight_quote'],
            observed_at=(NOW_LATER - timedelta(seconds=20)).isoformat())
        self.book.db.execute('UPDATE live_actions SET payload=? WHERE id=?',
                             (canonical(payload), self.action['id']))
        self.book.db.commit()
        refreshed = next(a for a in self.book.actions()
                         if a['id'] == self.action['id'])
        decision = self.claim()
        reasons = self.binding(decision, action=refreshed)['reasons']
        self.assertIn('RECEIPT_QUOTE_STALE', reasons)

    def test_capture_journal_position_order_funds_conflicts_are_specific(self):
        action = self.action
        baseline = self.claim()
        self.assertTrue(self.binding(baseline, action=action)['bound'])
        raw = json.loads(self.book.db.execute(
            'SELECT payload FROM broker_current WHERE id=1').fetchone()[0])
        raw['positions'] = [{'symbol': 'DEMO', 'quantity': '1',
                             'available_for_sells': '1',
                             'held_for_sells': '0'}]
        raw['orders'] = {'foreign-order': {'id': 'foreign-order',
                                           'state': 'confirmed',
                                           'symbol': 'DEMO', 'side': 'buy',
                                           'filled': '0', 'quantity': '1'}}
        raw['portfolio']['cash'] = '600'
        self.book.db.execute('UPDATE broker_current SET payload=? WHERE id=1',
                             (json.dumps(raw),))
        self.book.db.commit()
        decision = self.claim()
        self.assertIn('CURRENT_JOURNAL_POSITION_CONFLICT:DEMO',
                      ' '.join(decision['reasons']))
        self.assertIn('CURRENT_JOURNAL_ORDER_NOT_IN_CAPTURE:foreign-order',
                      decision['reasons'])
        self.assertIn('CURRENT_JOURNAL_FUNDS_MISMATCH:cash',
                      ' '.join(decision['reasons']))
        self.assertFalse(self.binding(baseline, action=action)['bound'])

    def test_proposal_authorization_context_is_rechecked(self):
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            decision = self.preflight.check_proposal(
                self.book, self.snap(), self.quote, self.plan,
                purpose='engineering', evidence=self.evidence)
        def check(purpose, evidence):
            return self.book.current_preflight_binding(
                decision, require_journal=False, purpose=purpose,
                evidence=evidence, snapshot=self.snap(),
                expected_account_key=self.account_key)
        self.assertTrue(check('engineering', self.evidence)['bound'])
        self.assertFalse(check('strategy', self.evidence)['bound'])
        self.assertIn('RECEIPT_AUTHORIZATION_PURPOSE_CHANGED',
                      check('strategy', self.evidence)['reasons'])
        changed = {**self.evidence, 'parameters_user_specified': False,
                   'parameters_valid_until': '2000-01-01T00:00:00+00:00'}
        self.assertIn('RECEIPT_AUTHORIZATION_EVIDENCE_CHANGED',
                      check('engineering', changed)['reasons'])
        self.assertIn('RECEIPT_AUTHORIZATION_CONTEXT_MISSING',
                      check(None, None)['reasons'])
        reordered = dict(reversed(list(self.evidence.items())))
        self.assertTrue(check('engineering', reordered)['bound'])

    def test_owner_trade_purpose_and_evidence_changes_break_binding(self):
        decision = self.claim()
        self.assertTrue(self.binding(decision)['bound'])
        state = self.book.state()
        trade = state['trades']['engineering-fixture']
        original = deepcopy(trade)
        for change in (
                {'purpose': 'strategy'},
                {'evidence': {**original['evidence'],
                              'parameters_user_specified': False}},
                {'evidence': {**original['evidence'],
                              'parameters_valid_until':
                                  '2000-01-01T00:00:00+00:00'}}):
            trade.update(deepcopy(change))
            self.book.db.execute(
                'UPDATE live_state SET payload=? WHERE id=1',
                (canonical(state),))
            self.book.db.commit()
            result = self.binding(decision)
            self.assertFalse(result['bound'], change)
            self.assertTrue(any(reason.startswith(
                'RECEIPT_AUTHORIZATION_') for reason in result['reasons']),
                result['reasons'])
            trade.update(original)
            self.book.db.execute(
                'UPDATE live_state SET payload=? WHERE id=1',
                (canonical(state),))
            self.book.db.commit()
        self.assertTrue(self.binding(decision)['bound'])

    def test_risk_and_staged_lineage_control_changes_break_binding(self):
        reader = StagedAuthorityReader()
        staged = CurrentEngineeringPreflight(
            self.bundle.root, self.public.manifest,
            account_key=self.account_key, account_suffix='0000',
            policy_sha256=digest(POLICY),
            source_fingerprint_value=source_fingerprint(),
            clock=lambda: NOW_LATER, authority_reader=reader)
        self.book._current_preflight = staged
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            decision = staged.check_claim(self.book, self.action)
        self.assertTrue(self.binding(decision)['bound'])
        reader.values['control']['blocked'] = True
        self.assertIn('RECEIPT_CONTROL_CHANGED',
                      self.binding(decision)['reasons'])
        reader.values['control']['blocked'] = False
        reader.values['lineage']['blocked'] = True
        self.assertIn('RECEIPT_LINEAGE_CHANGED',
                      self.binding(decision)['reasons'])

        # Default owner reader: a real risk-sidecar state change is rechecked.
        self.book._current_preflight = self.preflight
        decision = self.claim()
        state = self.book.risk_guard.state()
        state['blocked'] = True
        state['reasons'] = ['TEST_RISK_CHANGE']
        self.book.risk_guard.db.execute(
            'UPDATE account_risk SET payload=? WHERE id=1',
            (canonical(state),))
        self.book.risk_guard.db.commit()
        self.assertIn('RECEIPT_RISK_CHANGED',
                      self.binding(decision)['reasons'])

    def test_entry_denied_still_allows_protect_claim(self):
        trade = self.book.state()['trades']['engineering-fixture']
        self.book._queue(
            trade, 'protect',
            {'symbol': 'DEMO', 'side': 'sell', 'type': 'stop_market',
             'quantity': '2', 'stop_price': '39',
             'market_hours': 'regular_hours', 'time_in_force': 'gtc'},
            NOW_LATER, 'TEST_PROTECTION_CONTINUES')
        self.book.db.commit()
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            claimed = self.book.claim(allow_entries=True)
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed['kind'], 'protect')
        entry = next(a for a in self.book.actions()
                     if a['kind'] == 'entry')
        self.assertEqual(entry['state'], 'READY')

    def test_entry_denied_still_allows_cancel_claim(self):
        trade = self.book.state()['trades']['engineering-fixture']
        self.book._queue(trade, 'cancel', {'order_id': 'foreign-1'},
                         NOW_LATER, 'TEST_CANCEL_CONTINUES', target='foreign-1')
        self.book.db.commit()
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            claimed = self.book.claim(allow_entries=True)
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed['kind'], 'cancel')
        entry = next(a for a in self.book.actions() if a['kind'] == 'entry')
        self.assertEqual(entry['state'], 'READY')

    def test_entry_denied_still_allows_exit_claim(self):
        trade = self.book.state()['trades']['engineering-fixture']
        self.book._queue(
            trade, 'exit',
            {'symbol': 'DEMO', 'side': 'sell', 'type': 'market',
             'quantity': '2', 'market_hours': 'regular_hours',
             'time_in_force': 'gfd'},
            NOW_LATER, 'TEST_EXIT_CONTINUES')
        self.book.db.commit()
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            claimed = self.book.claim(allow_entries=True)
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed['kind'], 'exit')
        entry = next(a for a in self.book.actions() if a['kind'] == 'entry')
        self.assertEqual(entry['state'], 'READY')

    def test_synthetic_default_cannot_grant_real_book(self):
        real = ExecutionBook(Path(self.temp.name) / 'real-default.sqlite',
                             POLICY, simulation=False, clock=lambda: NOW_LATER)
        self.addCleanup(real.close)
        self.assertFalse(real._preflight_binding_ok({'allowed': True,
                                                     'reasons': []}))
        self.book._current_preflight = NoCurrentPreflight()
        self.assertTrue(self.book._preflight_binding_ok(
            {'allowed': True, 'reasons': []}))

    def test_compile_happens_outside_lock_and_recheck_is_bounded(self):
        calls = []
        original = self.preflight.check_claim

        def wrapped(book, action):
            calls.append(book.db.in_transaction)
            return original(book, action)

        self.preflight.check_claim = wrapped
        try:
            with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                       self.public.policy):
                self.book.claim(allow_entries=True)
        finally:
            self.preflight.check_claim = original
        self.assertTrue(calls)
        self.assertTrue(all(value is False for value in calls))

        decision = self.claim()
        self.book._begin()
        try:
            with patch('martin_agentic.engineering_preflight.source_fingerprint',
                       side_effect=AssertionError('full source scan under lock')):
                self.assertTrue(self.book.db.in_transaction)
                self.assertTrue(self.book._preflight_binding_ok(
                    decision, action=self.action, require_journal=True))
        finally:
            self.book.db.rollback()


class DefaultContextFactoryTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.bundle = BundleFixture(Path(self.temp.name) / 'bundle')
        self.public = PublicFixture(Path(self.temp.name) / 'public')
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            self.compiled = compile_current_evidence(
                self.bundle.root, self.public.manifest, now=NOW_LATER)
        self.generation = capture_generation(self.bundle.root)
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            self.public_evidence = load_public_product_evidence(
                self.public.manifest, now=NOW_LATER)
        self.registry_path = Path(self.temp.name) / 'registry.json'
        self.record = {
            'schema_version': 1,
            'kind': 'current_capture_registry_v1',
            'generation_id': self.generation['generation'],
            'capture_root': str(self.bundle.root),
            'capture_manifest_sha256': self.generation['manifest_sha256'],
            'raw_chain_sha256': self.generation['raw_chain_sha256'],
            'public_manifest': str(self.public.manifest),
            'public_manifest_sha256': self.public_evidence['manifest_sha256'],
            'account_key_hash': self.compiled['account']['account_key_hash'],
            'account_suffix': '0000',
            'policy_sha256': digest(POLICY),
            'source_fingerprint': source_fingerprint(),
            'published_at': NOW_LATER.isoformat(),
            'producer': 'test-fixture-producer',
        }
        self.write_registry()

    def write_registry(self):
        self.registry_path.write_text(json.dumps(self.record, sort_keys=True))
        os.chmod(self.registry_path, 0o600)

    def registry(self):
        return CurrentContextRegistry(
            self.registry_path,
            allowed_capture_roots=(self.bundle.root,),
            allowed_public_roots=(self.public.root,),
            clock=lambda: NOW_LATER)

    def test_factory_binds_owner_generation_and_reports_evidence_hash(self):
        book = ExecutionBook(Path(self.temp.name) / 'factory.sqlite', POLICY,
                             simulation=False, clock=lambda: NOW_LATER)
        self.addCleanup(book.close)
        factory = CurrentContextFactory(self.registry(),
                                        clock=lambda: NOW_LATER)
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            context = factory(book)
        self.assertIsInstance(context, CurrentEngineeringPreflight)
        self.assertEqual(context.expected_capture_generation,
                         self.record['generation_id'])
        self.assertEqual(factory.calls, 1)
        self.assertIsNone(factory.last_error)
        self.assertEqual(factory.last_evidence['capture_generation'],
                         self.generation['generation'])
        self.assertEqual(factory.last_evidence['registry_sha256'],
                         hashlib.sha256(self.registry_path.read_bytes()).hexdigest())
        self.assertEqual(factory.last_evidence['public_manifest_sha256'],
                         self.public_evidence['manifest_sha256'])

    def test_default_missing_context_is_structured_not_allowed(self):
        class BareBook:
            simulation = False
            policy = POLICY
            clock = staticmethod(lambda: NOW_LATER)
            _current_preflight = NoCurrentPreflight()

        book = BareBook()
        context, status = ensure_default_preflight(book)
        self.assertIsInstance(context, MissingCurrentContext)
        self.assertEqual(status['status'], 'missing')
        self.assertEqual(status['factory_calls'], 1)
        decision = context.check_claim(book, None)
        self.assertFalse(decision['allowed'])
        self.assertEqual(decision['reasons'],
                         ['CURRENT_PREFLIGHT_CONTEXT_MISSING'])
        self.assertEqual(
            {item['status'] for item in decision['components'].values()},
            {'not_evaluated'})

    def test_live_supervisor_and_host_use_default_receiver(self):
        book = ExecutionBook(Path(self.temp.name) / 'wire.sqlite', POLICY,
                             simulation=False, clock=lambda: NOW_LATER)
        self.addCleanup(book.close)
        supervisor = LiveSupervisor(book, source_mode='real')
        self.assertEqual(supervisor.current_context_status['status'], 'missing')
        self.assertEqual(supervisor.current_context_status['factory_calls'], 1)
        self.assertIsInstance(book._current_preflight, MissingCurrentContext)

        other = ExecutionBook(Path(self.temp.name) / 'host.sqlite', POLICY,
                              simulation=False, clock=lambda: NOW_LATER)
        self.addCleanup(other.close)
        bridge = HostExecutionBridge(other, Path(self.temp.name) / 'host')
        self.addCleanup(bridge.close)
        self.assertEqual(bridge.current_context_status['status'], 'missing')
        self.assertEqual(bridge.current_context_status['factory_calls'], 1)
        self.assertIsInstance(other._current_preflight, MissingCurrentContext)

    def test_host_entry_uses_valid_factory_receipt_and_stays_blocked(self):
        book = ExecutionBook(Path(self.temp.name) / 'host-valid.sqlite',
                             POLICY, simulation=True, clock=lambda: NOW_LATER)
        self.addCleanup(book.close)
        factory = CurrentContextFactory(self.registry(),
                                        clock=lambda: NOW_LATER)
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            context = factory(book)
        self.assertIsInstance(context, CurrentEngineeringPreflight)
        book._current_preflight = NoCurrentPreflight()
        plan = {'symbol': 'DEMO', 'quantity': 2, 'entry_ceiling': '40',
                'stop': '39', 'cost_buffer_per_share': '0.05'}
        quote = {'symbol': 'DEMO', 'bid': '39.99', 'ask': '40',
                 'bid_at': NOW_LATER.isoformat(),
                 'ask_at': NOW_LATER.isoformat(),
                 'observed_at': NOW_LATER.isoformat()}
        evidence = {'synthetic': True, 'source_sha256': 'fixture',
                    'engineering_scope_authorized': True,
                    'parameters_user_specified': True,
                    'parameters_valid_until': (NOW_LATER + timedelta(
                        minutes=30)).isoformat()}
        observed = self.compiled['observed_at']
        snap = snapshot(FakeBroker(), observed)
        snap['synthetic'] = True
        snap['human_must_approve_trades'] = False
        book.propose('engineering-host-fixture', plan, quote, snap,
                     purpose='engineering', evidence=evidence)
        book.reconcile(snap)
        journal = BrokerJournal(book.db_path, allow_synthetic=True)
        try:
            journal.ingest(snap)
        finally:
            journal.close()
        book._current_preflight = context
        action = next(a for a in book.actions() if a['kind'] == 'entry')
        bridge = HostExecutionBridge(book, Path(self.temp.name) / 'host-valid')
        self.addCleanup(bridge.close)
        request = {'schema_version': 1, 'binding': bridge.binding,
                   'operation': 'review', 'action_id': action['id'],
                   'requested_at': NOW_LATER.isoformat()}
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            with self.assertRaisesRegex(
                    ValueError, 'CURRENT_ENGINEERING_PREFLIGHT_BLOCKED'):
                bridge._prepare(request, snap, {'DEMO': quote}, True)

    def test_default_host_profile_binds_same_authority_to_factory_preflight_refresher(self):
        root = Path(self.temp.name) / 'default-profile'
        root.mkdir(mode=0o700)
        owner = root / 'owner'
        owner.mkdir(mode=0o700)
        policy = deepcopy(POLICY)
        policy['current_capture'] = {
            'root': str(self.bundle.root),
            'public_manifest': str(self.public.manifest),
            'producer': self.record['producer'], 'retention': 3}
        policy['supervised_engineering'] = {
            'enabled': True, 'owner_root': str(owner),
            'promotion': {'enabled': True}}
        # Move the already-valid registry into the configured capture root and
        # rebind only policy/source fields changed by this test composition.
        self.record['policy_sha256'] = digest(policy)
        self.record['source_fingerprint'] = source_fingerprint()
        registry_path = self.bundle.root / 'registry.json'
        registry_path.write_text(json.dumps(self.record, sort_keys=True))
        os.chmod(registry_path, 0o600)
        book = ExecutionBook(root / 'book.sqlite', policy,
                             simulation=False, clock=lambda: NOW_LATER)
        self.addCleanup(book.close)
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            bridge = HostExecutionBridge(book, root / 'host')
            self.addCleanup(bridge.close)
            self.assertEqual(bridge.current_context_status['status'], 'bound')
        context = book._current_preflight
        authority = book.supervised_engineering_authority
        self.assertIs(context.formal_authority, authority)
        self.assertIs(book.formal_authority, authority)
        self.assertIs(book._current_context_factory.formal_authority, authority)
        self.assertIs(
            book._current_context_refresher.factory.formal_authority, authority)
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            status = book._current_context_refresher.refresh()
            self.assertEqual(status['status'], 'bound')
        self.assertIs(book._current_preflight, context)
        self.assertIs(context.formal_authority, authority)
        self.assertIs(
            book._current_context_refresher.factory.formal_authority, authority)

    def test_real_default_temp_composition_evaluates_then_formally_rejects(self):
        book = ExecutionBook(Path(self.temp.name) / 'real.sqlite', POLICY,
                             simulation=False, clock=lambda: NOW_LATER)
        self.addCleanup(book.close)
        factory = CurrentContextFactory(self.registry(),
                                        clock=lambda: NOW_LATER)
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            context = factory(book)
        book._current_preflight = context
        observed = self.compiled['observed_at']
        snap = snapshot(FakeBroker(), observed)
        snap['human_must_approve_trades'] = False
        book.reconcile(snap)
        journal = BrokerJournal(book.db_path)
        try:
            journal.ingest(snap)
        finally:
            journal.close()
        evidence = {'synthetic': False, 'source_sha256': 'fixture',
                    'engineering_scope_authorized': True,
                    'parameters_user_specified': True,
                    'parameters_valid_until': (NOW_LATER + timedelta(
                        minutes=30)).isoformat()}
        plan = {'symbol': 'DEMO', 'quantity': 2, 'entry_ceiling': '40',
                'stop': '39', 'cost_buffer_per_share': '0.05'}
        quote = {'symbol': 'DEMO', 'bid': '39.99', 'ask': '40',
                 'bid_at': NOW_LATER.isoformat(),
                 'ask_at': NOW_LATER.isoformat(),
                 'observed_at': NOW_LATER.isoformat()}
        trade = {'id': 'engineering-real-factory', 'purpose': 'engineering',
                 'plan': plan, 'phase': 'ENTRY_PLANNED', 'stop': plan['stop'],
                 'initial_stop': plan['stop'], 'quantity': '0',
                 'entry_price': None, 'snapshot_at': observed,
                 'exit_requested': False, 'cancel_for_replace': None,
                 'evidence': evidence, 'preflight_quote': quote}
        state = book.state()
        state['trades'][trade['id']] = trade
        book.db.execute('UPDATE live_state SET payload=? WHERE id=1',
                        (canonical(state),))
        book.db.commit()
        book._queue(trade, 'entry',
                    {'symbol': 'DEMO', 'side': 'buy', 'type': 'limit',
                     'quantity': '2', 'limit_price': '40',
                     'market_hours': 'regular_hours', 'time_in_force': 'gfd'},
                    NOW_LATER, 'TEST_REAL_FACTORY')
        book.db.commit()
        action = next(a for a in book.actions() if a['kind'] == 'entry')
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            decision = book._current_preflight_decision(action=action)
        self.assertFalse(decision['allowed'])
        self.assertIn('FORMAL_ENGINEERING_CHANNEL_UNPROVEN',
                      decision['reasons'])
        self.assertEqual(decision['components']['risk']['status'],
                         'evaluated', decision['components']['risk'])
        self.assertEqual(decision['components']['lineage']['status'],
                         'evaluated', decision['components']['lineage'])
        self.assertTrue(decision['components']['lineage']['blocked'])
        self.assertEqual(decision['components']['control']['status'],
                         'evaluated', decision['components']['control'])
        self.assertFalse(decision['components']['control']['applicable'])
        self.assertIsNotNone(decision.get('receipt'))
        self.assertEqual(
            decision['receipt']['components']['journal_current']['status'],
            'evaluated')
        binding = book.current_preflight_binding(decision, action=action)
        self.assertTrue(binding['bound'], binding['reasons'])
