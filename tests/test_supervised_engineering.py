from copy import deepcopy
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import sqlite3
import tempfile
from unittest import TestCase
from unittest.mock import patch

from martin_agentic.audit import digest
from martin_agentic.execution import ExecutionBook
from martin_agentic.native_operations import NativeOperations
from martin_agentic.qualification import source_fingerprint
from martin_agentic.supervised_engineering import (
    AUTHORITY_BASIS, SupervisedEngineeringAuthority,
    SupervisedEngineeringError, build_permit, order_from_plan,
    parse_request)
from test_broker import snapshot

AT = '2026-10-16T14:00:00+00:00'
NOW = datetime.fromisoformat(AT)


def policy():
    return json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())


def plan():
    return {'symbol': 'DEMO', 'quantity': 2, 'entry_ceiling': '40',
            'stop': '39', 'cost_buffer_per_share': '0.05'}


def quote():
    return {'symbol': 'DEMO', 'bid': '39.99', 'ask': '40',
            'bid_at': AT, 'ask_at': AT, 'observed_at': AT}


def risk_terms(p):
    return {
        'account_capital_usd': str(p['account_capital_usd']),
        'max_planned_loss_per_trade_usd':
            str(p['max_planned_loss_per_trade_usd']),
        'max_position_notional_usd':
            str(p['max_position_notional_usd']),
        'stop_new_entries_at_daily_equity_loss_usd':
            str(p['stop_new_entries_at_daily_equity_loss_usd']),
        'project_max_drawdown_fraction':
            str(p['project_max_drawdown_fraction']),
        'leverage_allowed': False,
        'overnight_allowed': False,
    }


def assessment(p, account_key, now=NOW, *, purpose='engineering',
               valid_seconds=30):
    body = {
        'schema_version': 1,
        'kind': 'current_account_facts_assessment_v1',
        'status': 'facts_ready',
        'assessed_at': now.isoformat(),
        'observed_at': now.isoformat(),
        'observation_valid_from': now.isoformat(),
        'observation_valid_until': (now + timedelta(
            seconds=valid_seconds)).isoformat(),
        'purpose': purpose,
        'binding': {
            'kind': 'current_account_facts_binding_v1',
            'account_key': account_key,
            'account_suffix': '0000',
            'policy_sha256': digest(p),
            'source_fingerprint': source_fingerprint(),
            'capture_generation': None,
            'expected_capture_generation': None,
            'purpose': purpose,
        },
        'account': {'key_hash': account_key, 'mask': '••••0000',
                    'suffix': '0000'},
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
    body['assessment_id'] = digest(body)
    return body


def request(p, account_key, *, synthetic=True, signal_id='engineering-1',
            valid_seconds=120, **overrides):
    value = {
        'schema_version': 1,
        'kind': 'supervised_engineering_user_request_v1',
        'request_id': 'request-1',
        'signal_id': signal_id,
        'account_key': account_key,
        'account_suffix': '0000',
        'purpose': 'engineering',
        'order': order_from_plan(plan()),
        'risk_terms': risk_terms(p),
        'authority_basis': {'kind': AUTHORITY_BASIS,
                            'ref': 'engineering-small-order-grant'},
        'source': {
            'source_fingerprint': source_fingerprint(),
            'policy_sha256': digest(p),
            'capture_generation': None,
        },
        'requested_at': (NOW - timedelta(seconds=10)).isoformat(),
        'not_before': (NOW - timedelta(seconds=10)).isoformat(),
        'valid_until': (NOW + timedelta(seconds=valid_seconds)).isoformat(),
        'synthetic': synthetic,
    }
    value.update(overrides)
    return value


class FakePreflight:
    def __init__(self, facts):
        self.facts = facts

    def current_account_assessment(self, book, *, purpose=None):
        return deepcopy(self.facts)

    def validate_account_assessment(self, assessment, *, book=None,
                                    purpose=None):
        return True, None

    def check_proposal(self, book, snapshot, quote, plan, *, purpose, evidence,
                       candidate_action_id=None):
        return {'allowed': True, 'reasons': [],
                'reason': 'SYNTHETIC_SUPERVISED_FIXTURE',
                'entry_authorized': False}

    def check_claim(self, book, action):
        return {'allowed': True, 'reasons': [],
                'reason': 'SYNTHETIC_SUPERVISED_FIXTURE',
                'entry_authorized': False}


class SupervisedBook(ExecutionBook):
    def __init__(self, *args, preflight=None, **kwargs):
        super().__init__(*args, current_preflight=preflight, **kwargs)

    def _preflight_binding_ok(self, decision, **kwargs):
        return True


class OwnerStore:
    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        os.chmod(self.root, 0o700)

    def write(self, name, value):
        path = self.root / name
        with path.open('x') as handle:
            handle.write(json.dumps(value, sort_keys=True))
        os.chmod(path, 0o600)

    def replace(self, name, value):
        path = self.root / name
        if path.exists():
            path.unlink()
        self.write(name, value)


class SupervisedEngineeringTests(TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.policy = policy()
        self.plan = plan()
        self.quote = quote()
        self.snapshot = snapshot()
        self.snapshot['synthetic'] = True
        self.account_key = self.snapshot['account_key']
        self.facts = assessment(self.policy, self.account_key)
        self.owner = OwnerStore(self.root / 'owner')
        self.owner.write('supervised-engineering-request.json',
                         request(self.policy, self.account_key))
        self.authority = SupervisedEngineeringAuthority(
            self.owner.root, clock=lambda: NOW, allow_synthetic=True)
        self.book = SupervisedBook(
            self.root / 'book.sqlite', self.policy, simulation=True,
            clock=lambda: NOW, preflight=FakePreflight(self.facts),
            supervised_engineering_authority=self.authority)

    def tearDown(self):
        self.book.close()

    def evidence(self, *, synthetic=True, **extra):
        value = {
            'synthetic': synthetic,
            'source_sha256': 'fixture',
            'engineering_scope_authorized': True,
            'parameters_user_specified': True,
            'parameters_valid_until': (NOW + timedelta(
                minutes=30)).isoformat(),
        }
        value.update(extra)
        return value

    def intent(self):
        return self.book.record_supervised_engineering_intent(
            'engineering-1', self.plan, self.quote, self.snapshot,
            evidence=self.evidence())

    def review(self, action_id):
        ops = NativeOperations(self.book)
        packet = ops.begin(action_id, 'review', self.snapshot,
                           quote=self.quote)
        response = {'data': {
            **packet['parameters'],
            'order_checks': {},
            'customer_approval_required': False,
            'market_data_disclosure': 'Exact disclosure text',
            'quote_data': {},
        }}
        result = ops.record(packet['operation_id'], response)
        return packet, result

    def permit(self, action_id, packet, review):
        action = next(a for a in self.book.actions() if a['id'] == action_id)
        payload = json.loads(action['payload'])
        return build_permit(
            request(self.policy, self.account_key), {
                'operation_id': packet['operation_id'],
                'action_id': action_id,
                'parameters': packet['parameters'],
                'result': review,
            }, self.facts, account_key=self.account_key,
            account_suffix='0000', policy=self.policy,
            fingerprint=source_fingerprint(), action_id=action_id,
            ref_id=payload['ref_id'], now=NOW, one_shot_id='permit-1',
            valid_until=(NOW + timedelta(seconds=20)).isoformat())

    def reviewed_intent(self):
        result = self.intent()
        action_id = result['action_id']
        packet, review = self.review(action_id)
        return action_id, packet, review

    def write_permit(self, value):
        self.owner.write('supervised-engineering-permit.json', value)

    def consumption_count(self):
        exists = self.book.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND "
            "name='formal_authorization_consumptions'").fetchone()
        return (self.book.db.execute(
            'SELECT count(*) FROM formal_authorization_consumptions'
        ).fetchone()[0] if exists else 0)

    def test_synthetic_intent_review_permit_promote_claim(self):
        action_id, packet, review = self.reviewed_intent()
        action = next(a for a in self.book.actions() if a['id'] == action_id)
        self.assertEqual(action['state'], 'WAITING_USER')
        self.assertIsNone(self.book.claim())
        permit = self.permit(action_id, packet, review)
        self.write_permit(permit)
        result = self.book.promote_supervised_entry(action_id)
        self.assertEqual(result['state'], 'READY')
        self.assertIs(result['entry_authorized'], False)
        self.assertEqual(next(a for a in self.book.actions()
                              if a['id'] == action_id)['state'], 'READY')
        consumed = self.book.db.execute(
            'SELECT one_shot_id,action_id FROM formal_authorization_consumptions'
        ).fetchall()
        self.assertEqual(consumed, [('permit-1', action_id)])
        claimed = self.book.claim()
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed['kind'], 'entry')

    def test_owner_dry_assess_is_read_only_and_non_activating(self):
        action_id, packet, review = self.reviewed_intent()
        self.write_permit(self.permit(action_id, packet, review))
        self.book.refresh_current_account_assessment(purpose='engineering')
        before_actions = self.book.actions()
        before_audit = self.book.db.execute(
            'SELECT count(*) FROM audit_events').fetchone()[0]
        before_consumption = self.consumption_count()
        before_risk = digest(self.book.risk_guard.state())
        decision = self.book.dry_assess_supervised_engineering(action_id)
        self.assertEqual(decision['decision_scope'], 'DRY_ASSESSMENT')
        self.assertTrue(decision['allowed'], decision)
        self.assertFalse(decision['execution_authorized'])
        self.assertFalse(decision['entry_authorized'])
        self.assertEqual(self.book.actions(), before_actions)
        self.assertEqual(self.book.db.execute(
            'SELECT count(*) FROM audit_events').fetchone()[0], before_audit)
        self.assertEqual(self.consumption_count(), before_consumption)
        self.assertEqual(digest(self.book.risk_guard.state()), before_risk)
        with self.assertRaisesRegex(ValueError, 'SUPERVISED_ENGINEERING'):
            NativeOperations(self.book).begin(
                action_id, 'place', self.snapshot, quote=self.quote)

    def test_intent_is_not_claimable_or_placeable_before_promotion(self):
        action_id, _, _ = self.reviewed_intent()
        self.assertIsNone(self.book.claim())
        with self.assertRaisesRegex(ValueError,
                                    'SUPERVISED_ENGINEERING_PROMOTION_REQUIRED'):
            NativeOperations(self.book).begin(
                action_id, 'place', self.snapshot, quote=self.quote)

    def test_default_real_book_has_no_permit_verifier(self):
        self.book.close()
        owner = OwnerStore(self.root / 'owner-real')
        owner.write('supervised-engineering-request.json',
                    request(self.policy, self.account_key, synthetic=False,
                            signal_id='engineering-real'))
        real_snapshot = snapshot()
        real_snapshot['synthetic'] = False
        real = SupervisedBook(
            self.root / 'real.sqlite', self.policy, simulation=False,
            clock=lambda: NOW, preflight=FakePreflight(
                assessment(self.policy, self.account_key)),
            supervised_engineering_authority=SupervisedEngineeringAuthority(
                owner.root, clock=lambda: NOW, allow_synthetic=False))
        try:
            real.record_supervised_engineering_intent(
                'engineering-real', self.plan, self.quote, real_snapshot,
                evidence=self.evidence(synthetic=False))
            action = real.actions()[0]
            packet, review = self.review_for(real, action['id'],
                                             real_snapshot)
            # A real permit record may be validated read-only, but promotion
            # must still block because no accepted real-source verifier exists.
            real_permit = build_permit(
                request(self.policy, self.account_key, synthetic=False,
                        signal_id='engineering-real'), {
                    'operation_id': packet['operation_id'],
                    'action_id': action['id'],
                    'parameters': packet['parameters'],
                    'result': review,
                }, assessment(self.policy, self.account_key),
                account_key=self.account_key, account_suffix='0000',
                policy=self.policy, fingerprint=source_fingerprint(),
                action_id=action['id'],
                ref_id=json.loads(action['payload'])['ref_id'], now=NOW,
                one_shot_id='real-permit',
                valid_until=(NOW + timedelta(seconds=20)).isoformat())
            owner.write('supervised-engineering-permit.json', real_permit)
            with self.assertRaisesRegex(
                    ValueError,
                    'SUPERVISED_ENGINEERING_REAL_SOURCE_VERIFIER_UNAVAILABLE'):
                real.promote_supervised_entry(action['id'])
            self.assertEqual(real.actions()[0]['state'], 'WAITING_USER')
        finally:
            real.close()

    def review_for(self, book, action_id, snap):
        ops = NativeOperations(book)
        packet = ops.begin(action_id, 'review', snap, quote=self.quote)
        response = {'data': {
            **packet['parameters'], 'order_checks': {},
            'customer_approval_required': False,
            'market_data_disclosure': 'Exact disclosure text',
            'quote_data': {},
        }}
        return packet, ops.record(packet['operation_id'], response)

    def test_permit_account_order_ref_source_and_review_mismatch(self):
        action_id, packet, review = self.reviewed_intent()
        base = self.permit(action_id, packet, review)
        mutations = [
            ('account_key', '0' * 64,
             'SUPERVISED_ENGINEERING_ACCOUNT_MISMATCH'),
            ('action_id', 'live-wrong',
             'SUPERVISED_ENGINEERING_ACTION_MISMATCH'),
            ('ref_id', 'wrong-ref',
             'SUPERVISED_ENGINEERING_ACTION_MISMATCH'),
            ('order', {**base['order'], 'quantity': '3'},
             'SUPERVISED_ENGINEERING_ORDER_MISMATCH'),
            ('request_sha256', '0' * 64,
             'SUPERVISED_ENGINEERING_REQUEST_MISMATCH'),
            ('formal', {**base['formal'], 'review_response_hash': '0' * 64},
             'SUPERVISED_ENGINEERING_REVIEW_CHANGED'),
            ('revoked_at', '2026-10-16T14:00:00+00:00',
             'SUPERVISED_ENGINEERING_PERMIT_REVOKED'),
        ]
        for key, value, expected in mutations:
            with self.subTest(key=key):
                changed = deepcopy(base)
                changed[key] = value
                changed['permit_id'] = digest(
                    {**changed, 'permit_id': None})
                self.owner.replace('supervised-engineering-permit.json',
                                   changed)
                with self.assertRaisesRegex(ValueError, expected):
                    self.book.promote_supervised_entry(action_id)

    def test_request_rejects_bad_numbers_and_window(self):
        bad = request(self.policy, self.account_key)
        bad['order'] = {**bad['order'], 'quantity': '-1'}
        with self.assertRaisesRegex(
                ValueError, 'SUPERVISED_ENGINEERING_QUANTITY_INVALID'):
            parse_request(bad, account_key=self.account_key,
                          account_suffix='0000', policy=self.policy,
                          now=NOW, fingerprint=source_fingerprint())
        bad = request(self.policy, self.account_key)
        bad['order'] = {**bad['order'], 'quantity': '1e999999'}
        with self.assertRaisesRegex(
                ValueError,
                'SUPERVISED_ENGINEERING_(QUANTITY_INVALID|ORDER_TERMS)'):
            parse_request(bad, account_key=self.account_key,
                          account_suffix='0000', policy=self.policy,
                          now=NOW, fingerprint=source_fingerprint())
        bad = request(self.policy, self.account_key)
        bad['risk_terms'] = {**bad['risk_terms'],
                             'max_position_notional_usd': '101'}
        with self.assertRaisesRegex(
                ValueError, 'SUPERVISED_ENGINEERING_RISK_MISMATCH'):
            parse_request(bad, account_key=self.account_key,
                          account_suffix='0000', policy=self.policy,
                          now=NOW, fingerprint=source_fingerprint())

    def test_stale_review_and_replay_and_transaction_rollback(self):
        action_id, packet, review = self.reviewed_intent()
        permit = self.permit(action_id, packet, review)
        self.write_permit(permit)
        later = NOW + timedelta(seconds=31)
        self.book.clock = lambda: later
        self.authority.clock = lambda: later
        with self.assertRaisesRegex(
                ValueError, 'SUPERVISED_ENGINEERING_(FACTS_STALE|PERMIT_WINDOW)'):
            self.book.promote_supervised_entry(action_id)
        self.assertEqual(next(a for a in self.book.actions()
                              if a['id'] == action_id)['state'],
                         'WAITING_USER')

        self.book.clock = lambda: NOW
        self.authority.clock = lambda: NOW
        component = self.authority.assess(
            book=self.book, mode='claim',
            action=next(a for a in self.book.actions()
                        if a['id'] == action_id),
            purpose='engineering')
        self.assertTrue(component['allowed'])
        self.book._begin()
        try:
            self.authority.consume(self.book, component['component'],
                                   action_id, NOW)
            self.book.db.commit()
        except BaseException:
            self.book.db.rollback()
            raise
        self.book._begin()
        try:
            changed = deepcopy(component['component'])
            changed['binding'] = '0' * 64
            with self.assertRaisesRegex(
                    ValueError, 'SUPERVISED_ENGINEERING_PERMIT_REPLAYED'):
                self.authority.consume(self.book, changed, action_id, NOW)
        finally:
            self.book.db.rollback()

    def test_audit_failure_rolls_back_consumption_and_state(self):
        action_id, packet, review = self.reviewed_intent()
        self.write_permit(self.permit(action_id, packet, review))
        with patch.object(self.book.audit, 'append_in_transaction',
                          side_effect=ValueError('AUDIT_TEST_FAILURE')):
            with self.assertRaisesRegex(ValueError, 'AUDIT_TEST_FAILURE'):
                self.book.promote_supervised_entry(action_id)
        self.assertEqual(next(a for a in self.book.actions()
                              if a['id'] == action_id)['state'],
                         'WAITING_USER')
        exists = self.book.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND "
            "name='formal_authorization_consumptions'").fetchone()
        count = (self.book.db.execute(
            'SELECT count(*) FROM formal_authorization_consumptions'
        ).fetchone()[0] if exists else 0)
        self.assertEqual(count, 0)

    def test_caller_cannot_supply_permit_through_evidence(self):
        result = self.book.record_supervised_engineering_intent(
            'engineering-1', self.plan, self.quote, self.snapshot,
            evidence=self.evidence(
                supervised_permit={'allowed': True, 'synthetic': True}))
        self.assertFalse(result['entry_authorized'])
        self.assertEqual(next(a for a in self.book.actions()
                              if a['id'] == result['action_id'])['state'],
                         'REVIEW_PENDING')
        with self.assertRaisesRegex(
                ValueError, 'SUPERVISED_ENGINEERING_PERMIT_BLOCKED'):
            self.book.promote_supervised_entry(result['action_id'])

    def test_source_policy_capture_time_and_tamper_mismatches(self):
        action_id, packet, review = self.reviewed_intent()
        base = self.permit(action_id, packet, review)
        cases = []

        changed = deepcopy(base)
        changed['source'] = {**changed['source'],
                             'source_fingerprint': '0' * 64}
        changed['permit_id'] = digest({**changed, 'permit_id': None})
        cases.append((changed, 'SUPERVISED_ENGINEERING_SOURCE_MISMATCH'))

        changed = deepcopy(base)
        changed['source'] = {**changed['source'], 'policy_sha256': '0' * 64}
        changed['permit_id'] = digest({**changed, 'permit_id': None})
        cases.append((changed, 'SUPERVISED_ENGINEERING_POLICY_MISMATCH'))

        changed = deepcopy(base)
        changed['facts'] = {**changed['facts'],
                            'capture_generation': 'replaced'}
        changed['permit_id'] = digest({**changed, 'permit_id': None})
        cases.append((changed, 'SUPERVISED_ENGINEERING_CAPTURE_CHANGED'))

        changed = deepcopy(base)
        changed['valid_until'] = (NOW - timedelta(seconds=1)).isoformat()
        changed['permit_id'] = digest({**changed, 'permit_id': None})
        cases.append((changed, 'SUPERVISED_ENGINEERING_PERMIT_WINDOW'))

        changed = deepcopy(base)
        changed['formal'] = {**changed['formal'], 'order_checks_sha256': '0' * 64}
        changed['permit_id'] = digest({**changed, 'permit_id': None})
        cases.append((changed, 'SUPERVISED_ENGINEERING_REVIEW_CHANGED'))

        changed = deepcopy(base)
        changed['permit_id'] = '0' * 64
        cases.append((changed, 'SUPERVISED_ENGINEERING_PERMIT_TAMPERED'))

        for value, expected in cases:
            with self.subTest(expected=expected):
                self.owner.replace('supervised-engineering-permit.json', value)
                with self.assertRaisesRegex(ValueError, expected):
                    self.book.promote_supervised_entry(action_id)

    def test_future_window_and_stale_facts(self):
        value = request(self.policy, self.account_key)
        value['not_before'] = (NOW + timedelta(seconds=30)).isoformat()
        with self.assertRaisesRegex(
                ValueError, 'SUPERVISED_ENGINEERING_REQUEST_WINDOW'):
            parse_request(value, account_key=self.account_key,
                          account_suffix='0000', policy=self.policy,
                          now=NOW, fingerprint=source_fingerprint())
        action_id, packet, review = self.reviewed_intent()
        self.write_permit(self.permit(action_id, packet, review))
        later = NOW + timedelta(seconds=31)
        self.book.clock = lambda: later
        self.authority.clock = lambda: later
        with self.assertRaisesRegex(
                ValueError,
                'SUPERVISED_ENGINEERING_(FACTS_STALE|PERMIT_WINDOW)'):
            self.book.promote_supervised_entry(action_id)

    def test_issuer_rejects_old_review(self):
        action_id, packet, review = self.reviewed_intent()
        old = deepcopy(review)
        old['recorded_at'] = (NOW - timedelta(seconds=31)).isoformat()
        with self.assertRaisesRegex(
                ValueError, 'SUPERVISED_ENGINEERING_REVIEW_STALE'):
            self.permit(action_id, packet, old)

    def test_order_price_invalid_and_permit_tamper(self):
        for price in ('-1', 'NaN', 'Infinity', '0'):
            with self.subTest(price=price):
                value = request(self.policy, self.account_key)
                value['order'] = {**value['order'], 'limit_price': price}
                with self.assertRaisesRegex(
                        ValueError, 'SUPERVISED_ENGINEERING_LIMIT_INVALID'):
                    parse_request(value, account_key=self.account_key,
                                  account_suffix='0000',
                                  policy=self.policy, now=NOW,
                                  fingerprint=source_fingerprint())

    def test_recheck_accepts_consumed_synthetic_permit(self):
        action_id, packet, review = self.reviewed_intent()
        self.write_permit(self.permit(action_id, packet, review))
        self.book.refresh_current_account_assessment(purpose='engineering')
        action = next(a for a in self.book.actions() if a['id'] == action_id)
        decision = self.authority.assess(
            book=self.book, mode='claim', action=action,
            purpose='engineering')
        self.assertTrue(decision['allowed'])
        self.book._begin()
        try:
            self.authority.consume(self.book, decision['component'],
                                   action_id, NOW)
            self.book.db.commit()
        except BaseException:
            self.book.db.rollback()
            raise
        self.assertEqual(self.authority.recheck_receipt(
            decision['component'], book=self.book, action=action,
            purpose='engineering'), [])

    def test_intent_rejects_excess_notional_and_planned_loss(self):
        good = request(self.policy, self.account_key)
        bad = deepcopy(good)
        bad['order'] = {**bad['order'], 'quantity': '3'}
        self.owner.replace('supervised-engineering-request.json', bad)
        plan3 = {**self.plan, 'quantity': 3}
        with self.assertRaisesRegex(
                ValueError, 'SUPERVISED_ENGINEERING_ORDER_EXCEEDS_NOTIONAL'):
            self.book.record_supervised_engineering_intent(
                'engineering-1', plan3, self.quote, self.snapshot,
                evidence=self.evidence())
        self.owner.replace('supervised-engineering-request.json', good)
        plan_loss = {**self.plan, 'stop': '38'}
        with self.assertRaisesRegex(
                ValueError,
                'SUPERVISED_ENGINEERING_ORDER_EXCEEDS_PLANNED_LOSS'):
            self.book.record_supervised_engineering_intent(
                'engineering-1', plan_loss, self.quote, self.snapshot,
                evidence=self.evidence())
        self.owner.replace('supervised-engineering-request.json', good)

    def test_double_promotion_consumes_once(self):
        action_id, packet, review = self.reviewed_intent()
        self.write_permit(self.permit(action_id, packet, review))
        self.book.promote_supervised_entry(action_id)
        with self.assertRaisesRegex(
                ValueError, 'SUPERVISED_ENGINEERING_PROMOTION_STATE'):
            self.book.promote_supervised_entry(action_id)
        count = self.book.db.execute(
            'SELECT count(*) FROM formal_authorization_consumptions'
        ).fetchone()[0]
        self.assertEqual(count, 1)

    def test_promote_rejects_permit_expiry_before_transaction(self):
        action_id, packet, review = self.reviewed_intent()
        self.write_permit(self.permit(action_id, packet, review))
        holder = [NOW]
        self.book.clock = lambda: holder[0]
        self.authority.clock = lambda: holder[0]
        original = self.book._begin

        def begin_after_expiry():
            holder[0] = NOW + timedelta(seconds=31)
            original()

        self.book._begin = begin_after_expiry
        with self.assertRaisesRegex(
                ValueError, 'SUPERVISED_ENGINEERING_(FACTS_STALE|PERMIT_WINDOW)'):
            self.book.promote_supervised_entry(action_id)
        self.assertEqual(next(a for a in self.book.actions()
                              if a['id'] == action_id)['state'],
                         'WAITING_USER')
        self.assertEqual(self.consumption_count(), 0)

    def test_promote_rejects_revocation_during_transaction(self):
        action_id, packet, review = self.reviewed_intent()
        permit = self.permit(action_id, packet, review)
        self.write_permit(permit)
        revoked = deepcopy(permit)
        revoked['revoked_at'] = NOW.isoformat()
        revoked['permit_id'] = digest({**revoked, 'permit_id': None})
        original = self.book._begin

        def begin_after_revoke():
            self.owner.replace('supervised-engineering-permit.json', revoked)
            original()

        self.book._begin = begin_after_revoke
        with self.assertRaisesRegex(
                ValueError,
                'SUPERVISED_ENGINEERING_(OWNER_SOURCE_CHANGED|PERMIT_REVOKED)'):
            self.book.promote_supervised_entry(action_id)
        self.assertEqual(next(a for a in self.book.actions()
                              if a['id'] == action_id)['state'],
                         'WAITING_USER')
        self.assertEqual(self.consumption_count(), 0)

    def test_consume_without_finalization_rechecks_and_rejects_after_expiry(self):
        action_id, packet, review = self.reviewed_intent()
        self.write_permit(self.permit(action_id, packet, review))
        self.book.refresh_current_account_assessment(purpose='engineering')
        action = next(a for a in self.book.actions() if a['id'] == action_id)
        decision = self.authority.assess(
            book=self.book, mode='claim', action=action,
            purpose='engineering')
        self.assertTrue(decision['allowed'])
        self.book._begin()
        try:
            with self.assertRaisesRegex(
                    ValueError, 'SUPERVISED_ENGINEERING_(FACTS_STALE|PERMIT_WINDOW)'):
                self.authority.consume(
                    self.book, decision['component'], action_id,
                    NOW + timedelta(seconds=31))
        finally:
            self.book.db.rollback()

    def test_promote_rejects_source_pin_change_during_transaction(self):
        action_id, packet, review = self.reviewed_intent()
        self.write_permit(self.permit(action_id, packet, review))
        import martin_agentic.qualification as qualification
        original_begin = self.book._begin
        original_pin = qualification.source_pin

        def begin_after_source_change():
            qualification.source_pin = lambda: {
                'status': 'bounded', 'count': 999, 'digest': '0' * 64,
                'truncated': False}
            original_begin()

        self.book._begin = begin_after_source_change
        try:
            with self.assertRaisesRegex(
                    ValueError, 'SUPERVISED_ENGINEERING_SOURCE_CHANGED'):
                self.book.promote_supervised_entry(action_id)
        finally:
            qualification.source_pin = original_pin
        self.assertEqual(next(a for a in self.book.actions()
                              if a['id'] == action_id)['state'],
                         'WAITING_USER')
        self.assertEqual(self.consumption_count(), 0)

    def test_second_connection_cannot_reconsume_or_repromote(self):
        action_id, packet, review = self.reviewed_intent()
        self.write_permit(self.permit(action_id, packet, review))
        self.book.promote_supervised_entry(action_id)
        second = SupervisedBook(
            self.root / 'book.sqlite', self.policy, simulation=True,
            clock=lambda: NOW, preflight=FakePreflight(self.facts),
            supervised_engineering_authority=self.authority)
        try:
            with self.assertRaisesRegex(
                    ValueError, 'SUPERVISED_ENGINEERING_PROMOTION_STATE'):
                second.promote_supervised_entry(action_id)
            self.assertEqual(second.db.execute(
                'SELECT count(*) FROM formal_authorization_consumptions'
            ).fetchone()[0], 1)
        finally:
            second.close()

    def _finalized_component(self, action_id, packet, review):
        self.write_permit(self.permit(action_id, packet, review))
        self.book.refresh_current_account_assessment(purpose='engineering')
        action = next(a for a in self.book.actions() if a['id'] == action_id)
        decision = self.authority.assess(
            book=self.book, mode='claim', action=action,
            purpose='engineering')
        self.assertTrue(decision['allowed'])
        return action, decision['component']

    def test_consume_rejects_clock_rollback_after_finalize(self):
        action_id, packet, review = self.reviewed_intent()
        holder = [NOW]
        self.authority.clock = lambda: holder[0]
        action, component = self._finalized_component(
            action_id, packet, review)
        self.book._begin()
        try:
            finalized = self.authority.finalize_promotion(
                self.book, component, action_id, NOW)
            holder[0] = NOW - timedelta(seconds=60)
            with self.assertRaisesRegex(
                    ValueError, 'SUPERVISED_ENGINEERING_PERMIT_WINDOW'):
                self.authority.consume(
                    self.book, finalized['component'], action_id, NOW,
                    finalized=finalized)
        finally:
            self.book.db.rollback()
        self.assertEqual(next(a for a in self.book.actions()
                              if a['id'] == action_id)['state'],
                         'WAITING_USER')
        self.assertEqual(self.consumption_count(), 0)

    def test_consume_uses_current_owner_clock_not_caller_now(self):
        action_id, packet, review = self.reviewed_intent()
        holder = [NOW]
        self.authority.clock = lambda: holder[0]
        action, component = self._finalized_component(
            action_id, packet, review)
        self.book._begin()
        try:
            finalized = self.authority.finalize_promotion(
                self.book, component, action_id, NOW)
            holder[0] = NOW + timedelta(seconds=31)
            with self.assertRaisesRegex(
                    ValueError, 'SUPERVISED_ENGINEERING_PERMIT_WINDOW'):
                self.authority.consume(
                    self.book, finalized['component'], action_id, NOW,
                    finalized=finalized)
        finally:
            self.book.db.rollback()
        self.assertEqual(self.consumption_count(), 0)

    def test_consume_rejects_future_or_invalid_finalization(self):
        action_id, packet, review = self.reviewed_intent()
        holder = [NOW]
        self.authority.clock = lambda: holder[0]
        _, component = self._finalized_component(action_id, packet, review)
        self.book._begin()
        try:
            finalized = self.authority.finalize_promotion(
                self.book, component, action_id, NOW)
            future = deepcopy(finalized)
            future['finalized_at'] = NOW + timedelta(seconds=60)
            with self.assertRaisesRegex(
                    ValueError, 'SUPERVISED_ENGINEERING_TIME_REVERSED'):
                self.authority.consume(
                    self.book, future['component'], action_id, NOW,
                    finalized=future)
            naive = deepcopy(finalized)
            naive['not_before'] = datetime(2026, 10, 16, 14, 0, 0)
            with self.assertRaisesRegex(
                    ValueError, 'SUPERVISED_ENGINEERING_FINALIZATION_INVALID'):
                self.authority.consume(
                    self.book, naive['component'], action_id, NOW,
                    finalized=naive)
        finally:
            self.book.db.rollback()

    def test_final_window_failure_after_promotion_audit_rolls_back(self):
        action_id, packet, review = self.reviewed_intent()
        self.write_permit(self.permit(action_id, packet, review))
        holder = [NOW]
        self.book.clock = lambda: holder[0]
        self.authority.clock = lambda: holder[0]
        original = self.book.audit.append_in_transaction

        def slow_promotion_audit(event):
            result = original(event)
            if str(event.get('event_id', '')).endswith('-promoted'):
                holder[0] = NOW + timedelta(seconds=31)
            return result

        with patch.object(self.book.audit, 'append_in_transaction',
                          side_effect=slow_promotion_audit):
            with self.assertRaisesRegex(
                    ValueError, 'SUPERVISED_ENGINEERING_PERMIT_WINDOW'):
                self.book.promote_supervised_entry(action_id)
        self.assertEqual(next(a for a in self.book.actions()
                              if a['id'] == action_id)['state'],
                         'WAITING_USER')
        self.assertEqual(self.consumption_count(), 0)

    def test_host_cli_intent_review_promote_end_to_end(self):
        import io
        from contextlib import redirect_stdout
        from martin_agentic.cli import main as cli_main
        from martin_agentic.host_execution import HostExecutionBridge
        from martin_agentic.host_mailbox import (
            read_private_json, submit_receipt)
        state = self.book.state()
        state['account_key'] = self.account_key
        self.book._save(state)
        self.book.db.commit()
        hostdir = self.root / 'host'
        bridge = HostExecutionBridge(self.book, hostdir)
        proposal = self.root / 'proposal.json'
        proposal.write_text(json.dumps({
            'signal_id': 'engineering-1', 'plan': self.plan,
            'evidence': self.evidence()}))
        with patch('martin_agentic.host_mailbox._now',
                   return_value=NOW.isoformat()):
            with redirect_stdout(io.StringIO()):
                rc = cli_main(['host-execution', 'engineering-intent',
                               '--directory', str(hostdir),
                               '--proposal', str(proposal)])
        self.assertEqual(rc, 0)
        out = bridge(snapshot=self.snapshot, quotes={'DEMO': self.quote},
                     allow_entries=False)
        self.assertEqual(out['status'], 'HOST_ENGINEERING_INTENT_RECORDED',
                         out)
        action_id = out['action_id']
        self.assertEqual(out['state'], 'REVIEW_PENDING')
        self.assertFalse(out['claimable'])
        self.assertFalse(out['placeable'])
        manifest = read_private_json(hostdir / 'manifest.json')
        item = next(a for a in manifest['actions'] if a['id'] == action_id)
        self.assertEqual(item['status'], 'REVIEW_INTENT_RECORDED')
        self.assertFalse(item['claimable'])
        self.assertFalse(item['placeable'])
        # Review is read-only and can be prepared directly from REVIEW_PENDING.
        from martin_agentic.host_mailbox import submit_request
        review_request = submit_request(hostdir, action_id, 'review',
                                        clock=lambda: NOW)
        out = bridge(snapshot=self.snapshot, quotes={'DEMO': self.quote},
                     allow_entries=False)
        self.assertEqual(out['status'], 'HOST_PACKET_PREPARED', out)
        packet = read_private_json(
            hostdir / 'packets' / (review_request['request_id'] + '.json'))['packet']
        response = {'data': {
            **packet['parameters'], 'order_checks': {},
            'customer_approval_required': False,
            'market_data_disclosure': 'Exact disclosure text',
            'quote_data': {},
        }}
        submit_receipt(hostdir, review_request['request_id'], response,
                       clock=lambda: NOW)
        out = bridge(allow_dispatch=False)
        self.assertEqual(out['status'], 'HOST_RECEIPT_IMPORTED', out)
        row = self.book.db.execute(
            "SELECT result FROM native_operation_records WHERE action_id=? "
            "AND operation='review' ORDER BY rowid DESC LIMIT 1",
            (action_id,)).fetchone()
        review = json.loads(row[0])
        permit = self.permit(action_id, packet, review)
        self.write_permit(permit)
        with patch('martin_agentic.host_mailbox._now',
                   return_value=NOW.isoformat()):
            with redirect_stdout(io.StringIO()):
                rc = cli_main(['host-execution', 'engineering-promote',
                               '--directory', str(hostdir),
                               '--action-id', action_id,
                               '--permit-id', permit['permit_id']])
        self.assertEqual(rc, 0)
        out = bridge(snapshot=self.snapshot, quotes={'DEMO': self.quote},
                     allow_entries=True)
        self.assertEqual(out['status'], 'HOST_ENGINEERING_PROMOTED', out)
        self.assertEqual(out['state'], 'READY')
        self.assertFalse(out['entry_authorized'])
        self.assertEqual(next(a for a in self.book.actions()
                              if a['id'] == action_id)['state'], 'READY')
        bridge.close()

    def _host_bridge(self, name='host'):
        state = self.book.state()
        state['account_key'] = self.account_key
        self.book._save(state)
        self.book.db.commit()
        from martin_agentic.host_execution import HostExecutionBridge
        return HostExecutionBridge(self.book, self.root / name)

    def test_host_prepare_reads_trusted_entry_purpose_and_rejects_unknown(self):
        action_id, packet, review = self.reviewed_intent()
        bridge = self._host_bridge('host-purpose')
        try:
            request = {'schema_version': 1, 'binding': bridge.binding,
                       'operation': 'review', 'action_id': action_id,
                       'requested_at': NOW.isoformat()}
            captured = {}
            original = bridge.book.entry_authority.require_entry

            def capture(**kwargs):
                captured.update(kwargs)
                raise ValueError('PURPOSE_CAPTURED')

            with patch.object(bridge.book.entry_authority, 'require_entry',
                              side_effect=capture):
                with self.assertRaisesRegex(ValueError, 'PURPOSE_CAPTURED'):
                    bridge._prepare(request, self.snapshot,
                                    {'DEMO': self.quote}, True)
            self.assertEqual(captured.get('purpose'), 'engineering')
            state = self.book.state()
            trade = state['trades'][next(
                a for a in self.book.actions()
                if a['id'] == action_id)['trade_id']]
            trade['purpose'] = 'UNKNOWN'
            self.book._save(state)
            self.book.db.commit()
            with self.assertRaisesRegex(ValueError,
                                        'HOST_ENTRY_PURPOSE_INVALID'):
                bridge._prepare(request, self.snapshot,
                                {'DEMO': self.quote}, True)
        finally:
            bridge.close()

    def test_host_intent_writes_owner_receipt_idempotently(self):
        from martin_agentic.host_execution import HostExecutionBridge
        from martin_agentic.host_mailbox import submit_engineering_intent
        state = self.book.state()
        state['account_key'] = self.account_key
        self.book._save(state)
        self.book.db.commit()
        bridge = HostExecutionBridge(
            self.book, self.root / 'host-owner-receipt',
            engineering_owner_root=self.owner.root)
        try:
            request = submit_engineering_intent(
                bridge.directory, 'engineering-1', self.plan,
                self.evidence(), clock=lambda: NOW)
            first = bridge(snapshot=self.snapshot,
                           quotes={'DEMO': self.quote}, allow_entries=False)
            self.assertEqual(first['status'],
                             'HOST_ENGINEERING_INTENT_RECORDED')
            owner_request_value = json.loads(
                (self.owner.root /
                 'supervised-engineering-request.json').read_text())
            owner_request_id = owner_request_value['request_id']
            receipt_path = (self.owner.root / 'engineering-intent-receipts' /
                            (owner_request_id + '.json'))
            self.assertTrue(receipt_path.is_file())
            receipt = json.loads(receipt_path.read_text())
            self.assertEqual(receipt['action_id'], first['action_id'])
            self.assertEqual(receipt['request_sha256'],
                             digest(owner_request_value))
            result_path = (bridge.directory / 'results' /
                           (request['request_id'] + '.json'))
            result_path.unlink()
            owner_request_value = {
                **owner_request_value,
                'valid_until': (NOW + timedelta(minutes=10)).isoformat()}
            request_path = (self.owner.root /
                            'supervised-engineering-request.json')
            request_path.write_text(json.dumps(
                owner_request_value, sort_keys=True))
            os.chmod(request_path, 0o600)
            receipt_path.unlink()
            submit_engineering_intent(
                bridge.directory, 'engineering-1', self.plan,
                self.evidence(), request_id=request['request_id'],
                clock=lambda: NOW)
            second = bridge(snapshot=self.snapshot,
                            quotes={'DEMO': self.quote}, allow_entries=False)
            self.assertEqual(second['status'],
                             'HOST_ENGINEERING_INTENT_RECORDED')
            restored = json.loads(receipt_path.read_text())
            self.assertEqual(restored['request_sha256'],
                             receipt['request_sha256'])
            self.assertNotEqual(
                digest(json.loads((self.owner.root /
                    'supervised-engineering-request.json').read_text())),
                restored['request_sha256'])
            self.assertEqual(len(list((self.owner.root /
                'engineering-intent-receipts').iterdir())), 1)
        finally:
            bridge.close()

    def _prepared_receipt_bridge(self):
        from martin_agentic.host_execution import HostExecutionBridge
        state = self.book.state()
        state['account_key'] = self.account_key
        self.book._save(state)
        self.book.db.commit()
        return HostExecutionBridge(
            self.book, self.root / 'host-core-window',
            engineering_owner_root=self.owner.root)

    def test_core_committed_without_host_audit_recovers_same_text(self):
        from martin_agentic.host_mailbox import submit_engineering_intent
        bridge = self._prepared_receipt_bridge()
        try:
            request = submit_engineering_intent(
                bridge.directory, 'engineering-1', self.plan,
                self.evidence(), clock=lambda: NOW)
            real_append = self.book.audit.append

            def fail_host_audit(event):
                if str(event.get('event_id', '')).startswith(
                        'host-engineering-intent-'):
                    raise RuntimeError('INJECTED_HOST_AUDIT_FAILURE')
                return real_append(event)

            with patch.object(self.book.audit, 'append',
                              side_effect=fail_host_audit):
                failed = bridge(snapshot=self.snapshot,
                                quotes={'DEMO': self.quote},
                                allow_entries=False)
            self.assertNotEqual(failed['status'],
                                'HOST_ENGINEERING_INTENT_RECORDED')
            (bridge.directory / 'results' /
             (request['request_id'] + '.json')).unlink()
            action = next(a for a in self.book.actions()
                          if a['kind'] == 'entry')
            self.assertEqual(action['state'], 'REVIEW_PENDING')
            submit_engineering_intent(
                bridge.directory, 'engineering-1', self.plan,
                self.evidence(), request_id=request['request_id'],
                clock=lambda: NOW)
            recovered = bridge(snapshot=self.snapshot,
                               quotes={'DEMO': self.quote},
                               allow_entries=False)
            self.assertEqual(recovered['status'],
                             'HOST_ENGINEERING_INTENT_RECORDED', recovered)
            receipt = json.loads((self.owner.root /
                'engineering-intent-receipts' /
                'request-1.json').read_text())
            self.assertEqual(receipt['request_sha256'],
                             digest(json.loads((self.owner.root /
                'supervised-engineering-request.json').read_text())))
        finally:
            bridge.close()

    def test_core_committed_without_host_audit_rejects_changed_text(self):
        from martin_agentic.host_mailbox import submit_engineering_intent
        bridge = self._prepared_receipt_bridge()
        try:
            request = submit_engineering_intent(
                bridge.directory, 'engineering-1', self.plan,
                self.evidence(), clock=lambda: NOW)
            real_append = self.book.audit.append

            def fail_host_audit(event):
                if str(event.get('event_id', '')).startswith(
                        'host-engineering-intent-'):
                    raise RuntimeError('INJECTED_HOST_AUDIT_FAILURE')
                return real_append(event)

            with patch.object(self.book.audit, 'append',
                              side_effect=fail_host_audit):
                bridge(snapshot=self.snapshot, quotes={'DEMO': self.quote},
                       allow_entries=False)
            (bridge.directory / 'results' /
             (request['request_id'] + '.json')).unlink()
            owner_request = json.loads((self.owner.root /
                'supervised-engineering-request.json').read_text())
            owner_request['valid_until'] = (
                NOW + timedelta(minutes=10)).isoformat()
            path = self.owner.root / 'supervised-engineering-request.json'
            path.write_text(json.dumps(owner_request, sort_keys=True))
            os.chmod(path, 0o600)
            submit_engineering_intent(
                bridge.directory, 'engineering-1', self.plan,
                self.evidence(), request_id=request['request_id'],
                clock=lambda: NOW)
            rejected = bridge(snapshot=self.snapshot,
                              quotes={'DEMO': self.quote},
                              allow_entries=False)
            self.assertNotEqual(rejected['status'],
                                'HOST_ENGINEERING_INTENT_RECORDED')
            self.assertIn('OWNER_RECEIPT_CORE_REQUEST_MISMATCH',
                          rejected.get('reason', ''))
            self.assertEqual(len([a for a in self.book.actions()
                                  if a['kind'] == 'entry']), 1)
        finally:
            bridge.close()

    def test_host_intent_without_authority_is_explicitly_unavailable(self):
        from martin_agentic.host_mailbox import submit_engineering_intent
        self.book.supervised_engineering_authority = None
        bridge = self._host_bridge('host-no-authority')
        try:
            submit_engineering_intent(
                bridge.directory, 'engineering-1', self.plan,
                self.evidence(), clock=lambda: NOW)
            out = bridge(snapshot=self.snapshot,
                         quotes={'DEMO': self.quote}, allow_entries=False)
            self.assertEqual(out['status'], 'HOST_REQUEST_REJECTED')
            self.assertEqual(out['reason'],
                             'SUPERVISED_ENGINEERING_AUTHORITY_UNAVAILABLE')
            self.assertEqual(self.book.actions(), [])
        finally:
            bridge.close()
            self.book.supervised_engineering_authority = self.authority

    def test_host_intent_request_expiry_rejected(self):
        from martin_agentic.host_mailbox import submit_engineering_intent
        bridge = self._host_bridge('host-expired')
        try:
            submit_engineering_intent(
                bridge.directory, 'engineering-1', self.plan,
                self.evidence(), clock=lambda: NOW - timedelta(seconds=31))
            out = bridge(snapshot=self.snapshot,
                         quotes={'DEMO': self.quote}, allow_entries=False)
            self.assertEqual(out['status'], 'HOST_REQUEST_REJECTED')
            self.assertEqual(out['reason'], 'HOST_REQUEST_EXPIRED')
            self.assertEqual(self.book.actions(), [])
        finally:
            bridge.close()

    def test_host_promote_wrong_permit_reference_rejected(self):
        from martin_agentic.host_mailbox import submit_engineering_promote
        action_id, packet, review = self.reviewed_intent()
        self.write_permit(self.permit(action_id, packet, review))
        bridge = self._host_bridge('host-wrong-permit')
        try:
            submit_engineering_promote(
                bridge.directory, action_id, '0' * 64, clock=lambda: NOW)
            out = bridge(snapshot=self.snapshot,
                         quotes={'DEMO': self.quote}, allow_entries=True)
            self.assertEqual(out['status'], 'HOST_REQUEST_REJECTED')
            self.assertEqual(out['reason'],
                             'SUPERVISED_ENGINEERING_PERMIT_REFERENCE_MISMATCH')
            self.assertEqual(next(a for a in self.book.actions()
                                  if a['id'] == action_id)['state'],
                             'WAITING_USER')
            self.assertEqual(self.consumption_count(), 0)
        finally:
            bridge.close()

    def test_host_intent_restart_is_idempotent_and_keeps_review_pending(self):
        from martin_agentic.host_mailbox import submit_engineering_intent
        bridge = self._host_bridge('host-restart')
        try:
            hub = bridge.directory
            submit_engineering_intent(
                hub, 'engineering-1', self.plan, self.evidence(),
                clock=lambda: NOW)
            first = bridge(snapshot=self.snapshot,
                           quotes={'DEMO': self.quote}, allow_entries=False)
            self.assertEqual(first['status'],
                             'HOST_ENGINEERING_INTENT_RECORDED')
            action_id = first['action_id']
            bridge.close()
            bridge = self._host_bridge('host-restart')
            submit_engineering_intent(
                hub, 'engineering-1', self.plan, self.evidence(),
                clock=lambda: NOW)
            second = bridge(snapshot=self.snapshot,
                            quotes={'DEMO': self.quote}, allow_entries=False)
            self.assertEqual(second['status'],
                             'HOST_ENGINEERING_INTENT_RECORDED')
            self.assertEqual(second['action_id'], action_id)
            self.assertEqual(next(a for a in self.book.actions()
                                  if a['id'] == action_id)['state'],
                             'REVIEW_PENDING')
            self.assertEqual(len([a for a in self.book.actions()
                                  if a['kind'] == 'entry']), 1)
        finally:
            bridge.close()

    def test_host_recovers_promotion_committed_before_host_result(self):
        from martin_agentic.host_mailbox import submit_engineering_promote
        action_id, packet, review = self.reviewed_intent()
        permit = self.permit(action_id, packet, review)
        self.write_permit(permit)
        # Simulate the core commit followed by interruption before the Host
        # completion audit/result writes.
        self.book.promote_supervised_entry(action_id)
        self.assertEqual(self.consumption_count(), 1)
        bridge = self._host_bridge('host-recover')
        try:
            submit_engineering_promote(
                bridge.directory, action_id, permit['permit_id'],
                clock=lambda: NOW)
            out = bridge(snapshot=self.snapshot,
                         quotes={'DEMO': self.quote}, allow_entries=True)
            self.assertEqual(out['status'], 'HOST_ENGINEERING_PROMOTED', out)
            self.assertTrue(out['recovered'])
            self.assertTrue(out['historical_local_promotion'])
            self.assertFalse(out['entry_authorized'])
            self.assertEqual(out['state'], 'READY')
            self.assertEqual(self.consumption_count(), 1)
            self.assertTrue((bridge.directory / 'results' /
                             (out['request_id'] + '.json')).is_file())
            promoted = self.book.db.execute(
                "SELECT count(*) FROM audit_events WHERE event_id LIKE "
                "'supervised-intent-%-promoted'").fetchone()[0]
            self.assertEqual(promoted, 1)
            host_audits = self.book.db.execute(
                "SELECT count(*) FROM audit_events WHERE event_id LIKE "
                "'host-engineering-promote-%'").fetchone()[0]
            self.assertEqual(host_audits, 1)
            # A new equivalent request ID may recover the same durable fact
            # but must not create another consumption.
            submit_engineering_promote(
                bridge.directory, action_id, permit['permit_id'],
                clock=lambda: NOW)
            second = bridge(snapshot=self.snapshot,
                            quotes={'DEMO': self.quote}, allow_entries=True)
            self.assertEqual(second['status'], 'HOST_ENGINEERING_PROMOTED')
            self.assertEqual(self.consumption_count(), 1)
        finally:
            bridge.close()

    def test_host_recovery_does_not_reclaim_advanced_action(self):
        from martin_agentic.host_mailbox import submit_engineering_promote
        action_id, packet, review = self.reviewed_intent()
        self.write_permit(self.permit(action_id, packet, review))
        self.book.promote_supervised_entry(action_id)
        self.book.db.execute(
            "UPDATE live_actions SET state='REPORTED' WHERE id=?", (action_id,))
        self.book.db.commit()
        bridge = self._host_bridge('host-advanced')
        try:
            submit_engineering_promote(
                bridge.directory, action_id,
                self.permit(action_id, packet, review)['permit_id'],
                clock=lambda: NOW)
            out = bridge(snapshot=self.snapshot,
                         quotes={'DEMO': self.quote}, allow_entries=True)
            self.assertEqual(out['status'],
                             'HOST_ENGINEERING_PROMOTION_ALREADY_COMPLETED')
            self.assertEqual(out['current_state'], 'REPORTED')
            self.assertFalse(out['claimable'])
            self.assertFalse(out['placeable'])
            self.assertEqual(self.consumption_count(), 1)
        finally:
            bridge.close()

    def test_host_recovers_result_file_from_existing_completion_audit(self):
        from martin_agentic.host_mailbox import submit_engineering_promote
        action_id, packet, review = self.reviewed_intent()
        permit = self.permit(action_id, packet, review)
        self.write_permit(permit)
        self.book.promote_supervised_entry(action_id)
        bridge = self._host_bridge('host-result-recover')
        try:
            request = submit_engineering_promote(
                bridge.directory, action_id, permit['permit_id'],
                clock=lambda: NOW)
            first = bridge(snapshot=self.snapshot,
                           quotes={'DEMO': self.quote}, allow_entries=True)
            self.assertEqual(first['status'], 'HOST_ENGINEERING_PROMOTED')
            result_path = bridge.directory / 'results' / (request['request_id'] + '.json')
            self.assertTrue(result_path.is_file())
            result_path.unlink()
            submit_engineering_promote(
                bridge.directory, action_id, permit['permit_id'],
                request_id=request['request_id'], clock=lambda: NOW)
            second = bridge(snapshot=self.snapshot,
                            quotes={'DEMO': self.quote}, allow_entries=True)
            self.assertEqual(second['status'], 'HOST_ENGINEERING_PROMOTED')
            self.assertTrue(result_path.is_file())
            self.assertEqual(self.consumption_count(), 1)
        finally:
            bridge.close()

    def test_host_recovery_rejects_wrong_permit_after_core_commit(self):
        from martin_agentic.host_mailbox import submit_engineering_promote
        action_id, packet, review = self.reviewed_intent()
        permit = self.permit(action_id, packet, review)
        self.write_permit(permit)
        self.book.promote_supervised_entry(action_id)
        bridge = self._host_bridge('host-wrong-after-core')
        try:
            submit_engineering_promote(
                bridge.directory, action_id, '1' * 64, clock=lambda: NOW)
            out = bridge(snapshot=self.snapshot,
                         quotes={'DEMO': self.quote}, allow_entries=True)
            self.assertEqual(out['status'], 'HOST_REQUEST_REJECTED')
            self.assertEqual(out['reason'],
                             'SUPERVISED_ENGINEERING_PROMOTION_STATE')
            self.assertEqual(self.consumption_count(), 1)
            self.assertEqual(next(a for a in self.book.actions()
                                  if a['id'] == action_id)['state'], 'READY')
        finally:
            bridge.close()

    def test_host_same_request_result_regeneration_uses_current_action_state(self):
        from martin_agentic.host_mailbox import submit_engineering_promote
        action_id, packet, review = self.reviewed_intent()
        permit = self.permit(action_id, packet, review)
        self.write_permit(permit)
        self.book.promote_supervised_entry(action_id)
        bridge = self._host_bridge('host-current-state')
        try:
            request = submit_engineering_promote(
                bridge.directory, action_id, permit['permit_id'],
                clock=lambda: NOW)
            first = bridge(snapshot=self.snapshot,
                           quotes={'DEMO': self.quote}, allow_entries=True)
            self.assertEqual(first['status'], 'HOST_ENGINEERING_PROMOTED')
            result_path = bridge.directory / 'results' / (request['request_id'] + '.json')
            for state in ('REPORTED', 'UNKNOWN', 'CANCELLED'):
                with self.subTest(state=state):
                    self.book.db.execute(
                        'UPDATE live_actions SET state=? WHERE id=?',
                        (state, action_id))
                    self.book.db.commit()
                    from martin_agentic.host_mailbox import read_request_result
                    viewed = read_request_result(
                        bridge.directory, request['request_id'],
                        clock=lambda: NOW)
                    self.assertEqual(viewed['current_state'], state)
                    self.assertFalse(viewed['claimable'])
                    self.assertFalse(viewed['placeable'])
                    if result_path.exists():
                        result_path.unlink()
                    out = bridge(snapshot=self.snapshot,
                                 quotes={'DEMO': self.quote},
                                 allow_entries=True)
                    self.assertEqual(
                        out['status'],
                        'HOST_ENGINEERING_PROMOTION_ALREADY_COMPLETED', out)
                    self.assertEqual(out['current_state'], state)
                    self.assertEqual(out['historical_state'], 'READY')
                    self.assertFalse(out['claimable'])
                    self.assertFalse(out['placeable'])
                    self.assertEqual(self.consumption_count(), 1)
        finally:
            bridge.close()

    def test_host_recovery_action_missing_is_unknown_not_old_green(self):
        from martin_agentic.host_mailbox import submit_engineering_promote
        action_id, packet, review = self.reviewed_intent()
        permit = self.permit(action_id, packet, review)
        self.write_permit(permit)
        self.book.promote_supervised_entry(action_id)
        bridge = self._host_bridge('host-action-missing')
        try:
            request = submit_engineering_promote(
                bridge.directory, action_id, permit['permit_id'],
                clock=lambda: NOW)
            first = bridge(snapshot=self.snapshot,
                           quotes={'DEMO': self.quote}, allow_entries=True)
            self.assertEqual(first['status'], 'HOST_ENGINEERING_PROMOTED')
            self.book.db.execute(
                'DELETE FROM live_actions WHERE id=?', (action_id,))
            self.book.db.commit()
            result_path = bridge.directory / 'results' / (request['request_id'] + '.json')
            if result_path.exists():
                result_path.unlink()
            out = bridge(snapshot=self.snapshot,
                         quotes={'DEMO': self.quote}, allow_entries=True)
            self.assertEqual(out['status'],
                             'HOST_ENGINEERING_PROMOTION_ACTION_MISSING')
            self.assertEqual(out['current_state'], 'UNKNOWN')
            self.assertEqual(out['historical_state'], 'READY')
            self.assertFalse(out['claimable'])
            self.assertFalse(out['placeable'])
            self.assertEqual(self.consumption_count(), 1)
        finally:
            bridge.close()

    def test_host_intent_result_regeneration_uses_current_state(self):
        from martin_agentic.host_mailbox import submit_engineering_intent
        bridge = self._host_bridge('host-intent-state')
        try:
            request = submit_engineering_intent(
                bridge.directory, 'engineering-1', self.plan,
                self.evidence(), clock=lambda: NOW)
            first = bridge(snapshot=self.snapshot,
                           quotes={'DEMO': self.quote}, allow_entries=False)
            self.assertEqual(first['status'],
                             'HOST_ENGINEERING_INTENT_RECORDED')
            action_id = first['action_id']
            result_path = bridge.directory / 'results' / (request['request_id'] + '.json')
            for state, expected in (
                    ('REVIEW_PENDING', 'HOST_ENGINEERING_INTENT_RECORDED'),
                    ('WAITING_USER', 'HOST_ENGINEERING_INTENT_REVIEWED'),
                    ('READY', 'HOST_ENGINEERING_INTENT_ALREADY_PROGRESSED'),
                    ('REPORTED', 'HOST_ENGINEERING_INTENT_ALREADY_PROGRESSED')):
                with self.subTest(state=state):
                    self.book.db.execute(
                        'UPDATE live_actions SET state=? WHERE id=?',
                        (state, action_id))
                    self.book.db.commit()
                    if result_path.exists():
                        result_path.unlink()
                    submit_engineering_intent(
                        bridge.directory, 'engineering-1', self.plan,
                        self.evidence(), request_id=request['request_id'],
                        clock=lambda: NOW)
                    out = bridge(snapshot=self.snapshot,
                                 quotes={'DEMO': self.quote},
                                 allow_entries=False)
                    self.assertEqual(out['status'], expected, out)
                    self.assertEqual(out['current_state'], state)
                    self.assertEqual(out['historical_state'], 'REVIEW_PENDING')
                    self.assertFalse(out['claimable'])
                    self.assertFalse(out['placeable'])
                    self.assertEqual(self.consumption_count(), 0)
                    self.assertEqual(len([a for a in self.book.actions()
                                          if a['kind'] == 'entry']), 1)
                    audits = self.book.db.execute(
                        "SELECT count(*) FROM audit_events WHERE event_id=?",
                        ('host-engineering-intent-' + request['request_id'],)
                    ).fetchone()[0]
                    self.assertEqual(audits, 1)
            self.book.db.execute(
                'DELETE FROM live_actions WHERE id=?', (action_id,))
            self.book.db.commit()
            if result_path.exists():
                result_path.unlink()
            submit_engineering_intent(
                bridge.directory, 'engineering-1', self.plan,
                self.evidence(), request_id=request['request_id'],
                clock=lambda: NOW)
            out = bridge(snapshot=self.snapshot, quotes={'DEMO': self.quote},
                         allow_entries=False)
            self.assertEqual(out['status'],
                             'HOST_ENGINEERING_INTENT_ACTION_MISSING')
            self.assertEqual(out['current_state'], 'UNKNOWN')
            self.assertFalse(out['reviewable'])
        finally:
            bridge.close()

    def test_read_request_result_db_unavailable_and_binding_mismatch(self):
        from martin_agentic.host_mailbox import (
            read_private_json, read_request_result,
            submit_engineering_promote, write_private_json)
        action_id, packet, review = self.reviewed_intent()
        permit = self.permit(action_id, packet, review)
        self.write_permit(permit)
        self.book.promote_supervised_entry(action_id)
        bridge = self._host_bridge('host-db-unavailable')
        try:
            request = submit_engineering_promote(
                bridge.directory, action_id, permit['permit_id'],
                clock=lambda: NOW)
            first = bridge(snapshot=self.snapshot,
                           quotes={'DEMO': self.quote}, allow_entries=True)
            self.assertEqual(first['status'], 'HOST_ENGINEERING_PROMOTED')
            # A reader holding an exclusive write lock makes the bounded
            # mode=ro lookup unavailable rather than falling back to READY.
            blocker = sqlite3.connect(self.book.db_path, timeout=1)
            blocker.execute('BEGIN EXCLUSIVE')
            try:
                viewed = read_request_result(
                    bridge.directory, request['request_id'], clock=lambda: NOW)
            finally:
                blocker.rollback()
                blocker.close()
            self.assertEqual(
                viewed['status'],
                'HOST_ENGINEERING_PROMOTION_CURRENT_STATE_UNAVAILABLE')
            self.assertEqual(viewed['current_state'], 'UNKNOWN')
            self.assertFalse(viewed['claimable'])
            self.assertFalse(viewed['placeable'])
            # A mis-bound ledger is also unavailable, not old green.
            path = bridge.directory / 'results' / (request['request_id'] + '.json')
            result = read_private_json(path)
            changed = json.loads(json.dumps(result))
            changed['binding'] = {**changed['binding'],
                                  'account_key': '0' * 64}
            path.unlink()
            write_private_json(path, changed)
            viewed = read_request_result(
                bridge.directory, request['request_id'], clock=lambda: NOW)
            self.assertEqual(
                viewed['status'],
                'HOST_ENGINEERING_PROMOTION_CURRENT_STATE_UNAVAILABLE')
            self.assertEqual(viewed['current_state'], 'UNKNOWN')
            self.assertFalse(viewed['claimable'])
        finally:
            bridge.close()
