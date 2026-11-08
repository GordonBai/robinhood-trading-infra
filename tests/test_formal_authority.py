"""Owner-sourced formal entry authorization: synthetic positive/negative matrix."""
from datetime import timedelta
import hashlib
import json
import os
from pathlib import Path
import tempfile
from unittest import TestCase
from unittest.mock import patch

from martin_agentic.audit import digest
from martin_agentic.engineering import EngineeringPlans
from martin_agentic.broker import BrokerJournal
from martin_agentic.formal_authority import (
    FormalEntryAuthority, FormalSourceStore, NoFormalEntryAuthority,
    PROTECTION_REQUIRED, SOURCE_CHANNEL, SOURCE_PROTECTION, SOURCE_SYNTHETIC,
    SOURCE_USER, _contract_hash, _tool_map, order_parameters_from_plan,
    seal_record)
from trading_contracts.values import timestamp
from martin_agentic.execution import ExecutionBook
import test_assessment_receipt as receipt_module


NOW = receipt_module.NOW_LATER
PLAN = {'symbol': 'DEMO', 'quantity': 2, 'entry_ceiling': '40',
        'stop': '39', 'cost_buffer_per_share': '0.05'}
ACCOUNT = 'a' * 64
LATER = timestamp('2026-11-04T02:00:00+00:00')
METADATA_PATH = 'tests/fixtures/formal-tool-contract.json'


def policy():
    return json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())


def user_body(**overrides):
    body = {
        'schema_version': 1,
        'kind': 'user_order_authorization',
        'source_type': SOURCE_SYNTHETIC,
        'source_id': 'synthetic-user-order',
        'account_key': ACCOUNT,
        'account_suffix': '0000',
        'purpose': 'engineering',
        'project_scope_ref': 'synthetic-project-scope',
        'order_authorization_ref': 'synthetic-order-ref',
        'parameters': order_parameters_from_plan(PLAN),
        'authorized_at': NOW.isoformat(),
        'not_before': NOW.isoformat(),
        'captured_at': NOW.isoformat(),
        'valid_until': (NOW + timedelta(minutes=30)).isoformat(),
        'one_shot_id': 'synthetic-one-shot-1',
        'synthetic': True,
    }
    body.update(overrides)
    return seal_record(body)


def channel_body(**overrides):
    body = {
        'schema_version': 1,
        'kind': 'formal_channel_attestation',
        'source_type': SOURCE_SYNTHETIC,
        'source_id': 'synthetic-channel',
        'account_key': ACCOUNT,
        'account_suffix': '0000',
        'order_authorization_ref': 'synthetic-order-ref',
        'tool_identity': ['review_equity_order', 'place_equity_order'],
        'review_contract_sha256': 'b' * 64,
        'place_contract_sha256': 'c' * 64,
        'review_semantics': 'review_returns_quote_and_order_checks',
        'place_semantics': 'place_follows_current_review_contract',
        'app_confirmation_supported': True,
        'explicit_user_confirmation_supported': True,
        'local_bypass_allowed': False,
        'captured_at': NOW.isoformat(),
        'valid_until': (NOW + timedelta(minutes=30)).isoformat(),
        'synthetic': True,
    }
    body.update(overrides)
    return seal_record(body)


def protection_body(**overrides):
    body = {
        'schema_version': 1,
        'kind': 'protection_attestation',
        'source_type': SOURCE_SYNTHETIC,
        'source_id': 'synthetic-protection',
        'account_key': ACCOUNT,
        'account_suffix': '0000',
        'order_authorization_ref': 'synthetic-order-ref',
        'symbol': 'DEMO',
        'quantity': '2',
        'stop_price': '39',
        'protection_mode': 'synthetic_test_protection',
        'protection_tool_identity': ['review_advanced_order',
                                     'place_advanced_order'],
        'protection_contract_sha256': 'd' * 64,
        'feasible_before_entry': True,
        'max_submit_seconds': 30,
        'failure_exit_path': 'verified_failure_exit',
        'failure_exit_verified': True,
        'captured_at': NOW.isoformat(),
        'valid_until': (NOW + timedelta(minutes=30)).isoformat(),
        'synthetic': True,
    }
    body.update(overrides)
    return seal_record(body)



def metadata_contract():
    raw = Path(METADATA_PATH).read_bytes()
    tools = _tool_map(json.loads(raw))
    review = _contract_hash('review_equity_order',
                            tools['review_equity_order'])
    place = _contract_hash('place_equity_order',
                           tools['place_equity_order'])
    preview = _contract_hash('review_advanced_order',
                             tools['review_advanced_order'])
    protecc = _contract_hash('place_advanced_order',
                             tools['place_advanced_order'])
    return {
        'review_sha256': review,
        'place_sha256': place,
        'protection_sha256': digest({'review': preview, 'place': protecc}),
        'metadata_sha256': hashlib.sha256(raw).hexdigest(),
    }


def owner_user_body(plan_payload, **overrides):
    body = user_body(
        source_type=SOURCE_USER, synthetic=False, source_id='owner-user',
        account_key=ACCOUNT, account_suffix='0000', purpose='engineering',
        project_scope_ref=plan_payload['id'],
        order_authorization_ref=plan_payload['id'],
        parameters=order_parameters_from_plan(PLAN),
        authorized_at=plan_payload['created_at'],
        captured_at=LATER.isoformat(), not_before=LATER.isoformat(),
        valid_until=(LATER + timedelta(minutes=30)).isoformat(),
        one_shot_id='owner-one-shot-1',
        provenance={'kind': 'engineering_plan_row',
                    'plan_id': plan_payload['id'],
                    'record_sha256': digest(plan_payload)})
    body.update(overrides)
    return seal_record(body)


def owner_channel_body(plan_id, contract, **overrides):
    body = channel_body(
        source_type=SOURCE_CHANNEL, synthetic=False, source_id='owner-channel',
        account_key=ACCOUNT, order_authorization_ref=plan_id,
        review_contract_sha256=contract['review_sha256'],
        place_contract_sha256=contract['place_sha256'],
        captured_at=LATER.isoformat(),
        valid_until=(LATER + timedelta(minutes=30)).isoformat(),
        provenance={'kind': 'official_tool_metadata_capture',
                    'record_path': METADATA_PATH,
                    'record_sha256': contract['metadata_sha256']})
    body.update(overrides)
    return seal_record(body)


def protection_evidence(plan_id, contract):
    return {
        'schema_version': 1,
        'kind': 'owner_protection_evidence_v1',
        'producer': 'owner_protection_audit_v1',
        'recorded_at': LATER.isoformat(),
        'account_key': ACCOUNT,
        'order_authorization_ref': plan_id,
        'symbol': 'DEMO',
        'quantity': '2',
        'stop_price': '39',
        'protection_mode': 'verified_failure_exit',
        'max_submit_seconds': 30,
        'failure_exit_path': 'verified_failure_exit',
        'failure_exit_verified': True,
        'feasible_before_entry': True,
        'tool_identity': list(PROTECTION_REQUIRED),
        'tool_contract': {'record_path': METADATA_PATH,
                          'record_sha256': contract['metadata_sha256']},
        'tool_contract_sha256': contract['protection_sha256'],
    }


def owner_protection_body(plan_id, contract, evidence, **overrides):
    body = protection_body(
        source_type=SOURCE_PROTECTION, synthetic=False,
        source_id='owner-protection', account_key=ACCOUNT,
        order_authorization_ref=plan_id,
        protection_mode='verified_failure_exit',
        protection_tool_identity=list(PROTECTION_REQUIRED),
        protection_contract_sha256=contract['protection_sha256'],
        captured_at=LATER.isoformat(),
        valid_until=(LATER + timedelta(minutes=30)).isoformat(),
        provenance={'kind': 'owner_protection_evidence_v1',
                    'record_path': 'protection-evidence.json',
                    'record_sha256': digest(evidence)})
    body.update(overrides)
    return seal_record(body)


class SourceFixture:
    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.root.chmod(0o700)
        self.write('user-order-authorization.json', user_body())
        self.write('formal-channel-attestation.json', channel_body())
        self.write('protection-attestation.json', protection_body())

    def write(self, name, value):
        path = self.root / name
        path.write_text(json.dumps(value, sort_keys=True, separators=(',', ':')))
        os.chmod(path, 0o600)
        return path

    def store(self):
        return FormalSourceStore(self.root)


class FormalAuthorityTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.sources = SourceFixture(Path(self.temp.name) / 'formal-sources')
        self.db_path = Path(self.temp.name) / 'book.sqlite'
        self.book = ExecutionBook(self.db_path, policy(), simulation=True,
                                  clock=lambda: NOW)
        self.addCleanup(self.book.close)
        self.authority = FormalEntryAuthority(
            self.sources.store(), allow_synthetic=True, clock=lambda: NOW)

    def assess(self, mode='proposal', action=None, plan=PLAN,
               snapshot=None):
        return self.authority.assess(
            book=self.book, mode=mode, action=action, plan=plan,
            purpose='engineering',
            snapshot=snapshot if snapshot is not None else {
                'account_key': ACCOUNT})

    def component(self):
        decision = self.assess()
        self.assertTrue(decision['allowed'], decision)
        return decision['sanitized']

    def test_valid_synthetic_chain_allowed_and_once_only(self):
        decision = self.assess()
        self.assertTrue(decision['allowed'], decision)
        component = decision['sanitized']
        self.assertEqual(component['status'], 'allowed')
        self.assertEqual(component['source_sha256']['user'],
                         user_body()['source_sha256'])
        self.assertNotIn('raw', component)
        self.assertNotIn('source_path', component)
        self.authority.consume(self.book, component, 'action-1', NOW)
        claimed = self.assess(mode='claim', action={'id': 'action-1'})
        self.assertTrue(claimed['allowed'], claimed)
        with self.assertRaisesRegex(ValueError,
                                    'FORMAL_AUTHORIZATION_REPLAYED'):
            self.authority.consume(self.book, component, 'action-2', NOW)
        other = self.assess(mode='claim', action={'id': 'action-2'})
        self.assertFalse(other['allowed'])
        self.assertIn('FORMAL_AUTHORIZATION_ACTION_MISMATCH',
                      other['reasons'])

    def test_proposal_rejects_already_consumed_and_unconsumed_claim(self):
        component = self.component()
        unconsumed = self.assess(mode='claim', action={'id': 'action-1'})
        self.assertIn('FORMAL_AUTHORIZATION_NOT_CONSUMED',
                      unconsumed['reasons'])
        self.authority.consume(self.book, component, 'action-1', NOW)
        replay = self.assess()
        self.assertIn('FORMAL_AUTHORIZATION_REPLAYED', replay['reasons'])

    def test_missing_and_malformed_sources_fail_closed(self):
        self.sources.root.joinpath('user-order-authorization.json').unlink()
        self.assertIn('FORMAL_SOURCE_MISSING', self.assess()['reasons'])

        self.sources.write('user-order-authorization.json', user_body())
        path = self.sources.root / 'user-order-authorization.json'
        value = json.loads(path.read_text())
        value['synthetic'] = 'true'
        path.write_text(json.dumps(seal_record(value), sort_keys=True))
        self.assertIn('FORMAL_SYNTHETIC_FLAG_INVALID',
                      self.assess()['reasons'])

        self.sources.write('user-order-authorization.json', user_body())
        os.chmod(path, 0o644)
        self.assertIn('FORMAL_SOURCE_FILE_INVALID',
                      self.assess()['reasons'])
        os.chmod(path, 0o600)

        link = Path(self.temp.name) / 'formal-sources-link'
        link.symlink_to(self.sources.root, target_is_directory=True)
        linked = FormalEntryAuthority(
            FormalSourceStore(link), allow_synthetic=True, clock=lambda: NOW)
        decision = linked.assess(
            book=self.book, mode='proposal', plan=PLAN,
            purpose='engineering', snapshot={'account_key': ACCOUNT})
        self.assertIn('FORMAL_SOURCE_ROOT_INVALID', decision['reasons'])

        self.sources.write('user-order-authorization.json',
                           {**user_body(), 'source_sha256': '0' * 64})
        self.assertIn('FORMAL_SOURCE_DIGEST_MISMATCH',
                      self.assess()['reasons'])

    def test_account_purpose_parameters_and_time_are_bound(self):
        cases = [
            ('account_key', 'b' * 64, 'FORMAL_USER_ACCOUNT_MISMATCH'),
            ('account_suffix', '1111', 'FORMAL_USER_SUFFIX_MISMATCH'),
            ('purpose', 'strategy', 'FORMAL_USER_PURPOSE_MISMATCH'),
            ('parameters', {**order_parameters_from_plan(PLAN),
                            'quantity': '3'},
             'FORMAL_USER_PARAMETERS_MISMATCH'),
            ('valid_until', (NOW - timedelta(seconds=1)).isoformat(),
             'FORMAL_USER_EXPIRED'),
            ('not_before', (NOW + timedelta(seconds=1)).isoformat(),
             'FORMAL_USER_NOT_YET_VALID'),
        ]
        for field, value, reason in cases:
            with self.subTest(field=field):
                self.sources.write('user-order-authorization.json',
                                   user_body(**{field: value}))
                self.assertIn(reason, self.assess()['reasons'])
        self.sources.write('user-order-authorization.json', user_body())

    def test_channel_requires_current_review_place_and_no_local_bypass(self):
        for overrides, reason in (
                ({'tool_identity': ['review_equity_order']},
                 'FORMAL_CHANNEL_TOOLS_INCOMPLETE'),
                ({'local_bypass_allowed': True},
                 'FORMAL_CHANNEL_LOCAL_BYPASS_NOT_ALLOWED'),
                ({'app_confirmation_supported': False,
                  'explicit_user_confirmation_supported': False},
                 'FORMAL_CHANNEL_CONFIRMATION_UNPROVEN'),
                ({'order_authorization_ref': 'other'},
                 'FORMAL_CHANNEL_ORDER_REF_MISMATCH'),
                ({'valid_until': (NOW - timedelta(seconds=1)).isoformat()},
                 'FORMAL_CHANNEL_EXPIRED')):
            with self.subTest(reason=reason):
                self.sources.write('formal-channel-attestation.json',
                                   channel_body(**overrides))
                self.assertIn(reason, self.assess()['reasons'])
        self.sources.write('formal-channel-attestation.json', channel_body())

    def test_protection_must_be_feasible_exact_and_within_30_seconds(self):
        for overrides, reason in (
                ({'max_submit_seconds': 31},
                 'FORMAL_PROTECTION_SUBMIT_SECONDS_EXCEEDED'),
                ({'feasible_before_entry': False},
                 'FORMAL_PROTECTION_NOT_FEASIBLE_BEFORE_ENTRY'),
                ({'failure_exit_verified': False},
                 'FORMAL_PROTECTION_FAILURE_EXIT_UNVERIFIED'),
                ({'stop_price': '38'},
                 'FORMAL_PROTECTION_PARAMETERS_MISMATCH'),
                ({'symbol': 'OTHER'}, 'FORMAL_PROTECTION_PARAMETERS_MISMATCH'),
                ({'order_authorization_ref': 'other'},
                 'FORMAL_PROTECTION_ORDER_REF_MISMATCH')):
            with self.subTest(reason=reason):
                self.sources.write('protection-attestation.json',
                                   protection_body(**overrides))
                self.assertIn(reason, self.assess()['reasons'])
        self.sources.write('protection-attestation.json', protection_body())

    def test_synthetic_authority_is_rejected_on_real_book(self):
        real_path = Path(self.temp.name) / 'real.sqlite'
        real = ExecutionBook(real_path, policy(), simulation=False,
                             clock=lambda: NOW)
        self.addCleanup(real.close)
        decision = self.authority.assess(
            book=real, mode='proposal', plan=PLAN, purpose='engineering',
            snapshot={'account_key': ACCOUNT})
        self.assertFalse(decision['allowed'])
        self.assertIn('FORMAL_SYNTHETIC_AUTHORITY_ON_REAL_BOOK',
                      decision['reasons'])
        default = NoFormalEntryAuthority().assess(
            book=real, mode='proposal', plan=PLAN, purpose='engineering')
        self.assertIn('FORMAL_ENGINEERING_CHANNEL_UNPROVEN',
                      default['reasons'])

    def test_receipt_recheck_detects_source_change(self):
        component = self.component()
        self.authority.consume(self.book, component, 'action-1', NOW)
        action = {'id': 'action-1'}
        self.assertEqual(self.authority.recheck_receipt(
            component, book=self.book, action=action,
            purpose='engineering'), [])
        self.sources.write('formal-channel-attestation.json',
                           channel_body(**{
                               'place_contract_sha256': 'e' * 64}))
        reasons = self.authority.recheck_receipt(
            component, book=self.book, action=action,
            purpose='engineering')
        self.assertTrue(any(item.startswith('RECEIPT_FORMAL_')
                            for item in reasons), reasons)

    def test_evidence_dict_or_bool_cannot_create_permit(self):
        self.sources.root.joinpath('user-order-authorization.json').unlink()
        decision = self.authority.assess(
            book=self.book, mode='proposal', plan=PLAN,
            purpose='engineering', snapshot={'account_key': ACCOUNT},
            evidence={'formal_channel_allowed': True,
                      'user_order_authorization': {'allowed': True},
                      'engineering_scope_authorized': True})
        self.assertFalse(decision['allowed'])
        self.assertIn('FORMAL_SOURCE_MISSING', decision['reasons'])

    def test_default_real_preflight_still_denies_without_formal_sources(self):
        from martin_agentic.engineering_preflight import CurrentEngineeringPreflight
        real_path = Path(self.temp.name) / 'real-preflight.sqlite'
        real = ExecutionBook(real_path, policy(), simulation=False,
                             clock=lambda: NOW)
        self.addCleanup(real.close)
        preflight = CurrentEngineeringPreflight(
            Path(self.temp.name) / 'capture',
            Path(self.temp.name) / 'public.json',
            account_key=ACCOUNT, account_suffix='0000',
            policy_sha256=digest(policy()),
            source_fingerprint_value='0' * 64, clock=lambda: NOW)
        real._current_preflight = preflight
        decision = real._current_preflight_decision(
            proposal=True, snapshot={'account_key': ACCOUNT},
            quote={'symbol': 'DEMO'}, plan=PLAN, purpose='engineering',
            evidence={'synthetic': False})
        self.assertFalse(decision['allowed'])
        self.assertIn('FORMAL_ENGINEERING_CHANNEL_UNPROVEN',
                      decision['reasons'])


    def test_future_capture_authorized_and_scheduled_boundaries(self):
        self.sources.write(
            'user-order-authorization.json',
            user_body(captured_at=(NOW + timedelta(hours=1)).isoformat(),
                      authorized_at=NOW.isoformat(),
                      valid_until=(NOW + timedelta(hours=2)).isoformat()))
        self.assertIn('FORMAL_USER_CAPTURED_FUTURE', self.assess()['reasons'])

        self.sources.write(
            'user-order-authorization.json',
            user_body(captured_at=NOW.isoformat(),
                      authorized_at=(NOW + timedelta(hours=1)).isoformat(),
                      valid_until=(NOW + timedelta(hours=2)).isoformat()))
        self.assertIn('FORMAL_USER_AUTHORIZED_AT_FUTURE',
                      self.assess()['reasons'])

        self.sources.write(
            'user-order-authorization.json',
            user_body(captured_at=NOW.isoformat(),
                      authorized_at=NOW.isoformat(),
                      not_before=(NOW + timedelta(hours=1)).isoformat(),
                      valid_until=(NOW + timedelta(hours=2)).isoformat()))
        self.assertIn('FORMAL_USER_NOT_YET_VALID',
                      self.assess()['reasons'])

        self.sources.write(
            'user-order-authorization.json',
            user_body(captured_at=NOW.isoformat(),
                      authorized_at=NOW.isoformat(),
                      not_before=(NOW + timedelta(hours=1)).isoformat(),
                      valid_until=(NOW + timedelta(minutes=30)).isoformat()))
        self.assertIn('FORMAL_USER_WINDOW_INVALID',
                      self.assess()['reasons'])

        self.sources.write(
            'user-order-authorization.json',
            user_body(captured_at=(NOW - timedelta(seconds=1)).isoformat(),
                      authorized_at=NOW.isoformat(),
                      valid_until=(NOW + timedelta(minutes=30)).isoformat()))
        self.assertIn('FORMAL_USER_AUTHORIZED_AFTER_CAPTURE',
                      self.assess()['reasons'])

        self.sources.write(
            'user-order-authorization.json',
            user_body(captured_at=NOW.isoformat(),
                      authorized_at=NOW.isoformat(),
                      not_before=NOW.isoformat(),
                      valid_until=(NOW + timedelta(minutes=30)).isoformat()))
        self.assertTrue(self.assess()['allowed'])

        self.sources.write(
            'formal-channel-attestation.json',
            channel_body(captured_at=(NOW + timedelta(hours=1)).isoformat(),
                         valid_until=(NOW + timedelta(hours=2)).isoformat()))
        self.assertIn('FORMAL_CHANNEL_CAPTURED_FUTURE',
                      self.assess()['reasons'])
        self.sources.write('formal-channel-attestation.json', channel_body())

        self.sources.write(
            'protection-attestation.json',
            protection_body(captured_at=(NOW + timedelta(hours=1)).isoformat(),
                            valid_until=(NOW + timedelta(hours=2)).isoformat()))
        self.assertIn('FORMAL_PROTECTION_CAPTURED_FUTURE',
                      self.assess()['reasons'])


class OwnerProvenanceTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.sources = SourceFixture(Path(self.temp.name) / 'formal-sources')
        self.book = ExecutionBook(
            Path(self.temp.name) / 'real-provenance.sqlite', policy(),
            simulation=False, clock=lambda: LATER)
        self.addCleanup(self.book.close)
        self.plan_payload = EngineeringPlans(self.book).draft(
            PLAN, quote={},
            scope_authorization='USER_EXPLICITLY_ALLOWED_SMALL_ENGINEERING_TEST',
            parameters_user_specified=True)
        self.contract = metadata_contract()
        evidence = protection_evidence(self.plan_payload['id'], self.contract)
        self.sources.write('user-order-authorization.json',
                           owner_user_body(self.plan_payload))
        self.sources.write('formal-channel-attestation.json',
                           owner_channel_body(self.plan_payload['id'],
                                              self.contract))
        self.sources.write('protection-attestation.json',
                           owner_protection_body(self.plan_payload['id'],
                                                 self.contract, evidence))
        self.sources.write('protection-evidence.json', evidence)
        self.authority = FormalEntryAuthority(
            self.sources.store(), allow_synthetic=False, clock=lambda: LATER)

    def assess(self):
        return self.authority.assess(
            book=self.book, mode='proposal', plan=PLAN,
            purpose='engineering', snapshot={'account_key': ACCOUNT},
            evidence={'source_sha256': 'owner-evidence'})

    def test_owner_structural_chain_without_real_producer_is_unproven(self):
        decision = self.assess()
        self.assertFalse(decision['allowed'], decision)
        self.assertTrue(decision['schema_valid'])
        self.assertFalse(decision['provenance_proven'])
        self.assertIn('CHANNEL_PROTECTION_PATH_UNPROVEN',
                      decision['reasons'])
        self.sources.write(
            'formal-channel-attestation.json',
            owner_channel_body(self.plan_payload['id'], self.contract,
                               review_contract_sha256='e' * 64))
        changed = self.assess()
        self.assertFalse(changed['allowed'])
        self.assertTrue(changed['schema_valid'])
        self.assertFalse(changed['provenance_proven'])
        self.assertIn('PROVENANCE_REVIEW_CONTRACT_MISMATCH',
                      changed['reasons'])

    def test_owner_label_without_provenance_is_not_allowed(self):
        self.sources.write(
            'user-order-authorization.json',
            owner_user_body(self.plan_payload,
                            provenance=None))
        decision = self.assess()
        self.assertFalse(decision['allowed'])
        self.assertTrue(decision['schema_valid'])
        self.assertFalse(decision['provenance_proven'])
        self.assertIn('PROVENANCE_UNPROVEN:USER', decision['reasons'])

    def test_owner_protection_evidence_missing_is_not_allowed(self):
        self.sources.root.joinpath('protection-evidence.json').unlink()
        decision = self.assess()
        self.assertFalse(decision['allowed'])
        self.assertTrue(decision['schema_valid'])
        self.assertFalse(decision['provenance_proven'])
        self.assertIn('FORMAL_SOURCE_MISSING', decision['reasons'])

    def test_real_preflight_consumer_rejects_owner_label_without_provenance(self):
        inner = receipt_module.ReceiptBindingTests(
            'test_valid_matching_receipt_binding_is_true_while_source_gaps_block')
        inner.setUp()
        self.addCleanup(inner.doCleanups)
        preflight = inner.preflight
        metadata = metadata_contract()
        dummy = {'id': 'no-plan-row', 'created_at': NOW.isoformat()}
        evidence = protection_evidence('no-plan-row', metadata)
        self.sources.write(
            'user-order-authorization.json',
            owner_user_body(dummy, account_key=inner.account_key,
                            project_scope_ref='no-plan-row',
                            order_authorization_ref='no-plan-row',
                            provenance=None,
                            captured_at=NOW.isoformat(),
                            authorized_at=NOW.isoformat(),
                            not_before=NOW.isoformat(),
                            valid_until=(NOW + timedelta(minutes=30)).isoformat()))
        self.sources.write(
            'formal-channel-attestation.json',
            owner_channel_body('no-plan-row', metadata,
                               account_key=inner.account_key,
                               captured_at=NOW.isoformat(),
                               valid_until=(NOW + timedelta(minutes=30)).isoformat()))
        self.sources.write(
            'protection-attestation.json',
            owner_protection_body('no-plan-row', metadata, evidence,
                                  account_key=inner.account_key,
                                  captured_at=NOW.isoformat(),
                                  valid_until=(NOW + timedelta(minutes=30)).isoformat()))
        self.sources.write('protection-evidence.json', evidence)
        authority = FormalEntryAuthority(
            self.sources.store(), allow_synthetic=False,
            clock=lambda: NOW)
        preflight.formal_authority = authority
        book = ExecutionBook(
            Path(self.temp.name) / 'real-preflight.sqlite', policy(),
            simulation=False, clock=lambda: NOW,
            current_preflight=preflight, formal_authority=authority)
        self.addCleanup(book.close)
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   inner.public.policy):
            decision = book._current_preflight_decision(
                proposal=True, snapshot=inner.snap(), quote=inner.quote,
                plan=inner.plan, purpose='engineering',
                evidence=inner.evidence)
        self.assertFalse(decision['allowed'])
        self.assertIn('PROVENANCE_UNPROVEN:USER', decision['reasons'])
        formal = decision['components']['formal_channel']
        self.assertEqual(formal['status'], 'blocked')
        self.assertTrue(formal['schema_valid'])
        self.assertFalse(formal['provenance_proven'])


class FormalExecutionConsumptionTests(TestCase):
    """The same ExecutionBook caller consumes the synthetic source chain."""

    def setUp(self):
        self.inner = receipt_module.ReceiptBindingTests(
            'test_valid_matching_receipt_binding_is_true_while_source_gaps_block')
        self.inner.setUp()
        self.addCleanup(self.inner.doCleanups)
        self.sources = SourceFixture(Path(self.inner.temp.name) / 'formal-sources')
        account_key = self.inner.account_key
        self.sources.write('user-order-authorization.json',
                           user_body(account_key=account_key))
        self.sources.write('formal-channel-attestation.json',
                           channel_body(account_key=account_key))
        self.sources.write('protection-attestation.json',
                           protection_body(account_key=account_key))
        self.preflight = self.inner.preflight
        self.authority = FormalEntryAuthority(
            self.sources.store(), allow_synthetic=True, clock=lambda: NOW)
        self.preflight.formal_authority = self.authority
        self.book = ExecutionBook(
            Path(self.inner.temp.name) / 'formal-book.sqlite', policy(),
            simulation=True, clock=lambda: NOW,
            current_preflight=self.preflight,
            formal_authority=self.authority)
        self.addCleanup(self.book.close)

        original_proposal = self.preflight.check_proposal
        original_claim = self.preflight.check_claim

        def allowed_proposal(*args, **kwargs):
            value = original_proposal(*args, **kwargs)
            if (value.get('components', {}).get('formal_channel', {})
                    .get('status') != 'allowed'):
                return value
            return {**value, 'allowed': True, 'reasons': []}

        def allowed_claim(*args, **kwargs):
            value = original_claim(*args, **kwargs)
            if (value.get('components', {}).get('formal_channel', {})
                    .get('status') != 'allowed'):
                return value
            return {**value, 'allowed': True, 'reasons': []}

        self.preflight.check_proposal = allowed_proposal
        self.preflight.check_claim = allowed_claim

    def ingest(self):
        snapshot = self.inner.snap()
        self.book.reconcile(snapshot)
        journal = BrokerJournal(self.book.db_path, allow_synthetic=True)
        try:
            journal.ingest(snapshot)
        finally:
            journal.close()

    def propose(self):
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.inner.public.policy):
            result = self.book.propose(
                'engineering-formal-source', self.inner.plan, self.inner.quote,
                self.inner.snap(), purpose='engineering',
                evidence={**self.inner.evidence,
                          'account_key': self.inner.account_key})
        self.ingest()
        return result

    def propose_again(self, signal='engineering-formal-source', *,
                      plan=None, evidence=None):
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.inner.public.policy):
            return self.book.propose(
                signal, plan or self.inner.plan, self.inner.quote,
                self.inner.snap(), purpose='engineering',
                evidence=evidence or {**self.inner.evidence,
                                      'account_key': self.inner.account_key})

    def consumption_count(self):
        exists = self.book.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            ('formal_authorization_consumptions',)).fetchone()
        if not exists:
            return 0
        return self.book.db.execute(
            'SELECT count(*) FROM formal_authorization_consumptions').fetchone()[0]

    def test_same_proposal_is_duplicate_without_second_consumption(self):
        first = self.propose()
        self.assertFalse(first['duplicate'])
        second = self.propose_again()
        self.assertTrue(second['duplicate'])
        self.assertEqual(len([a for a in self.book.actions()
                              if a['kind'] == 'entry']), 1)
        self.assertEqual(self.consumption_count(), 1)
        with self.assertRaisesRegex(
                ValueError, 'FORMAL_AUTHORIZATION_REPLAYED'):
            self.propose_again('engineering-different-signal')
        with self.assertRaisesRegex(
                ValueError, 'FORMAL_USER_PARAMETERS_MISMATCH|'
                            'FORMAL_AUTHORIZATION_REPLAYED'):
            self.propose_again(plan={**self.inner.plan, 'entry_ceiling': '40.5', 'stop': '39.5'})
        with self.assertRaisesRegex(
                ValueError, 'FORMAL_AUTHORIZATION_REPLAYED'):
            self.propose_again(evidence={
                **self.inner.evidence,
                'account_key': self.inner.account_key,
                'source_sha256': 'changed-evidence'})

    def test_restart_same_identity_is_duplicate(self):
        first = self.propose()
        self.assertFalse(first['duplicate'])
        path = self.book.db_path
        self.book.close()
        self.book = ExecutionBook(
            path, policy(), simulation=True, clock=lambda: NOW,
            current_preflight=self.preflight,
            formal_authority=self.authority)
        self.addCleanup(self.book.close)
        second = self.propose_again()
        self.assertTrue(second['duplicate'])
        self.assertEqual(self.consumption_count(), 1)

    def test_failed_transaction_leaves_no_consumption(self):
        original = self.book._check_strategy_validity
        calls = {'count': 0}

        def fail_after_consume(*args, **kwargs):
            calls['count'] += 1
            if calls['count'] == 2:
                raise ValueError('TEST_TRANSACTION_FAILED')
            return original(*args, **kwargs)

        self.book._check_strategy_validity = fail_after_consume
        with self.assertRaisesRegex(ValueError, 'TEST_TRANSACTION_FAILED'):
            self.propose()
        self.assertEqual(self.consumption_count(), 0)
        self.assertEqual([a for a in self.book.actions()
                          if a['kind'] == 'entry'], [])

    def test_propose_and_claim_consume_same_owner_chain(self):
        result = self.propose()
        action = next(item for item in self.book.actions()
                      if item['kind'] == 'entry')
        row = self.book.db.execute(
            'SELECT action_id, binding FROM formal_authorization_consumptions'
        ).fetchone()
        self.assertEqual(row[0], action['id'])
        self.assertIsInstance(row[1], str)
        self.assertEqual(result['action_id'], action['id'])
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.inner.public.policy):
            claimed = self.book.claim()
        self.assertEqual(claimed['action_id'], action['id'])

    def test_native_review_then_source_change_blocks_place(self):
        from martin_agentic.native_operations import NativeOperations
        self.propose()
        action = next(item for item in self.book.actions()
                      if item['kind'] == 'entry')
        native = NativeOperations(self.book)
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.inner.public.policy):
            packet = native.begin(action['id'], 'review', self.inner.snap(),
                                  quote=self.inner.quote)
        self.assertIn('operation_id', packet)
        native.record(packet['operation_id'], {
            'data': {**packet['parameters'], 'order_checks': {},
                     'customer_approval_required': False,
                     'market_data_disclosure': 'fixture',
                     'quote_data': {}}})
        self.sources.write('formal-channel-attestation.json',
                           channel_body(**{
                               'account_key': self.inner.account_key,
                               'place_contract_sha256': 'e' * 64}))
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.inner.public.policy):
            with self.assertRaisesRegex(
                    ValueError, 'CURRENT_ENGINEERING_PREFLIGHT_BLOCKED|'
                                'FORMAL_AUTHORIZATION_ACTION_MISMATCH|'
                                'FORMAL_SOURCE'):
                native.begin(action['id'], 'place', self.inner.snap(),
                             quote=self.inner.quote)
        self.assertEqual(self.book.db.execute(
            "SELECT count(*) FROM external_operations WHERE operation='place'"
        ).fetchone()[0], 0)

    def test_native_final_transaction_rechecks_changed_source(self):
        from martin_agentic.native_operations import NativeOperations
        self.propose()
        action = next(item for item in self.book.actions()
                      if item['kind'] == 'entry')
        native = NativeOperations(self.book)
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.inner.public.policy):
            packet = native.begin(action['id'], 'review', self.inner.snap(),
                                  quote=self.inner.quote)
        native.record(packet['operation_id'], {
            'data': {**packet['parameters'], 'order_checks': {},
                     'customer_approval_required': False,
                     'market_data_disclosure': 'fixture',
                     'quote_data': {}}})
        original = self.book._native_entry_authorization

        def mutate_after_prelock(action_value, snapshot, operation):
            guard = original(action_value, snapshot, operation)
            if operation == 'place':
                self.sources.write(
                    'formal-channel-attestation.json',
                    channel_body(account_key=self.inner.account_key,
                                 place_contract_sha256='e' * 64))
            return guard

        self.book._native_entry_authorization = mutate_after_prelock
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.inner.public.policy):
            with self.assertRaisesRegex(
                    ValueError, 'CURRENT_ENGINEERING_PREFLIGHT_BINDING_CHANGED'):
                native.begin(action['id'], 'place', self.inner.snap(),
                             quote=self.inner.quote)
        self.assertEqual(self.book.db.execute(
            "SELECT count(*) FROM external_operations WHERE operation='place'"
        ).fetchone()[0], 0)
        self.assertEqual(self.book.db.execute(
            "SELECT count(*) FROM native_operation_records"
            " WHERE operation='place' AND state='STARTED'"
        ).fetchone()[0], 0)
