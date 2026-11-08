from datetime import datetime, timedelta, timezone
import json
from unittest import TestCase
from unittest.mock import patch

from martin_agentic.audit import digest
from martin_agentic.engineering_source import EngineeringSourceVerifier
from martin_agentic.qualification import source_fingerprint

NOW = datetime(2026, 10, 16, 14, 0, tzinfo=timezone.utc)
HEX = 'a' * 64


class _DB:
    def __init__(self, row):
        self.row = row

    def execute(self, *_args, **_kwargs):
        return self

    def fetchone(self):
        return self.row


class _Risk:
    def state(self):
        return {'trading_date': '2026-10-16', 'daily_pnl': '0'}


class Book:
    def __init__(self, *, action_id='action-1'):
        self.simulation = False
        self.policy = {'account_suffix': '0000',
                       'current_capture': {'root': '/tmp/capture',
                                           'public_manifest': '/tmp/public/manifest.json'}}
        self.risk_guard = _Risk()
        self._action = {'id': action_id, 'state': 'REVIEW_PENDING',
                        'payload': json.dumps({'plan': {'symbol': 'DEMO'}},
                                              sort_keys=True)}
        review_result = {
            'review_verified': True, 'response_usable': True,
            'response_hash': HEX, 'recorded_at': NOW.isoformat(),
            'order_checks': {'ok': True},
            'customer_approval_required': True,
            'customer_approval_required_reason': 'native',
            'requires_native_confirmation': True,
            'market_data_disclosure': 'delayed',
        }
        payload = {'action_id': action_id, 'operation': 'review',
                   'tool': 'review_equity_order', 'parameters': {'symbol': 'DEMO'}}
        self.db = _DB((HEX, action_id, 'review', json.dumps(payload),
                       json.dumps(review_result), 'RECORDED'))
        self._facts = {'assessment_id': 'f' * 64, 'observation_valid': True,
                       'observation_valid_until': '2026-10-16T14:00:30+00:00',
                       'binding': {'capture_generation': 'g' * 64}}
        self._review = review_result
        self._payload = payload

    def actions(self):
        return [self._action]

    def current_account_facts(self, *, purpose):
        assert purpose == 'engineering'
        return self._facts

    def review_result(self):
        return self._review

class EngineeringSourceTests(TestCase):
    def test_real_assess_requires_explicit_activation_gate(self):
        from martin_agentic.supervised_engineering import (
            SupervisedEngineeringAuthority)
        class RealBook:
            simulation = False
        authority = SupervisedEngineeringAuthority(
            '/tmp/058-owner-unused', clock=lambda: NOW,
            real_source_verifier=EngineeringSourceVerifier(clock=lambda: NOW))
        decision = authority.assess(
            book=RealBook(), mode='claim', action={'id': 'action-1'})
        self.assertFalse(decision['allowed'])
        self.assertEqual(decision['reasons'],
                         ['SUPERVISED_ENGINEERING_REAL_ACTIVATION_DISABLED'])
        self.assertFalse(decision['sanitized']['execution_authorized'])

    def result(self, book):
        permit = {
            'permit_id': 'p' * 64, 'request_sha256': 'c' * 64,
            'action_id': 'action-1', 'ref_id': 'ref-1',
            'purpose': 'engineering', 'account_key': 'a' * 64,
            'account_suffix': '0000',
            'source': {'policy_sha256': 'b' * 64,
                       'source_fingerprint': source_fingerprint(),
                       'capture_generation': 'g' * 64},
            'facts': {'assessment_id': 'f' * 64,
                      'capture_generation': 'g' * 64},
            'formal': {'review_operation_id': HEX,
                       'review_response_hash': HEX,
                       'review_parameters_sha256': digest(book._payload['parameters']),
                       'order_checks_sha256': digest(book._review['order_checks']),
                       'review_recorded_at': NOW.isoformat()},
            'valid_until': '2026-10-16T14:01:00+00:00',
            'one_shot_id': 'one-1',
        }
        request = {'request_id': 'request-1',
                   'request_sha256': 'c' * 64}
        return {'permit': permit, 'request': request,
                'action': {'id': 'action-1'}}

    def test_missing_owner_provenance_and_risk_lineage_block(self):
        verifier = EngineeringSourceVerifier(clock=lambda: NOW)
        book = Book()
        with patch('martin_agentic.engineering_source.CurrentContextRegistry') as registry:
            registry.return_value.read.return_value = {
                'generation_id': 'g' * 64,
                'source_fingerprint': source_fingerprint(),
                'account_key_hash': 'a' * 64,
                'policy_sha256': 'b' * 64,
            }
            evidence = verifier.verify(book, self.result(book))
        self.assertFalse(evidence['verified'])
        self.assertIn('ENGINEERING_SOURCE_OWNER_PROVENANCE_MISSING',
                      evidence['reasons'])

    def test_bound_owner_records_produce_digest_evidence(self):
        book = Book()
        provenance_record = {'request_id': 'request-1',
                             'action_id': 'action-1',
                             'request_sha256': 'c' * 64}
        risk = {'status': 'bound', 'lineage_id': 'lineage-1',
                'risk_state_sha256': digest(book.risk_guard.state()),
                'account_key': 'a' * 64, 'purpose': 'engineering',
                'source': 'owner-risk-lineage-v1',
                'observed_at': NOW.isoformat(),
                'valid_until': '2026-10-16T14:00:30+00:00'}
        verifier = EngineeringSourceVerifier(
            clock=lambda: NOW,
            provenance_reader=lambda _book, _result: {
                'kind': 'host_owner_request', 'record': provenance_record,
                'record_sha256': digest(provenance_record)},
            risk_lineage_reader=lambda _book, _result: risk)
        with patch('martin_agentic.engineering_source.CurrentContextRegistry') as registry:
            registry.return_value.read.return_value = {
                'generation_id': 'g' * 64,
                'source_fingerprint': source_fingerprint(),
                'account_key_hash': 'a' * 64,
                'policy_sha256': 'b' * 64,
            }
            evidence = verifier.verify(book, self.result(book))
        self.assertTrue(evidence['verified'], evidence)
        self.assertEqual(evidence['status'], 'verified')
        self.assertEqual(evidence['provenance_kind'], 'host_owner_request')
        self.assertEqual(evidence['source_sha256'], digest(
            {key: value for key, value in evidence.items()
             if key not in {'verified', 'status', 'reasons', 'source_sha256'}}))
        self.assertFalse(evidence['entry_authorized'])
        self.assertFalse(evidence['execution_authorized'])


class EngineeringBindingTests(TestCase):
    def _bound(self, book, *, provenance=None, risk=None, registry=None):
        provenance_record = provenance or {'request_id': 'request-1',
                                            'action_id': 'action-1',
                                            'request_sha256': 'c' * 64}
        risk_value = risk or {
            'status': 'bound', 'lineage_id': 'lineage-1',
            'risk_state_sha256': digest(book.risk_guard.state()),
            'account_key': 'a' * 64, 'purpose': 'engineering',
            'source': 'owner-risk-lineage-v1',
            'observed_at': NOW.isoformat(),
            'valid_until': '2026-10-16T14:00:30+00:00'}
        registry_value = registry or {
            'generation_id': 'g' * 64,
            'source_fingerprint': source_fingerprint(),
            'account_key_hash': 'a' * 64,
            'policy_sha256': 'b' * 64}
        verifier = EngineeringSourceVerifier(
            clock=lambda: NOW,
            provenance_reader=lambda _book, _result: {
                'kind': 'host_owner_request', 'record': provenance_record,
                'record_sha256': digest(provenance_record)},
            risk_lineage_reader=lambda _book, _result: risk_value)
        with patch('martin_agentic.engineering_source.CurrentContextRegistry') as patched:
            patched.return_value.read.return_value = registry_value
            return verifier.verify(book, EngineeringSourceTests().result(book))

    def test_source_hash_replacement_is_rejected(self):
        book = Book()
        evidence = self._bound(book, provenance={
            'request_id': 'request-1', 'action_id': 'action-1',
            'request_sha256': 'WRONG_REQUEST_HASH'})
        self.assertFalse(evidence['verified'])

    def test_risk_window_and_source_are_enforced(self):
        book = Book()
        for changes in (
                {'observed_at': '2026-10-14T14:00:00+00:00',
                 'valid_until': '2026-10-15T14:00:00+00:00',
                 'source': 'unrecognized'},
                {'observed_at': '2026-10-17T14:00:00+00:00'}):
            risk = {'status': 'bound', 'lineage_id': 'lineage-1',
                    'risk_state_sha256': digest(book.risk_guard.state()),
                    'account_key': 'a' * 64, 'purpose': 'engineering',
                    'source': 'owner-risk-lineage-v1',
                    'observed_at': NOW.isoformat(),
                    'valid_until': '2026-10-16T14:00:30+00:00'}
            risk.update(changes)
            self.assertFalse(self._bound(book, risk=risk)['verified'])

    def test_registry_generation_must_match_permit_and_facts(self):
        book = Book()
        evidence = self._bound(book, registry={
            'generation_id': 'h' * 64,
            'source_fingerprint': source_fingerprint(),
            'account_key_hash': 'a' * 64,
            'policy_sha256': 'b' * 64})
        self.assertFalse(evidence['verified'])

    def test_consumer_rejects_tampered_evidence(self):
        from martin_agentic import supervised_engineering as se
        book = Book()
        evidence = self._bound(book)
        self.assertTrue(evidence['verified'], evidence)
        result = EngineeringSourceTests().result(book)
        with patch.object(se, '_review_record', return_value={
                'operation_id': HEX, 'response_hash': HEX,
                'parameters_sha256': digest(book._payload['parameters']),
                'order_checks_sha256': digest(book._review['order_checks']),
                'recorded_at': NOW}):
            bad = dict(evidence)
            bad['source_sha256'] = '0' * 64
            with self.assertRaises(se.SupervisedEngineeringError):
                se._validate_source_evidence(
                    bad, result={**result,
                                 'facts': {'assessment_id': 'f' * 64},
                                 'review': {'recorded_at': NOW}},
                    book=book, now=NOW)

    def test_owner_configure_entry_keeps_activation_closed(self):
        import tempfile
        from pathlib import Path as _Path
        from martin_agentic.execution import ExecutionBook
        from martin_agentic.engineering_source import build_owner_engineering_authority
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        policy = {'account_suffix': '0000', 'leverage_allowed': False,
                  'overnight_allowed': False, 'session_date': '2026-10-16',
                  'account_capital_usd': '500',
                  'max_planned_loss_per_trade_usd': '2.50',
                  'max_position_notional_usd': '100',
                  'stop_new_entries_at_daily_equity_loss_usd': '10',
                  'project_max_drawdown_fraction': '0.30'}
        book = ExecutionBook(_Path(temp.name) / 'book.sqlite', policy,
                             simulation=True, clock=lambda: NOW)
        self.addCleanup(book.close)
        authority = book.configure_engineering_source(
            _Path(temp.name) / 'owner', provenance_reader=None,
            risk_lineage_reader=None)
        self.assertIs(book.supervised_engineering_authority, authority)
        self.assertFalse(authority.real_activation_enabled)
        self.assertIsNotNone(authority.real_source_verifier)

    def _consumer_result(self, book):
        result = EngineeringSourceTests().result(book)
        result['request']['valid_until'] = (
            NOW + __import__('datetime').timedelta(seconds=120)).isoformat()
        result['facts'] = {'assessment_id': 'f' * 64,
                           'observation_valid_until':
                           (NOW + __import__('datetime').timedelta(
                               seconds=30)).isoformat()}
        result['review'] = {'recorded_at': NOW}
        result['action'] = book.actions()[0]
        return result

    def test_consumer_valid_intersection_and_expiry_direction(self):
        from martin_agentic import supervised_engineering as se
        from datetime import timedelta
        book = Book()
        evidence = self._bound(book)
        result = self._consumer_result(book)
        # risk 60, facts/review 30, evidence 30 must pass.
        risk = dict(evidence)
        risk['risk_valid_until'] = (NOW + timedelta(seconds=60)).isoformat()
        risk['source_sha256'] = digest({k: v for k, v in risk.items()
            if k not in {'verified', 'status', 'reasons', 'source_sha256'}})
        se._validate_source_evidence(risk, result=result, book=book, now=NOW)
        # evidence 120 while facts/review are 30 must fail even with risk 60.
        extended = dict(risk)
        extended['valid_until'] = (NOW + timedelta(seconds=120)).isoformat()
        extended['source_sha256'] = digest({k: v for k, v in extended.items()
            if k not in {'verified', 'status', 'reasons', 'source_sha256'}})
        with self.assertRaises(se.SupervisedEngineeringError):
            se._validate_source_evidence(
                extended, result=result, book=book,
                now=NOW + timedelta(seconds=40))

    def test_consumer_uses_post_verification_clock(self):
        from martin_agentic import supervised_engineering as se
        from datetime import timedelta
        book = Book()
        evidence = self._bound(book)
        result = self._consumer_result(book)
        later = dict(evidence)
        later['observed_at'] = (NOW + timedelta(microseconds=1)).isoformat()
        later['source_sha256'] = digest({k: v for k, v in later.items()
            if k not in {'verified', 'status', 'reasons', 'source_sha256'}})
        with self.assertRaises(se.SupervisedEngineeringError):
            se._validate_source_evidence(later, result=result, book=book, now=NOW)
        se._validate_source_evidence(
            later, result=result, book=book, now=NOW + timedelta(microseconds=1))


class RealDryFixture:
    """Reusable real-branch dry fixture: actual ExecutionBook, NativeOperations,
    FormalSourceStore, CurrentContextRegistry and facts reader.

    ``simulation=False`` and ``allow_synthetic=False``; the permit is a local
    owner fixture, identical in shape to the production contract.  Nothing in
    this class talks to a broker, market or formal MCP service.
    """

    def __init__(self, *, now=NOW, confirmation_checks=None,
                 facts_valid_seconds=30, lineage=False,
                 prebuilt_intent=True, activate=False, profile=None):
        import tempfile
        from pathlib import Path
        from test_supervised_engineering import (
            policy as _policy, plan as _plan, quote as _quote,
            assessment as _assessment, request as _request)
        from test_broker import snapshot as _snapshot

        self._temp = tempfile.TemporaryDirectory()
        self.root = Path(self._temp.name)
        self.owner = self.root / 'owner'
        self.capture = self.root / 'capture'
        self.public = self.root / 'public'
        for directory in (self.owner, self.capture, self.public):
            directory.mkdir(mode=0o700)
        self.manifest = self.public / 'manifest.json'
        self.manifest.write_text('{}')
        self.policy = _policy()
        if profile is not None:
            self.policy['supervised_engineering'] = {
                **profile, 'owner_root': str(self.owner)}
        self.account_key = _snapshot()['account_key']
        self.generation = 'd1' * 32
        self.policy['current_capture'] = {
            'root': str(self.capture), 'public_manifest': str(self.manifest),
            'producer': '058r4c2-isolated-fixture', 'retention': 3}
        self.registry = {
            'schema_version': 1, 'kind': 'current_capture_registry_v1',
            'generation_id': self.generation, 'capture_root': str(self.capture),
            'capture_manifest_sha256': 'a' * 64, 'raw_chain_sha256': 'b' * 64,
            'public_manifest': str(self.manifest),
            'public_manifest_sha256': 'c' * 64,
            'account_key_hash': self.account_key, 'account_suffix': '0000',
            'policy_sha256': digest(self.policy),
            'source_fingerprint': source_fingerprint(),
            'published_at': NOW.isoformat(),
            'producer': '058r4c2-isolated-fixture'}
        self.write_registry(self.registry)
        self.facts = _assessment(self.policy, self.account_key,
                                 valid_seconds=facts_valid_seconds)
        self.facts['binding']['capture_generation'] = self.generation
        self.facts['binding']['expected_capture_generation'] = self.generation
        self.facts['binding']['policy_sha256'] = digest(self.policy)
        self.facts['binding']['source_fingerprint'] = source_fingerprint()
        self.facts['assessment_id'] = digest({
            key: value for key, value in self.facts.items()
            if key != 'assessment_id'})
        self.snap = _snapshot()
        self.snap['synthetic'] = False
        self._plan = _plan
        self._quote = _quote
        self._request = _request
        self._confirmation_checks = confirmation_checks
        self._lineage = lineage
        self._prebuilt_intent = prebuilt_intent
        self._activate = activate is True
        request_value = _request(self.policy, self.account_key,
                                 synthetic=False,
                                 signal_id='engineering-real-dry')
        request_value['source']['capture_generation'] = self.generation
        request_value['source']['policy_sha256'] = digest(self.policy)
        request_value['source']['source_fingerprint'] = source_fingerprint()
        self.write_request(request_value)
        self._clock_value = [now]
        self.provenance_fault = None
        self.book = None
        self._build()

    # --- owner files -----------------------------------------------------
    def write_request(self, value):
        self._owner_bytes('supervised-engineering-request.json',
                          json.dumps(value, sort_keys=True).encode())

    def write_permit(self, value=None):
        if value is None:
            from martin_agentic.supervised_engineering import build_permit
            action = self.book.actions()[0]
            value = build_permit(
                json.loads((self.owner /
                            'supervised-engineering-request.json').read_text()),
                {'operation_id': self.packet['operation_id'],
                 'action_id': self.action_id,
                 'parameters': self.packet['parameters'],
                 'result': self.review},
                self.facts, account_key=self.account_key,
                account_suffix='0000', policy=self.policy,
                fingerprint=source_fingerprint(), action_id=self.action_id,
                ref_id=json.loads(action['payload'])['ref_id'],
                now=self._clock_value[0], one_shot_id='real-dry-1',
                valid_until=(self._clock_value[0] + timedelta(
                    seconds=20)).isoformat())
        self._owner_bytes('supervised-engineering-permit.json',
                          json.dumps(value, sort_keys=True).encode())

    def _owner_bytes(self, name, value):
        path = self.owner / name
        path.write_bytes(value)
        self._os.chmod(path, 0o600)

    def read_permit(self):
        return json.loads(
            (self.owner / 'supervised-engineering-permit.json').read_text())

    def revoke_permit(self):
        value = self.read_permit()
        value['revoked_at'] = NOW.isoformat()
        value['permit_id'] = digest({**value, 'permit_id': None})
        self.write_permit(value)

    def request_bytes(self):
        return (self.owner /
                'supervised-engineering-request.json').read_bytes()

    def permit_bytes(self):
        return (self.owner /
                'supervised-engineering-permit.json').read_bytes()

    # --- execution book --------------------------------------------------
    @property
    def _os(self):
        import os
        return os

    def _clock(self):
        return self._clock_value[0]

    def advance(self, seconds):
        self._clock_value[0] = self._clock_value[0] + timedelta(seconds=seconds)

    def _build(self):
        from martin_agentic.execution import ExecutionBook
        from martin_agentic.native_operations import NativeOperations
        from test_supervised_engineering import FakePreflight
        self.book = ExecutionBook(
            self.root / 'book.sqlite', self.policy, simulation=False,
            clock=self._clock, current_preflight=FakePreflight(self.facts))
        if self._lineage:
            from martin_agentic.broker import BrokerJournal
            from martin_agentic.session_lineage import (
                bootstrap_session_lineage)
            path = self.root / 'book.sqlite'
            journal = BrokerJournal(path)
            try:
                journal.ingest(self.snap)
            finally:
                journal.close()
            self.book.reconcile(self.snap)
            from unittest.mock import patch as _patch
            with _patch('martin_agentic.session_lineage.CANONICAL_RUNTIME',
                        self.root):
                bootstrap_session_lineage(
                    path, self.policy, current_db=path,
                    current_policy=self.policy, clock=self._clock)
        self.configure()
        if self._prebuilt_intent:
            self.book.record_supervised_engineering_intent(
                'engineering-real-dry', self._plan(), self._quote(), self.snap,
                evidence={'synthetic': False, 'source_sha256': 'fixture',
                          'engineering_scope_authorized': True,
                          'parameters_user_specified': True,
                          'parameters_valid_until': (
                              NOW + timedelta(minutes=30)).isoformat()})
            self.finalize_intent(self.book.actions()[0]['id'])
        else:
            self.ops = NativeOperations(self.book)

    def finalize_intent(self, action_id):
        from martin_agentic.native_operations import NativeOperations
        self.action_id = action_id
        self.ops = NativeOperations(self.book)
        self.packet = self.ops.begin(
            self.action_id, 'review', self.snap, quote=self._quote())
        response = {'data': {
            **self.packet['parameters'],
            'order_checks': (self._confirmation_checks
                             if self._confirmation_checks is not None else {}),
            'customer_approval_required': False,
            'market_data_disclosure': 'Exact disclosure',
            'quote_data': {}}}
        self.review = self.ops.record(self.packet['operation_id'], response)
        self.write_permit()
        self.book.refresh_current_account_assessment(purpose='engineering')

    def adopt_review(self, action_id, packet, review):
        from martin_agentic.native_operations import NativeOperations
        self.action_id = action_id
        self.packet = packet
        self.review = review
        self.ops = NativeOperations(self.book)
        self.write_permit()
        self.book.refresh_current_account_assessment(purpose='engineering')

    def configure(self, *, provenance='default', risk='default'):
        self.provenance_reader = (
            self._default_provenance if provenance == 'default' else provenance)
        self.risk_reader = (
            self._default_risk if risk == 'default' else risk)
        self.book.configure_engineering_source(
            self.owner, provenance_reader=self.provenance_reader,
            risk_lineage_reader=self.risk_reader, allow_synthetic=False,
            real_activation_enabled=self._activate)

    def reopen(self):
        from martin_agentic.execution import ExecutionBook
        from martin_agentic.native_operations import NativeOperations
        from test_supervised_engineering import FakePreflight
        self.book.close()
        self.book = ExecutionBook(
            self.root / 'book.sqlite', self.policy, simulation=False,
            clock=self._clock, current_preflight=FakePreflight(self.facts))
        self.configure()
        self.book.refresh_current_account_assessment(purpose='engineering')
        self.ops = NativeOperations(self.book)

    def close(self):
        if self.book is not None:
            try:
                self.book.close()
            finally:
                self.book = None
        self._temp.cleanup()

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        self.close()
        return False

    # --- readers ---------------------------------------------------------
    def _default_provenance(self, book, result):
        fault = self.provenance_fault
        if fault == 'exception':
            raise RuntimeError('provenance-reader-boom')
        if fault == 'slow':
            self.advance(21)
        elif fault == 'revoke':
            value = self.read_permit()
            value['revoked_at'] = NOW.isoformat()
            self.write_permit(value)
        elif fault == 'permit-delete':
            try:
                (self.owner /
                 'supervised-engineering-permit.json').unlink()
            except FileNotFoundError:
                pass
        elif fault == 'request-mutate':
            value = json.loads(self.request_bytes())
            value['request_id'] = 'request-mutated'
            self.write_request(value)
        elif fault == 'action-payload':
            self.set_action_payload({'risk_terms': {'wrong': True}})
        elif fault == 'action-unknown':
            self.set_action(state='UNKNOWN')
        elif fault == 'facts-body':
            facts = self.book.current_account_facts(purpose='engineering')
            facts['status'] = 'mutated'
        elif fault == 'registry-delete':
            try:
                (self.capture / 'registry.json').unlink()
            except FileNotFoundError:
                pass
        elif fault == 'registry-replace':
            value = dict(self.registry)
            value['generation_id'] = 'e2' * 32
            self.write_registry(value)
        elif fault == 'consumption-insert':
            self.insert_consumption()
        elif fault == 'review-state':
            self.set_review_state('PENDING')
        elif (fault is not None and fault.startswith('review-')):
            self.mutate_review_result(fault[7:])
        raw = result['permit']['permit']
        parsed = result['request']
        record = {'request_id': parsed['request_id'],
                  'action_id': raw['action_id'],
                  'request_sha256': raw['request_sha256']}
        return {'kind': 'host_owner_request', 'record': record,
                'record_sha256': digest(record)}

    def risk_value(self, **overrides):
        value = {
            'status': 'bound', 'lineage_id': 'lineage-fixture',
            'risk_state_sha256': digest(self.book.risk_guard.state()),
            'account_key': self.account_key, 'purpose': 'engineering',
            'source': 'owner-risk-lineage-v1',
            'observed_at': (self._clock() - timedelta(seconds=1)).isoformat(),
            'valid_until': (self._clock() + timedelta(seconds=60)).isoformat()}
        value.update(overrides)
        return value

    def _default_risk(self, book, result):
        if self.provenance_fault == 'risk-exception':
            raise RuntimeError('risk-reader-boom')
        if self.provenance_fault == 'risk-state':
            value = self.risk_value()
            self.set_risk_row({'mutated': True})
            return value
        return self.risk_value()

    # --- injections ------------------------------------------------------
    def write_registry(self, value):
        path = self.capture / 'registry.json'
        path.write_text(json.dumps(value, sort_keys=True))
        self._os.chmod(path, 0o600)

    def delete_registry(self):
        (self.capture / 'registry.json').unlink()

    def set_action(self, *, state=None, payload_value=None):
        action = self.book.actions()[0]
        value = payload_value
        if state is not None:
            self.book.db.execute(
                'UPDATE live_actions SET state=? WHERE id=?',
                (state, self.action_id))
        if value is not None:
            self.book.db.execute(
                'UPDATE live_actions SET payload=? WHERE id=?',
                (json.dumps(value, sort_keys=True), self.action_id))
        self.book.db.commit()

    def set_action_payload(self, changes):
        value = json.loads(self.book.actions()[0]['payload'])
        value.update(changes)
        self.set_action(payload_value=value)

    def set_action_plan(self, changes):
        value = json.loads(self.book.actions()[0]['payload'])
        value['plan'].update(changes)
        self.set_action(payload_value=value)

    def set_action_parameters(self, changes):
        value = json.loads(self.book.actions()[0]['payload'])
        value['parameters'].update(changes)
        self.set_action(payload_value=value)

    def mutate_review_result(self, key):
        row = self.book.db.execute(
            "SELECT id,result FROM native_operation_records "
            "WHERE operation='review' ORDER BY rowid DESC LIMIT 1").fetchone()
        value = json.loads(row[1])
        if key == 'parameters':
            self._mutate_review_parameters()
            return
        if key == 'order-checks':
            value['order_checks'] = {'changed': True}
        elif key == 'approval':
            value['customer_approval_required'] = True
            value['customer_approval_required_reason'] = 'changed'
        elif key == 'reason':
            value['customer_approval_required_reason'] = 'changed'
        elif key == 'confirmation':
            value['requires_native_confirmation'] = False
        elif key == 'disclosure':
            value['market_data_disclosure'] = 'changed'
        elif key == 'time':
            value['recorded_at'] = (NOW + timedelta(seconds=1)).isoformat()
        self.book.db.execute(
            'UPDATE native_operation_records SET result=? WHERE id=?',
            (json.dumps(value, sort_keys=True), row[0]))
        self.book.db.commit()

    def _mutate_review_parameters(self):
        row = self.book.db.execute(
            "SELECT id,payload FROM native_operation_records "
            "WHERE operation='review' ORDER BY rowid DESC LIMIT 1").fetchone()
        value = json.loads(row[1])
        value['parameters']['symbol'] = 'WRONG_SYMBOL'
        self.book.db.execute(
            'UPDATE native_operation_records SET payload=? WHERE id=?',
            (json.dumps(value, sort_keys=True), row[0]))
        self.book.db.commit()

    def set_review_state(self, state):
        self.book.db.execute(
            "UPDATE native_operation_records SET state=? "
            "WHERE operation='review'", (state,))
        self.book.db.commit()

    def mutate_facts(self, changes, *, rehash=True):
        for key, value in changes.items():
            self.facts[key] = value
        if rehash:
            self.facts['assessment_id'] = digest({
                key: value for key, value in self.facts.items()
                if key != 'assessment_id'})
        self.book._current_account_facts['engineering'] = self.facts

    def set_risk_row(self, payload):
        self.book.risk_guard.db.execute(
            'INSERT INTO account_risk VALUES(1,?) '
            'ON CONFLICT(id) DO UPDATE SET payload=excluded.payload',
            (json.dumps(payload, sort_keys=True),))
        self.book.risk_guard.db.commit()

    def insert_consumption(self, one_shot_id='real-dry-1'):
        self.book.db.execute(
            'CREATE TABLE IF NOT EXISTS formal_authorization_consumptions'
            '(one_shot_id TEXT PRIMARY KEY,source_sha256 TEXT NOT NULL,'
            'binding TEXT NOT NULL,action_id TEXT NOT NULL,'
            'consumed_at TEXT NOT NULL)')
        self.book.db.execute(
            'INSERT OR REPLACE INTO formal_authorization_consumptions '
            'VALUES(?,?,?,?,?)',
            (one_shot_id, 'a' * 64, digest(self.read_permit()),
             self.action_id, NOW.isoformat()))
        self.book.db.commit()

    # --- observation -----------------------------------------------------
    def consumptions(self):
        exists = self.book.db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' "
            "AND name='formal_authorization_consumptions'").fetchone()[0]
        if not exists:
            return []
        return self.book.db.execute(
            'SELECT one_shot_id,source_sha256,binding,action_id,consumed_at '
            'FROM formal_authorization_consumptions ORDER BY rowid').fetchall()

    def risk_digest(self):
        try:
            return digest(self.book.risk_guard.state())
        except Exception as exc:
            return type(exc).__name__ + ':' + str(exc)

    def _file_bytes(self, name):
        try:
            return (self.owner / name).read_bytes()
        except FileNotFoundError:
            return b'<missing>'

    def snapshot(self):
        return {
            'request': self._file_bytes(
                'supervised-engineering-request.json'),
            'permit': self._file_bytes(
                'supervised-engineering-permit.json'),
            'actions': self.book.actions(),
            'audit': self.book.db.execute(
                'SELECT event_id,payload FROM audit_events '
                'ORDER BY rowid').fetchall(),
            'consumption': self.consumptions(),
            'risk': self.risk_digest(),
            'facts': digest(self.book.current_account_facts(
                purpose='engineering')),
            'transaction': self.book.db.in_transaction,
        }

    def assert_unchanged(self, before, label=''):
        after = self.snapshot()
        for key in ('request', 'permit', 'actions', 'audit', 'consumption',
                    'risk', 'facts', 'transaction'):
            if after[key] != before[key]:
                raise AssertionError(
                    label + ' dry mutated ' + key + ': '
                    + repr((before[key], after[key])))

    def dry(self):
        return self.book.dry_assess_supervised_engineering(self.action_id)


class RealDryBaselineTests(TestCase):
    def test_baseline_is_non_authorizing_and_dry_makes_no_writes(self):
        with RealDryFixture() as fixture:
            before = fixture.snapshot()
            decision = fixture.dry()
            self.assertTrue(decision['allowed'], decision)
            self.assertEqual(decision['decision_scope'], 'DRY_ASSESSMENT')
            self.assertFalse(decision['entry_authorized'])
            self.assertFalse(decision['execution_authorized'])
            fixture.assert_unchanged(before, 'baseline')
            second = fixture.dry()
            self.assertTrue(second['allowed'], second)
            fixture.assert_unchanged(before, 'repeat-dry')
            self.assertEqual(fixture.consumptions(), [])
            with self.assertRaises(ValueError):
                fixture.book.promote_supervised_entry(fixture.action_id)
            try:
                packet = fixture.ops.begin(
                    fixture.action_id, 'place', fixture.snap,
                    quote=fixture._quote())
            except ValueError:
                pass
            else:
                self.assertFalse(packet['execution_authorized_by_packet'])
                self.assertFalse(packet['specific_order_confirmed'])
                self.assertFalse(packet['broker_effect_verified'])

    def test_review_requiring_confirmation_is_not_execution_permission(self):
        with RealDryFixture(confirmation_checks={'ok': True}) as fixture:
            self.assertTrue(fixture.review['requires_native_confirmation'])
            before = fixture.snapshot()
            decision = fixture.dry()
            self.assertTrue(decision['allowed'], decision)
            self.assertFalse(decision['execution_authorized'])
            self.assertFalse(decision['entry_authorized'])
            self.assertEqual(decision['component']['confirmation_mode'],
                             'native_app_or_explicit_user')
            fixture.assert_unchanged(before, 'confirmation')
            with self.assertRaises(ValueError):
                fixture.book.promote_supervised_entry(fixture.action_id)

    def test_close_and_reopen_keeps_dry_consistent(self):
        with RealDryFixture() as fixture:
            before = fixture.snapshot()
            self.assertTrue(fixture.dry()['allowed'])
            fixture.assert_unchanged(before, 'before-reopen')
            fixture.reopen()
            after = fixture.snapshot()
            self.assertEqual(after['consumption'], [])
            decision = fixture.dry()
            self.assertTrue(decision['allowed'], decision)
            fixture.assert_unchanged(after, 'after-reopen')

    def test_pre_existing_consumption_rejects_replay(self):
        with RealDryFixture() as fixture:
            fixture.insert_consumption()
            before = fixture.snapshot()
            decision = fixture.dry()
            self.assertFalse(decision['allowed'], decision)
            self.assertTrue(any('PERMIT_REPLAYED' in reason
                                for reason in decision['reasons']),
                            decision['reasons'])
            fixture.assert_unchanged(before, 'replay')

    def test_unrelated_consumption_does_not_reject_current_permit(self):
        with RealDryFixture() as fixture:
            fixture.insert_consumption('other-one-shot')
            before = fixture.snapshot()
            decision = fixture.dry()
            self.assertTrue(decision['allowed'], decision)
            fixture.assert_unchanged(before, 'unrelated-consumption')

    def test_component_uses_short_risk_window(self):
        with RealDryFixture() as fixture:
            fixture.configure(risk=lambda book, result: fixture.risk_value(
                valid_until=(NOW + timedelta(seconds=5)).isoformat()))
            before = fixture.snapshot()
            decision = fixture.dry()
            self.assertTrue(decision['allowed'], decision)
            self.assertEqual(decision['component']['expires_at'],
                             (NOW + timedelta(seconds=5)).isoformat())
            self.assertEqual(
                decision['source_evidence']['valid_until'],
                (NOW + timedelta(seconds=5)).isoformat())
            fixture.assert_unchanged(before, 'short-risk')

    def test_component_uses_short_evidence_window(self):
        with RealDryFixture(facts_valid_seconds=8) as fixture:
            before = fixture.snapshot()
            decision = fixture.dry()
            self.assertTrue(decision['allowed'], decision)
            self.assertEqual(decision['component']['expires_at'],
                             (NOW + timedelta(seconds=8)).isoformat())
            self.assertEqual(
                decision['source_evidence']['valid_until'],
                (NOW + timedelta(seconds=8)).isoformat())
            fixture.assert_unchanged(before, 'short-evidence')


class RealDryLifecycleTests(TestCase):
    def test_pending_state_is_dry_allowed_but_not_execution(self):
        with RealDryFixture() as fixture:
            fixture.set_action(state='REVIEW_PENDING')
            before = fixture.snapshot()
            decision = fixture.dry()
            self.assertTrue(decision['allowed'], decision)
            self.assertFalse(decision['execution_authorized'])
            fixture.assert_unchanged(before, 'pending')

    def test_declined_and_unknown_states_block(self):
        for state in ('APPROVAL_DECLINED', 'APPROVAL_EXPIRED', 'UNKNOWN'):
            with self.subTest(state=state):
                with RealDryFixture() as fixture:
                    fixture.set_action(state=state)
                    before = fixture.snapshot()
                    decision = fixture.dry()
                    self.assertFalse(decision['allowed'], (state, decision))
                    fixture.assert_unchanged(before, state)


class RealDryPreflightMatrixTests(TestCase):
    def _blocked(self, fixture, expected=None):
        before = fixture.snapshot()
        decision = fixture.dry()
        self.assertFalse(decision['allowed'], decision)
        if expected is not None:
            self.assertTrue(any(expected in reason
                                for reason in decision['reasons']),
                            (expected, decision['reasons']))
        fixture.assert_unchanged(before, expected or 'preflight')

    def test_provenance_and_risk_reader_gaps(self):
        with RealDryFixture() as fixture:
            fixture.configure(provenance=None)
            self._blocked(fixture, 'OWNER_PROVENANCE_MISSING')
        with RealDryFixture() as fixture:
            fixture.provenance_fault = 'exception'
            fixture.configure()
            self._blocked(fixture, 'OWNER_PROVENANCE_MISSING')
        with RealDryFixture() as fixture:
            fixture.configure(risk=None)
            self._blocked(fixture, 'RISK_LINEAGE_MISSING')
        with RealDryFixture() as fixture:
            def raising_risk(_book, _result):
                raise RuntimeError('risk-reader-boom')
            fixture.configure(risk=raising_risk)
            self._blocked(fixture, 'RISK_LINEAGE_MISSING')

    def test_owner_file_replace_revoke_delete(self):
        with RealDryFixture() as fixture:
            value = json.loads(fixture.request_bytes())
            value['request_id'] = 'request-replaced'
            fixture.write_request(value)
            self._blocked(fixture)
        with RealDryFixture() as fixture:
            (fixture.owner /
             'supervised-engineering-request.json').unlink()
            self._blocked(fixture)
        with RealDryFixture() as fixture:
            value = fixture.read_permit()
            value['request_id'] = 'request-replaced'
            fixture.write_permit(value)
            self._blocked(fixture, 'PERMIT')
        with RealDryFixture() as fixture:
            fixture.revoke_permit()
            self._blocked(fixture, 'PERMIT_REVOKED')
        with RealDryFixture() as fixture:
            (fixture.owner /
             'supervised-engineering-permit.json').unlink()
            self._blocked(fixture, 'PERMIT')

    def test_action_ref_order_account_and_phase(self):
        with RealDryFixture() as fixture:
            fixture.set_action_payload({'ref_id': 'wrong-ref'})
            self._blocked(fixture, 'ACTION')
        with RealDryFixture() as fixture:
            fixture.set_action_plan({'quantity': 99})
            self._blocked(fixture, 'ORDER')
        with RealDryFixture() as fixture:
            fixture.set_action_payload({'account': '••••9999'})
            self.assertNotEqual(
                '••••9999', '••••' + fixture.book.policy['account_suffix'])
            self._blocked(fixture, 'BINDING')
        with RealDryFixture() as fixture:
            fixture.set_action(state='UNKNOWN')
            self._blocked(fixture, 'STATE')

    def test_review_record_changes(self):
        cases = (
            ('state',  lambda f: f.set_review_state('PENDING'), 'REVIEW'),
            ('parameters', lambda f: f.mutate_review_result('parameters'),
             'REVIEW'),
            ('order-checks', lambda f: f.mutate_review_result('order-checks'),
             'REVIEW'),
            ('approval', lambda f: f.mutate_review_result('approval'),
             'REVIEW'),
            ('reason', lambda f: f.mutate_review_result('reason'),
             'REVIEW'),
            ('confirmation', lambda f: f.mutate_review_result('confirmation'),
             'REVIEW'),
            ('disclosure', lambda f: f.mutate_review_result('disclosure'),
             'REVIEW'),
            ('time', lambda f: f.mutate_review_result('time'), 'REVIEW'),
        )
        for label, mutate, expected in cases:
            with self.subTest(label=label):
                with RealDryFixture() as fixture:
                    mutate(fixture)
                    self._blocked(fixture, expected)

    def test_facts_body_generation_expiry_future(self):
        with RealDryFixture() as fixture:
            fixture.mutate_facts({'status': 'mutated'}, rehash=False)
            self._blocked(fixture, 'FACTS')
        with RealDryFixture() as fixture:
            fixture.mutate_facts({'observation_valid_until': (
                NOW - timedelta(seconds=1)).isoformat()})
            self._blocked(fixture, 'FACTS')
        with RealDryFixture() as fixture:
            fixture.mutate_facts({'observed_at': (
                NOW + timedelta(seconds=1)).isoformat()})
            self._blocked(fixture, 'FACTS')
        with RealDryFixture() as fixture:
            binding = dict(fixture.facts['binding'])
            binding['capture_generation'] = 'e2' * 32
            fixture.facts['binding'] = binding
            fixture.facts['assessment_id'] = digest({
                key: value for key, value in fixture.facts.items()
                if key != 'assessment_id'})
            fixture.book._current_account_facts['engineering'] = fixture.facts
            self._blocked(fixture, 'FACTS')

    def test_registry_replace_delete_invalid(self):
        with RealDryFixture() as fixture:
            value = dict(fixture.registry)
            value['generation_id'] = 'e2' * 32
            fixture.write_registry(value)
            self._blocked(fixture, 'CAPTURE')
        with RealDryFixture() as fixture:
            fixture.delete_registry()
            self._blocked(fixture, 'CAPTURE')
        with RealDryFixture() as fixture:
            (fixture.capture / 'registry.json').write_text('{invalid')
            self._blocked(fixture, 'CAPTURE')

    def test_risk_source_window_and_state(self):
        with RealDryFixture() as fixture:
            fixture.configure(risk=lambda book, result: fixture.risk_value(
                source='unrecognized'))
            self._blocked(fixture, 'RISK')
        with RealDryFixture() as fixture:
            fixture.configure(risk=lambda book, result: fixture.risk_value(
                observed_at=(NOW - timedelta(seconds=120)).isoformat(),
                valid_until=(NOW - timedelta(seconds=60)).isoformat()))
            self._blocked(fixture, 'RISK')
        with RealDryFixture() as fixture:
            fixture.configure(risk=lambda book, result: fixture.risk_value(
                observed_at=(NOW + timedelta(seconds=5)).isoformat()))
            self._blocked(fixture, 'RISK')
        with RealDryFixture() as fixture:
            fixture.configure(risk=lambda book, result: fixture.risk_value(
                risk_state_sha256='f' * 64))
            self._blocked(fixture, 'RISK')

    def test_clock_normal_expiry_future_and_slow_reader(self):
        with RealDryFixture() as fixture:
            before = fixture.snapshot()
            self.assertTrue(fixture.dry()['allowed'])
            fixture.assert_unchanged(before, 'clock-normal')
        with RealDryFixture() as fixture:
            fixture.advance(21)
            self._blocked(fixture)
        with RealDryFixture() as fixture:
            fixture._clock_value[0] = NOW - timedelta(seconds=1)
            self._blocked(fixture)
        with RealDryFixture() as fixture:
            fixture.provenance_fault = 'slow'
            before = fixture.snapshot()
            decision = fixture.dry()
            self.assertFalse(decision['allowed'], decision)
            fixture.assert_unchanged(before, 'slow-reader')


class RealDryToctouTests(TestCase):
    """Owner changes injected while the verifier readers are running.

    The injection is the only allowed write; the dry evaluation must still
    leave audit, consumption, transaction and every non-injected owner field
    untouched.
    """

    def _blocked(self, fixture, fault, expected=None, check=None, skip=()):
        fixture.provenance_fault = fault
        before = fixture.snapshot()
        decision = fixture.dry()
        self.assertFalse(decision['allowed'], (fault, decision))
        if expected is not None:
            self.assertTrue(any(expected in reason
                                for reason in decision['reasons']),
                            (fault, expected, decision['reasons']))
        after = fixture.snapshot()
        for key in ('request', 'permit', 'actions', 'audit', 'consumption',
                    'risk', 'facts', 'transaction'):
            if key in skip:
                continue
            self.assertEqual(after[key], before[key], (fault, key))
        if check is not None:
            check(fixture)
        return decision

    def test_permit_revoke_and_delete(self):
        with RealDryFixture() as fixture:
            self._blocked(
                fixture, 'revoke', 'PERMIT', skip=('permit',), check=
                lambda f: self.assertIsNotNone(f.read_permit()['revoked_at']))
        with RealDryFixture() as fixture:
            self._blocked(
                fixture, 'permit-delete', 'PERMIT', skip=('permit',), check=
                lambda f: self.assertFalse(
                    (f.owner /
                     'supervised-engineering-permit.json').exists()))

    def test_request_and_action_changes(self):
        with RealDryFixture() as fixture:
            self._blocked(
                fixture, 'request-mutate', 'OWNER_REQUEST', skip=('request',), check=
                lambda f: self.assertEqual(
                    json.loads(f.request_bytes())['request_id'],
                    'request-mutated'))
        with RealDryFixture() as fixture:
            self._blocked(
                fixture, 'action-payload', 'OWNER_ACTION', skip=('actions',), check=
                lambda f: self.assertIn(
                    'wrong', json.loads(f.book.actions()[0]['payload'])
                    ['risk_terms']))
        with RealDryFixture() as fixture:
            self._blocked(
                fixture, 'action-unknown', 'ACTION', skip=('actions',), check=
                lambda f: self.assertEqual(f.book.actions()[0]['state'],
                                           'UNKNOWN'))

    def test_facts_review_registry_and_risk_changes(self):
        with RealDryFixture() as fixture:
            self._blocked(
                fixture, 'facts-body', 'OWNER_FACTS', skip=('facts',), check=
                lambda f: self.assertEqual(
                    f.book.current_account_facts(
                        purpose='engineering')['status'], 'mutated'))
        for fault, expected in (
                ('review-state', 'REVIEW_REQUIRED'),
                ('review-parameters', 'OWNER_REVIEW'),
                ('review-order-checks', 'OWNER_REVIEW'),
                ('review-approval', 'OWNER_REVIEW'),
                ('review-reason', 'OWNER_REVIEW'),
                ('review-confirmation', 'OWNER_REVIEW'),
                ('review-disclosure', 'OWNER_REVIEW'),
                ('review-time', 'OWNER_REVIEW')):
            with self.subTest(fault=fault):
                with RealDryFixture() as fixture:
                    self._blocked(fixture, fault, expected)
        with RealDryFixture() as fixture:
            self._blocked(
                fixture, 'registry-delete', 'CAPTURE', check=
                lambda f: self.assertFalse(
                    (f.capture / 'registry.json').exists()))
        with RealDryFixture() as fixture:
            self._blocked(
                fixture, 'registry-replace', 'CAPTURE', check=
                lambda f: self.assertEqual(
                    json.loads((f.capture / 'registry.json').read_text())
                    ['generation_id'], 'e2' * 32))
        with RealDryFixture() as fixture:
            self._blocked(fixture, 'risk-state', 'RISK', skip=('risk',))

    def test_consumption_inserted_during_reader_blocks(self):
        with RealDryFixture() as fixture:
            self._blocked(fixture, 'consumption-insert', 'PERMIT_REPLAYED',
                          skip=('consumption',),
                          check=lambda f: self.assertEqual(
                              len(f.consumptions()), 1))

    def test_consumption_read_failure_blocks(self):
        from unittest.mock import patch
        from martin_agentic import supervised_engineering as se
        with RealDryFixture() as fixture:
            real = se._consumption
            calls = {'count': 0}

            def flaky(book, one_shot_id):
                calls['count'] += 1
                if calls['count'] >= 2:
                    raise se.SupervisedEngineeringError(
                        'SUPERVISED_ENGINEERING_CONSUMPTION_LEDGER_UNAVAILABLE')
                return real(book, one_shot_id)

            before = fixture.snapshot()
            with patch.object(se, '_consumption', flaky):
                decision = fixture.dry()
            self.assertFalse(decision['allowed'], decision)
            self.assertIn(
                'SUPERVISED_ENGINEERING_CONSUMPTION_LEDGER_UNAVAILABLE',
                decision['reasons'])
            fixture.assert_unchanged(before, 'consumption-read-failure')


class RealDryComponentBoundaryTests(TestCase):
    def _result(self, fixture):
        action = fixture.book.actions()[0]
        authority = fixture.book.supervised_engineering_authority
        return authority._validate(
            fixture.book, action=action, purpose='engineering'), action, authority

    def test_expired_component_clock_rejected(self):
        from martin_agentic.supervised_engineering import (
            SupervisedEngineeringError)
        with RealDryFixture() as fixture:
            result, action, authority = self._result(fixture)
            with self.assertRaises(SupervisedEngineeringError):
                authority._component_from_result(
                    result, action, NOW + timedelta(seconds=21))

    def test_missing_real_evidence_rejected(self):
        from martin_agentic.supervised_engineering import (
            SupervisedEngineeringError)
        with RealDryFixture() as fixture:
            result, action, authority = self._result(fixture)
            result.pop('real_source_evidence')
            with self.assertRaises(SupervisedEngineeringError):
                authority._component_from_result(result, action, NOW)

    def test_component_rejects_evidence_source_mismatch(self):
        from martin_agentic.supervised_engineering import (
            SupervisedEngineeringError)
        with RealDryFixture() as fixture:
            result, action, authority = self._result(fixture)
            result['real_source_evidence'] = {
                **result['real_source_evidence'],
                'source_fingerprint': 'f' * 64}
            with self.assertRaises(SupervisedEngineeringError):
                authority._component_from_result(result, action, NOW)

    def test_short_risk_component_boundary_expires(self):
        from martin_agentic.supervised_engineering import (
            SupervisedEngineeringError)
        with RealDryFixture() as fixture:
            fixture.configure(risk=lambda book, result: fixture.risk_value(
                valid_until=(NOW + timedelta(seconds=5)).isoformat()))
            result, action, authority = self._result(fixture)
            with self.assertRaises(SupervisedEngineeringError):
                authority._component_from_result(
                    result, action, NOW + timedelta(seconds=6))
