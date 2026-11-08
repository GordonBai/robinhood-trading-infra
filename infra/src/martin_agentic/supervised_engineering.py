"""Supervised engineering permit binding and one-shot consumption core.

This is deliberately *not* a production entry authority.  It binds an owner
request, a current facts assessment, and an exact native review receipt into one
short-lived permit.  A permit cannot be created from caller dictionaries: the
consumer reads owner files and native records.  Real-book evaluation remains
blocked until a separately accepted real-source verifier exists.  The only
positive path is an explicitly enabled synthetic test path on a simulation
book.
"""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import json
import re
import secrets
import sqlite3

from trading_contracts.values import digest

from .formal_authority import (
    CONSUMPTION_TABLE, FormalAuthority, FormalSourceError, FormalSourceStore)

SCHEMA_VERSION = 1
REQUEST_KIND = 'supervised_engineering_user_request_v1'
PERMIT_KIND = 'supervised_engineering_permit_v1'
REQUEST_FILE = 'supervised-engineering-request.json'
PERMIT_FILE = 'supervised-engineering-permit.json'
MAX_TEXT = 400
MAX_REVIEW_AGE_SECONDS = 30
AUTHORITY_BASIS = 'project_small_order_authorization_v1'
CONFIRMATION_MODE = 'native_app_or_explicit_user'
_HEX64 = re.compile(r'[0-9a-f]{64}\Z')


class _RealFinalizationGrant:
    """Opaque authority-issued handle for one verified real assessment.

    The token is only meaningful inside the issuing authority's in-process
    registry.  It is deliberately not a JSON record or a digest a caller can
    recreate.  A process restart therefore requires a fresh trusted assess for
    a new consumption; durable consumption rows are used only for replay and
    recheck recovery, never as a new authorization.
    """
    __slots__ = ('_token',)

    def __init__(self, token):
        self._token = token


class _RealFinalization:
    __slots__ = ('token',)

    def __init__(self, token):
        self.token = token
_UUIDISH = re.compile(r'[A-Za-z0-9][A-Za-z0-9._:-]{0,199}\Z')
_ORDER_KEYS = frozenset({
    'symbol', 'side', 'type', 'quantity', 'limit_price', 'market_hours',
    'time_in_force'})
_RISK_KEYS = frozenset({
    'account_capital_usd', 'max_planned_loss_per_trade_usd',
    'max_position_notional_usd', 'stop_new_entries_at_daily_equity_loss_usd',
    'project_max_drawdown_fraction', 'leverage_allowed', 'overnight_allowed'})
_REQUEST_KEYS = frozenset({
    'schema_version', 'kind', 'request_id', 'signal_id', 'account_key',
    'account_suffix', 'purpose', 'order', 'risk_terms', 'authority_basis',
    'source', 'requested_at', 'not_before', 'valid_until', 'synthetic'})
_PERMIT_KEYS = frozenset({
    'schema_version', 'kind', 'permit_id', 'request_id', 'request_sha256',
    'account_key', 'account_suffix', 'purpose', 'action_id', 'ref_id',
    'order', 'risk_terms', 'authority_basis', 'source', 'facts', 'formal',
    'issued_at', 'not_before', 'valid_until', 'revoked_at', 'one_shot_id',
    'synthetic'})
_FACTS_KEYS = frozenset({
    'assessment_id', 'observed_at', 'observation_valid_until',
    'capture_generation', 'source_fingerprint', 'policy_sha256', 'purpose'})
_FORMAL_KEYS = frozenset({
    'review_operation_id', 'review_action_id', 'review_response_hash',
    'review_recorded_at', 'review_tool', 'review_parameters_sha256',
    'customer_approval_required', 'customer_approval_required_reason',
    'order_checks_sha256', 'requires_native_confirmation',
    'confirmation_mode', 'market_data_disclosure_sha256'})


class SupervisedEngineeringError(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def _fail(code):
    raise SupervisedEngineeringError(code)


def _text(value, code):
    if (not isinstance(value, str) or not value or len(value) > MAX_TEXT
            or any(ord(char) < 32 for char in value)):
        _fail(code)
    return value


def _hex(value, code):
    if not isinstance(value, str) or not _HEX64.fullmatch(value):
        _fail(code)
    return value


def _bool(value, code):
    if type(value) is not bool:
        _fail(code)
    return value


def _time(value, code):
    if isinstance(value, datetime):
        result = value
    elif isinstance(value, str):
        try:
            result = datetime.fromisoformat(value)
        except (TypeError, ValueError, OverflowError):
            _fail(code)
    else:
        _fail(code)
    if result.tzinfo is None:
        _fail(code)
    return result.astimezone(timezone.utc)


def _decimal(value, code, *, positive=False):
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        _fail(code)
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        _fail(code)
    if not result.is_finite() or (positive and result <= 0):
        _fail(code)
    return result


def _json_record(store, name, code):
    try:
        raw = store.read_bytes(name)
    except FormalSourceError as exc:
        raise SupervisedEngineeringError(exc.code) from None
    try:
        value = json.loads(raw)
    except (UnicodeError, ValueError):
        _fail(code)
    if not isinstance(value, dict):
        _fail(code)
    return value


_SOURCE_EVIDENCE_REQUIRED = frozenset({
    'kind', 'request_id', 'request_sha256', 'permit_id', 'action_id',
    'ref_id', 'purpose', 'account_key', 'account_suffix', 'policy_sha256',
    'source_fingerprint', 'capture_generation', 'facts_assessment_id',
    'review_operation_id', 'review_response_hash',
    'review_parameters_sha256', 'review_recorded_at',
    'action_payload_sha256', 'risk_lineage_id',
    'risk_state_sha256', 'risk_source', 'risk_observed_at',
    'risk_valid_until', 'provenance_kind', 'provenance_sha256',
    'provenance_request_sha256', 'native_receipt_status',
    'native_receipt_sha256', 'native_response_sha256',
    'native_response_identity_sha256', 'native_operation_id',
    'native_request_id', 'native_origin', 'native_parameters_sha256',
    'native_record_state', 'native_status',
    'native_confirmation_required', 'native_broker_effect_verified',
    'one_shot_id', 'observed_at',
    'valid_until', 'execution_authorized', 'entry_authorized',
    'production_ready', 'source_sha256', 'verified', 'status', 'reasons'})
_SOURCE_EVIDENCE_KIND = 'engineering_source_evidence_v1'


def _validate_source_evidence(evidence, *, result, book, now):
    """Validate the source evidence contract, not just a self-reported bool."""
    if not isinstance(evidence, dict) or set(evidence) != _SOURCE_EVIDENCE_REQUIRED:
        _fail('SUPERVISED_ENGINEERING_SOURCE_EVIDENCE_SCHEMA')
    if (evidence.get('kind') != _SOURCE_EVIDENCE_KIND
            or evidence.get('verified') is not True
            or evidence.get('status') != 'verified'
            or evidence.get('reasons') != []
            or evidence.get('execution_authorized') is not False
            or evidence.get('entry_authorized') is not False
            or evidence.get('production_ready') is not False):
        _fail('SUPERVISED_ENGINEERING_SOURCE_EVIDENCE_STATE')
    unsigned = {key: value for key, value in evidence.items()
                if key not in {'verified', 'status', 'reasons',
                               'source_sha256'}}
    if digest(unsigned) != evidence.get('source_sha256'):
        _fail('SUPERVISED_ENGINEERING_SOURCE_EVIDENCE_DIGEST')
    permit_wrapper = result.get('permit') or {}
    permit = (permit_wrapper.get('permit')
              if isinstance(permit_wrapper.get('permit'), dict)
              else permit_wrapper)
    request = result.get('request') or {}
    facts = result.get('facts') or {}
    review = result.get('review') or {}
    action = result.get('action') or {}
    expected = {
        'request_id': request.get('request_id'),
        'request_sha256': permit.get('request_sha256'),
        'permit_id': permit.get('permit_id'),
        'action_id': action.get('id') or permit.get('action_id'),
        'ref_id': permit.get('ref_id'),
        'purpose': permit.get('purpose'),
        'account_key': permit.get('account_key'),
        'account_suffix': permit.get('account_suffix'),
        'policy_sha256': (permit.get('source') or {}).get('policy_sha256'),
        'source_fingerprint': (permit.get('source') or {}).get(
            'source_fingerprint'),
        'capture_generation': (permit.get('facts') or {}).get(
            'capture_generation'),
        'facts_assessment_id': (permit.get('facts') or {}).get(
            'assessment_id'),
        'review_operation_id': (permit.get('formal') or {}).get(
            'review_operation_id'),
        'review_response_hash': (permit.get('formal') or {}).get(
            'review_response_hash'),
        'review_parameters_sha256': (permit.get('formal') or {}).get(
            'review_parameters_sha256'),
        'review_recorded_at': (permit.get('formal') or {}).get(
            'review_recorded_at'),
        'one_shot_id': permit.get('one_shot_id'),
    }
    for key, value in expected.items():
        if evidence.get(key) != value:
            _fail('SUPERVISED_ENGINEERING_SOURCE_EVIDENCE_BINDING:' + key)
    if evidence.get('action_payload_sha256') != digest(action.get('payload')):
        _fail('SUPERVISED_ENGINEERING_SOURCE_EVIDENCE_ACTION')
    if evidence.get('provenance_kind') not in {
            'host_owner_request', 'owner_mailbox_request'}:
        _fail('SUPERVISED_ENGINEERING_SOURCE_EVIDENCE_PROVENANCE')
    if evidence.get('provenance_request_sha256') != expected['request_sha256']:
        _fail('SUPERVISED_ENGINEERING_SOURCE_EVIDENCE_PROVENANCE')
    if evidence.get('risk_source') != 'owner-risk-lineage-v1':
        _fail('SUPERVISED_ENGINEERING_SOURCE_EVIDENCE_RISK')
    if evidence.get('risk_state_sha256') != digest(book.risk_guard.state()):
        _fail('SUPERVISED_ENGINEERING_SOURCE_EVIDENCE_RISK')
    if not isinstance(evidence.get('risk_lineage_id'), str)             or not evidence['risk_lineage_id']:
        _fail('SUPERVISED_ENGINEERING_SOURCE_EVIDENCE_RISK')
    native_status = evidence.get('native_receipt_status')
    if native_status not in {'not_configured', 'bound'}:
        _fail('SUPERVISED_ENGINEERING_SOURCE_EVIDENCE_NATIVE')
    if evidence.get('native_broker_effect_verified') is not False:
        _fail('SUPERVISED_ENGINEERING_SOURCE_EVIDENCE_NATIVE')
    if native_status == 'bound':
        for key in ('native_receipt_sha256', 'native_response_sha256',
                    'native_response_identity_sha256',
                    'native_parameters_sha256'):
            if (not isinstance(evidence.get(key), str)
                    or not re.fullmatch('[0-9a-f]{64}', evidence[key])):
                _fail('SUPERVISED_ENGINEERING_SOURCE_EVIDENCE_NATIVE')
        if (evidence.get('native_origin') not in {
                'host_submitted_response', 'formal_runtime_transport'}
                or evidence.get('native_record_state') != 'RECORDED'
                or evidence.get('native_operation_id') !=
                expected['review_operation_id']
                or not isinstance(evidence.get('native_request_id'), str)
                or type(evidence.get('native_confirmation_required'))
                is not bool):
            _fail('SUPERVISED_ENGINEERING_SOURCE_EVIDENCE_NATIVE')
    now = now.astimezone(timezone.utc)
    try:
        observed = _time(evidence.get('observed_at'), 'SOURCE_EVIDENCE_TIME')
        valid = _time(evidence.get('valid_until'), 'SOURCE_EVIDENCE_TIME')
        risk_observed = _time(evidence.get('risk_observed_at'),
                              'SOURCE_EVIDENCE_TIME')
        risk_valid = _time(evidence.get('risk_valid_until'),
                           'SOURCE_EVIDENCE_TIME')
    except SupervisedEngineeringError:
        _fail('SUPERVISED_ENGINEERING_SOURCE_EVIDENCE_TIME')
    try:
        review_at = _time(evidence.get('review_recorded_at'),
                          'SOURCE_EVIDENCE_TIME')
    except SupervisedEngineeringError:
        _fail('SUPERVISED_ENGINEERING_SOURCE_EVIDENCE_TIME')
    facts_valid = _time(facts.get('observation_valid_until'),
                        'SOURCE_EVIDENCE_TIME')
    request_valid = _time((result.get('request') or {}).get('valid_until'),
                          'SOURCE_EVIDENCE_TIME')
    permit_valid = _time(permit.get('valid_until'), 'SOURCE_EVIDENCE_TIME')
    if (observed > now or now > valid or risk_observed > now
            or now > risk_valid or review_at > now
            or now > review_at + timedelta(seconds=MAX_REVIEW_AGE_SECONDS)
            or valid > risk_valid or valid > facts_valid
            or valid > request_valid or valid > permit_valid
            or valid > review_at + timedelta(seconds=MAX_REVIEW_AGE_SECONDS)):
        _fail('SUPERVISED_ENGINEERING_SOURCE_EVIDENCE_WINDOW')
    if facts.get('observation_valid_until') is None:
        _fail('SUPERVISED_ENGINEERING_SOURCE_EVIDENCE_FACTS')
    return evidence


def _policy_risk_terms(policy):
    if not isinstance(policy, dict):
        _fail('SUPERVISED_ENGINEERING_POLICY_INVALID')
    try:
        terms = {
            'account_capital_usd': str(policy['account_capital_usd']),
            'max_planned_loss_per_trade_usd':
                str(policy['max_planned_loss_per_trade_usd']),
            'max_position_notional_usd':
                str(policy['max_position_notional_usd']),
            'stop_new_entries_at_daily_equity_loss_usd':
                str(policy['stop_new_entries_at_daily_equity_loss_usd']),
            'project_max_drawdown_fraction':
                str(policy['project_max_drawdown_fraction']),
            'leverage_allowed': policy['leverage_allowed'],
            'overnight_allowed': policy['overnight_allowed'],
        }
    except KeyError:
        _fail('SUPERVISED_ENGINEERING_POLICY_INVALID')
    if terms['leverage_allowed'] is not False or terms['overnight_allowed'] is not False:
        _fail('SUPERVISED_ENGINEERING_POLICY_UNSAFE')
    return terms


def _check_risk_terms(raw, policy):
    if not isinstance(raw, dict) or set(raw) != _RISK_KEYS:
        _fail('SUPERVISED_ENGINEERING_RISK_SCHEMA')
    expected = _policy_risk_terms(policy)
    for key in _RISK_KEYS:
        if key in {'leverage_allowed', 'overnight_allowed'}:
            if _bool(raw.get(key), 'SUPERVISED_ENGINEERING_RISK_FLAG_INVALID') is not expected[key]:
                _fail('SUPERVISED_ENGINEERING_RISK_MISMATCH:' + key)
            continue
        actual = _decimal(raw.get(key), 'SUPERVISED_ENGINEERING_RISK_INVALID')
        if actual != _decimal(expected[key], 'SUPERVISED_ENGINEERING_POLICY_INVALID'):
            _fail('SUPERVISED_ENGINEERING_RISK_MISMATCH:' + key)
    return dict(raw)


def _check_order(raw):
    if not isinstance(raw, dict) or set(raw) != _ORDER_KEYS:
        _fail('SUPERVISED_ENGINEERING_ORDER_SCHEMA')
    if (raw.get('side') != 'buy' or raw.get('type') != 'limit'
            or raw.get('market_hours') != 'regular_hours'
            or raw.get('time_in_force') != 'gfd'):
        _fail('SUPERVISED_ENGINEERING_ORDER_TERMS')
    _text(raw.get('symbol'), 'SUPERVISED_ENGINEERING_SYMBOL_INVALID')
    quantity = _decimal(raw.get('quantity'),
                        'SUPERVISED_ENGINEERING_QUANTITY_INVALID',
                        positive=True)
    if quantity != quantity.to_integral_value():
        _fail('SUPERVISED_ENGINEERING_QUANTITY_INVALID')
    limit = _decimal(raw.get('limit_price'),
                     'SUPERVISED_ENGINEERING_LIMIT_INVALID', positive=True)
    if quantity > Decimal('1000000000') or limit > Decimal('1000000000'):
        _fail('SUPERVISED_ENGINEERING_ORDER_TERMS')
    return dict(raw)


def order_from_plan(plan):
    if not isinstance(plan, dict):
        _fail('SUPERVISED_ENGINEERING_PLAN_INVALID')
    try:
        raw = {
            'symbol': plan['symbol'],
            'side': 'buy',
            'type': 'limit',
            'quantity': str(plan['quantity']),
            'limit_price': str(plan['entry_ceiling']),
            'market_hours': 'regular_hours',
            'time_in_force': 'gfd',
        }
    except KeyError:
        _fail('SUPERVISED_ENGINEERING_PLAN_INVALID')
    return _check_order(raw)


def _check_source(raw, *, policy_sha256, fingerprint, capture=None):
    if not isinstance(raw, dict) or set(raw) != {
            'source_fingerprint', 'policy_sha256', 'capture_generation'}:
        _fail('SUPERVISED_ENGINEERING_SOURCE_SCHEMA')
    if raw.get('source_fingerprint') != fingerprint:
        _fail('SUPERVISED_ENGINEERING_SOURCE_MISMATCH')
    if raw.get('policy_sha256') != policy_sha256:
        _fail('SUPERVISED_ENGINEERING_POLICY_MISMATCH')
    generation = raw.get('capture_generation')
    if generation is not None and not isinstance(generation, str):
        _fail('SUPERVISED_ENGINEERING_CAPTURE_INVALID')
    if capture is not None and generation != capture:
        _fail('SUPERVISED_ENGINEERING_CAPTURE_MISMATCH')
    return dict(raw)


def _check_plan_risk(plan, policy, order):
    try:
        quantity = _decimal(order.get('quantity'),
                            'SUPERVISED_ENGINEERING_QUANTITY_INVALID',
                            positive=True)
        entry = _decimal(order.get('limit_price'),
                         'SUPERVISED_ENGINEERING_LIMIT_INVALID',
                         positive=True)
        stop = _decimal(plan.get('stop'),
                        'SUPERVISED_ENGINEERING_STOP_INVALID',
                        positive=True)
        cost = _decimal(plan.get('cost_buffer_per_share', '0'),
                        'SUPERVISED_ENGINEERING_COST_INVALID')
    except (AttributeError, TypeError):
        _fail('SUPERVISED_ENGINEERING_PLAN_INVALID')
    if cost < 0 or entry <= stop:
        _fail('SUPERVISED_ENGINEERING_PLAN_INVALID')
    max_notional = _decimal(policy.get('max_position_notional_usd'),
                            'SUPERVISED_ENGINEERING_POLICY_INVALID',
                            positive=True)
    max_loss = _decimal(policy.get('max_planned_loss_per_trade_usd'),
                        'SUPERVISED_ENGINEERING_POLICY_INVALID',
                        positive=True)
    if quantity * entry > max_notional:
        _fail('SUPERVISED_ENGINEERING_ORDER_EXCEEDS_NOTIONAL')
    if quantity * (entry - stop + cost) > max_loss:
        _fail('SUPERVISED_ENGINEERING_ORDER_EXCEEDS_PLANNED_LOSS')
    return True

def parse_request(raw, *, account_key, account_suffix, policy, now,
                  fingerprint):
    if not isinstance(raw, dict) or set(raw) != _REQUEST_KEYS:
        _fail('SUPERVISED_ENGINEERING_REQUEST_SCHEMA')
    if raw.get('schema_version') != SCHEMA_VERSION or raw.get('kind') != REQUEST_KIND:
        _fail('SUPERVISED_ENGINEERING_REQUEST_SCHEMA')
    _text(raw.get('request_id'), 'SUPERVISED_ENGINEERING_REQUEST_ID')
    signal_id = _text(raw.get('signal_id'), 'SUPERVISED_ENGINEERING_SIGNAL_ID')
    if not signal_id.startswith('engineering-'):
        _fail('SUPERVISED_ENGINEERING_SIGNAL_ID')
    if raw.get('account_key') != account_key:
        _fail('SUPERVISED_ENGINEERING_ACCOUNT_MISMATCH')
    if raw.get('account_suffix') != account_suffix:
        _fail('SUPERVISED_ENGINEERING_SUFFIX_MISMATCH')
    if raw.get('purpose') != 'engineering':
        _fail('SUPERVISED_ENGINEERING_PURPOSE')
    order = _check_order(raw.get('order'))
    risk = _check_risk_terms(raw.get('risk_terms'), policy)
    basis = raw.get('authority_basis')
    if (not isinstance(basis, dict) or set(basis) != {'kind', 'ref'}
            or basis.get('kind') != AUTHORITY_BASIS):
        _fail('SUPERVISED_ENGINEERING_AUTHORITY_BASIS')
    _text(basis.get('ref'), 'SUPERVISED_ENGINEERING_AUTHORITY_REF')
    source = _check_source(raw.get('source'),
                           policy_sha256=digest(policy),
                           fingerprint=fingerprint)
    requested = _time(raw.get('requested_at'),
                      'SUPERVISED_ENGINEERING_REQUEST_TIME')
    not_before = _time(raw.get('not_before'),
                       'SUPERVISED_ENGINEERING_REQUEST_TIME')
    valid_until = _time(raw.get('valid_until'),
                        'SUPERVISED_ENGINEERING_REQUEST_TIME')
    now = now.astimezone(timezone.utc)
    if requested > now or not_before > now or valid_until <= now:
        _fail('SUPERVISED_ENGINEERING_REQUEST_WINDOW')
    if requested > not_before or not_before >= valid_until:
        _fail('SUPERVISED_ENGINEERING_REQUEST_WINDOW')
    synthetic = _bool(raw.get('synthetic'),
                      'SUPERVISED_ENGINEERING_SYNTHETIC_FLAG')
    return {
        'request': dict(raw), 'request_id': raw['request_id'],
        'signal_id': signal_id, 'order': order, 'risk_terms': risk,
        'authority_basis': dict(basis), 'source': source,
        'requested_at': requested, 'not_before': not_before,
        'valid_until': valid_until, 'synthetic': synthetic,
        'request_sha256': digest(raw),
    }


def _review_record(book, action_id):
    try:
        row = book.db.execute(
            'SELECT id,action_id,operation,payload,result,state '
            'FROM native_operation_records WHERE action_id=? AND '
            "operation='review' ORDER BY rowid DESC LIMIT 1",
            (action_id,)).fetchone()
    except sqlite3.Error:
        _fail('SUPERVISED_ENGINEERING_REVIEW_LEDGER_UNAVAILABLE')
    if not row:
        _fail('SUPERVISED_ENGINEERING_REVIEW_REQUIRED')
    operation_id, action_id_row, operation, raw_payload, raw_result, state = row
    if action_id_row != action_id or operation != 'review' or state != 'RECORDED':
        _fail('SUPERVISED_ENGINEERING_REVIEW_REQUIRED')
    try:
        payload = json.loads(raw_payload)
        result = json.loads(raw_result)
    except (TypeError, ValueError):
        _fail('SUPERVISED_ENGINEERING_REVIEW_INVALID')
    if not isinstance(payload, dict) or not isinstance(result, dict):
        _fail('SUPERVISED_ENGINEERING_REVIEW_INVALID')
    if (result.get('review_verified') is not True
            or result.get('response_usable') is not True
            or not _HEX64.fullmatch(str(result.get('response_hash') or ''))):
        _fail('SUPERVISED_ENGINEERING_REVIEW_UNUSABLE')
    if payload.get('action_id') != action_id or payload.get('operation') != 'review' \
            or payload.get('tool') != 'review_equity_order':
        _fail('SUPERVISED_ENGINEERING_REVIEW_BINDING')
    parameters = payload.get('parameters')
    if not isinstance(parameters, dict):
        _fail('SUPERVISED_ENGINEERING_REVIEW_BINDING')
    checks = result.get('order_checks')
    if not isinstance(checks, dict):
        _fail('SUPERVISED_ENGINEERING_REVIEW_INVALID')
    approval = result.get('customer_approval_required')
    if approval is not None and type(approval) is not bool:
        _fail('SUPERVISED_ENGINEERING_REVIEW_INVALID')
    requires = result.get('requires_native_confirmation')
    if type(requires) is not bool:
        _fail('SUPERVISED_ENGINEERING_REVIEW_INVALID')
    reason = result.get('customer_approval_required_reason')
    if reason is not None and not isinstance(reason, str):
        _fail('SUPERVISED_ENGINEERING_REVIEW_INVALID')
    disclosure = result.get('market_data_disclosure')
    if not isinstance(disclosure, str):
        _fail('SUPERVISED_ENGINEERING_REVIEW_DISCLOSURE')
    recorded = _time(result.get('recorded_at'),
                     'SUPERVISED_ENGINEERING_REVIEW_TIME')
    return {
        'operation_id': operation_id, 'action_id': action_id,
        'payload': payload, 'result': result, 'parameters': parameters,
        'response_hash': result['response_hash'], 'recorded_at': recorded,
        'order_checks_sha256': digest(checks),
        'customer_approval_required': approval,
        'customer_approval_required_reason': reason,
        'requires_native_confirmation': requires,
        'market_data_disclosure': disclosure,
        'parameters_sha256': digest(parameters),
    }


def _facts_record(book, purpose, *, account_key, policy_sha256, fingerprint,
                  now):
    reader = getattr(book, 'current_account_facts', None)
    assessment = reader(purpose=purpose) if callable(reader) else None
    if not isinstance(assessment, dict):
        _fail('SUPERVISED_ENGINEERING_FACTS_MISSING')
    if (assessment.get('kind') != 'current_account_facts_assessment_v1'
            or assessment.get('schema_version') != 1
            or assessment.get('entry_authorized') is not False
            or assessment.get('production_ready') is not False
            or assessment.get('facts_are_not_entry_authorization') is not True
            or not isinstance(assessment.get('assessment_id'), str)):
        _fail('SUPERVISED_ENGINEERING_FACTS_UNTRUSTED')
    body = {key: value for key, value in assessment.items()
            if key != 'assessment_id'}
    if digest(body) != assessment['assessment_id']:
        _fail('SUPERVISED_ENGINEERING_FACTS_UNTRUSTED')
    binding = assessment.get('binding')
    if (not isinstance(binding, dict)
            or binding.get('account_key') != account_key
            or binding.get('purpose') != purpose):
        _fail('SUPERVISED_ENGINEERING_FACTS_BINDING')
    if (binding.get('policy_sha256') != policy_sha256
            or binding.get('source_fingerprint') != fingerprint):
        _fail('SUPERVISED_ENGINEERING_FACTS_SOURCE')
    try:
        observed = _time(assessment.get('observed_at'),
                         'SUPERVISED_ENGINEERING_FACTS_TIME')
        valid_until = _time(assessment.get('observation_valid_until'),
                            'SUPERVISED_ENGINEERING_FACTS_TIME')
        assessed = _time(assessment.get('assessed_at'),
                         'SUPERVISED_ENGINEERING_FACTS_TIME')
    except SupervisedEngineeringError:
        raise
    now = now.astimezone(timezone.utc)
    if (observed > valid_until or observed > assessed or assessed > valid_until
            or assessed > now or now < observed or now > valid_until):
        _fail('SUPERVISED_ENGINEERING_FACTS_STALE')
    if assessment.get('observation_valid') is not True:
        _fail('SUPERVISED_ENGINEERING_FACTS_BLOCKED')
    return {
        'assessment': assessment, 'assessment_id': assessment['assessment_id'],
        'observed_at': observed, 'observation_valid_until': valid_until,
        'capture_generation': binding.get('capture_generation'),
        'source_fingerprint': binding.get('source_fingerprint'),
        'policy_sha256': binding.get('policy_sha256'),
    }


def _check_permit_permit(raw, *, request, account_key, account_suffix,
                         policy, fingerprint, action_id, ref_id, now):
    if not isinstance(raw, dict) or set(raw) != _PERMIT_KEYS:
        _fail('SUPERVISED_ENGINEERING_PERMIT_SCHEMA')
    if raw.get('schema_version') != SCHEMA_VERSION or raw.get('kind') != PERMIT_KIND:
        _fail('SUPERVISED_ENGINEERING_PERMIT_SCHEMA')
    _text(raw.get('permit_id'), 'SUPERVISED_ENGINEERING_PERMIT_ID')
    if digest({**raw, 'permit_id': None}) != raw['permit_id']:
        _fail('SUPERVISED_ENGINEERING_PERMIT_TAMPERED')
    if raw.get('request_id') != request['request_id']:
        _fail('SUPERVISED_ENGINEERING_REQUEST_MISMATCH')
    if raw.get('request_sha256') != request['request_sha256']:
        _fail('SUPERVISED_ENGINEERING_REQUEST_MISMATCH')
    if raw.get('account_key') != account_key:
        _fail('SUPERVISED_ENGINEERING_ACCOUNT_MISMATCH')
    if raw.get('account_suffix') != account_suffix:
        _fail('SUPERVISED_ENGINEERING_SUFFIX_MISMATCH')
    if raw.get('purpose') != 'engineering' or request['request'].get('purpose') != 'engineering':
        _fail('SUPERVISED_ENGINEERING_PURPOSE')
    if raw.get('action_id') != action_id or raw.get('ref_id') != ref_id:
        _fail('SUPERVISED_ENGINEERING_ACTION_MISMATCH')
    if _check_order(raw.get('order')) != request['order']:
        _fail('SUPERVISED_ENGINEERING_ORDER_MISMATCH')
    if _check_risk_terms(raw.get('risk_terms'), policy) != request['risk_terms']:
        _fail('SUPERVISED_ENGINEERING_RISK_MISMATCH')
    if raw.get('authority_basis') != request['authority_basis']:
        _fail('SUPERVISED_ENGINEERING_AUTHORITY_MISMATCH')
    source = _check_source(raw.get('source'), policy_sha256=digest(policy),
                           fingerprint=fingerprint)
    if source != request['source']:
        _fail('SUPERVISED_ENGINEERING_SOURCE_MISMATCH')
    facts = raw.get('facts')
    if not isinstance(facts, dict) or set(facts) != _FACTS_KEYS:
        _fail('SUPERVISED_ENGINEERING_FACTS_SCHEMA')
    formal = raw.get('formal')
    if not isinstance(formal, dict) or set(formal) != _FORMAL_KEYS:
        _fail('SUPERVISED_ENGINEERING_FORMAL_SCHEMA')
    _hex(formal.get('review_response_hash'),
         'SUPERVISED_ENGINEERING_REVIEW_HASH')
    _hex(formal.get('review_parameters_sha256'),
         'SUPERVISED_ENGINEERING_REVIEW_PARAMETERS')
    _hex(formal.get('order_checks_sha256'),
         'SUPERVISED_ENGINEERING_REVIEW_CHECKS')
    _hex(formal.get('market_data_disclosure_sha256'),
         'SUPERVISED_ENGINEERING_REVIEW_DISCLOSURE')
    if formal.get('review_action_id') != action_id \
            or formal.get('review_tool') != 'review_equity_order' \
            or formal.get('confirmation_mode') != CONFIRMATION_MODE:
        _fail('SUPERVISED_ENGINEERING_REVIEW_BINDING')
    _time(formal.get('review_recorded_at'),
          'SUPERVISED_ENGINEERING_REVIEW_TIME')
    if type(formal.get('requires_native_confirmation')) is not bool:
        _fail('SUPERVISED_ENGINEERING_REVIEW_CONFIRMATION')
    if raw.get('revoked_at') is not None:
        _fail('SUPERVISED_ENGINEERING_PERMIT_REVOKED')
    _text(raw.get('one_shot_id'), 'SUPERVISED_ENGINEERING_ONE_SHOT')
    issued = _time(raw.get('issued_at'), 'SUPERVISED_ENGINEERING_PERMIT_TIME')
    not_before = _time(raw.get('not_before'), 'SUPERVISED_ENGINEERING_PERMIT_TIME')
    valid_until = _time(raw.get('valid_until'), 'SUPERVISED_ENGINEERING_PERMIT_TIME')
    now = now.astimezone(timezone.utc)
    if issued > now or not_before > now or valid_until <= now:
        _fail('SUPERVISED_ENGINEERING_PERMIT_WINDOW')
    if issued > not_before or not_before >= valid_until:
        _fail('SUPERVISED_ENGINEERING_PERMIT_WINDOW')
    if valid_until > request['valid_until']:
        _fail('SUPERVISED_ENGINEERING_PERMIT_EXTENDS_REQUEST')
    synthetic = _bool(raw.get('synthetic'),
                      'SUPERVISED_ENGINEERING_SYNTHETIC_FLAG')
    if synthetic != request['synthetic']:
        _fail('SUPERVISED_ENGINEERING_SYNTHETIC_MIXED')
    return {
        'permit': dict(raw), 'source': source, 'facts': facts,
        'formal': formal, 'one_shot_id': raw['one_shot_id'],
        'valid_until': valid_until, 'not_before': not_before,
        'issued_at': issued,
        'synthetic': synthetic, 'order': _check_order(raw.get('order')),
        'ref_id': raw['ref_id'], 'action_id': raw['action_id'],
        'risk_terms': _check_risk_terms(raw.get('risk_terms'),
                                        policy),
        'request_sha256': raw['request_sha256'],
    }


def build_permit(request, review, facts_assessment, *, account_key,
                 account_suffix, policy, fingerprint, action_id, ref_id,
                 now, one_shot_id, valid_until):
    """Validate issuer inputs and return one exact permit record.

    This function does not write a file and grants no execution authority.  A
    caller writing the returned record is an owner action; the synthetic flag
    is only consumable by a simulation book with explicit test enablement.
    """
    if not isinstance(now, datetime) or now.tzinfo is None:
        _fail('SUPERVISED_ENGINEERING_CLOCK_INVALID')
    now = now.astimezone(timezone.utc)
    request_value = parse_request(
        request, account_key=account_key, account_suffix=account_suffix,
        policy=policy, now=now, fingerprint=fingerprint)
    review_value = _review_record_from_raw(review, action_id=action_id, now=now)
    facts = _facts_record_values(facts_assessment, purpose='engineering',
                                  account_key=account_key,
                                  policy_sha256=digest(policy),
                                  fingerprint=fingerprint, now=now)
    requested_until = _time(valid_until, 'SUPERVISED_ENGINEERING_PERMIT_TIME')
    expiry = min(request_value['valid_until'], facts['observation_valid_until'],
                 review_value['recorded_at'] + timedelta(
                     seconds=MAX_REVIEW_AGE_SECONDS), requested_until)
    if expiry <= now:
        _fail('SUPERVISED_ENGINEERING_PERMIT_WINDOW')
    formal = {
        'review_operation_id': review_value['operation_id'],
        'review_action_id': action_id,
        'review_response_hash': review_value['response_hash'],
        'review_recorded_at': review_value['recorded_at'].isoformat(),
        'review_tool': 'review_equity_order',
        'review_parameters_sha256': review_value['parameters_sha256'],
        'customer_approval_required':
            review_value['customer_approval_required'],
        'customer_approval_required_reason':
            review_value['customer_approval_required_reason'],
        'order_checks_sha256': review_value['order_checks_sha256'],
        'requires_native_confirmation':
            review_value['requires_native_confirmation'],
        'confirmation_mode': CONFIRMATION_MODE,
        'market_data_disclosure_sha256':
            digest(review_value['market_data_disclosure']),
    }
    facts_value = {
        'assessment_id': facts['assessment_id'],
        'observed_at': facts['observed_at'].isoformat(),
        'observation_valid_until': facts['observation_valid_until'].isoformat(),
        'capture_generation': facts['capture_generation'],
        'source_fingerprint': facts['source_fingerprint'],
        'policy_sha256': facts['policy_sha256'],
        'purpose': 'engineering',
    }
    body = {
        'schema_version': SCHEMA_VERSION, 'kind': PERMIT_KIND,
        'permit_id': None, 'request_id': request_value['request_id'],
        'request_sha256': request_value['request_sha256'],
        'account_key': account_key, 'account_suffix': account_suffix,
        'purpose': 'engineering', 'action_id': action_id, 'ref_id': ref_id,
        'order': request_value['order'],
        'risk_terms': request_value['risk_terms'],
        'authority_basis': request_value['authority_basis'],
        'source': request_value['source'], 'facts': facts_value,
        'formal': formal, 'issued_at': now.isoformat(),
        'not_before': now.isoformat(), 'valid_until': expiry.isoformat(),
        'revoked_at': None, 'one_shot_id': _text(
            one_shot_id, 'SUPERVISED_ENGINEERING_ONE_SHOT'),
        'synthetic': request_value['synthetic'],
    }
    body['permit_id'] = digest(body)
    return body


def _review_record_from_raw(raw, *, action_id, now):
    if not isinstance(raw, dict) or set(raw) != {
            'operation_id', 'action_id', 'parameters', 'result'}:
        _fail('SUPERVISED_ENGINEERING_REVIEW_SCHEMA')
    result = raw.get('result')
    parameters = raw.get('parameters')
    if not isinstance(result, dict) or not isinstance(parameters, dict):
        _fail('SUPERVISED_ENGINEERING_REVIEW_SCHEMA')
    if (raw.get('action_id') != action_id
            or result.get('review_verified') is not True
            or result.get('response_usable') is not True
            or not _HEX64.fullmatch(str(result.get('response_hash') or ''))):
        _fail('SUPERVISED_ENGINEERING_REVIEW_UNUSABLE')
    checks = result.get('order_checks')
    if not isinstance(checks, dict):
        _fail('SUPERVISED_ENGINEERING_REVIEW_INVALID')
    approval = result.get('customer_approval_required')
    if approval is not None and type(approval) is not bool:
        _fail('SUPERVISED_ENGINEERING_REVIEW_INVALID')
    requires = result.get('requires_native_confirmation')
    if type(requires) is not bool:
        _fail('SUPERVISED_ENGINEERING_REVIEW_INVALID')
    disclosure = result.get('market_data_disclosure')
    if not isinstance(disclosure, str):
        _fail('SUPERVISED_ENGINEERING_REVIEW_DISCLOSURE')
    recorded = _time(result.get('recorded_at'),
                     'SUPERVISED_ENGINEERING_REVIEW_TIME')
    if recorded > now or (now - recorded).total_seconds() > MAX_REVIEW_AGE_SECONDS:
        _fail('SUPERVISED_ENGINEERING_REVIEW_STALE')
    return {
        'operation_id': _text(raw.get('operation_id'),
                              'SUPERVISED_ENGINEERING_REVIEW_ID'),
        'action_id': action_id, 'parameters': parameters, 'result': result,
        'response_hash': result['response_hash'], 'recorded_at': recorded,
        'parameters_sha256': digest(parameters),
        'order_checks_sha256': digest(checks),
        'customer_approval_required': approval,
        'customer_approval_required_reason':
            result.get('customer_approval_required_reason'),
        'requires_native_confirmation': requires,
        'market_data_disclosure': disclosure,
    }


def _facts_record_values(assessment, *, purpose, account_key, policy_sha256,
                         fingerprint, now):
    if not isinstance(assessment, dict):
        _fail('SUPERVISED_ENGINEERING_FACTS_MISSING')
    fake_book = type('FactsBook', (), {'current_account_facts': staticmethod(
        lambda *, purpose: assessment)})()
    return _facts_record(fake_book, purpose, account_key=account_key,
                         policy_sha256=policy_sha256, fingerprint=fingerprint,
                         now=now)


def _action_binding(book, action, *, account_key, policy_sha256, fingerprint,
                    now):
    if not isinstance(action, dict) or action.get('kind') != 'entry':
        _fail('SUPERVISED_ENGINEERING_ACTION_REQUIRED')
    payload = None
    try:
        payload = json.loads(action.get('payload'))
    except (TypeError, ValueError):
        _fail('SUPERVISED_ENGINEERING_ACTION_INVALID')
    if (not isinstance(payload, dict)
            or payload.get('purpose') != 'engineering'
            or payload.get('account') != '••••' + str(book.policy.get('account_suffix'))
            or not isinstance(payload.get('ref_id'), str)
            or not isinstance(payload.get('action_id'), str)
            or payload.get('action_id') != action.get('id')):
        _fail('SUPERVISED_ENGINEERING_ACTION_BINDING')
    plan = payload.get('plan')
    order = order_from_plan(plan)
    if order != _check_order(payload.get('parameters')):
        _fail('SUPERVISED_ENGINEERING_ACTION_ORDER')
    return payload, plan, order


def validate_permit_against(book, action, *, purpose, now, permit_raw,
                            request_raw, account_key, policy, fingerprint,
                            allow_consumed=False):
    if purpose != 'engineering':
        _fail('SUPERVISED_ENGINEERING_PURPOSE')
    request = parse_request(
        request_raw, account_key=account_key,
        account_suffix=str(policy.get('account_suffix')), policy=policy,
        now=now, fingerprint=fingerprint)
    payload, plan, order = _action_binding(
        book, action, account_key=account_key, policy_sha256=digest(policy),
        fingerprint=fingerprint, now=now)
    permit = _check_permit_permit(
        permit_raw, request=request, account_key=account_key,
        account_suffix=str(policy.get('account_suffix')), policy=policy,
        fingerprint=fingerprint, action_id=action['id'],
        ref_id=payload['ref_id'], now=now)
    if permit['source']['source_fingerprint'] != fingerprint \
            or permit['source']['policy_sha256'] != digest(policy):
        _fail('SUPERVISED_ENGINEERING_SOURCE_MISMATCH')
    if permit['facts']['source_fingerprint'] != fingerprint \
            or permit['facts']['policy_sha256'] != digest(policy):
        _fail('SUPERVISED_ENGINEERING_FACTS_SOURCE')
    facts = _facts_record(book, purpose, account_key=account_key,
                          policy_sha256=digest(policy), fingerprint=fingerprint,
                          now=now)
    if facts['assessment_id'] != permit['facts']['assessment_id']:
        _fail('SUPERVISED_ENGINEERING_FACTS_CHANGED')
    if facts['observed_at'].isoformat() != permit['facts']['observed_at'] \
            or facts['observation_valid_until'].isoformat() != \
            permit['facts']['observation_valid_until']:
        _fail('SUPERVISED_ENGINEERING_FACTS_CHANGED')
    if facts['capture_generation'] != permit['facts']['capture_generation']:
        _fail('SUPERVISED_ENGINEERING_CAPTURE_CHANGED')
    if permit['valid_until'] > facts['observation_valid_until']:
        _fail('SUPERVISED_ENGINEERING_PERMIT_EXTENDS_FACTS')
    review = _review_record(book, action['id'])
    if _check_order(review['parameters']) != order:
        _fail('SUPERVISED_ENGINEERING_REVIEW_ORDER')
    if review['operation_id'] != permit['formal']['review_operation_id'] \
            or review['response_hash'] != permit['formal']['review_response_hash'] \
            or review['parameters_sha256'] != permit['formal']['review_parameters_sha256'] \
            or review['order_checks_sha256'] != permit['formal']['order_checks_sha256'] \
            or review['customer_approval_required'] != permit['formal']['customer_approval_required'] \
            or review['customer_approval_required_reason'] != permit['formal']['customer_approval_required_reason'] \
            or review['requires_native_confirmation'] != permit['formal']['requires_native_confirmation'] \
            or digest(review['market_data_disclosure']) != permit['formal']['market_data_disclosure_sha256'] \
            or review['recorded_at'].isoformat() != permit['formal']['review_recorded_at']:
        _fail('SUPERVISED_ENGINEERING_REVIEW_CHANGED')
    now = now.astimezone(timezone.utc)
    if not 0 <= (now - review['recorded_at']).total_seconds() <= MAX_REVIEW_AGE_SECONDS:
        _fail('SUPERVISED_ENGINEERING_REVIEW_STALE')
    permit_review_until = review['recorded_at'] + timedelta(seconds=MAX_REVIEW_AGE_SECONDS)
    if permit['valid_until'] > permit_review_until:
        _fail('SUPERVISED_ENGINEERING_REVIEW_EXTENDS')
    if permit['order'] != order:
        _fail('SUPERVISED_ENGINEERING_ORDER_MISMATCH')
    _check_plan_risk(plan, policy, order)
    if permit['ref_id'] != payload['ref_id']:
        _fail('SUPERVISED_ENGINEERING_REF_MISMATCH')
    _check_risk_terms(permit['risk_terms'], policy)
    binding = digest(permit['permit'])
    if not allow_consumed:
        existing = _consumption(book, permit['one_shot_id'])
        if existing is not None:
            _fail('SUPERVISED_ENGINEERING_PERMIT_REPLAYED')
    return {
        'permit': permit, 'request': request, 'facts': facts,
        'review': review, 'binding': binding, 'action': action,
        'plan': plan, 'order': order,
    }


def _consumption(book, one_shot_id):
    try:
        exists = book.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (CONSUMPTION_TABLE,)).fetchone()
        if not exists:
            return None
        return book.db.execute(
            'SELECT one_shot_id,source_sha256,binding,action_id,consumed_at '
            'FROM ' + CONSUMPTION_TABLE + ' WHERE one_shot_id=?',
            (one_shot_id,)).fetchone()
    except sqlite3.Error:
        _fail('SUPERVISED_ENGINEERING_CONSUMPTION_LEDGER_UNAVAILABLE')


class SupervisedEngineeringAuthority(FormalAuthority):
    """Read owner request/permit plus current facts/native review.

    Real books never become allowed here: no accepted real-source verifier is
    configured.  Synthetic positive evaluation requires ``allow_synthetic`` and
    ``book.simulation is True``.
    """

    def __init__(self, root, *, clock=None, allow_synthetic=False,
                 real_source_verifier=None, real_activation_enabled=False):
        self.store = FormalSourceStore(root)
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.allow_synthetic = bool(allow_synthetic)
        self.real_source_verifier = real_source_verifier
        self.real_activation_enabled = real_activation_enabled is True
        self._real_grants = {}
        self._real_finalizations = {}

    def _now(self):
        now = self.clock()
        if not isinstance(now, datetime) or now.tzinfo is None:
            _fail('SUPERVISED_ENGINEERING_CLOCK_INVALID')
        return now.astimezone(timezone.utc)

    def _load(self):
        try:
            request = _json_record(self.store, REQUEST_FILE,
                                   'SUPERVISED_ENGINEERING_REQUEST_INVALID')
            permit = _json_record(self.store, PERMIT_FILE,
                                  'SUPERVISED_ENGINEERING_PERMIT_INVALID')
        except SupervisedEngineeringError as exc:
            if exc.code in {'FORMAL_SOURCE_MISSING', 'FORMAL_SOURCE_ROOT_INVALID'}:
                raise SupervisedEngineeringError(
                    'SUPERVISED_ENGINEERING_PERMIT_UNAVAILABLE') from None
            raise
        return request, permit

    def _validate(self, book, *, action, purpose, allow_consumed=False):
        now = self._now()
        request_raw, permit_raw = self._load()
        account_key = book.state().get('account_key')
        if not isinstance(account_key, str) or not account_key:
            _fail('SUPERVISED_ENGINEERING_ACCOUNT_UNBOUND')
        from .qualification import source_fingerprint
        fingerprint = source_fingerprint()
        result = validate_permit_against(
            book, action, purpose=purpose, now=now, permit_raw=permit_raw,
            request_raw=request_raw, account_key=account_key,
            policy=book.policy, fingerprint=fingerprint,
            allow_consumed=allow_consumed)
        if result['permit']['synthetic'] is not book.simulation:
            _fail('SUPERVISED_ENGINEERING_SYNTHETIC_MIXED')
        if book.simulation is False:
            if self.real_source_verifier is None:
                _fail('SUPERVISED_ENGINEERING_REAL_SOURCE_VERIFIER_UNAVAILABLE')
            verify = getattr(self.real_source_verifier, 'verify', None)
            if not callable(verify):
                _fail('SUPERVISED_ENGINEERING_REAL_VERIFIER_INTERFACE')
            evidence = verify(book, result, allow_ready=allow_consumed)
            if not isinstance(evidence, dict) or evidence.get('verified') is not True:
                reason = None
                if isinstance(evidence, dict):
                    reason = (evidence.get('reasons') or [None])[0]
                _fail('SUPERVISED_ENGINEERING_REAL_SOURCE_UNVERIFIED'
                      + (':' + str(reason) if reason else ''))
            evidence_now = self._now()
            try:
                evidence = _validate_source_evidence(
                    evidence, result=result, book=book, now=evidence_now)
            except SupervisedEngineeringError as exc:
                _fail('SUPERVISED_ENGINEERING_SOURCE_EVIDENCE_INVALID:'
                      + exc.code)
            result['real_source_evidence'] = evidence
            final_now = self._now()
            self._revalidate_owner_sources(
                book, action['id'], result, evidence, final_now,
                allow_consumed=allow_consumed)
        elif result['permit']['synthetic'] is not True or not self.allow_synthetic:
            _fail('SUPERVISED_ENGINEERING_SYNTHETIC_NOT_ENABLED')
        return result

    def validate_current_permit(self, book, *, action, purpose,
                                allow_consumed=False):
        """Read-only current permit/owner binding for an outer entry gate.

        This is the same validation used by assess/finalize/consume, exposed
        without creating a grant or writing.  Callers still perform their own
        transaction and one-shot consume; this method does not authorize a
        place or bypass native confirmation.
        """
        return self._validate(book, action=action, purpose=purpose,
                               allow_consumed=allow_consumed)

    def _revalidate_owner_sources(self, book, action_id, result, evidence, now,
                                  *, allow_consumed=False):
        """Re-read owner state after slow readers; no long write transaction."""
        request_after, permit_after = self._load()
        request_sha = (result.get('request') or {}).get('request_sha256')
        if digest(request_after) != request_sha:
            _fail('SUPERVISED_ENGINEERING_OWNER_REQUEST_CHANGED')
        raw_permit = (result.get('permit') or {}).get('permit')
        if not isinstance(raw_permit, dict) or digest(permit_after) != digest(raw_permit):
            _fail('SUPERVISED_ENGINEERING_OWNER_PERMIT_CHANGED')
        action = next((item for item in book.actions() if item['id'] == action_id), None)
        if action is None or action.get('kind') != 'entry':
            _fail('SUPERVISED_ENGINEERING_OWNER_ACTION_MISSING')
        allowed_states = {'REVIEW_PENDING', 'WAITING_USER'}
        if allow_consumed:
            allowed_states.add('READY')
        if action.get('state') not in allowed_states:
            _fail('SUPERVISED_ENGINEERING_OWNER_ACTION_STATE')
        original_action = result.get('action') or {}
        if (digest(action.get('payload')) != digest(original_action.get('payload'))
                or action.get('trade_id') != original_action.get('trade_id')
                or action.get('kind') != original_action.get('kind')):
            _fail('SUPERVISED_ENGINEERING_OWNER_ACTION_CHANGED')
        facts_after = book.current_account_facts(purpose='engineering')             if callable(getattr(book, 'current_account_facts', None)) else None
        if (not isinstance(facts_after, dict)
                or digest(facts_after) !=
                digest((result.get('facts') or {}).get('assessment'))
                or digest({key: value for key, value in facts_after.items()
                           if key != 'assessment_id'})
                != facts_after.get('assessment_id')
                or facts_after.get('assessment_id') !=
                evidence.get('facts_assessment_id')):
            _fail('SUPERVISED_ENGINEERING_OWNER_FACTS_CHANGED')
        review_after = _review_record(book, action_id)
        original_review = result.get('review') or {}
        review_fields = (
            'operation_id', 'parameters', 'response_hash',
            'parameters_sha256', 'order_checks_sha256',
            'customer_approval_required',
            'customer_approval_required_reason',
            'requires_native_confirmation', 'market_data_disclosure')
        if (any(review_after.get(key) != original_review.get(key)
                for key in review_fields)
                or review_after.get('recorded_at') !=
                original_review.get('recorded_at')
                or digest(review_after.get('payload')) !=
                digest(original_review.get('payload'))):
            _fail('SUPERVISED_ENGINEERING_OWNER_REVIEW_CHANGED')
        if evidence.get('risk_state_sha256') != digest(book.risk_guard.state()):
            _fail('SUPERVISED_ENGINEERING_OWNER_RISK_CHANGED')
        config = (book.policy or {}).get('current_capture')
        if not isinstance(config, dict) or not config.get('root'):
            _fail('SUPERVISED_ENGINEERING_OWNER_CAPTURE_MISSING')
        try:
            from pathlib import Path as _Path
            from .current_context import CurrentContextRegistry
            root = _Path(config['root'])
            record = CurrentContextRegistry(
                root / 'registry.json',
                allowed_capture_roots=(root,),
                allowed_public_roots=(_Path(config['public_manifest']).parent,),
                clock=self.clock).read()
        except Exception as exc:
            _fail('SUPERVISED_ENGINEERING_OWNER_CAPTURE_UNAVAILABLE:'
                  + str(getattr(exc, 'code', None) or type(exc).__name__))
        if (record.get('generation_id') != evidence.get('capture_generation')
                or record.get('account_key_hash') !=
                evidence.get('account_key')
                or record.get('policy_sha256') != evidence.get('policy_sha256')
                or record.get('source_fingerprint') !=
                evidence.get('source_fingerprint')):
            _fail('SUPERVISED_ENGINEERING_OWNER_CAPTURE_CHANGED')
        one_shot_id = (raw_permit or {}).get('one_shot_id')
        if not isinstance(one_shot_id, str) or not one_shot_id:
            _fail('SUPERVISED_ENGINEERING_OWNER_CONSUMPTION')
        existing = _consumption(book, one_shot_id)
        if allow_consumed:
            if existing is None:
                _fail('SUPERVISED_ENGINEERING_OWNER_CONSUMPTION_MISSING')
            request_sha = (result.get('request') or {}).get('request_sha256')
            if (existing[1] != request_sha
                    or existing[2] != result.get('binding')
                    or existing[3] != action_id):
                _fail('SUPERVISED_ENGINEERING_OWNER_CONSUMPTION_MISMATCH')
        elif existing is not None:
            _fail('SUPERVISED_ENGINEERING_PERMIT_REPLAYED')
        final_now = self._now()
        try:
            valid = _time(evidence.get('valid_until'), 'OWNER_WINDOW')
        except SupervisedEngineeringError:
            _fail('SUPERVISED_ENGINEERING_OWNER_WINDOW')
        if final_now > valid:
            _fail('SUPERVISED_ENGINEERING_OWNER_WINDOW')
        return True

    def validate_intent(self, book, *, plan, snapshot, evidence, signal_id=None):
        """Read-only exact request validation; never an entry authorization."""
        now = self._now()
        request_raw = _json_record(self.store, REQUEST_FILE,
                                   'SUPERVISED_ENGINEERING_REQUEST_INVALID')
        account_key = snapshot.get('account_key') if isinstance(snapshot, dict) else None
        if not isinstance(account_key, str) or not account_key:
            _fail('SUPERVISED_ENGINEERING_ACCOUNT_UNBOUND')
        from .qualification import source_fingerprint
        request = parse_request(
            request_raw, account_key=account_key,
            account_suffix=str(book.policy.get('account_suffix')),
            policy=book.policy, now=now, fingerprint=source_fingerprint())
        if signal_id is not None and request['signal_id'] != signal_id:
            _fail('SUPERVISED_ENGINEERING_SIGNAL_MISMATCH')
        if request['order'] != order_from_plan(plan):
            _fail('SUPERVISED_ENGINEERING_ORDER_MISMATCH')
        _check_plan_risk(plan, book.policy, request['order'])
        if request['synthetic'] is not book.simulation:
            _fail('SUPERVISED_ENGINEERING_SYNTHETIC_MIXED')
        return request

    def _component_from_result(self, result, action, now, *, evidence=None):
        from .qualification import source_pin
        permit = result['permit']
        review = result['review']
        request = result['request']
        expires = min(
            permit['valid_until'], request['valid_until'],
            result['facts']['observation_valid_until'],
            review['recorded_at'] + timedelta(seconds=MAX_REVIEW_AGE_SECONDS))
        evidence = (result.get('real_source_evidence')
                    if evidence is None else evidence)
        if permit['synthetic'] is False:
            if (not isinstance(evidence, dict)
                    or evidence.get('verified') is not True):
                _fail('SUPERVISED_ENGINEERING_REAL_SOURCE_EVIDENCE_REQUIRED')
            permit_source = permit['permit'].get('source') or {}
            if (evidence.get('source_fingerprint') !=
                    permit_source.get('source_fingerprint')
                    or evidence.get('policy_sha256') !=
                    permit_source.get('policy_sha256')
                    or evidence.get('one_shot_id') != permit['one_shot_id']):
                _fail('SUPERVISED_ENGINEERING_COMPONENT_EVIDENCE_SOURCE')
            try:
                evidence_valid = _time(evidence.get('valid_until'),
                                       'COMPONENT_EVIDENCE_TIME')
                evidence_risk_valid = _time(evidence.get('risk_valid_until'),
                                            'COMPONENT_EVIDENCE_TIME')
            except SupervisedEngineeringError:
                _fail('SUPERVISED_ENGINEERING_COMPONENT_EVIDENCE_TIME')
            expires = min(expires, evidence_valid, evidence_risk_valid)
        not_before = max(
            permit['not_before'], request['not_before'], permit['issued_at'],
            result['facts']['observed_at'], review['recorded_at'])
        if not_before >= expires:
            _fail('SUPERVISED_ENGINEERING_PERMIT_WINDOW')
        if now.astimezone(timezone.utc) > expires:
            _fail('SUPERVISED_ENGINEERING_PERMIT_WINDOW')
        component = {
            'status': 'allowed', 'reason': None, 'reasons': [],
            'schema_valid': True, 'provenance_proven': True,
            'supervised_synthetic': permit['synthetic'],
            'synthetic_test': permit['synthetic'],
            'real_source_verified': (
                isinstance(evidence, dict)
                and evidence.get('verified') is True),
            'source_evidence_id': (evidence or {}).get('source_sha256'),
            'real_source_evidence_sha256': (evidence or {}).get(
                'source_sha256'),
            'real_source_evidence_valid_until': (evidence or {}).get(
                'valid_until'),
            'risk_lineage_id': (evidence or {}).get('risk_lineage_id'),
            'risk_state_sha256': (evidence or {}).get('risk_state_sha256'),
            'risk_valid_until': (evidence or {}).get('risk_valid_until'),
            'provenance_sha256': (evidence or {}).get('provenance_sha256'),
            'native_receipt_status': (evidence or {}).get(
                'native_receipt_status'),
            'native_origin': (evidence or {}).get('native_origin'),
            'native_confirmation_required': (evidence or {}).get(
                'native_confirmation_required'),
            'capture_generation': permit['facts']['capture_generation'],
            'activation_gate': ('ENABLED' if permit['synthetic'] is False
                                else 'SYNTHETIC_TEST'),
            'broker_effect_verified': False,
            'execution_authorized': False, 'entry_authorized': False,
            'one_shot_id': permit['one_shot_id'],
            'permit_id': permit['permit']['permit_id'],
            'permit_sha256': digest(permit['permit']),
            'request_sha256': request['request_sha256'],
            'binding': result['binding'],
            'action_id': action.get('id'),
            'action_payload_sha256': digest(action.get('payload')),
            'account_key': permit['permit']['account_key'],
            'policy_sha256': permit['permit']['source']['policy_sha256'],
            'source_fingerprint':
                permit['permit']['source']['source_fingerprint'],
            'source_pin': source_pin(),
            'facts_assessment_id': permit['facts']['assessment_id'],
            'facts_valid_until': permit['facts']['observation_valid_until'],
            'review_operation_id': permit['formal']['review_operation_id'],
            'review_response_hash': permit['formal']['review_response_hash'],
            'review_recorded_at': permit['formal']['review_recorded_at'],
            'permit_valid_until': permit['valid_until'].isoformat(),
            'request_valid_until': request['valid_until'].isoformat(),
            'confirmation_mode': permit['formal']['confirmation_mode'],
            'assessed_at': now.isoformat(),
            'not_before': not_before.isoformat(),
            'issued_at': permit['issued_at'].isoformat(),
            'facts_observed_at': result['facts']['observed_at'].isoformat(),
            'expires_at': expires.isoformat(),
        }
        return component, expires, not_before

    def assess(self, *, book, mode, action=None, plan=None, purpose=None,
               **kwargs):
        if book.simulation is False and not self.real_activation_enabled:
            reason = ('SUPERVISED_ENGINEERING_REAL_SOURCE_VERIFIER_UNAVAILABLE'
                      if self.real_source_verifier is None else
                      'SUPERVISED_ENGINEERING_REAL_ACTIVATION_DISABLED')
            return {'allowed': False, 'status': 'blocked',
                    'reasons': [reason],
                    'sanitized': {
                        'status': 'blocked', 'reason': reason,
                        'reasons': [reason],
                        'execution_authorized': False,
                        'entry_authorized': False,
                        'activation_gate': 'DISABLED'}}
        try:
            if mode not in {'proposal', 'claim'} or action is None:
                _fail('SUPERVISED_ENGINEERING_ACTION_REQUIRED')
            result = self._validate(book, action=action, purpose=purpose)
        except SupervisedEngineeringError as exc:
            return {'allowed': False, 'status': 'blocked',
                    'reasons': [exc.code],
                    'sanitized': {'status': 'blocked', 'reason': exc.code,
                                  'reasons': [exc.code]}}
        component, expires, not_before = self._component_from_result(
            result, action, self._now())
        if self._now() > expires:
            return {'allowed': False, 'status': 'blocked',
                    'reasons': ['SUPERVISED_ENGINEERING_PERMIT_WINDOW'],
                    'sanitized': {'status': 'blocked',
                                  'reason':
                                      'SUPERVISED_ENGINEERING_PERMIT_WINDOW',
                                  'reasons': [
                                      'SUPERVISED_ENGINEERING_PERMIT_WINDOW'],
                                  'execution_authorized': False,
                                  'entry_authorized': False,
                                  'activation_gate': 'DISABLED'}}
        # policy hash is a component binding; keep it injected by the caller
        component['policy_sha256'] = digest(book.policy)
        evidence = result.get('real_source_evidence')
        grant = None
        if component['supervised_synthetic'] is False:
            token = secrets.token_urlsafe(32)
            self._real_grants[token] = {
                'component_sha256': digest(component),
                'action_id': action.get('id'),
                'book_id': id(book),
                'db_path': getattr(book, 'db_path', None),
                'account_key': component.get('account_key'),
                'source_pin': component.get('source_pin'),
                'evidence': deepcopy(evidence),
                'expires_at': expires,
                'not_before': not_before,
            }
            grant = _RealFinalizationGrant(token)
        return {'allowed': True, 'status': 'allowed', 'reasons': [],
                'schema_valid': True, 'provenance_proven': True,
                'sanitized': component, 'component': component,
                'binding': result['binding'],
                'source_evidence': result.get('real_source_evidence'),
                'finalization_grant': grant}

    def dry_assess(self, *, book, action=None, purpose='engineering'):
        """Non-activating real-source dry assessment.

        It may verify owner-bound source evidence, but it never finalizes,
        consumes, claims or places.  Real activation remains disabled unless a
        separately reviewed gate is turned on.
        """
        if action is None:
            return {'allowed': False, 'status': 'blocked',
                    'reasons': ['SUPERVISED_ENGINEERING_ACTION_REQUIRED'],
                    'decision_scope': 'DRY_ASSESSMENT',
                    'activation_gate': 'DISABLED',
                    'execution_authorized': False, 'entry_authorized': False}
        try:
            result = self._validate(book, action=action, purpose=purpose)
        except SupervisedEngineeringError as exc:
            return {'allowed': False, 'status': 'blocked',
                    'reasons': [exc.code], 'decision_scope': 'DRY_ASSESSMENT',
                    'activation_gate': 'DISABLED',
                    'execution_authorized': False, 'entry_authorized': False}
        component, expires, _ = self._component_from_result(
            result, action, self._now())
        if self._now() > expires:
            return {'allowed': False, 'status': 'blocked',
                    'reasons': ['SUPERVISED_ENGINEERING_PERMIT_WINDOW'],
                    'decision_scope': 'DRY_ASSESSMENT',
                    'activation_gate': 'DISABLED',
                    'execution_authorized': False, 'entry_authorized': False}
        component['policy_sha256'] = digest(book.policy)
        component['decision_scope'] = 'DRY_ASSESSMENT'
        component['dry_token'] = secrets.token_urlsafe(24)
        component['activation_gate'] = 'DISABLED'
        component['execution_authorized'] = False
        component['entry_authorized'] = False
        return {'allowed': True, 'status': 'dry_allowed', 'reasons': [],
                'component': component, 'sanitized': component,
                'binding': result['binding'],
                'source_evidence': result.get('real_source_evidence'),
                'decision_scope': 'DRY_ASSESSMENT',
                'activation_gate': 'DISABLED',
                'execution_authorized': False, 'entry_authorized': False}

    def finalize_promotion(self, book, component, action_id, now, *,
                           grant=None, evidence=None):
        """Bounded in-transaction revalidation before one-shot consumption.

        It re-reads owner request/permit files, the exact action row, cached
        facts and the native review; it never recomputes the full source
        fingerprint or capture compiler.  It takes a final owner clock after
        those reads, and rejects changed/revoked/expired state.

        Real mode requires the exact source-evidence object produced by the
        pre-transaction verifier.  The transaction validates its digest and
        owner bindings again; it never accepts a caller boolean or a fresh
        verifier call while holding the write lock.
        """
        if not isinstance(component, dict) or component.get('status') != 'allowed':
            _fail('SUPERVISED_ENGINEERING_COMPONENT_INVALID')
        if component.get('decision_scope') is not None:
            _fail('SUPERVISED_ENGINEERING_DRY_NOT_CONSUMABLE')
        synthetic = component.get('supervised_synthetic')
        if type(synthetic) is not bool or synthetic is not book.simulation:
            _fail('SUPERVISED_ENGINEERING_SYNTHETIC_MIXED')
        if synthetic is True:
            if book.simulation is not True:
                _fail('SUPERVISED_ENGINEERING_SYNTHETIC_MIXED')
        else:
            if book.simulation is not False:
                _fail('SUPERVISED_ENGINEERING_SYNTHETIC_MIXED')
            if not self.real_activation_enabled:
                _fail('SUPERVISED_ENGINEERING_REAL_ACTIVATION_DISABLED')
            if (component.get('real_source_verified') is not True
                    or component.get('activation_gate') != 'ENABLED'
                    or not isinstance(component.get('real_source_evidence_sha256'),
                                      str)):
                _fail('SUPERVISED_ENGINEERING_REAL_SOURCE_EVIDENCE_REQUIRED')
            if not isinstance(grant, _RealFinalizationGrant):
                _fail('SUPERVISED_ENGINEERING_REAL_ASSESSMENT_GRANT_REQUIRED')
            grant_record = self._real_grants.get(grant._token)
            if not isinstance(grant_record, dict):
                _fail('SUPERVISED_ENGINEERING_REAL_ASSESSMENT_GRANT_INVALID')
            if (grant_record.get('book_id') != id(book)
                    or grant_record.get('db_path') != getattr(book, 'db_path', None)
                    or grant_record.get('action_id') != action_id
                    or grant_record.get('account_key') != component.get('account_key')
                    or grant_record.get('source_pin') != component.get('source_pin')
                    or grant_record.get('component_sha256') != digest(component)):
                _fail('SUPERVISED_ENGINEERING_REAL_ASSESSMENT_GRANT_MISMATCH')
            if (not isinstance(grant_record.get('evidence'), dict)
                    or grant_record['evidence'].get('source_sha256') !=
                    component.get('real_source_evidence_sha256')):
                _fail('SUPERVISED_ENGINEERING_REAL_ASSESSMENT_GRANT_INVALID')
            if evidence is not None and evidence != grant_record['evidence']:
                _fail('SUPERVISED_ENGINEERING_REAL_SOURCE_EVIDENCE_MISMATCH')
            evidence = grant_record['evidence']
        if not isinstance(now, datetime) or now.tzinfo is None:
            _fail('SUPERVISED_ENGINEERING_CLOCK_INVALID')
        now = now.astimezone(timezone.utc)
        if synthetic is False and now > grant_record['expires_at'].astimezone(
                timezone.utc):
            _fail('SUPERVISED_ENGINEERING_REAL_ASSESSMENT_GRANT_EXPIRED')
        action = next((item for item in book.actions()
                       if item['id'] == action_id), None)
        if action is None:
            _fail('SUPERVISED_ENGINEERING_ACTION_REQUIRED')
        if action.get('state') not in {'REVIEW_PENDING', 'WAITING_USER'}:
            _fail('SUPERVISED_ENGINEERING_PROMOTION_STATE')
        if digest(action.get('payload')) != component.get('action_payload_sha256'):
            _fail('SUPERVISED_ENGINEERING_ACTION_CHANGED')
        if digest(book.policy) != component.get('policy_sha256'):
            _fail('SUPERVISED_ENGINEERING_POLICY_CHANGED')
        if book.state().get('account_key') != component.get('account_key'):
            _fail('SUPERVISED_ENGINEERING_ACCOUNT_CHANGED')
        from .qualification import source_pin
        if source_pin() != component.get('source_pin'):
            _fail('SUPERVISED_ENGINEERING_SOURCE_CHANGED')
        request_raw, permit_raw = self._load()
        if digest(request_raw) != component.get('request_sha256') \
                or digest(permit_raw) != component.get('permit_sha256'):
            _fail('SUPERVISED_ENGINEERING_OWNER_SOURCE_CHANGED')
        result = validate_permit_against(
            book, action, purpose='engineering', now=now,
            permit_raw=permit_raw, request_raw=request_raw,
            account_key=component['account_key'], policy=book.policy,
            fingerprint=component['source_fingerprint'],
            allow_consumed=False)
        if synthetic is False:
            try:
                _validate_source_evidence(
                    evidence, result=result, book=book, now=now)
            except SupervisedEngineeringError:
                _fail('SUPERVISED_ENGINEERING_COMPONENT_EVIDENCE_INVALID')
            if (evidence.get('source_sha256') !=
                    component.get('real_source_evidence_sha256')
                    or evidence.get('source_sha256') !=
                    component.get('source_evidence_id')):
                _fail('SUPERVISED_ENGINEERING_COMPONENT_CHANGED')
            self._revalidate_owner_sources(
                book, action_id, result, evidence, now)
        fresh, expires, not_before = self._component_from_result(
            result, action, now, evidence=evidence)
        fresh['policy_sha256'] = digest(book.policy)
        if (fresh['binding'] != component.get('binding')
                or fresh['permit_id'] != component.get('permit_id')
                or fresh['one_shot_id'] != component.get('one_shot_id')
                or fresh['request_sha256'] != component.get('request_sha256')
                or fresh['supervised_synthetic'] != synthetic):
            _fail('SUPERVISED_ENGINEERING_COMPONENT_CHANGED')
        for key in ('real_source_verified', 'real_source_evidence_sha256',
                    'risk_lineage_id', 'risk_state_sha256',
                    'native_receipt_status', 'native_origin',
                    'capture_generation'):
            if fresh.get(key) != component.get(key):
                _fail('SUPERVISED_ENGINEERING_COMPONENT_CHANGED')
        # Re-read every owner/current input after the bounded checks.  A
        # revocation, replacement, deletion or facts/review change during the
        # check window must invalidate the old decision.
        request_after, permit_after = self._load()
        if digest(request_after) != component.get('request_sha256') \
                or digest(permit_after) != component.get('permit_sha256'):
            _fail('SUPERVISED_ENGINEERING_OWNER_SOURCE_CHANGED')
        if source_pin() != component.get('source_pin'):
            _fail('SUPERVISED_ENGINEERING_SOURCE_CHANGED')
        facts_after = getattr(book, 'current_account_facts', lambda **_: None)(
            purpose='engineering')
        if (not isinstance(facts_after, dict)
                or facts_after.get('assessment_id') !=
                component.get('facts_assessment_id')):
            _fail('SUPERVISED_ENGINEERING_FACTS_CHANGED')
        review_after = _review_record(book, action_id)
        if (review_after['response_hash'] != component.get('review_response_hash')
                or review_after['operation_id'] !=
                component.get('review_operation_id')):
            _fail('SUPERVISED_ENGINEERING_REVIEW_CHANGED')
        after = self._now()
        if after < not_before or after > expires:
            _fail('SUPERVISED_ENGINEERING_PERMIT_WINDOW')
        result_finalization = {'component': fresh, 'finalized_at': after,
                'not_before': not_before, 'expires_at': expires,
                'component_sha256': digest(fresh), 'action_id': action_id,
                'source_evidence': (evidence if synthetic is False else None)}
        if synthetic is False:
            token = secrets.token_urlsafe(32)
            self._real_finalizations[token] = {
                'component_sha256': digest(fresh),
                'action_id': action_id,
                'book_id': id(book),
                'db_path': getattr(book, 'db_path', None),
                'account_key': component.get('account_key'),
                'source_pin': component.get('source_pin'),
                'evidence': deepcopy(evidence),
                'expires_at': expires,
            }
            self._real_grants.pop(grant._token, None)
            result_finalization['finalization_token'] = token
        return result_finalization

    def _check_consume_window(self, component, finalized, current):
        expires = finalized.get('expires_at')
        not_before = finalized.get('not_before')
        finalized_at = finalized.get('finalized_at')
        if (not isinstance(expires, datetime)
                or not isinstance(not_before, datetime)
                or not isinstance(finalized_at, datetime)
                or expires.tzinfo is None or not_before.tzinfo is None
                or finalized_at.tzinfo is None):
            _fail('SUPERVISED_ENGINEERING_FINALIZATION_INVALID')
        current = current.astimezone(timezone.utc)
        issued = _time(component.get('issued_at'),
                       'SUPERVISED_ENGINEERING_PERMIT_TIME')
        facts_at = _time(component.get('facts_observed_at'),
                         'SUPERVISED_ENGINEERING_FACTS_TIME')
        review_at = _time(component.get('review_recorded_at'),
                          'SUPERVISED_ENGINEERING_REVIEW_TIME')
        expires = expires.astimezone(timezone.utc)
        not_before = not_before.astimezone(timezone.utc)
        finalized_at = finalized_at.astimezone(timezone.utc)
        if (issued > not_before or not_before > finalized_at
                or finalized_at > expires
                or facts_at > finalized_at or review_at > finalized_at):
            _fail('SUPERVISED_ENGINEERING_TIME_REVERSED')
        if current < not_before or current < finalized_at or current > expires:
            _fail('SUPERVISED_ENGINEERING_PERMIT_WINDOW')
        return current

    def final_consume_check(self, finalized, component, current=None):
        """Final bounded window check after audits, before owner commit."""
        if not isinstance(finalized, dict) or not isinstance(component, dict):
            _fail('SUPERVISED_ENGINEERING_FINALIZATION_INVALID')
        if finalized.get('component_sha256') != digest(component):
            _fail('SUPERVISED_ENGINEERING_FINALIZATION_INVALID')
        return self._check_consume_window(
            component, finalized, current or self._now())

    def _revalidate_consume_owner(self, book, component, action_id):
        """Bounded owner re-read immediately around the one-shot insert.

        The slow source verifier already ran before the transaction.  This
        check reads only local owner rows/files and rejects a changed request,
        permit, action, facts, review, risk state, account or source pin.
        """
        from .qualification import source_pin
        if book.state().get('account_key') != component.get('account_key'):
            _fail('SUPERVISED_ENGINEERING_ACCOUNT_CHANGED')
        if digest(book.policy) != component.get('policy_sha256'):
            _fail('SUPERVISED_ENGINEERING_POLICY_CHANGED')
        if source_pin() != component.get('source_pin'):
            _fail('SUPERVISED_ENGINEERING_SOURCE_CHANGED')
        request_raw, permit_raw = self._load()
        if (digest(request_raw) != component.get('request_sha256')
                or digest(permit_raw) != component.get('permit_sha256')):
            _fail('SUPERVISED_ENGINEERING_OWNER_SOURCE_CHANGED')
        action = next((item for item in book.actions()
                       if item['id'] == action_id), None)
        if action is None:
            _fail('SUPERVISED_ENGINEERING_ACTION_REQUIRED')
        if digest(action.get('payload')) != component.get('action_payload_sha256'):
            _fail('SUPERVISED_ENGINEERING_ACTION_CHANGED')
        facts = getattr(book, 'current_account_facts', lambda **_: None)(
            purpose='engineering')
        if (not isinstance(facts, dict)
                or facts.get('assessment_id') !=
                component.get('facts_assessment_id')):
            _fail('SUPERVISED_ENGINEERING_FACTS_CHANGED')
        review = _review_record(book, action_id)
        if (review['response_hash'] != component.get('review_response_hash')
                or review['operation_id'] !=
                component.get('review_operation_id')):
            _fail('SUPERVISED_ENGINEERING_REVIEW_CHANGED')
        expected_risk = component.get('risk_state_sha256')
        if expected_risk is not None \
                and expected_risk != digest(book.risk_guard.state()):
            _fail('SUPERVISED_ENGINEERING_OWNER_RISK_CHANGED')
        return action

    def _revalidate_first_consume_real(self, book, component, action_id,
                                        evidence):
        """Full bounded owner re-read for a first real consumption.

        Reuses the existing permit/action/facts/review/risk/capture validators;
        the slow source verifier already ran before the finalization grant.  No
        READY, UNKNOWN, terminal, replay or partially changed owner state can
        reach the first insert.
        """
        from .qualification import source_pin
        if book.state().get('account_key') != component.get('account_key'):
            _fail('SUPERVISED_ENGINEERING_ACCOUNT_CHANGED')
        if digest(book.policy) != component.get('policy_sha256'):
            _fail('SUPERVISED_ENGINEERING_POLICY_CHANGED')
        current_pin = source_pin()
        if (not isinstance(current_pin, dict)
                or current_pin.get('status') != 'bounded'
                or current_pin.get('truncated') is not False):
            _fail('SUPERVISED_ENGINEERING_SOURCE_PIN_UNAVAILABLE')
        if current_pin != component.get('source_pin'):
            _fail('SUPERVISED_ENGINEERING_SOURCE_CHANGED')
        request_raw, permit_raw = self._load()
        action = next((item for item in book.actions()
                       if item['id'] == action_id), None)
        if action is None:
            _fail('SUPERVISED_ENGINEERING_ACTION_REQUIRED')
        now = self._now()
        result = validate_permit_against(
            book, action, purpose='engineering', now=now,
            permit_raw=permit_raw, request_raw=request_raw,
            account_key=component.get('account_key'), policy=book.policy,
            fingerprint=component.get('source_fingerprint'),
            allow_consumed=False)
        permit = result.get('permit') or {}
        raw_permit = (permit.get('permit')
                      if isinstance(permit.get('permit'), dict) else permit)
        if permit.get('synthetic') is not False:
            _fail('SUPERVISED_ENGINEERING_SYNTHETIC_MIXED')
        if (result.get('binding') != component.get('binding')
                or raw_permit.get('permit_id') != component.get('permit_id')
                or result.get('request', {}).get('request_sha256') !=
                component.get('request_sha256')):
            _fail('SUPERVISED_ENGINEERING_COMPONENT_CHANGED')
        try:
            _validate_source_evidence(
                evidence, result=result, book=book, now=now)
        except SupervisedEngineeringError:
            _fail('SUPERVISED_ENGINEERING_COMPONENT_EVIDENCE_INVALID')
        self._revalidate_owner_sources(
            book, action_id, result, evidence, now)
        return action

    def consume(self, book, component, action_id, now, *, finalized=None):
        if not isinstance(component, dict) or component.get('status') != 'allowed':
            return None
        if component.get('decision_scope') is not None:
            _fail('SUPERVISED_ENGINEERING_DRY_NOT_CONSUMABLE')
        synthetic = component.get('supervised_synthetic')
        if type(synthetic) is not bool or synthetic is not book.simulation:
            _fail('SUPERVISED_ENGINEERING_SYNTHETIC_MIXED')
        if synthetic is False:
            if (not self.real_activation_enabled
                    or component.get('real_source_verified') is not True
                    or component.get('activation_gate') != 'ENABLED'
                    or not isinstance(component.get('real_source_evidence_sha256'),
                                      str)):
                _fail('SUPERVISED_ENGINEERING_REAL_SOURCE_UNVERIFIED')
        elif book.simulation is not True:
            _fail('SUPERVISED_ENGINEERING_SYNTHETIC_MIXED')
        one_shot = component.get('one_shot_id')
        binding = component.get('binding')
        source_sha = component.get('request_sha256')
        if (not isinstance(one_shot, str) or not one_shot
                or not isinstance(binding, str) or not _HEX64.fullmatch(binding)
                or not isinstance(source_sha, str) or not _HEX64.fullmatch(source_sha)):
            _fail('SUPERVISED_ENGINEERING_COMPONENT_INVALID')
        existing = _consumption(book, one_shot)
        if existing is not None:
            if (existing[1] == source_sha and existing[2] == binding
                    and existing[3] == action_id):
                self._revalidate_consume_owner(book, component, action_id)
                result = (dict(finalized) if isinstance(finalized, dict)
                          else {'component': component})
                result['consumed_at'] = _time(
                    existing[4], 'SUPERVISED_ENGINEERING_CONSUMPTION_TIME')
                result['binding'] = existing[2]
                result['replayed'] = True
                return result
            _fail('SUPERVISED_ENGINEERING_PERMIT_REPLAYED')
        if finalized is None:
            if synthetic is False:
                _fail('SUPERVISED_ENGINEERING_REAL_ASSESSMENT_GRANT_REQUIRED')
            finalized = self.finalize_promotion(book, component, action_id, now)
        if synthetic is False:
            token = (finalized.get('finalization_token')
                     if isinstance(finalized, dict) else None)
            record = self._real_finalizations.get(token)                 if isinstance(token, str) else None
            if not isinstance(record, dict):
                _fail('SUPERVISED_ENGINEERING_REAL_FINALIZATION_INVALID')
            if (record.get('book_id') != id(book)
                    or record.get('db_path') != getattr(book, 'db_path', None)
                    or record.get('action_id') != action_id
                    or record.get('component_sha256') != digest(component)
                    or record.get('account_key') != component.get('account_key')
                    or record.get('source_pin') != component.get('source_pin')
                    or record.get('evidence') != finalized.get('source_evidence')):
                _fail('SUPERVISED_ENGINEERING_REAL_FINALIZATION_MISMATCH')
            try:
                expired = (self._now() >
                           record['expires_at'].astimezone(timezone.utc))
            except (KeyError, TypeError, ValueError, SupervisedEngineeringError):
                _fail('SUPERVISED_ENGINEERING_REAL_FINALIZATION_INVALID')
            if expired:
                _fail('SUPERVISED_ENGINEERING_REAL_FINALIZATION_EXPIRED')
        if (not isinstance(finalized, dict)
                or finalized.get('component_sha256') != digest(component)
                or finalized.get('action_id') != action_id):
            _fail('SUPERVISED_ENGINEERING_FINALIZATION_INVALID')
        try:
            if (_time(finalized.get('expires_at'),
                     'SUPERVISED_ENGINEERING_FINALIZATION_INVALID')
                    != _time(component.get('expires_at'),
                             'SUPERVISED_ENGINEERING_FINALIZATION_INVALID')
                    or _time(finalized.get('not_before'),
                             'SUPERVISED_ENGINEERING_FINALIZATION_INVALID')
                    != _time(component.get('not_before'),
                             'SUPERVISED_ENGINEERING_FINALIZATION_INVALID')):
                _fail('SUPERVISED_ENGINEERING_FINALIZATION_INVALID')
        except SupervisedEngineeringError:
            _fail('SUPERVISED_ENGINEERING_FINALIZATION_INVALID')
        current = self._now()
        self._check_consume_window(component, finalized, current)
        if synthetic is False:
            action = self._revalidate_first_consume_real(
                book, component, action_id, record['evidence'])
        else:
            action = self._revalidate_consume_owner(book, component, action_id)
        book.db.execute(
            'CREATE TABLE IF NOT EXISTS ' + CONSUMPTION_TABLE +
            '(one_shot_id TEXT PRIMARY KEY,source_sha256 TEXT NOT NULL,'
            'binding TEXT NOT NULL,action_id TEXT NOT NULL,'
            'consumed_at TEXT NOT NULL)')
        consumed_at = self._now()
        self._check_consume_window(component, finalized, consumed_at)
        book.db.execute(
            'INSERT INTO ' + CONSUMPTION_TABLE + ' VALUES(?,?,?,?,?)',
            (one_shot, source_sha, binding, action_id,
             consumed_at.isoformat()))
        book.audit.append_in_transaction({
            'event_id': 'supervised-permit-' + one_shot + '-consumed',
            'occurred_at': consumed_at.isoformat(),
            'event_type': 'RECONCILIATION_ISSUE', 'actor': 'script',
            'synthetic': synthetic, 'trade_id': action['trade_id'],
            'details': {'status': 'SUPERVISED_ENGINEERING_PERMIT_CONSUMED',
                        'permit_id': component.get('permit_id'),
                        'one_shot_id': one_shot, 'binding': binding,
                        'action_id': action_id,
                        'account_key': book.state().get('account_key'),
                        'policy_sha256': digest(book.policy),
                        'source_fingerprint':
                            component.get('source_fingerprint'),
                        'synthetic': synthetic,
                        'execution_authorized': False,
                        'synthetic_only': synthetic is True},
        })
        if synthetic is False and isinstance(finalized.get('finalization_token'), str):
            self._real_finalizations.pop(finalized['finalization_token'], None)
        result = dict(finalized)
        result['consumed_at'] = consumed_at
        result['binding'] = binding
        return result

    def recheck_receipt(self, component, *, book, action=None, purpose=None,
                        evidence=None):
        if not isinstance(component, dict) or component.get('status') != 'allowed':
            return []
        synthetic = component.get('supervised_synthetic')
        if type(synthetic) is not bool or synthetic is not book.simulation:
            return ['SUPERVISED_ENGINEERING_SYNTHETIC_MIXED']
        if synthetic is False:
            if (not self.real_activation_enabled
                    or component.get('real_source_verified') is not True
                    or component.get('activation_gate') != 'ENABLED'
                    or not isinstance(component.get('real_source_evidence_sha256'),
                                      str)):
                return ['SUPERVISED_ENGINEERING_REAL_SOURCE_UNVERIFIED']
        elif book.simulation is not True:
            return ['SUPERVISED_ENGINEERING_SYNTHETIC_MIXED']
        if action is None:
            return ['SUPERVISED_ENGINEERING_ACTION_REQUIRED']
        try:
            result = self._validate(book, action=action, purpose=purpose,
                                    allow_consumed=True)
            fresh, _expires, _not_before = self._component_from_result(
                result, action, self._now())
            for key in ('binding', 'one_shot_id', 'permit_id',
                        'request_sha256', 'supervised_synthetic',
                        'risk_lineage_id', 'risk_state_sha256',
                        'native_receipt_status', 'native_origin',
                        'capture_generation'):
                if fresh.get(key) != component.get(key):
                    return ['SUPERVISED_ENGINEERING_COMPONENT_CHANGED']
        except SupervisedEngineeringError as exc:
            return [exc.code]
        return []
