"""Formal, owner-sourced entry authorization contracts.

These records are deliberately not a policy switch.  They are read from a
private owner directory, bound to one account/purpose/exact order/protection
plan, expire, and are consumed once.  A caller cannot create a permit by
putting a boolean, dict, receipt, or test double on an action.  Synthetic
records are accepted only by an explicitly test-scoped authority on a
``simulation=True`` book; a real book rejects them.

The module is read-only except for the one-shot consumption row, which is
written by ``ExecutionBook.propose`` inside its existing transaction.
"""
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat

from trading_contracts.values import digest, timestamp
from .qualification import candidate_root


SCHEMA_VERSION = 1
SOURCE_USER = 'owner_user_order_authorization_v1'
SOURCE_CHANNEL = 'owner_formal_tool_contract_v1'
SOURCE_PROTECTION = 'owner_protection_attestation_v1'
SOURCE_SYNTHETIC = 'synthetic_test_v1'
SOURCE_TYPES = {SOURCE_USER, SOURCE_CHANNEL, SOURCE_PROTECTION,
                SOURCE_SYNTHETIC}
SOURCE_FILES = {
    'user': 'user-order-authorization.json',
    'channel': 'formal-channel-attestation.json',
    'protection': 'protection-attestation.json',
}
MAX_SOURCE_BYTES = 65536
MAX_PROVENANCE_BYTES = 2000000
MAX_PROTECTION_EVIDENCE_BYTES = 262144
MAX_TEXT = 400
CONSUMPTION_TABLE = 'formal_authorization_consumptions'
PURPOSES = {'engineering', 'strategy'}
FORMAL_BINDING_KIND = 'formal_entry_authorization_binding_v1'
USER_PROVENANCE_KIND = 'engineering_plan_row'
CHANNEL_PROVENANCE_KIND = 'official_tool_metadata_capture'
PROTECTION_PROVENANCE_KIND = 'owner_protection_evidence_v1'
PROTECTION_PRODUCERS = {'owner_protection_audit_v1'}
PROJECT_ROOT = candidate_root()
CHANNEL_REQUIRED = ('review_equity_order', 'place_equity_order')
PROTECTION_REQUIRED = ('review_advanced_order', 'place_advanced_order')
_HEX64 = re.compile(r'[0-9a-f]{64}\Z')


class FormalSourceError(ValueError):
    """A fail-closed source/validation error with no sensitive detail."""

    def __init__(self, code):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class UserOrderAuthorization:
    source_id: str
    source_sha256: str
    source_type: str
    account_key: str
    account_suffix: str
    purpose: str
    project_scope_ref: str
    order_authorization_ref: str
    parameters: dict
    parameters_sha256: str
    authorized_at: str
    not_before: str
    valid_until: str
    one_shot_id: str
    synthetic: bool
    provenance: dict | None

    def sanitized(self):
        return {
            'source_id': self.source_id,
            'source_sha256': self.source_sha256,
            'source_type': self.source_type,
            'account_key': self.account_key,
            'account_suffix': self.account_suffix,
            'purpose': self.purpose,
            'project_scope_ref': self.project_scope_ref,
            'order_authorization_ref': self.order_authorization_ref,
            'parameters': dict(self.parameters),
            'parameters_sha256': self.parameters_sha256,
            'authorized_at': self.authorized_at,
            'not_before': self.not_before,
            'valid_until': self.valid_until,
            'one_shot_id': self.one_shot_id,
            'synthetic': self.synthetic,
            'provenance': _sanitized_provenance(self.provenance),
        }


@dataclass(frozen=True)
class FormalChannelAttestation:
    source_id: str
    source_sha256: str
    source_type: str
    account_key: str
    account_suffix: str
    order_authorization_ref: str
    tool_identity: tuple
    review_contract_sha256: str
    place_contract_sha256: str
    review_semantics: str
    place_semantics: str
    app_confirmation_supported: bool
    explicit_user_confirmation_supported: bool
    local_bypass_allowed: bool
    captured_at: str
    valid_until: str
    synthetic: bool
    provenance: dict | None

    def sanitized(self):
        return {
            'source_id': self.source_id,
            'source_sha256': self.source_sha256,
            'source_type': self.source_type,
            'account_key': self.account_key,
            'account_suffix': self.account_suffix,
            'order_authorization_ref': self.order_authorization_ref,
            'tool_identity': list(self.tool_identity),
            'review_contract_sha256': self.review_contract_sha256,
            'place_contract_sha256': self.place_contract_sha256,
            'review_semantics': self.review_semantics,
            'place_semantics': self.place_semantics,
            'app_confirmation_supported': self.app_confirmation_supported,
            'explicit_user_confirmation_supported':
                self.explicit_user_confirmation_supported,
            'local_bypass_allowed': self.local_bypass_allowed,
            'captured_at': self.captured_at,
            'valid_until': self.valid_until,
            'synthetic': self.synthetic,
            'provenance': _sanitized_provenance(self.provenance),
        }


@dataclass(frozen=True)
class ProtectionAttestation:
    source_id: str
    source_sha256: str
    source_type: str
    account_key: str
    account_suffix: str
    order_authorization_ref: str
    symbol: str
    quantity: str
    stop_price: str
    protection_mode: str
    protection_tool_identity: tuple
    protection_contract_sha256: str
    feasible_before_entry: bool
    max_submit_seconds: str
    failure_exit_path: str
    failure_exit_verified: bool
    captured_at: str
    valid_until: str
    synthetic: bool
    provenance: dict | None

    def sanitized(self):
        return {
            'source_id': self.source_id,
            'source_sha256': self.source_sha256,
            'source_type': self.source_type,
            'account_key': self.account_key,
            'account_suffix': self.account_suffix,
            'order_authorization_ref': self.order_authorization_ref,
            'symbol': self.symbol,
            'quantity': self.quantity,
            'stop_price': self.stop_price,
            'protection_mode': self.protection_mode,
            'protection_tool_identity': list(self.protection_tool_identity),
            'protection_contract_sha256': self.protection_contract_sha256,
            'feasible_before_entry': self.feasible_before_entry,
            'max_submit_seconds': self.max_submit_seconds,
            'failure_exit_path': self.failure_exit_path,
            'failure_exit_verified': self.failure_exit_verified,
            'captured_at': self.captured_at,
            'valid_until': self.valid_until,
            'synthetic': self.synthetic,
            'provenance': _sanitized_provenance(self.provenance),
        }


@dataclass(frozen=True)
class FormalSources:
    user: UserOrderAuthorization
    channel: FormalChannelAttestation
    protection: ProtectionAttestation


def order_parameters_from_plan(plan):
    """Exact native entry terms; no caller-selected substitutions."""
    if not isinstance(plan, dict):
        raise FormalSourceError('FORMAL_ORDER_PARAMETERS_INVALID')
    try:
        parameters = {
            'symbol': plan['symbol'],
            'side': 'buy',
            'type': 'limit',
            'quantity': str(plan['quantity']),
            'limit_price': str(plan['entry_ceiling']),
            'market_hours': 'regular_hours',
            'time_in_force': 'gfd',
        }
    except (KeyError, TypeError, ValueError):
        raise FormalSourceError('FORMAL_ORDER_PARAMETERS_INVALID') from None
    if (not isinstance(parameters['symbol'], str) or not parameters['symbol']
            or not isinstance(parameters['quantity'], str)
            or not parameters['quantity']
            or not isinstance(parameters['limit_price'], str)
            or not parameters['limit_price']):
        raise FormalSourceError('FORMAL_ORDER_PARAMETERS_INVALID')
    return parameters


def seal_record(body):
    """Add the record's self-consistency digest.  Not a trust primitive."""
    if not isinstance(body, dict):
        raise FormalSourceError('FORMAL_SOURCE_RECORD_INVALID')
    result = dict(body)
    result.pop('source_sha256', None)
    result['source_sha256'] = digest(result)
    return result


def _sanitized_provenance(value):
    if not isinstance(value, dict):
        return None
    result = {}
    for key in ('kind', 'plan_id', 'record_path', 'record_sha256',
                'record_id', 'producer', 'tool_contract_sha256'):
        if key in value:
            result[key] = value[key]
    return result


def _provenance(value, code):
    if value is None:
        return None
    if not isinstance(value, dict):
        raise FormalSourceError(code)
    return dict(value)


def _record_body(raw):
    if not isinstance(raw, dict):
        raise FormalSourceError('FORMAL_SOURCE_RECORD_INVALID')
    body = dict(raw)
    pin = body.get('source_sha256')
    if not isinstance(pin, str) or not _HEX64.fullmatch(pin):
        raise FormalSourceError('FORMAL_SOURCE_DIGEST_INVALID')
    unsigned = {key: value for key, value in body.items()
                if key != 'source_sha256'}
    if digest(unsigned) != pin:
        raise FormalSourceError('FORMAL_SOURCE_DIGEST_MISMATCH')
    return body


def _text(value, code):
    if not isinstance(value, str) or not value or len(value) > MAX_TEXT:
        raise FormalSourceError(code)
    return value


def _hex64(value, code):
    if not isinstance(value, str) or not _HEX64.fullmatch(value):
        raise FormalSourceError(code)
    return value


def _boolean(value, code):
    if type(value) is not bool:
        raise FormalSourceError(code)
    return value


def _time(value, code):
    if not isinstance(value, str) or not value:
        raise FormalSourceError(code)
    try:
        parsed = timestamp(value)
    except (TypeError, ValueError, OverflowError):
        raise FormalSourceError(code) from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise FormalSourceError(code)
    return parsed


def _window(body, *, now, prefix):
    captured = _time(body.get('captured_at'), prefix + '_CAPTURED_AT_INVALID')
    not_before = _time(body.get('not_before', body.get('captured_at')),
                       prefix + '_NOT_BEFORE_INVALID')
    valid_until = _time(body.get('valid_until'), prefix + '_VALID_UNTIL_INVALID')
    if captured > now:
        raise FormalSourceError(prefix + '_CAPTURED_FUTURE')
    if now > valid_until:
        raise FormalSourceError(prefix + '_EXPIRED')
    if valid_until <= not_before:
        raise FormalSourceError(prefix + '_WINDOW_INVALID')
    if now < not_before:
        raise FormalSourceError(prefix + '_NOT_YET_VALID')
    return captured, valid_until


def _source_type(body, *, kind, allow_synthetic):
    source_type = body.get('source_type')
    if source_type not in SOURCE_TYPES:
        raise FormalSourceError('FORMAL_SOURCE_TYPE_INVALID')
    synthetic = _boolean(body.get('synthetic'), 'FORMAL_SYNTHETIC_FLAG_INVALID')
    if (source_type == SOURCE_SYNTHETIC) is not synthetic:
        raise FormalSourceError('FORMAL_SOURCE_TYPE_SYNTHETIC_MISMATCH')
    if synthetic and not allow_synthetic:
        raise FormalSourceError('FORMAL_SYNTHETIC_SOURCE_NOT_ALLOWED')
    if not synthetic and source_type != kind:
        raise FormalSourceError('FORMAL_SOURCE_TYPE_KIND_MISMATCH')
    return source_type, synthetic


def parse_user_order_authorization(raw, *, now, expected_account_key,
                                   expected_account_suffix, expected_purpose,
                                   expected_parameters,
                                   allow_synthetic=False):
    body = _record_body(raw)
    if body.get('schema_version') != SCHEMA_VERSION:
        raise FormalSourceError('FORMAL_USER_SCHEMA_INVALID')
    if body.get('kind') != 'user_order_authorization':
        raise FormalSourceError('FORMAL_USER_KIND_INVALID')
    source_type, synthetic = _source_type(
        body, kind=SOURCE_USER, allow_synthetic=allow_synthetic)
    account_key = _hex64(body.get('account_key'), 'FORMAL_USER_ACCOUNT_INVALID')
    if account_key != expected_account_key:
        raise FormalSourceError('FORMAL_USER_ACCOUNT_MISMATCH')
    suffix = _text(body.get('account_suffix'), 'FORMAL_USER_SUFFIX_INVALID')
    if suffix != expected_account_suffix:
        raise FormalSourceError('FORMAL_USER_SUFFIX_MISMATCH')
    purpose = _text(body.get('purpose'), 'FORMAL_USER_PURPOSE_INVALID')
    if purpose not in PURPOSES or purpose != expected_purpose:
        raise FormalSourceError('FORMAL_USER_PURPOSE_MISMATCH')
    project_scope_ref = _text(
        body.get('project_scope_ref'), 'FORMAL_USER_SCOPE_REF_INVALID')
    order_ref = _text(
        body.get('order_authorization_ref'), 'FORMAL_USER_ORDER_REF_INVALID')
    parameters = body.get('parameters')
    if not isinstance(parameters, dict) or parameters != expected_parameters:
        raise FormalSourceError('FORMAL_USER_PARAMETERS_MISMATCH')
    try:
        parameters_sha256 = digest(parameters)
    except (TypeError, ValueError):
        raise FormalSourceError('FORMAL_USER_PARAMETERS_INVALID') from None
    authorized_at = _text(
        body.get('authorized_at'), 'FORMAL_USER_AUTHORIZED_AT_INVALID')
    authorized = _time(authorized_at, 'FORMAL_USER_AUTHORIZED_AT_INVALID')
    captured, _ = _window(body, now=now, prefix='FORMAL_USER')
    if authorized > now:
        raise FormalSourceError('FORMAL_USER_AUTHORIZED_AT_FUTURE')
    if authorized > captured:
        raise FormalSourceError('FORMAL_USER_AUTHORIZED_AFTER_CAPTURE')
    if authorized >= _time(body.get('valid_until'),
                           'FORMAL_USER_VALID_UNTIL_INVALID'):
        raise FormalSourceError('FORMAL_USER_AUTHORIZED_AFTER_VALID_UNTIL')
    one_shot = _text(body.get('one_shot_id'), 'FORMAL_USER_ONE_SHOT_INVALID')
    if len(one_shot) > 200:
        raise FormalSourceError('FORMAL_USER_ONE_SHOT_INVALID')
    return UserOrderAuthorization(
        source_id=_text(body.get('source_id'), 'FORMAL_USER_SOURCE_ID_INVALID'),
        source_sha256=body['source_sha256'],
        source_type=source_type,
        account_key=account_key,
        account_suffix=suffix,
        purpose=purpose,
        project_scope_ref=project_scope_ref,
        order_authorization_ref=order_ref,
        parameters=dict(parameters),
        parameters_sha256=parameters_sha256,
        authorized_at=authorized_at,
        not_before=_text(body.get('not_before', body.get('captured_at')),
                         'FORMAL_USER_NOT_BEFORE_INVALID'),
        valid_until=_text(body.get('valid_until'),
                          'FORMAL_USER_VALID_UNTIL_INVALID'),
        one_shot_id=one_shot,
        synthetic=synthetic,
        provenance=_provenance(
            body.get('provenance'), 'FORMAL_USER_PROVENANCE_SCHEMA_INVALID'))


def parse_formal_channel_attestation(raw, *, now, expected_account_key,
                                     expected_account_suffix,
                                     expected_order_ref,
                                     allow_synthetic=False):
    body = _record_body(raw)
    if body.get('schema_version') != SCHEMA_VERSION:
        raise FormalSourceError('FORMAL_CHANNEL_SCHEMA_INVALID')
    if body.get('kind') != 'formal_channel_attestation':
        raise FormalSourceError('FORMAL_CHANNEL_KIND_INVALID')
    source_type, synthetic = _source_type(
        body, kind=SOURCE_CHANNEL, allow_synthetic=allow_synthetic)
    account_key = _hex64(body.get('account_key'),
                         'FORMAL_CHANNEL_ACCOUNT_INVALID')
    if account_key != expected_account_key:
        raise FormalSourceError('FORMAL_CHANNEL_ACCOUNT_MISMATCH')
    suffix = _text(body.get('account_suffix'),
                   'FORMAL_CHANNEL_SUFFIX_INVALID')
    if suffix != expected_account_suffix:
        raise FormalSourceError('FORMAL_CHANNEL_SUFFIX_MISMATCH')
    order_ref = _text(body.get('order_authorization_ref'),
                      'FORMAL_CHANNEL_ORDER_REF_INVALID')
    if order_ref != expected_order_ref:
        raise FormalSourceError('FORMAL_CHANNEL_ORDER_REF_MISMATCH')
    tools = body.get('tool_identity')
    if (not isinstance(tools, list) or any(not isinstance(item, str)
                                          or not item or len(item) > MAX_TEXT
                                          for item in tools)):
        raise FormalSourceError('FORMAL_CHANNEL_TOOLS_INVALID')
    tools = tuple(sorted(set(tools)))
    required = {'review_equity_order', 'place_equity_order'}
    if not required.issubset(set(tools)):
        raise FormalSourceError('FORMAL_CHANNEL_TOOLS_INCOMPLETE')
    if _boolean(body.get('local_bypass_allowed'),
                'FORMAL_CHANNEL_BYPASS_FLAG_INVALID'):
        raise FormalSourceError('FORMAL_CHANNEL_LOCAL_BYPASS_NOT_ALLOWED')
    app = _boolean(body.get('app_confirmation_supported'),
                   'FORMAL_CHANNEL_APP_FLAG_INVALID')
    explicit = _boolean(body.get('explicit_user_confirmation_supported'),
                        'FORMAL_CHANNEL_EXPLICIT_FLAG_INVALID')
    if not app and not explicit:
        raise FormalSourceError('FORMAL_CHANNEL_CONFIRMATION_UNPROVEN')
    _window(body, now=now, prefix='FORMAL_CHANNEL')
    return FormalChannelAttestation(
        source_id=_text(body.get('source_id'),
                        'FORMAL_CHANNEL_SOURCE_ID_INVALID'),
        source_sha256=body['source_sha256'],
        source_type=source_type,
        account_key=account_key,
        account_suffix=suffix,
        order_authorization_ref=order_ref,
        tool_identity=tools,
        review_contract_sha256=_hex64(
            body.get('review_contract_sha256'),
            'FORMAL_CHANNEL_REVIEW_CONTRACT_INVALID'),
        place_contract_sha256=_hex64(
            body.get('place_contract_sha256'),
            'FORMAL_CHANNEL_PLACE_CONTRACT_INVALID'),
        review_semantics=_text(body.get('review_semantics'),
                               'FORMAL_CHANNEL_REVIEW_SEMANTICS_INVALID'),
        place_semantics=_text(body.get('place_semantics'),
                              'FORMAL_CHANNEL_PLACE_SEMANTICS_INVALID'),
        app_confirmation_supported=app,
        explicit_user_confirmation_supported=explicit,
        local_bypass_allowed=False,
        captured_at=_text(body.get('captured_at'),
                          'FORMAL_CHANNEL_CAPTURED_AT_INVALID'),
        valid_until=_text(body.get('valid_until'),
                          'FORMAL_CHANNEL_VALID_UNTIL_INVALID'),
        synthetic=synthetic,
        provenance=_provenance(
            body.get('provenance'), 'FORMAL_CHANNEL_PROVENANCE_SCHEMA_INVALID'))


def parse_protection_attestation(raw, *, now, expected_account_key,
                                 expected_account_suffix, expected_order_ref,
                                 expected_plan, allow_synthetic=False):
    body = _record_body(raw)
    if body.get('schema_version') != SCHEMA_VERSION:
        raise FormalSourceError('FORMAL_PROTECTION_SCHEMA_INVALID')
    if body.get('kind') != 'protection_attestation':
        raise FormalSourceError('FORMAL_PROTECTION_KIND_INVALID')
    source_type, synthetic = _source_type(
        body, kind=SOURCE_PROTECTION, allow_synthetic=allow_synthetic)
    account_key = _hex64(body.get('account_key'),
                         'FORMAL_PROTECTION_ACCOUNT_INVALID')
    if account_key != expected_account_key:
        raise FormalSourceError('FORMAL_PROTECTION_ACCOUNT_MISMATCH')
    suffix = _text(body.get('account_suffix'),
                   'FORMAL_PROTECTION_SUFFIX_INVALID')
    if suffix != expected_account_suffix:
        raise FormalSourceError('FORMAL_PROTECTION_SUFFIX_MISMATCH')
    order_ref = _text(body.get('order_authorization_ref'),
                      'FORMAL_PROTECTION_ORDER_REF_INVALID')
    if order_ref != expected_order_ref:
        raise FormalSourceError('FORMAL_PROTECTION_ORDER_REF_MISMATCH')
    if not isinstance(expected_plan, dict):
        raise FormalSourceError('FORMAL_PROTECTION_PLAN_INVALID')
    try:
        symbol = expected_plan['symbol']
        quantity = str(expected_plan['quantity'])
        stop_price = str(expected_plan['stop'])
    except (KeyError, TypeError, ValueError):
        raise FormalSourceError('FORMAL_PROTECTION_PLAN_INVALID') from None
    if (body.get('symbol') != symbol
            or str(body.get('quantity')) != quantity
            or str(body.get('stop_price')) != stop_price):
        raise FormalSourceError('FORMAL_PROTECTION_PARAMETERS_MISMATCH')
    mode = _text(body.get('protection_mode'),
                 'FORMAL_PROTECTION_MODE_INVALID')
    if synthetic:
        if mode != 'synthetic_test_protection':
            raise FormalSourceError('FORMAL_PROTECTION_SYNTHETIC_MODE_INVALID')
    elif mode not in {'preconfirmed_oco', 'atomic_bracket',
                      'verified_failure_exit'}:
        raise FormalSourceError('FORMAL_PROTECTION_MODE_INVALID')
    tools = body.get('protection_tool_identity')
    if (not isinstance(tools, list) or not tools
            or any(not isinstance(item, str) or not item or len(item) > MAX_TEXT
                   for item in tools)):
        raise FormalSourceError('FORMAL_PROTECTION_TOOLS_INVALID')
    feasible = _boolean(body.get('feasible_before_entry'),
                        'FORMAL_PROTECTION_FEASIBLE_FLAG_INVALID')
    if not feasible:
        raise FormalSourceError('FORMAL_PROTECTION_NOT_FEASIBLE_BEFORE_ENTRY')
    raw_seconds = body.get('max_submit_seconds')
    if isinstance(raw_seconds, bool) or raw_seconds is None:
        raise FormalSourceError('FORMAL_PROTECTION_SUBMIT_SECONDS_INVALID')
    try:
        seconds = Decimal(str(raw_seconds))
    except (InvalidOperation, TypeError, ValueError):
        raise FormalSourceError('FORMAL_PROTECTION_SUBMIT_SECONDS_INVALID') from None
    if not seconds.is_finite() or seconds <= 0 or seconds > 30:
        raise FormalSourceError('FORMAL_PROTECTION_SUBMIT_SECONDS_EXCEEDED')
    failure_path = _text(body.get('failure_exit_path'),
                         'FORMAL_PROTECTION_FAILURE_EXIT_INVALID')
    if not _boolean(body.get('failure_exit_verified'),
                    'FORMAL_PROTECTION_FAILURE_FLAG_INVALID'):
        raise FormalSourceError('FORMAL_PROTECTION_FAILURE_EXIT_UNVERIFIED')
    _window(body, now=now, prefix='FORMAL_PROTECTION')
    return ProtectionAttestation(
        source_id=_text(body.get('source_id'),
                        'FORMAL_PROTECTION_SOURCE_ID_INVALID'),
        source_sha256=body['source_sha256'],
        source_type=source_type,
        account_key=account_key,
        account_suffix=suffix,
        order_authorization_ref=order_ref,
        symbol=symbol,
        quantity=quantity,
        stop_price=stop_price,
        protection_mode=mode,
        protection_tool_identity=tuple(sorted(set(tools))),
        protection_contract_sha256=_hex64(
            body.get('protection_contract_sha256'),
            'FORMAL_PROTECTION_CONTRACT_INVALID'),
        feasible_before_entry=feasible,
        max_submit_seconds=str(seconds),
        failure_exit_path=failure_path,
        failure_exit_verified=True,
        captured_at=_text(body.get('captured_at'),
                          'FORMAL_PROTECTION_CAPTURED_AT_INVALID'),
        valid_until=_text(body.get('valid_until'),
                          'FORMAL_PROTECTION_VALID_UNTIL_INVALID'),
        synthetic=synthetic,
        provenance=_provenance(
            body.get('provenance'),
            'FORMAL_PROTECTION_PROVENANCE_SCHEMA_INVALID'))


class FormalSourceStore:
    """Read the three private owner records from one fixed directory."""

    def __init__(self, root):
        self.root = Path(root)

    def read_bytes(self, name, *, max_bytes=MAX_SOURCE_BYTES):
        if (not isinstance(name, str) or not name or '/' in name
                or name in {'.', '..'}):
            raise FormalSourceError('FORMAL_SOURCE_PATH_INVALID')
        try:
            dir_fd = os.open(self.root,
                             os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        except OSError:
            raise FormalSourceError('FORMAL_SOURCE_ROOT_INVALID') from None
        try:
            try:
                fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                             dir_fd=dir_fd)
            except OSError:
                raise FormalSourceError('FORMAL_SOURCE_MISSING') from None
            try:
                before = os.fstat(fd)
                if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
                        or before.st_size <= 0
                        or before.st_size > max_bytes
                        or before.st_mode & 0o077):
                    raise FormalSourceError('FORMAL_SOURCE_FILE_INVALID')
                raw = os.read(fd, max_bytes + 1)
                after = os.fstat(fd)
                if (len(raw) != before.st_size
                        or before.st_size != after.st_size
                        or before.st_mtime_ns != after.st_mtime_ns):
                    raise FormalSourceError('FORMAL_SOURCE_CHANGED')
            except OSError:
                raise FormalSourceError('FORMAL_SOURCE_READ_FAILED') from None
            finally:
                os.close(fd)
        finally:
            os.close(dir_fd)
        return raw

    def _read(self, name):
        raw = self.read_bytes(name)
        try:
            value = json.loads(raw)
        except (UnicodeError, ValueError):
            raise FormalSourceError('FORMAL_SOURCE_JSON_INVALID') from None
        if not isinstance(value, dict):
            raise FormalSourceError('FORMAL_SOURCE_RECORD_INVALID')
        return value

    def load(self):
        records = {}
        for key, name in SOURCE_FILES.items():
            records[key] = self._read(name)
        return records


def _read_project_bytes(relative):
    if (not isinstance(relative, str) or not relative
            or Path(relative).is_absolute()
            or '..' in Path(relative).parts):
        raise FormalSourceError('PROVENANCE_RECORD_PATH_INVALID')
    path = PROJECT_ROOT / relative
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError:
        raise FormalSourceError('PROVENANCE_RECORD_MISSING') from None
    try:
        before = os.fstat(fd)
        if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
                or before.st_size <= 0
                or before.st_size > MAX_PROVENANCE_BYTES):
            raise FormalSourceError('PROVENANCE_RECORD_INVALID')
        raw = os.read(fd, MAX_PROVENANCE_BYTES + 1)
        after = os.fstat(fd)
        if (len(raw) != before.st_size
                or before.st_size != after.st_size
                or before.st_mtime_ns != after.st_mtime_ns):
            raise FormalSourceError('PROVENANCE_RECORD_CHANGED')
    except OSError:
        raise FormalSourceError('PROVENANCE_RECORD_READ_FAILED') from None
    finally:
        os.close(fd)
    return raw


def _json_record(raw, code):
    try:
        value = json.loads(raw)
    except (UnicodeError, ValueError):
        raise FormalSourceError(code) from None
    if not isinstance(value, dict):
        raise FormalSourceError(code)
    return value


def _tool_map(metadata):
    tools = metadata.get('tools') if isinstance(metadata, dict) else None
    if not isinstance(tools, list):
        raise FormalSourceError('PROVENANCE_TOOL_METADATA_INVALID')
    result = {}
    for item in tools:
        if not isinstance(item, dict):
            continue
        name = item.get('base_name') or item.get('name')
        description = item.get('description')
        if isinstance(name, str) and name.startswith('mcp__'):
            name = name.split('__')[-1]
        if (not isinstance(name, str) or not name
                or not isinstance(description, str) or not description):
            continue
        result[name] = description
    return result


def _contract_hash(name, description):
    return digest({'name': name, 'description': description})


def _metadata_record(raw, *, now, captured_at):
    metadata = _json_record(raw, 'PROVENANCE_TOOL_METADATA_INVALID')
    if metadata.get('formal_api_calls') != 0:
        raise FormalSourceError('PROVENANCE_TOOL_METADATA_NOT_READONLY')
    metadata_at = _time(metadata.get('captured_at'),
                        'PROVENANCE_TOOL_METADATA_TIME_INVALID')
    if metadata_at > now:
        raise FormalSourceError('PROVENANCE_TOOL_METADATA_FUTURE')
    if captured_at < metadata_at:
        raise FormalSourceError('PROVENANCE_CHANNEL_BEFORE_METADATA')
    return metadata, metadata_at, _tool_map(metadata)


def _equity_contract(tools, channel):
    descriptions = {}
    for name in CHANNEL_REQUIRED:
        description = tools.get(name)
        if description is None:
            raise FormalSourceError('PROVENANCE_CHANNEL_TOOL_MISSING:' + name)
        descriptions[name] = description
    review = descriptions['review_equity_order']
    place = descriptions['place_equity_order']
    if ('simulate' not in review.lower()
            or not any(word in review.lower()
                       for word in ('pre-trade', 'alerts', 'review'))):
        raise FormalSourceError('PROVENANCE_REVIEW_SEMANTICS_UNPROVEN')
    if ('place a real equity order' not in place.lower()
            or not any(word in place.lower()
                       for word in ('review_equity_order',
                                    'customer_approval_required'))):
        raise FormalSourceError('PROVENANCE_PLACE_SEMANTICS_UNPROVEN')
    review_hash = _contract_hash('review_equity_order', review)
    place_hash = _contract_hash('place_equity_order', place)
    if channel.review_contract_sha256 != review_hash:
        raise FormalSourceError('PROVENANCE_REVIEW_CONTRACT_MISMATCH')
    if channel.place_contract_sha256 != place_hash:
        raise FormalSourceError('PROVENANCE_PLACE_CONTRACT_MISMATCH')
    return {'review_sha256': review_hash, 'place_sha256': place_hash}


def _protection_contract(tools, protection):
    descriptions = {}
    for name in PROTECTION_REQUIRED:
        description = tools.get(name)
        if description is None:
            raise FormalSourceError('PROVENANCE_PROTECTION_TOOL_MISSING:' + name)
        descriptions[name] = description
    review = descriptions['review_advanced_order']
    place = descriptions['place_advanced_order']
    for text in (review, place):
        lower = text.lower()
        if 'oco' not in lower or 'stop' not in lower:
            raise FormalSourceError('PROVENANCE_PROTECTION_SEMANTICS_UNPROVEN')
    if 'real money' not in place.lower():
        raise FormalSourceError('PROVENANCE_PROTECTION_PLACE_SEMANTICS_UNPROVEN')
    review_hash = _contract_hash('review_advanced_order', review)
    place_hash = _contract_hash('place_advanced_order', place)
    combined = digest({'review': review_hash, 'place': place_hash})
    if protection.protection_contract_sha256 != combined:
        raise FormalSourceError('PROVENANCE_PROTECTION_CONTRACT_MISMATCH')
    return {'review_sha256': review_hash, 'place_sha256': place_hash,
            'contract_sha256': combined}


class FormalAuthority:
    """Interface; only concrete owner adapters are accepted by preflight."""

    def assess(self, **kwargs):
        raise NotImplementedError

    def consume(self, book, component, action_id, now):
        raise NotImplementedError

    def recheck_receipt(self, component, *, book, action=None, purpose=None,
                        evidence=None):
        raise NotImplementedError


class NoFormalEntryAuthority(FormalAuthority):
    """Default real-book consumer: no owner formal sources are configured."""

    def assess(self, **kwargs):
        return {
            'allowed': False,
            'status': 'blocked',
            'reasons': ['FORMAL_ENGINEERING_CHANNEL_UNPROVEN'],
            'sanitized': {'status': 'blocked',
                          'reason': 'FORMAL_ENGINEERING_CHANNEL_UNPROVEN'},
        }

    def consume(self, book, component, action_id, now):
        return None

    def recheck_receipt(self, component, *, book, action=None, purpose=None,
                        evidence=None):
        if isinstance(component, dict) and component.get('status') == 'allowed':
            return ['RECEIPT_FORMAL_CHANNEL_MISSING']
        return []


class FormalEntryAuthority(FormalAuthority):
    """Fail-closed owner validator and one-shot consumer."""

    def __init__(self, store, *, allow_synthetic=False, clock=None):
        if not isinstance(store, FormalSourceStore):
            raise ValueError('FORMAL_SOURCE_STORE_INVALID')
        self.store = store
        self.allow_synthetic = bool(allow_synthetic)
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def _blocked(self, reasons, *, sources=None, schema_valid=False,
                 provenance_proven=False):
        reasons = sorted(set(reasons))
        return {
            'allowed': False,
            'status': 'blocked',
            'schema_valid': schema_valid,
            'provenance_proven': provenance_proven,
            'reasons': reasons,
            'sanitized': {
                'status': 'blocked',
                'reason': reasons[0] if reasons else
                'FORMAL_ENGINEERING_CHANNEL_UNPROVEN',
                'reasons': reasons,
                'source_types': sources or {},
                'schema_valid': schema_valid,
                'provenance_proven': provenance_proven,
            },
        }

    def _expected_account(self, book, snapshot=None, current=None):
        key = None
        if isinstance(current, dict):
            account = current.get('account') or {}
            if isinstance(account, dict):
                key = account.get('account_key_hash')
        if not _HEX64.fullmatch(key or '') and isinstance(snapshot, dict):
            key = snapshot.get('account_key')
        if not _HEX64.fullmatch(key or ''):
            try:
                state = book.state()
                key = state.get('account_key') if isinstance(state, dict) else None
            except Exception:
                key = None
        suffix = book.policy.get('account_suffix')
        if not _HEX64.fullmatch(key or '') or not isinstance(suffix, str) or not suffix:
            raise FormalSourceError('FORMAL_BOOK_ACCOUNT_UNBOUND')
        return key, suffix

    def _expected_parameters(self, plan):
        return order_parameters_from_plan(plan)

    def _consumption(self, book, one_shot_id):
        try:
            exists = book.db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (CONSUMPTION_TABLE,)).fetchone()
        except sqlite3.Error:
            raise FormalSourceError('FORMAL_CONSUMPTION_LEDGER_UNAVAILABLE') from None
        if not exists:
            return None
        try:
            row = book.db.execute(
                'SELECT one_shot_id,source_sha256,binding,action_id,consumed_at '
                'FROM ' + CONSUMPTION_TABLE + ' WHERE one_shot_id=?',
                (one_shot_id,)).fetchone()
        except sqlite3.Error:
            raise FormalSourceError('FORMAL_CONSUMPTION_LEDGER_INVALID') from None
        if row is None:
            return None
        return {'one_shot_id': row[0], 'source_sha256': row[1],
                'binding': row[2], 'action_id': row[3],
                'consumed_at': row[4]}

    def _prove_user(self, user, book, now):
        provenance = user.provenance
        if not isinstance(provenance, dict):
            raise FormalSourceError('PROVENANCE_UNPROVEN:USER')
        if provenance.get('kind') != USER_PROVENANCE_KIND:
            raise FormalSourceError('PROVENANCE_USER_KIND_UNPROVEN')
        plan_id = _text(provenance.get('plan_id'),
                        'PROVENANCE_USER_PLAN_ID_INVALID')
        if (user.project_scope_ref != plan_id
                or user.order_authorization_ref != plan_id):
            raise FormalSourceError('PROVENANCE_USER_PLAN_BINDING_MISMATCH')
        pin = _hex64(provenance.get('record_sha256'),
                     'PROVENANCE_USER_RECORD_PIN_INVALID')
        try:
            row = book.db.execute(
                'SELECT payload FROM engineering_plans WHERE id=?',
                (plan_id,)).fetchone()
        except sqlite3.Error:
            raise FormalSourceError('PROVENANCE_USER_PLAN_UNAVAILABLE') from None
        if row is None:
            raise FormalSourceError('PROVENANCE_USER_PLAN_MISSING')
        try:
            payload = json.loads(row[0])
        except (TypeError, ValueError):
            raise FormalSourceError('PROVENANCE_USER_PLAN_INVALID') from None
        if not isinstance(payload, dict):
            raise FormalSourceError('PROVENANCE_USER_PLAN_INVALID')
        if digest(payload) != pin:
            raise FormalSourceError('PROVENANCE_USER_RECORD_CHANGED')
        if payload.get('id') != plan_id:
            raise FormalSourceError('PROVENANCE_USER_PLAN_ID_MISMATCH')
        if payload.get('purpose') != user.purpose:
            raise FormalSourceError('PROVENANCE_USER_PURPOSE_MISMATCH')
        if payload.get('synthetic') is not user.synthetic:
            raise FormalSourceError('PROVENANCE_USER_SYNTHETIC_MISMATCH')
        if payload.get('scope_authorized') is not True:
            raise FormalSourceError('PROVENANCE_USER_SCOPE_UNPROVEN')
        if payload.get('parameters_user_specified') is not True:
            raise FormalSourceError('PROVENANCE_USER_PARAMETERS_UNPROVEN')
        if order_parameters_from_plan(payload.get('plan')) != user.parameters:
            raise FormalSourceError('PROVENANCE_USER_PARAMETERS_MISMATCH')
        created = _time(payload.get('created_at'),
                        'PROVENANCE_USER_PLAN_TIME_INVALID')
        if created > now:
            raise FormalSourceError('PROVENANCE_USER_PLAN_FUTURE')
        if created > _time(user.authorized_at,
                           'FORMAL_USER_AUTHORIZED_AT_INVALID'):
            raise FormalSourceError('PROVENANCE_USER_PLAN_AFTER_AUTH')
        return {'kind': USER_PROVENANCE_KIND, 'plan_id': plan_id,
                'record_sha256': pin, 'created_at': payload.get('created_at')}

    def _prove_channel(self, channel, now):
        provenance = channel.provenance
        if not isinstance(provenance, dict):
            raise FormalSourceError('PROVENANCE_UNPROVEN:CHANNEL')
        if provenance.get('kind') != CHANNEL_PROVENANCE_KIND:
            raise FormalSourceError('PROVENANCE_CHANNEL_KIND_UNPROVEN')
        path = _text(provenance.get('record_path'),
                     'PROVENANCE_CHANNEL_PATH_INVALID')
        pin = _hex64(provenance.get('record_sha256'),
                     'PROVENANCE_CHANNEL_PIN_INVALID')
        raw = _read_project_bytes(path)
        if hashlib.sha256(raw).hexdigest() != pin:
            raise FormalSourceError('PROVENANCE_CHANNEL_RECORD_CHANGED')
        captured = _time(channel.captured_at,
                         'FORMAL_CHANNEL_CAPTURED_AT_INVALID')
        _, metadata_at, tools = _metadata_record(
            raw, now=now, captured_at=captured)
        contract = _equity_contract(tools, channel)
        return {'kind': CHANNEL_PROVENANCE_KIND, 'record_path': path,
                'record_sha256': pin,
                'captured_at': metadata_at.isoformat(),
                **contract}

    def _prove_protection(self, protection, now):
        provenance = protection.provenance
        if not isinstance(provenance, dict):
            raise FormalSourceError('PROVENANCE_UNPROVEN:PROTECTION')
        if provenance.get('kind') != PROTECTION_PROVENANCE_KIND:
            raise FormalSourceError('PROVENANCE_PROTECTION_KIND_UNPROVEN')
        path = _text(provenance.get('record_path'),
                     'PROVENANCE_PROTECTION_PATH_INVALID')
        pin = _hex64(provenance.get('record_sha256'),
                     'PROVENANCE_PROTECTION_PIN_INVALID')
        raw = self.store.read_bytes(
            path, max_bytes=MAX_PROTECTION_EVIDENCE_BYTES)
        if hashlib.sha256(raw).hexdigest() != pin:
            raise FormalSourceError('PROVENANCE_PROTECTION_RECORD_CHANGED')
        evidence = _json_record(
            raw, 'PROVENANCE_PROTECTION_RECORD_INVALID')
        if (evidence.get('schema_version') != SCHEMA_VERSION
                or evidence.get('kind') != PROTECTION_PROVENANCE_KIND):
            raise FormalSourceError('PROVENANCE_PROTECTION_RECORD_INVALID')
        producer = _text(evidence.get('producer'),
                         'PROVENANCE_PROTECTION_PRODUCER_INVALID')
        if producer not in PROTECTION_PRODUCERS:
            raise FormalSourceError('PROVENANCE_PROTECTION_PRODUCER_UNTRUSTED')
        recorded_at = _time(evidence.get('recorded_at'),
                            'PROVENANCE_PROTECTION_TIME_INVALID')
        captured_at = _time(protection.captured_at,
                            'FORMAL_PROTECTION_CAPTURED_AT_INVALID')
        if recorded_at > now:
            raise FormalSourceError('PROVENANCE_PROTECTION_FUTURE')
        if recorded_at > captured_at:
            raise FormalSourceError('PROVENANCE_PROTECTION_AFTER_CAPTURE')
        if evidence.get('account_key') != protection.account_key:
            raise FormalSourceError('PROVENANCE_PROTECTION_ACCOUNT_MISMATCH')
        if (evidence.get('order_authorization_ref')
                != protection.order_authorization_ref):
            raise FormalSourceError('PROVENANCE_PROTECTION_ORDER_REF_MISMATCH')
        if (evidence.get('symbol') != protection.symbol
                or str(evidence.get('quantity')) != protection.quantity
                or str(evidence.get('stop_price')) != protection.stop_price):
            raise FormalSourceError('PROVENANCE_PROTECTION_PARAMETERS_MISMATCH')
        if evidence.get('protection_mode') != protection.protection_mode:
            raise FormalSourceError('PROVENANCE_PROTECTION_MODE_MISMATCH')
        try:
            seconds = Decimal(str(evidence.get('max_submit_seconds')))
        except (InvalidOperation, TypeError, ValueError):
            raise FormalSourceError('PROVENANCE_PROTECTION_SECONDS_INVALID') from None
        if not seconds.is_finite() or seconds <= 0 or seconds > 30 \
                or str(seconds) != protection.max_submit_seconds:
            raise FormalSourceError('PROVENANCE_PROTECTION_SECONDS_MISMATCH')
        if evidence.get('failure_exit_path') != protection.failure_exit_path:
            raise FormalSourceError('PROVENANCE_PROTECTION_FAILURE_PATH_MISMATCH')
        if evidence.get('failure_exit_verified') is not True:
            raise FormalSourceError('PROVENANCE_PROTECTION_FAILURE_UNVERIFIED')
        if evidence.get('feasible_before_entry') is not True:
            raise FormalSourceError('PROVENANCE_PROTECTION_NOT_FEASIBLE')
        tool_identity = evidence.get('tool_identity')
        if (not isinstance(tool_identity, list)
                or not set(PROTECTION_REQUIRED).issubset(set(tool_identity))):
            raise FormalSourceError('PROVENANCE_PROTECTION_TOOLS_UNPROVEN')
        tool_contract = evidence.get('tool_contract')
        if not isinstance(tool_contract, dict):
            raise FormalSourceError('PROVENANCE_PROTECTION_CONTRACT_UNPROVEN')
        tool_path = _text(tool_contract.get('record_path'),
                          'PROVENANCE_PROTECTION_CONTRACT_PATH_INVALID')
        tool_pin = _hex64(tool_contract.get('record_sha256'),
                          'PROVENANCE_PROTECTION_CONTRACT_PIN_INVALID')
        tool_raw = _read_project_bytes(tool_path)
        if hashlib.sha256(tool_raw).hexdigest() != tool_pin:
            raise FormalSourceError('PROVENANCE_PROTECTION_CONTRACT_CHANGED')
        _, metadata_at, tools = _metadata_record(
            tool_raw, now=now, captured_at=recorded_at)
        contract = _protection_contract(tools, protection)
        if evidence.get('tool_contract_sha256') != contract['contract_sha256']:
            raise FormalSourceError('PROVENANCE_PROTECTION_CONTRACT_MISMATCH')
        return {'kind': PROTECTION_PROVENANCE_KIND, 'record_path': path,
                'record_sha256': pin,
                'recorded_at': recorded_at.isoformat(),
                'tool_contract_sha256': contract['contract_sha256'],
                'metadata_captured_at': metadata_at.isoformat()}

    def _prove_sources(self, user, channel, protection, book, now, plan=None):
        if user.synthetic:
            return False, {}
        self._prove_user(user, book, now)
        self._prove_channel(channel, now)
        # Keep structural/reference checks, but do not promote a local
        # self-declared protection file to proof of this order's pre-entry
        # protection.  The reader consumes actual owner/native/broker records;
        # current official tools do not prove a pre-entry protection path.
        self._prove_protection(protection, now)
        from .protection_evidence import ProtectionEvidenceReader
        capability = ProtectionEvidenceReader(
            book, clock=self.clock).assess_capability(
                plan=plan, channel=channel, protection=protection)
        if capability.get('capability_proven') is not True:
            raise FormalSourceError(
                capability.get('qualification') or
                'CHANNEL_PROTECTION_PATH_UNPROVEN')
        raise FormalSourceError('PROTECTION_PROVENANCE_UNPROVEN')


    def _binding(self, user, channel, protection, *, evidence_sha256=None,
                 provenance=None):
        return digest({
            'schema_version': SCHEMA_VERSION,
            'kind': FORMAL_BINDING_KIND,
            'account_key': user.account_key,
            'account_suffix': user.account_suffix,
            'purpose': user.purpose,
            'parameters': user.parameters,
            'parameters_sha256': user.parameters_sha256,
            'project_scope_ref': user.project_scope_ref,
            'order_authorization_ref': user.order_authorization_ref,
            'one_shot_id': user.one_shot_id,
            'evidence_sha256': evidence_sha256,
            'user_source_sha256': user.source_sha256,
            'channel_source_sha256': channel.source_sha256,
            'protection_source_sha256': protection.source_sha256,
            'channel_review_contract_sha256': channel.review_contract_sha256,
            'channel_place_contract_sha256': channel.place_contract_sha256,
            'protection_contract_sha256':
                protection.protection_contract_sha256,
            'provenance': provenance or {},
        })

    def _evaluate(self, *, book, mode, action, plan, purpose, current=None,
                  snapshot=None, evidence=None, candidate_action_id=None):
        if self.allow_synthetic and book.simulation is not True:
            return self._blocked(['FORMAL_SYNTHETIC_AUTHORITY_ON_REAL_BOOK'])
        try:
            account_key, account_suffix = self._expected_account(
                book, snapshot=snapshot, current=current)
            parameters = self._expected_parameters(plan)
        except FormalSourceError as exc:
            return self._blocked([exc.code])
        now = self.clock()
        records = None
        try:
            records = self.store.load()
            user = parse_user_order_authorization(
                records['user'], now=now, expected_account_key=account_key,
                expected_account_suffix=account_suffix,
                expected_purpose=purpose, expected_parameters=parameters,
                allow_synthetic=self.allow_synthetic)
            channel = parse_formal_channel_attestation(
                records['channel'], now=now,
                expected_account_key=account_key,
                expected_account_suffix=account_suffix,
                expected_order_ref=user.order_authorization_ref,
                allow_synthetic=self.allow_synthetic)
            protection = parse_protection_attestation(
                records['protection'], now=now,
                expected_account_key=account_key,
                expected_account_suffix=account_suffix,
                expected_order_ref=user.order_authorization_ref,
                expected_plan=plan, allow_synthetic=self.allow_synthetic)
        except FormalSourceError as exc:
            sources = {}
            if isinstance(records, dict):
                for key in ('user', 'channel', 'protection'):
                    item = records.get(key)
                    if isinstance(item, dict):
                        sources[key] = item.get('source_type')
            return self._blocked([exc.code], sources=sources,
                                 schema_valid=False)
        except (KeyError, TypeError, ValueError, OSError, sqlite3.Error):
            return self._blocked(['FORMAL_SOURCE_EVALUATION_FAILED'],
                                 schema_valid=False)
        if (user.synthetic != channel.synthetic
                or user.synthetic != protection.synthetic):
            return self._blocked(['FORMAL_SOURCE_SYNTHETIC_MIXED'],
                                 schema_valid=True)
        if user.source_type == SOURCE_SYNTHETIC:
            if book.simulation is not True:
                return self._blocked(['FORMAL_SYNTHETIC_ON_REAL_BOOK'],
                                     schema_valid=True)
        elif book.simulation is True:
            return self._blocked(['FORMAL_OWNER_SOURCE_ON_SYNTHETIC_BOOK'],
                                 schema_valid=True)
        try:
            provenance_proven, provenance = self._prove_sources(
                user, channel, protection, book, now, plan=plan)
        except FormalSourceError as exc:
            return self._blocked([exc.code],
                                 sources={
                                     'user': user.source_type,
                                     'channel': channel.source_type,
                                     'protection': protection.source_type,
                                 },
                                 schema_valid=True,
                                 provenance_proven=False)
        except (KeyError, TypeError, ValueError, OSError, sqlite3.Error):
            return self._blocked(['PROVENANCE_EVALUATION_FAILED'],
                                 schema_valid=True,
                                 provenance_proven=False)
        evidence_sha256 = digest(evidence) if evidence is not None else None
        binding = self._binding(
            user, channel, protection, evidence_sha256=evidence_sha256,
            provenance=provenance)
        consumed = self._consumption(book, user.one_shot_id)
        duplicate_existing = False
        if mode == 'proposal':
            if consumed is not None:
                if (isinstance(candidate_action_id, str)
                        and candidate_action_id
                        and consumed.get('binding') == binding
                        and consumed.get('action_id') == candidate_action_id):
                    duplicate_existing = True
                else:
                    return self._blocked(['FORMAL_AUTHORIZATION_REPLAYED'],
                                         schema_valid=True,
                                         provenance_proven=provenance_proven)
        elif mode == 'claim':
            if consumed is None:
                return self._blocked(['FORMAL_AUTHORIZATION_NOT_CONSUMED'],
                                     schema_valid=True,
                                     provenance_proven=provenance_proven)
            if (consumed.get('binding') != binding
                    or not isinstance(action, dict)
                    or consumed.get('action_id') != action.get('id')):
                return self._blocked(['FORMAL_AUTHORIZATION_ACTION_MISMATCH'],
                                     schema_valid=True,
                                     provenance_proven=provenance_proven)
        else:
            return self._blocked(['FORMAL_AUTHORIZATION_MODE_INVALID'],
                                 schema_valid=True,
                                 provenance_proven=provenance_proven)
        component = {
            'status': 'allowed',
            'reason': None,
            'reasons': [],
            'schema_valid': True,
            'provenance_proven': provenance_proven,
            'synthetic_test': user.synthetic,
            'duplicate_existing': duplicate_existing,
            'source_types': {
                'user': user.source_type,
                'channel': channel.source_type,
                'protection': protection.source_type,
            },
            'source_sha256': {
                'user': user.source_sha256,
                'channel': channel.source_sha256,
                'protection': protection.source_sha256,
            },
            'provenance': provenance,
            'account_key': user.account_key,
            'account_suffix': user.account_suffix,
            'purpose': user.purpose,
            'parameters': dict(user.parameters),
            'parameters_sha256': user.parameters_sha256,
            'evidence_sha256': evidence_sha256,
            'project_scope_ref': user.project_scope_ref,
            'order_authorization_ref': user.order_authorization_ref,
            'one_shot_id': user.one_shot_id,
            'binding': binding,
            'user': user.sanitized(),
            'channel': channel.sanitized(),
            'protection': protection.sanitized(),
            'synthetic': user.synthetic,
        }
        return {'allowed': True, 'status': 'allowed', 'reasons': [],
                'schema_valid': True,
                'provenance_proven': provenance_proven,
                'sanitized': component, 'sources': FormalSources(
                    user=user, channel=channel, protection=protection),
                'binding': binding}

    def assess(self, *, book, mode, action=None, snapshot=None, quote=None,
               plan=None, purpose=None, evidence=None, current=None,
               candidate_action_id=None):
        if purpose not in PURPOSES:
            return self._blocked(['FORMAL_PURPOSE_REQUIRED'],
                                 schema_valid=False)
        return self._evaluate(book=book, mode=mode, action=action, plan=plan or {},
                              purpose=purpose, current=current,
                              snapshot=snapshot, evidence=evidence,
                              candidate_action_id=candidate_action_id)

    def consume(self, book, component, action_id, now):
        if not isinstance(component, dict) or component.get('status') != 'allowed':
            return None
        if not isinstance(action_id, str) or not action_id:
            raise ValueError('FORMAL_CONSUMPTION_ACTION_INVALID')
        one_shot = component.get('one_shot_id')
        source_sha = (component.get('source_sha256') or {}).get('user')
        binding = component.get('binding')
        if (not isinstance(one_shot, str) or not one_shot
                or not isinstance(binding, str) or not _HEX64.fullmatch(binding)
                or not isinstance(source_sha, str)
                or not _HEX64.fullmatch(source_sha)):
            raise ValueError('FORMAL_AUTHORIZATION_COMPONENT_INVALID')
        book.db.execute(
            'CREATE TABLE IF NOT EXISTS ' + CONSUMPTION_TABLE +
            '(one_shot_id TEXT PRIMARY KEY,source_sha256 TEXT NOT NULL,'
            'binding TEXT NOT NULL,action_id TEXT NOT NULL,'
            'consumed_at TEXT NOT NULL)')
        existing = self._consumption(book, one_shot)
        if existing is not None:
            if (existing.get('binding') == binding
                    and existing.get('action_id') == action_id
                    and existing.get('source_sha256') == source_sha):
                return None
            raise ValueError('FORMAL_AUTHORIZATION_REPLAYED')
        try:
            book.db.execute(
                'INSERT INTO ' + CONSUMPTION_TABLE + ' VALUES(?,?,?,?,?)',
                (one_shot, source_sha, binding, action_id,
                 now.isoformat()))
        except sqlite3.IntegrityError:
            raise ValueError('FORMAL_AUTHORIZATION_REPLAYED') from None
        return None

    def recheck_receipt(self, component, *, book, action=None, purpose=None,
                        evidence=None):
        if not isinstance(component, dict) or component.get('status') != 'allowed':
            return []
        if purpose not in PURPOSES:
            return ['RECEIPT_FORMAL_PURPOSE_INVALID']
        try:
            one_shot = component['one_shot_id']
            consumed = self._consumption(book, one_shot)
            if action is not None:
                if not isinstance(action, dict) or not isinstance(
                        action.get('id'), str):
                    return ['RECEIPT_FORMAL_AUTHORIZATION_ACTION_MISSING']
                if consumed is None:
                    return ['RECEIPT_FORMAL_AUTHORIZATION_NOT_CONSUMED']
                if (consumed.get('binding') != component.get('binding')
                        or consumed.get('action_id') != action.get('id')):
                    return ['RECEIPT_FORMAL_AUTHORIZATION_ACTION_MISMATCH']
            records = self.store.load()
            account_key = component.get('account_key')
            account_suffix = component.get('account_suffix')
            order_ref = component.get('order_authorization_ref')
            parameters = component.get('parameters')
            plan = {
                'symbol': parameters['symbol'],
                'quantity': parameters['quantity'],
                'entry_ceiling': parameters['limit_price'],
                'stop': component['protection']['stop_price'],
            }
            now = self.clock()
            user = parse_user_order_authorization(
                records['user'], now=now, expected_account_key=account_key,
                expected_account_suffix=account_suffix,
                expected_purpose=purpose, expected_parameters=parameters,
                allow_synthetic=self.allow_synthetic)
            channel = parse_formal_channel_attestation(
                records['channel'], now=now,
                expected_account_key=account_key,
                expected_account_suffix=account_suffix,
                expected_order_ref=order_ref,
                allow_synthetic=self.allow_synthetic)
            protection = parse_protection_attestation(
                records['protection'], now=now,
                expected_account_key=account_key,
                expected_account_suffix=account_suffix,
                expected_order_ref=order_ref,
                expected_plan=plan, allow_synthetic=self.allow_synthetic)
            _, provenance = self._prove_sources(
                user, channel, protection, book, now)
        except FormalSourceError as exc:
            return ['RECEIPT_FORMAL_SOURCE_CHANGED:' + exc.code]
        except (KeyError, TypeError, ValueError, OSError, sqlite3.Error):
            return ['RECEIPT_FORMAL_SOURCE_CHANGED']
        expected = {
            'user': user.source_sha256,
            'channel': channel.source_sha256,
            'protection': protection.source_sha256,
        }
        if expected != (component.get('source_sha256') or {}):
            return ['RECEIPT_FORMAL_SOURCE_CHANGED']
        if provenance != (component.get('provenance') or {}):
            return ['RECEIPT_FORMAL_PROVENANCE_CHANGED']
        evidence_sha256 = digest(evidence) if evidence is not None else None
        if evidence_sha256 != component.get('evidence_sha256'):
            return ['RECEIPT_FORMAL_EVIDENCE_CHANGED']
        if self._binding(
                user, channel, protection, evidence_sha256=evidence_sha256,
                provenance=provenance) != component.get('binding'):
            return ['RECEIPT_FORMAL_BINDING_CHANGED']
        return []


def component_of(decision):
    """Sanitized component only; never expose raw source records."""
    if not isinstance(decision, dict):
        return {'status': 'blocked',
                'reason': 'FORMAL_AUTHORITY_DECISION_INVALID',
                'reasons': ['FORMAL_AUTHORITY_DECISION_INVALID']}
    sanitized = decision.get('sanitized')
    if isinstance(sanitized, dict):
        return dict(sanitized)
    reasons = sorted(set(decision.get('reasons') or
                         ['FORMAL_ENGINEERING_CHANNEL_UNPROVEN']))
    return {'status': 'blocked',
            'reason': reasons[0] if reasons else
            'FORMAL_ENGINEERING_CHANNEL_UNPROVEN',
            'reasons': reasons}
