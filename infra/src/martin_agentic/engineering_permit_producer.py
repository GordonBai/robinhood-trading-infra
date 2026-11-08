"""Owner-bound supervised-engineering permit producer.

This module is deliberately a connector, not a new authority.  It reads the
existing owner request, the already-recorded native review, current owner facts
and the configured owner source verifier, then calls the existing
``build_permit`` contract.  Persisting the result uses the same private owner
directory/format consumed by ``SupervisedEngineeringAuthority``.  It never
promotes, consumes, claims, places or enables a live/entry switch.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path

from trading_contracts.values import digest

from .audit import digest as _audit_digest
from .host_mailbox import read_private_json, write_private_json
from .qualification import source_fingerprint
from .supervised_engineering import (
    PERMIT_FILE, REQUEST_FILE, SupervisedEngineeringError,
    _review_record_from_raw, build_permit, parse_request,
    validate_permit_against)


class EngineeringPermitError(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


# A permit is only issued from an owner action still awaiting its review result
# or the later user confirmation.  READY is accepted only for an already
# persisted permit so an idempotent re-read cannot report a promoted action as
# a new issuance.
_ISSUE_ACTION_STATES = frozenset({'REVIEW_PENDING', 'WAITING_USER'})
_EXISTING_ACTION_STATES = _ISSUE_ACTION_STATES | {'READY'}


def _fail(code):
    raise EngineeringPermitError(code)


def _clock(book):
    value = book.clock()
    if not isinstance(value, datetime) or value.tzinfo is None:
        _fail('SUPERVISED_ENGINEERING_PERMIT_CLOCK_INVALID')
    return value.astimezone(timezone.utc)


def _owner_root(book):
    profile = getattr(book, 'supervised_engineering_profile', None) or {}
    value = profile.get('owner_root')
    if isinstance(value, str) and value:
        return Path(value)
    authority = getattr(book, 'supervised_engineering_authority', None)
    root = getattr(getattr(authority, 'store', None), 'root', None)
    if root is None:
        _fail('SUPERVISED_ENGINEERING_PERMIT_OWNER_ROOT_MISSING')
    return Path(root)


def _review_from_record(row, action_id, now):
    operation_id, record_action, state, raw, result_raw = row
    if record_action != action_id or state != 'RECORDED':
        _fail('SUPERVISED_ENGINEERING_PERMIT_REVIEW_NOT_RECORDED')
    try:
        packet = json.loads(raw)
        result = json.loads(result_raw)
    except (TypeError, ValueError):
        _fail('SUPERVISED_ENGINEERING_PERMIT_REVIEW_INVALID')
    if not isinstance(packet, dict) or not isinstance(result, dict):
        _fail('SUPERVISED_ENGINEERING_PERMIT_REVIEW_INVALID')
    if (packet.get('operation_id') != operation_id
            or packet.get('action_id') != action_id
            or packet.get('operation') != 'review'
            or packet.get('tool') != 'review_equity_order'):
        _fail('SUPERVISED_ENGINEERING_PERMIT_REVIEW_BINDING')
    if not isinstance(packet.get('parameters'), dict):
        _fail('SUPERVISED_ENGINEERING_PERMIT_REVIEW_PARAMETERS')
    return {'operation_id': operation_id, 'action_id': action_id,
            'parameters': packet['parameters'], 'result': result}


def _current_review(book, action_id):
    row = None
    try:
        row = book.db.execute(
            "SELECT id,action_id,state,payload,result "
            "FROM native_operation_records WHERE action_id=? "
            "AND operation='review' ORDER BY rowid DESC LIMIT 1",
            (action_id,)).fetchone()
    except Exception as exc:
        _fail('SUPERVISED_ENGINEERING_PERMIT_REVIEW_STORE_UNAVAILABLE:'
              + (getattr(exc, 'code', None) or type(exc).__name__))
    if row is None:
        _fail('SUPERVISED_ENGINEERING_PERMIT_REVIEW_MISSING')
    operation_id, record_action, state, raw, result_raw = row
    return _review_from_record(
        (operation_id, record_action, state, raw, result_raw),
        action_id, book.clock())


def _incoming_request(owner_root):
    try:
        request = read_private_json(owner_root / REQUEST_FILE)
    except FileNotFoundError:
        _fail('SUPERVISED_ENGINEERING_PERMIT_REQUEST_MISSING')
    except (OSError, ValueError, json.JSONDecodeError):
        _fail('SUPERVISED_ENGINEERING_PERMIT_REQUEST_UNAVAILABLE')
    if not isinstance(request, dict):
        _fail('SUPERVISED_ENGINEERING_PERMIT_REQUEST_INVALID')
    return request


def _facts(book):
    reader = getattr(book, 'current_account_facts', None)
    if not callable(reader):
        _fail('SUPERVISED_ENGINEERING_PERMIT_FACTS_READER_MISSING')
    facts = reader(purpose='engineering')
    if not isinstance(facts, dict) or not facts.get('assessment_id'):
        _fail('SUPERVISED_ENGINEERING_PERMIT_FACTS_MISSING')
    return facts


def _action(book, action_id, *, allowed_states):
    action = next((item for item in book.actions()
                   if item.get('id') == action_id), None)
    if action is None or action.get('kind') != 'entry':
        _fail('SUPERVISED_ENGINEERING_PERMIT_ACTION_MISSING')
    if action.get('state') not in allowed_states:
        _fail('SUPERVISED_ENGINEERING_PERMIT_ACTION_STATE')
    payload = None
    try:
        payload = json.loads(action.get('payload'))
    except (TypeError, ValueError):
        _fail('SUPERVISED_ENGINEERING_PERMIT_ACTION_INVALID')
    if (not isinstance(payload, dict)
            or payload.get('purpose') != 'engineering'
            or not isinstance(payload.get('ref_id'), str)
            or not isinstance(payload.get('plan'), dict)):
        _fail('SUPERVISED_ENGINEERING_PERMIT_ACTION_INVALID')
    return action, payload


def _verified(book, authority, permit, parsed_request, review, action,
              facts, *, allow_ready=False):
    # The synthetic test composition has no formal source verifier by design;
    # it is explicitly enabled only on a simulation book with allow_synthetic.
    # This is never a formal capability or acceptance for a real book.
    if book.simulation is True and getattr(authority, 'allow_synthetic', False):
        if permit.get('synthetic') is not True:
            _fail('SUPERVISED_ENGINEERING_PERMIT_SYNTHETIC_MIXED')
        return {'verified': True, 'permit_id': permit.get('permit_id'),
                'synthetic_test_only': True}
    verifier = getattr(authority, 'real_source_verifier', None)
    if verifier is None or not callable(getattr(verifier, 'verify', None)):
        _fail('SUPERVISED_ENGINEERING_PERMIT_SOURCE_VERIFIER_UNAVAILABLE')
    result = {'permit': {'permit': permit}, 'request': parsed_request,
              'review': review, 'action': action,
              'facts': {'assessment': facts}}
    try:
        evidence = verifier.verify(book, result, allow_ready=allow_ready)
    except Exception as exc:
        _fail('SUPERVISED_ENGINEERING_PERMIT_SOURCE_VERIFY_FAILED:'
              + (getattr(exc, 'code', None) or type(exc).__name__))
    if not isinstance(evidence, dict) or evidence.get('verified') is not True:
        reason = ((evidence or {}).get('reasons') or
                  [(evidence or {}).get('reason') or 'UNKNOWN'])[0]
        _fail('SUPERVISED_ENGINEERING_PERMIT_SOURCE_UNVERIFIED:' + str(reason))
    if evidence.get('permit_id') != permit.get('permit_id'):
        _fail('SUPERVISED_ENGINEERING_PERMIT_SOURCE_PERMIT_MISMATCH')
    return evidence


def _existing_permit(path):
    try:
        return read_private_json(path)
    except FileNotFoundError:
        return None
    except (OSError, ValueError, json.JSONDecodeError):
        _fail('SUPERVISED_ENGINEERING_PERMIT_EXISTING_UNAVAILABLE')


def _snapshot(book, action_id, root, *, existing):
    """Read one current owner/ledger/source snapshot for a full check."""
    allowed_states = (_EXISTING_ACTION_STATES if existing
                      else _ISSUE_ACTION_STATES)
    now = _clock(book)
    request_raw = _incoming_request(root)
    action, payload = _action(book, action_id,
                              allowed_states=allowed_states)
    facts = deepcopy(_facts(book))
    review = _current_review(book, action_id)
    account_key = book.state().get('account_key')
    if not isinstance(account_key, str) or not account_key:
        _fail('SUPERVISED_ENGINEERING_PERMIT_ACCOUNT_UNBOUND')
    suffix = str(book.policy.get('account_suffix'))
    policy = deepcopy(book.policy)
    fingerprint = source_fingerprint()
    parsed_request = parse_request(
        request_raw, account_key=account_key, account_suffix=suffix,
        policy=policy, now=now, fingerprint=fingerprint)
    ref_id = payload.get('ref_id')
    if not isinstance(ref_id, str) or not ref_id:
        _fail('SUPERVISED_ENGINEERING_PERMIT_REF_INVALID')
    return {'now': now, 'request_raw': request_raw, 'action': action,
            'payload': payload, 'facts': facts, 'review': review,
            'account_key': account_key, 'suffix': suffix,
            'policy': policy, 'fingerprint': fingerprint,
            'parsed_request': parsed_request, 'ref_id': ref_id}


def _check_current_binding(before, after, prefix):
    """Compare the start snapshot with the post-source current snapshot."""
    if after['now'] < before['now']:
        _fail(prefix + ':CLOCK_REGRESSED')
    if digest(after['request_raw']) != digest(before['request_raw']):
        _fail(prefix + ':REQUEST_CHANGED')
    if (digest(after['action'].get('payload')) !=
            digest(before['action'].get('payload'))
            or after['action'].get('trade_id') !=
            before['action'].get('trade_id')
            or after['action'].get('kind') != before['action'].get('kind')):
        _fail(prefix + ':ACTION_CHANGED')
    if digest(after['facts']) != digest(before['facts']):
        _fail(prefix + ':FACTS_CHANGED')
    if digest(after['review']) != digest(before['review']):
        _fail(prefix + ':REVIEW_CHANGED')
    if after['fingerprint'] != before['fingerprint']:
        _fail(prefix + ':SOURCE_CHANGED')
    if digest(after['policy']) != digest(before['policy']):
        _fail(prefix + ':POLICY_CHANGED')


def _validate_full(book, snapshot, permit, *, allow_consumed, prefix):
    """Run the existing complete consumer contract before returning/publishing.

    ``build_permit`` and the source verifier are necessary but not sufficient:
    they do not compare the review order or a post-read owner change with the
    prospective permit.  This is the same validator used by the consumer; the
    consumer still repeats it independently at promotion/consumption time.
    """
    try:
        validate_permit_against(
            book, snapshot['action'], purpose='engineering',
            now=snapshot['now'], permit_raw=permit,
            request_raw=snapshot['request_raw'],
            account_key=snapshot['account_key'], policy=snapshot['policy'],
            fingerprint=snapshot['fingerprint'],
            allow_consumed=allow_consumed)
    except (EngineeringPermitError, SupervisedEngineeringError) as exc:
        _fail(prefix + ':' + str(getattr(exc, 'code', None) or type(exc).__name__))
    except (ValueError, KeyError, TypeError, OSError) as exc:
        _fail(prefix + ':' + type(exc).__name__)


def _result(status, permit, path):
    return {'status': status, 'permit': permit,
            'permit_id': permit.get('permit_id'), 'path': str(path)}


def produce_supervised_engineering_permit(book, action_id, *,
                                          owner_root=None):
    """Create/persist one permit from exact existing owner/native facts.

    The same request/action/native review/facts returns the existing permit.
    A different existing file is a conflict and is never overwritten.  Every
    publication is checked with the full existing permit contract against a
    fresh owner/ledger/source/clock snapshot after the source verifier.  The
    function does not evaluate activation, promote, consume or place.
    """
    authority = getattr(book, 'supervised_engineering_authority', None)
    if authority is None:
        _fail('SUPERVISED_ENGINEERING_AUTHORITY_UNAVAILABLE')
    root = Path(owner_root) if owner_root is not None else _owner_root(book)
    if root.is_symlink() or not root.is_dir():
        _fail('SUPERVISED_ENGINEERING_PERMIT_OWNER_ROOT_INVALID')
    path = root / PERMIT_FILE
    existing = _existing_permit(path)
    snapshot = _snapshot(book, action_id, root,
                         existing=existing is not None)
    if existing is not None:
        # Reuse the consumer's exact complete contract before reporting the
        # existing permit, then repeat it against a post-source current read.
        _validate_full(book, snapshot, existing, allow_consumed=True,
                       prefix='SUPERVISED_ENGINEERING_PERMIT_EXISTING_CONFLICT')
        _verified(book, authority, existing, snapshot['parsed_request'],
                  snapshot['review'], snapshot['action'], snapshot['facts'],
                  allow_ready=True)
        try:
            current = _snapshot(book, action_id, root, existing=True)
        except (EngineeringPermitError, SupervisedEngineeringError) as exc:
            _fail('SUPERVISED_ENGINEERING_PERMIT_EXISTING_CONFLICT:'
                  'POST_READ:' + str(getattr(exc, 'code', None)
                                     or type(exc).__name__))
        _check_current_binding(
            snapshot, current,
            'SUPERVISED_ENGINEERING_PERMIT_EXISTING_CONFLICT')
        current_existing = _existing_permit(path)
        if current_existing is None or current_existing != existing:
            _fail('SUPERVISED_ENGINEERING_PERMIT_EXISTING_CONFLICT:'
                  'CURRENT_CHANGED')
        _validate_full(book, current, current_existing, allow_consumed=True,
                       prefix='SUPERVISED_ENGINEERING_PERMIT_EXISTING_CONFLICT')
        return _result('EXISTING', current_existing, path)
    one_shot_id = _audit_digest([
        'supervised-engineering-permit-one-shot-v1', snapshot['request_raw'],
        action_id, snapshot['review']['operation_id'],
        snapshot['facts']['assessment_id'], snapshot['fingerprint'],
        digest(snapshot['policy'])])
    permit = build_permit(
        snapshot['request_raw'], snapshot['review'], snapshot['facts'],
        account_key=snapshot['account_key'], account_suffix=snapshot['suffix'],
        policy=snapshot['policy'], fingerprint=snapshot['fingerprint'],
        action_id=action_id, ref_id=snapshot['ref_id'], now=snapshot['now'],
        one_shot_id=one_shot_id,
        valid_until=snapshot['request_raw'].get('valid_until'))
    # A structurally valid review/order is not enough: run the same complete
    # owner/action/ref/order/review/facts/source/policy/window check first.
    _validate_full(book, snapshot, permit, allow_consumed=False,
                   prefix='SUPERVISED_ENGINEERING_PERMIT_BINDING')
    _verified(book, authority, permit, snapshot['parsed_request'],
              snapshot['review'], snapshot['action'], snapshot['facts'])
    try:
        current = _snapshot(book, action_id, root, existing=False)
    except (EngineeringPermitError, SupervisedEngineeringError) as exc:
        _fail('SUPERVISED_ENGINEERING_PERMIT_POST_READ:'
              + str(getattr(exc, 'code', None) or type(exc).__name__))
    _check_current_binding(
        snapshot, current, 'SUPERVISED_ENGINEERING_PERMIT_POST_READ')
    _validate_full(book, current, permit, allow_consumed=False,
                   prefix='SUPERVISED_ENGINEERING_PERMIT_POST_READ')
    raced = _existing_permit(path)
    if raced is not None:
        if raced != permit:
            _fail('SUPERVISED_ENGINEERING_PERMIT_WRITE_CONFLICT')
        return _result('EXISTING', raced, path)
    try:
        write_private_json(path, permit)
    except ValueError as exc:
        if str(exc) != 'HOST_FILE_CONFLICT':
            _fail('SUPERVISED_ENGINEERING_PERMIT_WRITE_FAILED')
        try:
            current_permit = read_private_json(path)
        except (OSError, ValueError, json.JSONDecodeError):
            _fail('SUPERVISED_ENGINEERING_PERMIT_WRITE_CONFLICT')
        if current_permit != permit:
            _fail('SUPERVISED_ENGINEERING_PERMIT_WRITE_CONFLICT')
        return _result('EXISTING', current_permit, path)
    return _result('ISSUED', permit, path)
