"""Versioned owner-bound current-engineering assessment receipts.

A receipt is produced only by the owner-bound current preflight from owner
readers: the saved capture bundle, the pinned public manifest, the execution
book's broker journal/health rows, the live action row, the book clock, the
canonical risk sidecar and the existing lineage/control gates.  It is a
binding/consistency record, not an entry authorization.

The short write-transaction recheck deliberately uses only a bounded stat
pin/source version and small manifest reads.  It never re-hashes the whole
project under the write lock.  Local hashes/stats prove local consistency,
not broker signatures and not resistance to an attacker who can rewrite all
files while preserving metadata.
"""
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo
import hashlib
import json
import os
from pathlib import Path
import time

from trading_contracts.values import canonical, digest, timestamp

from .current_evidence import (
    CurrentEvidenceError, PUBLIC_SOURCE_MAX_AGE, _read_public,
    TERMINAL_ORDER_STATES)
from .qualification import source_pin


SCHEMA_VERSION = 1
KIND = 'current_engineering_assessment_receipt_v1'
CAPTURE_KIND = 'capture_generation_v1'
MAX_OBSERVATION_AGE_SECONDS = 30
MAX_QUOTE_AGE_SECONDS = 15
CAPTURE_RECHECK_MAX_FILES = 10000
CAPTURE_RECHECK_MAX_SECONDS = 0.5
AUTHORITY_NAMES = ('risk', 'lineage', 'control')
TERMINAL = frozenset(TERMINAL_ORDER_STATES)
JOURNAL_ONLY = {'reconciliation_issues', 'new_executions_seen',
                'baseline_established'}
NY = ZoneInfo('America/New_York')


class ReceiptError(ValueError):
    def __init__(self, code, detail=None):
        self.code = code
        self.detail = detail
        super().__init__(code)

    def __str__(self):
        return self.code if self.detail is None else f'{self.code}:{self.detail}'


def _fail(code, detail=None):
    raise ReceiptError(code, detail)


def _sha256_text(value):
    return hashlib.sha256(value.encode()).hexdigest()


def _json_object(value):
    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        return None
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _decimal(value):
    if isinstance(value, bool) or value is None:
        raise ValueError('DECIMAL_INVALID')
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        raise ValueError('DECIMAL_INVALID') from None


def _time(value):
    if not isinstance(value, str):
        raise ValueError('TIME_INVALID')
    return timestamp(value)


def _freshness_reasons(prefix, value, now, max_age_seconds):
    """Recheck one owner timestamp against the current owner clock."""
    if not isinstance(value, str) or not value:
        return [prefix + '_TIME_INVALID']
    try:
        observed = _time(value)
        age = (now - observed).total_seconds()
    except (TypeError, ValueError, OverflowError):
        return [prefix + '_TIME_INVALID']
    if age < 0:
        return [prefix + '_FUTURE']
    if max_age_seconds is not None and age > max_age_seconds:
        return [prefix + '_STALE']
    return []


def _terminal(state):
    return str(state or '').lower() in TERMINAL


def _component_body(value, *, name):
    if not isinstance(value, dict):
        return {'status': 'not_evaluated', 'reason': 'AUTHORITY_COMPONENT_INVALID',
                'input_sha256': None}
    status = value.get('status')
    if status not in {'evaluated', 'not_evaluated'}:
        return {'status': 'not_evaluated', 'reason': 'AUTHORITY_COMPONENT_INVALID',
                'input_sha256': None}
    body = {'status': status,
            'reason': value.get('reason'),
            'blocked': value.get('blocked'),
            'input_sha256': digest(value)}
    # Keep the owner-bound identities visible without accepting arbitrary
    # caller fields. Their values are also included in input_sha256.
    for key in ('applicable', 'account_key', 'at', 'day', 'terms',
                'snapshot_hash', 'snapshot_at', 'registry_sequence',
                'registry_head_hash', 'scope'):
        if key in value:
            body[key] = value[key]
    # Digest only the semantic component body; the digest itself is added last.
    return body


def _with_digest(body):
    result = dict(body)
    result['digest'] = digest(result)
    return result


def normalize_authority_component(value):
    """Public normalizer for one owner-read risk/lineage/control component."""
    return _with_digest(_component_body(value, name='authority'))


def _authority_not_evaluated(reason, **extra):
    return {'status': 'not_evaluated', 'blocked': None,
            'reason': reason, **extra}


def _risk_observation(snapshot):
    if not isinstance(snapshot, dict) or not snapshot:
        return None
    return {key: value for key, value in snapshot.items()
            if key not in JOURNAL_ONLY}


def _risk_component(book, snapshot, now):
    try:
        state = book.risk_guard.state()
    except Exception:
        return _authority_not_evaluated('RISK_STATE_UNAVAILABLE')
    if not isinstance(state, dict) or not state:
        return _authority_not_evaluated('RISK_STATE_ABSENT')
    if not isinstance(now, datetime) or now.tzinfo is None:
        return _authority_not_evaluated('AUTHORITY_CLOCK_INVALID')
    observation = _risk_observation(snapshot)
    if observation is None:
        return _authority_not_evaluated('RISK_SNAPSHOT_MISSING')
    required = ('terms', 'simulation', 'account_key', 'units', 'nav_high',
                'project_halted', 'daily_halted', 'day', 'day_start_equity',
                'executions', 'sticky_reasons', 'at', 'cash', 'equity',
                'snapshot_hash', 'blocked', 'reasons')
    missing = [key for key in required if key not in state]
    if missing:
        return _authority_not_evaluated('RISK_STATE_SCHEMA_MISSING:' +
                                        ','.join(missing))
    if type(state['blocked']) is not bool:
        return _authority_not_evaluated('RISK_BLOCKED_NOT_BOOL')
    if type(state['project_halted']) is not bool or type(state['daily_halted']) is not bool:
        return _authority_not_evaluated('RISK_HALT_FLAG_NOT_BOOL')
    if type(state['simulation']) is not bool or state['simulation'] is not book.simulation:
        return _authority_not_evaluated('RISK_SIMULATION_MISMATCH')
    if not isinstance(state['account_key'], str) or not state['account_key']:
        return _authority_not_evaluated('RISK_ACCOUNT_INVALID')
    if not isinstance(state['terms'], str) or not state['terms']:
        return _authority_not_evaluated('RISK_TERMS_INVALID')
    if not isinstance(state['executions'], dict):
        return _authority_not_evaluated('RISK_EXECUTIONS_INVALID')
    for key in ('sticky_reasons', 'reasons'):
        if (not isinstance(state[key], list)
                or any(not isinstance(item, str) for item in state[key])):
            return _authority_not_evaluated('RISK_' + key.upper() + '_INVALID')
    if not isinstance(state['at'], str) or not isinstance(state['day'], str):
        return _authority_not_evaluated('RISK_TIME_OR_DAY_INVALID')
    if not isinstance(state['snapshot_hash'], str) or len(state['snapshot_hash']) != 64:
        return _authority_not_evaluated('RISK_SNAPSHOT_HASH_INVALID')
    try:
        at = timestamp(state['at'])
        observed_at = timestamp(observation.get('observed_at'))
        age = (now - at).total_seconds()
        observed_age = (now - observed_at).total_seconds()
    except (TypeError, ValueError):
        return _authority_not_evaluated('RISK_TIME_INVALID')
    if at != observed_at:
        return _authority_not_evaluated('RISK_AT_SNAPSHOT_MISMATCH')
    if age < 0 or observed_age < 0:
        return _authority_not_evaluated('RISK_TIME_FUTURE')
    if age > MAX_OBSERVATION_AGE_SECONDS or observed_age > MAX_OBSERVATION_AGE_SECONDS:
        return _authority_not_evaluated('RISK_TIME_STALE')
    if state['day'] != observed_at.astimezone(NY).date().isoformat():
        return _authority_not_evaluated('RISK_DAY_MISMATCH')
    try:
        terms = digest({key: book.policy[key] for key in (
            'account_suffix', 'account_capital_usd',
            'stop_new_entries_at_daily_equity_loss_usd',
            'project_max_drawdown_fraction')})
    except (KeyError, TypeError, AttributeError):
        return _authority_not_evaluated('RISK_POLICY_TERMS_MISSING')
    if state['terms'] != terms:
        return _authority_not_evaluated('RISK_TERMS_MISMATCH')
    if observation.get('synthetic') is not book.simulation \
            or observation.get('collection_complete') is not True \
            or observation.get('provider') != 'robinhood':
        return _authority_not_evaluated('RISK_SNAPSHOT_BINDING_INVALID')
    if observation.get('account_key') != state['account_key'] \
            or observation.get('account_mask') != '••••' + book.policy.get('account_suffix', ''):
        return _authority_not_evaluated('RISK_SNAPSHOT_ACCOUNT_MISMATCH')
    try:
        book_state = book.state()
    except (AttributeError, KeyError, TypeError, OSError, RuntimeError):
        return _authority_not_evaluated('RISK_BOOK_STATE_UNAVAILABLE')
    if not isinstance(book_state, dict) or not isinstance(book_state.get('account_key'), str) \
            or not book_state['account_key']:
        return _authority_not_evaluated('RISK_BOOK_ACCOUNT_UNBOUND')
    if book_state['account_key'] != state['account_key']:
        return _authority_not_evaluated('RISK_BOOK_ACCOUNT_MISMATCH')
    if digest(observation) != state['snapshot_hash']:
        return _authority_not_evaluated('RISK_SNAPSHOT_HASH_MISMATCH')
    for key in ('units', 'nav_high', 'day_start_equity', 'cash', 'equity'):
        try:
            value = _decimal(state[key])
        except (ValueError, TypeError):
            return _authority_not_evaluated('RISK_' + key.upper() + '_INVALID')
        if not value.is_finite():
            return _authority_not_evaluated('RISK_' + key.upper() + '_INVALID')
        if key in {'units', 'nav_high', 'day_start_equity', 'equity'} and value <= 0:
            return _authority_not_evaluated('RISK_' + key.upper() + '_NONPOSITIVE')
    reasons = sorted(set(state['reasons'] + state['sticky_reasons']))
    inconsistent = bool(
        state['blocked'] != bool(reasons or state['project_halted'] or state['daily_halted']))
    if inconsistent:
        return _authority_not_evaluated('RISK_BLOCKED_STATE_INCONSISTENT')
    if state['project_halted'] and 'PROJECT_DRAWDOWN_LOCKED' not in reasons:
        return _authority_not_evaluated('RISK_PROJECT_LOCK_REASON_MISSING')
    if state['daily_halted'] and 'DAILY_EQUITY_LOSS_LOCKED' not in reasons:
        return _authority_not_evaluated('RISK_DAILY_LOCK_REASON_MISSING')
    return {'status': 'evaluated', 'blocked': state['blocked'],
            'reason': ','.join(reasons) if state['blocked'] else None,
            'account_key': state['account_key'], 'at': state['at'],
            'day': state['day'], 'terms': state['terms'],
            'snapshot_hash': state['snapshot_hash'],
            'state_sha256': digest(state)}


def _lineage_component(book, snapshot, now):
    try:
        from .session_lineage import lineage_entry_gate
        value = lineage_entry_gate(book, snapshot)
    except Exception:
        return _authority_not_evaluated('LINEAGE_READ_FAILED')
    if not isinstance(value, dict) or type(value.get('blocked')) is not bool:
        return _authority_not_evaluated('LINEAGE_RESULT_INVALID')
    reason = value.get('reason')
    blockers = value.get('blockers')
    if not isinstance(reason, str) or not reason:
        return _authority_not_evaluated('LINEAGE_REASON_INVALID')
    if not isinstance(blockers, list) or any(not isinstance(item, str) for item in blockers):
        return _authority_not_evaluated('LINEAGE_BLOCKERS_INVALID')
    if value.get('automatic_execution_enabled') is True:
        return _authority_not_evaluated('LINEAGE_CLAIMS_EXECUTION_AUTHORITY')
    if value['blocked'] is False and book.simulation is not True:
        if not isinstance(snapshot, dict) or not snapshot:
            return _authority_not_evaluated('LINEAGE_SNAPSHOT_MISSING')
        try:
            snapshot_at = timestamp(snapshot.get('observed_at'))
            result_at = timestamp(value.get('snapshot_at'))
            sequence = value.get('registry_sequence')
            head_hash = value.get('registry_head_hash')
            if result_at != snapshot_at or type(sequence) is not int or sequence <= 0 \
                    or not isinstance(head_hash, str) or len(head_hash) != 64 \
                    or type(value.get('registered_sessions')) is not int \
                    or value['registered_sessions'] <= 0:
                raise ValueError()
        except (TypeError, ValueError, KeyError):
            return _authority_not_evaluated('LINEAGE_SUCCESS_UNBOUND')
    return {'status': 'evaluated', 'blocked': value['blocked'],
            'reason': reason, 'blockers': blockers,
            'applicable': book.simulation is not True,
            'result_sha256': digest(value)}


def _control_component(book, purpose, evidence):
    if purpose is None:
        return _authority_not_evaluated('PURPOSE_REQUIRED')
    try:
        value = book.engineering_verification_gate(
            purpose=purpose, evidence=evidence)
    except Exception:
        return _authority_not_evaluated('CONTROL_READ_FAILED')
    if not isinstance(value, dict):
        return _authority_not_evaluated('CONTROL_RESULT_INVALID')
    if type(value.get('blocked')) is not bool or type(value.get('applicable')) is not bool:
        return _authority_not_evaluated('CONTROL_RESULT_INVALID')
    if value.get('execution_authorized') is True:
        return _authority_not_evaluated('CONTROL_CLAIMS_EXECUTION_AUTHORITY')
    reason = value.get('reason')
    if value['blocked'] is True and (not isinstance(reason, str) or not reason):
        return _authority_not_evaluated('CONTROL_BLOCKED_REASON_MISSING')
    if value['applicable'] is True and (not isinstance(reason, str) or not reason):
        return _authority_not_evaluated('CONTROL_APPLICABLE_REASON_MISSING')
    return {'status': 'evaluated', 'blocked': value['blocked'],
            'reason': reason if isinstance(reason, str) else None,
            'applicable': value['applicable'],
            'result_sha256': digest(value)}


def read_book_authority(book, *, purpose=None, evidence=None, snapshot=None):
    """Owner reader for the canonical risk/lineage/control inputs.

    Missing, malformed, unbound or inconsistent authority is ``not_evaluated``;
    it is never replaced with an optimistic zero or a strategy-supplied value.
    """
    try:
        now = book.clock()
    except Exception:
        now = None
    if isinstance(now, datetime) and now.tzinfo is not None:
        now = now.astimezone(timezone.utc)
    else:
        now = None
    return {
        'risk': _risk_component(book, snapshot, now),
        'lineage': _lineage_component(book, snapshot, now),
        'control': _control_component(book, purpose, evidence),
    }

def capture_recheck_reasons(capture):
    """Bounded generation recheck used inside the short claim transaction."""
    reasons = []
    if not isinstance(capture, dict):
        return ['RECEIPT_CAPTURE_MISSING']
    expected_generation = capture.get('expected_generation')
    if (expected_generation is not None
            and capture.get('generation') != expected_generation):
        return ['RECEIPT_CAPTURE_GENERATION_REPLACED']
    try:
        root = Path(capture['root'])
        if root.is_symlink() or not root.is_dir():
            return ['RECEIPT_CAPTURE_GENERATION_CHANGED']
        expected = {}
        for item in capture.get('file_stats') or []:
            if not isinstance(item, dict):
                return ['RECEIPT_CAPTURE_GENERATION_CHANGED']
            name = item.get('path')
            if not isinstance(name, str) or not name or '..' in Path(name).parts:
                return ['RECEIPT_CAPTURE_GENERATION_CHANGED']
            expected[name] = item
        raw_expected = {name for name in expected if name.startswith('raw/')}
        if (len(expected) > CAPTURE_RECHECK_MAX_FILES
                or len(raw_expected) > CAPTURE_RECHECK_MAX_FILES):
            return ['RECEIPT_CAPTURE_FILE_LIMIT']
        manifest_path = root / 'manifest.json'
        data = _read_public(manifest_path)
        if hashlib.sha256(data).hexdigest() != capture.get('manifest_sha256'):
            return ['RECEIPT_CAPTURE_GENERATION_CHANGED']
        manifest_info = os.stat(manifest_path, follow_symlinks=False)
        item = expected.get('manifest.json')
        if (not item or manifest_info.st_size != item.get('size')
                or manifest_info.st_mtime_ns != item.get('mtime_ns')):
            return ['RECEIPT_CAPTURE_GENERATION_CHANGED']
        raw_dir = root / 'raw'
        actual = set()
        seen = 0
        started = time.monotonic()
        try:
            iterator = os.scandir(raw_dir)
        except OSError:
            return ['RECEIPT_CAPTURE_DIRECTORY_UNAVAILABLE']
        with iterator:
            for entry in iterator:
                seen += 1
                if time.monotonic() - started > CAPTURE_RECHECK_MAX_SECONDS:
                    return ['RECEIPT_CAPTURE_DIRECTORY_TIME_LIMIT']
                if seen > len(raw_expected) + 1:
                    return ['RECEIPT_CAPTURE_EXTRA_FILES']
                if entry.is_symlink() or not entry.is_file() or not entry.name.endswith('.raw'):
                    return ['RECEIPT_CAPTURE_EXTRA_FILES']
                name = 'raw/' + entry.name
                item = expected.get(name)
                if item is None:
                    return ['RECEIPT_CAPTURE_EXTRA_FILES']
                actual.add(name)
                info = os.stat(entry.path, follow_symlinks=False)
                if (not item or info.st_size != item.get('size')
                        or info.st_mtime_ns != item.get('mtime_ns')):
                    return ['RECEIPT_CAPTURE_GENERATION_CHANGED']
        if actual != raw_expected:
            return ['RECEIPT_CAPTURE_GENERATION_CHANGED']
        if len(actual) + 1 != capture.get('file_count'):
            return ['RECEIPT_CAPTURE_GENERATION_CHANGED']
    except (CurrentEvidenceError, OSError, KeyError, TypeError, ValueError):
        return ['RECEIPT_CAPTURE_GENERATION_CHANGED']
    return reasons


def _public_recheck_reasons(source, now):
    reasons = []
    try:
        path = source.get('public_manifest_path')
        if not isinstance(path, str) or not path:
            return ['RECEIPT_PUBLIC_MANIFEST_MISSING']
        data = _read_public(path)
        if hashlib.sha256(data).hexdigest() != source.get('public_manifest_sha256'):
            return ['RECEIPT_PUBLIC_MANIFEST_CHANGED']
        info = os.stat(path, follow_symlinks=False)
        if (source.get('manifest_size') is not None
                and info.st_size != source.get('manifest_size')):
            reasons.append('RECEIPT_PUBLIC_MANIFEST_CHANGED')
        if (source.get('manifest_mtime_ns') is not None
                and info.st_mtime_ns != source.get('manifest_mtime_ns')):
            reasons.append('RECEIPT_PUBLIC_MANIFEST_CHANGED')
        for item in source.get('public_files') or []:
            if not isinstance(item, dict):
                reasons.append('RECEIPT_PUBLIC_SOURCE_MISSING')
                continue
            name = item.get('name')
            item_path = item.get('path')
            if not isinstance(item_path, str) or not item_path:
                reasons.append('RECEIPT_PUBLIC_SOURCE_MISSING:' + str(name))
                continue
            info = os.stat(item_path, follow_symlinks=False)
            if (info.st_size != item.get('size')
                    or info.st_mtime_ns != item.get('mtime_ns')):
                reasons.append('RECEIPT_PUBLIC_SOURCE_CHANGED:' + str(name))
            retrieved = item.get('retrieved_at')
            expires = item.get('expires_at')
            if not isinstance(retrieved, str) or not isinstance(expires, str):
                reasons.append('RECEIPT_PUBLIC_SOURCE_TIME_INVALID:' + str(name))
                continue
            try:
                retrieved_at = _time(retrieved)
                expires_at = _time(expires)
            except (TypeError, ValueError, OverflowError):
                reasons.append('RECEIPT_PUBLIC_SOURCE_TIME_INVALID:' + str(name))
                continue
            if now < retrieved_at:
                reasons.append('RECEIPT_PUBLIC_SOURCE_FUTURE:' + str(name))
            if now - retrieved_at > PUBLIC_SOURCE_MAX_AGE:
                reasons.append('RECEIPT_PUBLIC_SOURCE_EXPIRED:' + str(name))
            if now > expires_at:
                reasons.append('RECEIPT_PUBLIC_SOURCE_EXPIRED:' + str(name))
            if expires_at < retrieved_at:
                reasons.append('RECEIPT_PUBLIC_SOURCE_TIME_INVALID:' + str(name))
        if len(source.get('public_files') or []) == 0:
            reasons.append('RECEIPT_PUBLIC_SOURCE_MISSING')
    except (CurrentEvidenceError, OSError, KeyError, TypeError, ValueError):
        reasons.append('RECEIPT_PUBLIC_SOURCE_CHANGED')
    return sorted(set(reasons))


def authorization_context(book, *, mode, action=None, purpose=None, evidence=None):
    """Owner-read trade/action authorization context; never a broker permit."""
    if mode not in {'proposal', 'claim'}:
        return {'status': 'not_evaluated',
                'reason': 'AUTHORIZATION_MODE_INVALID'}
    if not isinstance(purpose, str) or not purpose:
        return {'status': 'not_evaluated', 'reason': 'PURPOSE_REQUIRED'}
    trade_id = action_id = None
    if mode == 'claim':
        if not isinstance(action, dict) or not isinstance(action.get('id'), str):
            return {'status': 'not_evaluated', 'reason': 'AUTHORIZATION_ACTION_MISSING'}
        action_id = action['id']
        trade_id = action.get('trade_id')
        try:
            state = book.state()
        except Exception:
            return {'status': 'not_evaluated', 'reason': 'AUTHORIZATION_TRADE_STATE_UNAVAILABLE'}
        if not isinstance(state, dict) or not isinstance(state.get('trades'), dict):
            return {'status': 'not_evaluated', 'reason': 'AUTHORIZATION_TRADE_STATE_INVALID'}
        trade = state['trades'].get(trade_id)
        if not isinstance(trade, dict):
            return {'status': 'not_evaluated', 'reason': 'AUTHORIZATION_TRADE_MISSING'}
        owner_purpose = trade.get('purpose')
        owner_evidence = trade.get('evidence')
        if owner_purpose != purpose:
            return {'status': 'not_evaluated',
                    'reason': 'AUTHORIZATION_PURPOSE_CHANGED'}
        if digest(owner_evidence) != digest(evidence):
            return {'status': 'not_evaluated',
                    'reason': 'AUTHORIZATION_EVIDENCE_CHANGED'}
        if action.get('trade_id') != trade.get('id'):
            return {'status': 'not_evaluated',
                    'reason': 'AUTHORIZATION_TRADE_ID_MISMATCH'}
        evidence = owner_evidence
    evidence_digest = digest(evidence) if evidence is not None else None
    parameters = {}
    if isinstance(evidence, dict):
        for key in ('parameters_valid_until', 'parameters_user_specified',
                    'engineering_scope_authorized', 'source_sha256'):
            if key in evidence:
                parameters[key] = evidence[key]
    return {'status': 'evaluated', 'mode': mode,
            'purpose': purpose, 'evidence_sha256': evidence_digest,
            'trade_id': trade_id, 'action_id': action_id,
            'parameters': parameters}


def _authorization_recheck(book, receipt, action, purpose=None, evidence=None):
    recorded = receipt.get('authorization')
    if not isinstance(recorded, dict) or recorded.get('status') != 'evaluated':
        return ['RECEIPT_AUTHORIZATION_MISSING']
    if recorded.get('mode') != receipt.get('mode'):
        return ['RECEIPT_AUTHORIZATION_MODE_MISMATCH']
    if receipt.get('mode') == 'proposal':
        if not isinstance(purpose, str) or not purpose or evidence is None:
            return ['RECEIPT_AUTHORIZATION_CONTEXT_MISSING']
        if recorded.get('purpose') != purpose:
            return ['RECEIPT_AUTHORIZATION_PURPOSE_CHANGED']
        if recorded.get('evidence_sha256') != digest(evidence):
            return ['RECEIPT_AUTHORIZATION_EVIDENCE_CHANGED']
        return []
    if receipt.get('mode') != 'claim':
        return ['RECEIPT_AUTHORIZATION_MODE_MISMATCH']
    if not isinstance(action, dict) or not isinstance(action.get('id'), str):
        return ['RECEIPT_AUTHORIZATION_ACTION_MISSING']
    if (recorded.get('action_id') != action.get('id')
            or recorded.get('trade_id') != action.get('trade_id')):
        return ['RECEIPT_AUTHORIZATION_ACTION_CHANGED']
    try:
        state = book.state()
        trade = state['trades'].get(action.get('trade_id')) if isinstance(state, dict) else None
    except Exception:
        return ['RECEIPT_AUTHORIZATION_TRADE_STATE_UNAVAILABLE']
    if not isinstance(trade, dict):
        return ['RECEIPT_AUTHORIZATION_TRADE_MISSING']
    if recorded.get('purpose') != trade.get('purpose'):
        return ['RECEIPT_AUTHORIZATION_PURPOSE_CHANGED']
    if recorded.get('evidence_sha256') != digest(trade.get('evidence')):
        return ['RECEIPT_AUTHORIZATION_EVIDENCE_CHANGED']
    return []


def build_receipt(*, mode, simulation, owner_binding_id, account, policy_sha256,
                  source, capture, journal, journal_health, action, quote,
                  components, reasons, assessed_at, authorization=None):
    """Versioned receipt; callers must pass owner-read facts, not strategy data."""
    if mode not in {'proposal', 'claim'}:
        _fail('RECEIPT_MODE_INVALID')
    body = {
        'schema_version': SCHEMA_VERSION,
        'kind': KIND,
        'mode': mode,
        'simulation': bool(simulation),
        'owner_binding_id': owner_binding_id,
        'assessed_at': assessed_at,
        'account': dict(account),
        'policy_sha256': policy_sha256,
        'source': dict(source),
        'capture': dict(capture),
        'journal': dict(journal),
        'journal_health': dict(journal_health),
        'action': dict(action),
        'quote': dict(quote),
        'components': dict(components),
        'authorization': dict(authorization or {}),
        'reasons': sorted(set(reasons or [])),
        'binding_is_not_entry_authorization': True,
    }
    receipt = dict(body)
    receipt['receipt_id'] = digest(body)
    return receipt


def receipt_of(decision):
    """Single accessor: never guess receipt fields from evidence payloads."""
    if not isinstance(decision, dict):
        return None
    receipt = decision.get('receipt')
    if not isinstance(receipt, dict):
        return None
    if receipt.get('schema_version') != SCHEMA_VERSION or receipt.get('kind') != KIND:
        return None
    if not isinstance(receipt.get('receipt_id'), str):
        return None
    body = {key: value for key, value in receipt.items() if key != 'receipt_id'}
    if digest(body) != receipt['receipt_id']:
        return None
    return receipt


def _verified_decision_components(receipt):
    return receipt.get('components') if isinstance(receipt.get('components'), dict) else {}


def _journal_row(book):
    try:
        current = book.db.execute(
            'SELECT payload FROM broker_current WHERE id=1').fetchone()
        health = book.db.execute(
            'SELECT payload FROM broker_health WHERE id=1').fetchone()
    except Exception:
        return None, None, 'RECEIPT_JOURNAL_UNAVAILABLE'
    if current is None:
        return None, None, 'RECEIPT_JOURNAL_MISSING'
    if health is None:
        return current[0], None, 'RECEIPT_HEALTH_MISSING'
    return current[0], health[0], None


def _action_row(book, action_id):
    try:
        row = book.db.execute(
            'SELECT id,trade_id,kind,state,payload FROM live_actions WHERE id=?',
            (action_id,)).fetchone()
    except Exception:
        return None
    return row


def _journal_consistency(evidence, snapshot, health):
    """Semantic capture ↔ journal comparison; formats are never hash-compared."""
    reasons = []
    compared = []
    scope = ((evidence.get('account_scope_applicability') or {}).get('scopes') or {})
    funds = evidence.get('funds_availability') or {}
    try:
        capture_at = evidence.get('observed_at')
        journal_at = snapshot.get('observed_at')
        if capture_at != journal_at:
            reasons.append(f'CURRENT_JOURNAL_OBSERVATION_MISMATCH:{capture_at}!={journal_at}')
        if health is not None and health.get('at') != journal_at:
            reasons.append('CURRENT_JOURNAL_HEALTH_MISMATCH')
        if snapshot.get('account_key') != (evidence.get('account') or {}).get('account_key_hash'):
            reasons.append('CURRENT_JOURNAL_ACCOUNT_MISMATCH')
        # Equity positions are the only journal position collection.
        capture_equity = ((scope.get('equity') or {}).get('positions') or {})
        if capture_equity.get('status') == 'observed_complete':
            expected = {}
            for record in capture_equity.get('records') or []:
                try:
                    if _decimal(record.get('quantity')) != 0:
                        expected[str(record.get('symbol'))] = str(_decimal(record.get('quantity')))
                except (ValueError, TypeError):
                    reasons.append('CURRENT_CAPTURE_POSITION_INVALID')
            journal_positions = snapshot.get('positions') or {}
            actual = {}
            if isinstance(journal_positions, dict):
                values = journal_positions.items()
            elif isinstance(journal_positions, list):
                values = ((item.get('symbol'), item) for item in journal_positions
                          if isinstance(item, dict))
            else:
                values = None
            if values is None:
                reasons.append('CURRENT_JOURNAL_POSITION_INVALID')
            else:
                for symbol, record in values:
                    try:
                        qty = _decimal((record or {}).get('quantity'))
                    except (ValueError, TypeError):
                        reasons.append('CURRENT_JOURNAL_POSITION_INVALID:' + str(symbol))
                        continue
                    if qty != 0:
                        actual[str(symbol)] = str(qty)
            if expected != actual:
                for symbol in sorted(set(expected) | set(actual)):
                    if expected.get(symbol) != actual.get(symbol):
                        reasons.append(
                            'CURRENT_JOURNAL_POSITION_CONFLICT:' + symbol +
                            ':' + str(expected.get(symbol)) + ':' + str(actual.get(symbol)))
            compared.append('equity_positions')
        # Open order identities across the complete capture scopes.
        capture_open = set()
        capture_orders_known = True
        for name in ('equity', 'options', 'crypto'):
            item = ((scope.get(name) or {}).get('orders') or {})
            if item.get('status') != 'observed_complete':
                capture_orders_known = False
                continue
            for record in item.get('records') or []:
                if not _terminal(record.get('state')):
                    capture_open.add(str(record.get('id')))
        advanced_orders = ((scope.get('advanced') or {}).get('orders') or {})
        if advanced_orders.get('status') != 'observed_complete':
            capture_orders_known = False
        else:
            for record in advanced_orders.get('records') or []:
                if not _terminal(record.get('state')):
                    capture_open.add(str(record.get('id')))
        if capture_orders_known:
            journal_orders = snapshot.get('orders') or {}
            if not isinstance(journal_orders, dict):
                reasons.append('CURRENT_JOURNAL_ORDER_INVALID')
            else:
                journal_open = {str(oid) for oid, order in journal_orders.items()
                                if not _terminal((order or {}).get('state'))}
                for oid in sorted(capture_open - journal_open):
                    reasons.append('CURRENT_JOURNAL_ORDER_MISSING:' + oid)
                for oid in sorted(journal_open - capture_open):
                    reasons.append('CURRENT_JOURNAL_ORDER_NOT_IN_CAPTURE:' + oid)
                compared.append('open_orders')
        # Funds are separate current money facts.
        values = funds.get('values') if funds.get('money_facts_valid') is True else None
        if isinstance(values, dict):
            journal_values = {
                'cash': (snapshot.get('portfolio') or {}).get('cash'),
                'total_value': (snapshot.get('portfolio') or {}).get('total_value'),
                'pending_deposits': (snapshot.get('portfolio') or {}).get('pending_deposits'),
                'buying_power': snapshot.get('buying_power'),
                'unleveraged_buying_power': snapshot.get('unleveraged_buying_power'),
            }
            for key, capture_value in values.items():
                if key in {'asset_values', 'currency', 'source',
                           'buying_power_equals_unleveraged', 'cash_equals_buying_power',
                           'buying_power_negative', 'unleveraged_buying_power_negative',
                           'buying_power_source', 'unleveraged_buying_power_source',
                           'broker_buying_power_authoritative_for_spendability',
                           'cash_is_not_buying_power'}:
                    continue
                try:
                    left = _decimal(capture_value)
                    right = _decimal(journal_values.get(key))
                except (ValueError, TypeError):
                    reasons.append('CURRENT_JOURNAL_FUNDS_INVALID:' + key)
                    continue
                if left != right:
                    reasons.append(
                        'CURRENT_JOURNAL_FUNDS_MISMATCH:' + key +
                        ':' + str(left) + ':' + str(right))
            for key, capture_value in (values.get('asset_values') or {}).items():
                try:
                    left = _decimal(capture_value)
                    right = _decimal((snapshot.get('other_asset_values') or {}).get(key))
                except (ValueError, TypeError):
                    reasons.append('CURRENT_JOURNAL_ASSET_VALUE_INVALID:' + key)
                    continue
                if left != right:
                    reasons.append(
                        'CURRENT_JOURNAL_ASSET_VALUE_MISMATCH:' + key +
                        ':' + str(left) + ':' + str(right))
            compared.append('funds')
        approvals = ((evidence.get('account_scope_applicability') or {}).get('approvals') or {})
        if approvals.get('status') == 'observed_complete':
            try:
                capture_count = int(approvals.get('count'))
                journal_count = int(snapshot.get('pending_approvals'))
            except (TypeError, ValueError):
                reasons.append('CURRENT_JOURNAL_APPROVAL_INVALID')
            else:
                if capture_count != journal_count:
                    reasons.append(
                        'CURRENT_JOURNAL_APPROVAL_COUNT_MISMATCH:' +
                        str(capture_count) + ':' + str(journal_count))
                compared.append('approvals')
        setting = ((evidence.get('account_scope_applicability') or {}).get('approval_setting') or {})
        if setting.get('status') == 'observed_single_pass':
            if setting.get('human_must_approve_trades') is not snapshot.get('human_must_approve_trades'):
                reasons.append('CURRENT_JOURNAL_APPROVAL_SETTING_CHANGED')
            compared.append('approval_setting')
        advanced_groups = ((scope.get('advanced') or {}).get('groups') or {})
        if advanced_groups.get('status') == 'observed_complete':
            groups = snapshot.get('groups')
            if not isinstance(groups, list) or len(groups) != advanced_groups.get('count'):
                reasons.append('CURRENT_JOURNAL_ADVANCED_GROUPS_MISMATCH')
            compared.append('advanced_groups')
        option_open = ((scope.get('options') or {}).get('orders') or {}).get('open_count')
        crypto_open = ((scope.get('crypto') or {}).get('orders') or {}).get('open_count')
        if isinstance(option_open, int) and isinstance(crypto_open, int):
            try:
                journal_other = int(snapshot.get('other_asset_open_orders'))
            except (TypeError, ValueError):
                reasons.append('CURRENT_JOURNAL_OTHER_ASSET_ORDER_INVALID')
            else:
                if journal_other != option_open + crypto_open:
                    reasons.append(
                        'CURRENT_JOURNAL_OTHER_ASSET_ORDER_MISMATCH:' +
                        str(option_open + crypto_open) + ':' + str(journal_other))
                compared.append('other_asset_open_orders')
        for issue in snapshot.get('reconciliation_issues') or []:
            reasons.append('CURRENT_JOURNAL_RECONCILIATION_ISSUE:' + str(issue))
    except (KeyError, TypeError, ValueError, AttributeError):
        reasons.append('CURRENT_JOURNAL_CAPTURE_COMPARISON_FAILED')
    return sorted(set(reasons)), sorted(set(compared))


def journal_components(book, evidence):
    """Read broker_current/health rows as owner facts and compare semantics."""
    current_raw, health_raw, error = _journal_row(book)
    if error:
        return ({'status': 'not_evaluated', 'reason': error},
                {'status': 'not_evaluated', 'reason': error},
                {'status': 'not_evaluated', 'reason': error}, [error])
    snapshot = _json_object(current_raw)
    health = _json_object(health_raw)
    if snapshot is None:
        reason = 'CURRENT_PREFLIGHT_JOURNAL_INVALID'
        return ({'status': 'not_evaluated', 'reason': reason},
                {'status': 'not_evaluated', 'reason': reason},
                {'status': 'not_evaluated', 'reason': reason}, [reason])
    current = {
        'status': 'evaluated',
        'sha256': _sha256_text(current_raw),
        'observed_at': snapshot.get('observed_at'),
        'account_key': snapshot.get('account_key'),
        'collection_complete': snapshot.get('collection_complete'),
    }
    if health is None:
        health_component = {'status': 'not_evaluated',
                            'reason': 'CURRENT_PREFLIGHT_HEALTH_INVALID'}
        consistency, compared = ({'status': 'not_evaluated',
                                  'reason': 'CURRENT_PREFLIGHT_HEALTH_INVALID'}, [])
        reasons = ['CURRENT_PREFLIGHT_HEALTH_INVALID']
    else:
        health_component = {
            'status': 'evaluated',
            'sha256': _sha256_text(health_raw),
            'at': health.get('at'),
            'healthy': health.get('healthy'),
        }
        consistency_reasons, compared = _journal_consistency(
            evidence, snapshot, health)
        consistency = {'status': 'evaluated',
                       'reasons': consistency_reasons,
                       'compared': compared}
        reasons = consistency_reasons
    return current, health_component, consistency, reasons


def _authority_recheck(book, receipt, *, purpose, evidence, snapshot,
                       authority_reader):
    reasons = []
    components = _verified_decision_components(receipt)
    if authority_reader is None:
        for name in AUTHORITY_NAMES:
            if (components.get(name) or {}).get('status') == 'evaluated':
                reasons.append('RECEIPT_' + name.upper() + '_RECHECK_UNAVAILABLE')
        return reasons
    try:
        fresh = authority_reader(book, purpose=purpose, evidence=evidence,
                                 snapshot=snapshot)
    except Exception:
        return ['RECEIPT_AUTHORITY_RECHECK_FAILED']
    for name in AUTHORITY_NAMES:
        recorded = components.get(name) or {}
        if recorded.get('status') != 'evaluated':
            continue
        current = _with_digest(_component_body(fresh.get(name), name=name))
        if (current.get('digest') != recorded.get('digest')
                or current.get('blocked') != recorded.get('blocked')):
            reasons.append('RECEIPT_' + name.upper() + '_CHANGED')
    return reasons


def verify_receipt_binding(receipt, *, book, action=None, now, require_journal,
                           owner_binding_id=None, expected_source_fingerprint=None,
                           expected_account_key=None, purpose=None, evidence=None,
                           snapshot=None, authority_reader=None,
                           formal_authority=None):
    """Owner recheck of one receipt; binding truth is independent of entry deny."""
    reasons = []
    if receipt is None:
        return {'bound': False, 'reasons': ['RECEIPT_MISSING'], 'components': {}}
    components = _verified_decision_components(receipt)
    reasons.extend(_freshness_reasons(
        'RECEIPT_ASSESSED_AT', receipt.get('assessed_at'), now, None))
    if receipt.get('simulation') is not book.simulation:
        reasons.append('RECEIPT_SIMULATION_MISMATCH')
    if owner_binding_id is None or receipt.get('owner_binding_id') != owner_binding_id:
        reasons.append('RECEIPT_OWNER_BINDING_MISMATCH')
    account = receipt.get('account') or {}
    if (account.get('configured_key_hash') is not None
            and account.get('configured_key_hash') != account.get('key_hash')):
        reasons.append('RECEIPT_ACCOUNT_BINDING_MISMATCH')
    if expected_account_key is not None and account.get('key_hash') != expected_account_key:
        reasons.append('RECEIPT_ACCOUNT_MISMATCH')
    if account.get('suffix') != book.policy.get('account_suffix'):
        reasons.append('RECEIPT_ACCOUNT_SUFFIX_MISMATCH')
    if receipt.get('policy_sha256') != digest(book.policy):
        reasons.append('RECEIPT_POLICY_MISMATCH')
    source = receipt.get('source') or {}
    if expected_source_fingerprint is not None and source.get('expected_fingerprint') != expected_source_fingerprint:
        reasons.append('RECEIPT_SOURCE_BINDING_MISMATCH')
    if source.get('observed_fingerprint') != source.get('expected_fingerprint'):
        reasons.append('RECEIPT_SOURCE_FINGERPRINT_MISMATCH')
    pin = source.get('pin') or {}
    current_pin = source_pin()
    if (pin.get('status') != 'bounded'
            or pin.get('truncated') is not False
            or current_pin.get('status') != 'bounded'
            or current_pin.get('truncated') is not False):
        reasons.append('RECEIPT_SOURCE_PIN_LIMIT:' +
                       str(pin.get('reason') or current_pin.get('reason') or
                           'NOT_COMPLETE'))
    elif (pin.get('digest') != current_pin.get('digest')
            or pin.get('count') != current_pin.get('count')
            or pin.get('root') != current_pin.get('root')):
        reasons.append('RECEIPT_SOURCE_CHANGED')
    if (not isinstance(source.get('public_manifest_sha256'), str)
            or not source.get('public_manifest_sha256')):
        reasons.append('RECEIPT_PUBLIC_MANIFEST_MISSING')
    else:
        reasons.extend(_public_recheck_reasons(source, now))
    capture = receipt.get('capture') or {}
    reasons.extend(_freshness_reasons(
        'RECEIPT_CAPTURE', capture.get('observed_at'), now,
        MAX_OBSERVATION_AGE_SECONDS))
    try:
        capture_started = _time(capture.get('collection_started_at'))
        capture_observed = _time(capture.get('observed_at'))
        window = (capture_observed - capture_started).total_seconds()
        if window < 0:
            reasons.append('RECEIPT_CAPTURE_WINDOW_INVALID')
        elif window > MAX_OBSERVATION_AGE_SECONDS:
            reasons.append('RECEIPT_CAPTURE_WINDOW_EXCEEDED')
        if (capture.get('collection_window_seconds') is not None
                and abs(float(capture.get('collection_window_seconds')) - window) > 0.001):
            reasons.append('RECEIPT_CAPTURE_WINDOW_MISMATCH')
    except (TypeError, ValueError, OverflowError):
        reasons.append('RECEIPT_CAPTURE_WINDOW_INVALID')
    reasons.extend(capture_recheck_reasons(capture))
    mode = receipt.get('mode')
    if mode == 'claim' or require_journal is True:
        action_component = receipt.get('action') or {}
        if action is None:
            reasons.append('RECEIPT_ACTION_MISSING')
        elif action_component.get('status') != 'evaluated':
            reasons.append('RECEIPT_ACTION_NOT_EVALUATED')
        else:
            payload = action.get('payload')
            if (action_component.get('id') != action.get('id')
                    or action_component.get('trade_id') != action.get('trade_id')
                    or action_component.get('kind') != action.get('kind')
                    or action_component.get('state') != action.get('state')
                    or action_component.get('payload_sha256') != _sha256_text(str(payload or ''))):
                reasons.append('RECEIPT_ACTION_CHANGED')
    quote_component = receipt.get('quote') or {}
    if quote_component.get('status') == 'evaluated':
        for key, label in (('bid_at', 'BID'), ('ask_at', 'ASK'),
                           ('observed_at', 'OBSERVED')):
            reasons.extend(_freshness_reasons(
                'RECEIPT_QUOTE_' + label, quote_component.get(key), now,
                MAX_QUOTE_AGE_SECONDS))
        # Backward-compatible aggregate reason for the observed-at field.
        try:
            observed = _time(quote_component.get('observed_at'))
            age = (now - observed).total_seconds()
            if age < 0:
                reasons.append('RECEIPT_QUOTE_FUTURE')
            elif age > MAX_QUOTE_AGE_SECONDS:
                reasons.append('RECEIPT_QUOTE_STALE')
        except (TypeError, ValueError):
            reasons.append('RECEIPT_QUOTE_TIME_INVALID')
    elif mode == 'claim' or require_journal is True:
        reasons.append('RECEIPT_QUOTE_NOT_EVALUATED')
    if require_journal is True:
        journal = receipt.get('journal') or {}
        current_raw, health_raw, error = _journal_row(book)
        if error or current_raw is None:
            reasons.append(error or 'RECEIPT_JOURNAL_MISSING')
        else:
            if journal.get('status') != 'evaluated':
                reasons.append('RECEIPT_JOURNAL_NOT_EVALUATED')
            reasons.extend(_freshness_reasons(
                'RECEIPT_JOURNAL', journal.get('observed_at'), now,
                MAX_OBSERVATION_AGE_SECONDS))
            if journal.get('sha256') != _sha256_text(current_raw):
                reasons.append('RECEIPT_JOURNAL_CHANGED')
            journal_snapshot = _json_object(current_raw)
            health = _json_object(health_raw)
            health_component = receipt.get('journal_health') or {}
            if health_raw is None or health is None:
                reasons.append('RECEIPT_HEALTH_MISSING')
            else:
                if health_component.get('status') != 'evaluated':
                    reasons.append('RECEIPT_HEALTH_NOT_EVALUATED')
                if health_component.get('sha256') != _sha256_text(health_raw):
                    reasons.append('RECEIPT_HEALTH_CHANGED')
                reasons.extend(_freshness_reasons(
                    'RECEIPT_HEALTH', health.get('at'), now,
                    MAX_OBSERVATION_AGE_SECONDS))
                if health.get('at') != (journal_snapshot or {}).get('observed_at'):
                    reasons.append('RECEIPT_HEALTH_SNAPSHOT_MISMATCH')
                if health_component.get('at') != health.get('at'):
                    reasons.append('RECEIPT_HEALTH_CHANGED')
                if health_component.get('healthy') is not True or health.get('healthy') is not True:
                    reasons.append('RECEIPT_HEALTH_UNHEALTHY')
    authority_snapshot = snapshot
    if authority_snapshot is None and require_journal is True:
        current_raw, _, _ = _journal_row(book)
        authority_snapshot = _json_object(current_raw)
    reasons.extend(_authority_recheck(
        book, receipt, purpose=purpose, evidence=evidence,
        snapshot=authority_snapshot, authority_reader=authority_reader))
    reasons.extend(_authorization_recheck(
        book, receipt, action, purpose=purpose, evidence=evidence))
    formal_component = components.get('formal_channel')
    if formal_authority is not None and isinstance(formal_component, dict):
        reasons.extend(formal_authority.recheck_receipt(
            formal_component, book=book, action=action, purpose=purpose,
            evidence=evidence))
    return {'bound': not sorted(set(reasons)),
            'reasons': sorted(set(reasons)),
            'components': components}
