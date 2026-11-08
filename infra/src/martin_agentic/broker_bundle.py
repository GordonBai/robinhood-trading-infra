"""Bounded read-only broker capture into a private saved tool-envelope bundle.

The transport is injected.  This module never creates a broker transport, opens
SQLite, writes a ledger/risk/Host/control file, applies a baseline, or calls any
write/preview/approval tool.  Raw files are deterministic local JSON
serializations of the tool-result envelopes returned by the caller's read-only
transport; they are not broker wire bytes or signatures.
"""
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import stat
import time

from .baseline_evidence import (
    BundleError, _next, _unwrap, load_saved_bundle, normalize_bundle)
from .capture_archive import _immutable_write
from .transport import CodexBrokerReadTransport, TransportError
from trading_contracts.values import canonical, digest


MAX_PAGES_PER_TOOL_PASS = 100
MAX_TOTAL_CALLS = 512
MAX_PAGE_BYTES = 16 * 1024 * 1024
MAX_TOTAL_BYTES = 64 * 1024 * 1024
MAX_CAPTURE_SECONDS = 30
SINGLE_PASS_TOOLS = ('get_portfolio', 'get_trade_approval_setting')
TWO_PASS_TOOLS = (
    'get_equity_positions', 'get_equity_orders', 'get_advanced_orders',
    'get_option_positions', 'get_option_orders', 'get_crypto_positions',
    'get_crypto_orders', 'get_trade_approvals',
)
CRYPTO_TOOLS = frozenset({'get_crypto_positions', 'get_crypto_orders'})
PAGE_KEYS = frozenset({
    'tool', 'args', 'pass', 'page_index', 'cursor_requested',
    'cursor_returned', 'started_at', 'completed_at', 'raw_path',
    'raw_sha256', 'raw_bytes', 'request_args_sha256',
})


class CollectorError(ValueError):
    def __init__(self, code, detail=None):
        self.code = code
        self.detail = detail
        super().__init__(code)

    def __str__(self):
        return self.code if self.detail is None else f'{self.code}:{self.detail}'


def _fail(code, detail=None):
    raise CollectorError(code, detail)


class _State:
    def __init__(self, root, account_suffix, clock, monotonic, timeout_seconds):
        self.root = Path(root)
        self.raw_dir = self.root / 'raw'
        self.account_suffix = account_suffix
        self.clock = clock
        self.monotonic = monotonic
        self.timeout_seconds = timeout_seconds
        self.started_at = None
        self.completed_at = None
        self.started_mono = None
        self.deadline = None
        self.last_mono = None
        self.pages = []
        self.receipts = []
        self.calls = 0
        self.raw_bytes = 0
        self.account_key = None
        self.account_mask = '••••' + account_suffix
        self.account_number = None
        self.rhs_account_number = None
        self.missing_conditions = []
        self.captured_tools = []
        self.errors = []
        self.sequence = 0

    def wall(self):
        try:
            value = self.clock()
        except Exception:
            _fail('COLLECTOR_CLOCK_INVALID')
        if not isinstance(value, datetime) or value.tzinfo is None \
                or value.utcoffset() is None:
            _fail('COLLECTOR_CLOCK_INVALID')
        value = value.astimezone(timezone.utc)
        if self.completed_at is not None and value < self.completed_at:
            _fail('COLLECTOR_CLOCK_BACKWARDS')
        return value

    def mono(self):
        try:
            value = self.monotonic()
        except Exception:
            _fail('COLLECTOR_MONOTONIC_INVALID')
        if type(value) not in (int, float) or not math.isfinite(value):
            _fail('COLLECTOR_MONOTONIC_INVALID')
        value = float(value)
        if self.last_mono is not None and value < self.last_mono:
            _fail('COLLECTOR_MONOTONIC_BACKWARDS')
        self.last_mono = value
        return value

    def check_budget(self):
        now = self.mono()
        if now > self.deadline:
            _fail('COLLECTOR_TIME_BUDGET_EXCEEDED')
        wall = self.wall()
        if (wall - self.started_at).total_seconds() > MAX_CAPTURE_SECONDS:
            _fail('COLLECTOR_TIME_BUDGET_EXCEEDED')
        return wall


def _prepare_output(output_dir):
    output = Path(output_dir)
    if not output.name or '..' in output.parts:
        _fail('COLLECTOR_OUTPUT_INVALID')
    if output.exists() or output.is_symlink():
        _fail('COLLECTOR_OUTPUT_EXISTS')
    parent = output.parent
    if parent.is_symlink() or not parent.is_dir():
        _fail('COLLECTOR_PARENT_INVALID')
    try:
        parent_mode = parent.stat().st_mode
    except OSError:
        _fail('COLLECTOR_PARENT_INVALID')
    if parent_mode & 0o022:
        _fail('COLLECTOR_PARENT_NOT_PRIVATE')
    try:
        os.mkdir(output, 0o700)
    except FileExistsError:
        _fail('COLLECTOR_OUTPUT_EXISTS')
    except OSError:
        _fail('COLLECTOR_OUTPUT_UNAVAILABLE')
    os.chmod(output, 0o700)
    try:
        (output / 'raw').mkdir(mode=0o700)
    except OSError:
        _fail('COLLECTOR_OUTPUT_UNAVAILABLE')
    return output


def _envelope(response):
    if not isinstance(response, dict) or response.get('isError'):
        return None, None
    try:
        data = _unwrap(response)
    except BundleError as exc:
        _fail('COLLECTOR_TOOL_RESULT_INVALID', exc.code)
    if not isinstance(data, dict):
        _fail('COLLECTOR_TOOL_RESULT_INVALID', 'data')
    try:
        following = _next(data)
    except BundleError as exc:
        _fail('COLLECTOR_CURSOR_INVALID', exc.code)
    return data, following


def _account_identity_checked(value, account, rhs):
    if isinstance(value, dict):
        if 'account_number' in value:
            found = value['account_number']
            if not isinstance(found, str) or found != account:
                _fail('COLLECTOR_ACCOUNT_IDENTITY_CHANGED')
        if 'rhs_account_number' in value:
            found = value['rhs_account_number']
            if found is not None:
                if not rhs or not isinstance(found, str) or found != rhs:
                    _fail('COLLECTOR_ACCOUNT_IDENTITY_CHANGED')
        for item in value.values():
            _account_identity_checked(item, account, rhs)
    elif isinstance(value, list):
        for item in value:
            _account_identity_checked(item, account, rhs)


def _discover_account(data, suffix):
    accounts = data.get('accounts')
    if not isinstance(accounts, list) or any(
            not isinstance(row, dict) for row in accounts):
        _fail('COLLECTOR_DISCOVERY_SCHEMA')
    matches = [row for row in accounts
               if isinstance(row.get('account_number'), str)
               and row['account_number'].endswith(suffix)]
    if len(matches) != 1:
        _fail('COLLECTOR_ACCOUNT_AMBIGUOUS' if matches
              else 'COLLECTOR_ACCOUNT_NOT_FOUND')
    row = matches[0]
    if (row.get('agentic_allowed') is not True
            or row.get('deactivated') is not False
            or row.get('permanently_deactivated') is not False
            or row.get('state') != 'active'):
        _fail('COLLECTOR_ACCOUNT_INACTIVE_OR_UNAUTHORIZED')
    rhs = row.get('rhs_account_number')
    if not isinstance(rhs, str) or not rhs:
        rhs = None
    return row['account_number'], rhs


def _one_call(state, transport, tool, *, pass_number, page_index,
              cursor_requested, base_args):
    if page_index >= MAX_PAGES_PER_TOOL_PASS:
        _fail('COLLECTOR_PAGE_LIMIT_EXCEEDED', tool)
    if state.calls >= MAX_TOTAL_CALLS:
        _fail('COLLECTOR_CALL_LIMIT_EXCEEDED')
    state.check_budget()
    started_wall = state.wall()
    started_mono = state.mono()
    args = dict(base_args)
    if cursor_requested is not None:
        args['cursor'] = cursor_requested
    state.calls += 1
    try:
        response = transport(tool, args)
    except TransportError as exc:
        state.errors.append({'tool': tool, 'pass': pass_number,
                             'page_index': page_index, 'code': str(exc)})
        _fail('COLLECTOR_TRANSPORT_ERROR', str(exc))
    except ValueError:
        _fail('COLLECTOR_TRANSPORT_ARGUMENT_INVALID', tool)
    if not isinstance(response, dict):
        _fail('COLLECTOR_TOOL_RESULT_INVALID', tool)
    raw = canonical(response).encode()
    end_wall = state.wall()
    end_mono = state.mono()
    if end_wall < started_wall:
        _fail('COLLECTOR_CLOCK_BACKWARDS')
    if len(raw) > MAX_PAGE_BYTES:
        _fail('COLLECTOR_PAGE_BYTES_EXCEEDED', tool)
    if state.raw_bytes + len(raw) > MAX_TOTAL_BYTES:
        _fail('COLLECTOR_TOTAL_BYTES_EXCEEDED')
    state.sequence += 1
    name = f'{state.sequence:04d}-{tool}-p{pass_number}-{page_index}.raw'
    raw_path = state.raw_dir / name
    _immutable_write(raw_path, raw)
    state.raw_bytes += len(raw)
    state.completed_at = end_wall
    page = {
        'tool': tool, 'args': args, 'pass': pass_number,
        'page_index': page_index, 'cursor_requested': cursor_requested,
        'cursor_returned': None,
        'started_at': started_wall.isoformat(),
        'completed_at': end_wall.isoformat(),
        'raw_path': 'raw/' + name,
        'raw_sha256': hashlib.sha256(raw).hexdigest(),
        'raw_bytes': len(raw),
        'request_args_sha256': digest(args),
    }
    if set(page) != PAGE_KEYS:
        _fail('COLLECTOR_PAGE_SCHEMA')
    state.pages.append(page)
    receipt = {key: page[key] for key in (
        'tool', 'pass', 'page_index', 'cursor_requested', 'cursor_returned',
        'started_at', 'completed_at', 'raw_path', 'raw_sha256', 'raw_bytes',
        'request_args_sha256')}
    state.receipts.append(receipt)
    data, following = _envelope(response)
    page['cursor_returned'] = following
    receipt['cursor_returned'] = following
    if tool != 'get_accounts' and state.account_number is not None:
        _account_identity_checked(data, state.account_number,
                                  state.rhs_account_number)
    if (end_mono - started_mono > MAX_CAPTURE_SECONDS
            or end_mono > state.deadline
            or (end_wall - state.started_at).total_seconds() > MAX_CAPTURE_SECONDS):
        _fail('COLLECTOR_TIME_BUDGET_EXCEEDED')
    if response.get('isError'):
        state.errors.append({'tool': tool, 'pass': pass_number,
                             'page_index': page_index,
                             'code': 'COLLECTOR_TOOL_RESULT_ERROR'})
        _fail('COLLECTOR_TOOL_RESULT_ERROR', tool)
    return data, following


def _single_tool(state, transport, tool, base_args):
    data, following = _one_call(
        state, transport, tool, pass_number=1, page_index=0,
        cursor_requested=None, base_args=base_args)
    if following is not None:
        _fail('COLLECTOR_UNEXPECTED_PAGINATION', tool)
    state.captured_tools.append(tool)
    return data


def _paged_tool(state, transport, tool, pass_number, base_args):
    cursor = None
    seen = set()
    page_index = 0
    while True:
        data, following = _one_call(
            state, transport, tool, pass_number=pass_number,
            page_index=page_index, cursor_requested=cursor,
            base_args=base_args)
        page_index += 1
        if following is None:
            break
        if following in seen:
            _fail('COLLECTOR_CURSOR_LOOP', tool)
        if page_index >= MAX_PAGES_PER_TOOL_PASS:
            _fail('COLLECTOR_PAGE_LIMIT_EXCEEDED', tool)
        seen.add(following)
        cursor = following
    if tool not in state.captured_tools:
        state.captured_tools.append(tool)


def _base_args(state, tool):
    if tool in CRYPTO_TOOLS:
        return {'rhs_account_number': state.rhs_account_number}
    return {'account_number': state.account_number}


def _capture(state, transport):
    data, following = _one_call(
        state, transport, 'get_accounts', pass_number=1, page_index=0,
        cursor_requested=None, base_args={})
    if following is not None:
        _fail('COLLECTOR_UNEXPECTED_PAGINATION', 'get_accounts')
    account, rhs = _discover_account(data, state.account_suffix)
    state.account_number = account
    state.rhs_account_number = rhs
    state.account_key = hashlib.sha256(account.encode()).hexdigest()
    try:
        transport.bind(account, rhs)
    except ValueError:
        _fail('COLLECTOR_BIND_REJECTED')
    state.captured_tools.append('get_accounts')
    if rhs is None:
        state.missing_conditions.append('CRYPTO_RHS_MISSING')

    for tool in SINGLE_PASS_TOOLS:
        _single_tool(state, transport, tool, _base_args(state, tool))

    active_two_pass = [tool for tool in TWO_PASS_TOOLS
                       if tool not in CRYPTO_TOOLS or state.rhs_account_number]
    for pass_number in (1, 2):
        for tool in active_two_pass:
            _paged_tool(state, transport, tool, pass_number,
                        _base_args(state, tool))


def _manifest(state):
    return {
        'schema_version': 1, 'provider': 'robinhood',
        'account_suffix': state.account_suffix,
        'pages': state.pages,
    }


def _provenance(state, *, status, blockers=None, error=None,
                raw_chain_sha256=None):
    return {
        'schema_version': 1, 'kind': 'saved_bundle_capture_provenance',
        'status': status,
        'account_key': state.account_key, 'account_mask': state.account_mask,
        'account_suffix': state.account_suffix,
        'envelope_provenance': 'local_canonical_tool_envelope_json_v1',
        'not_broker_wire_bytes': True, 'not_broker_signature': True,
        'started_at': (state.started_at.isoformat()
                       if state.started_at is not None else None),
        'completed_at': ((state.completed_at or state.started_at).isoformat()
                         if (state.completed_at or state.started_at) is not None
                         else None),
        'calls': state.calls, 'pages': len(state.pages),
        'raw_bytes': state.raw_bytes,
        'raw_chain_sha256': raw_chain_sha256,
        'captured_tools': sorted(set(state.captured_tools)),
        'missing_conditions': list(state.missing_conditions),
        'page_receipts': list(state.receipts),
        'errors': list(state.errors),
        'blockers': list(blockers or []),
        'error': error,
        'production_ready': False, 'live_enabled': False,
        'execution_authorized': False,
    }


def _failure_summary(state, output, code, detail):
    return {
        'schema_version': 1, 'kind': 'saved_bundle_capture',
        'status': 'FAILED', 'blockers': [code], 'blocker_detail': detail,
        'output_dir': str(output), 'account_key': state.account_key,
        'account_mask': state.account_mask, 'account_suffix': state.account_suffix,
        'calls': state.calls, 'pages': len(state.pages),
        'raw_bytes': state.raw_bytes,
        'missing_conditions': list(state.missing_conditions),
        'baseline_snapshot': None, 'candidate_snapshot_present': False,
        'ready': False, 'conditions_missing': False,
        'production_ready': False, 'live_enabled': False,
        'execution_authorized': False,
    }


def _write_failure(state, output, code, detail):
    manifest = output / 'manifest.json'
    try:
        if manifest.exists() or manifest.is_symlink():
            manifest.unlink()
    except OSError:
        pass
    try:
        provenance = _provenance(
            state, status='FAILED', error={'code': code, 'detail': detail})
        _immutable_write(output / 'capture-provenance.json',
                         (canonical(provenance) + '\n').encode())
    except Exception:
        pass
    return _failure_summary(state, output, code, detail)


def collect_saved_bundle(output_dir, *, account_suffix, transport,
                         timeout_seconds=30, clock=None, monotonic=None,
                         deadline_monotonic=None):
    """Capture read-only transport envelopes into one new private bundle.

    Returns CAPTURED, CAPTURED_CONDITIONS_MISSING or FAILED.  Output-directory
    conflicts raise CollectorError before any new directory is used.
    """
    if not isinstance(account_suffix, str) or len(account_suffix) != 4 \
            or not account_suffix.isdigit():
        _fail('COLLECTOR_ACCOUNT_SUFFIX_INVALID')
    if type(timeout_seconds) not in (int, float) or timeout_seconds != timeout_seconds:
        _fail('COLLECTOR_TIMEOUT_INVALID')
    if not 0 < float(timeout_seconds) <= MAX_CAPTURE_SECONDS:
        _fail('COLLECTOR_TIMEOUT_INVALID')
    clock = clock or (lambda: datetime.now(timezone.utc))
    monotonic = monotonic or time.monotonic
    if deadline_monotonic is not None:
        if type(deadline_monotonic) not in (int, float) \
                or not math.isfinite(deadline_monotonic):
            _fail('COLLECTOR_DEADLINE_INVALID')
    output = _prepare_output(output_dir)
    state = _State(output, account_suffix, clock, monotonic,
                   float(timeout_seconds))
    try:
        now_mono = state.mono()
        capture_deadline = now_mono + state.timeout_seconds
        if deadline_monotonic is not None:
            capture_deadline = min(capture_deadline, deadline_monotonic)
        if capture_deadline <= now_mono:
            _fail('COLLECTOR_DEADLINE_EXPIRED')
        arm = getattr(transport, 'arm_deadline', None)
        if not callable(arm):
            _fail('COLLECTOR_TRANSPORT_DEADLINE_UNSUPPORTED')
        try:
            actual_deadline = arm(capture_deadline)
        except TransportError as exc:
            _fail('COLLECTOR_TRANSPORT_ERROR', str(exc))
        except (TypeError, ValueError):
            _fail('COLLECTOR_DEADLINE_INVALID')
        if type(actual_deadline) not in (int, float) \
                or not math.isfinite(actual_deadline) \
                or actual_deadline > capture_deadline \
                or actual_deadline <= now_mono:
            _fail('COLLECTOR_DEADLINE_INVALID')
        state.deadline = actual_deadline
        state.started_at = state.wall()
        state.started_mono = state.mono()
        _capture(state, transport)
        state.check_budget()
        state.completed_at = state.wall()
        manifest = _manifest(state)
        _immutable_write(output / 'manifest.json',
                         (canonical(manifest) + '\n').encode())
        try:
            normalized = normalize_bundle(
                load_saved_bundle(output), now=state.wall())
        except (BundleError, CollectorError) as exc:
            code = exc.code if isinstance(exc, CollectorError) else exc.code
            detail = exc.detail if hasattr(exc, 'detail') else None
            return _write_failure(state, output, code, detail)
        state.check_budget()
        blockers = list(normalized.get('blockers') or [])
        status = 'CAPTURED_CONDITIONS_MISSING' if blockers else 'CAPTURED'
        provenance = _provenance(
            state, status=status, blockers=blockers,
            raw_chain_sha256=normalized.get('raw_chain_sha256'))
        try:
            _immutable_write(output / 'capture-provenance.json',
                             (canonical(provenance) + '\n').encode())
        except Exception:
            return _write_failure(
                state, output, 'COLLECTOR_PROVENANCE_WRITE_FAILED', None)
        return {
            'schema_version': 1, 'kind': 'saved_bundle_capture',
            'status': status, 'blockers': blockers, 'blocker_detail': None,
            'output_dir': str(output),
            'account_key': state.account_key, 'account_mask': state.account_mask,
            'account_suffix': account_suffix,
            'calls': state.calls, 'pages': len(state.pages),
            'raw_bytes': state.raw_bytes,
            'raw_chain_sha256': normalized.get('raw_chain_sha256'),
            'coverage': normalized.get('coverage'),
            'missing_conditions': list(state.missing_conditions),
            'candidate_snapshot_present': isinstance(
                normalized.get('candidate_snapshot'), dict),
            'baseline_snapshot': None,
            'ready': False, 'conditions_missing': bool(blockers),
            'production_ready': False, 'live_enabled': False,
            'execution_authorized': False,
        }
    except CollectorError as exc:
        return _write_failure(state, output, exc.code, exc.detail)
    except TransportError as exc:
        return _write_failure(state, output, 'COLLECTOR_TRANSPORT_ERROR',
                              str(exc))
    except BundleError as exc:
        return _write_failure(state, output, exc.code, exc.detail)
    except (OSError, ValueError, TypeError, KeyError):
        return _write_failure(
            state, output, 'COLLECTOR_INVALID_OR_WRITE_FAILURE', None)


def add_parser(subparsers):
    parser = subparsers.add_parser(
        'capture-baseline-bundle',
        help='Bounded official read-only capture into a private saved bundle; never trades or approves')
    parser.add_argument('--output-dir', required=True)
    parser.add_argument('--account-suffix', required=True)
    parser.add_argument('--timeout-seconds', type=float, default=30)
    return parser


def run_cli(args, *, transport_factory=None):
    factory = transport_factory or CodexBrokerReadTransport
    try:
        deadline_monotonic = time.monotonic() + float(args.timeout_seconds)
        with factory(timeout=args.timeout_seconds,
                      deadline_monotonic=deadline_monotonic) as transport:
            result = collect_saved_bundle(
                args.output_dir, account_suffix=args.account_suffix,
                transport=transport, timeout_seconds=args.timeout_seconds,
                deadline_monotonic=deadline_monotonic)
    except (CollectorError, TransportError, ValueError, OSError) as exc:
        code = exc.code if isinstance(exc, CollectorError) else (
            str(exc) if isinstance(exc, TransportError)
            else 'COLLECTOR_INPUT_INVALID')
        result = {
            'schema_version': 1, 'kind': 'saved_bundle_capture',
            'status': 'FAILED', 'blockers': [code], 'blocker_detail': None,
            'baseline_snapshot': None, 'ready': False,
            'conditions_missing': False, 'production_ready': False,
            'live_enabled': False, 'execution_authorized': False,
        }
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))
    if result.get('status') == 'FAILED':
        return 4
    if result.get('conditions_missing'):
        return 3
    return 0
