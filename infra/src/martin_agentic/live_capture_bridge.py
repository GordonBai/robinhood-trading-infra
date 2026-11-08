"""Same-observation saved-bundle bridge for the limited live read path.

One read operation runs the existing bounded saved-bundle collector, derives a
supported-coverage broker snapshot from that same bundle, and leaves SQLite
journal/risk/execution work to the owner thread.  It never starts a second
parallel collector and never turns unsupported scopes or missing settlement
into empty/complete facts.
"""
from decimal import Decimal, InvalidOperation
import json
import os
from pathlib import Path
import sqlite3
import shutil
import time
import uuid

from .baseline_evidence import BundleError, load_saved_bundle, normalize_bundle
from .broker_bundle import CollectorError, collect_saved_bundle
from .broker import normalize_approval, normalize_positions
from .capture_publisher import CapturePublisher, _private_dir
from .current_evidence import capture_generation
from .qualification import source_fingerprint
from .transport import TransportError


STAGING_MARKER = 'bridge-staging.json'
STAGING_META_DIR = 'staging-meta'
STAGING_ACTIVE_GRACE_SECONDS = 30
STAGING_PENDING_MAX_SECONDS = 300
STAGING_SCAN_MAX_ENTRIES = 128
STAGING_SCAN_MAX_SECONDS = 1.0
STAGING_MAX_BYTES = 64 * 1024 * 1024
STAGING_MAX_OWNER_PENDING = 8
STAGING_OWNER_DIR = 'staging-owners'
RECOVERY_BLOCK_FILE = 'recovery-block.json'
_OWNER_LEASES = {}


SUPPORTED_TOOLS = (
    'get_equity_positions', 'get_equity_orders', 'get_advanced_orders',
    'get_option_positions', 'get_option_orders', 'get_crypto_positions',
    'get_crypto_orders',
)
UNSUPPORTED_SCOPES = ('futures', 'event_contracts', 'fixed_income',
                      'mutual_funds', 'settlements')
TERMINAL = frozenset({'filled','cancelled','canceled','rejected','failed',
                      'voided','expired','partially_filled_rest_cancelled',
                      'locate_failed'})
JOURNAL_ASSOCIATION_KEYS = (
    'provider', 'synthetic', 'account_key', 'account_mask', 'observed_at',
    'collection_complete', 'positions', 'orders', 'groups',
    'other_asset_open_orders', 'other_asset_values', 'portfolio',
    'buying_power', 'unleveraged_buying_power', 'human_must_approve_trades',
    'pending_approvals', 'pending_approval_ids', 'approval_states',
    'requested_approval_ids', 'missing_approval_ids',
)


def journal_association_projection(payload):
    """Stable semantic view of the actual owner-journal current payload.

    Bookkeeping added by journal ingest (execution counts, reconciliation
    issues, baseline flag) is intentionally excluded so a retry of the same
    observation remains idempotent without weakening the semantic comparison.
    """
    if not isinstance(payload, dict):
        _fail('LIVE_CAPTURE_JOURNAL_PAYLOAD_INVALID')
    return {key: payload.get(key) for key in JOURNAL_ASSOCIATION_KEYS}



class LiveCaptureBridgeError(ValueError):
    def __init__(self, code, detail=None):
        self.code = code
        self.detail = detail
        super().__init__(code)

    def __str__(self):
        return self.code if self.detail is None else f'{self.code}:{self.detail}'


def _fail(code, detail=None):
    raise LiveCaptureBridgeError(code, detail)


def bridge_config(policy):
    config = policy.get('current_capture') if isinstance(policy, dict) else None
    if not isinstance(config, dict):
        _fail('LIVE_CAPTURE_BRIDGE_NOT_CONFIGURED')
    root = config.get('root')
    public_manifest = config.get('public_manifest')
    if not isinstance(root, str) or not root:
        _fail('LIVE_CAPTURE_ROOT_MISSING')
    if not isinstance(public_manifest, str) or not public_manifest:
        _fail('LIVE_CAPTURE_PUBLIC_MANIFEST_MISSING')
    return {
        'root': Path(root),
        'public_manifest': Path(public_manifest),
        'producer': str(config.get('producer') or 'live-saved-bundle-v1'),
        'retention': int(config.get('retention', 3)),
    }


def _open_orders(records):
    return [order for order in (records or [])
            if str(order.get('state')).lower() not in TERMINAL]


def _parse_bundle_approvals(loaded, account_number):
    by_pass = {}
    for pass_number in (1, 2):
        records = {}
        pages = [page for page in loaded['pages']
                 if page['tool'] == 'get_trade_approvals'
                 and page['pass'] == pass_number]
        if not pages:
            _fail('LIVE_CAPTURE_APPROVALS_PASS_MISSING')
        for page in pages:
            data = page.get('data')
            if not isinstance(data, dict) or not isinstance(
                    data.get('approvals'), list):
                _fail('LIVE_CAPTURE_APPROVALS_INVALID')
            for raw in data['approvals']:
                try:
                    item = normalize_approval(raw, account_number)
                except ValueError:
                    _fail('LIVE_CAPTURE_APPROVALS_INVALID')
                if (item['approval_id'] in records
                        and records[item['approval_id']] != item):
                    _fail('LIVE_CAPTURE_APPROVALS_CONFLICT')
                records[item['approval_id']] = item
        by_pass[pass_number] = records
    if by_pass[1] != by_pass[2]:
        _fail('LIVE_CAPTURE_APPROVALS_TWO_PASS_MISMATCH')
    return by_pass[1]


def _money(value, code):
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        _fail(code)
    if not result.is_finite():
        _fail(code)
    return str(result)


def _saved_buying_power(loaded):
    pages = [page for page in loaded['pages']
             if page['tool'] == 'get_portfolio' and page['pass'] == 1]
    if len(pages) != 1:
        _fail('LIVE_CAPTURE_PORTFOLIO_PAGE_MISSING')
    data = pages[0].get('data')
    buying = data.get('buying_power') if isinstance(data, dict) else None
    if (not isinstance(buying, dict)
            or buying.get('display_currency') != 'USD'
            or 'buying_power' not in buying
            or 'unleveraged_buying_power' not in buying):
        _fail('LIVE_CAPTURE_BUYING_POWER_MISSING')
    return (_money(buying['buying_power'], 'LIVE_CAPTURE_BUYING_POWER_INVALID'),
            _money(buying['unleveraged_buying_power'],
                   'LIVE_CAPTURE_BUYING_POWER_INVALID'))



def _staging_marker_path(bundle_root):
    bundle = Path(bundle_root)
    return bundle.parent.parent / STAGING_META_DIR / (bundle.name + '.json')


def _staging_marker(bundle_root):
    path = _staging_marker_path(bundle_root)
    try:
        info = os.stat(path, follow_symlinks=False)
        if not os.path.isfile(path) or os.path.islink(path) \
                or info.st_size > 64 * 1024:
            return None
        value = json.loads(Path(path).read_text())
    except (OSError, TypeError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def _write_staging_marker(bundle_root, state, *, generation=None,
                          observed_at=None, account_key=None, reason=None,
                          source_fingerprint_value=None, owner_id=None):
    path = _staging_marker_path(bundle_root)
    _private_dir(path.parent)
    value = {
        'schema_version': 1, 'state': state,
        'generation': generation, 'observed_at': observed_at,
        'account_key': account_key, 'reason': reason,
        'source_fingerprint': source_fingerprint_value,
        'owner_id': owner_id,
        'updated_at': time.time(),
    }
    temp = path.with_name(path.name + '.tmp')
    try:
        with temp.open('x') as handle:
            os.chmod(temp, 0o600)
            handle.write(json.dumps(value, sort_keys=True) + '\n')
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, path)
    except OSError:
        try:
            temp.unlink()
        except OSError:
            pass
        raise


def _recovery_block_path(root):
    return Path(root) / 'staging-meta' / RECOVERY_BLOCK_FILE


def _recovery_blocked(root):
    path = _recovery_block_path(root)
    try:
        info = os.stat(path, follow_symlinks=False)
        return os.path.isfile(path) and not os.path.islink(path) \
            and info.st_size <= 4096
    except OSError:
        return False


def _set_recovery_block(root, reason=None):
    path = _recovery_block_path(root)
    if reason is None:
        try:
            path.unlink()
        except OSError:
            pass
        return
    _private_dir(path.parent)
    temp = path.with_name(path.name + '.tmp')
    try:
        with temp.open('w') as handle:
            os.chmod(temp, 0o600)
            handle.write(json.dumps({'reason': str(reason)[:256]}) + '\n')
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, path)
    except OSError:
        try:
            temp.unlink()
        except OSError:
            pass


def _owner_lock_path(root, owner_id):
    return Path(root) / STAGING_OWNER_DIR / (owner_id + '.lock')


def acquire_staging_owner(root):
    """Acquire one process/root lease; token is a random non-PID identity."""
    root = _private_dir(Path(root))
    key = str(root.resolve())
    current = _OWNER_LEASES.get(key)
    if current is not None:
        return current[0]
    _private_dir(root / STAGING_OWNER_DIR)
    for _ in range(32):
        owner_id = uuid.uuid4().hex
        path = _owner_lock_path(root, owner_id)
        try:
            descriptor = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
        except OSError:
            continue
        try:
            import fcntl
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (BlockingIOError, OSError):
            os.close(descriptor)
            continue
        _OWNER_LEASES[key] = (owner_id, descriptor)
        return owner_id
    _fail('LIVE_CAPTURE_STAGING_OWNER_UNAVAILABLE')


def release_staging_owner(root):
    key = str(Path(root).resolve())
    current = _OWNER_LEASES.pop(key, None)
    if current is None:
        return False
    _, descriptor = current
    try:
        import fcntl
        fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)
    return True


def _owner_alive(root, owner_id):
    if not owner_id:
        return False
    key = str(Path(root).resolve())
    current = _OWNER_LEASES.get(key)
    if current is not None and current[0] == owner_id:
        return True
    path = _owner_lock_path(root, owner_id)
    try:
        descriptor = os.open(path, os.O_RDWR)
    except OSError:
        return False
    try:
        import fcntl
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        return False
    finally:
        os.close(descriptor)


def _remove_staged_bundle(path):
    try:
        shutil.rmtree(path)
    finally:
        try:
            _staging_marker_path(path).unlink()
        except OSError:
            pass


def _bundle_bytes(root, *, deadline=None):
    """Bounded size estimate; stop at the shared deadline/byte cap."""
    total = 0
    started = time.monotonic()
    deadline = deadline or (started + STAGING_SCAN_MAX_SECONDS)
    try:
        stack = [Path(root)]
        while stack and total <= STAGING_MAX_BYTES:
            if time.monotonic() > deadline:
                return STAGING_MAX_BYTES + 1
            current = stack.pop()
            try:
                iterator = os.scandir(current)
            except OSError:
                return STAGING_MAX_BYTES + 1
            with iterator:
                for entry in iterator:
                    if time.monotonic() > deadline:
                        return STAGING_MAX_BYTES + 1
                    if entry.is_symlink():
                        continue
                    if entry.is_dir(follow_symlinks=False):
                        stack.append(Path(entry.path))
                    elif entry.is_file(follow_symlinks=False):
                        try:
                            total += entry.stat(follow_symlinks=False).st_size
                        except OSError:
                            return STAGING_MAX_BYTES + 1
    except OSError:
        return STAGING_MAX_BYTES + 1
    return total


def _owner_pending_count(staging):
    count = 0
    started = time.monotonic()
    try:
        with os.scandir(staging) as iterator:
            for entry in iterator:
                if count >= STAGING_MAX_OWNER_PENDING:
                    return count
                if time.monotonic() - started > STAGING_SCAN_MAX_SECONDS:
                    return STAGING_MAX_OWNER_PENDING
                if entry.is_symlink() or not entry.is_dir(follow_symlinks=False) \
                        or not entry.name.startswith('bridge-'):
                    continue
                marker = _staging_marker(Path(entry.path))
                if (marker or {}).get('state') in {'OWNER_PENDING', 'COMMIT_PENDING'}:
                    count += 1
    except OSError:
        return STAGING_MAX_OWNER_PENDING
    return count


def prune_staging(root, *, retention=3, protected=()):
    """Bounded cleanup with forward progress and owner-pending protection.

    Active collection dirs and all owner-pending dirs are pinned. A scan cap
    truncates the current batch but still processes/deletes the entries found;
    subsequent calls continue from the remaining bounded set. Pending capacity
    is enforced by read(), so pruning never substitutes a latest-N policy for
    ownership.
    """
    staging = _private_dir(Path(root) / 'staging')
    protected = {str(Path(item).resolve()) for item in protected if item}
    limit = max(1, int(retention))
    started = time.monotonic()
    deadline = started + STAGING_SCAN_MAX_SECONDS
    entries = []
    truncated = False
    try:
        with os.scandir(staging) as iterator:
            for entry in iterator:
                if len(entries) >= STAGING_SCAN_MAX_ENTRIES \
                        or time.monotonic() > deadline:
                    truncated = True
                    break
                if entry.is_symlink() or not entry.is_dir(follow_symlinks=False) \
                        or not entry.name.startswith('bridge-'):
                    continue
                path = Path(entry.path)
                resolved = str(path.resolve())
                marker = _staging_marker(path)
                age = time.time() - entry.stat(follow_symlinks=False).st_mtime
                state = (marker or {}).get('state')
                if resolved in protected:
                    entries.append((entry.stat(follow_symlinks=False).st_mtime_ns,
                                    path, state, True, age))
                    continue
                if state is None and age <= STAGING_ACTIVE_GRACE_SECONDS:
                    entries.append((entry.stat(follow_symlinks=False).st_mtime_ns,
                                    path, 'ACTIVE', False, age))
                    continue
                if state in {'OWNER_PENDING', 'COMMIT_PENDING'}:
                    owner_id = (marker or {}).get('owner_id')
                    if _recovery_blocked(root) or _owner_alive(root, owner_id):
                        state = 'LIVE_PENDING'
                    elif age <= STAGING_PENDING_MAX_SECONDS:
                        # Legacy/unclaimed handoff gets a conservative grace.
                        pass
                    else:
                        try:
                            _write_staging_marker(
                                path, 'ABANDONED',
                                generation=(marker or {}).get('generation'),
                                observed_at=(marker or {}).get('observed_at'),
                                account_key=(marker or {}).get('account_key'),
                                reason='OWNER_NOT_ALIVE_OR_LEGACY_UNCLAIMED',
                                owner_id=None)
                            state = 'ABANDONED'
                        except OSError:
                            # Keep evidence if the state transition cannot be
                            # persisted; it remains bounded by the scan cap.
                            pass
                    entries.append((entry.stat(follow_symlinks=False).st_mtime_ns,
                                    path, state, False, age))
                    continue
                entries.append((entry.stat(follow_symlinks=False).st_mtime_ns,
                                path, state or 'ORPHAN', False, age))
    except OSError:
        return {'status': 'pending', 'reason': 'STAGING_SCAN_FAILED',
                'deleted': [], 'kept': 0, 'truncated': False}
    entries.sort(reverse=True, key=lambda item: item[0])
    deleted = []
    kept = 0
    total_bytes = 0
    for _, path, state, pinned, age in entries:
        size = _bundle_bytes(path, deadline=deadline)
        if pinned or state in {'ACTIVE', 'LIVE_PENDING', 'OWNER_PENDING',
                                'COMMIT_PENDING'}:
            kept += 1
            total_bytes += size
            continue
        removable = state in {'REJECTED', 'FAILED', 'PUBLISHED', 'ORPHAN',
                              'COMMIT_PENDING', 'ABANDONED'}
        if not removable:
            kept += 1
            total_bytes += size
            continue
        if kept < limit and total_bytes + size <= STAGING_MAX_BYTES:
            kept += 1
            total_bytes += size
            continue
        try:
            _remove_staged_bundle(path)
            deleted.append(path.name)
        except OSError:
            return {'status': 'pending', 'reason': 'DELETE_FAILED',
                    'deleted': sorted(deleted), 'kept': kept,
                    'truncated': truncated}
    return {'status': 'pending' if truncated else 'complete',
            'reason': 'SCAN_LIMIT' if truncated else None,
            'deleted': sorted(deleted), 'kept': kept,
            'bytes': total_bytes, 'truncated': truncated}

def _blocker_is_scope_unknown_or_management(blocker):
    """True only for blockers that are explicitly unknown/non-fatal facts.

    Supported identity/schema/completeness errors are deliberately not in
    this allowlist; they must fail before journal ingest.
    """
    if blocker in {'SETTLEMENT_SOURCE_MISSING', 'APPROVALS_UNVERIFIED',
                   'CRYPTO_RHS_MISSING'}:
        return True
    code = blocker.split(':', 1)[0]
    return code in {
        'GETTER_MISSING', 'CONDITIONS_MISSING', 'SCOPE_UNAVAILABLE',
        'NONFLAT_POSITION', 'OPEN_ORDER', 'AGGREGATE_NONZERO',
    } or blocker == 'SCHEMA_UNVERIFIED:get_trade_approvals' \
        or blocker.startswith('BUNDLE_SCHEMA_UNVERIFIED:get_trade_approvals')


def _supported_fact_reasons(normalized):
    return [blocker for blocker in (normalized.get('blockers') or [])
            if not _blocker_is_scope_unknown_or_management(blocker)]


def _supported_snapshot(loaded, normalized, account, approval_ids=None):
    coverage = normalized.get('coverage') or {}
    for tool in SUPPORTED_TOOLS:
        if (coverage.get(tool) or {}).get('status') != 'complete':
            _fail('LIVE_CAPTURE_SUPPORTED_COVERAGE_INCOMPLETE', tool)
    fact_reasons = _supported_fact_reasons(normalized)
    if fact_reasons:
        # Identity/schema/two-pass/completeness errors are not scope-unknown
        # diagnostics. Reject before the owner journal can ingest a truncated
        # "first record wins" view.
        _fail('LIVE_CAPTURE_SUPPORTED_FACT_INVALID',
              ','.join(fact_reasons))
    observations = normalized.get('observations') or {}
    equity_positions = observations.get('equity_positions')
    equity_orders = observations.get('equity_orders')
    advanced = observations.get('advanced')
    option_positions = observations.get('option_positions')
    option_orders = observations.get('option_orders')
    crypto_positions = observations.get('crypto_positions')
    crypto_orders = observations.get('crypto_orders')
    portfolio = observations.get('portfolio')
    setting = observations.get('approval_setting')
    if (equity_positions is None or equity_orders is None or advanced is None
            or option_positions is None or option_orders is None
            or crypto_positions is None or crypto_orders is None
            or not isinstance(portfolio, dict) or not isinstance(setting, dict)):
        _fail('LIVE_CAPTURE_SUPPORTED_COVERAGE_INCOMPLETE')
    # Unsupported asset exposure is reported unknown, not deleted or zeroed.
    asset_values = {key: str(portfolio[key]) for key in (
        'options_value', 'crypto_value', 'futures_value',
        'event_contracts_value', 'fixed_income_value',
        'mutual_funds_value')}
    other_orders = _open_orders(option_orders) + _open_orders(crypto_orders)
    other_positions = {
        'options': option_positions,
        'crypto': crypto_positions,
    }
    approvals = _parse_bundle_approvals(loaded, account['account_number'])
    pending = sorted(approval_id for approval_id, item in approvals.items()
                     if item.get('state') == 'APPROVAL_STATE_PENDING')
    requested = sorted(set(approval_ids or ()) | set(approvals))
    missing = [identity for identity in requested if identity not in approvals]
    positions = normalize_positions([{
        'symbol': record['symbol'],
        'quantity': record['quantity'],
        'shares_available_for_sells': record['available_for_sells'],
        'shares_held_for_sells': record['held_for_sells'],
        'average_buy_price': record.get('average_price'),
    } for record in equity_positions])
    orders = {}
    for order in list(equity_orders) + list(advanced.get('orders') or []):
        order_id = order.get('id')
        if not order_id:
            _fail('LIVE_CAPTURE_ORDER_IDENTITY')
        if order_id in orders and orders[order_id] != order:
            _fail('LIVE_CAPTURE_ORDER_CONFLICT', order_id)
        orders[order_id] = order
    buying_power, unleveraged = _saved_buying_power(loaded)
    try:
        snapshot = {
            'schema_version': 1, 'provider': 'robinhood', 'synthetic': False,
            'account_key': account['account_key'],
            'account_mask': account['account_mask'],
            'started_at': normalized['started_at'],
            'observed_at': normalized['observed_at'],
            'collection_complete': True,
            'supported_coverage_complete': True,
            'full_baseline_complete': False,
            'settlement_ledger_complete': False,
            'account_flat_proven': False,
            'unsupported_scopes': list(UNSUPPORTED_SCOPES),
            'coverage': coverage,
            'positions': positions, 'orders': orders,
            'groups': list(advanced.get('groups') or []),
            'other_asset_open_orders': len(other_orders),
            'other_asset_values': asset_values,
            'other_asset_positions': other_positions,
            'portfolio': {key: str(portfolio[key]) for key in
                          ('cash', 'total_value', 'pending_deposits')},
            'buying_power': buying_power,
            'unleveraged_buying_power': unleveraged,
            'human_must_approve_trades': setting.get(
                'human_must_approve_trades'),
            'pending_approvals': len(pending),
            'pending_approval_ids': pending,
            'approval_states': approvals,
            'requested_approval_ids': requested,
            'missing_approval_ids': missing,
        }
    except (KeyError, TypeError, ValueError) as exc:
        _fail('LIVE_CAPTURE_SNAPSHOT_INVALID', str(exc))
    return snapshot


class SavedBundleObservationBridge:
    """One read = one saved bundle + one supported-coverage snapshot."""

    def __init__(self, policy, clock, config=None):
        self.policy = policy
        self.clock = clock
        self.config = config or bridge_config(policy)

    def _root(self):
        return _private_dir(Path(self.config['root']))

    def _mark(self, output, state, *, generation=None, observed_at=None,
              account_key=None, reason=None, owner_id=None):
        if not output or not Path(output).is_dir():
            return False
        existing = _staging_marker(output) or {}
        owner_id = owner_id or existing.get('owner_id')
        try:
            _write_staging_marker(
                output, state, generation=generation,
                observed_at=observed_at, account_key=account_key,
                reason=reason, source_fingerprint_value=None,
                owner_id=owner_id)
            return True
        except OSError:
            return False

    def _prune(self, *, protected=()):
        try:
            return prune_staging(
                self._root(), retention=self.config['retention'],
                protected=protected)
        except (OSError, ValueError, TypeError):
            return {'status': 'pending', 'reason': 'PRUNE_FAILED',
                    'deleted': [], 'kept': None}

    def _snapshot_from_bundle(self, output, *, approval_ids=None):
        loaded = load_saved_bundle(output)
        normalized = normalize_bundle(loaded, now=self.clock())
        account = normalized.get('account') or {}
        if not account.get('account_number'):
            _fail('LIVE_CAPTURE_ACCOUNT_MISSING')
        snapshot = _supported_snapshot(
            loaded, normalized, account, approval_ids=approval_ids)
        generation = capture_generation(output)['generation']
        snapshot['live_capture'] = {
            'bundle_root': str(output),
            'generation': generation,
            'public_manifest': str(self.config['public_manifest']),
            'producer': self.config['producer'],
            'supported_coverage_only': True,
            'full_baseline_complete': False,
            'settlement_ledger_complete': False,
            'unknown_scopes': list(UNSUPPORTED_SCOPES),
            'owned_staging': True,
            'staging_state': 'OWNER_PENDING',
        }
        return snapshot

    def read(self, transport, *, deadline_monotonic=None, approval_ids=None,
             timeout_seconds=10):
        root = self._root()
        staging = _private_dir(root / 'staging')
        owner_id = acquire_staging_owner(root)
        if _recovery_blocked(root):
            raise LiveCaptureBridgeError('LIVE_CAPTURE_JOURNAL_RECOVERY_BLOCKED')
        self._prune()
        if _owner_pending_count(staging) >= STAGING_MAX_OWNER_PENDING:
            raise LiveCaptureBridgeError('LIVE_CAPTURE_STAGING_CAPACITY')
        output = staging / ('bridge-' + uuid.uuid4().hex)
        try:
            result = collect_saved_bundle(
                output, account_suffix=self.policy['account_suffix'],
                transport=transport, timeout_seconds=timeout_seconds,
                clock=self.clock, deadline_monotonic=deadline_monotonic)
            if result.get('status') == 'FAILED':
                reason = ','.join(result.get('blockers') or ['UNKNOWN'])
                self._mark(output, 'FAILED', reason=reason, owner_id=owner_id)
                self._prune(protected=(output,))
                if ('COLLECTOR_TRANSPORT_ERROR' in (result.get('blockers') or [])
                        and isinstance(result.get('blocker_detail'), str)):
                    # Preserve the transport classification so the shared
                    # 10-second retry budget can make its normal decision.
                    raise TransportError(result['blocker_detail'])
                _fail('LIVE_CAPTURE_COLLECTOR_FAILED', reason)
            snapshot = self._snapshot_from_bundle(
                output, approval_ids=approval_ids)
            metadata = snapshot['live_capture']
            if not self._mark(
                    output, 'OWNER_PENDING',
                    generation=metadata['generation'],
                    observed_at=snapshot['observed_at'],
                    account_key=snapshot['account_key'], owner_id=owner_id):
                _fail('LIVE_CAPTURE_STAGING_PIN_FAILED')
            self._prune(protected=(output,))
            return snapshot
        except TransportError:
            raise
        except (BundleError, CollectorError, LiveCaptureBridgeError,
                OSError, ValueError, TypeError, KeyError) as exc:
            # Leave bounded failure evidence for the owner; never fall back to
            # a parallel live collector.
            code = getattr(exc, 'code', None) or str(exc)
            self._mark(output, 'REJECTED', reason=code, owner_id=owner_id)
            self._prune(protected=(output,))
            raise LiveCaptureBridgeError(code, str(exc)) from None

    def publish(self, snapshot, *, policy=None, book=None, journal_snapshot=None):
        """Owner-thread publication of the bundle that produced this snapshot."""
        metadata = snapshot.get('live_capture') if isinstance(snapshot, dict) else None
        if not isinstance(metadata, dict):
            return {'status': 'NOT_A_BRIDGE_OBSERVATION'}
        output = metadata.get('bundle_root')
        config = self.config if policy is None else bridge_config(policy)
        try:
            publisher = CapturePublisher(
                config['root'], config['public_manifest'],
                policy=self.policy if policy is None else policy,
                account_key=snapshot['account_key'],
                account_suffix=self.policy['account_suffix'],
                source_fingerprint_value=source_fingerprint(), clock=self.clock,
                retention=config['retention'])
            association_snapshot = (journal_association_projection(snapshot)
                                    if journal_snapshot is None
                                    else journal_snapshot)
            result = publisher.publish_bundle(
                output, producer=config['producer'],
                journal_snapshot=association_snapshot)
        except OSError as exc:
            # Generation may already be renamed but the pointer/cleanup write
            # failed. Keep the bounded bundle recoverable; recover() will
            # re-open the actual durable journal binding and use the publisher's
            # existing orphan/idempotent path. A permanent problem is rejected
            # by the publisher on the retry.
            code = getattr(exc, 'code', None) or str(exc)
            if output:
                self._mark(output, 'COMMIT_PENDING', reason=code)
                self._prune(protected=(output,))
            raise
        except (ValueError, KeyError, TypeError, RuntimeError) as exc:
            code = getattr(exc, 'code', None) or str(exc)
            if output:
                self._mark(output, 'REJECTED', reason=code)
                self._prune(protected=(output,))
            raise
        status = result.get('status')
        if status in {'PUBLISHED', 'RECOVERED', 'ALREADY_PUBLISHED'}:
            if output:
                self._mark(output, 'PUBLISHED', reason=None)
            try:
                if metadata.get('owned_staging') and output:
                    _remove_staged_bundle(output)
            except OSError:
                pass
        else:
            if output:
                self._mark(output, 'REJECTED',
                           reason=result.get('error') or result.get('reason'))
                self._prune(protected=(output,))
        if book is not None and getattr(book, '_current_context_refresher', None) is not None:
            try:
                result['current_context'] = book._current_context_refresher.refresh()
            except (ValueError, KeyError, TypeError, OSError, RuntimeError) as exc:
                result['current_context'] = {
                    'status': 'FAILED',
                    'reason': getattr(exc, 'code', None) or str(exc)}
                result['current_evidence_valid'] = False
        return result

    def recover(self, journal, *, book=None):
        """Owner startup recovery from the durable journal row, no network."""
        try:
            payload = journal.current_observation()
        except (OSError, ValueError, RuntimeError, sqlite3.Error) as exc:
            _set_recovery_block(self.config['root'],
                                getattr(exc, 'code', None) or str(exc))
            self._prune()
            return {'status': 'JOURNAL_UNREADABLE',
                    'reason': getattr(exc, 'code', None) or str(exc),
                    'entry_allowed': False,
                    'journal_recovery_blocked': True}
        _set_recovery_block(self.config['root'], None)
        if not isinstance(payload, dict):
            self._prune()
            return {'status': 'NO_CURRENT_JOURNAL_OBSERVATION',
                    'entry_allowed': False}
        metadata = payload.get('live_capture')
        if not isinstance(metadata, dict) or not metadata.get('bundle_root'):
            self._prune()
            return {'status': 'NO_SAVED_STAGING_OBSERVATION',
                    'entry_allowed': False}
        root = Path(self.config['root']).resolve()
        output = Path(metadata['bundle_root'])
        try:
            resolved = output.resolve()
        except OSError:
            self._prune()
            return {'status': 'STAGING_PATH_INVALID', 'entry_allowed': False}
        if root not in resolved.parents or output.name.startswith('capture-'):
            self._prune()
            return {'status': 'STAGING_PATH_NOT_OWNED', 'entry_allowed': False}
        if not output.is_dir() or output.is_symlink():
            self._prune()
            return {'status': 'STAGING_MISSING', 'entry_allowed': False}
        try:
            marker = _staging_marker(output)
            if marker is not None and marker.get('state') in {
                    'REJECTED', 'PUBLISHED', 'FAILED'}:
                self._prune()
                return {'status': 'STAGING_NOT_RECOVERABLE',
                        'state': marker.get('state'), 'entry_allowed': False}
            snapshot = self._snapshot_from_bundle(output)
            if snapshot['observed_at'] != payload.get('observed_at') \
                    or snapshot['account_key'] != payload.get('account_key'):
                raise ValueError('LIVE_CAPTURE_RECOVERY_BINDING_MISMATCH')
            if snapshot['live_capture']['generation'] != metadata.get('generation'):
                raise ValueError('LIVE_CAPTURE_RECOVERY_GENERATION_MISMATCH')
            result = self.publish(
                snapshot, book=book,
                journal_snapshot=journal_association_projection(payload))
            result['recovered_from_local_staging'] = True
            result['network_read_performed'] = False
            result['entry_allowed'] = False
            return result
        except (BundleError, CollectorError, LiveCaptureBridgeError,
                ValueError, KeyError, TypeError, OSError) as exc:
            code = getattr(exc, 'code', None) or str(exc)
            self._mark(output, 'REJECTED', reason=code)
            self._prune(protected=(output,))
            return {'status': 'REJECTED', 'error': code,
                    'recovered_from_local_staging': False,
                    'network_read_performed': False,
                    'entry_allowed': False}
