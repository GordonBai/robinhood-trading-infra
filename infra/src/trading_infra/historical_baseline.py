"""Immutable historical baseline facts for already-terminated account history.

This module is intentionally separate from live execution identity, attribution
and instance accounting.  A baseline classifies a complete, pinned, historical
account fact set as ``LEGACY_BASELINE``; it never creates a strategy owner,
trade, fill, budget reservation, PnL row or live order progression.

The module has no CLI and never opens a broker, market, MCP or Host.  Plan and
validate are read-only; apply writes only the baseline namespace in a caller
owned SQLite connection.
"""
from datetime import datetime, timezone
import hashlib
import json
import sqlite3
from decimal import Decimal, InvalidOperation

from trading_contracts.values import canonical, digest, number, timestamp
from .transactions import atomic

SCHEMA_VERSION = 1
PROVIDER = 'robinhood'
BASELINE_SCOPE_FULL = 'full_account_baseline_v1'
BASELINE_SCOPE_IDENTITY = 'terminal_equity_order_identity_v1'
IDENTITY_CLAIMS = {
    'order_identity_only': True,
    'full_account_flat': False,
    'other_asset_complete': False,
    'settlement_ledger_complete': False,
}
TERMINAL_ORDER_STATES = frozenset({'filled', 'cancelled', 'canceled',
                                    'rejected', 'expired'})
TERMINAL_GROUP_STATES = TERMINAL_ORDER_STATES
MAX_SNAPSHOT_AGE_SECONDS = 30
PIN_RE = __import__('re').compile(r'^[0-9a-f]{64}$')
SCHEMA_TABLES = frozenset({
    'historical_baseline_schema', 'historical_baseline_manifest',
    'historical_baseline_order', 'historical_baseline_execution',
    'historical_baseline_balance_component', 'historical_baseline_event'})
SCHEMA_COLUMNS = {
    'historical_baseline_schema': ('version',),
    'historical_baseline_manifest': (
        'baseline_id', 'sequence', 'status', 'manifest_sha256', 'account_key',
        'account_mask', 'provider', 'policy_sha256', 'source_fingerprint',
        'source_execution_db_sha256', 'risk_sidecar_sha256',
        'host_manifest_sha256', 'lineage_head_sha256', 'cutoff_utc',
        'snapshot_observed_at', 'snapshot_sha256', 'expires_at',
        'manifest_json', 'applied_at', 'approval_scope'),
    'historical_baseline_order': (
        'baseline_id', 'order_key', 'order_id', 'ref_id', 'account_key',
        'provider', 'canonical_order_sha256', 'executions_sha256',
        'order_json'),
    'historical_baseline_execution': (
        'baseline_id', 'execution_key', 'order_key', 'execution_id',
        'order_id', 'ref_id', 'account_key', 'provider',
        'canonical_execution_sha256', 'execution_json'),
    'historical_baseline_balance_component': (
        'baseline_id', 'component', 'amount', 'source'),
    'historical_baseline_event': (
        'event_id', 'baseline_id', 'kind', 'occurred_at', 'detail'),
}
MANIFEST_COLUMNS = SCHEMA_COLUMNS['historical_baseline_manifest']
CROSS_ASSET_SCOPES = ('options', 'crypto', 'futures', 'event_contracts',
                      'fixed_income', 'mutual_funds')
CROSS_ASSET_COLLECTIONS = (
    ('positions', 'other_asset_positions'),
    ('orders', 'other_asset_orders'),
    ('pending_settlements', 'other_asset_pending_settlements'),
)
IDENTITY_MIGRATION_VERSION = 1
CONDITIONS_MISSING = 'CONDITIONS_MISSING'
SNAPSHOT_TIME_SOURCE = 'collector_completed_at_utc'
EXECUTION_TIME_SOURCE = 'broker_execution_timestamp_utc'


class BaselineError(ValueError):
    def __init__(self, code, detail=None):
        self.code = code
        self.detail = detail
        super().__init__(code)

    def __str__(self):
        return self.code if self.detail is None else f'{self.code}:{self.detail}'


def _fail(code, detail=None):
    raise BaselineError(code, detail)


def _now(now, clock):
    """Injectable wall clock; an explicit caller time wins over the clock."""
    if now is not None:
        value = now
    elif clock is not None:
        value = clock()
    else:
        value = datetime.now(timezone.utc)
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        _fail('BASELINE_CLOCK_INVALID')
    return value.astimezone(timezone.utc)


def _text(value, code):
    if not isinstance(value, str) or not value.strip():
        _fail(code)
    return value


def _sha(value, code):
    if not isinstance(value, str) or not PIN_RE.fullmatch(value):
        _fail(code)
    return value


def _utc(value, code):
    try:
        return timestamp(value)
    except (TypeError, ValueError, OverflowError):
        _fail(code)


def _decimal(value, code, *, positive=False):
    try:
        result = number(value)
    except (TypeError, ValueError):
        _fail(code)
    if not isinstance(result, Decimal) or not result.is_finite() or result < 0 or (positive and result == 0):
        _fail(code)
    return result


def _order_key(provider, account_key, order_id):
    return digest(['baseline-order-1', provider, account_key, order_id])


def _execution_key(provider, account_key, execution_id):
    return digest(['baseline-execution-1', provider, account_key, execution_id])


def _executions_digest(executions):
    return digest([canonical(execution) for execution in executions])


def _table_exists(db, name):
    try:
        return db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (name,)).fetchone() is not None
    except sqlite3.Error:
        _fail('BASELINE_SCHEMA_READ_FAILED')


def _table_names(db):
    try:
        return {row[0] for row in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
    except sqlite3.Error:
        _fail('BASELINE_SCHEMA_READ_FAILED')


def _schema_state(db):
    """Return ABSENT/PRESENT or fail closed for any partial/corrupt schema."""
    names = _table_names(db)
    found = names & SCHEMA_TABLES
    if not found:
        return {'state': 'ABSENT'}
    if not SCHEMA_TABLES <= names:
        _fail('BASELINE_SCHEMA_PARTIAL')
    for table, expected in SCHEMA_COLUMNS.items():
        try:
            actual = tuple(row[1] for row in db.execute(
                f'PRAGMA table_info({table})'))
        except sqlite3.Error:
            _fail('BASELINE_SCHEMA_READ_FAILED')
        if actual != expected:
            _fail('BASELINE_SCHEMA_COLUMNS', table)
    try:
        versions = db.execute(
            'SELECT version FROM historical_baseline_schema').fetchall()
    except sqlite3.Error:
        _fail('BASELINE_SCHEMA_READ_FAILED')
    if len(versions) != 1:
        _fail('BASELINE_SCHEMA_VERSION_INVALID')
    if versions[0][0] != SCHEMA_VERSION:
        _fail('BASELINE_SCHEMA_VERSION_UNSUPPORTED')
    try:
        rows = db.execute(
            'SELECT * FROM historical_baseline_manifest ORDER BY sequence').fetchall()
    except sqlite3.Error:
        _fail('BASELINE_SCHEMA_READ_FAILED')
    active_count = sum(1 for row in rows if row[MANIFEST_COLUMNS.index('status')] == 'APPLIED')
    if active_count > 1:
        _fail('BASELINE_MULTIPLE_ACTIVE')
    sequences = [row[MANIFEST_COLUMNS.index('sequence')] for row in rows]
    if any(not isinstance(sequence, int) or sequence <= 0 for sequence in sequences) \
            or len(set(sequences)) != len(sequences):
        _fail('BASELINE_INDEX_CORRUPT', 'sequence')
    active_orders, active_refs, active_executions = {}, {}, {}
    for row in rows:
        values = dict(zip(MANIFEST_COLUMNS, row))
        baseline_id = values['baseline_id']
        status = values['status']
        manifest_sha256 = values['manifest_sha256']
        encoded = values['manifest_json']
        if status not in ('APPLIED', 'REVOKED'):
            _fail('BASELINE_INDEX_CORRUPT', 'status')
        try:
            parsed = json.loads(encoded)
        except (TypeError, ValueError):
            _fail('BASELINE_INDEX_CORRUPT', 'manifest_json')
        if not isinstance(parsed, dict) \
                or parsed.get('kind') != 'historical_baseline_manifest' \
                or parsed.get('schema_version') != SCHEMA_VERSION \
                or parsed.get('baseline_id') != baseline_id \
                or parsed.get('manifest_sha256') != manifest_sha256 \
                or parsed.get('manifest_sha256') != digest(
                    {key: value for key, value in parsed.items()
                     if key != 'manifest_sha256'}) \
                or canonical(parsed) != encoded:
            _fail('BASELINE_INDEX_CORRUPT', 'manifest')
        try:
            _manifest_rows_checked(parsed)
            _manifest_ids_checked(parsed)
            _materialized_checked(db, parsed, values)
        except BaselineError as exc:
            _fail('BASELINE_INDEX_CORRUPT', exc.code)
        if values['status'] == 'APPLIED':
            try:
                for order in parsed['orders']:
                    order_key = order['order_key']
                    active_orders[(parsed['account_key'], parsed['provider'],
                                   order['order_id'])] = {
                        'baseline_id': baseline_id, 'order_key': order_key,
                        'canonical_order_sha256': order['canonical_order_sha256'],
                        'executions_sha256': order['executions_sha256']}
                    if order.get('ref_id') is not None:
                        active_refs[(parsed['account_key'], parsed['provider'],
                                     order['ref_id'])] = {
                            'baseline_id': baseline_id, 'order_key': order_key}
                for execution in parsed['executions']:
                    active_executions[(parsed['account_key'], parsed['provider'],
                                       execution['execution_id'])] = {
                        'baseline_id': baseline_id,
                        'order_key': execution['order_key']}
            except (KeyError, TypeError):
                _fail('BASELINE_INDEX_CORRUPT', 'active_index')
    baseline_ids = {row[MANIFEST_COLUMNS.index('baseline_id')] for row in rows}
    try:
        _materialization_orphans_checked(db, baseline_ids)
    except BaselineError as exc:
        _fail('BASELINE_INDEX_CORRUPT', exc.code)
    return {'state': 'PRESENT', 'active_count': active_count,
            'active_orders': active_orders, 'active_refs': active_refs,
            'active_executions': active_executions}


_SCHEMA = (
    '''CREATE TABLE IF NOT EXISTS historical_baseline_schema(
        version INTEGER PRIMARY KEY)''',
    '''CREATE TABLE IF NOT EXISTS historical_baseline_manifest(
        baseline_id TEXT PRIMARY KEY, sequence INTEGER UNIQUE NOT NULL,
        status TEXT NOT NULL CHECK(status IN
            ('PLANNED','VALIDATED','APPLIED','SUPERSEDED','REVOKED','EXPIRED','REJECTED')),
        manifest_sha256 TEXT NOT NULL, account_key TEXT NOT NULL,
        account_mask TEXT NOT NULL, provider TEXT NOT NULL,
        policy_sha256 TEXT NOT NULL, source_fingerprint TEXT NOT NULL,
        source_execution_db_sha256 TEXT NOT NULL, risk_sidecar_sha256 TEXT NOT NULL,
        host_manifest_sha256 TEXT NOT NULL, lineage_head_sha256 TEXT NOT NULL,
        cutoff_utc TEXT NOT NULL, snapshot_observed_at TEXT NOT NULL,
        snapshot_sha256 TEXT NOT NULL, expires_at TEXT NOT NULL,
        manifest_json TEXT NOT NULL, applied_at TEXT, approval_scope TEXT)''',
    '''CREATE TABLE IF NOT EXISTS historical_baseline_order(
        baseline_id TEXT NOT NULL, order_key TEXT NOT NULL, order_id TEXT NOT NULL,
        ref_id TEXT, account_key TEXT NOT NULL, provider TEXT NOT NULL,
        canonical_order_sha256 TEXT NOT NULL, executions_sha256 TEXT NOT NULL,
        order_json TEXT NOT NULL, PRIMARY KEY(baseline_id,order_key))''',
    '''CREATE TABLE IF NOT EXISTS historical_baseline_execution(
        baseline_id TEXT NOT NULL, execution_key TEXT NOT NULL,
        order_key TEXT NOT NULL, execution_id TEXT NOT NULL, order_id TEXT NOT NULL,
        ref_id TEXT, account_key TEXT NOT NULL, provider TEXT NOT NULL,
        canonical_execution_sha256 TEXT NOT NULL, execution_json TEXT NOT NULL,
        PRIMARY KEY(baseline_id,execution_key))''',
    '''CREATE TABLE IF NOT EXISTS historical_baseline_balance_component(
        baseline_id TEXT NOT NULL, component TEXT NOT NULL, amount TEXT NOT NULL,
        source TEXT NOT NULL, PRIMARY KEY(baseline_id,component))''',
    '''CREATE TABLE IF NOT EXISTS historical_baseline_event(
        event_id TEXT PRIMARY KEY, baseline_id TEXT NOT NULL, kind TEXT NOT NULL,
        occurred_at TEXT NOT NULL, detail TEXT NOT NULL)''',
)


def _ensure_schema(db):
    """Create the baseline schema explicitly, never via executescript."""
    state = _schema_state(db)
    if state['state'] == 'PRESENT':
        return
    for statement in _SCHEMA:
        db.execute(statement)
    db.execute('INSERT INTO historical_baseline_schema(version) VALUES(?)',
               (SCHEMA_VERSION,))


def _active_manifests(db):
    if not _table_exists(db, 'historical_baseline_manifest'):
        return []
    return [dict(zip(('baseline_id', 'status', 'manifest_json'), row))
            for row in db.execute(
                'SELECT baseline_id,status,manifest_json FROM historical_baseline_manifest '
                "WHERE status='APPLIED' ORDER BY sequence")]


def _flat_trade_checked(key, trade):
    if not isinstance(key, str) or not key or not isinstance(trade, dict):
        _fail('BASELINE_GLOBAL_TRADE_INVALID')
    if trade.get('id') != key or not isinstance(trade.get('id'), str):
        _fail('BASELINE_GLOBAL_TRADE_INVALID')
    if trade.get('phase') != 'FLAT_VERIFIED':
        _fail('BASELINE_GLOBAL_TRADE_ACTIVE')
    if 'quantity' not in trade:
        _fail('CONDITIONS_MISSING', 'BASELINE_GLOBAL_TRADE_QUANTITY_MISSING')
    if _decimal(trade['quantity'], 'BASELINE_GLOBAL_TRADE_INVALID') != 0:
        _fail('BASELINE_GLOBAL_TRADE_ACTIVE')


def _global_unresolved(db):
    """Fail closed on any live/UNKNOWN/pending state outside the baseline."""
    names = _table_names(db)

    def any_row(sql, params=()):
        try:
            return db.execute(sql, params).fetchone() is not None
        except sqlite3.Error:
            _fail('BASELINE_GLOBAL_STATE_UNREADABLE')

    checks = (
        ('live_actions',
         "SELECT 1 FROM live_actions WHERE state IS NULL OR state NOT IN "
         "('EXPIRED','REJECTED','SUPERSEDED','OBSERVED') LIMIT 1",
         'BASELINE_GLOBAL_ACTION_UNRESOLVED'),
        ('execution_intents',
         "SELECT 1 FROM execution_intents WHERE state IS NULL OR state NOT IN "
         "('SETTLED','EXPIRED') LIMIT 1",
         'BASELINE_GLOBAL_EXECUTION_PENDING'),
        ('receipt_pending_executions',
         'SELECT 1 FROM receipt_pending_executions LIMIT 1',
         'BASELINE_GLOBAL_EXECUTION_PENDING'),
        ('external_operations',
         'SELECT 1 FROM external_operations WHERE report IS NULL LIMIT 1',
         'BASELINE_GLOBAL_OPERATION_PENDING'),
        ('native_operation_records',
         "SELECT 1 FROM native_operation_records WHERE state IS NULL "
         "OR state!='RECORDED' LIMIT 1",
         'BASELINE_GLOBAL_OPERATION_PENDING'),
        ('symbol_leases',
         'SELECT 1 FROM symbol_leases LIMIT 1',
         'BASELINE_GLOBAL_LEASE_ACTIVE'),
        ('receipt_order_progress',
         'SELECT 1 FROM receipt_order_progress WHERE terminal IS NULL '
         'OR terminal!=1 LIMIT 1',
         'BASELINE_GLOBAL_TRADE_ACTIVE'),
        ('execution_identity_conflict',
         'SELECT 1 FROM execution_identity_conflict LIMIT 1',
         'BASELINE_GLOBAL_IDENTITY_CONFLICT'),
        ('receipt_identity_quarantine',
         'SELECT 1 FROM receipt_identity_quarantine LIMIT 1',
         'BASELINE_GLOBAL_IDENTITY_QUARANTINE'),
        ('execution_integrity_faults',
         'SELECT 1 FROM execution_integrity_faults LIMIT 1',
         'BASELINE_GLOBAL_INTEGRITY_FAULT'),
    )
    for table, sql, code in checks:
        if table in names and any_row(sql):
            _fail(code)
    if 'receipt_identity_migration' in names:
        try:
            rows = db.execute(
                'SELECT version FROM receipt_identity_migration').fetchall()
        except sqlite3.Error:
            _fail('BASELINE_GLOBAL_STATE_UNREADABLE')
        if len(rows) != 1 or rows[0][0] != IDENTITY_MIGRATION_VERSION:
            _fail('BASELINE_GLOBAL_IDENTITY_MIGRATION')
    if 'live_state' in names:
        try:
            rows = db.execute('SELECT payload FROM live_state WHERE id=1').fetchall()
        except sqlite3.Error:
            _fail('BASELINE_GLOBAL_STATE_UNREADABLE')
        if len(rows) != 1:
            _fail('BASELINE_GLOBAL_STATE_UNREADABLE')
        try:
            state = json.loads(rows[0][0])
        except (TypeError, ValueError):
            _fail('BASELINE_GLOBAL_STATE_UNREADABLE')
        trades = state.get('trades') if isinstance(state, dict) else None
        if not isinstance(trades, dict):
            _fail('BASELINE_GLOBAL_STATE_UNREADABLE')
        issues = state.get('issues', []) if isinstance(state, dict) else None
        if not isinstance(issues, list):
            _fail('BASELINE_GLOBAL_STATE_UNREADABLE')
        # Historical issue text is retained for audit after a position is
        # verified flat.  It is not itself a pending action/order/lease fact;
        # the live action/order/approval checks below remain authoritative.
        for key, trade in trades.items():
            _flat_trade_checked(key, trade)
        broker_orders = state.get('broker_orders')
        if broker_orders is not None:
            if not isinstance(broker_orders, dict):
                _fail('BASELINE_GLOBAL_STATE_UNREADABLE')
            for key, broker_order in broker_orders.items():
                if not isinstance(key, str) or not isinstance(broker_order, dict) \
                        or str(broker_order.get('state') or '').lower() not in TERMINAL_ORDER_STATES:
                    _fail('BASELINE_GLOBAL_ORDER_ACTIVE')
        approvals = state.get('broker_approvals')
        if approvals is not None:
            if not isinstance(approvals, dict):
                _fail('BASELINE_GLOBAL_STATE_UNREADABLE')
            count = approvals.get('count')
            ids = approvals.get('ids')
            missing = approvals.get('missing')
            if not isinstance(count, int) or isinstance(count, bool) or count < 0 \
                    or not isinstance(ids, list) or not isinstance(missing, list) \
                    or count != len(ids) or ids or missing:
                _fail('BASELINE_GLOBAL_APPROVAL_PENDING')


def _live_overlap(db, account_key, provider, order_id, ref_id, execution_ids):
    """Reject any live namespace already owning this historical identity."""
    checks = (
        ('execution_attribution', 'ref_id', ref_id),
        ('receipt_order_aliases', 'order_id', order_id),
        ('receipt_order_progress', 'order_id', order_id),
    )
    for table, column, value in checks:
        if value is not None and _table_exists(db, table):
            if db.execute(
                    f'SELECT 1 FROM {table} WHERE {column}=? LIMIT 1',
                    (value,)).fetchone():
                _fail('BASELINE_LIVE_OWNER_CONFLICT')
    if _table_exists(db, 'execution_identity'):
        for execution_id in execution_ids:
            key = f'{provider}|{account_key}|{execution_id}'
            if db.execute(
                    'SELECT 1 FROM execution_identity WHERE identity_key=? LIMIT 1',
                    (key,)).fetchone():
                _fail('BASELINE_LIVE_OWNER_CONFLICT')
    if _table_exists(db, 'instance_fills'):
        for execution_id in execution_ids:
            if db.execute(
                    'SELECT 1 FROM instance_fills WHERE fill_id=? LIMIT 1',
                    (execution_id,)).fetchone():
                _fail('BASELINE_LIVE_OWNER_CONFLICT')
    if _table_exists(db, 'execution_fill_identity'):
        for execution_id in execution_ids:
            key = f'{provider}|{account_key}|{execution_id}'
            if db.execute(
                    'SELECT 1 FROM execution_fill_identity WHERE execution_key=? LIMIT 1',
                    (key,)).fetchone():
                _fail('BASELINE_LIVE_OWNER_CONFLICT')


def _validate_evidence(evidence, *, account_key, provider, policy_sha256,
                       source_fingerprint):
    if not isinstance(evidence, dict):
        _fail('BASELINE_EVIDENCE_REQUIRED')
    source = evidence.get('source')
    risk = evidence.get('risk')
    host = evidence.get('host')
    lineage = evidence.get('lineage')
    if not all(isinstance(item, dict) for item in (source, risk, host, lineage)):
        _fail('BASELINE_EVIDENCE_INCOMPLETE')
    required = {
        'source': ('execution_db_sha256', 'account_key', 'policy_sha256', 'source_fingerprint'),
        'risk': ('sidecar_sha256', 'account_key', 'terms_sha256'),
        'host': ('manifest_sha256', 'account_key', 'generation', 'epoch'),
        'lineage': ('head_sha256', 'account_key', 'policy_sha256', 'source_fingerprint'),
    }
    for name, keys in required.items():
        value = evidence[name]
        for key in keys:
            if key not in value or value[key] in (None, ''):
                _fail('BASELINE_EVIDENCE_MISSING:' + name + '.' + key)
    for key in ('execution_db_sha256', 'sidecar_sha256', 'manifest_sha256', 'head_sha256'):
        _sha(evidence['source' if key == 'execution_db_sha256' else
                    'risk' if key == 'sidecar_sha256' else
                    'host' if key == 'manifest_sha256' else 'lineage'][key],
             'BASELINE_EVIDENCE_HASH_INVALID')
    for name in ('source', 'risk', 'host', 'lineage'):
        if evidence[name]['account_key'] != account_key:
            _fail('BASELINE_EVIDENCE_ACCOUNT_MISMATCH')
    if source['policy_sha256'] != policy_sha256 or lineage['policy_sha256'] != policy_sha256:
        _fail('BASELINE_EVIDENCE_POLICY_MISMATCH')
    if source['source_fingerprint'] != source_fingerprint or lineage['source_fingerprint'] != source_fingerprint:
        _fail('BASELINE_EVIDENCE_SOURCE_MISMATCH')
    if risk.get('simulation') is True:
        _fail('BASELINE_EVIDENCE_SIMULATION')


def _cross_asset_checked(snapshot, observed):
    scopes = snapshot['provenance']['asset_scopes']
    if not isinstance(scopes, dict) or set(scopes) != set(CROSS_ASSET_SCOPES):
        _fail('CONDITIONS_MISSING', 'BASELINE_CROSS_ASSET_SCOPES_INCOMPLETE')
    for collection, key in CROSS_ASSET_COLLECTIONS:
        values = snapshot.get(key)
        if not isinstance(values, dict) or set(values) != set(CROSS_ASSET_SCOPES):
            _fail('CONDITIONS_MISSING', 'BASELINE_CROSS_ASSET_' + key)
        for asset in CROSS_ASSET_SCOPES:
            records = values[asset]
            if not isinstance(records, list):
                _fail('CONDITIONS_MISSING', 'BASELINE_CROSS_ASSET_' + key + ':' + asset)
            evidence = scopes[asset].get(collection) if isinstance(scopes[asset], dict) else None
            if not isinstance(evidence, dict) \
                    or set(evidence) != {'pagination', 'record_sha256', 'record_count'}:
                _fail('CONDITIONS_MISSING', 'BASELINE_CROSS_ASSET_' + key + ':' + asset)
            pagination = evidence['pagination']
            if not isinstance(pagination, dict) \
                    or set(pagination) != {'complete', 'pages', 'source', 'observed_at'}:
                _fail('CONDITIONS_MISSING', 'BASELINE_CROSS_ASSET_PAGINATION:' + asset)
            if pagination['complete'] is not True \
                    or not isinstance(pagination['pages'], int) \
                    or isinstance(pagination['pages'], bool) or pagination['pages'] < 1 \
                    or not isinstance(pagination['source'], str) or not pagination['source']:
                _fail('CONDITIONS_MISSING', 'BASELINE_CROSS_ASSET_PAGINATION:' + asset)
            started = _utc(pagination['observed_at'],
                           'BASELINE_CROSS_ASSET_TIME_INVALID')
            if started > observed or (observed - started).total_seconds() > MAX_SNAPSHOT_AGE_SECONDS:
                _fail('BASELINE_CROSS_ASSET_TIME_INVALID', asset)
            if evidence['record_count'] != len(records) \
                    or not isinstance(evidence['record_count'], int) \
                    or isinstance(evidence['record_count'], bool) \
                    or evidence['record_count'] < 0:
                _fail('BASELINE_CROSS_ASSET_RECORD_COUNT', asset)
            _sha(evidence['record_sha256'], 'BASELINE_CROSS_ASSET_DIGEST_INVALID')
            if evidence['record_sha256'] != digest(records):
                _fail('BASELINE_CROSS_ASSET_DIGEST_INVALID', asset)
            if collection == 'positions':
                for record in records:
                    if not isinstance(record, dict) or 'quantity' not in record:
                        _fail('CONDITIONS_MISSING',
                              'BASELINE_CROSS_ASSET_POSITION_INVALID:' + asset)
                    quantity = _decimal(record['quantity'],
                                        'BASELINE_CROSS_ASSET_POSITION_INVALID')
                    if quantity != 0:
                        _fail('BASELINE_CROSS_ASSET_POSITION_OPEN', asset)
                    held_key = ('shares_held_for_sells' if 'shares_held_for_sells' in record
                                else 'held_for_sells' if 'held_for_sells' in record
                                else 'held' if 'held' in record else None)
                    if held_key is None:
                        _fail('CONDITIONS_MISSING',
                              'BASELINE_CROSS_ASSET_HELD_MISSING:' + asset)
                    if _decimal(record[held_key],
                                'BASELINE_CROSS_ASSET_POSITION_INVALID') != 0:
                        _fail('BASELINE_CROSS_ASSET_POSITION_OPEN', asset)
                    for value_key in ('market_value', 'value', 'value_usd'):
                        if value_key in record:
                            value = _decimal(record[value_key],
                                             'BASELINE_CROSS_ASSET_VALUE_INVALID')
                            if value != 0:
                                _fail('BASELINE_CROSS_ASSET_POSITION_OPEN', asset)
            elif collection == 'orders':
                for record in records:
                    if not isinstance(record, dict) or 'state' not in record:
                        _fail('CONDITIONS_MISSING', 'BASELINE_CROSS_ASSET_ORDER_INVALID:' + asset)
                    if str(record['state']).lower() not in TERMINAL_ORDER_STATES:
                        _fail('BASELINE_CROSS_ASSET_ORDER_NOT_TERMINAL', asset)
            elif records:
                _fail('BASELINE_CROSS_ASSET_SETTLEMENT_PENDING', asset)


def _validate_snapshot(snapshot, *, account_key, provider, cutoff, observed_at, now):
    if not isinstance(snapshot, dict):
        _fail('CONDITIONS_MISSING', 'BASELINE_SNAPSHOT_REQUIRED')
    required = ('provider', 'synthetic', 'account_key', 'account_mask',
                'observed_at', 'collection_complete', 'positions', 'orders',
                'groups', 'other_asset_open_orders', 'other_asset_values',
                'other_asset_positions', 'other_asset_orders',
                'other_asset_pending_settlements', 'portfolio',
                'pending_approvals', 'pending_approval_ids', 'provenance')
    missing = [key for key in required if key not in snapshot]
    if missing:
        _fail('CONDITIONS_MISSING', 'BASELINE_SNAPSHOT_MISSING:' + ','.join(missing))
    allowed_other = {'other_asset_open_orders', 'other_asset_values',
                     'other_asset_positions', 'other_asset_orders',
                     'other_asset_pending_settlements'}
    if any(key.startswith('other_asset_') and key not in allowed_other
           for key in snapshot):
        _fail('BASELINE_CROSS_ASSET_UNKNOWN_FIELD')
    if snapshot['provider'] != provider or snapshot['synthetic'] is not False:
        _fail('BASELINE_SNAPSHOT_PROVIDER_INVALID')
    if snapshot['account_key'] != account_key or not isinstance(snapshot['account_mask'], str):
        _fail('BASELINE_SNAPSHOT_ACCOUNT_INVALID')
    if snapshot['collection_complete'] is not True:
        _fail('CONDITIONS_MISSING', 'BASELINE_SNAPSHOT_INCOMPLETE')
    observed = _utc(snapshot['observed_at'], 'BASELINE_SNAPSHOT_TIME_INVALID')
    if observed != observed_at or observed > now:
        _fail('BASELINE_SNAPSHOT_TIME_INVALID')
    if observed < cutoff or (now - observed).total_seconds() > MAX_SNAPSHOT_AGE_SECONDS:
        _fail('BASELINE_SNAPSHOT_STALE_OR_CUTOFF')
    provenance = snapshot['provenance']
    required_provenance = {'collection_source', 'positions_complete',
                           'positions_pagination_complete', 'orders_complete',
                           'executions_complete', 'approvals_complete',
                           'asset_scopes', 'snapshot_time_source',
                           'execution_time_source'}
    if not isinstance(provenance, dict) or set(provenance) != required_provenance:
        _fail('CONDITIONS_MISSING', 'BASELINE_PROVENANCE_INCOMPLETE')
    for key in ('positions_complete', 'positions_pagination_complete',
                'orders_complete', 'executions_complete', 'approvals_complete'):
        if provenance[key] is not True:
            _fail('CONDITIONS_MISSING', 'BASELINE_PROVENANCE_NOT_COMPLETE:' + key)
    if provenance['snapshot_time_source'] != SNAPSHOT_TIME_SOURCE:
        _fail('BASELINE_TIME_PROVENANCE_INVALID', 'snapshot')
    if provenance['execution_time_source'] != EXECUTION_TIME_SOURCE:
        _fail('BASELINE_TIME_PROVENANCE_INVALID', 'execution')
    if not isinstance(provenance['collection_source'], str) or not provenance['collection_source']:
        _fail('BASELINE_PROVENANCE_INVALID', 'collection_source')
    scopes = provenance['asset_scopes']
    if not isinstance(scopes, dict) or set(scopes) != set(CROSS_ASSET_SCOPES):
        _fail('CONDITIONS_MISSING', 'BASELINE_CROSS_ASSET_INCOMPLETE')
    _cross_asset_checked(snapshot, observed)
    positions = snapshot['positions']
    if not isinstance(positions, dict):
        _fail('BASELINE_POSITIONS_INVALID')
    for symbol, position in positions.items():
        if not isinstance(symbol, str) or not isinstance(position, dict):
            _fail('BASELINE_POSITIONS_INVALID')
        if 'quantity' not in position:
            _fail('CONDITIONS_MISSING', 'BASELINE_POSITION_QUANTITY_MISSING')
        held_key = ('shares_held_for_sells' if 'shares_held_for_sells' in position
                    else 'held_for_sells' if 'held_for_sells' in position else None)
        if held_key is None:
            _fail('CONDITIONS_MISSING', 'BASELINE_POSITION_HELD_MISSING')
        if _decimal(position['quantity'], 'BASELINE_POSITION_INVALID') != 0:
            _fail('BASELINE_POSITION_NOT_FLAT')
        if _decimal(position[held_key], 'BASELINE_POSITION_INVALID') != 0:
            _fail('BASELINE_POSITION_HELD')
    orders = snapshot['orders']
    if not isinstance(orders, dict):
        _fail('BASELINE_ORDERS_INVALID')
    other_values = snapshot['other_asset_values']
    required_other = {'options_value', 'crypto_value', 'futures_value',
                      'event_contracts_value', 'fixed_income_value',
                      'mutual_funds_value'}
    if not isinstance(other_values, dict) or set(other_values) != required_other:
        _fail('BASELINE_CROSS_ASSET_INCOMPLETE')
    if 'open_equity_orders' in snapshot and _decimal(
            snapshot['open_equity_orders'], 'BASELINE_OPEN_ORDER_INVALID') != 0:
        _fail('BASELINE_OPEN_EQUITY_ORDER')
    for value in other_values.values():
        if _decimal(value, 'BASELINE_CROSS_ASSET_INVALID') != 0:
            _fail('BASELINE_CROSS_ASSET_NOT_FLAT')
    if _decimal(snapshot.get('other_asset_open_orders', 0), 'BASELINE_OTHER_ORDER_INVALID') != 0:
        _fail('BASELINE_OTHER_ORDER_OPEN')
    groups = snapshot['groups']
    if not isinstance(groups, list):
        _fail('BASELINE_GROUPS_INVALID')
    for group in groups:
        if not isinstance(group, dict) or str(group.get('state') or '').lower() not in TERMINAL_GROUP_STATES:
            _fail('BASELINE_GROUP_NOT_TERMINAL')
    portfolio = snapshot['portfolio']
    if not isinstance(portfolio, dict) or any(
            key not in portfolio for key in ('cash', 'total_value', 'pending_deposits')):
        _fail('CONDITIONS_MISSING', 'BASELINE_PORTFOLIO_INCOMPLETE')
    _decimal(portfolio['cash'], 'BASELINE_PORTFOLIO_INVALID')
    _decimal(portfolio['total_value'], 'BASELINE_PORTFOLIO_INVALID', positive=True)
    if _decimal(portfolio['pending_deposits'], 'BASELINE_PORTFOLIO_INVALID') != 0:
        _fail('BASELINE_PENDING_DEPOSIT')
    if snapshot['pending_approvals'] != 0 or snapshot['pending_approval_ids'] != []:
        _fail('BASELINE_APPROVALS_OPEN')
    if snapshot.get('missing_approval_ids') not in (None, []):
        _fail('BASELINE_APPROVAL_IDS_MISSING')
    if any(str(order.get('state') or '').lower() not in TERMINAL_ORDER_STATES
           for order in orders.values() if isinstance(order, dict)):
        _fail('BASELINE_ORDER_NOT_TERMINAL')
    return orders


def _validate_order_identity_snapshot(snapshot, *, account_key, provider,
                                      cutoff, observed_at, now,
                                      enforce_freshness=True):
    if not isinstance(snapshot, dict):
        _fail('CONDITIONS_MISSING', 'BASELINE_IDENTITY_SNAPSHOT_REQUIRED')
    if snapshot.get('schema_version') != 1 \
            or snapshot.get('kind') != 'terminal_equity_order_identity' \
            or snapshot.get('baseline_scope') != BASELINE_SCOPE_IDENTITY:
        _fail('BASELINE_IDENTITY_SCOPE_INVALID')
    if snapshot.get('provider') != provider \
            or snapshot.get('account_key') != account_key \
            or not isinstance(snapshot.get('account_mask'), str):
        _fail('BASELINE_IDENTITY_ACCOUNT_INVALID')
    claims = snapshot.get('claims')
    if claims != IDENTITY_CLAIMS:
        _fail('BASELINE_IDENTITY_CLAIMS_INVALID')
    observed = _utc(snapshot.get('observed_at'),
                    'BASELINE_IDENTITY_TIME_INVALID')
    if observed != observed_at or observed > now or observed < cutoff:
        _fail('BASELINE_IDENTITY_TIME_INVALID')
    window = snapshot.get('collection_window_seconds')
    if not isinstance(window, (int, float)) or isinstance(window, bool) \
            or window < 0 or window > MAX_SNAPSHOT_AGE_SECONDS:
        _fail('BASELINE_IDENTITY_WINDOW_INVALID')
    if enforce_freshness and (now - observed).total_seconds() > MAX_SNAPSHOT_AGE_SECONDS:
        _fail('BASELINE_IDENTITY_STALE')
    source = snapshot.get('source')
    if not isinstance(source, dict) or set(source) != {
            'bundle_manifest_sha256', 'raw_chain_sha256', 'tool', 'passes',
            'query_scope'}:
        _fail('BASELINE_IDENTITY_SOURCE_INVALID')
    _sha(source['bundle_manifest_sha256'], 'BASELINE_IDENTITY_SOURCE_INVALID')
    _sha(source['raw_chain_sha256'], 'BASELINE_IDENTITY_SOURCE_INVALID')
    if source['tool'] != 'get_equity_orders' or source['passes'] != [1, 2] \
            or source['query_scope'] != 'full':
        _fail('BASELINE_IDENTITY_SOURCE_INVALID')
    orders = snapshot.get('orders')
    executions = snapshot.get('executions')
    if not isinstance(orders, list) or not orders or not isinstance(executions, list):
        _fail('BASELINE_IDENTITY_ORDERS_INVALID')
    flattened = []
    for order in orders:
        if not isinstance(order, dict):
            _fail('BASELINE_IDENTITY_ORDER_INVALID')
        flattened.extend(order.get('executions') or [])
    if canonical(flattened) != canonical(executions):
        _fail('BASELINE_IDENTITY_EXECUTIONS_INVALID')
    # _normalize_orders is the single exact quantity/execution/cutoff
    # validator; it also enforces terminal states and unique identities.
    _normalize_orders({order.get('id'): order for order in orders
                       if isinstance(order, dict)},
                      account_key=account_key, provider=provider,
                      cutoff=cutoff, observed_at=observed_at)
    if len(orders) != len({order.get('id') for order in orders
                           if isinstance(order, dict)}):
        _fail('BASELINE_IDENTITY_ORDER_ID_DUPLICATE')
    return orders


def plan_order_identity_baseline(snapshot, *, account_key, provider=PROVIDER,
                                 cutoff_utc, policy_sha256, source_fingerprint,
                                 evidence, expires_at, now=None):
    """Plan a narrow exact terminal-equity-order identity baseline."""
    now = _now(now, None)
    if not isinstance(snapshot, dict):
        _fail('CONDITIONS_MISSING', 'BASELINE_IDENTITY_SNAPSHOT_REQUIRED')
    cutoff = _utc(cutoff_utc, 'BASELINE_CUTOFF_INVALID')
    observed = _utc(snapshot.get('observed_at'),
                    'BASELINE_IDENTITY_TIME_INVALID')
    expiry = _utc(expires_at, 'BASELINE_EXPIRY_INVALID')
    if expiry <= now:
        _fail('BASELINE_PLAN_EXPIRED')
    if expiry < observed:
        _fail('BASELINE_EXPIRY_BEFORE_SNAPSHOT')
    _sha(policy_sha256, 'BASELINE_POLICY_HASH_INVALID')
    _sha(source_fingerprint, 'BASELINE_SOURCE_HASH_INVALID')
    _validate_evidence(evidence, account_key=account_key, provider=provider,
                       policy_sha256=policy_sha256,
                       source_fingerprint=source_fingerprint)
    _validate_order_identity_snapshot(
        snapshot, account_key=account_key, provider=provider, cutoff=cutoff,
        observed_at=observed, now=now, enforce_freshness=False)
    order_rows, execution_rows = _normalize_orders(
        {order['id']: order for order in snapshot['orders']},
        account_key=account_key, provider=provider, cutoff=cutoff,
        observed_at=observed)
    scope_id = digest([
        'historical-baseline-scope-1', account_key, provider,
        policy_sha256, source_fingerprint, cutoff.isoformat(),
        evidence['source']['execution_db_sha256'],
        evidence['risk']['sidecar_sha256'],
        evidence['host']['manifest_sha256'],
        evidence['lineage']['head_sha256'],
        BASELINE_SCOPE_IDENTITY])
    baseline_id = digest([
        'historical-baseline-1', account_key, provider, scope_id,
        cutoff.isoformat(), digest(snapshot)])
    manifest = {
        'schema_version': SCHEMA_VERSION,
        'kind': 'historical_baseline_manifest',
        'baseline_scope': BASELINE_SCOPE_IDENTITY,
        'status': 'PLANNED', 'baseline_id': baseline_id, 'scope_id': scope_id,
        'account_key': account_key,
        'account_mask': snapshot['account_mask'], 'provider': provider,
        'policy_sha256': policy_sha256,
        'source_fingerprint': source_fingerprint,
        'source_execution_db_sha256': evidence['source']['execution_db_sha256'],
        'risk_sidecar_sha256': evidence['risk']['sidecar_sha256'],
        'host_manifest_sha256': evidence['host']['manifest_sha256'],
        'lineage_head_sha256': evidence['lineage']['head_sha256'],
        'cutoff_utc': cutoff.isoformat(),
        'snapshot_observed_at': observed.isoformat(),
        'snapshot_sha256': digest(snapshot),
        'expires_at': expiry.isoformat(),
        'time_provenance': {
            'snapshot_observed_at_source': SNAPSHOT_TIME_SOURCE,
            'execution_time_source': EXECUTION_TIME_SOURCE,
            'collection_source': 'saved_official_tool_response_bundle_v1',
        },
        'created_at': now.isoformat(), 'evidence': evidence,
        'claims': dict(IDENTITY_CLAIMS),
        'opening_balance_components': [],
        'orders': order_rows, 'executions': execution_rows,
        'counts': {'orders': len(order_rows),
                   'executions': len(execution_rows)},
        'production_ready': False, 'live_enabled': False,
    }
    manifest['manifest_sha256'] = digest(
        {key: value for key, value in manifest.items()
         if key != 'manifest_sha256'})
    return manifest


def _normalize_orders(orders, *, account_key, provider, cutoff, observed_at):
    rows, executions, order_ids, refs, execution_ids = [], [], set(), set(), set()
    for key, order in orders.items():
        if not isinstance(order, dict):
            _fail('BASELINE_ORDER_INVALID')
        order_id = order.get('id') or order.get('order_id') or key
        if not isinstance(order_id, str) or not order_id or order_id in order_ids:
            _fail('BASELINE_ORDER_ID_DUPLICATE')
        order_ids.add(order_id)
        ref_id = order.get('ref_id')
        if ref_id is not None and (not isinstance(ref_id, str) or not ref_id):
            _fail('BASELINE_REF_INVALID')
        if ref_id is not None and ref_id in refs:
            _fail('BASELINE_REF_DUPLICATE')
        if ref_id is not None:
            refs.add(ref_id)
        state = str(order.get('state') or '').lower()
        if state not in TERMINAL_ORDER_STATES:
            _fail('BASELINE_ORDER_NOT_TERMINAL')
        qty = _decimal(order.get('quantity'), 'BASELINE_ORDER_QUANTITY_INVALID', positive=True)
        filled = _decimal(order.get('filled', '0'), 'BASELINE_ORDER_FILLED_INVALID')
        created = _utc(order.get('created_at'), 'BASELINE_ORDER_TIME_INVALID')
        if created > cutoff or created > observed_at:
            _fail('BASELINE_ORDER_AFTER_CUTOFF')
        raw_executions = order.get('executions')
        if not isinstance(raw_executions, list):
            _fail('BASELINE_EXECUTIONS_MISSING')
        order_executions = []
        total = Decimal(0)
        for execution in raw_executions:
            if not isinstance(execution, dict):
                _fail('BASELINE_EXECUTION_INVALID')
            eid = execution.get('id')
            if not isinstance(eid, str) or not eid or eid in execution_ids:
                _fail('BASELINE_EXECUTION_ID_DUPLICATE')
            execution_ids.add(eid)
            if execution.get('order_id') != order_id or execution.get('side') != order.get('side'):
                _fail('BASELINE_EXECUTION_ORDER_MISMATCH')
            eq = _decimal(execution.get('quantity'), 'BASELINE_EXECUTION_QUANTITY_INVALID', positive=True)
            price = _decimal(execution.get('price'), 'BASELINE_EXECUTION_PRICE_INVALID', positive=True)
            fees = _decimal(execution.get('fees', '0'), 'BASELINE_EXECUTION_FEES_INVALID')
            when = _utc(execution.get('executed_at') or execution.get('timestamp'),
                        'BASELINE_EXECUTION_TIME_INVALID')
            if when < created or when > cutoff or when > observed_at:
                _fail('BASELINE_EXECUTION_TIME_INVALID')
            total += eq
            row = {'execution_key': _execution_key(provider, account_key, eid),
                   'order_key': _order_key(provider, account_key, order_id),
                   'execution_id': eid, 'order_id': order_id, 'ref_id': ref_id,
                   'canonical_execution_sha256': digest(execution),
                   'execution': execution}
            order_executions.append(row)
            executions.append(row)
        if total != filled:
            _fail('BASELINE_EXECUTION_TOTAL_MISMATCH')
        if state == 'filled' and filled != qty:
            _fail('BASELINE_FILLED_QUANTITY_MISMATCH')
        if filled > qty:
            _fail('BASELINE_FILLED_OVER_QUANTITY')
        order_key = _order_key(provider, account_key, order_id)
        rows.append({'order_key': order_key, 'order_id': order_id, 'ref_id': ref_id,
                     'canonical_order_sha256': digest(order),
                     'executions_sha256': _executions_digest(raw_executions),
                     'order': order,
                     'executions': order_executions})
    return rows, executions


def _opening_components(snapshot, supplied):
    if supplied is not None:
        if not isinstance(supplied, list):
            _fail('BASELINE_OPENING_COMPONENTS_INVALID')
        result = []
        for item in supplied:
            if not isinstance(item, dict) or not isinstance(item.get('component'), str) \
                    or not isinstance(item.get('source'), str):
                _fail('BASELINE_OPENING_COMPONENTS_INVALID')
            result.append({'component': item['component'], 'source': item['source'],
                           'amount': str(_decimal(item.get('amount'), 'BASELINE_OPENING_AMOUNT_INVALID'))})
        return result
    portfolio = snapshot['portfolio']
    return [{'component': 'CASH', 'amount': str(_decimal(portfolio['cash'], 'BASELINE_CASH_INVALID')),
             'source': 'broker_portfolio_cash'},
            {'component': 'EQUITY', 'amount': str(_decimal(portfolio['total_value'], 'BASELINE_EQUITY_INVALID')),
             'source': 'broker_portfolio_total_value'},
            {'component': 'PENDING_DEPOSITS', 'amount': str(
                _decimal(portfolio['pending_deposits'], 'BASELINE_PENDING_INVALID')),
             'source': 'broker_portfolio_pending_deposits'}]


def plan_baseline(snapshot, *, account_key, provider=PROVIDER, cutoff_utc,
                  policy_sha256, source_fingerprint, evidence, expires_at,
                  now=None, opening_balance_components=None):
    """Build a deterministic plan without writing any database."""
    now = _now(now, None)
    if not isinstance(snapshot, dict):
        _fail('CONDITIONS_MISSING', 'BASELINE_SNAPSHOT_REQUIRED')
    cutoff = _utc(cutoff_utc, 'BASELINE_CUTOFF_INVALID')
    observed = _utc(snapshot.get('observed_at'), 'BASELINE_SNAPSHOT_TIME_INVALID')
    expiry = _utc(expires_at, 'BASELINE_EXPIRY_INVALID')
    if expiry <= now:
        _fail('BASELINE_PLAN_EXPIRED')
    if expiry < observed:
        _fail('BASELINE_EXPIRY_BEFORE_SNAPSHOT')
    _sha(policy_sha256, 'BASELINE_POLICY_HASH_INVALID')
    _sha(source_fingerprint, 'BASELINE_SOURCE_HASH_INVALID')
    _validate_evidence(evidence, account_key=account_key, provider=provider,
                       policy_sha256=policy_sha256, source_fingerprint=source_fingerprint)
    orders = _validate_snapshot(snapshot, account_key=account_key, provider=provider,
                                cutoff=cutoff, observed_at=observed, now=now)
    order_rows, execution_rows = _normalize_orders(
        orders, account_key=account_key, provider=provider, cutoff=cutoff,
        observed_at=observed)
    components = _opening_components(snapshot, opening_balance_components)
    scope_id = digest(['historical-baseline-scope-1', account_key, provider,
                       policy_sha256, source_fingerprint, cutoff.isoformat(),
                       evidence['source']['execution_db_sha256'],
                       evidence['risk']['sidecar_sha256'],
                       evidence['host']['manifest_sha256'],
                       evidence['lineage']['head_sha256']])
    baseline_id = digest(['historical-baseline-1', account_key, provider,
                          scope_id, cutoff.isoformat(), digest(snapshot)])
    manifest = {
        'schema_version': SCHEMA_VERSION, 'kind': 'historical_baseline_manifest',
        'status': 'PLANNED', 'baseline_id': baseline_id, 'scope_id': scope_id,
        'account_key': account_key, 'account_mask': snapshot['account_mask'],
        'provider': provider, 'policy_sha256': policy_sha256,
        'source_fingerprint': source_fingerprint,
        'source_execution_db_sha256': evidence['source']['execution_db_sha256'],
        'risk_sidecar_sha256': evidence['risk']['sidecar_sha256'],
        'host_manifest_sha256': evidence['host']['manifest_sha256'],
        'lineage_head_sha256': evidence['lineage']['head_sha256'],
        'cutoff_utc': cutoff.isoformat(), 'snapshot_observed_at': observed.isoformat(),
        'snapshot_sha256': digest(snapshot), 'expires_at': expiry.isoformat(),
        'time_provenance': {
            'snapshot_observed_at_source': snapshot['provenance']['snapshot_time_source'],
            'execution_time_source': snapshot['provenance']['execution_time_source'],
            'collection_source': snapshot['provenance']['collection_source']},
        'created_at': now.isoformat(), 'evidence': evidence,
        'opening_balance_components': components,
        'orders': order_rows, 'executions': execution_rows,
        'counts': {'orders': len(order_rows), 'executions': len(execution_rows)},
        'production_ready': False, 'live_enabled': False,
    }
    manifest['manifest_sha256'] = digest(
        {key: value for key, value in manifest.items() if key != 'manifest_sha256'})
    return manifest


def _manifest_rows_checked(manifest):
    orders = manifest.get('orders')
    executions = manifest.get('executions')
    counts = manifest.get('counts')
    if not isinstance(orders, list) or not isinstance(executions, list) \
            or not isinstance(counts, dict) or counts.get('orders') != len(orders) \
            or counts.get('executions') != len(executions) or not orders:
        _fail('BASELINE_MANIFEST_ROWS_INVALID')
    order_keys, execution_keys = set(), set()
    for order in orders:
        if not isinstance(order, dict) or not all(
                isinstance(order.get(key), str) and order.get(key)
                for key in ('order_key', 'order_id', 'canonical_order_sha256',
                            'executions_sha256')) or not isinstance(order.get('order'), dict):
            _fail('BASELINE_MANIFEST_ORDER_INVALID')
        if order['order_key'] in order_keys or order['canonical_order_sha256'] != digest(order['order']):
            _fail('BASELINE_MANIFEST_ORDER_INVALID')
        if order.get('executions_sha256') != _executions_digest(order['order'].get('executions') or []):
            _fail('BASELINE_MANIFEST_EXECUTIONS_INVALID')
        order_keys.add(order['order_key'])
    for execution in executions:
        if not isinstance(execution, dict) or not all(
                isinstance(execution.get(key), str) and execution.get(key)
                for key in ('execution_key', 'order_key', 'execution_id',
                            'canonical_execution_sha256')) \
                or not isinstance(execution.get('execution'), dict):
            _fail('BASELINE_MANIFEST_EXECUTION_INVALID')
        if execution['execution_key'] in execution_keys \
                or execution['order_key'] not in order_keys \
                or execution['canonical_execution_sha256'] != digest(execution['execution']):
            _fail('BASELINE_MANIFEST_EXECUTION_INVALID')
        execution_keys.add(execution['execution_key'])
    components = manifest.get('opening_balance_components')
    scope = manifest.get('baseline_scope')
    if scope is not None and scope not in {
            BASELINE_SCOPE_FULL, BASELINE_SCOPE_IDENTITY}:
        _fail('BASELINE_SCOPE_UNKNOWN', str(scope))
    if scope == BASELINE_SCOPE_IDENTITY:
        if manifest.get('claims') != IDENTITY_CLAIMS:
            _fail('BASELINE_IDENTITY_CLAIMS_INVALID')
        if components != []:
            _fail('BASELINE_MANIFEST_BALANCE_INVALID')
        return
    if not isinstance(components, list) or not components:
        _fail('BASELINE_MANIFEST_BALANCE_INVALID')
    component_names = set()
    for component in components:
        if not isinstance(component, dict) or not all(
                isinstance(component.get(key), str) and component.get(key)
                for key in ('component', 'amount', 'source')):
            _fail('BASELINE_MANIFEST_BALANCE_INVALID')
        if component['component'] in component_names:
            _fail('BASELINE_MANIFEST_BALANCE_INVALID')
        component_names.add(component['component'])


def _manifest_ids_checked(manifest):
    try:
        evidence = manifest['evidence']
        scope_material = [
            'historical-baseline-scope-1', manifest['account_key'],
            manifest['provider'], manifest['policy_sha256'],
            manifest['source_fingerprint'],
            _utc(manifest['cutoff_utc'], 'BASELINE_CUTOFF_INVALID').isoformat(),
            evidence['source']['execution_db_sha256'],
            evidence['risk']['sidecar_sha256'],
            evidence['host']['manifest_sha256'],
            evidence['lineage']['head_sha256']]
        if manifest.get('baseline_scope'):
            scope_material.append(manifest['baseline_scope'])
        scope_id = digest(scope_material)
        baseline_id = digest([
            'historical-baseline-1', manifest['account_key'],
            manifest['provider'], scope_id,
            _utc(manifest['cutoff_utc'], 'BASELINE_CUTOFF_INVALID').isoformat(),
            manifest['snapshot_sha256']])
    except (KeyError, TypeError, ValueError):
        _fail('BASELINE_MANIFEST_IDS_INVALID')
    if manifest.get('scope_id') != scope_id or manifest.get('baseline_id') != baseline_id:
        _fail('BASELINE_MANIFEST_IDS_INVALID')


def _revoke_event_id(baseline_id, manifest_sha256):
    return digest(['historical-baseline-event-1', baseline_id,
                   manifest_sha256, 'REVOKED'])


def _revoke_detail(manifest_sha256, approval_scope, reason):
    return canonical({
        'manifest_sha256': manifest_sha256,
        'old_status': 'APPLIED',
        'new_status': 'REVOKED',
        'approval_scope': approval_scope,
        'reason': reason,
    })


def _materialized_checked(db, manifest, values):
    """Reconcile every materialized row against the pinned manifest.

    The manifest JSON is the authority.  Stored digests are checked only as a
    redundant assertion; a changed row cannot make itself valid by editing its
    own hash column.
    """
    try:
        current_status = values.get('status')
        if current_status not in ('APPLIED', 'REVOKED'):
            _fail('BASELINE_MATERIALIZATION_INVALID', 'status')
        expected_columns = {
            'baseline_id': manifest['baseline_id'],
            'status': current_status,
            'manifest_sha256': manifest['manifest_sha256'],
            'account_key': manifest['account_key'],
            'account_mask': manifest['account_mask'],
            'provider': manifest['provider'],
            'policy_sha256': manifest['policy_sha256'],
            'source_fingerprint': manifest['source_fingerprint'],
            'source_execution_db_sha256': manifest['source_execution_db_sha256'],
            'risk_sidecar_sha256': manifest['risk_sidecar_sha256'],
            'host_manifest_sha256': manifest['host_manifest_sha256'],
            'lineage_head_sha256': manifest['lineage_head_sha256'],
            'cutoff_utc': manifest['cutoff_utc'],
            'snapshot_observed_at': manifest['snapshot_observed_at'],
            'snapshot_sha256': manifest['snapshot_sha256'],
            'expires_at': manifest['expires_at'],
            'manifest_json': canonical(manifest),
        }
        applied_at = _utc(values['applied_at'], 'BASELINE_APPLIED_AT_INVALID')
        approval_scope = values['approval_scope']
    except (BaselineError, KeyError, TypeError, ValueError):
        _fail('BASELINE_MATERIALIZATION_INVALID', 'manifest_columns')
    if not isinstance(approval_scope, str) or not approval_scope.strip():
        _fail('BASELINE_MATERIALIZATION_INVALID', 'approval_scope')
    for key, expected in expected_columns.items():
        if values.get(key) != expected:
            _fail('BASELINE_MATERIALIZATION_MISMATCH', key)

    expected_orders = {order['order_key']: order for order in manifest['orders']}
    try:
        order_rows = db.execute(
            'SELECT order_key,order_id,ref_id,account_key,provider,'
            'canonical_order_sha256,executions_sha256,order_json '
            'FROM historical_baseline_order WHERE baseline_id=?',
            (manifest['baseline_id'],)).fetchall()
    except sqlite3.Error:
        _fail('BASELINE_MATERIALIZATION_INVALID', 'order_read')
    if len(order_rows) != len(expected_orders) \
            or {row[0] for row in order_rows} != set(expected_orders):
        _fail('BASELINE_MATERIALIZATION_MISMATCH', 'order_set')
    for key, order_id, ref_id, account_key, provider, canonical_hash, \
            executions_hash, order_json in order_rows:
        expected = expected_orders[key]
        if (order_id != expected['order_id'] or ref_id != expected['ref_id']
                or account_key != manifest['account_key']
                or provider != manifest['provider']
                or canonical_hash != expected['canonical_order_sha256']
                or executions_hash != expected['executions_sha256']
                or order_json != canonical(expected['order'])):
            _fail('BASELINE_MATERIALIZATION_MISMATCH', 'order:' + key)

    expected_executions = {row['execution_key']: row
                           for row in manifest['executions']}
    try:
        execution_rows = db.execute(
            'SELECT execution_key,order_key,execution_id,order_id,ref_id,'
            'account_key,provider,canonical_execution_sha256,execution_json '
            'FROM historical_baseline_execution WHERE baseline_id=?',
            (manifest['baseline_id'],)).fetchall()
    except sqlite3.Error:
        _fail('BASELINE_MATERIALIZATION_INVALID', 'execution_read')
    if len(execution_rows) != len(expected_executions) \
            or {row[0] for row in execution_rows} != set(expected_executions):
        _fail('BASELINE_MATERIALIZATION_MISMATCH', 'execution_set')
    for key, order_key, execution_id, order_id, ref_id, account_key, provider, \
            canonical_hash, execution_json in execution_rows:
        expected = expected_executions[key]
        if (order_key != expected['order_key']
                or execution_id != expected['execution_id']
                or order_id != expected['order_id']
                or ref_id != expected['ref_id']
                or account_key != manifest['account_key']
                or provider != manifest['provider']
                or canonical_hash != expected['canonical_execution_sha256']
                or execution_json != canonical(expected['execution'])):
            _fail('BASELINE_MATERIALIZATION_MISMATCH', 'execution:' + key)

    expected_components = {item['component']: item
                           for item in manifest['opening_balance_components']}
    try:
        component_rows = db.execute(
            'SELECT component,amount,source FROM historical_baseline_balance_component '
            'WHERE baseline_id=?', (manifest['baseline_id'],)).fetchall()
    except sqlite3.Error:
        _fail('BASELINE_MATERIALIZATION_INVALID', 'balance_read')
    if len(component_rows) != len(expected_components) \
            or {row[0] for row in component_rows} != set(expected_components):
        _fail('BASELINE_MATERIALIZATION_MISMATCH', 'balance_set')
    for component, amount, source in component_rows:
        expected = expected_components[component]
        if amount != expected['amount'] or source != expected['source']:
            _fail('BASELINE_MATERIALIZATION_MISMATCH', 'balance:' + component)

    expected_applied_id = digest([
        'historical-baseline-event-1', manifest['baseline_id'],
        manifest['manifest_sha256'], 'APPLIED'])
    expected_applied_detail = canonical({
        'manifest_sha256': manifest['manifest_sha256'],
        'approval_scope': approval_scope})
    expected_revoked_id = _revoke_event_id(
        manifest['baseline_id'], manifest['manifest_sha256'])
    try:
        event_rows = db.execute(
            'SELECT event_id,baseline_id,kind,occurred_at,detail '
            'FROM historical_baseline_event WHERE baseline_id=?',
            (manifest['baseline_id'],)).fetchall()
    except sqlite3.Error:
        _fail('BASELINE_MATERIALIZATION_INVALID', 'event_read')
    if any(not isinstance(row[0], str) for row in event_rows):
        _fail('BASELINE_MATERIALIZATION_INVALID', 'event_id')
    by_kind = {}
    for event_id, baseline_id, kind, occurred_at, detail in event_rows:
        if baseline_id != manifest['baseline_id'] or kind in by_kind:
            _fail('BASELINE_MATERIALIZATION_MISMATCH', 'event_set')
        by_kind[kind] = (event_id, occurred_at, detail)
    if current_status == 'APPLIED':
        if set(by_kind) != {'BASELINE_APPLIED'}:
            _fail('BASELINE_MATERIALIZATION_MISMATCH', 'event_set')
        event_id, occurred_at, detail = by_kind['BASELINE_APPLIED']
        if (event_id != expected_applied_id
                or _utc(occurred_at, 'BASELINE_EVENT_TIME_INVALID') != applied_at
                or detail != expected_applied_detail):
            _fail('BASELINE_MATERIALIZATION_MISMATCH', 'event')
    else:
        if set(by_kind) != {'BASELINE_APPLIED', 'BASELINE_REVOKED'}:
            _fail('BASELINE_MATERIALIZATION_MISMATCH', 'event_set')
        applied = by_kind['BASELINE_APPLIED']
        revoked = by_kind['BASELINE_REVOKED']
        if (applied[0] != expected_applied_id
                or _utc(applied[1], 'BASELINE_EVENT_TIME_INVALID') != applied_at
                or applied[2] != expected_applied_detail):
            _fail('BASELINE_MATERIALIZATION_MISMATCH', 'applied_event')
        revoked_at = _utc(revoked[1], 'BASELINE_EVENT_TIME_INVALID')
        if revoked_at < applied_at or revoked[0] != expected_revoked_id:
            _fail('BASELINE_MATERIALIZATION_MISMATCH', 'revoked_event')
        try:
            revoke_detail = json.loads(revoked[2])
            revoke_scope = revoke_detail.get('approval_scope')
            revoke_reason = revoke_detail.get('reason')
        except (TypeError, ValueError, AttributeError):
            _fail('BASELINE_MATERIALIZATION_MISMATCH', 'revoked_detail')
        if (set(revoke_detail) != {
                'manifest_sha256', 'old_status', 'new_status',
                'approval_scope', 'reason'}
                or revoke_detail.get('manifest_sha256')
                != manifest['manifest_sha256']
                or revoke_detail.get('old_status') != 'APPLIED'
                or revoke_detail.get('new_status') != 'REVOKED'
                or not isinstance(revoke_scope, str) or not revoke_scope.strip()
                or not isinstance(revoke_reason, str) or not revoke_reason.strip()
                or canonical(revoke_detail) != revoked[2]):
            _fail('BASELINE_MATERIALIZATION_MISMATCH', 'revoked_detail')


def _materialization_orphans_checked(db, baseline_ids):
    for table in ('historical_baseline_order', 'historical_baseline_execution',
                  'historical_baseline_balance_component',
                  'historical_baseline_event'):
        try:
            rows = db.execute(
                f'SELECT DISTINCT baseline_id FROM {table}').fetchall()
        except sqlite3.Error:
            _fail('BASELINE_MATERIALIZATION_INVALID', 'orphan_read')
        if any(row[0] not in baseline_ids for row in rows):
            _fail('BASELINE_MATERIALIZATION_ORPHAN', table)


def _manifest_checked(manifest, manifest_pin, *, now, require_expiry=True):
    if not isinstance(manifest, dict) or manifest.get('kind') != 'historical_baseline_manifest' \
            or manifest.get('schema_version') != SCHEMA_VERSION:
        _fail('BASELINE_MANIFEST_INVALID')
    if manifest.get('status') not in ('PLANNED', 'VALIDATED', 'APPLIED'):
        _fail('BASELINE_MANIFEST_STATUS_INVALID')
    _manifest_rows_checked(manifest)
    _manifest_ids_checked(manifest)
    evidence = manifest.get('evidence')
    if not isinstance(evidence, dict) or any(
            manifest.get(column) != value for column, value in (
                ('source_execution_db_sha256', evidence.get('source', {}).get('execution_db_sha256')),
                ('risk_sidecar_sha256', evidence.get('risk', {}).get('sidecar_sha256')),
                ('host_manifest_sha256', evidence.get('host', {}).get('manifest_sha256')),
                ('lineage_head_sha256', evidence.get('lineage', {}).get('head_sha256')))):
        _fail('BASELINE_MANIFEST_IDS_INVALID')
    _sha(manifest_pin, 'BASELINE_MANIFEST_PIN_INVALID')
    expected = digest({key: value for key, value in manifest.items()
                       if key != 'manifest_sha256'})
    if manifest.get('manifest_sha256') != expected or manifest_pin != expected:
        _fail('BASELINE_MANIFEST_PIN_MISMATCH')
    if require_expiry and _utc(manifest['expires_at'], 'BASELINE_EXPIRY_INVALID') <= now:
        _fail('BASELINE_PLAN_EXPIRED')
    if manifest.get('live_enabled') is not False or manifest.get('production_ready') is not False:
        _fail('BASELINE_AUTHORITY_INVALID')
    time_provenance = manifest.get('time_provenance')
    if not isinstance(time_provenance, dict) \
            or time_provenance.get('snapshot_observed_at_source') != SNAPSHOT_TIME_SOURCE \
            or time_provenance.get('execution_time_source') != EXECUTION_TIME_SOURCE:
        _fail('BASELINE_TIME_PROVENANCE_INVALID', 'manifest')
    observed = _utc(manifest['snapshot_observed_at'],
                    'BASELINE_SNAPSHOT_TIME_INVALID')
    cutoff = _utc(manifest['cutoff_utc'], 'BASELINE_CUTOFF_INVALID')
    if observed > now or cutoff > observed:
        _fail('BASELINE_SNAPSHOT_TIME_INVALID')
    if _utc(manifest['expires_at'], 'BASELINE_EXPIRY_INVALID') < observed:
        _fail('BASELINE_EXPIRY_BEFORE_SNAPSHOT')
    return expected


def _check_db_overlap(db, manifest):
    _global_unresolved(db)
    account_key = manifest['account_key']
    provider = manifest['provider']
    for order in manifest['orders']:
        _live_overlap(db, account_key, provider, order['order_id'], order['ref_id'],
                      [execution['execution_id']
                       for execution in order['executions']])


def validate_baseline(db, manifest, *, manifest_pin, now=None, clock=None,
                      evidence=None, snapshot=None):
    """Read-only validation.  Never creates or writes baseline tables."""
    now = _now(now, clock)
    _manifest_checked(manifest, manifest_pin, now=now)
    if snapshot is None or evidence is None:
        _fail('BASELINE_CURRENT_INPUTS_REQUIRED')
    _validate_evidence(evidence, account_key=manifest['account_key'],
                       provider=manifest['provider'],
                       policy_sha256=manifest['policy_sha256'],
                       source_fingerprint=manifest['source_fingerprint'])
    if canonical(evidence) != canonical(manifest['evidence']):
        _fail('BASELINE_EVIDENCE_CHANGED')
    if digest(snapshot) != manifest['snapshot_sha256']:
        _fail('BASELINE_SNAPSHOT_CHANGED')
    scope = manifest.get('baseline_scope')
    if scope == BASELINE_SCOPE_IDENTITY:
        _validate_order_identity_snapshot(
            snapshot, account_key=manifest['account_key'],
            provider=manifest['provider'],
            cutoff=_utc(manifest['cutoff_utc'], 'BASELINE_CUTOFF_INVALID'),
            observed_at=_utc(manifest['snapshot_observed_at'],
                             'BASELINE_SNAPSHOT_TIME_INVALID'),
            now=now)
    elif scope in (None, BASELINE_SCOPE_FULL):
        _validate_snapshot(snapshot, account_key=manifest['account_key'],
                           provider=manifest['provider'],
                           cutoff=_utc(manifest['cutoff_utc'],
                                       'BASELINE_CUTOFF_INVALID'),
                           observed_at=_utc(manifest['snapshot_observed_at'],
                                            'BASELINE_SNAPSHOT_TIME_INVALID'),
                           now=now)
    else:
        _fail('BASELINE_SCOPE_UNKNOWN', str(scope))
    _schema_state(db)
    _check_db_overlap(db, manifest)
    return {'status': 'VALIDATED', 'baseline_id': manifest['baseline_id'],
            'manifest_sha256': manifest['manifest_sha256'],
            'counts': manifest['counts']}


ENTRY_AUTHORITY_MARKER_TABLE = 'entry_authority_marker'


def _store_entry_marker(db, manifest, now, *, reason):
    """Durable anchor outside the baseline schema; never an entry grant."""
    db.execute(
        'CREATE TABLE IF NOT EXISTS entry_authority_marker('
        'id INTEGER PRIMARY KEY CHECK(id=1),'
        'required INTEGER NOT NULL,'
        'reason TEXT NOT NULL,'
        'baseline_id TEXT,'
        'baseline_scope TEXT,'
        'created_at TEXT NOT NULL,'
        'updated_at TEXT NOT NULL)')
    db.execute(
        'INSERT OR REPLACE INTO entry_authority_marker('
        'id,required,reason,baseline_id,baseline_scope,created_at,updated_at) '
        'VALUES(1,1,?,?,?,?,?)',
        (reason, manifest.get('baseline_id'), manifest.get('baseline_scope'),
         now.isoformat(), now.isoformat()))


def _store_manifest(db, manifest, *, approval_scope, now):
    sequence = db.execute(
        'SELECT coalesce(max(sequence),0)+1 FROM historical_baseline_manifest'
    ).fetchone()[0]
    encoded = canonical(manifest)
    db.execute(
        'INSERT INTO historical_baseline_manifest('
        'baseline_id,sequence,status,manifest_sha256,account_key,account_mask,'
        'provider,policy_sha256,source_fingerprint,source_execution_db_sha256,'
        'risk_sidecar_sha256,host_manifest_sha256,lineage_head_sha256,cutoff_utc,'
        'snapshot_observed_at,snapshot_sha256,expires_at,manifest_json,applied_at,'
        'approval_scope) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
        (manifest['baseline_id'], sequence, 'APPLIED', manifest['manifest_sha256'],
         manifest['account_key'], manifest['account_mask'], manifest['provider'],
         manifest['policy_sha256'], manifest['source_fingerprint'],
         manifest['evidence']['source']['execution_db_sha256'],
         manifest['evidence']['risk']['sidecar_sha256'],
         manifest['evidence']['host']['manifest_sha256'],
         manifest['evidence']['lineage']['head_sha256'],
         manifest['cutoff_utc'], manifest['snapshot_observed_at'],
         manifest['snapshot_sha256'], manifest['expires_at'], encoded,
         now.isoformat(), approval_scope))
    for order in manifest['orders']:
        db.execute(
            'INSERT INTO historical_baseline_order(baseline_id,order_key,order_id,'
            'ref_id,account_key,provider,canonical_order_sha256,executions_sha256,'
            'order_json) VALUES(?,?,?,?,?,?,?,?,?)',
            (manifest['baseline_id'], order['order_key'], order['order_id'],
             order['ref_id'], manifest['account_key'], manifest['provider'],
             order['canonical_order_sha256'], order['executions_sha256'],
             canonical(order['order'])))
    for execution in manifest['executions']:
        order = next(order for order in manifest['orders']
                     if order['order_key'] == execution['order_key'])
        db.execute(
            'INSERT INTO historical_baseline_execution(baseline_id,execution_key,'
            'order_key,execution_id,order_id,ref_id,account_key,provider,'
            'canonical_execution_sha256,execution_json) VALUES(?,?,?,?,?,?,?,?,?,?)',
            (manifest['baseline_id'], execution['execution_key'], execution['order_key'],
             execution['execution_id'], execution['order_id'], execution['ref_id'],
             manifest['account_key'], manifest['provider'],
             execution['canonical_execution_sha256'], canonical(execution['execution'])))
    for component in manifest['opening_balance_components']:
        db.execute(
            'INSERT INTO historical_baseline_balance_component(baseline_id,component,'
            'amount,source) VALUES(?,?,?,?)',
            (manifest['baseline_id'], component['component'], component['amount'],
             component['source']))
    event_id = digest(['historical-baseline-event-1', manifest['baseline_id'],
                       manifest['manifest_sha256'], 'APPLIED'])
    db.execute(
        'INSERT INTO historical_baseline_event(event_id,baseline_id,kind,'
        'occurred_at,detail) VALUES(?,?,?,?,?)',
        (event_id, manifest['baseline_id'], 'BASELINE_APPLIED', now.isoformat(),
         canonical({'manifest_sha256': manifest['manifest_sha256'],
                    'approval_scope': approval_scope})))
    _store_entry_marker(db, manifest, now, reason='BASELINE_APPLIED')


def apply_baseline(db, manifest, *, manifest_pin, approval_scope, now=None,
                   clock=None, evidence=None, snapshot=None, prewrite=None):
    """Explicit local apply.  Joins a caller transaction or owns one.

    ``prewrite`` is an optional local-boundary hook.  It runs under the
    acquired write transaction after both validations and immediately before
    the first baseline DDL/DML.  It must not write baseline/live state.
    """
    if not isinstance(approval_scope, str) or not approval_scope.strip():
        _fail('BASELINE_APPROVAL_SCOPE_REQUIRED')
    if not isinstance(manifest, dict):
        _fail('BASELINE_MANIFEST_INVALID')
    explicit_now = now
    source_clock = clock if explicit_now is None else None
    now = _now(now, source_clock)
    state = _schema_state(db)
    if state['state'] == 'PRESENT':
        try:
            prior = db.execute(
                'SELECT manifest_sha256,manifest_json,status FROM historical_baseline_manifest '
                'WHERE baseline_id=?', (manifest.get('baseline_id'),)).fetchone()
        except sqlite3.Error:
            _fail('BASELINE_SCHEMA_READ_FAILED')
        if prior is not None:
            # Exact terminal replay is read-only and integrity-checked.  It
            # must never turn an old REVOKED row into a new admission.
            _manifest_checked(manifest, manifest_pin, now=now, require_expiry=False)
            if (prior[0] != manifest['manifest_sha256']
                    or prior[1] != canonical(manifest)):
                _fail('BASELINE_ID_PAYLOAD_CONFLICT')
            if prior[2] == 'APPLIED':
                return {'status': 'APPLIED',
                        'baseline_id': manifest['baseline_id'],
                        'duplicate': True, 'manifest_sha256': prior[0]}
            if prior[2] == 'REVOKED':
                return {'status': 'REVOKED', 'active': False,
                        'baseline_id': manifest['baseline_id'],
                        'duplicate': False, 'manifest_sha256': prior[0]}
            _fail('BASELINE_ID_PAYLOAD_CONFLICT')
        if state['active_count']:
            _fail('BASELINE_ACTIVE_EXISTS')
    if snapshot is None or evidence is None:
        _fail('BASELINE_CURRENT_INPUTS_REQUIRED')
    validate_baseline(db, manifest, manifest_pin=manifest_pin, now=now,
                      clock=clock, evidence=evidence, snapshot=snapshot)
    with atomic(db):
        # A write lock may have waited.  Re-read the clock and every
        # freshness/expiry/overlap condition under the lock before creating
        # any baseline schema or writing a row.
        if source_clock is not None:
            later = _now(None, source_clock)
        elif explicit_now is None:
            later = datetime.now(timezone.utc)
        else:
            later = now
        validate_baseline(db, manifest, manifest_pin=manifest_pin, now=later,
                          clock=clock, evidence=evidence, snapshot=snapshot)
        if prewrite is not None:
            if not callable(prewrite):
                _fail('BASELINE_PREWRITE_INVALID')
            prewrite()
        # The prewrite hook may perform a real source read and consume time.
        # Re-sample after it and re-run the complete freshness/expiry/evidence
        # validation immediately before the first baseline DDL/DML.  The
        # stored time is the final validation time, never the pre-hook sample.
        if source_clock is not None:
            final_now = _now(None, source_clock)
        elif explicit_now is None:
            final_now = datetime.now(timezone.utc)
        else:
            final_now = now
        validate_baseline(db, manifest, manifest_pin=manifest_pin,
                          now=final_now, clock=clock, evidence=evidence,
                          snapshot=snapshot)
        _ensure_schema(db)
        active = db.execute(
            "SELECT baseline_id FROM historical_baseline_manifest WHERE status='APPLIED' "
            'LIMIT 1').fetchone()
        if active is not None:
            _fail('BASELINE_ACTIVE_EXISTS')
        _store_manifest(db, manifest, approval_scope=approval_scope,
                        now=final_now)
    return {'status': 'APPLIED', 'baseline_id': manifest['baseline_id'],
            'duplicate': False, 'manifest_sha256': manifest['manifest_sha256'],
            'counts': manifest['counts'], 'production_ready': False,
            'live_enabled': False}


def _revoke_fact(db, baseline_id, manifest_sha256):
    try:
        rows = db.execute(
            'SELECT event_id,occurred_at,detail FROM historical_baseline_event '
            "WHERE baseline_id=? AND kind='BASELINE_REVOKED'",
            (baseline_id,)).fetchall()
    except sqlite3.Error:
        _fail('BASELINE_MATERIALIZATION_INVALID', 'revoke_event_read')
    if len(rows) != 1:
        _fail('BASELINE_MATERIALIZATION_MISMATCH', 'revoke_event_set')
    event_id, occurred_at, detail = rows[0]
    if event_id != _revoke_event_id(baseline_id, manifest_sha256):
        _fail('BASELINE_MATERIALIZATION_MISMATCH', 'revoke_event_id')
    try:
        parsed = json.loads(detail)
    except (TypeError, ValueError):
        _fail('BASELINE_MATERIALIZATION_MISMATCH', 'revoke_event_detail')
    if (not isinstance(parsed, dict)
            or set(parsed) != {
                'manifest_sha256', 'old_status', 'new_status',
                'approval_scope', 'reason'}
            or parsed.get('manifest_sha256') != manifest_sha256
            or parsed.get('old_status') != 'APPLIED'
            or parsed.get('new_status') != 'REVOKED'
            or not isinstance(parsed.get('approval_scope'), str)
            or not parsed['approval_scope'].strip()
            or not isinstance(parsed.get('reason'), str)
            or not parsed['reason'].strip()
            or canonical(parsed) != detail):
        _fail('BASELINE_MATERIALIZATION_MISMATCH', 'revoke_event_detail')
    return {'event_id': event_id,
            'revoked_at': _utc(occurred_at,
                               'BASELINE_EVENT_TIME_INVALID').isoformat(),
            'approval_scope': parsed['approval_scope'],
            'reason': parsed['reason']}


def revoke_baseline(db, baseline_id, manifest_pin, *, approval_scope, reason,
                    now=None, clock=None, prewrite=None):
    """Append-only revoke of the exact active baseline. Caller commits."""
    if not isinstance(baseline_id, str) or not baseline_id:
        _fail('BASELINE_REVOKE_TARGET_INVALID')
    if not isinstance(approval_scope, str) or not approval_scope.strip():
        _fail('BASELINE_REVOKE_APPROVAL_SCOPE_REQUIRED')
    if not isinstance(reason, str) or not reason.strip():
        _fail('BASELINE_REVOKE_REASON_REQUIRED')
    _sha(manifest_pin, 'BASELINE_REVOKE_PIN_INVALID')
    explicit_now = now
    source_clock = clock if explicit_now is None else None
    current = _now(now, source_clock)
    state = _schema_state(db)
    if state['state'] == 'ABSENT':
        _fail('BASELINE_REVOKE_TARGET_MISSING')
    try:
        row = db.execute(
            'SELECT status,manifest_sha256,manifest_json FROM '
            'historical_baseline_manifest WHERE baseline_id=?',
            (baseline_id,)).fetchone()
    except sqlite3.Error:
        _fail('BASELINE_REVOKE_READ_FAILED')
    if row is None:
        _fail('BASELINE_REVOKE_TARGET_MISSING')
    status, stored_pin, encoded = row
    if stored_pin != manifest_pin:
        _fail('BASELINE_REVOKE_PIN_MISMATCH')
    try:
        manifest = json.loads(encoded)
    except (TypeError, ValueError):
        _fail('BASELINE_REVOKE_MANIFEST_INVALID')
    _manifest_checked(manifest, manifest_pin, now=current,
                      require_expiry=False)
    if status == 'REVOKED':
        fact = _revoke_fact(db, baseline_id, manifest_pin)
        return {'status': 'ALREADY_REVOKED', 'baseline_id': baseline_id,
                'manifest_sha256': manifest_pin, **fact}
    if status != 'APPLIED':
        _fail('BASELINE_REVOKE_NOT_ACTIVE')
    _global_unresolved(db)
    with atomic(db):
        # Re-read target and unresolved state after the caller/write lock.
        try:
            row = db.execute(
                'SELECT status,manifest_sha256,manifest_json,applied_at FROM '
                'historical_baseline_manifest WHERE baseline_id=?',
                (baseline_id,)).fetchone()
        except sqlite3.Error:
            _fail('BASELINE_REVOKE_READ_FAILED')
        if row is None or row[0] != 'APPLIED' or row[1] != manifest_pin:
            _fail('BASELINE_REVOKE_CAS_FAILED')
        applied_at = _utc(row[3], 'BASELINE_APPLIED_AT_INVALID')
        _manifest_checked(manifest, manifest_pin,
                          now=_now(now, source_clock),
                          require_expiry=False)
        _global_unresolved(db)
        if prewrite is not None:
            if not callable(prewrite):
                _fail('BASELINE_REVOKE_PREWRITE_INVALID')
            prewrite()
        _global_unresolved(db)
        # Sample after the last long check.  The event timestamp must not
        # predate the source/global validation it is about to record.
        final_now = _now(now, source_clock)
        if final_now < applied_at:
            _fail('BASELINE_REVOKE_TIME_INVALID')
        result = db.execute(
            "UPDATE historical_baseline_manifest SET status='REVOKED' "
            'WHERE baseline_id=? AND manifest_sha256=? AND status=\'APPLIED\'',
            (baseline_id, manifest_pin))
        if result.rowcount != 1:
            _fail('BASELINE_REVOKE_CAS_FAILED')
        event_id = _revoke_event_id(baseline_id, manifest_pin)
        db.execute(
            'INSERT INTO historical_baseline_event(event_id,baseline_id,kind,'
            'occurred_at,detail) VALUES(?,?,?,?,?)',
            (event_id, baseline_id, 'BASELINE_REVOKED',
             final_now.isoformat(),
             _revoke_detail(manifest_pin, approval_scope, reason)))
        _store_entry_marker(db, manifest, final_now,
                            reason='BASELINE_REVOKED')
    fact = _revoke_fact(db, baseline_id, manifest_pin)
    return {'status': 'REVOKED', 'baseline_id': baseline_id,
            'manifest_sha256': manifest_pin, **fact}


def status_baseline(db):
    """Read-only status; absent schema is a normal legacy database."""
    try:
        state = _schema_state(db)
    except BaselineError as exc:
        return {'status': 'INVALID', 'active': None, 'reason': exc.code,
                'production_ready': False, 'live_enabled': False}
    if state['state'] == 'ABSENT':
        return {'status': 'ABSENT', 'active': None, 'production_ready': False,
                'live_enabled': False}
    try:
        rows = db.execute(
            'SELECT baseline_id,status,manifest_sha256,account_key,provider,cutoff_utc,'
            'snapshot_observed_at,applied_at,expires_at,manifest_json '
            'FROM historical_baseline_manifest ORDER BY sequence').fetchall()
    except sqlite3.Error:
        return {'status': 'INVALID', 'active': None,
                'reason': 'BASELINE_SCHEMA_READ_FAILED',
                'production_ready': False, 'live_enabled': False}
    history = []
    active = []
    for row in rows:
        entry = dict(zip((
            'baseline_id', 'status', 'manifest_sha256', 'account_key',
            'provider', 'cutoff_utc', 'snapshot_observed_at', 'applied_at',
            'expires_at', 'manifest_json'), row))
        try:
            encoded = json.loads(entry.pop('manifest_json'))
            entry['baseline_scope'] = (encoded.get('baseline_scope')
                                       if isinstance(encoded, dict) else None)
        except (TypeError, ValueError):
            entry['baseline_scope'] = None
        if entry['status'] == 'REVOKED':
            fact = _revoke_fact(db, entry['baseline_id'],
                                entry['manifest_sha256'])
            entry.update({
                'revoked_at': fact['revoked_at'],
                'revoke_approval_scope': fact['approval_scope'],
                'revoke_reason': fact['reason'],
            })
        history.append(entry)
        if entry['status'] == 'APPLIED':
            active.append(entry)
    return {'status': 'PRESENT', 'active': active, 'history': history,
            'production_ready': False, 'live_enabled': False}


class HistoricalBaselineIndex:
    """Read-only exact matcher used by the receipt importer."""

    # Bounded retries for a cache token/revision generation that changes while
    # it is being sampled.  Exhaustion is a conflict, never an old-state hit.
    _CACHE_ATTEMPTS = 4

    def __init__(self, db):
        self.db = db
        self._import_depth = 0
        self._import_state = None
        self._import_revision = None
        self._import_token = None

    def begin_import(self):
        """Cache one validated read view for a single snapshot import.

        The cache is scoped to the import call, never persisted across calls or
        transactions.  It avoids rescanning every materialized baseline row for
        each order while still validating the complete index once per import.
        A revision change (apply/revoke), including a commit by this same
        connection, discards the active maps.  Unrelated live writes only cause
        a small revision check; they do not trigger a full materialized scan.
        """
        self._import_depth += 1
        self._clear_cache()

    def end_import(self):
        if self._import_depth:
            self._import_depth -= 1
        self._clear_cache()

    def _clear_cache(self):
        self._import_state = None
        self._import_revision = None
        self._import_token = None

    def _token(self):
        """Return a cheap change token for this connection and its database.

        ``data_version`` changes when another connection commits.
        ``total_changes`` also changes for this connection's own committed or
        rolled-back DML.  Both are needed: a same-connection apply/revoke does
        not move ``data_version``.
        """
        try:
            data_version = self.db.execute(
                'PRAGMA data_version').fetchone()[0]
            # ``total_changes`` does not move on a rollback; transaction state
            # does, so a view cached inside a transaction is revalidated after
            # rollback instead of remaining pinned to the uncommitted state.
            return (data_version, self.db.total_changes,
                    bool(self.db.in_transaction))
        except (sqlite3.Error, AttributeError, TypeError):
            return None

    def _revision(self):
        """Baseline-only revision; no full _materialized validation.

        The manifest/event/schema rows are tiny.  Materialized rows are not
        re-read for every live write; they are fully validated whenever the
        state cache is rebuilt or the next import begins.
        """
        try:
            rows = self.db.execute(
                'SELECT baseline_id,status,manifest_sha256,'
                'length(manifest_json) FROM historical_baseline_manifest '
                'ORDER BY sequence').fetchall()
            events = self.db.execute(
                'SELECT event_id,kind,occurred_at,detail '
                'FROM historical_baseline_event ORDER BY event_id').fetchall()
            version = self.db.execute(
                'SELECT version FROM historical_baseline_schema').fetchall()
            return (tuple(rows), tuple(events), tuple(version))
        except (sqlite3.Error, TypeError):
            return None

    def _stable_revision(self):
        """Read the small revision with a stable token, bounded retries.

        The revision itself is three SELECTs.  A commit between them must not
        produce a mixed revision; the token before/after is checked and the
        read is retried a bounded number of times.
        """
        for _ in range(self._CACHE_ATTEMPTS):
            before = self._token()
            if before is None:
                return None
            revision = self._revision()
            after = self._token()
            if revision is not None and before == after:
                return revision, after
            if revision is None:
                return None
        _fail('BASELINE_INDEX_UNSTABLE')

    def _read_generation(self):
        """Return state/revision/token from one verified generation.

        No read transaction is held across the scans: that would block the
        owner's writer on a rollback-journal database.  Instead, token and
        revision checks bracket the state read.  A commit during any window
        changes the token/revision and forces a bounded retry.  Unrelated live
        writes that leave the baseline revision unchanged may keep the
        already-validated state; they never require a full materialized scan.
        """
        for _ in range(self._CACHE_ATTEMPTS):
            before = self._token()
            if before is None:
                break
            current = self._stable_revision()
            if current is None:
                state = _schema_state(self.db)
                after = self._token()
                if state.get('state') == 'ABSENT' and after == before:
                    return state, None, None
                continue
            pre_revision, mid = current
            state = _schema_state(self.db)
            state_token = self._token()
            if state_token is None:
                break
            if state_token != mid:
                # Another commit occurred between the revision and state
                # scans.  If the revision is unchanged it was an unrelated
                # live write; otherwise the old active map must be discarded.
                current = self._stable_revision()
                if current is not None and current[0] == pre_revision:
                    return state, pre_revision, current[1]
                continue
            current = self._stable_revision()
            if (current is not None and current[0] == pre_revision
                    and current[1] == state_token):
                return state, pre_revision, current[1]
        _fail('BASELINE_INDEX_UNSTABLE')

    def _state(self):
        if not self._import_depth:
            return _schema_state(self.db)
        if self._import_state is not None:
            token = self._token()
            if token is None:
                self._clear_cache()
                _fail('BASELINE_INDEX_UNSTABLE')
            # A token is not a transaction identity: ROLLBACK and
            # ROLLBACK TO SAVEPOINT can restore an older generation while
            # data_version, total_changes, and in_transaction stay unchanged.
            # Never trust the fast token path while a caller transaction is
            # open; always verify the real baseline revision there.  Outside
            # a transaction the token is sufficient for a true fast hit.
            if token == self._import_token and not self.db.in_transaction:
                return self._import_state
            # Same-connection writes move total_changes; other connections
            # move data_version; an open transaction may have rolled back to
            # a different savepoint.  A cheap revision check distinguishes
            # unrelated live writes (retain the validated view) from
            # apply/revoke/rollback (discard it).
            current = self._stable_revision()
            if (current is not None
                    and current[0] == self._import_revision):
                self._import_token = current[1]
                return self._import_state
            self._clear_cache()
        state, revision, token = self._read_generation()
        if self._import_depth and revision is not None and token is not None:
            self._import_state = state
            self._import_revision = revision
            self._import_token = token
        return state

    @property
    def available(self):
        try:
            return _schema_state(self.db)['state'] == 'PRESENT'
        except BaselineError:
            return False

    def match_order(self, order, account_key, provider=PROVIDER):
        """Return matched/conflict/none from one immutable validated view."""
        try:
            state = self._state()
        except BaselineError as exc:
            return {'status': 'conflict', 'reason': exc.code}
        if state['state'] == 'ABSENT':
            return {'status': 'none'}
        if not isinstance(order, dict):
            return {'status': 'conflict', 'reason': 'BASELINE_ORDER_INVALID'}
        order_id = order.get('id') or order.get('order_id')
        ref_id = order.get('ref_id')
        if not isinstance(order_id, str) or not order_id:
            return {'status': 'conflict', 'reason': 'BASELINE_ORDER_ID_INVALID'}
        executions = order.get('executions') if isinstance(order.get('executions'), list) else []
        order_digest = digest(order)
        exec_digest = _executions_digest(executions)
        active = state.get('active_orders', {}).get(
            (account_key, provider, order_id))
        if active is not None:
            if (active['canonical_order_sha256'] != order_digest
                    or active['executions_sha256'] != exec_digest):
                return {'status': 'conflict', 'reason': 'BASELINE_ORDER_REVISED',
                        'baseline_id': active['baseline_id']}
            return {'status': 'matched', 'baseline_id': active['baseline_id'],
                    'order_key': active['order_key']}
        if ref_id is not None:
            reused = state.get('active_refs', {}).get(
                (account_key, provider, ref_id))
            if reused is not None:
                return {'status': 'conflict', 'reason': 'BASELINE_REF_REUSED',
                        'baseline_id': reused['baseline_id']}
        for execution in executions:
            if not isinstance(execution, dict):
                continue
            execution_id = execution.get('id')
            if not isinstance(execution_id, str) or not execution_id:
                continue
            reused = state.get('active_executions', {}).get(
                (account_key, provider, execution_id))
            if reused is not None:
                return {'status': 'conflict', 'reason': 'BASELINE_EXECUTION_REUSED',
                        'baseline_id': reused['baseline_id']}
        return {'status': 'none'}
