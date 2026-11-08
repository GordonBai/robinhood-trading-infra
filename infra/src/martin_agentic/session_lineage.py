"""Project-scoped, account-bound admission across immutable execution sessions.

This is a new-risk guard, not another risk engine or a broker authorization.
Registration is explicit. Management remains the caller's existing lifecycle.
Local checksum chains detect inconsistent edits, not a hostile rewrite of all
local evidence or direct broker tools outside this application's boundaries.
"""
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3

from .audit import AuditTrail, canonical, digest
from .qualification import candidate_root
from .broker import TERMINAL, APPROVAL_NO_ORDER_TERMINAL
from .ledger import timestamp
from .recovery import recovery_disposition, _result_order_id
from .risk import number
from .transport import TransportError


CANONICAL_RUNTIME = candidate_root() / 'runtime'
ZERO_HASH = '0' * 64
JOURNAL_ONLY = {'reconciliation_issues', 'new_executions_seen', 'baseline_established'}
UNSETTLED = {'READY', 'RUNNING', 'UNKNOWN', 'WAITING_USER', 'REPORTED'}


def _now(clock):
    return (clock or (lambda: datetime.now(timezone.utc)))()


def _canonical_path(value):
    path = Path(value)
    if not path.is_absolute():
        path = CANONICAL_RUNTIME.parent / path
    resolved = path.resolve()
    if resolved.parent != CANONICAL_RUNTIME.resolve() or path.is_symlink():
        raise ValueError('LINEAGE_NONCANONICAL_EXECUTION_PATH')
    return resolved


def _risk_path(policy):
    suffix = policy.get('account_suffix')
    if not isinstance(suffix, str) or len(suffix) != 4 or not suffix.isdigit():
        raise ValueError('LINEAGE_ACCOUNT_SUFFIX_INVALID')
    path = CANONICAL_RUNTIME.resolve() / ('account-risk-' + suffix + '.sqlite')
    if path.is_symlink() or path.resolve() != path:
        raise ValueError('LINEAGE_NONCANONICAL_ACCOUNT_RISK_PATH')
    return path


def _terms(policy):
    return digest({k: policy[k] for k in ('account_suffix', 'account_capital_usd',
        'stop_new_entries_at_daily_equity_loss_usd', 'project_max_drawdown_fraction')})


@contextmanager
def _readonly(path):
    db = sqlite3.connect(Path(path).as_uri() + '?mode=ro', uri=True, timeout=.25)
    try:
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        yield db
    finally:
        db.close()


def _state(db):
    row = db.execute('SELECT payload FROM live_state WHERE id=1').fetchone()
    if not row:
        raise ValueError('LINEAGE_SOURCE_STATE_MISSING')
    return json.loads(row[0])


def _audit(db):
    audit = object.__new__(AuditTrail)
    audit.db = db
    return audit


def _policy_account(db, policy, account_key=None):
    state = _state(db)
    if state.get('policy_hash') != digest(policy) or state.get('simulation') is not False:
        raise ValueError('LINEAGE_SOURCE_POLICY_OR_MODE_CHANGED')
    if not state.get('account_key') or account_key is not None and state['account_key'] != account_key:
        raise ValueError('LINEAGE_SOURCE_ACCOUNT_MISMATCH')
    return state


def _fresh_current(path, policy, now, supplied=None):
    """Read the exact healthy, persisted, audited formal current observation."""
    path = _canonical_path(path)
    with _readonly(path) as db:
        _audit(db).verify()
        state = _policy_account(db, policy)
        current_row = db.execute('SELECT payload FROM broker_current WHERE id=1').fetchone()
        health_row = db.execute('SELECT payload FROM broker_health WHERE id=1').fetchone()
        if not current_row or not health_row:
            raise ValueError('LINEAGE_CURRENT_FORMAL_OBSERVATION_MISSING')
        current, health = json.loads(current_row[0]), json.loads(health_row[0])
        raw = {k: v for k, v in current.items() if k not in JOURNAL_ONLY}
        if supplied is not None and canonical({k: v for k, v in supplied.items() if k not in JOURNAL_ONLY}) != canonical(raw):
            raise ValueError('LINEAGE_SUPPLIED_SNAPSHOT_DIFFERS_FROM_JOURNAL')
        if (health.get('healthy') is not True or health.get('at') != raw.get('observed_at')
                or raw.get('provider') != 'robinhood' or raw.get('synthetic') is not False
                or raw.get('collection_complete') is not True
                or raw.get('account_key') != state['account_key']
                or raw.get('account_mask') != '••••' + policy['account_suffix']
                or not 0 <= (now - timestamp(raw['observed_at'])).total_seconds() <= 30
                or state.get('last_snapshot') and timestamp(raw['observed_at']) < timestamp(state['last_snapshot'])
                or current.get('reconciliation_issues')):
            raise ValueError('LINEAGE_CURRENT_FORMAL_OBSERVATION_INVALID')
        source_hash = hashlib.sha256(json.dumps(raw, sort_keys=True).encode()).hexdigest()
        observation = db.execute('SELECT payload FROM broker_observations WHERE digest=?', (source_hash,)).fetchone()
        observed = db.execute("SELECT payload FROM audit_events WHERE event_id=?", ('account-' + source_hash,)).fetchone()
        if not observation or canonical(json.loads(observation[0])) != canonical(raw) or not observed:
            raise ValueError('LINEAGE_CURRENT_SOURCE_PROOF_MISSING')
        event = json.loads(observed[0])
        if (event.get('event_type') != 'ACCOUNT_OBSERVED' or event.get('actor') != 'broker'
                or event.get('synthetic') is not False or event.get('account') != raw['account_mask']
                or event.get('occurred_at') != raw['observed_at']
                or event.get('details', {}).get('source_sha256') != source_hash):
            raise ValueError('LINEAGE_CURRENT_SOURCE_AUDIT_MISMATCH')
        return raw


def _flat(snapshot):
    if (any(number(p['quantity']) != 0 or number(p.get('held_for_sells', '0')) != 0 for p in snapshot['positions'].values())
            or any(o['state'] not in TERMINAL for o in snapshot['orders'].values())
            or any(g['state'] not in TERMINAL for g in snapshot.get('groups', []))
            or snapshot.get('other_asset_open_orders')
            or any(number(v) != 0 for v in snapshot.get('other_asset_values', {}).values())
            or snapshot.get('pending_approvals') or snapshot.get('pending_approval_ids')
            or snapshot.get('missing_approval_ids')):
        raise ValueError('LINEAGE_CURRENT_ACCOUNT_NOT_CLEAR')


class _ReadBook:
    def __init__(self, db, policy, now):
        self.db, self.policy, self.simulation = db, policy, False
        self.clock = lambda: now
        self.audit = _audit(db)

    def state(self):
        return _state(self.db)

    def actions(self):
        names = ('id', 'trade_id', 'kind', 'state', 'payload', 'created_at', 'updated_at', 'report')
        return [dict(zip(names, row)) for row in self.db.execute('SELECT * FROM live_actions ORDER BY created_at,id')]

    def _snapshot_check(self, snapshot, now):
        state = _policy_account(self.db, self.policy, snapshot.get('account_key'))
        if (snapshot.get('provider') != 'robinhood' or snapshot.get('synthetic') is not False
                or snapshot.get('collection_complete') is not True
                or snapshot.get('account_mask') != '••••' + self.policy['account_suffix']
                or not 0 <= (now - timestamp(snapshot['observed_at'])).total_seconds() <= 30
                or state.get('last_snapshot') and timestamp(snapshot['observed_at']) < timestamp(state['last_snapshot'])):
            raise ValueError('LINEAGE_SOURCE_FRESH_BINDING_INVALID')


def _refs(db):
    return sorted({row[0] for row in db.execute('SELECT ref_id FROM external_broker_refs')}
        | {row[0] for row in db.execute('SELECT broker_ref_id FROM external_operations WHERE broker_ref_id IS NOT NULL')})


def _approval_proof(snapshot, refs):
    if not set(refs) <= set(snapshot.get('requested_approval_ids', [])):
        raise ValueError('LINEAGE_PRIOR_APPROVAL_REFS_NOT_QUERIED')
    for ref in refs:
        approval = snapshot.get('approval_states', {}).get(ref, {})
        if (approval.get('approval_id') != ref or approval.get('is_edited') is not False
                or approval.get('asset_class') != 'ASSET_CLASS_EQUITY'
                or approval.get('state') not in APPROVAL_NO_ORDER_TERMINAL | {
                    'APPROVAL_STATE_APPROVED', 'APPROVAL_STATE_AUTO_APPROVED'}):
            raise ValueError('LINEAGE_PRIOR_APPROVAL_UNRESOLVED')


def _write_safety(db, snapshot, account_mask):
    """Inspect every native/manual write, including writes appended after ACK."""
    orders = snapshot['orders']
    events = [json.loads(row[0]) for row in db.execute('SELECT payload FROM audit_events ORDER BY sequence')]
    intents = {e['intent_id']: e for e in events if e.get('event_type') == 'AGENT_ACTION_INTENT'}
    results = {}
    for event in events:
        if event.get('event_type') == 'AGENT_ACTION_RESULT':
            results.setdefault(event['intent_id'], []).append(event)
    operations = {row[0]: row for row in db.execute(
        'SELECT operation_id,action_id,operation,report FROM external_operations')}
    state = _state(db)
    actions = {row[0]: row[1] for row in db.execute('SELECT id,trade_id FROM live_actions')}
    acknowledged_intents = set()
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='recovery_cases'").fetchone():
        for (encoded,) in db.execute("SELECT payload FROM recovery_cases WHERE status='ACKNOWLEDGED'"):
            acknowledged_intents.update(json.loads(encoded).get('intent_ids', []))

    def no_order(identity):
        operation = operations.get(identity)
        if not operation or operation[2] != 'place':
            return False
        aliases = [row[0] for row in db.execute('SELECT ref_id FROM external_broker_refs WHERE operation_id=?', (identity,))]
        if not aliases or db.execute('SELECT 1 FROM external_broker_order_ids WHERE operation_id=?', (identity,)).fetchone():
            return False
        return all(snapshot.get('approval_states', {}).get(alias, {}).get('state') in APPROVAL_NO_ORDER_TERMINAL
                   and not any(o.get('ref_id') == alias for o in orders.values()) for alias in aliases)

    def linked_orders(identity, intent):
        identities = {_result_order_id(e) for e in results.get(identity, [])}
        identities.discard(None)
        identities.update(row[0] for row in db.execute(
            'SELECT order_id FROM external_broker_order_ids WHERE operation_id=?', (identity,)))
        # Module dispatch has a durable logical action ref. It is evidence only
        # when that exact ref appears in the fresh broker order, never by ticker.
        if intent.get('ref_id'):
            identities.update(o['id'] for o in orders.values() if o.get('ref_id') == intent['ref_id'])
        return identities

    for identity, operation in operations.items():
        if operation[2] != 'review' and identity not in intents:
            raise ValueError('LINEAGE_PRIOR_NATIVE_INTENT_MISSING:' + identity)
    for identity, intent in intents.items():
        action = intent.get('details', {}).get('action')
        if action == 'review':
            continue
        if intent.get('synthetic') is not False or intent.get('account') != account_mask:
            raise ValueError('LINEAGE_PRIOR_WRITE_ACCOUNT_MISMATCH:' + identity)
        identities = linked_orders(identity, intent)
        if no_order(identity):
            continue
        action_id = operations[identity][1] if identity in operations else identity
        trade_id = actions.get(action_id)
        # A model-reported terminal order ID cannot by itself retire a new
        # manual write, particularly one appended to a recovered old action.
        # Existing audited recovery must cover it, or it must belong to a
        # normally completed module trade.
        if identity not in acknowledged_intents and (
                trade_id is None or state['trades'].get(trade_id, {}).get('phase') != 'FLAT_VERIFIED'):
            raise ValueError('LINEAGE_PRIOR_UNACKNOWLEDGED_WRITE:' + identity)
        if not identities or any(i not in orders or orders[i]['state'] not in TERMINAL for i in identities):
            raise ValueError('LINEAGE_PRIOR_UNKNOWN_OR_UNRESOLVED_WRITE:' + identity)
        if action == 'cancel' and identities != {intent['details']['parameters'].get('order_id')}:
            raise ValueError('LINEAGE_PRIOR_CANCEL_TARGET_MISMATCH:' + identity)


def _source_view(db, policy, snapshot, now):
    facade = _ReadBook(db, policy, now)
    facade.audit.verify()
    facade._snapshot_check(snapshot, now)
    refs = _refs(db)
    _approval_proof(snapshot, refs)
    recovery = recovery_disposition(facade, snapshot)
    if recovery['blockers']:
        raise ValueError('LINEAGE_PRIOR_RECOVERY_BLOCKED:' + ','.join(recovery['blockers']))
    state = facade.state()
    active = set(state['issues']) - set(recovery['covered_issue_ids'])
    if active:
        raise ValueError('LINEAGE_PRIOR_UNCOVERED_ISSUES:' + ','.join(sorted(active)))
    retired = set(recovery['acknowledged_trade_ids'])
    if any(t['phase'] != 'FLAT_VERIFIED' and key not in retired for key, t in state['trades'].items()):
        raise ValueError('LINEAGE_PRIOR_TRADE_NOT_CLOSED')
    if any(a['state'] in UNSETTLED and a['id'] not in recovery['acknowledged_action_ids'] for a in facade.actions()):
        raise ValueError('LINEAGE_PRIOR_UNRESOLVED_ACTION')
    _write_safety(db, snapshot, snapshot['account_mask'])
    return recovery


def _verify_record(db, record, snapshot):
    policy = record['policy']
    if digest(policy) != record['policy_sha256']:
        raise ValueError('LINEAGE_REGISTERED_POLICY_HASH_INVALID')
    state = _policy_account(db, policy, snapshot['account_key'])
    if record['account_key'] != state['account_key'] or record['account_mask'] != snapshot['account_mask']:
        raise ValueError('LINEAGE_REGISTERED_ACCOUNT_MISMATCH')
    _audit(db).verify()
    sequence = record['audit_sequence']
    row = db.execute('SELECT entry_hash FROM audit_events WHERE sequence=?', (sequence,)).fetchone() if sequence else None
    if sequence and (not row or row[0] != record['audit_head_hash']) or not sequence and record['audit_head_hash'] != ZERO_HASH:
        raise ValueError('LINEAGE_SOURCE_AUDIT_PREFIX_CHANGED')
    if sequence and db.execute('SELECT coalesce(max(sequence),0) FROM audit_events').fetchone()[0] < sequence:
        raise ValueError('LINEAGE_SOURCE_AUDIT_TRUNCATED')
    return state


def _read_registry(db, policy, account_key=None):
    risk_row = db.execute('SELECT payload FROM account_risk WHERE id=1').fetchone()
    risk = json.loads(risk_row[0]) if risk_row else {}
    if (risk.get('terms') != _terms(policy) or risk.get('simulation') is not False or not risk.get('account_key')
            or account_key is not None and risk['account_key'] != account_key):
        raise ValueError('LINEAGE_CANONICAL_RISK_BINDING_INVALID')
    head = db.execute('SELECT sequence,entry_hash FROM session_lineage_head WHERE id=1').fetchone()
    if not head:
        raise ValueError('LINEAGE_REGISTRY_UNINITIALIZED')
    previous, count, records = ZERO_HASH, 0, []
    for sequence, encoded, previous_hash, entry_hash in db.execute(
            'SELECT sequence,payload,previous_hash,entry_hash FROM session_lineage_events ORDER BY sequence'):
        payload = json.loads(encoded)
        if (sequence != count + 1 or canonical(payload) != encoded or previous_hash != previous
                or digest({'sequence': sequence, 'previous_hash': previous_hash, 'event': payload}) != entry_hash
                or payload.get('account_key') != risk['account_key'] or payload.get('terms') != risk['terms']
                or payload.get('kind') != ('BOOTSTRAP' if count == 0 else 'ENROLL')):
            raise ValueError('LINEAGE_REGISTRY_CHAIN_INVALID')
        record = payload['session']
        if _canonical_path(record['db']) != Path(record['db']) or record['account_key'] != risk['account_key']:
            raise ValueError('LINEAGE_REGISTRY_SOURCE_BINDING_INVALID')
        if any(r['db'] == record['db'] for r in records):
            raise ValueError('LINEAGE_DUPLICATE_REGISTERED_SOURCE')
        if records and record['predecessor_db'] != records[-1]['db'] or not records and record['predecessor_db'] is not None:
            raise ValueError('LINEAGE_REGISTRY_PREDECESSOR_INVALID')
        records.append(record)
        count, previous = sequence, entry_hash
    if count != head[0] or previous != head[1] or not count:
        raise ValueError('LINEAGE_REGISTRY_HEAD_MISMATCH')
    return records, risk, head


def _checked_sources(records, snapshot, now, *, current_path=None):
    for record in records:
        path = _canonical_path(record['db'])
        with _readonly(path) as db:
            _verify_record(db, record, snapshot)
            if path != current_path:
                _source_view(db, record['policy'], snapshot, now)


def _registration_record(path, policy, snapshot, now, predecessor):
    if policy.get('capture_only') is True:
        raise ValueError('LINEAGE_CAPTURE_CANNOT_REGISTER_EXECUTION')
    with _readonly(path) as db:
        _policy_account(db, policy, snapshot['account_key'])
        _source_view(db, policy, snapshot, now)
        row = db.execute('SELECT sequence,entry_hash FROM audit_events ORDER BY sequence DESC LIMIT 1').fetchone()
        return {'db': str(path), 'policy': policy, 'policy_sha256': digest(policy),
            'account_key': snapshot['account_key'], 'account_mask': snapshot['account_mask'],
            'audit_sequence': row[0] if row else 0, 'audit_head_hash': row[1] if row else ZERO_HASH,
            'predecessor_db': str(predecessor) if predecessor else None}


def _append_registration(db, kind, record, snapshot, current_path, current_policy, now, previous):
    sequence = previous[0] + 1
    event = {'kind': kind, 'registered_at': now.isoformat(), 'account_key': snapshot['account_key'],
        'terms': _terms(record['policy']), 'session': record,
        'current_snapshot_sha256': digest(snapshot), 'current_source_db': str(current_path),
        'current_source_policy_sha256': digest(current_policy)}
    entry_hash = digest({'sequence': sequence, 'previous_hash': previous[1], 'event': event})
    db.execute('INSERT INTO session_lineage_events VALUES(?,?,?,?)',
               (sequence, canonical(event), previous[1], entry_hash))
    db.execute('INSERT INTO session_lineage_head VALUES(1,?,?) ON CONFLICT(id) DO UPDATE SET sequence=excluded.sequence,entry_hash=excluded.entry_hash',
               (sequence, entry_hash))
    return {'registered': True, 'db': record['db'], 'sequence': sequence, 'entry_hash': entry_hash,
            'snapshot_at': snapshot['observed_at'], 'automatic_execution_enabled': False}


def bootstrap_session_lineage(source_db, source_policy, *, current_db, current_policy, clock=None):
    """Explicitly anchor the original execution history; never called by a gate."""
    now = _now(clock)
    source = _canonical_path(source_db)
    current = _canonical_path(current_db)
    if source_policy['account_suffix'] != current_policy['account_suffix'] or _terms(source_policy) != _terms(current_policy):
        raise ValueError('LINEAGE_REGISTRATION_POLICY_ACCOUNT_MISMATCH')
    snapshot = _fresh_current(current, current_policy, now)
    _flat(snapshot)
    record = _registration_record(source, source_policy, snapshot, now, None)
    risk_path = _risk_path(source_policy)
    # mode=rw refuses to silently create a replacement account risk sidecar.
    db = sqlite3.connect(risk_path.as_uri() + '?mode=rw', uri=True, timeout=.25)
    try:
        db.execute('BEGIN IMMEDIATE')
        risk_row = db.execute('SELECT payload FROM account_risk WHERE id=1').fetchone()
        if not risk_row:
            raise ValueError('LINEAGE_CANONICAL_RISK_BINDING_INVALID')
        risk = json.loads(risk_row[0])
        if risk.get('account_key') != snapshot['account_key'] or risk.get('terms') != _terms(source_policy) or risk.get('simulation') is not False:
            raise ValueError('LINEAGE_CANONICAL_RISK_BINDING_INVALID')
        db.execute('CREATE TABLE IF NOT EXISTS session_lineage_events(sequence INTEGER PRIMARY KEY,payload TEXT NOT NULL,previous_hash TEXT NOT NULL,entry_hash TEXT NOT NULL)')
        db.execute('CREATE TABLE IF NOT EXISTS session_lineage_head(id INTEGER PRIMARY KEY CHECK(id=1),sequence INTEGER NOT NULL,entry_hash TEXT NOT NULL)')
        db.execute("CREATE TRIGGER IF NOT EXISTS session_lineage_no_update BEFORE UPDATE ON session_lineage_events BEGIN SELECT RAISE(ABORT,'session lineage is append-only'); END")
        db.execute("CREATE TRIGGER IF NOT EXISTS session_lineage_no_delete BEFORE DELETE ON session_lineage_events BEGIN SELECT RAISE(ABORT,'session lineage is append-only'); END")
        if db.execute('SELECT 1 FROM session_lineage_events').fetchone() or db.execute('SELECT 1 FROM session_lineage_head').fetchone():
            raise ValueError('LINEAGE_BOOTSTRAP_ALREADY_EXISTS')
        result = _append_registration(db, 'BOOTSTRAP', record, snapshot, current, current_policy, now, (0, ZERO_HASH))
        db.commit()
        return result
    except BaseException:
        db.rollback()
        raise
    finally:
        db.close()


def enroll_session_lineage(next_db, next_policy, *, predecessor_db, current_db, current_policy, clock=None):
    """Enroll a new immutable book only after every registered source is clear."""
    now = _now(clock)
    path, predecessor, current = map(_canonical_path, (next_db, predecessor_db, current_db))
    if _canonical_path(next_policy.get('prior_execution_evidence_db', '')) != predecessor:
        raise ValueError('LINEAGE_DECLARED_PREDECESSOR_MISMATCH')
    if _terms(next_policy) != _terms(current_policy):
        raise ValueError('LINEAGE_REGISTRATION_POLICY_ACCOUNT_MISMATCH')
    snapshot = _fresh_current(current, current_policy, now)
    _flat(snapshot)
    db = sqlite3.connect(_risk_path(next_policy).as_uri() + '?mode=rw', uri=True, timeout=.25)
    try:
        db.execute('BEGIN IMMEDIATE')
        records, risk, head = _read_registry(db, next_policy, snapshot['account_key'])
        if records[-1]['db'] != str(predecessor) or any(r['db'] == str(path) for r in records):
            raise ValueError('LINEAGE_ENROLLMENT_PREDECESSOR_OR_DUPLICATE')
        _checked_sources(records, snapshot, now)
        record = _registration_record(path, next_policy, snapshot, now, predecessor)
        result = _append_registration(db, 'ENROLL', record, snapshot, current, current_policy, now, head)
        db.commit()
        return result
    except BaseException:
        db.rollback()
        raise
    finally:
        db.close()


def _book_paths(book):
    path = _canonical_path(book.db.execute('PRAGMA database_list').fetchone()[2])
    risk = Path(book.risk_guard.db.execute('PRAGMA database_list').fetchone()[2]).resolve()
    if risk != _risk_path(book.policy):
        raise ValueError('LINEAGE_NONCANONICAL_ACCOUNT_RISK_PATH')
    return path


def lineage_entry_gate(book, snapshot=None):
    """Deny new risk on any missing lineage/freshness proof; never mutate books."""
    if book.simulation:
        return {'blocked': False, 'reason': 'SIMULATION_LINEAGE_NOT_REQUIRED', 'blockers': []}
    if book.policy.get('capture_only') is True:
        return {'blocked': True, 'reason': 'CAPTURE_ONLY_POLICY_READ_ONLY', 'blockers': []}
    try:
        path = _book_paths(book)
        now = book.clock()
        current = _fresh_current(path, book.policy, now, supplied=snapshot)
        with _readonly(_risk_path(book.policy)) as registry:
            records, risk, head = _read_registry(registry, book.policy, current['account_key'])
        registered = next((record for record in records if record['db'] == str(path)), None)
        if registered is None or registered['policy_sha256'] != digest(book.policy):
            raise ValueError('LINEAGE_CURRENT_SESSION_NOT_REGISTERED')
        if registered['db'] != records[-1]['db']:
            raise ValueError('LINEAGE_SESSION_SUPERSEDED_BY_REGISTERED_SUCCESSOR')
        if registered['predecessor_db'] is not None and _canonical_path(book.policy.get('prior_execution_evidence_db', '')) != Path(registered['predecessor_db']):
            raise ValueError('LINEAGE_DECLARED_PREDECESSOR_MISMATCH')
        _checked_sources(records, current, now, current_path=path)
        return {'blocked': False, 'reason': 'LINEAGE_CURRENT_FRESH_SOURCES_VERIFIED', 'blockers': [],
                'registered_sessions': len(records), 'registry_sequence': head[0],
                'registry_head_hash': head[1], 'snapshot_at': current['observed_at'],
                'automatic_execution_enabled': False}
    except (OSError, sqlite3.Error, ValueError, KeyError, TypeError, TransportError) as exc:
        reason = str(exc) if isinstance(exc, (ValueError, TransportError)) else 'LINEAGE_EVIDENCE_UNAVAILABLE_OR_INVALID'
        return {'blocked': True, 'reason': reason, 'blockers': [reason], 'automatic_execution_enabled': False}


def lineage_approval_ids(book):
    """Return known registered source aliases for the next formal read.

    This read alone is never an admission proof. Missing registry remains an
    explicit gate blocker, even though the collector can still read the account.
    """
    if book.simulation or book.policy.get('capture_only') is True:
        return []
    try:
        _book_paths(book)
        with _readonly(_risk_path(book.policy)) as db:
            records, risk, head = _read_registry(db, book.policy)
        refs = set()
        for record in records:
            with _readonly(_canonical_path(record['db'])) as db:
                _policy_account(db, record['policy'], risk['account_key'])
                _audit(db).verify()
                refs.update(_refs(db))
        return sorted(refs)
    except (OSError, sqlite3.Error, ValueError, KeyError, TypeError, TransportError):
        return []
