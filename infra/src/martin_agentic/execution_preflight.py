"""Strictly read-only inventory for a future deterministic execution adapter.

This is not a risk engine, an action claimant, or authorization to trade. In
particular, it never constructs ExecutionBook (whose constructor writes schema
and opens the account risk sidecar), nor validates actions through mutating APIs.
"""
from datetime import datetime, timezone
from contextlib import contextmanager
import json
from pathlib import Path
import tempfile
import time

from .audit import AuditTrail, digest
from .capabilities import capability_blockers, capability_report
from .ledger import timestamp
from .native_operations import sanitized
from .capture_archive import _private_snapshot


class _ReadOnlyAudit(AuditTrail):
    def __init__(self, connection):
        # verify() only reads. Do not run the base schema/trigger constructor.
        self.db = connection


@contextmanager
def _snapshot(source):
    # Reuse bounded incremental backup, so full audit validation never pins a
    # read transaction on the live ROLLBACK-journal execution database.
    with tempfile.TemporaryDirectory(prefix='martin-script-preflight-') as directory:
        db, _, metadata = _private_snapshot(source, Path(directory), 30)
        deadline = time.monotonic() + 30
        db.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
        try:
            yield db, metadata
        finally:
            db.close()


def script_execution_preflight(db_path, policy, inventory, *, as_of=None, limit=100):
    """Observe queued actions without claiming, reconciling or approving them.

    Saved official metadata is diagnostic evidence, not a live permission grant.
    No formally qualified unattended adapter is currently registered. There is
    intentionally no JSON flag that can turn this report into live authority.
    """
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError('INVALID_SCRIPT_PREFLIGHT_LIMIT')
    now = timestamp(as_of) if as_of else datetime.now(timezone.utc)
    contract = capability_report(inventory, clock=lambda: now)
    source = Path(db_path).expanduser().resolve(strict=True)
    with _snapshot(source) as (db, snapshot_metadata):
        try:
            db.execute('PRAGMA query_only=ON')
            db.execute('BEGIN')
            state = json.loads(db.execute('SELECT payload FROM live_state WHERE id=1').fetchone()[0])
            if state.get('policy_hash') != digest(policy):
                raise ValueError('EXECUTION_POLICY_OR_MODE_CHANGED')
            if type(state.get('simulation')) is not bool:
                raise ValueError('EXECUTION_PROVENANCE_UNKNOWN')
            audit = _ReadOnlyAudit(db).verify()
            total = db.execute("SELECT count(*) FROM live_actions WHERE state IN ('READY','WAITING_USER','RUNNING','UNKNOWN','REPORTED')").fetchone()[0]
            rows = db.execute("""SELECT id, trade_id, kind, state, payload FROM live_actions
                WHERE state IN ('READY','WAITING_USER','RUNNING','UNKNOWN','REPORTED')
                ORDER BY CASE kind WHEN 'protect' THEN 0 WHEN 'cancel' THEN 1
                    WHEN 'exit' THEN 2 ELSE 3 END, created_at, id LIMIT ?""", (limit,)).fetchall()
            current = None
            if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='broker_current'").fetchone():
                row = db.execute('SELECT payload FROM broker_current WHERE id=1').fetchone()
                current = json.loads(row[0]) if row else None
        finally:
            db.rollback()
    # The connection is closed before formatting or publishing any report.
    actions = []
    for identity, trade, kind, state_name, raw in rows:
        payload = json.loads(raw)
        if payload.get('synthetic') is not state['simulation']:
            raise ValueError('SCRIPT_ACTION_PROVENANCE_MISMATCH')
        if payload.get('account') != '••••' + policy['account_suffix']:
            raise ValueError('SCRIPT_ACTION_ACCOUNT_MISMATCH')
        if (digest(payload.get('policy')) != digest(policy)
                or payload.get('action_id') != identity
                or payload.get('trade_id') != trade or payload.get('kind') != kind):
            raise ValueError('SCRIPT_ACTION_BINDING_MISMATCH')
        expired = now >= timestamp(payload['expires_at'])
        actions.append({'action_id': identity, 'trade_id': trade, 'kind': kind,
            'state': state_name, 'parameters': sanitized(payload['parameters']),
            'ref_id': payload['ref_id'], 'expires_at': payload['expires_at'],
            'expired': expired, 'payload_sha256': digest(payload),
            'disposition': 'RECONCILE_ONLY_NO_REPLAY' if state_name in {'RUNNING','UNKNOWN','REPORTED'}
                else 'EXPIRED_NO_DISPATCH' if expired else 'SHADOW_ONLY_NOT_VALIDATED',
            'risk_validation_performed': False, 'dispatch_allowed': False})
    blockers = capability_blockers(contract)
    if policy.get('live_enabled') is not True:
        blockers.append('POLICY_LIVE_DISABLED')
    if policy.get('capture_only') is True:
        blockers.append('CAPTURE_ONLY_POLICY_READ_ONLY')
    age = None
    if current:
        age = (now - timestamp(current['observed_at'])).total_seconds()
    if age is None or not 0 <= age <= 30:
        blockers.append('BROKER_OBSERVATION_NOT_CURRENT')
    if current and current.get('account_key') != state.get('account_key'):
        blockers.append('BROKER_OBSERVATION_ACCOUNT_BINDING_MISMATCH')
    return {'schema_version': 1, 'mode': 'read_only_shadow', 'as_of': now.isoformat(),
        'db': str(source), 'account': '••••' + policy['account_suffix'],
        'private_snapshot': snapshot_metadata,
        'simulation': state['simulation'], 'policy_sha256': digest(policy),
        'status': 'BLOCKED_CAPABILITY', 'blockers': blockers,
        'contract_evidence_sha256': contract['metadata_sha256'],
        'contract_evidence_captured_at': contract['metadata_captured_at'],
        'broker_snapshot_age_seconds': age, 'audit': audit,
        'queued_action_count': total, 'actions': actions, 'truncated': total > len(actions),
        'source_issues_preserved': state.get('issues', []),
        'shadow_is_not_live_admission': True, 'risk_and_lineage_admission_performed': False,
        'broker_calls': 0, 'broker_writes': 0, 'book_mutations': 0, 'model_calls': 0,
        'execution_authorized': False, 'production_ready': False}
