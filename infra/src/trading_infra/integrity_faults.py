"""Durable account-level execution-integrity faults.

An execution-identity conflict makes the account's execution facts unreliable:
the same broker execution is claimed twice, so neither the cash nor the position
derived from it can be trusted until a human reconciles it.

That fact belongs to the base infrastructure, not to the strategy-instance layer.
The earlier implementation kept it in a process-local flag, which meant a
restart with a clean snapshot looked healthy again. This ledger is a row in the
owner's database, so every supervisor, connection and session reads the same
answer.

Nothing here clears a fault: this process has no reconciliation authority, and
inventing one would let a clean snapshot silently reopen risk.
"""
from datetime import datetime, timezone

from trading_contracts.values import canonical, digest, timestamp
from .transactions import atomic

SCHEMA = '''
CREATE TABLE IF NOT EXISTS execution_integrity_faults(
    fault_id TEXT PRIMARY KEY, account_key TEXT, provider TEXT NOT NULL,
    source TEXT NOT NULL, execution_id TEXT, evidence TEXT NOT NULL,
    occurred_at TEXT, observed_at TEXT NOT NULL, first_seen_at TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('UNRESOLVED')));
'''


def unresolved_faults(db):
    """Read the unresolved rows without creating or writing anything.

    Read paths (entry, claim, admit, settle) must not take a schema lock or
    join a caller transaction just to ask a question. A database that never
    created the table has no recorded fault; any other read failure propagates
    so the caller can fail closed.
    """
    import json
    import sqlite3
    try:
        rows = db.execute(
            'SELECT fault_id,account_key,provider,source,execution_id,evidence,'
            ' occurred_at,observed_at,first_seen_at,state'
            ' FROM execution_integrity_faults WHERE state=?'
            ' ORDER BY first_seen_at,fault_id', ('UNRESOLVED',)).fetchall()
    except sqlite3.OperationalError as exc:
        if 'no such table' in str(exc).lower():
            return []
        raise
    return [{'fault_id': row[0], 'account_key': row[1], 'provider': row[2],
             'source': row[3], 'execution_id': row[4],
             'evidence': json.loads(row[5]), 'occurred_at': row[6],
             'observed_at': row[7], 'first_seen_at': row[8], 'state': row[9]}
            for row in rows]


class ExecutionIntegrityFaults:
    """Append-only unresolved execution faults, readable by any owner session."""

    def __init__(self, db, *, clock=None):
        if db is None:
            raise ValueError('INTEGRITY_FAULTS_DB_REQUIRED')
        self.db = db
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        # One statement through ``execute``: ``executescript`` implicitly
        # commits, which would split a transaction the caller already owns.
        self.db.execute(SCHEMA.strip().rstrip(';'))

    def _now(self):
        return self.clock().astimezone(timezone.utc).isoformat()

    @staticmethod
    def fault_id(*, account_key, provider, source, execution_id):
        """One identity per disputed execution, so a replay is idempotent."""
        return digest(['execution-integrity-fault-1', account_key, provider, source,
                       execution_id])

    def record(self, *, source, provider='robinhood', account_key=None,
               execution_id=None, evidence=None, occurred_at=None,
               observed_at=None):
        """Persist one unresolved fault before the caller returns it.

        ``account_key`` and ``execution_id`` may be genuinely unknown; they are
        stored as NULL rather than guessed. The evidence dictionary carries what
        the caller actually had.
        """
        if not isinstance(source, str) or not source:
            raise ValueError('INTEGRITY_FAULT_SOURCE_REQUIRED')
        if not isinstance(provider, str) or not provider:
            raise ValueError('INTEGRITY_FAULT_PROVIDER_REQUIRED')
        fault_id = self.fault_id(account_key=account_key, provider=provider,
                                 source=source, execution_id=execution_id)
        body = canonical(evidence or {})
        with atomic(self.db):
            self.db.execute(
                'INSERT OR IGNORE INTO execution_integrity_faults(fault_id,'
                ' account_key,provider,source,execution_id,evidence,occurred_at,'
                ' observed_at,first_seen_at,state) VALUES(?,?,?,?,?,?,?,?,?,?)',
                (fault_id, account_key, provider, source, execution_id, body,
                 occurred_at, observed_at or self._now(), self._now(), 'UNRESOLVED'))
        return fault_id

    def unresolved(self):
        return unresolved_faults(self.db)

    def count(self):
        return self.db.execute(
            'SELECT count(*) FROM execution_integrity_faults WHERE state=?',
            ('UNRESOLVED',)).fetchone()[0]

    def blocks(self):
        """True when any unresolved fault forbids new entries and settlement."""
        return self.count() > 0
