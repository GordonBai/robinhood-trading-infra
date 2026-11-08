"""One answer to "may this account take new risk or settle a trade?".

Two durable witnesses exist, and they are not interchangeable:

* the ``execution_integrity_faults`` row, written when the database lets us
  write it, and
* the sticky ``<db>.entry-inhibit.json`` side-car, written when even the
  fallback row could not be committed. It records
  ``EXECUTION_INTEGRITY_FAULT_UNRECORDED``: a conflict was detected, but the
  authoritative fault could not be stored.

The side-car must not be mistaken for an ordinary hold. An ordinary inhibit
means "an operator has not released this gate"; the integrity side-car means
"the account's execution facts are unknown". Both stop new entries, but only
the second one also means settlement cannot be decided and the session must
report itself as needing reconciliation.

This module reads both witnesses and fails closed when either cannot be read.
It never clears anything: there is no reconciliation authority in this process.
"""
import json
import sqlite3
from pathlib import Path

from trading_infra.integrity_faults import unresolved_faults

READ_ERRORS = (ValueError, KeyError, TypeError, OSError, sqlite3.Error)

SIDECAR_ISSUE = 'EXECUTION_INTEGRITY_FAULT_UNRECORDED'
SIDECAR_SUFFIX = '.entry-inhibit.json'


def _db_path(db, db_path):
    if db_path:
        return Path(db_path).resolve()
    row = db.execute('PRAGMA database_list').fetchone()
    if not row or not row[2]:
        return None
    return Path(row[2]).resolve()


def read_integrity(db, *, db_path=None, clock=None):
    """Return the account's durable integrity state.

    ``known`` is False when a witness exists in principle but cannot be read;
    ``blocked`` is then True regardless of what could be read. ``observed_at``
    keeps the original detection time; a later read never re-stamps an old
    fact as new.
    """
    state = {'blocked': False, 'known': True, 'reasons': [], 'faults': [],
             'sidecar': None, 'other_inhibit_issues': [], 'observed_at': None,
             'ledger_error': None, 'sidecar_error': None}
    try:
        faults = unresolved_faults(db)
    except READ_ERRORS as exc:
        state['known'] = False
        state['ledger_error'] = str(exc)
        state['reasons'].append('EXECUTION_INTEGRITY_LEDGER_UNAVAILABLE')
    else:
        state['faults'] = faults
        if faults:
            state['blocked'] = True
            state['reasons'].append('EXECUTION_INTEGRITY_FAULT_UNRESOLVED')
            state['observed_at'] = faults[0].get('first_seen_at')

    try:
        path = _db_path(db, db_path)
    except READ_ERRORS:
        path = None
        state['known'] = False
        state['sidecar_error'] = 'EXECUTION_INTEGRITY_DB_PATH_UNAVAILABLE'
        state['reasons'].append('EXECUTION_INTEGRITY_DB_PATH_UNAVAILABLE')
    sidecar_path = None if path is None else Path(str(path) + SIDECAR_SUFFIX)
    if sidecar_path is not None and sidecar_path.exists():
        try:
            value = json.loads(sidecar_path.read_text())
            if not isinstance(value, dict) or value.get('db') != str(path):
                raise ValueError('INTEGRITY_SIDECAR_BINDING_INVALID')
            issues = [item for item in (value.get('issues') or [])
                      if isinstance(item, str)]
            state['sidecar'] = {'path': str(sidecar_path), 'at': value.get('at'),
                                'issues': issues}
            if SIDECAR_ISSUE in issues:
                # Detected, but not durably recorded. The account's execution
                # facts are unknown even though the gate file is readable, and
                # no clean snapshot may clear it.
                state['blocked'] = True
                state['reasons'].append('EXECUTION_INTEGRITY_SIDECAR_UNRECORDED_FAULT')
                state['observed_at'] = value.get('at') or state['observed_at']
            else:
                state['other_inhibit_issues'] = issues
        except READ_ERRORS as exc:
            state['known'] = False
            state['sidecar_error'] = str(exc)
            state['reasons'].append('EXECUTION_INTEGRITY_SIDECAR_UNREADABLE')

    if not state['known']:
        state['blocked'] = True
    if state['blocked'] and not state['reasons']:
        state['reasons'].append('EXECUTION_INTEGRITY_FAULT_UNRESOLVED')
    return state


def integrity_blocked(db, *, db_path=None, clock=None):
    """Convenience: ``(blocked, unknown, state)`` for gate call sites."""
    state = read_integrity(db, db_path=db_path, clock=clock)
    return state['blocked'], not state['known'], state
