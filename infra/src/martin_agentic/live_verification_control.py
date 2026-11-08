"""Project-local user stop for real entries, never broker authority.

Canonical live books default to held; engineering grants need a new timestamp. Session policies and per-book gate release
cannot override this control. Noncanonical fixtures retain their existing
lineage checks; a not-applicable result is not permission to execute elsewhere.
"""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import stat

from .ledger import timestamp
from .qualification import candidate_root


CANONICAL_RUNTIME = candidate_root() / 'runtime'
CONTROL_NAME = 'live-verification-control.json'
MAX_BYTES = 16384


def _read_control():
    # Open the trusted project directory and reject symlink/nonregular controls,
    # including FIFOs, without blocking. There is no caller-selected control path.
    directory = os.open(CANONICAL_RUNTIME, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        fd = os.open(CONTROL_NAME, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
    finally:
        os.close(directory)
    try:
        before = os.fstat(fd)
        if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
                or before.st_mode & 0o077 or not 0 < before.st_size <= MAX_BYTES):
            raise ValueError('LIVE_VERIFICATION_CONTROL_INVALID_FILE')
        raw = os.read(fd, MAX_BYTES + 1)
        after = os.fstat(fd)
        if (len(raw) != before.st_size or before.st_size != after.st_size
                or before.st_mtime_ns != after.st_mtime_ns):
            raise ValueError('LIVE_VERIFICATION_CONTROL_CHANGED')
    finally:
        os.close(fd)
    value = json.loads(raw)
    if (not isinstance(value, dict) or type(value.get('schema_version')) is not int
            or value['schema_version'] != 1
            or type(value.get('live_trading_enabled')) is not bool
            or type(value.get('live_engineering_verification_enabled')) is not bool
            or not isinstance(value.get('reason'), str) or not value['reason'].strip()):
        raise ValueError('LIVE_VERIFICATION_CONTROL_INVALID_SCHEMA')
    if any(not isinstance(value.get(key), str) for key in ('recorded_at', 'grants_not_before')):
        raise ValueError('LIVE_VERIFICATION_CONTROL_TIMESTAMP_REQUIRED')
    recorded = timestamp(value['recorded_at'])
    boundary = timestamp(value['grants_not_before'])
    if boundary > recorded:
        raise ValueError('LIVE_VERIFICATION_CONTROL_INVALID_BOUNDARY')
    return value, recorded, boundary


def engineering_verification_gate(db_path, *, simulation, purpose, evidence=None, clock=None):
    result = {'blocked': False, 'applicable': False,
              'execution_authorized': False, 'scope': 'real_entry_user_stop'}
    if simulation is not False:
        return result
    if Path(db_path).resolve().parent != CANONICAL_RUNTIME.resolve():
        return {**result, 'reason': 'NONCANONICAL_RETAINS_EXISTING_LINEAGE_CHECKS'}
    result['applicable'] = True
    try:
        control, recorded, boundary = _read_control()
        now = (clock or (lambda: datetime.now(timezone.utc)))()
        if recorded > now:
            raise ValueError('LIVE_VERIFICATION_CONTROL_FUTURE')
    except (OSError, ValueError, TypeError, KeyError, UnicodeError):
        return {**result, 'blocked': True, 'reason': 'LIVE_VERIFICATION_CONTROL_MISSING_OR_INVALID'}
    if control['live_trading_enabled'] is not True:
        return {**result, 'blocked': True, 'reason': 'LIVE_TRADING_USER_HOLD'}
    if purpose == 'strategy':
        return {**result, 'reason': 'LOCAL_USER_HOLD_CLEAR_NOT_EXECUTION_AUTHORITY'}
    if purpose != 'engineering':
        return {**result, 'blocked': True, 'reason': 'EXPLICIT_TRADE_PURPOSE_REQUIRED'}
    if control['live_engineering_verification_enabled'] is not True:
        return {**result, 'blocked': True, 'reason': 'LIVE_ENGINEERING_VERIFICATION_USER_HOLD'}
    try:
        authorized_at = timestamp((evidence or {})['parameters_authorized_at'])
        if not boundary <= authorized_at <= now:
            raise ValueError('OLD_OR_FUTURE_ENGINEERING_GRANT')
    except (ValueError, TypeError, KeyError, AttributeError):
        return {**result, 'blocked': True, 'reason': 'NEW_ENGINEERING_AUTHORIZATION_REQUIRED'}
    return {**result, 'reason': 'LOCAL_USER_HOLD_CLEAR_NOT_EXECUTION_AUTHORITY'}
