"""Read-only source/risk/Host/lineage evidence for a baseline plan.

This module reads a stopped local runtime boundary. It does not open a broker,
market, MCP server, Host process, or network connection; it does not create
tables, initialize risk, repair SQLite, enroll lineage, or authorize an order.

The returned dictionary contains derived baseline evidence plus reader
provenance. External expected pins are inputs; the values in the returned
evidence are measured from the controlled files, never copied from those pins.
The reader result is a local consistency proof, not a broker signature and not
order authority.
"""
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat

from .live_verification_control import CANONICAL_RUNTIME
from .risk import number
from .session_control import process_matches
from .session_lineage import ZERO_HASH, _audit, _terms
from .sqlite_readonly import SQLiteReadOnlyError, readonly_uri
from . import runtime_checkpoint as checkpoint
from trading_contracts.values import canonical, digest, timestamp


MAX_FILE_BYTES = 64 * 1024 * 1024
MAX_MANIFEST_BYTES = 8 * 1024 * 1024
SHA_RE = re.compile(r'^[0-9a-f]{64}$')
SUFFIX_RE = re.compile(r'^\d{4}$')
EXPECTED_FIELDS = (
    'source_fingerprint', 'policy_sha256', 'account_key', 'account_suffix',
    'execution_db_sha256', 'risk_sidecar_sha256', 'host_manifest_sha256',
    'lineage_head_sha256', 'state_generation', 'terms_sha256', 'host_epoch',
)
RECORD_FIELDS = (
    'db', 'policy', 'policy_sha256', 'account_key', 'account_mask',
    'audit_sequence', 'audit_head_hash', 'predecessor_db',
)
_CURRENT_OWNER = ContextVar('source_evidence_owner', default=None)


class SourceEvidenceError(ValueError):
    def __init__(self, code, detail=None):
        self.code = code
        self.detail = detail
        super().__init__(code)

    def __str__(self):
        return self.code if self.detail is None else f'{self.code}:{self.detail}'


def _fail(code, detail=None):
    raise SourceEvidenceError(code, detail)


def _sha(value, code):
    if not isinstance(value, str) or SHA_RE.fullmatch(value) is None:
        _fail(code)
    return value


def _time(value, code):
    try:
        return timestamp(value)
    except (TypeError, ValueError, OverflowError):
        _fail(code)


def _file_sha(path):
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError:
        _fail('SOURCE_FILE_INVALID', str(path))
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_FILE_BYTES:
            _fail('SOURCE_FILE_INVALID', str(path))
        h = hashlib.sha256()
        size = 0
        while True:
            block = os.read(fd, 1024 * 1024)
            if not block:
                break
            size += len(block)
            if size > MAX_FILE_BYTES:
                _fail('SOURCE_FILE_TOO_LARGE', str(path))
            h.update(block)
        if size != info.st_size:
            _fail('SOURCE_FILE_CHANGED_DURING_READ', str(path))
    except OSError:
        _fail('SOURCE_FILE_INVALID', str(path))
    finally:
        os.close(fd)
    return h.hexdigest(), size


def _file_stat(path):
    try:
        info = Path(path).stat()
    except OSError:
        _fail('SOURCE_FILE_INVALID', str(path))
    return {'dev': info.st_dev, 'ino': info.st_ino, 'size': info.st_size,
            'mtime_ns': info.st_mtime_ns, 'mode': info.st_mode,
            'nlink': info.st_nlink}


def _permission_ok(path, code):
    try:
        mode = Path(path).lstat().st_mode
    except OSError:
        _fail(code, str(path))
    if ((not stat.S_ISREG(mode) and not stat.S_ISDIR(mode))
            or mode & 0o022):
        _fail(code, str(path))


def _safe_path(root, value, *, kind='file'):
    root_input = Path(root)
    if root_input.is_symlink() or not root_input.is_dir():
        _fail('SOURCE_ROOT_INVALID')
    _permission_ok(root_input, 'SOURCE_ROOT_PERMISSIONS_UNSAFE')
    root_resolved = root_input.resolve()
    candidate = Path(value)
    if candidate.is_absolute():
        candidate_abs = candidate
    else:
        candidate_abs = root_input / candidate
    try:
        resolved = candidate_abs.resolve()
        resolved_relative = resolved.relative_to(root_resolved)
    except ValueError:
        _fail('SOURCE_PATH_ESCAPE', str(candidate))
    try:
        lexical_relative = candidate_abs.relative_to(root_input)
        lexical = candidate_abs
        base = root_input
    except ValueError:
        lexical_relative = resolved_relative
        lexical = root_resolved / resolved_relative
        base = root_resolved
    if any(part in {'', '.', '..'} for part in lexical_relative.parts):
        _fail('SOURCE_PATH_ESCAPE', str(lexical))
    current = base
    for part in lexical_relative.parts:
        current = current / part
        if current.is_symlink():
            _fail('SOURCE_SYMLINK', str(current))
    if candidate.is_absolute() and candidate.is_symlink():
        _fail('SOURCE_SYMLINK', str(candidate))
    if kind == 'file':
        if not lexical.is_file() or lexical.is_symlink():
            _fail('SOURCE_FILE_INVALID', str(lexical))
        _permission_ok(lexical, 'SOURCE_PERMISSIONS_UNSAFE')
    else:
        if not lexical.is_dir() or lexical.is_symlink():
            _fail('SOURCE_DIRECTORY_INVALID', str(lexical))
        _permission_ok(lexical, 'SOURCE_PERMISSIONS_UNSAFE')
    return lexical


def _reject_wal(path):
    for suffix in ('-wal', '-shm', '-journal'):
        sidecar = Path(str(path) + suffix)
        if sidecar.is_symlink():
            _fail('SOURCE_SYMLINK', str(sidecar))
        if sidecar.exists() and not sidecar.is_file():
            _fail('SOURCE_SQLITE_SIDECAR_INVALID', str(sidecar))
        if sidecar.exists() and sidecar.stat().st_size:
            _fail('SOURCE_WAL_NOT_CONVERGED', str(sidecar))


def _lock_identity(path):
    try:
        info = Path(path).lstat()
    except OSError:
        _fail('SOURCE_OWNER_LOCK_REPLACED')
    if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
            or info.st_mode & 0o022):
        _fail('SOURCE_OWNER_LOCK_INVALID')
    return {'dev': info.st_dev, 'ino': info.st_ino, 'size': info.st_size,
            'mtime_ns': info.st_mtime_ns}


def _parent_identity(path):
    try:
        info = Path(path).lstat()
    except OSError:
        _fail('SOURCE_OWNER_BOUNDARY_CHANGED')
    if not stat.S_ISDIR(info.st_mode) or info.st_mode & 0o022:
        _fail('SOURCE_OWNER_BOUNDARY_CHANGED')
    return {'dev': info.st_dev, 'ino': info.st_ino,
            'mtime_ns': info.st_mtime_ns}


def _owner_pid_check(execution_path):
    # A stale PID is auxiliary evidence only. It never replaces the live flock
    # and never authorizes a read by itself.
    for name in (str(execution_path) + '.pid',
                 str(Path(execution_path).parent / 'live-daemon.pid')):
        path = Path(name)
        if path.is_symlink() or not path.is_file():
            continue
        try:
            pid = int(path.read_text().strip())
        except (OSError, ValueError):
            continue
        if process_matches(pid, execution_path):
            _fail('SOURCE_OWNER_RUNNING')


class _OwnerScope:
    """Opaque owner token; the actual flock fd is private to this module.

    ``owner['fd']`` opens a separate, caller-owned diagnostic descriptor on
    the same lock file.  It does not share the private lock OFD, so closing or
    reusing it cannot release or unlock the private flock.  The private fd is
    never returned, so cleanup closes only the descriptor this scope owns.
    """
    __slots__ = ('_path', '_lock_fd', '_identity', '_parent', '_pid',
                 '_active', '_fd_owned')

    def __init__(self, path, fd, identity, parent):
        self._path = path
        self._lock_fd = fd
        self._identity = identity
        self._parent = parent
        self._pid = os.getpid()
        self._active = True
        self._fd_owned = True

    def __getitem__(self, key):
        if key == 'fd':
            if self._active is not True:
                _fail('SOURCE_SCOPE_CLOSED')
            try:
                fd = os.open(self._path, os.O_RDWR | os.O_NOFOLLOW)
            except OSError:
                _fail('SOURCE_SCOPE_INVALID')
            try:
                info = os.fstat(fd)
                if (not stat.S_ISREG(info.st_mode)
                        or info.st_dev != self._identity['dev']
                        or info.st_ino != self._identity['ino']):
                    os.close(fd)
                    _fail('SOURCE_OWNER_LOCK_REPLACED')
            except BaseException:
                try:
                    os.close(fd)
                except OSError:
                    pass
                raise
            return fd
        if key == 'active':
            return self._active
        if key == 'pid':
            return self._pid
        if key == 'path':
            return self._path
        if key == 'identity':
            return self._identity
        if key == 'parent':
            return self._parent
        raise KeyError(key)

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def __contains__(self, key):
        return key in {'fd', 'active', 'pid', 'path', 'identity', 'parent'}


def _probe_lock_state(owner):
    """Return held/free using an independent descriptor, never reusing owner.

    A path/inode mismatch is a continuous-boundary failure, not proof that the
    private descriptor stopped being our resource.  The caller therefore keeps
    ownership of that descriptor and cleanup must still release it.
    """
    try:
        probe = os.open(owner._path, os.O_RDWR | os.O_NOFOLLOW)
    except OSError:
        _fail('SOURCE_OWNER_LOCK_REPLACED')
    try:
        info = os.fstat(probe)
        identity = owner._identity
        if (not stat.S_ISREG(info.st_mode)
                or info.st_dev != identity['dev']
                or info.st_ino != identity['ino']):
            _fail('SOURCE_OWNER_LOCK_REPLACED')
        try:
            fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return 'held'
        except OSError:
            _fail('SOURCE_SCOPE_INVALID')
        else:
            fcntl.flock(probe, fcntl.LOCK_UN)
            return 'free'
    finally:
        os.close(probe)


def _verify_owner_lifecycle(owner):
    if not isinstance(owner, _OwnerScope) or owner._active is not True:
        _fail('SOURCE_SCOPE_CLOSED')
    if owner._pid != os.getpid():
        _fail('SOURCE_SCOPE_CROSS_PROCESS')
    try:
        info = os.fstat(owner._lock_fd)
    except OSError:
        owner._fd_owned = False
        _fail('SOURCE_SCOPE_INVALID')
    identity = owner._identity
    if (not stat.S_ISREG(info.st_mode)
            or info.st_dev != identity['dev']
            or info.st_ino != identity['ino']):
        owner._fd_owned = False
        _fail('SOURCE_SCOPE_INVALID')
    if _probe_lock_state(owner) == 'free':
        # The private descriptor is no longer the lock holder.  Do not attempt
        # to reacquire or to close a descriptor that may have been reused.
        owner._fd_owned = False
        _fail('SOURCE_SCOPE_LOCK_LOST')
    try:
        fcntl.flock(owner._lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        owner._fd_owned = False
        _fail('SOURCE_SCOPE_LOCK_LOST')
    except OSError:
        owner._fd_owned = False
        _fail('SOURCE_SCOPE_INVALID')


@contextmanager
def _owner_exclusive(execution_path):
    """Hold the owner's real flock across the whole physical/logical read."""
    current = _CURRENT_OWNER.get()
    if current is not None:
        _verify_owner_lifecycle(current)
        expected = Path(str(execution_path) + '.lock').resolve()
        if Path(current['path']).resolve() != expected:
            _fail('SOURCE_SCOPE_MISMATCH')
        yield current
        _verify_owner_lifecycle(current)
        return
    lock = Path(str(execution_path) + '.lock')
    initial = _lock_identity(lock)
    parent = _parent_identity(lock.parent)
    try:
        fd = os.open(lock, os.O_RDWR | os.O_NOFOLLOW)
    except OSError:
        _fail('SOURCE_OWNER_LOCK_INVALID')
    acquired = False
    token = None
    owner = None
    try:
        opened = os.fstat(fd)
        if (not stat.S_ISREG(opened.st_mode) or opened.st_nlink != 1
                or opened.st_mode & 0o022
                or {'dev': opened.st_dev, 'ino': opened.st_ino}
                != {'dev': initial['dev'], 'ino': initial['ino']}):
            _fail('SOURCE_OWNER_LOCK_REPLACED')
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            _fail('SOURCE_OWNER_RUNNING')
        acquired = True
        if _lock_identity(lock)['ino'] != initial['ino']:
            _fail('SOURCE_OWNER_LOCK_REPLACED')
        _owner_pid_check(execution_path)
        owner = _OwnerScope(lock, fd, initial, parent)
        token = _CURRENT_OWNER.set(owner)
        yield owner
    finally:
        # fork(2) copies the scope object and shares the underlying OFD.  A
        # child exiting this inherited context must close only its descriptor
        # reference; LOCK_UN on the shared OFD would release the parent's lock.
        fork_child = owner is not None and os.getpid() != owner._pid
        if token is not None:
            if not fork_child:
                try:
                    _verify_owner_lifecycle(owner)
                except SourceEvidenceError:
                    # Lifecycle verification may set _fd_owned=False when the
                    # private descriptor is invalid/foreign.  A path
                    # replacement alone leaves it True, so the still-owned fd
                    # is released.
                    pass
                owner._active = False
            _CURRENT_OWNER.reset(token)
        fd_owned = True if owner is None else owner._fd_owned
        if fd_owned:
            if acquired and not fork_child:
                try:
                    fcntl.flock(fd, fcntl.LOCK_UN)
                except OSError:
                    pass
            try:
                os.close(fd)
            except OSError:
                pass


def _verify_owner_boundary(handle):
    _verify_owner_lifecycle(handle)
    lock = handle['path']
    current = _lock_identity(lock)
    if current['dev'] != handle['identity']['dev'] \
            or current['ino'] != handle['identity']['ino']:
        _fail('SOURCE_OWNER_LOCK_REPLACED')
    if _parent_identity(lock.parent) != handle['parent']:
        _fail('SOURCE_OWNER_BOUNDARY_CHANGED')


def _read_json_file(path, limit, code):
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError:
        _fail(code, str(path))
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size > limit:
            _fail(code, str(path))
        raw = os.read(fd, limit + 1)
        if len(raw) != info.st_size:
            _fail(code, str(path))
        value = json.loads(raw)
    except (ValueError, OSError):
        _fail(code, str(path))
    finally:
        os.close(fd)
    if not isinstance(value, dict):
        _fail(code, str(path))
    return value


def _open_ro(path, tables=()):
    db = None
    try:
        uri = readonly_uri(path)
    except SQLiteReadOnlyError as exc:
        code = ('SOURCE_WAL_NOT_CONVERGED'
                if exc.code == 'SQLITE_SIDECAR_NOT_CONVERGED'
                else 'SOURCE_SQLITE_SIDECAR_INVALID')
        _fail(code, exc.detail)
    try:
        db = sqlite3.connect(uri, uri=True, timeout=.5)
        db.execute('PRAGMA query_only=ON')
        if db.execute('PRAGMA integrity_check').fetchall() != [('ok',)]:
            _fail('SOURCE_SQLITE_INTEGRITY_FAILED', str(path))
        for table in tables:
            if not db.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                    (table,)).fetchone():
                _fail('SOURCE_TABLE_MISSING', table)
        return db
    except sqlite3.Error:
        if db is not None:
            db.close()
        _fail('SOURCE_SQLITE_INVALID', str(path))
    except BaseException:
        if db is not None:
            db.close()
        raise


def _policy_terms(policy, expected):
    suffix = policy.get('account_suffix')
    if suffix != expected['account_suffix'] or not isinstance(suffix, str) \
            or SUFFIX_RE.fullmatch(suffix) is None:
        _fail('SOURCE_POLICY_ACCOUNT_SUFFIX_MISMATCH')
    try:
        capital = number(policy['account_capital_usd'])
        daily = number(policy['stop_new_entries_at_daily_equity_loss_usd'])
        drawdown = number(policy['project_max_drawdown_fraction'])
        terms = _terms(policy)
    except (KeyError, TypeError, ValueError):
        _fail('SOURCE_POLICY_TERMS_INVALID')
    if capital <= 0 or daily <= 0 or not 0 < drawdown < 1:
        _fail('SOURCE_POLICY_TERMS_INVALID')
    if terms != expected['terms_sha256']:
        _fail('SOURCE_POLICY_TERMS_MISMATCH')
    return terms


def _read_source_state(path, expected):
    db = _open_ro(path, tables=('live_state',))
    try:
        row = db.execute('SELECT payload FROM live_state WHERE id=1').fetchone()
    finally:
        db.close()
    if not row:
        _fail('SOURCE_LIVE_STATE_MISSING')
    try:
        state = json.loads(row[0])
    except (TypeError, ValueError):
        _fail('SOURCE_LIVE_STATE_INVALID')
    if not isinstance(state, dict):
        _fail('SOURCE_LIVE_STATE_INVALID')
    if state.get('simulation') is not False:
        _fail('SOURCE_MODE_NOT_LIVE')
    if state.get('account_key') != expected['account_key']:
        _fail('SOURCE_ACCOUNT_MISMATCH')
    if state.get('policy_hash') != expected['policy_sha256']:
        _fail('SOURCE_POLICY_MISMATCH')
    return state


def _read_risk_state(path, expected, policy_terms):
    db = _open_ro(path, tables=('account_risk',))
    try:
        row = db.execute('SELECT payload FROM account_risk WHERE id=1').fetchone()
    finally:
        db.close()
    if not row:
        _fail('SOURCE_RISK_MISSING')
    try:
        state = json.loads(row[0])
    except (TypeError, ValueError):
        _fail('SOURCE_RISK_INVALID')
    if not isinstance(state, dict) or state.get('account_key') != expected['account_key'] \
            or state.get('simulation') is not False \
            or state.get('terms') != policy_terms \
            or state.get('terms') != expected['terms_sha256']:
        _fail('SOURCE_RISK_BINDING_MISMATCH')
    return state


def _read_lineage_source(root, record, expected, policy_terms):
    for field in RECORD_FIELDS:
        if field not in record:
            _fail('SOURCE_LINEAGE_RECORD_INVALID', field)
    policy = record['policy']
    if not isinstance(policy, dict):
        _fail('SOURCE_LINEAGE_RECORD_INVALID', 'policy')
    try:
        policy_hash = digest(policy)
        record_terms = _terms(policy)
    except (KeyError, TypeError, ValueError):
        _fail('SOURCE_LINEAGE_RECORD_INVALID', 'policy')
    if policy_hash != record['policy_sha256']:
        _fail('SOURCE_LINEAGE_RECORD_POLICY_INVALID')
    if policy.get('account_suffix') != expected['account_suffix'] \
            or record_terms != policy_terms \
            or record_terms != expected['terms_sha256']:
        _fail('SOURCE_LINEAGE_RECORD_TERMS_MISMATCH')
    if (not isinstance(record['db'], str)
            or not isinstance(record['account_key'], str)
            or not isinstance(record['account_mask'], str)
            or record['account_key'] != expected['account_key']
            or record['account_mask'] != expected['account_mask']):
        _fail('SOURCE_LINEAGE_RECORD_BINDING_MISMATCH')
    if record['predecessor_db'] is not None \
            and not isinstance(record['predecessor_db'], str):
        _fail('SOURCE_LINEAGE_PREDECESSOR_MISMATCH')
    try:
        source_path = _safe_path(root, record['db'], kind='file')
    except SourceEvidenceError:
        _fail('SOURCE_LINEAGE_PATH_INVALID')
    if Path(record['db']).resolve() != source_path.resolve():
        _fail('SOURCE_LINEAGE_PATH_INVALID')
    sequence = record['audit_sequence']
    head_hash = record['audit_head_hash']
    if type(sequence) is not int or sequence < 0 \
            or _sha(head_hash, 'SOURCE_LINEAGE_AUDIT_INVALID') != head_hash:
        _fail('SOURCE_LINEAGE_AUDIT_INVALID')
    _reject_wal(source_path)
    before = _stable_file(source_path)
    db = _open_ro(source_path, tables=('live_state', 'audit_events'))
    try:
        row = db.execute('SELECT payload FROM live_state WHERE id=1').fetchone()
        if not row:
            _fail('SOURCE_LINEAGE_SOURCE_VIEW_INVALID')
        try:
            state = json.loads(row[0])
        except (TypeError, ValueError):
            _fail('SOURCE_LINEAGE_SOURCE_VIEW_INVALID')
        if (not isinstance(state, dict)
                or state.get('account_key') != record['account_key']
                or state.get('policy_hash') != record['policy_sha256']
                or state.get('simulation') is not False):
            _fail('SOURCE_LINEAGE_SOURCE_VIEW_INVALID')
        try:
            _audit(db).verify()
        except (sqlite3.Error, ValueError, KeyError, TypeError):
            _fail('SOURCE_LINEAGE_AUDIT_INVALID')
        if sequence:
            audit_row = db.execute(
                'SELECT entry_hash FROM audit_events WHERE sequence=?',
                (sequence,)).fetchone()
            if not audit_row or audit_row[0] != head_hash:
                _fail('SOURCE_LINEAGE_AUDIT_PREFIX_MISMATCH')
        elif head_hash != ZERO_HASH:
            _fail('SOURCE_LINEAGE_AUDIT_PREFIX_MISMATCH')
        if sequence and db.execute(
                'SELECT coalesce(max(sequence),0) FROM audit_events'
        ).fetchone()[0] < sequence:
            _fail('SOURCE_LINEAGE_AUDIT_PREFIX_MISMATCH')
    finally:
        db.close()
    _reject_wal(source_path)
    after = _stable_file(source_path)
    if before != after:
        _fail('SOURCE_LINEAGE_SOURCE_CHANGED')
    return source_path


def _read_lineage(root, path, expected, policy_terms, current_policy):
    db = _open_ro(path, tables=('account_risk', 'session_lineage_head',
                                'session_lineage_events'))
    try:
        risk_row = db.execute(
            'SELECT payload FROM account_risk WHERE id=1').fetchone()
        head = db.execute(
            'SELECT sequence,entry_hash FROM session_lineage_head WHERE id=1'
        ).fetchone()
        events = db.execute(
            'SELECT sequence,payload,previous_hash,entry_hash '
            'FROM session_lineage_events ORDER BY sequence').fetchall()
    finally:
        db.close()
    if not risk_row or not head:
        _fail('SOURCE_LINEAGE_MISSING')
    try:
        risk = json.loads(risk_row[0])
    except (TypeError, ValueError):
        _fail('SOURCE_RISK_INVALID')
    if risk.get('account_key') != expected['account_key'] \
            or risk.get('terms') != policy_terms \
            or risk.get('terms') != expected['terms_sha256'] \
            or risk.get('simulation') is not False:
        _fail('SOURCE_RISK_BINDING_MISMATCH')
    previous, records = ZERO_HASH, []
    current_resolved = Path(expected['execution_path']).resolve()
    for sequence, encoded, previous_hash, entry_hash in events:
        try:
            payload = json.loads(encoded)
        except (TypeError, ValueError):
            _fail('SOURCE_LINEAGE_CHAIN_INVALID')
        if (not isinstance(payload, dict)
                or sequence != len(records) + 1
                or canonical(payload) != encoded
                or previous_hash != previous
                or digest({'sequence': sequence,
                           'previous_hash': previous_hash,
                           'event': payload}) != entry_hash):
            _fail('SOURCE_LINEAGE_CHAIN_INVALID', str(sequence))
        required = ('kind', 'registered_at', 'account_key', 'terms',
                    'session', 'current_snapshot_sha256', 'current_source_db',
                    'current_source_policy_sha256')
        if any(field not in payload for field in required):
            _fail('SOURCE_LINEAGE_CHAIN_INVALID', str(sequence))
        if (payload['kind'] != ('BOOTSTRAP' if sequence == 1 else 'ENROLL')
                or payload['account_key'] != expected['account_key']
                or payload['terms'] != policy_terms
                or payload['terms'] != expected['terms_sha256']):
            _fail('SOURCE_LINEAGE_CHAIN_INVALID', str(sequence))
        _time(payload['registered_at'], 'SOURCE_LINEAGE_CHAIN_INVALID')
        _sha(payload['current_snapshot_sha256'],
             'SOURCE_LINEAGE_CHAIN_INVALID')
        _sha(payload['current_source_policy_sha256'],
             'SOURCE_LINEAGE_CHAIN_INVALID')
        record = payload['session']
        if not isinstance(record, dict):
            _fail('SOURCE_LINEAGE_RECORD_INVALID', str(sequence))
        source_path = _read_lineage_source(
            root, record, expected, policy_terms)
        if payload['current_source_db'] != record['db'] \
                or Path(payload['current_source_db']).resolve() != source_path.resolve() \
                or payload['current_source_policy_sha256'] != record['policy_sha256']:
            _fail('SOURCE_LINEAGE_CURRENT_SOURCE_MISMATCH')
        if any(item.get('db') == record['db'] for item in records):
            _fail('SOURCE_LINEAGE_DUPLICATE')
        if records and record.get('predecessor_db') != records[-1].get('db'):
            _fail('SOURCE_LINEAGE_PREDECESSOR_MISMATCH')
        if not records and record.get('predecessor_db') is not None:
            _fail('SOURCE_LINEAGE_PREDECESSOR_MISMATCH')
        records.append(record)
        previous = entry_hash
    if not records or head[0] != len(records) or head[1] != previous:
        _fail('SOURCE_LINEAGE_HEAD_MISMATCH')
    current = records[-1]
    if Path(current['db']).resolve() != current_resolved:
        _fail('SOURCE_LINEAGE_CURRENT_NOT_REGISTERED')
    if (current['policy_sha256'] != expected['policy_sha256']
            or canonical(current['policy']) != canonical(current_policy)):
        _fail('SOURCE_LINEAGE_CURRENT_POLICY_MISMATCH')
    declared = current_policy.get('prior_execution_evidence_db')
    if declared is not None:
        if not isinstance(declared, str) or current['predecessor_db'] is None:
            _fail('SOURCE_LINEAGE_DECLARED_PREDECESSOR_MISMATCH')
        declared_path = Path(declared)
        if not declared_path.is_absolute():
            declared_path = Path(root).parent / declared_path
        if declared_path.resolve() != Path(current['predecessor_db']).resolve():
            _fail('SOURCE_LINEAGE_DECLARED_PREDECESSOR_MISMATCH')
    return {'head_sha256': head[1], 'sequence': head[0], 'records': records}


def _host_inventory(path):
    root = Path(path)
    inventory = checkpoint._host_inventory(root)
    for item in root.rglob('*'):
        _permission_ok(item, 'SOURCE_PERMISSIONS_UNSAFE')
    return inventory


def _check_expected(expected):
    if not isinstance(expected, dict):
        _fail('SOURCE_EXPECTED_REQUIRED')
    missing = [key for key in EXPECTED_FIELDS
               if not isinstance(expected.get(key), str) or not expected[key]]
    if missing:
        _fail('SOURCE_EXPECTED_MISSING', ','.join(missing))
    for key in ('source_fingerprint', 'policy_sha256', 'account_key',
                'execution_db_sha256', 'risk_sidecar_sha256',
                'host_manifest_sha256', 'lineage_head_sha256',
                'state_generation', 'terms_sha256'):
        _sha(expected[key], 'SOURCE_EXPECTED_HASH_INVALID:' + key)
    if not SUFFIX_RE.fullmatch(expected['account_suffix']):
        _fail('SOURCE_EXPECTED_ACCOUNT_INVALID')
    if not isinstance(expected['host_epoch'], str):
        _fail('SOURCE_EXPECTED_EPOCH_INVALID')


def _stable_file(path):
    before = _file_stat(path)
    sha, size = _file_sha(path)
    after = _file_stat(path)
    if before != after:
        _fail('SOURCE_FILE_CHANGED_DURING_READ', str(path))
    return {'sha256': sha, 'bytes': size, 'stat': before}


class SourceEvidenceProvider:
    """Read one stopped runtime boundary without writing any artifact."""

    def __init__(self, *, execution_db, risk_db, host_dir, expected, policy,
                 root=None, isolated=False, clock=None):
        self.root = Path(root) if root is not None else CANONICAL_RUNTIME
        self.isolated = isolated is True
        if not self.isolated and self.root.resolve() != CANONICAL_RUNTIME.resolve():
            _fail('SOURCE_NONCANONICAL_ROOT')
        if not isinstance(policy, dict):
            _fail('SOURCE_POLICY_REQUIRED')
        self.execution_db = _safe_path(self.root, execution_db, kind='file')
        self.risk_db = _safe_path(self.root, risk_db, kind='file')
        self.host_dir = _safe_path(self.root, host_dir, kind='directory')
        self.expected = dict(expected)
        self.policy = dict(policy)
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.expected['execution_path'] = self.execution_db
        suffix = self.expected.get('account_suffix')
        if isinstance(suffix, str):
            self.expected['account_mask'] = '••••' + suffix

    @contextmanager
    def exclusive_scope(self):
        """Share the exact stopped-owner lock across several read-only steps."""
        with _owner_exclusive(self.execution_db) as owner:
            yield owner

    def verify_scope(self, owner):
        if not isinstance(owner, _OwnerScope) \
                or _CURRENT_OWNER.get() is not owner:
            _fail('SOURCE_SCOPE_INVALID')
        _verify_owner_lifecycle(owner)
        if Path(owner['path']).resolve() != Path(
                str(self.execution_db) + '.lock').resolve():
            _fail('SOURCE_SCOPE_MISMATCH')
        _verify_owner_boundary(owner)

    def _blocked(self, exc):
        return {
            'schema_version': 1, 'kind': 'source_evidence_reader',
            'status': 'BLOCKED', 'blockers': [exc.code],
            'blocker_detail': exc.detail, 'evidence': None,
            'provenance': {}, 'source_state': None, 'risk_state': None,
            'production_ready': False, 'live_enabled': False,
            'execution_authorized': False,
            'reader_authority': 'local_source_pins_only_not_signature_or_order_authority',
        }

    def read(self):
        started = self.clock()
        if not isinstance(started, datetime) or started.tzinfo is None:
            _fail('SOURCE_CLOCK_INVALID')
        try:
            _check_expected(self.expected)
            if digest(self.policy) != self.expected['policy_sha256']:
                _fail('SOURCE_POLICY_HASH_MISMATCH')
            policy_terms = _policy_terms(self.policy, self.expected)
            if self.execution_db.name.endswith('-wal') or self.risk_db.name.endswith('-wal'):
                _fail('SOURCE_PATH_INVALID')
            if not self.risk_db.name == 'account-risk-' + self.expected['account_suffix'] + '.sqlite':
                _fail('SOURCE_RISK_SIDECAR_NAME_INVALID')
            with _owner_exclusive(self.execution_db) as owner:
                _reject_wal(self.execution_db)
                _reject_wal(self.risk_db)
                execution_before = _stable_file(self.execution_db)
                risk_before = _stable_file(self.risk_db)
                host_before = _host_inventory(self.host_dir)
                manifest_before = _stable_file(self.host_dir / 'manifest.json')
                if execution_before['sha256'] != self.expected['execution_db_sha256']:
                    _fail('SOURCE_EXECUTION_HASH_MISMATCH')
                if risk_before['sha256'] != self.expected['risk_sidecar_sha256']:
                    _fail('SOURCE_RISK_HASH_MISMATCH')
                if manifest_before['sha256'] != self.expected['host_manifest_sha256']:
                    _fail('SOURCE_HOST_MANIFEST_HASH_MISMATCH')
                source_state = _read_source_state(self.execution_db, self.expected)
                risk_state = _read_risk_state(
                    self.risk_db, self.expected, policy_terms)
                binding = checkpoint._host_binding(self.host_dir)
                expected_db = Path(binding.get('db', '')).resolve()
                if expected_db != self.execution_db.resolve():
                    _fail('SOURCE_HOST_DB_MISMATCH')
                if binding.get('account_key') != self.expected['account_key'] \
                        or binding.get('account') != self.expected['account_mask'] \
                        or binding.get('policy_sha256') != self.expected['policy_sha256'] \
                        or binding.get('source_fingerprint') != self.expected['source_fingerprint'] \
                        or binding.get('epoch') != self.expected['host_epoch']:
                    _fail('SOURCE_HOST_BINDING_MISMATCH')
                from .qualification import source_fingerprint as actual_source
                if actual_source() != self.expected['source_fingerprint']:
                    _fail('SOURCE_SOURCE_FINGERPRINT_MISMATCH')
                lineage = _read_lineage(
                    self.root, self.risk_db, self.expected, policy_terms,
                    self.policy)
                if lineage['head_sha256'] != self.expected['lineage_head_sha256']:
                    _fail('SOURCE_LINEAGE_PIN_MISMATCH')
                generation = checkpoint.runtime_generation(
                    self.execution_db, self.host_dir, self.risk_db,
                    account_suffix=self.expected['account_suffix'])
                if generation != self.expected['state_generation']:
                    _fail('SOURCE_GENERATION_PIN_MISMATCH')
                # Re-read after all logical checks; the lock stays held and the
                # owner path must still name the exact inode we locked.
                execution_after = _stable_file(self.execution_db)
                risk_after = _stable_file(self.risk_db)
                manifest_after = _stable_file(self.host_dir / 'manifest.json')
                host_after = _host_inventory(self.host_dir)
                generation_after = checkpoint.runtime_generation(
                    self.execution_db, self.host_dir, self.risk_db,
                    account_suffix=self.expected['account_suffix'])
                if execution_after != execution_before or risk_after != risk_before \
                        or manifest_after != manifest_before or host_after != host_before \
                        or generation_after != generation:
                    _fail('SOURCE_CHANGED_DURING_READ')
                _reject_wal(self.execution_db)
                _reject_wal(self.risk_db)
                _verify_owner_boundary(owner)
                owner_identity = {
                    'dev': owner['identity']['dev'],
                    'ino': owner['identity']['ino']}
        except SourceEvidenceError as exc:
            return self._blocked(exc)
        except (OSError, sqlite3.Error, KeyError, TypeError, ValueError) as exc:
            return self._blocked(SourceEvidenceError(
                'SOURCE_READ_FAILED', str(exc)))
        completed = self.clock()
        if not isinstance(completed, datetime) or completed.tzinfo is None:
            return self._blocked(SourceEvidenceError('SOURCE_CLOCK_INVALID'))
        evidence = {
            'source': {
                'execution_db_sha256': execution_after['sha256'],
                'account_key': self.expected['account_key'],
                'policy_sha256': self.expected['policy_sha256'],
                'source_fingerprint': self.expected['source_fingerprint'],
                'state_generation': generation,
            },
            'risk': {
                'sidecar_sha256': risk_after['sha256'],
                'account_key': self.expected['account_key'],
                'terms_sha256': risk_state['terms'],
                'state_sha256': digest(risk_state),
                'blocked': risk_state.get('blocked'),
                'daily_halted': bool(risk_state.get('daily_halted')),
                'project_halted': bool(risk_state.get('project_halted')),
            },
            'host': {
                'manifest_sha256': manifest_after['sha256'],
                'account_key': self.expected['account_key'],
                'generation': generation,
                'epoch': binding['epoch'],
            },
            'lineage': {
                'head_sha256': lineage['head_sha256'],
                'account_key': self.expected['account_key'],
                'policy_sha256': self.expected['policy_sha256'],
                'source_fingerprint': self.expected['source_fingerprint'],
                'sequence': lineage['sequence'],
            },
        }
        provenance = {
            'schema_version': 1, 'kind': 'local_source_pin_reader',
            'reader_authority': 'local_file_consistency_not_broker_signature_or_order_authority',
            'execution_path': str(self.execution_db),
            'risk_path': str(self.risk_db), 'host_path': str(self.host_dir),
            'read_started_at': started.astimezone(timezone.utc).isoformat(),
            'read_completed_at': completed.astimezone(timezone.utc).isoformat(),
            'owner_lock_stopped': True,
            'owner_lock_held_across_read': True,
            'owner_lock_identity': owner_identity,
            'wal_converged': True,
            'file_hashes': {
                'execution': execution_after['sha256'],
                'risk': risk_after['sha256'],
                'host_manifest': manifest_after['sha256'],
            },
            'host_inventory': host_after,
            'state_generation': generation,
            'lineage_sequence': lineage['sequence'],
            'source_state_account_key': source_state.get('account_key'),
            'source_state_policy_hash': source_state.get('policy_hash'),
            'source_state_simulation': source_state.get('simulation'),
            'risk_state': {
                'blocked': risk_state.get('blocked'),
                'daily_halted': bool(risk_state.get('daily_halted')),
                'project_halted': bool(risk_state.get('project_halted')),
            },
            'not_a_signature': True, 'execution_authorized': False,
        }
        return {
            'schema_version': 1, 'kind': 'source_evidence_reader',
            'status': 'READY', 'blockers': [], 'blocker_detail': None,
            'evidence': evidence, 'provenance': provenance,
            'source_state': source_state, 'risk_state': risk_state,
            'production_ready': False, 'live_enabled': False,
            'execution_authorized': False,
            'reader_authority': 'local_source_pins_only_not_signature_or_order_authority',
        }


def read_source_evidence(*, execution_db, risk_db, host_dir, expected,
                         policy, root=None, isolated=False, clock=None):
    return SourceEvidenceProvider(
        execution_db=execution_db, risk_db=risk_db, host_dir=host_dir,
        expected=expected, policy=policy, root=root, isolated=isolated,
        clock=clock).read()
