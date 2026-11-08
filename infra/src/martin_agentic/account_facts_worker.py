"""Bounded, read-only account-facts worker.

The owner loop must not compile the current-account facts assessment inline:
that can include source/public/capture file reads and the owner validator.  This
module runs the same canonical producer in a separate, bounded process with
its own SQLite connections opened read-only.  It never receives a broker
object, credentials, an execution/risk write handle or a Trading Book.

The worker result is only a *candidate*.  The owner binds it to a current epoch
and accepts it only after its own source/account/policy/generation/purpose/
journal/risk/time checks.
"""
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time

from .audit import digest
from .engineering_preflight import CurrentEngineeringPreflight
from .entry_authority import DurableEntryAuthority
from .live_verification_control import engineering_verification_gate as control_gate
from .process_registry import spawn_registered

REQUEST_KIND = 'current_account_facts_worker_request_v1'
RESPONSE_KIND = 'current_account_facts_worker_response_v1'
RESPONSE_SCHEMA_VERSION = 1
MAX_REQUEST_BYTES = 2 * 1024 * 1024
MAX_RESPONSE_BYTES = 2 * 1024 * 1024


def _sha256_text(value):
    return hashlib.sha256(str(value or '').encode()).hexdigest()


def _read_regular(path, limit):
    path = Path(path)
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError:
        raise ValueError('ACCOUNT_FACTS_WORKER_FILE_UNAVAILABLE') from None
    try:
        info = os.fstat(fd)
        if not (info.st_mode & 0o170000 == 0o100000) or info.st_size > limit:
            raise ValueError('ACCOUNT_FACTS_WORKER_FILE_INVALID')
        data = os.read(fd, limit + 1)
    finally:
        os.close(fd)
    if len(data) != info.st_size or len(data) > limit:
        raise ValueError('ACCOUNT_FACTS_WORKER_FILE_CHANGED')
    return data


def _write_atomic(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + '.tmp')
    data = (json.dumps(value, sort_keys=True, separators=(',', ':')) + '\n').encode()
    if len(data) > MAX_RESPONSE_BYTES:
        raise ValueError('ACCOUNT_FACTS_WORKER_RESPONSE_TOO_LARGE')
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(fd, 'wb') as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except OSError:
            pass


def _readonly_connection(path):
    db = sqlite3.connect(Path(path).as_uri() + '?mode=ro', uri=True, timeout=.25)
    db.execute('PRAGMA query_only=ON')
    db.execute('BEGIN')
    return db


class ReadOnlyRiskGuard:
    def __init__(self, state, path=None):
        self._state = state
        self.db = _readonly_connection(path) if path else None

    def state(self):
        return self._state

    def close(self):
        if self.db is not None:
            self.db.close()
            self.db = None


class ReadOnlyFactsBook:
    """The small book surface used by the producer/validator, read-only."""

    def __init__(self, db_path, risk_state, *, risk_db_path=None,
                 policy, simulation, clock):
        self.db_path = str(Path(db_path).resolve())
        self.policy = policy
        self.simulation = simulation
        self.clock = clock
        self.db = _readonly_connection(self.db_path)
        self.risk_guard = ReadOnlyRiskGuard(risk_state, risk_db_path)

    def state(self):
        row = self.db.execute('SELECT payload FROM live_state WHERE id=1').fetchone()
        if row is None:
            raise ValueError('ACCOUNT_FACTS_WORKER_STATE_MISSING')
        value = json.loads(row[0])
        if not isinstance(value, dict):
            raise ValueError('ACCOUNT_FACTS_WORKER_STATE_INVALID')
        return value

    def engineering_verification_gate(self, *, purpose, evidence=None):
        return control_gate(self.db_path, simulation=self.simulation,
                            purpose=purpose, evidence=evidence,
                            clock=self.clock)

    def close(self):
        self.db.close()
        self.risk_guard.close()


def _epoch_from_request(request):
    epoch = request.get('epoch')
    epoch_id = request.get('epoch_id')
    if not isinstance(epoch, dict) or not isinstance(epoch_id, str):
        raise ValueError('ACCOUNT_FACTS_WORKER_EPOCH_MISSING')
    if digest(epoch) != epoch_id:
        raise ValueError('ACCOUNT_FACTS_WORKER_EPOCH_ID_INVALID')
    return epoch, epoch_id


def _verify_epoch(book, epoch):
    current = book.db.execute(
        'SELECT payload FROM broker_current WHERE id=1').fetchone()
    health = book.db.execute(
        'SELECT payload FROM broker_health WHERE id=1').fetchone()
    if (current is None or health is None
            or _sha256_text(current[0]) != epoch.get('journal_current_sha256')
            or _sha256_text(health[0]) != epoch.get('journal_health_sha256')):
        raise ValueError('ACCOUNT_FACTS_WORKER_EPOCH_CHANGED')
    risk = book.risk_guard.state()
    if (not isinstance(risk, dict)
            or digest(risk) != epoch.get('risk_state_sha256')):
        raise ValueError('ACCOUNT_FACTS_WORKER_EPOCH_CHANGED')


def _compute(request):
    if not isinstance(request, dict) or request.get('kind') != REQUEST_KIND \
            or request.get('schema_version') != 1:
        raise ValueError('ACCOUNT_FACTS_WORKER_REQUEST_INVALID')
    clock_value = request.get('clock')
    try:
        clock = datetime.fromisoformat(clock_value)
    except (TypeError, ValueError):
        raise ValueError('ACCOUNT_FACTS_WORKER_CLOCK_INVALID') from None
    if clock.tzinfo is None:
        raise ValueError('ACCOUNT_FACTS_WORKER_CLOCK_INVALID')
    clock = clock.astimezone(timezone.utc)
    policy = request.get('policy')
    if not isinstance(policy, dict):
        raise ValueError('ACCOUNT_FACTS_WORKER_POLICY_INVALID')
    db_path = request.get('db_path')
    if not isinstance(db_path, str) or not db_path:
        raise ValueError('ACCOUNT_FACTS_WORKER_DB_INVALID')
    risk_state = request.get('risk_state')
    if risk_state is not None and not isinstance(risk_state, dict):
        raise ValueError('ACCOUNT_FACTS_WORKER_RISK_INVALID')
    risk_db_path = request.get('risk_db_path')
    if risk_db_path is not None and not isinstance(risk_db_path, str):
        raise ValueError('ACCOUNT_FACTS_WORKER_RISK_DB_INVALID')
    public_policy = request.get('public_sources_policy')
    if public_policy is not None:
        if not isinstance(public_policy, dict):
            raise ValueError('ACCOUNT_FACTS_WORKER_PUBLIC_POLICY_INVALID')
        from . import current_evidence as current_evidence_module
        current_evidence_module.PUBLIC_SOURCES = public_policy
    epoch, epoch_id = _epoch_from_request(request)
    book = ReadOnlyFactsBook(
        db_path, risk_state, risk_db_path=risk_db_path,
        policy=policy, simulation=request.get('simulation') is True,
        clock=lambda: clock)
    try:
        _verify_epoch(book, epoch)
        preflight = CurrentEngineeringPreflight(
            request['capture_root'], request['public_sources'],
            account_key=request['account_key'],
            account_suffix=request['account_suffix'],
            policy_sha256=request['policy_sha256'],
            source_fingerprint_value=request['source_fingerprint'],
            clock=lambda: clock,
            expected_capture_generation=request.get('expected_capture_generation'),
            registry_sha256=request.get('registry_sha256'),
            registry_path=request.get('registry_path'))
        purpose = request.get('purpose')
        assessment = preflight.current_account_assessment(
            book, purpose=purpose)
        validation = preflight.validate_account_assessment(
            assessment, book=book, purpose=purpose)
        cached = {'purpose': purpose, 'value': validation}

        def validate(candidate, *, purpose=None):
            if candidate is assessment and purpose == cached['purpose']:
                return cached['value']
            return preflight.validate_account_assessment(
                candidate, book=book, purpose=purpose)

        authority = DurableEntryAuthority(
            book.db, clock=lambda: clock,
            allow_synthetic_no_namespace=book.simulation,
            current_facts_reader=lambda **kwargs: assessment,
            current_facts_validator=validate)
        consumer = authority.check(
            source='AccountFactsWorker', purpose=purpose)
        return {
            'schema_version': RESPONSE_SCHEMA_VERSION,
            'kind': RESPONSE_KIND,
            'epoch_id': epoch_id,
            'assessment': assessment,
            'validation': {'valid': validation[0] is True,
                           'reason': validation[1]},
            'consumer': consumer,
            'read_only': True,
            'broker_writes': 0,
            'book_mutations': 0,
        }
    finally:
        book.close()


def run_worker(request_path, response_path):
    try:
        request = json.loads(_read_regular(request_path, MAX_REQUEST_BYTES))
        response = _compute(request)
    except Exception as exc:
        code = str(exc) if isinstance(exc, ValueError) else 'ACCOUNT_FACTS_WORKER_FAILED'
        response = {'schema_version': RESPONSE_SCHEMA_VERSION,
                    'kind': RESPONSE_KIND, 'error': code[:256],
                    'epoch_id': (request.get('epoch_id')
                                 if isinstance(locals().get('request'), dict)
                                 else None),
                    'read_only': True, 'broker_writes': 0,
                    'book_mutations': 0}
    try:
        _write_atomic(response_path, response)
    except Exception:
        return 1
    return 0


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if len(argv) != 2:
        return 2
    return run_worker(argv[0], argv[1])


class AccountFactsWorker:
    """One owner-thread-managed read-only child; no SQLite handle is shared."""

    def __init__(self, command=None, *, timeout_seconds=2.0,
                 max_response_bytes=MAX_RESPONSE_BYTES):
        if (not isinstance(command, (list, tuple)) or not command
                or any(not isinstance(part, str) or not part or '\0' in part
                       for part in command)):
            command = [sys.executable, '-m',
                       'martin_agentic.account_facts_worker']
        if (type(timeout_seconds) not in (int, float)
                or not math.isfinite(timeout_seconds)
                or timeout_seconds <= 0):
            raise ValueError('ACCOUNT_FACTS_WORKER_TIMEOUT_INVALID')
        if type(max_response_bytes) is not int or not (
                0 < max_response_bytes <= MAX_RESPONSE_BYTES):
            raise ValueError('ACCOUNT_FACTS_WORKER_OUTPUT_LIMIT_INVALID')
        self.command = tuple(command)
        self.timeout_seconds = float(timeout_seconds)
        self.max_response_bytes = max_response_bytes
        self._owner = threading.get_ident()
        self._process = None
        self._directory = None
        self._request_path = None
        self._response_path = None
        self._deadline = None
        self._started = None
        self._epoch_id = None
        self._cleanup_pending = False
        self._last_state = 'idle'
        self._last_error = None

    def _check_owner(self):
        if threading.get_ident() != self._owner:
            raise ValueError('ACCOUNT_FACTS_WORKER_OWNER_THREAD_REQUIRED')

    @property
    def inflight(self):
        return self._process is not None or self._cleanup_pending

    def start(self, request):
        self._check_owner()
        if self.inflight:
            raise ValueError('ACCOUNT_FACTS_WORKER_ALREADY_IN_FLIGHT')
        if not isinstance(request, dict):
            raise ValueError('ACCOUNT_FACTS_WORKER_REQUEST_REQUIRED')
        directory = None
        process = None
        try:
            directory = tempfile.mkdtemp(prefix='account-facts-')
            os.chmod(directory, 0o700)
            self._directory = directory
            self._request_path = os.path.join(directory, 'request.json')
            self._response_path = os.path.join(directory, 'response.json')
            _write_atomic(self._request_path, request)
            environment = {
                'PATH': os.defpath, 'LANG': 'C.UTF-8', 'LC_ALL': 'C.UTF-8',
                'TMPDIR': directory, 'PYTHONDONTWRITEBYTECODE': '1'}
            registry = os.environ.get('MARTIN_PROCESS_REGISTRY')
            if registry:
                environment['MARTIN_PROCESS_REGISTRY'] = registry
            process = spawn_registered(
                [*self.command, self._request_path, self._response_path],
                kind='account_facts_worker',
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                close_fds=True, cwd=directory, env=environment,
                shell=False, start_new_session=True)
            self._process = process
            self._started = time.monotonic()
            self._deadline = self._started + self.timeout_seconds
            self._epoch_id = request.get('epoch_id')
            self._cleanup_pending = False
            self._last_state = 'running'
            self._last_error = None
            return True
        except Exception:
            if process is not None:
                try:
                    if process.poll() is None:
                        process.kill()
                    process.wait(timeout=.1)
                except Exception:
                    pass
            self._process = None
            self._deadline = None
            self._epoch_id = None
            self._started = None
            self._cleanup_pending = False
            self._last_state = 'start_failed'
            self._last_error = 'ACCOUNT_FACTS_WORKER_START_FAILED'
            self._cleanup_directory()
            return False

    def _cleanup_directory(self):
        directory = self._directory
        if directory is None:
            return True
        try:
            shutil.rmtree(directory)
        except OSError:
            self._cleanup_pending = True
            return False
        self._directory = None
        self._request_path = None
        self._response_path = None
        return True

    def _finish(self, state, error=None):
        process = self._process
        if process is not None:
            try:
                process.wait(timeout=.1)
            except subprocess.TimeoutExpired:
                try:
                    if process.poll() is None:
                        process.kill()
                    process.wait(timeout=.1)
                except subprocess.TimeoutExpired:
                    self._cleanup_pending = True
                    self._last_state = state
                    self._last_error = error
                    return False
                except Exception:
                    self._cleanup_pending = True
                    self._last_state = state
                    self._last_error = error
                    return False
        self._process = None
        self._deadline = None
        self._started = None
        self._last_state = state
        self._last_error = error
        if self._cleanup_directory():
            self._cleanup_pending = False
            return True
        return False

    def cancel(self, reason='ACCOUNT_FACTS_WORKER_CANCELLED'):
        self._check_owner()
        if self._process is not None:
            try:
                if self._process.poll() is None:
                    self._process.kill()
            except OSError:
                pass
        result = self._finish('cancelled', reason)
        self._epoch_id = None
        return result

    def poll(self):
        self._check_owner()
        if self._process is None:
            if self._cleanup_pending:
                self._cleanup_directory()
            return None
        if time.monotonic() >= self._deadline:
            epoch_id = self._epoch_id
            self.cancel('ACCOUNT_FACTS_WORKER_TIMEOUT')
            self._last_state = 'timeout'
            self._last_error = 'ACCOUNT_FACTS_WORKER_TIMEOUT'
            return ('timeout', {'error': 'ACCOUNT_FACTS_WORKER_TIMEOUT',
                                'epoch_id': epoch_id})
        code = self._process.poll()
        if code is None:
            return None
        epoch_id = self._epoch_id
        response = None
        error = None
        if self._response_path and os.path.isfile(self._response_path):
            try:
                response = json.loads(
                    _read_regular(self._response_path,
                                  self.max_response_bytes))
            except (ValueError, OSError, TypeError):
                error = 'ACCOUNT_FACTS_WORKER_RESPONSE_INVALID'
        if code != 0:
            error = error or 'ACCOUNT_FACTS_WORKER_EXITED_WITH_ERROR'
        if error is None and not isinstance(response, dict):
            error = 'ACCOUNT_FACTS_WORKER_RESPONSE_INVALID'
        self._finish('completed' if error is None else 'failed', error)
        self._epoch_id = None
        if error is not None:
            return ('failed', {'error': error, 'epoch_id': epoch_id})
        return ('ok', response)

    def status(self):
        age = (time.monotonic() - self._started
               if self._started is not None else None)
        return {
            'running': self._process is not None,
            'inflight': self.inflight,
            'state': self._last_state,
            'error': self._last_error,
            'epoch_id': self._epoch_id,
            'age_seconds': age,
            'timeout_seconds': self.timeout_seconds,
            'deadline_remaining_seconds': (
                max(0.0, self._deadline - time.monotonic())
                if self._deadline is not None else None),
            'cleanup_pending': self._cleanup_pending,
            'read_only_child': True,
            'broker_writes': 0,
            'book_mutations': 0,
        }

    def close(self):
        self._check_owner()
        if self._process is not None:
            self.cancel('ACCOUNT_FACTS_WORKER_CLOSED')
        elif self._cleanup_pending:
            self._cleanup_directory()
        return self.status()


if __name__ == '__main__':
    raise SystemExit(main())
