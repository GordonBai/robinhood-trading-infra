"""Killable, single-operation SYNTHETIC fixture executor; no broker client.

The only child program is this module's fixed fixture entry point. No shell,
Python callable, token cache, URL or real transport is accepted. Child lifetime
is bounded independently of model latency. Terminating a client is NEVER proof
that a broker write did not occur: missing receipts stay UNKNOWN until the core
resolves them from exact observed facts.
"""
from copy import deepcopy
import hashlib
import json
import math
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time

from .audit import canonical, digest
from .script_execution import ScriptExecutionDriver

_OPERATIONS = {'review', 'place', 'cancel'}
_KINDS = {'entry', 'protect', 'cancel', 'exit'}
_OUTCOMES = {'normal', 'error', 'warning', 'approval'}
_MAX_FILE_BYTES = 2_000_000


def _fixture_spec(spec):
    try:
        value = json.loads(json.dumps(spec, allow_nan=False))
    except (TypeError, ValueError) as error:
        raise ValueError('SYNTHETIC_FIXTURE_JSON_REQUIRED') from error
    if not isinstance(value, dict) or set(value) - {'operations', 'by_kind'}:
        raise ValueError('SYNTHETIC_FIXTURE_SCHEMA_INVALID')

    def operations(entries):
        if not isinstance(entries, dict) or set(entries) - _OPERATIONS:
            raise ValueError('SYNTHETIC_FIXTURE_OPERATION_INVALID')
        for item in entries.values():
            if not isinstance(item, dict) or set(item) - {'delay_seconds', 'outcome'}:
                raise ValueError('SYNTHETIC_FIXTURE_BEHAVIOR_INVALID')
            delay = item.get('delay_seconds', 0)
            if (type(delay) not in (int, float) or not math.isfinite(delay)
                    or not 0 <= delay <= 120):
                raise ValueError('SYNTHETIC_FIXTURE_DELAY_INVALID')
            if not isinstance(item.get('outcome', 'normal'), str) or item.get('outcome', 'normal') not in _OUTCOMES:
                raise ValueError('SYNTHETIC_FIXTURE_OUTCOME_INVALID')

    operations(value.get('operations', {}))
    by_kind = value.get('by_kind', {})
    if not isinstance(by_kind, dict) or set(by_kind) - _KINDS:
        raise ValueError('SYNTHETIC_FIXTURE_KIND_INVALID')
    for entries in by_kind.values():
        operations(entries)
    return value


def _write_json(path, value):
    encoded = canonical(value)
    if len(encoded.encode('utf-8')) > _MAX_FILE_BYTES:
        raise ValueError('SYNTHETIC_PROTOCOL_FILE_TOO_LARGE')
    temporary = path.with_suffix('.tmp')
    with temporary.open('x', encoding='utf-8') as handle:
        os.chmod(temporary, 0o600)
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(path)
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _read_json(path):
    with path.open('rb') as handle:
        value = handle.read(_MAX_FILE_BYTES + 1)
    if len(value) > _MAX_FILE_BYTES:
        raise ValueError('SYNTHETIC_RECEIPT_TOO_LARGE')
    return json.loads(value)


def _unknown(reason):
    return {'isError': True, 'content': [{'type': 'text', 'text': reason}]}


def _remaining_child_time(request):
    timeout, deadline = request['timeout_seconds'], request['deadline_monotonic']
    if (type(timeout) not in (int, float) or not math.isfinite(timeout) or not 0 < timeout <= 5
            or type(deadline) not in (int, float) or not math.isfinite(deadline)):
        raise ValueError('SYNTHETIC_CHILD_DEADLINE_INVALID')
    remaining = deadline - time.monotonic()
    if not 0 < remaining <= timeout:
        raise ValueError('SYNTHETIC_CHILD_DEADLINE_EXPIRED_OR_EXTENDED')
    return remaining


class _FixtureMarker:
    simulation_only = True

    def execute_simulated(self, packet):
        raise RuntimeError('FIXTURE_IO_ONLY_IN_FIXED_CHILD')


class SubprocessScriptExecutionDriver:
    """Owner-thread SQLite, one killable fixed child, zero model calls.

    Example fixture_spec::

        {'by_kind': {'entry': {'place': {'delay_seconds': 120}}}}

    step() polls without waiting. The owner must continue polling to service its
    deadline; the child also has a <=5s SIGALRM failsafe if the owner disappears.
    Neither deadline promises filesystem/OS real-time scheduling. The lease is
    inherited by the child and retained by the owner until reaping AND receipt
    import, so an orphaned live child cannot silently lose write ownership.
    """
    def __init__(self, book, fixture_spec, receipt_dir, *, io_timeout_seconds=5):
        if (type(io_timeout_seconds) not in (int, float)
                or not math.isfinite(io_timeout_seconds) or not 0 < io_timeout_seconds <= 5):
            raise ValueError('SYNTHETIC_IO_TIMEOUT_MUST_BE_WITHIN_FIVE_SECONDS')
        self.book = book
        self.driver = ScriptExecutionDriver(book, _FixtureMarker())
        self.fixture_spec = _fixture_spec(fixture_spec)
        self.receipt_dir = Path(receipt_dir)
        self.io_timeout_seconds = float(io_timeout_seconds)
        self.owner = threading.get_ident()
        self.pending = None
        self.stopped = False
        self.last_terminal = None

    def _owner(self):
        if threading.get_ident() != self.owner:
            raise ValueError('SCRIPT_EXECUTION_OWNER_THREAD_REQUIRED')

    def status(self):
        pending = self.pending
        return {'synthetic_only': True, 'stopped': self.stopped,
                'running': pending is not None, 'pid': pending['process'].pid if pending else None,
                'operation_id': pending['packet']['operation_id'] if pending else None,
                'deadline_monotonic': pending['deadline'] if pending else None,
                'kill_requested': pending['kill_requested'] if pending else False,
                'reaped': pending['process'].returncode is not None if pending else bool(self.last_terminal),
                'lease_owned': pending is not None, 'automatic_resubmission_permitted': False,
                'last_terminal': deepcopy(self.last_terminal)}

    def _terminate(self, reason):
        pending = self.pending
        if pending and not pending['kill_requested']:
            pending['kill_requested'] = True
            pending['termination_reason'] = reason
            try:
                # The fixed child never forks children. Kill only this owned
                # Popen child, not a caller-provided PID or unrelated process.
                pending['process'].kill()
            except ProcessLookupError:
                pass

    def stop(self):
        self._owner()
        self.stopped = True
        if self.pending:
            self._terminate('STOP_REQUESTED_POSSIBLE_WRITE')
        return {'status': 'TERMINATING' if self.pending else 'STOPPED', **self.status()}

    def _reconcile(self, snapshot, quotes):
        try:
            self.book.reconcile(snapshot, quotes=quotes)
        except (ValueError, KeyError, TypeError) as error:
            return str(error)
        return None

    def step(self, snapshot, quotes=None, *, allow_entries=False):
        self._owner()
        blocked = self.driver._gate(snapshot)
        if blocked:
            return blocked
        quotes = quotes or {}
        if self.pending:
            return self._poll(snapshot, quotes)
        if self.stopped:
            error = self._reconcile(snapshot, quotes)
            return {'status': 'BLOCKED_VALIDATION' if error else 'STOPPED',
                    'reason': error, 'transport_called': False}
        prepared = self.driver.prepare(snapshot, quotes, allow_entries=allow_entries)
        if prepared['status'] != 'PREPARED':
            return prepared
        packet, lease = deepcopy(prepared['packet']), prepared['_lease']
        process = None
        try:
            action = next(a for a in self.book.actions() if a['id'] == packet['action_id'])
            folder = self.receipt_dir.resolve()
            folder.mkdir(parents=True, exist_ok=True)
            folder.chmod(0o700)
            prefix = folder / packet['operation_id']
            request_path = Path(str(prefix) + '.request.json')
            receipt_path = Path(str(prefix) + '.receipt.json')
            started_path = Path(str(prefix) + '.started.json')
            binding = {'db': str(Path(self.book.db.execute('PRAGMA database_list').fetchone()[2]).resolve()),
                       'account_key': self.book.state()['account_key'], 'policy_sha256': digest(self.book.policy),
                       'adapter_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
            started = time.monotonic()
            request = {'packet': packet, 'kind': action['kind'], 'binding': binding,
                       'fixture_spec': self.fixture_spec, 'receipt_path': str(receipt_path),
                       'started_path': str(started_path), 'timeout_seconds': self.io_timeout_seconds,
                       'deadline_monotonic': started + self.io_timeout_seconds,
                       'lease_fd': lease.fileno(), 'synthetic': True}
            _write_json(request_path, request)
            process = subprocess.Popen([sys.executable, '-m', 'martin_agentic.subprocess_script_execution',
                '--synthetic-child', str(request_path)], stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, close_fds=True,
                pass_fds=(lease.fileno(),), start_new_session=True,
                env={'PATH': '/usr/bin:/bin', 'PYTHONPATH': str(Path(__file__).resolve().parents[1]),
                     'PYTHONNOUSERSITE': '1', 'PYTHONDONTWRITEBYTECODE': '1'})
            self.pending = {'process': process, 'packet': packet, 'lease': lease,
                            'binding': binding, 'receipt_path': receipt_path, 'started_path': started_path,
                            'started': started, 'deadline': started + self.io_timeout_seconds,
                            'kill_requested': False, 'termination_reason': None}
        except Exception:
            # Before Popen returns no child is owned. Preserve ambiguity rather
            # than pretending a failed local launch is an authoritative reject.
            if process is not None:
                # No fallible work follows Popen in this implementation.
                raise
            try:
                result = self.driver.complete(packet, _unknown('SYNTHETIC_CHILD_START_FAILED'))
                result['reason'] = 'SYNTHETIC_CHILD_START_FAILED'
                return result
            finally:
                lease.close()
        return {'status': 'STARTED_SYNTHETIC', 'pid': process.pid, 'inflight': True,
                'operation_id': packet['operation_id'], 'action_id': packet['action_id'],
                'operation': packet['operation'], 'transport_called': False,
                'broker_effect_verified': False, 'synthetic': True}

    def _poll(self, snapshot, quotes):
        pending = self.pending
        process = pending['process']
        code = process.poll()  # WNOHANG; a returned code proves this child reaped.
        if code is None:
            if time.monotonic() >= pending['deadline']:
                self._terminate('SYNTHETIC_IO_DEADLINE_EXCEEDED')
            error = self._reconcile(snapshot, quotes)
            return {'status': 'TERMINATING' if pending['kill_requested'] else 'IN_FLIGHT',
                    'inflight': True, 'pid': process.pid, 'kill_requested': pending['kill_requested'],
                    'reaped': False, 'lease_owned': True, 'transport_called': False,
                    'reconciliation_error': error}
        if code == -signal.SIGALRM and not pending['termination_reason']:
            pending['termination_reason'] = 'SYNTHETIC_CHILD_FAILSAFE_DEADLINE_EXCEEDED'
        response = _unknown(pending['termination_reason'] or 'SYNTHETIC_CHILD_EXIT_WITHOUT_VALID_RECEIPT')
        verified_receipt = False
        if code == 0 and not pending['kill_requested']:
            try:
                receipt = _read_json(pending['receipt_path'])
                if (receipt.get('synthetic') is not True or receipt.get('binding') != pending['binding']
                        or receipt.get('packet_sha256') != digest(pending['packet'])
                        or receipt.get('packet') != pending['packet'] or receipt.get('pid') != process.pid
                        or not isinstance(receipt.get('response'), dict)):
                    raise ValueError('SYNTHETIC_CHILD_RECEIPT_BINDING_INVALID')
                response = receipt['response']
                verified_receipt = True
            except (OSError, ValueError, KeyError, TypeError):
                pass
        # Owner imports before releasing its lease. If import fails, retain the
        # reaped pending record and lease so retrying import is safe/idempotent.
        result = self.driver.complete(pending['packet'], response)
        error = self._reconcile(snapshot, quotes)
        self.last_terminal = {'pid': process.pid, 'returncode': code, 'reaped': True,
                              'operation_id': pending['packet']['operation_id'],
                              'elapsed_seconds': time.monotonic() - pending['started'],
                              'kill_requested': pending['kill_requested'],
                              'termination_reason': pending['termination_reason'],
                              'receipt_verified': verified_receipt}
        pending['lease'].close()
        self.pending = None
        result.update(child_reaped=True, child_pid=process.pid, lease_released=True,
                      termination_reason=self.last_terminal['termination_reason'],
                      receipt_verified=verified_receipt, reconciliation_error=error,
                      elapsed_seconds=self.last_terminal['elapsed_seconds'])
        return result


def _child(request_path):
    request = _read_json(Path(request_path))
    if request.get('synthetic') is not True:
        raise ValueError('SYNTHETIC_CHILD_ONLY')
    remaining = _remaining_child_time(request)
    # Inherited lease is held until process exit, including after parent death.
    os.fstat(request['lease_fd'])
    signal.signal(signal.SIGALRM, signal.SIG_DFL)
    # Do not grant a fresh timeout after startup/import/request I/O latency.
    signal.setitimer(signal.ITIMER_REAL, remaining)
    packet = request['packet']
    spec = _fixture_spec(request['fixture_spec'])
    behavior = {**spec.get('operations', {}).get(packet['operation'], {}),
                **spec.get('by_kind', {}).get(request['kind'], {}).get(packet['operation'], {})}
    _write_json(Path(request['started_path']), {'pid': os.getpid(), 'operation_id': packet['operation_id'],
                                              'delay_seconds': behavior.get('delay_seconds', 0), 'synthetic': True})
    time.sleep(behavior.get('delay_seconds', 0))
    outcome = behavior.get('outcome', 'normal')
    if outcome == 'error':
        response = _unknown('SYNTHETIC_CHILD_FIXTURE_ERROR')
    elif packet['operation'] == 'review':
        response = {'data': {**packet['parameters'], 'order_checks': {'alert': 'SYNTHETIC_WARNING'} if outcome == 'warning' else {},
                            'customer_approval_required': outcome == 'approval',
                            'market_data_disclosure': 'Synthetic subprocess fixture disclosure'}}
    elif packet['operation'] == 'cancel':
        response = {'data': {'accepted': True}}
    elif outcome == 'approval':
        response = {'data': {'approval_id': 'synthetic-' + packet['action_id']}}
    else:
        response = {'data': {'order_id': 'synthetic-' + packet['action_id']}}
    _write_json(Path(request['receipt_path']), {'packet': packet, 'packet_sha256': digest(packet),
                'binding': request['binding'], 'pid': os.getpid(), 'synthetic': True, 'response': response})
    signal.setitimer(signal.ITIMER_REAL, 0)


if __name__ == '__main__':
    if len(sys.argv) != 3 or sys.argv[1] != '--synthetic-child':
        raise SystemExit('Only the fixed synthetic child protocol is supported.')
    _child(sys.argv[2])
