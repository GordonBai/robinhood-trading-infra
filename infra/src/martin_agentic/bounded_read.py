"""Public bounded read-only collection entry point.

This is a thin, explicit supervisor around the existing session/collector/read
ownership code. It does not add a collector.

Preflight is side-effect free. Execute claims the target with one atomic
mkdir, writes the exact source/pin/protected/policy bytes it verified into a
sealed input file, and spawns one registered child under a total wall deadline
and a getter budget. The parent re-derives completion from the child's journal
and facts; the child's own status label is never sufficient.
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path
import signal
import time
import uuid

from trading_infra.plugin_probe import ProbeFailure, _run_bounded
from .process_registry import identity, read_entries, spawn_registered
from .qualification import source_fingerprint, source_pin

MAX_CYCLES = 3
MAX_GETTERS = 120
MAX_WALL_SECONDS = 90.0
CLEANUP_RESERVE_SECONDS = 2.0
PROFILE = 'read_only_shadow'
_ZOMBIE = {'Z', 'X', 'x'}


def _json(path):
    try:
        value = json.loads(Path(path).read_text())
    except (OSError, ValueError, TypeError):
        raise ValueError('BOUNDED_READ_JSON_INVALID') from None
    if not isinstance(value, dict):
        raise ValueError('BOUNDED_READ_OBJECT_REQUIRED')
    return value


def _sha(path):
    try:
        return hashlib.sha256(Path(path).read_bytes()).hexdigest()
    except OSError:
        raise ValueError('BOUNDED_READ_PROTECTED_UNREADABLE:' + str(path)) from None


def _digest(value):
    return hashlib.sha256(json.dumps(
        value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def _target_key(target):
    return hashlib.sha256(str(Path(target).absolute()).encode()).hexdigest()[:24]


def _kind(target):
    return 'bounded-read:' + _target_key(target)


def preflight(*, policy_path, target, expected_source, protected_manifest,
              registry, cycles=MAX_CYCLES, getter_budget=MAX_GETTERS,
              wall_seconds=MAX_WALL_SECONDS):
    """No transport, target, auth, getter or process side effects."""
    try:
        policy_bytes = Path(policy_path).read_bytes()
        policy = json.loads(policy_bytes)
    except (OSError, ValueError, TypeError):
        raise ValueError('BOUNDED_READ_JSON_INVALID') from None
    if not isinstance(policy, dict):
        raise ValueError('BOUNDED_READ_OBJECT_REQUIRED')
    if policy.get('capture_only') is not True:
        raise ValueError('BOUNDED_READ_POLICY_NOT_CAPTURE_ONLY')
    if policy.get('live_enabled') is not False:
        raise ValueError('BOUNDED_READ_POLICY_LIVE_NOT_DISABLED')
    if policy.get('scheduled') is True:
        raise ValueError('BOUNDED_READ_POLICY_SCHEDULED')
    if type(cycles) is not int or not 1 <= cycles <= MAX_CYCLES:
        raise ValueError('BOUNDED_READ_CYCLES_INVALID')
    if type(getter_budget) is not int or not 1 <= getter_budget <= MAX_GETTERS:
        raise ValueError('BOUNDED_READ_GETTER_BUDGET_INVALID')
    if type(wall_seconds) not in (int, float) or not 0 < wall_seconds <= MAX_WALL_SECONDS:
        raise ValueError('BOUNDED_READ_WALL_BUDGET_INVALID')
    expected_source = str(expected_source or '')
    if len(expected_source) != 64 or any(c not in '0123456789abcdef' for c in expected_source):
        raise ValueError('BOUNDED_READ_EXPECTED_SOURCE_REQUIRED')
    current_source = source_fingerprint()
    if current_source != expected_source:
        raise ValueError('BOUNDED_READ_SOURCE_MISMATCH')
    pin = source_pin()
    if pin.get('status') != 'bounded' or pin.get('truncated') is not False:
        raise ValueError('BOUNDED_READ_SOURCE_PIN_UNAVAILABLE')
    protected = _json(protected_manifest)
    if not protected or not all(isinstance(k, str) and isinstance(v, str)
                                and len(v) == 64 for k, v in protected.items()):
        raise ValueError('BOUNDED_READ_PROTECTED_MANIFEST_INVALID')
    for path, expected in sorted(protected.items()):
        item = Path(path)
        if item.is_symlink() or not item.is_file():
            raise ValueError('BOUNDED_READ_PROTECTED_MISSING:' + path)
        if _sha(item) != expected:
            raise ValueError('BOUNDED_READ_PROTECTED_HASH_MISMATCH:' + path)
    target = Path(target).absolute()
    if target.exists() or target.is_symlink() or not target.parent.is_dir():
        raise ValueError('BOUNDED_READ_TARGET_UNAVAILABLE')
    registry = Path(registry).absolute()
    entries, available = read_entries(str(registry))
    if not available:
        raise ValueError('BOUNDED_READ_REGISTRY_UNAVAILABLE')
    kind = _kind(target)
    for entry in entries:
        if entry.get('kind') != kind:
            continue
        state, _sid, _pgid, _started, stat = identity(entry.get('pid'))
        if state != 'absent' and not (stat and stat[:1] in _ZOMBIE):
            raise ValueError('BOUNDED_READ_ALREADY_RUNNING')
    return {
        'schema_version': 2, 'profile': PROFILE, 'status': 'PREFLIGHT_OK',
        'source_fingerprint': current_source,
        'source_pin': {'status': pin['status'], 'count': pin['count']},
        'source_pin_digest': _digest(pin),
        'policy_sha256': hashlib.sha256(policy_bytes).hexdigest(),
        'target': str(target), 'registry': str(registry),
        'cycles': cycles, 'getter_budget': getter_budget,
        'wall_budget_seconds': wall_seconds,
        'cleanup_reserve_seconds': min(CLEANUP_RESERVE_SECONDS, wall_seconds / 4),
        'protected': {path: _sha(path) for path in sorted(protected)},
        'transport_started': False, 'target_created': False,
        'auth_attempted': False, 'getter_calls': 0,
        'entry_authorized': False, 'production_ready': False,
    }


def _write_private(path, value):
    body = (json.dumps(value, indent=2, sort_keys=True) + '\n').encode()
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(fd, body)
        os.fsync(fd)
    finally:
        os.close(fd)


def _read_journal(target):
    """Read the append-only cycle/getter journal tolerating one torn tail line."""
    path = Path(target) / 'cycles.jsonl'
    try:
        lines = path.read_text().splitlines()
    except OSError:
        return None
    records = []
    for index, line in enumerate(lines):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except ValueError:
            if index == len(lines) - 1:
                break  # a kill can tear the final append; earlier facts stand
            return None
        if not isinstance(value, dict):
            return None
        records.append(value)
    return records


def _summary_from_target(target):
    try:
        value = json.loads((Path(target) / 'summary.json').read_text())
    except (OSError, ValueError, TypeError):
        return None
    return value if isinstance(value, dict) else None


def _sweep_owned(registry, offset, kind, deadline):
    """Kill and verify every registered process this run owned.

    Ownership is the worker entry kind plus its transitive owner chain. A
    process in its own session that the worker started is not in the worker's
    process group, so the registry is the only precise record.
    """
    entries, available = read_entries(registry)
    if not available:
        return {'state': 'UNRESOLVED', 'reason': 'REGISTRY_UNREADABLE',
                'killed': []}
    if offset > len(entries):
        return {'state': 'UNRESOLVED', 'reason': 'REGISTRY_TRUNCATED',
                'killed': []}
    tail = entries[offset:]
    ours = {entry['pid'] for entry in tail if entry.get('kind') == kind}
    changed = True
    while changed:
        changed = False
        for entry in tail:
            if entry.get('owner_pid') in ours and entry['pid'] not in ours:
                ours.add(entry['pid'])
                changed = True
    members = [entry for entry in tail if entry['pid'] in ours]
    killed = []
    try:
        for entry in members:
            state, _sid, _pgid, _started, stat = identity(
                entry['pid'], deadline=deadline)
            if state == 'unknown':
                return {'state': 'UNRESOLVED', 'reason': 'IDENTITY_UNKNOWN',
                        'killed': killed}
            if state == 'present' and not (stat and stat[:1] in _ZOMBIE):
                try:
                    os.killpg(entry['pgid'], signal.SIGKILL)
                except ProcessLookupError:
                    pass
                except PermissionError:
                    return {'state': 'UNRESOLVED',
                            'reason': 'KILL_PERMISSION_DENIED', 'killed': killed}
                killed.append(entry['pid'])
        for entry in members:
            state, _sid, _pgid, _started, stat = identity(
                entry['pid'], deadline=deadline)
            if state == 'unknown':
                return {'state': 'UNRESOLVED', 'reason': 'IDENTITY_UNKNOWN',
                        'killed': killed}
            if state == 'present' and not (stat and stat[:1] in _ZOMBIE):
                return {'state': 'UNRESOLVED', 'reason': 'PROCESS_SURVIVED',
                        'killed': killed}
    except RuntimeError:
        return {'state': 'UNRESOLVED', 'reason': 'CLEANUP_DEADLINE',
                'killed': killed}
    return {'state': 'CLEAN', 'killed': killed, 'owned': len(members)}


def verify_completion(*, run_id, cycles, getter_budget, child, journal,
                      cleanup_state, failure=None, returncode=0):
    """Parent-side PASS derivation. Returns ``(status, checks, failures)``."""
    checks = {}
    failures = []
    stale = (isinstance(child, dict) and child.get('run_id') not in (None, run_id))
    checks['child_run_identity'] = bool(isinstance(child, dict)
                                        and child.get('run_id') == run_id)
    checks['evidence_not_stale'] = not stale
    if stale:
        failures.append('STALE_EVIDENCE')
    if not checks['child_run_identity']:
        failures.append('CHILD_RUN_IDENTITY_MISMATCH')
    records = journal if isinstance(journal, list) else []
    checks['journal_present'] = bool(records)
    if not records:
        failures.append('JOURNAL_MISSING')
    run_started = [item for item in records if item.get('event') == 'run_started']
    cycle_rows = [item for item in records if item.get('event') == 'cycle']
    getter_rows = [item for item in records if item.get('event') == 'getter']
    checks['journal_identity'] = bool(run_started) and all(
        item.get('run_id') == run_id
        for item in records if isinstance(item, dict))
    if not checks['journal_identity']:
        failures.append('JOURNAL_IDENTITY_MISMATCH')
    completed = sum(1 for item in cycle_rows if item.get('completed') is True)
    numbers = sorted(item.get('cycle') for item in cycle_rows)
    checks['configured_cycles'] = cycles
    checks['completed_cycles'] = completed
    checks['cycle_sequence'] = numbers == list(range(1, completed + 1))
    checks['all_configured_cycles_completed'] = completed == cycles
    if completed != cycles:
        failures.append('CONFIGURED_CYCLES_NOT_COMPLETED')
    if not checks['cycle_sequence']:
        failures.append('CYCLE_SEQUENCE_INVALID')
    getters = len(getter_rows)
    checks['getter_reservations'] = getters
    checks['getters_positive'] = getters >= 1
    checks['getters_within_budget'] = 0 < getters <= getter_budget
    if getters < 1:
        failures.append('NO_GETTER_OBSERVED')
    if getters > getter_budget:
        failures.append('GETTER_BUDGET_EXCEEDED')
    if not isinstance(child, dict):
        child = {}
        failures.append('CHILD_SUMMARY_MISSING')
    checks['child_transport_started'] = child.get('transport_started') is True
    checks['child_completed_cycles'] = child.get('completed_cycles') == cycles
    checks['bounded_capture_completed'] = (
        child.get('bounded_capture_completed') is True)
    checks['bounded_session_stopped'] = (
        child.get('bounded_session_stopped') is True)
    checks['current_observation_healthy'] = (
        child.get('current_observation_healthy') is True)
    checks['read_only_capture_healthy'] = (
        child.get('read_only_capture_healthy') is True)
    checks['observation_recorded'] = bool(
        child.get('last_completed_observation_at'))
    checks['child_getters_match_journal'] = (
        child.get('getter_calls') == getters)
    checks['cleanup_verified'] = cleanup_state == 'CLEAN'
    checks['returncode_zero'] = returncode == 0 and failure is None
    for name in ('child_transport_started', 'child_completed_cycles',
                 'bounded_capture_completed', 'bounded_session_stopped',
                 'current_observation_healthy', 'read_only_capture_healthy',
                 'observation_recorded', 'child_getters_match_journal',
                 'cleanup_verified', 'returncode_zero'):
        if checks[name] is not True:
            failures.append(name.upper())
    status = 'PASS' if all(checks.values()) and not failures else 'FAILED'
    return status, checks, failures


def run_bounded_read(*, policy_path, target, expected_source, protected_manifest,
                     registry, symbols, cycles=MAX_CYCLES,
                     getter_budget=MAX_GETTERS, wall_seconds=MAX_WALL_SECONDS,
                     execute=False, worker_command=None):
    prepared = preflight(policy_path=policy_path, target=target,
                         expected_source=expected_source,
                         protected_manifest=protected_manifest,
                         registry=registry, cycles=cycles,
                         getter_budget=getter_budget, wall_seconds=wall_seconds)
    if not execute:
        return prepared
    if not isinstance(symbols, (list, tuple)) or not symbols or any(
            not isinstance(symbol, str) or not symbol for symbol in symbols):
        raise ValueError('BOUNDED_READ_SYMBOLS_REQUIRED')
    target = Path(target).absolute()
    run_id = uuid.uuid4().hex
    started = time.monotonic()
    deadline = started + float(wall_seconds)
    try:
        os.mkdir(target, 0o700)
    except FileExistsError:
        raise ValueError('BOUNDED_READ_TARGET_ALREADY_CLAIMED') from None
    except OSError:
        raise ValueError('BOUNDED_READ_TARGET_CLAIM_FAILED') from None
    policy_bytes = Path(policy_path).read_bytes()
    sealed = {'schema_version': 1, 'run_id': run_id,
              'source_fingerprint': prepared['source_fingerprint'],
              'source_pin_digest': prepared['source_pin_digest'],
              'policy_b64': base64.b64encode(policy_bytes).decode('ascii'),
              'policy_sha256': prepared['policy_sha256'],
              'protected': prepared['protected'],
              'cycles': cycles, 'getter_budget': getter_budget,
              'target': str(target), 'created_at': time.time()}
    inputs = target / 'inputs.json'
    _write_private(inputs, sealed)
    _write_private(target / 'claim.json', {
        'schema_version': 1, 'run_id': run_id, 'pid': os.getpid(),
        'target': str(target), 'source_fingerprint': prepared['source_fingerprint'],
        'source_pin_digest': prepared['source_pin_digest'],
        'policy_sha256': prepared['policy_sha256'],
        'protected_manifest_digest': _digest(prepared['protected']),
        'claimed_at': time.time(), 'entry_authorized': False})
    worker_command = list(worker_command) if worker_command is not None else [
        os.sys.executable, '-I', '-m', 'martin_agentic.bounded_read_worker',
        '--input', str(inputs), '--target', str(target),
        '--run-id', run_id, '--symbols', *map(str, symbols),
        '--cycles', str(cycles), '--getter-budget', str(getter_budget),
        '--wall-seconds', str(wall_seconds)]
    env = dict(os.environ)
    registry = str(Path(registry).absolute())
    env['MARTIN_PROCESS_REGISTRY'] = registry
    env['BOUNDED_READ_RUN_ID'] = run_id
    kind = _kind(target)
    entries_before, available = read_entries(registry)
    if not available:
        raise ValueError('BOUNDED_READ_REGISTRY_UNAVAILABLE')
    offset = len(entries_before)
    work_seconds = max(0.05, float(wall_seconds)
                       - float(prepared['cleanup_reserve_seconds']))
    stdout = b''
    failure = None
    returncode = None
    previous_registry = os.environ.get('MARTIN_PROCESS_REGISTRY')
    os.environ['MARTIN_PROCESS_REGISTRY'] = registry
    try:
        returncode, stdout, _stderr = _run_bounded(
            worker_command, timeout_seconds=work_seconds, env=env,
            spawn=lambda args, **kwargs: spawn_registered(args, kind=kind, **kwargs))
    except ProbeFailure as exc:
        failure = str(exc)
    except (OSError, ValueError) as exc:
        failure = 'BOUNDED_READ_PROCESS_FAILED:' + type(exc).__name__
    finally:
        if previous_registry is None:
            os.environ.pop('MARTIN_PROCESS_REGISTRY', None)
        else:
            os.environ['MARTIN_PROCESS_REGISTRY'] = previous_registry
    cleanup = _sweep_owned(registry, offset, kind, deadline)
    cleanup_state = cleanup['state']
    elapsed = time.monotonic() - started
    if elapsed > float(wall_seconds):
        failure = failure or 'BOUNDED_READ_DEADLINE_EXCEEDED'
    child = None
    try:
        child = json.loads(stdout.decode('utf-8').strip()) if stdout else None
    except (UnicodeDecodeError, ValueError):
        child = None
    if isinstance(child, dict) and child.get('run_id') != run_id:
        # stdout is not this run's evidence. It is not silently replaced by a
        # file that may belong to a previous run either.
        child = {'run_id': child.get('run_id'), 'stale_stdout': True}
    if not isinstance(child, dict):
        candidate = _summary_from_target(target)
        child = (candidate if isinstance(candidate, dict)
                 and candidate.get('run_id') == run_id else None)
    journal = _read_journal(target)
    status, checks, failures = verify_completion(
        run_id=run_id, cycles=cycles, getter_budget=getter_budget,
        child=child, journal=journal, cleanup_state=cleanup_state,
        failure=failure, returncode=0 if returncode is None else returncode)
    result = {
        **prepared,
        'run_id': run_id,
        'status': status,
        'returncode': returncode,
        'failure': failure,
        'verification': checks,
        'failure_codes': failures,
        'wall_seconds': elapsed,
        'work_deadline_seconds': work_seconds,
        'cleanup_deadline_seconds': float(wall_seconds),
        'cleanup_state': ('REAPED' if cleanup_state == 'CLEAN' and failure is None
                          else 'KILL_ATTEMPTED' if cleanup_state == 'CLEAN'
                          else 'UNRESOLVED'),
        'cleanup': cleanup,
        'resource_state': cleanup_state,
        'child': child,
        'journal': {'cycles': [item for item in (journal or [])
                               if item.get('event') == 'cycle'],
                    'getter_reservations': [item for item in (journal or [])
                                            if item.get('event') == 'getter']},
        'transport_started': bool(checks.get('child_transport_started')),
        'target_created': True,
        'entry_authorized': False,
        'production_ready': False,
    }
    try:
        (target / 'summary.json').write_text(
            json.dumps(result, indent=2, sort_keys=True, default=str) + '\n')
        os.chmod(target / 'summary.json', 0o600)
    except OSError:
        result['summary_write'] = 'FAILED'
    return result
