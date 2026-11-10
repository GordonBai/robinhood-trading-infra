"""Child worker for the public bounded read-only supervisor.

The worker consumes only the sealed inputs written by the parent after the
target was atomically claimed. It verifies the source generation, source pin,
protected files and policy bytes before the first transport is created and
again immediately before the first getter dispatch.

Every completed cycle and every getter reservation is appended to the target
journal with an fsync, so a timeout kill still leaves the actual partial
progress readable. The parent re-derives PASS from those facts; a child status
label alone is never enough.
"""
from __future__ import annotations

import argparse
import base64
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import threading

from . import live_session
from .qualification import source_fingerprint, source_pin
from .transport import TransportError

MAX_CYCLES = 3
MAX_GETTERS = 120
OBSERVATION_WAIT_SECONDS = 12.0


def _now():
    return datetime.now(timezone.utc).isoformat()


def _sha(data):
    return hashlib.sha256(data).hexdigest()


def _pin_digest(pin):
    body = json.dumps(pin, sort_keys=True, separators=(',', ':')).encode()
    return _sha(body)


def _verify_environment(sealed):
    """Recheck the exact generation the parent sealed. Fails closed on drift."""
    if source_fingerprint() != sealed.get('source_fingerprint'):
        raise ValueError('BOUNDED_READ_SOURCE_MISMATCH')
    pin = source_pin()
    if pin.get('status') != 'bounded' or pin.get('truncated') is not False:
        raise ValueError('BOUNDED_READ_SOURCE_PIN_UNAVAILABLE')
    if _pin_digest(pin) != sealed.get('source_pin_digest'):
        raise ValueError('BOUNDED_READ_SOURCE_PIN_MISMATCH')
    protected = sealed.get('protected')
    if not isinstance(protected, dict) or not protected:
        raise ValueError('BOUNDED_READ_PROTECTED_MANIFEST_INVALID')
    for path, expected in sorted(protected.items()):
        item = Path(path)
        if item.is_symlink() or not item.is_file():
            raise ValueError('BOUNDED_READ_PROTECTED_MISSING:' + path)
        if _sha(item.read_bytes()) != expected:
            raise ValueError('BOUNDED_READ_PROTECTED_HASH_MISMATCH:' + path)


def _append(state, record):
    """Append one journal line, fsynced and serialized against other writers."""
    line = (json.dumps(record, sort_keys=True) + '\n').encode()
    with state['journal_lock']:
        fd = os.open(state['journal'], os.O_WRONLY | os.O_CREAT | os.O_APPEND,
                     0o600)
        try:
            os.write(fd, line)
            os.fsync(fd)
        finally:
            os.close(fd)


def _counted(original, state):
    """Reserve at the real tool-dispatch boundary, not at generic RPC calls.

    CodexMarketTransport/CodexBrokerReadTransport call ``_read_call`` from
    ``__call__`` after startup and tool discovery. Counting RPC request() would
    miss the real getter path; counting here makes startup control RPC separate
    from getter reservations and fails before dispatch when the budget is spent.

    The guard runs once, immediately before the first reserved getter, so the
    source/protected/policy recheck sits at the last point before a broker
    read can leave the process. All transports in the run share this state,
    including nested calls and paginated reads that re-enter ``_read_call``.
    """
    class Counted(original):
        def _read_call(self, tool, arguments):
            with state['lock']:
                if not state['guarded']:
                    state['guard']()
                    state['guarded'] = True
                if state['count'] >= state['budget']:
                    raise TransportError('READ_ONLY_GETTER_BUDGET_EXCEEDED')
                state['count'] += 1
                state['transport_started'] = True
                sequence = state['count']
                try:
                    _append(state, {'event': 'getter', 'run_id': state['run_id'],
                                    'sequence': sequence, 'tool': str(tool),
                                    'at': _now()})
                except OSError:
                    state['count'] -= 1
                    raise TransportError('READ_ONLY_GETTER_JOURNAL_FAILED') from None
            return super()._read_call(tool, arguments)
    return Counted


def _completion_failures(result, *, cycles, getter_calls, budget, sealed):
    """Every fact a PASS must prove. A child status label is not consulted."""
    if not isinstance(result, dict) or not result:
        return ['RESULT_MISSING']
    failures = []
    if result.get('cycle') != cycles:
        failures.append('COMPLETED_CYCLES_MISMATCH')
    if result.get('bounded_capture_completed') is not True:
        failures.append('BOUNDED_CAPTURE_NOT_COMPLETED')
    if result.get('completed_observation_cycles') != cycles:
        failures.append('COMPLETED_OBSERVATIONS_MISMATCH')
    if result.get('bounded_session_stopped') is not True:
        failures.append('BOUNDED_SESSION_NOT_STOPPED')
    if result.get('current_observation_healthy') is not True:
        failures.append('CURRENT_OBSERVATION_UNHEALTHY')
    if result.get('read_only_capture_healthy') is not True:
        failures.append('READ_ONLY_CAPTURE_UNHEALTHY')
    if not result.get('last_completed_observation_at'):
        failures.append('OBSERVATION_MISSING')
    evidence = result.get('current_observation_evidence')
    if not isinstance(evidence, dict):
        failures.append('OBSERVATION_EVIDENCE_MISSING')
    else:
        if not evidence.get('account_at'):
            failures.append('ACCOUNT_OBSERVATION_MISSING')
        if not evidence.get('market_at'):
            failures.append('MARKET_OBSERVATION_MISSING')
        if evidence.get('latest_observation_state') != 'SUCCESS':
            failures.append('ACCOUNT_OBSERVATION_NOT_SUCCESS')
        if evidence.get('latest_market_state') not in {
                'SUCCESS', 'OPTIONAL_PREOPEN', 'OPTIONAL_CLOSE'}:
            failures.append('MARKET_OBSERVATION_NOT_SUCCESS')
        if evidence.get('cleanup_ok') is not True:
            failures.append('READ_CLEANUP_NOT_PROVEN')
    if result.get('source_fingerprint') != sealed.get('source_fingerprint'):
        failures.append('SOURCE_GENERATION_MISMATCH')
    if result.get('errors'):
        failures.append('CYCLE_ERRORS')
    if getter_calls < 1:
        failures.append('NO_REAL_GETTER')
    if getter_calls > budget:
        failures.append('GETTER_BUDGET_EXCEEDED')
    return failures


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True)
    parser.add_argument('--target', required=True)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--symbols', nargs='+', required=True)
    parser.add_argument('--cycles', type=int, default=3)
    parser.add_argument('--getter-budget', type=int, default=MAX_GETTERS)
    parser.add_argument('--wall-seconds', type=float, default=90)
    args = parser.parse_args(argv)
    target = Path(args.target).absolute()
    summary = {'run_id': args.run_id, 'status': 'FAILED',
               'transport_started': False, 'getter_calls': 0,
               'getter_budget': args.getter_budget,
               'configured_cycles': args.cycles, 'completed_cycles': 0,
               'entry_authorized': False, 'production_ready': False,
               'started_at': _now()}
    state = None
    try:
        if target.is_symlink() or not target.is_dir():
            raise ValueError('BOUNDED_READ_TARGET_UNCLAIMED')
        sealed = json.loads(Path(args.input).read_text())
        if not isinstance(sealed, dict):
            raise ValueError('BOUNDED_READ_INPUT_INVALID')
        if sealed.get('run_id') != args.run_id:
            raise ValueError('BOUNDED_READ_RUN_IDENTITY_MISMATCH')
        for key, value in (('cycles', args.cycles),
                           ('getter_budget', args.getter_budget)):
            if sealed.get(key) != value:
                raise ValueError('BOUNDED_READ_INPUT_MISMATCH:' + key)
        policy_bytes = base64.b64decode(sealed.get('policy_b64', ''), validate=True)
        if _sha(policy_bytes) != sealed.get('policy_sha256'):
            raise ValueError('BOUNDED_READ_POLICY_DIGEST_MISMATCH')
        policy = json.loads(policy_bytes)
        _verify_environment(sealed)
        summary.update(source_fingerprint=sealed.get('source_fingerprint'),
                       source_pin_digest=sealed.get('source_pin_digest'))
        state = {'count': 0, 'budget': args.getter_budget,
                 'lock': threading.Lock(), 'journal_lock': threading.Lock(),
                 'run_id': args.run_id, 'transport_started': False,
                 'journal': str(target / 'cycles.jsonl'), 'guarded': False,
                 'guard': lambda: _verify_environment(sealed)}
        _append(state, {'event': 'run_started', 'run_id': args.run_id,
                        'started_at': summary['started_at'],
                        'source_fingerprint': sealed.get('source_fingerprint'),
                        'source_pin_digest': sealed.get('source_pin_digest'),
                        'policy_sha256': sealed.get('policy_sha256'),
                        'cycles': args.cycles,
                        'getter_budget': args.getter_budget})

        def observe(event):
            result = event['result']
            # A cycle completed only if it reached an observation and recorded
            # no error. Capture-only health additionally gates the final PASS,
            # but an unhealthy read must not be journaled as completed either.
            completed = result.get('cycle_observation_completed') is True
            _append(state, {'event': 'cycle', 'run_id': args.run_id,
                            'cycle': event['cycle'],
                            'configured_cycles': args.cycles,
                            'completed': completed,
                            'observed_at': result.get('observed_at'),
                            'last_completed_observation_at':
                                result.get('last_completed_observation_at'),
                            'cycle_market_state': result.get('cycle_market_state'),
                            'cycle_market_consumed': result.get('cycle_market_consumed'),
                            'errors': len(result.get('errors') or []),
                            'fatal': event.get('fatal'),
                            'getter_calls': state['count'],
                            'at': event['at']})

        live_session.CodexBrokerReadTransport = _counted(
            live_session.CodexBrokerReadTransport, state)
        live_session.CodexMarketTransport = _counted(
            live_session.CodexMarketTransport, state)
        result = live_session.run_live_session(
            target / 'book.sqlite', policy, symbols=args.symbols,
            cycles=args.cycles, poll_seconds=5, agent_mode='disabled',
            output_dir=str(target / 'output'),
            status_file=str(target / 'status.json'),
            source_mode='real_read_only', until_close=False,
            budget_entry_limit=0, budget_total_limit=0,
            cycle_observer=observe,
            observation_wait_seconds=OBSERVATION_WAIT_SECONDS)
        failures = _completion_failures(
            result, cycles=args.cycles, getter_calls=state['count'],
            budget=args.getter_budget, sealed=sealed)
        summary.update(
            status='PASS' if not failures else 'FAILED',
            completed_cycles=(result.get('cycle')
                              if isinstance(result.get('cycle'), int) else 0),
            completed_observation_cycles=result.get('completed_observation_cycles'),
            bounded_capture_completed=result.get('bounded_capture_completed'),
            bounded_session_stopped=result.get('bounded_session_stopped'),
            current_observation_healthy=result.get('current_observation_healthy'),
            read_only_capture_healthy=result.get('read_only_capture_healthy'),
            last_completed_observation_at=result.get(
                'last_completed_observation_at'),
            current_observation_evidence=result.get(
                'current_observation_evidence'),
            source_fingerprint=result.get('source_fingerprint'),
            errors=list(result.get('errors') or []),
            failure_codes=failures,
            getter_calls=state['count'],
            transport_started=state['transport_started'])
    except Exception as exc:
        summary.update(status='FAILED', getter_calls=(state or {}).get('count', 0),
                       transport_started=bool(
                           (state or {}).get('transport_started', False)),
                       error=str(exc) or type(exc).__name__,
                       failure_codes=['WORKER_EXCEPTION'])
    summary['finished_at'] = _now()
    if state is not None:
        try:
            _append(state, {'event': 'run_finished', 'run_id': args.run_id,
                            'status': summary['status'],
                            'completed_cycles': summary.get('completed_cycles', 0),
                            'getter_calls': summary.get('getter_calls', 0),
                            'failure_codes': summary.get('failure_codes') or [],
                            'at': summary['finished_at']})
        except OSError:
            summary['status'] = 'FAILED'
            summary.setdefault('failure_codes', []).append('JOURNAL_WRITE_FAILED')
    try:
        path = target / 'summary.json'
        path.write_text(json.dumps(summary, indent=2, sort_keys=True) + '\n')
        os.chmod(path, 0o600)
    except OSError:
        summary['summary_write'] = 'FAILED'
    print(json.dumps(summary, sort_keys=True))
    return 0 if summary['status'] == 'PASS' else 1


if __name__ == '__main__':
    raise SystemExit(main())
