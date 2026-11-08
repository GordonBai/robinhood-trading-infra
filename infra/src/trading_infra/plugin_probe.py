"""Bounded single-operation child probe shared by public CLI paths.

The child is trusted Python, not a sandbox. The probe still isolates faults:
stdout and stderr are read with hard byte ceilings under one deadline, and the
whole process group is killed and the leader reaped on timeout, overflow,
malformed output, or a successful exit that leaves children behind.
"""
import base64
import json
import math
import os
import selectors
import signal
import subprocess
import sys
import time

from trading_contracts.codec import decode_decision, encode_input

PROBE_TIMEOUT_SECONDS = 5.0
PROBE_STDOUT_LIMIT = 1024 * 1024
PROBE_STDERR_LIMIT = 64 * 1024
_READ_SIZE = 65536


class ProbeFailure(Exception):
    """A bounded probe failed structurally; message is a stable error code."""


def _kill_group(process):
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    except PermissionError:
        try:
            process.kill()
        except ProcessLookupError:
            pass
    try:
        process.wait(timeout=1)
    except (subprocess.TimeoutExpired, ProcessLookupError):
        pass
    finally:
        for stream in (process.stdout, process.stderr):
            try:
                stream.close()
            except Exception:
                pass


def _group_alive(process):
    try:
        os.killpg(process.pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _run_bounded(args, timeout_seconds=PROBE_TIMEOUT_SECONDS,
                 stdout_limit=PROBE_STDOUT_LIMIT,
                 stderr_limit=PROBE_STDERR_LIMIT, env=None, spawn=None):
    """Run one child with one total deadline and hard output ceilings.

    Returns ``(returncode, stdout_bytes, stderr_bytes)``. A surviving child in
    the owned process group after the leader exits is a structural failure.
    """
    try:
        timeout = float(timeout_seconds)
        limits = {'stdout': int(stdout_limit), 'stderr': int(stderr_limit)}
    except (TypeError, ValueError):
        raise ProbeFailure('PLUGIN_PROBE_LIMIT_INVALID') from None
    if (not math.isfinite(timeout) or timeout <= 0
            or any(value <= 0 for value in limits.values())):
        raise ProbeFailure('PLUGIN_PROBE_LIMIT_INVALID')
    popen = spawn or subprocess.Popen
    process = popen(args, stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    start_new_session=True, env=env)
    selector = selectors.DefaultSelector()
    buffers = {'stdout': bytearray(), 'stderr': bytearray()}
    failure = None
    try:
        for name, stream in (('stdout', process.stdout), ('stderr', process.stderr)):
            os.set_blocking(stream.fileno(), False)
            selector.register(stream, selectors.EVENT_READ, name)
        deadline = time.monotonic() + timeout
        open_streams = 2
        while open_streams:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                failure = 'PLUGIN_PROBE_TIMEOUT'
                break
            for key, _ in selector.select(min(remaining, 0.1)):
                try:
                    chunk = os.read(key.fd, _READ_SIZE)
                except BlockingIOError:
                    continue
                if not chunk:
                    selector.unregister(key.fileobj)
                    open_streams -= 1
                    continue
                name = key.data
                if len(buffers[name]) + len(chunk) > limits[name]:
                    failure = 'PLUGIN_PROBE_OUTPUT_LIMIT'
                    break
                buffers[name].extend(chunk)
            if failure is not None:
                break
        if failure is None:
            remaining = max(0.0, deadline - time.monotonic())
            try:
                process.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                failure = 'PLUGIN_PROBE_TIMEOUT'
        if failure is None and _group_alive(process):
            # A successful return that leaves an owned child behind is still a
            # failure: every process started for a probe is reaped.
            failure = 'PLUGIN_PROBE_PROCESS_LEAK'
        if failure is not None:
            _kill_group(process)
            raise ProbeFailure(failure)
        return (process.returncode, bytes(buffers['stdout']),
                bytes(buffers['stderr']))
    finally:
        selector.close()
        if process.poll() is None:
            _kill_group(process)
        for stream in (process.stdout, process.stderr):
            try:
                stream.close()
            except Exception:
                pass


def probe(plugin_id, config, operation, *, request=None, view=None,
          timeout_seconds=None):
    timeout = PROBE_TIMEOUT_SECONDS if timeout_seconds is None else timeout_seconds
    args = [sys.executable, '-I', '-m', 'trading_infra.plugin_probe_worker',
            '--plugin', plugin_id,
            '--config', json.dumps(dict(config), sort_keys=True),
            '--op', operation]
    if request is not None:
        args += ['--request-b64', base64.b64encode(encode_input(request)).decode()]
    if view is not None:
        args += ['--view-b64', base64.b64encode(
            json.dumps(view, sort_keys=True).encode()).decode()]
    try:
        returncode, stdout, _stderr = _run_bounded(args, timeout)
    except ProbeFailure as exc:
        return {'ok': False, 'error': str(exc)}
    try:
        value = json.loads(stdout.decode('utf-8').strip())
    except (UnicodeDecodeError, ValueError, AttributeError):
        return {'ok': False, 'error': 'PLUGIN_CONFORMANCE_OUTPUT_INVALID'}
    if not isinstance(value, dict):
        return {'ok': False, 'error': 'PLUGIN_CONFORMANCE_OUTPUT_INVALID'}
    if returncode != 0 or value.get('ok') is not True:
        return {'ok': False,
                'error': value.get('error') or 'PLUGIN_CONFORMANCE_FAILED'}
    return value


def decision_probe(plugin_id, config, request, *, timeout_seconds=None):
    value = probe(plugin_id, config, 'decide', request=request,
                  timeout_seconds=timeout_seconds)
    if value.get('ok') is not True:
        return None, value.get('error', 'PLUGIN_DECIDE_FAILED')
    try:
        return decode_decision(base64.b64decode(value['decision_b64'])), None
    except Exception:
        return None, 'PLUGIN_CONFORMANCE_DECISION_DECODE_FAILED'


def list_plugins_bounded():
    """Describe every installed entry point without importing it in this process."""
    from importlib import metadata
    from trading_infra.plugin_protocol import ENTRY_POINT_GROUP
    entries = tuple(metadata.entry_points(group=ENTRY_POINT_GROUP))
    rows = []
    seen = set()
    for entry in sorted(entries, key=lambda item: item.name):
        if entry.name in seen:
            rows.append({'entry_point': entry.name,
                         'error': 'PLUGIN_ENTRY_POINT_AMBIGUOUS'})
            continue
        seen.add(entry.name)
        value = probe(entry.name, {}, 'describe')
        if value.get('ok') is not True:
            rows.append({'entry_point': entry.name,
                         'value': entry.value,
                         'error': value.get('error', 'PLUGIN_DESCRIPTOR_FAILED')})
            continue
        rows.append({'entry_point': entry.name,
                     'plugin_id': value['descriptor']['plugin_id'],
                     'version': value['descriptor']['version'],
                     'distribution': value.get('distribution'),
                     'distribution_version': value.get('distribution_version'),
                     'identity': value.get('distribution_digest')})
    return rows
