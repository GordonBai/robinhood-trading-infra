"""Opt-in registration for processes that use start_new_session.

The read-only supervisor runs in a parent process while transports and
workers are created in child processes. A new session is intentionally
outside the parent's process group, so a PID registry written before exec is
the explicit ownership record. Without MARTIN_PROCESS_REGISTRY the launcher is
not inserted and normal subprocess behavior is unchanged.
"""
import json
import os
import subprocess
import sys
import time

REGISTRY_ENV = 'MARTIN_PROCESS_REGISTRY'
_LAUNCHER = os.path.join(os.path.dirname(__file__),
                         'process_registry_launcher.py')


def registry_path():
    return os.environ.get(REGISTRY_ENV)


_SKIP_WRAPPER = 0
_ORIGINAL_POPEN = None


def _write_entry(entry):
    path = registry_path()
    if not path:
        return None
    line = json.dumps(entry, sort_keys=True) + '\n'
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        os.write(fd, line.encode())
        os.fsync(fd)
    finally:
        os.close(fd)
    return entry


def register_process(pid, kind):
    """Register an already-spawned process after identity is observable."""
    state, sid, pgid, started, stat = identity(pid)
    if state != 'present' or stat is None or stat[:1] in {'Z', 'X', 'x'}:
        raise RuntimeError('PROCESS_IDENTITY_UNAVAILABLE')
    return _write_entry({
        'pid': pid, 'sid': sid, 'pgid': pgid, 'owner_pid': os.getpid(),
        'kind': kind, 'started': started,
    })


def install_popen_registration():
    """Patch Popen in this (forked child) process to register sessions."""
    global _ORIGINAL_POPEN
    import subprocess
    if _ORIGINAL_POPEN is not None:
        return _ORIGINAL_POPEN
    original = subprocess.Popen

    def registered_popen(*args, **kwargs):
        process = original(*args, **kwargs)
        if kwargs.get('start_new_session') and not _SKIP_WRAPPER:
            try:
                register_process(process.pid, 'popen_start_new_session')
            except Exception as exc:
                try:
                    if process.poll() is None:
                        process.kill()
                    process.wait(timeout=0.2)
                except Exception:
                    pass
                raise RuntimeError('PROCESS_REGISTRY_UNAVAILABLE') from exc
        return process

    subprocess.Popen = registered_popen
    _ORIGINAL_POPEN = original
    return original


def restore_popen_registration(original):
    global _ORIGINAL_POPEN
    import subprocess
    if original is not None:
        subprocess.Popen = original
    _ORIGINAL_POPEN = None


def spawn_registered(command, *, kind, **kwargs):
    """Spawn through the launcher and wait for the registry entry.

    Registration happens in the session leader before exec. Waiting for that
    entry closes the short-lived-child window: a caller never uses a transport
    that the supervisor cannot identify. If registration fails, the process
    is killed and the spawn fails closed.
    """
    path = registry_path()
    if not path:
        return subprocess.Popen(command, **kwargs)
    wrapped = [sys.executable, _LAUNCHER, kind, '--', *list(command)]
    global _SKIP_WRAPPER
    _SKIP_WRAPPER += 1
    try:
        process = subprocess.Popen(wrapped, **kwargs)
    finally:
        _SKIP_WRAPPER -= 1
    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline:
        entries, available = read_entries(path)
        if (available and any(entry.get('pid') == process.pid
                              and entry.get('kind') == kind
                              for entry in entries)):
            return process
        if process.poll() is not None:
            break
        time.sleep(0.01)
    try:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=0.2)
    except Exception:
        pass
    raise RuntimeError('PROCESS_REGISTRY_UNAVAILABLE')


def _started_and_stat(pid, deadline=None):
    timeout = 2.0
    if deadline is not None:
        timeout = max(0.0, min(timeout, deadline - time.monotonic()))
        if timeout <= 0:
            raise RuntimeError('PROCESS_IDENTITY_UNAVAILABLE')
    try:
        done = subprocess.run(
            ['ps', '-p', str(pid), '-o', 'stat=,lstart='],
            capture_output=True, text=True, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired):
        raise RuntimeError('PROCESS_IDENTITY_UNAVAILABLE')
    parts = (done.stdout or '').strip().split(None, 1)
    if done.returncode != 0 or len(parts) != 2:
        raise RuntimeError('PROCESS_IDENTITY_UNAVAILABLE')
    return parts[1].strip(), parts[0]


def identity(pid, deadline=None):
    """Return (state, sid, pgid, started, stat); unknown is never killed."""
    try:
        sid = os.getsid(pid)
        pgid = os.getpgid(pid)
    except ProcessLookupError:
        return 'absent', None, None, None, None
    except PermissionError:
        return 'unknown', None, None, None, None
    try:
        started, stat = _started_and_stat(pid, deadline)
    except RuntimeError:
        return 'unknown', sid, pgid, None, None
    return 'present', sid, pgid, started, stat


def read_entries(path):
    if not path or not os.path.exists(path):
        return [], True
    entries = []
    try:
        with open(path, encoding='utf-8') as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                entry = json.loads(line)
                if not isinstance(entry, dict):
                    return [], False
                required = {'pid', 'sid', 'pgid', 'owner_pid', 'kind',
                            'started'}
                if not required.issubset(entry):
                    return [], False
                if any(not isinstance(entry[key], int)
                       for key in ('pid', 'sid', 'pgid', 'owner_pid')):
                    return [], False
                entries.append(entry)
    except (OSError, ValueError, TypeError):
        return [], False
    return entries, True
