"""Tiny launch wrapper: register the session leader before exec.

This file intentionally uses only the standard library and is executed by
path, so it works in the restricted environment used for account workers.
The registry is opt-in through MARTIN_PROCESS_REGISTRY; without it the
launcher is a transparent exec wrapper.
"""
import json
import os
import subprocess
import sys


def _started(pid):
    try:
        done = subprocess.run(
            ['ps', '-p', str(pid), '-o', 'lstart='], capture_output=True,
            text=True, timeout=2)
    except (OSError, subprocess.TimeoutExpired):
        raise RuntimeError('PROCESS_IDENTITY_UNAVAILABLE')
    if done.returncode != 0 or not (done.stdout or '').strip():
        raise RuntimeError('PROCESS_IDENTITY_UNAVAILABLE')
    return done.stdout.strip()


def _write(path, entry):
    line = json.dumps(entry, sort_keys=True) + '\n'
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        os.write(fd, line.encode())
        os.fsync(fd)
    finally:
        os.close(fd)


def main(argv):
    if not argv or argv[0] in {'-h', '--help'}:
        return 2
    separator = argv.index('--') if '--' in argv else len(argv)
    kind = argv[0] if argv else ''
    command = argv[separator + 1:]
    if not kind or not command:
        return 2
    path = os.environ.get('MARTIN_PROCESS_REGISTRY')
    if path:
        pid = os.getpid()
        try:
            entry = {
                'pid': pid,
                'sid': os.getsid(pid),
                'pgid': os.getpgid(pid),
                'owner_pid': os.getppid(),
                'kind': kind,
                'started': _started(pid),
            }
            _write(path, entry)
        except Exception:
            return 2
    os.execvp(command[0], command)
    return 127


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))
