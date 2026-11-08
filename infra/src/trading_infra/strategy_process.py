"""Nonblocking, bounded transport for a trusted strategy worker command.

Only typed advisory bytes cross this boundary. No SQLite connection, broker
object, credentials or parent environment is passed. This is fault isolation,
not an operating-system sandbox: trusted code can still access the user's file
system. Commands must not delegate to unmanaged descendants.
"""
import math
import os
import subprocess
import tempfile
import threading
import time

from trading_contracts.codec import encode_input, decode_decision
from trading_contracts.strategy import StrategyDecision, StrategyInput


class ProcessStrategyRunner:
    """One outstanding child; all observation/import occurs on the owner thread."""
    _IO_BYTES_PER_POLL = 65536

    def __init__(self, command, *, timeout_seconds=5, max_output_bytes=4*1024*1024):
        if (not isinstance(command, (list, tuple)) or not command
                or any(not isinstance(part, str) or not part or '\0' in part for part in command)):
            raise ValueError('STRATEGY_PROCESS_COMMAND_INVALID')
        if (type(timeout_seconds) not in (int, float) or not math.isfinite(timeout_seconds)
                or timeout_seconds <= 0):
            raise ValueError('STRATEGY_PROCESS_TIMEOUT_INVALID')
        if type(max_output_bytes) is not int or not 0 < max_output_bytes <= 16*1024*1024:
            raise ValueError('STRATEGY_PROCESS_OUTPUT_BOUND_INVALID')
        self.command = tuple(command)
        self.timeout_seconds = timeout_seconds
        self.max_output_bytes = max_output_bytes
        self._owner = threading.get_ident()
        self._closed = False
        self._process = None
        self._request = None
        self._directory = None
        self._deadline = None
        self._input = b''
        self._input_offset = 0
        self._stdout = bytearray()
        self._stderr_bytes = 0
        self._stdout_eof = self._stderr_eof = False
        self._stdin_broken = False

    def _check_owner(self):
        if threading.get_ident() != self._owner:
            raise ValueError('STRATEGY_PROCESS_OWNER_REQUIRED')

    @property
    def process(self):
        """The owned Popen handle for diagnostics, never a discovered PID."""
        return self._process

    @property
    def next_deadline_monotonic(self):
        self._check_owner()
        return self._deadline

    def _wire(self, request, payload):
        """Trusted owner bytes for an already-encoded request, or encode it now.

        The durable queue persists the exact wire form before dispatch; sending
        that same bytes object avoids a second full encode of a request whose
        payload carries the whole multi-instance checkpoint.
        """
        if payload is None:
            try:
                payload = encode_input(request)
            except Exception:
                raise ValueError('STRATEGY_PROCESS_INPUT_INVALID') from None
        elif not isinstance(payload, bytes):
            raise ValueError('STRATEGY_PROCESS_CODEC_BYTES_REQUIRED')
        return payload

    def start(self, request, *, payload=None):
        self._check_owner()
        if self._closed:
            raise ValueError('STRATEGY_PROCESS_CLOSED')
        if self._process is not None or self._directory is not None:
            raise ValueError('STRATEGY_PROCESS_REQUEST_IN_FLIGHT')
        if not isinstance(request, StrategyInput):
            raise ValueError('STRATEGY_PROCESS_INPUT_REQUIRED')
        payload = self._wire(request, payload)
        if not isinstance(payload, bytes):
            raise ValueError('STRATEGY_PROCESS_CODEC_BYTES_REQUIRED')
        self._request = request
        self._input = payload
        self._input_offset = 0
        self._stdout = bytearray()
        self._stderr_bytes = 0
        self._stdout_eof = self._stderr_eof = False
        self._stdin_broken = False
        self._deadline = time.monotonic()+self.timeout_seconds
        try:
            self._directory = tempfile.TemporaryDirectory(prefix='strategy-worker-')
            environment = {'PATH': os.defpath, 'LANG': 'C.UTF-8', 'LC_ALL': 'C.UTF-8',
                           'TMPDIR': self._directory.name}
            self._process = subprocess.Popen(self.command, stdin=subprocess.PIPE,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0,
                env=environment, cwd=self._directory.name, shell=False,
                close_fds=True, start_new_session=True)
            for stream in (self._process.stdin, self._process.stdout, self._process.stderr):
                os.set_blocking(stream.fileno(), False)
        except Exception:
            self._cleanup(kill=True)
            raise ValueError('STRATEGY_PROCESS_START_FAILED') from None

    def _read_stream(self, stream, *, stderr=False):
        if (self._stderr_eof if stderr else self._stdout_eof):
            return
        remaining = self._IO_BYTES_PER_POLL
        while remaining > 0:
            try:
                data = os.read(stream.fileno(), min(8192, remaining))
            except BlockingIOError:
                break
            if not data:
                if stderr: self._stderr_eof = True
                else: self._stdout_eof = True
                break
            remaining -= len(data)
            if stderr:
                self._stderr_bytes += len(data)
                if self._stderr_bytes > self.max_output_bytes:
                    self._fail('STRATEGY_PROCESS_STDERR_LIMIT')
            else:
                if len(self._stdout)+len(data) > self.max_output_bytes:
                    self._fail('STRATEGY_PROCESS_STDOUT_LIMIT')
                self._stdout.extend(data)

    def _write_input(self):
        stream = self._process.stdin
        if stream.closed or self._stdin_broken:
            return
        if self._input_offset < len(self._input):
            try:
                count = os.write(stream.fileno(), self._input[
                    self._input_offset:self._input_offset+self._IO_BYTES_PER_POLL])
                self._input_offset += count
            except BlockingIOError:
                return
            except BrokenPipeError:
                self._stdin_broken = True
        if self._input_offset == len(self._input) or self._stdin_broken:
            stream.close()

    def poll(self):
        self._check_owner()
        if self._process is None:
            return None
        if time.monotonic() >= self._deadline:
            self._fail('STRATEGY_PROCESS_TIMEOUT')
        try:
            self._read_stream(self._process.stdout)
            self._read_stream(self._process.stderr, stderr=True)
            self._write_input()
        except OSError:
            self._fail('STRATEGY_PROCESS_IO_FAILED')
        code = self._process.poll()
        if code is None:
            return None
        if code != 0:
            self._fail('STRATEGY_PROCESS_CRASHED')
        if self._stdin_broken or self._input_offset != len(self._input):
            self._fail('STRATEGY_PROCESS_INPUT_NOT_DELIVERED')
        if not self._stdout_eof or not self._stderr_eof:
            return None
        try:
            decision = decode_decision(bytes(self._stdout))
            if not isinstance(decision, StrategyDecision):
                raise ValueError('invalid decision')
            decision.validate_for(self._request)
        except Exception:
            self._fail('STRATEGY_PROCESS_INVALID_OUTPUT')
        self._cleanup(kill=False)
        return decision

    def _fail(self, reason):
        self._cleanup(kill=True)
        raise ValueError(reason) from None

    def _cleanup(self, *, kill):
        process = self._process
        if process is not None:
            if kill and process.poll() is None:
                try: process.kill()
                except ProcessLookupError: pass
            try:
                process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                # Keep the handle and prevent replacement, rather than pretend
                # the child was reaped. A repeated close may finish cleanup.
                raise ValueError('STRATEGY_PROCESS_REAP_PENDING') from None
            for stream in (process.stdin, process.stdout, process.stderr):
                if stream is not None:
                    try: stream.close()
                    except OSError: pass
        self._process = None
        self._request = None
        self._deadline = None
        self._input = b''
        self._stdout.clear()
        if self._directory is not None:
            try:
                self._directory.cleanup()
            except OSError:
                # Retain the directory handle and deny another start until an
                # explicit cleanup retry; never expose private filesystem text.
                raise ValueError('STRATEGY_PROCESS_WORKDIR_CLEANUP_FAILED') from None
            self._directory = None

    def close(self):
        self._check_owner()
        self._closed = True
        self._cleanup(kill=True)
