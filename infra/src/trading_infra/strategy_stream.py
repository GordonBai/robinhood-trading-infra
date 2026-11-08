"""Bounded framed requests to one warm, trusted advisory worker.

Retains the same owned child between decisions, avoiding per-event interpreter
and calendar startup. This is fault isolation, not an untrusted-code sandbox.
The trusted command must not create unmanaged descendant processes.
"""
import os
import struct
import subprocess
import time

from trading_contracts.codec import encode_input, decode_decision
from trading_contracts.strategy import StrategyInput, StrategyDecision
from .strategy_process import ProcessStrategyRunner


class WarmStrategyRunner(ProcessStrategyRunner):
    """Owner-thread start/poll API with one length-prefixed request in flight."""

    #: Bounded wait for the exit status after stdout reaches EOF, so failure
    #: classification does not race the child's own termination.
    _EOF_SETTLE_SECONDS = 0.5

    def start(self, request, *, payload=None):
        self._check_owner()
        if self._closed:
            raise ValueError('STRATEGY_PROCESS_CLOSED')
        if self._request is not None:
            raise ValueError('STRATEGY_PROCESS_REQUEST_IN_FLIGHT')
        if not isinstance(request, StrategyInput):
            raise ValueError('STRATEGY_PROCESS_INPUT_REQUIRED')
        if self._process is None:
            # Base owns spawning, secret-free environment, private cwd and all
            # kill/reap failure handling. The input is not written until poll.
            super().start(request, payload=payload)
            payload = self._input
        else:
            # Reject stale unsolicited frames before assigning another request.
            self._poll_idle()
            payload = self._wire(request, payload)
            self._request = request
            self._deadline = time.monotonic()+self.timeout_seconds
            self._input_offset = 0
            self._stdin_broken = False
        self._input = struct.pack('!I', len(payload))+payload

    def _read_stream(self, stream, *, stderr=False):
        if stderr:
            return super()._read_stream(stream, stderr=True)
        if self._stdout_eof:
            return
        remaining = self._IO_BYTES_PER_POLL
        while remaining > 0:
            try:
                data = os.read(stream.fileno(), min(8192, remaining))
            except BlockingIOError:
                break
            if not data:
                self._stdout_eof = True
                break
            remaining -= len(data)
            if len(self._stdout)+len(data) > self.max_output_bytes+4:
                self._fail('STRATEGY_PROCESS_STDOUT_LIMIT')
            self._stdout.extend(data)
            if len(self._stdout) >= 4:
                size = struct.unpack('!I', self._stdout[:4])[0]
                if size == 0 or size > self.max_output_bytes:
                    self._fail('STRATEGY_PROCESS_FRAME_LIMIT')
                if len(self._stdout) > size+4:
                    self._fail('STRATEGY_PROCESS_EXTRA_OUTPUT')

    def _write_input(self):
        # A warm worker waits for another frame; its stdin stays open after the
        # current request is delivered. Only close() or failure closes pipes.
        if self._input_offset < len(self._input):
            try:
                count = os.write(self._process.stdin.fileno(), self._input[
                    self._input_offset:self._input_offset+self._IO_BYTES_PER_POLL])
                self._input_offset += count
            except BlockingIOError:
                return
            except BrokenPipeError:
                self._fail('STRATEGY_PROCESS_INPUT_NOT_DELIVERED')

    def _poll_idle(self):
        try:
            self._read_stream(self._process.stdout)
            self._read_stream(self._process.stderr, stderr=True)
        except OSError:
            self._fail('STRATEGY_PROCESS_IO_FAILED')
        if self._process.poll() is not None:
            self._fail('STRATEGY_PROCESS_CRASHED')
        if self._stdout_eof or self._stdout:
            self._fail('STRATEGY_PROCESS_UNSOLICITED_OUTPUT')

    def poll(self):
        self._check_owner()
        if self._process is None:
            return None
        if self._request is None:
            self._poll_idle()
            return None
        if time.monotonic() >= self._deadline:
            self._fail('STRATEGY_PROCESS_TIMEOUT')
        try:
            self._read_stream(self._process.stdout)
            self._read_stream(self._process.stderr, stderr=True)
            if self._stdout and self._input_offset == 0:
                self._fail('STRATEGY_PROCESS_UNSOLICITED_OUTPUT')
            self._write_input()
        except OSError:
            self._fail('STRATEGY_PROCESS_IO_FAILED')
        if self._process.poll() is not None:
            self._fail('STRATEGY_PROCESS_CRASHED')
        if self._stdout_eof:
            # The child closed stdout. No complete frame can follow, so this is
            # already a hard failure; only the label is at stake. A child that
            # has closed stdout but not yet been reaped would otherwise be
            # reported as a truncated frame, so wait a bounded moment for the
            # real exit status. This runs once, on a path that always fails.
            try:
                self._process.wait(timeout=self._EOF_SETTLE_SECONDS)
            except subprocess.TimeoutExpired:
                pass
            if self._process.poll() is not None:
                self._fail('STRATEGY_PROCESS_CRASHED')
            self._fail('STRATEGY_PROCESS_TRUNCATED_FRAME')
        if len(self._stdout) < 4:
            return None
        size = struct.unpack('!I', self._stdout[:4])[0]
        if len(self._stdout) < size+4:
            return None
        if self._input_offset != len(self._input):
            self._fail('STRATEGY_PROCESS_INPUT_NOT_DELIVERED')
        try:
            decision = decode_decision(bytes(self._stdout[4:]))
            if not isinstance(decision, StrategyDecision):
                raise ValueError()
            decision.validate_for(self._request)
        except Exception:
            self._fail('STRATEGY_PROCESS_INVALID_OUTPUT')
        self._request = None
        self._deadline = None
        self._input = b''
        self._input_offset = 0
        self._stdout.clear()
        return decision
