"""Bounded market-data calls through the official Codex app-server.

No credential files, model turns, brokerage writes, or approval overrides.
The thread is an ephemeral protocol session, never a user-facing agent task.
"""

import json
import math
import os
import queue
import signal
import subprocess
import threading
import time

from .market import validate_request
from .process_registry import spawn_registered


class TransportError(RuntimeError):
    """Only fixed diagnostic codes are exposed; upstream errors may contain secrets."""


SAFE_ERRORS = frozenset({'APP_SERVER_PIPE_CLOSED', 'APP_SERVER_TIMEOUT',
    'APP_SERVER_TOTAL_BUDGET_EXCEEDED',
    'APP_SERVER_DISCONNECTED', 'APP_SERVER_INVALID_FRAME', 'APP_SERVER_RPC_ERROR',
    'APP_SERVER_RESPONSE_ID_MISMATCH', 'APP_SERVER_INVALID_RESPONSE',
    'APP_SERVER_SCHEMA_MISMATCH', 'APP_SERVER_HOST_PERMISSION_DENIED',
    'APP_SERVER_BINARY_NOT_FOUND', 'APP_SERVER_STARTUP_FAILED', 'BROKER_SNAPSHOT_SUPERSEDED', 'INTERACTIVE_AUTH_OR_APPROVAL_REQUIRED',
    'MCP_INVENTORY_PAGE_LIMIT', 'MCP_STARTUP_NOT_READY', 'MCP_TOOL_DISCOVERY_FAILED', 'ROBINHOOD_TOOLS_UNAVAILABLE',
    'READ_TOOL_UNAVAILABLE', 'ROBINHOOD_READ_FAILED'})


def safe_read_failure(error):
    # These exact collection races are detected by the read-only broker adapter;
    # retrying means obtain a new complete snapshot, never replay an order write.
    snapshot_races = {'UNSTABLE_ACCOUNT_SNAPSHOT', 'UNSTABLE_ORDER_SNAPSHOT', 'BROKER_COLLECTION_WINDOW_EXCEEDED'}
    if isinstance(error, ValueError) and str(error) in snapshot_races:
        return {'code': str(error), 'retryable': True}
    code = str(error) if isinstance(error, TransportError) and str(error) in SAFE_ERRORS else 'BROKER_READ_UNCLASSIFIED'
    return {'code': code, 'retryable': code in {'APP_SERVER_TIMEOUT', 'APP_SERVER_TOTAL_BUDGET_EXCEEDED', 'APP_SERVER_DISCONNECTED', 'APP_SERVER_PIPE_CLOSED', 'BROKER_SNAPSHOT_SUPERSEDED', 'MCP_STARTUP_NOT_READY', 'MCP_TOOL_DISCOVERY_FAILED'}}


class StdioRpc:
    def __init__(self, command, timeout=30, *, deadline_monotonic=None):
        if not 0 < timeout <= 120:
            raise ValueError('Timeout must be in (0, 120] seconds')
        self.timeout = timeout
        if deadline_monotonic is not None:
            if type(deadline_monotonic) not in (int,float) or not math.isfinite(deadline_monotonic):
                raise ValueError('FINITE_ABSOLUTE_RPC_DEADLINE_REQUIRED')
            if deadline_monotonic<=time.monotonic():
                raise TransportError('APP_SERVER_TOTAL_BUDGET_EXCEEDED')
        self.deadline_monotonic=deadline_monotonic
        self.sequence = 0
        self.last_request_id = None
        self.messages = queue.Queue(maxsize=128)
        self.closed = False
        self.host_permission_denied = False
        self.startup_error = False
        try:
            self.process = spawn_registered(
                command, kind='stdio_rpc', stdin=subprocess.PIPE,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                bufsize=1, start_new_session=True)
        except RuntimeError as exc:
            if str(exc) == 'PROCESS_REGISTRY_UNAVAILABLE':
                raise TransportError('APP_SERVER_STARTUP_FAILED') from None
            raise
        except FileNotFoundError:
            raise TransportError('APP_SERVER_BINARY_NOT_FOUND') from None
        except PermissionError:
            raise TransportError('APP_SERVER_HOST_PERMISSION_DENIED') from None
        self.stderr_reader = threading.Thread(target=self._read_stderr, daemon=True)
        self.stderr_reader.start()
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()

    def _read_stderr(self):
        # Drain bounded chunks; retain only a classification, never raw diagnostics.
        tail = ''
        def classify(line):
            if line.lstrip().startswith('WARNING:'):
                return
            if line.lstrip().startswith(('Error:', 'error:')):
                self.startup_error = True
            if any(marker in line for marker in ('Operation not permitted', 'Permission denied', 'Read-only file system')):
                self.host_permission_denied = True
        try:
            while True:
                chunk = self.process.stderr.read(256)
                if not chunk:
                    classify(tail)
                    return
                lines = (tail + chunk).split('\n')
                for line in lines[:-1]:
                    classify(line)
                tail = lines[-1][-8192:]
        except (OSError, ValueError):
            return

    def _read(self):
        try:
            while not self.closed:
                line = self.process.stdout.readline(16_000_001)
                if not line:
                    self.messages.put_nowait(None)
                    return
                if len(line) > 16_000_000:
                    raise ValueError('Oversized frame')
                self.messages.put_nowait(json.loads(line))
        except (ValueError, OSError, queue.Full):
            try:
                self.messages.put_nowait({'transport_error': True})
            except queue.Full:
                pass

    def send(self, message):
        if self.deadline_monotonic is not None and time.monotonic()>=self.deadline_monotonic:
            raise TransportError('APP_SERVER_TOTAL_BUDGET_EXCEEDED')
        try:
            self.process.stdin.write(json.dumps(message) + '\n')
            self.process.stdin.flush()
        except (BrokenPipeError, OSError, ValueError):
            raise TransportError('APP_SERVER_PIPE_CLOSED') from None

    def request(self, method, params):
        self.sequence += 1
        request_id = self.sequence
        self.last_request_id = request_id
        deadline = time.monotonic() + self.timeout
        total_limited=self.deadline_monotonic is not None and self.deadline_monotonic<=deadline
        if total_limited:deadline=self.deadline_monotonic
        timeout_code='APP_SERVER_TOTAL_BUDGET_EXCEEDED' if total_limited else 'APP_SERVER_TIMEOUT'
        self.send({'id': request_id, 'method': method, 'params': params})
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TransportError(timeout_code)
            try:
                message = self.messages.get(timeout=remaining)
            except queue.Empty:
                raise TransportError(timeout_code) from None
            if message is None:
                self.stderr_reader.join(timeout=.2)
                if self.host_permission_denied:
                    raise TransportError('APP_SERVER_HOST_PERMISSION_DENIED')
                if method == 'initialize' and self.startup_error:
                    raise TransportError('APP_SERVER_STARTUP_FAILED')
                raise TransportError('APP_SERVER_DISCONNECTED')
            if not isinstance(message, dict) or message.get('transport_error'):
                raise TransportError('APP_SERVER_INVALID_FRAME')
            if 'method' in message:
                if 'id' in message:
                    # Never approve a tool, permission, URL, or input request unattended.
                    try:
                        self.send({'id': message['id'], 'error': {
                            'code': -32601, 'message': 'Interactive requests are unsupported by this read-only client'}})
                    except TransportError:
                        # The interaction has already been observed. A failed
                        # refusal reply must not relabel it as a retryable pipe
                        # or budget failure and trigger an automatic reconnect.
                        pass
                    raise TransportError('INTERACTIVE_AUTH_OR_APPROVAL_REQUIRED')
                continue
            if message.get('id') != request_id:
                raise TransportError('APP_SERVER_RESPONSE_ID_MISMATCH')
            if 'error' in message:
                raise TransportError('APP_SERVER_RPC_ERROR')
            if 'result' not in message:
                raise TransportError('APP_SERVER_INVALID_RESPONSE')
            return message['result']

    def close(self, *, deadline_monotonic=None):
        previous=getattr(self,'cleanup_status',None)
        if self.closed and isinstance(previous,dict) and previous.get('complete'):
            return previous
        # A failed bounded close remains retryable.  A later explicit
        # deadline may replace the stored one; an implicit repeated close
        # stays bounded by the original shutdown deadline and never blocks
        # a read thread that is already past its budget.
        if deadline_monotonic is not None:
            self.cleanup_deadline_monotonic=deadline_monotonic
        effective=getattr(self,'cleanup_deadline_monotonic',None)
        self.closed = True

        def remaining(default):
            if effective is None:
                return default
            return max(0.0, min(default, effective - time.monotonic()))

        def snapshot():
            streams=(self.process.stdin,self.process.stdout,self.process.stderr)
            pipes_closed=all(stream is None or stream.closed for stream in streams)
            return {
                'complete': (self.process.poll() is not None
                             and not self.reader.is_alive()
                             and not self.stderr_reader.is_alive()
                             and pipes_closed),
                'process_alive': self.process.poll() is None,
                'pid': self.process.pid,
                'reader_alive': self.reader.is_alive(),
                'reader_name': self.reader.name,
                'stderr_reader_alive': self.stderr_reader.is_alive(),
                'stderr_reader_name': self.stderr_reader.name,
                'pipes_closed': pipes_closed,
            }

        if effective is not None and effective <= time.monotonic():
            # The shared shutdown budget was already spent before this owner
            # got its turn.  Report that fact; do not begin a fresh wait and
            # do not claim any process/pipe/reader was cleaned.
            self.cleanup_status=snapshot()
            self.cleanup_status['budget_exhausted']=True
            return self.cleanup_status

        def term_remaining():
            # Reserve half of a finite budget for SIGKILL/reap and reader
            # joins.  A TERM-ignoring child must not consume the whole
            # shutdown window before it is force-killed.
            if effective is None:
                return 3
            available=max(0.0,effective-time.monotonic())
            return min(3.0,available*0.5)

        self._signal_group(signal.SIGTERM)
        try:
            self.process.wait(timeout=term_remaining())
        except subprocess.TimeoutExpired:
            self._signal_group(signal.SIGKILL)
            try:
                self.process.wait(timeout=remaining(3))
            except subprocess.TimeoutExpired:
                pass
        self._signal_group(signal.SIGKILL)
        self.reader.join(timeout=remaining(1))
        self.stderr_reader.join(timeout=remaining(1))
        for stream in (self.process.stdin,self.process.stdout,self.process.stderr):
            try:
                stream.close()
            except (OSError,ValueError):
                # A failed write can leave buffered bytes in stdin. Closing it
                # after the child dies may raise BrokenPipeError again; still
                # close the other descriptors and permit a fresh connection.
                pass
        # Process, pipes and both reader threads are one owned resource set.
        # Any survivor means the close budget did not finish.
        self.cleanup_status=snapshot()
        self.cleanup_status['budget_exhausted']=False
        return self.cleanup_status

    def _signal_group(self, sig):
        try:
            os.killpg(self.process.pid, sig)
        except ProcessLookupError:
            pass
        except PermissionError:
            # Some desktop hosts restrict process-group signals; still reap our
            # own child without changing host permissions or signalling others.
            if self.process.poll() is None:
                try:
                    self.process.send_signal(sig)
                except ProcessLookupError:
                    pass


class CodexMarketTransport:
    def __init__(self, *, server='robinhood-trading', binary='codex', timeout=30, rpc=None, deadline_monotonic=None):
        if rpc is not None and deadline_monotonic is not None:
            raise ValueError('INJECTED_RPC_MUST_CONFIGURE_ITS_OWN_ABSOLUTE_DEADLINE')
        self.server = server
        self.rpc = rpc or StdioRpc([binary, 'app-server', '--stdio'], timeout,
                                    deadline_monotonic=deadline_monotonic)
        self.thread_id = None
        self.tools = set()
        self.tool_descriptors = {}
        self.last_call_id = None
        self.connection_id = __import__('uuid').uuid4().hex
        self.last_review_call = None

    def arm_deadline(self, deadline_monotonic):
        """Only tighten the existing absolute RPC deadline; never relax it."""
        if type(deadline_monotonic) not in (int, float) \
                or not math.isfinite(deadline_monotonic):
            raise ValueError('FINITE_ABSOLUTE_RPC_DEADLINE_REQUIRED')
        now = time.monotonic()
        current = getattr(self.rpc, 'deadline_monotonic', None)
        if current is not None:
            if type(current) not in (int, float) or not math.isfinite(current):
                raise ValueError('FINITE_ABSOLUTE_RPC_DEADLINE_REQUIRED')
            deadline_monotonic = min(current, deadline_monotonic)
        if deadline_monotonic <= now:
            raise TransportError('APP_SERVER_TOTAL_BUDGET_EXCEEDED')
        self.rpc.deadline_monotonic = deadline_monotonic
        return deadline_monotonic

    def __enter__(self):
        try:
            self.rpc.request('initialize', {'clientInfo': {
                'name': 'martin_shadow_reader', 'version': '0.1.0'},
                'capabilities': {'experimentalApi': True}})
            self.rpc.send({'method': 'initialized', 'params': {}})
            session = self.rpc.request('thread/start', {
                'ephemeral': True, 'approvalPolicy': 'never', 'sandbox': 'read-only'})
            self.thread_id = session['thread']['id']
            self._discover_tools()
            return self
        except (KeyError, TypeError):
            self.rpc.close()
            raise TransportError('APP_SERVER_SCHEMA_MISMATCH') from None
        except BaseException:
            self.rpc.close()
            raise

    def _discover_tools(self):
        # App-server can report a thread's MCP runtime before initialization has
        # completed. Only an explicit starting state permits this short wait;
        # never keep retrying disabled/missing configuration or interactive auth.
        for attempt in range(3):
            cursor = None
            tools = set()
            starting = discovery_failed = False
            for _ in range(20):
                inventory = self.rpc.request('mcpServerStatus/list', {
                    'threadId': self.thread_id, 'serverName': self.server,
                    'detail': 'toolsAndAuthOnly', 'limit': 100, 'cursor': cursor})
                if not isinstance(inventory, dict) or not isinstance(inventory.get('data'), list):
                    raise TransportError('APP_SERVER_SCHEMA_MISMATCH')
                for row in inventory['data']:
                    if not isinstance(row, dict):
                        raise TransportError('APP_SERVER_SCHEMA_MISMATCH')
                    if row.get('name') != self.server:
                        continue
                    state = row.get('runtimeStatus')
                    if state == 'authenticationRequired' or row.get('authStatus') == 'notLoggedIn':
                        raise TransportError('INTERACTIVE_AUTH_OR_APPROVAL_REQUIRED')
                    catalog = row.get('tools', {})
                    if not isinstance(catalog, dict):
                        raise TransportError('APP_SERVER_SCHEMA_MISMATCH')
                    if state in {'notStarted', 'starting'}:
                        starting = True
                    elif row.get('toolsError'):
                        # Raw upstream discovery errors can contain secrets.
                        discovery_failed = True
                    elif state in {None, 'connected'}:
                        tools.update(catalog)
                        self.tool_descriptors.update(catalog)
                cursor = inventory.get('nextCursor')
                if not cursor:
                    break
            else:
                raise TransportError('MCP_INVENTORY_PAGE_LIMIT')
            if tools and not starting and not discovery_failed:
                self.tools = tools
                return
            if discovery_failed:
                raise TransportError('MCP_TOOL_DISCOVERY_FAILED')
            if not starting:
                raise TransportError('ROBINHOOD_TOOLS_UNAVAILABLE')
            if attempt < 2:
                time.sleep(.25 * (attempt + 1))
        raise TransportError('MCP_STARTUP_NOT_READY')

    def __call__(self, tool, arguments):
        validate_request(tool, arguments)
        return self._read_call(tool, arguments)

    def _read_call(self, tool, arguments):
        if not self.thread_id or tool not in self.tools:
            raise TransportError('READ_TOOL_UNAVAILABLE')
        from datetime import datetime, timezone
        from trading_contracts.values import digest
        from .native_operations import response_identity
        started = datetime.now(timezone.utc)
        self.last_review_call = None
        try:
            result = self.rpc.request('mcpServer/tool/call', {
                'threadId': self.thread_id, 'server': self.server,
                'tool': tool, 'arguments': arguments})
        except Exception as exc:
            ended = datetime.now(timezone.utc)
            self.last_review_call = {
                'schema_version': 1, 'status': 'failed',
                'server': self.server, 'thread_id': self.thread_id,
                'connection_id': self.connection_id,
                'rpc_request_id': (str(self.rpc.last_request_id)
                                   if getattr(self.rpc, 'last_request_id', None)
                                   is not None else None),
                'tool': tool, 'arguments_sha256': digest(arguments),
                'parameters_sha256': digest({k:v for k,v in arguments.items() if k!='account_number'}),
                'account_scope_sha256': (__import__('hashlib').sha256(arguments['account_number'].encode()).hexdigest()
                    if isinstance(arguments.get('account_number'),str) else None),
                'descriptor_sha256': (digest(self.tool_descriptors[tool])
                                      if tool in self.tool_descriptors else None),
                'started_at': started.isoformat(),
                'ended_at': ended.isoformat(),
                'response_sha256': None, 'response_identity_sha256': None,
                'reason': safe_read_failure(exc)['code']}
            raise
        ended = datetime.now(timezone.utc)
        if not isinstance(result, dict) or result.get('isError'):
            self.last_review_call = {
                'schema_version': 1, 'status': 'failed',
                'server': self.server, 'thread_id': self.thread_id,
                'connection_id': self.connection_id,
                'rpc_request_id': (str(self.rpc.last_request_id)
                                   if getattr(self.rpc, 'last_request_id', None)
                                   is not None else None),
                'tool': tool, 'arguments_sha256': digest(arguments),
                'parameters_sha256': digest({k:v for k,v in arguments.items() if k!='account_number'}),
                'account_scope_sha256': (__import__('hashlib').sha256(arguments['account_number'].encode()).hexdigest()
                    if isinstance(arguments.get('account_number'),str) else None),
                'descriptor_sha256': (digest(self.tool_descriptors[tool])
                                      if tool in self.tool_descriptors else None),
                'started_at': started.isoformat(),
                'ended_at': ended.isoformat(),
                'response_sha256': (digest(result)
                                    if isinstance(result, dict) else None),
                'response_identity_sha256': None, 'reason': 'RESPONSE_ERROR'}
            raise TransportError('ROBINHOOD_READ_FAILED')
        _redacted, identity = response_identity(result)
        self.last_review_call = {
            'schema_version': 1, 'status': 'received',
            'server': self.server, 'thread_id': self.thread_id,
            'connection_id': self.connection_id,
            'rpc_request_id': (str(self.rpc.last_request_id)
                               if getattr(self.rpc, 'last_request_id', None)
                               is not None else None),
            'tool': tool, 'arguments_sha256': digest(arguments),
            'parameters_sha256': digest({k:v for k,v in arguments.items() if k!='account_number'}),
            'account_scope_sha256': (__import__('hashlib').sha256(arguments['account_number'].encode()).hexdigest()
                if isinstance(arguments.get('account_number'),str) else None),
            'descriptor_sha256': (digest(self.tool_descriptors[tool])
                                  if tool in self.tool_descriptors else None),
            'started_at': started.isoformat(), 'ended_at': ended.isoformat(),
            'response_sha256': digest(result),
            'response_identity_sha256': identity, 'reason': None}
        self.last_call_id = self.last_review_call['rpc_request_id']
        return result

    def __exit__(self, *args):
        deadline=getattr(self,'shutdown_deadline_monotonic',None)
        if deadline is None:
            self.rpc.close()
        else:
            self.rpc.close(deadline_monotonic=deadline)


class CodexBrokerReadTransport(CodexMarketTransport):
    """Separate account-bound observation boundary; never permits order writes."""

    ACCOUNT_TOOLS = frozenset({'get_portfolio', 'get_equity_positions', 'get_equity_orders',
                              'get_advanced_orders', 'get_trade_approval_setting',
                              'get_trade_approvals', 'get_option_positions',
                              'get_option_orders'})
    RHS_TOOLS = frozenset({'get_crypto_positions', 'get_crypto_orders'})

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.account_number = None
        self.rhs_account_number = None

    def bind(self, account_number, rhs_account_number=None):
        if self.account_number and self.account_number != account_number:
            raise ValueError('Cannot change a bound brokerage account')
        if rhs_account_number is not None:
            if not isinstance(rhs_account_number, str) or not rhs_account_number:
                raise ValueError('Invalid bound crypto account')
            if self.rhs_account_number and self.rhs_account_number != rhs_account_number:
                raise ValueError('Cannot change a bound crypto account')
            self.rhs_account_number = rhs_account_number
        self.account_number = account_number

    def __call__(self, tool, arguments):
        if not isinstance(arguments, dict):
            raise ValueError('Require read arguments object')
        if tool == 'get_accounts':
            if arguments:
                raise ValueError('Unexpected account discovery arguments')
        elif tool in self.ACCOUNT_TOOLS or tool in self.RHS_TOOLS:
            rhs_tool = tool in self.RHS_TOOLS
            key = 'rhs_account_number' if rhs_tool else 'account_number'
            expected = self.rhs_account_number if rhs_tool else self.account_number
            allowed = {key}
            if tool in {'get_equity_positions', 'get_equity_orders',
                        'get_advanced_orders', 'get_option_positions',
                        'get_option_orders', 'get_crypto_positions',
                        'get_crypto_orders', 'get_trade_approvals'}:
                allowed.add('cursor')
            if tool == 'get_equity_orders':
                allowed.add('order_id')
            if tool == 'get_trade_approvals':
                allowed.add('approval_ids')
                if 'approval_ids' in arguments:
                    ids=arguments['approval_ids']
                    if (not isinstance(ids,list) or not 1<=len(ids)<=100
                            or any(not isinstance(v,str) or not 0<len(v)<=200 for v in ids)
                            or len(set(ids))!=len(ids) or 'cursor' in arguments):
                        raise ValueError('Invalid exact approval identity query')
            if not expected or arguments.get(key) != expected or set(arguments) - allowed:
                raise ValueError('Account binding or read arguments invalid')
            if 'cursor' in arguments and (not isinstance(arguments['cursor'], str) or not arguments['cursor']):
                raise ValueError('Invalid opaque pagination cursor')
        else:
            raise ValueError('Only the explicit broker observation read allowlist is supported')
        return self._read_call(tool, arguments)
