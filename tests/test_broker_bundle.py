"""Bounded read-only collector tests; injected fake transport, no broker IO."""
from copy import deepcopy
from datetime import datetime, timezone, timedelta
import hashlib
import io
import json
import os
from pathlib import Path
import shlex
import stat
import sys
import tempfile
import time
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from martin_agentic import broker_bundle
from martin_agentic.baseline_evidence import load_saved_bundle, normalize_bundle
from martin_agentic.broker_bundle import (
    CollectorError, collect_saved_bundle, run_cli as run_bundle_cli)
from martin_agentic.cli import main
from martin_agentic.transport import CodexBrokerReadTransport, TransportError
from test_baseline_cli import run_cli as run_019_cli, tree_state
from test_source_evidence import SourceFixture
from trading_contracts.values import digest


AT = '2026-10-19T14:00:00+00:00'
ACCOUNT = 'SIM-0000'
RHS = 'RHS-0000'
RHC = 'RHC-0000'


def _portfolio():
    return {
        'cash': '500', 'total_value': '500', 'pending_deposits': '0',
        'currency': 'USD', 'options_value': '0', 'crypto_value': '0',
        'futures_value': '0', 'event_contracts_value': '0',
        'fixed_income_value': '0', 'mutual_funds_value': '0',
        'buying_power': {'buying_power': '500', 'display_currency': 'USD',
                         'unleveraged_buying_power': '500'},
    }


def _write_stdio_server(root, mode, *, startup_delay=0.0):
    server_python = Path(root) / f'stdio-{mode}-server.py'
    script = Path(root) / f'stdio-{mode}.py'
    tools = [
        'get_accounts', 'get_portfolio', 'get_trade_approval_setting',
        'get_equity_positions', 'get_equity_orders', 'get_advanced_orders',
        'get_option_positions', 'get_option_orders', 'get_crypto_positions',
        'get_crypto_orders', 'get_trade_approvals',
    ]
    source = '''#!__PYTHON__
import json,sys,time
from pathlib import Path
ACCOUNT=__ACCOUNT__
RHS=__RHS__
MODE=__MODE__
TOOLS=__TOOLS__
PORTFOLIO=__PORTFOLIO__
STARTUP_DELAY=__STARTUP_DELAY__
EVENTS=Path(__file__).with_suffix('.events.jsonl')
READY=Path(__file__).with_suffix('.ready')
def emit(name):
    with EVENTS.open('a') as handle:
        handle.write(json.dumps({'event':name,'monotonic':time.monotonic()})+'\\n')
def rows(tool):
    return {
        'get_equity_positions': {'positions': []},
        'get_equity_orders': {'orders': []},
        'get_advanced_orders': {'advanced_orders': []},
        'get_option_positions': {'positions': []},
        'get_option_orders': {'orders': []},
        'get_crypto_positions': {'results': []},
        'get_crypto_orders': {'results': []},
        'get_trade_approvals': {'approvals': []},
    }.get(tool, {})
if STARTUP_DELAY: time.sleep(STARTUP_DELAY)
READY.write_text('ready')
emit('startup')
for line in sys.stdin:
    message=json.loads(line)
    method=message.get('method')
    request_id=message.get('id')
    params=message.get('params') or {}
    reply={}
    if method=='initialize':
        emit('initialize')
        if MODE=='stall_initialize':
            emit('stall:initialize');time.sleep(5)
    elif method=='thread/start':
        emit('thread/start')
        reply={'thread':{'id':'local-stdio'}}
    elif method=='mcpServerStatus/list':
        emit('mcp_status')
        time.sleep(0.08)
        reply={'data':[{'name':'robinhood-trading','runtimeStatus':'connected',
            'tools':{tool:{} for tool in TOOLS}}]}
    elif method=='mcpServer/tool/call':
        tool=params.get('tool'); args=params.get('arguments') or {}
        emit('tool:'+str(tool))
        if MODE=='stall_portfolio' and tool=='get_portfolio':
            emit('stall:portfolio');time.sleep(5); continue
        if MODE=='stall_pagination' and tool=='get_equity_positions' and args.get('cursor'):
            emit('stall:pagination');time.sleep(5); continue
        if tool=='get_accounts':
            data={'accounts':[{'account_number':ACCOUNT,'rhs_account_number':RHS,
                'agentic_allowed':True,'deactivated':False,
                'permanently_deactivated':False,'state':'active'}]}
        elif tool=='get_portfolio':
            data=PORTFOLIO
        elif tool=='get_trade_approval_setting':
            data={'setting':{'account_number':ACCOUNT,'human_must_approve_trades':False}}
        elif tool=='get_equity_positions' and MODE=='stall_pagination' and args.get('cursor') is None:
            data={'positions':[], 'next':'p2'}
        else:
            data=rows(tool)
        reply={'isError':False,'content':[],
                'structuredContent':{'data':data}}
    if request_id is not None:
        print(json.dumps({'id':request_id,'result':reply}),flush=True)
'''
    source = source.replace('__PYTHON__', sys.executable)
    source = source.replace('__ACCOUNT__', repr(ACCOUNT))
    source = source.replace('__RHS__', repr(RHS))
    source = source.replace('__MODE__', repr(mode))
    source = source.replace('__TOOLS__', repr(tools))
    source = source.replace('__PORTFOLIO__', json.dumps(_portfolio()))
    source = source.replace('__STARTUP_DELAY__', repr(float(startup_delay)))
    server_python.write_text(source)
    script.write_text(
        '#!/bin/sh\nexec ' + shlex.quote(sys.executable) + ' '
        + shlex.quote(str(server_python)) + ' "$@"\n')
    os.chmod(script, 0o700)
    return script


def _stdio_server_artifact(script, suffix):
    path=Path(script)
    return path.with_name(path.stem+'-server'+suffix)


def _reset_stdio_artifacts(script):
    for suffix in ('.ready', '.events.jsonl'):
        try:_stdio_server_artifact(script, suffix).unlink()
        except FileNotFoundError:pass


def _wait_stdio_ready(script, timeout=10):
    ready=_stdio_server_artifact(script, '.ready')
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        if ready.is_file():
            return
        time.sleep(.01)
    raise AssertionError('STDIO_SERVER_READY_TIMEOUT')


def _stdio_events(script):
    events=_stdio_server_artifact(script, '.events.jsonl')
    if not events.is_file():
        return []
    return [json.loads(line) for line in events.read_text().splitlines()
            if line.strip()]


class FakeReadTransport:
    def __init__(self, *, account=ACCOUNT, rhs=RHS, rhc=RHC,
                 observed_crypto=False, handler=None):
        self.account = account
        self.rhs = rhs
        self.rhc = rhc
        self.observed_crypto = observed_crypto
        self.handler = handler
        self.calls = []
        self.counts = {}
        self.bound = None
        self.closed = False
        self.deadline_monotonic = None

    def arm_deadline(self, deadline_monotonic):
        if self.deadline_monotonic is not None:
            deadline_monotonic = min(self.deadline_monotonic,
                                     deadline_monotonic)
        self.deadline_monotonic = deadline_monotonic
        return deadline_monotonic

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.closed = True

    def bind(self, account_number, rhs_account_number=None):
        if self.bound is not None and self.bound[0] != account_number:
            raise ValueError('Cannot rebind')
        if rhs_account_number is not None and self.bound is not None \
                and self.bound[1] not in (None, rhs_account_number):
            raise ValueError('Cannot rebind rhs')
        self.bound = (account_number, rhs_account_number)

    def _default(self, tool):
        if tool == 'get_accounts':
            row = {
                'account_number': self.account,
                'rhs_account_number': self.rhs,
                'rhc_account_number': self.rhc,
                'agentic_allowed': True, 'deactivated': False,
                'permanently_deactivated': False, 'state': 'active',
                'brokerage_account_type': 'individual', 'type': 'cash',
                'option_level': 'option_level_0',
            }
            if self.rhs is None:
                row.pop('rhs_account_number')
            if self.rhc is None:
                row.pop('rhc_account_number')
            return {'accounts': [row]}
        if tool == 'get_portfolio':
            return _portfolio()
        if tool == 'get_trade_approval_setting':
            return {'setting': {'account_number': self.account,
                                'human_must_approve_trades': False}}
        if tool == 'get_crypto_orders' and self.observed_crypto:
            return {'crypto_account_number': self.rhc,
                    'rhs_account_number': self.rhs, 'results': []}
        return {
            'get_equity_positions': {'positions': []},
            'get_equity_orders': {'orders': []},
            'get_advanced_orders': {'advanced_orders': []},
            'get_option_positions': {'positions': []},
            'get_option_orders': {'orders': []},
            'get_crypto_positions': {'results': []},
            'get_crypto_orders': {'results': []},
            'get_trade_approvals': {'approvals': []},
        }[tool]

    def __call__(self, tool, args):
        self.calls.append((tool, deepcopy(args)))
        index = self.counts.get(tool, 0)
        self.counts[tool] = index + 1
        if self.handler is not None:
            value = self.handler(self, tool, args, index)
            if value is not None:
                return value
        return {'data': self._default(tool)}


class MutableClock:
    def __init__(self):
        self.value = datetime.fromisoformat(AT)

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += timedelta(seconds=seconds)


class MutableMonotonic:
    def __init__(self):
        self.value = 100.0

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += seconds


class BrokerBundleTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.root.chmod(0o700)
        self.output = self.root / 'bundle'

    def collect(self, transport=None, **kwargs):
        return collect_saved_bundle(
            self.output, account_suffix='0000',
            transport=transport or FakeReadTransport(), **kwargs)

    def test_capture_conditions_raw_receipts_and_019_cli_diagnostic(self):
        source = SourceFixture(self.root / 'source')
        transport = FakeReadTransport()
        before = tree_state(source.root)
        result = self.collect(transport=transport)
        self.assertEqual(result['status'], 'CAPTURED_CONDITIONS_MISSING')
        self.assertFalse(result['ready'])
        self.assertFalse(result['production_ready'])
        self.assertFalse(result['live_enabled'])
        self.assertIsNone(result['baseline_snapshot'])
        self.assertEqual(result['calls'], len(transport.calls))
        self.assertEqual(result['pages'], result['calls'])
        self.assertIn('GETTER_MISSING:futures_positions', result['blockers'])
        self.assertIn('SETTLEMENT_SOURCE_MISSING', result['blockers'])
        self.assertFalse(any(code.startswith('BUNDLE_')
                             for code in result['blockers']))
        self.assertNotIn(ACCOUNT, json.dumps(result))
        raw_files = sorted((self.output / 'raw').glob('*.raw'))
        self.assertEqual(len(raw_files), result['calls'])
        self.assertEqual(stat.S_IMODE(self.output.stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE((self.output / 'raw').stat().st_mode),
                         0o700)
        self.assertEqual(list(self.output.rglob('*.sqlite')), [])
        self.assertTrue(all(stat.S_IMODE(path.stat().st_mode) == 0o600
                            for path in raw_files))
        manifest = json.loads((self.output / 'manifest.json').read_text())
        self.assertEqual(len(manifest['pages']), result['calls'])
        receipts = json.loads(
            (self.output / 'capture-provenance.json').read_text())
        self.assertEqual(receipts['status'], 'CAPTURED_CONDITIONS_MISSING')
        self.assertEqual(len(receipts['page_receipts']), result['calls'])
        self.assertEqual(receipts['raw_chain_sha256'],
                         result['raw_chain_sha256'])
        for page, receipt in zip(manifest['pages'],
                                 receipts['page_receipts']):
            raw = self.output / page['raw_path']
            self.assertEqual(hashlib.sha256(raw.read_bytes()).hexdigest(),
                             page['raw_sha256'])
            self.assertEqual(len(raw.read_bytes()), page['raw_bytes'])
            self.assertEqual(digest(page['args']),
                             page['request_args_sha256'])
            self.assertEqual(receipt['raw_sha256'], page['raw_sha256'])
        for page in manifest['pages']:
            args = page['args']
            if page['tool'] == 'get_option_positions':
                self.assertEqual(set(args) - {'account_number', 'cursor'},
                                 set())
            if page['tool'] == 'get_equity_orders':
                self.assertFalse(set(args) & {
                    'state', 'symbol', 'order_id', 'placed_agent'})
            if page['tool'] == 'get_crypto_positions':
                self.assertEqual(set(args) - {'rhs_account_number', 'cursor'},
                                 set())
                self.assertEqual(args['rhs_account_number'], RHS)
        loaded = load_saved_bundle(self.output)
        normalized = normalize_bundle(loaded, now=datetime.now(timezone.utc))
        self.assertFalse(normalized['ready'])
        self.assertIsNone(normalized['baseline_snapshot'])
        self.assertEqual(tree_state(source.root), before)

        policy = self.root / 'policy.json'
        policy.write_text(json.dumps(source.policy))
        os.chmod(policy, 0o600)
        pins = self.root / 'pins.json'
        pins.write_text(json.dumps(source.expected))
        os.chmod(pins, 0o600)
        now = datetime.now(timezone.utc)
        cli = run_019_cli(
            'historical-baseline', 'plan', '--bundle-root', str(self.output),
            '--policy', str(policy), '--pins', str(pins),
            '--execution-db', str(source.execution),
            '--risk-db', str(source.risk), '--host-dir', str(source.host),
            '--root', str(source.root), '--isolated-root',
            '--cutoff', (now - timedelta(minutes=1)).isoformat(),
            '--expires-at', (now + timedelta(minutes=5)).isoformat())
        self.assertEqual(cli.returncode, 3, cli.stderr)
        output = json.loads(cli.stdout)
        self.assertEqual(output['status'], 'CONDITIONS_MISSING')
        self.assertEqual(output['source']['status'], 'READY')
        self.assertIn('GETTER_MISSING:futures_positions',
                      output['bundle']['blockers'])
        self.assertNotIn(ACCOUNT, cli.stdout)

    def test_observed_crypto_empty_shape_end_to_end_conditions(self):
        source = SourceFixture(self.root / 'source')
        before = tree_state(source.root)
        transport = FakeReadTransport(observed_crypto=True)
        result = self.collect(transport=transport)
        self.assertEqual(result['status'], 'CAPTURED_CONDITIONS_MISSING')
        self.assertFalse(result['ready'])
        self.assertIsNone(result['baseline_snapshot'])
        self.assertEqual(
            result['coverage']['get_crypto_orders']['status'], 'complete')
        self.assertFalse(any('get_crypto_orders' in code
                             for code in result['blockers']))
        self.assertIn('GETTER_MISSING:futures_positions',
                      result['blockers'])
        self.assertIn('SETTLEMENT_SOURCE_MISSING', result['blockers'])
        for tool, args in transport.calls:
            if tool in {'get_crypto_positions', 'get_crypto_orders'}:
                self.assertEqual(set(args) - {'rhs_account_number', 'cursor'},
                                 set())
                self.assertEqual(args['rhs_account_number'], RHS)
                self.assertNotIn(RHC, args.values())
        self.assertNotIn(ACCOUNT, json.dumps(result))
        self.assertNotIn(RHC, json.dumps(result))

        policy = self.root / 'policy.json'
        policy.write_text(json.dumps(source.policy))
        os.chmod(policy, 0o600)
        pins = self.root / 'pins.json'
        pins.write_text(json.dumps(source.expected))
        os.chmod(pins, 0o600)
        now = datetime.now(timezone.utc)
        cli = run_019_cli(
            'historical-baseline', 'plan', '--bundle-root', str(self.output),
            '--policy', str(policy), '--pins', str(pins),
            '--execution-db', str(source.execution),
            '--risk-db', str(source.risk), '--host-dir', str(source.host),
            '--root', str(source.root), '--isolated-root',
            '--cutoff', (now - timedelta(minutes=1)).isoformat(),
            '--expires-at', (now + timedelta(minutes=5)).isoformat())
        self.assertEqual(cli.returncode, 3, cli.stderr)
        output = json.loads(cli.stdout)
        self.assertEqual(output['status'], 'CONDITIONS_MISSING')
        self.assertEqual(output['source']['status'], 'READY')
        self.assertIn('GETTER_MISSING:futures_positions',
                      output['bundle']['blockers'])
        self.assertIn('SETTLEMENT_SOURCE_MISSING',
                      output['bundle']['blockers'])
        self.assertFalse(any('get_crypto_orders' in code
                             for code in output['bundle']['blockers']))
        self.assertNotIn(ACCOUNT, cli.stdout)
        self.assertNotIn(RHC, cli.stdout)
        self.assertEqual(tree_state(source.root), before)

    def test_collector_pass2_crypto_identity_change_is_condition(self):
        def handler(transport, tool, args, index):
            if tool != 'get_crypto_orders':
                return None
            account = RHC if index == 0 else 'RHC-OTHER'
            return {'data': {'crypto_account_number': account,
                             'rhs_account_number': RHS, 'results': []}}

        transport = FakeReadTransport(handler=handler)
        result = self.collect(transport=transport)
        self.assertEqual(result['status'], 'CAPTURED_CONDITIONS_MISSING')
        self.assertIn('BUNDLE_CRYPTO_IDENTITY_MISMATCH:pass2',
                      result['blockers'])
        self.assertNotEqual(
            result['coverage']['get_crypto_orders']['status'], 'complete')
        self.assertIsNone(result['baseline_snapshot'])
        self.assertFalse(result['ready'])

    def test_cli_entry_json_help_and_fake_transport(self):
        help_result = run_019_cli('capture-baseline-bundle', '--help')
        self.assertEqual(help_result.returncode, 0)
        self.assertIn('--output-dir', help_result.stdout)
        args = SimpleNamespace(output_dir=str(self.output),
                               account_suffix='0000',
                               timeout_seconds=30)
        output = io.StringIO()
        with patch('martin_agentic.broker_bundle.CodexBrokerReadTransport',
                   new=lambda **kwargs: FakeReadTransport()), \
                patch('sys.stdout', output):
            code = main(['capture-baseline-bundle',
                         '--output-dir', str(self.output),
                         '--account-suffix', '0000'])
        self.assertEqual(code, 3)
        payload = json.loads(output.getvalue())
        self.assertEqual(payload['status'], 'CAPTURED_CONDITIONS_MISSING')
        self.assertNotIn(ACCOUNT, output.getvalue())
        self.assertTrue((self.output / 'manifest.json').is_file())

    def test_pagination_repeated_empty_and_cursor_loop(self):
        def paged(transport, tool, args, index):
            if tool != 'get_equity_orders':
                return None
            cursor = args.get('cursor')
            if cursor is None:
                return {'data': {'orders': [], 'next': 'page-2'}}
            if cursor == 'page-2':
                return {'data': {'orders': [], 'next': None}}
            raise AssertionError('unexpected cursor')

        result = self.collect(transport=FakeReadTransport(handler=paged))
        self.assertEqual(result['status'], 'CAPTURED_CONDITIONS_MISSING')
        pages = load_saved_bundle(self.output)['pages']
        order_pages = [page for page in pages
                       if page['tool'] == 'get_equity_orders']
        self.assertEqual(len(order_pages), 4)  # two pages in each pass
        first_pass = [index for index, page in enumerate(pages)
                      if page['pass'] == 1]
        second_pass = [index for index, page in enumerate(pages)
                       if page['pass'] == 2]
        self.assertLess(max(first_pass), min(second_pass))

        def loop(transport, tool, args, index):
            if tool == 'get_equity_orders':
                return {'data': {'orders': [], 'next': 'loop'}}
            return None

        loop_output = self.root / 'loop-bundle'
        loop_result = collect_saved_bundle(
            loop_output, account_suffix='0000',
            transport=FakeReadTransport(handler=loop))
        self.assertEqual(loop_result['status'], 'FAILED')
        self.assertIn('COLLECTOR_CURSOR_LOOP', loop_result['blockers'])
        self.assertFalse((loop_output / 'manifest.json').exists())
        self.assertEqual(json.loads(
            (loop_output / 'capture-provenance.json').read_text())['status'],
            'FAILED')

    def test_page_call_byte_and_time_budgets_and_backwards_clock(self):
        def endless(transport, tool, args, index):
            if tool == 'get_equity_orders':
                return {'data': {'orders': [], 'next': f'p-{index}'}}
            return None

        with patch.object(broker_bundle, 'MAX_PAGES_PER_TOOL_PASS', 2):
            result = collect_saved_bundle(
                self.root / 'page-limit', account_suffix='0000',
                transport=FakeReadTransport(handler=endless))
        self.assertEqual(result['status'], 'FAILED')
        self.assertIn('COLLECTOR_PAGE_LIMIT_EXCEEDED', result['blockers'])

        with patch.object(broker_bundle, 'MAX_TOTAL_CALLS', 2):
            result = collect_saved_bundle(
                self.root / 'call-limit', account_suffix='0000',
                transport=FakeReadTransport())
        self.assertEqual(result['status'], 'FAILED')
        self.assertIn('COLLECTOR_CALL_LIMIT_EXCEEDED', result['blockers'])

        with patch.object(broker_bundle, 'MAX_TOTAL_BYTES', 32):
            result = collect_saved_bundle(
                self.root / 'byte-limit', account_suffix='0000',
                transport=FakeReadTransport())
        self.assertEqual(result['status'], 'FAILED')
        self.assertIn('COLLECTOR_TOTAL_BYTES_EXCEEDED', result['blockers'])

        clock = MutableClock()
        monotonic = MutableMonotonic()

        def slow(transport, tool, args, index):
            if tool == 'get_portfolio':
                clock.advance(31)
                monotonic.advance(31)
            return None

        result = collect_saved_bundle(
            self.root / 'time-limit', account_suffix='0000',
            transport=FakeReadTransport(handler=slow),
            clock=clock, monotonic=monotonic)
        self.assertEqual(result['status'], 'FAILED')
        self.assertIn('COLLECTOR_TIME_BUDGET_EXCEEDED', result['blockers'])

        backwards = MutableClock()

        def rewind(transport, tool, args, index):
            if tool == 'get_portfolio':
                backwards.advance(-1)
            return None

        result = collect_saved_bundle(
            self.root / 'clock-limit', account_suffix='0000',
            transport=FakeReadTransport(handler=rewind), clock=backwards)
        self.assertEqual(result['status'], 'FAILED')
        self.assertIn('COLLECTOR_CLOCK_BACKWARDS', result['blockers'])

    def test_local_stdio_absolute_deadline_bounds_blocking_portfolio(self):
        script = _write_stdio_server(self.root, 'stall_portfolio')
        for budget, rpc_timeout in ((2.0, 1.0), (2.0, 0.5)):
            with self.subTest(budget=budget, rpc_timeout=rpc_timeout):
                # Start outside the target operation's budget, wait for the
                # child's explicit ready handshake, then arm the real deadline.
                # This avoids treating fork/exec/Python startup as the target.
                _reset_stdio_artifacts(script)
                transport = CodexBrokerReadTransport(
                    binary=str(script), timeout=rpc_timeout,
                    deadline_monotonic=time.monotonic() + 10.0)
                try:
                    _wait_stdio_ready(script)
                    deadline = time.monotonic() + budget
                    transport.arm_deadline(deadline)
                    output = self.root / f'stdio-ready-{budget}-{rpc_timeout}'
                    started = time.monotonic()
                    with transport:
                        result = collect_saved_bundle(
                            output, account_suffix='0000', transport=transport,
                            timeout_seconds=budget,
                            deadline_monotonic=deadline)
                    elapsed = time.monotonic() - started
                finally:
                    if not transport.rpc.closed:
                        transport.rpc.close(
                            deadline_monotonic=time.monotonic() + 2.0)
                self.assertEqual(result['status'], 'FAILED')
                self.assertIn('COLLECTOR_TRANSPORT_ERROR',
                              result['blockers'])
                self.assertGreaterEqual(result['pages'], 1)
                self.assertLess(elapsed, budget + .5)
                self.assertEqual(transport.rpc.deadline_monotonic, deadline)
                self.assertTrue(transport.rpc.closed)
                self.assertIsNotNone(transport.rpc.process.poll())
                self.assertFalse(transport.rpc.reader.is_alive())
                self.assertFalse(transport.rpc.stderr_reader.is_alive())
                self.assertFalse((output / 'manifest.json').exists())
                self.assertTrue((output / 'capture-provenance.json').is_file())
                events = {row['event'] for row in _stdio_events(script)}
                self.assertIn('tool:get_portfolio', events)
                self.assertIn('stall:portfolio', events)

    def test_local_stdio_slow_pagination_and_initialize_deadline(self):
        script = _write_stdio_server(self.root, 'stall_pagination')
        budget = 2.0
        _reset_stdio_artifacts(script)
        transport = CodexBrokerReadTransport(
            binary=str(script), timeout=1.0,
            deadline_monotonic=time.monotonic() + 10.0)
        try:
            _wait_stdio_ready(script)
            deadline = time.monotonic() + budget
            transport.arm_deadline(deadline)
            started = time.monotonic()
            with transport:
                result = collect_saved_bundle(
                    self.root / 'stdio-pagination', account_suffix='0000',
                    transport=transport, timeout_seconds=budget,
                    deadline_monotonic=deadline)
            elapsed = time.monotonic() - started
        finally:
            if not transport.rpc.closed:
                transport.rpc.close(deadline_monotonic=time.monotonic() + 2.0)
        self.assertEqual(result['status'], 'FAILED')
        self.assertIn('COLLECTOR_TRANSPORT_ERROR', result['blockers'])
        self.assertLess(elapsed, budget + .5)
        self.assertIsNotNone(transport.rpc.process.poll())
        events = {row['event'] for row in _stdio_events(script)}
        self.assertIn('tool:get_equity_positions', events)
        self.assertIn('stall:pagination', events)

        init = _write_stdio_server(self.root, 'stall_initialize')
        budget = 0.5
        _reset_stdio_artifacts(init)
        transport = CodexBrokerReadTransport(
            binary=str(init), timeout=0.8,
            deadline_monotonic=time.monotonic() + 10.0)
        try:
            _wait_stdio_ready(init)
            deadline = time.monotonic() + budget
            transport.arm_deadline(deadline)
            started = time.monotonic()
            with self.assertRaises(TransportError) as raised:
                with transport:
                    pass
            elapsed = time.monotonic() - started
        finally:
            if not transport.rpc.closed:
                transport.rpc.close(deadline_monotonic=time.monotonic() + 2.0)
        self.assertEqual(str(raised.exception), 'APP_SERVER_TOTAL_BUDGET_EXCEEDED')
        self.assertLess(elapsed, budget + .5)
        self.assertTrue(transport.rpc.closed)
        self.assertIsNotNone(transport.rpc.process.poll())
        self.assertFalse(transport.rpc.reader.is_alive())
        events = {row['event'] for row in _stdio_events(init)}
        self.assertIn('stall:initialize', events)

    def test_local_stdio_startup_delay_does_not_refresh_initialization_deadline(self):
        script = _write_stdio_server(
            self.root, 'stall_portfolio', startup_delay=.7)
        _reset_stdio_artifacts(script)
        deadline = time.monotonic() + .5
        transport = CodexBrokerReadTransport(
            binary=str(script), timeout=1.0, deadline_monotonic=deadline)
        started = time.monotonic()
        try:
            with self.assertRaises(TransportError) as raised:
                with transport:
                    pass
        finally:
            if not transport.rpc.closed:
                transport.rpc.close(deadline_monotonic=time.monotonic() + 2.0)
        elapsed = time.monotonic() - started
        self.assertEqual(str(raised.exception),
                         'APP_SERVER_TOTAL_BUDGET_EXCEEDED')
        self.assertLess(elapsed, 1.2)
        self.assertEqual(transport.rpc.deadline_monotonic, deadline)
        self.assertTrue(transport.rpc.closed)
        self.assertIsNotNone(transport.rpc.process.poll())
        self.assertFalse(transport.rpc.reader.is_alive())
        self.assertEqual(_stdio_events(script), [])

    def test_local_stdio_startup_preparation_is_outside_target_deadline(self):
        script = _write_stdio_server(
            self.root, 'stall_portfolio', startup_delay=.6)
        _reset_stdio_artifacts(script)
        transport = CodexBrokerReadTransport(
            binary=str(script), timeout=1.0,
            deadline_monotonic=time.monotonic() + 10.0)
        try:
            started = time.monotonic()
            _wait_stdio_ready(script)
            ready_elapsed = time.monotonic() - started
            deadline = time.monotonic() + 2.0
            transport.arm_deadline(deadline)
            with transport:
                result = collect_saved_bundle(
                    self.root / 'stdio-slow-start', account_suffix='0000',
                    transport=transport, timeout_seconds=2.0,
                    deadline_monotonic=deadline)
        finally:
            if not transport.rpc.closed:
                transport.rpc.close(deadline_monotonic=time.monotonic() + 2.0)
        self.assertGreaterEqual(ready_elapsed, .5)
        self.assertEqual(result['status'], 'FAILED')
        self.assertIn('COLLECTOR_TRANSPORT_ERROR', result['blockers'])
        self.assertEqual(transport.rpc.deadline_monotonic, deadline)
        events = {row['event'] for row in _stdio_events(script)}
        self.assertIn('stall:portfolio', events)

    def test_local_stdio_combined_deadline_from_initialization_is_not_refreshed(self):
        script = _write_stdio_server(self.root, 'stall_portfolio')
        for budget, rpc_timeout in ((.5, 1.0), (.5, .5)):
            with self.subTest(budget=budget, rpc_timeout=rpc_timeout):
                _reset_stdio_artifacts(script)
                deadline = time.monotonic() + budget
                transport = CodexBrokerReadTransport(
                    binary=str(script), timeout=rpc_timeout,
                    deadline_monotonic=deadline)
                result = None
                error = None
                started = time.monotonic()
                try:
                    try:
                        with transport:
                            result = collect_saved_bundle(
                                self.root / f'combined-{budget}-{rpc_timeout}',
                                account_suffix='0000', transport=transport,
                                timeout_seconds=budget,
                                deadline_monotonic=deadline)
                    except TransportError as exc:
                        error = exc
                finally:
                    if not transport.rpc.closed:
                        transport.rpc.close(
                            deadline_monotonic=time.monotonic() + 2.0)
                elapsed = time.monotonic() - started
                # The original deadline is never refreshed or relaxed, even
                # when startup or initialization consumed it before the first
                # collector tool.  Both outcomes are explicit and cleaned.
                self.assertLess(elapsed, budget + .75)
                self.assertEqual(transport.rpc.deadline_monotonic, deadline)
                self.assertTrue(transport.rpc.closed)
                self.assertIsNotNone(transport.rpc.process.poll())
                self.assertFalse(transport.rpc.reader.is_alive())
                self.assertFalse(transport.rpc.stderr_reader.is_alive())
                if error is not None:
                    self.assertEqual(str(error),
                                     'APP_SERVER_TOTAL_BUDGET_EXCEEDED')
                else:
                    self.assertEqual(result['status'], 'FAILED')
                    self.assertTrue(set(result['blockers']) & {
                        'COLLECTOR_TRANSPORT_ERROR',
                        'COLLECTOR_DEADLINE_EXPIRED'})
                events = {row['event'] for row in _stdio_events(script)}
                # A phase is only expected if the child actually initialized;
                # the handshake-free test still proves the total-bound and
                # resource cleanup when startup itself consumes the budget.
                if 'startup' in events:
                    self.assertTrue('initialize' in events or error is not None)

    def test_cli_production_transport_uses_absolute_deadline(self):
        # Cold-start/initialize total budget is its own responsibility. A
        # 0.5s CLI budget that expires before portfolio must not require a
        # portfolio page; portfolio-phase classification is covered by the
        # ready-handshake transport test below.
        script = _write_stdio_server(self.root, 'stall_initialize')
        args = SimpleNamespace(output_dir=str(self.root / 'cli-stdio'),
                               account_suffix='0000', timeout_seconds=0.5)
        output = io.StringIO()
        created = []

        def factory(**kwargs):
            transport = CodexBrokerReadTransport(
                binary=str(script), timeout=1.0,
                deadline_monotonic=kwargs['deadline_monotonic'])
            created.append((transport, kwargs['deadline_monotonic']))
            return transport

        started = time.monotonic()
        try:
            with patch('sys.stdout', output):
                code = run_bundle_cli(args, transport_factory=factory)
            elapsed = time.monotonic() - started
            transport, deadline = created[0]
            closed_after_run = transport.rpc.closed
            poll_after_run = transport.rpc.process.poll()
            reader_after_run = transport.rpc.reader.is_alive()
            stderr_reader_after_run = transport.rpc.stderr_reader.is_alive()
            payload = json.loads(output.getvalue())
            events = {row['event'] for row in _stdio_events(script)}
        finally:
            for transport, _ in created:
                if not transport.rpc.closed:
                    transport.rpc.close(
                        deadline_monotonic=time.monotonic() + 2.0)
        self.assertEqual(code, 4)
        self.assertLess(elapsed, 0.8)
        self.assertEqual(payload['status'], 'FAILED')
        self.assertEqual(payload['blockers'],
                         ['APP_SERVER_TOTAL_BUDGET_EXCEEDED'])
        self.assertEqual(transport.rpc.deadline_monotonic, deadline)
        self.assertTrue(closed_after_run)
        self.assertIsNotNone(poll_after_run)
        self.assertFalse(reader_after_run)
        self.assertFalse(stderr_reader_after_run)
        self.assertNotIn('tool:get_portfolio', events)
        self.assertNotIn(ACCOUNT, output.getvalue())

    def test_deadline_protocol_and_earlier_deadline_not_relaxed(self):
        class NoDeadlineTransport(FakeReadTransport):
            arm_deadline = None

        no_arm = NoDeadlineTransport()
        result = collect_saved_bundle(
            self.root / 'no-deadline', account_suffix='0000',
            transport=no_arm)
        self.assertEqual(result['status'], 'FAILED')
        self.assertIn('COLLECTOR_TRANSPORT_DEADLINE_UNSUPPORTED',
                      result['blockers'])
        self.assertEqual(no_arm.calls, [])

        monotonic = MutableMonotonic()
        transport = FakeReadTransport()
        transport.deadline_monotonic = 120.0

        def over_deadline(transport, tool, args, index):
            if tool == 'get_portfolio':
                monotonic.advance(21)
            return None

        transport.handler = over_deadline
        result = collect_saved_bundle(
            self.root / 'earlier-deadline', account_suffix='0000',
            transport=transport, monotonic=monotonic,
            deadline_monotonic=125.0)
        self.assertEqual(result['status'], 'FAILED')
        self.assertIn('COLLECTOR_TIME_BUDGET_EXCEEDED', result['blockers'])
        self.assertEqual(transport.deadline_monotonic, 120.0)

        monotonic = MutableMonotonic()
        expired = FakeReadTransport()
        result = collect_saved_bundle(
            self.root / 'expired-deadline', account_suffix='0000',
            transport=expired, monotonic=monotonic,
            deadline_monotonic=99.0)
        self.assertEqual(result['status'], 'FAILED')
        self.assertIn('COLLECTOR_DEADLINE_EXPIRED', result['blockers'])
        self.assertEqual(expired.calls, [])

        with self.assertRaises(CollectorError):
            collect_saved_bundle(
                self.root / 'invalid-deadline', account_suffix='0000',
                transport=FakeReadTransport(), deadline_monotonic=float('inf'))

        mono = MutableMonotonic()

        def rewind_mono(transport, tool, args, index):
            if tool == 'get_portfolio':
                mono.value -= 1
            return None

        result = collect_saved_bundle(
            self.root / 'mono-backwards', account_suffix='0000',
            transport=FakeReadTransport(handler=rewind_mono), monotonic=mono)
        self.assertEqual(result['status'], 'FAILED')
        self.assertIn('COLLECTOR_MONOTONIC_BACKWARDS', result['blockers'])

    def test_transport_error_is_error_and_account_identity_ambiguity_rhs(self):
        def error(transport, tool, args, index):
            if tool == 'get_portfolio':
                return {'isError': True, 'content': []}
            return None

        result = collect_saved_bundle(
            self.root / 'iserror', account_suffix='0000',
            transport=FakeReadTransport(handler=error))
        self.assertEqual(result['status'], 'FAILED')
        self.assertIn('COLLECTOR_TOOL_RESULT_ERROR', result['blockers'])
        self.assertTrue(any((self.root / 'iserror' / 'raw').glob('*.raw')))

        def timeout(transport, tool, args, index):
            if tool == 'get_portfolio':
                raise TransportError('APP_SERVER_TIMEOUT')
            return None

        result = collect_saved_bundle(
            self.root / 'timeout', account_suffix='0000',
            transport=FakeReadTransport(handler=timeout))
        self.assertEqual(result['status'], 'FAILED')
        self.assertIn('COLLECTOR_TRANSPORT_ERROR', result['blockers'])

        def changed_identity(transport, tool, args, index):
            if tool == 'get_equity_orders':
                return {'data': {'orders': [{
                    'id': 'other', 'account_number': 'OTHER-0000',
                    'state': 'filled', 'quantity': '1', 'filled': '1',
                    'executions': []}]}}
            return None

        result = collect_saved_bundle(
            self.root / 'identity', account_suffix='0000',
            transport=FakeReadTransport(handler=changed_identity))
        self.assertEqual(result['status'], 'FAILED')
        self.assertIn('COLLECTOR_ACCOUNT_IDENTITY_CHANGED',
                      result['blockers'])

        ambiguous = FakeReadTransport()
        original = ambiguous._default

        def ambiguous_default(tool):
            data = original(tool)
            if tool == 'get_accounts':
                data['accounts'].append({
                    **data['accounts'][0],
                    'account_number': 'OTHER-0000'})
            return data
        ambiguous._default = ambiguous_default
        result = collect_saved_bundle(
            self.root / 'ambiguous', account_suffix='0000',
            transport=ambiguous)
        self.assertEqual(result['status'], 'FAILED')
        self.assertIn('COLLECTOR_ACCOUNT_AMBIGUOUS', result['blockers'])

        no_rhs = FakeReadTransport(rhs=None)
        result = collect_saved_bundle(
            self.root / 'no-rhs', account_suffix='0000', transport=no_rhs)
        self.assertEqual(result['status'], 'CAPTURED_CONDITIONS_MISSING')
        self.assertIn('CRYPTO_RHS_MISSING', result['missing_conditions'])
        self.assertFalse(any(tool in {
            'get_crypto_positions', 'get_crypto_orders'}
            for tool, _ in no_rhs.calls))

    def test_nonempty_unknown_schema_pass2_change_and_tamper(self):
        def unknown_schema(transport, tool, args, index):
            if tool == 'get_crypto_orders':
                return {'data': {'results': [{'id': 'crypto-order'}]}}
            if tool == 'get_trade_approvals':
                return {'data': {'approvals': [{
                    'approval_id': 'a-1', 'account_number': ACCOUNT,
                    'state': 'APPROVAL_STATE_PENDING'}]}}
            return None

        output = self.root / 'unknown'
        result = collect_saved_bundle(
            output, account_suffix='0000',
            transport=FakeReadTransport(handler=unknown_schema))
        self.assertEqual(result['status'], 'CAPTURED_CONDITIONS_MISSING')
        self.assertTrue(any(code.startswith('SCHEMA_UNVERIFIED')
                            for code in result['blockers']))
        self.assertTrue((output / 'manifest.json').is_file())
        pages = load_saved_bundle(output)['pages']
        crypto_raw = next(page for page in pages
                          if page['tool'] == 'get_crypto_orders'
                          and page['pass'] == 1)
        self.assertIn(b'crypto-order',
                      (output / crypto_raw['raw_path']).read_bytes())
        approval_raw = next(page for page in pages
                            if page['tool'] == 'get_trade_approvals'
                            and page['pass'] == 1)
        self.assertIn(b'a-1', (output / approval_raw['raw_path']).read_bytes())

        def mismatch(transport, tool, args, index):
            if tool == 'get_equity_positions':
                if index == 0:
                    return {'data': {'positions': []}}
                return {'data': {'positions': [{
                    'symbol': 'DEMO', 'quantity': '0',
                    'shares_available_for_sells': '0',
                    'shares_held_for_sells': '0',
                    'shares_held_for_asset_transfer': '0',
                    'shares_held_for_options_events': '0',
                    'shares_held_for_stock_grants': '0',
                    'shares_pending_from_options_events': '0'}]}}
            return None

        result = collect_saved_bundle(
            self.root / 'mismatch', account_suffix='0000',
            transport=FakeReadTransport(handler=mismatch))
        self.assertEqual(result['status'], 'CAPTURED_CONDITIONS_MISSING')
        self.assertIn('TWO_PASS_MISMATCH:get_equity_positions',
                      result['blockers'])

        # A complete bundle is only a local capture; modifying raw bytes must
        # fail the real adapter before any plan/validate use.
        raw = next((output / 'raw').glob('*.raw'))
        raw.write_bytes(raw.read_bytes() + b' ')
        with self.assertRaises(Exception):
            load_saved_bundle(output)

    def test_output_conflicts_and_partial_failure_marking(self):
        existing = self.root / 'existing'
        existing.mkdir()
        with self.assertRaises(CollectorError):
            collect_saved_bundle(existing, account_suffix='0000',
                                 transport=FakeReadTransport())

        link = self.root / 'link'
        link.symlink_to(self.root / 'target')
        with self.assertRaises(CollectorError):
            collect_saved_bundle(link, account_suffix='0000',
                                 transport=FakeReadTransport())

        def fail(transport, tool, args, index):
            if tool == 'get_portfolio':
                raise TransportError('APP_SERVER_TIMEOUT')
            return None

        output = self.root / 'partial'
        result = collect_saved_bundle(
            output, account_suffix='0000',
            transport=FakeReadTransport(handler=fail))
        self.assertEqual(result['status'], 'FAILED')
        self.assertFalse((output / 'manifest.json').exists())
        provenance = json.loads(
            (output / 'capture-provenance.json').read_text())
        self.assertEqual(provenance['status'], 'FAILED')
        self.assertEqual(provenance['error']['code'],
                         'COLLECTOR_TRANSPORT_ERROR')
        with self.assertRaises(Exception):
            load_saved_bundle(output)
