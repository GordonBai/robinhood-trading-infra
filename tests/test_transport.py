import json
from pathlib import Path
import sys
import time
import tempfile
from contextlib import nullcontext
from datetime import datetime
from unittest import TestCase
from unittest.mock import patch

from martin_agentic.capture import capture_market, market_events
from martin_agentic.transport import CodexMarketTransport, CodexBrokerReadTransport, StdioRpc, TransportError, safe_read_failure


class FakeRpc:
    def __init__(self, fail=False):
        self.calls = []
        self.closed = False
        self.fail = fail

    def send(self, msg):
        self.calls.append(msg)

    def request(self, method, params):
        self.calls.append({'method': method, 'params': params})
        if method == 'thread/start':
            return {'thread': {'id': 'ephemeral-session'}}
        if method == 'mcpServerStatus/list':
            return {'data': [{'name': 'robinhood-trading', 'tools': {'get_equity_quotes': {}}}]}
        if method == 'mcpServer/tool/call':
            return {'isError': self.fail, 'content': [], 'structuredContent': {'data': {'results': []}}}
        return {}

    def close(self):
        self.closed = True


class TransportTests(TestCase):
    def test_exact_approval_query_is_bound_bounded_and_readonly(self):
        rpc=FakeRpc();transport=CodexBrokerReadTransport(rpc=rpc)
        transport.thread_id='readonly';transport.tools={'get_trade_approvals'};transport.bind('BOUND')
        transport('get_trade_approvals',{'account_number':'BOUND','approval_ids':['exact']})
        calls=len(rpc.calls)
        for args in ({'account_number':'OTHER','approval_ids':['exact']},
                     {'account_number':'BOUND','approval_ids':[]},
                     {'account_number':'BOUND','approval_ids':['x']*101},
                     {'account_number':'BOUND','approval_ids':['x','x']},
                     {'account_number':'BOUND','approval_ids':['x'],'cursor':'page'},
                     {'account_number':'BOUND','approval_ids':['x'],'state':'DECLINED'}):
            with self.assertRaises(ValueError):transport('get_trade_approvals',args)
        with self.assertRaises(ValueError):transport('decline_trade_approval',{'account_number':'BOUND','approval_id':'exact'})
        self.assertEqual(len(rpc.calls),calls)

    def test_arm_deadline_only_tightens_existing_absolute_deadline(self):
        transport = CodexBrokerReadTransport(rpc=FakeRpc())
        early = time.monotonic() + 5
        transport.rpc.deadline_monotonic = early
        self.assertEqual(transport.arm_deadline(time.monotonic() + 10), early)
        tighter = time.monotonic() + 1
        self.assertEqual(transport.arm_deadline(tighter), tighter)
        with self.assertRaises(TransportError):
            transport.arm_deadline(time.monotonic() - 1)
        with self.assertRaises(ValueError):
            transport.arm_deadline(float('inf'))

    def test_option_and_crypto_position_allowlist_and_binding(self):
        rpc = FakeRpc()
        transport = CodexBrokerReadTransport(rpc=rpc)
        transport.thread_id = 'readonly'
        transport.tools = {'get_option_positions', 'get_crypto_positions',
                           'get_option_orders'}
        transport.bind('BOUND', 'RHS-BOUND')
        transport('get_option_positions',
                  {'account_number': 'BOUND', 'cursor': 'page'})
        transport('get_crypto_positions',
                  {'rhs_account_number': 'RHS-BOUND', 'cursor': 'page'})
        calls = len(rpc.calls)
        for tool, args in (
                ('get_option_positions', {'account_number': 'BOUND',
                                          'nonzero': True}),
                ('get_option_positions', {'rhs_account_number': 'RHS-BOUND'}),
                ('get_option_positions', {'account_number': 'BOUND',
                                          'rhc_account_number': 'RHC'}),
                ('get_crypto_positions', {'rhs_account_number': 'RHS-OTHER'}),
                ('get_crypto_positions', {'rhs_account_number': 'RHS-BOUND',
                                          'account_number': 'BOUND'}),
                ('get_crypto_positions', {'rhc_account_number': 'RHC'}),
                ('get_crypto_positions', {'rhs_account_number': 'RHS-BOUND',
                                          'symbol': 'BTC'}),
                ('place_option_order', {'account_number': 'BOUND'}),
                ('review_option_order', {'account_number': 'BOUND'})):
            with self.subTest(tool=tool, args=args), \
                    self.assertRaises(ValueError):
                transport(tool, args)
        self.assertEqual(len(rpc.calls), calls)
        with self.assertRaisesRegex(ValueError, 'crypto account'):
            transport.bind('BOUND', 'RHS-OTHER')
        with self.assertRaisesRegex(ValueError, 'brokerage account'):
            transport.bind('OTHER', 'RHS-BOUND')
        with self.assertRaisesRegex(ValueError, 'Invalid bound crypto'):
            transport.bind('BOUND', '')

    def test_write_and_unknown_fields_never_reach_rpc(self):
        rpc = FakeRpc()
        with CodexMarketTransport(rpc=rpc) as transport:
            before = len(rpc.calls)
            for tool, args in [('place_equity_order', {'symbols': ['AAPL']}),
                               ('get_equity_quotes', {'symbols': ['AAPL'], 'token': 'secret'})]:
                with self.assertRaises(ValueError):
                    transport(tool, args)
            self.assertEqual(len(rpc.calls), before)
        self.assertTrue(rpc.closed)
        self.assertTrue(next(c for c in rpc.calls if c['method'] == 'thread/start')['params']['ephemeral'])
        self.assertFalse(any(c['method'] == 'turn/start' for c in rpc.calls))

    def test_only_discovered_read_tool_is_callable(self):
        with CodexMarketTransport(rpc=FakeRpc()) as transport:
            response = transport('get_equity_quotes', {'symbols': ['AAPL']})
            self.assertFalse(response['isError'])
            transport.tools.clear()
            with self.assertRaisesRegex(TransportError, 'READ_TOOL_UNAVAILABLE'):
                transport('get_equity_quotes', {'symbols': ['AAPL']})

    def test_upstream_tool_error_stops_collection(self):
        with CodexMarketTransport(rpc=FakeRpc(fail=True)) as transport:
            with self.assertRaisesRegex(TransportError, 'ROBINHOOD_READ_FAILED'):
                transport('get_equity_quotes', {'symbols': ['AAPL']})

    def test_schema_failure_closes_session(self):
        rpc = FakeRpc()
        original = rpc.request
        rpc.request = lambda method, params: {} if method == 'thread/start' else original(method, params)
        with self.assertRaisesRegex(TransportError, 'SCHEMA_MISMATCH'):
            with CodexMarketTransport(rpc=rpc):
                pass
        self.assertTrue(rpc.closed)

    def inventory_rpc(self, rows):
        rpc = FakeRpc()
        original = rpc.request
        responses = iter(rows)
        def request(method, params):
            if method == 'mcpServerStatus/list':
                rpc.calls.append({'method': method, 'params': params})
                return next(responses)
            return original(method, params)
        rpc.request = request
        return rpc

    def test_starting_catalog_waits_bounded_then_uses_connected_inventory(self):
        rpc = self.inventory_rpc([
            {'data': [{'name':'robinhood-trading', 'runtimeStatus':'starting', 'tools':{}}]},
            {'data': [{'name':'robinhood-trading', 'runtimeStatus':'connected',
                       'tools':{'get_equity_quotes':{}}}]}])
        with patch('martin_agentic.transport.time.sleep') as sleep:
            with CodexMarketTransport(rpc=rpc) as transport:
                transport('get_equity_quotes', {'symbols':['AAPL']})
            sleep.assert_called_once_with(.25)

    def test_starting_timeout_is_retryable_but_never_uses_cached_catalog(self):
        row = {'data':[{'name':'robinhood-trading','runtimeStatus':'starting',
                        'tools':{'get_equity_quotes':{}}}]}
        rpc = self.inventory_rpc([row, row, row])
        with patch('martin_agentic.transport.time.sleep') as sleep:
            with self.assertRaisesRegex(TransportError, '^MCP_STARTUP_NOT_READY$') as caught:
                with CodexMarketTransport(rpc=rpc):pass
            self.assertEqual(sleep.call_count, 2)
        self.assertTrue(rpc.closed)
        self.assertTrue(safe_read_failure(caught.exception)['retryable'])
        self.assertFalse(any(c['method']=='mcpServer/tool/call' for c in rpc.calls))

    def test_explicit_authentication_state_never_retries_or_uses_cached_tools(self):
        for fields in [{'runtimeStatus':'authenticationRequired'}, {'authStatus':'notLoggedIn'}]:
            rpc = self.inventory_rpc([{'data':[{'name':'robinhood-trading',
                'tools':{'get_equity_quotes':{}}, **fields}]}])
            with patch('martin_agentic.transport.time.sleep') as sleep:
                with self.assertRaisesRegex(TransportError, '^INTERACTIVE_AUTH_OR_APPROVAL_REQUIRED$') as caught:
                    with CodexMarketTransport(rpc=rpc):pass
                sleep.assert_not_called()
            self.assertFalse(safe_read_failure(caught.exception)['retryable'])
            self.assertTrue(rpc.closed)

    def test_discovery_error_is_fixed_retryable_code_without_secret(self):
        rpc = self.inventory_rpc([{'data':[{'name':'robinhood-trading',
            'runtimeStatus':'connected','tools':{},'toolsError':'secret-token'}]}])
        with self.assertRaisesRegex(TransportError, '^MCP_TOOL_DISCOVERY_FAILED$') as caught:
            with CodexMarketTransport(rpc=rpc):pass
        self.assertTrue(safe_read_failure(caught.exception)['retryable'])
        self.assertTrue(rpc.closed)

    def test_only_exact_snapshot_races_are_retryable_value_errors(self):
        for code in ['UNSTABLE_ACCOUNT_SNAPSHOT','UNSTABLE_ORDER_SNAPSHOT','BROKER_COLLECTION_WINDOW_EXCEEDED']:
            self.assertEqual(safe_read_failure(ValueError(code)), {'code':code,'retryable':True})
        for code in ['ACCOUNT_MISMATCH','ORDER_TERMS_MISMATCH','SCHEMA_MISMATCH',
                     'UNSTABLE_ACCOUNT_SNAPSHOT secret-token']:
            self.assertEqual(safe_read_failure(ValueError(code)),
                {'code':'BROKER_READ_UNCLASSIFIED','retryable':False})
        self.assertFalse(safe_read_failure(RuntimeError('UNSTABLE_ACCOUNT_SNAPSHOT'))['retryable'])

    def test_missing_disabled_and_empty_connected_catalog_fail_without_wait(self):
        for rows in [[], [{'name':'robinhood-trading','runtimeStatus':'disabled','tools':{'get_equity_quotes':{}}}],
                     [{'name':'robinhood-trading','runtimeStatus':'connected','tools':{}}]]:
            rpc = self.inventory_rpc([{'data':rows}])
            with patch('martin_agentic.transport.time.sleep') as sleep:
                with self.assertRaisesRegex(TransportError, '^ROBINHOOD_TOOLS_UNAVAILABLE$') as caught:
                    with CodexMarketTransport(rpc=rpc):pass
                sleep.assert_not_called()
            self.assertFalse(safe_read_failure(caught.exception)['retryable'])
            self.assertTrue(rpc.closed)

    def test_paginated_catalog_is_collected_before_deciding_unavailable(self):
        rpc = self.inventory_rpc([
            {'data':[{'name':'another-server','tools':{}}], 'nextCursor':'page-2'},
            {'data':[{'name':'robinhood-trading','runtimeStatus':'connected',
                      'tools':{'get_equity_quotes':{}}}]}])
        with CodexMarketTransport(rpc=rpc) as transport:
            self.assertIn('get_equity_quotes', transport.tools)
        calls=[c for c in rpc.calls if c['method']=='mcpServerStatus/list']
        self.assertEqual(calls[-1]['params']['cursor'],'page-2')

    def test_malformed_catalog_fails_closed_with_fixed_schema_error(self):
        for response in [{'data':None}, {'data':[None]},
                         {'data':[{'name':'robinhood-trading','tools':[]}]}]:
            rpc=self.inventory_rpc([response])
            with self.assertRaisesRegex(TransportError, '^APP_SERVER_SCHEMA_MISMATCH$'):
                with CodexMarketTransport(rpc=rpc):pass
            self.assertTrue(rpc.closed)

    def rpc(self, source, timeout=1):
        rpc = StdioRpc([sys.executable, '-u', '-c', source], timeout=timeout)
        self.addCleanup(rpc.close)
        return rpc

    def test_protocol_notifications_and_response(self):
        rpc = self.rpc("import sys,json; r=json.loads(sys.stdin.readline()); print(json.dumps({'method':'notice'})); print(json.dumps({'id':r['id'],'result':{'ok':True}}))")
        self.assertTrue(rpc.request('probe', {})['ok'])

    def test_interactive_requests_are_rejected(self):
        rpc = self.rpc("import sys,json; sys.stdin.readline(); print(json.dumps({'id':50,'method':'item/permissions/requestApproval'})); sys.stdin.readline()")
        with self.assertRaisesRegex(TransportError, 'INTERACTIVE_AUTH_OR_APPROVAL_REQUIRED'):
            rpc.request('probe', {})

    def test_interaction_stays_nonretryable_when_refusal_crosses_total_deadline(self):
        import io
        import queue
        from types import SimpleNamespace
        rpc=StdioRpc.__new__(StdioRpc)
        rpc.timeout=1;rpc.deadline_monotonic=10;rpc.sequence=0
        rpc.messages=queue.Queue();rpc.process=SimpleNamespace(stdin=io.StringIO())
        rpc.messages.put({'id':50,'method':'item/permissions/requestApproval'})
        with patch('martin_agentic.transport.time.monotonic',side_effect=[9,9,9,11]):
            with self.assertRaisesRegex(TransportError,'^INTERACTIVE_AUTH_OR_APPROVAL_REQUIRED$') as caught:
                rpc.request('probe',{})
        self.assertFalse(safe_read_failure(caught.exception)['retryable'])
        # Only the initial read request reached the stream; no approval reply.
        self.assertEqual(len(rpc.process.stdin.getvalue().splitlines()),1)

    def test_rpc_error_does_not_leak_upstream_credentials(self):
        rpc = self.rpc("import sys,json; r=json.loads(sys.stdin.readline()); print(json.dumps({'id':r['id'],'error':{'message':'secret-token'}}))")
        with self.assertRaises(TransportError) as caught:
            rpc.request('probe', {})
        self.assertEqual(str(caught.exception), 'APP_SERVER_RPC_ERROR')

    def test_host_permission_failure_is_classified_without_stderr_leak(self):
        rpc = self.rpc("import sys; sys.stdin.readline(); sys.stderr.write('Operation not permitted secret-token'); sys.exit(1)")
        with self.assertRaisesRegex(TransportError, '^APP_SERVER_HOST_PERMISSION_DENIED$'):
            rpc.request('probe', {})

    def test_large_stderr_does_not_block_protocol(self):
        rpc = self.rpc("import sys,json; r=json.loads(sys.stdin.readline()); sys.stderr.write('x'*100000); sys.stderr.flush(); print(json.dumps({'id':r['id'],'result':{'ok':True}}))")
        self.assertTrue(rpc.request('probe', {})['ok'])

    def test_harmless_permission_warning_does_not_relabel_disconnect(self):
        rpc = self.rpc("import sys; sys.stdin.readline(); sys.stderr.write('WARNING: PATH aliases: Operation not permitted\\n'); sys.exit(1)")
        with self.assertRaisesRegex(TransportError, '^APP_SERVER_DISCONNECTED$'):
            rpc.request('probe', {})

    def test_fatal_startup_is_distinguished_from_broker_mcp_failure(self):
        rpc = self.rpc("import sys; sys.stdin.readline(); sys.stderr.write('WARNING: PATH: Operation not permitted\\nError: local runtime state secret-token\\n'); sys.exit(1)")
        with self.assertRaisesRegex(TransportError, '^APP_SERVER_STARTUP_FAILED$'):
            rpc.request('initialize', {})

    def test_timeout_and_disconnection(self):
        rpc = self.rpc('import sys,time; sys.stdin.readline(); time.sleep(3)', timeout=.1)
        with self.assertRaisesRegex(TransportError, 'TIMEOUT'):
            rpc.request('probe', {})
        rpc.close()
        rpc = self.rpc('import sys; sys.stdin.readline()')
        with self.assertRaisesRegex(TransportError, 'DISCONNECTED'):
            rpc.request('probe', {})

    def test_crashed_child_failed_write_still_closes_all_pipes(self):
        rpc=self.rpc('import time; time.sleep(60)')
        rpc.process.kill();rpc.process.wait(timeout=3)
        with self.assertRaisesRegex(TransportError,'^APP_SERVER_PIPE_CLOSED$'):
            rpc.send({'method':'probe-after-child-crash'})
        rpc.close()
        self.assertTrue(rpc.closed)
        self.assertTrue(all(stream.closed for stream in
                            (rpc.process.stdin,rpc.process.stdout,rpc.process.stderr)))
        self.assertIsNotNone(rpc.process.poll())
        self.assertFalse(rpc.reader.is_alive())
        self.assertFalse(rpc.stderr_reader.is_alive())
        rpc.close()

    def test_absolute_budget_is_shared_across_multiple_rpc_calls(self):
        source="import sys,json,time\nfor line in sys.stdin:\n r=json.loads(line);time.sleep(r['params'].get('delay',0));print(json.dumps({'id':r['id'],'result':{'ok':True}}),flush=True)"
        rpc=self.rpc(source,timeout=2)
        self.assertTrue(rpc.request('warmup',{})['ok'])
        started=time.monotonic();rpc.deadline_monotonic=started+.4
        with self.assertRaisesRegex(TransportError,'^APP_SERVER_TOTAL_BUDGET_EXCEEDED$'):
            for _ in range(3):rpc.request('slow-page',{'delay':.15})
        self.assertLess(time.monotonic()-started,1.5)

    def test_expired_absolute_budget_never_spawns_a_client(self):
        with patch('martin_agentic.transport.subprocess.Popen') as spawn:
            with self.assertRaisesRegex(TransportError,'^APP_SERVER_TOTAL_BUDGET_EXCEEDED$'):
                StdioRpc(['unused'],deadline_monotonic=time.monotonic()-1)
        spawn.assert_not_called()

    def test_injected_rpc_cannot_silently_ignore_an_absolute_deadline(self):
        with self.assertRaisesRegex(ValueError,'INJECTED_RPC_MUST_CONFIGURE'):
            CodexMarketTransport(rpc=FakeRpc(),deadline_monotonic=time.monotonic()+10)

    def test_mismatched_response_cannot_be_used(self):
        rpc = self.rpc("import sys,json; sys.stdin.readline(); print(json.dumps({'id':999,'result':{}}))")
        with self.assertRaisesRegex(TransportError, 'ID_MISMATCH'):
            rpc.request('probe', {})

    def test_invalid_request_is_rejected_before_connecting(self):
        def forbidden():
            self.fail('Transport should not open')
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(ValueError):
                capture_market([{'tool': 'place_equity_order', 'arguments': {}}], directory,
                               transport_factory=forbidden)
            with self.assertRaises(ValueError):
                capture_market([{'tool': 'get_equity_quotes', 'arguments': []}], directory,
                               transport_factory=forbidden)
            with self.assertRaises(ValueError):
                capture_market([{'tool':'get_equity_quotes','arguments':{'symbols':['AAPL']}}],
                    directory, inbox=directory, transport_factory=forbidden)

    def test_authentication_failure_publishes_pause(self):
        with tempfile.TemporaryDirectory() as directory:
            inbox = Path(directory) / 'events'
            def fail():
                raise TransportError('APP_SERVER_RPC_ERROR')
            with self.assertRaises(TransportError):
                capture_market([{'tool':'get_equity_quotes','arguments':{'symbols':['AAPL']}}],
                    Path(directory)/'captures', inbox=inbox, transport_factory=fail)
            events = json.loads(next(inbox.glob('*.json')).read_text())['events']
            self.assertFalse(events[0]['healthy'])

    def test_stale_batch_has_no_trade_inputs(self):
        recording = json.loads((Path(__file__).resolve().parents[1]/'tests/fixtures/synthetic_recording.json').read_text())
        events, normalized = market_events(recording, as_of='2026-10-15T17:44:03Z')
        self.assertFalse(normalized['data_usable'])
        self.assertEqual([e['kind'] for e in events], ['data_status'])
        self.assertFalse(events[0]['healthy'])

    def test_valid_capture_is_atomic_and_completion_pauses_source(self):
        now = datetime.fromisoformat('2026-10-15T14:00:00+00:00')
        response = {'data': {'results': [{'quote': {'symbol': 'AAPL', 'state': 'active',
            'has_traded': True, 'bid_price': '100', 'ask_price': '100.01',
            'venue_bid_time': now.isoformat(), 'venue_ask_time': now.isoformat()}}]}}
        with tempfile.TemporaryDirectory() as directory:
            inbox = Path(directory) / 'events'
            output = capture_market([{'tool':'get_equity_quotes','arguments':{'symbols':['AAPL']}}],
                Path(directory)/'captures', inbox=inbox, clock=lambda:now,
                transport_factory=lambda:nullcontext(lambda tool,args:response))
            self.assertTrue(output['data_usable'])
            self.assertFalse(output['execution_ready'])
            self.assertEqual(output['captures'][0]['quotes'], 1)
            self.assertFalse(list(Path(directory).rglob('*.tmp')))
            events = [e for p in inbox.glob('*.json') for e in json.loads(p.read_text())['events']]
            self.assertEqual(sum(e['kind']=='quote' for e in events), 1)
            self.assertTrue(any(e.get('healthy') is False for e in events))
