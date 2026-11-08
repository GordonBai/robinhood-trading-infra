"""Isolated production review transport logic; non-formal fake RPC evidence."""
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import hashlib
import json
import tempfile
import threading
from unittest import TestCase
from unittest.mock import patch

from martin_agentic.audit import digest
from martin_agentic.formal_review_ingress import (
    FormalReviewIngress, FormalReviewIngressError, TOOL)
from martin_agentic.host_execution import HostExecutionBridge
from martin_agentic.host_mailbox import (
    read_private_json, submit_engineering_intent, submit_request)
from martin_agentic.native_receipt_source import NativeReceiptEvidenceReader
from martin_agentic.native_review import EquityReviewTransport
from martin_agentic.transport import StdioRpc, TransportError
import test_engineering_source as matrix

NOW = datetime(2026, 10, 16, 14, 0, tzinfo=timezone.utc)
ACCOUNT = 'TEST-0000'


class FakeRpc:
    def __init__(self, *, accounts=None, review_error=False, review_wait=False,
                 fail_next_review=None):
        self.sequence = 0
        self.last_request_id = None
        self.accounts = accounts if accounts is not None else [{
            'account_number': ACCOUNT, 'agentic_allowed': True,
            'deactivated': False, 'permanently_deactivated': False,
            'state': 'active'}]
        self.review_error = review_error
        self.review_wait = review_wait
        self.fail_next_review = fail_next_review
        self.fail_next_review_exception = None
        self.calls = []

    def request(self, method, params):
        self.sequence += 1
        self.last_request_id = self.sequence
        self.calls.append((method, params))
        if params.get('tool') == 'get_accounts':
            return {'data': {'accounts': self.accounts}}
        if self.fail_next_review_exception is not None:
            exc = self.fail_next_review_exception
            self.fail_next_review_exception = None
            raise exc
        if self.fail_next_review == 'timeout':
            self.fail_next_review = None
            raise TransportError('APP_SERVER_TIMEOUT')
        if self.fail_next_review == 'iserror':
            self.fail_next_review = None
            return {'isError': True}
        if self.review_error:
            return {'isError': True}
        return {'data': {**params['arguments'], 'order_checks': {},
                         'customer_approval_required': False,
                         'market_data_disclosure': 'Exact disclosure',
                         'quote_data': {}}}


def packet(account_key):
    return {
        'operation_id': 'native-00000000-0000-4000-8000-000000000001',
        'action_id': 'action-1', 'operation': 'review',
        'tool': 'review_equity_order', 'account': '••••0000',
        'account_key': account_key, 'logical_ref_id': 'ref-1',
        'parameters': {'symbol': 'DEMO', 'side': 'buy', 'type': 'limit',
                       'quantity': '2', 'limit_price': '40',
                       'time_in_force': 'gfd',
                       'market_hours': 'regular_hours'}}


class ProductionReviewTransportTests(TestCase):
    def make(self, rpc):
        transport = EquityReviewTransport(rpc=rpc)
        transport.thread_id = 'thread-1'
        transport.tools = {'get_accounts', 'review_equity_order'}
        transport.tool_descriptors = {
            'review_equity_order': {'name': 'review_equity_order'}}
        return transport

    def test_get_accounts_then_review_exact_args_and_call_record(self):
        account_key = hashlib.sha256(ACCOUNT.encode()).hexdigest()
        rpc = FakeRpc()
        transport = self.make(rpc)
        response = transport.execute_review(packet(account_key))
        self.assertEqual(response['data']['symbol'], 'DEMO')
        self.assertEqual([c[1].get('tool') for c in rpc.calls],
                         ['get_accounts', 'review_equity_order'])
        call = transport.last_review_call
        self.assertEqual(call['status'], 'received')
        self.assertEqual(call['rpc_request_id'], '2')
        self.assertEqual(call['account_scope_sha256'], account_key)
        self.assertEqual(call['tool'], 'review_equity_order')
        self.assertEqual(call['server'], 'robinhood-trading')
        self.assertEqual(call['thread_id'], 'thread-1')
        self.assertEqual(call['connection_id'], transport.connection_id)
        self.assertEqual(call['arguments_sha256'], digest(
            {'account_number': ACCOUNT, **packet(account_key)['parameters']}))
        self.assertEqual(call['parameters_sha256'],
                         digest(packet(account_key)['parameters']))
        self.assertEqual(call['descriptor_sha256'],
                         digest(transport.tool_descriptors['review_equity_order']))
        self.assertEqual(call['response_sha256'], digest(response))
        self.assertIsInstance(call['response_identity_sha256'], str)
        self.assertLessEqual(
            datetime.fromisoformat(call['started_at']),
            datetime.fromisoformat(call['ended_at']))

    def test_inactive_or_ambiguous_account_blocks(self):
        account_key = hashlib.sha256(ACCOUNT.encode()).hexdigest()
        inactive = [{**FakeRpc().accounts[0], 'state': 'closed'}]
        with self.assertRaisesRegex(ValueError, 'TARGET_ACCOUNT_NOT_ACTIVE'):
            self.make(FakeRpc(accounts=inactive)).execute_review(
                packet(account_key))
        duplicate = [FakeRpc().accounts[0], FakeRpc().accounts[0]]
        with self.assertRaisesRegex(ValueError,
                                    'TARGET_ACCOUNT_NOT_UNIQUELY_ACCESSIBLE'):
            self.make(FakeRpc(accounts=duplicate)).execute_review(
                packet(account_key))
        not_allowed = [{**FakeRpc().accounts[0], 'agentic_allowed': False}]
        with self.assertRaisesRegex(
                ValueError, 'TARGET_ACCOUNT_NOT_ACTIVE_OR_ALLOWED'):
            self.make(FakeRpc(accounts=not_allowed)).execute_review(
                packet(account_key))
        with self.assertRaisesRegex(ValueError,
                                    'REVIEW_ACCOUNT_BINDING_CHANGED'):
            self.make(FakeRpc()).execute_review(packet('f' * 64))

    def test_timeout_and_error_replace_previous_call(self):
        account_key = hashlib.sha256(ACCOUNT.encode()).hexdigest()
        rpc = FakeRpc()
        transport = self.make(rpc)
        packet_value = packet(account_key)
        first = transport.execute_review(packet_value)
        self.assertEqual(transport.last_review_call['status'], 'received')
        self.assertEqual(transport.last_review_call['rpc_request_id'], '2')
        rpc.fail_next_review = 'timeout'
        with self.assertRaisesRegex(TransportError, 'APP_SERVER_TIMEOUT'):
            transport.execute_review(packet_value)
        failed = transport.last_review_call
        self.assertEqual(failed['status'], 'failed')
        self.assertEqual(failed['reason'], 'APP_SERVER_TIMEOUT')
        self.assertEqual(failed['rpc_request_id'], '4')
        self.assertEqual(failed['tool'], 'review_equity_order')
        self.assertEqual(failed['parameters_sha256'],
                         digest(packet_value['parameters']))
        self.assertIsNone(failed['response_sha256'])
        rpc.fail_next_review = 'iserror'
        with self.assertRaisesRegex(TransportError, 'ROBINHOOD_READ_FAILED'):
            transport.execute_review(packet_value)
        errored = transport.last_review_call
        self.assertEqual(errored['status'], 'failed')
        self.assertEqual(errored['reason'], 'RESPONSE_ERROR')
        self.assertEqual(errored['rpc_request_id'], '6')
        self.assertIsNone(errored['response_identity_sha256'])
        review_calls = [params for method, params in rpc.calls
                        if params.get('tool') == 'review_equity_order']
        self.assertEqual(review_calls[0]['arguments'],
                         {'account_number': ACCOUNT,
                          **packet_value['parameters']})
        self.assertEqual(review_calls[0]['server'], 'robinhood-trading')
        self.assertEqual(review_calls[0]['threadId'], 'thread-1')

    def test_unknown_failure_reason_is_fixed_and_never_persists_sentinel(self):
        account_key = hashlib.sha256(ACCOUNT.encode()).hexdigest()
        sentinel = 'PRIVATE-UPSTREAM-SENTINEL-061r11'
        for kind in ('message', 'code'):
            with self.subTest(kind=kind):
                rpc = FakeRpc()
                transport = self.make(rpc)
                packet_value = packet(account_key)
                if kind == 'message':
                    exc = RuntimeError(sentinel)
                else:
                    exc = RuntimeError('upstream detail')
                    exc.code = sentinel
                rpc.fail_next_review_exception = exc
                with self.assertRaises(RuntimeError):
                    transport.execute_review(packet_value)
                failed = transport.last_review_call
                self.assertEqual(failed['status'], 'failed')
                self.assertEqual(failed['reason'], 'BROKER_READ_UNCLASSIFIED')
                self.assertEqual(failed['rpc_request_id'], '2')
                self.assertEqual(failed['tool'], 'review_equity_order')
                self.assertEqual(failed['parameters_sha256'],
                                 digest(packet_value['parameters']))
                self.assertIsNone(failed['response_sha256'])
                self.assertNotIn(sentinel, json.dumps(failed, sort_keys=True))
                self.assertFalse(json.dumps(failed, sort_keys=True).find(sentinel) >= 0)

    def test_unknown_failure_ingress_artifact_has_no_sentinel(self):
        sentinel = 'PRIVATE-UPSTREAM-SENTINEL-061r11-INGRESS'
        with host_review_fixture() as (fixture, bridge, request_id, packet):
            temp = tempfile.TemporaryDirectory()
            self.addCleanup(temp.cleanup)
            root = __import__('pathlib').Path(temp.name) / 'ingress'
            class FailingScripted(ScriptedReviewTransport):
                def execute_review(self, packet):
                    self.calls += 1
                    raise RuntimeError(sentinel)
            transport = FailingScripted()
            adapter = FormalReviewIngress(root, clock=fixture._clock)
            with self.assertRaisesRegex(FormalReviewIngressError,
                                        'TRANSPORT_FAILED'):
                adapter.receive(
                    transport=transport, packet=packet,
                    request_id=request_id,
                    host_directory=bridge.directory, binding=bridge.binding)
            artifact = root / (packet['operation_id'] + '.json')
            raw = artifact.read_text()
            self.assertNotIn(sentinel, raw)
            value = json.loads(raw)
            self.assertEqual(value['status'], 'failed')
            self.assertEqual(value['failure'], 'BROKER_READ_UNCLASSIFIED')
            self.assertEqual(value['origin'], 'test_scripted_transport')
            self.assertEqual(transport.calls, 1)
            retry = FormalReviewIngress(root, clock=fixture._clock)
            with self.assertRaises(FormalReviewIngressError):
                retry.receive(
                    transport=transport, packet=packet,
                    request_id=request_id,
                    host_directory=bridge.directory, binding=bridge.binding)
            self.assertEqual(transport.calls, 1)

    def test_unconnected_or_missing_tool_blocks(self):
        account_key = hashlib.sha256(ACCOUNT.encode()).hexdigest()
        packet_value = packet(account_key)
        transport = self.make(FakeRpc())
        transport.thread_id = None
        with self.assertRaisesRegex(TransportError, 'READ_TOOL_UNAVAILABLE'):
            transport.execute_review(packet_value)
        transport = self.make(FakeRpc())
        transport.tools.discard('review_equity_order')
        with self.assertRaisesRegex(TransportError, 'READ_TOOL_UNAVAILABLE'):
            transport.execute_review(packet_value)

    def test_review_error_and_injected_rpc_is_not_formal(self):
        account_key = hashlib.sha256(ACCOUNT.encode()).hexdigest()
        with self.assertRaisesRegex(Exception, 'ROBINHOOD_READ_FAILED'):
            self.make(FakeRpc(review_error=True)).execute_review(
                packet(account_key))
        rpc = FakeRpc()
        transport = self.make(rpc)
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        adapter = FormalReviewIngress(temp.name, clock=lambda: NOW)
        with self.assertRaisesRegex(FormalReviewIngressError,
                                    'RPC_NOT_FORMAL'):
            adapter._formal(transport)
        transport.rpc = object.__new__(StdioRpc)
        transport.tool_descriptors = {}
        with self.assertRaisesRegex(FormalReviewIngressError,
                                    'TOOL_DESCRIPTOR_UNAVAILABLE'):
            adapter._formal(transport)
        transport.tools.discard('review_equity_order')
        with self.assertRaisesRegex(FormalReviewIngressError,
                                    'TOOL_DESCRIPTOR_UNAVAILABLE'):
            adapter._formal(transport)


class ScriptedReviewTransport:
    simulation_only = True
    tools = {'review_equity_order'}
    last_call_id = 'scripted-call-1'
    def __init__(self, *, block=None, release=None):
        self.calls = 0
        self.block = block
        self.release = release

    def execute_review(self, packet):
        self.calls += 1
        if self.block is not None:
            self.block.set()
        if self.release is not None:
            if not self.release.wait(timeout=5):
                raise TimeoutError('test release timeout')
        return {'data': {**packet['parameters'], 'order_checks': {},
                         'customer_approval_required': False,
                         'market_data_disclosure': 'Exact disclosure',
                         'quote_data': {}}}


class _ScriptedRpc:
    def __init__(self):
        self.closed = False
    def close(self, *args, **kwargs):
        self.closed = True


class ScriptedEntryTransport(ScriptedReviewTransport):
    test_only = True
    constructed = 0
    instances = []

    def __init__(self, **kwargs):
        super().__init__()
        self.rpc = _ScriptedRpc()
        type(self).constructed += 1
        type(self).instances.append(self)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class FailingEntryTransport(ScriptedEntryTransport):
    failure = TransportError('READ_TOOL_UNAVAILABLE')

    def execute_review(self, packet):
        self.calls += 1
        raise self.failure


class TimeoutEntryTransport(ScriptedEntryTransport):
    def execute_review(self, packet):
        self.calls += 1
        raise TransportError('APP_SERVER_TIMEOUT')


class IserrorEntryTransport(ScriptedEntryTransport):
    def execute_review(self, packet):
        self.calls += 1
        return {'isError': True}


class ExpiringEntryTransport(ScriptedEntryTransport):
    clock_fn = None

    def execute_review(self, packet):
        self.calls += 1
        type(self).clock_fn(NOW + timedelta(seconds=20))
        return super().execute_review(packet)


class ExpireOnConnectEntryTransport(ScriptedEntryTransport):
    clock_fn = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if type(self).clock_fn is not None:
            type(self).clock_fn(NOW + timedelta(seconds=20))


class IncompleteCleanupEntryTransport(ScriptedEntryTransport):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.rpc.close = lambda *args, **kwargs: {
            'complete': False, 'reader_alive': True}


class RaisingCleanupEntryTransport(ScriptedEntryTransport):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.rpc.close = lambda *args, **kwargs: (
            (_ for _ in ()).throw(RuntimeError('close failed')))


@contextmanager
def host_review_fixture():
    with matrix.RealDryFixture(prebuilt_intent=False, lineage=False) as fixture:
        fixture._clock_value[0] = NOW
        state = fixture.book.state()
        state['account_key'] = fixture.account_key
        fixture.book._save(state)
        fixture.book.db.commit()
        bridge = HostExecutionBridge(
            fixture.book, fixture.root / 'host',
            engineering_owner_root=fixture.owner)
        try:
            submit_engineering_intent(
                bridge.directory, 'engineering-real-dry',
                fixture._plan(), {
                    'synthetic': False, 'source_sha256': 'fixture',
                    'engineering_scope_authorized': True,
                    'parameters_user_specified': True,
                    'parameters_valid_until': (
                        NOW + timedelta(minutes=30)).isoformat()},
                clock=lambda: NOW)
            out = bridge(snapshot=fixture.snap,
                         quotes={'DEMO': fixture._quote()},
                         allow_entries=False)
            if out['status'] != 'HOST_ENGINEERING_INTENT_RECORDED':
                raise AssertionError(out)
            request = submit_request(
                bridge.directory, out['action_id'], 'review',
                clock=lambda: NOW)
            prepared = bridge(snapshot=fixture.snap,
                              quotes={'DEMO': fixture._quote()},
                              allow_entries=False)
            if prepared['status'] != 'HOST_PACKET_PREPARED':
                raise AssertionError(prepared)
            packet = read_private_json(
                bridge.directory / 'packets' /
                (request['request_id'] + '.json'))['packet']
            yield fixture, bridge, request['request_id'], packet
        finally:
            bridge.close()


class HostIngressConcurrencyTests(TestCase):
    def test_concurrent_same_operation_executes_once(self):
        with host_review_fixture() as (fixture, bridge, request_id, packet):
            temp = tempfile.TemporaryDirectory()
            self.addCleanup(temp.cleanup)
            root_path = __import__('pathlib').Path(temp.name) / 'ingress'
            started = threading.Event()
            release = threading.Event()
            first_transport = ScriptedReviewTransport(
                block=started, release=release)
            second_transport = ScriptedReviewTransport()
            first = FormalReviewIngress(root_path, clock=fixture._clock)
            second = FormalReviewIngress(root_path, clock=fixture._clock)
            results = {}
            errors = {}
            def run_first():
                try:
                    results['first'] = first.receive(
                        transport=first_transport, packet=packet,
                        request_id=request_id,
                        host_directory=bridge.directory,
                        binding=bridge.binding)
                except Exception as exc:
                    errors['first'] = exc
            thread = threading.Thread(target=run_first, daemon=True)
            thread.start()
            try:
                self.assertTrue(started.wait(timeout=5))
                with self.assertRaisesRegex(
                        FormalReviewIngressError, 'ALREADY_CLAIMED'):
                    second.receive(
                        transport=second_transport, packet=packet,
                        request_id=request_id,
                        host_directory=bridge.directory,
                        binding=bridge.binding)
            finally:
                release.set()
                thread.join(timeout=10)
            self.assertFalse(thread.is_alive())
            self.assertNotIn('first', errors)
            self.assertEqual(results['first']['status'], 'received')
            replay = first.receive(
                transport=first_transport, packet=packet,
                request_id=request_id,
                host_directory=bridge.directory,
                binding=bridge.binding)
            self.assertEqual(replay, results['first'])
            self.assertEqual(first_transport.calls, 1)
            self.assertEqual(second_transport.calls, 0)
            files = {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                     for p in root_path.glob('*.json')}
            self.assertEqual(set(files), {
                packet['operation_id'] + '.json',
                packet['operation_id'] + '.journal.json'})
            self.assertEqual(digest(json.loads(
                (root_path / (packet['operation_id'] + '.json')).read_text())),
                digest(results['first']))
            journal = json.loads((root_path / (
                packet['operation_id'] + '.journal.json')).read_text())
            self.assertEqual(journal['packet_sha256'], digest(packet))

    def test_expired_journal_recovers_without_transport_original_time(self):
        with host_review_fixture() as (fixture, bridge, request_id, packet):
            temp = tempfile.TemporaryDirectory()
            self.addCleanup(temp.cleanup)
            root_path = __import__('pathlib').Path(temp.name) / 'ingress'
            transport = ScriptedReviewTransport()
            adapter = FormalReviewIngress(root_path, clock=fixture._clock)
            import martin_agentic.formal_review_ingress as module
            with patch.object(module, 'submit_receipt',
                              side_effect=OSError('injected receipt failure')):
                with self.assertRaisesRegex(ValueError,
                                            'RECEIPT_PERSISTENCE_FAILED'):
                    adapter.receive(
                        transport=transport, packet=packet,
                        request_id=request_id,
                        host_directory=bridge.directory,
                        binding=bridge.binding)
            journal_path = (root_path / (packet['operation_id'] +
                                         '.journal.json'))
            journal = json.loads(journal_path.read_text())
            original_time = journal['response_received_at']
            fixture._clock_value[0] = NOW + timedelta(seconds=120)
            recovered = FormalReviewIngress(
                root_path, clock=fixture._clock).receive(
                    transport=None, packet=packet, request_id=request_id,
                    host_directory=bridge.directory, binding=bridge.binding)
            self.assertTrue(recovered['historical_recovery'])
            self.assertEqual(recovered['recovery_status'],
                             'historical_expired')
            self.assertEqual(recovered['received_at'], original_time)
            self.assertFalse(recovered['execution_authorized'])
            self.assertFalse(recovered['broker_effect_verified'])
            self.assertEqual(transport.calls, 1)
            replay = FormalReviewIngress(
                root_path, clock=fixture._clock).receive(
                    transport=None, packet=packet, request_id=request_id,
                    host_directory=bridge.directory, binding=bridge.binding)
            self.assertEqual(replay, recovered)
            self.assertEqual(transport.calls, 1)


class HostIngressBoundaryTests(TestCase):
    def _run_with_failure(self, fixture, bridge, request_id, packet,
                          *, fail_mode):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = __import__('pathlib').Path(temp.name) / 'ingress'
        transport = ScriptedReviewTransport()
        adapter = FormalReviewIngress(root, clock=fixture._clock)
        import martin_agentic.formal_review_ingress as module
        if fail_mode == 'claim':
            real_claim = adapter._claim
            def fail_after_claim(packet_value):
                real_claim(packet_value)
                raise OSError('injected after claim')
            adapter._claim = fail_after_claim
            with self.assertRaises(OSError):
                adapter.receive(
                    transport=transport, packet=packet,
                    request_id=request_id,
                    host_directory=bridge.directory,
                    binding=bridge.binding)
        elif fail_mode == 'before_journal':
            real_write = module.write_private_json
            def fail_journal(path, value):
                if str(path).endswith('.journal.json'):
                    raise OSError('injected before journal')
                return real_write(path, value)
            with patch.object(module, 'write_private_json',
                              side_effect=fail_journal):
                with self.assertRaises(OSError):
                    adapter.receive(
                        transport=transport, packet=packet,
                        request_id=request_id,
                        host_directory=bridge.directory,
                        binding=bridge.binding)
        elif fail_mode == 'after_journal':
            real_write = module.write_private_json
            def fail_after_journal(path, value):
                result = real_write(path, value)
                if str(path).endswith('.journal.json'):
                    raise OSError('injected after journal write')
                return result
            with patch.object(module, 'write_private_json',
                              side_effect=fail_after_journal):
                with self.assertRaises(OSError):
                    adapter.receive(
                        transport=transport, packet=packet,
                        request_id=request_id,
                        host_directory=bridge.directory,
                        binding=bridge.binding)
        elif fail_mode == 'receipt':
            real_submit = module.submit_receipt
            def fail_after_receipt(directory, rid, response, clock=None):
                real_submit(directory, rid, response, clock=clock)
                raise OSError('injected after receipt')
            with patch.object(module, 'submit_receipt',
                              side_effect=fail_after_receipt):
                with self.assertRaisesRegex(ValueError,
                                            'RECEIPT_PERSISTENCE_FAILED'):
                    adapter.receive(
                        transport=transport, packet=packet,
                        request_id=request_id,
                        host_directory=bridge.directory,
                        binding=bridge.binding)
        elif fail_mode == 'ingress':
            real_write = module.write_private_json
            def fail_ingress(path, value):
                if str(path).endswith(packet['operation_id'] + '.json'):
                    real_write(path, value)
                    raise OSError('injected after ingress')
                return real_write(path, value)
            with patch.object(module, 'write_private_json',
                              side_effect=fail_ingress):
                with self.assertRaises(OSError):
                    adapter.receive(
                        transport=transport, packet=packet,
                        request_id=request_id,
                        host_directory=bridge.directory,
                        binding=bridge.binding)
        return root, transport

    def test_claim_and_undurable_response_never_reexecutes(self):
        with host_review_fixture() as (fixture, bridge, request_id, packet):
            root, transport = self._run_with_failure(
                fixture, bridge, request_id, packet, fail_mode='claim')
            self.assertEqual(transport.calls, 0)
            retry = FormalReviewIngress(root, clock=fixture._clock)
            with self.assertRaisesRegex(FormalReviewIngressError,
                                        'ALREADY_CLAIMED'):
                retry.receive(
                    transport=transport, packet=packet,
                    request_id=request_id,
                    host_directory=bridge.directory,
                    binding=bridge.binding)
            self.assertEqual(transport.calls, 0)
        with host_review_fixture() as (fixture, bridge, request_id, packet):
            root, transport = self._run_with_failure(
                fixture, bridge, request_id, packet, fail_mode='before_journal')
            self.assertEqual(transport.calls, 1)
            retry = FormalReviewIngress(root, clock=fixture._clock)
            with self.assertRaisesRegex(FormalReviewIngressError,
                                        'ALREADY_CLAIMED'):
                retry.receive(
                    transport=transport, packet=packet,
                    request_id=request_id,
                    host_directory=bridge.directory,
                    binding=bridge.binding)
            self.assertEqual(transport.calls, 1)

    def test_journal_boundary_recovers_original_response(self):
        with host_review_fixture() as (fixture, bridge, request_id, packet):
            root, transport = self._run_with_failure(
                fixture, bridge, request_id, packet, fail_mode='after_journal')
            self.assertEqual(transport.calls, 1)
            receipt_path = (bridge.directory / 'receipts' /
                            (packet['operation_id'] + '.json'))
            self.assertFalse(receipt_path.exists())
            journal = json.loads((root / (
                packet['operation_id'] + '.journal.json')).read_text())
            original_time = journal['response_received_at']
            fixture._clock_value[0] = fixture._clock_value[0] + timedelta(seconds=1)
            recovered = FormalReviewIngress(
                root, clock=fixture._clock).receive(
                    transport=None, packet=packet, request_id=request_id,
                    host_directory=bridge.directory, binding=bridge.binding)
            self.assertEqual(recovered['received_at'], original_time)
            self.assertEqual(transport.calls, 1)
            self.assertEqual(recovered['packet_sha256'], digest(packet))
            self.assertEqual(recovered['response_sha256'],
                             journal['response_sha256'])

    def test_journal_invalid_fields_have_no_artifact_or_db_side_effects(self):
        with host_review_fixture() as (fixture, bridge, request_id, packet):
            temp = tempfile.TemporaryDirectory()
            self.addCleanup(temp.cleanup)
            root = __import__('pathlib').Path(temp.name) / 'ingress'
            transport = ScriptedReviewTransport()
            adapter = FormalReviewIngress(root, clock=fixture._clock)
            import martin_agentic.formal_review_ingress as module
            real_write = module.write_private_json
            def fail_after_journal(path, value):
                result = real_write(path, value)
                if str(path).endswith('.journal.json'):
                    raise OSError('injected after journal write')
                return result
            with patch.object(module, 'write_private_json',
                              side_effect=fail_after_journal):
                with self.assertRaises(OSError):
                    adapter.receive(
                        transport=transport, packet=packet,
                        request_id=request_id,
                        host_directory=bridge.directory,
                        binding=bridge.binding)
            self.assertEqual(transport.calls, 1)
            journal_path = root / (packet['operation_id'] + '.journal.json')
            ingress_path = root / (packet['operation_id'] + '.json')
            receipt_path = (bridge.directory / 'receipts' /
                            (packet['operation_id'] + '.json'))
            self.assertFalse(ingress_path.exists())
            self.assertFalse(receipt_path.exists())
            original = journal_path.read_bytes()
            value = json.loads(original)
            cases = [
                ('missing_response', {k: v for k, v in value.items()
                                      if k != 'response'}, 'JOURNAL_INVALID'),
                ('missing_call_time', {k: v for k, v in value.items()
                                       if k != 'call_started_at'},
                 'JOURNAL_TIME_INVALID'),
                ('wrong_action', {**value, 'action_id': 'wrong-action'},
                 'JOURNAL_INVALID'),
                ('wrong_account', {**value, 'account': '••••9999'},
                 'JOURNAL_INVALID'),
                ('wrong_request', {**value, 'request_id': 'wrong-request'},
                 'JOURNAL_INVALID'),
                ('wrong_ref', {**value, 'ref_id': 'wrong-ref'},
                 'JOURNAL_INVALID'),
                ('wrong_origin', {**value,
                                  'origin': 'formal_runtime_transport'},
                 'JOURNAL_CALL_INVALID'),
                ('wrong_tool', {**value, 'tool': 'place_equity_order'},
                 'JOURNAL_INVALID'),
                ('wrong_status', {**value, 'status': 'failed'},
                 'JOURNAL_INVALID'),
                ('execution_flag', {**value, 'execution_authorized': True},
                 'JOURNAL_INVALID'),
                ('broker_flag', {**value, 'broker_effect_verified': True},
                 'JOURNAL_INVALID'),
                ('native_flag',
                 {**value, 'native_confirmation_required': False},
                 'JOURNAL_INVALID'),
                ('packet_hash', {**value, 'packet_sha256': 'f' * 64},
                 'JOURNAL_INVALID'),
                ('parameters_hash', {**value, 'parameters_sha256': 'f' * 64},
                 'JOURNAL_INVALID'),
                ('response_hash', {**value, 'response_sha256': 'f' * 64},
                 'JOURNAL_INVALID'),
                ('response_identity',
                 {**value, 'response_identity_sha256': 'f' * 64},
                 'JOURNAL_INVALID'),
                ('response_raw', {**value, 'response': {'data': {}}},
                 'JOURNAL_INVALID'),
                ('expiry', {**value,
                            'expires_at': (NOW + timedelta(days=1)).isoformat()},
                 'JOURNAL_INVALID'),
                ('naive_received', {**value, 'response_received_at':
                                    value['response_received_at'].split('+')[0]},
                 'JOURNAL_TIME_INVALID'),
                ('reverse_call', {**value, 'call_started_at':
                                  (NOW + timedelta(seconds=2)).isoformat()},
                 'JOURNAL_TIME_ORDER'),
                ('future_received', {**value, 'response_received_at':
                                     (NOW + timedelta(days=1)).isoformat()},
                 'JOURNAL_TIME_ORDER'),
            ]
            for label, marked, expected in cases:
                with self.subTest(case=label):
                    journal_path.write_text(json.dumps(marked, sort_keys=True))
                    journal_path.chmod(0o600)
                    before_files = _artifacts_snapshot(bridge, root)
                    before_db = _db_snapshot(fixture.book)
                    with self.assertRaisesRegex(FormalReviewIngressError,
                                                expected):
                        FormalReviewIngress(
                            root, clock=fixture._clock).receive(
                                transport=None, packet=packet,
                                request_id=request_id,
                                host_directory=bridge.directory,
                                binding=bridge.binding)
                    self.assertEqual(
                        _artifacts_snapshot(bridge, root), before_files)
                    self.assertEqual(_db_snapshot(fixture.book), before_db)
                    self.assertEqual(transport.calls, 1)
                    self.assertFalse(ingress_path.exists())
                    self.assertFalse(receipt_path.exists())
                    _restore(journal_path, original)
            recovered = FormalReviewIngress(
                root, clock=fixture._clock).receive(
                    transport=None, packet=packet, request_id=request_id,
                    host_directory=bridge.directory, binding=bridge.binding)
            self.assertEqual(recovered['status'], 'received')
            self.assertEqual(recovered['received_at'],
                             value['response_received_at'])
            self.assertEqual(recovered['packet_sha256'], digest(packet))
            self.assertEqual(recovered['response_sha256'],
                             value['response_sha256'])
            self.assertFalse(recovered['historical_recovery'])
            self.assertEqual(transport.calls, 1)
            self.assertTrue(ingress_path.exists())
            self.assertTrue(receipt_path.exists())

    def test_receipt_and_ingress_boundaries_recover(self):
        for mode in ('receipt', 'ingress'):
            with self.subTest(mode=mode):
                with host_review_fixture() as (fixture, bridge, request_id,
                                               packet):
                    root, transport = self._run_with_failure(
                        fixture, bridge, request_id, packet, fail_mode=mode)
                    receipt_path = (bridge.directory / 'receipts' /
                                    (packet['operation_id'] + '.json'))
                    before = receipt_path.read_bytes()
                    recovered = FormalReviewIngress(
                        root, clock=fixture._clock).receive(
                            transport=None, packet=packet,
                            request_id=request_id,
                            host_directory=bridge.directory,
                            binding=bridge.binding)
                    self.assertEqual(recovered['status'], 'received')
                    self.assertEqual(transport.calls, 1)
                    self.assertEqual(receipt_path.read_bytes(), before)
                    files = list(root.glob('*.json'))
                    self.assertEqual(len([p for p in files
                                          if p.name.endswith('.journal.json')]),
                                     1)


def _tree_snapshot(root):
    return {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(root.rglob('*')) if path.is_file()}


def _db_snapshot(book):
    return hashlib.sha256('\n'.join(book.db.iterdump()).encode()).hexdigest()


def _artifacts_snapshot(bridge, ingress_root):
    return {
        'host': _tree_snapshot(bridge.directory),
        'ingress': _tree_snapshot(ingress_root),
    }


def _restore(path, raw):
    path.write_bytes(raw)


class OwnedReviewEntryTests(TestCase):
    def _factory(self, base=ScriptedEntryTransport):
        class Factory(base):
            constructed = 0
        return Factory

    def test_default_off_and_review_only_are_required_before_any_transport(self):
        with host_review_fixture() as (fixture, bridge, request_id, packet):
            factory = self._factory()
            with self.assertRaisesRegex(ValueError, 'OWNED_REVIEW_DISABLED'):
                bridge.run_prepared_review(
                    request_id, transport_factory=factory)
            self.assertEqual(factory.constructed, 0)
            with self.assertRaisesRegex(ValueError, 'OWNED_REVIEW_ONLY_REQUIRED'):
                bridge.run_prepared_review(
                    request_id, enabled=True, transport_factory=factory)
            self.assertEqual(factory.constructed, 0)
            with self.assertRaisesRegex(ValueError,
                                        'OWNED_REVIEW_DEADLINE_INVALID'):
                bridge.run_prepared_review(
                    request_id, enabled=True, review_only=True,
                    transport_factory=factory, deadline_seconds=0)
            self.assertEqual(factory.constructed, 0)
            self.assertIsNotNone(bridge.pending)

    def test_host_packet_to_ingress_receipt_import_and_native_reader(self):
        with host_review_fixture() as (fixture, bridge, request_id, packet):
            factory = self._factory()
            pending = bridge.pending
            self.assertIsNotNone(pending)
            result = bridge.run_prepared_review(
                request_id, enabled=True, review_only=True,
                transport_factory=factory)
            self.assertEqual(result['status'], 'WAITING_USER', result)
            self.assertTrue(result['review_verified'])
            self.assertEqual(result['formal_origin'], 'test_scripted_transport')
            self.assertFalse(result['execution_authorized'])
            self.assertFalse(result['broker_effect_verified'])
            self.assertEqual(result['broker_writes'], 0)
            self.assertEqual(factory.constructed, 1)
            self.assertIsNone(bridge.pending)
            receipt_path = (bridge.directory / 'receipts' /
                            (packet['operation_id'] + '.json'))
            ingress_path = (bridge.directory / 'review-ingress' /
                            (packet['operation_id'] + '.json'))
            self.assertTrue(receipt_path.exists())
            self.assertTrue(ingress_path.exists())
            ingress = json.loads(ingress_path.read_text())
            self.assertEqual(ingress['origin'], 'test_scripted_transport')
            self.assertFalse(ingress['execution_authorized'])
            self.assertFalse(ingress['broker_effect_verified'])
            binding = pending['binding']
            permit = {
                'action_id': packet['action_id'],
                'account_key': binding['account_key'],
                'account_suffix': '0000',
                'source': {
                    'policy_sha256': binding['policy_sha256'],
                    'source_fingerprint': binding['source_fingerprint']},
                'formal': {
                    'review_operation_id': packet['operation_id'],
                    'review_parameters_sha256': digest(
                        packet['parameters'])}}
            reader = NativeReceiptEvidenceReader(
                bridge.directory, clock=fixture._clock)
            evidence = reader.read(fixture.book, {
                'permit': permit,
                'review': {'operation_id': packet['operation_id']}})
            self.assertEqual(evidence['origin'], 'host_submitted_response')
            self.assertEqual(evidence['native_status'], 'needs_confirmation')
            self.assertFalse(evidence['broker_effect_verified'])

    def test_stale_request_source_and_account_conflicts_do_not_call_transport(self):
        for case in ('stale', 'request', 'source', 'account'):
            with self.subTest(case=case):
                with host_review_fixture() as (fixture, bridge, request_id,
                                               packet):
                    factory = self._factory()
                    if case == 'stale':
                        fixture._clock_value[0] = NOW + timedelta(seconds=30)
                    elif case == 'request':
                        path = (bridge.directory / 'requests' /
                                (request_id + '.json'))
                        value = json.loads(path.read_text())
                        value['action_id'] = 'wrong-action'
                        path.write_text(json.dumps(value, sort_keys=True))
                        path.chmod(0o600)
                    elif case == 'source':
                        bridge.binding['source_fingerprint'] = 'f' * 64
                        bridge.pending['binding']['source_fingerprint'] = 'f' * 64
                        path = (bridge.directory / 'requests' /
                                (request_id + '.json'))
                        value = json.loads(path.read_text())
                        value['binding']['source_fingerprint'] = 'f' * 64
                        path.write_text(json.dumps(value, sort_keys=True))
                        path.chmod(0o600)
                    else:
                        bridge.binding['account_key'] = 'f' * 64
                        bridge.pending['binding']['account_key'] = 'f' * 64
                        path = (bridge.directory / 'requests' /
                                (request_id + '.json'))
                        value = json.loads(path.read_text())
                        value['binding']['account_key'] = 'f' * 64
                        path.write_text(json.dumps(value, sort_keys=True))
                        path.chmod(0o600)
                    with self.assertRaisesRegex(
                            ValueError, 'OWNED_REVIEW_' + {
                                'stale': 'EXPIRED',
                                'request': 'REQUEST_INVALID',
                                'source': 'SOURCE_OR_ACCOUNT_MISMATCH',
                                'account': 'SOURCE_OR_ACCOUNT_MISMATCH'}[case]):
                        bridge.run_prepared_review(
                            request_id, enabled=True, review_only=True,
                            transport_factory=factory)
                    self.assertEqual(factory.constructed, 0)
                    self.assertEqual(
                        bridge.pending['packet']['operation_id']
                        if bridge.pending else None,
                        None if case == 'stale' else packet['operation_id'])

    def test_tool_timeout_and_error_failures_clean_pending(self):
        for factory, code in ((FailingEntryTransport, 'READ_TOOL_UNAVAILABLE'),
                              (TimeoutEntryTransport, 'APP_SERVER_TIMEOUT')):
            with self.subTest(code=code):
                with host_review_fixture() as (fixture, bridge, request_id,
                                               packet):
                    before = factory.constructed
                    result = bridge.run_prepared_review(
                        request_id, enabled=True, review_only=True,
                        transport_factory=factory)
                    self.assertEqual(result['status'], 'OWNED_REVIEW_FAILED')
                    self.assertEqual(result['failure'].split(':')[-1], code)
                    self.assertIsNone(bridge.pending)
                    self.assertEqual(factory.constructed, before + 1)
                    self.assertTrue(factory.instances[-1].rpc.closed)
                    self.assertFalse((bridge.directory / 'receipts' /
                                      (packet['operation_id'] + '.json')).exists())
                    lease = __import__(
                        'martin_agentic.control_tasks', fromlist=['worker_lease']
                    ).worker_lease(fixture.book.db.execute(
                        'PRAGMA database_list').fetchone()[2])
                    self.assertIsNotNone(lease)
                    lease.close()

    def test_iserror_and_post_return_expiry_are_named(self):
        with host_review_fixture() as (fixture, bridge, request_id, packet):
            factory = self._factory(IserrorEntryTransport)
            result = bridge.run_prepared_review(
                request_id, enabled=True, review_only=True,
                transport_factory=factory)
            self.assertEqual(result['status'], 'OWNED_REVIEW_FAILED')
            self.assertIn('RESPONSE_ERROR', result['failure'])
            self.assertIsNone(bridge.pending)
            self.assertEqual(factory.constructed, 1)
        with host_review_fixture() as (fixture, bridge, request_id, packet):
            factory = self._factory(ExpiringEntryTransport)
            factory.clock_fn = lambda value: fixture._clock_value.__setitem__(
                0, value)
            result = bridge.run_prepared_review(
                request_id, enabled=True, review_only=True,
                transport_factory=factory)
            self.assertEqual(
                result['status'], 'OWNED_REVIEW_IMPORT_REQUIRES_RECOVERY')
            self.assertIsNone(bridge.pending)
            self.assertEqual(factory.constructed, 1)

    def test_import_consumes_total_budget_and_preserves_original_record(self):
        with host_review_fixture() as (fixture, bridge, request_id, packet):
            factory = self._factory()
            mono = [100.0]
            real_import = bridge._import
            def slow_import(*args, **kwargs):
                result = real_import(*args, **kwargs)
                mono[0] = 120.0
                return result
            bridge._import = slow_import
            with patch('martin_agentic.host_execution.time.monotonic',
                       side_effect=lambda: mono[0]):
                result = bridge.run_prepared_review(
                    request_id, enabled=True, review_only=True,
                    transport_factory=factory)
            self.assertEqual(result['status'],
                             'OWNED_REVIEW_DEADLINE_EXCEEDED')
            self.assertTrue(result['deadline_exhausted'])
            self.assertEqual(result['host_import']['status'],
                             'HOST_RECEIPT_IMPORTED')
            self.assertIsNone(bridge.pending)
            self.assertEqual(factory.constructed, 1)
            receipt = json.loads((bridge.directory / 'receipts' /
                (packet['operation_id'] + '.json')).read_text())
            self.assertTrue(result['native_result']['review_verified'])
            self.assertEqual(result['host_import']['packet_sha256'],
                             digest(packet))

    def test_incomplete_or_raising_cleanup_is_not_green(self):
        for factory in (IncompleteCleanupEntryTransport,
                        RaisingCleanupEntryTransport):
            with self.subTest(factory=factory.__name__):
                with host_review_fixture() as (fixture, bridge, request_id,
                                               packet):
                    result = bridge.run_prepared_review(
                        request_id, enabled=True, review_only=True,
                        transport_factory=factory)
                    self.assertEqual(result['status'],
                                     'OWNED_REVIEW_CLEANUP_INCOMPLETE')
                    self.assertFalse(result['stages']['cleanup'])
                    self.assertIsNotNone(result['cleanup']['rpc_cleanup'])
                    self.assertFalse(result['cleanup']['rpc_cleanup'].get(
                        'complete', False))
                    self.assertIsNone(bridge.pending)
                    self.assertTrue((bridge.directory / 'receipts' /
                                     (packet['operation_id'] + '.json')).exists())

    def test_expiry_after_connect_is_named_and_cleans_pending(self):
        with host_review_fixture() as (fixture, bridge, request_id, packet):
            factory = self._factory(ExpireOnConnectEntryTransport)
            factory.clock_fn = lambda value: fixture._clock_value.__setitem__(
                0, value)
            earlier = json.loads((bridge.directory / 'packets' /
                                  (request_id + '.json')).read_text())
            result = bridge.run_prepared_review(
                request_id, enabled=True, review_only=True,
                transport_factory=factory)
            self.assertEqual(result['status'], 'OWNED_REVIEW_EXPIRED')
            self.assertEqual(factory.constructed, 1)
            self.assertIsNone(bridge.pending)
            self.assertFalse((bridge.directory / 'receipts' /
                              (packet['operation_id'] + '.json')).exists())
            self.assertEqual(json.loads((bridge.directory / 'packets' /
                (request_id + '.json')).read_text()), earlier)

    def test_durable_120_second_recovery_does_not_reconnect(self):
        with host_review_fixture() as (fixture, bridge, request_id, packet):
            factory = self._factory()
            real_import = bridge._import
            bridge._import = lambda *a, **kw: {
                'status': 'HOST_RECEIPT_IMPORT_REQUIRES_RECOVERY'}
            first = bridge.run_prepared_review(
                request_id, enabled=True, review_only=True,
                transport_factory=factory)
            self.assertEqual(first['status'],
                             'OWNED_REVIEW_IMPORT_REQUIRES_RECOVERY')
            self.assertIsNotNone(bridge.pending)
            bridge._import = real_import
            receipt_before = (bridge.directory / 'receipts' /
                              (packet['operation_id'] + '.json')).read_bytes()
            ingress_before = (bridge.directory / 'review-ingress' /
                              (packet['operation_id'] + '.json')).read_bytes()
            fixture._clock_value[0] = NOW + timedelta(seconds=120)
            result = bridge.run_prepared_review(
                request_id, enabled=True, review_only=True,
                transport_factory=factory)
            self.assertEqual(result['status'],
                             'OWNED_REVIEW_HISTORICAL_EXPIRED')
            self.assertTrue(result['historical'])
            self.assertTrue(result['expired'])
            self.assertIsNone(bridge.pending)
            self.assertEqual(factory.constructed, 1)
            self.assertEqual((bridge.directory / 'receipts' /
                (packet['operation_id'] + '.json')).read_bytes(),
                receipt_before)
            self.assertEqual((bridge.directory / 'review-ingress' /
                (packet['operation_id'] + '.json')).read_bytes(),
                ingress_before)

    def test_receipt_interruption_recovers_local_without_second_request(self):
        import martin_agentic.formal_review_ingress as module
        with host_review_fixture() as (fixture, bridge, request_id, packet):
            factory = self._factory()
            real_submit = module.submit_receipt
            calls = {'count': 0}
            def fail_first(directory, rid, response, clock=None):
                calls['count'] += 1
                if calls['count'] == 1:
                    raise OSError('injected receipt failure')
                return real_submit(directory, rid, response, clock=clock)
            with patch.object(module, 'submit_receipt', side_effect=fail_first):
                result = bridge.run_prepared_review(
                    request_id, enabled=True, review_only=True,
                    transport_factory=factory)
            self.assertIn(result['status'],
                          {'OWNED_REVIEW_RECOVERED_LOCAL', 'WAITING_USER'})
            self.assertIsNone(bridge.pending)
            self.assertEqual(factory.constructed, 1)
            self.assertTrue((bridge.directory / 'receipts' /
                             (packet['operation_id'] + '.json')).exists())

    def test_import_interruption_retains_durable_receipt_for_recovery(self):
        with host_review_fixture() as (fixture, bridge, request_id, packet):
            factory = self._factory()
            real_import = bridge._import
            def fail_import(pending=None, *, detached=False):
                return {'status': 'HOST_RECEIPT_IMPORT_REQUIRES_RECOVERY',
                        'operation_id': packet['operation_id'],
                        'attention_required': True,
                        'execution_authorized': False, 'model_calls': 0}
            bridge._import = fail_import
            result = bridge.run_prepared_review(
                request_id, enabled=True, review_only=True,
                transport_factory=factory)
            self.assertEqual(result['status'],
                             'OWNED_REVIEW_IMPORT_REQUIRES_RECOVERY')
            self.assertIsNotNone(bridge.pending)
            self.assertEqual(factory.constructed, 1)
            self.assertTrue((bridge.directory / 'review-ingress' /
                             (packet['operation_id'] + '.json')).exists())
            bridge._import = real_import
            recovered = bridge.run_prepared_review(
                request_id, enabled=True, review_only=True,
                transport_factory=factory)
            self.assertIn(recovered['status'],
                          {'OWNED_REVIEW_RECOVERED_LOCAL', 'WAITING_USER'})
            self.assertEqual(factory.constructed, 1)

class CallFileSchemaTests(TestCase):
    """Pure-data formal call-file branch; local fixture, not provenance."""

    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = __import__('pathlib').Path(temp.name) / 'ingress'
        self.adapter = FormalReviewIngress(self.root, clock=lambda: NOW)
        self.binding = {'account_key': 'e' * 64}
        self.ingress = {
            'call_id': '4', 'descriptor_sha256': 'a' * 64,
            'parameters_sha256': 'b' * 64,
            'response_sha256': 'c' * 64,
            'response_identity_sha256': 'd' * 64,
            'call_started_at': NOW.isoformat(),
            'call_ended_at': (NOW + timedelta(seconds=1)).isoformat()}
        self.call = {
            'schema_version': 1, 'status': 'received', 'tool': TOOL,
            'server': 'robinhood-trading', 'thread_id': 'thread-1',
            'connection_id': 'connection-1', 'rpc_request_id': '4',
            'arguments_sha256': '1' * 64,
            'parameters_sha256': 'b' * 64,
            'account_scope_sha256': 'e' * 64,
            'descriptor_sha256': 'a' * 64,
            'started_at': NOW.isoformat(),
            'ended_at': (NOW + timedelta(seconds=1)).isoformat(),
            'response_sha256': 'c' * 64,
            'response_identity_sha256': 'd' * 64}

    def test_call_record_schema_and_cross_binding_mismatch(self):
        self.adapter._validate_call_record(
            self.call, self.ingress, self.binding)
        for field, value, expected in (
                ('schema_version', 2, 'CALL_RECORD_INVALID'),
                ('status', 'failed', 'CALL_RECORD_INVALID'),
                ('server', '', 'CALL_RECORD_INVALID'),
                ('thread_id', None, 'CALL_RECORD_INVALID'),
                ('connection_id', '', 'CALL_RECORD_INVALID'),
                ('descriptor_sha256', 'f' * 64, 'CALL_RECORD_INVALID'),
                ('rpc_request_id', '5', 'CALL_RECORD_INVALID'),
                ('account_scope_sha256', 'f' * 64, 'CALL_RECORD_INVALID'),
                ('parameters_sha256', 'f' * 64, 'CALL_RECORD_INVALID'),
                ('response_sha256', 'f' * 64, 'CALL_RECORD_INVALID'),
                ('response_identity_sha256', 'f' * 64,
                 'CALL_RECORD_INVALID')):
            with self.subTest(field=field):
                with self.assertRaisesRegex(
                        FormalReviewIngressError, expected):
                    self.adapter._validate_call_record(
                        {**self.call, field: value}, self.ingress,
                        self.binding)
        naive = NOW.replace(tzinfo=None).isoformat()
        with self.assertRaisesRegex(FormalReviewIngressError,
                                    'CALL_TIME_INVALID'):
            self.adapter._validate_call_record(
                {**self.call, 'started_at': naive},
                {**self.ingress, 'call_started_at': naive}, self.binding)
        reversed_times = {**self.call,
                          'started_at': (NOW + timedelta(seconds=2)).isoformat(),
                          'ended_at': NOW.isoformat()}
        with self.assertRaisesRegex(FormalReviewIngressError,
                                    'CALL_TIME_INVALID'):
            self.adapter._validate_call_record(
                reversed_times, {**self.ingress,
                                 'call_started_at': reversed_times['started_at'],
                                 'call_ended_at': reversed_times['ended_at']},
                self.binding)

    def test_call_file_missing_conflict_and_valid_local_fixture(self):
        operation_id = 'native-00000000-0000-4000-8000-000000000002'
        ingress = {**self.ingress, 'call_record_sha256': digest(self.call)}
        with self.assertRaisesRegex(FormalReviewIngressError,
                                    'CACHE_CALL_MISSING'):
            self.adapter._load_call_record(
                operation_id, ingress, self.binding)
        path = self.root / (operation_id + '.call.json')
        path.write_text(json.dumps({**self.call, 'rpc_request_id': '9'}))
        with self.assertRaisesRegex(FormalReviewIngressError,
                                    'CACHE_CALL_CONFLICT'):
            self.adapter._load_call_record(
                operation_id, ingress, self.binding)
        path.write_text(json.dumps(self.call, sort_keys=True))
        self.adapter._load_call_record(operation_id, ingress, self.binding)

    def test_call_file_write_interruption_retry_keeps_original(self):
        import martin_agentic.formal_review_ingress as module
        operation_id = 'native-00000000-0000-4000-8000-000000000003'
        ingress = {**self.ingress, 'call_record_sha256': digest(self.call)}
        path = self.root / (operation_id + '.call.json')
        with patch.object(module, 'write_private_json',
                          side_effect=ValueError('HOST_FILE_CONFLICT')):
            with self.assertRaisesRegex(FormalReviewIngressError,
                                        'CALL_RECORD_CONFLICT'):
                self.adapter._persist_call_record(operation_id, self.call)
        self.assertFalse(path.exists())
        first = self.adapter._persist_call_record(operation_id, self.call)
        self.assertEqual(first, digest(self.call))
        real_write = module.write_private_json
        def write_then_interrupt(target, value):
            real_write(target, value)
            raise ValueError('HOST_FILE_CONFLICT')
        with patch.object(module, 'write_private_json',
                          side_effect=write_then_interrupt):
            with self.assertRaisesRegex(FormalReviewIngressError,
                                        'CALL_RECORD_CONFLICT'):
                self.adapter._persist_call_record(operation_id, self.call)
        self.assertTrue(path.exists())
        retried = self.adapter._persist_call_record(operation_id, self.call)
        self.assertEqual(retried, first)
        self.adapter._load_call_record(operation_id, ingress, self.binding)


class CacheInvariantTests(TestCase):
    def test_cache_invalid_fields_have_no_artifact_or_db_side_effects(self):
        with host_review_fixture() as (fixture, bridge, request_id, packet):
            temp = tempfile.TemporaryDirectory()
            self.addCleanup(temp.cleanup)
            root = __import__('pathlib').Path(temp.name) / 'ingress'
            transport = ScriptedReviewTransport()
            adapter = FormalReviewIngress(root, clock=fixture._clock)
            good = adapter.receive(
                transport=transport, packet=packet, request_id=request_id,
                host_directory=bridge.directory, binding=bridge.binding)
            ingress_path = root / (packet['operation_id'] + '.json')
            journal_path = root / (packet['operation_id'] + '.journal.json')
            receipt_path = (bridge.directory / 'receipts' /
                            (packet['operation_id'] + '.json'))
            baseline = {ingress_path: ingress_path.read_bytes(),
                        journal_path: journal_path.read_bytes(),
                        receipt_path: receipt_path.read_bytes()}
            ingress_value = json.loads(baseline[ingress_path])
            journal_value = json.loads(baseline[journal_path])
            receipt_value = json.loads(baseline[receipt_path])

            def changed_ingress(**updates):
                value = {**ingress_value, **updates}
                return value

            def changed_journal(**updates):
                return {**journal_value, **updates}

            def changed_receipt(**updates):
                return {**receipt_value, **updates}

            cases = [
                ('missing_host_receipt_sha', ingress_path,
                 {k: v for k, v in ingress_value.items()
                  if k != 'host_receipt_sha256'}, 'CACHE_RECEIPT_MISMATCH'),
                ('missing_received_at', ingress_path,
                 {k: v for k, v in ingress_value.items()
                  if k != 'received_at'}, 'CACHE_TIME_INVALID'),
                ('missing_response_hash', ingress_path,
                 {k: v for k, v in ingress_value.items()
                  if k != 'response_sha256'}, 'CACHE_RECEIPT_MISMATCH'),
                ('missing_call_started', ingress_path,
                 {k: v for k, v in ingress_value.items()
                  if k != 'call_started_at'}, 'CACHE_TIME_INVALID'),
                ('wrong_operation', ingress_path,
                 changed_ingress(operation_id='wrong-operation'), 'CACHE_INVALID'),
                ('wrong_request', ingress_path,
                 changed_ingress(request_id='wrong-request'), 'CACHE_INVALID'),
                ('wrong_action', ingress_path,
                 changed_ingress(action_id='wrong-action'), 'CACHE_INVALID'),
                ('wrong_account', ingress_path,
                 changed_ingress(account='••••9999'), 'CACHE_INVALID'),
                ('wrong_account_key', ingress_path,
                 changed_ingress(account_key='f' * 64), 'CACHE_INVALID'),
                ('wrong_ref', ingress_path,
                 changed_ingress(ref_id='wrong-ref'), 'CACHE_INVALID'),
                ('wrong_tool', ingress_path,
                 changed_ingress(tool='place_equity_order'), 'CACHE_INVALID'),
                ('execution_flag', ingress_path,
                 changed_ingress(execution_authorized=True), 'CACHE_INVALID'),
                ('broker_flag', ingress_path,
                 changed_ingress(broker_effect_verified=True), 'CACHE_INVALID'),
                ('native_flag', ingress_path,
                 changed_ingress(native_confirmation_required=False),
                 'CACHE_INVALID'),
                ('packet_hash', ingress_path,
                 changed_ingress(packet_sha256='f' * 64), 'CACHE_INVALID'),
                ('parameters_hash', ingress_path,
                 changed_ingress(parameters_sha256='f' * 64), 'CACHE_INVALID'),
                ('response_hash', ingress_path,
                 changed_ingress(response_sha256='f' * 64),
                 'CACHE_RECEIPT_MISMATCH'),
                ('response_identity', ingress_path,
                 changed_ingress(response_identity_sha256='f' * 64),
                 'CACHE_RECEIPT_MISMATCH'),
                ('call_id', ingress_path,
                 changed_ingress(call_id='wrong-call'),
                 'CACHE_JOURNAL_MISMATCH'),
                ('naive_received', ingress_path,
                 changed_ingress(received_at=ingress_value['received_at']
                                 .split('+')[0]), 'CACHE_TIME_INVALID'),
                ('future_received', ingress_path,
                 changed_ingress(received_at=(NOW + timedelta(days=1))
                                 .isoformat()), 'CACHE_TIME_ORDER'),
                ('reverse_call', ingress_path,
                 changed_ingress(call_started_at=(NOW + timedelta(seconds=2))
                                 .isoformat()), 'CACHE_TIME_ORDER'),
                ('receipt_hash', receipt_path,
                 changed_receipt(received_at=(NOW + timedelta(seconds=3))
                                 .isoformat()), 'CACHE_RECEIPT_MISMATCH'),
                ('receipt_response', receipt_path,
                 changed_receipt(response_sha256='f' * 64),
                 'CACHE_RECEIPT_MISMATCH'),
                ('receipt_identity', receipt_path,
                 changed_receipt(response={'data': {'tampered': True}}),
                 'CACHE_RECEIPT_MISMATCH'),
                ('journal_action', journal_path,
                 changed_journal(action_id='wrong-action'), 'JOURNAL_INVALID'),
                ('journal_account', journal_path,
                 changed_journal(account_key='f' * 64), 'JOURNAL_INVALID'),
                ('journal_request', journal_path,
                 changed_journal(request_id='wrong-request'),
                 'JOURNAL_INVALID'),
                ('journal_ref', journal_path,
                 changed_journal(ref_id='wrong-ref'), 'JOURNAL_INVALID'),
                ('journal_origin', journal_path,
                 changed_journal(origin='formal_runtime_transport'),
                 'JOURNAL_CALL_INVALID'),
                ('journal_tool', journal_path,
                 changed_journal(tool='place_equity_order'), 'JOURNAL_INVALID'),
                ('journal_status', journal_path,
                 changed_journal(status='failed'), 'JOURNAL_INVALID'),
                ('journal_execution', journal_path,
                 changed_journal(execution_authorized=True),
                 'JOURNAL_INVALID'),
                ('journal_broker', journal_path,
                 changed_journal(broker_effect_verified=True),
                 'JOURNAL_INVALID'),
                ('journal_native', journal_path,
                 changed_journal(native_confirmation_required=False),
                 'JOURNAL_INVALID'),
                ('journal_packet_hash', journal_path,
                 changed_journal(packet_sha256='f' * 64), 'JOURNAL_INVALID'),
                ('journal_parameters_hash', journal_path,
                 changed_journal(parameters_sha256='f' * 64),
                 'JOURNAL_INVALID'),
                ('journal_response_hash', journal_path,
                 changed_journal(response_sha256='f' * 64),
                 'JOURNAL_INVALID'),
                ('journal_response_identity', journal_path,
                 changed_journal(response_identity_sha256='f' * 64),
                 'JOURNAL_INVALID'),
                ('journal_expiry', journal_path,
                 changed_journal(expires_at=(NOW + timedelta(days=1))
                                 .isoformat()), 'JOURNAL_INVALID'),
                ('journal_naive_received', journal_path,
                 changed_journal(response_received_at=journal_value[
                     'response_received_at'].split('+')[0]),
                 'JOURNAL_TIME_INVALID'),
                ('journal_future_received', journal_path,
                 changed_journal(response_received_at=(NOW + timedelta(days=1))
                                 .isoformat()), 'JOURNAL_TIME_ORDER'),
                ('journal_reverse_call', journal_path,
                 changed_journal(call_started_at=(NOW + timedelta(seconds=2))
                                 .isoformat()), 'JOURNAL_TIME_ORDER'),
                ('journal_missing_call_time', journal_path,
                 {k: v for k, v in journal_value.items()
                  if k != 'call_started_at'}, 'JOURNAL_TIME_INVALID'),
            ]
            for label, path, value, expected in cases:
                with self.subTest(case=label):
                    original = baseline[path]
                    path.write_text(json.dumps(value, sort_keys=True))
                    path.chmod(0o600)
                    before_files = _artifacts_snapshot(bridge, root)
                    before_db = _db_snapshot(fixture.book)
                    with self.assertRaisesRegex(FormalReviewIngressError,
                                                expected):
                        FormalReviewIngress(
                            root, clock=fixture._clock).receive(
                                transport=None, packet=packet,
                                request_id=request_id,
                                host_directory=bridge.directory,
                                binding=bridge.binding)
                    self.assertEqual(
                        _artifacts_snapshot(bridge, root), before_files)
                    self.assertEqual(_db_snapshot(fixture.book), before_db)
                    self.assertEqual(transport.calls, 1)
                    _restore(path, original)
            positive = FormalReviewIngress(
                root, clock=fixture._clock).receive(
                    transport=None, packet=packet, request_id=request_id,
                    host_directory=bridge.directory, binding=bridge.binding)
            self.assertEqual(positive, good)
            self.assertEqual(transport.calls, 1)
