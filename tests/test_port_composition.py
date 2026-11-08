"""The real control loop reads and dispatches through injected ports."""
import json
import tempfile
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from martin_luk.live_session import run_live_session
from test_approval_unknown_lifecycle import ApprovalFlow, NoClaimDispatcher
from test_runner_result_merge import NOW, snapshot


class SpyBrokerPort:
    def __init__(self):
        self.calls = 0

    def positions(self):
        return {}

    def orders(self):
        return 0

    def snapshot(self, *args, **kwargs):
        self.calls += 1
        return snapshot(base=NOW)


class SpyMarketPort:
    def __init__(self):
        self.calls = []

    def collect_events(self, signals, *, symbols, requirements, clock,
                       capture_context, cancellation):
        self.calls.append({'symbols': list(symbols),
                           'requirements': tuple(requirements or ())})
        return [], {'quotes': 0, 'raw_capture_directory': '/tmp/spy',
                    'source_sha256': 'spy', 'at': NOW.isoformat()}


class SpyExecutionPort:
    def __init__(self):
        self.advances = []

    def advance_queue(self, *, allow_entries=False):
        self.advances.append(bool(allow_entries))
        return {'agent_called': False, 'reason': 'SPY_NO_ACTION'}


class SpyHostPort:
    def __init__(self):
        self.delivered = []

    def deliver(self, intent):
        self.delivered.append(dict(intent))
        return {'accepted': True, 'intent_id': intent['intent_id']}

    def receipt(self, intent_id):
        return None


class LoopPortTests(TestCase):
    def test_the_session_reads_and_dispatches_only_through_ports(self):
        with tempfile.TemporaryDirectory() as directory:
            policy = json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
            policy['capture_only'] = True
            broker, execution = SpyBrokerPort(), SpyExecutionPort()
            market = SpyMarketPort()
            bundle = type('Ports', (), {'market': market, 'broker': broker,
                                        'execution': execution, 'host': None})()
            with patch('martin_agentic.live_session.single_instance'), \
                 patch('martin_agentic.live_session._broker_read',
                       side_effect=AssertionError('provider read must not be used')), \
                 patch('martin_agentic.live_session.collect_live_events',
                       return_value=([], {'quotes': 0})), \
                 patch('martin_agentic.live_session.notify_local'), \
                 patch('martin_agentic.live_session.print'):
                result = run_live_session(
                    Path(directory) / 'book.sqlite', policy,
                    symbols=[str(index) for index in range(10)], cycles=2,
                    clock=lambda: NOW, status_file=Path(directory) / 'status.json',
                    output_dir=directory, budget_entry_limit=0,
                    budget_total_limit=0, ports=bundle)
            self.assertEqual(result['cycle'], 2)
            self.assertGreaterEqual(broker.calls, 2)
            self.assertGreaterEqual(len(execution.advances), 2)
            self.assertTrue(market.calls)
            self.assertIn('requirements', market.calls[0])


class HostDeliveryTests(ApprovalFlow):
    def test_a_qualified_entry_intent_is_delivered_by_identity(self):
        self.supervisor.dispatcher = NoClaimDispatcher()
        host = SpyHostPort()
        self.supervisor.host_port = host
        entry = self.make_entry()
        self.assertEqual(len(host.delivered), 1)
        delivery = host.delivered[0]
        self.assertEqual(delivery['trade_id'], entry['trade_id'])
        self.assertEqual(delivery['action_id'], entry['id'])
        self.assertTrue(delivery['intent_id'].startswith('intent-'))
        trade = self.book.state()['trades'][entry['trade_id']]
        self.assertTrue(trade['evidence']['qualified_strategy_entry'],
                        'an entry dispatched while entries are allowed is qualified')
        # The delivery identity matches the durable intent exactly.
        row = self.db.execute('SELECT intent_id FROM execution_intents'
                              ' WHERE trade_id=?', (entry['trade_id'],)).fetchone()
        self.assertEqual(delivery['intent_id'], row[0])


class CallSiteArchitectureTests(TestCase):
    def test_run_live_session_has_no_direct_provider_read_or_collection(self):
        import ast
        source = Path('infra/src/martin_agentic/live_session.py').read_text()
        tree = ast.parse(source)
        function = next(node for node in tree.body
                        if isinstance(node, ast.FunctionDef)
                        and node.name == 'run_live_session')
        names = {node.id for node in ast.walk(function)
                 if isinstance(node, ast.Name)}
        self.assertNotIn('_broker_read', names)
        self.assertNotIn('collect_live_events', names)
        # The composition factory is the only provider-aware call site.
        factory = next(node for node in tree.body
                       if isinstance(node, ast.FunctionDef)
                       and node.name == 'default_live_ports')
        factory_names = {node.id for node in ast.walk(factory)
                         if isinstance(node, ast.Name)}
        self.assertIn('_broker_read', factory_names)
        self.assertIn('collect_live_events', factory_names)
        attributes = {node.attr for node in ast.walk(function)
                      if isinstance(node, ast.Attribute)}
        self.assertIn('data_requirements', attributes,
                      'requirements must come from manifests, not a constant')

    def test_missing_ports_fail_closed_instead_of_falling_back(self):
        with tempfile.TemporaryDirectory() as directory:
            policy = json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
            policy['capture_only'] = True
            no_broker = type('Ports', (), {'market': object(), 'broker': None,
                                          'execution': None, 'host': None})()
            with self.assertRaisesRegex(ValueError, 'BROKER_READ_PORT_REQUIRED'):
                run_live_session(Path(directory) / 'a.sqlite', policy,
                                 symbols=[str(i) for i in range(10)], cycles=1,
                                 clock=lambda: NOW, output_dir=directory,
                                 ports=no_broker)
            no_market = type('Ports', (), {'market': None, 'broker': object(),
                                          'execution': None, 'host': None})()
            with self.assertRaisesRegex(ValueError, 'MARKET_DATA_PORT_REQUIRED'):
                run_live_session(Path(directory) / 'b.sqlite', policy,
                                 symbols=[str(i) for i in range(10)], cycles=1,
                                 clock=lambda: NOW, output_dir=directory,
                                 ports=no_market)

    def test_an_incomplete_snapshot_is_refused_by_the_broker_port(self):
        from martin_agentic.adapters import ProviderBrokerReadPort
        port = ProviderBrokerReadPort(lambda: {'observed_at': NOW.isoformat()})
        with self.assertRaisesRegex(ValueError, 'BROKER_SNAPSHOT_INCOMPLETE'):
            port.snapshot()


class HostPortDispatcherTests(ApprovalFlow):
    def test_targeted_delivery_and_receipt_use_the_action_identity(self):
        from martin_luk.live_session import HostPortDispatcher
        class ReceiptHost(SpyHostPort):
            def __init__(self):
                super().__init__()
                self.receipts = []

            def receipt(self, intent_id):
                self.receipts.append(intent_id)
                return {'status': 'HOST_RECEIPT_IMPORTED', 'verified': True,
                        'native_result': {'report': {'status': 'submitted'}}}

        host = ReceiptHost()
        self.supervisor.dispatcher = HostPortDispatcher(self.book, host)
        entry = self.make_entry()
        delivery = host.delivered[0]
        self.assertEqual(delivery['action_id'], entry['id'])
        self.assertEqual(host.receipts, [delivery['intent_id']])
        action = next(item for item in self.book.actions() if item['id'] == entry['id'])
        self.assertEqual(action['state'], 'REPORTED')
        self.assertEqual(json.loads(action['report'])['status'], 'submitted')

    def test_a_host_without_target_aware_delivery_is_refused(self):
        from martin_luk.live_session import HostPortDispatcher
        with self.assertRaisesRegex(ValueError, 'HOST_DELIVERY_PORT_REQUIRED'):
            HostPortDispatcher(self.book, object())


class DefaultExecutionPortTests(TestCase):
    def test_the_default_factory_routes_queue_advance_through_the_port(self):
        from martin_luk.live_session import (LiveSupervisor, _PortAdvanceDispatcher,
                                                 default_live_ports)
        calls = []

        class Dispatcher:
            requires_observation = False

            def __call__(self, *, allow_entries=True, allow_dispatch=True):
                calls.append(bool(allow_entries))
                return {'agent_called': False, 'reason': 'SPY'}

        book = __import__('unittest.mock', fromlist=['MagicMock']).MagicMock()
        policy = json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        ports = default_live_ports(book, policy, lambda: NOW,
                                   dispatcher=Dispatcher())
        supervisor = LiveSupervisor(book, dispatcher=Dispatcher(),
                                    process_strategy=False, agent_mode='review',
                                    ports=ports)
        self.assertIsInstance(supervisor.dispatcher, _PortAdvanceDispatcher)
        supervisor._dispatch({'observed_at': NOW.isoformat()}, {},
                             allow_entries=False, allow_dispatch=True)
        self.assertEqual(calls, [False])
        # Targeted operations have no target-aware adapter and must refuse.
        with self.assertRaisesRegex(ValueError,
                                    'ADAPTER_TARGETED_OPERATION_REQUIRED'):
            ports.execution.place({'action_id': 'x', 'allow_entries': True})

    def test_local_worker_default_ports_are_explicit_not_blocked_host(self):
        from types import SimpleNamespace
        from martin_luk.live_session import LocalWorkerDispatcher, default_live_ports
        from martin_agentic.local_worker_wake import worker_adapter_report
        class Bridge:
            task = SimpleNamespace(status=lambda: {'running': False})
            pending = None
            def __call__(self, *args, **kwargs):
                return {'agent_called': False}
            def enqueue_local_worker_action(self, action):
                return {'status': 'FIXTURE'}
            def consume_local_worker_results(self):
                return []
            def settle_local_worker_wake(self):
                return None
            def close(self):
                return {'status': 'FIXTURE'}
        dispatcher = LocalWorkerDispatcher(object(), Bridge())
        report = worker_adapter_report(SimpleNamespace(
            capability={'local_worker_event': True, 'official_queue': False},
            enqueue=lambda *args, **kwargs: None))
        ports = default_live_ports(object(), {'account_suffix': '0000'},
                                   lambda: NOW, dispatcher=dispatcher,
                                   host_surface=dispatcher,
                                   worker_adapter=report)
        self.assertIsNone(ports.host)
        self.assertEqual(ports.host_delivery_status['status'],
                         'LOCAL_WORKER_EVENT_NOT_TARGETED_HOST')
        self.assertFalse(ports.host_delivery_status['available'])
        self.assertEqual(ports.worker_adapter['selection'], 'local-worker-review')
        self.assertTrue(ports.worker_adapter['local_worker_event'])
        self.assertFalse(ports.worker_adapter['official_queue'])
        self.assertFalse(ports.worker_adapter['formal_adapter_qualified'])
        self.assertIsNone(ports.execution.advance)

    def test_an_observation_bound_dispatcher_is_not_wrapped(self):
        from martin_luk.live_session import LiveSupervisor, default_live_ports
        from unittest.mock import MagicMock

        class Bridge:
            requires_observation = True

            def __call__(self, **kwargs):
                return {'agent_called': False}

        book = MagicMock()
        policy = json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        bridge = Bridge()
        ports = default_live_ports(book, policy, lambda: NOW, dispatcher=bridge)
        supervisor = LiveSupervisor(book, dispatcher=bridge,
                                    process_strategy=False, agent_mode='review',
                                    ports=ports)
        self.assertIs(supervisor.dispatcher, bridge)
        with self.assertRaisesRegex(ValueError,
                                    'ADAPTER_QUEUE_ADVANCE_UNAVAILABLE'):
            ports.execution.advance_queue(allow_entries=False)


class HostDeliveryAdapterTests(TestCase):
    def test_the_composition_adapter_delivers_and_reads_the_same_identity(self):
        import tempfile
        from martin_agentic.adapters import BridgeHostDeliveryPort
        from martin_luk.live_session import default_live_ports
        with tempfile.TemporaryDirectory() as directory:
            class Book:
                def clock(self):
                    return NOW
            class Bridge:
                binding = {'db': 'x', 'account': '••••0000'}
                book = Book()
                def __init__(self):
                    self.directory = Path(directory)
            bridge = Bridge()
            ports = default_live_ports(bridge.book, {'account_suffix': '0000'},
                                       lambda: NOW, host_surface=bridge)
            self.assertTrue(ports.host.available)
            intent = {'intent_id': 'intent-1', 'action_id': 'action-1',
                      'trade_id': 'trade-1', 'kind': 'entry'}
            delivered = ports.host.deliver(intent)
            request_id = BridgeHostDeliveryPort.request_id('intent-1')
            self.assertEqual(delivered['request_id'], request_id)
            request = json.loads((Path(directory) / 'requests' /
                                  (request_id + '.json')).read_text())
            self.assertEqual((request['action_id'], request['trade_id']),
                             ('action-1', 'trade-1'))
            self.assertIsNone(ports.host.receipt('intent-1'),
                              'no result yet is not an inferred receipt')
            results = Path(directory) / 'results'
            results.mkdir()
            (results / (request_id + '.json')).write_text(json.dumps(
                {'status': 'HOST_PACKET_PREPARED', 'request_id': request_id}))
            receipt = ports.host.receipt('intent-1')
            self.assertEqual(receipt['request_id'], request_id)
            self.assertIsNone(ports.host.receipt('intent-other'),
                              'another intent never reads this receipt')

    def test_a_surface_without_targeted_delivery_is_refused_not_guessed(self):
        from martin_luk.live_session import default_live_ports
        ports = default_live_ports(object(), {'account_suffix': '0000'},
                                   lambda: NOW, host_surface=object())
        self.assertFalse(ports.host.available)
        with self.assertRaisesRegex(ValueError,
                                    'HOST_TARGETED_DELIVERY_UNAVAILABLE'):
            ports.host.deliver({'intent_id': 'i', 'action_id': 'a'})

    def test_the_default_session_reports_both_capabilities(self):
        with tempfile.TemporaryDirectory() as directory:
            policy = json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
            policy['capture_only'] = True
            with patch('martin_agentic.live_session.single_instance'), \
                 patch('martin_agentic.live_session._broker_read',
                       return_value=snapshot(base=NOW)), \
                 patch('martin_agentic.live_session.collect_live_events',
                       return_value=([], {'quotes': 0})), \
                 patch('martin_agentic.live_session.notify_local'), \
                 patch('martin_agentic.live_session.print'):
                result = run_live_session(
                    Path(directory) / 'book.sqlite', policy,
                    symbols=[str(index) for index in range(10)], cycles=1,
                    clock=lambda: NOW, status_file=Path(directory) / 'status.json',
                    output_dir=directory, budget_entry_limit=0,
                    budget_total_limit=0)
            self.assertFalse(result['host_delivery_status']['available'])
            self.assertEqual(result['execution_targeted_operations'], 'UNAVAILABLE')


class ReceiptIdentityValidationTests(TestCase):
    def setUp(self):
        import tempfile
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        from martin_agentic.adapters import BridgeHostDeliveryPort

        class Book:
            def clock(self):
                return NOW

        class Bridge:
            binding = {'db': 'x', 'account': '••••0000'}
            book = Book()
            directory = Path(self.temp.name)
        self.bridge = Bridge()
        self.port = BridgeHostDeliveryPort(self.bridge)

    def deliver(self, **overrides):
        intent = {'intent_id': 'intent-1', 'action_id': 'action-1',
                  'trade_id': 'trade-1', 'kind': 'entry'}
        intent.update(overrides)
        return self.port.deliver(intent)

    def write_packet(self, rid=None, *, action='action-1', trade='trade-1',
                     operation='review', packet=None):
        from martin_agentic.adapters import BridgeHostDeliveryPort
        from martin_agentic.audit import digest
        rid = rid or BridgeHostDeliveryPort.request_id('intent-1')
        packet = packet or {'action_id': action, 'operation': operation,
                            'parameters': {}, 'operation_id': 'op-1'}
        envelope = {'schema_version': 1, 'request_id': rid,
                    'binding': dict(self.bridge.binding), 'packet': packet}
        folder = Path(self.temp.name) / 'packets'
        folder.mkdir(exist_ok=True)
        (folder / (rid + '.json')).write_text(json.dumps(envelope))
        return digest(packet)

    def write_result(self, value, request_id=None, *, packet_hash=None):
        from martin_agentic.adapters import BridgeHostDeliveryPort
        rid = request_id or BridgeHostDeliveryPort.request_id('intent-1')
        if isinstance(value, dict) and value.get('status') != 'raw':
            value = {'schema_version': 1, 'request_id': rid,
                     'action_id': 'action-1', 'trade_id': 'trade-1',
                     'operation_id': 'op-1',
                     'binding': dict(self.bridge.binding),
                     'packet_sha256': packet_hash or self.write_packet(rid),
                     **value}
        results = Path(self.temp.name) / 'results'
        results.mkdir(exist_ok=True)
        (results / (rid + '.json')).write_text(
            value if isinstance(value, str) else json.dumps(value))

    def test_a_receipt_with_another_request_id_is_unverified(self):
        self.deliver()
        from martin_agentic.adapters import BridgeHostDeliveryPort
        rid = BridgeHostDeliveryPort.request_id('intent-1')
        self.write_packet(rid)
        self.write_result({'status': 'HOST_RECEIPT_IMPORTED'},
                          request_id=rid, packet_hash=None)
        # Overwrite the request id to another value after a valid packet/result.
        results = Path(self.temp.name) / 'results' / (rid + '.json')
        value = json.loads(results.read_text())
        value['request_id'] = 'someone-else'
        results.write_text(json.dumps(value))
        receipt = self.port.receipt('intent-1')
        self.assertEqual(receipt['reason'], 'HOST_RECEIPT_REQUEST_ID_MISMATCH')
        self.assertFalse(receipt['verified'])

    def test_action_trade_binding_or_packet_operation_mismatch_is_unverified(self):
        from martin_agentic.adapters import BridgeHostDeliveryPort
        rid = BridgeHostDeliveryPort.request_id('intent-1')
        cases = [('action_id', 'other-action', 'HOST_RECEIPT_ACTION_ID_MISMATCH'),
                 ('trade_id', 'other-trade', 'HOST_RECEIPT_TRADE_ID_MISMATCH'),
                 ('binding', {'db': 'other'}, 'HOST_RECEIPT_BINDING_MISMATCH')]
        for field, value, reason in cases:
            with self.subTest(field=field):
                self.deliver()
                self.write_result({field: value,
                                   'status': 'HOST_RECEIPT_IMPORTED'},
                                  request_id=rid)
                receipt = self.port.receipt('intent-1')
                self.assertEqual(receipt['reason'], reason, field)
                self.assertFalse(receipt['verified'])
        # The packet's own operation is the operation identity; a packet for a
        # different operation cannot verify this request.
        packet_hash = self.write_packet(rid, operation='place')
        self.write_result({'status': 'HOST_RECEIPT_IMPORTED'}, request_id=rid,
                          packet_hash=packet_hash)
        receipt = self.port.receipt('intent-1')
        self.assertEqual(receipt['reason'], 'HOST_RECEIPT_PACKET_BINDING_MISMATCH')
        self.assertFalse(receipt['verified'])

    def test_corrupt_receipt_and_missing_request_fail_closed(self):
        from martin_agentic.adapters import BridgeHostDeliveryPort
        rid = BridgeHostDeliveryPort.request_id('intent-1')
        results = Path(self.temp.name) / 'results'
        results.mkdir(exist_ok=True)
        (results / (rid + '.json')).write_text('{not json')
        receipt = self.port.receipt('intent-1')
        self.assertEqual(receipt['reason'], 'HOST_DELIVERY_REQUEST_MISSING')
        self.deliver()
        receipt = self.port.receipt('intent-1')
        self.assertEqual(receipt['reason'], 'HOST_RECEIPT_UNREADABLE')

    def test_only_an_imported_packet_bound_receipt_is_verified(self):
        self.deliver()
        self.write_result({'status': 'HOST_RECEIPT_IMPORTED'})
        receipt = self.port.receipt('intent-1')
        self.assertTrue(receipt['verified'])
        self.assertEqual(receipt['status'], 'HOST_RECEIPT_IMPORTED')
        # A merely prepared packet is not an imported receipt.
        self.write_result({'status': 'HOST_PACKET_PREPARED'})
        receipt = self.port.receipt('intent-1')
        self.assertFalse(receipt['verified'])
        self.assertEqual(receipt['reason'], 'HOST_RECEIPT_NOT_IMPORTED')

    def test_missing_required_identity_fields_fail_closed(self):
        from martin_agentic.adapters import BridgeHostDeliveryPort
        from martin_agentic.audit import digest
        rid = BridgeHostDeliveryPort.request_id('intent-1')
        self.deliver()
        packet_hash = self.write_packet(rid)
        base = {'schema_version': 1, 'request_id': rid,
                'action_id': 'action-1', 'trade_id': 'trade-1',
                'operation_id': 'op-1', 'binding': dict(self.bridge.binding),
                'packet_sha256': packet_hash,
                'status': 'HOST_RECEIPT_IMPORTED'}
        cases = [('schema_version', 'HOST_RECEIPT_SCHEMA_INVALID'),
                 ('request_id', 'HOST_RECEIPT_REQUEST_ID_MISMATCH'),
                 ('action_id', 'HOST_RECEIPT_ACTION_ID_MISMATCH'),
                 ('trade_id', 'HOST_RECEIPT_TRADE_ID_MISMATCH'),
                 ('binding', 'HOST_RECEIPT_BINDING_MISMATCH'),
                 ('operation_id', 'HOST_RECEIPT_PACKET_OPERATION_MISMATCH')]
        results = Path(self.temp.name) / 'results'
        results.mkdir(exist_ok=True)
        for field, reason in cases:
            with self.subTest(field=field):
                value = dict(base)
                if field == 'trade_id':
                    value[field] = 'other-trade'
                else:
                    value.pop(field)
                (results / (rid + '.json')).write_text(json.dumps(value))
                receipt = self.port.receipt('intent-1')
                self.assertFalse(receipt['verified'], field)
                self.assertEqual(receipt['reason'], reason, field)
        for field, missing_reason in (
                ('trade_id', 'HOST_RECEIPT_TRADE_ID_MISMATCH'),
                ('packet_sha256', 'HOST_RECEIPT_PACKET_HASH_MISSING')):
            for mutation, replacement in (('missing', None), ('null', None),
                                          ('empty', '')):
                with self.subTest(field=field, mutation=mutation):
                    value = dict(base)
                    if mutation == 'missing':
                        value.pop(field)
                    else:
                        value[field] = replacement
                    (results / (rid + '.json')).write_text(json.dumps(value))
                    receipt = self.port.receipt('intent-1')
                    self.assertFalse(receipt['verified'], (field, mutation))
                    self.assertEqual(receipt['reason'], missing_reason)
        for field, bad, reason in (
                ('trade_id', 'other-trade', 'HOST_RECEIPT_TRADE_ID_MISMATCH'),
                ('packet_sha256', '0' * 64,
                 'HOST_RECEIPT_PACKET_HASH_MISMATCH'),
                ('packet_sha256', 'not-a-hash',
                 'HOST_RECEIPT_PACKET_HASH_INVALID')):
            value = dict(base, **{field: bad})
            (results / (rid + '.json')).write_text(json.dumps(value))
            receipt = self.port.receipt('intent-1')
            self.assertFalse(receipt['verified'])
            self.assertEqual(receipt['reason'], reason)
        # The packet itself must name the operation identity, not just the
        # receipt; a packet without one cannot verify any result.
        bare = {'action_id': 'action-1', 'operation': 'review',
                'parameters': {}}
        envelope = {'schema_version': 1, 'request_id': rid,
                    'binding': dict(self.bridge.binding), 'packet': bare}
        (Path(self.temp.name) / 'packets' / (rid + '.json')).write_text(
            json.dumps(envelope))
        value = dict(base, packet_sha256=digest(bare))
        (results / (rid + '.json')).write_text(json.dumps(value))
        receipt = self.port.receipt('intent-1')
        self.assertFalse(receipt['verified'])
        self.assertEqual(receipt['reason'],
                         'HOST_RECEIPT_PACKET_OPERATION_MISSING')

    def test_a_late_result_is_not_a_current_receipt(self):
        from martin_agentic.adapters import BridgeHostDeliveryPort
        self.deliver()
        rid = BridgeHostDeliveryPort.request_id('intent-1')
        late = Path(self.temp.name) / 'late-read-results'
        late.mkdir(exist_ok=True)
        (late / (rid + '.json')).write_text(json.dumps(
            {'schema_version': 1, 'request_id': rid,
             'action_id': 'action-1', 'binding': dict(self.bridge.binding),
             'status': 'HOST_RECEIPT_IMPORTED'}))
        self.assertIsNone(self.port.receipt('intent-1'))

    def _delivery_files(self):
        from martin_agentic.adapters import BridgeHostDeliveryPort
        rid = BridgeHostDeliveryPort.request_id('intent-1')
        return (rid, self.port._index_path('intent-1'),
                Path(self.temp.name) / 'requests' / (rid + '.json'))

    def test_public_receipt_rejects_index_request_identity_mismatch(self):
        import copy
        self.deliver()
        self.write_result({'status': 'HOST_RECEIPT_IMPORTED'})
        rid, index_path, request_path = self._delivery_files()
        original_index = json.loads(index_path.read_text())
        original_request = json.loads(request_path.read_text())

        def cross_operation(index, request):
            index['operation'] = 'place'

        def cross_request_id(index, request):
            index['request_id'] = self.port.request_id('intent-1', 'place')

        def wrong_index_action(index, request):
            index['action_id'] = 'other-action'

        def wrong_index_trade(index, request):
            index['trade_id'] = 'other-trade'

        def wrong_request_intent(index, request):
            request['intent_id'] = 'other-intent'

        def wrong_request_action(index, request):
            request['action_id'] = 'other-action'

        def wrong_request_trade(index, request):
            request['trade_id'] = 'other-trade'

        def wrong_request_operation(index, request):
            request['operation'] = 'place'

        cases = [
            (cross_operation, 'HOST_DELIVERY_INDEX_IDENTITY_MISMATCH'),
            (cross_request_id, 'HOST_DELIVERY_INDEX_IDENTITY_MISMATCH'),
            (wrong_index_action, 'HOST_DELIVERY_INDEX_REQUEST_MISMATCH'),
            (wrong_index_trade, 'HOST_DELIVERY_INDEX_REQUEST_MISMATCH'),
            (wrong_request_intent, 'HOST_DELIVERY_REQUEST_INTENT_MISMATCH'),
            (wrong_request_action, 'HOST_DELIVERY_INDEX_REQUEST_MISMATCH'),
            (wrong_request_trade, 'HOST_DELIVERY_INDEX_REQUEST_MISMATCH'),
            (wrong_request_operation,
             'HOST_DELIVERY_REQUEST_IDENTITY_MISMATCH'),
        ]
        for mutate, reason in cases:
            with self.subTest(reason=reason):
                index = copy.deepcopy(original_index)
                request = copy.deepcopy(original_request)
                mutate(index, request)
                index_path.write_text(json.dumps(index))
                request_path.write_text(json.dumps(request))
                receipt = self.port.receipt('intent-1')
                self.assertFalse(receipt['verified'], receipt)
                self.assertEqual(receipt['reason'], reason)
        # Restore and prove the normal pair still verifies.
        index_path.write_text(json.dumps(original_index))
        request_path.write_text(json.dumps(original_request))
        self.assertTrue(self.port.receipt('intent-1')['verified'])

    def test_raw_gate_rejects_index_request_identity_mismatch(self):
        import copy
        from martin_agentic.audit import digest
        rid = self.deliver()['request_id']
        packet = {'action_id': 'action-1', 'operation': 'review',
                  'parameters': {}, 'operation_id': 'op-1'}
        folder = Path(self.temp.name) / 'packets'
        folder.mkdir(exist_ok=True)
        envelope = {'schema_version': 1, 'request_id': rid,
                    'binding': dict(self.bridge.binding), 'packet': packet}
        (folder / (rid + '.json')).write_text(json.dumps(envelope))
        raw = {'schema_version': 1, 'request_id': rid,
               'action_id': 'action-1', 'trade_id': 'trade-1',
               'operation_id': 'op-1',
               'binding': dict(self.bridge.binding),
               'packet_sha256': digest(packet)}
        pending = {'request_id': rid, 'packet': packet}
        self.assertTrue(self.port.validate_raw_receipt(pending, raw))
        rid, index_path, request_path = self._delivery_files()
        original_index = json.loads(index_path.read_text())
        original_request = json.loads(request_path.read_text())
        cases = [
            (lambda i, r: i.update(operation='place'),
             'HOST_DELIVERY_INDEX_IDENTITY_MISMATCH'),
            (lambda i, r: i.update(
                request_id=self.port.request_id('intent-1', 'place')),
             'HOST_RECEIPT_NOT_PORT_DELIVERED'),
            (lambda i, r: i.update(action_id='other-action'),
             'HOST_DELIVERY_INDEX_REQUEST_MISMATCH'),
            (lambda i, r: i.update(trade_id='other-trade'),
             'HOST_DELIVERY_INDEX_REQUEST_MISMATCH'),
            (lambda i, r: r.update(intent_id='other-intent'),
             'HOST_DELIVERY_REQUEST_INTENT_MISMATCH'),
            (lambda i, r: r.update(action_id='other-action'),
             'HOST_DELIVERY_INDEX_REQUEST_MISMATCH'),
            (lambda i, r: r.update(trade_id='other-trade'),
             'HOST_DELIVERY_INDEX_REQUEST_MISMATCH'),
            (lambda i, r: r.update(operation='place'),
             'HOST_DELIVERY_REQUEST_IDENTITY_MISMATCH'),
        ]
        for mutate, reason in cases:
            with self.subTest(reason=reason):
                index = copy.deepcopy(original_index)
                request = copy.deepcopy(original_request)
                mutate(index, request)
                index_path.write_text(json.dumps(index))
                request_path.write_text(json.dumps(request))
                with self.assertRaisesRegex(ValueError, reason):
                    self.port.validate_raw_receipt(pending, dict(raw))
        index_path.write_text(json.dumps(original_index))
        request_path.write_text(json.dumps(original_request))
        self.assertTrue(self.port.validate_raw_receipt(pending, dict(raw)))

    def test_operations_use_explicit_request_identity_and_review_place_transition(self):
        from martin_agentic.adapters import BridgeHostDeliveryPort
        review = self.deliver(operation='review')
        place = self.deliver(operation='place')
        self.assertNotEqual(review['request_id'], place['request_id'])
        self.assertEqual(review['operation'], 'review')
        self.assertEqual(place['operation'], 'place')
        rid = BridgeHostDeliveryPort.request_id('intent-1')
        self.assertEqual(review['request_id'], rid)
        review_request = json.loads(
            (Path(self.temp.name) / 'requests' / (review['request_id'] + '.json')).read_text())
        place_request = json.loads(
            (Path(self.temp.name) / 'requests' / (place['request_id'] + '.json')).read_text())
        self.assertEqual(review_request['operation'], 'review')
        self.assertEqual(place_request['operation'], 'place')
        self.assertEqual(place_request['action_id'], 'action-1')
        self.assertEqual(place_request['trade_id'], 'trade-1')
        cancel = self.deliver(intent_id='intent-cancel',
                              action_id='cancel-action', operation='cancel')
        cancel_request = json.loads(
            (Path(self.temp.name) / 'requests' /
             (cancel['request_id'] + '.json')).read_text())
        self.assertEqual(cancel['operation'], 'cancel')
        self.assertEqual(cancel_request['operation'], 'cancel')
        self.assertEqual(cancel['request_id'],
                         BridgeHostDeliveryPort.request_id('intent-cancel',
                                                           'cancel'))
        with self.assertRaisesRegex(ValueError, 'ADAPTER_OPERATION_INVALID'):
            self.deliver(operation='exit')
        # A place request cannot silently become another review attempt.
        with self.assertRaisesRegex(ValueError, 'HOST_DELIVERY_IDENTITY_CONFLICT'):
            self.deliver(operation='review')

    def test_raw_receipt_gate_checks_identity_before_import(self):
        from martin_agentic.adapters import BridgeHostDeliveryPort
        from martin_agentic.audit import digest
        rid = BridgeHostDeliveryPort.request_id('intent-1')
        recorded = self.deliver(operation='review')
        self.assertEqual(recorded['request_id'], rid)
        packet = {'action_id': 'action-1', 'operation': 'review',
                  'parameters': {}, 'operation_id': 'op-1'}
        envelope = {'schema_version': 1, 'request_id': rid,
                    'binding': dict(self.bridge.binding), 'packet': packet}
        folder = Path(self.temp.name) / 'packets'
        folder.mkdir(exist_ok=True)
        (folder / (rid + '.json')).write_text(json.dumps(envelope))
        pending = {'request_id': rid, 'packet': packet}
        base = {'schema_version': 1, 'request_id': rid,
                'action_id': 'action-1', 'trade_id': 'trade-1',
                'operation_id': 'op-1',
                'binding': dict(self.bridge.binding),
                'packet_sha256': digest(packet)}
        self.assertTrue(self.port.validate_raw_receipt(pending, dict(base)))
        cases = [
            ('schema_version', 'HOST_RECEIPT_SCHEMA_INVALID'),
            ('request_id', 'HOST_RECEIPT_REQUEST_ID_MISMATCH'),
            ('action_id', 'HOST_RECEIPT_ACTION_ID_MISMATCH'),
            ('trade_id', 'HOST_RECEIPT_TRADE_ID_MISMATCH'),
            ('binding', 'HOST_RECEIPT_BINDING_MISMATCH'),
            ('operation_id', 'HOST_RECEIPT_PACKET_OPERATION_MISMATCH'),
            ('packet_sha256', 'HOST_RECEIPT_PACKET_HASH_MISMATCH'),
        ]
        for field, reason in cases:
            with self.subTest(field=field):
                value = dict(base)
                value[field] = ('other' if field != 'packet_sha256'
                                else '0' * 64)
                with self.assertRaisesRegex(ValueError, reason):
                    self.port.validate_raw_receipt(pending, value)
        with self.assertRaisesRegex(ValueError,
                                    'HOST_RECEIPT_PACKET_HASH_INVALID'):
            self.port.validate_raw_receipt(
                pending, dict(base, packet_sha256='not-a-hash'))
        for field in ('schema_version', 'request_id', 'action_id',
                      'binding', 'operation_id'):
            with self.subTest(missing=field):
                value = dict(base)
                value.pop(field)
                with self.assertRaises(ValueError):
                    self.port.validate_raw_receipt(pending, value)
        for field, missing_reason in (
                ('trade_id', 'HOST_RECEIPT_TRADE_ID_MISMATCH'),
                ('packet_sha256', 'HOST_RECEIPT_PACKET_HASH_MISSING')):
            for mutation, replacement in (('missing', None), ('null', None),
                                          ('empty', '')):
                with self.subTest(field=field, mutation=mutation):
                    value = dict(base)
                    if mutation == 'missing':
                        value.pop(field)
                    else:
                        value[field] = replacement
                    with self.assertRaisesRegex(ValueError, missing_reason):
                        self.port.validate_raw_receipt(pending, value)
        wrong_operation = dict(packet, operation='place')
        with self.assertRaisesRegex(ValueError,
                                    'HOST_RECEIPT_PACKET_BINDING_MISMATCH'):
            self.port.validate_raw_receipt(
                {'request_id': rid, 'packet': wrong_operation}, dict(base))
        missing_packet_operation = dict(packet)
        missing_packet_operation.pop('operation_id')
        missing_envelope = dict(envelope, packet=missing_packet_operation)
        (folder / (rid + '.json')).write_text(json.dumps(missing_envelope))
        with self.assertRaisesRegex(ValueError,
                                    'HOST_RECEIPT_PACKET_OPERATION_MISSING'):
            self.port.validate_raw_receipt(
                {'request_id': rid, 'packet': missing_packet_operation},
                dict(base))
        self.port._index_path('intent-1').unlink()
        with self.assertRaisesRegex(ValueError,
                                    'HOST_RECEIPT_NOT_PORT_DELIVERED'):
            self.port.validate_raw_receipt(pending, dict(base))

    def test_the_same_intent_cannot_be_rebound_to_another_action(self):
        self.deliver()
        with self.assertRaisesRegex(ValueError,
                                    'HOST_DELIVERY_IDENTITY_CONFLICT'):
            self.deliver(action_id='other-action')
        from martin_agentic.adapters import BridgeHostDeliveryPort
        rid = BridgeHostDeliveryPort.request_id('intent-1')
        conflict = json.loads((Path(self.temp.name) / 'delivery-conflicts' /
                               (rid + '.json')).read_text())
        self.assertEqual(conflict['first']['action_id'], 'action-1')
        self.assertEqual(conflict['second']['action_id'], 'other-action')


class HostPortDispatcherReceiptStateTests(ApprovalFlow):
    def ready_entry(self):
        self.supervisor.dispatcher = NoClaimDispatcher()
        return self.make_entry()

    def dispatcher_with(self, receipt):
        from martin_luk.live_session import HostPortDispatcher

        class Host:
            def __init__(self):
                self.delivered = []
                self.receipts = []

            def deliver(self, intent):
                self.delivered.append(intent)

            def receipt(self, intent_id):
                self.receipts.append(intent_id)
                return receipt

        host = Host()
        return HostPortDispatcher(self.book, host), host

    def current(self, entry):
        return next(item for item in self.book.actions() if item['id'] == entry['id'])

    def test_a_waiting_receipt_is_not_reported_as_accepted(self):
        entry = self.ready_entry()
        dispatcher, _ = self.dispatcher_with(None)
        result = dispatcher(allow_entries=True, allow_dispatch=True)
        self.assertEqual(result['status'], 'HOST_TOOL_RESULT_PENDING')
        self.assertFalse(result['host_verified'])
        self.assertEqual(self.current(entry)['state'], 'RUNNING')

    def test_an_unverified_receipt_never_moves_the_action(self):
        entry = self.ready_entry()
        dispatcher, _ = self.dispatcher_with(
            {'status': 'HOST_RECEIPT_UNVERIFIED', 'verified': False,
             'reason': 'HOST_RECEIPT_REQUEST_ID_MISMATCH'})
        result = dispatcher(allow_entries=True, allow_dispatch=True)
        self.assertEqual(result['status'], 'HOST_RECEIPT_UNVERIFIED')
        self.assertFalse(result['host_verified'])
        self.assertEqual(self.current(entry)['state'], 'RUNNING')
        self.assertIsNone(self.current(entry)['report'])

    def run_status(self, native_status):
        entry = self.ready_entry()
        dispatcher, _ = self.dispatcher_with(
            {'status': 'HOST_RECEIPT_IMPORTED', 'verified': True,
             'request_id': 'r', 'native_result': {'report': {'status': native_status}}})
        result = dispatcher(allow_entries=True, allow_dispatch=True)
        self.assertTrue(result.get('host_verified'), result)
        return self.current(entry)

    def test_submitted_maps_to_reported(self):
        self.assertEqual(self.run_status('submitted')['state'], 'REPORTED')

    def test_unknown_maps_to_unknown_without_replay(self):
        action = self.run_status('unknown')
        self.assertEqual(action['state'], 'UNKNOWN')

    def test_needs_confirmation_maps_to_waiting_user(self):
        self.assertEqual(self.run_status('needs_confirmation')['state'],
                         'WAITING_USER')

    def test_pending_approval_never_looks_submitted(self):
        action = self.run_status('pending_approval')
        # The report keeps the native outcome; it is never rewritten into
        # "submitted" because a receipt merely arrived.
        self.assertEqual(json.loads(action['report'])['status'],
                         'pending_approval')
        self.assertNotEqual(action['state'], 'OBSERVED')

    def test_review_only_never_looks_submitted(self):
        action = self.run_status('review_only')
        self.assertEqual(json.loads(action['report'])['status'], 'review_only')
        self.assertNotEqual(action['state'], 'OBSERVED')

    def test_rejected_maps_to_reported_rejected(self):
        action = self.run_status('rejected')
        self.assertEqual(action['state'], 'REPORTED')
        self.assertEqual(json.loads(action['report'])['status'], 'rejected')
