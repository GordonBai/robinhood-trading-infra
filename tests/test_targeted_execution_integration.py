"""Real ExecutionBook + HostExecutionBridge integration for targeted operations."""
import json
import tempfile
from datetime import timedelta
from pathlib import Path
from unittest import TestCase

from martin_agentic.adapters import BridgeHostDeliveryPort
from martin_agentic.host_execution import HostExecutionBridge
from martin_agentic.host_mailbox import read_private_json, submit_receipt
from martin_luk.live_session import HostDeliveryDispatcher
import test_native_operations

NOW = None


class TargetedHostIntegrationTests(TestCase):
    def setUp(self):
        self.fixture = test_native_operations.NativeOperationTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.f = self.fixture.f
        self.book = self.fixture.book
        self.directory = Path(self.f.tmp.name) / 'host'
        self.bridge = HostExecutionBridge(self.book, self.directory)
        self.addCleanup(self.bridge.close)
        self.action = self.fixture.action
        # A second real READY action must never be selected by target A.
        original = next(a for a in self.book.actions() if a['id'] == self.action)
        payload = json.loads(original['payload'])
        payload.update(action_id='cancel-B', ref_id='ref-cancel-B',
                       parameters={'order_id': 'order-B'})
        self.book.db.execute(
            'INSERT INTO live_actions VALUES(?,?,?,?,?,?,?,NULL)',
            ('cancel-B', original['trade_id'], 'cancel', 'READY',
             json.dumps(payload), original['created_at'], original['updated_at']))
        self.book.db.commit()

    def _dispatcher(self):
        return HostDeliveryDispatcher(
            self.book, BridgeHostDeliveryPort(self.bridge), self.bridge)

    def _snapshot(self):
        return self.fixture.snapshot()

    def _quotes(self):
        return {'DEMO': self.f.quote}

    def _intent(self, snapshot=None, *, allow_entries=False):
        return {'action_id': self.action, 'snapshot': snapshot or self._snapshot(),
                'quotes': self._quotes(), 'allow_entries': allow_entries}

    def _respond(self, request_id, response):
        submit_receipt(self.directory, request_id, response,
                       clock=lambda: self.f.now)
        out = self.bridge(allow_dispatch=False)
        self.assertEqual(out['status'], 'HOST_RECEIPT_IMPORTED', out)
        return out

    def test_real_review_place_and_receipt_are_target_bound(self):
        dispatcher = self._dispatcher()
        review = dispatcher.operate(self.action, 'review', self._intent())
        self.assertEqual(review['host_status'], 'HOST_TOOL_RESULT_PENDING', review)
        request = read_private_json(
            self.directory / 'requests' / (review['request_id'] + '.json'))
        self.assertEqual(request['action_id'], self.action)
        packet = read_private_json(
            self.directory / 'packets' / (review['request_id'] + '.json'))['packet']
        response = {'data': {**packet['parameters'], 'order_checks': {},
                             'customer_approval_required': False,
                             'market_data_disclosure': 'Exact current disclosure',
                             'quote_data': {}}}
        self._respond(review['request_id'], response)
        imported = dispatcher.host.receipt(review['intent_id'])
        self.assertEqual(imported['status'], 'HOST_RECEIPT_IMPORTED', imported)
        self.assertTrue(imported['verified'])
        place = dispatcher.operate(self.action, 'place', self._intent())
        self.assertEqual(place['host_status'], 'HOST_TOOL_RESULT_PENDING', place)
        place_packet = read_private_json(
            self.directory / 'packets' / (place['request_id'] + '.json'))['packet']
        self._respond(place['request_id'], self.fixture.official_order(place_packet))
        place_receipt = dispatcher.host.receipt(place['intent_id'])
        self.assertEqual(place_receipt['status'], 'HOST_RECEIPT_IMPORTED',
                         place_receipt)
        self.assertTrue(place_receipt['verified'])
        self.assertFalse(place['execution_authorized'])
        self.assertFalse(place['broker_effect_verified'])
        self.assertEqual(
            next(a for a in self.book.actions() if a['id'] == 'cancel-B')['state'],
            'READY')
        self.assertFalse((self.directory / 'requests' / 'cancel-B.json').exists())

    def test_real_stale_snapshot_is_not_reported_imported(self):
        dispatcher = self._dispatcher()
        snapshot = self._snapshot()
        snapshot['observed_at'] = (
            self.f.now - timedelta(seconds=90)).isoformat()
        result = dispatcher.operate(self.action, 'review',
                                    self._intent(snapshot=snapshot))
        self.assertNotEqual(result['host_status'], 'HOST_RECEIPT_IMPORTED')
        self.assertFalse(result['verified'])
        self.assertFalse(result['fresh_snapshot_verified'])
        self.assertIn('HOST_FRESH_INPUT_REQUIRED',
                      json.dumps(result.get('bridge_reason') or '') +
                      json.dumps(result.get('reason') or '') +
                      json.dumps(result.get('bridge_status') or ''))
        self.assertIsNone(self.bridge.pending)
        self.assertEqual(
            next(a for a in self.book.actions() if a['id'] == self.action)['state'],
            'READY')


class PublicSessionTargetedIntegrationTests(TestCase):
    def test_public_default_session_calls_targeted_execution_interface(self):
        import json
        import tempfile
        from pathlib import Path
        from unittest.mock import patch
        from martin_luk.live_session import LiveSupervisor, run_live_session
        from martin_agentic.simulation import SimulationSources
        from test_default_lifecycle import (BrokerPort, BrokerState, Host,
                                            MarketPort, TradingClock)
        from test_supervised_host_default_loop import (SimulationReviewTransport,
                                                       SimulationWake)
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        workspace = Path(temp.name) / 'workspace'
        state = BrokerState(TradingClock())
        host = Host(workspace / 'output' / 'host-execution', state,
                    clock=state.clock)
        host.start()
        self.addCleanup(host.join, 1)
        self.addCleanup(host.stop.set)
        policy = json.loads(
            Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        policy.update(capture_only=False, live_enabled=False, strategy_instances=[
            {'instance_id': 'vwap-live', 'plugin': 'vwap-fixture'},
            {'instance_id': 'martin-observe', 'plugin': 'martin'}])
        calls = []
        original = LiveSupervisor.step

        def observe(supervisor, *args, **kwargs):
            result = original(supervisor, *args, **kwargs)
            if calls:
                return result
            execution = getattr(getattr(supervisor, 'ports', None),
                                'execution', None)
            if not callable(getattr(execution, 'operate', None)):
                return result
            ready = [a for a in supervisor.book.actions()
                     if a.get('state') == 'READY'
                     and a.get('kind') in {'protect', 'exit', 'entry'}]
            if not ready:
                return result
            snapshot = args[0] if args else kwargs.get('snapshot')
            events = args[1] if len(args) > 1 else kwargs.get('events') or []
            quotes = {}
            for event in events:
                if event.get('kind') == 'quote':
                    quotes[event['data']['symbol']] = event['data']
            try:
                calls.append(execution.review({
                    'action_id': ready[0]['id'], 'snapshot': snapshot,
                    'quotes': quotes, 'allow_entries': False}))
            except Exception as exc:
                calls.append({'error': str(exc)})
            return result

        with patch.object(LiveSupervisor, 'step', observe), \
                patch('martin_agentic.live_session.single_instance'), \
                patch('martin_agentic.live_session.wait_for_result_or_tick',
                      side_effect=lambda *a, **k: __import__('time').sleep(.05)), \
                patch('martin_agentic.live_session.notify_local'), \
                patch('martin_agentic.live_session.print'):
            result = run_live_session(
                workspace / 'book.sqlite', policy,
                symbols=['DEMO'] + [str(i) for i in range(9)],
                cycles=80, poll_seconds=5, agent_mode='supervised-host',
                simulation=True, simulation_workspace=workspace,
                simulation_sources=SimulationSources(
                    broker=BrokerPort(state), market=MarketPort(state.clock),
                    review=SimulationReviewTransport, wake=SimulationWake()),
                clock=state.clock, status_file=workspace / 'status.json',
                output_dir=workspace / 'output')
        self.assertTrue(calls, result)
        self.assertNotIn('error', calls[0], calls)
        self.assertEqual(calls[0]['status'], 'HOST_DELIVERY_RECORDED', calls)
        self.assertFalse(calls[0]['execution_authorized'])
