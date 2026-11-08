"""Default supervised-host composition end to end, synthetic only.

run_live_session builds the ExecutionBook, HostExecutionBridge, default ports,
HostDeliveryPort and LiveSupervisor itself; the test supplies explicitly marked
synthetic broker/market/review/wake surfaces and plays the formal host's file
I/O (packets in, receipts out) inside a fresh temporary workspace. Formal
provider reads and the production review transport are patched to fail if
touched. No provider, broker write or real account.
"""
import json
import sqlite3
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from martin_agentic.audit import digest
from martin_agentic.broker import BrokerJournal
from martin_agentic.host_mailbox import submit_request
from martin_luk.live_session import run_live_session
from martin_agentic.simulation import SimulationSources
from test_live_session import full_snapshot

NOW = datetime(2026, 10, 16, 14, 0, tzinfo=timezone.utc)


def quote_event(symbol='DEMO'):
    at = NOW.isoformat()
    return {'id': 'e-' + at, 'kind': 'quote', 'at': at, 'synthetic_input': True,
            'source_sha256': 'market-' + at,
            'data': {'symbol': symbol, 'bid': '39.99', 'ask': '40.00',
                     'bid_at': at, 'ask_at': at, 'observed_at': at}}


def review_response(packet):
    return {'data': {**packet['parameters'], 'order_checks': {},
                     'customer_approval_required': False,
                     'market_data_disclosure': 'Exact disclosure',
                     'quote_data': {}}}


def approval_response():
    return {'structuredContent': {'data': {'order': None, 'approval': {
        'approval_id': 'official-approval', 'asset_class': 'ASSET_CLASS_EQUITY',
        'state': 'APPROVAL_STATE_PENDING', 'is_edited': False},
        'guide': 'The order was NOT placed.'}}}


class SimulationReviewTransport:
    """The synthetic review fixture; never a broker client."""

    simulation_only = True

    def __init__(self, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def close(self):
        pass

    def execute_review(self, packet):
        return review_response(packet)


class SimulationBroker:
    """Marked synthetic broker snapshot surface."""

    simulation_only = True

    def __init__(self):
        self.calls = 0

    def __call__(self, approval_ids=()):
        self.calls += 1
        return full_snapshot(NOW.isoformat())


class SimulationMarket:
    """Marked synthetic market collection surface."""

    simulation_only = True

    def __init__(self):
        self.calls = 0

    def __call__(self, *args, **kwargs):
        self.calls += 1
        return [quote_event()], {'quotes': 1, 'source_sha256': 'market'}


class SimulationWake:
    """Marked synthetic wake surface. It is never eligible to enqueue."""

    simulation_only = True
    capability = {}

    def __init__(self):
        self.calls = 0

    def enqueue(self, *args, **kwargs):
        self.calls += 1
        raise AssertionError('simulation wake must not be called')


class HostPlayer(threading.Thread):
    """The formal host's file side of the contract, on a temporary directory."""

    def __init__(self, directory, clock, receipt_mutation=None):
        super().__init__(daemon=True)
        self.directory = Path(directory)
        self.clock = clock
        self.receipt_mutation = receipt_mutation
        self.stop = threading.Event()
        self.packets = {}
        self.receipts = []
        self.place_requests = []

    def run(self):
        while not self.stop.is_set():
            self.step()
            time.sleep(0.01)

    def step(self):
        folder = self.directory / 'packets'
        if not folder.exists():
            return
        for path in sorted(folder.glob('*.json')):
            try:
                envelope = json.loads(path.read_text())
            except (OSError, ValueError):
                continue
            packet = envelope['packet']
            self.packets[packet['operation_id']] = envelope
            receipt = self.directory / 'receipts' / (packet['operation_id'] + '.json')
            if not receipt.exists():
                response = (review_response(packet)
                            if packet['operation'] == 'review'
                            else approval_response())
                receipt.parent.mkdir(parents=True, exist_ok=True)
                value = {'schema_version': 1, 'binding': envelope['binding'],
                         'request_id': envelope['request_id'],
                         'action_id': packet['action_id'],
                         'trade_id': json.loads(
                             (self.directory / 'requests' /
                              (envelope['request_id'] + '.json')).read_text()
                         ).get('trade_id'),
                         'operation_id': packet['operation_id'],
                         'packet_sha256': envelope['packet_sha256'],
                         'received_at': self.clock().isoformat(),
                         'execution_authorized': False,
                         'broker_effect_verified': False,
                         'response_sha256': digest(response),
                         'response': response}
                if self.receipt_mutation is not None:
                    self.receipt_mutation(value)
                receipt.write_text(json.dumps(value))
                self.receipts.append((packet['operation_id'], value))


class SyntheticJournalTests(TestCase):
    def test_synthetic_observation_requires_explicit_journal_allowance(self):
        with tempfile.TemporaryDirectory() as directory:
            snapshot = full_snapshot(NOW.isoformat())
            default = BrokerJournal(Path(directory) / 'default.sqlite')
            self.addCleanup(default.close)
            with self.assertRaisesRegex(
                    ValueError, 'Require complete explicit real broker observation'):
                default.ingest(snapshot)
            allowed = BrokerJournal(Path(directory) / 'allowed.sqlite',
                                    allow_synthetic=True)
            self.addCleanup(allowed.close)
            allowed.ingest(snapshot)
            self.assertTrue(allowed.summary(
                as_of=NOW.isoformat())['read_healthy'])
            spoof = dict(snapshot, provider='not-robinhood')
            with self.assertRaisesRegex(
                    ValueError, 'Require complete explicit real broker observation'):
                allowed.ingest(spoof)


class DefaultSupervisedHostLoopTests(TestCase):
    def test_missing_host_receipt_stays_pending_and_never_reports(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory) / 'workspace'
            db_path = workspace / 'book.sqlite'
            output = workspace / 'output'
            policy = json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
            policy.update(capture_only=False, live_enabled=False,
                          strategy_instances=[{'instance_id': 'vwap-e2e',
                                               'plugin': 'vwap-fixture'}])
            broker = SimulationBroker()
            market = SimulationMarket()
            wake = SimulationWake()

            def forbidden(*args, **kwargs):
                raise AssertionError('formal surface called')

            def wait_until_entry_or_strategy_ready(*args, **kwargs):
                # The warm strategy child is a separate process. A fixed sleep
                # can end the bounded session before it has produced the first
                # proposal, especially under suite load. Poll the existing
                # owner/strategy handshake with one absolute deadline; return
                # as soon as an entry action exists or strategy work is idle.
                # No production timing limit is relaxed.
                supervisor = kwargs.get('strategy_supervisor')
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    probe = sqlite3.connect(db_path)
                    try:
                        found = probe.execute(
                            "SELECT 1 FROM live_actions"
                            " WHERE kind='entry' LIMIT 1").fetchone()
                    finally:
                        probe.close()
                    if found:
                        return
                    if supervisor is not None:
                        supervisor.advance_strategy()
                        if not supervisor.strategy_busy():
                            return
                    time.sleep(0.01)

            with patch('martin_agentic.live_session.single_instance'), \
                 patch('martin_agentic.live_session._broker_read',
                       side_effect=forbidden), \
                 patch('martin_agentic.live_session.collect_live_events',
                       side_effect=forbidden), \
                 patch('martin_agentic.live_session.notify_local'), \
                 patch('martin_agentic.live_session.print'), \
                 patch('martin_agentic.live_session.wait_for_result_or_tick',
                       side_effect=wait_until_entry_or_strategy_ready):
                result = run_live_session(
                    db_path, policy,
                    symbols=['DEMO'] + [str(index) for index in range(9)],
                    cycles=8, poll_seconds=5,
                    agent_mode='supervised-host', simulation=True,
                    simulation_workspace=workspace,
                    simulation_sources=SimulationSources(
                        broker=broker, market=market,
                        review=SimulationReviewTransport, wake=wake),
                    clock=lambda: NOW, status_file=workspace / 'status.json',
                    output_dir=output, budget_entry_limit=0,
                    budget_total_limit=0)
            self.assertEqual(result['worker_budget_entry_scope'],
                             'not_consumed_by_selected_delivery')
            db = sqlite3.connect(db_path)
            self.addCleanup(db.close)
            actions = db.execute(
                "SELECT id,state,report FROM live_actions WHERE kind='entry'"
            ).fetchall()
            self.assertEqual(len(actions), 1)
            action_id, state, report = actions[0]
            self.assertIn(state, {'RUNNING', 'WAITING_USER'})
            if report is not None:
                # Bridge shutdown may locally abandon a read-only review; that
                # is a fresh-review requirement, never an imported host report.
                self.assertEqual(json.loads(report)['status'], 'review_only')
                self.assertEqual(db.execute(
                    "SELECT count(*) FROM native_operation_records"
                    " WHERE state='RECORDED'").fetchone()[0], 0)
            self.assertEqual(db.execute(
                'SELECT count(*) FROM broker_executions').fetchone()[0], 0)
            self.assertTrue((output / 'host-execution' / 'packets').exists())
            self.assertEqual(len(list((output / 'host-execution' /
                                       'requests').glob('*.json'))), 1)
            results = [json.loads(path.read_text()) for path in
                       (output / 'host-execution' / 'results').glob('*.json')]
            self.assertNotIn('HOST_RECEIPT_IMPORTED',
                             {item.get('status') for item in results})
            dispatch = result.get('dispatch') or {}
            self.assertEqual(dispatch.get('host_status'),
                             'HOST_TOOL_RESULT_PENDING')
            self.assertFalse(dispatch.get('host_verified'))
            self.assertEqual([item['operation']
                              for item in dispatch.get('deliveries', [])],
                             ['review'])

    def test_receipt_missing_trade_identity_is_refused_before_book_report(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory) / 'workspace'
            db_path = workspace / 'book.sqlite'
            output = workspace / 'output'
            policy = json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
            policy.update(capture_only=False, live_enabled=False,
                          strategy_instances=[{'instance_id': 'vwap-e2e',
                                               'plugin': 'vwap-fixture'}])
            broker = SimulationBroker()
            market = SimulationMarket()
            wake = SimulationWake()

            def forbidden(*args, **kwargs):
                raise AssertionError('formal surface called')

            player = HostPlayer(
                output / 'host-execution', lambda: NOW,
                receipt_mutation=lambda value: value.pop('trade_id'))
            player.start()
            try:
                with patch('martin_agentic.live_session.single_instance'), \
                     patch('martin_agentic.live_session._broker_read',
                           side_effect=forbidden), \
                     patch('martin_agentic.live_session.collect_live_events',
                           side_effect=forbidden), \
                     patch('martin_agentic.live_session.notify_local'), \
                     patch('martin_agentic.live_session.print'), \
                     patch('martin_agentic.live_session.wait_for_result_or_tick',
                           side_effect=lambda *a, **k: time.sleep(0.02)):
                    result = run_live_session(
                        db_path, policy,
                        symbols=['DEMO'] + [str(index) for index in range(9)],
                        cycles=12, poll_seconds=5,
                        agent_mode='supervised-host', simulation=True,
                        simulation_workspace=workspace,
                        simulation_sources=SimulationSources(
                            broker=broker, market=market,
                            review=SimulationReviewTransport, wake=wake),
                        clock=lambda: NOW, status_file=workspace / 'status.json',
                        output_dir=output, budget_entry_limit=0,
                        budget_total_limit=0)
            finally:
                player.stop.set()
                player.join(1)
            db = sqlite3.connect(db_path)
            self.addCleanup(db.close)
            action = db.execute(
                "SELECT id,state,report FROM live_actions WHERE kind='entry'"
            ).fetchone()
            self.assertIsNotNone(action)
            self.assertNotEqual(action[1], 'REPORTED')
            if action[2] is not None:
                # Shutdown may abandon the read-only reservation locally; it is
                # a fresh-review requirement, not an imported host report.
                self.assertEqual(json.loads(action[2])['status'], 'review_only')
            self.assertEqual(db.execute(
                "SELECT count(*) FROM native_operation_records"
                " WHERE state='RECORDED'").fetchone()[0], 0)
            self.assertEqual(db.execute(
                'SELECT count(*) FROM broker_executions').fetchone()[0], 0)
            results = [json.loads(path.read_text()) for path in
                       (output / 'host-execution' / 'results').glob('*.json')]
            self.assertNotIn('HOST_RECEIPT_IMPORTED',
                             {item.get('status') for item in results})
            dispatch = result.get('dispatch') or {}
            self.assertFalse(any(item.get('verified') for item in
                                 dispatch.get('receipts', [])))
            self.assertEqual([item['operation']
                              for item in dispatch.get('deliveries', [])],
                             ['review'])

    def test_strategy_action_reaches_the_host_packet_and_book_state(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory) / 'workspace'
            db_path = workspace / 'book.sqlite'
            output = workspace / 'output'
            policy = json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
            policy.update(capture_only=False, live_enabled=False,
                          strategy_instances=[{'instance_id': 'vwap-e2e',
                                               'plugin': 'vwap-fixture'}])
            broker = SimulationBroker()
            market = SimulationMarket()
            wake = SimulationWake()
            formal = {'broker': 0, 'market': 0, 'review': 0}

            def forbidden(kind):
                def call(*args, **kwargs):
                    formal[kind] += 1
                    raise AssertionError('formal ' + kind + ' surface called')
                return call

            class FormalReview:
                simulation_only = False

                def __init__(self, **kwargs):
                    formal['review'] += 1
                    raise AssertionError('formal review transport constructed')

            # The host side runs concurrently with the session: a receipt can
            # only be imported while the supervisor is still ticking.
            player = HostPlayer(output / 'host-execution', lambda: NOW)
            player.start()
            try:
                with patch('martin_agentic.live_session.single_instance'), \
                     patch('martin_agentic.live_session._broker_read',
                           side_effect=forbidden('broker')), \
                     patch('martin_agentic.live_session.collect_live_events',
                           side_effect=forbidden('market')), \
                     patch('martin_agentic.live_session.notify_local'), \
                     patch('martin_agentic.live_session.print'), \
                     patch('martin_agentic.async_native_review.EquityReviewTransport',
                           FormalReview), \
                     patch('martin_agentic.live_session.wait_for_result_or_tick',
                           side_effect=lambda *a, **k: time.sleep(0.02)):
                    result = run_live_session(
                        db_path, policy,
                        symbols=['DEMO'] + [str(index) for index in range(9)],
                        cycles=40, poll_seconds=5,
                        agent_mode='supervised-host', simulation=True,
                        simulation_workspace=workspace,
                        simulation_sources=SimulationSources(
                            broker=broker, market=market,
                            review=SimulationReviewTransport, wake=wake),
                        clock=lambda: NOW,
                        status_file=workspace / 'status.json',
                        output_dir=output, budget_entry_limit=0,
                        budget_total_limit=0)
            finally:
                player.stop.set()
                player.join(1)
            self.assertTrue(result['synthetic_session'])
            self.assertFalse(result['production_authorized'])
            self.assertGreater(broker.calls, 0)
            self.assertGreater(market.calls, 0,
                               {k: result.get(k) for k in
                                ('reason', 'failure', 'market_failure',
                                 'broker_task', 'errors', 'closing_failure')})
            self.assertEqual(formal, {'broker': 0, 'market': 0, 'review': 0})
            self.assertEqual(wake.calls, 0)
            self.assertTrue(db_path.exists())
            self.assertTrue(Path(result.get('status_file', workspace / 'status.json')).exists())
            # Every artifact this run created lives under the synthetic
            # workspace; the temporary parent holds nothing else.
            self.assertEqual(list(Path(directory).iterdir()), [workspace])

            db = sqlite3.connect(db_path)
            self.addCleanup(db.close)
            actions = db.execute(
                "SELECT id,trade_id,state,report FROM live_actions WHERE kind='entry'"
            ).fetchall()
            self.assertEqual(len(actions), 1, 'exactly one strategy entry action')
            action_id, trade_id, state, report = actions[0]
            self.assertIn(state, {'REPORTED', 'WAITING_USER', 'OBSERVED'})
            intents = db.execute(
                'SELECT intent_id,trade_id,state FROM execution_intents').fetchall()
            self.assertEqual(len(intents), 1)
            self.assertEqual(intents[0][1], trade_id)
            # A submitted/imported receipt is not a broker fill. No execution
            # row and no protected position can appear from the host response.
            self.assertEqual(db.execute(
                'SELECT count(*) FROM broker_executions').fetchone()[0], 0)
            state_doc = json.loads(db.execute(
                'SELECT payload FROM live_state WHERE id=1').fetchone()[0])
            self.assertNotIn(
                state_doc['trades'][trade_id].get('phase'), {'PROTECTED'})
            self.assertEqual(json.loads(report)['status'], 'pending_approval')

            # The default dispatcher delivers both the review and the place
            # through the HostDeliveryPort. Each request/packet/receipt/result
            # carries the same action, trade and binding identity.
            host = output / 'host-execution'
            envelopes = list(player.packets.values())
            self.assertEqual({item['packet']['operation'] for item in envelopes},
                             {'review', 'place'})
            self.assertEqual(len(envelopes), 2)
            with_receipt = dict(player.receipts)
            for envelope in envelopes:
                request_id = envelope['request_id']
                packet = envelope['packet']
                request = json.loads(
                    (host / 'requests' / (request_id + '.json')).read_text())
                imported = json.loads(
                    (host / 'results' / (request_id + '.json')).read_text())
                self.assertEqual(request['request_id'], request_id)
                self.assertEqual(request['operation'], packet['operation'])
                self.assertEqual(request['action_id'], action_id)
                self.assertEqual(request['trade_id'], trade_id)
                self.assertEqual(envelope['request_id'], request_id)
                self.assertEqual(envelope['binding'], request['binding'])
                self.assertEqual(packet['action_id'], action_id)
                self.assertEqual(packet['operation'], request['operation'])
                receipt = with_receipt[packet['operation_id']]
                self.assertEqual(receipt['request_id'], request_id)
                self.assertEqual(receipt['action_id'], action_id)
                self.assertEqual(receipt.get('trade_id'), request['trade_id'])
                self.assertEqual(receipt['operation_id'], packet['operation_id'])
                self.assertEqual(receipt['packet_sha256'],
                                 envelope['packet_sha256'])
                self.assertFalse(receipt['execution_authorized'])
                self.assertFalse(receipt['broker_effect_verified'])
                self.assertEqual(imported['status'], 'HOST_RECEIPT_IMPORTED')
                self.assertEqual(imported['request_id'], request_id)
                self.assertEqual(imported['action_id'], action_id)
                self.assertEqual(imported['operation_id'],
                                 packet['operation_id'])
                self.assertEqual(imported['trade_id'], trade_id)
                self.assertEqual(imported['packet_sha256'],
                                 envelope['packet_sha256'])
                self.assertEqual(imported['binding'], request['binding'])
                self.assertFalse(imported['broker_effect_verified'])
            dispatch = result.get('dispatch') or {}
            self.assertEqual(dispatch.get('delivery_port'), 'HostDeliveryPort')
            self.assertEqual([item['operation']
                              for item in dispatch.get('deliveries', [])],
                             ['review', 'place'], dispatch)
            self.assertTrue(all(item.get('verified') for item in
                                dispatch.get('receipts', [])), dispatch)
