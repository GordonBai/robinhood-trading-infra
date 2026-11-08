from datetime import datetime, timedelta, timezone
import json
import sqlite3
import tempfile
import threading
import time
import uuid
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch
import os
import unittest
import martin_agentic
SITE_INSTALLED = 'site-packages' in Path(martin_agentic.__file__).parts

from martin_agentic.async_host_wakeup import AsyncHostWakeDispatcher
from martin_agentic.audit import digest
from martin_agentic.host_wakeup import HostWakeOutbox, UnavailableWakeAdapter
from martin_agentic.local_worker_wake import LocalWorkerWakeAdapter


def worker_revision(action):
    return digest({'action_id': action['action_id'], 'state': action['state'],
                   'operation': 'local-worker', 'ref': action.get('ref_id'),
                   'payload_sha256': digest(action)})


class LocalWorkerWakeTests(TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.db = sqlite3.connect(self.root / 'owner.sqlite')
        self.now = datetime(2026, 10, 27, 14, tzinfo=timezone.utc)
        self.binding = {'db': str(self.root / 'owner.sqlite'),
                        'account': '••••0000', 'account_key': 'a' * 64,
                        'policy_sha256': 'b' * 64,
                        'source_fingerprint': 'c' * 64,
                        'epoch': str(uuid.uuid4())}
        self.box = HostWakeOutbox(self.db, clock=lambda: self.now)
        self.calls = []
        self.gate = threading.Event()
        self.adapter = LocalWorkerWakeAdapter(self.dispatch, self.root / 'worker')
        self.dispatcher = AsyncHostWakeDispatcher(
            self.box, self.root / 'receipts', adapter=self.adapter,
            timeout_seconds=.2)
    def tearDown(self):
        self.gate.set()
        self.dispatcher.close()
        if self.dispatcher.task.thread:
            self.dispatcher.task.thread.join(2)
        self.adapter.close(); self.db.close(); self.tmp.cleanup()
    def dispatch(self, action):
        self.calls.append(action)
        self.gate.wait(1)
        return {'status': 'review_only', 'message': 'TEST_LOCAL_WORKER',
                'order_id': None, 'market_data_disclosure': 'TEST'}
    def event(self):
        action = {'action_id': 'action-local-1', 'kind': 'protect',
                  'state': 'WAITING_USER', 'trade_id': 'trade-1',
                  'parameters': {'symbol': 'DEMO', 'quantity': '1'},
                  'ref_id': 'ref-local-1', 'synthetic': False}
        return self.box.enqueue(binding=self.binding, action_id=action['action_id'],
            kind='protect', revision='d' * 64,
            expires_at=(self.now + timedelta(seconds=30)).isoformat(),
            worker_action=action)
    def wait(self):
        end = time.monotonic() + 2
        while time.monotonic() < end:
            result = self.dispatcher.tick(binding=self.binding)
            if self.dispatcher.pending is None and result.get('state') == 'ACCEPTED':
                return result
            time.sleep(.002)
        self.fail('local worker wake did not settle')

    def test_explicit_local_worker_starts_detached_without_owner_blocking(self):
        event = self.event()
        result = self.dispatcher.tick(binding=self.binding)
        self.assertEqual(result['status'], 'WAKE_QUEUE_IO_STARTED')
        self.gate.set()
        result = self.wait()
        self.assertEqual(result['state'], 'ACCEPTED')
        self.assertFalse(result['execution_authorized'])
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.calls[0]['action_id'], 'action-local-1')
        end = time.monotonic() + 2
        while time.monotonic() < end and not list((self.root / 'worker').glob('result-*.json')):
            time.sleep(.005)
        result_files = list((self.root / 'worker').glob('result-*.json'))
        self.assertEqual(len(result_files), 1)
        self.assertEqual(json.loads(result_files[0].read_text())['worker_result']['message'],
                         'TEST_LOCAL_WORKER')
        # A repeated tick must not start a second worker for the same attempt.
        self.dispatcher.tick(binding=self.binding)
        self.assertEqual(len(self.calls), 1)

    def test_default_and_missing_dispatch_cannot_claim_execution(self):
        self.assertFalse(HostWakeOutbox.adapter_available(UnavailableWakeAdapter()))
        with self.assertRaises(ValueError):
            LocalWorkerWakeAdapter(None, self.root / 'bad')
        event = self.event()
        result = self.dispatcher.tick(binding=self.binding)
        self.assertFalse(result.get('execution_authorized', False))

    def test_local_capability_is_not_an_official_queue(self):
        capability = self.adapter.capability
        self.assertFalse(capability['official_queue'])
        self.assertTrue(capability['local_worker_event'])
        self.assertTrue(HostWakeOutbox.adapter_available(self.adapter))


    def test_real_codex_worker_class_is_called_with_fake_runner(self):
        from martin_agentic.agent_worker import CodexOrderWorker
        seen = []
        worker = CodexOrderWorker(self.root, self.root / 'agent',
            runner=lambda command, prompt: seen.append((command, prompt)) or {
                'status': 'needs_confirmation', 'message': 'FAKE_WORKER_CONFIRM',
                'order_id': None, 'market_data_disclosure': 'FAKE_DISCLOSURE'})
        adapter = LocalWorkerWakeAdapter(worker, self.root / 'worker-real')
        dispatcher = AsyncHostWakeDispatcher(self.box, self.root / 'receipts-real',
            adapter=adapter, timeout_seconds=.5)
        action = {'action_id': 'action-real-1', 'kind': 'protect',
                  'state': 'WAITING_USER', 'trade_id': 'trade-real-1',
                  'parameters': {'symbol': 'DEMO', 'quantity': '1'},
                  'ref_id': 'ref-real-1', 'synthetic': False}
        self.box.enqueue(binding=self.binding, action_id=action['action_id'],
            kind='protect', revision='e' * 64,
            expires_at=(self.now + timedelta(seconds=30)).isoformat(),
            worker_action=action)
        dispatcher.tick(binding=self.binding)
        end = time.monotonic() + 2
        while time.monotonic() < end and not list((self.root / 'worker-real').glob('result-*.json')):
            time.sleep(.005)
        dispatcher.close();adapter.close()
        result_files = list((self.root / 'worker-real').glob('result-*.json'))
        self.assertEqual(len(seen), 1)
        self.assertEqual(len(result_files), 1)
        saved = json.loads(result_files[0].read_text())
        self.assertEqual(saved['worker_result']['status'], 'needs_confirmation')
        self.assertFalse(saved['execution_authorized'])
        self.assertEqual(saved['action_id'], 'action-real-1')

    def test_simulation_cannot_inject_local_worker_wake_adapter(self):
        from martin_luk.live_session import run_live_session
        with self.assertRaisesRegex(ValueError, 'HOST_WAKE_ADAPTER_REQUIRES_REAL_SESSION'):
            run_live_session(self.root / 'sim.sqlite',
                {'session_date': '2026-10-27', 'account_suffix': '0000',
                 'capture_only': True, 'live_enabled': False},
                symbols=[str(i) for i in range(10)],
                simulation=True, host_wake_adapter=self.adapter)

    def test_two_adapters_share_one_durable_attempt_and_close_stops_worker(self):
        class Worker:
            def __init__(self):
                self.calls=[];self.stop_calls=0;self.gate=threading.Event()
            def __call__(self,action):
                self.calls.append(action);self.gate.wait(2)
                return {'status':'review_only','order_id':None}
            def stop(self):self.stop_calls+=1;self.gate.set()
        worker=Worker();a=LocalWorkerWakeAdapter(worker,self.root/'shared')
        b=LocalWorkerWakeAdapter(worker,self.root/'shared')
        action={'action_id':'action-dup','kind':'protect','state':'WAITING_USER',
                'trade_id':'trade-dup','synthetic':False}
        event={'event_id':'event-dup','binding':self.binding,
               'action_id':'action-dup','revision':'f'*64,'worker_action':action}
        args={'idempotency_key':'event-dup','attempt_id':'attempt-dup',
              'timeout_seconds':1.0,'cancellation':threading.Event()}
        first=a.enqueue(event,**args);second=b.enqueue(event,**args)
        end=time.monotonic()+1
        while not worker.calls and time.monotonic()<end:time.sleep(.002)
        a.close();b.close()
        self.assertEqual(len(worker.calls),1)
        self.assertTrue(first['accepted']);self.assertTrue(second['accepted'])
        self.assertGreaterEqual(worker.stop_calls,1)

    def test_timeout_calls_worker_stop_and_durable_claim_never_reexecutes(self):
        class Worker:
            def __init__(self):self.calls=0;self.stop_calls=0;self.gate=threading.Event()
            def __call__(self,action):self.calls+=1;self.gate.wait(2);return {'status':'review_only'}
            def stop(self):self.stop_calls+=1;self.gate.set()
        worker=Worker();directory=self.root/'timeout'
        adapter=LocalWorkerWakeAdapter(worker,directory)
        action={'action_id':'action-timeout','kind':'protect','synthetic':False}
        event={'event_id':'event-timeout','binding':self.binding,
               'action_id':'action-timeout','revision':'a'*64,'worker_action':action}
        cancellation=threading.Event()
        self.assertTrue(adapter.enqueue(event,idempotency_key='event-timeout',
            attempt_id='attempt-timeout',timeout_seconds=.05,
            cancellation=cancellation)['accepted'])
        end=time.monotonic()+1
        while worker.stop_calls==0 and time.monotonic()<end:time.sleep(.002)
        self.assertGreaterEqual(worker.stop_calls,1)
        adapter.close()
        second=LocalWorkerWakeAdapter(worker,directory)
        result=second.enqueue(event,idempotency_key='event-timeout',
            attempt_id='attempt-timeout',timeout_seconds=.1,
            cancellation=threading.Event())
        self.assertTrue(result['accepted'], result)
        time.sleep(.05)
        self.assertEqual(worker.calls,1)

    def test_public_factory_denies_capture_and_constructs_no_worker(self):
        from martin_agentic.local_worker_wake import local_worker_wake_adapter
        class Book:
            policy = {'account_suffix': '0000', 'capture_only': True}
        with self.assertRaisesRegex(ValueError, 'HOST_LOCAL_WORKER_SCOPE_DENIED'):
            local_worker_wake_adapter(Book(), output_dir=self.root / 'factory')
        class Book2:
            policy = {'account_suffix': '0000', 'capture_only': False}
        adapter = local_worker_wake_adapter(
            Book2(), output_dir=self.root / 'factory2',
            worker_factory=lambda *args, **kwargs: self.dispatch)
        self.assertIsInstance(adapter, LocalWorkerWakeAdapter)
        self.assertFalse(adapter.capability['official_queue'])

    def test_local_worker_result_consumes_once_into_book_and_rejects_stale_epoch(self):
        from types import SimpleNamespace
        from martin_agentic.host_execution import HostExecutionBridge
        class Book:
            def __init__(self): self.reports=[]
            def actions(self):
                return [{'id':'action-consume','trade_id':'trade-consume',
                         'kind':'protect','state':'RUNNING',
                         'payload':json.dumps({'ref_id':'ref-consume',
                             'parameters':{'symbol':'DEMO'},
                             'target_order_id':None})}]
            def recovery_status(self): return {'acknowledged_action_ids':[]}
            def report(self,action_id,report): self.reports.append((action_id,report))
        book=Book();binding={**self.binding,'epoch':str(uuid.uuid4())}
        adapter=LocalWorkerWakeAdapter(lambda action: {'status':'submitted',
            'message':'FAKE_SUBMIT','order_id':None,'market_data_disclosure':'X'},
            self.root/'consume')
        action={'action_id':'action-consume','kind':'protect',
                'trade_id':'trade-consume','state':'RUNNING',
                'parameters':{'symbol':'DEMO'},'target_order_id':None,
                'ref_id':'ref-consume','synthetic':False}
        event={'event_id':'event-consume','binding':binding,
               'action_id':'action-consume','revision':'b'*64,'worker_action':action}
        adapter.enqueue(event,idempotency_key='event-consume',
            attempt_id='attempt-consume',timeout_seconds=1,
            cancellation=threading.Event())
        end=time.monotonic()+2
        while time.monotonic()<end and not list((self.root/'consume').glob('result-*.json')):
            time.sleep(.005)
        fake_bridge=SimpleNamespace(book=book,wake_dispatcher=SimpleNamespace(adapter=adapter),
            binding={**binding,'epoch':'different-epoch'})
        result=HostExecutionBridge._consume_local_worker_results(fake_bridge)
        self.assertEqual(result[0]['disposition'],'STALE_EPOCH_OR_BINDING_REJECTED')
        self.assertEqual(book.reports,[])
        adapter.close()

    def test_matching_local_worker_result_reports_once_and_is_not_consumed_twice(self):
        from types import SimpleNamespace
        from martin_agentic.host_execution import HostExecutionBridge
        class Book:
            def __init__(self): self.reports=[]
            def actions(self):
                return [{'id':'action-ok','trade_id':'trade-ok',
                         'kind':'protect','state':'RUNNING',
                         'payload':json.dumps({'ref_id':'ref-ok',
                             'parameters':{'symbol':'DEMO'},
                             'target_order_id':None})}]
            def recovery_status(self): return {'acknowledged_action_ids':[]}
            def report(self,action_id,report): self.reports.append((action_id,report))
        book=Book();binding={**self.binding,'epoch':str(uuid.uuid4())}
        adapter=LocalWorkerWakeAdapter(lambda action: {'status':'submitted',
            'message':'FAKE_SUBMIT','order_id':None,'market_data_disclosure':'X'},
            self.root/'consume-ok')
        action={'action_id':'action-ok','kind':'protect',
                'trade_id':'trade-ok','state':'RUNNING',
                'parameters':{'symbol':'DEMO'},'target_order_id':None,
                'ref_id':'ref-ok','synthetic':False}
        event={'event_id':'event-ok','binding':binding,'action_id':'action-ok',
               'revision':worker_revision(action),'worker_action':action}
        adapter.enqueue(event,idempotency_key='event-ok',attempt_id='attempt-ok',
            timeout_seconds=1,cancellation=threading.Event())
        end=time.monotonic()+2
        while time.monotonic()<end and not list((self.root/'consume-ok').glob('result-*.json')):
            time.sleep(.005)
        bridge=SimpleNamespace(book=book,wake_dispatcher=SimpleNamespace(adapter=adapter),binding=binding)
        first=HostExecutionBridge._consume_local_worker_results(bridge)
        second=HostExecutionBridge._consume_local_worker_results(bridge)
        self.assertEqual(first[0]['disposition'],'BOOK_REPORT_IMPORTED')
        self.assertEqual(len(book.reports),1)
        self.assertEqual(second,[])
        adapter.close()

    def test_real_session_public_factory_call_binds(self):
        import json
        from types import SimpleNamespace
        from datetime import datetime
        from martin_luk.live_session import run_live_session
        from test_default_lifecycle import BrokerPort, BrokerState, MarketPort, TradingClock
        policy = json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        policy.update(capture_only=False, live_enabled=False)
        calls = []
        def factory(book, output_dir, policy=None):
            calls.append((book.policy.get('account_suffix'), Path(output_dir), policy))
            return LocalWorkerWakeAdapter(lambda action: {'status': 'review_only'},
                                          Path(output_dir) / 'local-worker')
        clock = TradingClock()
        state = BrokerState(clock)
        class Broker:
            def snapshot(self, approval_ids=()):
                return state.snapshot()
        ports = SimpleNamespace(broker=Broker(), market=MarketPort(clock), host=None)
        result = run_live_session(
            self.root / 'real.sqlite', policy,
            symbols=[str(i) for i in range(10)], cycles=1, poll_seconds=5,
            agent_mode='supervised-host', clock=clock,
            status_file=self.root / 'real-status.json',
            output_dir=self.root / 'real-output', ports=ports,
            host_wake_adapter_factory=factory)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0], '0000')
        self.assertTrue((self.root / 'real-output' / 'local-worker').exists())
        self.assertEqual(result['agent_mode'], 'supervised-host')

    def test_real_execution_book_action_reaches_local_worker_and_reports_once(self):
        from test_native_operations import NativeOperationTests
        from martin_agentic.host_execution import HostExecutionBridge
        fixture = NativeOperationTests(
            'test_preview_does_not_reserve_approve_or_submit')
        fixture.setUp()
        try:
            book = fixture.book
            action = next(a for a in book.actions() if a['id'] == fixture.action)
            claimed = book.claim(allow_entries=False)
            self.assertIsInstance(claimed, dict)
            action = next(a for a in book.actions() if a['id'] == fixture.action)
            self.assertEqual(action['state'], 'RUNNING')
            adapter = LocalWorkerWakeAdapter(
                lambda action: {'status': 'submitted', 'message': 'REAL_BOOK',
                                'order_id': None, 'market_data_disclosure': 'X'},
                Path(fixture.f.tmp.name) / 'local-worker-real-book')
            bridge = HostExecutionBridge(
                book, Path(fixture.f.tmp.name) / 'host-real-book',
                wake_adapter=adapter)
            self.addCleanup(bridge.close)
            payload = json.loads(action['payload'])
            worker_action = {**payload, 'action_id': action['id'],
                             'kind': action['kind'],
                             'state': action['state'],
                             'trade_id': action['trade_id'],
                             'synthetic': False}
            event = {'event_id': 'event-real-book', 'action_id': action['id'],
                     'binding': bridge.binding,
                     'revision': worker_revision(worker_action),
                     'worker_action': worker_action}
            adapter.enqueue(event, idempotency_key='event-real-book',
                            attempt_id='attempt-real-book', timeout_seconds=2,
                            cancellation=threading.Event())
            end = time.monotonic() + 2
            while time.monotonic() < end and not list(
                    (Path(fixture.f.tmp.name) / 'local-worker-real-book').glob('result-*.json')):
                time.sleep(.005)
            out = bridge(snapshot=fixture.snapshot(), quotes={},
                         allow_entries=False, allow_dispatch=False)
            self.assertEqual(out['local_worker_results'][0]['disposition'],
                             'BOOK_REPORT_IMPORTED')
            self.assertEqual(next(a for a in book.actions()
                                  if a['id'] == fixture.action)['state'],
                             'REPORTED')
            adapter.close()
        finally:
            fixture.tearDown()

    def test_public_local_worker_dispatcher_claim_event_worker_and_book_report(self):
        from test_native_operations import NativeOperationTests
        from martin_agentic.host_execution import HostExecutionBridge
        from martin_luk.live_session import LocalWorkerDispatcher
        fixture = NativeOperationTests(
            'test_preview_does_not_reserve_approve_or_submit')
        fixture.setUp()
        try:
            adapter = LocalWorkerWakeAdapter(
                lambda action: {'status': 'submitted', 'message': 'PUBLIC_DISPATCH',
                                'order_id': None, 'market_data_disclosure': 'X'},
                Path(fixture.f.tmp.name) / 'public-local')
            bridge = HostExecutionBridge(
                fixture.book, Path(fixture.f.tmp.name) / 'public-host',
                wake_adapter=adapter)
            dispatcher = LocalWorkerDispatcher(fixture.book, bridge)
            self.addCleanup(dispatcher.close)
            out = dispatcher(snapshot=fixture.snapshot(), quotes={},
                             allow_entries=False, allow_dispatch=True)
            self.assertTrue(out['agent_called'])
            self.assertEqual(out['local_worker_wake']['status'],
                             'WAKE_QUEUE_IO_STARTED')
            action_id = out['action_id']
            self.assertEqual(next(a for a in fixture.book.actions()
                                  if a['id'] == action_id)['state'], 'RUNNING')
            end = time.monotonic() + 2
            while time.monotonic() < end and not list(
                    (Path(fixture.f.tmp.name) / 'public-local').glob('result-*.json')):
                time.sleep(.005)
            consumed = dispatcher(snapshot=fixture.snapshot(), quotes={},
                                  allow_entries=False, allow_dispatch=False)
            self.assertEqual(consumed['local_worker_results'][0]['disposition'],
                             'BOOK_REPORT_IMPORTED')
            self.assertEqual(next(a for a in fixture.book.actions()
                                  if a['id'] == action_id)['state'], 'REPORTED')
            adapter.close()
        finally:
            fixture.tearDown()

    def test_public_run_live_session_recovers_action_and_consumes_real_worker_result(self):
        """The whole public seam: session recovery -> claim -> outbox -> worker
        -> owner result consumption -> real ExecutionBook report.

        A real-mode ledger is seeded through public proposal/review and
        reconcile paths (no claimed entry, no hand-set RUNNING, no mocked
        claim/report), then the preparing connection is closed. The public
        session reopens the same database and must recover the existing READY
        protect action and consume the worker result into its own Book.
        """
        from types import SimpleNamespace
        from martin_agentic.agent_worker import CodexOrderWorker
        from martin_luk.live_session import run_live_session
        from martin_agentic.local_worker_wake import local_worker_wake_adapter
        from test_broker import FakeBroker, order, position, snapshot
        from test_default_lifecycle import MarketPort, TradingClock
        from test_engineering_source import RealDryFixture

        fixture = RealDryFixture(lineage=True, prebuilt_intent=True,
                                 activate=True)
        try:
            entry = fixture.book.actions()[0]
            payload = json.loads(entry['payload'])
            raw = order('o1', 'buy', str(payload['parameters']['quantity']),
                        '2', 'filled')
            raw['ref_id'] = payload['ref_id']
            raw['price'] = payload['parameters'].get('limit_price')
            raw['type'] = 'limit'
            raw['market_hours'] = payload['parameters']['market_hours']
            raw['time_in_force'] = payload['parameters']['time_in_force']
            broker = FakeBroker()
            broker.orders = [raw]
            broker.positions = [position('2', '0')]
            broker.cash = '420'
            seeded = snapshot(broker)
            seeded['synthetic'] = False
            fixture.book.reconcile(seeded, quotes={'DEMO': fixture._quote()})
            protect = next(a for a in fixture.book.actions()
                           if a['kind'] == 'protect')
            self.assertEqual(protect['state'], 'READY')
            fixture.book.close()

            clock = TradingClock(start=fixture._clock_value[0])
            market = MarketPort(clock)
            observed = []

            class SyntheticBroker:
                simulation_only = True

                def snapshot(self, approval_ids=()):
                    value = snapshot(broker, clock().isoformat())
                    value['synthetic'] = False
                    observed.append(value['observed_at'])
                    return value

            class SyntheticMarket:
                simulation_only = True

                def collect_events(self, *args, **kwargs):
                    return market(*args, **kwargs)

            calls = []

            def runner(command, prompt):
                calls.append((command, prompt))
                return {'status': 'submitted',
                        'message': 'PUBLIC_SESSION_WORKER',
                        'order_id': None,
                        'market_data_disclosure': 'SYNTHETIC'}

            def worker_factory(cwd, directory, review_only=True):
                return CodexOrderWorker(cwd, directory,
                                        review_only=review_only, runner=runner)

            factory_calls = []

            def factory(book, *, output_dir, policy=None):
                factory_calls.append((book, output_dir, policy))
                return local_worker_wake_adapter(
                    book, output_dir=output_dir, policy=policy,
                    worker_factory=worker_factory)

            ports = SimpleNamespace(broker=SyntheticBroker(),
                                    market=SyntheticMarket(), host=None)
            output = fixture.root / 'public-session'
            with patch('martin_agentic.live_session.wait_for_result_or_tick',
                       side_effect=lambda *a, **k: time.sleep(.02)), \
                 patch('martin_agentic.live_session.notify_local'), \
                 patch('martin_agentic.live_session.print'):
                result = run_live_session(
                    fixture.root / 'book.sqlite', fixture.policy,
                    symbols=['DEMO'] + [str(i) for i in range(9)],
                    cycles=30, poll_seconds=5,
                    agent_mode='supervised-host', clock=clock,
                    status_file=fixture.root / 'public-status.json',
                    output_dir=output, ports=ports,
                    host_wake_adapter_factory=factory,
                    budget_entry_limit=1, budget_total_limit=1)

            db = sqlite3.connect(fixture.root / 'book.sqlite')
            try:
                state, report = db.execute(
                    'SELECT state,report FROM live_actions WHERE id=?',
                    (protect['id'],)).fetchone()
                wake = db.execute(
                    'SELECT state FROM host_wake_events').fetchall()
            finally:
                db.close()
            worker_dir = output / 'local-worker'
            self.assertEqual(len(factory_calls), 1)
            self.assertEqual(len(calls), 1)
            self.assertFalse(result['synthetic_session'])
            self.assertFalse(result['production_authorized'])
            self.assertEqual(result['source_mode'], 'real')
            self.assertEqual(state, 'REPORTED')
            self.assertEqual(json.loads(report)['status'], 'submitted')
            self.assertEqual(wake, [('ACCEPTED',)])
            self.assertEqual(len(list(worker_dir.glob('claim-*.json'))), 1)
            self.assertEqual(json.loads(next(
                worker_dir.glob('claim-*.json')).read_text())['state'],
                'RESULT_RECORDED')
            self.assertEqual(len(list(worker_dir.glob('consumed-*.json'))), 1)
            self.assertFalse(list(worker_dir.glob('result-*.json')))
            self.assertTrue(observed)
            self.assertEqual(result['agent_mode'], 'supervised-host')
            self.assertEqual(result['worker_adapter']['selection'],
                             'local-worker-review')
            self.assertTrue(result['worker_adapter']['local_worker_event'])
            self.assertFalse(result['worker_adapter']['official_queue'])
            self.assertFalse(result['worker_adapter']['formal_adapter_qualified'])
            manifest = json.loads(
                (output / 'host-execution' / 'manifest.json').read_text())
            self.assertEqual(manifest['worker_adapter']['selection'],
                             'local-worker-review')
            self.assertFalse(manifest['worker_adapter']['official_queue'])
            self.assertTrue(manifest['worker_adapter']['requires_native_confirmation'])
        finally:
            fixture.close()

    def test_concurrent_adapters_share_one_claim_and_only_one_worker_runs(self):
        class Worker:
            def __init__(self):
                self.calls = 0
                self.gate = threading.Event()
                self.stop_calls = 0

            def __call__(self, action):
                self.calls += 1
                self.gate.wait(2)
                return {'status': 'submitted', 'message': 'CONCURRENT'}

            def stop(self):
                self.stop_calls += 1
                self.gate.set()

        worker = Worker()
        directory = self.root / 'concurrent-barrier'
        first = LocalWorkerWakeAdapter(worker, directory)
        second = LocalWorkerWakeAdapter(worker, directory)
        action = {'action_id': 'action-barrier', 'kind': 'protect',
                  'trade_id': 'trade-barrier', 'state': 'RUNNING',
                  'parameters': {'symbol': 'DEMO'},
                  'target_order_id': None, 'synthetic': False}
        event = {'event_id': 'event-barrier', 'binding': self.binding,
                 'action_id': 'action-barrier', 'revision': 'b' * 64,
                 'worker_action': action}
        barrier = threading.Barrier(2)
        outcomes = []

        def enqueue(adapter):
            barrier.wait()
            outcomes.append(adapter.enqueue(
                event, idempotency_key='event-barrier',
                attempt_id='attempt-barrier', timeout_seconds=1,
                cancellation=threading.Event()))

        threads = [threading.Thread(target=enqueue, args=(adapter,))
                   for adapter in (first, second)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(1)
        end = time.monotonic() + 1
        while worker.calls == 0 and time.monotonic() < end:
            time.sleep(.005)
        first.close()
        second.close()
        worker.gate.set()
        self.assertEqual(len(outcomes), 2)
        self.assertTrue(all(item['accepted'] for item in outcomes), outcomes)
        self.assertEqual(worker.calls, 1)


    def test_different_attempts_cannot_overlap_one_adapter_worker(self):
        class Worker:
            def __init__(self):
                self.calls = 0
                self.gate = threading.Event()

            def __call__(self, action):
                self.calls += 1
                self.gate.wait(2)
                return {'status': 'submitted', 'message': 'ONE_AT_A_TIME'}

            def stop(self):
                self.gate.set()

        worker = Worker()
        adapter = LocalWorkerWakeAdapter(worker, self.root / 'one-active')
        barrier = threading.Barrier(2)
        outcomes = []

        def action(identity):
            return {'action_id': identity, 'kind': 'protect',
                    'trade_id': 'trade-' + identity, 'state': 'RUNNING',
                    'parameters': {}, 'target_order_id': None,
                    'ref_id': 'ref-' + identity, 'synthetic': False}

        def enqueue(identity):
            value = action(identity)
            event = {'event_id': 'event-' + identity, 'binding': self.binding,
                     'action_id': identity,
                     'revision': worker_revision(value),
                     'worker_action': value}
            barrier.wait()
            outcomes.append(adapter.enqueue(
                event, idempotency_key='event-' + identity,
                attempt_id='attempt-' + identity, timeout_seconds=1,
                cancellation=threading.Event()))

        threads = [threading.Thread(target=enqueue, args=(name,))
                   for name in ('a', 'b')]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(1)
        end = time.monotonic() + 1
        while worker.calls == 0 and time.monotonic() < end:
            time.sleep(.005)
        adapter.close()
        worker.gate.set()
        self.assertEqual(worker.calls, 1)
        self.assertEqual(sum(item['accepted'] for item in outcomes), 1)
        busy = [item for item in outcomes if not item['accepted']]
        self.assertEqual(len(busy), 1)
        self.assertEqual(busy[0]['reason'], 'LOCAL_WORKER_BUSY')
        self.assertTrue(busy[0]['retryable'])

    def test_retryable_book_report_error_requeues_exact_result_without_replay(self):
        from types import SimpleNamespace
        from martin_agentic.host_execution import HostExecutionBridge
        calls = []
        reports = []

        class Book:
            def __init__(self):
                self.fail = True

            def actions(self):
                return [{'id': 'action-retry', 'trade_id': 'trade-retry',
                         'kind': 'protect', 'state': 'RUNNING',
                         'payload': json.dumps({'ref_id': 'ref-retry',
                             'parameters': {'symbol': 'DEMO'},
                             'target_order_id': None})}]

            def recovery_status(self):
                return {'acknowledged_action_ids': []}

            def report(self, action_id, report):
                reports.append((action_id, report))
                if self.fail:
                    self.fail = False
                    raise sqlite3.OperationalError('owner locked')

        book = Book()
        directory = self.root / 'retry-report'
        adapter = LocalWorkerWakeAdapter(
            lambda action: calls.append(action) or {
                'status': 'submitted', 'message': 'RETRY_ONCE',
                'order_id': None, 'market_data_disclosure': 'X'}, directory)
        action = {'action_id': 'action-retry', 'kind': 'protect',
                  'trade_id': 'trade-retry', 'state': 'RUNNING',
                  'parameters': {'symbol': 'DEMO'},
                  'target_order_id': None, 'ref_id': 'ref-retry',
                  'synthetic': False}
        event = {'event_id': 'event-retry', 'binding': self.binding,
                 'action_id': 'action-retry',
                 'revision': worker_revision(action),
                 'worker_action': action}
        adapter.enqueue(event, idempotency_key='event-retry',
                        attempt_id='attempt-retry', timeout_seconds=1,
                        cancellation=threading.Event())
        end = time.monotonic() + 1
        while time.monotonic() < end and not list(directory.glob('result-*.json')):
            time.sleep(.005)
        bridge = SimpleNamespace(book=book,
            wake_dispatcher=SimpleNamespace(adapter=adapter),
            binding=dict(self.binding))
        first = HostExecutionBridge._consume_local_worker_results(bridge)
        self.assertEqual(first[0]['disposition'], 'BOOK_REPORT_RETRY_DEFERRED')
        self.assertTrue(list(directory.glob('result-*.json')))
        second = HostExecutionBridge._consume_local_worker_results(bridge)
        self.assertEqual(second[0]['disposition'], 'BOOK_REPORT_IMPORTED')
        self.assertEqual(len(calls), 1, 'retry must not execute the worker again')
        self.assertEqual(len(reports), 2)
        self.assertEqual(reports[-1][1]['status'], 'submitted')
        self.assertEqual(len(list(directory.glob('consumed-*.json'))), 1)
        adapter.close()


    def test_processing_result_is_not_read_twice_and_close_reports_it(self):
        adapter = LocalWorkerWakeAdapter(
            lambda action: {'status': 'submitted', 'message': 'ONCE'},
            self.root / 'processing-owner')
        action = {'action_id': 'action-processing', 'kind': 'protect',
                  'trade_id': 'trade-processing', 'state': 'RUNNING',
                  'parameters': {}, 'target_order_id': None,
                  'synthetic': False}
        event = {'event_id': 'event-processing', 'binding': self.binding,
                 'action_id': 'action-processing', 'revision': 'd' * 64,
                 'worker_action': action}
        adapter.enqueue(event, idempotency_key='event-processing',
                        attempt_id='attempt-processing', timeout_seconds=1,
                        cancellation=threading.Event())
        end = time.monotonic() + 1
        while time.monotonic() < end and not list(
                (self.root / 'processing-owner').glob('result-*.json')):
            time.sleep(.005)
        first = adapter.take_result()
        self.assertIsNotNone(first)
        self.assertIsNone(adapter.take_result())
        status = adapter.close()
        self.assertEqual(status['processing_results'], [Path(first[0]).name])


    def test_result_page_is_bounded(self):
        adapter = LocalWorkerWakeAdapter(lambda action: {}, self.root / 'page')
        directory = self.root / 'page'
        for index in range(40):
            (directory / ('result-' + str(index).zfill(64) + '.json')).write_text(
                json.dumps({'index': index}))
        first = adapter.take_results()
        second = adapter.take_results()
        self.assertEqual(len(first), LocalWorkerWakeAdapter.MAX_RESULTS_PER_TAKE)
        self.assertEqual(len(second), 40 - LocalWorkerWakeAdapter.MAX_RESULTS_PER_TAKE)


    def test_hung_worker_stop_is_bounded_and_reported_at_close(self):
        class Worker:
            def __init__(self):
                self.release = threading.Event()

            def __call__(self, action):
                return {'status': 'submitted', 'message': 'FAST'}

            def stop(self):
                self.release.wait(5)

        worker = Worker()
        adapter = LocalWorkerWakeAdapter(worker, self.root / 'hung-stop')
        started = time.monotonic()
        status = adapter.close()
        elapsed = time.monotonic() - started
        self.assertLess(elapsed, 3.0)
        self.assertTrue(status['worker_stop_retained'])
        self.assertIn('WORKER_STOP', status['threads_alive'])
        worker.release.set()

    def test_current_action_revision_mismatch_is_rejected_before_report(self):
        from types import SimpleNamespace
        from martin_agentic.host_execution import HostExecutionBridge
        reports = []

        class Book:
            def actions(self):
                return [{'id': 'action-revision', 'trade_id': 'trade-revision',
                         'kind': 'protect', 'state': 'RUNNING',
                         'payload': json.dumps({'ref_id': 'ref-revision',
                             'parameters': {'symbol': 'CHANGED'},
                             'target_order_id': None})}]

            def recovery_status(self):
                return {'acknowledged_action_ids': []}

            def report(self, action_id, report):
                reports.append((action_id, report))

        adapter = LocalWorkerWakeAdapter(
            lambda action: {'status': 'submitted', 'message': 'STALE'},
            self.root / 'revision-mismatch')
        action = {'action_id': 'action-revision', 'kind': 'protect',
                  'trade_id': 'trade-revision', 'state': 'RUNNING',
                  'parameters': {'symbol': 'DEMO'},
                  'target_order_id': None, 'ref_id': 'ref-revision',
                  'synthetic': False}
        event = {'event_id': 'event-revision', 'binding': self.binding,
                 'action_id': 'action-revision',
                 'revision': worker_revision(action),
                 'worker_action': action}
        adapter.enqueue(event, idempotency_key='event-revision',
                        attempt_id='attempt-revision', timeout_seconds=1,
                        cancellation=threading.Event())
        end = time.monotonic() + 1
        while time.monotonic() < end and not list(
                (self.root / 'revision-mismatch').glob('result-*.json')):
            time.sleep(.005)
        bridge = SimpleNamespace(
            book=Book(), wake_dispatcher=SimpleNamespace(adapter=adapter),
            binding=dict(self.binding))
        result = HostExecutionBridge._consume_local_worker_results(bridge)
        self.assertEqual(result[0]['disposition'],
                         'WORKER_ACTION_REF_REVISION_MISMATCH')
        self.assertEqual(reports, [])
        adapter.close()

    def test_consumer_stops_at_adapter_result_limit(self):
        from types import SimpleNamespace
        from martin_agentic.host_execution import HostExecutionBridge

        class Adapter:
            MAX_RESULTS_PER_TAKE = 3

            def __init__(self):
                self.calls = 0
                self.finished = []

            def take_result(self):
                self.calls += 1
                return (self.calls, {'late_after_timeout': True,
                                     'action_id': 'action-limit'})

            def finish_result(self, path, disposition):
                self.finished.append((path, disposition))

        adapter = Adapter()
        bridge = SimpleNamespace(
            book=object(), wake_dispatcher=SimpleNamespace(adapter=adapter),
            binding={})
        result = HostExecutionBridge._consume_local_worker_results(bridge)
        self.assertEqual(adapter.calls, 3)
        self.assertEqual(len(result), 3)
        self.assertTrue(all(item['disposition'] == 'LATE_WORKER_RESULT_REJECTED'
                            for item in result))

    def _durable_result_for_restart(self, name):
        action = {'action_id': 'action-' + name, 'kind': 'protect',
                  'trade_id': 'trade-' + name, 'state': 'RUNNING',
                  'parameters': {'symbol': 'DEMO'},
                  'target_order_id': None, 'ref_id': 'ref-' + name,
                  'synthetic': False}
        directory = self.root / name
        adapter = LocalWorkerWakeAdapter(
            lambda value: {'status': 'submitted', 'message': 'RESTART'},
            directory)
        event = {'event_id': 'event-' + name, 'binding': self.binding,
                 'action_id': action['action_id'],
                 'revision': worker_revision(action),
                 'worker_action': action}
        adapter.enqueue(event, idempotency_key='event-' + name,
                        attempt_id='attempt-' + name, timeout_seconds=1,
                        cancellation=threading.Event())
        end = time.monotonic() + 1
        while time.monotonic() < end and not list(directory.glob('result-*.json')):
            time.sleep(.005)
        return action, directory, adapter


    def test_durable_result_survives_restart_and_consumes_once(self):
        from types import SimpleNamespace
        from martin_agentic.host_execution import HostExecutionBridge
        action, directory, first = self._durable_result_for_restart('restart')
        first.close()
        reports = []

        class Book:
            def actions(self):
                return [{'id': action['action_id'],
                         'trade_id': action['trade_id'],
                         'kind': action['kind'], 'state': 'RUNNING',
                         'payload': json.dumps({'ref_id': action['ref_id'],
                             'parameters': action['parameters'],
                             'target_order_id': None})}]

            def recovery_status(self):
                return {'acknowledged_action_ids': []}

            def report(self, action_id, report):
                reports.append((action_id, report))

        restarted = LocalWorkerWakeAdapter(
            lambda value: self.fail('restart must not run the worker'),
            directory)
        bridge = SimpleNamespace(
            book=Book(), wake_dispatcher=SimpleNamespace(adapter=restarted),
            binding=dict(self.binding))
        consumed = HostExecutionBridge._consume_local_worker_results(bridge)
        again = HostExecutionBridge._consume_local_worker_results(bridge)
        self.assertEqual(consumed[0]['disposition'], 'BOOK_REPORT_IMPORTED')
        self.assertEqual(again, [])
        self.assertEqual(len(reports), 1)
        restarted.close()


    def test_finish_failure_after_report_is_not_double_applied_on_restart(self):
        from types import SimpleNamespace
        from martin_agentic.host_execution import HostExecutionBridge
        action, directory, first = self._durable_result_for_restart('finish-fail')
        reports = []

        class Book:
            def __init__(self):
                self.state = 'RUNNING'
                self.stored = None

            def actions(self):
                return [{'id': action['action_id'],
                         'trade_id': action['trade_id'],
                         'kind': action['kind'], 'state': self.state,
                         'report': json.dumps(self.stored)
                                   if self.stored is not None else None,
                         'payload': json.dumps({'ref_id': action['ref_id'],
                             'parameters': action['parameters'],
                             'target_order_id': None})}]

            def recovery_status(self):
                return {'acknowledged_action_ids': []}

            def report(self, action_id, report):
                if self.state != 'RUNNING':
                    raise ValueError('ACTION_NOT_RUNNING')
                reports.append((action_id, report))
                self.state = 'REPORTED'
                self.stored = report

        original = first.finish_result
        first.finish_result = lambda path, disposition: None
        bridge = SimpleNamespace(
            book=Book(), wake_dispatcher=SimpleNamespace(adapter=first),
            binding=dict(self.binding))
        consumed = HostExecutionBridge._consume_local_worker_results(bridge)
        self.assertEqual(consumed[0]['disposition'], 'BOOK_REPORT_IMPORTED')
        self.assertEqual(len(reports), 1)
        self.assertEqual(len(first.processing_results()), 1)
        first.finish_result = original
        first.close()
        restarted = LocalWorkerWakeAdapter(lambda value: None, directory)
        second = SimpleNamespace(
            book=bridge.book,
            wake_dispatcher=SimpleNamespace(adapter=restarted),
            binding=dict(self.binding))
        recovered = HostExecutionBridge._consume_local_worker_results(second)
        self.assertEqual(recovered[0]['disposition'],
                         'ALREADY_APPLIED_RECOVERY_FINALIZED')
        self.assertEqual(len(reports), 1)
        self.assertEqual(restarted.processing_results(), [])
        restarted.close()

    def test_cancel_while_result_write_blocked_cannot_regress_unknown(self):
        class Worker:
            def __call__(self, action):
                return {'status': 'submitted', 'message': 'CONTROLLED'}

            def stop(self):
                pass

        adapter = LocalWorkerWakeAdapter(Worker(), self.root / 'cancel-window')
        result_write_started = threading.Event()
        release_write = threading.Event()
        original = adapter._private_json

        def gated(path, value):
            if Path(path).name.startswith('result-'):
                result_write_started.set()
                if not release_write.wait(2):
                    raise RuntimeError('release timeout')
            return original(path, value)

        adapter._private_json = gated
        cancellation = threading.Event()
        action = {'action_id': 'action-cancel-window', 'kind': 'protect',
                  'trade_id': 'trade-cancel-window', 'state': 'RUNNING',
                  'parameters': {}, 'target_order_id': None,
                  'ref_id': 'ref-cancel-window', 'synthetic': False}
        event = {'event_id': 'event-cancel-window',
                 'binding': self.binding,
                 'action_id': action['action_id'],
                 'revision': worker_revision(action),
                 'worker_action': action}
        adapter.enqueue(event, idempotency_key='event-cancel-window',
                        attempt_id='attempt-cancel-window',
                        timeout_seconds=2, cancellation=cancellation)
        self.assertTrue(result_write_started.wait(2))
        cancellation.set()
        end = time.monotonic() + 2
        while (time.monotonic() < end and
               (adapter.claim_state('event-cancel-window',
                                    'attempt-cancel-window') or {}
                ).get('state') != 'UNKNOWN'):
            time.sleep(.002)
        release_write.set()
        end = time.monotonic() + 2
        while time.monotonic() < end and adapter._active:
            time.sleep(.002)
        claim = adapter.claim_state('event-cancel-window',
                                    'attempt-cancel-window')
        result = json.loads(next(
            (self.root / 'cancel-window').glob('result-*.json')).read_text())
        self.assertEqual(claim['state'], 'UNKNOWN')
        self.assertTrue(result['late_after_timeout'])
        previous, changed = adapter._transition_claim(
            'event-cancel-window', 'attempt-cancel-window',
            {'RUNNING'}, 'RESULT_RECORDED')
        self.assertEqual(previous, 'UNKNOWN')
        self.assertFalse(changed)
        self.assertEqual(adapter.claim_state(
            'event-cancel-window', 'attempt-cancel-window')['state'],
            'UNKNOWN')
        adapter.close()

    def test_cancel_before_dispatch_cas_never_calls_worker(self):
        class Worker:
            def __init__(self):
                self.calls = 0

            def __call__(self, action):
                self.calls += 1
                return {'status': 'submitted', 'message': 'MUST_NOT_RUN'}

            def stop(self):
                pass

        worker = Worker()
        adapter = LocalWorkerWakeAdapter(worker, self.root / 'pre-dispatch')
        original = adapter._transition_claim
        entered = threading.Event()
        release = threading.Event()

        def gated(key, attempt_id, allowed, state, **fields):
            if state == 'RUNNING':
                entered.set()
                release.wait(2)
            return original(key, attempt_id, allowed, state, **fields)

        adapter._transition_claim = gated
        action = {'action_id': 'action-pre-dispatch', 'kind': 'protect',
                  'trade_id': 'trade-pre-dispatch', 'state': 'RUNNING',
                  'parameters': {}, 'target_order_id': None,
                  'ref_id': 'ref-pre-dispatch', 'synthetic': False}
        event = {'event_id': 'event-pre-dispatch',
                 'binding': self.binding,
                 'action_id': action['action_id'],
                 'revision': worker_revision(action),
                 'worker_action': action}
        adapter.enqueue(event, idempotency_key='event-pre-dispatch',
                        attempt_id='attempt-pre-dispatch',
                        timeout_seconds=2, cancellation=threading.Event())
        self.assertTrue(entered.wait(2))
        previous, changed = original(
            'event-pre-dispatch', 'attempt-pre-dispatch',
            {'CLAIMED', 'RUNNING'}, 'UNKNOWN',
            reason='TEST_CANCEL_BEFORE_DISPATCH')
        self.assertTrue(changed)
        release.set()
        end = time.monotonic() + 2
        while time.monotonic() < end and adapter._active:
            time.sleep(.002)
        self.assertEqual(worker.calls, 0)
        self.assertEqual(adapter.claim_state(
            'event-pre-dispatch', 'attempt-pre-dispatch')['state'],
            'UNKNOWN')
        result = json.loads(next(
            (self.root / 'pre-dispatch').glob('result-*.json')).read_text())
        self.assertTrue(result['late_after_timeout'])
        adapter.close()

    def _real_book_bridge(self, status='submitted', *, message='REAL_BOOK'):
        from test_native_operations import NativeOperationTests
        from martin_agentic.host_execution import HostExecutionBridge
        fixture = NativeOperationTests(
            'test_preview_does_not_reserve_approve_or_submit')
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        book = fixture.book
        claimed = book.claim(allow_entries=False)
        self.assertIsNotNone(claimed)
        action_id = claimed['action_id']
        directory = Path(fixture.f.tmp.name) / 'local-worker-real'
        adapter = LocalWorkerWakeAdapter(
            lambda action: {'status': status, 'message': message,
                            'order_id': None,
                            'market_data_disclosure': 'REAL_BOOK'},
            directory)
        bridge = HostExecutionBridge(
            book, Path(fixture.f.tmp.name) / 'host-real', wake_adapter=adapter)
        self.addCleanup(bridge.close)
        return fixture, book, action_id, adapter, bridge, directory

    @staticmethod
    def _wait_result(directory):
        end = time.monotonic() + 2
        while time.monotonic() < end and not list(Path(directory).glob(
                'result-*.json')):
            time.sleep(.005)
        result = list(Path(directory).glob('result-*.json'))
        if not result:
            raise AssertionError('worker result absent')
        return result[0]

    def test_real_book_persists_review_only_waiting_state_once(self):
        fixture, book, action_id, adapter, bridge, directory = self._real_book_bridge(
            'review_only', message='FRESH_PREVIEW_REVIEW_ONLY')
        current = next(a for a in book.actions() if a['id'] == action_id)
        bridge.enqueue_local_worker_action(current)
        self._wait_result(directory)
        consumed = bridge.consume_local_worker_results()
        self.assertEqual(consumed[0]['disposition'],
                         'WAITING_CONFIRMATION_RECORDED')
        action = next(a for a in book.actions() if a['id'] == action_id)
        self.assertEqual(action['state'], 'WAITING_USER')
        self.assertEqual(json.loads(action['report'])['status'], 'review_only')
        self.assertEqual(json.loads(action['report'])['message'],
                         'FRESH_PREVIEW_REVIEW_ONLY')
        self.assertIsNone(book.claim())
        self.assertEqual(bridge.consume_local_worker_results(), [])

    def test_real_book_persists_unknown_state_once(self):
        fixture, book, action_id, adapter, bridge, directory = self._real_book_bridge(
            'unknown', message='OUTCOME_UNKNOWN')
        current = next(a for a in book.actions() if a['id'] == action_id)
        bridge.enqueue_local_worker_action(current)
        self._wait_result(directory)
        consumed = bridge.consume_local_worker_results()
        self.assertEqual(consumed[0]['disposition'],
                         'UNKNOWN_REQUIRES_RECONCILIATION')
        action = next(a for a in book.actions() if a['id'] == action_id)
        self.assertEqual(action['state'], 'UNKNOWN')
        self.assertEqual(json.loads(action['report'])['status'], 'unknown')
        self.assertIsNone(book.claim())
        self.assertEqual(bridge.consume_local_worker_results(), [])

    def test_real_book_recovers_processing_before_report_after_restart(self):
        fixture, book, action_id, adapter, bridge, directory = self._real_book_bridge(
            'submitted', message='PRE_REPORT_CRASH')
        current = next(a for a in book.actions() if a['id'] == action_id)
        bridge.enqueue_local_worker_action(current)
        self._wait_result(directory)
        taken = adapter.take_result()
        self.assertIsNotNone(taken)
        adapter.close()
        restarted = LocalWorkerWakeAdapter(
            lambda action: self.fail('restart must not execute worker'),
            directory)
        bridge.wake_dispatcher.adapter = restarted
        consumed = bridge.consume_local_worker_results()
        self.assertEqual(consumed[0]['disposition'], 'BOOK_REPORT_IMPORTED')
        action = next(a for a in book.actions() if a['id'] == action_id)
        self.assertEqual(action['state'], 'REPORTED')
        self.assertEqual(json.loads(action['report'])['message'],
                         'PRE_REPORT_CRASH')
        self.assertEqual(restarted.processing_results(), [])

    def test_real_book_finish_failure_after_report_is_finalized_not_reapplied(self):
        fixture, book, action_id, adapter, bridge, directory = self._real_book_bridge(
            'submitted', message='POST_REPORT_CRASH')
        current = next(a for a in book.actions() if a['id'] == action_id)
        bridge.enqueue_local_worker_action(current)
        self._wait_result(directory)
        taken = adapter.take_result()
        self.assertIsNotNone(taken)
        book.report(action_id, {'status': 'submitted',
                                'message': 'POST_REPORT_CRASH',
                                'order_id': None,
                                'market_data_disclosure': 'REAL_BOOK'})
        adapter.close()
        event_id = action_id + '-report'
        self.assertEqual(book.db.execute(
            'SELECT count(*) FROM audit_events WHERE event_id=?',
            (event_id,)).fetchone()[0], 1)
        restarted = LocalWorkerWakeAdapter(
            lambda action: self.fail('restart must not execute worker'),
            directory)
        bridge.wake_dispatcher.adapter = restarted
        recovered = bridge.consume_local_worker_results()
        self.assertEqual(recovered[0]['disposition'],
                         'ALREADY_APPLIED_RECOVERY_FINALIZED')
        self.assertEqual(book.db.execute(
            'SELECT count(*) FROM audit_events WHERE event_id=?',
            (event_id,)).fetchone()[0], 1)
        self.assertEqual(next(a for a in book.actions()
                              if a['id'] == action_id)['state'], 'REPORTED')
        self.assertEqual(restarted.processing_results(), [])

    def test_real_book_requeues_after_sqlite_busy_then_applies_once(self):
        fixture, book, action_id, adapter, bridge, directory = self._real_book_bridge(
            'submitted', message='SQLITE_BUSY_RETRY')
        current = next(a for a in book.actions() if a['id'] == action_id)
        bridge.enqueue_local_worker_action(current)
        self._wait_result(directory)
        path = book.db.execute('PRAGMA database_list').fetchone()[2]
        book.db.execute('PRAGMA busy_timeout=50')
        blocker = sqlite3.connect(path, isolation_level=None, timeout=1)
        try:
            blocker.execute('BEGIN IMMEDIATE')
            first = bridge.consume_local_worker_results()
            self.assertEqual(first[0]['disposition'],
                             'BOOK_REPORT_RETRY_DEFERRED')
            self.assertEqual(next(a for a in book.actions()
                                  if a['id'] == action_id)['state'], 'RUNNING')
            self.assertTrue(list(Path(directory).glob('result-*.json')))
            blocker.execute('ROLLBACK')
        finally:
            blocker.close()
        second = bridge.consume_local_worker_results()
        self.assertEqual(second[0]['disposition'], 'BOOK_REPORT_IMPORTED')
        self.assertEqual(next(a for a in book.actions()
                              if a['id'] == action_id)['state'], 'REPORTED')

    def test_stop_exception_is_not_reported_as_clean_stopped(self):
        class Worker:
            def __call__(self, action):
                return {'status': 'submitted', 'message': 'STOP_ERROR'}

            def stop(self):
                raise RuntimeError('STOP_BROKEN')

        adapter = LocalWorkerWakeAdapter(Worker(), self.root / 'stop-error')
        status = adapter.close()
        self.assertFalse(status['worker_stop_ok'])
        self.assertIn('STOP_BROKEN', status['worker_stop_error'])
        self.assertEqual(status['status'],
                         'LOCAL_WORKER_WAKE_ADAPTER_RESOURCES_UNRESOLVED')

    def test_fake_cli_process_group_and_descendant_are_stopped_by_close(self):
        """Managed fake CLI only; this is not real Codex or a formal Host channel."""
        import os
        import shlex
        import subprocess
        from martin_agentic.agent_worker import CodexOrderWorker
        directory = self.root / 'fake-cli'
        directory.mkdir()
        pidfile = directory / 'pids'
        import sys
        python = sys.executable
        script = directory / 'fake-codex'
        script.write_text(
            '#!/bin/sh\n'
            + shlex.quote(python) + ' -c '
            + shlex.quote('import time; time.sleep(60)') + ' &\n'
            'child=$!\n'
            'echo "$$ $child" > ' + shlex.quote(str(pidfile)) + '\n'
            'wait "$child"\n')
        script.chmod(0o700)
        worker = CodexOrderWorker(directory, directory, binary=str(script),
                                  timeout=5)
        adapter = LocalWorkerWakeAdapter(worker, directory / 'worker')
        action = {'action_id': 'action-process', 'kind': 'protect',
                  'trade_id': 'trade-process', 'state': 'RUNNING',
                  'parameters': {'symbol': 'DEMO'},
                  'target_order_id': None, 'ref_id': 'ref-process',
                  'synthetic': False}
        event = {'event_id': 'event-process', 'binding': self.binding,
                 'action_id': action['action_id'],
                 'revision': worker_revision(action),
                 'worker_action': action}
        adapter.enqueue(event, idempotency_key='event-process',
                        attempt_id='attempt-process', timeout_seconds=5,
                        cancellation=threading.Event())
        end = time.monotonic() + 3
        while time.monotonic() < end and not pidfile.exists():
            time.sleep(.01)
        self.assertTrue(pidfile.exists())
        pids = [int(value) for value in pidfile.read_text().split()]
        self.assertEqual(len(pids), 2, pids)

        def running(pid):
            value = subprocess.run(
                ['ps', '-o', 'stat=', '-p', str(pid)],
                capture_output=True, text=True)
            state = value.stdout.strip()
            return bool(state) and not state.startswith('Z')

        def parent_of(pid):
            value = subprocess.run(
                ['ps', '-o', 'ppid=', '-p', str(pid)],
                capture_output=True, text=True)
            return int(value.stdout.strip())

        self.assertTrue(all(running(pid) for pid in pids), pids)
        self.assertEqual(parent_of(pids[1]), pids[0])
        self.assertEqual(os.getpgid(pids[0]), os.getpgid(pids[1]))
        time.sleep(.2)
        self.assertTrue(all(running(pid) for pid in pids), pids)
        status = adapter.close()
        end = time.monotonic() + 2
        while time.monotonic() < end and any(running(pid) for pid in pids):
            time.sleep(.02)
        self.assertFalse(any(running(pid) for pid in pids), pids)
        self.assertTrue(status['worker_stop_ok'])
        self.assertEqual(status['status'],
                         'LOCAL_WORKER_WAKE_ADAPTER_STOPPED')

    def test_live_processing_lease_blocks_second_owner_until_release(self):
        action, directory, first = self._durable_result_for_restart('lease')
        taken = first.take_result()
        self.assertIsNotNone(taken)
        second = LocalWorkerWakeAdapter(lambda value: None, directory)
        self.assertIsNone(second.take_result())
        first.close()
        recovered = second.take_result()
        self.assertIsNotNone(recovered)
        second.close()

    def test_cross_adapter_cancel_cas_keeps_unknown_terminal(self):
        class Worker:
            def __init__(self):
                self.gate = threading.Event()
                self.calls = 0

            def __call__(self, action):
                self.calls += 1
                self.gate.wait(2)
                return {'status': 'submitted', 'message': 'CROSS_CANCEL'}

            def stop(self):
                self.gate.set()

        worker = Worker()
        directory = self.root / 'cross-cancel'
        first = LocalWorkerWakeAdapter(worker, directory)
        second = LocalWorkerWakeAdapter(worker, directory)
        action = {'action_id': 'action-cross-cancel', 'kind': 'protect',
                  'trade_id': 'trade-cross-cancel', 'state': 'RUNNING',
                  'parameters': {}, 'target_order_id': None,
                  'ref_id': 'ref-cross-cancel', 'synthetic': False}
        event = {'event_id': 'event-cross-cancel', 'binding': self.binding,
                 'action_id': action['action_id'],
                 'revision': worker_revision(action),
                 'worker_action': action}
        first.enqueue(event, idempotency_key='event-cross-cancel',
                      attempt_id='attempt-cross-cancel', timeout_seconds=2,
                      cancellation=threading.Event())
        end = time.monotonic() + 2
        while time.monotonic() < end and (
                first.claim_state('event-cross-cancel',
                                  'attempt-cross-cancel') or {}
                ).get('state') != 'RUNNING':
            time.sleep(.002)
        previous, changed = second._transition_claim(
            'event-cross-cancel', 'attempt-cross-cancel',
            {'RUNNING'}, 'UNKNOWN', reason='CROSS_ADAPTER_CANCEL')
        self.assertTrue(changed)
        worker.gate.set()
        end = time.monotonic() + 2
        while time.monotonic() < end and first._active:
            time.sleep(.002)
        self.assertEqual(first.claim_state(
            'event-cross-cancel', 'attempt-cross-cancel')['state'],
            'UNKNOWN')
        result = json.loads(next(directory.glob('result-*.json')).read_text())
        self.assertTrue(result['late_after_timeout'])
        first.close()
        second.close()

    def test_real_book_persists_needs_confirmation_waiting_state_once(self):
        fixture, book, action_id, adapter, bridge, directory = self._real_book_bridge(
            'needs_confirmation', message='EXACT_FRESH_CONFIRMATION_REQUIRED')
        current = next(a for a in book.actions() if a['id'] == action_id)
        bridge.enqueue_local_worker_action(current)
        self._wait_result(directory)
        consumed = bridge.consume_local_worker_results()
        self.assertEqual(consumed[0]['disposition'],
                         'WAITING_CONFIRMATION_RECORDED')
        action = next(a for a in book.actions() if a['id'] == action_id)
        self.assertEqual(action['state'], 'WAITING_USER')
        self.assertEqual(json.loads(action['report'])['status'],
                         'needs_confirmation')
        self.assertIsNone(book.claim())

    def test_result_write_failure_is_explicit_and_never_replays_worker(self):
        calls = []

        class Worker:
            def __call__(self, action):
                calls.append(1)
                return {'status': 'submitted', 'message': 'WRITE_FAILED'}

            def stop(self):
                pass

        adapter = LocalWorkerWakeAdapter(Worker(), self.root / 'write-failure')
        original = adapter._private_json

        def fail_results(path, value):
            if Path(path).name.startswith('result-'):
                return False
            return original(path, value)

        adapter._private_json = fail_results
        action = {'action_id': 'action-write-failure', 'kind': 'protect',
                  'trade_id': 'trade-write-failure', 'state': 'RUNNING',
                  'parameters': {}, 'target_order_id': None,
                  'ref_id': 'ref-write-failure', 'synthetic': False}
        event = {'event_id': 'event-write-failure', 'binding': self.binding,
                 'action_id': action['action_id'],
                 'revision': worker_revision(action),
                 'worker_action': action}
        adapter.enqueue(event, idempotency_key='event-write-failure',
                        attempt_id='attempt-write-failure',
                        timeout_seconds=1, cancellation=threading.Event())
        end = time.monotonic() + 2
        while time.monotonic() < end and adapter._active:
            time.sleep(.002)
        self.assertEqual(len(calls), 1)
        self.assertEqual(adapter.claim_state(
            'event-write-failure', 'attempt-write-failure')['state'],
            'UNKNOWN')
        failures = [json.loads(path.read_text()) for path in
                    (self.root / 'write-failure').glob('failure-*.json')]
        self.assertIn('RESULT_WRITE_FAILED',
                      {item.get('reason') for item in failures})
        adapter.close()

    def test_wake_dispatcher_surfaces_adapter_stop_error(self):
        class Worker:
            def __call__(self, action):
                return {'status': 'submitted', 'message': 'STOP_ERROR'}

            def stop(self):
                raise RuntimeError('STOP_BROKEN')

        adapter = LocalWorkerWakeAdapter(Worker(), self.root / 'wake-stop-error')
        dispatcher = AsyncHostWakeDispatcher(
            self.box, self.root / 'wake-stop-receipts', adapter=adapter)
        status = dispatcher.close()
        self.assertFalse(status['adapter_shutdown']['worker_stop_ok'])
        self.assertIn('STOP_BROKEN',
                      status['adapter_shutdown']['worker_stop_error'])
        self.assertTrue(status['attention_required'])

    def test_real_book_report_rollback_requeues_then_applies_once(self):
        fixture, book, action_id, adapter, bridge, directory = self._real_book_bridge(
            'submitted', message='ROLLBACK_RETRY')
        current = next(a for a in book.actions() if a['id'] == action_id)
        bridge.enqueue_local_worker_action(current)
        self._wait_result(directory)
        book.db.execute(
            "CREATE TRIGGER injected_report_rollback BEFORE INSERT ON"
            " audit_events BEGIN SELECT RAISE(ABORT, 'injected rollback'); END")
        book.db.commit()
        first = bridge.consume_local_worker_results()
        self.assertEqual(first[0]['disposition'], 'BOOK_REPORT_RETRY_DEFERRED')
        self.assertEqual(next(a for a in book.actions()
                              if a['id'] == action_id)['state'], 'RUNNING')
        self.assertTrue(list(Path(directory).glob('result-*.json')))
        book.db.execute('DROP TRIGGER injected_report_rollback')
        book.db.commit()
        second = bridge.consume_local_worker_results()
        self.assertEqual(second[0]['disposition'], 'BOOK_REPORT_IMPORTED')
        self.assertEqual(next(a for a in book.actions()
                              if a['id'] == action_id)['state'], 'REPORTED')

    def test_intake_rename_marker_window_is_not_stealable(self):
        directory = self.root / 'intake-gap'
        directory.mkdir()
        first = LocalWorkerWakeAdapter(lambda value: None, directory)
        second = LocalWorkerWakeAdapter(lambda value: None, directory)
        (directory / 'result-probe.json').write_text(
            json.dumps({'action_id': 'act'}))
        entered = threading.Event()
        release = threading.Event()
        original = first._write_processing_owner

        def gated(path):
            entered.set()
            self.assertTrue(release.wait(2))
            return original(path)

        first._write_processing_owner = gated
        outcome = []
        thread = threading.Thread(target=lambda: outcome.append(
            first.take_result()))
        thread.start()
        self.assertTrue(entered.wait(2))
        # The pause is inside the intake critical section. A second owner must
        # not steal or move the processing path while the lease is unpublished.
        self.assertIsNone(second.take_result())
        self.assertTrue(any(item.get('reason') == 'INTAKE_LOCK_BUSY_NO_STEAL'
                            for item in second.failures), second.failures)
        release.set()
        thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertIsNotNone(outcome[0])
        processing = Path(outcome[0][0])
        self.assertTrue(processing.exists())
        self.assertTrue(second._processing_owner_active(processing))
        self.assertFalse((directory / 'result-probe.json').exists())
        first.close()
        second.close()

    def test_finish_and_requeue_refuse_foreign_processing_owner(self):
        action, directory, first = self._durable_result_for_restart('foreign-owner')
        taken = first.take_result()
        self.assertIsNotNone(taken)
        second = LocalWorkerWakeAdapter(lambda value: None, directory)
        self.assertIsNone(second.finish_result(
            taken[0], 'BOOK_REPORT_IMPORTED'))
        self.assertIsNone(second.requeue_result(taken[0], 'BUSY'))
        self.assertTrue(Path(taken[0]).exists())
        self.assertIn('RESULT_FINISH_OWNER_MISMATCH',
                      {item.get('reason') for item in second.failures})
        self.assertIn('RESULT_REQUEUE_OWNER_MISMATCH',
                      {item.get('reason') for item in second.failures})
        first.close()
        second.close()

    def test_missing_owner_recovers_but_unreadable_owner_is_quarantined(self):
        directory = self.root / 'owner-evidence'
        directory.mkdir()
        missing = directory / 'result-missing.processing.json'
        missing.write_text(json.dumps({'action_id': 'missing'}))
        damaged = directory / 'result-damaged.processing.json'
        damaged.write_text(json.dumps({'action_id': 'damaged'}))
        (directory / 'result-damaged.processing.json.owner').write_text('{bad')
        adapter = LocalWorkerWakeAdapter(lambda value: None, directory)
        recovered = adapter.take_result()
        self.assertIsNotNone(recovered)
        self.assertIn('result-missing', Path(recovered[0]).name)
        self.assertIsNone(adapter.take_result())
        self.assertTrue(damaged.exists())
        self.assertIn('PROCESSING_OWNER_UNREADABLE_QUARANTINED',
                      {item.get('reason') for item in adapter.failures})
        adapter.close()

    @unittest.skipUnless(SITE_INSTALLED, 'installed package required for -I child')
    def test_independent_process_take_sigkill_then_new_owner_recovers(self):
        import os
        import subprocess
        import sys
        import martin_agentic
        directory = self.root / 'process-crash'
        directory.mkdir()
        code = (
            'import json,sys,time\n'
            'from pathlib import Path\n'
            'from martin_agentic.local_worker_wake import LocalWorkerWakeAdapter\n'
            'import martin_agentic\n'
            'root=Path(sys.argv[1])\n'
            '(root/"result-probe.json").write_text(json.dumps({"action_id":"act"}))\n'
            'a=LocalWorkerWakeAdapter(lambda value:None,root)\n'
            'item=a.take_result()\n'
            'print(json.dumps({"module_file":martin_agentic.__file__,'
            '"prefix":sys.prefix}),flush=True)\n'
            'print(item[0],flush=True)\n'
            'time.sleep(60)\n')
        env = dict(os.environ)
        env.pop('PYTHONPATH', None)
        env.pop('VIRTUAL_ENV', None)
        env.pop('PYTHONHOME', None)
        process = subprocess.Popen(
            [sys.executable, '-I', '-c', code, str(directory)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            env=env)
        try:
            lines = []

            def read_two():
                for _ in range(2):
                    value = process.stdout.readline()
                    if not value:
                        break
                    lines.append(value.strip())

            reader = threading.Thread(target=read_two, daemon=True)
            reader.start()
            reader.join(3)
            if len(lines) < 2:
                process.kill()
                process.wait(2)
                self.fail(process.stderr.read())
            identity = json.loads(lines[0])
            self.assertEqual(Path(identity['module_file']).resolve(),
                             Path(martin_agentic.__file__).resolve())
            self.assertEqual(identity['prefix'], sys.prefix)
            taken = lines[1]
            self.assertTrue(Path(taken).exists())
            self.assertIsNone(process.poll())
            process.kill()
            process.wait(2)
            owner = json.loads(Path(taken + '.owner').read_text())
            self.assertEqual(owner['pid'], process.pid)
            new_owner = LocalWorkerWakeAdapter(lambda value: None, directory)
            recovered = new_owner.take_result()
            self.assertIsNotNone(recovered)
            self.assertTrue(new_owner._processing_owner_active(Path(taken)))
            marker = json.loads(Path(taken + '.owner').read_text())
            self.assertEqual(marker['owner_id'], new_owner.owner_id)
            new_owner.close()
        finally:
            if process.poll() is None:
                process.kill()
            process.wait(2)
            process.stdout.close()
            process.stderr.close()

    def test_strategy_instance_reaches_local_worker_through_default_ports(self):
        """Existing vwap fixture -> instance admission -> default ports -> worker.

        Real ExecutionBook/claim/default port composition; synthetic broker,
        market, clock and worker executor only. No TestBook/admission override.
        """
        import sqlite3
        from types import SimpleNamespace
        from martin_agentic.agent_worker import CodexOrderWorker
        from martin_luk.live_session import run_live_session
        from martin_agentic.local_worker_wake import LocalWorkerWakeAdapter
        from martin_agentic.simulation import SimulationSources
        from test_supervised_host_default_loop import (
            SimulationBroker, SimulationMarket, SimulationReviewTransport)

        class SimulationLocalWorkerWake(LocalWorkerWakeAdapter):
            simulation_only = True

        now = datetime(2026, 10, 16, 14, tzinfo=timezone.utc)
        calls = []
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory) / 'workspace'
            output = workspace / 'output'
            db_path = workspace / 'book.sqlite'
            policy = json.loads(
                Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
            policy.update(capture_only=False, live_enabled=False,
                          strategy_instances=[{'instance_id': 'vwap-e2e',
                                               'plugin': 'vwap-fixture'}])
            worker_dir = Path(directory) / 'strategy-worker'
            worker = CodexOrderWorker(
                Path.cwd(), worker_dir, review_only=True,
                runner=lambda command, prompt: calls.append(
                    (command, prompt)) or {
                        'status': 'review_only',
                        'message': 'STRATEGY_LOCAL_WORKER',
                        'order_id': None,
                        'market_data_disclosure': 'SYNTHETIC'})
            wake = SimulationLocalWorkerWake(worker, worker_dir)
            with patch('martin_agentic.live_session.wait_for_result_or_tick',
                       side_effect=lambda *a, **k: time.sleep(.02)), \
                 patch('martin_agentic.live_session.notify_local'), \
                 patch('martin_agentic.live_session.print'):
                result = run_live_session(
                    db_path, policy,
                    symbols=['DEMO'] + [str(i) for i in range(9)],
                    cycles=80, poll_seconds=5,
                    agent_mode='supervised-host', simulation=True,
                    simulation_workspace=workspace,
                    simulation_sources=SimulationSources(
                        broker=SimulationBroker(), market=SimulationMarket(),
                        review=SimulationReviewTransport, wake=wake),
                    clock=lambda: now, status_file=workspace / 'status.json',
                    output_dir=output, budget_entry_limit=1,
                    budget_total_limit=1)
            db = sqlite3.connect(db_path)
            try:
                actions = db.execute(
                    "SELECT kind,state,report FROM live_actions").fetchall()
                budgets = db.execute(
                    "SELECT kind FROM worker_call_reservations").fetchall()
            finally:
                db.close()
            status = json.loads((workspace / 'status.json').read_text())
            manifest = json.loads(
                (output / 'host-execution' / 'manifest.json').read_text())
            self.assertEqual(len(calls), 1)
            self.assertEqual(len(actions), 1)
            kind, state, report = actions[0]
            self.assertEqual(kind, 'entry')
            self.assertEqual(state, 'WAITING_USER')
            self.assertEqual(json.loads(report)['status'], 'review_only')
            self.assertEqual(json.loads(report)['message'],
                             'STRATEGY_LOCAL_WORKER')
            self.assertEqual(budgets, [('entry',)])
            self.assertEqual(result['worker_adapter']['selection'],
                             'local-worker-review')
            self.assertEqual(status['worker_adapter']['selection'],
                             'local-worker-review')
            self.assertEqual(manifest['worker_adapter']['selection'],
                             'local-worker-review')
            self.assertEqual(result['worker_budget_entry_scope'],
                             'local_model_worker')
            self.assertEqual(result['host_delivery_status']['status'],
                             'LOCAL_WORKER_EVENT_NOT_TARGETED_HOST')
            self.assertNotEqual(result.get('reason'),
                                'BROKER_OR_LIFECYCLE_UNAVAILABLE')

    def test_second_plugin_and_instance_keep_separate_worker_attribution(self):
        """Two vwap instances plus slow-exit sidecar -> one account.

        The shared symbol lease refuses the same symbol to the second vwap
        instance; a different symbol is then admitted to that instance. The
        second supported plugin runs in the same group without producing an
        entry or breaking either vwap instance. Each worker call carries one
        admitted intent and each attribution row names exactly one instance.
        No order is placed or filled.
        """
        import sqlite3
        from martin_agentic.agent_worker import CodexOrderWorker
        from martin_luk.live_session import run_live_session
        from martin_agentic.local_worker_wake import LocalWorkerWakeAdapter
        from martin_agentic.simulation import SimulationSources
        from test_supervised_host_default_loop import (
            SimulationBroker, SimulationReviewTransport)

        class SimulationLocalWorkerWake(LocalWorkerWakeAdapter):
            simulation_only = True

        class TwoSymbolMarket:
            simulation_only = True
            def __init__(self):
                self.calls = 0
            def __call__(self, *args, **kwargs):
                self.calls += 1
                at = now.isoformat()
                events = []
                for symbol in ('DEMO', 'DEMO2'):
                    identity = 'two-symbol-' + symbol + '-' + str(self.calls)
                    events.append({
                        'id': 'quote-' + identity, 'kind': 'quote', 'at': at,
                        'synthetic_input': True,
                        'source_sha256': 'market-' + identity,
                        'data': {'symbol': symbol, 'bid': '39.99',
                                 'ask': '40.00', 'bid_at': at, 'ask_at': at,
                                 'observed_at': at}})
                return events, {'quotes': len(events),
                                'source_sha256': 'market'}

        now = datetime(2026, 10, 16, 14, tzinfo=timezone.utc)
        calls = []
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory) / 'workspace'
            db_path = workspace / 'book.sqlite'
            policy = json.loads(
                Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
            policy.update(capture_only=False, live_enabled=False,
                          strategy_instances=[
                              {'instance_id': 'vwap-a', 'plugin': 'vwap-fixture'},
                              {'instance_id': 'vwap-b', 'plugin': 'vwap-fixture'},
                              {'instance_id': 'slow-sidecar',
                               'plugin': 'slow-exit-fixture'}])
            worker_dir = Path(directory) / 'two-instance-worker'
            worker = CodexOrderWorker(
                Path.cwd(), worker_dir, review_only=True,
                runner=lambda command, prompt: calls.append(
                    (command, prompt)) or {
                        'status': 'review_only',
                        'message': 'TWO_INSTANCE_WORKER',
                        'order_id': None,
                        'market_data_disclosure': 'SYNTHETIC'})
            wake = SimulationLocalWorkerWake(worker, worker_dir)
            with patch('martin_agentic.live_session.wait_for_result_or_tick',
                       side_effect=lambda *a, **k: time.sleep(.02)), \
                 patch('martin_agentic.live_session.notify_local'), \
                 patch('martin_agentic.live_session.print'):
                result = run_live_session(
                    db_path, policy, symbols=['DEMO', 'DEMO2']
                    + [str(i) for i in range(8)],
                    cycles=80, poll_seconds=5,
                    agent_mode='supervised-host', simulation=True,
                    simulation_workspace=workspace,
                    simulation_sources=SimulationSources(
                        broker=SimulationBroker(), market=TwoSymbolMarket(),
                        review=SimulationReviewTransport, wake=wake),
                    clock=lambda: now, status_file=workspace / 'status.json',
                    output_dir=workspace / 'output', budget_entry_limit=2,
                    budget_total_limit=2)
            db = sqlite3.connect(db_path)
            try:
                actions = db.execute(
                    "SELECT kind,state FROM live_actions ORDER BY id").fetchall()
                intents = db.execute(
                    "SELECT instance_id,symbol,state,trade_id"
                    " FROM execution_intents ORDER BY instance_id").fetchall()
                attribution = db.execute(
                    "SELECT ref_id,instance_id,trade_id FROM execution_attribution"
                    " ORDER BY trade_id,ref_id").fetchall()
                budgets = db.execute(
                    "SELECT kind FROM worker_call_reservations"
                    " ORDER BY kind").fetchall()
            finally:
                db.close()
            self.assertEqual(len(calls), 2)
            self.assertEqual(actions, [('entry', 'WAITING_USER')] * 2)
            self.assertEqual([row[1] for row in intents], ['DEMO', 'DEMO2'])
            self.assertEqual([row[0] for row in intents], ['vwap-a', 'vwap-b'])
            intent_owner = {row[3]: row[0] for row in intents}
            by_trade = {}
            for ref_id, instance_id, trade_id in attribution:
                self.assertTrue(ref_id)
                self.assertEqual(intent_owner[trade_id], instance_id)
                by_trade.setdefault(trade_id, set()).add(instance_id)
            self.assertEqual(len(by_trade), 2)
            self.assertEqual(sorted(next(iter(owners)) for owners in by_trade.values()),
                             ['vwap-a', 'vwap-b'])
            self.assertEqual(len({row[0] for row in attribution}), len(attribution))
            self.assertEqual(budgets, [('entry',), ('entry',)])
            self.assertEqual(result['worker_adapter']['selection'],
                             'local-worker-review')

    def test_local_worker_dispatcher_idle_never_reserves_budget_or_calls_model(self):
        import tempfile
        from types import SimpleNamespace
        from martin_luk.live_session import LocalWorkerDispatcher
        from martin_agentic.worker_budget import WorkerBudget

        class Book:
            def __init__(self):
                self.claims = 0
            def claim(self, *, allow_entries=True):
                self.claims += 1
                return None

        class Bridge:
            task = SimpleNamespace(status=lambda: {'running': False})
            pending = None
            def __call__(self, *args, **kwargs):
                return {'agent_called': False}
            def enqueue_local_worker_action(self, action):
                raise AssertionError('idle dispatch must not wake worker')
            def consume_local_worker_results(self):
                return []
            def settle_local_worker_wake(self):
                return None
            def close(self):
                return {'status': 'FIXTURE'}

        with tempfile.TemporaryDirectory() as directory:
            book = Book()
            budget = WorkerBudget(Path(directory) / 'budget.sqlite',
                                  '2026-10-16', entry_limit=1, total_limit=1)
            try:
                dispatcher = LocalWorkerDispatcher(book, Bridge(), budget=budget)
                for _ in range(3):
                    result = dispatcher(snapshot={}, quotes={},
                                        allow_entries=False)
                    self.assertFalse(result['agent_called'])
                    self.assertEqual(result['reason'], 'NO_DISPATCHABLE_ACTION')
                self.assertEqual(book.claims, 3)
                self.assertEqual(budget.status()['total_calls_reserved'], 0)
            finally:
                budget.close()

    def test_strategy_entry_budget_exhaustion_does_not_claim_or_create_unknown(self):
        import sqlite3
        from martin_agentic.agent_worker import CodexOrderWorker
        from martin_luk.live_session import run_live_session
        from martin_agentic.local_worker_wake import LocalWorkerWakeAdapter
        from martin_agentic.simulation import SimulationSources
        from test_supervised_host_default_loop import (
            SimulationBroker, SimulationMarket, SimulationReviewTransport)

        class SimulationLocalWorkerWake(LocalWorkerWakeAdapter):
            simulation_only = True

        now = datetime(2026, 10, 16, 14, tzinfo=timezone.utc)
        calls = []
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory) / 'workspace'
            db_path = workspace / 'book.sqlite'
            policy = json.loads(
                Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
            policy.update(capture_only=False, live_enabled=False,
                          strategy_instances=[{'instance_id': 'vwap-e2e',
                                               'plugin': 'vwap-fixture'}])
            worker_dir = Path(directory) / 'budget-worker'
            worker = CodexOrderWorker(
                Path.cwd(), worker_dir, review_only=True,
                runner=lambda command, prompt: calls.append(
                    (command, prompt)) or {
                        'status': 'review_only',
                        'message': 'SHOULD_NOT_RUN',
                        'order_id': None,
                        'market_data_disclosure': 'SYNTHETIC'})
            wake = SimulationLocalWorkerWake(worker, worker_dir)
            with patch('martin_agentic.live_session.wait_for_result_or_tick',
                       side_effect=lambda *a, **k: time.sleep(.02)), \
                 patch('martin_agentic.live_session.notify_local'), \
                 patch('martin_agentic.live_session.print'):
                result = run_live_session(
                    db_path, policy,
                    symbols=['DEMO'] + [str(i) for i in range(9)],
                    cycles=80, poll_seconds=5,
                    agent_mode='supervised-host', simulation=True,
                    simulation_workspace=workspace,
                    simulation_sources=SimulationSources(
                        broker=SimulationBroker(), market=SimulationMarket(),
                        review=SimulationReviewTransport, wake=wake),
                    clock=lambda: now, status_file=workspace / 'status.json',
                    output_dir=workspace / 'output', budget_entry_limit=0,
                    budget_total_limit=1)
            db = sqlite3.connect(db_path)
            try:
                actions = db.execute(
                    "SELECT kind,state,report FROM live_actions").fetchall()
                budgets = db.execute(
                    "SELECT kind FROM worker_call_reservations").fetchall()
                leases = db.execute(
                    "SELECT symbol,instance_id,trade_id FROM symbol_leases"
                ).fetchall()
                reserved = db.execute(
                    "SELECT instance_id,trade_id"
                    " FROM account_budget_reservations").fetchall()
                intents = db.execute(
                    "SELECT state,instance_id,symbol,trade_id"
                    " FROM execution_intents").fetchall()
            finally:
                db.close()
            self.assertEqual(calls, [])
            self.assertEqual(budgets, [])
            self.assertEqual(actions, [])
            self.assertEqual(leases, [])
            self.assertEqual(reserved, [])
            self.assertEqual(intents, [])
            self.assertIn('WORKER_ENTRY_BUDGET_EXHAUSTED',
                          result['new_entries_blockers'])
            self.assertEqual(result['worker_adapter']['selection'],
                             'local-worker-review')

    def test_entry_budget_exhaustion_still_claims_management(self):
        import tempfile
        from test_native_operations import NativeOperationTests
        from martin_agentic.host_execution import HostExecutionBridge
        from martin_luk.live_session import LocalWorkerDispatcher
        from martin_agentic.local_worker_wake import LocalWorkerWakeAdapter
        from martin_agentic.worker_budget import WorkerBudget
        fixture = NativeOperationTests(
            'test_preview_does_not_reserve_approve_or_submit')
        fixture.setUp()
        try:
            book = fixture.book
            directory = Path(fixture.f.tmp.name) / 'budget-manage'
            budget = WorkerBudget(
                book.db.execute('PRAGMA database_list').fetchone()[2],
                fixture.f.policy['session_date'], entry_limit=0, total_limit=1)
            adapter = LocalWorkerWakeAdapter(
                lambda action: {'status': 'review_only', 'message': 'MANAGE',
                                'order_id': None,
                                'market_data_disclosure': 'SYNTHETIC'},
                directory)
            bridge = HostExecutionBridge(book, directory / 'host',
                                         wake_adapter=adapter)
            dispatcher = LocalWorkerDispatcher(book, bridge, budget=budget)
            try:
                result = dispatcher(snapshot=fixture.snapshot(), quotes={},
                                    allow_entries=True)
                self.assertTrue(result['agent_called'])
                self.assertEqual(result['budget']['entry_calls_reserved'], 0)
                end = time.monotonic() + 2
                while time.monotonic() < end and not list(
                        directory.glob('result-*.json')):
                    time.sleep(.005)
                self.assertTrue(list(directory.glob('result-*.json')))
                consumed = bridge.consume_local_worker_results()
                self.assertEqual(consumed[0]['disposition'],
                                 'WAITING_CONFIRMATION_RECORDED')
                action_id = result['action_id']
                action = next(a for a in book.actions()
                              if a['id'] == action_id)
                self.assertEqual(action['state'], 'WAITING_USER')
                self.assertEqual(budget.status()['entry_calls_reserved'], 0)
                self.assertEqual(budget.status()['total_calls_reserved'], 1)
            finally:
                adapter.close()
                bridge.close()
                budget.close()
        finally:
            fixture.tearDown()

    def _real_dry_protect(self):
        from test_engineering_source import RealDryFixture
        from test_broker import FakeBroker, order, position, snapshot
        fixture = RealDryFixture(lineage=True, prebuilt_intent=True,
                                 activate=True)
        self.addCleanup(fixture.close)
        entry = fixture.book.actions()[0]
        payload = json.loads(entry['payload'])
        raw = order('o1', 'buy', str(payload['parameters']['quantity']),
                    '2', 'filled')
        raw.update(ref_id=payload['ref_id'],
                   price=payload['parameters'].get('limit_price'),
                   type='limit',
                   market_hours=payload['parameters']['market_hours'],
                   time_in_force=payload['parameters']['time_in_force'])
        broker = FakeBroker()
        broker.orders = [raw]
        broker.positions = [position('2', '0')]
        broker.cash = '420'
        snap = snapshot(broker)
        snap['synthetic'] = False
        fixture.book.reconcile(snap, quotes={'DEMO': fixture._quote()})
        fixture._reserve_snapshot = snap
        protect = next(a for a in fixture.book.actions()
                       if a['kind'] == 'protect')
        return fixture, protect

    def _real_budget_and_dispatcher(self, fixture, *, entry_limit=1,
                                    total_limit=2):
        from martin_agentic.host_execution import HostExecutionBridge
        from martin_luk.live_session import LocalWorkerDispatcher
        from martin_agentic.local_worker_wake import LocalWorkerWakeAdapter
        from martin_agentic.worker_budget import WorkerBudget
        db_path = fixture.book.db.execute(
            'PRAGMA database_list').fetchone()[2]
        budget = WorkerBudget(db_path, fixture.policy['session_date'],
                              entry_limit=entry_limit, total_limit=total_limit)
        self.addCleanup(budget.close)
        root = (Path(fixture.root) if hasattr(fixture, 'root')
                else Path(fixture.temp.name))
        calls = []
        adapter = LocalWorkerWakeAdapter(
            lambda action: calls.append(action), root / 'reserve-worker')
        bridge = HostExecutionBridge(
            fixture.book, root / 'reserve-host', wake_adapter=adapter)
        self.addCleanup(bridge.close)
        self.addCleanup(adapter.close)
        return budget, LocalWorkerDispatcher(
            fixture.book, bridge, budget=budget), calls

    def test_real_book_reserve_false_withdraws_claim_not_running(self):
        fixture, protect = self._real_dry_protect()
        budget, dispatcher, calls = self._real_budget_and_dispatcher(fixture)
        with patch.object(budget, 'reserve', return_value=False):
            result = dispatcher(allow_entries=False)
        action = next(a for a in fixture.book.actions()
                      if a['id'] == protect['id'])
        self.assertEqual(result['reason'],
                         'BUDGET_RESERVATION_FAILED_NOT_DISPATCHED')
        self.assertFalse(result['attention_required'])
        self.assertEqual(result['not_dispatched']['state'], 'SUPERSEDED')
        self.assertEqual(action['state'], 'SUPERSEDED')
        self.assertEqual(json.loads(action['report'])['status'],
                         'not_dispatched')
        with self.assertRaisesRegex(ValueError, 'INVALID_AGENT_REPORT'):
            fixture.book.report(protect['id'], {'status': 'not_dispatched'})
        self.assertEqual(calls, [])
        self.assertEqual(budget.status()['total_calls_reserved'], 0)
        self.assertFalse(fixture.book.db.in_transaction)
        self.assertFalse(budget.db.in_transaction)
        event = json.loads(fixture.book.db.execute(
            'SELECT payload FROM audit_events WHERE event_id=?',
            (protect['id'] + '-not-dispatched',)).fetchone()[0])
        self.assertEqual(event['event_type'], 'AGENT_ACTION_RESULT')
        self.assertEqual(event['details']['outcome'], 'failed')
        self.assertEqual(event['details']['dispatch_proof'],
                         'RESERVATION_REJECTED_WITHOUT_RECORD')
        self.assertFalse(event['details']['broker_verified'])
        self.assertTrue(fixture.book.audit.verify()['integrity_valid'])
        audit_report = fixture.book.audit.report(
            fixture.policy['session_date'])
        self.assertNotIn(protect['id'],
                         audit_report['unresolved_agent_intents'])
        # A fresh reconcile must recover the same management duty through a
        # new generation; the old SUPERSEDED row and its audit stay intact.
        fixture.book.reconcile(fixture._reserve_snapshot,
                               quotes={'DEMO': fixture._quote()})
        recovered = [a for a in fixture.book.actions()
                     if a['kind'] == 'protect' and a['id'] != protect['id']]
        self.assertEqual(len(recovered), 1)
        self.assertEqual(recovered[0]['state'], 'READY')
        claimed = fixture.book.claim(allow_entries=False)
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed['action_id'], recovered[0]['id'])
        self.assertEqual(next(a for a in fixture.book.actions()
                              if a['id'] == recovered[0]['id'])['state'],
                         'RUNNING')
        self.assertTrue(fixture.book.audit.verify()['integrity_valid'])

    def test_real_book_reserve_error_rolls_back_without_phantom(self):
        fixture, protect = self._real_dry_protect()
        budget, dispatcher, calls = self._real_budget_and_dispatcher(fixture)
        # Real WorkerBudget connection, real reserve(), SQLite fault injected
        # at the reservation table rather than mocking the method.
        budget.db.execute(
            'CREATE TRIGGER fail_reservation BEFORE INSERT ON'
            ' worker_call_reservations BEGIN SELECT RAISE(ABORT,'
            " 'injected reservation failure'); END")
        budget.db.commit()
        result = dispatcher(allow_entries=False)
        action = next(a for a in fixture.book.actions()
                      if a['id'] == protect['id'])
        self.assertEqual(result['reason'],
                         'BUDGET_RESERVATION_ERROR_ROLLED_BACK')
        self.assertEqual(action['state'], 'SUPERSEDED')
        self.assertEqual(calls, [])
        self.assertEqual(budget.status()['total_calls_reserved'], 0)
        self.assertFalse(fixture.book.db.in_transaction)
        self.assertFalse(budget.db.in_transaction)
        event = json.loads(fixture.book.db.execute(
            'SELECT payload FROM audit_events WHERE event_id=?',
            (protect['id'] + '-not-dispatched',)).fetchone()[0])
        self.assertEqual(event['details']['dispatch_proof'],
                         'RESERVATION_ERROR_ROLLED_BACK')
        self.assertTrue(fixture.book.audit.verify()['integrity_valid'])

    def test_real_book_existing_reservation_stays_unknown(self):
        fixture, protect = self._real_dry_protect()
        budget, dispatcher, calls = self._real_budget_and_dispatcher(fixture)
        self.assertTrue(budget.reserve({'action_id': protect['id'],
                                        'kind': 'protect'}))
        result = dispatcher(allow_entries=False)
        action = next(a for a in fixture.book.actions()
                      if a['id'] == protect['id'])
        self.assertEqual(result['reason'],
                         'BUDGET_RESERVATION_RACE_OR_FAILURE')
        self.assertTrue(result['attention_required'])
        self.assertEqual(action['state'], 'UNKNOWN')
        self.assertEqual(json.loads(action['report'])['status'], 'unknown')
        self.assertEqual(calls, [])
        self.assertEqual(budget.status()['total_calls_reserved'], 1)
        self.assertTrue(fixture.book.audit.verify()['integrity_valid'])

    def _slow_exit_ready(self):
        """Real slow-exit plugin advisory applied by the real Book."""
        from test_vwap_default_exit import VwapDefaultExitTests
        from trading_infra.exit_advisory import ExitAdvisoryService
        fixture = VwapDefaultExitTests(
            'test_default_runner_advisor_timeout_keeps_reconcile_and_protection')
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        fixture.register_owner(state=None, instance_id='slow-sidecar',
                               plugin_id='slow-exit-fixture')
        book = fixture.book
        trade = book.state()['trades'][fixture.trade_id]
        quote = dict(fixture.fixture.quote)
        view = {
            'instance_id': 'slow-sidecar', 'plugin_id': 'slow-exit-fixture',
            'trade_id': fixture.trade_id, 'symbol': 'DEMO',
            'quantity': str(trade['quantity']),
            'entry_price': str(trade['entry_price']),
            'stop': str(trade['stop']),
            'initial_stop': str(trade['initial_stop']),
            'quote': quote, 'features': {},
            'as_of': fixture.now.isoformat(),
            'position_version': book.exit_position_version(trade)}
        service = ExitAdvisoryService(
            {'slow-exit-fixture': 'trading_strategies.slow_exit_fixture:advise'},
            timeout_seconds=6, advice_ttl_seconds=20)
        advice = service.advise_one(view)
        self.assertEqual(advice.get('action'), 'EXIT', advice)
        self.assertEqual(advice.get('reason'), 'SLOW_FIXTURE_MUST_TIMEOUT')
        fixture._slow_quote = quote
        book.reconcile(fixture.snapshot, quotes={'DEMO': quote},
                       exit_advices={fixture.trade_id: advice})
        exit_action = next(item for item in book.actions()
                           if item['kind'] == 'exit')
        self.assertEqual(exit_action['state'], 'READY')
        self.assertEqual(book.state()['trades'][fixture.trade_id]
                         ['last_exit_advice']['action'], 'EXIT')
        return fixture, exit_action

    def test_slow_exit_plugin_advice_reaches_real_exit_worker(self):
        """Second supported plugin: real advisory -> Book exit -> worker.

        The slow fixture is the second plugin family. Its real advisory rule
        runs in the existing ExitAdvisoryService, its EXIT is applied by the
        real ExecutionBook exit path, and the resulting action is claimed and
        sent through the real HostExecutionBridge to CodexOrderWorker with a
        fake runner. No order is placed or filled.
        """
        import time
        from martin_agentic.agent_worker import CodexOrderWorker
        from martin_agentic.host_execution import HostExecutionBridge
        from martin_luk.live_session import LocalWorkerDispatcher
        from martin_agentic.local_worker_wake import LocalWorkerWakeAdapter
        from martin_agentic.worker_budget import WorkerBudget
        fixture, exit_action = self._slow_exit_ready()
        book = fixture.book
        worker_dir = Path(fixture.temp.name) / 'slow-worker'
        calls = []
        worker = CodexOrderWorker(
            Path.cwd(), worker_dir, review_only=True,
            runner=lambda command, prompt: calls.append(
                (command, prompt)) or {
                    'status': 'review_only', 'message': 'SLOW_EXIT_WORKER',
                    'order_id': None, 'market_data_disclosure': 'SYNTHETIC'})
        adapter = LocalWorkerWakeAdapter(worker, worker_dir)
        self.addCleanup(adapter.close)
        bridge = HostExecutionBridge(
            book, Path(fixture.temp.name) / 'slow-host',
            wake_adapter=adapter)
        self.addCleanup(bridge.close)
        db_path = book.db.execute('PRAGMA database_list').fetchone()[2]
        budget = WorkerBudget(db_path, fixture.policy['session_date'],
                              entry_limit=0, total_limit=1)
        self.addCleanup(budget.close)
        dispatcher = LocalWorkerDispatcher(book, bridge, budget=budget)
        result = dispatcher(allow_entries=False)
        self.assertTrue(result['agent_called'])
        end = time.monotonic() + 2
        while time.monotonic() < end and not list(worker_dir.glob('result-*.json')):
            time.sleep(.005)
        consumed = bridge.consume_local_worker_results()
        self.assertEqual(consumed[0]['disposition'],
                         'WAITING_CONFIRMATION_RECORDED')
        action = next(item for item in book.actions()
                      if item['id'] == exit_action['id'])
        self.assertEqual(action['state'], 'WAITING_USER')
        self.assertEqual(json.loads(action['report'])['status'], 'review_only')
        self.assertEqual(len(calls), 1)
        self.assertEqual(budget.status()['total_calls_reserved'], 1)

    def test_real_book_exit_reserve_false_withdraws_claim_not_running(self):
        fixture, exit_action = self._slow_exit_ready()
        budget, dispatcher, calls = self._real_budget_and_dispatcher(
            fixture, entry_limit=0, total_limit=1)
        with patch.object(budget, 'reserve', return_value=False):
            result = dispatcher(allow_entries=False)
        action = next(item for item in fixture.book.actions()
                      if item['id'] == exit_action['id'])
        self.assertEqual(result['reason'],
                         'BUDGET_RESERVATION_FAILED_NOT_DISPATCHED')
        self.assertEqual(action['state'], 'SUPERSEDED')
        self.assertEqual(calls, [])
        self.assertEqual(budget.status()['total_calls_reserved'], 0)
        event = json.loads(fixture.book.db.execute(
            'SELECT payload FROM audit_events WHERE event_id=?',
            (exit_action['id'] + '-not-dispatched',)).fetchone()[0])
        self.assertEqual(event['details']['outcome'], 'failed')
        self.assertFalse(event['details']['broker_verified'])
        self.assertTrue(fixture.book.audit.verify()['integrity_valid'])
        fixture.book.reconcile(fixture.snapshot,
                               quotes={'DEMO': fixture._slow_quote})
        recovered = [a for a in fixture.book.actions()
                     if a['kind'] == 'exit' and a['id'] != exit_action['id']]
        self.assertEqual(len(recovered), 1)
        self.assertEqual(recovered[0]['state'], 'READY')
        claimed = fixture.book.claim(allow_entries=False)
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed['action_id'], recovered[0]['id'])

    def test_real_book_active_worker_blocks_undispatched_withdraw(self):
        import threading
        from martin_agentic.host_execution import HostExecutionBridge
        fixture, protect = self._real_dry_protect()
        book = fixture.book
        gate = threading.Event()
        started = threading.Event()
        def work(action):
            started.set()
            gate.wait(2)
            return {'status': 'review_only', 'message': 'ACTIVE_WORKER',
                    'order_id': None, 'market_data_disclosure': 'SYNTHETIC'}
        adapter = LocalWorkerWakeAdapter(work, fixture.root / 'active-worker')
        self.addCleanup(adapter.close)
        bridge = HostExecutionBridge(book, fixture.root / 'active-host',
                                     wake_adapter=adapter)
        self.addCleanup(bridge.close)
        book.claim(allow_entries=False)
        running = next(a for a in book.actions() if a['id'] == protect['id'])
        try:
            bridge.enqueue_local_worker_action(running)
            self.assertTrue(started.wait(2))
            with self.assertRaisesRegex(
                    ValueError, 'NOT_DISPATCHED_DISPATCH_EVIDENCE_PRESENT'):
                book.abandon_not_dispatched(
                    protect['id'], reason='probe',
                    proof='RESERVATION_REJECTED_WITHOUT_RECORD')
            action = next(a for a in book.actions()
                          if a['id'] == protect['id'])
            self.assertEqual(action['state'], 'RUNNING')
            self.assertIsNone(action['report'])
            self.assertFalse(book.db.execute(
                'SELECT 1 FROM audit_events WHERE event_id=?',
                (protect['id'] + '-not-dispatched',)).fetchone())
        finally:
            gate.set()

    def test_real_book_worker_reservation_blocks_undispatched_withdraw(self):
        fixture, protect = self._real_dry_protect()
        budget, dispatcher, calls = self._real_budget_and_dispatcher(fixture)
        fixture.book.claim(allow_entries=False)
        self.assertTrue(budget.reserve({'action_id': protect['id'],
                                        'kind': 'protect'}))
        with self.assertRaisesRegex(
                ValueError, 'NOT_DISPATCHED_DISPATCH_EVIDENCE_PRESENT'):
            fixture.book.abandon_not_dispatched(
                protect['id'], reason='probe',
                proof='RESERVATION_REJECTED_WITHOUT_RECORD')
        action = next(a for a in fixture.book.actions()
                      if a['id'] == protect['id'])
        self.assertEqual(action['state'], 'RUNNING')
        self.assertIsNone(action['report'])
        self.assertEqual(budget.status()['total_calls_reserved'], 1)
        self.assertFalse(fixture.book.db.execute(
            'SELECT 1 FROM audit_events WHERE event_id=?',
            (protect['id'] + '-not-dispatched',)).fetchone())
        self.assertTrue(fixture.book.audit.verify()['integrity_valid'])

    def test_total_budget_exhaustion_never_claims_or_calls_worker(self):
        import tempfile
        from test_native_operations import NativeOperationTests
        from martin_agentic.host_execution import HostExecutionBridge
        from martin_luk.live_session import LocalWorkerDispatcher
        from martin_agentic.local_worker_wake import LocalWorkerWakeAdapter
        from martin_agentic.worker_budget import WorkerBudget
        fixture = NativeOperationTests(
            'test_preview_does_not_reserve_approve_or_submit')
        fixture.setUp()
        try:
            book = fixture.book
            directory = Path(fixture.f.tmp.name) / 'budget-zero'
            budget = WorkerBudget(
                book.db.execute('PRAGMA database_list').fetchone()[2],
                fixture.f.policy['session_date'], entry_limit=0, total_limit=0)
            adapter = LocalWorkerWakeAdapter(
                lambda action: self.fail('zero budget must not call worker'),
                directory)
            bridge = HostExecutionBridge(book, directory / 'host',
                                         wake_adapter=adapter)
            dispatcher = LocalWorkerDispatcher(book, bridge, budget=budget)
            try:
                result = dispatcher(snapshot=fixture.snapshot(), quotes={},
                                    allow_entries=True)
                self.assertFalse(result['agent_called'])
                self.assertEqual(result['reason'],
                                 'MODEL_BUDGET_EXHAUSTED_MANAGEMENT_PENDING')
                self.assertFalse(result['attention_required'])
                self.assertEqual(budget.status()['total_calls_reserved'], 0)
                self.assertEqual(next(a for a in book.actions()
                                      if a['id'] == fixture.action)['state'],
                                 'READY')
            finally:
                adapter.close()
                bridge.close()
                budget.close()
        finally:
            fixture.tearDown()
