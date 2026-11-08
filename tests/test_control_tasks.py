from datetime import datetime,timezone
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
from unittest import TestCase
from unittest.mock import MagicMock,patch

from martin_agentic.control_tasks import BackgroundTask,FrozenSignals,ReadTransportOwner,owned_read_transport
from martin_luk.live_session import AsyncDispatcher
from martin_agentic.worker_budget import WorkerBudget


class CloseOrderTests(TestCase):
    def test_production_exit_first_close_uses_shared_shutdown_deadline(self):
        from martin_agentic.transport import CodexMarketTransport
        calls = []
        class Rpc:
            def close(self, *, deadline_monotonic=None):
                calls.append(deadline_monotonic)
                return {'complete': True}
        class Transport:
            def __init__(self, **kwargs):
                self.rpc = Rpc()
            def __enter__(self):
                return self
            __exit__ = CodexMarketTransport.__exit__
        owner = ReadTransportOwner()
        with owned_read_transport(Transport, owner,
                                  close_deadline_monotonic=110.0,
                                  deadline_monotonic=110.0):
            pass
        self.assertEqual(calls, [110.0, 110.0])
        self.assertTrue(owner.cleanup_status()['rpc_cleanup']['complete'])

    def test_production_exit_incomplete_first_close_is_not_green(self):
        from martin_agentic.transport import CodexMarketTransport
        calls = []
        class Rpc:
            def close(self, *, deadline_monotonic=None):
                calls.append(deadline_monotonic)
                return {'complete': False, 'reader_alive': True}
        class Transport:
            def __init__(self, **kwargs):
                self.rpc = Rpc()
            def __enter__(self):
                return self
            __exit__ = CodexMarketTransport.__exit__
        owner = ReadTransportOwner()
        with owned_read_transport(Transport, owner,
                                  close_deadline_monotonic=110.0,
                                  deadline_monotonic=110.0):
            pass
        self.assertEqual(calls, [110.0, 110.0])
        self.assertFalse(owner.cleanup_status()['rpc_cleanup']['complete'])

    def test_enter_failure_still_closes_with_shared_deadline(self):
        from martin_agentic.transport import CodexMarketTransport
        calls = []
        class Rpc:
            def close(self, *, deadline_monotonic=None):
                calls.append(deadline_monotonic)
                return {'complete': True}
        class Transport:
            def __init__(self, **kwargs):
                self.rpc = Rpc()
            def __enter__(self):
                raise RuntimeError('enter failed')
            __exit__ = CodexMarketTransport.__exit__
        owner = ReadTransportOwner()
        with self.assertRaisesRegex(RuntimeError, 'enter failed'):
            with owned_read_transport(Transport, owner,
                                      close_deadline_monotonic=110.0,
                                      deadline_monotonic=110.0):
                self.fail('must not enter')
        self.assertEqual(calls, [110.0])
        self.assertTrue(owner.cleanup_status()['rpc_cleanup']['complete'])

    def test_legacy_ownerless_factory_keeps_default_close_path(self):
        from martin_agentic.transport import CodexMarketTransport
        calls = []
        class Rpc:
            def close(self):
                calls.append(None)
        class Transport:
            def __init__(self, **kwargs):
                self.rpc = Rpc()
            def __enter__(self):
                return self
            __exit__ = CodexMarketTransport.__exit__
        with owned_read_transport(Transport, deadline_monotonic=110.0):
            pass
        self.assertEqual(calls, [None, None])

class ControlTasksTests(TestCase):
    def test_thread_start_failure_does_not_leave_unstarted_task_owned(self):
        task=BackgroundTask('unstarted')
        with patch('martin_agentic.control_tasks.threading.Thread.start',side_effect=RuntimeError('start failed')):
            with self.assertRaises(RuntimeError):task.start(lambda:None)
        self.assertIsNone(task.thread);self.assertIsNone(task.started)
        task.start(lambda:1);task.thread.join(1);self.assertEqual(task.take(),(True,1))
    def test_owned_context_reaps_child_when_registration_hook_fails_after_attach(self):
        import sys
        from martin_agentic.transport import StdioRpc
        children=[]
        class Owner(ReadTransportOwner):
            def attach(self,rpc):
                super().attach(rpc)
                raise RuntimeError('synthetic registration hook failure')
        class Transport:
            def __init__(self):
                self.rpc=StdioRpc([sys.executable,'-u','-c','import time;time.sleep(30)'],timeout=5)
                children.append(self.rpc)
            def __enter__(self):raise AssertionError('registration must fail first')
        owner=Owner()
        try:
            with self.assertRaisesRegex(RuntimeError,'synthetic registration hook failure'):
                with owned_read_transport(Transport,owner):self.fail('cannot enter')
            self.assertIsNotNone(children[0].process.poll());self.assertIsNone(owner.rpc)
            self.assertTrue(all(s.closed for s in (children[0].process.stdin,children[0].process.stdout,children[0].process.stderr)))
        finally:
            for rpc in children:rpc.close()

    def test_owned_context_can_cancel_actual_child_during_transport_initialization(self):
        import sys
        from martin_agentic.transport import StdioRpc
        owner=ReadTransportOwner();started=threading.Event();children=[]
        class Transport:
            def __init__(self):
                self.rpc=StdioRpc([sys.executable,'-u','-c','import time;time.sleep(30)'],timeout=5)
                children.append(self.rpc)
            def __enter__(self):
                started.set();self.rpc.request('initialize',{})
                return self
            def __exit__(self,*args):self.rpc.close()
        def read():
            with owned_read_transport(Transport,owner):self.fail('blocked initialization cannot finish')
        task=BackgroundTask('startup-read');task.start(read)
        try:
            self.assertTrue(started.wait(1));owner.cancel();task.thread.join(1)
            self.assertFalse(task.thread.is_alive());self.assertFalse(task.take()[0])
            self.assertIsNotNone(children[0].process.poll());self.assertIsNone(owner.rpc)
        finally:
            owner.cancel()
            for rpc in children:rpc.close()

    def test_read_shutdown_reaps_actual_owned_child_while_request_is_blocked(self):
        import sys
        from martin_agentic.transport import StdioRpc
        rpc=StdioRpc([sys.executable,'-u','-c','import time;time.sleep(30)'],timeout=5)
        owner=ReadTransportOwner();owner.attach(rpc);task=BackgroundTask('owned-read')
        task.start(rpc.request,'blocked',{})
        try:
            owner.cancel();task.thread.join(1)
            self.assertFalse(task.thread.is_alive());self.assertIsNotNone(rpc.process.poll())
            self.assertTrue(all(s.closed for s in (rpc.process.stdin,rpc.process.stdout,rpc.process.stderr)))
            self.assertFalse(task.take()[0]);owner.cancel()
        finally:rpc.close()

    def test_read_shutdown_wins_late_registration_and_prevents_reconnect(self):
        from martin_luk.live_session import _broker_read
        from martin_agentic.transport import TransportError
        owner=ReadTransportOwner();owner.cancel();rpc=MagicMock()
        with self.assertRaisesRegex(RuntimeError,'READ_ONLY_COLLECTION_STOPPED'):owner.attach(rpc)
        rpc.close.assert_called_once()
        with patch('martin_agentic.live_session.CodexBrokerReadTransport') as factory:
            with self.assertRaisesRegex(RuntimeError,'READ_ONLY_COLLECTION_STOPPED'):
                _broker_read({'account_suffix':'0000'},lambda:None,cancellation=owner)
            factory.assert_not_called()
        owner=ReadTransportOwner()
        def interrupted(*args,**kwargs):
            owner.cancel();raise TransportError('APP_SERVER_PIPE_CLOSED')
        with patch('martin_agentic.live_session.CodexBrokerReadTransport') as factory:
            factory.return_value.__enter__.side_effect=interrupted
            with self.assertRaisesRegex(RuntimeError,'READ_ONLY_COLLECTION_STOPPED'):
                _broker_read({'account_suffix':'0000'},lambda:None,cancellation=owner)
            factory.assert_called_once()

    def test_real_blocked_collection_does_not_block_owner_or_share_mutable_state(self):
        task=BackgroundTask('test-blocked-read');release=threading.Event();started=threading.Event()
        source={'ema9':{'SOFI':'10'}};detached=FrozenSignals(source);source['ema9']['SOFI']='999'
        def slow():
            started.set();release.wait(1);return detached.state()
        task.start(slow);self.assertTrue(started.wait(.5));owner_ticks=[]
        begin=time.monotonic()
        for _ in range(5):
            self.assertIsNone(task.take());owner_ticks.append(time.monotonic());time.sleep(.02)
        self.assertTrue(task.thread.is_alive());self.assertLess(time.monotonic()-begin,.5)
        self.assertEqual(len(owner_ticks),5);release.set();task.thread.join(.5)
        self.assertEqual(task.take(),(True,{'ema9':{'SOFI':'10'}}))

    def test_slow_worker_has_one_claim_and_reports_only_on_owner_after_durable_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            book=MagicMock();action={'action_id':'one','kind':'entry','parameters':{'quantity':'1'}}
            book.claim.return_value=action;book.actions.return_value=[{'id':'one','state':'RUNNING'}];release=threading.Event();started=threading.Event();owner=threading.get_ident();worker_threads=[]
            def worker(data):
                worker_threads.append(threading.get_ident());started.set();release.wait(1)
                return {'status':'needs_confirmation','message':'formal user confirmation required'}
            budget=WorkerBudget(Path(directory)/'budget.sqlite','2026-10-19')
            dispatch=AsyncDispatcher(book,worker,directory,budget)
            try:
                self.assertTrue(dispatch()['agent_called']);self.assertTrue(started.wait(.5))
                for _ in range(5):
                    self.assertEqual(dispatch()['reason'],'WORKER_IN_FLIGHT');time.sleep(.01)
                self.assertEqual(book.claim.call_count,1);book.report.assert_not_called()
                self.assertNotEqual(worker_threads[0],owner);release.set();dispatch.task.thread.join(.5)
                report_threads=[];book.report.side_effect=lambda *args:report_threads.append(threading.get_ident())
                completed=dispatch(allow_entries=False,allow_dispatch=False)['completed'];receipt=json.loads(Path(completed['receipt_file']).read_text())
                self.assertEqual(receipt['action_id'],'one');self.assertEqual(len(receipt['action_sha256']),64)
                self.assertEqual(report_threads,[owner]);self.assertEqual(budget.status()['total_calls_reserved'],1)
                self.assertEqual(dispatch(allow_dispatch=False)['reason'],'FRESH_BROKER_OBSERVATION_REQUIRED')
                self.assertEqual(book.claim.call_count,1)
            finally:release.set();budget.close()

    def test_budget_exhaustion_preserves_management_action_without_claim(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'budget.sqlite';budget=WorkerBudget(path,'2026-10-19',entry_limit=1,total_limit=1)
            budget.reserve({'action_id':'prior','kind':'entry'});budget.close()
            budget=WorkerBudget(path,'2026-10-19',entry_limit=1,total_limit=1);book=MagicMock()
            result=AsyncDispatcher(book,lambda _:self.fail('no model'),directory,budget)()
            self.assertEqual(result['reason'],'MODEL_BUDGET_EXHAUSTED_MANAGEMENT_PENDING')
            book.claim.assert_not_called();book.report.assert_not_called();budget.close()

    def test_entry_budget_preserves_remaining_management_reserve(self):
        with tempfile.TemporaryDirectory() as directory:
            budget=WorkerBudget(Path(directory)/'budget.sqlite','2026-10-19',entry_limit=1,total_limit=2)
            self.assertTrue(budget.reserve({'action_id':'entry','kind':'entry'}))
            self.assertFalse(budget.status()['entries_available']);self.assertTrue(budget.status()['management_available'])
            self.assertTrue(budget.reserve({'action_id':'protect','kind':'protect'}));self.assertFalse(budget.reserve({'action_id':'protect','kind':'protect'}));budget.close()

    def test_background_task_rejects_second_owner_even_result_not_consumed(self):
        task=BackgroundTask('single');task.start(lambda:1);task.thread.join(.5)
        with self.assertRaises(ValueError):task.start(lambda:2)
        self.assertEqual(task.take(),(True,1))

    def test_broker_wait_ignores_unconsumed_market_result_but_wakes_on_broker_completion(self):
        from martin_luk.live_session import wait_for_result_or_tick
        broker=BackgroundTask('broker-wait');market=BackgroundTask('market-ready')
        release=threading.Event()
        broker.start(lambda:release.wait(1));market.start(lambda:'ready');market.thread.join(.5)
        try:
            begin=time.monotonic()
            wait_for_result_or_tick(broker,None,.05)
            self.assertGreaterEqual(time.monotonic()-begin,.04)
            self.assertFalse(market.mailbox.empty())
            release.set();broker.thread.join(.5)
            begin=time.monotonic();wait_for_result_or_tick(broker,None,.5)
            self.assertLess(time.monotonic()-begin,.1)
            self.assertEqual(broker.take(),(True,True))
        finally:
            release.set()
            if broker.thread:broker.thread.join(.5)
            market.take()

    def test_live_controller_ticks_during_real_market_and_agent_blocking(self):
        from contextlib import ExitStack
        from datetime import timedelta
        from martin_luk.live_session import run_live_session
        blocked=threading.Event();release=threading.Event();worker_started=threading.Event()
        baseline=datetime.fromisoformat('2026-10-19T14:00:00+00:00');real_start=time.monotonic()
        clock=lambda:baseline+timedelta(seconds=time.monotonic()-real_start)
        def slow_market(*args,**kwargs):
            blocked.set();release.wait(2);return [],{}
        def slow_worker(action):
            worker_started.set();release.wait(2);return {'status':'review_only','message':'review only'}
        journal=MagicMock();journal.summary.return_value={'read_healthy':True,'reconciliation_issues':[],
            'positions':{},'open_equity_orders':0,'pending_approvals':0}
        with tempfile.TemporaryDirectory() as directory,ExitStack() as stack:
            policy={'session_date':'2026-10-19'}
            # The default path now owns a durable strategy queue, so the ledger
            # double needs a real SQLite handle, policy and clock. Book behaviour
            # under test (claim/reconcile/report) stays mocked; only the storage
            # contract the queue depends on becomes real.
            store=sqlite3.connect(Path(directory)/'store.sqlite',isolation_level=None)
            book=MagicMock();book.summary.return_value={'state':{'issues':[],'trades':{}}};book.pending_approval_ids.return_value=[];book.recover.return_value={}
            book.claim.side_effect=[{'action_id':'protect-one','kind':'protect','synthetic':False},None]
            book.db=store;book.policy=policy;book.clock=clock;book.simulation=True
            def begin():
                store.execute('BEGIN IMMEDIATE')
            book._begin=begin
            target='martin_agentic.live_session.'
            for name,value in [('single_instance',MagicMock()),('ExecutionBook',lambda *a,**kw:book),
                    ('BrokerJournal',lambda *a,**kw:journal),
                    ('CodexOrderWorker',lambda *a,**kw:slow_worker),('collect_live_events',slow_market),
                    ('_broker_read',lambda *a,**kw:__import__('test_live_session').full_snapshot(clock().isoformat())),('notify_local',lambda *a:None),('print',lambda *a,**kw:None)]:
                stack.enter_context(patch(target+name,value))
            # Keep real blocking tasks and real wall time. Only scale the poll interval.
            real_sleep=time.sleep
            stack.enter_context(patch(target+'wait_for_result_or_tick',lambda market,dispatcher,seconds,**kwargs:real_sleep(.02)))
            result=run_live_session(Path(directory)/'book.sqlite',policy,symbols=[str(i) for i in range(10)],
                cycles=5,agent_mode='review',status_file=Path(directory)/'status.json',output_dir=directory,clock=clock)
            # The loop under test still runs the default strategy-process path:
            # the queue reported progress and shut down cleanly.
            self.assertTrue(result['strategy_queue']['enabled'])
            self.assertTrue(result['strategy_shutdown']['closed'])
            self.assertTrue(blocked.is_set());self.assertTrue(worker_started.is_set())
            self.assertEqual(book.reconcile.call_count,5);self.assertEqual(book.claim.call_count,1)
            self.assertEqual(result['cycle'],5);self.assertTrue(result['market_task']['thread_alive'])
            self.assertTrue(result['worker_task']['thread_alive'])
            # Final bounded drain must neither return immediately nor wait
            # indefinitely when the read ignores cancellation.
            self.assertGreaterEqual(time.monotonic()-real_start,1.9)
            self.assertLess(time.monotonic()-real_start,5)
            book.report.assert_not_called();release.set();real_sleep(.03)
            store.close()

    def test_restart_with_live_or_completed_old_worker_never_retries_unknown(self):
        from test_execution import ExecutionTests
        fixture=ExecutionTests();fixture.setUp();release=threading.Event();started=threading.Event()
        budget=WorkerBudget(fixture.path,fixture.policy['session_date'])
        def slow(action):
            started.set();release.wait(1);return {'status':'unknown','message':'write outcome ambiguous'}
        try:
            fixture.propose();old=AsyncDispatcher(fixture.book,slow,fixture.tmp.name,budget);old()
            self.assertTrue(started.wait(.5));fixture.book.recover()
            self.assertEqual(fixture.book.actions()[0]['state'],'UNKNOWN')
            new=AsyncDispatcher(fixture.book,lambda _:self.fail('must not retry'),fixture.tmp.name,budget)
            self.assertEqual(new()['reason'],'WORKER_OWNER_STILL_ACTIVE');release.set();old.task.thread.join(.5)
            self.assertEqual(new()['reason'],'NO_DISPATCHABLE_ACTION')
            completed=old()['completed'];self.assertFalse(completed['report_applied'])
            self.assertTrue(Path(completed['receipt_file']).is_file());self.assertEqual(fixture.book.actions()[0]['state'],'UNKNOWN')
            from martin_agentic.control_tasks import inspect_worker_receipts
            inventory=inspect_worker_receipts(fixture.book,fixture.tmp.name)
            self.assertTrue(inventory['receipts'][0]['valid_binding']);self.assertFalse(inventory['automatic_replay_allowed'])
            self.assertEqual(fixture.book.actions()[0]['state'],'UNKNOWN')
            receipt=Path(completed['receipt_file']);value=json.loads(receipt.read_text());value['binding']['db']='foreign-book';receipt.write_text(json.dumps(value))
            self.assertFalse(inspect_worker_receipts(fixture.book,fixture.tmp.name)['receipts'][0]['valid_binding'])
        finally:release.set();budget.close();fixture.tearDown()

    def test_codex_worker_stop_terminates_owned_child_before_lease_release(self):
        from martin_agentic.agent_worker import CodexOrderWorker
        with tempfile.TemporaryDirectory() as directory:
            binary=Path(directory)/'blocked-worker';binary.write_text('#!/bin/sh\nexec /bin/sleep 30\n');binary.chmod(0o700)
            worker=CodexOrderWorker('.',directory,binary=str(binary));result=[]
            thread=threading.Thread(target=lambda:result.append(worker({'kind':'probe','synthetic':False})))
            thread.start();deadline=time.monotonic()+1
            while worker._process is None and time.monotonic()<deadline:time.sleep(.01)
            process=worker._process;self.assertIsNotNone(process);self.assertTrue(thread.is_alive())
            begin=time.monotonic();worker.stop();thread.join(1)
            self.assertFalse(thread.is_alive());self.assertIsNotNone(process.poll());self.assertLess(time.monotonic()-begin,1)
            self.assertEqual(result[0]['status'],'unknown')

    def test_model_reservation_records_real_wallclock_once_across_restarts(self):
        with tempfile.TemporaryDirectory() as directory:
            p=Path(directory)/'budget.sqlite';before=datetime.now(timezone.utc)
            budget=WorkerBudget(p,'2026-10-19');self.assertTrue(budget.reserve({'action_id':'one','kind':'protect'}))
            reserved=budget.status()['last_reserved_at'];self.assertGreaterEqual(datetime.fromisoformat(reserved),before);budget.close()
            budget=WorkerBudget(p,'2026-10-19');self.assertFalse(budget.reserve({'action_id':'one','kind':'protect'}))
            self.assertEqual(budget.status()['last_reserved_at'],reserved);self.assertEqual(budget.status()['total_calls_reserved'],1);budget.close()


class BoundedReadShutdown034r1Tests(TestCase):
    def test_shutdown_deadline_starts_before_slow_cancel(self):
        from martin_luk.live_session import _drain_read_task
        class SlowCancel:
            def cancel(self):
                time.sleep(.4)
        task=BackgroundTask('slow-cancel');task.start(lambda:1);time.sleep(.01)
        begin=time.monotonic()
        drained=_drain_read_task(task,SlowCancel(),grace=.05)
        elapsed=time.monotonic()-begin
        self.assertLess(elapsed,.2)
        self.assertFalse(drained['drained'])
        self.assertFalse(drained['cancel']['done'])
        self.assertEqual(drained['cancel']['contract'],'COMPATIBILITY_THREAD')

    def test_deadline_aware_cancel_that_blocks_is_still_bounded(self):
        from martin_luk.live_session import _drain_read_task
        class SlowCancel:
            def cancel(self,*,deadline_monotonic=None):
                time.sleep(.4)
        task=BackgroundTask('slow-deadline-cancel');task.start(lambda:1);time.sleep(.01)
        begin=time.monotonic()
        drained=_drain_read_task(task,SlowCancel(),grace=.05)
        self.assertLess(time.monotonic()-begin,.2)
        self.assertFalse(drained['drained'])
        self.assertTrue(drained['cancel']['cancel_thread_alive'])
        self.assertIsNotNone(drained['cancel']['cancel_thread_id'])
        self.assertEqual(drained['cancel']['contract'],'DEADLINE')

    def test_failing_cancel_is_reported_as_a_failed_drain(self):
        from martin_luk.live_session import _drain_read_task
        class FailingCancel:
            def cancel(self,*,deadline_monotonic=None):
                raise RuntimeError('synthetic cancellation failure')
        task=BackgroundTask('failing-cancel');task.start(lambda:1);time.sleep(.01)
        drained=_drain_read_task(task,FailingCancel(),grace=.1)
        self.assertFalse(drained['drained'])
        self.assertEqual(drained['cancel']['error'],'RuntimeError')
        self.assertEqual(drained['cancel']['contract'],'DEADLINE')

    def test_read_thread_that_does_not_exit_fails_the_drain(self):
        from martin_luk.live_session import _drain_read_task
        class Cancel:
            def cancel(self):return {'rpc_cleanup':None}
        release=threading.Event();task=BackgroundTask('non-exiting')
        task.start(lambda:release.wait(2))
        begin=time.monotonic()
        drained=_drain_read_task(task,Cancel(),grace=.1)
        elapsed=time.monotonic()-begin
        try:
            self.assertLess(elapsed,.3)
            self.assertFalse(drained['drained'])
            self.assertTrue(drained['thread_alive'])
            self.assertEqual(drained['resource']['task'],'non-exiting')
            self.assertIsNotNone(drained['resource']['thread_id'])
        finally:release.set();task.thread.join(1)

    def test_term_unresponsive_child_exhausting_budget_is_reported_not_ignored(self):
        import sys
        from martin_luk.live_session import _drain_read_task
        from martin_agentic.transport import StdioRpc
        source=('import signal,time;'
                'signal.signal(signal.SIGTERM,signal.SIG_IGN);time.sleep(30)')
        rpc=StdioRpc([sys.executable,'-u','-c',source],timeout=5)
        owner=ReadTransportOwner();owner.attach(rpc);task=BackgroundTask('term-unresponsive')
        task.start(rpc.request,'blocked',{});time.sleep(.15)
        try:
            begin=time.monotonic()
            drained=_drain_read_task(task,owner,
                deadline_monotonic=time.monotonic()-1)
            elapsed=time.monotonic()-begin
            self.assertLess(elapsed,.1)
            self.assertFalse(drained['drained'])
            self.assertIsNotNone(drained['cleanup'])
            self.assertFalse(drained['cleanup']['rpc_cleanup']['complete'])
            self.assertTrue(drained['cleanup']['rpc_cleanup']['budget_exhausted'])
            self.assertTrue(drained['thread_alive'])
            # An explicit later cleanup may finish; it is not silently claimed
            # by the exhausted shutdown budget.
            retry=rpc.close(deadline_monotonic=time.monotonic()+2)
            self.assertTrue(retry['complete']);self.assertIsNotNone(rpc.process.poll())
            self.assertTrue(retry['pipes_closed'])
            task.thread.join(1)
            self.assertFalse(task.thread.is_alive())
            completed=task.take()
            self.assertIsNotNone(completed);self.assertFalse(completed[0])
        finally:
            rpc.close(deadline_monotonic=time.monotonic()+2)

    def test_normal_drain_reclaims_process_pipes_and_readers(self):
        import sys
        from martin_luk.live_session import _drain_read_task
        from martin_agentic.transport import StdioRpc
        source=('import signal,time;'
                'signal.signal(signal.SIGTERM,signal.SIG_IGN);time.sleep(30)')
        rpc=StdioRpc([sys.executable,'-u','-c',source],timeout=5)
        owner=ReadTransportOwner();owner.attach(rpc);task=BackgroundTask('normal-drain')
        task.start(rpc.request,'blocked',{});time.sleep(.15)
        try:
            begin=time.monotonic()
            drained=_drain_read_task(task,owner,grace=1.0)
            self.assertGreaterEqual(time.monotonic()-begin,.4)
            self.assertLess(time.monotonic()-begin,1.2)
            self.assertTrue(drained['drained'])
            self.assertFalse(drained['status']['thread_alive'])
            self.assertTrue(drained['cleanup']['rpc_cleanup']['complete'])
            self.assertFalse(drained['cleanup']['rpc_cleanup']['process_alive'])
            self.assertFalse(drained['cleanup']['rpc_cleanup']['reader_alive'])
            self.assertFalse(drained['cleanup']['rpc_cleanup']['stderr_reader_alive'])
            self.assertTrue(drained['cleanup']['rpc_cleanup']['pipes_closed'])
            self.assertFalse(drained['result'][0])
        finally:
            rpc.close(deadline_monotonic=time.monotonic()+2)

    def test_late_registration_is_closed_under_the_original_shutdown_deadline(self):
        import sys
        from martin_agentic.transport import StdioRpc
        owner=ReadTransportOwner();owner.cancel(deadline_monotonic=time.monotonic()+.5)
        rpc=StdioRpc([sys.executable,'-u','-c','import time;time.sleep(30)'],timeout=5)
        try:
            with self.assertRaisesRegex(RuntimeError,'READ_ONLY_COLLECTION_STOPPED'):
                owner.attach(rpc)
            cleanup=owner.cleanup_status()
            self.assertTrue(cleanup['late_registration']['complete'])
            self.assertIsNotNone(rpc.process.poll())
            self.assertTrue(all(s.closed for s in (rpc.process.stdin,rpc.process.stdout,rpc.process.stderr)))
        finally:rpc.close(deadline_monotonic=time.monotonic()+2)
