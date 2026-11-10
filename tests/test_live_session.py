from datetime import datetime
import json
import sqlite3
from pathlib import Path
import tempfile
from unittest import TestCase

from entry_authority_fixture import TestExecutionBook as ExecutionBook
from martin_luk.live_session import LiveSupervisor
from shadow_fixture import events
from test_broker import snapshot


class LiveSupervisorTests(TestCase):
    def test_engineering_session_keeps_strategy_candidate_without_creating_strategy_entry(self):
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as directory:
            policy=json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text());policy['engineering_only']=True
            now=datetime.fromisoformat('2026-10-16T14:00:00+00:00')
            book=ExecutionBook(Path(directory)/'engineering.sqlite',policy,simulation=True,clock=lambda:now)
            try:
                supervisor=LiveSupervisor(book)
                state=supervisor.signals.state();state['pending_signals']={'candidate':{'id':'candidate'}}
                book.db.execute('UPDATE live_strategy SET payload=? WHERE id=1',(json.dumps(state),));book.db.commit()
                observed=snapshot(at=now.isoformat());observed['synthetic']=True
                with patch.object(book,'propose',side_effect=AssertionError('engineering session cannot admit strategy')) as proposed:
                    supervisor.step(observed,[],allow_entries=True)
                proposed.assert_not_called();self.assertEqual(book.actions(),[])
                self.assertEqual(supervisor.signals.pending(),[{'id':'candidate'}])
            finally:book.close()

    def test_capture_management_keeps_a_durable_strategy_candidate_without_proposing_or_dispatching_it(self):
        from unittest.mock import Mock,patch
        with tempfile.TemporaryDirectory() as directory:
            policy=json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text());policy['capture_only']=True
            now=datetime.fromisoformat('2026-10-16T14:00:00+00:00')
            book=ExecutionBook(Path(directory)/'capture.sqlite',policy,simulation=True,clock=lambda:now)
            worker=Mock(side_effect=AssertionError('capture cannot wake a model'))
            try:
                supervisor=LiveSupervisor(book,worker=worker)
                state=supervisor.signals.state();state['pending_signals']={'candidate':{'id':'candidate'}}
                book.db.execute('UPDATE live_strategy SET payload=? WHERE id=1',(json.dumps(state),));book.db.commit()
                observed=snapshot(at=now.isoformat());observed['synthetic']=True
                with patch.object(book,'propose',side_effect=AssertionError('candidate may be recorded but never admitted')) as propose:
                    result=supervisor.step(observed,[],allow_entries=False)
                propose.assert_not_called();worker.assert_not_called();self.assertEqual(book.actions(),[])
                self.assertEqual(supervisor.signals.pending(),[{'id':'candidate'}])
                self.assertTrue(result['new_entries_blocked'])
            finally:book.close()

    def test_preopen_real_book_reconciliation_keeps_unknown_and_ready_protect_without_claim(self):
        from unittest.mock import Mock,patch
        with tempfile.TemporaryDirectory() as directory:
            policy=json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
            now=datetime.fromisoformat('2026-10-16T13:00:00+00:00')
            book=ExecutionBook(Path(directory)/'fixture.sqlite',policy,simulation=True,clock=lambda:now)
            worker=Mock(side_effect=AssertionError('No model or native dispatch before regular hours'))
            try:
                protect=book._queue({'id':'retained-protect','plan':{'symbol':'DEMO'}},'protect',
                    {'symbol':'DEMO','quantity':'1','side':'sell'},now,'synthetic pending preopen')
                unknown=book._queue({'id':'retained-unknown','plan':{'symbol':'DEMO'}},'exit',
                    {'symbol':'DEMO','quantity':'1','side':'sell'},now,'synthetic interrupted prior action')
                book.db.execute("UPDATE live_actions SET state='UNKNOWN' WHERE id=?",(unknown,));book.db.commit()
                before={a['id']:a['state'] for a in book.actions()}
                supervisor=LiveSupervisor(book,worker=worker)
                observed=snapshot(at=now.isoformat());observed['synthetic']=True
                with patch.object(book,'reconcile',wraps=book.reconcile) as reconcile,patch.object(book,'claim',side_effect=AssertionError('Do not expire preopen actions by claiming')) as claim:
                    result=supervisor.step(observed,[],allow_entries=False,allow_dispatch=False)
                reconcile.assert_called_once();claim.assert_not_called();worker.assert_not_called()
                self.assertEqual({a['id']:a['state'] for a in book.actions()},before)
                self.assertEqual(before,{protect:'READY',unknown:'UNKNOWN'})
                self.assertEqual(book.state()['last_snapshot'],now.isoformat())
                self.assertTrue(result['new_entries_blocked']);self.assertFalse(result['dispatch']['agent_called'])
            finally:book.close()

    def test_real_lifecycle_path_signal_to_outbox_to_agent_without_paper_backend(self):
        with tempfile.TemporaryDirectory() as directory:
            policy=json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text());policy['session_date']='2026-10-15'
            clock=[datetime.fromisoformat('2026-10-15T09:25:00-04:00')]
            book=ExecutionBook(Path(directory)/'live.sqlite',policy,simulation=True,clock=lambda:clock[0])
            calls=[]
            def worker(action):
                calls.append(action);return {'status':'needs_confirmation','message':'specific order confirmation required'}
            supervisor=LiveSupervisor(book,worker=worker)
            try:
                results=[]
                for event in events()[1:5]:
                    clock[0]=datetime.fromisoformat(event['at'])
                    broker=snapshot(at=clock[0].isoformat());broker['synthetic']=True
                    result=supervisor.step(broker,[{**event,'synthetic_input':True,'source_sha256':'fixture-'+event['id']}])
                    results.append(result)
                self.assertEqual(len(calls),1)
                self.assertEqual(calls[0]['kind'],'entry')
                self.assertTrue(results[-1]['dispatch']['agent_called'])
                self.assertEqual(book.actions()[0]['state'],'WAITING_USER')
                self.assertEqual(book.state()['trades'][calls[0]['trade_id']]['quantity'],'0')
                supervisor.step(broker,[])
                self.assertEqual(len(calls),1)
            finally:book.close()


def full_snapshot(at):
    from test_broker import FakeBroker, snapshot as broker_snapshot
    value = broker_snapshot(FakeBroker(), at)
    value['synthetic'] = True
    return value


class LiveSessionRecoveryTests(TestCase):
    def run_session(self, *, at='2026-10-16T14:00:00+00:00', cycles=2,
                    market_error=None, broker_error=None, positions=None, events=None,
                    require_watchdog=False,guardian_results=None,inhibit_error=None,capture_only=False,lineage_gate=None,until_close=False,tick_seconds=None):
        from contextlib import ExitStack
        from unittest.mock import patch, MagicMock
        from martin_luk.live_session import run_live_session
        policy=json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        if capture_only:policy['capture_only']=True
        self.book=MagicMock();self.book.summary.return_value={'state':{'issues':[]}}
        # The close path now reads the durable integrity authority; a mock
        # needs a real, empty connection so "no fault" is a fact, not a guess.
        self.book.db=sqlite3.connect(':memory:',isolation_level='')
        self.addCleanup(self.book.db.close);self.book.db_path=None
        if lineage_gate is not None:
            self.book.simulation=False;self.book.policy=policy;self.book.entry_gate.return_value=lineage_gate
        self.book.active_issues.return_value=[]
        self.book.recovery_status.return_value={'acknowledged_trade_ids':[],'acknowledged_action_ids':[],'covered_issue_ids':[],'blockers':[]}
        self.book.recover.return_value={'recovery_disposition':'NO_PENDING_OPERATIONS','native_operations_pending':[]}
        self.book.pending_approval_ids.return_value=[]
        self.journal=MagicMock();self.journal.summary.return_value={
            'read_healthy':True,'reconciliation_issues':[],'blockers':[],
            'positions':positions or {},'open_equity_orders':0,'other_asset_open_orders':0,'pending_approvals':0}
        self.supervisor=MagicMock()
        self.supervisor.step.side_effect=lambda snapshot,events,**kwargs:{'lifecycle':{},'new_entries_blocked':not kwargs.get('allow_entries',True)}
        self.now=datetime.fromisoformat(at)
        with ExitStack() as stack:
            def patcher(name,**kwargs):return stack.enter_context(patch('martin_agentic.live_session.'+name,**kwargs))
            patcher('single_instance');patcher('ExecutionBook',return_value=self.book)
            patcher('BrokerJournal',return_value=self.journal)
            stack.enter_context(patch('martin_luk.live_session.LiveSupervisor',return_value=self.supervisor))
            self.read=patcher('_broker_read',side_effect=broker_error if broker_error is not None else lambda *a,**k:full_snapshot(self.now.isoformat()))
            self.market=patcher('collect_live_events',return_value=(events or [],{'quotes':10}),side_effect=market_error)
            patcher('notify_local');patcher('time.sleep');patcher('print')
            self.capture_disposition=patcher('record_capture_disposition')
            if lineage_gate is not None:
                stack.enter_context(patch('martin_agentic.session_lineage.lineage_approval_ids',return_value=[]))
            self.cycle_wait=patcher('wait_for_result_or_tick')
            if tick_seconds is not None:
                from datetime import timedelta
                def tick(*args,**kwargs):self.now+=timedelta(seconds=tick_seconds)
                self.cycle_wait.side_effect=tick
            if guardian_results is not None:
                stack.enter_context(patch('martin_agentic.watchdog.watchdog_entry_admission',side_effect=guardian_results))
            if inhibit_error is not None:
                stack.enter_context(patch('martin_agentic.watchdog.inhibit_new_entries',side_effect=inhibit_error))
            with tempfile.TemporaryDirectory() as directory:
                status=Path(directory)/'status.json'
                result=run_live_session(Path(directory)/'db',policy,symbols=[str(i) for i in range(10)],
                    cycles=cycles,status_file=status,clock=lambda:self.now,
                    require_watchdog=require_watchdog,watchdog_policy_path='tests/fixtures/policies/live_trial_2026-10-16.json',until_close=until_close)
                self.persisted=json.loads(status.read_text())
                return result

    def test_required_observer_missing_at_startup_blocks_only_new_risk_and_keeps_management_ticks(self):
        result=self.run_session(require_watchdog=True,cycles=3)
        self.assertEqual(self.read.call_count,3);self.assertEqual(self.supervisor.step.call_count,3)
        self.assertTrue(all(not c.kwargs['allow_entries'] for c in self.supervisor.step.call_args_list))
        self.assertFalse(result['watchdog_guard']['ready']);self.assertTrue(result['entry_inhibit']['blocked'])
        self.assertEqual(result['reason'],'WATCHDOG_UNAVAILABLE_MANAGING_EXISTING')

    def test_until_close_runs_past_cycle_limit_and_requires_actual_close_observation(self):
        reads=[]
        def broker(*args,**kwargs):
            reads.append(1)
            if len(reads)==2:self.now=datetime.fromisoformat('2026-10-16T20:00:01+00:00')
            return full_snapshot(self.now.isoformat())
        result=self.run_session(cycles=1,until_close=True,broker_error=broker)
        self.assertEqual(len(reads),2)
        self.assertEqual(result['reason'],'AUTHORIZED_SESSION_CLOSED')
        self.assertFalse(result['final_flat_verified'])
        self.assertTrue(result['supported_scope_flat'])
        self.assertFalse(result['full_account_flat_proven'])
        self.assertEqual(result['close_disposition'],
                         'SUPPORTED_SCOPE_FLAT_FULL_ACCOUNT_UNPROVEN')
        self.assertEqual(result['session_run_mode'],'until_authorized_close')

    def test_until_close_hung_account_read_has_explicit_unverified_close_handoff(self):
        import threading
        release=threading.Event();threads=[]
        def broker(*args,**kwargs):
            threads.append(threading.current_thread())
            self.now=datetime.fromisoformat('2026-10-16T20:00:31+00:00')
            release.wait(2)
            return full_snapshot(self.now.isoformat())
        try:
            result=self.run_session(cycles=1,until_close=True,broker_error=broker)
            self.assertEqual(self.read.call_count,1)
            self.assertEqual(result['reason'],'CLOSE_BROKER_CONFIRMATION_DEADLINE_EXCEEDED')
            self.assertFalse(result['final_flat_verified']);self.assertFalse(result['healthy'])
            self.assertTrue(result['attention_required'])
            self.assertEqual(result['close_disposition'],'MANUAL_INTERVENTION_REQUIRED')
        finally:
            release.set()
            for thread in threads:thread.join(timeout=1)

    def test_capture_until_close_requires_positive_flat_proof_for_success_health(self):
        result=self.run_session(at='2026-10-16T20:00:01+00:00',until_close=True,capture_only=True)
        self.assertEqual(result['reason'],'AUTHORIZED_SESSION_CLOSED')
        self.assertTrue(result['final_broker_check_performed'])
        self.assertFalse(result['final_flat_verified'])
        self.assertTrue(result['supported_scope_flat'])
        self.assertFalse(result['full_account_flat_proven'])
        self.assertTrue(result['new_entries_blocked'])

    def test_until_close_does_not_roll_over_to_a_new_policy_date(self):
        result=self.run_session(at='2026-10-19T14:00:00+00:00',until_close=True)
        self.read.assert_not_called();self.market.assert_not_called()
        self.assertEqual(result['reason'],'OUTSIDE_AUTHORIZED_SESSION')

    def test_capture_only_keeps_entry_inhibit_without_making_healthy_reads_require_release(self):
        ready={'required':True,'ready':True,'issues':[]}
        dead={'required':True,'ready':False,'issues':['WATCHDOG_PROCESS_NOT_VERIFIED']}
        result=self.run_session(require_watchdog=True,capture_only=True,cycles=2,guardian_results=[dead,ready])
        self.assertTrue(result['entry_inhibit']['blocked'])
        self.assertTrue(result['read_only_capture_healthy']);self.assertTrue(result['operational_healthy'])
        self.assertTrue(result['new_entries_blocked']);self.assertFalse(result['attention_required'])
        self.assertTrue(all(not call.kwargs['allow_entries'] for call in self.supervisor.step.call_args_list))

    def test_blocked_broker_read_keeps_ticks_without_duplicate_read_or_old_snapshot_dispatch(self):
        import threading
        import time
        release=threading.Event();finished=threading.Event();threads=[]
        owner=threading.get_ident()
        def blocked(*args,**kwargs):
            threads.append(threading.get_ident())
            try:
                release.wait(60)
                return full_snapshot(self.now.isoformat())
            finally:finished.set()
        begin=time.monotonic()
        try:
            result=self.run_session(cycles=4,broker_error=blocked)
            self.assertEqual(result['cycle'],4)
            self.assertEqual(result['reason'],'BROKER_COLLECTION_IN_FLIGHT')
            self.assertGreaterEqual(time.monotonic()-begin,1.9)
            self.assertLess(time.monotonic()-begin,5)
            self.assertEqual(self.read.call_count,1)
            self.assertEqual(len(threads),1);self.assertNotEqual(threads[0],owner)
            self.journal.ingest.assert_not_called();self.supervisor.step.assert_not_called()
            self.book.claim.assert_not_called();self.market.assert_not_called()
            self.assertFalse(result['broker_snapshot_reused_for_dispatch'])
            self.assertTrue(result['new_entries_blocked']);self.assertTrue(result['attention_required'])
            self.assertIn('NO_CLOSE_PROOF',result['stop_disposition'])
            self.assertFalse(result['current_observation_healthy'])
            self.assertFalse(result['operational_healthy'])
            self.assertEqual(result['model_budget']['total_calls_reserved'],0)
            self.assertEqual(self.cycle_wait.call_count,3)
            for call in self.cycle_wait.call_args_list:
                self.assertNotIn('other_tasks',call.kwargs)
                self.assertEqual(call.args[0].name,'broker-read-only')
            self.assertIn('control_heartbeat_at',self.persisted)
        finally:
            release.set();self.assertTrue(finished.wait(1))

    def test_slow_broker_read_finishing_during_bounded_drain_is_not_failed_as_inflight(self):
        import threading
        release=threading.Event()
        def slow(*args,**kwargs):
            release.wait(0.5)
            return full_snapshot(self.now.isoformat())
        result=self.run_session(cycles=1,broker_error=slow)
        self.assertEqual(result['broker_read_cleanup']['thread_alive'],False)
        self.assertFalse(result['broker_read_cleanup'].get('drain_grace_seconds',0)>0
                         and result['broker_read_cleanup'].get('running'))
        self.assertNotIn('READ_ONLY_BROKER_READ_IN_FLIGHT_NO_CLOSE_PROOF',
                         result.get('stop_disposition',''))
        # The read finished during shutdown, not inside a cycle: it must be
        # retained as evidence but never counted as a completed observation.
        self.assertFalse(result['bounded_capture_completed'])
        self.assertEqual(result['completed_observation_cycles'], 0)
        self.assertFalse(result.get('read_only_capture_healthy'))

    def test_session_shutdown_reaps_actual_market_read_child_without_broker_writes(self):
        import sys
        from martin_agentic.control_tasks import owned_read_transport
        from martin_agentic.transport import StdioRpc
        children=[]
        class Transport:
            def __init__(self):
                self.rpc=StdioRpc([sys.executable,'-u','-c','import time;time.sleep(30)'],timeout=5)
                children.append(self.rpc)
            def __enter__(self):return self
            def __exit__(self,*args):self.rpc.close()
        def blocked_market(*args,**kwargs):
            with owned_read_transport(Transport,kwargs['cancellation']) as transport:
                transport.rpc.request('blocked',{})
            return [],{}
        try:
            result=self.run_session(cycles=2,market_error=blocked_market)
            self.assertEqual(len(children),1);self.assertIsNotNone(children[0].process.poll())
            self.assertFalse(result['market_read_cleanup']['thread_alive'])
            self.assertFalse(result.get('market_read_cleanup_incomplete',False))
            self.book.claim.assert_not_called()
            self.assertEqual(result['model_budget']['total_calls_reserved'],0)
        finally:
            for rpc in children:rpc.close()

    def test_shutdown_preserves_unconsumed_market_failure_and_success_dispositions(self):
        from unittest.mock import patch
        from martin_agentic.control_tasks import BackgroundTask
        class DeferredMarket(BackgroundTask):
            def __init__(self,name):super().__init__(name);self.deferred=False
            def take(self):
                if self.name=='market-read-only' and not self.deferred:
                    self.deferred=True
                    return None
                return super().take()
        with patch('martin_agentic.live_session.BackgroundTask',DeferredMarket):
            result=self.run_session(cycles=1,market_error=ValueError('sensitive upstream text'))
        self.assertEqual(result['market_shutdown_failure']['code'],'BROKER_READ_UNCLASSIFIED')
        self.assertTrue(result['market_shutdown_failure']['result_ready_before_stop'])
        self.assertTrue(result['attention_required']);self.assertFalse(result['healthy'])
        self.assertNotIn('sensitive upstream text',json.dumps(result))
        self.assertFalse(result['current_observation_healthy'])
        self.assertFalse(result['operational_healthy'])
        self.assertEqual(self.capture_disposition.call_args.kwargs['reason'],'MARKET_SHUTDOWN_RESULT')
        self.assertEqual(self.capture_disposition.call_args.args[1]['source_type'],'local_control_diagnostic')
        event={'id':'retained','kind':'quote','data':{}}
        with patch('martin_agentic.live_session.BackgroundTask',DeferredMarket):
            result=self.run_session(cycles=1,events=[event])
        self.assertEqual(result['market_shutdown_events_archived'],1)
        self.assertFalse(result['current_observation_healthy'])
        self.assertFalse(result['operational_healthy'])
        self.capture_disposition.assert_called_once_with(self.book,event,reason='SESSION_STOPPED_BEFORE_CONSUMPTION')

    def test_observer_death_blocks_entries_and_fresh_observer_does_not_auto_release_sticky_gate(self):
        ready={'required':True,'ready':True,'issues':[]}
        dead={'required':True,'ready':False,'issues':['WATCHDOG_PROCESS_NOT_VERIFIED']}
        result=self.run_session(require_watchdog=True,cycles=3,guardian_results=[ready,dead,ready])
        first,second,third=self.supervisor.step.call_args_list
        self.assertTrue(first.kwargs['allow_entries']);self.assertFalse(second.kwargs['allow_entries'])
        self.assertFalse(third.kwargs['allow_entries']);self.assertTrue(result['entry_inhibit']['blocked'])
        self.assertEqual(self.read.call_count,3)

    def test_failed_durable_guard_inhibit_completes_management_then_stops_for_handoff(self):
        dead={'required':True,'ready':False,'issues':['WATCHDOG_PROCESS_NOT_VERIFIED']}
        result=self.run_session(require_watchdog=True,cycles=3,guardian_results=[dead],inhibit_error=OSError('fixture disk unavailable'))
        self.assertEqual(self.read.call_count,1);self.supervisor.step.assert_called_once()
        self.assertFalse(self.supervisor.step.call_args.kwargs['allow_entries'])
        self.assertTrue(result['new_entries_blocked']);self.assertTrue(result['attention_required'])
        self.assertIn('MANUAL_HANDOFF_REQUIRED',result['stop_disposition'])
        self.assertIn('WATCHDOG_ENTRY_INHIBIT_WRITE_FAILED',result['watchdog_guard']['issues'])

    def test_preopen_flat_authenticates_and_reconciles_before_waiting_without_market_or_dispatch(self):
        at='2026-10-16T13:00:00+00:00'
        result=self.run_session(at=at,cycles=2,require_watchdog=True)
        self.assertEqual(self.read.call_count,2);self.assertEqual(self.journal.ingest.call_count,2)
        self.assertEqual(self.supervisor.step.call_count,2);self.market.assert_not_called()
        for call in self.supervisor.step.call_args_list:
            self.assertEqual(call.args[1],[]);self.assertFalse(call.kwargs['allow_entries'])
            self.assertFalse(call.kwargs['allow_dispatch'])
        self.assertEqual(result['reason'],'WAITING_FOR_REGULAR_SESSION')
        self.assertEqual(result['observed_at'],at);self.assertTrue(result['healthy'])
        self.assertTrue(result['new_entries_blocked']);self.assertFalse(result['attention_required'])

    def test_preopen_exposure_is_actionable_even_while_entries_and_market_are_disabled(self):
        result=self.run_session(at='2026-10-16T13:00:00+00:00',cycles=1,positions={'SOFI':{'quantity':'1'}})
        self.assertFalse(result['healthy']);self.assertTrue(result['attention_required'])
        self.assertIn('PREOPEN_POSITION_REQUIRES_ATTENTION',result['preopen_attention_reasons'])
        self.market.assert_not_called();self.supervisor.step.assert_called_once()

    def test_preopen_auth_failure_is_detected_before_open_and_cannot_claim_freshness(self):
        from martin_agentic.transport import TransportError
        result=self.run_session(at='2026-10-16T13:00:00+00:00',cycles=3,
            broker_error=TransportError('INTERACTIVE_AUTH_OR_APPROVAL_REQUIRED'))
        self.read.assert_called_once();self.market.assert_not_called();self.supervisor.step.assert_not_called()
        self.assertFalse(result['healthy']);self.assertTrue(result['attention_required'])
        self.assertNotIn('observed_at',result)
        self.assertEqual(result['reason'],'PREOPEN_BROKER_OR_LIFECYCLE_UNAVAILABLE')

    def test_preopen_transient_read_failures_have_same_bounded_retry_and_no_management_on_missing_fact(self):
        from martin_agentic.transport import TransportError
        result=self.run_session(at='2026-10-16T13:00:00+00:00',cycles=5,
            broker_error=TransportError('APP_SERVER_DISCONNECTED'))
        self.assertEqual(self.read.call_count,3);self.supervisor.step.assert_not_called()
        self.assertTrue(result['attention_required']);self.assertTrue(result['new_entries_blocked'])

    def test_until_close_recovers_after_more_than_three_transient_reads_without_dispatching_stale_facts(self):
        from martin_agentic.transport import TransportError
        attempts=[]
        def read(*args,**kwargs):
            attempts.append(self.now)
            if len(attempts)<=5:raise TransportError('MCP_TOOL_DISCOVERY_FAILED')
            self.now=datetime.fromisoformat('2026-10-16T20:00:01+00:00')
            return full_snapshot(self.now.isoformat())
        result=self.run_session(until_close=True,broker_error=read,tick_seconds=5)
        self.assertIn(len(attempts),(6,7))
        self.assertGreaterEqual((attempts[4]-attempts[3]).total_seconds(),10)
        self.assertGreaterEqual((attempts[5]-attempts[4]).total_seconds(),20)
        self.assertEqual(result['reason'],'AUTHORIZED_SESSION_CLOSED')
        self.assertFalse(result['final_flat_verified'])
        self.assertTrue(result['supported_scope_flat'])
        self.assertTrue(all(not c.kwargs['allow_entries'] for c in self.supervisor.step.call_args_list))

    def test_until_close_auth_failure_still_stops_without_reconnect(self):
        from martin_agentic.transport import TransportError
        result=self.run_session(until_close=True,broker_error=TransportError('INTERACTIVE_AUTH_OR_APPROVAL_REQUIRED'))
        self.assertEqual(self.read.call_count,1)
        self.supervisor.step.assert_not_called()
        self.assertEqual(result['reason'],'BROKER_OR_LIFECYCLE_UNAVAILABLE')
        self.assertTrue(result['new_entries_blocked'])

    def test_until_close_preopen_outage_recovers_after_three_failures(self):
        from martin_agentic.transport import TransportError
        attempts=[]
        def read(*args,**kwargs):
            attempts.append(self.now)
            if len(attempts)<=4:raise TransportError('APP_SERVER_DISCONNECTED')
            self.now=datetime.fromisoformat('2026-10-16T20:00:01+00:00')
            return full_snapshot(self.now.isoformat())
        result=self.run_session(at='2026-10-16T13:00:00+00:00',until_close=True,broker_error=read,tick_seconds=5)
        self.assertIn(len(attempts),(5,6))
        self.assertFalse(result['final_flat_verified'])
        self.assertTrue(result['supported_scope_flat'])

    def test_capture_only_regular_data_loop_consumes_events_but_always_blocks_signal_admission(self):
        at='2026-10-16T14:00:00+00:00'
        events=[{'kind':'quote','data':{'symbol':'SOFI','bid':'10','ask':'10.01',
            **{key:at for key in ('bid_at','ask_at','observed_at')}}}]
        result=self.run_session(capture_only=True,events=events,cycles=2)
        self.assertEqual(self.market.call_count,2);self.assertEqual(self.read.call_count,2)
        for call in self.supervisor.step.call_args_list:
            self.assertEqual(call.args[1],events);self.assertFalse(call.kwargs['allow_entries'])
        self.assertTrue(result['read_only_capture']);self.assertTrue(result['read_only_capture_healthy'])
        self.assertFalse(result['healthy']);self.assertTrue(result['new_entries_blocked'])
        self.assertTrue(result['operational_healthy']);self.assertEqual(result['health_scope'],'read_only_capture')
        self.assertEqual(result['reason'],'READ_ONLY_CAPTURE_CYCLE_COMPLETED')
        self.assertTrue(result['bounded_capture_completed'])
        self.assertFalse(result['broker_close_or_flat_proof'])
        self.assertIsNotNone(result['last_completed_observation_at'])
        self.assertTrue(result['last_completed_observation_healthy'])
        self.assertTrue(result['current_observation_evidence_fresh'])
        self.assertTrue(result['current_observation_healthy'])
        self.assertFalse(result['broker_read_cleanup']['thread_alive'])
        self.assertFalse(result['market_read_cleanup']['thread_alive'])

    def test_capture_only_refuses_review_and_execute_before_creating_book_or_worker(self):
        from unittest.mock import patch
        from martin_luk.live_session import run_live_session
        policy=json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text());policy['capture_only']=True
        for mode in ('review','execute'):
            with patch('martin_agentic.live_session.ExecutionBook') as book,patch('martin_agentic.live_session.CodexOrderWorker') as worker:
                with self.assertRaisesRegex(ValueError,'CAPTURE_ONLY_REQUIRES_DISABLED_AGENT'):
                    run_live_session('never-created.sqlite',policy,symbols=[str(i) for i in range(10)],agent_mode=mode)
                book.assert_not_called();worker.assert_not_called()

    def test_market_timeout_keeps_broker_observation_and_management_running(self):
        from martin_agentic.transport import TransportError
        result=self.run_session(market_error=TransportError('APP_SERVER_TIMEOUT'))
        self.assertEqual(self.read.call_count,2)
        self.assertEqual(self.supervisor.step.call_count,2)
        for call in self.supervisor.step.call_args_list:
            self.assertEqual(call.args[1],[])
            self.assertFalse(call.kwargs['allow_entries'])
        self.assertFalse(result['healthy']);self.assertTrue(result['attention_required'])
        self.assertFalse(self.persisted['running'])

    def test_market_auth_pauses_market_reconnect_but_keeps_account_management(self):
        from martin_agentic.transport import TransportError
        self.run_session(market_error=TransportError('INTERACTIVE_AUTH_OR_APPROVAL_REQUIRED'),cycles=3)
        self.assertEqual(self.market.call_count,1)
        self.assertEqual(self.read.call_count,3)
        self.assertEqual(self.supervisor.step.call_count,3)

    def test_generic_market_read_failure_recovers_on_next_cycle(self):
        from martin_agentic.transport import TransportError
        at='2026-10-16T14:00:00+00:00'
        events=[{'kind':'quote','data':{'symbol':'SOFI','bid':'10','ask':'10.01',
            **{key:at for key in ('bid_at','ask_at','observed_at')}}}]
        result=self.run_session(market_error=[TransportError('ROBINHOOD_READ_FAILED'),(events,{'quotes':10})])
        self.assertEqual(self.market.call_count,2)
        self.assertEqual(self.read.call_count,2)
        first,second=self.supervisor.step.call_args_list
        self.assertFalse(first.kwargs['allow_entries']);self.assertEqual(first.args[1],[])
        self.assertTrue(second.kwargs['allow_entries']);self.assertEqual(second.args[1],events)
        self.assertTrue(result['healthy']);self.assertIsNone(result['market_failure'])
        self.assertFalse(result['new_entries_blocked'])
        self.assertEqual(result['reason'],'LIVE_CYCLE_COMPLETED')
        self.assertFalse(result['attention_required'])

    def test_repeated_generic_market_read_failure_remains_degraded_without_dispatching_entries(self):
        from martin_agentic.transport import TransportError
        result=self.run_session(market_error=TransportError('ROBINHOOD_READ_FAILED'),cycles=3)
        self.assertEqual(self.market.call_count,3)
        self.assertEqual(self.supervisor.step.call_count,3)
        self.assertTrue(all(not call.kwargs['allow_entries'] for call in self.supervisor.step.call_args_list))
        self.assertFalse(result['healthy']);self.assertTrue(result['new_entries_blocked'])
        self.assertEqual(result['market_failure']['code'],'ROBINHOOD_READ_FAILED')

    def test_broker_auth_failure_never_dispatches_and_stops(self):
        from martin_agentic.transport import TransportError
        result=self.run_session(broker_error=TransportError('INTERACTIVE_AUTH_OR_APPROVAL_REQUIRED'),cycles=3)
        self.assertEqual(self.read.call_count,1)
        self.supervisor.step.assert_not_called()
        self.assertTrue(result['attention_required'])

    def test_broker_disconnect_reconnect_is_bounded(self):
        from martin_agentic.transport import TransportError
        self.run_session(broker_error=TransportError('APP_SERVER_DISCONNECTED'),cycles=5)
        self.assertEqual(self.read.call_count,3)
        self.supervisor.step.assert_not_called()

    def test_close_requires_fresh_actual_broker_flat_check(self):
        result=self.run_session(at='2026-10-16T20:00:01+00:00')
        self.read.assert_called_once();self.market.assert_not_called()
        self.book.reconcile.assert_called_once()
        self.assertTrue(result['final_broker_check_performed'])
        self.assertFalse(result['final_flat_verified'])
        self.assertTrue(result['supported_scope_flat'])
        self.assertFalse(result['full_account_flat_proven'])
        self.assertEqual(result['close_disposition'],
                         'SUPPORTED_SCOPE_FLAT_FULL_ACCOUNT_UNPROVEN')
        self.supervisor.step.assert_not_called()

    def test_close_with_residual_position_requires_manual_intervention(self):
        result=self.run_session(at='2026-10-16T20:00:01+00:00',positions={'SOFI':{'quantity':'1'}})
        self.assertFalse(result['final_flat_verified'])
        self.assertTrue(result['attention_required'])
        self.assertEqual(result['close_disposition'],'MANUAL_INTERVENTION_REQUIRED')

    def test_close_read_failure_cannot_claim_safe_stop(self):
        from martin_agentic.transport import TransportError
        result=self.run_session(at='2026-10-16T20:00:01+00:00',broker_error=TransportError('APP_SERVER_TIMEOUT'))
        self.assertFalse(result['final_broker_check_performed'])
        self.assertFalse(result['final_flat_verified'])
        self.assertTrue(result['attention_required'])

    def test_wrong_day_never_calls_broker(self):
        result=self.run_session(at='2026-10-17T14:00:00+00:00')
        self.read.assert_not_called();self.supervisor.step.assert_not_called()
        self.assertEqual(result['reason'],'OUTSIDE_AUTHORIZED_SESSION')

    def test_management_only_supervisor_does_not_propose_or_consume_pending_entries(self):
        from unittest.mock import MagicMock, patch
        book=MagicMock();signals=MagicMock();signals.state.return_value={'ema9':{},'machines':{}}
        with patch('martin_luk.live_session.LiveSignals',return_value=signals),patch('martin_agentic.live_session.dispatch_one') as dispatch:
            supervisor=LiveSupervisor(book,worker=MagicMock())
            supervisor.step({'observed_at':'2026-10-16T14:00:00Z'},[],allow_entries=False)
            book.reconcile.assert_called_once_with({'observed_at':'2026-10-16T14:00:00Z'},quotes={},features={},exit_advices=None)
            book.propose.assert_not_called();signals.pending.assert_not_called()
            dispatch.assert_called_once_with(book,supervisor.worker,allow_entries=False)


    def test_stale_quote_is_removed_before_management_and_blocks_entries(self):
        quote={'symbol':'SOFI','bid':'10','ask':'10.01','bid_at':'2026-10-16T13:59:00+00:00',
            'ask_at':'2026-10-16T13:59:00+00:00','observed_at':'2026-10-16T13:59:00+00:00'}
        result=self.run_session(cycles=1,events=[{'kind':'quote','data':quote}])
        self.assertEqual(self.supervisor.step.call_args.args[1],[])
        self.assertFalse(self.supervisor.step.call_args.kwargs['allow_entries'])
        self.assertEqual(result['market_failure']['code'],'MARKET_QUOTE_STALE')
        self.capture_disposition.assert_called_once()
        self.assertEqual(self.capture_disposition.call_args.kwargs['reason'],'MARKET_QUOTE_STALE')

    def test_collection_crossing_close_never_dispatches_old_cycle(self):
        def crossing(*args,**kwargs):
            self.now=datetime.fromisoformat('2026-10-16T20:00:01+00:00')
            return [],{'quotes':10}
        result=self.run_session(at='2026-10-16T19:59:59+00:00',market_error=crossing)
        self.supervisor.step.assert_not_called()
        self.assertTrue(result['final_broker_check_performed'])
        self.assertFalse(result['final_flat_verified'])
        self.assertTrue(result['supported_scope_flat'])
        self.assertEqual(result['close_disposition'],
                         'SUPPORTED_SCOPE_FLAT_FULL_ACCOUNT_UNPROVEN')

    def test_bounded_stop_with_position_requires_explicit_handoff(self):
        result=self.run_session(cycles=1,positions={'SOFI':{'quantity':'1'}})
        self.assertTrue(result['attention_required'])
        self.assertEqual(result['stop_disposition'],'MANAGEMENT_STOPPED_MANUAL_HANDOFF_REQUIRED')
        self.assertEqual(self.persisted['stop_disposition'],result['stop_disposition'])

    def test_read_retry_only_reconnects_for_classified_transient_errors(self):
        from unittest.mock import patch
        from martin_luk.live_session import _broker_read
        from martin_agentic.transport import TransportError
        policy={'account_suffix':'0000'}
        with patch('martin_agentic.live_session.CodexBrokerReadTransport') as transport, \
             patch('martin_agentic.live_capture_bridge.SavedBundleObservationBridge') as bridge:
            bridge.return_value.read.side_effect=[TransportError('APP_SERVER_TIMEOUT'),{'ok':True}]
            self.assertEqual(_broker_read(policy,lambda:None),{'ok':True})
            self.assertEqual(transport.call_count,2)
            self.assertEqual(bridge.return_value.read.call_count,2)
        with patch('martin_agentic.live_session.CodexBrokerReadTransport') as transport, \
             patch('martin_agentic.live_capture_bridge.SavedBundleObservationBridge') as bridge:
            bridge.return_value.read.side_effect=TransportError('INTERACTIVE_AUTH_OR_APPROVAL_REQUIRED')
            with self.assertRaises(TransportError):_broker_read(policy,lambda:None)
            self.assertEqual(transport.call_count,1)

    def test_exact_approval_ids_survive_a_read_only_reconnect(self):
        from unittest.mock import patch
        from martin_luk.live_session import _broker_read
        from martin_agentic.transport import TransportError
        ids=['explicit-official-approval-id']
        with patch('martin_agentic.live_session.CodexBrokerReadTransport'),patch(
                'martin_agentic.live_capture_bridge.SavedBundleObservationBridge') as bridge:
            bridge.return_value.read.side_effect=[
                TransportError('APP_SERVER_TIMEOUT'),{'approval_states':{}}]
            self.assertEqual(_broker_read({'account_suffix':'0000'},lambda:None,approval_ids=ids),{'approval_states':{}})
            self.assertEqual(bridge.return_value.read.call_count,2)
            self.assertTrue(all(call.kwargs['approval_ids']==ids for call in bridge.return_value.read.call_args_list))

    def test_read_retry_uses_same_absolute_budget(self):
        from unittest.mock import patch
        from martin_luk.live_session import _broker_read
        from martin_agentic.transport import TransportError
        with patch('martin_agentic.live_session.time.monotonic',side_effect=[100,100,103]), \
             patch('martin_agentic.live_session.CodexBrokerReadTransport') as transport, \
             patch('martin_agentic.live_capture_bridge.SavedBundleObservationBridge') as bridge:
            bridge.return_value.read.side_effect=[TransportError('APP_SERVER_DISCONNECTED'),{'ok':True}]
            self.assertEqual(_broker_read({'account_suffix':'0000'},lambda:None),{'ok':True})
        self.assertEqual([call.kwargs['deadline_monotonic'] for call in transport.call_args_list],[110,110])

    def test_read_retry_cannot_restart_an_expired_budget(self):
        from unittest.mock import patch
        from martin_luk.live_session import _broker_read
        from martin_agentic.transport import TransportError
        with patch('martin_agentic.live_session.time.monotonic',side_effect=[100,100,111]), \
             patch('martin_agentic.live_session.CodexBrokerReadTransport') as transport, \
             patch('martin_agentic.live_capture_bridge.SavedBundleObservationBridge') as bridge:
            bridge.return_value.read.side_effect=TransportError('APP_SERVER_TIMEOUT')
            with self.assertRaisesRegex(TransportError,'APP_SERVER_TOTAL_BUDGET_EXCEEDED'):
                _broker_read({'account_suffix':'0000'},lambda:None)
        self.assertEqual(transport.call_count,1)


class ClosingIntentSafetyTests(TestCase):
    def test_preopen_unknown_external_write_remains_attention_required(self):
        from unittest.mock import patch
        fixture=LiveSessionRecoveryTests()
        with patch('martin_agentic.recovery.unresolved_write_intents',return_value=['manual-unknown']):
            result=fixture.run_session(at='2026-10-16T13:00:00+00:00')
        self.assertFalse(result['healthy'])
        self.assertTrue(result['attention_required'])
        self.assertIn('PREOPEN_UNRESOLVED_EXTERNAL_WRITE_REQUIRES_ATTENTION',result['preopen_attention_reasons'])
        self.assertEqual(result['unresolved_external_write_ids'],['manual-unknown'])

    def test_standalone_manual_and_native_unknown_writes_prevent_false_safe_close(self):
        from martin_luk.live_session import _closing_check
        from martin_agentic.broker import BrokerJournal
        from martin_agentic.watchdog import watchdog_check
        for kind in ('manual','native'):
            with self.subTest(kind=kind),tempfile.TemporaryDirectory() as directory:
                now=datetime.fromisoformat('2026-10-16T20:00:01+00:00')
                policy=json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
                path=Path(directory)/'book.sqlite';book=ExecutionBook(path,policy,clock=lambda:now)
                journal=BrokerJournal(path)
                try:
                    if kind=='manual':
                        book.audit.append({'event_id':'manual-intent','occurred_at':now.isoformat(),
                            'event_type':'AGENT_ACTION_INTENT','actor':'agent','synthetic':False,
                            'account':'••••0000','intent_id':'unknown-write',
                            'details':{'action':'entry','parameters':{'symbol':'DEMO','quantity':'1'}}})
                    else:
                        book.db.execute('INSERT INTO external_operations VALUES(?,?,?,?,NULL,NULL)',
                            ('unknown-write','orphan-action','place',now.isoformat()));book.db.commit()
                    observed=snapshot(at=now.isoformat())
                    result=_closing_check(book,journal,policy,lambda:now,observation=(True,observed))
                    self.assertFalse(result['final_flat_verified'])
                    self.assertEqual(result['unresolved_external_write_ids'],['unknown-write'])
                    # Even an old incorrectly SAFE_FLAT status cannot retire the
                    # independent guardian while the durable write is ambiguous.
                    status=Path(directory)/'status.json'
                    status.write_text(json.dumps({'db':str(path.resolve()),'pid':None,
                        'reason':'AUTHORIZED_SESSION_CLOSED','final_flat_verified':True,
                        'observed_at':now.isoformat(),'control_heartbeat_at':now.isoformat()}))
                    check=watchdog_check(path,status,policy=policy,clock=lambda:now,
                        process_checker=lambda *args:False,notifier=lambda *args:None)
                    self.assertFalse(check['closed_safe'])
                    self.assertIn('UNRESOLVED_EXTERNAL_WRITE',check['issues'])
                finally:journal.close();book.close()

    def test_real_book_unknown_submission_is_not_cleared_by_empty_broker_snapshot(self):
        from test_execution import ExecutionTests
        from martin_luk.live_session import _closing_check
        from unittest.mock import MagicMock,patch
        fixture=ExecutionTests();fixture.setUp()
        try:
            fixture.propose();action=fixture.book.claim()
            fixture.book.report(action['action_id'],{'status':'unknown','message':'timeout'})
            fixture.now=datetime.fromisoformat('2026-10-16T20:00:01+00:00')
            broker=MagicMock();broker.summary.return_value={'read_healthy':True,'blockers':[],
                'positions':{},'open_equity_orders':0,'other_asset_open_orders':0,'pending_approvals':0}
            with patch('martin_agentic.live_session._broker_read',return_value=fixture.snap()):
                result=_closing_check(fixture.book,broker,fixture.policy,lambda:fixture.now)
            self.assertEqual(result['close_disposition'],'MANUAL_INTERVENTION_REQUIRED')
            self.assertFalse(result['final_flat_verified'])
            self.assertIn(action['action_id'],result['unresolved_intent_ids'])
            self.assertEqual(fixture.book.actions()[0]['state'],'UNKNOWN')
        finally:fixture.tearDown()

    def test_never_dispatched_expired_entry_can_be_verified_flat(self):
        from test_execution import ExecutionTests
        from martin_luk.live_session import _closing_check
        from unittest.mock import MagicMock,patch
        fixture=ExecutionTests();fixture.setUp()
        try:
            fixture.propose();fixture.now=datetime.fromisoformat('2026-10-16T20:00:01+00:00')
            self.assertIsNone(fixture.book.claim())
            broker=MagicMock();broker.summary.return_value={'read_healthy':True,'blockers':[],
                'positions':{},'open_equity_orders':0,'other_asset_open_orders':0,'pending_approvals':0}
            with patch('martin_agentic.live_session._broker_read',return_value=fixture.snap()):
                result=_closing_check(fixture.book,broker,fixture.policy,lambda:fixture.now)
            self.assertFalse(result['final_flat_verified'])
            self.assertTrue(result['supported_scope_flat'])
            self.assertEqual(result['close_disposition'],
                             'SUPPORTED_SCOPE_FLAT_FULL_ACCOUNT_UNPROVEN')
            self.assertEqual(result['unresolved_intent_ids'],[])
            self.assertEqual(result['nonterminal_trade_ids'],[])
        finally:fixture.tearDown()

    def test_every_unsettled_action_state_blocks_flat_claim(self):
        from martin_luk.live_session import _closing_check
        from unittest.mock import MagicMock,patch
        for state in ['READY','RUNNING','UNKNOWN','WAITING_USER','REPORTED']:
            with self.subTest(state=state):
                book=MagicMock();book.actions.return_value=[{'id':'unsafe','state':state}]
                book.summary.return_value={'state':{'trades':{}}}
                journal=MagicMock();journal.summary.return_value={'read_healthy':True,'blockers':[],
                    'positions':{},'open_equity_orders':0,'other_asset_open_orders':0,'pending_approvals':0}
                with patch('martin_agentic.live_session._broker_read',return_value={'observed_at':'2026-10-16T20:00:01Z'}):
                    result=_closing_check(book,journal,{},lambda:datetime.fromisoformat('2026-10-16T20:00:01+00:00'))
                self.assertFalse(result['final_flat_verified'])
                self.assertTrue(result['attention_required'])


class CaptureCadenceTests(TestCase):
    run_session=LiveSessionRecoveryTests.run_session
    def test_capture_idle_is_capped_to_five_seconds_without_agent(self):
        result=self.run_session(cycles=2,capture_only=True)
        self.assertEqual(result['capture_idle_seconds'],5)
        self.assertEqual(self.cycle_wait.call_args.args[2],5)
        self.assertEqual(result['agent_mode'],'disabled')

    def test_execution_control_keeps_requested_poll_bound(self):
        result=self.run_session(cycles=2,capture_only=False)
        self.assertIsNone(result['capture_idle_seconds'])
        self.assertEqual(self.cycle_wait.call_args.args[2],15)


class LineageManagementControlTests(TestCase):
    run_session=LiveSessionRecoveryTests.run_session

    def test_missing_predecessor_blocks_entry_but_not_fresh_reads_or_management(self):
        gate={'blocked':True,'reason':'LINEAGE_PRIOR_UNKNOWN_OR_UNRESOLVED_WRITE:old'}
        result=self.run_session(cycles=2,lineage_gate=gate)
        self.assertEqual(self.read.call_count,2)
        self.assertEqual(self.supervisor.step.call_count,2)
        self.assertTrue(all(not c.kwargs['allow_entries'] for c in self.supervisor.step.call_args_list))
        self.assertEqual(result['entry_admission'],gate)
        self.assertEqual(result['reason'],'PREDECESSOR_LINEAGE_BLOCKED_MANAGING_EXISTING')
        self.assertTrue(result['new_entries_blocked']);self.assertTrue(result['attention_required'])

    def test_prior_alias_lookup_failure_keeps_own_pending_aliases_readable(self):
        from unittest.mock import Mock,patch
        from martin_luk.live_session import broker_approval_ids
        book=Mock();book.simulation=False;book.policy={};book.pending_approval_ids.return_value=['own']
        with patch('martin_agentic.session_lineage.lineage_approval_ids',side_effect=ValueError('old missing')):
            self.assertEqual(broker_approval_ids(book),['own'])


class CurrentObservationHealth034r1Tests(TestCase):
    run_session=LiveSessionRecoveryTests.run_session

    def test_stop_time_delay_crossing_account_and_quote_thresholds_fails_current_health(self):
        from unittest.mock import patch
        from datetime import timedelta
        from martin_luk.live_session import _drain_read_task
        at='2026-10-16T14:00:00+00:00'
        events=[{'kind':'quote','data':{'symbol':'SOFI','bid':'10','ask':'10.01',
            **{key:at for key in ('bid_at','ask_at','observed_at')}}}]
        original=_drain_read_task
        def age_during_cleanup(*args,**kwargs):
            drained=original(*args,**kwargs)
            self.now+=timedelta(seconds=20)
            return drained
        with patch('martin_agentic.live_session._drain_read_task',
                   side_effect=age_during_cleanup):
            result=self.run_session(capture_only=True,events=events,cycles=2)
        self.assertEqual(result['last_completed_observation_at'],at)
        self.assertTrue(result['last_completed_observation_healthy'])
        self.assertFalse(result['current_observation_evidence_fresh'])
        self.assertFalse(result['current_observation_healthy'])
        self.assertFalse(result['read_only_capture_healthy'])
        self.assertFalse(result['operational_healthy'])
        self.assertTrue(result['attention_required'])
        self.assertEqual(result['current_observation_reason'],
                         'CURRENT_OBSERVATION_MISSING_OR_STALE')
        self.assertTrue(result['bounded_capture_completed'])

    def test_current_health_is_false_when_no_market_evidence_was_ever_consumed(self):
        result=self.run_session(capture_only=True,cycles=1,market_error=ValueError('synthetic'))
        self.assertFalse(result['current_observation_healthy'])
        self.assertFalse(result['read_only_capture_healthy'])
        self.assertFalse(result['operational_healthy'])


class CurrentObservationHealth034r2Tests(TestCase):
    run_session=LiveSessionRecoveryTests.run_session

    def test_success_then_broker_error_keeps_evidence_fresh_but_not_current_health(self):
        from martin_agentic.transport import TransportError
        at='2026-10-16T14:00:00+00:00'
        events=[{'kind':'quote','data':{'symbol':'SOFI','bid':'10','ask':'10.01',
            **{key:at for key in ('bid_at','ask_at','observed_at')}}}]
        calls=[]
        def broker(*args,**kwargs):
            calls.append(1)
            if len(calls)>1:raise TransportError('APP_SERVER_DISCONNECTED')
            return full_snapshot(self.now.isoformat())
        result=self.run_session(capture_only=True,events=events,cycles=2,broker_error=broker)
        self.assertEqual(result['reason'],'BROKER_OR_LIFECYCLE_UNAVAILABLE')
        self.assertTrue(result['current_observation_evidence_fresh'])
        self.assertFalse(result['current_observation_healthy'])
        self.assertEqual(result['current_observation_reason'],
                         'CURRENT_OBSERVATION_READ_ERROR')
        self.assertEqual(result['current_observation_evidence']['latest_observation_state'],'ERROR')
        self.assertEqual(result['current_observation_evidence']['latest_observation_error']['code'],
                         'APP_SERVER_DISCONNECTED')
        self.assertTrue(result['last_completed_observation_healthy'])
        self.assertFalse(result['read_only_capture_healthy'])
        self.assertFalse(result['operational_healthy'])

    def test_success_then_market_error_keeps_evidence_fresh_but_not_current_health(self):
        from martin_agentic.transport import TransportError
        at='2026-10-16T14:00:00+00:00'
        events=[{'kind':'quote','data':{'symbol':'SOFI','bid':'10','ask':'10.01',
            **{key:at for key in ('bid_at','ask_at','observed_at')}}}]
        calls=[]
        def market(*args,**kwargs):
            calls.append(1)
            if len(calls)>1:raise TransportError('APP_SERVER_DISCONNECTED')
            return ([],{})
        result=self.run_session(capture_only=True,events=events,cycles=2,market_error=market)
        self.assertEqual(len(calls),2)
        self.assertTrue(result['current_observation_evidence_fresh'])
        self.assertFalse(result['current_observation_healthy'])
        self.assertEqual(result['current_observation_reason'],
                         'CURRENT_OBSERVATION_MARKET_ERROR')
        self.assertEqual(result['current_observation_evidence']['latest_market_state'],'ERROR')
        self.assertFalse(result['read_only_capture_healthy'])
        self.assertFalse(result['operational_healthy'])

    def test_cleanup_failure_cannot_claim_current_health(self):
        from unittest.mock import patch
        from martin_luk.live_session import _drain_read_task
        at='2026-10-16T14:00:00+00:00'
        events=[{'kind':'quote','data':{'symbol':'SOFI','bid':'10','ask':'10.01',
            **{key:at for key in ('bid_at','ask_at','observed_at')}}}]
        original=_drain_read_task
        def fail_market(task,*args,**kwargs):
            drained=original(task,*args,**kwargs)
            if task.name=='market-read-only':
                drained=dict(drained);drained['drained']=False
            return drained
        with patch('martin_agentic.live_session._drain_read_task',side_effect=fail_market):
            result=self.run_session(capture_only=True,events=events,cycles=1)
        self.assertTrue(result['current_observation_evidence_fresh'])
        self.assertFalse(result['current_observation_healthy'])
        self.assertEqual(result['current_observation_reason'],
                         'CURRENT_OBSERVATION_CLEANUP_FAILED')
        self.assertFalse(result['current_observation_evidence']['cleanup_ok'])
        self.assertFalse(result['read_only_capture_healthy'])
        self.assertFalse(result['operational_healthy'])

    def test_inflight_final_read_without_new_completed_observation_is_not_current(self):
        import threading
        calls=[]
        def broker(*args,**kwargs):
            calls.append(1)
            if len(calls)>1:threading.Event().wait(.2)
            return full_snapshot(self.now.isoformat())
        result=self.run_session(capture_only=True,cycles=2,broker_error=broker)
        self.assertEqual(len(calls),2)
        self.assertEqual(result['reason'],'BROKER_COLLECTION_IN_FLIGHT')
        self.assertTrue(result['current_observation_evidence_fresh'])
        self.assertFalse(result['current_observation_healthy'])
        self.assertEqual(result['current_observation_reason'],
                         'CURRENT_OBSERVATION_NOT_COMPLETED')
        self.assertEqual(result['current_observation_evidence']['latest_observation_state'],
                         'IN_FLIGHT')

    def test_preopen_and_close_keep_current_health_when_observation_is_fresh(self):
        preopen=self.run_session(at='2026-10-16T13:00:00+00:00',cycles=1)
        self.assertTrue(preopen['current_observation_evidence_fresh'])
        self.assertTrue(preopen['current_observation_healthy'])
        closed=self.run_session(at='2026-10-16T20:00:01+00:00',
                                cycles=1,capture_only=True)
        self.assertTrue(closed['current_observation_evidence_fresh'])
        self.assertTrue(closed['current_observation_healthy'])
        self.assertFalse(closed['broker_close_or_flat_proof'])
        self.assertTrue(closed['supported_scope_flat'])

    def test_entry_baseline_block_does_not_make_read_observation_unhealthy(self):
        gate={'blocked':True,'reason':'LINEAGE_PRIOR_UNKNOWN_OR_UNRESOLVED_WRITE:old'}
        result=self.run_session(cycles=1,lineage_gate=gate)
        self.assertFalse(result['healthy'])
        self.assertTrue(result['current_observation_evidence_fresh'])
        self.assertTrue(result['current_observation_healthy'])
