from datetime import timedelta
import json
from pathlib import Path
import threading
import time
from unittest import TestCase
from unittest.mock import MagicMock,patch

from martin_agentic.async_native_review import AsyncNativeReviewDispatcher
from martin_agentic.control_tasks import worker_lease
from martin_luk.live_session import LiveSupervisor
import test_execution


class AsyncNativeReviewTests(TestCase):
    def setUp(self):
        self.f=test_execution.ExecutionTests();self.f.setUp();self.addCleanup(self.f.tearDown)
        self.buy=self.f.entry_filled();self.snapshot=self.f.snap([self.buy],qty='2')
        self.action=next(a['id'] for a in self.f.book.actions() if a['kind']=='protect')
        self.release=threading.Event();self.started=threading.Event();self.calls=[];outer=self
        class Factory:
            simulation_only=True
            def __init__(self,**kwargs):self.rpc=MagicMock();self.rpc.close.side_effect=outer.release.set
            def __enter__(self):
                outer.started.set();outer.release.wait(2);return self
            def __exit__(self,*args):self.rpc.close()
            def execute_review(self,packet):
                outer.calls.append(threading.get_ident())
                return {'data':{**packet['parameters'],'order_checks':{},'customer_approval_required':False,
                    'market_data_disclosure':'Exact review disclosure'}}
        self.factory=Factory
        self.directory=Path(self.f.tmp.name)/'receipts'
        self.driver=AsyncNativeReviewDispatcher(self.f.book,self.directory,transport_factory=Factory)
        self.addCleanup(self.cleanup)

    def cleanup(self):
        self.release.set()
        if self.driver.task.thread:self.driver.task.thread.join(1)
        self.driver.close()

    def step(self,**kwargs):
        return self.driver(snapshot=self.snapshot,quotes={'DEMO':self.f.quote},**kwargs)

    def test_live_supervisor_keeps_owner_reconciliation_during_blocked_script_review(self):
        supervisor=LiveSupervisor(self.f.book,dispatcher=self.driver)
        event={'id':'quote','kind':'quote','at':self.f.now.isoformat(),'synthetic_input':True,
            'source_sha256':'synthetic-source','data':self.f.quote}
        begin=time.monotonic()
        result=supervisor.step(self.snapshot,[event],allow_entries=False)
        self.assertEqual(result['dispatch']['status'],'SCRIPT_REVIEW_STARTED')
        self.assertFalse(result['dispatch']['agent_called']);self.assertTrue(self.started.wait(1))
        writes=[];original=self.f.book._begin
        def tracked():writes.append(threading.get_ident());return original()
        with patch.object(self.f.book,'_begin',side_effect=tracked):
            for _ in range(5):
                result=supervisor.step(self.snapshot,[],allow_entries=False)
                self.assertEqual(result['dispatch']['status'],'REVIEW_IN_FLIGHT')
            self.assertLess(time.monotonic()-begin,1)
            self.assertIsNone(worker_lease(self.f.path))
            self.release.set();self.driver.task.thread.join(1)
            completed=self.driver(allow_dispatch=False)
        self.assertEqual(completed['status'],'WAITING_USER');self.assertEqual(len(self.calls),1)
        self.assertNotEqual(self.calls[0],threading.get_ident());self.assertTrue(writes)
        self.assertEqual(set(writes),{threading.get_ident()})
        waiting=self.step()
        self.assertEqual(waiting['status'],'WAITING_USER');self.assertTrue(waiting['attention_required'])
        self.assertEqual(waiting['native_result']['market_data_disclosure'],'Exact review disclosure')
        pending_read=self.driver(allow_dispatch=False)
        self.assertEqual(pending_read['status'],'WAITING_USER');self.assertTrue(pending_read['attention_required'])
        lease=worker_lease(self.f.path);self.assertIsNotNone(lease);lease.close()
        self.assertTrue(self.f.book.audit.verify()['integrity_valid'])

    def test_receipt_tampering_is_failed_review_and_does_not_enable_place(self):
        self.step();self.release.set();self.driver.task.thread.join(1)
        path=next(self.directory.glob('*.json'));receipt=json.loads(path.read_text())
        receipt['binding']['account_key']='changed';path.write_text(json.dumps(receipt))
        result=self.driver(allow_dispatch=False)
        self.assertEqual(result['status'],'REVIEW_FAILED');self.assertTrue(result['attention_required'])
        self.assertFalse(result['execution_authorized'])
        self.assertEqual(next(a for a in self.f.book.actions() if a['id']==self.action)['state'],'WAITING_USER')

    def test_stop_closes_transport_and_imports_cancelled_review_without_model_or_retry(self):
        self.step();self.assertTrue(self.started.wait(1))
        result=self.driver.close()
        self.assertFalse(result['thread_alive']);self.assertIsNone(result['pending_operation_id'])
        self.assertEqual(result['receipt_import']['status'],'REVIEW_FAILED');self.assertEqual(self.calls,[])
        self.assertEqual(self.step()['status'],'NO_REVIEW_DISPATCH')

    def test_real_factory_rejects_synthetic_book_without_reservation(self):
        from martin_agentic.native_review import EquityReviewTransport
        self.driver.factory=EquityReviewTransport
        before=self.f.book.audit.verify()
        self.assertEqual(self.step()['status'],'REVIEW_SCOPE_DENIED')
        self.assertEqual(self.f.book.audit.verify(),before)
        self.assertIsNone(self.driver.pending)

    def test_failed_core_import_keeps_recovery_attention_on_later_ticks_and_stop(self):
        self.step();self.release.set();self.driver.task.thread.join(1)
        with patch('martin_agentic.async_native_review.NativeOperations.record',side_effect=OSError('ledger failed')):
            failed=self.driver(allow_dispatch=False)
        self.assertEqual(failed['status'],'REVIEW_IMPORT_REQUIRES_RECOVERY')
        self.assertEqual(self.step(),failed)
        self.assertTrue(self.driver.close()['attention_required'])
        self.assertTrue(failed['operation_id'])

    def test_restart_does_not_replay_pending_native_review_or_hide_its_reconciliation(self):
        started=self.step();self.assertTrue(self.started.wait(1))
        self.f.book.recover()
        other=AsyncNativeReviewDispatcher(self.f.book,self.directory,transport_factory=self.factory)
        pending=other(snapshot=self.snapshot,quotes={'DEMO':self.f.quote})
        self.assertEqual(pending['status'],'NATIVE_RECONCILIATION_REQUIRED')
        self.assertTrue(pending['attention_required']);self.assertIn(self.action,pending['unresolved_action_ids'])
        self.assertIsNone(other.task.thread);self.assertIsNone(other.pending)
        self.assertEqual(other(allow_dispatch=False)['status'],'NATIVE_RECONCILIATION_REQUIRED')
        self.assertTrue(other.close()['attention_required'])
        self.assertEqual(self.driver.pending['packet']['operation_id'],started['operation_id'])
        self.release.set();self.driver.task.thread.join(1)
        self.assertEqual(self.driver(allow_dispatch=False)['status'],'WAITING_USER')

    def test_start_failure_records_failed_review_and_releases_lease(self):
        with patch.object(self.driver.task,'start',side_effect=RuntimeError('cannot start')):
            result=self.step()
        self.assertEqual(result['status'],'CORE_REVIEW_NOT_DISPATCHED')
        self.assertEqual(next(a for a in self.f.book.actions() if a['id']==self.action)['state'],'WAITING_USER')
        lease=worker_lease(self.f.path);self.assertIsNotNone(lease);lease.close()

    def test_freshness_deadline_expires_during_startup_before_formal_review(self):
        self.f.quote.update({k:(self.f.now-timedelta(seconds=14.98)).isoformat()
            for k in ('bid_at','ask_at','observed_at')})
        self.step();self.assertTrue(self.started.wait(1));time.sleep(.04)
        self.release.set();self.driver.task.thread.join(1)
        result=self.driver(allow_dispatch=False)
        self.assertEqual(self.calls,[]);self.assertEqual(result['status'],'REVIEW_FAILED')

    def test_full_controller_script_mode_uses_zero_models_and_keeps_confirmation_notice(self):
        from contextlib import ExitStack
        from martin_luk.live_session import run_live_session
        journal=MagicMock();journal.summary.return_value={'read_healthy':True,'reconciliation_issues':[],
            'positions':{},'open_equity_orders':0,'pending_approvals':0,'blockers':[]}
        event={'id':'controller-quote','kind':'quote','at':self.f.now.isoformat(),'synthetic_input':True,
            'source_sha256':'synthetic-source','data':self.f.quote}
        waits=[];final={};original_close=self.f.book.close
        def tick(*args,**kwargs):
            waits.append(1)
            if len(waits)>=3:self.release.set()
            time.sleep(.03)
        def close():
            final['actions']=self.f.book.actions()
            final['reservations']=self.f.book.db.execute('SELECT count(*) FROM worker_call_reservations').fetchone()[0]
            final['native_count']=self.f.book.db.execute('SELECT count(*) FROM native_operation_records').fetchone()[0]
            original_close()
        with ExitStack() as stack:
            prefix='martin_agentic.live_session.'
            stack.enter_context(patch(prefix+'ExecutionBook',return_value=self.f.book))
            stack.enter_context(patch(prefix+'BrokerJournal',return_value=journal))
            stack.enter_context(patch(prefix+'_broker_read',return_value=self.snapshot))
            stack.enter_context(patch(prefix+'collect_live_events',return_value=([event],{})))
            stack.enter_context(patch(prefix+'wait_for_result_or_tick',side_effect=tick))
            stack.enter_context(patch(prefix+'CodexOrderWorker',side_effect=AssertionError('no model permitted')))
            stack.enter_context(patch(prefix+'print'))
            notify=stack.enter_context(patch(prefix+'notify_local'))
            stack.enter_context(patch.object(self.f.book,'close',side_effect=close))
            stack.enter_context(patch('martin_agentic.async_native_review.EquityReviewTransport',self.factory))
            result=run_live_session(self.f.path,self.f.policy,symbols=[str(i) for i in range(10)],cycles=8,
                agent_mode='script-review',clock=lambda:self.f.now,status_file=Path(self.f.tmp.name)/'status.json',
                output_dir=Path(self.f.tmp.name)/'worker',budget_entry_limit=0,budget_total_limit=0)
        self.assertEqual(result['cycle'],8);self.assertEqual(final['reservations'],0)
        self.assertEqual(final['native_count'],1);self.assertEqual(len(self.calls),1)
        self.assertEqual(result['dispatch']['status'],'WAITING_USER');self.assertTrue(result['attention_required'])
        self.assertFalse(result['healthy']);self.assertTrue(result['new_entries_blocked'])
        confirmation_notices=[c for c in notify.call_args_list if c.args[0].startswith('有交易动作')]
        self.assertEqual(len(confirmation_notices),1)
