import json
from datetime import datetime
from pathlib import Path
import tempfile
from unittest import TestCase
from unittest.mock import patch

from martin_agentic.capture_archive import export_capture_archive, persist_raw_record, read_decision_context
from entry_authority_fixture import TestExecutionBook as ExecutionBook
from martin_luk.live_session import LiveSupervisor, record_capture_disposition
from martin_agentic.market import RobinhoodReader
from shadow_fixture import events


class LiveCaptureArchiveIntegrationTests(TestCase):
    def book(self, directory):
        policy=json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        policy['session_date']='2026-10-15'; policy['capture_only']=True
        self.now=datetime.fromisoformat('2026-10-15T09:25:00-04:00')
        book=ExecutionBook(Path(directory)/'book.sqlite',policy,simulation=True,clock=lambda:self.now)
        self.addCleanup(book.close)
        return book

    def test_screen_no_signal_and_signal_disposition_reproduce_without_latest_state(self):
        with tempfile.TemporaryDirectory() as folder:
            book=self.book(folder); supervisor=LiveSupervisor(book)
            signals=supervisor.signals
            for original in events()[1:5]:
                self.now=datetime.fromisoformat(original['at'])
                signals.consume({**original,'synthetic_input':True,'source_sha256':'fixture-'+original['id']})
            pending=signals.pending();self.assertEqual(len(pending),1)
            signals.complete(pending[0]['id'],result={'rejected':True,'reason':'CAPTURE_ONLY'})
            contexts=[read_decision_context(book.db,r[0],r[1]) for r in book.db.execute('SELECT event_id,result_id FROM live_decision_archive ORDER BY rowid')]
            self.assertEqual(contexts[0]['reason'],'UNIVERSE_SCREENED')
            self.assertEqual(len(contexts[0]['screening']['candidates'])+len(contexts[0]['screening']['rejected']),10)
            self.assertEqual(contexts[-1]['result']['reason'],'CAPTURE_ONLY')
            self.assertEqual(contexts[-2]['reason'],'SIGNAL_CREATED')
            exported=export_capture_archive(Path(folder)/'book.sqlite',Path(folder)/'export')
            self.assertEqual(exported['event_count'],4);self.assertEqual(exported['decision_count'],5)
            self.assertFalse(exported['actual_point_in_time_verified'])
            self.assertEqual(book.actions(),[])

    def test_archive_failure_rolls_back_strategy_and_consumed_input_together(self):
        with tempfile.TemporaryDirectory() as folder:
            book=self.book(folder);signals=LiveSupervisor(book).signals;before=signals.state()
            event={**events()[1],'synthetic_input':True,'source_sha256':'fixture'}
            with patch('martin_luk.live_signals.archive_decision',side_effect=OSError('fixture disk')):
                with self.assertRaises(OSError):signals.consume(event)
            self.assertEqual(signals.state(),before)
            self.assertEqual(book.db.execute('SELECT COUNT(*) FROM live_inputs').fetchone()[0],0)
            self.assertEqual(book.db.execute('SELECT COUNT(*) FROM live_event_archive').fetchone()[0],0)

    def test_rejected_quote_is_retained_without_strategy_consumption(self):
        with tempfile.TemporaryDirectory() as folder:
            book=self.book(folder);signals=LiveSupervisor(book).signals;before=signals.state()
            event={'id':'stale-q','kind':'quote','at':self.now.isoformat(),'source_sha256':'fixture','data':{'symbol':'SOFI'}}
            record_capture_disposition(book,event,reason='MARKET_QUOTE_STALE')
            self.assertEqual(signals.state(),before)
            self.assertFalse(book.db.execute('SELECT 1 FROM live_inputs').fetchone())
            context=json.loads(book.db.execute('SELECT payload FROM live_decision_archive').fetchone()[0])
            self.assertEqual(context['status'],'NOT_CONSUMED')

    def test_each_successful_response_survives_later_transport_failure(self):
        with tempfile.TemporaryDirectory() as folder:
            now=datetime.fromisoformat('2026-10-20T18:20:00+00:00')
            calls=[]
            def transport(*args):
                calls.append(args)
                if len(calls)==2:raise RuntimeError('fixture later request failed')
                return {'data':{'results':[]}}
            def observe(record):
                persist_raw_record(folder,json.dumps(record).encode(),observed_at=record['observed_at'])
            reader=RobinhoodReader(transport,clock=lambda:now,record_observer=observe)
            reader.fetch('get_equity_quotes',{'symbols':['AAPL']})
            with self.assertRaises(RuntimeError):reader.fetch('get_equity_quotes',{'symbols':['SOFI']})
            self.assertEqual(len(list(Path(folder).glob('*.raw'))),1)
            receipt=json.loads(next(Path(folder).glob('*.receipt.json')).read_text())
            self.assertEqual(receipt['observed_at'],now.isoformat())

    def test_completed_market_result_wakes_without_full_poll_delay(self):
        from martin_agentic.control_tasks import BackgroundTask
        from martin_luk.live_session import wait_for_result_or_tick
        from unittest.mock import Mock
        task=BackgroundTask('fixture');task.start(lambda:'result');task.thread.join()
        with patch('martin_agentic.live_session.time.sleep') as sleep:
            wait_for_result_or_tick(task,None,15)
            sleep.assert_not_called()
        self.assertEqual(task.take(),(True,'result'))

    def test_waiting_market_has_bounded_tick_and_no_busy_loop(self):
        from martin_agentic.control_tasks import BackgroundTask
        from martin_luk.live_session import wait_for_result_or_tick
        task=BackgroundTask('fixture');task.thread=object()
        with patch('martin_agentic.live_session.time.sleep') as sleep:
            wait_for_result_or_tick(task,None,5)
            self.assertAlmostEqual(sum(c.args[0] for c in sleep.call_args_list),5)
            self.assertLessEqual(sleep.call_count,51)

    def test_stale_strategy_input_is_archived_and_does_not_skip_management_dispatch(self):
        from unittest.mock import Mock
        from test_broker import snapshot
        with tempfile.TemporaryDirectory() as folder:
            book=self.book(folder);dispatcher=Mock(return_value={'agent_called':False})
            supervisor=LiveSupervisor(book,worker=Mock(),dispatcher=dispatcher)
            old={**events()[1],'at':'2026-10-15T09:00:00-04:00','synthetic_input':True,'source_sha256':'fixture'}
            broker=snapshot(at=self.now.isoformat());broker['synthetic']=True
            with patch.object(book,'reconcile',wraps=book.reconcile) as reconcile:
                result=supervisor.step(broker,[old],allow_entries=True)
                reconcile.assert_called_once()
            dispatcher.assert_called_once_with(allow_entries=False)
            self.assertTrue(result['new_entries_blocked'])
            self.assertEqual(result['errors'][0]['reason'],'STALE_STRATEGY_EVENT')
            saved=json.loads(book.db.execute('SELECT payload FROM live_event_archive').fetchone()[0])
            self.assertEqual(saved['kind'],'strategy_rejection')
            self.assertEqual(saved['data']['original_event'],old)
            self.assertFalse(book.db.execute('SELECT 1 FROM live_inputs').fetchone())

    def test_universe_cache_date_and_archived_state_commit_as_one_transaction(self):
        with tempfile.TemporaryDirectory() as folder:
            book=self.book(folder);signals=LiveSupervisor(book).signals
            signals.consume({**events()[1],'synthetic_input':True,'source_sha256':'fixture'})
            row=book.db.execute('SELECT event_id,result_id FROM live_decision_archive').fetchone()
            decision=read_decision_context(book.db,row[0],row[1])
            self.assertEqual(decision['state_after']['universe_date'],'2026-10-15')
            self.assertEqual(decision['state_after'],signals.state())

    def test_invalid_input_does_not_drop_later_batch_events_or_expose_bad_quotes(self):
        from unittest.mock import Mock
        from test_broker import snapshot
        with tempfile.TemporaryDirectory() as folder:
            book=self.book(folder);supervisor=LiveSupervisor(book)
            original=events()[1];valid={**original,'synthetic_input':True,'source_sha256':'fixture'}
            broker=snapshot(at=self.now.isoformat());broker['synthetic']=True
            result=supervisor.step(broker,[None,valid],allow_entries=True)
            self.assertTrue(result['new_entries_blocked'])
            self.assertEqual(supervisor.signals.state()['universe_date'],'2026-10-15')
            self.assertEqual(book.db.execute('SELECT COUNT(*) FROM live_event_archive').fetchone()[0],2)
            self.assertEqual(book.actions(),[])

    def test_integrity_failure_persists_entry_inhibit_but_still_manages(self):
        from unittest.mock import Mock
        from test_broker import snapshot
        from martin_agentic.watchdog import entry_inhibit
        with tempfile.TemporaryDirectory() as folder:
            book=self.book(folder);supervisor=LiveSupervisor(book)
            for original in events()[1:4]:
                self.now=datetime.fromisoformat(original['at'])
                supervisor.signals.consume({**original,'synthetic_input':True,'source_sha256':'fixture-'+original['id']})
            revised={**events()[3],'id':'revised-new-id','synthetic_input':True,'source_sha256':'changed'}
            revised['data']={**revised['data'],'high':'99'}
            broker=snapshot(at=self.now.isoformat());broker['synthetic']=True
            with patch.object(book,'reconcile',wraps=book.reconcile) as reconcile:
                result=supervisor.step(broker,[revised],allow_entries=True)
                reconcile.assert_called_once()
            self.assertEqual(result['errors'][0]['reason'],'CONSUMED_BAR_REVISED')
            gate=entry_inhibit(str(Path(folder)/'book.sqlite')+'.entry-inhibit.json',Path(folder)/'book.sqlite')
            self.assertTrue(gate['blocked']);self.assertEqual(book.actions(),[])

    def test_real_protection_action_is_dispatched_despite_stale_strategy_event(self):
        from test_execution import ExecutionTests
        fixture=ExecutionTests();fixture.setUp()
        try:
            buy=fixture.entry_filled();calls=[]
            def worker(action):
                calls.append(action)
                return {'status':'needs_confirmation','message':'synthetic worker preview only'}
            supervisor=LiveSupervisor(fixture.book,worker=worker)
            stale={'id':'stale-market','kind':'quote','at':'2026-10-16T13:00:00+00:00',
                'data':fixture.quote,'synthetic_input':True,'source_sha256':'fixture'}
            result=supervisor.step(fixture.snap([buy],qty='2'),[stale],allow_entries=True)
            self.assertTrue(result['new_entries_blocked'])
            self.assertTrue(result['dispatch']['agent_called'])
            self.assertEqual(len(calls),1);self.assertEqual(calls[0]['kind'],'protect')
            self.assertEqual(calls[0]['parameters']['quantity'],'2')
            action=next(a for a in fixture.book.actions() if a['id']==calls[0]['action_id'])
            self.assertEqual(action['state'],'WAITING_USER')
            self.assertTrue(fixture.book.audit.verify()['integrity_valid'])
        finally:fixture.tearDown()
