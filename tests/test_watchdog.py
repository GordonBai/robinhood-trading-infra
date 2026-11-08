from contextlib import closing
from datetime import datetime,timedelta,timezone
import json
from pathlib import Path
import sqlite3
import tempfile
from unittest import TestCase
from unittest.mock import Mock
from martin_agentic.watchdog import watchdog_check,entry_inhibit


class WatchdogTests(TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.db=(Path(self.tmp.name)/'book.sqlite').resolve();self.status=Path(self.tmp.name)/'status.json'
        self.now=datetime(2026,10,19,14,tzinfo=timezone.utc);self.notify=Mock()
        with closing(sqlite3.connect(self.db,isolation_level=None)) as db:
            db.executescript('CREATE TABLE live_actions(id TEXT,kind TEXT,state TEXT,created_at TEXT);CREATE TABLE live_state(id INTEGER,payload TEXT);')
            db.execute('INSERT INTO live_state VALUES(1,?)',(json.dumps({'trades':{}}),))
        self.value={'db':str(self.db),'pid':123,'control_heartbeat_at':self.now.isoformat(),'observed_at':self.now.isoformat()};self.save()
    def tearDown(self):self.tmp.cleanup()
    def save(self):self.status.write_text(json.dumps(self.value))
    def check(self,**kwargs):return watchdog_check(self.db,self.status,clock=lambda:self.now,notifier=self.notify,process_checker=lambda pid,db:pid==123,**kwargs)

    def test_process_killed_independent_watchdog_blocks_once_preserves_protect(self):
        with closing(sqlite3.connect(self.db,isolation_level=None)) as db:db.execute('INSERT INTO live_actions VALUES(?,?,?,?)',('protect','protect','WAITING_USER',self.now.isoformat()))
        self.value['pid']=None;self.save()
        first=self.check();self.assertIn('CONTROL_PROCESS_NOT_VERIFIED',first['issues']);self.assertTrue(first['new_entries_blocked'])
        self.assertEqual(self.check()['notifications_sent'],0);self.notify.assert_called_once()
        with closing(sqlite3.connect(self.db,isolation_level=None)) as db:self.assertEqual(db.execute('SELECT state FROM live_actions').fetchone()[0],'WAITING_USER')
        self.assertEqual(first['broker_writes'],0);self.assertEqual(first['model_calls'],0)

    def test_hung_control_and_broker_staleness_block_risk_no_auto_clear(self):
        self.now+=timedelta(seconds=40);bad=self.check();self.assertIn('CONTROL_HEARTBEAT_STALE',bad['issues'])
        self.value.update(control_heartbeat_at=self.now.isoformat(),observed_at=self.now.isoformat());self.save()
        recovered=self.check();self.assertTrue(recovered['healthy']);self.assertTrue(recovered['new_entries_blocked'])

    def test_actual_unprotected_clock_not_reset_by_waiting_confirmation(self):
        with closing(sqlite3.connect(self.db,isolation_level=None)) as db:db.execute('UPDATE live_state SET payload=?',(json.dumps({'trades':{'t':{'unprotected_since':(self.now-timedelta(seconds=31)).isoformat()}}}),))
        self.assertIn('ACTUAL_POSITION_UNPROTECTED_OVERDUE',self.check()['issues'])

    def test_missing_or_foreign_status_fails_closed(self):
        self.status.unlink();self.assertIn('CONTROL_STATUS_MISSING_OR_INVALID',self.check()['issues'])
        self.value['db']='/different/book.sqlite';self.save();self.assertIn('CONTROL_STATUS_MISSING_OR_INVALID',self.check()['issues'])

    def test_invalid_inhibit_never_allows_entry(self):
        gate=Path(str(self.db)+'.entry-inhibit.json');gate.write_text('bad-json')
        self.assertTrue(entry_inhibit(gate,self.db)['blocked']);gate.write_text(json.dumps({'db':'other','blocked':True}))
        self.assertTrue(entry_inhibit(gate,self.db)['blocked'])

    def test_known_closed_safe_book_does_not_need_live_pid_or_recent_clock(self):
        self.value.update(pid=None,reason='AUTHORIZED_SESSION_CLOSED',final_flat_verified=True);self.save();self.now+=timedelta(days=1)
        result=self.check();self.assertTrue(result['closed_safe']);self.assertEqual(result['issues'],[]);self.notify.assert_not_called()

    def test_prior_closed_owner_cannot_hide_a_new_expected_owner_failure(self):
        self.value.update(pid=456,reason='AUTHORIZED_SESSION_CLOSED',final_flat_verified=True);self.save()
        result=self.check(expected_control_pid=123)
        self.assertIn('CONTROL_OWNER_STATUS_BINDING_INVALID',result['issues'])
        self.assertFalse(result['closed_safe']);self.assertFalse(result['healthy'])

    def test_json_arrays_in_status_and_gate_fail_closed(self):
        self.status.write_text('[]');self.assertIn('CONTROL_STATUS_MISSING_OR_INVALID',self.check()['issues'])
        gate=Path(str(self.db)+'.entry-inhibit.json');gate.write_text('[]');self.assertTrue(entry_inhibit(gate,self.db)['blocked'])

    def test_running_worker_age_includes_time_since_status_was_published(self):
        self.value['worker_task']={'running':True,'age_seconds':130};self.save();self.now+=timedelta(seconds=6)
        self.assertIn('WORKER_OVERDUE',self.check()['issues'])

    def test_single_watchdog_owner(self):
        from martin_agentic.shadow import single_instance
        from martin_agentic.watchdog import run_watchdog
        with single_instance(str(self.db)+'.watchdog'):
            with self.assertRaises(ValueError):run_watchdog(self.db,self.status)

    def test_stop_on_safe_close_requires_the_bound_stopped_owner_and_complete_proof(self):
        from unittest.mock import patch
        from martin_agentic.watchdog import run_watchdog
        safe={'control_pid':123,'closed_safe':True,'healthy':True,'issues':[], 'process_verified':False}
        samples=[safe|{'control_pid':456},safe|{'process_verified':True},
            safe|{'issues':['UNRESOLVED_NATIVE_OPERATION']},safe]
        with patch('martin_agentic.watchdog.watchdog_check',side_effect=samples) as check,\
             patch('martin_agentic.watchdog.time.sleep'):
            result=run_watchdog(self.db,self.status,cycles=6,stop_on_safe_close=True,expected_control_pid=123)
        self.assertEqual(check.call_count,4);self.assertTrue(result['stopped_after_verified_safe_close'])

    def test_unsafe_close_keeps_observer_running_to_its_explicit_bound(self):
        from unittest.mock import patch
        from martin_agentic.watchdog import run_watchdog
        unsafe={'control_pid':123,'closed_safe':False,'healthy':False,
            'issues':['PENDING_ACTION_REQUIRES_ATTENTION'],'process_verified':False}
        with patch('martin_agentic.watchdog.watchdog_check',return_value=unsafe) as check,\
             patch('martin_agentic.watchdog.time.sleep'):
            result=run_watchdog(self.db,self.status,cycles=3,stop_on_safe_close=True,expected_control_pid=123)
        self.assertEqual(check.call_count,3);self.assertNotIn('stopped_after_verified_safe_close',result)

    def test_automatic_close_stop_without_exact_owner_is_rejected_before_reading(self):
        from unittest.mock import patch
        from martin_agentic.watchdog import run_watchdog
        with patch('martin_agentic.watchdog.watchdog_check') as check:
            with self.assertRaisesRegex(ValueError,'BOUND_CONTROL_PID'):
                run_watchdog(self.db,self.status,stop_on_safe_close=True)
        check.assert_not_called()

    def test_retired_history_closed_session_uses_exact_readonly_recovery_proof(self):
        import test_recovery
        f=test_recovery.RecoveryTests('test_ack_keeps_failure_state_issues_risk_and_manual_provenance');f.setUp();self.addCleanup(f.tearDown)
        f.acknowledged();status=Path(f.tmp.name)/'closed-status.json'
        status.write_text(json.dumps({'db':str(f.path.resolve()),'pid':None,'reason':'AUTHORIZED_SESSION_CLOSED',
            'final_flat_verified':True,'observed_at':f.snapshot['observed_at']}))
        checked=watchdog_check(f.path,status,policy=f.book.policy,clock=lambda:f.now+timedelta(days=1),
            process_checker=lambda *a:False,notifier=self.notify)
        self.assertTrue(checked['closed_safe']);self.assertTrue(checked['historical_manual_recovery'])
        self.assertTrue(checked['closed_safety_is_historical']);self.assertEqual(checked['issues'],[])
        with f.book.db:
            f.book.db.execute("INSERT INTO live_actions VALUES(?,?,?,?,?,?,?,?)",('new-unknown','new','entry','UNKNOWN','{}',f.now.isoformat(),f.now.isoformat(),None))
        changed=watchdog_check(f.path,status,policy=f.book.policy,clock=lambda:f.now+timedelta(days=1),
            process_checker=lambda *a:False,notifier=self.notify)
        self.assertFalse(changed['closed_safe']);self.assertTrue(changed['new_entries_blocked']);self.assertEqual(changed['pending_actions'],1)

    def test_release_rejects_failed_source_and_new_unresolved_manual_write(self):
        from martin_agentic.watchdog import release_entry_inhibit
        import test_recovery
        f=test_recovery.RecoveryTests('test_ack_keeps_failure_state_issues_risk_and_manual_provenance');f.setUp();self.addCleanup(f.tearDown)
        state=f.book.state();state['issues']=[i for i in state['issues'] if not i.startswith('PROTECTION_WAIT_EXCEEDED')]
        f.book._save(state);f.book.db.commit();f.acknowledged()
        f.journal.failure(at=f.now.isoformat(),reason='APP_SERVER_DISCONNECTED')
        with self.assertRaisesRegex(ValueError,'CURRENT_HEALTHY'):release_entry_inhibit(f.book,f.snapshot,reason='Cannot clear failed source')
        f.journal.ingest(f.snapshot)
        f.book.audit.append({'event_id':'manual-new-intent','occurred_at':f.now.isoformat(),'event_type':'AGENT_ACTION_INTENT',
            'actor':'agent','synthetic':False,'account':'••••0000','intent_id':'manual-unresolved',
            'details':{'action':'entry','parameters':{'symbol':'SOFI','quantity':'1'}}})
        with self.assertRaisesRegex(ValueError,'MANUAL_WRITE_UNRESOLVED'):release_entry_inhibit(f.book,f.snapshot,reason='Flat snapshot cannot disprove pending write')

    def test_release_requires_exact_formal_terminal_approval_for_no_order(self):
        from martin_agentic.watchdog import release_entry_inhibit
        import test_recovery
        f=test_recovery.RecoveryTests('test_ack_keeps_failure_state_issues_risk_and_manual_provenance');f.setUp();self.addCleanup(f.tearDown)
        state=f.book.state();state['issues']=[i for i in state['issues'] if not i.startswith('PROTECTION_WAIT_EXCEEDED')]
        f.book._save(state);f.book.db.commit();f.acknowledged()
        with f.book.db:
            f.book.db.execute('INSERT INTO external_operations VALUES(?,?,?,?,?,?)',('native-expired','old-action','place',f.now.isoformat(),None,'approval-exact'))
            f.book.db.execute('INSERT INTO external_broker_refs VALUES(?,?,?)',('approval-exact','old-action','native-expired'))
        f.book.audit.append({'event_id':'expired-native-intent','occurred_at':f.now.isoformat(),'event_type':'AGENT_ACTION_INTENT',
            'actor':'agent','synthetic':False,'account':'••••0000','intent_id':'native-expired',
            'details':{'action':'entry','parameters':{'symbol':'SOFI','quantity':'1'}}})
        with self.assertRaisesRegex(ValueError,'TRACKED_APPROVAL_NOT_QUERIED'):
            release_entry_inhibit(f.book,f.snapshot,reason='Pending list absence is insufficient')
        f.snapshot['requested_approval_ids']=['approval-exact'];f.snapshot['approval_states']={'approval-exact':{
            'approval_id':'approval-exact','asset_class':'ASSET_CLASS_EQUITY','is_edited':False,'state':'APPROVAL_STATE_EXPIRED'}}
        f.snapshot['observed_at']=(f.now+timedelta(seconds=1)).isoformat();f.now+=timedelta(seconds=1);f.journal.ingest(f.snapshot)
        result=release_entry_inhibit(f.book,f.snapshot,reason='Exact formal expiration, no order, fresh full account')
        self.assertTrue(result['released']);self.assertFalse(result['execution_enabled'])
        f.snapshot['approval_states']['approval-exact']['is_edited']=True
        f.snapshot['observed_at']=(f.now+timedelta(seconds=1)).isoformat();f.now+=timedelta(seconds=1);f.journal.ingest(f.snapshot)
        with self.assertRaisesRegex(ValueError,'APPROVAL_UNRESOLVED'):
            release_entry_inhibit(f.book,f.snapshot,reason='Edited formal intent cannot be inherited')
