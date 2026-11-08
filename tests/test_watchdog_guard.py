from datetime import datetime,timedelta,timezone
import json
from pathlib import Path
import tempfile
from unittest import TestCase
from unittest.mock import Mock

from martin_agentic.audit import digest
from martin_agentic.watchdog import watchdog_entry_admission


class WatchdogEntryAdmissionTests(TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name).resolve();self.db=self.root/'book.sqlite'
        self.status=self.root/'status.json';self.policy_path=self.root/'policy.json'
        self.policy={'account_suffix':'0000','live_enabled':False};self.now=datetime.now(timezone.utc)
        self.value={'db':str(self.db),'status_file':str(self.status),'control_pid':123,
            'pid':456,'policy_hash':digest(self.policy),'source_fingerprint':'tested-build',
            'checked_at':self.now.isoformat(),'healthy':True,'issues':[],
            'process_verified':True,'closed_safe':False,'new_entries_blocked':False}
        self.process=Mock(return_value=True);self.save()

    def save(self):Path(str(self.db)+'.watchdog.json').write_text(json.dumps(self.value))
    def check(self):
        return watchdog_entry_admission(self.db,self.status,self.policy,policy_path=self.policy_path,
            build_fingerprint='tested-build',control_pid=123,clock=lambda:self.now,process_checker=self.process)

    def test_ready_requires_fresh_bound_independent_actual_process(self):
        value=self.check();self.assertTrue(value['ready'])
        self.process.assert_called_once_with(456,self.db,self.policy_path,self.status)
        self.assertEqual(value['broker_writes'],0);self.assertFalse(value['automatic_execution_enabled'])
        self.process.return_value=False
        self.assertIn('WATCHDOG_PROCESS_NOT_VERIFIED',self.check()['issues'])

    def test_missing_and_corrupt_startup_status_fail_closed_without_model_calls(self):
        path=Path(str(self.db)+'.watchdog.json');path.unlink()
        self.assertFalse(self.check()['ready'])
        path.write_text('[]');self.assertFalse(self.check()['ready'])
        path.write_text('not-json');self.assertFalse(self.check()['ready'])
        self.process.assert_not_called()

    def test_wrong_database_status_control_owner_policy_or_build_is_rejected(self):
        for key,value in [('db','/foreign.sqlite'),('status_file','/foreign-status.json'),
                ('control_pid',999),('policy_hash','wrong-policy'),('source_fingerprint','old-build')]:
            original=self.value[key];self.value[key]=value;self.save()
            with self.subTest(key=key):self.assertFalse(self.check()['ready'])
            self.value[key]=original

    def test_stale_or_future_heartbeat_does_not_authorize_new_risk(self):
        for delta in (-16,1):
            self.value['checked_at']=(self.now+timedelta(seconds=delta)).isoformat();self.save()
            self.assertIn('WATCHDOG_HEARTBEAT_STALE',self.check()['issues'])

    def test_historical_close_sticky_inhibit_unhealthy_or_same_process_is_not_independent_admission(self):
        for key,value in [('closed_safe',True),('healthy',False),
                ('process_verified',False),('issues',['CONTROL_HEARTBEAT_STALE']),('pid',123)]:
            original=self.value[key];self.value[key]=value;self.save()
            with self.subTest(key=key):self.assertFalse(self.check()['ready'])
            self.value[key]=original

    def test_observers_old_sticky_gate_copy_is_separate_from_current_admission(self):
        self.value['new_entries_blocked']=True;self.save()
        self.assertTrue(self.check()['ready'])
        # Observer proof does not create or clear the actual entry inhibit.
        self.assertFalse(Path(str(self.db)+'.entry-inhibit.json').exists())
