import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock,patch

from martin_agentic.session_control import process_matches,session_control,watchdog_process_matches
from martin_agentic.shadow import single_instance


class SessionControlTests(TestCase):
    def test_matching_requires_exact_module_and_database(self):
        db=str(Path('runtime/ours.sqlite').resolve())
        with patch('martin_agentic.session_control.subprocess.run',return_value=SimpleNamespace(returncode=0,stdout='python -m other_service --db '+db+' ')):
            self.assertFalse(process_matches(42,db))
        with patch('martin_agentic.session_control.subprocess.run',return_value=SimpleNamespace(returncode=0,stdout='python -m martin_agentic live-session --db '+db+' --cycles 2')):
            self.assertTrue(process_matches(42,db))
            self.assertFalse(process_matches(42,'runtime/another.sqlite'))
    def test_legacy_none_process_is_compatible_but_local_is_not_inferred(self):
        db=str((Path('runtime/legacy.sqlite')).resolve())
        legacy='python -m martin_agentic live-session --db '+db+' --cycles 2 '
        explicit_none=legacy+'--worker-adapter none '
        explicit_local=legacy+'--worker-adapter local-worker-review '
        with patch('martin_agentic.session_control.subprocess.run',
                   return_value=SimpleNamespace(returncode=0,stdout=legacy)):
            self.assertTrue(process_matches(42,db,worker_adapter='none'))
            self.assertFalse(process_matches(42,db,worker_adapter='local-worker-review'))
        with patch('martin_agentic.session_control.subprocess.run',
                   return_value=SimpleNamespace(returncode=0,stdout=explicit_none)):
            self.assertTrue(process_matches(42,db,worker_adapter='none'))
            self.assertFalse(process_matches(42,db,worker_adapter='local-worker-review'))
        with patch('martin_agentic.session_control.subprocess.run',
                   return_value=SimpleNamespace(returncode=0,stdout=explicit_local)):
            self.assertFalse(process_matches(42,db,worker_adapter='none'))
            self.assertTrue(process_matches(42,db,worker_adapter='local-worker-review'))

    def test_process_identity_includes_composition_and_each_plugin_flag(self):
        db = str(Path('runtime/composition.sqlite').resolve())
        command = ('python -m martin_agentic live-session --db ' + db
                   + ' --strategy external --strategy-plugin a --strategy-plugin b ')
        with patch('martin_agentic.session_control.subprocess.run',
                   return_value=SimpleNamespace(returncode=0, stdout=command)):
            self.assertTrue(process_matches(42, db, strategy='external',
                                            strategy_plugins=('a', 'b')))
            self.assertFalse(process_matches(42, db, strategy='external',
                                             strategy_plugins=('a', 'c')))
            self.assertFalse(process_matches(42, db, strategy='martin'))

    def test_stale_pid_of_other_process_is_never_signalled(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory);db=path/'live.sqlite';status=path/'status.json';metadata=path/'control.json'
            status.write_text(json.dumps({'pid':42}))
            with patch('martin_agentic.session_control.process_matches',return_value=False),patch('martin_agentic.session_control.os.kill') as kill:
                result=session_control('stop',db,'tests/fixtures/policies/live_trial_2026-10-16.json',status_file=status,metadata_file=metadata)
                self.assertTrue(result['stopped']);kill.assert_not_called()
    def test_starting_second_instance_is_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory);status=path/'status.json';status.write_text(json.dumps({'pid':42}))
            metadata=path/'control.json';metadata.write_text(json.dumps({'pid':42,'watchdog_pid':43,'db':str((path/'live.sqlite').resolve()),'require_watchdog':True}))
            with patch('martin_agentic.session_control.process_matches',return_value=True),patch('martin_agentic.session_control.watchdog_process_matches',return_value=True),patch('martin_agentic.session_control.subprocess.Popen') as spawn:
                result=session_control('start',path/'live.sqlite','tests/fixtures/policies/live_trial_2026-10-16.json',status_file=status,metadata_file=path/'control.json')
                self.assertTrue(result['already_running']);self.assertTrue(result['paired_processes_verified']);spawn.assert_not_called()
    def test_controller_never_accepts_write_mode(self):
        with self.assertRaises(ValueError):session_control('start','runtime/x.sqlite','tests/fixtures/policies/live_trial_2026-10-16.json',agent_mode='execute')

    def test_missing_owner_files_with_runtime_lock_never_spawn_or_claim_stopped(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory);db=path/'live.sqlite'
            with single_instance(db),patch('martin_agentic.session_control.subprocess.Popen') as spawn,patch('martin_agentic.session_control.os.kill') as kill:
                for mode in ('start','status','stop'):
                    result=session_control(mode,db,'tests/fixtures/policies/live_trial_2026-10-16.json',
                        status_file=path/'missing-status.json',metadata_file=path/'missing-control.json')
                    self.assertEqual(result['reason'],'RUNTIME_LOCK_HELD_OWNER_UNVERIFIED')
                    self.assertTrue(result['runtime_lock_held']);self.assertIsNone(result['process_running'])
                    self.assertFalse(result['safe_to_start']);self.assertFalse(result['owner_verified'])
                    if mode=='start':self.assertFalse(result['started'])
                    elif mode=='stop':self.assertFalse(result['stopped']);self.assertEqual(result['signals_sent'],0)
                spawn.assert_not_called();kill.assert_not_called()
                self.assertFalse((path/'missing-control.json').exists())

    def test_adapter_mismatch_is_specific_for_status_start_and_stop(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory);db=path/'live.sqlite';status=path/'status.json';meta=path/'control.json'
            status.write_text(json.dumps({'pid':42}))
            meta.write_text(json.dumps({'pid':42,'db':str(db.resolve()),'worker_adapter':'none'}))
            def matches(pid,db_path,command_marker=None,worker_adapter=None):
                return worker_adapter is None
            with single_instance(db), \
                    patch('martin_agentic.session_control.process_matches',
                          side_effect=matches), \
                    patch('martin_agentic.session_control.os.kill') as kill, \
                    patch('martin_agentic.session_control.subprocess.Popen') as spawn:
                for mode in ('status','start','stop'):
                    result=session_control(mode,db,
                        'tests/fixtures/policies/live_trial_2026-10-16.json',
                        status_file=status,metadata_file=meta,
                        agent_mode='supervised-host',
                        worker_adapter='local-worker-review')
                    self.assertEqual(result['reason'],'WORKER_ADAPTER_MISMATCH')
                    self.assertFalse(result['worker_adapter_matches'])
                    self.assertTrue(result['worker_adapter_mismatch'])
                    if mode=='start':
                        self.assertFalse(result['started'])
                    elif mode=='stop':
                        self.assertFalse(result['stopped'])
                        self.assertEqual(result['signals_sent'],0)
                kill.assert_not_called()
                spawn.assert_not_called()

    def test_stale_owner_metadata_with_runtime_lock_is_not_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory);db=path/'live.sqlite';meta=path/'control.json'
            old=json.dumps({'pid':42,'db':str(db.resolve())});meta.write_text(old)
            with single_instance(db),patch('martin_agentic.session_control.process_matches',return_value=False),patch('martin_agentic.session_control.subprocess.Popen') as spawn:
                result=session_control('start',db,'tests/fixtures/policies/live_trial_2026-10-16.json',
                    status_file=path/'missing.json',metadata_file=meta)
                self.assertFalse(result['started']);spawn.assert_not_called()
                self.assertEqual(meta.read_text(),old)

    def test_start_child_immediate_failure_does_not_publish_alive_metadata_or_retry(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory);meta=path/'control.json'
            child=SimpleNamespace(pid=42,poll=lambda:1)
            with patch('martin_agentic.session_control.subprocess.Popen',return_value=child) as spawn:
                result=session_control('start',path/'live.sqlite','tests/fixtures/policies/live_trial_2026-10-16.json',
                    symbols=['AAPL','AMD','HOOD','NVDA','TSLA','META','PLTR','SOFI','IONQ','RKLB'],
                    status_file=path/'status.json',metadata_file=meta)
                self.assertFalse(result['started']);self.assertEqual(result['exit_code'],1)
                self.assertEqual(result['reason'],'CHILD_EXITED_BEFORE_HEALTH_VERIFICATION')
                self.assertEqual(spawn.call_count,1);self.assertFalse(meta.exists())

    def test_free_runtime_lock_and_no_process_are_explicitly_safe_to_start(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)
            result=session_control('status',path/'live.sqlite','tests/fixtures/policies/live_trial_2026-10-16.json',
                status_file=path/'status.json',metadata_file=path/'meta.json')
            self.assertFalse(result['process_running']);self.assertFalse(result['runtime_lock_held'])
            self.assertTrue(result['safe_to_start'])


class PairedSessionControlTests(TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name).resolve();self.db=self.root/'live.sqlite'
        self.status=self.root/'status.json';self.metadata=self.root/'control.json'
        self.policy=Path('tests/fixtures/policies/live_trial_2026-10-19.json').resolve()
        self.symbols=['AAPL','AMD','HOOD','NVDA','TSLA','META','PLTR','SOFI','IONQ','RKLB']
        self.target='martin_agentic.session_control.'

    def run_control(self,mode='start',**kwargs):
        return session_control(mode,self.db,self.policy,status_file=self.status,metadata_file=self.metadata,
                               symbols=self.symbols,**kwargs)

    def recorded(self):
        self.metadata.write_text(json.dumps({'pid':42,'watchdog_pid':43,'db':str(self.db),'agent_mode':'review','require_watchdog':True}))

    def test_watchdog_identity_binds_module_database_policy_and_status_file(self):
        command=f'python -m martin_agentic watchdog --db {self.db} --policy {self.policy} --status-file {self.status} --cycles 1000'
        with patch(self.target+'subprocess.run',return_value=SimpleNamespace(returncode=0,stdout=command)):
            self.assertTrue(watchdog_process_matches(43,self.db,self.policy,self.status))
            self.assertFalse(watchdog_process_matches(43,self.db,self.policy,self.root/'other.json'))
            self.assertFalse(watchdog_process_matches(43,self.db,self.root/'other-policy.json',self.status))
            self.assertFalse(watchdog_process_matches(43,self.root/'other.sqlite',self.policy,self.status))
        with patch(self.target+'subprocess.run',return_value=SimpleNamespace(returncode=0,stdout=command.replace(' watchdog ',' live-session '))):
            self.assertFalse(watchdog_process_matches(43,self.db,self.policy,self.status))

    def test_new_control_start_requests_independent_watchdog_without_execute_or_health_claim(self):
        children=[SimpleNamespace(pid=42,poll=lambda:None),SimpleNamespace(pid=43,poll=lambda:None)]
        with patch(self.target+'subprocess.Popen',side_effect=children) as spawn:
            result=self.run_control(agent_mode='disabled')
        self.assertTrue(result['started']);self.assertFalse(result['health_verified'])
        self.assertTrue(result['watchdog_start_requested']);self.assertEqual(result['watchdog_pid'],43)
        calls=[c.args[0] for c in spawn.call_args_list]
        self.assertEqual(calls[0][3],'live-session');self.assertEqual(calls[0][calls[0].index('--agent-mode')+1],'disabled')
        self.assertIn('--require-watchdog',calls[0])
        self.assertEqual(calls[1][3],'watchdog')
        for arguments in calls:
            for key,value in [('--db',str(self.db)),('--policy',str(self.policy)),('--status-file',str(self.status))]:
                self.assertEqual(arguments[arguments.index(key)+1],value)
        record=json.loads(self.metadata.read_text());self.assertEqual(record['watchdog_pid'],43)
        self.assertFalse(record['launchd_qualified'])

    def test_worker_adapter_is_an_explicit_process_identity(self):
        children=[SimpleNamespace(pid=42,poll=lambda:None),SimpleNamespace(pid=43,poll=lambda:None)]
        with patch(self.target+'subprocess.Popen',side_effect=children) as spawn:
            result=self.run_control(agent_mode='supervised-host',
                                    worker_adapter='local-worker-review')
        self.assertTrue(result['started'])
        args=spawn.call_args_list[0].args[0]
        self.assertEqual(args[args.index('--worker-adapter')+1],'local-worker-review')
        record=json.loads(self.metadata.read_text())
        self.assertEqual(record['worker_adapter'],'local-worker-review')
        capability=result['worker_adapter_capability']
        self.assertFalse(capability['official_queue'])
        self.assertTrue(capability['local_worker_event'])
        self.assertTrue(capability['review_only'])
        self.assertFalse(capability['formal_adapter_qualified'])
        self.assertTrue(capability['requires_native_confirmation'])
        self.assertNotIn('--budget-entry-limit',args)
        other=SimpleNamespace(returncode=0,stdout='python -m martin_agentic live-session --db '+str(self.db)+' --worker-adapter none ')
        with patch(self.target+'subprocess.run',return_value=other):
            self.assertFalse(process_matches(42,self.db,worker_adapter='local-worker-review'))
            self.assertTrue(process_matches(42,self.db,worker_adapter='none'))

    def test_external_composition_forwards_each_plugin_as_its_own_flag(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            children = [SimpleNamespace(pid=42, poll=lambda: None),
                        SimpleNamespace(pid=43, poll=lambda: None)]
            with patch('martin_agentic.session_control.subprocess.Popen',
                       side_effect=children) as spawn:
                result = session_control(
                    'start', path / 'live.sqlite',
                    'tests/fixtures/policies/live_trial_2026-10-16.json',
                    symbols=['AAPL','AMD','HOOD','NVDA','TSLA','META','PLTR','SOFI','IONQ','RKLB'],
                    status_file=path / 'status.json',
                    metadata_file=path / 'control.json',
                    agent_mode='disabled', strategy='external',
                    strategy_plugins=['example-momentum', 'example-mean-reversion'])
            self.assertTrue(result['started'])
            args = spawn.call_args_list[0].args[0]
            self.assertEqual([args[i + 1] for i, value in enumerate(args)
                              if value == '--strategy-plugin'],
                             ['example-momentum', 'example-mean-reversion'])
            self.assertEqual(args[args.index('--strategy') + 1], 'external')

    def test_worker_adapter_requires_supervised_host(self):
        with self.assertRaisesRegex(ValueError,'LOCAL_WORKER_ADAPTER_REQUIRES_SUPERVISED_HOST'):
            self.run_control(agent_mode='disabled',worker_adapter='local-worker-review')

    def test_script_modes_keep_zero_model_budget_on_managed_start(self):
        for mode in ('disabled','script-review','supervised-host'):
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as directory:
                path=Path(directory)
                children=[SimpleNamespace(pid=42,poll=lambda:None),SimpleNamespace(pid=43,poll=lambda:None)]
                with patch(self.target+'subprocess.Popen',side_effect=children) as spawn:
                    result=session_control('start',path/'live.sqlite',self.policy,symbols=self.symbols,
                        status_file=path/'status.json',metadata_file=path/'control.json',agent_mode=mode)
                self.assertTrue(result['started'])
                args=spawn.call_args_list[0].args[0]
                self.assertEqual(args[args.index('--budget-entry-limit')+1],'0')
                self.assertEqual(args[args.index('--budget-total-limit')+1],'0')
                self.assertEqual(args[args.index('--poll-seconds')+1],'5')
                self.assertIn('--until-close',args)
                guard_args=spawn.call_args_list[1].args[0]
                self.assertIn('--stop-on-safe-close',guard_args)
                self.assertEqual(guard_args[guard_args.index('--expected-control-pid')+1],'42')

    def test_existing_control_missing_watchdog_repairs_only_observer(self):
        self.recorded()
        with patch(self.target+'process_matches',return_value=True),patch(self.target+'watchdog_process_matches',return_value=False),patch(self.target+'subprocess.Popen',return_value=SimpleNamespace(pid=44,poll=lambda:None)) as spawn:
            result=self.run_control()
        self.assertFalse(result['started']);self.assertTrue(result['already_running'])
        self.assertEqual(result['pid'],42);self.assertEqual(result['watchdog_pid'],44)
        spawn.assert_called_once();self.assertEqual(spawn.call_args.args[0][3],'watchdog')

    def test_unknown_watchdog_lock_owner_prevents_start_and_never_signals_foreign_pid(self):
        self.recorded()
        with single_instance(str(self.db)+'.watchdog'),patch(self.target+'process_matches',return_value=False),patch(self.target+'watchdog_process_matches',return_value=False),patch(self.target+'subprocess.Popen') as spawn,patch(self.target+'os.kill') as kill:
            status=self.run_control('status');self.assertFalse(status['safe_to_start']);self.assertTrue(status['attention_required'])
            started=self.run_control();self.assertFalse(started['started']);self.assertEqual(started['reason'],'WATCHDOG_LOCK_HELD_OWNER_UNVERIFIED')
            stopped=self.run_control('stop');self.assertFalse(stopped['watchdog_stopped']);self.assertTrue(stopped['attention_required'])
            spawn.assert_not_called();kill.assert_not_called()
        gate=json.loads(Path(str(self.db)+'.entry-inhibit.json').read_text());self.assertTrue(gate['blocked'])

    def test_failed_watchdog_spawn_inhibits_and_stops_only_just_created_control(self):
        child=SimpleNamespace(pid=42,poll=lambda:None)
        with patch(self.target+'subprocess.Popen',side_effect=[child,OSError('fixture spawn failure')]),patch(self.target+'_stop_created_control',return_value=True) as stop:
            result=self.run_control()
        self.assertFalse(result['started']);self.assertTrue(result['new_entries_blocked'])
        self.assertTrue(result['created_control_stopped']);stop.assert_called_once_with(child)
        self.assertTrue(json.loads(Path(str(self.db)+'.entry-inhibit.json').read_text())['blocked'])
        self.assertTrue(json.loads(self.metadata.read_text())['pairing_failed'])

    def test_failed_observer_repair_does_not_signal_existing_management_process(self):
        self.recorded()
        with patch(self.target+'process_matches',return_value=True),patch(self.target+'watchdog_process_matches',return_value=False),patch(self.target+'subprocess.Popen',side_effect=OSError('fixture spawn failure')),patch(self.target+'_stop_created_control') as stop,patch(self.target+'os.kill') as kill:
            result=self.run_control()
        self.assertTrue(result['already_running']);self.assertTrue(result['new_entries_blocked'])
        self.assertFalse(result['created_control_stopped']);stop.assert_not_called();kill.assert_not_called()

    def test_immediate_watchdog_exit_never_reports_a_paired_start(self):
        child=SimpleNamespace(pid=42,poll=lambda:None)
        with patch(self.target+'subprocess.Popen',side_effect=[child,SimpleNamespace(pid=43,poll=lambda:2)]),patch(self.target+'_stop_created_control',return_value=False):
            result=self.run_control()
        self.assertFalse(result['started']);self.assertTrue(result['attention_required'])
        self.assertFalse(result['created_control_stopped']);self.assertFalse(result['watchdog_running'])

    def test_control_spawn_error_leaves_sticky_inhibit_and_no_metadata_claim(self):
        with patch(self.target+'subprocess.Popen',side_effect=OSError('fixture spawn failure')):
            result=self.run_control()
        self.assertEqual(result['reason'],'CONTROL_CHILD_SPAWN_FAILED');self.assertFalse(result['started'])
        self.assertTrue(result['new_entries_blocked']);self.assertFalse(self.metadata.exists())

    def test_old_unmanaged_control_is_never_reported_as_guarded_by_observer_repair(self):
        self.recorded();record=json.loads(self.metadata.read_text());record.pop('require_watchdog');self.metadata.write_text(json.dumps(record))
        with patch(self.target+'process_matches',return_value=True),patch(self.target+'watchdog_process_matches',return_value=False),patch(self.target+'subprocess.Popen',return_value=SimpleNamespace(pid=44,poll=lambda:None)):
            result=self.run_control()
        self.assertTrue(result['requires_control_restart_for_guard']);self.assertTrue(result['attention_required'])
        self.assertFalse(json.loads(self.metadata.read_text())['require_watchdog'])
        self.assertTrue(json.loads(Path(str(self.db)+'.entry-inhibit.json').read_text())['blocked'])

    def test_failure_stops_created_control_even_if_inhibit_file_cannot_be_written(self):
        child=SimpleNamespace(pid=42,poll=lambda:None)
        with patch(self.target+'subprocess.Popen',side_effect=[child,OSError('fixture observer failure')]),patch(self.target+'_inhibit_failed_start',side_effect=OSError('fixture disk failure')),patch(self.target+'_stop_created_control',return_value=True) as stop:
            result=self.run_control()
        stop.assert_called_once_with(child);self.assertTrue(result['created_control_stopped'])
        self.assertFalse(result['entry_inhibit_written']);self.assertTrue(result['new_entries_blocked'])

    def test_stop_preserves_observer_when_final_flat_claim_lacks_independent_proof(self):
        self.recorded();self.status.write_text(json.dumps({'db':str(self.db),'pid':42,
            'reason':'AUTHORIZED_SESSION_CLOSED','final_flat_verified':True,'observed_at':'2026-10-20T12:00:00+00:00'}))
        with patch(self.target+'process_matches',return_value=False),patch(self.target+'watchdog_process_matches',return_value=True),patch(self.target+'_stop_watchdog') as stop:
            result=self.run_control('stop')
        self.assertTrue(result['watchdog_preserved']);self.assertTrue(result['attention_required']);stop.assert_not_called()

    def test_safe_closed_stop_joins_observer_only_with_fresh_exact_independent_proof(self):
        from datetime import datetime,timezone
        self.recorded();at=datetime.now(timezone.utc).isoformat()
        self.status.write_text(json.dumps({'db':str(self.db),'pid':42,'reason':'AUTHORIZED_SESSION_CLOSED',
            'final_flat_verified':True,'observed_at':at}))
        watcher={'db':str(self.db),'pid':43,'status_file':str(self.status),'checked_at':at,
            'closed_safe':True,'issues':[],'control_status_observed_at':at,'control_status_reason':'AUTHORIZED_SESSION_CLOSED'}
        path=Path(str(self.db)+'.watchdog.json');path.write_text(json.dumps(watcher))
        with patch(self.target+'process_matches',return_value=False),patch(self.target+'watchdog_process_matches',return_value=True),patch(self.target+'_stop_watchdog',return_value={'stopped':True,'signals_sent':1}) as stop:
            result=self.run_control('stop')
            self.assertTrue(result['watchdog_stopped']);stop.assert_called_once_with(43,self.db,self.policy,self.status)
            watcher['control_status_observed_at']='another-snapshot';path.write_text(json.dumps(watcher));stop.reset_mock()
            result=self.run_control('stop');self.assertTrue(result['watchdog_preserved']);stop.assert_not_called()

    def test_management_that_does_not_stop_keeps_observer_running(self):
        self.recorded()
        with patch(self.target+'process_matches',return_value=True),patch(self.target+'watchdog_process_matches',return_value=True),patch(self.target+'os.kill') as kill,patch(self.target+'time.sleep'),patch(self.target+'_stop_watchdog') as stop:
            result=self.run_control('stop')
        self.assertFalse(result['stopped']);self.assertTrue(result['watchdog_preserved']);stop.assert_not_called()
        kill.assert_called_once()
