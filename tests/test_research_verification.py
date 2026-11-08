"""Registered replay verification uses isolated synthetic fixtures only."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sqlite3
from unittest import TestCase
from unittest.mock import patch

from martin_luk.research import strategy_freeze, _real_coverage
from martin_luk.research_verification import verify_research_run, _reproduce
import test_research


class ResearchVerificationTests(TestCase):
    def setUp(self):
        self.fixture=test_research.ResearchTests('test_registered_replay_retains_no_signal_folds_and_never_claims_profitability')
        self.fixture.setUp();self.addCleanup(self.fixture.doCleanups)
        self.run=self.fixture.run_research()
        self.registry=Path(self.run['registry_path']);self.report=Path(self.run['report_path'])

    def verify(self,**kwargs):
        return verify_research_run(self.registry,self.run['run_id'],self.fixture.path,**kwargs)

    def test_exact_replay_of_all_fields_without_new_runs_or_input_changes(self):
        paths=[self.registry,self.report,self.fixture.path,self.fixture.root/'source.json',self.fixture.root/'events.json']
        before={p:p.read_bytes() for p in paths}
        result=self.verify()
        self.assertTrue(result['reproducible'],result)
        self.assertEqual(result['input_data_status'],'SYNTHETIC_ONLY')
        self.assertFalse(result['data_contract_qualified'])
        self.assertFalse(result['research_admitted']);self.assertFalse(result['production_ready'])
        self.assertEqual(result['registered_result_sha256'],result['reproduced_result_sha256'])
        self.assertEqual(result['new_runs_registered'],0)
        for path in paths:self.assertEqual(path.read_bytes(),before[path])
        connection=sqlite3.connect(self.registry)
        try:self.assertEqual(connection.execute('SELECT count(*) FROM runs').fetchone()[0],1)
        finally:connection.close()
        self.assertIn('results',result['exact_result_fields_compared'])

    def test_altered_report_is_not_a_registered_result(self):
        changed=json.loads(self.report.read_text());changed['results'][0]['net_trading_pnl_usd']='9999'
        self.report.write_text(json.dumps(changed))
        result=self.verify();self.assertFalse(result['reproducible'])
        self.assertIn('VERIFICATION_REPORT_REGISTRY_BYTES_MISMATCH',result['blockers'])

    def test_consistently_forged_registry_and_report_still_fail_full_replay(self):
        changed=json.loads(self.report.read_text());changed['results'][0]['journal'][0]['outcome']='FORGED'
        encoded=json.dumps(changed,ensure_ascii=False,indent=2)
        connection=sqlite3.connect(self.registry)
        connection.execute('UPDATE runs SET result=? WHERE id=?',(encoded,self.run['run_id']));connection.commit();connection.close()
        self.report.write_text(encoded+'\n')
        result=self.verify();self.assertFalse(result['reproducible'])
        self.assertIn('VERIFICATION_EXACT_REPLAY_RESULT_MISMATCH',result['blockers'])

    def test_modified_source_or_dataset_is_not_verified(self):
        (self.fixture.root/'source.json').write_text('tampered')
        result=self.verify();self.assertFalse(result['reproducible'])
        self.assertIn('VERIFICATION_FROZEN_SOURCE_HASH_MISMATCH',result['blockers'])

    def test_changed_manifest_fails_raw_registered_binding(self):
        self.fixture.manifest['costs']['per_share_fee']='0';self.fixture.save()
        self.assertIn('VERIFICATION_MANIFEST_REGISTRY_MISMATCH',self.verify()['blockers'])

    def test_current_strategy_change_invalidates_old_run(self):
        freeze=deepcopy(strategy_freeze());freeze['version']='changed'
        with patch('martin_luk.research_verification.strategy_freeze',return_value=freeze):
            result=self.verify()
        self.assertIn('VERIFICATION_STRATEGY_FREEZE_MISMATCH',result['blockers'])

    def test_source_replacement_during_replay_is_detected(self):
        source=self.fixture.root/'source.json'
        def replaced(*args,**kwargs):
            output=_reproduce(*args,**kwargs)
            replacement=source.with_name('replacement.json');replacement.write_bytes(source.read_bytes());replacement.replace(source)
            return output
        with patch('martin_luk.research_verification._reproduce',side_effect=replaced):result=self.verify()
        self.assertIn('VERIFICATION_SOURCE_CHANGED_DURING_REPLAY',result['blockers'])

    def test_registry_change_during_replay_is_detected(self):
        def changed(*args,**kwargs):
            output=_reproduce(*args,**kwargs)
            connection=sqlite3.connect(self.registry)
            connection.execute("UPDATE runs SET started_at='changed' WHERE id=?",(self.run['run_id'],));connection.commit();connection.close()
            return output
        with patch('martin_luk.research_verification._reproduce',side_effect=changed):result=self.verify()
        self.assertIn('VERIFICATION_REGISTRY_CHANGED_DURING_REPLAY',result['blockers'])

    def test_report_change_during_replay_is_detected(self):
        def changed(*args,**kwargs):
            output=_reproduce(*args,**kwargs);self.report.write_text('{}');return output
        with patch('martin_luk.research_verification._reproduce',side_effect=changed):result=self.verify()
        self.assertIn('VERIFICATION_DOCUMENT_CHANGED_DURING_REPLAY',result['blockers'])

    def test_terminal_data_gap_is_reproduced_but_never_data_qualified(self):
        self.fixture.manifest['synthetic_input']=False
        self.fixture.manifest['sources'][0]['synthetic']=False
        data_path=self.fixture.root/'events.json';events=json.loads(data_path.read_text())
        for event in events:event['synthetic_input']=False
        data_path.write_text(json.dumps(events));self.fixture.manifest['dataset']['sha256']=hashlib.sha256(data_path.read_bytes()).hexdigest()
        self.fixture.save();self.run=self.fixture.run_research();self.report=Path(self.run['report_path'])
        result=self.verify();self.assertTrue(result['reproducible'],result)
        self.assertEqual(result['registered_status'],'DATA_GAP')
        self.assertEqual(result['input_data_status'],'DATA_GAP')
        self.assertFalse(result['data_contract_qualified']);self.assertFalse(result['research_admitted'])
        self.assertTrue(result['data_gaps'])

    def test_registered_future_data_failure_is_reproducible_without_becoming_valid(self):
        data_path=self.fixture.root/'events.json';events=json.loads(data_path.read_text())
        events[-1]['data']['ask_at']='2026-10-17T15:55:00-04:00';data_path.write_text(json.dumps(events))
        self.fixture.manifest['dataset']['sha256']=hashlib.sha256(data_path.read_bytes()).hexdigest()
        self.fixture.save();self.run=self.fixture.run_research();self.report=Path(self.run['report_path'])
        self.assertEqual(self.run['status'],'FAILED')
        result=self.verify();self.assertTrue(result['reproducible'],result)
        self.assertEqual(result['input_data_status'],'INVALID_RESEARCH_INPUT')
        self.assertFalse(result['data_contract_qualified'])

    def test_nonterminal_registry_row_cannot_be_verified(self):
        connection=sqlite3.connect(self.registry)
        connection.execute("UPDATE runs SET status='STARTED',result=NULL WHERE id=?",(self.run['run_id'],));connection.commit();connection.close()
        self.assertIn('VERIFICATION_REGISTERED_RUN_NOT_TERMINAL',self.verify()['blockers'])

    def test_missing_registry_never_created(self):
        missing=self.registry.with_name('missing.sqlite')
        result=verify_research_run(missing,self.run['run_id'],self.fixture.path)
        self.assertFalse(result['reproducible']);self.assertFalse(missing.exists())

    def test_resource_limits_reject_before_replay(self):
        with patch('martin_luk.research_verification._replay',side_effect=AssertionError('bounded first')):
            result=self.verify(limits={'max_events':1})
        self.assertIn('VERIFICATION_REPLAY_WORK_LIMIT_EXCEEDED',result['blockers'])
        result=self.verify(limits={'max_document_bytes':1})
        self.assertIn('VERIFICATION_REGISTERED_DOCUMENT_LIMIT_EXCEEDED',result['blockers'])

    def test_boolean_schema_versions_are_rejected(self):
        self.fixture.manifest['schema_version']=True;self.fixture.save();run=self.fixture.run_research()
        self.assertEqual(run['status'],'FAILED');self.assertIn('RESEARCH_SCHEMA_UNSUPPORTED',run['error'])
        manifest={'coverage':{'schema_version':True}}
        self.assertEqual(_real_coverage([],manifest,{}),['REAL_COVERAGE_CONTRACT_REQUIRED'])

    def test_input_symlink_is_not_followed(self):
        source=self.fixture.root/'source.json';target=source.with_name('actual.json');source.rename(target);source.symlink_to(target)
        self.assertIn('VERIFICATION_SYMLINK_INPUT_UNSUPPORTED',self.verify()['blockers'])

    def test_fifo_input_is_rejected_without_blocking_or_running_replay(self):
        import os
        import subprocess
        import sys
        source=self.fixture.root/'source.json';source.unlink();os.mkfifo(source)
        command="import json,sys;from martin_luk.research_verification import verify_research_run;print(json.dumps(verify_research_run(sys.argv[1],sys.argv[2],sys.argv[3])))"
        completed=subprocess.run([sys.executable,'-c',command,str(self.registry),self.run['run_id'],str(self.fixture.path)],
            capture_output=True,text=True,timeout=3)
        self.assertEqual(completed.returncode,0,completed.stderr)
        self.assertIn('VERIFICATION_REGULAR_FILE_REQUIRED',json.loads(completed.stdout)['blockers'])

    def test_wal_registry_requires_consistent_snapshot_without_creating_sidecars(self):
        connection=sqlite3.connect(self.registry)
        self.assertEqual(connection.execute('PRAGMA journal_mode=WAL').fetchone()[0],'wal')
        connection.close()
        sidecars=[Path(str(self.registry)+suffix) for suffix in ('-wal','-shm')]
        self.assertFalse(any(path.exists() for path in sidecars))
        before=self.registry.read_bytes()
        with patch('martin_luk.research_verification.sqlite3.connect',side_effect=AssertionError('reject WAL before SQLite opens')):
            result=self.verify()
        self.assertIn('VERIFICATION_REGISTRY_CONSISTENT_SNAPSHOT_REQUIRED',result['blockers'])
        self.assertEqual(self.registry.read_bytes(),before)
        self.assertFalse(any(path.exists() for path in sidecars))

    def test_declared_coverage_work_is_bounded_before_load(self):
        self.fixture.manifest['coverage']={'folds':{fold['id']:{'universe_symbols':['DEMO']*100}
            for fold in self.fixture.manifest['folds']}}
        self.fixture.save();self.run=self.fixture.run_research();self.report=Path(self.run['report_path'])
        with patch('martin_luk.research_verification._load',side_effect=AssertionError('coverage budget before validator')):
            result=self.verify(limits={'max_coverage_work_units':1})
        self.assertIn('VERIFICATION_COVERAGE_WORK_LIMIT_EXCEEDED',result['blockers'])

    def test_strategy_change_during_replay_is_not_accepted(self):
        original=strategy_freeze();changed=deepcopy(original);changed['version']='changed-after'
        with patch('martin_luk.research_verification.strategy_freeze',side_effect=[original,changed]):
            result=self.verify()
        self.assertIn('VERIFICATION_STRATEGY_CHANGED_DURING_REPLAY',result['blockers'])

    def test_existing_absolute_and_parent_source_references_keep_manifest_semantics(self):
        nested=self.fixture.root/'manifest-folder';nested.mkdir()
        self.fixture.path=nested/'manifest.json'
        self.fixture.manifest['sources'][0]['path']='../source.json'
        self.fixture.manifest['dataset']['path']=str(self.fixture.root/'events.json')
        self.fixture.save();self.run=self.fixture.run_research();self.report=Path(self.run['report_path'])
        self.assertEqual(self.run['status'],'SIMULATED_COMPLETE',self.run.get('error'))
        result=self.verify();self.assertTrue(result['reproducible'],result)
        self.assertEqual(result['registered_result_sha256'],result['reproduced_result_sha256'])

    def test_duplicate_source_declarations_cannot_bypass_work_limits(self):
        source=deepcopy(self.fixture.manifest['sources'][0])
        self.fixture.manifest['sources']=[deepcopy(source) for _ in range(5)]
        self.fixture.save();self.run=self.fixture.run_research();self.report=Path(self.run['report_path'])
        with patch('martin_luk.research_verification._load',side_effect=AssertionError('limit before loader')):
            result=self.verify(limits={'max_input_files':4})
        self.assertFalse(result['reproducible'])
        self.assertIn('VERIFICATION_INPUT_FILE_LIMIT_EXCEEDED',result['blockers'])

    def test_repeated_large_source_bytes_are_budgeted_even_when_unique_files_fit(self):
        source=deepcopy(self.fixture.manifest['sources'][0])
        self.fixture.manifest['sources']=[deepcopy(source) for _ in range(5)]
        self.fixture.save();self.run=self.fixture.run_research();self.report=Path(self.run['report_path'])
        unique=sum((self.fixture.root/name).stat().st_size for name in ('source.json','events.json'))
        with patch('martin_luk.research_verification._load',side_effect=AssertionError('limit before loader')):
            result=self.verify(limits={'max_input_bytes':unique+1})
        self.assertFalse(result['reproducible'])
        self.assertIn('VERIFICATION_DECLARED_INPUT_WORK_BYTES_LIMIT_EXCEEDED',result['blockers'])
