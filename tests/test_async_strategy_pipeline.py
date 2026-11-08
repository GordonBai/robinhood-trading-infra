"""Default process-host composition through actual queue and synthetic outbox."""
from datetime import datetime
import json
from pathlib import Path
import tempfile
import time
from unittest import TestCase
from unittest.mock import patch

from entry_authority_fixture import TestExecutionBook as ExecutionBook
from martin_luk.live_session import LiveSupervisor,wait_for_result_or_tick
from martin_agentic.qualification import source_fingerprint
from martin_luk.live_signals import LiveSignals
from martin_agentic.control_tasks import BackgroundTask
from shadow_fixture import events
from test_broker import snapshot


class AsyncStrategyPipelineTests(TestCase):
    def test_default_daemon_composition_uses_worker_and_preserves_proposal_deadline(self):
        with tempfile.TemporaryDirectory() as directory:
            policy=json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text());policy['session_date']='2026-10-15'
            now=[datetime.fromisoformat('2026-10-15T09:25:00-04:00')]
            book=ExecutionBook(Path(directory)/'process.sqlite',policy,simulation=True,clock=lambda:now[0]);self.addCleanup(book.close)
            supervisor=LiveSupervisor(book,process_strategy=True,owner_source=source_fingerprint())
            self.addCleanup(supervisor.strategy_queue.close)
            legacy=ExecutionBook(Path(directory)/'inline.sqlite',policy,simulation=True,clock=lambda:now[0]);self.addCleanup(legacy.close)
            inline=LiveSignals(legacy)
            child_ids=set()
            for raw in events()[1:5]:
                event={**raw,'synthetic_input':True,'source_sha256':'fixture-'+raw['id']}
                now[0]=datetime.fromisoformat(event['at']);observed=snapshot(at=event['at']);observed['synthetic']=True
                inline.consume(event)
                with patch.object(supervisor.signals.plugin,'decide',side_effect=AssertionError('owner called inline strategy')):
                    supervisor.step(observed,[event],allow_entries=False)
                    deadline=time.monotonic()+5
                    while supervisor.strategy_queue.status()['pending'] and time.monotonic()<deadline:
                        wait_for_result_or_tick(BackgroundTask('no-broker-or-market-read'),None,.01,
                            strategy_supervisor=supervisor)
                        if supervisor.strategy_queue.runner.process:child_ids.add(supervisor.strategy_queue.runner.process.pid)
                self.assertEqual(supervisor.strategy_queue.status()['pending'],0)
                self.assertEqual(supervisor._strategy_pending_errors,[])
                for key,value in inline.state().items():
                    if key not in ('pending_signals','signal_results'):self.assertEqual(supervisor.signals.state()[key],value)
            self.assertEqual(len(child_ids),1)
            candidate=supervisor.signals.pending()[0]
            self.assertIn('strategy_valid_until',candidate['evidence'])
            observed=snapshot(at=now[0].isoformat());observed['synthetic']=True
            result=supervisor.step(observed,[],allow_entries=True)
            self.assertEqual(result['errors'],[])
            action=book.actions()[0];payload=json.loads(action['payload'])
            self.assertEqual(action['state'],'READY')
            self.assertEqual(datetime.fromisoformat(payload['expires_at']),datetime.fromisoformat(candidate['evidence']['strategy_valid_until']))
            self.assertFalse(result['dispatch']['agent_called'])
            self.assertEqual(book.db.execute("SELECT count(*) FROM strategy_tasks WHERE state='COMMITTED'").fetchone()[0],4)

    def test_worker_rejects_owner_source_pin_mismatch_before_consumption(self):
        with tempfile.TemporaryDirectory() as directory:
            policy=json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text());policy['session_date']='2026-10-15'
            at=datetime.fromisoformat('2026-10-15T09:25:00-04:00')
            book=ExecutionBook(Path(directory)/'pin.sqlite',policy,simulation=True,clock=lambda:at);self.addCleanup(book.close)
            supervisor=LiveSupervisor(book,process_strategy=True,owner_source='0'*64)
            self.addCleanup(supervisor.strategy_queue.close)
            event={**events()[1],'synthetic_input':True,'source_sha256':'fixture-source-pin'}
            supervisor.strategy_queue.enqueue(event);errors=[];deadline=time.monotonic()+5
            while supervisor.strategy_queue.status()['pending'] and time.monotonic()<deadline:
                errors.extend(supervisor.advance_strategy()['errors']);time.sleep(.001)
            self.assertEqual(supervisor.strategy_queue.status()['rejected'],1)
            self.assertEqual(len(errors),1)
            self.assertIn(errors[0]['reason'],{'STRATEGY_PROCESS_CRASHED','STRATEGY_PROCESS_INPUT_NOT_DELIVERED'})
            self.assertEqual(book.db.execute('SELECT count(*) FROM live_inputs').fetchone()[0],0)
            self.assertEqual(book.actions(),[])
