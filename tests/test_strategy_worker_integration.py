"""Real subprocess Martin computation committed only by the synthetic owner."""
import sys
import time
import tempfile
from unittest import TestCase

import test_signal_host_isolation as fixture
from shadow_fixture import events
from trading_infra.strategy_process import ProcessStrategyRunner


class StrategyWorkerIntegrationTests(TestCase):
    books=fixture.SignalHostIsolationTests.books
    event=fixture.SignalHostIsolationTests.event

    def test_martin_worker_roundtrip_matches_inline_without_owner_lock(self):
        with tempfile.TemporaryDirectory() as directory:
            _,host=self.books(directory)
            runner=ProcessStrategyRunner([sys.executable,'-I','-m','martin_luk.strategy_worker'],timeout_seconds=10)
            self.addCleanup(runner.close)
            event=self.event(events()[1]);self.now=self.now.fromisoformat(event['at'])
            prepared=host.prepare(event)
            expected=host.plugin.decide(prepared.request)
            runner.start(prepared.request)
            deadline=time.monotonic()+15;decision=None;ticks=0
            while decision is None and time.monotonic()<deadline:
                self.assertFalse(host.book.db.in_transaction)
                self.assertEqual(host.book.db.execute('SELECT count(*) FROM live_inputs').fetchone()[0],0)
                decision=runner.poll();ticks+=1
                if decision is None:time.sleep(.005)
            self.assertIsNotNone(decision);self.assertGreater(ticks,1)
            self.assertEqual(decision,expected)
            host.commit(prepared,decision)
            self.assertEqual(host.book.db.execute('SELECT count(*) FROM live_inputs').fetchone()[0],1)
            self.assertTrue(host.state()['universe_complete'])

    def test_blocked_worker_does_not_prevent_owner_protection_tick(self):
        from unittest.mock import Mock
        import test_execution
        from martin_luk.live_session import LiveSupervisor
        with tempfile.TemporaryDirectory() as directory:
            _,signals=self.books(directory);event=self.event(events()[1]);self.now=self.now.fromisoformat(event['at'])
            prepared=signals.prepare(event)
            runner=ProcessStrategyRunner([sys.executable,'-I','-c','import time; time.sleep(60)'],timeout_seconds=10)
            self.addCleanup(runner.close);runner.start(prepared.request)
            self.assertIsNone(runner.poll())
            execution=test_execution.ExecutionTests('test_empty_queue_has_zero_agent_calls')
            execution.setUp();self.addCleanup(execution.tearDown)
            buy=execution.entry_filled();dispatcher=Mock(return_value={'management_tick':True,'broker_writes':0})
            supervisor=LiveSupervisor(execution.book,dispatcher=dispatcher)
            result=supervisor.step(execution.snap([buy],qty='2'),[],allow_entries=False)
            self.assertTrue(result['dispatch']['management_tick'])
            self.assertTrue(any(a['kind']=='protect' and a['state']=='READY' for a in execution.book.actions()))
            self.assertIsNone(runner.process.poll())
            child=runner.process;runner.close();self.assertIsNotNone(child.poll())

    def test_worker_actual_cost_must_match_pinned_owner_config(self):
        with tempfile.TemporaryDirectory() as directory:
            _,host=self.books(directory,cost='0.10');event=self.event(events()[1]);self.now=self.now.fromisoformat(event['at'])
            prepared=host.prepare(event)
            wrong=ProcessStrategyRunner([sys.executable,'-I','-m','martin_luk.strategy_worker'],timeout_seconds=10)
            self.addCleanup(wrong.close);wrong.start(prepared.request)
            deadline=time.monotonic()+15
            with self.assertRaisesRegex(ValueError,'STRATEGY_PROCESS_CRASHED'):
                while time.monotonic()<deadline:
                    wrong.poll();time.sleep(.005)
            self.assertEqual(host.book.db.execute('SELECT count(*) FROM live_inputs').fetchone()[0],0)
            correct=ProcessStrategyRunner([sys.executable,'-I','-m','martin_luk.strategy_worker',
                '--cost-buffer-per-share','0.10'],timeout_seconds=10)
            self.addCleanup(correct.close);correct.start(prepared.request)
            result=None;deadline=time.monotonic()+15
            while result is None and time.monotonic()<deadline:
                result=correct.poll()
                if result is None:time.sleep(.005)
            self.assertEqual(result,host.plugin.decide(prepared.request));host.commit(prepared,result)
