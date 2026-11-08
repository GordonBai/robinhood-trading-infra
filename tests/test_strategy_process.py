"""Actual owned fake children; no broker, account or execution database I/O."""
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
import os
import subprocess
import sys
import threading
import time
from unittest import TestCase
from unittest.mock import patch

from trading_contracts.strategy import StrategyIdentity, StrategyInput
from trading_infra.strategy_process import ProcessStrategyRunner


SUCCESS = '''
import os,sys
from trading_contracts.codec import decode_input,encode_decision
from trading_contracts.strategy import StrategyDecision
r=decode_input(sys.stdin.buffer.read())
d=StrategyDecision(identity=r.identity,decision_id='child-decision',as_of=r.as_of,
 data_snapshot_id=r.data_snapshot_id,consumed_cursor=r.cursor,synthetic=r.synthetic,
 next_checkpoint={'value':r.market.get('value','ok')},
 diagnostics={'environment_keys':sorted(os.environ),'private_workdir':os.path.realpath(os.getcwd())==os.path.realpath(os.environ['TMPDIR'])})
sys.stdout.buffer.write(encode_decision(d))
'''


class StrategyProcessTests(TestCase):
    def request(self, **changes):
        fields=dict(identity=StrategyIdentity('instance','test-plugin','v1'),
            as_of=datetime(2026,10,29,15,tzinfo=timezone.utc),cursor='event',
            data_snapshot_id='snapshot',source_sha256='a'*64,config_version='config',
            synthetic=True,market={'value':Decimal('1.25')})
        fields.update(changes)
        return StrategyInput(**fields)

    def runner(self, code, **changes):
        runner=ProcessStrategyRunner([sys.executable,'-I','-c',code],**changes)
        self.addCleanup(runner.close)
        return runner

    def finish(self, runner, *, timeout=3):
        end=time.monotonic()+timeout
        while time.monotonic()<end:
            result=runner.poll()
            if result is not None:return result
            time.sleep(.002)
        self.fail('runner did not complete within bounded test wait')

    def assert_reaped(self, child):
        self.assertIsNotNone(child.poll())
        self.assertTrue(all(stream.closed for stream in (child.stdin,child.stdout,child.stderr)))

    def test_typed_roundtrip_minimal_environment_and_reusable_completed_runner(self):
        runner=self.runner(SUCCESS,timeout_seconds=2)
        with patch.dict(os.environ,{'BROKER_TEST_SECRET':'must-not-inherit','AWS_SECRET_ACCESS_KEY':'fixture'}):
            runner.start(self.request())
            child=runner.process;decision=self.finish(runner)
        self.assertEqual(decision.next_checkpoint['value'],Decimal('1.25'))
        self.assertTrue(decision.diagnostics['private_workdir'])
        # macOS may add its own non-secret locale variable after exec; it is
        # not copied from the parent's credential-bearing environment.
        self.assertLessEqual(set(decision.diagnostics['environment_keys']),
            {'PATH','LANG','LC_ALL','LC_CTYPE','TMPDIR','__CF_USER_TEXT_ENCODING'})
        self.assertFalse(hasattr(decision,'book'));self.assert_reaped(child)
        self.assertIsNone(runner.process);self.assertIsNone(runner.next_deadline_monotonic)
        runner.start(replace(self.request(),cursor='next-event'))
        self.assertEqual(self.finish(runner).consumed_cursor,'next-event')

    def test_blocked_child_does_not_block_owner_ticks_and_times_out_reaped(self):
        runner=self.runner('import sys,time;sys.stdin.buffer.read();time.sleep(10)',timeout_seconds=.08)
        started=time.monotonic();runner.start(self.request());child=runner.process
        self.assertLess(time.monotonic()-started,1)
        ticks=0;error=None;end=time.monotonic()+2
        while time.monotonic()<end:
            try:self.assertIsNone(runner.poll())
            except ValueError as exc:error=exc;break
            ticks+=1;time.sleep(.002)
        self.assertGreater(ticks,3);self.assertEqual(str(error),'STRATEGY_PROCESS_TIMEOUT')
        self.assert_reaped(child)

    def test_stdin_backpressure_is_nonblocking_for_large_typed_input(self):
        code='import time;time.sleep(.08)\n'+SUCCESS
        request=self.request(market={'items':['x'*500]*1000})
        runner=self.runner(code,timeout_seconds=3)
        runner.start(request);child=runner.process
        self.assertLess(runner._input_offset,len(runner._input))
        self.assertIsNone(runner.poll())
        decision=self.finish(runner)
        self.assertEqual(decision.next_checkpoint['value'],'ok');self.assert_reaped(child)

    def test_crash_and_malformed_output_are_bounded_failures(self):
        cases=[('import sys;sys.stderr.write("private-upstream-message");sys.exit(7)',
                'STRATEGY_PROCESS_CRASHED'),
               ('import sys;sys.stdin.buffer.read();sys.stdout.buffer.write(b"not-json")',
                'STRATEGY_PROCESS_INVALID_OUTPUT'),
               (SUCCESS.replace('consumed_cursor=r.cursor','consumed_cursor="wrong-event"'),
                'STRATEGY_PROCESS_INVALID_OUTPUT')]
        for code, expected in cases:
            with self.subTest(expected=expected):
                runner=self.runner(code,timeout_seconds=2);runner.start(self.request());child=runner.process
                with self.assertRaisesRegex(ValueError,'^'+expected+'$') as raised:self.finish(runner)
                self.assertNotIn('private-upstream',str(raised.exception));self.assert_reaped(child)

    def test_stdout_and_stderr_are_bounded_even_when_child_floods(self):
        for stream,expected in [('stdout','STRATEGY_PROCESS_STDOUT_LIMIT'),('stderr','STRATEGY_PROCESS_STDERR_LIMIT')]:
            with self.subTest(stream=stream):
                code='import sys,time;sys.stdin.buffer.read();sys.'+stream+'.buffer.write(b"x"*1000000);sys.'+stream+'.buffer.flush();time.sleep(5)'
                runner=self.runner(code,timeout_seconds=2,max_output_bytes=1024)
                runner.start(self.request());child=runner.process
                with self.assertRaisesRegex(ValueError,'^'+expected+'$'):self.finish(runner)
                self.assert_reaped(child)

    def test_outstanding_request_cannot_be_replaced_and_close_reaps_only_owned_child(self):
        runner=self.runner('import time;time.sleep(10)',timeout_seconds=5)
        runner.start(self.request());child=runner.process
        with self.assertRaisesRegex(ValueError,'REQUEST_IN_FLIGHT'):runner.start(self.request())
        self.assertIs(runner.process,child)
        runner.close();runner.close();self.assert_reaped(child)
        self.assertIsNone(runner.poll())
        with self.assertRaisesRegex(ValueError,'CLOSED'):runner.start(self.request())

    def test_failed_reap_retains_handle_for_explicit_close_retry(self):
        runner=self.runner('import time;time.sleep(10)')
        runner.start(self.request());child=runner.process
        with patch.object(child,'wait',side_effect=subprocess.TimeoutExpired('owned-child',1)):
            with self.assertRaisesRegex(ValueError,'REAP_PENDING'):runner.close()
        self.assertIs(runner.process,child)
        runner.close();self.assert_reaped(child);self.assertIsNone(runner.process)

    def test_owner_and_configuration_validation(self):
        for args in ({'timeout_seconds':True},{'timeout_seconds':float('nan')},
                     {'max_output_bytes':True},{'max_output_bytes':0}):
            with self.subTest(args=args),self.assertRaises(ValueError):
                ProcessStrategyRunner([sys.executable],**args)
        with self.assertRaises(ValueError):ProcessStrategyRunner('shell command')
        runner=self.runner('import time;time.sleep(10)');runner.start(self.request())
        errors=[]
        def wrong_owner():
            try:runner.poll()
            except ValueError as exc:errors.append(str(exc))
        thread=threading.Thread(target=wrong_owner);thread.start();thread.join(1)
        self.assertEqual(errors,['STRATEGY_PROCESS_OWNER_REQUIRED'])

    def test_start_filesystem_failure_is_classified_without_private_path(self):
        runner=self.runner(SUCCESS)
        with patch('trading_infra.strategy_process.tempfile.TemporaryDirectory',
                side_effect=OSError('private-filesystem-detail')):
            with self.assertRaisesRegex(ValueError,'^STRATEGY_PROCESS_START_FAILED$') as raised:
                runner.start(self.request())
        self.assertNotIn('private-filesystem',str(raised.exception))
        self.assertIsNone(runner.process)
        runner.start(self.request())
        self.assertIsNotNone(self.finish(runner))
