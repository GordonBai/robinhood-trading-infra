"""Real framed children and actual warm Martin composition, synthetic only."""
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
import os
import struct
import sys
import time
import tempfile
from unittest import TestCase
from unittest.mock import patch

from trading_contracts.strategy import StrategyIdentity,StrategyInput
from trading_infra.strategy_stream import WarmStrategyRunner
import test_signal_host_isolation as fixture
from shadow_fixture import events


LOOP='''
import os,struct,sys,time
from trading_contracts.codec import decode_input,encode_decision
from trading_contracts.strategy import StrategyDecision

def exact(n):
 b=bytearray()
 while len(b)<n:
  c=sys.stdin.buffer.read(n-len(b))
  if not c:sys.exit(0)
  b.extend(c)
 return bytes(b)
while True:
 r=decode_input(exact(struct.unpack('!I',exact(4))[0]))
 d=StrategyDecision(identity=r.identity,decision_id='child-decision',as_of=r.as_of,
 data_snapshot_id=r.data_snapshot_id,consumed_cursor=r.cursor,synthetic=r.synthetic,
 next_checkpoint={'value':r.market.get('value','ok')},
 diagnostics={'pid':os.getpid(),'environment_keys':sorted(os.environ)})
 raw=encode_decision(d)
 FRAME_WRITE
'''
SUCCESS=LOOP.replace('FRAME_WRITE',"sys.stdout.buffer.write(struct.pack('!I',len(raw))+raw);sys.stdout.buffer.flush()")


class StrategyStreamTests(TestCase):
    books=fixture.SignalHostIsolationTests.books
    event=fixture.SignalHostIsolationTests.event

    def request(self, **changes):
        fields=dict(identity=StrategyIdentity('instance','test-plugin','v1'),
            as_of=datetime(2026,10,29,15,tzinfo=timezone.utc),cursor='event',
            data_snapshot_id='snapshot',source_sha256='a'*64,config_version='config',
            synthetic=True,market={'value':Decimal('1.25')})
        fields.update(changes);return StrategyInput(**fields)

    def runner(self,code=SUCCESS,**options):
        runner=WarmStrategyRunner([sys.executable,'-I','-c',code],**options)
        self.addCleanup(runner.close);return runner

    def finish(self,runner,timeout=5):
        deadline=time.monotonic()+timeout
        while time.monotonic()<deadline:
            result=runner.poll()
            if result is not None:return result
            time.sleep(.001)
        self.fail('bounded framed child completion timed out')

    def test_multiple_requests_reuse_child_and_do_not_inherit_secrets(self):
        runner=self.runner(timeout_seconds=3)
        with patch.dict(os.environ,{'BROKER_TEST_SECRET':'never-inherit','AWS_SECRET_ACCESS_KEY':'fixture'}):
            runner.start(self.request());first=self.finish(runner);child=runner.process
        self.assertEqual(first.next_checkpoint['value'],Decimal('1.25'))
        self.assertNotIn('BROKER_TEST_SECRET',first.diagnostics['environment_keys'])
        self.assertNotIn('AWS_SECRET_ACCESS_KEY',first.diagnostics['environment_keys'])
        self.assertIsNone(child.poll());self.assertIsNone(runner.next_deadline_monotonic)
        self.assertIsNone(runner.poll())
        for index in range(10):
            runner.start(self.request(cursor='next-'+str(index),market={'value':Decimal(index)}))
            result=self.finish(runner)
            self.assertEqual(result.consumed_cursor,'next-'+str(index))
            self.assertEqual(result.next_checkpoint['value'],Decimal(index))
            self.assertEqual(result.diagnostics['pid'],child.pid)
            self.assertIs(runner.process,child)
        runner.close();runner.close();self.assertIsNotNone(child.poll())
        self.assertTrue(all(s.closed for s in (child.stdin,child.stdout,child.stderr)))

    def test_fragmented_header_and_payload_are_reassembled(self):
        writer="""frame=struct.pack('!I',len(raw))+raw
 for offset in range(0,len(frame),11):
  sys.stdout.buffer.write(frame[offset:offset+11]);sys.stdout.buffer.flush();time.sleep(.0002)"""
        runner=self.runner(LOOP.replace('FRAME_WRITE',writer),timeout_seconds=3)
        runner.start(self.request());self.assertEqual(self.finish(runner).next_checkpoint['value'],Decimal('1.25'))
        runner.start(self.request(cursor='again'));self.assertEqual(self.finish(runner).consumed_cursor,'again')

    def test_backpressure_and_outstanding_request_rejection(self):
        runner=self.runner('import time;time.sleep(.05)\n'+SUCCESS,timeout_seconds=3)
        request=self.request(market={'items':['x'*500]*1000})
        runner.start(request)
        with self.assertRaisesRegex(ValueError,'REQUEST_IN_FLIGHT'):runner.start(self.request())
        self.assertIsNone(runner.poll())
        self.assertLess(runner._input_offset,len(runner._input))
        self.assertEqual(self.finish(runner).next_checkpoint['value'],'ok')

    def test_timeout_kills_owned_child_and_replacement_is_clean(self):
        # The command sleeps only for the specifically requested cursor.
        code=SUCCESS.replace("d=StrategyDecision", "\n if r.cursor=='hang':time.sleep(10)\n d=StrategyDecision")
        runner=self.runner(code,timeout_seconds=.1)
        runner.start(self.request(cursor='hang'));child=runner.process
        with self.assertRaisesRegex(ValueError,'STRATEGY_PROCESS_TIMEOUT'):self.finish(runner)
        self.assertIsNotNone(child.poll());self.assertIsNone(runner.process)
        runner.timeout_seconds=3;runner.start(self.request(cursor='recovered'))
        self.assertEqual(self.finish(runner).consumed_cursor,'recovered')
        self.assertNotEqual(runner.process.pid,child.pid)

    def test_oversize_mismatch_invalid_and_extra_frames_are_rejected(self):
        cases=[(LOOP.replace('FRAME_WRITE',"sys.stdout.buffer.write(struct.pack('!I',16777217));sys.stdout.buffer.flush()"),'FRAME_LIMIT'),
            (SUCCESS.replace('consumed_cursor=r.cursor','consumed_cursor="wrong"'),'INVALID_OUTPUT'),
            (LOOP.replace('FRAME_WRITE',"sys.stdout.buffer.write(struct.pack('!I',3)+b'bad');sys.stdout.buffer.flush()"),'INVALID_OUTPUT'),
            (LOOP.replace('FRAME_WRITE',"sys.stdout.buffer.write(struct.pack('!I',len(raw))+raw+b'extra');sys.stdout.buffer.flush()"),'EXTRA_OUTPUT'),
            ('import sys;sys.stderr.write("private-error");sys.exit(8)','CRASHED')]
        for code,reason in cases:
            with self.subTest(reason=reason):
                runner=self.runner(code,timeout_seconds=3);runner.start(self.request());child=runner.process
                with self.assertRaisesRegex(ValueError,'STRATEGY_PROCESS_'+reason) as error:self.finish(runner)
                self.assertNotIn('private-error',str(error.exception));self.assertIsNotNone(child.poll())

    def test_stderr_is_bounded_and_no_model_output_is_returned(self):
        code=LOOP.replace('FRAME_WRITE',"sys.stderr.buffer.write(b'x'*100000);sys.stderr.buffer.flush();time.sleep(5)")
        runner=self.runner(code,timeout_seconds=3,max_output_bytes=1024)
        runner.start(self.request());child=runner.process
        with self.assertRaisesRegex(ValueError,'STDERR_LIMIT'):self.finish(runner)
        self.assertIsNotNone(child.poll())

    def test_actual_martin_warm_worker_matches_complete_inline_trace(self):
        with tempfile.TemporaryDirectory() as directory:
            _,host=self.books(directory,cost='0.10')
            runner=WarmStrategyRunner([sys.executable,'-I','-m','martin_luk.strategy_stream_worker',
                '--cost-buffer-per-share','0.10'],timeout_seconds=5)
            self.addCleanup(runner.close);child=None;count=0
            for raw in events()[1:]:
                event=self.event(raw);self.now=self.now.fromisoformat(event['at'])
                prepared=host.prepare(event)
                if prepared is None:continue
                expected=host.plugin.decide(prepared.request)
                runner.start(prepared.request);actual=self.finish(runner)
                self.assertEqual(actual,expected)
                if child is None:child=runner.process
                self.assertIs(runner.process,child)
                host.commit(prepared,actual);count+=1
            self.assertGreater(count,4);self.assertIsNone(child.poll())

    def test_actual_worker_rejects_wrong_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            _,host=self.books(directory,cost='0.10');event=self.event(events()[1]);self.now=self.now.fromisoformat(event['at'])
            runner=WarmStrategyRunner([sys.executable,'-I','-m','martin_luk.strategy_stream_worker'],timeout_seconds=5)
            self.addCleanup(runner.close);runner.start(host.prepare(event).request);child=runner.process
            with self.assertRaisesRegex(ValueError,'CRASHED'):self.finish(runner)
            self.assertIsNotNone(child.poll())

    def test_idle_unsolicited_frame_cannot_become_next_request_result(self):
        code=SUCCESS.replace('sys.stdout.buffer.flush()',
            "sys.stdout.buffer.flush();time.sleep(.02);sys.stdout.buffer.write(struct.pack('!I',len(raw))+raw);sys.stdout.buffer.flush()")
        runner=self.runner(code,timeout_seconds=3);runner.start(self.request())
        self.finish(runner);child=runner.process;time.sleep(.04)
        with self.assertRaisesRegex(ValueError,'UNSOLICITED_OUTPUT'):runner.poll()
        self.assertIsNotNone(child.poll())

    def test_partial_response_that_never_finishes_times_out_and_reaps(self):
        code=LOOP.replace('FRAME_WRITE',"sys.stdout.buffer.write(struct.pack('!I',len(raw))+raw[:5]);sys.stdout.buffer.flush();time.sleep(10)")
        runner=self.runner(code,timeout_seconds=.1);runner.start(self.request());child=runner.process
        with self.assertRaisesRegex(ValueError,'TIMEOUT'):self.finish(runner)
        self.assertIsNotNone(child.poll())
