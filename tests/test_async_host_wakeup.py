"""Detached synthetic adapters cannot freeze the control owner or clear UNKNOWN."""
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
from unittest import TestCase
from unittest.mock import patch
import uuid

from martin_agentic.async_host_wakeup import AsyncHostWakeDispatcher
from martin_agentic.host_mailbox import read_private_json
from martin_agentic.host_wakeup import HostWakeOutbox


class SyntheticQueue:
    capability={'official_queue':True,'durable_idempotency':True,'cooperative_cancellation':True}
    def __init__(self, gate=None, accepted=True):
        self.gate=gate;self.accepted=accepted;self.calls=[];self.entered=threading.Event()
    def enqueue(self,event,**kwargs):
        self.calls.append((event,kwargs,threading.get_ident()));self.entered.set()
        if self.gate:self.gate.wait(3)
        return {'event_id':event['event_id'],'attempt_id':kwargs['attempt_id'],
                'accepted':self.accepted,'receipt_id':'synthetic-only-queue-receipt','retryable':True}


class AsyncHostWakeTests(TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
        self.db=sqlite3.connect(self.root/'owner.sqlite')
        self.now=datetime(2026,10,27,14,tzinfo=timezone.utc)
        self.binding={'db':str(self.root/'owner.sqlite'),'account':'••••0000','account_key':'a'*64,
            'policy_sha256':'b'*64,'source_fingerprint':'c'*64,'epoch':str(uuid.uuid4())}
        self.box=HostWakeOutbox(self.db,clock=lambda:self.now)
        self.dispatchers=[];self.gates=[]
    def tearDown(self):
        for gate in self.gates:gate.set()
        for dispatcher in self.dispatchers:
            dispatcher.close()
            if dispatcher.task.thread:dispatcher.task.thread.join(3)
            if dispatcher.pending:dispatcher.close()
        self.db.close();self.tmp.cleanup()
    def event(self,action='protect',kind='protect'):
        return self.box.enqueue(binding=self.binding,action_id=action,kind=kind,revision='d'*64,
            expires_at=(self.now+timedelta(seconds=30)).isoformat())
    def dispatcher(self,adapter=None,timeout=.2):
        value=AsyncHostWakeDispatcher(self.box,self.root/'receipts',adapter=adapter,timeout_seconds=timeout)
        self.dispatchers.append(value);return value
    def wait_result(self,dispatcher):
        end=time.monotonic()+2
        while time.monotonic()<end:
            result=dispatcher.tick(binding=self.binding)
            if dispatcher.pending is None and result.get('state') not in {None,'STARTED'}:return result
            time.sleep(.002)
        self.fail('synthetic callback failed to finish')

    def test_default_adapter_never_starts_callback_or_claims_delivery(self):
        event=self.event();dispatcher=self.dispatcher()
        result=dispatcher.tick(binding=self.binding)
        self.assertEqual(result['state'],'BLOCKED');self.assertEqual(result['attempts'],0)
        self.assertIsNone(dispatcher.task.thread);self.assertFalse(result['execution_authorized'])

    def test_complete_queue_io_detached_and_durably_bound_before_owner_import(self):
        event=self.event();queue=SyntheticQueue();dispatcher=self.dispatcher(queue)
        result=dispatcher.tick(binding=self.binding)
        self.assertEqual(result['status'],'WAKE_QUEUE_IO_STARTED')
        result=self.wait_result(dispatcher)
        self.assertEqual(result['state'],'ACCEPTED')
        self.assertNotEqual(queue.calls[0][2],threading.get_ident())
        self.assertFalse(result['host_execution_verified']);self.assertFalse(result['broker_effect_verified'])
        receipt=self.root/'receipts'/(result['detail']['attempt_id']+'.json')
        self.assertEqual(receipt.stat().st_mode&0o777,0o600)
        self.assertEqual(read_private_json(receipt)['reservation']['event_id'],event)

    def test_ignored_timeout_does_not_block_control_and_late_acceptance_stays_unknown(self):
        event=self.event();gate=threading.Event();self.gates.append(gate)
        queue=SyntheticQueue(gate);dispatcher=self.dispatcher(queue,timeout=.02)
        dispatcher.tick(binding=self.binding);self.assertTrue(queue.entered.wait(.2))
        ticks=0;deadline=time.monotonic()+.06
        while time.monotonic()<deadline:
            dispatcher.tick(binding=self.binding)
            self.db.execute('CREATE TABLE IF NOT EXISTS independent_owner_ticks(n INTEGER)')
            self.db.execute('INSERT INTO independent_owner_ticks VALUES(?)',(ticks,));self.db.commit()
            ticks+=1;time.sleep(.001)
        self.assertGreater(ticks,10);self.assertEqual(self.box.get(event)['state'],'UNKNOWN')
        self.assertTrue(queue.calls[0][1]['cancellation'].is_set())
        other=self.event('exit','exit')
        for _ in range(5):dispatcher.tick(binding=self.binding)
        self.assertEqual(len(queue.calls),1);self.assertEqual(self.box.get(other)['attempts'],0)
        gate.set();result=self.wait_result(dispatcher)
        self.assertEqual(result['state'],'UNKNOWN');self.assertTrue(result['detail']['late_queue_accepted'])
        self.assertEqual(self.box.get(event)['attempts'],1)

    def test_cancellation_deadline_fires_without_waiting_for_owner_tick(self):
        event=self.event();gate=threading.Event();self.gates.append(gate)
        queue=SyntheticQueue(gate);dispatcher=self.dispatcher(queue,timeout=.02)
        dispatcher.tick(binding=self.binding);self.assertTrue(queue.entered.wait(.2))
        self.assertTrue(queue.calls[0][1]['cancellation'].wait(.2))
        # The timer touches detached cancellation only. The database remains
        # exclusively owner-managed until this explicit deadline observation.
        self.assertEqual(self.box.get(event)['state'],'STARTED')
        self.assertEqual(dispatcher.tick(binding=self.binding)['state'],'UNKNOWN')

    def test_early_superseded_completed_callback_is_unknown_before_deadline(self):
        event=self.event();queue=SyntheticQueue();dispatcher=self.dispatcher(queue)
        dispatcher.tick(binding=self.binding);dispatcher.task.thread.join(1)
        self.box.retire_superseded(binding=self.binding,current_event_ids=[])
        self.assertEqual(self.box.get(event)['state'],'STARTED')
        result=dispatcher.tick(binding=self.binding,current_event_ids=[])
        self.assertEqual(result['state'],'UNKNOWN')
        self.assertEqual(result['detail']['reason'],'QUEUE_ACTION_SUPERSEDED_NO_AUTOMATIC_REPLAY')
        self.assertTrue(result['detail']['late_queue_accepted'])

    def test_wall_clock_expiry_before_import_cannot_accept_old_queue_work(self):
        event=self.event();queue=SyntheticQueue();dispatcher=self.dispatcher(queue)
        dispatcher.tick(binding=self.binding);dispatcher.task.thread.join(1)
        self.now+=timedelta(seconds=31)
        result=dispatcher.tick(binding=self.binding)
        self.assertEqual(result['state'],'UNKNOWN')
        self.assertTrue(result['detail']['late_queue_accepted'])

    def test_changed_epoch_before_import_preserves_old_attempt_unknown(self):
        event=self.event();queue=SyntheticQueue();dispatcher=self.dispatcher(queue)
        dispatcher.tick(binding=self.binding);dispatcher.task.thread.join(1)
        changed={**self.binding,'epoch':str(uuid.uuid4())}
        result=dispatcher.tick(binding=changed)
        self.assertEqual(result['state'],'UNKNOWN')
        self.assertEqual(result['detail']['reason'],'QUEUE_SESSION_CHANGED_NO_AUTOMATIC_REPLAY')

    def test_owner_close_retires_completed_but_unimported_receipt_conservatively(self):
        event=self.event();queue=SyntheticQueue();dispatcher=self.dispatcher(queue)
        dispatcher.tick(binding=self.binding);dispatcher.task.thread.join(1)
        self.assertEqual(dispatcher.close()['pending_result']['state'],'UNKNOWN')
        self.assertTrue(self.box.get(event)['detail']['late_queue_accepted'])

    def test_next_deadline_notifies_once_without_hot_loop_after_timeout(self):
        event=self.event();gate=threading.Event();self.gates.append(gate)
        queue=SyntheticQueue(gate);dispatcher=self.dispatcher(queue,timeout=.02)
        self.assertIsNone(dispatcher.next_deadline_monotonic)
        dispatcher.tick(binding=self.binding)
        self.assertIsNotNone(dispatcher.next_deadline_monotonic)
        self.assertTrue(queue.entered.wait(.2));self.assertTrue(queue.calls[0][1]['cancellation'].wait(.2))
        dispatcher.tick(binding=self.binding)
        self.assertIsNone(dispatcher.next_deadline_monotonic)

    def test_retained_unknown_attempt_persists_once_across_ticks_close_and_late_receipt(self):
        event=self.event();gate=threading.Event();self.gates.append(gate)
        queue=SyntheticQueue(gate);dispatcher=self.dispatcher(queue,timeout=.02)
        dispatcher.tick(binding=self.binding);self.assertTrue(queue.entered.wait(.2))
        self.assertTrue(queue.calls[0][1]['cancellation'].wait(.2))
        with patch.object(self.box,'mark_attempt_unknown',wraps=self.box.mark_attempt_unknown) as mark:
            first=dispatcher.tick(binding=self.binding)
            first['detail']['reason']='caller mutation must not alter retained evidence'
            for _ in range(10):
                result=dispatcher.tick(binding=self.binding)
                self.assertEqual(result['detail']['reason'],'QUEUE_DEADLINE_EXCEEDED_NO_AUTOMATIC_REPLAY')
                self.assertTrue(result['callback_retained'])
            dispatcher.close();dispatcher.close()
            self.assertEqual(mark.call_count,1)
            gate.set();dispatcher.task.thread.join(1)
            result=dispatcher.close()['pending_result']
            self.assertEqual(mark.call_count,1)
        self.assertEqual(result['state'],'UNKNOWN')
        self.assertTrue(result['detail']['late_queue_accepted'])
        self.assertEqual(self.box.get(event)['attempts'],1)

    def test_shutdown_is_nonblocking_and_preserves_owned_unknown_callback(self):
        event=self.event();gate=threading.Event();self.gates.append(gate)
        queue=SyntheticQueue(gate);dispatcher=self.dispatcher(queue)
        dispatcher.tick(binding=self.binding);self.assertTrue(queue.entered.wait(.2))
        started=time.monotonic();result=dispatcher.close()
        self.assertLess(time.monotonic()-started,.1)
        self.assertTrue(result['callback_retained']);self.assertTrue(result['callback_alive'])
        self.assertEqual(self.box.get(event)['state'],'UNKNOWN')
        gate.set();dispatcher.task.thread.join(1);dispatcher.close()
        self.assertEqual(self.box.get(event)['state'],'UNKNOWN')

    def test_failed_unknown_commit_is_retried_before_caching(self):
        event=self.event();gate=threading.Event();self.gates.append(gate)
        queue=SyntheticQueue(gate);dispatcher=self.dispatcher(queue,timeout=.02)
        dispatcher.tick(binding=self.binding);self.assertTrue(queue.entered.wait(.2))
        self.assertTrue(queue.calls[0][1]['cancellation'].wait(.2))
        with patch.object(self.box,'mark_attempt_unknown',side_effect=sqlite3.OperationalError('synthetic commit failure')):
            with self.assertRaises(sqlite3.OperationalError):dispatcher.tick(binding=self.binding)
        self.assertFalse(dispatcher.pending['timeout_recorded'])
        self.assertEqual(self.box.get(event)['state'],'STARTED')
        self.assertEqual(dispatcher.tick(binding=self.binding)['state'],'UNKNOWN')
        self.assertTrue(dispatcher.pending['timeout_recorded'])

    def test_unsent_old_revision_expires_but_inflight_is_not_forgiven(self):
        event=self.event();gate=threading.Event();self.gates.append(gate)
        queue=SyntheticQueue(gate);dispatcher=self.dispatcher(queue)
        dispatcher.tick(binding=self.binding);self.assertTrue(queue.entered.wait(.2))
        self.box.retire_superseded(binding=self.binding,current_event_ids=[])
        self.assertEqual(self.box.get(event)['state'],'STARTED')
        dispatcher.close();self.assertEqual(self.box.get(event)['state'],'UNKNOWN')

    def test_uncooperative_contract_not_eligible_for_async_transport(self):
        event=self.event();queue=SyntheticQueue();queue.capability={
            'official_queue':True,'durable_idempotency':True}
        dispatcher=self.dispatcher(queue);result=dispatcher.tick(binding=self.binding)
        self.assertEqual(result['state'],'BLOCKED');self.assertEqual(queue.calls,[])

    def test_corrupt_persisted_receipt_is_not_accepted(self):
        event=self.event();queue=SyntheticQueue();dispatcher=self.dispatcher(queue)
        dispatcher.tick(binding=self.binding);dispatcher.task.thread.join(1)
        receipt=next((self.root/'receipts').glob('*.json'))
        value=read_private_json(receipt);value['receipt']['receipt_id']='tampered'
        receipt.write_text(json.dumps(value))
        result=dispatcher.tick(binding=self.binding)
        self.assertEqual(result['state'],'UNKNOWN');self.assertFalse(result['detail']['late_receipt_valid'])

    def test_task_start_failure_is_unknown_not_rearmed(self):
        event=self.event();dispatcher=self.dispatcher(SyntheticQueue())
        with patch.object(dispatcher.task,'start',side_effect=RuntimeError('cannot start')):
            result=dispatcher.tick(binding=self.binding)
        self.assertEqual(result['state'],'UNKNOWN');self.assertIsNone(dispatcher.pending)
        self.assertEqual(self.box.get(event)['attempts'],1)

    def test_worker_never_receives_outbox_sqlite_or_trading_authority(self):
        self.event();queue=SyntheticQueue();dispatcher=self.dispatcher(queue)
        dispatcher.tick(binding=self.binding);self.wait_result(dispatcher)
        event,kwargs,thread=queue.calls[0]
        self.assertFalse(event['execution_authorized']);self.assertFalse(event['broker_effect_verified'])
        self.assertEqual(set(kwargs),{'idempotency_key','attempt_id','timeout_seconds','cancellation'})
        self.assertLessEqual(kwargs['timeout_seconds'],.2)


class HostBridgeWakeIsolationTests(TestCase):
    def test_hung_wake_adapter_does_not_stop_normal_owner_reconciliation(self):
        import test_native_operations
        from martin_agentic.host_execution import HostExecutionBridge
        fixture=test_native_operations.NativeOperationTests();fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        gate=threading.Event();queue=SyntheticQueue(gate)
        bridge=HostExecutionBridge(fixture.book,Path(fixture.f.tmp.name)/'async-wake-bridge',
            wake_adapter=queue,wake_timeout_seconds=.02)
        try:
            packet,response=fixture.review();fixture.ops.record(packet['operation_id'],response)
            bridge._publish();self.assertTrue(queue.entered.wait(.2))
            started=time.monotonic();ticks=0
            while time.monotonic()-started<.07:
                fixture.book.reconcile(fixture.snapshot())
                result=bridge(snapshot=fixture.snapshot(),quotes={'DEMO':fixture.f.quote},allow_dispatch=False)
                self.assertFalse(result['execution_authorized'])
                self.assertEqual(result['broker_writes_by_bridge'],0)
                ticks+=1
            self.assertGreater(ticks,3)
            event=queue.calls[0][0]['event_id']
            self.assertEqual(bridge.wake.get(event)['state'],'UNKNOWN')
            self.assertEqual(len(queue.calls),1)
            self.assertTrue(queue.calls[0][1]['cancellation'].is_set())
            self.assertTrue(fixture.book.audit.verify()['integrity_valid'])
        finally:
            gate.set()
            if bridge.wake_dispatcher.task.thread:bridge.wake_dispatcher.task.thread.join(2)
            bridge.close()
