"""Exact protection budget and current-preview handoff regressions; no broker I/O."""
from datetime import timedelta
import json
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from martin_agentic.async_native_review import AsyncNativeReviewDispatcher
from martin_agentic.host_execution import HostExecutionBridge
from martin_agentic.host_mailbox import submit_request,read_request_result,read_private_json
from martin_agentic.ledger import timestamp
import test_native_operations


class ProtectionHandoffBudgetTests(TestCase):
    def setUp(self):
        self.fixture=test_native_operations.NativeOperationTests();self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.f=self.fixture.f;self.book=self.fixture.book;self.ops=self.fixture.ops
        self.directory=Path(self.f.tmp.name)/'host-budget'
        self.bridge=HostExecutionBridge(self.book,self.directory)
        self.addCleanup(self.bridge.close)
        guard=patch('martin_agentic.native_review.EquityReviewTransport.__init__',
            side_effect=AssertionError('no external I/O permitted'))
        guard.start();self.addCleanup(guard.stop)

    def action(self):return next(a for a in self.book.actions() if a['id']==self.fixture.action)

    def advance_to_budget_age(self,seconds):
        deadline=self.bridge.review.protection_deadline(self.action())
        self.f.now=deadline-timedelta(seconds=30-seconds)
        self.f.quote.update(**{k:self.f.now.isoformat() for k in ('bid_at','ask_at','observed_at')})
        return deadline

    def test_host_review_cannot_outlive_actual_exposure_deadline(self):
        deadline=self.advance_to_budget_age(25)
        request=submit_request(self.directory,self.fixture.action,'review',clock=lambda:self.f.now)
        result=self.bridge(snapshot=self.fixture.snapshot(),quotes={'DEMO':self.f.quote})
        self.assertEqual(result['status'],'HOST_PACKET_PREPARED')
        envelope=read_private_json(self.directory/'packets'/(request['request_id']+'.json'))
        self.assertEqual(timestamp(envelope['expires_at']),deadline)
        self.f.now=deadline
        # The mailbox consumer rejects a stale packet before any official call.
        picked=read_request_result(self.directory,request['request_id'],clock=lambda:self.f.now)
        self.assertEqual(picked['status'],'HOST_PACKET_EXPIRED_RECONCILE')
        self.assertNotIn('packet',picked)
        result=self.bridge(allow_dispatch=False)
        self.assertEqual(result['status'],'HOST_REVIEW_ABANDONED')
        self.assertIsNone(self.bridge.pending)

    def test_host_place_cannot_gain_extra_quote_ttl_beyond_protection_deadline(self):
        deadline=self.advance_to_budget_age(25)
        packet,response=self.fixture.review();self.ops.record(packet['operation_id'],response)
        request=submit_request(self.directory,self.fixture.action,'place',clock=lambda:self.f.now)
        result=self.bridge(snapshot=self.fixture.snapshot(),quotes={'DEMO':self.f.quote})
        self.assertEqual(result['status'],'HOST_PACKET_PREPARED')
        envelope=read_private_json(self.directory/'packets'/(request['request_id']+'.json'))
        self.assertEqual(timestamp(envelope['expires_at']),deadline)
        self.f.now=deadline
        picked=read_request_result(self.directory,request['request_id'],clock=lambda:self.f.now)
        self.assertEqual(picked['status'],'HOST_PACKET_EXPIRED_RECONCILE')
        self.assertNotIn('packet',picked)
        # Ambiguous writes remain UNKNOWN; only provable reads may be abandoned.
        result=self.bridge(allow_dispatch=False)
        self.assertEqual(result['status'],'HOST_HANDOFF_EXPIRED_RECONCILE')
        self.assertEqual(self.action()['state'],'UNKNOWN')

    def test_manifest_exposes_budget_without_claiming_protection(self):
        deadline=self.advance_to_budget_age(23)
        self.bridge._publish()
        item=read_private_json(self.directory/'manifest.json')['actions'][0]
        self.assertEqual(item['protection_deadline'],deadline.isoformat())
        self.assertEqual(item['protection_seconds_remaining'],7)
        self.assertEqual(item['state'],'READY')

    def test_waiting_uses_latest_abandoned_attempt_not_previous_verified_preview(self):
        previous,response=self.fixture.review();self.ops.record(previous['operation_id'],response)
        newer,_=self.fixture.review();self.ops.abandon_read(newer['operation_id'])
        waiting=self.bridge.review._waiting(self.action())
        self.assertEqual(waiting['status'],'FRESH_REVIEW_REQUIRED')
        self.assertEqual(waiting['operation_id'],newer['operation_id'])
        self.assertEqual(waiting['review_state'],'ABANDONED_READ')
        self.assertNotIn('native_result',waiting)
        self.assertFalse(waiting['execution_authorized'])
        request=submit_request(self.directory,self.fixture.action,'place',clock=lambda:self.f.now)
        result=self.bridge(snapshot=self.fixture.snapshot(),quotes={'DEMO':self.f.quote})
        self.assertEqual(result['status'],'HOST_REQUEST_REJECTED')
        self.assertEqual(result['reason'],'FRESH_NATIVE_REVIEW_REQUIRED')
        self.assertFalse((self.directory/'packets'/(request['request_id']+'.json')).exists())

    def test_waiting_review_is_not_displayed_as_current_after_budget_expiry(self):
        packet,response=self.fixture.review();self.ops.record(packet['operation_id'],response)
        deadline=self.advance_to_budget_age(30)
        waiting=self.bridge.review._waiting(self.action())
        self.assertEqual(waiting['status'],'FRESH_REVIEW_REQUIRED')
        self.assertEqual(waiting['protection_seconds_remaining'],0)
        self.assertEqual(timestamp(waiting['review_valid_until']),deadline)
        # Old confirmation cannot revive the protection once core selects exit.
        with self.assertRaisesRegex(ValueError,'NOT_AVAILABLE|NO_LONGER_CURRENT'):
            self.ops.begin(self.fixture.action,'place',self.fixture.snapshot(),quote=self.f.quote)
        self.assertTrue(any(a['kind']=='exit' and a['state']=='READY' for a in self.book.actions()))

    def test_waiting_management_precedes_older_entry(self):
        packet,response=self.fixture.review();self.ops.record(packet['operation_id'],response)
        original=self.action();earlier=(self.f.now-timedelta(seconds=1)).isoformat()
        payload=json.loads(original['payload']);payload.update(action_id='older-entry',kind='entry',ref_id='older-entry')
        self.book.db.execute('INSERT INTO live_actions VALUES(?,?,?,?,?,?,?,NULL)',
            ('older-entry',original['trade_id'],'entry','WAITING_USER',json.dumps(payload),earlier,earlier))
        self.book.db.commit()
        result=self.bridge.review(allow_dispatch=False)
        self.assertEqual(result['action_id'],self.fixture.action)
        self.assertEqual(result['status'],'WAITING_USER')

    def test_actionable_preview_emits_one_durable_blocked_event_without_polling_model(self):
        packet,response=self.fixture.review();self.ops.record(packet['operation_id'],response)
        self.bridge._publish()
        first=read_private_json(self.directory/'manifest.json')['host_wakeup']
        self.assertEqual(first['state'],'BLOCKED')
        self.assertEqual(first['detail']['reason'],'OFFICIAL_HOST_QUEUE_UNAVAILABLE')
        self.assertFalse(first['execution_authorized']);self.assertFalse(first['host_execution_verified'])
        count=self.book.db.execute('SELECT count(*) FROM host_wake_history').fetchone()[0]
        for _ in range(4):self.bridge._publish()
        self.assertEqual(self.book.db.execute('SELECT count(*) FROM host_wake_events').fetchone()[0],1)
        self.assertEqual(self.book.db.execute('SELECT count(*) FROM host_wake_history').fetchone()[0],count)
        request=submit_request(self.directory,self.fixture.action,'place',clock=lambda:self.f.now)
        result=self.bridge(snapshot=self.fixture.snapshot(),quotes={'DEMO':self.f.quote})
        self.assertEqual(result['status'],'HOST_PACKET_PREPARED')
        self.assertEqual(self.book.db.execute('SELECT count(*) FROM host_wake_events').fetchone()[0],2)
        # Preparing the exact packet supersedes the earlier review wake. A
        # future official adapter must not dispatch that obsolete revision.
        self.assertEqual(self.bridge.wake.get(first['event_id'])['state'],'EXPIRED')
        self.assertEqual(self.book.db.execute('SELECT sum(attempts) FROM host_wake_events').fetchone()[0],0)
        self.assertEqual(self.book.db.execute("SELECT count(*) FROM external_operations WHERE operation='place'").fetchone()[0],1)

    def test_bridge_close_immediately_retires_pending_wake_without_delivery(self):
        packet,response=self.fixture.review();self.ops.record(packet['operation_id'],response)
        self.bridge._publish()
        event=read_private_json(self.directory/'manifest.json')['host_wakeup']['event_id']
        self.bridge.close()
        self.assertEqual(self.bridge.wake.get(event)['state'],'EXPIRED')
        self.assertEqual(self.bridge.wake.get(event)['attempts'],0)
        manifest=read_private_json(self.directory/'manifest.json')
        self.assertFalse(manifest['running'])
        self.assertEqual(manifest['host_wakeup']['reason'],'HOST_BRIDGE_NOT_RUNNING')
