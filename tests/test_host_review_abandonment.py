"""Read reservations can settle locally; no such recovery exists for writes."""
from datetime import timedelta
import json
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from martin_agentic.control_tasks import worker_lease
from entry_authority_fixture import TestExecutionBook as ExecutionBook
from martin_agentic.host_execution import HostExecutionBridge
from martin_agentic.host_mailbox import submit_request,submit_receipt,read_private_json
from martin_agentic.native_operations import NativeOperations
import test_native_operations


class ReadAbandonmentTests(TestCase):
    def setUp(self):
        self.fixture=test_native_operations.NativeOperationTests();self.fixture.setUp();self.addCleanup(self.fixture.doCleanups)
        self.book=self.fixture.book;self.f=self.fixture.f;self.ops=self.fixture.ops

    def action(self):return next(a for a in self.book.actions() if a['id']==self.fixture.action)

    def test_explicit_read_abandonment_is_atomic_local_control_and_idempotent(self):
        packet,_=self.fixture.review();result=self.ops.abandon_read(packet['operation_id'])
        self.assertEqual(self.action()['state'],'WAITING_USER')
        self.assertEqual(result['source'],'local_control');self.assertFalse(result['review_verified'])
        self.assertTrue(result['no_broker_response']);self.assertNotIn('response_hash',result)
        self.assertEqual(self.book.db.execute('SELECT state FROM native_operation_records WHERE id=?',(packet['operation_id'],)).fetchone()[0],'ABANDONED_READ')
        count=self.book.audit.verify()['events_verified']
        self.assertEqual(self.ops.abandon_read(packet['operation_id']),result)
        self.assertEqual(self.book.audit.verify()['events_verified'],count)

    def test_cold_open_recovers_only_exact_running_read(self):
        packet,_=self.fixture.review();path=self.book.db.execute('PRAGMA database_list').fetchone()[2]
        self.book.close();cold=ExecutionBook(path,self.f.policy,simulation=True,clock=lambda:self.f.now);self.addCleanup(cold.close)
        result=NativeOperations(cold).abandon_read(packet['operation_id'],reason='EXPLICIT_READ_RECOVERY')
        self.assertEqual(result['state'],'ABANDONED_READ')
        self.assertEqual(next(a for a in cold.actions() if a['id']==self.fixture.action)['state'],'WAITING_USER')

    def test_unknown_action_and_legacy_action_intent_are_never_cleared(self):
        packet,_=self.fixture.review()
        self.book.db.execute("UPDATE live_actions SET state='UNKNOWN' WHERE id=?",(self.fixture.action,));self.book.db.commit()
        with self.assertRaisesRegex(ValueError,'ACTION_NOT_RUNNING'):self.ops.abandon_read(packet['operation_id'])
        self.assertEqual(self.action()['state'],'UNKNOWN')
        self.book.db.execute("UPDATE live_actions SET state='RUNNING' WHERE id=?",(self.fixture.action,));self.book.db.commit()
        event=json.loads(self.book.db.execute('SELECT payload FROM audit_events WHERE event_id=?',(packet['operation_id']+'-intent',)).fetchone()[0])
        event.update(event_id=self.fixture.action+'-intent',intent_id=self.fixture.action)
        self.book.audit.append(event)
        with self.assertRaisesRegex(ValueError,'LEGACY_ACTION_INTENT'):self.ops.abandon_read(packet['operation_id'])
        self.assertEqual(self.action()['state'],'RUNNING')

    def test_new_abandoned_review_invalidates_older_verified_review_for_place(self):
        old,response=self.fixture.review();self.ops.record(old['operation_id'],response)
        newer,_=self.fixture.review();self.ops.abandon_read(newer['operation_id'])
        with self.assertRaisesRegex(ValueError,'FRESH_NATIVE_REVIEW_REQUIRED'):
            self.ops.begin(self.fixture.action,'place',self.fixture.snapshot(),quote=self.f.quote)

    def test_late_read_receipt_keeps_new_running_operation_untouched(self):
        old,response=self.fixture.review();self.ops.abandon_read(old['operation_id'])
        newer,_=self.fixture.review();late=self.ops.record(old['operation_id'],response)
        self.assertTrue(late['late_receipt_retained']);self.assertFalse(late['review_verified'])
        self.assertEqual(self.action()['state'],'RUNNING')
        self.assertEqual(self.book.db.execute('SELECT state,result FROM native_operation_records WHERE id=?',(newer['operation_id'],)).fetchone(),('STARTED',None))
        count=self.book.audit.verify()['events_verified'];self.assertEqual(self.ops.record(old['operation_id'],response),late)
        self.assertEqual(self.book.audit.verify()['events_verified'],count)

    def test_place_cannot_be_abandoned_and_remains_unknown_after_expiry(self):
        packet=self.fixture.place_packet()
        with self.assertRaisesRegex(ValueError,'NATIVE_READ_OPERATION_REQUIRED'):self.ops.abandon_read(packet['operation_id'])
        self.ops.mark_uncertain(packet['operation_id'],'HOST_HANDOFF_EXPIRED')
        with self.assertRaisesRegex(ValueError,'NATIVE_READ_OPERATION_REQUIRED'):self.ops.abandon_read(packet['operation_id'])
        self.assertEqual(self.action()['state'],'UNKNOWN')

    def test_unresolved_shared_write_blocks_local_read_abandonment(self):
        packet,_=self.fixture.review()
        self.book.db.execute('INSERT INTO external_operations VALUES(?,?,?,?,NULL,NULL)',('unresolved-write',self.fixture.action,'place',self.f.now.isoformat()));self.book.db.commit()
        with self.assertRaisesRegex(ValueError,'UNRESOLVED_WRITE_BLOCKS'):self.ops.abandon_read(packet['operation_id'])
        self.assertEqual(self.action()['state'],'RUNNING')


class HostReadAbandonmentTests(TestCase):
    def setUp(self):
        ReadAbandonmentTests.setUp(self);self.directory=Path(self.f.tmp.name)/'host';self.bridge=HostExecutionBridge(self.book,self.directory)
        self.addCleanup(self.bridge.close)
        deny=patch('martin_agentic.native_review.EquityReviewTransport.__init__',side_effect=AssertionError('no broker I/O'))
        deny.start();self.addCleanup(deny.stop)

    def action(self):return next(a for a in self.book.actions() if a['id']==self.fixture.action)

    def prepare(self):
        request=submit_request(self.directory,self.fixture.action,'review',clock=lambda:self.f.now)
        result=self.bridge(snapshot=self.fixture.snapshot(),quotes={'DEMO':self.f.quote})
        self.assertEqual(result['status'],'HOST_PACKET_PREPARED')
        prepared=read_private_json(self.directory/'packets'/(request['request_id']+'.json'))
        return request,prepared

    def test_expired_read_releases_handoff_lease_without_fake_broker_receipt(self):
        request,prepared=self.prepare();self.f.now+=timedelta(seconds=16)
        result=self.bridge(allow_dispatch=False)
        self.assertEqual(result['status'],'HOST_REVIEW_ABANDONED')
        self.assertIsNone(self.bridge.pending);self.assertEqual(self.action()['state'],'WAITING_USER')
        lease=worker_lease(self.bridge.binding['db']);self.assertIsNotNone(lease);lease.close()
        self.assertFalse((self.directory/'receipts'/(prepared['packet']['operation_id']+'.json')).exists())

    def test_close_settles_only_pending_read(self):
        _,prepared=self.prepare();result=self.bridge.close()
        self.assertIsNone(result['pending_operation_id']);self.assertIsNone(self.bridge.pending)
        self.assertEqual(self.action()['state'],'WAITING_USER')
        row=self.book.db.execute('SELECT state FROM native_operation_records WHERE id=?',(prepared['packet']['operation_id'],)).fetchone()
        self.assertEqual(row[0],'ABANDONED_READ')

    def test_late_original_host_read_receipt_does_not_release_new_pending_review(self):
        old,prepared=self.prepare();self.f.now+=timedelta(seconds=16);self.bridge(allow_dispatch=False)
        self.f.quote.update(**{k:self.f.now.isoformat() for k in ('bid_at','ask_at','observed_at')})
        newer,new_packet=self.prepare()
        response={'data':{**prepared['packet']['parameters'],'order_checks':{},'customer_approval_required':False,
            'market_data_disclosure':'SYNTHETIC late review','quote_data':{}}}
        submit_receipt(self.directory,old['request_id'],response,clock=lambda:self.f.now)
        result=self.bridge(allow_dispatch=False)
        self.assertIn(prepared['packet']['operation_id'],result['retired_receipts_imported'])
        self.assertEqual(self.bridge.pending['packet']['operation_id'],new_packet['packet']['operation_id'])
        self.assertEqual(self.action()['state'],'RUNNING')
