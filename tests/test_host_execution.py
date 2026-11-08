from datetime import timedelta
import json
import uuid
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from martin_agentic.audit import digest
from martin_agentic.control_tasks import worker_lease
from martin_agentic.host_execution import HostExecutionBridge
from martin_agentic.host_mailbox import submit_request,submit_receipt,read_private_json
from martin_agentic.native_operations import NativeOperations
import test_native_operations


class HostExecutionTests(TestCase):
    def add_manifest_entries(self,count=25):
        original=next(a for a in self.book.actions() if a['id']==self.fixture.action)
        earlier=(self.f.now-timedelta(seconds=1)).isoformat()
        for index in range(count):
            payload=json.loads(original['payload']);identity='older-entry-'+str(index).zfill(3)
            payload.update(action_id=identity,kind='entry',ref_id=identity)
            self.book.db.execute('INSERT INTO live_actions VALUES(?,?,?,?,?,?,?,NULL)',
                (identity,original['trade_id'],'entry','READY',json.dumps(payload),earlier,earlier))
        self.book.db.commit()

    def test_manifest_prioritizes_management_over_more_than_twenty_older_entries(self):
        self.add_manifest_entries()
        self.bridge._publish()
        manifest=read_private_json(self.directory/'manifest.json')
        self.assertTrue(manifest['actions_truncated']);self.assertEqual(len(manifest['actions']),20)
        self.assertEqual(manifest['actions'][0]['id'],self.fixture.action)
        entries=[a['id'] for a in manifest['actions'] if a['kind']=='entry']
        self.assertEqual(entries,sorted(entries))

    def test_pending_packet_remains_pickable_when_later_management_fills_manifest(self):
        from martin_agentic.host_mailbox import read_request_result
        request,prepared=self.prepare('review')
        original=next(a for a in self.book.actions() if a['id']==self.fixture.action)
        earlier=(self.f.now-timedelta(seconds=1)).isoformat()
        for index in range(25):
            identity='older-protect-'+str(index).zfill(3);payload=json.loads(original['payload'])
            payload.update(action_id=identity,ref_id=identity)
            self.book.db.execute('INSERT INTO live_actions VALUES(?,?,?,?,?,?,?,NULL)',
                (identity,original['trade_id'],'protect','READY',json.dumps(payload),earlier,earlier))
        self.book.db.commit();self.bridge._publish()
        manifest=read_private_json(self.directory/'manifest.json')
        self.assertTrue(manifest['actions_truncated'])
        self.assertEqual(manifest['actions'][0]['id'],self.fixture.action)
        picked=read_request_result(self.directory,request['request_id'],clock=lambda:self.f.now)
        self.assertEqual(picked['status'],'PREPARED');self.assertEqual(picked['packet'],prepared['packet'])

    def test_completed_request_history_does_not_consume_pending_capacity_or_get_deleted(self):
        requests=self.directory/'requests';results=self.directory/'results'
        requests.mkdir(exist_ok=True);results.mkdir(exist_ok=True)
        for index in range(1001):
            name=str(uuid.UUID(int=index+1))+'.json'
            (requests/name).write_text('{}');(results/name).write_text('{}')
        request=submit_request(self.directory,self.fixture.action,'review',clock=lambda:self.f.now)
        result=self.tick()
        self.assertEqual(result['status'],'HOST_PACKET_PREPARED',result)
        self.assertEqual(result['request_id'],request['request_id'])
        self.assertEqual(len(list(requests.glob('*.json'))),1002)
        self.assertEqual(len(list(results.glob('*.json'))),1001)

    def test_more_than_thousand_unprocessed_requests_still_fail_closed(self):
        requests=self.directory/'requests';requests.mkdir(exist_ok=True)
        for index in range(1001):
            name=str(uuid.UUID(int=index+1))+'.json';(requests/name).write_text('{}')
        result=self.tick()
        self.assertEqual(result['status'],'HOST_MAILBOX_LIMIT_REQUIRES_ARCHIVE')
        self.assertTrue(result['attention_required'])
        self.assertIsNone(self.bridge.pending)

    def setUp(self):
        self.fixture=test_native_operations.NativeOperationTests();self.fixture.setUp();self.addCleanup(self.fixture.doCleanups)
        self.f=self.fixture.f;self.book=self.fixture.book;self.directory=Path(self.f.tmp.name)/'host'
        self.bridge=HostExecutionBridge(self.book,self.directory)
        self.addCleanup(self.bridge.close)

    def tick(self,**kwargs):
        return self.bridge(snapshot=self.fixture.snapshot(),quotes={'DEMO':self.f.quote},**kwargs)

    def prepare(self,operation,action_id=None):
        request=submit_request(self.directory,action_id or self.fixture.action,operation,clock=lambda:self.f.now)
        out=self.tick()
        self.assertEqual(out['status'],'HOST_PACKET_PREPARED',out)
        prepared=read_private_json(self.directory/'packets'/(request['request_id']+'.json'))
        return request,prepared

    def respond(self,request,response):
        submit_receipt(self.directory,request['request_id'],response,clock=lambda:self.f.now)
        out=self.bridge(allow_dispatch=False)
        self.assertEqual(out['status'],'HOST_RECEIPT_IMPORTED',out)
        self.assertFalse(out['broker_effect_verified']);return out

    def review(self):
        request,prepared=self.prepare('review');packet=prepared['packet']
        result=self.respond(request,{'data':{**packet['parameters'],'order_checks':{},
            'customer_approval_required':False,'market_data_disclosure':'Exact current disclosure','quote_data':{}}})
        self.assertTrue(result['native_result']['requires_native_confirmation'])
        return result

    def place(self):
        self.review();return self.prepare('place')

    def test_single_owner_handoff_import_and_broker_fact_are_separate(self):
        request,prepared=self.place();packet=prepared['packet']
        self.assertFalse(packet['execution_authorized_by_packet']);self.assertTrue(packet['requires_native_confirmation'])
        response=self.fixture.official_order(packet)
        response['structuredContent']['data']['order']['account_number']='SIM-0000'
        receipt=submit_receipt(self.directory,request['request_id'],response,clock=lambda:self.f.now)
        self.assertEqual(receipt['response']['structuredContent']['data']['order']['account_number'],'SIM-0000')
        out=self.bridge(allow_dispatch=False)
        self.assertEqual(out['status'],'HOST_RECEIPT_IMPORTED')
        self.assertNotEqual(next(iter(self.book.state()['trades'].values()))['phase'],'PROTECTED')
        stop=response['structuredContent']['data']['order']
        self.book.reconcile(self.f.snap([self.fixture.buy,stop],qty='2',held='2'))
        self.assertEqual(next(iter(self.book.state()['trades'].values()))['phase'],'PROTECTED')
        self.assertEqual(next(a for a in self.book.actions() if a['id']==self.fixture.action)['state'],'OBSERVED')
        self.assertTrue(self.book.audit.verify()['integrity_valid'])

    def test_expired_handoff_unknown_accepts_original_late_receipt_without_replay(self):
        request,prepared=self.place();packet=prepared['packet']
        self.f.now+=timedelta(seconds=16)
        out=self.bridge(allow_dispatch=False)
        self.assertEqual(out['status'],'HOST_HANDOFF_EXPIRED_RECONCILE')
        self.assertEqual(next(a for a in self.book.actions() if a['id']==self.fixture.action)['state'],'UNKNOWN')
        self.assertIsNone(self.book.claim())
        self.assertEqual(self.book.db.execute('SELECT state,result FROM native_operation_records WHERE id=?',(packet['operation_id'],)).fetchone(),('STARTED',None))
        result=self.respond(request,self.fixture.official_order(packet))
        self.assertEqual(result['native_result']['report']['status'],'submitted')
        self.assertEqual(self.book.db.execute('SELECT order_id FROM external_broker_order_ids').fetchone()[0],'official-order')
        self.assertFalse(result['native_result']['broker_effect_verified'])

    def test_late_result_never_regresses_already_observed_protection(self):
        request,prepared=self.place();response=self.fixture.official_order(prepared['packet'])
        self.book.reconcile(self.f.snap([self.fixture.buy,response['structuredContent']['data']['order']],qty='2',held='2'))
        self.f.now+=timedelta(seconds=16);self.bridge(allow_dispatch=False)
        self.respond(request,response)
        self.assertEqual(next(a for a in self.book.actions() if a['id']==self.fixture.action)['state'],'OBSERVED')

    def test_wrong_account_tool_response_retains_unknown_instead_of_registering_order(self):
        request,prepared=self.place();response=self.fixture.official_order(prepared['packet'])
        response['structuredContent']['data']['order']['account_number']='WRONG-0000'
        result=self.respond(request,response)
        self.assertFalse(result['native_result']['response_usable'])
        self.assertEqual(result['native_result']['report']['status'],'unknown')
        self.assertEqual(self.book.db.execute('SELECT count(*) FROM external_broker_order_ids').fetchone()[0],0)

    def test_broker_pending_does_not_prepare_new_host_request(self):
        request=submit_request(self.directory,self.fixture.action,'review',clock=lambda:self.f.now)
        self.bridge(allow_dispatch=False)
        self.assertFalse((self.directory/'packets'/(request['request_id']+'.json')).exists())
        self.assertEqual(next(a for a in self.book.actions() if a['id']==self.fixture.action)['state'],'READY')

    def test_restarted_owner_does_not_replay_old_request_or_receipt(self):
        request,prepared=self.place();self.bridge.close()
        other=HostExecutionBridge(self.book,self.directory);self.addCleanup(other.close)
        result=other(allow_dispatch=False)
        self.assertEqual(result['status'],'NATIVE_RECONCILIATION_REQUIRED')
        submit_receipt(self.directory,request['request_id'],self.fixture.official_order(prepared['packet']),clock=lambda:self.f.now)
        self.assertEqual(other(allow_dispatch=False)['status'],'NATIVE_RECONCILIATION_REQUIRED')
        self.assertEqual(self.book.db.execute('SELECT state FROM native_operation_records WHERE id=?',(prepared['packet']['operation_id'],)).fetchone()[0],'STARTED')
        self.bridge.close()
        self.assertTrue(read_private_json(self.directory/'manifest.json')['running'])

    def test_held_worker_lease_rejects_before_reservation(self):
        held=worker_lease(self.f.path)
        try:
            request=submit_request(self.directory,self.fixture.action,'review',clock=lambda:self.f.now)
            out=self.tick()
            self.assertEqual(out['status'],'HOST_REQUEST_REJECTED')
            self.assertEqual(out['reason'],'WORKER_OWNER_STILL_ACTIVE')
            self.assertIsNone(self.bridge.pending)
            self.assertFalse((self.directory/'packets'/(request['request_id']+'.json')).exists())
        finally:held.close()

    def test_packet_publication_failure_preserves_uncertain_intent(self):
        self.review();request=submit_request(self.directory,self.fixture.action,'place',clock=lambda:self.f.now)
        with patch('martin_agentic.host_execution.write_private_json',side_effect=OSError('synthetic disk failure')):
            out=self.tick()
        self.assertEqual(out['status'],'HOST_REQUEST_REJECTED')
        self.assertIsNotNone(self.bridge.pending)
        self.assertEqual(next(a for a in self.book.actions() if a['id']==self.fixture.action)['state'],'UNKNOWN')
        self.assertFalse((self.directory/'packets'/(request['request_id']+'.json')).exists())

    def test_receipt_import_sql_failure_can_retry_local_import_without_tool_call(self):
        request,prepared=self.place();response=self.fixture.official_order(prepared['packet'])
        submit_receipt(self.directory,request['request_id'],response,clock=lambda:self.f.now)
        self.book.db.execute("""CREATE TRIGGER fail_host_import BEFORE UPDATE ON native_operation_records
            WHEN NEW.state='RECORDED' BEGIN SELECT RAISE(ABORT,'synthetic SQL failure'); END""");self.book.db.commit()
        out=self.bridge(allow_dispatch=False);self.assertEqual(out['status'],'HOST_RECEIPT_IMPORT_REQUIRES_RECOVERY')
        self.assertIsNotNone(self.bridge.pending)
        self.book.db.execute('DROP TRIGGER fail_host_import');self.book.db.commit()
        self.assertEqual(self.bridge(allow_dispatch=False)['status'],'HOST_RECEIPT_IMPORTED')
        self.assertIsNone(self.bridge.pending)

    def test_stale_request_or_old_epoch_rejected_before_reservation(self):
        request=submit_request(self.directory,self.fixture.action,'review',clock=lambda:self.f.now-timedelta(seconds=31))
        self.assertEqual(self.tick()['reason'],'HOST_REQUEST_EXPIRED')
        second=submit_request(self.directory,self.fixture.action,'review',clock=lambda:self.f.now)
        self.bridge.binding['epoch']='ffffffff-ffff-ffff-ffff-ffffffffffff'
        self.assertEqual(self.tick()['reason'],'HOST_REQUEST_BINDING_INVALID')

    def test_arbitrary_unknown_without_exact_host_marker_stays_nonimportable(self):
        request,prepared=self.place();packet=prepared['packet']
        self.book.db.execute("UPDATE live_actions SET state='UNKNOWN' WHERE id=?",(packet['action_id'],));self.book.db.commit()
        with self.assertRaisesRegex(ValueError,'EXTERNAL_OPERATION_NOT_RUNNING'):
            NativeOperations(self.book).record(packet['operation_id'],self.fixture.official_order(packet))

    def test_cancel_timeout_late_acceptance_is_not_a_terminal_cancellation(self):
        request,prepared=self.place();response=self.fixture.official_order(prepared['packet']);self.respond(request,response)
        snapshot=self.f.snap([self.fixture.buy,response['structuredContent']['data']['order']],qty='2',held='2')
        self.book.reconcile(snapshot)
        self.book.reconcile(snapshot,quotes={'DEMO':{**self.f.quote,'bid':'38.99','ask':'39'}})
        cancel=next(a for a in self.book.actions() if a['kind']=='cancel')
        request=submit_request(self.directory,cancel['id'],'cancel',clock=lambda:self.f.now)
        out=self.bridge(snapshot=snapshot,quotes={'DEMO':self.f.quote})
        self.assertEqual(out['status'],'HOST_PACKET_PREPARED',out)
        self.f.now+=timedelta(seconds=16);self.bridge(allow_dispatch=False)
        result=self.respond(request,{'data':{'accepted':True}})
        self.assertEqual(result['native_result']['report']['status'],'cancel_requested')
        self.assertFalse(result['native_result']['cancel_terminal_verified'])

    def test_place_packet_expires_with_original_review_even_when_new_quote_is_fresh(self):
        self.review();self.f.now+=timedelta(seconds=29)
        self.f.quote.update({key:self.f.now.isoformat() for key in ('bid_at','ask_at','observed_at')})
        _,prepared=self.prepare('place')
        from martin_agentic.ledger import timestamp
        self.assertEqual((timestamp(prepared['expires_at'])-self.f.now).total_seconds(),1)

    def test_expired_recorded_exit_review_is_refreshable_without_stale_data_or_write_retry(self):
        # Use an actual core timeout-generated exit, rather than editing an
        # action into an exit or extending a protection deadline in the fixture.
        self.f.now+=timedelta(seconds=31)
        self.f.quote.update({key:self.f.now.isoformat() for key in ('bid_at','ask_at','observed_at')})
        self.book.reconcile(self.fixture.snapshot(),quotes={'DEMO':self.f.quote})
        self.fixture.action=next(a['id'] for a in self.book.actions() if a['kind']=='exit')
        self.review()
        action=next(a for a in self.book.actions() if a['id']==self.fixture.action)
        parameters=json.loads(action['payload'])['parameters']
        self.f.now+=timedelta(seconds=31)
        self.f.quote.update({key:self.f.now.isoformat() for key in ('bid_at','ask_at','observed_at')})
        request=submit_request(self.directory,self.fixture.action,'place',clock=lambda:self.f.now)
        result=self.tick()
        self.assertEqual(result['status'],'HOST_REQUEST_REJECTED')
        self.assertEqual(result['reason'],'FRESH_NATIVE_REVIEW_REQUIRED')
        self.assertIsNone(self.bridge.pending)
        self.assertFalse((self.directory/'packets'/(request['request_id']+'.json')).exists())
        self.assertEqual(self.book.db.execute("SELECT count(*) FROM external_operations WHERE operation='place'").fetchone()[0],0)
        self.review()
        _,prepared=self.prepare('place')
        self.assertEqual({k:v for k,v in prepared['packet']['parameters'].items() if k!='ref_id'},parameters)
        self.assertTrue(prepared['packet']['requires_native_confirmation'])
        self.assertFalse(prepared['packet']['execution_authorized_by_packet'])

    def test_fresh_review_does_not_hide_stale_quote_or_account(self):
        self.review()
        request=submit_request(self.directory,self.fixture.action,'place',clock=lambda:self.f.now)
        stale={**self.f.quote,'bid_at':(self.f.now-timedelta(seconds=16)).isoformat()}
        result=self.bridge(snapshot=self.fixture.snapshot(),quotes={'DEMO':stale})
        self.assertEqual(result['reason'],'HOST_FRESH_INPUT_REQUIRED')
        self.assertIsNone(self.bridge.pending)
        self.assertFalse((self.directory/'packets'/(request['request_id']+'.json')).exists())
        request=submit_request(self.directory,self.fixture.action,'place',clock=lambda:self.f.now)
        stale_snapshot=self.fixture.snapshot()
        stale_snapshot['observed_at']=(self.f.now-timedelta(seconds=31)).isoformat()
        result=self.bridge(snapshot=stale_snapshot,quotes={'DEMO':self.f.quote})
        self.assertEqual(result['reason'],'HOST_FRESH_INPUT_REQUIRED')
        self.assertIsNone(self.bridge.pending)
        self.assertFalse((self.directory/'packets'/(request['request_id']+'.json')).exists())

    def test_local_gate_release_runs_in_owner_and_never_prepares_broker_packet(self):
        from martin_agentic.host_mailbox import submit_gate_release
        request=submit_gate_release(self.directory,'Verified flat startup repair',clock=lambda:self.f.now)
        with patch('martin_agentic.watchdog.release_entry_inhibit',return_value={
                'released':True,'execution_enabled':False,'risk_locks_unchanged':True}) as release:
            out=self.tick()
        self.assertEqual(out['status'],'HOST_GATE_RELEASED');self.assertFalse(out['execution_authorized'])
        self.assertIsNone(self.bridge.pending);self.assertFalse((self.directory/'packets'/(request['request_id']+'.json')).exists())
        self.assertIs(release.call_args.args[0],self.book)
        self.assertEqual(release.call_args.args[1]['account_key'],self.fixture.snapshot()['account_key'])
        self.assertTrue(self.book.audit.verify()['integrity_valid'])

    def test_local_gate_release_cannot_skip_existing_real_flat_account_checks(self):
        from martin_agentic.host_mailbox import submit_gate_release
        submit_gate_release(self.directory,'Cannot release a synthetic positioned account',clock=lambda:self.f.now)
        out=self.tick()
        self.assertEqual(out['status'],'HOST_REQUEST_REJECTED')
        self.assertEqual(out['reason'],'WATCHDOG_RELEASE_REAL_BROKER_REQUIRED')
        self.assertIsNone(self.bridge.pending)

    def test_precise_broker_effect_releases_lease_for_next_management_before_late_receipt(self):
        request,prepared=self.place();response=self.fixture.official_order(prepared['packet'])
        snapshot=self.f.snap([self.fixture.buy,response['structuredContent']['data']['order']],qty='2',held='2')
        self.book.reconcile(snapshot)
        self.assertEqual(self.bridge(snapshot=snapshot,allow_dispatch=False)['observed_receipts_pending'],[prepared['packet']['operation_id']])
        self.assertIsNone(self.bridge.pending)
        available=worker_lease(self.f.path);self.assertIsNotNone(available);available.close()
        # A current broker state may now drive the normal next risk-management action.
        self.book.reconcile(snapshot,quotes={'DEMO':{**self.f.quote,'bid':'38.99','ask':'39'}})
        cancel=next(a for a in self.book.actions() if a['kind']=='cancel')
        next_request=submit_request(self.directory,cancel['id'],'cancel',clock=lambda:self.f.now)
        out=self.bridge(snapshot=snapshot,quotes={'DEMO':self.f.quote})
        self.assertEqual(out['status'],'HOST_PACKET_PREPARED',out)
        active_id=self.bridge.pending['packet']['operation_id']
        submit_receipt(self.directory,request['request_id'],response,clock=lambda:self.f.now)
        out=self.bridge(allow_dispatch=False)
        self.assertIn(prepared['packet']['operation_id'],out['retired_receipts_imported'])
        self.assertEqual(self.bridge.pending['packet']['operation_id'],active_id)
        self.assertEqual(self.bridge.pending['request_id'],next_request['request_id'])

    def test_protection_request_precedes_alphabetically_earlier_entry_request(self):
        entry=next(a for a in self.book.actions() if a['kind']=='entry')
        submit_request(self.directory,entry['id'],'review',request_id='00000000-0000-0000-0000-000000000001',clock=lambda:self.f.now)
        protect=submit_request(self.directory,self.fixture.action,'review',request_id='ffffffff-ffff-ffff-ffff-ffffffffffff',clock=lambda:self.f.now)
        out=self.tick()
        self.assertEqual(out['request_id'],protect['request_id'])

    def test_default_dispatcher_maps_protection_to_review_then_place(self):
        from martin_luk.live_session import HostDeliveryDispatcher
        class Host:
            def deliver(self, intent):
                return {'request_id': 'not-used'}
            def receipt(self, intent_id):
                return None
        class Bridge:
            book = self.book
            def __call__(self, **kwargs):
                return {}
        dispatcher = HostDeliveryDispatcher(self.book, Host(), Bridge())
        snapshot = self.fixture.snapshot()
        quotes = {'DEMO': self.f.quote}
        action, operation, reason = dispatcher._select(snapshot, quotes, False)
        self.assertEqual(action['kind'], 'protect')
        self.assertEqual(operation, 'review')
        self.assertIsNone(reason)
        packet, response = self.fixture.review()
        self.fixture.ops.record(packet['operation_id'], response)
        action, operation, reason = dispatcher._select(snapshot, quotes, False)
        self.assertEqual(action['kind'], 'protect')
        self.assertEqual(operation, 'place')
        self.assertIsNone(reason)

    def test_full_controller_supervised_host_uses_mailbox_and_zero_models(self):
        """Synthetic I/O/controller integration, never a formal confirmation test."""
        from contextlib import ExitStack
        from types import SimpleNamespace
        import time
        from unittest.mock import MagicMock
        from martin_luk.live_session import run_live_session
        from martin_agentic.host_mailbox import read_request_result
        output=Path(self.f.tmp.name)/'controller-worker';directory=output/'host-execution'
        journal=MagicMock();journal.summary.return_value={'read_healthy':True,'reconciliation_issues':[],
            'positions':{},'open_equity_orders':0,'pending_approvals':0,'blockers':[]}
        host={};calls=[]
        class ReviewFixture:
            simulation_only=True
            def __init__(self,**kwargs):self.rpc=SimpleNamespace(close=lambda:None)
            def __enter__(self):return self
            def __exit__(self,*args):pass
            def execute_review(_,packet):
                calls.append(packet['operation'])
                return {'data':{**packet['parameters'],'order_checks':{},'customer_approval_required':False,
                    'market_data_disclosure':'SYNTHETIC_CONTROLLER_DISCLOSURE'}}
        def broker(*args,**kwargs):
            return self.f.snap([self.fixture.buy,*([host['stop']] if 'stop' in host else [])],
                qty='2',held='2' if 'stop' in host else '0')
        def host_tick(*args,**kwargs):
            folder=directory/'requests'
            answered=host.setdefault('answered',set())
            for path in sorted(folder.glob('*.json')) if folder.exists() else []:
                result=read_request_result(directory,path.stem,clock=lambda:self.f.now)
                if result['status']!='PREPARED':continue
                packet=result['packet']
                key=(path.stem,packet['operation'])
                if key in answered:continue
                if packet['operation']=='review':
                    response={'data':{**packet['parameters'],'order_checks':{},
                        'customer_approval_required':False,
                        'market_data_disclosure':'SYNTHETIC_CONTROLLER_DISCLOSURE'}}
                    calls.append('review')
                else:
                    response=self.fixture.official_order(packet)
                    host['stop']=response['structuredContent']['data']['order']
                    calls.append('place_fixture')
                submit_receipt(directory,path.stem,response,clock=lambda:self.f.now)
                answered.add(key)
            time.sleep(.02)
        quote={'id':'host-controller-quote','kind':'quote','at':self.f.now.isoformat(),
            'data':self.f.quote,'synthetic_input':True,'source_sha256':'SYNTHETIC_CONTROLLER'}
        with ExitStack() as stack:
            prefix='martin_agentic.live_session.'
            stack.enter_context(patch(prefix+'ExecutionBook',return_value=self.book))
            stack.enter_context(patch(prefix+'BrokerJournal',return_value=journal))
            stack.enter_context(patch(prefix+'_broker_read',side_effect=broker))
            stack.enter_context(patch(prefix+'collect_live_events',return_value=([quote],{})))
            stack.enter_context(patch(prefix+'wait_for_result_or_tick',side_effect=host_tick))
            stack.enter_context(patch(prefix+'CodexOrderWorker',side_effect=AssertionError('no model permitted')))
            stack.enter_context(patch(prefix+'print'));stack.enter_context(patch(prefix+'notify_local'))
            stack.enter_context(patch.object(self.book,'close'))
            stack.enter_context(patch('martin_agentic.async_native_review.EquityReviewTransport',ReviewFixture))
            result=run_live_session(self.f.path,self.f.policy,symbols=[str(i) for i in range(10)],cycles=20,
                agent_mode='supervised-host',clock=lambda:self.f.now,status_file=Path(self.f.tmp.name)/'status.json',
                output_dir=output,budget_entry_limit=0,budget_total_limit=0)
        self.assertEqual(calls,['review','place_fixture'])
        self.assertEqual(self.book.db.execute('SELECT count(*) FROM worker_call_reservations').fetchone()[0],0)
        self.assertEqual(next(iter(self.book.state()['trades'].values()))['phase'],'PROTECTED')
        self.assertEqual(self.book.db.execute("SELECT count(*) FROM native_operation_records WHERE state='RECORDED'").fetchone()[0],2)
        self.assertEqual(result['agent_mode'],'supervised-host')
