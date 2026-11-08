from copy import deepcopy
from datetime import datetime,timedelta,timezone
import json
import os
from pathlib import Path
import tempfile
from unittest import TestCase
from unittest.mock import patch
import uuid

from martin_agentic.audit import digest
from martin_agentic.host_mailbox import (read_private_json,write_private_json,
    submit_request,submit_gate_release,submit_engineering_exit,submit_engineering_proposal,read_request_result,submit_receipt,MAX_BYTES)


class HostMailboxTests(TestCase):
    def test_engineering_proposal_queues_terms_without_broker_facts_or_sqlite(self):
        plan={'symbol':'DEMO','quantity':1,'entry_ceiling':'10','stop':'9','cost_buffer_per_share':'0.05'}
        evidence={'synthetic':True,'source_sha256':'fixture','parameters_user_specified':True}
        with patch('sqlite3.connect',side_effect=AssertionError('client cannot open DB')):
            request=submit_engineering_proposal(self.root,'engineering-fixture',plan,evidence,
                request_id=self.rid,clock=lambda:self.now)
        self.assertEqual(request['operation'],'engineering-propose')
        self.assertEqual(request['proposal'],{'signal_id':'engineering-fixture','plan':plan,'evidence':evidence})
        self.assertNotIn('snapshot',request);self.assertNotIn('quote',request)
        self.assertFalse(Path(self.binding['db']).exists())
        with self.assertRaisesRegex(ValueError,'CREDENTIAL_FIELDS_FORBIDDEN'):
            submit_engineering_proposal(self.root,'engineering-bad',plan,{'access_token':'forbidden'})

    def test_engineering_exit_client_only_queues_exact_bound_intent_without_sqlite(self):
        with patch('sqlite3.connect',side_effect=AssertionError('client must not open DB')):
            request=submit_engineering_exit(self.root,'engineering-fixture',request_id=self.rid,clock=lambda:self.now)
        self.assertEqual(request['operation'],'engineering-exit')
        self.assertEqual(request['trade_id'],'engineering-fixture')
        self.assertEqual(request['binding'],self.binding)
        self.assertNotIn('packet',request)
        self.assertFalse(Path(self.binding['db']).exists())
        self.assertEqual(request,submit_engineering_exit(self.root,'engineering-fixture',request_id=self.rid,clock=lambda:self.now))
        with self.assertRaisesRegex(ValueError,'HOST_REQUEST_CONFLICT'):
            submit_engineering_exit(self.root,'engineering-other',request_id=self.rid,clock=lambda:self.now)
        with self.assertRaisesRegex(ValueError,'HOST_ENGINEERING_TRADE_ID_REQUIRED'):
            submit_engineering_exit(self.root,'strategy-fixture')

    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)/'private';self.now=datetime(2026,10,26,14,tzinfo=timezone.utc)
        self.binding={'db':str(self.root/'never-opened.sqlite'),'account':'••••0000',
            'account_key':'a'*64,'policy_sha256':'b'*64,'source_fingerprint':'c'*64,'epoch':str(uuid.uuid4())}
        self.manifest={'schema_version':1,'running':True,'binding':self.binding,'published_at':self.now.isoformat()}
        write_private_json(self.root/'manifest.json',self.manifest)
        self.rid=str(uuid.uuid4());self.operation_id='native-'+str(uuid.uuid4())

    def request(self,**kwargs):
        return submit_request(self.root,'protect-action','place',request_id=self.rid,clock=lambda:self.now,**kwargs)

    def prepared(self):
        packet={'operation_id':self.operation_id,'action_id':'protect-action','operation':'place',
            'tool':'place_equity_order','account':'••••0000','account_key':'a'*64,
            'parameters':{'symbol':'DEMO','side':'sell','quantity':'1','type':'stop_market','stop_price':'39'},
            'specific_order_confirmed':False,'execution_authorized_by_packet':False}
        envelope={'schema_version':1,'status':'PREPARED','request_id':self.rid,'binding':self.binding,
            'packet':packet,'packet_sha256':digest(packet),'prepared_at':self.now.isoformat(),
            'expires_at':(self.now+timedelta(seconds=15)).isoformat(),
            'execution_authorized':False,'broker_effect_verified':False}
        write_private_json(self.root/'packets'/(self.rid+'.json'),envelope)
        manifest=deepcopy(self.manifest)
        manifest.update(pending_operation_id=self.operation_id,actions=[{'id':'protect-action','state':'RUNNING'}])
        (self.root/'manifest.json').write_text(json.dumps(manifest))
        return envelope

    def test_request_is_bounded_data_with_private_files_and_no_execution_db(self):
        with patch('sqlite3.connect',side_effect=AssertionError('must not open SQLite')):
            request=self.request()
        self.assertEqual(request['operation'],'place');self.assertEqual(request['binding'],self.binding)
        self.assertFalse((self.root/'never-opened.sqlite').exists())
        for path in (self.root,self.root/'requests'):
            self.assertEqual(path.stat().st_mode&0o777,0o700)
        self.assertEqual((self.root/'requests'/(self.rid+'.json')).stat().st_mode&0o777,0o600)

    def test_same_request_is_idempotent_without_refreshing_creation_time(self):
        first=self.request();self.now+=timedelta(seconds=30)
        self.assertEqual(self.request(),first)
        self.assertEqual(len(list((self.root/'requests').glob('*.json'))),1)
        with self.assertRaisesRegex(ValueError,'REQUEST_CONFLICT'):
            submit_request(self.root,'different-action','place',request_id=self.rid)

    def test_stopped_manifest_rejects_new_request(self):
        value=deepcopy(self.manifest);value['running']=False
        (self.root/'manifest.json').write_text(json.dumps(value))
        with self.assertRaisesRegex(ValueError,'CONTROLLER_NOT_RUNNING'):self.request()
        self.assertFalse((self.root/'requests').exists())

    def test_manifest_wrong_account_binding_and_uuid_rejected(self):
        for key,value in [('db','relative.sqlite'),('account_key','short'),('epoch','../escape')]:
            modified=deepcopy(self.manifest);modified['binding'][key]=value
            (self.root/'manifest.json').write_text(json.dumps(modified))
            with self.subTest(key=key),self.assertRaises(ValueError):self.request()

    def test_traversal_and_unsupported_operations_rejected(self):
        with self.assertRaises(ValueError):submit_request(self.root,'one','approve')
        for value in ('../escape','native-'+self.rid,self.rid.upper(),''):
            with self.subTest(value=value),self.assertRaises(ValueError):read_request_result(self.root,value)

    def test_result_prefers_terminal_result_and_checks_request_identity(self):
        self.assertEqual(read_request_result(self.root,self.rid,clock=lambda:self.now)['status'],'PENDING_OWNER')
        envelope=self.prepared()
        self.assertEqual(read_request_result(self.root,self.rid,clock=lambda:self.now),envelope)
        done={'schema_version':1,'request_id':self.rid,'status':'RECORDED','execution_authorized':False}
        write_private_json(self.root/'results'/(self.rid+'.json'),done)
        self.assertEqual(read_request_result(self.root,self.rid,clock=lambda:self.now),done)
        done['request_id']=str(uuid.uuid4());(self.root/'results'/(self.rid+'.json')).write_text(json.dumps(done))
        with self.assertRaisesRegex(ValueError,'RESULT_BINDING'):read_request_result(self.root,self.rid)

    def assert_no_callable_packet(self,result,status):
        self.assertEqual(result['status'],status)
        self.assertNotIn('packet',result);self.assertNotIn('parameters',result)
        self.assertFalse(result['execution_authorized']);self.assertFalse(result['automatic_replay_allowed'])

    def test_expired_or_future_prepared_packet_is_reconciliation_only(self):
        self.prepared()
        for offset in (-1,15,16,300):
            with self.subTest(offset=offset):
                result=read_request_result(self.root,self.rid,clock=lambda:self.now+timedelta(seconds=offset))
                self.assert_no_callable_packet(result,'HOST_PACKET_EXPIRED_RECONCILE')

    def test_stopped_old_epoch_binding_and_stale_manifest_do_not_expose_packet(self):
        original=self.prepared();path=self.root/'manifest.json'
        initial=json.loads(path.read_text())
        changes=[('running',False),('binding',{**self.binding,'epoch':str(uuid.uuid4())}),
            ('binding',{**self.binding,'policy_sha256':'d'*64}),
            ('published_at',(self.now-timedelta(seconds=31)).isoformat()),
            ('published_at',(self.now+timedelta(seconds=1)).isoformat())]
        for key,value in changes:
            with self.subTest(key=key,value=value):
                manifest=deepcopy(initial);manifest[key]=value;path.write_text(json.dumps(manifest))
                result=read_request_result(self.root,self.rid,clock=lambda:self.now)
                self.assert_no_callable_packet(result,'HOST_CONTROLLER_NOT_CURRENT_RECONCILE')
        # The stopped/stale/old-epoch guard must not destroy a real late result.
        response={'data':{'order':{'account_number':'ORIGINAL_SCOPE','id':'real-return'}}}
        receipt=submit_receipt(self.root,self.rid,response,clock=lambda:self.now+timedelta(seconds=300))
        self.assertEqual(receipt['binding'],original['binding']);self.assertEqual(receipt['response'],response)

    def test_nonpending_or_unknown_observed_or_missing_action_does_not_expose_packet(self):
        self.prepared();path=self.root/'manifest.json';initial=json.loads(path.read_text())
        changes=[('pending_operation_id','native-'+str(uuid.uuid4())),('pending_operation_id',None),
            ('actions',[{'id':'protect-action','state':'UNKNOWN'}]),
            ('actions',[{'id':'protect-action','state':'OBSERVED'}]),('actions',[])]
        for key,value in changes:
            manifest=deepcopy(initial);manifest[key]=value;path.write_text(json.dumps(manifest))
            with self.subTest(key=key,value=value):
                result=read_request_result(self.root,self.rid,clock=lambda:self.now)
                self.assert_no_callable_packet(result,'HOST_PACKET_NOT_CURRENT_RECONCILE')

    def test_terminal_advisory_result_survives_stale_manifest_without_call_packet(self):
        self.prepared()
        terminal={'schema_version':1,'request_id':self.rid,'status':'RECORDED',
            'execution_authorized':False,'broker_effect_verified':False}
        write_private_json(self.root/'results'/(self.rid+'.json'),terminal)
        (self.root/'manifest.json').unlink()
        self.assertEqual(read_request_result(self.root,self.rid,clock=lambda:self.now+timedelta(seconds=300)),terminal)

    def test_original_account_scope_preserved_and_receipt_idempotent(self):
        prepared=self.prepared();response={'data':{'order':{'account_number':'PRIVATE_ACCOUNT_SCOPE','id':'official-order'}}}
        result=submit_receipt(self.root,self.rid,response,clock=lambda:self.now)
        self.assertEqual(result['response'],response)
        self.assertEqual(result['response_sha256'],digest(response))
        self.assertEqual(result['packet_sha256'],prepared['packet_sha256'])
        self.assertFalse(result['execution_authorized']);self.assertFalse(result['broker_effect_verified'])
        self.now+=timedelta(seconds=100)
        self.assertEqual(submit_receipt(self.root,self.rid,response,clock=lambda:self.now),result)
        path=self.root/'receipts'/(self.operation_id+'.json')
        self.assertEqual(path.stat().st_mode&0o777,0o600)
        with self.assertRaisesRegex(ValueError,'RECEIPT_CONFLICT'):
            submit_receipt(self.root,self.rid,{'data':{'order':{'id':'different'}}})
        self.assertEqual(read_private_json(path),result)

    def test_late_receipt_preserves_old_epoch_after_manifest_replaced_or_stopped(self):
        prepared=self.prepared();next_manifest=deepcopy(self.manifest)
        next_manifest['binding']['epoch']=str(uuid.uuid4());next_manifest['running']=False
        (self.root/'manifest.json').write_text(json.dumps(next_manifest))
        self.now+=timedelta(seconds=300)
        receipt=submit_receipt(self.root,self.rid,{'isError':True},clock=lambda:self.now)
        self.assertEqual(receipt['binding'],prepared['binding'])
        self.assertNotEqual(receipt['binding']['epoch'],next_manifest['binding']['epoch'])

    def test_credential_fields_in_nested_json_tool_content_are_rejected(self):
        self.prepared()
        for response in ({'data':{'access_token':'secret'}},{'content':[{'type':'text','text':'{"data":{"authorization":"secret"}}'}]},
                {'data':{'headers':{'x-api-key':'secret'}}}):
            with self.subTest(response=response),self.assertRaisesRegex(ValueError,'CREDENTIAL'):
                submit_receipt(self.root,self.rid,response)
        self.assertFalse((self.root/'receipts').exists())

    def test_wrong_packet_hash_tool_and_operation_path_are_rejected(self):
        prepared=self.prepared();path=self.root/'packets'/(self.rid+'.json')
        for mutation in ('hash','tool','operation_id'):
            altered=deepcopy(prepared)
            if mutation=='hash':altered['packet_sha256']='wrong'
            elif mutation=='tool':altered['packet']['tool']='cancel_equity_order';altered['packet_sha256']=digest(altered['packet'])
            else:altered['packet']['operation_id']='../../outside';altered['packet_sha256']=digest(altered['packet'])
            path.write_text(json.dumps(altered))
            with self.subTest(mutation=mutation),self.assertRaises(ValueError):submit_receipt(self.root,self.rid,{'data':{}})

    def test_exclusive_publish_cannot_overwrite_or_follow_symlink(self):
        path=self.root/'stable.json';write_private_json(path,{'value':'original'})
        with self.assertRaisesRegex(ValueError,'FILE_CONFLICT'):write_private_json(path,{'value':'changed'})
        self.assertEqual(read_private_json(path),{'value':'original'})
        link=self.root/'link.json';link.symlink_to(path)
        with self.assertRaises(OSError):read_private_json(link)
        with self.assertRaises(OSError):write_private_json(link,{'value':'original'})
        self.assertEqual(read_private_json(path),{'value':'original'})

    def test_nonregular_oversize_and_symlink_directory_fail_bounded(self):
        fifo=self.root/'pipe';os.mkfifo(fifo)
        with self.assertRaisesRegex(ValueError,'REGULAR_FILE'):read_private_json(fifo)
        large=self.root/'large';large.write_bytes(b' '* (MAX_BYTES+1))
        with self.assertRaisesRegex(ValueError,'TOO_LARGE'):read_private_json(large)
        with self.assertRaisesRegex(ValueError,'TOO_LARGE'):write_private_json(self.root/'large-write',{'x':' '*MAX_BYTES})
        alias=self.root/'alias';alias.symlink_to(self.root,target_is_directory=True)
        with self.assertRaises(OSError):read_private_json(alias/'manifest.json')

    def test_file_and_directory_are_fsynced_before_publish_completes(self):
        real=os.fsync;seen=[]
        def sync(fd):seen.append(os.fstat(fd).st_mode);return real(fd)
        with patch('martin_agentic.host_mailbox.os.fsync',side_effect=sync):write_private_json(self.root/'durable.json',{'x':1})
        import stat
        self.assertTrue(any(stat.S_ISREG(x) for x in seen));self.assertTrue(any(stat.S_ISDIR(x) for x in seen))
        self.assertEqual(list(self.root.glob('*.tmp')),[])

    def test_failed_fsync_does_not_publish_request(self):
        with patch('martin_agentic.host_mailbox.os.fsync',side_effect=OSError('synthetic disk failure')):
            with self.assertRaises(OSError):self.request()
        self.assertFalse((self.root/'requests'/(self.rid+'.json')).exists())

    def test_concurrent_same_request_and_receipt_reuse_winner_timestamp(self):
        import threading
        from concurrent.futures import ThreadPoolExecutor
        def race(callback):
            barrier=threading.Barrier(2);lock=threading.Lock();counter=[0]
            def clock():
                with lock:counter[0]+=1;offset=counter[0]
                barrier.wait(timeout=2)
                return self.now+timedelta(milliseconds=offset)
            with ThreadPoolExecutor(max_workers=2) as pool:
                values=list(pool.map(lambda _:callback(clock),range(2)))
            self.assertEqual(values[0],values[1])
        race(lambda clock:submit_request(self.root,'protect-action','place',request_id=self.rid,clock=clock))
        self.prepared()
        race(lambda clock:submit_receipt(self.root,self.rid,{'data':{'order':{'id':'same-order'}}},clock=clock))

    def test_gate_release_queues_only_local_bound_reason_without_db_or_confirmation(self):
        with patch('sqlite3.connect',side_effect=AssertionError('no SQLite')):
            request=submit_gate_release(self.root,'Current complete flat snapshot reviewed',
                request_id=self.rid,clock=lambda:self.now)
        self.assertEqual(request,{'schema_version':1,'request_id':self.rid,'binding':self.binding,
            'action_id':'operator-release-inhibit','operation':'release-inhibit',
            'reason':'Current complete flat snapshot reviewed','requested_at':self.now.isoformat()})
        self.assertFalse(Path(self.binding['db']).exists())
        self.assertNotIn('packet',request);self.assertNotIn('specific_order_confirmed',request)
        self.assertNotIn('execution_authorized',request)
        with self.assertRaisesRegex(ValueError,'OPERATION_INVALID'):
            submit_request(self.root,'operator-release-inhibit','release-inhibit')

    def test_gate_release_requires_running_manifest_and_bounded_nonempty_reason(self):
        for reason in ('','   ',None,False,'x'*1001):
            with self.subTest(reason=reason),self.assertRaisesRegex(ValueError,'REASON_REQUIRED'):
                submit_gate_release(self.root,reason,request_id=self.rid)
        stopped={**self.manifest,'running':False}
        (self.root/'manifest.json').write_text(json.dumps(stopped))
        with self.assertRaisesRegex(ValueError,'CONTROLLER_NOT_RUNNING'):
            submit_gate_release(self.root,'valid reason',request_id=self.rid)
        self.assertFalse((self.root/'requests').exists())

    def test_gate_release_same_id_is_idempotent_and_changed_reason_conflicts(self):
        first=submit_gate_release(self.root,'flat verified',request_id=self.rid,clock=lambda:self.now)
        self.now+=timedelta(seconds=1)
        self.assertEqual(submit_gate_release(self.root,'flat verified',request_id=self.rid,clock=lambda:self.now),first)
        with self.assertRaisesRegex(ValueError,'REQUEST_CONFLICT'):
            submit_gate_release(self.root,'different reason',request_id=self.rid,clock=lambda:self.now)
        self.assertEqual(read_private_json(self.root/'requests'/(self.rid+'.json')),first)
