"""Synthetic real-core engineering admission through the sole book owner."""
from copy import deepcopy
from datetime import timedelta
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch
import uuid

from martin_agentic.audit import digest
from martin_agentic.host_execution import HostExecutionBridge
from martin_agentic.host_mailbox import write_private_json,read_private_json
import test_native_lifecycle


class HostEngineeringProposeTests(TestCase):
    def setUp(self):
        self.f=test_native_lifecycle.NativeLifecycleTests();self.f.setUp();self.addCleanup(self.f.tearDown)
        self.book=self.f.book;self.book.reconcile(self.f.snap())
        self.directory=Path(self.f.temp.name)/'host';self.bridge=HostExecutionBridge(self.book,self.directory)
        self.addCleanup(self.bridge.close)
        deny=patch('martin_agentic.native_review.EquityReviewTransport.__init__',side_effect=AssertionError('no broker I/O'))
        deny.start();self.addCleanup(deny.stop)

    def request(self):
        return {'schema_version':1,'binding':dict(self.bridge.binding),'request_id':str(uuid.uuid4()),
            'requested_at':self.f.now.isoformat(),'operation':'engineering-propose',
            'action_id':'operator-engineering-propose','proposal':{
                'signal_id':'engineering-host-proposal','plan':deepcopy(self.f.plan),
                'evidence':{'synthetic':True,'engineering_scope_authorized':True,'parameters_user_specified':True,
                    'source_sha256':digest(['synthetic','parameters']),
                    'parameters_valid_until':(self.f.now+timedelta(minutes=1)).isoformat()}}}

    def prepare(self,request,*,allow_entries=True,snapshot=None,quotes=None):
        return self.bridge._prepare(request,self.f.snap() if snapshot is None else snapshot,
            {'DEMO':self.f.quote()} if quotes is None else quotes,allow_entries)

    def test_owner_queues_engineering_entry_without_native_write_or_confirmation(self):
        request=self.request();write_private_json(self.directory/'requests'/(request['request_id']+'.json'),request)
        out=self.bridge(snapshot=self.f.snap(),quotes={'DEMO':self.f.quote()},allow_entries=True)
        self.assertEqual(out['status'],'HOST_ENGINEERING_ENTRY_PROPOSED')
        self.assertEqual(out['broker_writes_by_bridge'],0);self.assertFalse(out['execution_authorized'])
        self.assertEqual(len(self.book.actions()),1)
        self.assertEqual(self.book.actions()[0]['state'],'READY')
        self.assertEqual(self.book.state()['trades']['engineering-host-proposal']['purpose'],'engineering')
        self.assertEqual(read_private_json(self.directory/'results'/(request['request_id']+'.json'))['action_id'],out['action_id'])
        self.assertTrue(self.book.audit.verify()['integrity_valid'])

    def test_exact_request_is_idempotent_and_changed_payload_conflicts(self):
        request=self.request();out=self.prepare(request);count=self.book.audit.verify()['events_verified']
        self.assertEqual(self.prepare(request),out);self.assertEqual(len(self.book.actions()),1)
        self.assertEqual(self.book.audit.verify()['events_verified'],count)
        changed=deepcopy(request);changed['proposal']['plan']['quantity']=1
        with self.assertRaisesRegex(ValueError,'HOST_ENGINEERING_REQUEST_CONFLICT'):self.prepare(changed)

    def test_core_commit_before_host_audit_failure_recovers_same_entry(self):
        request=self.request()
        with patch.object(self.book.audit,'append',side_effect=OSError('injected host audit publish failure')):
            with self.assertRaises(OSError):self.prepare(request)
        self.assertEqual(len(self.book.actions()),1);identity=self.book.actions()[0]['id']
        recovered=self.prepare(request)
        self.assertEqual(recovered['action_id'],identity);self.assertTrue(recovered['result']['duplicate'])
        self.assertEqual(len(self.book.actions()),1)

    def test_result_publication_failure_reuses_durable_host_audit(self):
        request=self.request()
        with patch('martin_agentic.host_execution.write_private_json',side_effect=OSError('injected result publish failure')):
            with self.assertRaises(OSError):self.prepare(request)
        recovered=self.prepare(request);self.assertEqual(len(self.book.actions()),1)
        self.assertEqual(recovered['status'],'HOST_ENGINEERING_ENTRY_PROPOSED')

    def test_guardian_admission_and_missing_owner_quote_fail_before_core_intent(self):
        request=self.request()
        with self.assertRaisesRegex(ValueError,'HOST_NEW_ENTRIES_BLOCKED'):self.prepare(request,allow_entries=False)
        with self.assertRaisesRegex(ValueError,'HOST_ENGINEERING_FRESH_QUOTE_REQUIRED'):self.prepare(request,quotes={})
        self.assertFalse(self.book.actions())

    def test_wrong_binding_expired_request_wrong_purpose_and_provenance_rejected(self):
        request=self.request();wrong=deepcopy(request);wrong['binding']['epoch']=str(uuid.uuid4())
        with self.assertRaisesRegex(ValueError,'HOST_REQUEST_BINDING_INVALID'):self.prepare(wrong)
        old=deepcopy(request);old['requested_at']=(self.f.now-timedelta(seconds=31)).isoformat()
        with self.assertRaisesRegex(ValueError,'HOST_REQUEST_EXPIRED'):self.prepare(old)
        wrong=deepcopy(request);wrong['proposal']['evidence']['purpose']='strategy'
        with self.assertRaisesRegex(ValueError,'TRADE_PURPOSE_CONFLICT'):self.prepare(wrong)
        snapshot=self.f.snap();snapshot['synthetic']=False
        with self.assertRaisesRegex(ValueError,'HOST_SNAPSHOT_PROVENANCE_MISMATCH'):self.prepare(request,snapshot=snapshot)
        self.assertFalse(self.book.actions())

    def test_missing_expired_or_next_session_parameter_deadlines_rejected(self):
        for deadline in (None,self.f.now.isoformat(),(self.f.now+timedelta(days=1)).isoformat()):
            request=self.request()
            if deadline is None:del request['proposal']['evidence']['parameters_valid_until']
            else:request['proposal']['evidence']['parameters_valid_until']=deadline
            with self.assertRaisesRegex(ValueError,'HOST_ENGINEERING_PARAMETER_DEADLINE'):self.prepare(request)
        self.assertFalse(self.book.actions())

    def test_client_cannot_inject_snapshot_or_quote_and_core_risk_limits_still_apply(self):
        request=self.request();request['proposal']['quote']=self.f.quote()
        with self.assertRaisesRegex(ValueError,'HOST_ENGINEERING_PROPOSAL_INVALID'):self.prepare(request)
        request=self.request();request['proposal']['plan']['quantity']=3
        with self.assertRaisesRegex(ValueError,'ENTRY_GATES'):self.prepare(request)
        quote=self.f.quote();quote['ask_at']=(self.f.now-timedelta(seconds=16)).isoformat()
        with self.assertRaisesRegex(ValueError,'ENTRY_GATES'):self.prepare(self.request(),quotes={'DEMO':quote})
        self.assertFalse(self.book.actions())
