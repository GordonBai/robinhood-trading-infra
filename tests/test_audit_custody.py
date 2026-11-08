import base64
from copy import deepcopy
from datetime import datetime,timedelta,timezone
import hashlib
import io
import json
from pathlib import Path
import tempfile
from unittest import TestCase,skipUnless
from unittest.mock import MagicMock,patch
from contextlib import redirect_stdout,redirect_stderr

from martin_agentic.audit import AuditTrail,canonical
from martin_agentic.audit_anchor import export_anchor
from martin_agentic.audit_custody import (prepare_custody,ObjectLockCustody,CustodyFailure,
    persist_custody_receipt,run_custody)
try:
    import boto3
    from botocore.stub import Stubber
    from botocore.response import StreamingBody
except ImportError:boto3=None


class CustodyPreparationTests(TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.db=self.root/'audit.sqlite'
        self.now=datetime(2026,10,21,20,tzinfo=timezone.utc)
        self.audit=AuditTrail(self.db);self.addCleanup(self.audit.close)
        self.audit.append({'event_id':'synthetic-one','occurred_at':self.now.isoformat(),
            'event_type':'ACCOUNT_OBSERVED','actor':'simulation','synthetic':True,'details':{'fixture':True}})
        self.path=self.root/'anchor.json';export_anchor(self.db,self.path,clock=lambda:self.now)
        self.anchor=json.loads(self.path.read_text())
        self.config={'schema_version':1,'provider':'aws_s3_object_lock','bucket':'synthetic-custody-bucket',
            'prefix':'synthetic/prefix','bucket_owner':'111111111111','writer_account':'222222222222',
            'retain_until':(self.now+timedelta(days=30)).isoformat(),'region':'us-east-1'}

    def prepare(self):return prepare_custody(self.db,self.anchor,self.config,now=self.now)

    def test_offline_preparation_validates_current_prefix_and_leaks_no_db_path_or_payload(self):
        plan=self.prepare()
        self.assertEqual(plan['external_calls'],0);self.assertFalse(plan['external_immutable_storage_verified'])
        self.assertNotIn(str(self.db),canonical(plan));self.assertNotIn('synthetic-one',canonical(plan))
        self.audit.append({'event_id':'synthetic-two','occurred_at':self.now.isoformat(),
            'event_type':'ACCOUNT_OBSERVED','actor':'simulation','synthetic':True,'details':{}})
        self.assertEqual(self.prepare()['key'],plan['key'])

    def test_invalid_config_or_extra_payload_cannot_be_prepared(self):
        for field,value in [('bucket_owner','222222222222'),('provider','s3-compatible'),
                ('retain_until',self.now.isoformat()),('bucket','https://unexpected'),('region','untrusted/url')]:
            with self.subTest(field=field):
                original=dict(self.config);self.config[field]=value
                with self.assertRaises(ValueError):self.prepare()
                self.config=original
        self.anchor['raw_logs']='must not upload arbitrary data'
        with self.assertRaisesRegex(ValueError,'MINIMAL_ANCHOR'):self.prepare()

    def test_local_receipt_is_private_and_fsynced(self):
        path=Path(persist_custody_receipt({'synthetic':True},self.root/'receipts'))
        self.assertEqual(path.stat().st_mode&0o777,0o600)
        self.assertEqual(json.loads(path.read_text()),{'synthetic':True})

    def test_cli_prepare_is_offline_and_does_not_modify_source_database(self):
        from martin_agentic.cli import main
        config=self.root/'config.json';config.write_text(json.dumps(self.config))
        before=hashlib.sha256(self.db.read_bytes()).hexdigest();output=io.StringIO()
        with (patch('martin_agentic.audit_custody.run_custody',side_effect=AssertionError('no external operation')),
                patch('martin_agentic.cli.ExecutionBook',side_effect=AssertionError('no trading ledger construction')),
                redirect_stdout(output),redirect_stderr(io.StringIO())):
            code=main(['audit-custody','prepare','--db',str(self.db),'--anchor',str(self.path),'--config',str(config)])
        self.assertEqual(code,0);self.assertEqual(json.loads(output.getvalue())['external_calls'],0)
        self.assertEqual(hashlib.sha256(self.db.read_bytes()).hexdigest(),before)


@skipUnless(boto3 is not None,'Install the audit-storage extra for SDK protocol coverage')
class CustodySdkTests(TestCase):
    def test_custodian_exact_version_verification_is_readonly_and_not_iam_independence_proof(self):
        self.preflight(writer=self.config['bucket_owner']);stream=self.readback()
        result=run_custody('verify-custodian',self.plan,self.fixture.root/'custodian-receipts',
            version_id='pinned-v1',factory=lambda *a,**kw:self.transport)
        self.assertEqual(result['status'],'CUSTODIAN_PINNED_VERSION_VERIFIED')
        self.assertEqual(result['verification_identity_scope'],'custodian')
        self.assertFalse(result['independent_custody_verified'])
        self.assertFalse(result['custodian_readback_verified'])  # synthetic SDK fixture
        self.assertTrue(stream._raw_stream.closed)
        self.assertTrue(Path(result['receipt_file']).is_file())
        self.ss.assert_no_pending_responses();self.ts.assert_no_pending_responses()

    def test_writer_cannot_masquerade_as_custodian_before_any_s3_read(self):
        self.ts.add_response('get_caller_identity',{'Account':self.config['writer_account'],
            'UserId':'synthetic','Arn':'arn:aws:iam::'+self.config['writer_account']+':user/synthetic'},{})
        with self.assertRaisesRegex(ValueError,'CUSTODIAN_IDENTITY_MISMATCH'):
            self.transport.verify_custodian(self.plan,'pinned-v1')
        self.ss.assert_no_pending_responses();self.ts.assert_no_pending_responses()

    def test_custodian_cannot_use_writer_publish_path(self):
        self.ts.add_response('get_caller_identity',{'Account':self.config['bucket_owner'],
            'UserId':'synthetic','Arn':'arn:aws:iam::'+self.config['bucket_owner']+':user/synthetic'},{})
        with self.assertRaises(CustodyFailure) as raised:self.transport.publish(self.plan)
        self.assertFalse(raised.exception.details['write_attempted'])
        self.ss.assert_no_pending_responses();self.ts.assert_no_pending_responses()

    def test_custodian_still_rejects_tampered_pinned_content(self):
        self.preflight(writer=self.config['bucket_owner']);stream=self.readback(body=b'not the committed prefix')
        with self.assertRaisesRegex(ValueError,'REMOTE_CONTENT_MISMATCH'):
            self.transport.verify_custodian(self.plan,'pinned-v1')
        self.assertTrue(stream._raw_stream.closed)
        self.ss.assert_no_pending_responses();self.ts.assert_no_pending_responses()

    def setUp(self):
        self.fixture=CustodyPreparationTests();self.fixture.setUp();self.addCleanup(self.fixture.doCleanups)
        self.plan=self.fixture.prepare();self.config=self.plan['config'];self.now=self.fixture.now
        # Explicit dummy credentials plus Stubber: no network or credential lookup.
        self.s3=boto3.client('s3',region_name='us-east-1',aws_access_key_id='testing',aws_secret_access_key='testing')
        self.sts=boto3.client('sts',region_name='us-east-1',aws_access_key_id='testing',aws_secret_access_key='testing')
        self.addCleanup(self.s3.close);self.addCleanup(self.sts.close)
        self.ss=Stubber(self.s3);self.ts=Stubber(self.sts);self.ss.activate();self.ts.activate()
        self.addCleanup(self.ss.deactivate);self.addCleanup(self.ts.deactivate)
        self.transport=ObjectLockCustody(self.s3,self.sts,synthetic=True,clock=lambda:self.now)
        self.owner={'Bucket':self.plan['bucket'],'ExpectedBucketOwner':self.config['bucket_owner']}
        self.args={**self.owner,'Key':self.plan['key'],'VersionId':'pinned-v1'}

    def preflight(self,*,writer='222222222222',lock='Enabled',versioning='Enabled'):
        self.ts.add_response('get_caller_identity',{'Account':writer,'UserId':'synthetic',
            'Arn':'arn:aws:iam::'+writer+':user/synthetic'},{})
        self.ss.add_response('get_object_lock_configuration',{'ObjectLockConfiguration':{'ObjectLockEnabled':lock}},self.owner)
        self.ss.add_response('get_bucket_versioning',{'Status':versioning},self.owner)

    def upload(self,*,error=None):
        body=(canonical(self.plan['anchor'])+'\n').encode()
        args={**self.owner,'Key':self.plan['key'],'Body':body,'ContentType':'application/json','IfNoneMatch':'*',
            'ChecksumAlgorithm':'SHA256',
            'ChecksumSHA256':base64.b64encode(hashlib.sha256(body).digest()).decode(),
            'ObjectLockMode':'COMPLIANCE','ObjectLockRetainUntilDate':datetime.fromisoformat(self.config['retain_until'])}
        if error:self.ss.add_client_error('put_object',service_error_code=error,http_status_code=412,expected_params=args)
        else:self.ss.add_response('put_object',{'VersionId':'pinned-v1'},args)

    def readback(self,*,mode='COMPLIANCE',until=None,body=None,version='pinned-v1'):
        self.ss.add_response('get_object_retention',{'Retention':{'Mode':mode,
            'RetainUntilDate':until or datetime.fromisoformat(self.config['retain_until'])}},self.args)
        if body is None:body=(canonical(self.plan['anchor'])+'\n').encode()
        stream=StreamingBody(io.BytesIO(body),len(body))
        self.ss.add_response('get_object',{'VersionId':version,'Body':stream},self.args)
        return stream

    def test_sdk_publish_sends_conditional_compliance_put_and_verifies_exact_version(self):
        self.preflight();self.upload();self.preflight();stream=self.readback()
        result=self.transport.publish(self.plan)
        self.assertEqual(result['version_id'],'pinned-v1');self.assertTrue(result['provider_lock_metadata_verified'])
        self.assertTrue(stream._raw_stream.closed)
        self.assertFalse(result['external_immutable_storage_verified']);self.assertFalse(result['independent_custody_verified'])
        self.assertTrue(result['synthetic']);self.assertFalse(result['production_ready'])
        self.ss.assert_no_pending_responses();self.ts.assert_no_pending_responses()

    def test_ambiguous_or_existing_upload_never_retries_put(self):
        for error in ('RequestTimeout','PreconditionFailed'):
            with self.subTest(error=error):
                self.preflight();self.upload(error=error)
                with self.assertRaises(CustodyFailure) as raised:self.transport.publish(self.plan)
                self.assertTrue(raised.exception.details['write_attempted'])
                self.assertIsNone(raised.exception.details['version_id'])
                self.assertFalse(raised.exception.details['automatic_retry_allowed'])
                self.ss.assert_no_pending_responses();self.ts.assert_no_pending_responses()

    def test_explicit_unknown_upload_recovery_only_heads_then_reads_pinned_version(self):
        self.preflight();self.ss.add_response('head_object',{'VersionId':'pinned-v1'},
            {**self.owner,'Key':self.plan['key']})
        self.preflight();self.readback()
        self.assertEqual(self.transport.discover(self.plan)['version_id'],'pinned-v1')
        self.ss.assert_no_pending_responses();self.ts.assert_no_pending_responses()

    def test_discovery_readback_failure_retains_discovered_version_without_retrying_head_or_put(self):
        self.preflight();self.ss.add_response('head_object',{'VersionId':'pinned-v1'},
            {**self.owner,'Key':self.plan['key']})
        self.preflight()
        self.ss.add_client_error('get_object_retention',service_error_code='AccessDenied',expected_params=self.args)
        with self.assertRaises(CustodyFailure) as raised:
            run_custody('discover',self.plan,self.fixture.root/'receipts',factory=lambda *a,**kw:self.transport)
        self.assertEqual(raised.exception.details['version_id'],'pinned-v1')
        self.assertEqual(raised.exception.details['phase'],'discover_verify_pinned_version')
        self.assertFalse(raised.exception.details['write_attempted'])
        self.assertFalse(raised.exception.details['automatic_retry_allowed'])
        self.ss.assert_no_pending_responses();self.ts.assert_no_pending_responses()

    def test_governance_or_short_retention_is_not_accepted_as_immutable_custody(self):
        for mode,until in [('GOVERNANCE',self.now+timedelta(days=30)),('COMPLIANCE',self.now+timedelta(days=1))]:
            self.preflight()
            self.ss.add_response('get_object_retention',{'Retention':{'Mode':mode,'RetainUntilDate':until}},self.args)
            with self.assertRaisesRegex(ValueError,'RETENTION_NOT_VERIFIED'):self.transport.verify(self.plan,'pinned-v1')
            self.ss.assert_no_pending_responses();self.ts.assert_no_pending_responses()

    def test_wrong_version_or_bytes_fail_and_always_close_body(self):
        for body,version in [(b'changed','pinned-v1'),(None,'other-version')]:
            self.preflight();stream=self.readback(body=body,version=version)
            with self.assertRaisesRegex(ValueError,'CONTENT_MISMATCH'):self.transport.verify(self.plan,'pinned-v1')
            self.assertTrue(stream._raw_stream.closed)

    def test_readback_failure_after_put_preserves_known_version_for_explicit_recovery(self):
        self.preflight();self.upload();self.preflight()
        self.ss.add_client_error('get_object_retention',service_error_code='AccessDenied',expected_params=self.args)
        with self.assertRaises(CustodyFailure) as raised:self.transport.publish(self.plan)
        self.assertEqual(raised.exception.details['version_id'],'pinned-v1')
        self.assertEqual(raised.exception.details['phase'],'verify_pinned_version')
        self.assertFalse(raised.exception.details['external_immutable_storage_verified'])

    def test_unknown_writer_and_suspended_versioning_stop_before_put(self):
        self.ts.add_response('get_caller_identity',{'Account':'333333333333'},{} )
        with self.assertRaises(CustodyFailure) as raised:self.transport.publish(self.plan)
        self.assertFalse(raised.exception.details['write_attempted'])
        self.preflight(versioning='Suspended')
        with self.assertRaises(CustodyFailure) as raised:self.transport.publish(self.plan)
        self.assertFalse(raised.exception.details['write_attempted'])
        self.ss.assert_no_pending_responses();self.ts.assert_no_pending_responses()

    def test_remote_success_local_receipt_failure_keeps_remote_version_not_success(self):
        factory=MagicMock(return_value=self.transport)
        self.preflight();self.upload();self.preflight();self.readback()
        with patch('martin_agentic.audit_custody.os.fsync',side_effect=OSError('synthetic failure')):
            with self.assertRaises(CustodyFailure) as raised:
                run_custody('publish',self.plan,self.fixture.root/'receipts',factory=factory)
        self.assertEqual(raised.exception.details['version_id'],'pinned-v1')
        self.assertEqual(raised.exception.details['phase'],'local_receipt_persistence')
        self.assertTrue(raised.exception.details['write_attempted'])

    def test_successful_read_only_verify_persists_exact_version_receipt(self):
        self.preflight();self.readback()
        result=run_custody('verify',self.plan,self.fixture.root/'receipts',version_id='pinned-v1',
            factory=lambda *a,**kw:self.transport)
        receipt=json.loads(Path(result['receipt_file']).read_text())
        self.assertEqual(receipt['version_id'],'pinned-v1')
        self.assertEqual(receipt['sha256'],self.plan['sha256'])
        self.assertFalse(receipt['external_immutable_storage_verified'])
        self.assertFalse(receipt['independent_custody_verified'])
        self.ss.assert_no_pending_responses();self.ts.assert_no_pending_responses()
