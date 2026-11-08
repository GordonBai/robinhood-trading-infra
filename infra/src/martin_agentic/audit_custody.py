"""Optional AWS S3 Object Lock custody for a minimal audit-prefix commitment.

No bucket creation, retention shortening, deletion, raw-log upload or broker I/O.
SDK response checks are distinct from independently reviewing custodial access.
"""
import base64
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import re
import uuid

from .audit import canonical
from .audit_anchor import verify_anchor
from .ledger import timestamp


def prepare_custody(db_path,anchor,config,*,now=None):
    now=now or datetime.now(timezone.utc)
    fields={'schema_version','generated_at','head_sequence','head_hash','events_committed',
        'scope','external_immutable_storage_verified','broker_receipts_cryptographically_authenticated'}
    if (not isinstance(anchor,dict) or set(anchor)!=fields
            or anchor['scope']!='local_audit_prefix_commitment'
            or anchor['external_immutable_storage_verified'] is not False
            or anchor['broker_receipts_cryptographically_authenticated'] is not False):
        raise ValueError('CUSTODY_MINIMAL_ANCHOR_REQUIRED')
    if timestamp(anchor['generated_at'])>now:raise ValueError('CUSTODY_FUTURE_ANCHOR')
    verify_anchor(db_path,anchor)
    if not isinstance(config,dict) or set(config)!={'schema_version','provider','bucket','prefix','bucket_owner','writer_account','retain_until','region'}:
        raise ValueError('CUSTODY_CONFIG_SCHEMA_INVALID')
    if config['schema_version']!=1 or config['provider']!='aws_s3_object_lock':
        raise ValueError('CUSTODY_PROVIDER_NOT_SUPPORTED')
    for key in ('bucket_owner','writer_account'):
        if not isinstance(config[key],str) or not re.fullmatch('[0-9]{12}',config[key]):
            raise ValueError('CUSTODY_ACCOUNT_ID_REQUIRED')
    if config['bucket_owner']==config['writer_account']:raise ValueError('CUSTODY_SEPARATE_ACCOUNT_REQUIRED')
    if not isinstance(config['bucket'],str) or not re.fullmatch('[a-z0-9][a-z0-9-]{1,61}[a-z0-9]',config['bucket']):
        raise ValueError('CUSTODY_BUCKET_NAME_INVALID')
    if not isinstance(config['prefix'],str) or not re.fullmatch('[A-Za-z0-9][A-Za-z0-9/_-]{0,300}',config['prefix']):
        raise ValueError('CUSTODY_PREFIX_INVALID')
    if not isinstance(config['region'],str) or not re.fullmatch('[a-z]{2}-[a-z]+-[0-9]+',config['region']):
        raise ValueError('CUSTODY_REGION_INVALID')
    if timestamp(config['retain_until'])<=now:raise ValueError('CUSTODY_RETENTION_EXPIRED')
    body=(canonical(anchor)+'\n').encode();sha=hashlib.sha256(body).hexdigest()
    key=config['prefix'].rstrip('/')+'/'+str(anchor['head_sequence'])+'-'+sha+'.json'
    return {'schema_version':1,'bucket':config['bucket'],'key':key,'sha256':sha,
        'anchor':anchor,'config':dict(config),'payload_bytes':len(body),'external_calls':0,
        'external_immutable_storage_verified':False,'independent_custody_verified':False,
        'production_ready':False}


class CustodyFailure(ValueError):
    def __init__(self,phase,plan,*,version_id=None,write_attempted=False):
        self.details={'status':'CUSTODY_REQUIRES_RECONCILIATION' if write_attempted else 'CUSTODY_FAILED',
            'phase':phase,'bucket':plan['bucket'],'key':plan['key'],'version_id':version_id,
            'write_attempted':write_attempted,'automatic_retry_allowed':False,
            'external_immutable_storage_verified':False,'production_ready':False}
        super().__init__(canonical(self.details))


class ObjectLockCustody:
    def __init__(self,s3,sts,*,synthetic=False,clock=None):
        if type(synthetic) is not bool:raise ValueError('CUSTODY_PROVENANCE_REQUIRED')
        self.s3=s3;self.sts=sts;self.synthetic=synthetic
        self.clock=clock or (lambda:datetime.now(timezone.utc))

    def _preflight(self,plan,*,identity_scope='writer'):
        config=plan['config'];owner={'Bucket':plan['bucket'],'ExpectedBucketOwner':config['bucket_owner']}
        if config['bucket_owner']==config['writer_account']:raise ValueError('CUSTODY_SEPARATE_ACCOUNT_REQUIRED')
        if timestamp(config['retain_until'])<=self.clock():raise ValueError('CUSTODY_RETENTION_EXPIRED')
        if identity_scope not in {'writer','custodian'}:raise ValueError('CUSTODY_IDENTITY_SCOPE_INVALID')
        expected=config['writer_account'] if identity_scope=='writer' else config['bucket_owner']
        if self.sts.get_caller_identity().get('Account')!=expected:
            raise ValueError('CUSTODY_WRITER_IDENTITY_MISMATCH' if identity_scope=='writer'
                else 'CUSTODY_CUSTODIAN_IDENTITY_MISMATCH')
        lock=self.s3.get_object_lock_configuration(**owner)
        if lock.get('ObjectLockConfiguration',{}).get('ObjectLockEnabled')!='Enabled':
            raise ValueError('CUSTODY_OBJECT_LOCK_NOT_ENABLED')
        if self.s3.get_bucket_versioning(**owner).get('Status')!='Enabled':
            raise ValueError('CUSTODY_VERSIONING_NOT_ENABLED')
        return owner

    def verify(self,plan,version_id,*,identity_scope='writer'):
        if not isinstance(version_id,str) or not version_id or version_id=='null' or len(version_id)>1024:
            raise ValueError('CUSTODY_EXACT_VERSION_REQUIRED')
        owner=self._preflight(plan,identity_scope=identity_scope)
        args={**owner,'Key':plan['key'],'VersionId':version_id}
        retention=self.s3.get_object_retention(**args).get('Retention',{})
        until=retention.get('RetainUntilDate')
        until=timestamp(until) if isinstance(until,str) else until
        if (retention.get('Mode')!='COMPLIANCE' or not isinstance(until,datetime)
                or until.tzinfo is None or until<timestamp(plan['config']['retain_until']) or until<=self.clock()):
            raise ValueError('CUSTODY_RETENTION_NOT_VERIFIED')
        result=self.s3.get_object(**args);stream=result['Body']
        try:body=stream.read(plan['payload_bytes']+1)
        finally:stream.close()
        if (result.get('VersionId')!=version_id or body!=(canonical(plan['anchor'])+'\n').encode()
                or hashlib.sha256(body).hexdigest()!=plan['sha256']):
            raise ValueError('CUSTODY_REMOTE_CONTENT_MISMATCH')
        return {'schema_version':1,'status':('CUSTODIAN_PINNED_VERSION_VERIFIED'
            if identity_scope=='custodian' else 'PINNED_VERSION_VERIFIED'),'bucket':plan['bucket'],
            'key':plan['key'],'version_id':version_id,'sha256':plan['sha256'],
            'verified_at':self.clock().isoformat(),'retain_until':until.isoformat(),
            'retention_mode':'COMPLIANCE','synthetic':self.synthetic,
            'provider_lock_metadata_verified':True,
            'external_immutable_storage_verified':not self.synthetic,
            'separate_account_boundary_observed':True,'independent_custody_verified':False,
            'verification_identity_scope':identity_scope,
            'custodian_readback_verified':identity_scope=='custodian' and not self.synthetic,
            'custodial_permissions_review_required':True,
            'scope':'audit_prefix_commitment_only_not_full_log_backup',
            'broker_receipts_cryptographically_authenticated':False,'production_ready':False}

    def verify_custodian(self,plan,version_id):
        """Read exact retained version as the bucket-owner account; never PUT.

        This observes a distinct account boundary, not an independent IAM review
        or evidence that credentials are controlled by different people.
        """
        return self.verify(plan,version_id,identity_scope='custodian')

    def publish(self,plan):
        phase='preflight';attempted=False;version=None
        try:
            owner=self._preflight(plan);phase='put_object';attempted=True
            body=(canonical(plan['anchor'])+'\n').encode()
            response=self.s3.put_object(**owner,Key=plan['key'],Body=body,
                ContentType='application/json',IfNoneMatch='*',
                ChecksumAlgorithm='SHA256',
                ChecksumSHA256=base64.b64encode(hashlib.sha256(body).digest()).decode(),
                ObjectLockMode='COMPLIANCE',ObjectLockRetainUntilDate=timestamp(plan['config']['retain_until']))
            version=response.get('VersionId');phase='verify_pinned_version'
            return self.verify(plan,version)
        except Exception:
            # Includes precondition failure, ambiguous upload and readback loss.
            # Never retry PUT or infer retention from a successful write alone.
            raise CustodyFailure(phase,plan,version_id=version,write_attempted=attempted) from None

    def discover(self,plan):
        """Explicit read-only recovery of an ambiguous PUT; immediately pin version."""
        owner=self._preflight(plan)
        head=self.s3.head_object(**owner,Key=plan['key'])
        version=head.get('VersionId')
        try:return self.verify(plan,version)
        except Exception:
            raise CustodyFailure('discover_verify_pinned_version',plan,version_id=version) from None


def aws_custody(config,*,profile=None):
    try:
        import boto3
        from botocore.config import Config
    except ImportError:raise ValueError('INSTALL_OPTIONAL_AUDIT_STORAGE_EXTRA') from None
    options=Config(connect_timeout=5,read_timeout=10,retries={'total_max_attempts':1},
        ignore_configured_endpoint_urls=True)
    session=boto3.Session(profile_name=profile,region_name=config['region'])
    return ObjectLockCustody(session.client('s3',config=options),session.client('sts',config=options))


def persist_custody_receipt(result,directory):
    directory=Path(directory);directory.mkdir(parents=True,exist_ok=True);directory.chmod(0o700)
    path=directory/('custody-'+uuid.uuid4().hex+'.json');temporary=path.with_suffix('.tmp')
    with temporary.open('x') as handle:
        os.chmod(temporary,0o600);handle.write(canonical(result)+'\n');handle.flush();os.fsync(handle.fileno())
    temporary.replace(path)
    descriptor=os.open(directory,os.O_RDONLY)
    try:os.fsync(descriptor)
    finally:os.close(descriptor)
    return str(path)


def run_custody(mode,plan,receipt_dir,*,version_id=None,profile=None,factory=aws_custody):
    if mode not in {'publish','verify','discover','verify-custodian'}:raise ValueError('CUSTODY_MODE_INVALID')
    result=None
    try:
        transport=factory(plan['config'],profile=profile)
        result=(transport.publish(plan) if mode=='publish' else transport.discover(plan) if mode=='discover'
            else transport.verify_custodian(plan,version_id) if mode=='verify-custodian'
            else transport.verify(plan,version_id))
        path=persist_custody_receipt(result,receipt_dir)
        return {**result,'receipt_file':path}
    except CustodyFailure:raise
    except Exception:
        raise CustodyFailure('local_receipt_persistence' if result else 'connect_or_readback',plan,
            version_id=result.get('version_id') if result else version_id,
            write_attempted=mode=='publish' and result is not None) from None
