"""Private Host handoff files, never order confirmation or a broker transport.

This client never writes the execution database. One bounded mode=ro lookup is
used only to overlay current action state on a historical promotion result;
original response account scope is retained for the owner to validate and
credentials are forbidden. Local hashes detect mismatch, not malicious
modification by the local filesystem owner.
"""
from datetime import datetime,timezone
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import uuid

from .audit import canonical,digest
from .ledger import timestamp


MAX_BYTES=2_000_000
FORBIDDEN={'access_token','refresh_token','authorization','headers','cookie','cookies',
    'password','passwords','api_key','secret_key','client_secret'}


def _identity(value,*,native=False):
    raw=value[7:] if native and isinstance(value,str) and value.startswith('native-') else value
    if native and raw==value:raise ValueError('HOST_NATIVE_OPERATION_ID_INVALID')
    try:valid=isinstance(raw,str) and str(uuid.UUID(raw))==raw
    except (ValueError,TypeError,AttributeError):valid=False
    if not valid:raise ValueError('HOST_REQUEST_ID_INVALID')
    return value


def _directory(path,*,create=False):
    path=Path(path)
    if create:
        path.mkdir(mode=0o700,parents=True,exist_ok=True)
    fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    if create:os.fchmod(fd,0o700)
    return fd


def read_private_json(path):
    """Bounded regular-file read without following final path/parent symlinks."""
    path=Path(path);parent=_directory(path.parent)
    try:fd=os.open(path.name,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=parent)
    finally:os.close(parent)
    with os.fdopen(fd,'rb') as handle:
        info=os.fstat(handle.fileno())
        if not stat.S_ISREG(info.st_mode):raise ValueError('HOST_REGULAR_FILE_REQUIRED')
        if info.st_size>MAX_BYTES:raise ValueError('HOST_FILE_TOO_LARGE')
        raw=handle.read(MAX_BYTES+1)
    if len(raw)>MAX_BYTES:raise ValueError('HOST_FILE_TOO_LARGE')
    value=json.loads(raw)
    if not isinstance(value,dict):raise ValueError('HOST_JSON_OBJECT_REQUIRED')
    return value


def write_private_json(path,value):
    """Exclusive atomic publish; exact canonical duplicates are idempotent.

    A hard link publishes the fsynced temporary inode without overwriting an
    existing target. Failure never removes an existing request or receipt.
    """
    if not isinstance(value,dict):raise ValueError('HOST_JSON_OBJECT_REQUIRED')
    raw=(canonical(value)+'\n').encode()
    if len(raw)>MAX_BYTES:raise ValueError('HOST_FILE_TOO_LARGE')
    path=Path(path);parent=_directory(path.parent,create=True)
    temporary='.'+path.name+'.'+uuid.uuid4().hex+'.tmp';created=False
    try:
        fd=os.open(temporary,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600,dir_fd=parent)
        created=True
        with os.fdopen(fd,'wb') as handle:
            os.fchmod(handle.fileno(),0o600);handle.write(raw);handle.flush();os.fsync(handle.fileno())
        try:os.link(temporary,path.name,src_dir_fd=parent,dst_dir_fd=parent,follow_symlinks=False)
        except FileExistsError:
            if canonical(read_private_json(path))!=canonical(value):raise ValueError('HOST_FILE_CONFLICT') from None
        os.fsync(parent)
        return str(path)
    finally:
        if created:
            try:os.unlink(temporary,dir_fd=parent)
            except FileNotFoundError:pass
        os.close(parent)


def _binding(value):
    expected={'db','account','account_key','policy_sha256','source_fingerprint','epoch'}
    account = value.get('account') if isinstance(value, dict) else None
    suffix = account[-4:] if isinstance(account,str) else ''
    if (not isinstance(value,dict) or set(value)!=expected
            or not isinstance(account,str) or not account.startswith('••••')
            or not (suffix.isdigit() and len(suffix) == 4)
            or not isinstance(value['db'],str) or not Path(value['db']).is_absolute()):
        raise ValueError('HOST_BINDING_INVALID')
    for key in ('account_key','policy_sha256','source_fingerprint'):
        if not isinstance(value[key],str) or not re.fullmatch('[0-9a-f]{64}',value[key]):
            raise ValueError('HOST_BINDING_INVALID')
    _identity(value['epoch'])
    return value


def _now(clock):return (clock or (lambda:datetime.now(timezone.utc)))().isoformat()


def submit_request(directory,action_id,operation,request_id=None,clock=None):
    if operation not in {'review','place','cancel'}:raise ValueError('HOST_OPERATION_INVALID')
    if not isinstance(action_id,str) or not 0<len(action_id)<=256:raise ValueError('HOST_ACTION_ID_INVALID')
    return _submit_local_request(directory,action_id,operation,request_id,clock)


def submit_gate_release(directory,reason,request_id=None,clock=None):
    """Queue an operator's local gate disposition; never clears it client-side.

    The owner must perform its existing current-snapshot/risk/intent checks.
    This request is neither broker write authority nor order confirmation.
    """
    if not isinstance(reason,str) or not reason.strip() or len(reason)>1000:
        raise ValueError('HOST_GATE_RELEASE_REASON_REQUIRED')
    return _submit_local_request(directory,'operator-release-inhibit','release-inhibit',
        request_id,clock,reason=reason)


def submit_engineering_exit(directory,trade_id,request_id=None,clock=None):
    """Queue a local exit intent for the owner; never cancel or sell here."""
    if not isinstance(trade_id,str) or not trade_id.startswith('engineering-') or len(trade_id)>256:
        raise ValueError('HOST_ENGINEERING_TRADE_ID_REQUIRED')
    return _submit_local_request(directory,'operator-engineering-exit','engineering-exit',
        request_id,clock,trade_id=trade_id)


def submit_engineering_proposal(directory,signal_id,plan,evidence,request_id=None,clock=None):
    """Queue exact engineering terms; owner supplies all broker facts/risk checks."""
    if (not isinstance(signal_id,str) or not signal_id.startswith('engineering-') or len(signal_id)>256
            or not isinstance(plan,dict) or not isinstance(evidence,dict)):
        raise ValueError('HOST_ENGINEERING_PROPOSAL_REQUIRED')
    proposal={'signal_id':signal_id,'plan':plan,'evidence':evidence}
    _no_credentials(proposal)
    if len(canonical(proposal).encode())>64000:raise ValueError('HOST_ENGINEERING_PROPOSAL_TOO_LARGE')
    return _submit_local_request(directory,'operator-engineering-propose','engineering-propose',
        request_id,clock,proposal=proposal)



def submit_engineering_dry_assess(directory,action_id,request_id=None,clock=None):
    """Queue a default-off owner dry diagnostic for one engineering action."""
    if not isinstance(action_id,str) or not 0<len(action_id)<=256:
        raise ValueError('HOST_ACTION_ID_INVALID')
    return _submit_local_request(directory,action_id,
                                 'engineering-dry-assess',request_id,clock)


def submit_engineering_intent(directory,signal_id,plan,evidence,request_id=None,clock=None):
    """Queue an exact engineering review intent; never entry/place authority."""
    if (not isinstance(signal_id,str) or not signal_id.startswith('engineering-') or len(signal_id)>256
            or not isinstance(plan,dict) or not isinstance(evidence,dict)):
        raise ValueError('HOST_ENGINEERING_INTENT_REQUIRED')
    proposal={'signal_id':signal_id,'plan':plan,'evidence':evidence}
    _no_credentials(proposal)
    if len(canonical(proposal).encode())>64000:raise ValueError('HOST_ENGINEERING_INTENT_TOO_LARGE')
    return _submit_local_request(directory,'operator-engineering-intent','engineering-intent',
        request_id,clock,proposal=proposal)


def submit_engineering_promote(directory,action_id,permit_id,request_id=None,clock=None):
    """Queue an owner promotion for one exact action/permit identity."""
    if not isinstance(action_id,str) or not action_id or len(action_id)>256:
        raise ValueError('HOST_ENGINEERING_ACTION_ID_REQUIRED')
    if not isinstance(permit_id,str) or not re.fullmatch('[0-9a-f]{64}',permit_id):
        raise ValueError('HOST_ENGINEERING_PERMIT_ID_REQUIRED')
    return _submit_local_request(directory,action_id,'engineering-promote',
        request_id,clock,permit_id=permit_id)

def _submit_local_request(directory,action_id,operation,request_id,clock,*,reason=None,trade_id=None,proposal=None,permit_id=None):
    root=Path(directory);manifest=read_private_json(root/'manifest.json')
    if manifest.get('schema_version')!=1 or manifest.get('running') is not True:
        raise ValueError('HOST_CONTROLLER_NOT_RUNNING')
    binding=_binding(manifest['binding']);timestamp(manifest['published_at'])
    request_id=_identity(request_id or str(uuid.uuid4()))
    path=root/'requests'/(request_id+'.json')
    expected={'schema_version':1,'request_id':request_id,'binding':binding,'action_id':action_id,
        'operation':operation}
    if reason is not None:expected['reason']=reason
    if trade_id is not None:expected['trade_id']=trade_id
    if proposal is not None:expected['proposal']=proposal
    if permit_id is not None:expected['permit_id']=permit_id
    try:previous=read_private_json(path)
    except FileNotFoundError:previous=None
    if previous is not None:
        if {k:v for k,v in previous.items() if k!='requested_at'}!=expected:
            raise ValueError('HOST_REQUEST_CONFLICT')
        return previous
    value={**expected,'requested_at':_now(clock)}
    timestamp(value['requested_at'])
    try:write_private_json(path,value)
    except ValueError as exc:
        if str(exc)!='HOST_FILE_CONFLICT':raise
        winner=read_private_json(path)
        if {k:v for k,v in winner.items() if k!='requested_at'}!={k:v for k,v in value.items() if k!='requested_at'}:
            raise ValueError('HOST_REQUEST_CONFLICT') from None
        return winner
    return value


def _current_action_state(root, action_id, binding=None):
    """Bounded mode=ro single-row current action state.

    Returns ``{'state': state_or_None, 'reason': None_or_code}``.  A missing
    action row is distinct from a known ``UNKNOWN`` action state and from an
    unavailable/misbound ledger.  Manifest actions are never used as current
    state; they may only provide a verified binding/cache.
    """
    if not isinstance(action_id,str) or not action_id:
        return {'state':None,'reason':'HOST_CURRENT_STATE_ACTION_ID_INVALID'}
    if binding is None:
        try:
            manifest=read_private_json(root/'manifest.json')
            binding=manifest.get('binding')
        except (OSError,ValueError,TypeError):
            return {'state':None,'reason':'HOST_CURRENT_STATE_MANIFEST_UNAVAILABLE'}
    try:
        binding=_binding(binding)
        db=binding['db']
        conn=sqlite3.connect('file:'+str(Path(db))+'?mode=ro',uri=True,timeout=1)
    except (OSError,sqlite3.Error,KeyError,TypeError,ValueError):
        return {'state':None,'reason':'HOST_CURRENT_STATE_UNAVAILABLE'}
    try:
        try:
            raw=conn.execute('SELECT payload FROM live_state WHERE id=1').fetchone()
            if not raw:return {'state':None,
                'reason':'HOST_CURRENT_STATE_BINDING_UNAVAILABLE'}
            state_payload=json.loads(raw[0])
            if (not isinstance(state_payload,dict)
                    or state_payload.get('account_key')!=binding['account_key']
                    or state_payload.get('policy_hash')!=binding['policy_sha256']):
                return {'state':None,'reason':'HOST_CURRENT_STATE_BINDING_INVALID'}
            row=conn.execute('SELECT state FROM live_actions WHERE id=?',
                             (action_id,)).fetchone()
        except (sqlite3.Error,TypeError,ValueError):
            return {'state':None,'reason':'HOST_CURRENT_STATE_UNAVAILABLE'}
        if row is None:
            return {'state':None,'reason':'HOST_CURRENT_STATE_ACTION_MISSING'}
        state=row[0]
        if state not in {'REVIEW_PENDING','WAITING_USER','READY','RUNNING',
                         'UNKNOWN','REPORTED','OBSERVED','EXPIRED',
                         'SUPERSEDED','REJECTED','CANCELLED'}:
            return {'state':None,'reason':'HOST_CURRENT_STATE_INVALID'}
        return {'state':state,'reason':None}
    finally:
        conn.close()

def read_request_result(directory,request_id,clock=None):
    """Read advisory results; expose a pending call packet only while current.

    This is a fresh-pickup guard, never human confirmation or atomic permission
    to invoke a tool later. Original late receipts use their original packet
    directly in submit_receipt and are not discarded by these freshness gates.
    """
    request_id=_identity(request_id);root=Path(directory)
    def reconcile(status):
        return {'schema_version':1,'request_id':request_id,'status':status,
            'attention_required':True,'automatic_replay_allowed':False,
            'execution_authorized':False,'broker_effect_verified':False}
    for folder in ('results','packets'):
        try:
            result=read_private_json(root/folder/(request_id+'.json'))
            if result.get('schema_version')!=1 or result.get('request_id')!=request_id:
                raise ValueError('HOST_RESULT_BINDING_INVALID')
            if result.get('historical_local_promotion') is True:
                lookup=_current_action_state(root,result.get('action_id'),
                                             result.get('binding'))
                state=lookup['state'];reason=lookup['reason']
                if state is None:
                    status=('HOST_ENGINEERING_PROMOTION_ACTION_MISSING'
                            if reason=='HOST_CURRENT_STATE_ACTION_MISSING'
                            else 'HOST_ENGINEERING_PROMOTION_CURRENT_STATE_UNAVAILABLE')
                    reviewable=claimable=False
                    current_state='UNKNOWN'
                else:
                    status=('HOST_ENGINEERING_PROMOTED' if state=='READY'
                            else 'HOST_ENGINEERING_PROMOTION_ALREADY_COMPLETED')
                    reviewable=claimable=state=='READY'
                    current_state=state
                current=dict(result)
                current.update({'state':current_state,
                    'current_state':current_state,'state_reason':reason,
                    'status':status,'reviewable':reviewable,
                    'claimable':claimable,'placeable':False,
                    'entry_authorized':False,'execution_authorized':False,
                    'broker_effect_verified':False,
                    'attention_required':current_state!='READY',
                    'automatic_replay_allowed':False})
                result=current
            if result.get('historical_local_intent') is True:
                lookup=_current_action_state(root,result.get('action_id'),
                                             result.get('binding'))
                state=lookup['state'];reason=lookup['reason']
                if state is None:
                    status=('HOST_ENGINEERING_INTENT_ACTION_MISSING'
                            if reason=='HOST_CURRENT_STATE_ACTION_MISSING'
                            else 'HOST_ENGINEERING_INTENT_CURRENT_STATE_UNAVAILABLE')
                    reviewable=False
                    current_state='UNKNOWN'
                elif state=='REVIEW_PENDING':
                    status='HOST_ENGINEERING_INTENT_RECORDED'
                    reviewable=True
                    current_state=state
                elif state=='WAITING_USER':
                    status='HOST_ENGINEERING_INTENT_REVIEWED'
                    reviewable=False
                    current_state=state
                else:
                    status='HOST_ENGINEERING_INTENT_ALREADY_PROGRESSED'
                    reviewable=False
                    current_state=state
                current=dict(result)
                current.update({'state':current_state,
                    'current_state':current_state,'state_reason':reason,
                    'status':status,'reviewable':reviewable,
                    'claimable':False,'placeable':False,
                    'entry_authorized':False,'execution_authorized':False,
                    'broker_effect_verified':False,
                    'attention_required':current_state not in {
                        'REVIEW_PENDING','WAITING_USER'},
                    'automatic_replay_allowed':False})
                result=current
            if result.get('status')=='PREPARED':
                now=timestamp(_now(clock))
                try:
                    if not timestamp(result['prepared_at'])<=now<timestamp(result['expires_at']):
                        return reconcile('HOST_PACKET_EXPIRED_RECONCILE')
                except (ValueError,KeyError,TypeError):return reconcile('HOST_PACKET_EXPIRED_RECONCILE')
                try:
                    manifest=read_private_json(root/'manifest.json')
                    age=(now-timestamp(manifest['published_at'])).total_seconds()
                    if (manifest.get('schema_version')!=1 or manifest.get('running') is not True
                            or _binding(manifest['binding'])!=_binding(result['binding']) or not 0<=age<=30):
                        return reconcile('HOST_CONTROLLER_NOT_CURRENT_RECONCILE')
                    packet=result['packet']
                    if (not isinstance(packet,dict) or result.get('packet_sha256')!=digest(packet)
                            or result.get('execution_authorized') is not False
                            or result.get('broker_effect_verified') is not False
                            or packet.get('account')!=result['binding']['account']
                            or packet.get('account_key')!=result['binding']['account_key']
                            or packet.get('operation') not in {'review','place','cancel'}
                            or packet.get('tool')!={'review':'review_equity_order','place':'place_equity_order',
                                'cancel':'cancel_equity_order'}.get(packet.get('operation'))):
                        return reconcile('HOST_PACKET_NOT_CURRENT_RECONCILE')
                    _identity(packet['operation_id'],native=True)
                    actions=manifest.get('actions')
                    matching=[a for a in actions if isinstance(a,dict)
                        and a.get('id',a.get('action_id'))==packet.get('action_id')] if isinstance(actions,list) else []
                    if (manifest.get('pending_operation_id')!=packet.get('operation_id')
                            or len(matching)!=1 or matching[0].get('state')!='RUNNING'):
                        return reconcile('HOST_PACKET_NOT_CURRENT_RECONCILE')
                    if packet['operation']=='place' and matching[0].get('kind')=='entry':
                        from .live_verification_control import engineering_verification_gate
                        gate=engineering_verification_gate(result['binding']['db'],simulation=False,
                            purpose=matching[0].get('purpose'),
                            evidence={'parameters_authorized_at':packet.get('engineering_parameters_authorized_at')},
                            clock=lambda:now)
                        if gate['blocked']:return reconcile(gate['reason'])
                except (ValueError,KeyError,TypeError,OSError):
                    return reconcile('HOST_CONTROLLER_NOT_CURRENT_RECONCILE')
            return result
        except FileNotFoundError:pass
    return {'schema_version':1,'request_id':request_id,'status':'PENDING_OWNER',
        'execution_authorized':False,'broker_effect_verified':False}


def _no_credentials(value,depth=0):
    if depth>40:raise ValueError('HOST_RESPONSE_NESTING_LIMIT')
    if isinstance(value,dict):
        for key,child in value.items():
            if not isinstance(key,str) or key.lower() in FORBIDDEN:raise ValueError('HOST_CREDENTIAL_FIELDS_FORBIDDEN')
            _no_credentials(child,depth+1)
    elif isinstance(value,list):
        for child in value:_no_credentials(child,depth+1)
    elif isinstance(value,str) and value.lstrip().startswith(('{','[')):
        try:nested=json.loads(value)
        except (ValueError,TypeError):return
        _no_credentials(nested,depth+1)


def submit_receipt(directory,request_id,response,clock=None):
    root=Path(directory);request_id=_identity(request_id)
    prepared=read_private_json(root/'packets'/(request_id+'.json'))
    if (prepared.get('schema_version')!=1 or prepared.get('status')!='PREPARED'
            or prepared.get('request_id')!=request_id or prepared.get('execution_authorized') is not False
            or prepared.get('broker_effect_verified') is not False):raise ValueError('HOST_PREPARED_PACKET_REQUIRED')
    binding=_binding(prepared['binding']);packet=prepared['packet']
    if (not isinstance(packet,dict) or prepared.get('packet_sha256')!=digest(packet)
            or packet.get('operation') not in {'review','place','cancel'}
            or packet.get('tool')!={'review':'review_equity_order','place':'place_equity_order','cancel':'cancel_equity_order'}.get(packet.get('operation'))
            or packet.get('account')!=binding['account'] or packet.get('account_key')!=binding['account_key']):
        raise ValueError('HOST_PREPARED_PACKET_INVALID')
    _identity(packet['operation_id'],native=True)
    timestamp(prepared['prepared_at']);timestamp(prepared['expires_at'])
    if not isinstance(response,dict):raise ValueError('HOST_RESPONSE_OBJECT_REQUIRED')
    _no_credentials(response)
    path=root/'receipts'/(packet['operation_id']+'.json')
    try:previous=read_private_json(path)
    except FileNotFoundError:previous=None
    if previous is not None:
        if (previous.get('schema_version')!=1 or previous.get('request_id')!=request_id
                or previous.get('binding')!=binding or previous.get('packet_sha256')!=digest(packet)
                or previous.get('response')!=response or previous.get('response_sha256')!=digest(response)
                or previous.get('execution_authorized') is not False or previous.get('broker_effect_verified') is not False):
            raise ValueError('HOST_RECEIPT_CONFLICT')
        return previous
    try:
        trade_id=read_private_json(root/'requests'/(request_id+'.json')).get('trade_id')
    except (OSError,ValueError):
        trade_id=None
    receipt={'schema_version':1,'request_id':request_id,
        'action_id':packet['action_id'],'trade_id':trade_id,
        'operation_id':packet['operation_id'],
        'binding':binding,'packet_sha256':digest(packet),
        'response':response,'response_sha256':digest(response),'received_at':_now(clock),
        'execution_authorized':False,'broker_effect_verified':False}
    timestamp(receipt['received_at'])
    try:write_private_json(path,receipt)
    except ValueError as exc:
        if str(exc)!='HOST_FILE_CONFLICT':raise
        winner=read_private_json(path)
        if {k:v for k,v in winner.items() if k!='received_at'}!={k:v for k,v in receipt.items() if k!='received_at'}:
            raise ValueError('HOST_RECEIPT_CONFLICT') from None
        return winner
    return receipt
