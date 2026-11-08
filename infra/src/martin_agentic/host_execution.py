"""Single SQLite owner bridge to an online, formally confirming MCP Host.

Files reserve exact operations and carry original receipts; they never approve
orders. Only the online Host calls the official write tools. No write RPC exists
in this module and no model is created here.
"""
from datetime import timedelta
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import threading
import time
import uuid

from .audit import canonical,digest
from .async_native_review import AsyncNativeReviewDispatcher
from .control_tasks import worker_lease
from .host_mailbox import read_private_json,write_private_json
from .host_wakeup import HostWakeOutbox
from .local_worker_wake import worker_adapter_report
from .async_host_wakeup import AsyncHostWakeDispatcher
from .ledger import timestamp
from .native_operations import NativeOperations, native_data
from .qualification import source_fingerprint


def validate_late_receipt_context(action_payload, packet, receipt):
    """Validate the exact logical reference/order identity of a late receipt."""
    if not isinstance(action_payload, dict) or not isinstance(packet, dict):
        raise ValueError('HOST_LATE_RECEIPT_CONTEXT_INVALID')
    logical_ref = action_payload.get('ref_id')
    if (not isinstance(logical_ref, str) or not logical_ref
            or packet.get('logical_ref_id') != logical_ref
            or packet.get('parameters', {}).get('ref_id')
            not in (None, logical_ref)):
        raise ValueError('HOST_LATE_RECEIPT_REF_MISMATCH')
    try:
        order = native_data(receipt['response']).get('order')
    except (ValueError, KeyError, TypeError):
        order = None
    if (isinstance(order, dict) and order.get('ref_id') is not None
            and order.get('ref_id') != logical_ref):
        raise ValueError('HOST_LATE_RECEIPT_ORDER_REF_MISMATCH')
    return True


class HostExecutionBridge:
    requires_observation=True

    def __init__(self,book,directory,*,review_factory=None,wake_adapter=None,wake_timeout_seconds=2,receipt_gate=None,engineering_owner_root=None,
                 engineering_dry_enabled=False,engineering_dry_provenance_reader=None,
                 engineering_dry_risk_reader=None):
        suffix = str(book.policy.get('account_suffix', ''))
        if not (suffix.isdigit() and len(suffix) == 4) or book.policy.get('capture_only') is True:
            raise ValueError('HOST_EXECUTION_POLICY_SCOPE_DENIED')
        if receipt_gate is not None and not callable(receipt_gate):
            raise ValueError('HOST_RECEIPT_GATE_NOT_CALLABLE')
        self.book=book;self.directory=Path(directory).resolve();self.directory.mkdir(parents=True,exist_ok=True);self.directory.chmod(0o700)
        self.receipt_gate=receipt_gate
        self.late_receipt_results=[]
        self._late_receipt_seen=set()
        self._late_scan_cursor=0
        from .engineering_profile import (
            configure_supervised_engineering_profile, review_settings)
        profile_owner_root=configure_supervised_engineering_profile(
            book,native_receipt_root=self.directory,
            native_ingress_root=self.directory/'review-ingress')
        if (profile_owner_root is not None and engineering_owner_root is not None
                and Path(engineering_owner_root).resolve()
                != Path(profile_owner_root).resolve()):
            raise ValueError('SUPERVISED_ENGINEERING_PROFILE_OWNER_MISMATCH')
        profile=getattr(book,'supervised_engineering_profile',None) or {}
        self.engineering_owner_root=(Path(engineering_owner_root).resolve()
                                     if engineering_owner_root is not None
                                     else (Path(profile_owner_root).resolve()
                                           if profile_owner_root is not None
                                           else (Path(profile['owner_root']).resolve()
                                                 if isinstance(
                                                     profile.get('owner_root'), str)
                                                 and profile.get('owner_root')
                                                 else None)))
        self.engineering_review=review_settings(book)
        from .engineering_profile import permit_settings, promotion_settings
        self.engineering_permit=permit_settings(book)
        self.engineering_promotion=promotion_settings(book)
        self._auto_promotion_observed=set()
        self.engineering_ingress_root=Path(
            profile.get('native_ingress_root')
            or (self.directory/'review-ingress')).resolve()
        self.engineering_dry_enabled=engineering_dry_enabled is True
        self.engineering_dry_provenance_reader=engineering_dry_provenance_reader
        self.engineering_dry_risk_reader=engineering_dry_risk_reader
        from .current_context import ensure_default_preflight
        _,self.current_context_status=ensure_default_preflight(
            book,clock=getattr(book,'clock',None),
            formal_authority=getattr(book,'formal_authority',None))
        self.current_context_refresher=getattr(
            book,'_current_context_refresher',None)
        path=book.db.execute('PRAGMA database_list').fetchone()[2]
        if not path:raise ValueError('FILE_BACKED_HOST_BRIDGE_REQUIRED')
        self.binding={'db':str(Path(path).resolve()),'account':'••••'+suffix,'account_key':book.state().get('account_key'),
            'policy_sha256':digest(book.policy),'source_fingerprint':source_fingerprint(),'epoch':str(uuid.uuid4())}
        previous=self.directory/'manifest.json'
        if previous.exists():
            old=read_private_json(previous).get('binding',{})
            if old.get('db')!=self.binding['db'] or old.get('policy_sha256')!=self.binding['policy_sha256']:
                raise ValueError('HOST_DIRECTORY_BINDING_CONFLICT')
        self.review=AsyncNativeReviewDispatcher(book,self.directory/'reviews',transport_factory=review_factory)
        self.task=self.review.task;self.owner_thread=threading.get_ident();self.pending=None;self.retired={};self.abandoned_reads={};self.stopped=False;self.failure=None
        self.wake=HostWakeOutbox(book.db,clock=book.clock)
        self.wake.recover_interrupted()
        self.wake_dispatcher=AsyncHostWakeDispatcher(self.wake,self.directory/'wake-receipts',adapter=wake_adapter,timeout_seconds=wake_timeout_seconds)
        self._publish()

    def _owner(self):
        if threading.get_ident()!=self.owner_thread:raise ValueError('HOST_BRIDGE_OWNER_THREAD_REQUIRED')

    def _review_pending(self,request_id,*,require_fresh=True):
        if (not isinstance(request_id,str)
                or not re.fullmatch('[0-9a-f-]{36}',request_id)):
            raise ValueError('OWNED_REVIEW_REQUEST_ID_INVALID')
        pending=self.pending
        if (not isinstance(pending,dict)
                or pending.get('request_id')!=request_id
                or pending.get('schema_version')!=1
                or pending.get('status')!='PREPARED'):
            raise ValueError('OWNED_REVIEW_PENDING_MISSING')
        packet=pending.get('packet')
        if (not isinstance(packet,dict)
                or packet.get('operation')!='review'
                or packet.get('tool')!='review_equity_order'
                or packet.get('account')!=self.binding['account']
                or not isinstance(packet.get('logical_ref_id'),str)
                or not packet.get('logical_ref_id')
                or pending.get('packet_sha256')!=digest(packet)
                or pending.get('execution_authorized') is not False
                or pending.get('broker_effect_verified') is not False
                or pending.get('binding')!=self.binding):
            raise ValueError('OWNED_REVIEW_PACKET_INVALID')
        try:
            request=read_private_json(
                self.directory/'requests'/(request_id+'.json'))
            action=next((a for a in self.book.actions()
                         if a['id']==packet.get('action_id')),None)
            payload=json.loads(action['payload']) if action else None
        except (OSError,ValueError,KeyError,TypeError,json.JSONDecodeError):
            raise ValueError('OWNED_REVIEW_REQUEST_INVALID') from None
        if (action is None
                or request.get('schema_version')!=1
                or request.get('request_id')!=request_id
                or request.get('operation')!='review'
                or request.get('action_id')!=action['id']
                or request.get('binding')!=self.binding
                or payload.get('parameters')!=packet.get('parameters')):
            raise ValueError('OWNED_REVIEW_REQUEST_INVALID')
        db_path=self.book.db.execute('PRAGMA database_list').fetchone()[2]
        if (not db_path
                or str(Path(db_path).resolve())!=self.binding.get('db')
                or self.binding.get('account')!='••••'+str(self.book.policy.get('account_suffix', ''))
                or packet.get('account_key')!=self.binding.get('account_key')
                or self.binding.get('policy_sha256')!=digest(self.book.policy)
                or self.binding.get('source_fingerprint')!=source_fingerprint()
                or not (str(self.book.policy.get('account_suffix', '')).isdigit() and len(str(self.book.policy.get('account_suffix', ''))) == 4)):
            raise ValueError('OWNED_REVIEW_SOURCE_OR_ACCOUNT_MISMATCH')
        try:
            prepared=timestamp(pending['prepared_at'])
            expires=timestamp(pending['expires_at'])
        except (KeyError,TypeError,ValueError):
            raise ValueError('OWNED_REVIEW_TIME_INVALID') from None
        now=self.book.clock()
        if prepared>now:
            raise ValueError('OWNED_REVIEW_TIME_INVALID')
        if require_fresh and now>=expires:
            raise ValueError('OWNED_REVIEW_EXPIRED')
        return pending,packet,request,expires,now

    def _failure_code(self,error):
        from .formal_review_ingress import FormalReviewIngressError
        from .transport import safe_read_failure
        if isinstance(error,FormalReviewIngressError):
            code=getattr(error,'code',None)
            if isinstance(code,str) and re.fullmatch('[A-Z_0-9:]{1,120}',code):
                return code
        if isinstance(error,ValueError) and str(error) in {
                'OWNED_REVIEW_DEADLINE_EXCEEDED'}:
            return str(error)
        return safe_read_failure(error)['code']

    def _settle_review_failure(self,pending,request_id,ingress_root,error):
        from .formal_review_ingress import FormalReviewIngress
        packet=pending['packet'];operation_id=packet['operation_id']
        code=self._failure_code(error)
        journal=Path(ingress_root)/(operation_id+'.journal.json')
        if journal.exists():
            try:
                FormalReviewIngress(ingress_root,
                    clock=self.book.clock).receive(
                        transport=None,packet=packet,request_id=request_id,
                        host_directory=self.directory,binding=self.binding)
                imported=self._import(pending)
                return {'schema_version':1,'status':'OWNED_REVIEW_RECOVERED_LOCAL',
                    'operation_id':operation_id,'failure':code,
                    'host_import':imported,'model_calls':0,
                    'broker_writes':0,'execution_authorized':False,
                    'broker_effect_verified':False,
                    'native_confirmation_required':True,
                    'attention_required':True}
            except Exception as recovery_error:
                return {'schema_version':1,
                    'status':'OWNED_REVIEW_RECOVERY_REQUIRED',
                    'operation_id':operation_id,'failure':code,
                    'recovery_failure':self._failure_code(recovery_error),
                    'model_calls':0,'broker_writes':0,
                    'execution_authorized':False,
                    'broker_effect_verified':False,
                    'native_confirmation_required':True,
                    'attention_required':True}
        try:
            cleaned=self._abandon_pending_review(
                pending,'EXPLICIT_READ_RECOVERY')
        except Exception as cleanup_error:
            return {'schema_version':1,
                'status':'OWNED_REVIEW_CLEANUP_FAILED',
                'operation_id':operation_id,'failure':code,
                'cleanup_failure':self._failure_code(cleanup_error),
                'model_calls':0,'broker_writes':0,
                'execution_authorized':False,
                'broker_effect_verified':False,
                'native_confirmation_required':True,
                'attention_required':True}
        return {'schema_version':1,'status':'OWNED_REVIEW_FAILED',
            'operation_id':operation_id,'failure':code,
            'cleanup':cleaned,'model_calls':0,'broker_writes':0,
            'execution_authorized':False,
            'broker_effect_verified':False,
            'native_confirmation_required':True,
            'attention_required':True}

    def _durable_review(self,pending,request_id,ingress_root,adapter,
                        expires,now,deadline):
        packet=pending['packet'];operation_id=packet['operation_id']
        expired=now>=expires
        try:
            ingress=adapter.receive(
                transport=None,packet=packet,request_id=request_id,
                host_directory=self.directory,binding=self.binding)
        except Exception as exc:
            return {'schema_version':1,
                'status':'OWNED_REVIEW_HISTORICAL_RECOVERY_REQUIRED',
                'operation_id':operation_id,'historical':True,
                'expired':expired,'failure':self._failure_code(exc),
                'model_calls':0,'broker_writes':0,
                'execution_authorized':False,
                'broker_effect_verified':False,
                'native_confirmation_required':True,
                'attention_required':True}
        try:
            imported=self._import(pending,detached=expired)
        except Exception as exc:
            return {'schema_version':1,
                'status':'OWNED_REVIEW_HISTORICAL_RECOVERY_REQUIRED',
                'operation_id':operation_id,'historical':True,
                'expired':expired,'failure':self._failure_code(exc),
                'model_calls':0,'broker_writes':0,
                'execution_authorized':False,
                'broker_effect_verified':False,
                'native_confirmation_required':True,
                'attention_required':True}
        if (imported.get('status')=='HOST_RECEIPT_IMPORTED'
                and self.pending is pending):
            self.pending=None
        native_result=imported.get('native_result') or {}
        verified=native_result.get('review_verified') is True
        deadline_exhausted=time.monotonic()>=deadline
        status=('OWNED_REVIEW_HISTORICAL_EXPIRED' if expired
                else 'WAITING_USER' if verified
                and imported.get('status')=='HOST_RECEIPT_IMPORTED'
                else 'OWNED_REVIEW_RECOVERED_LOCAL'
                if imported.get('status')=='HOST_RECEIPT_IMPORTED'
                else 'OWNED_REVIEW_IMPORT_REQUIRES_RECOVERY')
        if deadline_exhausted:
            status='OWNED_REVIEW_DEADLINE_EXCEEDED'
        return {'schema_version':1,'status':status,
            'operation_id':operation_id,'action_id':packet['action_id'],
            'historical':True,'expired':expired,
            'review_verified':verified,
            'formal_origin':ingress.get('origin'),
            'receipt_file':imported.get('receipt_file'),
            'ingress_sha256':digest(ingress),
            'host_import':imported,'native_result':native_result,
            'model_calls':0,'broker_writes':0,
            'execution_authorized':False,
            'broker_effect_verified':False,
            'native_confirmation_required':True,
            'deadline_exhausted':deadline_exhausted,
            'cleanup':{'owned':False,'rpc_cleanup':None},
            'stages':{'connect':False,'local_recovery':True,
                      'receipt':True,
                      'import':imported.get('status')=='HOST_RECEIPT_IMPORTED',
                      'rpc_cleanup':None,
                      'pending_cleanup':self.pending is None,
                      'cleanup':self.pending is None},
            'attention_required':True}

    def _finalize_review_run(self,value,cleanup,deadline):
        deadline_exhausted=time.monotonic()>=deadline
        rpc_cleanup=cleanup.get('rpc_cleanup')
        late=cleanup.get('late_registration')
        cleanup_complete=(isinstance(rpc_cleanup,dict)
                          and rpc_cleanup.get('complete') is True
                          and late is None)
        value=dict(value)
        value['cleanup']={**cleanup,'owned':True}
        value['deadline_exhausted']=deadline_exhausted
        stages=dict(value.get('stages') or {})
        stages['rpc_cleanup']=cleanup_complete
        stages['pending_cleanup']=self.pending is None
        stages['cleanup']=cleanup_complete and self.pending is None
        value['stages']=stages
        if not cleanup_complete:
            value['status']='OWNED_REVIEW_CLEANUP_INCOMPLETE'
            value['attention_required']=True
        elif deadline_exhausted:
            value['status']='OWNED_REVIEW_DEADLINE_EXCEEDED'
            value['attention_required']=True
        return value

    def run_prepared_review(self,request_id,*,enabled=False,review_only=False,
                            ingress_root=None,transport_factory=None,
                            deadline_seconds=10):
        """Consume one already prepared Host review packet, default disabled.

        The bridge-owned pending lease is reused; no second ledger/owner or
        write path is created.  Scripted transports require the explicit
        ``test_only`` marker and remain non-formal.
        """
        self._owner()
        if enabled is not True:
            raise ValueError('OWNED_REVIEW_DISABLED')
        if review_only is not True:
            raise ValueError('OWNED_REVIEW_ONLY_REQUIRED')
        if (type(deadline_seconds) not in (int,float)
                or not math.isfinite(deadline_seconds)
                or not 0<deadline_seconds<=10):
            raise ValueError('OWNED_REVIEW_DEADLINE_INVALID')
        if transport_factory is None:
            from .native_review import EquityReviewTransport
            transport_factory=EquityReviewTransport
        if getattr(transport_factory,'test_only',None) is not True:
            if (type(getattr(transport_factory,'simulation_only',None))
                    is not bool
                    or self.book.simulation is not
                    transport_factory.simulation_only):
                raise ValueError('OWNED_REVIEW_PROVENANCE_MISMATCH')
        try:
            pending,packet,request,expires,now=self._review_pending(
                request_id,require_fresh=False)
        except ValueError:
            raise
        ingress_root=Path(ingress_root or (self.directory/'review-ingress'))
        deadline=time.monotonic()+float(deadline_seconds)
        from .control_tasks import ReadTransportOwner,owned_read_transport
        from .formal_review_ingress import FormalReviewIngress
        adapter=FormalReviewIngress(ingress_root,clock=self.book.clock)
        durable=((ingress_root/(packet['operation_id']+'.json')).exists()
                 or (ingress_root/(packet['operation_id']+'.journal.json')).exists()
                 or (self.directory/'receipts'/
                     (packet['operation_id']+'.json')).exists())
        if durable:
            return self._durable_review(
                pending,request_id,ingress_root,adapter,expires,now,deadline)
        if now>=expires:
            try:self._abandon_pending_review(
                pending,'HOST_REVIEW_EXPIRED')
            except Exception:pass
            raise ValueError('OWNED_REVIEW_EXPIRED')
        owner=ReadTransportOwner()
        raw=None;failure=None
        try:
            with owned_read_transport(
                    transport_factory,owner,
                    close_deadline_monotonic=deadline,
                    deadline_monotonic=deadline) as transport:
                if time.monotonic()>=deadline:
                    raise ValueError('OWNED_REVIEW_DEADLINE_EXCEEDED')
                # Re-read after connect; the original expiry is never extended.
                pending,packet,request,expires,now=self._review_pending(
                    request_id,require_fresh=True)
                if time.monotonic()>=deadline:
                    raise ValueError('OWNED_REVIEW_DEADLINE_EXCEEDED')
                try:
                    ingress=adapter.receive(
                        transport=transport,packet=packet,
                        request_id=request_id,
                        host_directory=self.directory,
                        binding=self.binding)
                except Exception as exc:
                    failure=exc
                else:
                    imported=self._import(pending)
                    native_result=imported.get('native_result') or {}
                    verified=native_result.get('review_verified') is True
                    if imported.get('status')!='HOST_RECEIPT_IMPORTED':
                        raw={'schema_version':1,
                            'status':'OWNED_REVIEW_IMPORT_REQUIRES_RECOVERY',
                            'operation_id':packet['operation_id'],
                            'ingress':{k:ingress.get(k) for k in (
                                'origin','call_id','response_sha256',
                                'response_identity_sha256',
                                'host_receipt_sha256','received_at')},
                            'host_import':imported,'model_calls':0,
                            'broker_writes':0,'execution_authorized':False,
                            'broker_effect_verified':False,
                            'native_confirmation_required':True,
                            'stages':{'connect':True,'ingress_received':True,
                                      'receipt':True,'import':False},
                            'attention_required':True}
                    else:
                        raw={'schema_version':1,
                            'status':('WAITING_USER' if verified
                                      else 'REVIEW_FAILED'),
                            'operation_id':packet['operation_id'],
                            'action_id':packet['action_id'],
                            'review_verified':verified,
                            'formal_origin':ingress.get('origin'),
                            'receipt_file':imported.get('receipt_file'),
                            'ingress_sha256':digest(ingress),
                            'host_import':imported,
                            'native_result':native_result,
                            'model_calls':0,'broker_writes':0,
                            'execution_authorized':False,
                            'broker_effect_verified':False,
                            'native_confirmation_required':True,
                            'stages':{'connect':True,
                                      'ingress_received':True,
                                      'receipt':True,'import':True},
                            'attention_required':not verified}
        except Exception as exc:
            failure=exc
        cleanup=owner.cleanup_status()
        if failure is not None:
            if isinstance(failure,ValueError) and str(failure)=='OWNED_REVIEW_EXPIRED':
                try:self._abandon_pending_review(
                    pending,'HOST_REVIEW_EXPIRED')
                except Exception:pass
                result={'schema_version':1,'status':'OWNED_REVIEW_EXPIRED',
                    'operation_id':pending['packet']['operation_id'],
                    'failure':'OWNED_REVIEW_EXPIRED','model_calls':0,
                    'broker_writes':0,'execution_authorized':False,
                    'broker_effect_verified':False,
                    'native_confirmation_required':True,
                    'attention_required':True}
                return self._finalize_review_run(result,cleanup,deadline)
            result=self._settle_review_failure(
                pending,request_id,ingress_root,failure)
            return self._finalize_review_run(result,cleanup,deadline)
        return self._finalize_review_run(raw,cleanup,deadline)

    def _engineering_review_requested(self,request,output):
        """Only the explicit profile review caller for a REVIEW_PENDING entry."""
        if (not isinstance(self.engineering_review,dict)
                or output.get('status')!='HOST_PACKET_PREPARED'
                or request.get('operation')!='review'):
            return False
        action=next((a for a in self.book.actions()
                     if a['id']==output.get('action_id')),None)
        # NativeOperations.begin marks the review reservation RUNNING before
        # the packet is published.  The pending packet is the same review
        # operation; no other state is eligible for this automatic caller.
        if (action is None or action.get('kind')!='entry'
                or action.get('state') not in {'REVIEW_PENDING','RUNNING'}):
            return False
        trade=(self.book.state().get('trades') or {}).get(
            action.get('trade_id')) or {}
        return trade.get('purpose')=='engineering'

    def _review_failure_stage(self,prepared,*,pre_dispatch_proven=False):
        """Classify an exception by exact durable facts, never by self-status.

        Absence or unreadability of evidence is not proof that no provider call
        occurred.  Only an explicit caller-proven pre-dispatch fact may return
        NOT_DISPATCHED; the default path is conservative UNKNOWN/recovery.
        """
        operation_id=prepared.get('operation_id')
        action_id=prepared.get('action_id')
        request_id=prepared.get('request_id')
        row=None;db_error=None;file_error=False
        if isinstance(operation_id,str):
            try:
                row=self.book.db.execute(
                    "SELECT action_id,operation,state,payload,created_at,result "
                    "FROM native_operation_records WHERE id=?",
                    (operation_id,)).fetchone()
            except Exception as exc:
                db_error=getattr(exc,'code',None) or type(exc).__name__
        packet_sha=None;native_state=None;exact=False
        if row is not None:
            row_action,operation,state,raw,created,raw_result=row
            try:
                packet=json.loads(raw)
            except (TypeError,ValueError,KeyError):
                packet=None
            try:
                account_key=self.book.state().get('account_key')
            except Exception as exc:
                db_error=db_error or (getattr(exc,'code',None)
                                      or type(exc).__name__)
                account_key=None
            exact=(row_action==action_id and operation=='review'
                   and isinstance(packet,dict)
                   and packet.get('action_id')==action_id
                   and packet.get('operation')=='review'
                   and packet.get('account_key')==account_key)
            native_state=state if exact else 'UNVERIFIED_ROW'
            packet_sha=digest(packet) if isinstance(packet,dict) else None

        def exists(path):
            nonlocal file_error
            try:
                return path.is_file()
            except Exception:
                file_error=True
                return False

        receipt=(isinstance(operation_id,str)
                 and exists(self.directory/'receipts'/
                            (operation_id+'.json')))
        ingress=(isinstance(operation_id,str)
                 and exists(self.engineering_ingress_root/
                            (operation_id+'.json')))
        journal=(isinstance(operation_id,str)
                 and exists(self.engineering_ingress_root/
                            (operation_id+'.journal.json')))
        result=(isinstance(request_id,str)
                and exists(self.directory/'results'/
                           (request_id+'.json')))
        pending=(self.pending is not None
                 and isinstance(self.pending.get('packet'),dict)
                 and self.pending['packet'].get('operation_id')==operation_id)
        evidence={'operation_id':operation_id,'action_id':action_id,
                  'request_id':request_id,'native_state':native_state,
                  'packet_sha256':packet_sha,'receipt':receipt,
                  'ingress':ingress,'journal':journal,'result_file':result,
                  'pending':pending,'db_error':db_error,
                  'file_error':file_error,
                  'pre_dispatch_proven':pre_dispatch_proven is True}
        if native_state=='RECORDED':
            return {'status':'REVIEW_RESULT_RECOVERY_REQUIRED',
                    'stage':'IMPORTED_FINAL_RESULT_FAILED',
                    'review_status':'RECORDED','recovery_required':True,
                    'evidence':evidence}
        if db_error is not None or file_error:
            return {'status':'REVIEW_RECOVERY_REQUIRED',
                    'stage':'DURABLE_EVIDENCE_UNREADABLE',
                    'review_status':'UNKNOWN','recovery_required':True,
                    'evidence':evidence}
        if native_state in {'STARTED','ABANDONED_READ','UNVERIFIED_ROW'} \
                or receipt or ingress or journal or result or pending:
            return {'status':'REVIEW_RECOVERY_REQUIRED',
                    'stage':('PROVIDER_MAY_HAVE_BEEN_CALLED'
                             if native_state=='STARTED' else
                             'DISPATCHED_OR_UNKNOWN'),
                    'review_status':'UNKNOWN','recovery_required':True,
                    'evidence':evidence}
        if pre_dispatch_proven is True:
            return {'status':'ENGINEERING_REVIEW_REJECTED',
                    'stage':'PRE_DISPATCH_PROVEN',
                    'review_status':'NOT_DISPATCHED','recovery_required':False,
                    'evidence':evidence}
        return {'status':'REVIEW_RECOVERY_REQUIRED',
                'stage':'DISPATCHED_OR_UNKNOWN','review_status':'UNKNOWN',
                'recovery_required':True,'evidence':evidence}

    def _run_engineering_review(self,request,prepared):
        """Run exactly one review-only collection for a prepared profile packet.

        The prepared packet, Host lease, binding and ingress are the existing
        ones.  No permit, READY transition, claim or place is produced here.
        """
        settings=self.engineering_review
        try:
            result=self.run_prepared_review(
                request['request_id'],enabled=True,review_only=True,
                ingress_root=self.engineering_ingress_root,
                transport_factory=self.review.factory,
                deadline_seconds=settings['deadline_seconds'])
        except Exception as exc:
            code=self._failure_code(exc)
            try:
                stage=self._review_failure_stage(prepared)
            except Exception:
                stage={'status':'REVIEW_RECOVERY_REQUIRED',
                       'stage':'CLASSIFIER_FAILED','review_status':'UNKNOWN',
                       'recovery_required':True,'evidence':{}}
            return {**prepared,**stage,'reason':code,'review_only':True,
                'execution_authorized':False,'entry_authorized':False,
                'broker_effect_verified':False,'model_calls':0,
                'native_confirmation_required':bool(
                    stage.get('recovery_required')),
                'attention_required':True}
        review_status=result.get('status')
        status=('WAITING_NATIVE_CONFIRMATION'
                if review_status=='WAITING_USER' else review_status)
        output={**prepared,'status':status,'review_status':review_status,
            'review_result':result,'review_only':True,
            'execution_authorized':False,'entry_authorized':False,
            'broker_effect_verified':False,
            'native_confirmation_required':result.get(
                'native_confirmation_required',True),
            'model_calls':0,
            'attention_required':bool(result.get('attention_required'))}
        if (isinstance(getattr(self,'engineering_permit',None),dict)
                and review_status=='WAITING_USER'):
            from .engineering_permit_producer import (
                EngineeringPermitError,
                produce_supervised_engineering_permit)
            current_facts=self.book.current_account_facts(
                purpose='engineering')
            if (not isinstance(current_facts, dict)
                    or not current_facts.get('assessment_id')):
                self.book.refresh_current_account_assessment(
                    purpose='engineering')
            try:
                permit=produce_supervised_engineering_permit(
                    self.book,result.get('action_id'))
            except EngineeringPermitError as exc:
                output.update(permit_status='BLOCKED',
                    permit_error=exc.code,permit_issued=False,
                    attention_required=True)
            except Exception as exc:
                output.update(permit_status='RECOVERY_REQUIRED',
                    permit_error=getattr(exc,'code',None) or type(exc).__name__,
                    permit_issued=False,attention_required=True)
            else:
                output.update(permit_status=permit.get('status'),
                    permit_id=permit.get('permit_id'),
                    permit_path=permit.get('path'),permit_issued=True,
                    entry_authorized=False,execution_authorized=False)
        return output

    def _auto_issue_engineering_permit(self):
        """Issue one permit from an existing WAITING_USER native review.

        This is the default profile caller when review completed in an earlier
        tick.  It is idempotent and never promotes/claims/places.
        """
        if (not isinstance(getattr(self,'engineering_permit',None),dict)
                or self.pending is not None or self.stopped):
            return None
        for action in self.book.actions():
            if action.get('kind')!='entry' or action.get('state')!='WAITING_USER':
                continue
            trade=(self.book.state().get('trades') or {}).get(
                action.get('trade_id')) or {}
            if trade.get('purpose')!='engineering':
                continue
            from .engineering_permit_producer import (
                EngineeringPermitError,
                produce_supervised_engineering_permit)
            current_facts=self.book.current_account_facts(
                purpose='engineering')
            if (not isinstance(current_facts, dict)
                    or not current_facts.get('assessment_id')):
                self.book.refresh_current_account_assessment(
                    purpose='engineering')
            try:
                permit=produce_supervised_engineering_permit(
                    self.book,action['id'])
            except EngineeringPermitError as exc:
                return {'schema_version':1,
                    'status':'ENGINEERING_PERMIT_BLOCKED',
                    'action_id':action['id'],'permit_status':'BLOCKED',
                    'permit_error':exc.code,'permits_issued':False,
                    'entry_authorized':False,'execution_authorized':False,
                    'broker_effect_verified':False,'model_calls':0,
                    'attention_required':True}
            except Exception as exc:
                return {'schema_version':1,
                    'status':'ENGINEERING_PERMIT_RECOVERY_REQUIRED',
                    'action_id':action['id'],
                    'permit_status':'RECOVERY_REQUIRED',
                    'permit_error':getattr(exc,'code',None) or type(exc).__name__,
                    'permits_issued':False,'entry_authorized':False,
                    'execution_authorized':False,'broker_effect_verified':False,
                    'model_calls':0,'attention_required':True}
            if permit.get('status')=='EXISTING':
                return None
            return {'schema_version':1,'status':'ENGINEERING_PERMIT_ISSUED',
                'action_id':action['id'],'permit_status':permit.get('status'),
                'permit_id':permit.get('permit_id'),
                'permit_path':permit.get('path'),'permits_issued':True,
                'entry_authorized':False,'execution_authorized':False,
                'broker_effect_verified':False,'model_calls':0,
                'attention_required':False}
        return None

    def _engineering_promotion_consumption(self, permit):
        """Read the existing one-shot row for an exact permit, never write."""
        one_shot=permit.get('one_shot_id'); action_id=permit.get('action_id')
        if (not isinstance(one_shot,str) or not one_shot
                or not isinstance(action_id,str) or not action_id):
            return {'status':'INVALID'}
        try:
            exists=self.book.db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                ('formal_authorization_consumptions',)).fetchone()
            if not exists:
                return {'status':'ABSENT'}
            row=self.book.db.execute(
                'SELECT source_sha256,binding,action_id,consumed_at '
                'FROM formal_authorization_consumptions WHERE one_shot_id=?',
                (one_shot,)).fetchone()
        except Exception as exc:
            return {'status':'UNAVAILABLE',
                    'reason':getattr(exc,'code',None) or type(exc).__name__}
        if row is None:
            return {'status':'ABSENT'}
        return {'status':'PRESENT','source_sha256':row[0],'binding':row[1],
                'action_id':row[2],'consumed_at':row[3]}

    def _engineering_promotion_fact(self, permit, action):
        """Return exact durable consumption+READY audit or a named gap."""
        if not isinstance(permit,dict) or not isinstance(action,dict):
            return {'status':'MISSING'}
        one_shot=permit.get('one_shot_id'); permit_id=permit.get('permit_id')
        action_id=permit.get('action_id')
        if (not isinstance(one_shot,str) or not one_shot
                or not isinstance(permit_id,str) or not permit_id
                or action.get('id')!=action_id):
            return {'status':'INVALID'}
        try:
            binding=digest(permit)
        except Exception:
            return {'status':'INVALID'}
        consumed=self._engineering_promotion_consumption(permit)
        if consumed.get('status')!='PRESENT':
            return {'status':'MISSING',
                    'reason':consumed.get('reason') or consumed.get('status')}
        if (consumed.get('source_sha256')!=permit.get('request_sha256')
                or consumed.get('binding')!=binding
                or consumed.get('action_id')!=action_id):
            return {'status':'CONFLICT'}
        try:
            row=self.book.db.execute(
                'SELECT payload FROM audit_events WHERE event_id=?',
                ('supervised-intent-'+action_id+'-promoted',)).fetchone()
        except Exception as exc:
            return {'status':'UNAVAILABLE',
                    'reason':getattr(exc,'code',None) or type(exc).__name__}
        if row is None:
            return {'status':'MISSING','reason':'PROMOTION_AUDIT_MISSING'}
        try:
            details=json.loads(row[0])['details']
        except (TypeError,ValueError,KeyError):
            return {'status':'INVALID'}
        if (details.get('status')!='SUPERVISED_ENGINEERING_INTENT_PROMOTED'
                or details.get('action_id')!=action_id
                or details.get('permit_id')!=permit_id
                or details.get('one_shot_id')!=one_shot
                or details.get('binding')!=consumed.get('binding')):
            return {'status':'CONFLICT'}
        return {'status':'FOUND','binding':consumed['binding'],
                'consumed_at':consumed['consumed_at'],
                'source_sha256':consumed['source_sha256']}

    def _promotion_claim_recheck(self, action):
        """Read-only current eligibility check; never claims or writes.

        READY and a historical consume are facts, not a current claim grant.
        The existing authority validation is re-run with the already-consumed
        one-shot allowed, then current preflight and entry-authority checks are
        read without mutating the action.  A future claim still performs the
        complete claim transaction.
        """
        authority=self.book.supervised_engineering_authority
        if authority is None:
            return {'status':'UNKNOWN','reason':'SUPERVISED_ENGINEERING_AUTHORITY_UNAVAILABLE'}
        from .supervised_engineering import SupervisedEngineeringError
        try:
            authority._validate(self.book,action=action,purpose='engineering',
                                allow_consumed=True)
        except (SupervisedEngineeringError,ValueError,KeyError,TypeError,OSError) as exc:
            return {'status':'BLOCKED',
                    'reason':getattr(exc,'code',None) or type(exc).__name__}
        except Exception as exc:
            return {'status':'UNKNOWN',
                    'reason':getattr(exc,'code',None) or type(exc).__name__}
        try:
            preflight=self.book.check_current_entry_action(action)
        except Exception as exc:
            return {'status':'UNKNOWN',
                    'reason':getattr(exc,'code',None) or type(exc).__name__}
        if preflight.get('allowed') is not True:
            return {'status':'BLOCKED',
                    'reason':','.join(preflight.get('reasons') or ['CURRENT_PREFLIGHT_BLOCKED'])}
        try:
            entry=self.book.entry_authority.check(
                source='HostExecutionBridge.auto-promotion.claim-recheck',
                action=action,purpose='engineering')
        except Exception as exc:
            return {'status':'UNKNOWN',
                    'reason':getattr(exc,'code',None) or type(exc).__name__}
        if entry.get('allowed') is not True:
            return {'status':'BLOCKED',
                    'reason':entry.get('reason') or 'ENTRY_AUTHORITY_BLOCKED'}
        return {'status':'ALLOWED_FOR_FULL_CLAIM_CHECK','reason':None}

    def _promotion_result(self, permit, action, fact, *, status, recovered):
        state=action.get('state') if isinstance(action,dict) else 'UNKNOWN'
        recheck=(self._promotion_claim_recheck(action)
                 if state=='READY' else {'status':'NOT_READY','reason':None})
        claim_ready=recheck.get('status')=='ALLOWED_FOR_FULL_CLAIM_CHECK'
        attention=(state=='READY' and not claim_ready)
        return {'schema_version':1,'status':status,
            'request_id':permit.get('request_id'),
            'action_id':permit.get('action_id'),
            'permit_id':permit.get('permit_id'),'state':state,
            'current_state':state,
            'historical_state':('READY' if recovered else state),
            'recovered':recovered,
            'historical_local_promotion':recovered,
            'historical_binding':(fact or {}).get('binding'),
            'historical_consumed_at':(fact or {}).get('consumed_at'),
            'reviewable':True,'claimable':False,
            'claim_check_required':(state=='READY'),
            'claim_ready_for_check':claim_ready,
            'claim_recheck_status':recheck.get('status'),
            'claim_recheck_reason':recheck.get('reason'),
            'placeable':False,
            'entry_authorized':False,'execution_authorized':False,
            'broker_effect_verified':False,'binding':dict(self.binding),
            'model_calls':0,'attention_required':attention}

    def _auto_promote_engineering_permit(self, *, allow_entries):
        """Consume the exact current owner permit through the existing core.

        This is the default profile caller: it reads the permit's exact
        action/ref, calls ``ExecutionBook.promote_supervised_entry`` (same
        authority/local grant/finalize/consume/READY transaction), and never
        claims or places.  A READY action with exact durable consumption and
        promotion audit is reported as recovered without another consume.
        """
        if (not isinstance(getattr(self,'engineering_promotion',None),dict)
                or self.pending is not None or self.stopped
                or allow_entries is not True):
            return None
        root=self.engineering_owner_root
        if root is None:
            return None
        from .supervised_engineering import PERMIT_FILE
        permit_path=Path(root)/PERMIT_FILE
        try:
            permit=read_private_json(permit_path)
        except FileNotFoundError:
            return None
        except (OSError,ValueError,json.JSONDecodeError):
            return {'schema_version':1,'status':'ENGINEERING_PROMOTION_BLOCKED',
                'permit_status':'BLOCKED','promotion_error':
                'SUPERVISED_ENGINEERING_PERMIT_UNAVAILABLE',
                'permits_issued':False,'entry_authorized':False,
                'execution_authorized':False,'broker_effect_verified':False,
                'model_calls':0,'attention_required':True}
        action_id=permit.get('action_id'); permit_id=permit.get('permit_id')
        if (not isinstance(action_id,str) or not action_id
                or not isinstance(permit_id,str)
                or not re.fullmatch('[0-9a-f]{64}',permit_id)
                or permit.get('purpose')!='engineering'):
            return {'schema_version':1,'status':'ENGINEERING_PROMOTION_BLOCKED',
                'permit_status':'BLOCKED','promotion_error':
                'SUPERVISED_ENGINEERING_PERMIT_BINDING',
                'entry_authorized':False,'execution_authorized':False,
                'broker_effect_verified':False,'model_calls':0,
                'attention_required':True}
        action=next((a for a in self.book.actions()
                     if a.get('id')==action_id),None)
        if action is None:
            return {'schema_version':1,'status':'ENGINEERING_PROMOTION_RECOVERY_REQUIRED',
                'permit_status':'RECOVERY_REQUIRED',
                'promotion_error':'ENGINEERING_PROMOTION_ACTION_MISSING',
                'action_id':action_id,'permit_id':permit_id,
                'entry_authorized':False,'execution_authorized':False,
                'broker_effect_verified':False,'model_calls':0,
                'attention_required':True}
        try:
            payload=json.loads(action.get('payload'))
        except (TypeError,ValueError):
            payload=None
        if (not isinstance(payload,dict)
                or payload.get('purpose')!='engineering'
                or payload.get('ref_id')!=permit.get('ref_id')):
            return {'schema_version':1,'status':'ENGINEERING_PROMOTION_BLOCKED',
                'permit_status':'BLOCKED','promotion_error':
                'SUPERVISED_ENGINEERING_PERMIT_ACTION_BINDING',
                'action_id':action_id,'permit_id':permit_id,
                'entry_authorized':False,'execution_authorized':False,
                'broker_effect_verified':False,'model_calls':0,
                'attention_required':True}
        if self.book.state().get('account_key')!=permit.get('account_key'):
            return {'schema_version':1,'status':'ENGINEERING_PROMOTION_BLOCKED',
                'permit_status':'BLOCKED','promotion_error':
                'SUPERVISED_ENGINEERING_ACCOUNT_MISMATCH',
                'action_id':action_id,'permit_id':permit_id,
                'entry_authorized':False,'execution_authorized':False,
                'broker_effect_verified':False,'model_calls':0,
                'attention_required':True}
        state=action.get('state')
        if state=='READY':
            if action_id in self._auto_promotion_observed:
                return None
            fact=self._engineering_promotion_fact(permit,action)
            if fact.get('status')=='FOUND':
                return self._promotion_result(
                    permit,action,fact,status=(
                        'ENGINEERING_PROMOTION_ALREADY_COMPLETED'),
                    recovered=True)
            return {'schema_version':1,'status':'ENGINEERING_PROMOTION_RECOVERY_REQUIRED',
                'permit_status':'RECOVERY_REQUIRED',
                'promotion_error':('ENGINEERING_PROMOTION_EVIDENCE_'
                    +str(fact.get('status') or 'MISSING')),
                'action_id':action_id,'permit_id':permit_id,
                'state':state,'current_state':state,'claimable':False,
                'placeable':False,'entry_authorized':False,
                'execution_authorized':False,'broker_effect_verified':False,
                'model_calls':0,'attention_required':True}
        if state not in {'REVIEW_PENDING','WAITING_USER'}:
            return None
        consumed=self._engineering_promotion_consumption(permit)
        if consumed.get('status')!='ABSENT':
            return {'schema_version':1,'status':'ENGINEERING_PROMOTION_RECOVERY_REQUIRED',
                'permit_status':'RECOVERY_REQUIRED',
                'promotion_error':'ENGINEERING_PROMOTION_CONSUMPTION_'
                    +str(consumed.get('status') or 'UNKNOWN'),
                'action_id':action_id,'permit_id':permit_id,
                'state':state,'current_state':state,'claimable':False,
                'placeable':False,'entry_authorized':False,
                'execution_authorized':False,'broker_effect_verified':False,
                'model_calls':0,'attention_required':True}
        try:
            self.book.promote_supervised_entry(
                action_id,expected_permit_id=permit_id)
        except Exception as exc:
            text=str(exc)
            code=(text if re.fullmatch('[A-Z0-9_:]{1,300}',text)
                  else (getattr(exc,'code',None) or type(exc).__name__))
            current=next((a for a in self.book.actions()
                          if a.get('id')==action_id),None)
            if current is not None and current.get('state')=='READY':
                fact=self._engineering_promotion_fact(permit,current)
                if fact.get('status')=='FOUND':
                    return self._promotion_result(
                        permit,current,fact,status=(
                            'ENGINEERING_PROMOTION_ALREADY_COMPLETED'),
                        recovered=True)
            blocked_prefixes=(
                'SUPERVISED_ENGINEERING_PERMIT_BLOCKED:',
                'ENTRY_AUTHORITY_BLOCKED:',
                'CURRENT_ENGINEERING_PREFLIGHT_BLOCKED:',
                'SUPERVISED_ENGINEERING_PERMIT_REFERENCE_MISMATCH',
                'SUPERVISED_ENGINEERING_REAL_ACTIVATION_DISABLED',
                'SUPERVISED_ENGINEERING_REAL_SOURCE',
                'SUPERVISED_ENGINEERING_ACTION_',
                'SUPERVISED_ENGINEERING_POLICY_CHANGED',
                'SUPERVISED_ENGINEERING_ACCOUNT_CHANGED',
                'SUPERVISED_ENGINEERING_SOURCE_CHANGED',
                'SUPERVISED_ENGINEERING_OWNER_',
                'SUPERVISED_ENGINEERING_COMPONENT_',
                'SUPERVISED_ENGINEERING_FACTS_',
                'SUPERVISED_ENGINEERING_REVIEW_',
                'SUPERVISED_ENGINEERING_PERMIT_WINDOW',
                'SUPERVISED_ENGINEERING_PROMOTION_STATE')
            recovery=not str(code).startswith(blocked_prefixes)
            return {'schema_version':1,'status':(
                    'ENGINEERING_PROMOTION_RECOVERY_REQUIRED' if recovery
                    else 'ENGINEERING_PROMOTION_BLOCKED'),
                'permit_status':('RECOVERY_REQUIRED' if recovery
                                 else 'BLOCKED'),
                'promotion_error':code,'action_id':action_id,
                'permit_id':permit_id,'state':(current or {}).get('state'),
                'current_state':(current or {}).get('state') or 'UNKNOWN',
                'claimable':False,'placeable':False,
                'entry_authorized':False,'execution_authorized':False,
                'broker_effect_verified':False,'model_calls':0,
                'attention_required':True}
        current=next((a for a in self.book.actions()
                      if a.get('id')==action_id),None)
        if current is None or current.get('state')!='READY':
            return {'schema_version':1,'status':'ENGINEERING_PROMOTION_RECOVERY_REQUIRED',
                'permit_status':'RECOVERY_REQUIRED',
                'promotion_error':'ENGINEERING_PROMOTION_READY_NOT_VISIBLE',
                'action_id':action_id,'permit_id':permit_id,
                'entry_authorized':False,'execution_authorized':False,
                'broker_effect_verified':False,'model_calls':0,
                'attention_required':True}
        return self._promotion_result(
            permit,current,None,status='ENGINEERING_PROMOTION_READY',
            recovered=False)

    def _wake_worker_action(self, action):
        if getattr(getattr(self.wake_dispatcher, 'adapter', None),
                   'needs_action', False) is not True:
            return None
        try:
            payload = json.loads(action['payload'])
        except (KeyError, TypeError, ValueError):
            return None
        return {**payload, 'action_id': action['id'],
                'kind': action['kind'], 'state': action['state'],
                'trade_id': action['trade_id'], 'synthetic': False}

    def enqueue_local_worker_action(self, action):
        """Public explicit path from a claimed action to a local wake event."""
        # The first reconciled snapshot binds the account after bridge
        # construction. Refresh the exact owner binding before reserving the
        # durable event; a stale/empty account key is never a wake authority.
        self.binding['account_key'] = self.book.state().get('account_key')
        worker_action = self._wake_worker_action(action)
        if worker_action is None:
            raise ValueError('HOST_LOCAL_WORKER_ADAPTER_NOT_SELECTED')
        try:
            payload = json.loads(action['payload'])
            expires = payload['expires_at']
        except (KeyError, TypeError, ValueError):
            raise ValueError('HOST_LOCAL_WORKER_ACTION_INVALID') from None
        revision = digest({'action_id': action['id'],
                           'state': action['state'],
                           'operation': 'local-worker',
                           'ref': worker_action.get('ref_id'),
                           'payload_sha256': digest(worker_action)})
        event_id = self.wake.enqueue(
            binding=dict(self.binding), action_id=action['id'],
            kind=action['kind'], revision=revision,
            expires_at=expires, worker_action=worker_action)
        status = self.wake_dispatcher.tick(binding=dict(self.binding),
                                           current_event_ids=[event_id])
        return {'event_id': event_id, 'action_id': action['id'],
                **status, 'execution_authorized': False,
                'broker_effect_verified': False}

    def consume_local_worker_results(self):
        return self._consume_local_worker_results()

    def settle_local_worker_wake(self):
        """Collect a completed wake receipt without reserving new work."""
        pending = getattr(self.wake_dispatcher, 'pending', None)
        if pending is None:
            return None
        return self.wake_dispatcher.tick(binding=dict(self.binding))

    def _consume_local_worker_results(self):
        """Consume only this bridge's bound local-worker results once."""
        adapter = getattr(self.wake_dispatcher, 'adapter', None)
        if not hasattr(adapter, 'take_result'):
            return []
        results = []
        limit = int(getattr(adapter, 'MAX_RESULTS_PER_TAKE', 32))
        for _ in range(max(1, limit)):
            item = adapter.take_result()
            if item is None:
                break
            path, value = item
            status = ((value.get('worker_result') or {}).get('status')
                      if isinstance(value.get('worker_result'), dict) else None)
            claim = (adapter.claim_state(value.get('idempotency_key'),
                                         value.get('attempt_id'))
                     if hasattr(adapter, 'claim_state') else None)
            if (value.get('late_after_timeout') is True
                    or not isinstance(claim, dict)
                    or claim.get('state') != 'RESULT_RECORDED'
                    or claim.get('idempotency_key') != value.get('idempotency_key')
                    or claim.get('attempt_id') != value.get('attempt_id')
                    or claim.get('binding') != value.get('binding')
                    or claim.get('action_id') != value.get('action_id')
                    or claim.get('revision') != value.get('revision')):
                disposition = ('LATE_WORKER_RESULT_REJECTED'
                               if value.get('late_after_timeout') is True
                               else 'UNKNOWN_OR_UNBOUND_WORKER_RESULT_REJECTED')
                adapter.finish_result(path, disposition)
                results.append({'action_id': value.get('action_id'),
                                'disposition': disposition,
                                'execution_authorized': False})
                continue
            action_id = value.get('action_id')
            binding = value.get('binding')
            action = next((a for a in self.book.actions()
                           if a.get('id') == action_id), None)
            retired = (action_id in self.book.recovery_status().get(
                'acknowledged_action_ids', []))
            if (binding != self.binding or action is None or retired):
                disposition = ('STALE_EPOCH_OR_BINDING_REJECTED'
                               if binding != self.binding
                               else 'ACTION_MISSING_OR_RETIRED_REJECTED')
                adapter.finish_result(path, disposition)
                results.append({'action_id': action_id,
                                'disposition': disposition,
                                'execution_authorized': False})
                continue
            if status not in {'needs_confirmation', 'review_only', 'unknown',
                              'submitted', 'pending_approval',
                              'cancel_requested', 'rejected'}:
                adapter.finish_result(path, 'WORKER_STATUS_REJECTED')
                results.append({'action_id': action_id,
                                'disposition': 'WORKER_STATUS_REJECTED',
                                'execution_authorized': False})
                continue
            try:
                current_payload = json.loads(action.get('payload') or '{}')
                current_ref = current_payload.get('ref_id')
            except (TypeError, ValueError):
                current_payload = {}
                current_ref = None
            claim_action = claim.get('action') or {}
            claim_ref = (claim_action.get('ref_id')
                         or (claim_action.get('parameters') or {}).get('ref_id'))
            current_parameters = current_payload.get('parameters')
            claim_parameters = claim_action.get('parameters')
            expected_revision = digest({
                'action_id': action_id,
                'state': claim_action.get('state'),
                'operation': 'local-worker',
                'ref': claim_ref,
                'payload_sha256': digest(claim_action),
            })
            if (claim_action.get('action_id') != action_id
                    or not current_ref or claim_ref != current_ref
                    or claim_action.get('kind') != action.get('kind')
                    or claim_action.get('trade_id') != action.get('trade_id')
                    or current_parameters != claim_parameters
                    or claim_action.get('target_order_id')
                    != current_payload.get('target_order_id')
                    or claim_action.get('state') != 'RUNNING'
                    or claim.get('revision') != expected_revision):
                disposition = 'WORKER_ACTION_REF_REVISION_MISMATCH'
                adapter.finish_result(path, disposition)
                results.append({'action_id': action_id,
                                'disposition': disposition,
                                'execution_authorized': False})
                continue
            report = {key: value['worker_result'][key] for key in
                      ('status', 'message', 'order_id',
                       'market_data_disclosure')
                      if key in value['worker_result']}
            expected_state = ('UNKNOWN' if status == 'unknown' else
                              'WAITING_USER'
                              if status in {'needs_confirmation', 'review_only'}
                              else 'REPORTED')

            def applied(current):
                if current is None or current.get('state') != expected_state:
                    return False
                try:
                    return json.loads(current.get('report') or '{}') == report
                except (TypeError, ValueError):
                    return False

            if action.get('state') != 'RUNNING':
                if applied(action):
                    adapter.finish_result(
                        path, 'ALREADY_APPLIED_RECOVERY_FINALIZED')
                    results.append({
                        'action_id': action_id,
                        'disposition': 'ALREADY_APPLIED_RECOVERY_FINALIZED',
                        'execution_authorized': False})
                else:
                    adapter.finish_result(
                        path, 'BOOK_ALREADY_ADVANCED_RESULT_RETAINED')
                    results.append({
                        'action_id': action_id,
                        'disposition': 'BOOK_ALREADY_ADVANCED_RESULT_RETAINED',
                        'execution_authorized': False})
                continue
            try:
                self.book.report(action_id, report)
            except sqlite3.Error as exc:
                # A locked/busy/full owner is not proof that the result was
                # invalid. Return the exact durable result to the queue for a
                # later owner tick; never start another worker execution.
                requeued = (adapter.requeue_result(path, type(exc).__name__)
                            if hasattr(adapter, 'requeue_result') else None)
                disposition = ('BOOK_REPORT_RETRY_DEFERRED' if requeued
                               else 'BOOK_REPORT_DEFER_FAILED_REQUIRES_HANDOFF')
                results.append({'action_id': action_id,
                                'disposition': disposition,
                                'execution_authorized': False})
                break
            except (ValueError, KeyError, TypeError):
                current = next((a for a in self.book.actions()
                                if a.get('id') == action_id), None)
                if applied(current):
                    adapter.finish_result(
                        path, 'ALREADY_APPLIED_RECOVERY_FINALIZED')
                    results.append({
                        'action_id': action_id,
                        'disposition': 'ALREADY_APPLIED_RECOVERY_FINALIZED',
                        'execution_authorized': False})
                else:
                    adapter.finish_result(path, 'BOOK_REPORT_REJECTED')
                    results.append({'action_id': action_id,
                                    'disposition': 'BOOK_REPORT_REJECTED',
                                    'execution_authorized': False})
                continue
            disposition = ('WAITING_CONFIRMATION_RECORDED'
                           if status in {'needs_confirmation', 'review_only'}
                           else 'UNKNOWN_REQUIRES_RECONCILIATION'
                           if status == 'unknown' else 'BOOK_REPORT_IMPORTED')
            adapter.finish_result(path, disposition)
            results.append({'action_id': action_id,
                            'disposition': disposition,
                            'execution_authorized': False})
        return results

    def _publish(self):
        self.binding['account_key']=self.book.state().get('account_key')
        actions=[]
        retired=set(self.book.recovery_status()['acknowledged_action_ids'])
        for action in self.book.actions():
            if action['id'] in retired or action['state'] not in {'READY','WAITING_USER','RUNNING','UNKNOWN','REPORTED','REVIEW_PENDING'}:continue
            payload=json.loads(action['payload'])
            item={k:action[k] for k in ('id','kind','state')}
            item.update(parameters=payload['parameters'],expires_at=payload['expires_at'],purpose=payload.get('purpose'))
            if action['state']=='REVIEW_PENDING':
                item.update(reviewable=True,claimable=False,placeable=False,
                    entry_authorized=False,status='REVIEW_INTENT_RECORDED')
            elif action['kind']=='entry' and action['state']=='READY':
                # READY is a local fact, not a current claim authorization.
                # Engineering promotion (manual or auto) must re-run the full
                # claim checks; never publish a static claimable=true.
                if payload.get('purpose')=='engineering':
                    item.update(reviewable=True,claimable=False,
                        claim_check_required=True,
                        claim_recheck_status='REQUIRES_CURRENT_CLAIM_CHECK',
                        placeable=False,entry_authorized=False,
                        status='LOCAL_READY')
                else:
                    item.update(reviewable=True,claimable=True,placeable=False,
                        entry_authorized=False,status='LOCAL_READY')
            deadline=self.review.protection_deadline(action)
            if deadline:item.update(protection_deadline=deadline.isoformat(),
                protection_seconds_remaining=max(0,(deadline-self.book.clock()).total_seconds()))
            if action['state']=='WAITING_USER':item['review']=self.review._waiting(action)
            actions.append(item)
        # The bounded public view must not hide a callable pending packet or
        # management work behind older entry intents. actions() already has a
        # stable created_at/id order; stable sorting preserves it within rank.
        pending_action=self.pending['packet']['action_id'] if self.pending else None
        actions.sort(key=lambda item:(-1 if item['id']==pending_action else
            {'protect':0,'cancel':1,'exit':2,'entry':3}.get(item['kind'],4)))
        # Persist exact actionable transitions once. This default outbox has no
        # configured official Host queue and never starts a model or tool call.
        if not self.stopped and self.binding['account_key']:
            current_wake_ids=[]
            for item in actions:
                waiting=item.get('review',{})
                if waiting.get('status')!='WAITING_USER' or not waiting.get('native_result',{}).get('review_verified'):continue
                expiry=min(timestamp(item['expires_at']),timestamp(waiting['review_valid_until']))
                if expiry<=self.book.clock():continue
                current_wake_ids.append(self.wake.enqueue(binding=dict(self.binding),action_id=item['id'],kind=item['kind'],
                    revision=digest({'state':item['state'],'operation_id':waiting['operation_id'],
                        'response_hash':waiting['native_result'].get('response_hash')}),expires_at=expiry.isoformat(),
                    worker_action=self._wake_worker_action(
                        next((a for a in self.book.actions() if a['id']==item['id']), item))))
            if self.pending and self.book.clock()<timestamp(self.pending['expires_at']):
                packet=self.pending['packet']
                item=next((a for a in actions if a['id']==packet['action_id']),None)
                if item:current_wake_ids.append(self.wake.enqueue(binding=dict(self.binding),action_id=item['id'],kind=item['kind'],
                    revision=digest({'packet_sha256':digest(packet),'status':'PREPARED'}),
                    expires_at=self.pending['expires_at'],
                    worker_action=self._wake_worker_action(
                        next((a for a in self.book.actions() if a['id']==item['id']), item))))
            self.wake.retire_superseded(binding=dict(self.binding),current_event_ids=current_wake_ids)
            wake_status=self.wake_dispatcher.tick(binding=dict(self.binding),current_event_ids=current_wake_ids)
        else:
            if self.binding['account_key']:
                self.wake.retire_superseded(binding=dict(self.binding),current_event_ids=[])
            wake_status={'dispatched':False,'reason':'HOST_BRIDGE_NOT_RUNNING','execution_authorized':False}
        value={'schema_version':1,'running':not self.stopped and bool(self.binding['account_key']),
            'binding':self.binding,'published_at':self.book.clock().isoformat(),'actions':actions[:20],
            'actions_truncated':len(actions)>20,'execution_authorized':False,'broker_writes_by_bridge':0,
            'pending_operation_id':self.pending['packet']['operation_id'] if self.pending else None,
            'observed_receipts_pending':list(self.retired),
            'abandoned_read_receipts_pending':list(self.abandoned_reads),
            'requires_online_formal_host':True,'host_wakeup':wake_status,
            'worker_adapter':worker_adapter_report(
                getattr(self.wake_dispatcher,'adapter',None)),
            'production_ready':False}
        path=self.directory/'manifest.json';temporary=path.with_name('manifest-'+uuid.uuid4().hex+'.tmp')
        with temporary.open('x') as handle:
            os.chmod(temporary,0o600);handle.write(canonical(value)+'\n');handle.flush();os.fsync(handle.fileno())
        temporary.replace(path)
        descriptor=os.open(self.directory,os.O_RDONLY)
        try:os.fsync(descriptor)
        finally:os.close(descriptor)

    def _uncertain(self,reason,pending=None):
        pending=pending or self.pending
        if pending and pending['packet']['operation'] in {'place','cancel'}:
            NativeOperations(self.book).mark_uncertain(pending['packet']['operation_id'],reason)

    def _late_diagnostics(self, value):
        key = (value.get('operation_id'), value.get('status'),
               value.get('response_sha256'))
        if key in self._late_receipt_seen:
            return
        self._late_receipt_seen.add(key)
        if len(self._late_receipt_seen) > 512:
            self._late_receipt_seen.clear()
            self._late_receipt_seen.add(key)
        if len(self.late_receipt_results) >= 64:
            self.late_receipt_results.pop(0)
        self.late_receipt_results.append(value)

    @staticmethod
    def _receipt_import_output(*, request_id, binding, packet, action_id,
                              trade_id, result):
        report = result.get('report') if isinstance(result, dict) else None
        status = report.get('status') if isinstance(report, dict) else None
        return {
            'schema_version': 1, 'status': 'HOST_RECEIPT_IMPORTED',
            'request_id': request_id, 'binding': binding,
            'operation_id': packet['operation_id'], 'action_id': action_id,
            'trade_id': trade_id, 'packet_sha256': digest(packet),
            'native_result': result, 'broker_effect_verified': False,
            'execution_authorized': False, 'model_calls': 0,
            'attention_required': (
                not result.get('late_receipt_retained', False)
                and status in {'unknown', 'pending_approval',
                               'needs_confirmation', 'review_only'}),
        }

    def _recover_late_receipts(self):
        """Import exact historical receipts for durable native operations.

        This is the restart/default-loop consumer.  It is deliberately a
        bounded, cursor-based scan of the native ledger, not a directory
        ignore/delete or an in-memory pending replay.  Every candidate is
        initialized independently: one malformed file cannot escape into the
        next candidate or interrupt protective management.
        """
        cursor = getattr(self, '_late_scan_cursor', 0)
        try:
            rows = self.book.db.execute(
                "SELECT rowid,id,action_id,operation,payload,state,result"
                " FROM native_operation_records"
                " WHERE rowid>? AND state IN ('STARTED','RECORDED')"
                " AND operation IN ('place','cancel')"
                " ORDER BY rowid LIMIT 100", (cursor,)).fetchall()
        except sqlite3.OperationalError:
            return list(self.late_receipt_results)
        if not rows and cursor:
            self._late_scan_cursor = 0
            try:
                rows = self.book.db.execute(
                    "SELECT rowid,id,action_id,operation,payload,state,result"
                    " FROM native_operation_records"
                    " WHERE state IN ('STARTED','RECORDED')"
                    " AND operation IN ('place','cancel')"
                    " ORDER BY rowid LIMIT 100").fetchall()
            except sqlite3.OperationalError:
                return list(self.late_receipt_results)
        if rows:
            self._late_scan_cursor = rows[-1][0]
        for rowid, operation_id, action_id, operation, raw, state, stored in rows:
            receipt = None
            packet = None
            try:
                path = self.directory/'receipts'/(operation_id+'.json')
                if not path.is_file():
                    continue
                receipt = read_private_json(path)
                if not isinstance(receipt, dict):
                    raise ValueError('HOST_LATE_RECEIPT_INVALID')
                if (receipt.get('schema_version') != 1
                        or receipt.get('operation_id') != operation_id
                        or receipt.get('action_id') != action_id
                        or receipt.get('execution_authorized') is not False
                        or receipt.get('broker_effect_verified') is not False
                        or not isinstance(receipt.get('response'), dict)
                        or receipt.get('response_sha256') != digest(
                            receipt['response'])):
                    raise ValueError('HOST_LATE_RECEIPT_INVALID')
                request_id = receipt.get('request_id')
                if not isinstance(request_id, str) or not request_id:
                    raise ValueError('HOST_LATE_RECEIPT_REQUEST_INVALID')
                if (self.pending is not None
                        and (self.pending.get('request_id') == request_id
                             or self.pending.get('packet', {}).get(
                                 'operation_id') == operation_id)):
                    # The active pending import owns its lease/result; do not
                    # publish a divergent historical result ahead of it.
                    continue
                packet = json.loads(raw)
                action_row = self.book.db.execute(
                    'SELECT trade_id,payload FROM live_actions WHERE id=?',
                    (action_id,)).fetchone()
                if action_row is None:
                    raise ValueError('HOST_LATE_RECEIPT_ACTION_MISSING')
                trade_id, raw_payload = action_row
                payload = json.loads(raw_payload)
                validate_late_receipt_context(payload, packet, receipt)
                envelope = read_private_json(
                    self.directory/'packets'/(request_id+'.json'))
                if not isinstance(envelope.get('prepared_at'), str):
                    raise ValueError('HOST_LATE_RECEIPT_TIME_INVALID')
                received = timestamp(receipt.get('received_at'))
                if (received < timestamp(envelope['prepared_at'])
                        or received > self.book.clock()):
                    raise ValueError('HOST_LATE_RECEIPT_TIME_INVALID')
                pending = {'request_id': request_id, 'packet': packet,
                           'binding': receipt.get('binding')}
                gate = self.receipt_gate
                if gate is None:
                    from .adapters import BridgeHostDeliveryPort
                    gate = BridgeHostDeliveryPort(self).validate_raw_receipt
                if gate(pending, receipt) is not True:
                    raise ValueError('HOST_LATE_RECEIPT_GATE_REJECTED')
                result = NativeOperations(self.book).record(
                    operation_id, receipt['response'])
                output = self._receipt_import_output(
                    request_id=request_id, binding=receipt.get('binding'),
                    packet=packet, action_id=action_id, trade_id=trade_id,
                    result=result)
                result_path = self.directory/'results'/(request_id+'.json')
                if result_path.exists():
                    existing = read_private_json(result_path)
                    if existing != output:
                        raise ValueError('HOST_LATE_RESULT_CONFLICT')
                else:
                    write_private_json(result_path, output)
                value = {'operation_id': operation_id,
                         'action_id': action_id, 'operation': operation,
                         'status': 'HOST_LATE_RECEIPT_IMPORTED',
                         'request_id': request_id, 'native_result': result,
                         'response_sha256': receipt.get('response_sha256'),
                         'result_path': str(result_path)}
                self._late_diagnostics(value)
            except Exception as exc:
                value = {'operation_id': operation_id, 'action_id': action_id,
                         'operation': operation,
                         'status': 'HOST_LATE_RECEIPT_REJECTED',
                         'response_sha256': (receipt.get('response_sha256')
                                             if isinstance(receipt, dict)
                                             else None),
                         'reason': str(exc)}
                self._late_diagnostics(value)
        return list(self.late_receipt_results)

    def _import(self,pending=None,*,detached=False):
        pending=pending or self.pending;packet=pending['packet'];path=self.directory/'receipts'/(packet['operation_id']+'.json')
        if (not detached and packet['operation']=='review'
                and self.book.clock()>=timestamp(pending['expires_at'])):
            return self._abandon_pending_review(pending,'HOST_REVIEW_EXPIRED')
        if not path.exists():
            if detached:return None
            expired=self.book.clock()>=timestamp(pending['expires_at'])
            if expired:self._uncertain('HOST_HANDOFF_EXPIRED')
            return {'status':'HOST_HANDOFF_EXPIRED_RECONCILE' if expired else 'HOST_TOOL_RESULT_PENDING',
                'operation_id':packet['operation_id'],'action_id':packet['action_id'],
                'attention_required':True,'execution_authorized':False,'model_calls':0}
        try:
            receipt=read_private_json(path)
            if (receipt.get('schema_version')!=1 or receipt.get('binding')!=pending['binding']
                    or receipt.get('request_id')!=pending['request_id']
                    or receipt.get('packet_sha256')!=digest(packet)
                    or receipt.get('response_sha256')!=digest(receipt['response'])
                    or receipt.get('execution_authorized') is not False or receipt.get('broker_effect_verified') is not False
                    or not timestamp(pending['prepared_at'])<=timestamp(receipt['received_at'])<=self.book.clock()):
                raise ValueError('HOST_RECEIPT_BINDING_INVALID')
        except (ValueError,KeyError,TypeError,OSError):
            self._uncertain('HOST_RECEIPT_INVALID',pending)
            return {'status':'HOST_RECEIPT_INVALID_RECONCILE','operation_id':packet['operation_id'],
                'attention_required':True,'execution_authorized':False,'model_calls':0}
        if self.receipt_gate is not None:
            try:
                if self.receipt_gate(pending,receipt) is not True:
                    raise ValueError('HOST_RECEIPT_IDENTITY_UNVERIFIED')
            except (ValueError,KeyError,TypeError,OSError) as exc:
                self._uncertain('HOST_RECEIPT_INVALID',pending)
                reason=str(exc) if re.fullmatch('[A-Z_]{1,100}',str(exc)) else 'HOST_RECEIPT_IDENTITY_UNVERIFIED'
                return {'status':'HOST_RECEIPT_IDENTITY_UNVERIFIED',
                    'reason':reason,'operation_id':packet['operation_id'],
                    'action_id':packet['action_id'],'attention_required':True,
                    'execution_authorized':False,'model_calls':0}
        trade_id=None
        try:
            request=read_private_json(self.directory/'requests'/(pending['request_id']+'.json'))
            trade_id=request.get('trade_id')
        except (OSError,ValueError):
            pass
        try:
            result=NativeOperations(self.book).record(packet['operation_id'],receipt['response'])
            output=self._receipt_import_output(
                request_id=pending['request_id'],binding=pending['binding'],
                packet=packet,action_id=packet['action_id'],trade_id=trade_id,
                result=result)
            result_folder='late-read-results' if result.get('late_receipt_retained',False) else 'results'
            write_private_json(self.directory/result_folder/(pending['request_id']+'.json'),output)
        except Exception:
            return {'status':'HOST_RECEIPT_IMPORT_REQUIRES_RECOVERY','operation_id':packet['operation_id'],
                'attention_required':True,'execution_authorized':False,'model_calls':0}
        pending['lease'].close()
        if not detached:self.pending=None
        return output

    def _abandon_pending_review(self,pending,reason):
        packet=pending['packet']
        result=NativeOperations(self.book).abandon_read(packet['operation_id'],reason=reason)
        output={'schema_version':1,'status':'HOST_REVIEW_ABANDONED','request_id':pending['request_id'],
            'binding':pending['binding'],'operation_id':packet['operation_id'],'action_id':packet['action_id'],
            'native_result':result,'execution_authorized':False,'broker_effect_verified':False,
            'attention_required':True,'model_calls':0}
        try:write_private_json(self.directory/'results'/(pending['request_id']+'.json'),output)
        finally:
            pending['lease'].close();self.abandoned_reads[packet['operation_id']]=pending
            if self.pending is pending:self.pending=None
        return output

    def _retire_observed(self,snapshot):
        """A genuine broker effect releases handoff I/O, not its receipt history."""
        if self.pending is None or snapshot is None:return False
        pending=self.pending;packet=pending['packet']
        if packet['operation'] not in {'place','cancel'}:return False
        self.book._snapshot_check(snapshot,self.book.clock())
        action=next(a for a in self.book.actions() if a['id']==packet['action_id'])
        if action['state']!='OBSERVED':return False
        payload=json.loads(action['payload'])
        matches=[o for o in snapshot['orders'].values() if self.book._order_matches(action,o)]
        if len(matches)!=1:return False
        order=matches[0]
        if packet['operation']=='cancel':
            from .broker import TERMINAL
            if order['id']!=payload['target_order_id'] or order['state'] not in TERMINAL:return False
        else:self.book._check_order_terms(order,payload)
        marker={'schema_version':1,'operation_id':packet['operation_id'],'request_id':pending['request_id'],
            'binding':pending['binding'],'packet_sha256':digest(packet),'broker_order_id':order['id'],
            'broker_snapshot_sha256':digest(snapshot),'observed_at':snapshot['observed_at'],
            'receipt_pending':True,'order_replay_allowed':False}
        self.book._begin()
        try:
            identity=packet['operation_id']+'-host-broker-observed'
            prior=self.book.db.execute('SELECT payload FROM audit_events WHERE event_id=?',(identity,)).fetchone()
            if prior:
                stored=json.loads(prior[0])['details']
                original={key:stored[key] for key in marker}
                for key in ('operation_id','request_id','binding','packet_sha256','broker_order_id'):
                    if original[key]!=marker[key]:raise ValueError('HOST_OBSERVED_MARKER_BINDING_CONFLICT')
                marker=original
            else:
                self.book.audit.append_in_transaction({'event_id':identity,'occurred_at':self.book.clock().isoformat(),
                    'event_type':'RECONCILIATION_ISSUE','actor':'script','synthetic':self.book.simulation,
                    'trade_id':action['trade_id'],'broker_order_id':order['id'],
                    'details':{'status':'HOST_EFFECT_OBSERVED_RECEIPT_PENDING',**marker}})
            self.book.db.commit()
        except BaseException:self.book.db.rollback();raise
        write_private_json(self.directory/'observed'/(packet['operation_id']+'.json'),marker)
        pending['lease'].close();self.retired[packet['operation_id']]=pending;self.pending=None
        return True

    def _compose_intent_result(self,request,action_id,signal_id,action,*,historical=None):
        prior=historical if isinstance(historical,dict) else {}
        if action is None:
            state='UNKNOWN';status='HOST_ENGINEERING_INTENT_ACTION_MISSING'
            reviewable=claimable=placeable=False;attention=True
        else:
            state=action['state']
            if state=='REVIEW_PENDING':
                status='HOST_ENGINEERING_INTENT_RECORDED'
                reviewable=True;attention=False
            elif state=='WAITING_USER':
                status='HOST_ENGINEERING_INTENT_REVIEWED'
                reviewable=False;attention=False
            else:
                status='HOST_ENGINEERING_INTENT_ALREADY_PROGRESSED'
                reviewable=False;attention=True
            claimable=placeable=False
        historical_state=prior.get('state') or prior.get('historical_state') or 'REVIEW_PENDING'
        return {'schema_version':1,'status':status,'request_id':request['request_id'],
            'signal_id':signal_id,'action_id':action_id,'state':state,
            'current_state':state,'historical_state':historical_state,
            'recovered':True,'historical_local_intent':True,
            'historical_signal_id':prior.get('signal_id',signal_id),
            'reviewable':reviewable,'claimable':claimable,'placeable':placeable,
            'entry_authorized':False,'execution_authorized':False,
            'broker_effect_verified':False,'binding':dict(self.binding),
            'result':{'recovered':True,'automatic_replay_allowed':False},
            'model_calls':0,'attention_required':attention,
            'automatic_replay_allowed':False}

    def _compose_promotion_result(self,request,action_id,permit_id,action,fact,*,historical=None):
        """Compose current action state with an auditable historical promotion fact."""
        prior=historical if isinstance(historical,dict) else {}
        if action is None:
            state='UNKNOWN'
            status='HOST_ENGINEERING_PROMOTION_ACTION_MISSING'
            reviewable=claimable=placeable=False
            attention=True
        else:
            state=action['state']
            reviewable=state=='READY'
            claimable=state=='READY'
            placeable=False
            attention=state!='READY'
            status=('HOST_ENGINEERING_PROMOTED' if state=='READY'
                    else 'HOST_ENGINEERING_PROMOTION_ALREADY_COMPLETED')
        historical_state=prior.get('state') or prior.get('historical_state') or 'READY'
        return {'schema_version':1,'status':status,
            'request_id':request['request_id'],'action_id':action_id,
            'permit_id':permit_id,'state':state,'current_state':state,
            'historical_state':historical_state,
            'recovered':True,'historical_local_promotion':True,
            'historical_permit_id':prior.get('historical_permit_id',permit_id),
            'historical_binding':prior.get('historical_binding',fact.get('binding')),
            'historical_consumed_at':prior.get('historical_consumed_at',fact.get('consumed_at')),
            'reviewable':reviewable,'claimable':claimable,
            'placeable':placeable,'entry_authorized':False,
            'execution_authorized':False,'broker_effect_verified':False,
            'binding':dict(self.binding),
            'result':{'recovered':True,'automatic_replay_allowed':False},
            'model_calls':0,'attention_required':attention,
            'automatic_replay_allowed':False}

    def _recover_engineering_promotion(self,request,action_id,permit_id,now):
        """Recover an already-committed core promotion without consuming again.

        Only exact durable consumption + promotion audit + current action facts
        are accepted.  This is a historical local fact, never a new entry
        permit, broker fill, or protection claim.
        """
        action=next((a for a in self.book.actions() if a['id']==action_id),None)
        account_key=self.book.state().get('account_key')
        policy_sha256=digest(self.book.policy)
        if (account_key!=self.binding['account_key']
                or policy_sha256!=self.binding['policy_sha256']):
            return None
        matches=[]
        try:
            rows=self.book.db.execute(
                "SELECT payload FROM audit_events WHERE event_id LIKE ?",
                ('supervised-permit-%-consumed',)).fetchall()
        except Exception:return None
        for (raw,) in rows:
            try:event=json.loads(raw);details=event.get('details') or {}
            except (TypeError,ValueError):continue
            if (details.get('status')!='SUPERVISED_ENGINEERING_PERMIT_CONSUMED'
                    or details.get('permit_id')!=permit_id
                    or details.get('action_id')!=action_id):
                continue
            if (details.get('account_key')!=account_key
                    or details.get('policy_sha256')!=policy_sha256):
                continue
            one_shot=details.get('one_shot_id');binding=details.get('binding')
            if (not isinstance(one_shot,str) or not one_shot
                    or not isinstance(binding,str)):
                continue
            try:
                consumed=self.book.db.execute(
                    'SELECT source_sha256,binding,action_id,consumed_at '
                    'FROM formal_authorization_consumptions WHERE one_shot_id=?',
                    (one_shot,)).fetchone()
                promoted=self.book.db.execute(
                    'SELECT payload FROM audit_events WHERE event_id=?',
                    ('supervised-intent-'+action_id+'-promoted',)).fetchone()
            except Exception:return None
            if (not consumed or consumed[1]!=binding
                    or consumed[2]!=action_id or not promoted):
                continue
            try:promotion=json.loads(promoted[0]);pd=promotion.get('details') or {}
            except (TypeError,ValueError):continue
            if (pd.get('status')!='SUPERVISED_ENGINEERING_INTENT_PROMOTED'
                    or pd.get('permit_id')!=permit_id
                    or pd.get('binding')!=binding
                    or pd.get('action_id')!=action_id
                    or pd.get('account_key')!=account_key
                    or pd.get('policy_sha256')!=policy_sha256):
                continue
            matches.append({'one_shot_id':one_shot,
                'binding':binding,'consumed_at':consumed[3],
                'source_sha256':consumed[0]})
        if len(matches)>1:
            raise ValueError('HOST_ENGINEERING_PROMOTION_EVIDENCE_CONFLICT')
        if not matches:return None
        fact=matches[0]
        output=self._compose_promotion_result(
            request,action_id,permit_id,action,fact)
        identity='host-engineering-promote-'+request['request_id']
        previous=self.book.db.execute(
            'SELECT payload FROM audit_events WHERE event_id=?',(identity,)).fetchone()
        if previous:
            stored=json.loads(previous[0])['details']['host_result']
            output=self._compose_promotion_result(
                request,action_id,permit_id,action,fact,historical=stored)
            path=self.directory/'results'/(request['request_id']+'.json')
            if not path.exists():
                write_private_json(path,output)
            return output
        self.book.audit.append({'event_id':identity,'occurred_at':now.isoformat(),
            'event_type':'RECONCILIATION_ISSUE','actor':'script',
            'synthetic':self.book.simulation,'trade_id':action['trade_id'],
            'details':{'status':output['status'],
                'host_request_sha256':digest({'binding':request['binding'],
                    'action_id':action_id,'permit_id':permit_id}),
                'host_result':output,'recovered_from_durable_fact':True}})
        write_private_json(self.directory/'results'/(request['request_id']+'.json'),output)
        return output

    def run_engineering_source_dry_assess(
            self,action_id,*,enabled=False,review_only=False,
            provenance_reader=None,risk_lineage_reader=None,
            native_receipt_root=None,native_ingress_root=None):
        """Explicit, default-off owner-bound engineering dry diagnostic.

        It only configures the existing owner source/readers and invokes the
        existing dry assessor.  It never promotes, consumes, claims or places;
        real activation remains disabled and no entry policy is enabled.
        """
        self._owner()
        if enabled is not True:
            raise ValueError('ENGINEERING_DRY_DIAGNOSTIC_DISABLED')
        if review_only is not True:
            raise ValueError('ENGINEERING_DRY_REVIEW_ONLY_REQUIRED')
        if self.engineering_owner_root is None:
            raise ValueError('ENGINEERING_OWNER_ROOT_REQUIRED')
        if self.book.simulation is not False:
            raise ValueError('ENGINEERING_DRY_REAL_BOOK_REQUIRED')
        self.book.configure_engineering_source(
            self.engineering_owner_root,
            provenance_reader=(provenance_reader
                               if provenance_reader is not None
                               else self.engineering_dry_provenance_reader),
            risk_lineage_reader=(risk_lineage_reader
                                 if risk_lineage_reader is not None
                                 else self.engineering_dry_risk_reader),
            owner_source_root=self.engineering_owner_root,
            native_receipt_root=native_receipt_root,
            native_ingress_root=native_ingress_root,
            allow_synthetic=False,real_activation_enabled=False)
        return self.book.dry_assess_supervised_engineering(action_id)

    def _owner_request_snapshot(self,signal_id):
        """Return the exact owner request accepted for this intent, if configured."""
        if self.engineering_owner_root is None:
            return None
        from .host_mailbox import read_private_json
        from .owner_engineering_source import REQUEST_FILE
        try:
            request=read_private_json(self.engineering_owner_root/REQUEST_FILE)
        except FileNotFoundError:
            raise ValueError('OWNER_RECEIPT_REQUEST_MISSING') from None
        if request.get('signal_id')!=signal_id:
            raise ValueError('OWNER_RECEIPT_REQUEST_BINDING')
        return request

    def _build_engineering_receipt(self,request,owner_request,action,
                                   request_hash,signal_id,identity,now):
        if self.engineering_owner_root is None:
            return None
        from .owner_engineering_source import EngineeringOwnerSource
        source=EngineeringOwnerSource(self.engineering_owner_root,
                                      clock=self.book.clock)
        return source.build_receipt(
            owner_request,host_request_id=request['request_id'],
            host_request_sha256=request_hash,signal_id=signal_id,
            action=action,host_binding=dict(self.binding),
            audit_event_id=identity,received_at=now)

    def _write_engineering_receipt(self,receipt):
        if receipt is None or self.engineering_owner_root is None:
            return None
        from .owner_engineering_source import EngineeringOwnerSource
        return EngineeringOwnerSource(
            self.engineering_owner_root,
            clock=self.book.clock).write_receipt(receipt)

    def _prepare(self,request,snapshot,quotes,allow_entries):
        if request.get('schema_version')!=1 or request.get('binding')!=self.binding:
            raise ValueError('HOST_REQUEST_BINDING_INVALID')
        if request.get('operation') not in {'review','place','cancel','release-inhibit','engineering-exit','engineering-propose','engineering-intent','engineering-promote','engineering-dry-assess'}:raise ValueError('HOST_OPERATION_INVALID')
        now=self.book.clock()
        if not 0<=(now-timestamp(request['requested_at'])).total_seconds()<=30:raise ValueError('HOST_REQUEST_EXPIRED')
        if getattr(self,'current_context_refresher',None) is not None:
            self.current_context_status=self.current_context_refresher.refresh()
        if request['operation']=='engineering-dry-assess':
            if self.engineering_dry_enabled is not True:
                raise ValueError('HOST_ENGINEERING_DRY_DISABLED')
            action_id=request.get('action_id')
            action=next((item for item in self.book.actions()
                         if item['id']==action_id),None)
            if action is None or action.get('kind')!='entry':
                raise ValueError('HOST_ENGINEERING_DRY_ACTION_REQUIRED')
            payload=json.loads(action['payload'])
            if payload.get('purpose')!='engineering':
                raise ValueError('HOST_ENGINEERING_DRY_PURPOSE_REQUIRED')
            decision=self.run_engineering_source_dry_assess(
                action_id,enabled=True,review_only=True,
                provenance_reader=self.engineering_dry_provenance_reader,
                risk_lineage_reader=self.engineering_dry_risk_reader)
            output={'schema_version':1,'status':'HOST_ENGINEERING_DRY_ASSESSED',
                'request_id':request['request_id'],'action_id':action_id,
                'decision':decision,'diagnostic_only':True,
                'entry_authorized':False,'execution_authorized':False,
                'broker_effect_verified':False,'model_calls':0,
                'attention_required':decision.get('allowed') is not True}
            write_private_json(self.directory/'results'/(request['request_id']+'.json'),output)
            return output
        if request['operation']=='engineering-propose':
            if not allow_entries:raise ValueError('HOST_NEW_ENTRIES_BLOCKED')
            if request.get('action_id')!='operator-engineering-propose':raise ValueError('HOST_ENGINEERING_PROPOSAL_INVALID')
            proposal=request.get('proposal')
            if not isinstance(proposal,dict) or set(proposal)!={'signal_id','plan','evidence'}:
                raise ValueError('HOST_ENGINEERING_PROPOSAL_INVALID')
            signal_id=proposal['signal_id'];plan=proposal['plan'];evidence=proposal['evidence']
            if (not isinstance(signal_id,str) or not signal_id.startswith('engineering-') or len(signal_id)>256
                    or not isinstance(plan,dict) or not isinstance(evidence,dict)):
                raise ValueError('HOST_ENGINEERING_PROPOSAL_INVALID')
            if snapshot.get('synthetic') is not self.book.simulation:
                raise ValueError('HOST_SNAPSHOT_PROVENANCE_MISMATCH')
            from .calendar import session
            market=session(now.isoformat())
            try:deadline=timestamp(evidence['parameters_valid_until'])
            except (KeyError,ValueError,TypeError):raise ValueError('HOST_ENGINEERING_PARAMETER_DEADLINE_REQUIRED') from None
            if (market is None or market['date']!=self.book.policy['session_date']
                    or not now<deadline<=market['close']):
                raise ValueError('HOST_ENGINEERING_PARAMETER_DEADLINE_INVALID')
            quote=quotes.get(plan.get('symbol'))
            if quote is None:raise ValueError('HOST_ENGINEERING_FRESH_QUOTE_REQUIRED')
            identity='host-engineering-propose-'+request['request_id']
            request_hash=digest({'binding':request['binding'],'proposal':proposal})
            previous=self.book.db.execute('SELECT payload FROM audit_events WHERE event_id=?',(identity,)).fetchone()
            if previous:
                details=json.loads(previous[0])['details']
                if details.get('host_request_sha256')!=request_hash:raise ValueError('HOST_ENGINEERING_REQUEST_CONFLICT')
                output=details['host_result']
            else:
                result=self.book.propose(signal_id,plan,quote,snapshot,evidence=evidence,purpose='engineering')
                # If the core committed but publishing the host result failed,
                # exact core signal idempotency recovers its original action.
                entries=[a for a in self.book.actions() if a['trade_id']==signal_id and a['kind']=='entry']
                if len(entries)!=1:raise ValueError('HOST_ENGINEERING_ACTION_IDENTITY_INVALID')
                output={'schema_version':1,'status':'HOST_ENGINEERING_ENTRY_PROPOSED',
                    'request_id':request['request_id'],'signal_id':signal_id,'action_id':entries[0]['id'],
                    'binding':dict(self.binding),'result':result,'execution_authorized':False,
                    'broker_effect_verified':False,'model_calls':0,'attention_required':False}
                self.book.audit.append({'event_id':identity,'occurred_at':now.isoformat(),
                    'event_type':'RECONCILIATION_ISSUE','actor':'script','synthetic':self.book.simulation,
                    'trade_id':signal_id,'details':{'status':'HOST_ENGINEERING_ENTRY_QUEUED',
                        'host_request_sha256':request_hash,'host_result':output}})
            write_private_json(self.directory/'results'/(request['request_id']+'.json'),output)
            return output
        if request['operation']=='engineering-intent':
            if request.get('action_id')!='operator-engineering-intent':
                raise ValueError('HOST_ENGINEERING_INTENT_INVALID')
            proposal=request.get('proposal')
            if not isinstance(proposal,dict) or set(proposal)!={'signal_id','plan','evidence'}:
                raise ValueError('HOST_ENGINEERING_INTENT_INVALID')
            signal_id=proposal['signal_id'];plan=proposal['plan'];evidence=proposal['evidence']
            if (not isinstance(signal_id,str) or not signal_id.startswith('engineering-') or len(signal_id)>256
                    or not isinstance(plan,dict) or not isinstance(evidence,dict)):
                raise ValueError('HOST_ENGINEERING_INTENT_INVALID')
            if snapshot.get('synthetic') is not self.book.simulation:
                raise ValueError('HOST_SNAPSHOT_PROVENANCE_MISMATCH')
            if self.book.supervised_engineering_authority is None:
                raise ValueError('SUPERVISED_ENGINEERING_AUTHORITY_UNAVAILABLE')
            quote=quotes.get(plan.get('symbol'))
            if quote is None:raise ValueError('HOST_ENGINEERING_FRESH_QUOTE_REQUIRED')
            identity='host-engineering-intent-'+request['request_id']
            request_hash=digest({'binding':request['binding'],'proposal':proposal})
            previous=self.book.db.execute('SELECT payload FROM audit_events WHERE event_id=?',(identity,)).fetchone()
            if previous:
                details=json.loads(previous[0])['details']
                if details.get('host_request_sha256')!=request_hash:
                    raise ValueError('HOST_ENGINEERING_REQUEST_CONFLICT')
                stored=details['host_result']
                stored_signal=stored.get('signal_id')
                if stored_signal not in {None,signal_id}:
                    raise ValueError('HOST_ENGINEERING_REQUEST_CONFLICT')
                stored_action=stored.get('action_id')
                current=next((a for a in self.book.actions()
                              if a['id']==stored_action),None)
                if current is not None and current.get('trade_id')!=signal_id:
                    raise ValueError('HOST_ENGINEERING_REQUEST_CONFLICT')
                output=self._compose_intent_result(
                    request,stored_action,signal_id,current,historical=stored)
                if self.engineering_owner_root is not None:
                    original=details.get('owner_receipt')
                    if not isinstance(original,dict):
                        raise ValueError('OWNER_RECEIPT_AUDIT_LEGACY_UNVERIFIABLE')
                    if (original.get('host_request_id')!=request['request_id']
                            or original.get('host_request_sha256')!=request_hash):
                        raise ValueError('OWNER_RECEIPT_AUDIT_BINDING')
                    self._write_engineering_receipt(original)
                path=self.directory/'results'/(request['request_id']+'.json')
                if not path.exists():write_private_json(path,output)
                return output
            else:
                owner_request=self._owner_request_snapshot(signal_id)
                if owner_request is not None and self.engineering_owner_root is not None:
                    from .owner_engineering_source import EngineeringOwnerSource
                    prior=EngineeringOwnerSource(
                        self.engineering_owner_root,
                        clock=self.book.clock).existing_receipt(owner_request)
                    if prior is not None:
                        raise ValueError('OWNER_RECEIPT_ALREADY_RECORDED')
                result=self.book.record_supervised_engineering_intent(
                    signal_id,plan,quote,snapshot,evidence=evidence)
                if owner_request is not None:
                    if (result.get('request_sha256')!=digest(owner_request)
                            or result.get('request_id')!=
                            owner_request.get('request_id')):
                        raise ValueError(
                            'OWNER_RECEIPT_CORE_REQUEST_MISMATCH')
                action=next((a for a in self.book.actions()
                             if a['id']==result['action_id']),None)
                if action is None or action['state']!='REVIEW_PENDING':
                    raise ValueError('HOST_ENGINEERING_INTENT_IDENTITY_INVALID')
                receipt=self._build_engineering_receipt(
                    request,owner_request,action,request_hash,signal_id,identity,now)
                output={'schema_version':1,'status':'HOST_ENGINEERING_INTENT_RECORDED',
                    'request_id':request['request_id'],'signal_id':signal_id,
                    'action_id':action['id'],'state':'REVIEW_PENDING',
                    'reviewable':True,'claimable':False,'placeable':False,
                    'entry_authorized':False,'execution_authorized':False,
                    'broker_effect_verified':False,'binding':dict(self.binding),
                    'result':result,'model_calls':0,'attention_required':False}
                if receipt is not None:
                    output['owner_request_sha256']=receipt['request_sha256']
                details={'status':'HOST_ENGINEERING_INTENT_RECORDED',
                    'host_request_sha256':request_hash,'host_result':output}
                if receipt is not None:
                    details['owner_receipt']=receipt
                self.book.audit.append({'event_id':identity,'occurred_at':now.isoformat(),
                    'event_type':'RECONCILIATION_ISSUE','actor':'script',
                    'synthetic':self.book.simulation,'trade_id':signal_id,
                    'details':details})
                self._write_engineering_receipt(receipt)
            write_private_json(self.directory/'results'/(request['request_id']+'.json'),output)
            return output
        if request['operation']=='engineering-promote':
            if not allow_entries:raise ValueError('HOST_NEW_ENTRIES_BLOCKED')
            action_id=request.get('action_id');permit_id=request.get('permit_id')
            if (not isinstance(action_id,str) or not action_id
                    or not isinstance(permit_id,str)
                    or not re.fullmatch('[0-9a-f]{64}',permit_id)):
                raise ValueError('HOST_ENGINEERING_PROMOTION_INVALID')
            if self.book.supervised_engineering_authority is None:
                raise ValueError('SUPERVISED_ENGINEERING_AUTHORITY_UNAVAILABLE')
            identity='host-engineering-promote-'+request['request_id']
            request_hash=digest({'binding':request['binding'],'action_id':action_id,
                                 'permit_id':permit_id})
            previous=self.book.db.execute('SELECT payload FROM audit_events WHERE event_id=?',(identity,)).fetchone()
            if previous is None:
                recovered=self._recover_engineering_promotion(
                    request,action_id,permit_id,now)
                if recovered is not None:return recovered
            if previous:
                details=json.loads(previous[0])['details']
                if details.get('host_request_sha256')!=request_hash:
                    raise ValueError('HOST_ENGINEERING_REQUEST_CONFLICT')
                stored=details['host_result']
                current=next((a for a in self.book.actions()
                              if a['id']==action_id),None)
                output=self._compose_promotion_result(
                    request,action_id,permit_id,current,
                    {'binding':stored.get('historical_binding'),
                     'consumed_at':stored.get('historical_consumed_at')},
                    historical=stored)
                path=self.directory/'results'/(request['request_id']+'.json')
                if not path.exists():write_private_json(path,output)
                return output
            else:
                result=self.book.promote_supervised_entry(
                    action_id,expected_permit_id=permit_id)
                action=next((a for a in self.book.actions() if a['id']==action_id),None)
                if action is None or action['state']!='READY':
                    raise ValueError('HOST_ENGINEERING_PROMOTION_IDENTITY_INVALID')
                output={'schema_version':1,'status':'HOST_ENGINEERING_PROMOTED',
                    'request_id':request['request_id'],'action_id':action_id,
                    'permit_id':permit_id,'state':'READY','reviewable':True,
                    'claimable':True,'placeable':False,'entry_authorized':False,
                    'execution_authorized':False,'broker_effect_verified':False,
                    'binding':dict(self.binding),'result':result,
                    'model_calls':0,'attention_required':False}
                self.book.audit.append({'event_id':identity,'occurred_at':now.isoformat(),
                    'event_type':'RECONCILIATION_ISSUE','actor':'script',
                    'synthetic':self.book.simulation,'trade_id':action['trade_id'],
                    'details':{'status':'HOST_ENGINEERING_PROMOTED',
                        'host_request_sha256':request_hash,'host_result':output}})
            write_private_json(self.directory/'results'/(request['request_id']+'.json'),output)
            return output
        if request['operation']=='engineering-exit':
            trade_id=request.get('trade_id')
            if (request.get('action_id')!='operator-engineering-exit' or not isinstance(trade_id,str)
                    or not trade_id.startswith('engineering-') or len(trade_id)>256):
                raise ValueError('HOST_ENGINEERING_TRADE_ID_REQUIRED')
            identity='host-engineering-exit-'+request['request_id']
            previous=self.book.db.execute('SELECT payload FROM audit_events WHERE event_id=?',(identity,)).fetchone()
            if previous:output=json.loads(previous[0])['details']['host_result']
            else:
                result=self.book.request_engineering_exit(trade_id,snapshot)
                output={'schema_version':1,'status':'HOST_ENGINEERING_EXIT_REQUESTED',
                    'request_id':request['request_id'],'trade_id':trade_id,'binding':dict(self.binding),
                    'result':result,'execution_authorized':False,'broker_effect_verified':False,
                    'model_calls':0,'attention_required':False}
                self.book.audit.append({'event_id':identity,'occurred_at':now.isoformat(),
                    'event_type':'RECONCILIATION_ISSUE','actor':'script','synthetic':self.book.simulation,
                    'trade_id':trade_id,'details':{'status':'HOST_ENGINEERING_EXIT_QUEUED','host_result':output}})
            write_private_json(self.directory/'results'/(request['request_id']+'.json'),output)
            return output
        if request['operation']=='release-inhibit':
            if (request.get('action_id')!='operator-release-inhibit' or not isinstance(request.get('reason'),str)
                    or not request['reason'].strip() or len(request['reason'])>1000):
                raise ValueError('HOST_GATE_RELEASE_REASON_REQUIRED')
            from .watchdog import release_entry_inhibit
            identity='host-gate-release-'+request['request_id']
            previous=self.book.db.execute('SELECT payload FROM audit_events WHERE event_id=?',(identity,)).fetchone()
            if previous:output=json.loads(previous[0])['details']['host_result']
            else:
                result=release_entry_inhibit(self.book,snapshot,reason=request['reason'])
                output={'schema_version':1,'status':'HOST_GATE_RELEASED','request_id':request['request_id'],
                    'binding':dict(self.binding),'result':result,'execution_authorized':False,
                    'broker_effect_verified':False,'model_calls':0,'attention_required':False}
                self.book.audit.append({'event_id':identity,'occurred_at':now.isoformat(),
                    'event_type':'RECONCILIATION_ISSUE','actor':'script','synthetic':self.book.simulation,
                    'details':{'status':'HOST_GATE_RELEASE_COMPLETED','host_result':output}})
            write_private_json(self.directory/'results'/(request['request_id']+'.json'),output)
            return output
        action=next((a for a in self.book.actions() if a['id']==request.get('action_id')),None)
        if action is None:raise ValueError('HOST_ACTION_NOT_FOUND')
        if (action['kind']=='entry' and action['state']!='REVIEW_PENDING'
                and not allow_entries):raise ValueError('HOST_NEW_ENTRIES_BLOCKED')
        if action['kind']=='entry' and action['state']!='REVIEW_PENDING':
            trade=(self.book.state().get('trades') or {}).get(
                action.get('trade_id'), {})
            purpose=trade.get('purpose') if isinstance(trade,dict) else None
            if purpose not in {'engineering','strategy'}:
                raise ValueError('HOST_ENTRY_PURPOSE_INVALID')
            self.book.entry_authority.require_entry(
                source='HostExecutionBridge', snapshot=snapshot, action=action,
                purpose=purpose)
            preflight=self.book.check_current_entry_action(action)
            if preflight.get('allowed') is not True:
                raise ValueError('CURRENT_ENGINEERING_PREFLIGHT_BLOCKED:'+
                    ','.join(preflight.get('reasons') or ['UNKNOWN']))
        if snapshot.get('synthetic') is not self.book.simulation:raise ValueError('HOST_SNAPSHOT_PROVENANCE_MISMATCH')
        payload=json.loads(action['payload']);quote=quotes.get(payload['plan']['symbol'])
        limits=[timestamp(payload['expires_at']),now+timedelta(seconds=15),timestamp(snapshot['observed_at'])+timedelta(seconds=30)]
        protection_deadline=self.review.protection_deadline(action)
        if protection_deadline:limits.append(protection_deadline)
        if request['operation']=='place':
            row=self.book.db.execute("SELECT created_at,state FROM native_operation_records WHERE action_id=? AND operation='review' ORDER BY rowid DESC LIMIT 1",(action['id'],)).fetchone()
            if not row or row[1]!='RECORDED':raise ValueError('FRESH_NATIVE_REVIEW_REQUIRED')
            reviewed_at=timestamp(row[0])
            # A recorded preview can still be expired. Classify it before the
            # shared account/quote envelope check so the Host refreshes the
            # preview instead of retrying unchanged market data indefinitely.
            # This does not extend its original deadline or reserve a write.
            if not 0<=(now-reviewed_at).total_seconds()<30:
                raise ValueError('FRESH_NATIVE_REVIEW_REQUIRED')
            limits.append(reviewed_at+timedelta(seconds=30))
        if quote:limits.extend(timestamp(quote[key])+timedelta(seconds=15) for key in ('bid_at','ask_at','observed_at'))
        expires=min(limits)
        if expires<=now:raise ValueError('HOST_FRESH_INPUT_REQUIRED')
        lease=worker_lease(self.binding['db'])
        if lease is None:raise ValueError('WORKER_OWNER_STILL_ACTIVE')
        packet=None;published=False;publication_attempted=False
        try:
            ops=NativeOperations(self.book)
            packet=ops.begin(action['id'],request['operation'],snapshot,quote=quote)
            prepared_at=self.book.clock()
            created_row=self.book.db.execute(
                'SELECT created_at FROM native_operation_records WHERE id=?',
                (packet['operation_id'],)).fetchone()
            if (created_row is None
                    or prepared_at<timestamp(created_row[0])):
                raise ValueError('HOST_PREPARED_CLOCK_INVALID')
            if prepared_at>=expires:
                raise ValueError('HOST_FRESH_INPUT_REQUIRED')
            envelope={'schema_version':1,'status':'PREPARED','request_id':request['request_id'],
                'binding':dict(self.binding),'packet':packet,'packet_sha256':digest(packet),
                'prepared_at':prepared_at.isoformat(),'expires_at':expires.isoformat(),
                'execution_authorized':False,'broker_effect_verified':False}
            publication_attempted=True
            write_private_json(self.directory/'packets'/(request['request_id']+'.json'),envelope)
            published=True
            self.pending={**envelope,'lease':lease}
            return {'status':'HOST_PACKET_PREPARED','request_id':request['request_id'],
                'operation_id':packet['operation_id'],'action_id':packet['action_id'],
                'attention_required':True,'execution_authorized':False,'model_calls':0}
        except BaseException as exc:
            if (packet is not None and publication_attempted
                    and not published):
                # Publication may have partially completed. Keep the exact
                # local packet and lease as an explicit recoverable pending
                # state; never republish it as if nothing was written.
                self.pending={**envelope,'lease':lease}
                if packet.get('operation') in {'place','cancel'}:
                    self._uncertain('HOST_PACKET_PUBLICATION_FAILED')
                raise
            errors=self._settle_after_begin_failure(
                packet,lease,published=published,
                expired=(str(exc)=='HOST_FRESH_INPUT_REQUIRED'))
            if errors:
                raise ValueError('HOST_PREPARATION_CLEANUP_FAILED:'+errors) from exc
            raise

    def _settle_after_begin_failure(self,packet,lease,*,published,expired):
        """Deterministically settle a reservation after begin succeeded."""
        errors=[]
        try:lease.close()
        except Exception as exc:errors.append('lease:'+type(exc).__name__)
        if packet is not None and not published:
            ops=NativeOperations(self.book)
            operation_id=packet.get('operation_id')
            try:
                if packet.get('operation')=='review':
                    ops.abandon_read(operation_id,
                        reason=('HOST_REVIEW_EXPIRED' if expired
                                else 'EXPLICIT_READ_RECOVERY'))
                else:
                    ops.mark_uncertain(operation_id,
                        'HOST_PACKET_PUBLICATION_FAILED')
            except Exception as exc:
                errors.append('reservation:'
                              +str(getattr(exc,'code',None) or type(exc).__name__))
        return ';'.join(errors)

    def __call__(self,*,snapshot=None,quotes=None,allow_entries=False,allow_dispatch=True,
                 allow_autonomous_review=True,target_request_id=None):
        self._owner();output=None
        local_worker_results=self._consume_local_worker_results()
        self._recover_late_receipts()
        imported=[];retired_failures=[]
        for identity,pending in list(self.abandoned_reads.items()):
            result=self._import(pending,detached=True)
            if result and result['status']=='HOST_RECEIPT_IMPORTED':
                imported.append(identity);del self.abandoned_reads[identity]
        for identity,pending in list(self.retired.items()):
            result=self._import(pending,detached=True)
            if result and result['status']=='HOST_RECEIPT_IMPORTED':
                imported.append(identity);del self.retired[identity]
            elif result:retired_failures.append({'operation_id':identity,'status':result['status']})
        if self.pending:output=self._import()
        if self.pending and snapshot is not None and self._retire_observed(snapshot):output=None
        if output is None and self.pending is None and not self.stopped and allow_dispatch and snapshot is not None and self.task.thread is None:
            if target_request_id is not None and (not isinstance(target_request_id,str)
                    or not target_request_id):
                raise ValueError('HOST_TARGET_REQUEST_ID_INVALID')
            folder=self.directory/'requests'
            # Completed requests remain intact for audit/late-receipt recovery.
            # Only unprocessed work consumes queue capacity; historical files
            # must not eventually stop protection/exit handoffs.
            files=sorted(path for path in folder.glob('*.json')
                if (target_request_id is None or path.stem == target_request_id)
                and not (self.directory/'results'/path.name).exists()
                and not (self.directory/'packets'/path.name).exists()) if folder.exists() else []
            if len(files)>1000:output={'status':'HOST_MAILBOX_LIMIT_REQUIRES_ARCHIVE','attention_required':True}
            if output is None:
                kinds={a['id']:a['kind'] for a in self.book.actions()}
                def priority(path):
                    try:
                        request=read_private_json(path)
                        rank=(2 if request.get('operation')=='engineering-exit' else
                            3 if request.get('operation') in {'engineering-propose','engineering-promote'} else
                            4 if request.get('operation')=='engineering-intent' else
                            {'protect':0,'cancel':1,'exit':2,'entry':3}.get(kinds.get(request.get('action_id')),9))
                        return rank,path.name
                    except (ValueError,KeyError,TypeError,OSError):return 9,path.name
                files.sort(key=priority)
            for path in files[:1000] if output is None else []:
                if (self.directory/'results'/path.name).exists() or (self.directory/'packets'/path.name).exists():continue
                if not re.fullmatch('[0-9a-f-]{36}',path.stem):
                    output={'status':'HOST_MAILBOX_INVALID_FILENAME','attention_required':True};break
                try:
                    request=read_private_json(path)
                    if request.get('request_id')!=path.stem:raise ValueError('HOST_REQUEST_ID_MISMATCH')
                    output=self._prepare(request,snapshot,quotes or {},allow_entries)
                    if (allow_autonomous_review
                            and self._engineering_review_requested(request,output)):
                        output=self._run_engineering_review(request,output)
                except Exception as exc:
                    code=str(exc) if re.fullmatch('[A-Z_]{1,100}',str(exc)) else 'HOST_REQUEST_REJECTED'
                    output={'schema_version':1,'status':'HOST_REQUEST_REJECTED','request_id':path.stem,
                        'reason':code,'attention_required':True,'execution_authorized':False,'model_calls':0}
                    if self.pending is None:write_private_json(self.directory/'results'/path.name,output)
                break
        if output is None and allow_dispatch:
            permit_output=self._auto_issue_engineering_permit()
            if permit_output is not None:
                output=permit_output
            else:
                promotion_output=self._auto_promote_engineering_permit(
                    allow_entries=allow_entries)
                if promotion_output is not None:
                    output=promotion_output
        if output is None:
            output=self.review(snapshot=snapshot,quotes=quotes,allow_entries=allow_entries,
                allow_dispatch=(allow_dispatch and allow_autonomous_review
                                and not self.stopped))
        self._publish()
        if (output.get('status') in {
                'ENGINEERING_PROMOTION_READY',
                'ENGINEERING_PROMOTION_ALREADY_COMPLETED'}
                and isinstance(output.get('action_id'),str)):
            self._auto_promotion_observed.add(output['action_id'])
        return {**output,'host_manifest':str(self.directory/'manifest.json'),'requires_online_formal_host':True,
            'local_worker_results':local_worker_results,
            'broker_writes_by_bridge':0,'model_calls':0,'execution_authorized':False,
            'retired_receipts_imported':imported,'observed_receipts_pending':list(self.retired),
            'observed_receipt_errors':retired_failures,
            'attention_required':bool(output.get('attention_required') or retired_failures)}

    def close(self):
        self._owner();self.stopped=True
        review=self.review.close();wake_shutdown=self.wake_dispatcher.close();uncertain=False
        if self.pending:
            pending=self.pending
            try:
                if pending['packet']['operation']=='review':self._abandon_pending_review(pending,'HOST_CONTROLLER_STOPPED')
                else:self._uncertain('HOST_CONTROLLER_STOPPED')
            except Exception:uncertain=True
            finally:pending['lease'].close()
        current=read_private_json(self.directory/'manifest.json')
        if current.get('binding',{}).get('epoch')==self.binding['epoch']:self._publish()
        return {'status':'HOST_BRIDGE_STOPPED','review_shutdown':review,'wake_shutdown':wake_shutdown,
            'pending_operation_id':self.pending['packet']['operation_id'] if self.pending else None,
            'observed_receipts_pending':list(self.retired),
            'attention_required':uncertain or self.pending is not None or bool(self.retired) or review.get('attention_required',False) or wake_shutdown.get('callback_retained',False) or wake_shutdown.get('attention_required',False),
            'automatic_replay_allowed':False}
