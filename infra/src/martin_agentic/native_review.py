"""Deterministic formal equity reviews. No place/cancel channel exists here."""
import hashlib
import json
import os
from pathlib import Path
import time

from .audit import canonical,digest
from .broker import rows
from .control_tasks import owned_read_transport,worker_lease
from .market import unwrap_response
from .native_operations import NativeOperations,sanitized
from .transport import CodexMarketTransport,safe_read_failure


class NativeReviewFailure(ValueError):
    def __init__(self,phase,packet,error,recorded):
        self.details={'phase':phase,'operation_id':packet['operation_id'] if packet else None,
            'failure':safe_read_failure(error),'exception_type':type(error).__name__,
            'failed_review_recorded':recorded,'broker_writes':0,
            'pending_operation_requires_recovery':bool(packet) and not recorded}
        super().__init__('NATIVE_REVIEW_ADAPTER_FAILED:'+json.dumps(self.details,sort_keys=True))


class EquityReviewTransport(CodexMarketTransport):
    """Separate authenticated review boundary; inherited market reads stay denied."""
    simulation_only=False

    def __call__(self,tool,arguments):
        if tool!='get_accounts' or arguments!={}:raise ValueError('REVIEW_TRANSPORT_TOOL_DENIED')
        return self._read_call(tool,arguments)

    def execute_review(self,packet):
        account_mask = packet.get('account')
        suffix = account_mask[-4:] if isinstance(account_mask, str) else ''
        if (packet.get('operation')!='review' or packet.get('tool')!='review_equity_order'
                or not isinstance(account_mask, str) or not account_mask.startswith('••••')
                or not (suffix.isdigit() and len(suffix) == 4)):
            raise ValueError('EXACT_PROJECT_REVIEW_PACKET_REQUIRED')
        parameters=packet.get('parameters')
        allowed={'symbol','side','type','quantity','limit_price','stop_price','time_in_force','market_hours'}
        required={'symbol','side','type','quantity','time_in_force','market_hours'}
        if not isinstance(parameters,dict) or set(parameters)-allowed or not required<=set(parameters):
            raise ValueError('REVIEW_PARAMETERS_SCHEMA_INVALID')
        accounts=rows(unwrap_response(self('get_accounts',{})),'accounts')
        matches=[a for a in accounts if isinstance(a.get('account_number'),str) and a['account_number'].endswith(suffix)]
        if len(matches)!=1:raise ValueError('TARGET_ACCOUNT_NOT_UNIQUELY_ACCESSIBLE')
        account=matches[0]
        if (account.get('agentic_allowed') is not True or account.get('deactivated') is not False
                or account.get('permanently_deactivated') is not False or account.get('state')!='active'):
            raise ValueError('TARGET_ACCOUNT_NOT_ACTIVE_OR_ALLOWED')
        if hashlib.sha256(account['account_number'].encode()).hexdigest()!=packet.get('account_key'):
            raise ValueError('REVIEW_ACCOUNT_BINDING_CHANGED')
        # Only this operation can reach the RPC call. No dynamic packet tool dispatch.
        return self._read_call('review_equity_order',{'account_number':account['account_number'],**parameters})


def run_native_review(book,action_id,snapshot,quote,receipt_dir,*,transport_factory=EquityReviewTransport):
    """One existing core action -> formal review -> durable receipt -> core record.

    This synchronous command is a first adapter slice, not the production daemon.
    An explicit synthetic factory may only service a synthetic book/snapshot.
    Real factories never receive synthetic inputs. No local approval is created.
    """
    if (type(getattr(transport_factory,'simulation_only',None)) is not bool
            or book.simulation is not transport_factory.simulation_only
            or snapshot.get('synthetic') is not book.simulation):
        raise ValueError('NATIVE_REVIEW_PROVENANCE_MISMATCH')
    suffix = str(book.policy.get('account_suffix', ''))
    if not (suffix.isdigit() and len(suffix) == 4) or book.policy.get('capture_only') is True:
        raise ValueError('NATIVE_REVIEW_POLICY_SCOPE_DENIED')
    native=NativeOperations(book)
    preview=native.preview(action_id,snapshot,quote=quote)
    if preview['next_operation']!='review':raise ValueError('REVIEW_ADAPTER_CANNOT_CANCEL')
    db=book.db.execute('PRAGMA database_list').fetchone()[2]
    if not db:raise ValueError('FILE_BACKED_WORKER_LEASE_REQUIRED')
    lease=worker_lease(db)
    if lease is None:raise ValueError('WORKER_OWNER_STILL_ACTIVE')
    packet=None;phase='connect'
    try:
        # Connect before reservation, then revalidate all core inputs after startup.
        deadline=time.monotonic()+10
        with owned_read_transport(transport_factory,deadline_monotonic=deadline) as transport:
            packet=native.begin(action_id,'review',snapshot,quote=quote)
            phase='formal_review'
            try:response=transport.execute_review(packet)
            except (ValueError,KeyError,TypeError,OSError,RuntimeError) as exc:
                failure=safe_read_failure(exc)
                response={'isError':True,'content':[{'type':'text','text':failure['code']}]}
        phase='receipt_persistence'
        binding={'db':str(Path(db).resolve()),'account_key':snapshot['account_key'],
            'policy_sha256':digest(book.policy),'synthetic':book.simulation}
        path=persist_review_receipt(packet,response,binding,receipt_dir,book.clock().isoformat())
        phase='receipt_import'
        result=native.record(packet['operation_id'],response)
        return {'status':'WAITING_USER' if result.get('review_verified') else 'REVIEW_FAILED',
            'operation_id':packet['operation_id'],'action_id':action_id,'receipt_file':str(path),
            'native_result':result,'broker_writes':0,'model_calls':0,'broker_effect_verified':False,
            'execution_authorized':False,'production_ready':False}
    except Exception as exc:
        recorded=False
        if packet is not None:
            try:
                native.record(packet['operation_id'],{'isError':True,'content':[
                    {'type':'text','text':'NATIVE_REVIEW_ADAPTER_FAILED_NO_ORDER_SUBMITTED'}]})
                recorded=True
            except Exception:pass
        raise NativeReviewFailure(phase,packet,exc,recorded) from None
    finally:lease.close()


def persist_review_receipt(packet,response,binding,receipt_dir,received_at):
    receipt={'binding':binding,'packet':packet,'packet_sha256':digest(packet),
        'response':sanitized(response),'response_sha256':digest(sanitized(response)),
        'received_at':received_at,'broker_effect_verified':False}
    directory=Path(receipt_dir);directory.mkdir(parents=True,exist_ok=True);directory.chmod(0o700)
    path=directory/(packet['operation_id']+'.json');temporary=path.with_suffix('.tmp')
    with temporary.open('x') as handle:
        os.chmod(temporary,0o600);handle.write(canonical(receipt)+'\n');handle.flush();os.fsync(handle.fileno())
    temporary.replace(path)
    descriptor=os.open(directory,os.O_RDONLY)
    try:os.fsync(descriptor)
    finally:os.close(descriptor)
    return str(path)
