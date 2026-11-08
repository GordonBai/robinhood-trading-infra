"""Audited handoff for official, conversation-executed broker operations.

This module NEVER calls a broker write API and NEVER grants confirmation. It
reserves exact actions before a native tool call and reconnects its advisory
result to the lifecycle. Only subsequent broker observations prove effects.
"""
from datetime import timedelta
import hashlib
import json
import uuid

from .audit import canonical,digest
from .broker import normalize_order
from .ledger import timestamp
from .risk import number

PRIVATE={'account_number','rhs_account_number','rhc_account_number','authorization',
         'access_token','refresh_token','password','cookie','cookies','headers','api_key'}

def sanitized(value):
    if isinstance(value,dict):return {k:sanitized(v) for k,v in value.items() if k.lower() not in PRIVATE}
    if isinstance(value,list):return [sanitized(v) for v in value]
    return value


def native_envelope(response):
    if not isinstance(response,dict) or response.get('isError'):
        raise ValueError('NATIVE_RESPONSE_NOT_VERIFIED')
    envelope=response.get('structuredContent',response)
    if isinstance(envelope,dict) and isinstance(envelope.get('data'),dict):return envelope
    for block in response.get('content',[]):
        if not isinstance(block,dict):continue
        if block.get('type')=='text':
            try:envelope=json.loads(block['text'])
            except (ValueError,TypeError):continue
            if isinstance(envelope,dict) and isinstance(envelope.get('data'),dict):return envelope
    raise ValueError('NATIVE_RESPONSE_SCHEMA_UNKNOWN')


def native_data(response):
    return native_envelope(response)['data']


def response_identity(response):
    scope_hashes=[]
    try:
        data=native_data(response)
        for source in (data,data.get('order')):
            if isinstance(source,dict) and 'account_number' in source:
                scope_hashes.append(digest(source['account_number']))
    except (ValueError,KeyError,TypeError):pass
    redacted=sanitized(response)
    return redacted,digest({'redacted_response':redacted,'account_scope_hashes':scope_hashes})


class NativeOperations:
    def __init__(self,book):
        self.book=book
        if book.db.in_transaction:raise ValueError('NATIVE_OPERATIONS_REQUIRE_NO_ACTIVE_TRANSACTION')
        book.db.execute('''CREATE TABLE IF NOT EXISTS native_operation_records(
            id TEXT PRIMARY KEY,action_id TEXT NOT NULL,operation TEXT NOT NULL,
            state TEXT NOT NULL,payload TEXT NOT NULL,result TEXT,created_at TEXT NOT NULL)''')
        book.db.commit()

    def _action(self,action_id):
        a=next((a for a in self.book.actions() if a['id']==action_id),None)
        if not a:raise ValueError('NATIVE_ACTION_NOT_FOUND')
        return a

    def _verified_returned_order(self,packet,response):
        data=native_data(response);order=data.get('order')
        if (not isinstance(order,dict) or data.get('approval') is not None
                or 'approval_id' in data or 'order_id' in data and data['order_id']!=order.get('id')):
            raise ValueError('NATIVE_DIRECT_ORDER_REQUIRED')
        normalized=normalize_order(order,self.book.clock().isoformat())
        self.book._check_order_terms(normalized,{'parameters':packet['parameters']})
        bound_key=packet.get('account_key') or self.book.state()['account_key']
        if not bound_key or bound_key!=self.book.state()['account_key']:
            raise ValueError('NATIVE_ORDER_ACCOUNT_BINDING_CHANGED')
        for source in (data,order):
            if 'account_number' in source:
                account=source['account_number']
                if not isinstance(account,str) or hashlib.sha256(account.encode()).hexdigest()!=bound_key:
                    raise ValueError('NATIVE_ORDER_ACCOUNT_MISMATCH')
        return normalized,bound_key

    def reconnect_recorded_order(self,operation_id,response):
        """Explicitly recover one exact stored receipt's ID; never claim fills.

        No broker call, action/trade transition, issue clearing, or automatic
        reconciliation occurs. The caller chooses whether to migrate a receipt.
        """
        redacted,fingerprint=response_identity(response)
        self.book._begin()
        try:
            row=self.book.db.execute('SELECT action_id,operation,payload,result,state FROM native_operation_records WHERE id=?',(operation_id,)).fetchone()
            if not row or row[1]!='place' or row[4]!='RECORDED':raise ValueError('RECORDED_NATIVE_PLACE_REQUIRED')
            action_id,_,raw,old,_=row;packet=json.loads(raw);recorded=json.loads(old)
            # Pre-upgrade hashes covered the redacted full response. Accept that
            # exact legacy digest only here; never manufacture another receipt.
            if recorded.get('response_hash') not in {fingerprint,digest(redacted)}:
                raise ValueError('NATIVE_RESPONSE_ID_CONFLICT')
            normalized,bound_key=self._verified_returned_order(packet,response)
            identity=normalized['id']
            if (not recorded.get('response_usable') or recorded.get('order_id')!=identity
                    or recorded.get('report',{}).get('status') not in {'submitted','rejected'}):
                raise ValueError('RECORDED_NATIVE_ORDER_ID_NOT_VERIFIED')
            owner=self.book.db.execute('SELECT action_id,operation_id,account_key FROM external_broker_order_ids WHERE order_id=?',(identity,)).fetchone()
            if owner and owner!=(action_id,operation_id,bound_key):raise ValueError('BROKER_ORDER_ID_OWNER_CONFLICT')
            if not owner:
                self.book.db.execute('INSERT INTO external_broker_order_ids VALUES(?,?,?,?)',(identity,action_id,operation_id,bound_key))
                self.book.audit.append_in_transaction({'event_id':operation_id+'-order-identity-reconnected',
                    'occurred_at':self.book.clock().isoformat(),'event_type':'RECONCILIATION_ISSUE','actor':'script',
                    'synthetic':self.book.simulation,'account':packet['account'],'trade_id':self._action(action_id)['trade_id'],
                    'broker_order_id':identity,'details':{'status':'identity_registered','broker_verified':False,
                        'evidence':'exact_recorded_native_response_hash_and_order_terms','operation_id':operation_id,
                        'no_broker_write':True,'no_trade_transition':True,'no_issue_clearing':True}})
            self.book.db.commit()
            return {'operation_id':operation_id,'action_id':action_id,'order_id':identity,
                'registered':True,'duplicate':bool(owner),'broker_effect_verified':False,
                'requires_fresh_bound_broker_reconciliation':True}
        except BaseException:self.book.db.rollback();raise

    def preview(self,action_id,snapshot,quote=None):
        action=self.book.validate_external_action(action_id,snapshot,quote=quote)
        return {'action_id':action_id,'kind':action['kind'],'parameters':action['parameters'],
            'ref_id':action['ref_id'],'account':action['account'],'requires_official_tool':True,
            'specific_order_confirmed':False,'broker_effect_verified':False,
            'next_operation':'cancel' if action['kind']=='cancel' else 'review'}

    def begin(self,action_id,operation,snapshot,quote=None):
        if operation not in {'review','place','cancel'}:raise ValueError('INVALID_NATIVE_OPERATION')
        if self.book.policy.get('capture_only') is True and operation in {'place','cancel'}:
            raise ValueError('CAPTURE_ONLY_POLICY_READ_ONLY')
        self.book.reconcile(snapshot,quotes={})
        action=self.book.validate_external_action(action_id,snapshot,quote=quote)
        if (action['kind']=='cancel')!=(operation=='cancel'):
            raise ValueError('NATIVE_OPERATION_ACTION_MISMATCH')
        review=None
        if operation=='place':
            row=self.book.db.execute("SELECT payload,result,created_at,state FROM native_operation_records WHERE action_id=? AND operation='review' ORDER BY rowid DESC LIMIT 1",(action_id,)).fetchone()
            if not row or row[3]!='RECORDED':raise ValueError('FRESH_NATIVE_REVIEW_REQUIRED')
            prior,result,created=json.loads(row[0]),json.loads(row[1]),timestamp(row[2])
            if not result.get('review_verified') or prior['parameters']!=action['parameters'] or not 0<=(self.book.clock()-created).total_seconds()<=30:
                raise ValueError('FRESH_NATIVE_REVIEW_REQUIRED')
            review=result
        identity='native-'+str(uuid.uuid4())
        # Reservation/intent is a mutex and audit event, never an order approval.
        packet=None
        def record_reservation(action):
            nonlocal packet
            tool={'review':'review_equity_order','place':'place_equity_order','cancel':'cancel_equity_order'}[operation]
            parameters=dict(action['parameters'])
            if operation=='place':parameters['ref_id']=action['ref_id']
            packet={'operation_id':identity,'action_id':action_id,'operation':operation,'tool':tool,
                'account':action['account'],'account_key':snapshot['account_key'],'parameters':parameters,'logical_ref_id':action['ref_id'],
                'specific_order_confirmed':False,'execution_authorized_by_packet':False,
                'requires_native_confirmation':operation=='cancel' or operation=='place' and bool(review and review['requires_native_confirmation']),
                'confirmation_source':'current_native_tool_contract','broker_effect_verified':False}
            if action['kind']=='entry':
                trade=self.book.state()['trades'][action['trade_id']]
                packet['engineering_parameters_authorized_at']=trade.get('evidence',{}).get('parameters_authorized_at')
            if review:
                packet.update(market_data_disclosure=review['market_data_disclosure'],order_checks=review['order_checks'],
                    customer_approval_required=review.get('customer_approval_required'),
                    customer_approval_required_reason=review.get('customer_approval_required_reason'))
            self.book.db.execute('INSERT INTO native_operation_records VALUES(?,?,?,?,?,NULL,?)',
                (identity,action_id,operation,'STARTED',canonical(packet),self.book.clock().isoformat()))
        self.book.prepare_external_action(action_id,snapshot,quote=quote,operation_id=identity,operation=operation,
            reservation_writer=record_reservation)
        return packet

    def mark_uncertain(self,operation_id,reason):
        """Preserve an original write intent while awaiting its genuine receipt.

        Do not install a fabricated response: a late real receipt must still be
        importable. Broker-observed action states are never regressed.
        """
        if reason not in {'HOST_HANDOFF_EXPIRED','HOST_CONTROLLER_STOPPED','HOST_PACKET_PUBLICATION_FAILED','HOST_RECEIPT_INVALID'}:
            raise ValueError('INVALID_NATIVE_UNCERTAINTY_REASON')
        self.book._begin()
        try:
            row=self.book.db.execute('SELECT action_id,operation,state FROM native_operation_records WHERE id=?',(operation_id,)).fetchone()
            if not row or row[1] not in {'place','cancel'}:raise ValueError('NATIVE_WRITE_OPERATION_REQUIRED')
            action=self._action(row[0])
            changed=row[2]=='STARTED' and action['state']=='RUNNING'
            if changed:
                self.book.db.execute("UPDATE live_actions SET state='UNKNOWN',updated_at=? WHERE id=?",(self.book.clock().isoformat(),row[0]))
                self.book.audit.append_in_transaction({'event_id':operation_id+'-host-unknown',
                    'occurred_at':self.book.clock().isoformat(),'event_type':'AGENT_ACTION_UNKNOWN',
                    'actor':'agent','synthetic':self.book.simulation,'intent_id':operation_id,
                    'trade_id':action['trade_id'],'reason':reason})
            self.book.db.commit()
            return {'operation_id':operation_id,'changed':changed,'broker_effect_verified':False}
        except BaseException:self.book.db.rollback();raise

    def abandon_read(self,operation_id,*,reason='EXPLICIT_READ_RECOVERY'):
        """Settle only a provably read-only reservation, never an unknown write.

        The caller owns the execution book. This is a local control disposition,
        not a fabricated broker response and not a verified/fresh order review.
        """
        if reason not in {'HOST_REVIEW_EXPIRED','HOST_CONTROLLER_STOPPED','EXPLICIT_READ_RECOVERY'}:
            raise ValueError('NATIVE_READ_ABANDON_REASON_INVALID')
        row=self.book.db.execute('SELECT action_id,operation,state,payload,result FROM native_operation_records WHERE id=?',(operation_id,)).fetchone()
        if not row or row[1]!='review':raise ValueError('NATIVE_READ_OPERATION_REQUIRED')
        action_id,_,state,raw,previous=row;packet=json.loads(raw)
        if state=='ABANDONED_READ':
            result=json.loads(previous)
            if result.get('source')!='local_control' or result.get('review_verified') is not False:
                raise ValueError('NATIVE_ABANDONED_READ_RECORD_INVALID')
            return result
        if state!='STARTED':raise ValueError('NATIVE_READ_NOT_STARTED')
        if (packet.get('operation')!='review' or packet.get('tool')!='review_equity_order'
                or packet.get('action_id')!=action_id or packet.get('account_key')!=self.book.state()['account_key']):
            raise ValueError('NATIVE_READ_BINDING_INVALID')
        if self._action(action_id)['state']!='RUNNING':raise ValueError('NATIVE_READ_ACTION_NOT_RUNNING')
        def safety():
            if self.book.db.execute('SELECT 1 FROM audit_events WHERE event_id=?',(action_id+'-intent',)).fetchone():
                raise ValueError('LEGACY_ACTION_INTENT_BLOCKS_READ_ABANDONMENT')
            if self.book.db.execute("SELECT 1 FROM live_actions WHERE id!=? AND state IN ('RUNNING','UNKNOWN')",(action_id,)).fetchone():
                raise ValueError('UNRESOLVED_ACTION_BLOCKS_READ_ABANDONMENT')
            writes=self.book.db.execute("SELECT e.report,a.state FROM external_operations e JOIN live_actions a ON a.id=e.action_id WHERE e.operation IN ('place','cancel')").fetchall()
            if any(report is None or json.loads(report).get('status')=='unknown'
                    or action_state!='OBSERVED' and json.loads(report).get('status') in {'submitted','pending_approval','cancel_requested'}
                    for report,action_state in writes):
                raise ValueError('UNRESOLVED_WRITE_BLOCKS_READ_ABANDONMENT')
        safety()
        now=self.book.clock().isoformat();report={'status':'review_only','message':'LOCAL_READ_ABANDONED_FRESH_REVIEW_REQUIRED'}
        result={'operation_id':operation_id,'action_id':action_id,'state':'ABANDONED_READ',
            'source':'local_control','reason':reason,'recorded_at':now,'review_verified':False,
            'response_usable':False,'broker_effect_verified':False,'execution_authorized':False,
            'requires_fresh_review':True,'no_broker_response':True,'report':report}
        def settle():
            current=self.book.db.execute('SELECT state,operation FROM native_operation_records WHERE id=?',(operation_id,)).fetchone()
            if current!=('STARTED','review'):raise ValueError('NATIVE_READ_RESERVATION_CHANGED')
            safety()
            self.book.db.execute("UPDATE native_operation_records SET state='ABANDONED_READ',result=? WHERE id=?",(canonical(result),operation_id))
            self.book.audit.append_in_transaction({'event_id':operation_id+'-read-abandoned',
                'occurred_at':now,'event_type':'RECONCILIATION_ISSUE','actor':'script','synthetic':self.book.simulation,
                'intent_id':operation_id,'trade_id':self._action(action_id)['trade_id'],
                'details':{'status':'ABANDONED_READ','source':'local_control','reason':reason,
                    'packet_sha256':digest(packet),'review_verified':False,'no_broker_response':True}})
        self.book.record_external_report(action_id,report,operation_id=operation_id,result_writer=settle)
        return result

    def record(self,operation_id,response):
        row=self.book.db.execute('SELECT action_id,operation,payload,result,state FROM native_operation_records WHERE id=?',(operation_id,)).fetchone()
        if not row:raise ValueError('NATIVE_OPERATION_NOT_FOUND')
        action_id,operation,raw,old,state=row;packet=json.loads(raw)
        # Redacted response is evidence of a tool return, never a fill or approval.
        original_response=response
        response,response_hash=response_identity(response)
        if state=='ABANDONED_READ':
            if operation!='review' or json.loads(old).get('source')!='local_control':
                raise ValueError('NATIVE_ABANDONED_READ_RECORD_INVALID')
            identity=operation_id+'-late-review-'+response_hash
            self.book._begin()
            try:
                previous=self.book.db.execute('SELECT payload FROM audit_events WHERE event_id=?',(identity,)).fetchone()
                if previous:late=json.loads(previous[0])['details']['late_result']
                else:
                    late={'operation_id':operation_id,'action_id':action_id,'response_hash':response_hash,
                        'recorded_at':self.book.clock().isoformat(),'late_receipt_retained':True,
                        'review_verified':False,'response_usable':False,'broker_effect_verified':False,
                        'execution_authorized':False,'action_state_unchanged':True,'requires_fresh_review':True,
                        'report':{'status':'review_only','message':'LATE_ABANDONED_REVIEW_METADATA_ONLY'}}
                    self.book.audit.append_in_transaction({'event_id':identity,'occurred_at':late['recorded_at'],
                        'event_type':'RECONCILIATION_ISSUE','actor':'script','synthetic':self.book.simulation,
                        'intent_id':operation_id,'trade_id':self._action(action_id)['trade_id'],
                        'details':{'status':'LATE_ABANDONED_READ_RECEIPT','late_result':late}})
                self.book.db.commit();return late
            except BaseException:self.book.db.rollback();raise
        if state=='RECORDED':
            result=json.loads(old)
            if result['response_hash'] not in {response_hash,digest(response)}:raise ValueError('NATIVE_RESPONSE_ID_CONFLICT')
            return result
        result={'operation_id':operation_id,'action_id':action_id,'response_hash':response_hash,
            'recorded_at':self.book.clock().isoformat(),'broker_effect_verified':False}
        report={'status':'unknown','message':'NATIVE_RESPONSE_REQUIRES_BROKER_RECONCILIATION'};alias=None;broker_order_id=None;order_account_key=None
        try:
            envelope=native_envelope(response);data=envelope['data']
            if 'guide' in envelope:
                if not isinstance(envelope['guide'],str):raise ValueError('NATIVE_GUIDE_SCHEMA_INVALID')
                result['native_guide']=envelope['guide']
            if operation=='review':
                expected=packet['parameters']
                for key in ('symbol','side','type','quantity','limit_price','stop_price'):
                    if key not in expected:continue
                    if key not in data:raise ValueError('NATIVE_REVIEW_TERMS_MISSING')
                    if key in {'quantity','limit_price','stop_price'}:
                        if number(data[key])!=number(expected[key]):raise ValueError('NATIVE_REVIEW_TERMS_CHANGED')
                    elif data[key]!=expected[key]:raise ValueError('NATIVE_REVIEW_TERMS_CHANGED')
                checks=data.get('order_checks');approval=data.get('customer_approval_required')
                if not isinstance(checks,dict) or approval is not None and type(approval) is not bool:
                    raise ValueError('NATIVE_REVIEW_SCHEMA_INVALID')
                disclosure=data.get('market_data_disclosure')
                if not isinstance(disclosure,str):raise ValueError('NATIVE_DISCLOSURE_REQUIRED')
                reason=data.get('customer_approval_required_reason')
                if reason is not None and not isinstance(reason,str):raise ValueError('NATIVE_REVIEW_SCHEMA_INVALID')
                result.update(review_verified=True,order_checks=checks,customer_approval_required=approval,
                    customer_approval_required_reason=reason,
                    market_data_disclosure=disclosure,requires_native_confirmation=approval is not True or bool(checks),
                    quote_data=data.get('quote_data'))
                report={'status':'needs_confirmation','message':'NATIVE_REVIEW_RECORDED_FOLLOW_CURRENT_TOOL_CONFIRMATION',
                    'market_data_disclosure':disclosure}
            elif operation=='cancel':
                if data.get('accepted') is True:report={'status':'cancel_requested','message':'CANCEL_ACCEPTED_NOT_TERMINAL'}
                elif data.get('accepted') is False:report={'status':'rejected','message':'CANCEL_NOT_ACCEPTED_RECONCILE_TARGET'}
                else:raise ValueError('NATIVE_CANCEL_SCHEMA_UNKNOWN')
                result['cancel_terminal_verified']=False
            else:
                # Official metadata returns EITHER data.order or data.approval;
                # both are nullable. An approval is not an order or a fill.
                approval=data.get('approval');order=data.get('order')
                if approval is not None and order is not None:
                    raise ValueError('NATIVE_AMBIGUOUS_PLACE_RESPONSE')
                if approval is not None:
                    if not isinstance(approval,dict):raise ValueError('NATIVE_APPROVAL_SCHEMA_UNKNOWN')
                    candidate=approval.get('approval_id')
                    if ('approval_id' in data and data['approval_id']!=candidate) or 'order_id' in data:
                        raise ValueError('NATIVE_CONFLICTING_PLACE_IDENTITIES')
                    if not isinstance(candidate,str) or not 0<len(candidate)<=200:raise ValueError('NATIVE_APPROVAL_ID_INVALID')
                    alias=candidate;result['approval_id']=alias
                    if approval.get('asset_class')!='ASSET_CLASS_EQUITY' or approval.get('is_edited') is not False:
                        raise ValueError('NATIVE_APPROVAL_EDITED_OR_ASSET_MISMATCH')
                    approval_state=approval.get('state');result['approval_state']=approval_state
                    if approval_state=='APPROVAL_STATE_PENDING':
                        report={'status':'pending_approval','message':'APP_APPROVAL_PENDING_NOT_ACTIVE_PROTECTION'}
                        result['approval_id']=alias
                    elif approval_state in {'APPROVAL_STATE_APPROVED','APPROVAL_STATE_AUTO_APPROVED'}:
                        report={'status':'submitted','message':'APPROVAL_REPORTED_APPROVED_BROKER_ORDER_REQUIRED'}
                        result['approval_id']=alias
                    elif approval_state in {'APPROVAL_STATE_DECLINED','APPROVAL_STATE_EXPIRED'}:
                        report={'status':'rejected','message':'APPROVAL_TERMINATED_WITHOUT_ORDER_CONFIRMATION'}
                        result['approval_id']=alias
                    else:raise ValueError('NATIVE_APPROVAL_STATE_UNKNOWN')
                elif order is not None:
                    if not isinstance(order,dict):raise ValueError('NATIVE_ORDER_SCHEMA_UNKNOWN')
                    if 'approval_id' in data or ('order_id' in data and data['order_id']!=order.get('id')):
                        raise ValueError('NATIVE_CONFLICTING_PLACE_IDENTITIES')
                    normalized,bound_key=self._verified_returned_order(packet,original_response)
                    rejected=normalized['state'] in {'rejected','failed','voided','locate_failed'}
                    report={'status':'rejected' if rejected else 'submitted','order_id':normalized['id'],
                        'message':'NATIVE_RESULT_RECORDED_BROKER_OBSERVATION_REQUIRED'}
                    result.update(order_id=normalized['id'],reported_order_state=normalized['state'])
                    broker_order_id=normalized['id'];order_account_key=bound_key
                elif self.book.simulation and ('approval_id' in data or 'order_id' in data):
                    # Historical local fixtures used compact receipts. Those are
                    # deliberately unavailable on a real-money execution book.
                    result['synthetic_compact_receipt']=True
                    alias=data.get('approval_id');oid=data.get('order_id')
                    if alias is not None:
                        if not isinstance(alias,str) or not 0<len(alias)<=200:raise ValueError('NATIVE_APPROVAL_ID_INVALID')
                        report={'status':'pending_approval','message':'SYNTHETIC_COMPACT_APPROVAL'}
                        result['approval_id']=alias
                    elif isinstance(oid,str) and 0<len(oid)<=200:
                        report={'status':'submitted','order_id':oid,'message':'SYNTHETIC_COMPACT_ORDER'}
                        result['order_id']=oid
                    else:raise ValueError('NATIVE_ORDER_ID_MISSING')
                else:raise ValueError('NATIVE_PLACE_RESPONSE_SCHEMA_UNKNOWN')

        except (ValueError,KeyError,TypeError):
            # Ambiguous transport/error/schema outcomes MUST stay unknown.
            result['response_usable']=False
            if operation=='review':
                report={'status':'review_only','message':'NATIVE_REVIEW_INVALID_NO_ORDER_SUBMITTED'}
        else:result['response_usable']=True
        result['report']=report
        def record_result():
            nonlocal result
            # Recheck under the core transaction: another receipt import may
            # have committed after the initial SELECT but before BEGIN IMMEDIATE.
            current=self.book.db.execute('SELECT state,result,payload,action_id,operation FROM native_operation_records WHERE id=?',(operation_id,)).fetchone()
            if not current or current[2:]!=(raw,action_id,operation):
                raise ValueError('NATIVE_OPERATION_CHANGED_DURING_IMPORT')
            if current[0]=='RECORDED':
                previous=json.loads(current[1])
                if previous['response_hash'] not in {response_hash,digest(response)}:
                    raise ValueError('NATIVE_RESPONSE_ID_CONFLICT')
                result=previous
                return
            if current[0]!='STARTED':raise ValueError('NATIVE_OPERATION_NOT_IMPORTABLE')
            self.book.db.execute("UPDATE native_operation_records SET state='RECORDED',result=? WHERE id=?",(canonical(result),operation_id))
        self.book.record_external_report(action_id,report,operation_id=operation_id,
            broker_ref_id=alias if operation=='place' and isinstance(alias,str) and 0<len(alias)<=200 else None,
            broker_order_id=broker_order_id,broker_account_key=order_account_key,result_writer=record_result)
        return result
