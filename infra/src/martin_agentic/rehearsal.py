"""Deterministic offline fault scenarios against the real ExecutionBook API.

There is no transport/credential/runtime handle input. All observations and
receipts are explicitly synthetic. Delay cases are stress scenarios, not an
empirical latency distribution or Robinhood execution acceptance.
"""
from copy import deepcopy
from datetime import datetime,timedelta
import hashlib
import json
from pathlib import Path
import tempfile

from .broker import normalize_order
from .execution import ExecutionBook
from .native_operations import NativeOperations
from .qualification import candidate_root


class _Fixture:
    def __init__(self,path,policy):
        self.at=datetime.fromisoformat(policy['session_date']+'T14:00:00+00:00');self.initial=self.at
        self.book=ExecutionBook(path,policy,simulation=True,clock=lambda:self.at)
        self.plan={'symbol':'DEMO','quantity':2,'entry_ceiling':'40','stop':'39','cost_buffer_per_share':'0.05'}
    def quote(self):return {'symbol':'DEMO','bid':'39.99','ask':'40',**{k:self.at.isoformat() for k in ('bid_at','ask_at','observed_at')}}
    def snap(self,orders=(),qty=0,held=0):
        return {'schema_version':1,'provider':'robinhood','synthetic':True,
            'account_key':hashlib.sha256(b'SYNTHETIC-0000').hexdigest(),'account_mask':'••••0000',
            'started_at':self.at.isoformat(),'observed_at':self.at.isoformat(),
            'positions':{} if not qty else {'DEMO':{'symbol':'DEMO','quantity':str(qty),
                'available_for_sells':str(qty-held),'held_for_sells':str(held),'average_price':'40'}},
            'orders':{o['id']:deepcopy(o) for o in orders},'groups':[],
            'other_asset_open_orders':0,'other_asset_values':{},
            'portfolio':{'cash':str(500-40*qty),'total_value':'500','pending_deposits':'0'},
            'buying_power':str(500-40*qty),'unleveraged_buying_power':str(500-40*qty),
            'human_must_approve_trades':True,'pending_approvals':0,'pending_approval_ids':[],
            'approval_states':{},'requested_approval_ids':[],'missing_approval_ids':[],'collection_complete':True}
    def raw_order(self,action,filled=0,state='confirmed'):
        p=action['parameters'];oid='synthetic-'+action['action_id']
        return {'id':oid,'symbol':'DEMO','ref_id':action['ref_id'],'side':p['side'],
            'quantity':p['quantity'],'cumulative_quantity':str(filled),'type':'market' if p['type']=='stop_market' else p['type'],
            'trigger':'stop' if p['type']=='stop_market' else 'immediate','stop_price':p.get('stop_price'),
            'price':p.get('limit_price'),'created_at':self.initial.isoformat(),'state':state,
            'market_hours':p['market_hours'],'time_in_force':p['time_in_force'],
            'executions':[] if not filled else [{'id':oid+'-fill','price':'40','quantity':str(filled),
                'fees':'0','timestamp':self.initial.isoformat()}]}
    def order(self,action,filled=0,state='confirmed'):
        return normalize_order(self.raw_order(action,filled,state),self.at.isoformat())
    def propose(self):return self.book.propose('scenario',self.plan,self.quote(),self.snap(),evidence={'source_sha256':'synthetic','synthetic':True})
    def entry(self,quantity=2):
        self.propose();a=self.book.claim();self.book.report(a['action_id'],{'status':'submitted'})
        buy=self.order(a,quantity,'filled' if quantity==2 else 'partially_filled')
        self.book.reconcile(self.snap([buy],quantity));return buy,a


def _scenario(name,policy):
    with tempfile.TemporaryDirectory(prefix='martin-fault-') as tmp:
        f=_Fixture(Path(tmp)/'scenario.sqlite',policy);b=f.book
        try:
            if name.startswith('delay_'):
                seconds=int(name.split('_')[1]);buy,_=f.entry();f.at+=timedelta(seconds=seconds)
                b.reconcile(f.snap([buy],2));trade=b.state()['trades']['scenario']
                passed=bool(trade['exit_requested'])==(seconds>=30)
                details={'delay_seconds':seconds,'exit_requested':trade['exit_requested'],'issues':b.state()['issues'],
                    'timing_basis':trade.get('unprotected_timing_basis'),'unprotected_since':trade.get('unprotected_since')}
            elif name=='duplicate_event':
                first=f.propose();second=f.propose()
                passed=second['duplicate'] and len(b.actions())==1;details={'actions':len(b.actions()),'duplicate':second['duplicate']}
            elif name=='write_before_receipt_crash':
                f.propose();action=b.claim();b.recover();locked=b.claim() is None
                state_before=next(a['state'] for a in b.actions() if a['id']==action['action_id']);buy=f.order(action,2,'filled');b.reconcile(f.snap([buy],2))
                state_after=next(a['state'] for a in b.actions() if a['id']==action['action_id'])
                passed=locked and state_before=='UNKNOWN' and state_after=='OBSERVED' and len([a for a in b.actions() if a['kind']=='entry'])==1
                details={'before_reconcile':state_before,'after_reconcile':state_after,'blind_retry_blocked':locked}
            elif name=='partial_fill':
                _,_=f.entry(1);protect=next(a for a in b.actions() if a['kind']=='protect');payload=json.loads(protect['payload'])
                passed=payload['parameters']['quantity']=='1' and any(a['kind']=='cancel' for a in b.actions())
                details={'protection_quantity':payload['parameters']['quantity'],'entry_remainder_cancel_queued':any(a['kind']=='cancel' for a in b.actions())}
            elif name=='cancel_fill_race':
                buy,_=f.entry();protect=b.claim();b.report(protect['action_id'],{'status':'submitted'});stop=f.order(protect)
                b.reconcile(f.snap([buy,stop],2,2));q={**f.quote(),'bid':'38.99','ask':'39'}
                b.reconcile(f.snap([buy,stop],2,2),quotes={'DEMO':q});cancel=b.claim();b.report(cancel['action_id'],{'status':'cancel_requested'})
                f.at+=timedelta(seconds=1);stop=f.order(protect,2,'filled');stop['executions'][0]['executed_at']=f.at.isoformat()
                b.reconcile(f.snap([buy,stop]),quotes={'DEMO':{**q,**{k:f.at.isoformat() for k in ('bid_at','ask_at','observed_at')}}})
                passed=b.state()['trades']['scenario']['phase']=='FLAT_VERIFIED' and not any(a['kind']=='exit' and a['state']=='READY' for a in b.actions())
                details={'phase':b.state()['trades']['scenario']['phase'],'extra_exit_ready':any(a['kind']=='exit' and a['state']=='READY' for a in b.actions())}
            elif name=='native_write_before_receipt_crash':
                buy,_=f.entry();protect=next(a for a in b.actions() if a['kind']=='protect');ops=NativeOperations(b)
                review=ops.begin(protect['id'],'review',f.snap([buy],2),quote=f.quote())
                ops.record(review['operation_id'],{'data':{**review['parameters'],'order_checks':{},'customer_approval_required':False,'market_data_disclosure':'synthetic'}})
                place=ops.begin(protect['id'],'place',f.snap([buy],2),quote=f.quote())
                b.recover();locked=b.claim() is None
                action=json.loads(protect['payload']);raw=f.raw_order(action);raw['ref_id']='synthetic-rewritten-broker-ref'
                receipt={'structuredContent':{'data':{'order':raw,'approval':None}}}
                recorded=ops.record(place['operation_id'],receipt)
                stop=normalize_order(raw,f.at.isoformat());b.reconcile(f.snap([buy,stop],2,2))
                phase=b.state()['trades']['scenario']['phase']
                passed=locked and recorded['report']['status']=='submitted' and phase=='PROTECTED' and len([a for a in b.actions() if a['kind']=='protect'])==1
                details={'blind_retry_blocked':locked,'phase':phase,'actual_ref_rewritten':True,
                    'exact_order_id_registered':bool(b.db.execute('SELECT 1 FROM external_broker_order_ids WHERE order_id=?',(raw['id'],)).fetchone())}
            elif name=='approval_expired':
                buy,_=f.entry();protect=next(a for a in b.actions() if a['kind']=='protect');ops=NativeOperations(b)
                packet=ops.begin(protect['id'],'review',f.snap([buy],2),quote=f.quote())
                ops.record(packet['operation_id'],{'data':{**packet['parameters'],'order_checks':{},'customer_approval_required':False,'market_data_disclosure':'synthetic'}})
                packet=ops.begin(protect['id'],'place',f.snap([buy],2),quote=f.quote())
                result=ops.record(packet['operation_id'],{'structuredContent':{'data':{'order':None,'approval':{
                    'approval_id':'synthetic-expired','account_number':'SYNTHETIC-0000','asset_class':'ASSET_CLASS_EQUITY',
                    'state':'APPROVAL_STATE_EXPIRED','is_edited':False}}}})
                # Receipt alone is not a broker order/protection observation.
                passed=result['report']['status']=='rejected' and not result['broker_effect_verified'] and b.state()['trades']['scenario']['phase']!='PROTECTED'
                details={'report_status':result['report']['status'],'broker_effect_verified':result['broker_effect_verified'],'phase':b.state()['trades']['scenario']['phase']}
            else:raise ValueError('UNKNOWN_REHEARSAL_SCENARIO')
            return {'name':name,'passed':bool(passed),'details':details,'synthetic':True,'audit_integrity':b.audit.verify()}
        finally:b.close()


def run_rehearsal(output_dir,policy_path=None):
    policy=json.loads(Path(policy_path or candidate_root()/'config/synthetic_policy.json').read_text())
    scenarios=['delay_30','delay_31','delay_107','delay_120','duplicate_event','write_before_receipt_crash','native_write_before_receipt_crash','partial_fill','cancel_fill_race','approval_expired']
    results=[]
    for name in scenarios:
        try:results.append(_scenario(name,policy))
        except (ValueError,KeyError,TypeError,RuntimeError) as exc:results.append({'name':name,'passed':False,'synthetic':True,'error':type(exc).__name__+':'+str(exc)})
    report={'mode':'deterministic_executionbook_fault_rehearsal','passed':all(r['passed'] for r in results),
        'scenarios':results,'broker_calls':0,'synthetic':True,'robinhood_live_acceptance':False,
        'delay_scenarios_are_empirical_distribution':False,
        'alpaca_paper':{'enabled':False,'requires_separate_explicit_adapter':True,
            'adapter_contract':['read_snapshot','submit_bound_intent','observe_execution','observe_protection','cancel_terminal','reconcile'],
            'does_not_validate':['Robinhood MCP authentication','Robinhood confirmation contract','real queue position','real latency/slippage']}}
    out=Path(output_dir);out.mkdir(parents=True,exist_ok=True);target=out/'fault-rehearsal.json';target.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    return report|{'report_path':str(target.resolve())}
