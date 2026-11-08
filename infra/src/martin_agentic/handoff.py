"""Concrete operator handoff, never a local substitute for human confirmation."""
from datetime import datetime,timezone
import json

from .audit import digest
from .ledger import timestamp


def operator_packet(book,*,action_id=None):
    actions=book.actions()
    selectable=[a for a in actions if a['state'] in {'READY','WAITING_USER','RUNNING','UNKNOWN','REPORTED'}]
    if action_id:selectable=[a for a in selectable if a['id']==action_id]
    if not selectable:return {'available':False,'reason':'NO_PENDING_ACTION','specific_order_confirmed':False}
    action=selectable[0];payload=json.loads(action['payload'])
    report=json.loads(action['report']) if action['report'] else {}
    now=book.clock();expired=now>=timestamp(payload['expires_at'])
    parameters=payload['parameters']
    plan=payload['plan']
    protective=None
    if action['kind']=='entry':
        protective={'symbol':plan['symbol'],'side':'sell','type':'stop_market','quantity':str(plan['quantity']),
            'stop_price':plan['stop'],'market_hours':'regular_hours','time_in_force':'gtc',
            'condition':'Only after exactly this quantity actually fills; partial fill requires refreshed exact preview.'}
    identity='operator-'+action['id']+'-'+digest([parameters,payload['ref_id']])[:16]
    audit_event={'event_id':identity+'-intent','occurred_at':now.isoformat(),'event_type':'AGENT_ACTION_INTENT',
        'actor':'agent','synthetic':book.simulation,'account':payload['account'],'trade_id':action['trade_id'],
        'intent_id':identity,'ref_id':payload['ref_id'],'symbol':plan['symbol'],
        'details':{'action':{'entry':'entry','protect':'stop_loss','cancel':'cancel','exit':'exit'}[action['kind']],
            'parameters':parameters,'origin':'concrete_native_tool_handoff'}}
    return {'available':True,'account':payload['account'],'action_id':action['id'],'state':action['state'],
        'parameters':parameters,'ref_id':payload['ref_id'],'plan':plan,'contingent_initial_protection':protective,
        'expired':expired,'fresh_review_required':True,'specific_order_confirmed':False,
        'broker_quote_disclosure':report.get('market_data_disclosure',''),'native_preview':report.get('message',''),
        'submission_allowed_by_packet':False,'audit_before_native_submission':audit_event,
        'operator_steps':['Refresh exact target account, actual position/orders, quote and tradability.',
            'Call native review with exact parameters; present disclosure/alerts verbatim as required.',
            'Obtain the specific native-tool-required confirmation in this conversation or follow App approval route.',
            'If expired or price/quantity/structure changed, refresh the proposal rather than silently alter the order.',
            'Append intent before native call; keep the same logical ref_id; append result or UNKNOWN after.',
            'Reconcile broker fill/position before protecting; never infer protection from the submission response.']}
