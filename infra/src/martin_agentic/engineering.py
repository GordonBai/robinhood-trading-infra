"""Explicitly classified engineering plans, distinct from strategy signals.

Scope authorization is not specific order confirmation. Drafts never submit or
approve orders, and cannot establish strategy qualification or broker fills.
"""
from datetime import datetime,timezone
import json
import uuid

from .audit import canonical
from .risk import number


class EngineeringPlans:
    def __init__(self,book):
        self.book=book
        book.db.execute('CREATE TABLE IF NOT EXISTS engineering_plans(id TEXT PRIMARY KEY,payload TEXT NOT NULL)')
        book.db.commit()

    def draft(self,plan,*,quote,scope_authorization,parameters_user_specified=False):
        if scope_authorization!='USER_EXPLICITLY_ALLOWED_SMALL_ENGINEERING_TEST':
            raise ValueError('ENGINEERING_SCOPE_NOT_AUTHORIZED')
        q=plan['quantity'];entry=number(plan['entry_ceiling']);stop=number(plan['stop']);cost=number(plan['cost_buffer_per_share'])
        if type(q) is not int or q<=0 or not 0<stop<entry or cost<0:raise ValueError('INVALID_ENGINEERING_PLAN')
        if q*entry>number(self.book.policy['max_position_notional_usd']) or q*(entry-stop+cost)>number(self.book.policy['max_planned_loss_per_trade_usd']):
            raise ValueError('ENGINEERING_PLAN_EXCEEDS_TRIAL_LIMITS')
        identity='engineering-'+str(uuid.uuid4());at=self.book.clock().isoformat()
        payload={'id':identity,'purpose':'engineering','synthetic':self.book.simulation,
            'status':'PARAMETERS_SPECIFIED_NOT_REVIEWED' if parameters_user_specified else 'AWAITING_PARAMETER_SELECTION',
            'created_at':at,'plan':plan,'quote_at_draft':quote,'scope_authorized':True,
            'parameters_user_specified':parameters_user_specified,'specific_order_confirmed':False,
            'submitted':False,'strategy_qualified':False,'planned_notional':str(q*entry),
            'planned_risk_with_buffer':str(q*(entry-stop+cost)),
            'explanation':'Small execution/receipt/protection acceptance test; excluded from strategy performance.'}
        self.book._begin()
        try:
            self.book.db.execute('INSERT INTO engineering_plans VALUES(?,?)',(identity,canonical(payload)))
            self.book.audit.append_in_transaction({'event_id':identity+'-draft','occurred_at':at,
                'event_type':'ENGINEERING_TEST_PLAN','actor':'script','synthetic':self.book.simulation,
                'symbol':plan['symbol'],'quantity':str(q),'price':plan['entry_ceiling'],'stop':plan['stop'],
                'trade_id':identity,'details':payload})
            self.book.db.commit();return payload
        except BaseException:self.book.db.rollback();raise
