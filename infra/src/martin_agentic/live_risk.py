"""Durable account risk stops, shared across live session databases.

Unknown cash flows freeze new risk instead of pretending they are trading P&L.
Neither an equity recovery nor a new session unlocks a project drawdown stop.
"""
from decimal import Decimal
import json
from pathlib import Path
import sqlite3
from zoneinfo import ZoneInfo

from .audit import canonical, digest
from .ledger import timestamp
from .risk import number
from .transport import TransportError


class LiveRiskGuard:
    def __init__(self,path,policy,*,simulation=False):
        self.policy=policy;self.simulation=simulation
        terms={k:policy[k] for k in ('account_suffix','account_capital_usd',
            'stop_new_entries_at_daily_equity_loss_usd','project_max_drawdown_fraction')}
        self.terms=digest(terms)
        if not 0<number(policy['project_max_drawdown_fraction'])<1 or number(policy['account_capital_usd'])<=0 or number(policy['stop_new_entries_at_daily_equity_loss_usd'])<=0:
            raise ValueError('INVALID_LIVE_ACCOUNT_RISK_POLICY')
        self.db=sqlite3.connect(path,timeout=10)
        self.db.execute('CREATE TABLE IF NOT EXISTS account_risk(id INTEGER PRIMARY KEY CHECK(id=1),payload TEXT NOT NULL)')
        self.db.commit()
        state=self.state()
        if state and (state['terms']!=self.terms or state['simulation']!=simulation):
            self.close();raise ValueError('LIVE_ACCOUNT_RISK_POLICY_CHANGED')

    def close(self):self.db.close()
    def state(self):
        row=self.db.execute('SELECT payload FROM account_risk WHERE id=1').fetchone()
        return json.loads(row[0]) if row else None

    def observe(self,snapshot):
        if snapshot.get('synthetic') is not self.simulation or snapshot.get('collection_complete') is not True or snapshot.get('account_mask')!='••••'+self.policy['account_suffix']:
            raise ValueError('LIVE_RISK_REQUIRES_BOUND_COMPLETE_SNAPSHOT')
        at=timestamp(snapshot['observed_at']);day=at.astimezone(ZoneInfo('America/New_York')).date().isoformat()
        equity=number(snapshot['portfolio']['total_value']);cash=number(snapshot['portfolio']['cash'])
        if equity<=0:raise ValueError('INVALID_LIVE_ACCOUNT_EQUITY')
        fills={}
        for order in snapshot['orders'].values():
            for fill in order['executions']:
                encoded=digest(fill)
                if fill['id'] in fills and fills[fill['id']][0]!=encoded:raise ValueError('LIVE_RISK_EXECUTION_CONFLICT')
                delta=number(fill['quantity'])*number(fill['price'])*(1 if fill['side']=='sell' else -1)-number(fill['fees'])
                fills[fill['id']]=(encoded,delta)
        self.db.execute('BEGIN IMMEDIATE')
        try:
            state=self.state();reasons=[]
            if state:
                if state['account_key']!=snapshot['account_key']:
                    raise ValueError('LIVE_RISK_ACCOUNT_OR_CLOCK_CHANGED')
                if at<timestamp(state['at']):
                    raise TransportError('BROKER_SNAPSHOT_SUPERSEDED')
                if at==timestamp(state['at']) and state['snapshot_hash']==digest(snapshot):
                    self.db.rollback();return state
                expected=number(state['cash'])
                for identity,(fp,delta) in fills.items():
                    old=state['executions'].get(identity)
                    if old and old!=fp:raise ValueError('LIVE_RISK_EXECUTION_CONFLICT')
                    if not old:expected+=delta
                if abs(cash-expected)>Decimal('0.01'):reasons.append('UNRECONCILED_CASH_FLOW')
            else:
                capital=number(self.policy['account_capital_usd'])
                state={'terms':self.terms,'simulation':self.simulation,'account_key':snapshot['account_key'],
                    'units':str(capital),'nav_high':'1','project_halted':False,'daily_halted':False,
                    'day':day,'day_start_equity':str(capital),'executions':{},'sticky_reasons':[]}
                if abs(equity-capital)>Decimal('0.01'):reasons.append('INITIAL_CAPITAL_NOT_RECONCILED')
            if number(snapshot['portfolio']['pending_deposits'])!=0:reasons.append('PENDING_CASH_FLOW')
            # A detected unexplained transfer cannot be washed away by the next mark.
            state['sticky_reasons']=sorted(set(state['sticky_reasons']+reasons))
            if state['day']!=day:
                state.update(day=day,day_start_equity=state['equity'],daily_halted=False)
            if not state['sticky_reasons']:
                nav=equity/number(state['units']);high=max(number(state['nav_high']),nav)
                state['nav_high']=str(high);state['nav']=str(nav);state['drawdown']=str(1-nav/high)
                state['project_halted']=state['project_halted'] or 1-nav/high>=number(self.policy['project_max_drawdown_fraction'])
            state['daily_halted']=state['daily_halted'] or equity<=number(state['day_start_equity'])-number(self.policy['stop_new_entries_at_daily_equity_loss_usd'])
            reasons=list(state['sticky_reasons'])
            if state['project_halted']:reasons.append('PROJECT_DRAWDOWN_LOCKED')
            if state['daily_halted']:reasons.append('DAILY_EQUITY_LOSS_LOCKED')
            state.update(at=at.isoformat(),cash=str(cash),equity=str(equity),
                snapshot_hash=digest(snapshot),blocked=bool(reasons),reasons=sorted(set(reasons)))
            state['executions'].update({k:fp for k,(fp,_) in fills.items()})
            self.db.execute('INSERT INTO account_risk VALUES(1,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload',(canonical(state),))
            self.db.commit();return state
        except BaseException:self.db.rollback();raise
