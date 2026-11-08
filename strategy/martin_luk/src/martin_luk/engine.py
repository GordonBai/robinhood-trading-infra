"""Persistent end-to-end shadow engine. Its only execution backend is synthetic.

All strategy/order/portfolio changes and the input event are one SQLite commit.
No live write tool, credential or broker transport is reachable from this engine.
"""

from datetime import timedelta
from decimal import Decimal, ROUND_FLOOR
import hashlib
import json
from pathlib import Path
import sqlite3

from martin_agentic.calendar import is_regular, session
from .exits import exit_decision
from .features import build_candidates
from martin_agentic.ledger import timestamp
from martin_agentic.paper import PaperDesk
from martin_agentic.risk import number
from .strategy import PullbackMachine
from martin_agentic.audit import AuditTrail


class ShadowEngine:
    def __init__(self, path: str | Path):
        self.db=sqlite3.connect(path,timeout=10)
        self.db.executescript('''
          CREATE TABLE IF NOT EXISTS engine_state(id INTEGER PRIMARY KEY CHECK(id=1), payload TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS engine_events(sequence INTEGER PRIMARY KEY AUTOINCREMENT,
             event_id TEXT UNIQUE NOT NULL, fingerprint TEXT NOT NULL, input TEXT NOT NULL, result TEXT NOT NULL);
        ''')
        self.audit=AuditTrail(connection=self.db)

    def close(self):
        self.db.close()

    def snapshot(self):
        row=self.db.execute('SELECT payload FROM engine_state WHERE id=1').fetchone()
        if not row:
            raise ValueError('Engine not initialized')
        return json.loads(row[0])

    def apply(self,event):
        if not isinstance(event,dict) or not isinstance(event.get('id'),str) or not event['id']:
            raise ValueError('Event needs an id')
        now=timestamp(event['at'])
        raw=json.dumps(event,sort_keys=True,separators=(',',':'))
        digest=hashlib.sha256(raw.encode()).hexdigest()
        self.db.execute('BEGIN IMMEDIATE')
        try:
            self.audit.verify()
            prior=self.db.execute('SELECT fingerprint,result FROM engine_events WHERE event_id=?',(event['id'],)).fetchone()
            if prior:
                if prior[0]!=digest:
                    raise ValueError('Event id conflict')
                self.db.rollback()
                return json.loads(prior[1])
            row=self.db.execute('SELECT payload FROM engine_state WHERE id=1').fetchone()
            state=json.loads(row[0]) if row else None
            if state and state.get('day') and 'day_start_nav' not in state:
                # v1 engine had no external flows: units were constant, so the
                # old equity-based day anchor can be migrated without guessing.
                state['day_start_nav']=str(number(state['day_start_equity'])/number(state['paper']['nav']['units']))
            if state and now<timestamp(state['at']):
                raise ValueError('Out-of-order engine event')
            state,actions=self._transition(state,event,now)
            state['at']=event['at']
            state['engine_schema_version']=2
            result={'event_id':event['id'],'actions':actions,'state':state}
            self.db.execute('INSERT INTO engine_state VALUES(1,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload',
                            (json.dumps(state,sort_keys=True),))
            self.db.execute('INSERT INTO engine_events(event_id,fingerprint,input,result) VALUES(?,?,?,?)',
                            (event['id'],digest,raw,json.dumps(result,sort_keys=True)))
            self.audit.append_in_transaction({'event_id':'shadow-'+event['id'],'occurred_at':event['at'],
                'event_type':'SHADOW_TRANSITION','actor':'simulation','synthetic':True,
                'details':{'source_event_id':event['id'],'input_sha256':digest,'input_kind':event['kind'],
                           'actions':actions,'market_input_synthetic':state['synthetic_input']}})
            self.db.commit()
            return result
        except Exception:
            self.db.rollback()
            raise

    @staticmethod
    def _transition(state,event,now):
        kind=event['kind']
        actions=[]
        if state is None:
            if kind!='init' or event.get('mode')!='shadow' or type(event.get('synthetic')) is not bool:
                raise ValueError('Explicit shadow initialization required')
            policy=event['policy']
            if not 0 < number(policy['daily_loss_fraction']) < 1 or type(policy['max_daily_entries']) is not int or policy['max_daily_entries']<1:
                raise ValueError('Explicit shadow daily risk policy required')
            paper,_=PaperDesk._transition(None,{'kind':'init','synthetic':True,'capital':event['capital'],'policy':policy},now)
            return {'mode':'shadow','synthetic_input':event['synthetic'],'paper':paper,'machines':{},'universe':None,
                    'indicators':{},'order_expiries':{},'entries_by_day':{},'day':None,'day_start_equity':None,
                    'data_health':{},'execution_ready':False,'live_orders':0},[{'action':'INITIALIZED'}]
        if kind=='init':
            raise ValueError('Cannot reset an engine')
        paper=state['paper']

        def execute(e):
            nonlocal paper
            paper,outcome=PaperDesk._transition(paper,e,now)
            paper['at']=now.isoformat()
            actions.append({'action':'SIMULATED_'+e['kind'].upper(),'details':outcome})

        # Expired entries and drawdown cancel requests are acknowledged by the
        # synthetic exchange only. Live cancel acknowledgements are not assumed.
        for oid,order in list(paper['orders'].items()):
            expiry=state['order_expiries'].get(oid)
            if order['remaining'] and (order.get('cancel_requested') or expiry and now>=timestamp(expiry)):
                execute({'kind':'cancel','order_id':oid})
        if kind=='universe':
            data=event['data']
            screening=build_candidates(data['histories'],data['indicators'],as_of=event['at'],universe_id=data['universe_id'])
            state['universe']=screening
            state['indicators']=data['indicators']
            # A repeated universe cannot reset an existing setup into a new entry.
            for setup in screening['candidates']:
                old=state['machines'].get(setup['symbol'])
                if old and timestamp(old['setup']['known_at']).date()==now.date():
                    continue
                state['machines'][setup['symbol']]=PullbackMachine(setup).checkpoint()
            actions.append({'action':'SCREEN','candidates':len(screening['candidates']),
                            'rejected':screening['rejected'],'universe_complete':screening['universe_complete']})
        elif kind=='data_status':
            if type(event.get('healthy')) is not bool:
                raise ValueError('Explicit source health required')
            state['data_paused']=not event['healthy']
            state['source_health']={'at':event['at'],'healthy':event['healthy'],'errors':event.get('errors',[])}
            if state['data_paused']:
                for oid,order in list(paper['orders'].items()):
                    if order['remaining']:
                        execute({'kind':'cancel','order_id':oid})
                actions.append({'action':'BLOCKED','reason':'SOURCE_DATA_NOT_FRESH'})
        elif kind=='flow':
            execute({'kind':'flow','amount':event['amount']})
        elif kind=='bar':
            bar=event['data']
            if bar['symbol'] in state['machines']:
                machine=PullbackMachine.restore(state['machines'][bar['symbol']])
                actions.append({'action':'STRATEGY','result':machine.on_bar(bar,as_of=event['at'])})
                state['machines'][bar['symbol']]=machine.checkpoint()
        elif kind=='quote':
            quote=event['data']
            symbol=quote['symbol']
            valid=all(0 <= (now-timestamp(quote[k])).total_seconds() <= 15 for k in ('bid_at','ask_at','observed_at'))
            bid,ask=number(quote['bid']),number(quote['ask'])
            valid=valid and 0<bid<=ask
            state['data_health'][symbol]={'fresh':bool(valid),'at':event['at']}
            if not valid:
                actions.append({'action':'BLOCKED','reason':'STALE_OR_INVALID_QUOTE'})
            elif not is_regular(event['at']):
                actions.append({'action':'BLOCKED','reason':'OUTSIDE_REGULAR_SESSION'})
            else:
                day=session(event['at'])['date']
                if state['day']!=day:
                    state['day'],state['day_start_equity']=day,paper['nav']['equity']
                    state['day_start_nav']=paper['nav']['nav']
                # Fill only on a later quote, at its executable ask; never at a bar high.
                for oid,order in list(paper['orders'].items()):
                    if order['symbol']!=symbol or not order['remaining']:
                        continue
                    if paper['nav']['halted'] or bid<=number(order['stop']):
                        execute({'kind':'cancel','order_id':oid})
                    elif ask<=number(order['limit']):
                        execute({'kind':'fill','order_id':oid,'quantity':order['remaining'],'price':str(ask)})
                        execute({'kind':'protect','symbol':symbol,'quantity':paper['positions'][symbol]['quantity']})
                if symbol in paper['positions']:
                    execute({'kind':'mark','symbol':symbol,'price':str(bid)})
                    point=state['indicators'].get(symbol,{}).get('9')
                    decision=exit_decision(paper['positions'][symbol],quote,as_of=event['at'],
                                           daily_ema9=point['value'] if point else None,
                                           allow_overnight=False)
                    actions.append({'action':'EXIT_DECISION','details':decision})
                    if decision['action']=='EXIT_ALL':
                        for oid,order in paper['orders'].items():
                            if order['symbol']==symbol and order['remaining']:
                                execute({'kind':'cancel','order_id':oid})
                        execute({'kind':'exit','symbol':symbol,'quantity':paper['positions'][symbol]['quantity'],'price':decision['price']})
                    elif decision['action']=='RAISE_STOP':
                        execute({'kind':'raise_stop','symbol':symbol,'stop':decision['stop']})
                daily_loss=1-number(paper['nav']['nav'])/number(state['day_start_nav'])
                allowed=not state.get('data_paused',False) and not paper['nav']['halted'] and daily_loss<number(paper['policy']['daily_loss_fraction'])
                allowed=allowed and (state.get('universe') or {}).get('universe_complete',False)
                # Every held symbol must have a recent mark before adding risk.
                allowed=allowed and all(s in state['data_health'] and
                    (now-timestamp(state['data_health'][s]['at'])).total_seconds()<=15 and state['data_health'][s]['fresh']
                    for s in paper['positions'])
                if symbol in state['machines']:
                    machine=PullbackMachine.restore(state['machines'][symbol])
                    before=machine.state
                    signal=machine.on_quote(quote,as_of=event['at'])
                    state['machines'][symbol]=machine.checkpoint()
                    actions.append({'action':'STRATEGY','result':signal})
                    daily_count=state['entries_by_day'].get(day,0)
                    if before!='TRIGGERED' and machine.state=='TRIGGERED':
                        if allowed and daily_count<paper['policy']['max_daily_entries'] and symbol not in paper['positions']:
                            oid='shadow-'+hashlib.sha256((machine.VERSION+symbol+machine.known_at.isoformat()).encode()).hexdigest()[:24]
                            ceiling=min(ask+machine.tick,machine.trigger*Decimal('1.002'))
                            ceiling=(ceiling/machine.tick).to_integral_value(rounding=ROUND_FLOOR)*machine.tick
                            execute({'kind':'submit','order_id':oid,'symbol':symbol,'entry_ceiling':str(ceiling),
                                     'stop':str(machine.stop),'cost_buffer_per_share':paper['policy']['cost_buffer_per_share']})
                            if oid in paper['orders']:
                                state['order_expiries'][oid]=min(machine.expires,now+timedelta(seconds=60)).isoformat()
                                state['entries_by_day'][day]=daily_count+1
                        else:
                            actions.append({'action':'BLOCKED','reason':'PORTFOLIO_DAILY_OR_DATA_GATE'})
        elif kind=='timer':
            actions.append({'action':'TIMER','note':'No prices invented; existing positions need fresh quotes for exit simulation'})
        else:
            raise ValueError('Unknown engine event')
        # A mark may have tripped drawdown during this same event.
        for oid,order in list(paper['orders'].items()):
            if order['remaining'] and order.get('cancel_requested'):
                execute({'kind':'cancel','order_id':oid})
        state['paper']=paper
        return state,actions
