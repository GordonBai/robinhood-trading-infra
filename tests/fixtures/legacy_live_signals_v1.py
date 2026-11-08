# Frozen pre-isolation persistence/algorithm reference for synthetic parity only.
"""Persistent deterministic strategy signals, with no simulated order backend."""
from decimal import Decimal, ROUND_FLOOR
import json

from martin_agentic.audit import canonical, digest
from martin_luk.features import build_candidates
from martin_agentic.ledger import timestamp
from martin_agentic.risk import number
from martin_luk.strategy import PullbackMachine
from martin_agentic.calendar import session
from martin_agentic.capture_archive import ensure_archive_schema, archive_event, archive_decision


def bar_observation_identity(data):
    """Economic bar contents; receipt time and response index are not revisions."""
    key=digest([data['symbol'],timestamp(data['begins_at']).isoformat()])
    contents={name:str(number(data[name]).normalize()) for name in ('open','high','low','close','volume')}
    contents.update({name:data.get(name) for name in ('interval','bounds','adjustment')})
    return key,digest(contents)


def filter_unchanged_bars(bars,state):
    """Only omit exact economic matches already durably consumed by the owner.

    Changed bars must reach the owner so its revision gate can reject them.
    Raw provider responses are archived before this collection optimization.
    """
    known=state.get('bar_observations',{})
    changed=[];omitted=0
    for data in bars:
        key,fingerprint=bar_observation_identity(data)
        if known.get(key)==fingerprint:omitted+=1
        else:changed.append(data)
    return changed,omitted


class LiveSignals:
    def __init__(self,book,*,cost_buffer_per_share='0.05'):
        self.book=book;self.cost=number(cost_buffer_per_share)
        if self.cost<0:raise ValueError('INVALID_COST_BUFFER')
        book.db.executescript('''CREATE TABLE IF NOT EXISTS live_strategy(id INTEGER PRIMARY KEY CHECK(id=1),payload TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS live_inputs(id TEXT PRIMARY KEY,fingerprint TEXT NOT NULL);''')
        ensure_archive_schema(book.db)
        if not book.db.execute('SELECT 1 FROM live_strategy').fetchone():
            book.db.execute('INSERT INTO live_strategy VALUES(1,?)',(canonical({'machines':{},'ema9':{},'universe_complete':False}),));book.db.commit()

    def state(self):return json.loads(self.book.db.execute('SELECT payload FROM live_strategy WHERE id=1').fetchone()[0])

    def consume(self,event):
        now=self.book.clock()
        if not isinstance(event,dict):raise ValueError('STRATEGY_EVENT_OBJECT_REQUIRED')
        if event.get('synthetic_input') is not self.book.simulation or not event.get('source_sha256'):
            raise ValueError('STRATEGY_EVENT_PROVENANCE_REQUIRED')
        if not 0<=(now-timestamp(event['at'])).total_seconds()<=30:raise ValueError('STALE_STRATEGY_EVENT')
        fingerprint=digest(event);db=self.book.db;self.book._begin()
        try:
            old=db.execute('SELECT fingerprint FROM live_inputs WHERE id=?',(event['id'],)).fetchone()
            if old:
                if old[0]!=fingerprint:raise ValueError('STRATEGY_EVENT_ID_CONFLICT')
                db.rollback();return None
            state=self.state();before_state=json.loads(canonical(state));signal=None;kind=event['kind']
            decision_reason='NO_NEW_TRIGGER';screen=None
            if kind=='universe':
                data=event['data'];screen=build_candidates(data['histories'],data['indicators'],as_of=event['at'],universe_id=data['universe_id'])
                decision_reason='UNIVERSE_SCREENED'
                market=session(event['at'])
                if state.get('universe_date')!=(market['date'] if market else None):
                    state['bar_observations']={}
                state['universe_date']=market['date'] if market else None
                state['universe_complete']=screen['universe_complete']
                state['screening_summary']={'universe_id':data['universe_id'],'candidate_count':len(screen['candidates']),'rejected':screen['rejected']}
                state['ema9']={s:p['9']['value'] for s,p in data['indicators'].items() if '9' in p}
                state['indicator_values']={s:{k:p[k] for k in ('9','21','50') if k in p} for s,p in data['indicators'].items()}
                for setup in screen['candidates']:
                    old=state['machines'].get(setup['symbol'])
                    if not old or timestamp(old['setup']['known_at']).date()!=timestamp(setup['known_at']).date():
                        state['machines'][setup['symbol']]=PullbackMachine(setup).checkpoint()
            elif kind in {'bar','quote'}:
                data=event['data'];cp=state['machines'].get(data['symbol'])
                if kind=='bar':
                    key,bar_fingerprint=bar_observation_identity(data)
                    # This cache is a collection optimization, not a new market
                    # data admission policy. Existing machine revision checks
                    # below still run; failures roll this update back as well.
                    state.setdefault('bar_observations',{})[key]=bar_fingerprint
                if not cp:decision_reason='NO_QUALIFIED_SETUP'
                elif not state['universe_complete']:decision_reason='UNIVERSE_INCOMPLETE'
                if cp and state['universe_complete']:
                    machine=PullbackMachine.restore(cp);before=machine.state
                    if kind=='bar':
                        start=timestamp(data['begins_at'])
                        # Polling overlapping windows is normal. Previously consumed bars are immutable in price/volume.
                        if start>=machine.known_at and (machine.last_start is None or start>machine.last_start):
                            machine.on_bar(data,as_of=event['at'])
                        elif machine.last_start==start:
                            keys=('symbol','begins_at','open','high','low','close','volume')
                            if any(data[k]!=machine.last_bar[k] for k in keys):raise ValueError('CONSUMED_BAR_REVISED')
                    else:machine.on_quote(data,as_of=event['at'])
                    state['machines'][machine.symbol]=machine.checkpoint()
                    if before!='TRIGGERED' and machine.state=='TRIGGERED':
                        ceiling=min(number(data['ask'])+machine.tick,machine.trigger*Decimal('1.002'))
                        ceiling=(ceiling/machine.tick).to_integral_value(rounding=ROUND_FLOOR)*machine.tick
                        quantity=int(min(number(self.book.policy['max_position_notional_usd'])/ceiling,
                            number(self.book.policy['max_planned_loss_per_trade_usd'])/(ceiling-machine.stop+self.cost)).to_integral_value(rounding=ROUND_FLOOR))
                        decision_reason='SIGNAL_CREATED' if quantity>0 else 'ZERO_AFFORDABLE_QUANTITY'
                        if quantity>0:
                            signal={'id':'signal-'+digest([machine.VERSION,machine.symbol,machine.known_at.isoformat()]),
                                'plan':{'symbol':machine.symbol,'quantity':quantity,'entry_ceiling':str(ceiling),'stop':str(machine.stop),
                                    'cost_buffer_per_share':str(self.cost)},'quote':data,
                                'evidence':{'source_sha256':event['source_sha256'],'synthetic':self.book.simulation,
                                    'strategy_version':machine.VERSION,'setup':machine.setup,'triggered_at':event['at'],
                                    'source_event_id':event['id']}}
            elif kind=='data_status':state['universe_complete']=state['universe_complete'] and event['healthy']
            else:raise ValueError('UNSUPPORTED_LIVE_STRATEGY_EVENT')
            # A returned signal remains in durable strategy state until the caller enqueues/declines it.
            if signal:state.setdefault('pending_signals',{})[signal['id']]=signal
            db.execute('UPDATE live_strategy SET payload=? WHERE id=1',(canonical(state),))
            archive_event(db,event,consumed_at=now.isoformat())
            archive_decision(db,event_id=event['id'],result_id='consumed',recorded_at=now.isoformat(),
                context={'status':'CONSUMED','reason':decision_reason,'strategy_version':PullbackMachine.VERSION,
                    'state_before':before_state,'state_after':state,'screening':screen,'signal':signal})
            db.execute('INSERT INTO live_inputs VALUES(?,?)',(event['id'],fingerprint));db.commit();return signal
        except BaseException:db.rollback();raise

    def pending(self):return list(self.state().get('pending_signals',{}).values())
    def complete(self,signal_id,*,result):
        db=self.book.db;self.book._begin()
        try:
            state=self.state();signal=state.get('pending_signals',{}).pop(signal_id,None)
            if signal:
                state.setdefault('signal_results',{})[signal_id]=result
                source_event=signal.get('evidence',{}).get('source_event_id')
                if source_event:
                    archive_decision(db,event_id=source_event,result_id='signal-result-'+signal_id,
                        recorded_at=self.book.clock().isoformat(),context={'status':'SIGNAL_DISPOSITION',
                            'signal_id':signal_id,'result':result})
            db.execute('UPDATE live_strategy SET payload=? WHERE id=1',(canonical(state),));db.commit()
        except BaseException:db.rollback();raise
