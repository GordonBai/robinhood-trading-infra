"""Persistent deterministic strategy signals, with no simulated order backend."""
import json
from dataclasses import dataclass

from martin_agentic.audit import canonical, digest
from martin_agentic.ledger import timestamp
from martin_agentic.risk import number
from .strategy import PullbackMachine
from trading_contracts.instance import config_version
from trading_contracts.strategy import (PreparedSignalInput, ProposalAction, StrategyDecision,
                                        StrategyIdentity, StrategyInput)
from trading_contracts.values import thaw
from trading_contracts.market import bar_observation_identity
from trading_infra.market_calendar import ExchangeCalendarView
from trading_infra.sizing import size_entry
from trading_infra.strategy_host import evaluate_strategy
from trading_strategies.martin.signals import MartinSignalPlugin
from martin_agentic.capture_archive import ensure_archive_schema, archive_event, archive_decision


def filter_unchanged_bars(bars,state):
    """Only omit exact economic matches already durably consumed by the owner.

    Changed bars must reach the owner so its revision gate can reject them.
    Raw provider responses are archived before this collection optimization.

    ``state`` may also be a :class:`CollectionView`. That is the multi-instance
    form: a bar is omitted only when *every* consumer already holds the same
    content, so one instance being ahead cannot starve another.
    """
    if hasattr(state,'should_emit'):
        changed=[];omitted=0
        for data in bars:
            if state.should_emit(data):changed.append(data)
            else:omitted+=1
        return changed,omitted
    known=state.get('bar_observations',{})
    changed=[];omitted=0
    for data in bars:
        key,fingerprint=bar_observation_identity(data)
        if known.get(key)==fingerprint:omitted+=1
        else:changed.append(data)
    return changed,omitted


class LiveSignals:
    def __init__(self,book,*,cost_buffer_per_share='0.05',persist_contract_metadata=False):
        if type(persist_contract_metadata) is not bool:raise ValueError('STRATEGY_METADATA_FLAG_INVALID')
        self.persist_contract_metadata=persist_contract_metadata
        self.book=book;self.cost=number(cost_buffer_per_share)
        if self.cost<0:raise ValueError('INVALID_COST_BUFFER')
        self.plugin=MartinSignalPlugin(calendar_view=ExchangeCalendarView(),cost_buffer_per_share=str(self.cost))
        self.identity=StrategyIdentity('martin-legacy-default','martin',PullbackMachine.VERSION)
        self.config_version=config_version(self.identity.plugin_id,self.identity.version,
            {'cost_buffer_per_share':str(self.cost)})
        book.db.executescript('''CREATE TABLE IF NOT EXISTS live_strategy(id INTEGER PRIMARY KEY CHECK(id=1),payload TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS live_inputs(id TEXT PRIMARY KEY,fingerprint TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS bar_observation_index(bar_key TEXT PRIMARY KEY,fingerprint TEXT NOT NULL,observed_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS bar_observation_meta(id INTEGER PRIMARY KEY CHECK(id=1),universe_date TEXT);''')
        ensure_archive_schema(book.db)
        if not book.db.execute('SELECT 1 FROM live_strategy').fetchone():
            book.db.execute('INSERT INTO live_strategy VALUES(1,?)',(canonical({'machines':{},'ema9':{},'universe_complete':False}),));book.db.commit()

    def state(self):return json.loads(self.book.db.execute('SELECT payload FROM live_strategy WHERE id=1').fetchone()[0])

    # -- per-bar observation index -------------------------------------
    # The strategy never reads these fingerprints; only the collector de-dup
    # below does. Keeping them out of the request means the payload that
    # crosses the process boundary stops growing with every consumed bar, while
    # the persisted state keeps the exact same content.
    def _index_date(self,db):
        row=db.execute('SELECT universe_date FROM bar_observation_meta WHERE id=1').fetchone()
        return row[0] if row else None

    def _index_snapshot(self,db):
        return {key:fingerprint for key,fingerprint in
                db.execute('SELECT bar_key,fingerprint FROM bar_observation_index ORDER BY bar_key')}

    def _clear_index(self,db,date):
        db.execute('DELETE FROM bar_observation_index')
        db.execute('INSERT INTO bar_observation_meta(id,universe_date) VALUES(1,?)'
                   ' ON CONFLICT(id) DO UPDATE SET universe_date=excluded.universe_date',(date,))

    def _record_bar_observation(self,db,event,state,now):
        """Mirror the plugin's own record/reset rule in the container the host owns."""
        target=state.get('universe_date')
        if target!=self._index_date(db):self._clear_index(db,target)
        if event.get('kind')=='bar':
            key,fingerprint=bar_observation_identity(event['data'])
            db.execute('INSERT INTO bar_observation_index(bar_key,fingerprint,observed_at) VALUES(?,?,?)'
                       ' ON CONFLICT(bar_key) DO UPDATE SET fingerprint=excluded.fingerprint,'
                       ' observed_at=excluded.observed_at',(key,fingerprint,now.isoformat()))

    def prepare(self,event):
        """Pin a detached input without holding an execution write transaction."""
        now=self.book.clock()
        if not isinstance(event,dict):raise ValueError('STRATEGY_EVENT_OBJECT_REQUIRED')
        if event.get('synthetic_input') is not self.book.simulation or not event.get('source_sha256'):
            raise ValueError('STRATEGY_EVENT_PROVENANCE_REQUIRED')
        if not 0<=(now-timestamp(event['at'])).total_seconds()<=30:raise ValueError('STALE_STRATEGY_EVENT')
        if self.book.db.in_transaction:raise ValueError('STRATEGY_PREPARE_REQUIRES_IDLE_TRANSACTION')
        event=json.loads(canonical(event));fingerprint=digest(event)
        old=self.book.db.execute('SELECT fingerprint FROM live_inputs WHERE id=?',(event['id'],)).fetchone()
        if old:
            if old[0]!=fingerprint:raise ValueError('STRATEGY_EVENT_ID_CONFLICT')
            return None
        state=self.state()
        checkpoint={key:value for key,value in state.items()
                    if key not in ('pending_signals','signal_results','bar_observations')}
        request=StrategyInput(identity=self.identity,as_of=now,cursor=event['id'],
            data_snapshot_id='event-'+fingerprint,source_sha256=fingerprint,
            config_version=self.config_version,
            synthetic=self.book.simulation,market={'event':event},checkpoint=checkpoint)
        return PreparedSignalInput(request,digest(checkpoint))

    def consume(self,event):
        prepared=self.prepare(event)
        if prepared is None:return None
        decision=evaluate_strategy(self.plugin,prepared.request)
        return self.commit(prepared,decision)

    def commit(self,prepared,decision):
        """Owner-only atomic CAS of checkpoint, consumed event and private outbox.

        A delayed worker cannot overwrite another decision or resurrect host
        signal dispositions. No strategy code executes under this transaction.
        """
        if not isinstance(prepared,PreparedSignalInput):raise ValueError('STRATEGY_PREPARED_INPUT_REQUIRED')
        request=prepared.request
        if not isinstance(request,StrategyInput) or not isinstance(decision,StrategyDecision):
            raise ValueError('STRATEGY_HOST_DECISION_CONTRACT_REQUIRED')
        if (request.identity!=self.identity or request.synthetic is not self.book.simulation
                or request.config_version!=self.config_version):
            raise ValueError('STRATEGY_PREPARED_CONFIG_CHANGED')
        decision.validate_for(request)
        event=thaw(request.market['event']);fingerprint=digest(event);now=self.book.clock()
        if (fingerprint!=request.source_sha256 or event['id']!=request.cursor
                or digest(thaw(request.checkpoint))!=prepared.checkpoint_fingerprint):
            raise ValueError('STRATEGY_PREPARED_INPUT_CHANGED')
        if not 0<=(now-timestamp(event['at'])).total_seconds()<=30:raise ValueError('STALE_STRATEGY_EVENT')
        if now<request.as_of or any(now>=p.valid_until for p in decision.proposals):
            raise ValueError('STRATEGY_DECISION_EXPIRED')
        db=self.book.db;self.book._begin()
        try:
            old=db.execute('SELECT fingerprint FROM live_inputs WHERE id=?',(event['id'],)).fetchone()
            if old:
                if old[0]!=fingerprint:raise ValueError('STRATEGY_EVENT_ID_CONFLICT')
                db.rollback();return None
            state=self.state();before_state=json.loads(canonical(state));signal=None
            host_state={key:state[key] for key in ('pending_signals','signal_results') if key in state}
            checkpoint={key:value for key,value in state.items()
                        if key not in host_state and key!='bar_observations'}
            if digest(checkpoint)!=prepared.checkpoint_fingerprint:
                raise ValueError('STRATEGY_CHECKPOINT_CHANGED_RETRY_REQUIRED')
            if {'pending_signals','signal_results'} & set(decision.next_checkpoint):
                raise ValueError('STRATEGY_CHECKPOINT_CANNOT_CONTAIN_HOST_OUTBOX')
            state=thaw(decision.next_checkpoint);state.pop('bar_observations',None);state.update(host_state)
            decision_reason=decision.diagnostics['reason'];screen=thaw(decision.diagnostics.get('screening'))
            if len(decision.proposals)>1:raise ValueError('LEGACY_HOST_SINGLE_ENTRY_PROPOSAL_REQUIRED')
            for proposal in decision.proposals:
                if proposal.action is not ProposalAction.ENTER:raise ValueError('LEGACY_HOST_ENTRY_PROPOSAL_REQUIRED')
                quantity=size_entry(entry_ceiling=proposal.entry_ceiling,stop=proposal.stop_price,
                    cost_buffer_per_share=self.cost,
                    max_position_notional=self.book.policy['max_position_notional_usd'],
                    max_planned_loss=self.book.policy['max_planned_loss_per_trade_usd'],
                    quantity_limit=proposal.quantity_limit)
                decision_reason='SIGNAL_CREATED' if quantity>0 else 'ZERO_AFFORDABLE_QUANTITY'
                if quantity>0:
                    evidence=thaw(proposal.evidence)
                    signal={'id':evidence['legacy_signal_id'],
                        'plan':{'symbol':proposal.symbol,'quantity':quantity,'entry_ceiling':str(proposal.entry_ceiling),
                            'stop':str(proposal.stop_price),'cost_buffer_per_share':str(self.cost)},
                        'quote':event['data'],
                        'evidence':{'source_sha256':evidence['source_sha256'],'synthetic':self.book.simulation,
                            'strategy_version':evidence['strategy_version'],'setup':evidence['setup'],
                            'triggered_at':evidence['triggered_at'],'source_event_id':evidence['source_event_id']}}
                    if self.persist_contract_metadata:
                        signal['evidence'].update(strategy_proposal_id=proposal.proposal_id,
                            strategy_decision_id=decision.decision_id,strategy_instance_id=proposal.identity.instance_id,
                            strategy_generated_at=proposal.generated_at.isoformat(),
                            strategy_valid_until=proposal.valid_until.isoformat())
            # A returned signal remains in durable strategy state until the caller enqueues/declines it.
            if signal:state.setdefault('pending_signals',{})[signal['id']]=signal
            # BEGIN IMMEDIATE/audit verification may have waited. Re-read the
            # clock under the acquired lock, immediately before durable writes;
            # the pre-lock check alone cannot establish current validity.
            now=self.book.clock()
            if not 0<=(now-timestamp(event['at'])).total_seconds()<=30:
                raise ValueError('STALE_STRATEGY_EVENT')
            if now<request.as_of or any(now>=p.valid_until for p in decision.proposals):
                raise ValueError('STRATEGY_DECISION_EXPIRED')
            self._record_bar_observation(db,event,state,now)
            state['bar_observations']=self._index_snapshot(db)
            db.execute('UPDATE live_strategy SET payload=? WHERE id=1',(canonical(state),))
            archive_event(db,event,consumed_at=now.isoformat())
            archive_decision(db,event_id=event['id'],result_id='consumed',recorded_at=now.isoformat(),
                context={'status':'CONSUMED','reason':decision_reason,'strategy_version':PullbackMachine.VERSION,
                    'state_before':before_state,'state_after':state,'screening':screen,'signal':signal})
            db.execute('INSERT INTO live_inputs VALUES(?,?)',(event['id'],fingerprint))
            # Archive serialization may itself be slow. Do not publish a result
            # that expired while assembling its atomic transaction either.
            committed_at=self.book.clock()
            if not 0<=(committed_at-timestamp(event['at'])).total_seconds()<=30:
                raise ValueError('STALE_STRATEGY_EVENT')
            if committed_at<request.as_of or any(committed_at>=p.valid_until for p in decision.proposals):
                raise ValueError('STRATEGY_DECISION_EXPIRED')
            db.commit();return signal
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
