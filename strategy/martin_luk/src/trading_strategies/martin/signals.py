"""Pure Martin signal decisions over an explicit market/checkpoint input.

The host owns event deduplication, final sizing, durable outbox and execution.
This plugin returns advisory prices and a copied algorithm checkpoint only.
"""
from datetime import timedelta
from decimal import Decimal, ROUND_FLOOR

from trading_contracts.market import CalendarView, bar_observation_identity
from trading_contracts.strategy import (
    StrategyInput, StrategyDecision, StrategyProposal, ProposalAction,
)
from trading_contracts.values import number, timestamp, digest, thaw, contract_digest
from .pullback import PullbackMachine
from .screening import build_candidates


class MartinSignalPlugin:
    PLUGIN_ID = 'martin'
    VERSION = PullbackMachine.VERSION

    def __init__(self, *, calendar_view: CalendarView, cost_buffer_per_share='0.05'):
        self.calendar = calendar_view
        self.cost = number(cost_buffer_per_share)
        if self.cost < 0:
            raise ValueError('INVALID_COST_BUFFER')

    def decide(self, request: StrategyInput) -> StrategyDecision:
        if not isinstance(request, StrategyInput):
            raise ValueError('STRATEGY_INPUT_REQUIRED')
        if request.identity.plugin_id != self.PLUGIN_ID or request.identity.version != self.VERSION:
            raise ValueError('MARTIN_PLUGIN_IDENTITY_MISMATCH')
        event = thaw(request.market.get('event'))
        if not isinstance(event, dict):
            raise ValueError('STRATEGY_EVENT_OBJECT_REQUIRED')
        if event.get('synthetic_input') is not request.synthetic or not event.get('source_sha256'):
            raise ValueError('STRATEGY_EVENT_PROVENANCE_REQUIRED')
        if event.get('id') != request.cursor or digest(event) != request.source_sha256:
            raise ValueError('STRATEGY_INPUT_EVENT_BINDING_INVALID')
        if not 0 <= (request.as_of - timestamp(event['at'])).total_seconds() <= 30:
            raise ValueError('STALE_STRATEGY_EVENT')
        state = thaw(request.checkpoint)
        if 'pending_signals' in state or 'signal_results' in state:
            raise ValueError('MARTIN_HOST_OUTBOX_STATE_FORBIDDEN')
        state.setdefault('machines', {})
        state.setdefault('ema9', {})
        state.setdefault('universe_complete', False)
        reason = 'NO_NEW_TRIGGER'
        screen = None
        proposal = None
        legacy_id = None
        # A decision identity binds the complete explicit request and constructor
        # configuration, not merely its market event. Replays with a different
        # checkpoint, time or config must never reuse an old decision identity.
        decision_id = 'decision-' + contract_digest({
            'identity': {
                'instance_id': request.identity.instance_id,
                'plugin_id': request.identity.plugin_id,
                'version': request.identity.version,
            },
            'as_of': request.as_of, 'cursor': request.cursor,
            'data_snapshot_id': request.data_snapshot_id, 'source_sha256': request.source_sha256,
            'config_version': request.config_version, 'synthetic': request.synthetic,
            'market': request.market, 'allocation': request.allocation, 'budget': request.budget,
            'checkpoint': request.checkpoint, 'cost_buffer_per_share': str(self.cost),
        })
        kind = event['kind']
        if kind == 'universe':
            data = event['data']
            screen = build_candidates(data['histories'], data['indicators'], as_of=event['at'],
                universe_id=data['universe_id'], calendar_view=self.calendar)
            reason = 'UNIVERSE_SCREENED'
            market = self.calendar.session(event['at'])
            current_date = market['date'] if market else None
            if state.get('universe_date') != current_date:
                state['bar_observations'] = {}
            state['universe_date'] = current_date
            state['universe_complete'] = screen['universe_complete']
            state['screening_summary'] = {
                'universe_id': data['universe_id'], 'candidate_count': len(screen['candidates']),
                'rejected': screen['rejected'],
            }
            state['ema9'] = {symbol: points['9']['value'] for symbol, points in data['indicators'].items() if '9' in points}
            state['indicator_values'] = {symbol: {key: points[key] for key in ('9', '21', '50') if key in points}
                for symbol, points in data['indicators'].items()}
            for setup in screen['candidates']:
                old = state['machines'].get(setup['symbol'])
                if not old or timestamp(old['setup']['known_at']).date() != timestamp(setup['known_at']).date():
                    state['machines'][setup['symbol']] = PullbackMachine(setup,
                        calendar_view=self.calendar).checkpoint()
        elif kind in {'bar', 'quote'}:
            data = event['data']
            checkpoint = state['machines'].get(data['symbol'])
            if kind == 'bar':
                key, fingerprint = bar_observation_identity(data)
                state.setdefault('bar_observations', {})[key] = fingerprint
            if not checkpoint:
                reason = 'NO_QUALIFIED_SETUP'
            elif not state['universe_complete']:
                reason = 'UNIVERSE_INCOMPLETE'
            if checkpoint and state['universe_complete']:
                machine = PullbackMachine.restore(checkpoint, calendar_view=self.calendar)
                before = machine.state
                if kind == 'bar':
                    start = timestamp(data['begins_at'])
                    if start >= machine.known_at and (machine.last_start is None or start > machine.last_start):
                        machine.on_bar(data, as_of=event['at'])
                    elif machine.last_start == start:
                        keys = ('symbol', 'begins_at', 'open', 'high', 'low', 'close', 'volume')
                        if any(data[key] != machine.last_bar[key] for key in keys):
                            raise ValueError('CONSUMED_BAR_REVISED')
                else:
                    machine.on_quote(data, as_of=event['at'])
                state['machines'][machine.symbol] = machine.checkpoint()
                if before != 'TRIGGERED' and machine.state == 'TRIGGERED':
                    ceiling = min(number(data['ask']) + machine.tick, machine.trigger * Decimal('1.002'))
                    ceiling = (ceiling / machine.tick).to_integral_value(rounding=ROUND_FLOOR) * machine.tick
                    legacy_id = 'signal-' + digest([machine.VERSION, machine.symbol, machine.known_at.isoformat()])
                    proposal = StrategyProposal(
                        identity=request.identity,
                        proposal_id='proposal-' + digest([request.identity.instance_id, decision_id, legacy_id]),
                        decision_id=decision_id, setup_id='setup-' + digest(machine.setup),
                        data_snapshot_id=request.data_snapshot_id, source_sha256=request.source_sha256,
                        generated_at=request.as_of, valid_until=request.as_of + timedelta(seconds=15),
                        action=ProposalAction.ENTER, symbol=machine.symbol, reason='SIGNAL_CREATED',
                        synthetic=request.synthetic, entry_ceiling=ceiling, stop_price=machine.stop,
                        evidence={
                            'legacy_signal_id': legacy_id, 'source_sha256': event['source_sha256'],
                            'source_event_id': event['id'], 'strategy_version': machine.VERSION,
                            'setup': machine.setup, 'triggered_at': event['at'],
                            'cost_buffer_per_share': str(self.cost),
                        },
                    )
                    reason = 'SIGNAL_CREATED'
        elif kind == 'data_status':
            state['universe_complete'] = state['universe_complete'] and event['healthy']
        else:
            raise ValueError('UNSUPPORTED_LIVE_STRATEGY_EVENT')
        return StrategyDecision(
            identity=request.identity, decision_id=decision_id, as_of=request.as_of,
            data_snapshot_id=request.data_snapshot_id, consumed_cursor=request.cursor,
            synthetic=request.synthetic, proposals=(proposal,) if proposal else (),
            next_checkpoint=state,
            diagnostics={'reason': reason, 'screening': screen, 'legacy_signal_id': legacy_id},
        ).validate_for(request)
