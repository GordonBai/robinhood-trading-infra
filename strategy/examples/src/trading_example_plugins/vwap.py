"""A second, quotes-only plugin family used to prove composition selection.

It is a fixture, not a trading strategy. It tracks a session VWAP from its own
quote stream and may emit one legal entry per event stream; it needs no Martin
universe/screening/bar bootstrap and still runs through the configured
production worker path. The exit feature remains owned by this plugin's
checkpoint and is selected by plugin_id, never imported by the execution core.
"""
from datetime import timedelta
from decimal import Decimal

from trading_contracts.strategy import (ProposalAction, StrategyDecision,
                                         StrategyProposal)


class VwapFixturePlugin:
    PLUGIN_ID = 'vwap-fixture'
    VERSION = '1.0'

    def __init__(self, *, cost_buffer_per_share='0.05'):
        self.cost = cost_buffer_per_share

    def decide(self, request):
        checkpoint = dict(request.checkpoint)
        seen = dict(checkpoint.get('seen') or {})
        event = request.market.get('event') or {}
        data = event.get('data') or {}
        symbol = data.get('symbol', 'NONE')
        seen[symbol] = seen.get(symbol, 0) + 1
        # The fixture's own exit feature. It is computed only from this
        # instance's quote stream and stored in this instance's checkpoint; the
        # infrastructure reads it by the declared feature key, never by name.
        next_checkpoint = {'seen': seen}
        try:
            bid = Decimal(str(data['bid']))
            ask = Decimal(str(data['ask']))
            if 0 < bid <= ask:
                if checkpoint.get('session_vwap_symbol') == symbol:
                    total = Decimal(str(checkpoint.get('session_vwap_total', '0')))
                    count = int(checkpoint.get('session_vwap_count', 0))
                else:
                    total = Decimal('0')
                    count = 0
                total += (bid + ask) / 2
                count += 1
                next_checkpoint.update(
                    session_vwap=str((total / count).quantize(
                        Decimal('0.01'))),
                    session_vwap_total=str(total),
                    session_vwap_count=count,
                    session_vwap_symbol=symbol)
        except (KeyError, TypeError, ValueError, ArithmeticError):
            pass
        proposals = ()
        if seen[symbol] == 1:
            # Engineering fixture: one legal entry per event stream so the
            # default composition has a strategy action to hand to the host.
            proposals = (StrategyProposal(
                identity=request.identity,
                proposal_id='vwap-proposal-' + request.cursor,
                decision_id='vwap-' + request.cursor,
                setup_id='vwap-setup-' + request.cursor,
                data_snapshot_id=request.data_snapshot_id,
                source_sha256=request.source_sha256,
                generated_at=request.as_of,
                valid_until=request.as_of + timedelta(seconds=20),
                action=ProposalAction.ENTER, symbol=symbol,
                reason='VWAP_FIXTURE_ENTRY', synthetic=request.synthetic,
                quantity_limit=1, entry_ceiling=Decimal('40.00'),
                stop_price=Decimal('39.20')),)
        return StrategyDecision(
            identity=request.identity, decision_id='vwap-' + request.cursor,
            as_of=request.as_of, data_snapshot_id=request.data_snapshot_id,
            consumed_cursor=request.cursor, synthetic=request.synthetic,
            proposals=proposals, next_checkpoint=next_checkpoint,
            diagnostics={'fixture': True, 'needs': 'quotes',
                         'session_vwap': next_checkpoint.get('session_vwap')})
