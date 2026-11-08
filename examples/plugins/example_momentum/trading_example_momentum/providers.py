"""Two deliberately different synthetic rules behind the public plugin API."""
from datetime import timedelta
from decimal import Decimal

from trading_contracts.strategy import (
    ProposalAction, StrategyDecision, StrategyProposal,
)
from trading_infra.plugin_protocol import PluginDescriptor

_SCHEMA = {
    'type': 'object',
    'additionalProperties': False,
    'required': [],
    'properties': {
        'lookback': {'type': 'integer', 'minimum': 1, 'maximum': 20},
        'threshold_bps': {'type': 'integer', 'minimum': 0, 'maximum': 1000},
    },
}


class _Base:
    PLUGIN_ID = None
    VERSION = '1.0.0'
    RULE = None

    def __init__(self, config):
        self.lookback = int(config.get('lookback', 3))
        self.threshold_bps = int(config.get('threshold_bps', 0))

    def _checkpoint(self, request):
        checkpoint = dict(request.checkpoint)
        prices = dict(checkpoint.get('prices') or {})
        event = request.market.get('event') or {}
        data = event.get('data') or {}
        symbol = data.get('symbol', 'NONE')
        bid, ask = Decimal(str(data['bid'])), Decimal(str(data['ask']))
        history = list(prices.get(symbol) or [])
        history.append(str((bid + ask) / 2))
        prices[symbol] = history[-self.lookback:]
        average = sum(Decimal(value) for value in history[-self.lookback:]) / len(history[-self.lookback:])
        return checkpoint, prices, symbol, bid, ask, average

    def decide(self, request):
        checkpoint, prices, symbol, bid, ask, average = self._checkpoint(request)
        in_position = bool(checkpoint.get('in_position'))
        execution = request.allocation.get('execution') or {}
        confirmed = Decimal(str(execution.get('confirmed_quantity', '0')))
        if execution.get('lifecycle') in {'not_executed', 'flat_verified'} and confirmed <= 0:
            in_position = False
        elif confirmed > 0:
            in_position = True
        proposals = []
        reason = 'HOLD'
        if not in_position:
            threshold = Decimal(self.threshold_bps) / Decimal(10000)
            if self.RULE == 'momentum' and bid >= average * (1 + threshold):
                proposals.append(self._entry(request, symbol, bid, ask, average,
                                             'MOMENTUM_ABOVE_AVERAGE'))
                in_position = True
            elif self.RULE == 'mean_reversion' and bid <= average * (1 - threshold):
                proposals.append(self._entry(request, symbol, bid, ask, average,
                                             'MEAN_REVERSION_BELOW_AVERAGE'))
                in_position = True
        next_checkpoint = {'prices': prices, 'in_position': in_position,
                           self._feature_key(): {symbol: str(average)}}
        return StrategyDecision(
            identity=request.identity,
            decision_id='decision-' + request.cursor,
            as_of=request.as_of,
            data_snapshot_id=request.data_snapshot_id,
            consumed_cursor=request.cursor,
            synthetic=request.synthetic,
            proposals=tuple(proposals),
            next_checkpoint=next_checkpoint,
            diagnostics={'rule': self.RULE, 'feature': self._feature_key()})

    def _entry(self, request, symbol, bid, ask, average, reason):
        stop = (bid * Decimal('0.98')).quantize(Decimal('0.01'))
        entry = (ask * Decimal('1.01')).quantize(Decimal('0.01'))
        return StrategyProposal(
            identity=request.identity,
            proposal_id='entry-' + request.cursor,
            decision_id='decision-' + request.cursor,
            setup_id='setup-' + request.cursor,
            data_snapshot_id=request.data_snapshot_id,
            source_sha256=request.source_sha256,
            generated_at=request.as_of,
            valid_until=request.as_of + timedelta(seconds=20),
            action=ProposalAction.ENTER, symbol=symbol, reason=reason,
            synthetic=request.synthetic, quantity_limit=1,
            entry_ceiling=entry, stop_price=stop)

    def _feature_key(self):
        return 'close_ma' if self.RULE == 'momentum' else 'entry_anchor'


class _Momentum(_Base):
    PLUGIN_ID = 'example-momentum'
    RULE = 'momentum'


class _MeanReversion(_Base):
    PLUGIN_ID = 'example-mean-reversion'
    RULE = 'mean_reversion'


class MomentumProvider:
    @staticmethod
    def describe():
        return PluginDescriptor(
            plugin_id='example-momentum', version='1.0.0',
            state_schema='example-momentum-1', config_schema=_SCHEMA,
            data_requirements=('quotes',), capabilities=('advisory-entry',),
            feature_keys=('close_ma',),
            exit_rule='trading_example_momentum.rules:momentum_exit')

    @staticmethod
    def create(config):
        return _Momentum(config)


class MeanReversionProvider:
    @staticmethod
    def describe():
        return PluginDescriptor(
            plugin_id='example-mean-reversion', version='1.0.0',
            state_schema='example-mean-reversion-1', config_schema=_SCHEMA,
            data_requirements=('quotes',), capabilities=('advisory-entry',),
            feature_keys=('entry_anchor',),
            exit_rule='trading_example_momentum.rules:mean_reversion_exit')

    @staticmethod
    def create(config):
        return _MeanReversion(config)
