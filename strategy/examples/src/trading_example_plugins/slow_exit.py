"""Synthetic fixture used only to exercise bounded exit-advisor timeout.

This is not a trading strategy and is never a production-capable plugin. Its
worker publishes a deterministic session feature; its exit rule deliberately
sleeps so the infrastructure timeout path can be observed without replacing
the ExitAdvisoryService or any default composition decision.
"""
import time

from trading_contracts.strategy import StrategyDecision


class SlowExitFixturePlugin:
    PLUGIN_ID = 'slow-exit-fixture'
    VERSION = '1.0'

    def __init__(self, *, cost_buffer_per_share='0.05'):
        self.cost = cost_buffer_per_share

    def decide(self, request):
        data = (request.market.get('event') or {}).get('data') or {}
        symbol = data.get('symbol', 'NONE')
        return StrategyDecision(
            identity=request.identity,
            decision_id='slow-exit-' + request.cursor,
            as_of=request.as_of,
            data_snapshot_id=request.data_snapshot_id,
            consumed_cursor=request.cursor,
            synthetic=request.synthetic,
            proposals=(),
            next_checkpoint={'seen': symbol, 'session_vwap': '99.00'},
            diagnostics={'fixture': True, 'timeout_probe': True})


def advise(view):
    time.sleep(5)
    return {'action': 'EXIT', 'reason': 'SLOW_FIXTURE_MUST_TIMEOUT'}
