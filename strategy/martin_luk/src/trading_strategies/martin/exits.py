"""Martin's exit recommendation, expressed only through the neutral contract.

The strategy sees a read-only ``ExitContext``; it cannot read the book, submit
an order, or loosen the current stop. Safety checks (quote freshness, session,
structural stop, intraday close) run in ``trading_infra.exit_policy`` before
this code is ever consulted.
"""
from trading_contracts import ExitAction, ExitRecommendation


class MartinExitStrategy:
    """Legacy Martin rules: EMA break, three-R breakeven, otherwise hold."""

    def decide_exit(self, context):
        ema9 = context.features.get('ema9')
        if ema9 is not None and context.bid < ema9:
            return ExitRecommendation(ExitAction.EXIT_ALL, 'PRIOR_DAILY_EMA9_BREAK')
        entry = context.entry_price
        initial = context.initial_stop
        # An absent basis must not invent a risk multiple. The neutral boundary
        # turns the resulting exception into a bounded HOLD diagnostic.
        risk = entry - initial
        if risk > 0 and context.bid >= entry + 3 * risk and context.current_stop < entry:
            return ExitRecommendation(ExitAction.RAISE_STOP, 'THREE_R_BREAKEVEN', entry)
        return ExitRecommendation(ExitAction.HOLD, 'TREND_CONTINUES')
