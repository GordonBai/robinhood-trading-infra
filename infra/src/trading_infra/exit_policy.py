"""Safety-first coordination with an injected advisory strategy.

Calendar and Decimal helpers come from the neutral packages below. This module
never imports a concrete strategy and never reaches into the legacy package.

An unknown entry basis can reach a strategy which does not need it (such as an
indicator exit); no risk basis is fabricated. Advisory input/strategy failures
return HOLD with a bounded diagnostic so the caller can continue broker-based
protection and reconciliation. Quote/time/stop validation remains a hard error.
"""
from datetime import timedelta

from trading_contracts import ExitAction, ExitContext, ExitRecommendation, ExitStrategy
from trading_contracts.values import number, timestamp
from .exchange_calendar import session


def _unavailable(code, exception=None):
    diagnostic = {'code': code}
    if exception is not None:
        # Never copy arbitrary plugin exception messages into the ledger.
        diagnostic['exception_type'] = type(exception).__name__[:64]
    return {'action': 'HOLD', 'reason': 'STRATEGY_EXIT_UNAVAILABLE',
            'strategy_diagnostic': diagnostic}


def evaluate_exit(position, quote, *, as_of, strategy: ExitStrategy, features=None,
                  earnings_date=None, allow_overnight=False):
    now = timestamp(as_of)
    for key in ('bid_at', 'ask_at', 'observed_at'):
        if not 0 <= (now - timestamp(quote[key])).total_seconds() <= 15:
            raise ValueError('Exit quote stale or future')
    bid, ask = number(quote['bid']), number(quote['ask'])
    if not 0 < bid <= ask:
        raise ValueError('Invalid exit quote')
    market = session(as_of)
    if market is None or not market['open'] <= now < market['close']:
        return {'action': 'WAIT', 'reason': 'OUTSIDE_REGULAR_SESSION'}
    stop = number(position['stop'])
    if bid <= stop:
        return {'action': 'EXIT_ALL', 'reason': 'STRUCTURAL_STOP', 'price': str(bid)}
    if now >= market['close'] - timedelta(minutes=5):
        if not allow_overnight:
            return {'action': 'EXIT_ALL', 'reason': 'INTRADAY_CLOSE_POLICY', 'price': str(bid)}
        if earnings_date is None or earnings_date <= market['date']:
            return {'action': 'EXIT_ALL', 'reason': 'EARNINGS_UNKNOWN_OR_DUE', 'price': str(bid)}
    try:
        entry = position.get('entry_price')
        initial = position.get('initial_stop', position['stop'])
        context = ExitContext(number(entry) if entry is not None else None,
            number(initial) if initial is not None else None, stop, bid, ask,
            {key: number(value) for key, value in (features or {}).items() if value is not None})
    except (ValueError, TypeError, AttributeError) as exc:
        return _unavailable('EXIT_STRATEGY_INPUT_INVALID', exc)
    try:
        advice = strategy.decide_exit(context)
    except Exception as exc:
        # Only the advisory callback is isolated. Safety checks above and
        # process-control BaseExceptions are deliberately not masked.
        return _unavailable('EXIT_STRATEGY_EXCEPTION', exc)
    if not isinstance(advice, ExitRecommendation):
        return _unavailable('EXIT_STRATEGY_RECOMMENDATION_REQUIRED')
    try:
        # Revalidate the object at the boundary as well as in its constructor.
        advice = ExitRecommendation(advice.action, advice.reason, advice.stop)
    except (ValueError, TypeError, AttributeError) as exc:
        return _unavailable('EXIT_STRATEGY_RECOMMENDATION_INVALID', exc)
    result = {'action': advice.action.value, 'reason': advice.reason}
    if advice.action is ExitAction.EXIT_ALL:
        result['price'] = str(bid)
    elif advice.action is ExitAction.RAISE_STOP:
        if not stop < advice.stop < bid:
            return _unavailable('EXIT_STRATEGY_CANNOT_LOOSEN_OR_CROSS_STOP')
        result['stop'] = str(advice.stop)
    return result
