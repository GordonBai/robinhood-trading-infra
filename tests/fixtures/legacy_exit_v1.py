# Frozen pre-isolation implementation, parity oracle only.
"""Frozen research exit rules. Stops use executable bid, not fictional stop fills."""

from datetime import timedelta
from decimal import Decimal

from martin_agentic.calendar import session
from martin_agentic.ledger import timestamp
from martin_agentic.risk import number


def exit_decision(position, quote, *, as_of, daily_ema9=None, earnings_date=None, allow_overnight=False):
    now=timestamp(as_of)
    for key in ("bid_at","ask_at","observed_at"):
        if not 0 <= (now-timestamp(quote[key])).total_seconds() <= 15:
            raise ValueError("Exit quote stale or future")
    bid,ask=number(quote["bid"]),number(quote["ask"])
    if not 0 < bid <= ask:
        raise ValueError("Invalid exit quote")
    market=session(as_of)
    if market is None or not market["open"] <= now < market["close"]:
        return {"action":"WAIT","reason":"OUTSIDE_REGULAR_SESSION"}
    stop=number(position["stop"])
    if bid <= stop:
        return {"action":"EXIT_ALL","reason":"STRUCTURAL_STOP","price":str(bid)}
    if now >= market["close"]-timedelta(minutes=5):
        if not allow_overnight:
            return {"action":"EXIT_ALL","reason":"INTRADAY_CLOSE_POLICY","price":str(bid)}
        if earnings_date is None or earnings_date <= market["date"]:
            return {"action":"EXIT_ALL","reason":"EARNINGS_UNKNOWN_OR_DUE","price":str(bid)}
    if daily_ema9 is not None and bid < number(daily_ema9):
        return {"action":"EXIT_ALL","reason":"PRIOR_DAILY_EMA9_BREAK","price":str(bid)}
    entry=number(position["entry_price"])
    initial_stop=number(position.get("initial_stop",position["stop"]))
    risk=entry-initial_stop
    if risk > 0 and bid >= entry+3*risk and stop < entry:
        return {"action":"RAISE_STOP","reason":"THREE_R_BREAKEVEN","stop":str(entry)}
    return {"action":"HOLD","reason":"TREND_CONTINUES"}
