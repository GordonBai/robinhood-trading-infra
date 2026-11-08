# Frozen pre-isolation algorithm, test oracle only.
"""Research-only, prequalified-support five-minute flush/reclaim state machine.

Support discovery and proprietary RS reconstruction are NOT implemented here.
Input evidence must be supplied and known before the first processed bar.
"""

from datetime import timedelta
from copy import deepcopy
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from functools import wraps

from martin_agentic.ledger import timestamp
from martin_agentic.market import bar_available_at
from martin_agentic.risk import number


CATEGORIES = {"ema", "avwap", "gap", "swing", "weekly"}


def atomic_transition(method):
    @wraps(method)
    def wrapped(self, *args, **kwargs):
        before = deepcopy(self.__dict__)
        try:
            return method(self, *args, **kwargs)
        except Exception:
            self.__dict__.clear()
            self.__dict__.update(before)
            raise
    return wrapped


class PullbackMachine:
    VERSION = "pullback-research-0.1"

    def __init__(self, setup):
        self.setup = dict(setup)
        self.symbol = setup["symbol"]
        self.known_at, self.expires = timestamp(setup["known_at"]), timestamp(setup["expires_at"])
        self.low, self.high, self.tick = (number(setup[k]) for k in ("support_low", "support_high", "tick"))
        if not isinstance(self.symbol, str) or not self.symbol or not 0 < self.low <= self.high or self.tick <= 0:
            raise ValueError("Invalid support zone/tick/symbol")
        if self.expires <= self.known_at:
            raise ValueError("Setup expires before it is known")
        categories = setup["support_categories"]
        if not isinstance(categories, list) or not set(categories) <= CATEGORIES or len(set(categories)) < 3:
            raise ValueError("Require at least three distinct support categories")
        features = setup["features"]
        if timestamp(features["known_at"]) > self.known_at:
            raise ValueError("Features were not known at setup creation")
        ema9, ema21, ema50 = (number(features[k]) for k in ("ema9", "ema21", "ema50"))
        rs, adr, dollars = (number(features[k]) for k in ("rs_percentile", "adr20_fraction", "dollar_volume20"))
        if not 0 <= rs <= 1 or adr < 0 or dollars < 0 or min(ema9, ema21, ema50) <= 0:
            raise ValueError("Invalid feature units/values")
        self.state = "WAIT_FLUSH" if (ema9 > ema21 > ema50 and rs > Decimal("0.90")
                                      and adr > Decimal("0.05") and dollars > 50_000_000) else "REJECTED"
        self.reason = "SCREEN_PASSED" if self.state == "WAIT_FLUSH" else "SCREEN_NOT_QUALIFIED"
        self.stop = self.trigger = self.reclaim_available = self.last_start = None
        self.last_event_at = self.known_at
        self.last_bar = None
        self.waited = 0

    def snapshot(self):
        return {"strategy_version": self.VERSION, "symbol": self.symbol, "state": self.state,
                "reason": self.reason, "stop": str(self.stop) if self.stop is not None else None,
                "trigger": str(self.trigger) if self.trigger is not None else None,
                "known_at": self.known_at.isoformat(), "expires_at": self.expires.isoformat(),
                "live_order": False}

    def checkpoint(self):
        result={"setup":self.setup,"state":self.state,"reason":self.reason,"waited":self.waited,
                "stop":str(self.stop) if self.stop is not None else None,
                "trigger":str(self.trigger) if self.trigger is not None else None,
                "expires":self.expires.isoformat(),"last_event_at":self.last_event_at.isoformat(),
                "last_bar":self.last_bar}
        for key in ("reclaim_available","last_start"):
            value=getattr(self,key)
            result[key]=value.isoformat() if value else None
        return result

    @classmethod
    def restore(cls, checkpoint):
        result=cls(checkpoint["setup"])
        for key in ("state","reason","waited","last_bar"):
            setattr(result,key,checkpoint[key])
        for key in ("stop","trigger"):
            setattr(result,key,number(checkpoint[key]) if checkpoint[key] is not None else None)
        for key in ("expires","last_event_at","reclaim_available","last_start"):
            setattr(result,key,timestamp(checkpoint[key]) if checkpoint[key] is not None else None)
        return result

    def _clock(self, at):
        now = timestamp(at)
        if now < self.last_event_at:
            raise ValueError("Out-of-order strategy event")
        self.last_event_at = now
        if now >= self.expires and self.state not in {"TRIGGERED", "REJECTED", "INVALIDATED"}:
            self.state, self.reason = "EXPIRED", "SETUP_EXPIRED"
        return now

    @atomic_transition
    def on_bar(self, bar, *, as_of):
        now = self._clock(as_of)
        start = timestamp(bar["begins_at"])
        if (bar["symbol"] != self.symbol or bar.get("interval") != "5minute"
                or bar.get("bounds") != "regular" or bar.get("adjustment") != "split"):
            raise ValueError("Bar does not match setup symbol/interval/session/adjustment")
        if max(timestamp(bar["available_at"]), timestamp(bar["observed_at"])) > now:
            raise ValueError("Bar has not completed/been observed")
        if timestamp(bar["available_at"]) < bar_available_at(bar["begins_at"], "5minute"):
            raise ValueError("Bar completion timestamp was understated")
        if type(bar.get("volume")) is not int or bar["volume"] <= 0 or bar.get("interpolated", False) is not False:
            raise ValueError("Require a real positive-volume trading bar")
        if start < self.known_at:
            raise ValueError("Support was not known before this bar")
        if self.state in {"TRIGGERED", "REJECTED", "INVALIDATED", "EXPIRED"}:
            return self.snapshot()
        if self.last_start is not None:
            if start == self.last_start:
                if bar == self.last_bar:
                    return self.snapshot()
                raise ValueError("Previously consumed bar was revised")
            if start < self.last_start:
                raise ValueError("Bars must be strictly ascending")
            if start != self.last_start + timedelta(minutes=5):
                self.state, self.reason = "INVALIDATED", "BAR_GAP_REQUIRES_NEW_SETUP"
        self.last_start, self.last_bar = start, dict(bar)
        op, high, low, close = (number(bar[k]) for k in ("open", "high", "low", "close"))
        if not 0 < low <= min(op, close) <= max(op, close) <= high:
            raise ValueError("Invalid strategy OHLC")
        if self.state == "WAIT_FLUSH" and low < self.low:
            self.stop = ((low - self.tick) / self.tick).to_integral_value(rounding=ROUND_FLOOR) * self.tick
            if self.stop <= 0:
                self.state, self.reason = "INVALIDATED", "INVALID_STRUCTURAL_STOP"
            else:
                self.state, self.reason = "WAIT_RECLAIM", "FLUSH_CONFIRMED"
        elif self.state == "WAIT_RECLAIM":
            self.waited += 1
            if low <= self.stop:
                self.state, self.reason = "INVALIDATED", "FLUSH_STRUCTURE_BROKEN"
            elif close > self.high:
                self.trigger = ((high + self.tick) / self.tick).to_integral_value(rounding=ROUND_CEILING) * self.tick
                self.reclaim_available = timestamp(bar["available_at"])
                self.expires = min(self.expires, self.reclaim_available + timedelta(minutes=15))
                self.state, self.reason = "ARMED", "RECLAIM_CONFIRMED"
            elif self.waited >= 3:
                self.state, self.reason = "EXPIRED", "RECLAIM_TIMEOUT"
        elif self.state == "ARMED" and low <= self.stop:
            self.state, self.reason = "INVALIDATED", "STOP_BROKEN_BEFORE_ENTRY"
        return self.snapshot()

    @atomic_transition
    def on_quote(self, quote, *, as_of):
        now = self._clock(as_of)
        if quote["symbol"] != self.symbol:
            raise ValueError("Quote symbol mismatch")
        if self.state != "ARMED":
            return self.snapshot()
        times = [timestamp(quote[k]) for k in ("observed_at", "bid_at", "ask_at")]
        if any(not 0 <= (now - at).total_seconds() <= 15 for at in times):
            raise ValueError("STALE_OR_FUTURE_QUOTE")
        if min(times) < self.reclaim_available:
            raise ValueError("Quote predates completed reclaim")
        bid, ask = number(quote["bid"]), number(quote["ask"])
        if not 0 < bid <= ask:
            raise ValueError("Invalid two-sided quote")
        if bid <= self.stop:
            self.state, self.reason = "INVALIDATED", "STOP_BROKEN_BEFORE_ENTRY"
        elif (ask - bid) / ((ask + bid) / 2) > Decimal("0.0025"):
            self.reason = "SPREAD_TOO_WIDE"
        elif bid >= self.trigger:
            if ask > self.trigger * Decimal("1.002"):
                self.state, self.reason = "INVALIDATED", "CHASE_LIMIT_EXCEEDED"
            else:
                self.state, self.reason = "TRIGGERED", "BID_CONFIRMED_BREAKOUT"
        return self.snapshot()


def replay_setup(scenario):
    if scenario.get("mode") != "paper" or scenario.get("synthetic") is not True:
        raise ValueError("Strategy replay currently requires an explicitly synthetic paper fixture")
    machine = PullbackMachine(scenario["setup"])
    transitions = [machine.snapshot()]
    for event in scenario["events"]:
        if event["kind"] == "bar":
            result = machine.on_bar(event["data"], as_of=event["at"])
        elif event["kind"] == "quote":
            result = machine.on_quote(event["data"], as_of=event["at"])
        else:
            raise ValueError("Unsupported strategy event")
        transitions.append(result)
    return {"mode": "paper", "synthetic": True, "transitions": transitions,
            "current": machine.snapshot(), "execution_ready": False,
            "notice": "Research rule replay, not a backtest or broker order."}
