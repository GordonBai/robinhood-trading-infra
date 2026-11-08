"""Read-only Robinhood capture/replay boundary. No auth/token or order code.

Observation timestamps prevent a freshly fetched history from masquerading as
point-in-time evidence. Daily date labels are UTC dates, NOT instants to convert
to the previous New York date. Completion is conservatively next local midnight.
"""

from datetime import datetime, time, timedelta, timezone
from decimal import Decimal
import json
import re
from zoneinfo import ZoneInfo

from .ledger import timestamp
from .risk import number


NY = ZoneInfo("America/New_York")
READ_TOOLS = frozenset({"get_equity_quotes", "get_equity_historicals",
                        "get_equity_technical_indicators"})


def unwrap_response(response):
    if not isinstance(response, dict) or response.get("isError"):
        raise ValueError("Robinhood tool returned an error")
    if "data" in response:
        return response["data"]
    if "data" in response.get("structuredContent", {}):
        return response["structuredContent"]["data"]
    for block in response.get("content", []):
        if block.get("type") == "text":
            value = json.loads(block["text"], parse_float=Decimal)
            if isinstance(value, dict) and "data" in value:
                return value["data"]
    raise ValueError("Missing structured data; guide text is never executed")


def validate_request(tool, request):
    if tool not in READ_TOOLS:
        raise ValueError("Only the explicit market-data read allowlist is supported")
    if not isinstance(request, dict):
        raise ValueError("Market-data arguments must be an object")
    symbols = request.get("symbols")
    cap = 20 if tool == "get_equity_quotes" else 10
    if (not isinstance(symbols, list) or not 1 <= len(symbols) <= cap
            or any(not isinstance(s, str) or not re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,14}", s)
                   for s in symbols) or len(set(symbols)) != len(symbols)):
        raise ValueError("Require a unique explicit list of uppercase symbols")
    allowed = {"symbols"}
    if tool != "get_equity_quotes":
        allowed |= {"interval", "bounds", "adjustment_type", "start_time", "end_time"}
        if request.get("bounds") != "regular" or request.get("adjustment_type") != "split":
            raise ValueError("I2 requires explicit regular/split data")
        if request.get("interval") not in {"day", "5minute"}:
            raise ValueError("I2 supports day and 5minute intervals only")
        if timestamp(request["start_time"]) >= timestamp(request["end_time"]):
            raise ValueError("Invalid requested time range")
    if tool == "get_equity_technical_indicators":
        allowed |= {"type", "period", "output"}
        if request.get("type") != "ema" or type(request.get("period")) is not int or request["period"] <= 0:
            raise ValueError("I2 indicator normalization supports explicit EMA periods only")
        if request.get("output") not in {"series", "latest", "last:3"}:
            raise ValueError("Unsupported indicator output selection")
    if set(request) - allowed:
        raise ValueError("Unexpected request fields")


class RobinhoodReader:
    """Inject a formally authenticated MCP transport; never inspect desktop tokens.

    transport(tool_name, arguments) returns an MCP envelope. Authentication and
    SDK integration belong to the caller; this does not make a CLI authenticated.
    """

    def __init__(self, transport, clock=None, record_observer=None):
        self.transport = transport
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.record_observer = record_observer

    def fetch(self, tool, request):
        validate_request(tool, request)
        data = unwrap_response(self.transport(tool, dict(request)))
        record = {"tool": tool, "request": dict(request),
                  "observed_at": self.clock().isoformat(), "response": {"data": data}}
        if self.record_observer is not None:
            self.record_observer(record)
        return record


def positive(value):
    result = number(value)
    if result <= 0:
        raise ValueError("Price/indicator must be positive")
    return result


def bar_available_at(begins_at, interval):
    """Compatibility re-export; the rule lives with the neutral calendar."""
    from trading_infra.exchange_calendar import bar_available_at as rule
    return rule(begins_at, interval)


def inspect_recording(recording, *, as_of, max_quote_age_seconds=15):
    """Normalize usable points, report blockers; never produce a trade decision.

    No fallback to last trade when a two-sided executable quote is unavailable.
    Historical records are accepted only for as_of >= their actual observation.
    """
    now = timestamp(as_of)
    if type(max_quote_age_seconds) is not int or max_quote_age_seconds <= 0:
        raise ValueError("Quote age limit must be a positive integer")
    if (not isinstance(recording, dict) or recording.get("schema_version") != 1 or recording.get("provider") != "robinhood"
            or type(recording.get("synthetic")) is not bool
            or not isinstance(recording.get("records"), list) or not recording["records"]):
        raise ValueError("Invalid recording envelope")
    output = {"schema_version": 1, "provider": "robinhood", "as_of": now.isoformat(),
              "max_quote_age_seconds": max_quote_age_seconds,
              "synthetic": recording["synthetic"], "quotes": [], "bars": [], "indicators": [],
              "errors": [], "warnings": [], "execution_ready": False,
              "note": "Read-only inspection; exchange calendar, permissions and strategy gates are separate."}
    seen = set()
    for index, record in enumerate(recording["records"]):
        lengths = {key: len(output[key]) for key in ("quotes", "bars", "indicators")}
        seen_before = set(seen)
        try:
            tool, request = record["tool"], record["request"]
            validate_request(tool, request)
            observed = timestamp(record["observed_at"])
            if observed > now:
                raise ValueError("Recording was not yet observed at requested as_of")
            data = unwrap_response(record["response"])
            if not isinstance(data, dict) or not isinstance(data.get("results"), list):
                raise ValueError("Missing result collection")
            if data.get("not_found"):
                raise ValueError("Unresolved symbols: " + ",".join(data["not_found"]))
            resolved = set()
            for result in data["results"]:
                symbol = (result.get("quote") or {}).get("symbol") if tool == "get_equity_quotes" else result.get("symbol")
                if symbol not in request["symbols"] or symbol in resolved:
                    raise ValueError("Unrequested, missing or duplicate response symbol")
                resolved.add(symbol)
                provenance = {"symbol": symbol, "source": tool,
                              "observed_at": observed.isoformat(), "record_index": index}
                if tool == "get_equity_quotes":
                    point = _quote(result, now, max_quote_age_seconds, observed)
                    key = ("quote", symbol)
                    rows = [(key, {**provenance, **point})]
                    target = "quotes"
                else:
                    if result.get("bounds") != request["bounds"] or result.get("interval") != request["interval"]:
                        raise ValueError("Response interval/session mismatch")
                    if tool == "get_equity_historicals":
                        series = result.get("bars")
                        target = "bars"
                    else:
                        indicators = result.get("indicators")
                        if not isinstance(indicators, list) or len(indicators) != 1:
                            raise ValueError("Expected exactly one indicator")
                        indicator = indicators[0]
                        if indicator.get("type") != "ema" or indicator.get("params") != {"period": request["period"]}:
                            raise ValueError("Indicator parameters mismatch")
                        series = indicator.get("series")
                        target = "indicators"
                    if not isinstance(series, list) or not series:
                        raise ValueError("Empty time series")
                    rows, previous = [], None
                    for bar in series:
                        start = timestamp(bar["begins_at"])
                        if previous is not None and start <= previous:
                            raise ValueError("Time series must be strictly ascending and unique")
                        previous = start
                        if not timestamp(request["start_time"]) <= start < timestamp(request["end_time"]):
                            raise ValueError("Bar outside requested range")
                        available = bar_available_at(bar["begins_at"], request["interval"])
                        if available > min(now, observed):
                            output["warnings"].append({"record": index, "symbol": symbol,
                                                       "reason": "INCOMPLETE_BAR", "begins_at": bar["begins_at"]})
                            continue
                        point = {**provenance, "begins_at": start.isoformat(),
                                 "available_at": available.isoformat(), "interval": request["interval"],
                                 "bounds": "regular", "adjustment": "split"}
                        key = (target, symbol, request["interval"], start.isoformat())
                        if target == "indicators":
                            # Provider EMA values are JSON numbers, unlike money inputs.
                            point.update(type="ema", period=request["period"], value=str(positive(str(bar["value"]))))
                            key += (request["period"],)
                        else:
                            if bar.get("session") != "reg":
                                raise ValueError("Unknown/non-regular bar session")
                            if type(bar.get("interpolated", False)) is not bool or bar.get("interpolated", False):
                                raise ValueError("Synthetic gap-fill bar is not a trading observation")
                            prices = [positive(bar[k + "_price"]) for k in ("open", "high", "low", "close")]
                            op, high, low, close = prices
                            if not low <= min(op, close) <= max(op, close) <= high:
                                raise ValueError("Invalid OHLC geometry")
                            if type(bar["volume"]) is not int or bar["volume"] <= 0:
                                raise ValueError("Trading bar requires positive integer volume")
                            point.update({k: str(v) for k, v in zip(("open", "high", "low", "close"), prices)})
                            point["volume"] = bar["volume"]
                        rows.append((key, point))
                    if not rows:
                        raise ValueError("No completed observed bars available")
                # Validate the entire symbol series before publishing any of it.
                for key, point in rows:
                    if key in seen:
                        raise ValueError("Duplicate observation across captures")
                    seen.add(key)
                    output[target].append(point)
            if resolved != set(request["symbols"]):
                raise ValueError("Missing requested response symbols")
        except (ValueError, TypeError, KeyError, AttributeError) as exc:
            for key, length in lengths.items():
                del output[key][length:]
            seen = seen_before
            output["errors"].append({"record": index, "reason": str(exc)})
    output["schema_valid"] = not output["errors"]
    # Compatibility flag is only structural; readiness is evaluated independently.
    from .calendar import prior_daily_date
    expected=prior_daily_date(as_of)
    latest={}
    for point in output["bars"]+output["indicators"]:
        key=(point["symbol"],point["interval"],point.get("type"),point.get("period"))
        if key not in latest or timestamp(point["begins_at"]) > timestamp(latest[key]["begins_at"]):
            latest[key]=point
    stale=[]
    for key,p in latest.items():
        if (p["interval"]=="day" and timestamp(p["begins_at"]).date().isoformat()!=expected
                or p["interval"]=="5minute" and (now-timestamp(p["available_at"])).total_seconds()>360):
            stale.append({"symbol":p["symbol"],"reason":"STALE_FEATURE_STREAM","interval":p["interval"]})
    output["freshness_errors"]=stale
    output["stream_fresh"]=not stale and not output["errors"]
    output["data_usable"]=output["schema_valid"] and output["stream_fresh"]
    symbols={p["symbol"] for p in output["quotes"]}
    output["feature_set_complete"]=bool(symbols) and all(
        all((s,"day","ema",p) in latest for p in (9,21,50))
        and sum(b["symbol"]==s and b["interval"]=="day" for b in output["bars"])>=64 for s in symbols)
    output["strategy_ready"]=output["data_usable"] and output["feature_set_complete"]
    return output


def _quote(result, now, max_age, observed):
    quote = result["quote"]
    if quote.get("has_traded") is not True or quote.get("state") != "active":
        raise ValueError("Quote instrument is not active/traded")
    bid, ask = positive(quote["bid_price"]), positive(quote["ask_price"])
    if bid > ask:
        raise ValueError("Crossed quote")
    times = [timestamp(quote[key]) for key in ("venue_bid_time", "venue_ask_time")]
    if any(t > observed or not 0 <= (now - t).total_seconds() <= max_age for t in times):
        raise ValueError("STALE_OR_FUTURE_QUOTE")
    close = result.get("close")
    if close and close.get("symbol") != quote["symbol"]:
        raise ValueError("Settled close belongs to another symbol")
    return {"bid": str(bid), "ask": str(ask), "bid_at": times[0].isoformat(),
            "ask_at": times[1].isoformat(), "spread_fraction": str((ask - bid) / ((ask + bid) / 2)),
            "prior_session_close": close, "feed_coverage": "unverified"}
