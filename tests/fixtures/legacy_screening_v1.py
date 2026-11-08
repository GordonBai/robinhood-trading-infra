# Frozen pre-isolation algorithm, test oracle only.
"""Deterministic universe-relative screen and daily-bar AVWAP approximation.

Standard EMAs are provider values. ADR, RS rank and structure are explicitly
defined research adaptations, not proprietary IBD ratings or exact tick AVWAP.
"""

from datetime import timedelta
from decimal import Decimal

from martin_agentic.calendar import calendar, prior_daily_date, session
from martin_agentic.ledger import timestamp
from martin_agentic.risk import number


def build_candidates(histories, indicators, *, as_of, universe_id):
    now = timestamp(as_of)
    if not isinstance(universe_id, str) or not universe_id or len(histories) < 10:
        raise ValueError("Require a named universe of at least ten symbols for relative ranking")
    expected = prior_daily_date(as_of)
    metrics, rejected = {}, []
    for symbol, bars in histories.items():
        try:
            if not isinstance(bars, list) or len(bars) < 64:
                raise ValueError("Require at least 64 daily bars")
            dates, clean = [], []
            for b in bars:
                if b.get("symbol") != symbol or b.get("interval") != "day" or b.get("bounds") != "regular" or b.get("adjustment") != "split":
                    raise ValueError("Daily bar contract mismatch")
                if max(timestamp(b["available_at"]), timestamp(b["observed_at"])) > now:
                    raise ValueError("Future/unobserved daily input")
                op, high, low, close = (number(b[k]) for k in ("open", "high", "low", "close"))
                if not 0 < low <= min(op, close) <= max(op, close) <= high or type(b["volume"]) is not int or b["volume"] <= 0:
                    raise ValueError("Invalid daily OHLCV")
                if b.get("interpolated", False) is not False:
                    raise ValueError("Interpolated history is not admissible")
                dates.append(timestamp(b["begins_at"]).date().isoformat())
                clean.append((high, low, close, number(b["volume"])))
            if dates != sorted(set(dates)) or dates[-1] != expected:
                raise ValueError("Daily history is duplicate, unsorted or stale")
            sessions = [d.date().isoformat() for d in calendar().sessions_in_range(dates[0], dates[-1])]
            if sessions != dates:
                raise ValueError("Daily history has session gaps")
            emas = {}
            for period in (9, 21, 50):
                p = indicators[symbol][str(period)]
                if (p.get("type") != "ema" or p.get("period") != period
                        or p.get("interval") != "day" or p.get("bounds") != "regular"
                        or p.get("adjustment") != "split"
                        or timestamp(p["begins_at"]).date().isoformat() != expected
                        or max(timestamp(p["available_at"]), timestamp(p["observed_at"])) > now):
                    raise ValueError("EMA provenance/session/date mismatch")
                emas[str(period)] = number(p["value"])
                if emas[str(period)] <= 0:
                    raise ValueError("Invalid EMA")
            metrics[symbol] = {"return63": clean[-1][2] / clean[-64][2] - 1,
                               "adr": sum((h-l)/c for h,l,c,v in clean[-20:]) / 20,
                               "dollars": sum(c*v for h,l,c,v in clean[-20:]) / 20,
                               "emas": emas, "clean": clean, "bars": bars}
        except (ValueError, KeyError, TypeError) as exc:
            rejected.append({"symbol": symbol, "reason": str(exc)})
    # Do not silently rank a different universe after missing-data exclusions.
    if rejected:
        return {"candidates": [], "rejected": rejected, "universe_complete": False,
                "universe_id": universe_id, "expected_daily_date": expected}
    candidates = []
    returns = [m["return63"] for m in metrics.values()]
    market_session = session(as_of)
    if market_session is None:
        return {"candidates": [], "rejected": [{"reason": "NON_TRADING_DAY"}], "universe_complete": True}
    for symbol, m in metrics.items():
        rank = Decimal(sum(r < m["return63"] for r in returns)) / (len(returns)-1)
        e = m["emas"]
        checks={"RS_TOP_DECILE":rank>Decimal('0.90'),"ADR_ABOVE_5_PERCENT":m['adr']>Decimal('0.05'),
                "DOLLAR_VOLUME_ABOVE_50M":m['dollars']>50_000_000,"EMA_STACK":e['9']>e['21']>e['50']}
        if not all(checks.values()):
            rejected.append({"symbol": symbol, "reason": "SCREEN_NOT_QUALIFIED",
                             "failed_checks":[k for k,v in checks.items() if not v],
                             "metrics":{"rs_percentile":str(rank),"adr20_fraction":str(m['adr']),
                                        "dollar_volume20":str(m['dollars'])}})
            continue
        clean = m["clean"]
        levels = [(e["9"], "ema"), (e["21"], "ema")]
        pivots = [i for i in range(2,len(clean)-2)
                  if clean[i][1] < min(clean[j][1] for j in (i-2,i-1,i+1,i+2))]
        for i in pivots[-10:]:
            low = clean[i][1]
            if all(row[2] >= low for row in clean[i+3:]):
                levels.append((low,"swing"))
        if pivots:
            anchor = pivots[-1]
            tail = clean[anchor:]
            avwap = sum(((h+l+c)/3)*v for h,l,c,v in tail) / sum(v for h,l,c,v in tail)
            levels.append((avwap,"avwap"))
        price = clean[-1][2]
        clusters=[]
        for level,_ in levels:
            group=[(p,c) for p,c in levels if abs(p-level)/price <= Decimal("0.015")]
            categories=sorted({c for p,c in group})
            lo,hi=min(p for p,c in group),max(p for p,c in group)
            if len(categories)>=3 and hi-lo <= price*Decimal("0.025") and lo <= price <= hi*Decimal("1.08"):
                clusters.append((abs(price-hi),lo,hi,categories))
        if not clusters:
            rejected.append({"symbol":symbol,"reason":"NO_THREE_CATEGORY_SUPPORT"})
            continue
        _,lo,hi,categories=min(clusters)
        expires=market_session["close"]-timedelta(minutes=15)
        if now >= expires:
            rejected.append({"symbol":symbol,"reason":"TOO_LATE_FOR_ENTRY"})
            continue
        candidates.append({"symbol":symbol,"known_at":as_of,"expires_at":expires.isoformat(),
                           "support_low":str(lo),"support_high":str(hi),"support_categories":categories,
                           "tick":"0.01", "features":{"known_at":as_of,"ema9":str(e['9']),
                           "ema21":str(e['21']),"ema50":str(e['50']),"rs_percentile":str(rank),
                           "adr20_fraction":str(m['adr']),"dollar_volume20":str(m['dollars'])},
                           "evidence":{"universe_id":universe_id,"rs_definition":"63-session universe-relative strict rank",
                                       "daily_date":expected,"avwap_method":"HLC3 daily-bar approximation",
                                       "anchor_date":m['bars'][pivots[-1]]['begins_at']}})
    return {"candidates":candidates,"rejected":rejected,"universe_complete":True,
            "universe_id":universe_id,"expected_daily_date":expected}
