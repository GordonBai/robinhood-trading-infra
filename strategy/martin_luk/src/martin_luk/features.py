"""Legacy application composition for the pure Martin universe screen."""
from trading_infra.market_calendar import ExchangeCalendarView
from trading_strategies.martin.screening import build_candidates as _build_candidates


def build_candidates(histories, indicators, *, as_of, universe_id):
    return _build_candidates(histories, indicators, as_of=as_of, universe_id=universe_id,
        calendar_view=ExchangeCalendarView())
