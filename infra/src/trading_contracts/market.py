"""Calendar port and economic bar identity; no provider or exchange library."""
from datetime import datetime
from typing import Protocol

from .values import digest, number, timestamp


class CalendarView(Protocol):
    def session(self, at: str) -> dict | None: ...
    def prior_daily_date(self, at: str) -> str: ...
    def sessions_between(self, start: str, end: str) -> tuple[str, ...]: ...
    def bar_available_at(self, begins_at: str, interval: str) -> datetime: ...


def bar_observation_identity(data):
    key = digest([data['symbol'], timestamp(data['begins_at']).isoformat()])
    contents = {name: str(number(data[name]).normalize())
        for name in ('open', 'high', 'low', 'close', 'volume')}
    contents.update({name: data.get(name) for name in ('interval', 'bounds', 'adjustment')})
    return key, digest(contents)
