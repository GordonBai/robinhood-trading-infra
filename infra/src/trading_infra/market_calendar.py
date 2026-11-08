"""Explicit exchange-calendar service supplied to trusted strategy plugins."""
from . import exchange_calendar


class ExchangeCalendarView:
    def session(self, at):
        return exchange_calendar.session(at)

    def prior_daily_date(self, at):
        return exchange_calendar.prior_daily_date(at)

    def sessions_between(self, start, end):
        return tuple(value.date().isoformat()
                     for value in exchange_calendar.calendar().sessions_in_range(start, end))

    def bar_available_at(self, begins_at, interval):
        return exchange_calendar.bar_available_at(begins_at, interval)
