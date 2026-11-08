"""Pinned exchange calendar and bar-completion rules, independent of any broker.

Venue-specific halts still require broker data. This module is the neutral home
for session boundaries and for deciding when a bar is closed enough to be used;
it depends only on the standard library, an optional calendar package and the
contract value helpers.
"""
from datetime import datetime, time, timedelta, timezone
from functools import lru_cache
from zoneinfo import ZoneInfo

from trading_contracts.values import timestamp

NY = ZoneInfo('America/New_York')
REGULAR_OPEN = time(9, 30)
REGULAR_CLOSE = time(16, 0)
BAR_INTERVALS = {'1minute': 1, '5minute': 5}


@lru_cache(maxsize=1)
def calendar():
    import exchange_calendars
    return exchange_calendars.get_calendar('XNYS', start='2000-01-01', end='2035-12-31')


def session(at):
    now = timestamp(at)
    day = now.astimezone(NY).date().isoformat()
    cal = calendar()
    if not cal.is_session(day):
        return None
    return {'date': day, 'open': cal.session_open(day).to_pydatetime(),
            'close': cal.session_close(day).to_pydatetime()}


def is_regular(at):
    now = timestamp(at)
    value = session(at)
    return bool(value and value['open'] <= now < value['close'])


def prior_daily_date(at):
    # Conservative daily-feature policy: only sessions strictly before local day.
    day = timestamp(at).astimezone(NY).date() - timedelta(days=1)
    return calendar().date_to_session(day.isoformat(), direction='previous').date().isoformat()


def bar_available_at(begins_at, interval):
    """First instant at which a bar starting at ``begins_at`` is complete.

    A bar becomes usable only after it closes plus a two-second settling delay;
    using ``begins_at`` as "already closed" would feed an unfinished bar into a
    signal. Unaligned bars and bars outside the exchange session are refused
    rather than rounded.
    """
    start = timestamp(begins_at)
    if interval in BAR_INTERVALS:
        width = BAR_INTERVALS[interval]
        local = start.astimezone(NY)
        if local.second or local.microsecond or local.minute % width:
            raise ValueError('Unaligned {} bar'.format(
                'five-minute' if width == 5 else 'one-minute'))
        if local.weekday() >= 5 or not REGULAR_OPEN <= local.time() < REGULAR_CLOSE:
            raise ValueError('Bar is outside basic regular-session bounds')
        schedule = session(begins_at)
        if (not schedule or not schedule['open'] <= start
                or start + timedelta(minutes=width) > schedule['close']):
            raise ValueError('Bar is outside the exchange session')
        return start + timedelta(minutes=width, seconds=2)
    if interval == 'day':
        utc = start.astimezone(timezone.utc)
        if utc.time() != time(0):
            raise ValueError('Daily data requires midnight UTC date labels')
        if not calendar().is_session(utc.date().isoformat()):
            raise ValueError('Daily label is not an exchange session')
        return datetime.combine(utc.date() + timedelta(days=1), time(0), NY)
    raise ValueError('Unsupported interval')
