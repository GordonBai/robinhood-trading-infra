"""Legacy application composition for the pure Martin pullback algorithm."""
from trading_infra.market_calendar import ExchangeCalendarView
from trading_strategies.martin.pullback import (
    CATEGORIES, atomic_transition, PullbackMachine as _PullbackMachine, replay_setup as _replay_setup,
)


class PullbackMachine(_PullbackMachine):
    def __init__(self, setup, *, calendar_view=None):
        super().__init__(setup, calendar_view=calendar_view if calendar_view is not None else ExchangeCalendarView())

    @classmethod
    def restore(cls, checkpoint, *, calendar_view=None):
        return super().restore(checkpoint, calendar_view=calendar_view if calendar_view is not None else ExchangeCalendarView())


def replay_setup(scenario):
    return _replay_setup(scenario, calendar_view=ExchangeCalendarView())
