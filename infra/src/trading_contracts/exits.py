"""Immutable advisory inputs/outputs; a recommendation is never an order."""
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from types import MappingProxyType
from typing import Mapping, Protocol


class ExitAction(str, Enum):
    HOLD = 'HOLD'
    EXIT_ALL = 'EXIT_ALL'
    RAISE_STOP = 'RAISE_STOP'


@dataclass(frozen=True)
class ExitContext:
    # Unknown cost basis may still permit an indicator exit. It must never
    # produce a risk-multiple recommendation based on an invented entry price.
    entry_price: Decimal | None
    initial_stop: Decimal | None
    current_stop: Decimal
    bid: Decimal
    ask: Decimal
    features: Mapping[str, Decimal] = field(default_factory=dict)

    def __post_init__(self):
        values = [self.current_stop, self.bid, self.ask]
        values += [v for v in (self.entry_price, self.initial_stop) if v is not None]
        if any(not isinstance(v, Decimal) or not v.is_finite() for v in values):
            raise ValueError('EXIT_CONTEXT_FINITE_DECIMALS_REQUIRED')
        features = dict(self.features)
        if any(not isinstance(k, str) or not isinstance(v, Decimal) or not v.is_finite()
               for k, v in features.items()):
            raise ValueError('EXIT_CONTEXT_FEATURES_INVALID')
        object.__setattr__(self, 'features', MappingProxyType(features))


@dataclass(frozen=True)
class ExitRecommendation:
    action: ExitAction
    reason: str
    stop: Decimal | None = None

    def __post_init__(self):
        if (not isinstance(self.action, ExitAction) or not isinstance(self.reason, str)
                or not self.reason or len(self.reason) > 256):
            raise ValueError('EXIT_RECOMMENDATION_INVALID')
        if self.action is ExitAction.RAISE_STOP:
            if not isinstance(self.stop, Decimal) or not self.stop.is_finite():
                raise ValueError('EXIT_RECOMMENDATION_STOP_INVALID')
        elif self.stop is not None:
            raise ValueError('EXIT_RECOMMENDATION_UNEXPECTED_STOP')


class ExitStrategy(Protocol):
    def decide_exit(self, context: ExitContext) -> ExitRecommendation: ...
