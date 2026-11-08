"""Typed boundaries between the infrastructure and whatever it is talking to.

A market feed, a broker read surface, an execution surface and a host delivery
surface are different powers. Declaring them separately is what lets a strategy
run against a replay feed, a shadow broker or a formal channel without knowing
which one it is, and lets the infrastructure refuse a capability instead of
silently degrading.

These are structural protocols, not implementations. A port never carries an
account number, a credential or a confirmation bypass; confirmation state is
reported, never asserted.
"""
from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping, Protocol, runtime_checkable

CONFIRMATION_STATES = ('not_required', 'required', 'granted', 'denied', 'unknown')


def _labels(values, name):
    if isinstance(values, (str, bytes)) or not isinstance(values, (tuple, list)):
        raise ValueError('CAPABILITY_' + name + '_INVALID')
    result = tuple(values)
    if any(not isinstance(item, str) or not item or len(item) > 128 for item in result):
        raise ValueError('CAPABILITY_' + name + '_INVALID')
    if not result or len(set(result)) != len(result):
        raise ValueError('CAPABILITY_' + name + '_INVALID')
    return result


@dataclass(frozen=True)
class CapabilityMatrix:
    """What one concrete adapter can actually do, and what it must refuse.

    An adapter that cannot satisfy a declared requirement is rejected at
    negotiation time instead of failing halfway through a live trade. Absence of
    a capability is a refusal, never a fallback.
    """
    adapter_id: str
    provides: tuple
    confirmation: str
    continuous_authorisation: bool
    synthetic: bool

    def __post_init__(self):
        if not isinstance(self.adapter_id, str) or not self.adapter_id.strip() \
                or len(self.adapter_id) > 128:
            raise ValueError('CAPABILITY_ADAPTER_ID_INVALID')
        object.__setattr__(self, 'provides', _labels(self.provides, 'PROVIDES'))
        if self.confirmation not in CONFIRMATION_STATES:
            raise ValueError('CAPABILITY_CONFIRMATION_STATE_INVALID')
        for name in ('continuous_authorisation', 'synthetic'):
            if type(getattr(self, name)) is not bool:
                raise ValueError('CAPABILITY_' + name.upper() + '_INVALID')

    def supports(self, requirement):
        if not isinstance(requirement, str) or not requirement:
            raise ValueError('CAPABILITY_REQUIREMENT_INVALID')
        return requirement in self.provides

    def require(self, requirements):
        """Refuse the whole negotiation if any requirement is unmet."""
        missing = [item for item in _labels(requirements, 'REQUIREMENTS')
                   if item not in self.provides]
        if missing:
            raise ValueError('CAPABILITY_NOT_PROVIDED:' + ','.join(sorted(missing)))
        return self

    def as_dict(self):
        return MappingProxyType({'adapter_id': self.adapter_id,
                                 'provides': list(self.provides),
                                 'confirmation': self.confirmation,
                                 'continuous_authorisation': self.continuous_authorisation,
                                 'synthetic': self.synthetic})


@runtime_checkable
class MarketDataPort(Protocol):
    """Read-only market observations. No order authority of any kind."""

    def quotes(self, symbols, *, as_of):
        ...

    def bars(self, symbols, *, interval, start, end):
        ...


@runtime_checkable
class BrokerReadPort(Protocol):
    """Accounts, positions and orders, readable only."""

    def positions(self):
        ...

    def orders(self):
        ...

    def snapshot(self):
        """The complete, account-bound observation a decision may rely on.

        ``orders()`` is not this: a count cannot stand in for the order set, and
        an observation without its own time or completeness flag cannot prove
        freshness. An adapter without this method cannot serve as the account
        authority.
        """
        ...


@runtime_checkable
class ExecutionPort(Protocol):
    """Order intent submission. Confirmation is reported by the adapter.

    Queue advancement is a different power from acting on one intent: an
    advance can touch whatever is next, while review/place/cancel must name the
    exact action they operate on.
    """

    def advance_queue(self, *, allow_entries=False):
        ...

    def review(self, intent):
        ...

    def place(self, intent):
        ...

    def cancel(self, ref_id):
        ...


@runtime_checkable
class HostDeliveryPort(Protocol):
    """Delivery of a formal intent to the host that holds the confirmation."""

    def deliver(self, intent):
        ...

    def receipt(self, intent_id):
        ...


def assess(adapter, *, provides, requires, confirmation, continuous_authorisation, synthetic):
    """Declare what an adapter provides, then refuse if a requirement is unmet."""
    matrix = CapabilityMatrix(adapter_id=adapter, provides=tuple(provides),
                              confirmation=confirmation,
                              continuous_authorisation=continuous_authorisation,
                              synthetic=synthetic)
    return matrix.require(requires)


@dataclass(frozen=True)
class PositionView:
    """What an exit advisor may see: its own position, never the account."""
    instance_id: str
    plugin_id: str
    trade_id: str
    symbol: str
    quantity: str
    entry_price: str
    stop: str
    initial_stop: str
    quote: Mapping
    features: Mapping
    as_of: str
    position_version: str


@dataclass(frozen=True)
class ExitAdvice:
    """A strategy suggestion, bound to the exact position it was computed for."""
    plugin_id: str
    trade_id: str
    action: str
    reason: str
    as_of: str
    valid_until: str
    position_version: str
    suggested_stop: str | None = None

    def __post_init__(self):
        if self.action not in {'HOLD', 'EXIT', 'REDUCE', 'TIGHTEN_PROTECTION'}:
            raise ValueError('EXIT_ADVICE_ACTION_INVALID')
        for name in ('plugin_id', 'trade_id', 'reason', 'as_of',
                     'valid_until', 'position_version'):
            value = getattr(self, name)
            if not isinstance(value, str) or not value or len(value) > 128:
                raise ValueError('EXIT_ADVICE_' + name.upper() + '_INVALID')


@runtime_checkable
class ExitAdvisorPort(Protocol):
    """Bounded advice boundary. Never executed inside a write transaction."""

    def advise_many(self, views):
        ...


@runtime_checkable
class MarketCollectionPort(Protocol):
    """Collect quote/bar observations for a declared requirement set.

    ``requirements`` comes from the plugin manifests that will consume the
    events. An adapter that cannot serve a requirement must refuse explicitly;
    it may not silently bootstrap another provider profile.
    """

    def collect_events(self, signals, *, symbols, requirements, clock,
                       capture_context, cancellation):
        ...
