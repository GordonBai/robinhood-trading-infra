"""V1 advisory strategy boundary. No account, persistence or broker authority.

Inputs contain only the requesting instance's views. Values are copied into
immutable containers, not shared strategy/host objects. These contracts are
validation and dependency boundaries, not a sandbox for untrusted Python.
"""
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
import re
from types import MappingProxyType
from typing import Mapping, Protocol


_FORBIDDEN = frozenset({
    'account', 'account_number', 'account_key', 'broker_account', 'broker_ref',
    'ref_id', 'order_id', 'broker_order_id', 'approval_id', 'execution_authorized',
    'specific_order_confirmed', 'access_token', 'refresh_token', 'authorization',
    'headers', 'cookies', 'cookie', 'password', 'api_key', 'secret_key', 'client_secret',
    'book', 'connection', 'credentials',
})


def _label(value, name):
    if not isinstance(value, str) or not value.strip() or len(value) > 256:
        raise ValueError('STRATEGY_' + name + '_INVALID')
    return value


def _time(value):
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError('STRATEGY_AWARE_DATETIME_REQUIRED')
    return value.astimezone(timezone.utc)


def _hash(value):
    if not isinstance(value, str) or not re.fullmatch('[0-9a-f]{64}', value):
        raise ValueError('STRATEGY_SOURCE_SHA256_REQUIRED')


def _price(value):
    if not isinstance(value, Decimal) or not value.is_finite() or value <= 0:
        raise ValueError('STRATEGY_POSITIVE_DECIMAL_REQUIRED')


def _freeze(value, *, max_nodes=10000):
    remaining = [max_nodes]

    def visit(item, depth):
        remaining[0] -= 1
        if depth > 16 or remaining[0] < 0:
            raise ValueError('STRATEGY_PAYLOAD_BOUND_EXCEEDED')
        if item is None or type(item) in (bool, int):
            return item
        if isinstance(item, str):
            if len(item) > 16384:
                raise ValueError('STRATEGY_PAYLOAD_STRING_TOO_LONG')
            return item
        if isinstance(item, Decimal):
            if not item.is_finite():
                raise ValueError('STRATEGY_FINITE_DECIMAL_REQUIRED')
            return item
        if isinstance(item, Mapping):
            copied = {}
            for key, child in item.items():
                if not isinstance(key, str) or len(key) > 256:
                    raise ValueError('STRATEGY_PAYLOAD_KEY_INVALID')
                if key.lower().replace('-', '_') in _FORBIDDEN:
                    raise ValueError('STRATEGY_EXECUTION_OR_CREDENTIAL_FIELD_FORBIDDEN')
                copied[key] = visit(child, depth + 1)
            return MappingProxyType(copied)
        if isinstance(item, (tuple, list)):
            return tuple(visit(child, depth + 1) for child in item)
        raise ValueError('STRATEGY_PRIMITIVE_PAYLOAD_REQUIRED')

    if not isinstance(value, Mapping):
        raise ValueError('STRATEGY_MAPPING_REQUIRED')
    return visit(value, 0)


@dataclass(frozen=True)
class StrategyIdentity:
    instance_id: str
    plugin_id: str
    version: str

    def __post_init__(self):
        for name in ('instance_id', 'plugin_id', 'version'):
            _label(getattr(self, name), name.upper())


@dataclass(frozen=True)
class StrategyInput:
    identity: StrategyIdentity
    as_of: datetime
    cursor: str
    data_snapshot_id: str
    source_sha256: str
    config_version: str
    synthetic: bool
    market: Mapping = field(default_factory=dict)
    allocation: Mapping = field(default_factory=dict)
    budget: Mapping = field(default_factory=dict)
    checkpoint: Mapping = field(default_factory=dict)

    def __post_init__(self):
        if not isinstance(self.identity, StrategyIdentity) or type(self.synthetic) is not bool:
            raise ValueError('STRATEGY_INPUT_IDENTITY_OR_PROVENANCE_INVALID')
        object.__setattr__(self, 'as_of', _time(self.as_of))
        for name in ('cursor', 'data_snapshot_id', 'config_version'):
            _label(getattr(self, name), name.upper())
        _hash(self.source_sha256)
        for name in ('market', 'allocation', 'budget', 'checkpoint'):
            object.__setattr__(self, name, _freeze(getattr(self, name),
                max_nodes=100000 if name == 'market' else 10000))
        if self.allocation.get('instance_id', self.identity.instance_id) != self.identity.instance_id:
            raise ValueError('STRATEGY_ALLOCATION_INSTANCE_MISMATCH')


class ProposalAction(str, Enum):
    ENTER = 'ENTER'
    REDUCE = 'REDUCE'
    EXIT = 'EXIT'
    TIGHTEN_PROTECTION = 'TIGHTEN_PROTECTION'
    HOLD = 'HOLD'


@dataclass(frozen=True)
class StrategyProposal:
    identity: StrategyIdentity
    proposal_id: str
    decision_id: str
    setup_id: str
    data_snapshot_id: str
    source_sha256: str
    generated_at: datetime
    valid_until: datetime
    action: ProposalAction
    symbol: str
    reason: str
    synthetic: bool
    purpose: str = 'strategy'
    side: str = 'long'
    quantity_limit: int | None = None
    entry_ceiling: Decimal | None = None
    stop_price: Decimal | None = None
    evidence: Mapping = field(default_factory=dict)

    def __post_init__(self):
        if not isinstance(self.identity, StrategyIdentity) or not isinstance(self.action, ProposalAction):
            raise ValueError('STRATEGY_PROPOSAL_IDENTITY_OR_ACTION_INVALID')
        for name in ('proposal_id', 'decision_id', 'setup_id', 'data_snapshot_id', 'reason'):
            _label(getattr(self, name), name.upper())
        _hash(self.source_sha256)
        generated, until = _time(self.generated_at), _time(self.valid_until)
        if until <= generated:
            raise ValueError('STRATEGY_PROPOSAL_VALIDITY_INVALID')
        object.__setattr__(self, 'generated_at', generated)
        object.__setattr__(self, 'valid_until', until)
        if (type(self.synthetic) is not bool or self.purpose not in {'strategy', 'engineering'}
                or self.side != 'long' or not isinstance(self.symbol, str)
                or not re.fullmatch('[A-Z][A-Z0-9.-]{0,15}', self.symbol)):
            raise ValueError('STRATEGY_PROPOSAL_SCOPE_INVALID')
        if self.quantity_limit is not None and (type(self.quantity_limit) is not int or self.quantity_limit <= 0):
            raise ValueError('STRATEGY_WHOLE_SHARE_LIMIT_REQUIRED')
        if self.action is ProposalAction.ENTER:
            _price(self.entry_ceiling); _price(self.stop_price)
            if self.stop_price >= self.entry_ceiling:
                raise ValueError('STRATEGY_ENTRY_STOP_NOT_BELOW_CEILING')
        else:
            if self.entry_ceiling is not None:
                raise ValueError('STRATEGY_UNEXPECTED_ENTRY_PRICE')
            if self.action is ProposalAction.TIGHTEN_PROTECTION:
                _price(self.stop_price)
            elif self.stop_price is not None:
                raise ValueError('STRATEGY_UNEXPECTED_STOP_PRICE')
            if self.action is ProposalAction.REDUCE:
                if self.quantity_limit is None:
                    raise ValueError('STRATEGY_REDUCTION_QUANTITY_REQUIRED')
            elif self.quantity_limit is not None:
                raise ValueError('STRATEGY_UNEXPECTED_QUANTITY_LIMIT')
        object.__setattr__(self, 'evidence', _freeze(self.evidence))

    @property
    def execution_authorized(self):
        return False


@dataclass(frozen=True)
class StrategyDecision:
    identity: StrategyIdentity
    decision_id: str
    as_of: datetime
    data_snapshot_id: str
    consumed_cursor: str
    synthetic: bool
    proposals: tuple[StrategyProposal, ...] = ()
    next_checkpoint: Mapping = field(default_factory=dict)
    diagnostics: Mapping = field(default_factory=dict)

    def __post_init__(self):
        if not isinstance(self.identity, StrategyIdentity) or type(self.synthetic) is not bool:
            raise ValueError('STRATEGY_DECISION_IDENTITY_OR_PROVENANCE_INVALID')
        object.__setattr__(self, 'as_of', _time(self.as_of))
        for name in ('decision_id', 'data_snapshot_id', 'consumed_cursor'):
            _label(getattr(self, name), name.upper())
        if not isinstance(self.proposals, (tuple, list)):
            raise ValueError('STRATEGY_PROPOSALS_SEQUENCE_REQUIRED')
        proposals = tuple(self.proposals)
        seen = set()
        for proposal in proposals:
            if (not isinstance(proposal, StrategyProposal) or proposal.identity != self.identity
                    or proposal.decision_id != self.decision_id
                    or proposal.data_snapshot_id != self.data_snapshot_id
                    or proposal.generated_at != self.as_of or proposal.synthetic is not self.synthetic):
                raise ValueError('STRATEGY_PROPOSAL_DECISION_BINDING_INVALID')
            if proposal.proposal_id in seen:
                raise ValueError('STRATEGY_DUPLICATE_PROPOSAL_ID')
            seen.add(proposal.proposal_id)
        object.__setattr__(self, 'proposals', proposals)
        object.__setattr__(self, 'next_checkpoint', _freeze(self.next_checkpoint))
        object.__setattr__(self, 'diagnostics', _freeze(self.diagnostics))

    def validate_for(self, request: StrategyInput):
        """Host checks the returned decision against its exact input; returns self.

        Constructor checks bind proposals to their decision. This second check
        binds that decision to the request the host actually issued, including
        the source fingerprint; it grants no execution authority.
        """
        if (not isinstance(request, StrategyInput) or self.identity != request.identity
                or self.as_of != request.as_of or self.data_snapshot_id != request.data_snapshot_id
                or self.consumed_cursor != request.cursor or self.synthetic is not request.synthetic
                or any(proposal.source_sha256 != request.source_sha256 for proposal in self.proposals)):
            raise ValueError('STRATEGY_DECISION_INPUT_BINDING_INVALID')
        return self


@dataclass(frozen=True)
class PreparedSignalInput:
    """A pinned request and the exact checkpoint fingerprint it was computed from.

    Infrastructure carries this pair between preparation and commit; it grants
    no execution authority and holds no live objects.
    """
    request: StrategyInput
    checkpoint_fingerprint: str

    def __post_init__(self):
        if not isinstance(self.request, StrategyInput):
            raise ValueError('STRATEGY_PREPARED_REQUEST_REQUIRED')
        _hash(self.checkpoint_fingerprint)


class StrategyPlugin(Protocol):
    def decide(self, request: StrategyInput) -> StrategyDecision: ...
