"""Strict portable JSON codec for the strategy process boundary.

Only the declared V1 contracts and type tags are accepted. There is no pickle,
class lookup from caller input, dynamic import, execution or persistence here.
Mapping iteration order and Decimal representation survive the boundary.
"""
from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
import json
import re
from types import MappingProxyType

from .strategy import (
    StrategyIdentity, StrategyInput, StrategyProposal, StrategyDecision, ProposalAction,
)


MAX_BYTES = 16 * 1024 * 1024
MAX_NODES = 200000
MAX_DEPTH = 24
_MAX_WIRE_DEPTH = 96
_SCHEMA = 'trading-strategy-contract'
_FIELDS = {
    StrategyIdentity: ('instance_id', 'plugin_id', 'version'),
    StrategyInput: ('identity', 'as_of', 'cursor', 'data_snapshot_id', 'source_sha256',
        'config_version', 'synthetic', 'market', 'allocation', 'budget', 'checkpoint'),
    StrategyProposal: ('identity', 'proposal_id', 'decision_id', 'setup_id', 'data_snapshot_id',
        'source_sha256', 'generated_at', 'valid_until', 'action', 'symbol', 'reason', 'synthetic',
        'purpose', 'side', 'quantity_limit', 'entry_ceiling', 'stop_price', 'evidence'),
    StrategyDecision: ('identity', 'decision_id', 'as_of', 'data_snapshot_id', 'consumed_cursor',
        'synthetic', 'proposals', 'next_checkpoint', 'diagnostics'),
}
_TAGS = {StrategyIdentity: 'identity', StrategyInput: 'input',
    StrategyProposal: 'proposal', StrategyDecision: 'decision'}
_CLASSES = {tag: cls for cls, tag in _TAGS.items()}


def _text(value):
    if type(value) is not str:
        raise ValueError()
    value.encode('utf-8')  # Reject unpaired surrogate escapes too.
    return value


class _Walk:
    def __init__(self):
        self.remaining = MAX_NODES
        self.wire_size = 0
        self._string_sizes = {}

    def charge(self, size):
        self.wire_size += size
        if self.wire_size > MAX_BYTES:
            raise ValueError()

    def string_size(self, value):
        # Escape one bounded scalar in C; never serialize the whole tree before
        # its exact UTF-8 size has been proven. Cache common tags/field values.
        cached = self._string_sizes.get(value)
        if cached is not None:
            return cached
        if len(value) > 16384:
            raise ValueError()
        size = len(json.encoder.encode_basestring(value).encode('utf-8'))
        if len(self._string_sizes) < 512:
            self._string_sizes[value] = size
        return size

    def tagged(self, tag, value):
        self.charge(13+len(tag))  # {"t":"tag","v":value}
        return {'t': tag, 'v': value}

    def step(self, depth):
        self.remaining -= 1
        if depth > MAX_DEPTH or self.remaining < 0:
            raise ValueError()

    def pack(self, value, depth=0):
        self.step(depth)
        cls = type(value)
        if cls in _FIELDS:
            fields = _FIELDS[cls]
            self.charge(2+max(0, len(fields)-1)
                +sum(self.string_size(key)+1 for key in fields))
            return self.tagged(_TAGS[cls], {key: self.pack(getattr(value, key), depth+1)
                for key in fields})
        if value is None:
            self.charge(12)
            return {'t': 'null'}
        if cls is bool:
            self.charge(4 if value else 5)
            return self.tagged('bool', value)
        if cls is int:
            raw = str(value)
            if len(raw) > 4096:
                raise ValueError()
            self.charge(self.string_size(raw))
            return self.tagged('int', raw)
        if cls is ProposalAction:
            self.charge(self.string_size(value.value))
            return self.tagged('action', value.value)
        if cls is str:
            self.charge(self.string_size(value))
            return self.tagged('str', value)
        if cls is Decimal:
            raw = str(value)
            if not value.is_finite() or len(raw) > 16384:
                raise ValueError()
            self.charge(self.string_size(raw))
            return self.tagged('decimal', raw)
        if cls is datetime:
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError()
            raw = value.isoformat()
            self.charge(self.string_size(raw))
            return self.tagged('datetime', raw)
        if isinstance(value, Mapping):
            self.charge(2+max(0, len(value)-1))
            pairs = []
            for key, child in value.items():
                if type(key) is not str:
                    raise ValueError()
                self.charge(3+self.string_size(key))
                pairs.append([key, self.pack(child, depth+1)])
            return self.tagged('map', pairs)
        if cls is tuple:
            self.charge(2+max(0, len(value)-1))
            return self.tagged('tuple', [self.pack(child, depth+1) for child in value])
        raise ValueError()

    def unpack(self, value, depth=0):
        self.step(depth)
        if type(value) is not dict or type(value.get('t')) is not str:
            raise ValueError()
        tag = value['t']
        if tag == 'null':
            if set(value) != {'t'}:
                raise ValueError()
            return None
        if set(value) != {'t', 'v'}:
            raise ValueError()
        raw = value['v']
        if tag in _CLASSES:
            cls = _CLASSES[tag]
            if type(raw) is not dict or set(raw) != set(_FIELDS[cls]):
                raise ValueError()
            return cls(**{key: self.unpack(raw[key], depth+1) for key in _FIELDS[cls]})
        if tag == 'bool':
            if type(raw) is not bool:
                raise ValueError()
            return raw
        if tag == 'str':
            return _text(raw)
        if tag == 'int':
            if type(raw) is not str or len(raw) > 4096 or not re.fullmatch(r'0|-?[1-9][0-9]*', raw):
                raise ValueError()
            return int(raw)
        if tag == 'decimal':
            if type(raw) is not str or not 0 < len(raw) <= 16384:
                raise ValueError()
            result = Decimal(raw)
            if not result.is_finite() or str(result) != raw:
                raise ValueError()
            return result
        if tag == 'datetime':
            if type(raw) is not str or len(raw) > 64:
                raise ValueError()
            result = datetime.fromisoformat(raw)
            if result.tzinfo is None or result.utcoffset() is None or result.isoformat() != raw:
                raise ValueError()
            return result
        if tag == 'action':
            return ProposalAction(_text(raw))
        if tag == 'map':
            if type(raw) is not list:
                raise ValueError()
            result = {}
            for pair in raw:
                if type(pair) is not list or len(pair) != 2:
                    raise ValueError()
                key = _text(pair[0])
                if key in result:
                    raise ValueError()
                result[key] = self.unpack(pair[1], depth+1)
            return MappingProxyType(result)
        if tag == 'tuple':
            if type(raw) is not list:
                raise ValueError()
            return tuple(self.unpack(child, depth+1) for child in raw)
        raise ValueError()


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError()
        result[key] = value
    return result


def _reject_number(value):
    raise ValueError()


def _json_int(value):
    if len(value) > 16:
        raise ValueError()
    return int(value)


def _wire_bounds(text):
    # Bound nesting/container allocation before the JSON parser constructs it.
    depth = containers = 0
    in_string = escaped = False
    for char in text:
        if in_string:
            if escaped:
                escaped = False
            elif char == '\\':
                escaped = True
            elif char == '"':
                in_string = False
        elif char == '"':
            in_string = True
        elif char in '[{':
            depth += 1; containers += 1
            if depth > _MAX_WIRE_DEPTH or containers > MAX_NODES*3:
                raise ValueError()
        elif char in ']}':
            depth -= 1
            if depth < 0:
                raise ValueError()


def _encode(value, cls, kind):
    try:
        if type(value) is not cls:
            raise ValueError()
        walk = _Walk()
        packed = walk.pack(value)
        # Envelope punctuation and ASCII constants, excluding the payload node.
        envelope_size = len('{"schema":"'+_SCHEMA+'","version":1,"kind":"'+kind+'","payload":}')
        if walk.wire_size+envelope_size > MAX_BYTES:
            raise ValueError()
        payload = {'schema': _SCHEMA, 'version': 1, 'kind': kind, 'payload': packed}
        raw = json.dumps(payload, sort_keys=True, separators=(',', ':'),
            ensure_ascii=False, allow_nan=False).encode('utf-8')
        if len(raw) != walk.wire_size+envelope_size:
            raise ValueError()
        # Apply decode validation too; frozen dataclasses can be bypassed by a
        # caller using object.__setattr__, so class identity alone is insufficient.
        _decode(raw, cls, kind, bounds_checked=True)
        return raw
    except (ValueError, TypeError, KeyError, AttributeError, UnicodeError,
            ArithmeticError, RecursionError, OverflowError):
        raise ValueError('STRATEGY_CODEC_INVALID') from None


def _decode(raw, cls, kind, *, bounds_checked=False):
    """Decode one contract frame.

    ``bounds_checked`` is set only when the caller produced these exact bytes
    itself from an already bounded packed tree. It skips the defensive wire
    scan, which otherwise walks every character of a payload that can carry a
    whole multi-symbol checkpoint. Untrusted bytes always get the scan.
    """
    try:
        if type(raw) is not bytes or not 0 < len(raw) <= MAX_BYTES:
            raise ValueError()
        text = raw.decode('utf-8')
        if not bounds_checked:
            _wire_bounds(text)
        payload = json.loads(text, object_pairs_hook=_pairs, parse_constant=_reject_number,
            parse_float=_reject_number, parse_int=_json_int)
        if (type(payload) is not dict or set(payload) != {'schema', 'version', 'kind', 'payload'}
                or payload['schema'] != _SCHEMA or type(payload['version']) is not int
                or payload['version'] != 1 or payload['kind'] != kind):
            raise ValueError()
        result = _Walk().unpack(payload['payload'])
        if type(result) is not cls:
            raise ValueError()
        return result
    except (ValueError, TypeError, KeyError, AttributeError, UnicodeError,
            ArithmeticError, RecursionError, OverflowError):
        raise ValueError('STRATEGY_CODEC_INVALID') from None


def encode_input(request):
    return _encode(request, StrategyInput, 'input')


def decode_input(raw):
    return _decode(raw, StrategyInput, 'input')


def encode_decision(decision):
    return _encode(decision, StrategyDecision, 'decision')


def decode_decision(raw):
    return _decode(raw, StrategyDecision, 'decision')
