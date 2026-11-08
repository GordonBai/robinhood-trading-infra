"""Deterministic primitive conversion and hashing, without account or I/O access."""
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from collections.abc import Mapping


def number(value):
    if isinstance(value, (float, bool)):
        raise ValueError('Use decimal strings or integers, not floats/bools')
    try:
        result = Decimal(value)
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError('Invalid decimal value') from exc
    if not result.is_finite():
        raise ValueError('Decimal must be finite')
    return result


def timestamp(value):
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError('Timestamp must include a timezone')
    return result


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def thaw(value):
    """Make detached primitive working state from an immutable contract value."""
    if isinstance(value, Mapping):
        return {key: thaw(child) for key, child in value.items()}
    if isinstance(value, (tuple, list)):
        return [thaw(child) for child in value]
    return value


def contract_digest(value):
    """Type-tagged deterministic identity for immutable decision inputs.

    Unlike JSON coercion, Decimal/string and boolean/integer cannot collide.
    The bound covers the combined individually bounded input contract fields.
    """
    remaining = [200000]
    def encode(item, depth=0):
        remaining[0] -= 1
        if depth > 20 or remaining[0] < 0:
            raise ValueError('STRATEGY_IDENTITY_PAYLOAD_BOUND_EXCEEDED')
        if item is None:
            return ['null']
        if type(item) is bool:
            return ['bool', item]
        if type(item) is int:
            return ['int', str(item)]
        if isinstance(item, str):
            return ['str', item]
        if isinstance(item, Decimal) and item.is_finite():
            return ['decimal', str(item)]
        if isinstance(item, datetime) and item.tzinfo is not None and item.utcoffset() is not None:
            return ['datetime', item.astimezone(timezone.utc).isoformat()]
        if isinstance(item, Mapping):
            if any(not isinstance(key, str) for key in item):
                raise ValueError('STRATEGY_IDENTITY_MAPPING_KEYS_REQUIRED')
            return ['map', [[key, encode(item[key], depth+1)] for key in sorted(item)]]
        if isinstance(item, (tuple, list)):
            return ['seq', [encode(child, depth+1) for child in item]]
        raise ValueError('STRATEGY_IDENTITY_PRIMITIVE_REQUIRED')
    return digest(encode(value))
