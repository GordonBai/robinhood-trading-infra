"""Bind every management action to its instance, and report its lifecycle.

The entry path already binds the reference it submits. Protection, cancel and
exit actions are created later by the book itself, so their references were
never attributable: a broker receipt carrying a protection's rewritten ref had
no owner to route to.

Binding is by identity only — the action's logical ref, its approval aliases,
and its exact broker order ids — joined to the trade's instance through the
attribution table. Nothing here looks at a symbol, re-submits an order, or
changes a reference that was already committed.

Lifecycle feedback is derived from the book's own facts (the trade phase and the
broker order state), never from an agent report. Each fact is recorded once with
a pinned first-observed time so a replay produces the same event.
"""
import json

from trading_contracts.values import canonical, digest
from trading_infra.transactions import atomic

TERMINAL_ORDER_STATES = frozenset({'filled', 'cancelled', 'canceled', 'rejected',
                                   'expired'})

_SCHEMA = '''
CREATE TABLE IF NOT EXISTS lifecycle_first_seen(
    event_key TEXT PRIMARY KEY, observed_at TEXT NOT NULL);
'''


def _payload(action):
    try:
        value = json.loads(action.get('payload') or '{}')
    except ValueError:
        return {}
    return value if isinstance(value, dict) else {}


def action_refs(book, action):
    """Every exact identity this action was submitted under."""
    refs = set()
    payload = _payload(action)
    if isinstance(payload.get('ref_id'), str) and payload['ref_id']:
        refs.add(payload['ref_id'])
    member = book._order_refs(action)
    if isinstance(member, (set, list, tuple)):
        refs.update(member)
    order_ids = [row[0] for row in book.db.execute(
        'SELECT order_id FROM external_broker_order_ids WHERE action_id=?',
        (action['id'],))]
    return refs, order_ids


def bind_management_actions(book, *, attribution, importer, account_key):
    """Attach every action's identities to the instance that owns its trade.

    Returns counts and failures. A storage failure propagates: the caller must
    treat "could not bind" as an execution-integrity fault, not as success with
    a diagnostic field.
    """
    bound, aliases, skipped, failures = [], [], [], []
    for action in book.actions():
        trade_id = action.get('trade_id')
        if not isinstance(trade_id, str) or not trade_id:
            continue
        owner = attribution.owner_for_trade(trade_id)
        if owner is None:
            # No instance owns this trade: a legacy or manually adopted record.
            # It is left alone rather than forced into the current strategy.
            skipped.append(action['id'])
            continue
        refs, order_ids = action_refs(book, action)
        for ref in sorted(refs):
            try:
                attribution.bind(ref, owner['instance_id'], owner['trade_id'])
            except (ValueError, OSError) as exc:
                if str(exc) == 'ATTRIBUTION_REF_ALREADY_BOUND':
                    failures.append({'action_id': action['id'], 'ref_id': ref,
                                     'reason': 'ATTRIBUTION_REF_ALREADY_BOUND'})
                    continue
                raise
            bound.append({'action_id': action['id'], 'ref_id': ref,
                          'instance_id': owner['instance_id']})
        for order_id in order_ids:
            ref = sorted(refs)[0] if refs else None
            if ref is None:
                continue
            result = importer.alias(order_id, ref, account_key)
            aliases.append({'order_id': order_id, 'ref_id': ref,
                            'duplicate': result['duplicate']})
    return {'bound': bound, 'aliases': aliases, 'unowned_actions': skipped,
            'failures': failures}


def record_lifecycle_feedback(book, *, attribution, account_key, observed_at):
    """Record protection / exit / flat facts derived from the book itself.

    A confirmed stop is not proof that the whole quantity is protected, and an
    accepted cancel request is not a terminal state; each fact is taken from the
    broker's own order state for that action.
    """
    book.db.executescript(_SCHEMA)
    recorded = []
    state = book.state()
    orders = state.get('broker_orders') or {}
    for trade_id, trade in (state.get('trades') or {}).items():
        owner = attribution.owner_for_trade(trade_id)
        if owner is None:
            continue
        actions = [action for action in book.actions()
                   if action.get('trade_id') == trade_id]
        quantity = _number(trade.get('quantity'))
        protected = 0
        exited = False
        for action in actions:
            refs, _ids = action_refs(book, action)
            matched = [order for order in orders.values()
                       if order.get('ref_id') in refs
                       or order.get('id') in refs]
            filled = [order for order in matched
                      if str(order.get('state') or '').lower() in
                      {'filled', 'confirmed', 'partially_filled'}]
            if action['kind'] == 'protect':
                covered = sum(_number(order.get('remaining')) for order in filled)
                protected = max(protected, covered)
            elif action['kind'] == 'exit' and filled:
                exited = True
        if quantity > 0 and protected >= quantity:
            recorded.append(_record(book, attribution, owner, trade_id,
                                    'protection_covered', 'protect',
                                    account_key, observed_at,
                                    {'quantity': str(quantity)}))
        if exited:
            recorded.append(_record(book, attribution, owner, trade_id, 'exit',
                                    'exit', account_key, observed_at, {}))
        if trade.get('phase') == 'FLAT_VERIFIED':
            recorded.append(_record(book, attribution, owner, trade_id, 'flat',
                                    'flat', account_key, observed_at, {}))
    return {'recorded': [item for item in recorded if item]}


def _number(value):
    from decimal import Decimal, InvalidOperation
    if value is None:
        return Decimal(0)
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(0)


def _record(book, attribution, owner, trade_id, kind, stage, account_key,
            observed_at, extra):
    """Write one lifecycle fact exactly once, with a pinned occurrence time."""
    event_id = 'life:%s:%s' % (kind, digest(['lifecycle-1', account_key, trade_id,
                                             stage]))
    row = book.db.execute(
        'SELECT observed_at FROM lifecycle_first_seen WHERE event_key=?',
        (event_id,)).fetchone()
    with atomic(book.db):
        if row is None:
            book.db.execute(
                'INSERT INTO lifecycle_first_seen(event_key,observed_at) VALUES(?,?)',
                (event_id, observed_at))
            seen_at = observed_at
        else:
            seen_at = row[0]
        attribution.record({'event_id': event_id, 'ref_id': owner['ref_id'],
                            'kind': kind, 'occurred_at': seen_at,
                            'trade_id': trade_id, 'account_key': account_key,
                            'synthetic': book.simulation, **extra})
    return {'event_id': event_id, 'kind': kind, 'trade_id': trade_id}
