"""Route execution feedback to the strategy instance that owns it.

With several instances on one account, an order reference is not a strategy
identity. A reference is bound to an instance at the moment the infrastructure
accepts the entry intent; every later fill, protection, cancel or flat event
carries that reference and is therefore attributed without guessing from the
symbol, which two instances may both be watching.

Properties this module keeps:

- A reference is bound once. Rebinding it to another instance is a conflict,
  never an overwrite.
- An event with an unbound reference is refused, not filed under "unknown".
- Duplicate delivery is idempotent; the same event id with different content is
  a conflict.
- Delivery order does not matter. Each event keeps its occurrence time, and
  readers ask for the latest state of a kind rather than the last one written.
"""
from datetime import datetime, timezone
import json

from trading_contracts.values import canonical, timestamp
from .transactions import atomic

KINDS = ('rejected', 'pending_confirmation', 'unknown', 'order_accepted', 'partial_fill',
         'filled', 'protection_covered', 'cancel_terminal', 'exit', 'flat')

_SCHEMA = '''
CREATE TABLE IF NOT EXISTS execution_attribution(
    ref_id TEXT PRIMARY KEY, instance_id TEXT NOT NULL, trade_id TEXT NOT NULL,
    bound_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS execution_feedback(
    event_id TEXT PRIMARY KEY, ref_id TEXT NOT NULL, instance_id TEXT NOT NULL,
    trade_id TEXT NOT NULL, kind TEXT NOT NULL, occurred_at TEXT NOT NULL,
    received_at TEXT NOT NULL, payload TEXT NOT NULL, payload_digest TEXT NOT NULL,
    sequence INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS execution_feedback_instance
    ON execution_feedback(instance_id, sequence);
'''


class ExecutionAttribution:
    """Bind order references to instances and route feedback. Places nothing."""

    def __init__(self, db, *, clock=None):
        self.db = db
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.db.executescript(_SCHEMA)

    def _now(self):
        return self.clock().astimezone(timezone.utc).isoformat()

    # -- binding ---------------------------------------------------------
    def bind_in_transaction(self, ref_id, instance_id, trade_id):
        """Bind inside a caller-owned transaction, for the admission path."""
        if not self.db.in_transaction:
            raise ValueError('ATTRIBUTION_TRANSACTION_REQUIRED')
        return self._bind(ref_id, instance_id, trade_id)

    def bind(self, ref_id, instance_id, trade_id):
        """Bind one order reference to one instance and trade. Idempotent."""
        for name, value in (('ref_id', ref_id), ('instance_id', instance_id),
                            ('trade_id', trade_id)):
            if not isinstance(value, str) or not value or len(value) > 128:
                raise ValueError('ATTRIBUTION_BINDING_INVALID')
        return self._bind(ref_id, instance_id, trade_id)

    def _bind(self, ref_id, instance_id, trade_id):
        for name, value in (('ref_id', ref_id), ('instance_id', instance_id),
                            ('trade_id', trade_id)):
            if not isinstance(value, str) or not value or len(value) > 128:
                raise ValueError('ATTRIBUTION_BINDING_INVALID')
        prior = self.owner(ref_id)
        if prior is not None:
            if (prior['instance_id'], prior['trade_id']) != (instance_id, trade_id):
                raise ValueError('ATTRIBUTION_REF_ALREADY_BOUND')
            return prior
        with atomic(self.db):
            self.db.execute('INSERT INTO execution_attribution'
                            '(ref_id,instance_id,trade_id,bound_at) VALUES(?,?,?,?)',
                            (ref_id, instance_id, trade_id, self._now()))
        return self.owner(ref_id)

    def owner(self, ref_id):
        row = self.db.execute('SELECT ref_id,instance_id,trade_id,bound_at'
                              ' FROM execution_attribution WHERE ref_id=?',
                              (ref_id,)).fetchone()
        return None if row is None else {'ref_id': row[0], 'instance_id': row[1],
                                         'trade_id': row[2], 'bound_at': row[3]}

    def owner_for_trade(self, trade_id):
        """The instance that owns a trade, from the bound execution identity."""
        if not isinstance(trade_id, str) or not trade_id:
            raise ValueError('ATTRIBUTION_TRADE_ID_INVALID')
        row = self.db.execute(
            'SELECT ref_id,instance_id,trade_id,bound_at FROM execution_attribution'
            ' WHERE trade_id=? ORDER BY bound_at,ref_id LIMIT 1', (trade_id,)).fetchone()
        return None if row is None else {'ref_id': row[0], 'instance_id': row[1],
                                         'trade_id': row[2], 'bound_at': row[3]}

    # -- feedback --------------------------------------------------------
    def record(self, event):
        """Store one execution-feedback event for its owning instance.

        Returns ``{'event_id', 'instance_id', 'trade_id', 'duplicate'}`` when
        accepted, and raises for an unbound reference, a malformed event or a
        conflicting redelivery. Nothing is inferred from the symbol.
        """
        if not isinstance(event, dict):
            raise ValueError('ATTRIBUTION_EVENT_REQUIRED')
        event_id, ref_id, kind = event.get('event_id'), event.get('ref_id'), event.get('kind')
        for name, value in (('event_id', event_id), ('ref_id', ref_id)):
            if not isinstance(value, str) or not value or len(value) > 128:
                raise ValueError('ATTRIBUTION_EVENT_INVALID')
        if kind not in KINDS:
            raise ValueError('ATTRIBUTION_EVENT_KIND_INVALID')
        occurred = timestamp(event.get('occurred_at'))
        payload = {key: value for key, value in event.items()
                   if key not in ('event_id', 'ref_id')}
        body = canonical(payload)
        owner = self.owner(ref_id)
        if owner is None:
            raise ValueError('ATTRIBUTION_REF_UNKNOWN')
        prior = self.db.execute('SELECT payload,instance_id,trade_id FROM execution_feedback'
                                ' WHERE event_id=?', (event_id,)).fetchone()
        if prior is not None:
            if prior[0] != body or (prior[1], prior[2]) != (owner['instance_id'],
                                                            owner['trade_id']):
                raise ValueError('ATTRIBUTION_EVENT_CONFLICT')
            return {'event_id': event_id, 'instance_id': owner['instance_id'],
                    'trade_id': owner['trade_id'], 'duplicate': True}
        with atomic(self.db):
            return self._insert_event(event_id, ref_id, owner, body, occurred, kind, payload)

    def _insert_event(self, event_id, ref_id, owner, body, occurred, kind, payload):
        from trading_contracts.values import digest
        sequence = self.db.execute('SELECT coalesce(max(sequence),0)+1'
                                   ' FROM execution_feedback').fetchone()[0]
        self.db.execute('INSERT INTO execution_feedback'
                        '(event_id,ref_id,instance_id,trade_id,kind,occurred_at,received_at,'
                        ' payload,payload_digest,sequence) VALUES(?,?,?,?,?,?,?,?,?,?)',
                        (event_id, ref_id, owner['instance_id'], owner['trade_id'], kind,
                         occurred.isoformat(), self._now(), body, digest(payload), sequence))
        return {'event_id': event_id, 'instance_id': owner['instance_id'],
                'trade_id': owner['trade_id'], 'duplicate': False}

    # -- reads -----------------------------------------------------------
    def events(self, instance_id, *, trade_id=None):
        query = ('SELECT event_id,ref_id,trade_id,kind,occurred_at,received_at,payload'
                 ' FROM execution_feedback WHERE instance_id=?')
        arguments = [instance_id]
        if trade_id is not None:
            query += ' AND trade_id=?'
            arguments.append(trade_id)
        query += ' ORDER BY sequence'
        return [{'event_id': row[0], 'ref_id': row[1], 'trade_id': row[2], 'kind': row[3],
                 'occurred_at': row[4], 'received_at': row[5], 'payload': json.loads(row[6])}
                for row in self.db.execute(query, arguments)]

    def latest(self, instance_id, kind, *, trade_id=None):
        """The most recently *occurred* event of a kind, independent of arrival order."""
        if kind not in KINDS:
            raise ValueError('ATTRIBUTION_EVENT_KIND_INVALID')
        candidates = [item for item in self.events(instance_id, trade_id=trade_id)
                      if item['kind'] == kind]
        if not candidates:
            return None
        return max(candidates, key=lambda item: (item['occurred_at'], item['event_id']))

    def open_trades(self, instance_id):
        """Trades that have reported progress but not a terminal flat event."""
        by_trade = {}
        for item in self.events(instance_id):
            by_trade.setdefault(item['trade_id'], []).append(item['kind'])
        return sorted(trade for trade, kinds in by_trade.items() if 'flat' not in kinds)
