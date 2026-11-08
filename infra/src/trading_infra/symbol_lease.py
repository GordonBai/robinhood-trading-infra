"""Exclusive per-symbol ownership across strategy instances on one account.

A symbol is an account-level shared resource: if two instances could both hold
SPY, each would size, exit and reconcile against the other's position. This
module owns only the durable fact "which instance and trade currently own a
symbol". It never places, cancels or reconciles orders, and it holds no
position state.

V1 is an exclusive lease with a deterministic conflict order:

* Symbols are validated, never normalised. ``spy`` is rejected rather than
  silently upper-cased or trimmed, so two distinct strings can never alias to
  one lease.
* Every request is sorted before any check, so callers naming the same set in
  a different order receive the same decision and cannot deadlock on a shared
  subset.
* Check-and-insert for all symbols happens inside one ``BEGIN IMMEDIATE``
  transaction. A rejected or failed request inserts nothing, and a partial
  acquisition cannot persist.
* Re-acquiring the same ``(instance_id, trade_id)`` with the same set is
  idempotent. The same identity with a different set is a contract violation
  and raises rather than silently moving ownership.

Unknown state is not absence. A lease can be marked unsettled after an
ambiguous write, an unknown order outcome or a partial fill. It still counts
as held, and it cannot pass :meth:`assert_releasable` until
:meth:`settle` clears it. "Cancel accepted" is not a terminal state either;
ownership is released only after an explicit terminal reason.

The sqlite connection belongs to the caller (the execution owner) and is
never created, committed or closed here. One owner thread is assumed, like
the rest of the execution path.
"""
from collections.abc import Sequence
from datetime import datetime, timezone
import re

RELEASE_REASONS = frozenset({'flat_confirmed', 'rejected_entry', 'abandoned'})
RELEASABLE = 'SYMBOL_LEASE_RELEASABLE'
RELEASE_BLOCKED = 'SYMBOL_LEASE_RELEASE_BLOCKED'
RELEASE_NOT_OWNER = 'SYMBOL_LEASE_RELEASE_NOT_OWNER'
UNSETTLED = 'SYMBOL_LEASE_UNSETTLED'
HELD = 'SYMBOL_LEASE_HELD'
INVALID_SYMBOLS = 'SYMBOL_LEASE_INVALID_SYMBOLS'
INSTANCE_INVALID = 'SYMBOL_LEASE_INSTANCE_INVALID'
ACQUISITION_CONFLICT = 'SYMBOL_LEASE_ACQUISITION_CONFLICT'
HOLDING_REQUIRED = 'SYMBOL_LEASE_HOLDING_REQUIRED'
CLOSED = 'SYMBOL_LEASE_CLOSED'

_SYMBOL = re.compile(r'[A-Z][A-Z0-9.-]{0,14}')
_MAX_ID = 128

_SCHEMA = '''
CREATE TABLE IF NOT EXISTS symbol_leases(
    symbol TEXT PRIMARY KEY,
    instance_id TEXT NOT NULL,
    trade_id TEXT NOT NULL,
    acquired_at TEXT NOT NULL,
    unsettled INTEGER NOT NULL DEFAULT 0 CHECK(unsettled IN (0,1)));
CREATE INDEX IF NOT EXISTS symbol_leases_owner
    ON symbol_leases(instance_id,trade_id);
'''

_FIELDS = ('symbol', 'instance_id', 'trade_id', 'acquired_at')


def _valid_id(value):
    return isinstance(value, str) and bool(value.strip()) and len(value) <= _MAX_ID


def _require_id(value):
    if not _valid_id(value):
        raise ValueError(INSTANCE_INVALID)
    return value


def _valid_symbol(value):
    return isinstance(value, str) and _SYMBOL.fullmatch(value) is not None


def _sorted_symbols(symbols):
    """Validate an exact symbol set and return it sorted.

    Nothing is trimmed or case-folded: a caller that passes 'spy' has a bug,
    and quietly repairing it would let 'spy' and 'SPY' contend for one lease.
    """
    if isinstance(symbols, (str, bytes)) or not isinstance(symbols, Sequence):
        raise ValueError(INVALID_SYMBOLS)
    values = list(symbols)
    if not values:
        raise ValueError(INVALID_SYMBOLS)
    for value in values:
        if not _valid_symbol(value):
            raise ValueError(INVALID_SYMBOLS)
    if len(set(values)) != len(values):
        raise ValueError(INVALID_SYMBOLS)
    return tuple(sorted(values))


def _lease(row):
    return dict(zip(_FIELDS, row))


class SymbolLease:
    """Durable exclusive symbol ownership. No execution authority."""

    def __init__(self, db, *, clock=None):
        self.db = db
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self._closed = False
        self.db.executescript(_SCHEMA)

    def _now(self):
        return self.clock().astimezone(timezone.utc).isoformat()

    def _writable(self):
        if self._closed:
            raise ValueError(CLOSED)

    def acquire(self, instance_id, symbols, *, trade_id):
        """Claim every symbol atomically, or reject the whole request.

        Successful requests return ``{'acquired': True, ...}``. Rejections
        return ``{'acquired': False, 'reason': CODE, 'symbols': [...],
        'conflicts': [...], 'holder': {...}, 'holders': [...]}`` and insert
        nothing. Reusing an identity for a different symbol set raises
        ``SYMBOL_LEASE_ACQUISITION_CONFLICT``: identities are not reassigned.
        """
        self._writable()
        if not (_valid_id(instance_id) and _valid_id(trade_id)):
            return {'acquired': False, 'reason': INSTANCE_INVALID, 'symbols': [],
                    'conflicts': [], 'holder': None, 'holders': []}
        try:
            requested = _sorted_symbols(symbols)
        except ValueError:
            return {'acquired': False, 'reason': INVALID_SYMBOLS, 'symbols': [],
                    'conflicts': [], 'holder': None, 'holders': []}

        self.db.execute('BEGIN IMMEDIATE')
        try:
            outcome = self._acquire_in_transaction(instance_id, requested, trade_id)
            self.db.commit()
            return outcome
        except BaseException:
            self.db.rollback()
            raise

    def acquire_in_transaction(self, instance_id, symbols, *, trade_id):
        """Acquire inside a caller-owned transaction, for the admission path."""
        self._writable()
        if not self.db.in_transaction:
            raise ValueError('SYMBOL_LEASE_TRANSACTION_REQUIRED')
        if not (_valid_id(instance_id) and _valid_id(trade_id)):
            return {'acquired': False, 'reason': INSTANCE_INVALID, 'symbols': [],
                    'conflicts': [], 'holder': None, 'holders': []}
        try:
            requested = _sorted_symbols(symbols)
        except ValueError:
            return {'acquired': False, 'reason': INVALID_SYMBOLS, 'symbols': [],
                    'conflicts': [], 'holder': None, 'holders': []}
        return self._acquire_in_transaction(instance_id, requested, trade_id)

    def _acquire_in_transaction(self, instance_id, requested, trade_id):
        if True:
            # The identity's own set is checked first. Reusing a trade id for
            # a wider or narrower set is a caller contract violation even if
            # one of the new symbols happens to be held elsewhere.
            owned = [row[0] for row in self.db.execute(
                'SELECT symbol FROM symbol_leases WHERE instance_id=? AND trade_id=?'
                ' ORDER BY symbol', (instance_id, trade_id))]
            if owned and owned != list(requested):
                raise ValueError(ACQUISITION_CONFLICT)

            rows = self.db.execute(
                'SELECT symbol,instance_id,trade_id,acquired_at FROM symbol_leases'
                ' WHERE symbol IN (' + ','.join('?' * len(requested)) + ')'
                ' ORDER BY symbol', requested).fetchall()
            foreign = [row for row in rows
                       if (row[1], row[2]) != (instance_id, trade_id)]
            if foreign:
                holders = [_lease(row) for row in foreign]
                return {'acquired': False, 'reason': HELD,
                        'symbols': list(requested),
                        'conflicts': [holder['symbol'] for holder in holders],
                        'holder': holders[0], 'holders': holders}

            if rows:
                # Every requested symbol is already ours under this identity.
                return {'acquired': True, 'instance_id': instance_id,
                        'trade_id': trade_id, 'symbols': list(requested)}

            acquired_at = self._now()
            self.db.executemany(
                'INSERT INTO symbol_leases(symbol,instance_id,trade_id,acquired_at,'
                ' unsettled) VALUES(?,?,?,?,0)',
                [(symbol, instance_id, trade_id, acquired_at) for symbol in requested])
            return {'acquired': True, 'instance_id': instance_id,
                    'trade_id': trade_id, 'symbols': list(requested)}

    def release_in_transaction(self, instance_id, trade_id):
        """Release inside a caller-owned transaction, for settlement."""
        self._writable()
        _require_id(instance_id)
        _require_id(trade_id)
        if not self.db.in_transaction:
            raise ValueError('SYMBOL_LEASE_TRANSACTION_REQUIRED')
        return self.db.execute(
            'DELETE FROM symbol_leases WHERE instance_id=? AND trade_id=?',
            (instance_id, trade_id)).rowcount

    def release(self, instance_id, trade_id):
        """Delete this identity's leases and return how many were removed.

        This is the mechanical step, not the policy guard. Callers must ask
        :meth:`assert_releasable` first; releasing while a position or an
        unknown order may exist would hand the symbol to another instance
        while the account still holds exposure.
        """
        self._writable()
        _require_id(instance_id)
        _require_id(trade_id)
        self.db.execute('BEGIN IMMEDIATE')
        try:
            removed = self.db.execute(
                'DELETE FROM symbol_leases WHERE instance_id=? AND trade_id=?',
                (instance_id, trade_id)).rowcount
            self.db.commit()
            return removed
        except BaseException:
            self.db.rollback()
            raise

    def owner(self, symbol):
        """Return the owning instance id, or None when the symbol is free."""
        if not _valid_symbol(symbol):
            raise ValueError(INVALID_SYMBOLS)
        row = self.db.execute('SELECT instance_id FROM symbol_leases WHERE symbol=?',
                              (symbol,)).fetchone()
        return row[0] if row else None

    def held(self):
        """All leases, sorted by symbol."""
        return [_lease(row) for row in self.db.execute(
            'SELECT symbol,instance_id,trade_id,acquired_at FROM symbol_leases'
            ' ORDER BY symbol')]

    def holding(self, instance_id):
        """One instance's leases across all of its trades, sorted by symbol."""
        _require_id(instance_id)
        return [_lease(row) for row in self.db.execute(
            'SELECT symbol,instance_id,trade_id,acquired_at FROM symbol_leases'
            ' WHERE instance_id=? ORDER BY symbol', (instance_id,))]

    def assert_releasable(self, instance_id, trade_id, *, reason):
        """V1 guard: may this identity give its symbols back right now?

        Only a caller that can name a terminal reason may release. An unknown
        outcome leaves the lease unsettled, and a settled lease the caller no
        longer owns is not releasable either.
        """
        if not (_valid_id(instance_id) and _valid_id(trade_id)):
            return {'releasable': False, 'reason': RELEASE_NOT_OWNER}
        rows = self.db.execute(
            'SELECT unsettled FROM symbol_leases WHERE instance_id=? AND trade_id=?',
            (instance_id, trade_id)).fetchall()
        if not rows:
            return {'releasable': False, 'reason': RELEASE_NOT_OWNER}
        if reason not in RELEASE_REASONS:
            return {'releasable': False, 'reason': RELEASE_BLOCKED}
        if any(row[0] for row in rows):
            return {'releasable': False, 'reason': UNSETTLED}
        return {'releasable': True, 'reason': RELEASABLE}

    def mark_unsettled(self, instance_id, trade_id, symbols):
        """Record that these held symbols have an unknown outcome.

        The rows stay held. Repeating the call is harmless; the flag stays set
        until :meth:`settle` proves the outcome is known again.
        """
        self._writable()
        _require_id(instance_id)
        _require_id(trade_id)
        requested = _sorted_symbols(symbols)
        self.db.execute('BEGIN IMMEDIATE')
        try:
            found = {row[0] for row in self.db.execute(
                'SELECT symbol FROM symbol_leases WHERE instance_id=? AND trade_id=?'
                ' AND symbol IN (' + ','.join('?' * len(requested)) + ')',
                (instance_id, trade_id) + requested)}
            if any(symbol not in found for symbol in requested):
                # Never record an unknown outcome against a symbol this
                # identity does not own: that would claim knowledge it lacks.
                raise ValueError(HOLDING_REQUIRED)
            self.db.executemany(
                'UPDATE symbol_leases SET unsettled=1'
                ' WHERE symbol=? AND instance_id=? AND trade_id=?',
                [(symbol, instance_id, trade_id) for symbol in requested])
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def settle(self, instance_id, trade_id):
        """Clear the unsettled flag for this identity after the outcome is known."""
        self._writable()
        _require_id(instance_id)
        _require_id(trade_id)
        self.db.execute('BEGIN IMMEDIATE')
        try:
            self.db.execute(
                'UPDATE symbol_leases SET unsettled=0'
                ' WHERE instance_id=? AND trade_id=?', (instance_id, trade_id))
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def unsettled(self):
        """Leases whose outcome is still unknown, sorted by symbol."""
        return [_lease(row) for row in self.db.execute(
            'SELECT symbol,instance_id,trade_id,acquired_at FROM symbol_leases'
            ' WHERE unsettled=1 ORDER BY symbol')]

    def close(self):
        """Stop further mutations. Does not release leases and never closes db."""
        self._closed = True
