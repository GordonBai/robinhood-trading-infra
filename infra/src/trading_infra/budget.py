"""Single account-level admission authority behind many strategy instances.

An account runs several strategy instances, each with its own
``InstanceBudget`` ceiling. Those ceilings are not additive: instances that
each stay inside their own limit can still jointly exceed what the account
can fund. This module is the one place that admits or refuses a trade
against the account's money limits *and* the requesting instance's ceiling.

Reservations are durable rows on the caller's SQLite connection. Amounts are
stored as canonical decimal text and every sum is computed with ``Decimal``
in Python; SQLite numeric coercion (which would produce REAL/float) is never
used. The caller owns the connection and is the single owner thread, so this
module opens no connections, starts no threads and never closes the caller's
database.
"""
from collections.abc import Mapping
from datetime import datetime, timezone
from decimal import Decimal

from trading_contracts.instance import StrategyInstance
from trading_contracts.values import number

_LIMIT_KEYS = ('max_open_cash', 'max_open_notional', 'max_open_planned_loss',
               'max_concurrent_trades')

_SCHEMA = '''
CREATE TABLE IF NOT EXISTS account_budget_limits(
    id INTEGER PRIMARY KEY CHECK(id = 1),
    max_open_cash TEXT NOT NULL,
    max_open_notional TEXT NOT NULL,
    max_open_planned_loss TEXT NOT NULL,
    max_concurrent_trades INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS account_budget_instance_limits(
    instance_id TEXT PRIMARY KEY,
    max_cash TEXT NOT NULL,
    max_planned_loss TEXT NOT NULL,
    max_notional TEXT NOT NULL,
    max_concurrent INTEGER NOT NULL,
    updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS account_budget_reservations(
    instance_id TEXT NOT NULL,
    trade_id TEXT NOT NULL,
    cash TEXT NOT NULL,
    planned_loss TEXT NOT NULL,
    notional TEXT NOT NULL,
    reserved_at TEXT NOT NULL,
    PRIMARY KEY(instance_id, trade_id));
'''


def _amount(value):
    """A non-negative finite Decimal; float and bool are never money."""
    try:
        result = number(value)
    except ValueError:
        raise ValueError('BUDGET_AMOUNT_INVALID') from None
    if result < 0:
        raise ValueError('BUDGET_AMOUNT_INVALID')
    return result


def _trade_id(value):
    if not isinstance(value, str) or not value or len(value) > 128:
        raise ValueError('BUDGET_TRADE_ID_INVALID')
    return value


def _limits(limits):
    """Validate the complete limit mapping. Absent keys are never padded."""
    if not isinstance(limits, Mapping) or set(limits) != set(_LIMIT_KEYS):
        raise ValueError('BUDGET_LIMITS_INVALID')
    values = {}
    for key in ('max_open_cash', 'max_open_notional', 'max_open_planned_loss'):
        try:
            value = number(limits[key])
        except ValueError:
            raise ValueError('BUDGET_LIMITS_INVALID') from None
        if value < 0:
            raise ValueError('BUDGET_LIMITS_INVALID')
        values[key] = value
    concurrent = limits['max_concurrent_trades']
    if type(concurrent) is not int or concurrent < 0:
        raise ValueError('BUDGET_LIMITS_INVALID')
    values['max_concurrent_trades'] = concurrent
    return values


def _totals():
    return {'cash': Decimal(0), 'notional': Decimal(0),
            'planned_loss': Decimal(0), 'trades': 0}


def _accumulate(totals, cash, planned_loss, notional):
    totals['cash'] += cash
    totals['planned_loss'] += planned_loss
    totals['notional'] += notional
    totals['trades'] += 1


class AccountBudgetAuthority:
    """Admit, release and report durable account-wide trade reservations."""

    def __init__(self, db, limits, *, clock=None):
        values = _limits(limits)
        self.db = db
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self._closed = False
        self.db.executescript(_SCHEMA)
        self.db.execute('BEGIN IMMEDIATE')
        try:
            self.db.execute(
                'INSERT INTO account_budget_limits'
                '(id,max_open_cash,max_open_notional,max_open_planned_loss,'
                'max_concurrent_trades) VALUES(1,?,?,?,?)'
                ' ON CONFLICT(id) DO UPDATE SET'
                ' max_open_cash=excluded.max_open_cash,'
                ' max_open_notional=excluded.max_open_notional,'
                ' max_open_planned_loss=excluded.max_open_planned_loss,'
                ' max_concurrent_trades=excluded.max_concurrent_trades',
                (str(values['max_open_cash']), str(values['max_open_notional']),
                 str(values['max_open_planned_loss']),
                 values['max_concurrent_trades']))
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    # -- admission -------------------------------------------------------
    def reserve(self, instance, *, trade_id, cash, planned_loss, notional):
        """Atomically reserve account and instance capacity for one trade.

        Same ``(instance_id, trade_id)`` and numerically identical amounts is
        an idempotent success. Changed amounts under the same identity raise
        ``BUDGET_RESERVATION_CONFLICT`` instead of silently rewriting history.
        """
        self._ensure_open()
        self.db.execute('BEGIN IMMEDIATE')
        try:
            outcome = self._reserve_in_transaction(
                instance, trade_id=trade_id, cash=cash, planned_loss=planned_loss,
                notional=notional)
            self.db.commit()
            return outcome
        except BaseException:
            self.db.rollback()
            raise

    def reserve_in_transaction(self, instance, *, trade_id, cash, planned_loss, notional):
        """Reserve as part of a caller-owned transaction.

        The admission path must commit budget, symbol lease, attribution and the
        execution intent together. A component that opens its own transaction
        cannot participate in that, so it exposes this form and the caller owns
        begin/commit/rollback. Refusing outside a transaction keeps the two
        entry points from silently diverging.
        """
        self._ensure_open()
        if not self.db.in_transaction:
            raise ValueError('BUDGET_TRANSACTION_REQUIRED')
        return self._reserve_in_transaction(
            instance, trade_id=trade_id, cash=cash, planned_loss=planned_loss,
            notional=notional)

    def _reserve_in_transaction(self, instance, *, trade_id, cash, planned_loss, notional):
        if True:
            if not isinstance(instance, StrategyInstance):
                raise ValueError('BUDGET_INSTANCE_REQUIRED')
            trade_id = _trade_id(trade_id)
            cash = _amount(cash)
            planned_loss = _amount(planned_loss)
            notional = _amount(notional)
            instance_id = instance.instance_id
            prior = self.db.execute(
                'SELECT cash,planned_loss,notional FROM account_budget_reservations'
                ' WHERE instance_id=? AND trade_id=?',
                (instance_id, trade_id)).fetchone()
            if prior is not None:
                if (Decimal(prior[0]), Decimal(prior[1]), Decimal(prior[2])) != (
                        cash, planned_loss, notional):
                    raise ValueError('BUDGET_RESERVATION_CONFLICT')
                return {'reserved': True, 'instance_id': instance_id,
                        'trade_id': trade_id, 'cash': prior[0],
                        'planned_loss': prior[1], 'notional': prior[2]}

            account_limits = self._read_limits()
            instance_total = _totals()
            account_total = _totals()
            for row in self.db.execute(
                    'SELECT instance_id,cash,planned_loss,notional'
                    ' FROM account_budget_reservations'):
                row_cash, row_loss, row_notional = (
                    Decimal(row[1]), Decimal(row[2]), Decimal(row[3]))
                _accumulate(account_total, row_cash, row_loss, row_notional)
                if row[0] == instance_id:
                    _accumulate(instance_total, row_cash, row_loss, row_notional)

            budget = instance.budget
            if instance_total['cash'] + cash > budget.max_cash:
                reason = 'INSTANCE_CASH_LIMIT'
            elif instance_total['notional'] + notional > budget.max_notional:
                reason = 'INSTANCE_NOTIONAL_LIMIT'
            elif instance_total['planned_loss'] + planned_loss > budget.max_planned_loss:
                reason = 'INSTANCE_PLANNED_LOSS_LIMIT'
            elif instance_total['trades'] + 1 > budget.max_concurrent:
                reason = 'INSTANCE_CONCURRENCY_LIMIT'
            elif account_total['trades'] + 1 > account_limits['max_concurrent_trades']:
                reason = 'ACCOUNT_CONCURRENCY_LIMIT'
            elif account_total['cash'] + cash > account_limits['max_open_cash']:
                reason = 'ACCOUNT_CASH_LIMIT'
            elif account_total['notional'] + notional > account_limits['max_open_notional']:
                reason = 'ACCOUNT_NOTIONAL_LIMIT'
            elif (account_total['planned_loss'] + planned_loss >
                  account_limits['max_open_planned_loss']):
                reason = 'ACCOUNT_PLANNED_LOSS_LIMIT'
            else:
                reason = None

            if reason is not None:
                return {'reserved': False, 'reason': reason,
                        'instance_id': instance_id, 'trade_id': trade_id}

            now = self._now()
            self.db.execute(
                'INSERT INTO account_budget_instance_limits'
                '(instance_id,max_cash,max_planned_loss,max_notional,'
                'max_concurrent,updated_at) VALUES(?,?,?,?,?,?)'
                ' ON CONFLICT(instance_id) DO UPDATE SET'
                ' max_cash=excluded.max_cash,'
                ' max_planned_loss=excluded.max_planned_loss,'
                ' max_notional=excluded.max_notional,'
                ' max_concurrent=excluded.max_concurrent,'
                ' updated_at=excluded.updated_at',
                (instance_id, str(budget.max_cash), str(budget.max_planned_loss),
                 str(budget.max_notional), budget.max_concurrent, now))
            self.db.execute(
                'INSERT INTO account_budget_reservations'
                '(instance_id,trade_id,cash,planned_loss,notional,reserved_at)'
                ' VALUES(?,?,?,?,?,?)',
                (instance_id, trade_id, str(cash), str(planned_loss),
                 str(notional), now))
            return {'reserved': True, 'instance_id': instance_id,
                    'trade_id': trade_id, 'cash': str(cash),
                    'planned_loss': str(planned_loss), 'notional': str(notional)}

    def release_in_transaction(self, instance_id, trade_id):
        """Release inside a caller-owned transaction, for settlement."""
        self._ensure_open()
        if not self.db.in_transaction:
            raise ValueError('BUDGET_TRANSACTION_REQUIRED')
        return self.db.execute(
            'DELETE FROM account_budget_reservations'
            ' WHERE instance_id=? AND trade_id=?',
            (instance_id, trade_id)).rowcount > 0

    def release(self, instance_id, trade_id):
        """Remove one instance's reservation; never another instance's."""
        self._ensure_open()
        self.db.execute('BEGIN IMMEDIATE')
        try:
            removed = self.db.execute(
                'DELETE FROM account_budget_reservations'
                ' WHERE instance_id=? AND trade_id=?',
                (instance_id, trade_id)).rowcount > 0
            self.db.commit()
            return removed
        except BaseException:
            self.db.rollback()
            raise

    # -- reporting -------------------------------------------------------
    def status(self):
        """Current headroom from durable rows, never from in-memory state."""
        self._ensure_open()
        limits = self._read_limits()
        ceilings = {}
        for row in self.db.execute(
                'SELECT instance_id,max_cash,max_planned_loss,max_notional,'
                'max_concurrent FROM account_budget_instance_limits'):
            ceilings[row[0]] = {
                'cash': Decimal(row[1]), 'planned_loss': Decimal(row[2]),
                'notional': Decimal(row[3]), 'trades': row[4]}

        totals = {}
        account_total = _totals()
        for row in self.db.execute(
                'SELECT instance_id,cash,planned_loss,notional'
                ' FROM account_budget_reservations'):
            cash, planned_loss, notional = (
                Decimal(row[1]), Decimal(row[2]), Decimal(row[3]))
            _accumulate(totals.setdefault(row[0], _totals()),
                        cash, planned_loss, notional)
            _accumulate(account_total, cash, planned_loss, notional)

        per_instance = {}
        for instance_id in sorted(set(ceilings) | set(totals)):
            if instance_id not in ceilings:
                raise ValueError('BUDGET_STATE_INCONSISTENT')
            open_totals = totals.get(instance_id, _totals())
            ceiling = ceilings[instance_id]
            per_instance[instance_id] = {
                'open_cash': str(open_totals['cash']),
                'open_notional': str(open_totals['notional']),
                'open_planned_loss': str(open_totals['planned_loss']),
                'open_trades': open_totals['trades'],
                'max_cash': str(ceiling['cash']),
                'max_notional': str(ceiling['notional']),
                'max_planned_loss': str(ceiling['planned_loss']),
                'max_concurrent': ceiling['trades'],
                'available': (open_totals['trades'] < ceiling['trades']
                              and open_totals['cash'] < ceiling['cash']
                              and open_totals['notional'] < ceiling['notional']
                              and open_totals['planned_loss']
                              < ceiling['planned_loss']),
            }

        return {
            'open_cash': str(account_total['cash']),
            'open_notional': str(account_total['notional']),
            'open_planned_loss': str(account_total['planned_loss']),
            'open_trades': account_total['trades'],
            'max_open_cash': str(limits['max_open_cash']),
            'max_open_notional': str(limits['max_open_notional']),
            'max_open_planned_loss': str(limits['max_open_planned_loss']),
            'max_concurrent_trades': limits['max_concurrent_trades'],
            'per_instance': per_instance,
            'capacity_available': (
                account_total['trades'] < limits['max_concurrent_trades']
                and account_total['cash'] < limits['max_open_cash']
                and account_total['notional'] < limits['max_open_notional']
                and account_total['planned_loss'] < limits['max_open_planned_loss']),
        }

    def instance_status(self, instance_id):
        """One instance's entry, shaped exactly like ``status()['per_instance']``."""
        self._ensure_open()
        per_instance = self.status()['per_instance']
        if instance_id not in per_instance:
            raise ValueError('BUDGET_INSTANCE_UNKNOWN')
        return per_instance[instance_id]

    # -- lifecycle -------------------------------------------------------
    def close(self):
        """Retire this authority. The caller-owned connection stays open."""
        self._closed = True

    def _ensure_open(self):
        if self._closed:
            raise ValueError('BUDGET_CLOSED')

    def _read_limits(self):
        row = self.db.execute(
            'SELECT max_open_cash,max_open_notional,max_open_planned_loss,'
            'max_concurrent_trades FROM account_budget_limits WHERE id=1'
        ).fetchone()
        if row is None:
            raise ValueError('BUDGET_LIMITS_INVALID')
        return {'max_open_cash': Decimal(row[0]),
                'max_open_notional': Decimal(row[1]),
                'max_open_planned_loss': Decimal(row[2]),
                'max_concurrent_trades': row[3]}

    def _now(self):
        return self.clock().astimezone(timezone.utc).isoformat()
