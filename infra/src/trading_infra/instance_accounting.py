"""Per-instance fill, fee and realised-PnL accounting.

Several instances share one account, so "the account made $40" is not an answer
a strategy review can use: the infrastructure has to say which instance earned
it. This module keeps that ledger.

It knows nothing about instruments beyond the multiplier its caller supplies.
An option fill at premium 2.60 is worth 260.00 only because the adapter said the
multiplier is 100; this module never infers a multiplier from a symbol, and
never treats a missing multiplier as 1.

Fees are never multiplied. They are charged per contract or per order by the
venue, not per unit of notional.
"""
from datetime import datetime, timezone
from decimal import Decimal

from trading_contracts.values import digest, number
from .transactions import atomic

_SCHEMA = '''
CREATE TABLE IF NOT EXISTS instance_fills(
    instance_id TEXT NOT NULL, trade_id TEXT NOT NULL, fill_id TEXT NOT NULL,
    side TEXT NOT NULL CHECK(side IN ('buy','sell')),
    quantity TEXT NOT NULL, price TEXT NOT NULL, multiplier INTEGER NOT NULL,
    fees TEXT NOT NULL, cash_delta TEXT NOT NULL, occurred_at TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    PRIMARY KEY(instance_id, trade_id, fill_id));
CREATE INDEX IF NOT EXISTS instance_fills_instance
    ON instance_fills(instance_id, occurred_at);
CREATE TABLE IF NOT EXISTS execution_fill_identity(
    execution_key TEXT PRIMARY KEY, instance_id TEXT NOT NULL,
    trade_id TEXT NOT NULL, fill_id TEXT NOT NULL,
    payload_digest TEXT NOT NULL, recorded_at TEXT NOT NULL);
'''

SIDES = ('buy', 'sell')


def _text(value, name):
    if not isinstance(value, str) or not value or len(value) > 128:
        raise ValueError('ACCOUNTING_' + name + '_INVALID')
    return value


def _decimal(value, name, *, allow_zero=True):
    try:
        result = number(value)
    except ValueError:
        raise ValueError('ACCOUNTING_' + name + '_INVALID') from None
    if result < 0 or (not allow_zero and result == 0):
        raise ValueError('ACCOUNTING_' + name + '_INVALID')
    return result


class InstanceAccounting:
    """Realised cash, fees and PnL per instance and per trade. It places nothing."""

    def __init__(self, db, *, clock=None):
        self.db = db
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.db.executescript(_SCHEMA)

    def _now(self):
        return self.clock().astimezone(timezone.utc).isoformat()

    # -- writes ----------------------------------------------------------
    def record_synthetic_fill(self, instance_id, trade_id, **fields):
        """Record a locally generated fill under a namespaced identity.

        Synthetic fills are not venue executions, so they get a key that cannot
        collide with a broker one. They are still one-per-identity, and a
        synthetic fill can never claim a venue key.
        """
        key = 'synthetic:%s:%s:%s' % (instance_id, trade_id, fields.get('fill_id'))
        return self._record(instance_id, trade_id, execution_key=key, **fields)

    def record_fill(self, instance_id, trade_id, *, execution_key, **fields):
        """Record one venue execution. An account-level identity is mandatory.

        Two instances booking the same broker execution is how the account
        showed one fill as two cash flows, so this entry point refuses to run
        without an execution identity. Use ``record_synthetic_fill`` for local
        simulation.
        """
        if not isinstance(execution_key, str) or not execution_key:
            raise ValueError('ACCOUNTING_EXECUTION_KEY_REQUIRED')
        return self._record(instance_id, trade_id, execution_key=execution_key, **fields)

    def _record(self, instance_id, trade_id, *, fill_id, side, quantity, price,
                multiplier=1, fees='0', occurred_at=None, execution_key=None):
        """Record one execution. Idempotent by (instance, trade, fill).

        Returns ``{'recorded': bool, 'fill_id', 'cash_delta', 'fees'}``; a
        redelivery with identical terms reports ``recorded: False`` and changes
        nothing, while different terms under the same identity is a conflict.
        """
        instance_id = _text(instance_id, 'INSTANCE_ID')
        trade_id = _text(trade_id, 'TRADE_ID')
        fill_id = _text(fill_id, 'FILL_ID')
        if side not in SIDES:
            raise ValueError('ACCOUNTING_SIDE_INVALID')
        if type(multiplier) is not int or multiplier <= 0:
            raise ValueError('ACCOUNTING_MULTIPLIER_INVALID')
        amount = _decimal(quantity, 'QUANTITY', allow_zero=False)
        unit = _decimal(price, 'PRICE')
        fee = _decimal(fees, 'FEES')
        stamp = occurred_at or self._now()
        # Gross is multiplied; the venue fee is not.
        gross = amount * unit * multiplier
        cash_delta = (gross if side == 'sell' else -gross) - fee
        with atomic(self.db):
            if execution_key is not None:
                # One venue execution belongs to one instance, once. Without an
                # account-level identity the same broker fill could be booked to
                # two instances and both would show the cash.
                claimed = self.db.execute(
                    'SELECT instance_id,trade_id,fill_id,payload_digest'
                    ' FROM execution_fill_identity WHERE execution_key=?',
                    (execution_key,)).fetchone()
                digest_value = digest(['execution-fill-1', side, str(amount), str(unit),
                                       multiplier, str(fee), str(stamp)])
                if claimed is not None:
                    same = ((claimed[0], claimed[1], claimed[2]) ==
                            (instance_id, trade_id, fill_id)
                            and claimed[3] == digest_value)
                    if same:
                        return {'recorded': False, 'fill_id': fill_id,
                                'cash_delta': str(cash_delta), 'fees': str(fee)}
                    raise ValueError('ACCOUNTING_EXECUTION_ALREADY_ATTRIBUTED')
            prior = self.db.execute(
                'SELECT side,quantity,price,multiplier,fees,occurred_at,cash_delta'
                ' FROM instance_fills'
                ' WHERE instance_id=? AND trade_id=? AND fill_id=?',
                (instance_id, trade_id, fill_id)).fetchone()
            if prior is not None:
                # Cash flow alone is not identity: buy 1 at 40 and buy 2 at 20
                # both move -40. A redelivery must match every economic field,
                # or the two fills would be silently treated as one.
                expected = (side, str(amount), str(unit), multiplier, str(fee), str(stamp))
                actual = (prior[0], prior[1], prior[2], prior[3], prior[4], prior[5])
                if actual != expected:
                    raise ValueError('ACCOUNTING_FILL_CONFLICT')
                if execution_key is not None:
                    # A fill recorded before account-level identity existed must
                    # adopt it on replay. Returning here without registering the
                    # key would let another instance claim the same execution.
                    held = self.db.execute(
                        'SELECT instance_id,trade_id,fill_id FROM execution_fill_identity'
                        ' WHERE execution_key=?', (execution_key,)).fetchone()
                    if held is not None and (held[0], held[1], held[2]) != (
                            instance_id, trade_id, fill_id):
                        raise ValueError('ACCOUNTING_EXECUTION_ALREADY_ATTRIBUTED')
                    if held is None:
                        self.db.execute(
                            'INSERT INTO execution_fill_identity(execution_key,'
                            ' instance_id,trade_id,fill_id,payload_digest,recorded_at)'
                            ' VALUES(?,?,?,?,?,?)',
                            (execution_key, instance_id, trade_id, fill_id,
                             digest(['execution-fill-1', side, str(amount), str(unit),
                                     multiplier, str(fee), str(stamp)]), self._now()))
                        # The adopted identity commits with the surrounding
                        # transaction; dropping it here would let another
                        # instance claim the same execution next time.
                        return {'recorded': False, 'fill_id': fill_id,
                                'cash_delta': prior[6], 'fees': prior[4],
                                'identity_adopted': True}
                return {'recorded': False, 'fill_id': fill_id,
                        'cash_delta': prior[6], 'fees': prior[4]}
            if side == 'sell':
                # Fail the write, not a later report: a sale that cannot be
                # matched against a bought lot is bad data, and leaving it in
                # the ledger would silently corrupt every later summary.
                self._check_sell_matches(instance_id, trade_id, amount, multiplier,
                                         occurred_at=str(stamp))
            self.db.execute(
                'INSERT INTO instance_fills(instance_id,trade_id,fill_id,side,quantity,price,'
                ' multiplier,fees,cash_delta,occurred_at,recorded_at)'
                ' VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                (instance_id, trade_id, fill_id, side, str(amount), str(unit), multiplier,
                 str(fee), str(cash_delta), str(stamp), self._now()))
            if execution_key is not None:
                self.db.execute(
                    'INSERT INTO execution_fill_identity(execution_key,instance_id,'
                    ' trade_id,fill_id,payload_digest,recorded_at) VALUES(?,?,?,?,?,?)',
                    (execution_key, instance_id, trade_id, fill_id,
                     digest(['execution-fill-1', side, str(amount), str(unit), multiplier,
                             str(fee), str(stamp)]), self._now()))
            return {'recorded': True, 'fill_id': fill_id,
                    'cash_delta': str(cash_delta), 'fees': str(fee)}

    def _check_sell_matches(self, instance_id, trade_id, quantity, multiplier,
                            *, occurred_at=None):
        """A sale may only be matched against lots that already existed.

        Matching a later buy would invent a position that did not exist when the
        sale happened. Time is the constraint: a lot bought after the sell is
        not a basis for it.
        """
        open_lots = []
        for row in self.fills(instance_id, trade_id=trade_id):
            if occurred_at is not None and row['occurred_at'] > occurred_at:
                continue
            if row['side'] == 'buy':
                open_lots.append([Decimal(row['quantity']), row['multiplier']])
                continue
            remaining = Decimal(row['quantity'])
            while remaining > 0 and open_lots:
                matched = min(remaining, open_lots[0][0])
                open_lots[0][0] -= matched
                remaining -= matched
                if open_lots[0][0] == 0:
                    open_lots.pop(0)
        available = sum((lot[0] for lot in open_lots), Decimal(0))
        if quantity > available:
            raise ValueError('ACCOUNTING_POSITION_UNDERFLOW')
        if open_lots and open_lots[0][1] != multiplier:
            raise ValueError('ACCOUNTING_MULTIPLIER_MISMATCH')

    # -- reads -----------------------------------------------------------
    def fills(self, instance_id, *, trade_id=None):
        query = ('SELECT trade_id,fill_id,side,quantity,price,multiplier,fees,cash_delta,'
                 ' occurred_at FROM instance_fills WHERE instance_id=?')
        arguments = [instance_id]
        if trade_id is not None:
            query += ' AND trade_id=?'
            arguments.append(trade_id)
        query += ' ORDER BY occurred_at,fill_id'
        return [{'trade_id': row[0], 'fill_id': row[1], 'side': row[2],
                 'quantity': row[3], 'price': row[4], 'multiplier': row[5],
                 'fees': row[6], 'cash_delta': row[7], 'occurred_at': row[8]}
                for row in self.db.execute(query, arguments)]

    def _summarise(self, rows):
        """FIFO the fills. Cash flow is not profit while a position is open.

        Reporting total cash flow as realised PnL would show an open position as
        a loss equal to its cost basis. Realised PnL is gross of fees; the venue
        fee is reported separately and netted only in ``net_cash_flow``.
        """
        lots = []
        realised = Decimal(0)
        fees = Decimal(0)
        cash = Decimal(0)
        bought = Decimal(0)
        sold = Decimal(0)
        for row in rows:
            quantity = Decimal(row['quantity'])
            price = Decimal(row['price'])
            cash += Decimal(row['cash_delta'])
            fees += Decimal(row['fees'])
            if row['side'] == 'buy':
                bought += quantity
                lots.append([quantity, price, row['multiplier']])
                continue
            sold += quantity
            remaining = quantity
            # Only lots that existed at the sale's own time can be matched; a
            # later purchase is not a basis for an earlier sale.
            while remaining > 0:
                if not lots:
                    raise ValueError('ACCOUNTING_POSITION_UNDERFLOW')
                lot = lots[0]
                if lot[2] != row['multiplier']:
                    raise ValueError('ACCOUNTING_MULTIPLIER_MISMATCH')
                matched = min(remaining, lot[0])
                realised += (price - lot[1]) * matched * row['multiplier']
                lot[0] -= matched
                remaining -= matched
                if lot[0] == 0:
                    lots.pop(0)
        open_quantity = sum((lot[0] for lot in lots), Decimal(0))
        open_basis = sum((lot[0] * lot[1] * lot[2] for lot in lots), Decimal(0))
        return {'fills': len(rows), 'net_cash_flow': str(cash), 'fees': str(fees),
                'realised_pnl': str(realised), 'quantity_bought': str(bought),
                'quantity_sold': str(sold), 'open_quantity': str(open_quantity),
                'open_cost_basis': str(open_basis),
                'digest': digest(['instance-accounting-1',
                                  [[row['fill_id'], row['cash_delta'], row['fees']]
                                   for row in rows]])}

    def trade_summary(self, instance_id, trade_id):
        return self._summarise(self.fills(instance_id, trade_id=trade_id))

    def instance_summary(self, instance_id):
        rows = self.fills(instance_id)
        summary = self._summarise(rows)
        summary['trades'] = sorted({row['trade_id'] for row in rows})
        summary['instance_id'] = instance_id
        return summary

    def all_instances(self):
        return [row[0] for row in self.db.execute(
            'SELECT DISTINCT instance_id FROM instance_fills ORDER BY instance_id')]
