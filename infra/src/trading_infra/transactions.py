"""One rule for who owns a SQLite commit.

The execution book is opened with the driver's implicit transaction mode, while
the infrastructure components sometimes open ``BEGIN IMMEDIATE`` themselves.
Mixing the two left writes uncommitted after a dispatch, which surfaced as
"cannot start a transaction within a transaction" on the next reconciliation.

``atomic`` makes ownership explicit and local: a caller already inside a
transaction keeps it (and commits), a caller outside gets one opened, committed
and rolled back for it. No component guesses, and no code path leaves a
half-finished transaction behind.
"""
from contextlib import contextmanager


@contextmanager
def atomic(db):
    """Run a block as one transaction, or join the caller's existing one."""
    if not isinstance(getattr(db, 'in_transaction', None), bool):
        raise ValueError('TRANSACTION_CONNECTION_REQUIRED')
    if db.in_transaction:
        # The caller owns begin/commit; a nested begin would raise, and
        # committing here would end their transaction early.
        yield False
        return
    db.execute('BEGIN IMMEDIATE')
    try:
        yield True
        db.commit()
    except BaseException:
        db.rollback()
        raise
