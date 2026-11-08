"""Import authoritative broker order and execution facts into instance books.

The book already reconciles orders against its own actions. What it did not do
is turn those authoritative executions into per-instance feedback, accounting
and release facts: the fake-broker round trip only closed because tests called
``record_fill`` and ``settle`` by hand.

This importer closes that gap on the owner thread:

* routing is by reference, never by symbol — an order without a known reference
  is quarantined rather than attributed to whoever happens to trade that ticker;
* the identity of a fill is the account, the order and the broker's own
  execution id, so a replay, a partial and a late arrival are all the same fact
  arriving again, not a second fill;
* the broker's own execution timestamp is the occurrence time; the observation
  time is recorded separately and never rewrites it;
* every write joins one transaction on whatever connection the book uses, so it
  works on the default implicit-transaction handle and not only on an autocommit
  test connection.

Nothing here places, cancels or confirms an order. A receipt is evidence of
what the broker already did.
"""
from datetime import datetime, timezone

from trading_contracts.values import canonical, digest, number, timestamp
from .transactions import atomic

TERMINAL_ORDER_STATES = frozenset({'filled', 'cancelled', 'canceled', 'rejected',
                                   'expired'})
CANCELLED_STATES = frozenset({'cancelled', 'canceled'})
ACTIVE_ACTION_STATES = frozenset({'READY', 'RUNNING', 'REPORTED', 'UNKNOWN',
                                  'WAITING_USER'})

_SCHEMA = '''
CREATE TABLE IF NOT EXISTS receipt_order_aliases(
    order_id TEXT PRIMARY KEY, ref_id TEXT NOT NULL, account_key TEXT NOT NULL,
    bound_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS receipt_quarantine(
    receipt_key TEXT PRIMARY KEY, account_key TEXT, observed_at TEXT NOT NULL,
    reason TEXT NOT NULL, payload TEXT NOT NULL, recorded_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS receipt_pending_executions(
    account_key TEXT NOT NULL, order_id TEXT NOT NULL, execution_id TEXT NOT NULL,
    ref_id TEXT NOT NULL, body TEXT NOT NULL, first_seen_at TEXT NOT NULL,
    PRIMARY KEY(account_key,order_id,execution_id));
CREATE TABLE IF NOT EXISTS receipt_state_first_seen(
    account_key TEXT NOT NULL, order_id TEXT NOT NULL, state TEXT NOT NULL,
    first_observed_at TEXT NOT NULL, PRIMARY KEY(account_key,order_id,state));
CREATE TABLE IF NOT EXISTS execution_identity(
    identity_key TEXT PRIMARY KEY, provider TEXT NOT NULL,
    account_key TEXT NOT NULL, execution_id TEXT NOT NULL,
    order_id TEXT, ref_id TEXT NOT NULL, instance_id TEXT NOT NULL,
    trade_id TEXT NOT NULL, economic_digest TEXT NOT NULL,
    occurred_at TEXT NOT NULL, first_seen_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS execution_identity_conflict(
    conflict_id TEXT PRIMARY KEY, identity_key TEXT NOT NULL,
    evidence_source TEXT,
    provider TEXT NOT NULL, account_key TEXT NOT NULL, execution_id TEXT NOT NULL,
    accepted_order_id TEXT, accepted_ref_id TEXT, accepted_instance_id TEXT,
    accepted_trade_id TEXT, accepted_economic_digest TEXT, accepted_occurred_at TEXT,
    rejected_order_id TEXT, rejected_ref_id TEXT, rejected_instance_id TEXT,
    rejected_trade_id TEXT, rejected_economic_digest TEXT, rejected_occurred_at TEXT,
    first_seen_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS receipt_identity_quarantine(
    identity_key TEXT PRIMARY KEY, provider TEXT, account_key TEXT,
    execution_id TEXT, reason TEXT NOT NULL, detail TEXT NOT NULL,
    recorded_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS receipt_identity_migration(version INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS receipt_order_progress(
    account_key TEXT NOT NULL, order_id TEXT NOT NULL, state TEXT NOT NULL,
    terminal INTEGER NOT NULL, observed_at TEXT NOT NULL,
    PRIMARY KEY(account_key, order_id));
'''


#: Broker order state to the neutral feedback kind it represents. Each entry is
#: a distinct immutable fact, so each gets its own event identity.
STATE_KINDS = {
    'partially_filled': 'partial_fill',
    'filled': 'filled',
    'cancelled': 'cancel_terminal',
    'canceled': 'cancel_terminal',
    'rejected': 'rejected',
    'unknown': 'unknown',
}


def _thaw(body):
    import json
    return json.loads(body)


class _IdentityConflict(Exception):
    """A rejected claim about an execution whose binding is already settled."""

    def __init__(self, *, account_key, execution_id, order_id, ref, owner,
                 economic_digest, occurred):
        super().__init__('RECEIPT_EXECUTION_IDENTITY_CONFLICT')
        self.account_key = account_key
        self.execution_id = execution_id
        self.order_id = order_id
        self.ref = ref
        self.owner = owner
        self.economic_digest = economic_digest
        self.occurred = occurred


class _AwaitingBasis(Exception):
    """Raised when a sale has no lot that existed before it."""


def _now(clock):
    return clock().astimezone(timezone.utc).isoformat()


#: The single provider this importer may be constructed with. An execution id is
#: only unique inside one provider and one formal account; mixing providers
#: without an explicit scope would let one venue's ids collide with another's.
SUPPORTED_PROVIDER = 'robinhood'
IDENTITY_MIGRATION_VERSION = 1


class ExecutionReceiptImporter:
    """Route broker receipts to their owning instance, once each."""

    def __init__(self, db, *, attribution, accounting, clock=None,
                 provider=SUPPORTED_PROVIDER, baseline=None):
        for name, value in (('attribution', attribution), ('accounting', accounting)):
            if value is None:
                raise ValueError('RECEIPT_COMPONENT_REQUIRED')
        if getattr(attribution, 'db', None) is not db or getattr(accounting, 'db', None) is not db:
            raise ValueError('RECEIPT_SINGLE_CONNECTION_REQUIRED')
        if baseline is not None and getattr(baseline, 'db', None) is not db:
            raise ValueError('RECEIPT_BASELINE_CONNECTION_REQUIRED')
        self.db = db
        self.attribution = attribution
        self.accounting = accounting
        self.baseline = baseline
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        if provider != SUPPORTED_PROVIDER:
            # Identity scoping is provider + account + execution id. A second
            # provider would need its own importer instance and its own scope,
            # which this interface does not provide.
            raise ValueError('RECEIPT_PROVIDER_UNSUPPORTED')
        self.provider = provider
        self.db.executescript(_SCHEMA)
        self.migrate_identity()

    # -- migration -------------------------------------------------------
    def migrate_identity(self):
        """Translate order-scoped legacy fill keys to stable execution identity.

        The old ``execution_fill_identity`` keyed on ``account|order|execution``,
        so the same execution under another order produced a second key — the
        defect this round fixes. Legacy rows are kept, never deleted, and mapped
        to the stable identity using the single supported provider. Rows whose
        execution already has a different bound owner are quarantined and the
        importer fails closed rather than guessing which one is real.

        The version marker is written last, so an interrupted run simply runs
        again; the operation is idempotent.
        """
        recorded = self.db.execute(
            'SELECT max(version) FROM receipt_identity_migration').fetchone()[0]
        if recorded == IDENTITY_MIGRATION_VERSION:
            return
        legacy = self.db.execute(
            'SELECT execution_key,instance_id,trade_id,fill_id,payload_digest,'
            ' recorded_at FROM execution_fill_identity').fetchall()
        quarantined = []
        with atomic(self.db):
            for execution_key, instance_id, trade_id, fill_id, payload, recorded_at \
                    in legacy:
                parts = str(execution_key).split('|')
                if len(parts) != 3:
                    quarantined.append((execution_key, 'RECEIPT_IDENTITY_UNPARSEABLE'))
                    continue
                account_key, order_id, execution_id = parts
                key = self.identity_key(account_key, execution_id)
                owner_ref = self.db.execute(
                    'SELECT ref_id FROM execution_attribution WHERE instance_id=?'
                    ' AND trade_id=? LIMIT 1', (instance_id, trade_id)).fetchone()
                ref = owner_ref[0] if owner_ref else execution_key
                prior = self.db.execute(
                    'SELECT order_id,ref_id,instance_id,trade_id,economic_digest'
                    ' FROM execution_identity WHERE identity_key=?', (key,)).fetchone()
                if prior is not None:
                    same = (prior[2], prior[3]) == (instance_id, trade_id) and \
                           prior[4] == payload
                    if not same:
                        quarantined.append((execution_key,
                                            'RECEIPT_IDENTITY_CONFLICT'))
                    continue
                # The occurrence time must be the broker's own execution time,
                # which the accounting row already holds. Falling back to the
                # legacy row's record time would make a replay of the same
                # execution look like a different fact.
                occurred = self.db.execute(
                    'SELECT occurred_at FROM instance_fills'
                    ' WHERE instance_id=? AND trade_id=? AND fill_id=?',
                    (instance_id, trade_id, fill_id)).fetchall()
                if len(occurred) != 1:
                    quarantined.append((execution_key,
                                        'RECEIPT_IDENTITY_HISTORY_AMBIGUOUS'))
                    continue
                self.db.execute(
                    'INSERT INTO execution_identity(identity_key,provider,account_key,'
                    ' execution_id,order_id,ref_id,instance_id,trade_id,'
                    ' economic_digest,occurred_at,first_seen_at)'
                    ' VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                    (key, self.provider, account_key, execution_id, order_id, ref,
                     instance_id, trade_id, payload, occurred[0][0], recorded_at))
            for execution_key, reason in quarantined:
                self.db.execute(
                    'INSERT OR IGNORE INTO receipt_identity_quarantine(identity_key,'
                    ' provider,account_key,execution_id,reason,detail,recorded_at)'
                    ' VALUES(?,?,?,?,?,?,?)',
                    (execution_key, self.provider, None, None, reason,
                     execution_key, self.clock().astimezone(timezone.utc).isoformat()))
            self.db.execute('DELETE FROM receipt_identity_migration')
            self.db.execute('INSERT INTO receipt_identity_migration(version) VALUES(?)',
                            (IDENTITY_MIGRATION_VERSION,))
        self.identity_quarantined = [item[0] for item in quarantined]

    def identity_conflicts(self):
        """Every unresolved identity conflict: migration and live, both durable."""
        conflicts = [{'conflict_id': row[0], 'identity_key': row[1],
                      'provider': row[2], 'account_key': row[3],
                      'execution_id': row[4],
                      'accepted': {'order_id': row[5], 'ref_id': row[6],
                                   'instance_id': row[7], 'trade_id': row[8],
                                   'economic_digest': row[9], 'occurred_at': row[10]},
                      'rejected': {'order_id': row[11], 'ref_id': row[12],
                                   'instance_id': row[13], 'trade_id': row[14],
                                   'economic_digest': row[15], 'occurred_at': row[16]},
                      'first_seen_at': row[17], 'source': 'live'}
                     for row in self.db.execute(
                         'SELECT conflict_id,identity_key,provider,account_key,'
                         ' execution_id,accepted_order_id,accepted_ref_id,'
                         ' accepted_instance_id,accepted_trade_id,'
                         ' accepted_economic_digest,accepted_occurred_at,'
                         ' rejected_order_id,rejected_ref_id,rejected_instance_id,'
                         ' rejected_trade_id,rejected_economic_digest,'
                         ' rejected_occurred_at,first_seen_at'
                         ' FROM execution_identity_conflict'
                         ' ORDER BY first_seen_at,conflict_id')]
        conflicts.extend({'identity_key': row[0], 'reason': row[1],
                          'source': 'migration'}
                         for row in self.db.execute(
                             'SELECT identity_key,reason FROM receipt_identity_quarantine'
                             ' ORDER BY recorded_at'))
        return conflicts

    # -- aliases ---------------------------------------------------------
    def alias(self, order_id, ref_id, account_key):
        """Record that a broker order id belongs to one reference. Never reassign."""
        if not all(isinstance(value, str) and value
                   for value in (order_id, ref_id, account_key)):
            raise ValueError('RECEIPT_ALIAS_INVALID')
        prior = self.db.execute(
            'SELECT ref_id,account_key FROM receipt_order_aliases WHERE order_id=?',
            (order_id,)).fetchone()
        if prior is not None:
            if (prior[0], prior[1]) != (ref_id, account_key):
                raise ValueError('RECEIPT_ALIAS_CONFLICT')
            return {'order_id': order_id, 'ref_id': ref_id, 'duplicate': True}
        with atomic(self.db):
            self.db.execute(
                'INSERT INTO receipt_order_aliases(order_id,ref_id,account_key,bound_at)'
                ' VALUES(?,?,?,?)', (order_id, ref_id, account_key, _now(self.clock)))
        return {'order_id': order_id, 'ref_id': ref_id, 'duplicate': False}

    def _live_overlap(self, order, ref, account_key):
        """True when the exact historical identity already belongs to live state."""
        def has_table(name):
            return self.db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (name,)).fetchone() is not None
        if ref is not None and self.attribution.owner(ref) is not None:
            return True
        order_id = order.get('id') or order.get('order_id')
        for table, column, value in (
                ('receipt_order_aliases', 'order_id', order_id),
                ('receipt_order_progress', 'order_id', order_id)):
            if value and self.db.execute(
                    'SELECT 1 FROM '+table+' WHERE '+column+'=? LIMIT 1',
                    (value,)).fetchone():
                return True
        for execution in order.get('executions') or ():
            execution_id = execution.get('id') if isinstance(execution, dict) else None
            if not execution_id:
                continue
            key = self.identity_key(account_key, execution_id)
            if self.db.execute(
                    'SELECT 1 FROM execution_identity WHERE identity_key=? LIMIT 1',
                    (key,)).fetchone():
                return True
            if has_table('execution_fill_identity') and self.db.execute(
                    'SELECT 1 FROM execution_fill_identity WHERE execution_key=? LIMIT 1',
                    (key,)).fetchone():
                return True
            if has_table('instance_fills') and self.db.execute(
                    'SELECT 1 FROM instance_fills WHERE fill_id=? LIMIT 1',
                    (execution_id,)).fetchone():
                return True
        return False

    def resolve(self, order, account_key):
        """The reference an order belongs to, or ``None`` when it cannot be known."""
        ref = order.get('ref_id')
        if isinstance(ref, str) and ref:
            return ref
        order_id = order.get('id') or order.get('order_id')
        if not isinstance(order_id, str) or not order_id:
            return None
        row = self.db.execute(
            'SELECT ref_id FROM receipt_order_aliases WHERE order_id=? AND account_key=?',
            (order_id, account_key)).fetchone()
        return row[0] if row else None

    # -- import ----------------------------------------------------------
    def import_snapshot(self, snapshot):
        """Import one snapshot with one consistent baseline-index view."""
        baseline = self.baseline
        begin = getattr(baseline, 'begin_import', None)
        end = getattr(baseline, 'end_import', None)
        if callable(begin):
            begin()
        try:
            return self._import_snapshot(snapshot)
        finally:
            if callable(end):
                end()

    def _import_snapshot(self, snapshot):
        """Import one account-bound broker snapshot. Idempotent per receipt key.

        The snapshot must be complete and carry its own observation time. An
        empty or partial order list is not evidence that nothing is open, so a
        snapshot that cannot say is refused rather than read as "all clear".
        """
        if not isinstance(snapshot, dict):
            raise ValueError('RECEIPT_SNAPSHOT_REQUIRED')
        account_key = snapshot.get('account_key')
        observed_at = snapshot.get('observed_at')
        orders = snapshot.get('orders')
        if not isinstance(account_key, str) or not account_key:
            raise ValueError('RECEIPT_ACCOUNT_REQUIRED')
        if not isinstance(observed_at, str) or not observed_at:
            raise ValueError('RECEIPT_OBSERVED_AT_REQUIRED')
        timestamp(observed_at)
        if snapshot.get('collection_complete') is not True or account_key != snapshot.get('account_key'):
            raise ValueError('RECEIPT_SNAPSHOT_INCOMPLETE')
        if not isinstance(orders, dict):
            raise ValueError('RECEIPT_ORDERS_REQUIRED')

        imported, duplicated, quarantined, deferred = [], [], [], []
        baseline_matched, routed = [], []
        for key, order in orders.items():
            if not isinstance(order, dict):
                raise ValueError('RECEIPT_ORDER_INVALID')
            ref = self.resolve(order, account_key)
            order_id = order.get('id') or order.get('order_id')
            baseline = (self.baseline.match_order(order, account_key, self.provider)
                        if self.baseline is not None else {'status': 'none'})
            if baseline.get('status') == 'conflict':
                quarantined.append(self._quarantine(
                    order, account_key=account_key, observed_at=observed_at,
                    reason=baseline.get('reason') or 'BASELINE_CONFLICT'))
                continue
            if baseline.get('status') == 'matched':
                # Baseline is a separate namespace.  A later live owner or
                # reused execution is a conflict, never a live-first override.
                if self._live_overlap(order, ref, account_key):
                    quarantined.append(self._quarantine(
                        order, account_key=account_key, observed_at=observed_at,
                        reason='BASELINE_LIVE_OWNER_CONFLICT'))
                    continue
                baseline_matched.append({'baseline_id': baseline.get('baseline_id'),
                                         'order_id': order_id,
                                         'order_key': baseline.get('order_key')})
                continue
            if ref is None:
                quarantined.append(self._quarantine(
                    order, account_key=account_key, observed_at=observed_at,
                    reason='RECEIPT_OWNER_UNKNOWN'))
                continue
            owner = self.attribution.owner(ref)
            if owner is None:
                quarantined.append(self._quarantine(
                    order, account_key=account_key, observed_at=observed_at,
                    reason='RECEIPT_REFERENCE_UNBOUND'))
                continue
            routed.append((order, ref, owner, order_id))

        # Executions are ordered by when the venue says they happened, not by
        # the order the provider happened to list the orders in. A snapshot may
        # return the exit order before the entry order while the executions are
        # still buy-before-sell; importing per order would try to sell first and
        # underflow forever.
        chronological = []
        for order, ref, owner, order_id in routed:
            for execution in order.get('executions') or ():
                chronological.append((self._occurred(execution, observed_at),
                                      str(execution.get('id') or ''), order, ref,
                                      owner, order_id, execution))
        chronological.sort(key=lambda item: (item[0], item[1]))

        for occurred, _eid, order, ref, owner, order_id, execution in chronological:
            try:
                outcome = self._import_execution(
                    execution, owner=owner, ref=ref, account_key=account_key,
                    order_id=order_id,
                    order_state=str(order.get('state') or '').lower(),
                    observed_at=observed_at,
                    order_filled=str(order.get('state') or '').lower() == 'filled',
                    occurred=occurred)
            except _AwaitingBasis:
                # The sale has no lot that existed before it. Hold it with its
                # own identity and time instead of booking a negative position
                # or inventing an opening buy.
                self._defer_execution(execution, account_key=account_key,
                                      order_id=order_id or ref, ref=ref,
                                      observed_at=observed_at)
                deferred.append({'status': 'deferred', 'execution_id':
                                 execution.get('id'),
                                 'reason': 'RECEIPT_AWAITING_BASIS'})
                continue
            if outcome['status'] == 'imported':
                imported.append(outcome)
            elif outcome['status'] == 'duplicate':
                duplicated.append(outcome)
            else:
                deferred.append(outcome)

        orders_recorded = []
        for order, ref, owner, order_id in routed:
            orders_recorded.append(self._record_order_state(
                order, ref=ref, owner=owner, order_id=order_id,
                account_key=account_key, observed_at=observed_at))

        imported.extend(self._retry_deferred(account_key=account_key,
                                             observed_at=observed_at))
        return {'imported': imported, 'duplicate': duplicated,
                'quarantined': quarantined, 'deferred': deferred,
                'baseline_matched': baseline_matched,
                'orders': orders_recorded,
                'account_key': account_key, 'observed_at': observed_at}

    @staticmethod
    def _occurred(execution, observed_at):
        raw = execution.get('executed_at') or execution.get('timestamp')
        if not isinstance(raw, str) or not raw:
            raise ValueError('RECEIPT_EXECUTION_TIME_REQUIRED')
        return timestamp(raw)

    def _record_order_state(self, order, *, ref, owner, order_id, account_key,
                            observed_at):
        state = str(order.get('state') or '').lower()
        with atomic(self.db):
            existing = self.db.execute(
                'SELECT state,terminal,observed_at FROM receipt_order_progress'
                ' WHERE account_key=? AND order_id=?',
                (account_key, order_id or ref)).fetchone()
            terminal = state in TERMINAL_ORDER_STATES
            if existing is None or self._advances(existing, state, terminal,
                                                  observed_at):
                self.db.execute(
                    'INSERT INTO receipt_order_progress(account_key,order_id,state,'
                    ' terminal,observed_at) VALUES(?,?,?,?,?)'
                    ' ON CONFLICT(account_key,order_id) DO UPDATE SET'
                    ' state=excluded.state,terminal=excluded.terminal,'
                    ' observed_at=excluded.observed_at',
                    (account_key, order_id or ref, state, 1 if terminal else 0,
                     observed_at))
            self._record_order_feedback(owner, ref, order, state, terminal,
                                        account_key=account_key,
                                        observed_at=observed_at)
        return {'status': 'imported', 'ref_id': ref, 'order_id': order_id,
                'instance_id': owner['instance_id'], 'trade_id': owner['trade_id'],
                'state': state, 'executions': []}

    def _retry_deferred(self, *, account_key, observed_at):
        """Book executions whose dependent buy has since arrived.

        A sell that cannot be matched yet is neither imported nor lost: it is
        held with its original identity and time, and retried on every later
        snapshot until its basis exists. Its timestamp is never refreshed.
        """
        recovered = []
        rows = self.db.execute(
            'SELECT order_id,execution_id,ref_id,body,first_seen_at'
            ' FROM receipt_pending_executions WHERE account_key=?'
            ' ORDER BY first_seen_at,execution_id', (account_key,)).fetchall()
        for order_id, execution_id, ref_id, body, first_seen_at in rows:
            owner = self.attribution.owner(ref_id)
            if owner is None:
                continue
            execution = _thaw(body)
            try:
                outcome = self._import_execution(
                    execution, owner=owner, ref=ref_id, account_key=account_key,
                    order_id=order_id, order_state='filled',
                    observed_at=observed_at, order_filled=True,
                    occurred=timestamp(execution['executed_at']))
            except _AwaitingBasis:
                continue
            if outcome['status'] == 'imported':
                recovered.append(outcome)
        return recovered

    def _import_order(self, order, *, account_key, observed_at):
        ref = self.resolve(order, account_key)
        order_id = order.get('id') or order.get('order_id')
        if ref is None:
            return self._quarantine(order, account_key=account_key,
                                    observed_at=observed_at,
                                    reason='RECEIPT_OWNER_UNKNOWN')
        owner = self.attribution.owner(ref)
        if owner is None:
            return self._quarantine(order, account_key=account_key,
                                    observed_at=observed_at,
                                    reason='RECEIPT_REFERENCE_UNBOUND')
        state = str(order.get('state') or '').lower()
        terminal = state in TERMINAL_ORDER_STATES
        executions = [execution for execution in (order.get('executions') or ())]
        with atomic(self.db):
            recorded = [self._import_execution(
                execution, owner=owner, ref=ref, account_key=account_key,
                order_id=order_id, order_state=state, observed_at=observed_at,
                order_filled=state == 'filled')
                for execution in executions]
            existing = self.db.execute(
                'SELECT state,terminal,observed_at FROM receipt_order_progress'
                ' WHERE account_key=? AND order_id=?',
                (account_key, order_id or ref)).fetchone()
            if existing is None or self._advances(existing, state, terminal, observed_at):
                self.db.execute(
                    'INSERT INTO receipt_order_progress(account_key,order_id,state,'
                    ' terminal,observed_at) VALUES(?,?,?,?,?)'
                    ' ON CONFLICT(account_key,order_id) DO UPDATE SET'
                    ' state=excluded.state,terminal=excluded.terminal,'
                    ' observed_at=excluded.observed_at',
                    (account_key, order_id or ref, state, 1 if terminal else 0,
                     observed_at))
            self._record_order_feedback(owner, ref, order, state, terminal,
                                        account_key=account_key, observed_at=observed_at)
        return {'status': 'imported', 'ref_id': ref, 'order_id': order_id,
                'instance_id': owner['instance_id'], 'trade_id': owner['trade_id'],
                'state': state, 'executions': recorded}

    @staticmethod
    def _advances(existing, state, terminal, observed_at):
        """Whether a new observation may replace the recorded order progress.

        A terminal state never goes back to an in-flight one, and an older
        observation never replaces a newer one: a late or out-of-order snapshot
        is evidence about the past, not a correction of the present.
        """
        prior_state, prior_terminal, prior_at = existing
        if prior_terminal and not terminal:
            return False
        if observed_at < prior_at:
            return False
        return True

    def _import_execution(self, execution, *, owner, ref, account_key, order_id,
                          order_state, observed_at, order_filled, occurred=None,
                          pending_key=None):
        if not isinstance(execution, dict):
            raise ValueError('RECEIPT_EXECUTION_INVALID')
        execution_id = execution.get('id')
        if not isinstance(execution_id, str) or not execution_id:
            raise ValueError('RECEIPT_EXECUTION_INVALID')
        executed_at = execution.get('executed_at') or execution.get('timestamp')
        if not isinstance(executed_at, str) or not executed_at:
            raise ValueError('RECEIPT_EXECUTION_TIME_REQUIRED')
        occurred = occurred or timestamp(executed_at)
        if occurred > timestamp(observed_at):
            raise ValueError('RECEIPT_EXECUTION_IN_FUTURE')
        economic = digest(['receipt-execution-1', execution['side'],
                           execution['quantity'], execution['price'],
                           execution.get('fees', '0')])
        key = self.identity_key(account_key, execution_id)
        try:
            booked = self._book_with_identity(
                execution, owner=owner, ref=ref, account_key=account_key,
                order_id=order_id, execution_id=execution_id, key=key,
                economic=economic, occurred=occurred)
        except _IdentityConflict as conflict:
            # The booking transaction has rolled back. Record the audit row in
            # its own transaction; a storage failure here propagates so the
            # caller stays fail-closed rather than reporting a silent conflict.
            self._record_identity_conflict(conflict, account_key=account_key)
            raise ValueError('RECEIPT_EXECUTION_IDENTITY_CONFLICT') from None
        return booked

    def _book_with_identity(self, execution, *, owner, ref, account_key, order_id,
                            execution_id, key, economic, occurred):
        with atomic(self.db):
            try:
                claim = self._claim_identity(
                    account_key=account_key, execution_id=execution_id,
                    order_id=order_id or ref, ref=ref, owner=owner,
                    economic_digest=economic, occurred=occurred)
            except ValueError as exc:
                if str(exc) != 'RECEIPT_EXECUTION_IDENTITY_CONFLICT':
                    raise
                # The booking transaction rolls back; the conflict is then
                # recorded in its own transaction. A failure to record it is
                # not swallowed: the caller must stay fail-closed either way.
                raise _IdentityConflict(
                    account_key=account_key, execution_id=execution_id,
                    order_id=order_id or ref, ref=ref, owner=owner,
                    economic_digest=economic, occurred=occurred) from None
            if claim['duplicate']:
                # The same execution, unchanged: clear any stale pending row and
                # report as a duplicate without touching the ledger again.
                self.db.execute(
                    'DELETE FROM receipt_pending_executions'
                    ' WHERE account_key=? AND execution_id=?',
                    (account_key, execution_id))
                return {'status': 'duplicate', 'execution_id': execution_id,
                        'recorded': False}
            return self._book_execution(
                execution, owner=owner, ref=ref, account_key=account_key,
                order_id=order_id, execution_id=execution_id, key=key,
                occurred=occurred)

    def _record_identity_conflict(self, conflict, *, account_key,
                                  accepted_override=None):
        """Append one durable audit row for a rejected claim, after rollback.

        The accepted binding is read from the identity table; the rejected claim
        is whatever just arrived. The conflict id covers both, so an exact repeat
        is idempotent while a different rejected claim is its own row.
        """
        key = self.identity_key(account_key, conflict.execution_id)
        accepted = accepted_override or self.db.execute(
            'SELECT order_id,ref_id,instance_id,trade_id,economic_digest,occurred_at'
            ' FROM execution_identity WHERE identity_key=?', (key,)).fetchone()
        if accepted is None:
            # The dispute is with a deferred binding that has not been booked
            # yet: the pending order is the accepted side, the rest is unknown.
            pending = self.db.execute(
                'SELECT order_id,ref_id FROM receipt_pending_executions'
                ' WHERE account_key=? AND execution_id=?',
                (account_key, conflict.execution_id)).fetchone()
            accepted = ((pending[0], pending[1]) + (None,) * 4) if pending \
                else (None,) * 6
        rejected = (conflict.order_id, conflict.ref, conflict.owner['instance_id'],
                    conflict.owner['trade_id'], conflict.economic_digest,
                    conflict.occurred.isoformat())
        conflict_id = digest(['execution-identity-conflict-1', key,
                              list(accepted), list(rejected)])
        with atomic(self.db):
            self.db.execute(
                'INSERT OR IGNORE INTO execution_identity_conflict(conflict_id,'
                ' identity_key,provider,account_key,execution_id,accepted_order_id,'
                ' accepted_ref_id,accepted_instance_id,accepted_trade_id,'
                ' accepted_economic_digest,accepted_occurred_at,rejected_order_id,'
                ' rejected_ref_id,rejected_instance_id,rejected_trade_id,'
                ' rejected_economic_digest,rejected_occurred_at,first_seen_at,'
                ' evidence_source)'
                ' VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                (conflict_id, key, self.provider, account_key,
                 conflict.execution_id, *accepted, *rejected,
                 self.clock().astimezone(timezone.utc).isoformat(),
                 getattr(conflict, 'evidence_source', None)))
        return conflict_id

    def canonical_claim(self, *, execution_id, order_id, ref, owner,
                        economic_digest, occurred):
        """One execution's claim, in the single shape every path compares.

        The order, the reference and the owner may be unknown at the moment a
        claim is formed (an unbound or deferred receipt), so they are allowed to
        be ``None`` here rather than being filled with a guess. Identity is
        provider + account + execution id; the rest are bound attributes.
        """
        # Normalised here so a claim built from a snapshot and one read back
        # from storage compare with the same digest.
        when = occurred.isoformat() if hasattr(occurred, 'isoformat') else occurred
        return {'provider': self.provider, 'execution_id': execution_id,
                'order_id': order_id, 'ref_id': ref,
                'instance_id': (owner or {}).get('instance_id'),
                'trade_id': (owner or {}).get('trade_id'),
                'economic_digest': economic_digest,
                'occurred_at': when}

    @staticmethod
    def claim_digest(claim):
        return digest(['execution-claim-1', claim.get('order_id'),
                       claim.get('ref_id'), claim.get('instance_id'),
                       claim.get('trade_id'), claim.get('economic_digest'),
                       claim.get('occurred_at')])

    def _economic_digest(self, execution):
        return digest(['receipt-execution-1', execution['side'],
                       execution['quantity'], execution['price'],
                       execution.get('fees', '0')])

    def _stored_claim(self, key):
        row = self.db.execute(
            'SELECT order_id,ref_id,instance_id,trade_id,economic_digest,occurred_at'
            ' FROM execution_identity WHERE identity_key=?', (key,)).fetchone()
        if row is None:
            return None
        return {'provider': self.provider, 'execution_id': key.rsplit('|', 1)[-1],
                'order_id': row[0], 'ref_id': row[1], 'instance_id': row[2],
                'trade_id': row[3], 'economic_digest': row[4], 'occurred_at': row[5]}

    def _deferred_claim(self, account_key, execution_id):
        row = self.db.execute(
            'SELECT order_id,ref_id,body FROM receipt_pending_executions'
            ' WHERE account_key=? AND execution_id=?',
            (account_key, execution_id)).fetchone()
        if row is None:
            return None
        body = _thaw(row[2])
        owner = self.attribution.owner(row[1])
        return self.canonical_claim(
            execution_id=execution_id, order_id=row[0], ref=row[1], owner=owner,
            economic_digest=self._economic_digest(body),
            occurred=body.get('executed_at'))

    def record_risk_conflict(self, snapshot, *, prior_execution_ids=()):
        """Persist a conflict the risk guard saw before the importer could.

        The accepted side comes from a real binding when one exists — the
        receipt identity or a deferred pending row. When the execution is known
        only to the risk ledger, its hash cannot be turned back into fields, so
        the accepted side is recorded as unknown and the dispute is marked with
        that provenance instead of inventing an order or an owner.

        Every variant in the snapshot that differs from the accepted claim is
        written as its own row. A single new variant is therefore still a
        recorded dispute, not a silent ``conflicts=0``.
        """
        account_key = snapshot.get('account_key')
        if not isinstance(account_key, str) or not account_key:
            raise ValueError('RECEIPT_ACCOUNT_REQUIRED')
        observed_at = snapshot.get('observed_at')
        variants = {}
        for order in (snapshot.get('orders') or {}).values():
            ref = self.resolve(order, account_key)
            owner = self.attribution.owner(ref) if ref else None
            for fill in order.get('executions') or ():
                claim = self.canonical_claim(
                    execution_id=fill['id'], order_id=order.get('id'), ref=ref,
                    owner=owner, economic_digest=self._economic_digest(fill),
                    occurred=timestamp(fill.get('executed_at') or observed_at))
                variants.setdefault(fill['id'], []).append(claim)
        written = []
        for execution_id, claims in variants.items():
            key = self.identity_key(account_key, execution_id)
            accepted = self._stored_claim(key) or \
                self._deferred_claim(account_key, execution_id)
            source = 'receipt_identity' if accepted is not None else None
            distinct = {self.claim_digest(claim) for claim in claims}
            if accepted is None and len(distinct) >= 2:
                # Two variants arrive together with no prior binding at all.
                # Nothing is claimed as accepted; every variant is recorded so
                # the dispute is visible and stays blocked.
                source = 'snapshot_only_no_binding'
            elif accepted is None and execution_id in set(prior_execution_ids or ()):
                # The risk ledger knows this execution only as a hash.
                source = 'risk_ledger_hash_only'
            elif accepted is None:
                continue
            rejected = ([claim for claim in claims] if accepted is None else
                        [claim for claim in claims
                         if self.claim_digest(claim) != self.claim_digest(accepted)])
            for claim in rejected:
                written.append(self._write_conflict_row(
                    execution_id=execution_id, key=key, account_key=account_key,
                    accepted=accepted, rejected=claim, source=source))
        return written

    def _write_conflict_row(self, *, execution_id, key, account_key, accepted,
                            rejected, source):
        accepted_fields = () if accepted is None else (
            accepted.get('order_id'), accepted.get('ref_id'),
            accepted.get('instance_id'), accepted.get('trade_id'),
            accepted.get('economic_digest'), accepted.get('occurred_at'))
        accepted_fields = tuple('' if value is None else value
                                for value in accepted_fields) or (None,) * 6
        if accepted is None:
            accepted_fields = (None,) * 6
        if len(accepted_fields) != 6:
            accepted_fields = (None,) * 6
        rejected_fields = (rejected.get('order_id'), rejected.get('ref_id'),
                           rejected.get('instance_id'), rejected.get('trade_id'),
                           rejected.get('economic_digest'), rejected.get('occurred_at'))
        conflict_id = digest(['execution-identity-conflict-2', key,
                              [str(v) for v in accepted_fields],
                              [str(v) for v in rejected_fields]])
        with atomic(self.db):
            self.db.execute(
                'INSERT OR IGNORE INTO execution_identity_conflict(conflict_id,'
                ' identity_key,evidence_source,provider,account_key,execution_id,'
                ' accepted_order_id,accepted_ref_id,accepted_instance_id,'
                ' accepted_trade_id,accepted_economic_digest,accepted_occurred_at,'
                ' rejected_order_id,rejected_ref_id,rejected_instance_id,'
                ' rejected_trade_id,rejected_economic_digest,rejected_occurred_at,'
                ' first_seen_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                (conflict_id, key, source, self.provider, account_key, execution_id,
                 *accepted_fields, *rejected_fields,
                 self.clock().astimezone(timezone.utc).isoformat()))
        return conflict_id

    def identity_key(self, account_key, execution_id):
        """The stable identity of one broker execution.

        Provider and formal account scope the id; the order, reference and
        owner are *attributes* of that execution, never part of its identity.
        Composing the order into the key is what let the same execution be booked
        twice simply by submitting it under a different order.
        """
        if not isinstance(account_key, str) or not account_key:
            raise ValueError('RECEIPT_ACCOUNT_REQUIRED')
        if not isinstance(execution_id, str) or not execution_id:
            raise ValueError('RECEIPT_EXECUTION_INVALID')
        return '%s|%s|%s' % (self.provider, account_key, execution_id)

    def _claim_identity(self, *, account_key, execution_id, order_id, ref, owner,
                        economic_digest, occurred):
        """Assert that this execution's bound attributes never change."""
        key = self.identity_key(account_key, execution_id)
        # A deferred execution has already bound its order. A later arrival
        # claiming another order for the same execution is in dispute, and must
        # not be booked as if it were a fresh fact.
        deferred = self.db.execute(
            'SELECT order_id FROM receipt_pending_executions'
            ' WHERE account_key=? AND execution_id=?',
            (account_key, execution_id)).fetchone()
        if deferred is not None and deferred[0] != (order_id or ref):
            raise ValueError('RECEIPT_EXECUTION_IDENTITY_CONFLICT')
        row = self.db.execute(
            'SELECT order_id,ref_id,instance_id,trade_id,economic_digest,occurred_at'
            ' FROM execution_identity WHERE identity_key=?', (key,)).fetchone()
        if row is None:
            self.db.execute(
                'INSERT INTO execution_identity(identity_key,provider,account_key,'
                ' execution_id,order_id,ref_id,instance_id,trade_id,economic_digest,'
                ' occurred_at,first_seen_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                (key, self.provider, account_key, execution_id, order_id, ref,
                 owner['instance_id'], owner['trade_id'], economic_digest,
                 occurred.isoformat(), self.clock().astimezone(timezone.utc).isoformat()))
            return {'identity_key': key, 'duplicate': False}
        expected = (order_id, ref, owner['instance_id'], owner['trade_id'],
                    economic_digest, occurred.isoformat())
        if tuple(row) != expected:
            # A changed order, reference, owner or economics under one execution
            # is a different claim about a fact that is already settled.
            raise ValueError('RECEIPT_EXECUTION_IDENTITY_CONFLICT')
        return {'identity_key': key, 'duplicate': True}

    def _book_execution(self, execution, *, owner, ref, account_key, order_id,
                        execution_id, key, occurred):
        """Book one execution and its feedback as a single transaction.

        Splitting them would allow a fill to exist with no attributable event,
        or an event with no fill, whenever the second write fails.
        """
        try:
            booked = self.accounting.record_fill(
                owner['instance_id'], owner['trade_id'], fill_id=execution_id,
                side=execution['side'], quantity=execution['quantity'],
                price=execution['price'], fees=execution.get('fees', '0'),
                occurred_at=occurred.isoformat(), execution_key=key)
        except ValueError as exc:
            if str(exc) == 'ACCOUNTING_POSITION_UNDERFLOW':
                raise _AwaitingBasis() from None
            raise
        # A fill is an immutable fact: the same execution seen again must
        # produce the same event. Order progression (partial -> filled) and the
        # observation time are separate, mutable facts and are recorded as
        # their own events, so a later snapshot cannot conflict with this one.
        self.attribution.record({
            'event_id': 'fill:' + digest(['receipt-fill-2', key]),
            'ref_id': ref,
            'kind': 'filled',
            'occurred_at': occurred.isoformat(),
            'order_id': order_id, 'execution_id': execution_id,
            'quantity': execution['quantity'], 'price': execution['price'],
            'fees': execution.get('fees', '0'),
            'account_key': account_key})
        # Whatever deferred this execution before is now resolved.
        self.db.execute(
            'DELETE FROM receipt_pending_executions'
            ' WHERE account_key=? AND order_id=? AND execution_id=?',
            (account_key, order_id or ref, execution_id))
        return {'status': 'imported' if booked['recorded'] else 'duplicate',
                'execution_id': execution_id, 'recorded': booked['recorded']}

    def _defer_execution(self, execution, *, account_key, order_id, ref, observed_at):
        body = canonical({'id': execution['id'], 'side': execution['side'],
                          'quantity': execution['quantity'],
                          'price': execution['price'],
                          'fees': execution.get('fees', '0'),
                          'executed_at': execution.get('executed_at')
                          or execution.get('timestamp')})
        with atomic(self.db):
            prior = self.db.execute(
                'SELECT ref_id,body FROM receipt_pending_executions'
                ' WHERE account_key=? AND order_id=? AND execution_id=?',
                (account_key, order_id, execution['id'])).fetchone()
            if prior is not None:
                if (prior[0], prior[1]) != (ref, body):
                    # Same identity, different economics or owner. Keeping the
                    # old value silently would hide a corrected receipt.
                    raise ValueError('RECEIPT_PENDING_CONFLICT')
                return
            self.db.execute(
                'INSERT INTO receipt_pending_executions(account_key,'
                ' order_id,execution_id,ref_id,body,first_seen_at)'
                ' VALUES(?,?,?,?,?,?)',
                (account_key, order_id, execution['id'], ref, body, observed_at))

    def _record_order_feedback(self, owner, ref, order, state, terminal, *,
                               account_key, observed_at):
        order_id = order.get('id') or ref
        """Record an order-state transition as its own immutable fact.

        One event per (order, state): replaying the same snapshot produces the
        same event id and payload, and a later state gets a different id rather
        than mutating the earlier one. Observation time is deliberately absent
        — it changes every poll and belongs to progress, not to the fact.
        """
        # Acceptance of a cancel *request* is not a terminal state, and a
        # pending state maps to nothing at all; only the broker's own reported
        # state produces a fact.
        kind = STATE_KINDS.get(state)
        if kind is None:
            return
        # The broker does not timestamp a state change, and the local poll time
        # changes every snapshot. The first time this state was observed is the
        # stable occurrence time, so a replay produces an identical fact.
        row = self.db.execute(
            'SELECT first_observed_at FROM receipt_state_first_seen'
            ' WHERE account_key=? AND order_id=? AND state=?',
            (account_key, order_id, state)).fetchone()
        if row is None:
            self.db.execute(
                'INSERT INTO receipt_state_first_seen(account_key,order_id,state,'
                ' first_observed_at) VALUES(?,?,?,?)',
                (account_key, order_id, state, observed_at))
            seen_at = observed_at
        else:
            seen_at = row[0]
        self.attribution.record({
            # Keyed by the broker state, not by the mapped kind: 'cancelled'
            # and 'canceled' share a kind but are different reported facts.
            'event_id': 'order:' + digest(['receipt-order-3', account_key,
                                           order_id, state]),
            'ref_id': ref, 'kind': kind, 'occurred_at': seen_at,
            'order_state': state, 'account_key': account_key})

    def _quarantine(self, order, *, account_key, observed_at, reason):
        from trading_contracts.values import canonical
        key = digest(['receipt-quarantine-1', account_key, observed_at, reason, order])
        with atomic(self.db):
            self.db.execute(
                'INSERT OR IGNORE INTO receipt_quarantine(receipt_key,account_key,'
                ' observed_at,reason,payload,recorded_at) VALUES(?,?,?,?,?,?)',
                (key, account_key, observed_at, reason, canonical(order),
                 _now(self.clock)))
        return {'status': 'quarantined', 'reason': reason, 'receipt_key': key}

    # -- reads -----------------------------------------------------------
    def pending_execution_count(self):
        """How many executions are still waiting for their basis to arrive."""
        return self.db.execute(
            'SELECT count(*) FROM receipt_pending_executions').fetchone()[0]

    def quarantined(self):
        return [{'receipt_key': row[0], 'account_key': row[1], 'observed_at': row[2],
                 'reason': row[3]}
                for row in self.db.execute(
                    'SELECT receipt_key,account_key,observed_at,reason'
                    ' FROM receipt_quarantine ORDER BY recorded_at')]
