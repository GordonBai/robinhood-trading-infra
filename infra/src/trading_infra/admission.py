"""The single owner of the path from an advisory proposal to an execution intent.

An instance proposes; the account decides. Everything that costs money or
reserves a symbol happens here, in one durable transaction, so two instances
cannot each believe they were admitted for the same cash or the same ticker, and
a storage failure cannot leave a lease held with no reservation behind it.

What this module guarantees, and the tests pin:

* **One transaction.** Lease, reservation, attribution and the pending execution
  intent commit together or not at all. The components expose
  ``*_in_transaction`` forms for exactly this; a component that commits on its
  own cannot participate.
* **One cost.** Sizing, the reservation and the execution plan all use the
  instance's declared cost buffer, taken from its registered manifest. Planned
  risk includes that buffer, so a strategy cannot quietly exceed the account's
  per-trade loss limit.
* **One clock.** The response window is re-checked immediately before the write,
  not only on entry, because a lease or budget check can take time.
* **Evidence, not a string.** Releasing a trade requires authoritative facts
  (no remaining quantity, no open orders, no pending approvals, no unknown
  intent). A caller-supplied reason is not proof.
"""
import sqlite3
from datetime import datetime, timezone

from trading_contracts.strategy import ProposalAction
from trading_contracts.values import digest, number, timestamp
from trading_infra.sizing import size_entry
from trading_infra.transactions import atomic

_SLACK = 1e-9

_SCHEMA = '''
CREATE TABLE IF NOT EXISTS execution_intents(
    intent_id TEXT PRIMARY KEY, instance_id TEXT NOT NULL, trade_id TEXT NOT NULL,
    proposal_id TEXT NOT NULL, ref_id TEXT NOT NULL UNIQUE, symbol TEXT NOT NULL,
    quantity INTEGER NOT NULL, entry_ceiling TEXT NOT NULL, stop_price TEXT NOT NULL,
    planned_loss TEXT NOT NULL, notional TEXT NOT NULL, plan TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN
        ('PENDING','DISPATCHING','DISPATCHED','SETTLED','EXPIRED','QUARANTINED')),
    created_at TEXT NOT NULL, generated_at TEXT, valid_until TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS execution_intents_pending
    ON execution_intents(instance_id,state,valid_until);
CREATE TABLE IF NOT EXISTS admission_schema(version INTEGER NOT NULL);
'''

#: Bumped whenever the intent table's shape or allowed states change. The
#: version is recorded in the database, because "the column exists" is not the
#: same fact as "the migration finished".
SCHEMA_VERSION = 2

_ADMISSION_COLUMNS = ('intent_id', 'instance_id', 'trade_id', 'proposal_id', 'ref_id',
                      'symbol', 'quantity', 'entry_ceiling', 'stop_price',
                      'planned_loss', 'notional', 'plan', 'state', 'created_at',
                      'generated_at', 'valid_until')


class AdmissionService:
    """Admit or refuse one instance proposal against account-wide limits."""

    def __init__(self, *, budget, leases, attribution, policy, registry=None,
                 clock=None, db_path=None):
        for name, value in (('budget', budget), ('leases', leases),
                            ('attribution', attribution)):
            if value is None:
                raise ValueError('ADMISSION_COMPONENT_REQUIRED')
        for key in ('max_position_notional_usd', 'max_planned_loss_per_trade_usd'):
            if key not in policy:
                raise ValueError('ADMISSION_POLICY_INCOMPLETE')
        # One owner thread, one connection: components that cannot share a
        # transaction with each other cannot be made atomic by this service.
        connections = {id(getattr(item, 'db', None))
                       for item in (budget, leases, attribution)}
        if len(connections) != 1 or None in connections:
            raise ValueError('ADMISSION_SINGLE_CONNECTION_REQUIRED')
        self.budget = budget
        self.leases = leases
        self.attribution = attribution
        self.policy = policy
        self.registry = registry
        self.db = budget.db
        # Known path keeps the side-car witnesses reachable even when the
        # connection itself is the thing that is broken.
        self.db_path = db_path
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        if self.db.in_transaction:
            # executescript implicitly commits an open caller transaction;
            # refuse before any DDL so the caller's rollback boundary survives.
            raise ValueError('ADMISSION_SCHEMA_REQUIRES_IDLE_CONNECTION')
        self._check_schema_version()
        self.db.executescript(_SCHEMA)
        self._migrate()

    def _schema_version(self):
        present = self.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table'"
            " AND name='admission_schema'").fetchone()
        if not present:
            return None
        row = self.db.execute('SELECT max(version) FROM admission_schema').fetchone()
        if row is None or row[0] is None:
            return None
        if type(row[0]) is not int or row[0] < 1:
            raise ValueError('ADMISSION_SCHEMA_VERSION_INVALID')
        return row[0]

    def _check_schema_version(self):
        recorded = self._schema_version()
        if recorded is not None and recorded > SCHEMA_VERSION:
            raise ValueError('ADMISSION_SCHEMA_VERSION_UNSUPPORTED')

    def _migrate(self):
        """Bring an older ``execution_intents`` table to the current shape.

        Two rules make this safe to interrupt:

        * every statement runs through ``execute`` inside one ``BEGIN
          IMMEDIATE``. ``executescript`` implicitly commits the open
          transaction, so a failure after it could not roll the rename back and
          the next start would call a half-built table "migrated".
        * completion is decided by the recorded version *and* the absence of a
          leftover ``execution_intents_old``. A leftover means a previous run
          stopped mid-way; the rows it holds are still the source of truth, so
          the migration resumes instead of declaring victory over an empty new
          table.
        """
        self.db.execute(
            'CREATE TABLE IF NOT EXISTS admission_schema(version INTEGER NOT NULL)')
        leftover = self.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table'"
            " AND name='execution_intents_old'").fetchone()
        recorded = self._schema_version()
        if recorded is not None and recorded > SCHEMA_VERSION:
            raise ValueError('ADMISSION_SCHEMA_VERSION_UNSUPPORTED')
        columns = {row[1] for row in self.db.execute('PRAGMA table_info(execution_intents)')}
        if recorded == SCHEMA_VERSION and not leftover:
            # A version marker cannot authorize a damaged or older shape.
            if not self._structure_valid(columns):
                raise ValueError('ADMISSION_SCHEMA_STRUCTURE_MISMATCH')
            return
        self._rebuild_intent_table()
        with atomic(self.db):
            self.db.execute('DELETE FROM admission_schema')
            self.db.execute('INSERT INTO admission_schema(version) VALUES(?)',
                            (SCHEMA_VERSION,))

    _TARGET_DDL = (
        'CREATE TABLE execution_intents_rebuild(intent_id TEXT PRIMARY KEY,'
        ' instance_id TEXT NOT NULL, trade_id TEXT NOT NULL,'
        ' proposal_id TEXT NOT NULL, ref_id TEXT NOT NULL UNIQUE,'
        ' symbol TEXT NOT NULL, quantity INTEGER NOT NULL,'
        ' entry_ceiling TEXT NOT NULL, stop_price TEXT NOT NULL,'
        ' planned_loss TEXT NOT NULL, notional TEXT NOT NULL,'
        ' plan TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN'
        " ('PENDING','DISPATCHING','DISPATCHED','SETTLED','EXPIRED',"
        "  'QUARANTINED')),"
        ' created_at TEXT NOT NULL, generated_at TEXT,'
        ' valid_until TEXT NOT NULL)')

    def _existing_tables(self):
        rows = self.db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name IN"
            " ('execution_intents','execution_intents_old',"
            "  'execution_intents_rebuild')").fetchall()
        return {row[0] for row in rows}

    def _rows_of(self, table):
        columns = {row[1] for row in self.db.execute('PRAGMA table_info(' + table + ')')}
        if not columns:
            return []
        select = ','.join(
            name if name in columns else 'NULL'
            for name in _ADMISSION_COLUMNS)
        return [dict(zip(_ADMISSION_COLUMNS, row)) for row in
                self.db.execute('SELECT ' + select + ' FROM ' + table).fetchall()]

    def _rebuild_intent_table(self):
        """Merge every source into a controlled target, then swap atomically.

        The previous version reused whatever ``execution_intents`` happened to
        exist. A leftover table from an interrupted run is often the broken
        shape itself (``generated_at NOT NULL``), so ``INSERT OR IGNORE``
        silently dropped the NULL rows and then deleted the only remaining copy.
        Merging into a table this method creates means the target shape is never
        in question, and a conflict fails closed with both sources intact.
        """
        tables = self._existing_tables()
        current = self._rows_of('execution_intents') if 'execution_intents' in tables else []
        leftover = self._rows_of('execution_intents_old') if 'execution_intents_old' in tables else []
        pending = [row for row in current
                   if row['state'] not in ('PENDING', 'DISPATCHING')]
        if not leftover and not pending and not self._needs_rebuild(current):
            return
        merged = {}
        for row in current + leftover:
            key = row['intent_id']
            prior = merged.get(key)
            if prior is None:
                merged[key] = row
                continue
            if self._same_row(prior, row):
                continue
            # Two sources disagree about one identity. Guessing which is right
            # would silently lose the other, so the migration stops here and
            # leaves every original table in place.
            raise ValueError('ADMISSION_MIGRATION_IDENTITY_CONFLICT:' + str(key))
        self.db.execute('BEGIN IMMEDIATE')
        try:
            self.db.execute('DROP TABLE IF EXISTS execution_intents_rebuild')
            self.db.execute(self._TARGET_DDL)
            for row in merged.values():
                values = []
                for name in _ADMISSION_COLUMNS:
                    values.append(row[name])
                self.db.execute(
                    'INSERT INTO execution_intents_rebuild(' +
                    ','.join(_ADMISSION_COLUMNS) + ') VALUES(' +
                    ','.join('?' * len(_ADMISSION_COLUMNS)) + ')', values)
            self.db.execute(
                "UPDATE execution_intents_rebuild SET state='QUARANTINED'"
                " WHERE state='PENDING' AND (generated_at IS NULL OR generated_at='')")
            self.db.execute(
                "UPDATE execution_intents_rebuild SET generated_at=''"
                " WHERE generated_at IS NULL")
            # Conservation check before anything is destroyed.
            copied = self.db.execute(
                'SELECT count(*) FROM execution_intents_rebuild').fetchone()[0]
            if copied != len(merged):
                raise ValueError('ADMISSION_MIGRATION_ROW_LOSS')
            self.db.execute('DROP TABLE IF EXISTS execution_intents')
            self.db.execute('DROP TABLE IF EXISTS execution_intents_old')
            self.db.execute(
                'ALTER TABLE execution_intents_rebuild RENAME TO execution_intents')
            self.db.execute(
                'CREATE INDEX IF NOT EXISTS execution_intents_pending'
                ' ON execution_intents(instance_id,state,valid_until)')
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def _structure_valid(self, columns=None):
        present = columns or {row[1] for row in
                              self.db.execute('PRAGMA table_info(execution_intents)')}
        sql = self.db.execute(
            "SELECT sql FROM sqlite_master WHERE type='table'"
            " AND name='execution_intents'").fetchone()
        return (present == set(_ADMISSION_COLUMNS)
                and sql is not None
                and 'DISPATCHING' in (sql[0] or '')
                and 'QUARANTINED' in (sql[0] or '')
                and 'generated_at TEXT' in (sql[0] or ''))

    def _needs_rebuild(self, rows):
        sql = self.db.execute(
            "SELECT sql FROM sqlite_master WHERE type='table'"
            " AND name='execution_intents'").fetchone()
        present = {row[1] for row in self.db.execute('PRAGMA table_info(execution_intents)')}
        return bool(present) and (set(_ADMISSION_COLUMNS) - present
                                  or 'DISPATCHING' not in (sql[0] or ''))

    @staticmethod
    def _same_row(left, right):
        return all(left.get(name) == right.get(name) for name in _ADMISSION_COLUMNS)

    # -- helpers ---------------------------------------------------------
    def _refuse(self, code, proposal, **extra):
        return {'admitted': False, 'reason': code,
                'instance_id': proposal.identity.instance_id,
                'proposal_id': proposal.proposal_id, **extra}

    @staticmethod
    def _cost(instance):
        """The instance's declared cost buffer, never a caller default."""
        raw = instance.manifest.config.get('cost_buffer_per_share')
        if raw is None:
            raise ValueError('ADMISSION_INSTANCE_COST_REQUIRED')
        return number(raw)

    def _live(self, proposal, now, *, event_at=None):
        if now >= proposal.valid_until:
            return 'ADMISSION_PROPOSAL_EXPIRED'
        if now < proposal.generated_at:
            return 'ADMISSION_PROPOSAL_FUTURE'
        if event_at is not None and not 0 <= (now - event_at).total_seconds() <= 30:
            return 'ADMISSION_EVENT_STALE'
        return None

    # -- admission -------------------------------------------------------
    def execution_integrity_block(self):
        """Why the durable account gate forbids new risk, or ``None``.

        The fault ledger lives in the base infrastructure, so admission reads it
        directly instead of trusting a caller's flag. A ledger that cannot be
        read is a closed gate: unknown is not clear.
        """
        try:
            from trading_infra.integrity_gate import read_integrity
            state = read_integrity(self.db, db_path=getattr(self, 'db_path', None),
                                   clock=self.clock)
        except (ValueError, KeyError, TypeError, OSError, sqlite3.Error):
            return 'EXECUTION_INTEGRITY_GATE_UNAVAILABLE'
        if not state['known']:
            return 'EXECUTION_INTEGRITY_GATE_UNAVAILABLE'
        if not state['blocked']:
            return None
        # The side-car means "detected, not recorded"; the database row means
        # "recorded". Both refuse; neither is reported as an ordinary hold.
        return ('EXECUTION_INTEGRITY_SIDECAR_UNRECORDED_FAULT'
                if 'EXECUTION_INTEGRITY_SIDECAR_UNRECORDED_FAULT' in state['reasons']
                else 'EXECUTION_INTEGRITY_FAULT_UNRESOLVED')

    def admit(self, instance, proposal, *, allow_entries, trade_id=None,
              event_at=None, capture_only=False, qualified_strategy_entry=False):
        """Return a pending execution intent, or a refusal that wrote nothing.

        ``allow_entries`` is not decoration: when the session is held, capturing
        or blocked by account risk, nothing here may consume a shared-account
        reservation or claim the proposal.
        """
        now = self.clock()
        if allow_entries is not True or capture_only:
            return self._refuse('ADMISSION_ENTRIES_NOT_PERMITTED', proposal)
        integrity = self.execution_integrity_block()
        if integrity:
            return self._refuse(integrity, proposal)
        # Re-read the registry: a caller may hold a snapshot from start-up, and
        # an instance disabled since then must not be able to admit.
        registry = getattr(self, 'registry', None)
        if registry is not None:
            current = registry.find(instance.instance_id)
            if current is None:
                return self._refuse('ADMISSION_INSTANCE_NOT_REGISTERED', proposal)
            if (current.mode, current.enabled) != (instance.mode, instance.enabled):
                instance = current
        if not instance.enabled or instance.mode == 'disabled':
            return self._refuse('ADMISSION_INSTANCE_DISABLED', proposal)
        if instance.mode == 'read_only_shadow':
            return self._refuse('ADMISSION_READ_ONLY_EXECUTION_CAPABILITY_NONE',
                                proposal)
        if instance.instance_id != proposal.identity.instance_id:
            return self._refuse('ADMISSION_INSTANCE_MISMATCH', proposal)
        if 'advisory-entry' not in instance.manifest.capabilities:
            return self._refuse('ADMISSION_CAPABILITY_ABSENT', proposal)
        if proposal.action is not ProposalAction.ENTER:
            return self._refuse('ADMISSION_ACTION_UNSUPPORTED', proposal)
        reason = self._live(proposal, now, event_at=event_at)
        if reason:
            return self._refuse(reason, proposal)
        if proposal.entry_ceiling is None or proposal.stop_price is None:
            return self._refuse('ADMISSION_ENTRY_TERMS_REQUIRED', proposal)
        cost = self._cost(instance)
        quantity = size_entry(
            entry_ceiling=proposal.entry_ceiling, stop=proposal.stop_price,
            cost_buffer_per_share=cost,
            max_position_notional=self.policy['max_position_notional_usd'],
            max_planned_loss=self.policy['max_planned_loss_per_trade_usd'],
            quantity_limit=proposal.quantity_limit)
        if quantity <= 0:
            return self._refuse('ADMISSION_ZERO_AFFORDABLE_QUANTITY', proposal)
        key = trade_id or proposal.proposal_id
        # Planned risk includes the cost buffer, the same way sizing does.
        planned_loss = (proposal.entry_ceiling - proposal.stop_price + cost) * quantity
        cash = proposal.entry_ceiling * quantity
        ref_id = 'ref-' + digest(['admission-2', instance.instance_id, key,
                                  proposal.proposal_id])
        existing = self.db.execute(
            "SELECT intent_id FROM execution_intents WHERE ref_id=?", (ref_id,)).fetchone()
        if existing is not None:
            return self._refuse('ADMISSION_ALREADY_ADMITTED', proposal,
                                intent_id=existing[0])
        self.db.execute('BEGIN IMMEDIATE')
        try:
            lease = self.leases.acquire_in_transaction(
                instance.instance_id, [proposal.symbol], trade_id=key)
            if not lease['acquired']:
                self.db.rollback()
                return self._refuse('ADMISSION_SYMBOL_HELD', proposal,
                                    symbol=proposal.symbol, lease=lease['reason'])
            reservation = self.budget.reserve_in_transaction(
                instance, trade_id=key, cash=cash, planned_loss=planned_loss,
                notional=cash)
            if not reservation['reserved']:
                self.db.rollback()
                return self._refuse('ADMISSION_BUDGET_REFUSED', proposal,
                                    budget=reservation['reason'])
            self.attribution.bind_in_transaction(ref_id, instance.instance_id, key)
            # Re-check the window at the point of write: the lease and budget
            # work above can consume the whole remaining validity.
            reason = self._live(proposal, self.clock(), event_at=event_at)
            if reason:
                self.db.rollback()
                return self._refuse(reason, proposal)
            intent_id = 'intent-' + digest(['execution-intent-1', ref_id])
            plan = {'symbol': proposal.symbol, 'quantity': quantity,
                    'entry_ceiling': str(proposal.entry_ceiling),
                    'stop': str(proposal.stop_price),
                    'cost_buffer_per_share': str(cost),
                    # Only an entry the session actually qualified may later be
                    # handed to a native channel as a strategy entry. The flag
                    # travels with the durable plan, not a caller's word.
                    'qualified_strategy_entry': bool(qualified_strategy_entry)}
            self.db.execute(
                'INSERT INTO execution_intents(intent_id,instance_id,trade_id,'
                ' proposal_id,ref_id,symbol,quantity,entry_ceiling,stop_price,'
                ' planned_loss,notional,plan,state,created_at,generated_at,'
                ' valid_until) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                (intent_id, instance.instance_id, key, proposal.proposal_id, ref_id,
                 proposal.symbol, quantity, str(proposal.entry_ceiling),
                 str(proposal.stop_price), str(planned_loss), str(cash),
                 _canonical(plan), 'PENDING', self.clock().isoformat(),
                 proposal.generated_at.isoformat(), proposal.valid_until.isoformat()))
            # The insert itself runs after the check above. Verify once more at
            # the real commit boundary so a slow write cannot publish an
            # already-expired intent.
            reason = self._live(proposal, self.clock(), event_at=event_at)
            if reason:
                self.db.rollback()
                return self._refuse(reason, proposal)
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return {'admitted': True, 'instance_id': instance.instance_id,
                'trade_id': key, 'proposal_id': proposal.proposal_id,
                'intent_id': intent_id, 'ref_id': ref_id, 'symbol': proposal.symbol,
                'side': 'buy', 'quantity': quantity,
                'entry_ceiling': str(proposal.entry_ceiling),
                'stop_price': str(proposal.stop_price),
                'notional': str(cash), 'planned_loss': str(planned_loss),
                'cost_buffer_per_share': str(cost),
                'generated_at': proposal.generated_at.isoformat(),
                'valid_until': proposal.valid_until.isoformat(),
                'confirmation_required': True,
                'qualified_strategy_entry': bool(qualified_strategy_entry)}

    def pending_intents(self, instance_id=None, *, now=None):
        """Pending execution intents, expired ones already marked."""
        moment = (now or self.clock()).astimezone(timezone.utc).isoformat()
        query = ("SELECT intent_id,instance_id,trade_id,proposal_id,ref_id,symbol,"
                 " quantity,entry_ceiling,stop_price,planned_loss,notional,plan,"
                 " valid_until,generated_at FROM execution_intents"
                 " WHERE state='PENDING' AND generated_at IS NOT NULL"
                " AND generated_at<>'' AND valid_until>?")
        arguments = [moment]
        if instance_id is not None:
            query += ' AND instance_id=?'
            arguments.append(instance_id)
        query += ' ORDER BY created_at,intent_id'
        return [{'intent_id': row[0], 'instance_id': row[1], 'trade_id': row[2],
                 'state': 'PENDING',
                 'proposal_id': row[3], 'ref_id': row[4], 'symbol': row[5],
                 'quantity': row[6], 'entry_ceiling': row[7], 'stop_price': row[8],
                 'planned_loss': row[9], 'notional': row[10],
                 'plan': _thaw_plan(row[11]), 'valid_until': row[12],
                 'generated_at': row[13]}
                for row in self.db.execute(query, arguments)]

    def expire_intents(self, *, now=None):
        """Terminate intents that were never dispatched, and release them.

        Only ``PENDING`` intents qualify: a dispatched intent may correspond to
        a real order, so it is never treated as "expired and unsent". Releasing
        here is what stops an undispatched reservation from holding cash and a
        symbol forever once its validity passed.
        """
        moment = (now or self.clock()).astimezone(timezone.utc).isoformat()
        expired = []
        rows = self.db.execute(
            "SELECT intent_id,instance_id,trade_id FROM execution_intents"
            " WHERE state='PENDING' AND valid_until<=?", (moment,)).fetchall()
        for intent_id, instance_id, trade_id in rows:
            self.db.execute('BEGIN IMMEDIATE')
            try:
                cursor = self.db.execute(
                    "UPDATE execution_intents SET state='EXPIRED'"
                    " WHERE intent_id=? AND state='PENDING'", (intent_id,))
                if cursor.rowcount != 1:
                    self.db.rollback()
                    continue
                self.leases.release_in_transaction(instance_id, trade_id)
                self.budget.release_in_transaction(instance_id, trade_id)
                self.db.commit()
            except BaseException:
                self.db.rollback()
                raise
            expired.append({'intent_id': intent_id, 'instance_id': instance_id,
                            'trade_id': trade_id, 'released': True})
        return expired

    def dispatching_intents(self):
        """Intents whose dispatch started but was never confirmed.

        They are neither pending (a propose may have run) nor dispatched (no
        confirmation was recorded), so only the owner's recovery pass can
        resolve them against the execution book.
        """
        return [{'intent_id': row[0], 'instance_id': row[1], 'trade_id': row[2],
                 'ref_id': row[3], 'symbol': row[4], 'plan': _thaw_plan(row[5]),
                 'valid_until': row[6], 'generated_at': row[7]}
                for row in self.db.execute(
                    "SELECT intent_id,instance_id,trade_id,ref_id,symbol,plan,"
                    " valid_until,generated_at FROM execution_intents"
                    " WHERE state='DISPATCHING' ORDER BY created_at,intent_id")]

    def abandon_intent(self, intent_id):
        """Terminate an intent the owner proved never reached the book.

        The caller must have checked the execution authority for an action
        with this trade's identity. Releasing here is safe only under that
        proof; a timer alone is not proof.
        """
        row = self.db.execute(
            "SELECT instance_id,trade_id FROM execution_intents"
            " WHERE intent_id=? AND state='DISPATCHING'", (intent_id,)).fetchone()
        if row is None:
            return {'abandoned': False, 'reason': 'ADMISSION_INTENT_NOT_DISPATCHING'}
        instance_id, trade_id = row
        self.db.execute('BEGIN IMMEDIATE')
        try:
            cursor = self.db.execute(
                "UPDATE execution_intents SET state='EXPIRED'"
                " WHERE intent_id=? AND state='DISPATCHING'", (intent_id,))
            if cursor.rowcount != 1:
                self.db.rollback()
                return {'abandoned': False, 'reason': 'ADMISSION_INTENT_NOT_DISPATCHING'}
            self.leases.release_in_transaction(instance_id, trade_id)
            self.budget.release_in_transaction(instance_id, trade_id)
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return {'abandoned': True, 'intent_id': intent_id, 'instance_id': instance_id,
                'trade_id': trade_id}

    def quarantined_intents(self):
        """Intents held back for reconciliation: no dispatch, no timer release."""
        return [{'intent_id': row[0], 'instance_id': row[1], 'trade_id': row[2],
                 'state': row[3], 'created_at': row[4]}
                for row in self.db.execute(
                    "SELECT intent_id,instance_id,trade_id,state,created_at"
                    " FROM execution_intents WHERE state='QUARANTINED'"
                    " ORDER BY created_at,intent_id")]

    def mark_intent(self, intent_id, state):
        if state not in ('DISPATCHING', 'DISPATCHED', 'SETTLED', 'EXPIRED'):
            raise ValueError('ADMISSION_INTENT_STATE_INVALID')
        with atomic(self.db):
            if state == 'DISPATCHING':
                # Only a still-pending intent may enter dispatch, so a repeated
                # or concurrent caller cannot start the same trade twice.
                cursor = self.db.execute(
                    "UPDATE execution_intents SET state='DISPATCHING'"
                    " WHERE intent_id=? AND state='PENDING'", (intent_id,))
            else:
                cursor = self.db.execute(
                    "UPDATE execution_intents SET state=?"
                    " WHERE intent_id=? AND state IN ('DISPATCHING','DISPATCHED')",
                    (state, intent_id))
        return cursor.rowcount

    # -- settlement ------------------------------------------------------
    def settle(self, instance_id, trade_id, *, authority):
        """Release money and symbol ownership only on authoritative evidence.

        ``evidence`` must come from the execution authority, not from the
        caller's opinion: remaining quantity, open orders, pending approvals and
        unknown intents all have to be zero. A trade still holding a share
        cannot release its symbol, which is what let another instance take a
        symbol the account was still exposed to.
        """
        # Settlement releases a symbol and money. An unreconciled account's
        # execution facts are exactly what this decision would depend on, so it
        # reads the same durable gate as entries instead of assuming flat.
        integrity = self.execution_integrity_block()
        if integrity:
            return {'settled': False, 'reason': integrity}
        reader = getattr(authority, 'flat_evidence', None)
        if not callable(reader):
            # A dictionary of numbers is a claim; an authority is a fact. Only
            # the execution book can say whether this account is flat.
            raise ValueError('ADMISSION_FLAT_EVIDENCE_REQUIRED')
        evidence = reader(trade_id)
        if not isinstance(evidence, dict) or evidence.get('source') != 'execution_book':
            raise ValueError('ADMISSION_FLAT_EVIDENCE_UNTRUSTED')
        # The evidence must be a fact about this trade, observed recently, and
        # must state every quantity explicitly. Anything missing is refused
        # rather than defaulted, because a default is what let a caller declare
        # a flat account that still held a share.
        required = ('remaining_quantity', 'open_orders', 'pending_approvals',
                    'unknown_intents', 'active_actions', 'trade_id', 'observed_at')
        missing = [key for key in required if key not in evidence]
        if missing:
            raise ValueError('ADMISSION_FLAT_EVIDENCE_INCOMPLETE:' + ','.join(missing))
        if evidence['trade_id'] != trade_id:
            return {'settled': False, 'reason': 'ADMISSION_EVIDENCE_TRADE_MISMATCH'}
        # The trade must be finished, not merely absent from the order list. An
        # entry that has not filled has a quantity of zero too, and releasing on
        # that would free the symbol while a READY entry could still execute.
        if evidence.get('phase') != 'FLAT_VERIFIED':
            return {'settled': False, 'reason': 'ADMISSION_TRADE_NOT_TERMINAL'}
        try:
            observed = timestamp(evidence['observed_at'])
        except (ValueError, TypeError):
            raise ValueError('ADMISSION_FLAT_EVIDENCE_INVALID') from None
        age = (self.clock() - observed).total_seconds()
        if not 0 <= age <= 30:
            return {'settled': False, 'reason': 'ADMISSION_EVIDENCE_STALE'}
        try:
            remaining = number(evidence.get('remaining_quantity', '0'))
            orders = int(evidence.get('open_orders', 0))
            approvals = int(evidence.get('pending_approvals', 0))
            unknown = int(evidence.get('unknown_intents', 0))
        except (ValueError, TypeError):
            raise ValueError('ADMISSION_FLAT_EVIDENCE_INVALID') from None
        if remaining > _SLACK:
            return {'settled': False, 'reason': 'ADMISSION_POSITION_NOT_FLAT'}
        if orders or approvals or unknown or int(evidence.get('active_actions') or 0):
            # A READY or RUNNING action is still an intention to trade, even
            # when the broker reports no open order for it yet.
            return {'settled': False, 'reason': 'ADMISSION_TRADE_STILL_ACTIVE'}
        guard = self.leases.assert_releasable(instance_id, trade_id,
                                              reason=evidence.get('reason',
                                                                  'flat_confirmed'))
        if not guard['releasable']:
            return {'settled': False, 'reason': guard['reason']}
        self.db.execute('BEGIN IMMEDIATE')
        try:
            released = self.leases.release_in_transaction(instance_id, trade_id)
            freed = self.budget.release_in_transaction(instance_id, trade_id)
            self.db.execute("UPDATE execution_intents SET state='SETTLED'"
                            " WHERE instance_id=? AND trade_id=? AND state='DISPATCHED'",
                            (instance_id, trade_id))
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return {'settled': True, 'leases_released': released, 'budget_released': freed}


def _canonical(value):
    from trading_contracts.values import canonical
    return canonical(value)


def _thaw_plan(encoded):
    import json
    return json.loads(encoded)
