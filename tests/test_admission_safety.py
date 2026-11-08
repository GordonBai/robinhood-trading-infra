"""Second-round review findings, pinned as permanent regressions.

Each test here is one of the review's reproductions: a partial reservation, a
release without evidence, a wrong cost, a missing permission gate, a final
commit that outlives its validity, and a fill redelivery that changed its
economics while keeping the same cash flow.
"""
import sqlite3
import tempfile
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest import TestCase
from unittest.mock import patch

from trading_contracts.instance import InstanceBudget, PluginManifest
from trading_contracts.strategy import ProposalAction, StrategyIdentity, StrategyProposal
from trading_infra.admission import AdmissionService
from trading_infra.attribution import ExecutionAttribution
from trading_infra.budget import AccountBudgetAuthority
from trading_infra.instance_registry import InstanceRegistry
from trading_infra.symbol_lease import SymbolLease

AT = datetime(2026, 10, 29, 14, 0, tzinfo=timezone.utc)
ACCOUNT = {'max_open_cash': Decimal('500'), 'max_open_notional': Decimal('500'),
           'max_open_planned_loss': Decimal('10.00'), 'max_concurrent_trades': 4}
POLICY = {'max_position_notional_usd': '100', 'max_planned_loss_per_trade_usd': '2.50'}


def manifest(plugin_id='alpha', cost='0.05'):
    return PluginManifest(plugin_id=plugin_id, version='1.0', state_schema='counter-1',
                          config={'step': '1', 'cost_buffer_per_share': cost},
                          data_requirements=('quotes',),
                          capabilities=('advisory-entry',), max_inflight=64)


def budget():
    return InstanceBudget(max_cash=Decimal('100'), max_planned_loss=Decimal('2.50'),
                          max_notional=Decimal('100'), max_concurrent=2)


class Base(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = sqlite3.connect(self.temp.name + '/safety.sqlite', isolation_level=None)
        self.addCleanup(self.db.close)
        self.now = AT
        self.clock = lambda: self.now
        self.registry = InstanceRegistry(self.db, clock=self.clock)
        self.registry.register('alpha', manifest(), budget(), mode='shadow', enabled=True)
        self.money = AccountBudgetAuthority(self.db, ACCOUNT, clock=self.clock)
        self.addCleanup(self.money.close)
        self.leases = SymbolLease(self.db, clock=self.clock)
        self.addCleanup(self.leases.close)
        self.feedback = ExecutionAttribution(self.db, clock=self.clock)
        self.service = AdmissionService(budget=self.money, leases=self.leases,
                                        attribution=self.feedback, policy=POLICY,
                                        registry=self.registry, clock=self.clock)

    def proposal(self, *, instance='alpha', symbol='SPY', ceiling='40.00',
                 stop='39.60', valid_for=20, quantity_limit=10):
        return StrategyProposal(
            identity=StrategyIdentity(instance, 'alpha', '1.0'),
            proposal_id='p-' + symbol, decision_id='d-' + symbol, setup_id='s-1',
            data_snapshot_id='snap', source_sha256='a' * 64,
            generated_at=self.now, valid_until=self.now + timedelta(seconds=valid_for),
            action=ProposalAction.ENTER, symbol=symbol, reason='FIXTURE_ENTRY',
            synthetic=True, quantity_limit=quantity_limit,
            entry_ceiling=Decimal(ceiling), stop_price=Decimal(stop))

    def admit(self, **kw):
        return self.service.admit(self.registry.instance(kw.pop('instance', 'alpha')),
                                  self.proposal(**kw), allow_entries=True)


class AtomicityTests(Base):
    """R2-02: a storage failure must leave no lease and no reservation."""

    @contextmanager
    def failing(self, target, method):
        with patch.object(target, method,
                          side_effect=sqlite3.OperationalError('injected failure')):
            yield

    def test_a_budget_failure_leaves_no_lease(self):
        with self.failing(self.money, 'reserve_in_transaction'):
            with self.assertRaises(sqlite3.OperationalError):
                self.admit()
        self.assertIsNone(self.leases.owner('SPY'))
        self.assertEqual(self.money.status()['open_cash'], '0')
        self.assertEqual(self.service.pending_intents(), [])

    def test_an_attribution_failure_leaves_no_lease_and_no_reservation(self):
        with self.failing(self.feedback, 'bind_in_transaction'):
            with self.assertRaises(sqlite3.OperationalError):
                self.admit()
        self.assertIsNone(self.leases.owner('SPY'))
        self.assertEqual(self.money.status()['open_cash'], '0')
        self.assertEqual(self.service.pending_intents(), [])

    def test_a_lease_failure_leaves_no_reservation(self):
        with self.failing(self.leases, 'acquire_in_transaction'):
            with self.assertRaises(sqlite3.OperationalError):
                self.admit()
        self.assertEqual(self.money.status()['open_cash'], '0')

    def test_components_must_share_one_connection(self):
        other = sqlite3.connect(self.temp.name + '/other.sqlite', isolation_level=None)
        self.addCleanup(other.close)
        stray = SymbolLease(other, clock=self.clock)
        self.addCleanup(stray.close)
        with self.assertRaises(ValueError) as ctx:
            AdmissionService(budget=self.money, leases=stray, attribution=self.feedback,
                             policy=POLICY, registry=self.registry, clock=self.clock)
        self.assertEqual(str(ctx.exception), 'ADMISSION_SINGLE_CONNECTION_REQUIRED')


class FakeAuthority:
    """Stands in for the execution book: the only producer of release facts."""

    def __init__(self, **facts):
        self.facts = facts

    def flat_evidence(self, trade_id):
        evidence = {'source': 'execution_book', 'account_key': 'acct',
                    'trade_id': trade_id, 'phase': 'FLAT_VERIFIED',
                    'remaining_quantity': '0', 'open_orders': 0,
                    'pending_approvals': 0, 'unknown_intents': 0,
                    'active_actions': 0,
                    'observed_at': AT.isoformat()}
        evidence.update(self.facts)
        return evidence


def flat(**overrides):
    return FakeAuthority(**overrides)


class EvidenceTests(Base):
    """R2-03: ownership is released on facts, not on a caller's string."""

    def test_a_position_that_is_not_flat_blocks_release(self):
        self.assertTrue(self.admit()['admitted'])
        result = self.service.settle('alpha', 'p-SPY',
                                     authority=flat(remaining_quantity='1'))
        self.assertFalse(result['settled'])
        self.assertEqual(result['reason'], 'ADMISSION_POSITION_NOT_FLAT')
        self.assertEqual(self.leases.owner('SPY'), 'alpha')

    def test_open_orders_approvals_and_unknown_intents_all_block_release(self):
        self.assertTrue(self.admit()['admitted'])
        for key in ('open_orders', 'pending_approvals', 'unknown_intents'):
            result = self.service.settle('alpha', 'p-SPY',
                                         authority=flat(**{key: 1}))
            self.assertFalse(result['settled'], key)
            self.assertEqual(result['reason'], 'ADMISSION_TRADE_STILL_ACTIVE')
            self.assertEqual(self.leases.owner('SPY'), 'alpha')

    def test_a_reason_string_alone_is_not_evidence(self):
        self.assertTrue(self.admit()['admitted'])
        # Missing the authoritative facts: refused, nothing released.
        with self.assertRaises(ValueError):
            self.service.settle('alpha', 'p-SPY',
                                authority={'remaining_quantity': '0',
                                           'reason': 'flat_confirmed'})
        self.assertEqual(self.leases.owner('SPY'), 'alpha')
        # With every fact present and this trade's identity, it settles.
        self.assertTrue(self.service.settle('alpha', 'p-SPY',
                                            authority=flat())['settled'])
        self.assertIsNone(self.leases.owner('SPY'))

    def test_evidence_for_another_trade_is_refused(self):
        self.assertTrue(self.admit()['admitted'])
        result = self.service.settle('alpha', 'p-SPY', authority=flat(trade_id='other'))
        self.assertFalse(result['settled'])
        self.assertEqual(result['reason'], 'ADMISSION_EVIDENCE_TRADE_MISMATCH')

    def test_malformed_evidence_is_refused(self):
        self.assertTrue(self.admit()['admitted'])
        for value in (None, {}, {'remaining_quantity': 'abc'}):
            with self.assertRaises(ValueError):
                self.service.settle('alpha', 'p-SPY', authority=value)
        # A stale observation is refused as a fact about the account, not an error.
        stale = self.service.settle('alpha', 'p-SPY',
                                    authority=flat(observed_at='2026-10-29T13:00:00+00:00'))
        self.assertFalse(stale['settled'])
        self.assertEqual(stale['reason'], 'ADMISSION_EVIDENCE_STALE')
        self.assertEqual(self.leases.owner('SPY'), 'alpha')


class CostTests(Base):
    """R2-04: one cost, used by sizing, reservation and the plan."""

    def test_the_instances_declared_cost_is_the_one_used(self):
        self.registry.register('cost10', manifest('cost10', cost='0.10'), budget(),
                               mode='shadow', enabled=True)
        result = self.service.admit(
            self.registry.instance('cost10'),
            self.proposal(instance='cost10', ceiling='10.00', stop='9.70',
                          quantity_limit=99),
            allow_entries=True)
        # 2.50 risk / (0.30 spread + 0.10 cost) = 6 whole shares.
        self.assertEqual(result['quantity'], 6)
        self.assertEqual(Decimal(result['cost_buffer_per_share']), Decimal('0.10'))
        self.assertEqual(Decimal(result['planned_loss']), Decimal('2.40'))
        self.assertLessEqual(Decimal(result['planned_loss']), Decimal('2.50'))

    def test_an_instance_without_a_declared_cost_is_refused(self):
        no_cost = PluginManifest(plugin_id='nocost', version='1.0', state_schema='s-1',
                                 config={'step': '1'}, data_requirements=('quotes',),
                                 capabilities=('advisory-entry',), max_inflight=4)
        self.registry.register('nocost', no_cost, budget(), mode='shadow', enabled=True)
        with self.assertRaises(ValueError) as ctx:
            self.service.admit(self.registry.instance('nocost'),
                               self.proposal(instance='nocost'), allow_entries=True)
        self.assertEqual(str(ctx.exception), 'ADMISSION_INSTANCE_COST_REQUIRED')


class PermissionTests(Base):
    """R2-05: a held, capturing or disabled session reserves nothing."""

    def test_entries_not_permitted_reserves_nothing(self):
        for kwargs in ({'allow_entries': False}, {'allow_entries': True,
                                                  'capture_only': True}):
            result = self.service.admit(self.registry.instance('alpha'),
                                        self.proposal(), **kwargs)
            self.assertFalse(result['admitted'])
            self.assertEqual(result['reason'], 'ADMISSION_ENTRIES_NOT_PERMITTED')
        self.assertIsNone(self.leases.owner('SPY'))
        self.assertEqual(self.money.status()['open_cash'], '0')

    def test_a_disabled_instance_cannot_admit_even_with_a_queued_proposal(self):
        instance = self.registry.instance('alpha')
        self.registry.set_mode('alpha', 'disabled', enabled=False)
        result = self.service.admit(instance, self.proposal(), allow_entries=True)
        self.assertEqual(result['reason'], 'ADMISSION_INSTANCE_DISABLED')
        self.assertIsNone(self.leases.owner('SPY'))


class FinalWindowTests(Base):
    """R2-06: the window is re-checked at the write, not only on entry."""

    def test_a_proposal_that_expires_during_the_lease_stage_is_refused(self):
        original = self.leases.acquire_in_transaction
        service = self.service

        def slow(instance_id, symbols, *, trade_id):
            self.now = self.now + timedelta(seconds=21)
            return original(instance_id, symbols, trade_id=trade_id)

        with patch.object(self.leases, 'acquire_in_transaction', side_effect=slow):
            result = service.admit(self.registry.instance('alpha'),
                                   self.proposal(valid_for=20), allow_entries=True)
        self.assertFalse(result['admitted'])
        self.assertEqual(result['reason'], 'ADMISSION_PROPOSAL_EXPIRED')
        self.assertIsNone(self.leases.owner('SPY'))
        self.assertEqual(self.money.status()['open_cash'], '0')
        self.assertEqual(service.pending_intents(), [])

    def test_a_stale_event_is_refused(self):
        result = self.service.admit(
            self.registry.instance('alpha'), self.proposal(), allow_entries=True,
            event_at=self.now - timedelta(seconds=31))
        self.assertFalse(result['admitted'])
        self.assertEqual(result['reason'], 'ADMISSION_EVENT_STALE')


class IntentBridgeTests(Base):
    """R2-01: admission leaves a durable, recoverable execution intent."""

    def test_an_admitted_proposal_leaves_a_pending_intent(self):
        result = self.admit()
        self.assertTrue(result['admitted'])
        pending = self.service.pending_intents('alpha')
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]['intent_id'], result['intent_id'])
        self.assertEqual(pending[0]['ref_id'], result['ref_id'])
        self.assertEqual(pending[0]['plan']['quantity'], result['quantity'])
        self.assertEqual(self.feedback.owner(result['ref_id'])['instance_id'], 'alpha')

    def test_an_expired_intent_is_not_pending(self):
        self.admit()
        self.now = self.now + timedelta(seconds=60)
        self.assertEqual(self.service.pending_intents('alpha'), [])

    def test_the_same_proposal_cannot_produce_two_intents(self):
        first = self.admit()
        self.assertTrue(first['admitted'])
        second = self.service.admit(self.registry.instance('alpha'),
                                    self.proposal(), allow_entries=True)
        self.assertFalse(second['admitted'])
        self.assertEqual(len(self.service.pending_intents('alpha')), 1)

    def test_an_intent_that_entered_dispatch_is_never_released_on_a_timer(self):
        result = self.admit()
        # DISPATCHING means a propose may already have created an action.
        self.assertEqual(self.service.mark_intent(result['intent_id'], 'DISPATCHING'), 1)
        self.now = self.now + timedelta(seconds=60)
        self.assertEqual(self.service.expire_intents(), [])
        self.assertEqual(self.leases.owner('SPY'), 'alpha')
        self.assertEqual(self.money.status()['open_cash'], '80.00')

    def test_an_intent_cannot_enter_dispatch_twice(self):
        result = self.admit()
        self.assertEqual(self.service.mark_intent(result['intent_id'], 'DISPATCHING'), 1)
        self.assertEqual(self.service.mark_intent(result['intent_id'], 'DISPATCHING'), 0)

    def test_an_undispatched_intent_is_released_when_its_window_closes(self):
        result = self.admit()
        self.assertTrue(result['admitted'])
        self.assertEqual(self.money.status()['open_cash'], '80.00')
        # Still inside the window: nothing is released yet.
        self.assertEqual(self.service.expire_intents(), [])
        self.assertEqual(self.leases.owner('SPY'), 'alpha')

        self.now = self.now + timedelta(seconds=60)
        expired = self.service.expire_intents()
        self.assertEqual(len(expired), 1)
        self.assertTrue(expired[0]['released'])
        self.assertIsNone(self.leases.owner('SPY'))
        self.assertEqual(self.money.status()['open_cash'], '0')
        row = self.db.execute('SELECT state FROM execution_intents').fetchone()
        self.assertEqual(row[0], 'EXPIRED')

    def test_a_dispatched_intent_is_never_released_as_unsent(self):
        result = self.admit()
        self.assertEqual(self.service.mark_intent(result['intent_id'], 'DISPATCHING'), 1)
        self.assertEqual(self.service.mark_intent(result['intent_id'], 'DISPATCHED'), 1)
        self.now = self.now + timedelta(seconds=60)
        self.assertEqual(self.service.expire_intents(), [])
        self.assertEqual(self.leases.owner('SPY'), 'alpha')
        self.assertEqual(self.money.status()['open_cash'], '80.00')

    def test_an_upgraded_database_gains_generated_at_and_retires_legacy_rows(self):
        # Simulate the previous schema, with one pending intent left behind.
        import sqlite3 as _sqlite
        with tempfile.TemporaryDirectory() as directory:
            path = directory + '/legacy.sqlite'
            db = _sqlite.connect(path, isolation_level=None)
            self.addCleanup(db.close)
            db.executescript('''
                CREATE TABLE execution_intents(
                    intent_id TEXT PRIMARY KEY, instance_id TEXT NOT NULL,
                    trade_id TEXT NOT NULL, proposal_id TEXT NOT NULL,
                    ref_id TEXT NOT NULL UNIQUE, symbol TEXT NOT NULL,
                    quantity INTEGER NOT NULL, entry_ceiling TEXT NOT NULL,
                    stop_price TEXT NOT NULL, planned_loss TEXT NOT NULL,
                    notional TEXT NOT NULL, plan TEXT NOT NULL, state TEXT NOT NULL,
                    created_at TEXT NOT NULL, valid_until TEXT NOT NULL);
                INSERT INTO execution_intents VALUES(
                    'old-1','alpha','old-trade','p','ref-old','SPY',1,'40','39.6',
                    '0.4','40','{}','PENDING','2026-10-29T14:00:00+00:00',
                    '2030-01-01T00:00:00+00:00');
            ''')
            registry = InstanceRegistry(db, clock=self.clock)
            registry.register('alpha', manifest(), budget(), mode='shadow', enabled=True)
            money = AccountBudgetAuthority(db, ACCOUNT, clock=self.clock)
            self.addCleanup(money.close)
            leases = SymbolLease(db, clock=self.clock)
            self.addCleanup(leases.close)
            service = AdmissionService(budget=money, leases=leases,
                                       attribution=ExecutionAttribution(db, clock=self.clock),
                                       policy=POLICY, registry=registry, clock=self.clock)
            columns = {row[1] for row in db.execute('PRAGMA table_info(execution_intents)')}
            self.assertIn('generated_at', columns)
            # The legacy intent has no original envelope, so it is quarantined:
            # it may already have produced a book action, so neither dispatch
            # nor release is safe. Nothing is freed on its behalf.
            self.assertEqual(service.pending_intents(), [])
            row = db.execute("SELECT state FROM execution_intents WHERE intent_id='old-1'").fetchone()
            self.assertEqual(row[0], 'QUARANTINED')
            self.assertEqual(service.expire_intents(), [])
            self.assertTrue(service.quarantined_intents())

    def test_a_pending_legacy_table_without_dispatching_is_rebuilt(self):
        import sqlite3 as _sqlite
        with tempfile.TemporaryDirectory() as directory:
            db = _sqlite.connect(directory + '/v1.sqlite', isolation_level=None)
            self.addCleanup(db.close)
            # The previous release's real DDL: generated_at present, but the
            # CHECK constraint knows nothing about DISPATCHING.
            db.executescript('''
                CREATE TABLE execution_intents(
                    intent_id TEXT PRIMARY KEY, instance_id TEXT NOT NULL,
                    trade_id TEXT NOT NULL, proposal_id TEXT NOT NULL,
                    ref_id TEXT NOT NULL UNIQUE, symbol TEXT NOT NULL,
                    quantity INTEGER NOT NULL, entry_ceiling TEXT NOT NULL,
                    stop_price TEXT NOT NULL, planned_loss TEXT NOT NULL,
                    notional TEXT NOT NULL, plan TEXT NOT NULL,
                    state TEXT NOT NULL CHECK(state IN
                        ('PENDING','DISPATCHED','SETTLED','EXPIRED')),
                    created_at TEXT NOT NULL, generated_at TEXT NOT NULL,
                    valid_until TEXT NOT NULL);
            ''')
            registry = InstanceRegistry(db, clock=self.clock)
            registry.register('alpha', manifest(), budget(), mode='shadow', enabled=True)
            money = AccountBudgetAuthority(db, ACCOUNT, clock=self.clock)
            self.addCleanup(money.close)
            leases = SymbolLease(db, clock=self.clock)
            self.addCleanup(leases.close)
            service = AdmissionService(budget=money, leases=leases,
                                       attribution=ExecutionAttribution(db, clock=self.clock),
                                       policy=POLICY, registry=registry, clock=self.clock)
            # The old constraint would reject this outright.
            admitted = service.admit(registry.instance('alpha'), self.proposal(),
                                     allow_entries=True)
            self.assertTrue(admitted['admitted'])
            self.assertEqual(service.mark_intent(admitted['intent_id'], 'DISPATCHING'), 1)
            self.assertEqual(service.mark_intent(admitted['intent_id'], 'DISPATCHED'), 1)

    def test_settle_refuses_a_trade_that_is_not_terminal(self):
        result = self.admit()
        self.assertTrue(result['admitted'])
        for phase in ('ENTRY_PLANNED', 'ENTRY_OPEN', 'NEEDS_PROTECTION', None):
            refused = self.service.settle('alpha', result['trade_id'],
                                          authority=flat(phase=phase))
            self.assertFalse(refused['settled'], phase)
            self.assertEqual(refused['reason'], 'ADMISSION_TRADE_NOT_TERMINAL')
            self.assertEqual(self.leases.owner('SPY'), 'alpha')
            self.assertEqual(self.money.status()['open_cash'], '80.00')

    def test_a_null_legacy_row_survives_the_rebuild_and_stays_visible(self):
        import sqlite3 as _sqlite
        with tempfile.TemporaryDirectory() as directory:
            db = _sqlite.connect(directory + '/null.sqlite', isolation_level=None)
            self.addCleanup(db.close)
            db.executescript('''
                CREATE TABLE execution_intents(
                    intent_id TEXT PRIMARY KEY, instance_id TEXT NOT NULL,
                    trade_id TEXT NOT NULL, proposal_id TEXT NOT NULL,
                    ref_id TEXT NOT NULL UNIQUE, symbol TEXT NOT NULL,
                    quantity INTEGER NOT NULL, entry_ceiling TEXT NOT NULL,
                    stop_price TEXT NOT NULL, planned_loss TEXT NOT NULL,
                    notional TEXT NOT NULL, plan TEXT NOT NULL,
                    state TEXT NOT NULL CHECK(state IN
                        ('PENDING','DISPATCHED','SETTLED','EXPIRED')),
                    created_at TEXT NOT NULL, generated_at TEXT,
                    valid_until TEXT NOT NULL);
            ''')
            # A row written by the previous release's broken migration.
            db.execute("INSERT INTO execution_intents VALUES('old-null','alpha',"
                       "'old-trade','p','ref-old','SPY',1,'40','39.6','0.4','40',"
                       "'{}','PENDING','2026-10-29T14:00:00+00:00',NULL,"
                       "'2030-01-01T00:00:00+00:00')")
            registry = InstanceRegistry(db, clock=self.clock)
            registry.register('alpha', manifest(), budget(), mode='shadow', enabled=True)
            money = AccountBudgetAuthority(db, ACCOUNT, clock=self.clock)
            self.addCleanup(money.close)
            leases = SymbolLease(db, clock=self.clock)
            self.addCleanup(leases.close)
            service = AdmissionService(budget=money, leases=leases,
                                       attribution=ExecutionAttribution(db, clock=self.clock),
                                       policy=POLICY, registry=registry, clock=self.clock)
            # The row is neither lost nor released: it is held for reconciliation.
            row = db.execute("SELECT state,generated_at FROM execution_intents"
                             " WHERE intent_id='old-null'").fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row[0], 'QUARANTINED')
            # Still no original envelope: normalised to empty, never to now.
            self.assertFalse(row[1] or '')
            self.assertEqual(service.pending_intents(), [])
            self.assertEqual([item['intent_id'] for item in service.quarantined_intents()],
                             ['old-null'])
            # A second start must not report the migration wrong or drop it.
            again = AdmissionService(budget=money, leases=leases,
                                     attribution=ExecutionAttribution(db, clock=self.clock),
                                     policy=POLICY, registry=registry, clock=self.clock)
            self.assertEqual([item['intent_id'] for item in again.quarantined_intents()],
                             ['old-null'])

    def _legacy_service(self, db, registry, money, leases):
        return AdmissionService(budget=money, leases=leases,
                                attribution=ExecutionAttribution(db, clock=self.clock),
                                policy=POLICY, registry=registry, clock=self.clock)

    def test_a_leftover_old_table_with_a_null_row_is_merged_not_dropped(self):
        import sqlite3 as _sqlite
        with tempfile.TemporaryDirectory() as directory:
            db = _sqlite.connect(directory + '/leftover.sqlite', isolation_level=None)
            self.addCleanup(db.close)
            # Exactly the state an interrupted R6 migration leaves behind: the
            # new table exists but is the broken NOT NULL shape and empty.
            db.executescript('''
                CREATE TABLE execution_intents(
                    intent_id TEXT PRIMARY KEY, instance_id TEXT NOT NULL,
                    trade_id TEXT NOT NULL, proposal_id TEXT NOT NULL,
                    ref_id TEXT NOT NULL UNIQUE, symbol TEXT NOT NULL,
                    quantity INTEGER NOT NULL, entry_ceiling TEXT NOT NULL,
                    stop_price TEXT NOT NULL, planned_loss TEXT NOT NULL,
                    notional TEXT NOT NULL, plan TEXT NOT NULL, state TEXT NOT NULL,
                    created_at TEXT NOT NULL, generated_at TEXT NOT NULL,
                    valid_until TEXT NOT NULL);
                CREATE TABLE execution_intents_old(
                    intent_id TEXT PRIMARY KEY, instance_id TEXT NOT NULL,
                    trade_id TEXT NOT NULL, proposal_id TEXT NOT NULL,
                    ref_id TEXT NOT NULL UNIQUE, symbol TEXT NOT NULL,
                    quantity INTEGER NOT NULL, entry_ceiling TEXT NOT NULL,
                    stop_price TEXT NOT NULL, planned_loss TEXT NOT NULL,
                    notional TEXT NOT NULL, plan TEXT NOT NULL, state TEXT NOT NULL,
                    created_at TEXT NOT NULL, generated_at TEXT,
                    valid_until TEXT NOT NULL);
                INSERT INTO execution_intents_old VALUES(
                    'res-1','alpha','res-trade','p','ref-res','SPY',1,'40','39.6',
                    '0.4','40','{}','PENDING','2026-10-29T14:00:00+00:00',NULL,
                    '2030-01-01T00:00:00+00:00');
            ''')
            registry = InstanceRegistry(db, clock=self.clock)
            registry.register('alpha', manifest(), budget(), mode='shadow', enabled=True)
            money = AccountBudgetAuthority(db, ACCOUNT, clock=self.clock)
            self.addCleanup(money.close)
            leases = SymbolLease(db, clock=self.clock)
            self.addCleanup(leases.close)
            service = self._legacy_service(db, registry, money, leases)
            row = db.execute("SELECT state FROM execution_intents"
                             " WHERE intent_id='res-1'").fetchone()
            self.assertIsNotNone(row, 'the leftover row must survive the merge')
            self.assertEqual(row[0], 'QUARANTINED')
            tables = {r[0] for r in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertNotIn('execution_intents_old', tables)
            self.assertEqual([item['intent_id'] for item in service.quarantined_intents()],
                             ['res-1'])

    def test_recovery_keeps_valid_rows_already_in_the_current_table(self):
        import sqlite3 as _sqlite
        with tempfile.TemporaryDirectory() as directory:
            db = _sqlite.connect(directory + '/both.sqlite', isolation_level=None)
            self.addCleanup(db.close)
            db.executescript('''
                CREATE TABLE execution_intents(
                    intent_id TEXT PRIMARY KEY, instance_id TEXT NOT NULL,
                    trade_id TEXT NOT NULL, proposal_id TEXT NOT NULL,
                    ref_id TEXT NOT NULL UNIQUE, symbol TEXT NOT NULL,
                    quantity INTEGER NOT NULL, entry_ceiling TEXT NOT NULL,
                    stop_price TEXT NOT NULL, planned_loss TEXT NOT NULL,
                    notional TEXT NOT NULL, plan TEXT NOT NULL, state TEXT NOT NULL,
                    created_at TEXT NOT NULL, generated_at TEXT NOT NULL,
                    valid_until TEXT NOT NULL);
                CREATE TABLE execution_intents_old(
                    intent_id TEXT PRIMARY KEY, instance_id TEXT NOT NULL,
                    trade_id TEXT NOT NULL, proposal_id TEXT NOT NULL,
                    ref_id TEXT NOT NULL UNIQUE, symbol TEXT NOT NULL,
                    quantity INTEGER NOT NULL, entry_ceiling TEXT NOT NULL,
                    stop_price TEXT NOT NULL, planned_loss TEXT NOT NULL,
                    notional TEXT NOT NULL, plan TEXT NOT NULL, state TEXT NOT NULL,
                    created_at TEXT NOT NULL, generated_at TEXT,
                    valid_until TEXT NOT NULL);
                INSERT INTO execution_intents VALUES(
                    'live-1','beta','live-trade','p','ref-live','QQQ',1,'40','39.6',
                    '0.4','40','{}','DISPATCHED','2026-10-29T14:00:00+00:00',
                    '2026-10-29T13:59:00+00:00','2030-01-01T00:00:00+00:00');
                INSERT INTO execution_intents_old VALUES(
                    'res-2','alpha','res-trade','p','ref-res2','SPY',1,'40','39.6',
                    '0.4','40','{}','PENDING','2026-10-29T14:00:00+00:00',NULL,
                    '2030-01-01T00:00:00+00:00');
            ''')
            registry = InstanceRegistry(db, clock=self.clock)
            registry.register('alpha', manifest(), budget(), mode='shadow', enabled=True)
            money = AccountBudgetAuthority(db, ACCOUNT, clock=self.clock)
            self.addCleanup(money.close)
            leases = SymbolLease(db, clock=self.clock)
            self.addCleanup(leases.close)
            self._legacy_service(db, registry, money, leases)
            kept = {row[0]: row[1] for row in db.execute(
                'SELECT intent_id,state FROM execution_intents')}
            self.assertEqual(kept.get('live-1'), 'DISPATCHED')
            self.assertEqual(kept.get('res-2'), 'QUARANTINED')

    def test_two_sources_disagreeing_about_one_intent_fail_closed(self):
        import sqlite3 as _sqlite
        with tempfile.TemporaryDirectory() as directory:
            db = _sqlite.connect(directory + '/clash.sqlite', isolation_level=None)
            self.addCleanup(db.close)
            db.executescript('''
                CREATE TABLE execution_intents(
                    intent_id TEXT PRIMARY KEY, instance_id TEXT NOT NULL,
                    trade_id TEXT NOT NULL, proposal_id TEXT NOT NULL,
                    ref_id TEXT NOT NULL UNIQUE, symbol TEXT NOT NULL,
                    quantity INTEGER NOT NULL, entry_ceiling TEXT NOT NULL,
                    stop_price TEXT NOT NULL, planned_loss TEXT NOT NULL,
                    notional TEXT NOT NULL, plan TEXT NOT NULL, state TEXT NOT NULL,
                    created_at TEXT NOT NULL, generated_at TEXT NOT NULL,
                    valid_until TEXT NOT NULL);
                CREATE TABLE execution_intents_old(
                    intent_id TEXT PRIMARY KEY, instance_id TEXT NOT NULL,
                    trade_id TEXT NOT NULL, proposal_id TEXT NOT NULL,
                    ref_id TEXT NOT NULL UNIQUE, symbol TEXT NOT NULL,
                    quantity INTEGER NOT NULL, entry_ceiling TEXT NOT NULL,
                    stop_price TEXT NOT NULL, planned_loss TEXT NOT NULL,
                    notional TEXT NOT NULL, plan TEXT NOT NULL, state TEXT NOT NULL,
                    created_at TEXT NOT NULL, generated_at TEXT,
                    valid_until TEXT NOT NULL);
                INSERT INTO execution_intents VALUES(
                    'same','beta','b','p','ref-b','QQQ',1,'40','39.6','0.4','40','{}',
                    'DISPATCHED','2026-10-29T14:00:00+00:00','2026-10-29T14:00:00+00:00',
                    '2030-01-01T00:00:00+00:00');
                INSERT INTO execution_intents_old VALUES(
                    'same','alpha','a','p','ref-a','SPY',1,'40','39.6','0.4','40','{}',
                    'PENDING','2026-10-29T14:00:00+00:00',NULL,
                    '2030-01-01T00:00:00+00:00');
            ''')
            registry = InstanceRegistry(db, clock=self.clock)
            registry.register('alpha', manifest(), budget(), mode='shadow', enabled=True)
            money = AccountBudgetAuthority(db, ACCOUNT, clock=self.clock)
            self.addCleanup(money.close)
            leases = SymbolLease(db, clock=self.clock)
            self.addCleanup(leases.close)
            with self.assertRaises(ValueError) as ctx:
                self._legacy_service(db, registry, money, leases)
            self.assertIn('ADMISSION_MIGRATION_IDENTITY_CONFLICT', str(ctx.exception))
            tables = {r[0] for r in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            # Both originals survive: nothing was destroyed on the error path.
            self.assertIn('execution_intents', tables)
            self.assertIn('execution_intents_old', tables)


class MigrationSafetyTests(Base):
    """C5-05: future schemas and caller transactions fail before any rewrite."""

    def _stack(self, db):
        registry = InstanceRegistry(db, clock=lambda: AT)
        registry.register('alpha', manifest(), budget(), mode='shadow', enabled=True)
        money = AccountBudgetAuthority(db, {'max_open_cash': Decimal('500'),
            'max_open_notional': Decimal('500'),
            'max_open_planned_loss': Decimal('10.00'),
            'max_concurrent_trades': 4}, clock=lambda: AT)
        leases = SymbolLease(db, clock=lambda: AT)
        attribution = ExecutionAttribution(db, clock=lambda: AT)
        return lambda: AdmissionService(budget=money, leases=leases,
            attribution=attribution, policy=POLICY, registry=registry,
            clock=lambda: AT), money, leases

    def test_a_future_admission_version_is_refused_without_rewriting_rows(self):
        import sqlite3 as _sqlite
        with tempfile.TemporaryDirectory() as directory:
            db = _sqlite.connect(directory + '/future.sqlite', isolation_level=None)
            self.addCleanup(db.close)
            db.executescript('''
                CREATE TABLE admission_schema(version INTEGER NOT NULL);
                INSERT INTO admission_schema VALUES(99);
                CREATE TABLE execution_intents(
                    intent_id TEXT PRIMARY KEY, instance_id TEXT NOT NULL,
                    trade_id TEXT NOT NULL, proposal_id TEXT NOT NULL,
                    ref_id TEXT NOT NULL UNIQUE, symbol TEXT NOT NULL,
                    quantity INTEGER NOT NULL, entry_ceiling TEXT NOT NULL,
                    stop_price TEXT NOT NULL, planned_loss TEXT NOT NULL,
                    notional TEXT NOT NULL, plan TEXT NOT NULL, state TEXT NOT NULL,
                    created_at TEXT NOT NULL, generated_at TEXT,
                    valid_until TEXT NOT NULL);
                INSERT INTO execution_intents VALUES(
                    'future-1','alpha','trade','p','ref-future','SPY',1,'40','39.6',
                    '0.4','40','{}','DISPATCHED','2026-10-29T14:00:00+00:00',
                    '2026-10-29T14:00:00+00:00','2030-01-01T00:00:00+00:00');
            ''')
            make, money, leases = self._stack(db)
            self.addCleanup(money.close); self.addCleanup(leases.close)
            with self.assertRaises(ValueError) as raised:
                make()
            self.assertEqual(str(raised.exception),
                             'ADMISSION_SCHEMA_VERSION_UNSUPPORTED')
            self.assertEqual(db.execute('SELECT intent_id,state FROM execution_intents'
                                        ).fetchall(), [('future-1', 'DISPATCHED')])
            self.assertEqual(db.execute('SELECT version FROM admission_schema').fetchall(),
                             [(99,)])
            tables = {row[0] for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertNotIn('execution_intents_rebuild', tables)

    def test_a_current_version_with_a_damaged_shape_is_refused(self):
        import sqlite3 as _sqlite
        with tempfile.TemporaryDirectory() as directory:
            db = _sqlite.connect(directory + '/damaged.sqlite', isolation_level=None)
            self.addCleanup(db.close)
            db.executescript('''
                CREATE TABLE admission_schema(version INTEGER NOT NULL);
                INSERT INTO admission_schema VALUES(2);
                CREATE TABLE execution_intents(
                    intent_id TEXT PRIMARY KEY, instance_id TEXT NOT NULL,
                    trade_id TEXT NOT NULL, proposal_id TEXT NOT NULL,
                    ref_id TEXT NOT NULL UNIQUE, symbol TEXT NOT NULL,
                    quantity INTEGER NOT NULL, entry_ceiling TEXT NOT NULL,
                    stop_price TEXT NOT NULL, planned_loss TEXT NOT NULL,
                    notional TEXT NOT NULL, plan TEXT NOT NULL, state TEXT NOT NULL,
                    created_at TEXT NOT NULL, generated_at TEXT,
                    valid_until TEXT NOT NULL);
                INSERT INTO execution_intents VALUES(
                    'damaged-1','alpha','trade','p','ref-damaged','SPY',1,'40',
                    '39.6','0.4','40','{}','DISPATCHED',
                    '2026-10-29T14:00:00+00:00',
                    '2026-10-29T14:00:00+00:00','2030-01-01T00:00:00+00:00');
            ''')
            make, money, leases = self._stack(db)
            self.addCleanup(money.close); self.addCleanup(leases.close)
            with self.assertRaises(ValueError) as raised:
                make()
            self.assertEqual(str(raised.exception),
                             'ADMISSION_SCHEMA_STRUCTURE_MISMATCH')
            self.assertEqual(db.execute(
                'SELECT intent_id,state FROM execution_intents').fetchall(),
                [('damaged-1', 'DISPATCHED')])
            tables = {row[0] for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertNotIn('execution_intents_rebuild', tables)

    def test_an_open_caller_transaction_is_refused_before_ddl(self):
        self.db.execute('BEGIN IMMEDIATE')
        with self.assertRaises(ValueError) as raised:
            self.service
            AdmissionService(budget=self.money, leases=self.leases,
                             attribution=self.feedback, policy=POLICY,
                             registry=self.registry, clock=self.clock)
        self.assertEqual(str(raised.exception),
                         'ADMISSION_SCHEMA_REQUIRES_IDLE_CONNECTION')
        self.assertTrue(self.db.in_transaction)
        self.db.rollback()

    def test_a_failed_copy_rolls_back_every_table(self):
        import sqlite3 as _sqlite

        class Flaky(_sqlite.Connection):
            fail = False

            def execute(self, sql, *args):
                if (Flaky.fail and 'UPDATE execution_intents_rebuild'
                        ' SET generated_at' in sql):
                    raise _sqlite.OperationalError('injected migration failure')
                return super().execute(sql, *args)

        with tempfile.TemporaryDirectory() as directory:
            db = _sqlite.connect(directory + '/rollback.sqlite',
                                 isolation_level=None, factory=Flaky)
            self.addCleanup(db.close)
            db.executescript('''
                CREATE TABLE execution_intents(
                    intent_id TEXT PRIMARY KEY, instance_id TEXT NOT NULL,
                    trade_id TEXT NOT NULL, proposal_id TEXT NOT NULL,
                    ref_id TEXT NOT NULL UNIQUE, symbol TEXT NOT NULL,
                    quantity INTEGER NOT NULL, entry_ceiling TEXT NOT NULL,
                    stop_price TEXT NOT NULL, planned_loss TEXT NOT NULL,
                    notional TEXT NOT NULL, plan TEXT NOT NULL,
                    state TEXT NOT NULL CHECK(state IN
                        ('PENDING','DISPATCHED','SETTLED','EXPIRED')),
                    created_at TEXT NOT NULL, generated_at TEXT NOT NULL,
                    valid_until TEXT NOT NULL);
                INSERT INTO execution_intents VALUES(
                    'old-1','alpha','trade','p','ref-old','SPY',1,'40','39.6',
                    '0.4','40','{}','PENDING','2026-10-29T14:00:00+00:00',
                    '2026-10-29T13:59:00+00:00','2030-01-01T00:00:00+00:00');
            ''')
            make, money, leases = self._stack(db)
            self.addCleanup(money.close); self.addCleanup(leases.close)
            Flaky.fail = True
            with self.assertRaises(_sqlite.OperationalError):
                make()
            tables = {row[0] for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertIn('execution_intents', tables)
            self.assertNotIn('execution_intents_rebuild', tables)
            self.assertEqual(db.execute(
                "SELECT intent_id,state FROM execution_intents").fetchall(),
                [('old-1', 'PENDING')])
            self.assertEqual(db.execute(
                'SELECT * FROM admission_schema').fetchall(), [])
            Flaky.fail = False
            make()
            # This source's real DDL declared generated_at NOT NULL, so a row
            # that survived it has a real envelope and remains PENDING; only a
            # row lacking that envelope is quarantined. The retry must rebuild
            # without changing this row's identity or timestamp.
            self.assertEqual(db.execute(
                'SELECT intent_id,state,generated_at FROM execution_intents'
            ).fetchall(), [('old-1', 'PENDING', '2026-10-29T13:59:00+00:00')])
