"""One broker execution is one fact, whatever order it arrives under.

The identity of a fill is provider + formal account + execution id. The order,
the reference and the owning instance are attributes of that execution, so
re-submitting the same execution under a new order, a new reference or another
instance must fail closed instead of booking a second fill.

Temporary synthetic databases; no broker call.
"""
import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest import TestCase
from unittest.mock import patch

from trading_infra.attribution import ExecutionAttribution
from trading_infra.instance_accounting import InstanceAccounting
from trading_infra.receipts import ExecutionReceiptImporter

AT = datetime(2026, 10, 31, 14, 0, tzinfo=timezone.utc)
ACCOUNT = 'acct-0000'
OTHER_ACCOUNT = 'acct-1234'


def execution(execution_id='same-exec', *, side='buy', quantity='1', price='40.00',
              fees='0', minutes=-5):
    return {'id': execution_id, 'order_id': 'ignored', 'symbol': 'SPY',
            'side': side, 'quantity': quantity, 'price': price, 'fees': fees,
            'executed_at': (AT + timedelta(minutes=minutes)).isoformat()}


def order(*, order_id='ord-a', ref='ref-a', state='filled', executions=None,
          symbol='SPY'):
    return {'id': order_id, 'ref_id': ref, 'symbol': symbol, 'side': 'buy',
            'state': state, 'quantity': '1', 'cumulative_quantity': '1',
            'executions': executions if executions is not None else [execution()]}


def snapshot(orders, *, account=ACCOUNT, minutes=0):
    return {'account_key': account,
            'observed_at': (AT + timedelta(minutes=minutes)).isoformat(),
            'collection_complete': True,
            'orders': {item['id']: item for item in orders}}


class IdentityTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = self.temp.name + '/identity.sqlite'
        self.db = sqlite3.connect(self.path, isolation_level='')
        self.addCleanup(self.db.close)
        self.attribution = ExecutionAttribution(self.db, clock=lambda: AT)
        self.accounting = InstanceAccounting(self.db, clock=lambda: AT)
        self.attribution.bind('ref-a', 'alpha', 'trade-a')
        self.attribution.bind('ref-b', 'beta', 'trade-b')
        self.importer = ExecutionReceiptImporter(
            self.db, attribution=self.attribution, accounting=self.accounting,
            clock=lambda: AT)

    def test_an_exact_replay_books_one_fill(self):
        first = self.importer.import_snapshot(snapshot([order()]))
        second = self.importer.import_snapshot(snapshot([order()]))
        self.assertEqual(len(first['imported']), 1)
        self.assertEqual(second['imported'], [])
        self.assertEqual([item['recorded'] for item in second['duplicate']], [False])
        self.assertEqual(len(self.accounting.fills('alpha')), 1)
        self.assertEqual(len(self.accounting.fills('beta')), 0)

    def test_the_same_execution_under_another_order_and_owner_is_refused(self):
        self.importer.import_snapshot(snapshot([order()]))
        with self.assertRaises(ValueError) as ctx:
            self.importer.import_snapshot(
                snapshot([order(order_id='ord-b', ref='ref-b')], minutes=1))
        self.assertEqual(str(ctx.exception), 'RECEIPT_EXECUTION_IDENTITY_CONFLICT')
        self.assertEqual(len(self.accounting.fills('alpha')), 1)
        self.assertEqual(self.accounting.fills('beta'), [],
                         'the same execution must never be booked to a second instance')

    def test_the_same_execution_with_changed_economics_is_refused(self):
        self.importer.import_snapshot(snapshot([order()]))
        changed = order(executions=[execution(price='41.00')])
        with self.assertRaises(ValueError) as ctx:
            self.importer.import_snapshot(snapshot([changed], minutes=1))
        self.assertEqual(str(ctx.exception), 'RECEIPT_EXECUTION_IDENTITY_CONFLICT')

    def test_the_same_execution_with_changed_time_is_refused(self):
        self.importer.import_snapshot(snapshot([order()]))
        moved = order(executions=[execution(minutes=-9)])
        with self.assertRaises(ValueError) as ctx:
            self.importer.import_snapshot(snapshot([moved], minutes=1))
        self.assertEqual(str(ctx.exception), 'RECEIPT_EXECUTION_IDENTITY_CONFLICT')

    def test_the_same_execution_id_in_another_account_does_not_conflict(self):
        self.importer.import_snapshot(snapshot([order()]))
        self.attribution.bind('ref-c', 'alpha', 'trade-c')
        other = order(order_id='ord-c', ref='ref-c')
        outcome = self.importer.import_snapshot(
            snapshot([other], account=OTHER_ACCOUNT, minutes=1))
        self.assertEqual(len(outcome['imported']), 1,
                         'execution ids are only unique inside one account')
        self.assertEqual(len(self.accounting.fills('trade-c' if False else 'alpha')), 2)

    def test_an_unsupported_provider_is_refused(self):
        with self.assertRaises(ValueError) as ctx:
            ExecutionReceiptImporter(self.db, attribution=self.attribution,
                                     accounting=self.accounting, provider='alpaca')
        self.assertEqual(str(ctx.exception), 'RECEIPT_PROVIDER_UNSUPPORTED')

    def test_a_failure_mid_booking_rolls_back_identity_and_fill(self):
        with patch.object(self.attribution, 'record',
                          side_effect=sqlite3.OperationalError('injected')):
            with self.assertRaises(sqlite3.OperationalError):
                self.importer.import_snapshot(snapshot([order()]))
        self.assertEqual(self.accounting.fills('alpha'), [])
        self.assertEqual(self.db.execute(
            'SELECT count(*) FROM execution_identity').fetchone()[0], 0)
        self.assertFalse(self.db.in_transaction)
        # The replay then books exactly once.
        self.importer.import_snapshot(snapshot([order()]))
        self.assertEqual(len(self.accounting.fills('alpha')), 1)


class DeferredIdentityTests(TestCase):
    """A deferred execution keeps the same identity when it is retried."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = sqlite3.connect(self.temp.name + '/deferred.sqlite',
                                  isolation_level='')
        self.addCleanup(self.db.close)
        self.attribution = ExecutionAttribution(self.db, clock=lambda: AT)
        self.accounting = InstanceAccounting(self.db, clock=lambda: AT)
        self.attribution.bind('ref-a', 'alpha', 'trade-a')
        self.attribution.bind('ref-b', 'beta', 'trade-b')
        self.importer = ExecutionReceiptImporter(
            self.db, attribution=self.attribution, accounting=self.accounting,
            clock=lambda: AT)

    def sell(self, *, order_id='ord-sell', ref='ref-a', execution_id='E2'):
        return {'id': order_id, 'ref_id': ref, 'symbol': 'SPY', 'side': 'sell',
                'state': 'filled', 'quantity': '1', 'cumulative_quantity': '1',
                'executions': [{'id': execution_id, 'order_id': order_id,
                                'symbol': 'SPY', 'side': 'sell', 'quantity': '1',
                                'price': '41.00', 'fees': '0',
                                'executed_at': (AT - timedelta(minutes=5)).isoformat()}]}

    def buy(self, *, order_id='ord-buy', ref='ref-a', execution_id='E1'):
        return {'id': order_id, 'ref_id': ref, 'symbol': 'SPY', 'side': 'buy',
                'state': 'filled', 'quantity': '1', 'cumulative_quantity': '1',
                'executions': [{'id': execution_id, 'order_id': order_id,
                                'symbol': 'SPY', 'side': 'buy', 'quantity': '1',
                                'price': '40.00', 'fees': '0',
                                'executed_at': (AT - timedelta(minutes=10)).isoformat()}]}

    def test_a_deferred_sale_retried_under_another_order_conflicts(self):
        first = self.importer.import_snapshot(snapshot([self.sell()]))
        self.assertEqual(len(first['deferred']), 1)
        # The basis arrives, but the same sale now claims a different order.
        # The deferred identity already bound it to ord-sell, so this is a
        # conflict rather than a second booking or a silent re-binding.
        with self.assertRaises(ValueError) as ctx:
            self.importer.import_snapshot(
                snapshot([self.buy(), self.sell(order_id='ord-sell-2')], minutes=1))
        self.assertEqual(str(ctx.exception), 'RECEIPT_EXECUTION_IDENTITY_CONFLICT')
        # The unrelated buy is a real fact and stands; only the disputed
        # execution is refused, and it is not booked under any order.
        self.assertEqual([fill['side'] for fill in self.accounting.fills('alpha')],
                         ['buy'])
        self.assertEqual(self.db.execute(
            'SELECT count(*) FROM execution_identity WHERE execution_id=?',
            ('E2',)).fetchone()[0], 0)

    def test_the_same_execution_in_another_instance_does_not_double_book(self):
        self.importer.import_snapshot(snapshot([self.sell()]))
        self.importer.import_snapshot(snapshot([self.buy()], minutes=1))
        before = len(self.accounting.fills('alpha'))
        with self.assertRaises(ValueError):
            self.importer.import_snapshot(
                snapshot([self.sell(order_id='ord-sell-9', ref='ref-b')], minutes=2))
        self.assertEqual(len(self.accounting.fills('alpha')), before)
        self.assertEqual(self.accounting.fills('beta'), [])


class LegacyMigrationTests(TestCase):
    """The old order-scoped keys are translated, never deleted or invented."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = sqlite3.connect(self.temp.name + '/legacy.sqlite',
                                  isolation_level='')
        self.addCleanup(self.db.close)
        self.attribution = ExecutionAttribution(self.db, clock=lambda: AT)
        self.accounting = InstanceAccounting(self.db, clock=lambda: AT)
        self.attribution.bind('ref-a', 'alpha', 'trade-a')

    def seed_legacy(self, rows, *, with_fills=True):
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS execution_fill_identity(
                execution_key TEXT PRIMARY KEY, instance_id TEXT NOT NULL,
                trade_id TEXT NOT NULL, fill_id TEXT NOT NULL,
                payload_digest TEXT NOT NULL, recorded_at TEXT NOT NULL);
        ''')
        self.accounting  # the legacy fill it refers to lives in this ledger
        for key, instance_id, trade_id, fill_id, payload in rows:
            self.db.execute(
                'INSERT INTO execution_fill_identity VALUES(?,?,?,?,?,?)',
                (key, instance_id, trade_id, fill_id, payload, AT.isoformat()))
            if with_fills:
                # The authoritative occurrence time is the accounting row's.
                self.db.execute(
                    'INSERT INTO instance_fills(instance_id,trade_id,fill_id,side,'
                    ' quantity,price,multiplier,fees,cash_delta,occurred_at,'
                    ' recorded_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                    (instance_id, trade_id, fill_id, 'buy', '1', '40.00', 1, '0',
                     '-40.00', (AT - timedelta(minutes=5)).isoformat(),
                     AT.isoformat()))
        self.db.commit()

    def importer(self):
        return ExecutionReceiptImporter(self.db, attribution=self.attribution,
                                        accounting=self.accounting, clock=lambda: AT)

    def test_an_order_scoped_key_becomes_a_stable_identity(self):
        self.seed_legacy([(ACCOUNT + '|ord-a|E1', 'alpha', 'trade-a', 'E1', 'digest-1')])
        importer = self.importer()
        rows = self.db.execute(
            'SELECT identity_key,order_id FROM execution_identity').fetchall()
        self.assertEqual(rows, [('robinhood|' + ACCOUNT + '|E1', 'ord-a')])
        self.assertEqual(importer.identity_conflicts(), [])
        # The legacy row is preserved, not deleted.
        self.assertEqual(self.db.execute(
            'SELECT count(*) FROM execution_fill_identity').fetchone()[0], 1)

    def test_repeat_initialisation_is_a_no_op(self):
        self.seed_legacy([(ACCOUNT + '|ord-a|E1', 'alpha', 'trade-a', 'E1', 'digest-1')])
        self.importer()
        self.importer()
        self.assertEqual(self.db.execute(
            'SELECT count(*) FROM execution_identity').fetchone()[0], 1)

    def test_a_legacy_row_with_no_accounting_history_is_quarantined(self):
        self.seed_legacy([(ACCOUNT + '|ord-a|E1', 'alpha', 'trade-a', 'E1',
                           'digest-1')], with_fills=False)
        importer = self.importer()
        self.assertEqual(self.db.execute(
            'SELECT count(*) FROM execution_identity').fetchone()[0], 0)
        conflicts = importer.identity_conflicts()
        self.assertEqual(conflicts[0]['reason'], 'RECEIPT_IDENTITY_HISTORY_AMBIGUOUS')

    def test_two_legacy_rows_for_one_execution_quarantine(self):
        # The first row has real accounting history; the second claims the same
        # execution for a different owner, so it cannot be normalised safely.
        self.seed_legacy([
            (ACCOUNT + '|ord-a|E1', 'alpha', 'trade-a', 'E1', 'digest-1')])
        self.db.execute(
            'INSERT INTO execution_fill_identity VALUES(?,?,?,?,?,?)',
            (ACCOUNT + '|ord-b|E1', 'beta', 'trade-b', 'E1', 'digest-2',
             AT.isoformat()))
        self.db.commit()
        importer = self.importer()
        self.assertEqual(self.db.execute(
            'SELECT count(*) FROM execution_identity').fetchone()[0], 1)
        conflicts = importer.identity_conflicts()
        self.assertEqual(len(conflicts), 1)
        self.assertEqual(conflicts[0]['reason'], 'RECEIPT_IDENTITY_CONFLICT')


class MigrationGateTests(TestCase):
    """A quarantined legacy identity blocks entries and settlement."""

    def test_identity_quarantine_is_counted_by_the_supervisor_gate(self):
        from test_003r1v_evidence import DeferredGateTests
        fixture = DeferredGateTests('runTest')
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        supervisor, attribution, accounting = fixture.supervisor()
        # A migration conflict is already persisted in this database.
        supervisor.receipt_importer.db.execute(
            'INSERT INTO receipt_identity_quarantine(identity_key,provider,account_key,'
            ' execution_id,reason,detail,recorded_at) VALUES(?,?,?,?,?,?,?)',
            ('robinhood|acct|E9', 'robinhood', 'acct', 'E9',
             'RECEIPT_IDENTITY_CONFLICT', 'legacy', AT.isoformat()))
        supervisor.receipt_importer.db.commit()
        errors = []
        outcome = supervisor._import_receipts(fixture.snapshot([]), errors)
        self.assertTrue(supervisor._receipts_incomplete)
        self.assertIn('RECEIPT_IDENTITY_CONFLICT',
                      [item.get('reason') for item in errors])
        self.assertEqual(supervisor._settle_flat_trades()['blocked'],
                         'RECEIPT_IMPORT_INCOMPLETE')


class DurableConflictTests(IdentityTests):
    """A live conflict must leave a durable, idempotent audit trail."""

    def conflict(self, *, order_id='ord-b', ref='ref-b', account=ACCOUNT,
                 minutes=1, price='40.00'):
        return snapshot([order(order_id=order_id, ref=ref,
                               executions=[execution(price=price)])],
                        account=account, minutes=minutes)

    def test_a_cross_owner_conflict_is_persisted_and_blocks_the_second_instance(self):
        self.importer.import_snapshot(snapshot([order()]))
        with self.assertRaises(ValueError):
            self.importer.import_snapshot(self.conflict())
        self.assertEqual(len(self.accounting.fills('alpha')), 1)
        self.assertEqual(self.accounting.fills('beta'), [])
        conflicts = self.importer.identity_conflicts()
        live = [item for item in conflicts if item['source'] == 'live']
        self.assertEqual(len(live), 1)
        self.assertEqual(live[0]['execution_id'], 'same-exec')
        self.assertEqual(live[0]['accepted']['order_id'], 'ord-a')
        self.assertEqual(live[0]['accepted']['instance_id'], 'alpha')
        self.assertEqual(live[0]['rejected']['order_id'], 'ord-b')
        self.assertEqual(live[0]['rejected']['instance_id'], 'beta')

    def test_an_exact_duplicate_conflict_is_idempotent(self):
        self.importer.import_snapshot(snapshot([order()]))
        for _ in range(3):
            with self.assertRaises(ValueError):
                self.importer.import_snapshot(self.conflict())
        live = [item for item in self.importer.identity_conflicts()
                if item['source'] == 'live']
        self.assertEqual(len(live), 1)

    def test_different_rejected_claims_are_each_auditable(self):
        self.importer.import_snapshot(snapshot([order()]))
        with self.assertRaises(ValueError):
            self.importer.import_snapshot(self.conflict())
        with self.assertRaises(ValueError):
            self.importer.import_snapshot(self.conflict(order_id='ord-c', minutes=2))
        with self.assertRaises(ValueError):
            self.importer.import_snapshot(self.conflict(price='41.00', minutes=3))
        live = [item for item in self.importer.identity_conflicts()
                if item['source'] == 'live']
        self.assertEqual(len(live), 3)

    def test_a_clean_replay_creates_no_conflict(self):
        self.importer.import_snapshot(snapshot([order()]))
        self.importer.import_snapshot(snapshot([order()], minutes=1))
        self.assertEqual([item for item in self.importer.identity_conflicts()
                          if item['source'] == 'live'], [])

    def test_a_restarted_importer_still_sees_the_conflict(self):
        self.importer.import_snapshot(snapshot([order()]))
        with self.assertRaises(ValueError):
            self.importer.import_snapshot(self.conflict())
        reopened = sqlite3.connect(self.path, isolation_level='')
        self.addCleanup(reopened.close)
        restarted = ExecutionReceiptImporter(
            reopened, attribution=ExecutionAttribution(reopened, clock=lambda: AT),
            accounting=InstanceAccounting(reopened, clock=lambda: AT), clock=lambda: AT)
        live = [item for item in restarted.identity_conflicts()
                if item['source'] == 'live']
        self.assertEqual(len(live), 1)
        self.assertEqual(live[0]['rejected']['instance_id'], 'beta')

    def test_a_storage_failure_while_recording_the_conflict_is_not_swallowed(self):
        self.importer.import_snapshot(snapshot([order()]))
        with patch.object(self.importer, '_record_identity_conflict',
                          side_effect=sqlite3.OperationalError('injected audit failure')):
            with self.assertRaises(sqlite3.OperationalError):
                self.importer.import_snapshot(self.conflict())
        # Neither the second fill nor a conflict row was fabricated.
        self.assertEqual(self.accounting.fills('beta'), [])
        self.assertFalse(self.db.in_transaction)


class SupervisorConflictGateTests(TestCase):
    """The default supervisor keeps the gate closed across restarts."""

    def setUp(self):
        from test_003r1v_evidence import DeferredGateTests
        self.fixture = DeferredGateTests('runTest')
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)

    def test_a_live_conflict_blocks_entries_and_settlement_after_restart(self):
        supervisor, attribution, accounting = self.fixture.supervisor()
        attribution.bind('ref-t', 'alpha', 'trade-a')
        attribution.bind('ref-u', 'beta', 'trade-b')
        buy = self.fixture.execution('E1', 'buy', '1', '40', -10)
        supervisor._import_receipts(self.fixture.snapshot([self.fixture.order('buy', buy)]), [])
        self.assertEqual(len(accounting.fills('alpha')), 1)

        rebadged = dict(self.fixture.order('buy', self.fixture.execution('E1', 'buy', '1', '40', -10)),
                        id='ord-other', ref_id='ref-u')
        errors = []
        supervisor._import_receipts(self.fixture.snapshot([rebadged]), errors)
        self.assertTrue({'RECEIPT_EXECUTION_IDENTITY_CONFLICT',
                         'RECEIPT_IDENTITY_CONFLICT'}
                        & {item.get('reason') for item in errors})
        self.assertTrue(supervisor._receipts_incomplete)
        self.assertEqual(supervisor._settle_flat_trades()['blocked'],
                         'RECEIPT_IMPORT_INCOMPLETE')

        # Restart: a new supervisor over the same database keeps the gate shut
        # because the conflict is a durable row, not an in-memory flag.
        restarted, attribution2, accounting2 = self.fixture.supervisor(fresh_runtime=True)
        attribution2.bind('ref-t', 'alpha', 'trade-a')
        attribution2.bind('ref-u', 'beta', 'trade-b')
        errors = []
        restarted._import_receipts(self.fixture.snapshot([]), errors)
        self.assertIn('RECEIPT_IDENTITY_CONFLICT',
                      [item.get('reason') for item in errors])
        self.assertTrue(restarted._receipts_incomplete)
        self.assertEqual(restarted._settle_flat_trades()['blocked'],
                         'RECEIPT_IMPORT_INCOMPLETE')
        self.assertEqual(accounting2.fills('beta'), [])
