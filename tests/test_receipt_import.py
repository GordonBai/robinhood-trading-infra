"""Broker receipts become per-instance feedback and accounting, once each.

Routing is by reference only. A fill's identity is account + order + execution
id, so a replay, a partial and a late arrival are the same fact seen again. The
broker's execution timestamp stays the occurrence time; the observation time is
recorded separately.

Temporary synthetic database, no broker call.
"""
import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest import TestCase

from trading_infra.attribution import ExecutionAttribution
from trading_infra.instance_accounting import InstanceAccounting
from trading_infra.receipts import ExecutionReceiptImporter

AT = datetime(2026, 10, 30, 14, 0, tzinfo=timezone.utc)
ACCOUNT = 'acct-0000'


def order(*, ref='ref-a', order_id='ord-1', state='filled', executions=None,
          symbol='SPY'):
    return {'id': order_id, 'ref_id': ref, 'symbol': symbol, 'side': 'buy',
            'state': state, 'quantity': '2', 'cumulative_quantity': '1',
            'executions': executions if executions is not None else
            [execution()]}


def execution(execution_id='exec-1', *, quantity='1', price='40.00', fees='0.00',
              at=None):
    return {'id': execution_id, 'order_id': 'ord-1', 'symbol': 'SPY', 'side': 'buy',
            'quantity': quantity, 'price': price, 'fees': fees,
            'executed_at': (at or (AT - timedelta(seconds=5))).isoformat()}


def snapshot(orders, *, at=None, account=ACCOUNT, complete=True):
    return {'account_key': account, 'observed_at': (at or AT).isoformat(),
            'collection_complete': complete,
            'orders': {item['id']: item for item in orders}}


class ReceiptTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = sqlite3.connect(self.temp.name + '/receipts.sqlite',
                                  isolation_level='')
        self.addCleanup(self.db.close)
        self.attribution = ExecutionAttribution(self.db, clock=lambda: AT)
        self.accounting = InstanceAccounting(self.db, clock=lambda: AT)
        self.attribution.bind('ref-a', 'alpha', 'trade-a')
        self.importer = ExecutionReceiptImporter(
            self.db, attribution=self.attribution, accounting=self.accounting,
            clock=lambda: AT)

    def test_a_fill_reaches_its_owning_instance(self):
        outcome = self.importer.import_snapshot(snapshot([order()]))
        self.assertEqual(len(outcome['imported']), 1)
        fills = self.accounting.fills('alpha', trade_id='trade-a')
        self.assertEqual(len(fills), 1)
        self.assertEqual(Decimal(fills[0]['cash_delta']), Decimal('-40.00'))
        self.assertEqual(self.accounting.fills('beta'), [])
        events = self.attribution.events('alpha')
        self.assertTrue(any(item['kind'] == 'filled' for item in events))

    def test_a_replay_of_the_same_snapshot_adds_nothing(self):
        self.importer.import_snapshot(snapshot([order()]))
        again = self.importer.import_snapshot(snapshot([order()]))
        self.assertEqual(len(self.accounting.fills('alpha')), 1)
        self.assertEqual([item['recorded'] for item in again['duplicate']], [False])

    def test_a_late_execution_of_the_same_order_is_one_fill(self):
        self.importer.import_snapshot(snapshot([order()]))
        later = snapshot([order(executions=[execution('exec-2', quantity='1')])],
                         at=AT + timedelta(seconds=30))
        self.importer.import_snapshot(later)
        # exec-1 was not repeated by the broker in this snapshot, so only the
        # new execution is booked; the first fill is not duplicated.
        self.assertEqual(len(self.accounting.fills('alpha')), 2)
        keys = {row[0] for row in self.db.execute(
            'SELECT execution_key FROM execution_fill_identity')}
        self.assertEqual(len(keys), 2)

    def test_the_broker_timestamp_is_the_occurrence_time(self):
        occurred = AT - timedelta(minutes=3)
        self.importer.import_snapshot(
            snapshot([order(executions=[execution(at=occurred)])]))
        fill = self.accounting.fills('alpha')[0]
        self.assertEqual(datetime.fromisoformat(fill['occurred_at']), occurred)

    def test_an_order_without_a_known_reference_is_quarantined_not_guessed(self):
        unknown = order(ref=None, order_id='ord-x', symbol='SPY')
        unknown.pop('ref_id')
        outcome = self.importer.import_snapshot(snapshot([unknown]))
        self.assertEqual(len(outcome['quarantined']), 1)
        self.assertEqual(outcome['quarantined'][0]['reason'], 'RECEIPT_OWNER_UNKNOWN')
        self.assertEqual(self.accounting.fills('alpha'), [])

    def test_a_known_alias_routes_without_a_reference_on_the_order(self):
        self.importer.alias('ord-2', 'ref-a', ACCOUNT)
        legacy = order(ref=None, order_id='ord-2')
        legacy.pop('ref_id')
        outcome = self.importer.import_snapshot(snapshot([legacy]))
        self.assertEqual(len(outcome['imported']), 1)
        self.assertEqual(len(self.accounting.fills('alpha')), 1)

    def test_an_unbound_reference_is_quarantined(self):
        self.attribution  # ref-a is bound; use a different one
        outcome = self.importer.import_snapshot(snapshot([order(ref='ref-unknown')]))
        self.assertEqual(outcome['quarantined'][0]['reason'], 'RECEIPT_REFERENCE_UNBOUND')

    def test_a_cancelled_order_reports_a_terminal_cancel(self):
        self.importer.import_snapshot(snapshot([order(state='cancelled',
                                                      executions=[])]))
        kinds = [item['kind'] for item in self.attribution.events('alpha')]
        self.assertIn('cancel_terminal', kinds)

    def test_a_pending_cancel_is_not_a_terminal_state(self):
        self.importer.import_snapshot(snapshot([order(state='pending_cancel',
                                                      executions=[])]))
        kinds = [item['kind'] for item in self.attribution.events('alpha')]
        self.assertNotIn('cancel_terminal', kinds)

    def test_a_partial_order_reports_a_partial_fill(self):
        self.importer.import_snapshot(snapshot([order(state='partially_filled')]))
        kinds = [item['kind'] for item in self.attribution.events('alpha')]
        self.assertIn('partial_fill', kinds)

    def test_an_incomplete_snapshot_is_refused(self):
        with self.assertRaises(ValueError) as ctx:
            self.importer.import_snapshot(snapshot([order()], complete=False))
        self.assertEqual(str(ctx.exception), 'RECEIPT_SNAPSHOT_INCOMPLETE')
        self.assertEqual(self.accounting.fills('alpha'), [])

    def test_a_missing_observation_time_is_refused(self):
        broken = snapshot([order()])
        broken.pop('observed_at')
        with self.assertRaises(ValueError) as ctx:
            self.importer.import_snapshot(broken)
        self.assertEqual(str(ctx.exception), 'RECEIPT_OBSERVED_AT_REQUIRED')

    def test_an_execution_after_the_observation_is_refused(self):
        future = execution(at=AT + timedelta(seconds=10))
        with self.assertRaises(ValueError) as ctx:
            self.importer.import_snapshot(snapshot([order(executions=[future])]))
        self.assertEqual(str(ctx.exception), 'RECEIPT_EXECUTION_IN_FUTURE')
        self.assertEqual(self.accounting.fills('alpha'), [])

    def test_the_default_implicit_transaction_connection_is_used(self):
        self.assertNotEqual(self.db.isolation_level, None)
        self.importer.import_snapshot(snapshot([order()]))
        self.assertFalse(self.db.in_transaction,
                         'the importer must not leave a transaction open')
        # The write is durable through a second reader.
        other = sqlite3.connect(self.temp.name + '/receipts.sqlite')
        self.addCleanup(other.close)
        self.assertEqual(other.execute(
            'SELECT count(*) FROM instance_fills').fetchone()[0], 1)


class FlatEvidenceTests(TestCase):
    """Release facts must come from an accepted, account-bound snapshot."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        import json as _json
        from pathlib import Path as _Path
        from entry_authority_fixture import TestExecutionBook as ExecutionBook
        policy = _json.loads(_Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        self.book = ExecutionBook(_Path(self.temp.name) / 'book.sqlite', policy,
                                  simulation=True, clock=lambda: AT)
        self.addCleanup(self.book.close)

    def snapshot(self, orders=None):
        from test_broker import FakeBroker, snapshot as broker_snapshot
        fake = FakeBroker()
        fake.orders = orders or []
        value = broker_snapshot(fake, AT.isoformat())
        value['synthetic'] = True
        return value

    def test_no_accepted_snapshot_is_refused_rather_than_read_as_empty(self):
        # A fresh book has never reconciled; it cannot claim to be flat.
        self.book._begin()
        try:
            self.book._save({**self.book.state(), 'trades': {
                't1': {'phase': 'FLAT_VERIFIED', 'quantity': '0', 'plan': {}}}})
            self.book.db.commit()
        finally:
            pass
        with self.assertRaises(ValueError) as ctx:
            self.book.flat_evidence('t1')
        self.assertEqual(str(ctx.exception), 'FLAT_EVIDENCE_SNAPSHOT_REQUIRED')

    def test_an_accepted_snapshot_supplies_order_facts_and_time(self):
        self.book.reconcile(self.snapshot())
        state = self.book.state()
        self.assertIn('broker_orders', state)
        self.assertTrue(state.get('last_snapshot'))


class ReplayShapeTests(TestCase):
    """The coordinator's exact replay shapes must be stable and permanent."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = sqlite3.connect(self.temp.name + '/replay.sqlite', isolation_level='')
        self.addCleanup(self.db.close)
        self.attribution = ExecutionAttribution(self.db, clock=lambda: AT)
        self.accounting = InstanceAccounting(self.db, clock=lambda: AT)
        self.attribution.bind('ref-a', 'alpha', 'trade-a')
        self.importer = ExecutionReceiptImporter(
            self.db, attribution=self.attribution, accounting=self.accounting,
            clock=lambda: AT)

    def test_partial_then_full_replay_of_the_same_execution_is_stable(self):
        partial = order(state='partially_filled',
                        executions=[execution('E1', quantity='1')])
        self.importer.import_snapshot(snapshot([partial]))
        full = order(state='filled',
                     executions=[execution('E1', quantity='1'),
                                 execution('E2', quantity='1')])
        second = self.importer.import_snapshot(
            snapshot([full], at=AT + timedelta(seconds=20)))
        # E1 is the same fill; only E2 is new cash.
        self.assertEqual(len(second['imported']), 1)
        self.assertEqual(len(self.accounting.fills('alpha')), 2)
        kinds = [item['kind'] for item in self.attribution.events('alpha')]
        self.assertEqual(kinds.count('partial_fill'), 1)
        self.assertIn('filled', kinds)

    def test_a_cancelled_order_replayed_on_a_new_observation_does_not_conflict(self):
        cancelled = order(state='cancelled', executions=[])
        self.importer.import_snapshot(snapshot([cancelled]))
        again = self.importer.import_snapshot(
            snapshot([cancelled], at=AT + timedelta(seconds=40)))
        self.assertEqual(len(again['orders']), 1)
        terminals = [item for item in self.attribution.events('alpha')
                     if item['kind'] == 'cancel_terminal']
        self.assertEqual(len(terminals), 1)

    def test_order_progress_does_not_regress_on_a_late_snapshot(self):
        self.importer.import_snapshot(snapshot([order(state='filled')],
                                               at=AT + timedelta(seconds=30)))
        late = self.importer.import_snapshot(
            snapshot([order(state='partially_filled')], at=AT + timedelta(seconds=10)))
        self.assertEqual(len(late['orders']), 1)
        row = self.db.execute(
            "SELECT state,terminal FROM receipt_order_progress WHERE order_id='ord-1'"
        ).fetchone()
        self.assertEqual(row[0], 'filled', 'a late snapshot must not roll progress back')
        self.assertEqual(row[1], 1)


class ProviderShapeTests(TestCase):
    """The real collect_broker shape must be used, not a convenient fiction."""

    def setUp(self):
        import json as _json
        from pathlib import Path as _Path
        from entry_authority_fixture import TestExecutionBook as ExecutionBook
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.policy = _json.loads(_Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        self.book = ExecutionBook(_Path(self.temp.name) / 'shape.sqlite', self.policy,
                                  simulation=True, clock=lambda: AT)
        self.addCleanup(self.book.close)

    def provider_snapshot(self, *, approvals=0, orders=None, positions=None):
        from test_broker import FakeBroker, snapshot as broker_snapshot
        fake = FakeBroker()
        fake.orders = orders or []
        fake.positions = positions or []
        if approvals:
            from test_broker import approval as broker_approval
            fake.approvals = [broker_approval('appr-1')]
        value = broker_snapshot(fake, AT.isoformat())
        value['synthetic'] = True
        return value

    def test_a_pending_approval_does_not_break_reconciliation(self):
        value = self.provider_snapshot(approvals=1,
                                       positions=[{'symbol': 'DEMO', 'quantity': '2',
                                                   'shares_available_for_sells': '2',
                                                   'shares_held_for_sells': '0',
                                                   'average_buy_price': '40'}])
        self.assertIsInstance(value['pending_approvals'], int)
        self.assertIsInstance(value['pending_approval_ids'], list)
        self.assertIsInstance(value['approval_states'], dict)
        self.book.reconcile(value)          # used to raise TypeError here
        state = self.book.state()
        approvals = state['broker_approvals']
        self.assertEqual(approvals['count'], len(approvals['ids']))
        self.assertEqual(approvals['missing'], [])
        self.assertEqual(approvals['ids'], ['appr-1'])

    def test_inconsistent_approval_facts_are_refused_by_release(self):
        value = self.provider_snapshot(approvals=0)
        self.book.reconcile(value)
        self.book._begin()
        try:
            state = self.book.state()
            state['broker_approvals'] = {'count': 2, 'ids': ['a'],
                                         'states': {'a': 'PENDING'}, 'missing': []}
            state['trades'] = {'t1': {'phase': 'FLAT_VERIFIED', 'quantity': '0',
                                      'plan': {'symbol': 'DEMO'}}}
            self.book._save(state)
            self.book.db.commit()
        finally:
            pass
        with self.assertRaises(ValueError) as ctx:
            self.book.flat_evidence('t1')
        self.assertEqual(str(ctx.exception), 'FLAT_EVIDENCE_APPROVALS_INCONSISTENT')


class IdentityChainTests(TestCase):
    """A broker-rewritten reference is matched by its exact order id."""

    def setUp(self):
        import json as _json
        from pathlib import Path as _Path
        from entry_authority_fixture import TestExecutionBook as ExecutionBook
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.policy = _json.loads(_Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        # The trial policy is dated; use its session so entry gates pass.
        self.at = datetime.fromisoformat('2026-10-16T14:00:00+00:00')
        self.book = ExecutionBook(_Path(self.temp.name) / 'identity.sqlite', self.policy,
                                  simulation=True, clock=lambda: self.at)
        self.addCleanup(self.book.close)
        self.plan = {'symbol': 'DEMO', 'quantity': 2, 'entry_ceiling': '40',
                     'stop': '39', 'cost_buffer_per_share': '0.05'}
        self.quote = {'symbol': 'DEMO', 'bid': '39.99', 'ask': '40',
                      'bid_at': self.at.isoformat(), 'ask_at': self.at.isoformat(),
                      'observed_at': self.at.isoformat()}

    def snapshot(self, orders=None):
        from test_broker import FakeBroker, snapshot as broker_snapshot
        fake = FakeBroker()
        fake.orders = orders or []
        value = broker_snapshot(fake, self.at.isoformat())
        value['synthetic'] = True
        return value

    def test_an_open_order_matched_only_by_exact_id_is_counted(self):
        # A submitted action whose broker reference was rewritten: only the
        # exact broker order id ties the live order back to this trade.
        self.book.propose('t1', self.plan, self.quote, self.snapshot(),
                          evidence={'source_sha256': 'synthetic-fixture',
                                    'synthetic': True})
        entry = next(a for a in self.book.actions() if a['kind'] == 'entry')
        account = self.book.state()['account_key']
        self.book.db.execute(
            "INSERT INTO external_broker_refs(ref_id,action_id,operation_id)"
            " VALUES('rewritten-approval-alias',?,'op-1')", (entry['id'],))
        self.book.db.execute(
            "INSERT INTO external_broker_order_ids(order_id,action_id,operation_id,"
            " account_key) VALUES('broker-ord-9',?,'op-1',?)", (entry['id'], account))
        self.book.db.commit()
        self.book._begin()
        try:
            state = self.book.state()
            state['last_snapshot'] = self.at.isoformat()
            state['broker_approvals'] = {'count': 0, 'ids': [], 'states': {},
                                         'missing': []}
            state['broker_orders'] = {'broker-ord-9': {
                'id': 'broker-ord-9', 'ref_id': 'rewritten-approval-alias',
                'state': 'confirmed', 'remaining': '2',
                'external_broker_order_id': None}}
            self.book._save(state)
            self.book.db.commit()
        finally:
            pass
        evidence = self.book.flat_evidence('t1')
        self.assertEqual(evidence['open_orders'], 1,
                         'a live order must be counted even when only its exact id matches')
        self.assertEqual(evidence['pending_approvals'], 0)


class ReverseOrderTests(TestCase):
    """A snapshot may list the exit order before the entry order."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = sqlite3.connect(self.temp.name + '/reverse.sqlite', isolation_level='')
        self.addCleanup(self.db.close)
        self.attribution = ExecutionAttribution(self.db, clock=lambda: AT)
        self.accounting = InstanceAccounting(self.db, clock=lambda: AT)
        self.attribution.bind('ref-entry', 'alpha', 'trade-a')
        self.attribution.bind('ref-exit', 'alpha', 'trade-a')
        self.importer = ExecutionReceiptImporter(
            self.db, attribution=self.attribution, accounting=self.accounting,
            clock=lambda: AT)

    def buy(self, execution_id='E1', *, quantity='2', at=None):
        return {'id': execution_id, 'order_id': 'ord-buy', 'symbol': 'SPY',
                'side': 'buy', 'quantity': quantity, 'price': '40.00',
                'fees': '0.00',
                'executed_at': (at or (AT - timedelta(minutes=10))).isoformat()}

    def sell(self, execution_id='E2', *, quantity='2', at=None):
        return {'id': execution_id, 'order_id': 'ord-sell', 'symbol': 'SPY',
                'side': 'sell', 'quantity': quantity, 'price': '41.00',
                'fees': '0.00',
                'executed_at': (at or (AT - timedelta(minutes=5))).isoformat()}

    def entry_order(self, executions):
        return {'id': 'ord-buy', 'ref_id': 'ref-entry', 'symbol': 'SPY',
                'side': 'buy', 'state': 'filled', 'quantity': '2',
                'cumulative_quantity': '2', 'executions': executions}

    def exit_order(self, executions):
        return {'id': 'ord-sell', 'ref_id': 'ref-exit', 'symbol': 'SPY',
                'side': 'sell', 'state': 'filled', 'quantity': '2',
                'cumulative_quantity': '2', 'executions': executions}

    def snapshot(self, orders, *, at=None):
        return {'account_key': ACCOUNT, 'observed_at': (at or AT).isoformat(),
                'collection_complete': True,
                'orders': {item['id']: item for item in orders}}

    def test_an_exit_listed_first_is_still_booked_after_its_entry(self):
        # orders arrive exit-then-entry, executions are buy-before-sell.
        value = self.snapshot([self.exit_order([self.sell()]),
                               self.entry_order([self.buy()])])
        outcome = self.importer.import_snapshot(value)
        self.assertEqual(outcome['deferred'], [])
        fills = self.accounting.fills('alpha', trade_id='trade-a')
        self.assertEqual([fill['side'] for fill in fills], ['buy', 'sell'])
        summary = self.accounting.trade_summary('alpha', 'trade-a')
        self.assertEqual(Decimal(summary['realised_pnl']), Decimal('2.00'))

    def test_a_sell_before_its_buy_is_deferred_then_recovered(self):
        first = self.snapshot([self.exit_order([self.sell()])],
                              at=AT - timedelta(minutes=4))
        outcome = self.importer.import_snapshot(first)
        self.assertEqual(len(outcome['deferred']), 1)
        self.assertEqual(outcome['deferred'][0]['reason'], 'RECEIPT_AWAITING_BASIS')
        self.assertEqual(self.accounting.fills('alpha'), [],
                         'a deferred sell is not booked and not marked imported')
        pending = self.db.execute(
            'SELECT execution_id FROM receipt_pending_executions').fetchall()
        self.assertEqual([row[0] for row in pending], ['E2'])

        second = self.snapshot([self.entry_order([self.buy()]),
                                self.exit_order([self.sell()])])
        recovered = self.importer.import_snapshot(second)
        self.assertEqual(recovered['deferred'], [])
        fills = self.accounting.fills('alpha', trade_id='trade-a')
        self.assertEqual([fill['side'] for fill in fills], ['buy', 'sell'])
        # The deferred execution kept its original venue timestamp.
        self.assertEqual(fills[1]['occurred_at'],
                         (AT - timedelta(minutes=5)).isoformat())
        self.assertEqual(self.db.execute(
            'SELECT count(*) FROM receipt_pending_executions').fetchone()[0], 0)


class BasisTimeTests(TestCase):
    """A sale can only be matched against a lot that existed before it."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = sqlite3.connect(self.temp.name + '/basis.sqlite', isolation_level='')
        self.addCleanup(self.db.close)
        self.attribution = ExecutionAttribution(self.db, clock=lambda: AT)
        self.accounting = InstanceAccounting(self.db, clock=lambda: AT)
        self.attribution.bind('ref-t', 'alpha', 'trade-a')
        self.importer = ExecutionReceiptImporter(
            self.db, attribution=self.attribution, accounting=self.accounting,
            clock=lambda: AT)

    def execution(self, execution_id, side, quantity, price, minutes):
        return {'id': execution_id, 'order_id': 'ord-' + side, 'symbol': 'SPY',
                'side': side, 'quantity': quantity, 'price': price, 'fees': '0',
                'executed_at': (AT + timedelta(minutes=minutes)).isoformat()}

    def order(self, side, execution):
        return {'id': 'ord-' + side, 'ref_id': 'ref-t', 'symbol': 'SPY',
                'side': side, 'state': 'filled', 'quantity': execution['quantity'],
                'cumulative_quantity': execution['quantity'],
                'executions': [execution]}

    def snapshot(self, orders):
        return {'account_key': ACCOUNT, 'observed_at': (AT + timedelta(minutes=30)).isoformat(),
                'collection_complete': True,
                'orders': {item['id']: item for item in orders}}

    def test_a_later_buy_is_not_a_basis_for_an_earlier_sale(self):
        sell = self.execution('E2', 'sell', '2', '41', -5)
        first = self.importer.import_snapshot(self.snapshot([self.order('sell', sell)]))
        self.assertEqual(len(first['deferred']), 1)
        # A buy that happened AFTER the sale arrives next.
        buy_later = self.execution('E3', 'buy', '2', '40', 10)
        second = self.importer.import_snapshot(
            self.snapshot([self.order('buy', buy_later)]))
        fills = self.accounting.fills('alpha')
        self.assertEqual([fill['side'] for fill in fills], ['buy'])
        self.assertEqual(self.importer.pending_execution_count(), 1,
                         'the sale stays pending: its basis must be earlier than it')

    def test_an_earlier_buy_that_arrives_late_still_recovers_the_sale(self):
        sell = self.execution('E2', 'sell', '2', '41', -5)
        self.importer.import_snapshot(self.snapshot([self.order('sell', sell)]))
        buy_earlier = self.execution('E1', 'buy', '2', '40', -10)
        second = self.importer.import_snapshot(
            self.snapshot([self.order('buy', buy_earlier)]))
        self.assertEqual(len(second['imported']), 2, 'buy and the recovered sale')
        fills = self.accounting.fills('alpha')
        self.assertEqual([fill['side'] for fill in fills], ['buy', 'sell'])
        self.assertEqual(self.importer.pending_execution_count(), 0)

    def test_a_pending_execution_with_new_economics_conflicts(self):
        sell = self.execution('E2', 'sell', '2', '41', -5)
        self.importer.import_snapshot(self.snapshot([self.order('sell', sell)]))
        changed = self.execution('E2', 'sell', '3', '41', -5)
        with self.assertRaises(ValueError) as ctx:
            self.importer.import_snapshot(self.snapshot([self.order('sell', changed)]))
        self.assertEqual(str(ctx.exception), 'RECEIPT_PENDING_CONFLICT')
