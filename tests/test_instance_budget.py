"""AccountBudgetAuthority: durable admission shared by strategy instances."""
from datetime import datetime, timezone
from decimal import Decimal
import os
import sqlite3
import tempfile
from unittest import TestCase

from trading_contracts.instance import (InstanceBudget, PluginManifest,
                                        StrategyInstance)
from trading_infra.budget import AccountBudgetAuthority


LIMITS = {'max_open_cash': '10000', 'max_open_notional': '10000',
          'max_open_planned_loss': '10000', 'max_concurrent_trades': 20}


def make_instance(instance_id, *, max_cash='1000', max_planned_loss='1000',
                  max_notional='1000', max_concurrent=10):
    manifest = PluginManifest(
        plugin_id='plugin-' + instance_id, version='1.0.0',
        state_schema='state-1', config={'threshold': '1'},
        data_requirements=('quotes',), capabilities=('entries',), max_inflight=1)
    budget = InstanceBudget(max_cash=max_cash, max_planned_loss=max_planned_loss,
                            max_notional=max_notional,
                            max_concurrent=max_concurrent)
    return StrategyInstance(instance_id=instance_id, manifest=manifest,
                            budget=budget, mode='shadow', enabled=False)


class AccountBudgetAuthorityTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = os.path.join(self.temp.name, 'account-budget.sqlite3')
        self.db = sqlite3.connect(self.path)
        self.addCleanup(self.db.close)
        self.clock = lambda: datetime(2026, 10, 29, 15, tzinfo=timezone.utc)

    def authority(self, **overrides):
        limits = dict(LIMITS)
        limits.update(overrides)
        authority = AccountBudgetAuthority(self.db, limits, clock=self.clock)
        self.addCleanup(authority.close)
        return authority

    def assert_rejected(self, authority, instance, trade_id, *, cash,
                        planned_loss, notional, reason):
        before = authority.status()
        result = authority.reserve(instance, trade_id=trade_id, cash=cash,
                                   planned_loss=planned_loss, notional=notional)
        self.assertEqual(result, {'reserved': False, 'reason': reason,
                                  'instance_id': instance.instance_id,
                                  'trade_id': trade_id})
        self.assertEqual(authority.status(), before)
        return result

    # 1. successful admission is visible in durable status
    def test_reserve_within_both_limits_updates_status(self):
        authority = self.authority()
        alpha = make_instance('alpha')
        result = authority.reserve(alpha, trade_id='trade-1', cash='25',
                                   planned_loss='5', notional='150')
        self.assertEqual(result, {'reserved': True, 'instance_id': 'alpha',
                                  'trade_id': 'trade-1', 'cash': '25',
                                  'planned_loss': '5', 'notional': '150'})
        status = authority.status()
        self.assertEqual(status['open_cash'], '25')
        self.assertEqual(status['open_notional'], '150')
        self.assertEqual(status['open_planned_loss'], '5')
        self.assertEqual(status['open_trades'], 1)
        self.assertEqual(status['max_open_cash'], '10000')
        self.assertEqual(status['max_open_notional'], '10000')
        self.assertEqual(status['max_open_planned_loss'], '10000')
        self.assertEqual(status['max_concurrent_trades'], 20)
        self.assertTrue(status['capacity_available'])
        self.assertEqual(status['per_instance']['alpha'],
                         authority.instance_status('alpha'))
        self.assertEqual(status['per_instance']['alpha']['open_cash'], '25')
        self.assertEqual(status['per_instance']['alpha']['max_cash'], '1000')
        self.assertTrue(status['per_instance']['alpha']['available'])

    # 2. idempotent re-reservation with identical amounts
    def test_identical_reservation_is_idempotent(self):
        authority = self.authority()
        alpha = make_instance('alpha')
        first = authority.reserve(alpha, trade_id='trade-1', cash='25',
                                  planned_loss='5', notional='150')
        second = authority.reserve(alpha, trade_id='trade-1', cash='25.00',
                                   planned_loss='5.0', notional='150.000')
        self.assertEqual(second, first)
        status = authority.status()
        self.assertEqual(status['open_trades'], 1)
        self.assertEqual(status['open_cash'], '25')
        self.assertEqual(status['open_notional'], '150')

    # 3. same identity with changed amounts is a conflict
    def test_changed_amounts_for_same_identity_conflict(self):
        authority = self.authority()
        alpha = make_instance('alpha')
        authority.reserve(alpha, trade_id='trade-1', cash='25',
                          planned_loss='5', notional='150')
        before = authority.status()
        for changed in ({'cash': '26'}, {'planned_loss': '6'},
                        {'notional': '151'}):
            amounts = {'cash': '25', 'planned_loss': '5', 'notional': '150'}
            amounts.update(changed)
            with self.assertRaisesRegex(ValueError,
                                        '^BUDGET_RESERVATION_CONFLICT$'):
                authority.reserve(alpha, trade_id='trade-1', **amounts)
            self.assertEqual(authority.status(), before)

    # 4. each rejection code, one distinct case per code
    def test_instance_cash_limit(self):
        authority = self.authority()
        alpha = make_instance('alpha', max_cash='100')
        self.assert_rejected(authority, alpha, 'trade-1', cash='101',
                             planned_loss='0', notional='0',
                             reason='INSTANCE_CASH_LIMIT')

    def test_instance_notional_limit(self):
        authority = self.authority()
        alpha = make_instance('alpha', max_notional='100')
        self.assert_rejected(authority, alpha, 'trade-1', cash='0',
                             planned_loss='0', notional='101',
                             reason='INSTANCE_NOTIONAL_LIMIT')

    def test_instance_planned_loss_limit(self):
        authority = self.authority()
        alpha = make_instance('alpha', max_planned_loss='10')
        self.assert_rejected(authority, alpha, 'trade-1', cash='0',
                             planned_loss='11', notional='0',
                             reason='INSTANCE_PLANNED_LOSS_LIMIT')

    def test_instance_concurrency_limit(self):
        authority = self.authority()
        alpha = make_instance('alpha', max_concurrent=1)
        self.assertTrue(authority.reserve(
            alpha, trade_id='trade-1', cash='1', planned_loss='0',
            notional='0')['reserved'])
        self.assert_rejected(authority, alpha, 'trade-2', cash='1',
                             planned_loss='0', notional='0',
                             reason='INSTANCE_CONCURRENCY_LIMIT')

    def test_account_concurrency_limit(self):
        authority = self.authority(max_concurrent_trades=1)
        alpha = make_instance('alpha', max_concurrent=10)
        self.assertTrue(authority.reserve(
            alpha, trade_id='trade-1', cash='1', planned_loss='0',
            notional='0')['reserved'])
        self.assert_rejected(authority, alpha, 'trade-2', cash='1',
                             planned_loss='0', notional='0',
                             reason='ACCOUNT_CONCURRENCY_LIMIT')

    def test_account_cash_limit(self):
        authority = self.authority(max_open_cash='100')
        alpha = make_instance('alpha', max_cash='1000')
        self.assertTrue(authority.reserve(
            alpha, trade_id='trade-1', cash='60', planned_loss='0',
            notional='0')['reserved'])
        self.assert_rejected(authority, alpha, 'trade-2', cash='50',
                             planned_loss='0', notional='0',
                             reason='ACCOUNT_CASH_LIMIT')

    def test_account_notional_limit(self):
        authority = self.authority(max_open_notional='100')
        alpha = make_instance('alpha', max_notional='1000')
        self.assertTrue(authority.reserve(
            alpha, trade_id='trade-1', cash='0', planned_loss='0',
            notional='60')['reserved'])
        self.assert_rejected(authority, alpha, 'trade-2', cash='0',
                             planned_loss='0', notional='50',
                             reason='ACCOUNT_NOTIONAL_LIMIT')

    def test_account_planned_loss_limit(self):
        authority = self.authority(max_open_planned_loss='10')
        alpha = make_instance('alpha', max_planned_loss='1000')
        self.assertTrue(authority.reserve(
            alpha, trade_id='trade-1', cash='0', planned_loss='6',
            notional='0')['reserved'])
        self.assert_rejected(authority, alpha, 'trade-2', cash='0',
                             planned_loss='5', notional='0',
                             reason='ACCOUNT_PLANNED_LOSS_LIMIT')

    # 5. per-instance ceilings do not add up to account headroom
    def test_two_instances_cannot_collectively_exceed_account_limit(self):
        authority = self.authority(max_open_cash='100')
        alpha = make_instance('alpha', max_cash='100')
        beta = make_instance('beta', max_cash='100')
        self.assertTrue(authority.reserve(
            alpha, trade_id='alpha-1', cash='60', planned_loss='0',
            notional='0')['reserved'])
        self.assert_rejected(authority, beta, 'beta-1', cash='60',
                             planned_loss='0', notional='0',
                             reason='ACCOUNT_CASH_LIMIT')
        status = authority.status()
        self.assertEqual(status['open_cash'], '60')
        self.assertEqual(status['open_trades'], 1)
        self.assertEqual(status['per_instance']['alpha']['open_cash'], '60')
        self.assertNotIn('beta', status['per_instance'])

    # 6. release is instance-scoped and frees account capacity
    def test_release_frees_capacity_only_for_its_own_instance(self):
        authority = self.authority(max_open_cash='100')
        alpha = make_instance('alpha', max_cash='100')
        beta = make_instance('beta', max_cash='100')
        authority.reserve(alpha, trade_id='alpha-1', cash='60',
                          planned_loss='0', notional='0')
        authority.reserve(beta, trade_id='beta-1', cash='40',
                          planned_loss='0', notional='0')

        # A release scoped to alpha never deletes beta's reservation.
        self.assertFalse(authority.release('alpha', 'beta-1'))
        self.assertEqual(authority.status()['per_instance']['beta']['open_cash'],
                         '40')

        self.assertTrue(authority.release('alpha', 'alpha-1'))
        self.assertFalse(authority.release('alpha', 'alpha-1'))
        status = authority.status()
        self.assertEqual(status['open_trades'], 1)
        self.assertEqual(status['open_cash'], '40')
        self.assertEqual(status['per_instance']['alpha']['open_cash'], '0')
        self.assertEqual(status['per_instance']['beta']['open_cash'], '40')

        # Freed capacity is actually usable again.
        self.assertTrue(authority.reserve(
            alpha, trade_id='alpha-2', cash='50', planned_loss='0',
            notional='0')['reserved'])
        self.assertEqual(authority.status()['open_cash'], '90')

    # 7. a rejection rolls back everything, including instance bookkeeping
    def test_rejection_leaves_status_and_rows_unchanged(self):
        authority = self.authority(max_open_cash='100')
        alpha = make_instance('alpha', max_cash='1000')
        authority.reserve(alpha, trade_id='trade-1', cash='60',
                          planned_loss='1', notional='60')
        before = authority.status()
        reservations = self.db.execute(
            'SELECT count(*) FROM account_budget_reservations').fetchone()[0]
        ceilings = self.db.execute(
            'SELECT count(*) FROM account_budget_instance_limits').fetchone()[0]

        result = authority.reserve(alpha, trade_id='trade-2', cash='50',
                                   planned_loss='1', notional='60')
        self.assertEqual(result['reason'], 'ACCOUNT_CASH_LIMIT')
        self.assertEqual(authority.status(), before)
        self.assertEqual(self.db.execute(
            'SELECT count(*) FROM account_budget_reservations').fetchone()[0],
            reservations)
        self.assertEqual(self.db.execute(
            'SELECT count(*) FROM account_budget_instance_limits').fetchone()[0],
            ceilings)
        self.assertIsNone(self.db.execute(
            "SELECT 1 FROM account_budget_reservations"
            " WHERE instance_id='alpha' AND trade_id='trade-2'").fetchone())

    # 8. float, bool and negative amounts are never money
    def test_float_bool_and_negative_amounts_are_rejected(self):
        authority = self.authority()
        alpha = make_instance('alpha')
        bad_values = (1.5, 0.0, True, False, -1, Decimal('-0.01'),
                      'NaN', 'Infinity')
        for field in ('cash', 'planned_loss', 'notional'):
            for value in bad_values:
                amounts = {'cash': '1', 'planned_loss': '1', 'notional': '1'}
                amounts[field] = value
                with self.assertRaisesRegex(ValueError,
                                            '^BUDGET_AMOUNT_INVALID$'):
                    authority.reserve(alpha, trade_id='trade-1', **amounts)
        status = authority.status()
        self.assertEqual(status['open_trades'], 0)
        self.assertEqual(status['per_instance'], {})

    def test_trade_id_validation(self):
        authority = self.authority()
        alpha = make_instance('alpha')
        for trade_id in ('', 'x' * 129, 123, None, b'trade'):
            with self.assertRaisesRegex(ValueError,
                                        '^BUDGET_TRADE_ID_INVALID$'):
                authority.reserve(alpha, trade_id=trade_id, cash='1',
                                  planned_loss='1', notional='1')
        result = authority.reserve(alpha, trade_id='x' * 128, cash='1',
                                   planned_loss='1', notional='1')
        self.assertTrue(result['reserved'])

    def test_limit_mapping_requires_exactly_the_named_keys(self):
        valid = dict(LIMITS)
        cases = [
            {**valid, 'extra_limit': '1'},
            {key: value for key, value in valid.items()
             if key != 'max_open_cash'},
            {**valid, 'max_open_cash': 1.0},
            {**valid, 'max_open_cash': True},
            {**valid, 'max_open_cash': '-1'},
            {**valid, 'max_open_notional': 'NaN'},
            {**valid, 'max_open_planned_loss': 'Infinity'},
            {**valid, 'max_concurrent_trades': 1.0},
            {**valid, 'max_concurrent_trades': True},
            {**valid, 'max_concurrent_trades': -1},
            None,
            'not-a-mapping',
        ]
        for limits in cases:
            with self.assertRaisesRegex(ValueError,
                                        '^BUDGET_LIMITS_INVALID$'):
                AccountBudgetAuthority(self.db, limits)

    # persistence: status must come from the file, not this instance
    def test_status_survives_a_new_connection_to_the_same_file(self):
        authority = self.authority()
        alpha = make_instance('alpha', max_cash='100')
        authority.reserve(alpha, trade_id='trade-1', cash='12.5',
                          planned_loss='2.25', notional='7.125')
        first = authority.status()

        other = sqlite3.connect(self.path)
        self.addCleanup(other.close)
        reopened = AccountBudgetAuthority(other, dict(LIMITS), clock=self.clock)
        self.addCleanup(reopened.close)

        self.assertEqual(reopened.status(), first)
        self.assertEqual(reopened.instance_status('alpha'),
                         first['per_instance']['alpha'])

        reopened.reserve(alpha, trade_id='trade-2', cash='1',
                         planned_loss='1', notional='1')
        status = authority.status()
        self.assertEqual(status['open_trades'], 2)
        self.assertEqual(status['open_cash'], '13.5')

    def test_close_retires_authority_without_closing_caller_connection(self):
        authority = self.authority()
        authority.close()
        authority.close()
        self.assertEqual(self.db.execute('SELECT 1').fetchone(), (1,))
        with self.assertRaisesRegex(ValueError, '^BUDGET_CLOSED$'):
            authority.status()
        with self.assertRaisesRegex(ValueError, '^BUDGET_CLOSED$'):
            authority.release('alpha', 'trade-1')
