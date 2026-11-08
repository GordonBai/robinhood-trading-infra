"""Per-instance realised cash, fees and PnL over a temporary synthetic database."""
import sqlite3
import tempfile
from datetime import datetime, timezone
from decimal import Decimal
from unittest import TestCase

from trading_infra.instance_accounting import InstanceAccounting

AT = datetime(2026, 10, 29, 14, 0, tzinfo=timezone.utc)


class AccountingTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = self.temp.name + '/accounting.sqlite'
        self.db = sqlite3.connect(self.path, isolation_level=None)
        self.addCleanup(self.db.close)
        self.ledger = InstanceAccounting(self.db, clock=lambda: AT)

    def buy(self, instance='alpha', trade='t1', fill='f1', quantity='1', price='40.00',
            multiplier=1, fees='0'):
        return self.ledger.record_synthetic_fill(instance, trade, fill_id=fill, side='buy',
                                       quantity=quantity, price=price,
                                       multiplier=multiplier, fees=fees)

    def sell(self, instance='alpha', trade='t1', fill='f2', quantity='1', price='41.00',
             multiplier=1, fees='0'):
        return self.ledger.record_synthetic_fill(instance, trade, fill_id=fill, side='sell',
                                       quantity=quantity, price=price,
                                       multiplier=multiplier, fees=fees)

    def test_equity_round_trip_realises_the_difference(self):
        self.buy()
        self.sell()
        summary = self.ledger.trade_summary('alpha', 't1')
        self.assertEqual(Decimal(summary['realised_pnl']), Decimal('1.00'))
        self.assertEqual(Decimal(summary['open_quantity']), Decimal('0'))

    def test_option_multiplier_applies_to_premium_but_not_to_fees(self):
        self.buy(quantity='1', price='2.60', multiplier=100, fees='0.01')
        self.sell(quantity='1', price='2.94', multiplier=100, fees='0.01')
        summary = self.ledger.trade_summary('alpha', 't1')
        # 294 - 260 = 34 gross; fees are 0.02 and are not multiplied.
        self.assertEqual(Decimal(summary['realised_pnl']), Decimal('34.00'))
        self.assertEqual(Decimal(summary['fees']), Decimal('0.02'))
        self.assertEqual(Decimal(summary['net_cash_flow']), Decimal('33.98'))

    def test_instances_never_see_each_others_fills(self):
        self.buy(instance='alpha')
        self.sell(instance='alpha')
        self.buy(instance='beta', price='80.00')
        alpha = self.ledger.instance_summary('alpha')
        beta = self.ledger.instance_summary('beta')
        self.assertEqual(Decimal(alpha['realised_pnl']), Decimal('1.00'))
        self.assertEqual(Decimal(beta['realised_pnl']), Decimal('0'))
        self.assertEqual(Decimal(beta['open_cost_basis']), Decimal('80'))
        self.assertEqual(alpha['trades'], ['t1'])
        self.assertEqual(beta['trades'], ['t1'])

    def test_redelivery_is_idempotent(self):
        self.assertFalse(self.buy()['recorded'] is False)
        again = self.buy()
        self.assertFalse(again['recorded'])
        self.assertEqual(len(self.ledger.fills('alpha')), 1)

    def test_conflicting_redelivery_is_refused(self):
        self.buy()
        with self.assertRaises(ValueError) as ctx:
            self.ledger.record_synthetic_fill('alpha', 't1', fill_id='f1', side='buy',
                                    quantity='2', price='40.00')
        # A redelivery that changed its economics is refused either as an
        # identity conflict or an execution conflict; both refuse the write.
        self.assertIn(str(ctx.exception),
                      {'ACCOUNTING_FILL_CONFLICT',
                       'ACCOUNTING_EXECUTION_ALREADY_ATTRIBUTED'})
        self.assertEqual(len(self.ledger.fills('alpha')), 1)

    def test_a_missing_multiplier_is_never_assumed(self):
        for value in (0, -1, 1.0, True, '100'):
            with self.assertRaises(ValueError):
                self.buy(multiplier=value)

    def test_side_quantity_price_and_fee_are_validated(self):
        with self.assertRaises(ValueError):
            self.ledger.record_synthetic_fill('alpha', 't1', fill_id='x', side='short',
                                    quantity='1', price='1')
        with self.assertRaises(ValueError):
            self.buy(quantity='0')
        with self.assertRaises(ValueError):
            self.buy(quantity=1.5)
        with self.assertRaises(ValueError):
            self.buy(fees='-1')

    def test_partial_fills_accumulate(self):
        self.buy(quantity='1', fill='a1')
        self.buy(quantity='1', fill='a2', price='41.00')
        self.sell(quantity='1', fill='b1', price='42.00')
        summary = self.ledger.trade_summary('alpha', 't1')
        self.assertEqual(summary['fills'], 3)
        self.assertEqual(Decimal(summary['quantity_bought']), Decimal('2'))
        self.assertEqual(Decimal(summary['open_quantity']), Decimal('1'))
        self.assertEqual(Decimal(summary['open_cost_basis']), Decimal('41'))
        # Only the matched unit is realised: 42 - 40. The open lot is not profit.
        self.assertEqual(Decimal(summary['realised_pnl']), Decimal('2.00'))
        self.assertEqual(Decimal(summary['net_cash_flow']), Decimal('-39.00'))

    def test_an_empty_instance_reports_zero_not_an_error(self):
        summary = self.ledger.instance_summary('ghost')
        self.assertEqual(Decimal(summary['realised_pnl']), Decimal('0'))
        self.assertEqual(summary['fills'], 0)
        self.assertEqual(summary['trades'], [])

    def test_accounting_survives_a_new_connection(self):
        self.buy()
        self.sell()
        reopened = sqlite3.connect(self.path, isolation_level=None)
        self.addCleanup(reopened.close)
        again = InstanceAccounting(reopened, clock=lambda: AT)
        self.assertEqual(Decimal(again.instance_summary('alpha')['realised_pnl']),
                         Decimal('1.00'))

    def test_selling_more_than_was_bought_is_refused_at_write_time(self):
        with self.assertRaises(ValueError) as ctx:
            self.sell(quantity='1', price='42.00')
        self.assertEqual(str(ctx.exception), 'ACCOUNTING_POSITION_UNDERFLOW')
        self.assertEqual(self.ledger.fills('alpha'), [])

    def test_matching_lots_must_share_a_multiplier(self):
        self.buy(multiplier=1)
        with self.assertRaises(ValueError) as ctx:
            self.sell(multiplier=100)
        self.assertEqual(str(ctx.exception), 'ACCOUNTING_MULTIPLIER_MISMATCH')

    def test_all_instances_lists_only_those_with_fills(self):
        self.buy(instance='alpha')
        self.buy(instance='beta', trade='x1')
        self.assertEqual(self.ledger.all_instances(), ['alpha', 'beta'])
        self.assertEqual(self.ledger.all_instances().count('gamma'), 0)

    def test_one_venue_execution_cannot_be_booked_to_two_instances(self):
        first = self.ledger.record_fill('alpha', 'trade-a', fill_id='venue-1', side='buy',
                                        quantity='1', price='40', execution_key='acct:venue-1')
        self.assertTrue(first['recorded'])
        with self.assertRaises(ValueError) as ctx:
            self.ledger.record_fill('beta', 'trade-b', fill_id='venue-1', side='buy',
                                    quantity='1', price='40', execution_key='acct:venue-1')
        self.assertEqual(str(ctx.exception), 'ACCOUNTING_EXECUTION_ALREADY_ATTRIBUTED')
        self.assertEqual(len(self.ledger.fills('alpha')), 1)
        self.assertEqual(self.ledger.fills('beta'), [])

    def test_replaying_the_same_venue_execution_is_idempotent(self):
        first = self.ledger.record_fill('alpha', 'trade-a', fill_id='venue-2', side='buy',
                                        quantity='1', price='40', execution_key='acct:venue-2')
        again = self.ledger.record_fill('alpha', 'trade-a', fill_id='venue-2', side='buy',
                                        quantity='1', price='40', execution_key='acct:venue-2')
        self.assertTrue(first['recorded'])
        self.assertFalse(again['recorded'])
        self.assertEqual(len(self.ledger.fills('alpha')), 1)

    def test_the_same_venue_id_with_different_economics_is_refused(self):
        self.ledger.record_fill('alpha', 'trade-a', fill_id='venue-3', side='buy',
                                quantity='1', price='40', execution_key='acct:venue-3')
        with self.assertRaises(ValueError):
            self.ledger.record_fill('alpha', 'trade-a', fill_id='venue-3', side='buy',
                                    quantity='2', price='20', execution_key='acct:venue-3')

    def test_a_venue_fill_without_an_execution_key_is_refused(self):
        # The signature makes the account-level identity mandatory, so a caller
        # cannot record a venue fill without one.
        with self.assertRaises(TypeError):
            self.ledger.record_fill('alpha', 'trade-a', fill_id='v', side='buy',
                                    quantity='1', price='40')
        with self.assertRaises(ValueError) as ctx:
            self.ledger.record_fill('alpha', 'trade-a', fill_id='v', side='buy',
                                    quantity='1', price='40', execution_key='')
        self.assertEqual(str(ctx.exception), 'ACCOUNTING_EXECUTION_KEY_REQUIRED')
        self.assertEqual(self.ledger.fills('alpha'), [])

    def test_a_synthetic_fill_never_collides_with_a_venue_key(self):
        self.ledger.record_fill('alpha', 'trade-a', fill_id='venue-9', side='buy',
                                quantity='1', price='40', execution_key='acct:venue-9')
        # Different namespaces, so a synthetic record cannot shadow a venue one.
        synthetic = self.ledger.record_synthetic_fill('beta', 'trade-b',
                                                      fill_id='venue-9', side='buy',
                                                      quantity='1', price='40')
        self.assertTrue(synthetic['recorded'])
        self.assertEqual(len(self.ledger.fills('alpha')), 1)
        self.assertEqual(len(self.ledger.fills('beta')), 1)

    def test_a_legacy_fill_adopts_its_identity_on_replay(self):
        # A fill recorded before account-level identity existed.
        self.ledger.record_synthetic_fill('alpha', 'trade-a', fill_id='legacy-1',
                                          side='buy', quantity='1', price='40')
        replayed = self.ledger.record_fill('alpha', 'trade-a', fill_id='legacy-1',
                                           side='buy', quantity='1', price='40',
                                           execution_key='acct:legacy-1')
        self.assertFalse(replayed['recorded'])
        self.assertTrue(replayed.get('identity_adopted'))
        # Now the same venue execution cannot be booked to another instance.
        with self.assertRaises(ValueError) as ctx:
            self.ledger.record_fill('beta', 'trade-b', fill_id='legacy-1', side='buy',
                                    quantity='1', price='40', execution_key='acct:legacy-1')
        self.assertEqual(str(ctx.exception), 'ACCOUNTING_EXECUTION_ALREADY_ATTRIBUTED')
        self.assertEqual(self.ledger.fills('beta'), [])
