"""Exclusive symbol ownership over real temporary sqlite files; no broker calls."""
from datetime import datetime, timedelta, timezone
import sqlite3
import tempfile
import os
from unittest import TestCase

from trading_infra.symbol_lease import SymbolLease


class FixedClock:
    def __init__(self):
        self.now = datetime(2026, 10, 29, 15, 0, tzinfo=timezone.utc)

    def __call__(self):
        return self.now


class SymbolLeaseTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = os.path.join(self.temp.name, 'lease.sqlite3')
        self.clock = FixedClock()
        self.db = sqlite3.connect(self.path)
        self.addCleanup(self.db.close)
        self.lease = SymbolLease(self.db, clock=self.clock)
        self.addCleanup(self.lease.close)

    def connect(self):
        db = sqlite3.connect(self.path)
        self.addCleanup(db.close)
        lease = SymbolLease(db, clock=self.clock)
        self.addCleanup(lease.close)
        return lease

    def decisions(self, request):
        """Run the same two acquisitions on a fresh file and return both dicts."""
        with tempfile.TemporaryDirectory() as temp:
            db = sqlite3.connect(os.path.join(temp, 'lease.sqlite3'))
            try:
                lease = SymbolLease(db, clock=self.clock)
                return (lease.acquire('instance-a', request, trade_id='trade-1'),
                        lease.acquire('instance-b', ['SPY', 'QQQ'], trade_id='trade-2'))
            finally:
                db.close()

    def test_acquire_holds_sorted_symbols_with_injected_timestamp(self):
        result = self.lease.acquire('instance-a', ['SPY', 'QQQ'], trade_id='trade-1')
        self.assertEqual(result, {'acquired': True, 'instance_id': 'instance-a',
                                  'trade_id': 'trade-1', 'symbols': ['QQQ', 'SPY']})
        self.assertEqual(self.lease.owner('SPY'), 'instance-a')
        self.assertEqual(self.lease.owner('QQQ'), 'instance-a')
        self.assertIsNone(self.lease.owner('IWM'))
        self.assertEqual(self.lease.held(), [
            {'symbol': 'QQQ', 'instance_id': 'instance-a', 'trade_id': 'trade-1',
             'acquired_at': self.clock.now.isoformat()},
            {'symbol': 'SPY', 'instance_id': 'instance-a', 'trade_id': 'trade-1',
             'acquired_at': self.clock.now.isoformat()}])
        self.assertEqual(self.lease.holding('instance-a'), self.lease.held())
        self.assertEqual(self.lease.holding('instance-b'), [])

    def test_input_order_does_not_change_success_or_conflict(self):
        forward = self.decisions(['QQQ', 'SPY'])
        reverse = self.decisions(['SPY', 'QQQ'])
        self.assertEqual(forward, reverse)
        self.assertTrue(forward[0]['acquired'])
        self.assertEqual(forward[0]['symbols'], ['QQQ', 'SPY'])
        self.assertFalse(forward[1]['acquired'])
        self.assertEqual(forward[1]['reason'], 'SYMBOL_LEASE_HELD')
        self.assertEqual(forward[1]['symbols'], ['QQQ', 'SPY'])
        self.assertEqual(forward[1]['conflicts'], ['QQQ', 'SPY'])

    def test_second_instance_rejected_with_holder_and_nothing_inserted(self):
        self.lease.acquire('instance-a', ['SPY', 'QQQ'], trade_id='trade-1')
        result = self.lease.acquire('instance-b', ['SPY'], trade_id='trade-2')
        self.assertFalse(result['acquired'])
        self.assertEqual(result['reason'], 'SYMBOL_LEASE_HELD')
        self.assertEqual(result['symbols'], ['SPY'])
        self.assertEqual(result['conflicts'], ['SPY'])
        self.assertEqual(result['holder'], {
            'symbol': 'SPY', 'instance_id': 'instance-a', 'trade_id': 'trade-1',
            'acquired_at': self.clock.now.isoformat()})
        self.assertEqual(len(self.lease.held()), 2)
        self.assertEqual(self.lease.owner('SPY'), 'instance-a')

    def test_one_conflicting_symbol_rejects_the_whole_request(self):
        self.lease.acquire('instance-a', ['SPY'], trade_id='trade-1')
        result = self.lease.acquire('instance-b', ['QQQ', 'SPY'], trade_id='trade-2')
        self.assertFalse(result['acquired'])
        self.assertEqual(result['reason'], 'SYMBOL_LEASE_HELD')
        self.assertEqual(result['conflicts'], ['SPY'])
        self.assertIsNone(self.lease.owner('QQQ'))
        self.assertEqual(self.lease.held(), [
            {'symbol': 'SPY', 'instance_id': 'instance-a', 'trade_id': 'trade-1',
             'acquired_at': self.clock.now.isoformat()}])

    def test_same_instance_different_trade_is_held_not_idempotent(self):
        self.lease.acquire('instance-a', ['SPY'], trade_id='trade-1')
        result = self.lease.acquire('instance-a', ['SPY'], trade_id='trade-2')
        self.assertFalse(result['acquired'])
        self.assertEqual(result['reason'], 'SYMBOL_LEASE_HELD')
        self.assertEqual(result['holder']['trade_id'], 'trade-1')
        self.assertEqual(len(self.lease.held()), 1)

    def test_reacquire_same_identity_and_set_is_idempotent(self):
        acquired_at = self.clock.now.isoformat()
        first = self.lease.acquire('instance-a', ['QQQ', 'SPY'], trade_id='trade-1')
        self.clock.now += timedelta(hours=1)
        again = self.lease.acquire('instance-a', ['SPY', 'QQQ'], trade_id='trade-1')
        self.assertEqual(again, first)
        self.assertEqual(len(self.lease.held()), 2)
        self.assertEqual({row['acquired_at'] for row in self.lease.held()},
                         {acquired_at})

    def test_reusing_identity_for_a_different_set_raises_and_writes_nothing(self):
        self.lease.acquire('instance-a', ['SPY'], trade_id='trade-1')
        with self.assertRaisesRegex(ValueError, 'SYMBOL_LEASE_ACQUISITION_CONFLICT'):
            self.lease.acquire('instance-a', ['SPY', 'QQQ'], trade_id='trade-1')
        with self.assertRaisesRegex(ValueError, 'SYMBOL_LEASE_ACQUISITION_CONFLICT'):
            self.lease.acquire('instance-a', ['QQQ'], trade_id='trade-1')
        self.assertEqual([row['symbol'] for row in self.lease.held()], ['SPY'])
        self.assertIsNone(self.lease.owner('QQQ'))

    def test_owner_release_frees_symbol_for_another_instance(self):
        self.lease.acquire('instance-a', ['SPY', 'QQQ'], trade_id='trade-1')
        self.assertEqual(self.lease.release('instance-a', 'trade-1'), 2)
        self.assertEqual(self.lease.release('instance-a', 'trade-1'), 0)
        self.assertEqual(self.lease.held(), [])
        self.assertIsNone(self.lease.owner('SPY'))
        result = self.lease.acquire('instance-b', ['SPY'], trade_id='trade-2')
        self.assertTrue(result['acquired'])
        self.assertEqual(self.lease.owner('SPY'), 'instance-b')

    def test_non_owner_release_removes_nothing(self):
        self.lease.acquire('instance-a', ['SPY'], trade_id='trade-1')
        self.assertEqual(self.lease.release('instance-b', 'trade-2'), 0)
        self.assertEqual(self.lease.release('instance-a', 'trade-2'), 0)
        self.assertEqual(self.lease.owner('SPY'), 'instance-a')
        self.assertEqual(len(self.lease.held()), 1)

    def test_assert_releasable_checks_owner_reason_and_unknown_outcome(self):
        self.lease.acquire('instance-a', ['SPY', 'QQQ'], trade_id='trade-1')
        self.assertEqual(self.lease.assert_releasable('instance-b', 'trade-2',
                                                      reason='flat_confirmed'),
                         {'releasable': False,
                          'reason': 'SYMBOL_LEASE_RELEASE_NOT_OWNER'})
        self.assertEqual(self.lease.assert_releasable('instance-a', 'trade-1',
                                                      reason='hopeful'),
                         {'releasable': False,
                          'reason': 'SYMBOL_LEASE_RELEASE_BLOCKED'})
        self.lease.mark_unsettled('instance-a', 'trade-1', ['SPY', 'QQQ'])
        self.assertEqual([row['symbol'] for row in self.lease.unsettled()],
                         ['QQQ', 'SPY'])
        # An unknown outcome still counts as held.
        self.assertEqual(self.lease.owner('SPY'), 'instance-a')
        self.assertFalse(self.lease.acquire('instance-b', ['SPY'],
                                            trade_id='trade-2')['acquired'])
        for reason in ('flat_confirmed', 'rejected_entry', 'abandoned'):
            self.assertEqual(self.lease.assert_releasable('instance-a', 'trade-1',
                                                          reason=reason),
                             {'releasable': False,
                              'reason': 'SYMBOL_LEASE_UNSETTLED'})
        self.lease.settle('instance-a', 'trade-1')
        self.assertEqual(self.lease.unsettled(), [])
        self.assertEqual(self.lease.assert_releasable('instance-a', 'trade-1',
                                                      reason='flat_confirmed'),
                         {'releasable': True, 'reason': 'SYMBOL_LEASE_RELEASABLE'})
        self.assertEqual(self.lease.release('instance-a', 'trade-1'), 2)

    def test_mark_unsettled_requires_holding_the_symbol(self):
        self.lease.acquire('instance-a', ['SPY'], trade_id='trade-1')
        self.lease.mark_unsettled('instance-a', 'trade-1', ['SPY'])
        self.lease.mark_unsettled('instance-a', 'trade-1', ['SPY'])
        self.assertEqual([row['symbol'] for row in self.lease.unsettled()], ['SPY'])
        with self.assertRaisesRegex(ValueError, 'SYMBOL_LEASE_HOLDING_REQUIRED'):
            self.lease.mark_unsettled('instance-a', 'trade-1', ['SPY', 'QQQ'])
        self.assertIsNone(self.lease.owner('QQQ'))
        self.assertEqual([row['symbol'] for row in self.lease.unsettled()], ['SPY'])

    def test_invalid_symbol_inputs_are_rejected_without_normalisation(self):
        cases = [[], 'SPY', ['spy'], ['SPY', 'SPY'], ['SPY/QQQ'], ['SP Y'],
                 ['A' * 16], ['1SPY'], [''], [None], [123], None]
        for symbols in cases:
            with self.subTest(symbols=symbols):
                result = self.lease.acquire('instance-a', symbols, trade_id='trade-1')
                self.assertFalse(result['acquired'])
                self.assertEqual(result['reason'], 'SYMBOL_LEASE_INVALID_SYMBOLS')
                self.assertEqual(self.lease.held(), [])
            self.assertEqual(self.db.execute(
                'SELECT count(*) FROM symbol_leases').fetchone()[0], 0)

    def test_boundary_symbols_are_accepted(self):
        result = self.lease.acquire('instance-a', ['A', 'BRK.B', 'ABC-1',
                                                   'A' * 15], trade_id='trade-1')
        self.assertTrue(result['acquired'])
        self.assertEqual(result['symbols'], ['A', 'A' * 15, 'ABC-1', 'BRK.B'])

    def test_invalid_instance_or_trade_ids_are_rejected(self):
        for instance_id, trade_id in [('', 'trade-1'), ('x' * 129, 'trade-1'),
                                      ('instance-a', ''), (None, 'trade-1'),
                                      ('instance-a', 7), ('   ', 'trade-1')]:
            with self.subTest(instance_id=instance_id, trade_id=trade_id):
                result = self.lease.acquire(instance_id, ['SPY'], trade_id=trade_id)
                self.assertFalse(result['acquired'])
                self.assertEqual(result['reason'], 'SYMBOL_LEASE_INSTANCE_INVALID')
                self.assertEqual(self.lease.held(), [])

    def test_owner_rejects_a_malformed_symbol_query(self):
        with self.assertRaisesRegex(ValueError, 'SYMBOL_LEASE_INVALID_SYMBOLS'):
            self.lease.owner('spy')

    def test_leases_persist_across_a_new_connection(self):
        self.lease.acquire('instance-a', ['SPY'], trade_id='trade-1')
        self.lease.mark_unsettled('instance-a', 'trade-1', ['SPY'])
        self.lease.close()
        # close() stops mutations but leaves the caller's connection open.
        self.assertEqual(self.db.execute('SELECT count(*) FROM symbol_leases')
                         .fetchone()[0], 1)
        with self.assertRaisesRegex(ValueError, 'SYMBOL_LEASE_CLOSED'):
            self.lease.acquire('instance-b', ['QQQ'], trade_id='trade-2')

        restarted = self.connect()
        self.assertEqual(restarted.owner('SPY'), 'instance-a')
        self.assertEqual([row['symbol'] for row in restarted.unsettled()], ['SPY'])
        self.assertEqual(restarted.held(), [
            {'symbol': 'SPY', 'instance_id': 'instance-a', 'trade_id': 'trade-1',
             'acquired_at': self.clock.now.isoformat()}])
        self.assertEqual(restarted.release('instance-a', 'trade-1'), 1)
        self.assertIsNone(restarted.owner('SPY'))
