"""Instance execution feedback is instance-owned, authoritative and non-flat by default."""
import json
from pathlib import Path
import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest import TestCase

from martin_agentic.execution import ExecutionBook
from trading_contracts.instance import InstanceBudget, PluginManifest
from trading_infra.attribution import ExecutionAttribution
from trading_infra.instance_accounting import InstanceAccounting
from trading_infra.instance_host import InstanceStrategyHost
from trading_infra.instance_registry import InstanceRegistry

POLICY = json.loads(Path(
    'tests/fixtures/policies/live_trial_2026-10-16.json').read_text())

AT = datetime(2026, 11, 7, 15, 0, tzinfo=timezone.utc)


def manifest():
    return PluginManifest(
        plugin_id='demo', version='1.0.0', state_schema='demo-1',
        config={'cost_buffer_per_share': '0.05'}, data_requirements=('quotes',),
        capabilities=('advisory-entry',), max_inflight=256)


def budget():
    return InstanceBudget(max_cash=Decimal('100'),
                          max_planned_loss=Decimal('2.50'),
                          max_notional=Decimal('100'), max_concurrent=1)


class InstanceExecutionFeedbackTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = sqlite3.connect(self.temp.name + '/feedback.sqlite',
                                  isolation_level=None)
        self.addCleanup(self.db.close)
        self.registry = InstanceRegistry(self.db, clock=lambda: AT)
        self.registry.register('alpha', manifest(), budget(), mode='shadow',
                               enabled=True)
        self.host = InstanceStrategyHost(
            self.registry, self.registry.instance('alpha'), clock=lambda: AT)
        self.attribution = ExecutionAttribution(self.db, clock=lambda: AT)
        self.accounting = InstanceAccounting(self.db, clock=lambda: AT)
        self.attribution.bind('ref-a', 'alpha', 'trade-a')
        # The real execution owner writes the broker-observation time. The raw
        # test connection only reads the same database.
        self.book = ExecutionBook(Path(self.temp.name) / 'feedback.sqlite',
                                  POLICY, simulation=True, clock=lambda: AT)
        self.addCleanup(self.book.close)
        self.observe()

    def observe(self, at=AT):
        """Record one complete owner broker observation, as the book does."""
        self.book._begin()
        state = self.book.state()
        state['last_snapshot'] = at.isoformat()
        self.book._save(state)
        self.book.db.commit()

    def queue(self, state='READY', *, kind='exit', quantity='2', at=AT,
              trade_id='trade-a'):
        """Create one management action through the real execution book."""
        trade = {'id': trade_id, 'plan': {'symbol': 'DEMO'},
                 'purpose': 'unclassified'}
        parameters = {'symbol': 'DEMO', 'side': 'sell', 'type': 'market',
                      'quantity': quantity, 'market_hours': 'regular_hours',
                      'time_in_force': 'gfd'}
        self.book._begin()
        action_id = self.book._queue(trade, kind, parameters, at, 'TEST',
                                     state=state)
        self.book.db.commit()
        return action_id

    def event(self, event_id, kind, at=AT, **extra):
        self.attribution.record({'event_id': event_id, 'ref_id': 'ref-a',
                                 'kind': kind, 'occurred_at': at.isoformat(),
                                 **extra})

    def test_empty_is_not_executed_not_flat(self):
        view = self.host.execution_view()
        self.assertEqual(view['lifecycle'], 'not_executed')
        self.assertEqual(view['confirmed_quantity'], '0')
        self.assertFalse(view['fact_fresh'])
        self.assertTrue(view['observation_fresh'])
        self.assertEqual(view['available_quantity'], 'unknown')

    def test_pending_and_unknown_never_become_flat(self):
        self.event('pending-1', 'pending_confirmation')
        self.assertEqual(self.host.execution_view()['lifecycle'], 'pending')
        self.event('unknown-1', 'unknown')
        self.assertEqual(self.host.execution_view()['lifecycle'], 'unknown')

    def test_partial_protection_exit_and_fresh_flat(self):
        self.accounting.record_fill(
            'alpha', 'trade-a', fill_id='f1', side='buy', quantity='2',
            price='40', execution_key='exec-f1', occurred_at=AT.isoformat())
        self.event('fill-1', 'filled', quantity='2', price='40')
        view = self.host.execution_view()
        self.assertEqual(view['lifecycle'], 'partial')
        self.assertEqual(view['confirmed_quantity'], '2')
        self.event('protect-1', 'protection_covered', quantity='2')
        self.assertEqual(self.host.execution_view()['lifecycle'], 'protected')
        self.event('exit-1', 'exit')
        self.assertEqual(self.host.execution_view()['lifecycle'], 'exit_pending')
        self.accounting.record_fill(
            'alpha', 'trade-a', fill_id='f2', side='sell', quantity='2',
            price='41', execution_key='exec-f2',
            occurred_at=AT.isoformat())
        self.event('flat-1', 'flat', at=AT)
        self.assertEqual(self.host.execution_view()['lifecycle'], 'flat_verified')
        self.assertEqual(self.host.execution_view()['confirmed_quantity'], '0')
        self.assertEqual(self.host.execution_view()['available_quantity'], '0')

    def test_stale_observation_does_not_verify_flat(self):
        self.observe(AT - timedelta(minutes=2))
        self.event('flat-recent', 'flat', at=AT)
        view = self.host.execution_view()
        self.assertTrue(view['fact_fresh'])
        self.assertFalse(view['observation_fresh'])
        self.assertEqual(view['lifecycle'], 'unknown')

    def test_long_idle_flat_with_fresh_observation_remains_flat(self):
        self.event('flat-idle', 'flat', at=AT - timedelta(minutes=2))
        view = self.host.execution_view()
        self.assertFalse(view['fact_fresh'])
        self.assertTrue(view['observation_fresh'])
        self.assertEqual(view['lifecycle'], 'flat_verified')

    def test_old_flat_does_not_overwrite_a_newer_pending_or_unknown(self):
        self.event('flat-old', 'flat', at=AT - timedelta(minutes=2))
        self.event('pending-new', 'pending_confirmation',
                   at=AT - timedelta(seconds=1))
        self.assertEqual(self.host.execution_view()['lifecycle'], 'pending')
        self.event('unknown-new', 'unknown', at=AT)
        self.assertEqual(self.host.execution_view()['lifecycle'], 'unknown')
        self.accounting.record_fill(
            'alpha', 'trade-a', fill_id='f1', side='buy', quantity='1',
            price='40', execution_key='exec-f1', occurred_at=AT.isoformat())
        self.event('fill-new', 'filled', at=AT, quantity='1', price='40')
        self.event('flat-old-2', 'flat', at=AT - timedelta(seconds=1))
        view = self.host.execution_view()
        self.assertEqual(view['lifecycle'], 'partial')
        self.assertEqual(view['confirmed_quantity'], '1')

    def test_current_owner_states_are_visible_and_never_fabricate_flat(self):
        for state in ('READY', 'RUNNING', 'WAITING_USER', 'UNKNOWN'):
            with self.subTest(state=state):
                self.book.db.execute('DELETE FROM live_actions')
                self.book.db.commit()
                self.queue(state, kind='exit')
                view = self.host.execution_view()
                self.assertEqual(view['owner_state'], state)
                self.assertEqual(view['owner_action_kind'], 'exit')
                self.assertNotEqual(view['lifecycle'], 'flat_verified')

    def test_ready_exit_with_quantity_makes_available_known(self):
        earlier = AT - timedelta(seconds=2)
        self.accounting.record_fill(
            'alpha', 'trade-a', fill_id='f1', side='buy', quantity='2',
            price='40', execution_key='exec-f1', occurred_at=earlier.isoformat())
        self.event('fill-1', 'filled', at=earlier, quantity='2', price='40')
        self.assertEqual(self.host.execution_view()['available_quantity'],
                         'unknown')
        self.queue('READY', kind='exit', quantity='2')
        view = self.host.execution_view()
        self.assertEqual(view['lifecycle'], 'exit_pending')
        self.assertEqual(view['available_quantity'], '0')
        self.assertEqual(view['available_quantity_state'], 'confirmed')

    def test_sell_reservation_without_quantity_stays_unknown(self):
        self.accounting.record_fill(
            'alpha', 'trade-a', fill_id='f1', side='buy', quantity='2',
            price='40', execution_key='exec-f1', occurred_at=AT.isoformat())
        self.event('fill-1', 'filled', quantity='2', price='40')
        self.queue('RUNNING', kind='exit', quantity='')
        view = self.host.execution_view()
        self.assertEqual(view['available_quantity'], 'unknown')
        self.assertEqual(view['available_quantity_state'], 'unknown')

    def test_static_flat_survives_restart_with_fresh_observation(self):
        self.event('flat-1', 'flat', at=AT - timedelta(minutes=2))
        first = self.host.execution_view()
        restarted = InstanceStrategyHost(
            self.registry, self.registry.instance('alpha'), clock=lambda: AT)
        second = restarted.execution_view()
        self.assertEqual(first['lifecycle'], 'flat_verified')
        self.assertEqual(second['lifecycle'], 'flat_verified')
        self.assertEqual(second['feedback_version'], first['feedback_version'])
        self.assertEqual(second['observation_at'], first['observation_at'])

    def test_two_instance_flat_and_position_are_isolated(self):
        self.registry.register('beta', manifest(), budget(), mode='shadow',
                               enabled=True)
        beta = InstanceStrategyHost(
            self.registry, self.registry.instance('beta'), clock=lambda: AT)
        self.attribution.bind('ref-b', 'beta', 'trade-b')
        self.accounting.record_fill(
            'beta', 'trade-b', fill_id='b1', side='buy', quantity='3',
            price='40', execution_key='exec-b1', occurred_at=AT.isoformat())
        self.attribution.record({'event_id': 'beta-fill', 'ref_id': 'ref-b',
                                 'kind': 'filled', 'occurred_at': AT.isoformat(),
                                 'quantity': '3', 'price': '40'})
        self.attribution.record({'event_id': 'alpha-flat', 'ref_id': 'ref-a',
                                 'kind': 'flat', 'occurred_at': AT.isoformat()})
        self.assertEqual(self.host.execution_view()['lifecycle'], 'flat_verified')
        beta_view = beta.execution_view()
        self.assertEqual(beta_view['lifecycle'], 'partial')
        self.assertEqual(beta_view['confirmed_quantity'], '3')
        self.assertEqual(beta_view['available_quantity'], 'unknown')

    def _two_open_trades(self):
        earlier = AT - timedelta(seconds=2)
        self.attribution.bind('ref-a2', 'alpha', 'trade-a2')
        for trade, fill, ref in (('trade-a', 'f1', 'ref-a'),
                                 ('trade-a2', 'f2', 'ref-a2')):
            self.accounting.record_fill(
                'alpha', trade, fill_id=fill, side='buy', quantity='1',
                price='40', execution_key='exec-' + fill,
                occurred_at=earlier.isoformat())
            self.attribution.record({
                'event_id': 'fill-' + fill, 'ref_id': ref, 'kind': 'filled',
                'occurred_at': earlier.isoformat(), 'quantity': '1',
                'price': '40'})

    def test_multiple_live_exits_sum_reserved_quantity(self):
        self._two_open_trades()
        self.queue('READY', kind='exit', quantity='1', trade_id='trade-a')
        self.queue('READY', kind='exit', quantity='1', trade_id='trade-a2')
        view = self.host.execution_view()
        self.assertEqual(view['confirmed_quantity'], '2')
        self.assertEqual(view['available_quantity'], '0')
        self.assertEqual(view['available_quantity_state'], 'confirmed')

    def test_partial_sell_reservation_leaves_only_the_remainder_available(self):
        self._two_open_trades()
        self.queue('READY', kind='exit', quantity='1', trade_id='trade-a')
        view = self.host.execution_view()
        self.assertEqual(view['confirmed_quantity'], '2')
        self.assertEqual(view['available_quantity'], '1')

    def test_invalid_or_nonpositive_sell_quantity_is_unknown(self):
        self._two_open_trades()
        for quantity in ('abc', '-1', '0'):
            with self.subTest(quantity=quantity):
                self.book.db.execute('DELETE FROM live_actions')
                self.book.db.commit()
                self.queue('READY', kind='exit', quantity=quantity,
                           trade_id='trade-a')
                view = self.host.execution_view()
                self.assertEqual(view['available_quantity'], 'unknown')
                self.assertEqual(view['available_quantity_state'], 'unknown')

    def test_waiting_user_or_unknown_sell_state_never_reports_available(self):
        self._two_open_trades()
        for state in ('WAITING_USER', 'UNKNOWN'):
            with self.subTest(state=state):
                self.book.db.execute('DELETE FROM live_actions')
                self.book.db.commit()
                self.queue(state, kind='exit', quantity='2', trade_id='trade-a')
                view = self.host.execution_view()
                self.assertEqual(view['owner_state'], state)
                self.assertEqual(view['available_quantity'], 'unknown')
                self.assertEqual(view['available_quantity_state'], 'unknown')
                self.assertEqual(view['lifecycle'], 'unknown')

    def test_other_instance_action_never_leaks_in(self):
        self.registry.register('beta', manifest(), budget(), mode='shadow',
                               enabled=True)
        beta = InstanceStrategyHost(
            self.registry, self.registry.instance('beta'), clock=lambda: AT)
        self.attribution.bind('ref-b', 'beta', 'trade-b')
        self.queue('WAITING_USER', kind='exit', trade_id='trade-b')
        self.assertIsNone(self.host.execution_view()['owner_state'])
        self.assertEqual(beta.execution_view()['owner_state'], 'WAITING_USER')

    def test_rejected_is_not_executed_and_duplicate_does_not_advance(self):
        self.event('rejected-1', 'rejected')
        first = self.host.execution_view()
        self.assertEqual(first['lifecycle'], 'not_executed')
        self.event('rejected-1', 'rejected')
        second = self.host.execution_view()
        self.assertEqual(second['feedback_version'], first['feedback_version'])
        self.assertEqual(second['lifecycle'], 'not_executed')

    def test_duplicate_and_restart_keep_same_feedback_view(self):
        self.event('pending-1', 'pending_confirmation')
        first = self.host.execution_view()
        self.event('pending-1', 'pending_confirmation')
        restarted = InstanceStrategyHost(
            self.registry, self.registry.instance('alpha'), clock=lambda: AT)
        self.assertEqual(restarted.execution_view()['feedback_version'],
                         first['feedback_version'])
        self.assertEqual(restarted.execution_view()['lifecycle'], 'pending')

    def test_instances_are_isolated_by_instance_not_symbol(self):
        self.registry.register('beta', manifest(), budget(), mode='shadow',
                               enabled=True)
        beta = InstanceStrategyHost(
            self.registry, self.registry.instance('beta'), clock=lambda: AT)
        self.attribution.bind('ref-b', 'beta', 'trade-b')
        self.accounting.record_fill(
            'alpha', 'trade-a', fill_id='f1', side='buy', quantity='1',
            price='40', execution_key='exec-f1', occurred_at=AT.isoformat())
        self.accounting.record_fill(
            'beta', 'trade-b', fill_id='f2', side='buy', quantity='3',
            price='40', execution_key='exec-f2', occurred_at=AT.isoformat())
        self.attribution.record({'event_id': 'b-fill', 'ref_id': 'ref-b',
                                 'kind': 'filled', 'occurred_at': AT.isoformat()})
        self.assertEqual(beta.execution_view()['lifecycle'], 'partial')
        self.assertEqual(beta.execution_view()['confirmed_quantity'], '3')
        self.assertEqual(self.host.execution_view()['confirmed_quantity'], '1')
