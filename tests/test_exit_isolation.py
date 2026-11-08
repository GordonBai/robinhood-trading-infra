"""Exit advice is bounded, out-of-process, and selected by plugin ownership.

The owner loop no longer computes Martin's exit rule. Each advice is produced
in a short-lived child process, bound to the exact position view by the
infrastructure, and validated again at the execution authority. A hung or
crashed advisor cannot stop reconciliation or protective management.
"""
import json
import os
import time
from datetime import timedelta
from unittest import TestCase

from martin_luk.live_session import LiveSupervisor
from test_partial_lifecycle import PartialFlow, manifest
from trading_infra.exit_advisory import ExitAdvisoryService


def rule_exit(view):
    return {'action': 'EXIT', 'reason': 'FIXTURE_EXIT'}


def rule_hold(view):
    return {'action': 'HOLD', 'reason': 'FIXTURE_HOLD'}


def rule_hang(view):
    time.sleep(5)
    return {'action': 'HOLD', 'reason': 'NEVER'}


def rule_crash(view):
    raise RuntimeError('fixture advisor crash')


class ExitAdvisoryTests(PartialFlow):
    def setUp(self):
        super().setUp()
        # The instance manifest in this fixture declares plugin_id 'alpha'.
        self.book.exit_decider = None

    def advisor(self, spec, *, timeout=2.0):
        service = ExitAdvisoryService({'alpha': spec}, timeout_seconds=timeout,
                                      advice_ttl_seconds=20)
        self.supervisor.exit_advisor = service
        return service

    def test_a_plugin_exit_rule_runs_in_a_child_process_and_is_applied(self):
        entry, protection, entry_order, resting = self.full_entry_protected()
        self.advisor('test_exit_isolation:rule_exit')
        # Tick 1 submits the bounded advisor; the owner reconciles at once.
        self.now += timedelta(seconds=1)
        self.step([entry_order, resting], qty='2')
        # Tick 2 harvests the finished advice and applies it.
        time.sleep(0.5)
        self.now += timedelta(seconds=1)
        self.step([entry_order, resting], qty='2')
        trade = self.book.state()['trades'][entry['trade_id']]
        self.assertTrue(trade['exit_requested'])
        advice = trade['last_exit_advice']
        self.assertEqual(advice['action'], 'EXIT')
        self.assertNotEqual(advice['advisor_pid'], os.getpid(),
                            'the rule must not run in the owner process')
        self.assertEqual(advice['position_version'],
                         self.book.exit_position_version(trade))

    def test_a_different_rule_is_selected_by_plugin_identity(self):
        entry, protection, entry_order, resting = self.full_entry_protected()
        self.advisor('test_exit_isolation:rule_hold')
        self.now += timedelta(seconds=1)
        self.step([entry_order, resting], qty='2')
        time.sleep(0.5)
        self.now += timedelta(seconds=1)
        self.step([entry_order, resting], qty='2')
        trade = self.book.state()['trades'][entry['trade_id']]
        self.assertFalse(trade['exit_requested'])
        self.assertEqual(trade['last_exit_advice']['action'], 'HOLD')

    def test_a_hung_advisor_times_out_without_stopping_protection(self):
        entry, protection, entry_order, resting = self.full_entry_protected()
        self.advisor('test_exit_isolation:rule_hang', timeout=0.3)
        self.now += timedelta(minutes=5)
        started = time.monotonic()
        result = self.step([entry_order, resting], qty='2', bid='38.90', ask='38.95')
        elapsed = time.monotonic() - started
        self.assertLess(elapsed, 2, 'the owner must not wait for the plugin')
        self.assertIsNotNone(self.action('cancel'),
                             'protection ran without waiting for the advisor')
        time.sleep(0.4)
        self.now += timedelta(seconds=1)
        result = self.step([entry_order, resting], qty='2', bid='38.90', ask='38.95')
        reasons = [item.get('reason') for item in
                   result.get('strategy_exit_advice_errors') or []]
        self.assertIn('EXIT_ADVISOR_TIMEOUT', reasons)
        # Infrastructure management still ran: the breached stop is retired.
        self.assertIsNotNone(self.action('cancel'))
        trade = self.book.state()['trades'][entry['trade_id']]
        self.assertTrue(trade['exit_requested'])

    def test_a_crashed_advisor_is_reported_and_does_not_invent_an_exit(self):
        entry, protection, entry_order, resting = self.full_entry_protected()
        service = self.advisor('test_exit_isolation:rule_crash')
        self.now += timedelta(seconds=1)
        self.step([entry_order, resting], qty='2')
        deadline = time.monotonic() + 5
        while service._pending and any(
                process.is_alive()
                for process, _output, _view, _deadline in service._pending.values()):
            if time.monotonic() >= deadline:
                self.fail('advisor child did not finish within bounded wait')
            time.sleep(0.01)
        processes = [process for process, _output, _view, _deadline
                     in service._pending.values()]
        self.now += timedelta(seconds=1)
        result = self.step([entry_order, resting], qty='2')
        self.assertFalse(any(process.is_alive() for process in processes),
                         'completed crashed advisor child was not reaped')
        reasons = [item.get('reason') for item in
                   result.get('strategy_exit_advice_errors') or []]
        self.assertTrue(any(item.startswith('EXIT_ADVISOR_FAILED') for item in reasons))
        trade = self.book.state()['trades'][entry['trade_id']]
        self.assertFalse(trade['exit_requested'])

    def test_stale_or_misbound_advice_is_refused_at_the_authority(self):
        entry, protection, entry_order, resting = self.full_entry_protected()
        self.now += timedelta(seconds=1)
        self.step([entry_order, resting], qty='2')
        trade = self.book.state()['trades'][entry['trade_id']]
        good = {'action': 'EXIT', 'reason': 'FIXTURE',
                'as_of': self.now.isoformat(),
                'valid_until': (self.now + timedelta(seconds=20)).isoformat(),
                'position_version': self.book.exit_position_version(trade)}
        stale = dict(good, valid_until=(self.now - timedelta(seconds=1)).isoformat())
        self.assertEqual(self.book._decision_from_advice(trade, stale, self.now)['action'],
                         'HOLD')
        self.assertEqual(trade['exit_advice_diagnostic'], 'EXIT_ADVICE_STALE')
        wrong = dict(good, position_version='not-this-position')
        self.assertEqual(self.book._decision_from_advice(trade, wrong, self.now)['action'],
                         'HOLD')
        self.assertEqual(trade['exit_advice_diagnostic'],
                         'EXIT_ADVICE_POSITION_MISMATCH')
        # A valid advice for this exact position is accepted.
        self.assertEqual(self.book._decision_from_advice(trade, good, self.now)['action'],
                         'EXIT_ALL')

    def test_an_unsupported_action_is_refused_not_silently_applied(self):
        entry, protection, entry_order, resting = self.full_entry_protected()
        self.now += timedelta(seconds=1)
        self.step([entry_order, resting], qty='2')
        trade = self.book.state()['trades'][entry['trade_id']]
        reduce = {'action': 'REDUCE', 'reason': 'FIXTURE',
                  'as_of': self.now.isoformat(),
                  'valid_until': (self.now + timedelta(seconds=20)).isoformat(),
                  'position_version': self.book.exit_position_version(trade)}
        self.assertEqual(self.book._decision_from_advice(trade, reduce, self.now)['action'],
                         'HOLD')
        self.assertEqual(trade['manual_intervention_required'],
                         'EXIT_ADVICE_UNSUPPORTED:REDUCE')


class SecondPluginFamilyTests(PartialFlow):
    """A quotes-only plugin family composes without Martin's daily bootstrap."""

    def setUp(self):
        from unittest.mock import patch
        import test_partial_lifecycle as base

        from trading_contracts.instance import PluginManifest

        def vwap_manifest():
            return PluginManifest(plugin_id='vwap', version='1.0',
                                  state_schema='vwap-1',
                                  config={'step': '1',
                                          'cost_buffer_per_share': '0.05'},
                                  data_requirements=('quotes',),
                                  capabilities=('advisory-entry',),
                                  max_inflight=8)
        with patch.object(base, 'manifest', vwap_manifest):
            super().setUp()

    def advisor(self, spec, *, timeout=2.0):
        from trading_infra.exit_advisory import ExitAdvisoryService
        self.supervisor.exit_advisor = ExitAdvisoryService(
            {'vwap': spec}, timeout_seconds=timeout, advice_ttl_seconds=20)

    def test_a_second_plugin_enters_and_uses_its_own_exit_rule(self):
        self.assertEqual(self.registry.instance('alpha').manifest.plugin_id, 'vwap')
        entry, protection, entry_order, resting = self.full_entry_protected()
        self.assertEqual(json.loads(entry['payload'])['trade_id'],
                         entry['trade_id'])
        self.advisor('test_exit_isolation:rule_exit')
        # Tick 1 submits the bounded advisor; the owner reconciles at once.
        self.now += timedelta(seconds=1)
        self.step([entry_order, resting], qty='2')
        # Tick 2 harvests the finished advice and applies it.
        time.sleep(0.5)
        self.now += timedelta(seconds=1)
        self.step([entry_order, resting], qty='2')
        trade = self.book.state()['trades'][entry['trade_id']]
        self.assertTrue(trade['exit_requested'], (trade.get('last_exit_advice'), trade.get('exit_advice_diagnostic')))
        self.assertEqual(trade['last_exit_advice']['action'], 'EXIT')
        self.assertNotEqual(trade['last_exit_advice']['advisor_pid'], os.getpid())


class NonBlockingAdvisoryServiceTests(TestCase):
    def views(self, count):
        at = '2026-10-16T14:00:00+00:00'
        quote = {'symbol': 'DEMO', 'bid': '40', 'ask': '40.01', 'bid_at': at,
                 'ask_at': at, 'observed_at': at}
        return [{'instance_id': 'alpha', 'plugin_id': 'alpha',
                 'trade_id': 't-%d' % index, 'symbol': 'DEMO', 'quantity': '1',
                 'entry_price': '40', 'stop': '39', 'initial_stop': '39',
                 'quote': quote, 'features': {}, 'as_of': at,
                 'position_version': 'v-%d' % index} for index in range(count)]

    def test_three_hung_positions_cost_one_timeout_not_three(self):
        from trading_infra.exit_advisory import ExitAdvisoryService
        service = ExitAdvisoryService({'alpha': 'test_exit_isolation:rule_hang'},
                                      timeout_seconds=0.3)
        started = time.monotonic()
        self.assertEqual(service.submit(self.views(3)), 3)
        self.assertLess(time.monotonic() - started, 0.25,
                        'submission must not wait for any advisor')
        self.assertEqual(service.collect(), {})
        time.sleep(0.4)
        results = service.collect()
        elapsed = time.monotonic() - started
        self.assertEqual(sorted(results), ['t-0', 't-1', 't-2'])
        self.assertEqual({item['error'] for item in results.values()},
                         {'EXIT_ADVISOR_TIMEOUT'})
        self.assertLess(elapsed, 1.0,
                        'three hung positions share one deadline, not a sum')


class ConfiguredPluginExitClosedLoopTests(PartialFlow):
    """A vwap-fixture position's real feature view drives its own exit rule."""

    def setUp(self):
        from unittest.mock import patch
        import test_partial_lifecycle as base
        from trading_contracts.instance import PluginManifest

        def vwap_manifest():
            return PluginManifest(plugin_id='vwap-fixture', version='1.0',
                                  state_schema='vwap-fixture-1',
                                  config={'cost_buffer_per_share': '0.05'},
                                  data_requirements=('quotes',),
                                  capabilities=('advisory-entry',), max_inflight=8)
        with patch.object(base, 'manifest', vwap_manifest):
            super().setUp()

    def test_the_position_feature_view_and_rule_are_plugin_owned(self):
        entry, protection, entry_order, resting = self.full_entry_protected()
        current = self.registry.state('alpha') or {}
        self.registry.put_state('alpha', {**current, 'session_vwap': '99'},
                                expected=current)
        self.now += timedelta(seconds=1)
        quote = {'symbol': 'DEMO', 'bid': '39.99', 'ask': '40.00',
                 'bid_at': self.now.isoformat(), 'ask_at': self.now.isoformat(),
                 'observed_at': self.now.isoformat()}
        views = self.supervisor._exit_advice_views(
            self.snap([entry_order, resting], qty='2'), {'DEMO': quote})
        self.assertEqual(len(views), 1)
        self.assertEqual(views[0]['plugin_id'], 'vwap-fixture')
        self.assertEqual(views[0]['features'], {'session_vwap': '99'},
                         'the view carries only this plugin\'s declared feature')

        # The configured vwap rule runs in a child process on that view.
        service = ExitAdvisoryService(
            {'vwap-fixture': 'martin_luk.exit_composition:vwap_advise'},
            timeout_seconds=2)
        advice = service.advise_one(views[0])
        self.assertEqual(advice['action'], 'EXIT')
        self.assertEqual(advice['reason'], 'BELOW_SESSION_VWAP')
        self.assertNotEqual(advice['advisor_pid'], os.getpid())

        # The Book/authority re-validates the plugin advice against this exact
        # position and produces the allowed exit plan.
        trade = self.book.state()['trades'][entry['trade_id']]
        decision = self.book._decision_from_advice(trade, advice, self.book.clock())
        self.assertEqual(decision['action'], 'EXIT_ALL')
        self.assertEqual(trade['last_exit_advice']['position_version'],
                         self.book.exit_position_version(trade))
