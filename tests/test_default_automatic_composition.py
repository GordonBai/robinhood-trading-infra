"""Default offline composition for two strategy plugins, not a live channel."""
import json
from unittest import TestCase

from test_default_lifecycle import DefaultLifecycleTests


class DefaultAutomaticCompositionTests(TestCase):
    def test_martin_absent_profile_off_leaves_one_full_local_lifecycle(self):
        fixture = DefaultLifecycleTests('test_default_runner_reaches_authoritative_flat')
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        result, state, con, actions, trades, trace = fixture.run_default(
            cycles=120, market_exit_after=10,
            require_protection_before_exit=True,
            strategy_instances=[{'instance_id': 'vwap-live',
                                 'plugin': 'vwap-fixture'}])
        self.assertTrue(trades, (result.get('reason'), actions))
        if state.position_qty != 0:
            self.fail(('martin_absent_incomplete', {
                'phase': [trade.get('phase') for trade in trades.values()],
                'actions': [(action[0], action[1]) for action in actions],
                'position': str(state.position_qty),
                'reason': result.get('reason'),
                'errors': result.get('errors'),
                'strategy_queue': result.get('strategy_queue'),
                'dispatch': result.get('dispatch'),
            }))
        instances = {row[0] for row in con.execute(
            'SELECT instance_id FROM strategy_instances').fetchall()}
        self.assertEqual(instances, {'vwap-live'})
        self.assertNotIn('martin-observe', instances)
        self.assertNotIn('supervised_engineering', fixture.policy)
        self.assertEqual(state.position_qty, 0)
        self.assertEqual(state.open_orders(), [])
        payloads = [json.loads(action[3]) for action in actions]
        self.assertTrue(payloads)
        self.assertEqual({payload.get('purpose') for payload in payloads},
                         {'strategy'})
        self.assertFalse(any(payload.get('supervised_intent') is True
                             for payload in payloads))
        self.assertTrue(all(trade['phase'] == 'FLAT_VERIFIED'
                            for trade in trades.values()))
        self.assertEqual({action[0] for action in actions},
                         {'entry', 'protect', 'cancel', 'exit'})
        phases = []
        for sample in trace:
            for trade in sample.values():
                phase = trade.get('phase')
                if phase and (not phases or phases[-1] != phase):
                    phases.append(phase)
        self.assertIn('PROTECTED', phases, phases)
        self.assertIn('EXIT_REQUESTED', phases, phases)
        self.assertLess(phases.index('PROTECTED'),
                        phases.index('EXIT_REQUESTED'), phases)
        protect_state = next(action[1] for action in actions
                             if action[0] == 'protect')
        self.assertEqual(protect_state, 'OBSERVED')
        cancel = next(action for action in actions if action[0] == 'cancel')
        self.assertIn(cancel[1], {'OBSERVED', 'REPORTED'}, cancel)
        self.assertTrue(result['production_admission']['engineering_scope_unchanged'])
        self.assertFalse(result.get('production_authorized', True))

    def test_two_plugins_share_one_default_owner_with_profile_off(self):
        fixture = DefaultLifecycleTests('test_default_runner_reaches_authoritative_flat')
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        result, state, con, actions, trades, trace = fixture.run_default(
            cycles=120)
        self.assertTrue(trades, (result.get('reason'), actions))
        if state.position_qty != 0:
            self.fail(('martin_absent_incomplete', {
                'phase': [trade.get('phase') for trade in trades.values()],
                'actions': [(action[0], action[1]) for action in actions],
                'position': str(state.position_qty),
                'reason': result.get('reason'),
                'errors': result.get('errors'),
                'strategy_queue': result.get('strategy_queue'),
                'dispatch': result.get('dispatch'),
            }))
        instances = {row[0] for row in con.execute(
            'SELECT instance_id FROM strategy_instances').fetchall()}
        self.assertEqual(instances, {'vwap-live', 'martin-observe'})
        self.assertNotIn('engineering', {action[0] for action in actions})
        self.assertEqual({action[0] for action in actions},
                         {'entry', 'protect', 'cancel', 'exit'})
        phases = []
        for sample in trace:
            for trade in sample.values():
                phase = trade.get('phase')
                if phase and (not phases or phases[-1] != phase):
                    phases.append(phase)
        self.assertIn('PROTECTED', phases, phases)
        self.assertIn('EXIT_REQUESTED', phases, phases)
        self.assertLess(phases.index('PROTECTED'),
                        phases.index('EXIT_REQUESTED'), phases)
        protect_state = next(action[1] for action in actions
                             if action[0] == 'protect')
        self.assertEqual(protect_state, 'OBSERVED')
        cancel = next(action for action in actions if action[0] == 'cancel')
        self.assertIn(cancel[1], {'OBSERVED', 'REPORTED'}, cancel)
        owners = {row[0] for row in con.execute(
            'SELECT instance_id FROM execution_attribution').fetchall()}
        self.assertEqual(owners, {'vwap-live'})
        self.assertTrue(result['production_admission']['engineering_scope_unchanged'])
        self.assertFalse(result.get('production_authorized', True))
        self.assertTrue(result.get('synthetic_session'))
