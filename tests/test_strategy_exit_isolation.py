"""Actual legacy call path parity and independent plugin/safety boundaries."""
import ast
import json
from datetime import timedelta
from decimal import Decimal
import importlib.util
from itertools import product
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from martin_luk.exits import exit_decision
from trading_contracts import ExitAction, ExitRecommendation
from trading_infra.exit_policy import evaluate_exit


class StrategyExitIsolationTests(TestCase):
    def quote(self, price='43', at='2026-10-28T15:00:00+00:00'):
        return dict(bid=price, ask=str(Decimal(price)+Decimal('.01')),
            bid_at=at, ask_at=at, observed_at=at)

    def test_existing_call_path_matches_frozen_pre_migration_behavior(self):
        spec = importlib.util.spec_from_file_location('frozen_exit', 'tests/fixtures/legacy_exit_v1.py')
        legacy = importlib.util.module_from_spec(spec); spec.loader.exec_module(legacy)
        for price, stop, ema, at, overnight, earnings in product(
                ['38', '39', '40', '42.99', '43', '44'], ['39', '40'],
                [None, '42', '45'], ['2026-10-28T15:00:00+00:00',
                '2026-10-28T19:56:00+00:00', '2026-10-28T21:00:00+00:00'],
                [False, True], [None, '2026-10-28', '2026-10-29']):
            position = dict(entry_price='40', initial_stop='39', stop=stop)
            kwargs = dict(as_of=at, daily_ema9=ema, allow_overnight=overnight, earnings_date=earnings)
            self.assertEqual(exit_decision(position, self.quote(price, at), **kwargs),
                legacy.exit_decision(position, self.quote(price, at), **kwargs))

    def test_second_plugin_is_used_and_receives_no_execution_or_mutable_state(self):
        class OtherStrategy:
            def decide_exit(inner, context):
                with self.assertRaises(TypeError): context.features['x'] = Decimal(1)
                self.assertFalse(hasattr(context, 'book'))
                return ExitRecommendation(ExitAction.EXIT_ALL, 'OTHER_STRATEGY_REASON')
        result = evaluate_exit(dict(entry_price='40', initial_stop='39', stop='39'),
            self.quote(), as_of='2026-10-28T15:00:00+00:00', strategy=OtherStrategy())
        self.assertEqual(result, dict(action='EXIT_ALL', reason='OTHER_STRATEGY_REASON', price='43'))

    def test_safety_stops_and_close_do_not_depend_on_plugin_health(self):
        class Broken:
            def decide_exit(self, context): raise AssertionError('safety must run first')
        position = dict(entry_price='40', initial_stop='39', stop='39')
        self.assertEqual(evaluate_exit(position, self.quote('38'),
            as_of='2026-10-28T15:00:00+00:00', strategy=Broken())['reason'], 'STRUCTURAL_STOP')
        at = '2026-10-28T19:56:00+00:00'
        self.assertEqual(evaluate_exit(position, self.quote(at=at), as_of=at,
            strategy=Broken())['reason'], 'INTRADAY_CLOSE_POLICY')
        with self.assertRaisesRegex(ValueError, 'stale'):
            evaluate_exit(position, self.quote(), as_of='2026-10-28T15:00:16+00:00', strategy=Broken())

    def test_plugin_cannot_loosen_or_cross_current_protection(self):
        class Unsafe:
            def decide_exit(self, context):
                return ExitRecommendation(ExitAction.RAISE_STOP, 'unsafe', Decimal('38'))
        result = evaluate_exit(dict(entry_price='40', initial_stop='39', stop='39'), self.quote(),
            as_of='2026-10-28T15:00:00+00:00', strategy=Unsafe())
        self.assertEqual(result['action'], 'HOLD')
        self.assertEqual(result['strategy_diagnostic']['code'], 'EXIT_STRATEGY_CANNOT_LOOSEN_OR_CROSS_STOP')

    def test_ema_exit_preserves_legacy_short_circuit_without_entry_basis(self):
        position = {'stop': '39'}
        self.assertEqual(exit_decision(position, self.quote('40'),
            as_of='2026-10-28T15:00:00+00:00', daily_ema9='41'),
            {'action': 'EXIT_ALL', 'reason': 'PRIOR_DAILY_EMA9_BREAK', 'price': '40'})
        # An absent entry basis cannot invent a risk multiple or tighten a stop.
        unavailable = exit_decision(position, self.quote('43'), as_of='2026-10-28T15:00:00+00:00')
        self.assertEqual(unavailable['action'], 'HOLD')
        self.assertEqual(unavailable['strategy_diagnostic']['code'], 'EXIT_STRATEGY_EXCEPTION')

    def test_crashed_or_invalid_plugin_returns_bounded_diagnostic_without_masking_process_control(self):
        position = dict(entry_price='40', initial_stop='39', stop='39')
        class Broken:
            def decide_exit(self, context): raise RuntimeError('private-unbounded-message-' * 10000)
        class Invalid:
            def decide_exit(self, context): return {'action': 'EXIT_ALL'}
        for plugin, code in [(Broken(), 'EXIT_STRATEGY_EXCEPTION'),
                             (Invalid(), 'EXIT_STRATEGY_RECOMMENDATION_REQUIRED')]:
            result = evaluate_exit(position, self.quote(), as_of='2026-10-28T15:00:00+00:00', strategy=plugin)
            self.assertEqual(result['action'], 'HOLD')
            self.assertEqual(result['strategy_diagnostic']['code'], code)
            self.assertLess(len(str(result['strategy_diagnostic'])), 200)
            self.assertNotIn('private-unbounded', str(result))
        class Interrupted:
            def decide_exit(self, context): raise KeyboardInterrupt()
        with self.assertRaises(KeyboardInterrupt):
            evaluate_exit(position, self.quote(), as_of='2026-10-28T15:00:00+00:00', strategy=Interrupted())

    def test_advisory_input_failure_does_not_mask_hard_quote_or_stop_validation(self):
        class Unused:
            def decide_exit(self, context): raise AssertionError('invalid features never reach plugin')
        position = dict(entry_price='40', initial_stop='39', stop='39')
        result = evaluate_exit(position, self.quote(), as_of='2026-10-28T15:00:00+00:00',
            strategy=Unused(), features={'bad': 'not-a-decimal'})
        self.assertEqual(result['action'], 'HOLD')
        self.assertEqual(result['strategy_diagnostic']['code'], 'EXIT_STRATEGY_INPUT_INVALID')
        with self.assertRaises(ValueError):
            evaluate_exit({**position, 'stop': 'invalid'}, self.quote(),
                as_of='2026-10-28T15:00:00+00:00', strategy=Unused())

    def test_actual_legacy_reconcile_keeps_protection_when_plugin_crashes(self):
        import test_execution
        fixture = test_execution.ExecutionTests('test_empty_queue_has_zero_agent_calls')
        fixture.setUp(); self.addCleanup(fixture.tearDown)
        buy = fixture.entry_filled()
        fixture.now += timedelta(seconds=1)
        class Broken:
            def decide_exit(self, context): raise RuntimeError('fixture plugin crash')
        with patch('martin_luk.exits.MartinExitStrategy', return_value=Broken()):
            fixture.book.reconcile(fixture.snap([buy], qty='2'), quotes={'DEMO': fixture.quote})
        protection = [a for a in fixture.book.actions() if a['kind'] == 'protect']
        self.assertEqual(len(protection), 1)
        self.assertEqual(protection[0]['state'], 'READY')
        self.assertEqual(fixture.book.state()['last_snapshot'], fixture.now.isoformat())
        diagnostics=[json.loads(row[0]) for row in fixture.book.db.execute('SELECT payload FROM audit_events')
            if json.loads(row[0]).get('details',{}).get('status')=='STRATEGY_ADVISORY_UNAVAILABLE']
        self.assertEqual(len(diagnostics),1)
        self.assertEqual(diagnostics[0]['details']['diagnostic']['code'],'EXIT_STRATEGY_EXCEPTION')

    def test_rejected_advice_cannot_cancel_an_observed_effective_stop(self):
        import test_execution
        fixture = test_execution.ExecutionTests('test_empty_queue_has_zero_agent_calls')
        fixture.setUp(); self.addCleanup(fixture.tearDown)
        buy = fixture.entry_filled()
        protect = fixture.book.claim()
        fixture.book.report(protect['action_id'], {'status': 'submitted'})
        stop = fixture.bound(protect)
        fixture.book.reconcile(fixture.snap([buy, stop], qty='2', held='2'))
        fixture.now += timedelta(seconds=1)
        class Unsafe:
            def decide_exit(self, context):
                return ExitRecommendation(ExitAction.RAISE_STOP, 'unsafe', Decimal('38'))
        with patch('martin_luk.exits.MartinExitStrategy', return_value=Unsafe()):
            fixture.book.reconcile(fixture.snap([buy, stop], qty='2', held='2'),
                quotes={'DEMO': fixture.quote})
        trade = fixture.book.state()['trades']['signal-1']
        self.assertEqual(trade['stop'], '39')
        self.assertEqual(trade['phase'], 'PROTECTED')
        self.assertFalse(any(a['kind'] == 'cancel' for a in fixture.book.actions()))

    def test_dependency_direction_is_checked_not_only_directory_names(self):
        for root in ('infra/src/trading_contracts', 'strategy/martin_luk/src/trading_strategies'):
            for path in Path(root).rglob('*.py'):
                for node in ast.walk(ast.parse(path.read_text())):
                    modules = ([node.module or ''] if isinstance(node, ast.ImportFrom)
                               else [x.name for x in node.names] if isinstance(node, ast.Import) else [])
                    for module in modules:
                        self.assertFalse(module.startswith(('martin_agentic', 'trading_infra')), (path, module))
        for path in Path('infra/src/trading_infra').rglob('*.py'):
            for node in ast.walk(ast.parse(path.read_text())):
                modules = ([node.module or ''] if isinstance(node, ast.ImportFrom)
                           else [x.name for x in node.names] if isinstance(node, ast.Import) else [])
                for module in modules:
                    self.assertFalse(module.startswith('trading_strategies'), (path, module))
