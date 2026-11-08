"""A second, non-Martin strategy composes through the same infrastructure.

The point of this file is the separation claim: swapping the strategy requires
no change to the execution core, and the core never has to learn an indicator
name. The fixture strategy uses a feature Martin does not use (VWAP) and a
feature key the infrastructure has never seen.
"""
import ast
from decimal import Decimal
from pathlib import Path
from unittest import TestCase

from martin_luk.exits import exit_decision
from trading_contracts.exits import ExitAction, ExitContext, ExitRecommendation
from trading_infra.exit_policy import evaluate_exit

ROOT = Path(__file__).resolve().parents[1]
AT = '2026-10-29T15:00:00+00:00'
POSITION = {'stop': '39.00', 'initial_stop': '39.00', 'entry_price': '40.00'}
QUOTE = {'bid': '41.00', 'ask': '41.02', 'bid_at': AT, 'ask_at': AT, 'observed_at': AT}


class VwapExitStrategy:
    """Deterministic fixture: exit when price is back below the session VWAP."""

    def decide_exit(self, context):
        vwap = context.features.get('session_vwap')
        if vwap is None:
            return ExitRecommendation(ExitAction.HOLD, 'NO_VWAP')
        if context.bid < vwap:
            return ExitRecommendation(ExitAction.EXIT_ALL, 'BELOW_SESSION_VWAP')
        return ExitRecommendation(ExitAction.HOLD, 'ABOVE_SESSION_VWAP')


class SecondPluginTests(TestCase):
    def test_a_foreign_feature_key_reaches_a_foreign_strategy(self):
        decision = evaluate_exit(POSITION, QUOTE, as_of=AT, strategy=VwapExitStrategy(),
                                 features={'session_vwap': '41.50'})
        self.assertEqual(decision['action'], 'EXIT_ALL')
        self.assertEqual(decision['reason'], 'BELOW_SESSION_VWAP')

    def test_the_same_input_holds_when_the_feature_clears(self):
        decision = evaluate_exit(POSITION, QUOTE, as_of=AT, strategy=VwapExitStrategy(),
                                 features={'session_vwap': '40.50'})
        self.assertEqual(decision['action'], 'HOLD')

    def test_missing_features_hold_rather_than_invent_a_basis(self):
        decision = evaluate_exit(POSITION, QUOTE, as_of=AT, strategy=VwapExitStrategy(),
                                 features={})
        self.assertEqual(decision['action'], 'HOLD')

    def test_infrastructure_safety_rules_still_win_over_the_strategy(self):
        """A structural stop is the infrastructure's own rule, not the plugin's."""
        decision = evaluate_exit({'stop': '41.50', 'initial_stop': '41.50', 'entry_price': '40.00'},
                                 QUOTE, as_of=AT, strategy=VwapExitStrategy(),
                                 features={'session_vwap': '40.50'})
        self.assertEqual(decision['action'], 'EXIT_ALL')
        self.assertEqual(decision['reason'], 'STRUCTURAL_STOP')

    def test_composition_layer_accepts_any_strategy(self):
        decision = exit_decision(POSITION, QUOTE, as_of=AT, strategy=VwapExitStrategy(),
                                 features={'session_vwap': '41.50'})
        self.assertEqual(decision['reason'], 'BELOW_SESSION_VWAP')

    def test_a_raising_strategy_is_isolated_not_fatal(self):
        class Broken:
            def decide_exit(self, context):
                raise RuntimeError('fixture fault')

        decision = evaluate_exit(POSITION, QUOTE, as_of=AT, strategy=Broken(), features={})
        self.assertEqual(decision['action'], 'HOLD')
        self.assertEqual(decision['reason'], 'STRATEGY_EXIT_UNAVAILABLE')
        self.assertEqual(decision['strategy_diagnostic']['exception_type'], 'RuntimeError')

    def test_a_strategy_cannot_loosen_the_stop(self):
        class Loosening:
            def decide_exit(self, context):
                return ExitRecommendation(ExitAction.RAISE_STOP, 'LOOSEN', Decimal('38.00'))

        decision = evaluate_exit(POSITION, QUOTE, as_of=AT, strategy=Loosening(), features={})
        self.assertEqual(decision['action'], 'HOLD')
        self.assertEqual(decision['reason'], 'STRATEGY_EXIT_UNAVAILABLE')
        self.assertEqual(decision['strategy_diagnostic']['code'],
                         'EXIT_STRATEGY_CANNOT_LOOSEN_OR_CROSS_STOP')


class CoreKnowsNoIndicatorTests(TestCase):
    """The execution core must not name any strategy term."""

    TERMS = ('ema9', 'ema21', 'vwap', 'pullback', '3r', 'macd', 'rsi')

    def offenders(self, relative_path):
        text = (ROOT / relative_path).read_text()
        tree = ast.parse(text)
        names = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Name):
                names.add(node.id.lower())
            elif isinstance(node, ast.Attribute):
                names.add(node.attr.lower())
            elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                names.add(node.value.lower())
        return sorted(term for term in self.TERMS if term in names)

    def test_execution_book_names_no_indicator(self):
        self.assertEqual(self.offenders('infra/src/martin_agentic/execution.py'), [])

    def test_exit_policy_names_no_indicator(self):
        self.assertEqual(self.offenders('infra/src/trading_infra/exit_policy.py'), [])

    def test_sizing_names_no_indicator(self):
        self.assertEqual(self.offenders('infra/src/trading_infra/sizing.py'), [])

    def test_execution_book_hands_features_over_opaquely(self):
        text = (ROOT / 'infra/src/martin_agentic/execution.py').read_text()
        self.assertIn('features=features.get(symbol)', text)
