"""Martin application exit catalog over the generic registry.

Martin's own rule stays here; the example plugins' rules are owned by the
examples application. The infrastructure only receives spec strings by id.
"""
from trading_example_plugins.rules import (
    EXIT_RULES as _EXAMPLE_RULES,
    FEATURE_KEYS as _EXAMPLE_FEATURE_KEYS,
    vwap_advise,
)
from trading_infra.exit_composition import ExitRuleRegistry
from trading_infra.exit_policy import evaluate_exit
from trading_strategies.martin.exits import MartinExitStrategy


def martin_advise(view):
    """Pure Martin exit advice, executed inside the bounded advisor process."""
    position = {'stop': view['stop'], 'initial_stop': view['initial_stop'],
                'entry_price': view['entry_price']}
    decision = evaluate_exit(position, view['quote'], as_of=view['as_of'],
                             strategy=MartinExitStrategy(),
                             features=view.get('features') or {},
                             allow_overnight=False)
    mapped = {'EXIT_ALL': 'EXIT', 'REDUCE': 'REDUCE',
              'RAISE_STOP': 'TIGHTEN_PROTECTION', 'HOLD': 'HOLD',
              'WAIT': 'HOLD'}
    if decision.get('action') not in mapped:
        return {'action': 'HOLD', 'reason': 'UNSUPPORTED_EXIT_ACTION'}
    return {'action': mapped[decision['action']],
            'reason': decision.get('reason', decision['action']),
            'suggested_stop': decision.get('stop')}


EXIT_RULES = {'martin': 'martin_luk.exit_composition:martin_advise',
              **_EXAMPLE_RULES}
FEATURE_KEYS = {'martin': ('ema9',), **_EXAMPLE_FEATURE_KEYS}
_REGISTRY = ExitRuleRegistry(EXIT_RULES, FEATURE_KEYS)


def feature_keys_for(plugin_id):
    return _REGISTRY.feature_keys_for(plugin_id)


def specs_for(plugin_ids):
    return _REGISTRY.specs_for(plugin_ids)
