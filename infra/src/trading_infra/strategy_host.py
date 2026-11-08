"""Pure synchronous contract boundary; durable inbox ownership stays with caller.

No database, account or broker object is passed to the plugin. Process/time
isolation is a subsequent migration and is not claimed by this adapter.
"""
from trading_contracts.strategy import StrategyDecision, StrategyInput


def evaluate_strategy(plugin, request):
    if not isinstance(request, StrategyInput):
        raise ValueError('STRATEGY_HOST_INPUT_CONTRACT_REQUIRED')
    try:
        decision = plugin.decide(request)
    except (ValueError, KeyError, TypeError):
        # Preserve existing input-integrity classifications; the owner uses
        # some exact reasons to inhibit new entries while managing positions.
        raise
    except Exception:
        # Unexpected advisory faults must reach the owner's existing strategy
        # rejection path, not escape before fresh broker reconciliation. No
        # arbitrary exception text crosses the boundary. Process-control
        # BaseExceptions remain visible to the caller.
        raise ValueError('STRATEGY_PLUGIN_EVALUATION_FAILED') from None
    if not isinstance(decision, StrategyDecision):
        raise ValueError('STRATEGY_HOST_DECISION_CONTRACT_REQUIRED')
    return decision.validate_for(request)
