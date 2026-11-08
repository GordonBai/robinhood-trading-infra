"""Final whole-share sizing from hard infrastructure limits, never a plugin order."""
from decimal import ROUND_FLOOR

from trading_contracts.values import number


def size_entry(*, entry_ceiling, stop, cost_buffer_per_share,
               max_position_notional, max_planned_loss, quantity_limit=None):
    ceiling, structural_stop, cost = map(number, (entry_ceiling, stop, cost_buffer_per_share))
    notional, risk = map(number, (max_position_notional, max_planned_loss))
    if not 0 < structural_stop < ceiling or min(cost, notional, risk) < 0:
        raise ValueError('INVALID_INFRA_SIZING_LIMITS')
    quantity = int(min(notional / ceiling, risk / (ceiling - structural_stop + cost))
        .to_integral_value(rounding=ROUND_FLOOR))
    if quantity_limit is not None:
        if type(quantity_limit) is not int or quantity_limit <= 0:
            raise ValueError('WHOLE_SHARE_ADVISORY_LIMIT_REQUIRED')
        quantity = min(quantity, quantity_limit)
    return quantity
