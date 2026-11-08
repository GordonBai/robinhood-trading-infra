"""Deterministic long-only whole-share sizing. Prices are worst-case assumptions."""

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_FLOOR


def number(value: object) -> Decimal:
    if isinstance(value, (float, bool)):
        raise ValueError("Use decimal strings or integers, not floats/bools")
    try:
        result = Decimal(value)
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("Invalid decimal value") from exc
    if not result.is_finite():
        raise ValueError("Decimal must be finite")
    return result


@dataclass(frozen=True)
class RiskPolicy:
    risk_fraction: Decimal
    max_position_fraction: Decimal

    def __post_init__(self):
        for field in (self.risk_fraction, self.max_position_fraction):
            if not isinstance(field, Decimal) or not field.is_finite() or not 0 < field <= 1:
                raise ValueError("Risk and position fractions must be Decimal in (0, 1]")


def size_position(*, equity: Decimal, available_buying_power: Decimal,
                  entry_ceiling: Decimal, stop: Decimal,
                  cost_buffer_per_share: Decimal, remaining_portfolio_risk: Decimal,
                  policy: RiskPolicy, halted: bool = False) -> dict:
    values = (equity, available_buying_power, entry_ceiling, stop,
              cost_buffer_per_share, remaining_portfolio_risk)
    if any(not isinstance(v, Decimal) or not v.is_finite() for v in values):
        raise ValueError("All sizing values must be finite Decimals")
    if equity <= 0 or not 0 < stop < entry_ceiling:
        raise ValueError("Require positive equity and 0 < stop < entry ceiling")
    if min(available_buying_power, cost_buffer_per_share, remaining_portfolio_risk) < 0:
        raise ValueError("Buying power, cost buffer and remaining risk cannot be negative")
    unit_risk = entry_ceiling - stop + cost_buffer_per_share
    # Conservatively reserve the full buffer as cash as well as stop-risk budget.
    unit_cash = entry_ceiling + cost_buffer_per_share
    risk_budget = equity * policy.risk_fraction
    caps = {
        "trade_risk": risk_budget / unit_risk,
        "position_weight": equity * policy.max_position_fraction / entry_ceiling,
        "buying_power": available_buying_power / unit_cash,
        "portfolio_risk": remaining_portfolio_risk / unit_risk,
    }
    whole_caps = {key: int(value.to_integral_value(rounding=ROUND_FLOOR))
                  for key, value in caps.items()}
    quantity = 0 if halted else min(whole_caps.values())
    return {
        "decision": "BLOCKED" if halted else "ELIGIBLE" if quantity else "SKIP",
        "reason": "DRAWDOWN_HALT" if halted else "SIZED" if quantity else "NO_WHOLE_SHARE",
        "quantity": quantity,
        "caps": whole_caps,
        "risk_budget": str(risk_budget),
        "estimated_stop_loss_including_buffer": str(quantity * unit_risk),
        "notional": str(quantity * entry_ceiling),
        "cash_reservation": str(quantity * unit_cash),
        "live_order": False,
    }
