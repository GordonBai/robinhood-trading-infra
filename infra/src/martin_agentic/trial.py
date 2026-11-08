"""Date-scoped proposal checks for a user-approved supervised trial.

These checks do not submit, approve, or qualify an order for execution.
"""

from zoneinfo import ZoneInfo

from .calendar import is_regular
from .ledger import timestamp
from .risk import number


def trial_proposal(policy, broker, quote, plan, *, as_of):
    now = timestamp(as_of)
    errors = []
    confirmation_contract = policy.get('confirmation_policy') == 'follow_current_tool_contract' or (
        'confirmation_policy' not in policy and policy.get('per_order_user_approval_required') is True)
    if policy.get('user_confirmed_test_limits') is not True or not confirmation_contract:
        errors.append('CONFIRMED_TRIAL_POLICY_AND_TOOL_CONTRACT_REQUIRED')
    if now.astimezone(ZoneInfo('America/New_York')).date().isoformat() != policy['session_date']:
        errors.append('OUTSIDE_AUTHORIZED_TRIAL_DATE')
    if broker.get('account') != '••••' + policy['account_suffix']:
        errors.append('TRIAL_ACCOUNT_MISMATCH')
    if not is_regular(as_of):
        errors.append('OUTSIDE_REGULAR_SESSION')
    if broker.get('read_healthy') is not True or broker.get('new_risk_blocked') is not False:
        errors.append('BROKER_OBSERVATION_NOT_READY')
    if not 0 <= (now-timestamp(broker['last_observed_at'])).total_seconds() <= 30:
        errors.append('BROKER_OBSERVATION_STALE')
    if broker.get('positions') or broker.get('open_equity_orders') or broker.get('other_asset_open_orders'):
        errors.append('FIRST_TRIAL_REQUIRES_FLAT_ACCOUNT_WITHOUT_ORDERS')
    equity = number(broker['portfolio']['total_value'])
    start = number(policy['account_capital_usd'])
    day_start = number(broker.get('risk_state',{}).get('day_start_equity', str(start)))
    loss_limit = number(policy['stop_new_entries_at_daily_equity_loss_usd'])
    if equity <= day_start - loss_limit:
        errors.append('TRIAL_DAILY_LOSS_LIMIT_REACHED')
    if number(broker['portfolio']['pending_deposits']) != 0:
        errors.append('UNSETTLED_DEPOSIT_REQUIRES_REVIEW')
    if plan['symbol'] != quote['symbol']:
        errors.append('QUOTE_SYMBOL_MISMATCH')
    for key in ('bid_at','ask_at','observed_at'):
        if not 0 <= (now-timestamp(quote[key])).total_seconds() <= 15:
            errors.append('QUOTE_STALE_OR_FUTURE')
    bid, ask = number(quote['bid']), number(quote['ask'])
    entry, stop, cost = (number(plan[k]) for k in ('entry_ceiling','stop','cost_buffer_per_share'))
    quantity = plan['quantity']
    if type(quantity) is not int or quantity <= 0 or not 0 < stop < bid <= ask <= entry or cost < 0:
        raise ValueError('Invalid whole-share proposal or quote geometry')
    notional = quantity * entry
    risk = quantity * (entry-stop+cost)
    if notional > number(policy['max_position_notional_usd']):
        errors.append('TRIAL_POSITION_NOTIONAL_LIMIT')
    if risk > number(policy['max_planned_loss_per_trade_usd']):
        errors.append('TRIAL_PLANNED_RISK_LIMIT')
    cash_limit = min(number(broker['portfolio']['cash']), number(broker['buying_power']),
                     number(broker['unleveraged_buying_power']), start)
    if notional+quantity*cost > cash_limit:
        errors.append('TRIAL_UNLEVERAGED_CASH_LIMIT')
    return {'mode':'supervised_trial_proposal','symbol':plan['symbol'],'quantity':quantity,
            'planned_notional_usd':str(notional),'planned_risk_with_cost_buffer_usd':str(risk),
            'within_trial_limits':not errors,'blockers':sorted(set(errors)),
            'specific_order_approved':False,'broker_review_required':True,
            'additional_project_confirmation_required':policy.get('per_order_user_approval_required') is True,
            'input_authenticity_verified':False,
            'protection_confirmation_path_required':True,'execution_ready':False,'live_orders':0}
