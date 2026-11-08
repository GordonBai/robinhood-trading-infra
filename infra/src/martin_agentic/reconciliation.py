"""Read-only identity and term reconciliation; never infers ownership by ticker."""
from .risk import number


def matches_order_identity(action_id,order,*,logical_and_approval_refs,exact_order_owner,account_key):
    by_ref=order.get('ref_id') in logical_and_approval_refs
    if exact_order_owner and exact_order_owner[1]!=account_key:
        raise ValueError('BROKER_ORDER_ID_ACCOUNT_MISMATCH')
    if exact_order_owner and exact_order_owner[0]!=action_id:
        if by_ref:raise ValueError('BROKER_ORDER_ID_REF_OWNER_CONFLICT')
        return False
    return bool(exact_order_owner) or by_ref


def check_order_terms(order,payload):
    expected=payload['parameters']
    if (order['symbol']!=expected['symbol'] or order['side']!=expected['side']
            or number(order['quantity'])!=number(expected['quantity'])):
        raise ValueError('BROKER_ORDER_DOES_NOT_MATCH_INTENT')
    if (order['type']!=expected['type'] or order['market_hours']!=expected['market_hours']
            or order['time_in_force']!=expected['time_in_force']
            or order['type']=='limit' and (order.get('limit_price') is None or number(order['limit_price'])!=number(expected['limit_price']))
            or order['type']=='stop_market' and (order.get('stop') is None or number(order['stop'])!=number(expected['stop_price']))):
        raise ValueError('BROKER_ORDER_TERMS_MISMATCH')
