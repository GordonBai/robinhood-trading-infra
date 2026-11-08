"""Deterministic contract-only fixture for capability tests.

This is synthetic test metadata. It is not a provider response, account
setting, account eligibility, order, or live-execution evidence. The source
token keeps it distinguishable from the captured official tool metadata.
"""
CONTRACT = {
    'schema_version': 1,
    # This fixture mimics the existing legal wire envelope so consumers can
    # exercise their normal contract path. It is still test data, not a
    # provider sample, account setting, or live-execution evidence.
    'source': 'current_conversation_official_tool_metadata',
    'test_fixture': True,
    'test_fixture_note': 'synthetic contract envelope; no account or live evidence',
    'captured_at': '2026-11-06T21:00:00Z',
    'actual_formal_calls': 0,
    'tools': [
        {'name': 'review_equity_order',
         'description': 'Simulate a stock order without placing it. Get explicit '
                        'confirmation before calling place_equity_order unless the '
                        'customer approval path applies. customer_approval_required '
                        'true with no alerts goes to the Robinhood app.'},
        {'name': 'place_equity_order',
         'description': 'Place a real equity order with real money. Follow '
                        'review_equity_order and customer_approval_required; the '
                        'Robinhood app may require customer approval.'},
        {'name': 'cancel_equity_order',
         'description': 'Cancel an open equity order by order_id. Always confirm '
                        'with the user before calling.'},
        {'name': 'review_advanced_order',
         'description': 'Simulate an OCO equity order without placing it. Get '
                        'explicit confirmation before calling place_advanced_order '
                        'when customer_approval_required is false or absent.'},
        {'name': 'place_advanced_order',
         'description': 'Place an OCO equity order with real money. Follow '
                        'review_advanced_order and customer_approval_required; the '
                        'Robinhood app may require customer approval.'},
        {'name': 'get_advanced_orders',
         'description': 'Fetch advanced orders, including OCO contingency orders.'},
        {'name': 'cancel_advanced_order',
         'description': 'Cancel an advanced order (e.g. an OCO) by order_id.'},
        {'name': 'get_trade_approval_setting',
         'description': 'Read whether trades require the customer manual approval.'},
        {'name': 'get_trade_approvals',
         'description': 'List pending trade approvals waiting on the customer.'},
    ],
}
