"""A qualified strategy entry may use the native channel, by its own evidence.

The qualification is a durable admission fact written while the session allowed
strategy entries. Purpose or an ID cannot substitute for it, the native channel
re-validates the terms, and the official approval alias / broker order still
decide what actually happens.
"""
import json
from datetime import timedelta
from unittest import TestCase

from martin_agentic.native_operations import NativeOperations
from test_approval_unknown_lifecycle import ApprovalFlow, NoClaimDispatcher


class QualifiedStrategyEntryTests(ApprovalFlow):
    def setUp(self):
        super().setUp()
        self.supervisor.dispatcher = NoClaimDispatcher()
        self.entry = self.make_entry()
        self.trade = self.book.state()['trades'][self.entry['trade_id']]
        self.assertIs(self.trade['evidence'].get('qualified_strategy_entry'), True)
        self.ops = NativeOperations(self.book)

    def quote(self):
        at = self.now.isoformat()
        return {'symbol': 'DEMO', 'bid': '39.99', 'ask': '40.00',
                'bid_at': at, 'ask_at': at, 'observed_at': at}

    def review_response(self, packet):
        return {'data': {**packet['parameters'], 'order_checks': {},
                         'customer_approval_required': False,
                         'market_data_disclosure': 'Exact disclosure',
                         'quote_data': {}}}

    def official_approval(self, state='APPROVAL_STATE_PENDING'):
        return {'structuredContent': {'data': {'order': None, 'approval': {
            'approval_id': 'official-approval', 'asset_class': 'ASSET_CLASS_EQUITY',
            'state': state, 'is_edited': False},
            'guide': 'The order was NOT placed.'}}}

    def placed_with_approval(self):
        snapshot = self.snap([], qty='0')
        packet = self.ops.begin(self.entry['id'], 'review', snapshot, quote=self.quote())
        self.ops.record(packet['operation_id'], self.review_response(packet))
        place = self.ops.begin(self.entry['id'], 'place', snapshot, quote=self.quote())
        result = self.ops.record(place['operation_id'], self.official_approval())
        self.assertEqual(result['report']['status'], 'pending_approval')
        action = next(a for a in self.book.actions() if a['id'] == self.entry['id'])
        self.assertEqual(action['state'], 'REPORTED')
        self.assertEqual(self.accounting.fills('alpha'), [])
        return place

    def test_the_qualified_entry_reaches_native_review_and_approval(self):
        self.placed_with_approval()
        aliases = {row[0] for row in self.db.execute(
            'SELECT ref_id FROM external_broker_refs WHERE action_id=?',
            (self.entry['id'],))}
        self.assertIn('official-approval', aliases)
        # The approval state alone is not a fill.
        self.assertEqual(self.accounting.fills('alpha'), [])
        self.assertIn(self.book.state()['trades'][self.entry['trade_id']]['phase'],
                      {'ENTRY_PLANNED','ENTRY_AWAITING_BROKER'})

    def test_the_late_broker_order_under_the_alias_books_once(self):
        self.placed_with_approval()
        fill = self.execution('exec-entry-1', 2)
        self.pay_executions([fill])
        self.now += timedelta(seconds=2)
        order = self.order_for(self.entry, filled=2, state='filled',
                               executions=[fill])
        order['ref_id'] = 'official-approval'
        self.step([order], qty='2', ticks=2)
        self.assertEqual([(item['side'], item['quantity'])
                          for item in self.accounting.fills('alpha')],
                         [('buy', '2')])
        owner = self.feedback.owner('official-approval')
        self.assertEqual(owner['instance_id'], 'alpha')
        trade = self.book.state()['trades'][self.entry['trade_id']]
        self.assertEqual(trade['quantity'], '2')
        self.assertIsNotNone(self.action('protect'))

    def test_an_unknown_place_is_isolated_and_never_re_sent(self):
        snapshot = self.snap([], qty='0')
        packet = self.ops.begin(self.entry['id'], 'review', snapshot, quote=self.quote())
        self.ops.record(packet['operation_id'], self.review_response(packet))
        place = self.ops.begin(self.entry['id'], 'place', snapshot, quote=self.quote())
        result = self.ops.record(place['operation_id'],
                                 {'isError': True,
                                  'content': [{'type': 'text', 'text': 'timeout'}]})
        self.assertEqual(result['report']['status'], 'unknown')
        action = next(a for a in self.book.actions() if a['id'] == self.entry['id'])
        self.assertEqual(action['state'], 'UNKNOWN')
        for _ in range(3):
            self.now += timedelta(seconds=1)
            self.step([], ticks=1)
        entries = [a for a in self.book.actions() if a['kind'] == 'entry']
        self.assertEqual(len(entries), 1)
        self.assertEqual(next(a for a in entries)['state'], 'UNKNOWN')
        self.assertEqual(self.accounting.fills('alpha'), [])
