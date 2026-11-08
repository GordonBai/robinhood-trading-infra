"""The concrete Robinhood surfaces satisfy the declared ports through adapters.

Each adapter is a thin delegation, so the tests check two things: the port is
structurally satisfied, and calling through the port produces exactly what the
concrete surface produces. No broker, no network.
"""
from unittest import TestCase

from martin_agentic.adapters import (AdapterBundle, BrokerReadPortAdapter,
                                     ExecutionPortAdapter, HostDeliveryPortAdapter,
                                     MarketDataPortAdapter)
from trading_contracts import ports


class FakeReader:
    def __init__(self):
        self.calls = []

    def fetch(self, tool, request):
        self.calls.append((tool, request))
        return {'tool': tool, 'request': request,
                'response': {'data': {'echo': tool, 'args': request}}}


class FakeJournal:
    def __init__(self, **summary):
        self._summary = summary

    def summary(self, **kwargs):
        return self._summary


class FakeInbox:
    def __init__(self):
        self.items = {}

    def get(self, key):
        return self.items.get(key)


class MarketAdapterTests(TestCase):
    def test_satisfies_the_market_data_port(self):
        self.assertIsInstance(MarketDataPortAdapter(FakeReader()), ports.MarketDataPort)

    def test_it_is_not_an_execution_surface(self):
        adapter = MarketDataPortAdapter(FakeReader())
        self.assertNotIsInstance(adapter, ports.ExecutionPort)
        self.assertNotIsInstance(adapter, ports.BrokerReadPort)

    def test_quotes_and_bars_use_the_declared_tools(self):
        reader = FakeReader()
        adapter = MarketDataPortAdapter(reader)
        adapter.quotes(['SPY', 'QQQ'])
        adapter.bars(['SPY'], interval='5minute', start='a', end='b')
        self.assertEqual([call[0] for call in reader.calls],
                         ['get_equity_quotes', 'get_equity_historicals'])
        self.assertEqual(reader.calls[0][1], {'symbols': ['SPY', 'QQQ']})
        self.assertEqual(reader.calls[1][1]['interval'], '5minute')
        self.assertEqual(reader.calls[1][1]['bounds'], 'regular')

    def test_a_surface_failure_is_not_swallowed(self):
        class Broken:
            def fetch(self, tool, request):
                raise ValueError('transport down')

        with self.assertRaises(ValueError):
            MarketDataPortAdapter(Broken()).quotes(['SPY'])

    def test_a_reader_without_fetch_is_refused(self):
        with self.assertRaises(ValueError):
            MarketDataPortAdapter(object())


class BrokerAdapterTests(TestCase):
    def test_satisfies_the_broker_read_port(self):
        self.assertIsInstance(BrokerReadPortAdapter(FakeJournal()), ports.BrokerReadPort)

    def test_missing_values_are_not_invented(self):
        adapter = BrokerReadPortAdapter(FakeJournal())
        self.assertEqual(adapter.positions(), {})
        self.assertEqual(adapter.orders(), 0)

    def test_reported_values_pass_through(self):
        adapter = BrokerReadPortAdapter(
            FakeJournal(positions={'SPY': 5}, open_equity_orders=2))
        self.assertEqual(adapter.positions(), {'SPY': 5})
        self.assertEqual(adapter.orders(), 2)


class ExecutionAdapterTests(TestCase):
    def adapter(self, **kwargs):
        return ExecutionPortAdapter(**kwargs)

    def test_satisfies_the_execution_port(self):
        self.assertIsInstance(self.adapter(operate=lambda *a: {}), ports.ExecutionPort)

    def test_queue_advancement_is_a_separate_power(self):
        calls = []
        adapter = self.adapter(advance=lambda **kw: calls.append(kw) or {'ok': True})
        self.assertEqual(adapter.advance_queue(allow_entries=True), {'ok': True})
        adapter.advance_queue()
        self.assertEqual(calls, [{'allow_entries': True}, {'allow_entries': False}])
        # Advancement is a different power and never stands in for a target.
        with self.assertRaises(ValueError) as ctx:
            adapter.cancel('ref-a')
        self.assertEqual(str(ctx.exception), 'ADAPTER_TARGETED_OPERATION_REQUIRED')
        self.assertEqual(len(calls), 2)

    def test_a_targeted_operation_without_a_target_adapter_is_refused(self):
        adapter = self.adapter(advance=lambda **kw: {'advanced': True})
        for call in (lambda: adapter.review({'action_id': 'a1'}),
                     lambda: adapter.place({'action_id': 'a1', 'allow_entries': True}),
                     lambda: adapter.cancel('a1')):
            with self.assertRaises(ValueError) as ctx:
                call()
            self.assertEqual(str(ctx.exception), 'ADAPTER_TARGETED_OPERATION_REQUIRED')

    def test_cancel_targets_exactly_one_identity(self):
        seen = []
        adapter = self.adapter(operate=lambda action_id, operation, intent:
                               seen.append((action_id, operation, intent)) or {'ok': True})
        adapter.cancel('ref-a')
        adapter.cancel('ref-b')
        self.assertEqual([item[0] for item in seen], ['ref-a', 'ref-b'])
        self.assertEqual({item[1] for item in seen}, {'cancel'})
        self.assertNotEqual(seen[0][2], seen[1][2])

    def test_review_and_place_bind_their_own_intent(self):
        seen = []
        adapter = self.adapter(operate=lambda action_id, operation, intent:
                               seen.append((action_id, operation, intent)) or {})
        adapter.review({'action_id': 'a1', 'symbol': 'SPY'})
        adapter.place({'action_id': 'a2', 'symbol': 'QQQ', 'allow_entries': True})
        self.assertEqual([item[1] for item in seen], ['review', 'place'])
        self.assertEqual([item[2]['symbol'] for item in seen], ['SPY', 'QQQ'])

    def test_place_requires_an_explicit_entry_intent(self):
        adapter = self.adapter(operate=lambda *a: {})
        with self.assertRaises(ValueError) as ctx:
            adapter.place({'action_id': 'a1'})
        self.assertEqual(str(ctx.exception), 'ADAPTER_ENTRY_INTENT_REQUIRED')

    def test_a_missing_action_id_is_refused_before_any_effect(self):
        seen = []
        adapter = self.adapter(operate=lambda *a: seen.append(a) or {})
        with self.assertRaises(ValueError) as ctx:
            adapter.review({'symbol': 'SPY'})
        self.assertEqual(str(ctx.exception), 'ADAPTER_ACTION_ID_REQUIRED')
        self.assertEqual(seen, [])

    def test_cancel_requires_a_reference(self):
        adapter = self.adapter(operate=lambda *a: {})
        with self.assertRaises(ValueError):
            adapter.cancel('')

    def test_an_adapter_with_no_callable_is_refused(self):
        with self.assertRaises(ValueError):
            ExecutionPortAdapter()


class HostAdapterTests(TestCase):
    def test_satisfies_the_host_delivery_port(self):
        inbox = FakeInbox()
        self.assertIsInstance(HostDeliveryPortAdapter(lambda i, x: {}, inbox=inbox),
                              ports.HostDeliveryPort)

    def test_a_delivery_carries_its_intent_identity(self):
        seen = []
        inbox = FakeInbox()
        adapter = HostDeliveryPortAdapter(
            lambda intent_id, intent: seen.append((intent_id, intent)) or {'delivered': True},
            inbox=inbox)
        self.assertEqual(adapter.deliver({'intent_id': 'i-1', 'symbol': 'SPY'}),
                         {'delivered': True})
        self.assertEqual(seen[0][0], 'i-1')
        self.assertEqual(seen[0][1]['symbol'], 'SPY')
        with self.assertRaises(ValueError) as ctx:
            adapter.deliver({'symbol': 'SPY'})
        self.assertEqual(str(ctx.exception), 'ADAPTER_INTENT_ID_REQUIRED')

    def test_a_receipt_is_read_back_by_intent_id(self):
        inbox = FakeInbox()
        inbox.items['intent-1'] = {'state': 'PENDING_APPROVAL'}
        adapter = HostDeliveryPortAdapter(lambda i, x: {'delivered': True}, inbox=inbox)
        adapter.deliver({'intent_id': 'intent-1'})
        self.assertEqual(adapter.receipt('intent-1'), {'state': 'PENDING_APPROVAL'})
        self.assertIsNone(adapter.receipt('missing'))

    def test_a_host_adapter_is_not_an_execution_port(self):
        adapter = HostDeliveryPortAdapter(lambda i, x: {}, inbox=FakeInbox())
        self.assertNotIsInstance(adapter, ports.ExecutionPort)

    def test_missing_wiring_is_refused(self):
        with self.assertRaises(ValueError):
            HostDeliveryPortAdapter(None, inbox=FakeInbox())
        with self.assertRaises(ValueError):
            HostDeliveryPortAdapter(lambda i, x: {}, inbox=object())


class BundleTests(TestCase):
    def test_bundle_names_the_adapters_actually_assembled(self):
        bundle = AdapterBundle(market=MarketDataPortAdapter(FakeReader()),
                               broker=BrokerReadPortAdapter(FakeJournal()))
        self.assertEqual(bundle.as_dict(), {'market': 'MarketDataPortAdapter',
                                            'broker': 'BrokerReadPortAdapter',
                                            'execution': None, 'host': None})
