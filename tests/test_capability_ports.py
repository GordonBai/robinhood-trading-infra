"""Capability negotiation and the typed port boundaries.

A port is a structural protocol; a fixture adapter satisfies it without knowing
any broker field name. Negotiation refuses a missing capability instead of
degrading silently.
"""
from unittest import TestCase

from trading_contracts import ports


class MatrixTests(TestCase):
    def matrix(self, **overrides):
        fields = dict(adapter_id='fake-replay', provides=('quotes', 'bars'),
                      confirmation='not_required', continuous_authorisation=False,
                      synthetic=True)
        fields.update(overrides)
        return ports.CapabilityMatrix(**fields)

    def test_supports_is_exact_membership(self):
        matrix = self.matrix()
        self.assertTrue(matrix.supports('quotes'))
        self.assertFalse(matrix.supports('execution'))

    def test_require_refuses_any_missing_capability(self):
        with self.assertRaises(ValueError) as ctx:
            self.matrix().require(['quotes', 'execution'])
        self.assertEqual(str(ctx.exception), 'CAPABILITY_NOT_PROVIDED:execution')
        matrix = self.matrix()
        self.assertIs(matrix.require(['quotes']), matrix)

    def test_duplicate_or_empty_provides_is_refused(self):
        with self.assertRaises(ValueError):
            self.matrix(provides=('quotes', 'quotes'))
        with self.assertRaises(ValueError):
            self.matrix(provides=())

    def test_unknown_confirmation_state_is_refused(self):
        with self.assertRaises(ValueError):
            self.matrix(confirmation='assumed')

    def test_flags_must_be_real_booleans(self):
        for name in ('continuous_authorisation', 'synthetic'):
            with self.assertRaises(ValueError):
                self.matrix(**{name: 1})

    def test_matrix_is_immutable(self):
        matrix = self.matrix()
        with self.assertRaises(Exception):
            matrix.confirmation = 'granted'

    def test_assess_refuses_unmet_requirements(self):
        with self.assertRaises(ValueError):
            ports.assess('robinhood-shadow', provides=['quotes'],
                         requires=['quotes', 'continuous_authorisation'],
                         confirmation='required', continuous_authorisation=False,
                         synthetic=False)
        matrix = ports.assess('fake-replay', provides=['quotes'],
                              requires=['quotes'], confirmation='not_required',
                              continuous_authorisation=False, synthetic=True)
        self.assertEqual(matrix.adapter_id, 'fake-replay')

    def test_synthetic_adapter_stays_labelled(self):
        self.assertTrue(self.matrix().as_dict()['synthetic'])


class PortTests(TestCase):
    def test_a_fixture_adapter_satisfies_the_market_data_port(self):
        class Fixture:
            def quotes(self, symbols, *, as_of):
                return {}

            def bars(self, symbols, *, interval, start, end):
                return []

        self.assertIsInstance(Fixture(), ports.MarketDataPort)

    def test_a_market_adapter_is_not_an_execution_port(self):
        class Fixture:
            def quotes(self, symbols, *, as_of):
                return {}

            def bars(self, symbols, *, interval, start, end):
                return []

        self.assertNotIsInstance(Fixture(), ports.ExecutionPort)
        self.assertNotIsInstance(Fixture(), ports.BrokerReadPort)

    def test_an_execution_adapter_separates_queue_advance_from_targeted_operations(self):
        class Partial:
            def review(self, intent):
                return {}

        self.assertNotIsInstance(Partial(), ports.ExecutionPort)

        class Complete:
            def advance_queue(self, *, allow_entries=False):
                return {'agent_called': False}

            def review(self, intent):
                return {}

            def place(self, intent):
                return {}

            def cancel(self, ref_id):
                return {}

        self.assertIsInstance(Complete(), ports.ExecutionPort)

    def test_a_host_delivery_adapter_is_separate_from_execution(self):
        class Delivery:
            def deliver(self, intent):
                return {}

            def receipt(self, intent_id):
                return None

        self.assertIsInstance(Delivery(), ports.HostDeliveryPort)
        self.assertNotIsInstance(Delivery(), ports.ExecutionPort)

    def test_ports_carry_no_account_or_credential_surface(self):
        for name in ('MarketDataPort', 'BrokerReadPort', 'ExecutionPort',
                     'HostDeliveryPort'):
            protocol = getattr(ports, name)
            for method in getattr(protocol, '__protocol_attrs__', ()):
                self.assertNotIn('account', method.lower())
                self.assertNotIn('credential', method.lower())
