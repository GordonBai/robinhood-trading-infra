"""Portable contract boundary, fixture-only; no broker, process or database I/O."""
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from types import MappingProxyType
from unittest import TestCase
from unittest.mock import patch

from trading_contracts import codec
from trading_contracts.strategy import (
    StrategyIdentity, StrategyInput, StrategyProposal, StrategyDecision, ProposalAction,
)
from trading_contracts.values import digest
from trading_infra.market_calendar import ExchangeCalendarView
from trading_strategies.martin.signals import MartinSignalPlugin
from shadow_fixture import events


NOW = datetime(2026, 10, 29, 15, tzinfo=timezone.utc)
IDENTITY = StrategyIdentity('codec-fixture', 'martin', MartinSignalPlugin.VERSION)


class StrategyCodecTests(TestCase):
    def request(self):
        return StrategyInput(IDENTITY, NOW, 'cursor-a', 'snapshot-a', 'a'*64, 'config-v1', True,
            market={'z': Decimal('1.2300'), 'a': ['1.2300', True, 1, None, '汉字']},
            budget={'zero': Decimal('-0')}, checkpoint={'state': ['WAIT']})

    def decision(self):
        proposal = StrategyProposal(IDENTITY, 'proposal-a', 'decision-a', 'setup-a',
            'snapshot-a', 'a'*64, NOW, NOW+timedelta(seconds=15), ProposalAction.ENTER,
            'DEMO', 'fixture', True, quantity_limit=1, entry_ceiling=Decimal('10.00'),
            stop_price=Decimal('9.0'), evidence={'sample': [Decimal('1E+3')]})
        return StrategyDecision(IDENTITY, 'decision-a', NOW, 'snapshot-a', 'cursor-a', True,
            proposals=[proposal], next_checkpoint={'state': ['TRIGGERED']})

    def wire(self, mutate):
        value = json.loads(codec.encode_input(self.request()))
        mutate(value)
        return json.dumps(value, separators=(',', ':')).encode()

    def assert_invalid(self, value):
        with self.assertRaisesRegex(ValueError, '^STRATEGY_CODEC_INVALID$'):
            codec.decode_input(value)

    def test_input_roundtrip_preserves_decimal_scale_types_order_and_immutability(self):
        request = self.request(); raw = codec.encode_input(request)
        result = codec.decode_input(raw)
        self.assertEqual(result, request)
        self.assertEqual(codec.encode_input(result), raw)
        self.assertEqual(tuple(result.market), ('z', 'a'))
        self.assertEqual(result.market['z'].as_tuple(), Decimal('1.2300').as_tuple())
        self.assertEqual(result.budget['zero'].as_tuple(), Decimal('-0').as_tuple())
        self.assertIs(type(result.market['a'][1]), bool)
        self.assertIs(type(result.market['a'][2]), int)
        self.assertEqual(result.market['a'][-1], '汉字')
        with self.assertRaises(TypeError): result.market['a'] = ()

    def test_decision_roundtrip_restores_exact_declared_classes_and_binding(self):
        decision = self.decision(); raw = codec.encode_decision(decision)
        restored = codec.decode_decision(raw)
        self.assertIs(type(restored), StrategyDecision)
        self.assertIs(type(restored.proposals[0]), StrategyProposal)
        self.assertIs(type(restored.proposals[0].action), ProposalAction)
        self.assertEqual(restored, decision)
        self.assertIs(restored.validate_for(self.request()), restored)
        self.assertEqual(codec.encode_decision(restored), raw)

    def test_full_martin_request_and_decision_roundtrip_keep_exact_algorithm_result(self):
        plugin = MartinSignalPlugin(calendar_view=ExchangeCalendarView())
        checkpoint = {}
        for original in events()[1:5]:
            event = {**original, 'source_sha256': 'fixture-'+original['id'], 'synthetic_input': True}
            at = datetime.fromisoformat(event['at'])
            request = StrategyInput(IDENTITY, at, event['id'], 'event-'+digest(event),
                digest(event), 'v1', True, market={'event': event}, checkpoint=checkpoint)
            decoded = codec.decode_input(codec.encode_input(request))
            self.assertEqual(decoded, request)
            expected = plugin.decide(request)
            actual = plugin.decide(decoded)
            self.assertEqual(actual, expected)
            self.assertEqual(codec.decode_decision(codec.encode_decision(actual)), expected)
            checkpoint = actual.next_checkpoint
        self.assertEqual(len(expected.proposals), 1)

    def test_strict_root_kind_and_allowed_fields(self):
        self.assert_invalid(codec.encode_decision(self.decision()))
        for mutate in (
            lambda x: x.update(version=True), lambda x: x.update(version=2),
            lambda x: x.update(schema='pickle'), lambda x: x.update(extra='unexpected'),
            lambda x: x['payload'].update(t='unknown-class'),
            lambda x: x['payload']['v'].pop('cursor'),
            lambda x: x['payload']['v'].update(account_number={'t': 'str', 'v': 'forbidden'}),
        ):
            self.assert_invalid(self.wire(mutate))

    def test_duplicate_json_keys_and_duplicate_map_entries_are_rejected(self):
        self.assert_invalid(b'{"version":1,"version":1}')
        self.assert_invalid(b'{"payload":{"t":"input","t":"input"}}')
        self.assert_invalid(self.wire(lambda x: x['payload']['v']['market']['v'].append(
            ['z', {'t': 'decimal', 'v': '99'}])))

    def test_nonfinite_floats_wrong_primitive_types_and_naive_times_are_rejected(self):
        self.assert_invalid(b'{"version":NaN}')
        self.assert_invalid(b'{"version":Infinity}')
        self.assert_invalid(b'{"version":1.0}')
        self.assert_invalid(self.wire(lambda x: x['payload']['v']['as_of'].update(v='2026-10-29T15:00:00')))
        self.assert_invalid(self.wire(lambda x: x['payload']['v']['synthetic'].update(v=1)))
        self.assert_invalid(self.wire(lambda x: x['payload']['v']['market']['v'][0][1].update(v='NaN')))
        self.assert_invalid(self.wire(lambda x: x['payload']['v']['market']['v'][0][1].update(t='int', v=True)))

    def test_existing_contract_constructors_still_reject_credentials(self):
        self.assert_invalid(self.wire(lambda x: x['payload']['v']['market']['v'].append(
            ['nested', {'t': 'map', 'v': [['access_token', {'t': 'str', 'v': 'must-not-echo'}]]}])))

    def test_size_depth_bytes_and_utf8_bounds_return_generic_errors(self):
        for payload in (b'', b'\xff', 'not-bytes', bytearray(b'{}'),
                        b'['*100+b']'*100, b' '* (codec.MAX_BYTES+1)):
            self.assert_invalid(payload)
        self.assert_invalid(self.wire(lambda x: x['payload']['v']['cursor'].update(v='\ud800')))
        with patch.object(codec, 'MAX_BYTES', 32):
            with self.assertRaisesRegex(ValueError, '^STRATEGY_CODEC_INVALID$'):
                codec.encode_input(self.request())

    def test_encoder_revalidates_mutated_frozen_objects_and_rejects_wrong_root(self):
        for value, encoder in ((self.request(), codec.encode_decision),
                               (self.decision(), codec.encode_input)):
            with self.assertRaisesRegex(ValueError, '^STRATEGY_CODEC_INVALID$'):
                encoder(value)
        request = self.request()
        object.__setattr__(request, 'budget', MappingProxyType({'value': Decimal('Infinity')}))
        with self.assertRaisesRegex(ValueError, '^STRATEGY_CODEC_INVALID$'):
            codec.encode_input(request)
        request = self.request(); object.__setattr__(request, 'synthetic', 1)
        with self.assertRaisesRegex(ValueError, '^STRATEGY_CODEC_INVALID$'):
            codec.encode_input(request)

    def test_repeated_strings_reject_before_whole_json_materialization(self):
        shared = 'x'*4096
        request = replace(self.request(), market={'repeat': [shared]*3000})
        with patch.object(codec, 'MAX_BYTES', 4096), \
                patch.object(codec.json, 'dumps', side_effect=AssertionError('whole encoder must not run')) as whole:
            with self.assertRaisesRegex(ValueError, '^STRATEGY_CODEC_INVALID$'):
                codec.encode_input(request)
        whole.assert_not_called()

    def test_exact_wire_budget_handles_control_escapes_unicode_and_boundary(self):
        request = replace(self.request(), market={'quote"\\汉': ['\x00\n\t"\\汉字😀', '\u2028', False, None, -123]})
        raw = codec.encode_input(request)
        self.assertEqual(codec.decode_input(raw), request)
        with patch.object(codec, 'MAX_BYTES', len(raw)):
            self.assertEqual(codec.encode_input(request), raw)
        with patch.object(codec, 'MAX_BYTES', len(raw)-1), \
                patch.object(codec.json, 'dumps', side_effect=AssertionError('whole encoder must not run')) as whole:
            with self.assertRaisesRegex(ValueError, '^STRATEGY_CODEC_INVALID$'):
                codec.encode_input(request)
        whole.assert_not_called()

    def test_bounded_packed_payload_uses_c_encoder_fast_path(self):
        # The fallback Python streaming encoder must not be selected after the
        # exact-size preflight has established that a whole payload is bounded.
        original = json.dumps
        calls = []
        def tracked(value, **kwargs):
            calls.append(kwargs)
            return original(value, **kwargs)
        with patch.object(codec.json, 'dumps', tracked):
            codec.encode_input(self.request())
        self.assertEqual(len(calls), 1)
        self.assertFalse(calls[0]['ensure_ascii'])
