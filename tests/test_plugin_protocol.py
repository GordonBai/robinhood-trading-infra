"""Public external-plugin descriptor, config and installed-entry contracts."""
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from trading_infra.plugin_protocol import (
    PROTOCOL_VERSION, PluginDescriptor, list_plugins, load_plugin,
    validate_config,
)


def descriptor(**overrides):
    values = dict(
        plugin_id='demo-plugin', version='1.2.3', state_schema='demo-1',
        config_schema={'type': 'object', 'additionalProperties': False,
                       'required': ['lookback'],
                       'properties': {'lookback': {'type': 'integer',
                                                   'minimum': 1,
                                                   'maximum': 20}}},
        data_requirements=('quotes',), capabilities=('advisory-entry',))
    values.update(overrides)
    return PluginDescriptor(**values)


class PluginProtocolTests(TestCase):
    def test_descriptor_and_closed_config(self):
        item = descriptor()
        self.assertEqual(item.protocol_version, PROTOCOL_VERSION)
        self.assertEqual(item.manifest({'lookback': 3}).config['lookback'], 3)
        with self.assertRaisesRegex(ValueError, 'PLUGIN_CONFIG_UNKNOWN_FIELD'):
            validate_config(item.config_schema, {'lookback': 3, 'extra': 1})
        with self.assertRaisesRegex(ValueError, 'PLUGIN_CONFIG_VALUE_INVALID'):
            validate_config(item.config_schema, {'lookback': 0})
        with self.assertRaisesRegex(ValueError, 'PLUGIN_CONFIG_MISSING_FIELD'):
            validate_config(item.config_schema, {})

    def test_descriptor_as_dict_is_json_serializable_and_declares_bounds(self):
        import json
        value = descriptor().as_dict()
        encoded = json.dumps(value, sort_keys=True)
        self.assertIn('warmup_bars', encoded)
        self.assertEqual(value['warmup_bars'], 0)
        with self.assertRaisesRegex(ValueError, 'PLUGIN_CONFIG_SCHEMA_RANGE_INVALID'):
            descriptor(config_schema={
                'type': 'object', 'additionalProperties': False,
                'required': [], 'properties': {
                    'flag': {'type': 'boolean', 'minimum': 1}}})

    def test_protocol_version_and_data_requirements_fail_closed(self):
        with self.assertRaisesRegex(ValueError, 'PLUGIN_PROTOCOL_VERSION_UNSUPPORTED'):
            descriptor(protocol_version=2)
        self.assertEqual(descriptor(data_requirements=('5minute-bars',)).data_requirements,
                         ('5minute-bars',))
        with self.assertRaisesRegex(ValueError, 'PLUGIN_DATA_REQUIREMENT_UNSUPPORTED'):
            descriptor(data_requirements=('tick-by-tick',))
        with self.assertRaisesRegex(ValueError, 'PLUGIN_EXIT_RULE_REQUIRES_FEATURE_KEYS'):
            descriptor(exit_rule='demo.rules:advise')

    def test_unknown_entry_point_is_not_silently_loaded(self):
        with patch('trading_infra.plugin_protocol.metadata.entry_points',
                   return_value=()):
            with self.assertRaisesRegex(ValueError, 'PLUGIN_NOT_INSTALLED'):
                load_plugin('missing')
            self.assertEqual(list_plugins(), [])

    def test_duplicate_entry_points_are_rejected(self):
        class Dist:
            metadata = {'Name': 'demo-dist'}
            version = '1.0'
            files = ()
        entry = SimpleNamespace(name='demo-plugin', value='x:y', dist=Dist(),
                                load=lambda: (_ for _ in ()).throw(
                                    AssertionError('must not load')))
        with patch('trading_infra.plugin_protocol.metadata.entry_points',
                   return_value=(entry, entry)):
            with self.assertRaisesRegex(ValueError, 'PLUGIN_ENTRY_POINT_AMBIGUOUS'):
                load_plugin('demo-plugin')
