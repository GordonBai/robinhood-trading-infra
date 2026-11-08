"""Application composition for installed external plugins.

Loads only explicitly named entry points. The catalog supplies contracts,
config validation, worker command, feature keys and exit rules to the same
instance/Book/admission runtime used by built-in plugins.
"""
from importlib import metadata
from types import MappingProxyType
import json
import sys

from trading_infra.instance_composition import (
    build_admission_stack as _build_admission_stack,
    build_instance_group as _build_instance_group,
)
from trading_infra.exit_composition import ExitRuleRegistry
from trading_infra.plugin_protocol import (
    ENTRY_POINT_GROUP, load_plugin, validate_config,
)


class ExternalCatalog:
    def __init__(self, plugin_ids):
        if isinstance(plugin_ids, str) or not isinstance(plugin_ids, (tuple, list)):
            raise ValueError('PLUGIN_SELECTION_REQUIRED')
        ids = tuple(plugin_ids)
        if not ids or len(set(ids)) != len(ids):
            raise ValueError('PLUGIN_SELECTION_INVALID')
        self.plugins = {plugin_id: load_plugin(plugin_id) for plugin_id in ids}
        rules = {}
        features = {}
        for plugin_id, loaded in self.plugins.items():
            descriptor = loaded.descriptor
            features[plugin_id] = descriptor.feature_keys
            if descriptor.exit_rule is not None:
                rules[plugin_id] = (descriptor.exit_rule + '@'
                                    + loaded.distribution.digest)
        self.exit_registry = ExitRuleRegistry(rules, features)

    def contract(self, plugin_id):
        descriptor = self.plugins[plugin_id].descriptor
        return {'version': descriptor.version,
                'state_schema': descriptor.state_schema,
                'data_requirements': descriptor.data_requirements,
                'capabilities': descriptor.capabilities,
                'feature_keys': descriptor.feature_keys,
                'exit_rule': descriptor.exit_rule,
                'config_schema': descriptor.config_schema,
                'plugin_identity': self.plugins[plugin_id].distribution.digest}

    def validate_config(self, plugin_id, config):
        # The infrastructure cost buffer is a host-owned manifest field. It is
        # present in the registered config for admission, but it is not a
        # required member of the plugin's own authoring schema.
        plugin_config = dict(config)
        plugin_config.pop('cost_buffer_per_share', None)
        return validate_config(self.plugins[plugin_id].descriptor.config_schema,
                               plugin_config)

    def config_for(self, plugin_id, cost, extra):
        schema = self.plugins[plugin_id].descriptor.config_schema
        config = dict(extra)
        config['cost_buffer_per_share'] = str(cost)
        plugin_config = dict(config)
        plugin_config.pop('cost_buffer_per_share', None)
        validate_config(schema, plugin_config)
        return config

    def create(self, plugin_id, config):
        plugin_config = dict(config)
        plugin_config.pop('cost_buffer_per_share', None)
        return self.plugins[plugin_id].create(plugin_config)

    def worker_command(self, plugin_id, python, *, cost_buffer_per_share,
                       source_fingerprint, config=None):
        loaded = self.plugins[plugin_id]
        descriptor = loaded.descriptor
        if config is None:
            config = {'cost_buffer_per_share': str(cost_buffer_per_share)}
        return [python, '-I', '-m', 'trading_infra.external_worker',
                '--plugin', plugin_id,
                '--plugin-digest', loaded.distribution.digest,
                '--plugin-identity', loaded.distribution.digest,
                '--config', json.dumps(dict(config), sort_keys=True,
                                        separators=(',', ':')),
                '--source-fingerprint', str(source_fingerprint)]

    def feature_keys_for(self, plugin_id):
        return self.exit_registry.feature_keys_for(plugin_id)

    def exit_specs_for(self, plugin_ids):
        return self.exit_registry.specs_for(plugin_ids)


class ExternalComposition:
    def __init__(self, catalog):
        self.catalog = catalog

    def build_instance_group(self, book, policy, *, source_fingerprint,
                             clock=None, runner_factory=None, mode='shadow',
                             enabled=True):
        return _build_instance_group(
            book, policy, source_fingerprint=source_fingerprint,
            catalog=self.catalog, clock=clock, runner_factory=runner_factory,
            mode=mode, enabled=enabled)

    def build_admission_stack(self, book, policy, *, clock=None, limits=None):
        return _build_admission_stack(book, policy, clock=clock, limits=limits)

    def feature_keys_for(self, plugin_id):
        return self.catalog.feature_keys_for(plugin_id)

    def exit_specs_for(self, plugin_ids):
        return self.catalog.exit_specs_for(plugin_ids)


def external_composition(plugin_ids):
    return ExternalComposition(ExternalCatalog(tuple(plugin_ids)))


def installed_plugin_ids():
    """Return unique entry-point names; duplicates and malformed ids are errors."""
    names = [entry.name for entry in metadata.entry_points(group=ENTRY_POINT_GROUP)]
    if len(set(names)) != len(names):
        raise ValueError('PLUGIN_ENTRY_POINT_AMBIGUOUS')
    return tuple(sorted(names))
