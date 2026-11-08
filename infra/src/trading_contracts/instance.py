"""Versioned strategy-instance identity, plugin manifest and instance budget.

An instance binds one plugin version to one durable configuration and one
isolated state namespace. Manifests declare what the infrastructure must
enforce for that plugin; they never carry account numbers, broker handles,
credentials or order identities.

These contracts are validation and dependency boundaries. They are not a
sandbox for untrusted Python.
"""
from dataclasses import dataclass
from decimal import Decimal
import re
from types import MappingProxyType
from typing import Mapping

from .values import digest, number

#: Durable strategy-instance modes. ``shadow`` consumes synthetic replay;
#: ``read_only_shadow`` consumes a real-source read-only snapshot. Neither mode
#: grants order/execution capability; that remains an infrastructure gate.
MODES = ('shadow', 'read_only_shadow', 'disabled')
MODE_PROVENANCE = {'shadow': 'synthetic',
                   'read_only_shadow': 'real_read_only',
                   'disabled': 'none'}
MODE_EXECUTION_CAPABILITY = {'shadow': 'NONE',
                             'read_only_shadow': 'NONE',
                             'disabled': 'NONE'}
MODE_SCHEMA_VERSION = 2
#: The single pre-multi-instance strategy namespace. Existing durable state and
#: task history keep this owner when it is migrated, never a guessed new one.
LEGACY_INSTANCE_ID = 'martin-legacy-default'
_CONFIG_KEY = re.compile(r'[a-z][a-z0-9_]{0,63}')


def _label(value, name):
    if not isinstance(value, str) or not value.strip() or len(value) > 128:
        raise ValueError('INSTANCE_' + name + '_INVALID')
    return value


def _labels(values, name, *, allow_empty=False):
    if isinstance(values, (str, bytes)) or not isinstance(values, (tuple, list)):
        raise ValueError('INSTANCE_' + name + '_INVALID')
    result = tuple(_label(item, name).strip() for item in values)
    if len(set(result)) != len(result) or (not result and not allow_empty):
        raise ValueError('INSTANCE_' + name + '_INVALID')
    return result


def _positive(value, name):
    try:
        result = number(value)
    except ValueError:
        raise ValueError('INSTANCE_' + name + '_INVALID') from None
    if result <= 0:
        raise ValueError('INSTANCE_' + name + '_INVALID')
    return result


def _positive_int(value, name):
    if type(value) is not int or value <= 0:
        raise ValueError('INSTANCE_' + name + '_INVALID')
    return value


def _scalar_config(config):
    """Explicit, flat configuration: no nested containers, no floats, no None.

    A plugin whose configuration cannot be stated as bounded scalars has to
    declare a schema of its own first. Accepting a nested structure here would
    make the registered fingerprint depend on arbitrary JSON depth.
    """
    if not isinstance(config, Mapping):
        raise ValueError('INSTANCE_CONFIG_INVALID')
    result = {}
    for key, value in config.items():
        if not isinstance(key, str) or not _CONFIG_KEY.fullmatch(key):
            raise ValueError('INSTANCE_CONFIG_KEY_INVALID')
        if type(value) is bool or type(value) is int:
            result[key] = value
        elif isinstance(value, str) and len(value) <= 512:
            result[key] = value
        elif isinstance(value, Decimal) and value.is_finite():
            result[key] = str(value)
        else:
            raise ValueError('INSTANCE_CONFIG_VALUE_INVALID')
    return MappingProxyType(result)


@dataclass(frozen=True)
class InstanceBudget:
    """Per-instance ceiling. The account authority always applies on top."""
    max_cash: Decimal
    max_planned_loss: Decimal
    max_notional: Decimal
    max_concurrent: int

    def __post_init__(self):
        for name in ('max_cash', 'max_planned_loss', 'max_notional'):
            object.__setattr__(self, name, _positive(getattr(self, name), name.upper()))
        object.__setattr__(self, 'max_concurrent',
                           _positive_int(self.max_concurrent, 'MAX_CONCURRENT'))

    def as_dict(self):
        return {'max_cash': str(self.max_cash), 'max_planned_loss': str(self.max_planned_loss),
                'max_notional': str(self.max_notional), 'max_concurrent': self.max_concurrent}


@dataclass(frozen=True)
class PluginManifest:
    """What the infrastructure must know about one plugin version."""
    plugin_id: str
    version: str
    state_schema: str
    config: Mapping
    data_requirements: tuple
    capabilities: tuple
    max_inflight: int
    plugin_identity: str | None = None

    def __post_init__(self):
        for name in ('plugin_id', 'version', 'state_schema'):
            _label(getattr(self, name), name.upper())
        object.__setattr__(self, 'config', _scalar_config(self.config))
        object.__setattr__(self, 'data_requirements', _labels(self.data_requirements, 'DATA_REQUIREMENT'))
        object.__setattr__(self, 'capabilities', _labels(self.capabilities, 'CAPABILITY',
                                                         allow_empty=True))
        object.__setattr__(self, 'max_inflight', _positive_int(self.max_inflight, 'MAX_INFLIGHT'))
        if self.plugin_identity is not None and (
                not isinstance(self.plugin_identity, str)
                or not re.fullmatch('[0-9a-f]{64}', self.plugin_identity)):
            raise ValueError('INSTANCE_PLUGIN_IDENTITY_INVALID')

    def identity_fields(self):
        result = {'plugin_id': self.plugin_id, 'version': self.version,
                  'state_schema': self.state_schema, 'config': dict(self.config),
                  'data_requirements': list(self.data_requirements),
                  'capabilities': list(self.capabilities), 'max_inflight': self.max_inflight}
        if self.plugin_identity is not None:
            result['plugin_identity'] = self.plugin_identity
        return result

    def digest(self):
        """Stable identity of the registered plugin contract and configuration."""
        return digest(['strategy-manifest-1', self.identity_fields()])

    def config_fingerprint(self):
        parts = ['strategy-config-1', self.plugin_id, self.version, dict(self.config)]
        if self.plugin_identity is not None:
            parts.append(self.plugin_identity)
        return digest(parts)


@dataclass(frozen=True)
class StrategyInstance:
    instance_id: str
    manifest: PluginManifest
    budget: InstanceBudget
    mode: str
    enabled: bool

    def __post_init__(self):
        _label(self.instance_id, 'ID')
        if not isinstance(self.manifest, PluginManifest):
            raise ValueError('INSTANCE_MANIFEST_REQUIRED')
        if not isinstance(self.budget, InstanceBudget):
            raise ValueError('INSTANCE_BUDGET_REQUIRED')
        if self.mode not in MODES:
            raise ValueError('INSTANCE_MODE_INVALID')
        if type(self.enabled) is not bool:
            raise ValueError('INSTANCE_ENABLED_INVALID')
        if self.mode == 'disabled' and self.enabled:
            raise ValueError('INSTANCE_DISABLED_MODE_CANNOT_BE_ENABLED')

    @property
    def source_mode(self):
        return MODE_PROVENANCE[self.mode]

    @property
    def execution_capability(self):
        return MODE_EXECUTION_CAPABILITY[self.mode]

    def strategy_identity(self):
        """Contract identity handed to the plugin and used in decision ids."""
        from .strategy import StrategyIdentity
        return StrategyIdentity(self.instance_id, self.manifest.plugin_id, self.manifest.version)

    def digest(self):
        return digest(['strategy-instance-1', self.instance_id, self.manifest.digest(),
                       self.budget.as_dict(), self.mode, self.enabled])


#: Version of the strategy-configuration binding protocol. A host and a worker
#: must derive the same value from the same (plugin, version, config) triple, in
#: the same protocol version. Bumping this invalidates every outstanding request
#: on purpose: an old worker must not silently accept a new configuration rule.
CONFIG_BINDING_VERSION = 1


def config_version(plugin_id, version, config, plugin_identity=None):
    """The single declared way to bind a plugin's configuration to a request.

    Both the host that issues a request and the worker that validates it call
    this function. Deriving it in two places is what let an instance host and a
    worker disagree while each was individually correct.
    """
    if not isinstance(plugin_id, str) or not plugin_id or len(plugin_id) > 128:
        raise ValueError('CONFIG_PLUGIN_ID_INVALID')
    if not isinstance(version, str) or not version or len(version) > 128:
        raise ValueError('CONFIG_VERSION_INVALID')
    if not isinstance(config, Mapping):
        raise ValueError('CONFIG_MAPPING_REQUIRED')
    if plugin_identity is not None and (
            not isinstance(plugin_identity, str)
            or not re.fullmatch('[0-9a-f]{64}', plugin_identity)):
        raise ValueError('CONFIG_PLUGIN_IDENTITY_INVALID')
    return 'config-' + digest(['strategy-config-binding',
                               CONFIG_BINDING_VERSION, plugin_id, version,
                               dict(config)] +
                              ([plugin_identity] if plugin_identity is not None else []))
