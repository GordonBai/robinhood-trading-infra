"""Versioned public strategy-plugin descriptor and installed entry-point loader.

An external distribution registers one provider in the ``trading_infra.strategies``
entry-point group. The provider exposes ``describe()`` and ``create(config)``.
No infrastructure source, catalog or CLI branch is edited to add a plugin.

Python plugins run as trusted code. This module validates contracts and
identity; it is not an OS sandbox.
"""
from dataclasses import dataclass
from importlib import metadata
from pathlib import Path
import hashlib
import json
import re
from types import MappingProxyType
from typing import Mapping

from trading_contracts.instance import PluginManifest
from trading_contracts.values import digest

PROTOCOL_VERSION = 1
ENTRY_POINT_GROUP = 'trading_infra.strategies'
SUPPORTED_DATA_REQUIREMENTS = frozenset({'quotes', '5minute-bars', 'universe-screen'})
_ID = re.compile(r'[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*')
_VERSION = re.compile(r'[0-9A-Za-z][0-9A-Za-z.+_-]{0,63}')
_SCHEMA_KEYS = frozenset({
    'type', 'properties', 'required', 'additionalProperties', 'enum',
    'pattern', 'minLength', 'maxLength', 'minimum', 'maximum',
})
_PROPERTY_TYPES = {'string', 'integer', 'boolean'}


def _label(value, code, pattern=_ID):
    if not isinstance(value, str) or not pattern.fullmatch(value):
        raise ValueError(code)
    return value


def _jsonable(value):
    """Deep JSON representation for descriptor/schema output."""
    if isinstance(value, Mapping):
        return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_jsonable(item) for item in value]
    return value


def _schema(value):
    if not isinstance(value, Mapping) or value.get('type') != 'object':
        raise ValueError('PLUGIN_CONFIG_SCHEMA_REQUIRED')
    unknown = set(value) - _SCHEMA_KEYS
    if unknown:
        raise ValueError('PLUGIN_CONFIG_SCHEMA_UNSUPPORTED:' + ','.join(sorted(unknown)))
    if value.get('additionalProperties') is not False:
        raise ValueError('PLUGIN_CONFIG_SCHEMA_MUST_BE_CLOSED')
    properties = value.get('properties')
    if not isinstance(properties, Mapping) or not properties:
        raise ValueError('PLUGIN_CONFIG_PROPERTIES_REQUIRED')
    copied = {}
    for name, rule in properties.items():
        _label(name, 'PLUGIN_CONFIG_PROPERTY_INVALID', re.compile(r'[a-z][a-z0-9_]{0,63}'))
        if not isinstance(rule, Mapping):
            raise ValueError('PLUGIN_CONFIG_PROPERTY_SCHEMA_INVALID:' + name)
        unknown = set(rule) - _SCHEMA_KEYS
        if unknown:
            raise ValueError('PLUGIN_CONFIG_PROPERTY_SCHEMA_INVALID:' + name)
        if rule.get('type') not in _PROPERTY_TYPES:
            raise ValueError('PLUGIN_CONFIG_PROPERTY_TYPE_INVALID:' + name)
        if 'enum' in rule:
            choices = rule['enum']
            if not isinstance(choices, (list, tuple)) or not choices:
                raise ValueError('PLUGIN_CONFIG_ENUM_INVALID:' + name)
            for choice in choices:
                _check_scalar(name, choice, rule)
        kind = rule['type']
        if 'minimum' in rule or 'maximum' in rule:
            if kind != 'integer':
                raise ValueError('PLUGIN_CONFIG_SCHEMA_RANGE_INVALID:' + name)
        if 'minLength' in rule or 'maxLength' in rule or 'pattern' in rule:
            if kind != 'string':
                raise ValueError('PLUGIN_CONFIG_SCHEMA_STRING_RULE_INVALID:' + name)
        copied[name] = MappingProxyType(dict(rule))
    required = value.get('required', ())
    if not isinstance(required, (list, tuple)) or any(
            not isinstance(item, str) or item not in copied for item in required):
        raise ValueError('PLUGIN_CONFIG_REQUIRED_INVALID')
    return MappingProxyType({'type': 'object',
        'properties': MappingProxyType(copied),
        'required': tuple(required), 'additionalProperties': False})


def _check_scalar(name, value, rule):
    kind = rule['type']
    if kind == 'string':
        if not isinstance(value, str) or len(value) > 512:
            raise ValueError('PLUGIN_CONFIG_VALUE_INVALID:' + name)
        if 'pattern' in rule and not re.fullmatch(rule['pattern'], value):
            raise ValueError('PLUGIN_CONFIG_VALUE_INVALID:' + name)
        if 'minLength' in rule and len(value) < rule['minLength']:
            raise ValueError('PLUGIN_CONFIG_VALUE_INVALID:' + name)
        if 'maxLength' in rule and len(value) > rule['maxLength']:
            raise ValueError('PLUGIN_CONFIG_VALUE_INVALID:' + name)
    elif kind == 'integer':
        if type(value) is not int:
            raise ValueError('PLUGIN_CONFIG_VALUE_INVALID:' + name)
    elif kind == 'boolean':
        if type(value) is not bool:
            raise ValueError('PLUGIN_CONFIG_VALUE_INVALID:' + name)
    if 'minimum' in rule and value < rule['minimum']:
        raise ValueError('PLUGIN_CONFIG_VALUE_INVALID:' + name)
    if 'maximum' in rule and value > rule['maximum']:
        raise ValueError('PLUGIN_CONFIG_VALUE_INVALID:' + name)
    if 'enum' in rule and value not in rule['enum']:
        raise ValueError('PLUGIN_CONFIG_VALUE_INVALID:' + name)


def validate_config(schema, config):
    """Validate a flat scalar plugin config against the declared closed schema."""
    if not isinstance(config, Mapping):
        raise ValueError('PLUGIN_CONFIG_REQUIRED')
    declared = schema['properties']
    unknown = set(config) - set(declared)
    if unknown:
        raise ValueError('PLUGIN_CONFIG_UNKNOWN_FIELD:' + ','.join(sorted(unknown)))
    for name in schema['required']:
        if name not in config:
            raise ValueError('PLUGIN_CONFIG_MISSING_FIELD:' + name)
    for name, value in config.items():
        _check_scalar(name, value, declared[name])
    return MappingProxyType(dict(config))


@dataclass(frozen=True)
class PluginDescriptor:
    plugin_id: str
    version: str
    state_schema: str
    config_schema: Mapping
    data_requirements: tuple
    capabilities: tuple = ()
    feature_keys: tuple = ()
    exit_rule: str | None = None
    warmup_bars: int = 0
    history_bars: int = 0
    protocol_version: int = PROTOCOL_VERSION

    def __post_init__(self):
        _label(self.plugin_id, 'PLUGIN_ID_INVALID')
        _label(self.version, 'PLUGIN_VERSION_INVALID', _VERSION)
        _label(self.state_schema, 'PLUGIN_STATE_SCHEMA_INVALID', re.compile(r'[A-Za-z0-9._-]{1,128}'))
        if type(self.protocol_version) is not int or self.protocol_version != PROTOCOL_VERSION:
            raise ValueError('PLUGIN_PROTOCOL_VERSION_UNSUPPORTED')
        object.__setattr__(self, 'config_schema', _schema(self.config_schema))
        for name in ('data_requirements', 'capabilities', 'feature_keys'):
            values = getattr(self, name)
            if isinstance(values, (str, bytes)) or not isinstance(values, (tuple, list)):
                raise ValueError('PLUGIN_' + name.upper() + '_INVALID')
            values = tuple(values)
            if any(not isinstance(item, str) or not item for item in values):
                raise ValueError('PLUGIN_' + name.upper() + '_INVALID')
            if name != 'data_requirements' and any(
                    not _ID.fullmatch(item) for item in values):
                raise ValueError('PLUGIN_' + name.upper() + '_INVALID')
            if len(set(values)) != len(values):
                raise ValueError('PLUGIN_' + name.upper() + '_INVALID')
            object.__setattr__(self, name, values)
        if not self.data_requirements:
            raise ValueError('PLUGIN_DATA_REQUIREMENTS_REQUIRED')
        unknown = set(self.data_requirements) - SUPPORTED_DATA_REQUIREMENTS
        if unknown:
            raise ValueError('PLUGIN_DATA_REQUIREMENT_UNSUPPORTED:' + ','.join(sorted(unknown)))
        for name in ('warmup_bars', 'history_bars'):
            value = getattr(self, name)
            if type(value) is not int or value < 0 or value > 100000:
                raise ValueError('PLUGIN_HISTORY_BOUND_INVALID')
        if self.exit_rule is not None:
            if not isinstance(self.exit_rule, str) or not re.fullmatch(
                    r'[A-Za-z_][A-Za-z0-9_.]*:[A-Za-z_][A-Za-z0-9_]*', self.exit_rule):
                raise ValueError('PLUGIN_EXIT_RULE_INVALID')
            if not self.feature_keys:
                raise ValueError('PLUGIN_EXIT_RULE_REQUIRES_FEATURE_KEYS')

    def manifest(self, config, *, plugin_identity=None):
        plugin_config = dict(config)
        cost = plugin_config.pop('cost_buffer_per_share', None)
        checked = dict(validate_config(self.config_schema, plugin_config))
        if cost is not None:
            checked['cost_buffer_per_share'] = cost
        return PluginManifest(
            plugin_id=self.plugin_id, version=self.version,
            state_schema=self.state_schema, config=checked,
            data_requirements=self.data_requirements,
            capabilities=self.capabilities, max_inflight=256,
            plugin_identity=plugin_identity)

    def as_dict(self):
        return {'protocol_version': self.protocol_version,
                'plugin_id': self.plugin_id, 'version': self.version,
                'state_schema': self.state_schema,
                'config_schema': _jsonable(self.config_schema),
                'data_requirements': list(self.data_requirements),
                'capabilities': list(self.capabilities),
                'feature_keys': list(self.feature_keys),
                'exit_rule': self.exit_rule,
                'warmup_bars': self.warmup_bars,
                'history_bars': self.history_bars}


@dataclass(frozen=True)
class DistributionIdentity:
    distribution: str
    distribution_version: str
    digest: str
    files: tuple

    def as_dict(self):
        return {'distribution': self.distribution,
                'distribution_version': self.distribution_version,
                'digest': self.digest, 'files': list(self.files)}


def _identity_for_dist(dist):
    try:
        direct = dist.read_text('direct_url.json')
        if direct and json.loads(direct).get('dir_info', {}).get('editable') is True:
            raise ValueError('PLUGIN_EDITABLE_DISTRIBUTION_UNSUPPORTED')
    except ValueError:
        raise
    except Exception:
        pass
    name = dist.metadata.get('Name')
    version = dist.version
    if not name or not version:
        raise ValueError('PLUGIN_DISTRIBUTION_IDENTITY_INVALID')
    rows = []
    for member in dist.files or ():
        if member.parts and ('__pycache__' in member.parts
                             or member.suffix in {'.pyc', '.pyo'}):
            continue
        path = Path(dist.locate_file(member))
        if not path.is_file() or path.is_symlink():
            raise ValueError('PLUGIN_DISTRIBUTION_FILE_INVALID:' + member.as_posix())
        rows.append([member.as_posix(), hashlib.sha256(path.read_bytes()).hexdigest()])
    if not rows:
        raise ValueError('PLUGIN_DISTRIBUTION_FILES_MISSING')
    rows.sort()
    return DistributionIdentity(name, version,
        digest(['plugin-distribution-1', name, version, rows]), tuple(
            row[0] for row in rows))


def distribution_identity(entry_point):
    """Hash the installed distribution's actual files, not a self-reported version."""
    dist = getattr(entry_point, 'dist', None)
    if dist is None:
        raise ValueError('PLUGIN_DISTRIBUTION_REQUIRED')
    return _identity_for_dist(dist)


def distribution_identity_for_module(module_name):
    """Identity of the installed distribution that owns one imported module."""
    mappings = metadata.packages_distributions()
    root = module_name.split('.')[0]
    names = mappings.get(root) or ()
    if len(names) != 1:
        raise ValueError('PLUGIN_MODULE_DISTRIBUTION_AMBIGUOUS:' + module_name)
    return _identity_for_dist(metadata.distribution(names[0]))


@dataclass(frozen=True)
class LoadedPlugin:
    descriptor: PluginDescriptor
    provider: object
    entry_point: str
    distribution: DistributionIdentity

    def create(self, config):
        validate_config(self.descriptor.config_schema, config)
        factory = getattr(self.provider, 'create', None)
        if not callable(factory):
            raise ValueError('PLUGIN_CREATE_REQUIRED')
        try:
            plugin = factory(dict(config))
        except (ValueError, KeyError, TypeError):
            raise
        except Exception:
            raise ValueError('PLUGIN_CREATE_FAILED') from None
        if not callable(getattr(plugin, 'decide', None)):
            raise ValueError('PLUGIN_DECIDE_REQUIRED')
        return plugin


def _entry_points():
    return tuple(metadata.entry_points(group=ENTRY_POINT_GROUP))


def list_plugins():
    rows = []
    by_id = {}
    for entry in _entry_points():
        try:
            loaded = load_plugin(entry.name)
        except ValueError as exc:
            by_id.setdefault(entry.name, []).append(entry)
            rows.append({'entry_point': entry.name, 'value': entry.value,
                         'error': str(exc)})
            continue
        rows.append({'entry_point': entry.name,
                     'plugin_id': loaded.descriptor.plugin_id,
                     'version': loaded.descriptor.version,
                     'distribution': loaded.distribution.distribution,
                     'distribution_version': loaded.distribution.distribution_version,
                     'identity': loaded.distribution.digest})
        by_id.setdefault(entry.name, []).append(entry)
    for entry_point, entries in by_id.items():
        if len(entries) > 1:
            rows.append({'entry_point': entry_point,
                         'error': 'PLUGIN_ENTRY_POINT_AMBIGUOUS'})
    return sorted(rows, key=lambda row: row['entry_point'])


def entry_point_for(plugin_id):
    """Return the unique installed metadata entry point without importing it."""
    _label(plugin_id, 'PLUGIN_ID_INVALID')
    entries = [entry for entry in _entry_points() if entry.name == plugin_id]
    if not entries:
        raise ValueError('PLUGIN_NOT_INSTALLED:' + plugin_id)
    if len(entries) != 1:
        raise ValueError('PLUGIN_ENTRY_POINT_AMBIGUOUS:' + plugin_id)
    return entries[0]


def load_plugin(plugin_id):
    """Load exactly one installed provider; unknown/duplicate ids fail closed."""
    entry = entry_point_for(plugin_id)
    try:
        provider = entry.load()
    except Exception:
        raise ValueError('PLUGIN_ENTRY_POINT_LOAD_FAILED:' + plugin_id) from None
    describe = getattr(provider, 'describe', None)
    if not callable(describe):
        raise ValueError('PLUGIN_DESCRIPTOR_REQUIRED:' + plugin_id)
    try:
        descriptor = describe()
    except ValueError:
        raise
    except Exception:
        raise ValueError('PLUGIN_DESCRIPTOR_FAILED:' + plugin_id) from None
    if not isinstance(descriptor, PluginDescriptor):
        raise ValueError('PLUGIN_DESCRIPTOR_CONTRACT_REQUIRED:' + plugin_id)
    if descriptor.plugin_id != plugin_id:
        raise ValueError('PLUGIN_ID_MISMATCH:' + plugin_id)
    return LoadedPlugin(descriptor, provider, entry.name,
                        distribution_identity(entry))
