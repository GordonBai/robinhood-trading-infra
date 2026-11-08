"""Build a strategy-instance group from policy and an application catalog.

The infrastructure owns registration, budget, lease and runner plumbing. It
does not know which concrete plugin ids exist: the caller supplies a catalog
with ``contract(plugin_id)`` and ``worker_command(...)``. An absent
``strategy_instances`` key keeps the single legacy instance.
"""
import sys

from trading_contracts.instance import InstanceBudget, PluginManifest
from trading_contracts.values import number
from trading_infra.instance_registry import InstanceRegistry
from trading_infra.instance_runtime import InstanceRuntime
from trading_infra.instance_supervisor import InstanceSupervisor
from trading_infra.admission import AdmissionService
from trading_infra.attribution import ExecutionAttribution
from trading_infra.budget import AccountBudgetAuthority
from trading_infra.instance_accounting import InstanceAccounting
from trading_infra.strategy_stream import WarmStrategyRunner
from trading_infra.symbol_lease import SymbolLease

_SPEC_KEYS = frozenset({'instance_id', 'plugin', 'cost_buffer_per_share', 'mode', 'config'})
_INSTANCE_MODES = ('shadow', 'read_only_shadow')
_ACCOUNT_KEYS = ('max_position_notional_usd', 'max_planned_loss_per_trade_usd')


def _catalog_contract(catalog, plugin_id):
    contract = getattr(catalog, 'contract', None)
    if not callable(contract):
        raise ValueError('STRATEGY_CATALOG_REQUIRED')
    try:
        return contract(plugin_id)
    except ValueError:
        raise ValueError('INSTANCE_SPEC_UNKNOWN_PLUGIN') from None


def _spec(entry, catalog):
    if not isinstance(entry, dict):
        raise ValueError('INSTANCE_SPEC_INVALID')
    unknown = set(entry) - _SPEC_KEYS
    if unknown:
        raise ValueError('INSTANCE_SPEC_UNKNOWN_FIELD:' + ','.join(sorted(unknown)))
    instance_id = entry.get('instance_id')
    if not isinstance(instance_id, str) or not instance_id or len(instance_id) > 128:
        raise ValueError('INSTANCE_SPEC_INVALID')
    plugin_id = entry.get('plugin')
    if not isinstance(plugin_id, str) or not plugin_id:
        raise ValueError('INSTANCE_SPEC_UNKNOWN_PLUGIN')
    _catalog_contract(catalog, plugin_id)
    cost = entry.get('cost_buffer_per_share', '0.05')
    value = number(cost)
    if value < 0:
        raise ValueError('INSTANCE_SPEC_INVALID')
    instance_mode = entry.get('mode', 'shadow')
    if instance_mode not in _INSTANCE_MODES:
        raise ValueError('INSTANCE_SPEC_MODE_INVALID')
    extra = entry.get('config', {})
    if extra is None:
        extra = {}
    if not isinstance(extra, dict):
        raise ValueError('INSTANCE_SPEC_CONFIG_INVALID')
    builder = getattr(catalog, 'config_for', None)
    if callable(builder):
        config = builder(plugin_id, str(value), extra)
    else:
        config = {'cost_buffer_per_share': str(value)}
        config.update(extra)
    validator = getattr(catalog, 'validate_config', None)
    if callable(validator):
        validator(plugin_id, config)
    return instance_id, str(value), plugin_id, instance_mode, dict(config)


def build_instance_group(book, policy, *, source_fingerprint, catalog, clock=None,
                         runner_factory=None, mode='shadow', enabled=True):
    """Return an :class:`InstanceSupervisor`, or ``None`` when policy declares none.

    ``None`` is the compatibility answer: the caller keeps the single legacy
    instance instead of silently starting an empty group. ``catalog`` is the
    application's explicit plugin family map; an unknown id is refused.
    """
    specs = policy.get('strategy_instances')
    if specs is None:
        return None
    if not isinstance(specs, list) or not 1 <= len(specs) <= 16:
        raise ValueError('INSTANCE_SPEC_LIST_INVALID')
    for key in _ACCOUNT_KEYS:
        number(policy[key])
    registry = InstanceRegistry(book.db, clock=clock)
    runtimes = []
    for entry in specs:
        instance_id, cost, plugin_id, instance_mode, config = _spec(entry, catalog)
        family = _catalog_contract(catalog, plugin_id)
        manifest = PluginManifest(
            plugin_id=plugin_id, version=family['version'],
            state_schema=family['state_schema'],
            config=config,
            data_requirements=tuple(family['data_requirements']),
            capabilities=tuple(family['capabilities']), max_inflight=256,
            plugin_identity=family.get('plugin_identity'))
        budget = InstanceBudget(
            max_cash=policy['max_position_notional_usd'],
            max_planned_loss=policy['max_planned_loss_per_trade_usd'],
            max_notional=policy['max_position_notional_usd'], max_concurrent=1)
        try:
            registry.register(instance_id, manifest, budget,
                              mode=instance_mode or mode, enabled=enabled)
        except ValueError as exc:
            if str(exc) != 'INSTANCE_REGISTRATION_CONFLICT':
                raise
            # The 005r2b manifest added the quote input. That is a routing-only
            # extension of the same plugin contract; migrate it explicitly so
            # the existing checkpoint, proposals and management keep running.
            registry.upgrade_data_requirements(instance_id, manifest, budget)
        factory = runner_factory or _warm_runner
        import inspect
        parameters = inspect.signature(factory).parameters
        runner = (factory(cost, source_fingerprint, plugin_id=plugin_id, catalog=catalog,
                          config=dict(config))
                  if 'config' in parameters else
                  factory(cost, source_fingerprint, plugin_id=plugin_id, catalog=catalog)
                  if 'catalog' in parameters else
                  factory(cost, source_fingerprint, plugin_id=plugin_id)
                  if 'plugin_id' in parameters
                  else factory(cost, source_fingerprint))
        runtimes.append(InstanceRuntime(
            registry, registry.instance(instance_id),
            runner=runner, source_fingerprint=source_fingerprint, clock=clock))
    return InstanceSupervisor(runtimes, clock=clock)


def _warm_runner(cost_buffer_per_share, source_fingerprint, *, plugin_id, catalog,
                 config=None):
    command = getattr(catalog, 'worker_command', None)
    if not callable(command):
        raise ValueError('STRATEGY_CATALOG_REQUIRED')
    kwargs = dict(cost_buffer_per_share=cost_buffer_per_share,
                  source_fingerprint=source_fingerprint)
    if config is not None and 'config' in __import__('inspect').signature(command).parameters:
        kwargs['config'] = config
    return WarmStrategyRunner(command(plugin_id, sys.executable, **kwargs),
                              timeout_seconds=5)


#: Account-wide ceilings used when a policy does not state its own. They mirror
#: the documented trial boundary; a policy that wants different limits must say
#: so explicitly rather than inherit something wider by accident.
DEFAULT_ACCOUNT_LIMITS = {
    'max_open_cash': '100',
    'max_open_notional': '100',
    'max_open_planned_loss': '10.00',
    'max_concurrent_trades': 2,
}


def build_admission_stack(book, policy, *, clock=None, limits=None):
    """Assemble the account-wide authority an instance group must consult.

    Returns ``None`` when the policy declares no instances: a session that never
    admits has no business reserving account money. Otherwise one authority,
    one lease table and one attribution table are shared by every instance, on
    the same connection, so admission can be a single transaction.
    """
    if policy.get('strategy_instances') is None:
        return None
    declared = dict(DEFAULT_ACCOUNT_LIMITS)
    declared.update(policy.get('account_limits') or {})
    for key in ('max_open_cash', 'max_open_notional', 'max_open_planned_loss'):
        declared[key] = number(declared[key])
    budget = AccountBudgetAuthority(book.db, declared, clock=clock)
    leases = SymbolLease(book.db, clock=clock)
    attribution = ExecutionAttribution(book.db, clock=clock)
    registry = InstanceRegistry(book.db, clock=clock)
    service = AdmissionService(budget=budget, leases=leases, attribution=attribution,
                               policy=policy, registry=registry, clock=clock,
                               db_path=getattr(book, 'db_path', None))
    # The accounting ledger and the receipt importer share the same connection,
    # so a broker execution, its attribution and its cash effect commit together.
    service.accounting = InstanceAccounting(book.db, clock=clock)
    return service
