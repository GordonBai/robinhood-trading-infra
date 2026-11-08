"""Plugin catalog for the example application. It never imports Martin."""
from trading_infra.exit_composition import ExitRuleRegistry

from trading_example_plugins.rules import EXIT_RULES, FEATURE_KEYS
from trading_example_plugins.slow_exit import SlowExitFixturePlugin
from trading_example_plugins.vwap import VwapFixturePlugin

_FAMILIES = {
    'vwap-fixture': {
        'version': VwapFixturePlugin.VERSION,
        'state_schema': 'vwap-fixture-1',
        'data_requirements': ('quotes',),
        'capabilities': ('advisory-entry',),
        'worker_module': 'trading_example_app.stream_worker',
    },
    'slow-exit-fixture': {
        'version': SlowExitFixturePlugin.VERSION,
        'state_schema': 'slow-exit-fixture-1',
        'data_requirements': ('quotes',),
        'capabilities': (),
        'worker_module': 'trading_example_app.stream_worker',
    },
}


def contract(plugin_id):
    try:
        return dict(_FAMILIES[plugin_id])
    except KeyError:
        raise ValueError('STRATEGY_PLUGIN_UNSUPPORTED:' + str(plugin_id)) from None


def create(plugin_id, *, cost_buffer_per_share='0.05', calendar_view=None):
    contract(plugin_id)
    if plugin_id == 'vwap-fixture':
        return VwapFixturePlugin(cost_buffer_per_share=cost_buffer_per_share)
    if plugin_id == 'slow-exit-fixture':
        return SlowExitFixturePlugin(cost_buffer_per_share=cost_buffer_per_share)
    raise ValueError('STRATEGY_PLUGIN_WORKER_UNIMPLEMENTED:' + plugin_id)


def worker_command(plugin_id, python, *, cost_buffer_per_share, source_fingerprint):
    family = contract(plugin_id)
    return [python, '-I', '-m', family['worker_module'], '--plugin', plugin_id,
            '--cost-buffer-per-share', str(cost_buffer_per_share),
            '--source-fingerprint', str(source_fingerprint)]


EXIT_REGISTRY = ExitRuleRegistry(EXIT_RULES, FEATURE_KEYS)
