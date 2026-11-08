"""Martin compatibility registry over the concrete plugin families.

The generic example catalog lives in ``trading_example_app``. This module is
the Martin application's registry; it is never imported by generic infra.
Unknown ids are refused, never silently served by Martin's worker.
"""
from trading_strategies.martin.signals import MartinSignalPlugin

_FAMILIES = {
    'martin': {
        'version': MartinSignalPlugin.VERSION,
        'state_schema': 'martin-pullback-state-1',
        'data_requirements': ('quotes', '5minute-bars', 'universe-screen'),
        'capabilities': ('advisory-entry', 'structural-stop'),
        'exit_feature_keys': ('ema9',),
        'worker_module': 'martin_luk.strategy_stream_worker',
    },
    'vwap-fixture': {
        'version': '1.0',
        'state_schema': 'vwap-fixture-1',
        'data_requirements': ('quotes',),
        'capabilities': ('advisory-entry',),
        'exit_feature_keys': ('session_vwap',),
        'worker_module': 'trading_example_app.stream_worker',
    },
    # Synthetic timeout fixture; it has no production trading capability.
    'slow-exit-fixture': {
        'version': '1.0',
        'state_schema': 'slow-exit-fixture-1',
        'data_requirements': ('quotes',),
        'capabilities': (),
        'exit_feature_keys': ('session_vwap',),
        'worker_module': 'trading_example_app.stream_worker',
    },
}


def create(plugin_id, *, cost_buffer_per_share='0.05', calendar_view=None):
    contract(plugin_id)
    if plugin_id == 'martin':
        from trading_strategies.martin.signals import MartinSignalPlugin
        return MartinSignalPlugin(calendar_view=calendar_view,
                                  cost_buffer_per_share=cost_buffer_per_share)
    from trading_example_app import catalog as example_catalog
    return example_catalog.create(
        plugin_id, cost_buffer_per_share=cost_buffer_per_share,
        calendar_view=calendar_view)


def contract(plugin_id):
    try:
        return dict(_FAMILIES[plugin_id])
    except KeyError:
        raise ValueError('STRATEGY_PLUGIN_UNSUPPORTED:' + str(plugin_id)) from None


def worker_command(plugin_id, python, *, cost_buffer_per_share, source_fingerprint):
    family = contract(plugin_id)
    return [python, '-I', '-m', family['worker_module'], '--plugin', plugin_id,
            '--cost-buffer-per-share', str(cost_buffer_per_share),
            '--source-fingerprint', str(source_fingerprint)]
