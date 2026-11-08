"""Exit rules owned by the example plugins, not by Martin or the core."""
import time


def vwap_advise(view):
    vwap = view.get('features', {}).get('session_vwap')
    if vwap is None:
        return {'action': 'HOLD', 'reason': 'NO_SESSION_VWAP'}
    if float(view['quote']['bid']) < float(vwap):
        return {'action': 'EXIT', 'reason': 'BELOW_SESSION_VWAP'}
    return {'action': 'HOLD', 'reason': 'ABOVE_SESSION_VWAP'}


def slow_exit_advise(view):
    time.sleep(5)
    return {'action': 'EXIT', 'reason': 'SLOW_FIXTURE_MUST_TIMEOUT'}


EXIT_RULES = {
    'vwap-fixture': 'trading_example_plugins.rules:vwap_advise',
    'slow-exit-fixture': 'trading_example_plugins.rules:slow_exit_advise',
}
FEATURE_KEYS = {
    'vwap-fixture': ('session_vwap',),
    'slow-exit-fixture': ('session_vwap',),
}
