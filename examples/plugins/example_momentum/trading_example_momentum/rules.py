"""Bounded exit rules for the example plugins; no account or broker access."""


def momentum_exit(view):
    anchor = view.get('features', {}).get('close_ma')
    if anchor is None:
        return {'action': 'HOLD', 'reason': 'NO_CLOSE_MA'}
    if float(view['quote']['bid']) < float(anchor):
        return {'action': 'EXIT', 'reason': 'MOMENTUM_LOST_AVERAGE'}
    return {'action': 'HOLD', 'reason': 'ABOVE_CLOSE_MA'}


def mean_reversion_exit(view):
    anchor = view.get('features', {}).get('entry_anchor')
    if anchor is None:
        return {'action': 'HOLD', 'reason': 'NO_ENTRY_ANCHOR'}
    if float(view['quote']['ask']) >= float(anchor):
        return {'action': 'EXIT', 'reason': 'MEAN_REVERSION_REACHED_AVERAGE'}
    return {'action': 'HOLD', 'reason': 'BELOW_ENTRY_ANCHOR'}
