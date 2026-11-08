"""Application composition: bind the Martin exit strategy to the neutral boundary.

The execution book hands over an opaque per-symbol feature mapping. This module
is the only place that decides which concrete exit strategy interprets it, so a
different strategy can be composed here without touching the book.
"""
from trading_infra.exit_policy import evaluate_exit
from trading_strategies.martin import MartinExitStrategy


def exit_decision(position, quote, *, as_of, features=None, earnings_date=None,
                  allow_overnight=False, strategy=None, daily_ema9=None):
    """Compose one exit decision.

    ``features`` is the neutral, strategy-declared mapping handed over by the
    execution book. ``daily_ema9`` is the older single-feature spelling kept for
    existing composition callers; it is translated here and nowhere else, so no
    indicator name reaches the book.
    """
    if daily_ema9 is not None:
        if features is not None:
            raise ValueError('EXIT_FEATURES_AMBIGUOUS')
        features = {'ema9': daily_ema9}
    return evaluate_exit(position, quote, as_of=as_of, strategy=strategy or MartinExitStrategy(),
        features=features, earnings_date=earnings_date, allow_overnight=allow_overnight)
