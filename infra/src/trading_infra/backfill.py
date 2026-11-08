"""Split a historical batch into chunks the input window can actually absorb.

One cold-start batch measured 24.9 ms per event against a 30-second input-age
window, so a single batch holds roughly 1200 events. A whole trading day of
5-minute bars over 30 symbols is about 2340 events, which does not fit: enqueued
at once, the tail expires before it is prepared.

That is a usage error, not a throughput failure. This module makes the correct
usage mechanical: chunk by 5-minute window first (a window is the natural unit
of arrival), then by an event ceiling. Ordering inside the batch is preserved so
a replay still sees bars in the sequence the venue published them.
"""
from datetime import datetime

DEFAULT_MAX_EVENTS = 1200
DEFAULT_MAX_SECONDS = 30
DEFAULT_FORWARD_SECONDS_PER_EVENT = 0.025


def _window_key(event):
    """The arrival slice an event belongs to: its completion time, else its own."""
    data = event.get('data') if isinstance(event, dict) else None
    for name in ('available_at', 'begins_at'):
        if isinstance(data, dict) and isinstance(data.get(name), str):
            return data[name]
    at = event.get('at') if isinstance(event, dict) else None
    return at if isinstance(at, str) else ''


def plan_chunks(events, *, max_events=DEFAULT_MAX_EVENTS,
                max_seconds=DEFAULT_MAX_SECONDS,
                seconds_per_event=DEFAULT_FORWARD_SECONDS_PER_EVENT):
    """Return ordered chunks, each of which should drain inside one window.

    The plan is derived from the caller's declared cost, not from a measured
    run, so a slower machine lowers ``max_events`` rather than silently
    producing chunks that expire. ``max_events`` alone is the binding limit when
    ``max_seconds`` would allow more.
    """
    if not isinstance(events, (list, tuple)) or not events:
        raise ValueError('BACKFILL_EVENTS_REQUIRED')
    if type(max_events) is not int or max_events < 1:
        raise ValueError('BACKFILL_LIMIT_INVALID')
    if not 0 < max_seconds <= 600 or not 0 < seconds_per_event <= 5:
        raise ValueError('BACKFILL_LIMIT_INVALID')
    per_window = max(1, int(max_seconds / seconds_per_event))
    ceiling = min(max_events, per_window)
    chunks = []
    current = []
    current_window = None
    for event in events:
        if not isinstance(event, dict) or not isinstance(event.get('id'), str):
            raise ValueError('BACKFILL_EVENT_INVALID')
        window = _window_key(event)
        split_for_window = current and window != current_window
        if current and (split_for_window or len(current) >= ceiling):
            chunks.append(current)
            current = []
        if not current:
            current_window = window
        current.append(event)
    if current:
        chunks.append(current)
    return {'chunks': chunks, 'chunk_count': len(chunks),
            'events': len(events), 'ceiling_per_chunk': ceiling,
            'estimated_seconds_per_chunk': round(ceiling * seconds_per_event, 3),
            'fits_one_batch': len(events) <= ceiling}


def describe(plan):
    """Compact, durable summary suitable for a batch report."""
    return {'events': plan['events'], 'chunks': plan['chunk_count'],
            'ceiling_per_chunk': plan['ceiling_per_chunk'],
            'fits_one_batch': plan['fits_one_batch'],
            'chunk_sizes': [len(chunk) for chunk in plan['chunks']]}
