"""Finite read-only collection, atomic captures, and shadow inbox publication."""

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import time
import uuid

from .market import RobinhoodReader, inspect_recording, validate_request
from .transport import CodexMarketTransport, TransportError


def market_events(recording, *, as_of):
    normalized = inspect_recording(recording, as_of=as_of)
    digest = hashlib.sha256(json.dumps(recording, sort_keys=True).encode()).hexdigest()
    base = {'at': as_of, 'source_sha256': digest, 'synthetic_input': recording['synthetic']}
    events = [{**base, 'id': digest + '-health', 'kind': 'data_status',
               'healthy': normalized['data_usable'],
               'errors': normalized['errors'] + normalized['freshness_errors']}]
    # A partially invalid batch must never produce trade inputs.
    if normalized['data_usable']:
        for bar in sorted(normalized['bars'], key=lambda b: b['begins_at']):
            if bar['interval'] == '5minute':
                events.append({**base, 'kind': 'bar', 'data': bar})
        for quote in normalized['quotes']:
            events.append({**base, 'kind': 'quote', 'data': quote})
    for index, event in enumerate(events[1:], 1):
        event['id'] = digest + '-' + str(index)
    return events, normalized


def _publish(directory, payload):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    name = uuid.uuid4().hex
    temp, final = directory / (name + '.tmp'), directory / (name + '.json')
    try:
        with temp.open('x') as handle:
            json.dump(payload, handle, indent=2)
            handle.flush()
        temp.replace(final)
    finally:
        temp.unlink(missing_ok=True)
    return str(final)


def capture_market(requests, output_dir, *, cycles=1, poll_seconds=5, inbox=None,
                   binary='codex', transport_factory=None, clock=None, sleeper=time.sleep):
    if not isinstance(requests, list) or not 1 <= len(requests) <= 8:
        raise ValueError('Require 1–8 explicit market-data requests')
    if type(cycles) is not int or not 1 <= cycles <= 1000 or not 1 <= poll_seconds <= 60:
        raise ValueError('Require 1–1000 cycles and a poll interval of 1–60 seconds')
    if inbox and Path(inbox).resolve() == Path(output_dir).resolve():
        raise ValueError('Capture directory and event inbox must be different')
    for request in requests:
        if not isinstance(request, dict) or set(request) != {'tool', 'arguments'}:
            raise ValueError('Each request requires only tool and arguments')
        validate_request(request['tool'], request['arguments'])
    clock = clock or (lambda: datetime.now(timezone.utc))
    factory = transport_factory or (lambda: CodexMarketTransport(binary=binary))
    summaries = []
    # No retry on unknown authentication or approval state. The caller may reconnect
    # explicitly; each invocation creates a new official authenticated connection.
    try:
        with factory() as transport:
            reader = RobinhoodReader(transport, clock=clock)
            for cycle in range(cycles):
                recording = {'schema_version': 1, 'provider': 'robinhood', 'synthetic': False,
                             'records': [reader.fetch(r['tool'], r['arguments']) for r in requests]}
                now = clock().isoformat()
                events, normalized = market_events(recording, as_of=now)
                capture_path = _publish(output_dir, recording)
                event_path = _publish(inbox, {'events': events}) if inbox else None
                summaries.append({'capture': capture_path, 'events': event_path, 'at': now,
                                  'data_usable': normalized['data_usable'],
                                  'quotes': len(normalized['quotes']), 'bars': len(normalized['bars']),
                                  'indicators': len(normalized['indicators']),
                                  'errors': normalized['errors'] + normalized['freshness_errors']})
                if cycle + 1 < cycles:
                    sleeper(poll_seconds)
    except (TransportError, OSError, ValueError, KeyError, TypeError, KeyboardInterrupt):
        if inbox:
            _publish(inbox, {'events': [{'id': 'source-failure-' + uuid.uuid4().hex,
                'kind': 'data_status', 'at': clock().isoformat(), 'healthy': False,
                'errors': [{'reason': 'AUTHENTICATED_SOURCE_UNAVAILABLE'}]}]})
        raise
    if inbox:
        _publish(inbox, {'events': [{'id': 'source-stopped-' + uuid.uuid4().hex,
            'kind': 'data_status', 'at': clock().isoformat(), 'healthy': False,
            'errors': [{'reason': 'BOUNDED_COLLECTION_STOPPED'}]}]})
    return {'mode': 'shadow', 'authenticated_market_read_verified': True,
            'data_usable': all(c['data_usable'] for c in summaries),
            'cycles_completed': len(summaries), 'captures': summaries, 'live_orders': 0,
            'continuous_source_verified': False, 'execution_ready': False}
