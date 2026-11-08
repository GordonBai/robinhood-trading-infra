"""Bounded read-only historical acquisition; request coverage is not provider attestation.

The reader is injected: this module never opens a network connection or trades.
A response below its requested cap does not prove exhaustion. Explicit captured
continuation evidence is required; saturated windows are subdivided when possible.
"""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import uuid


_ALLOWED = {'tool', 'symbols', 'start', 'end', 'limit', 'feed', 'sort', 'timeframe'}
_TOOLS = {'get_stock_quotes': 'quotes', 'get_stock_bars': 'bars'}
_SECRET = re.compile(r'token|secret|password|authorization|api.?key|credential|cookie', re.I)
_TIME = re.compile(r'^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?(Z|[+-]\d\d:\d\d)$')


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def _hash(value):
    return hashlib.sha256(_canonical(value).encode()).hexdigest()


def _ns(value):
    match = _TIME.fullmatch(value) if isinstance(value, str) else None
    if not match:
        raise ValueError('INVALID_TIMESTAMP')
    dt = datetime.fromisoformat(match[1] + match[3].replace('Z', '+00:00'))
    return int(dt.timestamp()) * 1_000_000_000 + int((match[2] or '').ljust(9, '0'))


def _stamp(ns):
    seconds, fraction = divmod(ns, 1_000_000_000)
    return datetime.fromtimestamp(seconds, timezone.utc).strftime('%Y-%m-%dT%H:%M:%S') + f'.{fraction:09d}Z'


def _safe(value):
    if isinstance(value, dict):
        if any(_SECRET.search(str(key)) and str(key) != 'next_page_token' for key in value):
            raise ValueError('SECRET_FIELD_REJECTED')
        for child in value.values():
            _safe(child)
    elif isinstance(value, list):
        for child in value:
            _safe(child)


def _request(value):
    if not isinstance(value, dict) or set(value) - _ALLOWED:
        raise ValueError('REQUEST_FIELDS_INVALID')
    result = deepcopy(value)
    if result.get('tool') not in _TOOLS or result.get('feed') != 'sip' or result.get('sort') != 'asc':
        raise ValueError('EXPLICIT_READ_TOOL_SIP_ASC_REQUIRED')
    symbols = result.get('symbols')
    if not isinstance(symbols, list) or not symbols or len(set(symbols)) != len(symbols) or any(not isinstance(s, str) or not re.fullmatch(r'[A-Z][A-Z0-9.\-]{0,14}', s) for s in symbols):
        raise ValueError('EXPLICIT_SYMBOLS_REQUIRED')
    if type(result.get('limit')) is not int or not 1 <= result['limit'] <= 10000:
        raise ValueError('LIMIT_INVALID')
    if _ns(result.get('start')) >= _ns(result.get('end')):
        raise ValueError('WINDOW_INVALID')
    if result['tool'] == 'get_stock_bars' and (not isinstance(result.get('timeframe'), str) or not result['timeframe']):
        raise ValueError('TIMEFRAME_REQUIRED')
    if result['tool'] == 'get_stock_quotes' and 'timeframe' in result:
        raise ValueError('QUOTES_TIMEFRAME_INVALID')
    return result


def _response(request, response):
    if not isinstance(response, dict):
        raise ValueError('RESPONSE_OBJECT_REQUIRED')
    _safe(response)
    if response.get('tool') != request['tool']:
        raise ValueError('RESPONSE_TOOL_MISMATCH')
    echoed = response.get('request')
    if not isinstance(echoed, dict) or any(echoed.get(k) != v for k, v in request.items() if k != 'tool'):
        raise ValueError('RESPONSE_REQUEST_MISMATCH')
    pagination = response.get('pagination')
    metadata = [response] + ([pagination] if isinstance(pagination, dict) else [])
    exhaustion_claimed = isinstance(pagination, dict) and pagination.get('exhausted') is True
    for meta in metadata:
        for flag in ('truncated', 'has_more', 'has_next_page', 'exhausted', 'pagination_exhausted'):
            if flag in meta and type(meta[flag]) is not bool:
                raise ValueError('PAGINATION_FLAG_INVALID')
        if meta.get('truncated') is True:
            raise ValueError('RESPONSE_TRUNCATED')
        continuation = (meta.get('next_page_token') is not None or meta.get('has_more') is True
                        or meta.get('has_next_page') is True)
        rejects_exhaustion = meta.get('exhausted') is False or meta.get('pagination_exhausted') is False
        if exhaustion_claimed and (continuation or rejects_exhaustion):
            raise ValueError('PAGINATION_EVIDENCE_CONTRADICTORY')
    family = _TOOLS[request['tool']]
    rows = response.get(family)
    if not isinstance(rows, dict) or set(rows) != set(request['symbols']):
        raise ValueError('RESPONSE_SYMBOL_COVERAGE_MISSING_OR_EXTRA')
    flattened = []
    for symbol in sorted(rows):
        entries = rows[symbol]
        if not isinstance(entries, list):
            raise ValueError('RECORD_LIST_REQUIRED')
        previous = None
        for row in entries:
            if not isinstance(row, dict) or row.get('symbol', symbol) != symbol:
                raise ValueError('RECORD_SYMBOL_MISMATCH')
            event = _ns(row.get('timestamp'))
            if not _ns(request['start']) <= event <= _ns(request['end']):
                raise ValueError('RECORD_OUTSIDE_WINDOW')
            if previous is not None and event < previous:
                raise ValueError('RECORD_SORT_MISMATCH')
            previous = event
            flattened.append((symbol, event, row))
    counts = response.get('counts')
    if not isinstance(counts, dict) or type(counts.get('records')) is not int or counts['records'] != len(flattened) or not isinstance(counts.get('per_symbol'), dict) or any(type(v) is not int for v in counts['per_symbol'].values()) or counts.get('per_symbol') != {s: len(rows[s]) for s in request['symbols']}:
        raise ValueError('RESPONSE_COUNTS_MISMATCH')
    if len(flattened) > request['limit']:
        raise ValueError('RESPONSE_EXCEEDS_REQUEST_LIMIT')
    return flattened


def acquire(requests, output_dir, reader, *, max_requests=100, max_depth=20,
            max_response_bytes=20_000_000, resume_manifest=None, clock=None,
            reader_observation_kind='injected_reader_return'):
    """Persist a new immutable run, including failures and budget-limited gaps.

    ``reader(request)`` returns the explicit envelope described in this module.
    Resume revalidates raw captures and reruns the subdivision tree; cached calls
    do not consume the new request budget. Missing/modified captures stop reuse.
    No pagination tokens are inferred or followed.
    """
    now = clock or (lambda: datetime.now(timezone.utc))
    run = Path(output_dir) / ('acquisition-' + uuid.uuid4().hex)
    run.mkdir(parents=True, exist_ok=False)
    manifest = {'schema_version': 1, 'run_id': run.name, 'started_at': now().isoformat(),
                'scope': 'bounded_request_window_coverage', 'reader_observation_kind': reader_observation_kind,
                'provider_network_requests_verified': False, 'reader_must_enforce_transport_deadlines': True, 'provider_authenticity_verified': False,
                'completeness_assumption': 'none; below-cap responses do not prove pagination exhaustion',
                'pagination_inferred': False,
                'limitations': ['provider timestamp precision is not independently verified',
                    'quotes without provider identity use exact-record overlap deduplication; repeated identical updates across captures cannot be distinguished'],
                'captures': [], 'coverage': [], 'gaps': [],
                'reader_calls': 0, 'requests': [], 'records_path': None,
                'complete': False, 'request_plan_captured': False, 'failures': []}
    with (run / 'run-start.json').open('x') as output:
        output.write(_canonical({'run_id': manifest['run_id'], 'started_at': manifest['started_at'],
                                 'scope': manifest['scope'], 'state': 'STARTED_NOT_COMPLETED'}) + '\n')
    cache = {}
    records = {}
    provenance = {}
    try:
        if reader_observation_kind not in ('injected_reader_return', 'offline_capture_replay'):
            raise ValueError('READER_OBSERVATION_KIND_INVALID')
        if type(max_requests) is not int or max_requests < 0 or type(max_depth) is not int or not 0 <= max_depth <= 60 or type(max_response_bytes) is not int or max_response_bytes < 1:
            raise ValueError('BUDGET_INVALID')
        normalized = [_request(r) for r in requests]
        if not normalized:
            raise ValueError('REQUESTS_REQUIRED')
        manifest['requests'] = normalized
        manifest['budgets'] = {'max_requests': max_requests, 'max_depth': max_depth, 'max_response_bytes': max_response_bytes}
        if resume_manifest:
            previous_path = Path(resume_manifest).resolve()
            previous = json.loads(previous_path.read_text())
            if previous.get('schema_version') != 1 or previous.get('requests') != normalized:
                raise ValueError('RESUME_REQUESTS_MISMATCH')
            manifest['resumed_from'] = {'manifest_sha256': _hash(previous), 'run_id': previous.get('run_id')}
            for capture in previous.get('captures', []):
                relative = Path(capture['path'])
                if relative.is_absolute() or '..' in relative.parts:
                    raise ValueError('RESUME_CAPTURE_PATH_INVALID')
                raw = json.loads((previous_path.parent / relative).read_text())
                if _hash(raw) != capture['sha256'] or _hash(raw['request']) != capture['request_sha256']:
                    raise ValueError('RESUME_CAPTURE_HASH_MISMATCH')
                if len(_canonical(raw['response']).encode()) > max_response_bytes:
                    raise ValueError('RESUME_RESPONSE_BYTE_BUDGET_EXCEEDED')
                if raw['acquired_at'] is not None:
                    _ns(raw['acquired_at'])
                _ns(raw.get('observed_at', raw['acquired_at']))
                _request(raw['request'])
                _response(raw['request'], raw['response'])
                cache[capture['request_sha256']] = raw

        def visit(request, depth):
            request_hash = _hash(request)
            raw = cache.get(request_hash)
            reused = raw is not None
            if raw is None:
                if manifest['reader_calls'] >= max_requests:
                    manifest['gaps'].append({'request': request, 'reason': 'REQUEST_BUDGET_EXHAUSTED'})
                    return
                manifest['reader_calls'] += 1
                request_started_at = now().isoformat()
                try:
                    response = reader(deepcopy(request))
                    observed_at = now().isoformat()
                    if _ns(observed_at) < _ns(request_started_at):
                        raise ValueError('RECEIPT_BEFORE_REQUEST_START')
                    acquired_at = None if reader_observation_kind == 'offline_capture_replay' else observed_at
                    _safe(response)
                    if len(_canonical(response).encode()) > max_response_bytes:
                        raise ValueError('RESPONSE_BYTE_BUDGET_EXCEEDED')
                    raw = {'request': request, 'response': response, 'acquired_at': acquired_at, 'observed_at': observed_at, 'request_started_at': request_started_at,
                           'acquired_at_scope': 'unknown_original_acquisition' if reader_observation_kind == 'offline_capture_replay' else 'local_reader_observation_not_provider_attestation'}
                except Exception as exc:
                    # Never persist exception text: transport errors can contain credentials.
                    manifest['gaps'].append({'request': request, 'reason': 'READER_FAILED', 'error_type': type(exc).__name__,
                                             'request_started_at': request_started_at, 'failed_at': now().isoformat()})
                    return
            capture_name = f'capture-{len(manifest["captures"]):06d}.json'
            with (run / capture_name).open('x') as output:
                output.write(_canonical(raw) + '\n')
            manifest['captures'].append({'path': capture_name, 'sha256': _hash(raw),
                'request_sha256': request_hash, 'response_sha256': _hash(raw['response']),
                'request_started_at': raw.get('request_started_at'), 'acquired_at': raw['acquired_at'], 'observed_at': raw.get('observed_at'), 'acquired_at_scope': raw.get('acquired_at_scope'), 'reused': reused})
            try:
                entries = _response(request, raw['response'])
            except ValueError as exc:
                manifest['gaps'].append({'request': request, 'reason': str(exc)})
                return
            occurrences = {}
            for source_index, (symbol, event, row) in enumerate(entries):
                normalized_row = row | {'symbol': symbol, 'timestamp': _stamp(event)}
                identity = (request['tool'], request.get('timeframe'), symbol, event)
                if request['tool'] == 'get_stock_quotes':
                    provider_id = row.get('quote_id', row.get('id', row.get('sequence')))
                    if provider_id is not None:
                        identity = (request['tool'], symbol, 'provider_id', _canonical(provider_id))
                    else:
                        # A timestamp, even with venues, is not a unique quote ID.
                        # Preserve distinct updates and within-response multiplicity.
                        digest = _hash(normalized_row)
                        occurrence = occurrences.get(digest, 0)
                        occurrences[digest] = occurrence + 1
                        identity += ('exact_record_assumption', digest, occurrence)
                if identity in records and records[identity] != normalized_row:
                    manifest['gaps'].append({'request': request, 'reason': 'CONFLICTING_RECORDS', 'symbol': symbol, 'event_at': _stamp(event)})
                    return
                records[identity] = normalized_row
                provenance.setdefault(identity, []).append({'capture': capture_name, 'flattened_source_index': source_index, 'flattening_order': 'sorted_symbol_then_raw_array_index'})
            if len(entries) >= request['limit']:
                start, end = _ns(request['start']), _ns(request['end'])
                middle = (start + end) // 2
                if depth >= max_depth or middle in (start, end):
                    manifest['gaps'].append({'request': request, 'reason': 'SATURATED_WINDOW_UNSPLITTABLE', 'depth': depth})
                    return
                # Inclusive overlap at midpoint avoids guessing provider end exclusivity.
                visit(request | {'end': _stamp(middle)}, depth + 1)
                visit(request | {'start': _stamp(middle)}, depth + 1)
            else:
                pagination = raw['response'].get('pagination')
                exhausted = (isinstance(pagination, dict) and pagination.get('exhausted') is True
                             and 'next_page_token' in pagination and pagination['next_page_token'] is None)
                manifest['coverage'].append({'request': request, 'request_sha256': request_hash, 'records': len(entries), 'depth': depth,
                    'status': 'explicit_exhaustion_reported' if exhausted else 'request_window_returned_only'})
                if not exhausted:
                    manifest['gaps'].append({'request': request, 'reason': 'CONTINUATION_UNVERIFIABLE'})

        for request in normalized:
            visit(request, 0)
        records_name = 'records.json'
        ordered_items = sorted(records.items(), key=lambda item: (_ns(item[1]['timestamp']), item[1]['symbol'], repr(item[0])))
        ordered = [row for _, row in ordered_items]
        with (run / records_name).open('x') as output:
            output.write(_canonical(ordered) + '\n')
        with (run / 'record-provenance.json').open('x') as output:
            output.write(_canonical([{'record_sha256': _hash(row), 'sources': provenance[key]} for key, row in ordered_items]) + '\n')
        manifest['provenance_path'] = 'record-provenance.json'
        manifest['records_path'] = records_name
        manifest['records_sha256'] = _hash(ordered)
        manifest['unique_records'] = len(ordered)
        manifest['request_plan_captured'] = all(gap['reason'] == 'CONTINUATION_UNVERIFIABLE' for gap in manifest['gaps'])
        manifest['complete'] = not manifest['gaps']
    except Exception as exc:
        # Store only our stable validation code; arbitrary external exceptions are redacted.
        reason = str(exc) if type(exc) is ValueError and re.fullmatch('[A-Z_]+', str(exc)) else type(exc).__name__
        manifest['failures'].append(reason)
    manifest['finished_at'] = now().isoformat()
    with (run / 'manifest.json').open('x') as output:
        output.write(_canonical(manifest) + '\n')
    return manifest | {'manifest_path': str((run / 'manifest.json').resolve())}
