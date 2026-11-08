"""Acceptance rules for the C3 default dual-plugin throughput evidence.

The checker treats the exported raw events/tasks/checkpoint/proposal records as
the authority. Summary fields are cross-checked, never trusted on their own.
"""
from datetime import datetime
from decimal import Decimal, InvalidOperation
import math

MAX_INPUT_AGE_SECONDS = 30
MAX_QUOTE_AGE_SECONDS = 15
MIN_HISTORICAL_SYMBOLS = 30
MIN_HISTORICAL_DAILY_BARS = 1920
MIN_QUOTE_ROUNDS = 12
MIN_QUOTE_EVENTS = 360
STALE_ID = 'c3-stale-control'
KIND_REQUIREMENT = {'quote': 'quotes', 'bar': '5minute-bars',
                    'universe': 'universe-screen'}
KNOWN_KINDS = set(KIND_REQUIREMENT) | {'data_status'}
EXPECTED_PLUGINS = {'martin', 'vwap-fixture'}


def _time(value):
    if not isinstance(value, str):
        raise ValueError('TIME_NOT_STRING')
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError('TIME_NOT_AWARE')
    return parsed


def _number(value):
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not result.is_finite():
        return None
    return result


def _raw(report):
    raw = report.get('raw') if isinstance(report, dict) else None
    if not isinstance(raw, dict):
        return None
    for key in ('events', 'tasks', 'proposals', 'checkpoints', 'manifests',
                'market_call_times'):
        if key not in raw:
            return None
    return raw


def evaluate_report(report):
    """Return sorted failure reasons. Empty list means acceptable."""
    if not isinstance(report, dict):
        return ['REPORT_NOT_OBJECT']
    failures = []
    raw = _raw(report)
    if raw is None:
        return ['RAW_EVIDENCE_MISSING']
    events, tasks = raw['events'], raw['tasks']
    manifests = raw['manifests']
    if not isinstance(events, list) or not events or not isinstance(tasks, list) or not tasks:
        return ['RAW_RECORDS_EMPTY']
    if not isinstance(manifests, dict) or len(manifests) < 2:
        return ['TWO_INSTANCE_MANIFESTS_MISSING']
    plugin_ids = {manifest.get('plugin_id') for manifest in manifests.values()
                  if isinstance(manifest, dict)}
    if plugin_ids != EXPECTED_PLUGINS:
        failures.append('PLUGIN_SET_MISMATCH')
    for instance, manifest in manifests.items():
        requirements = manifest.get('data_requirements') if isinstance(manifest, dict) else None
        if not isinstance(requirements, list):
            failures.append('MANIFEST_REQUIREMENTS_MISSING:%s' % instance)
    # -- clock and formal boundaries ------------------------------------
    if report.get('clock_mode') != 'monotonic':
        failures.append('CLOCK_NOT_MONOTONIC')
    started = _number(report.get('clock_started_monotonic'))
    ended = _number(report.get('clock_ended_monotonic'))
    elapsed = _number(report.get('elapsed_seconds'))
    if started is None or ended is None or elapsed is None or ended <= started:
        failures.append('CLOCK_DID_NOT_ADVANCE')
    if (started is not None and ended is not None and elapsed is not None
            and abs((ended - started) - elapsed) > max(Decimal('1.0'), abs(elapsed) * Decimal('.01'))):
        failures.append('ELAPSED_INCONSISTENT')
    if elapsed is None or elapsed <= 0:
        failures.append('ELAPSED_NOT_POSITIVE')
    if report.get('source_unchanged') is not True:
        failures.append('SOURCE_CHANGED')
    if report.get('broker_calls_by_formal', 0) != 0:
        failures.append('FORMAL_BROKER_CALL')
    if report.get('formal_io_calls', 0) != 0:
        failures.append('FORMAL_IO_CALL')
    if int(report.get('execution_intents', -1)) != 0:
        failures.append('EXECUTION_INTENT_CREATED')
    if int(report.get('entry_actions', -1)) != 0:
        failures.append('ENTRY_ACTION_CREATED')
    # -- event identity and source shape --------------------------------
    event_by_id = {}
    quote_rounds = {}
    quote_symbols = set()
    bar_count = 0
    universe = None
    for event in events:
        if not isinstance(event, dict):
            failures.append('EVENT_NOT_OBJECT')
            continue
        event_id = event.get('id')
        kind = event.get('kind')
        if not isinstance(event_id, str) or not event_id:
            failures.append('EVENT_ID_MISSING')
            continue
        if event_id in event_by_id:
            failures.append('DUPLICATE_EVENT_ID')
            continue
        event_by_id[event_id] = event
        if kind not in KNOWN_KINDS:
            failures.append('UNKNOWN_EVENT_KIND')
            continue
        try:
            event_at = _time(event.get('at'))
        except ValueError:
            failures.append('EVENT_TIME_INVALID')
            continue
        if kind == 'universe':
            universe = event
        elif kind == 'bar':
            bar_count += 1
            data = event.get('data')
            if not isinstance(data, dict):
                failures.append('BAR_PAYLOAD_MISSING')
        elif kind == 'quote':
            data = event.get('data')
            if not isinstance(data, dict):
                failures.append('QUOTE_PAYLOAD_MISSING')
                continue
            try:
                bid, ask = _number(data.get('bid')), _number(data.get('ask'))
                qtimes = [_time(data.get(key)) for key in
                          ('bid_at', 'ask_at', 'observed_at')]
            except (ValueError, TypeError):
                failures.append('QUOTE_TIME_INVALID')
                continue
            if bid is None or ask is None or bid <= 0 or ask < bid:
                failures.append('QUOTE_PRICE_INVALID')
            if any(qt > event_at for qt in qtimes):
                failures.append('QUOTE_FUTURE')
            if event_id.startswith('c3-q-'):
                pieces = event_id.split('-')
                if len(pieces) >= 4:
                    quote_rounds[pieces[2]] = quote_rounds.get(pieces[2], 0) + 1
                quote_symbols.add(data.get('symbol'))
    if universe is None:
        failures.append('UNIVERSE_BATCH_MISSING')
    else:
        data = universe.get('data') or {}
        histories = data.get('histories') if isinstance(data, dict) else None
        if not isinstance(histories, dict) or len(histories) < MIN_HISTORICAL_SYMBOLS:
            failures.append('HISTORICAL_SYMBOL_COUNT_TOO_SMALL')
        else:
            bars = sum(len(value) for value in histories.values() if isinstance(value, list))
            if bars < MIN_HISTORICAL_DAILY_BARS:
                failures.append('HISTORICAL_BAR_COUNT_TOO_SMALL')
    if len(quote_rounds) < MIN_QUOTE_ROUNDS or sum(quote_rounds.values()) < MIN_QUOTE_EVENTS:
        failures.append('STEADY_QUOTE_WORKLOAD_TOO_SMALL')
    if len([value for value in quote_symbols if value]) < MIN_HISTORICAL_SYMBOLS:
        failures.append('QUOTE_SYMBOL_COUNT_TOO_SMALL')
    if bar_count < 2:
        failures.append('FLUSH_RECLAIM_BARS_MISSING')
    if 'c3-trigger-quote' not in event_by_id:
        failures.append('TRIGGER_QUOTE_MISSING')
    # -- expected routing and actual task records -----------------------
    expected_committed = {instance: 0 for instance in manifests}
    stale_expected = {instance: 0 for instance in manifests}
    routed = set()
    for event in event_by_id.values():
        kind = event.get('kind')
        required = KIND_REQUIREMENT.get(kind)
        for instance, manifest in manifests.items():
            requirements = set(manifest.get('data_requirements') or [])
            if required is not None and required not in requirements:
                continue
            routed.add((instance, event['id']))
            if event['id'] == STALE_ID:
                stale_expected[instance] += 1
            else:
                expected_committed[instance] += 1
    actual = {}
    created = {}
    all_task_ids = set()
    rejection_reasons = {}
    for task in tasks:
        if not isinstance(task, dict):
            failures.append('TASK_NOT_OBJECT')
            continue
        instance, event_id = task.get('instance_id'), task.get('event_id')
        if (instance, event_id) not in routed:
            failures.append('TASK_NOT_ROUTED')
            continue
        key = (instance, event_id)
        if key in all_task_ids:
            failures.append('DUPLICATE_TASK')
            continue
        all_task_ids.add(key)
        actual.setdefault(instance, {'COMMITTED': 0, 'REJECTED': 0, 'QUEUED': 0,
                                     'RUNNING': 0})
        state = task.get('state')
        if state not in actual[instance]:
            failures.append('TASK_STATE_INVALID')
            continue
        actual[instance][state] += 1
        created.setdefault(instance, []).append(task)
        event = event_by_id.get(event_id)
        if event is None:
            failures.append('TASK_EVENT_MISSING')
            continue
        if event.get('kind') not in KNOWN_KINDS:
            continue
        if state == 'REJECTED':
            reason = task.get('reason')
            if event_id != STALE_ID or reason != 'STALE_STRATEGY_EVENT':
                failures.append('NONCONTROL_REJECTION:%s' % instance)
            rejection_reasons.setdefault(instance, {})[reason or 'UNSPECIFIED'] = \
                rejection_reasons.setdefault(instance, {}).get(reason or 'UNSPECIFIED', 0) + 1
            try:
                finished = _time(task.get('finished_at'))
                event_at = _time(event.get('at'))
                if (finished - event_at).total_seconds() <= MAX_INPUT_AGE_SECONDS:
                    failures.append('STALE_CONTROL_NOT_STALE:%s' % instance)
            except ValueError:
                failures.append('TASK_TIME_INVALID')
            continue
        if state != 'COMMITTED':
            failures.append('UNDRAINED_TAIL:%s' % instance)
            continue
        try:
            queued = _time(task.get('queued_at'))
            started_at = _time(task.get('started_at'))
            finished = _time(task.get('finished_at'))
            event_at = _time(event.get('at'))
        except ValueError:
            failures.append('TASK_TIME_INVALID')
            continue
        if not (queued <= started_at <= finished):
            failures.append('TASK_TIME_ORDER_INVALID')
        age = (finished - event_at).total_seconds()
        if age < 0:
            failures.append('NEGATIVE_EVENT_AGE')
        if event.get('kind') == 'quote':
            data = event.get('data') or {}
            try:
                quote_times = [_time(data.get(key)) for key in
                               ('bid_at', 'ask_at', 'observed_at')]
            except ValueError:
                failures.append('QUOTE_TIME_INVALID')
                continue
            quote_age = max((finished - stamp).total_seconds() for stamp in quote_times)
            if quote_age < 0:
                failures.append('NEGATIVE_QUOTE_AGE')
            if quote_age > MAX_QUOTE_AGE_SECONDS:
                failures.append('QUOTE_AGE_EXCEEDED:%s' % instance)
        elif age > MAX_INPUT_AGE_SECONDS:
            failures.append('INPUT_AGE_EXCEEDED:%s' % instance)
    # Missing tasks (including stale controls) and summary consistency.
    for instance in manifests:
        if actual.get(instance, {}).get('COMMITTED', 0) == 0:
            failures.append('ZERO_COMMITTED:%s' % instance)
        if actual.get(instance, {}).get('QUEUED', 0) or actual.get(instance, {}).get('RUNNING', 0):
            failures.append('UNDRAINED_TAIL:%s' % instance)
        if actual.get(instance, {}).get('COMMITTED', 0) != expected_committed.get(instance, 0):
            failures.append('COMMITTED_MISMATCH:%s' % instance)
        if stale_expected.get(instance, 0) == 0:
            failures.append('STALE_CONTROL_MISSING:%s' % instance)
        if actual.get(instance, {}).get('REJECTED', 0) != stale_expected.get(instance, 0):
            failures.append('UNEXPECTED_REJECTIONS:%s' % instance)
        if rejection_reasons.get(instance, {}).get('STALE_STRATEGY_EVENT', 0) != stale_expected.get(instance, 0):
            failures.append('STALE_CONTROL_NOT_REJECTED:%s' % instance)
    # Summary must agree with the raw denominator and rejected controls.
    summary_expected = (report.get('workload') or {}).get('expected_committed') or {}
    summary_stale = (report.get('workload') or {}).get('stale_control') or {}
    summary_stats = report.get('task_stats') or {}
    for instance in manifests:
        if int(summary_expected.get(instance, -1)) != expected_committed.get(instance, 0):
            failures.append('SUMMARY_EXPECTED_MISMATCH:%s' % instance)
        if int(summary_stale.get(instance, -1)) != stale_expected.get(instance, 0):
            failures.append('SUMMARY_STALE_MISMATCH:%s' % instance)
        compact = {key: value for key, value in (actual.get(instance) or {}).items()
                   if value}
        if summary_stats.get(instance) != compact:
            failures.append('SUMMARY_TASK_STATS_MISMATCH:%s' % instance)
    # -- latency is recomputed from tasks, not copied from summary -----
    for instance in manifests:
        ages, waits = [], []
        for task in created.get(instance, []):
            if task.get('state') != 'COMMITTED':
                continue
            event = event_by_id.get(task.get('event_id'))
            try:
                ages.append((_time(task['finished_at']) - _time(event['at'])).total_seconds())
                waits.append((_time(task['finished_at']) - _time(task['queued_at'])).total_seconds())
            except (TypeError, ValueError):
                failures.append('LATENCY_EVIDENCE_INVALID:%s' % instance)
        if not ages:
            failures.append('NO_LATENCY_EVIDENCE:%s' % instance)
        summary = (report.get('latency') or {}).get(instance) or {}
        if _number(summary.get('max_event_age_seconds')) is None:
            failures.append('SUMMARY_LATENCY_MISSING:%s' % instance)
        elif ages and abs(_number(summary['max_event_age_seconds']) - Decimal(str(max(ages)))) > Decimal('.01'):
            failures.append('SUMMARY_LATENCY_MISMATCH:%s' % instance)
    # -- poll cadence evidence ------------------------------------------
    times = raw.get('market_call_times')
    if not isinstance(times, list) or len(times) < MIN_QUOTE_ROUNDS + 2:
        failures.append('POLL_EVIDENCE_MISSING')
    else:
        try:
            stamps = [_time(value) for value in times]
            intervals = [(b - a).total_seconds() for a, b in zip(stamps, stamps[1:])]
        except (TypeError, ValueError):
            failures.append('POLL_EVIDENCE_INVALID')
        else:
            if report.get('requested_poll_seconds') != 5:
                failures.append('POLL_WINDOW_NOT_FIVE')
            if not any(interval >= 4.5 for interval in intervals):
                failures.append('POLL_CADENCE_NOT_REAL')
            if any(interval > 7.0 for interval in intervals):
                failures.append('POLL_CADENCE_TOO_SLOW')
    # -- proposal identity/TTL -----------------------------------------
    proposals = raw.get('proposals')
    if not isinstance(proposals, list):
        failures.append('RAW_PROPOSALS_MISSING')
    else:
        proposal_counts = {}
        for proposal in proposals:
            if not isinstance(proposal, dict):
                failures.append('PROPOSAL_NOT_OBJECT')
                continue
            instance = proposal.get('instance_id')
            proposal_counts[instance] = proposal_counts.get(instance, 0) + 1
            payload = proposal.get('payload')
            if not isinstance(payload, dict):
                failures.append('PROPOSAL_PAYLOAD_MISSING')
                continue
            try:
                generated = _time(payload.get('generated_at'))
                valid = _time(payload.get('valid_until'))
            except ValueError:
                failures.append('PROPOSAL_TIME_INVALID')
                continue
            ttl = (valid - generated).total_seconds()
            if ttl <= 0 or ttl > 20:
                failures.append('PROPOSAL_TTL_CHANGED:%s' % instance)
        if proposal_counts.get('martin-default', 0) < 1:
            failures.append('MARTIN_CANDIDATE_MISSING')
        summary_counts = report.get('proposal_counts') or {}
        for instance, count in proposal_counts.items():
            if int(summary_counts.get(instance, -1)) != count:
                failures.append('SUMMARY_PROPOSAL_COUNT_MISMATCH:%s' % instance)
    # -- checkpoint/cursor progress ------------------------------------
    checkpoints = raw.get('checkpoints') or {}
    martin = checkpoints.get('martin-default') or {}
    vwap = checkpoints.get('vwap-default') or {}
    if not (martin.get('universe_complete') is True
            and (martin.get('machines') or {}).get('DEMO', {}).get('state') == 'TRIGGERED'):
        failures.append('MARTIN_CHECKPOINT_NOT_TRIGGERED')
    seen = vwap.get('seen') if isinstance(vwap.get('seen'), dict) else {}
    quote_committed = sum(1 for task in created.get('vwap-default', [])
                          if task.get('state') == 'COMMITTED'
                          and (event_by_id.get(task.get('event_id')) or {}).get('kind') == 'quote')
    if sum(seen.values()) != quote_committed or sum(seen.values()) <= 0:
        failures.append('VWAP_CURSOR_MISMATCH')
    summary_checkpoint = report.get('checkpoint') or {}
    if summary_checkpoint.get('martin-default', {}).get('progress') is not True:
        failures.append('SUMMARY_CURSOR_MARTIN_MISSING')
    if summary_checkpoint.get('vwap-default', {}).get('progress') is not True:
        failures.append('SUMMARY_CURSOR_VWAP_MISSING')
    return sorted(set(failures))
