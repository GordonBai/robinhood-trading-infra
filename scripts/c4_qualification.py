"""Acceptance rules for C4 default dual-plugin child-fault and protection run."""
from datetime import datetime

MIN_NORMAL_ELAPSED = 120.0
MAX_CONTROL_GAP = 7.0


def _time(value):
    return datetime.fromisoformat(value)


def _fault_records(report):
    return [record for record in report.get('fault_processes') or []
            if isinstance(record, dict) and record.get('fault') is True]


def _phases(report):
    return [next(iter(row.values())).get('phase') for row in report.get('trace') or []
            if row]


def _action_states(report):
    return {(item.get('kind'), item.get('state')) for item in report.get('actions') or []
            if isinstance(item, dict)}


def evaluate_c4(report):
    failures = []
    if not isinstance(report, dict):
        return ['REPORT_NOT_OBJECT']
    if report.get('clock_mode') != 'monotonic':
        failures.append('CLOCK_NOT_MONOTONIC')
    elapsed = report.get('elapsed_seconds')
    if not isinstance(elapsed, (int, float)) or elapsed < MIN_NORMAL_ELAPSED:
        failures.append('ELAPSED_BELOW_120')
    started = report.get('run_started_monotonic')
    ended = report.get('run_ended_monotonic')
    if (not isinstance(started, (int, float)) or not isinstance(ended, (int, float))
            or ended <= started
            or abs((ended - started) - (elapsed or 0)) > 2):
        failures.append('RUN_CLOCK_INCONSISTENT')
    before = report.get('source_fingerprint_before')
    after = report.get('source_fingerprint_after')
    if not isinstance(before, str) or not isinstance(after, str) or before != after:
        failures.append('SOURCE_CHANGED')
    if report.get('source_unchanged') is not True:
        failures.append('SOURCE_FLAG_NOT_TRUE')
    if report.get('broker_calls_by_formal', 0) != 0:
        failures.append('FORMAL_BROKER_CALL')
    if report.get('formal_io_calls', 0) != 0:
        failures.append('FORMAL_IO_CALL')
    if report.get('plugins') != ['vwap-fixture', 'slow-exit-fixture']:
        failures.append('PLUGIN_COMBINATION_MISMATCH')
    records = _fault_records(report)
    starts = [record for record in records if record.get('kind') == 'start']
    exceptions = [record for record in records if record.get('kind') == 'exception']
    cleanups = [record for record in records if record.get('kind') == 'cleanup']
    if len({record.get('pid') for record in starts}) < 2:
        failures.append('FAULT_CHILDREN_MISSING')
    if not any(record.get('error') == 'STRATEGY_PROCESS_TIMEOUT' for record in exceptions):
        failures.append('CHILD_TIMEOUT_MISSING')
    if not any(record.get('error') == 'STRATEGY_PROCESS_CRASHED' for record in exceptions):
        failures.append('CHILD_CRASH_MISSING')
    for exception in exceptions:
        pid = exception.get('pid')
        related = [record for record in cleanups if record.get('pid') == pid]
        if not related or not any(record.get('reaped') is True for record in related):
            failures.append('FAULT_PID_NOT_REAPED:%s' % pid)
        if (exception.get('error') == 'STRATEGY_PROCESS_CRASHED'
                and not any(isinstance(record.get('returncode'), int)
                            and record.get('returncode') != 0
                            for record in related)):
            failures.append('CRASH_PID_RETURNCODE_MISSING:%s' % pid)
    fault_sessions = sorted(record['session_at'] for record in starts
                            if record.get('session_at'))
    if len(exceptions) < 2 or len(fault_sessions) < 2:
        failures.append('REPEATED_FAULT_WINDOW_MISSING')
    tasks = report.get('tasks') or []
    final_tail = [task for task in tasks if task.get('state') in {'QUEUED', 'RUNNING'}]
    if final_tail:
        failures.append('UNDRAINED_TAIL')
    fault_tasks = [task for task in tasks if task.get('instance_id') == 'fault-slow']
    healthy_tasks = [task for task in tasks if task.get('instance_id') == 'vwap-live']
    if not any(task.get('state') == 'REJECTED' and
               task.get('reason') == 'STRATEGY_PROCESS_TIMEOUT' for task in fault_tasks):
        failures.append('FAULT_TIMEOUT_TASK_MISSING')
    if not any(task.get('state') == 'REJECTED' and
               task.get('reason') == 'STRATEGY_PROCESS_CRASHED' for task in fault_tasks):
        failures.append('FAULT_CRASH_TASK_MISSING')
    healthy_committed = [task for task in healthy_tasks
                         if task.get('state') == 'COMMITTED' and task.get('finished_at')]
    if len(healthy_committed) < 2:
        failures.append('HEALTHY_PROGRESS_MISSING')
    if not fault_sessions:
        failures.append('FAULT_START_TIME_MISSING')
    else:
        first, last = fault_sessions[0], fault_sessions[-1]
        before_tasks = [task for task in healthy_committed
                        if task['finished_at'] <= last]
        during_tasks = [task for task in healthy_committed
                        if first <= task['finished_at'] <= last]
        after_tasks = [task for task in healthy_committed
                       if task['finished_at'] > last]
        if len(before_tasks) < 2 or not during_tasks or len(after_tasks) < 2:
            failures.append('HEALTHY_PROGRESS_NOT_CONTINUING')
    feedback = report.get('feedback') or []
    feedback_kinds = {(item.get('instance_id'), item.get('kind')): item.get('count')
                      for item in feedback if isinstance(item, dict)}
    if not feedback_kinds.get(('vwap-live', 'protection_covered'), 0):
        failures.append('PROTECTION_FEEDBACK_MISSING')
    trace = report.get('trace') or []
    phases = _phases(report)
    if 'PROTECTED' not in phases:
        failures.append('PROTECTED_PHASE_MISSING')
    if 'FLAT_VERIFIED' not in phases:
        failures.append('FLAT_PHASE_MISSING')
    protected_rows = [next(iter(row.values())) for row in trace
                      if row and next(iter(row.values())).get('phase') == 'PROTECTED']
    covered = [item for item in protected_rows if item.get('first_protection_observed_at')
               and item.get('last_full_coverage_observed_at')]
    if not covered:
        failures.append('COVERAGE_TIMESTAMP_MISSING')
    if 'PROTECTED' in phases and 'FLAT_VERIFIED' in phases:
        if phases.index('PROTECTED') >= phases.index('FLAT_VERIFIED'):
            failures.append('PROTECTION_ORDER_INVALID')
    if covered and fault_sessions:
        first_coverage = min(item['first_protection_observed_at'] for item in covered)
        if not any(session > first_coverage for session in fault_sessions):
            failures.append('FAULT_NOT_OVERLAPPING_PROTECTION')
    states = _action_states(report)
    for kind in ('entry', 'protect', 'cancel', 'exit'):
        if (kind, 'OBSERVED') not in states:
            failures.append('ACTION_NOT_BROKER_OBSERVED:%s' % kind)
    executions = report.get('executions_detail') or []
    sides = {((row.get('payload') or {}).get('side')) for row in executions
             if isinstance(row, dict)}
    if len(executions) < 2 or not {'buy', 'sell'}.issubset(sides):
        failures.append('EXECUTION_FACTS_MISSING')
    book_state = report.get('book_state') or {}
    trades = book_state.get('trades') or {}
    if not trades or not any(trade.get('phase') == 'FLAT_VERIFIED'
                             and str(trade.get('quantity')) == '0'
                             for trade in trades.values()):
        failures.append('FINAL_FLAT_FIELDS_MISSING')
    samples = report.get('broker_samples') or []
    if len(samples) < 20:
        failures.append('BROKER_OBSERVATIONS_MISSING')
    if samples:
        times = [_time(sample['at']) for sample in samples]
        gaps = [(b - a).total_seconds() for a, b in zip(times, times[1:])]
        if not gaps or any(gap <= 0 for gap in gaps) or max(gaps) > MAX_CONTROL_GAP:
            failures.append('BROKER_SAMPLE_TIME_INVALID')
        if (times[-1] - times[0]).total_seconds() < MIN_NORMAL_ELAPSED:
            failures.append('BROKER_OBSERVATION_WINDOW_TOO_SHORT')
    if report.get('execution_intents', 0) < 1:
        failures.append('ENTRY_INTENT_MISSING')
    return sorted(set(failures))


def evaluate_timeout_control(report):
    failures = []
    if report.get('scenario') != 'protection_timeout':
        failures.append('SCENARIO_MISMATCH')
    if report.get('elapsed_seconds', 0) < 30:
        failures.append('DEADLINE_ELAPSED_TOO_SHORT')
    feedback = report.get('feedback') or []
    if any(item.get('kind') == 'protection_covered' for item in feedback):
        failures.append('UNCONFIRMED_PROTECTION_REPORTED_COVERED')
    state = report.get('book_state') or {}
    issues = state.get('issues') or []
    if not any(str(issue).startswith('PROTECTION_WAIT_EXCEEDED:') for issue in issues):
        failures.append('PROTECTION_DEADLINE_ISSUE_MISSING')
    phases = _phases(report)
    if 'PROTECTED' in phases:
        failures.append('UNCONFIRMED_PROTECTION_MARKED_PROTECTED')
    if 'EXIT_REQUESTED' not in phases:
        failures.append('FORCED_EXIT_MISSING')
    return sorted(set(failures))


def evaluate_noop_control(report):
    failures = []
    if report.get('noop_reconcile') is not True:
        failures.append('NOOP_FLAG_MISSING')
    if 'PROTECTED' in _phases(report):
        failures.append('NOOP_RECONCILE_WAS_ACCEPTED')
    return sorted(set(failures))
