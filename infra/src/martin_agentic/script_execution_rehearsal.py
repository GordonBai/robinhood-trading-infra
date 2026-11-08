"""Offline deterministic-driver rehearsal; no live transport or credentials."""
from datetime import timedelta
import json
from pathlib import Path
import uuid
import time

from .audit import digest
from .broker import normalize_order
from .qualification import source_fingerprint
from .rehearsal import _Fixture
from .script_execution import ScriptExecutionDriver


class _SyntheticChannel:
    simulation_only = True

    def __init__(self, fixture):
        self.fixture = fixture
        self.calls = []

    def execute_simulated(self, packet):
        self.calls.append(packet)
        if packet['operation'] == 'review':
            return {'data': {**packet['parameters'], 'order_checks': {},
                'customer_approval_required': False,
                'market_data_disclosure': 'SYNTHETIC offline fixture; not a market quote'}}
        if packet['operation'] == 'cancel':
            return {'data': {'accepted': True}}
        return {'data': {'order': self.raw_order(packet), 'approval': None}}

    def raw_order(self, packet, *, filled=0, state='confirmed'):
        action = json.loads(next(a['payload'] for a in self.fixture.book.actions()
                                 if a['id'] == packet['action_id']))
        raw = self.fixture.raw_order(action, filled, state)
        raw['created_at'] = self.fixture.at.isoformat()
        for fill in raw['executions']:
            fill['timestamp'] = self.fixture.at.isoformat()
        return raw

    def observed(self, packet, *, filled=0, state='confirmed'):
        return normalize_order(self.raw_order(packet, filled=filled, state=state),
                               self.fixture.at.isoformat())


def rehearse_script_execution(output_dir, policy):
    """Run the real action/receipt/reconcile core with explicit synthetic facts."""
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    directory = root / ('script-rehearsal-' + uuid.uuid4().hex)
    directory.mkdir(mode=0o700)
    f = _Fixture(directory / 'synthetic.sqlite', policy)
    channel = _SyntheticChannel(f)
    driver = ScriptExecutionDriver(f.book, channel)
    steps = []

    def step(snapshot, *, entries=False):
        result = driver.step(snapshot, {'DEMO': f.quote()}, allow_entries=entries)
        steps.append(result)
        f.at += timedelta(seconds=1)
        return result

    def place(snapshot_fn, *, entries=False):
        if step(snapshot_fn(), entries=entries)['status'] != 'REVIEWED_SYNTHETIC':
            raise ValueError('SYNTHETIC_DRIVER_REVIEW_FAILED')
        if step(snapshot_fn(), entries=entries)['status'] != 'submitted':
            raise ValueError('SYNTHETIC_DRIVER_SUBMIT_FAILED')
        return channel.calls[-1]

    try:
        trade_id = 'engineering-script-rehearsal'
        f.book.propose(trade_id, f.plan, f.quote(), f.snap(), purpose='engineering',
            evidence={'synthetic': True, 'source_sha256': digest(f.plan),
                'engineering_scope_authorized': True, 'parameters_user_specified': True})
        entry = place(f.snap, entries=True)
        buy = channel.observed(entry, filled=2, state='filled')
        protection = place(lambda: f.snap([buy], 2))
        stop = channel.observed(protection)
        protected = lambda: f.snap([buy, stop], 2, 2)
        step(protected())
        protected_trade = f.book.state()['trades'][trade_id]
        coverage_observed = protected_trade['phase'] == 'PROTECTED'
        delay = protected_trade.get('protection_observation_delay_seconds')
        f.book.request_engineering_exit(trade_id, protected())
        cancel = step(protected())
        count_after_cancel = len(channel.calls)
        step(protected())
        waits_for_terminal = (len(channel.calls) == count_after_cancel
            and not any(a['kind'] == 'exit' for a in f.book.actions()))
        stop['state'] = 'cancelled'
        exit_packet = place(lambda: f.snap([buy, stop], 2))
        sell = channel.observed(exit_packet, filled=2, state='filled')
        step(f.snap([buy, stop, sell]))
        final = f.book.state()['trades'][trade_id]
        operations = f.book.db.execute('SELECT count(*) FROM external_operations').fetchone()[0]
        receipts = f.book.db.execute('SELECT count(*) FROM script_execution_receipts').fetchone()[0]
        audit = f.book.audit.verify()
        checks = {'protection_requires_observed_coverage': coverage_observed,
            'cancel_acceptance_waits_for_terminal': waits_for_terminal,
            'cancel_submitted': cancel['status'] == 'cancel_requested',
            'flat_after_observed_exit': final['phase'] == 'FLAT_VERIFIED',
            'each_operation_has_durable_receipt': operations == receipts == len(channel.calls),
            'audit_integrity': audit['integrity_valid']}
        result = {'schema_version': 1, 'passed': all(checks.values()),
            'synthetic': True, 'channel': 'in_process_synthetic_fixture',
            'source_fingerprint': source_fingerprint(), 'checks': checks,
            'model_calls': 0, 'broker_calls': 0, 'real_broker_writes': 0,
            'synthetic_operations': operations,
            'synthetic_protection_observation_delay_seconds': delay,
            'latency_is_simulated_not_live_measurement': True,
            'final_phase': final['phase'], 'audit': audit,
            'live_execution_enabled': False, 'unattended_ready': False,
            'production_ready': False, 'db': str(directory / 'synthetic.sqlite'),
            'steps': steps}
        report = directory / 'result.json'
        result['report_path'] = str(report)
        report.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
        return result
    finally:
        f.book.close()


def rehearse_bounded_entry_protection(output_dir, policy, *, io_timeout_seconds=5):
    """Kill a genuinely sleeping fixture child after an exact synthetic fill.

    Wall time is real; all orders, fills and protection observations are synthetic.
    The fixed child has no broker transport, credentials or arbitrary command.
    """
    from .subprocess_script_execution import SubprocessScriptExecutionDriver
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    directory = root / ('bounded-script-' + uuid.uuid4().hex)
    directory.mkdir(mode=0o700)
    f = _Fixture(directory / 'synthetic.sqlite', policy)
    driver = SubprocessScriptExecutionDriver(f.book,
        {'by_kind': {'entry': {'place': {'delay_seconds': 120}}}},
        directory / 'receipts', io_timeout_seconds=io_timeout_seconds)
    initial = f.at
    started = time.monotonic()
    buy = stop = marker = timeout_terminal = None
    first_fill_elapsed = protection_dispatch_elapsed = coverage_elapsed = None
    events = []
    try:
        trade_id = 'engineering-bounded-script'
        f.book.propose(trade_id, f.plan, f.quote(), f.snap(), purpose='engineering',
            evidence={'synthetic': True, 'source_sha256': digest(f.plan),
                'engineering_scope_authorized': True, 'parameters_user_specified': True})
        entry_action = f.book.actions()[0]
        entry_payload = json.loads(entry_action['payload'])
        deadline = started + io_timeout_seconds + 12
        while time.monotonic() < deadline:
            elapsed = time.monotonic() - started
            f.at = initial + timedelta(seconds=elapsed)
            orders = [o for o in (buy, stop) if o is not None]
            snapshot = f.snap(orders, 2 if buy else 0, 2 if stop else 0)
            result = driver.step(snapshot, {'DEMO': f.quote()}, allow_entries=True)
            if result.get('status') not in {'IN_FLIGHT', 'IDLE'}:
                events.append({k: result[k] for k in ('status', 'action_id', 'operation',
                    'operation_id', 'child_pid', 'child_reaped', 'termination_reason',
                    'lease_released', 'elapsed_seconds') if k in result})
            if (result.get('action_id') == entry_action['id']
                    and result.get('operation') == 'place' and result.get('child_reaped')):
                timeout_terminal = {**driver.status()['last_terminal'],
                                    'action_report_status': result['status']}
            pending = driver.pending
            if (buy is None and pending and pending['packet']['action_id'] == entry_action['id']
                    and pending['packet']['operation'] == 'place'
                    and pending['started_path'].is_file()):
                marker = json.loads(pending['started_path'].read_text())
                raw = f.raw_order(entry_payload, 2, 'filled')
                raw['executions'][0]['timestamp'] = f.at.isoformat()
                buy = normalize_order(raw, f.at.isoformat())
                first_fill_elapsed = elapsed
            for action in f.book.actions():
                if action['kind'] != 'protect':
                    continue
                operation = f.book.db.execute("SELECT 1 FROM external_operations WHERE action_id=? AND operation='place'",
                                              (action['id'],)).fetchone()
                if operation and protection_dispatch_elapsed is None:
                    protection_dispatch_elapsed = elapsed
                if action['state'] == 'REPORTED' and action['report']:
                    report = json.loads(action['report'])
                    if report['status'] == 'submitted' and stop is None:
                        raw = f.raw_order(json.loads(action['payload']))
                        raw['id'] = report['order_id']
                        stop = normalize_order(raw, f.at.isoformat())
            trade = f.book.state()['trades'][trade_id]
            if trade['phase'] == 'PROTECTED':
                coverage_elapsed = elapsed
                break
            time.sleep(.02)
        trade = f.book.state()['trades'][trade_id]
        observed_delay = trade.get('protection_observation_delay_seconds')
        checks = {
            'child_really_entered_120_second_delay': bool(marker and marker.get('delay_seconds') == 120),
            'entry_child_reaped_before_followup': bool(timeout_terminal and timeout_terminal['reaped']),
            'timeout_recorded_unknown_not_retried': bool(timeout_terminal and timeout_terminal['action_report_status'] == 'unknown'),
            'entry_remains_observed_after_unknown_receipt': next(a['state'] for a in f.book.actions() if a['id'] == entry_action['id']) == 'OBSERVED',
            'protection_dispatched_within_30_seconds': protection_dispatch_elapsed is not None
                and first_fill_elapsed is not None and 0 <= protection_dispatch_elapsed - first_fill_elapsed < 30,
            'coverage_observed_within_30_seconds': coverage_elapsed is not None
                and first_fill_elapsed is not None and 0 <= coverage_elapsed - first_fill_elapsed < 30
                and observed_delay is not None and 0 <= float(observed_delay) < 30,
            'final_synthetic_phase_protected': trade['phase'] == 'PROTECTED',
            'source_audit_valid': f.book.audit.verify()['integrity_valid']}
        result = {'passed': all(checks.values()), 'synthetic': True, 'checks': checks,
            'source_fingerprint': source_fingerprint(), 'scope': 'Real child timeout and reap; synthetic exact entry fill and confirmed protection only',
            'io_timeout_seconds': io_timeout_seconds, 'actual_elapsed_seconds': time.monotonic() - started,
            'first_fill_real_elapsed': first_fill_elapsed, 'protection_dispatch_real_elapsed': protection_dispatch_elapsed,
            'protection_coverage_real_elapsed': coverage_elapsed,
            'synthetic_observed_protection_delay_seconds': observed_delay,
            'entry_child_started_evidence': marker, 'entry_child_terminal_evidence': timeout_terminal,
            'events': events, 'broker_calls': 0, 'real_broker_writes': 0, 'model_calls': 0,
            'production_ready': False, 'unattended_ready': False,
            'report_path': str(directory / 'result.json')}
        Path(result['report_path']).write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
        return result
    finally:
        driver.stop()
        # Poll/reap only our fixed child; stop is never treated as cancellation.
        cleanup_deadline = time.monotonic() + 2
        while driver.pending and time.monotonic() < cleanup_deadline:
            f.at = initial + timedelta(seconds=time.monotonic() - started)
            driver.step(f.snap([o for o in (buy, stop) if o is not None], 2 if buy else 0, 2 if stop else 0),
                        {'DEMO': f.quote()})
            time.sleep(.01)
        if driver.pending:
            raise RuntimeError('SYNTHETIC_CHILD_NOT_REAPED_MANUAL_INSPECTION_REQUIRED')
        f.book.close()
