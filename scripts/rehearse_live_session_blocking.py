"""Full controller blocking rehearsal, using only temporary, synthetic fixtures.

The real controller, supervisor, execution book, broker journal, dispatcher,
budgets and SQLite audit run. Only transport/data and the model callable are
replaced. Fixture snapshots use the production wire shape (synthetic=False),
because BrokerJournal deliberately rejects synthetic snapshots; the temporary
database MUST NOT be used as broker/production acceptance evidence.
"""
import argparse
from contextlib import ExitStack
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
from pathlib import Path
import sys
import tempfile
import threading
import time
from unittest.mock import patch
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tests'))
from test_broker import FakeBroker, order, position, snapshot
from martin_agentic.entry_authority import DurableEntryAuthority
from entry_authority_fixture import SyntheticTestPreflight, TestExecutionBook
from martin_agentic.execution import ExecutionBook
from martin_luk.live_session import run_live_session
from martin_agentic.qualification import source_fingerprint
from martin_agentic.audit import digest


class _SyntheticHarnessAuthority:
    """Explicit synthetic harness authority; never a production admission path."""

    def __init__(self, db):
        self.durable = DurableEntryAuthority(db)

    def check(self, *, source, snapshot=None, action=None, purpose=None):
        decision = self.durable.check(
            source=source, snapshot=snapshot, action=action, purpose=purpose)
        if decision.get('reason') == 'ENTRY_AUTHORITY_NAMESPACE_ABSENT':
            return {'allowed': True,
                    'reason': 'SYNTHETIC_HARNESS_ENTRY_AUTHORITY',
                    'source': source}
        return decision

    def require_entry(self, *, source, snapshot=None, action=None, purpose=None):
        decision = self.check(source=source, snapshot=snapshot,
                              action=action, purpose=purpose)
        if decision.get('allowed') is not True:
            raise ValueError('ENTRY_AUTHORITY_BLOCKED:' +
                             (decision.get('reason') or 'BLOCKED'))
        return decision


def run_rehearsal(*, duration=120.0, poll_seconds=5.0, agent_mode='review'):
    if agent_mode not in {'review','script-review'} or not 0 < duration <= 180 or not 0 < poll_seconds <= 10:
        raise ValueError('INVALID_SYNTHETIC_REHEARSAL_DURATION')
    wall_started = datetime.now(timezone.utc)
    started = time.monotonic()
    # Pin to an authorized synthetic session, but advance with actual elapsed
    # monotonic time. No timer jumps or fake elapsed-time evidence.
    baseline = datetime(2026, 10, 19, 14, tzinfo=timezone.utc)
    clock = lambda: baseline + timedelta(seconds=time.monotonic() - started)
    owner = threading.get_ident()
    write_threads = []
    control_samples = []
    worker_calls = []
    callbacks = {}
    threads = []
    release = threading.Event()
    worker_started = threading.Event()
    last_state = {}
    active_book = []
    unknown_identity = []
    real_sleep = time.sleep
    build_before = source_fingerprint()

    class TrackedBook(ExecutionBook):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._entry_authority = _SyntheticHarnessAuthority(self.db)
            self._current_preflight = SyntheticTestPreflight()
            active_book.append(self)

        def _current_preflight_decision(self, **kwargs):
            if kwargs.get('proposal'):
                return self._current_preflight.check_proposal(
                    self, kwargs.get('snapshot'), kwargs.get('quote'),
                    kwargs.get('plan'), purpose=kwargs.get('purpose'),
                    evidence=kwargs.get('evidence'))
            return self._current_preflight.check_claim(self, kwargs.get('action'))

        def _preflight_binding_ok(self, decision, **kwargs):
            return True

        def _begin(self):
            write_threads.append(threading.get_ident())
            return super()._begin()

        def reconcile(self, broker, **kwargs):
            if worker_started.is_set() and not unknown_identity:
                unknown_id = self._queue({'id': 'independent-unknown', 'plan': {'symbol': 'OTHER'}},
                    'entry', {'symbol': 'OTHER', 'side': 'buy', 'type': 'limit', 'quantity': '1',
                              'limit_price': '1', 'market_hours': 'regular_hours', 'time_in_force': 'gfd'},
                    clock(), 'SYNTHETIC_UNKNOWN_NONRETRY_FIXTURE')
                self.db.execute("UPDATE live_actions SET state='UNKNOWN' WHERE id=?", (unknown_id,))
                self.db.commit()
                unknown_identity.append(unknown_id)
            result = super().reconcile(broker, **kwargs)
            state = self.state()
            trade = state['trades']['blocking-fixture']
            control_samples.append({
                'elapsed_seconds': time.monotonic() - started,
                'broker_observed_at': broker['observed_at'],
                'phase': trade['phase'], 'quantity': trade['quantity'],
                'market_blocked': 'market' in callbacks and 'ended' not in callbacks['market'],
                'worker_blocked': worker_started.is_set() and 'ended' not in callbacks.get('worker', {}),
            })
            return result

        def close(self):
            last_state.update(state=self.state(), actions=self.actions(), audit=self.audit.verify(),
                model_reservations=self.db.execute('SELECT count(*) FROM worker_call_reservations').fetchone()[0])
            return super().close()

    def blocked(name):
        threads.append(threading.current_thread())
        callbacks[name] = {'started': time.monotonic() - started,
                           'thread_id': threading.get_ident()}
        if name == 'worker':
            worker_started.set()
        # Actual elapsed blocking per callback. release is only an emergency
        # cleanup path, never used to shorten successful runs.
        callbacks[name]['interrupted'] = release.wait(duration)
        callbacks[name]['ended'] = time.monotonic() - started

    first_quote_sent=[]
    def market(*args, **kwargs):
        if agent_mode=='script-review' and not first_quote_sent:
            first_quote_sent.append(True)
            at=clock().isoformat()
            return [{'id':'synthetic-first-quote','kind':'quote','at':at,'synthetic_input':False,
                'source_sha256':'SYNTHETIC_REVIEW_FIXTURE','data':{'symbol':'DEMO','bid':'39.99','ask':'40',
                    'bid_at':at,'ask_at':at,'observed_at':at}}],{'synthetic_rehearsal':True}
        if 'market' not in callbacks:
            blocked('market')
        return [], {'synthetic_rehearsal': True}

    def worker(action):
        worker_calls.append(deepcopy(action))
        blocked('worker')
        return {'status': 'unknown', 'message': 'SYNTHETIC_AMBIGUOUS_WORKER_RESPONSE'}

    def model_factory(*args,**kwargs):
        if agent_mode=='script-review':raise AssertionError('SCRIPT_REVIEW_MUST_NOT_CREATE_MODEL_WORKER')
        return worker

    class ReviewFixture:
        # Same production wire shape as the temporary BrokerJournal fixture.
        # The entire run is explicitly synthetic and cannot authorize a trade.
        simulation_only=False
        def __init__(self,**kwargs):self.rpc=SimpleNamespace(close=lambda:None)
        def __enter__(self):return self
        def __exit__(self,*args):self.rpc.close()
        def execute_review(self,packet):
            worker_calls.append(deepcopy(packet));blocked('worker')
            return {'data':{**packet['parameters'],'order_checks':{},
                'customer_approval_required':False,'market_data_disclosure':'SYNTHETIC_ONLY_DISCLOSURE'}}

    with tempfile.TemporaryDirectory(prefix='martin-full-controller-synthetic-') as directory, patch(
            'martin_agentic.session_lineage.CANONICAL_RUNTIME',Path(directory)):
        db = Path(directory) / 'synthetic-only.sqlite'
        policy = json.loads((ROOT / 'tests/fixtures/policies/live_trial_2026-10-19.json').read_text())
        policy['live_enabled'] = False
        fixture = FakeBroker()
        seed = TestExecutionBook(db, policy, clock=clock)
        seed._entry_authority = _SyntheticHarnessAuthority(seed.db)
        seed._current_preflight = SyntheticTestPreflight()
        from martin_agentic.broker import BrokerJournal
        from martin_agentic.session_lineage import bootstrap_session_lineage
        initial=snapshot(fixture,clock().isoformat())
        source=BrokerJournal(db)
        try:source.ingest(initial)
        finally:source.close()
        seed.reconcile(initial)
        bootstrap_session_lineage(db,policy,current_db=db,current_policy=policy,clock=clock)
        try:
            at = clock().isoformat()
            quote = {'symbol': 'DEMO', 'bid': '39.99', 'ask': '40',
                     'bid_at': at, 'ask_at': at, 'observed_at': at}
            seed.propose('blocking-fixture', {'symbol': 'DEMO', 'quantity': 2,
                'entry_ceiling': '40', 'stop': '39', 'cost_buffer_per_share': '0.05'},
                quote, snapshot(fixture, clock().isoformat()),
                evidence={'source_sha256': 'SYNTHETIC_FULL_CONTROLLER_FIXTURE', 'synthetic': False})
            entry = seed.claim()
            seed.report(entry['action_id'], {'status': 'submitted', 'message': 'synthetic fixture'})

            def bound(action, *, filled='0', state='confirmed'):
                p = action['parameters']
                raw = order(action['action_id'], p['side'], p['quantity'], filled, state, p.get('stop_price'))
                raw.update(ref_id=action['ref_id'], price=p.get('limit_price'),
                    type='limit' if p['type'] == 'limit' else 'market',
                    market_hours=p['market_hours'], time_in_force=p['time_in_force'],
                    created_at=clock().isoformat())
                for execution in raw['executions']:
                    execution['timestamp'] = clock().isoformat()
                return raw

            buy = bound(entry, filled='2', state='filled')
            fixture.orders = [buy]
            fixture.positions = [position('2')]
            fixture.cash = '420'
            seed.reconcile(snapshot(fixture, clock().isoformat()))
            protection = json.loads(next(a['payload'] for a in seed.actions() if a['kind'] == 'protect'))
            stop = bound(protection)
        finally:
            seed.close()

        def broker_read(*args, **kwargs):
            # Confirmed stop arrives independently while its model callback is
            # still blocked. No model output is treated as fill/protection proof.
            fixture.orders = [buy, stop] if worker_started.is_set() else [buy]
            fixture.positions = [position('2', '2' if worker_started.is_set() else '0')]
            return snapshot(fixture, clock().isoformat())

        # Cycles are real controller ticks. One tick after callback completion
        # consumes the durable receipt; another verifies no retry/regression.
        cycles = math.ceil(duration / poll_seconds) + 4
        def fixed_pace_wait(market_task, dispatcher, requested, *, strategy_supervisor=None):
            # Same wake condition as production: return as soon as either
            # background task posts a result. Only the maximum wait is fixed.
            enabled = getattr(strategy_supervisor, 'strategy_process_enabled', False) is True
            end = time.monotonic() + poll_seconds
            tasks = [market_task] + ([dispatcher.task] if dispatcher is not None else [])
            while True:
                if enabled:
                    strategy_supervisor.advance_strategy()
                if any(task.thread is not None and not task.mailbox.empty() for task in tasks):
                    return
                if time.monotonic() >= end:
                    return
                real_sleep(min(.01 if enabled else .05, max(0.0, end - time.monotonic())))
        try:
            with ExitStack() as stack:
                if agent_mode=='script-review':
                    stack.enter_context(patch('martin_agentic.async_native_review.EquityReviewTransport',ReviewFixture))
                replacements = {
                    'ExecutionBook': TrackedBook,
                    'CodexOrderWorker': model_factory,
                    'collect_live_events': market,
                    '_broker_read': broker_read,
                    'notify_local': lambda *args: None,
                    'print': lambda *args, **kwargs: None,
                }
                for name, replacement in replacements.items():
                    stack.enter_context(patch('martin_agentic.live_session.' + name, replacement))
                # Production public API keeps its >=5 s poll validation. The
                # Harness fixes control tick pacing; callback duration and clock remain real.
                # Prompt mailbox wake scheduling has separate direct integration tests.
                # Advisory Owner progress still runs inside the wait, exactly as the
                # production wait does, so blocking callbacks cannot silently stop
                # the default strategy-process path from being exercised here.
                stack.enter_context(patch('martin_agentic.live_session.wait_for_result_or_tick',
                    fixed_pace_wait))
                result = run_live_session(db, policy, symbols=[f'FIXTURE{i}' for i in range(10)],
                    cycles=cycles, poll_seconds=5, agent_mode=agent_mode,
                    budget_entry_limit=0 if agent_mode=='script-review' else 20,
                    budget_total_limit=0 if agent_mode=='script-review' else 40,
                    output_dir=Path(directory) / 'worker', status_file=Path(directory) / 'status.json',
                    clock=clock,require_watchdog=True,
                    watchdog_policy_path=ROOT/'tests/fixtures/policies/live_trial_2026-10-19.json')
                guardian=result.get('watchdog_guard',{})
                inhibit_path=Path(str(db)+'.entry-inhibit.json')
                guardian_inhibit=json.loads(inhibit_path.read_text()) if inhibit_path.exists() else {}
                receipts = [json.loads(p.read_text()) for p in (Path(directory) / ('worker/native-reviews' if agent_mode=='script-review' else 'worker/mailbox')).glob('*.json')]
        finally:
            release.set()
            for thread in threads:
                thread.join(timeout=2)
        actions = last_state['actions']
        unknown = next(a for a in actions if a['id'] == unknown_identity[0])
        protected = next(a for a in actions if a['id'] == protection['action_id'])

    overlapping = [s for s in control_samples if s['market_blocked'] and s['worker_blocked']]
    protected_overlapping = [s for s in overlapping if s['phase'] == 'PROTECTED' and s['quantity'] == '2']
    gaps = [b['elapsed_seconds'] - a['elapsed_seconds'] for a, b in zip(overlapping, overlapping[1:])]
    max_gap = max(gaps, default=0)
    build_after = source_fingerprint()
    checks = {
        'missing_guardian_blocks_new_entries': guardian.get('required') is True
            and guardian.get('ready') is False and guardian_inhibit.get('blocked') is True,
        'both_callbacks_blocked_for_requested_real_elapsed': all(
            name in callbacks and callbacks[name].get('ended', 0) - callbacks[name]['started'] >= duration
            and not callbacks[name]['interrupted'] for name in ('market', 'worker')),
        'control_reconciled_protected_position_while_both_blocked': len(protected_overlapping) >= 2,
        'control_tick_gap_within_bound': max_gap <= max(1.0, poll_seconds * 2 + .5),
        'one_worker_call_only': len(worker_calls) == 1 and (worker_calls[0].get('operation')=='review'
            and worker_calls[0].get('action_id')==protection['action_id'] if agent_mode=='script-review' else worker_calls[0]['kind']=='protect'),
        'no_model_reservation_in_script_review': agent_mode!='script-review' or last_state['model_reservations']==0,
        'unknown_action_not_retried_or_cleared': unknown['state'] == 'UNKNOWN',
        'late_receipt_does_not_regress_observed_protection': protected['state'] == 'OBSERVED',
        'durable_exact_bound_worker_receipt': len(receipts)==1 and (receipts[0]['packet']['action_id']==protection['action_id']
            and receipts[0]['packet_sha256']==digest(receipts[0]['packet'])
            and receipts[0]['response_sha256']==digest(receipts[0]['response'])
            and receipts[0]['binding']['account_key']==last_state['state']['account_key']
            and receipts[0]['binding']['db']==str(db.resolve())
            and receipts[0]['binding']['policy_sha256']==digest(policy)
            if agent_mode=='script-review' else receipts[0]['action_id']==protection['action_id'] and receipts[0]['report']['status']=='unknown'),
        'all_execution_transactions_on_owner_thread': bool(write_threads) and set(write_threads) == {owner},
        'all_owned_callback_threads_joined': all(not thread.is_alive() for thread in threads),
        'source_unchanged_during_run': build_before == build_after,
        'bounded_session_stopped_with_explicit_handoff': result['running'] is False
            and result['stop_disposition'] == 'MANAGEMENT_STOPPED_MANUAL_HANDOFF_REQUIRED',
    }
    return {'schema_version': 1, 'synthetic': True, 'agent_mode':agent_mode,'passed': all(checks.values()),
        'model_reservations':last_state['model_reservations'],'broker_writes':0,'production_ready':False,
        'started_at_utc': wall_started.isoformat(), 'ended_at_utc': datetime.now(timezone.utc).isoformat(),
        'elapsed_seconds': time.monotonic() - started, 'requested_callback_block_seconds': duration,
        'poll_seconds': poll_seconds, 'production_default_poll_seconds': 15,
        'full_requested_120_second_scope': duration >= 120 and poll_seconds >= 5,
        'checks': checks, 'callbacks': callbacks, 'owner_thread_id': owner,
        'transaction_thread_ids': sorted(set(write_threads)), 'control_tick_count': len(control_samples),
        'simultaneously_blocked_reconciliation_count': len(overlapping),
        'protected_position_observations_during_block': len(protected_overlapping),
        'max_control_tick_gap_during_block_seconds': max_gap,
        'control_samples': control_samples, 'worker_calls': len(worker_calls),
        'guardian_required':True,'guardian_intentionally_absent':True,
        'unknown_final_state': unknown['state'], 'protection_action_final_state': protected['state'],
        'source_fingerprint_before': build_before, 'source_fingerprint_after': build_after,
        'harness_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'fixture_sha256': hashlib.sha256((ROOT / 'tests/test_broker.py').read_bytes()).hexdigest(),
        'scope': 'actual controller, supervisor, execution book, broker journal, chosen dispatcher, budgets, audit and durable receipts; isolated in-process synthetic I/O fixtures',
        'limits': ['No real broker/network/model calls, credentials or live database touched.',
            'Fixture snapshots use the production API shape including synthetic=False solely in a discarded temporary database; this report is explicitly synthetic.',
            'Clock is pinned to a historical authorized fixture session and advances with actual monotonic elapsed time.',
            'Exercises missing-guardian new-risk inhibition while existing-position management continues.',
            'Confirms local orchestration/reconciliation under blocked callbacks, not actual placement, 30-second broker protection, Mac sleep, authentication recovery or production readiness.',
            'Controller broker read is intentionally fast; blocking broker transport is a separate transport/watchdog requirement.',
            'script-review fixture deliberately ignores the RPC deadline to exercise a hung dependency; this is not an actual transport deadline test.']}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--duration', type=float, default=120)
    parser.add_argument('--poll-seconds', type=float, default=5)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--agent-mode',choices=['review','script-review'],default='review')
    args = parser.parse_args()
    report = run_rehearsal(duration=args.duration, poll_seconds=args.poll_seconds,agent_mode=args.agent_mode)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'passed': report['passed'], 'checks': report['checks'], 'output': str(args.output),
                      'elapsed_seconds': report['elapsed_seconds']}))
    raise SystemExit(0 if report['passed'] else 1)
