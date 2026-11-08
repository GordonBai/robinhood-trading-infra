"""Owned-child fault rehearsal; no broker, model, host sleep or authentication calls.

This supplies local subprocess evidence, not Mac sleep or real transport recovery
acceptance. All databases and process signals are confined to a temporary folder
and child PIDs created by this invocation.
"""
import argparse
from contextlib import closing
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time

from martin_agentic.control_tasks import BackgroundTask
from martin_agentic.execution import ExecutionBook
from martin_luk.live_session import AsyncDispatcher
from martin_agentic.qualification import source_fingerprint
from martin_agentic.watchdog import entry_inhibit, watchdog_check
from martin_agentic.worker_budget import WorkerBudget


def write(path, value):
    path = Path(path)
    temp = path.with_suffix('.tmp')
    temp.write_text(json.dumps(value) + '\n')
    temp.replace(path)


def initialize_book(path):
    policy = json.loads((Path(__file__).resolve().parents[1] / 'tests/fixtures/policies/live_trial_2026-10-19.json').read_text())
    base = datetime(2026, 10, 19, 14, tzinfo=timezone.utc)
    started = time.monotonic()
    book = ExecutionBook(path, policy, simulation=True,
                         clock=lambda: base + timedelta(seconds=time.monotonic() - started))
    trade = {'id': 'synthetic-local-process', 'plan': {'symbol': 'DEMO'}}
    action_id = book._queue(trade, 'protect', {'side': 'sell', 'quantity': '1'}, book.clock(), 'synthetic fault fixture')
    book.db.commit()
    return book, policy, action_id


def child(mode, directory, duration, poll):
    root = Path(directory).resolve()
    db = root / 'fixture.sqlite'
    if mode == 'control':
        book, _, action_id = initialize_book(db)
        book.db.execute("UPDATE live_actions SET state='WAITING_USER' WHERE id=?", (action_id,))
        book._queue({'id': 'synthetic-unknown', 'plan': {'symbol': 'DEMO'}}, 'exit',
                    {'side': 'sell', 'quantity': '1'}, book.clock(), 'unknown preserved fixture')
        book.db.execute("UPDATE live_actions SET state='UNKNOWN' WHERE kind='exit'")
        book.db.commit()
        write(root / 'initial-actions.json', book.actions())
        while True:
            now = datetime.now(timezone.utc).isoformat()
            write(root / 'status.json', {'db': str(db), 'pid': os.getpid(),
                  'control_heartbeat_at': now, 'observed_at': now})
            time.sleep(poll)
    if mode == 'watchdog':
        pid = int((root / 'controller.pid').read_text())
        def owned_process(target, target_db):
            if target != pid or Path(target_db) != db:
                return False
            try:
                os.kill(pid, 0)
                return True
            except ProcessLookupError:
                return False
        def notify(message):
            with (root / 'notifications.ndjson').open('a') as handle:
                handle.write(json.dumps({'at': datetime.now(timezone.utc).isoformat(), 'message': message}) + '\n')
        while True:
            # Thresholds are deliberately scaled for the process fault test.
            value = watchdog_check(db, root / 'status.json', process_checker=owned_process,
                notifier=notify, heartbeat_limit=.5, broker_limit=.5,
                worker_limit=135, protection_limit=1e12)
            write(root / 'watchdog-result.json', value)
            time.sleep(poll)
    if mode == 'slow':
        book, policy, _ = initialize_book(db)
        budget = WorkerBudget(db, policy['session_date'])
        task = BackgroundTask('synthetic-blocked-market')
        market_started = threading.Event()
        worker_started = threading.Event()
        release_callbacks = threading.Event()
        callback_durations = {}
        def slow_market():
            market_started.set()
            release_callbacks.wait(5)
            start = time.monotonic()
            time.sleep(duration)
            callback_durations['market_seconds'] = time.monotonic() - start
            return {'synthetic': True}
        def slow_worker(action):
            worker_started.set()
            release_callbacks.wait(5)
            start = time.monotonic()
            time.sleep(duration)
            callback_durations['worker_seconds'] = time.monotonic() - start
            return {'status': 'unknown', 'message': 'synthetic ambiguous outcome; never retry'}
        dispatch = AsyncDispatcher(book, slow_worker, root, budget)
        task.start(slow_market)
        first = dispatch(allow_entries=False)
        if first.get('agent_called') is not True:
            raise RuntimeError('SYNTHETIC_WORKER_NOT_STARTED')
        if not market_started.wait(1) or not worker_started.wait(1):
            raise RuntimeError('BLOCKED_CALLBACK_NOT_STARTED')
        release_callbacks.set()
        started = time.monotonic()
        ticks = []
        concurrent_ticks = 0
        while time.monotonic() - started < duration:
            result = dispatch(allow_entries=False)
            if result.get('agent_called'):
                raise RuntimeError('DUPLICATE_WORKER_STARTED')
            ticks.append(time.monotonic())
            if task.thread.is_alive() and dispatch.task.thread is not None and dispatch.task.thread.is_alive():
                concurrent_ticks += 1
            time.sleep(poll)
        task.thread.join(5)
        task.take()
        if dispatch.task.thread is not None:
            dispatch.task.thread.join(5)
            dispatch(allow_entries=False)
        book.recover()
        states_before = [(a['id'], a['state']) for a in book.actions()]
        calls = budget.status()['total_calls_reserved']
        budget.close()
        book.close()
        # A genuine close/reopen boundary must not reclaim the ambiguous action.
        book = ExecutionBook(db, policy, simulation=True)
        book.recover()
        claimed = book.claim(allow_entries=False)
        states_after = [(a['id'], a['state']) for a in book.actions()]
        book.close()
        write(root / 'slow-result.json', {'duration_seconds': duration,
            'elapsed_seconds': time.monotonic() - started, 'control_ticks': len(ticks),
            'ticks_while_both_callbacks_blocked': concurrent_ticks,
            'callback_wall_time': callback_durations,
            'maximum_tick_gap_seconds': max([b-a for a,b in zip(ticks,ticks[1:])] or [0]),
            'budget_reservations': calls, 'restart_claim': claimed,
            'unknown_preserved_across_restart': states_before == states_after and all(s == 'UNKNOWN' for _,s in states_after),
            'actual_model_calls': 0, 'broker_writes': 0,
            'scope': 'real dispatcher/background tasks; synthetic book and blocked callbacks; not full live-session or broker transport'})


def wait_for(predicate, timeout=6):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            value = predicate()
            if value:
                return value
        except (OSError, ValueError, KeyError):
            pass
        time.sleep(.025)
    raise TimeoutError('OWNED_PROCESS_REHEARSAL_CONDITION_NOT_OBSERVED')


def run_rehearsal(output_dir, *, duration_seconds=120, poll_seconds=.05):
    if not .05 <= duration_seconds <= 180 or not .01 <= poll_seconds <= .25:
        raise ValueError('INVALID_LOCAL_REHEARSAL_BOUNDS')
    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    children = []
    script = str(Path(__file__).resolve())
    def spawn(mode, folder):
        process = subprocess.Popen([sys.executable, script, '--child-mode', mode,
            '--output-dir', str(folder), '--duration-seconds', str(duration_seconds),
            '--poll-seconds', str(poll_seconds)], stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        children.append(process)
        return process
    try:
        with tempfile.TemporaryDirectory(prefix='martin-local-process-') as directory:
            root = Path(directory).resolve()
            controller = spawn('control', root)
            wait_for(lambda: (root / 'status.json').exists())
            (root / 'controller.pid').write_text(str(controller.pid))
            watchdog = spawn('watchdog', root)
            read = lambda: json.loads((root / 'watchdog-result.json').read_text())
            wait_for(lambda: read() if read().get('healthy') else None)
            os.kill(controller.pid, signal.SIGSTOP)
            frozen = wait_for(lambda: read() if 'CONTROL_HEARTBEAT_STALE' in read()['issues'] else None)
            count = lambda: len((root / 'notifications.ndjson').read_text().splitlines())
            wait_for(lambda: count() >= 1)
            before = count()
            time.sleep(.2)
            unchanged_once = count() == before == 1
            os.kill(controller.pid, signal.SIGCONT)
            resumed = wait_for(lambda: read() if read().get('healthy') else None)
            sticky = entry_inhibit(str(root / 'fixture.sqlite') + '.entry-inhibit.json', root / 'fixture.sqlite')['blocked']
            controller.kill()
            controller.wait(3)
            killed = wait_for(lambda: read() if 'CONTROL_PROCESS_NOT_VERIFIED' in read()['issues'] else None)
            watchdog.terminate()
            watchdog.wait(3)
            with closing(sqlite3.connect(root / 'fixture.sqlite')) as connection:
                after = [dict(zip(('id','trade_id','kind','state','payload','created_at','updated_at','report'), row))
                         for row in connection.execute('SELECT * FROM live_actions ORDER BY created_at,id')]
            preserved = after == json.loads((root / 'initial-actions.json').read_text())
            slow_root = root / 'slow'
            slow_root.mkdir()
            slow = spawn('slow', slow_root)
            slow.wait(timeout=duration_seconds + 15)
            if slow.returncode:
                raise RuntimeError('SLOW_CHILD_FAILED:' + slow.stderr.read().decode()[-2000:])
            slow_result = json.loads((slow_root / 'slow-result.json').read_text())
            passed = (unchanged_once and sticky and preserved and resumed['new_entries_blocked']
                and frozen['new_entries_blocked'] and killed['new_entries_blocked']
                and slow_result['budget_reservations'] == 1 and slow_result['restart_claim'] is None
                and slow_result['unknown_preserved_across_restart'] and slow_result['control_ticks'] >= 2
                and slow_result['ticks_while_both_callbacks_blocked'] >= 2
                and all(v >= duration_seconds for v in slow_result['callback_wall_time'].values())
                and len(slow_result['callback_wall_time']) == 2
                and slow_result['maximum_tick_gap_seconds'] < 1)
            result = {'schema_version': 1, 'at': datetime.now(timezone.utc).isoformat(),
                'source_fingerprint': source_fingerprint(), 'passed': passed,
                'independent_process_watchdog': {'controller_pid': controller.pid, 'watchdog_pid': watchdog.pid,
                    'owned_controller_sigstop_detected': True, 'owned_controller_sigkill_detected': True,
                    'unchanged_incident_notified_once': unchanged_once, 'sticky_inhibit_after_sigcont': sticky,
                    'protection_and_unknown_rows_unchanged': preserved,
                    'process_checker_scope': 'actual owned fixture PID; production command identity matcher not exercised'},
                'blocked_callbacks': slow_result, 'broker_writes': 0, 'actual_model_calls': 0,
                'host_sleep_tested': False, 'real_auth_expiry_tested': False,
                'real_network_outage_tested': False, 'live_acceptance': False,
                'automatic_execute_inheritance': False,
                'limitations': ['Synthetic controller heartbeat is not a broker health observation.',
                    'No host sleep, lock-screen, application reopen, real network or credential invalidation performed.',
                    'This rehearsal never enables execution and does not prove unattended recovery.']}
            write(output / 'local-process-faults.json', result)
            return result
    finally:
        for process in children:
            if process.poll() is None:
                # SIGCONT ensures a stopped fixture can receive termination.
                try:
                    os.kill(process.pid, signal.SIGCONT)
                    process.kill()
                except ProcessLookupError:
                    pass
                process.wait(3)
            if process.stderr:
                process.stderr.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', required=True)
    parser.add_argument('--duration-seconds', type=float, default=120)
    parser.add_argument('--poll-seconds', type=float, default=.05)
    parser.add_argument('--child-mode', choices=['control', 'watchdog', 'slow'], help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.child_mode:
        child(args.child_mode, args.output_dir, args.duration_seconds, args.poll_seconds)
    else:
        result = run_rehearsal(args.output_dir, duration_seconds=args.duration_seconds, poll_seconds=args.poll_seconds)
        print(json.dumps(result, indent=2))
        sys.exit(0 if result['passed'] else 2)
