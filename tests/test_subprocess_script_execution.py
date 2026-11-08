from datetime import timedelta
from pathlib import Path
import json
import os
import subprocess
import signal
import threading
import time
from unittest import TestCase
from unittest.mock import patch

import test_script_execution
from martin_agentic.control_tasks import worker_lease
from martin_agentic.subprocess_script_execution import SubprocessScriptExecutionDriver
from martin_agentic.subprocess_script_execution import _remaining_child_time


class SubprocessScriptExecutionTests(TestCase):
    def setUp(self):
        self.f = test_script_execution.ScriptExecutionTests('test_expired_action_is_not_dispatched')
        self.f.setUp()
        self.folder = Path(self.f.f.tmp.name) / 'subprocess-receipts'
        self.started = time.monotonic()
        self.base = self.f.f.now
        self.driver = SubprocessScriptExecutionDriver(self.f.book, {}, self.folder, io_timeout_seconds=1)
        self.orders = []
        self.qty = '0'
        self.held = '0'
        self.addCleanup(self.cleanup)
        self.f.engineering()

    def cleanup(self):
        if self.driver.pending:
            self.driver.stop()
            process = self.driver.pending['process']
            process.wait(timeout=3)
            self.driver.pending['lease'].close()
            self.driver.pending = None
        self.f.doCleanups()

    def snapshot(self):
        # Advance the fixture clock by ACTUAL elapsed monotonic time. A fixed
        # broker timestamp must not hide the latency this test is proving.
        self.f.f.now = self.base + timedelta(seconds=time.monotonic() - self.started)
        current = self.f.f.now.isoformat()
        self.f.f.quote.update(bid_at=current, ask_at=current, observed_at=current)
        return self.f.f.snap(self.orders, qty=self.qty, held=self.held)

    def step(self):
        return self.driver.step(self.snapshot(), self.f.quotes, allow_entries=True)

    def await_terminal(self):
        deadline = time.monotonic() + 3
        result = None
        while self.driver.pending and time.monotonic() < deadline:
            result = self.step()
            if self.driver.pending:
                time.sleep(.01)
        self.assertIsNone(self.driver.pending, result)
        return result

    def review_entry(self):
        result = self.step()
        self.assertEqual(result['operation'], 'review')
        self.assertEqual(self.await_terminal()['status'], 'REVIEWED_SYNTHETIC')

    def hang_entry_place(self):
        self.driver = SubprocessScriptExecutionDriver(self.f.book,
            {'by_kind': {'entry': {'place': {'delay_seconds': 120}}}}, self.folder, io_timeout_seconds=1)
        self.review_entry()
        self.assertEqual(self.step()['operation'], 'place')
        pending = self.driver.pending
        deadline = time.monotonic() + .8
        while not pending['started_path'].exists() and time.monotonic() < deadline:
            time.sleep(.005)
        self.assertTrue(pending['started_path'].exists(), 'real child must enter its 120s fixture')
        marker = json.loads(pending['started_path'].read_text())
        self.assertEqual(marker['delay_seconds'], 120)
        self.assertEqual(marker['pid'], pending['process'].pid)
        return pending['packet']

    def observed_buy(self, packet):
        self.snapshot()
        buy = self.f.bound(packet, filled='2', state='filled')
        buy['created_at'] = self.f.f.now.isoformat()
        buy['executions'][0]['timestamp'] = self.f.f.now.isoformat()
        self.orders = [buy]
        self.qty = '2'
        return buy

    def test_hung_entry_is_killed_reaped_then_protected_with_elapsed_clock_under_30_seconds(self):
        """Actual subprocess sleeps120; kill<=5; synthetic coverage<30.

        Reproduce this exact test for the timing acceptance report. Broker facts
        are synthetic, while child PID, kill/reap and monotonic latency are real.
        """
        packet = self.hang_entry_place()
        process = self.driver.pending['process']
        self.assertIsNone(process.poll())
        self.observed_buy(packet)
        fill_real = time.monotonic()
        fill_sim = self.f.f.now
        self.assertEqual(self.step()['status'], 'IN_FLIGHT')
        self.assertTrue(any(a['kind'] == 'protect' for a in self.f.book.actions()))
        self.assertIsNone(worker_lease(self.f.f.path))
        result = self.await_terminal()
        terminal = self.driver.status()['last_terminal']
        self.assertEqual(result['status'], 'unknown')
        self.assertTrue(terminal['reaped'])
        self.assertTrue(terminal['kill_requested'] or terminal['returncode'] == -signal.SIGALRM)
        self.assertIn(terminal['termination_reason'], {'SYNTHETIC_IO_DEADLINE_EXCEEDED',
                                                      'SYNTHETIC_CHILD_FAILSAFE_DEADLINE_EXCEEDED'})
        self.assertLess(terminal['returncode'], 0)
        self.assertLess(terminal['elapsed_seconds'], 5)
        self.assertIsNotNone(process.returncode)
        self.assertFalse(any(a['state'] == 'UNKNOWN' for a in self.f.book.actions()))
        self.assertEqual(self.step()['operation'], 'review')
        dispatch_delay = time.monotonic() - fill_real
        self.assertLess(dispatch_delay, 30)
        self.assertEqual(self.await_terminal()['status'], 'REVIEWED_SYNTHETIC')
        self.assertEqual(self.step()['operation'], 'place')
        protect_packet = self.driver.pending['packet']
        self.assertEqual(self.await_terminal()['status'], 'submitted')
        self.snapshot()
        stop = self.f.bound(protect_packet)
        stop['created_at'] = self.f.f.now.isoformat()
        self.orders.append(stop)
        self.held = '2'
        self.step()
        trade = self.f.book.state()['trades']['engineering-script']
        self.assertEqual(trade['phase'], 'PROTECTED')
        coverage_delay_real = time.monotonic() - fill_real
        coverage_delay_sim = (self.f.f.now - fill_sim).total_seconds()
        self.assertGreater(coverage_delay_real, .2)
        self.assertLess(coverage_delay_real, 30)
        self.assertLess(coverage_delay_sim, 30)
        self.assertGreater(float(trade['protection_observation_delay_seconds']), .2)
        self.assertTrue(trade['protection_timing_synthetic'])
        lease = worker_lease(self.f.f.path)
        self.assertIsNotNone(lease)
        lease.close()

    def test_timeout_without_exact_broker_order_stays_unknown_and_does_not_resubmit(self):
        self.hang_entry_place()
        packet = self.driver.pending['packet']
        self.assertEqual(self.await_terminal()['status'], 'unknown')
        count = self.f.book.db.execute('SELECT COUNT(*) FROM external_operations').fetchone()[0]
        for _ in range(3):
            self.assertEqual(self.step()['status'], 'AWAITING_RECONCILIATION')
        self.assertEqual(self.f.book.db.execute('SELECT COUNT(*) FROM external_operations').fetchone()[0], count)
        self.assertFalse(any(a['kind'] == 'protect' for a in self.f.book.actions()))
        self.observed_buy(packet)
        self.assertEqual(self.step()['operation'], 'review')
        self.assertEqual(self.driver.pending['packet']['parameters']['side'], 'sell')

    def protected_position(self):
        self.review_entry()
        self.assertEqual(self.step()['operation'], 'place')
        entry = self.driver.pending['packet']
        self.assertEqual(self.await_terminal()['status'], 'submitted')
        self.observed_buy(entry)
        self.assertEqual(self.step()['operation'], 'review')
        self.assertEqual(self.await_terminal()['status'], 'REVIEWED_SYNTHETIC')
        self.assertEqual(self.step()['operation'], 'place')
        protection = self.driver.pending['packet']
        self.assertEqual(self.await_terminal()['status'], 'submitted')
        self.snapshot()
        stop = self.f.bound(protection)
        stop['created_at'] = self.f.f.now.isoformat()
        self.orders.append(stop)
        self.held = '2'
        self.step()
        self.assertEqual(self.f.book.state()['trades']['engineering-script']['phase'], 'PROTECTED')
        return stop

    def test_fixed_child_complete_round_trip_waits_for_cancel_terminal(self):
        stop = self.protected_position()
        self.f.book.request_engineering_exit('engineering-script', self.snapshot())
        self.assertEqual(self.step()['operation'], 'cancel')
        self.assertEqual(self.await_terminal()['status'], 'cancel_requested')
        self.step()
        self.assertFalse(any(a['kind'] == 'exit' for a in self.f.book.actions()))
        stop['state'] = 'cancelled'
        self.held = '0'
        self.assertEqual(self.step()['operation'], 'review')
        self.assertEqual(self.await_terminal()['status'], 'REVIEWED_SYNTHETIC')
        self.assertEqual(self.step()['operation'], 'place')
        exit_packet = self.driver.pending['packet']
        self.assertEqual(self.await_terminal()['status'], 'submitted')
        self.snapshot()
        sell = self.f.bound(exit_packet, filled='2', state='filled')
        sell['executions'][0]['timestamp'] = self.f.f.now.isoformat()
        self.orders.append(sell)
        self.qty = '0'
        self.step()
        self.assertEqual(self.f.book.state()['trades']['engineering-script']['phase'], 'FLAT_VERIFIED')
        self.assertEqual(self.f.book.db.execute('SELECT count(*) FROM script_execution_receipts').fetchone()[0], 7)
        self.assertTrue(self.f.book.audit.verify()['integrity_valid'])

    def test_fixed_child_cancel_fill_race_does_not_submit_another_exit(self):
        stop = self.protected_position()
        self.f.book.request_engineering_exit('engineering-script', self.snapshot())
        self.assertEqual(self.step()['operation'], 'cancel')
        self.assertEqual(self.await_terminal()['status'], 'cancel_requested')
        count = self.f.book.db.execute('SELECT count(*) FROM external_operations').fetchone()[0]
        self.snapshot()
        stop.update(state='filled', cumulative_quantity='2', average_price='39')
        stop['executions'] = [{'id': 'fixed-child-stop-fill', 'timestamp': self.f.f.now.isoformat(),
                               'price': '39', 'quantity': '2', 'fees': '0'}]
        self.qty = self.held = '0'
        self.step()
        self.assertIsNone(self.driver.pending)
        self.assertEqual(self.f.book.state()['trades']['engineering-script']['phase'], 'FLAT_VERIFIED')
        self.assertEqual(self.f.book.db.execute('SELECT count(*) FROM external_operations').fetchone()[0], count)

    def test_stop_kill_does_not_release_until_owned_child_is_reaped(self):
        self.hang_entry_place()
        process = self.driver.pending['process']
        self.assertEqual(self.driver.stop()['status'], 'TERMINATING')
        # Hold observation at the unreaped stage, without altering the PID or
        # sending signals anywhere except this fixture's owned Popen child.
        with patch.object(process, 'poll', return_value=None):
            self.assertEqual(self.step()['status'], 'TERMINATING')
            self.assertFalse(self.driver.status()['reaped'])
            self.assertIsNone(worker_lease(self.f.f.path))
        result = self.await_terminal()
        self.assertEqual(result['status'], 'unknown')
        self.assertTrue(result['child_reaped'])
        self.assertEqual(self.step()['status'], 'STOPPED')
        self.assertEqual(self.f.book.actions()[0]['state'], 'UNKNOWN')
        lease = worker_lease(self.f.f.path)
        self.assertIsNotNone(lease)
        lease.close()

    def test_review_timeout_waits_user_without_unknown_write(self):
        self.driver = SubprocessScriptExecutionDriver(self.f.book,
            {'operations': {'review': {'delay_seconds': 120}}}, self.folder, io_timeout_seconds=.5)
        self.step()
        self.assertEqual(self.await_terminal()['status'], 'WAITING_USER')
        self.assertEqual(self.f.book.actions()[0]['state'], 'WAITING_USER')
        self.assertEqual(self.step()['status'], 'WAITING_USER')

    def test_real_book_snapshot_cannot_create_files_lease_process_or_schema(self):
        before = list(self.f.book.db.iterdump())
        self.f.book.simulation = False
        try:
            self.assertEqual(self.step()['status'], 'BLOCKED_CAPABILITY')
        finally:
            self.f.book.simulation = True
        snap = self.snapshot()
        snap['synthetic'] = False
        self.assertEqual(self.driver.step(snap)['status'], 'BLOCKED_CAPABILITY')
        self.assertEqual(list(self.f.book.db.iterdump()), before)
        self.assertFalse(self.folder.exists())
        self.assertFalse(Path(str(self.f.f.path) + '.worker.lock').exists())
        self.assertIsNone(self.driver.pending)

    def test_lease_exclusion_and_owner_thread_enforced(self):
        lease = worker_lease(self.f.f.path)
        try:
            self.assertEqual(self.step()['status'], 'BLOCKED_OWNERSHIP')
        finally:
            lease.close()
        failures = []
        def nonowner():
            try:
                self.driver.step({'synthetic': True})
            except Exception as error:
                failures.append(error)
        thread = threading.Thread(target=nonowner)
        thread.start()
        thread.join(1)
        self.assertEqual(str(failures[0]), 'SCRIPT_EXECUTION_OWNER_THREAD_REQUIRED')
        self.assertFalse(self.folder.exists())

    def test_fixture_schema_rejects_commands_callables_and_longer_deadlines(self):
        for spec in ({'command': '/bin/sh'}, {'operations': {'place': {'callable': lambda: 1}}},
                     {'operations': {'place': {'delay_seconds': True}}},
                     {'operations': {'place': {'outcome': []}}}):
            with self.assertRaises(ValueError):
                SubprocessScriptExecutionDriver(self.f.book, spec, self.folder)
        for timeout in (True, 6, 0, float('nan')):
            with self.assertRaises(ValueError):
                SubprocessScriptExecutionDriver(self.f.book, {}, self.folder, io_timeout_seconds=timeout)
        self.assertFalse(self.folder.exists())

    def test_child_deadline_deducts_startup_instead_of_granting_fresh_timeout(self):
        with patch('martin_agentic.subprocess_script_execution.time.monotonic', return_value=10):
            self.assertEqual(_remaining_child_time({'timeout_seconds': 5, 'deadline_monotonic': 12}), 2)
            for deadline in [9, 10, 16, True, float('nan')]:
                with self.assertRaises(ValueError):
                    _remaining_child_time({'timeout_seconds': 5, 'deadline_monotonic': deadline})

    def test_child_receipt_is_bound_to_operation_and_has_minimal_environment(self):
        with patch('martin_agentic.subprocess_script_execution.subprocess.Popen', wraps=subprocess.Popen) as spawn:
            self.review_entry()
        arguments = spawn.call_args.kwargs
        self.assertEqual(set(arguments['env']), {'PATH', 'PYTHONPATH', 'PYTHONNOUSERSITE', 'PYTHONDONTWRITEBYTECODE'})
        self.assertTrue(arguments['close_fds'])
        self.assertEqual(len(arguments['pass_fds']), 1)
        self.assertNotIn('shell', arguments)
        receipt = json.loads(next(self.folder.glob('*.receipt.json')).read_text())
        self.assertEqual(receipt['binding']['db'], str(self.f.f.path.resolve()))
        self.assertEqual(receipt['packet']['operation'], 'review')
        self.assertTrue(receipt['synthetic'])
        self.assertTrue(self.driver.status()['last_terminal']['receipt_verified'])
        self.assertNotEqual(receipt['pid'], os.getpid())
        self.assertEqual(Path(next(self.folder.glob('*.receipt.json'))).stat().st_mode & 0o777, 0o600)

    def test_child_inherits_lease_until_it_dies_even_if_owner_handle_closes(self):
        self.hang_entry_place()
        # Emulate only the owner's lock descriptor disappearing. The child
        # inherited that same open-file description and must still hold it.
        self.driver.pending['lease'].close()
        self.assertIsNone(worker_lease(self.f.f.path))
        self.driver.stop()
        self.assertEqual(self.await_terminal()['status'], 'unknown')
        lease = worker_lease(self.f.f.path)
        self.assertIsNotNone(lease)
        lease.close()

    def test_warning_and_pending_approval_remain_unverified(self):
        self.driver = SubprocessScriptExecutionDriver(self.f.book,
            {'operations': {'review': {'outcome': 'warning'}}}, self.folder, io_timeout_seconds=1)
        self.step()
        self.assertEqual(self.await_terminal()['status'], 'WAITING_USER')
        self.assertEqual(self.step()['status'], 'WAITING_USER')
        self.assertEqual(self.f.book.actions()[0]['state'], 'WAITING_USER')
