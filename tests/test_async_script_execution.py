from pathlib import Path
import json
import threading
import time
from unittest import TestCase
from unittest.mock import patch

import test_script_execution
from martin_agentic.async_script_execution import AsyncScriptExecutionDriver
from martin_agentic.control_tasks import worker_lease


class BlockingTransport(test_script_execution.SyntheticTransport):
    def __init__(self):
        super().__init__()
        self.entered = threading.Event()
        self.release = threading.Event()
        self.block = False
        self.thread_ids = []

    def execute_simulated(self, packet):
        self.thread_ids.append(threading.get_ident())
        if self.block:
            self.entered.set()
            if not self.release.wait(2):
                raise TimeoutError('fixture block deadline')
        return super().execute_simulated(packet)


class AsyncScriptExecutionTests(TestCase):
    def setUp(self):
        self.f = test_script_execution.ScriptExecutionTests('test_expired_action_is_not_dispatched')
        self.f.setUp()
        self.transport = BlockingTransport()
        self.folder = Path(self.f.f.tmp.name) / 'script-receipts'
        self.driver = AsyncScriptExecutionDriver(self.f.book, self.transport, self.folder)
        self.addCleanup(self.cleanup)
        self.f.engineering()

    def cleanup(self):
        self.transport.release.set()
        if self.driver.worker.thread:
            self.driver.worker.thread.join(3)
        self.f.doCleanups()

    def step(self, snapshot=None):
        return self.driver.step(snapshot or self.f.f.snap(), self.f.quotes, allow_entries=True)

    def complete(self, snapshot=None):
        self.driver.worker.thread.join(2)
        self.assertFalse(self.driver.worker.thread.is_alive())
        return self.step(snapshot)

    def test_slow_transport_does_not_block_owner_reconciliation_and_lease(self):
        self.transport.block = True
        owner = threading.get_ident()
        self.assertEqual(self.step()['status'], 'STARTED_SYNTHETIC')
        self.assertTrue(self.transport.entered.wait(1))
        before = time.monotonic()
        for _ in range(3):
            self.assertEqual(self.step()['status'], 'IN_FLIGHT')
        self.assertLess(time.monotonic() - before, .5)
        self.assertIsNone(worker_lease(self.f.f.path))
        self.assertTrue(all(t != owner for t in self.transport.thread_ids))
        self.assertIsNotNone(self.f.book.state()['last_snapshot'])
        self.transport.release.set()
        self.assertEqual(self.complete()['status'], 'REVIEWED_SYNTHETIC')
        lease = worker_lease(self.f.f.path)
        self.assertIsNotNone(lease)
        lease.close()
        receipt = json.loads(next(self.folder.glob('*.json')).read_text())
        self.assertEqual(receipt['binding']['db'], str(self.f.f.path.resolve()))
        self.assertEqual(receipt['packet']['operation'], 'review')

    def test_review_then_place_are_separate_owner_steps_without_model(self):
        self.step()
        self.assertEqual(self.complete()['status'], 'REVIEWED_SYNTHETIC')
        self.assertEqual(len(self.transport.calls), 1)
        self.assertEqual(self.step()['operation'], 'place')
        result = self.complete()
        self.assertEqual(result['status'], 'submitted')
        self.assertFalse(result['broker_effect_verified'])
        self.assertNotEqual(self.f.book.state()['trades']['engineering-script']['phase'], 'PROTECTED')
        self.assertEqual(len(list(self.folder.glob('*.json'))), 2)
        self.assertEqual(self.f.book.db.execute('SELECT COUNT(*) FROM script_execution_receipts').fetchone()[0], 2)

    def test_stop_keeps_late_receipt_and_never_claims_no_write(self):
        self.step()
        self.complete()
        self.transport.block = True
        self.step()
        self.assertTrue(self.transport.entered.wait(1))
        self.assertEqual(self.driver.stop()['status'], 'STOPPING_AWAITING_RECEIPT')
        self.assertIsNone(worker_lease(self.f.f.path))
        self.assertEqual(self.step()['status'], 'IN_FLIGHT')
        self.transport.release.set()
        self.assertEqual(self.complete()['status'], 'submitted')
        self.assertEqual(self.step()['status'], 'STOPPED')
        self.assertEqual(len(self.transport.calls), 2)
        self.assertFalse(self.driver.status()['automatic_resubmission_permitted'])

    def test_late_place_receipt_after_observed_fill_does_not_regress_core(self):
        self.step()
        self.complete()
        self.transport.block = True
        self.step()
        self.assertTrue(self.transport.entered.wait(1))
        packet = self.driver.pending
        buy = self.f.bound(packet, filled='2', state='filled')
        snap = self.f.f.snap([buy], qty='2')
        self.assertEqual(self.step(snap)['status'], 'IN_FLIGHT')
        before = self.f.book.state()['trades']['engineering-script']['phase']
        self.transport.release.set()
        self.assertEqual(self.complete(snap)['status'], 'submitted')
        self.assertEqual(self.f.book.state()['trades']['engineering-script']['phase'], before)
        self.assertTrue(any(a['kind'] == 'protect' for a in self.f.book.actions()))

    def test_receipt_fsync_failure_after_place_becomes_unknown_not_retry(self):
        self.step()
        self.complete()
        with patch('martin_agentic.async_script_execution.os.fsync', side_effect=OSError('disk')):
            self.step()
            self.driver.worker.thread.join(2)
        self.assertEqual(self.step()['status'], 'unknown')
        self.assertEqual(self.step()['status'], 'AWAITING_RECONCILIATION')
        self.assertEqual(len(self.transport.calls), 2)

    def test_transport_exception_unknown_blocks_restarted_dispatcher(self):
        self.step()
        self.complete()
        self.transport.fail = True
        self.step()
        self.assertEqual(self.complete()['status'], 'unknown')
        self.driver = AsyncScriptExecutionDriver(self.f.book, self.transport, self.folder)
        self.assertEqual(self.step()['status'], 'AWAITING_RECONCILIATION')
        self.assertEqual(len(self.transport.calls), 2)

    def test_real_path_refused_without_files_schema_or_trade_changes(self):
        before = list(self.f.book.db.iterdump())
        self.f.book.simulation = False
        try:
            self.assertEqual(self.step()['status'], 'BLOCKED_CAPABILITY')
        finally:
            self.f.book.simulation = True
        self.assertEqual(list(self.f.book.db.iterdump()), before)
        self.assertFalse(self.folder.exists())
        self.assertFalse(Path(str(self.f.f.path) + '.worker.lock').exists())
        self.assertEqual(self.transport.calls, [])

    def test_existing_worker_lease_prevents_reservation(self):
        lease = worker_lease(self.f.f.path)
        try:
            self.assertEqual(self.step()['status'], 'BLOCKED_OWNERSHIP')
        finally:
            lease.close()
        self.assertEqual(self.f.book.db.execute('SELECT COUNT(*) FROM external_operations').fetchone()[0], 0)

    def test_nonowner_call_rejected_before_sqlite_access(self):
        failures = []
        def other_owner():
            try:
                self.driver.step({'synthetic': True})
            except Exception as error:
                failures.append(error)
        thread = threading.Thread(target=other_owner)
        thread.start()
        thread.join(1)
        self.assertEqual(len(failures), 1)
        self.assertIsInstance(failures[0], ValueError)
        self.assertEqual(str(failures[0]), 'SCRIPT_EXECUTION_OWNER_THREAD_REQUIRED')
        self.assertFalse(self.folder.exists())
