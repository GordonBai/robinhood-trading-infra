"""077r1: kill/reopen an isolated owner child at durable broker barriers."""
import json
import os
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
import uuid
from decimal import Decimal
from pathlib import Path
from unittest import TestCase

from test_receipt_identity import ACCOUNT, execution, order, snapshot

ROOT = Path(__file__).resolve().parents[1]
CHILD = ROOT / 'tests' / 'support' / 'owner_child.py'


class OwnerCrashRecoveryTests(TestCase):
    def test_two_active_instances_import_independent_fills_and_restart(self):
        from trading_infra.attribution import ExecutionAttribution
        from trading_infra.instance_accounting import InstanceAccounting
        from trading_infra.receipts import ExecutionReceiptImporter
        import sqlite3
        import tempfile
        from test_receipt_identity import AT

        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        db = sqlite3.connect(Path(temp.name) / 'two.sqlite', isolation_level='')
        self.addCleanup(db.close)
        attribution = ExecutionAttribution(db, clock=lambda: AT)
        accounting = InstanceAccounting(db, clock=lambda: AT)
        attribution.bind('ref-a', 'alpha', 'trade-a')
        attribution.bind('ref-b', 'beta', 'trade-b')
        importer = ExecutionReceiptImporter(
            db, attribution=attribution, accounting=accounting,
            clock=lambda: AT)
        first = order(order_id='ord-a', ref='ref-a',
                      executions=[execution('exec-a', fees='0.05')])
        second = order(order_id='ord-b', ref='ref-b',
                       executions=[execution('exec-b', fees='0.05')])
        outcome = importer.import_snapshot(snapshot([first, second]))
        self.assertEqual(len(outcome['imported']), 2)
        self.assertEqual(len(accounting.fills('alpha')), 1)
        self.assertEqual(len(accounting.fills('beta')), 1)
        again = InstanceAccounting(db, clock=lambda: AT)
        self.assertEqual(len(again.fills('alpha')), 1)
        self.assertEqual(len(again.fills('beta')), 1)

    def _spawn(self, workspace, mode):
        result = workspace / ('result-' + mode + '-' + uuid.uuid4().hex + '.json')
        token = uuid.uuid4().hex
        env = dict(os.environ)
        env['PYTHONPATH'] = str(ROOT / 'tests') + os.pathsep + env.get(
            'PYTHONPATH', '')
        process = subprocess.Popen(
            [sys.executable, str(CHILD), '--workspace', str(workspace),
             '--mode', mode, '--result', str(result), '--token', token],
            cwd=ROOT, env=env, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True)
        return process, result, token

    def _wait(self, predicate, process=None, timeout=20):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(.02)
        detail = ''
        if process is not None:
            if process.poll() is None:
                os.kill(process.pid, signal.SIGKILL)
                process.wait(timeout=5)
            stdout, stderr = process.communicate(timeout=5)
            detail = (stdout[-1000:], stderr[-3000:])
        self.fail('durable barrier/result timeout %r' % (detail,))

    def _kill(self, process):
        self.assertIsNone(process.poll())
        os.kill(process.pid, signal.SIGKILL)
        process.wait(timeout=5)
        # PID is reaped; no child-owned process may remain.
        self.assertIsNotNone(process.returncode)
        if process.stdout is not None:
            process.stdout.close()
        if process.stderr is not None:
            process.stderr.close()

    def _communicate(self, process, timeout=15):
        try:
            return process.communicate(timeout=timeout)
        except BaseException:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)
            if process.stdout is not None:
                process.stdout.close()
            if process.stderr is not None:
                process.stderr.close()
            raise

    def _state(self, workspace):
        return json.loads((workspace / 'broker.json').read_text())

    def _trade_rows(self, workspace):
        db = workspace / 'book.sqlite'
        con = sqlite3.connect(db)
        try:
            state = json.loads(con.execute(
                'SELECT payload FROM live_state WHERE id=1').fetchone()[0])
            actions = con.execute(
                'SELECT kind,state FROM live_actions').fetchall()
            external = con.execute(
                'SELECT operation,report FROM external_operations').fetchall()
            return state, actions, external
        finally:
            con.close()

    def test_crash_after_protection_accepted_without_receipt_recovers(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        workspace = Path(temp.name) / 'workspace'
        workspace.mkdir()
        child, _, token = self._spawn(workspace, 'protect-lost')
        try:
            self._wait(lambda: (workspace / 'barrier.json').exists(), child)
            marker = json.loads((workspace / 'barrier.json').read_text())
            self.assertEqual(marker['pid'], child.pid)
            self.assertEqual(marker['token'], token)
            self.assertEqual(marker['workspace'], str(workspace.resolve()))
            broker = self._state(workspace)
            stops = [order for order in broker['orders']
                     if order['side'] == 'sell' and order['trigger'] == 'stop']
            self.assertEqual(len(stops), 1)
            self._kill(child)
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=5)
        # The old request/packet/receipt/manifest must remain on disk.
        old_packets = sorted((workspace / 'output' / 'host-execution' /
                              'packets').glob('*.json'))
        self.assertTrue(old_packets)
        # A fresh owner must read the broker fact, not replay the old place.
        restart, result, _ = self._spawn(workspace, 'resume-protect')
        stdout, stderr = self._communicate(restart)
        self.assertEqual(restart.returncode, 0, (stdout, stderr))
        self.assertEqual(json.loads(result.read_text())['phase'], 'PROTECTED')
        state, actions, external = self._trade_rows(workspace)
        trade = next(iter(state['trades'].values()))
        self.assertEqual(trade['phase'], 'PROTECTED')
        self.assertEqual(sum(kind == 'protect' for kind, _ in actions), 1)
        self.assertFalse(any(kind == 'exit' for kind, _ in actions))
        self.assertEqual(len([order for order in self._state(workspace)[
            'orders'] if order['side'] == 'sell'
            and order['trigger'] == 'stop']), 1)
        self.assertTrue(all(path.exists() for path in old_packets),
                        'old Host packet was deleted')
        from martin_agentic.host_mailbox import read_request_result
        old_request = old_packets[0].stem
        viewed = read_request_result(
            workspace / 'output' / 'host-execution', old_request,
            clock=lambda: __import__('datetime').datetime.now(
                __import__('datetime').timezone.utc))
        self.assertNotEqual(viewed.get('status'), 'PREPARED',
                            'old epoch packet became callable again')

    def test_second_real_owner_is_refused_and_third_acquires_after_kill(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        workspace = Path(temp.name) / 'workspace'
        workspace.mkdir()
        first, _, token = self._spawn(workspace, 'protect-lost')
        try:
            self._wait(lambda: (workspace / 'barrier.json').exists(), first)
            second, second_result, _ = self._spawn(workspace, 'resume-protect')
            stdout, stderr = self._communicate(second, timeout=8)
            self.assertNotEqual(second.returncode, 0, (stdout, stderr))
            self.assertIn('already owns this database', stderr)
            self.assertFalse(second_result.exists(),
                             'second owner reached result handling')
            self.assertIsNone(first.poll())
            self._kill(first)
        finally:
            if first.poll() is None:
                first.kill()
                first.wait(timeout=5)
        third, result, _ = self._spawn(workspace, 'resume-protect')
        stdout, stderr = self._communicate(third)
        self.assertEqual(third.returncode, 0, (stdout, stderr))
        self.assertEqual(json.loads(result.read_text())['phase'], 'PROTECTED')
        self.assertTrue((workspace / 'barrier.json').exists(),
                        'old crash marker/evidence was removed')
        state, actions, _ = self._trade_rows(workspace)
        self.assertEqual(next(iter(state['trades'].values()))['phase'],
                         'PROTECTED')
        self.assertEqual(sum(kind == 'protect' for kind, _ in actions), 1)

    def test_crash_after_cancel_accepted_then_stop_fill_goes_flat(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        workspace = Path(temp.name) / 'workspace'
        workspace.mkdir()
        child, _, token = self._spawn(workspace, 'cancel-fill')
        try:
            self._wait(lambda: (workspace / 'barrier.json').exists(), child)
            marker = json.loads((workspace / 'barrier.json').read_text())
            self.assertEqual(marker['pid'], child.pid)
            self.assertEqual(marker['token'], token)
            self.assertEqual(marker['workspace'], str(workspace.resolve()))
            self._kill(child)
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=5)
        broker = self._state(workspace)
        stop = next(order for order in broker['orders']
                    if order['side'] == 'sell' and order['trigger'] == 'stop')
        self.assertEqual(stop['state'], 'pending_cancelled')
        # External broker fact after the crash: the stop actually filled.
        stop['state'] = 'filled'
        stop['cumulative_quantity'] = stop['quantity']
        stop['executions'] = [{
            'id': 'crash-stop-fill', 'symbol': 'DEMO', 'side': 'sell',
            'quantity': stop['quantity'], 'price': '39', 'fees': '0.05',
            'timestamp': broker.get('saved_at', '2026-10-16T14:00:00+00:00'),
            'order_id': stop['id']}]
        broker['position_qty'] = '0'
        broker['cash'] = '497.90'
        (workspace / 'broker.json').write_text(json.dumps(broker))
        restart, result, _ = self._spawn(workspace, 'resume-cancel')
        stdout, stderr = self._communicate(restart)
        self.assertEqual(restart.returncode, 0, (stdout, stderr))
        self.assertEqual(json.loads(result.read_text())['phase'],
                         'FLAT_VERIFIED')
        state, actions, external = self._trade_rows(workspace)
        trade = next(iter(state['trades'].values()))
        self.assertEqual(trade['phase'], 'FLAT_VERIFIED')
        self.assertFalse(any(kind == 'exit' for kind, _ in actions))
        sells = [order for order in self._state(workspace)['orders']
                 if order['side'] == 'sell' and order['state'] not in
                 {'filled', 'cancelled', 'canceled', 'rejected', 'expired'}]
        self.assertEqual(sells, [])
        self.assertEqual(broker['position_qty'], '0')
