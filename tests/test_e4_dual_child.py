"""078r3b: dual active owner child is SIGKILLed and a fresh child reopens."""
import copy
import hashlib
import json
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest import TestCase

from martin_agentic.audit import AuditTrail, digest
from test_default_lifecycle import NOW, TradingClock

CHILD = Path('tests/support/dual_owner_child.py').resolve()


def prefix_digest(rows):
    return hashlib.sha256(json.dumps(
        [row[1] for row in rows], separators=(',', ':'),
        sort_keys=True).encode()).hexdigest()


class DualChildRecoveryTests(TestCase):
    def _wait(self, path, child, timeout=20):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if path.exists() and path.read_text().strip():
                try:
                    return json.loads(path.read_text())
                except json.JSONDecodeError:
                    pass
            if child.poll() is not None:
                break
            time.sleep(.02)
        if child.poll() is None:
            child.kill()
        stdout, stderr = child.communicate(timeout=2)
        self.fail('child did not reach barrier/result rc=%r stdout=%r stderr=%r'
                  % (child.returncode, stdout, stderr))

    def _reap(self, child):
        if child.poll() is None:
            child.kill()
        try:
            child.communicate(timeout=2)
        except subprocess.TimeoutExpired:
            child.kill()
            child.communicate(timeout=2)

    def _spawn(self, workspace, mode, token, result):
        child = subprocess.Popen(
            [sys.executable, str(CHILD), '--workspace', str(workspace),
             '--mode', mode, '--token', token, '--result', str(result)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.addCleanup(self._reap, child)
        return child

    def _spawn_fact(self, workspace, fact, token, result):
        child = subprocess.Popen(
            [sys.executable, str(CHILD), '--workspace', str(workspace),
             '--mode', 'facts', '--fact', fact, '--token', token,
             '--result', str(result)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.addCleanup(self._reap, child)
        return child

    def _spawn_late(self, workspace, case, token, result):
        child = subprocess.Popen(
            [sys.executable, str(CHILD), '--workspace', str(workspace),
             '--mode', 'epoch-resume', '--late-case', case,
             '--token', token, '--result', str(result)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.addCleanup(self._reap, child)
        return child

    def test_two_active_protections_survive_sigkill_and_fresh_child(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        workspace = Path(temp.name) / 'workspace'
        workspace.mkdir()
        token = uuid.uuid4().hex
        initial_result = workspace / 'initial.json'
        resume_result = workspace / 'resume.json'
        child = self._spawn(workspace, 'seed', token, initial_result)
        before = self._wait(workspace / 'barrier.json', child)
        self.assertEqual(before['pid'], child.pid)
        self.assertEqual(before['token'], token)
        self.assertEqual(before['workspace'], str(workspace.resolve()))
        checkpoint = before['checkpoint']
        self.assertEqual(checkpoint['budget']['open_cash'], '80.00')
        self.assertEqual(checkpoint['budget']['open_notional'], '80.00')
        self.assertEqual(checkpoint['budget']['open_planned_loss'], '0.90')
        self.assertEqual(checkpoint['budget']['open_trades'], 2)
        self.assertEqual(checkpoint['leases'], {'DEMO': 'alpha', 'SPY': 'beta'})
        self.assertEqual(checkpoint['cash'], '419.90')
        self.assertEqual({t['phase'] for t in checkpoint['trades'].values()},
                         {'PROTECTED'})
        self.assertEqual(checkpoint['fills'], {'alpha': 1, 'beta': 1})
        self.assertEqual(checkpoint['orders'], 4)
        self.assertGreaterEqual(len(checkpoint['refs']), 2)
        self.assertEqual(len(checkpoint['intents']), 2)
        self.assertEqual({row[1] for row in checkpoint['refs']},
                         {'alpha', 'beta'})
        self.assertEqual(len(checkpoint['fill_owners']), 2)
        self.assertEqual({row[1] for row in checkpoint['fill_owners']},
                         {'alpha', 'beta'})
        self.assertEqual(len(checkpoint['protections']), 2)
        self.assertEqual({row[2] for row in checkpoint['protections']},
                         {'confirmed'})
        for instance_id in ('alpha', 'beta'):
            summary = checkpoint['accounting'][instance_id]
            self.assertEqual(summary['fills'], 1)
            self.assertEqual(summary['fees'], '0.05')
            self.assertEqual(summary['open_quantity'], '1')
        self.assertEqual(set(checkpoint['risk']['executions']),
                         {'seed-DEMO-exec', 'seed-SPY-exec'})
        self.assertEqual(checkpoint['equity'], '499.90')
        self.assertEqual(checkpoint['marks'], {'DEMO': '40.00', 'SPY': '40.00'})
        self.assertEqual(checkpoint['risk_rows'], 1)
        self.assertEqual(len(checkpoint['risk_db']), 1)
        self.assertEqual(checkpoint['risk_db'][0][0], 0)
        self.assertEqual(checkpoint['risk_db'][0][1], 'main')
        self.assertTrue(checkpoint['risk_db'][0][2].endswith('/risk.sqlite'))
        self.assertTrue(checkpoint['risk_db'][0][2].startswith(str(workspace.resolve())))
        risk = checkpoint['risk']
        self.assertEqual(risk['units'], '500')
        self.assertEqual(risk['day_start_equity'], '500')
        self.assertEqual(risk['equity'], '499.90')
        self.assertEqual(risk['cash'], '419.90')
        self.assertEqual(risk['nav'], '0.9998')
        self.assertEqual(risk['nav_high'], '1.0002')
        self.assertEqual(
            risk['drawdown'],
            str(Decimal('1') - Decimal('499.90') / Decimal('500.10')))
        self.assertFalse(risk['daily_halted'])
        self.assertFalse(risk['project_halted'])
        self.assertFalse(risk['blocked'])
        self.assertEqual(risk['reasons'], [])
        pid = child.pid
        os.kill(pid, signal.SIGKILL)
        child.wait(timeout=10)
        child.communicate(timeout=2)
        self.assertFalse(Path('/proc/%d' % pid).exists()
                         if Path('/proc').exists() else False)

        db = workspace / 'book.sqlite'
        con = sqlite3.connect(db)
        try:
            action_rows = con.execute(
                'SELECT id,kind,state,trade_id FROM live_actions ORDER BY id'
            ).fetchall()
            audit_prefix = con.execute(
                'SELECT sequence,entry_hash FROM audit_events ORDER BY sequence'
            ).fetchall()
        finally:
            con.close()
        self.assertEqual(len(action_rows), 4)
        self.assertEqual([row[1] for row in action_rows].count('protect'), 2)

        resumed = self._spawn(workspace, 'resume', token + '-new',
                              resume_result)
        self.assertNotEqual(resumed.pid, pid)
        after = self._wait(resume_result, resumed)
        rc = resumed.wait(timeout=10)
        stdout, stderr = resumed.communicate(timeout=2)
        self.assertEqual(rc, 0, (stdout, stderr))
        self.assertEqual(after['budget'], checkpoint['budget'])
        self.assertEqual(after['leases'], checkpoint['leases'])
        self.assertEqual(after['trades'], checkpoint['trades'])
        self.assertEqual(after['fills'], checkpoint['fills'])
        self.assertEqual(after['refs'], checkpoint['refs'])
        self.assertEqual(after['intents'], checkpoint['intents'])
        self.assertEqual(after['fill_owners'], checkpoint['fill_owners'])
        self.assertEqual(after['accounting'], checkpoint['accounting'])
        self.assertEqual(after['protections'], checkpoint['protections'])
        self.assertEqual(len(audit_prefix), checkpoint['audit'])
        self.assertEqual(checkpoint['audit_prefix_sha256'],
                         prefix_digest(audit_prefix))
        self.assertEqual(after['cash'], '419.90')
        self.assertEqual(after['orders'], 4)
        self.assertEqual(after['equity'], '499.90')
        self.assertEqual(after['risk'], checkpoint['risk'])
        self.assertEqual(after['risk_rows'], 1)
        self.assertEqual(after['risk_db'], checkpoint['risk_db'])
        before_risk = after['risk_before']
        self.assertEqual(before_risk['nav_high'], '1.0002')
        self.assertEqual(before_risk['day_start_equity'], '500')
        self.assertEqual(before_risk['units'], '500')
        self.assertEqual(set(before_risk['executions']),
                         {'seed-DEMO-exec', 'seed-SPY-exec'})
        self.assertFalse(before_risk['daily_halted'])
        self.assertFalse(before_risk['project_halted'])
        self.assertEqual(after['risk']['units'], '500')
        self.assertEqual(after['risk']['day_start_equity'], '500')
        self.assertEqual(after['risk']['nav_high'], '1.0002')
        self.assertEqual(after['risk']['drawdown'], risk['drawdown'])
        self.assertGreaterEqual(after['audit'], checkpoint['audit'])

        con = sqlite3.connect(db)
        try:
            self.assertEqual(con.execute(
                'SELECT id,kind,state,trade_id FROM live_actions ORDER BY id'
            ).fetchall(), action_rows)
            after_audit = con.execute(
                'SELECT sequence,entry_hash FROM audit_events ORDER BY sequence'
            ).fetchall()
            self.assertEqual(after_audit[:len(audit_prefix)], audit_prefix)
            self.assertEqual(prefix_digest(audit_prefix),
                             checkpoint['audit_prefix_sha256'])
            self.assertEqual(con.execute(
                "SELECT count(*) FROM live_actions WHERE kind='protect'").fetchone()[0], 2)
            self.assertEqual(con.execute(
                'SELECT count(*) FROM broker_executions').fetchone()[0], 2)
            self.assertEqual(con.execute(
                'SELECT count(*) FROM execution_intents WHERE state=\'DISPATCHED\''
            ).fetchone()[0], 2)
            trail = AuditTrail(connection=con)
            self.assertTrue(trail.verify()['integrity_valid'])
        finally:
            con.close()

    def test_missing_or_empty_copied_risk_file_fails_closed(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        source = Path(temp.name) / 'source'
        source.mkdir()
        child = self._spawn(source, 'seed', uuid.uuid4().hex,
                            source / 'initial.json')
        before = self._wait(source / 'barrier.json', child)
        checkpoint = before['checkpoint']
        os.kill(child.pid, signal.SIGKILL)
        child.wait(timeout=10)
        child.communicate(timeout=2)
        con = sqlite3.connect(source / 'book.sqlite')
        try:
            source_actions = con.execute(
                'SELECT id,kind,state,trade_id FROM live_actions ORDER BY id'
            ).fetchall()
        finally:
            con.close()
        db_hash = hashlib.sha256((source / 'book.sqlite').read_bytes()).hexdigest()
        broker_hash = hashlib.sha256(
            (source / 'broker.json').read_bytes()).hexdigest()
        for label, mutate in (
                ('missing', lambda folder: (folder / 'risk.sqlite').unlink()),
                ('empty', lambda folder: (folder / 'risk.sqlite').write_bytes(b''))):
            with self.subTest(label=label):
                broken = Path(temp.name) / label
                shutil.copytree(source, broken)
                mutate(broken)
                result = Path(temp.name) / (label + '.json')
                resumed = self._spawn(broken, 'resume', uuid.uuid4().hex,
                                      result)
                answer = self._wait(result, resumed)
                rc = resumed.wait(timeout=10)
                stdout, stderr = resumed.communicate(timeout=2)
                self.assertNotEqual(rc, 0, (label, stdout, stderr))
                self.assertEqual(answer['error'], 'RISK_STATE_MISSING')
                con = sqlite3.connect(broken / 'book.sqlite')
                try:
                    self.assertEqual(con.execute(
                        'SELECT id,kind,state,trade_id FROM live_actions '
                        'ORDER BY id').fetchall(), source_actions)
                finally:
                    con.close()
                self.assertEqual(
                    hashlib.sha256((broken / 'book.sqlite').read_bytes()).hexdigest(),
                    db_hash)
                self.assertEqual(
                    hashlib.sha256(
                        (broken / 'broker.json').read_bytes()).hexdigest(),
                    broker_hash)
                self.assertEqual(checkpoint['risk_rows'], 1)

    def test_unprotected_deadline_survives_restart_without_new_window(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        workspace = Path(temp.name) / 'workspace'
        workspace.mkdir()
        token = uuid.uuid4().hex
        initial_result = workspace / 'initial.json'
        resume_result = workspace / 'resume.json'
        child = self._spawn(workspace, 'sla-seed', token, initial_result)
        before = self._wait(workspace / 'barrier.json', child)
        checkpoint = before['checkpoint']
        trades = checkpoint['trades']
        demo = next(trade for trade in trades.values()
                    if trade['phase'] == 'NEEDS_PROTECTION')
        spy = next(trade for trade in trades.values()
                   if trade['phase'] == 'PROTECTED')
        demo_id = next(trade_id for trade_id, trade in trades.items()
                       if trade is demo)
        spy_id = next(trade_id for trade_id, trade in trades.items()
                      if trade is spy)
        self.assertIsNotNone(demo['first_fill_at'])
        self.assertIsNotNone(demo['unprotected_since'])
        self.assertEqual(spy['unprotected_since'], None)
        deadline = (datetime.fromisoformat(demo['unprotected_since'])
                    + timedelta(seconds=30))
        self.assertEqual(checkpoint['sla']['unprotected'][demo_id][
            'unprotected_since'], demo['unprotected_since'])
        self.assertEqual(datetime.fromisoformat(
            checkpoint['sla']['unprotected'][demo_id]['deadline']), deadline)
        self.assertLess(datetime.fromisoformat(
            checkpoint['sla']['state_time']), deadline)
        self.assertEqual(checkpoint['sla']['unconfirmed_protections'],
                         ['DEMO'])
        protections = {row[0]: row for row in checkpoint['protections']}
        self.assertEqual(protections['DEMO'][2], 'submitted')
        self.assertEqual(protections['SPY'][2], 'confirmed')
        self.assertEqual(checkpoint['risk_rows'], 1)
        self.assertEqual(checkpoint['risk']['day_start_equity'], '500')
        host_receipts = list(
            (workspace / 'output' / 'host-execution' / 'receipts').glob('*.json'))
        stop_receipt = False
        for receipt_path in host_receipts:
            value = json.loads(receipt_path.read_text())
            data = (value.get('response', {}).get('structuredContent', {})
                    .get('data', {}))
            order = data.get('order') or {}
            if (order.get('id') == protections['DEMO'][1]
                    and order.get('side') == 'sell'
                    and order.get('trigger') == 'stop'):
                stop_receipt = True
        self.assertTrue(stop_receipt, 'local Host stop receipt not bound')
        self.assertEqual(checkpoint['risk']['units'], '500')
        self.assertEqual(checkpoint['risk']['nav'], '0.9998')
        self.assertEqual(checkpoint['fills'], {'alpha': 1, 'beta': 1})

        con = sqlite3.connect(workspace / 'book.sqlite')
        try:
            before_actions = con.execute(
                'SELECT id,kind,trade_id FROM live_actions '
                'ORDER BY created_at,id').fetchall()
        finally:
            con.close()
        pid = child.pid
        os.kill(pid, signal.SIGKILL)
        child.wait(timeout=10)
        child.communicate(timeout=2)

        resumed = self._spawn(workspace, 'sla-resume', token + '-new',
                              resume_result)
        self.assertNotEqual(resumed.pid, pid)
        after = self._wait(resume_result, resumed)
        rc = resumed.wait(timeout=10)
        stdout, stderr = resumed.communicate(timeout=2)
        self.assertEqual(rc, 0, (stdout, stderr))
        after_trades = after['trades']
        after_demo = after_trades[demo_id]
        after_spy = after_trades[spy_id]
        self.assertEqual(after_demo['first_fill_at'], demo['first_fill_at'])
        self.assertEqual(after_demo['unprotected_since'],
                         demo['unprotected_since'])
        self.assertEqual(after_demo['phase'], 'EXIT_REQUESTED')
        self.assertEqual(after_spy['phase'], 'PROTECTED')
        self.assertIn('PROTECTION_WAIT_EXCEEDED:DEMO', after['issues'])
        self.assertGreater(datetime.fromisoformat(
            after['sla']['state_time']), deadline)
        self.assertGreater(after['sla']['snapshot_calls'], 0)
        self.assertEqual(after['budget'], checkpoint['budget'])
        self.assertEqual(after['leases'], checkpoint['leases'])
        self.assertEqual(after['fills'], checkpoint['fills'])
        self.assertEqual(after['cash'], checkpoint['cash'])
        self.assertEqual(after['risk'], checkpoint['risk'])
        self.assertEqual(after['risk_db'], checkpoint['risk_db'])
        before_risk = after['risk_before']
        self.assertEqual(before_risk['day_start_equity'], '500')
        self.assertEqual(before_risk['units'], '500')
        self.assertEqual(before_risk['nav'], '0.9998')
        self.assertEqual(set(before_risk['executions']),
                         {'seed-DEMO-exec', 'seed-SPY-exec'})
        self.assertFalse(before_risk['daily_halted'])
        self.assertFalse(before_risk['project_halted'])
        self.assertGreaterEqual(after['audit'], checkpoint['audit'])
        after_protections = {row[0]: row for row in after['protections']}
        self.assertEqual(after_protections['DEMO'][1],
                         protections['DEMO'][1])
        self.assertEqual(after_protections['DEMO'][2], 'cancelled')
        self.assertEqual(after_protections['SPY'][1], protections['SPY'][1])
        self.assertEqual(after_protections['SPY'][2], 'confirmed')
        self.assertEqual(after['orders'], 5, after['protections'])
        self.assertEqual(after['positions'], {'DEMO': '1', 'SPY': '1'})

        con = sqlite3.connect(workspace / 'book.sqlite')
        try:
            actions = con.execute(
                'SELECT id,kind,state,trade_id,created_at FROM live_actions '
                'ORDER BY created_at,id').fetchall()
            self.assertEqual(
                [(row[0], row[1], row[3]) for row in
                 actions[:len(before_actions)]], before_actions)
            kinds = [row[1] for row in actions]
            self.assertEqual(kinds.count('entry'), 2)
            self.assertEqual(kinds.count('protect'), 2)
            self.assertEqual(kinds.count('cancel'), 1)
            self.assertEqual(kinds.count('exit'), 1)
            cancel = next(row for row in actions if row[1] == 'cancel')
            exit_action = next(row for row in actions if row[1] == 'exit')
            self.assertEqual(cancel[3], demo_id)
            self.assertEqual(exit_action[3], demo_id)
            self.assertEqual(cancel[4] < exit_action[4], True)
            self.assertIn(exit_action[2], {'REPORTED', 'OBSERVED'})
            deadline_events = []
            for (payload,) in con.execute(
                    'SELECT payload FROM audit_events').fetchall():
                event = json.loads(payload)
                if event.get('event_id', '').startswith(
                        'protection-deadline-'):
                    deadline_events.append(event)
            self.assertEqual(len(deadline_events), 1)
            self.assertTrue(any(
                'PROTECTION_WAIT_EXCEEDED:DEMO' in
                event.get('details', {}).get('issues', [])
                for event in deadline_events))
        finally:
            con.close()

    def test_external_broker_facts_envelope_fails_closed(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        source = Path(temp.name) / 'source'
        source.mkdir()
        child = self._spawn(source, 'seed', uuid.uuid4().hex,
                            source / 'initial.json')
        self._wait(source / 'barrier.json', child)
        os.kill(child.pid, signal.SIGKILL)
        child.wait(timeout=10)
        child.communicate(timeout=2)
        source_db = hashlib.sha256((source / 'book.sqlite').read_bytes()).hexdigest()
        source_risk = hashlib.sha256((source / 'risk.sqlite').read_bytes()).hexdigest()

        def mutate(folder, name):
            path = folder / 'broker.json'
            value = json.loads(path.read_text())
            value.pop(name)
            path.write_text(json.dumps(value, sort_keys=True) + '\n')

        cases = {
            'missing-file': lambda folder: (folder / 'broker.json').unlink(),
            'empty-file': lambda folder: (folder / 'broker.json').write_bytes(b''),
            'invalid-json': lambda folder: (folder / 'broker.json').write_text('{'),
            'missing-orders': lambda folder: mutate(folder, 'orders'),
            'missing-positions': lambda folder: mutate(folder, 'positions'),
            'missing-cash': lambda folder: mutate(folder, 'cash'),
            'missing-saved-at': lambda folder: mutate(folder, 'saved_at'),
        }
        expected = {
            'missing-file': 'BROKER_FACTS_MISSING',
            'empty-file': 'BROKER_FACTS_MISSING',
            'invalid-json': 'BROKER_FACTS_INVALID',
            'missing-orders': 'BROKER_FACTS_INCOMPLETE:orders',
            'missing-positions': 'BROKER_FACTS_INCOMPLETE:positions',
            'missing-cash': 'BROKER_FACTS_INCOMPLETE:cash',
            'missing-saved-at': 'BROKER_FACTS_INCOMPLETE:saved_at',
        }
        for label, mutation in cases.items():
            with self.subTest(label=label):
                broken = Path(temp.name) / label
                shutil.copytree(source, broken)
                mutation(broken)
                result = Path(temp.name) / (label + '.json')
                resumed = self._spawn(broken, 'resume', uuid.uuid4().hex,
                                      result)
                answer = self._wait(result, resumed)
                rc = resumed.wait(timeout=10)
                stdout, stderr = resumed.communicate(timeout=2)
                self.assertNotEqual(rc, 0, (label, stdout, stderr))
                self.assertEqual(answer['error'], expected[label])
                self.assertEqual(
                    hashlib.sha256(
                        (broken / 'book.sqlite').read_bytes()).hexdigest(),
                    source_db)
                self.assertEqual(
                    hashlib.sha256(
                        (broken / 'risk.sqlite').read_bytes()).hexdigest(),
                    source_risk)

    def test_default_composition_bad_facts_fail_closed_then_recover(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        source = Path(temp.name) / 'source'
        source.mkdir()
        child = self._spawn(source, 'seed', uuid.uuid4().hex,
                            source / 'initial.json')
        self._wait(source / 'barrier.json', child)
        os.kill(child.pid, signal.SIGKILL)
        child.wait(timeout=10)
        child.communicate(timeout=2)
        source_actions = sqlite3.connect(source / 'book.sqlite')
        try:
            action_rows = source_actions.execute(
                'SELECT id,kind,trade_id FROM live_actions ORDER BY id'
            ).fetchall()
        finally:
            source_actions.close()

        cases = {
            'stale': 'EXECUTION_REQUIRES_FRESH_COMPLETE_BOUND_ACCOUNT',
            'incomplete': 'EXECUTION_REQUIRES_FRESH_COMPLETE_BOUND_ACCOUNT',
            'transport': 'BROKER_READ_UNAVAILABLE',
        }
        for case, expected in cases.items():
            with self.subTest(case=case):
                broken = Path(temp.name) / case
                shutil.copytree(source, broken)
                result = Path(temp.name) / (case + '.json')
                probe = self._spawn_fact(broken, case, uuid.uuid4().hex,
                                         result)
                answer = self._wait(result, probe)
                rc = probe.wait(timeout=10)
                stdout, stderr = probe.communicate(timeout=2)
                self.assertEqual(rc, 0, (case, stdout, stderr))
                if case == 'transport':
                    self.assertEqual(answer['outcome']['errors'][0]['error'],
                                     expected)
                    self.assertEqual(answer['outcome']['errors'][1]['error'],
                                     'BROKER_SNAPSHOT_REQUIRED')
                else:
                    self.assertIn(expected, answer['outcome']['error'])
                self.assertEqual(answer['before']['budget'],
                                 answer['blocked']['budget'])
                self.assertEqual(answer['before']['budget'],
                                 answer['after']['budget'])
                self.assertEqual(answer['before']['leases'],
                                 answer['blocked']['leases'])
                self.assertEqual(answer['before']['fills'],
                                 answer['blocked']['fills'])
                self.assertEqual(answer['before']['fills'],
                                 answer['after']['fills'])
                self.assertEqual(answer['before']['orders'],
                                 answer['after']['orders'])
                self.assertEqual(answer['before']['protections'],
                                 answer['after']['protections'])
                self.assertEqual(
                    {trade.get('phase') for trade in
                     answer['before']['trades'].values()},
                    {trade.get('phase') for trade in
                     answer['after']['trades'].values()},
                    'recovery changed the trade phase')
                action_con = sqlite3.connect(broken / 'book.sqlite')
                try:
                    after_actions = action_con.execute(
                        'SELECT id,kind,trade_id FROM live_actions ORDER BY id'
                    ).fetchall()
                finally:
                    action_con.close()
                self.assertEqual(after_actions, action_rows)

        # The complete missing-history observation is accepted by production
        # input validation but must not be treated as flat or release budget.
        broken = Path(temp.name) / 'missing-history'
        shutil.copytree(source, broken)
        result = Path(temp.name) / 'missing-history.json'
        probe = self._spawn_fact(broken, 'missing-history',
                                 uuid.uuid4().hex, result)
        answer = self._wait(result, probe)
        rc = probe.wait(timeout=10)
        stdout, stderr = probe.communicate(timeout=2)
        self.assertEqual(rc, 0, (stdout, stderr))
        blocked = answer['blocked']
        self.assertEqual(blocked['trades'][
            next(iter(blocked['trades']))]['phase'], 'ENTRY_AWAITING_BROKER')
        self.assertEqual(answer['before']['budget'], blocked['budget'])
        self.assertEqual(answer['before']['budget'], answer['after']['budget'])
        self.assertEqual(answer['before']['fills'], answer['after']['fills'])
        self.assertTrue(answer['blocked']['risk']['blocked'])
        self.assertTrue(answer['blocked']['risk']['daily_halted'])
        self.assertTrue(answer['after']['risk']['blocked'])
        self.assertTrue(answer['after']['risk']['daily_halted'])
        con = sqlite3.connect(broken / 'book.sqlite')
        try:
            after_rows = con.execute(
                'SELECT id,kind,trade_id FROM live_actions ORDER BY id'
            ).fetchall()
        finally:
            con.close()
        self.assertEqual(after_rows, action_rows)

    def test_execution_identity_conflict_blocks_and_sticks_after_restart(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        workspace = Path(temp.name) / 'workspace'
        workspace.mkdir()
        token = uuid.uuid4().hex
        child = self._spawn(workspace, 'seed', token,
                            workspace / 'initial.json')
        self._wait(workspace / 'barrier.json', child)
        os.kill(child.pid, signal.SIGKILL)
        child.wait(timeout=10)
        child.communicate(timeout=2)
        db = workspace / 'book.sqlite'
        con = sqlite3.connect(db)
        try:
            before_actions = con.execute(
                'SELECT id,kind,state,trade_id FROM live_actions ORDER BY id'
            ).fetchall()
            before_audit = con.execute(
                'SELECT sequence,entry_hash FROM audit_events ORDER BY sequence'
            ).fetchall()
        finally:
            con.close()
        initial = json.loads((workspace / 'initial.json').read_text())
        self.assertEqual(initial['budget']['open_trades'], 2)

        conflict = self._spawn(workspace, 'identity', token + '-conflict',
                               workspace / 'identity.json')
        answer = self._wait(workspace / 'identity.json', conflict)
        rc = conflict.wait(timeout=10)
        stdout, stderr = conflict.communicate(timeout=2)
        self.assertEqual(rc, 0, (stdout, stderr))
        outcome = answer['outcome']
        self.assertIsNone(outcome['error'])
        self.assertFalse(outcome['reconciled'])
        self.assertTrue(outcome['new_entries_blocked'])
        self.assertFalse(outcome['management_dispatched'])
        self.assertFalse(outcome['management_continues'])
        self.assertEqual(outcome['integrity_faults_unresolved'], 1)
        self.assertIn('EXECUTION_INTEGRITY_FAULT_UNRESOLVED',
                      outcome['integrity_reasons'])
        self.assertTrue(any(
            error.get('reason') == 'RECEIPT_EXECUTION_IDENTITY_CONFLICT'
            and error.get('conflicts') == 1
            for error in outcome['errors']))
        before = answer['before']
        after = answer['after']
        self.assertEqual(after['entry_gate'], {
            'blocked': True, 'reason': 'EXECUTION_INTEGRITY_FAULT_UNRESOLVED'})
        self.assertEqual(after['integrity']['faults'][0]['execution_id'],
                         'seed-DEMO-exec')
        self.assertEqual(after['integrity']['faults'][0]['source'],
                         'execution_identity_conflict')
        self.assertEqual(after['integrity']['faults'][0]['state'],
                         'UNRESOLVED')
        self.assertEqual(before['budget'], after['budget'])
        self.assertEqual(before['leases'], after['leases'])
        self.assertEqual(before['fills'], after['fills'])
        self.assertEqual(before['accounting'], after['accounting'])
        self.assertEqual(before['refs'], after['refs'])
        self.assertEqual(before['fill_owners'], after['fill_owners'])
        self.assertEqual(before['actions'], after['actions'])
        self.assertEqual(before['cash'], after['cash'])
        self.assertEqual(before['risk'], after['risk'])
        con = sqlite3.connect(db)
        try:
            self.assertEqual(con.execute(
                'SELECT count(*) FROM execution_identity_conflict').fetchone()[0], 1)
            self.assertEqual(con.execute(
                'SELECT count(*) FROM execution_integrity_faults').fetchone()[0], 1)
            conflict_rows = con.execute(
                'SELECT accepted_instance_id,rejected_instance_id,'
                ' accepted_trade_id,rejected_trade_id '
                'FROM execution_identity_conflict').fetchall()
            self.assertEqual(conflict_rows[0][0], 'alpha')
            self.assertEqual(conflict_rows[0][1], 'beta')
            intents = {row[3]: row[2] for row in before['intents']}
            self.assertEqual(conflict_rows[0][0], 'alpha')
            self.assertEqual(conflict_rows[0][2], intents['DEMO'])
            self.assertEqual(conflict_rows[0][3], intents['SPY'])
            after_audit = con.execute(
                'SELECT sequence,entry_hash FROM audit_events ORDER BY sequence'
            ).fetchall()
            self.assertEqual(after_audit[:len(before_audit)], before_audit)
            trail = AuditTrail(connection=con)
            self.assertTrue(trail.verify()['integrity_valid'])
        finally:
            con.close()

        # A fresh process and clean snapshot must not clear the durable gate.
        clean = self._spawn(workspace, 'resume', token + '-clean',
                            workspace / 'clean.json')
        recovery = self._wait(workspace / 'clean.json', clean)
        rc = clean.wait(timeout=10)
        stdout, stderr = clean.communicate(timeout=2)
        self.assertEqual(rc, 0, (stdout, stderr))
        self.assertEqual({trade['phase'] for trade in
                          recovery['trades'].values()}, {'PROTECTED'})
        self.assertEqual(recovery['entry_gate'], {
            'blocked': True, 'reason': 'EXECUTION_INTEGRITY_FAULT_UNRESOLVED'})
        self.assertEqual(recovery['integrity']['faults'][0]['fault_id'],
                         after['integrity']['faults'][0]['fault_id'])
        self.assertEqual(recovery['budget'], before['budget'])
        self.assertEqual(recovery['leases'], before['leases'])
        self.assertEqual(recovery['fills'], before['fills'])
        self.assertEqual(recovery['accounting'], before['accounting'])
        self.assertEqual(recovery['actions'], before['actions'])
        con = sqlite3.connect(db)
        try:
            clean_audit = con.execute(
                'SELECT sequence,entry_hash FROM audit_events ORDER BY sequence'
            ).fetchall()
            self.assertEqual(clean_audit[:len(before_audit)], before_audit)
            trail = AuditTrail(connection=con)
            self.assertTrue(trail.verify()['integrity_valid'])
        finally:
            con.close()

    def test_partial_additional_fill_protects_actual_quantity_and_is_idempotent(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        workspace = Path(temp.name) / 'workspace'
        workspace.mkdir()
        token = uuid.uuid4().hex
        child = self._spawn(workspace, 'partial-seed', token,
                            workspace / 'initial.json')
        stage1 = self._wait(workspace / 'barrier.json', child)
        checkpoint = stage1['checkpoint']
        self.assertEqual(checkpoint['budget']['open_cash'], '120.00')
        self.assertEqual(checkpoint['budget']['open_notional'], '120.00')
        self.assertEqual(checkpoint['budget']['open_planned_loss'], '1.35')
        self.assertEqual(checkpoint['budget']['open_trades'], 2)
        self.assertEqual(checkpoint['cash'], '419.90')
        self.assertEqual(checkpoint['fills'], {'alpha': 1, 'beta': 1})
        self.assertEqual(checkpoint['accounting']['alpha']['fees'], '0.05')
        self.assertEqual(checkpoint['accounting']['beta']['fees'], '0.05')
        broker1 = json.loads((workspace / 'broker.json').read_text())
        demo_entry = next(order for order in broker1['orders']
                          if order['id'] == 'seed-DEMO')
        self.assertEqual(demo_entry['state'], 'partially_filled')
        self.assertEqual(demo_entry['quantity'], '2')
        self.assertEqual(demo_entry['cumulative_quantity'], '1')
        self.assertEqual(demo_entry['executions'][0]['id'], 'seed-DEMO-exec')
        self.assertEqual(broker1['positions'], {'DEMO': '1', 'SPY': '1'})
        stage1_protections = {row[0]: row for row in checkpoint['protections']}
        self.assertEqual(stage1_protections['DEMO'][2], 'confirmed')
        self.assertEqual(stage1_protections['DEMO'][4], '1')
        self.assertEqual(stage1_protections['SPY'][2], 'confirmed')
        self.assertEqual(stage1_protections['SPY'][4], '1')
        self.assertEqual(checkpoint['trades'][
            next(iter(checkpoint['trades']))]['phase'], 'PROTECTED')
        con = sqlite3.connect(workspace / 'book.sqlite')
        try:
            stage1_actions = con.execute(
                'SELECT id,kind,state,trade_id,created_at FROM live_actions '
                'ORDER BY created_at,id').fetchall()
            stage1_audit = con.execute(
                'SELECT sequence,entry_hash FROM audit_events ORDER BY sequence'
            ).fetchall()
        finally:
            con.close()

        os.kill(child.pid, signal.SIGKILL)
        child.wait(timeout=10)
        child.communicate(timeout=2)
        resumed = self._spawn(workspace, 'partial-resume', token + '-new',
                              workspace / 'resume.json')
        stage2 = self._wait(workspace / 'resume.json', resumed)
        rc = resumed.wait(timeout=10)
        stdout, stderr = resumed.communicate(timeout=2)
        self.assertEqual(rc, 0, (stdout, stderr))
        self.assertEqual(stage2['cash'], '379.85')
        self.assertEqual(stage2['equity'], '499.85')
        self.assertEqual(stage2['fills'], {'alpha': 2, 'beta': 1})
        self.assertEqual(stage2['accounting']['alpha']['fees'], '0.10')
        self.assertEqual(stage2['accounting']['alpha']['open_quantity'], '2')
        self.assertEqual(stage2['accounting']['beta']['fees'], '0.05')
        self.assertEqual(stage2['accounting']['beta']['open_quantity'], '1')
        self.assertEqual(stage2['budget'], checkpoint['budget'])
        self.assertEqual(stage2['leases'], checkpoint['leases'])
        self.assertFalse(stage2['risk']['blocked'])
        self.assertNotIn('UNRECONCILED_CASH_FLOW', stage2['risk']['reasons'])
        self.assertEqual(stage2['risk']['cash'], '379.85')
        self.assertEqual(stage2['risk']['equity'], '499.85')
        broker2 = json.loads((workspace / 'broker.json').read_text())
        demo2 = next(order for order in broker2['orders']
                     if order['id'] == 'seed-DEMO')
        self.assertEqual(len(demo2['executions']), 2)
        self.assertEqual(demo2['state'], 'filled')
        second_execution = demo2['executions'][1]
        self.assertEqual(second_execution['id'], 'seed-DEMO-exec-2')
        trace = stage2['partial_trace']
        self.assertTrue(trace)
        self.assertEqual(trace[0]['unprotected_since'],
                         second_execution['timestamp'])
        self.assertNotEqual(trace[0]['unprotected_since'],
                            trace[0]['first_fill_at'])
        self.assertEqual(trace[0]['quantity'], '2')
        protections = {}
        for row in stage2['protections']:
            protections.setdefault(row[0], []).append(row)
        demo_stops = sorted(protections['DEMO'], key=lambda row: row[2])
        self.assertEqual({row[2] for row in demo_stops},
                         {'cancelled', 'confirmed'})
        old_stop = next(row for row in demo_stops if row[2] == 'cancelled')
        new_stop = next(row for row in demo_stops if row[2] == 'confirmed')
        self.assertEqual(old_stop[4], '1')
        self.assertEqual(new_stop[4], '2')
        self.assertEqual(protections['SPY'][0][2], 'confirmed')
        self.assertEqual(protections['SPY'][0][4], '1')
        self.assertEqual(stage2['positions'], {'DEMO': '2', 'SPY': '1'})
        active_sell_qty = {'DEMO': 0, 'SPY': 0}
        for row in stage2['protections']:
            if row[2] in {'confirmed', 'partially_filled'}:
                active_sell_qty[row[0]] += int(row[4])
        self.assertEqual(active_sell_qty, {'DEMO': 2, 'SPY': 1})

        con = sqlite3.connect(workspace / 'book.sqlite')
        try:
            actions = con.execute(
                'SELECT id,kind,state,trade_id,created_at FROM live_actions '
                'ORDER BY created_at,id').fetchall()
            self.assertEqual(
                [(row[0], row[1], row[3]) for row in
                 actions[:len(stage1_actions)][:4]],
                [(row[0], row[1], row[3]) for row in stage1_actions[:4]])
            kinds = [row[1] for row in actions]
            self.assertEqual(kinds.count('entry'), 2)
            self.assertEqual(kinds.count('protect'), 3)
            self.assertEqual(kinds.count('cancel'), 2)
            cancels = [row for row in actions if row[1] == 'cancel']
            new_protect = next(
                row for row in actions
                if row[1] == 'protect' and row[0] not in
                {item[0] for item in stage1_actions})
            target = json.loads(con.execute(
                'SELECT payload FROM live_actions WHERE id=?',
                (cancels[-1][0],)).fetchone()[0])['target_order_id']
            self.assertEqual(target, old_stop[1])
            self.assertLess(cancels[-1][4], new_protect[4])
            self.assertFalse(any(
                row[1] == 'cancel' and 'SPY' in json.dumps(json.loads(
                    con.execute('SELECT payload FROM live_actions WHERE id=?',
                                (row[0],)).fetchone()[0]))
                for row in actions))
            final_audit = con.execute(
                'SELECT sequence,entry_hash FROM audit_events ORDER BY sequence'
            ).fetchall()
            self.assertEqual(final_audit[:len(stage1_audit)], stage1_audit)
            trail = AuditTrail(connection=con)
            self.assertTrue(trail.verify()['integrity_valid'])
        finally:
            con.close()

    def test_old_epoch_packet_rejected_late_receipt_history_and_new_management(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        workspace = Path(temp.name) / 'workspace'
        workspace.mkdir()
        token = uuid.uuid4().hex
        child = self._spawn(workspace, 'epoch-seed', token,
                            workspace / 'initial.json')
        stage1 = self._wait(workspace / 'barrier.json', child)
        checkpoint = stage1['checkpoint']
        epoch = json.loads((workspace / 'epoch.json').read_text())
        old_envelope = epoch['old_envelope']
        old_receipt = epoch['late_receipt']
        old_binding = old_envelope['binding']
        old_request = old_envelope['request_id']
        old_packet = old_envelope['packet']
        old_operation = old_packet['operation_id']
        old_action = old_packet['action_id']
        old_stop = old_receipt['response']['structuredContent']['data']['order']['id']
        now_before = datetime.fromisoformat(
            checkpoint['sla']['state_time'])
        self.assertLessEqual(datetime.fromisoformat(
            old_envelope['prepared_at']), now_before)
        self.assertLess(now_before, datetime.fromisoformat(
            old_envelope['expires_at']))
        self.assertFalse((workspace / 'output' / 'host-execution' /
                          'receipts' / (old_operation + '.json')).exists())
        self.assertFalse((workspace / 'output' / 'host-execution' /
                          'results' / (old_request + '.json')).exists())
        self.assertEqual(checkpoint['trades'][
            next(iter(checkpoint['trades']))]['phase'], 'PROTECTED')
        self.assertEqual(checkpoint['protections'][0][2], 'confirmed')
        old_demo_trade = old_packet.get('parameters', {}).get('symbol')
        self.assertEqual(old_demo_trade, 'DEMO')
        con = sqlite3.connect(workspace / 'book.sqlite')
        try:
            native_before = con.execute(
                'SELECT state FROM native_operation_records WHERE id=?',
                (old_operation,)).fetchone()
        finally:
            con.close()
        self.assertEqual(native_before, ('STARTED',))
        from martin_agentic.host_execution import validate_late_receipt_context
        con = sqlite3.connect(workspace / 'book.sqlite')
        try:
            action_payload = json.loads(con.execute(
                'SELECT payload FROM live_actions WHERE id=?',
                (old_action,)).fetchone()[0])
        finally:
            con.close()
        wrong_packet = copy.deepcopy(old_packet)
        wrong_packet['logical_ref_id'] = 'wrong-ref'
        with self.assertRaisesRegex(ValueError,
                                    'HOST_LATE_RECEIPT_REF_MISMATCH'):
            validate_late_receipt_context(
                action_payload, wrong_packet, old_receipt)
        wrong_receipt = copy.deepcopy(old_receipt)
        wrong_receipt['response']['structuredContent']['data'][
            'order']['ref_id'] = 'wrong-ref'
        with self.assertRaisesRegex(ValueError,
                                    'HOST_LATE_RECEIPT_ORDER_REF_MISMATCH'):
            validate_late_receipt_context(
                action_payload, old_packet, wrong_receipt)

        # Wrong late envelopes are refused by the production receipt writer
        # before any book mutation; use a copied Host directory so the original
        # operation cannot be contaminated.
        from martin_agentic.host_mailbox import submit_receipt

        def negative_copy(label):
            destination = Path(temp.name) / ('negative-' + label)
            shutil.copytree(workspace / 'output' / 'host-execution',
                            destination)
            return destination / 'packets' / (old_request + '.json')

        packet_path = negative_copy('operation')
        wrong = json.loads(packet_path.read_text())
        wrong['packet']['operation'] = 'cancel'
        wrong['packet_sha256'] = digest(wrong['packet'])
        packet_path.write_text(json.dumps(wrong, sort_keys=True) + '\n')
        with self.assertRaisesRegex(ValueError,
                                    'HOST_PREPARED_PACKET_INVALID'):
            submit_receipt(packet_path.parents[1], old_request,
                           old_receipt['response'])

        packet_path = negative_copy('hash')
        wrong = json.loads(packet_path.read_text())
        wrong['packet']['parameters']['price'] = '40.01'
        packet_path.write_text(json.dumps(wrong, sort_keys=True) + '\n')
        with self.assertRaisesRegex(ValueError,
                                    'HOST_PREPARED_PACKET_INVALID'):
            submit_receipt(packet_path.parents[1], old_request,
                           old_receipt['response'])

        os.kill(child.pid, signal.SIGKILL)
        child.wait(timeout=10)
        child.communicate(timeout=2)
        resumed = self._spawn(workspace, 'epoch-resume', token + '-new',
                              workspace / 'resume.json')
        stage2 = self._wait(workspace / 'resume.json', resumed)
        rc = resumed.wait(timeout=10)
        stdout, stderr = resumed.communicate(timeout=2)
        self.assertEqual(rc, 0, (stdout, stderr))
        self.assertEqual(stage2['validator']['status'],
                         'HOST_CONTROLLER_NOT_CURRENT_RECONCILE')
        self.assertLessEqual(datetime.fromisoformat(
            stage2['old_prepared_at']),
            datetime.fromisoformat(stage2['validator_now']))
        self.assertLess(datetime.fromisoformat(stage2['validator_now']),
                        datetime.fromisoformat(stage2['old_expires_at']))
        self.assertNotEqual(stage2['old_binding_epoch'],
                            stage2['new_binding_epoch'])
        late_results = stage2['late_results']
        old_late = next(item for item in late_results
                        if item.get('operation_id') == old_operation)
        self.assertEqual(old_late['status'], 'HOST_LATE_RECEIPT_IMPORTED')
        self.assertEqual(old_late['native_result']['order_id'], old_stop)
        self.assertTrue(stage2['port_receipt'].get('verified'),
                        stage2['port_receipt'])
        self.assertEqual(stage2['port_receipt'].get('status'),
                         'HOST_RECEIPT_IMPORTED')
        result_path = (workspace / 'output' / 'host-execution' / 'results' /
                       (old_request + '.json'))
        self.assertTrue(result_path.is_file())
        persisted = json.loads(result_path.read_text())
        self.assertEqual(persisted['status'], 'HOST_RECEIPT_IMPORTED')
        self.assertEqual(persisted['operation_id'], old_operation)
        self.assertEqual(persisted['action_id'], old_action)
        old_request_data = json.loads(
            (workspace / 'output' / 'host-execution' / 'requests' /
             (old_request + '.json')).read_text())
        self.assertEqual(persisted['trade_id'],
                         old_request_data['trade_id'])
        self.assertEqual(persisted['packet_sha256'], digest(old_packet))
        self.assertFalse(persisted['execution_authorized'])
        self.assertFalse(persisted['broker_effect_verified'])
        after = stage2['after']
        self.assertEqual(after['trades'][
            next(iter(after['trades']))]['phase'], 'PROTECTED')
        self.assertEqual(stage2['spy_exit'], True)
        protections = {row[0]: [] for row in after['protections']}
        for row in after['protections']:
            protections[row[0]].append(row)
        self.assertEqual(len(protections['DEMO']), 1)
        self.assertEqual(protections['DEMO'][0][1], old_stop)
        self.assertEqual(protections['DEMO'][0][2], 'confirmed')
        self.assertEqual(protections['SPY'][0][2], 'cancelled')
        self.assertEqual(after['fills'], {'alpha': 1, 'beta': 1})
        self.assertEqual(after['accounting']['alpha']['fees'], '0.05')
        self.assertEqual(after['accounting']['beta']['fees'], '0.05')
        self.assertEqual(after['budget']['open_trades'], 2)
        self.assertEqual(after['leases'], checkpoint['leases'])
        con = sqlite3.connect(workspace / 'book.sqlite')
        try:
            native_after = con.execute(
                'SELECT state FROM native_operation_records WHERE id=?',
                (old_operation,)).fetchone()
        finally:
            con.close()
        self.assertEqual(native_after, ('RECORDED',))
        self.assertFalse(after['risk']['blocked'])
        actions = after['actions']
        kinds = [row[1] for row in actions]
        self.assertEqual(kinds.count('entry'), 2)
        self.assertEqual(kinds.count('protect'), 2)
        self.assertEqual(kinds.count('cancel'), 1)
        self.assertEqual(kinds.count('exit'), 1)
        self.assertIn(old_action, {row[0] for row in actions})
        cancel_action = next(row for row in actions if row[1] == 'cancel')
        exit_action = next(row for row in actions if row[1] == 'exit')
        self.assertEqual(cancel_action[3], exit_action[3])
        packet_bindings = {}
        for packet_path in (workspace / 'output' / 'host-execution' /
                            'packets').glob('*.json'):
            envelope = json.loads(packet_path.read_text())
            packet_bindings[envelope['packet']['action_id']] = envelope
        self.assertEqual(packet_bindings[cancel_action[0]]['binding']['epoch'],
                         stage2['new_binding_epoch'])
        self.assertEqual(packet_bindings[cancel_action[0]]['packet'][
            'operation'], 'cancel')
        self.assertEqual(packet_bindings[exit_action[0]]['binding']['epoch'],
                         stage2['new_binding_epoch'])
        con = sqlite3.connect(workspace / 'book.sqlite')
        try:
            audit = con.execute(
                'SELECT sequence,entry_hash FROM audit_events ORDER BY sequence'
            ).fetchall()
            self.assertTrue(audit)
            trail = AuditTrail(connection=con)
            self.assertTrue(trail.verify()['integrity_valid'])
        finally:
            con.close()

    def test_default_late_receipt_fault_matrix_and_recovery(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        source = Path(temp.name) / 'source'
        source.mkdir()
        child = self._spawn(source, 'epoch-seed', uuid.uuid4().hex,
                            source / 'initial.json')
        self._wait(source / 'barrier.json', child)
        os.kill(child.pid, signal.SIGKILL)
        child.wait(timeout=10)
        child.communicate(timeout=2)
        epoch = json.loads((source / 'epoch.json').read_text())
        old_operation = epoch['old_envelope']['packet']['operation_id']
        for case in ('valid', 'hash', 'time', 'flags', 'json', 'gate',
                     'conflict'):
            with self.subTest(case=case):
                workspace = Path(temp.name) / case
                shutil.copytree(source, workspace)
                manifest_path = (workspace / 'output' / 'host-execution' /
                                 'manifest.json')
                manifest = json.loads(manifest_path.read_text())
                manifest['binding']['db'] = str(
                    (workspace / 'book.sqlite').resolve())
                manifest_path.write_text(json.dumps(
                    manifest, sort_keys=True) + '\n')
                result = Path(temp.name) / (case + '.json')
                probe = self._spawn_late(workspace, case,
                                         uuid.uuid4().hex, result)
                answer = self._wait(result, probe)
                rc = probe.wait(timeout=10)
                stdout, stderr = probe.communicate(timeout=2)
                self.assertEqual(rc, 0, (case, stdout, stderr))
                old_bad = [item for item in answer['bad_late_results']
                           if item.get('operation_id') == old_operation]
                old_conflict = [item for item in
                                answer['conflict_late_results']
                                if item.get('operation_id') == old_operation]
                old_good = [item for item in answer['late_results']
                            if item.get('operation_id') == old_operation
                            and item.get('status') ==
                            'HOST_LATE_RECEIPT_IMPORTED']
                if case == 'valid':
                    self.assertEqual(old_bad, [])
                    self.assertEqual(answer['native_state'], 'RECORDED')
                elif case == 'conflict':
                    self.assertTrue(old_conflict, (case, answer))
                    self.assertTrue(any(item.get('status') ==
                                        'HOST_LATE_RECEIPT_REJECTED'
                                        for item in old_conflict))
                    self.assertEqual(answer['native_state'], 'RECORDED')
                else:
                    self.assertTrue(old_bad, (case, answer))
                    self.assertTrue(any(item.get('status') ==
                                        'HOST_LATE_RECEIPT_REJECTED'
                                        for item in old_bad))
                    self.assertEqual(answer['native_state'], 'RECORDED')
                self.assertTrue(old_good, (case, answer))
                self.assertEqual(answer['spy_exit'], True)
                self.assertEqual(answer['after']['fills'],
                                 {'alpha': 1, 'beta': 1})
                self.assertEqual(answer['after']['budget']['open_trades'], 2)
                self.assertFalse(answer['after']['risk']['blocked'])

    def test_record_committed_result_publish_failure_recovers_on_restart(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        source = Path(temp.name) / 'source'
        source.mkdir()
        child = self._spawn(source, 'epoch-seed', uuid.uuid4().hex,
                            source / 'initial.json')
        self._wait(source / 'barrier.json', child)
        os.kill(child.pid, signal.SIGKILL)
        child.wait(timeout=10)
        child.communicate(timeout=2)
        workspace = Path(temp.name) / 'result-fail'
        shutil.copytree(source, workspace)
        manifest_path = (workspace / 'output' / 'host-execution' /
                         'manifest.json')
        manifest = json.loads(manifest_path.read_text())
        manifest['binding']['db'] = str((workspace / 'book.sqlite').resolve())
        manifest_path.write_text(json.dumps(manifest, sort_keys=True) + '\n')
        epoch = json.loads((source / 'epoch.json').read_text())
        old_operation = epoch['old_envelope']['packet']['operation_id']
        old_request = epoch['old_envelope']['request_id']
        result_path = (workspace / 'output' / 'host-execution' / 'results' /
                       (old_request + '.json'))
        failed = self._spawn_late(workspace, 'result-fail',
                                  uuid.uuid4().hex,
                                  Path(temp.name) / 'failed.json')
        answer = self._wait(Path(temp.name) / 'failed.json', failed)
        rc = failed.wait(timeout=10)
        stdout, stderr = failed.communicate(timeout=2)
        self.assertEqual(rc, 0, (stdout, stderr))
        self.assertEqual(answer['native_state'], 'RECORDED')
        self.assertFalse(result_path.exists())

        recovered = self._spawn_late(workspace, 'valid', uuid.uuid4().hex,
                                     Path(temp.name) / 'recovered.json')
        answer = self._wait(Path(temp.name) / 'recovered.json', recovered)
        rc = recovered.wait(timeout=10)
        stdout, stderr = recovered.communicate(timeout=2)
        self.assertEqual(rc, 0, (stdout, stderr))
        self.assertTrue(result_path.is_file())
        persisted = json.loads(result_path.read_text())
        self.assertEqual(persisted['status'], 'HOST_RECEIPT_IMPORTED')
        old_good = [item for item in answer['late_results']
                    if item.get('operation_id') == old_operation
                    and item.get('status') == 'HOST_LATE_RECEIPT_IMPORTED']
        self.assertTrue(old_good)
        self.assertEqual(answer['after']['fills'], {'alpha': 1, 'beta': 1})
        self.assertEqual(answer['after']['budget']['open_trades'], 2)
