"""Public bounded read preflight, budget, source and owned-process controls."""
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import time
from unittest import TestCase

from martin_agentic.bounded_read import (_kind, preflight, run_bounded_read,
                                         verify_completion)
from martin_agentic.bounded_read_worker import _completion_failures, _counted
from martin_agentic.transport import CodexBrokerReadTransport, TransportError
from martin_agentic.cli import main
from martin_agentic.qualification import source_fingerprint


def wait_dead(pid, seconds=1.0):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        time.sleep(0.02)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    return False


class BoundedReadTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.policy = self.root / 'policy.json'
        self.policy.write_text(json.dumps({'capture_only': True,
                                           'live_enabled': False,
                                           'scheduled': False,
                                           'account_suffix': '0000',
                                           'current_capture': {
                                               'root': str(self.root / 'capture'),
                                               'public_manifest': str(
                                                   self.root / 'public-manifest.json'),
                                               'retention': 3}}))
        protected_file = self.root / 'protected.dat'
        protected_file.write_text('synthetic-protected')
        self.protected = self.root / 'protected.json'
        self.protected.write_text(json.dumps({
            str(protected_file): hashlib.sha256(
                protected_file.read_bytes()).hexdigest()}))
        self.registry = self.root / 'registry.jsonl'
        self.registry.write_text('')
        self.target = self.root / 'run'

    def args(self, **changes):
        value = dict(policy_path=self.policy, target=self.target,
                     expected_source=source_fingerprint(),
                     protected_manifest=self.protected,
                     registry=self.registry, cycles=3, getter_budget=120,
                     wall_seconds=5)
        value.update(changes)
        return value

    def test_preflight_has_no_target_or_transport_side_effect(self):
        result = preflight(**self.args())
        self.assertEqual(result['status'], 'PREFLIGHT_OK')
        self.assertFalse(self.target.exists())
        self.assertFalse(result['transport_started'])
        self.assertEqual(result['getter_calls'], 0)
        with self.assertRaisesRegex(ValueError, 'BOUNDED_READ_TARGET_UNAVAILABLE'):
            preflight(**self.args(target=str(self.root / 'missing' / 'run')))

    def test_public_cli_preflight_returns_agent_json_without_target(self):
        import contextlib
        import io
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            rc = main(['bounded-read', '--policy', str(self.policy),
                       '--target', str(self.target),
                       '--expected-source', source_fingerprint(),
                       '--expected-protected', str(self.protected),
                       '--registry', str(self.registry)])
        self.assertEqual(rc, 0)
        value = json.loads(output.getvalue())
        self.assertEqual(value['status'], 'PREFLIGHT_OK')
        self.assertFalse(self.target.exists())

    def test_execute_requires_a_current_capture_bridge_without_side_effects(self):
        missing = self.root / 'missing-capture-policy.json'
        missing.write_text(json.dumps({'capture_only': True,
                                       'live_enabled': False,
                                       'scheduled': False,
                                       'account_suffix': '0000'}))
        from martin_agentic.bounded_read import preflight as preflight_fn
        public = preflight_fn(**self.args(policy_path=missing))
        self.assertEqual(public['status'], 'PREFLIGHT_OK')
        self.assertTrue(public['current_capture_required'] is False)
        self.assertFalse(self.target.exists())
        with self.assertRaisesRegex(
                ValueError, 'BOUNDED_READ_LIVE_CAPTURE_BRIDGE_NOT_CONFIGURED'):
            preflight_fn(**self.args(policy_path=missing,
                                     require_current_capture=True))
        target = self.root / 'run-missing-capture'
        with self.assertRaisesRegex(
                ValueError, 'BOUNDED_READ_LIVE_CAPTURE_BRIDGE_NOT_CONFIGURED'):
            run_bounded_read(**self.args(policy_path=missing, target=target,
                                         execute=True, symbols=['DEMO']))
        self.assertFalse(target.exists())

    def test_source_drift_and_budget_are_refused(self):
        with self.assertRaisesRegex(ValueError, 'BOUNDED_READ_SOURCE_MISMATCH'):
            preflight(**self.args(expected_source='0' * 64))
        with self.assertRaisesRegex(ValueError, 'BOUNDED_READ_CYCLES_INVALID'):
            preflight(**self.args(cycles=4))
        with self.assertRaisesRegex(ValueError, 'BOUNDED_READ_GETTER_BUDGET_INVALID'):
            preflight(**self.args(getter_budget=121))

    def test_existing_matching_process_is_refused(self):
        kind = _kind(self.target)
        self.registry.write_text(json.dumps({
            'pid': os.getpid(), 'sid': os.getsid(0), 'pgid': os.getpgid(0),
            'owner_pid': os.getpid(), 'kind': kind, 'started': 'fixture'}) + '\n')
        with self.assertRaisesRegex(ValueError, 'BOUNDED_READ_ALREADY_RUNNING'):
            preflight(**self.args())

    def _journal(self, run_id, cycles=3, getters=3):
        records = [{'event': 'run_started', 'run_id': run_id, 'cycles': cycles}]
        for cycle in range(1, cycles + 1):
            records.append({'event': 'cycle', 'run_id': run_id,
                            'cycle': cycle, 'completed': True,
                            'getter_calls': cycle})
        for sequence in range(1, getters + 1):
            records.append({'event': 'getter', 'run_id': run_id,
                            'sequence': sequence, 'tool': 'get_accounts'})
        return records

    def _summary(self, run_id, **changes):
        value = {'run_id': run_id, 'status': 'PASS', 'transport_started': True,
                 'getter_calls': 3, 'getter_budget': 120,
                 'configured_cycles': 3, 'completed_cycles': 3,
                 'completed_observation_cycles': 3,
                 'bounded_capture_completed': True,
                 'bounded_session_stopped': True,
                 'current_observation_healthy': True,
                 'read_only_capture_healthy': True,
                 'last_completed_observation_at': '2026-11-07T15:00:00+00:00',
                 'current_observation_evidence': {
                     'account_at': '2026-11-07T15:00:00+00:00',
                     'market_at': '2026-11-07T15:00:00+00:00',
                     'latest_observation_state': 'SUCCESS',
                     'latest_market_state': 'SUCCESS', 'cleanup_ok': True},
                 'entry_authorized': False, 'production_ready': False}
        value.update(changes)
        return value

    def _worker_script(self, summary, journal=None, target=None):
        target = Path(target or self.target)
        body = ['import json, os', 'run=os.environ["BOUNDED_READ_RUN_ID"]']
        if journal is not None:
            body.append('records=json.loads(%r)' % json.dumps(journal))
            body.append('for item in records: item["run_id"]=run')
            body.append('open(%r,"w").write("".join(json.dumps(i)+"\\n" '
                        'for i in records))' % str(target / 'cycles.jsonl'))
        body.append('value=json.loads(%r)' % json.dumps(summary))
        body.append('value["run_id"]=run')
        body.append('print(json.dumps(value))')
        return [sys.executable, '-c', '\n'.join(body)]

    def test_successful_child_json_is_passed_through(self):
        summary = self._summary('placeholder')
        command = [sys.executable, '-c',
                   ('import json, os\n'
                    'run=os.environ["BOUNDED_READ_RUN_ID"]\n'
                    'value=json.loads(%r)\n'
                    'value["run_id"]=run\n'
                    'records=json.loads(%r)\n'
                    'for item in records: item["run_id"]=run\n'
                    'open(%r,"w").write("".join(json.dumps(i)+"\\n" for i in records))\n'
                    'print(json.dumps(value))') % (
                       json.dumps(summary), json.dumps(self._journal('x')),
                       str(self.target / 'cycles.jsonl'))]
        result = run_bounded_read(**self.args(execute=True, symbols=['DEMO'],
                                              worker_command=command))
        self.assertEqual(result['status'], 'PASS', result)
        self.assertEqual(result['child']['getter_calls'], 3)
        self.assertEqual(result['verification']['completed_cycles'], 3)
        self.assertTrue((self.target / 'summary.json').is_file())
        self.assertTrue((self.target / 'claim.json').is_file())
        self.assertTrue((self.target / 'inputs.json').is_file())

    def test_atomic_claim_allows_exactly_one_executor(self):
        from unittest.mock import patch
        import martin_agentic.bounded_read as module
        original = module.preflight

        def racing_preflight(**kwargs):
            prepared = original(**kwargs)
            # The loser of a preflight race claims first.
            os.mkdir(self.target, 0o700)
            return prepared
        with patch.object(module, 'preflight', racing_preflight):
            with self.assertRaisesRegex(
                    ValueError, 'BOUNDED_READ_TARGET_ALREADY_CLAIMED'):
                run_bounded_read(**self.args(
                    execute=True, symbols=['DEMO'],
                    worker_command=[sys.executable, '-c', 'pass']))
        # preflight still refuses an existing target with no side effects
        with self.assertRaisesRegex(ValueError, 'BOUNDED_READ_TARGET_UNAVAILABLE'):
            preflight(**self.args())
        self.assertFalse((self.target / 'inputs.json').exists())

    def test_stale_summary_is_never_accepted(self):
        stale = self._summary('other-run')
        command = [sys.executable, '-c', 'print(%r)' % json.dumps(stale)]
        result = run_bounded_read(**self.args(execute=True, symbols=['DEMO'],
                                              worker_command=command))
        self.assertEqual(result['status'], 'FAILED')
        self.assertIn('CHILD_RUN_IDENTITY_MISMATCH', result['failure_codes'])
        self.assertIn('STALE_EVIDENCE', result['failure_codes'])
        self.assertFalse(result['verification']['evidence_not_stale'])

    def test_empty_result_zero_getters_and_missing_cycle_are_failed(self):
        for index, summary in enumerate(
                ({}, self._summary('x', getter_calls=0),
                 self._summary('x', completed_cycles=2,
                               bounded_capture_completed=False),
                 self._summary('x', completed_observation_cycles=2))):
            target = self.root / ('run-%d' % index)
            command = self._worker_script(summary, self._journal('x', cycles=2),
                                          target=target)
            result = run_bounded_read(**self.args(
                target=target, execute=True, symbols=['DEMO'],
                worker_command=command))
            self.assertEqual(result['status'], 'FAILED', summary)

    def test_journal_facts_survive_a_killed_child(self):
        code = ('import json, os\n'
                'run=os.environ["BOUNDED_READ_RUN_ID"]\n'
                'records=[{"event":"run_started","run_id":run},'
                '{"event":"cycle","run_id":run,"cycle":1,"completed":True},'
                '{"event":"getter","run_id":run,"sequence":1,"tool":"get_accounts"}]\n'
                'open(%r,"w").write("".join(json.dumps(i)+"\\n" for i in records))\n'
                'import time; time.sleep(30)\n') % str(self.target / 'cycles.jsonl')
        result = run_bounded_read(**self.args(execute=True, symbols=['DEMO'],
                                              wall_seconds=1.2,
                                              worker_command=[sys.executable, '-c', code]))
        self.assertEqual(result['status'], 'FAILED')
        self.assertEqual(result['verification']['completed_cycles'], 1)
        self.assertEqual(result['verification']['getter_reservations'], 1)
        self.assertEqual(result['cleanup_state'], 'KILL_ATTEMPTED')

    def test_parent_rejects_a_status_label_without_facts(self):
        status, checks, failures = verify_completion(
            run_id='run', cycles=3, getter_budget=10,
            child=self._summary('run', status='PASS', transport_started=False,
                                getter_calls=0, completed_observation_cycles=0),
            journal=[{'event': 'run_started', 'run_id': 'run'}],
            cleanup_state='CLEAN')
        self.assertEqual(status, 'FAILED')
        self.assertFalse(checks['child_transport_started'])
        self.assertIn('CHILD_TRANSPORT_STARTED', failures)

    def test_timeout_kills_owned_child_and_group(self):
        pidfile = self.root / 'child.pid'
        code = ('import json,os,subprocess,time\n'
                'p=subprocess.Popen([__import__("sys").executable,"-c",'
                '"import time; time.sleep(30)"])\n'
                'open(%r,"w").write("%%d %%d" %% (p.pid, os.getpgrp()))\n'
                'time.sleep(30)\n') % str(pidfile)
        command = [sys.executable, '-c', code]
        result = run_bounded_read(**self.args(execute=True, symbols=['DEMO'],
                                              wall_seconds=1.2,
                                              worker_command=command))
        self.assertEqual(result['status'], 'FAILED', result)
        self.assertEqual(result['cleanup_state'], 'KILL_ATTEMPTED')
        pid, pgid = map(int, pidfile.read_text().split())
        self.assertTrue(wait_dead(pid), 'owned child survived bounded read')
        try:
            os.killpg(pgid, 0)
        except ProcessLookupError:
            pass
        else:
            self.fail('owned process group survived bounded read')

    def test_getter_reservation_is_atomic_and_never_resets(self):
        import threading
        from martin_agentic.transport import TransportError
        class Base:
            def _read_call(self, tool, arguments):
                return tool
        state = {'count': 0, 'budget': 2, 'lock': threading.Lock(),
                 'journal_lock': threading.Lock(), 'guarded': False,
                 'guard': lambda: None, 'run_id': 'test',
                 'journal': str(self.root / 'cycles.jsonl'),
                 'transport_started': False}
        Counted = _counted(Base, state)
        transport = Counted()
        self.assertEqual(transport._read_call('a', {}), 'a')
        self.assertEqual(transport._read_call('b', {}), 'b')
        with self.assertRaisesRegex(TransportError,
                                    'READ_ONLY_GETTER_BUDGET_EXCEEDED'):
            transport._read_call('c', {})
        self.assertEqual(state['count'], 2)

    def test_nested_pagination_and_transports_share_one_counter_and_guard(self):
        import threading
        guard_calls = []

        class Base:
            def _read_call(self, tool, arguments):
                if tool == 'page-1':
                    return self._read_call('page-2', {})
                return tool
        state = {'count': 0, 'budget': 2, 'lock': threading.Lock(),
                 'journal_lock': threading.Lock(), 'guarded': False,
                 'guard': lambda: guard_calls.append(1), 'run_id': 'test',
                 'journal': str(self.root / 'cycles.jsonl'),
                 'transport_started': False}
        Counted = _counted(Base, state)
        first, second = Counted(), Counted()
        self.assertEqual(first._read_call('page-1', {}), 'page-2')
        self.assertEqual(state['count'], 2, 'both pagination calls reserved')
        self.assertEqual(guard_calls, [1], 'generation rechecked once before getter')
        with self.assertRaisesRegex(TransportError,
                                    'READ_ONLY_GETTER_BUDGET_EXCEEDED'):
            second._read_call('third', {})
        self.assertEqual(state['count'], 2)

    def test_guard_drift_rejects_before_any_dispatch(self):
        import threading

        class Base:
            def _read_call(self, tool, arguments):
                raise AssertionError('drift must reject before dispatch')
        state = {'count': 0, 'budget': 1, 'lock': threading.Lock(),
                 'journal_lock': threading.Lock(), 'guarded': False,
                 'guard': lambda: (_ for _ in ()).throw(
                     ValueError('BOUNDED_READ_SOURCE_MISMATCH')),
                 'run_id': 'test',
                 'journal': str(self.root / 'cycles.jsonl'),
                 'transport_started': False}
        with self.assertRaisesRegex(ValueError, 'BOUNDED_READ_SOURCE_MISMATCH'):
            _counted(Base, state)()._read_call('get_accounts', {})
        self.assertEqual(state['count'], 0)

    def test_real_transport_dispatch_reserves_budget_before_read(self):
        import threading
        from unittest.mock import patch
        class Rpc:
            last_request_id = 1
            def request(self, *args, **kwargs):
                raise AssertionError('stub must not dispatch')
            def close(self):
                pass
        state = {'count': 0, 'budget': 1, 'lock': threading.Lock(),
                 'journal_lock': threading.Lock(), 'guarded': False,
                 'guard': lambda: None, 'run_id': 'test',
                 'journal': str(self.root / 'cycles.jsonl'),
                 'transport_started': False}
        Counted = _counted(CodexBrokerReadTransport, state)
        transport = Counted(rpc=Rpc())
        transport.thread_id = 'thread'
        transport.tools = {'get_accounts'}
        with patch.object(CodexBrokerReadTransport, '_read_call',
                          return_value={'data': {}}):
            self.assertEqual(transport('get_accounts', {}),
                             {'data': {}})
            with self.assertRaisesRegex(TransportError,
                                        'READ_ONLY_GETTER_BUDGET_EXCEEDED'):
                transport('get_accounts', {})
        self.assertEqual(state['count'], 1)

    def test_nonzero_child_is_failed_without_retry(self):
        command = [sys.executable, '-c', 'raise SystemExit(7)']
        result = run_bounded_read(**self.args(execute=True, symbols=['DEMO'],
                                              worker_command=command))
        self.assertEqual(result['status'], 'FAILED')
        self.assertEqual(result['returncode'], 7)

    def test_registered_independent_session_child_is_killed_and_verified(self):
        pidfile = self.root / 'registered.pid'
        code = (
            'import subprocess, sys, time\n'
            'from martin_agentic.process_registry import spawn_registered\n'
            'child=spawn_registered([sys.executable,"-c","import time; time.sleep(30)"],'
            'kind="stdio_rpc",stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,'
            'stderr=subprocess.DEVNULL,start_new_session=True)\n'
            'open(%r,"w").write(str(child.pid))\n'
            'time.sleep(30)\n') % str(pidfile)
        result = run_bounded_read(**self.args(execute=True, symbols=['DEMO'],
                                              wall_seconds=2.5,
                                              worker_command=[sys.executable, '-c', code]))
        self.assertEqual(result['status'], 'FAILED')
        pid = int(pidfile.read_text())
        self.assertTrue(wait_dead(pid), 'independent registered child survived')
        self.assertEqual(result['resource_state'], 'CLEAN')

    def test_worker_input_policy_drift_fails_closed_before_transport(self):
        from martin_agentic import bounded_read_worker as worker
        target = self.root / 'worker-run'
        target.mkdir(mode=0o700)
        inputs = target / 'inputs.json'
        inputs.write_text(json.dumps({
            'run_id': 'run-1', 'cycles': 3, 'getter_budget': 10,
            'source_fingerprint': source_fingerprint(),
            'source_pin_digest': '0' * 64,
            'policy_b64': 'e30=',
            'policy_sha256': '0' * 64,
            'protected': {}}))
        import contextlib
        import io
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            rc = worker.main(['--input', str(inputs), '--target', str(target),
                              '--run-id', 'run-1', '--symbols', 'DEMO',
                              '--cycles', '3', '--getter-budget', '10'])
        summary = json.loads((target / 'summary.json').read_text())
        self.assertEqual(rc, 1)
        self.assertEqual(summary['status'], 'FAILED')
        self.assertFalse(summary['transport_started'])
        self.assertEqual(summary['getter_calls'], 0)
        self.assertIn(summary['error'], {
            'BOUNDED_READ_POLICY_DIGEST_MISMATCH',
            'BOUNDED_READ_SOURCE_PIN_MISMATCH'})

    def test_worker_rejects_a_different_run_identity(self):
        from martin_agentic import bounded_read_worker as worker
        target = self.root / 'worker-run-2'
        target.mkdir(mode=0o700)
        inputs = target / 'inputs.json'
        inputs.write_text(json.dumps({'run_id': 'other', 'cycles': 3,
                                      'getter_budget': 10}))
        import contextlib
        import io
        with contextlib.redirect_stdout(io.StringIO()):
            rc = worker.main(['--input', str(inputs), '--target', str(target),
                              '--run-id', 'run-1', '--symbols', 'DEMO',
                              '--cycles', '3', '--getter-budget', '10'])
        summary = json.loads((target / 'summary.json').read_text())
        self.assertEqual(rc, 1)
        self.assertEqual(summary['error'], 'BOUNDED_READ_RUN_IDENTITY_MISMATCH')
        self.assertFalse(summary['transport_started'])
