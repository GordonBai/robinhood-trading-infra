"""Bounded external-plugin probe: output caps, deadlines and process cleanup."""
import json
from unittest import TestCase

from external_environment import ExternalEnvironment


SCRIPT = r'''
import json
import os
import subprocess
import tempfile
import time
from datetime import datetime, timezone

from trading_contracts.instance import config_version
from trading_contracts.strategy import StrategyIdentity, StrategyInput
from trading_infra.plugin_probe import probe

folder = tempfile.mkdtemp(prefix='probe-owner-')
out = {}


def owner_state(path):
    if not os.path.exists(path):
        return {'pid': None, 'pgrp': None, 'alive': False, 'zombie': False}
    pid, pgrp = map(int, open(path).read().split())
    alive = True
    zombie = False
    for _ in range(40):
        try:
            os.kill(pid, 0)
            alive = True
        except ProcessLookupError:
            alive = False
        try:
            stat = subprocess.run(['ps', '-o', 'stat=', '-p', str(pid)],
                                  capture_output=True, text=True, timeout=5).stdout
            zombie = stat.strip().startswith('Z')
        except Exception:
            zombie = False
        if not alive or zombie:
            break
        time.sleep(0.025)
    return {'pid': pid, 'pgrp': pgrp, 'alive': alive, 'zombie': zombie}


def make_request():
    at = datetime(2026, 10, 16, 14, 0, tzinfo=timezone.utc)
    config = {'decide_sleep': 2}
    return StrategyInput(
        identity=StrategyIdentity('probe-test', 'test-probe-slow', '1.0.0'),
        as_of=at, cursor='probe-1', data_snapshot_id='probe-snapshot-1',
        source_sha256='a' * 64,
        config_version=config_version('test-probe-slow', '1.0.0', config),
        synthetic=True,
        market={'event': {'id': 'probe-1', 'kind': 'quote',
                          'at': at.isoformat(), 'synthetic_input': True,
                          'source_sha256': 'a' * 64,
                          'data': {'symbol': 'DEMO', 'bid': '40.00',
                                   'ask': '40.01', 'bid_at': at.isoformat(),
                                   'ask_at': at.isoformat(),
                                   'observed_at': at.isoformat()}}},
        checkpoint={})


out['overflow'] = probe('test-probe-noisy', {}, 'describe')
out['bad_json'] = probe('test-probe-badjson', {}, 'create')

leak_pid = os.path.join(folder, 'leak.pid')
out['leak'] = probe('test-probe-leaky',
                    {'spawn_child': True, 'pid_file': leak_pid}, 'create')
out['leak_owner'] = owner_state(leak_pid)

create_pid = os.path.join(folder, 'create.pid')
out['create_timeout'] = probe(
    'test-probe-slow', {'create_sleep': 2, 'pid_file': create_pid},
    'create', timeout_seconds=0.2)
out['create_owner'] = owner_state(create_pid)

decide_pid = os.path.join(folder, 'decide.pid')
out['decide_timeout'] = probe(
    'test-probe-slow', {'decide_sleep': 2, 'pid_file': decide_pid},
    'decide', request=make_request(), timeout_seconds=0.2)
out['decide_owner'] = owner_state(decide_pid)

exit_pid = os.path.join(folder, 'exit.pid')
out['exit_timeout'] = probe(
    'test-probe-slow', {}, 'exit',
    view={'sleep_seconds': 2, 'pid_file': exit_pid}, timeout_seconds=0.2)
out['exit_owner'] = owner_state(exit_pid)

print(json.dumps(out, sort_keys=True))
'''


class BoundedProbeTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.environment = ExternalEnvironment.shared()
        cls.result = cls.environment.run_script(SCRIPT)

    def setUp(self):
        self.result = self.__class__.result

    def owner_dead(self, name):
        state = self.result[name]
        self.assertFalse(state['alive'] and not state['zombie'], state)
        try:
            import os
            os.killpg(state['pgrp'], 0)
        except ProcessLookupError:
            return
        except PermissionError:
            return
        self.fail('process group survived probe: %s' % (state,))

    def test_output_over_and_bad_json_fail_structurally(self):
        self.assertEqual(self.result['overflow'],
                         {'ok': False, 'error': 'PLUGIN_PROBE_OUTPUT_LIMIT'})
        self.assertEqual(self.result['bad_json'],
                         {'ok': False,
                          'error': 'PLUGIN_CONFORMANCE_OUTPUT_INVALID'})

    def test_successful_return_with_child_is_a_leak_and_is_reaped(self):
        self.assertEqual(self.result['leak'],
                         {'ok': False, 'error': 'PLUGIN_PROBE_PROCESS_LEAK'})
        self.assertIsNotNone(self.result['leak_owner']['pid'])
        self.owner_dead('leak_owner')

    def test_create_decide_exit_timeouts_reap_the_owned_group(self):
        for name in ('create_timeout', 'decide_timeout', 'exit_timeout'):
            with self.subTest(name=name):
                self.assertEqual(self.result[name],
                                 {'ok': False,
                                  'error': 'PLUGIN_PROBE_TIMEOUT'})
        for name in ('create_owner', 'decide_owner', 'exit_owner'):
            with self.subTest(name=name):
                self.assertIsNotNone(self.result[name]['pid'])
                self.owner_dead(name)
