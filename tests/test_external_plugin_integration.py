"""Installed external template: conformance, identity gates and a Book cycle."""
import json
from unittest import TestCase

from external_environment import ExternalEnvironment


CONFORMANCE_SCRIPT = r'''
import json
from trading_infra.plugin_conformance import run_conformance
positive = run_conformance('example-momentum',
                           {'lookback': 3, 'threshold_bps': 0}, timeout_seconds=5)
nondeterministic = run_conformance('test-nondeterministic', {}, timeout_seconds=5)
timeout = run_conformance('test-probe-slow', {'create_sleep': 2}, timeout_seconds=0.2)
print(json.dumps({'positive': {'passed': positive.get('passed'),
                                   'checks': positive.get('checks'),
                                   'errors': positive.get('errors')},
                  'nondeterministic': {
                      'passed': nondeterministic.get('passed'),
                      'errors': nondeterministic.get('errors'),
                      'checks': nondeterministic.get('checks')},
                  'timeout': {'passed': timeout.get('passed'),
                              'checks': timeout.get('checks'),
                              'errors': timeout.get('errors')}},
                 sort_keys=True))
'''

WORKER_SCRIPT = r'''
import json
import os
import struct
import subprocess
import sys
from datetime import datetime, timezone

from trading_contracts.codec import encode_input
from trading_contracts.instance import config_version
from trading_contracts.strategy import StrategyIdentity, StrategyInput
from trading_infra.exit_advisory import _resolve
from trading_infra.external_application import ExternalCatalog

out = {}
catalog = ExternalCatalog(('example-momentum',))
config = catalog.config_for('example-momentum', '0.05',
                            {'lookback': 3, 'threshold_bps': 0})
command = catalog.worker_command(
    'example-momentum', sys.executable, cost_buffer_per_share='0.05',
    source_fingerprint='a' * 64, config=config)


def run(command, data=b''):
    result = subprocess.run(command, input=data, capture_output=True, timeout=30)
    return {'returncode': result.returncode,
            'stderr': result.stderr.decode('utf-8', 'replace').strip(),
            'stdout_bytes': len(result.stdout)}


bad_identity = list(command)
bad_identity[bad_identity.index('--plugin-digest') + 1] = '0' * 64
out['identity'] = run(bad_identity)
out['identity_ok_no_input'] = run(command)

at = datetime(2026, 10, 16, 14, 0, tzinfo=timezone.utc)
wrong_config = {'lookback': 4, 'threshold_bps': 0,
                'cost_buffer_per_share': '0.05'}
identity = catalog.contract('example-momentum')['plugin_identity']
request = StrategyInput(
    identity=StrategyIdentity('external-test', 'example-momentum', '1.0.0'),
    as_of=at, cursor='worker-1', data_snapshot_id='worker-snapshot-1',
    source_sha256='a' * 64,
    config_version=config_version('example-momentum', '1.0.0',
                                  wrong_config, identity),
    synthetic=True,
    market={'event': {'id': 'worker-1', 'kind': 'quote', 'at': at.isoformat(),
                      'synthetic_input': True, 'source_sha256': 'a' * 64,
                      'data': {'symbol': 'DEMO', 'bid': '42.00',
                               'ask': '42.01', 'bid_at': at.isoformat(),
                               'ask_at': at.isoformat(),
                               'observed_at': at.isoformat()}}},
    checkpoint={})
encoded = encode_input(request)
out['config_mismatch'] = run(command,
                             struct.pack('!I', len(encoded)) + encoded)
try:
    _resolve('trading_example_momentum.rules:momentum_exit@' + '0' * 64)
    out['exit_change'] = {'ok': True}
except ValueError as exc:
    out['exit_change'] = {'ok': False, 'error': str(exc)}
print(json.dumps(out, sort_keys=True))
'''

BOOK_SCRIPT = r'''
import json
import sys

ROOT = %(root)r
sys.path.insert(0, ROOT + '/tests')

from martin_agentic.live_session import run_live_session as generic
from trading_infra.external_application import external_composition
import test_default_lifecycle as dl


class RisingMarket(dl.MarketPort):
    def __call__(self, *args, **kwargs):
        self.calls += 1
        at = self.clock().isoformat()
        bid = '42.00' if self.calls < 10 else '43.00'
        ask = '42.01' if bid == '42.00' else '43.01'
        return ([{'id': 'e-' + str(self.calls), 'kind': 'quote', 'at': at,
                  'synthetic_input': True, 'source_sha256': 'm-' + str(self.calls),
                  'data': {'symbol': 'DEMO', 'bid': bid, 'ask': ask,
                           'bid_at': at, 'ask_at': at, 'observed_at': at}}],
                {'quotes': 1})


def main():
    dl.MarketPort = RisingMarket
    fixture = dl.DefaultLifecycleTests(
        'test_default_runner_reaches_authoritative_flat')
    fixture.setUp()
    try:
        def external_run(*args, **kwargs):
            kwargs['composition'] = external_composition(
                ['example-momentum', 'example-mean-reversion'])
            return generic(*args, **kwargs)
        dl.run_live_session = external_run
        result, state, con, actions, trades, trace = fixture.run_default(
            cycles=120, market_exit_after=10,
            strategy_instances=[
                {'instance_id': 'mom', 'plugin': 'example-momentum',
                 'config': {'lookback': 3, 'threshold_bps': 0}},
                {'instance_id': 'mean', 'plugin': 'example-mean-reversion',
                 'config': {'lookback': 5, 'threshold_bps': 0}}])
        print(json.dumps({
            'healthy': result.get('healthy'),
            'reason': result.get('reason'),
            'errors': result.get('errors'),
            'actions': [(str(row[0]), str(row[1])) for row in actions],
            'trades': {key: {'phase': value.get('phase'),
                             'quantity': str(value.get('quantity')),
                             'exit_requested': value.get('exit_requested')}
                       for key, value in trades.items()},
            'position': state.position_qty,
            'cash': str(state.fake.cash),
            'instances': [row[0] for row in con.execute(
                'SELECT instance_id FROM strategy_instances')],
            'owners': [row[0] for row in con.execute(
                'SELECT instance_id FROM execution_attribution')],
            'state_rows': con.execute(
                'SELECT instance_id, state_key, payload '
                'FROM strategy_instance_state').fetchall(),
        }, sort_keys=True, default=str))
    finally:
        fixture.doCleanups()


if __name__ == '__main__':
    main()
'''



FEEDBACK_SCRIPT = r"""
import json
import sys

ROOT = %(root)r
sys.path.insert(0, ROOT + '/tests')

from martin_agentic.live_session import run_live_session as generic
from trading_infra.external_application import external_composition
import test_default_lifecycle as dl


class CycleMarket(dl.MarketPort):
    def __call__(self, *args, **kwargs):
        self.calls += 1
        at = self.clock().isoformat()
        index = (self.calls - 1) %% 4
        bid = '42.00' if index < 2 else '43.00'
        ask = '42.01' if index < 2 else '43.01'
        return ([{'id': 'feedback-' + str(self.calls), 'kind': 'quote', 'at': at,
                  'synthetic_input': True,
                  'source_sha256': 'feedback-' + str(self.calls),
                  'data': {'symbol': 'DEMO', 'bid': bid, 'ask': ask,
                           'bid_at': at, 'ask_at': at, 'observed_at': at}}],
                {'quotes': 1})


def main():
    dl.MarketPort = CycleMarket
    fixture = dl.DefaultLifecycleTests(
        'test_default_runner_reaches_authoritative_flat')
    fixture.setUp()
    try:
        def external_run(*args, **kwargs):
            kwargs['composition'] = external_composition(
                ['example-mean-reversion'])
            return generic(*args, **kwargs)
        dl.run_live_session = external_run
        result, state, con, actions, trades, trace = fixture.run_default(
            cycles=160, market_exit_after=10,
            strategy_instances=[{'instance_id': 'feedback',
                                 'plugin': 'example-mean-reversion',
                                 'config': {'lookback': 3, 'threshold_bps': 0}}])
        print(json.dumps({
            'healthy': result.get('healthy'),
            'errors': result.get('errors'),
            'actions': [(str(row[0]), str(row[1])) for row in actions],
            'trades': {str(key): {'phase': value.get('phase'),
                                  'quantity': str(value.get('quantity'))}
                       for key, value in trades.items()},
            'position': str(state.position_qty),
            'owners': [row[0] for row in con.execute(
                'SELECT instance_id FROM execution_attribution')],
        }, sort_keys=True, default=str))
    finally:
        fixture.doCleanups()


if __name__ == '__main__':
    main()
"""

class ExternalInstalledPluginTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.environment = ExternalEnvironment.shared()
        cls.conformance = cls.environment.run_script(CONFORMANCE_SCRIPT)
        cls.worker = cls.environment.run_script(WORKER_SCRIPT)
        cls.book = cls.environment.run_script(
            BOOK_SCRIPT % {'root': str(cls.environment.root)}, timeout=180)
        cls.feedback = cls.environment.run_script(
            FEEDBACK_SCRIPT % {'root': str(cls.environment.root)}, timeout=180)

    def test_nondeterministic_plugin_fails_conformance(self):
        positive = self.conformance['positive']
        self.assertTrue(positive['passed'], positive)
        self.assertTrue(all(item.get('passed') for item in positive['checks']),
                        positive)
        nondeterministic = self.conformance['nondeterministic']
        self.assertFalse(nondeterministic['passed'], nondeterministic)
        self.assertIn('PLUGIN_DECISION_NOT_DETERMINISTIC',
                      nondeterministic['errors'] or [], nondeterministic)
        timeout = self.conformance['timeout']
        self.assertFalse(timeout['passed'], timeout)
        self.assertIn('PLUGIN_PROBE_TIMEOUT',
                      [item.get('error') for item in timeout['checks']],
                      timeout)

    def test_worker_rejects_identity_and_config_changes(self):
        self.assertNotEqual(self.worker['identity']['returncode'], 0,
                            self.worker)
        self.assertEqual(self.worker['identity_ok_no_input']['returncode'], 0,
                         self.worker['identity_ok_no_input'])
        self.assertNotEqual(self.worker['config_mismatch']['returncode'], 0,
                            self.worker)
        self.assertFalse(self.worker['exit_change']['ok'], self.worker)

    def test_installed_template_reenters_after_confirmed_flat(self):
        report = self.feedback
        self.assertTrue(report['healthy'], report)
        self.assertEqual(report['errors'], [], report)
        self.assertGreaterEqual(len(report['trades']), 2, report)
        for trade_id, trade in report['trades'].items():
            self.assertEqual(trade['phase'], 'FLAT_VERIFIED',
                             (trade_id, trade, report))
            self.assertEqual(trade['quantity'], '0', (trade_id, report))
        self.assertEqual(str(report['position']), '0', report)
        self.assertEqual(set(report['owners']), {'feedback'}, report)
        self.assertTrue({'entry', 'protect', 'cancel', 'exit'}.issubset(
            {row[0] for row in report['actions']}), report)

    def test_installed_template_closes_book_and_owns_two_instances(self):
        report = self.book
        self.assertTrue(report['healthy'], report)
        self.assertEqual(report['errors'], [], report)
        self.assertEqual({row[0] for row in report['actions']},
                         {'entry', 'protect', 'cancel', 'exit'}, report)
        self.assertEqual({row[1] for row in report['actions']}, {'OBSERVED'},
                         report)
        self.assertTrue(report['trades'], report)
        for trade_id, trade in report['trades'].items():
            self.assertEqual(trade['phase'], 'FLAT_VERIFIED',
                             (trade_id, trade, report))
            self.assertEqual(trade['quantity'], '0', (trade_id, report))
        self.assertEqual(int(report['position']), 0, report)
        self.assertEqual(sorted(report['instances']), ['mean', 'mom'], report)
        self.assertEqual(sorted(set(report['owners'])), ['mean', 'mom'], report)
        rows = {row[0]: row[2] for row in report['state_rows']}
        self.assertEqual(set(rows), {'mean', 'mom'}, report)
        self.assertIn('entry_anchor', rows['mean'], report)
        self.assertIn('close_ma', rows['mom'], report)
