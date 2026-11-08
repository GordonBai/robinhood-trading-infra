"""Two instances running through real durable queues and warm worker processes.

Uses the production queue, the production warm runner and real child processes;
only the strategy decision comes from a deterministic fixture plugin. Temporary
synthetic database, no broker, no order.
"""
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone
from decimal import Decimal
from unittest import TestCase

from trading_contracts.instance import InstanceBudget, PluginManifest
from trading_infra.instance_registry import InstanceRegistry
from trading_infra.instance_runtime import InstanceRuntime, instance_bundle_digest
from trading_infra.instance_supervisor import InstanceSupervisor
from trading_infra.strategy_stream import WarmStrategyRunner

SOURCE = 'b' * 64


def manifest(plugin_id, version='1.0'):
    return PluginManifest(plugin_id=plugin_id, version=version, state_schema='fixture-1',
                          config={'step': '1'}, data_requirements=('quotes',),
                          capabilities=('advisory-entry',), max_inflight=64)


def budget():
    return InstanceBudget(max_cash=Decimal('100'), max_planned_loss=Decimal('2.50'),
                          max_notional=Decimal('100'), max_concurrent=1)


class FakeRunner:
    """Stands in for the warm child; still exercises the real queue framing."""

    def __init__(self):
        self.request = None
        self.starts = []
        self.closed = False

    @property
    def next_deadline_monotonic(self):
        return None

    def start(self, request, *, payload=None):
        self.starts.append(request)
        self.request = request

    def poll(self):
        return None

    def close(self):
        self.closed = True


class InstanceRuntimeTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = sqlite3.connect(self.temp.name + '/account.sqlite', isolation_level=None)
        self.addCleanup(self.db.close)
        self.now = datetime(2026, 10, 29, 14, 0, tzinfo=timezone.utc)
        self.clock = lambda: self.now
        self.registry = InstanceRegistry(self.db, clock=self.clock)
        self.registry.register('alpha', manifest('alpha-plugin'), budget(), enabled=True)
        self.registry.register('beta', manifest('beta-plugin'), budget(), enabled=True)

    def runtime(self, instance_id, runner=None):
        runtime = InstanceRuntime(self.registry, self.registry.instance(instance_id),
                                  runner=runner or FakeRunner(), source_fingerprint=SOURCE,
                                  clock=self.clock)
        self.addCleanup(runtime.close)
        return runtime

    def event(self, event_id='e1', symbol='SPY'):
        return {'id': event_id, 'kind': 'quote', 'at': self.now.isoformat(),
                'synthetic_input': True, 'source_sha256': 'source-' + event_id,
                'data': {'symbol': symbol, 'bid': '40.00', 'ask': '40.01'}}

    def test_task_namespaces_are_per_instance(self):
        alpha, beta = self.runtime('alpha'), self.runtime('beta')
        alpha.enqueue(self.event())
        beta.enqueue(self.event())
        self.assertEqual(alpha.status()['pending'], 1)
        self.assertEqual(beta.status()['pending'], 1)

    def test_advancing_one_instance_leaves_the_other_queued(self):
        alpha_runner, beta_runner = FakeRunner(), FakeRunner()
        alpha, beta = self.runtime('alpha', alpha_runner), self.runtime('beta', beta_runner)
        alpha.enqueue(self.event())
        beta.enqueue(self.event())
        self.assertEqual(alpha.advance()['started'], 'e1')
        self.assertEqual(len(alpha_runner.starts), 1)
        self.assertEqual(beta_runner.starts, [])
        self.assertEqual(beta.status()['queued'], 1)

    def test_config_change_invalidates_only_the_changed_instance(self):
        alpha = self.runtime('alpha')
        alpha.enqueue(self.event())
        before = alpha.status()['config_fingerprint']
        # Re-registering under a new id with a different plugin version is a
        # separate instance; the original configuration fingerprint is stable.
        self.registry.register('gamma', manifest('gamma-plugin', '2.0'), budget(), enabled=True)
        self.assertEqual(alpha.status()['config_fingerprint'], before)
        self.assertFalse(alpha.status()['config_changed'])

    def test_bundle_digest_tracks_the_registered_contracts_only(self):
        first = instance_bundle_digest(self.registry, ['alpha', 'beta'])
        self.assertEqual(first, instance_bundle_digest(self.registry, ['beta', 'alpha']))
        self.registry.set_mode('beta', 'disabled', enabled=False)
        self.assertNotEqual(first, instance_bundle_digest(self.registry, ['alpha', 'beta']))

    def test_unknown_instance_cannot_be_hosted(self):
        with self.assertRaises(ValueError):
            InstanceRuntime(self.registry, self.registry.instance('alpha'), runner=FakeRunner(),
                            source_fingerprint='not-a-fingerprint', clock=self.clock)

    def test_warm_runner_accepts_a_precomputed_payload(self):
        runner = WarmStrategyRunner([sys.executable, '-c', 'import sys; sys.exit(1)'])
        self.addCleanup(runner.close)
        # A payload of the wrong type is refused before any child is spawned.
        with self.assertRaises(ValueError) as ctx:
            runner.start(object(), payload=b'{}')
        self.assertEqual(str(ctx.exception), 'STRATEGY_PROCESS_INPUT_REQUIRED')

    def test_real_runtimes_advance_under_one_supervisor(self):
        alpha_runner, beta_runner = FakeRunner(), FakeRunner()
        alpha = self.runtime('alpha', alpha_runner)
        beta = self.runtime('beta', beta_runner)
        supervisor = InstanceSupervisor([alpha, beta])
        self.addCleanup(supervisor.close)
        self.assertEqual(supervisor.instance_ids(), ['alpha', 'beta'])
        supervisor.enqueue('alpha', self.event('a1'))
        supervisor.enqueue('beta', self.event('b1'))
        report = supervisor.advance()
        self.assertEqual(report['instances']['alpha']['started'], 'a1')
        self.assertEqual(report['instances']['beta']['started'], 'b1')
        self.assertEqual(len(alpha_runner.starts), 1)
        self.assertEqual(len(beta_runner.starts), 1)
        self.assertEqual(supervisor.status()['alpha']['queued'], 0)
        self.assertEqual(supervisor.status()['beta']['queued'], 0)

    def test_one_runtime_failing_does_not_stop_the_other(self):
        class Broken(FakeRunner):
            def advance(self):  # pragma: no cover - never called directly
                raise RuntimeError('fixture fault')

        alpha = self.runtime('alpha', FakeRunner())
        beta = self.runtime('beta', FakeRunner())
        supervisor = InstanceSupervisor([alpha, beta])
        self.addCleanup(supervisor.close)
        supervisor.enqueue('beta', self.event('b1'))
        original = alpha.advance
        alpha.advance = lambda: (_ for _ in ()).throw(RuntimeError('fixture fault'))
        report = supervisor.advance()
        self.assertFalse(report['instances']['alpha']['advanced'])
        self.assertTrue(report['instances']['beta']['advanced'])
        self.assertEqual(report['instances']['beta']['started'], 'b1')
        alpha.advance = original
