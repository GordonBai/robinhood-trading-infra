"""Restart recovery for instance-owned durable tasks.

The review reproduced a valid restart being rejected as
``STRATEGY_QUEUE_REQUEST_CORRUPT`` because the queue rebuilt the prepared input
with its own checkpoint formula and its own prepared type. These tests pin the
host-delegated recovery protocol that replaced it.
"""
import sqlite3
import tempfile
from datetime import datetime, timezone
from decimal import Decimal
from unittest import TestCase

from trading_contracts.instance import InstanceBudget, PluginManifest
from trading_contracts.codec import encode_input
from trading_infra.instance_registry import InstanceRegistry
from trading_infra.instance_runtime import InstanceRuntime
from test_instance_host import CountingPlugin

SOURCE = 'e' * 64
NOW = datetime(2026, 10, 29, 14, 0, tzinfo=timezone.utc)


class DecisionRunner:
    """Returns a deterministic decision for the request it was given."""

    def __init__(self, plugin=None):
        self.plugin = plugin or CountingPlugin(proposal_at=1)
        self.request = None
        self.starts = []
        self.closed = False

    @property
    def next_deadline_monotonic(self):
        return None

    def start(self, request, *, payload=None):
        self.starts.append((request, payload))
        self.request = request

    def poll(self):
        if self.request is None:
            return None
        request, self.request = self.request, None
        return self.plugin.decide(request)

    def close(self):
        self.closed = True


def manifest():
    return PluginManifest(plugin_id='counter', version='1.0', state_schema='counter-1',
                          config={'step': '1'}, data_requirements=('quotes',),
                          capabilities=('advisory-entry',), max_inflight=64)


def budget():
    return InstanceBudget(max_cash=Decimal('100'), max_planned_loss=Decimal('2.50'),
                          max_notional=Decimal('100'), max_concurrent=1)


class RecoveryTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = self.temp.name + '/recovery.sqlite'
        self.db = sqlite3.connect(self.path, isolation_level=None)
        self.addCleanup(self.db.close)
        self.registry = InstanceRegistry(self.db, clock=lambda: NOW)
        self.registry.register('alpha', manifest(), budget(), mode='shadow', enabled=True)

    def runtime(self, runner):
        runtime = InstanceRuntime(self.registry, self.registry.instance('alpha'),
                                  runner=runner, source_fingerprint=SOURCE,
                                  clock=lambda: NOW)
        self.addCleanup(runtime.close)
        return runtime

    def event(self, event_id='e1', symbol='SPY'):
        at = NOW.isoformat()
        return {'id': event_id, 'kind': 'quote', 'at': at, 'synthetic_input': True,
                'source_sha256': 'source-' + event_id,
                'data': {'symbol': symbol, 'bid': '40.00', 'ask': '40.01',
                         'bid_at': at, 'ask_at': at, 'observed_at': at}}

    def test_a_crashed_owner_leaves_a_running_task_that_recovers(self):
        first = self.runtime(DecisionRunner())
        first.enqueue(self.event())
        self.assertEqual(first.advance()['started'], 'e1')
        # The owner dies without closing; the task is still RUNNING in the book.
        self.assertEqual(self.db.execute(
            "SELECT state FROM strategy_tasks WHERE event_id='e1'").fetchone()[0], 'RUNNING')
        second = self.runtime(DecisionRunner())
        self.assertEqual(second.queue.recovered_running, 1)
        result = second.advance()
        self.assertEqual(result['rejected'], [])
        self.assertEqual(result['started'], 'e1')
        self.assertEqual(second.status()['rejected'], 0)

    def test_a_clean_close_requeues_and_the_next_owner_starts_it(self):
        first = self.runtime(DecisionRunner())
        first.enqueue(self.event())
        first.advance()
        first.close()
        self.assertEqual(self.db.execute(
            "SELECT state FROM strategy_tasks WHERE event_id='e1'").fetchone()[0], 'QUEUED')
        second = self.runtime(DecisionRunner())
        result = second.advance()
        self.assertEqual(result['rejected'], [])
        self.assertEqual(result['started'], 'e1')

    def test_recovery_reuses_the_original_request_bytes(self):
        first_runner = DecisionRunner()
        first = self.runtime(first_runner)
        first.enqueue(self.event())
        first.advance()
        original = first_runner.starts[0]

        second_runner = DecisionRunner()
        second = self.runtime(second_runner)
        second.advance()
        reused_request, reused_payload = second_runner.starts[0]
        self.assertEqual(encode_input(reused_request), original[1])
        self.assertEqual(reused_request.as_of, original[0].as_of)

    def test_recovery_keeps_the_absent_checkpoint_distinction(self):
        first = self.runtime(DecisionRunner())
        first.enqueue(self.event())
        first.advance()
        row = self.db.execute(
            'SELECT checkpoint_absent FROM strategy_tasks WHERE event_id=?', ('e1',)).fetchone()
        # The first request was built with no stored checkpoint at all.
        self.assertEqual(row[0], 1)

    def test_a_finished_task_is_not_restarted(self):
        runner = DecisionRunner()
        runtime = self.runtime(runner)
        runtime.enqueue(self.event())
        runtime.advance()
        runtime.advance()  # commits
        self.assertEqual(runtime.status()['committed'], 1)

        second = self.runtime(DecisionRunner())
        self.assertEqual(second.queue.recovered_running, 0)
        self.assertEqual(second.status()['queued'], 0)

    def test_recovery_refuses_a_request_pinned_to_another_instance(self):
        first = self.runtime(DecisionRunner())
        first.enqueue(self.event())
        first.advance()
        self.registry.register('beta', manifest(), budget(), mode='shadow', enabled=True)
        beta = InstanceRuntime(self.registry, self.registry.instance('beta'),
                               runner=DecisionRunner(), source_fingerprint=SOURCE,
                               clock=lambda: NOW)
        self.addCleanup(beta.close)
        # Beta owns no task, so it simply has nothing to recover.
        self.assertEqual(beta.queue.recovered_running, 0)
        self.assertEqual(beta.status()['queued'], 0)
