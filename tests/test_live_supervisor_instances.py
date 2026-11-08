"""The control supervisor can advance a multi-instance loop instead of one queue.

Default behaviour must not change: with no instance supervisor it advances the
single legacy queue exactly as before. Both paths are checked here because the
default daemon path is the one that must never silently switch.
"""
import sqlite3
import tempfile
from datetime import datetime, timezone
from decimal import Decimal
from unittest import TestCase

from martin_luk.live_session import LiveSupervisor
from trading_contracts.instance import InstanceBudget, PluginManifest
from trading_infra.instance_registry import InstanceRegistry
from trading_infra.instance_runtime import InstanceRuntime
from trading_infra.instance_supervisor import InstanceSupervisor

SOURCE = 'c' * 64
NOW = datetime(2026, 10, 29, 14, 0, tzinfo=timezone.utc)


class Owner:
    """Minimal owner-shaped object: the supervisor only needs these attributes."""

    def __init__(self, db, clock):
        self.db = db
        self.clock = clock
        self.simulation = True
        self.policy = {}

    def _begin(self):
        self.db.execute('BEGIN IMMEDIATE')


class FakeRunner:
    def __init__(self):
        self.started = []

    @property
    def next_deadline_monotonic(self):
        return None

    def start(self, request, *, payload=None):
        self.started.append(request)

    def poll(self):
        return None

    def close(self):
        pass


def manifest(plugin_id):
    return PluginManifest(plugin_id=plugin_id, version='1.0', state_schema='fixture-1',
                          config={'step': '1'}, data_requirements=('quotes',),
                          capabilities=('advisory-entry',), max_inflight=64)


def budget():
    return InstanceBudget(max_cash=Decimal('100'), max_planned_loss=Decimal('2.50'),
                          max_notional=Decimal('100'), max_concurrent=1)


class SupervisorInstanceTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = sqlite3.connect(self.temp.name + '/live.sqlite', isolation_level=None)
        self.addCleanup(self.db.close)
        self.registry = InstanceRegistry(self.db, clock=lambda: NOW)
        self.registry.register('alpha', manifest('alpha-plugin'), budget(), enabled=True)
        self.registry.register('beta', manifest('beta-plugin'), budget(), enabled=True)
        self.runtimes = []
        for instance_id in ('alpha', 'beta'):
            runtime = InstanceRuntime(self.registry, self.registry.instance(instance_id),
                                      runner=FakeRunner(), source_fingerprint=SOURCE,
                                      clock=lambda: NOW)
            self.addCleanup(runtime.close)
            self.runtimes.append(runtime)

    def event(self, event_id='e1'):
        return {'id': event_id, 'kind': 'quote', 'at': NOW.isoformat(),
                'synthetic_input': True, 'source_sha256': 'source-' + event_id,
                'data': {'symbol': 'SPY', 'bid': '40.00', 'ask': '40.01'}}

    def owner(self):
        return Owner(self.db, lambda: NOW)

    def supervisor(self, **overrides):
        fields = dict(agent_mode='review', owner_source=SOURCE)
        fields.update(overrides)
        return LiveSupervisor(self.owner(), process_strategy=False, **fields)

    def test_default_path_reports_disabled_without_an_instance_loop(self):
        supervisor = self.supervisor()
        self.assertFalse(supervisor.instance_supervisor_enabled)
        self.assertIsNone(supervisor.instance_supervisor)
        self.assertEqual(supervisor.advance_strategy(), {'enabled': False, 'errors': []})

    def test_instance_loop_advances_every_instance_once(self):
        loop = InstanceSupervisor(self.runtimes)
        supervisor = LiveSupervisor(self.owner(), process_strategy=False, agent_mode='review',
                                    owner_source=SOURCE, instance_supervisor=loop)
        loop.enqueue('alpha', self.event('a1'))
        loop.enqueue('beta', self.event('b1'))
        progress = supervisor.advance_strategy()
        self.assertTrue(progress['enabled'])
        self.assertEqual(sorted(progress['instances']), ['alpha', 'beta'])
        self.assertEqual(progress['instances']['alpha']['started'], 'a1')
        self.assertEqual(progress['instances']['beta']['started'], 'b1')
        self.assertEqual(progress['errors'], [])

    def test_a_faulting_instance_is_surfaced_without_hiding_the_other(self):
        loop = InstanceSupervisor(self.runtimes)
        supervisor = LiveSupervisor(self.owner(), process_strategy=False, agent_mode='review',
                                    owner_source=SOURCE, instance_supervisor=loop)
        loop.enqueue('beta', self.event('b1'))
        self.runtimes[0].advance = lambda: (_ for _ in ()).throw(RuntimeError('fixture fault'))
        progress = supervisor.advance_strategy()
        self.assertEqual(progress['errors'][0]['instance_id'], 'alpha')
        self.assertEqual(progress['errors'][0]['scope'], 'STRATEGY_INSTANCE')
        self.assertEqual(progress['instances']['beta']['started'], 'b1')
        self.assertGreaterEqual(progress['undelivered_error_count'], 1)

    def test_errors_are_bounded(self):
        loop = InstanceSupervisor(self.runtimes)
        supervisor = LiveSupervisor(self.owner(), process_strategy=False, agent_mode='review',
                                    owner_source=SOURCE, instance_supervisor=loop)
        self.runtimes[0].advance = lambda: (_ for _ in ()).throw(RuntimeError('fixture fault'))
        for _ in range(300):
            supervisor.advance_strategy()
        self.assertLessEqual(len(supervisor._strategy_pending_errors), 256)

    def test_the_step_path_routes_events_to_every_instance(self):
        loop = InstanceSupervisor(self.runtimes)
        supervisor = LiveSupervisor(self.owner(), process_strategy=False, agent_mode='review',
                                    owner_source=SOURCE, instance_supervisor=loop)
        # Reconciliation and final dispatch are covered elsewhere; this test is
        # about where the events go, so only that stage runs for real.
        supervisor.book.reconcile = lambda *args, **kwargs: None
        supervisor.book.actions = lambda: []
        supervisor._dispatch = lambda *args, **kwargs: None
        supervisor._finish_step = lambda *args, **kwargs: {'reconciled': True}
        snapshot = {'account_key': 'acct', 'observed_at': NOW.isoformat(), 'orders': {}}
        result = supervisor._step_instances(snapshot, [self.event('step-1')],
                                            allow_entries=False, allow_dispatch=False)
        self.assertEqual(sorted(result['strategy_queue']['instances']), ['alpha', 'beta'])
        self.assertEqual(loop.status()['alpha']['queued'], 0)
        self.assertEqual(loop.status()['beta']['queued'], 0)
        self.assertEqual(loop.status()['alpha']['running'], 1)
        self.assertEqual(loop.status()['beta']['running'], 1)

    def test_the_same_event_is_offered_to_both_instances(self):
        loop = InstanceSupervisor(self.runtimes)
        supervisor = LiveSupervisor(self.owner(), process_strategy=False, agent_mode='review',
                                    owner_source=SOURCE, instance_supervisor=loop)
        supervisor.book.reconcile = lambda *args, **kwargs: None
        supervisor.book.actions = lambda: []
        supervisor._dispatch = lambda *args, **kwargs: None
        supervisor._finish_step = lambda *args, **kwargs: {'reconciled': True}
        snapshot = {'account_key': 'acct', 'observed_at': NOW.isoformat(), 'orders': {}}
        supervisor._step_instances(snapshot, [self.event('shared-1')],
                                   allow_entries=False, allow_dispatch=False)
        # Each instance owns its own task namespace for the same market event.
        for instance_id in ('alpha', 'beta'):
            row = self.db.execute(
                'SELECT state FROM strategy_tasks WHERE instance_id=? AND event_id=?',
                (instance_id, 'shared-1')).fetchone()
            self.assertIsNotNone(row, instance_id)
            self.assertEqual(row[0], 'RUNNING')

    def test_a_stale_event_is_refused_by_each_instance(self):
        loop = InstanceSupervisor(self.runtimes)
        supervisor = LiveSupervisor(self.owner(), process_strategy=False, agent_mode='review',
                                    owner_source=SOURCE, instance_supervisor=loop)
        supervisor.book.reconcile = lambda *args, **kwargs: None
        supervisor.book.actions = lambda: []
        supervisor._dispatch = lambda *args, **kwargs: None
        supervisor._finish_step = lambda *args, **kwargs: {'reconciled': True}
        stale = self.event('stale-1')
        stale['at'] = '2026-10-29T13:00:00+00:00'
        snapshot = {'account_key': 'acct', 'observed_at': NOW.isoformat(), 'orders': {}}
        result = supervisor._step_instances(snapshot, [stale], allow_entries=False,
                                            allow_dispatch=False)
        queue_errors = result['strategy_queue'].get('errors') or []
        per_instance = [item for item in queue_errors if item.get('scope') == 'STRATEGY_INSTANCE']
        # A stale event is refused by every instance rather than filed under one.
        self.assertEqual({item['instance_id'] for item in per_instance}, {'alpha', 'beta'})
        self.assertTrue(all(item['reason'] == 'STALE_STRATEGY_EVENT'
                            for item in per_instance))
        self.assertEqual(loop.status()['alpha']['queued'], 0)

    def test_entries_blocked_without_any_error_still_reports_blocked(self):
        loop = InstanceSupervisor(self.runtimes)
        supervisor = LiveSupervisor(self.owner(), process_strategy=False,
                                    agent_mode='review', owner_source=SOURCE,
                                    instance_supervisor=loop)
        supervisor.book.reconcile = lambda *a, **k: None
        supervisor.book.actions = lambda: []
        supervisor._dispatch = lambda *a, **k: None
        supervisor._finish_step = lambda *a, **k: {'reconciled': True}
        snapshot = {'account_key': 'acct', 'observed_at': NOW.isoformat(), 'orders': {}}
        result = supervisor._step_instances(snapshot, [], allow_entries=False,
                                            allow_dispatch=False)
        # The stub for _finish_step does not surface errors; the point here is
        # that the gate is decided by the blocking reasons, not by an error list.
        self.assertTrue(result['new_entries_blocked'],
                        'a held session is blocked even with no errors')
        self.assertIn('ENTRIES_NOT_PERMITTED_BY_CALLER',
                      result['new_entries_blockers'])

    def test_capture_only_is_a_blocker_in_its_own_right(self):
        loop = InstanceSupervisor(self.runtimes)
        supervisor = LiveSupervisor(self.owner(), process_strategy=False,
                                    agent_mode='review', owner_source=SOURCE,
                                    instance_supervisor=loop)
        supervisor.book.reconcile = lambda *a, **k: None
        supervisor.book.actions = lambda: []
        supervisor._dispatch = lambda *a, **k: None
        supervisor._finish_step = lambda *a, **k: {'reconciled': True}
        supervisor.book.policy = {'capture_only': True}
        snapshot = {'account_key': 'acct', 'observed_at': NOW.isoformat(), 'orders': {}}
        result = supervisor._step_instances(snapshot, [], allow_entries=True,
                                            allow_dispatch=False)
        self.assertTrue(result['new_entries_blocked'])
        self.assertIn('CAPTURE_ONLY', result['new_entries_blockers'])
