"""A revised bar must be refused by the instance owner, not silently stored.

Two different failure modes matter and they are not the same:

* the same event id with different content — an identity conflict;
* a different event id describing the same bar with different economics — a
  revision of an already-consumed observation.

The second is what the previous frontier update silently overwrote. These tests
use the real host, a temporary registry and a trivial fixture plugin, so nothing
depends on a strategy that happens to remember only its latest bar.
"""
import sqlite3
import tempfile
from datetime import datetime, timezone
from decimal import Decimal
from unittest import TestCase

from trading_contracts.instance import InstanceBudget, PluginManifest
from trading_contracts.market import bar_observation_identity
from trading_contracts.strategy import StrategyDecision
from trading_infra.instance_host import InstanceStrategyHost
from trading_infra.instance_registry import InstanceRegistry

NOW = datetime(2026, 10, 30, 14, 0, tzinfo=timezone.utc)


class CounterPlugin:
    def __init__(self):
        self.seen = []

    def decide(self, request):
        self.seen.append(request.cursor)
        return StrategyDecision(identity=request.identity,
                                decision_id='d-' + request.cursor,
                                as_of=request.as_of,
                                data_snapshot_id=request.data_snapshot_id,
                                consumed_cursor=request.cursor,
                                synthetic=request.synthetic,
                                next_checkpoint={'count': len(self.seen)})


def manifest():
    return PluginManifest(plugin_id='counter', version='1.0', state_schema='counter-1',
                          config={'step': '1', 'cost_buffer_per_share': '0.05'},
                          data_requirements=('quotes',),
                          capabilities=('advisory-entry',), max_inflight=8)


def budget():
    return InstanceBudget(max_cash=Decimal('100'), max_planned_loss=Decimal('2.50'),
                          max_notional=Decimal('100'), max_concurrent=1)


def bar_event(event_id, *, close='40.00', begins='2026-10-30T13:30:00+00:00',
              symbol='DEMO'):
    at = NOW.isoformat()
    return {'id': event_id, 'kind': 'bar', 'at': at, 'synthetic_input': True,
            'source_sha256': 'source-' + event_id,
            'data': {'symbol': symbol, 'begins_at': begins, 'available_at': begins,
                     'observed_at': at, 'interval': '5minute', 'bounds': 'regular',
                     'adjustment': 'split', 'open': '40', 'high': '40.1',
                     'low': '39.9', 'close': close, 'volume': 10000}}


class RevisionTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = sqlite3.connect(self.temp.name + '/rev.sqlite', isolation_level=None)
        self.addCleanup(self.db.close)
        self.registry = InstanceRegistry(self.db, clock=lambda: NOW)
        self.registry.register('alpha', manifest(), budget(), mode='shadow', enabled=True)
        self.host = InstanceStrategyHost(self.registry, self.registry.instance('alpha'),
                                         clock=lambda: NOW)
        self.plugin = CounterPlugin()

    def consume(self, event):
        prepared = self.host.prepare(event)
        self.assertIsNotNone(prepared)
        return self.host.commit(prepared, self.plugin.decide(prepared.request))

    def test_a_different_event_id_describing_a_revised_bar_is_refused(self):
        self.consume(bar_event('bar-1', close='40.00'))
        key, fingerprint = bar_observation_identity(bar_event('bar-1')['data'])
        self.assertEqual(self.host.observation(key), fingerprint)

        revised = bar_event('bar-2', close='41.00')
        with self.assertRaises(ValueError) as ctx:
            self.host.prepare(revised)
        self.assertEqual(str(ctx.exception), 'CONSUMED_BAR_REVISED')

    def test_a_refused_revision_leaves_every_piece_of_state_untouched(self):
        self.consume(bar_event('bar-1', close='40.00'))
        before_state = self.host.state()
        key, fingerprint = bar_observation_identity(bar_event('bar-1')['data'])
        before_frontier = self.host.observations()
        before_consumed = self.db.execute(
            'SELECT count(*) FROM strategy_instance_consumed').fetchone()[0]
        before_outbox = self.db.execute(
            "SELECT count(*) FROM strategy_instance_proposals").fetchone()[0]

        with self.assertRaises(ValueError):
            self.consume(bar_event('bar-2', close='41.00'))

        self.assertEqual(self.host.state(), before_state)
        self.assertEqual(self.host.observations(), before_frontier)
        self.assertEqual(self.host.observation(key), fingerprint)
        self.assertEqual(self.db.execute(
            'SELECT count(*) FROM strategy_instance_consumed').fetchone()[0],
            before_consumed)
        self.assertEqual(self.db.execute(
            "SELECT count(*) FROM strategy_instance_proposals").fetchone()[0],
            before_outbox)

    def test_an_identical_repeat_is_still_idempotent(self):
        self.consume(bar_event('bar-1'))
        prepared = self.host.prepare(bar_event('bar-1'))
        self.assertIsNone(prepared, 'a consumed identity is not recomputed')

    def test_the_same_event_id_with_different_content_is_an_identity_conflict(self):
        self.consume(bar_event('bar-1', close='40.00'))
        with self.assertRaises(ValueError) as ctx:
            self.host.prepare(bar_event('bar-1', close='41.00'))
        self.assertEqual(str(ctx.exception), 'STRATEGY_EVENT_ID_CONFLICT')

    def test_the_commit_boundary_rechecks_before_any_write(self):
        prepared = self.host.prepare(bar_event('bar-1', close='40.00'))
        decision = self.plugin.decide(prepared.request)
        # Another owner consumed a different revision of the same bar in the
        # meantime; the final commit must notice rather than overwrite.
        key, _ = bar_observation_identity(bar_event('bar-1')['data'])
        self.host.record_observation(key, 'a-different-fingerprint', at=NOW)
        with self.assertRaises(ValueError) as ctx:
            self.host.commit(prepared, decision)
        self.assertEqual(str(ctx.exception), 'CONSUMED_BAR_REVISED')
        self.assertEqual(self.host.observations()[key], 'a-different-fingerprint')

    def test_a_revision_of_an_older_bar_is_refused_too(self):
        for index in range(4):
            self.consume(bar_event('bar-' + str(index),
                                   begins='2026-10-30T13:{}0:00+00:00'.format(index)))
        # Revise the first bar, not the most recent one.
        with self.assertRaises(ValueError) as ctx:
            self.host.prepare(bar_event('bar-new', close='42.00',
                                        begins='2026-10-30T13:00:00+00:00'))
        self.assertEqual(str(ctx.exception), 'CONSUMED_BAR_REVISED')


class ControlPathRevisionTests(TestCase):
    """The rejection must reach the control loop's integrity path, not vanish."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = sqlite3.connect(self.temp.name + '/ctl.sqlite', isolation_level=None)
        self.addCleanup(self.db.close)
        self.registry = InstanceRegistry(self.db, clock=lambda: NOW)
        self.registry.register('alpha', manifest(), budget(), mode='shadow', enabled=True)
        from trading_infra.instance_runtime import InstanceRuntime
        self.runtime = InstanceRuntime(self.registry,
                                       self.registry.instance('alpha'),
                                       runner=ImmediateRunner(),
                                       source_fingerprint='f' * 64, clock=lambda: NOW)
        self.addCleanup(self.runtime.close)

    def drain(self, event):
        self.runtime.enqueue(event)
        reports = []
        for _ in range(6):
            report = self.runtime.advance()
            reports.append(report)
            if not report['pending'] and not report['busy']:
                break
        return reports

    def test_the_control_path_surfaces_the_revision(self):
        self.drain(bar_event('bar-1', close='40.00'))
        reports = self.drain(bar_event('bar-2', close='41.00'))
        reasons = [item['reason'] for report in reports for item in report['errors']]
        rejected = [item['reason'] for report in reports
                    for item in report['rejected']]
        self.assertIn('CONSUMED_BAR_REVISED', reasons + rejected)
        status = self.runtime.status()
        self.assertEqual(status['committed'], 1, 'only the original bar committed')
        key, fingerprint = bar_observation_identity(bar_event('bar-1')['data'])
        self.assertEqual(self.runtime.host.observation(key), fingerprint)


class ImmediateRunner:
    """Decides as soon as it is polled; the queue still frames the request."""

    def __init__(self):
        self.request = None
        self.plugin = CounterPlugin()

    @property
    def next_deadline_monotonic(self):
        return None

    def start(self, request, *, payload=None):
        self.request = request

    def poll(self):
        if self.request is None:
            return None
        request, self.request = self.request, None
        return self.plugin.decide(request)

    def close(self):
        pass


class LegacyFrontierTests(TestCase):
    """A checkpoint written before the index existed still records consumption."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = sqlite3.connect(self.temp.name + '/legacy.sqlite', isolation_level=None)
        self.addCleanup(self.db.close)
        self.registry = InstanceRegistry(self.db, clock=lambda: NOW)
        self.registry.register('alpha', manifest(), budget(), mode='shadow', enabled=True)
        self.host = InstanceStrategyHost(self.registry, self.registry.instance('alpha'),
                                         clock=lambda: NOW)
        self.plugin = CounterPlugin()

    def legacy_checkpoint(self, observations):
        self.registry.put_state('alpha', {'bar_observations': observations,
                                          'seen': {'SPY': 1}}, expected=None)

    def key_and_fingerprint(self, *, close='40.00', begins='2026-10-30T13:30:00+00:00'):
        return bar_observation_identity(
            bar_event('x', close=close, begins=begins)['data'])

    def test_a_revision_of_a_checkpoint_only_bar_is_refused(self):
        key, fingerprint = self.key_and_fingerprint()
        self.legacy_checkpoint({key: fingerprint})
        # The new index has no row for this key at all.
        self.assertEqual(self.db.execute(
            'SELECT count(*) FROM strategy_instance_observations').fetchone()[0], 0)
        with self.assertRaises(ValueError) as ctx:
            self.host.prepare(bar_event('bar-2', close='41.00'))
        self.assertEqual(str(ctx.exception), 'CONSUMED_BAR_REVISED')

    def test_the_checkpoint_is_untouched_after_a_refused_legacy_revision(self):
        key, fingerprint = self.key_and_fingerprint()
        self.legacy_checkpoint({key: fingerprint})
        before = self.host.state()
        with self.assertRaises(ValueError):
            self.host.prepare(bar_event('bar-2', close='41.00'))
        self.assertEqual(self.host.state(), before)
        self.assertEqual(self.host.observations(), {key: fingerprint})
        self.assertEqual(self.db.execute(
            'SELECT count(*) FROM strategy_instance_consumed').fetchone()[0], 0)

    def test_an_older_checkpoint_bar_cannot_be_revised_either(self):
        old_key, old_fingerprint = self.key_and_fingerprint(
            begins='2026-10-30T13:00:00+00:00')
        recent_key, recent_fingerprint = self.key_and_fingerprint(
            begins='2026-10-30T13:30:00+00:00')
        self.legacy_checkpoint({old_key: old_fingerprint,
                                recent_key: recent_fingerprint})
        with self.assertRaises(ValueError) as ctx:
            self.host.prepare(bar_event('bar-new', close='42.00',
                                        begins='2026-10-30T13:00:00+00:00'))
        self.assertEqual(str(ctx.exception), 'CONSUMED_BAR_REVISED')

    def test_the_same_content_migrates_into_the_index_without_complaint(self):
        key, fingerprint = self.key_and_fingerprint()
        self.legacy_checkpoint({key: fingerprint})
        self.consume(bar_event('bar-1', close='40.00'))
        self.assertEqual(self.host.observation(key), fingerprint)
        self.assertEqual(self.db.execute(
            'SELECT count(*) FROM strategy_instance_observations').fetchone()[0], 1)

    def consume(self, event):
        prepared = self.host.prepare(event)
        self.assertIsNotNone(prepared)
        return self.host.commit(prepared, self.plugin.decide(prepared.request))

    def test_two_sources_disagreeing_about_one_key_fail_closed(self):
        import sqlite3 as _sqlite
        with tempfile.TemporaryDirectory() as directory:
            db = _sqlite.connect(directory + '/mixed.sqlite', isolation_level=None)
            self.addCleanup(db.close)
            registry = InstanceRegistry(db, clock=lambda: NOW)
            registry.register('alpha', manifest(), budget(), mode='shadow', enabled=True)
            host = InstanceStrategyHost(registry, registry.instance('alpha'),
                                        clock=lambda: NOW)
            key, fingerprint = self.key_and_fingerprint()
            registry.put_state('alpha', {'bar_observations': {key: fingerprint}},
                               expected=None)
            # Written straight to the index, i.e. a genuinely inconsistent
            # pair of sources rather than one the guard would have prevented.
            db.execute('INSERT INTO strategy_instance_observations(instance_id,'
                       ' observation_key,fingerprint,observed_at) VALUES(?,?,?,?)',
                       ('alpha', key, 'a-different-fingerprint', NOW.isoformat()))
            with self.assertRaises(ValueError) as ctx:
                host.observation(key)
            self.assertEqual(str(ctx.exception),
                             'INSTANCE_OBSERVATION_SOURCE_CONFLICT')
            with self.assertRaises(ValueError):
                host.observations()

    def test_a_restart_over_the_same_database_still_refuses(self):
        key, fingerprint = self.key_and_fingerprint()
        self.legacy_checkpoint({key: fingerprint})
        restarted = InstanceStrategyHost(self.registry,
                                         self.registry.instance('alpha'),
                                         clock=lambda: NOW)
        with self.assertRaises(ValueError) as ctx:
            restarted.prepare(bar_event('bar-2', close='41.00'))
        self.assertEqual(str(ctx.exception), 'CONSUMED_BAR_REVISED')


class FrontierPersistenceTests(TestCase):
    """Historical frontier must outlive a commit that no longer carries it."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = sqlite3.connect(self.temp.name + '/persist.sqlite', isolation_level=None)
        self.addCleanup(self.db.close)
        self.registry = InstanceRegistry(self.db, clock=lambda: NOW)
        self.registry.register('alpha', manifest(), budget(), mode='shadow', enabled=True)
        self.host = InstanceStrategyHost(self.registry, self.registry.instance('alpha'),
                                         clock=lambda: NOW)
        self.plugin = DroppingPlugin()

    def key_and_fingerprint(self, begins, close='40.00'):
        return bar_observation_identity(
            bar_event('x', begins=begins, close=close)['data'])

    def quote(self, event_id='q-1'):
        at = NOW.isoformat()
        return {'id': event_id, 'kind': 'quote', 'at': at, 'synthetic_input': True,
                'source_sha256': 'source-' + event_id,
                'data': {'symbol': 'DEMO', 'bid': '40.00', 'ask': '40.01',
                         'bid_at': at, 'ask_at': at, 'observed_at': at}}

    def consume(self, event):
        prepared = self.host.prepare(event)
        self.assertIsNotNone(prepared)
        return self.host.commit(prepared, self.plugin.decide(prepared.request))

    def test_another_commit_cannot_drop_the_legacy_frontier(self):
        older = self.key_and_fingerprint('2026-10-30T13:00:00+00:00')
        newer = self.key_and_fingerprint('2026-10-30T13:30:00+00:00')
        self.registry.put_state('alpha',
                                {'bar_observations': {older[0]: older[1],
                                                      newer[0]: newer[1]}},
                                expected=None)
        self.assertEqual(self.db.execute(
            'SELECT count(*) FROM strategy_instance_observations').fetchone()[0], 0)

        # An unrelated, ordinary event. The plugin returns a checkpoint with no
        # bar_observations at all, which used to erase the only record.
        self.consume(self.quote())
        stored = self.host.observations()
        self.assertEqual(stored.get(older[0]), older[1])
        self.assertEqual(stored.get(newer[0]), newer[1])

        # Restart, then revise the OLDER key: still refused, still intact.
        restarted = InstanceStrategyHost(self.registry,
                                         self.registry.instance('alpha'),
                                         clock=lambda: NOW)
        revised = bar_event('bar-revised', close='41.00',
                            begins='2026-10-30T13:00:00+00:00')
        with self.assertRaises(ValueError) as ctx:
            restarted.prepare(revised)
        self.assertEqual(str(ctx.exception), 'CONSUMED_BAR_REVISED')
        self.assertEqual(restarted.observations().get(older[0]), older[1])
        self.assertEqual(restarted.observations().get(newer[0]), newer[1])

    def test_a_conflicting_historical_key_blocks_the_commit(self):
        key, fingerprint = self.key_and_fingerprint('2026-10-30T13:00:00+00:00')
        self.registry.put_state('alpha', {'bar_observations': {key: fingerprint}},
                                expected=None)
        self.db.execute('INSERT INTO strategy_instance_observations(instance_id,'
                        ' observation_key,fingerprint,observed_at) VALUES(?,?,?,?)',
                        ('alpha', key, 'a-different-fingerprint', NOW.isoformat()))
        before = self.host.state()
        with self.assertRaises(ValueError) as ctx:
            self.consume(self.quote())
        self.assertEqual(str(ctx.exception), 'INSTANCE_OBSERVATION_SOURCE_CONFLICT')
        self.assertEqual(self.host.state(), before)
        self.assertEqual(self.db.execute(
            'SELECT count(*) FROM strategy_instance_consumed').fetchone()[0], 0)


class DroppingPlugin:
    """Returns a checkpoint that deliberately drops bar_observations."""

    def decide(self, request):
        return StrategyDecision(identity=request.identity,
                                decision_id='d-' + request.cursor,
                                as_of=request.as_of,
                                data_snapshot_id=request.data_snapshot_id,
                                consumed_cursor=request.cursor,
                                synthetic=request.synthetic,
                                next_checkpoint={'seen': {'DEMO': 1}})
