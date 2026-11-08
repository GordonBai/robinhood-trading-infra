"""Versioned real-source read-only shadow with two isolated plugin instances."""
import json
import sqlite3
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

import test_execution
from test_partial_lifecycle import PartialFlow
from martin_luk.live_session import LiveSupervisor, run_live_session
from martin_agentic.simulation import SimulationSources
from trading_contracts.instance import InstanceBudget, PluginManifest
from trading_infra.instance_registry import InstanceRegistry
from test_live_session import full_snapshot
from test_supervised_host_default_loop import (SimulationReviewTransport,
                                               SimulationWake)

NOW = datetime(2026, 10, 16, 14, 0, tzinfo=timezone.utc)


class RealShapeBroker:
    simulation_only = True

    def __call__(self, approval_ids=()):
        return full_snapshot(NOW.isoformat())


class RealShapeMarket:
    simulation_only = True

    def __init__(self):
        self.calls = 0

    def __call__(self, *args, **kwargs):
        self.calls += 1
        at = NOW.isoformat()
        return ([{'id': 'real-shape-quote-1', 'kind': 'quote', 'at': at,
                  'synthetic_input': False, 'source_sha256': 'real-shape-source',
                  'source_type': 'desensitized_fixture',
                  'data': {'symbol': 'DEMO', 'bid': '39.99', 'ask': '40.00',
                           'bid_at': at, 'ask_at': at, 'observed_at': at}}],
                {'quotes': 1})


class ReadOnlyShadowTests(TestCase):
    def policy(self):
        value = json.loads(
            Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        value.update(capture_only=False, live_enabled=False,
                     strategy_instances=[
                         {'instance_id': 'martin-ro',
                          'plugin': 'martin', 'mode': 'read_only_shadow'},
                         {'instance_id': 'vwap-ro',
                          'plugin': 'vwap-fixture',
                          'mode': 'read_only_shadow'}])
        return value

    def test_two_plugins_share_real_source_without_state_or_capability_leak(self):
        policy = self.policy()
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory) / 'workspace'
            db = workspace / 'book.sqlite'
            market = RealShapeMarket()
            sources = SimulationSources(
                broker=RealShapeBroker(), market=market,
                review=SimulationReviewTransport, wake=SimulationWake())
            with patch('martin_agentic.live_session.single_instance'), \
                 patch('martin_agentic.live_session._broker_read',
                       side_effect=AssertionError('formal broker read')), \
                 patch('martin_agentic.live_session.collect_live_events',
                       side_effect=AssertionError('formal market read')), \
                 patch('martin_agentic.live_session.wait_for_result_or_tick',
                       side_effect=lambda *a, **k: time.sleep(0.1)), \
                 patch('martin_agentic.live_session.notify_local'), \
                 patch('martin_agentic.live_session.print'):
                result = run_live_session(
                    db, policy,
                    symbols=['DEMO'] + [str(index) for index in range(9)],
                    cycles=12, poll_seconds=5, agent_mode='disabled',
                    simulation=True, simulation_workspace=workspace,
                    simulation_sources=sources, source_mode='real_read_only',
                    clock=lambda: NOW, status_file=workspace / 'status.json',
                    output_dir=workspace / 'output')
            self.assertGreater(market.calls, 0)
            self.assertTrue(result['synthetic_session'])
            self.assertEqual(result['source_mode'], 'real_read_only')
            self.assertFalse(result['production_authorized'])
            con = sqlite3.connect(db)
            self.addCleanup(con.close)
            modes = dict(con.execute(
                'SELECT instance_id,mode FROM strategy_instances').fetchall())
            self.assertEqual(modes, {'martin-ro': 'read_only_shadow',
                                     'vwap-ro': 'read_only_shadow'})
            states = {row[0]: json.loads(row[1]) for row in con.execute(
                'SELECT instance_id,payload FROM strategy_instance_state'
                " WHERE state_key='checkpoint'").fetchall()}
            self.assertIn('machines', states['martin-ro'])
            self.assertNotIn('session_vwap', states['martin-ro'])
            self.assertIn('session_vwap', states['vwap-ro'])
            self.assertNotIn('machines', states['vwap-ro'])
            self.assertEqual(con.execute(
                'SELECT count(*) FROM broker_executions').fetchone()[0], 0)
            self.assertEqual(con.execute(
                "SELECT count(*) FROM live_actions WHERE kind='entry'"
            ).fetchone()[0], 0)
            self.assertEqual(con.execute(
                'SELECT count(*) FROM execution_intents').fetchone()[0], 0)
            blocked = ((result.get('strategy_admissions') or {}).get('outcomes')
                       or [])
            self.assertTrue(any(item.get('reason') ==
                                'READ_ONLY_EXECUTION_CAPABILITY_NONE'
                                for item in blocked), blocked)
            proposals = con.execute(
                'SELECT instance_id,count(*) FROM strategy_instance_proposals'
                ' GROUP BY instance_id').fetchall()
            self.assertIn(('vwap-ro', 1), proposals)
            self.assertNotIn('martin-ro', {row[0] for row in proposals})
            queue = result.get('strategy_queue') or {}
            self.assertTrue(queue.get('enabled'))
            self.assertEqual(queue.get('errors'), [])
            for instance_id in ('martin-ro', 'vwap-ro'):
                self.assertEqual(
                    queue['instances'][instance_id]['errors'], [],
                    queue)
                self.assertGreaterEqual(
                    queue['status'][instance_id]['committed'], 1, queue)

    def test_old_two_mode_registry_migrates_without_losing_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            db = sqlite3.connect(Path(directory) / 'old.sqlite')
            self.addCleanup(db.close)
            db.executescript('''
                CREATE TABLE strategy_instances(
                    instance_id TEXT PRIMARY KEY,
                    plugin_id TEXT NOT NULL, version TEXT NOT NULL,
                    state_schema TEXT NOT NULL, config_json TEXT NOT NULL,
                    config_fingerprint TEXT NOT NULL, manifest_digest TEXT NOT NULL,
                    data_requirements TEXT NOT NULL, capabilities TEXT NOT NULL,
                    max_inflight INTEGER NOT NULL, max_cash TEXT NOT NULL,
                    max_planned_loss TEXT NOT NULL, max_notional TEXT NOT NULL,
                    max_concurrent INTEGER NOT NULL,
                    mode TEXT NOT NULL CHECK(mode IN ('shadow','disabled')),
                    enabled INTEGER NOT NULL CHECK(enabled IN (0,1)),
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
                INSERT INTO strategy_instances VALUES(
                    'alpha','vwap-fixture','1.0','vwap-fixture-1','{}',
                    'f','d','["quotes"]','[]',8,'100','2.50','100',1,
                    'shadow',1,'2026-10-16T14:00:00+00:00',
                    '2026-10-16T14:00:00+00:00');
            ''')
            registry = InstanceRegistry(db)
            sql = db.execute("SELECT sql FROM sqlite_master WHERE type='table'"
                             " AND name='strategy_instances'").fetchone()[0]
            self.assertIn('read_only_shadow', sql)
            self.assertEqual(registry.instance('alpha').mode, 'shadow')
            reopened = InstanceRegistry(db)
            self.assertEqual(reopened.instance('alpha').mode, 'shadow')
            self.assertEqual(db.execute(
                'SELECT version FROM strategy_instance_schema WHERE id=1'
            ).fetchone()[0], 2)
            manifest = PluginManifest(
                plugin_id='vwap-fixture', version='1.0',
                state_schema='vwap-fixture-1',
                config={'cost_buffer_per_share': '0.05'},
                data_requirements=('quotes',),
                capabilities=('advisory-entry',), max_inflight=256)
            budget = InstanceBudget(max_cash='100', max_planned_loss='2.50',
                                    max_notional='100', max_concurrent=1)
            registered = registry.register(
                'read-only', manifest, budget,
                mode='read_only_shadow', enabled=True)
            self.assertEqual(registered.source_mode, 'real_read_only')
            self.assertEqual(registered.execution_capability, 'NONE')

    def test_future_or_mismatched_schema_version_is_never_rewritten(self):
        with tempfile.TemporaryDirectory() as directory:
            db = sqlite3.connect(Path(directory) / 'future.sqlite')
            self.addCleanup(db.close)
            db.executescript('''
                CREATE TABLE strategy_instance_schema(
                    id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL);
                INSERT INTO strategy_instance_schema VALUES(1,99);
                CREATE TABLE strategy_instances(
                    instance_id TEXT PRIMARY KEY,
                    plugin_id TEXT NOT NULL, version TEXT NOT NULL,
                    state_schema TEXT NOT NULL, config_json TEXT NOT NULL,
                    config_fingerprint TEXT NOT NULL, manifest_digest TEXT NOT NULL,
                    data_requirements TEXT NOT NULL, capabilities TEXT NOT NULL,
                    max_inflight INTEGER NOT NULL, max_cash TEXT NOT NULL,
                    max_planned_loss TEXT NOT NULL, max_notional TEXT NOT NULL,
                    max_concurrent INTEGER NOT NULL,
                    mode TEXT NOT NULL CHECK(mode IN ('shadow','disabled')),
                    enabled INTEGER NOT NULL CHECK(enabled IN (0,1)),
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
                INSERT INTO strategy_instances VALUES(
                    'alpha','vwap-fixture','1.0','vwap-fixture-1','{}',
                    'f','d','["quotes"]','[]',8,'100','2.50','100',1,
                    'shadow',1,'2026-10-16T14:00:00+00:00',
                    '2026-10-16T14:00:00+00:00');
            ''')
            with self.assertRaisesRegex(
                    ValueError, 'INSTANCE_SCHEMA_VERSION_UNSUPPORTED'):
                InstanceRegistry(db)
            self.assertEqual(db.execute(
                'SELECT version FROM strategy_instance_schema WHERE id=1'
            ).fetchone()[0], 99)
            self.assertEqual(db.execute(
                "SELECT count(*) FROM strategy_instances WHERE instance_id='alpha'"
            ).fetchone()[0], 1)
        with tempfile.TemporaryDirectory() as directory:
            db = sqlite3.connect(Path(directory) / 'mismatch.sqlite')
            self.addCleanup(db.close)
            db.executescript('''
                CREATE TABLE strategy_instance_schema(
                    id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL);
                INSERT INTO strategy_instance_schema VALUES(1,2);
                CREATE TABLE strategy_instances(
                    instance_id TEXT PRIMARY KEY,
                    plugin_id TEXT NOT NULL, version TEXT NOT NULL,
                    state_schema TEXT NOT NULL, config_json TEXT NOT NULL,
                    config_fingerprint TEXT NOT NULL, manifest_digest TEXT NOT NULL,
                    data_requirements TEXT NOT NULL, capabilities TEXT NOT NULL,
                    max_inflight INTEGER NOT NULL, max_cash TEXT NOT NULL,
                    max_planned_loss TEXT NOT NULL, max_notional TEXT NOT NULL,
                    max_concurrent INTEGER NOT NULL,
                    mode TEXT NOT NULL CHECK(mode IN ('shadow','disabled')),
                    enabled INTEGER NOT NULL CHECK(enabled IN (0,1)),
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
            ''')
            with self.assertRaisesRegex(
                    ValueError, 'INSTANCE_SCHEMA_STRUCTURE_MISMATCH'):
                InstanceRegistry(db)
            self.assertEqual(db.execute(
                'SELECT version FROM strategy_instance_schema WHERE id=1'
            ).fetchone()[0], 2)

    def test_failed_mode_migration_rolls_back_preserving_old_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            db = sqlite3.connect(Path(directory) / 'rollback.sqlite')
            self.addCleanup(db.close)
            db.executescript('''
                CREATE TABLE strategy_instances(
                    instance_id TEXT PRIMARY KEY,
                    plugin_id TEXT NOT NULL, version TEXT NOT NULL,
                    state_schema TEXT NOT NULL, config_json TEXT NOT NULL,
                    config_fingerprint TEXT NOT NULL, manifest_digest TEXT NOT NULL,
                    data_requirements TEXT NOT NULL, capabilities TEXT NOT NULL,
                    max_inflight INTEGER NOT NULL, max_cash TEXT NOT NULL,
                    max_planned_loss TEXT NOT NULL, max_notional TEXT NOT NULL,
                    max_concurrent INTEGER NOT NULL,
                    mode TEXT NOT NULL CHECK(mode IN ('shadow','disabled')),
                    enabled INTEGER NOT NULL CHECK(enabled IN (0,1)),
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
                INSERT INTO strategy_instances VALUES(
                    'alpha','vwap-fixture','1.0','vwap-fixture-1','{}',
                    'f','d','["quotes"]','[]',8,'100','2.50','100',1,
                    'shadow',1,'2026-10-16T14:00:00+00:00',
                    '2026-10-16T14:00:00+00:00');
                CREATE TABLE strategy_instances_v2(conflict TEXT);
            ''')
            with self.assertRaises(sqlite3.OperationalError):
                InstanceRegistry(db)
            self.assertEqual(db.execute(
                "SELECT count(*) FROM strategy_instances WHERE instance_id='alpha'"
            ).fetchone()[0], 1)
            sql = db.execute("SELECT sql FROM sqlite_master WHERE type='table'"
                             " AND name='strategy_instances'").fetchone()[0]
            self.assertNotIn('read_only_shadow', sql)

    def test_public_supervisor_read_only_never_calls_dispatcher(self):
        class Spy:
            def __init__(self):
                self.calls = []
            def __call__(self, **kwargs):
                self.calls.append(kwargs)
                return {'agent_called': True}
        fixture = test_execution.ExecutionTests(
            'test_empty_queue_has_zero_agent_calls')
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        fixture.entry_filled()
        spy = Spy()
        supervisor = LiveSupervisor(
            fixture.book, dispatcher=spy, agent_mode='supervised-host',
            source_mode='real_read_only', owner_source='f' * 64)
        result = supervisor.step(fixture.snap(), [], allow_entries=False)
        self.assertEqual(spy.calls, [])
        self.assertEqual(result['dispatch']['reason'],
                         'READ_ONLY_EXECUTION_CAPABILITY_NONE')

    def test_unknown_or_write_capable_source_mode_is_rejected(self):
        policy = self.policy()
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory) / 'workspace'
            sources = SimulationSources(
                broker=RealShapeBroker(), market=RealShapeMarket(),
                review=SimulationReviewTransport, wake=SimulationWake())
            with self.assertRaisesRegex(ValueError, 'SOURCE_MODE_INVALID'):
                run_live_session(
                    workspace / 'book.sqlite', policy,
                    symbols=['DEMO'] + [str(index) for index in range(9)],
                    cycles=1, agent_mode='disabled', simulation=True,
                    simulation_workspace=workspace,
                    simulation_sources=sources, source_mode='other',
                    clock=lambda: NOW, budget_entry_limit=0,
                    budget_total_limit=0)
            self.assertFalse(workspace.exists())
            with self.assertRaisesRegex(
                    ValueError, 'REAL_READ_ONLY_REQUIRES_DISABLED_AGENT'):
                run_live_session(
                    workspace / 'book.sqlite', policy,
                    symbols=['DEMO'] + [str(index) for index in range(9)],
                    cycles=1, agent_mode='supervised-host', simulation=True,
                    simulation_workspace=workspace,
                    simulation_sources=sources, source_mode='real_read_only',
                    clock=lambda: NOW, budget_entry_limit=0,
                    budget_total_limit=0)
            self.assertFalse(workspace.exists())


class MixedReadOnlyInstanceTests(PartialFlow):
    """A read-only observer must not stop another instance's protection."""

    def test_read_only_observer_does_not_block_alpha_protection_dispatch(self):
        from test_partial_lifecycle import (DecisionRunner, TwoSharePlugin,
                                            instance_budget, manifest)
        from trading_infra.instance_runtime import InstanceRuntime
        from trading_infra.instance_supervisor import InstanceSupervisor
        entry = self.make_entry()
        self.step([self.order_for(entry, filled=0, state='confirmed')],
                  dispatch=False)
        fill = self.execution('exec-mixed', 2)
        self.pay_executions([fill])
        self.now += timedelta(seconds=5)
        self.step([self.order_for(entry, filled=2, state='filled',
                                  executions=[fill])], qty='2', dispatch=False)
        protection = self.action('protect')
        self.assertIsNotNone(protection)
        self.assertEqual(protection['state'], 'READY')

        self.registry.register('observer', manifest('vwap-fixture'),
                               instance_budget(), mode='read_only_shadow',
                               enabled=True)
        observer = InstanceRuntime(
            self.registry, self.registry.instance('observer'),
            runner=DecisionRunner(TwoSharePlugin(proposal_at=1)),
            source_fingerprint='f' * 64, clock=lambda: self.now)
        self.addCleanup(observer.close)
        self.supervisor = LiveSupervisor(
            self.book, dispatcher=self.dispatcher, process_strategy=False,
            agent_mode='review', owner_source='f' * 64,
            instance_supervisor=InstanceSupervisor([self.runtime, observer]),
            admission=self.admission, accounting=self.accounting,
            source_mode='synthetic')
        before = len(self.dispatcher.reported)
        result = self.step([self.order_for(entry, filled=2, state='filled',
                                           executions=[fill])], qty='2')
        current = next(item for item in self.book.actions()
                       if item['id'] == protection['id'])
        self.assertNotEqual(current['state'], 'READY', result.get('errors'))
        self.assertGreater(len(self.dispatcher.reported), before)
        self.assertIn(protection['id'], self.dispatcher.reported)


class RegistryTransactionBoundaryTests(TestCase):
    def test_active_caller_transaction_is_rejected_before_ddl(self):
        with tempfile.TemporaryDirectory() as directory:
            db = sqlite3.connect(Path(directory) / 'tx.sqlite')
            self.addCleanup(db.close)
            db.execute('CREATE TABLE sentinel(value TEXT)')
            db.execute('BEGIN IMMEDIATE')
            db.execute("INSERT INTO sentinel VALUES('uncommitted')")
            with self.assertRaisesRegex(
                    ValueError, 'INSTANCE_SCHEMA_REQUIRES_IDLE_CONNECTION'):
                InstanceRegistry(db)
            self.assertTrue(db.in_transaction)
            self.assertIsNone(db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
                " AND name='strategy_instances'").fetchone())
            db.rollback()
            self.assertEqual(db.execute(
                'SELECT count(*) FROM sentinel').fetchone()[0], 0)


class RegistryStructureKeywordTests(TestCase):
    def test_keyword_only_fake_structure_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            db = sqlite3.connect(Path(directory) / 'fake.sqlite')
            self.addCleanup(db.close)
            db.executescript('''
                CREATE TABLE strategy_instance_schema(
                    id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL);
                INSERT INTO strategy_instance_schema VALUES(1,2);
                CREATE TABLE strategy_instances(
                    note TEXT CHECK(note IN ('read_only_shadow')));
            ''')
            with self.assertRaisesRegex(
                    ValueError, 'INSTANCE_SCHEMA_STRUCTURE_MISMATCH'):
                InstanceRegistry(db)
            self.assertEqual(db.execute(
                'SELECT version FROM strategy_instance_schema WHERE id=1'
            ).fetchone()[0], 2)
            self.assertEqual(
                [row[1] for row in db.execute(
                    'PRAGMA table_info(strategy_instances)')], ['note'])
