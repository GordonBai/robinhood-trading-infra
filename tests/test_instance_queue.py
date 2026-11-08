"""Instance-namespaced durable advisory tasks and the V1 table migration."""
import sqlite3
import tempfile
from unittest import TestCase

import test_signal_host_isolation as fixture
from martin_agentic.strategy_queue import (LEGACY_INSTANCE_ID, DurableStrategyQueue,
                                           ensure_strategy_task_schema)
from test_strategy_queue import FakeRunner


class InstanceQueueTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.fixture = fixture.SignalHostIsolationTests()
        _, self.signals = self.fixture.books(self.temp.name)
        self.addCleanup(self.fixture.doCleanups)

    def event(self, identity='tick-a'):
        at = self.fixture.now.isoformat()
        return dict(id=identity, kind='quote', at=at, synthetic_input=True,
                    source_sha256='fixture-' + identity,
                    data=dict(symbol='DEMO', bid='40', ask='40.01', bid_at=at,
                              ask_at=at, observed_at=at))

    def queue(self, instance_id, *, source='a' * 64, bound=2048):
        runner = FakeRunner(self.signals.plugin, warm=True)
        queue = DurableStrategyQueue(self.signals, runner=runner, source_fingerprint=source,
                                     max_pending=bound, instance_id=instance_id)
        self.addCleanup(queue.close)
        return queue, runner

    def test_two_instances_own_the_same_event_id_independently(self):
        alpha, alpha_runner = self.queue('alpha')
        beta, beta_runner = self.queue('beta')
        event = self.event()
        self.assertTrue(alpha.enqueue(event)['enqueued'])
        self.assertTrue(beta.enqueue(event)['enqueued'])
        self.assertEqual(alpha.advance()['started'], 'tick-a')
        self.assertEqual(beta.advance()['started'], 'tick-a')
        self.assertEqual(len(alpha_runner.starts), 1)
        self.assertEqual(len(beta_runner.starts), 1)
        # Task namespaces are separate; the advisory identity still comes from
        # the host's own signals object, which this legacy fixture leaves fixed.
        self.assertEqual([run.cursor for run in alpha_runner.starts], ['tick-a'])
        self.assertEqual([run.cursor for run in beta_runner.starts], ['tick-a'])
        self.assertEqual(alpha_runner.starts[0].identity, beta_runner.starts[0].identity)

    def test_status_and_bounds_are_per_instance(self):
        alpha, _ = self.queue('alpha', bound=1)
        beta, _ = self.queue('beta', bound=1)
        alpha.enqueue(self.event('one'))
        # Alpha's bound is exhausted; beta is unaffected by alpha's queue depth.
        with self.assertRaisesRegex(ValueError, 'CAPACITY_EXCEEDED'):
            alpha.enqueue(self.event('two'))
        self.assertTrue(beta.enqueue(self.event('one'))['enqueued'])
        self.assertEqual(alpha.status()['pending'], 1)
        self.assertEqual(beta.status()['pending'], 1)

    def test_one_instance_starting_does_not_start_the_other(self):
        alpha, alpha_runner = self.queue('alpha')
        beta, beta_runner = self.queue('beta')
        alpha.enqueue(self.event('alpha-only'))
        beta.enqueue(self.event('beta-only'))
        self.assertEqual(alpha.advance()['started'], 'alpha-only')
        self.assertEqual(len(alpha_runner.starts), 1)
        self.assertEqual(len(beta_runner.starts), 0)
        self.assertEqual(beta.status()['queued'], 1)

    def test_restart_only_recovers_its_own_running_tasks(self):
        alpha, _ = self.queue('alpha')
        alpha.enqueue(self.event('recover-me'))
        alpha.advance()
        restarted, runner = self.queue('alpha')
        self.assertEqual(restarted.recovered_running, 1)
        self.assertEqual(restarted.advance()['started'], 'recover-me')
        other, other_runner = self.queue('other')
        self.assertEqual(other.recovered_running, 0)
        self.assertEqual(len(other_runner.starts), 0)

    def test_invalid_instance_id_is_refused(self):
        for value in ('', None, 7, 'x' * 129):
            with self.assertRaises(ValueError):
                DurableStrategyQueue(self.signals, runner=FakeRunner(self.signals.plugin),
                                     source_fingerprint='a' * 64, instance_id=value)


class LegacyTaskMigrationTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = sqlite3.connect(self.temp.name + '/legacy-tasks.sqlite', isolation_level=None)
        self.addCleanup(self.db.close)

    def _legacy_table(self):
        self.db.executescript('''
            CREATE TABLE strategy_tasks(
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,event_id TEXT NOT NULL UNIQUE,
                event_hash TEXT NOT NULL,event_payload BLOB NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('QUEUED','RUNNING','COMMITTED','REJECTED')),
                queued_at TEXT NOT NULL,source_fingerprint TEXT NOT NULL,config_fingerprint TEXT NOT NULL,
                request_payload BLOB,request_hash TEXT,checkpoint_hash TEXT,started_at TEXT,finished_at TEXT,
                reason TEXT,result_json TEXT,decision_payload BLOB,decision_hash TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,recoveries INTEGER NOT NULL DEFAULT 0);
            INSERT INTO strategy_tasks(event_id,event_hash,event_payload,state,queued_at,
                source_fingerprint,config_fingerprint) VALUES('old','h',X'00','COMMITTED','t','s','c');
        ''')

    def test_legacy_rows_keep_their_history_under_the_legacy_instance(self):
        self._legacy_table()
        ensure_strategy_task_schema(self.db)
        row = self.db.execute('SELECT instance_id,event_id,state FROM strategy_tasks').fetchall()
        self.assertEqual(row, [(LEGACY_INSTANCE_ID, 'old', 'COMMITTED')])

    def test_migration_is_idempotent(self):
        self._legacy_table()
        ensure_strategy_task_schema(self.db)
        ensure_strategy_task_schema(self.db)
        self.assertEqual(self.db.execute('SELECT count(*) FROM strategy_tasks').fetchone()[0], 1)
        self.assertIsNone(self.db.execute(
            "SELECT 1 FROM sqlite_master WHERE name='strategy_tasks_legacy_v1'").fetchone())

    def test_migrated_table_forgets_the_global_event_id_uniqueness(self):
        self._legacy_table()
        ensure_strategy_task_schema(self.db)
        self.db.execute("INSERT INTO strategy_tasks(instance_id,event_id,event_hash,event_payload,"
                        "state,queued_at,source_fingerprint,config_fingerprint)"
                        " VALUES('other','old','h',X'00','QUEUED','t','s','c')")
        owners = self.db.execute('SELECT instance_id FROM strategy_tasks WHERE event_id=?'
                                 ' ORDER BY instance_id', ('old',)).fetchall()
        self.assertEqual([row[0] for row in owners], ['martin-legacy-default', 'other'])
