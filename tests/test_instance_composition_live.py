"""Two real Martin warm workers through the default composition path.

This is the review's required integration: the policy declares two instances,
``build_instance_group`` builds them the way the daemon would, each one runs a
real child process, and each must commit one legal input with no crash and no
configuration rejection. A fake runner would not prove the configuration
binding agreement between host and worker.

Temporary synthetic database. No broker, no order.
"""
import json
import sqlite3
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from unittest import TestCase

from martin_luk.instance_composition import build_instance_group
from martin_agentic.qualification import source_fingerprint

NOW = datetime(2026, 10, 15, 13, 25, tzinfo=timezone.utc)


class Owner:
    def __init__(self, db, clock):
        self.db = db
        self.clock = clock
        self.simulation = True
        self.policy = {}

    def _begin(self):
        self.db.execute('BEGIN IMMEDIATE')


class TwoRealWorkerTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = sqlite3.connect(self.temp.name + '/instances.sqlite', isolation_level=None)
        self.addCleanup(self.db.close)
        self.book = Owner(self.db, lambda: NOW)
        self.policy = json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        self.policy.update(
            session_date='2026-10-15',
            strategy_instances=[
                {'instance_id': 'martin-a', 'plugin': 'martin',
                 'cost_buffer_per_share': '0.05'},
                {'instance_id': 'martin-b', 'plugin': 'martin',
                 'cost_buffer_per_share': '0.10'}])

    def event(self, event_id, symbol='DEMO'):
        at = NOW.isoformat()
        return {'id': event_id, 'kind': 'quote', 'at': at, 'synthetic_input': True,
                'source_sha256': 'fixture-' + event_id,
                'data': {'symbol': symbol, 'bid': '40.00', 'ask': '40.01',
                         'bid_at': at, 'ask_at': at, 'observed_at': at}}

    def drain(self, group, *, ceiling=240):
        """Advance until nothing is pending, using real child processes."""
        for _ in range(ceiling):
            report = group.advance()
            if not report['pending'] and not report['busy']:
                return report
            time.sleep(0.005)
        self.fail('instances did not drain in time')

    def test_two_real_workers_commit_one_input_each(self):
        group = build_instance_group(self.book, self.policy,
                                     source_fingerprint=source_fingerprint(),
                                     clock=lambda: NOW)
        self.addCleanup(group.close)
        self.assertEqual(group.instance_ids(), ['martin-a', 'martin-b'])
        group.enqueue('martin-a', self.event('a-1'))
        group.enqueue('martin-b', self.event('b-1'))
        report = self.drain(group)
        self.assertEqual(report['errors'], [])
        for instance_id in ('martin-a', 'martin-b'):
            status = group.status()[instance_id]
            self.assertEqual(status['committed'], 1, instance_id)
            self.assertEqual(status['rejected'], 0, instance_id)
            published = report['instances'][instance_id]
            self.assertEqual(published['errors'], [], instance_id)

    def test_a_wrong_source_pin_is_still_refused(self):
        group = build_instance_group(self.book, self.policy,
                                     source_fingerprint='0' * 64, clock=lambda: NOW)
        self.addCleanup(group.close)
        group.enqueue('martin-a', self.event('a-2'))
        report = self.drain(group)
        status = group.status()['martin-a']
        self.assertEqual(status['committed'], 0)
        self.assertEqual(status['rejected'], 1)
        self.assertTrue(report['errors'] or report['instances']['martin-a']['rejected'])


class ConfiguredPluginFamilyTests(TestCase):
    """A second, quotes-only family runs through the production worker path."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = sqlite3.connect(self.temp.name + '/families.sqlite',
                                  isolation_level=None)
        self.addCleanup(self.db.close)
        self.book = Owner(self.db, lambda: NOW)
        self.policy = json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        self.policy.update(
            session_date='2026-10-15',
            strategy_instances=[
                {'instance_id': 'martin-a', 'plugin': 'martin'},
                {'instance_id': 'vwap-a', 'plugin': 'vwap-fixture'}])

    def event(self, event_id):
        at = NOW.isoformat()
        return {'id': event_id, 'kind': 'quote', 'at': at, 'synthetic_input': True,
                'source_sha256': 'fixture-' + event_id,
                'data': {'symbol': 'DEMO', 'bid': '40.00', 'ask': '40.01',
                         'bid_at': at, 'ask_at': at, 'observed_at': at}}

    def test_two_plugin_families_use_their_declared_workers(self):
        group = build_instance_group(self.book, self.policy,
                                     source_fingerprint=source_fingerprint(),
                                     clock=lambda: NOW)
        self.addCleanup(group.close)
        self.assertEqual(group.instance_ids(), ['martin-a', 'vwap-a'])
        vwap = group.status()['vwap-a']
        self.assertEqual(group.runtime('vwap-a').instance.manifest.plugin_id,
                         'vwap-fixture')
        self.assertEqual(group.runtime('vwap-a').instance.manifest.data_requirements,
                         ('quotes',))
        group.enqueue('vwap-a', self.event('v-1'))
        for _ in range(240):
            report = group.advance()
            if not report['pending'] and not report['busy']:
                break
            time.sleep(0.005)
        self.assertEqual(report['errors'], [])
        self.assertEqual(group.status()['vwap-a']['committed'], 1)
        self.assertEqual(group.status()['vwap-a']['rejected'], 0)
