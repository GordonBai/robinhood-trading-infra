"""Strategy-instance contracts and durable registry over a temporary synthetic DB."""
import json
import sqlite3
import tempfile
from datetime import datetime, timezone
from decimal import Decimal
from unittest import TestCase

from trading_contracts.instance import InstanceBudget, PluginManifest, StrategyInstance
from trading_infra.instance_registry import (LEGACY_INSTANCE_ID, InstanceRegistry,
                                             migrate_legacy_live_strategy)

AT = datetime(2026, 10, 29, 14, 0, tzinfo=timezone.utc)


def manifest(**overrides):
    fields = dict(plugin_id='martin', version='pullback-research-0.1',
                  state_schema='martin-pullback-state-1',
                  config={'cost_buffer_per_share': '0.05', 'ema_periods': '9,21,50'},
                  data_requirements=('5minute-bars', 'universe-screen'),
                  capabilities=('advisory-entry', 'structural-stop'), max_inflight=256)
    fields.update(overrides)
    return PluginManifest(**fields)


def budget(**overrides):
    fields = dict(max_cash=Decimal('100'), max_planned_loss=Decimal('2.50'),
                  max_notional=Decimal('100'), max_concurrent=1)
    fields.update(overrides)
    return InstanceBudget(**fields)


class ManifestTests(TestCase):
    def test_digest_is_stable_and_config_sensitive(self):
        self.assertEqual(manifest().digest(), manifest().digest())
        changed = manifest(config={'cost_buffer_per_share': '0.06', 'ema_periods': '9,21,50'})
        self.assertNotEqual(manifest().digest(), changed.digest())
        self.assertNotEqual(manifest().config_fingerprint(), changed.config_fingerprint())

    def test_nested_config_is_rejected(self):
        with self.assertRaises(ValueError):
            manifest(config={'nested': {'a': 1}})

    def test_float_and_none_config_are_rejected(self):
        for value in (1.5, None, [1, 2]):
            with self.assertRaises(ValueError):
                manifest(config={'cost_buffer_per_share': value})

    def test_config_key_must_be_a_lower_snake_identifier(self):
        for key in ('Cost', 'cost-buffer', '', '1cost'):
            with self.assertRaises(ValueError):
                manifest(config={key: '1'})

    def test_duplicate_or_empty_requirements_rejected(self):
        with self.assertRaises(ValueError):
            manifest(data_requirements=('a', 'a'))
        with self.assertRaises(ValueError):
            manifest(data_requirements=())

    def test_capabilities_may_be_empty(self):
        self.assertEqual(manifest(capabilities=()).capabilities, ())

    def test_max_inflight_must_be_positive_int(self):
        for value in (0, -1, 1.5, '8', True):
            with self.assertRaises(ValueError):
                manifest(max_inflight=value)

    def test_config_is_immutable(self):
        with self.assertRaises(TypeError):
            manifest().config['cost_buffer_per_share'] = '0.99'


class InstanceContractTests(TestCase):
    def test_budget_requires_positive_values(self):
        for field in ('max_cash', 'max_planned_loss', 'max_notional'):
            with self.assertRaises(ValueError):
                budget(**{field: Decimal('0')})
        with self.assertRaises(ValueError):
            budget(max_concurrent=0)
        with self.assertRaises(ValueError):
            budget(max_cash=1.5)

    def test_disabled_mode_cannot_be_enabled(self):
        with self.assertRaises(ValueError):
            StrategyInstance('one', manifest(), budget(), 'disabled', True)
        with self.assertRaises(ValueError):
            StrategyInstance('one', manifest(), budget(), 'live', False)

    def test_strategy_identity_comes_from_the_manifest(self):
        instance = StrategyInstance('one', manifest(), budget(), 'shadow', True)
        identity = instance.strategy_identity()
        self.assertEqual((identity.instance_id, identity.plugin_id, identity.version),
                         ('one', 'martin', 'pullback-research-0.1'))


class RegistryTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = sqlite3.connect(self.temp.name + '/registry.sqlite', isolation_level=None)
        self.addCleanup(self.db.close)
        self.registry = InstanceRegistry(self.db, clock=lambda: AT)

    def test_register_and_read_back(self):
        registered = self.registry.register('alpha', manifest(), budget(), mode='shadow',
                                            enabled=True)
        self.assertEqual(registered.instance_id, 'alpha')
        self.assertEqual(registered.manifest.digest(), manifest().digest())
        self.assertEqual(registered.budget.as_dict(), budget().as_dict())
        self.assertTrue(registered.enabled)

    def test_registration_is_idempotent(self):
        self.registry.register('alpha', manifest(), budget(), enabled=True)
        again = self.registry.register('alpha', manifest(), budget(), enabled=True)
        self.assertEqual(again.instance_id, 'alpha')
        self.assertEqual(len(self.registry.instances()), 1)

    def test_changed_manifest_is_a_conflict_not_an_update(self):
        self.registry.register('alpha', manifest(), budget())
        with self.assertRaises(ValueError) as ctx:
            self.registry.register('alpha', manifest(version='pullback-research-0.2'), budget())
        self.assertEqual(str(ctx.exception), 'INSTANCE_REGISTRATION_CONFLICT')

    def test_changed_budget_is_a_conflict_not_an_update(self):
        self.registry.register('alpha', manifest(), budget())
        with self.assertRaises(ValueError) as ctx:
            self.registry.register('alpha', manifest(), budget(max_cash=Decimal('500')))
        self.assertEqual(str(ctx.exception), 'INSTANCE_REGISTRATION_CONFLICT')

    def test_enabled_flag_change_requires_set_mode(self):
        self.registry.register('alpha', manifest(), budget(), enabled=False)
        with self.assertRaises(ValueError) as ctx:
            self.registry.register('alpha', manifest(), budget(), enabled=True)
        self.assertEqual(str(ctx.exception), 'INSTANCE_MODE_CHANGE_REQUIRES_SET_MODE')

    def test_unregistered_instance_is_refused(self):
        with self.assertRaises(ValueError) as ctx:
            self.registry.instance('missing')
        self.assertEqual(str(ctx.exception), 'INSTANCE_NOT_REGISTERED')
        self.assertIsNone(self.registry.find('missing'))

    def test_set_mode_disables_without_changing_the_contract(self):
        self.registry.register('alpha', manifest(), budget(), enabled=True)
        before = self.registry.instance('alpha')
        after = self.registry.set_mode('alpha', 'disabled', enabled=False)
        self.assertFalse(after.enabled)
        self.assertEqual(after.manifest.digest(), before.manifest.digest())
        self.assertEqual(self.registry.enabled_instances(), [])
        with self.assertRaises(ValueError):
            self.registry.set_mode('alpha', 'disabled', enabled=True)

    def test_two_instances_keep_separate_contracts(self):
        self.registry.register('alpha', manifest(), budget(), enabled=True)
        self.registry.register('beta', manifest(plugin_id='other', version='flat-0.1'),
                               budget(max_cash=Decimal('50')), enabled=True)
        enabled = {item.instance_id for item in self.registry.enabled_instances()}
        self.assertEqual(enabled, {'alpha', 'beta'})
        self.assertNotEqual(self.registry.instance('alpha').manifest.digest(),
                            self.registry.instance('beta').manifest.digest())

    def test_state_compare_and_set(self):
        self.registry.register('alpha', manifest(), budget())
        first = self.registry.put_state('alpha', {'machines': {}}, expected=None)
        self.assertEqual(self.registry.state_digest('alpha'), first)
        with self.assertRaises(ValueError) as ctx:
            self.registry.put_state('alpha', {'machines': {}}, expected='stale')
        self.assertEqual(str(ctx.exception), 'INSTANCE_STATE_CONFLICT')
        second = self.registry.put_state('alpha', {'machines': {'SPY': {}}},
                                        expected={'machines': {}})
        self.assertNotEqual(first, second)
        self.assertEqual(self.registry.state('alpha'), {'machines': {'SPY': {}}})

    def test_state_is_isolated_per_instance(self):
        self.registry.register('alpha', manifest(), budget())
        self.registry.register('beta', manifest(plugin_id='other', version='flat-0.1'), budget())
        self.registry.put_state('alpha', {'symbols': ['SPY']}, expected=None)
        self.registry.put_state('beta', {'symbols': ['QQQ']}, expected=None)
        self.assertEqual(self.registry.state('alpha'), {'symbols': ['SPY']})
        self.assertEqual(self.registry.state('beta'), {'symbols': ['QQQ']})

    def test_instance_events_are_deduplicated_per_instance(self):
        self.registry.register('alpha', manifest(), budget())
        self.registry.record('alpha', 'rejection', 'event-1', {'reason': 'X'})
        self.registry.record('alpha', 'rejection', 'event-1', {'reason': 'Y'})
        events = self.registry.events('alpha')
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]['detail'], {'reason': 'X'})

    def test_registry_survives_a_new_connection(self):
        self.registry.register('alpha', manifest(), budget(), enabled=True)
        self.registry.put_state('alpha', {'machines': {}}, expected=None)
        reopened = sqlite3.connect(self.temp.name + '/registry.sqlite', isolation_level=None)
        self.addCleanup(reopened.close)
        registry = InstanceRegistry(reopened, clock=lambda: AT)
        self.assertEqual(registry.instance('alpha').manifest.digest(), manifest().digest())
        self.assertEqual(registry.state('alpha'), {'machines': {}})


class LegacyMigrationTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = sqlite3.connect(self.temp.name + '/legacy.sqlite', isolation_level=None)
        self.addCleanup(self.db.close)
        self.registry = InstanceRegistry(self.db, clock=lambda: AT)
        self.registry.register(LEGACY_INSTANCE_ID, manifest(), budget(), enabled=True)

    def _legacy(self, payload):
        self.db.executescript('CREATE TABLE IF NOT EXISTS live_strategy'
                              '(id INTEGER PRIMARY KEY CHECK(id=1),payload TEXT NOT NULL);')
        self.db.execute('DELETE FROM live_strategy')
        self.db.execute('INSERT INTO live_strategy VALUES(1,?)',
                        (json.dumps(payload, sort_keys=True, separators=(',', ':')),))

    def test_absent_legacy_table_is_not_an_error(self):
        self.assertEqual(migrate_legacy_live_strategy(self.db, self.registry)['reason'],
                         'LEGACY_TABLE_ABSENT')

    def test_absent_legacy_row_is_not_an_error(self):
        self.db.executescript('CREATE TABLE IF NOT EXISTS live_strategy'
                              '(id INTEGER PRIMARY KEY CHECK(id=1),payload TEXT NOT NULL);')
        self.assertEqual(migrate_legacy_live_strategy(self.db, self.registry)['reason'],
                         'LEGACY_STATE_ABSENT')

    def test_copy_is_idempotent_and_never_writes_the_legacy_table(self):
        self._legacy({'machines': {}, 'ema9': {}, 'universe_complete': False})
        before = self.db.execute('SELECT payload FROM live_strategy WHERE id=1').fetchone()[0]
        first = migrate_legacy_live_strategy(self.db, self.registry)
        self.assertTrue(first['migrated'])
        second = migrate_legacy_live_strategy(self.db, self.registry)
        self.assertEqual(second['reason'], 'ALREADY_CURRENT')
        self.assertEqual(self.db.execute('SELECT payload FROM live_strategy WHERE id=1').fetchone()[0],
                         before)
        self.assertEqual(self.registry.state(LEGACY_INSTANCE_ID),
                         {'machines': {}, 'ema9': {}, 'universe_complete': False})

    def test_advanced_instance_state_is_never_overwritten(self):
        self._legacy({'machines': {}, 'ema9': {}, 'universe_complete': False})
        self.registry.put_state(LEGACY_INSTANCE_ID, {'machines': {'SPY': {}}}, expected=None)
        with self.assertRaises(ValueError) as ctx:
            migrate_legacy_live_strategy(self.db, self.registry)
        self.assertEqual(str(ctx.exception), 'INSTANCE_STATE_CONFLICT')

    def test_migration_requires_a_registered_instance(self):
        self.registry.set_mode(LEGACY_INSTANCE_ID, 'disabled', enabled=False)
        self._legacy({'machines': {}})
        with self.assertRaises(ValueError):
            migrate_legacy_live_strategy(self.db, self.registry, instance_id='unknown')
