"""C5-specific upgrade boundaries: old Host identity and manifest compatibility."""
import json
import sqlite3
import tempfile
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest import TestCase

from martin_agentic.adapters import BridgeHostDeliveryPort
from martin_luk.instance_composition import build_instance_group
from martin_luk.live_session import HostDeliveryDispatcher
from trading_contracts.instance import InstanceBudget, PluginManifest
from trading_infra.instance_registry import InstanceRegistry

NOW = datetime(2026, 10, 16, 14, 0, tzinfo=timezone.utc)


class _Book:
    def __init__(self, actions, db=None):
        self._actions = actions
        self.db = db

    def actions(self):
        return self._actions


def _request(root, value):
    folder = root / 'requests'
    folder.mkdir(exist_ok=True)
    (folder / (value['request_id'] + '.json')).write_text(json.dumps(value))


def _intent_db(intent_id, trade_id, *, native=False):
    db = sqlite3.connect(':memory:', isolation_level=None)
    db.execute('CREATE TABLE execution_intents(intent_id TEXT, trade_id TEXT)')
    db.execute('INSERT INTO execution_intents VALUES(?,?)', (intent_id, trade_id))
    if native:
        db.execute('CREATE TABLE native_operation_records('
                   'id TEXT, action_id TEXT, operation TEXT)')
        db.execute("INSERT INTO native_operation_records VALUES"
                   "('native-refresh','manage-1','review')")
    return db


class LegacyHostIdentityTests(TestCase):
    def test_old_management_index_is_detected_and_blocks_dispatch(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            deliveries = root / 'deliveries'
            deliveries.mkdir()
            value = {
                'intent_id': 'intent-old-entry', 'request_id': 'a8de6dfb',
                'operation': 'place', 'action_id': 'manage-1',
                'trade_id': 'trade-1'}
            (deliveries / 'old.json').write_text(json.dumps(value))
            _request(root, value)
            book = _Book([{'id': 'manage-1', 'trade_id': 'trade-1',
                           'kind': 'protect', 'state': 'REPORTED'}],
                         _intent_db('intent-old-entry', 'trade-1'))
            class Bridge:
                def __init__(self):
                    self.book = book
                    self.directory = root
                    self.binding = {'account': 'x'}
                    self.clock = lambda: NOW

                def __call__(self, *args, **kwargs):
                    return {}
            port = BridgeHostDeliveryPort(Bridge())
            legacy = port.legacy_management_deliveries()
            self.assertEqual(legacy, [{'action_id': 'manage-1',
                                       'intent_id': 'intent-old-entry',
                                       'operation': 'place',
                                       'request_id': 'a8de6dfb'}])
            dispatcher = HostDeliveryDispatcher(book, port, Bridge())
            self.assertTrue(dispatcher.legacy_block)
            self.assertEqual(dispatcher._select(None, None, True)[2],
                             'LEGACY_HOST_IDENTITY_REQUIRES_RECONCILIATION')
            # The old index is preserved; no new identity was written.
            self.assertTrue((deliveries / 'old.json').exists())
            diagnostic = json.loads((root /
                'legacy-identity-diagnostic.json').read_text())
            self.assertEqual(diagnostic['status'],
                             'LEGACY_HOST_IDENTITY_REQUIRES_RECONCILIATION')
            # Restart with the same durable directory sees the old index again
            # and does not clear the isolation because the files still exist.
            again = HostDeliveryDispatcher(book, port, Bridge())
            self.assertTrue(again.legacy_block)
            self.assertEqual(again._select(None, None, True)[2],
                             'LEGACY_HOST_IDENTITY_REQUIRES_RECONCILIATION')


class LegacyHostFailClosedTests(TestCase):
    def _bridge(self, root, actions):
        book = _Book(actions)

        class Bridge:
            def __init__(self):
                self.book = book
                self.directory = root
                self.binding = {'account': 'x'}
                self.clock = lambda: NOW
                self.task = None

            def __call__(self, *args, **kwargs):
                return {}

        return Bridge()

    def _port(self, root, actions):
        return BridgeHostDeliveryPort(self._bridge(root, actions))

    def test_unreadable_and_malformed_indexes_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            deliveries = root / 'deliveries'
            deliveries.mkdir()
            actions = [{'id': 'manage-1', 'trade_id': 'trade-1',
                        'kind': 'protect', 'state': 'REPORTED'}]
            port = self._port(root, actions)
            for name, content in (('bad.json', '{'),
                                  ('list.json', '[]')):
                (deliveries / name).write_text(content)
                with self.assertRaises(ValueError) as raised:
                    port.legacy_management_deliveries()
                self.assertEqual(str(raised.exception),
                                 'LEGACY_HOST_IDENTITY_SCAN_FAILED')
                diagnostic = json.loads((root /
                    'legacy-identity-diagnostic.json').read_text())
                self.assertEqual(diagnostic['status'],
                                 'LEGACY_HOST_IDENTITY_SCAN_FAILED')

    def test_unknown_action_index_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'deliveries').mkdir()
            (root / 'deliveries' / 'old.json').write_text(json.dumps({
                'intent_id': 'intent-old-entry', 'action_id': 'gone',
                'trade_id': 'trade-1', 'request_id': 'old-request',
                'operation': 'place'}))
            port = self._port(root, [])
            with self.assertRaises(ValueError) as raised:
                port.legacy_management_deliveries()
            self.assertEqual(str(raised.exception),
                             'LEGACY_HOST_IDENTITY_ACTION_UNKNOWN')

    def test_scan_oserror_blocks_delivery_before_ready_action(self):
        class Book:
            def actions(self):
                return [{'id': 'manage-1', 'trade_id': 'trade-1',
                         'kind': 'protect', 'state': 'READY',
                         'payload': '{}', 'created_at': NOW}]

            def recovery_status(self):
                return {'acknowledged_action_ids': []}

            def validate_external_action(self, *args, **kwargs):
                return None

        class Host:
            def __init__(self):
                self.deliveries = []

            def legacy_management_deliveries(self):
                raise OSError('directory unreadable')

            def deliver(self, delivery):
                self.deliveries.append(delivery)
                return {'request_id': 'should-not-happen'}

            def receipt(self, intent_id):
                return None

        class Bridge:
            book = Book()
            pending = None
            task = None

            def __call__(self, *args, **kwargs):
                return {}

        host = Host()
        dispatcher = HostDeliveryDispatcher(Bridge.book, host, Bridge())
        result = dispatcher(snapshot=None, quotes=None, allow_entries=True)
        self.assertEqual(host.deliveries, [])
        self.assertEqual(dispatcher.legacy_error,
                         'LEGACY_HOST_IDENTITY_SCAN_FAILED')
        self.assertEqual(result['reason'],
                         'LEGACY_HOST_IDENTITY_SCAN_FAILED')


class LegacyHostIdentityShapeTests(TestCase):
    def _root(self, directory):
        root = Path(directory)
        (root / 'deliveries').mkdir()
        return root

    def _fixture(self, root, value, *, native=False):
        (root / 'deliveries' / 'old.json').write_text(json.dumps(value))
        if value.get('request_id'):
            _request(root, value)
        book = _Book([{'id': 'manage-1', 'trade_id': 'trade-1',
                       'kind': 'exit', 'state': 'WAITING_USER',
                       'payload': '{}', 'created_at': NOW}],
                     _intent_db('intent-old-entry', 'trade-1', native=native))
        class Bridge:
            def __init__(self):
                self.book = book
                self.directory = root
                self.binding = {'account': 'x'}
                self.clock = lambda: NOW
            def __call__(self, *args, **kwargs):
                return {}
        return BridgeHostDeliveryPort(Bridge())

    def test_missing_intent_and_unknown_intent_fail_closed(self):
        cases = (
            {'action_id': 'manage-1', 'trade_id': 'trade-1',
             'operation': 'place', 'request_id': 'req-missing'},
            {'action_id': 'manage-1', 'trade_id': 'trade-1',
             'intent_id': 'not-an-entry-or-refresh',
             'operation': 'place', 'request_id': 'req-unknown'},
        )
        for value in cases:
            with tempfile.TemporaryDirectory() as directory:
                root = self._root(directory)
                port = self._fixture(root, value)
                with self.assertRaises(ValueError) as raised:
                    port.legacy_management_deliveries()
                self.assertEqual(str(raised.exception),
                                 'LEGACY_HOST_IDENTITY_SCAN_FAILED')

    def test_invalid_operation_and_request_binding_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = self._root(directory)
            value = {'action_id': 'manage-1', 'trade_id': 'trade-1',
                     'intent_id': 'intent-old-entry',
                     'operation': 'transfer', 'request_id': 'req-operation'}
            port = self._fixture(root, value)
            with self.assertRaises(ValueError):
                port.legacy_management_deliveries()
        with tempfile.TemporaryDirectory() as directory:
            root = self._root(directory)
            value = {'action_id': 'manage-1', 'trade_id': 'trade-1',
                     'intent_id': 'intent-old-entry',
                     'operation': 'place', 'request_id': 'req-binding'}
            port = self._fixture(root, value)
            request = root / 'requests' / 'req-binding.json'
            request.write_text(json.dumps({**value, 'intent_id': 'other'}))
            with self.assertRaises(ValueError):
                port.legacy_management_deliveries()

    def test_refresh_identity_must_bind_to_native_review(self):
        with tempfile.TemporaryDirectory() as directory:
            root = self._root(directory)
            value = {'action_id': 'manage-1', 'trade_id': 'trade-1',
                     'intent_id': 'action-manage-1-review-refresh-native-refresh',
                     'operation': 'review', 'request_id': 'req-refresh'}
            port = self._fixture(root, value, native=True)
            request_id = port.request_id(value['intent_id'], 'review')
            value['request_id'] = request_id
            (root / 'deliveries' / 'old.json').unlink()
            port._index_path(value['intent_id']).write_text(json.dumps(value))
            _request(root, value)
            self.assertEqual(port.legacy_management_deliveries(), [])
        with tempfile.TemporaryDirectory() as directory:
            root = self._root(directory)
            value = {'action_id': 'manage-1', 'trade_id': 'trade-1',
                     'intent_id': 'action-manage-1-review-refresh-missing',
                     'operation': 'review', 'request_id': 'req-refresh-bad'}
            port = self._fixture(root, value, native=True)
            value['request_id'] = port.request_id(value['intent_id'], 'review')
            (root / 'deliveries' / 'old.json').unlink()
            port._index_path(value['intent_id']).write_text(json.dumps(value))
            _request(root, value)
            with self.assertRaises(ValueError):
                port.legacy_management_deliveries()


class LegacyHostReadyIsolationTests(TestCase):
    def test_legacy_index_blocks_a_ready_action_before_deliver(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'deliveries').mkdir()
            value = {
                'intent_id': 'intent-old-entry', 'request_id': 'old-request',
                'operation': 'place', 'action_id': 'manage-1',
                'trade_id': 'trade-1'}
            (root / 'deliveries' / 'old.json').write_text(json.dumps(value))
            _request(root, value)
            book = _Book([{'id': 'manage-1', 'trade_id': 'trade-1',
                           'kind': 'exit', 'state': 'READY',
                           'payload': '{}', 'created_at': NOW}],
                         _intent_db('intent-old-entry', 'trade-1'))
            class Bridge:
                def __init__(self):
                    self.book = book
                    self.directory = root
                    self.binding = {'account': 'x'}
                    self.clock = lambda: NOW
                    self.task = None
                    self.pending = None
                def __call__(self, *args, **kwargs):
                    return {}
            bridge = Bridge()
            port = BridgeHostDeliveryPort(bridge)
            delivered = []
            class Host:
                def deliver(self, value):
                    delivered.append(value)
                    return {'request_id': 'must-not-happen'}
                def receipt(self, intent_id):
                    return None
                def legacy_management_deliveries(self):
                    return port.legacy_management_deliveries()
            dispatcher = HostDeliveryDispatcher(book, Host(), bridge)
            result = dispatcher(snapshot=None, quotes=None, allow_entries=True)
            self.assertEqual(delivered, [])
            self.assertTrue(dispatcher.legacy_block)
            self.assertEqual(result['reason'],
                             'LEGACY_HOST_IDENTITY_REQUIRES_RECONCILIATION')


class RestartDispatchSafetyTests(TestCase):
    def _book(self, action, records):
        db = sqlite3.connect(':memory:', isolation_level=None)
        self.addCleanup(db.close)
        db.execute('CREATE TABLE native_operation_records('
                   'id TEXT PRIMARY KEY, action_id TEXT, operation TEXT,'
                   ' state TEXT, payload TEXT, result TEXT, created_at TEXT)')
        db.executemany('INSERT INTO native_operation_records VALUES(?,?,?,?,?,?,?)',
                       records)
        class Book:
            def __init__(self):
                self.db = db
                self._actions = [action]
            def clock(self):
                return NOW
            def actions(self):
                return self._actions
            def recovery_status(self):
                return {'acknowledged_action_ids': []}
            def validate_external_action(self, *args, **kwargs):
                return None
        return Book()

    def _host(self, deliver):
        class Host:
            def deliver(self, value):
                deliver.append(value)
                return {'request_id': 'x'}
            def receipt(self, intent_id):
                return None
            def legacy_management_deliveries(self):
                return []
        return Host()

    def _bridge(self, book):
        class Bridge:
            def __init__(self):
                self.book = book
                self.pending = None
                self.task = None
            def __call__(self, *args, **kwargs):
                return {}
        return Bridge()

    def test_unknown_action_is_never_redelivered_or_reviewed(self):
        action = {'id': 'manage-1', 'trade_id': 'trade-1', 'kind': 'exit',
                  'state': 'UNKNOWN', 'payload': '{}', 'created_at': NOW}
        book = self._book(action, [])
        delivered = []
        dispatcher = HostDeliveryDispatcher(book, self._host(delivered),
                                            self._bridge(book))
        result = dispatcher(snapshot=None, quotes=None, allow_entries=True)
        self.assertEqual(delivered, [])
        self.assertEqual(result['reason'], 'UNRESOLVED_ACTION')

    def test_abandoned_read_with_a_write_record_is_not_refreshed(self):
        action = {'id': 'manage-1', 'trade_id': 'trade-1', 'kind': 'exit',
                  'state': 'WAITING_USER', 'payload': '{}', 'created_at': NOW}
        result = json.dumps({'source': 'local_control',
                             'review_verified': False,
                             'requires_fresh_review': True,
                             'no_broker_response': True})
        book = self._book(action, [
            ('review-1', 'manage-1', 'review', 'ABANDONED_READ', '{}', result,
             NOW.isoformat()),
            ('place-1', 'manage-1', 'place', 'STARTED', '{}', None,
             NOW.isoformat())])
        delivered = []
        dispatcher = HostDeliveryDispatcher(book, self._host(delivered),
                                            self._bridge(book))
        result = dispatcher(snapshot=None, quotes=None, allow_entries=True)
        self.assertEqual(delivered, [])
        self.assertFalse(dispatcher._abandoned_read(action))


class MartinManifestUpgradeTests(TestCase):
    def _manifest(self, requirements):
        return PluginManifest(
            plugin_id='martin', version='pullback-research-0.1',
            state_schema='martin-pullback-state-1',
            config={'cost_buffer_per_share': '0.05'},
            data_requirements=tuple(requirements),
            capabilities=('advisory-entry', 'structural-stop'), max_inflight=256)

    def _budget(self):
        return InstanceBudget(max_cash=Decimal('100'),
                              max_planned_loss=Decimal('2.50'),
                              max_notional=Decimal('100'), max_concurrent=1)

    class Runner:
        @property
        def next_deadline_monotonic(self):
            return None
        def start(self, request, *, payload=None):
            pass
        def poll(self):
            return None
        def close(self):
            pass

    def test_default_group_explicitly_upgrades_quotes_and_preserves_state(self):
        with tempfile.TemporaryDirectory() as directory:
            db = sqlite3.connect(Path(directory) / 'registry.sqlite',
                                 isolation_level=None)
            self.addCleanup(db.close)
            registry = InstanceRegistry(db, clock=lambda: NOW)
            old = self._manifest(('5minute-bars', 'universe-screen'))
            registry.register('martin', old, self._budget(),
                              mode='shadow', enabled=True)
            registry.put_state('martin', {'seen': {'DEMO': 3}}, expected=None)
            before = registry.state('martin')
            book = type('Book', (), {'db': db})()
            policy = {'max_position_notional_usd': '100',
                      'max_planned_loss_per_trade_usd': '2.50',
                      'strategy_instances': [
                          {'instance_id': 'martin', 'plugin': 'martin'}]}
            group = build_instance_group(
                book, policy, source_fingerprint='a' * 64, clock=lambda: NOW,
                runner_factory=lambda cost, pin: self.Runner())
            self.addCleanup(group.close)
            upgraded = registry.instance('martin')
            self.assertEqual(upgraded.manifest.data_requirements,
                             ('quotes', '5minute-bars', 'universe-screen'))
            self.assertEqual(registry.state('martin'), before)
            events = registry.events('martin')
            self.assertEqual([(event['record_kind'], event['detail']['status']) for event in events],
                [('manifest_upgrade', 'DATA_REQUIREMENTS_ADDED')])

    def test_incompatible_manifest_change_is_still_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            db = sqlite3.connect(Path(directory) / 'registry.sqlite',
                                 isolation_level=None)
            self.addCleanup(db.close)
            registry = InstanceRegistry(db, clock=lambda: NOW)
            old = self._manifest(('5minute-bars', 'universe-screen'))
            registry.register('martin', old, self._budget(),
                              mode='shadow', enabled=True)
            wrong = PluginManifest(
                plugin_id='martin', version='other-version',
                state_schema='martin-pullback-state-1',
                config={'cost_buffer_per_share': '0.05'},
                data_requirements=('quotes', '5minute-bars', 'universe-screen'),
                capabilities=('advisory-entry', 'structural-stop'), max_inflight=256)
            with self.assertRaises(ValueError) as raised:
                registry.upgrade_data_requirements('martin', wrong, self._budget())
            self.assertEqual(str(raised.exception), 'INSTANCE_REGISTRATION_CONFLICT')


class MartinManifestCompatibilityTests(TestCase):
    def _manifest(self, requirements):
        return PluginManifest(
            plugin_id='martin', version='pullback-research-0.1',
            state_schema='martin-pullback-state-1',
            config={'cost_buffer_per_share': '0.05'},
            data_requirements=tuple(requirements),
            capabilities=('advisory-entry', 'structural-stop'), max_inflight=256)

    def _budget(self):
        return InstanceBudget(max_cash=Decimal('100'),
                              max_planned_loss=Decimal('2.50'),
                              max_notional=Decimal('100'), max_concurrent=1)

    def test_old_martin_manifest_conflict_does_not_reset_state(self):
        with tempfile.TemporaryDirectory() as directory:
            db = sqlite3.connect(Path(directory) / 'registry.sqlite',
                                 isolation_level=None)
            self.addCleanup(db.close)
            registry = InstanceRegistry(db, clock=lambda: NOW)
            old = self._manifest(('5minute-bars', 'universe-screen'))
            registry.register('martin-old', old, self._budget(),
                              mode='shadow', enabled=True)
            registry.put_state('martin-old', {'seen': {'DEMO': 3}},
                               expected=None)
            before = registry.state('martin-old')
            with self.assertRaises(ValueError) as raised:
                registry.register('martin-old',
                                  self._manifest(('quotes', '5minute-bars',
                                                  'universe-screen')),
                                  self._budget(), mode='shadow', enabled=True)
            self.assertEqual(str(raised.exception),
                             'INSTANCE_REGISTRATION_CONFLICT')
            self.assertEqual(registry.state('martin-old'), before)
            self.assertEqual(registry.instance('martin-old').manifest.digest(),
                             old.digest())
