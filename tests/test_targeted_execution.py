"""Target-aware default execution provider: named operations never advance another action."""
import json
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest import TestCase

from martin_luk.live_session import HostDeliveryDispatcher, default_live_ports

NOW = datetime(2026, 11, 2, 17, 0, tzinfo=timezone.utc)


class TargetedExecutionTests(TestCase):
    def _book(self, actions):
        db = sqlite3.connect(':memory:', isolation_level=None)
        self.addCleanup(db.close)
        db.execute('CREATE TABLE native_operation_records('
                   'id TEXT, action_id TEXT, operation TEXT, state TEXT,'
                   ' result TEXT, created_at TEXT)')
        class Book:
            def __init__(self):
                self.db = db
                self._actions = actions
            def clock(self):
                return NOW
            def actions(self):
                return self._actions
            def recovery_status(self):
                return {'acknowledged_action_ids': []}
            def validate_external_action(self, *args, **kwargs):
                return None
        return Book(), db

    def _bridge(self, root, book):
        class Bridge:
            def __init__(self):
                self.book = book
                self.directory = root
                self.binding = {'db': str(root / 'book.sqlite'),
                                'account': '••••0000', 'account_key': 'acct',
                                'policy_sha256': 'policy', 'epoch': 'epoch'}
                self.clock = lambda: NOW
                self.pending = None
                self.task = None
                self.target_calls = []
            def __call__(self, *args, **kwargs):
                self.target_calls.append(kwargs.get('target_request_id'))
                return {'agent_called': False}
        return Bridge()

    def _ports(self, book, bridge):
        policy = {'account_suffix': '0000', 'max_position_notional_usd': '100',
                  'max_planned_loss_per_trade_usd': '2.50'}
        return default_live_ports(
            book, policy, lambda: NOW, host_surface=bridge,
            broker_surface=lambda approval_ids=(): {'account_key': 'acct',
                'observed_at': NOW.isoformat(), 'orders': {},
                'collection_complete': True},
            market_surface=lambda *args, **kwargs: ([], {'quotes': 0}))

    def test_unverified_or_mismatched_receipt_is_not_reported_imported(self):
        for receipt in (
                {'verified': False, 'status': 'HOST_RECEIPT_UNVERIFIED',
                 'reason': 'HOST_RECEIPT_IDENTITY_MISMATCH'},
                {'verified': True, 'status': 'HOST_RECEIPT_IMPORTED',
                 'reason': None, 'request_id': 'wrong',
                 'action_id': 'exit-A', 'trade_id': 'trade-A'}):
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                book, _ = self._book([
                    {'id': 'exit-A', 'trade_id': 'trade-A', 'kind': 'exit',
                     'state': 'READY', 'payload': '{}', 'created_at': NOW}])
                bridge = self._bridge(root, book)
                class Host:
                    def legacy_management_deliveries(self):
                        return []
                    def deliver(self, intent):
                        return {'request_id': 'request-A'}
                    def receipt(self, intent_id):
                        return receipt
                dispatcher = HostDeliveryDispatcher(book, Host(), bridge)
                result = dispatcher.operate(
                    'exit-A', 'review',
                    {'action_id': 'exit-A', 'snapshot': {}, 'quotes': {}})
                self.assertEqual(result['host_status'], 'HOST_RECEIPT_UNVERIFIED',
                                 result)
                self.assertFalse(result['verified'])

    def test_unknown_state_is_refused_without_delivery(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            book, _ = self._book([
                {'id': 'exit-A', 'trade_id': 'trade-A', 'kind': 'exit',
                 'state': 'UNKNOWN', 'payload': '{}', 'created_at': NOW}])
            bridge = self._bridge(root, book)
            ports = self._ports(book, bridge)
            with self.assertRaises(ValueError) as raised:
                ports.execution.review({'action_id': 'exit-A'})
            self.assertEqual(str(raised.exception),
                             'ADAPTER_TARGET_REVIEW_STATE_INVALID')
            self.assertFalse((root / 'requests').exists())

    def test_snapshot_processing_is_scoped_to_the_target_request(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            book, _ = self._book([
                {'id': 'exit-A', 'trade_id': 'trade-A', 'kind': 'exit',
                 'state': 'READY', 'payload': '{}', 'created_at': NOW}])
            bridge = self._bridge(root, book)
            ports = self._ports(book, bridge)
            result = ports.execution.review({
                'action_id': 'exit-A', 'snapshot': {'synthetic': True},
                'quotes': {}})
            self.assertEqual(bridge.target_calls, [result['request_id']])
            self.assertTrue(result['snapshot_supplied'])
            self.assertFalse(result['fresh_snapshot_verified'])

    def test_existing_bridge_pending_blocks_target_without_delivery(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            book, _ = self._book([
                {'id': 'exit-A', 'trade_id': 'trade-A', 'kind': 'exit',
                 'state': 'READY', 'payload': '{}', 'created_at': NOW}])
            bridge = self._bridge(root, book)
            bridge.pending = {'packet': {'action_id': 'other'}}
            ports = self._ports(book, bridge)
            with self.assertRaises(ValueError) as raised:
                ports.execution.review({'action_id': 'exit-A'})
            self.assertEqual(str(raised.exception), 'HOST_OPERATION_IN_FLIGHT')
            self.assertFalse((root / 'requests').exists())

    def test_duplicate_target_delivery_reuses_one_request(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            book, _ = self._book([
                {'id': 'exit-A', 'trade_id': 'trade-A', 'kind': 'exit',
                 'state': 'READY', 'payload': '{}', 'created_at': NOW}])
            bridge = self._bridge(root, book)
            ports = self._ports(book, bridge)
            first = ports.execution.review({'action_id': 'exit-A'})
            second = ports.execution.review({'action_id': 'exit-A'})
            self.assertEqual(first['request_id'], second['request_id'])
            self.assertEqual(len(list((root / 'requests').glob('*.json'))), 1)

    def test_retired_cancel_ref_is_refused_without_delivery(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            book, _ = self._book([
                {'id': 'cancel-A', 'trade_id': 'trade-A', 'kind': 'cancel',
                 'state': 'READY',
                 'payload': json.dumps({'parameters': {'order_id': 'order-A'}}),
                 'created_at': NOW}])
            book.recovery_status = lambda: {'acknowledged_action_ids': ['cancel-A']}
            bridge = self._bridge(root, book)
            ports = self._ports(book, bridge)
            with self.assertRaises(ValueError) as raised:
                ports.execution.cancel('order-A')
            self.assertEqual(str(raised.exception), 'ADAPTER_TARGET_ACTION_RETIRED')
            self.assertFalse((root / 'requests').exists())

    def test_review_targets_only_the_named_action(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            actions = [
                {'id': 'exit-A', 'trade_id': 'trade-A', 'kind': 'exit',
                 'state': 'READY', 'payload': '{}', 'created_at': NOW},
                {'id': 'exit-B', 'trade_id': 'trade-B', 'kind': 'exit',
                 'state': 'READY', 'payload': '{}', 'created_at': NOW},
            ]
            book, _ = self._book(actions)
            bridge = self._bridge(root, book)
            ports = self._ports(book, bridge)
            result = ports.execution.review({'action_id': 'exit-A'})
            self.assertEqual(result['status'], 'HOST_DELIVERY_RECORDED')
            self.assertEqual(result['action_id'], 'exit-A')
            requests = list((root / 'requests').glob('*.json'))
            self.assertEqual(len(requests), 1)
            request = json.loads(requests[0].read_text())
            self.assertEqual(request['action_id'], 'exit-A')
            self.assertEqual(request['operation'], 'review')
            self.assertNotEqual(request['action_id'], 'exit-B')

    def test_cancel_ref_resolves_the_owning_cancel_action(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            payload = json.dumps({'parameters': {'order_id': 'order-A'}})
            actions = [
                {'id': 'cancel-A', 'trade_id': 'trade-A', 'kind': 'cancel',
                 'state': 'READY', 'payload': payload, 'created_at': NOW},
                {'id': 'cancel-B', 'trade_id': 'trade-B', 'kind': 'cancel',
                 'state': 'READY',
                 'payload': json.dumps({'parameters': {'order_id': 'order-B'}}),
                 'created_at': NOW},
            ]
            book, _ = self._book(actions)
            bridge = self._bridge(root, book)
            ports = self._ports(book, bridge)
            result = ports.execution.cancel('order-A')
            self.assertEqual(result['action_id'], 'cancel-A')
            request = json.loads(next((root / 'requests').glob('*.json')).read_text())
            self.assertEqual(request['action_id'], 'cancel-A')

    def test_unknown_or_ambiguous_cancel_ref_is_refused_without_delivery(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            book, _ = self._book([
                {'id': 'cancel-A', 'trade_id': 'trade-A', 'kind': 'cancel',
                 'state': 'READY',
                 'payload': json.dumps({'parameters': {'order_id': 'same'}}),
                 'created_at': NOW},
                {'id': 'cancel-B', 'trade_id': 'trade-B', 'kind': 'cancel',
                 'state': 'READY',
                 'payload': json.dumps({'parameters': {'order_id': 'same'}}),
                 'created_at': NOW}])
            bridge = self._bridge(root, book)
            ports = self._ports(book, bridge)
            with self.assertRaises(ValueError) as raised:
                ports.execution.cancel('same')
            self.assertEqual(str(raised.exception),
                             'ADAPTER_CANCEL_TARGET_AMBIGUOUS')
            with self.assertRaises(ValueError):
                ports.execution.cancel('missing')
            self.assertFalse((root / 'requests').exists())

    def test_place_requires_the_exact_review_and_does_not_touch_another_action(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            book, db = self._book([
                {'id': 'exit-A', 'trade_id': 'trade-A', 'kind': 'exit',
                 'state': 'WAITING_USER', 'payload': '{}', 'created_at': NOW},
                {'id': 'exit-B', 'trade_id': 'trade-B', 'kind': 'exit',
                 'state': 'WAITING_USER', 'payload': '{}', 'created_at': NOW}])
            db.execute("INSERT INTO native_operation_records VALUES"
                       "('review-A','exit-A','review','RECORDED',? ,?)",
                       (json.dumps({'review_verified': True}), NOW.isoformat()))
            bridge = self._bridge(root, book)
            ports = self._ports(book, bridge)
            result = ports.execution.place({'action_id': 'exit-A'})
            self.assertEqual(result['action_id'], 'exit-A')
            self.assertEqual(result['operation'], 'place')
            request = json.loads(next((root / 'requests').glob('*.json')).read_text())
            self.assertEqual(request['action_id'], 'exit-A')
            with self.assertRaises(ValueError) as raised:
                ports.execution.place({'action_id': 'exit-B'})
            self.assertEqual(str(raised.exception),
                             'ADAPTER_TARGET_PLACE_REQUIRES_REVIEW')
            self.assertEqual(len(list((root / 'requests').glob('*.json'))), 1)
