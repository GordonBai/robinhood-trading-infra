"""Offline tests for owner intent receipts and risk-lineage readers."""
from datetime import datetime, timedelta
import json
from pathlib import Path
import sqlite3
import tempfile
from unittest import TestCase
from unittest.mock import patch

from martin_agentic.audit import digest
from martin_agentic.broker import BrokerJournal
from martin_agentic.owner_engineering_source import EngineeringOwnerSource
from martin_agentic.qualification import source_fingerprint
from martin_agentic.session_lineage import (
    bootstrap_session_lineage, enroll_session_lineage, lineage_entry_gate)
from entry_authority_fixture import TestExecutionBook
from test_broker import FakeBroker, snapshot
import test_engineering_source as matrix

NOW = matrix.NOW


class OwnerReceiptTests(TestCase):
    def _fixture(self):
        fixture = matrix.RealDryFixture()
        self.addCleanup(fixture.close)
        return fixture

    def _receipt(self, fixture, *, host_sha='a' * 64, signal_id=None,
                 audit=True):
        signal_id = signal_id or json.loads(
            fixture.request_bytes())['signal_id']
        action = fixture.book.actions()[0]
        event_id = 'host-engineering-intent-host-receipt-test'
        source = EngineeringOwnerSource(fixture.owner, clock=lambda: NOW)
        db_path = fixture.book.db.execute(
            'PRAGMA database_list').fetchone()[2]
        binding = {
            'db': str(Path(db_path).resolve()), 'account': '••••0000',
            'account_key': fixture.account_key,
            'policy_sha256': digest(fixture.policy),
            'source_fingerprint': source_fingerprint(), 'epoch': 'fixture'}
        request = json.loads(fixture.request_bytes())
        receipt = source.build_receipt(
            request, host_request_id='host-receipt-test',
            host_request_sha256=host_sha, signal_id=signal_id,
            action=action, host_binding=binding,
            audit_event_id=event_id, received_at=NOW)
        if audit:
            fixture.book.audit.append({
                'event_id': event_id, 'occurred_at': NOW.isoformat(),
                'event_type': 'RECONCILIATION_ISSUE', 'actor': 'script',
                'synthetic': False, 'trade_id': signal_id,
                'details': {'status': 'HOST_ENGINEERING_INTENT_RECORDED',
                            'host_request_sha256': host_sha,
                            'owner_receipt': receipt,
                            'host_result': {'action_id': action['id'],
                                            'signal_id': signal_id}}})
        source.write_receipt(receipt)
        return source, receipt

    def test_receipt_binds_exact_owner_request_and_audit(self):
        fixture = self._fixture()
        source, receipt = self._receipt(fixture)
        self.assertEqual(receipt['request_sha256'],
                         digest(json.loads(fixture.request_bytes())))
        fixture.configure(provenance=source.provenance_reader)
        decision = fixture.dry()
        self.assertTrue(decision['allowed'], decision)
        self.assertFalse(decision['execution_authorized'])
        self.assertFalse(decision['entry_authorized'])

    def test_default_owner_request_calls_dry_diagnostic_and_does_not_execute(self):
        fixture = matrix.RealDryFixture(lineage=True)
        self.addCleanup(fixture.close)
        self._receipt(fixture)
        from martin_agentic.host_execution import HostExecutionBridge
        from martin_agentic.host_mailbox import submit_engineering_dry_assess
        bridge = HostExecutionBridge(
            fixture.book, Path(tempfile.mkdtemp()) / 'host',
            engineering_owner_root=fixture.owner,
            engineering_dry_enabled=True,
            engineering_dry_provenance_reader=fixture.provenance_reader,
            engineering_dry_risk_reader=fixture.risk_reader)
        self.addCleanup(bridge.close)
        request = submit_engineering_dry_assess(
            bridge.directory, fixture.action_id, clock=lambda: NOW)
        before = fixture.snapshot()
        output = bridge(snapshot=fixture.snap, quotes={'DEMO': fixture._quote()},
                        allow_entries=False)
        self.assertEqual(output['status'], 'HOST_ENGINEERING_DRY_ASSESSED',
                         output)
        self.assertEqual(output['decision']['decision_scope'], 'DRY_ASSESSMENT')
        self.assertTrue(output['decision']['allowed'], output)
        self.assertFalse(output['execution_authorized'])
        self.assertFalse(output['entry_authorized'])
        fixture.assert_unchanged(before, 'host-request-dry')
        result_file = bridge.directory / 'results' / (
            request['request_id'] + '.json')
        self.assertTrue(result_file.exists())

    def test_default_owner_dry_request_is_disabled_by_default(self):
        fixture = matrix.RealDryFixture(lineage=True)
        self.addCleanup(fixture.close)
        from martin_agentic.host_execution import HostExecutionBridge
        from martin_agentic.host_mailbox import submit_engineering_dry_assess
        bridge = HostExecutionBridge(
            fixture.book, Path(tempfile.mkdtemp()) / 'host',
            engineering_owner_root=fixture.owner)
        self.addCleanup(bridge.close)
        submit_engineering_dry_assess(
            bridge.directory, fixture.action_id, clock=lambda: NOW)
        before = fixture.snapshot()
        output = bridge(snapshot=fixture.snap, quotes={'DEMO': fixture._quote()},
                        allow_entries=False)
        self.assertNotEqual(output.get('status'), 'HOST_ENGINEERING_DRY_ASSESSED')
        self.assertEqual(output.get('reason'), 'HOST_ENGINEERING_DRY_DISABLED')
        fixture.assert_unchanged(before, 'host-request-dry-disabled')

    def test_host_dry_missing_owner_receipt_is_named_without_writes(self):
        fixture = matrix.RealDryFixture(lineage=True)
        self.addCleanup(fixture.close)
        from martin_agentic.host_execution import HostExecutionBridge
        bridge = HostExecutionBridge(
            fixture.book, Path(tempfile.mkdtemp()) / 'host',
            engineering_owner_root=fixture.owner)
        self.addCleanup(bridge.close)
        before = fixture.snapshot()
        decision = bridge.run_engineering_source_dry_assess(
            fixture.action_id, enabled=True, review_only=True)
        self.assertFalse(decision['allowed'])
        self.assertTrue(any('OWNER_PROVENANCE_MISSING' in reason
                            for reason in decision['reasons']), decision)
        self.assertFalse(decision['entry_authorized'])
        fixture.assert_unchanged(before, 'host-dry-missing')
        self.assertEqual(fixture.consumptions(), [])

    def test_host_owner_dry_diagnostic_default_off_and_no_writes(self):
        fixture = matrix.RealDryFixture(lineage=True)
        self.addCleanup(fixture.close)
        source, receipt = self._receipt(fixture)
        from martin_agentic.host_execution import HostExecutionBridge
        directory = Path(tempfile.mkdtemp()) / 'host'
        self.addCleanup(lambda: None)
        bridge = HostExecutionBridge(fixture.book, directory,
                                     engineering_owner_root=fixture.owner)
        self.addCleanup(bridge.close)
        with self.assertRaisesRegex(ValueError,
                                    'ENGINEERING_DRY_DIAGNOSTIC_DISABLED'):
            bridge.run_engineering_source_dry_assess(fixture.action_id)
        before = fixture.snapshot()
        decision = bridge.run_engineering_source_dry_assess(
            fixture.action_id, enabled=True, review_only=True,
            provenance_reader=fixture.provenance_reader,
            risk_lineage_reader=fixture.risk_reader)
        self.assertTrue(decision['allowed'], decision)
        self.assertEqual(decision['decision_scope'], 'DRY_ASSESSMENT')
        self.assertFalse(decision['entry_authorized'])
        self.assertFalse(decision['execution_authorized'])
        fixture.assert_unchanged(before, 'host-dry')
        self.assertEqual(fixture.consumptions(), [])
        with self.assertRaises(ValueError):
            fixture.book.promote_supervised_entry(fixture.action_id)

    def test_prepare_expiry_after_reservation_does_not_publish(self):
        from martin_agentic.host_execution import HostExecutionBridge
        from martin_agentic.host_mailbox import (
            submit_engineering_intent, submit_request)
        from martin_agentic.native_operations import (
            NativeOperations as _NativeOperations)
        with matrix.RealDryFixture(lineage=True,
                                   prebuilt_intent=False) as fixture:
            with patch('martin_agentic.session_lineage.CANONICAL_RUNTIME',
                       fixture.root):
                state = fixture.book.state()
                state['account_key'] = fixture.account_key
                fixture.book._save(state)
                fixture.book.db.commit()
                bridge = HostExecutionBridge(
                    fixture.book, fixture.root / 'host-expiry',
                    engineering_owner_root=fixture.owner)
                try:
                    submit_engineering_intent(
                        bridge.directory, 'engineering-real-dry',
                        fixture._plan(), {
                            'synthetic': False, 'source_sha256': 'fixture',
                            'engineering_scope_authorized': True,
                            'parameters_user_specified': True,
                            'parameters_valid_until': (
                                NOW + timedelta(minutes=30)).isoformat()},
                        clock=lambda: NOW)
                    out = bridge(snapshot=fixture.snap,
                                 quotes={'DEMO': fixture._quote()},
                                 allow_entries=False)
                    self.assertEqual(out['status'],
                                     'HOST_ENGINEERING_INTENT_RECORDED', out)
                    review_request = submit_request(
                        bridge.directory, out['action_id'], 'review',
                        clock=lambda: NOW)
                    real_begin = _NativeOperations.begin

                    def expired_begin(ops_self, *args, **kwargs):
                        packet = real_begin(ops_self, *args, **kwargs)
                        fixture.advance(30)
                        return packet

                    with patch.object(_NativeOperations, 'begin',
                                      expired_begin):
                        result = bridge(
                            snapshot=fixture.snap,
                            quotes={'DEMO': fixture._quote()},
                            allow_entries=False)
                    self.assertNotEqual(result['status'],
                                        'HOST_PACKET_PREPARED', result)
                    self.assertFalse((bridge.directory / 'packets' /
                                      (review_request['request_id'] +
                                       '.json')).exists())
                    self.assertIsNone(bridge.pending)
                finally:
                    bridge.close()

    def test_post_begin_clock_backwards_settles_reservation(self):
        from martin_agentic.host_execution import HostExecutionBridge
        from martin_agentic.host_mailbox import (
            submit_engineering_intent, submit_request)
        from martin_agentic.native_operations import (
            NativeOperations as _NativeOperations)
        from martin_agentic.control_tasks import worker_lease as _worker_lease
        with matrix.RealDryFixture(lineage=True,
                                   prebuilt_intent=False) as fixture:
            with patch('martin_agentic.session_lineage.CANONICAL_RUNTIME',
                       fixture.root):
                state = fixture.book.state()
                state['account_key'] = fixture.account_key
                fixture.book._save(state)
                fixture.book.db.commit()
                bridge = HostExecutionBridge(
                    fixture.book, fixture.root / 'host-backwards',
                    engineering_owner_root=fixture.owner)
                handles = []
                try:
                    submit_engineering_intent(
                        bridge.directory, 'engineering-real-dry',
                        fixture._plan(), {
                            'synthetic': False, 'source_sha256': 'fixture',
                            'engineering_scope_authorized': True,
                            'parameters_user_specified': True,
                            'parameters_valid_until': (
                                NOW + timedelta(minutes=30)).isoformat()},
                        clock=lambda: NOW)
                    out = bridge(snapshot=fixture.snap,
                                 quotes={'DEMO': fixture._quote()},
                                 allow_entries=False)
                    self.assertEqual(out['status'],
                                     'HOST_ENGINEERING_INTENT_RECORDED', out)
                    review_request = submit_request(
                        bridge.directory, out['action_id'], 'review',
                        clock=lambda: NOW)
                    real_begin = _NativeOperations.begin

                    def backwards_begin(ops_self, *args, **kwargs):
                        packet = real_begin(ops_self, *args, **kwargs)
                        fixture.advance(-0.001)
                        return packet

                    def lease_spy(path):
                        handle = _worker_lease(path)
                        handles.append(handle)
                        return handle

                    with patch.object(_NativeOperations, 'begin',
                                      backwards_begin), patch(
                            'martin_agentic.host_execution.worker_lease',
                            side_effect=lease_spy):
                        result = bridge(
                            snapshot=fixture.snap,
                            quotes={'DEMO': fixture._quote()},
                            allow_entries=False)
                    self.assertNotEqual(result['status'],
                                        'HOST_PACKET_PREPARED', result)
                    self.assertIsNone(bridge.pending)
                    self.assertTrue(handles[-1].closed)
                    reacquired = _worker_lease(fixture.book.db.execute(
                        'PRAGMA database_list').fetchone()[2])
                    self.assertIsNotNone(reacquired)
                    reacquired.close()
                    row = fixture.book.db.execute(
                        "SELECT state FROM native_operation_records "
                        "WHERE operation='review' ORDER BY rowid DESC LIMIT 1"
                    ).fetchone()
                    self.assertEqual(row[0], 'ABANDONED_READ')
                    self.assertFalse((bridge.directory / 'packets' /
                                      (review_request['request_id'] +
                                       '.json')).exists())
                finally:
                    bridge.close()

    def test_formal_review_ingress_scripted_and_guards(self):
        from martin_agentic.formal_review_ingress import (
            FormalReviewIngress, FormalReviewIngressError)
        from martin_agentic.host_execution import HostExecutionBridge
        from martin_agentic.host_mailbox import (
            read_private_json, submit_engineering_intent, submit_request)
        class Scripted:
            simulation_only = True
            tools = {'review_equity_order'}
            last_call_id = 'scripted-call-1'
            def execute_review(self, packet):
                return {'data': {
                    **packet['parameters'], 'order_checks': {},
                    'customer_approval_required': False,
                    'market_data_disclosure': 'Exact disclosure',
                    'quote_data': {}}}
        with matrix.RealDryFixture(lineage=True,
                                   prebuilt_intent=False) as fixture:
            with patch('martin_agentic.session_lineage.CANONICAL_RUNTIME',
                       fixture.root):
                state = fixture.book.state()
                state['account_key'] = fixture.account_key
                fixture.book._save(state)
                fixture.book.db.commit()
                bridge = HostExecutionBridge(
                    fixture.book, fixture.root / 'host-ingress',
                    engineering_owner_root=fixture.owner)
                try:
                    submit_engineering_intent(
                        bridge.directory, 'engineering-real-dry',
                        fixture._plan(), {
                            'synthetic': False, 'source_sha256': 'fixture',
                            'engineering_scope_authorized': True,
                            'parameters_user_specified': True,
                            'parameters_valid_until': (
                                NOW + timedelta(minutes=30)).isoformat()},
                        clock=lambda: NOW)
                    out = bridge(snapshot=fixture.snap,
                                 quotes={'DEMO': fixture._quote()},
                                 allow_entries=False)
                    review_request = submit_request(
                        bridge.directory, out['action_id'], 'review',
                        clock=lambda: NOW)
                    bridge(snapshot=fixture.snap,
                           quotes={'DEMO': fixture._quote()},
                           allow_entries=False)
                    host_packet = read_private_json(
                        bridge.directory / 'packets' /
                        (review_request['request_id'] + '.json'))['packet']
                    adapter = FormalReviewIngress(
                        fixture.root / 'ingress', clock=lambda: NOW)
                    class CountingScripted(Scripted):
                        calls = 0
                        def execute_review(self, packet):
                            self.calls += 1
                            return super().execute_review(packet)
                    counting = CountingScripted()
                    record = adapter.receive(
                        transport=counting, packet=host_packet,
                        request_id=review_request['request_id'],
                        host_directory=bridge.directory,
                        binding=bridge.binding)
                    self.assertEqual(record['origin'],
                                     'test_scripted_transport')
                    again = adapter.receive(
                        transport=counting, packet=host_packet,
                        request_id=review_request['request_id'],
                        host_directory=bridge.directory,
                        binding=bridge.binding)
                    self.assertEqual(again, record)
                    self.assertEqual(counting.calls, 1)
                    class FakeFormal(Scripted):
                        simulation_only = False
                        tool_descriptors = {
                            'review_equity_order': {'name': 'fake'}}
                        last_call_id = 'fake-call'
                        calls = 0
                        def execute_review(self, packet):
                            self.calls += 1
                            return super().execute_review(packet)
                    fake = FakeFormal()
                    with self.assertRaisesRegex(
                            FormalReviewIngressError,
                            'TRANSPORT_NOT_FORMAL'):
                        adapter._formal(fake)
                    self.assertEqual(fake.calls, 0)
                    self.assertFalse(record['execution_authorized'])
                    imported = bridge(allow_dispatch=False)
                    self.assertEqual(imported['status'],
                                     'HOST_RECEIPT_IMPORTED', imported)
                    row = fixture.book.db.execute(
                        "SELECT result FROM native_operation_records "
                        "WHERE operation='review' ORDER BY rowid DESC LIMIT 1"
                    ).fetchone()
                    fixture.adopt_review(out['action_id'], host_packet,
                                         json.loads(row[0]))
                    fixture.book.configure_engineering_source(
                        fixture.owner, owner_source_root=fixture.owner,
                        native_receipt_root=bridge.directory,
                        native_ingress_root=fixture.root / 'ingress')
                    blocked = fixture.dry()
                    self.assertFalse(blocked['allowed'], blocked)
                    self.assertTrue(any('NATIVE_RECEIPT' in reason
                                        for reason in blocked['reasons']),
                                    blocked['reasons'])
                    with self.assertRaisesRegex(
                            FormalReviewIngressError,
                            'FORMAL_INGRESS_REVIEW_ONLY'):
                        adapter.receive(
                            transport=Scripted(),
                            packet={**host_packet,
                                    'operation': 'place',
                                    'tool': 'place_equity_order'},
                            request_id=review_request['request_id'],
                            host_directory=bridge.directory,
                            binding=bridge.binding)
                    class MissingDescriptor(Scripted):
                        simulation_only = False
                        tool_descriptors = {}
                    with self.assertRaisesRegex(
                            FormalReviewIngressError,
                            '(TOOL_DESCRIPTOR_UNAVAILABLE|TRANSPORT_NOT_FORMAL)'):
                        adapter._formal(MissingDescriptor())
                finally:
                    bridge.close()

    def test_formal_ingress_journal_recovery_after_receipt_failure(self):
        from pathlib import Path as _Path
        from martin_agentic.formal_review_ingress import FormalReviewIngress
        from martin_agentic.host_mailbox import write_private_json
        import martin_agentic.formal_review_ingress as ingress_module
        from martin_agentic.host_mailbox import submit_receipt as real_submit
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = _Path(temp.name)
        host = root / 'host'
        packet = {
            'operation_id': 'native-00000000-0000-4000-8000-000000000002', 'action_id': 'action-1',
            'operation': 'review', 'tool': 'review_equity_order',
            'account': '••••0000', 'account_key': 'a' * 64,
            'logical_ref_id': 'ref-1',
            'parameters': {'symbol': 'DEMO', 'side': 'buy', 'type': 'limit',
                           'quantity': '2', 'limit_price': '40',
                           'time_in_force': 'gfd', 'market_hours': 'regular_hours'}}
        binding = {'db': str((root / 'book.sqlite').resolve()),
                   'account': '••••0000', 'account_key': 'a' * 64,
                   'policy_sha256': 'b' * 64,
                   'source_fingerprint': 'c' * 64, 'epoch': '00000000-0000-4000-8000-000000000003'}
        request_id = '00000000-0000-4000-8000-000000000001'
        write_private_json(host / 'packets' / (request_id + '.json'), {
            'schema_version': 1, 'status': 'PREPARED',
            'request_id': request_id, 'binding': binding,
            'packet': packet, 'packet_sha256': digest(packet),
            'prepared_at': NOW.isoformat(),
            'expires_at': (NOW + timedelta(minutes=1)).isoformat(),
            'execution_authorized': False, 'broker_effect_verified': False})
        write_private_json(host / 'requests' / (request_id + '.json'), {
            'request_id': request_id, 'action_id': 'action-1',
            'operation': 'review', 'binding': binding})
        class Scripted:
            simulation_only = True
            last_call_id = 'scripted-journal-call'
            calls = 0
            def execute_review(self, packet):
                self.calls += 1
                return {'data': {
                    **packet['parameters'], 'order_checks': {},
                    'customer_approval_required': False,
                    'market_data_disclosure': 'Exact disclosure',
                    'quote_data': {}}}
        clock_value = [NOW]
        adapter = FormalReviewIngress(root / 'ingress',
                                      clock=lambda: clock_value[0])
        transport = Scripted()
        def write_then_fail(directory, request_id, response, clock=None):
            real_submit(directory, request_id, response, clock=clock)
            raise OSError('injected receipt write failure')
        with patch.object(ingress_module, 'submit_receipt',
                          side_effect=write_then_fail):
            with self.assertRaisesRegex(ValueError,
                                        'RECEIPT_PERSISTENCE_FAILED'):
                adapter.receive(
                    transport=transport, packet=packet,
                    request_id=request_id, host_directory=host,
                    binding=binding)
        clock_value[0] = NOW + timedelta(seconds=1)
        recovered = adapter.receive(
            transport=transport, packet=packet, request_id=request_id,
            host_directory=host, binding=binding)
        self.assertEqual(recovered['status'], 'received')
        self.assertEqual(recovered['received_at'], NOW.isoformat())
        self.assertFalse(recovered['historical_recovery'])
        self.assertEqual(transport.calls, 1)
        ingress_path = root / 'ingress' / (packet['operation_id'] + '.json')
        journal_path = root / 'ingress' / (packet['operation_id'] +
                                           '.journal.json')
        receipt_path = host / 'receipts' / (packet['operation_id'] + '.json')
        original_receipt = receipt_path.read_bytes()
        ingress_path.unlink()
        bad_journal = json.loads(journal_path.read_text())
        bad_journal['action_id'] = 'wrong-action'
        journal_path.write_text(json.dumps(bad_journal, sort_keys=True))
        journal_path.chmod(0o600)
        with self.assertRaisesRegex(ValueError, 'JOURNAL_INVALID'):
            adapter.receive(
                transport=transport, packet=packet, request_id=request_id,
                host_directory=host, binding=binding)
        self.assertEqual(receipt_path.read_bytes(), original_receipt)
        self.assertEqual(transport.calls, 1)

    def test_receipt_binding_and_time_tamper_blocks(self):
        fixture = self._fixture()
        source, receipt = self._receipt(fixture)
        fixture.configure(provenance=source.provenance_reader)
        path = source._receipt_path(receipt['request_id'])
        for changes in (
                {'host_binding': {**receipt['host_binding'],
                                  'db': '/other/database.sqlite'}},
                {'host_binding': {**receipt['host_binding'],
                                  'policy_sha256': 'f' * 64}},
                {'received_at': (NOW + timedelta(days=1)).isoformat()}):
            with self.subTest(changes=tuple(changes)):
                value = {**receipt, **changes}
                path.write_text(json.dumps(value, sort_keys=True))
                path.chmod(0o600)
                self.assertFalse(fixture.dry()['allowed'])
        path.write_text(json.dumps(receipt, sort_keys=True))
        path.chmod(0o600)

    def test_legacy_audit_without_owner_receipt_blocks(self):
        fixture = self._fixture()
        source = EngineeringOwnerSource(fixture.owner, clock=lambda: NOW)
        action = fixture.book.actions()[0]
        request = json.loads(fixture.request_bytes())
        event_id = 'host-engineering-intent-host-receipt-test'
        fixture.book.audit.append({
            'event_id': event_id, 'occurred_at': NOW.isoformat(),
            'event_type': 'RECONCILIATION_ISSUE', 'actor': 'script',
            'synthetic': False, 'trade_id': request['signal_id'],
            'details': {'status': 'HOST_ENGINEERING_INTENT_RECORDED',
                        'host_request_sha256': 'a' * 64,
                        'host_result': {'action_id': action['id'],
                                        'signal_id': request['signal_id']}}})
        db_path = fixture.book.db.execute(
            'PRAGMA database_list').fetchone()[2]
        receipt = source.build_receipt(
            request, host_request_id='host-receipt-test',
            host_request_sha256='a' * 64,
            signal_id=request['signal_id'], action=action,
            host_binding={
                'db': str(Path(db_path).resolve()), 'account': '••••0000',
                'account_key': fixture.account_key,
                'policy_sha256': digest(fixture.policy),
                'source_fingerprint': source_fingerprint(),
                'epoch': 'fixture'}, audit_event_id=event_id,
            received_at=NOW)
        source.write_receipt(receipt)
        fixture.configure(provenance=source.provenance_reader)
        decision = fixture.dry()
        self.assertFalse(decision['allowed'], decision)

    def test_record_intent_cannot_rebind_changed_file_after_audit(self):
        fixture = self._fixture()
        source, receipt = self._receipt(fixture)
        value = json.loads(fixture.request_bytes())
        value['valid_until'] = (NOW + timedelta(minutes=10)).isoformat()
        fixture.owner.joinpath(
            'supervised-engineering-request.json').write_text(
                json.dumps(value, sort_keys=True))
        source._receipt_path(receipt['request_id']).unlink()
        with self.assertRaisesRegex(ValueError, 'OWNER_RECEIPT_AUDIT_BINDING'):
            source.record_intent(
                fixture.book,
                host_request_id=receipt['host_request_id'],
                host_request_sha256=receipt['host_request_sha256'],
                signal_id=receipt['signal_id'],
                action=fixture.book.actions()[0],
                host_binding=receipt['host_binding'],
                audit_event_id=receipt['audit_event_id'],
                received_at=NOW)

    def test_missing_receipt_blocks(self):
        fixture = self._fixture()
        source = EngineeringOwnerSource(fixture.owner, clock=lambda: NOW)
        permit = fixture.read_permit()
        request = json.loads(fixture.request_bytes())
        with self.assertRaisesRegex(ValueError, 'OWNER_RECEIPT_MISSING'):
            source.provenance_reader(
                fixture.book, {'permit': {'permit': permit},
                               'request': {'request_id':
                                           request['request_id'],
                                           'request_sha256':
                                           digest(request)}})
        fixture.configure(provenance=source.provenance_reader)
        before = fixture.snapshot()
        decision = fixture.dry()
        self.assertFalse(decision['allowed'], decision)
        fixture.assert_unchanged(before, 'missing-receipt')

    def test_receipt_body_or_audit_tamper_blocks(self):
        fixture = self._fixture()
        source, receipt = self._receipt(fixture)
        fixture.configure(provenance=source.provenance_reader)
        path = source._receipt_path(receipt['request_id'])
        value = json.loads(path.read_text())
        value['request_sha256'] = 'b' * 64
        path.write_text(json.dumps(value, sort_keys=True))
        self.assertFalse(fixture.dry()['allowed'])
        value['request_sha256'] = receipt['request_sha256']
        value['audit_event_id'] = 'missing-audit-event'
        path.write_text(json.dumps(value, sort_keys=True))
        self.assertFalse(fixture.dry()['allowed'])

    def test_actual_host_intent_to_receipt_to_dry(self):
        from martin_agentic.host_execution import HostExecutionBridge
        from martin_agentic.host_mailbox import submit_engineering_intent
        with matrix.RealDryFixture(lineage=True,
                                   prebuilt_intent=False) as fixture:
            with patch('martin_agentic.session_lineage.CANONICAL_RUNTIME',
                       fixture.root):
                state = fixture.book.state()
                state['account_key'] = fixture.account_key
                fixture.book._save(state)
                fixture.book.db.commit()
                bridge = HostExecutionBridge(
                    fixture.book, fixture.root / 'host',
                    engineering_owner_root=fixture.owner)
                try:
                    request = submit_engineering_intent(
                        bridge.directory, 'engineering-real-dry',
                        fixture._plan(), {
                            'synthetic': False, 'source_sha256': 'fixture',
                            'engineering_scope_authorized': True,
                            'parameters_user_specified': True,
                            'parameters_valid_until': (
                                NOW + timedelta(minutes=30)).isoformat()},
                        clock=lambda: NOW)
                    out = bridge(snapshot=fixture.snap,
                                 quotes={'DEMO': fixture._quote()},
                                 allow_entries=False)
                    self.assertEqual(out['status'],
                                     'HOST_ENGINEERING_INTENT_RECORDED', out)
                    from martin_agentic.host_mailbox import (
                        read_private_json, submit_receipt, submit_request)
                    review_request = submit_request(
                        bridge.directory, out['action_id'], 'review',
                        clock=lambda: NOW)
                    from martin_agentic.native_operations import (
                        NativeOperations as _NativeOperations)
                    real_begin = _NativeOperations.begin

                    def dynamic_begin(ops_self, *args, **kwargs):
                        fixture.advance(0.001)
                        return real_begin(ops_self, *args, **kwargs)

                    with patch.object(_NativeOperations, 'begin',
                                      dynamic_begin):
                        prepared = bridge(
                            snapshot=fixture.snap,
                            quotes={'DEMO': fixture._quote()},
                            allow_entries=False)
                    self.assertEqual(prepared['status'],
                                     'HOST_PACKET_PREPARED', prepared)
                    host_packet = read_private_json(
                        bridge.directory / 'packets' /
                        (review_request['request_id'] + '.json'))['packet']
                    raw_data = {
                        **host_packet['parameters'], 'order_checks': {},
                        'customer_approval_required': False,
                        'market_data_disclosure': 'Exact disclosure',
                        'quote_data': {}}
                    response = {'content': [{
                        'type': 'text',
                        'text': json.dumps({'data': raw_data})}]}
                    submit_receipt(bridge.directory,
                                   review_request['request_id'], response,
                                   clock=fixture._clock)
                    imported = bridge(allow_dispatch=False)
                    self.assertEqual(imported['status'],
                                     'HOST_RECEIPT_IMPORTED', imported)
                    row = fixture.book.db.execute(
                        "SELECT result FROM native_operation_records "
                        "WHERE operation='review' ORDER BY rowid DESC LIMIT 1"
                    ).fetchone()
                    fixture.adopt_review(out['action_id'], host_packet,
                                         json.loads(row[0]))
                    fixture.book.configure_engineering_source(
                        fixture.owner, owner_source_root=fixture.owner,
                        native_receipt_root=bridge.directory)
                    before = fixture.snapshot()
                    decision = fixture.dry()
                    self.assertTrue(decision['allowed'], decision)
                    self.assertEqual(
                        decision['source_evidence']['provenance_kind'],
                        'host_owner_request')
                    self.assertEqual(
                        decision['source_evidence']['native_receipt_status'],
                        'bound')
                    self.assertEqual(
                        decision['source_evidence']['native_origin'],
                        'host_submitted_response')
                    self.assertFalse(
                        decision['source_evidence']
                        ['native_broker_effect_verified'])
                    self.assertEqual(decision['component']
                                     ['native_receipt_status'], 'bound')
                    self.assertEqual(decision['component']['native_origin'],
                                     'host_submitted_response')
                    self.assertFalse(decision['component']
                                     ['broker_effect_verified'])
                    self.assertTrue(decision['component']
                                    ['native_confirmation_required'])
                    fixture.assert_unchanged(before, 'host-e2e')
                    receipt_file = (bridge.directory / 'receipts' /
                                    (host_packet['operation_id'] + '.json'))
                    packet_file = (bridge.directory / 'packets' /
                                   (review_request['request_id'] + '.json'))
                    receipt_bytes = receipt_file.read_bytes()
                    packet_bytes = packet_file.read_bytes()
                    receipt_value = json.loads(receipt_bytes)
                    fixture.book.db.execute(
                        'UPDATE native_operation_records SET created_at=? '
                        'WHERE id=?',
                        ((NOW - timedelta(seconds=2)).isoformat(),
                         host_packet['operation_id']))
                    fixture.book.db.commit()
                    prepared_value = json.loads(packet_bytes)
                    prepared_value['prepared_at'] = (
                        NOW - timedelta(seconds=1)).isoformat()
                    packet_file.write_text(json.dumps(
                        prepared_value, sort_keys=True))
                    packet_file.chmod(0o600)
                    self.assertTrue(fixture.dry()['allowed'])
                    packet_file.write_bytes(packet_bytes)
                    future = {**receipt_value,
                              'received_at': (NOW + timedelta(hours=1)).isoformat()}
                    receipt_file.write_text(json.dumps(future, sort_keys=True))
                    receipt_file.chmod(0o600)
                    self.assertFalse(fixture.dry()['allowed'])
                    receipt_file.write_bytes(receipt_bytes)
                    packet_value = json.loads(packet_bytes)
                    packet_value['packet'] = {
                        **packet_value['packet'],
                        'operation': 'place',
                        'tool': 'place_equity_order'}
                    packet_value['packet_sha256'] = digest(
                        packet_value['packet'])
                    packet_file.write_text(json.dumps(
                        packet_value, sort_keys=True))
                    packet_file.chmod(0o600)
                    changed_receipt = {
                        **receipt_value,
                        'packet_sha256': packet_value['packet_sha256']}
                    receipt_file.write_text(json.dumps(
                        changed_receipt, sort_keys=True))
                    receipt_file.chmod(0o600)
                    self.assertFalse(fixture.dry()['allowed'])
                    packet_file.write_bytes(packet_bytes)
                    receipt_file.write_bytes(receipt_bytes)
                    escape = {**receipt_value, 'request_id': '../escape'}
                    receipt_file.write_text(json.dumps(escape, sort_keys=True))
                    receipt_file.chmod(0o600)
                    self.assertFalse(fixture.dry()['allowed'])
                    receipt_file.unlink()
                    blocked = fixture.dry()
                    self.assertFalse(blocked['allowed'], blocked)
                    self.assertTrue(any('NATIVE_RECEIPT' in reason
                                        for reason in blocked['reasons']),
                                    blocked['reasons'])
                finally:
                    bridge.close()

    def test_explicit_owner_config_uses_both_production_readers(self):
        with matrix.RealDryFixture(lineage=True) as fixture:
            with patch('martin_agentic.session_lineage.CANONICAL_RUNTIME',
                       fixture.root):
                self._receipt(fixture)
                fixture.book.configure_engineering_source(
                    fixture.owner, owner_source_root=fixture.owner)
                before = fixture.snapshot()
                decision = fixture.dry()
                self.assertTrue(decision['allowed'], decision)
                self.assertEqual(decision['source_evidence']['risk_source'],
                                 'owner-risk-lineage-v1')
                self.assertFalse(decision['execution_authorized'])
                fixture.assert_unchanged(before, 'production-readers')

    def test_receipt_change_between_reads_blocks(self):
        fixture = self._fixture()
        source, receipt = self._receipt(fixture)
        path = source._receipt_path(receipt['request_id'])
        real = source.provenance_reader
        state = {'count': 0}

        def changing(book, result):
            value = real(book, result)
            state['count'] += 1
            if state['count'] == 1:
                current = json.loads(path.read_text())
                current['request_sha256'] = 'b' * 64
                path.write_text(json.dumps(current, sort_keys=True))
            return value

        fixture.configure(provenance=changing)
        decision = fixture.dry()
        self.assertFalse(decision['allowed'], decision)
        self.assertTrue(any('PROVENANCE_CHANGED' in reason
                            for reason in decision['reasons']),
                        decision['reasons'])

    def test_receipt_conflict_does_not_duplicate(self):
        fixture = self._fixture()
        source, receipt = self._receipt(fixture)
        first = receipt
        binding = receipt['host_binding']
        second = source.record_intent(
            fixture.book, host_request_id='host-receipt-test',
            host_request_sha256=receipt['host_request_sha256'],
            signal_id=receipt['signal_id'],
            action=fixture.book.actions()[0],
            host_binding=binding,
            audit_event_id=receipt['audit_event_id'], received_at=NOW)
        self.assertEqual(second, first)
        with self.assertRaisesRegex(ValueError, 'OWNER_RECEIPT_(CONFLICT|AUDIT_BINDING)'):
            source.record_intent(
                fixture.book, host_request_id='host-receipt-test',
                host_request_sha256='c' * 64,
                signal_id=receipt['signal_id'],
                action=fixture.book.actions()[0],
                host_binding=binding,
                audit_event_id=receipt['audit_event_id'], received_at=NOW)
        self.assertEqual(len(list((fixture.owner / 'engineering-intent-receipts')
                                  .iterdir())), 1)


class OwnerRiskReaderTests(TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.runtime = Path(self.folder.name) / 'runtime'
        self.runtime.mkdir()
        self.patch = patch('martin_agentic.session_lineage.CANONICAL_RUNTIME',
                           self.runtime)
        self.patch.start()
        self.addCleanup(self.patch.stop)
        self.now = datetime.fromisoformat('2026-10-19T14:00:00+00:00')
        self.policy = json.loads(
            Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        self.path = self.runtime / 'book.sqlite'
        self.book = TestExecutionBook(self.path, self.policy,
                                      clock=lambda: self.now)
        self.addCleanup(self.book.close)
        current = snapshot(FakeBroker(), self.now.isoformat())
        journal = BrokerJournal(self.path)
        try:
            journal.ingest(current)
        finally:
            journal.close()
        self.book.reconcile(current)
        self.source = EngineeringOwnerSource(
            self.runtime / 'owner', clock=lambda: self.now)
        self.result = {'permit': {'permit': {
            'account_key': self.book.risk_guard.state()['account_key'],
            'purpose': 'engineering'}}, 'request': {}}

    def _bootstrap(self):
        bootstrap_session_lineage(
            self.path, self.policy, current_db=self.path,
            current_policy=self.policy, clock=lambda: self.now)

    def test_bound_reader_uses_registry_head(self):
        self._bootstrap()
        value = self.source.risk_lineage_reader(self.book, self.result)
        db = sqlite3.connect(self.runtime / 'account-risk-0000.sqlite')
        try:
            head = db.execute(
                'SELECT sequence,entry_hash FROM session_lineage_head').fetchone()
        finally:
            db.close()
        self.assertEqual(value['status'], 'bound')
        self.assertEqual(value['lineage_id'], head[1])
        self.assertEqual(value['source'], 'owner-risk-lineage-v1')
        self.assertEqual(value['account_key'],
                         self.book.risk_guard.state()['account_key'])

    def test_registry_head_change_between_reads_blocks(self):
        self._bootstrap()
        real = self.source.risk_lineage_reader
        state = {'count': 0}

        def changing(book, result):
            value = real(book, result)
            state['count'] += 1
            if state['count'] == 1:
                db = sqlite3.connect(
                    self.runtime / 'account-risk-0000.sqlite')
                try:
                    db.execute(
                        'UPDATE session_lineage_head SET entry_hash=? WHERE id=1',
                        ('f' * 64,))
                    db.commit()
                finally:
                    db.close()
            return value

        changing(self.book, self.result)
        with self.assertRaisesRegex(ValueError, 'HEAD_MISMATCH'):
            changing(self.book, self.result)

    def test_capture_only_and_unregistered_session_block(self):
        self._bootstrap()
        original = self.book.policy
        self.book.policy = {**original, 'capture_only': True}
        try:
            with self.assertRaisesRegex(ValueError,
                                        'CAPTURE_ONLY_POLICY_READ_ONLY'):
                self.source.risk_lineage_reader(self.book, self.result)
        finally:
            self.book.policy = original
        next_path = self.runtime / 'unregistered.sqlite'
        next_policy = {**self.policy, 'session_date': '2026-10-19'}
        next_book = TestExecutionBook(next_path, next_policy,
                                      clock=lambda: self.now)
        self.addCleanup(next_book.close)
        current = snapshot(FakeBroker(), self.now.isoformat())
        journal = BrokerJournal(next_path)
        try:
            journal.ingest(current)
        finally:
            journal.close()
        next_book.reconcile(current)
        result = {'permit': {'permit': {
            'account_key': next_book.risk_guard.state()['account_key'],
            'purpose': 'engineering'}}, 'request': {}}
        with self.assertRaisesRegex(ValueError,
                                    'CURRENT_SESSION_NOT_REGISTERED'):
            self.source.risk_lineage_reader(next_book, result)

    def test_missing_registry_and_lock_block(self):
        with self.assertRaisesRegex(ValueError, 'LINEAGE_REGISTRY_UNINITIALIZED'):
            self.source.risk_lineage_reader(self.book, self.result)
        self._bootstrap()
        state = self.book.risk_guard.state()
        state['blocked'] = True
        state['reasons'] = ['OWNER_TEST_LOCK']
        self.book.risk_guard.db.execute(
            'UPDATE account_risk SET payload=? WHERE id=1',
            (json.dumps(state),))
        self.book.risk_guard.db.commit()
        with self.assertRaisesRegex(ValueError, 'OWNER_RISK_LOCKED'):
            self.source.risk_lineage_reader(self.book, self.result)

    def test_stale_reader_and_successor_block(self):
        self._bootstrap()
        later = EngineeringOwnerSource(
            self.runtime / 'owner',
            clock=lambda: self.now + timedelta(seconds=31))
        with self.assertRaisesRegex(ValueError, 'OWNER_RISK_STALE'):
            later.risk_lineage_reader(self.book, self.result)
        next_path = self.runtime / 'next.sqlite'
        next_policy = {**self.policy, 'session_date': '2026-10-19',
                       'prior_execution_evidence_db': 'runtime/book.sqlite'}
        next_book = TestExecutionBook(next_path, next_policy,
                                      clock=lambda: self.now)
        self.addCleanup(next_book.close)
        current = snapshot(FakeBroker(), self.now.isoformat())
        journal = BrokerJournal(next_path)
        try:
            journal.ingest(current)
        finally:
            journal.close()
        next_book.reconcile(current)
        enroll_session_lineage(
            next_path, next_policy, predecessor_db=self.path,
            current_db=next_path, current_policy=next_policy,
            clock=lambda: self.now)
        with self.assertRaisesRegex(ValueError, 'SUPERSEDED'):
            self.source.risk_lineage_reader(self.book, self.result)
