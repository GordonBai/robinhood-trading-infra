from copy import deepcopy
from datetime import timedelta
import json
from unittest import TestCase

import test_execution
from martin_agentic.control_tasks import worker_lease
from martin_agentic.script_execution import ScriptExecutionDriver


class SyntheticTransport:
    simulation_only = True

    def __init__(self):
        self.calls = []
        self.response = None
        self.fail = False
        self.checks = {}
        self.approval_required = False

    def execute_simulated(self, packet):
        self.calls.append(deepcopy(packet))
        if self.fail:
            raise TimeoutError('ambiguous')
        if self.response is not None:
            return self.response
        if packet['operation'] == 'review':
            return {'data': {**packet['parameters'], 'order_checks': self.checks,
                'customer_approval_required': self.approval_required,
                'market_data_disclosure': 'Synthetic fixture disclosure'}}
        if packet['operation'] == 'cancel':
            return {'data': {'accepted': True}}
        return {'data': {'order_id': 'synthetic-' + packet['action_id']}}


class ScriptExecutionTests(TestCase):
    def setUp(self):
        self.f = test_execution.ExecutionTests('test_empty_queue_has_zero_agent_calls')
        self.f.setUp()
        self.addCleanup(self.f.tearDown)
        self.book = self.f.book
        self.transport = SyntheticTransport()
        self.driver = ScriptExecutionDriver(self.book, self.transport)
        self.quotes = {'DEMO': self.f.quote}

    def step(self, snapshot=None, **kw):
        return self.driver.step(snapshot or self.f.snap(), self.quotes, **kw)

    def engineering(self):
        return self.book.propose('engineering-script', self.f.plan, self.f.quote,
            self.f.snap(), purpose='engineering', evidence={'source_sha256': 'synthetic-fixture',
                'synthetic': True, 'engineering_scope_authorized': True, 'parameters_user_specified': True})

    def placed(self, snapshot=None, **kw):
        self.assertEqual(self.step(snapshot, **kw)['status'], 'REVIEWED_SYNTHETIC')
        result = self.step(snapshot, **kw)
        self.assertEqual(result['status'], 'submitted')
        return self.transport.calls[-1]

    def bound(self, packet, **kw):
        action = json.loads(next(a['payload'] for a in self.book.actions() if a['id'] == packet['action_id']))
        return self.f.bound(action, oid='synthetic-' + packet['action_id'], **kw)

    def protected(self):
        self.engineering()
        entry = self.placed(allow_entries=True)
        buy = self.bound(entry, filled='2', state='filled')
        snap = self.f.snap([buy], qty='2')
        protect = self.placed(snap)
        stop = self.bound(protect)
        self.step(self.f.snap([buy, stop], qty='2', held='2'))
        self.assertEqual(self.book.state()['trades']['engineering-script']['phase'], 'PROTECTED')
        return buy, stop

    def test_complete_synthetic_core_chain_uses_broker_observations(self):
        buy, stop = self.protected()
        snap = self.f.snap([buy, stop], qty='2', held='2')
        self.book.request_engineering_exit('engineering-script', snap)
        self.assertEqual(self.step(snap)['status'], 'cancel_requested')
        count = len(self.transport.calls)
        self.step(snap)
        self.assertEqual(len(self.transport.calls), count)
        self.assertFalse(any(a['kind'] == 'exit' for a in self.book.actions()))
        stop['state'] = 'cancelled'
        snap = self.f.snap([buy, stop], qty='2')
        exit_packet = self.placed(snap)
        sell = self.bound(exit_packet, filled='2', state='filled')
        self.step(self.f.snap([buy, stop, sell]))
        self.assertEqual(self.book.state()['trades']['engineering-script']['phase'], 'FLAT_VERIFIED')
        operations = self.book.db.execute('SELECT COUNT(*) FROM external_operations').fetchone()[0]
        self.assertEqual(operations, len(self.transport.calls))
        receipts = self.book.db.execute('SELECT COUNT(*) FROM script_execution_receipts').fetchone()[0]
        self.assertEqual(receipts, operations)
        self.book.audit.verify()

    def test_cancel_fill_race_never_sends_duplicate_sell(self):
        buy, stop = self.protected()
        snap = self.f.snap([buy, stop], qty='2', held='2')
        self.book.request_engineering_exit('engineering-script', snap)
        self.step(snap)
        count = len(self.transport.calls)
        stop.update(state='filled', cumulative_quantity='2', average_price='39')
        stop['executions'] = [{'id': 'stop-fill', 'timestamp': self.f.now.isoformat(), 'price': '39', 'quantity': '2', 'fees': '0'}]
        self.step(self.f.snap([buy, stop]))
        self.assertEqual(len(self.transport.calls), count)
        self.assertEqual(self.book.state()['trades']['engineering-script']['phase'], 'FLAT_VERIFIED')

    def test_unknown_write_does_not_retry(self):
        self.engineering()
        self.step(allow_entries=True)
        self.transport.fail = True
        self.assertEqual(self.step(allow_entries=True)['status'], 'unknown')
        count = len(self.transport.calls)
        self.assertEqual(self.step(allow_entries=True)['status'], 'AWAITING_RECONCILIATION')
        self.assertEqual(len(self.transport.calls), count)

    def test_real_book_rejected_without_schema_or_state_mutation(self):
        before = list(self.book.db.iterdump())
        self.book.simulation = False
        try:
            result = self.step()
        finally:
            self.book.simulation = True
        self.assertEqual(result['status'], 'BLOCKED_CAPABILITY')
        self.assertEqual(before, list(self.book.db.iterdump()))
        self.assertEqual(self.transport.calls, [])
        self.assertFalse(self.f.path.with_suffix('.sqlite.worker.lock').exists())

    def test_real_snapshot_or_nonfixture_transport_blocked_before_changes(self):
        before = list(self.book.db.iterdump())
        snap = self.f.snap()
        snap['synthetic'] = False
        self.assertEqual(self.step(snap)['status'], 'BLOCKED_CAPABILITY')
        self.transport.simulation_only = False
        self.assertEqual(self.step()['status'], 'BLOCKED_CAPABILITY')
        self.assertEqual(before, list(self.book.db.iterdump()))

    def test_entries_default_disabled_and_worker_lease_exclusive(self):
        self.engineering()
        self.assertEqual(self.step()['status'], 'IDLE')
        lease = worker_lease(self.f.path)
        try:
            self.assertEqual(self.step(allow_entries=True)['status'], 'BLOCKED_OWNERSHIP')
        finally:
            lease.close()
        self.assertEqual(self.transport.calls, [])

    def test_expired_action_is_not_dispatched(self):
        self.engineering()
        self.f.now += timedelta(seconds=16)
        self.f.quote.update(bid_at=self.f.now.isoformat(), ask_at=self.f.now.isoformat(), observed_at=self.f.now.isoformat())
        result = self.step(allow_entries=True)
        self.assertFalse(result['transport_called'])
        self.assertEqual(self.transport.calls, [])

    def test_expired_review_cannot_place(self):
        self.engineering()
        self.step(allow_entries=True)
        self.f.now += timedelta(seconds=31)
        self.f.quote.update(bid_at=self.f.now.isoformat(), ask_at=self.f.now.isoformat(), observed_at=self.f.now.isoformat())
        self.assertFalse(self.step(allow_entries=True)['transport_called'])
        self.assertEqual(len(self.transport.calls), 1)

    def test_warning_or_app_approval_never_auto_place(self):
        self.engineering()
        self.transport.checks = {'alert': 'Exact alert text'}
        self.assertEqual(self.step(allow_entries=True)['status'], 'WAITING_USER')
        self.assertEqual(self.step(allow_entries=True)['status'], 'WAITING_USER')
        self.assertEqual(len(self.transport.calls), 1)
        stored = self.book.db.execute('SELECT response FROM script_execution_receipts').fetchone()[0]
        self.assertIn('Exact alert text', stored)

    def test_pending_approval_is_not_protection_and_not_retried(self):
        buy = self.f.entry_filled()
        snap = self.f.snap([buy], qty='2')
        self.step(snap)
        self.transport.response = {'data': {'approval_id': 'fixture-pending'}}
        self.assertEqual(self.step(snap)['status'], 'pending_approval')
        count = len(self.transport.calls)
        self.step(snap)
        self.assertEqual(len(self.transport.calls), count)
        self.assertNotEqual(self.book.state()['trades']['signal-1']['phase'], 'PROTECTED')

    def test_review_error_waits_without_write_unknown(self):
        self.engineering()
        self.transport.fail = True
        self.assertEqual(self.step(allow_entries=True)['status'], 'WAITING_USER')
        self.assertFalse(self.step(allow_entries=True)['transport_called'])
        self.assertEqual(self.book.actions()[0]['state'], 'WAITING_USER')

    def test_app_approval_required_review_waits_without_place(self):
        self.engineering()
        self.transport.approval_required = True
        self.assertEqual(self.step(allow_entries=True)['status'], 'WAITING_USER')
        self.assertEqual(self.step(allow_entries=True)['status'], 'WAITING_USER')
        self.assertEqual(len(self.transport.calls), 1)
        self.assertFalse(any(p['operation'] == 'place' for p in self.transport.calls))

    def test_stale_broker_snapshot_blocks_before_transport(self):
        self.engineering()
        snap = self.f.snap()
        self.f.now += timedelta(seconds=31)
        self.assertEqual(self.step(snap, allow_entries=True)['status'], 'BLOCKED_VALIDATION')
        self.assertEqual(self.transport.calls, [])

    def test_restarted_driver_does_not_repeat_pending_unknown_write(self):
        self.engineering()
        self.step(allow_entries=True)
        self.transport.fail = True
        self.step(allow_entries=True)
        count = len(self.transport.calls)
        self.driver = ScriptExecutionDriver(self.book, self.transport)
        self.assertEqual(self.step(allow_entries=True)['status'], 'AWAITING_RECONCILIATION')
        self.assertEqual(len(self.transport.calls), count)

    def test_action_provenance_account_policy_and_identity_are_bound(self):
        self.engineering()
        row = self.book.actions()[0]
        original = json.loads(row['payload'])
        for key, value in [('synthetic', False), ('account', '••••9999'),
                           ('policy', {}), ('action_id', 'foreign'),
                           ('trade_id', 'foreign'), ('kind', 'exit')]:
            damaged = {**original, key: value}
            self.book.db.execute('UPDATE live_actions SET payload=? WHERE id=?',
                                 (json.dumps(damaged), row['id']))
            self.book.db.commit()
            before = self.book.db.execute('SELECT count(*) FROM external_operations').fetchone()[0]
            result = self.step(allow_entries=True)
            self.assertEqual(result['status'], 'BLOCKED_VALIDATION', key)
            self.assertEqual(self.transport.calls, [], key)
            self.assertEqual(self.book.db.execute('SELECT count(*) FROM external_operations').fetchone()[0], before)
            self.book.db.execute('UPDATE live_actions SET payload=? WHERE id=?', (row['payload'], row['id']))
            self.book.db.commit()

    def test_durable_receipt_reimport_is_idempotent_and_conflicts_are_rejected(self):
        self.engineering()
        prepared = self.driver.prepare(self.f.snap(), self.quotes, allow_entries=True)
        try:
            packet = prepared['packet']
            response = self.transport.execute_simulated(packet)
            first = self.driver.complete(packet, response)
            audit_before = self.book.audit.verify()
            second = self.driver.complete(packet, response)
            self.assertFalse(first['receipt_duplicate'])
            self.assertTrue(second['receipt_duplicate'])
            self.assertEqual(self.book.audit.verify(), audit_before)
            self.assertEqual(self.book.db.execute('SELECT count(*) FROM script_execution_receipts').fetchone()[0], 1)
            with self.assertRaisesRegex(ValueError, 'SCRIPT_RECEIPT_CONFLICT'):
                self.driver.complete(packet, {'data': {'different': 'response'}})
        finally:
            prepared['_lease'].close()

    def test_receipt_packet_must_match_exact_prepared_operation(self):
        self.engineering()
        prepared = self.driver.prepare(self.f.snap(), self.quotes, allow_entries=True)
        try:
            packet = prepared['packet']
            with self.assertRaisesRegex(ValueError, 'SCRIPT_PACKET_BINDING_MISMATCH'):
                self.driver.complete({**packet, 'action_id': 'foreign'}, {'data': {}})
            self.assertFalse(self.book.db.execute("SELECT 1 FROM sqlite_master WHERE name='script_execution_receipts'").fetchone())
        finally:
            prepared['_lease'].close()
