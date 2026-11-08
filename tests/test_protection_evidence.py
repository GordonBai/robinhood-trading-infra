"""Actual protection evidence from native records and broker observations."""
from types import SimpleNamespace
import json
from unittest import TestCase

from martin_agentic.broker import BrokerJournal
from martin_agentic.audit import canonical
from martin_agentic.native_operations import NativeOperations
from martin_agentic.protection_evidence import ProtectionEvidenceReader
from test_broker import order
import test_execution as execution_module


class ProtectionEvidenceTests(TestCase):
    def setUp(self):
        self.f = execution_module.ExecutionTests(
            'test_empty_queue_has_zero_agent_calls')
        self.f.setUp()
        self.addCleanup(self.f.doCleanups)
        self.book = self.f.book
        self.native = NativeOperations(self.book)

    def quote(self):
        return {**self.f.quote, **{key: self.f.now.isoformat()
                                  for key in ('bid_at', 'ask_at', 'observed_at')}}

    def review_place(self, approval=None, stop_qty='2', stop_filled='0'):
        buy = self.f.entry_filled()
        protect = next(a for a in self.book.actions() if a['kind'] == 'protect')
        snap = self.f.snap([buy], qty='2')
        review = self.native.begin(protect['id'], 'review', snap,
                                   quote=self.quote())
        self.native.record(review['operation_id'], {
            'data': {**review['parameters'], 'order_checks': {},
                     'customer_approval_required': False,
                     'market_data_disclosure': 'fixture', 'quote_data': {}}})
        place = self.native.begin(protect['id'], 'place', snap,
                                  quote=self.quote())
        if approval is not None:
            response = {'data': {'approval': approval}}
            self.native.record(place['operation_id'], response)
            return buy, protect, None, place
        raw = order('stop-1', side='sell', qty=stop_qty, filled=stop_filled,
                    state='confirmed', stop='39')
        raw['time_in_force'] = 'gtc'
        self.native.record(place['operation_id'], {'data': {'order': raw}})
        return buy, protect, raw, place

    def ingest(self, orders, qty='2'):
        snap = self.f.snap(orders, qty=qty)
        journal = BrokerJournal(self.book.db_path, allow_synthetic=True)
        try:
            journal.ingest(snap)
        finally:
            journal.close()
        self.book.reconcile(snap)
        return snap

    def read(self, **kwargs):
        return ProtectionEvidenceReader(
            self.book, clock=lambda: self.f.now).assess_actual(
                trade_id='signal-1', **kwargs)

    def test_full_actual_synthetic_coverage_is_proven_and_scoped(self):
        buy, protect, stop, place = self.review_place()
        self.ingest([buy, stop])
        decision = self.read()
        self.assertTrue(decision['evidence_proven'], decision)
        self.assertEqual(decision['qualification'], 'SYNTHETIC_TEST_ONLY')
        self.assertTrue(decision['layers']['review']['review_verified'])
        self.assertEqual(decision['layers']['place']['status'],
                         'submitted_verified')
        self.assertEqual(decision['layers']['broker']['covered_quantity'], '2')
        self.assertTrue(decision['layers']['timing']['seconds'] <= 30)
        self.assertEqual(decision['binding']['broker_order_id'], 'stop-1')
        self.assertFalse(decision['capability_proven'])
        self.assertTrue(self.book.protection_evidence('signal-1')['evidence_proven'])

    def save_trade(self, **changes):
        state = self.book.state()
        state['trades']['signal-1'].update(changes)
        self.book.db.execute(
            'UPDATE live_state SET payload=? WHERE id=1',
            (canonical(state),))
        self.book.db.commit()

    def test_current_stop_raised_rejects_old_action_stop(self):
        buy, protect, stop, place = self.review_place()
        self.ingest([buy, stop])
        self.save_trade(stop='40')
        decision = self.read()
        self.assertFalse(decision['evidence_proven'])
        self.assertIn('PROTECTION_ACTION_STOP_NOT_CURRENT',
                      decision['reasons'])
        self.assertEqual(decision['binding']['current_trade_stop'], '40')
        self.assertEqual(decision['binding']['action_stop'], '39')

    def test_continuing_coverage_after_31_keeps_initial_latency(self):
        buy, protect, stop, place = self.review_place()
        self.ingest([buy, stop])
        from datetime import timedelta
        self.f.now += timedelta(seconds=31)
        self.ingest([buy, stop])
        decision = self.read()
        self.assertTrue(decision['evidence_proven'], decision)
        self.assertEqual(decision['layers']['timing']['seconds'], 0.0)
        self.assertEqual(
            decision['layers']['timing']['latest_coverage_at'],
            '2026-10-16T14:00:31+00:00')

    def test_future_or_reversed_timing_rejected(self):
        buy, protect, stop, place = self.review_place()
        self.ingest([buy, stop])
        from datetime import timedelta
        future = (self.f.now + timedelta(hours=1)).isoformat()
        self.save_trade(first_fill_at=future,
                        last_full_coverage_observed_at=future,
                        first_protection_observed_at=future)
        decision = self.read()
        self.assertFalse(decision['evidence_proven'])
        self.assertIn('PROTECTION_TIMING_FUTURE', decision['reasons'])

    def test_unknown_basis_and_quantity_mismatch_rejected(self):
        buy, protect, stop, place = self.review_place()
        self.ingest([buy, stop])
        self.save_trade(protection_timing_basis='unknown_basis')
        decision = self.read()
        self.assertFalse(decision['evidence_proven'])
        self.assertIn('PROTECTION_TIMING_BASIS_UNPROVEN',
                      decision['reasons'])
        self.save_trade(
            protection_timing_basis='first_fill_to_first_full_coverage_observation',
            first_protection_observed_quantity='1')
        decision = self.read()
        self.assertFalse(decision['evidence_proven'])
        self.assertIn('PROTECTION_TIMING_QUANTITY_MISMATCH',
                      decision['reasons'])

    def test_recovery_after_initial_coverage_rejected(self):
        buy, protect, stop, place = self.review_place()
        self.ingest([buy, stop])
        from datetime import timedelta
        self.save_trade(unprotected_first_observed_at=(
            self.f.now + timedelta(seconds=1)).isoformat())
        decision = self.read()
        self.assertFalse(decision['evidence_proven'])
        self.assertIn('PROTECTION_RECOVERY_TIMING_UNPROVEN',
                      decision['reasons'])

    def mutate_execution(self, **changes):
        identity, raw = self.book.db.execute(
            'SELECT id,payload FROM broker_executions LIMIT 1').fetchone()
        fill = json.loads(raw)
        fill.update(changes)
        self.book.db.execute(
            'UPDATE broker_executions SET payload=? WHERE id=?',
            (canonical(fill), identity))
        self.book.db.commit()
        return fill

    def test_foreign_same_symbol_execution_is_rejected(self):
        buy, protect, stop, place = self.review_place()
        self.ingest([buy, stop])
        fill = self.mutate_execution(order_id='unrelated-entry-order',
                                     quantity='99')
        decision = self.read()
        self.assertFalse(decision['evidence_proven'])
        self.assertIn('PROTECTION_FIRST_FILL_UNPROVEN',
                      decision['reasons'])
        self.assertEqual(fill['order_id'], 'unrelated-entry-order')

    def test_execution_quantity_or_id_conflict_is_rejected(self):
        buy, protect, stop, place = self.review_place()
        self.ingest([buy, stop])
        self.mutate_execution(quantity='99')
        decision = self.read()
        self.assertFalse(decision['evidence_proven'])
        self.assertIn('PROTECTION_FIRST_FILL_EXECUTION_MISMATCH',
                      decision['reasons'])
        self.mutate_execution(id='other-execution')
        decision = self.read()
        self.assertFalse(decision['evidence_proven'])
        self.assertIn('PROTECTION_FIRST_FILL_EXECUTION_MISMATCH',
                      decision['reasons'])

    def test_same_entry_split_execution_at_same_time_is_valid(self):
        buy, protect, stop, place = self.review_place()
        stamp = buy['executions'][0]['timestamp']
        buy['executions'] = [
            {'id': 'buy-split-1', 'price': '40', 'quantity': '1',
             'fees': '0', 'timestamp': stamp},
            {'id': 'buy-split-2', 'price': '40', 'quantity': '1',
             'fees': '0', 'timestamp': stamp}]
        self.ingest([buy, stop])
        decision = self.read()
        self.assertTrue(decision['evidence_proven'], decision)
        self.assertEqual(len(decision['layers']['timing']['execution_ids']), 2)
        self.assertEqual(decision['layers']['timing']['execution_quantity'], '2')

    def test_observation_limit_cannot_exceed_30(self):
        for value in (31, 60, 0, True, None, '30'):
            with self.subTest(value=value):
                with self.assertRaisesRegex(
                        ValueError, 'PROTECTION_OBSERVATION_LIMIT'):
                    ProtectionEvidenceReader(
                        self.book, clock=lambda: self.f.now,
                        max_observation_seconds=value)
        limit = ProtectionEvidenceReader(
            self.book, clock=lambda: self.f.now,
            max_observation_seconds=30)
        self.assertEqual(limit.max_observation_seconds, 30)

    def test_stale_observation_is_rejected_at_default_limit(self):
        buy, protect, stop, place = self.review_place()
        self.ingest([buy, stop])
        from datetime import timedelta
        self.f.now += timedelta(seconds=31)
        decision = self.read()
        self.assertFalse(decision['evidence_proven'])
        self.assertIn('BROKER_OBSERVATION_STALE_OR_FUTURE',
                      decision['reasons'])
        self.assertEqual(decision['limits']['observation_seconds'], 30)
        self.assertEqual(decision['limits']['protection_timing_seconds'], 30)

    def test_submitted_without_broker_observation_is_not_proven(self):
        self.review_place()
        decision = self.read()
        self.assertFalse(decision['evidence_proven'])
        self.assertIn('BROKER_OBSERVATION_MISSING', decision['reasons'])
        self.assertEqual(decision['layers']['place']['status'],
                         'submitted_verified')

    def test_pending_approval_is_not_active_protection(self):
        approval = {'approval_id': 'approval-1',
                    'asset_class': 'ASSET_CLASS_EQUITY',
                    'state': 'APPROVAL_STATE_PENDING', 'is_edited': False}
        self.review_place(approval=approval)
        decision = self.read()
        self.assertFalse(decision['evidence_proven'])
        self.assertIn('PROTECTION_APPROVAL_PENDING_NOT_ACTIVE',
                      decision['reasons'])
        self.assertNotEqual(decision['layers']['place']['status'],
                            'submitted_verified')

    def test_approved_approval_is_still_not_a_broker_order(self):
        approval = {'approval_id': 'approval-1',
                    'asset_class': 'ASSET_CLASS_EQUITY',
                    'state': 'APPROVAL_STATE_APPROVED', 'is_edited': False}
        self.review_place(approval=approval)
        decision = self.read()
        self.assertFalse(decision['evidence_proven'])
        self.assertIn('PROTECTION_APPROVAL_NOT_BROKER_ORDER',
                      decision['reasons'])

    def test_wrong_broker_account_is_rejected(self):
        buy, protect, stop, place = self.review_place()
        self.ingest([buy, stop])
        row = self.book.db.execute(
            'SELECT payload FROM broker_current WHERE id=1').fetchone()
        payload = json.loads(row[0])
        payload['account_key'] = 'f' * 64
        from martin_agentic.audit import canonical
        self.book.db.execute('UPDATE broker_current SET payload=? WHERE id=1',
                             (canonical(payload),))
        self.book.db.commit()
        decision = self.read()
        self.assertFalse(decision['evidence_proven'])
        self.assertIn('BROKER_OBSERVATION_ACCOUNT_MISMATCH',
                      decision['reasons'])

    def test_partial_quantity_is_not_full_coverage(self):
        buy, protect, stop, place = self.review_place(stop_qty='2',
                                                       stop_filled='1')
        self.ingest([buy, stop])
        decision = self.read()
        self.assertFalse(decision['evidence_proven'])
        self.assertIn('PROTECTION_COVERAGE_PARTIAL', decision['reasons'])

    def test_cancel_pending_blocks_actual_protection(self):
        buy, protect, stop, place = self.review_place()
        self.ingest([buy, stop])
        trade = self.book.state()['trades']['signal-1']
        self.book._queue(trade, 'cancel', {'order_id': 'stop-1'},
                         self.f.now, 'TEST_CANCEL_PENDING', target='stop-1')
        self.book.db.commit()
        decision = self.read()
        self.assertFalse(decision['evidence_proven'])
        self.assertIn('PROTECTION_CANCEL_PENDING', decision['reasons'])

    def test_terminal_order_is_not_coverage(self):
        buy, protect, stop, place = self.review_place()
        stopped = {**stop, 'state': 'cancelled'}
        snap = self.f.snap([buy, stopped], qty='2')
        journal = BrokerJournal(self.book.db_path, allow_synthetic=True)
        try:
            journal.ingest(snap)
        finally:
            journal.close()
        decision = self.read()
        self.assertFalse(decision['evidence_proven'])
        self.assertIn('BROKER_PROTECTION_ORDER_TERMINAL',
                      decision['reasons'])

    def test_unknown_action_is_rejected(self):
        buy, protect, stop, place = self.review_place()
        self.ingest([buy, stop])
        self.book.db.execute(
            "UPDATE live_actions SET state='UNKNOWN' WHERE id=?",
            (protect['id'],))
        self.book.db.commit()
        decision = self.read()
        self.assertFalse(decision['evidence_proven'])
        self.assertIn('PROTECTION_ACTION_UNKNOWN', decision['reasons'])

    def test_wrong_action_is_rejected(self):
        self.review_place()
        decision = ProtectionEvidenceReader(
            self.book, clock=lambda: self.f.now).assess_actual(
                trade_id='signal-1', action_id='not-an-action')
        self.assertFalse(decision['schema_valid'])
        self.assertIn('PROTECTION_ACTION_MISSING', decision['reasons'])

    def test_wrong_action_ref_is_rejected(self):
        self.review_place()
        action = next(a for a in self.book.actions()
                      if a['kind'] == 'protect')
        payload = json.loads(action['payload'])
        payload['ref_id'] = 'other-ref'
        from martin_agentic.audit import canonical
        self.book.db.execute(
            'UPDATE live_actions SET payload=? WHERE id=?',
            (canonical(payload), action['id']))
        self.book.db.commit()
        decision = self.read()
        self.assertFalse(decision['evidence_proven'])
        self.assertIn('PROTECTION_PLACE_BINDING_MISMATCH',
                      decision['reasons'])

    def test_changed_broker_stop_is_not_current(self):
        buy, protect, stop, place = self.review_place()
        self.ingest([buy, stop])
        changed = dict(stop)
        changed['stop_price'] = '38'
        changed['time_in_force'] = 'gtc'
        snap = self.f.snap([buy, changed], qty='2')
        journal = BrokerJournal(self.book.db_path, allow_synthetic=True)
        try:
            journal.ingest(snap)
        finally:
            journal.close()
        decision = self.read()
        self.assertFalse(decision['evidence_proven'])
        self.assertIn('BROKER_PROTECTION_STOP_NOT_CURRENT',
                      decision['reasons'])

    def test_late_coverage_is_not_proven(self):
        buy, protect, stop, place = self.review_place()
        self.ingest([buy, stop])
        state = self.book.state()
        trade = state['trades']['signal-1']
        for key in ('first_protection_observed_at',
                    'first_protection_observed_quantity',
                    'last_full_coverage_observed_at',
                    'last_full_coverage_observed_quantity',
                    'protection_observation_delay_seconds',
                    'protection_timing_basis',
                    'protection_timing_synthetic'):
            trade.pop(key, None)
        from martin_agentic.audit import canonical
        self.book.db.execute('UPDATE live_state SET payload=? WHERE id=1',
                             (canonical(state),))
        self.book.db.commit()
        from datetime import timedelta
        self.f.now += timedelta(seconds=31)
        self.ingest([buy, stop])
        decision = self.read()
        self.assertFalse(decision['evidence_proven'])
        self.assertIn('PROTECTION_COVERAGE_LATE', decision['reasons'])

    def test_synthetic_observation_cannot_be_real_evidence(self):
        buy, protect, stop, place = self.review_place()
        self.ingest([buy, stop])
        self.book.simulation = False
        decision = self.read()
        self.assertFalse(decision['evidence_proven'])
        self.assertIn('BROKER_OBSERVATION_BINDING_INVALID',
                      decision['reasons'])
        self.assertNotEqual(decision['qualification'],
                            'REAL_ACTUAL_PROTECTION_EVIDENCE')

    def test_capability_remains_unproven_even_with_advanced_tools(self):
        tools = SimpleNamespace(
            protection_tool_identity=('review_advanced_order',
                                      'place_advanced_order'))
        decision = ProtectionEvidenceReader(
            self.book, clock=lambda: self.f.now).assess_capability(
                plan=self.f.plan, protection=tools)
        self.assertFalse(decision['capability_proven'])
        self.assertIn('CHANNEL_PROTECTION_PATH_UNPROVEN',
                      decision['reasons'])
        self.assertEqual(decision['layers']['tool_contract']['status'],
                         'descriptive_only')
