"""Real-mode supervised engineering finalization/consume contract (066).

Offline fixture owner records only.  No broker, MCP, network or real account is
touched; real activation is explicitly enabled on a temporary book and remains
closed everywhere else.
"""
from copy import deepcopy
from datetime import datetime, timedelta
from unittest import TestCase
from unittest.mock import patch

from martin_agentic.audit import digest
from martin_agentic.supervised_engineering import (
    SupervisedEngineeringError)
from test_engineering_source import NOW, RealDryFixture


class RealConsumeContractTests(TestCase):
    def _decision(self, fixture):
        action = next(a for a in fixture.book.actions()
                      if a['id'] == fixture.action_id)
        decision = fixture.book.supervised_engineering_authority.assess(
            book=fixture.book, mode='claim', action=action,
            purpose='engineering')
        self.assertTrue(decision['allowed'], decision)
        return action, decision

    def _finalize(self, fixture, decision):
        return fixture.book.supervised_engineering_authority.finalize_promotion(
            fixture.book, decision['component'], fixture.action_id,
            fixture._clock(), grant=decision.get('finalization_grant'))

    def _consume(self, fixture, decision, *, finalized=None):
        authority = fixture.book.supervised_engineering_authority
        fixture.book._begin()
        try:
            if finalized is None:
                finalized = self._finalize(fixture, decision)
            result = authority.consume(
                fixture.book, finalized['component'], fixture.action_id,
                fixture._clock(), finalized=finalized)
            fixture.book.db.commit()
        except BaseException:
            fixture.book.db.rollback()
            raise
        return result

    def _rows(self, fixture):
        exists = fixture.book.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND "
            "name='formal_authorization_consumptions'").fetchone()
        if not exists:
            return []
        return fixture.book.db.execute(
            'SELECT one_shot_id,source_sha256,binding,action_id,consumed_at '
            'FROM formal_authorization_consumptions ORDER BY rowid').fetchall()

    def test_real_assess_finalize_consume_and_recheck(self):
        with RealDryFixture(activate=True) as fixture:
            action, decision = self._decision(fixture)
            component = decision['component']
            self.assertIs(component['supervised_synthetic'], False)
            self.assertIs(component['real_source_verified'], True)
            self.assertEqual(component['activation_gate'], 'ENABLED')
            self.assertIsNotNone(component['real_source_evidence_sha256'])
            self.assertIs(component['entry_authorized'], False)
            self.assertIs(component['execution_authorized'], False)
            result = self._consume(fixture, decision)
            self.assertEqual(len(self._rows(fixture)), 1)
            row = self._rows(fixture)[0]
            self.assertEqual(row[1], component['request_sha256'])
            self.assertEqual(row[2], component['binding'])
            self.assertEqual(row[3], fixture.action_id)
            self.assertEqual(result['consumed_at'].isoformat(), row[4])
            audit = fixture.book.db.execute(
                "SELECT payload FROM audit_events WHERE event_id=?",
                ('supervised-permit-' + component['one_shot_id'] + '-consumed',
                 )).fetchone()
            self.assertIsNotNone(audit)
            payload = __import__('json').loads(audit[0])
            self.assertIs(payload['synthetic'], False)
            self.assertIs(payload['details']['synthetic'], False)
            self.assertIs(payload['details']['synthetic_only'], False)
            self.assertEqual(
                fixture.book.supervised_engineering_authority.recheck_receipt(
                    component, book=fixture.book, action=action,
                    purpose='engineering'), [])

    def test_real_default_activation_still_closed(self):
        with RealDryFixture(activate=False) as fixture:
            action = next(a for a in fixture.book.actions()
                          if a['id'] == fixture.action_id)
            decision = fixture.book.supervised_engineering_authority.assess(
                book=fixture.book, mode='claim', action=action,
                purpose='engineering')
            self.assertFalse(decision['allowed'], decision)
            self.assertIn('SUPERVISED_ENGINEERING_REAL_ACTIVATION_DISABLED',
                          decision['reasons'])
            self.assertEqual(self._rows(fixture), [])
            with self.assertRaisesRegex(
                    ValueError, 'SUPERVISED_ENGINEERING_PERMIT_BLOCKED'):
                fixture.book.promote_supervised_entry(fixture.action_id)
            self.assertEqual(self._rows(fixture), [])

    def test_real_consume_after_clock_advance_keeps_component_binding(self):
        with RealDryFixture(activate=True) as fixture:
            _, decision = self._decision(fixture)
            finalized = self._finalize(fixture, decision)
            fixture.advance(1)
            result = self._consume(fixture, decision, finalized=finalized)
            self.assertEqual(result['binding'],
                             finalized['component']['binding'])
            self.assertEqual(len(self._rows(fixture)), 1)

    def test_real_consume_replay_returns_original_time_and_binding(self):
        with RealDryFixture(activate=True) as fixture:
            action, decision = self._decision(fixture)
            finalized = self._finalize(fixture, decision)
            first = self._consume(fixture, decision, finalized=finalized)
            fixture.advance(2)
            second = self._consume(fixture, decision, finalized=finalized)
            self.assertEqual(second['consumed_at'], first['consumed_at'])
            self.assertEqual(second['binding'], first['binding'])
            self.assertIs(second.get('replayed'), True)
            self.assertEqual(len(self._rows(fixture)), 1)
            self.assertEqual(
                fixture.book.supervised_engineering_authority.recheck_receipt(
                    decision['component'], book=fixture.book, action=action,
                    purpose='engineering'), [])

    def test_real_mixed_and_forged_inputs_fail_closed(self):
        with RealDryFixture(activate=True) as fixture:
            _, decision = self._decision(fixture)
            component = decision['component']
            authority = fixture.book.supervised_engineering_authority
            synthetic = dict(component)
            synthetic['supervised_synthetic'] = True
            with self.assertRaisesRegex(
                    ValueError, 'SUPERVISED_ENGINEERING_SYNTHETIC_MIXED'):
                authority.finalize_promotion(
                    fixture.book, synthetic, fixture.action_id, fixture._clock(),
                    evidence=decision.get('source_evidence'))
            with self.assertRaisesRegex(
                    ValueError, 'REAL_ASSESSMENT_GRANT_REQUIRED'):
                authority.finalize_promotion(
                    fixture.book, component, fixture.action_id, fixture._clock())
            dry = fixture.book.supervised_engineering_authority.dry_assess(
                book=fixture.book, action=next(
                    a for a in fixture.book.actions()
                    if a['id'] == fixture.action_id),
                purpose='engineering')
            self.assertTrue(dry['allowed'], dry)
            with self.assertRaisesRegex(
                    ValueError, 'DRY_NOT_CONSUMABLE'):
                authority.finalize_promotion(
                    fixture.book, dry['component'], fixture.action_id,
                    fixture._clock(), evidence=dry.get('source_evidence'))
            forged = deepcopy(decision.get('source_evidence'))
            forged['source_sha256'] = '0' * 64
            with self.assertRaisesRegex(
                    ValueError, 'REAL_SOURCE_EVIDENCE_MISMATCH'):
                authority.finalize_promotion(
                    fixture.book, component, fixture.action_id, fixture._clock(),
                    grant=decision.get('finalization_grant'), evidence=forged)
            self.assertEqual(self._rows(fixture), [])

    def test_real_owner_changes_after_assess_are_rejected(self):
        cases = (
            ('permit-revoke', lambda f: f.revoke_permit(),
             'OWNER_SOURCE_CHANGED|PERMIT'),
            ('action-state', lambda f: f.set_action(state='UNKNOWN'),
             'PROMOTION_STATE'),
            ('facts', lambda f: f.mutate_facts({'status': 'changed'}),
             'FACTS'),
            ('risk', lambda f: f.set_risk_row({'mutated': True}),
             'EVIDENCE_INVALID|RISK'),
            ('review', lambda f: f.mutate_review_result('approval'), 'REVIEW'),
            ('action-payload', lambda f: f.set_action_payload(
                {'risk_terms': {'wrong': True}}), 'ACTION'),
            ('request-replace', lambda f: f.write_request(
                {**__import__('json').loads(f.request_bytes()),
                 'request_id': 'request-replaced'}),
             'OWNER_SOURCE_CHANGED'),
            ('review-state', lambda f: f.set_review_state('PENDING'),
             'REVIEW'),
            ('registry-delete', lambda f: f.delete_registry(), 'CAPTURE'),
        )
        for label, mutate, expected in cases:
            with self.subTest(label=label):
                with RealDryFixture(activate=True) as fixture:
                    _, decision = self._decision(fixture)
                    mutate(fixture)
                    with self.assertRaisesRegex(ValueError, expected):
                        self._finalize(fixture, decision)
                    self.assertEqual(self._rows(fixture), [])

    def test_real_finalize_respects_activation_once_closed(self):
        with RealDryFixture(activate=True) as fixture:
            _, decision = self._decision(fixture)
            fixture.book.supervised_engineering_authority.real_activation_enabled = False
            with self.assertRaisesRegex(
                    ValueError, 'REAL_ACTIVATION_DISABLED'):
                self._finalize(fixture, decision)
            self.assertEqual(self._rows(fixture), [])

    def test_real_component_binding_tamper_is_rejected(self):
        cases = (
            ('account', {'account_key': '0' * 64}, 'ACCOUNT_CHANGED'),
            ('policy', {'policy_sha256': '0' * 64}, 'POLICY_CHANGED'),
            ('source', {'source_fingerprint': '0' * 64}, 'SOURCE'),
            ('action', {'action_payload_sha256': '0' * 64},
             'ACTION_CHANGED'),
            ('permit', {'permit_sha256': '0' * 64}, 'OWNER_SOURCE_CHANGED'),
            ('one-shot', {'one_shot_id': 'other-one-shot'},
             'COMPONENT_CHANGED'),
        )
        for label, changes, expected in cases:
            with self.subTest(label=label):
                with RealDryFixture(activate=True) as fixture:
                    _, decision = self._decision(fixture)
                    component = deepcopy(decision['component'])
                    component.update(changes)
                    with self.assertRaisesRegex(
                            ValueError, 'ASSESSMENT_GRANT_MISMATCH|' + expected):
                        fixture.book.supervised_engineering_authority \
                            .finalize_promotion(
                                fixture.book, component, fixture.action_id,
                                fixture._clock(),
                                grant=decision.get('finalization_grant'))
                    self.assertEqual(self._rows(fixture), [])

    def test_real_missing_owner_readers_block_before_transaction(self):
        for kwargs in ({'provenance': None}, {'risk': None}):
            with self.subTest(kwargs=kwargs):
                with RealDryFixture(activate=True) as fixture:
                    fixture.configure(**kwargs)
                    action = next(a for a in fixture.book.actions()
                                  if a['id'] == fixture.action_id)
                    decision = (fixture.book
                                .supervised_engineering_authority.assess(
                                    book=fixture.book, mode='claim',
                                    action=action, purpose='engineering'))
                    self.assertFalse(decision['allowed'], decision)
                    self.assertEqual(self._rows(fixture), [])

    def test_real_slow_reader_blocks_before_transaction(self):
        with RealDryFixture(activate=True) as fixture:
            fixture.provenance_fault = 'slow'
            action = next(a for a in fixture.book.actions()
                          if a['id'] == fixture.action_id)
            decision = fixture.book.supervised_engineering_authority.assess(
                book=fixture.book, mode='claim', action=action,
                purpose='engineering')
            self.assertFalse(decision['allowed'], decision)
            self.assertEqual(self._rows(fixture), [])

    def test_real_activation_does_not_bypass_outer_entry_gate(self):
        with RealDryFixture(activate=True) as fixture:
            with self.assertRaisesRegex(
                    ValueError, 'ENTRY_AUTHORITY_BLOCKED'):
                fixture.book.promote_supervised_entry(fixture.action_id)
            self.assertEqual(self._rows(fixture), [])

    def test_real_expiry_after_assess_is_rejected(self):
        with RealDryFixture(activate=True) as fixture:
            _, decision = self._decision(fixture)
            fixture.advance(31)
            with self.assertRaisesRegex(
                    ValueError, 'PERMIT_WINDOW|FACTS|GRANT_EXPIRED'):
                self._finalize(fixture, decision)
            self.assertEqual(self._rows(fixture), [])

    def test_real_second_connection_replays_without_second_consumption(self):
        from martin_agentic.execution import ExecutionBook
        from test_supervised_engineering import FakePreflight
        with RealDryFixture(activate=True) as fixture:
            action, decision = self._decision(fixture)
            finalized = self._finalize(fixture, decision)
            self._consume(fixture, decision, finalized=finalized)
            second = ExecutionBook(
                fixture.root / 'book.sqlite', fixture.policy,
                simulation=False, clock=fixture._clock,
                current_preflight=FakePreflight(fixture.facts),
                supervised_engineering_authority=(
                    fixture.book.supervised_engineering_authority))
            try:
                second.refresh_current_account_assessment(purpose='engineering')
                second._begin()
                replay = second.supervised_engineering_authority.consume(
                    second, decision['component'], fixture.action_id,
                    fixture._clock(), finalized=finalized)
                second.db.commit()
                self.assertIs(replay.get('replayed'), True)
                self.assertEqual(second.db.execute(
                    'SELECT count(*) FROM formal_authorization_consumptions'
                ).fetchone()[0], 1)
            finally:
                second.close()

    def test_real_cross_action_replay_is_rejected(self):
        with RealDryFixture(activate=True) as fixture:
            _, decision = self._decision(fixture)
            finalized = self._finalize(fixture, decision)
            self._consume(fixture, decision, finalized=finalized)
            fixture.book._begin()
            try:
                with self.assertRaisesRegex(
                        ValueError, 'PERMIT_REPLAYED'):
                    fixture.book.supervised_engineering_authority.consume(
                        fixture.book, finalized['component'], 'other-action',
                        fixture._clock(), finalized=finalized)
            finally:
                fixture.book.db.rollback()
            self.assertEqual(len(self._rows(fixture)), 1)

    def test_real_audit_failure_rolls_back_consumption(self):
        with RealDryFixture(activate=True) as fixture:
            _, decision = self._decision(fixture)
            with patch.object(fixture.book.audit, 'append_in_transaction',
                              side_effect=ValueError('TEST_AUDIT_FAILURE')):
                with self.assertRaisesRegex(ValueError, 'TEST_AUDIT_FAILURE'):
                    self._consume(fixture, decision)
            self.assertEqual(self._rows(fixture), [])

    def test_real_restart_recheck_and_no_second_consumption(self):
        # Component-only history: direct consume, not a full promotion/READY.
        # Full READY/recovery is covered by the promotion test below.
        with RealDryFixture(activate=True) as fixture:
            action, decision = self._decision(fixture)
            finalized = self._finalize(fixture, decision)
            self._consume(fixture, decision, finalized=finalized)
            fixture.reopen()
            self.assertEqual(len(self._rows(fixture)), 1)
            self.assertEqual(
                fixture.book.supervised_engineering_authority.recheck_receipt(
                    decision['component'], book=fixture.book, action=action,
                    purpose='engineering'), [])
            with self.assertRaisesRegex(
                    ValueError, 'REAL_ASSESSMENT_GRANT_INVALID'):
                fixture.book.supervised_engineering_authority \
                    .finalize_promotion(
                        fixture.book, decision['component'], fixture.action_id,
                        fixture._clock(),
                        grant=decision['finalization_grant'])
            fixture.book._begin()
            try:
                replay = fixture.book.supervised_engineering_authority.consume(
                    fixture.book, decision['component'], fixture.action_id,
                    fixture._clock())
                fixture.book.db.commit()
            except BaseException:
                fixture.book.db.rollback()
                raise
            self.assertIs(replay.get('replayed'), True)
            self.assertEqual(replay['binding'], finalized['component']['binding'])
            self.assertEqual(len(self._rows(fixture)), 1)

    def test_real_rehashed_evidence_and_component_without_grant_rejected(self):
        with RealDryFixture(activate=True) as fixture:
            _, decision = self._decision(fixture)
            evidence = deepcopy(decision['source_evidence'])
            evidence['provenance_sha256'] = '0' * 64
            evidence['risk_lineage_id'] = 'parent-forged-lineage'
            evidence['source_sha256'] = digest({
                key: value for key, value in evidence.items()
                if key not in {'verified', 'status', 'reasons',
                               'source_sha256'}})
            component = deepcopy(decision['component'])
            component.update(
                provenance_sha256=evidence['provenance_sha256'],
                risk_lineage_id=evidence['risk_lineage_id'],
                real_source_evidence_sha256=evidence['source_sha256'],
                source_evidence_id=evidence['source_sha256'])
            with self.assertRaisesRegex(
                    ValueError, 'REAL_ASSESSMENT_GRANT_REQUIRED'):
                fixture.book.supervised_engineering_authority \
                    .finalize_promotion(
                        fixture.book, component, fixture.action_id,
                        fixture._clock())
            with self.assertRaisesRegex(
                    ValueError, 'ASSESSMENT_GRANT_MISMATCH|'
                                'REAL_SOURCE_EVIDENCE_MISMATCH'):
                fixture.book.supervised_engineering_authority \
                    .finalize_promotion(
                        fixture.book, component, fixture.action_id,
                        fixture._clock(),
                        grant=decision['finalization_grant'],
                        evidence=evidence)
            self.assertEqual(self._rows(fixture), [])

    def test_real_grant_is_authority_book_and_action_bound(self):
        from martin_agentic.supervised_engineering import (
            SupervisedEngineeringAuthority)
        from martin_agentic.execution import ExecutionBook
        from test_supervised_engineering import FakePreflight
        with RealDryFixture(activate=True) as fixture:
            _, decision = self._decision(fixture)
            other = SupervisedEngineeringAuthority(
                fixture.owner, clock=fixture._clock,
                real_activation_enabled=True)
            with self.assertRaisesRegex(
                    ValueError, 'REAL_ASSESSMENT_GRANT_INVALID'):
                other.finalize_promotion(
                    fixture.book, decision['component'], fixture.action_id,
                    fixture._clock(), grant=decision['finalization_grant'])
            with self.assertRaisesRegex(
                    ValueError, 'REAL_ASSESSMENT_GRANT_MISMATCH'):
                fixture.book.supervised_engineering_authority \
                    .finalize_promotion(
                        fixture.book, decision['component'], 'other-action',
                        fixture._clock(),
                        grant=decision['finalization_grant'])
            other_book = ExecutionBook(
                fixture.root / 'book.sqlite', fixture.policy,
                simulation=False, clock=fixture._clock,
                current_preflight=FakePreflight(fixture.facts))
            try:
                other_book.refresh_current_account_assessment(
                    purpose='engineering')
                with self.assertRaisesRegex(
                        ValueError, 'REAL_ASSESSMENT_GRANT_MISMATCH'):
                    fixture.book.supervised_engineering_authority \
                        .finalize_promotion(
                            other_book, decision['component'], fixture.action_id,
                            fixture._clock(),
                            grant=decision['finalization_grant'])
            finally:
                other_book.close()
            self.assertEqual(self._rows(fixture), [])

    def test_real_dry_stripped_marker_cannot_consume(self):
        with RealDryFixture(activate=True) as fixture:
            _, decision = self._decision(fixture)
            action = next(a for a in fixture.book.actions()
                          if a['id'] == fixture.action_id)
            dry = fixture.book.supervised_engineering_authority.dry_assess(
                book=fixture.book, action=action, purpose='engineering')
            self.assertTrue(dry['allowed'], dry)
            stripped = deepcopy(dry['component'])
            stripped.pop('decision_scope', None)
            stripped['activation_gate'] = 'ENABLED'
            with self.assertRaisesRegex(
                    ValueError, 'REAL_ASSESSMENT_GRANT_REQUIRED|'
                                'REAL_ASSESSMENT_GRANT_MISMATCH'):
                fixture.book.supervised_engineering_authority \
                    .finalize_promotion(
                        fixture.book, stripped, fixture.action_id,
                        fixture._clock(),
                        grant=decision['finalization_grant'])
            self.assertEqual(self._rows(fixture), [])

    def test_real_ready_recheck_requires_matching_consumption(self):
        with RealDryFixture(activate=True) as fixture:
            _, decision = self._decision(fixture)
            fixture.set_action(state='READY')
            action = next(a for a in fixture.book.actions()
                          if a['id'] == fixture.action_id)
            reasons = fixture.book.supervised_engineering_authority \
                .recheck_receipt(decision['component'], book=fixture.book,
                                 action=action, purpose='engineering')
            self.assertTrue(any('OWNER_CONSUMPTION_MISSING' in reason
                                for reason in reasons), reasons)
            self.assertEqual(self._rows(fixture), [])

    def test_real_unknown_after_consumption_not_reauthorized(self):
        with RealDryFixture(activate=True) as fixture:
            _, decision = self._decision(fixture)
            self._consume(fixture, decision)
            fixture.set_action(state='UNKNOWN')
            action = next(a for a in fixture.book.actions()
                          if a['id'] == fixture.action_id)
            reasons = fixture.book.supervised_engineering_authority \
                .recheck_receipt(decision['component'], book=fixture.book,
                                 action=action, purpose='engineering')
            self.assertTrue(any('ACTION_STATE' in reason for reason in reasons),
                            reasons)

    def _offline_promotion_gates(self, book):
        from contextlib import ExitStack
        stack = ExitStack()
        stack.enter_context(patch.object(
            book.entry_authority, 'check',
            return_value={'allowed': True, 'reason': 'OFFLINE_TEST_ENTRY'}))
        stack.enter_context(patch.object(
            book, '_current_preflight_decision',
            return_value={'allowed': True, 'reasons': [],
                          'entry_authorized': False}))
        stack.enter_context(patch.object(
            book, '_preflight_binding_ok', return_value=True))
        stack.enter_context(patch.object(
            book, '_check_engineering_verification', return_value=None))
        stack.enter_context(patch.object(
            book, 'engineering_verification_gate',
            return_value={'allowed': True, 'reason': 'OFFLINE_TEST_CONTROL'}))
        return stack

    def _offline_global_gates(self):
        from contextlib import ExitStack
        stack = ExitStack()
        stack.enter_context(patch(
            'martin_agentic.execution.receipt_of',
            return_value={'offline_test_receipt': True}))
        stack.enter_context(patch(
            'martin_agentic.execution.verify_receipt_binding',
            return_value={'bound': True}))
        return stack

    def test_real_full_promotion_ready_recheck_and_restart(self):
        with RealDryFixture(activate=True) as fixture:
            _, decision = self._decision(fixture)
            with self._offline_global_gates():
                with self._offline_promotion_gates(fixture.book):
                    promoted = fixture.book.promote_supervised_entry(
                        fixture.action_id)
            self.assertEqual(promoted['state'], 'READY')
            action = next(a for a in fixture.book.actions()
                          if a['id'] == fixture.action_id)
            self.assertEqual(action['state'], 'READY')
            self.assertEqual(len(self._rows(fixture)), 1)
            consumed_at = self._rows(fixture)[0][4]
            self.assertEqual(
                fixture.book.supervised_engineering_authority.recheck_receipt(
                    decision['component'], book=fixture.book, action=action,
                    purpose='engineering'), [])
            fixture.reopen()
            action = next(a for a in fixture.book.actions()
                          if a['id'] == fixture.action_id)
            self.assertEqual(action['state'], 'READY')
            self.assertEqual(
                fixture.book.supervised_engineering_authority.recheck_receipt(
                    decision['component'], book=fixture.book, action=action,
                    purpose='engineering'), [])
            fixture.book._begin()
            try:
                replay = fixture.book.supervised_engineering_authority.consume(
                    fixture.book, decision['component'], fixture.action_id,
                    fixture._clock())
                fixture.book.db.commit()
            except BaseException:
                fixture.book.db.rollback()
                raise
            self.assertEqual(replay['consumed_at'].isoformat(), consumed_at)
            self.assertEqual(len(self._rows(fixture)), 1)

    def test_real_two_connections_barrier_one_ready_and_consumption(self):
        import threading
        from martin_agentic.execution import ExecutionBook
        from test_supervised_engineering import FakePreflight
        import martin_agentic.execution as execution
        from unittest.mock import Mock
        original_receipt_of = execution.receipt_of
        original_verify_binding = execution.verify_receipt_binding
        with RealDryFixture(activate=True) as fixture:
            fixture.book.close()
            fixture.book = None
            barrier = threading.Barrier(2)
            results = []
            lock = threading.Lock()

            def worker():
                book = ExecutionBook(
                    fixture.root / 'book.sqlite', fixture.policy,
                    simulation=False, clock=fixture._clock,
                    current_preflight=FakePreflight(fixture.facts))
                try:
                    def provenance_reader(book, result):
                        raw = result['permit']['permit']
                        record = {
                            'request_id': result['request']['request_id'],
                            'action_id': raw['action_id'],
                            'request_sha256': raw['request_sha256']}
                        return {'kind': 'host_owner_request',
                                'record': record,
                                'record_sha256': digest(record)}
                    def risk_reader(book, result):
                        return {
                            'status': 'bound', 'lineage_id': 'lineage-fixture',
                            'risk_state_sha256': digest(book.risk_guard.state()),
                            'account_key': fixture.account_key,
                            'purpose': 'engineering',
                            'source': 'owner-risk-lineage-v1',
                            'observed_at': (
                                fixture._clock() - timedelta(seconds=1)
                            ).isoformat(),
                            'valid_until': (
                                fixture._clock() + timedelta(seconds=60)
                            ).isoformat()}
                    book.configure_engineering_source(
                        fixture.owner,
                        provenance_reader=provenance_reader,
                        risk_lineage_reader=risk_reader,
                        allow_synthetic=False, real_activation_enabled=True)
                    book.refresh_current_account_assessment(
                        purpose='engineering')
                    barrier.wait(timeout=10)
                    with self._offline_promotion_gates(book):
                        result = book.promote_supervised_entry(
                            fixture.action_id)
                    with lock:
                        results.append(('ok', result))
                except BaseException as exc:
                    with lock:
                        results.append(('error', type(exc).__name__,
                                        str(exc)))
                finally:
                    book.close()

            global_gates = self._offline_global_gates()
            global_gates.__enter__()
            try:
                threads = [threading.Thread(target=worker) for _ in range(2)]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join(timeout=30)
                self.assertFalse(any(thread.is_alive() for thread in threads))
            finally:
                global_gates.__exit__(None, None, None)
            self.assertIs(execution.receipt_of, original_receipt_of)
            self.assertIs(execution.verify_receipt_binding,
                          original_verify_binding)
            self.assertNotIsInstance(execution.receipt_of, Mock)
            self.assertNotIsInstance(execution.verify_receipt_binding, Mock)
            successes = [item for item in results if item[0] == 'ok']
            self.assertEqual(len(successes), 1, results)
            self.assertEqual(successes[0][1]['state'], 'READY')
            probe = ExecutionBook(
                fixture.root / 'book.sqlite', fixture.policy,
                simulation=False, clock=fixture._clock,
                current_preflight=FakePreflight(fixture.facts))
            try:
                self.assertEqual(probe.db.execute(
                    'SELECT count(*) FROM formal_authorization_consumptions'
                ).fetchone()[0], 1)
                self.assertEqual(probe.db.execute(
                    "SELECT count(*) FROM live_actions WHERE kind='entry'"
                ).fetchone()[0], 1)
                self.assertEqual(probe.db.execute(
                    "SELECT count(*) FROM audit_events WHERE "
                    "event_id LIKE 'supervised-intent-%-promoted'"
                ).fetchone()[0], 1)
                self.assertEqual(next(a for a in probe.actions()
                                      if a['id'] == fixture.action_id)['state'],
                                 'READY')
            finally:
                probe.close()

    def test_real_full_promotion_rollback_after_consume_ready_and_audit(self):
        for mode in ('after_consume_insert', 'after_ready', 'after_audit'):
            with self.subTest(mode=mode):
                with RealDryFixture(activate=True) as fixture:
                    _, decision = self._decision(fixture)
                    original = fixture.book.audit.append_in_transaction
                    calls = {'count': 0}

                    def fail_audit(event):
                        calls['count'] += 1
                        if ((mode == 'after_consume_insert'
                             and calls['count'] == 1)
                                or (mode == 'after_ready'
                                    and calls['count'] == 2)):
                            raise ValueError('TEST_' + mode)
                        return original(event)

                    patches = self._offline_promotion_gates(fixture.book)
                    if mode == 'after_audit':
                        patches.enter_context(patch.object(
                            fixture.book.supervised_engineering_authority,
                            'final_consume_check',
                            side_effect=ValueError('TEST_after_audit')))
                    patches.enter_context(patch.object(
                        fixture.book.audit, 'append_in_transaction',
                        side_effect=fail_audit))
                    with self._offline_global_gates(), patches:
                        with self.assertRaisesRegex(
                                ValueError, 'TEST_after_'):
                            fixture.book.promote_supervised_entry(
                                fixture.action_id)
                    self.assertEqual(self._rows(fixture), [])
                    self.assertEqual(next(
                        a for a in fixture.book.actions()
                        if a['id'] == fixture.action_id)['state'],
                        'WAITING_USER')
                    self.assertEqual(fixture.book.db.execute(
                        "SELECT count(*) FROM audit_events WHERE "
                        "event_id LIKE 'supervised-intent-%-promoted'"
                    ).fetchone()[0], 0)

    def test_real_mutations_after_finalize_before_first_consume_are_rejected(self):
        cases = (
            ('registry-delete', lambda f: f.delete_registry(), 'CAPTURE'),
            ('registry-replace', lambda f: f.write_registry(
                {**f.registry, 'generation_id': 'e2' * 32}), 'CAPTURE'),
            ('action-unknown', lambda f: f.set_action(state='UNKNOWN'),
             'ACTION_STATE'),
            ('action-terminal', lambda f: f.set_action(state='CANCELLED'),
             'ACTION_STATE'),
            ('action-payload', lambda f: f.set_action_payload(
                {'risk_terms': {'wrong': True}}),
             'ACTION|EVIDENCE'),
            ('facts-body', lambda f: f.mutate_facts(
                {'status': 'changed'}), 'FACTS'),
            ('facts-id', lambda f: f.mutate_facts(
                {'assessment_id': '0' * 64}, rehash=False), 'FACTS'),
            ('facts-expiry', lambda f: f.mutate_facts({
                'observation_valid_until': (NOW - timedelta(
                    seconds=1)).isoformat()}), 'FACTS'),
            ('review-state', lambda f: f.set_review_state('PENDING'),
             'REVIEW'),
            ('review-receipt', lambda f: f.mutate_review_result(
                'approval'), 'REVIEW'),
            ('owner-revoke', lambda f: f.revoke_permit(), 'PERMIT'),
            ('owner-replace', lambda f: f.write_permit(
                {**f.read_permit(), 'request_id': 'request-replaced'}),
             'PERMIT|OWNER'),
            ('request-replace', lambda f: f.write_request(
                {**__import__('json').loads(f.request_bytes()),
                 'request_id': 'request-replaced'}), 'REQUEST|OWNER'),
            ('risk-state', lambda f: f.set_risk_row({'mutated': True}),
             'RISK|EVIDENCE'),
        )
        for label, mutate, expected in cases:
            with self.subTest(label=label):
                with RealDryFixture(activate=True) as fixture:
                    action, decision = self._decision(fixture)
                    finalized = self._finalize(fixture, decision)
                    mutate(fixture)
                    with self.assertRaisesRegex(ValueError, expected):
                        self._consume(fixture, decision, finalized=finalized)
                    self.assertEqual(self._rows(fixture), [])
                    self.assertEqual(next(
                        a for a in fixture.book.actions()
                        if a['id'] == fixture.action_id)['state'],
                        'WAITING_USER' if label not in {
                            'action-unknown', 'action-terminal'} else
                        ('UNKNOWN' if label == 'action-unknown'
                         else 'CANCELLED'))
                    self.assertEqual(fixture.book.db.execute(
                        "SELECT count(*) FROM audit_events WHERE "
                        "event_id LIKE 'supervised-permit-%consumed'"
                    ).fetchone()[0], 0)

    def test_real_finalize_to_consume_risk_window_expiry_rejected(self):
        with RealDryFixture(activate=True) as fixture:
            _, decision = self._decision(fixture)
            finalized = self._finalize(fixture, decision)
            fixture.advance(31)
            with self.assertRaisesRegex(
                    ValueError, 'FINALIZATION_EXPIRED|PERMIT_WINDOW|RISK'):
                self._consume(fixture, decision, finalized=finalized)
            self.assertEqual(self._rows(fixture), [])

    def test_real_concurrency_patch_cleanup_then_formal_source_binding(self):
        import martin_agentic.execution as execution
        from unittest.mock import Mock
        from test_formal_authority import FormalExecutionConsumptionTests
        receipt_of = execution.receipt_of
        verify_binding = execution.verify_receipt_binding
        self.test_real_two_connections_barrier_one_ready_and_consumption()
        self.assertIs(execution.receipt_of, receipt_of)
        self.assertIs(execution.verify_receipt_binding, verify_binding)
        self.assertNotIsInstance(execution.receipt_of, Mock)
        self.assertNotIsInstance(execution.verify_receipt_binding, Mock)
        formal = FormalExecutionConsumptionTests(
            'test_native_final_transaction_rechecks_changed_source')
        formal.setUp()
        try:
            formal.test_native_final_transaction_rechecks_changed_source()
        finally:
            formal.doCleanups()
        self.assertIs(execution.receipt_of, receipt_of)
        self.assertIs(execution.verify_receipt_binding, verify_binding)

    def test_real_current_source_account_policy_after_finalize_rejected(self):
        from unittest.mock import patch
        with RealDryFixture(activate=True) as fixture:
            _, decision = self._decision(fixture)
            finalized = self._finalize(fixture, decision)
            changed = {**decision['component']['source_pin'],
                       'digest': '0' * 64}
            with patch('martin_agentic.qualification.source_pin',
                       return_value=changed):
                with self.assertRaisesRegex(
                        ValueError, 'SOURCE_CHANGED'):
                    self._consume(fixture, decision, finalized=finalized)
            self.assertEqual(self._rows(fixture), [])
        with RealDryFixture(activate=True) as fixture:
            _, decision = self._decision(fixture)
            finalized = self._finalize(fixture, decision)
            with patch('martin_agentic.qualification.source_pin',
                       return_value={'status': 'limit_exceeded',
                                     'truncated': True}):
                with self.assertRaisesRegex(
                        ValueError, 'SOURCE_PIN_UNAVAILABLE'):
                    self._consume(fixture, decision, finalized=finalized)
            self.assertEqual(self._rows(fixture), [])
        with RealDryFixture(activate=True) as fixture:
            _, decision = self._decision(fixture)
            finalized = self._finalize(fixture, decision)
            state = fixture.book.state()
            state['account_key'] = '0' * 64
            fixture.book._save(state)
            fixture.book.db.commit()
            with self.assertRaisesRegex(ValueError, 'ACCOUNT_CHANGED'):
                self._consume(fixture, decision, finalized=finalized)
            self.assertEqual(self._rows(fixture), [])
        with RealDryFixture(activate=True) as fixture:
            _, decision = self._decision(fixture)
            finalized = self._finalize(fixture, decision)
            original_suffix = fixture.book.policy['account_suffix']
            fixture.book.policy['account_suffix'] = '9999'
            self.assertNotEqual(fixture.book.policy['account_suffix'], original_suffix)
            with self.assertRaisesRegex(ValueError, 'POLICY_CHANGED'):
                self._consume(fixture, decision, finalized=finalized)
            self.assertEqual(self._rows(fixture), [])
