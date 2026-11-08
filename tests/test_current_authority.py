"""Canonical risk/lineage/control authority completeness tests (temp/fake only)."""
from copy import deepcopy
from datetime import datetime
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
from unittest import TestCase
from unittest.mock import MagicMock, patch

from martin_agentic.assessment_receipt import (
    _control_component, _lineage_component, _risk_component,
    read_book_authority)
from martin_agentic.live_risk import LiveRiskGuard
from test_broker import FakeBroker, snapshot
from test_broker_bundle import AT


class AuthorityTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.now = datetime.fromisoformat(AT)
        self.policy = json.loads(
            Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        self.snapshot = snapshot(FakeBroker(), self.now.isoformat())
        self.guard = LiveRiskGuard(
            Path(self.temp.name) / 'risk.sqlite', self.policy,
            simulation=False)
        self.addCleanup(self.guard.close)
        self.state = self.guard.observe(self.snapshot)
        self.book = SimpleNamespace(
            risk_guard=self.guard, policy=self.policy,
            simulation=False, clock=lambda: self.now,
            state=lambda: {'account_key': self.snapshot['account_key'],
                           'last_snapshot': self.snapshot['observed_at']})

    def component(self, state=None):
        target = MagicMock()
        target.state.return_value = deepcopy(self.state if state is None else state)
        book = SimpleNamespace(
            risk_guard=target, policy=self.policy, simulation=False,
            clock=lambda: self.now,
            state=lambda: {'account_key': self.snapshot['account_key'],
                           'last_snapshot': self.snapshot['observed_at']})
        return _risk_component(book, self.snapshot, self.now)

    def test_authority_read_does_not_modify_risk_state(self):
        before = self.guard.db.execute(
            'SELECT payload FROM account_risk WHERE id=1').fetchone()[0]
        self.component()
        after = self.guard.db.execute(
            'SELECT payload FROM account_risk WHERE id=1').fetchone()[0]
        self.assertEqual(before, after)

    def test_real_risk_state_is_evaluated_with_full_binding(self):
        value = self.component()
        self.assertEqual(value['status'], 'evaluated', value)
        self.assertFalse(value['blocked'])
        self.assertEqual(value['account_key'], self.snapshot['account_key'])
        self.assertEqual(value['at'], self.snapshot['observed_at'])

    def test_empty_and_malformed_risk_states_are_not_evaluated(self):
        for state in (
                {},
                {**self.state, 'blocked': 'false'},
                {key: value for key, value in self.state.items()
                 if key != 'blocked'},
                {**self.state, 'account_key': '0' * 64},
                {**self.state, 'terms': '0' * 64},
                {**self.state, 'simulation': True},
                {**self.state, 'day': '1999-01-01'},
                {**self.state, 'snapshot_hash': '0' * 64},
                {**self.state, 'at': '2026-10-19T13:58:00+00:00'},
                {**self.state, 'at': '2026-10-19T14:00:31+00:00'},
                {**self.state, 'nav_high': 'not-a-number'},
                {**self.state, 'blocked': False,
                 'sticky_reasons': ['UNRECONCILED_CASH_FLOW']},
                {**self.state, 'blocked': False,
                 'project_halted': True}):
            value = self.component(state)
            self.assertEqual(value['status'], 'not_evaluated', state)
            self.assertIsNone(value['blocked'])

    def test_existing_project_lock_is_evaluated_and_blocked(self):
        locked = {**self.state, 'blocked': True, 'project_halted': True,
                  'sticky_reasons': ['UNRECONCILED_CASH_FLOW'],
                  'reasons': ['PROJECT_DRAWDOWN_LOCKED']}
        value = self.component(locked)
        self.assertEqual(value['status'], 'evaluated', value)
        self.assertTrue(value['blocked'])
        self.assertIn('PROJECT_DRAWDOWN_LOCKED', value['reason'])

    def test_unbound_book_account_cannot_evaluate_risk(self):
        for bad in ({}, {'account_key': None}, None, [], 'bad'):
            book = SimpleNamespace(
                risk_guard=MagicMock(state=MagicMock(return_value=self.state)),
                policy=self.policy, simulation=False, clock=lambda: self.now,
                state=lambda value=bad: value)
            value = _risk_component(book, self.snapshot, self.now)
            self.assertEqual(value['status'], 'not_evaluated', bad)
            self.assertIsNone(value['blocked'])

    def test_persistent_risk_lock_survives_restart_without_reset(self):
        self.guard.db.execute(
            'UPDATE account_risk SET payload=? WHERE id=1',
            (json.dumps({**self.state, 'blocked': True,
                         'project_halted': True,
                         'reasons': ['PROJECT_DRAWDOWN_LOCKED'],
                         'sticky_reasons': ['UNRECONCILED_CASH_FLOW']}),
             ))
        self.guard.db.commit()
        self.guard.close()
        reopened = LiveRiskGuard(
            Path(self.temp.name) / 'risk.sqlite', self.policy,
            simulation=False)
        self.addCleanup(reopened.close)
        self.assertTrue(reopened.state()['project_halted'])
        self.assertEqual(reopened.state()['sticky_reasons'],
                         ['UNRECONCILED_CASH_FLOW'])

    def test_missing_lineage_or_control_fields_are_not_unblocked(self):
        book = SimpleNamespace(
            risk_guard=MagicMock(state=MagicMock(return_value=self.state)),
            policy=self.policy, simulation=False, clock=lambda: self.now,
            state=lambda: {'account_key': self.snapshot['account_key']},
            engineering_verification_gate=MagicMock(return_value={
                'blocked': False, 'applicable': True,
                'reason': 'LOCAL_USER_HOLD_CLEAR_NOT_EXECUTION_AUTHORITY',
                'execution_authorized': False}))
        for bad in ({}, {'blocked': 'false', 'reason': 'x', 'blockers': []},
                    {'blocked': False}, {'blocked': False, 'reason': 'x'}):
            with patch('martin_agentic.session_lineage.lineage_entry_gate',
                       return_value=bad):
                value = _lineage_component(book, self.snapshot, self.now)
            self.assertEqual(value['status'], 'not_evaluated', bad)
        for bad in ({}, {'blocked': False}, {'blocked': 'false',
                                             'applicable': False,
                                             'reason': 'x'},
                    {'blocked': False, 'applicable': True,
                     'execution_authorized': True, 'reason': 'x'}):
            book.engineering_verification_gate.return_value = bad
            value = _control_component(book, 'engineering', {})
            self.assertEqual(value['status'], 'not_evaluated', bad)
        book.engineering_verification_gate.return_value = {
            'blocked': False, 'applicable': False,
            'reason': 'NONCANONICAL_RETAINS_EXISTING_LINEAGE_CHECKS',
            'execution_authorized': False}
        value = _control_component(book, 'engineering', {})
        self.assertEqual(value['status'], 'evaluated')
        self.assertFalse(value['applicable'])

    def test_valid_lineage_and_control_results_are_bound(self):
        book = SimpleNamespace(
            risk_guard=MagicMock(state=MagicMock(return_value=self.state)),
            policy=self.policy, simulation=False, clock=lambda: self.now,
            state=lambda: {'account_key': self.snapshot['account_key']},
            engineering_verification_gate=MagicMock(return_value={
                'blocked': False, 'applicable': True,
                'reason': 'LOCAL_USER_HOLD_CLEAR_NOT_EXECUTION_AUTHORITY',
                'execution_authorized': False}))
        value = {
            'blocked': False, 'reason': 'LINEAGE_CURRENT_FRESH_SOURCES_VERIFIED',
            'blockers': [], 'registered_sessions': 1, 'registry_sequence': 1,
            'registry_head_hash': 'a' * 64,
            'snapshot_at': self.snapshot['observed_at'],
            'automatic_execution_enabled': False}
        with patch('martin_agentic.session_lineage.lineage_entry_gate',
                   return_value=value):
            lineage = _lineage_component(book, self.snapshot, self.now)
        self.assertEqual(lineage['status'], 'evaluated', lineage)
        self.assertFalse(lineage['blocked'])
        control = _control_component(book, 'engineering', {})
        self.assertEqual(control['status'], 'evaluated', control)
        self.assertTrue(control['applicable'])

    def test_empty_public_authority_reader_is_not_evaluated(self):
        book = MagicMock()
        book.risk_guard.state.return_value = {}
        book.clock.return_value = self.now
        book.engineering_verification_gate.return_value = {}
        with patch('martin_agentic.session_lineage.lineage_entry_gate',
                   return_value={}):
            value = read_book_authority(
                book, purpose='engineering', snapshot={})
        for name in ('risk', 'lineage', 'control'):
            self.assertEqual(value[name]['status'], 'not_evaluated', name)
            self.assertIsNone(value[name]['blocked'])
