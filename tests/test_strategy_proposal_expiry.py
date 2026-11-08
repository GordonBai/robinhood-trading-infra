"""Typed proposal validity survives the durable execution handoff."""
from datetime import timedelta
import json
from unittest import TestCase
from unittest.mock import patch

import test_execution as execution_fixture


class StrategyProposalExpiryTests(TestCase):
    def setUp(self):
        self.fixture = execution_fixture.ExecutionTests(methodName='runTest')
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.book = self.fixture.book

    def evidence(self, *, duration=5):
        now = self.fixture.now
        return {'source_sha256': 'synthetic-proposal-fixture', 'synthetic': True,
                'strategy_proposal_id': 'typed-proposal-1',
                'strategy_generated_at': (now - timedelta(seconds=1)).isoformat(),
                'strategy_valid_until': (now + timedelta(seconds=duration)).isoformat()}

    def propose(self, evidence):
        return self.book.propose('typed-signal-1', self.fixture.plan,
                                 self.fixture.quote, self.fixture.snap(), evidence=evidence)

    def assert_no_intent(self):
        self.assertEqual(self.book.actions(), [])
        self.assertEqual(self.book.db.execute('SELECT count(*) FROM live_signals').fetchone()[0], 0)
        self.assertEqual(self.book.state()['trades'], {})
        self.assertFalse(self.book.db.in_transaction)

    def test_short_proposal_deadline_is_persisted_without_extension(self):
        evidence = self.evidence()
        self.propose(evidence)
        payload = json.loads(self.book.actions()[0]['payload'])
        self.assertEqual(payload['expires_at'], evidence['strategy_valid_until'])
        self.assertEqual(self.book.state()['trades']['typed-signal-1']['evidence'], evidence)

    def test_long_proposal_cannot_extend_default_entry_delivery_deadline(self):
        self.propose(self.evidence(duration=60))
        payload = json.loads(self.book.actions()[0]['payload'])
        self.assertEqual(payload['expires_at'], (self.fixture.now + timedelta(seconds=15)).isoformat())

    def test_expired_proposal_is_rejected_before_intent(self):
        with self.assertRaisesRegex(ValueError, '^STRATEGY_PROPOSAL_EXPIRED$'):
            self.propose(self.evidence(duration=0))
        self.assert_no_intent()

    def test_future_or_partial_envelope_cannot_create_intent(self):
        future = self.evidence()
        future['strategy_generated_at'] = (self.fixture.now + timedelta(seconds=1)).isoformat()
        partial = self.evidence()
        del partial['strategy_valid_until']
        for evidence in (future, partial):
            with self.subTest(evidence=evidence):
                with self.assertRaisesRegex(ValueError, '^STRATEGY_PROPOSAL_VALIDITY_INVALID$'):
                    self.propose(evidence)
                self.assert_no_intent()

    def test_claim_at_proposal_deadline_expires_action_without_dispatch(self):
        self.propose(self.evidence())
        self.fixture.now += timedelta(seconds=5)
        self.assertIsNone(self.book.claim())
        self.assertEqual(self.book.actions()[0]['state'], 'EXPIRED')
        self.assertIsNone(self.book.claim())

    def test_lock_wait_expiry_rolls_back_entire_execution_intent(self):
        evidence = self.evidence()
        original = self.book._begin

        def delayed_begin():
            original()
            self.fixture.now += timedelta(seconds=5)

        with patch.object(self.book, '_begin', side_effect=delayed_begin):
            with self.assertRaisesRegex(ValueError, '^STRATEGY_PROPOSAL_EXPIRED$'):
                self.propose(evidence)
        self.assert_no_intent()

    def test_expiry_during_persistence_rolls_back_execution_intent(self):
        evidence = self.evidence()
        original = self.book._save

        def delayed_save(state):
            original(state)
            self.fixture.now += timedelta(seconds=5)

        with patch.object(self.book, '_save', side_effect=delayed_save):
            with self.assertRaisesRegex(ValueError, '^STRATEGY_PROPOSAL_EXPIRED$'):
                self.propose(evidence)
        self.assert_no_intent()
