from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest import TestCase

from trading_contracts.strategy import (
    StrategyIdentity, StrategyInput, StrategyProposal, StrategyDecision, ProposalAction,
)


NOW = datetime(2026, 10, 28, 15, tzinfo=timezone.utc)
IDENTITY = StrategyIdentity('instance-a', 'martin', 'v1')


class StrategyContractsTests(TestCase):
    def proposal(self, **changes):
        fields = dict(identity=IDENTITY, proposal_id='proposal-a', decision_id='decision-a',
            setup_id='setup-a', data_snapshot_id='snapshot-a', source_sha256='a'*64,
            generated_at=NOW, valid_until=NOW+timedelta(seconds=15), action=ProposalAction.ENTER,
            symbol='DEMO', reason='fixture', synthetic=True, quantity_limit=1,
            entry_ceiling=Decimal('10'), stop_price=Decimal('9'))
        fields.update(changes)
        return StrategyProposal(**fields)

    def decision(self, **changes):
        fields = dict(identity=IDENTITY, decision_id='decision-a', as_of=NOW,
            data_snapshot_id='snapshot-a', consumed_cursor='event-a', synthetic=True,
            proposals=[self.proposal()])
        fields.update(changes)
        return StrategyDecision(**fields)

    def test_inputs_copy_nested_payloads_and_expose_no_mutable_alias(self):
        market = {'bars': [{'close': Decimal('10')}], 'healthy': True}
        checkpoint = {'seen': ['event-old']}
        value = StrategyInput(IDENTITY, NOW, 'event-a', 'snapshot-a', 'a'*64, 'config-v1', True,
            market=market, checkpoint=checkpoint, allocation={'instance_id': 'instance-a', 'quantity': 1})
        market['bars'][0]['close'] = Decimal('50'); checkpoint['seen'].append('event-new')
        self.assertEqual(value.market['bars'][0]['close'], Decimal('10'))
        self.assertEqual(value.checkpoint['seen'], ('event-old',))
        with self.assertRaises(TypeError): value.market['bars'][0]['close'] = Decimal('20')
        with self.assertRaises(FrozenInstanceError): value.cursor = 'changed'

    def test_payload_rejects_credentials_execution_claims_and_objects(self):
        for payload in ({'nested': {'access_token': 'forbidden'}}, {'order_id': 'not-an-advice'},
                        {'execution_authorized': True}, {'book': object()}, {'value': 1.5},
                        {'value': Decimal('NaN')}):
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                self.proposal(evidence=payload)

    def test_proposal_rejects_float_bool_fractional_short_and_invalid_time(self):
        for changes in ({'quantity_limit': True}, {'quantity_limit': Decimal('1')},
                        {'quantity_limit': 1.5}, {'quantity_limit': 0}, {'side': 'short'},
                        {'entry_ceiling': 10.0}, {'stop_price': Decimal('Infinity')},
                        {'generated_at': NOW.replace(tzinfo=None)}, {'valid_until': NOW},
                        {'source_sha256': 'fixture'}, {'synthetic': 1}, {'symbol': 'demo'}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.proposal(**changes)

    def test_actions_have_distinct_long_only_advisory_semantics(self):
        self.assertFalse(self.proposal().execution_authorized)
        for action in (ProposalAction.EXIT, ProposalAction.HOLD):
            self.proposal(action=action, quantity_limit=None, entry_ceiling=None, stop_price=None)
        self.proposal(action=ProposalAction.REDUCE, entry_ceiling=None, stop_price=None)
        self.proposal(action=ProposalAction.TIGHTEN_PROTECTION, quantity_limit=None,
            entry_ceiling=None, stop_price=Decimal('9.5'))
        with self.assertRaises(ValueError):
            self.proposal(action=ProposalAction.HOLD, quantity_limit=None)
        with self.assertRaises(ValueError):
            self.proposal(action=ProposalAction.REDUCE, quantity_limit=None, entry_ceiling=None, stop_price=None)

    def test_cross_instance_source_time_and_synthetic_bindings_are_enforced(self):
        for proposal in (self.proposal(identity=StrategyIdentity('instance-b', 'martin', 'v1')),
                         self.proposal(data_snapshot_id='snapshot-b'),
                         self.proposal(decision_id='another-decision'),
                         self.proposal(generated_at=NOW+timedelta(seconds=1)),
                         self.proposal(synthetic=False)):
            with self.assertRaisesRegex(ValueError, 'BINDING'):
                self.decision(proposals=[proposal])
        with self.assertRaisesRegex(ValueError, 'DUPLICATE'):
            self.decision(proposals=[self.proposal(), self.proposal()])
        with self.assertRaisesRegex(ValueError, 'INSTANCE'):
            StrategyInput(IDENTITY, NOW, 'event-a', 'snapshot-a', 'a'*64, 'v1', True,
                allocation={'instance_id': 'another-instance'})

    def test_decision_freezes_aliases_and_does_not_claim_execution(self):
        proposals = [self.proposal()]; state = {'next': ['event-a']}
        result = self.decision(proposals=proposals, next_checkpoint=state)
        proposals.clear(); state['next'].clear()
        self.assertEqual(len(result.proposals), 1)
        self.assertEqual(result.next_checkpoint['next'], ('event-a',))
        self.assertFalse(hasattr(result, 'account_number'))
        self.assertEqual(replace(result, diagnostics={'result': 'advisory'}).diagnostics['result'], 'advisory')

    def test_host_validation_binds_returned_decision_to_exact_issued_input(self):
        request = StrategyInput(IDENTITY, NOW, 'event-a', 'snapshot-a', 'a'*64, 'v1', True)
        decision = self.decision()
        self.assertIs(decision.validate_for(request), decision)
        for changed in (replace(request, source_sha256='b'*64), replace(request, cursor='event-b'),
                        replace(request, identity=StrategyIdentity('instance-b', 'martin', 'v1')),
                        replace(request, as_of=NOW+timedelta(seconds=1)),
                        replace(request, data_snapshot_id='snapshot-b'), replace(request, synthetic=False)):
            with self.subTest(changed=changed), self.assertRaisesRegex(ValueError, 'INPUT_BINDING'):
                decision.validate_for(changed)
