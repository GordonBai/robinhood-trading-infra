"""Deterministic fixture-only plugin checks; no database or broker transport."""
import ast
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from unittest import TestCase

from trading_contracts.strategy import StrategyIdentity, StrategyInput, ProposalAction
from trading_contracts.values import digest, timestamp, thaw
from trading_infra.market_calendar import ExchangeCalendarView
from trading_strategies.martin.signals import MartinSignalPlugin
from shadow_fixture import events


class MartinSignalPluginTests(TestCase):
    def setUp(self):
        self.plugin = MartinSignalPlugin(calendar_view=ExchangeCalendarView())
        self.identity = StrategyIdentity('martin-plugin-fixture', 'martin', self.plugin.VERSION)
        self.events = [{**event, 'source_sha256': 'fixture-'+event['id'], 'synthetic_input': True}
            for event in events()[1:5]]

    def request(self, event, checkpoint=None, **changes):
        args = dict(identity=self.identity, as_of=timestamp(event['at']), cursor=event['id'],
            data_snapshot_id='event-'+digest(event), source_sha256=digest(event), config_version='fixture-v1',
            synthetic=True, market={'event': event}, checkpoint=checkpoint or {})
        args.update(changes)
        return StrategyInput(**args)

    def prefix(self, count):
        checkpoint = {}
        for event in self.events[:count]:
            decision = self.plugin.decide(self.request(event, checkpoint))
            checkpoint = thaw(decision.next_checkpoint)
        return checkpoint

    def test_baseline_universe_flush_reclaim_trigger_prices_and_checkpoint(self):
        checkpoint = {}
        states = []
        for event in self.events:
            request = self.request(event, checkpoint)
            decision = self.plugin.decide(request)
            self.assertIs(decision.validate_for(request), decision)
            checkpoint = thaw(decision.next_checkpoint)
            states.append(checkpoint['machines']['DEMO']['state'])
        self.assertEqual(states, ['WAIT_FLUSH', 'WAIT_RECLAIM', 'ARMED', 'TRIGGERED'])
        self.assertEqual(len(decision.proposals), 1)
        proposal = decision.proposals[0]
        self.assertEqual(proposal.action, ProposalAction.ENTER)
        self.assertEqual(proposal.symbol, 'DEMO')
        self.assertIsNone(proposal.quantity_limit)
        machine = checkpoint['machines']['DEMO']
        tick = Decimal(machine['setup']['tick'])
        trigger = Decimal(machine['trigger'])
        raw = min(Decimal(self.events[-1]['data']['ask'])+tick, trigger*Decimal('1.002'))
        self.assertEqual(proposal.entry_ceiling, raw // tick * tick)
        self.assertEqual(proposal.stop_price, Decimal(machine['stop']))
        self.assertEqual(proposal.evidence['cost_buffer_per_share'], '0.05')
        self.assertEqual(proposal.evidence['source_sha256'], self.events[-1]['source_sha256'])
        self.assertEqual(proposal.evidence['legacy_signal_id'], 'signal-'+digest([
            self.plugin.VERSION, 'DEMO', timestamp(machine['setup']['known_at']).isoformat()]))
        self.assertNotIn('pending_signals', checkpoint)
        self.assertNotIn('signal_results', checkpoint)

    def test_same_input_is_deterministic_and_does_not_read_budget_for_sizing(self):
        request = self.request(self.events[-1], self.prefix(3))
        before = thaw(request.checkpoint)
        first = self.plugin.decide(request)
        self.assertEqual(first, self.plugin.decide(request))
        other_budget = self.plugin.decide(replace(request, budget={'max_notional': 0}))
        self.assertNotEqual(first.decision_id, other_budget.decision_id)
        self.assertEqual(first.proposals[0].entry_ceiling, other_budget.proposals[0].entry_ceiling)
        self.assertEqual(first.proposals[0].stop_price, other_budget.proposals[0].stop_price)
        self.assertEqual(thaw(request.checkpoint), before)
        self.assertIsNone(first.proposals[0].quantity_limit)

    def test_decision_identity_binds_checkpoint_config_time_and_constructor_cost(self):
        request = self.request(self.events[-1], self.prefix(3))
        first = self.plugin.decide(request)
        checkpoint = thaw(request.checkpoint); checkpoint['local_marker'] = 'different-input'
        variants = [self.plugin.decide(replace(request, checkpoint=checkpoint)),
                    self.plugin.decide(replace(request, config_version='fixture-v2')),
                    self.plugin.decide(replace(request, as_of=request.as_of+timedelta(seconds=1))),
                    MartinSignalPlugin(calendar_view=ExchangeCalendarView(),
                        cost_buffer_per_share='0.10').decide(request)]
        ids = {first.decision_id}
        for other in variants:
            self.assertNotIn(other.decision_id, ids)
            ids.add(other.decision_id)
            self.assertNotEqual(first.proposals[0].proposal_id, other.proposals[0].proposal_id)
            self.assertEqual(first.proposals[0].evidence['legacy_signal_id'],
                             other.proposals[0].evidence['legacy_signal_id'])

    def test_proposal_identity_is_namespaced_without_changing_legacy_reference(self):
        request = self.request(self.events[-1], self.prefix(3))
        first = self.plugin.decide(request)
        other = self.plugin.decide(replace(request,
            identity=StrategyIdentity('different-instance', 'martin', self.plugin.VERSION)))
        self.assertNotEqual(first.proposals[0].proposal_id, other.proposals[0].proposal_id)
        self.assertEqual(first.proposals[0].evidence['legacy_signal_id'], other.proposals[0].evidence['legacy_signal_id'])
        self.assertNotEqual(first.decision_id, other.decision_id)

    def test_revised_last_bar_is_rejected_without_mutating_input_checkpoint(self):
        checkpoint = self.prefix(2)
        revised = deepcopy(self.events[1]); revised['id'] = 'revised-bar'
        revised['data']['close'] = str(Decimal(revised['data']['close'])+Decimal('.01'))
        request = self.request(revised, checkpoint)
        before = thaw(request.checkpoint)
        with self.assertRaisesRegex(ValueError, 'CONSUMED_BAR_REVISED'):
            self.plugin.decide(request)
        self.assertEqual(thaw(request.checkpoint), before)
        self.assertEqual(checkpoint, before)

    def test_gap_invalidates_setup_and_missing_universe_cannot_trigger(self):
        skipped = deepcopy(self.events[2]); skipped['id'] = 'gap-bar'
        for key in ('begins_at', 'available_at', 'observed_at'):
            skipped['data'][key] = (timestamp(skipped['data'][key])+timedelta(minutes=5)).isoformat()
        skipped['at'] = (timestamp(skipped['at'])+timedelta(minutes=5)).isoformat()
        decision = self.plugin.decide(self.request(skipped, self.prefix(2)))
        self.assertFalse(decision.proposals)
        machine = decision.next_checkpoint['machines']['DEMO']
        self.assertEqual(machine['state'], 'INVALIDATED')
        self.assertEqual(machine['reason'], 'BAR_GAP_REQUIRES_NEW_SETUP')
        missing = self.plugin.decide(self.request(self.events[-1]))
        self.assertFalse(missing.proposals)
        self.assertEqual(missing.diagnostics['reason'], 'NO_QUALIFIED_SETUP')

    def test_event_source_provenance_clock_and_host_state_are_checked(self):
        request = self.request(self.events[0])
        for changed in (replace(request, source_sha256='b'*64), replace(request, cursor='another-event'),
                        replace(request, synthetic=False), replace(request, as_of=request.as_of+timedelta(seconds=31)),
                        replace(request, identity=StrategyIdentity('fixture', 'martin', 'wrong-version')),
                        replace(request, checkpoint={'pending_signals': {}})):
            with self.assertRaises(ValueError):
                self.plugin.decide(changed)
        with self.assertRaisesRegex(ValueError, 'INVALID_COST_BUFFER'):
            MartinSignalPlugin(calendar_view=ExchangeCalendarView(), cost_buffer_per_share='-1')

    def test_plugin_has_no_infra_io_or_account_policy_dependency(self):
        module = ast.parse(Path('strategy/martin_luk/src/trading_strategies/martin/signals.py').read_text())
        for node in ast.walk(module):
            names = ([node.module or ''] if isinstance(node, ast.ImportFrom)
                     else [item.name for item in node.names] if isinstance(node, ast.Import) else [])
            for name in names:
                self.assertFalse(name.startswith(('martin_agentic', 'trading_infra', 'sqlite3', 'subprocess', 'socket')))
        self.assertFalse(hasattr(self.plugin, 'book'))
        self.assertFalse(hasattr(self.plugin, 'policy'))
