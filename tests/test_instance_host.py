"""Per-instance prepare/commit isolation over temporary synthetic state.

Two deterministic fixture plugins share one durable registry and prove that
state, identity and consumption stay separated. No broker, no order, no money.
"""
import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest import TestCase

from trading_contracts.instance import InstanceBudget, PluginManifest, StrategyInstance
from trading_contracts.strategy import (ProposalAction, StrategyDecision, StrategyProposal)
from trading_contracts.values import digest
from trading_infra.instance_host import InstanceStrategyHost, PreparedInstanceInput
from trading_infra.instance_registry import InstanceRegistry


class CountingPlugin:
    """Deterministic advisory fixture: one counter per symbol in the checkpoint."""

    def __init__(self, *, step=1, proposal_at=None, quantity_limit=1):
        self.step = step
        self.proposal_at = proposal_at
        self.quantity_limit = quantity_limit

    def decide(self, request):
        event = request.market['event']
        checkpoint = dict(request.checkpoint)
        seen = dict(checkpoint.get('seen') or {})
        symbol = event.get('data', {}).get('symbol', 'NONE')
        seen[symbol] = seen.get(symbol, 0) + self.step
        proposals = ()
        if self.proposal_at is not None and seen[symbol] >= self.proposal_at:
            proposals = (StrategyProposal(
                identity=request.identity,
                proposal_id='proposal-' + request.cursor,
                decision_id='decision-' + request.cursor,
                setup_id='setup-' + request.cursor,
                data_snapshot_id=request.data_snapshot_id,
                source_sha256=request.source_sha256,
                generated_at=request.as_of,
                valid_until=request.as_of + timedelta(seconds=20),
                action=ProposalAction.ENTER, symbol=symbol, reason='FIXTURE_ENTRY',
                synthetic=request.synthetic, quantity_limit=self.quantity_limit,
                entry_ceiling=Decimal('40.00'), stop_price=Decimal('39.60')),)
        return StrategyDecision(
            identity=request.identity,
            decision_id='decision-' + request.cursor,
            as_of=request.as_of, data_snapshot_id=request.data_snapshot_id,
            consumed_cursor=request.cursor, synthetic=request.synthetic,
            proposals=proposals, next_checkpoint={'seen': seen},
            diagnostics={'fixture': True})


def manifest(plugin_id='counter', version='1.0'):
    return PluginManifest(plugin_id=plugin_id, version=version, state_schema='counter-1',
                          config={'step': '1'},
                          data_requirements=('quotes',), capabilities=('advisory-entry',),
                          max_inflight=64)


def budget():
    return InstanceBudget(max_cash=Decimal('100'), max_planned_loss=Decimal('2.50'),
                          max_notional=Decimal('100'), max_concurrent=1)


class InstanceHostTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = self.temp.name + '/state.sqlite'
        self.db = sqlite3.connect(self.path, isolation_level=None)
        self.addCleanup(self.db.close)
        self.now = datetime(2026, 10, 29, 14, 0, tzinfo=timezone.utc)
        self.registry = InstanceRegistry(self.db, clock=lambda: self.now)
        self.registry.register('alpha', manifest(), budget(), mode='shadow', enabled=True)
        self.registry.register('beta', manifest('counter-b', '2.0'), budget(),
                               mode='shadow', enabled=True)
        self.alpha = InstanceStrategyHost(self.registry, self.registry.instance('alpha'),
                                          clock=lambda: self.now)
        self.beta = InstanceStrategyHost(self.registry, self.registry.instance('beta'),
                                         clock=lambda: self.now)

    def event(self, event_id='evt-1', symbol='SPY', *, at=None, synthetic=True, source=None):
        stamp = (at or self.now).isoformat()
        return {'id': event_id, 'kind': 'quote', 'at': stamp, 'synthetic_input': synthetic,
                'source_sha256': source or ('source-' + event_id),
                'data': {'symbol': symbol, 'bid': '40.00', 'ask': '40.01'}}

    # -- happy path ------------------------------------------------------
    def test_commit_advances_the_instance_checkpoint(self):
        prepared = self.alpha.prepare(self.event())
        outcome = self.alpha.commit(prepared, CountingPlugin().decide(prepared.request))
        self.assertFalse(outcome['duplicate'])
        self.assertEqual(self.alpha.state(), {'seen': {'SPY': 1}})

    def test_prepare_returns_none_for_an_already_consumed_event(self):
        prepared = self.alpha.prepare(self.event())
        self.alpha.commit(prepared, CountingPlugin().decide(prepared.request))
        self.assertIsNone(self.alpha.prepare(self.event()))

    def test_duplicate_commit_is_reported_not_reapplied(self):
        prepared = self.alpha.prepare(self.event())
        decision = CountingPlugin().decide(prepared.request)
        self.alpha.commit(prepared, decision)
        again = self.alpha.commit(prepared, decision)
        self.assertTrue(again['duplicate'])
        self.assertEqual(self.alpha.state(), {'seen': {'SPY': 1}})

    def test_proposals_are_returned_but_grant_nothing(self):
        prepared = self.alpha.prepare(self.event())
        outcome = self.alpha.commit(prepared, CountingPlugin(proposal_at=1).decide(prepared.request))
        self.assertEqual(len(outcome['proposals']), 1)
        # The acknowledgement carries a JSON-safe summary, not the object.
        self.assertEqual(outcome['proposals'][0]['action'], 'ENTER')
        # The advice itself is durable, so a restart cannot drop it.
        pending = self.alpha.pending_proposals()
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]['action'], 'ENTER')
        self.assertEqual(pending[0]['proposal_id'], 'proposal-evt-1')

    # -- input integrity -------------------------------------------------
    def test_stale_event_is_rejected(self):
        old = self.now - timedelta(seconds=31)
        with self.assertRaises(ValueError) as ctx:
            self.alpha.prepare(self.event(at=old))
        self.assertEqual(str(ctx.exception), 'STALE_STRATEGY_EVENT')

    def test_future_event_is_rejected(self):
        with self.assertRaises(ValueError) as ctx:
            self.alpha.prepare(self.event(at=self.now + timedelta(seconds=1)))
        self.assertEqual(str(ctx.exception), 'STALE_STRATEGY_EVENT')

    def test_provenance_must_match_the_instance_mode(self):
        with self.assertRaises(ValueError) as ctx:
            self.alpha.prepare(self.event(synthetic=False))
        self.assertEqual(str(ctx.exception), 'STRATEGY_EVENT_PROVENANCE_REQUIRED')

    def test_mode_switch_changes_the_required_provenance_immediately(self):
        self.registry.set_mode('alpha', 'read_only_shadow', enabled=True)
        with self.assertRaisesRegex(ValueError,
                                    'STRATEGY_EVENT_PROVENANCE_REQUIRED'):
            self.alpha.prepare(self.event('shadow-event'))
        prepared = self.alpha.prepare(self.event('real-event', synthetic=False))
        self.assertIs(prepared.request.synthetic, False)

    def test_same_event_id_with_a_different_body_conflicts(self):
        prepared = self.alpha.prepare(self.event('evt-1'))
        self.alpha.commit(prepared, CountingPlugin().decide(prepared.request))
        with self.assertRaises(ValueError) as ctx:
            self.alpha.prepare(self.event('evt-1', symbol='QQQ'))
        self.assertEqual(str(ctx.exception), 'STRATEGY_EVENT_ID_CONFLICT')

    # -- isolation -------------------------------------------------------
    def test_two_instances_consume_the_same_event_independently(self):
        event = self.event()
        alpha = self.alpha.prepare(event)
        beta = self.beta.prepare(event)
        self.assertIsNotNone(alpha)
        self.assertIsNotNone(beta)
        self.assertEqual(alpha.request.identity.instance_id, 'alpha')
        self.assertEqual(beta.request.identity.instance_id, 'beta')
        self.alpha.commit(alpha, CountingPlugin(step=1).decide(alpha.request))
        self.beta.commit(beta, CountingPlugin(step=5).decide(beta.request))
        self.assertEqual(self.alpha.state(), {'seen': {'SPY': 1}})
        self.assertEqual(self.beta.state(), {'seen': {'SPY': 5}})

    def test_a_decision_from_another_instance_cannot_be_committed(self):
        event = self.event()
        alpha = self.alpha.prepare(event)
        beta = self.beta.prepare(event)
        foreign = CountingPlugin().decide(beta.request)
        with self.assertRaises(ValueError):
            self.alpha.commit(alpha, foreign)

    def test_stale_computation_cannot_overwrite_newer_state(self):
        first = self.alpha.prepare(self.event('evt-1'))
        second = self.alpha.prepare(self.event('evt-2', symbol='QQQ'))
        self.alpha.commit(first, CountingPlugin().decide(first.request))
        with self.assertRaises(ValueError) as ctx:
            self.alpha.commit(second, CountingPlugin().decide(second.request))
        self.assertEqual(str(ctx.exception), 'INSTANCE_STATE_CONFLICT')
        self.assertEqual(self.alpha.state(), {'seen': {'SPY': 1}})

    def test_contract_change_after_prepare_is_refused(self):
        prepared = self.alpha.prepare(self.event())
        self.registry.set_mode('alpha', 'disabled', enabled=False)
        with self.assertRaises(ValueError):
            self.alpha.commit(prepared, CountingPlugin().decide(prepared.request))

    def test_prepared_input_is_immutable(self):
        prepared = self.alpha.prepare(self.event())
        self.assertIsInstance(prepared, PreparedInstanceInput)
        with self.assertRaises(Exception):
            prepared.request.cursor = 'other'

    def test_state_survives_a_new_connection(self):
        prepared = self.alpha.prepare(self.event())
        self.alpha.commit(prepared, CountingPlugin().decide(prepared.request))
        reopened = sqlite3.connect(self.path, isolation_level=None)
        self.addCleanup(reopened.close)
        registry = InstanceRegistry(reopened, clock=lambda: self.now)
        host = InstanceStrategyHost(registry, registry.instance('alpha'),
                                    clock=lambda: self.now)
        self.assertEqual(host.state(), {'seen': {'SPY': 1}})
        self.assertEqual(host.consumed_hash('evt-1'), digest(self.event()))

    # -- oracle: the advice outbox --------------------------------------
    def test_a_proposal_is_consumable_exactly_once(self):
        prepared = self.alpha.prepare(self.event())
        self.alpha.commit(prepared, CountingPlugin(proposal_at=1).decide(prepared.request))
        self.assertEqual(self.alpha.claim_proposal('proposal-evt-1')['claim'], 'CONSUMED')
        self.assertEqual(self.alpha.claim_proposal('proposal-evt-1')['claim'], 'CONSUMED')
        self.assertEqual(self.alpha.pending_proposals(), [])

    def test_an_unclaimed_proposal_expires_on_its_own_validity(self):
        prepared = self.alpha.prepare(self.event())
        self.alpha.commit(prepared, CountingPlugin(proposal_at=1).decide(prepared.request))
        self.assertEqual(len(self.alpha.pending_proposals()), 1)
        later = self.now + timedelta(seconds=60)
        self.assertEqual(self.alpha.expire_proposals(now=later), 1)
        self.assertEqual(self.alpha.pending_proposals(now=later), [])
        self.assertEqual(self.alpha.claim_proposal('proposal-evt-1')['claim'], 'EXPIRED')

    def test_no_proposal_means_an_empty_outbox(self):
        prepared = self.alpha.prepare(self.event())
        self.alpha.commit(prepared, CountingPlugin().decide(prepared.request))
        self.assertEqual(self.alpha.pending_proposals(), [])

    def test_an_expired_proposal_is_never_committed(self):
        prepared = self.alpha.prepare(self.event())
        decision = CountingPlugin(proposal_at=1).decide(prepared.request)
        self.now = self.now + timedelta(seconds=30)
        with self.assertRaises(ValueError) as ctx:
            self.alpha.commit(prepared, decision)
        self.assertEqual(str(ctx.exception), 'STRATEGY_DECISION_EXPIRED')
        self.assertEqual(self.alpha.state(), {})
        self.assertEqual(self.alpha.pending_proposals(), [])

    def test_an_event_that_aged_out_while_waiting_is_refused(self):
        prepared = self.alpha.prepare(self.event())
        decision = CountingPlugin().decide(prepared.request)
        self.now = self.now + timedelta(seconds=31)
        with self.assertRaises(ValueError) as ctx:
            self.alpha.commit(prepared, decision)
        self.assertEqual(str(ctx.exception), 'STALE_STRATEGY_EVENT')
        self.assertEqual(self.alpha.state(), {})

    # -- guard rails -----------------------------------------------------
    def test_unregistered_instance_cannot_be_hosted(self):
        stray = StrategyInstance('stray', manifest(), budget(), 'shadow', True)
        with self.assertRaises(ValueError) as ctx:
            InstanceStrategyHost(self.registry, stray, clock=lambda: self.now)
        self.assertEqual(str(ctx.exception), 'INSTANCE_NOT_REGISTERED')
