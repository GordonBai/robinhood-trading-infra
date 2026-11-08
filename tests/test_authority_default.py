"""Default saved-bundle observation plus real temp lineage/control receipt path."""
import json
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from martin_agentic.assessment_receipt import read_book_authority
from martin_agentic.broker import BrokerJournal
from martin_agentic.current_context import CurrentContextFactory, CurrentContextRegistry
from martin_agentic.execution import ExecutionBook
from martin_agentic.live_capture_bridge import (
    SavedBundleObservationBridge, journal_association_projection)
import test_live_capture_bridge as bridge_module
import test_session_lineage as lineage_module
import os


class DefaultAuthorityCombinationTests(TestCase):
    def test_saved_bundle_owner_risk_publication_factory_receipt(self):
        lineage = lineage_module.SessionLineageTests()
        lineage.setUp()
        self.addCleanup(lineage.doCleanups)
        bridge = bridge_module.BridgeCase()
        bridge.setUp()
        self.addCleanup(bridge.doCleanups)
        snapshot = bridge.read()
        # The lineage fixture owns the temporary canonical runtime/risk path;
        # the bridge owns only the saved bundle/public capture roots.
        lineage.next_policy['current_capture'] = bridge.policy['current_capture']
        next_book = lineage.next_book()
        journal = BrokerJournal(next_book.db_path)
        try:
            journal.ingest(snapshot)
        finally:
            journal.close()
        next_book.reconcile(snapshot)
        lineage.bootstrap()
        lineage.enroll(next_book)

        # Publish the same saved observation and build the default factory
        # context from that registry, not a mocked gate result.
        journal = BrokerJournal(next_book.db_path)
        try:
            projection = journal_association_projection(
                journal.current_observation())
        finally:
            journal.close()
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   bridge.public.policy):
            bridge2 = SavedBundleObservationBridge(
                next_book.policy, lambda: bridge.now,
                config=bridge.bridge().config)
            published = bridge2.publish(
                snapshot, book=next_book, journal_snapshot=projection)
        self.assertIn(published['status'],
                      {'PUBLISHED', 'ALREADY_PUBLISHED'}, published)
        registry = CurrentContextRegistry(
            bridge.root / 'captures' / 'registry.json',
            allowed_capture_roots=(bridge.root / 'captures',),
            allowed_public_roots=(bridge.public.root,),
            clock=lambda: bridge.now)
        factory = CurrentContextFactory(registry, clock=lambda: bridge.now)
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   bridge.public.policy):
            context = factory(next_book)
        self.assertEqual(type(context).__name__, 'CurrentEngineeringPreflight')
        next_book._current_preflight = context

        policy = next_book.policy
        observed = snapshot['observed_at']
        plan = {'symbol': 'DEMO', 'quantity': 2, 'entry_ceiling': '40',
                'stop': '39', 'cost_buffer_per_share': '0.05'}
        quote = {'symbol': 'DEMO', 'bid': '39.99', 'ask': '40',
                 'bid_at': observed, 'ask_at': observed,
                 'observed_at': observed}
        evidence = {
            'synthetic': False, 'source_sha256': 'fixture',
            'engineering_scope_authorized': True,
            'parameters_user_specified': True,
            'parameters_valid_until': observed,
            'parameters_authorized_at': observed,
        }
        trade = {'id': 'authority-default-trade', 'purpose': 'engineering',
                 'plan': plan, 'phase': 'ENTRY_PLANNED', 'stop': plan['stop'],
                 'initial_stop': plan['stop'], 'quantity': '0',
                 'entry_price': None, 'snapshot_at': observed,
                 'exit_requested': False, 'cancel_for_replace': None,
                 'evidence': evidence, 'preflight_quote': quote}
        state = next_book.state()
        state['trades'][trade['id']] = trade
        next_book.db.execute('UPDATE live_state SET payload=? WHERE id=1',
                             (json.dumps(state, sort_keys=True, separators=(',', ':')),))
        next_book.db.commit()
        next_book._queue(
            trade, 'entry',
            {'symbol': 'DEMO', 'side': 'buy', 'type': 'limit',
             'quantity': '2', 'limit_price': '40',
             'market_hours': 'regular_hours', 'time_in_force': 'gfd'},
            bridge.now, 'AUTHORITY_DEFAULT_FIXTURE')
        next_book.db.commit()
        control_patch = patch(
            'martin_agentic.live_verification_control.CANONICAL_RUNTIME',
            lineage.runtime)
        control_patch.start()
        self.addCleanup(control_patch.stop)
        control = {
            'schema_version': 1, 'live_trading_enabled': False,
            'live_engineering_verification_enabled': False,
            'reason': 'TEMP_AUTHORITY_FIXTURE_USER_HOLD',
            'recorded_at': '2026-10-19T13:00:00+00:00',
            'grants_not_before': '2026-10-19T13:00:00+00:00'}
        control_path = lineage.runtime / 'live-verification-control.json'
        control_path.write_text(json.dumps(control, sort_keys=True))
        os.chmod(control_path, 0o600)

        action = next((a for a in next_book.actions()
                       if a['kind'] == 'entry'), None)
        self.assertIsNotNone(action)
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   bridge.public.policy):
            decision = next_book._current_preflight_decision(action=action)
        self.assertFalse(decision['allowed'])
        components = decision['components']
        self.assertEqual(components['risk']['status'], 'evaluated')
        self.assertEqual(components['lineage']['status'], 'evaluated')
        self.assertFalse(components['lineage']['blocked'])
        self.assertEqual(components['control']['status'], 'evaluated')
        self.assertTrue(components['control']['blocked'])
        self.assertEqual(components['formal_channel']['status'], 'blocked')
        binding = next_book.current_preflight_binding(decision, action=action)
        self.assertTrue(binding['bound'], binding['reasons'])

        # Owner trade context is reread at the final binding boundary.
        state = next_book.state()
        state['trades'][trade['id']]['purpose'] = 'strategy'
        next_book.db.execute('UPDATE live_state SET payload=? WHERE id=1',
                             (json.dumps(state, sort_keys=True, separators=(',', ':')),))
        next_book.db.commit()
        changed = next_book.current_preflight_binding(decision, action=action)
        self.assertFalse(changed['bound'])
        self.assertIn('RECEIPT_AUTHORIZATION_PURPOSE_CHANGED',
                      changed['reasons'])
