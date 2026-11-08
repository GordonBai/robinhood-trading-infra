"""Protection, cancel and exit identities belong to their instance.

Entry references were bound at submission; management actions are created later
by the book itself, so their references were never attributable. These tests
bind them by identity only and derive lifecycle feedback from the book's own
facts.

Synthetic fixtures and temporary databases; no broker call.
"""
import json
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from decimal import Decimal
from unittest import TestCase

from entry_authority_fixture import TestExecutionBook as ExecutionBook
from martin_agentic.management_binding import (bind_management_actions,
                                               record_lifecycle_feedback)
from trading_infra.attribution import ExecutionAttribution
from trading_infra.instance_accounting import InstanceAccounting
from trading_infra.receipts import ExecutionReceiptImporter

AT = datetime.fromisoformat('2026-10-16T14:00:00+00:00')
ACCOUNT = 'acct-0000'


def provider_order(*, ref, order_id='broker-1', state='confirmed', remaining='2',
                   symbol='DEMO'):
    return {'id': order_id, 'ref_id': ref, 'symbol': symbol, 'state': state,
            'remaining': remaining, 'quantity': '2', 'cumulative_quantity': '0',
            'executions': []}


class BindingTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.policy = json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        self.book = ExecutionBook(Path(self.temp.name) / 'bind.sqlite', self.policy,
                                  simulation=True, clock=lambda: AT)
        self.addCleanup(self.book.close)
        self.attribution = ExecutionAttribution(self.book.db, clock=lambda: AT)
        self.accounting = InstanceAccounting(self.book.db, clock=lambda: AT)
        self.importer = ExecutionReceiptImporter(
            self.book.db, attribution=self.attribution, accounting=self.accounting,
            clock=lambda: AT)
        self.attribution.bind('ref-entry', 'alpha', 'trade-1')

    def snapshot(self, orders=()):
        return {'account_key': ACCOUNT, 'observed_at': AT.isoformat(),
                'collection_complete': True,
                'orders': {item['id']: item for item in orders}}

    def seed_action(self, kind='protect', *, ref='ref-protect'):
        payload = {'action_id': 'act-' + kind, 'ref_id': ref, 'trade_id': 'trade-1',
                   'account': '••••0000', 'created_at': AT.isoformat(),
                   'expires_at': AT.isoformat(),
                   'parameters': {'symbol': 'DEMO', 'side': 'sell', 'type': 'market',
                                  'quantity': '2', 'market_hours': 'regular_hours',
                                  'time_in_force': 'gfd'},
                   'plan': {'symbol': 'DEMO'}, 'reason': 'TEST'}
        self.book.db.execute(
            "INSERT INTO live_actions(id,trade_id,kind,state,payload,created_at,"
            " updated_at,report) VALUES(?,?,?,?,?,?,?,?)",
            ('act-' + kind, 'trade-1', kind, 'RUNNING', json.dumps(payload),
             AT.isoformat(), AT.isoformat(), None))
        self.book.db.commit()
        return 'act-' + kind

    def test_a_protection_reference_is_bound_to_the_trade_owner(self):
        self.seed_action('protect', ref='ref-protect')
        result = bind_management_actions(self.book, attribution=self.attribution,
                                         importer=self.importer, account_key=ACCOUNT)
        self.assertEqual([item['ref_id'] for item in result['bound']], ['ref-protect'])
        self.assertEqual(self.attribution.owner('ref-protect')['instance_id'], 'alpha')
        self.assertEqual(self.attribution.owner('ref-protect')['trade_id'], 'trade-1')

    def test_an_exact_order_id_becomes_a_routable_alias(self):
        action = self.seed_action('exit', ref='ref-exit')
        self.book.db.execute(
            "INSERT INTO external_broker_refs(ref_id,action_id,operation_id)"
            " VALUES('rewritten-exit-ref',?,'op-1')", (action,))
        self.book.db.execute(
            "INSERT INTO external_broker_order_ids(order_id,action_id,operation_id,"
            " account_key) VALUES('broker-exit-9',?,'op-1',?)", (action, ACCOUNT))
        self.book.db.commit()
        result = bind_management_actions(self.book, attribution=self.attribution,
                                         importer=self.importer, account_key=ACCOUNT)
        self.assertIn('rewritten-exit-ref',
                      [item['ref_id'] for item in result['bound']])
        self.assertIn('broker-exit-9', [item['order_id'] for item in result['aliases']])
        self.assertEqual(self.attribution.owner('rewritten-exit-ref')['instance_id'],
                         'alpha')

    def test_an_action_with_no_owning_instance_is_left_alone(self):
        self.attribution.bind('ref-legacy', 'alpha', 'legacy-trade')
        self.seed_action('protect', ref='ref-legacy')
        # Re-point the action at a trade no instance owns.
        self.book.db.execute("UPDATE live_actions SET trade_id='orphan' WHERE kind='protect'")
        self.book.db.commit()
        result = bind_management_actions(self.book, attribution=self.attribution,
                                         importer=self.importer, account_key=ACCOUNT)
        self.assertEqual(result['bound'], [])
        self.assertEqual(result['unowned_actions'], ['act-protect'])

    def test_binding_is_idempotent_across_repeats(self):
        self.seed_action('protect', ref='ref-protect')
        first = bind_management_actions(self.book, attribution=self.attribution,
                                        importer=self.importer, account_key=ACCOUNT)
        second = bind_management_actions(self.book, attribution=self.attribution,
                                         importer=self.importer, account_key=ACCOUNT)
        self.assertEqual(len(first['bound']), 1)
        self.assertEqual(len(second['bound']), 1, 'same ref, same owner, no conflict')
        self.assertEqual(second['failures'], [])


class LifecycleTests(BindingTests):
    def state_with(self, *, phase, orders=(), quantity='2', trades=None):
        self.book._begin()
        try:
            state = self.book.state()
            state['account_key'] = ACCOUNT
            state['last_snapshot'] = AT.isoformat()
            state['broker_orders'] = {item['id']: item for item in orders}
            state['trades'] = trades if trades is not None else {
                'trade-1': {'id': 'trade-1', 'phase': phase, 'quantity': quantity,
                            'plan': {'symbol': 'DEMO'}}}
            self.book._save(state)
            self.book.db.commit()
        finally:
            pass

    def test_a_confirmed_stop_covers_the_quantity(self):
        self.seed_action('protect', ref='ref-protect')
        self.state_with(phase='PROTECTED',
                        orders=[provider_order(ref='ref-protect', remaining='2')])
        record_lifecycle_feedback(self.book, attribution=self.attribution,
                                  account_key=ACCOUNT, observed_at=AT.isoformat())
        kinds = [item['kind'] for item in self.attribution.events('alpha')]
        self.assertIn('protection_covered', kinds)

    def test_a_partial_stop_does_not_claim_full_coverage(self):
        self.seed_action('protect', ref='ref-protect')
        self.state_with(phase='PROTECTED',
                        orders=[provider_order(ref='ref-protect', remaining='1')])
        record_lifecycle_feedback(self.book, attribution=self.attribution,
                                  account_key=ACCOUNT, observed_at=AT.isoformat())
        kinds = [item['kind'] for item in self.attribution.events('alpha')]
        self.assertNotIn('protection_covered', kinds,
                         'one covered share is not protection of the whole position')

    def test_an_accepted_cancel_request_is_not_a_terminal_state(self):
        self.seed_action('protect', ref='ref-protect')
        self.state_with(phase='PROTECTED',
                        orders=[provider_order(ref='ref-protect',
                                               state='pending_cancel',
                                               remaining='2')])
        record_lifecycle_feedback(self.book, attribution=self.attribution,
                                  account_key=ACCOUNT, observed_at=AT.isoformat())
        kinds = [item['kind'] for item in self.attribution.events('alpha')]
        self.assertNotIn('cancel_terminal', kinds)
        self.assertNotIn('protection_covered', kinds)

    def test_flat_is_recorded_once_and_replays_identically(self):
        self.state_with(phase='FLAT_VERIFIED', orders=[])
        for _ in range(3):
            record_lifecycle_feedback(self.book, attribution=self.attribution,
                                      account_key=ACCOUNT, observed_at=AT.isoformat())
        flats = [item for item in self.attribution.events('alpha')
                 if item['kind'] == 'flat']
        self.assertEqual(len(flats), 1)
