"""Execution feedback routed to the owning strategy instance.

Synthetic durable state only; nothing here places, cancels or observes an order.
"""
import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone
from unittest import TestCase

from trading_infra.attribution import ExecutionAttribution

AT = datetime(2026, 10, 29, 14, 0, tzinfo=timezone.utc)


class AttributionTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = self.temp.name + '/attribution.sqlite'
        self.db = sqlite3.connect(self.path, isolation_level=None)
        self.addCleanup(self.db.close)
        self.attribution = ExecutionAttribution(self.db, clock=lambda: AT)
        self.attribution.bind('ref-a', 'alpha', 'alpha-1')
        self.attribution.bind('ref-b', 'beta', 'beta-1')

    def event(self, event_id, ref_id, kind, *, offset=0, **extra):
        return {'event_id': event_id, 'ref_id': ref_id, 'kind': kind,
                'occurred_at': (AT + timedelta(seconds=offset)).isoformat(), **extra}

    def test_binding_is_idempotent_and_single_owner(self):
        self.assertEqual(self.attribution.bind('ref-a', 'alpha', 'alpha-1')['instance_id'],
                         'alpha')
        with self.assertRaises(ValueError) as ctx:
            self.attribution.bind('ref-a', 'beta', 'beta-1')
        self.assertEqual(str(ctx.exception), 'ATTRIBUTION_REF_ALREADY_BOUND')
        self.assertEqual(self.attribution.owner('ref-a')['instance_id'], 'alpha')

    def test_feedback_reaches_the_owning_instance_only(self):
        self.attribution.record(self.event('e1', 'ref-a', 'filled', price='40.02'))
        self.assertEqual(len(self.attribution.events('alpha')), 1)
        self.assertEqual(self.attribution.events('beta'), [])

    def test_unbound_reference_is_refused_rather_than_guessed(self):
        with self.assertRaises(ValueError) as ctx:
            self.attribution.record(self.event('e1', 'ref-unknown', 'filled'))
        self.assertEqual(str(ctx.exception), 'ATTRIBUTION_REF_UNKNOWN')
        self.assertEqual(self.attribution.events('alpha'), [])

    def test_unknown_kind_is_refused(self):
        with self.assertRaises(ValueError) as ctx:
            self.attribution.record(self.event('e1', 'ref-a', 'teleported'))
        self.assertEqual(str(ctx.exception), 'ATTRIBUTION_EVENT_KIND_INVALID')

    def test_redelivery_is_idempotent_and_conflicting_redelivery_is_refused(self):
        event = self.event('e1', 'ref-a', 'partial_fill', quantity='1')
        self.assertFalse(self.attribution.record(event)['duplicate'])
        self.assertTrue(self.attribution.record(event)['duplicate'])
        with self.assertRaises(ValueError) as ctx:
            self.attribution.record(self.event('e1', 'ref-a', 'partial_fill', quantity='2'))
        self.assertEqual(str(ctx.exception), 'ATTRIBUTION_EVENT_CONFLICT')
        self.assertEqual(len(self.attribution.events('alpha')), 1)

    def test_out_of_order_arrival_keeps_the_latest_occurrence(self):
        self.attribution.record(self.event('later', 'ref-a', 'order_accepted', offset=10))
        self.attribution.record(self.event('earlier', 'ref-a', 'order_accepted', offset=0))
        latest = self.attribution.latest('alpha', 'order_accepted')
        self.assertEqual(latest['event_id'], 'later')

    def test_latest_returns_none_for_a_kind_that_never_arrived(self):
        self.attribution.record(self.event('e1', 'ref-a', 'order_accepted'))
        self.assertIsNone(self.attribution.latest('alpha', 'flat'))

    def test_unknown_is_recorded_as_unknown_not_as_failure(self):
        self.attribution.record(self.event('e1', 'ref-a', 'unknown', reason='ambiguous write'))
        self.assertEqual(self.attribution.latest('alpha', 'unknown')['payload']['reason'],
                         'ambiguous write')
        self.assertEqual(self.attribution.events('alpha')[0]['kind'], 'unknown')

    def test_open_trades_close_only_on_a_flat_event(self):
        self.attribution.record(self.event('e1', 'ref-a', 'filled'))
        self.attribution.record(self.event('e2', 'ref-b', 'filled'))
        self.assertEqual(self.attribution.open_trades('alpha'), ['alpha-1'])
        self.attribution.record(self.event('e3', 'ref-a', 'flat', offset=30))
        self.assertEqual(self.attribution.open_trades('alpha'), [])
        self.assertEqual(self.attribution.open_trades('beta'), ['beta-1'])

    def test_events_can_be_filtered_by_trade(self):
        self.attribution.bind('ref-a2', 'alpha', 'alpha-2')
        self.attribution.record(self.event('e1', 'ref-a', 'filled'))
        self.attribution.record(self.event('e2', 'ref-a2', 'filled'))
        self.assertEqual(len(self.attribution.events('alpha')), 2)
        self.assertEqual([item['event_id']
                          for item in self.attribution.events('alpha', trade_id='alpha-2')],
                         ['e2'])

    def test_feedback_survives_a_new_connection(self):
        self.attribution.record(self.event('e1', 'ref-a', 'filled'))
        reopened = sqlite3.connect(self.path, isolation_level=None)
        self.addCleanup(reopened.close)
        again = ExecutionAttribution(reopened, clock=lambda: AT)
        self.assertEqual(len(again.events('alpha')), 1)
        self.assertEqual(again.owner('ref-a')['trade_id'], 'alpha-1')

    def test_malformed_events_are_refused(self):
        for bad in ({}, {'event_id': '', 'ref_id': 'ref-a', 'kind': 'filled',
                         'occurred_at': AT.isoformat()},
                    {'event_id': 'e1', 'ref_id': 'ref-a', 'kind': 'filled',
                     'occurred_at': 'not-a-time'}):
            with self.assertRaises(ValueError):
                self.attribution.record(bad)
