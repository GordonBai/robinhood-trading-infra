"""Historical backfill is chunked so no event is enqueued beyond its window."""
from datetime import datetime, timedelta
from unittest import TestCase

from trading_infra.backfill import describe, plan_chunks

BASE = datetime(2026, 10, 15, 9, 30)


def day(*, windows=78, symbols=30):
    events = []
    for step in range(windows):
        begins = BASE + timedelta(minutes=5 * step)
        for index in range(symbols):
            events.append({'id': f'w{step}-s{index}', 'kind': 'bar',
                           'at': begins.isoformat(),
                           'data': {'symbol': f'S{index}',
                                    'begins_at': begins.isoformat(),
                                    'available_at': (begins + timedelta(minutes=5)).isoformat()}})
    return events


class PlanTests(TestCase):
    def test_a_single_window_batch_is_small_enough_to_keep_whole(self):
        plan = plan_chunks(day(windows=20))
        self.assertTrue(plan['fits_one_batch'])
        self.assertEqual(plan['chunk_count'], 20)
        self.assertTrue(all(len(chunk) == 30 for chunk in plan['chunks']))

    def test_a_full_day_is_split_rather_than_enqueued_at_once(self):
        events = day()
        self.assertEqual(len(events), 2340)
        plan = plan_chunks(events)
        self.assertFalse(plan['fits_one_batch'])
        self.assertGreater(plan['chunk_count'], 1)
        self.assertTrue(all(len(chunk) <= plan['ceiling_per_chunk'] for chunk in plan['chunks']))
        self.assertEqual(sum(len(chunk) for chunk in plan['chunks']), len(events))

    def test_a_chunk_never_splits_a_five_minute_window(self):
        plan = plan_chunks(day())
        for chunk in plan['chunks']:
            windows = {event['data']['available_at'] for event in chunk}
            self.assertEqual(len(windows), 1, 'a chunk mixed two arrival windows')

    def test_order_is_preserved_across_chunks(self):
        events = day(windows=10)
        plan = plan_chunks(events)
        flattened = [event['id'] for chunk in plan['chunks'] for event in chunk]
        self.assertEqual(flattened, [event['id'] for event in events])

    def test_a_declared_slower_machine_shrinks_the_chunk(self):
        # One large arrival window, so the time budget is what binds.
        events = day(windows=1, symbols=200)
        fast = plan_chunks(events, seconds_per_event=0.025)
        slow = plan_chunks(events, seconds_per_event=0.250)
        self.assertLess(slow['ceiling_per_chunk'], fast['ceiling_per_chunk'])
        self.assertGreater(slow['chunk_count'], fast['chunk_count'])
        self.assertEqual(sum(len(chunk) for chunk in slow['chunks']), 200)

    def test_the_event_ceiling_binds_when_the_time_budget_would_allow_more(self):
        plan = plan_chunks(day(windows=20), max_events=1)
        self.assertEqual(plan['ceiling_per_chunk'], 1)

    def test_invalid_input_is_refused(self):
        for value in ([], 'nope', None):
            with self.assertRaises(ValueError):
                plan_chunks(value)
        with self.assertRaises(ValueError):
            plan_chunks(day(windows=2), max_events=0)
        with self.assertRaises(ValueError):
            plan_chunks([{'id': 'x'}], max_seconds=0)
        with self.assertRaises(ValueError):
            plan_chunks([{'kind': 'bar'}])

    def test_describe_is_a_compact_durable_summary(self):
        summary = describe(plan_chunks(day(windows=4)))
        self.assertEqual(summary['events'], 120)
        self.assertEqual(summary['chunks'], 4)
        self.assertEqual(summary['chunk_sizes'], [30, 30, 30, 30])
