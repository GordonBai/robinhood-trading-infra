"""The per-bar observation index is host-owned but must keep state() unchanged.

The collector de-duplication index used to travel inside the strategy
checkpoint. It now lives in the host while the persisted state keeps exactly the
same content, so the frozen legacy equivalence test still holds. These tests pin
the two facts that make that safe: what crosses the boundary, and what is
persisted.
"""
import tempfile
from datetime import datetime
from unittest import TestCase

import test_signal_host_isolation as fixture
from shadow_fixture import events


class BarObservationIndexTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.harness = fixture.SignalHostIsolationTests()
        self.old, self.new = self.harness.books(self.temp.name)
        self.addCleanup(self.harness.doCleanups)
        self.seen = []
        original = self.new.plugin.decide

        def recording(request):
            self.seen.append(request)
            return original(request)

        self.new.plugin.decide = recording

    def consume_all(self):
        for raw in events()[1:]:
            event = self.harness.event(raw)
            self.harness.now = datetime.fromisoformat(event['at'])
            expected = self.old.consume(event)
            actual = self.new.consume(event)
            self.assertEqual(actual, expected)
            if actual:
                self.old.complete(actual['id'], result={'accepted': True})
                self.new.complete(actual['id'], result={'accepted': True})

    def test_persisted_state_still_matches_the_frozen_legacy_trace(self):
        self.consume_all()
        self.assertEqual(self.new.state(), self.old.state())

    def test_the_boundary_checkpoint_never_carries_the_index(self):
        self.consume_all()
        self.assertTrue(self.seen)
        for request in self.seen:
            self.assertNotIn('bar_observations', request.checkpoint)

    def test_the_persisted_state_does_carry_the_index(self):
        self.consume_all()
        self.assertIn('bar_observations', self.new.state())
        self.assertEqual(len(self.new.state()['bar_observations']), 2)

    def test_the_index_and_the_persisted_map_never_diverge(self):
        self.consume_all()
        rows = dict(self.new.book.db.execute(
            'SELECT bar_key, fingerprint FROM bar_observation_index'))
        self.assertEqual(rows, self.new.state()['bar_observations'])


class CollectorIndexTests(TestCase):
    """The collector must keep de-duplicating after the index moved."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.harness = fixture.SignalHostIsolationTests()
        self.old, self.new = self.harness.books(self.temp.name)
        self.addCleanup(self.harness.doCleanups)

    def test_unchanged_bars_are_still_omitted_after_a_commit(self):
        from martin_luk.live_signals import filter_unchanged_bars

        bar = next(raw for raw in events()[1:] if raw['kind'] == 'bar')
        event = self.harness.event(bar)
        self.harness.now = datetime.fromisoformat(event['at'])
        self.new.consume(event)
        changed, omitted = filter_unchanged_bars([event['data']], self.new.state())
        self.assertEqual((changed, omitted), ([], 1))

    def test_a_changed_bar_is_never_silently_omitted(self):
        from martin_luk.live_signals import filter_unchanged_bars

        bar = next(raw for raw in events()[1:] if raw['kind'] == 'bar')
        event = self.harness.event(bar)
        self.harness.now = datetime.fromisoformat(event['at'])
        self.new.consume(event)
        revised = dict(event['data'])
        revised['close'] = '41.00'
        changed, omitted = filter_unchanged_bars([revised], self.new.state())
        self.assertEqual((len(changed), omitted), (1, 0))
