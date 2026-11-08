import copy
import json
from pathlib import Path
import unittest

from martin_luk.strategy import PullbackMachine, replay_setup


ROOT = Path(__file__).resolve().parents[1]
SCENARIO = json.loads((ROOT / "examples/pullback_setup.json").read_text())


class StrategyTests(unittest.TestCase):
    def setUp(self):
        self.scenario = copy.deepcopy(SCENARIO)
        self.machine = PullbackMachine(self.scenario["setup"])

    def arm(self):
        for event in self.scenario["events"][:2]:
            self.machine.on_bar(event["data"], as_of=event["at"])

    def test_flush_reclaim_then_quote_produces_one_trigger(self):
        result = replay_setup(self.scenario)
        self.assertEqual([s["state"] for s in result["transitions"]],
                         ["WAIT_FLUSH", "WAIT_RECLAIM", "ARMED", "TRIGGERED"])
        self.assertEqual(result["current"]["stop"], "39.20")
        self.assertEqual(result["current"]["trigger"], "40.00")
        self.assertFalse(result["execution_ready"])

    def test_no_same_bar_high_fill(self):
        event = self.scenario["events"][0]
        bar = {**event["data"], "high": "45", "close": "44"}
        result = self.machine.on_bar(bar, as_of=event["at"])
        self.assertEqual(result["state"], "WAIT_RECLAIM")

    def test_quote_before_reclaim_never_triggers(self):
        event = self.scenario["events"][-1]
        self.assertEqual(self.machine.on_quote(event["data"], as_of=event["at"])["state"], "WAIT_FLUSH")

    def test_duplicate_reclaim_does_not_reset_expiry(self):
        self.arm()
        event = self.scenario["events"][1]
        before = self.machine.snapshot()
        self.assertEqual(self.machine.on_bar(event["data"], as_of=event["at"]), before)

    def test_revised_bar_rejected(self):
        self.arm()
        event = self.scenario["events"][1]
        with self.assertRaises(ValueError):
            self.machine.on_bar({**event["data"], "close": "39.85"}, as_of=event["at"])

    def test_future_or_unfinished_bar_rejected(self):
        event = self.scenario["events"][0]
        with self.assertRaises(ValueError):
            self.machine.on_bar(event["data"], as_of="2026-10-15T09:34:00-04:00")

    def test_understated_bar_completion_rejected(self):
        event = self.scenario["events"][0]
        data = {**event["data"], "available_at": "2026-10-15T09:30:01-04:00"}
        with self.assertRaises(ValueError):
            self.machine.on_bar(data, as_of=event["at"])

    def test_invalid_event_does_not_poison_clock_or_state(self):
        event = self.scenario["events"][0]
        before = self.machine.snapshot()
        with self.assertRaises(ValueError):
            self.machine.on_bar({**event["data"], "symbol": "OTHER"},
                                as_of="2026-10-15T12:00:00-04:00")
        self.assertEqual(self.machine.snapshot(), before)
        self.assertEqual(self.machine.on_bar(event["data"], as_of=event["at"])["state"], "WAIT_RECLAIM")

    def test_future_support_features_rejected(self):
        self.scenario["setup"]["features"]["known_at"] = "2026-10-15T10:00:00-04:00"
        with self.assertRaises(ValueError):
            PullbackMachine(self.scenario["setup"])

    def test_correlated_emas_do_not_count_as_three_support_categories(self):
        self.scenario["setup"]["support_categories"] = ["ema", "ema", "ema"]
        with self.assertRaises(ValueError):
            PullbackMachine(self.scenario["setup"])

    def test_bad_screen_remains_rejected(self):
        self.scenario["setup"]["features"]["rs_percentile"] = "0.80"
        self.assertEqual(replay_setup(self.scenario)["current"]["state"], "REJECTED")

    def test_reclaim_cannot_expand_structural_stop(self):
        first, second = self.scenario["events"][:2]
        self.machine.on_bar(first["data"], as_of=first["at"])
        result = self.machine.on_bar({**second["data"], "low": "39.00"}, as_of=second["at"])
        self.assertEqual(result["state"], "INVALIDATED")
        self.assertEqual(result["stop"], "39.20")

    def test_bar_gap_invalidates_setup(self):
        self.arm()
        bar = {**self.scenario["events"][1]["data"], "begins_at": "2026-10-15T09:45:00-04:00",
               "available_at": "2026-10-15T09:50:02-04:00", "observed_at": "2026-10-15T09:50:02-04:00"}
        self.assertEqual(self.machine.on_bar(bar, as_of=bar["available_at"])["reason"], "BAR_GAP_REQUIRES_NEW_SETUP")

    def test_stale_quote_is_rejected(self):
        self.arm()
        event = self.scenario["events"][-1]
        with self.assertRaises(ValueError):
            self.machine.on_quote(event["data"], as_of="2026-10-15T09:41:00-04:00")

    def test_bid_must_confirm_breakout_not_only_ask(self):
        self.arm()
        event = self.scenario["events"][-1]
        quote = {**event["data"], "bid": "39.99", "ask": "40.01"}
        self.assertEqual(self.machine.on_quote(quote, as_of=event["at"])["state"], "ARMED")

    def test_chase_limit_blocks_late_entry(self):
        self.arm()
        event = self.scenario["events"][-1]
        quote = {**event["data"], "bid": "40.09", "ask": "40.10"}
        self.assertEqual(self.machine.on_quote(quote, as_of=event["at"])["reason"], "CHASE_LIMIT_EXCEEDED")

    def test_expiry_before_trigger(self):
        self.arm()
        event = self.scenario["events"][-1]
        self.assertEqual(self.machine.on_quote(event["data"], as_of="2026-10-15T09:55:02-04:00")["state"], "EXPIRED")


if __name__ == "__main__":
    unittest.main()
