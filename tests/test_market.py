import copy
from datetime import datetime
import json
from pathlib import Path
import unittest

from martin_agentic.market import RobinhoodReader, bar_available_at, inspect_recording


ROOT = Path(__file__).resolve().parents[1]
RECORDING = json.loads((ROOT / "tests/fixtures/synthetic_recording.json").read_text())
AS_OF = "2026-10-15T17:44:03+00:00"


class MarketTests(unittest.TestCase):
    def setUp(self):
        self.capture = copy.deepcopy(RECORDING)

    def inspect(self, **kwargs):
        return inspect_recording(self.capture, as_of=kwargs.pop("as_of", AS_OF),
                                 max_quote_age_seconds=kwargs.pop("max_quote_age_seconds", 60), **kwargs)

    def test_actual_recording_normalizes_all_three_data_kinds(self):
        result = self.inspect()
        self.assertTrue(result["data_usable"], result["errors"])
        self.assertEqual(len(result["quotes"]), 2)
        self.assertEqual(len(result["indicators"]), 18)
        self.assertEqual(len(result["bars"]), 28)
        self.assertFalse(result["execution_ready"])

    def test_strict_default_rejects_old_quote_even_if_api_responded(self):
        result = self.inspect(max_quote_age_seconds=15)
        self.assertFalse(result["data_usable"])
        self.assertIn("STALE_OR_FUTURE_QUOTE", result["errors"][0]["reason"])
        self.assertEqual(result["quotes"], [])

    def test_no_time_travel_to_before_actual_observation(self):
        result = self.inspect(as_of="2026-10-15T17:43:00Z")
        self.assertFalse(result["data_usable"])
        self.assertEqual(result["bars"], [])

    def test_daily_date_is_not_shifted_back_by_timezone(self):
        result = self.inspect()
        point = result["indicators"][-1]
        self.assertTrue(point["begins_at"].startswith("2026-10-14"))
        self.assertEqual(point["available_at"], "2026-10-15T00:00:00-04:00")

    def test_dst_completion_uses_new_york_clock(self):
        self.assertEqual(bar_available_at("2026-11-02T00:00:00Z", "day").isoformat(),
                         "2026-11-03T00:00:00-05:00")

    def test_incomplete_bar_is_never_emitted(self):
        self.capture["records"] = [self.capture["records"][-1]]
        record = self.capture["records"][0]
        record["request"]["end_time"] = "2026-10-15T17:50:00Z"
        for result in record["response"]["data"]["results"]:
            bar = copy.deepcopy(result["bars"][-1])
            bar["begins_at"] = "2026-10-15T17:40:00Z"
            result["bars"].append(bar)
        result = self.inspect()
        self.assertTrue(result["data_usable"])
        self.assertEqual(len(result["bars"]), 28)
        self.assertEqual(len(result["warnings"]), 2)

    def test_gap_fill_rejected_with_no_partial_record_published(self):
        self.capture["records"][-1]["response"]["data"]["results"][1]["bars"][0]["interpolated"] = True
        result = self.inspect()
        self.assertFalse(result["data_usable"])
        self.assertEqual(result["bars"], [])

    def test_bad_bar_geometry_rejected(self):
        self.capture["records"][-1]["response"]["data"]["results"][0]["bars"][0]["low_price"] = "999"
        self.assertFalse(self.inspect()["data_usable"])

    def test_indicator_period_cannot_silently_change(self):
        self.capture["records"][1]["response"]["data"]["results"][0]["indicators"][0]["params"]["period"] = 10
        self.assertFalse(self.inspect()["data_usable"])

    def test_bounds_and_adjustment_cannot_silently_change(self):
        self.capture["records"][-1]["request"]["adjustment_type"] = "none"
        self.assertFalse(self.inspect()["data_usable"])

    def test_missing_symbol_is_not_silently_dropped(self):
        self.capture["records"][1]["response"]["data"]["results"].pop()
        result = self.inspect()
        self.assertFalse(result["data_usable"])
        self.assertTrue(any("Missing requested" in e["reason"] for e in result["errors"]))

    def test_duplicate_series_and_record_rejected(self):
        self.capture["records"].append(copy.deepcopy(self.capture["records"][-1]))
        self.assertFalse(self.inspect()["data_usable"])

    def test_zero_or_crossed_quote_rejected(self):
        quote = self.capture["records"][0]["response"]["data"]["results"][0]["quote"]
        for bid in ("0", "999"):
            quote["bid_price"] = bid
            self.assertFalse(self.inspect()["data_usable"])

    def test_restricted_symbol_rejected(self):
        self.capture["records"][0]["response"]["data"]["results"][0]["quote"]["state"] = "inactive"
        self.assertFalse(self.inspect()["data_usable"])

    def test_read_adapter_blocks_write_before_transport_called(self):
        calls = []
        reader = RobinhoodReader(lambda *args: calls.append(args))
        with self.assertRaises(ValueError):
            reader.fetch("place_equity_order", {"symbols": ["AAPL"]})
        self.assertEqual(calls, [])

    def test_read_adapter_handles_real_mcp_text_envelope(self):
        source = self.capture["records"][0]
        reader = RobinhoodReader(lambda *_: {"content": [
            {"type": "text", "text": json.dumps(source["response"])}]},
            clock=lambda: datetime.fromisoformat(AS_OF))
        self.assertEqual(reader.fetch(source["tool"], source["request"])["response"], source["response"])

    def test_tool_error_is_not_zero_data(self):
        reader = RobinhoodReader(lambda *_: {"isError": True, "content": []})
        with self.assertRaises(ValueError):
            reader.fetch("get_equity_quotes", {"symbols": ["AAPL"]})


if __name__ == "__main__":
    unittest.main()
