import json
import subprocess
import sys
import tempfile
import unittest
from decimal import Decimal as D
from pathlib import Path

from martin_agentic.cli import dry_run
from martin_agentic.ledger import PaperLedger
from martin_agentic.risk import RiskPolicy, number, size_position


ROOT = Path(__file__).resolve().parents[1]


class SizingTests(unittest.TestCase):
    def size(self, **changes):
        inputs = dict(equity=D("500"), available_buying_power=D("500"),
                      entry_ceiling=D("40"), stop=D("39.20"),
                      cost_buffer_per_share=D("0.05"), remaining_portfolio_risk=D("2.50"),
                      policy=RiskPolicy(D("0.005"), D("0.33")))
        inputs.update(changes)
        return size_position(**inputs)

    def test_buffer_reduces_three_shares_to_two(self):
        # 3 * 0.85 = 2.55 exceeds 2.50; ignoring friction would permit 3.
        result = self.size()
        self.assertEqual(result["quantity"], 2)
        self.assertEqual(D(result["estimated_stop_loss_including_buffer"]), D("1.70"))

    def test_each_independent_cap_binds(self):
        for changes in (
            {"available_buying_power": D("40.05")},
            {"remaining_portfolio_risk": D("0.85")},
            {"policy": RiskPolicy(D("0.005"), D("0.08"))},
        ):
            with self.subTest(changes=changes):
                self.assertEqual(self.size(**changes)["quantity"], 1)

    def test_insufficient_whole_share_skips(self):
        result = self.size(entry_ceiling=D("180"), stop=D("179"),
                           policy=RiskPolicy(D("0.005"), D("0.20")))
        self.assertEqual(result["decision"], "SKIP")

    def test_halt_blocks_otherwise_valid_size(self):
        self.assertEqual(self.size(halted=True)["reason"], "DRAWDOWN_HALT")
        self.assertEqual(self.size(halted=True)["quantity"], 0)

    def test_no_overspend_from_friction(self):
        self.assertEqual(self.size(available_buying_power=D("80"))["quantity"], 1)

    def test_invalid_values_fail_closed(self):
        for changes in ({"stop": D("40")}, {"equity": D("0")},
                        {"cost_buffer_per_share": D("-1")},
                        {"available_buying_power": D("NaN")},
                        {"remaining_portfolio_risk": D("-1")}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.size(**changes)

    def test_money_rejects_float_nan_bool_and_missing(self):
        for value in (0.1, True, "NaN", "Infinity", None):
            with self.subTest(value=value), self.assertRaises(ValueError):
                number(value)

    def test_missing_policy_is_not_defaulted(self):
        with self.assertRaises(ValueError):
            RiskPolicy(None, D("0.20"))


class LedgerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "paper.sqlite"
        self.ledger = PaperLedger(self.path)
        self.counter = 0
        self.send("init", "500", "09:30")

    def tearDown(self):
        self.ledger.close()
        self.tmp.cleanup()

    def send(self, kind, amount, time):
        self.counter += 1
        return self.ledger.apply({"id": str(self.counter), "kind": kind,
                                  "amount": amount, "at": f"2026-10-15T{time}:00-04:00"})

    def test_threshold_at_initial_capital(self):
        self.assertFalse(self.send("mark", "350.01", "10:00")["halted"])
        self.assertTrue(self.send("mark", "350", "10:01")["halted"])

    def test_high_watermark_ratchets_and_breach_latches(self):
        self.send("mark", "600", "10:00")
        self.assertTrue(self.send("mark", "420", "11:00")["halted"])
        self.assertTrue(self.send("mark", "650", "12:00")["halted"])
        self.ledger.close()
        self.ledger = PaperLedger(self.path)
        self.assertTrue(self.ledger.snapshot()["halted"])

    def test_deposit_does_not_hide_drawdown(self):
        self.send("mark", "600", "10:00")
        before = self.send("mark", "480", "11:00")
        after = self.send("flow", "120", "11:00")
        self.assertEqual(D(before["drawdown"]), D("0.2"))
        self.assertEqual(after["drawdown"], before["drawdown"])
        self.assertEqual(D(after["current_halt_equity"]), D("525"))
        self.assertTrue(self.send("mark", "525", "12:00")["halted"])

    def test_withdrawal_does_not_create_a_loss(self):
        self.send("mark", "500", "10:00")
        state = self.send("flow", "-200", "10:00")
        self.assertEqual(D(state["drawdown"]), 0)
        self.assertEqual(D(state["current_halt_equity"]), D("210"))

    def test_stale_flow_and_out_of_order_rejected_without_state_change(self):
        before = self.ledger.snapshot()
        for kind, amount, time in (("flow", "100", "10:00"), ("mark", "500", "09:00")):
            with self.assertRaises(ValueError):
                self.send(kind, amount, time)
            self.assertEqual(self.ledger.snapshot(), before)

    def test_duplicate_id_is_idempotent_conflicting_payload_rejected(self):
        event = {"id": "deposit", "kind": "flow", "amount": "100",
                 "at": "2026-10-15T09:30:00-04:00"}
        first = self.ledger.apply(event)
        self.assertEqual(self.ledger.apply(event), first)
        self.assertEqual(D(self.ledger.snapshot()["equity"]), 600)
        with self.assertRaises(ValueError):
            self.ledger.apply({**event, "amount": "200"})
        self.assertEqual(self.ledger.snapshot(), first)

    def test_zero_equity_halts_and_cannot_be_recapitalized_silently(self):
        state = self.send("mark", "0", "10:00")
        self.assertTrue(state["halted"])
        with self.assertRaises(ValueError):
            self.send("flow", "500", "10:00")

    def test_full_withdrawal_rejected(self):
        with self.assertRaises(ValueError):
            self.send("flow", "-500", "09:30")

    def test_halt_persists_after_deposit_and_drives_sizing(self):
        self.send("mark", "350", "10:00")
        state = self.send("flow", "650", "10:00")
        result = size_position(equity=number(state["equity"]),
                               available_buying_power=D("1000"), entry_ceiling=D("40"),
                               stop=D("39"), cost_buffer_per_share=D("0"),
                               remaining_portfolio_risk=D("5"),
                               policy=RiskPolicy(D("0.005"), D("0.33")),
                               halted=state["halted"])
        self.assertEqual(result["quantity"], 0)


class CLITests(unittest.TestCase):
    def test_synthetic_scenario_and_gap_risk(self):
        scenario = json.loads((ROOT / "examples/dry_run_500.json").read_text())
        result = dry_run(scenario)
        self.assertEqual(result["sizing"]["quantity"], 2)
        self.assertEqual(D(result["outcomes"][-1]["pnl_after_assumed_cost"]), D("-8.10"))
        with self.assertRaises(ValueError):
            dry_run({**scenario, "mode": "live"})

    def test_cli_persists_replays_and_prints_current_state(self):
        with tempfile.TemporaryDirectory() as temp:
            path = str(Path(temp) / "ledger.sqlite")
            args = [sys.executable, "-m", "martin_agentic", "replay-ledger", "--events",
                    str(ROOT / "examples/nav_events.json"), "--db", path]
            first = subprocess.run(args, capture_output=True, text=True, check=True)
            second = subprocess.run(args, capture_output=True, text=True, check=True)
            self.assertEqual(first.stdout, second.stdout)
            current = json.loads(second.stdout)["current"]
            self.assertTrue(current["halted"])
            self.assertEqual(D(current["max_drawdown"]), D("0.30"))


if __name__ == "__main__":
    unittest.main()
