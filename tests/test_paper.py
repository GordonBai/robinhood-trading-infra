import copy
import json
from pathlib import Path
import tempfile
import unittest

from martin_agentic.paper import PaperDesk


ROOT = Path(__file__).resolve().parents[1]
EVENTS = json.loads((ROOT / "examples/paper_execution.json").read_text())


class PaperTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "paper.sqlite"
        self.desk = PaperDesk(self.path)
        self.desk.apply(EVENTS[0])

    def tearDown(self):
        self.desk.close()
        self.temp.cleanup()

    def test_complete_example_cash_and_equity_reconcile(self):
        for event in EVENTS[1:]:
            self.desk.apply(event)
        state = self.desk.snapshot()
        self.assertEqual(state["cash"], "507.89")
        self.assertEqual(state["nav"]["equity"], "507.89")
        self.assertEqual(state["positions"], {})
        self.assertEqual(state["unprotected_quantity"], 0)

    def test_partial_fill_only_protects_actual_quantity(self):
        for event in EVENTS[1:3]:
            self.desk.apply(event)
        self.assertEqual(self.desk.snapshot()["unprotected_quantity"], 1)
        with self.assertRaises(ValueError):
            self.desk.apply({**EVENTS[3], "quantity": 2})
        self.desk.apply(EVENTS[3])
        self.desk.apply(EVENTS[4])
        self.assertEqual(self.desk.snapshot()["unprotected_quantity"], 1)

    def test_unprotected_position_blocks_new_risk(self):
        for event in EVENTS[1:3]:
            self.desk.apply(event)
        event = {**EVENTS[1], "id": "other", "order_id": "other", "symbol": "OTHER",
                 "at": "2026-10-15T09:40:10-04:00"}
        with self.assertRaisesRegex(ValueError, "UNPROTECTED"):
            self.desk.apply(event)

    def test_pending_orders_consume_cash_and_risk_atomically(self):
        for index in range(10):
            self.desk.apply({**EVENTS[1], "id": f"submit-{index}", "order_id": str(index), "symbol": f"S{index}"})
        state = self.desk.snapshot()
        self.assertLessEqual(sum(o["quantity"] for o in state["orders"].values()), 11)
        # The 2% aggregate risk cap (10 dollars) binds before all ten can reserve.
        self.assertLess(len(state["orders"]), 10)

    def test_duplicate_event_and_reopen_do_not_duplicate_fills(self):
        for event in EVENTS[1:3]:
            self.desk.apply(event)
        before = self.desk.snapshot()
        self.desk.close()
        self.desk = PaperDesk(self.path)
        self.desk.apply(EVENTS[2])
        self.assertEqual(self.desk.snapshot(), before)
        with self.assertRaises(ValueError):
            self.desk.apply({**EVENTS[2], "price": "39.99"})

    def test_cancel_remainder_does_not_remove_filled_position(self):
        for event in EVENTS[1:3]:
            self.desk.apply(event)
        self.desk.apply({"id": "cancel", "kind": "cancel", "order_id": "entry-demo",
                         "at": "2026-10-15T09:41:00-04:00"})
        state = self.desk.snapshot()
        self.assertEqual(state["orders"]["entry-demo"]["remaining"], 0)
        self.assertEqual(state["positions"]["DEMO"]["quantity"], 1)
        with self.assertRaises(ValueError):
            self.desk.apply({**EVENTS[4], "at": "2026-10-15T09:42:00-04:00"})

    def test_exit_with_pending_entry_rejected(self):
        for event in EVENTS[1:3]:
            self.desk.apply(event)
        with self.assertRaises(ValueError):
            self.desk.apply({**EVENTS[-1], "quantity": 1})

    def test_oversell_and_bad_fill_roll_back(self):
        for event in EVENTS[1:-1]:
            self.desk.apply(event)
        before = self.desk.snapshot()
        with self.assertRaises(ValueError):
            self.desk.apply({**EVENTS[-1], "quantity": 3})
        self.assertEqual(self.desk.snapshot(), before)

    def test_30_percent_halt_persists_and_blocks_submit(self):
        # Separate high-risk synthetic policy solely to reach the boundary in a fixture.
        self.desk.close()
        self.path = Path(self.temp.name) / "drawdown.sqlite"
        self.desk = PaperDesk(self.path)
        init = copy.deepcopy(EVENTS[0])
        init["policy"] = {"risk_fraction": "0.5", "max_position_fraction": "1", "portfolio_risk_fraction": "1"}
        self.desk.apply(init)
        self.desk.apply(EVENTS[1])
        self.desk.apply({**EVENTS[2], "quantity": 12})
        self.desk.apply({**EVENTS[3], "quantity": 12})
        self.desk.apply({"id": "loss", "kind": "mark", "symbol": "DEMO", "price": "20",
                         "at": "2026-10-15T10:00:00-04:00"})
        self.assertTrue(self.desk.snapshot()["nav"]["halted"])
        self.desk.close()
        self.desk = PaperDesk(self.path)
        result = self.desk.apply({**EVENTS[1], "id": "after-halt", "order_id": "other", "symbol": "OTHER",
                                  "at": "2026-10-15T10:01:00-04:00"})
        self.assertEqual(result["outcome"]["reason"], "DRAWDOWN_HALT")


if __name__ == "__main__":
    unittest.main()
