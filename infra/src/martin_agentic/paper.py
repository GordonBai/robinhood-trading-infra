"""Transactional synthetic execution journal, not a price-path matching engine.

Fills/protection are explicitly supplied simulation events, never presumed from
bar highs/lows. No symbol can have two simultaneous entry orders in this version.
"""

from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sqlite3

from .ledger import PaperLedger, timestamp
from .risk import RiskPolicy, number, size_position


class PaperDesk:
    def __init__(self, path: str | Path):
        self.db = sqlite3.connect(path, timeout=10)
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS paper_state(id INTEGER PRIMARY KEY CHECK(id=1),payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS paper_events(sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT UNIQUE NOT NULL, fingerprint TEXT NOT NULL,
                event TEXT NOT NULL, result TEXT NOT NULL);
        """)

    def close(self):
        self.db.close()

    def snapshot(self):
        row = self.db.execute("SELECT payload FROM paper_state WHERE id=1").fetchone()
        if not row:
            raise ValueError("Paper desk is not initialized")
        return json.loads(row[0])

    def apply(self, event):
        if not isinstance(event, dict) or not isinstance(event.get("id"), str) or not event["id"]:
            raise ValueError("Require event id")
        at = timestamp(event["at"])
        raw = json.dumps(event, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(raw.encode()).hexdigest()
        self.db.execute("BEGIN IMMEDIATE")
        try:
            row = self.db.execute("SELECT fingerprint,result FROM paper_events WHERE event_id=?", (event["id"],)).fetchone()
            if row:
                if row[0] != digest:
                    raise ValueError("Paper event id reused with different content")
                self.db.rollback()
                return json.loads(row[1])
            row = self.db.execute("SELECT payload FROM paper_state WHERE id=1").fetchone()
            state = json.loads(row[0]) if row else None
            if state and at < timestamp(state["at"]):
                raise ValueError("Out-of-order paper event")
            state, outcome = self._transition(state, event, at)
            state["at"] = at.isoformat()
            encoded = json.dumps(state, sort_keys=True)
            result = {"event_id": event["id"], "outcome": outcome, "state": state}
            self.db.execute("INSERT INTO paper_state VALUES(1,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload", (encoded,))
            self.db.execute("INSERT INTO paper_events(event_id,fingerprint,event,result) VALUES(?,?,?,?)",
                            (event["id"], digest, raw, json.dumps(result, sort_keys=True)))
            self.db.commit()
            return result
        except Exception:
            self.db.rollback()
            raise

    @staticmethod
    def _transition(state, event, at):
        kind = event["kind"]
        if not state:
            if kind != "init" or event.get("synthetic") is not True:
                raise ValueError("First event must initialize an explicitly synthetic desk")
            initial = number(event["capital"])
            policy = event["policy"]
            RiskPolicy(number(policy["risk_fraction"]), number(policy["max_position_fraction"]))
            if not 0 < number(policy["portfolio_risk_fraction"]) <= 1:
                raise ValueError("Invalid explicit portfolio risk fraction")
            nav = PaperLedger._transition(None, "init", at, initial)
            return {"mode": "paper", "synthetic": True, "cash": str(initial), "policy": policy,
                    "orders": {}, "positions": {}, "nav": nav}, {"status": "INITIALIZED"}
        if kind == "init":
            raise ValueError("Cannot reinitialize a paper desk")
        orders, positions = state["orders"], state["positions"]
        cash = number(state["cash"])
        outcome = {"status": kind.upper()}
        if kind == "submit":
            symbol, order_id = event["symbol"], event["order_id"]
            if not isinstance(symbol, str) or not symbol:
                raise ValueError("Missing symbol")
            if order_id in orders or any(o["symbol"] == symbol and (o["remaining"] or symbol in positions)
                                         for o in orders.values()):
                raise ValueError("Duplicate order or same-symbol exposure")
            if any(p["protected"] < p["quantity"] for p in positions.values()):
                raise ValueError("UNPROTECTED_POSITION_BLOCKS_NEW_RISK")
            entry, stop, cost = (number(event[k]) for k in ("entry_ceiling", "stop", "cost_buffer_per_share"))
            pending_cash = sum(number(o["limit"]) * o["remaining"] + number(o["cost"]) * o["remaining"] for o in orders.values())
            pending_risk = sum((number(o["limit"]) - number(o["stop"]) + number(o["cost"])) * o["remaining"] for o in orders.values())
            open_risk = sum(max(number(p["mark"]) - number(p["stop"]), Decimal(0)) * p["quantity"] for p in positions.values())
            equity = number(state["nav"]["equity"])
            policy = state["policy"]
            remaining_risk = max(Decimal(0), equity * number(policy["portfolio_risk_fraction"]) - pending_risk - open_risk)
            outcome = size_position(equity=equity, available_buying_power=max(Decimal(0), cash - pending_cash),
                                    entry_ceiling=entry, stop=stop, cost_buffer_per_share=cost,
                                    remaining_portfolio_risk=remaining_risk,
                                    policy=RiskPolicy(number(policy["risk_fraction"]), number(policy["max_position_fraction"])),
                                    halted=state["nav"]["halted"])
            quantity = outcome["quantity"]
            if quantity:
                orders[order_id] = {"symbol": symbol, "quantity": quantity, "remaining": quantity,
                                    "filled": 0, "limit": str(entry), "stop": str(stop), "cost": str(cost),
                                    "status": "OPEN", "cancel_requested": False}
        elif kind in {"fill", "cancel"}:
            order = orders[event["order_id"]]
            if kind == "cancel":
                if not order["remaining"]:
                    raise ValueError("No open entry remainder to cancel")
                order["remaining"], order["status"] = 0, "CANCELLED"
                order["cancel_requested"] = False
            else:
                quantity, price = event["quantity"], number(event["price"])
                if type(quantity) is not int or not 0 < quantity <= order["remaining"] or not 0 < price <= number(order["limit"]):
                    raise ValueError("Invalid supplied limit-order fill")
                debit = quantity * (price + number(order["cost"]))
                if debit > cash:
                    raise ValueError("Insufficient simulated cash for supplied fill")
                cash -= debit
                order["remaining"] -= quantity
                order["filled"] += quantity
                order["status"] = "PARTIAL" if order["remaining"] else "FILLED"
                symbol = order["symbol"]
                position = positions.setdefault(symbol, {"quantity": 0, "protected": 0,
                                                         "mark": str(price), "stop": order["stop"]})
                old_quantity=position["quantity"]
                old_entry=number(position.get("entry_price",price))
                position["quantity"] += quantity
                position["mark"] = str(price)
                position["entry_price"]=str((old_entry*old_quantity+price*quantity)/position["quantity"])
                position.setdefault("initial_stop", order["stop"])
                if state["nav"]["halted"] or order.get("cancel_requested"):
                    state["requires_reconciliation"] = True
                    outcome["late_fill_after_halt"] = True
        elif kind == "protect":
            position = positions[event["symbol"]]
            qty = event["quantity"]
            if type(qty) is not int or not 0 <= qty <= position["quantity"]:
                raise ValueError("Protection cannot exceed actual filled position")
            position["protected"] = qty
            outcome["note"] = "Explicit synthetic protection acknowledgement, not a broker stop order"
        elif kind == "mark":
            price = number(event["price"])
            if price <= 0:
                raise ValueError("Position mark must be positive")
            positions[event["symbol"]]["mark"] = str(price)
        elif kind == "raise_stop":
            position = positions[event["symbol"]]
            stop = number(event["stop"])
            if not number(position["stop"]) <= stop < number(position["mark"]):
                raise ValueError("Stop may only tighten below current mark")
            position["stop"] = str(stop)
            # Simulated replacement acknowledgement is explicit in this event.
            outcome["note"] = "Synthetic stop replacement only"
        elif kind == "flow":
            amount=number(event['amount'])
            pending=sum((number(o['limit'])+number(o['cost']))*o['remaining'] for o in orders.values())
            if amount==0 or amount<0 and -amount>cash-pending:
                raise ValueError('Flow cannot withdraw reserved or invested cash')
            equity=cash+sum(number(p['mark'])*p['quantity'] for p in positions.values())
            state['nav']=PaperLedger._transition(state['nav'],'mark',at,equity)
            state['nav']=PaperLedger._transition(state['nav'],'flow',at,amount)
            cash+=amount
        elif kind == "exit":
            symbol, qty, price = event["symbol"], event["quantity"], number(event["price"])
            position = positions[symbol]
            if any(o["symbol"] == symbol and o["remaining"] for o in orders.values()):
                raise ValueError("Cancel pending entry remainder before exit")
            if type(qty) is not int or not 0 < qty <= position["quantity"] or price <= 0:
                raise ValueError("Invalid supplied exit fill")
            cash += qty * price
            position["quantity"] -= qty
            position["protected"] = min(position["protected"], position["quantity"])
            position["mark"] = str(price)
            if not position["quantity"]:
                del positions[symbol]
        else:
            raise ValueError("Unsupported paper event")
        state["cash"] = str(cash)
        equity = cash + sum(number(p["mark"]) * p["quantity"] for p in positions.values())
        state["nav"] = PaperLedger._transition(state["nav"], "mark", at, equity)
        if state["nav"]["halted"]:
            for order in orders.values():
                if order["remaining"]:
                    order["cancel_requested"] = True
                    order["status"] = "CANCEL_PENDING"
            state["halt_actions"] = [
                {"action": "CANCEL_ENTRY", "order_id": oid}
                for oid, order in orders.items() if order["remaining"]]
        state["unprotected_quantity"] = sum(p["quantity"] - p["protected"] for p in positions.values())
        return state, outcome
