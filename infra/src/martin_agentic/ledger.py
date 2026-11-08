"""Single-paper-account, unitized NAV ledger with transactional idempotent events."""

import hashlib
import json
import sqlite3
from datetime import datetime
from decimal import Decimal, localcontext
from pathlib import Path

from .risk import number


def timestamp(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("Timestamp must include a timezone")
    return result


class PaperLedger:
    """Flow events require a preceding mark at exactly the same timestamp.

    Account NAV includes unrealized P&L. The 30% breach is latched persistently.
    No method resets a breach or sends a broker instruction.
    """

    def __init__(self, path: str | Path):
        self.db = sqlite3.connect(path, timeout=10)
        self.db.execute("PRAGMA foreign_keys = ON")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS state (
                id INTEGER PRIMARY KEY CHECK(id=1), payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS events (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE, fingerprint TEXT NOT NULL,
                payload TEXT NOT NULL, result TEXT NOT NULL);
        """)

    def close(self):
        self.db.close()

    def snapshot(self) -> dict:
        row = self.db.execute("SELECT payload FROM state WHERE id=1").fetchone()
        if row is None:
            raise ValueError("Ledger is not initialized")
        return json.loads(row[0])

    def apply(self, event: dict) -> dict:
        if set(event) != {"id", "kind", "at", "amount"}:
            raise ValueError("Event requires exactly id, kind, at, amount")
        if not isinstance(event["id"], str) or not event["id"].strip():
            raise ValueError("Event id must be a nonempty string")
        at = timestamp(event["at"])
        invalid_number=False
        try:
            amount = number(event["amount"])
        except ValueError:
            if event["kind"] != "mark":
                raise
            amount=Decimal(-1)
            invalid_number=True
        canonical = json.dumps(event, sort_keys=True, separators=(",", ":"))
        fingerprint = hashlib.sha256(canonical.encode()).hexdigest()
        self.db.execute("BEGIN IMMEDIATE")
        try:
            prior = self.db.execute(
                "SELECT fingerprint,result FROM events WHERE event_id=?", (event["id"],)
            ).fetchone()
            if prior:
                if prior[0] != fingerprint:
                    raise ValueError("Event id reused with different content")
                self.db.rollback()
                return json.loads(prior[1])
            row = self.db.execute("SELECT payload FROM state WHERE id=1").fetchone()
            with localcontext() as ctx:
                ctx.prec = 40
                state = self._transition(json.loads(row[0]) if row else None,
                                         event["kind"], at, amount)
                if invalid_number:
                    state["health_reason"]="INVALID_EQUITY_NUMBER"
                    state["reported_equity"]=str(event["amount"])
            payload = json.dumps(state, sort_keys=True)
            self.db.execute("INSERT INTO state VALUES (1,?) ON CONFLICT(id) "
                            "DO UPDATE SET payload=excluded.payload", (payload,))
            self.db.execute("INSERT INTO events(event_id,fingerprint,payload,result) "
                            "VALUES (?,?,?,?)", (event["id"], fingerprint, canonical, payload))
            self.db.commit()
            return state
        except Exception:
            self.db.rollback()
            raise

    @staticmethod
    def _transition(state, kind, at, amount):
        if state is None:
            if kind != "init" or amount <= 0:
                raise ValueError("First event must initialize positive equity")
            equity, units, nav, high, max_dd, halted = amount, amount, Decimal(1), Decimal(1), Decimal(0), False
            last_mark = at
        else:
            if at < timestamp(state["at"]):
                raise ValueError("Out-of-order event")
            equity, units, nav, high, max_dd = (
                number(state[key]) for key in ("equity", "units", "nav", "nav_high", "max_drawdown")
            )
            halted = state["halted"]
            last_mark = timestamp(state["last_mark_at"])
            if kind == "mark":
                if amount < 0:
                    # Keep the last trusted valuation while durably freezing new risk.
                    return {**state, "halted": True, "account_health": "INVALID_VALUATION",
                            "health_reason": "NEGATIVE_EQUITY", "reported_equity": str(amount),
                            "at": at.isoformat()}
                equity = amount
                nav = equity / units
                high = max(high, nav)
                last_mark = at
            elif kind == "flow":
                if at != last_mark:
                    raise ValueError("Flow requires a fresh mark at the same timestamp")
                if amount == 0 or nav <= 0 or equity + amount <= 0:
                    raise ValueError("Flow must be nonzero and leave positive equity/NAV")
                units += amount / nav
                equity += amount
                # Per-unit NAV and NAV high remain unchanged by external cash flow.
            else:
                raise ValueError("Expected mark or flow on an initialized ledger")
        dd = (high - nav) / high
        max_dd = max(max_dd, dd)
        # Compare equity directly to threshold to avoid division rounding at the boundary.
        halted = halted or equity <= units * high * Decimal("0.70")
        return {
            "mode": "paper", "equity": str(equity), "units": str(units),
            "nav": str(nav), "nav_high": str(high), "drawdown": str(dd),
            "max_drawdown": str(max_dd), "max_drawdown_limit": "0.30",
            "halted": halted, "at": at.isoformat(),
            "last_mark_at": last_mark.isoformat(),
            "account_health": state.get("account_health", "HEALTHY") if state else "HEALTHY",
            "health_reason": state.get("health_reason") if state else None,
            "reported_equity": state.get("reported_equity") if state else None,
            "current_halt_equity": str(units * high * Decimal("0.70")),
        }
