"""Local capture-inbox monitor. No broker/network/model calls are made here."""

from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import json
from pathlib import Path
import sqlite3
import time

from .market import inspect_recording


@contextmanager
def single_instance(db_path):
    # One canonical name for every symlink spelling of the same owner target.
    # SQLite hardlinks are unsafe (journal and lock names diverge); reject them
    # rather than pretending a path-based flock protects the shared inode.
    target = Path(db_path).expanduser().resolve()
    if target.exists() and target.stat().st_nlink != 1:
        raise ValueError("Multiple hardlinks are not supported for an owned database")
    with open(str(target) + ".lock", "a", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ValueError("Shadow monitor already owns this database") from exc
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


class ShadowMonitor:
    def __init__(self, db_path):
        self.db = sqlite3.connect(db_path, timeout=10)
        self.db.execute("""CREATE TABLE IF NOT EXISTS shadow_checks(
            path TEXT PRIMARY KEY, content_hash TEXT NOT NULL, checked_at TEXT NOT NULL,
            usable INTEGER NOT NULL, payload TEXT NOT NULL)""")
        self.db.commit()

    def close(self):
        self.db.close()

    def scan(self, inbox, *, as_of=None):
        folder = Path(inbox)
        if not folder.is_dir():
            raise ValueError("Capture inbox must be an existing directory")
        now = as_of or datetime.now(timezone.utc).isoformat()
        reports = []
        for path in sorted(folder.glob("*.json")):
            if path.is_symlink() or not path.is_file():
                continue
            digest = "unreadable"
            try:
                raw = path.read_bytes()
                digest = hashlib.sha256(raw).hexdigest()
                report = inspect_recording(json.loads(raw), as_of=now)
            except (ValueError, KeyError, TypeError, OSError) as exc:
                report = {"data_usable": False, "execution_ready": False,
                          "errors": [{"reason": str(exc)}], "warnings": []}
            # Recheck even unchanged files: a once-fresh quote ages out.
            self.db.execute("""INSERT INTO shadow_checks VALUES(?,?,?,?,?)
                ON CONFLICT(path) DO UPDATE SET content_hash=excluded.content_hash,
                checked_at=excluded.checked_at,usable=excluded.usable,payload=excluded.payload""",
                (str(path.resolve()), digest, now, int(report["data_usable"]), json.dumps(report, sort_keys=True)))
            reports.append({"file": path.name, "sha256": digest, "data_usable": report["data_usable"],
                            "strategy_ready": report.get("strategy_ready", False),
                            "errors": report["errors"]+report.get('freshness_errors',[]), "warnings": report["warnings"]})
        # Removed captures must not remain visible as healthy cached data.
        paths = {str(p.resolve()) for p in folder.glob("*.json") if p.is_file() and not p.is_symlink()}
        for (old_path,) in self.db.execute("SELECT path FROM shadow_checks").fetchall():
            if old_path not in paths:
                self.db.execute("DELETE FROM shadow_checks WHERE path=?", (old_path,))
        self.db.commit()
        return {"mode": "shadow-files", "checked_at": now, "files": reports,
                "data_usable": bool(reports) and all(r["data_usable"] for r in reports),
                "strategy_ready": False,
                "execution_ready": False, "broker_calls": 0,
                "note": "Local recording monitor only; independent live authentication is not implemented."}


def watch_inbox(inbox, db_path, *, once=False, poll_seconds=5):
    if not 0 < poll_seconds <= 60:
        raise ValueError("Poll interval must be in (0,60] seconds")
    with single_instance(db_path):
        monitor = ShadowMonitor(db_path)
        try:
            while True:
                result = monitor.scan(inbox)
                if once:
                    return result
                print(json.dumps(result, ensure_ascii=False, sort_keys=True), flush=True)
                time.sleep(poll_seconds)
        finally:
            monitor.close()
