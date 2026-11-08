import copy
import json
from pathlib import Path
import tempfile
import unittest

from martin_agentic.shadow import ShadowMonitor, single_instance
from test_market import RECORDING


class ShadowTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.inbox = Path(self.temp.name) / "inbox"
        self.inbox.mkdir()
        self.db_path = Path(self.temp.name) / "shadow.sqlite"
        self.monitor = ShadowMonitor(self.db_path)

    def tearDown(self):
        self.monitor.close()
        self.temp.cleanup()

    def test_unchanged_capture_becomes_stale_and_deleted_file_is_removed(self):
        capture = copy.deepcopy(RECORDING)
        capture["records"] = [capture["records"][0]]
        capture["records"][0]["observed_at"] = "2026-10-15T17:43:37Z"
        path = self.inbox / "quotes.json"
        path.write_text(json.dumps(capture))
        self.assertTrue(self.monitor.scan(self.inbox, as_of="2026-10-15T17:43:40Z")["data_usable"])
        self.assertFalse(self.monitor.scan(self.inbox, as_of="2026-10-15T17:44:40Z")["data_usable"])
        path.unlink()
        self.assertFalse(self.monitor.scan(self.inbox, as_of="2026-10-15T17:45:00Z")["data_usable"])
        self.assertEqual(self.monitor.db.execute("SELECT COUNT(*) FROM shadow_checks").fetchone()[0], 0)

    def test_partial_file_is_an_error_not_old_healthy_result(self):
        (self.inbox / "partial.json").write_text('{"unfinished":')
        result = self.monitor.scan(self.inbox, as_of="2026-10-15T17:44:03Z")
        self.assertFalse(result["data_usable"])
        self.assertEqual(result["broker_calls"], 0)

    def test_single_instance_lock(self):
        with single_instance(self.db_path):
            with self.assertRaises(ValueError):
                with single_instance(self.db_path):
                    self.fail("Acquired a second lock")

    def test_restart_keeps_cache_but_revalidates_freshness(self):
        (self.inbox / "recording.json").write_text(json.dumps(RECORDING))
        self.monitor.scan(self.inbox, as_of="2026-10-15T17:44:03Z")
        self.monitor.close()
        self.monitor = ShadowMonitor(self.db_path)
        result = self.monitor.scan(self.inbox, as_of="2026-10-16T17:44:03Z")
        self.assertFalse(result["data_usable"])
        self.assertFalse(result["execution_ready"])


if __name__ == "__main__":
    unittest.main()
