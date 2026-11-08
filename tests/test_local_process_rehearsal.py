import importlib.util
import json
from pathlib import Path
import tempfile
from unittest import TestCase


class LocalProcessRehearsalTests(TestCase):
    def module(self):
        source = Path(__file__).resolve().parents[1] / 'scripts/rehearse_local_process_faults.py'
        spec = importlib.util.spec_from_file_location('local_process_rehearsal', source)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_owned_process_faults_and_restart_preserve_pending_without_broker_calls(self):
        with tempfile.TemporaryDirectory() as folder:
            result = self.module().run_rehearsal(folder, duration_seconds=.25)
            self.assertTrue(result['passed'])
            self.assertEqual(json.loads((Path(folder) / 'local-process-faults.json').read_text()), result)
            self.assertEqual(result['broker_writes'], 0)
            self.assertEqual(result['actual_model_calls'], 0)
            self.assertTrue(result['independent_process_watchdog']['protection_and_unknown_rows_unchanged'])
            self.assertGreaterEqual(result['blocked_callbacks']['ticks_while_both_callbacks_blocked'], 2)
            self.assertFalse(result['host_sleep_tested'])
            self.assertFalse(result['real_auth_expiry_tested'])
            self.assertFalse(result['live_acceptance'])

    def test_rehearsal_rejects_unbounded_inputs(self):
        with tempfile.TemporaryDirectory() as folder:
            for seconds in (0, 181, float('nan')):
                with self.assertRaisesRegex(ValueError, 'INVALID_LOCAL_REHEARSAL_BOUNDS'):
                    self.module().run_rehearsal(folder, duration_seconds=seconds)
