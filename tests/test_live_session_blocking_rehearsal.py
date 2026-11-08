import importlib.util
from pathlib import Path
from unittest import TestCase


class FullControllerBlockingRehearsalTests(TestCase):
    def test_script_review_controller_reconciles_during_blocked_io_without_model(self):
        path=Path(__file__).resolve().parents[1]/'scripts/rehearse_live_session_blocking.py'
        spec=importlib.util.spec_from_file_location('script_review_rehearsal',path)
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        report=module.run_rehearsal(duration=1,poll_seconds=.02,agent_mode='script-review')
        self.assertTrue(report['passed'],report['checks']);self.assertTrue(report['synthetic'])
        self.assertEqual(report['model_reservations'],0);self.assertEqual(report['worker_calls'],1)
        self.assertEqual(report['unknown_final_state'],'UNKNOWN')
        self.assertEqual(report['protection_action_final_state'],'OBSERVED')
        self.assertFalse(report['production_ready'])

    def test_real_controller_reconciles_protection_and_retains_unknown_while_callbacks_block(self):
        path = Path(__file__).resolve().parents[1] / 'scripts/rehearse_live_session_blocking.py'
        spec = importlib.util.spec_from_file_location('full_controller_rehearsal', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        report = module.run_rehearsal(duration=1.0, poll_seconds=.02)
        self.assertTrue(report['passed'], report['checks'])
        self.assertFalse(report['full_requested_120_second_scope'])
        self.assertTrue(report['synthetic'])
        self.assertTrue(report['checks']['missing_guardian_blocks_new_entries'])
        self.assertGreaterEqual(report['protected_position_observations_during_block'], 2)
        self.assertEqual(report['unknown_final_state'], 'UNKNOWN')
        self.assertEqual(report['protection_action_final_state'], 'OBSERVED')
