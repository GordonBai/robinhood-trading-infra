from pathlib import Path
import json
import tempfile
from unittest import TestCase
from martin_agentic.rehearsal import run_rehearsal


class FaultRehearsalTests(TestCase):
    def test_executionbook_fault_matrix_preserves_real_simulation_boundary(self):
        with tempfile.TemporaryDirectory() as directory:
            report=run_rehearsal(directory)
            self.assertTrue(report['passed'],report['scenarios'])
            self.assertEqual(len(report['scenarios']),10)
            self.assertEqual(report['broker_calls'],0)
            self.assertFalse(report['robinhood_live_acceptance'])
            self.assertTrue(all(s['synthetic'] and s['audit_integrity']['integrity_valid'] for s in report['scenarios']))
            self.assertEqual(json.loads(Path(report['report_path']).read_text())['scenarios'],report['scenarios'])
