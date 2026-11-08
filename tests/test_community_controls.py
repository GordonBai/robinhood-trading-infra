"""Cross-entry controls and durable dispositions; all broker data are fixtures."""
from datetime import timedelta
import json
from pathlib import Path
import tempfile
from unittest import TestCase

from martin_agentic.worker_budget import WorkerBudget
from martin_agentic.watchdog import entry_inhibit,release_entry_inhibit


class CommunityControlsTests(TestCase):
    def test_budget_restart_cannot_raise_frozen_limits_or_reset_reservations(self):
        with tempfile.TemporaryDirectory() as directory:
            p=Path(directory)/'book.sqlite';b=WorkerBudget(p,'2026-10-19',entry_limit=1,total_limit=2)
            b.reserve({'action_id':'one','kind':'entry'});b.close()
            with self.assertRaisesRegex(ValueError,'CANNOT_INCREASE'):
                WorkerBudget(p,'2026-10-19',entry_limit=2,total_limit=3)
            b=WorkerBudget(p,'2026-10-19',entry_limit=1,total_limit=2)
            self.assertFalse(b.status()['entries_available']);self.assertEqual(b.status()['total_calls_reserved'],1)
            b.close()

    def test_sticky_gate_blocks_propose_and_claim_but_never_existing_protection(self):
        import test_execution
        f=test_execution.ExecutionTests();f.setUp();self.addCleanup(f.tearDown)
        f.propose()
        gate=Path(str(f.path.resolve())+'.entry-inhibit.json')
        gate.write_text(json.dumps({'db':str(f.path.resolve()),'blocked':True}))
        with self.assertRaisesRegex(ValueError,'ENTRY_CONTROL_INHIBITED'):f.propose()
        self.assertIsNone(f.book.claim())
        gate.unlink() # Test preparation only; production release uses the audited API below.
        f.entry_filled()
        gate.write_text(json.dumps({'db':str(f.path.resolve()),'blocked':True}))
        self.assertEqual(f.book.claim()['kind'],'protect')

    def test_audited_release_accepts_safe_recovery_but_preserves_failure_and_risk(self):
        import test_recovery
        f=test_recovery.RecoveryTests('test_ack_keeps_failure_state_issues_risk_and_manual_provenance');f.setUp();self.addCleanup(f.tearDown)
        state=f.book.state();state['issues']=[i for i in state['issues'] if not i.startswith('PROTECTION_WAIT_EXCEEDED')]
        f.book._save(state);f.book.db.commit();f.acknowledged()
        gate=Path(str(f.path.resolve())+'.entry-inhibit.json');gate.write_text(json.dumps({'db':str(f.path.resolve()),'blocked':True}))
        before=f.book.state();risk=f.book.risk_guard.state()
        r=release_entry_inhibit(f.book,f.snapshot,reason='Fresh fixture reconciled flat')
        self.assertTrue(r['released']);self.assertFalse(r['execution_enabled'])
        self.assertFalse(entry_inhibit(gate,f.path)['blocked'])
        self.assertEqual(f.book.state(),before);self.assertEqual(f.book.risk_guard.state(),risk)
        content=json.loads(gate.read_text());content['reason']='tampered';gate.write_text(json.dumps(content))
        self.assertTrue(entry_inhibit(gate,f.path)['blocked'])

    def test_unaudited_false_gate_and_stale_release_fail_closed(self):
        import test_execution
        f=test_execution.ExecutionTests();f.setUp();self.addCleanup(f.tearDown)
        gate=Path(str(f.path.resolve())+'.entry-inhibit.json')
        gate.write_text(json.dumps({'db':str(f.path.resolve()),'blocked':False,'release_id':'not-a-receipt'}))
        self.assertTrue(entry_inhibit(gate,f.path)['blocked'])
        s=f.snap();f.now+=timedelta(seconds=31)
        with self.assertRaisesRegex(ValueError,'FRESH'):release_entry_inhibit(f.book,s,reason='Too old')
