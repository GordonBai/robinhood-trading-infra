from copy import deepcopy
from datetime import datetime,timedelta
import json
from pathlib import Path
import tempfile
from unittest import TestCase

from martin_agentic.live_risk import LiveRiskGuard
from martin_agentic.transport import TransportError,safe_read_failure
from test_broker import snapshot,FakeBroker,order

class LiveRiskTests(TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.path=Path(self.tmp.name)/'risk.sqlite'
        self.policy=json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        self.guard=LiveRiskGuard(self.path,self.policy,simulation=True)
        self.at=datetime.fromisoformat('2026-10-16T14:00:00Z')
    def tearDown(self):self.guard.close();self.tmp.cleanup()
    def snap(self,equity='500',cash='500',orders=None,days=0):
        f=FakeBroker();f.cash=cash;f.orders=orders or []
        s=snapshot(f,(self.at+timedelta(days=days)).isoformat());s['synthetic']=True;s['portfolio']['total_value']=equity
        self.at+=timedelta(seconds=1)
        return s
    def test_daily_stop_remains_locked_after_recovery_and_restart(self):
        self.guard.observe(self.snap())
        r=self.guard.observe(self.snap(equity='490'));self.assertIn('DAILY_EQUITY_LOSS_LOCKED',r['reasons'])
        self.guard.close();self.guard=LiveRiskGuard(self.path,self.policy,simulation=True)
        self.assertTrue(self.guard.observe(self.snap(equity='500'))['blocked'])
        self.assertFalse(self.guard.observe(self.snap(days=3))['blocked'])
    def test_project_high_water_stop_persists_across_session_and_recovery(self):
        self.guard.observe(self.snap());self.guard.observe(self.snap(equity='1000'))
        r=self.guard.observe(self.snap(equity='700'))
        self.assertIn('PROJECT_DRAWDOWN_LOCKED',r['reasons'])
        self.guard.close();self.guard=LiveRiskGuard(self.path,self.policy,simulation=True)
        r=self.guard.observe(self.snap(equity='1100',days=3))
        self.assertIn('PROJECT_DRAWDOWN_LOCKED',r['reasons'])
    def test_unexplained_deposit_cannot_unlock_risk_or_raise_nav_high(self):
        self.guard.observe(self.snap())
        r=self.guard.observe(self.snap(equity='1000',cash='1000'))
        self.assertIn('UNRECONCILED_CASH_FLOW',r['reasons']);self.assertEqual(r['nav_high'],'1')
        self.assertTrue(self.guard.observe(self.snap(equity='1000',cash='1000'))['blocked'])
    def test_execution_explained_cash_changes_are_not_deposits(self):
        self.guard.observe(self.snap())
        buy=order();buy['quantity']='1';buy['cumulative_quantity']='1';buy['executions']=buy['executions'][:1]
        # Test only one exact broker execution with explicit cash bookkeeping.
        buy['executions'][0].update(quantity='1',price='40',fees='0')
        r=self.guard.observe(self.snap(cash='460',orders=[buy]))
        self.assertFalse(r['blocked'])
        r=self.guard.observe(self.snap(cash='460',orders=[buy]));self.assertFalse(r['blocked'])
    def test_changed_account_or_policy_does_not_reset_guard(self):
        self.guard.observe(self.snap())
        other=self.snap();other['account_key']='different'
        with self.assertRaisesRegex(ValueError,'ACCOUNT_OR_CLOCK'):self.guard.observe(other)
        with self.assertRaisesRegex(ValueError,'POLICY_CHANGED'):
            LiveRiskGuard(self.path,{**self.policy,'project_max_drawdown_fraction':'0.5'},simulation=True)
    def test_bootstrap_and_pending_deposits_freeze_unreconciled_capital(self):
        r=self.guard.observe(self.snap(equity='600',cash='600'))
        self.assertIn('INITIAL_CAPITAL_NOT_RECONCILED',r['reasons'])
    def test_stale_observation_cannot_rewind_risk(self):
        s=self.snap();self.guard.observe(s)
        s['observed_at']='2026-10-15T14:00:00Z'
        with self.assertRaisesRegex(TransportError,'BROKER_SNAPSHOT_SUPERSEDED'):self.guard.observe(s)
        self.assertTrue(safe_read_failure(TransportError('BROKER_SNAPSHOT_SUPERSEDED'))['retryable'])

    def test_new_day_first_read_cannot_erase_gap_loss(self):
        self.guard.observe(self.snap())
        r=self.guard.observe(self.snap(equity='490',days=3))
        self.assertEqual(r['day_start_equity'],'500')
        self.assertIn('DAILY_EQUITY_LOSS_LOCKED',r['reasons'])
