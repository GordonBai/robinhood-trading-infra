import json
from pathlib import Path
from unittest import TestCase

from martin_agentic.trial import trial_proposal


AT='2026-10-16T14:00:00+00:00'


class TrialTests(TestCase):
    def setUp(self):
        self.policy=json.loads((Path(__file__).resolve().parents[1]/'tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        self.broker={'account':'••••0000','read_healthy':True,'new_risk_blocked':False,'last_observed_at':AT,
            'positions':{},'open_equity_orders':0,'other_asset_open_orders':0,
            'portfolio':{'cash':'500','total_value':'500','pending_deposits':'0'},
            'buying_power':'1000','unleveraged_buying_power':'500'}
        self.quote={'symbol':'DEMO','bid':'39.99','ask':'40','bid_at':AT,'ask_at':AT,'observed_at':AT}
        self.plan={'symbol':'DEMO','quantity':2,'entry_ceiling':'40','stop':'39','cost_buffer_per_share':'0.05'}

    def check(self,as_of=AT):
        return trial_proposal(self.policy,self.broker,self.quote,self.plan,as_of=as_of)

    def test_within_limits_is_neither_confirmation_nor_execution(self):
        result=self.check()
        self.assertTrue(result['within_trial_limits'])
        self.assertEqual(result['planned_risk_with_cost_buffer_usd'],'2.10')
        self.assertFalse(result['specific_order_approved'])
        self.assertFalse(result['execution_ready'])

    def test_three_shares_fail_both_notional_and_risk(self):
        self.plan['quantity']=3
        result=self.check()
        self.assertIn('TRIAL_POSITION_NOTIONAL_LIMIT',result['blockers'])
        self.assertIn('TRIAL_PLANNED_RISK_LIMIT',result['blockers'])

    def test_daily_loss_at_threshold_blocks_even_without_realized_losses(self):
        self.broker['portfolio']['total_value']='490'
        self.assertIn('TRIAL_DAILY_LOSS_LIMIT_REACHED',self.check()['blockers'])

    def test_margin_buying_power_does_not_expand_trial_cash_limit(self):
        self.broker['unleveraged_buying_power']='50'
        self.assertIn('TRIAL_UNLEVERAGED_CASH_LIMIT',self.check()['blockers'])

    def test_trial_cannot_roll_over_to_monday(self):
        self.assertIn('OUTSIDE_AUTHORIZED_TRIAL_DATE',self.check('2026-10-19T14:00:00Z')['blockers'])

    def test_stopped_source_and_old_quote_do_not_allow_proposal(self):
        self.broker['read_healthy']=False
        result=self.check('2026-10-16T14:00:31Z')
        self.assertIn('BROKER_OBSERVATION_NOT_READY',result['blockers'])
        self.assertIn('BROKER_OBSERVATION_STALE',result['blockers'])
        self.assertIn('QUOTE_STALE_OR_FUTURE',result['blockers'])

    def test_single_position_trial_rejects_adds(self):
        self.broker['positions']={'DEMO':{'quantity':'1'}}
        self.assertIn('FIRST_TRIAL_REQUIRES_FLAT_ACCOUNT_WITHOUT_ORDERS',self.check()['blockers'])

    def test_cash_in_transit_requires_review(self):
        self.broker['portfolio']['pending_deposits']='10'
        self.assertIn('UNSETTLED_DEPOSIT_REQUIRES_REVIEW',self.check()['blockers'])

    def test_no_extra_project_approval_does_not_bypass_tool_contract(self):
        self.policy.update(per_order_user_approval_required=False,confirmation_policy='follow_current_tool_contract')
        result=self.check()
        self.assertTrue(result['within_trial_limits'])
        self.assertFalse(result['additional_project_confirmation_required'])
        self.assertFalse(result['specific_order_approved'])
        self.policy['confirmation_policy']='skip_all'
        self.assertFalse(self.check()['within_trial_limits'])

    def test_live_guard_daily_baseline_uses_actual_session_equity(self):
        self.broker['risk_state']={'day_start_equity':'600'}
        self.broker['portfolio']['total_value']='590'
        self.assertIn('TRIAL_DAILY_LOSS_LIMIT_REACHED',self.check()['blockers'])
