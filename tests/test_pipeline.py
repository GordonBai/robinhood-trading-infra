from copy import deepcopy
from decimal import Decimal
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from martin_agentic.capabilities import confirmation_path, check_oco
from martin_agentic.calendar import is_regular, prior_daily_date
from martin_agentic.collector import universe_event
from martin_luk.engine import ShadowEngine
from martin_luk.exits import exit_decision
from martin_luk.features import build_candidates
from martin_agentic.ledger import PaperLedger
from martin_agentic.market import inspect_recording,bar_available_at
from martin_agentic.paper import PaperDesk
from martin_luk.service import run_shadow
from martin_agentic.shadow import ShadowMonitor
from shadow_fixture import events,universe


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.path=Path(self.temp.name)/'engine.sqlite'
        self.engine=ShadowEngine(self.path)

    def tearDown(self):
        self.engine.close()
        self.temp.cleanup()

    def test_full_pipeline_generates_support_orders_fills_and_exit(self):
        results=[self.engine.apply(e) for e in events()]
        self.assertEqual(results[1]['state']['universe']['candidates'][0]['symbol'],'DEMO')
        self.assertEqual(results[4]['actions'][-1]['action'],'SIMULATED_SUBMIT')
        self.assertEqual(len(results[4]['state']['paper']['positions']),0)
        self.assertEqual(len(results[5]['state']['paper']['positions']),1)
        self.assertTrue(any(a['action']=='SIMULATED_RAISE_STOP' for a in results[6]['actions']))
        state=self.engine.snapshot()
        self.assertEqual(state['paper']['positions'],{})
        self.assertEqual(state['paper']['cash'],'505.94')
        self.assertEqual(state['live_orders'],0)

    def test_restart_armed_then_continue_and_replay_without_duplicate(self):
        source=events()
        for e in source[:4]: self.engine.apply(e)
        self.engine.close()
        self.engine=ShadowEngine(self.path)
        self.assertEqual(self.engine.snapshot()['machines']['DEMO']['state'],'ARMED')
        for e in source[4:]: self.engine.apply(e)
        before=self.engine.snapshot()
        for e in source: self.engine.apply(e)
        self.assertEqual(self.engine.snapshot(),before)
        self.assertEqual(len(before['paper']['orders']),1)

    def test_conflicting_event_rolls_back_strategy_and_account_together(self):
        e=events()[0]
        self.engine.apply(e)
        before=self.engine.snapshot()
        with self.assertRaises(ValueError): self.engine.apply({**e,'capital':'1000'})
        self.assertEqual(self.engine.snapshot(),before)

    def test_failure_after_fill_rolls_back_and_retries_once_after_restart(self):
        source=events()
        for e in source[:5]: self.engine.apply(e)
        before=self.engine.snapshot()
        with patch('martin_luk.engine.exit_decision',side_effect=RuntimeError('injected process failure')):
            with self.assertRaises(RuntimeError): self.engine.apply(source[5])
        self.assertEqual(self.engine.snapshot(),before)
        self.engine.close()
        self.engine=ShadowEngine(self.path)
        self.engine.apply(source[5])
        self.engine.apply(source[5])
        self.assertEqual(self.engine.snapshot()['paper']['positions']['DEMO']['quantity'],1)

    def test_source_failure_cancels_pending_entry_without_inventing_fill(self):
        source=events()
        for e in source[:5]: self.engine.apply(e)
        self.engine.apply({'id':'bad-source','kind':'data_status','healthy':False,
                           'at':'2026-10-15T09:40:06-04:00','errors':['stale']})
        self.assertTrue(self.engine.snapshot()['data_paused'])
        self.assertEqual(next(iter(self.engine.snapshot()['paper']['orders'].values()))['status'],'CANCELLED')
        self.assertEqual(self.engine.snapshot()['paper']['positions'],{})

    def test_deposit_does_not_change_unit_nav_or_daily_loss_basis(self):
        source=events()
        for e in source[:6]: self.engine.apply(e)
        before=self.engine.snapshot()
        self.engine.apply({'id':'deposit','kind':'flow','amount':'500','at':'2026-10-15T09:41:00-04:00'})
        after=self.engine.snapshot()
        self.assertAlmostEqual(Decimal(after['paper']['nav']['nav']),Decimal(before['paper']['nav']['nav']),places=25)
        self.assertEqual(after['day_start_nav'],before['day_start_nav'])
        self.assertAlmostEqual(Decimal(after['paper']['nav']['drawdown']),Decimal(before['paper']['nav']['drawdown']),places=25)

    def test_repeated_universe_does_not_reset_triggered_setup(self):
        source=events()
        for e in source[:6]: self.engine.apply(e)
        self.engine.apply({**source[1],'id':'new-universe','at':'2026-10-15T10:00:00-04:00'})
        self.assertEqual(self.engine.snapshot()['machines']['DEMO']['state'],'TRIGGERED')

    def test_quote_timeout_does_not_fill_pending_entry(self):
        source=events()
        for e in source[:5]: self.engine.apply(e)
        q=deepcopy(source[5]);q['at']='2026-10-15T09:40:30-04:00'
        result=self.engine.apply(q)
        self.assertEqual(result['state']['paper']['positions'],{})
        self.assertFalse(result['state']['data_health']['DEMO']['fresh'])

    def test_expired_entry_cancels_and_releases_reservation(self):
        source=events()
        for e in source[:5]: self.engine.apply(e)
        result=self.engine.apply({'id':'timer','kind':'timer','at':'2026-10-15T09:42:00-04:00'})
        order=next(iter(result['state']['paper']['orders'].values()))
        self.assertEqual(order['status'],'CANCELLED')
        self.assertEqual(order['remaining'],0)

    def test_bad_event_rolls_back_without_losing_last_good_state(self):
        for e in events()[:2]: self.engine.apply(e)
        before=self.engine.snapshot()
        bad=deepcopy(events()[2]);bad['data']['available_at']='2026-10-16T00:00:00Z'
        with self.assertRaises(ValueError): self.engine.apply(bad)
        self.assertEqual(self.engine.snapshot(),before)

    def test_service_once_persists_full_pipeline_and_restarts(self):
        inbox=Path(self.temp.name)/'inbox';inbox.mkdir()
        (inbox/'events.json').write_text(json.dumps({'events':events()}))
        result=run_shadow(inbox,self.path,once=True)
        self.assertTrue(result['healthy'])
        self.assertFalse(result['current_market_ready'])
        self.assertEqual(run_shadow(inbox,self.path,once=True)['events_checked'],8)
        self.assertEqual(self.engine.snapshot()['paper']['cash'],'505.94')

    def test_bad_service_file_blocks_whole_batch(self):
        inbox=Path(self.temp.name)/'inbox';inbox.mkdir()
        (inbox/'bad.json').write_text('[]')
        (inbox/'good.json').write_text(json.dumps({'events':events()}))
        result=run_shadow(inbox,self.path,once=True)
        self.assertFalse(result['healthy'])
        self.assertEqual(result['events_checked'],0)


class FeatureExitTests(unittest.TestCase):
    def screen(self,data):
        return build_candidates(data['histories'],data['indicators'],as_of='2026-10-15T09:25:00-04:00',universe_id=data['universe_id'])

    def test_features_derive_rank_adr_and_three_category_support(self):
        result=self.screen(universe())
        self.assertEqual(len(result['candidates']),1)
        setup=result['candidates'][0]
        self.assertEqual(setup['features']['rs_percentile'],'1')
        self.assertGreater(Decimal(setup['features']['adr20_fraction']),Decimal('.05'))
        self.assertEqual(set(setup['support_categories']),{'ema','avwap','swing'})

    def test_missing_one_symbol_prevents_silent_universe_change(self):
        data=universe();data['histories']['S1'].pop()
        result=self.screen(data)
        self.assertFalse(result['universe_complete'])
        self.assertEqual(result['candidates'],[])

    def test_history_gap_and_future_features_fail_closed(self):
        data=universe();data['histories']['DEMO'].pop(20)
        self.assertFalse(self.screen(data)['universe_complete'])
        data=universe();data['indicators']['DEMO']['9']['observed_at']='2026-10-16T00:00:00Z'
        self.assertFalse(self.screen(data)['universe_complete'])

    def test_wrong_ema_period_is_not_accepted(self):
        data=universe();data['indicators']['DEMO']['9']['period']=10
        self.assertFalse(self.screen(data)['universe_complete'])

    def test_calendar_holiday_early_close_and_dst(self):
        self.assertFalse(is_regular('2026-12-25T10:00:00-05:00'))
        self.assertFalse(is_regular('2026-11-27T13:01:00-05:00'))
        self.assertTrue(is_regular('2026-11-02T09:30:00-05:00'))
        self.assertEqual(prior_daily_date('2026-12-28T09:30:00-05:00'),'2026-12-24')
        with self.assertRaises(ValueError): bar_available_at('2026-11-27T18:00:00Z','5minute')

    def decide(self,bid,at='2026-10-15T10:00:00-04:00',**kwargs):
        position={'stop':'39.20','entry_price':'40','initial_stop':'39.20','quantity':2}
        quote={'bid':bid,'ask':str(Decimal(bid)+Decimal('.01')),'bid_at':at,'ask_at':at,'observed_at':at}
        return exit_decision(position,quote,as_of=at,**kwargs)

    def test_gap_stop_exits_at_bid_not_theoretical_stop(self):
        result=self.decide('36')
        self.assertEqual(result['action'],'EXIT_ALL')
        self.assertEqual(result['price'],'36')

    def test_trend_break_and_three_r_breakeven(self):
        self.assertEqual(self.decide('39.50',daily_ema9='39.60')['reason'],'PRIOR_DAILY_EMA9_BREAK')
        self.assertEqual(self.decide('42.40')['action'],'RAISE_STOP')

    def test_early_close_and_unknown_earnings(self):
        self.assertEqual(self.decide('41','2026-11-27T12:55:00-05:00')['reason'],'INTRADAY_CLOSE_POLICY')
        self.assertEqual(self.decide('41','2026-10-15T15:55:00-04:00',allow_overnight=True)['reason'],'EARNINGS_UNKNOWN_OR_DUE')

    def test_no_regular_stop_execution_outside_session(self):
        self.assertEqual(self.decide('36','2026-10-15T18:00:00-04:00')['action'],'WAIT')


class RegressionAndCapabilityTests(unittest.TestCase):
    def test_halt_requests_cancel_and_late_fill_is_still_accounted(self):
        with tempfile.TemporaryDirectory() as d:
            desk=PaperDesk(Path(d)/'desk.sqlite')
            source=[
                {'kind':'init','synthetic':True,'capital':'500','policy':{'risk_fraction':'.5','max_position_fraction':'.5','portfolio_risk_fraction':'1'}},
                {'kind':'submit','order_id':'A','symbol':'A','entry_ceiling':'40','stop':'39','cost_buffer_per_share':'0'},
                {'kind':'submit','order_id':'B','symbol':'B','entry_ceiling':'40','stop':'39','cost_buffer_per_share':'0'},
                {'kind':'fill','order_id':'A','quantity':6,'price':'40'},
                {'kind':'mark','symbol':'A','price':'10'}]
            for i,e in enumerate(source):
                desk.apply({**e,'id':str(i),'at':f'2026-10-15T09:0{i}:00-04:00'})
            self.assertTrue(desk.snapshot()['nav']['halted'])
            self.assertEqual(desk.snapshot()['orders']['B']['status'],'CANCEL_PENDING')
            self.assertIn({'action':'CANCEL_ENTRY','order_id':'B'},desk.snapshot()['halt_actions'])
            desk.apply({'id':'late','kind':'fill','at':'2026-10-15T09:05:00-04:00','order_id':'B','quantity':6,'price':'40'})
            self.assertTrue(desk.snapshot()['requires_reconciliation'])
            self.assertEqual(desk.snapshot()['positions']['B']['quantity'],6)
            desk.close()

    def test_protection_rejects_tight_prices_and_nonregular_session(self):
        at='2026-10-15T10:00:00-04:00'
        quote={'symbol':'DEMO','bid':'40','ask':'40.01','bid_at':at,'ask_at':at,'observed_at':at}
        plan={'symbol':'DEMO','side':'sell','quantity':1,'stop':'39.99','take_profit':'40.02','market_hours':'regular_hours'}
        self.assertFalse(check_oco(plan,quote,held_quantity=1,as_of=at)['locally_valid'])

    def test_week_old_indicator_recording_is_not_usable(self):
        root=Path(__file__).resolve().parents[1]
        data=json.loads((root/'tests/fixtures/synthetic_recording.json').read_text())
        data['records']=[r for r in data['records'] if r['tool']=='get_equity_technical_indicators']
        result=inspect_recording(data,as_of='2026-10-22T18:00:00Z')
        self.assertTrue(result['schema_valid'])
        self.assertFalse(result['data_usable'])
        self.assertFalse(result['strategy_ready'])

    def test_negative_and_invalid_equity_latch_until_reconciliation(self):
        for amount in ('-1','NaN'):
            with tempfile.TemporaryDirectory() as d:
                ledger=PaperLedger(Path(d)/'nav.sqlite')
                ledger.apply({'id':'init','kind':'init','at':'2026-10-15T09:00:00-04:00','amount':'500'})
                ledger.apply({'id':'bad','kind':'mark','at':'2026-10-15T10:00:00-04:00','amount':amount})
                self.assertTrue(ledger.snapshot()['halted'])
                self.assertEqual(ledger.snapshot()['equity'],'500')
                ledger.apply({'id':'good','kind':'mark','at':'2026-10-15T11:00:00-04:00','amount':'500'})
                self.assertTrue(ledger.snapshot()['halted'])
                ledger.close()

    def test_wrong_shape_json_is_isolated(self):
        with tempfile.TemporaryDirectory() as d:
            folder=Path(d)/'inbox';folder.mkdir()
            (folder/'bad.json').write_text('[]')
            monitor=ShadowMonitor(Path(d)/'monitor.sqlite')
            self.assertFalse(monitor.scan(folder)['data_usable'])
            monitor.close()

    def test_confirmations_do_not_disappear_with_app_setting_off(self):
        self.assertEqual(confirmation_path({'customer_approval_required':False,'order_checks':{}}),'EXPLICIT_CHAT_CONFIRMATION_REQUIRED')
        self.assertEqual(confirmation_path({'customer_approval_required':True,'order_checks':{}}),'SUBMIT_FOR_APP_APPROVAL_ONLY')
        self.assertEqual(confirmation_path({'customer_approval_required':True,'order_checks':{'alert':'x'}}),'SHOW_WARNING_AND_CONFIRM')

    def test_protection_validation_is_not_broker_acceptance(self):
        at='2026-10-15T10:00:00-04:00'
        quote={'symbol':'DEMO','bid':'40','ask':'40.01','bid_at':at,'ask_at':at,'observed_at':at}
        plan={'symbol':'DEMO','side':'sell','quantity':2,'stop':'39.20','take_profit':'44','market_hours':'regular_hours'}
        result=check_oco(plan,quote,held_quantity=2,as_of=at)
        self.assertTrue(result['locally_valid'])
        self.assertFalse(result['broker_accepted'])
        self.assertFalse(check_oco(plan,quote,held_quantity=1,as_of=at)['locally_valid'])
