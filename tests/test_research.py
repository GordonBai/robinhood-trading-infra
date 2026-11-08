"""Synthetic fixtures test research accounting, never establish strategy returns."""
from copy import deepcopy
from datetime import datetime,timedelta
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
from unittest import TestCase

from martin_luk.research import run_research,strategy_freeze
from shadow_fixture import events


class ResearchTests(TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
        source=self.root/'source.json';source.write_text(json.dumps({'synthetic':True,'fixture':'shadow_fixture.events'}))
        fp=hashlib.sha256(source.read_bytes()).hexdigest()
        data=[{**e,'source_sha256':fp,'synthetic_input':True} for e in events()[1:]]
        # First fold is a complete original intraday replay; second fold has its
        # own explicit later universe and no trigger (not silently omitted).
        second=deepcopy(data[0]);second.update(id='second-universe',at='2026-10-16T09:25:00-04:00')
        # Daily data for the second fixture deliberately reuses old information.
        # A quote-only/no-entry fold still records the candidate data diagnostics.
        second['data']['universe_id']='second-fixture-universe'
        end=deepcopy(data[-1]);end.update(id='second-quote',at='2026-10-16T15:55:00-04:00')
        end['data'].update(observed_at=end['at'],bid_at=end['at'],ask_at=end['at'])
        data += [second,end]
        dataset=self.root/'events.json';dataset.write_text(json.dumps(data))
        self.manifest={'schema_version':1,'strategy_freeze':strategy_freeze(),'synthetic_input':True,
            'policy':json.loads(Path('tests/fixtures/policies/live_trial_2026-10-19.json').read_text()),
            'sources':[{'path':'source.json','sha256':fp,'provider':'synthetic_fixture','synthetic':True}],
            'dataset':{'path':'events.json','sha256':hashlib.sha256(dataset.read_bytes()).hexdigest()},
            'folds':[{'id':'one','market_regime':'SYNTHETIC_TREND','train_start':'2026-10-15T09:00:00-04:00','train_end':'2026-10-15T09:20:00-04:00','test_start':'2026-10-15T09:25:00-04:00','test_end':'2026-10-15T16:00:00-04:00'},
                {'id':'two','market_regime':'SYNTHETIC_NO_SIGNAL','train_start':'2026-10-16T09:00:00-04:00','train_end':'2026-10-16T09:20:00-04:00','test_start':'2026-10-16T09:25:00-04:00','test_end':'2026-10-16T16:00:00-04:00'}],
            'costs':{'fee_basis':'roundtrip_buffer_charged_on_entry','per_share_fee':'0.01','entry_slippage_per_share':'0','exit_slippage_per_share':'0.01','model_cost_usd':'0.10','data_cost_usd':'0.10'},
            'scenarios':[{'id':'zero','protection_delay_seconds':0,'fill_fraction_per_quote':'1','stop_stress_slippage_per_share':'0.01'}]}
        self.path=self.root/'manifest.json';self.save()

    def save(self):self.path.write_text(json.dumps(self.manifest))
    def run_research(self):return run_research(self.path,self.root/'reports')

    def test_registered_replay_retains_no_signal_folds_and_never_claims_profitability(self):
        r=self.run_research()
        self.assertEqual(r['status'],'SIMULATED_COMPLETE',r.get('error'))
        self.assertEqual(len(r['results']),2)
        self.assertFalse(r['strategy_profitability_validated']);self.assertFalse(r['robinhood_execution_validated'])
        self.assertTrue(r['synthetic_market_input'])
        self.assertGreater(r['results'][1]['no_signal_events'],0)
        self.assertIn('screening',r['results'][1]['journal'][0])

    def test_freeze_pins_split_infra_module_and_detects_tamper(self):
        from unittest.mock import patch
        from martin_luk import research as research_module
        freeze = strategy_freeze()
        source_root = research_module.candidate_root()
        target = 'infra/src/martin_agentic/paper.py'
        self.assertIn(target, freeze['files'])
        self.assertIn('strategy/martin_luk/src/martin_luk/strategy.py',
                      freeze['files'])
        self.assertNotIn('paper.py', freeze['files'])
        original = Path.read_bytes
        def altered(path):
            if path.as_posix().endswith(target):
                return b'# tampered split module'
            return original(path)
        with patch.object(Path, 'read_bytes', altered), \
                patch.object(research_module, 'candidate_root',
                             return_value=source_root):
            changed = strategy_freeze()
        self.assertNotEqual(freeze['files'][target], changed['files'][target])

    def test_changed_source_and_frozen_strategy_are_registered_failures(self):
        (self.root/'source.json').write_text('tampered')
        first=self.run_research();self.assertEqual(first['status'],'FAILED');self.assertIn('SOURCE_HASH_MISMATCH',first['error'])
        self.manifest['strategy_freeze']['version']='different';self.save();second=self.run_research()
        self.assertIn('FREEZE_MISMATCH',second['error'])
        db=sqlite3.connect(second['registry_path'])
        try:self.assertEqual(db.execute('SELECT count(*) FROM runs').fetchone()[0],2)
        finally:db.close()

    def test_missing_manifest_is_also_registered(self):
        self.path.unlink();r=self.run_research()
        self.assertEqual(r['status'],'FAILED');self.assertIn('MANIFEST_MISSING',r['error'])
        self.assertTrue(Path(r['report_path']).is_file())

    def test_future_quote_and_missing_fold_data_never_complete(self):
        dataset=self.root/'events.json';data=json.loads(dataset.read_text())
        data[-1]['data']['ask_at']='2026-10-17T15:55:00-04:00';dataset.write_text(json.dumps(data))
        self.manifest['dataset']['sha256']=hashlib.sha256(dataset.read_bytes()).hexdigest();self.save()
        r=self.run_research();self.assertEqual(r['status'],'FAILED');self.assertIn('FUTURE_DATA',r['error'])

    def test_scenarios_are_all_reported_not_selected_by_returns(self):
        self.manifest['scenarios'] += [{'id':'stress','protection_delay_seconds':107,'fill_fraction_per_quote':'0.5','stop_stress_slippage_per_share':'0.20'}];self.save()
        r=self.run_research();self.assertEqual(len(r['results']),4,r.get('error'))
        self.assertEqual({x['scenario']['id'] for x in r['results']},{'zero','stress'})

    def test_cash_net_reconciliation_and_capital_highwater_carry_between_folds(self):
        r=self.run_research();self.assertEqual(r['status'],'SIMULATED_COMPLETE',r.get('error'))
        a,b=r['results']
        self.assertTrue(a['cash_pnl_reconciled']);self.assertTrue(b['cash_pnl_reconciled'])
        self.assertEqual(a['closing_cash_usd'],b['opening_cash_usd'])
        self.assertTrue(b['risk_state_carried_across_folds'])
        self.assertEqual(a['risk_checkpoint']['paper']['nav']['nav_high'],b['risk_checkpoint']['paper']['nav']['nav_high'])
        for fold in (a,b):
            from decimal import Decimal
            self.assertEqual(Decimal(fold['closing_cash_usd'])-Decimal(fold['opening_cash_usd']),Decimal(fold['net_trading_pnl_usd']))
            self.assertEqual(fold['fee_basis'],'roundtrip_buffer_charged_on_entry')

    def test_duplicate_fill_quote_does_not_create_extra_partial_execution(self):
        self.manifest['scenarios'][0]['fill_fraction_per_quote']='0.5';self.save()
        baseline=self.run_research()
        dataset=self.root/'events.json';data=json.loads(dataset.read_text())
        i=next(i for i,e in enumerate(data) if e['id']=='fill');data.insert(i+1,deepcopy(data[i]))
        dataset.write_text(json.dumps(data));self.manifest['dataset']['sha256']=hashlib.sha256(dataset.read_bytes()).hexdigest();self.save()
        replay=self.run_research()
        self.assertEqual(replay['status'],baseline['status'])
        self.assertEqual(replay['results'][0]['trades'],baseline['results'][0]['trades'])
        self.assertEqual(replay['results'][0]['closing_cash_usd'],baseline['results'][0]['closing_cash_usd'])

    def test_folds_cannot_overlap_or_train_on_test_future(self):
        self.manifest['folds'][0]['train_end']=self.manifest['folds'][0]['test_end'];self.save()
        r=self.run_research();self.assertEqual(r['status'],'FAILED');self.assertIn('BOUNDARIES',r['error'])

    def test_explicit_fee_basis_required_and_invalid_binary_manifest_registered(self):
        del self.manifest['costs']['fee_basis'];self.save();r=self.run_research()
        self.assertIn('FEE_BASIS',r['error'])
        self.path.write_bytes(bytes([255,254,253]));r=self.run_research()
        self.assertEqual(r['status'],'FAILED');self.assertTrue(Path(r['report_path']).is_file())

    def test_no_real_source_data_is_gap_never_oos_profit_evidence(self):
        self.manifest['sources'][0]['path']='absent-actual-history.json';self.save()
        r=self.run_research();self.assertEqual(r['status'],'DATA_GAP')
        self.assertFalse(r['strategy_profitability_validated']);self.assertEqual(r['results'],[])
        self.assertTrue(any('SOURCE_MISSING' in x for x in r['data_gaps']))

    def test_open_exposure_blocks_next_fold_without_reset(self):
        data=json.loads((self.root/'events.json').read_text())
        data=[e for e in data if e['id'] not in {'close','profit'}]
        dataset=self.root/'events.json';dataset.write_text(json.dumps(data));self.manifest['dataset']['sha256']=hashlib.sha256(dataset.read_bytes()).hexdigest();self.save()
        r=self.run_research();self.assertEqual(r['status'],'INCOMPLETE_OPEN_EXPOSURE',r.get('error'))
        self.assertEqual(r['results'][1]['status'],'BLOCKED_PREVIOUS_FOLD_OPEN_EXPOSURE')
        self.assertTrue(r['results'][1]['risk_state_not_reset'])

    def test_real_dataset_event_must_exist_in_frozen_source(self):
        self.manifest['synthetic_input']=False
        data=json.loads((self.root/'events.json').read_text())
        for event in data:event['synthetic_input']=False
        source=self.root/'source.json';source.write_text(json.dumps({'events':[{'id':'unrelated','at':data[0]['at']}]}))
        fp=hashlib.sha256(source.read_bytes()).hexdigest()
        for event in data:event['source_sha256']=fp
        self.manifest['sources'][0].update(sha256=fp,synthetic=False,provider='recorded_fixture')
        dataset=self.root/'events.json';dataset.write_text(json.dumps(data));self.manifest['dataset']['sha256']=hashlib.sha256(dataset.read_bytes()).hexdigest();self.save()
        r=self.run_research();self.assertEqual(r['status'],'DATA_GAP')
        self.assertTrue(any('EVENT_NOT_IN_FROZEN_SOURCE' in x for x in r['data_gaps']))
        self.assertIn('REAL_COVERAGE_CONTRACT_REQUIRED',r['data_gaps'])
        self.assertEqual(r['results'],[])

    def test_filled_stop_risk_remains_in_budget_occupancy(self):
        from decimal import Decimal
        r=self.run_research();self.assertEqual(r['status'],'SIMULATED_COMPLETE',r.get('error'))
        exposure=next(e for e in r['results'][0]['exposures'] if e['at']=='2026-10-15T09:40:06-04:00')
        self.assertEqual(Decimal(exposure['reserved_risk_usd']),0)
        self.assertGreater(Decimal(exposure['open_position_risk_usd']),0)
        self.assertEqual(Decimal(exposure['occupied_risk_usd']),Decimal(exposure['open_position_risk_usd']))
        self.assertEqual(Decimal(exposure['risk_budget_utilization_fraction']),
            Decimal(exposure['occupied_risk_usd'])/Decimal(exposure['portfolio_risk_budget_usd']))
        self.assertEqual(Decimal(exposure['remaining_risk_budget_usd']),
            Decimal(exposure['portfolio_risk_budget_usd'])-Decimal(exposure['occupied_risk_usd']))
        self.assertEqual(exposure['unprotected_quantity'],0)
        pending=next(e for e in r['results'][0]['exposures'] if e['at']=='2026-10-15T09:40:05-04:00')
        self.assertGreater(Decimal(pending['reserved_risk_usd']),0)
        self.assertEqual(Decimal(pending['open_position_risk_usd']),0)
        final=r['results'][0]['exposures'][-1]
        self.assertEqual(Decimal(final['occupied_risk_usd']),0)

    def test_unprotected_inventory_and_cost_assumptions_are_explicit(self):
        self.manifest['scenarios'][0]['protection_delay_seconds']=30;self.save()
        r=self.run_research();self.assertEqual(r['status'],'SIMULATED_COMPLETE',r.get('error'))
        fold=r['results'][0]
        exposure=next(e for e in fold['exposures'] if e['at']=='2026-10-15T09:40:06-04:00')
        self.assertGreater(exposure['unprotected_quantity'],0)
        from decimal import Decimal
        self.assertGreater(Decimal(exposure['unprotected_notional_usd']),0)
        self.assertGreater(Decimal(exposure['occupied_risk_usd']),0)
        self.assertEqual(Decimal(fold['net_after_run_cost_usd']),
            Decimal(fold['net_trading_pnl_usd'])-Decimal(fold['model_cost_usd'])-Decimal(fold['data_cost_usd']))
        self.assertIn('not observed',fold['run_cost_basis'])

    def add_after_profit_quote(self,*,bid):
        dataset=self.root/'events.json';data=json.loads(dataset.read_text())
        quote=deepcopy(next(e for e in data if e['id']=='fill'))
        quote.update(id='after-three-r',at='2026-10-15T10:01:00-04:00')
        from decimal import Decimal
        quote['data'].update(bid=str(bid),ask=str(Decimal(bid)+Decimal('.01')),
            **{key:quote['at'] for key in ('observed_at','bid_at','ask_at')})
        data.insert(next(i for i,e in enumerate(data) if e['id']=='close'),quote)
        dataset.write_text(json.dumps(data))
        self.manifest['dataset']['sha256']=hashlib.sha256(dataset.read_bytes()).hexdigest();self.save()

    def test_three_r_suggestion_does_not_create_unavailable_breakeven_stop(self):
        from decimal import Decimal
        data=json.loads((self.root/'events.json').read_text())
        entry=Decimal(next(e for e in data if e['id']=='fill')['data']['ask'])
        self.add_after_profit_quote(bid=entry-Decimal('.01'))
        r=self.run_research();self.assertEqual(r['status'],'SIMULATED_COMPLETE',r.get('error'))
        fold=r['results'][0];profit=next(row for row in fold['journal'] if row['event_id']=='profit')
        self.assertEqual(profit['exit_decision']['action'],'RAISE_STOP')
        deferred=profit['execution_events'][0]
        self.assertEqual(deferred['outcome'],'DEFERRED_STOP_ADJUSTMENT')
        self.assertEqual(Decimal(deferred['suggested_stop']),entry)
        self.assertLess(Decimal(deferred['effective_stop']),entry-Decimal('.01'))
        retrace=next(row for row in fold['journal'] if row['event_id']=='after-three-r')
        self.assertEqual(retrace['exit_decision']['action'],'HOLD')
        self.assertEqual(fold['trades'][0]['at'],'2026-10-15T15:55:00-04:00')
        self.assertEqual(fold['trades'][0]['exit_reason'],'INTRADAY_CLOSE_POLICY')
        self.assertTrue(fold['cash_pnl_reconciled'])
        self.assertEqual(fold['stop_adjustment_execution'],'deferred_safe_native_replacement_not_verified')
        self.assertFalse(r['strategy_profitability_validated'])

    def test_original_structural_stop_still_exits_after_deferred_three_r(self):
        self.add_after_profit_quote(bid='35')
        r=self.run_research();self.assertEqual(r['status'],'SIMULATED_COMPLETE',r.get('error'))
        fold=r['results'][0]
        self.assertEqual(fold['trades'][0]['at'],'2026-10-15T10:01:00-04:00')
        self.assertEqual(fold['trades'][0]['exit_reason'],'STRUCTURAL_STOP')
        self.assertTrue(fold['cash_pnl_reconciled'])

    def test_loss_streak_and_tail_survive_no_signal_fold(self):
        self.manifest['costs']['exit_slippage_per_share']='8';self.save()
        r=self.run_research();self.assertEqual(r['status'],'SIMULATED_COMPLETE',r.get('error'))
        first,second=r['results']
        self.assertEqual(first['max_consecutive_losses'],1)
        self.assertEqual(first['loss_streak_at_end'],1)
        self.assertEqual(second['max_consecutive_losses'],1)
        self.assertEqual(second['loss_streak_at_end'],1)
        self.assertEqual(second['fold_max_consecutive_losses'],0)
        self.assertIsNone(second['loss_tail_min_usd'])
        self.assertEqual(second['scenario_loss_tail_min_usd'],first['loss_tail_min_usd'])


class RealCoverageTests(TestCase):
    """Coverage verifier fixtures are synthetic and confer no market evidence."""
    def setUp(self):
        from martin_luk.research import _real_coverage
        self.validate=_real_coverage
        from shadow_fixture import universe
        from martin_agentic.calendar import calendar,prior_daily_date,NY
        self.events=[];self.sources={'frozen-source':{'coverage_evidence':{},'regime_evidence':{}}}
        self.manifest={'folds':[], 'coverage':{'schema_version':1,'regime_classifier':{'method':'endpoint_return_v1','absolute_return_threshold':'0.02'},'folds':{}}}
        for i,day in enumerate(('15','16')):
            fid=str(i);start=f'2026-10-{day}T09:25:00-04:00';end=f'2026-10-{day}T09:45:00-04:00'
            fold={'id':fid,'test_start':start,'test_end':end,'market_regime':'TREND_UP'}
            self.manifest['folds'].append(fold)
            data=universe();data['histories']['A']=data['histories'].pop('DEMO');data['indicators']['A']=data['indicators'].pop('DEMO')
            data['histories']['B']=data['histories'].pop('S1');data['indicators']['B']=data['indicators'].pop('S1')
            symbols=list(data['histories'])
            sessions=calendar().sessions_window(prior_daily_date(start),-64)
            for symbol in symbols:
                for bar,dt in zip(data['histories'][symbol],sessions):
                    bar['symbol']=symbol;bar['begins_at']=dt.to_pydatetime().replace(tzinfo=__import__('datetime').timezone.utc).isoformat()
                    bar['available_at']=(datetime.combine(dt.date(),__import__('datetime').time(),NY)+timedelta(days=1)).isoformat()
                    bar['observed_at']=start
                for point in data['indicators'][symbol].values():
                    point.update(begins_at=data['histories'][symbol][-1]['begins_at'],available_at=data['histories'][symbol][-1]['available_at'],observed_at=start)
            self.manifest['coverage']['folds'][fid]={'universe_symbols':symbols,'quote_max_gap_seconds':15,'bar_max_publish_lag_seconds':15,
                'source_completeness':{'source_sha256':'frozen-source','id':fid},
                'regime_evidence':{'source_sha256':'frozen-source','id':fid}}
            self.sources['frozen-source']['coverage_evidence'][fid]={'symbols':symbols,'start':start,'end':end,
                'pagination_exhausted':True,'truncated':False}
            self.sources['frozen-source']['regime_evidence'][fid]={'method':'endpoint_return_v1','benchmark':'SPY',
                'absolute_return_threshold':'0.02','classification':'TREND_UP',
                'observations':[{'at':f'2026-10-{day}T09:30:00-04:00','close':'100'},{'at':end,'close':'103'}]}
            self.events.append({'kind':'universe','at':start,'data':data})
            opened=datetime.fromisoformat(f'2026-10-{day}T09:30:00-04:00')
            for symbol in symbols:
                for n in range(3):
                    t=opened+timedelta(minutes=5*n)
                    self.events.append({'kind':'bar','at':(t+timedelta(minutes=5)).isoformat(),
                        'data':{'symbol':symbol,'begins_at':t.isoformat(),'interval':'5minute','bounds':'regular'}})
                for n in range(61):
                    self.events.append({'kind':'quote','at':(opened+timedelta(seconds=15*n)).isoformat(),'data':{'symbol':symbol}})

    def check(self):return self.validate(self.events,self.manifest,self.sources)

    def test_complete_explicit_schedule_and_frozen_evidence_pass_coverage_only(self):
        self.assertEqual(self.check(),[])
        del self.manifest['coverage']
        self.assertEqual(self.check(),['REAL_COVERAGE_CONTRACT_REQUIRED'])

    def test_missing_middle_bar_and_symbol_are_not_declared_complete(self):
        self.events=[e for e in self.events if not (e['kind']=='bar' and e['data']['symbol']=='A'
            and e['data']['begins_at']=='2026-10-15T09:35:00-04:00')]
        self.assertTrue(any('COVERAGE_5MIN_BAR_MISSING:0:A:' in gap for gap in self.check()))
        self.events=[e for e in self.events if e['kind']=='universe' or e['data']['symbol']!='B']
        self.assertTrue(any('COVERAGE_QUOTE_GAP:0:B:' in gap for gap in self.check()))
        del self.events[0]['data']['histories']['B']
        self.assertTrue(any('COVERAGE_UNIVERSE_SYMBOL_MISSING:0:' in gap for gap in self.check()))

    def test_quote_hole_truncation_and_unproven_regime_fail_closed(self):
        self.events=[e for e in self.events if not (e['kind']=='quote' and e['data']['symbol']=='A'
            and e['at']=='2026-10-15T09:35:00-04:00')]
        self.sources['frozen-source']['coverage_evidence']['0']['truncated']=True
        self.sources['frozen-source']['regime_evidence'].pop('0')
        gaps=self.check()
        self.assertIn('SOURCE_COMPLETENESS_NOT_PROVEN:0',gaps)
        self.assertIn('MARKET_REGIME_NOT_PROVEN:0',gaps)
        self.assertTrue(any('COVERAGE_QUOTE_GAP:0:A:' in gap for gap in gaps))

    def test_regime_label_must_match_frozen_benchmark_arithmetic(self):
        record=self.sources['frozen-source']['regime_evidence']['0']
        record['observations'][-1]['close']='97'
        self.assertIn('MARKET_REGIME_NOT_PROVEN:0',self.check())
        record['classification']='ADVERSE';self.manifest['folds'][0]['market_regime']='ADVERSE'
        self.assertEqual(self.check(),[])

    def test_warmup_universe_is_retained_and_late_bar_or_tuned_threshold_rejected(self):
        self.manifest['folds'][0]['test_start']='2026-10-15T09:30:00-04:00'
        self.sources['frozen-source']['coverage_evidence']['0']['start']=self.manifest['folds'][0]['test_start']
        self.assertEqual(self.check(),[])
        bar=next(e for e in self.events if e['kind']=='bar' and e['data']['symbol']=='A')
        bar['at']='2026-10-15T09:40:00-04:00'
        self.assertTrue(any('COVERAGE_5MIN_BAR_MISSING:0:A:' in gap for gap in self.check()))
        self.sources['frozen-source']['regime_evidence']['0']['absolute_return_threshold']='0.01'
        self.assertIn('MARKET_REGIME_NOT_PROVEN:0',self.check())

    def test_full_coverage_cannot_upgrade_modeled_or_missing_provenance(self):
        from martin_luk.research import _load
        declarations={'input_mode':'recorded_point_in_time','actual_receipt_timestamps_verified':True,
            'actual_point_in_time_verified':True,'historical_adjustment_as_of_verified':True}
        with tempfile.TemporaryDirectory() as tmp:
            base=Path(tmp)
            manifest=deepcopy(self.manifest)
            for fold in manifest['folds']:
                fold['train_start']=fold['test_start'].replace('09:25:00','09:00:00')
                fold['train_end']=fold['test_start'].replace('09:25:00','09:20:00')
            manifest.update(schema_version=1,synthetic_input=False,strategy_freeze=strategy_freeze(),
                policy=json.loads(Path('tests/fixtures/policies/live_trial_2026-10-19.json').read_text()),
                costs={'fee_basis':'roundtrip_buffer_charged_on_entry','per_share_fee':'0','entry_slippage_per_share':'0',
                    'exit_slippage_per_share':'0','model_cost_usd':'0','data_cost_usd':'0'},
                scenarios=[{'id':'baseline','protection_delay_seconds':0,'fill_fraction_per_quote':'1','stop_stress_slippage_per_share':'0'}],
                **declarations)
            events=[]
            for i,event in enumerate(sorted(self.events,key=lambda e:datetime.fromisoformat(e['at']))):
                events.append({**deepcopy(event),'id':'coverage-'+str(i)})
            manifest['folds'][1]['market_regime']='ADVERSE'
            self.sources['frozen-source']['regime_evidence']['1']['classification']='ADVERSE'
            self.sources['frozen-source']['regime_evidence']['1']['observations'][-1]['close']='97'
            source={'events':events,'coverage_evidence':self.sources['frozen-source']['coverage_evidence'],
                'regime_evidence':self.sources['frozen-source']['regime_evidence'],'provenance':dict(declarations)}
            def load(source_proof,manifest_proof):
                source['provenance']=source_proof
                raw=json.dumps(source);(base/'source.json').write_text(raw)
                fp=hashlib.sha256(raw.encode()).hexdigest()
                dataset=[{**event,'synthetic_input':False,'source_sha256':fp} for event in events]
                raw=json.dumps(dataset);(base/'events.json').write_text(raw)
                candidate=deepcopy(manifest)
                for key in declarations:candidate.pop(key,None)
                candidate.update(manifest_proof)
                for spec in candidate['coverage']['folds'].values():
                    spec['source_completeness']['source_sha256']=fp;spec['regime_evidence']['source_sha256']=fp
                candidate.update(sources=[{'path':'source.json','sha256':fp,'provider':'controlled_test_fixture','synthetic':False}],
                    dataset={'path':'events.json','sha256':hashlib.sha256(raw.encode()).hexdigest()})
                gaps=_load(candidate,base)[1]
                if gaps:
                    path=base/'manifest.json';path.write_text(json.dumps(candidate))
                    self.assertEqual(run_research(path,base/'reports')['status'],'DATA_GAP')
                return gaps
            # Controlled assertions test the gate, never authenticate real data.
            self.assertEqual(load(declarations,declarations),[])
            modeled={**declarations,'input_mode':'historical_simulation','actual_point_in_time_verified':False,
                'historical_adjustment_as_of_verified':False}
            gaps=load(declarations,modeled)
            self.assertIn('RECORDED_POINT_IN_TIME_MODE_REQUIRED:manifest',gaps)
            self.assertTrue(any('actual_point_in_time_verified' in gap for gap in gaps))
            gaps=load(modeled,declarations)
            self.assertIn('RECORDED_POINT_IN_TIME_MODE_REQUIRED:source:source.json',gaps)
            missing=dict(declarations);missing.pop('actual_receipt_timestamps_verified')
            gaps=load(missing,declarations)
            self.assertIn('RECORDED_PROOF_DECLARATION_REQUIRED:source:source.json:actual_receipt_timestamps_verified',gaps)
            numeric={**declarations,'actual_receipt_timestamps_verified':1}
            self.assertIn('RECORDED_PROOF_DECLARATION_REQUIRED:manifest:actual_receipt_timestamps_verified',load(declarations,numeric))
