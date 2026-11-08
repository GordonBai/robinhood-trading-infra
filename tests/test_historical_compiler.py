"""Compiler proof limits and raw price preservation, never strategy evidence."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from martin_luk.historical_compiler import UNIVERSE, compile_historical


class HistoricalCompilerTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        self.spec=dict(schema_version=1,universe_symbols=list(UNIVERSE),assumptions=dict(
            bar_publish_lag_seconds=2,quote_receipt_lag_seconds=1,adjustment='assume_split',ema_method='first_close_seed_all_available_daily'),
            regime_classifier=dict(method='endpoint_return_v1',absolute_return_threshold='0.02',benchmark='SPY'),
            folds=[dict(id='a',train_start='2026-10-15T12:00:00Z',train_end='2026-10-15T13:00:00Z',test_start='2026-10-15T13:30:00Z',test_end='2026-10-15T13:40:00Z',market_regime='RANGE'),
                dict(id='b',train_start='2026-10-16T12:00:00Z',train_end='2026-10-16T13:00:00Z',test_start='2026-10-16T13:30:00Z',test_end='2026-10-16T13:40:00Z',market_regime='RANGE')],
            sources=[],policy={},costs={},scenarios=[])
        self.quote={'symbol':'SOFI','timestamp':'2026-10-15T13:30:00.000432999Z','bid_price':15,'ask_price':15.01,'bid_size':100,'ask_size':100}
        self.add_source('quotes.json',dict(request={'feed':'sip'},quotes={'SOFI':[self.quote]}),'quote')

    def add_source(self,name,data,kind,**metadata):
        path=self.root/name;path.write_text(json.dumps(data))
        self.spec['sources'].append(dict(path=name,sha256=hashlib.sha256(path.read_bytes()).hexdigest(),kind=kind,acquired_at='2026-10-20T12:00:00Z',**metadata))

    def run_compile(self):
        path=self.root/'spec.json';path.write_text(json.dumps(self.spec))
        return compile_historical(path,self.root/'output',clock=lambda:datetime(2026,10,20,13,tzinfo=timezone.utc))

    def test_raw_probe_is_gap_not_false_coverage_and_keeps_nanoseconds(self):
        r=self.run_compile();self.assertEqual(r['status'],'DATA_GAP')
        self.assertFalse(r['actual_point_in_time_verified']);self.assertFalse(r['actual_adjustment_verified'])
        self.assertIn('SPLIT_ADJUSTMENT_NOT_VERIFIED:provider_default_raw',r['gaps'])
        self.assertTrue(any(g.startswith('PAGINATION_NOT_PROVEN') for g in r['gaps']))
        events=json.loads((self.root/'output/events.json').read_text())
        quote=next(e for e in events if e['kind']=='quote')
        self.assertEqual(quote['data']['raw_exchange_timestamp'],self.quote['timestamp'])
        self.assertEqual(quote['historical_receipt_mode'],'MODELED')
        self.assertEqual(quote['data']['actual_acquired_at'],'2026-10-20T12:00:00Z')
        self.assertEqual(quote['data']['bid'],'15')
        self.assertFalse(r['legacy_strict_runner_authorized'])

    def test_hash_tampering_fails_before_price_import(self):
        (self.root/'quotes.json').write_text('{}')
        r=self.run_compile();self.assertTrue(any('RAW_SOURCE_HASH_MISMATCH' in g for g in r['gaps']))
        self.assertFalse((self.root/'output/events.json').exists())

    def test_other_feed_is_not_consolidated_quote_coverage(self):
        self.spec['sources']=[]
        self.add_source('iex.json',dict(request={'feed':'iex'},quotes={'SOFI':[self.quote]}),'quote')
        r=self.run_compile();self.assertIn('SIP_REQUIRED:iex.json',r['gaps'])
        events=json.loads((self.root/'output/events.json').read_text())
        self.assertFalse(any(e['kind']=='quote' for e in events))

    def test_early_close_and_dst_are_exchange_schedule_not_fixed_clock(self):
        self.spec['folds'][0].update(test_start='2026-11-27T14:30:00Z',test_end='2026-11-27T21:00:00Z')
        self.spec['folds'][1].update(train_start='2026-11-30T12:00:00Z',train_end='2026-11-30T13:00:00Z',test_start='2026-11-30T14:30:00Z',test_end='2026-11-30T14:40:00Z')
        self.spec['folds'][0].update(train_start='2026-11-27T12:00:00Z',train_end='2026-11-27T13:00:00Z')
        r=self.run_compile()
        missing=[g for g in r['gaps'] if g.startswith('FIVE_MINUTE_BAR_MISSING:a:SOFI:')]
        self.assertEqual(len(missing),42)
        self.assertTrue(missing[0].endswith('14:30:00+00:00'))
        self.assertTrue(missing[-1].endswith('17:55:00+00:00'))

    def test_outputs_do_not_overwrite_existing_freeze(self):
        self.run_compile()
        with self.assertRaisesRegex(ValueError,'MUST_BE_EMPTY'):self.run_compile()

    def test_fixed_universe_cannot_be_silently_reduced(self):
        self.spec['universe_symbols']=['SOFI'];r=self.run_compile()
        self.assertTrue(any('FIXED_TEN_SYMBOL_UNIVERSE_REQUIRED' in g for g in r['gaps']))

    def test_adjacent_nanosecond_quotes_are_not_deduplicated(self):
        self.spec['sources']=[]
        second=dict(self.quote,timestamp='2026-10-15T13:30:00.000432998Z',bid_price=14.99)
        self.add_source('nano.json',dict(request={'feed':'sip'},quotes={'SOFI':[second,self.quote]}),'quote')
        self.run_compile();events=json.loads((self.root/'output/events.json').read_text())
        quotes=[e for e in events if e['kind']=='quote']
        self.assertEqual([e['data']['raw_exchange_timestamp'] for e in quotes],[second['timestamp'],self.quote['timestamp']])
        self.assertLess(quotes[0]['at'],quotes[1]['at'])

    def test_robinhood_split_request_keeps_daily_utc_date_label(self):
        self.add_source('rh.json',dict(tool='get_equity_historicals',observed_at='2026-10-20T12:00:00Z',request=dict(symbols=['SOFI'],start_time='2026-10-14T00:00:00Z',end_time='2026-10-15T00:00:00Z',interval='day',bounds='regular',adjustment_type='split'),
            response={'data':{'results':[dict(symbol='SOFI',interval='day',bounds='regular',bars=[dict(begins_at='2026-10-14T00:00:00Z',
                open_price='15',high_price='16',low_price='14',close_price='15.1',volume=1000,interpolated=False)])]}}),'daily',pagination_exhausted=True,truncated=False)
        r=self.run_compile();self.assertTrue(r['actual_adjustment_verified']);self.assertFalse(r['historical_adjustment_as_of_verified'])
        events=json.loads((self.root/'output/events.json').read_text())
        bar=next(e for e in events if e['kind']=='universe')['data']['histories']['SOFI'][0]
        self.assertEqual(bar['begins_at'],'2026-10-14T00:00:00+00:00')
        self.assertEqual(bar['adjustment'],'split')
        self.assertEqual(bar['adjustment_evidence'],'EXPLICIT_FROZEN_RH_SPLIT_REQUEST')

    def test_robinhood_interpolation_rejected(self):
        self.add_source('rh.json',dict(tool='get_equity_historicals',observed_at='2026-10-20T12:00:00Z',request=dict(symbols=['SOFI'],start_time='2026-10-14T00:00:00Z',end_time='2026-10-15T00:00:00Z',interval='day',bounds='regular',adjustment_type='split'),
            response={'data':{'results':[dict(symbol='SOFI',interval='day',bounds='regular',bars=[dict(begins_at='2026-10-14T00:00:00Z',
                open_price='15',high_price='16',low_price='14',close_price='15.1',volume=1000,interpolated=True)])]}}),'daily')
        r=self.run_compile();self.assertTrue(any('INTERPOLATED_RH_BAR' in g for g in r['gaps']))

    def rh_source(self,*,symbol='SOFI',start='2026-10-14T00:00:00Z',end='2026-10-15T00:00:00Z',duplicate=False):
        group=dict(symbol='SOFI',interval='day',bounds='regular',bars=[dict(begins_at='2026-10-14T00:00:00Z',
            open_price='15',high_price='16',low_price='14',close_price='15.1',volume=1000,interpolated=False)])
        self.add_source('rh.json',dict(tool='get_equity_historicals',observed_at='2026-10-20T12:00:00Z',
            request=dict(symbols=[symbol],start_time=start,end_time=end,interval='day',bounds='regular',adjustment_type='split'),
            response={'data':{'results':[group,group] if duplicate else [group]}}),'daily',pagination_exhausted=True,truncated=False)

    def test_rh_unrequested_symbol_rejected(self):
        self.rh_source(symbol='MSFT');r=self.run_compile()
        self.assertTrue(any('RH_RESULT_SYMBOL_NOT_REQUESTED' in g for g in r['gaps']))
        self.assertFalse(r['actual_adjustment_verified'])

    def test_rh_row_outside_frozen_request_rejected(self):
        self.rh_source(start='2026-10-20T00:00:00Z',end='2026-10-21T00:00:00Z');r=self.run_compile()
        self.assertTrue(any('RH_ROW_OUTSIDE_REQUEST_WINDOW' in g for g in r['gaps']))

    def test_rh_duplicate_symbol_group_rejected_before_overwrite(self):
        self.rh_source(duplicate=True);r=self.run_compile()
        self.assertTrue(any('RH_DUPLICATE_SYMBOL_GROUP' in g for g in r['gaps']))

    def test_same_nanosecond_distinct_and_repeated_quote_updates_preserved(self):
        self.spec['sources']=[]
        second=dict(self.quote,bid_price=14.99)
        self.add_source('same-time.json',dict(request={'feed':'sip'},quotes={'SOFI':[self.quote,second,second]}),'quote')
        self.run_compile();events=json.loads((self.root/'output/events.json').read_text())
        quotes=[e for e in events if e['kind']=='quote']
        self.assertEqual([e['data']['bid'] for e in quotes],['15','14.99','14.99'])
        self.assertEqual(len({e['id'] for e in quotes}),3)

    def test_saturday_folds_cannot_be_ready_with_no_events(self):
        for fold,hour in zip(self.spec['folds'],('13','14')):
            fold.update(train_start='2026-10-17T11:00:00Z',train_end='2026-10-17T12:00:00Z',
                test_start='2026-10-17T'+hour+':30:00Z',test_end='2026-10-17T'+hour+':40:00Z')
        r=self.run_compile();self.assertEqual(r['status'],'DATA_GAP');self.assertEqual(r['event_count'],0)
        for fid in ('a','b'):
            self.assertIn('FOLD_NO_REGULAR_SESSION_OVERLAP:'+fid,r['gaps'])
            self.assertIn('FOLD_NO_NORMALIZED_EVENTS:'+fid,r['gaps'])

    def test_weekday_afterhours_only_folds_remain_data_gap(self):
        for fold,day in zip(self.spec['folds'],('15','16')):
            fold.update(test_start='2026-10-'+day+'T21:00:00Z',test_end='2026-10-'+day+'T21:10:00Z')
        r=self.run_compile();self.assertEqual(r['status'],'DATA_GAP');self.assertEqual(r['event_count'],0)
        self.assertIn('FOLD_NO_REGULAR_SESSION_OVERLAP:a',r['gaps'])
        self.assertIn('FOLD_NO_REGULAR_SESSION_OVERLAP:b',r['gaps'])

    def test_regular_benchmark_cannot_use_saturday_rows(self):
        rows=[dict(symbol='SPY',timestamp='2026-10-17T13:'+minute+':00Z',open='100',high='101',low='99',close='100',volume=1000) for minute in ('30','35')]
        self.add_source('weekend.json',dict(request={'feed':'sip','timeframe':'5Min'},bars={'SPY':rows}),'bar')
        r=self.run_compile();self.assertEqual(r['status'],'DATA_GAP')
        self.assertTrue(any('REGULAR_5MIN_BAR_OUTSIDE_SESSION' in g for g in r['gaps']))

    def test_raw_intraday_bars_do_not_inherit_daily_split_proof(self):
        self.rh_source()
        row=dict(symbol='SOFI',timestamp='2026-10-15T13:30:00Z',open='15',high='16',low='14',close='15',volume=1000)
        self.add_source('raw-bar.json',dict(request={'feed':'sip','timeframe':'5Min'},bars={'SOFI':[row]}),'bar')
        r=self.run_compile();self.assertTrue(r['daily_adjustment_verified']);self.assertFalse(r['actual_adjustment_verified'])
