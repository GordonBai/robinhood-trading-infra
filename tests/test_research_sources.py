import json
from pathlib import Path
import tempfile
from unittest import TestCase
from martin_luk.research_sources import inspect_provider_capture

class ProviderCoverageTests(TestCase):
    def capture(self,**changes):
        d={'tool':'get_stock_quotes','request':{'symbols':['DEMO'],'start':'2026-10-16T13:30:00Z',
            'end':'2026-10-16T13:31:00Z','limit':1,'feed':'sip'},'counts':{'records':1,'per_symbol':{'DEMO':1}},
            'quotes':{'DEMO':[{'symbol':'DEMO','timestamp':'2026-10-16T13:30:01Z',
                'bid_price':10,'ask_price':10.01,'bid_size':100,'ask_size':100}]}}
        d.update(changes);return d
    def inspect(self,data):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'raw.json';p.write_text(json.dumps(data))
            return inspect_provider_capture(p,acquired_at='2026-10-20T00:50:00Z')
    def test_limit_hit_never_claims_full_minute_or_point_in_time(self):
        r=self.inspect(self.capture())
        self.assertIn('POSSIBLE_LIMIT_TRUNCATION',r['gaps'])
        self.assertTrue(r['data_access_proven']);self.assertFalse(r['ready_for_strategy_research'])
        self.assertNotEqual(r['acquisition_upper_bound_at'],r['symbols']['DEMO']['last_market_at'])
    def test_crossed_quotes_and_count_mismatch_are_visible(self):
        d=self.capture();d['quotes']['DEMO'][0]['bid_price']=11;d['counts']['records']=2
        r=self.inspect(d)
        self.assertIn('INVALID_MARKET_RECORDS:DEMO',r['gaps']);self.assertIn('PROVIDER_COUNTS_MISMATCH',r['gaps'])
    def test_single_venue_and_short_response_do_not_imply_complete_nbbo(self):
        d=self.capture();d['request'].update(feed='iex',limit=1000)
        r=self.inspect(d)
        self.assertIn('NOT_CONSOLIDATED_SIP_QUOTES',r['gaps'])
        self.assertIn('PROVIDER_PAGINATION_COMPLETENESS_NOT_PROVEN',r['gaps'])
        self.assertFalse(r['provider_capture_complete'])

    def test_provider_inclusive_end_record_is_not_a_false_data_error(self):
        d=self.capture();d['quotes']['DEMO'][0]['timestamp']=d['request']['end']
        r=self.inspect(d)
        self.assertEqual(r['symbols']['DEMO']['invalid_records'],0)
        self.assertNotIn('INVALID_MARKET_RECORDS:DEMO',r['gaps'])
        self.assertFalse(r['provider_capture_complete'])
