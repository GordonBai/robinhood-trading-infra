from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
import unittest

from martin_agentic.historical_acquisition import acquire, _ns, _stamp


class AcquisitionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.request = {'tool': 'get_stock_bars', 'symbols': ['SOFI'], 'start': '2026-01-01T00:00:00Z',
                        'end': '2026-01-01T00:00:04Z', 'limit': 3, 'feed': 'sip', 'sort': 'asc', 'timeframe': '1Min'}
        self.clock = lambda: datetime(2026, 10, 20, tzinfo=timezone.utc)

    def envelope(self, request, records, exhausted=True):
        family = 'bars' if request['tool'] == 'get_stock_bars' else 'quotes'
        rows = {symbol: [row for row in records if row.get('symbol', 'SOFI') == symbol and _ns(request['start']) <= _ns(row['timestamp']) <= _ns(request['end'])] for symbol in request['symbols']}
        remaining = request['limit']
        for symbol in rows:
            rows[symbol] = rows[symbol][:remaining]
            remaining -= len(rows[symbol])
        result = {'tool': request['tool'], 'request': {k: v for k, v in request.items() if k != 'tool'}, family: rows,
                  'counts': {'records': sum(map(len, rows.values())), 'per_symbol': {s: len(v) for s, v in rows.items()}}}
        if exhausted:
            result['pagination'] = {'exhausted': True, 'next_page_token': None}
        return result

    def run_acquire(self, reader, **kwargs):
        return acquire([self.request], self.temp.name, reader, clock=self.clock, **kwargs)

    def test_saturation_splits_inclusive_overlap_deduplicates_and_preserves_raw(self):
        rows = [{'timestamp': f'2026-01-01T00:00:0{i}Z', 'close': i} for i in range(5)]
        result = self.run_acquire(lambda req: self.envelope(req, rows))
        self.assertTrue(result['complete'])
        self.assertGreater(result['reader_calls'], 1)
        self.assertEqual(result['unique_records'], 5)
        self.assertFalse(result['provider_authenticity_verified'])
        path = Path(result['manifest_path']).parent
        for capture in result['captures']:
            raw = json.loads((path / capture['path']).read_text())
            self.assertEqual(raw['acquired_at'], '2026-10-20T00:00:00+00:00')
        provenance = json.loads((path / result['provenance_path']).read_text())
        self.assertTrue(any(len(row['sources']) > 1 for row in provenance))

    def test_below_cap_never_implies_exhaustion(self):
        result = self.run_acquire(lambda req: self.envelope(req, [], exhausted=False))
        self.assertFalse(result['complete'])
        self.assertTrue(result['request_plan_captured'])
        self.assertEqual(result['gaps'][0]['reason'], 'CONTINUATION_UNVERIFIABLE')

    def test_missing_symbol_and_count_mismatch(self):
        self.request['symbols'].append('AMD')
        def missing(req):
            response = self.envelope(req, [])
            del response['bars']['AMD']
            return response
        result = self.run_acquire(missing)
        self.assertFalse(result['complete'])
        self.assertEqual(result['gaps'][0]['reason'], 'RESPONSE_SYMBOL_COVERAGE_MISSING_OR_EXTRA')
        def bad_count(req):
            response = self.envelope(req, [])
            response['counts']['records'] = 1
            return response
        self.assertEqual(self.run_acquire(bad_count)['gaps'][0]['reason'], 'RESPONSE_COUNTS_MISMATCH')

    def test_budget_stop_then_immutable_resume(self):
        rows = [{'timestamp': f'2026-01-01T00:00:0{i}Z', 'close': i} for i in range(5)]
        reader = lambda req: self.envelope(req, rows)
        first = self.run_acquire(reader, max_requests=1)
        original = Path(first['manifest_path']).read_bytes()
        self.assertFalse(first['complete'])
        self.assertTrue(any(g['reason'] == 'REQUEST_BUDGET_EXHAUSTED' for g in first['gaps']))
        second = self.run_acquire(reader, max_requests=20, resume_manifest=first['manifest_path'])
        self.assertTrue(second['complete'])
        self.assertTrue(second['captures'][0]['reused'])
        self.assertEqual(Path(first['manifest_path']).read_bytes(), original)
        self.assertNotEqual(first['manifest_path'], second['manifest_path'])

    def test_resume_tampering_and_no_blind_completion(self):
        first = self.run_acquire(lambda req: self.envelope(req, [], False))
        capture = Path(first['manifest_path']).parent / first['captures'][0]['path']
        raw = json.loads(capture.read_text()); raw['response']['counts']['records'] = 40
        capture.write_text(json.dumps(raw))
        result = self.run_acquire(lambda req: self.envelope(req, []), resume_manifest=first['manifest_path'])
        self.assertFalse(result['complete'])
        self.assertEqual(result['failures'], ['RESUME_CAPTURE_HASH_MISMATCH'])
        self.assertEqual(result['reader_calls'], 0)

    def test_conflicting_bar_same_event_fails(self):
        rows = [{'timestamp': '2026-01-01T00:00:02Z', 'close': 1}, {'timestamp': '2026-01-01T00:00:02Z', 'close': 2}]
        result = self.run_acquire(lambda req: self.envelope(req, rows))
        self.assertFalse(result['complete'])
        self.assertEqual(result['gaps'][0]['reason'], 'CONFLICTING_RECORDS')

    def test_quotes_same_timestamp_distinct_updates_not_false_conflict(self):
        self.request['tool'] = 'get_stock_quotes'; del self.request['timeframe']; self.request['limit'] = 10
        rows = [{'timestamp': '2026-01-01T00:00:02.000000001Z', 'bid_price': value} for value in (1, 2, 2)]
        result = self.run_acquire(lambda req: self.envelope(req, rows))
        self.assertTrue(result['complete'])
        self.assertEqual(result['unique_records'], 3)
        self.assertTrue(result['limitations'])

    def test_explicit_quote_identity_conflict_fails(self):
        self.request['tool'] = 'get_stock_quotes'; del self.request['timeframe']
        rows = [{'timestamp': '2026-01-01T00:00:02Z', 'id': 9, 'bid_price': value} for value in (1, 2)]
        result = self.run_acquire(lambda req: self.envelope(req, rows))
        self.assertEqual(result['gaps'][0]['reason'], 'CONFLICTING_RECORDS')

    def test_nanosecond_precision_and_depth_stop(self):
        timestamp = '2026-01-01T00:00:02.123456789Z'
        self.assertEqual(_stamp(_ns(timestamp)), timestamp)
        self.request['limit'] = 1
        result = self.run_acquire(lambda req: self.envelope(req, [{'timestamp': timestamp}]), max_depth=0)
        self.assertFalse(result['complete'])
        self.assertEqual(result['gaps'][0]['reason'], 'SATURATED_WINDOW_UNSPLITTABLE')

    def test_failure_registry_redacts_transport_and_invalid_request(self):
        def fail(req):
            raise RuntimeError('secret bearer credential')
        result = self.run_acquire(fail)
        self.assertFalse(result['complete'])
        self.assertNotIn('bearer', Path(result['manifest_path']).read_text())
        self.request['tool'] = 'place_order'
        result = self.run_acquire(fail)
        self.assertEqual(result['reader_calls'], 0)
        self.assertEqual(result['failures'], ['EXPLICIT_READ_TOOL_SIP_ASC_REQUIRED'])
        self.assertEqual(result['requests'], [])

    def test_offline_replay_does_not_invent_original_acquisition_time(self):
        result = self.run_acquire(lambda req: self.envelope(req, []), reader_observation_kind='offline_capture_replay')
        self.assertIsNone(result['captures'][0]['acquired_at'])
        self.assertEqual(result['captures'][0]['observed_at'], '2026-10-20T00:00:00+00:00')
        self.assertEqual(result['captures'][0]['acquired_at_scope'], 'unknown_original_acquisition')
        resumed = self.run_acquire(lambda req: self.fail('must reuse capture'), resume_manifest=result['manifest_path'])
        self.assertTrue(resumed['complete'])
        self.assertIsNone(resumed['captures'][0]['acquired_at'])

    def test_receipt_time_is_after_reader_return_not_request_start(self):
        state = {'now': datetime(2026, 10, 20, tzinfo=timezone.utc)}
        self.clock = lambda: state['now']
        def reader(req):
            state['now'] = datetime(2026, 10, 20, 0, 2, tzinfo=timezone.utc)
            return self.envelope(req, [])
        result = self.run_acquire(reader)
        capture = result['captures'][0]
        self.assertEqual(capture['request_started_at'], '2026-10-20T00:00:00+00:00')
        self.assertEqual(capture['observed_at'], '2026-10-20T00:02:00+00:00')
        self.assertEqual(capture['acquired_at'], capture['observed_at'])
        resumed = self.run_acquire(lambda req: self.fail('must reuse'), resume_manifest=result['manifest_path'])
        self.assertEqual(resumed['captures'][0]['observed_at'], capture['observed_at'])

    def test_any_explicit_pagination_contradiction_prevents_completion(self):
        variants = [('next_page_token', 'more-pages'), ('has_more', True), ('has_next_page', True),
                    ('exhausted', False), ('pagination_exhausted', False), ('truncated', True)]
        for key, value in variants:
            with self.subTest(key=key):
                def reader(req):
                    response = self.envelope(req, [])
                    response[key] = value
                    return response
                result = self.run_acquire(reader)
                self.assertFalse(result['complete'])
                self.assertIn(result['gaps'][0]['reason'], ('PAGINATION_EVIDENCE_CONTRADICTORY', 'RESPONSE_TRUNCATED'))
        def nested(req):
            response = self.envelope(req, [])
            response['pagination']['has_more'] = True
            return response
        self.assertFalse(self.run_acquire(nested)['complete'])

    def test_secret_response_never_written(self):
        def reader(req):
            response = self.envelope(req, [])
            response['access_token'] = 'do-not-persist'
            return response
        result = self.run_acquire(reader)
        self.assertFalse(result['complete'])
        self.assertFalse(result['captures'])
        self.assertNotIn('do-not-persist', Path(result['manifest_path']).read_text())


if __name__ == '__main__':
    unittest.main()
