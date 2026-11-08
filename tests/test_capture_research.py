import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from martin_agentic.audit import canonical, digest
from martin_agentic.capture_research import compile_capture


class CaptureResearchTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.archive = self.root / 'archive'
        self.archive.mkdir()
        self.output = self.root / 'compiled'
        self.event = {'id': 'q1', 'at': '2026-10-20T14:00:00Z', 'kind': 'quote',
                      'source_sha256': 'a' * 64, 'synthetic_input': False,
                      'data': {'symbol': 'SOFI', 'bid': '14', 'ask': '14.01',
                               'observed_at': '2026-10-20T14:00:00Z'}}

    def freeze(self, events, decisions=None):
        rows = [{'capture_sequence': n, 'id': event['id'], 'payload': event,
                 'fingerprint': digest(event), 'consumed_at': '2026-10-20T14:01:00Z'}
                for n, event in enumerate(events, 1)]
        raw = ''.join(canonical(row) + '\n' for row in rows).encode()
        (self.archive / 'events.ndjson').write_bytes(raw)
        manifest = {'version': 2, 'scope': 'local_sqlite_capture_snapshot',
                    'event_count': len(rows), 'files': {'events.ndjson': {
                        'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw),
                        'records': len(rows)}}}
        decisions = decisions if decisions is not None else [
            {'event_id': row['id'], 'result_id': 'consumed',
             'context': {'status': 'CONSUMED'}, 'recorded_at': row['consumed_at']}
            for row in rows]
        records = [dict(row, decision_sequence=n, fingerprint=digest({
            'event_id': row['event_id'], 'result_id': row['result_id'], 'context': row['context']}))
            for n, row in enumerate(decisions, 1)]
        encoded = ''.join(canonical(row) + '\n' for row in records).encode()
        (self.archive / 'decisions.ndjson').write_bytes(encoded)
        manifest['decision_count'] = len(records)
        manifest['files']['decisions.ndjson'] = {'sha256': hashlib.sha256(encoded).hexdigest(),
                                                'bytes': len(encoded), 'records': len(records)}
        (self.archive / 'manifest.json').write_text(json.dumps(manifest))

    def decision(self, event_id, status, *, result_id='consumed', at='2026-10-20T14:01:00Z'):
        return {'event_id': event_id, 'result_id': result_id,
                'context': {'status': status, 'reason': 'SESSION_STOPPED_BEFORE_CONSUMPTION'},
                'recorded_at': at}

    def test_unconsumed_quote_is_excluded_without_rewriting_original_archive(self):
        abandoned = dict(self.event, id='abandoned')
        self.freeze([self.event, abandoned], [self.decision('q1', 'CONSUMED'),
            self.decision('abandoned', 'NOT_CONSUMED', result_id='admission')])
        before = {p.name: p.read_bytes() for p in self.archive.iterdir()}
        report = compile_capture(self.archive, self.output)
        events = json.loads((self.output / 'events.json').read_text())
        self.assertEqual([event['id'] for event in events], ['q1'])
        self.assertEqual(events[0]['archive_consumption_status'], 'CONSUMED')
        self.assertEqual(report['excluded_not_consumed_events'], 1)
        self.assertEqual(report['not_consumed_reasons'], {'SESSION_STOPPED_BEFORE_CONSUMPTION': 1})
        self.assertEqual(before, {p.name: p.read_bytes() for p in self.archive.iterdir()})
        self.assertEqual(len(list(self.output.iterdir())), 4)
        self.assertFalse(list(self.root.glob('.capture-research-*')))

    def test_only_unconsumed_events_produce_no_replay_input_and_no_admission(self):
        self.freeze([self.event], [self.decision('q1', 'NOT_CONSUMED', result_id='admission')])
        report = compile_capture(self.archive, self.output)
        self.assertEqual(json.loads((self.output / 'events.json').read_text()), [])
        self.assertEqual(report['events'], 0)
        self.assertEqual(report['status'], 'RESEARCH_NOT_ADMITTED')
        self.assertFalse(report['strategy_validated'])

    def test_decision_file_tampering_never_publishes_a_dataset(self):
        self.freeze([self.event])
        with (self.archive / 'decisions.ndjson').open('ab') as stream:
            stream.write(b' ')
        with self.assertRaises(ValueError):
            compile_capture(self.archive, self.output)
        self.assertFalse(self.output.exists())

    def test_decision_fingerprint_checked_even_when_file_manifest_matches(self):
        self.freeze([self.event])
        path = self.archive / 'decisions.ndjson'
        row = json.loads(path.read_text()); row['context']['status'] = 'NOT_CONSUMED'
        raw = (canonical(row) + '\n').encode(); path.write_bytes(raw)
        p = self.archive / 'manifest.json'; manifest = json.loads(p.read_text())
        manifest['files']['decisions.ndjson'].update(sha256=hashlib.sha256(raw).hexdigest(), bytes=len(raw))
        p.write_text(json.dumps(manifest))
        with self.assertRaisesRegex(ValueError, 'CAPTURE_DECISION_INTEGRITY_FAILURE'):
            compile_capture(self.archive, self.output)
        self.assertFalse(self.output.exists())

    def test_contradictory_consumption_decisions_are_not_guessed(self):
        self.freeze([self.event], [self.decision('q1', 'CONSUMED'),
            self.decision('q1', 'NOT_CONSUMED', result_id='admission')])
        with self.assertRaisesRegex(ValueError, 'CAPTURE_CONSUMPTION_DISPOSITION_CONFLICT'):
            compile_capture(self.archive, self.output)
        self.assertFalse(self.output.exists())

    def test_missing_consumption_decision_is_not_assumed_consumed(self):
        self.freeze([self.event], [])
        with self.assertRaisesRegex(ValueError, 'CAPTURE_CONSUMPTION_DECISION_REQUIRED'):
            compile_capture(self.archive, self.output)
        self.assertFalse(self.output.exists())

    def test_decision_for_missing_event_is_rejected(self):
        self.freeze([self.event], [self.decision('q1', 'CONSUMED'), self.decision('absent', 'CONSUMED')])
        with self.assertRaisesRegex(ValueError, 'CAPTURE_DECISION_EVENT_MISSING'):
            compile_capture(self.archive, self.output)
        self.assertFalse(self.output.exists())

    def test_original_consumption_time_must_match_the_verified_decision(self):
        self.freeze([self.event], [self.decision('q1', 'CONSUMED', at='2026-10-20T14:02:00Z')])
        with self.assertRaisesRegex(ValueError, 'CAPTURE_CONSUMPTION_TIME_MISMATCH'):
            compile_capture(self.archive, self.output)
        self.assertFalse(self.output.exists())

    def test_source_and_dataset_binding_preserves_times_and_raw_source(self):
        self.freeze([self.event])
        before = (self.archive / 'events.ndjson').read_bytes()
        report = compile_capture(self.archive, self.output)
        source = json.loads((self.output / report['source']['path']).read_text())
        events = json.loads((self.output / report['dataset']['path']).read_text())
        self.assertEqual(events[0]['captured_event_at'], self.event['at'])
        self.assertEqual(events[0]['at'], '2026-10-20T14:01:00Z')
        self.assertEqual(events[0]['data'], self.event['data'])
        self.assertEqual(events[0]['raw_source_sha256'], self.event['source_sha256'])
        self.assertEqual(events[0]['source_sha256'], report['source']['sha256'])
        self.assertEqual({k: v for k, v in events[0].items()
                          if k not in ('source_sha256', 'synthetic_input')}, source['events'][0])
        self.assertFalse(source['provenance']['actual_point_in_time_verified'])
        self.assertEqual(report['status'], 'RESEARCH_NOT_ADMITTED')
        self.assertIn('POINT_IN_TIME_UNIVERSE_MISSING', report['gaps'])
        self.assertEqual(before, (self.archive / 'events.ndjson').read_bytes())

    def test_tampered_file_never_published(self):
        self.freeze([self.event])
        with (self.archive / 'events.ndjson').open('ab') as stream:
            stream.write(b'\n')
        with self.assertRaises(ValueError):
            compile_capture(self.archive, self.output)
        self.assertFalse(self.output.exists())

    def test_event_fingerprint_checked_even_when_file_manifest_matches(self):
        self.freeze([self.event])
        path = self.archive / 'events.ndjson'
        row = json.loads(path.read_text())
        row['payload']['data']['bid'] = '999'
        raw = (canonical(row) + '\n').encode()
        path.write_bytes(raw)
        manifest_path = self.archive / 'manifest.json'
        manifest = json.loads(manifest_path.read_text())
        manifest['files']['events.ndjson'].update(sha256=hashlib.sha256(raw).hexdigest(), bytes=len(raw))
        manifest_path.write_text(json.dumps(manifest))
        with self.assertRaisesRegex(ValueError, 'CAPTURE_EVENT_INTEGRITY_FAILURE'):
            compile_capture(self.archive, self.output)
        self.assertFalse(self.output.exists())

    def test_synthetic_input_rejected(self):
        self.freeze([dict(self.event, synthetic_input=True)])
        with self.assertRaisesRegex(ValueError, 'REAL_CAPTURE_PROVENANCE_REQUIRED'):
            compile_capture(self.archive, self.output)

    def test_delayed_market_events_preserve_actual_consumption_order(self):
        earlier = dict(self.event, id='q2', at='2026-10-20T13:59:59Z')
        self.freeze([self.event, earlier])
        compile_capture(self.archive, self.output)
        events = json.loads((self.output / 'events.json').read_text())
        self.assertEqual([e['id'] for e in events], ['q1', 'q2'])
        self.assertEqual(events[1]['captured_event_at'], earlier['at'])
        self.assertEqual(events[0]['at'], events[1]['at'])

    def test_future_event_before_actual_consumption_rejected(self):
        self.freeze([dict(self.event, at='2026-10-20T14:02:00Z')])
        with self.assertRaisesRegex(ValueError, 'CONSUMPTION_BEFORE_EVENT'):
            compile_capture(self.archive, self.output)

    def test_diagnostic_excluded_with_explicit_count(self):
        diagnostic = dict(self.event, id='diagnostic', source_type='local_control_diagnostic',
                          kind='capture_rejection')
        self.freeze([self.event, diagnostic])
        report = compile_capture(self.archive, self.output)
        self.assertEqual(report['events'], 1)
        self.assertEqual(report['excluded_control_diagnostics'], 1)

    def test_existing_output_is_not_replaced(self):
        self.freeze([self.event])
        self.output.mkdir()
        marker = self.output / 'keep'
        marker.write_text('original')
        with self.assertRaises(FileExistsError):
            compile_capture(self.archive, self.output)
        self.assertEqual(marker.read_text(), 'original')


if __name__ == '__main__':
    unittest.main()
