"""C3 qualification-sensitivity tests: the raw evidence must be authoritative."""
from copy import deepcopy
from datetime import datetime, timedelta
import sys
from pathlib import Path
from unittest import TestCase

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.c3_qualification import evaluate_report

AT = datetime(2026, 10, 16, 9, 30, tzinfo=__import__('datetime').timezone.utc)


def quote(event_id, symbol, at=AT):
    stamp = at.isoformat()
    return {'id': event_id, 'kind': 'quote', 'at': stamp,
            'synthetic_input': True, 'source_sha256': 'source-' + event_id,
            'data': {'symbol': symbol, 'bid': '40.00', 'ask': '40.01',
                     'bid_at': stamp, 'ask_at': stamp, 'observed_at': stamp}}


def build_fixture():
    histories = {('DEMO' if index == 0 else 'S' + str(index)):
                 [{'symbol': 'X'} for _ in range(64)] for index in range(30)}
    events = [{'id': 'c3-universe-2026-10-16', 'kind': 'universe',
               'at': (AT - timedelta(seconds=1)).isoformat(),
               'synthetic_input': True, 'source_sha256': 'source-universe',
               'data': {'universe_id': 'u', 'histories': histories,
                        'indicators': {}}}]
    for index in range(30):
        symbol = 'DEMO' if index == 0 else 'S' + str(index)
        for round_number in range(1, 13):
            events.append(quote('c3-q-%02d-%s' % (round_number, symbol), symbol))
    events.append({'id': 'c3-bar-flush', 'kind': 'bar', 'at': AT.isoformat(),
                   'synthetic_input': True, 'source_sha256': 'source-bar-1',
                   'data': {'symbol': 'DEMO'}})
    events.append({'id': 'c3-bar-reclaim', 'kind': 'bar',
                   'at': (AT + timedelta(minutes=10)).isoformat(),
                   'synthetic_input': True, 'source_sha256': 'source-bar-2',
                   'data': {'symbol': 'DEMO'}})
    events.append(quote('c3-trigger-quote', 'DEMO',
                        AT + timedelta(minutes=10, seconds=2)))
    events.append({'id': 'c3-stale-control', 'kind': 'data_status',
                   'at': (AT - timedelta(seconds=31)).isoformat(),
                   'synthetic_input': True, 'source_sha256': 'source-stale',
                   'data': {'healthy': True}})
    manifests = {
        'martin-default': {'plugin_id': 'martin',
                           'data_requirements': ['quotes', '5minute-bars',
                                                 'universe-screen']},
        'vwap-default': {'plugin_id': 'vwap-fixture',
                         'data_requirements': ['quotes']},
    }
    tasks = []
    for instance, requirements in (
            ('martin-default', {'quotes', '5minute-bars', 'universe-screen'}),
            ('vwap-default', {'quotes'})):
        for event in events:
            required = {'quote': 'quotes', 'bar': '5minute-bars',
                        'universe': 'universe-screen'}.get(event['kind'])
            if required is not None and required not in requirements:
                continue
            stale = event['id'] == 'c3-stale-control'
            stamp = AT.isoformat()
            event_at = datetime.fromisoformat(event['at'])
            finished = (AT + timedelta(seconds=1) if stale else
                        max(AT + timedelta(seconds=1), event_at + timedelta(seconds=1)))
            tasks.append({'instance_id': instance, 'event_id': event['id'],
                          'state': 'REJECTED' if stale else 'COMMITTED',
                          'queued_at': stamp, 'started_at': None if stale else stamp,
                          'finished_at': finished.isoformat(),
                          'reason': 'STALE_STRATEGY_EVENT' if stale else 'COMMITTED',
                          'event_at': event['at'], 'kind': event['kind'],
                          'source_sha256': event['source_sha256'],
                          'quote': event.get('data') if event['kind'] == 'quote' else None})
    stats = {'martin-default': {'COMMITTED': 364, 'REJECTED': 1},
             'vwap-default': {'COMMITTED': 361, 'REJECTED': 1}}
    rejection = {'martin-default': {'STALE_STRATEGY_EVENT': 1},
                 'vwap-default': {'STALE_STRATEGY_EVENT': 1}}
    proposals = [{'instance_id': 'martin-default', 'proposal_id': 'p',
                  'state': 'PENDING', 'valid_until': '2026-10-16T09:40:17+00:00',
                  'payload': {'generated_at': '2026-10-16T09:40:02+00:00',
                              'valid_until': '2026-10-16T09:40:17+00:00'}}]
    for index in range(30):
        proposals.append({'instance_id': 'vwap-default',
                          'proposal_id': 'p' + str(index), 'state': 'PENDING',
                          'valid_until': '2026-10-16T09:30:20+00:00',
                          'payload': {'generated_at': AT.isoformat(),
                                      'valid_until': (AT + timedelta(seconds=20)).isoformat()}})
    checkpoints = {
        'martin-default': {'universe_complete': True,
                           'machines': {'DEMO': {'state': 'TRIGGERED'}}},
        'vwap-default': {'seen': {'DEMO': 361}},
    }
    call_times = [(AT + timedelta(seconds=5 * index)).isoformat()
                  for index in range(14)]
    report = {
        'clock_mode': 'monotonic', 'clock_started_monotonic': 10.0,
        'clock_ended_monotonic': 70.0, 'elapsed_seconds': 60.0,
        'requested_poll_seconds': 5, 'source_unchanged': True,
        'broker_calls_by_formal': 0, 'formal_io_calls': 0,
        'execution_intents': 0, 'entry_actions': 0,
        'workload': {
            'historical_symbols': 30, 'historical_daily_bars': 1920,
            'quote_rounds': 12, 'quote_events_emitted': 361,
            'bar_events_emitted': 2, 'candidate_expected': 1,
            'expected_committed': {'martin-default': 364, 'vwap-default': 361},
            'stale_control': {'martin-default': 1, 'vwap-default': 1},
            'instances': ['martin-default', 'vwap-default'],
        },
        'stale_control': {'martin-default': 1, 'vwap-default': 1},
        'task_stats': stats, 'rejection_reasons': rejection,
        'latency': {
            'martin-default': {'events': 364, 'max_event_age_seconds': 2.0},
            'vwap-default': {'events': 361, 'max_event_age_seconds': 1.0},
        },
        'checkpoint': {
            'martin-default': {'progress': True},
            'vwap-default': {'progress': True},
        },
        'proposal_counts': {'martin-default': 1, 'vwap-default': 30},
        'raw': {'events': events, 'tasks': tasks, 'proposals': proposals,
                'checkpoints': checkpoints, 'manifests': manifests,
                'market_call_times': call_times},
    }
    return report


class C3QualificationTests(TestCase):
    def test_valid_report_passes(self):
        self.assertEqual(evaluate_report(build_fixture()), [])

    def test_zero_progress_all_queued_fails(self):
        report = build_fixture()
        for task in report['raw']['tasks']:
            if task['event_id'] != 'c3-stale-control':
                task['state'] = 'QUEUED'
                task['started_at'] = None
                task['finished_at'] = None
        report['task_stats'] = {'martin-default': {'QUEUED': 364, 'REJECTED': 1},
                                'vwap-default': {'QUEUED': 361, 'REJECTED': 1}}
        failures = evaluate_report(report)
        self.assertIn('ZERO_COMMITTED:martin-default', failures)
        self.assertIn('UNDRAINED_TAIL:vwap-default', failures)

    def test_frozen_clock_and_inconsistent_elapsed_fail(self):
        report = build_fixture()
        report['clock_mode'] = 'frozen'
        report['clock_ended_monotonic'] = report['clock_started_monotonic'] + 60
        report['elapsed_seconds'] = 0.001
        failures = evaluate_report(report)
        self.assertIn('CLOCK_NOT_MONOTONIC', failures)
        self.assertIn('ELAPSED_INCONSISTENT', failures)

    def test_stuck_cursor_fails(self):
        report = build_fixture()
        report['raw']['checkpoints']['martin-default'] = {
            'universe_complete': True,
            'machines': {'DEMO': {'state': 'WAIT_FLUSH'}}}
        self.assertIn('MARTIN_CHECKPOINT_NOT_TRIGGERED',
                      evaluate_report(report))

    def test_missing_stale_control_fails_even_if_summary_removed(self):
        report = build_fixture()
        report['raw']['events'] = [event for event in report['raw']['events']
                                   if event['id'] != 'c3-stale-control']
        report['raw']['tasks'] = [task for task in report['raw']['tasks']
                                  if task['event_id'] != 'c3-stale-control']
        report['stale_control'] = {}
        report['workload']['stale_control'] = {}
        report['rejection_reasons'] = {}
        report['task_stats'] = {
            'martin-default': {'COMMITTED': 364},
            'vwap-default': {'COMMITTED': 361}}
        failures = evaluate_report(report)
        self.assertIn('STALE_CONTROL_MISSING:martin-default', failures)

    def test_quote_twenty_seconds_old_fails(self):
        report = build_fixture()
        task = next(task for task in report['raw']['tasks']
                    if task['event_id'].startswith('c3-q-'))
        task['finished_at'] = (AT + timedelta(seconds=20)).isoformat()
        failures = evaluate_report(report)
        self.assertIn('QUOTE_AGE_EXCEEDED:martin-default', failures)
        self.assertIn('SUMMARY_LATENCY_MISMATCH:martin-default', failures)

    def test_missing_steady_quote_workload_fails(self):
        report = build_fixture()
        report['raw']['events'] = [event for event in report['raw']['events']
                                   if not event['id'].startswith('c3-q-')]
        failures = evaluate_report(report)
        self.assertIn('STEADY_QUOTE_WORKLOAD_TOO_SMALL', failures)

    def test_unknown_kind_and_nonfinite_time_fail(self):
        report = build_fixture()
        report['raw']['events'][0]['kind'] = 'mystery'
        failures = evaluate_report(report)
        self.assertIn('UNKNOWN_EVENT_KIND', failures)

    def test_missing_raw_evidence_fails(self):
        report = build_fixture()
        report.pop('raw')
        self.assertEqual(evaluate_report(report), ['RAW_EVIDENCE_MISSING'])
