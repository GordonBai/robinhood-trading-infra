"""C4 checker sensitivity: no-op reconciliation and deadline controls fail."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import sys
from pathlib import Path
from unittest import TestCase

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.c4_qualification import (evaluate_c4, evaluate_noop_control,  # noqa: E402
                                       evaluate_timeout_control)


def normal_report():
    fault_records = [
        {'kind': 'start', 'fault': True, 'pid': 101,
         'started_at': 10.0, 'session_at': '2026-10-16T14:00:30+00:00'},
        {'kind': 'exception', 'fault': True, 'pid': 101,
         'error': 'STRATEGY_PROCESS_TIMEOUT'},
        {'kind': 'cleanup', 'fault': True, 'pid': 101, 'reaped': True},
        {'kind': 'start', 'fault': True, 'pid': 102,
         'started_at': 15.0, 'session_at': '2026-10-16T14:00:35+00:00'},
        {'kind': 'exception', 'fault': True, 'pid': 102,
         'error': 'STRATEGY_PROCESS_CRASHED'},
        {'kind': 'cleanup', 'fault': True, 'pid': 102, 'reaped': True,
         'returncode': 7},
    ]
    tasks = [
        {'instance_id': 'vwap-live', 'state': 'COMMITTED',
         'finished_at': '2026-10-16T14:00:20+00:00'},
        {'instance_id': 'vwap-live', 'state': 'COMMITTED',
         'finished_at': '2026-10-16T14:00:32+00:00'},
        {'instance_id': 'vwap-live', 'state': 'COMMITTED',
         'finished_at': '2026-10-16T14:00:40+00:00'},
        {'instance_id': 'vwap-live', 'state': 'COMMITTED',
         'finished_at': '2026-10-16T14:01:30+00:00'},
        {'instance_id': 'fault-slow', 'state': 'REJECTED',
         'reason': 'STRATEGY_PROCESS_TIMEOUT'},
        {'instance_id': 'fault-slow', 'state': 'REJECTED',
         'reason': 'STRATEGY_PROCESS_CRASHED'},
    ]
    samples = [{'at': (datetime(2026, 10, 16, 14, 0, tzinfo=timezone.utc)
                       + timedelta(seconds=5 * index)).isoformat()}
               for index in range(25)]
    return {
        'clock_mode': 'monotonic', 'elapsed_seconds': 187.0,
        'run_started_monotonic': 10.0, 'run_ended_monotonic': 197.0,
        'source_fingerprint_before': 'a' * 64,
        'source_fingerprint_after': 'a' * 64,
        'source_unchanged': True, 'broker_calls_by_formal': 0,
        'formal_io_calls': 0, 'execution_intents': 1,
        'executions_detail': [{'payload': {'side': 'buy'}},
                              {'payload': {'side': 'sell'}}],
        'plugins': ['vwap-fixture', 'slow-exit-fixture'],
        'fault_processes': fault_records, 'tasks': tasks,
        'feedback': [{'instance_id': 'vwap-live', 'kind': 'protection_covered',
                      'count': 1}],
        'trace': [{'e': {'phase': 'PROTECTED',
                         'first_protection_observed_at': '2026-10-16T14:00:25+00:00',
                         'last_full_coverage_observed_at': '2026-10-16T14:02:16+00:00'}},
                  {'e': {'phase': 'FLAT_VERIFIED'}}],
        'actions': [{'kind': 'entry', 'state': 'OBSERVED'},
                    {'kind': 'protect', 'state': 'OBSERVED'},
                    {'kind': 'cancel', 'state': 'OBSERVED'},
                    {'kind': 'exit', 'state': 'OBSERVED'}],
        'broker_samples': samples,
        'book_state': {'trades': {'e': {'phase': 'FLAT_VERIFIED', 'quantity': '0'}}},
    }


class C4QualificationTests(TestCase):
    def test_normal_report_passes(self):
        self.assertEqual(evaluate_c4(normal_report()), [])

    def test_noop_reconcile_report_fails(self):
        report = normal_report()
        report['noop_reconcile'] = True
        report['trace'] = [{'e': {'phase': 'ENTRY_PLANNED'}}]
        failures = evaluate_c4(report)
        self.assertIn('PROTECTED_PHASE_MISSING', failures)
        self.assertEqual(evaluate_noop_control(report), [])

    def test_protection_timeout_control_passes_only_with_issue(self):
        report = {
            'scenario': 'protection_timeout', 'elapsed_seconds': 55.0,
            'feedback': [], 'book_state': {'issues': ['PROTECTION_WAIT_EXCEEDED:DEMO']},
            'trace': [{'e': {'phase': 'EXIT_REQUESTED'}}],
        }
        self.assertEqual(evaluate_timeout_control(report), [])
        report['book_state']['issues'] = []
        self.assertIn('PROTECTION_DEADLINE_ISSUE_MISSING',
                      evaluate_timeout_control(report))
        report['trace'] = [{'e': {'phase': 'PROTECTED'}}]
        self.assertIn('UNCONFIRMED_PROTECTION_MARKED_PROTECTED',
                      evaluate_timeout_control(report))
