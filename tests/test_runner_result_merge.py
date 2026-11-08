"""The real ``run_live_session`` must consume a failed step without crashing.

The regular-session tick used to hand ``supervisor.step(...)`` straight into
``dict.update`` alongside its own ``healthy=``, which is a ``TypeError`` the
moment a step reports ``healthy``. These tests run the actual session loop on a
temporary database with synthetic broker and market inputs; no daemon, no
network, no broker.

The health merge is checked as an invariant: a step failure can never be masked
by a healthy base read, and a step success can never relax the base gates.
"""
import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
import tempfile
import threading
from unittest import TestCase
from contextlib import ExitStack
from unittest.mock import MagicMock, patch

from martin_luk.live_session import merge_step_result, run_live_session

from contextlib import contextmanager


@contextmanager
def _KeepDirectory(directory):
    """Reuse a caller-supplied directory so two sessions share one database."""
    yield directory


SYMBOLS = [str(index) for index in range(10)]
NOW = datetime.fromisoformat('2026-10-16T14:00:00+00:00')


def snapshot(*, executions=None, base=None):
    at = (base or NOW).isoformat()
    # ``run_live_session`` builds the book without simulation, so the snapshot
    # must be a live-shaped one for the book to accept it.
    value = {'schema_version': 1, 'provider': 'robinhood', 'synthetic': False,
             'account_key': 'acct-key', 'account_mask': '••••0000',
             'started_at': at, 'observed_at': at,
             'positions': {}, 'orders': {}, 'groups': [],
             'other_asset_open_orders': 0,
             'other_asset_values': {key: '0' for key in
                                    ('options_value', 'crypto_value', 'futures_value',
                                     'event_contracts_value', 'fixed_income_value',
                                     'mutual_funds_value')},
             'portfolio': {'cash': '500', 'total_value': '500', 'pending_deposits': '0'},
             'buying_power': '500', 'unleveraged_buying_power': '500',
             'human_must_approve_trades': True, 'pending_approvals': 0,
             'pending_approval_ids': [], 'approval_states': {},
             'requested_approval_ids': [], 'missing_approval_ids': [],
             'collection_complete': True}
    if executions:
        value['orders'] = {item['id']: item for item in executions}
    return value


def broker_order(*, order_id='ord-a', ref='ref-a', execution_id='same-exec',
                 price='40', minutes=0, base=None):
    origin = base or NOW
    at = (origin + timedelta(minutes=minutes)).isoformat()
    return {'id': order_id, 'ref_id': ref, 'symbol': 'DEMO', 'side': 'buy',
            'quantity': '1', 'cumulative_quantity': '1', 'type': 'market',
            'trigger': 'immediate', 'stop_price': None,
            'created_at': origin.isoformat(), 'state': 'filled',
            'market_hours': 'regular_hours', 'time_in_force': 'gfd',
            'executions': [{'id': execution_id, 'order_id': order_id,
                            'symbol': 'DEMO', 'side': 'buy', 'quantity': '1',
                            'price': price, 'fees': '0', 'executed_at': at}]}


class MergeInvariantTests(TestCase):
    def test_health_is_an_and_and_attention_is_an_or(self):
        result = {'healthy': True, 'attention_required': False, 'reason': 'BASE'}
        merge_step_result(result, {'healthy': False, 'attention_required': True,
                                   'reconciled': False, 'reason': 'STEP'})
        self.assertFalse(result['healthy'])
        self.assertTrue(result['attention_required'])
        self.assertFalse(result['reconciled'])
        self.assertEqual(result['reason'], 'STEP')

    def test_a_healthy_step_does_not_relax_a_closed_base(self):
        result = {'healthy': False, 'attention_required': True}
        merge_step_result(result, {'healthy': True, 'attention_required': False})
        self.assertFalse(result['healthy'], 'the base gate must survive')
        self.assertTrue(result['attention_required'])


class SessionLoopTests(TestCase):
    def run_session(self, snapshots, *, ticks=2, now=None, extra_patches=(),
                    directory=None):
        policy = json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        policy['capture_only'] = True
        at = now or NOW
        reads = list(snapshots)
        calls = {'market': 0}

        def broker_read(*args, **kwargs):
            return reads.pop(0) if reads else snapshots[-1]

        def collect(*args, **kwargs):
            calls['market'] += 1
            return [], {'quotes': 0}

        with (tempfile.TemporaryDirectory() if directory is None
              else _KeepDirectory(directory)) as directory:
            journal = MagicMock()
            journal.summary.return_value = {'read_healthy': True,
                                            'reconciliation_issues': [],
                                            'positions': {}, 'open_equity_orders': 0,
                                            'pending_approvals': 0}
            target = 'martin_agentic.live_session.'
            with patch(target + 'single_instance', MagicMock()), \
                 patch(target + 'BrokerJournal', lambda *a, **kw: journal), \
                 patch(target + 'collect_live_events', collect), \
                 patch(target + '_broker_read', broker_read), \
                 patch(target + 'notify_local', lambda *a, **k: None), \
                 patch(target + 'print', lambda *a, **k: None), \
                 ExitStack() as stack:
                for item in extra_patches:
                    stack.enter_context(item)
                result = run_live_session(
                    Path(directory) / 'book.sqlite', policy, symbols=SYMBOLS,
                    cycles=ticks, poll_seconds=5, agent_mode='disabled',
                    status_file=Path(directory) / 'status.json',
                    output_dir=directory, clock=lambda: at,
                    budget_entry_limit=0, budget_total_limit=0)
            status = json.loads((Path(directory) / 'status.json').read_text())
        return result, status

    def test_a_clean_session_completes_without_a_merge_error(self):
        result, status = self.run_session([snapshot()], ticks=2)
        self.assertEqual(result['cycle'], 2)
        self.assertNotIn('needs_reconciliation', result)
        self.assertEqual(status['cycle'], 2)

    def test_an_execution_conflict_is_reported_and_never_crashes_the_loop(self):
        # A conflict needs the risk ledger to see one execution twice.
        first = snapshot(executions=[broker_order(price='40')])
        second = snapshot(executions=[broker_order(order_id='ord-b', ref='ref-b',
                                                   price='41', minutes=1)])
        result, status = self.run_session([first, second], ticks=2)
        self.assertEqual(result['cycle'], 2, 'the loop must survive the failed tick')
        for payload in (result, status):
            self.assertFalse(payload['healthy'])
            self.assertTrue(payload['attention_required'])
        self.assertTrue(result.get('needs_reconciliation'))
        self.assertEqual(result.get('cycle'), 2)
        self.assertEqual(result.get('reconciliation_reason'),
                         'LIVE_RISK_EXECUTION_CONFLICT')
        self.assertEqual(result.get('reason'),
                         'EXECUTION_RECONCILIATION_REQUIRED_ENTRIES_BLOCKED')
        self.assertFalse(result.get('capture_healthy'),
                         'capture health is the conjunction, not the observation alone')
        self.assertEqual(result.get('settlement_blocked'),
                         'RECEIPT_IMPORT_INCOMPLETE')
        self.assertFalse(result.get('flat_claimed'))
        self.assertTrue(result.get('new_entries_blocked'))
        self.assertIn('management_state', result)
        self.assertIn('protection_status', result)
        self.assertFalse(result.get('management_continues'))

    def conflict_pair(self):
        first = snapshot(executions=[broker_order(price='40')])
        second = snapshot(executions=[broker_order(order_id='ord-b', ref='ref-b',
                                                   price='41', minutes=1)])
        return first, second

    def test_an_audit_failure_is_reported_and_the_loop_continues(self):
        first, second = self.conflict_pair()
        from martin_luk.live_session import LiveSupervisor
        failure = patch.object(LiveSupervisor, '_record_risk_conflict',
                               side_effect=sqlite3.OperationalError('injected audit'))
        result, status = self.run_session([first, second], ticks=2,
                                          extra_patches=[failure])
        self.assertEqual(result['cycle'], 2)
        self.assertFalse(result['healthy'])
        self.assertTrue(result['attention_required'])
        self.assertIn('RECEIPT_CONFLICT_AUDIT_FAILED',
                      [item.get('reason') for item in result.get('errors') or []])
        self.assertTrue(status.get('conflict_audit_failed'))
        self.assertEqual(status.get('settlement_blocked'),
                         'RECEIPT_IMPORT_INCOMPLETE')

    def test_a_risk_storage_failure_is_structured_and_never_crashes(self):
        from martin_agentic.live_risk import LiveRiskGuard
        failure = patch.object(LiveRiskGuard, 'observe',
                               side_effect=sqlite3.OperationalError('injected risk'))
        result, status = self.run_session([snapshot(executions=[broker_order()])],
                                          ticks=2, extra_patches=[failure])
        self.assertEqual(result['cycle'], 2)
        self.assertFalse(result['healthy'])
        self.assertEqual(result.get('reconciliation_reason'),
                         'RECONCILIATION_STORAGE_FAILED')
        self.assertIn('RECONCILIATION_STORAGE_FAILED',
                      [item.get('reason') for item in result.get('errors') or []])
        self.assertTrue(status['attention_required'])

    def test_a_preopen_conflict_does_not_report_capture_as_healthy(self):
        # 13:00 UTC is 09:00 New York: before the regular session opens. The
        # snapshots are stamped at that time, or the book rejects them as
        # future observations.
        preopen = datetime.fromisoformat('2026-10-16T13:00:00+00:00')
        first = snapshot(base=preopen, executions=[broker_order(base=preopen, price='40')])
        second = snapshot(base=preopen, executions=[broker_order(
            base=preopen, order_id='ord-b', ref='ref-b', price='41', minutes=1)])
        result, status = self.run_session([first, second], ticks=2, now=preopen)
        self.assertFalse(result['healthy'])
        self.assertFalse(result['read_only_capture_healthy'],
                         'a reconciliation failure is not capture-healthy')
        self.assertTrue(result['capture_observation_healthy'])
        self.assertFalse(result['capture_healthy'])
        self.assertEqual(result['reason'],
                         'EXECUTION_RECONCILIATION_REQUIRED_DURING_PREOPEN')
        self.assertFalse(status['healthy'])

    def test_a_failed_tick_keeps_the_gate_closed_on_the_next_tick(self):
        first, second = self.conflict_pair()
        result, status = self.run_session([first, second, snapshot()], ticks=3)
        self.assertEqual(result['cycle'], 3)
        # The durable gate opened by the first failure stays closed on the
        # later ticks even though those snapshots themselves reconcile.
        self.assertTrue(result.get('reconciliation_pending'))
        self.assertFalse(result['healthy'])
        self.assertTrue(result['new_entries_blocked'])
        self.assertEqual(status.get('settlement_blocked'),
                         'RECEIPT_IMPORT_INCOMPLETE')

    def test_the_conflict_gate_is_durable_across_restarts_without_an_importer(self):
        first = snapshot(executions=[broker_order(price='40')])
        second = snapshot(executions=[broker_order(order_id='ord-b', ref='ref-b',
                                                   price='41', minutes=1)])
        with tempfile.TemporaryDirectory() as shared:
            session_one, status_one = self.run_session([first, second], ticks=2,
                                                       directory=shared)
            self.assertFalse(session_one['healthy'])
            self.assertTrue(session_one.get('needs_reconciliation'))
            self.assertEqual(status_one.get('settlement_blocked'),
                             'RECEIPT_IMPORT_INCOMPLETE')

            # A brand new session on the same database, with a completely clean
            # snapshot and no importer in the process at all.
            session_two, status_two = self.run_session([snapshot()], ticks=2,
                                                       directory=shared)
            self.assertFalse(session_two['healthy'],
                             'a clean snapshot must not clear a durable fault')
            self.assertTrue(session_two.get('reconciliation_pending'))
            self.assertTrue(session_two.get('new_entries_blocked'))
            self.assertEqual(session_two.get('settlement_blocked'),
                             'RECEIPT_IMPORT_INCOMPLETE')
            self.assertEqual(status_two.get('settlement_blocked'),
                             'RECEIPT_IMPORT_INCOMPLETE')
            self.assertTrue(status_two['attention_required'])
            self.assertFalse(status_two.get('read_only_capture_healthy'))

            reopened = sqlite3.connect(Path(shared) / 'book.sqlite')
            self.addCleanup(reopened.close)
            rows = reopened.execute(
                'SELECT source,state,execution_id FROM execution_integrity_faults'
            ).fetchall()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0][1], 'UNRESOLVED')

    def test_a_fault_ledger_api_failure_falls_back_to_a_durable_row(self):
        first, second = self.conflict_pair()
        from trading_infra.integrity_faults import ExecutionIntegrityFaults
        broken = patch.object(ExecutionIntegrityFaults, 'record',
                              side_effect=sqlite3.OperationalError('injected ledger'))
        with tempfile.TemporaryDirectory() as shared:
            session_one, _ = self.run_session([first, second], ticks=2,
                                              directory=shared, extra_patches=[broken])
            self.assertEqual(session_one['cycle'], 2, 'the loop must survive')
            self.assertFalse(session_one['healthy'])
            self.assertTrue(session_one.get('new_entries_blocked'))
            # The convenience wrapper failed, but the conflict is still written
            # with the fewest dependencies and stays authoritative.
            self.assertFalse(session_one.get('integrity_gate_unavailable'))
            reopened = sqlite3.connect(Path(shared) / 'book.sqlite')
            self.addCleanup(reopened.close)
            rows = reopened.execute(
                'SELECT source,state FROM execution_integrity_faults').fetchall()
            self.assertEqual(rows, [('execution_identity_conflict', 'UNRESOLVED')])

            # A brand-new session with a fully clean snapshot cannot wash it.
            session_two, status_two = self.run_session([snapshot()], ticks=2,
                                                       directory=shared)
            self.assertFalse(session_two['healthy'])
            self.assertTrue(session_two.get('reconciliation_pending'))
            self.assertTrue(session_two.get('new_entries_blocked'))
            self.assertFalse(session_two.get('read_only_capture_healthy'))
            self.assertFalse(status_two['healthy'])

    def test_an_unwritten_fault_sidecar_keeps_the_next_session_isolated(self):
        first, second = self.conflict_pair()
        from trading_infra.integrity_faults import ExecutionIntegrityFaults
        broken = patch.object(ExecutionIntegrityFaults, 'fault_id',
                              side_effect=sqlite3.OperationalError('injected ledger'))
        with tempfile.TemporaryDirectory() as shared:
            session_one, _ = self.run_session([first, second], ticks=2,
                                              directory=shared,
                                              extra_patches=[broken])
            self.assertTrue(session_one.get('integrity_gate_unavailable'))
            self.assertIn('MANUAL_HANDOFF_REQUIRED',
                          session_one.get('stop_disposition', ''))
            sidecar = json.loads(
                (Path(shared).resolve() / 'book.sqlite.entry-inhibit.json').read_text())
            self.assertIn('EXECUTION_INTEGRITY_FAULT_UNRECORDED',
                          sidecar['issues'])

            # Injection removed and the ledger writable again: a brand-new
            # session with a clean snapshot must still read the side-car as an
            # integrity fault, not as a healthy account with an entry hold.
            later = NOW + timedelta(hours=1)
            session_two, status_two = self.run_session(
                [snapshot(base=later)], ticks=2, directory=shared, now=later)
            for payload in (session_two, status_two):
                self.assertTrue(payload['attention_required'])
                self.assertFalse(payload['healthy'])
            self.assertFalse(session_two['read_only_capture_healthy'])
            self.assertFalse(session_two.get('capture_healthy'))
            self.assertTrue(session_two.get('reconciliation_pending'))
            self.assertTrue(session_two.get('new_entries_blocked'))
            self.assertEqual(session_two.get('settlement_blocked'),
                             'RECEIPT_IMPORT_INCOMPLETE')
            self.assertIn('EXECUTION_INTEGRITY_SIDECAR_UNRECORDED_FAULT',
                          session_two.get('integrity_reasons') or [])
            self.assertEqual(session_two.get('integrity_observed_at'),
                             sidecar['at'],
                             'the original detection time is preserved')
            self.assertEqual(session_two.get('integrity_faults_unresolved'), 0,
                             'no database row was invented')
            self.assertTrue(session_two['entry_inhibit']['blocked'])

    def test_an_unwritable_fault_is_a_fatal_handoff_not_a_memory_flag(self):
        first, second = self.conflict_pair()
        from trading_infra.integrity_faults import ExecutionIntegrityFaults
        # Both the typed writer and the fallback derive the fault id, so this
        # makes every durable write impossible without touching the database.
        broken = patch.object(ExecutionIntegrityFaults, 'fault_id',
                              side_effect=sqlite3.OperationalError('injected ledger'))
        with tempfile.TemporaryDirectory() as shared:
            result, status = self.run_session([first, second, snapshot()], ticks=3,
                                              directory=shared, extra_patches=[broken])
            self.assertEqual(result['cycle'], 2,
                             'the failure tick is reported, then the loop hands off')
            self.assertFalse(result['healthy'])
            self.assertTrue(result['attention_required'])
            self.assertTrue(result.get('new_entries_blocked'))
            self.assertTrue(result.get('integrity_gate_unavailable'))
            self.assertIn('MANUAL_HANDOFF_REQUIRED', result.get('stop_disposition', ''))
            self.assertTrue(status.get('integrity_gate_unavailable'))
            self.assertTrue(status.get('attention_required'))
            # A sticky inhibit is persisted outside the broken ledger.
            inhibit = json.loads(
                (Path(shared).resolve() / 'book.sqlite.entry-inhibit.json').read_text())
            self.assertTrue(inhibit['blocked'])
            self.assertIn('EXECUTION_INTEGRITY_FAULT_UNRECORDED', inhibit['issues'])
