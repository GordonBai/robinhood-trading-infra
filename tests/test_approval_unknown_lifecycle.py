"""Approval waits, UNKNOWN outcomes and exit retry through the default loop.

Same composed path as ``test_partial_lifecycle``: strategy instance ->
LiveSupervisor -> InstanceSupervisor -> AdmissionService -> ExecutionBook ->
reporting dispatcher -> synthetic broker. The test only submits strategy events,
claims/reports dispatcher actions and answers with broker snapshots; it never
calls ``book.propose``, ``record_fill`` or ``admission.settle`` to complete a
scenario.

Scenario matrix (see plan/iterations/003r5/IMPLEMENTATION.md):

* A1 pending approval is not a fill; a later real execution is
* A2 approval declined / expired ends the trade; an empty approval list does not
* A3 UNKNOWN action keeps its identity across a new book and is only reconciled
* A4 a definitive exit rejection gets a new auditable attempt, then fills flat
* A5 an UNKNOWN exit is reconciled, never re-sent
* A6 stop fill during a pending cancel exits only the remaining shares, to flat
"""
import json
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from unittest import TestCase

from entry_authority_fixture import TestExecutionBook as ExecutionBook
from martin_luk.live_session import LiveSupervisor
from test_broker import FakeBroker, position
from test_broker import snapshot as broker_snapshot
from test_instance_host import CountingPlugin
from test_instance_recovery import DecisionRunner
from trading_infra.admission import AdmissionService
from trading_infra.attribution import ExecutionAttribution
from trading_infra.budget import AccountBudgetAuthority
from trading_infra.instance_accounting import InstanceAccounting
from trading_infra.instance_registry import InstanceRegistry
from trading_infra.instance_runtime import InstanceRuntime
from trading_infra.instance_supervisor import InstanceSupervisor
from trading_infra.symbol_lease import SymbolLease

from test_partial_lifecycle import (ACCOUNT_LIMITS, POLICY, FailingPlugin,
                                   PartialFlow, TwoSharePlugin, instance_budget,
                                   manifest)
import test_native_operations


class StatusForKindDispatcher:
    """Reports a fixed status for one action kind; everything else submits."""

    def __init__(self, book, kind, status):
        self.book = book
        self.kind = kind
        self.status = status
        self.reported = []

    def __call__(self, *, allow_entries=True, allow_dispatch=True):
        if not allow_dispatch:
            return {'agent_called': False, 'reason': 'FRESH_BROKER_OBSERVATION_REQUIRED'}
        action = self.book.claim(allow_entries=allow_entries)
        if not action:
            return {'agent_called': False, 'reason': 'NO_DISPATCHABLE_ACTION'}
        status = self.status if action['kind'] == self.kind else 'submitted'
        self.book.report(action['action_id'], {'status': status})
        self.reported.append(action['action_id'])
        return {'agent_called': True, 'action_id': action['action_id'],
                'status': status}


class ApprovalFlow(PartialFlow):
    def rebuild(self):
        """A new owner connection and supervisor on the same database."""
        path = Path(self.temp.name) / 'partial.sqlite'
        policy = json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        book = ExecutionBook(path, policy, simulation=True, clock=lambda: self.now)
        self.addCleanup(book.close)
        registry = InstanceRegistry(book.db, clock=lambda: self.now)
        registry.register('alpha', manifest(), instance_budget(),
                          mode='shadow', enabled=True)
        runtime = InstanceRuntime(registry, registry.instance('alpha'),
                                  runner=DecisionRunner(CountingPlugin(proposal_at=None)),
                                  source_fingerprint='f' * 64, clock=lambda: self.now)
        self.addCleanup(runtime.close)
        money = AccountBudgetAuthority(book.db, ACCOUNT_LIMITS, clock=lambda: self.now)
        self.addCleanup(money.close)
        leases = SymbolLease(book.db, clock=lambda: self.now)
        self.addCleanup(leases.close)
        feedback = ExecutionAttribution(book.db, clock=lambda: self.now)
        accounting = InstanceAccounting(book.db, clock=lambda: self.now)
        admission = AdmissionService(budget=money, leases=leases,
                                     attribution=feedback, policy=POLICY,
                                     registry=registry, clock=lambda: self.now)
        dispatcher = StatusForKindDispatcher(book, 'entry', 'submitted')
        supervisor = LiveSupervisor(book, dispatcher=dispatcher,
                                    process_strategy=False, agent_mode='review',
                                    owner_source='f' * 64,
                                    instance_supervisor=InstanceSupervisor([runtime]),
                                    admission=admission, accounting=accounting)
        self.book, self.registry, self.runtime = book, registry, runtime
        self.money, self.leases = money, leases
        self.feedback, self.accounting = feedback, accounting
        self.dispatcher, self.supervisor = dispatcher, supervisor
        return supervisor


class PendingApprovalTests(ApprovalFlow):
    def test_a_pending_approval_is_not_a_fill(self):
        self.supervisor.dispatcher = StatusForKindDispatcher(
            self.book, 'entry', 'pending_approval')
        entry = self.make_entry()
        result = self.step([], ticks=2)
        self.assertFalse(result.get('errors'))
        action = self.book.actions()[0]
        # Strategy placement reports pending approval as a broker-facing
        # outcome; the formal native approval state machine is the engineering
        # handoff path. Either way it is not a fill.
        self.assertEqual(action['state'], 'REPORTED')
        self.assertEqual(json.loads(action['report'])['status'], 'pending_approval')
        self.assertEqual(self.accounting.fills('alpha'), [])
        self.assertIsNone(self.action('protect'))
        self.assertEqual(self.book.state()['trades'][entry['trade_id']]['quantity'], '0')
        self.assertEqual(self.leases.owner('DEMO'), 'alpha',
                         'a waiting entry keeps its symbol reservation')

        # The broker later confirms the exact order with a real execution.
        fill = self.execution('exec-entry-1', 2)
        self.pay_executions([fill])
        self.now += timedelta(seconds=5)
        order = self.order_for(entry, filled=2, state='filled', executions=[fill])
        self.step([order], qty='2', ticks=2)
        self.assertEqual([(item['side'], item['quantity'])
                          for item in self.accounting.fills('alpha')],
                         [('buy', '2')])
        self.assertIsNotNone(self.action('protect'))

    def test_an_unsubmitted_entry_expires_and_a_late_reply_cannot_revive_it(self):
        self.supervisor.dispatcher = NoClaimDispatcher()
        entry = self.make_entry()
        self.step([], ticks=1)
        self.assertEqual(self.book.actions()[0]['state'], 'READY')
        self.assertEqual(self.accounting.fills('alpha'), [])

        # The entry delivery window closes with no broker order.
        self.now += timedelta(seconds=30)
        self.step([], ticks=2)
        action = self.book.actions()[0]
        self.assertEqual(action['state'], 'EXPIRED')
        self.assertEqual(self.accounting.fills('alpha'), [])
        self.assertIsNone(self.action('protect'))
        trade = self.book.state()['trades'][entry['trade_id']]
        self.assertEqual(trade['phase'], 'FLAT_VERIFIED')
        self.assertEqual(self.accounting.fills('alpha'), [])

        # A late broker execution is a real fact: it is booked and managed as
        # the broker's position, but the expired intent is not re-dispatched as
        # if it had been alive.
        late = self.execution('exec-late-1', 2)
        self.pay_executions([late])
        order = self.order_for(action, filled=2, state='filled', executions=[late])
        self.step([order], qty='2', ticks=2)
        self.assertEqual(self.book.actions()[0]['state'], 'OBSERVED')
        self.assertEqual([(item['side'], item['quantity'])
                          for item in self.accounting.fills('alpha')],
                         [('buy', '2')],
                         'the broker execution is booked once, not ignored')
        entries = [item for item in self.book.actions() if item['kind'] == 'entry']
        self.assertEqual(len(entries), 2,
                         'only the natural second proposal exists; the expired '
                         'one is not re-dispatched')


class UnknownOutcomeTests(ApprovalFlow):
    def test_unknown_keeps_identity_across_a_new_book_and_is_only_reconciled(self):
        self.supervisor.dispatcher = StatusForKindDispatcher(
            self.book, 'entry', 'unknown')
        entry = self.make_entry()
        self.step([], ticks=2)
        action = self.book.actions()[0]
        self.assertEqual(action['state'], 'UNKNOWN')
        original_id = action['id']
        original_ref = self.payload(action)['ref_id']
        self.assertEqual(self.accounting.fills('alpha'), [])
        self.assertEqual(self.leases.owner('DEMO'), 'alpha')
        self.assertEqual(self.money.status()['open_cash'], '80.00')

        self.rebuild()
        for _ in range(3):
            self.now += timedelta(seconds=1)
            result = self.step([], ticks=1)
            self.assertFalse(result.get('errors'))
        actions = self.book.actions()
        self.assertEqual([item['id'] for item in actions], [original_id],
                         'a new supervisor never re-submits an UNKNOWN action')
        self.assertEqual(actions[0]['state'], 'UNKNOWN')
        self.assertEqual(self.payload(actions[0])['ref_id'], original_ref)
        self.assertEqual(self.dispatcher.reported, [])

        # Only the broker's own order and execution settle the unknown.
        fill = self.execution('exec-entry-1', 2)
        self.pay_executions([fill])
        self.now += timedelta(seconds=20)
        order = self.order_for(actions[0], filled=2, state='filled',
                               executions=[fill])
        self.step([order], qty='2', ticks=2)
        self.assertEqual([(item['side'], item['quantity'])
                          for item in self.accounting.fills('alpha')],
                         [('buy', '2')])
        owner = self.feedback.owner(original_ref)
        self.assertEqual(owner['instance_id'], 'alpha')
        self.assertIsNotNone(self.action('protect'))


class NativeExitDispatcher:
    """The host's native exit handoff, with a scripted broker response."""

    def __init__(self, book, snapshot, response, *, quote=None):
        self.book = book
        self.snapshot = snapshot
        self.response = response
        self.quote = quote
        self.calls = []

    def __call__(self, *, allow_entries=True, allow_dispatch=True):
        action = next((item for item in self.book.actions()
                       if item['kind'] == 'exit' and item['state'] == 'READY'), None)
        if action is None:
            return {'agent_called': False, 'reason': 'NO_DISPATCHABLE_ACTION'}
        operation_id = 'op-' + action['id']
        self.book.prepare_external_action(
            action['id'], self.snapshot, operation_id=operation_id,
            operation='place', quote=self.quote)
        report = dict(self.response)
        self.book.record_external_report(
            action['id'], report, operation_id=operation_id,
            broker_order_id=report.get('order_id'),
            broker_account_key=self.book.state()['account_key'])
        self.calls.append((action['id'], operation_id, report['status']))
        return {'agent_called': True, 'action_id': action['id'],
                'operation': 'place', 'status': report['status']}


class NoClaimDispatcher:
    """A dispatcher that is deliberately busy: nothing is claimed this tick."""

    def __init__(self):
        self.reported = []

    def __call__(self, *, allow_entries=True, allow_dispatch=True):
        return {'agent_called': False, 'reason': 'NO_DISPATCHABLE_ACTION'}


class ExitRetryTests(ApprovalFlow):
    def breach_and_queue_exit(self):
        """Reach a READY exit without letting a dispatcher claim it."""
        entry, protection, entry_order, resting = self.full_entry_protected()
        self.now += timedelta(minutes=5)
        self.supervisor.dispatcher = NoClaimDispatcher()
        self.step([entry_order, resting], qty='2', bid='38.90', ask='38.95', ticks=2)
        cancelled = self.order_for(protection, filled=0, state='canceled')
        self.step([entry_order, cancelled], qty='2', bid='38.90', ask='38.95', ticks=3)
        exits = [item for item in self.book.actions() if item['kind'] == 'exit']
        self.assertTrue(exits, 'the breach must actually queue an exit')
        self.assertEqual({item['state'] for item in exits}, {'READY'})
        return entry, protection, entry_order, cancelled, exits

    def native(self, response, *, quote=None):
        """A native exit handoff bound to the current synthetic facts."""
        snapshot = self.snap(getattr(self, 'last_orders', []), qty='2')
        return NativeExitDispatcher(self.book, snapshot, response,
                                    quote=quote or {'symbol': 'DEMO', 'bid': '38.90',
                                                    'ask': '38.95',
                                                    'bid_at': self.now.isoformat(),
                                                    'ask_at': self.now.isoformat(),
                                                    'observed_at': self.now.isoformat()})

    def current_orders(self, entry_order, cancelled):
        self.orders['current'] = [entry_order, cancelled]
        return [entry_order, cancelled]

    def test_an_agent_only_rejection_cannot_authorise_a_retry(self):
        entry, protection, entry_order, cancelled, exits = self.breach_and_queue_exit()
        self.supervisor.dispatcher = StatusForKindDispatcher(
            self.book, 'exit', 'rejected')
        self.step([entry_order, cancelled], qty='2', bid='38.90', ask='38.95')
        first = exits[-1]
        first = next(item for item in self.book.actions() if item['id'] == first['id'])
        self.assertEqual((first['state'], json.loads(first['report'])['status']),
                         ('REPORTED', 'rejected'))
        # No native operation and no exact terminal broker order: the agent's
        # own word is not evidence that the exit has no effect.
        self.supervisor.dispatcher = NoClaimDispatcher()
        self.step([entry_order, cancelled], qty='2', bid='38.90', ask='38.95', ticks=2)
        exits = [item for item in self.book.actions() if item['kind'] == 'exit']
        self.assertEqual([item['id'] for item in exits], [first['id']])

    def test_a_native_refusal_gets_a_new_auditable_attempt_then_fills_flat(self):
        entry, protection, entry_order, cancelled, exits = self.breach_and_queue_exit()
        first = exits[-1]
        self.supervisor.dispatcher = native = self.native(
            {'status': 'rejected', 'message': 'broker refused the exit'})
        self.step([entry_order, cancelled], qty='2', bid='38.90', ask='38.95')
        self.assertEqual(native.calls[-1][0], first['id'])
        row = self.db.execute(
            "SELECT report FROM external_operations WHERE action_id=? AND operation='place'",
            (first['id'],)).fetchone()
        self.assertEqual(json.loads(row[0])['status'], 'rejected')

        # The native refusal is definitive: reconciliation prepares a distinct
        # attempt, and the refused one stays recorded.
        self.supervisor.dispatcher = NoClaimDispatcher()
        self.step([entry_order, cancelled], qty='2', bid='38.90', ask='38.95')
        exits = [item for item in self.book.actions() if item['kind'] == 'exit']
        self.assertGreaterEqual(len(exits), 2, 'a native refusal must produce a new attempt')
        new = [item for item in exits if item['id'] != first['id']][-1]
        self.assertEqual(new['state'], 'READY')
        self.assertEqual(json.loads(new['payload'])['exit_attempt'], 2)
        self.assertNotEqual(json.loads(new['payload'])['ref_id'],
                            json.loads(first['payload'])['ref_id'])
        self.assertEqual(next(item for item in exits
                              if item['id'] == first['id'])['state'], 'REJECTED')

        # The retry is submitted through the native path with its own order id.
        retry = self.native({'status': 'submitted', 'order_id': 'official-exit-order'})
        self.supervisor.dispatcher = retry
        self.step([entry_order, cancelled], qty='2', bid='38.90', ask='38.95', ticks=2)
        self.assertEqual(retry.calls[-1][0], new['id'])
        sell = self.execution('exec-exit-1', 2, price='38.90')
        self.delivered.add(sell['id'])
        self.cash += Decimal('38.90') * 2
        filled = self.order_for(new, filled=2, state='filled', executions=[sell],
                                order_id='official-exit-order')
        self.step([entry_order, cancelled, filled], qty='0',
                  bid='38.90', ask='38.95', ticks=4)
        self.assertEqual(self.book.state()['trades'][entry['trade_id']]['phase'],
                         'FLAT_VERIFIED')
        self.assertEqual(self.accounting.instance_summary('alpha')['open_quantity'],
                         '0')
        self.assertIsNone(self.leases.owner('DEMO'))
        self.assertEqual(self.money.status()['open_cash'], '0')

    def test_a_broker_rejected_terminal_exit_gets_a_new_attempt(self):
        entry, protection, entry_order, cancelled, exits = self.breach_and_queue_exit()
        first = exits[-1]
        self.supervisor.dispatcher = StatusForKindDispatcher(
            self.book, 'entry', 'submitted')
        self.step([entry_order, cancelled], qty='2', bid='38.90', ask='38.95')
        first = next(item for item in self.book.actions() if item['id'] == first['id'])
        self.assertEqual(first['state'], 'REPORTED')

        # The broker later reports this exact order terminally rejected with no
        # executions. Reconciliation observes it; that is a definitive refusal.
        rejected = self.order_for(first, filled=0, state='rejected', executions=[])
        self.supervisor.dispatcher = NoClaimDispatcher()
        self.step([entry_order, cancelled, rejected], qty='2',
                  bid='38.90', ask='38.95')
        first = next(item for item in self.book.actions() if item['id'] == first['id'])
        self.assertEqual(first['state'], 'OBSERVED')

        for _ in range(3):
            self.now += timedelta(seconds=1)
            self.step([entry_order, cancelled, rejected], qty='2',
                      bid='38.90', ask='38.95')
        exits = [item for item in self.book.actions() if item['kind'] == 'exit']
        self.assertGreaterEqual(len(exits), 2,
                                'a broker terminal rejection must produce a retry')
        new = [item for item in exits if item['id'] != first['id']][-1]
        self.assertEqual(json.loads(new['payload'])['exit_attempt'], 2)
        self.assertEqual(new['state'], 'READY')
        self.assertNotEqual(json.loads(new['payload'])['ref_id'],
                            json.loads(first['payload'])['ref_id'])
        old = next(item for item in exits if item['id'] == first['id'])
        self.assertIn(old['state'], {'REJECTED','OBSERVED'},
                      'the refused attempt keeps its identity in the ledger')

    def _ready_exits(self):
        return [a for a in self.book.actions()
                if a['kind'] == 'exit' and a['state'] == 'READY']

    def test_not_dispatched_then_two_native_refusals_progresses(self):
        entry, protection, entry_order, cancelled, exits = self.breach_and_queue_exit()
        first = exits[-1]
        claimed = self.book.claim(allow_entries=False)
        self.assertEqual(claimed['action_id'], first['id'])
        self.book.abandon_not_dispatched(
            first['id'], reason='probe',
            proof='RESERVATION_REJECTED_WITHOUT_RECORD')
        self.supervisor.dispatcher = NoClaimDispatcher()
        self.step([entry_order, cancelled], qty='2',
                  bid='38.90', ask='38.95')
        for _ in range(2):
            self.supervisor.dispatcher = self.native(
                {'status': 'rejected', 'message': 'synthetic definitive refusal'})
            self.step([entry_order, cancelled], qty='2',
                      bid='38.90', ask='38.95')
            self.supervisor.dispatcher = NoClaimDispatcher()
            self.step([entry_order, cancelled], qty='2',
                      bid='38.90', ask='38.95')
        ready = self._ready_exits()
        self.assertEqual(len(ready), 1)
        payload = json.loads(ready[0]['payload'])
        self.assertEqual(payload['not_dispatched_generation'], 2)
        self.assertEqual(payload['exit_attempt'], 3)
        exits = [a for a in self.book.actions() if a['kind'] == 'exit']
        self.assertEqual(len([a for a in exits if a['state'] == 'REJECTED']), 2)
        self.assertEqual(len({json.loads(a['payload'])['ref_id'] for a in exits}),
                         len(exits))

    def test_native_refusal_then_not_dispatched_then_refusal_progresses(self):
        entry, protection, entry_order, cancelled, exits = self.breach_and_queue_exit()
        self.supervisor.dispatcher = self.native(
            {'status': 'rejected', 'message': 'first refusal'})
        self.step([entry_order, cancelled], qty='2', bid='38.90', ask='38.95')
        self.supervisor.dispatcher = NoClaimDispatcher()
        self.step([entry_order, cancelled], qty='2', bid='38.90', ask='38.95')
        retry = self._ready_exits()[-1]
        claimed = self.book.claim(allow_entries=False)
        self.assertEqual(claimed['action_id'], retry['id'])
        self.book.abandon_not_dispatched(
            retry['id'], reason='probe',
            proof='RESERVATION_REJECTED_WITHOUT_RECORD')
        self.step([entry_order, cancelled], qty='2',
                  bid='38.90', ask='38.95')
        self.supervisor.dispatcher = self.native(
            {'status': 'rejected', 'message': 'second refusal'})
        self.step([entry_order, cancelled], qty='2', bid='38.90', ask='38.95')
        self.supervisor.dispatcher = NoClaimDispatcher()
        self.step([entry_order, cancelled], qty='2', bid='38.90', ask='38.95')
        current = self._ready_exits()[-1]
        payload = json.loads(current['payload'])
        self.assertEqual(payload['not_dispatched_generation'], 2)
        self.assertEqual(payload['exit_attempt'], 3)

    def test_mixed_exit_retry_replay_and_restart_keep_identity(self):
        entry, protection, entry_order, cancelled, exits = self.breach_and_queue_exit()
        first = exits[-1]
        self.book.claim(allow_entries=False)
        self.book.abandon_not_dispatched(
            first['id'], reason='probe',
            proof='RESERVATION_REJECTED_WITHOUT_RECORD')
        self.supervisor.dispatcher = NoClaimDispatcher()
        self.step([entry_order, cancelled], qty='2',
                  bid='38.90', ask='38.95')
        before = [(a['id'], a['payload']) for a in self._ready_exits()]
        self.step([entry_order, cancelled], qty='2',
                  bid='38.90', ask='38.95')
        self.assertEqual([(a['id'], a['payload'])
                          for a in self._ready_exits()], before)
        self.rebuild()
        self.supervisor.dispatcher = NoClaimDispatcher()
        self.step([entry_order, cancelled], qty='2',
                  bid='38.90', ask='38.95')
        self.assertEqual([(a['id'], a['payload'])
                          for a in self._ready_exits()], before)

    def test_mixed_exit_retry_preserves_not_dispatched_generation_cap(self):
        entry, protection, entry_order, cancelled, exits = self.breach_and_queue_exit()
        first = exits[-1]
        self.book.claim(allow_entries=False)
        self.book.abandon_not_dispatched(
            first['id'], reason='probe',
            proof='RESERVATION_REJECTED_WITHOUT_RECORD')
        for _ in range(2):
            self.supervisor.dispatcher = NoClaimDispatcher()
            self.step([entry_order, cancelled], qty='2',
                      bid='38.90', ask='38.95')
            current = self._ready_exits()[-1]
            claimed = self.book.claim(allow_entries=False)
            self.assertEqual(claimed['action_id'], current['id'])
            self.book.abandon_not_dispatched(
                current['id'], reason='probe',
                proof='RESERVATION_REJECTED_WITHOUT_RECORD')
        self.supervisor.dispatcher = NoClaimDispatcher()
        self.step([entry_order, cancelled], qty='2',
                  bid='38.90', ask='38.95')
        self.assertEqual(self._ready_exits(), [])
        trade = self.book.state()['trades'][entry['trade_id']]
        self.assertTrue(trade.get('manual_intervention_required','').startswith(
            'EXIT_NOT_DISPATCHED_RETRY_LIMIT_EXCEEDED'))

    def test_mixed_exit_retry_preserves_formal_refusal_cap(self):
        entry, protection, entry_order, cancelled, exits = self.breach_and_queue_exit()
        first = exits[-1]
        self.book.claim(allow_entries=False)
        self.book.abandon_not_dispatched(
            first['id'], reason='probe',
            proof='RESERVATION_REJECTED_WITHOUT_RECORD')
        self.supervisor.dispatcher = NoClaimDispatcher()
        self.step([entry_order, cancelled], qty='2',
                  bid='38.90', ask='38.95')
        for _ in range(3):
            self.supervisor.dispatcher = self.native(
                {'status': 'rejected', 'message': 'refusal'})
            self.step([entry_order, cancelled], qty='2',
                      bid='38.90', ask='38.95')
            self.supervisor.dispatcher = NoClaimDispatcher()
            self.step([entry_order, cancelled], qty='2',
                      bid='38.90', ask='38.95')
        self.assertEqual(self._ready_exits(), [])
        trade = self.book.state()['trades'][entry['trade_id']]
        self.assertTrue(trade.get('manual_intervention_required','').startswith(
            'EXIT_RETRY_LIMIT_EXCEEDED'))
        latest = max((a for a in self.book.actions() if a['kind'] == 'exit'),
                     key=lambda a:(json.loads(a['payload']).get(
                         'not_dispatched_generation',1),
                         json.loads(a['payload']).get('exit_attempt',1)))
        self.assertEqual(latest['state'], 'REPORTED')

    def test_a_submitted_exit_with_no_broker_order_is_never_retried(self):
        entry, protection, entry_order, cancelled, exits = self.breach_and_queue_exit()
        first = exits[-1]
        self.supervisor.dispatcher = StatusForKindDispatcher(
            self.book, 'entry', 'submitted')
        self.step([entry_order, cancelled], qty='2', bid='38.90', ask='38.95')
        first = next(item for item in self.book.actions() if item['id'] == first['id'])
        self.assertEqual(first['state'], 'REPORTED')
        # Neither a submitted report nor an absent order proves the exit has no
        # effect, so nothing is re-sent.
        for _ in range(3):
            self.now += timedelta(seconds=1)
            self.step([entry_order, cancelled], qty='2',
                      bid='38.90', ask='38.95')
        exits = [item for item in self.book.actions() if item['kind'] == 'exit']
        self.assertEqual([item['id'] for item in exits], [first['id']])
        self.assertEqual(self.leases.owner('DEMO'), 'alpha')

    def test_a_native_unknown_exit_is_reconciled_never_re_sent(self):
        entry, protection, entry_order, cancelled, exits = self.breach_and_queue_exit()
        first = exits[-1]
        unknown = self.native({'status': 'unknown', 'message': 'timeout'})
        self.supervisor.dispatcher = unknown
        self.step([entry_order, cancelled], qty='2', bid='38.90', ask='38.95')
        first = next(item for item in self.book.actions() if item['id'] == first['id'])
        self.assertEqual(first['state'], 'UNKNOWN')

        self.rebuild()
        for _ in range(3):
            self.now += timedelta(seconds=1)
            self.step([entry_order, cancelled], qty='2', bid='38.90',
                      ask='38.95', ticks=1)
        exits = [item for item in self.book.actions() if item['kind'] == 'exit']
        self.assertEqual([item['id'] for item in exits], [first['id']],
                         'an UNKNOWN exit is never replayed, native or not')
        self.assertEqual(exits[0]['state'], 'UNKNOWN')
        self.assertEqual(self.dispatcher.reported, [])
        self.assertEqual(self.leases.owner('DEMO'), 'alpha',
                         'nothing is released while the exit is unknown')

    def test_a_stop_fill_during_cancel_exits_the_remaining_share_to_flat(self):
        entry, protection, entry_order, resting = self.full_entry_protected()
        self.now += timedelta(minutes=5)
        self.step([entry_order, resting], qty='2', bid='38.90', ask='38.95', ticks=2)
        self.assertIsNone(self.action('exit'))
        sold = self.execution('exec-protect-1', 1, price='39.60')
        self.delivered.add(sold['id'])
        self.cash += Decimal('39.60')
        partial = self.order_for(protection, filled=1, state='partially_filled',
                                 executions=[sold])
        self.step([entry_order, partial], qty='1', bid='38.90', ask='38.95', ticks=2)
        self.assertIsNone(self.action('exit'),
                          'the stop cancel is not terminal yet; no exit may race it')
        terminal_stop = self.order_for(protection, filled=1, state='canceled',
                                       executions=[sold])
        self.step([entry_order, terminal_stop], qty='1', bid='38.90', ask='38.95',
                  ticks=2)
        exits = [item for item in self.book.actions() if item['kind'] == 'exit']
        self.assertTrue(exits, 'the remaining share must actually be exited')
        exit_action = exits[-1]
        self.assertEqual(json.loads(exit_action['payload'])['parameters']['quantity'],
                         '1')
        # Finish the close: the remaining share fills and the trade is flat.
        rest = self.execution('exec-exit-1', 1, price='38.90')
        self.delivered.add(rest['id'])
        self.cash += Decimal('38.90')
        filled = self.order_for(exit_action, filled=1, state='filled',
                                executions=[rest])
        self.step([entry_order, terminal_stop, filled], qty='0',
                  bid='38.90', ask='38.95', ticks=4)
        self.assertEqual(self.book.state()['trades'][entry['trade_id']]['phase'],
                         'FLAT_VERIFIED')
        self.assertEqual(self.accounting.instance_summary('alpha')['open_quantity'],
                         '0')
        self.assertIsNone(self.leases.owner('DEMO'))
        self.assertEqual(self.money.status()['open_cash'], '0')


class ScriptedNative:
    """The host native handoff with a scripted broker response per tick."""

    def __init__(self, ops, action_id, snapshot, quote, steps):
        self.ops = ops
        self.action_id = action_id
        self.snapshot = snapshot
        self.quote = quote
        self.steps = list(steps)
        self.log = []

    def __call__(self, *, allow_entries=True, allow_dispatch=True):
        if not allow_dispatch:
            return {'agent_called': False, 'reason': 'FRESH_BROKER_OBSERVATION_REQUIRED'}
        if not self.steps:
            return {'agent_called': False, 'reason': 'NO_SCRIPTED_NATIVE_STEP'}
        operation, response = self.steps.pop(0)
        packet = self.ops.begin(self.action_id, operation, self.snapshot(),
                                quote=self.quote)
        if callable(response):
            response = response(packet)
        result = self.ops.record(packet['operation_id'], response)
        self.log.append((operation, result.get('report', {}).get('status')))
        return {'agent_called': True, 'operation': operation, 'result': result}


class NativeApprovalLifecycleTests(TestCase):
    """Formal native approval facts through the default supervisor path."""

    def setUp(self):
        fixture = test_native_operations.NativeOperationTests(
            'test_preview_does_not_reserve_approve_or_submit')
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        self.fixture = fixture
        self.book, self.ops, self.f = fixture.book, fixture.ops, fixture.f
        self.action_id = fixture.action
        self.script = None
        self.supervisor = LiveSupervisor(
            self.book, dispatcher=self.handle, process_strategy=False,
            agent_mode='review', owner_source='f' * 64)
        self.pending = self.start_pending_approval()

    def handle(self, *, allow_entries=True, allow_dispatch=True):
        return self.script(allow_entries=allow_entries, allow_dispatch=allow_dispatch)

    def broker(self, *, approvals=None, orders=None):
        value = self.f.snap([self.fixture.buy, *(orders or [])], qty='2')
        if approvals is not None:
            value['requested_approval_ids'] = list(approvals)
            value['approval_states'] = dict(approvals)
            value['missing_approval_ids'] = []
        return value

    @staticmethod
    def review_response(packet):
        return {'data': {**packet['parameters'], 'order_checks': {},
                         'customer_approval_required': False,
                         'market_data_disclosure': 'Exact disclosure text',
                         'quote_data': {}}}

    def approval(self, state, *, edited=False):
        return {'official-approval': {'approval_id': 'official-approval',
                                      'asset_class': 'ASSET_CLASS_EQUITY',
                                      'state': state, 'is_edited': edited}}

    def step(self, snapshot=None):
        return self.supervisor.step(self.broker() if snapshot is None else snapshot, [])

    def start_pending_approval(self):
        self.script = ScriptedNative(
            self.ops, self.action_id,
            lambda: self.f.snap([self.fixture.buy], qty='2'), self.f.quote,
            [('review', self.review_response),
             ('place', self.fixture.official_approval())])
        # Two native operations, one per tick: review then place.
        self.assertEqual(self.script.log, [])
        review_result = self.step()
        self.assertTrue(review_result is not None)
        self.script.snapshot = lambda: self.f.snap([self.fixture.buy], qty='2')
        self.step()
        action = next(a for a in self.book.actions() if a['id'] == self.action_id)
        self.assertEqual(action['state'], 'REPORTED')
        self.assertEqual(json.loads(action['report'])['status'], 'pending_approval')
        aliases = {row[0] for row in self.book.db.execute(
            'SELECT ref_id FROM external_broker_refs WHERE action_id=?',
            (self.action_id,))}
        self.assertIn('official-approval', aliases)
        self.assertNotEqual(next(iter(self.book.state()['trades'].values()))['phase'],
                            'PROTECTED')
        return self.script

    def test_pending_approval_holds_and_an_empty_list_does_not_clear_it(self):
        self.step()
        action = next(a for a in self.book.actions() if a['id'] == self.action_id)
        self.assertEqual(action['state'], 'REPORTED')
        self.assertEqual(json.loads(action['report'])['status'], 'pending_approval')
        trade = next(iter(self.book.state()['trades'].values()))
        self.assertNotEqual(trade['phase'], 'PROTECTED')
        self.assertEqual(trade.get('exit_requested'), False)

    def test_an_edited_approval_is_a_manual_reconciliation_issue(self):
        self.step(self.broker(approvals=self.approval(
            'APPROVAL_STATE_PENDING', edited=True)))
        issues = self.book.state()['issues']
        self.assertTrue(any(item.startswith(
            'APPROVAL_EDITED_REQUIRES_MANUAL_RECONCILIATION')
            for item in issues), issues)
        action = next(a for a in self.book.actions() if a['id'] == self.action_id)
        self.assertNotIn(action['state'], {'OBSERVED', 'APPROVAL_DECLINED',
                                           'APPROVAL_EXPIRED'})
        # A later empty approval list is not evidence that anything changed.
        self.step()
        self.assertTrue(any(item.startswith(
            'APPROVAL_EDITED_REQUIRES_MANUAL_RECONCILIATION')
            for item in self.book.state()['issues']))

    def test_a_declined_approval_is_terminal_and_a_late_order_contradicts_it(self):
        self.step(self.broker(approvals=self.approval('APPROVAL_STATE_DECLINED')))
        action = next(a for a in self.book.actions() if a['id'] == self.action_id)
        self.assertEqual(action['state'], 'APPROVAL_DECLINED')
        trade = next(iter(self.book.state()['trades'].values()))
        self.assertTrue(trade['exit_requested'])
        # A late order under the exact approval alias cannot silently revive it.
        late = {'id': 'late-order', 'symbol': 'DEMO', 'side': 'sell',
                'quantity': '2', 'cumulative_quantity': '0', 'type': 'market',
                'trigger': 'stop', 'stop_price': '39', 'created_at': self.f.now.isoformat(),
                'state': 'confirmed', 'market_hours': 'regular_hours',
                'time_in_force': 'gtc', 'executions': [],
                'ref_id': 'official-approval'}
        with self.assertRaisesRegex(ValueError,
                                    'BROKER_APPROVAL_TERMINAL_ORDER_CONTRADICTION'):
            self.step(self.broker(approvals=self.approval('APPROVAL_STATE_DECLINED'),
                                  orders=[late]))

    def test_an_expired_approval_is_terminal(self):
        self.step(self.broker(approvals=self.approval('APPROVAL_STATE_EXPIRED')))
        action = next(a for a in self.book.actions() if a['id'] == self.action_id)
        self.assertEqual(action['state'], 'APPROVAL_EXPIRED')
        trade = next(iter(self.book.state()['trades'].values()))
        self.assertTrue(trade['exit_requested'])

    def test_approval_state_alone_does_not_prove_the_order(self):
        # An approved state without any broker order is not a placement.
        self.step(self.broker(approvals=self.approval('APPROVAL_STATE_APPROVED')))
        action = next(a for a in self.book.actions() if a['id'] == self.action_id)
        self.assertEqual(action['state'], 'REPORTED')
        self.assertEqual(json.loads(action['report'])['status'], 'pending_approval')
        self.assertNotEqual(next(iter(self.book.state()['trades'].values()))['phase'],
                            'PROTECTED')

        # Only the broker's own order under that alias proves the placement.
        stop = {'id': 'official-stop', 'symbol': 'DEMO', 'side': 'sell',
                'quantity': '2', 'cumulative_quantity': '0', 'type': 'market',
                'trigger': 'stop', 'stop_price': '39',
                'created_at': self.f.now.isoformat(), 'state': 'confirmed',
                'market_hours': 'regular_hours', 'time_in_force': 'gtc',
                'executions': [], 'ref_id': 'official-approval'}
        value = self.broker(orders=[stop])
        value['orders'][stop['id']]['ref_id'] = 'official-approval'
        self.step(value)
        action = next(a for a in self.book.actions() if a['id'] == self.action_id)
        self.assertEqual(action['state'], 'OBSERVED')
        self.assertEqual(next(iter(self.book.state()['trades'].values()))['phase'],
                         'PROTECTED')

