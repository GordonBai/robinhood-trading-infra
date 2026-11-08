"""Partial and late executions through the default composed control loop.

Every test here drives the production path: strategy instance -> supervisor ->
admission -> execution book -> reporting dispatcher, with a synthetic broker
answering snapshots. The test never calls ``book.propose``, ``record_fill`` or
``admission.settle`` to make a scenario happen; it only submits proposals through
the loop and reports broker facts.

The broker is economically consistent: cash moves by every execution delivered
and positions match the executions the broker has reported, so the risk guard is
never asked to ignore an unexplained cash flow.

Scenario matrix (see plan/iterations/003r4/IMPLEMENTATION.md):

* S1 partial entry 1/2 -> protect 1 + cancel remaining buy
* S2 protection identity/accounting, no coverage before broker confirmation
* S3 late second fill while the remainder cancel is pending
* S4 protection rejected -> no coverage, exit requested, new risk blocked
* S5 protection partially fills while its cancel is pending (no oversell)
* S6 duplicate / out-of-order receipts book once
* S7 one instance fault does not stop another instance's management
* S8 partial state survives a new book and supervisor construction
"""
import json
import tempfile
from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest import TestCase

from entry_authority_fixture import TestExecutionBook as ExecutionBook
from martin_luk.live_session import LiveSupervisor
from test_broker import FakeBroker, position
from test_broker import snapshot as broker_snapshot
from test_instance_host import CountingPlugin
from test_instance_recovery import DecisionRunner
from trading_contracts.instance import InstanceBudget, PluginManifest
from trading_infra.admission import AdmissionService
from trading_infra.attribution import ExecutionAttribution
from trading_infra.budget import AccountBudgetAuthority
from trading_infra.instance_accounting import InstanceAccounting
from trading_infra.instance_registry import InstanceRegistry
from trading_infra.instance_runtime import InstanceRuntime
from trading_infra.instance_supervisor import InstanceSupervisor
from trading_infra.symbol_lease import SymbolLease

AT = datetime.fromisoformat('2026-10-16T14:00:00+00:00')
ACCOUNT_LIMITS = {'max_open_cash': Decimal('500'),
                  'max_open_notional': Decimal('500'),
                  'max_open_planned_loss': Decimal('10.00'),
                  'max_concurrent_trades': 4}
POLICY = {'max_position_notional_usd': '100',
          'max_planned_loss_per_trade_usd': '2.50'}


class TwoSharePlugin(CountingPlugin):
    """Same deterministic advice, but a quantity the broker can half-fill."""

    def decide(self, request):
        decision = super().decide(request)
        proposals = tuple(replace(item, quantity_limit=2)
                          for item in decision.proposals)
        return type(decision)(**{**decision.__dict__, 'proposals': proposals})


def manifest(plugin_id='alpha'):
    return PluginManifest(plugin_id=plugin_id, version='1.0', state_schema='s-1',
                          config={'step': '1', 'cost_buffer_per_share': '0.05'},
                          data_requirements=('quotes',),
                          capabilities=('advisory-entry',), max_inflight=8)


def instance_budget():
    return InstanceBudget(max_cash=Decimal('100'), max_planned_loss=Decimal('2.50'),
                          max_notional=Decimal('100'), max_concurrent=1)


class ReportingDispatcher:
    """The daemon's worker: claim one action, report it submitted."""

    def __init__(self, book):
        self.book = book
        self.reported = []

    def __call__(self, *, allow_entries=True, allow_dispatch=True):
        if not allow_dispatch:
            return {'agent_called': False, 'reason': 'FRESH_BROKER_OBSERVATION_REQUIRED'}
        action = self.book.claim(allow_entries=allow_entries)
        if not action:
            return {'agent_called': False, 'reason': 'NO_DISPATCHABLE_ACTION'}
        self.book.report(action['action_id'], {'status': 'submitted'})
        self.reported.append(action['action_id'])
        return {'agent_called': True, 'action_id': action['action_id']}


class PartialFlow(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.now = AT
        self.cash = Decimal('500')
        self.fees = Decimal('0')
        self.policy = json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        self.book = ExecutionBook(Path(self.temp.name) / 'partial.sqlite',
                                  self.policy, simulation=True, clock=lambda: self.now)
        self.addCleanup(self.book.close)
        self.db = self.book.db
        self.registry = InstanceRegistry(self.db, clock=lambda: self.now)
        self.registry.register('alpha', manifest(), instance_budget(),
                               mode='shadow', enabled=True)
        runtime = InstanceRuntime(self.registry, self.registry.instance('alpha'),
                                  runner=DecisionRunner(TwoSharePlugin(proposal_at=1)),
                                  source_fingerprint='f' * 64, clock=lambda: self.now)
        self.addCleanup(runtime.close)
        self.runtime = runtime
        self.money = AccountBudgetAuthority(self.db, ACCOUNT_LIMITS,
                                            clock=lambda: self.now)
        self.addCleanup(self.money.close)
        self.leases = SymbolLease(self.db, clock=lambda: self.now)
        self.addCleanup(self.leases.close)
        self.feedback = ExecutionAttribution(self.db, clock=lambda: self.now)
        self.accounting = InstanceAccounting(self.db, clock=lambda: self.now)
        self.admission = AdmissionService(budget=self.money, leases=self.leases,
                                          attribution=self.feedback, policy=POLICY,
                                          registry=self.registry, clock=lambda: self.now)
        self.dispatcher = ReportingDispatcher(self.book)
        self.supervisor = self.build_supervisor(runtime)
        self.delivered = set()
        self.orders = {}

    def build_supervisor(self, *runtimes):
        return LiveSupervisor(self.book, dispatcher=self.dispatcher,
                              process_strategy=False, agent_mode='review',
                              owner_source='f' * 64,
                              instance_supervisor=InstanceSupervisor(list(runtimes)),
                              admission=self.admission,
                              accounting=self.accounting)

    # -- broker -----------------------------------------------------------
    def snap(self, orders=(), *, qty='0', held='0'):
        fake = FakeBroker()
        fake.orders = list(orders)
        fake.positions = [] if qty == '0' else [position(qty, held)]
        fake.cash = str(self.cash)
        value = broker_snapshot(fake, self.now.isoformat())
        value['synthetic'] = True
        return value

    def event(self, *, symbol='DEMO', bid='39.99', ask='40.00'):
        at = self.now.isoformat()
        return {'id': 'e-' + at, 'kind': 'quote', 'at': at, 'synthetic_input': True,
                'source_sha256': 'source-' + at,
                'data': {'symbol': symbol, 'bid': bid, 'ask': ask,
                         'bid_at': at, 'ask_at': at, 'observed_at': at}}

    def execution(self, execution_id, quantity, *, price='40', at=None, fees='0'):
        return {'id': execution_id, 'price': price, 'quantity': str(quantity),
                'fees': fees, 'timestamp': (at or (self.now - timedelta(seconds=5))).isoformat()}

    def pay_executions(self, executions):
        """Apply the broker's economic effect exactly once per execution id."""
        for item in executions:
            if item['id'] in self.delivered:
                continue
            self.delivered.add(item['id'])
            delta = Decimal(item['price']) * Decimal(item['quantity'])
            self.cash -= delta + Decimal(item['fees'])
            self.fees += Decimal(item['fees'])

    def order_for(self, action, *, filled, state, executions=(), order_id=None):
        payload = json.loads(action['payload'])
        parameters = payload['parameters']
        kind, trigger = {'limit': ('limit', 'immediate'),
                         'market': ('market', 'immediate'),
                         'stop_market': ('market', 'stop'),
                         'stop_limit': ('limit', 'stop')}[parameters['type']]
        raw = {'id': order_id or ('broker-' + action['id']),
               'symbol': parameters['symbol'], 'side': parameters['side'],
               'quantity': parameters['quantity'],
               'cumulative_quantity': str(filled), 'type': kind,
               'trigger': trigger, 'price': parameters.get('limit_price'),
               'stop_price': parameters.get('stop_price'),
               'created_at': self.now.isoformat(), 'state': state,
               'market_hours': parameters['market_hours'],
               'time_in_force': parameters['time_in_force'],
               'executions': list(executions), 'ref_id': payload['ref_id']}
        self.orders[action['id']] = raw
        return raw

    def step(self, orders=(), *, qty='0', held='0', bid='39.99', ask='40.00',
             ticks=1, entries=True, dispatch=True, snapshot=None):
        last = None
        self.last_orders = list(orders)
        for _ in range(ticks):
            value = (snapshot if snapshot is not None
                     else self.snap(orders, qty=qty, held=held))
            last = self.supervisor.step(value, [self.event(bid=bid, ask=ask)],
                                        allow_entries=entries, allow_dispatch=dispatch)
        return last

    def action(self, kind):
        found = [item for item in self.book.actions() if item['kind'] == kind]
        return found[-1] if found else None

    def actions(self):
        return [(item['kind'], item['state']) for item in self.book.actions()]

    def payload(self, action):
        return json.loads(action['payload'])

    def make_entry(self):
        for _ in range(4):
            self.step()
        entry = self.action('entry')
        self.assertIsNotNone(entry, 'the loop must create an entry action')
        return entry

    def snap_order(self, entry):
        """The entry order as the broker already reported it."""
        return self.orders[entry['id']]

    def full_entry_protected(self):
        """Two shares filled and a confirmed stop covering both."""
        entry = self.make_entry()
        self.step([self.order_for(entry, filled=0, state='confirmed')])
        fill = self.execution('exec-entry-1', 2)
        self.entry_execution = fill
        self.pay_executions([fill])
        self.now += timedelta(seconds=5)
        self.step([self.order_for(entry, filled=2, state='filled',
                                  executions=[fill])], qty='2')
        protection = self.action('protect')
        self.assertIsNotNone(protection)
        resting = self.order_for(protection, filled=0, state='confirmed')
        self.step([self.orders[entry['id']], resting], qty='2', ticks=2)
        kinds = [item['kind'] for item in self.feedback.events('alpha')]
        self.assertIn('protection_covered', kinds)
        return entry, protection, self.orders[entry['id']], resting

    def protected_one_share(self):
        """Drive an entry to: one share filled, one resting stop covering it."""
        entry = self.make_entry()
        self.step([self.order_for(entry, filled=0, state='confirmed')])
        first = self.execution('exec-entry-1', 1)
        self.pay_executions([first])
        self.now += timedelta(seconds=5)
        self.step([self.order_for(entry, filled=1, state='partially_filled',
                                  executions=[first])], qty='1')
        protection = self.action('protect')
        self.assertIsNotNone(protection)
        resting = self.order_for(protection, filled=0, state='confirmed')
        entry_order = self.orders[entry['id']]
        self.step([entry_order, resting], qty='1', ticks=2)
        kinds = [item['kind'] for item in self.feedback.events('alpha')]
        self.assertIn('protection_covered', kinds)
        return entry, protection, entry_order, resting, first


class RejectOnKindDispatcher(ReportingDispatcher):
    """Reports the broker's refusal for one action kind."""

    def __init__(self, book, kind):
        super().__init__(book)
        self.kind = kind

    def __call__(self, *, allow_entries=True, allow_dispatch=True):
        if not allow_dispatch:
            return {'agent_called': False, 'reason': 'FRESH_BROKER_OBSERVATION_REQUIRED'}
        action = self.book.claim(allow_entries=allow_entries)
        if not action:
            return {'agent_called': False, 'reason': 'NO_DISPATCHABLE_ACTION'}
        status = 'rejected' if action['kind'] == self.kind else 'submitted'
        self.book.report(action['action_id'], {'status': status})
        self.reported.append(action['action_id'])
        return {'agent_called': True, 'action_id': action['action_id'], 'status': status}


class PartialEntryTests(PartialFlow):
    def test_partial_fill_protects_actual_quantity_and_cancels_remainder(self):
        entry = self.make_entry()
        self.assertEqual(self.payload(entry)['parameters']['quantity'], '2')
        submitted = self.order_for(entry, filled=0, state='confirmed')
        self.step([submitted])

        first = self.execution('exec-entry-1', 1)
        self.pay_executions([first])
        partial = self.order_for(entry, filled=1, state='partially_filled',
                                 executions=[first])
        self.now += timedelta(seconds=5)
        result = self.step([partial], qty='1')

        self.assertFalse(result.get('errors'),
                         'a normal partial tranche is not an error')
        self.assertIsNotNone(self.action('cancel'),
                             'the unfilled buy remainder is cancelled')
        protection = self.action('protect')
        self.assertIsNotNone(protection, 'the filled share is protected')
        parameters = self.payload(protection)['parameters']
        self.assertEqual(parameters['quantity'], '1')
        self.assertEqual(parameters['type'], 'stop_market')
        self.assertEqual(parameters['stop_price'], '39.60')
        self.assertEqual(parameters['side'], 'sell')
        # Protection belongs to the same instance and the same trade.
        owner = self.feedback.owner(self.payload(protection)['ref_id'])
        self.assertEqual(owner['instance_id'], 'alpha')
        self.assertEqual(owner['trade_id'], entry['trade_id'])
        # Accounting is the execution the broker reported, once.
        fills = self.accounting.fills('alpha')
        self.assertEqual([(item['side'], item['quantity']) for item in fills],
                         [('buy', '1')])
        self.assertEqual(self.book.state()['trades'][entry['trade_id']]['phase'],
                         'NEEDS_PROTECTION')
        # The broker order that carries the protection keeps the same exact
        # reference, so a later receipt resolves to the owning instance rather
        # than to a stranger. (The worker-side exact order-id alias is exercised
        # by test_management_binding and the native-operation suites.)
        resting = self.order_for(protection, filled=0, state='confirmed')
        self.assertEqual(resting['ref_id'], self.payload(protection)['ref_id'])
        self.step([resting], qty='1', ticks=2)
        owner = self.feedback.owner(resting['ref_id'])
        self.assertEqual((owner['instance_id'], owner['trade_id']),
                         ('alpha', entry['trade_id']))

    def test_a_submitted_protection_is_not_reported_as_covered(self):
        entry = self.make_entry()
        self.step([self.order_for(entry, filled=0, state='confirmed')])
        first = self.execution('exec-entry-1', 2)
        self.pay_executions([first])
        self.step([self.order_for(entry, filled=2, state='filled',
                                  executions=[first])], qty='2')
        protection = self.action('protect')
        self.assertIsNotNone(protection)
        covered = [item['kind'] for item in self.feedback.events('alpha')]
        self.assertNotIn('protection_covered', covered,
                         'a submitted stop is not an observed covering order')
        # Broker confirms the stop with no execution: now it covers.
        confirmed = self.order_for(protection, filled=0, state='confirmed')
        self.step([self.snap_order(entry), confirmed], qty='2', ticks=2)
        covered = [item['kind'] for item in self.feedback.events('alpha')]
        self.assertIn('protection_covered', covered)

class PartialExitTests(PartialFlow):
    def test_a_partial_exit_does_not_release_until_the_trade_is_really_flat(self):
        entry, protection, entry_order, resting = self.full_entry_protected()
        self.now += timedelta(minutes=5)
        self.step([entry_order, resting], qty='2', bid='38.90', ask='38.95', ticks=2)
        cancelled = self.order_for(protection, filled=0, state='canceled')
        self.step([entry_order, cancelled], qty='2', bid='38.90', ask='38.95', ticks=3)
        exit_action = self.action('exit')
        self.assertIsNotNone(exit_action)

        # The exit fills one share first: the trade is smaller, not closed.
        sell_one = self.execution('exec-exit-1', 1, price='38.90')
        self.delivered.add(sell_one['id'])
        self.cash += Decimal('38.90')
        partial_exit = self.order_for(exit_action, filled=1, state='partially_filled',
                                      executions=[sell_one])
        self.step([entry_order, cancelled, partial_exit], qty='1',
                  bid='38.90', ask='38.95', ticks=2)
        self.assertEqual(self.accounting.instance_summary('alpha')['open_quantity'], '1')
        self.assertEqual(self.leases.owner('DEMO'), 'alpha',
                         'a partly sold position still owns its symbol')
        self.assertEqual(self.money.status()['open_cash'], '80.00')

        # The remainder fills and the broker shows no position and no resting
        # orders: only now may money and symbol be released.
        sell_two = self.execution('exec-exit-2', 1, price='38.90')
        self.delivered.add(sell_two['id'])
        self.cash += Decimal('38.90')
        full_exit = self.order_for(exit_action, filled=2, state='filled',
                                   executions=[sell_one, sell_two])
        flat_entry = self.order_for(entry, filled=2, state='filled',
                                    executions=[self.entry_execution])
        self.step([flat_entry, cancelled, full_exit], qty='0',
                  bid='38.90', ask='38.95', ticks=4)
        self.assertEqual(self.book.state()['trades'][entry['trade_id']]['phase'],
                         'FLAT_VERIFIED')
        self.assertIsNone(self.leases.owner('DEMO'))
        self.assertEqual(self.money.status()['open_cash'], '0')
        self.assertEqual(self.accounting.instance_summary('alpha')['open_quantity'],
                         '0')
        kinds = [item['kind'] for item in self.feedback.events('alpha')]
        self.assertIn('flat', kinds)
        # Three executions were delivered; each is booked once.
        self.assertEqual(len(self.accounting.fills('alpha')), 3)


class ProtectionDeadlineTests(PartialFlow):
    def test_the_deadline_runs_from_the_unprotected_fill_not_from_now(self):
        entry, protection, entry_order, resting, first = self.protected_one_share()
        second_at = self.now + timedelta(seconds=5)
        second = self.execution('exec-entry-2', 1, at=second_at)
        self.pay_executions([second])
        self.now = second_at + timedelta(seconds=1)
        late = self.order_for(entry, filled=2, state='partially_filled',
                              executions=[first, second])

        # 29 seconds after the real fill: still inside the window.
        self.now = second_at + timedelta(seconds=29)
        self.step([late, resting], qty='2')
        self.assertFalse(any(item.startswith('PROTECTION_WAIT_EXCEEDED')
                             for item in self.book.state()['issues']))
        # 30 seconds after the real fill: the window closes even though this
        # observation is fresh.
        self.now = second_at + timedelta(seconds=30)
        result = self.step([late, resting], qty='2')
        self.assertFalse(result.get('errors'))
        self.assertTrue(any(item.startswith('PROTECTION_WAIT_EXCEEDED')
                            for item in self.book.state()['issues']),
                        self.book.state()['issues'])
        trade = self.book.state()['trades'][entry['trade_id']]
        self.assertTrue(trade['exit_requested'])
        self.assertEqual(trade['unprotected_since'], second_at.isoformat())


class ProtectionFailureTests(PartialFlow):
    def test_a_rejected_protection_never_claims_coverage_and_forces_the_exit(self):
        entry = self.make_entry()
        self.step([self.order_for(entry, filled=0, state='confirmed')])
        fill = self.execution('exec-entry-1', 2)
        self.pay_executions([fill])
        self.now += timedelta(seconds=5)
        self.supervisor.dispatcher = RejectOnKindDispatcher(self.book, 'protect')
        self.step([self.order_for(entry, filled=2, state='filled',
                                  executions=[fill])], qty='2', ticks=3)
        kinds = [item['kind'] for item in self.feedback.events('alpha')]
        self.assertNotIn('protection_covered', kinds)
        issues = self.book.state()['issues']
        self.assertTrue(any(item.startswith('PROTECTION_REJECTED')
                            for item in issues), issues)
        trade = self.book.state()['trades'][entry['trade_id']]
        self.assertTrue(trade['exit_requested'])
        exits = [item for item in self.book.actions() if item['kind'] == 'exit']
        self.assertTrue(exits, 'a rejected protection must really queue the exit')
        for item in exits:
            self.assertEqual(json.loads(item['payload'])['parameters']['quantity'],
                             '2', 'the exit is sized by the real position')

    def test_a_partial_protection_fill_during_cancel_updates_the_real_quantity(self):
        entry, protection, entry_order, resting = self.full_entry_protected()
        # The market breaches the stop, so the resting stop must be cancelled
        # before any exit; an accepted cancel is not terminal.
        self.now += timedelta(minutes=5)
        self.step([entry_order, resting], qty='2', bid='38.90', ask='38.95')
        self.assertIsNone(self.action('exit'),
                          'no exit while the protection cancel is only accepted')
        # While the cancel is pending, the stop executes for one share.
        sold = self.execution('exec-protect-1', 1, price='39.60')
        sold['id'] = 'exec-protect-1'
        # A stop sale adds cash.
        self.delivered.add(sold['id'])
        self.cash += Decimal('39.60')
        cancelled = self.order_for(protection, filled=1, state='partially_filled',
                                   executions=[sold])
        self.step([entry_order, cancelled], qty='1', bid='38.90', ask='38.95',
                  ticks=2)
        trade = self.book.state()['trades'][entry['trade_id']]
        self.assertEqual(trade['quantity'], '1',
                         'the book follows the broker position, not the old size')
        self.assertIsNone(self.action('exit'),
                          'the partly filled stop is not terminal yet')
        # The broker finally reports the stop cancelled after its one-share
        # execution; only then may the remaining share be exited.
        terminal = self.order_for(protection, filled=1, state='canceled',
                                  executions=[sold])
        self.step([entry_order, terminal], qty='1', bid='38.90', ask='38.95',
                  ticks=2)
        exits = [item for item in self.book.actions() if item['kind'] == 'exit']
        self.assertTrue(exits, 'the remaining share must really be exited')
        for item in exits:
            quantity = json.loads(item['payload'])['parameters']['quantity']
            self.assertEqual(quantity, '1',
                             'the exit may only sell what is still available')
        self.assertEqual(self.accounting.instance_summary('alpha')['open_quantity'],
                         '1')


class FailingPlugin(CountingPlugin):
    def decide(self, request):
        raise RuntimeError('STRATEGY_FAILED')


class MultiInstanceTests(PartialFlow):
    def test_a_failing_instance_does_not_stop_another_instances_management(self):
        entry, protection, entry_order, resting = self.full_entry_protected()
        self.registry.register('beta', manifest('beta'), instance_budget(),
                               mode='shadow', enabled=True)
        beta = InstanceRuntime(self.registry, self.registry.instance('beta'),
                               runner=DecisionRunner(FailingPlugin()),
                               source_fingerprint='b' * 64,
                               clock=lambda: self.now)
        self.addCleanup(beta.close)
        self.supervisor = self.build_supervisor(self.runtime, beta)

        self.now += timedelta(minutes=5)
        results = [self.step([entry_order, resting], qty='2', bid='38.90',
                             ask='38.95') for _ in range(3)]
        reasons = [(item.get('instance_id'), item.get('reason'))
                   for result in results for item in result.get('errors') or []]
        self.assertIn(('beta', 'STRATEGY_TASK_RUNNER_FAILED'), reasons,
                      'the failing strategy is reported, not hidden')
        # Alpha's management continues from broker facts despite beta failing.
        self.assertIsNotNone(self.action('cancel'),
                             'alpha still retires its stop on the breach')
        trade = self.book.state()['trades'][entry['trade_id']]
        self.assertTrue(trade['exit_requested'])
        self.assertEqual(self.book.state()['trades'][entry['trade_id']]['phase'],
                         'EXIT_REQUESTED')

        # Finish the management path while the other instance keeps failing.
        cancelled = self.order_for(protection, filled=0, state='canceled')
        results = [self.step([entry_order, cancelled], qty='2', bid='38.90',
                             ask='38.95') for _ in range(4)]
        exit_action = self.action('exit')
        self.assertIsNotNone(exit_action,
                             'alpha reaches a real exit while beta stays broken')
        self.assertEqual(json.loads(exit_action['payload'])['parameters']['quantity'],
                         '2')
        self.assertFalse([item for item in self.book.actions()
                          if item['kind'] == 'entry'
                          and item['trade_id'] != entry['trade_id']],
                         'a failed instance cannot open unrelated risk')


class ReceiptIntegrityTests(PartialFlow):
    def test_duplicate_and_reordered_receipts_book_each_execution_once(self):
        entry = self.make_entry()
        self.step([self.order_for(entry, filled=0, state='confirmed')])
        self.now += timedelta(seconds=5)
        late = self.execution('exec-entry-1', 1, at=self.now - timedelta(seconds=1),
                              fees='0.01')
        early = self.execution('exec-entry-2', 1, at=self.now - timedelta(seconds=4))
        self.pay_executions([late, early])
        # The provider lists the two executions in reverse chronological order.
        filled = self.order_for(entry, filled=2, state='filled',
                                executions=[late, early])
        self.step([filled], qty='2', ticks=2)
        first = [(item['fill_id'], item['quantity'], item['fees'])
                 for item in self.accounting.fills('alpha')]
        self.assertEqual(len(first), 2, first)
        self.assertEqual(sorted(item[1] for item in first), ['1', '1'])
        self.assertEqual(sorted(item[2] for item in first), ['0', '0.01'],
                         'each fee is booked with its own execution')
        summary = self.accounting.instance_summary('alpha')
        self.assertEqual(summary['open_quantity'], '2')

        # The same receipts arrive again in a later, otherwise clean snapshot.
        self.now += timedelta(seconds=3)
        self.step([filled], qty='2', ticks=2)
        again = [(item['fill_id'], item['quantity'], item['fees'])
                 for item in self.accounting.fills('alpha')]
        self.assertEqual(again, first, 'a replayed execution is never re-booked')
        self.assertEqual(self.accounting.instance_summary('alpha')['open_quantity'],
                         '2')
        self.assertEqual(self.money.status()['open_cash'], '80.00')
        self.assertFalse([item for item in self.book.state()['issues']
                          if 'EXECUTION' in item])

    def test_a_cumulative_quantity_is_not_accepted_as_a_new_execution(self):
        entry = self.make_entry()
        self.step([self.order_for(entry, filled=0, state='confirmed')])
        first = self.execution('exec-entry-1', 1)
        self.pay_executions([first])
        # The provider says two shares are filled but reports only the first
        # execution. The adapter refuses to turn an average into a fill.
        bad = self.order_for(entry, filled=2, state='partially_filled',
                             executions=[first])
        with self.assertRaises(ValueError):
            self.step([bad], qty='1')
        self.assertEqual(self.accounting.fills('alpha'), [])


class ReconstructionTests(PartialFlow):
    def test_partial_state_survives_a_new_book_and_supervisor(self):
        entry, protection, entry_order, resting, first = self.protected_one_share()
        path = Path(self.temp.name) / 'partial.sqlite'
        policy = json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        # A second owner connection, exactly as a restarted process would open
        # it. The first connection stays open, idle, so nothing claims the
        # second view comes from the same in-memory object.
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
        dispatcher = ReportingDispatcher(book)
        supervisor = LiveSupervisor(book, dispatcher=dispatcher,
                                    process_strategy=False, agent_mode='review',
                                    owner_source='f' * 64,
                                    instance_supervisor=InstanceSupervisor([runtime]),
                                    admission=admission, accounting=accounting)
        self.supervisor, self.book, self.dispatcher = supervisor, book, dispatcher
        self.money, self.leases = money, leases
        self.feedback, self.accounting = feedback, accounting

        # Same broker facts, fresh process: the position, its stop and the
        # booked execution are read back, not re-created.
        result = self.step([entry_order, resting], qty='1', ticks=2)
        self.assertFalse(result.get('errors'))
        self.assertEqual(self.accounting.instance_summary('alpha')['open_quantity'],
                         '1')
        self.assertEqual(len(self.accounting.fills('alpha')), 1)
        protections = [item for item in self.book.actions()
                       if item['kind'] == 'protect']
        self.assertEqual(len(protections), 1, 'no duplicate stop after restart')
        self.assertEqual(json.loads(protections[0]['payload'])['parameters']['quantity'],
                         '1')
        self.assertEqual(self.leases.owner('DEMO'), 'alpha')
        self.assertEqual(self.money.status()['open_cash'], '80.00')
        self.assertEqual(self.book.state()['trades'][entry['trade_id']]['phase'],
                         'PROTECTED')


class LateFillTests(PartialFlow):
    def test_a_late_second_fill_opens_a_new_tranche_on_real_execution_time(self):
        entry, protection, entry_order, resting, first = self.protected_one_share()
        # The remainder cancel is still pending; the broker then reports a
        # second, distinct execution on the same order.
        second_at = self.now + timedelta(seconds=8)
        second = self.execution('exec-entry-2', 1, at=second_at)
        self.pay_executions([second])
        self.now = second_at + timedelta(seconds=2)
        late = self.order_for(entry, filled=2, state='partially_filled',
                              executions=[first, second])
        self.step([late, resting], qty='2')
        self.assertIsNone(self.action('exit'),
                          'no replacement exit while the old stop is still resting')

        trade = self.book.state()['trades'][entry['trade_id']]
        self.assertEqual(trade['quantity'], '2')
        self.assertEqual(trade['unprotected_new_entry_execution_at'],
                         second_at.isoformat())
        self.assertEqual(trade['unprotected_since'], second_at.isoformat(),
                         'the exposure clock is the real execution, not now')
        self.assertEqual(trade['unprotected_timing_basis'],
                         'additional_actual_entry_execution_old_tranche_still_covered')
        # The old one-share stop cannot be left as if it covered two shares.
        # The pending entry-remainder cancel must not stand in for cancelling
        # the stop: each target needs its own action.
        cancels = [item for item in self.book.actions() if item['kind'] == 'cancel']
        targets = {json.loads(item['payload'])['target_order_id'] for item in cancels}
        self.assertEqual(targets, {entry_order['id'], resting['id']},
                         'one cancel per distinct broker order')
        self.assertEqual([item['kind'] for item in self.feedback.events('alpha')]
                         .count('protection_covered'), 1,
                         'the late tranche is not reported as covered')

        # Both cancel actions must actually reach the dispatcher before the
        # broker is allowed to report their targets terminal. The dispatcher
        # claims one action per tick, so give it the ticks it needs.
        stop_cancel = next(item for item in cancels
                           if json.loads(item['payload'])['target_order_id'] == resting['id'])
        for _ in range(3):
            self.step([late, resting], qty='2')
        claimed = {item['id']: item['state'] for item in self.book.actions()
                   if item['kind'] == 'cancel'}
        self.assertIn(claimed[stop_cancel['id']], {'RUNNING', 'REPORTED'},
                      'the stop-target cancel was dispatched')
        self.assertEqual(
            sum(1 for item in self.book.actions() if item['kind'] == 'cancel'), 2,
            'repeated reconciliation reuses the same per-target cancels')

        # Only now may the old stop reach a terminal cancel, and only then may a
        # replacement for the real quantity (or a safe exit) be dispatched.
        cancelled = self.order_for(protection, filled=0, state='canceled')
        terminal_entry = self.order_for(entry, filled=2, state='canceled',
                                        executions=[first, second])
        self.step([terminal_entry, cancelled], qty='2', ticks=3)
        post = self.book.actions()
        replacements = [item for item in post
                        if item['kind'] == 'protect'
                        and json.loads(item['payload'])['parameters']['quantity'] == '2']
        exits = [item for item in post if item['kind'] == 'exit']
        # The book may replace the stop for the real quantity or, because the
        # retired stop is now terminal without covering the trade, exit. Both
        # are safe answers; neither may size more than the broker's position.
        self.assertTrue(replacements or exits,
                        'the gap must end in a replacement stop or a safe exit')
        for item in replacements + exits:
            self.assertEqual(json.loads(item['payload'])['parameters']['quantity'], '2')
        trade = self.book.state()['trades'][entry['trade_id']]
        self.assertIn(trade['phase'], {'NEEDS_PROTECTION', 'EXIT_REQUESTED'})
        self.assertEqual(self.accounting.instance_summary('alpha')['open_quantity'], '2')
