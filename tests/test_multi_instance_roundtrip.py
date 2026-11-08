"""Two instances through the real control loop and a fake broker.

Chain under test: advisory proposal -> account admission -> execution intent ->
entry action -> fill -> protection -> cancel terminal -> flat -> settlement,
with per-instance ownership and accounting. The components are the real ones;
only the broker is simulated, exactly as the review asked.

Synthetic database and synthetic snapshots. No broker call, no order.
"""
import json
import sqlite3
import tempfile
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest import TestCase

from entry_authority_fixture import TestExecutionBook as ExecutionBook
from martin_luk.live_session import LiveSupervisor
from test_broker import FakeBroker, order, position, snapshot
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

AT = '2026-10-16T14:00:00+00:00'
ACCOUNT = {'max_open_cash': Decimal('500'), 'max_open_notional': Decimal('500'),
           'max_open_planned_loss': Decimal('10.00'), 'max_concurrent_trades': 4}
POLICY = {'max_position_notional_usd': '100', 'max_planned_loss_per_trade_usd': '2.50'}


def manifest(plugin_id):
    return PluginManifest(plugin_id=plugin_id, version='1.0', state_schema='counter-1',
                          config={'step': '1', 'cost_buffer_per_share': '0.05'},
                          data_requirements=('quotes',),
                          capabilities=('advisory-entry',), max_inflight=64)


def budget():
    return InstanceBudget(max_cash=Decimal('100'), max_planned_loss=Decimal('2.50'),
                          max_notional=Decimal('100'), max_concurrent=1)


class RoundTripTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'roundtrip.sqlite'
        self.now = datetime.fromisoformat(AT)
        self.policy = json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        self.book = ExecutionBook(self.path, self.policy, simulation=True,
                                  clock=lambda: self.now)
        self.addCleanup(self.book.close)
        db = sqlite3.connect(self.path, isolation_level=None)
        self.addCleanup(db.close)
        self.db = db
        self.registry = InstanceRegistry(db, clock=lambda: self.now)
        self.registry.register('alpha', manifest('alpha'), budget(), mode='shadow', enabled=True)
        self.registry.register('beta', manifest('beta'), budget(), mode='shadow', enabled=True)
        self.runtimes = []
        for instance_id in ('alpha', 'beta'):
            runtime = InstanceRuntime(self.registry, self.registry.instance(instance_id),
                                      runner=DecisionRunner(CountingPlugin(proposal_at=1)),
                                      source_fingerprint='f' * 64, clock=lambda: self.now)
            self.addCleanup(runtime.close)
            self.runtimes.append(runtime)
        self.money = AccountBudgetAuthority(db, ACCOUNT, clock=lambda: self.now)
        self.addCleanup(self.money.close)
        self.leases = SymbolLease(db, clock=lambda: self.now)
        self.addCleanup(self.leases.close)
        self.feedback = ExecutionAttribution(db, clock=lambda: self.now)
        self.ledger = InstanceAccounting(db, clock=lambda: self.now)
        self.admission = AdmissionService(budget=self.money, leases=self.leases,
                                          attribution=self.feedback, policy=POLICY,
                                          registry=self.registry, clock=lambda: self.now)
        self.supervisor = LiveSupervisor(self.book, process_strategy=False,
                                         agent_mode='review', owner_source='f' * 64,
                                         instance_supervisor=InstanceSupervisor(self.runtimes),
                                         admission=self.admission)

    def flat(self, trade_id, **overrides):
        observed = self.now.isoformat()

        class Authority:
            def flat_evidence(self, requested):
                evidence = {'source': 'execution_book', 'account_key': 'acct',
                            'trade_id': requested, 'phase': 'FLAT_VERIFIED',
                            'remaining_quantity': '0', 'open_orders': 0,
                            'pending_approvals': 0, 'unknown_intents': 0,
                            'active_actions': 0,
                            'observed_at': observed}
                evidence.update(overrides)
                return evidence

        return Authority()

    # -- fake broker ------------------------------------------------------
    def snap(self, orders=None, qty='0', held='0'):
        fake = FakeBroker()
        fake.orders = orders or []
        fake.positions = [] if qty == '0' else [position(qty, held)]
        value = snapshot(fake, self.now.isoformat())
        value['synthetic'] = True
        return value

    def event(self, event_id, symbol):
        at = self.now.isoformat()
        return {'id': event_id, 'kind': 'quote', 'at': at, 'synthetic_input': True,
                'source_sha256': 'source-' + event_id,
                'data': {'symbol': symbol, 'bid': '39.99', 'ask': '40.00',
                         'bid_at': at, 'ask_at': at, 'observed_at': at}}

    def bound(self, action, *, filled='0', state='confirmed', oid=None):
        payload = json.loads(action['payload'])
        parameters = payload['parameters']
        raw = order(oid or payload['action_id'], parameters['side'],
                    parameters['quantity'], filled, state, parameters.get('stop_price'))
        raw['ref_id'] = payload['ref_id']
        raw['price'] = parameters.get('limit_price')
        raw['type'] = 'limit' if parameters['type'] == 'limit' else 'market'
        raw['market_hours'] = parameters['market_hours']
        raw['time_in_force'] = parameters['time_in_force']
        return raw

    # -- drive ------------------------------------------------------------
    def admit_once(self, symbol='DEMO'):
        """Run control ticks until one instance is admitted for the symbol."""
        for _ in range(8):
            step = self.supervisor.step(self.snap(), [self.event('e-' + symbol, symbol)],
                                        allow_entries=True, allow_dispatch=False)
            admitted = [item for item in step['strategy_admissions']['outcomes']
                        if item.get('admitted')]
            if admitted:
                return admitted[0]
        self.fail('no admission produced')

    def enter(self, intent):
        plan = {'symbol': intent['symbol'], 'quantity': intent['quantity'],
                'entry_ceiling': intent['entry_ceiling'], 'stop': intent['stop_price'],
                'cost_buffer_per_share': '0.05'}
        quote = {'symbol': intent['symbol'], 'bid': '39.99', 'ask': '40.00',
                 'bid_at': self.now.isoformat(), 'ask_at': self.now.isoformat(),
                 'observed_at': self.now.isoformat()}
        return self.book.propose(intent['trade_id'], plan, quote, self.snap(),
                                 evidence={'source_sha256': 'synthetic-fixture',
                                           'synthetic': True})

    def test_admitted_intent_becomes_a_real_entry_action(self):
        intent = self.admit_once()
        result = self.enter(intent)
        self.assertFalse(result['duplicate'])
        self.assertIsNotNone(result['action_id'])
        actions = self.book.actions()
        self.assertTrue(actions)
        entry = next(item for item in actions if item['kind'] == 'entry')
        parameters = json.loads(entry['payload'])['parameters']
        self.assertEqual(parameters['quantity'], str(intent['quantity']))
        self.assertEqual(entry['trade_id'], intent['trade_id'])
        self.assertEqual(self.leases.owner('DEMO'), intent['instance_id'])

    def test_fill_creates_protection_owned_by_the_same_instance(self):
        intent = self.admit_once()
        self.enter(intent)
        entry = next(item for item in self.book.actions() if item['kind'] == 'entry')
        # The claim guard requires the execution identity to be bound first;
        # this test enters through the component API, so bind it here.
        self.feedback.bind(json.loads(entry['payload'])['ref_id'],
                           intent['instance_id'], intent['trade_id'])
        claimed = self.book.claim()
        self.book.report(claimed['action_id'], {'status': 'submitted',
                                                'message': 'not broker proof'})
        self.book.reconcile(self.snap([self.bound(entry, filled=str(intent['quantity']))],
                                      qty=str(intent['quantity'])))
        protections = [item for item in self.book.actions() if item['kind'] == 'protect']
        self.assertTrue(protections, 'a filled entry must produce a protection action')
        self.assertEqual(self.leases.owner('DEMO'), intent['instance_id'])

    def test_contested_symbol_is_admitted_once_and_costs_nothing_elsewhere(self):
        first = self.admit_once()
        self.assertEqual(self.leases.owner('DEMO'), first['instance_id'])
        # A second instance asking for the same symbol is refused by the lease.
        second = [item for item in self.supervisor.step(
            self.snap(), [self.event('e-2', 'DEMO')],
            allow_entries=True, allow_dispatch=False)['strategy_admissions']['outcomes']
            if not item.get('admitted')]
        self.assertTrue(any(item['reason'] == 'ADMISSION_SYMBOL_HELD' for item in second))
        self.assertEqual(self.money.status()['open_trades'], 1)

    def test_fills_and_pnl_are_attributed_to_the_owning_instance(self):
        intent = self.admit_once()
        self.enter(intent)
        owner = intent['instance_id']
        other = 'beta' if owner == 'alpha' else 'alpha'
        self.ledger.record_synthetic_fill(owner, intent['trade_id'], fill_id='f1', side='buy',
                                quantity=str(intent['quantity']), price='40.00',
                                fees='0')
        self.ledger.record_synthetic_fill(owner, intent['trade_id'], fill_id='f2', side='sell',
                                quantity=str(intent['quantity']), price='41.00',
                                fees='0')
        summary = self.ledger.instance_summary(owner)
        self.assertEqual(Decimal(summary['realised_pnl']),
                         Decimal(intent['quantity']) * Decimal('1.00'))
        self.assertEqual(Decimal(self.ledger.instance_summary(other)['realised_pnl']),
                         Decimal('0'))

    def test_settlement_requires_evidence_and_releases_everything(self):
        intent = self.admit_once()
        self.enter(intent)
        # A caller-supplied reason is not proof of a flat account.
        premature = self.admission.settle(intent['instance_id'], intent['trade_id'],
                                          authority=self.flat(intent['trade_id'],
                                                             remaining_quantity='1'))
        self.assertFalse(premature['settled'])
        self.assertEqual(premature['reason'], 'ADMISSION_POSITION_NOT_FLAT')
        self.assertEqual(self.leases.owner('DEMO'), intent['instance_id'])

        still_open = self.admission.settle(intent['instance_id'], intent['trade_id'],
                                           authority=self.flat(intent['trade_id'],
                                                              open_orders=1))
        self.assertFalse(still_open['settled'])
        self.assertEqual(still_open['reason'], 'ADMISSION_TRADE_STILL_ACTIVE')

        self.admission.mark_intent(intent['intent_id'], 'DISPATCHED')
        settled = self.admission.settle(intent['instance_id'], intent['trade_id'],
                                        authority=self.flat(intent['trade_id']))
        self.assertTrue(settled['settled'])
        self.assertIsNone(self.leases.owner('DEMO'))
        self.assertEqual(self.money.status()['open_cash'], '0')
        self.assertEqual(self.money.status()['open_trades'], 0)

    def test_restart_does_not_replay_the_admission(self):
        intent = self.admit_once()
        # A fresh authority over the same database sees the same reservation and
        # the same owner; it must not admit the proposal again.
        money = AccountBudgetAuthority(self.db, ACCOUNT, clock=lambda: self.now)
        self.addCleanup(money.close)
        self.assertEqual(money.status()['open_trades'], 1)
        self.assertEqual(self.leases.owner('DEMO'), intent['instance_id'])


class ControlLoopChainTests(RoundTripTests):
    """The whole chain driven only by the control loop and fake broker input.

    Nothing here calls ``book.propose``, ``record_fill`` or ``admission.settle``
    directly: the loop admits, dispatches, reconciles and settles.
    """

    def drive(self, symbol='DEMO', *, ticks=12):
        """Run control ticks with a broker snapshot this test describes."""
        outcomes = []
        for _ in range(ticks):
            step = self.supervisor.step(self.snap(), [self.event('e-' + symbol, symbol)],
                                        allow_entries=True, allow_dispatch=True)
            outcomes.append(step)
            if any(item.get('dispatched') for item in
                   step['strategy_admissions']['outcomes']):
                return outcomes
        self.fail('the control loop never dispatched an intent')

    def test_the_loop_admits_and_dispatches_without_test_help(self):
        outcomes = self.drive()
        dispatched = [item for step in outcomes
                      for item in step['strategy_admissions']['outcomes']
                      if item.get('dispatched')]
        self.assertTrue(dispatched)
        self.assertTrue(self.book.actions())
        entry = next(a for a in self.book.actions() if a['kind'] == 'entry')
        self.assertEqual(entry['state'], 'READY')

    def test_the_intent_is_durable_and_marked_dispatched(self):
        self.drive()
        rows = self.db.execute(
            'SELECT intent_id,ref_id,state,instance_id,trade_id FROM execution_intents'
        ).fetchall()
        self.assertEqual(len(rows), 1)
        intent_id, ref_id, state, instance_id, trade_id = rows[0]
        self.assertEqual(state, 'DISPATCHED')
        self.assertEqual(self.feedback.owner(ref_id)['instance_id'], instance_id)
        self.assertEqual(self.leases.owner('DEMO'), instance_id)
        # The bound identity is the reference the book actually submitted with,
        # so a broker receipt carrying it can be routed back to the instance.
        payload = json.loads(next(a['payload'] for a in self.book.actions()
                                  if a['kind'] == 'entry'))
        self.assertTrue(self.feedback.owner(payload['ref_id']))
        self.assertEqual(self.feedback.owner(payload['ref_id'])['instance_id'], instance_id)
        # A dispatched intent is no longer offered to a dispatcher.
        self.assertEqual(self.admission.pending_intents(), [])

    def test_an_intent_held_back_from_dispatch_is_sent_on_a_later_tick(self):
        # First admit with dispatch withheld.
        admitted = None
        for _ in range(8):
            step = self.supervisor.step(self.snap(), [self.event('e-DEMO', 'DEMO')],
                                        allow_entries=True, allow_dispatch=False)
            found = [item for item in step['strategy_admissions']['outcomes']
                     if item.get('admitted')]
            if found:
                admitted = found[0]
                break
        self.assertIsNotNone(admitted)
        self.assertFalse(admitted.get('dispatched', False))
        self.assertEqual(self.book.actions(), [])
        self.assertEqual(len(self.admission.pending_intents()), 1)

        # Now allow dispatch: the pending intent must be consumed without a
        # second proposal being produced.
        step = self.supervisor.step(self.snap(), [self.event('e-2', 'DEMO')],
                                    allow_entries=True, allow_dispatch=True)
        sent = [item for item in step['strategy_admissions']['outcomes']
                if item.get('dispatched')]
        self.assertTrue(sent)
        self.assertTrue(self.book.actions())
        self.assertEqual(self.admission.pending_intents(), [])


class DefaultConnectionTests(RoundTripTests):
    """R4-01: the real book connection must not be left mid-transaction.

    The earlier fixtures opened a second connection with ``isolation_level=None``,
    which hid the difference from production. This one uses the book's own
    connection, exactly as the daemon does.
    """

    def setUp(self):
        super().setUp()
        # Rebuild the account authority on the book connection itself.
        self.money.close()
        self.leases.close()
        from trading_infra.budget import AccountBudgetAuthority as Budget
        from trading_infra.symbol_lease import SymbolLease as Lease
        self.money = Budget(self.book.db, ACCOUNT, clock=lambda: self.now)
        self.addCleanup(self.money.close)
        self.leases = Lease(self.book.db, clock=lambda: self.now)
        self.addCleanup(self.leases.close)
        self.feedback = ExecutionAttribution(self.book.db, clock=lambda: self.now)
        self.admission = AdmissionService(budget=self.money, leases=self.leases,
                                          attribution=self.feedback, policy=POLICY,
                                          registry=self.registry, clock=lambda: self.now)
        self.supervisor = LiveSupervisor(self.book, process_strategy=False,
                                         agent_mode='review', owner_source='f' * 64,
                                         instance_supervisor=InstanceSupervisor(self.runtimes),
                                         admission=self.admission)

    def test_two_ticks_leave_no_open_transaction(self):
        self.assertNotEqual(self.book.db.isolation_level, None)
        for _ in range(3):
            self.supervisor.step(self.snap(), [self.event('e-DEMO', 'DEMO')],
                                 allow_entries=True, allow_dispatch=True)
            self.assertFalse(self.book.db.in_transaction,
                             'the control loop left a transaction open')
        self.assertTrue(self.book.actions())
        # Reconciliation must still work afterwards.
        self.book.reconcile(self.snap())

    def test_a_rejected_dispatch_releases_instead_of_holding_forever(self):
        # A quote the book refuses makes propose raise before any action exists.
        for _ in range(8):
            step = self.supervisor.step(self.snap(), [self.event('e-DEMO', 'DEMO')],
                                        allow_entries=True, allow_dispatch=False)
            if any(item.get('admitted') for item in step['strategy_admissions']['outcomes']):
                break
        self.assertEqual(len(self.admission.pending_intents()), 1)
        # Dispatch with a quote the book will reject.
        bad = {'symbol': 'DEMO', 'bid': '41.99', 'ask': '42.00',
               'bid_at': self.now.isoformat(), 'ask_at': self.now.isoformat(),
               'observed_at': self.now.isoformat()}
        outcome = self.supervisor._dispatch_intent(
            {**self.admission.pending_intents()[0]}, self.snap(), {'DEMO': bad})
        self.assertFalse(outcome['dispatched'])
        # No action exists, so the reservation is provably unsent and released.
        self.assertTrue(outcome.get('released'))
        self.assertIsNone(self.leases.owner('DEMO'))
        self.assertEqual(self.money.status()['open_cash'], '0')
        self.assertEqual(self.admission.dispatching_intents(), [])

    def test_a_crash_after_propose_is_adopted_not_reproposed(self):
        for _ in range(8):
            step = self.supervisor.step(self.snap(), [self.event('e-DEMO', 'DEMO')],
                                        allow_entries=True, allow_dispatch=False)
            if any(item.get('admitted') for item in step['strategy_admissions']['outcomes']):
                break
        intent = self.admission.pending_intents()[0]
        quote = {'symbol': 'DEMO', 'bid': '39.99', 'ask': '40.00',
                 'bid_at': self.now.isoformat(), 'ask_at': self.now.isoformat(),
                 'observed_at': self.now.isoformat()}
        # Pretend the process died between propose and confirmation.
        self.admission.mark_intent(intent['intent_id'], 'DISPATCHING')
        plan = {'symbol': intent['symbol'], 'quantity': intent['quantity'],
                'entry_ceiling': intent['entry_ceiling'], 'stop': intent['stop_price'],
                'cost_buffer_per_share': '0.05'}
        self.book.propose(intent['trade_id'], plan, quote, self.snap(),
                          evidence={'source_sha256': intent['intent_id'],
                                    'synthetic': self.book.simulation})
        self.assertEqual(len([a for a in self.book.actions() if a['kind'] == 'entry']), 1)

        # The next tick adopts it: no second entry, identity bound.
        self.supervisor.step(self.snap(), [self.event('e-2', 'DEMO')],
                             allow_entries=True, allow_dispatch=True)
        entries = [a for a in self.book.actions() if a['kind'] == 'entry']
        self.assertEqual(len(entries), 1)
        payload = json.loads(entries[0]['payload'])
        self.assertTrue(self.feedback.owner(payload['ref_id']))
        self.assertEqual(self.admission.dispatching_intents(), [])

    def test_an_unbound_entry_action_cannot_be_claimed_or_admitted_again(self):
        from unittest.mock import patch
        # Hold dispatch so an intent exists, then fail every bind attempt.
        for _ in range(8):
            step = self.supervisor.step(self.snap(), [self.event('e-DEMO', 'DEMO')],
                                        allow_entries=True, allow_dispatch=False)
            if any(item.get('admitted') for item in step['strategy_admissions']['outcomes']):
                break

        import sqlite3 as _sqlite
        with patch.object(self.feedback, 'bind',
                          side_effect=_sqlite.OperationalError('injected bind failure')):
            step = self.supervisor.step(self.snap(), [self.event('e-2', 'DEMO')],
                                        allow_entries=True, allow_dispatch=True)
        outcomes = step['strategy_admissions']['outcomes']
        unbound = [item for item in outcomes if item.get('execution_ref_bound') is False]
        self.assertTrue(unbound)
        self.assertFalse(any(item.get('dispatched') for item in unbound))
        # The action exists but is not claimable: its identity has no owner.
        entries = [a for a in self.book.actions() if a['kind'] == 'entry']
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]['state'], 'READY')
        self.assertIsNone(self.book.claim(allow_entries=True))

    def test_recovery_binds_before_declaring_dispatch_and_retries(self):
        from unittest.mock import patch
        import sqlite3 as _sqlite
        for _ in range(8):
            step = self.supervisor.step(self.snap(), [self.event('e-DEMO', 'DEMO')],
                                        allow_entries=True, allow_dispatch=False)
            if any(item.get('admitted') for item in step['strategy_admissions']['outcomes']):
                break
        intent = self.admission.pending_intents()[0]
        quote = {'symbol': 'DEMO', 'bid': '39.99', 'ask': '40.00',
                 'bid_at': self.now.isoformat(), 'ask_at': self.now.isoformat(),
                 'observed_at': self.now.isoformat()}
        self.admission.mark_intent(intent['intent_id'], 'DISPATCHING')
        plan = {'symbol': intent['symbol'], 'quantity': intent['quantity'],
                'entry_ceiling': intent['entry_ceiling'], 'stop': intent['stop_price'],
                'cost_buffer_per_share': '0.05'}
        self.book.propose(intent['trade_id'], plan, quote, self.snap(),
                          evidence={'source_sha256': intent['intent_id'],
                                    'synthetic': self.book.simulation})

        with patch.object(self.feedback, 'bind',
                          side_effect=_sqlite.OperationalError('injected bind failure')):
            self.supervisor.step(self.snap(), [self.event('e-3', 'DEMO')],
                                 allow_entries=True, allow_dispatch=True)
        # Still recoverable, not declared done.
        self.assertEqual(len(self.admission.dispatching_intents()), 1)
        self.assertEqual(self.book.actions()[0]['kind'], 'entry')

        step = self.supervisor.step(self.snap(), [self.event('e-4', 'DEMO')],
                                    allow_entries=True, allow_dispatch=True)
        self.assertEqual(self.admission.dispatching_intents(), [])
        entries = [a for a in self.book.actions() if a['kind'] == 'entry']
        self.assertEqual(len(entries), 1, 'recovery must never propose a second order')
        ref = json.loads(entries[0]['payload'])['ref_id']
        self.assertTrue(self.feedback.owner(ref))

    def test_an_unbound_action_surfaces_as_a_tick_error_and_blocks_entries(self):
        from unittest.mock import patch
        import sqlite3 as _sqlite
        for _ in range(8):
            step = self.supervisor.step(self.snap(), [self.event('e-DEMO', 'DEMO')],
                                        allow_entries=True, allow_dispatch=False)
            if any(item.get('admitted') for item in step['strategy_admissions']['outcomes']):
                break
        with patch.object(self.feedback, 'bind',
                          side_effect=_sqlite.OperationalError('injected bind failure')):
            step = self.supervisor.step(self.snap(), [self.event('e-2', 'DEMO')],
                                        allow_entries=True, allow_dispatch=True)
        reasons = [item.get('reason') for item in step['errors']]
        self.assertIn('EXECUTION_REFERENCE_UNBOUND', reasons)
        self.assertTrue(all(item.get('scope') == 'STRATEGY_EXECUTION'
                            for item in step['errors']
                            if item.get('reason') == 'EXECUTION_REFERENCE_UNBOUND'))
        self.assertTrue(step['new_entries_blocked'])
        # The claim guard still holds the order back.
        self.assertIsNone(self.book.claim(allow_entries=True))

    def test_the_error_clears_once_binding_succeeds(self):
        from unittest.mock import patch
        import sqlite3 as _sqlite
        for _ in range(8):
            step = self.supervisor.step(self.snap(), [self.event('e-DEMO', 'DEMO')],
                                        allow_entries=True, allow_dispatch=False)
            if any(item.get('admitted') for item in step['strategy_admissions']['outcomes']):
                break
        with patch.object(self.feedback, 'bind',
                          side_effect=_sqlite.OperationalError('injected bind failure')):
            self.supervisor.step(self.snap(), [self.event('e-2', 'DEMO')],
                                 allow_entries=True, allow_dispatch=True)
        step = self.supervisor.step(self.snap(), [self.event('e-3', 'DEMO')],
                                    allow_entries=True, allow_dispatch=True)
        reasons = [item.get('reason') for item in step['errors']]
        self.assertNotIn('EXECUTION_REFERENCE_UNBOUND', reasons)
        self.assertEqual(self.admission.dispatching_intents(), [])


class ReceiptRoundTripTests(TestCase):
    """The whole round trip runs through the control loop and fake broker only.

    Nothing here calls ``enter``, ``book.propose``, ``ledger.record_fill`` or
    ``admission.settle``: the loop admits, dispatches, reconciles, imports the
    receipts, accounts them and releases on authoritative evidence. Every
    component shares the book's own connection, exactly as production does.
    """

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'receipts.sqlite'
        self.now = datetime.fromisoformat(AT)
        self.policy = json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        self.book = ExecutionBook(self.path, self.policy, simulation=True,
                                  clock=lambda: self.now)
        self.addCleanup(self.book.close)
        self.db = self.book.db
        self.registry = InstanceRegistry(self.db, clock=lambda: self.now)
        self.registry.register('alpha', manifest('alpha'), budget(), mode='shadow',
                               enabled=True)
        self.runtimes = []
        runtime = InstanceRuntime(self.registry, self.registry.instance('alpha'),
                                  runner=DecisionRunner(CountingPlugin(proposal_at=1)),
                                  source_fingerprint='f' * 64, clock=lambda: self.now)
        self.addCleanup(runtime.close)
        self.runtimes.append(runtime)
        self.money = AccountBudgetAuthority(self.db, ACCOUNT, clock=lambda: self.now)
        self.addCleanup(self.money.close)
        self.leases = SymbolLease(self.db, clock=lambda: self.now)
        self.addCleanup(self.leases.close)
        self.feedback = ExecutionAttribution(self.db, clock=lambda: self.now)
        self.accounting = InstanceAccounting(self.db, clock=lambda: self.now)
        self.admission = AdmissionService(budget=self.money, leases=self.leases,
                                          attribution=self.feedback, policy=POLICY,
                                          registry=self.registry, clock=lambda: self.now)
        self.supervisor = LiveSupervisor(self.book, process_strategy=False,
                                         agent_mode='review', owner_source='f' * 64,
                                         instance_supervisor=InstanceSupervisor(self.runtimes),
                                         admission=self.admission,
                                         accounting=self.accounting)

    def snap(self, orders=None, qty='0', held='0'):
        fake = FakeBroker()
        fake.orders = orders or []
        fake.positions = [] if qty == '0' else [position(qty, held)]
        value = snapshot(fake, self.now.isoformat())
        value['synthetic'] = True
        value['collection_complete'] = True
        return value

    def event(self, event_id, symbol):
        at = self.now.isoformat()
        return {'id': event_id, 'kind': 'quote', 'at': at, 'synthetic_input': True,
                'source_sha256': 'source-' + event_id,
                'data': {'symbol': symbol, 'bid': '39.99', 'ask': '40.00',
                         'bid_at': at, 'ask_at': at, 'observed_at': at}}

    def bound(self, action, *, filled='0', state='confirmed', oid=None):
        payload = json.loads(action['payload'])
        parameters = payload['parameters']
        raw = order(oid or payload['action_id'], parameters['side'],
                    parameters['quantity'], filled, state, parameters.get('stop_price'))
        raw['ref_id'] = payload['ref_id']
        raw['price'] = parameters.get('limit_price')
        raw['type'] = 'limit' if parameters['type'] == 'limit' else 'market'
        raw['market_hours'] = parameters['market_hours']
        raw['time_in_force'] = parameters['time_in_force']
        return raw

    def drive(self, orders=None, *, qty='0', ticks=6):
        """Run control ticks; return the first step that did the thing asked."""
        last = None
        for _ in range(ticks):
            last = self.supervisor.step(self.snap(orders=orders, qty=qty),
                                        [self.event('e-DEMO', 'DEMO')],
                                        allow_entries=True, allow_dispatch=True)
            if orders is None and last['strategy_admissions']['outcomes']:
                return last
            if orders is not None and last['strategy_receipts'].get('imported'):
                return last
        return last

    def test_a_broker_fill_becomes_accounting_and_feedback_without_hand_calls(self):
        step = self.drive()
        self.assertTrue(step['strategy_admissions']['outcomes'])
        entry = next(a for a in self.book.actions() if a['kind'] == 'entry')
        payload = json.loads(entry['payload'])
        filled = self.bound(entry, filled='1')
        filled['state'] = 'filled'
        step = self.drive([filled], qty='1')
        self.assertEqual(step['strategy_receipts']['imported'], 1)
        owner = self.feedback.owner(payload['ref_id'])['instance_id']
        fills = self.accounting.fills(owner)
        self.assertEqual(len(fills), 1)
        self.assertEqual(Decimal(fills[0]['cash_delta']), Decimal('-40.00'))
        kinds = [item['kind'] for item in self.feedback.events(owner)]
        self.assertIn('filled', kinds)

    def test_the_same_snapshot_twice_does_not_double_book(self):
        self.drive()
        entry = next(a for a in self.book.actions() if a['kind'] == 'entry')
        filled = self.bound(entry, filled='1')
        filled['state'] = 'filled'
        self.drive([filled], qty='1')
        step = self.drive([filled], qty='1')
        self.assertEqual(step['strategy_receipts']['imported'], 0,
                         'the second snapshot imports no new execution')
        owner = self.feedback.owner(json.loads(entry['payload'])['ref_id'])['instance_id']
        self.assertEqual(len(self.accounting.fills(owner)), 1)

    def test_a_receipt_with_no_reference_blocks_entries_but_not_protection(self):
        self.drive()
        entry = next(a for a in self.book.actions() if a['kind'] == 'entry')
        stray = self.bound(entry, filled='1')
        stray['state'] = 'filled'
        stray.pop('ref_id')
        stray['id'] = 'unknown-order'
        step = self.drive([stray], qty='1')
        reasons = [item.get('reason') for item in step['errors']]
        self.assertIn('RECEIPT_OWNER_UNKNOWN', reasons)
        self.assertIn('RECEIPT_FACTS_INCOMPLETE', reasons)
        self.assertNotIn('RECEIPT_IDENTITY_CONFLICT', reasons)
        self.assertTrue(step['new_entries_blocked'])
        self.assertTrue(self.book.actions())

    def test_an_unbound_reference_is_not_reported_as_an_identity_conflict(self):
        self.drive()
        entry = next(a for a in self.book.actions() if a['kind'] == 'entry')
        stray = self.bound(entry, filled='1')
        stray['state'] = 'filled'
        stray['ref_id'] = 'unbound-ref'
        stray['id'] = 'unbound-order'
        step = self.drive([stray], qty='1')
        reasons = [item.get('reason') for item in step['errors']]
        self.assertIn('RECEIPT_REFERENCE_UNBOUND', reasons)
        self.assertIn('RECEIPT_FACTS_INCOMPLETE', reasons)
        self.assertNotIn('RECEIPT_OWNER_UNKNOWN', reasons)
        self.assertNotIn('RECEIPT_IDENTITY_CONFLICT', reasons)
        self.assertTrue(step['new_entries_blocked'])
