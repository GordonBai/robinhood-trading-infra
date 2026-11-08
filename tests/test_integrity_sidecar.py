"""The unrecorded-fault side-car is an integrity witness, not an ordinary hold.

When every database write failed, the only surviving evidence that an execution
conflict happened is ``<db>.entry-inhibit.json`` with
``EXECUTION_INTEGRITY_FAULT_UNRECORDED``. A later session that reads only the
(now healthy) database would look clean. These tests pin that the side-car is
read by the same authority as the ledger: it blocks entries and settlement,
keeps its original detection time, and is never confused with a normal hold.
"""
import json
import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import TestCase

from entry_authority_fixture import TestExecutionBook as ExecutionBook
from martin_luk.instance_composition import build_admission_stack
from martin_agentic.watchdog import inhibit_new_entries
from trading_contracts.instance import (InstanceBudget, PluginManifest,
                                        StrategyInstance)
from trading_infra.integrity_gate import (SIDECAR_ISSUE, read_integrity)

from test_runner_result_merge import NOW, snapshot

AT = datetime(2026, 10, 16, 14, 0, tzinfo=timezone.utc)


def policy():
    return json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())


def write_sidecar(path, issues, *, at=AT, db=None, blocked=True):
    target = Path(str(Path(path).resolve()) + '.entry-inhibit.json')
    target.write_text(json.dumps({'db': str(db or Path(path).resolve()),
                                  'blocked': blocked, 'at': at.isoformat(),
                                  'issues': issues,
                                  'clearing_requires':
                                      'EXPLICIT_RECONCILED_OPERATOR_DISPOSITION'}))
    return target


class SidecarAuthorityTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'book.sqlite'
        self.db = sqlite3.connect(self.path, isolation_level='')
        self.addCleanup(self.db.close)

    def read(self):
        return read_integrity(self.db, db_path=self.path, clock=lambda: AT)

    def test_the_unrecorded_fault_blocks_and_keeps_the_original_time(self):
        write_sidecar(self.path, [SIDECAR_ISSUE])
        state = self.read()
        self.assertTrue(state['blocked'])
        self.assertTrue(state['known'])
        self.assertIn('EXECUTION_INTEGRITY_SIDECAR_UNRECORDED_FAULT',
                      state['reasons'])
        self.assertEqual(state['faults'], [],
                         'nothing is invented in the database')
        self.assertEqual(state['observed_at'], AT.isoformat())

    def test_an_ordinary_hold_is_not_an_execution_integrity_fault(self):
        write_sidecar(self.path, ['WATCHDOG_UNAVAILABLE'])
        state = self.read()
        self.assertFalse(state['blocked'],
                         'an operator hold is not a conflicting execution')
        self.assertTrue(state['known'])
        self.assertEqual(state['other_inhibit_issues'], ['WATCHDOG_UNAVAILABLE'])
        self.assertEqual(state['reasons'], [])

    def test_an_unreadable_sidecar_fails_closed(self):
        target = Path(str(self.path.resolve()) + '.entry-inhibit.json')
        target.write_text('{not json')
        state = self.read()
        self.assertTrue(state['blocked'])
        self.assertFalse(state['known'])
        self.assertIn('EXECUTION_INTEGRITY_SIDECAR_UNREADABLE', state['reasons'])

    def test_a_sidecar_bound_to_another_database_fails_closed(self):
        write_sidecar(self.path, [SIDECAR_ISSUE], db='/somewhere/else.sqlite')
        state = self.read()
        self.assertTrue(state['blocked'])
        self.assertFalse(state['known'])

    def test_a_released_bit_does_not_reopen_an_unrecorded_fault(self):
        # No release authority exists this round; the side-car says the fault
        # was never recorded, so a ``blocked: false`` flag cannot clear it.
        write_sidecar(self.path, [SIDECAR_ISSUE], blocked=False)
        self.assertTrue(self.read()['blocked'])

    def test_the_db_fault_and_the_sidecar_are_both_reported(self):
        from trading_infra.integrity_faults import ExecutionIntegrityFaults
        ExecutionIntegrityFaults(self.db, clock=lambda: AT).record(
            source='execution_identity_conflict', account_key='acct',
            execution_id='same-exec')
        write_sidecar(self.path, [SIDECAR_ISSUE])
        state = self.read()
        self.assertTrue(state['blocked'])
        self.assertEqual(len(state['faults']), 1)
        self.assertIn('EXECUTION_INTEGRITY_FAULT_UNRESOLVED', state['reasons'])
        self.assertIn('EXECUTION_INTEGRITY_SIDECAR_UNRECORDED_FAULT',
                      state['reasons'])


def plan():
    return {'symbol': 'DEMO', 'quantity': 1, 'entry_ceiling': '40',
            'stop': '39', 'cost_buffer_per_share': '0.05'}


def quote():
    return {'symbol': 'DEMO', 'bid': '39.99', 'ask': '40',
            'bid_at': NOW.isoformat(), 'ask_at': NOW.isoformat(),
            'observed_at': NOW.isoformat()}


def instance(instance_id='i1'):
    manifest = PluginManifest(
        plugin_id='martin', version='1.0.0', state_schema='s-1',
        config={}, data_requirements=('quotes',),
        capabilities=('advisory-entry',), max_inflight=1)
    budget = InstanceBudget(max_cash='100', max_planned_loss='2.50',
                            max_notional='100', max_concurrent=1)
    return StrategyInstance(instance_id=instance_id, manifest=manifest,
                            budget=budget, mode='shadow', enabled=True)


class SecondSessionGateTests(TestCase):
    """A fresh process must not treat a writable database as a clean account."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'book.sqlite'
        self.spec = policy()
        self.spec['strategy_instances'] = [{'instance_id': 'i1', 'plugin': 'martin'}]

    def make_ready_entry(self):
        # Simulation keeps the book off the real lineage/account path while
        # still writing a real READY action row that a later process must not
        # claim. The integrity gate is evaluated before that bypass.
        book = ExecutionBook(self.path, self.spec, simulation=True,
                             clock=lambda: NOW)
        result = book.propose(
            'sidecar-entry', plan(), quote(),
            {**snapshot(), 'synthetic': True},
            evidence={'source_sha256': 'synthetic', 'synthetic': True})
        self.assertFalse(result.get('duplicate'))
        self.assertEqual([a['state'] for a in book.actions()], ['READY'])
        book.close()

    def second_session(self):
        book = ExecutionBook(self.path, self.spec, simulation=True,
                             clock=lambda: NOW)
        self.addCleanup(book.close)
        admission = build_admission_stack(book, self.spec, clock=lambda: NOW)
        return book, admission

    def test_a_ready_entry_is_not_claimed_and_is_not_expired_away(self):
        self.make_ready_entry()
        inhibit_new_entries(str(self.path), [SIDECAR_ISSUE],
                            clock=lambda: NOW)
        book, _ = self.second_session()
        self.assertTrue(book.integrity_block())
        self.assertEqual(book.entry_gate(),
                         {'blocked': True,
                          'reason': 'EXECUTION_INTEGRITY_FAULT_UNRESOLVED'})
        self.assertIsNone(book.claim(allow_entries=True))
        self.assertEqual([a['state'] for a in book.actions()], ['READY'],
                         'a blocked claim must leave the entry, not drop it')

    def test_settlement_refuses_and_keeps_budget_and_lease(self):
        self.make_ready_entry()
        inhibit_new_entries(str(self.path), [SIDECAR_ISSUE],
                            clock=lambda: NOW)
        book, admission = self.second_session()
        self.assertEqual(admission.execution_integrity_block(),
                         'EXECUTION_INTEGRITY_SIDECAR_UNRECORDED_FAULT')
        held = instance()
        admission.registry.register('i1', held.manifest, held.budget,
                                    mode='shadow', enabled=True)
        reserved = admission.budget.reserve(held, trade_id='trade-1',
                                            cash='40', planned_loss='1.5',
                                            notional='40')
        self.assertTrue(reserved['reserved'])
        leased = admission.leases.acquire('i1', ['DEMO'], trade_id='trade-1')
        self.assertTrue(leased['acquired'])
        before = (admission.budget.status(), admission.leases.holding('i1'),
                  admission.budget.instance_status('i1'))

        outcome = admission.settle('i1', 'trade-1', authority=book)
        self.assertFalse(outcome['settled'])
        self.assertEqual(outcome['reason'],
                         'EXECUTION_INTEGRITY_SIDECAR_UNRECORDED_FAULT')
        self.assertEqual((admission.budget.status(),
                          admission.leases.holding('i1'),
                          admission.budget.instance_status('i1')), before)

    def test_a_fresh_connection_reads_the_same_sidecar(self):
        self.make_ready_entry()
        inhibit_new_entries(str(self.path), [SIDECAR_ISSUE],
                            clock=lambda: NOW)
        for _ in range(2):
            book, admission = self.second_session()
            self.assertTrue(book.integrity_block())
            self.assertEqual(
                admission.execution_integrity_block(),
                'EXECUTION_INTEGRITY_SIDECAR_UNRECORDED_FAULT')
            book.close()
