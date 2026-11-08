"""Policy-declared strategy instances, built by the application composition layer.

An absent ``strategy_instances`` key must keep the single legacy instance, so
these tests check the compatibility answer first and the group only after.
"""
import json
import sqlite3
import tempfile
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest import TestCase

from martin_luk.instance_composition import (build_admission_stack,
                                                 build_instance_group)
from martin_luk.live_session import build_strategy_services
from trading_infra.instance_registry import InstanceRegistry

SOURCE = 'd' * 64
NOW = datetime(2026, 10, 29, 14, 0, tzinfo=timezone.utc)


class FakeRunner:
    def __init__(self):
        self.started = []

    @property
    def next_deadline_monotonic(self):
        return None

    def start(self, request, *, payload=None):
        self.started.append(request)

    def poll(self):
        return None

    def close(self):
        pass


class CompositionTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = sqlite3.connect(self.temp.name + '/policy.sqlite', isolation_level=None)
        self.addCleanup(self.db.close)
        self.book = type('Book', (), {'db': self.db})()
        self.policy = json.loads(
            Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        self.policy.update(session_date='2026-10-15')

    def build(self, policy=None):
        return build_instance_group(self.book, policy or self.policy,
                                    source_fingerprint=SOURCE, clock=lambda: NOW,
                                    runner_factory=lambda cost, pin: FakeRunner())

    def test_absent_key_keeps_the_single_legacy_instance(self):
        self.assertIsNone(self.build())
        tables = {row[0] for row in self.db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertNotIn('strategy_instances', tables)

    def test_a_declared_group_registers_every_instance(self):
        policy = dict(self.policy, strategy_instances=[
            {'instance_id': 'martin-a', 'plugin': 'martin', 'cost_buffer_per_share': '0.05'},
            {'instance_id': 'martin-b', 'plugin': 'martin', 'cost_buffer_per_share': '0.10'}])
        group = self.build(policy)
        self.addCleanup(group.close)
        self.assertEqual(group.instance_ids(), ['martin-a', 'martin-b'])
        registry = InstanceRegistry(self.db, clock=lambda: NOW)
        self.assertEqual(registry.instance('martin-a').manifest.config['cost_buffer_per_share'],
                         '0.05')
        self.assertEqual(
            Decimal(registry.instance('martin-b').manifest.config['cost_buffer_per_share']),
            Decimal('0.10'))

    def test_budget_comes_from_the_account_policy_not_the_strategy(self):
        policy = dict(self.policy, strategy_instances=[
            {'instance_id': 'martin-a', 'plugin': 'martin'}])
        group = self.build(policy)
        self.addCleanup(group.close)
        budget = InstanceRegistry(self.db, clock=lambda: NOW).instance('martin-a').budget
        self.assertEqual(budget.max_planned_loss,
                         Decimal(str(self.policy['max_planned_loss_per_trade_usd'])))
        self.assertEqual(budget.max_notional,
                         Decimal(str(self.policy['max_position_notional_usd'])))

    def test_an_unknown_field_is_refused_rather_than_ignored(self):
        policy = dict(self.policy, strategy_instances=[
            {'instance_id': 'martin-a', 'plugin': 'martin', 'max_planned_loss': '999'}])
        with self.assertRaises(ValueError) as ctx:
            self.build(policy)
        self.assertIn('INSTANCE_SPEC_UNKNOWN_FIELD', str(ctx.exception))

    def test_an_unknown_plugin_is_refused(self):
        policy = dict(self.policy, strategy_instances=[
            {'instance_id': 'x', 'plugin': 'not-installed'}])
        with self.assertRaises(ValueError) as ctx:
            self.build(policy)
        self.assertEqual(str(ctx.exception), 'INSTANCE_SPEC_UNKNOWN_PLUGIN')

    def test_an_empty_or_oversized_list_is_refused(self):
        for value in ([], [{'instance_id': 'x', 'plugin': 'martin'}] * 17, 'nope'):
            with self.assertRaises(ValueError):
                self.build(dict(self.policy, strategy_instances=value))

    def test_two_specs_cannot_share_an_instance_id(self):
        spec = {'instance_id': 'same', 'plugin': 'martin'}
        with self.assertRaises(ValueError):
            self.build(dict(self.policy, strategy_instances=[spec, dict(spec)]))

    def test_a_negative_cost_is_refused(self):
        with self.assertRaises(ValueError):
            self.build(dict(self.policy, strategy_instances=[
                {'instance_id': 'x', 'plugin': 'martin', 'cost_buffer_per_share': '-0.1'}]))

    def test_no_instances_means_no_account_authority(self):
        self.assertIsNone(build_admission_stack(self.book, self.policy, clock=lambda: NOW))

    def test_a_configured_group_gets_one_shared_authority(self):
        policy = dict(self.policy, strategy_instances=[
            {'instance_id': 'martin-a', 'plugin': 'martin'}])
        stack = build_admission_stack(self.book, policy, clock=lambda: NOW)
        self.addCleanup(stack.budget.close)
        self.addCleanup(stack.leases.close)
        self.assertIsNotNone(stack)
        # One connection for every component, so admission can be one transaction.
        self.assertIs(stack.budget.db, self.db)
        self.assertIs(stack.leases.db, self.db)
        self.assertIs(stack.attribution.db, self.db)

    def test_the_account_ceiling_cannot_be_widened_by_a_strategy_spec(self):
        policy = dict(self.policy, strategy_instances=[
            {'instance_id': 'martin-a', 'plugin': 'martin'}],
            account_limits={'max_open_cash': '999999'})
        stack = build_admission_stack(self.book, policy, clock=lambda: NOW)
        self.addCleanup(stack.budget.close)
        self.addCleanup(stack.leases.close)
        # Explicit policy may widen it; a strategy spec cannot, because the
        # spec vocabulary does not include account limits.
        self.assertEqual(str(stack.budget.status()['max_open_cash']), '999999')
        with self.assertRaises(ValueError):
            self.build(dict(self.policy, strategy_instances=[
                {'instance_id': 'x', 'plugin': 'martin', 'max_open_cash': '999999'}]))

    def test_the_default_bootstrap_resolves_all_three_shapes(self):
        # No instances declared: the single legacy loop, and no account authority.
        supervisor, admission = build_strategy_services(
            self.book, self.policy, source_fingerprint=SOURCE, clock=lambda: NOW)
        self.assertIsNone(supervisor)
        self.assertIsNone(admission)

        # Instances declared by policy: both are built, without a NameError.
        declared = dict(self.policy, strategy_instances=[
            {'instance_id': 'martin-a', 'plugin': 'martin'}])
        supervisor, admission = build_strategy_services(
            self.book, declared, source_fingerprint=SOURCE, clock=lambda: NOW)
        self.assertIsNotNone(supervisor)
        self.assertIsNotNone(admission)
        self.addCleanup(supervisor.close)
        self.addCleanup(admission.budget.close)
        self.addCleanup(admission.leases.close)

        # An injected supervisor wins and still gets an authority.
        injected = build_instance_group(self.book, declared,
                                        source_fingerprint=SOURCE, clock=lambda: NOW,
                                        runner_factory=lambda c, p: FakeRunner())
        self.addCleanup(injected.close)
        supervisor, admission = build_strategy_services(
            self.book, self.policy, source_fingerprint=SOURCE, clock=lambda: NOW,
            instance_supervisor=injected)
        self.assertIs(supervisor, injected)
        self.assertIsNone(admission)
