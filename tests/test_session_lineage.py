"""Deterministic formal-journal fixtures; no broker transport or production DB."""
from copy import deepcopy
from datetime import datetime, timedelta
import json
from pathlib import Path
import sqlite3
import tempfile
from unittest import TestCase
from unittest.mock import patch

from martin_agentic.audit import canonical, digest
from martin_agentic.broker import BrokerJournal
from entry_authority_fixture import TestExecutionBook as ExecutionBook
from martin_agentic.session_lineage import (bootstrap_session_lineage, enroll_session_lineage,
    lineage_entry_gate, lineage_approval_ids)
from test_broker import snapshot, FakeBroker, approval


class SessionLineageTests(TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory(); self.addCleanup(self.folder.cleanup)
        self.runtime = Path(self.folder.name) / 'runtime'; self.runtime.mkdir()
        self.patch = patch('martin_agentic.session_lineage.CANONICAL_RUNTIME', self.runtime)
        self.patch.start(); self.addCleanup(self.patch.stop)
        self.now = datetime.fromisoformat('2026-10-19T14:00:00+00:00')
        self.policy = json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        self.old_path = self.runtime / 'old.sqlite'; self.next_path = self.runtime / 'next.sqlite'
        self.old = self.make_book(self.old_path, self.policy)
        self.fresh(self.old, self.old_path)
        self.next_policy = {**self.policy, 'session_date': '2026-10-19',
            'prior_execution_evidence_db': 'runtime/old.sqlite'}

    def make_book(self, path, policy):
        book = ExecutionBook(path, policy, clock=lambda: self.now)
        self.addCleanup(book.close)
        return book

    def fresh(self, book, path, fake=None):
        current = snapshot(fake or FakeBroker(), self.now.isoformat())
        journal = BrokerJournal(path)
        try: journal.ingest(current)
        finally: journal.close()
        book.reconcile(current)
        return current

    def bootstrap(self):
        return bootstrap_session_lineage(self.old_path, self.policy,
            current_db=self.old_path, current_policy=self.policy, clock=lambda: self.now)

    def next_book(self):
        book = self.make_book(self.next_path, self.next_policy)
        self.fresh(book, self.next_path)
        return book

    def enroll(self, next_book):
        return enroll_session_lineage(self.next_path, self.next_policy, predecessor_db=self.old_path,
            current_db=self.next_path, current_policy=self.next_policy, clock=lambda: self.now)

    def registry(self):
        db = sqlite3.connect(self.runtime / 'account-risk-0000.sqlite')
        self.addCleanup(db.close)
        return db

    def test_new_filename_cannot_bypass_missing_registry_and_old_unknown(self):
        old_state = self.old.state(); old_state['issues'] = ['UNKNOWN_NATIVE_WRITE']
        self.old._save(old_state); self.old.db.commit()
        book = self.next_book()
        self.assertTrue(book.entry_gate()['blocked'])
        self.assertNotIn('session_lineage_events', {r[0] for r in self.registry().execute("SELECT name FROM sqlite_master")})
        with self.assertRaisesRegex(ValueError, 'PRIOR_UNCOVERED_ISSUES'):
            self.bootstrap()
        self.assertTrue(book.entry_gate()['blocked'])

    def test_explicit_bootstrap_and_enrollment_preserve_risk_and_history(self):
        before_risk = self.registry().execute('SELECT payload FROM account_risk').fetchone()[0]
        before_history = self.old_path.read_bytes()
        self.assertTrue(self.bootstrap()['registered'])
        book = self.next_book(); self.enroll(book)
        self.assertFalse(book.entry_gate()['blocked'])
        self.assertEqual(self.old_path.read_bytes(), before_history)
        self.assertEqual(self.registry().execute('SELECT payload FROM account_risk').fetchone()[0], before_risk)
        with self.assertRaisesRegex(ValueError, 'BOOTSTRAP_ALREADY_EXISTS'): self.bootstrap()
        with self.assertRaisesRegex(ValueError, 'PREDECESSOR_OR_DUPLICATE'): self.enroll(book)

    def test_policy_pointer_is_not_enrollment_and_cannot_be_removed(self):
        self.bootstrap(); book = self.next_book()
        self.assertEqual(book.entry_gate()['reason'], 'LINEAGE_CURRENT_SESSION_NOT_REGISTERED')
        self.enroll(book); self.assertFalse(book.entry_gate()['blocked'])
        original = book.policy
        book.policy = {k: v for k, v in original.items() if k != 'prior_execution_evidence_db'}
        self.assertTrue(book.entry_gate()['blocked'])
        book.policy = original
        wrong = {**self.next_policy, 'prior_execution_evidence_db': 'runtime/wrong.sqlite'}
        with self.assertRaisesRegex(ValueError, 'DECLARED_PREDECESSOR_MISMATCH'):
            enroll_session_lineage(self.next_path, wrong, predecessor_db=self.old_path,
                current_db=self.next_path, current_policy=self.next_policy, clock=lambda: self.now)

    def test_source_policy_account_and_audit_prefix_changes_fail_closed(self):
        self.bootstrap(); book = self.next_book(); self.enroll(book)
        state = self.old.state()
        for field, value in [('policy_hash', 'wrong'), ('account_key', 'wrong')]:
            changed = {**state, field: value}; self.old._save(changed); self.old.db.commit()
            self.assertTrue(book.entry_gate()['blocked'])
            self.old._save(state); self.old.db.commit()
        self.old.db.execute('DROP TRIGGER audit_no_delete')
        self.old.db.execute('DELETE FROM audit_events'); self.old.db.commit()
        self.assertIn('AUDIT_PREFIX', book.entry_gate()['reason'])

    def test_missing_source_and_changed_sidecar_paths_fail_closed(self):
        self.bootstrap(); book = self.next_book(); self.enroll(book)
        moved = self.old_path.with_suffix('.saved'); self.old_path.rename(moved)
        try: self.assertTrue(book.entry_gate()['blocked'])
        finally: moved.rename(self.old_path)
        outside = Path(self.folder.name) / 'outside'; outside.mkdir()
        out = self.make_book(outside / 'elsewhere.sqlite', self.policy)
        self.assertEqual(out.entry_gate()['reason'], 'LINEAGE_NONCANONICAL_EXECUTION_PATH')

    def test_chain_mutation_and_valid_prefix_truncation_are_rejected(self):
        self.bootstrap(); book = self.next_book(); self.enroll(book)
        db = self.registry()
        original = db.execute('SELECT payload FROM session_lineage_events WHERE sequence=1').fetchone()[0]
        with self.assertRaisesRegex(sqlite3.IntegrityError, 'append-only'):
            db.execute("UPDATE session_lineage_events SET payload='{}' WHERE sequence=1")
        db.execute('DROP TRIGGER session_lineage_no_update')
        db.execute('DROP TRIGGER session_lineage_no_delete')
        db.execute("UPDATE session_lineage_events SET payload='{}' WHERE sequence=1"); db.commit()
        self.assertEqual(book.entry_gate()['reason'], 'LINEAGE_REGISTRY_CHAIN_INVALID')
        db.execute('UPDATE session_lineage_events SET payload=? WHERE sequence=1', (original,))
        db.execute('DELETE FROM session_lineage_events WHERE sequence=2'); db.commit()
        self.assertEqual(book.entry_gate()['reason'], 'LINEAGE_REGISTRY_HEAD_MISMATCH')

    def test_stale_unhealthy_or_supplied_fake_snapshot_never_admits(self):
        self.bootstrap(); book = self.next_book(); self.enroll(book)
        current = self.fresh(book, self.next_path)
        fake = deepcopy(current); fake['portfolio']['cash'] = '999'
        self.assertEqual(lineage_entry_gate(book, fake)['reason'], 'LINEAGE_SUPPLIED_SNAPSHOT_DIFFERS_FROM_JOURNAL')
        self.now += timedelta(seconds=31)
        self.assertEqual(book.entry_gate()['reason'], 'LINEAGE_CURRENT_FORMAL_OBSERVATION_INVALID')
        self.now -= timedelta(seconds=31)
        book.db.execute('UPDATE broker_health SET payload=? WHERE id=1', (canonical({'healthy': False, 'at': current['observed_at']}),)); book.db.commit()
        self.assertEqual(book.entry_gate()['reason'], 'LINEAGE_CURRENT_FORMAL_OBSERVATION_INVALID')

    def test_actual_integrated_propose_claim_native_validation_respect_predecessor(self):
        self.bootstrap(); book = self.next_book(); self.enroll(book)
        current = self.fresh(book, self.next_path)
        plan = {'symbol':'DEMO','quantity':1,'entry_ceiling':'40','stop':'39','cost_buffer_per_share':'0.05'}
        quote = {'symbol':'DEMO','bid':'39.99','ask':'40',
            'bid_at':self.now.isoformat(),'ask_at':self.now.isoformat(),'observed_at':self.now.isoformat()}
        evidence = {'source_sha256':'fixture-only','synthetic':False,'engineering_scope_authorized':True,
            'parameters_user_specified':True}
        proposed = book.propose('engineering-lineage-test',plan,quote,current,evidence=evidence,purpose='engineering')
        old = self.old.state(); old['issues'] = ['PROTECTION_WAIT_EXCEEDED:DEMO']
        self.old._save(old); self.old.db.commit()
        self.assertIsNone(book.claim())
        self.assertEqual(book.actions()[0]['state'], 'READY')
        with self.assertRaisesRegex(ValueError, 'PRIOR_UNCOVERED_ISSUES'):
            book.validate_external_action(proposed['action_id'],current,quote=quote)
        with self.assertRaisesRegex(ValueError, 'PRIOR_UNCOVERED_ISSUES'):
            book.prepare_external_action(proposed['action_id'],current,quote=quote,operation_id='no-native-write',operation='place')
        self.assertEqual(book.db.execute('SELECT count(*) FROM external_operations').fetchone()[0], 0)

    def test_current_protection_management_is_not_subject_to_lineage_entry_gate(self):
        from test_execution import ExecutionTests
        fixture = ExecutionTests(); fixture.setUp(); self.addCleanup(fixture.tearDown)
        fixture.entry_filled()
        with patch.object(fixture.book, 'entry_gate', return_value={'blocked':True,'reason':'LINEAGE_FAILED'}):
            action = fixture.book.claim(allow_entries=False)
        self.assertEqual(action['kind'], 'protect')

    def test_simulation_and_capture_remain_isolated_from_real_registry(self):
        with patch('martin_agentic.session_lineage._book_paths', side_effect=AssertionError('Must not touch registry')):
            self.assertFalse(lineage_entry_gate(type('Sim', (), {'simulation':True})())['blocked'])
            self.assertTrue(lineage_entry_gate(type('Capture', (), {'simulation':False,'policy':{'capture_only':True}})())['blocked'])

    def test_new_unknown_manual_intent_after_registration_blocks(self):
        self.bootstrap(); book = self.next_book(); self.enroll(book)
        self.old.audit.append({'event_id':'late-manual-intent','occurred_at':self.now.isoformat(),
            'event_type':'AGENT_ACTION_INTENT','actor':'agent','synthetic':False,
            'account':'••••0000','intent_id':'late-manual','trade_id':'manual-untracked',
            'details':{'action':'exit','parameters':{'symbol':'DEMO','quantity':'1'}}})
        self.assertIn('PRIOR_UNACKNOWLEDGED_WRITE:late-manual',book.entry_gate()['reason'])

    def test_post_registration_unknown_native_operation_is_not_hidden_by_flat_account(self):
        self.bootstrap(); book = self.next_book(); self.enroll(book)
        self.old.db.execute('INSERT INTO external_operations VALUES(?,?,?,?,NULL,NULL)',
            ('unlogged-native','old-action','place',self.now.isoformat())); self.old.db.commit()
        self.assertEqual(book.entry_gate()['reason'], 'LINEAGE_PRIOR_NATIVE_INTENT_MISSING:unlogged-native')

    def test_approval_union_requires_exact_fresh_alias_results(self):
        self.bootstrap(); book = self.next_book(); self.enroll(book)
        self.old.db.execute('INSERT INTO external_operations VALUES(?,?,?,?,?,?)',
            ('review','old-action','review',self.now.isoformat(),canonical({'status':'review_only'}),'prior-alias'))
        self.old.db.execute('INSERT INTO external_broker_refs VALUES(?,?,?)', ('prior-alias','old-action','review'))
        self.old.db.commit()
        self.assertEqual(lineage_approval_ids(book), ['prior-alias'])
        self.assertEqual(book.entry_gate()['reason'], 'LINEAGE_PRIOR_APPROVAL_REFS_NOT_QUERIED')
        current = snapshot(at=self.now.isoformat())
        current.update(requested_approval_ids=['prior-alias'],approval_states={'prior-alias':{
            'approval_id':'prior-alias','state':'APPROVAL_STATE_DECLINED','is_edited':False,'asset_class':'ASSET_CLASS_EQUITY'}})
        journal = BrokerJournal(self.next_path)
        try:journal.ingest(current)
        finally:journal.close()
        self.assertFalse(book.entry_gate()['blocked'])

    def test_registration_requires_bound_source_and_fresh_formal_journal(self):
        source = self.make_book(self.next_path, self.next_policy)
        with self.assertRaisesRegex(ValueError,'SOURCE_ACCOUNT_MISMATCH'):
            bootstrap_session_lineage(self.next_path,self.next_policy,current_db=self.old_path,
                current_policy=self.policy,clock=lambda:self.now)
        self.now += timedelta(seconds=31)
        with self.assertRaisesRegex(ValueError,'CURRENT_FORMAL_OBSERVATION_INVALID'): self.bootstrap()

    def test_routine_lineage_does_not_add_flat_only_exposure_policy(self):
        from test_broker import position, order
        self.bootstrap(); book = self.next_book(); self.enroll(book)
        fake = FakeBroker(); fake.positions = [position('1','1')]
        fake.orders = [order('current-owned-stop','sell',qty='1',filled='0',state='confirmed',stop='39')]
        current = snapshot(fake,self.now.isoformat())
        # Persist a consistent formal baseline on a fresh current journal. The
        # application's existing ownership/risk gates, not lineage, decide entry.
        book.db.execute('DELETE FROM broker_current'); book.db.commit()
        journal = BrokerJournal(self.next_path)
        try:journal.ingest(current)
        finally:journal.close()
        self.assertFalse(lineage_entry_gate(book)['blocked'])

    def recovered_source(self):
        from test_recovery import RecoveryTests
        fixture = RecoveryTests('test_ack_keeps_failure_state_issues_risk_and_manual_provenance')
        fixture.setUp(); self.addCleanup(fixture.tearDown)
        state = fixture.book.state()
        state['issues'] = [i for i in state['issues'] if not i.startswith('PROTECTION_WAIT_EXCEEDED')]
        fixture.book._save(state); fixture.book.db.commit(); case = fixture.acknowledged()
        recovered = self.runtime / 'recovered.sqlite'
        copied = sqlite3.connect(recovered)
        try:fixture.book.db.backup(copied)
        finally:copied.close()
        current = deepcopy(fixture.snapshot); current['observed_at'] = self.now.isoformat()
        journal = BrokerJournal(self.old_path)
        try:journal.ingest(current)
        finally:journal.close()
        self.old.reconcile(current)
        bootstrap_session_lineage(recovered,self.policy,current_db=self.old_path,
            current_policy=self.policy,clock=lambda:self.now)
        self.next_policy['prior_execution_evidence_db'] = 'runtime/recovered.sqlite'
        book = self.make_book(self.next_path,self.next_policy)
        journal = BrokerJournal(self.next_path)
        try:journal.ingest(current)
        finally:journal.close()
        book.reconcile(current)
        enroll_session_lineage(self.next_path,self.next_policy,predecessor_db=recovered,
            current_db=self.next_path,current_policy=self.next_policy,clock=lambda:self.now)
        return book,recovered,current,case

    def test_acknowledged_manual_history_uses_fresh_current_terminal_evidence(self):
        book,recovered,current,case = self.recovered_source()
        before = recovered.read_bytes()
        self.assertFalse(lineage_entry_gate(book)['blocked'])
        self.assertEqual(recovered.read_bytes(),before)
        changed = deepcopy(current); changed['orders'].pop(case['cancelled_order_ids'][0])
        journal = BrokerJournal(self.next_path)
        try:journal.ingest(changed)
        finally:journal.close()
        self.assertIn('RECOVERY_TERMINAL_EVIDENCE_CHANGED',book.entry_gate()['reason'])

    def test_new_native_write_after_ack_is_not_retired_with_old_trade(self):
        book,recovered,current,case = self.recovered_source()
        source = self.make_book(recovered,self.policy)
        source.audit.append({'event_id':'post-ack-native-intent','occurred_at':self.now.isoformat(),
            'event_type':'AGENT_ACTION_INTENT','actor':'agent','synthetic':False,
            'account':'••••0000','intent_id':'post-ack-native','trade_id':case['trade_id'],
            'details':{'action':'entry','parameters':{'symbol':'DEMO','quantity':'1'}}})
        source.db.execute('INSERT INTO external_operations VALUES(?,?,?,?,NULL,NULL)',
            ('post-ack-native','old-action','place',self.now.isoformat())); source.db.commit()
        self.assertIn('PRIOR_UNACKNOWLEDGED_WRITE:post-ack-native',book.entry_gate()['reason'])

    def test_symlinked_risk_sidecar_is_rejected_before_registration_mutation(self):
        risk = self.runtime / 'account-risk-0000.sqlite'
        outside = Path(self.folder.name) / 'outside-risk.sqlite'
        outside.write_bytes(risk.read_bytes())
        before = outside.read_bytes()
        risk.unlink(); risk.symlink_to(outside)
        with self.assertRaisesRegex(ValueError, 'NONCANONICAL_ACCOUNT_RISK_PATH'):
            self.bootstrap()
        self.assertEqual(outside.read_bytes(), before)
        self.assertTrue(self.old.entry_gate()['blocked'])

    def test_registered_predecessor_cannot_resume_entry_after_successor_enrollment(self):
        self.bootstrap();book=self.next_book();self.enroll(book)
        self.assertFalse(book.entry_gate()['blocked'])
        self.assertEqual(self.old.entry_gate()['reason'],'LINEAGE_SESSION_SUPERSEDED_BY_REGISTERED_SUCCESSOR')
