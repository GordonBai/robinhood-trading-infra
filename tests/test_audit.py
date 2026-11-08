from datetime import datetime
import json
from pathlib import Path
import sqlite3
import tempfile
from unittest import TestCase

from martin_agentic.audit import AuditTrail
from martin_agentic.broker import BrokerJournal
from martin_luk.engine import ShadowEngine
from martin_agentic.protection import ProtectionJournal
from shadow_fixture import events as shadow_events
from test_broker import FakeBroker, snapshot, position, order
from test_protection import scenario


AT='2026-10-16T14:00:00+00:00'


def intent():
    return {'event_id':'agent-intent-1','occurred_at':AT,'event_type':'AGENT_ACTION_INTENT',
        'actor':'agent','synthetic':False,'account':'••••0000','intent_id':'intent-1',
        'symbol':'DEMO','quantity':'1','ref_id':'logical-ref-1',
        'details':{'action':'entry','parameters':{'type':'limit','limit_price':'40','quantity':'1'}}}


class AuditTests(TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.path=Path(self.tmp.name)/'audit.sqlite'
        self.audit=AuditTrail(self.path,clock=lambda:datetime.fromisoformat(AT))

    def tearDown(self):
        self.audit.close()
        self.tmp.cleanup()

    def test_durable_pre_dispatch_intent_and_response_link(self):
        first=self.audit.append(intent())
        self.assertFalse(first['duplicate'])
        self.assertEqual(self.audit.report('2026-10-16')['unresolved_agent_intents'],['intent-1'])
        self.audit.append({'event_id':'result-1','occurred_at':AT,'event_type':'AGENT_ACTION_RESULT',
            'actor':'agent','synthetic':False,'intent_id':'intent-1','details':{'outcome':'pending_approval'}})
        self.assertEqual(self.audit.report('2026-10-16')['unresolved_agent_intents'],[])
        self.assertEqual(self.audit.verify()['events_verified'],2)

    def test_unknown_submission_remains_unresolved(self):
        self.audit.append(intent())
        self.audit.append({'event_id':'unknown-1','occurred_at':AT,'event_type':'AGENT_ACTION_UNKNOWN',
            'actor':'agent','synthetic':False,'intent_id':'intent-1','reason':'TRANSPORT_TIMEOUT'})
        self.assertEqual(self.audit.report('2026-10-16')['unresolved_agent_intents'],['intent-1'])

    def test_response_without_intent_is_rejected(self):
        with self.assertRaisesRegex(ValueError,'no durable'):
            self.audit.append({'event_id':'unknown-1','occurred_at':AT,'event_type':'AGENT_ACTION_UNKNOWN',
                'actor':'agent','synthetic':False,'intent_id':'missing','reason':'TRANSPORT_TIMEOUT'})

    def test_duplicate_event_idempotent_but_conflict_rejected(self):
        first=self.audit.append(intent())
        second=self.audit.append(intent())
        self.assertTrue(second['duplicate'])
        self.assertEqual(first['entry_hash'],second['entry_hash'])
        with self.assertRaises(ValueError):self.audit.append({**intent(),'quantity':'2'})

    def test_different_event_id_cannot_reuse_same_agent_intent(self):
        self.audit.append(intent())
        with self.assertRaisesRegex(ValueError,'already exists'):
            self.audit.append({**intent(),'event_id':'other'})

    def test_mutations_prohibited(self):
        self.audit.append(intent())
        with self.assertRaises(sqlite3.IntegrityError):self.audit.db.execute('DELETE FROM audit_events')
        self.audit.db.rollback()
        with self.assertRaises(sqlite3.IntegrityError):self.audit.db.execute("UPDATE audit_events SET payload='{}'")
        self.audit.db.rollback()
        self.assertEqual(self.audit.verify()['events_verified'],1)

    def test_hash_chain_detects_out_of_band_tampering(self):
        self.audit.append(intent())
        self.audit.db.execute('DROP TRIGGER audit_no_update')
        changed={**intent(),'quantity':'9'}
        self.audit.db.execute('UPDATE audit_events SET payload=?',(json.dumps(changed),))
        self.audit.db.commit()
        with self.assertRaisesRegex(ValueError,'AUDIT_INTEGRITY_FAILED'):self.audit.verify()

    def test_credentials_and_unmasked_account_are_rejected(self):
        for details in ({'action':'entry','parameters':{'account_number':'private'}},
                        {'action':'entry','parameters':{'headers':{'Authorization':'secret'}}}):
            with self.assertRaises(ValueError):self.audit.append({**intent(),'details':details})
        with self.assertRaises(ValueError):self.audit.append({**intent(),'account':'FULL-ACCOUNT'})
        self.assertEqual(self.audit.verify()['events_verified'],0)

    def test_money_floats_are_not_silently_serialized(self):
        with self.assertRaises(ValueError):self.audit.append({**intent(),'price':40.1})

    def test_report_uses_record_date_and_keeps_actual_execution_time(self):
        event={'event_id':'late-fill','occurred_at':'2026-10-15T19:00:00Z','event_type':'BROKER_EXECUTION_OBSERVED',
            'actor':'broker','synthetic':False,'symbol':'DEMO','quantity':'1','price':'40'}
        self.audit.append(event)
        report=self.audit.report('2026-10-16')
        self.assertEqual(report['real_execution_observations'],1)
        self.assertIn('2026-10-15',report['events'][0]['occurred_local'])
        self.assertEqual(self.audit.report('2026-10-15')['event_count'],0)

    def test_report_pins_consistent_snapshot_during_concurrent_append(self):
        self.audit.db.execute('PRAGMA journal_mode=WAL')
        self.audit.append(intent())
        other=AuditTrail(self.path,clock=lambda:datetime.fromisoformat(AT))
        original=self.audit.verify
        def interleaved():
            integrity=original()
            other.append({'event_id':'concurrent-observation','occurred_at':AT,
                'event_type':'ACCOUNT_OBSERVED','actor':'broker','synthetic':False})
            return integrity
        self.audit.verify=interleaved
        try:
            report=self.audit.report('2026-10-16')
            self.assertEqual(report['events_verified'],report['event_count'])
            self.assertEqual(report['head_hash'],report['events'][-1]['entry_hash'])
            self.assertEqual(report['event_count'],1)
        finally:self.audit.verify=original;other.close()
        self.assertEqual(self.audit.report('2026-10-16')['event_count'],2)

    def test_export_three_formats_and_empty_day_does_not_invent_trades(self):
        self.audit.append(intent())
        result=self.audit.export('2026-10-16',Path(self.tmp.name)/'reports')
        for path in result['artifacts'].values():self.assertTrue(Path(path).is_file())
        self.assertEqual(len(Path(result['artifacts']['jsonl']).read_text().splitlines()),1)
        self.assertEqual(result['real_execution_observations'],0)
        self.assertFalse(result['broker_end_of_day_reconciliation_performed'])
        self.assertEqual(self.audit.report('2026-10-17')['event_count'],0)

    def test_shared_transaction_rolls_back_audit_and_business_facts(self):
        broker=BrokerJournal(self.path)
        try:
            broker.ingest(snapshot())
            count=broker.audit.verify()['events_verified']
            broker.db.execute("CREATE TRIGGER fail_current BEFORE UPDATE ON broker_current BEGIN SELECT RAISE(FAIL,'injected'); END")
            broker.db.commit()
            fake=FakeBroker();fake.positions=[position()];fake.orders=[order()];fake.cash='460'
            with self.assertRaises(sqlite3.IntegrityError):broker.ingest(snapshot(fake,'2026-10-16T14:00:01+00:00'))
            self.assertEqual(broker.audit.verify()['events_verified'],count)
            self.assertEqual(broker.db.execute('SELECT COUNT(*) FROM broker_executions').fetchone()[0],0)
        finally:broker.close()

    def test_broker_repeated_snapshot_never_repeats_execution_audit(self):
        broker=BrokerJournal(self.path)
        broker.audit.clock=lambda:datetime.fromisoformat(AT)
        try:
            fake=FakeBroker();fake.positions=[position()];fake.orders=[order()]
            broker.ingest(snapshot(fake));broker.ingest(snapshot(fake))
            report=broker.audit.report('2026-10-16')
            self.assertEqual(report['event_types']['BROKER_EXECUTION_OBSERVED'],1)
        finally:broker.close()

    def test_shadow_actions_are_always_labeled_simulation(self):
        engine=ShadowEngine(self.path)
        engine.audit.clock=lambda:datetime.fromisoformat(AT)
        try:
            for e in shadow_events():engine.apply(e)
            report=engine.audit.report('2026-10-16')
            self.assertEqual(report['real_execution_observations'],0)
            self.assertTrue(all(e['event']['synthetic'] for e in report['events']))
            self.assertEqual(report['event_count'],len(shadow_events()))
        finally:engine.close()

    def test_protection_rehearsal_does_not_masquerade_as_broker_protection(self):
        journal=ProtectionJournal(self.path)
        journal.audit.clock=lambda:datetime.fromisoformat(AT)
        try:
            for e in scenario():journal.apply(e)
            report=journal.audit.report('2026-10-16')
            self.assertEqual(report['real_execution_observations'],0)
            self.assertEqual(report['event_types']['PROTECTION_REHEARSAL'],7)
        finally:journal.close()
