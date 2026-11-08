from pathlib import Path
import io
import json
import sqlite3
from contextlib import redirect_stdout
import tempfile
from unittest import TestCase
from unittest.mock import patch

from martin_agentic.protection import ProtectionJournal
from martin_agentic.cli import main


INTENT = '143272e0-5be5-42a0-97fb-5d8fae747b3c'
SECOND = '14896321-0426-4fbb-baca-ef8c04fdd26c'
AT = '2026-10-16T14:00:00+00:00'
UNTIL = '2026-10-16T20:00:00+00:00'


def event(kind, id=None, at=AT, **kwargs):
    return {'kind':kind, 'id':id or kind, 'at':at, 'account_ref':'agentic-rehearsal', **kwargs}


def scenario():
    return [event('init', mode='protection_rehearsal', synthetic=True,
                  max_unprotected_seconds=10, max_broker_age_seconds=30),
            event('fill', execution_id='buy-1', symbol='DEMO', side='buy', quantity=2,
                  price='40', initial_stop='39'),
            event('position_check', positions={'DEMO':2}),
            event('protect_intent', intent_id=INTENT, symbol='DEMO', quantity=2, stop='39', valid_until=UNTIL),
            event('approve', intent_id=INTENT, approved=True),
            event('submit', intent_id=INTENT),
            observed()]


def observed(id='ack', status='ACTIVE', filled=0, **kwargs):
    data = dict(intent_id=INTENT, status=status, broker_id='broker-stop-1', filled_quantity=filled,
                quantity=2, stop='39', symbol='DEMO', side='sell', order_type='stop_market',
                market_hours='regular_hours', valid_until=UNTIL)
    data.update(kwargs)
    return event('broker_order', id=id, **data)


class ProtectionTests(TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name)/'protect.sqlite'
        self.journal = ProtectionJournal(self.path)

    def tearDown(self):
        self.journal.close()
        self.tmp.cleanup()

    def replay(self, events=None):
        for e in events or scenario():
            result = self.journal.apply(e)
        return result

    def test_only_broker_ack_activates_coverage(self):
        for e in scenario()[:-1]:
            state = self.journal.apply(e)
            if 'DEMO' in state['positions']:
                self.assertEqual(state['positions']['DEMO']['verified_protected_quantity'],0)
        state = self.journal.apply(observed())
        self.assertEqual(state['positions']['DEMO']['protection_state'],'PROTECTED')
        self.assertFalse(state['new_risk_blocked'])
        self.assertFalse(state['broker_protection_verified'])
        self.assertFalse(state['execution_ready'])

    def test_no_synthetic_or_explicit_latency_policy_is_rejected(self):
        e = scenario()[0]
        with self.assertRaises(ValueError):self.journal.apply({**e,'synthetic':False})
        with self.assertRaises(ValueError):self.journal.apply({**e,'max_unprotected_seconds':True})

    def test_submission_without_specific_approval_rolls_back(self):
        self.replay(scenario()[:4])
        before = self.journal.snapshot()
        with self.assertRaises(ValueError):self.journal.apply(scenario()[5])
        self.assertEqual(self.journal.snapshot(),before)

    def test_unprotected_timeout_and_stale_ack_block_risk(self):
        self.replay(scenario()[:4])
        late = self.journal.apply(event('timer',at='2026-10-16T14:00:11Z'))
        self.assertIn('PROTECTION_DEADLINE_EXCEEDED:DEMO',late['blockers'])

    def test_stale_observation_cannot_be_current_coverage(self):
        self.replay()
        view = self.journal.snapshot(as_of='2026-10-16T14:00:31Z')
        self.assertEqual(view['positions']['DEMO']['verified_protected_quantity'],0)
        self.assertTrue(view['new_risk_blocked'])
        self.assertEqual(self.journal.snapshot()['orders'][INTENT]['status'],'ACTIVE')

    def test_restart_and_duplicate_events_do_not_duplicate_fills_or_intents(self):
        before = self.replay()
        self.journal.close()
        self.journal = ProtectionJournal(self.path)
        self.assertEqual(self.replay(),before)
        same = scenario()[1]
        state=self.journal.apply({**same,'id':'second-observation'})
        self.assertEqual(state['positions']['DEMO']['quantity'],2)
        self.assertEqual(state['orders'][INTENT]['ref_id'],before['orders'][INTENT]['ref_id'])

    def test_execution_identity_conflict_latches_reconciliation(self):
        self.replay()
        conflict={**scenario()[1],'id':'conflicting-execution','quantity':3}
        state=self.journal.apply(conflict)
        self.assertIn('CONFLICTING_EXECUTION_IDENTITY',state['reconciliation_issues'])
        self.assertEqual(state['positions']['DEMO']['quantity'],2)

    def test_event_id_conflict_and_account_mismatch_rejected(self):
        self.replay()
        with self.assertRaises(ValueError):self.journal.apply({**scenario()[1],'quantity':5})
        with self.assertRaises(ValueError):self.journal.apply(event('timer',account_ref='another-account'))

    def test_pending_cancel_keeps_quantity_reserved(self):
        self.replay()
        state=self.journal.apply(event('cancel_request',intent_id=INTENT))
        self.assertEqual(state['positions']['DEMO']['reserved_exit_quantity'],2)
        with self.assertRaises(ValueError):
            self.journal.apply(event('protect_intent',id='replacement',intent_id=SECOND,
                symbol='DEMO',quantity=2,stop='39.5',valid_until=UNTIL))

    def test_timeout_after_submit_does_not_allow_resubmission_or_new_key(self):
        self.replay(scenario()[:-1])
        self.journal.apply(event('submit_unknown',intent_id=INTENT))
        with self.assertRaises(ValueError):self.journal.apply(event('submit',id='retry',intent_id=INTENT))
        with self.assertRaises(ValueError):
            self.journal.apply(event('protect_intent',id='new-key',intent_id=SECOND,
                symbol='DEMO',quantity=2,stop='39',valid_until=UNTIL))
        state=self.journal.apply(observed())
        self.assertEqual(state['positions']['DEMO']['verified_protected_quantity'],2)

    def test_partial_buy_requires_incremental_protection(self):
        self.replay()
        state=self.journal.apply(event('fill',id='buy-2',execution_id='buy-2',symbol='DEMO',
            side='buy',quantity=1,price='41',initial_stop='39'))
        self.assertEqual(state['positions']['DEMO']['unprotected_quantity'],1)
        self.assertEqual(state['positions']['DEMO']['initial_stop'],'39')
        self.assertTrue(state['new_risk_blocked'])

    def test_triggered_order_is_not_effective_stop_coverage(self):
        self.replay()
        state=self.journal.apply(observed(id='trigger',status='TRIGGERED'))
        self.assertEqual(state['positions']['DEMO']['verified_protected_quantity'],0)
        self.assertTrue(state['new_risk_blocked'])

    def test_partial_sell_requires_fresh_remaining_ack(self):
        self.replay()
        state=self.journal.apply(event('fill',id='sell-1',execution_id='sell-1',symbol='DEMO',side='sell',
            quantity=1,price='39',intent_id=INTENT))
        self.assertEqual(state['positions']['DEMO']['quantity'],1)
        self.assertEqual(state['positions']['DEMO']['verified_protected_quantity'],0)
        state=self.journal.apply(observed(id='remaining',filled=1))
        self.assertEqual(state['positions']['DEMO']['verified_protected_quantity'],1)

    def test_late_fill_after_cancel_is_booked_not_discarded(self):
        self.replay()
        self.journal.apply(event('cancel_request',intent_id=INTENT))
        self.journal.apply(observed(id='cancelled',status='CANCELLED'))
        state=self.journal.apply(event('fill',id='late-sell',execution_id='late-sell',symbol='DEMO',side='sell',
            quantity=2,price='39',intent_id=INTENT))
        self.assertEqual(state['positions']['DEMO']['quantity'],0)
        self.assertEqual(state['orders'][INTENT]['status'],'FILLED')

    def test_unknown_external_sell_preserves_fact_and_latches_halt(self):
        self.replay()
        state=self.journal.apply(event('fill',id='extra-sell',execution_id='external',symbol='DEMO',
            side='sell',quantity=3,price='38'))
        self.assertEqual(state['positions']['DEMO']['quantity'],-1)
        self.assertIn('UNEXPECTED_REVERSE_POSITION',state['reconciliation_issues'])
        self.assertIn('EXIT_ORDERS_REMAIN',state['blockers'])

    def test_changed_broker_terms_do_not_count_as_coverage(self):
        self.replay()
        state=self.journal.apply(observed(id='wrong-stop',stop='38'))
        self.assertIn('BROKER_ORDER_TERMS_CHANGED',state['reconciliation_issues'])
        self.assertEqual(state['positions']['DEMO']['verified_protected_quantity'],0)

    def test_missing_execution_requires_reconciliation_not_invented_sell(self):
        self.replay()
        state=self.journal.apply(observed(id='filled-but-no-trades',status='FILLED',filled=2))
        self.assertEqual(state['positions']['DEMO']['quantity'],2)
        self.assertIn('MISSING_OR_CONFLICTING_EXECUTIONS',state['reconciliation_issues'])

    def test_position_mismatch_does_not_silently_overwrite_ledger(self):
        self.replay()
        state=self.journal.apply(event('position_check',id='external-state',positions={'DEMO':1}))
        self.assertEqual(state['positions']['DEMO']['quantity'],2)
        self.assertIn('POSITION_SNAPSHOT_MISMATCH',state['reconciliation_issues'])

    def test_ack_outside_regular_session_is_not_current_effective_protection(self):
        self.replay()
        state=self.journal.snapshot(as_of='2026-10-16T20:01:00Z')
        self.assertEqual(state['positions']['DEMO']['verified_protected_quantity'],0)
        self.assertFalse(state['execution_ready'])

    def test_failure_before_commit_rolls_back_all_state(self):
        self.replay(scenario()[:3])
        before=self.journal.snapshot()
        with patch.object(ProtectionJournal,'_view',side_effect=RuntimeError('crash before persistence')):
            with self.assertRaises(RuntimeError):self.journal.apply(scenario()[3])
        self.assertEqual(self.journal.snapshot(),before)
        self.journal.apply(scenario()[3])
        self.assertEqual(len(self.journal.snapshot()['orders']),1)

    def test_flat_requires_post_fill_position_check(self):
        self.replay()
        state=self.journal.apply(event('fill',id='exit',execution_id='exit',symbol='DEMO',side='sell',
            quantity=2,price='39',intent_id=INTENT))
        self.assertEqual(state['positions']['DEMO']['protection_state'],'FLAT_UNVERIFIED')
        self.assertTrue(state['new_risk_blocked'])
        state=self.journal.apply(event('position_check',id='final-check',positions={}))
        self.assertEqual(state['positions']['DEMO']['protection_state'],'FLAT_VERIFIED')
        self.assertFalse(state['new_risk_blocked'])

    def test_approval_and_submission_timestamps_are_distinct(self):
        self.replay()
        order=self.journal.snapshot()['orders'][INTENT]
        self.assertEqual(order['approved_at'],AT)
        self.assertEqual(order['submitted_at'],AT)
        self.assertEqual(order['first_active_observed_at'],AT)

    def test_approved_intent_cannot_be_submitted_after_expiry(self):
        self.replay(scenario()[:5])
        with self.assertRaises(ValueError):
            self.journal.apply(event('submit',id='late-submit',at=UNTIL,intent_id=INTENT))

    def test_wrong_order_side_cannot_protect_long(self):
        self.replay()
        state=self.journal.apply(observed(id='wrong-side',side='buy'))
        self.assertEqual(state['positions']['DEMO']['verified_protected_quantity'],0)
        self.assertIn('BROKER_ORDER_TERMS_CHANGED',state['reconciliation_issues'])

    def test_cli_replays_fixture_and_same_events_are_idempotent(self):
        source=Path(__file__).resolve().parents[1]/'examples/protection_lifecycle.json'
        args=['replay-protection','--events',str(source),'--db',str(self.path)]
        with redirect_stdout(io.StringIO()) as output:
            self.assertEqual(main(args),0)
        first=json.loads(output.getvalue())['current']
        self.assertEqual(first['positions']['DEMO']['protection_state'],'FLAT_VERIFIED')
        with redirect_stdout(io.StringIO()) as output:
            self.assertEqual(main(args),0)
        self.assertEqual(json.loads(output.getvalue())['current'],first)

    def test_cli_reports_explicit_stale_clock_as_blocked(self):
        self.replay()
        with redirect_stdout(io.StringIO()) as output:
            code=main(['protection-status','--db',str(self.path),'--as-of','2026-10-16T14:00:31Z'])
        self.assertEqual(code,2)
        self.assertTrue(json.loads(output.getvalue())['new_risk_blocked'])

    def test_event_insert_failure_rolls_back_already_written_state(self):
        self.replay(scenario()[:3])
        before=self.journal.snapshot()
        self.journal.db.execute("CREATE TRIGGER fail_event BEFORE INSERT ON protection_events BEGIN SELECT RAISE(FAIL,'injected'); END")
        self.journal.db.commit()
        with self.assertRaises(sqlite3.IntegrityError):
            self.journal.apply(scenario()[3])
        self.assertEqual(self.journal.snapshot(),before)
        self.journal.db.execute('DROP TRIGGER fail_event')
        self.journal.db.commit()
        self.journal.apply(scenario()[3])
        self.assertEqual(len(self.journal.snapshot()['orders']),1)
