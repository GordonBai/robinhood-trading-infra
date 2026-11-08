"""Owner checkpoint CAS and no plugin computation under a write lock."""
from datetime import timedelta
from unittest import TestCase
from unittest.mock import patch
import json
import tempfile

import test_signal_host_isolation as fixture
from shadow_fixture import events
from martin_agentic.audit import canonical


class SignalPrepareCommitTests(TestCase):
    books=fixture.SignalHostIsolationTests.books
    event=fixture.SignalHostIsolationTests.event
    def test_computation_has_no_owner_write_transaction(self):
        with tempfile.TemporaryDirectory() as directory:
            _,host=self.books(directory);event=self.event(events()[1]);self.now=self.now.fromisoformat(event['at'])
            original=host.plugin.decide
            def decide(request):
                self.assertFalse(host.book.db.in_transaction)
                return original(request)
            with patch.object(host.plugin,'decide',side_effect=decide):host.consume(event)

    def test_checkpoint_cas_rejects_obsolete_result(self):
        with tempfile.TemporaryDirectory() as directory:
            _,host=self.books(directory);event=self.event(events()[1]);self.now=self.now.fromisoformat(event['at'])
            prepared=host.prepare(event);decision=host.plugin.decide(prepared.request)
            state=host.state();state['revision']='other-owner-write'
            host.book.db.execute('UPDATE live_strategy SET payload=?',(canonical(state),));host.book.db.commit()
            with self.assertRaisesRegex(ValueError,'CHECKPOINT_CHANGED'):host.commit(prepared,decision)
            self.assertEqual(host.state(),state)
            self.assertEqual(host.book.db.execute('SELECT count(*) FROM live_inputs').fetchone()[0],0)

    def test_host_disposition_during_computation_is_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            _,host=self.books(directory);event=self.event(events()[1]);self.now=self.now.fromisoformat(event['at'])
            prepared=host.prepare(event);decision=host.plugin.decide(prepared.request)
            state=host.state();state['signal_results']={'old':{'accepted':False}}
            host.book.db.execute('UPDATE live_strategy SET payload=?',(canonical(state),));host.book.db.commit()
            host.commit(prepared,decision)
            self.assertEqual(host.state()['signal_results'],state['signal_results'])
            self.assertIsNone(host.commit(prepared,decision))

    def test_input_is_detached_and_delayed_results_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            _,host=self.books(directory);event=self.event(events()[1]);self.now=self.now.fromisoformat(event['at'])
            prepared=host.prepare(event);decision=host.plugin.decide(prepared.request)
            event['data']['universe_id']='mutated'
            self.assertNotEqual(prepared.request.market['event']['data']['universe_id'],'mutated')
            self.now+=timedelta(seconds=31)
            with self.assertRaisesRegex(ValueError,'STALE_STRATEGY_EVENT'):host.commit(prepared,decision)
            self.assertEqual(host.book.db.execute('SELECT count(*) FROM live_inputs').fetchone()[0],0)

    def test_expired_proposal_never_enters_private_outbox(self):
        with tempfile.TemporaryDirectory() as directory:
            _,host=self.books(directory)
            found=False
            for raw in events()[1:]:
                event=self.event(raw);self.now=self.now.fromisoformat(event['at'])
                prepared=host.prepare(event);decision=host.plugin.decide(prepared.request)
                if decision.proposals:
                    before=host.state();self.now+=timedelta(seconds=16)
                    with self.assertRaisesRegex(ValueError,'STRATEGY_DECISION_EXPIRED'):
                        host.commit(prepared,decision)
                    self.assertEqual(host.state(),before)
                    self.assertFalse(host.book.db.execute('SELECT 1 FROM live_inputs WHERE id=?',(event['id'],)).fetchone())
                    found=True;break
                host.commit(prepared,decision)
            self.assertTrue(found,'fixture must exercise a real proposal')

    def test_lock_wait_expiry_is_rechecked_under_transaction(self):
        for with_proposal in (False,True):
            with self.subTest(with_proposal=with_proposal),tempfile.TemporaryDirectory() as directory:
                _,host=self.books(directory)
                for raw in events()[1:]:
                    event=self.event(raw);self.now=self.now.fromisoformat(event['at'])
                    prepared=host.prepare(event);decision=host.plugin.decide(prepared.request)
                    if not with_proposal or decision.proposals:break
                    host.commit(prepared,decision)
                if with_proposal:self.assertTrue(decision.proposals)
                before=host.state();count=host.book.db.execute('SELECT count(*) FROM live_inputs').fetchone()[0]
                original_begin=host.book._begin
                def delayed_begin():
                    self.now+=timedelta(seconds=16 if with_proposal else 31)
                    return original_begin()
                reason='STRATEGY_DECISION_EXPIRED' if with_proposal else 'STALE_STRATEGY_EVENT'
                with patch.object(host.book,'_begin',side_effect=delayed_begin):
                    with self.assertRaisesRegex(ValueError,reason):host.commit(prepared,decision)
                self.assertEqual(host.state(),before)
                self.assertEqual(host.book.db.execute('SELECT count(*) FROM live_inputs').fetchone()[0],count)
                self.assertFalse(host.book.db.in_transaction)

    def test_archive_delay_expiry_rolls_back_all_writes(self):
        import martin_luk.live_signals as module
        with tempfile.TemporaryDirectory() as directory:
            _,host=self.books(directory);event=self.event(events()[1]);self.now=self.now.fromisoformat(event['at'])
            prepared=host.prepare(event);decision=host.plugin.decide(prepared.request);before=host.state()
            archive=module.archive_event
            def delayed_archive(*args,**kwargs):
                result=archive(*args,**kwargs);self.now+=timedelta(seconds=31);return result
            with patch.object(module,'archive_event',side_effect=delayed_archive):
                with self.assertRaisesRegex(ValueError,'STALE_STRATEGY_EVENT'):host.commit(prepared,decision)
            self.assertEqual(host.state(),before)
            self.assertEqual(host.book.db.execute('SELECT count(*) FROM live_inputs').fetchone()[0],0)
