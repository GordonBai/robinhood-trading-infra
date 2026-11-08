from datetime import datetime,timedelta
import json
from pathlib import Path
import tempfile
from unittest import TestCase

from entry_authority_fixture import TestExecutionBook as ExecutionBook
from martin_agentic.handoff import operator_packet
from test_broker import snapshot


class HandoffTests(TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.now=datetime.fromisoformat('2026-10-16T14:00:00Z')
        self.policy=json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        self.book=ExecutionBook(Path(self.tmp.name)/'handoff.sqlite',self.policy,simulation=True,clock=lambda:self.now)
    def tearDown(self):self.book.close();self.tmp.cleanup()
    def queue(self):
        at=self.now.isoformat();snap=snapshot(at=at);snap['synthetic']=True
        self.book.propose('signal',{'symbol':'DEMO','quantity':2,'entry_ceiling':'40','stop':'39','cost_buffer_per_share':'0.05'},
            {'symbol':'DEMO','bid':'39.99','ask':'40','bid_at':at,'ask_at':at,'observed_at':at},snap,
            evidence={'synthetic':True,'source_sha256':'fixture'})
    def test_empty_queue_never_invents_order(self):
        self.assertFalse(operator_packet(self.book)['available'])
    def test_concrete_entry_and_conditional_protection_are_not_approval(self):
        self.queue();packet=operator_packet(self.book)
        self.assertFalse(packet['specific_order_confirmed']);self.assertFalse(packet['submission_allowed_by_packet'])
        self.assertEqual(packet['parameters']['quantity'],'2')
        self.assertEqual(packet['contingent_initial_protection']['stop_price'],'39')
        self.assertFalse(self.book.audit.verify()['events_verified'])
    def test_expired_order_cannot_silently_be_presented_as_current(self):
        self.queue();self.now+=timedelta(seconds=16)
        self.assertTrue(operator_packet(self.book)['expired'])
    def test_unknown_submission_preserves_ref_and_requests_reconciliation(self):
        self.queue();a=self.book.claim();self.book.report(a['action_id'],{'status':'unknown'})
        packet=operator_packet(self.book)
        self.assertEqual(packet['state'],'UNKNOWN');self.assertEqual(packet['ref_id'],a['ref_id'])
        self.assertFalse(packet['submission_allowed_by_packet'])

class WorkerRestrictionTests(TestCase):
    def test_review_worker_catalog_excludes_write_tools(self):
        from martin_agentic.agent_worker import CodexOrderWorker
        captured=[]
        def runner(command,prompt):
            captured.append(command);return {'status':'review_only','message':'test'}
        with tempfile.TemporaryDirectory() as directory:
            worker=CodexOrderWorker('.',directory,runner=runner)
            worker({'kind':'entry','synthetic':False})
        command=captured[0]
        configured=next(v for v in command if v.startswith('mcp_servers.robinhood-trading.enabled_tools='))
        allowed=json.loads(configured.split('=',1)[1])
        self.assertIn('review_equity_order',allowed)
        self.assertFalse(any(t.startswith(('place_','cancel_','replace_')) for t in allowed))
        self.assertEqual(command[command.index('--sandbox')+1],'read-only')
    def test_connectivity_probe_has_only_three_read_tools(self):
        from martin_agentic.agent_worker import CodexOrderWorker
        captured=[]
        with tempfile.TemporaryDirectory() as directory:
            CodexOrderWorker('.',directory,runner=lambda c,p:captured.append(c) or {'status':'review_only'})({'kind':'probe','synthetic':False})
        configured=next(v for v in captured[0] if v.startswith('mcp_servers.robinhood-trading.enabled_tools='))
        self.assertEqual(set(json.loads(configured.split('=',1)[1])),{'get_accounts','get_portfolio','get_trade_approval_setting'})
