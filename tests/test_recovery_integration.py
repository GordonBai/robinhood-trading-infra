from copy import deepcopy
from datetime import timedelta
import json
from unittest import TestCase
from unittest.mock import patch

from martin_agentic.audit import canonical
from martin_agentic.native_operations import NativeOperations
import test_recovery


class RecoveryIntegrationTests(TestCase):
    def setUp(self):
        self.fixture = test_recovery.RecoveryTests('test_ack_keeps_failure_state_issues_risk_and_manual_provenance')
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.book = self.fixture.book
        state = self.book.state()
        # This positive fixture has only explanation issues. A separate test
        # retains timeout/risk failures to verify they remain active blockers.
        state['issues'] = [i for i in state['issues'] if not i.startswith('PROTECTION_WAIT_EXCEEDED')]
        self.book._save(state)
        payload = {'action_id':'old-ready-protect','trade_id':'old','kind':'protect',
                   'account':'••••0000','synthetic':False,'parameters':{'symbol':'DEMO',
                   'side':'sell','type':'stop_market','quantity':'1','stop_price':'39',
                   'market_hours':'regular_hours','time_in_force':'gtc'},'ref_id':'old-logical-ref',
                   'target_order_id':None,'reason':'Failed module historical protection action',
                   'created_at':self.fixture.now.isoformat(),
                   'expires_at':(self.fixture.now+timedelta(seconds=120)).isoformat(),
                   'plan':{'symbol':'DEMO'}}
        self.book.db.execute('INSERT INTO live_actions VALUES(?,?,?,?,?,?,?,NULL)',
            ('old-ready-protect','old','protect','READY',canonical(payload),
             self.fixture.now.isoformat(),self.fixture.now.isoformat()))
        for kind in ('exit','cancel'):
            extra = deepcopy(payload)
            extra.update(action_id='old-ready-'+kind,kind=kind)
            extra['parameters'] = {'order_id':'stop'} if kind=='cancel' else {
                'symbol':'DEMO','side':'sell','type':'market','quantity':'1',
                'market_hours':'regular_hours','time_in_force':'gfd'}
            if kind=='cancel':
                extra['target_order_id'] = 'stop'
            self.book.db.execute('INSERT INTO live_actions VALUES(?,?,?,?,?,?,?,NULL)',
                (extra['action_id'],'old',kind,'READY',canonical(extra),
                 self.fixture.now.isoformat(),self.fixture.now.isoformat()))
        self.book.db.commit()
        self.fixture.acknowledged()
        # Real-mode bookkeeping fixtures stay inside their own canonical temp
        # runtime and explicitly establish lineage; no production sidecar access.
        binding=patch('martin_agentic.session_lineage.CANONICAL_RUNTIME',self.fixture.path.parent)
        binding.start();self.addCleanup(binding.stop)
        self.book.reconcile(self.fixture.snapshot)
        from martin_agentic.session_lineage import bootstrap_session_lineage
        bootstrap_session_lineage(self.fixture.path,self.book.policy,current_db=self.fixture.path,
            current_policy=self.book.policy,clock=lambda:self.fixture.now)
        self.original_trade = deepcopy(self.book.state()['trades']['old'])
        self.original_action = next(a for a in self.book.actions() if a['id']=='old-ready-protect')
        self.plan = {'symbol':'DEMO','quantity':1,'entry_ceiling':'40','stop':'39','cost_buffer_per_share':'0.05'}
        self.quote = {'symbol':'DEMO','bid':'39.99','ask':'40',
                      'bid_at':self.fixture.now.isoformat(),'ask_at':self.fixture.now.isoformat(),
                      'observed_at':self.fixture.now.isoformat()}

    def propose(self):
        return self.book.propose('new-supervised',self.plan,self.quote,self.fixture.snapshot,
                                 evidence={'source_sha256':'fixture-source-only','synthetic':False})

    def test_ack_allows_new_supervised_proposal_without_rewriting_failed_trade(self):
        self.assertEqual(self.book.active_issues(self.fixture.snapshot),[])
        self.book.reconcile(self.fixture.snapshot)
        proposal = self.propose()
        self.assertTrue(proposal['action_id'])
        self.assertEqual(self.book.state()['trades']['old'],self.original_trade)
        self.assertEqual(next(a for a in self.book.actions() if a['id']=='old-ready-protect'),self.original_action)
        claimed = self.book.claim()
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed['trade_id'],'new-supervised')

    def test_repeated_new_unmanaged_exposure_revokes_issue_coverage(self):
        changed = deepcopy(self.fixture.snapshot)
        changed['positions']['NEW'] = {'quantity':'1','available_for_sells':'1','held_for_sells':'0'}
        self.book.reconcile(changed)
        self.assertIn('UNMANAGED_POSITION_OR_ORDER',self.book.active_issues(changed))
        self.assertTrue(any('RECOVERY_ISSUE_RECURRED' in i for i in self.book.active_issues(changed)))

    def test_timeout_and_risk_failure_are_never_covered(self):
        state = self.book.state()
        state['issues'].extend(['PROTECTION_WAIT_EXCEEDED:DEMO','PROJECT_MAX_DRAWDOWN_LOCKED'])
        self.book._save(state)
        self.book.db.commit()
        with self.assertRaisesRegex(ValueError,'UNRESOLVED|BLOCKED|EXISTS|ACTIVE'):
            self.propose()
        self.assertIn('PROTECTION_WAIT_EXCEEDED:DEMO',self.book.active_issues(self.fixture.snapshot))
        self.assertIn('PROJECT_MAX_DRAWDOWN_LOCKED',self.book.active_issues(self.fixture.snapshot))

    def test_old_ready_action_cannot_be_claimed_or_dispatched_after_recovery(self):
        self.assertIsNone(self.book.claim())
        for kind in ('protect','exit','cancel'):
            with self.subTest(kind=kind), self.assertRaisesRegex(ValueError,'RECOVER|RETIRED'):
                NativeOperations(self.book).preview('old-ready-'+kind,self.fixture.snapshot,quote=self.quote)
        self.assertEqual(next(a for a in self.book.actions() if a['id']=='old-ready-protect'),self.original_action)

