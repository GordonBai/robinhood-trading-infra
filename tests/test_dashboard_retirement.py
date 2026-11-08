"""Closed incident displays use audited retirement, not raw historical status."""
from datetime import timedelta
import json
from unittest import TestCase
from unittest.mock import patch

from martin_agentic.audit import canonical
from martin_agentic.dashboard import DashboardData
import test_manual_takeover


class DashboardRetirementTests(TestCase):
    def setUp(self):
        self.f=test_manual_takeover.ManualTakeoverTests('test_manual_receipt_closes_effective_intent_without_fabrication_or_risk_reset')
        self.f.setUp();self.addCleanup(self.f.tearDown)
        payload={'policy':self.f.book.policy,'parameters':{'symbol':'DEMO','quantity':'1','type':'market'},
                 'ref_id':'old-exit-ref','plan':{'symbol':'DEMO'}}
        self.f.book.db.execute('UPDATE live_actions SET payload=? WHERE id=?',(canonical(payload),'old-exit'))
        self.f.book.db.commit()
        case=self.f.opened();self.f.cases.verify(case['id'],self.f.snapshot)
        self.f.cases.acknowledge(case['id'],self.f.snapshot,reason='exact fresh formal fixture evidence')
        self.status=self.f.path.with_name('status.json');self.status.write_text(json.dumps({
            'running':False,'observed_at':self.f.now.isoformat(),'reason':'MANUAL_INTERVENTION_REQUIRED'}))
        self.data=DashboardData(self.f.path,self.status,self.f.path.parent/'captures',
            self.f.path.parent/'release.json',clock=lambda:self.f.now+timedelta(days=1))

    def inspect(self):
        with patch('martin_agentic.dashboard.process_matches',return_value=False):
            return self.data.snapshot()

    def test_closed_display_retires_historical_waiting_without_rewriting_or_live_authority(self):
        before=self.f.path.read_bytes();result=self.inspect()
        self.assertEqual(result['actions'][0]['state'],'WAITING_USER')
        self.assertTrue(result['actions'][0]['effective_retired'])
        self.assertTrue(result['recovery_disposition']['historical_only'])
        self.assertFalse(result['recovery_disposition']['live_admission_authorized'])
        self.assertFalse(result['runtime']['healthy_now'])
        self.assertEqual(self.f.path.read_bytes(),before)

    def test_tampered_ack_cannot_hide_pending_history(self):
        self.f.book.db.execute("UPDATE recovery_cases SET payload=replace(payload,'ACKNOWLEDGED','ALTERED')")
        self.f.book.db.commit();result=self.inspect()
        self.assertFalse(result['actions'][0]['effective_retired'])
        self.assertTrue(result['recovery_disposition']['blockers'])

    def test_reported_running_does_not_use_historical_clock_to_hide_stale_live_actions(self):
        self.status.write_text(json.dumps({'running':True,'observed_at':self.f.now.isoformat()}))
        result=self.inspect()
        self.assertFalse(result['actions'][0]['effective_retired'])
        self.assertFalse(result['recovery_disposition']['historical_only'])
        self.assertTrue(result['recovery_disposition']['blockers'])
