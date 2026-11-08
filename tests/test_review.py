from datetime import datetime
from pathlib import Path
import tempfile
from unittest import TestCase

from martin_agentic.audit import AuditTrail
from martin_agentic.review import session_review
from martin_agentic.transport import TransportError


class ReviewTests(TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.db=Path(self.tmp.name)/'audit.sqlite'
        self.output=Path(self.tmp.name)/'reports'
        audit=AuditTrail(self.db);audit.close()
        self.clock=lambda:datetime.fromisoformat('2026-10-16T20:10:00+00:00')

    def tearDown(self):self.tmp.cleanup()

    def broker(self,*args,**kwargs):
        return {'observations':[{'account':'••••0000','last_observed_at':self.clock().isoformat(),
            'read_healthy':True,'blockers':[],'positions':{},'open_equity_orders':0,
            'other_asset_open_orders':0,'pending_approvals':0}]}

    def test_offline_report_never_claims_broker_flat(self):
        def forbidden(*args,**kwargs):self.fail('Offline export must not query broker')
        report=session_review(self.db,session_date='2026-10-16',output_dir=self.output,observer=forbidden)
        self.assertFalse(report['broker_end_of_day_reconciliation_performed'])
        self.assertFalse(report['broker_end_of_day_flat_verified'])

    def test_before_close_is_rejected_before_official_read(self):
        def forbidden(*args,**kwargs):self.fail('Must not read before allowed close check')
        with self.assertRaisesRegex(ValueError,'requires this session'):
            session_review(self.db,session_date='2026-10-16',output_dir=self.output,account_suffix='0000',verify_broker=True,
                observer=forbidden,clock=lambda:datetime.fromisoformat('2026-10-16T14:00:00+00:00'))

    def test_fresh_official_empty_snapshot_can_confirm_close(self):
        report=session_review(self.db,session_date='2026-10-16',output_dir=self.output,account_suffix='0000',
            verify_broker=True,observer=self.broker,clock=self.clock)
        self.assertTrue(report['broker_end_of_day_flat_verified'])
        self.assertTrue(report['healthy'])
        self.assertTrue(Path(report['artifacts']['markdown']).exists())

    def test_read_failure_leaves_failure_report_not_fictional_flat(self):
        def failure(*args,**kwargs):raise RuntimeError('do not leak private upstream data')
        report=session_review(self.db,session_date='2026-10-16',output_dir=self.output,account_suffix='0000',
            verify_broker=True,observer=failure,clock=self.clock)
        self.assertFalse(report['healthy'])
        self.assertFalse(report['broker_end_of_day_reconciliation_performed'])
        self.assertNotIn('private',Path(report['artifacts']['json']).read_text())

    def test_transient_disconnect_retries_read_once(self):
        calls=[]
        def retry(*args,**kwargs):
            calls.append(1)
            if len(calls)==1:raise TransportError('APP_SERVER_DISCONNECTED')
            return self.broker()
        result=session_review(self.db,session_date='2026-10-16',output_dir=self.output,account_suffix='0000',
            verify_broker=True,observer=retry,clock=self.clock)
        self.assertTrue(result['broker_end_of_day_flat_verified'])
        self.assertEqual(result['closing_read_attempts'],2)
        self.assertIsNone(result['closing_read_failure_detail'])

    def test_permission_error_is_not_retried_or_marked_flat(self):
        calls=[]
        def denied(*args,**kwargs):
            calls.append(1);raise TransportError('APP_SERVER_HOST_PERMISSION_DENIED')
        result=session_review(self.db,session_date='2026-10-16',output_dir=self.output,account_suffix='0000',
            verify_broker=True,observer=denied,clock=self.clock)
        self.assertFalse(result['healthy'])
        self.assertEqual(len(calls),1)
        self.assertEqual(result['closing_read_failure_detail']['code'],'APP_SERVER_HOST_PERMISSION_DENIED')

    def test_transient_failure_is_bounded_and_unknown_errors_are_redacted(self):
        calls=[]
        def disconnected(*args,**kwargs):
            calls.append(1);raise TransportError('APP_SERVER_DISCONNECTED')
        result=session_review(self.db,session_date='2026-10-16',output_dir=self.output,account_suffix='0000',
            verify_broker=True,observer=disconnected,clock=self.clock)
        self.assertEqual(len(calls),2)
        self.assertFalse(result['broker_end_of_day_flat_verified'])
        def unknown(*args,**kwargs):raise TransportError('secret-private-token')
        result=session_review(self.db,session_date='2026-10-16',output_dir=self.output,account_suffix='0000',
            verify_broker=True,observer=unknown,clock=self.clock)
        self.assertEqual(result['closing_read_failure_detail']['code'],'BROKER_READ_UNCLASSIFIED')
        self.assertNotIn('secret-private-token',Path(result['artifacts']['json']).read_text())

    def test_residual_position_prevents_flat_claim(self):
        def held(*args,**kwargs):
            r=self.broker();r['observations'][0]['positions']={'DEMO':{'quantity':'1'}}
            return r
        report=session_review(self.db,session_date='2026-10-16',output_dir=self.output,account_suffix='0000',
            verify_broker=True,observer=held,clock=self.clock)
        self.assertTrue(report['broker_end_of_day_reconciliation_performed'])
        self.assertFalse(report['broker_end_of_day_flat_verified'])

    def test_stale_snapshot_cannot_confirm_close(self):
        def stale(*args,**kwargs):
            r=self.broker();r['observations'][0]['last_observed_at']='2026-10-16T20:00:00Z'
            return r
        report=session_review(self.db,session_date='2026-10-16',output_dir=self.output,account_suffix='0000',
            verify_broker=True,observer=stale,clock=self.clock)
        self.assertFalse(report['healthy'])
        self.assertFalse(report['broker_end_of_day_flat_verified'])
