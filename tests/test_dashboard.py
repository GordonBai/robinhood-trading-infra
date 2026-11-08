from datetime import datetime
import http.client
import json
from pathlib import Path
import tempfile
import threading
from unittest import TestCase
from unittest.mock import patch

from martin_agentic.dashboard import DashboardData,make_server,clean
from martin_agentic.engineering import EngineeringPlans
from entry_authority_fixture import TestExecutionBook as ExecutionBook


class DashboardTests(TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
        self.db=self.root/'live.sqlite';self.now=datetime.fromisoformat('2026-10-16T14:40:00Z')
        self.policy=json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        self.book=ExecutionBook(self.db,self.policy,simulation=False,clock=lambda:self.now)
        self.status=self.root/'status.json';self.status.write_text(json.dumps({'running':True,'healthy':True,
            'pid':42,'observed_at':self.now.isoformat(),'broker':{'account':'••••0000','portfolio':{'total_value':'500'}}}))
        self.data=DashboardData(self.db,self.status,self.root/'captures',self.root/'release.json',clock=lambda:self.now)
    def tearDown(self):self.book.close();self.tmp.cleanup()
    def plan(self):return {'symbol':'SOFI','quantity':1,'entry_ceiling':'15.25','stop':'14.90','cost_buffer_per_share':'0.05'}
    def test_capture_health_is_displayed_without_claiming_entry_admission(self):
        (self.root/'release.json').write_text(json.dumps({'production_ready':False}))
        status=json.loads(self.status.read_text());status.update(healthy=False,
            read_only_capture=True,read_only_capture_healthy=True,agent_mode='disabled',
            new_entries_blocked=True,attention_required=False)
        self.status.write_text(json.dumps(status))
        with patch('martin_agentic.dashboard.process_matches',return_value=True):
            result=self.data.snapshot()
        self.assertTrue(result['runtime']['healthy_now'])
        self.assertEqual(result['runtime']['health_scope'],'read_only_capture')
        self.assertTrue(result['runtime']['new_entries_blocked']);self.assertEqual(result['actions'],[])
        self.assertFalse(result['release']['production_ready'])

    def test_capture_attention_and_dead_process_prevent_green_health(self):
        status=json.loads(self.status.read_text());status.update(healthy=False,
            read_only_capture=True,read_only_capture_healthy=True,agent_mode='disabled',
            attention_required=True)
        self.status.write_text(json.dumps(status))
        with patch('martin_agentic.dashboard.process_matches',return_value=True):
            self.assertFalse(self.data.snapshot()['runtime']['healthy_now'])
        status['attention_required']=False;self.status.write_text(json.dumps(status))
        with patch('martin_agentic.dashboard.process_matches',return_value=False):
            self.assertFalse(self.data.snapshot()['runtime']['healthy_now'])
    def archive_quote(self,identity,status,at):
        from martin_agentic.capture_archive import ensure_archive_schema,archive_event,archive_decision
        ensure_archive_schema(self.book.db)
        event={'id':identity,'kind':'quote','at':at,'synthetic_input':False,'source_sha256':'a'*64,
            'data':{'symbol':'DEMO','bid':'10','ask':'10.01','bid_at':at,'ask_at':at,'observed_at':at}}
        self.book.db.execute('BEGIN IMMEDIATE')
        archive_event(self.book.db,event,consumed_at=at)
        archive_decision(self.book.db,event_id=identity,result_id='owner',context={'status':status},recorded_at=at)
        self.book.db.commit()

    def test_owner_consumed_quotes_survive_background_pending_without_legacy_capture_files(self):
        self.archive_quote('accepted','CONSUMED',self.now.isoformat())
        self.archive_quote('rejected-newer','NOT_CONSUMED',self.now.isoformat())
        result=self.data.snapshot()
        self.assertEqual(len(result['quotes']),1)
        self.assertTrue(result['quotes'][0]['fresh_now']);self.assertTrue(result['quotes'][0]['owner_consumed'])
        self.assertEqual(result['quotes'][0]['source_type'],'owner_consumed_quote_archive')
        self.assertFalse(result['data_quality']['provenance_cryptographically_authenticated'])

    def test_archive_quotes_keep_original_age_and_rejected_quotes_do_not_appear(self):
        from datetime import timedelta
        self.archive_quote('rejected','NOT_CONSUMED',self.now.isoformat())
        self.assertEqual(self.data.snapshot()['quotes'],[])
        old=(self.now-timedelta(seconds=20)).isoformat();self.archive_quote('old-accepted','CONSUMED',old)
        quote=self.data.snapshot()['quotes'][0]
        self.assertEqual(quote['observed_at'],old);self.assertFalse(quote['fresh_now'])
        self.assertEqual(quote['age_seconds'],20)
    def draft(self):
        return EngineeringPlans(self.book).draft(self.plan(),quote={'observed_at':self.now.isoformat()},
            scope_authorization='USER_EXPLICITLY_ALLOWED_SMALL_ENGINEERING_TEST')
    def test_engineering_draft_is_separate_from_strategy_and_actual_orders(self):
        draft=self.draft();result=self.data.snapshot()
        self.assertEqual(result['engineering_plans'][0]['purpose'],'engineering')
        self.assertFalse(draft['specific_order_confirmed']);self.assertFalse(draft['submitted'])
        self.assertEqual(result['actions'],[]);self.assertEqual(result['trades'],[]);self.assertEqual(result['fills'],[])
        self.assertEqual(result['performance']['strategy']['closed_trades'],0)
    def test_dashboard_read_does_not_mutate_database_or_create_live_order(self):
        self.draft();before=self.book.db.execute('SELECT COUNT(*) FROM audit_events').fetchone()[0]
        self.data.snapshot();self.data.snapshot()
        self.assertEqual(self.book.db.execute('SELECT COUNT(*) FROM audit_events').fetchone()[0],before)
        self.assertFalse(self.book.actions())
    def test_reported_running_cannot_hide_dead_process(self):
        with patch('martin_agentic.dashboard.process_matches',return_value=False):
            result=self.data.snapshot()
        self.assertFalse(result['runtime']['healthy_now']);self.assertTrue(result['runtime']['reported_running'])
    def test_old_snapshot_is_stale_even_if_process_exists(self):
        content=json.loads(self.status.read_text());content['observed_at']='2026-10-16T13:00:00Z';self.status.write_text(json.dumps(content))
        with patch('martin_agentic.dashboard.process_matches',return_value=True):result=self.data.snapshot()
        self.assertFalse(result['runtime']['healthy_now'])
    def test_full_accounts_credentials_and_internal_account_key_are_removed(self):
        result=clean({'account_number':'SECRET','details':{'access_token':'SECRET','account_key':'private','symbol':'SOFI'}})
        self.assertNotIn('SECRET',json.dumps(result));self.assertNotIn('account_key',json.dumps(result))
        self.assertEqual(result['details']['symbol'],'SOFI')
    def test_missing_files_are_not_empty_proof_of_healthy_trading(self):
        missing=DashboardData(self.root/'missing.sqlite',self.root/'missing.json',self.root/'missing',self.root/'none',clock=lambda:self.now).snapshot()
        self.assertIn('DATABASE_MISSING',missing['errors']);self.assertFalse(missing['runtime']['healthy_now'])
        self.assertFalse((self.root/'missing.sqlite').exists())
    def test_http_local_json_no_cors_no_arbitrary_files_and_no_writes(self):
        server=make_server(self.data,0);thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        try:
            connection=http.client.HTTPConnection('127.0.0.1',server.server_port)
            connection.request('GET','/api/state');response=connection.getresponse();content=json.loads(response.read())
            self.assertEqual(response.status,200);self.assertTrue(content['read_only'])
            self.assertIsNone(response.getheader('Access-Control-Allow-Origin'))
            connection.request('POST','/api/place',body='{}');response=connection.getresponse();response.read();self.assertEqual(response.status,405)
            connection.request('GET','/../../.env');response=connection.getresponse();response.read();self.assertEqual(response.status,404)
            connection.request('GET','/api/state',headers={'Host':'evil.example'});response=connection.getresponse();response.read();self.assertEqual(response.status,403)
            connection.request('GET','/api/state',headers={'Origin':'https://evil.example'});response=connection.getresponse();response.read();self.assertEqual(response.status,403)
            connection.close()
        finally:server.shutdown();server.server_close();thread.join(timeout=2)
    def test_engineering_realized_pnl_never_enters_strategy_performance(self):
        state=self.book.state();state['trades']={'engineering-fixture':{'id':'engineering-fixture','purpose':'engineering','phase':'FLAT_VERIFIED','plan':{'symbol':'DEMO'}}}
        self.book.db.execute('UPDATE live_state SET payload=? WHERE id=1',(json.dumps(state),))
        self.book.db.executescript('CREATE TABLE broker_current(id INTEGER PRIMARY KEY,payload TEXT); CREATE TABLE broker_executions(id TEXT PRIMARY KEY,payload TEXT)')
        orders={}
        for ident,side,price in [('buy','buy','10'),('sell','sell','10.5')]:
            payload={'ref_id':ident,'plan':{'symbol':'DEMO'},'parameters':{'symbol':'DEMO'}}
            self.book.db.execute('INSERT INTO live_actions VALUES(?,?,?,?,?,?,?,NULL)',(ident,'engineering-fixture','entry' if side=='buy' else 'exit','OBSERVED',json.dumps(payload),self.now.isoformat(),self.now.isoformat()))
            orders[ident]={'ref_id':ident}
            fill={'id':ident,'order_id':ident,'symbol':'DEMO','side':side,'quantity':'1','price':price,'fees':'0','executed_at':self.now.isoformat()}
            self.book.db.execute('INSERT INTO broker_executions VALUES(?,?)',(ident,json.dumps(fill)))
        self.book.db.execute('INSERT INTO broker_current VALUES(1,?)',(json.dumps({'orders':orders}),));self.book.db.commit()
        result=self.data.snapshot()
        self.assertEqual(result['performance']['engineering'],{'closed_trades':1,'realized_pnl':'0.5'})
        self.assertEqual(result['performance']['strategy'],{'closed_trades':0,'realized_pnl':'0'})
        self.assertTrue(all(f['purpose']=='engineering' for f in result['fills']))

    def test_exact_native_order_id_classifies_rewritten_ref_only_in_bound_account(self):
        state=self.book.state();state['account_key']='bound-account'
        state['trades']={'engineering-fixture':{'id':'engineering-fixture','purpose':'engineering','phase':'ENTRY_WORKING','plan':{'symbol':'DEMO'}}}
        self.book._save(state)
        self.book.db.executescript('CREATE TABLE broker_current(id INTEGER PRIMARY KEY,payload TEXT); CREATE TABLE broker_executions(id TEXT PRIMARY KEY,payload TEXT)')
        payload={'ref_id':'logical','parameters':{'symbol':'DEMO'}}
        self.book.db.execute('INSERT INTO live_actions VALUES(?,?,?,?,?,?,?,NULL)',
            ('action','engineering-fixture','entry','OBSERVED',json.dumps(payload),self.now.isoformat(),self.now.isoformat()))
        self.book.db.execute('INSERT INTO external_broker_order_ids VALUES(?,?,?,?)',('actual-order','action','receipt','bound-account'))
        broker={'account_key':'bound-account','orders':{'actual-order':{'ref_id':'rewritten'}}}
        self.book.db.execute('INSERT INTO broker_current VALUES(1,?)',(json.dumps(broker),))
        fill={'id':'execution','order_id':'actual-order','side':'buy','symbol':'DEMO','quantity':'1','price':'10','fees':'0'}
        self.book.db.execute('INSERT INTO broker_executions VALUES(?,?)',('execution',json.dumps(fill)))
        self.book.db.commit()
        self.assertEqual(self.data.snapshot()['fills'][0]['purpose'],'engineering')
        broker['account_key']='other-account'
        self.book.db.execute('UPDATE broker_current SET payload=?',(json.dumps(broker),));self.book.db.commit()
        self.assertIsNone(self.data.snapshot()['fills'][0]['trade_id'])

    def test_scope_and_budget_are_required_for_engineering_draft(self):
        plans=EngineeringPlans(self.book)
        with self.assertRaises(ValueError):plans.draft(self.plan(),quote={},scope_authorization='unconfirmed')
        with self.assertRaises(ValueError):plans.draft({**self.plan(),'quantity':100},quote={},scope_authorization='USER_EXPLICITLY_ALLOWED_SMALL_ENGINEERING_TEST')
    def test_engineering_cannot_be_queued_as_qualified_strategy(self):
        from test_broker import snapshot
        snap=snapshot(at=self.now.isoformat());snap['synthetic']=False
        q={'symbol':'SOFI','bid':'15.19','ask':'15.20','observed_at':self.now.isoformat(),'bid_at':self.now.isoformat(),'ask_at':self.now.isoformat()}
        # Scope/purpose validation is isolated from unrelated session admission.
        from unittest.mock import patch
        with patch.object(self.book,'_check_entry_gate'),self.assertRaisesRegex(ValueError,'PURPOSE_CONFLICT'):
            self.book.propose('engineering-x',self.plan(),q,snap,evidence={'purpose':'engineering','synthetic':False,'source_sha256':'fixture'})
        with patch.object(self.book,'_check_entry_gate'),self.assertRaisesRegex(ValueError,'EXACT_PARAMETERS_REQUIRED'):
            self.book.propose('engineering-x',self.plan(),q,snap,evidence={'purpose':'engineering','synthetic':False,'source_sha256':'fixture'},purpose='engineering')

    def test_explicit_approval_alias_keeps_engineering_fill_classification(self):
        state=self.book.state();state['trades']={'engineering-fixture':{'id':'engineering-fixture','purpose':'engineering','phase':'FLAT_VERIFIED','plan':{'symbol':'DEMO'}}}
        self.book.db.execute('UPDATE live_state SET payload=? WHERE id=1',(json.dumps(state),))
        self.book.db.executescript('CREATE TABLE broker_current(id INTEGER PRIMARY KEY,payload TEXT); CREATE TABLE broker_executions(id TEXT PRIMARY KEY,payload TEXT)')
        orders={}
        for ident,side,price in [('buy','buy','10'),('sell','sell','10.5')]:
            payload={'ref_id':ident,'plan':{'symbol':'DEMO'},'parameters':{'symbol':'DEMO'}}
            self.book.db.execute('INSERT INTO live_actions VALUES(?,?,?,?,?,?,?,NULL)',(ident,'engineering-fixture','entry' if side=='buy' else 'exit','OBSERVED',json.dumps(payload),self.now.isoformat(),self.now.isoformat()))
            self.book.db.execute('INSERT INTO external_broker_refs VALUES(?,?,?)',('approval-'+ident,ident,'native-'+ident))
            orders[ident]={'ref_id':'approval-'+ident}
            fill={'id':ident,'order_id':ident,'symbol':'DEMO','side':side,'quantity':'1','price':price,'fees':'0','executed_at':self.now.isoformat()}
            self.book.db.execute('INSERT INTO broker_executions VALUES(?,?)',(ident,json.dumps(fill)))
        self.book.db.execute('INSERT INTO broker_current VALUES(1,?)',(json.dumps({'orders':orders}),));self.book.db.commit()
        result=self.data.snapshot()
        self.assertTrue(all(f['purpose']=='engineering' for f in result['fills']))
        self.assertEqual(result['performance']['engineering']['realized_pnl'],'0.5')
        self.assertEqual(result['performance']['strategy']['realized_pnl'],'0')

    def test_normal_flat_close_is_historical_and_never_live_healthy(self):
        content=json.loads(self.status.read_text())
        content.update(db=str(self.db.resolve()),running=False,reason='AUTHORIZED_SESSION_CLOSED',final_flat_verified=True,attention_required=False)
        self.book.db.execute('CREATE TABLE broker_current(id INTEGER PRIMARY KEY,payload TEXT)')
        broker={'account_mask':'••••0000','observed_at':content['observed_at'],'pending_approvals':0,'positions':{},'orders':{}}
        self.book.db.execute('INSERT INTO broker_current VALUES(1,?)',(json.dumps(broker),));self.book.db.commit()
        self.status.write_text(json.dumps(content))
        with patch('martin_agentic.dashboard.process_matches',return_value=False):result=self.data.snapshot()
        self.assertTrue(result['runtime']['session_closed_safe'])
        self.assertFalse(result['runtime']['healthy_now'])
        content['attention_required']=True;self.status.write_text(json.dumps(content))
        with patch('martin_agentic.dashboard.process_matches',return_value=False):result=self.data.snapshot()
        self.assertFalse(result['runtime']['session_closed_safe'])

    def test_foreign_or_unbound_closed_status_cannot_claim_safe_history(self):
        content=json.loads(self.status.read_text())
        content.update(db=str(self.root/'other.sqlite'),running=False,reason='AUTHORIZED_SESSION_CLOSED',final_flat_verified=True,attention_required=False)
        self.status.write_text(json.dumps(content))
        with patch('martin_agentic.dashboard.process_matches',return_value=False):result=self.data.snapshot()
        self.assertFalse(result['runtime']['session_closed_safe'])
        content['db']=str(self.db.resolve());self.status.write_text(json.dumps(content))
        with patch('martin_agentic.dashboard.process_matches',return_value=False):result=self.data.snapshot()
        self.assertFalse(result['runtime']['session_closed_safe'])

    def test_safe_close_history_remains_bound_to_audited_snapshot_after_new_read(self):
        import hashlib
        content=json.loads(self.status.read_text())
        content.update(db=str(self.db.resolve()),running=False,reason='AUTHORIZED_SESSION_CLOSED',
                       final_flat_verified=True,attention_required=False,observed_at='2026-10-16T13:00:00Z')
        self.status.write_text(json.dumps(content))
        old={'account_mask':'••••0000','observed_at':content['observed_at'],'pending_approvals':0,'positions':{},'orders':{}}
        raw=json.dumps(old,sort_keys=True);source_hash=hashlib.sha256(raw.encode()).hexdigest()
        self.book.db.executescript('CREATE TABLE broker_current(id INTEGER PRIMARY KEY,payload TEXT); CREATE TABLE broker_observations(digest TEXT PRIMARY KEY,payload TEXT)')
        self.book.db.execute('INSERT INTO broker_observations VALUES(?,?)',(source_hash,raw))
        newer={**old,'observed_at':self.now.isoformat()}
        self.book.db.execute('INSERT INTO broker_current VALUES(1,?)',(json.dumps(newer),));self.book.db.commit()
        event={'event_id':'account-'+source_hash,'occurred_at':old['observed_at'],'event_type':'ACCOUNT_OBSERVED',
               'actor':'broker','synthetic':False,'account':'••••0000',
               'details':{'source_sha256':source_hash,'reconciliation_issues':[]}}
        self.book.audit.append(event)
        with patch('martin_agentic.dashboard.process_matches',return_value=False):result=self.data.snapshot()
        self.assertTrue(result['runtime']['session_closed_safe']);self.assertFalse(result['runtime']['healthy_now'])
        self.book.db.execute('UPDATE broker_observations SET payload=? WHERE digest=?',(raw+' ',source_hash));self.book.db.commit()
        with patch('martin_agentic.dashboard.process_matches',return_value=False):result=self.data.snapshot()
        self.assertFalse(result['runtime']['session_closed_safe'])
