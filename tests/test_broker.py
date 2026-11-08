from contextlib import nullcontext
from copy import deepcopy
from datetime import datetime
import json
from pathlib import Path
import sqlite3
import tempfile
from unittest import TestCase

from martin_agentic.broker import BrokerJournal, collect_broker, fetch_pages, normalize_order, observe_broker
from martin_agentic.transport import CodexBrokerReadTransport, TransportError


AT='2026-10-16T14:00:00+00:00'
LATER='2026-10-16T14:00:01+00:00'
ACCOUNT='SIM-0000'


def position(q='1',held='0'):
    return {'symbol':'DEMO','quantity':q,'shares_available_for_sells':str(int(q)-int(held)),
            'shares_held_for_sells':held,'average_buy_price':'40'}


def order(oid='buy-1',side='buy',qty='1',filled='1',state='filled',stop=None):
    return {'id':oid,'symbol':'DEMO','side':side,'quantity':qty,'cumulative_quantity':filled,
            'type':'market','trigger':'stop' if stop else 'immediate','stop_price':stop,
            'created_at':'2026-10-16T13:59:00Z','state':state,'market_hours':'regular_hours','time_in_force':'gfd',
            'executions':[] if filled=='0' else [{'id':oid+'-exec','price':'40','quantity':filled,'fees':'0','timestamp':AT}]}


def approval(identity='approval-1',state='APPROVAL_STATE_PENDING',**changes):
    return {'approval_id':identity,'account_number':ACCOUNT,'asset_class':'ASSET_CLASS_EQUITY',
        'state':state,'is_edited':False,**changes}


class FakeBroker:
    def __init__(self):
        self.calls=[]
        self.positions=[]
        self.orders=[]
        self.groups=[]
        self.options=[]
        self.crypto=[]
        self.account=ACCOUNT
        self.positions_calls=0
        self.after=None
        self.cash='500'
        self.approvals=[]

    def bind(self,account_number,rhs_account_number=None):
        if account_number != ACCOUNT:raise ValueError('Wrong scope')

    def __call__(self,tool,args):
        self.calls.append((tool,deepcopy(args)))
        if tool=='get_accounts':
            data={'accounts':[{'account_number':self.account,'rhs_account_number':'SIM-RHS','agentic_allowed':True,'deactivated':False,'permanently_deactivated':False,'state':'active'}]}
        elif tool=='get_equity_positions':
            self.positions_calls+=1
            data={'positions':self.after if self.positions_calls==2 and self.after is not None else self.positions}
        elif tool=='get_equity_orders':
            data={'orders':[o for o in self.orders if not args.get('order_id') or o['id']==args['order_id']]}
        elif tool=='get_advanced_orders':data={'advanced_orders':self.groups}
        elif tool=='get_option_orders':data={'orders':self.options}
        elif tool=='get_crypto_orders':data={'results':self.crypto}
        elif tool=='get_trade_approval_setting':data={'setting':{'account_number':ACCOUNT,'human_must_approve_trades':True}}
        elif tool=='get_trade_approvals':
            ids=args.get('approval_ids')
            data={'approvals':[a for a in self.approvals if a['approval_id'] in ids] if ids is not None
                else [a for a in self.approvals if a['state']=='APPROVAL_STATE_PENDING']}
        elif tool=='get_portfolio':
            data={k:'0' for k in ('options_value','crypto_value','futures_value','event_contracts_value','fixed_income_value','mutual_funds_value','pending_deposits')}
            data.update(currency='USD',cash=self.cash,total_value='500',buying_power={
                'display_currency':'USD','buying_power':'500','unleveraged_buying_power':'500'})
        else:raise AssertionError('Unexpected tool')
        return {'data':deepcopy(data)}


def snapshot(fake=None,at=AT):
    return collect_broker(fake or FakeBroker(),account_suffix='0000',clock=lambda:datetime.fromisoformat(at))


class BrokerTests(TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.path=Path(self.tmp.name)/'broker.sqlite'
        self.journal=BrokerJournal(self.path)

    def tearDown(self):
        self.journal.close()
        self.tmp.cleanup()

    def test_collection_uses_only_selected_account_and_masks_identity(self):
        fake=FakeBroker()
        result=snapshot(fake)
        self.assertEqual(result['account_mask'],'••••0000')
        self.assertNotIn(ACCOUNT,json.dumps(result))
        self.assertTrue(all(not name.startswith(('place_','cancel_','review_')) for name,args in fake.calls))

    def test_wrong_account_or_disabled_target_rejected(self):
        fake=FakeBroker();fake.account='OTHER-1234'
        with self.assertRaises(ValueError):snapshot(fake)

    def test_restricted_closed_or_unknown_account_state_is_rejected_before_account_reads(self):
        for changes in ({'state':'restricted'},{'state':None},{'permanently_deactivated':True},
                        {'permanently_deactivated':None}):
            with self.subTest(changes=changes):
                fake=FakeBroker()
                def transport(tool,args):
                    response=fake(tool,args)
                    if tool=='get_accounts':response['data']['accounts'][0].update(changes)
                    return response
                with self.assertRaisesRegex(ValueError,'TARGET_ACCOUNT_NOT_ACTIVE'):
                    collect_broker(transport,account_suffix='0000',clock=lambda:datetime.fromisoformat(AT))
                self.assertEqual([name for name,_ in fake.calls],['get_accounts'])

    def test_cursor_is_opaque_and_cycles_rejected(self):
        seen=[]
        def transport(tool,args):
            seen.append(args)
            return {'data':{'orders':[],'next':'opaque://cursor'}}
        with self.assertRaisesRegex(ValueError,'PAGINATION_INVALID'):
            fetch_pages(transport,'get_equity_orders',{'account_number':ACCOUNT},'orders')
        self.assertEqual(seen[1]['cursor'],'opaque://cursor')

    def test_unstable_positions_rejected(self):
        fake=FakeBroker();fake.after=[position()]
        with self.assertRaisesRegex(ValueError,'UNSTABLE_ACCOUNT_SNAPSHOT'):snapshot(fake)

    def test_concurrent_old_snapshot_is_retryable_without_regressing_journal(self):
        self.journal.ingest(snapshot(at=LATER))
        with self.assertRaisesRegex(TransportError,'BROKER_SNAPSHOT_SUPERSEDED'):
            self.journal.ingest(snapshot(at=AT))
        self.assertEqual(self.journal.summary(as_of=LATER)['last_observed_at'],LATER)
        changed=snapshot(at=LATER);changed['account_key']='different-account'
        with self.assertRaisesRegex(ValueError,'account binding changed'):
            self.journal.ingest(changed)
        self.assertTrue(self.journal.audit.verify()['integrity_valid'])

    def test_partial_fill_normalizes_decimal_quantity_and_timestamp(self):
        o=normalize_order(order(qty='2',filled='1',state='partially_filled'),AT)
        self.assertEqual(o['remaining'],'1')
        self.assertEqual(o['executions'][0]['executed_at'],AT)

    def test_cumulative_without_execution_detail_is_not_fabricated(self):
        raw=order();raw['executions']=None
        with self.assertRaisesRegex(ValueError,'DETAILS_MISSING'):normalize_order(raw,AT)
        raw['executions']=[]
        with self.assertRaisesRegex(ValueError,'CUMULATIVE_EXECUTION_MISMATCH'):normalize_order(raw,AT)

    def test_future_execution_rejected(self):
        raw=order();raw['executions'][0]['timestamp']=LATER
        with self.assertRaisesRegex(ValueError,'EXECUTION_INVALID'):normalize_order(raw,AT)

    def test_duplicate_execution_identity_within_order_rejected(self):
        raw=order(qty='2',filled='2');raw['executions']*=2
        with self.assertRaisesRegex(ValueError,'EXECUTION_INVALID'):normalize_order(raw,AT)

    def test_new_execution_and_position_delta_reconcile_after_restart(self):
        self.journal.ingest(snapshot())
        self.journal.close();self.journal=BrokerJournal(self.path)
        fake=FakeBroker();fake.positions=[position()];fake.orders=[order()]
        fake.cash='460'
        result=self.journal.ingest(snapshot(fake,LATER))
        self.assertEqual(result['executions_seen'],1)
        self.assertEqual(result['reconciliation_issues'],[])
        self.assertIn('UNPROTECTED_POSITION:DEMO',result['blockers'])
        repeated=self.journal.ingest(snapshot(fake,LATER))
        self.assertEqual(repeated['executions_seen'],1)
        self.assertEqual(repeated['new_executions_seen'],0)

    def test_initial_history_is_a_baseline_not_replayed_into_holdings(self):
        fake=FakeBroker();fake.positions=[position()];fake.orders=[order()]
        result=self.journal.ingest(snapshot(fake))
        self.assertEqual(result['reconciliation_issues'],[])
        self.assertEqual(result['positions']['DEMO']['quantity'],'1')

    def test_unexplained_position_change_latches_issue(self):
        self.journal.ingest(snapshot())
        fake=FakeBroker();fake.positions=[position()]
        result=self.journal.ingest(snapshot(fake,LATER))
        self.assertIn('POSITION_EXECUTION_DELTA_MISMATCH',result['reconciliation_issues'])
        result=self.journal.ingest(snapshot(fake,LATER))
        self.assertIn('POSITION_EXECUTION_DELTA_MISMATCH',result['reconciliation_issues'])

    def test_execution_correction_does_not_overwrite_original(self):
        fake=FakeBroker();fake.positions=[position()];fake.orders=[order()]
        self.journal.ingest(snapshot(fake))
        fake.orders[0]['executions'][0]['price']='41'
        result=self.journal.ingest(snapshot(fake,LATER))
        self.assertIn('EXECUTION_ID_CONFLICT',result['reconciliation_issues'])
        stored=self.journal.db.execute('SELECT payload FROM broker_executions').fetchone()[0]
        self.assertEqual(json.loads(stored)['price'],'40')

    def test_confirmed_stop_counts_once_not_take_profit_leg(self):
        fake=FakeBroker();fake.positions=[position(held='1')]
        stop=order('stop','sell',filled='0',state='confirmed',stop='39')
        take=order('take','sell',filled='0',state='confirmed');take['type']='limit'
        fake.orders=[stop,take]
        fake.groups=[{'id':'oco','account_number':ACCOUNT,'state':'confirmed','contingency_type':'oco',
            'advanced_order_links':[{'order_id':'stop','order':stop},{'order_id':'take','order':take}]}]
        result=self.journal.ingest(snapshot(fake))
        self.assertEqual(result['equity_orders_seen'],2)
        self.assertEqual(result['positions']['DEMO']['observed_stop_coverage'],'1')
        self.assertFalse(result['execution_ready'])

    def test_missing_hydrated_leg_is_resolved_via_order_id(self):
        fake=FakeBroker();fake.orders=[order('stop','sell',filled='0',state='confirmed',stop='39')]
        fake.groups=[{'id':'oco','account_number':ACCOUNT,'state':'confirmed','contingency_type':'oco',
            'advanced_order_links':[{'order_id':'stop','order':None}]}]
        snapshot(fake)
        self.assertTrue(any(args.get('order_id')=='stop' for tool,args in fake.calls))

    def test_partial_stop_fill_is_not_counted_as_waiting_stop(self):
        fake=FakeBroker();fake.positions=[position()]
        fake.orders=[order('stop','sell',qty='2',filled='1',state='partially_filled',stop='39')]
        result=self.journal.ingest(snapshot(fake))
        self.assertEqual(result['positions']['DEMO']['observed_stop_coverage'],'0')

    def test_residual_exit_on_flat_position_is_blocked(self):
        fake=FakeBroker();fake.orders=[order('stop','sell',filled='0',state='confirmed',stop='39')]
        result=self.journal.ingest(snapshot(fake))
        self.assertIn('EXIT_ORDER_POSITION_MISMATCH',result['blockers'])

    def test_other_asset_open_orders_block_equity_trial(self):
        fake=FakeBroker();fake.options=[{'state':'queued'}]
        result=self.journal.ingest(snapshot(fake))
        self.assertIn('OTHER_ASSET_EXPOSURE_OR_ORDERS',result['blockers'])

    def test_failed_observation_and_stale_clock_do_not_leave_healthy_status(self):
        self.journal.ingest(snapshot())
        self.journal.failure(at=LATER,reason='test_disconnect')
        self.assertFalse(self.journal.summary(as_of=LATER)['read_healthy'])
        self.journal.ingest(snapshot(at=LATER))
        self.assertFalse(self.journal.summary(as_of='2026-10-16T14:01:00Z')['read_healthy'])

    def test_rollback_after_execution_insert(self):
        self.journal.ingest(snapshot())
        self.journal.db.execute("CREATE TRIGGER fail_current BEFORE UPDATE ON broker_current BEGIN SELECT RAISE(FAIL,'injected'); END")
        self.journal.db.commit()
        fake=FakeBroker();fake.positions=[position()];fake.orders=[order()]
        with self.assertRaises(sqlite3.IntegrityError):self.journal.ingest(snapshot(fake,LATER))
        self.assertEqual(self.journal.db.execute('SELECT COUNT(*) FROM broker_executions').fetchone()[0],0)

    def test_market_transport_does_not_acquire_broker_or_write_access(self):
        transport=CodexBrokerReadTransport.__new__(CodexBrokerReadTransport)
        transport.account_number=ACCOUNT;transport.rhs_account_number='SIM-RHS'
        transport._read_call=lambda tool,args: {'ok':True}
        with self.assertRaises(ValueError):transport('place_equity_order',{'account_number':ACCOUNT})
        with self.assertRaises(ValueError):transport('get_portfolio',{'account_number':'wrong'})
        self.assertTrue(transport('get_portfolio',{'account_number':ACCOUNT})['ok'])

    def test_bounded_observer_closes_as_non_running(self):
        fake=FakeBroker()
        result=observe_broker(self.path,account_suffix='0000',clock=lambda:datetime.fromisoformat(AT),
            transport_factory=lambda:nullcontext(fake))
        self.assertEqual(result['cycles_completed'],1)
        self.assertFalse(result['current']['read_healthy'])
        self.assertFalse(result['continuous_monitor_running'])

    def test_pending_approval_blocks_new_entry(self):
        fake=FakeBroker();fake.approvals=[approval('pending')]
        result=self.journal.ingest(snapshot(fake))
        self.assertIn('PENDING_APPROVAL_REQUIRES_RECONCILIATION',result['blockers'])

    def test_exact_approval_reads_return_terminal_evidence_and_missing_ids(self):
        fake=FakeBroker();fake.approvals=[approval('declined','APPROVAL_STATE_DECLINED'),approval('pending')]
        result=collect_broker(fake,account_suffix='0000',clock=lambda:datetime.fromisoformat(AT),
            approval_ids=['declined','pending','missing'])
        self.assertEqual(result['pending_approvals'],1)
        self.assertEqual(result['approval_states']['declined']['state'],'APPROVAL_STATE_DECLINED')
        self.assertEqual(result['missing_approval_ids'],['missing'])
        self.assertNotIn(ACCOUNT,json.dumps(result))
        self.assertEqual([a for t,a in fake.calls if t=='get_trade_approvals'][-1]['approval_ids'],['declined','pending','missing'])

    def test_exact_approval_reads_batch_at_one_hundred(self):
        fake=FakeBroker();ids=[f'a-{i}' for i in range(205)]
        result=collect_broker(fake,account_suffix='0000',clock=lambda:datetime.fromisoformat(AT),approval_ids=ids)
        self.assertEqual(result['missing_approval_ids'],ids)
        self.assertEqual([len(a['approval_ids']) for t,a in fake.calls if t=='get_trade_approvals' and 'approval_ids' in a],[100,100,5])

    def test_approval_wrong_account_or_duplicate_or_unsolicited_identity_fails(self):
        for mode in ('account','duplicate','unsolicited'):
            fake=FakeBroker();original=fake.__call__
            def transport(tool,args):
                if tool=='get_trade_approvals' and 'approval_ids' in args:
                    rows=[approval('exact',**({'account_number':'FOREIGN'} if mode=='account' else {}))]
                    if mode=='duplicate':rows*=2
                    if mode=='unsolicited':rows=[approval('other')]
                    return {'data':{'approvals':rows}}
                return original(tool,args)
            transport.bind=fake.bind
            with self.subTest(mode=mode),self.assertRaisesRegex(ValueError,'BROKER_.*APPROVAL'):
                collect_broker(transport,account_suffix='0000',clock=lambda:datetime.fromisoformat(AT),approval_ids=['exact'])

    def test_unknown_or_edited_approval_evidence_is_preserved_without_inferred_terminal(self):
        fake=FakeBroker();fake.approvals=[approval('unknown','NEW_UNRECOGNIZED_STATE'),approval('edited','APPROVAL_STATE_DECLINED',is_edited=True)]
        result=collect_broker(fake,account_suffix='0000',clock=lambda:datetime.fromisoformat(AT),approval_ids=['unknown','edited'])
        self.assertEqual(result['approval_states']['unknown']['state'],'NEW_UNRECOGNIZED_STATE')
        self.assertTrue(result['approval_states']['edited']['is_edited'])

    def test_unexplained_deposit_cannot_hide_trial_loss(self):
        self.journal.ingest(snapshot())
        fake=FakeBroker();fake.cash='510'
        result=self.journal.ingest(snapshot(fake,LATER))
        self.assertIn('UNEXPLAINED_CASH_FLOW',result['reconciliation_issues'])

    def test_missing_previously_open_order_is_not_assumed_cancelled(self):
        fake=FakeBroker();fake.orders=[order(qty='1',filled='0',state='queued')]
        self.journal.ingest(snapshot(fake))
        result=self.journal.ingest(snapshot(at=LATER))
        self.assertIn('OPEN_ORDER_DISAPPEARED',result['reconciliation_issues'])

    def test_execution_numeric_format_change_does_not_create_conflict(self):
        fake=FakeBroker();fake.positions=[position()];fake.orders=[order()]
        self.journal.ingest(snapshot(fake))
        fake.orders[0]['executions'][0].update(quantity='1.00',price='40.0000',fees='0.00')
        result=self.journal.ingest(snapshot(fake,LATER))
        self.assertNotIn('EXECUTION_ID_CONFLICT',result['blockers'])
        self.assertEqual(result['executions_seen'],1)
