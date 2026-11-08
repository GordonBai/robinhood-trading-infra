"""Real legacy host path parity over isolated synthetic databases, no broker calls."""
from copy import deepcopy
from dataclasses import replace
from datetime import datetime
from decimal import Decimal
import importlib.util
import json
from pathlib import Path
import tempfile
from unittest import TestCase
from unittest.mock import Mock, patch

from entry_authority_fixture import TestExecutionBook as ExecutionBook
from martin_luk.live_signals import LiveSignals
from shadow_fixture import events, universe
from trading_contracts.strategy import StrategyIdentity, StrategyInput
from trading_contracts.values import digest
from trading_infra.sizing import size_entry
from trading_strategies.martin.signals import MartinSignalPlugin
from trading_infra.market_calendar import ExchangeCalendarView


class SignalHostIsolationTests(TestCase):
    def books(self, directory, *, cost='0.05', notional='100', risk='2.50'):
        spec=importlib.util.spec_from_file_location('frozen_live_signals','tests/fixtures/legacy_live_signals_v1.py')
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        self.now=datetime.fromisoformat('2026-10-15T09:25:00-04:00')
        policy=json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        policy.update(session_date='2026-10-15',max_position_notional_usd=notional,max_planned_loss_per_trade_usd=risk)
        old=ExecutionBook(Path(directory)/'old.sqlite',policy,simulation=True,clock=lambda:self.now)
        new=ExecutionBook(Path(directory)/'new.sqlite',policy,simulation=True,clock=lambda:self.now)
        self.addCleanup(old.close);self.addCleanup(new.close)
        return module.LiveSignals(old,cost_buffer_per_share=cost),LiveSignals(new,cost_buffer_per_share=cost)

    def event(self, event):
        return {**deepcopy(event),'synthetic_input':True,'source_sha256':'fixture-'+event['id']}

    def test_full_legacy_trace_checkpoint_signals_and_sizing_match(self):
        for cost,notional,risk in [('0','100','2.50'),('0.05','100','2.50'),
                                   ('0.10','80','1.00'),('0.05','1','0.01')]:
            with self.subTest(cost=cost,notional=notional,risk=risk),tempfile.TemporaryDirectory() as directory:
                old,new=self.books(directory,cost=cost,notional=notional,risk=risk)
                for raw in events()[1:]:
                    event=self.event(raw);self.now=datetime.fromisoformat(event['at'])
                    expected=old.consume(event);actual=new.consume(event)
                    self.assertEqual(actual,expected);self.assertEqual(new.state(),old.state())
                    if actual:
                        old.complete(actual['id'],result={'accepted':True})
                        new.complete(actual['id'],result={'accepted':True})
                        self.assertEqual(new.state(),old.state())

    def test_duplicate_inbox_skips_plugin_and_conflict_is_atomic(self):
        with tempfile.TemporaryDirectory() as directory:
            _,new=self.books(directory);event=self.event(events()[1]);self.now=datetime.fromisoformat(event['at'])
            new.consume(event);before=new.state()
            with patch.object(new.plugin,'decide',side_effect=AssertionError('duplicate reached plugin')):
                self.assertIsNone(new.consume(event))
            self.assertEqual(new.state(),before)
            changed=deepcopy(event);changed['data']['universe_id']='changed'
            with self.assertRaisesRegex(ValueError,'STRATEGY_EVENT_ID_CONFLICT'):new.consume(changed)
            self.assertEqual(new.state(),before)

    def test_plugin_failure_or_host_outbox_injection_rolls_back_checkpoint_and_inbox(self):
        with tempfile.TemporaryDirectory() as directory:
            _,new=self.books(directory);event=self.event(events()[1]);self.now=datetime.fromisoformat(event['at'])
            before=new.state()
            with patch.object(new.plugin,'decide',side_effect=ValueError('fixture crash')):
                with self.assertRaisesRegex(ValueError,'fixture crash'):new.consume(event)
            self.assertEqual(new.state(),before)
            self.assertEqual(new.book.db.execute('SELECT count(*) FROM live_inputs').fetchone()[0],0)
            original=new.plugin.decide
            def inject(request):
                self.assertFalse(hasattr(request,'book'));self.assertEqual(dict(request.budget),{})
                self.assertNotIn('pending_signals',request.checkpoint)
                return replace(original(request),next_checkpoint={'pending_signals':{'fake':'injected'}})
            with patch.object(new.plugin,'decide',side_effect=inject):
                with self.assertRaisesRegex(ValueError,'CANNOT_CONTAIN_HOST_OUTBOX'):new.consume(event)
            self.assertEqual(new.state(),before)
            self.assertEqual(new.book.db.execute('SELECT count(*) FROM live_inputs').fetchone()[0],0)

    def test_actual_supported_thirty_symbol_universe_crosses_contract_boundary(self):
        data=universe();keys=list(data['histories'])
        for index in range(20):
            original=keys[index%len(keys)];symbol='X'+str(index)
            data['histories'][symbol]=deepcopy(data['histories'][original])
            for bar in data['histories'][symbol]:bar['symbol']=symbol
            data['indicators'][symbol]=deepcopy(data['indicators'][original])
        at=datetime.fromisoformat('2026-10-15T09:25:00-04:00')
        event=dict(id='thirty',kind='universe',at=at.isoformat(),data=data,synthetic_input=True,source_sha256='fixture')
        plugin=MartinSignalPlugin(calendar_view=ExchangeCalendarView())
        request=StrategyInput(StrategyIdentity('thirty-fixture','martin',plugin.VERSION),at,'thirty',
            'snapshot-thirty',digest(event),'fixture-config',True,market={'event':event})
        result=plugin.decide(request)
        self.assertTrue(result.next_checkpoint['universe_complete'])
        self.assertEqual(result.diagnostics['screening']['universe_id'],data['universe_id'])

    def test_infra_sizing_obeys_both_hard_caps_and_advisory_ceiling(self):
        args=dict(entry_ceiling='40',stop='39',cost_buffer_per_share='0.05',
                  max_position_notional='100',max_planned_loss='2.50')
        self.assertEqual(size_entry(**args),2)
        self.assertEqual(size_entry(**args,quantity_limit=1),1)
        self.assertEqual(size_entry(**{**args,'max_planned_loss':'0'}),0)
        for bad in (True,1.5,Decimal('1.5')):
            with self.assertRaises(ValueError):size_entry(**args,quantity_limit=bad)
        with self.assertRaises(ValueError):size_entry(**{**args,'stop':'40'})

    def test_unexpected_plugin_fault_keeps_actual_supervisor_management_running(self):
        import test_execution
        from martin_luk.live_session import LiveSupervisor
        fixture=test_execution.ExecutionTests('test_empty_queue_has_zero_agent_calls')
        fixture.setUp();self.addCleanup(fixture.tearDown)
        buy=fixture.entry_filled()
        dispatcher=Mock(return_value={'agent_called':False,'management_tick':True,'broker_writes':0})
        supervisor=LiveSupervisor(fixture.book,dispatcher=dispatcher)
        event=dict(id='ordinary-plugin-crash',kind='quote',at=fixture.now.isoformat(),
            data=fixture.quote,source_sha256='isolated-fixture',synthetic_input=True)
        with patch.object(supervisor.signals.plugin,'decide',
                side_effect=RuntimeError('private-error-message-'*10000)), \
                patch.object(fixture.book,'reconcile',wraps=fixture.book.reconcile) as reconcile:
            result=supervisor.step(fixture.snap([buy],qty='2'),[event])
        reconcile.assert_called_once();dispatcher.assert_called_once_with(allow_entries=False)
        self.assertTrue(result['new_entries_blocked'])
        self.assertTrue(result['dispatch']['management_tick'])
        self.assertEqual(result['dispatch']['broker_writes'],0)
        self.assertEqual(result['errors'][0]['reason'],'STRATEGY_PLUGIN_EVALUATION_FAILED')
        self.assertNotIn('private-error-message',str(result))
        self.assertEqual(fixture.book.db.execute('SELECT count(*) FROM live_inputs').fetchone()[0],0)
        self.assertTrue(any(a['kind']=='protect' and a['state']=='READY' for a in fixture.book.actions()))

    def test_runner_preserves_integrity_classification_and_process_interrupts(self):
        from trading_infra.strategy_host import evaluate_strategy
        at=datetime.fromisoformat('2026-10-15T09:25:00-04:00')
        request=StrategyInput(StrategyIdentity('fixture','martin','v1'),at,'cursor',
            'snapshot',digest({}),'config',True)
        for error in (ValueError('CONSUMED_BAR_REVISED'), KeyError('field'), TypeError('type'), KeyboardInterrupt()):
            plugin=Mock();plugin.decide.side_effect=error
            with self.assertRaises(type(error)) as raised:evaluate_strategy(plugin,request)
            self.assertIs(raised.exception,error)
