"""Default owner-loop current-account facts diagnostics (053)."""
import hashlib
import io
import json
import queue
import sys
from contextlib import redirect_stdout
import tempfile
import time
from copy import deepcopy
from datetime import timedelta
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from martin_agentic.account_facts_worker import AccountFactsWorker
from martin_agentic.audit import digest
from martin_agentic.broker import BrokerJournal
from martin_agentic.capture_publisher import CapturePublisher
from martin_agentic.current_context import (
    CurrentContextFactory, CurrentContextRegistry, MissingCurrentContext,
    ensure_default_preflight)
from martin_agentic.current_evidence import compile_current_evidence
from martin_agentic.engineering_preflight import CurrentEngineeringPreflight
from martin_agentic.execution import ExecutionBook
from martin_luk.live_session import (
    LiveSupervisor, run_live_session, wait_for_result_or_tick)
from martin_agentic.qualification import source_fingerprint
from martin_agentic.simulation import SimulationSources
from test_baseline_evidence import (
    NOW_LATER, OBSERVED, BundleFixture, page_data)
from test_broker import FakeBroker, snapshot
from test_broker_bundle import FakeReadTransport
from test_capture_publisher import (
    ACCOUNT_KEY as CAPTURE_ACCOUNT_KEY, T as CAPTURE_T, CashTransport)
from test_current_evidence import PublicFixture
from test_engineering_preflight import POLICY, facts_assessment
from test_simulation_isolation import (
    NOW as SIM_NOW, SYMBOLS as SIM_SYMBOLS, policy as simulation_policy)
from test_supervised_host_default_loop import (
    SimulationBroker, SimulationMarket, SimulationReviewTransport,
    SimulationWake)


class SlowFactsWorker:
    """Test double for a suspended read-only facts child; never compiles owner."""

    def __init__(self):
        self.inflight=False
        self.starts=[]
        self.cancelled=[]
        self.mode='hold'
        self.response=None
        self.state='idle'
        self.epoch_id=None

    def start(self, request):
        if self.inflight:
            raise ValueError('ALREADY_IN_FLIGHT')
        self.inflight=True
        self.starts.append(request)
        self.epoch_id=request['epoch_id']
        self.mode='hold'
        self.state='running'
        return True

    def poll(self):
        if not self.inflight:
            return None
        if self.mode=='timeout':
            self.inflight=False
            self.state='timeout'
            return ('timeout', {'error':'ACCOUNT_FACTS_WORKER_TIMEOUT',
                                'epoch_id':self.epoch_id})
        if self.mode=='release':
            self.inflight=False
            self.state='completed'
            return ('ok', self.response)
        return None

    def cancel(self, reason=''):
        self.cancelled.append(reason)
        self.inflight=False
        self.state='cancelled'
        return True

    def close(self):
        self.cancelled.append('CLOSED')
        self.inflight=False
        self.state='closed'
        return self.status()

    def status(self):
        return {'running':self.inflight,'inflight':self.inflight,
                'state':self.state,'error':None,'epoch_id':self.epoch_id,
                'age_seconds':0.0,'timeout_seconds':2.0,
                'deadline_remaining_seconds':None,'cleanup_pending':False,
                'read_only_child':True,'broker_writes':0,
                'book_mutations':0}


class ScriptedFactsWorker:
    """Deterministic candidate response for status-mapping tests."""

    def __init__(self, *, ready=True, epoch_override=None):
        self.ready=ready
        self.epoch_override=epoch_override
        self.request=None
        self.inflight=False
        self.state='idle'

    def start(self, request):
        if self.inflight:
            raise ValueError('ALREADY_IN_FLIGHT')
        self.request=request
        self.inflight=True
        self.state='running'
        return True

    def poll(self):
        if not self.inflight:
            return None
        self.inflight=False
        self.state='completed'
        return ('ok', _facts_response(
            self.request, ready=self.ready,
            epoch_override=self.epoch_override))

    def cancel(self, reason=''):
        self.inflight=False
        self.state='cancelled'
        return True

    def close(self):
        self.inflight=False
        self.state='closed'
        return self.status()

    def status(self):
        return {'running':self.inflight,'inflight':self.inflight,
                'state':self.state,'error':None,
                'epoch_id':self.request.get('epoch_id') if self.request else None,
                'age_seconds':0.0,'timeout_seconds':2.0,
                'deadline_remaining_seconds':None,'cleanup_pending':False,
                'read_only_child':True,'broker_writes':0,
                'book_mutations':0}


def _facts_response(request, *, ready=True, epoch_override=None):
    epoch=request['epoch']
    clock=request['clock']
    assessment=facts_assessment(
        account_key=request['account_key'],
        policy_sha256=request['policy_sha256'],
        purpose=request['purpose'],
        observation_valid=True,
        facts_qualified=True,
        account_entry_readiness=ready,
        readiness_reasons=([] if ready else
                           ['CURRENT_POSITION_NOT_FLAT:equity']),
        observed_at=clock,
        observation_valid_from=clock,
        observation_valid_until=(__import__('datetime').datetime.fromisoformat(
            clock)+timedelta(seconds=30)).isoformat(),
        assessed_at=clock,
        binding={
            'kind':'current_account_facts_binding_v1',
            'owner_binding_id':epoch['binding_id'],
            'account_key':request['account_key'],
            'account_suffix':request['account_suffix'],
            'policy_sha256':request['policy_sha256'],
            'source_fingerprint':request['source_fingerprint'],
            'capture_generation':epoch['expected_capture_generation'],
            'expected_capture_generation':epoch['expected_capture_generation'],
            'purpose':request['purpose'],
        },
        components={
            'source':{'pin':epoch['source_pin']},
            'journal_current':{'status':'evaluated',
                               'sha256':epoch['journal_current_sha256']},
            'journal_health':{'status':'evaluated',
                              'sha256':epoch['journal_health_sha256']},
        },
        status='facts_ready',
    )
    consumer=({'allowed':False,
               'reason':'ENTRY_AUTHORITY_CURRENT_FACTS_READY_PERMIT_MISSING',
               'facts_ready':True,'permit_missing':True}
              if ready else
              {'allowed':False,
               'reason':'ENTRY_AUTHORITY_CURRENT_FACTS_BLOCKED',
               'facts_ready':False,'permit_missing':False})
    return {'schema_version':1,
            'kind':'current_account_facts_worker_response_v1',
            'epoch_id':epoch_override or request['epoch_id'],
            'assessment':assessment,
            'validation':{'valid':True,'reason':None},
            'consumer':consumer,'read_only':True,
            'broker_writes':0,'book_mutations':0}


class LiveSessionAccountFactsTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.bundle = BundleFixture(Path(self.temp.name) / 'bundle')
        self.public = PublicFixture(Path(self.temp.name) / 'public')
        self.now = [NOW_LATER]
        page = next(p for p in self.bundle.pages
                    if p['tool'] == 'get_trade_approval_setting')
        data = page_data(self.bundle, page)
        data['setting']['human_must_approve_trades'] = True
        self.bundle.rewrite_raw(page, data)
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            compiled = compile_current_evidence(
                self.bundle.root, self.public.manifest, now=NOW_LATER)
        self.account_key = compiled['account']['account_key_hash']
        self.capture_generation = compiled['capture']['generation']
        self.policy = deepcopy(POLICY)
        self.preflight = self._preflight(self.policy, self.now)
        self.book = self._book(Path(self.temp.name) / 'book.sqlite',
                               self.policy, self.preflight)
        self.supervisor = LiveSupervisor(self.book)
        self.addCleanup(self.supervisor.close)

    def _preflight(self, policy, now):
        return CurrentEngineeringPreflight(
            self.bundle.root, self.public.manifest,
            account_key=self.account_key, account_suffix='0000',
            policy_sha256=digest(policy),
            source_fingerprint_value=source_fingerprint(),
            clock=lambda: now[0],
            expected_capture_generation=self.capture_generation)

    def _book(self, path, policy, preflight):
        book = ExecutionBook(
            path, policy, simulation=True, clock=lambda: self.now[0],
            current_preflight=preflight)
        self.addCleanup(book.close)
        return book

    def snap(self, at=None):
        value = snapshot(FakeBroker(), at or OBSERVED)
        value['synthetic'] = True
        return value

    def _align_bundle_and_journal(self, book=None):
        book = book or self.book
        value = self.snap()
        book.reconcile(value)
        journal = BrokerJournal(book.db_path, allow_synthetic=True)
        try:
            journal.ingest(value)
        finally:
            journal.close()

    def _full_marker(self, book=None, scope='full_account_baseline_v1'):
        book = book or self.book
        book.db.execute(
            'CREATE TABLE IF NOT EXISTS entry_authority_marker('
            'required INTEGER,reason TEXT,baseline_id TEXT,'
            'baseline_scope TEXT,created_at TEXT)')
        book.db.execute(
            'INSERT INTO entry_authority_marker VALUES(1,?,?,?,?)',
            ('BASELINE_APPLIED', 'fixture-full', scope,
             NOW_LATER.isoformat()))
        state = book.state()
        state['account_key'] = self.account_key
        book._save(state)
        book.db.commit()

    def _step(self, supervisor=None, *, allow_entries=False, snap=None):
        supervisor = supervisor or self.supervisor
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            return supervisor.step(
                snap if snap is not None else self.snap(), [],
                allow_entries=allow_entries)

    def _step_until_facts(self, supervisor=None, *, allow_entries=False,
                          snap=None, max_steps=300):
        for _ in range(max_steps):
            result=self._step(supervisor,allow_entries=allow_entries,snap=snap)
            if result['account_facts']['status']!='inflight':
                return result
            time.sleep(.01)
        raise AssertionError('facts worker did not settle')

    def test_default_step_without_signal_or_entry_has_diagnostics(self):
        self._align_bundle_and_journal()
        self._full_marker()
        result = self._step_until_facts()
        facts = result['account_facts']
        self.assertIn(facts['status'], {'facts_blocked', 'observation_blocked'})
        self.assertIs(facts['entry_authorized'], False)
        self.assertIs(facts['production_ready'], False)
        self.assertIs(facts['facts_are_not_entry_authorization'], True)
        self.assertIsNotNone(facts['observed_at'])
        self.assertIsNotNone(facts['observation_valid_until'])
        self.assertEqual(facts['generation'], self.capture_generation)
        self.assertEqual(facts['purpose'], 'strategy')
        self.assertIn('BLOCKED_SOURCE:futures',
                      facts['layers']['facts']['reasons'])
        self.assertIn('BLOCKED_SOURCE:event_contracts',
                      facts['layers']['facts']['reasons'])
        self.assertNotIn('key_hash', facts['account'])
        self.assertEqual(facts['account']['suffix'], '0000')
        self.assertEqual(self.book.actions(), [])
        self.assertEqual(result['dispatch'].get('agent_called'), False)

    def test_same_generation_reuses_budget_and_journal_change_recompiles(self):
        self._align_bundle_and_journal()
        self._full_marker()
        first = self._step_until_facts()['account_facts']
        second = self._step()['account_facts']
        self.assertEqual(first['refresh']['count'], 1)
        self.assertEqual(second['refresh']['count'], 1)
        self.assertIs(second['refresh']['reused'], True)
        self.assertIs(second['refresh']['compiled'], False)
        self.assertEqual(second['observed_at'], first['observed_at'])
        self.assertEqual(second['observation_valid_until'],
                         first['observation_valid_until'])

        row = self.book.db.execute(
            'SELECT payload FROM broker_current WHERE id=1').fetchone()
        payload = json.loads(row[0])
        payload['post_cache_change'] = True
        self.book.db.execute(
            'UPDATE broker_current SET payload=? WHERE id=1',
            (json.dumps(payload),))
        self.book.db.commit()
        third = self._step_until_facts()['account_facts']
        self.assertEqual(third['refresh']['count'], 2)
        self.assertIs(third['refresh']['reused'], False)
        self.assertIn(third['status'],
                      {'observation_blocked', 'facts_blocked'})

    def test_source_pin_change_forces_recompile(self):
        self._align_bundle_and_journal()
        self._full_marker()
        first = self._step_until_facts()['account_facts']
        self.assertEqual(first['refresh']['count'], 1)
        changed_pin = {'status': 'bounded', 'digest': '0' * 64,
                       'count': 355, 'root': str(Path.cwd()),
                       'truncated': False}
        with patch('martin_agentic.live_session.source_pin',
                   return_value=changed_pin):
            changed = self._step_until_facts()['account_facts']
        self.assertEqual(changed['refresh']['count'], 2)
        self.assertIs(changed['refresh']['reused'], False)
        self.assertIs(changed['entry_authorized'], False)

    def test_risk_state_change_forces_recompile(self):
        self._align_bundle_and_journal()
        self._full_marker()
        first = self._step_until_facts()['account_facts']
        self.assertEqual(first['refresh']['count'], 1)
        risk = self.book.risk_guard.state()
        risk['account_facts_probe'] = 'changed-after-assessment'
        self.book.risk_guard.db.execute(
            'UPDATE account_risk SET payload=? WHERE id=1',
            (json.dumps(risk),))
        self.book.risk_guard.db.commit()
        changed = self._step_until_facts()['account_facts']
        self.assertEqual(changed['refresh']['count'], 2)
        self.assertIs(changed['refresh']['reused'], False)
        self.assertIs(changed['entry_authorized'], False)

    def test_owner_wait_services_completed_child_without_strategy(self):
        self._align_bundle_and_journal()
        self._full_marker()
        first=self._step()['account_facts']
        self.assertEqual(first['status'],'inflight')
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            wait_for_result_or_tick(
                None,None,15,account_facts_supervisor=self.supervisor)
        facts=self.supervisor.account_facts_status
        self.assertNotEqual(facts['status'],'inflight')
        self.assertIn(facts['status'],
                      {'facts_blocked','observation_blocked'})
        self.assertIs(facts['entry_authorized'],False)
        self.assertIs(facts['worker']['inflight'],False)
        self.assertEqual(facts['worker']['state'],'completed')
        self.assertIs(facts['fresh_cycle'],False)
        self.assertIs(facts['wait_progress'],True)
        self.assertEqual(facts['refresh']['source'],'owner_wait_progress')
        self.assertEqual(facts['refresh']['consumed_count'],1)
        self.assertIsNotNone(facts['refresh']['last_completed'])
        self.assertEqual(facts['refresh']['last_completed']['status'],
                         facts['status'])
        self.assertIs(facts['refresh']['last_completed']['fresh_cycle'],False)

    def test_owner_wait_services_child_while_task_is_hanging(self):
        self._align_bundle_and_journal()
        self._full_marker()
        first=self._step()['account_facts']
        self.assertEqual(first['status'],'inflight')

        class HangingTask:
            def __init__(self):
                self.thread=object()
                self.mailbox=queue.Queue()

        started=time.monotonic()
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            wait_for_result_or_tick(
                HangingTask(),None,15,
                account_facts_supervisor=self.supervisor)
        elapsed=time.monotonic()-started
        facts=self.supervisor.account_facts_status
        self.assertEqual(facts['status'],'facts_blocked')
        self.assertLess(elapsed,5.0)
        self.assertIs(facts['worker']['inflight'],False)

    def test_worker_uses_read_only_sqlite_without_book_writes(self):
        self._align_bundle_and_journal()
        self._full_marker()
        purpose=self.supervisor._account_facts_purpose()
        context=self.supervisor._account_facts_context()
        now=self.supervisor._account_facts_now()
        epoch=self.supervisor._account_facts_epoch_for(purpose,context)
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            request=self.supervisor._account_facts_worker_request(
                purpose,epoch,now)
        risk_path=self.book.risk_guard.db.execute(
            'PRAGMA database_list').fetchone()[2]
        paths={'execution':Path(self.book.db_path)}
        if isinstance(risk_path,str) and Path(risk_path).is_file():
            paths['risk']=Path(risk_path)
        before={name:hashlib.sha256(path.read_bytes()).hexdigest()
                for name,path in paths.items()}
        worker=AccountFactsWorker(timeout_seconds=2.0)
        self.addCleanup(worker.close)
        self.assertTrue(worker.start(request))
        ready=None
        for _ in range(300):
            ready=worker.poll()
            if ready is not None:
                break
            time.sleep(.01)
        self.assertIsNotNone(ready)
        kind,response=ready
        self.assertEqual(kind,'ok',response)
        self.assertEqual(response['kind'],
                         'current_account_facts_worker_response_v1')
        self.assertIs(response['read_only'],True)
        after={name:hashlib.sha256(path.read_bytes()).hexdigest()
               for name,path in paths.items()}
        self.assertEqual(after,before)

    def test_supervisor_close_stops_real_facts_child_without_entry(self):
        self._align_bundle_and_journal()
        self._full_marker()
        worker = AccountFactsWorker(
            command=[sys.executable, '-c', 'import time; time.sleep(30)'],
            timeout_seconds=30.0)
        self.addCleanup(worker.close)
        supervisor = LiveSupervisor(self.book,
                                    account_facts_worker=worker)
        self.addCleanup(supervisor.close)
        result = self._step(supervisor)
        self.assertEqual(result['account_facts']['status'], 'inflight')
        self.assertTrue(worker.inflight)
        before_actions = self.book.actions()
        shutdown = supervisor.close()
        worker_status = shutdown['account_facts_worker']
        self.assertIs(worker_status['inflight'], False)
        self.assertIs(worker_status['cleanup_pending'], False)
        self.assertIn(worker.status()['state'], {'closed', 'cancelled'})
        self.assertEqual(self.book.actions(), before_actions)
        self.assertIs(result['account_facts']['entry_authorized'], False)

    def test_account_facts_worker_timeout_cleans_child(self):
        worker=AccountFactsWorker(
            command=[sys.executable,'-c','import time; time.sleep(5)'],
            timeout_seconds=.05)
        self.addCleanup(worker.close)
        self.assertTrue(worker.start({'epoch_id':'e'*64}))
        time.sleep(.06)
        kind,value=worker.poll()
        self.assertEqual(kind,'timeout')
        self.assertEqual(value['error'],'ACCOUNT_FACTS_WORKER_TIMEOUT')
        status=worker.status()
        self.assertIs(status['inflight'],False)
        self.assertIs(status['cleanup_pending'],False)
        self.assertEqual(status['state'],'timeout')

    def test_generation_replacement_never_reuses_previous_green(self):
        self._align_bundle_and_journal()
        self._full_marker()
        first = self._step_until_facts()['account_facts']
        self.assertNotEqual(first['status'], 'missing')
        self.preflight.expected_capture_generation = '0' * 64
        replaced = self._step_until_facts()['account_facts']
        self.assertEqual(replaced['status'], 'observation_blocked')
        self.assertIn('CURRENT_ACCOUNT_FACTS_WORKER_VALIDATION_FAILED',
                      replaced['reason'])
        self.assertIs(replaced['entry_authorized'], False)

    def test_cycle_without_a_fresh_step_reports_missing_not_last_green(self):
        self._align_bundle_and_journal()
        self._full_marker()
        first = self._step_until_facts()['account_facts']
        self.assertNotEqual(first['status'], 'missing')
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy):
            unrefreshed = self.supervisor.account_facts_unrefreshed_status()
        self.assertEqual(unrefreshed['status'], first['status'])
        self.assertIs(unrefreshed['fresh_cycle'], False)
        self.assertIs(unrefreshed['wait_progress'], True)
        self.assertIs(unrefreshed['entry_authorized'], False)
        self.assertIsNotNone(unrefreshed['observed_at'])

    def test_expired_original_observation_is_not_reused(self):
        self._align_bundle_and_journal()
        self._full_marker()
        first = self._step_until_facts()['account_facts']
        self.assertNotEqual(first['status'], 'observation_blocked')
        self.now[0] += timedelta(seconds=31)
        expired = self._step_until_facts(
            snap=self.snap(self.now[0].isoformat()))['account_facts']
        self.assertEqual(expired['status'], 'observation_blocked')
        self.assertIn('STALE', expired['reason'])
        self.assertIs(expired['entry_authorized'], False)

    def test_publication_failure_clears_previous_green(self):
        self._align_bundle_and_journal()
        self._full_marker()
        self._step_until_facts()
        self.supervisor.post_reconcile_publication = lambda snapshot: {
            'status': 'FAILED', 'committed': False,
            'current_evidence_valid': False,
            'reason': 'PUBLICATION_PROBE'}
        failed = self._step()['account_facts']
        self.assertEqual(failed['status'], 'observation_blocked')
        self.assertEqual(failed['reason'],
                         'CURRENT_CAPTURE_PUBLICATION_FAILED')
        self.assertIs(failed['refresh']['compiled'], False)
        self.assertIsNone(failed['observed_at'])
        self.assertEqual(self.book._current_account_facts.get('strategy'),
                         None)
        self.assertIs(failed['entry_authorized'], False)

    def test_journal_inconsistency_blocks_compiled_facts(self):
        self._align_bundle_and_journal()
        self._full_marker()
        self._step_until_facts()
        row = self.book.db.execute(
            'SELECT payload FROM broker_current WHERE id=1').fetchone()
        payload = json.loads(row[0])
        from datetime import datetime
        payload['observed_at'] = (
            datetime.fromisoformat(payload['observed_at'])
            + timedelta(seconds=1)).isoformat()
        self.book.db.execute(
            'UPDATE broker_current SET payload=? WHERE id=1',
            (json.dumps(payload),))
        self.book.db.commit()
        blocked = self._step_until_facts()['account_facts']
        self.assertEqual(blocked['status'], 'observation_blocked')
        self.assertIs(blocked['entry_authorized'], False)

    def test_context_disappearance_returns_missing_not_old_green(self):
        self._align_bundle_and_journal()
        self._full_marker()
        self._step_until_facts()
        self.book._current_preflight = MissingCurrentContext(
            'CURRENT_CONTEXT_REGISTRY_MISSING')
        missing = self._step()['account_facts']
        self.assertEqual(missing['status'], 'missing')
        self.assertIsNone(missing['observed_at'])
        self.assertIs(missing['entry_authorized'], False)
        self.assertEqual(self.book._current_account_facts.get('strategy'),
                         None)

    def test_restart_starts_missing_and_does_not_reuse_process_history(self):
        self._align_bundle_and_journal()
        self._full_marker()
        self._step_until_facts()
        restarted = LiveSupervisor(self.book)
        self.addCleanup(restarted.close)
        self.assertEqual(restarted.account_facts_status['status'], 'missing')
        self.assertIsNone(restarted.account_facts_status['observed_at'])
        self.book._current_preflight = MissingCurrentContext(
            'CURRENT_CONTEXT_REGISTRY_MISSING')
        missing = restarted.step(self.snap(), [],
                                 allow_entries=False)['account_facts']
        self.assertEqual(missing['status'], 'missing')
        self.assertIs(missing['entry_authorized'], False)

    def test_slow_worker_does_not_block_management_ticks_and_timeout(self):
        events = []

        class Recorder:
            requires_observation = False
            def __call__(self, *, allow_entries=True, allow_dispatch=True):
                events.append('dispatch')
                return {'agent_called': False,
                        'reason': 'RECORDED_MANAGEMENT_PATH'}

        self._align_bundle_and_journal()
        worker=SlowFactsWorker()
        self.supervisor.account_facts_worker=worker
        self.supervisor.dispatcher=Recorder()
        started=time.monotonic()
        statuses=[]
        for _ in range(3):
            statuses.append(self._step()['account_facts'])
        elapsed=time.monotonic()-started
        self.assertTrue(all(item['status']=='inflight' for item in statuses))
        self.assertEqual(len(worker.starts),1)
        self.assertEqual(events,['dispatch','dispatch','dispatch'])
        self.assertLess(elapsed,1.0)
        worker.mode='timeout'
        result=self._step()
        facts=result['account_facts']
        self.assertEqual(facts['status'],'observation_blocked')
        self.assertEqual(facts['reason'],
                         'CURRENT_ACCOUNT_FACTS_WORKER_TIMEOUT')
        self.assertEqual(facts['worker']['state'],'timeout')
        self.assertEqual(events,['dispatch']*4)
        self.assertIs(facts['entry_authorized'],False)

    def test_epoch_change_cancels_inflight_worker_and_restarts(self):
        self._align_bundle_and_journal()
        worker=SlowFactsWorker()
        self.supervisor.account_facts_worker=worker
        self._step()
        self.assertEqual(len(worker.starts),1)
        payload=json.loads(self.book.db.execute(
            'SELECT payload FROM broker_current WHERE id=1').fetchone()[0])
        payload['epoch_probe']='changed'
        self.book.db.execute(
            'UPDATE broker_current SET payload=? WHERE id=1',
            (json.dumps(payload),))
        self.book.db.commit()
        changed=self._step()['account_facts']
        self.assertIn('CURRENT_ACCOUNT_FACTS_WORKER_EPOCH_CHANGED',
                      worker.cancelled)
        self.assertEqual(len(worker.starts),2)
        self.assertEqual(changed['status'],'inflight')
        self.assertIs(changed['entry_authorized'],False)

    def test_late_worker_result_epoch_is_discarded(self):
        self._align_bundle_and_journal()
        worker=SlowFactsWorker()
        self.supervisor.account_facts_worker=worker
        self._step()
        self.assertEqual(len(worker.starts),1)
        worker.response=_facts_response(
            worker.starts[0],ready=True,epoch_override='0'*64)
        worker.mode='release'
        facts=self._step()['account_facts']
        self.assertEqual(facts['status'],'observation_blocked')
        self.assertEqual(facts['reason'],
                         'CURRENT_ACCOUNT_FACTS_WORKER_EPOCH_CHANGED')
        self.assertIs(facts['entry_authorized'],False)
        self.assertIsNone(self.supervisor._account_facts_last_status)

    def test_two_owners_keep_purpose_and_cache_isolated(self):
        self._align_bundle_and_journal()
        self._full_marker()
        engineering_policy = deepcopy(POLICY)
        engineering_policy['engineering_only'] = True
        engineering_preflight = self._preflight(
            engineering_policy, self.now)
        engineering_book = self._book(
            Path(self.temp.name) / 'engineering.sqlite',
            engineering_policy, engineering_preflight)
        self._align_bundle_and_journal(engineering_book)
        self._full_marker(engineering_book)
        engineering_supervisor = LiveSupervisor(engineering_book)
        self.addCleanup(engineering_supervisor.close)
        strategy = self._step_until_facts()['account_facts']
        engineering = self._step_until_facts(engineering_supervisor)['account_facts']
        self.assertEqual(strategy['purpose'], 'strategy')
        self.assertEqual(engineering['purpose'], 'engineering')
        self.assertTrue(all(value is None for value in
                            self.book._current_account_facts.values()))
        self.assertTrue(all(value is None for value in
                            engineering_book._current_account_facts.values()))
        self.assertNotEqual(strategy['refresh']['epoch_id'],
                            engineering['refresh']['epoch_id'])
        self.assertEqual(strategy['account']['suffix'], '0000')
        self.assertEqual(engineering['account']['suffix'], '0000')
        self.assertIs(strategy['entry_authorized'], False)
        self.assertIs(engineering['entry_authorized'], False)

    def test_readiness_blocked_is_reported_without_entry_authority(self):
        self._align_bundle_and_journal()
        self._full_marker()
        self.supervisor.account_facts_worker=ScriptedFactsWorker(ready=False)
        facts = self._step_until_facts()['account_facts']
        self.assertEqual(facts['status'], 'readiness_blocked')
        self.assertEqual(facts['reason'],
                         'ENTRY_AUTHORITY_CURRENT_FACTS_BLOCKED')
        self.assertIn('CURRENT_POSITION_NOT_FLAT:equity',
                      facts['layers']['readiness']['reasons'])
        self.assertIs(facts['permit_missing'], False)
        self.assertIs(facts['entry_authorized'], False)

    def test_facts_ready_is_only_permit_missing_in_default_step(self):
        self._align_bundle_and_journal()
        self._full_marker()
        self.supervisor.account_facts_worker=ScriptedFactsWorker(ready=True)
        facts = self._step_until_facts()['account_facts']
        self.assertEqual(facts['status'], 'facts_ready')
        self.assertIs(facts['permit_missing'], True)
        self.assertIs(facts['facts_ready'], True)
        self.assertEqual(
            facts['reason'],
            'ENTRY_AUTHORITY_CURRENT_FACTS_READY_PERMIT_MISSING')
        self.assertEqual(facts['consumer']['reason'],
                         'ENTRY_AUTHORITY_CURRENT_FACTS_READY_PERMIT_MISSING')
        self.assertIs(facts['entry_authorized'], False)
        self.assertIs(facts['production_ready'], False)
        self.assertEqual(self.book.actions(), [])


class ReconciliationFactsTests(TestCase):
    def test_unreconciled_step_reports_observation_blocked(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        bundle = BundleFixture(Path(temp.name) / 'bundle')
        public = PublicFixture(Path(temp.name) / 'public')
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   public.policy):
            compiled = compile_current_evidence(
                bundle.root, public.manifest, now=NOW_LATER)
        policy = deepcopy(POLICY)
        preflight = CurrentEngineeringPreflight(
            bundle.root, public.manifest,
            account_key=compiled['account']['account_key_hash'],
            account_suffix='0000', policy_sha256=digest(policy),
            source_fingerprint_value=source_fingerprint(),
            clock=lambda: NOW_LATER)
        book = ExecutionBook(
            Path(temp.name) / 'book.sqlite', policy, simulation=True,
            clock=lambda: NOW_LATER, current_preflight=preflight)
        self.addCleanup(book.close)
        supervisor = LiveSupervisor(book)
        with patch.object(supervisor, '_reconcile', return_value={
                'reconciled': False,
                'needs_reconciliation': True,
                'reason': 'RECONCILIATION_STORAGE_FAILED'}):
            result = supervisor.step({'observed_at': OBSERVED}, [],
                                     allow_entries=False)
        self.assertEqual(result['account_facts']['status'],
                         'observation_blocked')
        self.assertEqual(result['account_facts']['reason'],
                         'CURRENT_RECONCILIATION_FAILED')
        self.assertIs(result['account_facts']['entry_authorized'], False)


class ContextGenerationTests(TestCase):
    def test_step_consumes_published_generation_b_or_reports_missing(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        bundle = BundleFixture(root / 'bundle')
        public = PublicFixture(root / 'public')
        now = [CAPTURE_T]
        policy = deepcopy(POLICY)
        publisher = CapturePublisher(
            root / 'captures', public.manifest, policy=policy,
            account_key=CAPTURE_ACCOUNT_KEY, account_suffix='0000',
            source_fingerprint_value=source_fingerprint(),
            clock=lambda: now[0])
        registry = CurrentContextRegistry(
            publisher.registry_path,
            allowed_capture_roots=(publisher.root,),
            allowed_public_roots=(public.root,),
            clock=lambda: now[0])
        factory = CurrentContextFactory(registry, clock=lambda: now[0])
        book = ExecutionBook(
            root / 'book.sqlite', policy, simulation=False,
            clock=lambda: now[0])
        self.addCleanup(book.close)
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   public.policy):
            first = publisher.produce_and_publish(
                FakeReadTransport(), timeout_seconds=5)
            context, status = ensure_default_preflight(
                book, factory=factory, clock=lambda: now[0])
            self.assertEqual(status['status'], 'bound', status)
            supervisor = LiveSupervisor(book)
            self.addCleanup(supervisor.close)
            def step_until(snapshot_value):
                for _ in range(300):
                    result=supervisor.step(
                        snapshot_value, [], allow_entries=False)
                    if result['account_facts']['status']!='inflight':
                        return result
                    time.sleep(.01)
                raise AssertionError('facts worker did not settle')
            observed = snapshot(FakeBroker(), now[0].isoformat())
            journal=BrokerJournal(book.db_path)
            try: journal.ingest(observed)
            finally: journal.close()
            step_a = step_until(observed)
            self.assertEqual(step_a['account_facts']['generation'],
                             first['generation_id'])
            second = publisher.produce_and_publish(
                CashTransport('501'), timeout_seconds=5)
            observed = snapshot(FakeBroker(), now[0].isoformat())
            journal=BrokerJournal(book.db_path)
            try: journal.ingest(observed)
            finally: journal.close()
            step_b = step_until(observed)
        self.assertEqual(step_b['account_facts']['generation'],
                         second['generation_id'])
        self.assertEqual(step_b['account_facts']['context']['status'],
                         'bound')
        self.assertNotEqual(step_b['account_facts']['generation'],
                            first['generation_id'])
        self.assertIs(step_b['account_facts']['entry_authorized'], False)


class DefaultCompositionFactsTests(TestCase):
    def test_run_live_session_status_file_has_diagnostics_without_signal(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        workspace = root / 'workspace'
        status_file = workspace / 'status.json'
        with redirect_stdout(io.StringIO()):
            result = run_live_session(
                workspace / 'book.sqlite', simulation_policy(),
                symbols=SIM_SYMBOLS, cycles=1, agent_mode='disabled',
                simulation=True, simulation_workspace=workspace,
                simulation_sources=SimulationSources(
                    broker=SimulationBroker(), market=SimulationMarket(),
                    review=SimulationReviewTransport, wake=SimulationWake()),
                output_dir=workspace / 'output', status_file=status_file,
                clock=lambda: SIM_NOW, budget_entry_limit=0,
                budget_total_limit=0)
        persisted = json.loads(status_file.read_text())
        facts = persisted['account_facts']
        self.assertEqual(facts['status'], 'missing')
        self.assertEqual(facts['reason'],
                         'CURRENT_ACCOUNT_FACTS_READER_MISSING')
        self.assertIs(facts['entry_authorized'], False)
        self.assertIs(facts['production_ready'], False)
        self.assertIs(facts['facts_are_not_entry_authorization'], True)
        self.assertEqual(result['account_facts'], facts)
        self.assertFalse(status_file.read_text().count('"entry_authorized": true'))


class DefaultWaitSchedulerFactsTests(TestCase):
    def test_default_run_loop_harvests_real_child_during_wait(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        public = PublicFixture(root / 'public')
        policy = simulation_policy()
        workspace = root / 'workspace'
        publisher = CapturePublisher(
            root / 'captures', public.manifest, policy=policy,
            account_key=CAPTURE_ACCOUNT_KEY, account_suffix='0000',
            source_fingerprint_value=source_fingerprint(),
            clock=lambda: CAPTURE_T)
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   public.policy):
            published = publisher.produce_and_publish(
                FakeReadTransport(), timeout_seconds=5)
            self.assertEqual(published['status'], 'PUBLISHED')
            preflight = CurrentEngineeringPreflight(
                published['capture_root'], public.manifest,
                account_key=CAPTURE_ACCOUNT_KEY, account_suffix='0000',
                policy_sha256=digest(policy),
                source_fingerprint_value=source_fingerprint(),
                clock=lambda: CAPTURE_T,
                expected_capture_generation=published['generation_id'])
            status_file = workspace / 'status.json'
            with redirect_stdout(io.StringIO()):
                result = run_live_session(
                    workspace / 'book.sqlite', policy,
                    symbols=SIM_SYMBOLS, cycles=2, agent_mode='disabled',
                    simulation=True, simulation_workspace=workspace,
                    simulation_sources=SimulationSources(
                        broker=SimulationBroker(), market=SimulationMarket(),
                        review=SimulationReviewTransport,
                        wake=SimulationWake()),
                    output_dir=workspace / 'output',
                    status_file=status_file, clock=lambda: SIM_NOW,
                    budget_entry_limit=0, budget_total_limit=0,
                    current_preflight=preflight)
        persisted = json.loads(status_file.read_text())
        facts = persisted['account_facts']
        self.assertGreaterEqual(facts['refresh']['consumed_count'], 1)
        self.assertIsNotNone(facts['refresh']['last_completed'])
        self.assertIs(facts['refresh']['last_completed']['fresh_cycle'], False)
        self.assertIs(facts['entry_authorized'], False)
        self.assertIs(facts['production_ready'], False)
        self.assertEqual(persisted['lifecycle']['actions'], [])
        shutdown = persisted.get('account_facts_shutdown', {}).get(
            'account_facts_worker', {})
        self.assertIs(shutdown.get('inflight'), False)
        self.assertIs(shutdown.get('cleanup_pending'), False)
        self.assertEqual(result['account_facts']['entry_authorized'], False)
