"""Bounded real-market lifecycle supervisor; model calls only for queued actions."""
from datetime import datetime, timedelta, timezone
from itertools import count
import json
import hashlib
import inspect
import math
import os
import sqlite3
import sys
from pathlib import Path
import threading
import time

from martin_agentic.account_facts_worker import AccountFactsWorker, REQUEST_KIND
from martin_agentic.agent_worker import CodexOrderWorker, dispatch_one, notify_local
from martin_agentic import current_evidence as current_evidence_module
from martin_agentic.control_tasks import BackgroundTask, FrozenSignals, persist_worker_result, worker_lease, run_leased_worker, inspect_worker_receipts
from martin_agentic.worker_budget import WorkerBudget
from martin_agentic.audit import canonical, digest
from martin_agentic.broker import BrokerJournal, collect_broker
from martin_agentic.calendar import is_regular, session
from martin_agentic.collector import collect_daily, universe_event
from martin_agentic.execution import ExecutionBook, utc_now
from martin_agentic.local_worker_wake import worker_adapter_report
from martin_agentic.capture_archive import persist_raw_record, archive_event, archive_decision
from martin_agentic.ledger import timestamp
from martin_agentic.risk import number
from martin_agentic.runtime_health import health_scope, observation_healthy
from martin_agentic.qualification import source_fingerprint, source_pin
from martin_agentic.market import RobinhoodReader, bar_available_at, inspect_recording
from martin_agentic.shadow import single_instance
from martin_agentic.transport import CodexBrokerReadTransport, CodexMarketTransport, TransportError, safe_read_failure
from trading_contracts.market import bar_observation_identity


def filter_unchanged_bars(bars,state):
    """Only omit exact economic matches already durably consumed by the owner.

    Changed bars must reach the owner so its revision gate can reject them.
    Raw provider responses are archived before this collection optimization.

    ``state`` may also be a collection view. That is the multi-instance form: a
    bar is omitted only when *every* consumer already holds the same content.
    """
    if hasattr(state,'should_emit'):
        changed=[];omitted=0
        for data in bars:
            if state.should_emit(data):changed.append(data)
            else:omitted+=1
        return changed,omitted
    known=state.get('bar_observations',{})
    changed=[];omitted=0
    for data in bars:
        key,fingerprint=bar_observation_identity(data)
        if known.get(key)==fingerprint:omitted+=1
        else:changed.append(data)
    return changed,omitted


def _no_exit_features(state):
    """Generic sessions return no strategy features; the Book keeps hard rules."""
    return {}


class _NoStrategySignals:
    """Stand-in when a generic session runs no legacy in-process strategy.

    The instance path owns strategy state per instance. The legacy entry loop
    refuses to consume rather than falling back to a concrete strategy.
    """

    def __init__(self,book):
        self.book=book

    def state(self):
        return {'machines':{}}

    def consume(self,event):
        raise ValueError('STRATEGY_RUNTIME_NOT_CONFIGURED')

    def pending(self):
        return []

    def complete(self,signal_id,*,result):
        return None


def _no_feature_keys(plugin_id):
    return ()


SOURCE_MODES=('synthetic','real','real_read_only')
READ_SHUTDOWN_GRACE_SECONDS=2.0
READ_SHUTDOWN_TOTAL_SECONDS=4.0
ACCOUNT_FACTS_REUSE_TTL_SECONDS=5.0
ACCOUNT_FACTS_WORKER_TIMEOUT_SECONDS=2.0
ACCOUNT_FACTS_REASON_LIMIT=20
ACCOUNT_FACTS_REASON_MAX_LENGTH=256


class LiveSupervisor:
    def __init__(self,book,*,worker=None,dispatcher=None,agent_mode='review',owner_source=None,strategy_queue=None,process_strategy=False,instance_supervisor=None,admission=None,accounting=None,exit_advisor=None,ports=None,source_mode=None,current_preflight=None,account_facts_worker=None,worker_budget=None,worker_budget_entries=False,signals=None,signals_factory=None,exit_features=None,feature_keys_for=None,strategy_composition=None):
        if type(process_strategy) is not bool:raise ValueError('STRATEGY_PROCESS_FLAG_INVALID')
        if source_mode is None:
            source_mode='synthetic' if book.simulation else 'real'
        if source_mode not in SOURCE_MODES:
            raise ValueError('SOURCE_MODE_INVALID')
        self.source_mode=source_mode
        self.exit_features_hook=exit_features or _no_exit_features
        self.feature_keys_for=feature_keys_for or _no_feature_keys
        if process_strategy and strategy_queue is None:
            builder=getattr(strategy_composition,'build_strategy_queue',None)
            if not callable(builder):
                raise ValueError('STRATEGY_RUNTIME_NOT_CONFIGURED')
            strategy_build=owner_source or source_fingerprint()
            strategy_queue=builder(book,owner_source=strategy_build,
                source_fingerprint=strategy_build)
        self._strategy_pending_errors=[]
        self.strategy_process_enabled=strategy_queue is not None
        self.book=book
        self.capture_only=(isinstance(getattr(book,'policy',None),dict)
            and book.policy.get('capture_only') is True)
        if current_preflight is not None:
            self.book._current_preflight=current_preflight
            self.book._current_context_refresher=None
            self.book._current_context_factory=None
            self.current_context_status={'status':'injected','factory_calls':0}
        else:
            from martin_agentic.current_context import ensure_default_preflight
            _,self.current_context_status=ensure_default_preflight(book,clock=getattr(book,'clock',None))
        self.current_context_refresher=getattr(book,'_current_context_refresher',None)
        self.account_facts_worker=(
            account_facts_worker or AccountFactsWorker(
                timeout_seconds=ACCOUNT_FACTS_WORKER_TIMEOUT_SECONDS))
        self._account_facts_worker_starts=0
        self._account_facts_consumed_count=0
        self._account_facts_inflight_epoch_id=None
        self._account_facts_last_epoch_id=None
        self._account_facts_last_completed_at=None
        self._account_facts_last_status=None
        self._account_facts_last_assessment=None
        self._account_facts_history=None
        self._account_facts_worker_state='idle'
        self._account_facts_worker_error=None
        self.account_facts_status=self._account_facts_missing(
            'CURRENT_ACCOUNT_FACTS_NOT_REFRESHED_THIS_PROCESS',
            purpose=self._account_facts_purpose(),
            context=self._account_facts_context(),
            now=self._account_facts_now(),
            worker=self._account_facts_worker_snapshot())
        # Owner-only post-reconcile hook. The saved observation is ingested and
        # reconciled by the owner before publication; assessment then runs
        # against the refreshed current context in the same step.
        self.post_reconcile_publication=None
        self.capture_publication_result=None
        if strategy_queue is not None:
            self.signals=strategy_queue.signals
        elif signals is not None:
            self.signals=signals
        elif signals_factory is not None:
            self.signals=signals_factory(book)
        else:
            self.signals=_NoStrategySignals(book)
        self.strategy_queue=strategy_queue;self.worker=worker;self.dispatcher=dispatcher
        # Optional multi-instance loop. When absent the supervisor keeps its
        # single legacy instance, so the default daemon path is unchanged.
        self.instance_supervisor=instance_supervisor
        self.instance_supervisor_enabled=instance_supervisor is not None
        # Optional same-session WorkerBudget. Admission may not create an entry
        # intent the local worker is not allowed to claim; that would reserve
        # account money and a symbol with no path to a worker.
        self.worker_budget=worker_budget
        # A WorkerBudget belongs to the model worker path that actually
        # consumes it. A targeted Host wake or a no-model profile must not be
        # blocked by an unrelated local worker's zero entry allowance.
        self.worker_budget_entries=bool(worker_budget_entries)
        # Account-wide admission for instance proposals. Absent means instance
        # advice is produced but never turned into an intent.
        self.admission=admission
        # One importer per session: broker receipts become instance feedback and
        # accounting here, on the owner connection, not in a test helper.
        self.receipt_importer=None
        self._receipts_incomplete=False
        self._bindings_incomplete=False
        # An execution-identity conflict is only cleared by an explicit
        # reconciliation decision this process does not have; a later clean
        # snapshot must not silently reopen the gate.
        # Durable, account-level integrity faults. This is the authoritative
        # gate; the in-memory flags below only cache this session's view.
        from trading_infra.integrity_faults import ExecutionIntegrityFaults
        try:
            self.integrity_faults=ExecutionIntegrityFaults(
                getattr(book,'db',None),clock=getattr(book,'clock',None))
            self.integrity_gate_unavailable=False
        except (ValueError,KeyError,TypeError,OSError,sqlite3.Error):
            # The ledger cannot even be opened: the gate is unknown, which is
            # not the same as clear. Every tick will report it closed.
            self.integrity_faults=None
            self.integrity_gate_unavailable=True
        self._gate_inhibit_persisted=False
        self._conflict_seen=False
        self.baseline_index=None
        if admission is not None and accounting is not None:
            from trading_infra.historical_baseline import HistoricalBaselineIndex
            from trading_infra.receipts import ExecutionReceiptImporter
            self.baseline_index=HistoricalBaselineIndex(self.book.db)
            self.receipt_importer=ExecutionReceiptImporter(
                self.book.db,attribution=admission.attribution,
                accounting=accounting,clock=getattr(self.book,'clock',None),
                baseline=self.baseline_index)
        if admission is not None:
            # Bind the claim guard to this session's attribution table, so an
            # entry whose reference has no owner can never be executed.
            self.book.entry_claim_guard=self._entry_reference_bound
        if strategy_queue is not None and self.signals.book is not book:
            raise ValueError('STRATEGY_QUEUE_OWNER_BOOK_MISMATCH')
        from martin_agentic.production_admission import ProductionAdmission
        # Bounded, out-of-process exit advice. ``None`` keeps the legacy
        # in-process decider for direct supervisor tests; the default session
        # composition always supplies one.
        self.exit_advisor=exit_advisor
        self._exit_advice_errors=[];self._exit_advice_history=[]
        # Application ports. Only the composition layer chooses concrete
        # adapters; the loop calls the port, never a provider tool name.
        self.ports=ports
        host_port=getattr(ports,'host',None) if ports is not None else None
        declared_host_status=getattr(ports,'host_delivery_status',None) if ports is not None else None
        self.host_delivery_status=(dict(declared_host_status)
            if isinstance(declared_host_status,dict) else
            ({'available':True} if host_port is not None
             and getattr(host_port,'available',False) else
             {'available':False,'reason':getattr(host_port,'reason',
                 'HOST_TARGETED_DELIVERY_NOT_CONFIGURED')}))
        self.host_port=host_port if self.host_delivery_status['available'] else None
        execution=getattr(ports,'execution',None) if ports is not None else None
        if execution is not None:
            default_ports=getattr(ports,'default',False)
            available=(callable(getattr(execution,'advance',None))
                       if default_ports
                       else callable(getattr(execution,'advance_queue',None)))
            if available and (dispatcher is None or default_ports):
                dispatcher=_PortAdvanceDispatcher(execution)
        if dispatcher is not None:self.dispatcher=dispatcher
        self.production=ProductionAdmission(book,agent_mode=agent_mode,
            owner_source=owner_source or source_fingerprint())

    def step(self,snapshot,events,*,allow_entries=True,allow_dispatch=True):
        self.capture_publication_result=None
        if getattr(self,'current_context_refresher',None) is not None:
            self.current_context_status=self.current_context_refresher.refresh()
        if self.instance_supervisor_enabled:
            return self._step_instances(snapshot,events,allow_entries=allow_entries,allow_dispatch=allow_dispatch)
        if self.strategy_queue is not None:
            return self._step_async(snapshot,events,allow_entries=allow_entries,allow_dispatch=allow_dispatch)
        quotes={};errors=[]
        no_strategy_capture=(self.capture_only
            and isinstance(self.signals,_NoStrategySignals))
        for event in events:
            if no_strategy_capture:
                # Capture-only with no strategy consumer still keeps the real
                # event and its source in the archive, but must not invoke a
                # strategy runtime that was never configured. "No consumer" is
                # an expected capture disposition, not a strategy error.
                try:
                    record_capture_disposition(
                        self.book,event,
                        reason='NO_STRATEGY_CONSUMER_CAPTURE_ONLY')
                except (ValueError,TypeError,KeyError,OSError,sqlite3.Error):
                    errors.append({'rejected':True,
                        'reason':'REJECTION_ARCHIVE_FAILED',
                        'scope':'STRATEGY_INPUT'})
                if event.get('kind')=='quote' and isinstance(event.get('data'),dict):
                    symbol=event['data'].get('symbol')
                    if symbol is not None:quotes[symbol]=event['data']
                continue
            try:
                self.signals.consume(event)
                if event['kind']=='quote':quotes[event['data']['symbol']]=event['data']
            except (ValueError,KeyError,TypeError) as exc:
                reason=str(exc);errors.append({'rejected':True,'reason':reason,'scope':'STRATEGY_INPUT'})
                at=self.book.clock().isoformat()
                rejection={'id':'strategy-rejected-'+digest([event,reason,at]),'at':at,
                    'kind':'strategy_rejection','data':{'original_event':event,'reason':reason},
                    'source_sha256':digest(event),'source_type':'local_control_diagnostic',
                    'synthetic_input':self.source_mode=='synthetic'}
                try:record_capture_disposition(self.book,rejection,reason=reason)
                except (ValueError,TypeError,KeyError,OSError,sqlite3.Error):
                    errors.append({'rejected':True,'reason':'REJECTION_ARCHIVE_FAILED','scope':'STRATEGY_INPUT'})
                if reason in {'CONSUMED_BAR_REVISED','STRATEGY_EVENT_ID_CONFLICT','STRATEGY_EVENT_PROVENANCE_REQUIRED'}:
                    from martin_agentic.watchdog import inhibit_new_entries
                    db=self.book.db.execute('PRAGMA database_list').fetchone()[2]
                    try:inhibit_new_entries(db,['STRATEGY_INPUT_INTEGRITY:'+reason],clock=self.book.clock)
                    except OSError:
                        errors.append({'rejected':True,'reason':'ENTRY_INHIBIT_PERSISTENCE_FAILED','scope':'STRATEGY_INPUT'})
        # The legacy synchronous shape reads the same reconciliation authority
        # as the queued and instance shapes. It used to call the book directly,
        # so an execution conflict escaped as a read error and was never written
        # down; a restart then had nothing to consult.
        reconciled=self._reconcile(snapshot,quotes,errors)
        if not reconciled['reconciled']:
            return self._unreconciled_step(snapshot,quotes,errors,reconciled)
        durable_blocked=self._durable_integrity_blocked()
        if durable_blocked or self.integrity_gate_unavailable:
            errors.append({'reason':('EXECUTION_INTEGRITY_FAULT_UNRESOLVED'
                                     if durable_blocked else
                                     'EXECUTION_INTEGRITY_GATE_UNAVAILABLE'),
                           'scope':'STRATEGY_EXECUTION','source':'durable_gate'})
        result=self._finish_step(snapshot,quotes,errors,allow_entries=allow_entries,
            allow_dispatch=allow_dispatch,reconciled=True)
        if durable_blocked or self.integrity_gate_unavailable:
            result.update(reconciliation_pending=True,new_entries_blocked=True,
                settlement_blocked='EXECUTION_INTEGRITY_FAULT_UNRESOLVED',
                integrity_inhibit_persisted=self._gate_inhibit_persisted,
                **self._integrity_fields())
            if self.integrity_gate_unavailable:
                result['stop_disposition']=('EXECUTION_INTEGRITY_FAULT_LEDGER_UNAVAILABLE_'
                                            'MANUAL_HANDOFF_REQUIRED')
        return result

    def _session_read_only_forbidden(self):
        return self.source_mode == 'real_read_only'

    # --- current-account facts diagnostics in the default owner loop --------
    #
    # The facts assessment used to exist only as a side effect of propose/
    # claim/native requests.  A resident owner with no pending order therefore
    # had no current account-facts status at all.  These methods compile the
    # same canonical producer before returning every step, after reconcile and
    # owner publication, and expose a bounded, sanitized diagnostic.  They never
    # authorize an entry and never call a broker write.

    def _account_facts_purpose(self):
        policy=getattr(self.book,'policy',None)
        return ('engineering' if isinstance(policy,dict)
                and policy.get('engineering_only') is True else 'strategy')

    def _account_facts_now(self):
        try:
            value=self.book.clock()
        except Exception:
            return None
        return value if isinstance(value,datetime) else None

    @staticmethod
    def _account_facts_text(value):
        if not isinstance(value,str):
            return None
        value=value[:ACCOUNT_FACTS_REASON_MAX_LENGTH]
        return value or None

    @classmethod
    def _account_facts_reasons(cls,values):
        result=[]
        for value in values or []:
            text=cls._account_facts_text(value)
            if text is None or text in result:
                continue
            result.append(text)
            if len(result)>=ACCOUNT_FACTS_REASON_LIMIT:
                break
        return result

    def _account_facts_context(self):
        status=getattr(self,'current_context_status',None)
        if not isinstance(status,dict):
            return {'status':'unknown','generation_id':None,
                    'reason':None,'factory_calls':None}
        return {'status':self._account_facts_text(status.get('status')),
                'generation_id':self._account_facts_text(
                    status.get('generation_id')),
                'reason':self._account_facts_text(status.get('reason')),
                'factory_calls':(status.get('factory_calls')
                                 if type(status.get('factory_calls')) is int
                                 else None)}

    @classmethod
    def _account_facts_publication(cls,publication):
        if not isinstance(publication,dict):
            return None
        result={}
        for key in ('status','committed','current_evidence_valid','generation'):
            value=publication.get(key)
            if key in {'status','generation'}:
                value=cls._account_facts_text(value)
            elif type(value) is not bool:
                value=None
            if value is not None:
                result[key]=value
        reason=cls._account_facts_text(publication.get('reason'))
        if reason is not None:
            result['reason']=reason
        return result

    def _account_facts_worker_snapshot(self):
        try:
            value=self.account_facts_worker.status()
        except Exception:
            value=None
        if not isinstance(value,dict):
            value={'running':False,'inflight':False,
                   'state':self._account_facts_worker_state,
                   'error':self._account_facts_worker_error}
        result=dict(value)
        for key in ('age_seconds','timeout_seconds',
                    'deadline_remaining_seconds'):
            value=result.get(key)
            if type(value) not in (int,float) or not math.isfinite(value):
                result[key]=None
        for key in ('epoch_id','state','error'):
            value=result.get(key)
            if value is not None:
                result[key]=self._account_facts_text(value)
        for key in ('running','inflight','cleanup_pending',
                    'read_only_child'):
            result[key]=result.get(key) is True
        for key in ('broker_writes','book_mutations'):
            if type(result.get(key)) is not int:
                result[key]=None
        return result

    def _account_facts_history_summary(self):
        if isinstance(self._account_facts_history,dict):
            return dict(self._account_facts_history)
        last=self._account_facts_last_status
        if not isinstance(last,dict):
            return None
        return {'status':self._account_facts_text(last.get('status')),
                'reason':self._account_facts_text(last.get('reason')),
                'observed_at':self._account_facts_text(
                    last.get('observed_at')),
                'generation':self._account_facts_text(
                    last.get('generation')),
                'epoch_id':self._account_facts_text(
                    self._account_facts_last_epoch_id),
                'fresh_cycle':False}

    def _account_facts_refresh_fields(self,*,now,compiled,reused,reason,
                                      epoch_id=None):
        return {'compiled':bool(compiled),'reused':bool(reused),
                'last_completed':self._account_facts_history_summary(),
                'count':self._account_facts_worker_starts,
                'worker_starts':self._account_facts_worker_starts,
                'consumed_count':self._account_facts_consumed_count,
                'reuse_ttl_seconds':ACCOUNT_FACTS_REUSE_TTL_SECONDS,
                'worker_timeout_seconds':ACCOUNT_FACTS_WORKER_TIMEOUT_SECONDS,
                'epoch_id':self._account_facts_text(epoch_id),
                'at':(now.isoformat()
                      if isinstance(now,datetime) else None),
                'source':'LiveSession.account_facts_worker',
                'reason':self._account_facts_text(reason)}

    def _account_facts_freshness(self,*,compiled,reused):
        return {'source':('compiled_by_worker' if compiled else
                          'reused_cache' if reused else 'unavailable'),
                'validation':('worker_validated_and_owner_epoch_checked'
                              if compiled else
                              'not_revalidated_on_reuse' if reused else
                              'no_current_assessment')}

    def _account_facts_account(self,assessment=None):
        policy=getattr(self.book,'policy',None)
        suffix=(policy.get('account_suffix')
                if isinstance(policy,dict) else None)
        if not (isinstance(suffix,str) and len(suffix)==4
                and suffix.isdigit()):
            suffix=None
        mask=None
        if isinstance(assessment,dict):
            account=assessment.get('account')
            if isinstance(account,dict):
                candidate=account.get('mask')
                if isinstance(candidate,str) and candidate.startswith('••••'):
                    mask=candidate
                candidate=account.get('suffix')
                if (isinstance(candidate,str) and len(candidate)==4
                        and candidate.isdigit()):
                    suffix=candidate
        if mask is None and suffix is not None:
            mask='••••'+suffix
        return {'mask':mask,'suffix':suffix}

    def _account_facts_missing(self,reason,*,purpose=None,context=None,
                               publication=None,consumer_reason=None,
                               compiled=False,reused=False,now=None,
                               status='missing',worker=None,epoch_id=None):
        reason=self._account_facts_text(reason) or 'CURRENT_ACCOUNT_FACTS_MISSING'
        if status not in {'missing','observation_blocked','inflight'}:
            status='missing'
        context=(context if isinstance(context,dict)
                 else self._account_facts_context())
        publication=self._account_facts_publication(publication)
        return {
            'schema_version':1,
            'kind':'current_account_facts_diagnostic_v1',
            'status':status,
            'assessment_status':None,
            'reason':reason,
            'reasons':[reason],
            'observed_at':None,
            'observation_valid_from':None,
            'observation_valid_until':None,
            'assessed_at':None,
            'generation':None,
            'expected_generation':None,
            'purpose':purpose,
            'account':self._account_facts_account(),
            'observation_valid':False,
            'facts_qualified':False,
            'account_entry_readiness':False,
            'entry_authorized':False,
            'production_ready':False,
            'facts_are_not_entry_authorization':True,
            'facts_ready':False,
            'permit_missing':False,
            'layers':{
                'observation':{'valid':False,'reasons':[reason]},
                'facts':{'qualified':False,'reasons':[]},
                'readiness':{'ready':False,'reasons':[]},
            },
            'consumer':{'reason':self._account_facts_text(consumer_reason),
                        'facts_ready':False,'permit_missing':False},
            'context':context,
            'publication':publication,
            'worker':(worker if isinstance(worker,dict)
                      else self._account_facts_worker_snapshot()),
            'refresh':self._account_facts_refresh_fields(
                now=now,compiled=compiled,reused=reused,reason=reason,
                epoch_id=epoch_id),
            'freshness':self._account_facts_freshness(
                compiled=compiled,reused=reused),
        }

    def _account_facts_diagnostic(self,assessment,*,purpose,context,
                                  consumer,publication,compiled,reused,
                                  now,worker=None,epoch_id=None,
                                  fresh_cycle=True):
        binding=(assessment.get('binding')
                 if isinstance(assessment.get('binding'),dict) else {})
        observation_valid=assessment.get('observation_valid') is True
        facts_qualified=assessment.get('facts_qualified') is True
        readiness=assessment.get('account_entry_readiness') is True
        if not observation_valid:
            assessment_status='observation_blocked'
        elif not facts_qualified:
            assessment_status='facts_blocked'
        elif not readiness:
            assessment_status='readiness_blocked'
        else:
            assessment_status='facts_ready'
        observation_reasons=self._account_facts_reasons(
            assessment.get('observation_reasons'))
        facts_reasons=self._account_facts_reasons(
            assessment.get('facts_reasons'))
        readiness_reasons=self._account_facts_reasons(
            assessment.get('readiness_reasons'))
        consumer=(consumer if isinstance(consumer,dict) else {})
        consumer_reason=self._account_facts_text(consumer.get('reason'))
        consumer_allowed=consumer.get('allowed') is True
        status=assessment_status
        reason=None
        permit_missing=False
        facts_ready=False
        if consumer_reason=='ENTRY_AUTHORITY_CURRENT_FACTS_READY_PERMIT_MISSING':
            if assessment_status=='facts_ready':
                status='facts_ready'
                facts_ready=True
                permit_missing=True
                reason=consumer_reason
        elif consumer_reason=='ENTRY_AUTHORITY_CURRENT_FACTS_BLOCKED':
            if status=='facts_ready':
                status='facts_blocked'
            reason=consumer_reason
        elif consumer_reason in {
                'ENTRY_AUTHORITY_CURRENT_FACTS_STALE',
                'ENTRY_AUTHORITY_CURRENT_FACTS_UNTRUSTED'}:
            status='observation_blocked'
            reason=consumer_reason
        elif (consumer_reason is not None
              and consumer_reason.startswith('ENTRY_AUTHORITY_CURRENT_FACTS')):
            # Includes MISSING.  A stale diagnostic cannot be promoted to
            # current facts merely because an older assessment compiles.
            status='missing'
            reason=consumer_reason
        elif consumer_reason is not None and not consumer_allowed:
            # Identity-only/REVOKED/legacy/unknown/tampered authority is not a
            # facts-ready state and must not be cleared by this diagnostic.
            status='missing'
            reason=consumer_reason
        elif assessment_status=='facts_ready':
            # The only allowed consumer here is the synthetic no-namespace
            # compatibility path.  It is still not an entry permit.
            status='facts_ready'
            facts_ready=True
            permit_missing=True
            reason='ENTRY_AUTHORITY_CURRENT_FACTS_READY_PERMIT_MISSING'
        if status=='observation_blocked' and reason is None:
            reason=(observation_reasons[0] if observation_reasons
                    else 'CURRENT_ACCOUNT_FACTS_OBSERVATION_BLOCKED')
        elif status=='facts_blocked' and reason is None:
            reason=(facts_reasons[0] if facts_reasons
                    else 'CURRENT_ACCOUNT_FACTS_BLOCKED')
        elif status=='readiness_blocked' and reason is None:
            reason=(readiness_reasons[0] if readiness_reasons
                    else 'CURRENT_ACCOUNT_FACTS_READINESS_BLOCKED')
        elif status=='missing' and reason is None:
            reason='ENTRY_AUTHORITY_CURRENT_FACTS_MISSING'
        reasons=self._account_facts_reasons(
            observation_reasons+facts_reasons+readiness_reasons+
            ([reason] if reason is not None else []))
        generation=self._account_facts_text(
            binding.get('capture_generation'))
        expected=self._account_facts_text(
            binding.get('expected_capture_generation'))
        return {
            'schema_version':1,
            'kind':'current_account_facts_diagnostic_v1',
            'status':status,
            'assessment_status':assessment_status,
            'reason':reason,
            'reasons':reasons,
            'observed_at':self._account_facts_text(
                assessment.get('observed_at')),
            'observation_valid_from':self._account_facts_text(
                assessment.get('observation_valid_from')),
            'observation_valid_until':self._account_facts_text(
                assessment.get('observation_valid_until')),
            'assessed_at':self._account_facts_text(
                assessment.get('assessed_at')),
            'generation':generation,
            'expected_generation':expected,
            'purpose':purpose,
            'account':self._account_facts_account(assessment),
            'observation_valid':assessment.get('observation_valid') is True,
            'facts_qualified':assessment.get('facts_qualified') is True,
            'account_entry_readiness':
                assessment.get('account_entry_readiness') is True,
            'entry_authorized':False,
            'production_ready':False,
            'facts_are_not_entry_authorization':True,
            'fresh_cycle':bool(fresh_cycle),
            'wait_progress':not bool(fresh_cycle),
            'facts_ready':facts_ready,
            'permit_missing':permit_missing,
            'layers':{
                'observation':{
                    'valid':assessment.get('observation_valid') is True,
                    'reasons':observation_reasons},
                'facts':{
                    'qualified':assessment.get('facts_qualified') is True,
                    'reasons':facts_reasons},
                'readiness':{
                    'ready':assessment.get('account_entry_readiness') is True,
                    'reasons':readiness_reasons},
            },
            'consumer':{
                'reason':consumer_reason,
                'facts_ready':consumer.get('facts_ready') is True,
                'permit_missing':consumer.get('permit_missing') is True,
            },
            'context':context,
            'publication':self._account_facts_publication(publication),
            'worker':(worker if isinstance(worker,dict)
                      else self._account_facts_worker_snapshot()),
            'refresh':self._account_facts_refresh_fields(
                now=now,compiled=compiled,reused=reused,reason=reason,
                epoch_id=epoch_id),
            'freshness':self._account_facts_freshness(
                compiled=compiled,reused=reused),
        }

    @staticmethod
    def _account_facts_stat_key(path):
        try:
            info=os.stat(path)
        except (OSError,TypeError,ValueError):
            return None
        return [info.st_size,info.st_mtime_ns]

    def _account_facts_epoch_for(self,purpose,context):
        """Cheap owner epoch; never compiles capture/source or asks authority."""
        try:
            preflight=getattr(self.book,'_current_preflight',None)
            if preflight is None or getattr(preflight,'binding_id',None) is None:
                return None
            state=self.book.state()
            if not isinstance(state,dict):
                return None
            policy=getattr(self.book,'policy',None)
            if not isinstance(policy,dict):
                return None
            current_row=self.book.db.execute(
                'SELECT payload FROM broker_current WHERE id=1').fetchone()
            health_row=self.book.db.execute(
                'SELECT payload FROM broker_health WHERE id=1').fetchone()
            current_sha=(hashlib.sha256(current_row[0].encode()).hexdigest()
                         if current_row is not None else None)
            health_sha=(hashlib.sha256(health_row[0].encode()).hexdigest()
                        if health_row is not None else None)
            pin=source_pin()
            pin_key={'status':pin.get('status'),
                     'digest':pin.get('digest'),
                     'count':pin.get('count'),
                     'root':pin.get('root'),
                     'truncated':pin.get('truncated')}
            try:
                risk_state=self.book.risk_guard.state()
                risk_sha=(digest(risk_state)
                          if isinstance(risk_state,dict) else None)
            except Exception:
                risk_sha=None
            publication=getattr(self,'capture_publication_result',None)
            publication_generation=(
                publication.get('generation')
                if isinstance(publication,dict) else None)
            return {
                'schema_version':1,
                'kind':'account_facts_epoch_v1',
                'binding_id':getattr(preflight,'binding_id',None),
                'expected_capture_generation':getattr(
                    preflight,'expected_capture_generation',None),
                'context_generation':context.get('generation_id'),
                'account_key':state.get('account_key'),
                'policy_sha256':digest(policy),
                'simulation':self.book.simulation is True,
                'journal_current_sha256':current_sha,
                'journal_health_sha256':health_sha,
                'publication_generation':publication_generation,
                'purpose':purpose,
                'source_pin':pin_key,
                'risk_state_sha256':risk_sha,
                'public_policy_sha256':digest(
                    getattr(current_evidence_module,'PUBLIC_SOURCES',{})),
                'public_manifest':self._account_facts_stat_key(
                    getattr(preflight,'public_sources',None)),
                'capture_manifest':self._account_facts_stat_key(
                    str(Path(getattr(preflight,'capture_root',''))/
                        'manifest.json')
                    if getattr(preflight,'capture_root',None) else None),
            }
        except Exception:
            return None

    def _account_facts_worker_request(self,purpose,epoch,now):
        try:
            preflight=getattr(self.book,'_current_preflight',None)
            if (preflight is None
                    or not isinstance(getattr(preflight,'binding_id',None),str)
                    or not isinstance(getattr(preflight,'capture_root',None),str)
                    or not isinstance(getattr(preflight,'public_sources',None),str)
                    or not isinstance(getattr(preflight,'account_key',None),str)
                    or not isinstance(getattr(preflight,'account_suffix',None),str)
                    or not isinstance(getattr(preflight,'policy_sha256',None),str)
                    or not isinstance(getattr(preflight,'source_fingerprint_value',None),str)):
                return None
            db_path=getattr(self.book,'db_path',None)
            if not isinstance(db_path,str) or not db_path:
                row=self.book.db.execute('PRAGMA database_list').fetchone()
                db_path=row[2] if row else None
            if (not isinstance(db_path,str) or not db_path
                    or db_path==':memory:'):
                return None
            risk_db_path=None
            try:
                row=self.book.risk_guard.db.execute(
                    'PRAGMA database_list').fetchone()
                risk_db_path=row[2] if row else None
                if risk_db_path==':memory:':
                    risk_db_path=None
            except Exception:
                risk_db_path=None
            risk_state=self.book.risk_guard.state()
            return {
                'schema_version':1,
                'kind':REQUEST_KIND,
                'db_path':str(Path(db_path).resolve()),
                'risk_db_path':(str(Path(risk_db_path).resolve())
                                if risk_db_path else None),
                'risk_state':risk_state,
                'public_sources_policy':dict(
                    getattr(current_evidence_module,'PUBLIC_SOURCES',{})),
                'policy':dict(getattr(self.book,'policy',{})),
                'simulation':self.book.simulation is True,
                'clock':now.isoformat() if isinstance(now,datetime) else None,
                'purpose':purpose,
                'capture_root':preflight.capture_root,
                'public_sources':preflight.public_sources,
                'account_key':preflight.account_key,
                'account_suffix':preflight.account_suffix,
                'policy_sha256':preflight.policy_sha256,
                'source_fingerprint':preflight.source_fingerprint_value,
                'expected_capture_generation':
                    preflight.expected_capture_generation,
                'registry_sha256':getattr(preflight,'registry_sha256',None),
                'registry_path':getattr(preflight,'registry_path',None),
                'epoch':epoch,
                'epoch_id':digest(epoch),
            }
        except Exception:
            return None

    def _account_facts_clear_state(self):
        self._account_facts_last_epoch_id=None
        self._account_facts_last_completed_at=None
        self._account_facts_last_status=None
        self._account_facts_last_assessment=None
        try:
            self.book.clear_current_account_facts(
                purpose=self._account_facts_purpose())
        except Exception:
            pass

    def _account_facts_cache_is_current(self,assessment,now):
        if not isinstance(assessment,dict) or not isinstance(now,datetime):
            return False
        try:
            observed=timestamp(assessment.get('observed_at'))
            valid_until=timestamp(
                assessment.get('observation_valid_until'))
            return observed<=now<=valid_until
        except (TypeError,ValueError,OverflowError):
            return False

    def _account_facts_reuse(self,*,epoch_id,purpose,context,now):
        status=self._account_facts_last_status
        assessment=self._account_facts_last_assessment
        completed=self._account_facts_last_completed_at
        if (not isinstance(status,dict)
                or self._account_facts_last_epoch_id!=epoch_id
                or not isinstance(completed,datetime)
                or not isinstance(now,datetime)):
            return None
        age=(now-completed).total_seconds()
        if age<0 or age>ACCOUNT_FACTS_REUSE_TTL_SECONDS:
            return None
        if assessment is not None and not self._account_facts_cache_is_current(
                assessment,now):
            return None
        result=dict(status)
        result['refresh']=self._account_facts_refresh_fields(
            now=now,compiled=False,reused=True,
            reason='SAME_OWNER_EPOCH_WITHIN_REUSE_TTL',epoch_id=epoch_id)
        result['freshness']=self._account_facts_freshness(
            compiled=False,reused=True)
        result['worker']=self._account_facts_worker_snapshot()
        return result

    def _account_facts_worker_result_error(self,value,reason):
        return self._account_facts_missing(
            reason,purpose=self._account_facts_purpose(),
            context=self._account_facts_context(),
            publication=getattr(self,'capture_publication_result',None),
            now=self._account_facts_now(),
            status='observation_blocked',
            worker=self._account_facts_worker_snapshot(),
            epoch_id=self._account_facts_inflight_epoch_id)

    def _account_facts_cancel(self,reason):
        self._account_facts_inflight_epoch_id=None
        self._account_facts_worker_state='cancelled'
        self._account_facts_worker_error=reason
        self._account_facts_clear_state()
        try:
            return self.account_facts_worker.cancel(reason)
        except Exception:
            return False

    def _account_facts_start(self,purpose,context,publication,now,epoch,
                             epoch_id):
        request=self._account_facts_worker_request(purpose,epoch,now)
        if request is None:
            self._account_facts_clear_state()
            status=self._account_facts_missing(
                'CURRENT_ACCOUNT_FACTS_WORKER_REQUEST_UNAVAILABLE',
                purpose=purpose,context=context,publication=publication,
                now=now,worker=self._account_facts_worker_snapshot(),
                epoch_id=epoch_id)
            self.account_facts_status=status
            return status
        try:
            started=self.account_facts_worker.start(request)
        except Exception:
            started=False
        if started is not True:
            self._account_facts_inflight_epoch_id=None
            self._account_facts_worker_state='start_failed'
            self._account_facts_worker_error='ACCOUNT_FACTS_WORKER_START_FAILED'
            self._account_facts_clear_state()
            status=self._account_facts_missing(
                'CURRENT_ACCOUNT_FACTS_WORKER_START_FAILED',
                purpose=purpose,context=context,publication=publication,
                now=now,worker=self._account_facts_worker_snapshot(),
                epoch_id=epoch_id)
            self.account_facts_status=status
            return status
        self._account_facts_worker_starts+=1
        self._account_facts_worker_state='running'
        self._account_facts_worker_error=None
        self._account_facts_inflight_epoch_id=epoch_id
        self._account_facts_clear_state()
        status=self._account_facts_missing(
            'CURRENT_ACCOUNT_FACTS_WORKER_INFLIGHT',purpose=purpose,
            context=context,publication=publication,now=now,
            status='inflight',worker=self._account_facts_worker_snapshot(),
            epoch_id=epoch_id)
        self.account_facts_status=status
        return status

    def _account_facts_candidate_valid(self,assessment,*,epoch,epoch_id,
                                       purpose,now):
        if (not isinstance(assessment,dict)
                or assessment.get('kind')!=
                'current_account_facts_assessment_v1'
                or assessment.get('schema_version')!=1
                or assessment.get('facts_are_not_entry_authorization') is not True
                or assessment.get('entry_authorized') is not False
                or assessment.get('production_ready') is not False):
            return False,'CURRENT_ACCOUNT_FACTS_WORKER_ASSESSMENT_INVALID'
        supplied=assessment.get('assessment_id')
        if (not isinstance(supplied,str)
                or digest({key:value for key,value in assessment.items()
                           if key!='assessment_id'})!=supplied):
            return False,'CURRENT_ACCOUNT_FACTS_WORKER_ASSESSMENT_TAMPERED'
        if not isinstance(epoch,dict) or digest(epoch)!=epoch_id:
            return False,'CURRENT_ACCOUNT_FACTS_WORKER_EPOCH_CHANGED'
        binding=assessment.get('binding')
        preflight=getattr(self.book,'_current_preflight',None)
        if not isinstance(binding,dict) or preflight is None:
            return False,'CURRENT_ACCOUNT_FACTS_WORKER_BINDING_MISSING'
        expected_generation=getattr(
            preflight,'expected_capture_generation',None)
        if (binding.get('kind')!='current_account_facts_binding_v1'
                or binding.get('owner_binding_id')!=
                getattr(preflight,'binding_id',None)
                or binding.get('account_key')!=
                getattr(preflight,'account_key',None)
                or binding.get('account_suffix')!=
                getattr(preflight,'account_suffix',None)
                or binding.get('policy_sha256')!=
                getattr(preflight,'policy_sha256',None)
                or binding.get('source_fingerprint')!=
                getattr(preflight,'source_fingerprint_value',None)
                or (purpose is not None and binding.get('purpose')!=purpose)):
            return False,'CURRENT_ACCOUNT_FACTS_WORKER_BINDING_CHANGED'
        if (expected_generation is not None
                and binding.get('capture_generation')!=expected_generation):
            return False,'CURRENT_ACCOUNT_FACTS_WORKER_GENERATION_CHANGED'
        if binding.get('expected_capture_generation') not in (
                None,expected_generation):
            return False,'CURRENT_ACCOUNT_FACTS_WORKER_GENERATION_CHANGED'
        if binding.get('capture_generation')!=epoch.get(
                'expected_capture_generation') and epoch.get(
                    'expected_capture_generation') is not None:
            return False,'CURRENT_ACCOUNT_FACTS_WORKER_EPOCH_CHANGED'
        if (binding.get('account_key')!=epoch.get('account_key')
                or binding.get('policy_sha256')!=epoch.get('policy_sha256')
                or binding.get('parent_purpose') is not None
                or binding.get('purpose')!=epoch.get('purpose')):
            return False,'CURRENT_ACCOUNT_FACTS_WORKER_EPOCH_CHANGED'
        components=assessment.get('components')
        if not isinstance(components,dict):
            return False,'CURRENT_ACCOUNT_FACTS_WORKER_COMPONENTS_MISSING'
        source=components.get('source')
        pin=(source or {}).get('pin') if isinstance(source,dict) else None
        epoch_pin=epoch.get('source_pin') or {}
        if (not isinstance(pin,dict)
                or pin.get('status')!=epoch_pin.get('status')
                or pin.get('digest')!=epoch_pin.get('digest')
                or pin.get('count')!=epoch_pin.get('count')
                or pin.get('root')!=epoch_pin.get('root')
                or pin.get('truncated')!=epoch_pin.get('truncated')):
            return False,'CURRENT_ACCOUNT_FACTS_WORKER_SOURCE_CHANGED'
        for name,key in (('journal_current','journal_current_sha256'),
                         ('journal_health','journal_health_sha256')):
            component=components.get(name) or {}
            if (component.get('status')=='evaluated'
                    and component.get('sha256')!=epoch.get(key)):
                return False,'CURRENT_ACCOUNT_FACTS_WORKER_JOURNAL_CHANGED'
        try:
            observed=datetime.fromisoformat(assessment.get('observed_at'))
            valid_until=datetime.fromisoformat(
                assessment.get('observation_valid_until'))
            assessed=datetime.fromisoformat(assessment.get('assessed_at'))
        except (TypeError,ValueError,OverflowError,AttributeError):
            return False,'CURRENT_ACCOUNT_FACTS_WORKER_STALE'
        if any(item.tzinfo is None for item in
               (observed,valid_until,assessed)):
            return False,'CURRENT_ACCOUNT_FACTS_WORKER_STALE'
        observed=observed.astimezone(timezone.utc)
        valid_until=valid_until.astimezone(timezone.utc)
        assessed=assessed.astimezone(timezone.utc)
        now=now.astimezone(timezone.utc)
        if (observed>valid_until or observed>assessed
                or assessed>valid_until or assessed>now
                or now<observed or now>valid_until):
            return False,'CURRENT_ACCOUNT_FACTS_WORKER_STALE'
        return True,None

    def _account_facts_consume(self,response,*,epoch,epoch_id,purpose,
                               context,publication,now,fresh_cycle=True):
        if (not isinstance(response,dict)
                or response.get('kind')!=
                'current_account_facts_worker_response_v1'):
            self._account_facts_clear_state()
            status=self._account_facts_missing(
                'CURRENT_ACCOUNT_FACTS_WORKER_RESPONSE_INVALID',
                purpose=purpose,context=context,publication=publication,
                now=now,status='observation_blocked',
                worker=self._account_facts_worker_snapshot(),
                epoch_id=epoch_id)
            self.account_facts_status=status
            return status
        if response.get('epoch_id')!=epoch_id:
            self._account_facts_clear_state()
            status=self._account_facts_missing(
                'CURRENT_ACCOUNT_FACTS_WORKER_EPOCH_CHANGED',
                purpose=purpose,context=context,publication=publication,
                now=now,status='observation_blocked',
                worker=self._account_facts_worker_snapshot(),
                epoch_id=epoch_id)
            self.account_facts_status=status
            return status
        if response.get('error'):
            reason=self._account_facts_text(response.get('error')) or 'UNKNOWN'
            self._account_facts_clear_state()
            status=self._account_facts_missing(
                'CURRENT_ACCOUNT_FACTS_WORKER_FAILED:'+reason,
                purpose=purpose,context=context,publication=publication,
                now=now,worker=self._account_facts_worker_snapshot(),
                epoch_id=epoch_id)
            self.account_facts_status=status
            return status
        validation=response.get('validation')
        if (not isinstance(validation,dict)
                or validation.get('valid') is not True):
            reason=self._account_facts_text(
                (validation or {}).get('reason')
                if isinstance(validation,dict) else None) or 'UNKNOWN'
            self._account_facts_clear_state()
            status=self._account_facts_missing(
                'CURRENT_ACCOUNT_FACTS_WORKER_VALIDATION_FAILED:'+reason,
                purpose=purpose,context=context,publication=publication,
                now=now,status='observation_blocked',
                worker=self._account_facts_worker_snapshot(),
                epoch_id=epoch_id)
            self.account_facts_status=status
            return status
        assessment=response.get('assessment')
        valid,reason=self._account_facts_candidate_valid(
            assessment,epoch=epoch,epoch_id=epoch_id,purpose=purpose,
            now=now)
        if not valid:
            self._account_facts_clear_state()
            status=self._account_facts_missing(
                reason,purpose=purpose,context=context,
                publication=publication,now=now,
                status='observation_blocked',
                worker=self._account_facts_worker_snapshot(),
                epoch_id=epoch_id)
            self.account_facts_status=status
            return status
        consumer=response.get('consumer')
        if not isinstance(consumer,dict):
            consumer={'allowed':False,
                      'reason':'ENTRY_AUTHORITY_DIAGNOSTIC_FAILED'}
        status=self._account_facts_diagnostic(
            assessment,purpose=purpose,context=context,consumer=consumer,
            publication=publication,compiled=True,reused=False,now=now,
            worker=self._account_facts_worker_snapshot(),epoch_id=epoch_id,
            fresh_cycle=fresh_cycle)
        if fresh_cycle is not True:
            status['refresh']['source']='owner_wait_progress'
            status['refresh']['reason']='WAIT_PROGRESS_COMPLETED'
            status['freshness']={
                'source':'wait_progress_completed',
                'validation':'worker_validated_and_owner_epoch_checked_during_wait'}
        self._account_facts_last_status=status
        self._account_facts_last_assessment=assessment
        self._account_facts_last_epoch_id=epoch_id
        self._account_facts_last_completed_at=now
        self._account_facts_consumed_count+=1
        self._account_facts_history={
            'status':self._account_facts_text(status.get('status')),
            'reason':self._account_facts_text(status.get('reason')),
            'observed_at':self._account_facts_text(
                status.get('observed_at')),
            'generation':self._account_facts_text(
                status.get('generation')),
            'epoch_id':self._account_facts_text(epoch_id),
            'fresh_cycle':False,
        }
        # The returned status must describe this consumption, not the previous
        # one.  Counters/history were updated above after the diagnostic was
        # built; backfill its refresh view so the first successful wait
        # already reports count=1 and its own last_completed.
        refresh=status.get('refresh')
        if isinstance(refresh,dict):
            refresh['consumed_count']=self._account_facts_consumed_count
            refresh['last_completed']=self._account_facts_history_summary()
        self._account_facts_worker_state='completed'
        self._account_facts_worker_error=None
        self.account_facts_status=status
        return status

    def account_facts_wait_state(self):
        """Cheap wait-loop view; never compiles or starts a child."""
        worker=getattr(self,'account_facts_worker',None)
        if worker is None:
            return {'inflight':False,'deadline_remaining_seconds':None}
        status=self._account_facts_worker_snapshot()
        inflight=status.get('inflight') is True
        remaining=status.get('deadline_remaining_seconds') if inflight else None
        if type(remaining) not in (int,float) or not math.isfinite(remaining):
            remaining=None
        return {'inflight':inflight,'deadline_remaining_seconds':remaining}

    def account_facts_progress(self):
        """Service one in-flight child from an owner wait/progress loop.

        The child was started by a fresh step.  This path only polls, validates
        and consumes a current-epoch candidate; it never dispatches, starts a
        new broker read or treats the historical observation as fresh.
        """
        worker=getattr(self,'account_facts_worker',None)
        if worker is None:
            return {'inflight':False,'changed':False,
                    'deadline_remaining_seconds':None}
        state=self.account_facts_wait_state()
        if state.get('inflight') is not True:
            return {'inflight':False,'changed':False,
                    'deadline_remaining_seconds':None}
        before_starts=self._account_facts_worker_starts
        before_status=self.account_facts_status
        self._refresh_current_account_facts(progress=True)
        after=self.account_facts_wait_state()
        changed=(self._account_facts_worker_starts!=before_starts
                 or self.account_facts_status is not before_status
                 or after.get('inflight') is not True)
        return {'inflight':after.get('inflight') is True,
                'changed':changed,
                'deadline_remaining_seconds':after.get(
                    'deadline_remaining_seconds'),
                'status':self.account_facts_status}

    def account_facts_unrefreshed_status(self):
        """Status for a cycle without a fresh broker step.

        A result completed while waiting is observable, but is explicitly
        marked historical/wait-progress: it is not a new broker collection and
        cannot become an entry authority.  An expired, changed-epoch or
        missing result is not carried forward as green.
        """
        worker_state=self._account_facts_worker_snapshot()
        if worker_state.get('inflight') is True:
            return self._account_facts_missing(
                'CURRENT_ACCOUNT_FACTS_WORKER_INFLIGHT',
                purpose=self._account_facts_purpose(),
                context=self._account_facts_context(),
                now=self._account_facts_now(),
                status='inflight',worker=worker_state,
                epoch_id=self._account_facts_inflight_epoch_id)
        purpose=self._account_facts_purpose()
        context=self._account_facts_context()
        now=self._account_facts_now()
        epoch=self._account_facts_epoch_for(purpose,context)
        epoch_id=digest(epoch) if isinstance(epoch,dict) else None
        last=self._account_facts_last_status
        assessment=self._account_facts_last_assessment
        same_epoch=(isinstance(last,dict)
                    and epoch_id is not None
                    and self._account_facts_last_epoch_id==epoch_id)
        if same_epoch and (assessment is None
                           or self._account_facts_cache_is_current(
                               assessment,now)):
            result=dict(last)
            result['fresh_cycle']=False
            result['wait_progress']=True
            result['worker']=worker_state
            result['refresh']=self._account_facts_refresh_fields(
                now=now,compiled=False,reused=True,
                reason='HISTORICAL_WAIT_PROGRESS',epoch_id=epoch_id)
            result['refresh']['source']='owner_wait_progress'
            result['freshness']={
                'source':'wait_progress_completed',
                'validation':'epoch_and_original_window_checked'}
            return result
        if not isinstance(last,dict):
            reason='CURRENT_ACCOUNT_FACTS_NOT_REFRESHED_IN_CURRENT_CYCLE'
        elif not same_epoch:
            reason='CURRENT_ACCOUNT_FACTS_HISTORICAL_EPOCH_CHANGED'
        else:
            reason='CURRENT_ACCOUNT_FACTS_HISTORICAL_EXPIRED'
        return self._account_facts_missing(
            reason,purpose=purpose,context=context,now=now,
            worker=worker_state,epoch_id=epoch_id)

    def _refresh_current_account_facts(self,*,progress=False):
        """Schedule/poll bounded diagnostic work without compiling on owner.

        The management/entry dispatch has already run when this is called.  The
        owner computes only a cheap epoch and a nonblocking worker state; the
        child compiles and validates the canonical assessment with read-only
        SQLite connections.  A result is consumed only when its epoch and
        original time window still match the owner's current clock.
        """
        purpose=self._account_facts_purpose()
        now=self._account_facts_now()
        context=self._account_facts_context()
        publication=getattr(self,'capture_publication_result',None)
        try:
            if publication is not None and not capture_publication_ok(publication):
                self._account_facts_cancel('CURRENT_CAPTURE_PUBLICATION_FAILED')
                try:
                    self.book.clear_current_account_facts(purpose=purpose)
                except Exception:
                    pass
                status=self._account_facts_missing(
                    'CURRENT_CAPTURE_PUBLICATION_FAILED',purpose=purpose,
                    context=context,publication=publication,now=now,
                    status='observation_blocked',
                    worker=self._account_facts_worker_snapshot(),
                    epoch_id=self._account_facts_last_epoch_id)
                self.account_facts_status=status
                return status
            preflight=getattr(self.book,'_current_preflight',None)
            if (preflight is None
                    or not isinstance(getattr(preflight,'binding_id',None),str)):
                self._account_facts_cancel('CURRENT_CONTEXT_MISSING')
                try:
                    self.book.clear_current_account_facts(purpose=purpose)
                except Exception:
                    pass
                reason=(context.get('reason') or
                        'CURRENT_ACCOUNT_FACTS_READER_MISSING')
                status=self._account_facts_missing(
                    reason,purpose=purpose,context=context,
                    publication=publication,now=now,
                    worker=self._account_facts_worker_snapshot())
                self.account_facts_status=status
                return status
            if context.get('status')=='missing':
                self._account_facts_cancel('CURRENT_CONTEXT_MISSING')
                try:
                    self.book.clear_current_account_facts(purpose=purpose)
                except Exception:
                    pass
                reason=context.get('reason') or 'CURRENT_CONTEXT_MISSING'
                status=self._account_facts_missing(
                    reason,purpose=purpose,context=context,
                    publication=publication,now=now,
                    worker=self._account_facts_worker_snapshot())
                self.account_facts_status=status
                return status
            epoch=self._account_facts_epoch_for(purpose,context)
            if epoch is None:
                self._account_facts_cancel('CURRENT_ACCOUNT_FACTS_EPOCH_UNAVAILABLE')
                status=self._account_facts_missing(
                    'CURRENT_ACCOUNT_FACTS_EPOCH_UNAVAILABLE',
                    purpose=purpose,context=context,publication=publication,
                    now=now,status='observation_blocked',
                    worker=self._account_facts_worker_snapshot())
                self.account_facts_status=status
                return status
            epoch_id=digest(epoch)
            worker=getattr(self,'account_facts_worker',None)
            if worker is None:
                status=self._account_facts_missing(
                    'CURRENT_ACCOUNT_FACTS_WORKER_UNAVAILABLE',
                    purpose=purpose,context=context,publication=publication,
                    now=now,worker=self._account_facts_worker_snapshot(),
                    epoch_id=epoch_id)
                self.account_facts_status=status
                return status
            if getattr(worker,'inflight',False) is True:
                if self._account_facts_inflight_epoch_id!=epoch_id:
                    self._account_facts_cancel(
                        'CURRENT_ACCOUNT_FACTS_WORKER_EPOCH_CHANGED')
                    if getattr(worker,'inflight',False) is not True:
                        return self._account_facts_start(
                            purpose,context,publication,now,epoch,epoch_id)
                    status=self._account_facts_missing(
                        'CURRENT_ACCOUNT_FACTS_WORKER_EPOCH_CHANGED',
                        purpose=purpose,context=context,
                        publication=publication,now=now,
                        status='observation_blocked',
                        worker=self._account_facts_worker_snapshot(),
                        epoch_id=epoch_id)
                    self.account_facts_status=status
                    return status
                try:
                    ready=worker.poll()
                except Exception:
                    self._account_facts_cancel(
                        'CURRENT_ACCOUNT_FACTS_WORKER_POLL_FAILED')
                    status=self._account_facts_missing(
                        'CURRENT_ACCOUNT_FACTS_WORKER_POLL_FAILED',
                        purpose=purpose,context=context,
                        publication=publication,now=now,
                        worker=self._account_facts_worker_snapshot(),
                        epoch_id=epoch_id)
                    self.account_facts_status=status
                    return status
                if ready is None:
                    status=self._account_facts_missing(
                        'CURRENT_ACCOUNT_FACTS_WORKER_INFLIGHT',
                        purpose=purpose,context=context,
                        publication=publication,now=now,
                        status='inflight',
                        worker=self._account_facts_worker_snapshot(),
                        epoch_id=epoch_id)
                    self.account_facts_status=status
                    return status
                kind,value=ready if isinstance(ready,tuple) and len(ready)==2 else ('failed',{})
                self._account_facts_inflight_epoch_id=None
                if kind=='timeout':
                    self._account_facts_clear_state()
                    status=self._account_facts_missing(
                        'CURRENT_ACCOUNT_FACTS_WORKER_TIMEOUT',
                        purpose=purpose,context=context,
                        publication=publication,now=now,
                        status='observation_blocked',
                        worker=self._account_facts_worker_snapshot(),
                        epoch_id=epoch_id)
                    self.account_facts_status=status
                    return status
                if kind!='ok':
                    reason=(value or {}).get('error') if isinstance(value,dict) else None
                    self._account_facts_clear_state()
                    status=self._account_facts_missing(
                        'CURRENT_ACCOUNT_FACTS_WORKER_FAILED:'+
                        (self._account_facts_text(reason) or 'UNKNOWN'),
                        purpose=purpose,context=context,
                        publication=publication,now=now,
                        worker=self._account_facts_worker_snapshot(),
                        epoch_id=epoch_id)
                    self.account_facts_status=status
                    return status
                consume_now=self._account_facts_now() or now
                current_epoch=self._account_facts_epoch_for(
                    purpose,context)
                if (current_epoch is None
                        or digest(current_epoch)!=epoch_id):
                    self._account_facts_clear_state()
                    status=self._account_facts_missing(
                        'CURRENT_ACCOUNT_FACTS_WORKER_EPOCH_CHANGED',
                        purpose=purpose,context=context,
                        publication=publication,now=consume_now,
                        status='observation_blocked',
                        worker=self._account_facts_worker_snapshot(),
                        epoch_id=epoch_id)
                    self.account_facts_status=status
                    return status
                return self._account_facts_consume(
                    value,epoch=current_epoch,epoch_id=epoch_id,
                    purpose=purpose,context=context,
                    publication=publication,now=consume_now,
                    fresh_cycle=not progress)
            reused=self._account_facts_reuse(
                epoch_id=epoch_id,purpose=purpose,context=context,now=now)
            if reused is not None:
                self.account_facts_status=reused
                return reused
            return self._account_facts_start(
                purpose,context,publication,now,epoch,epoch_id)
        except Exception:
            self._account_facts_cancel('CURRENT_ACCOUNT_FACTS_DIAGNOSTIC_FAILED')
            status=self._account_facts_missing(
                'CURRENT_ACCOUNT_FACTS_DIAGNOSTIC_FAILED',purpose=purpose,
                context=context,publication=publication,now=now,
                worker=self._account_facts_worker_snapshot())
            self.account_facts_status=status
            return status

    def close(self):
        """Release the bounded diagnostic child; never touches management."""
        try:
            status=self.account_facts_worker.close()
        except Exception:
            status={'state':'close_failed','inflight':True,
                    'cleanup_pending':True,'read_only_child':True}
        return {'account_facts_worker':status}

    def _dispatch(self,snapshot,quotes,*,allow_entries,allow_dispatch):
        if not allow_dispatch:return {'agent_called':False,'reason':'OUTSIDE_REGULAR_SESSION_READ_ONLY_MANAGEMENT'}
        if self._session_read_only_forbidden():
            return {'agent_called':False,'reason':'READ_ONLY_EXECUTION_CAPABILITY_NONE',
                    'execution_authorized':False,'broker_writes':0,'model_calls':0}
        if self.dispatcher:
            return (self.dispatcher(snapshot=snapshot,quotes=quotes,allow_entries=allow_entries)
                if getattr(self.dispatcher,'requires_observation',False) is True
                else self.dispatcher(allow_entries=allow_entries))
        if self.worker:
            return dispatch_one(self.book,self.worker) if allow_entries else dispatch_one(self.book,self.worker,allow_entries=False)
        return {'agent_called':False,'reason':'WORKER_DISABLED'}

    def advance_strategy(self):
        """Owner-only advisory progress without using an old broker snapshot.

        Errors observed by read-wait polling remain pending for the next fresh
        step. Integrity faults inhibit entries immediately, even during waits.
        """
        if self.instance_supervisor_enabled:
            return self._advance_instances()
        if self.strategy_queue is None:return {'enabled':False,'errors':[]}
        try:
            report=self.strategy_queue.advance()
            if not isinstance(report,dict) or not isinstance(report.get('errors',[]),list):
                raise ValueError('STRATEGY_QUEUE_RESULT_INVALID')
            if any(not isinstance(e,dict) or not isinstance(e.get('reason'),str) or len(e['reason'])>256
                    for e in report.get('errors',[])):
                raise ValueError('STRATEGY_QUEUE_RESULT_INVALID')
        except (ValueError,KeyError,TypeError,OSError,RuntimeError,sqlite3.Error):
            report={'errors':[{'reason':'STRATEGY_QUEUE_ADVANCE_FAILED','scope':'STRATEGY_INPUT'}]}
        errors=report.get('errors',[])
        if any(e['reason'] in {'CONSUMED_BAR_REVISED','STRATEGY_EVENT_ID_CONFLICT',
                'STRATEGY_EVENT_PROVENANCE_REQUIRED'} for e in errors):
            from martin_agentic.watchdog import inhibit_new_entries
            db=self.book.db.execute('PRAGMA database_list').fetchone()[2]
            try:inhibit_new_entries(db,['STRATEGY_INPUT_INTEGRITY'],clock=self.book.clock)
            except OSError:errors.append({'reason':'ENTRY_INHIBIT_PERSISTENCE_FAILED','scope':'STRATEGY_INPUT'})
        self._strategy_pending_errors.extend(errors)
        if len(self._strategy_pending_errors)>256:
            self._strategy_pending_errors=self._strategy_pending_errors[:255]+[
                {'reason':'STRATEGY_ERROR_BUFFER_LIMIT_SEE_DURABLE_QUEUE','scope':'STRATEGY_INPUT'}]
        return {'enabled':True,**report,'undelivered_error_count':len(self._strategy_pending_errors)}

    @property
    def strategy_schedule_enabled(self):
        """True when this owner advances strategy work at all (either shape)."""
        return self.strategy_process_enabled or self.instance_supervisor_enabled

    def strategy_busy(self):
        if self.instance_supervisor_enabled:
            return bool(self.instance_supervisor.strategy_busy())
        if self.strategy_queue is None:
            return False
        return bool(self.strategy_queue.status().get('busy'))

    def strategy_deadline_monotonic(self):
        if self.instance_supervisor_enabled:
            return self.instance_supervisor.next_deadline_monotonic()
        if self.strategy_queue is None:
            return None
        return self.strategy_queue.status().get('next_deadline_monotonic')

    def _advance_instances(self):
        """Advance every registered instance once; one fault never stops the rest."""
        try:
            report=self.instance_supervisor.advance()
        except (ValueError,KeyError,TypeError,OSError,RuntimeError,sqlite3.Error):
            report={'instances':{},'errors':[{'reason':'INSTANCE_SUPERVISOR_ADVANCE_FAILED'}],
                    'pending':0,'busy':False}
        errors=[{'reason':item['reason'],'scope':item.get('scope','STRATEGY_INSTANCE'),
                 'instance_id':item.get('instance_id')}
                for item in report.get('errors',[])
                if isinstance(item,dict) and isinstance(item.get('reason'),str)]
        self._strategy_pending_errors.extend(errors)
        if len(self._strategy_pending_errors)>256:
            self._strategy_pending_errors=self._strategy_pending_errors[:255]+[
                {'reason':'STRATEGY_ERROR_BUFFER_LIMIT_SEE_DURABLE_QUEUE','scope':'STRATEGY_INPUT'}]
        return {'enabled':True,'instances':report.get('instances',{}),
                'status':self.instance_supervisor.status(),
                'pending':report.get('pending',0),'busy':bool(report.get('busy')),
                'errors':errors,'undelivered_error_count':len(self._strategy_pending_errors)}

    def _management_quotes(self,events,errors):
        """Executable market observations, independent of any alpha computation."""
        quotes={}
        for event in events:
            try:
                if not isinstance(event,dict):raise ValueError('STRATEGY_EVENT_OBJECT_REQUIRED')
                if event.get('synthetic_input') is not (self.source_mode=='synthetic') or not event.get('source_sha256'):
                    raise ValueError('STRATEGY_EVENT_PROVENANCE_REQUIRED')
                if not 0<=(self.book.clock()-timestamp(event['at'])).total_seconds()<=30:
                    raise ValueError('STALE_STRATEGY_EVENT')
                if event['kind']=='quote':
                    data=event['data']
                    if (not isinstance(data,dict) or not isinstance(data.get('symbol'),str)
                            or not 0<number(data['bid'])<=number(data['ask'])
                            or not all(0<=(self.book.clock()-timestamp(data[key])).total_seconds()<=15
                                for key in ('bid_at','ask_at','observed_at'))):
                        raise ValueError('STRATEGY_MANAGEMENT_QUOTE_INVALID')
                    quotes[data['symbol']]=data
            except (ValueError,KeyError,TypeError):
                # Bad quotes cannot interrupt protective reconciliation. The
                # queue separately preserves the exact input/rejection record.
                errors.append({'rejected':True,'reason':'STRATEGY_MANAGEMENT_INPUT_INVALID','scope':'STRATEGY_INPUT'})
        return quotes

    def _step_instances(self,snapshot,events,*,allow_entries,allow_dispatch):
        """Advance a group of instances instead of the single legacy queue.

        Protective reconciliation and management dispatch happen first and do
        not depend on any instance succeeding. Each event is offered to every
        enabled instance; a rejection is recorded per instance rather than
        failing the tick.
        """
        errors=[]
        quotes=self._management_quotes(events,errors)
        reconciled=self._reconcile(snapshot,quotes,errors)
        if not reconciled['reconciled']:
            # Reconciliation aborted, so nothing may be released or admitted on
            # this snapshot. Protection state is reported, never assumed.
            return self._unreconciled_step(snapshot,quotes,errors,reconciled)
        receipts=self._import_receipts(snapshot,errors)
        bindings=self._bind_management(snapshot,errors)
        management=any(a['kind']!='entry' and a['state'] in
            {'READY','RUNNING','UNKNOWN','WAITING_USER','REPORTED'} for a in self.book.actions())
        early=self._dispatch(snapshot,quotes,allow_entries=False,allow_dispatch=allow_dispatch) if management else None
        for instance_id in self.instance_supervisor.instance_ids():
            runtime=self.instance_supervisor.runtime(instance_id)
            declared=set(runtime.instance.manifest.data_requirements)
            for event in events:
                required=({'quote':'quotes','bar':'5minute-bars',
                           'universe':'universe-screen'}.get(
                               event.get('kind') if isinstance(event,dict) else None))
                if required is not None and required not in declared:
                    continue
                try:self.instance_supervisor.enqueue(instance_id,event)
                except (ValueError,KeyError,TypeError,OSError,sqlite3.Error) as exc:
                    reason=str(exc) if isinstance(exc,(ValueError,KeyError,TypeError)) else 'INSTANCE_ENQUEUE_FAILED'
                    errors.append({'rejected':True,'reason':reason,'scope':'STRATEGY_INSTANCE',
                                   'instance_id':instance_id})
        progress=self.advance_strategy();errors.extend(self._strategy_pending_errors)
        self._strategy_pending_errors=[]
        self._settle_flat_trades()
        # Shared-account reservation happens only after the same gates that
        # govern any other entry: hold, capture, engineering scope, production
        # admission and accumulated input faults.
        production=self.production.assess(snapshot)
        worker_entries_available=True
        if self.worker_budget is not None and self.worker_budget_entries:
            worker_entries_available=bool(
                self.worker_budget.status().get('entries_available'))
        effective_entries=(allow_entries and not errors
            and worker_entries_available
            and self.book.policy.get('engineering_only') is not True
            and self.book.policy.get('capture_only') is not True
            and (not production['required_for_current_dispatch']
                 or production['automatic_strategy_entry_allowed'])
            and self.production.permits_entry_dispatch(admission=production,
                                                       allow_entries=allow_entries))
        durable_blocked=self._durable_integrity_blocked()
        if (durable_blocked or self._receipts_incomplete or self._bindings_incomplete
                or self.integrity_gate_unavailable):
            # The gate is a durable account fact, not a session flag: a clean
            # snapshot or a restart does not clear it.
            reason=('EXECUTION_INTEGRITY_FAULT_UNRESOLVED'
                    if durable_blocked else
                    self._receipt_incomplete_reason()
                    if self._receipts_incomplete else
                    'MANAGEMENT_BINDING_INCOMPLETE')
            errors.append({'reason':reason,
                           'scope':'STRATEGY_EXECUTION','source':'durable_gate'})
        qualified_entry=(self.book.policy.get('capture_only') is not True
            and self.book.policy.get('engineering_only') is not True
            and (not production['required_for_current_dispatch']
                 or production['automatic_strategy_entry_allowed']))
        admissions=self._admit_pending(allow_entries=effective_entries,
                                       allow_dispatch=allow_dispatch,snapshot=snapshot,quotes=quotes,
                                       qualified_strategy_entry=bool(qualified_entry))
        # Admission and dispatch can discover execution-integrity faults after
        # the earlier drain. They belong in this tick's errors, otherwise the
        # session reports itself healthy while an action has no owner.
        if self._strategy_pending_errors:
            errors.extend(self._strategy_pending_errors)
            self._strategy_pending_errors=[]
        if any(e.get('reason') in {'CONSUMED_BAR_REVISED','STRATEGY_EVENT_ID_CONFLICT',
                'STRATEGY_EVENT_PROVENANCE_REQUIRED'} for e in errors):
            from martin_agentic.watchdog import inhibit_new_entries
            db=self.book.db.execute('PRAGMA database_list').fetchone()[2]
            try:inhibit_new_entries(db,['STRATEGY_INPUT_INTEGRITY'],clock=self.book.clock)
            except OSError:errors.append({'rejected':True,'reason':'ENTRY_INHIBIT_PERSISTENCE_FAILED','scope':'STRATEGY_INPUT'})
        result=self._finish_step(snapshot,quotes,errors,allow_entries=allow_entries,
            allow_dispatch=allow_dispatch,reconciled=True,early_dispatch=early)
        result['strategy_queue']=progress;result['strategy_admissions']=admissions
        result['strategy_receipts']=receipts;result['strategy_bindings']=bindings
        if (durable_blocked or self._receipts_incomplete or self._bindings_incomplete
                or self.integrity_gate_unavailable):
            result['reconciliation_pending']=True
            result['settlement_blocked']='RECEIPT_IMPORT_INCOMPLETE'
            result['new_entries_blocked']=True
            result['integrity_inhibit_persisted']=self._gate_inhibit_persisted
            result.update(self._integrity_fields())
            if self.integrity_gate_unavailable:
                result['stop_disposition']=('EXECUTION_INTEGRITY_FAULT_LEDGER_UNAVAILABLE_'
                                            'MANUAL_HANDOFF_REQUIRED')
        # The gate is the conjunction of every reason entries are held, not
        # merely "errors is non-empty": entries can be blocked with no errors at
        # all (hold, capture, production admission), and an error list must not
        # overwrite that answer.
        blockers=[]
        if allow_entries is not True:blockers.append('ENTRIES_NOT_PERMITTED_BY_CALLER')
        if self.book.policy.get('capture_only') is True:blockers.append('CAPTURE_ONLY')
        if self.book.policy.get('engineering_only') is True:blockers.append('ENGINEERING_ONLY_POLICY')
        if not (not production['required_for_current_dispatch']
                or production['automatic_strategy_entry_allowed']):
            blockers.append('PRODUCTION_ADMISSION_BLOCKS_ENTRIES')
        if not worker_entries_available:
            blockers.append('WORKER_ENTRY_BUDGET_EXHAUSTED')
        if errors:blockers.append('INPUT_OR_EXECUTION_ERRORS')
        result['new_entries_blocked']=bool(blockers) or bool(result.get('new_entries_blocked'))
        result['new_entries_blockers']=blockers
        return result

    def _admit_pending(self, *, allow_entries, allow_dispatch=False, snapshot=None, quotes=None,
                       qualified_strategy_entry=False):
        """Turn instance advice into account-checked execution intents.

        Advice only becomes an intent here, after the account authority has
        agreed to the money and the symbol. A refusal is reported per proposal
        and never silently dropped, and an admitted proposal is claimed in the
        instance's own outbox so it cannot be admitted twice.

        Admission is a shared-account action, so it obeys the same gates as any
        other entry. When entries are held, capturing or blocked by an input
        fault, nothing is reserved and no proposal is consumed.
        """
        if self.admission is None or not self.instance_supervisor_enabled:
            return {'enabled':False,'outcomes':[]}
        if self._session_read_only_forbidden():
            # Read-only shadow can expose advice but cannot create a new
            # executable intent/action or resolve an old one by dispatch.
            blocked=[]
            for instance_id in self.instance_supervisor.instance_ids():
                runtime=self.instance_supervisor.runtime(instance_id)
                for summary in runtime.host.pending_proposals():
                    blocked.append({'admitted':False,'instance_id':instance_id,
                        'proposal_id':summary['proposal_id'],
                        'reason':'READ_ONLY_EXECUTION_CAPABILITY_NONE'})
            for intent in (self.admission.pending_intents()
                           + self.admission.dispatching_intents()):
                blocked.append({'admitted':False,
                    'instance_id':intent.get('instance_id'),
                    'intent_id':intent.get('intent_id'),
                    'reason':'READ_ONLY_EXECUTION_CAPABILITY_NONE'})
            return {'enabled':True,'outcomes':blocked,'blocked':blocked,
                    'reason':'READ_ONLY_EXECUTION_CAPABILITY_NONE',
                    'expired':[]}
        # Recovery runs whatever the entry gate says: an intent stuck mid
        # dispatch is holding account money, and leaving it there because new
        # entries are paused is how a reservation becomes permanent.
        recovered=self._recover_dispatching()
        if allow_entries is not True or self.book.policy.get('capture_only') is True:
            return {'enabled':True,'outcomes':recovered,'released':False,
                    'reason':'ADMISSION_ENTRIES_NOT_PERMITTED'}
        outcomes=list(recovered)
        for instance_id in self.instance_supervisor.instance_ids():
            runtime=self.instance_supervisor.runtime(instance_id)
            host=runtime.host
            # The registry is the authority, not the runtime's snapshot: an
            # instance disabled after this session started must not admit.
            current=self.instance_supervisor.registry.instance(instance_id) if hasattr(self.instance_supervisor,'registry') else runtime.instance
            for summary in host.pending_proposals():
                try:
                    detail=host.proposal_detail(summary['proposal_id'])
                    outcome=self.admission.admit(current,detail['proposal'],
                        trade_id=detail['trade_id'],allow_entries=True,
                        qualified_strategy_entry=qualified_strategy_entry)
                except (ValueError,KeyError,TypeError,OSError,sqlite3.Error) as exc:
                    outcomes.append({'admitted':False,'instance_id':instance_id,
                        'proposal_id':summary['proposal_id'],
                        'reason':str(exc) if isinstance(exc,(ValueError,KeyError,TypeError)) else 'ADMISSION_FAILED'})
                    continue
                if outcome.get('admitted'):
                    host.claim_proposal(summary['proposal_id'])
                    if allow_dispatch:
                        outcome=dict(outcome,**self._dispatch_intent(outcome,snapshot,quotes))
                outcomes.append(outcome)
        outcomes.extend(self._drain_pending_intents(allow_dispatch,snapshot,quotes))
        for item in outcomes:
            if item.get('action_id') and item.get('execution_ref_bound') is False:
                # An action exists but has no owner. It cannot be routed when a
                # receipt arrives, so it is reported as an execution fault and
                # blocks further entries instead of only appearing in the
                # per-instance outcome list.
                self._strategy_pending_errors.append(
                    {'reason':'EXECUTION_REFERENCE_UNBOUND','scope':'STRATEGY_EXECUTION',
                     'instance_id':item.get('instance_id'),
                     'action_id':item.get('action_id')})
        return {'enabled':True,'outcomes':outcomes,'expired':self._expire_intents()}

    def exit_features(self):
        """Per-symbol exit features, routed through each trade's real owner."""
        if not self.instance_supervisor_enabled:
            return self.exit_features_hook(self.signals.state())
        return self._exit_features_by_owner()

    def _exit_features_by_owner(self):
        """Read each active trade's features from the instance that owns it.

        Two instances may watch the same symbol with different checkpoints and
        configurations, so a single shared state cannot answer for both. A trade
        with no attribution gets no features at all: the book still applies its
        own structural safety rules, but nothing is borrowed from another
        instance or from the legacy state.
        """
        reader=getattr(self.book,'state',None)
        if not callable(reader):
            # A book that cannot report its trades has nothing to route; the
            # caller still gets the book's own structural safety rules.
            self.exit_feature_diagnostics={'unresolved_trades':[],
                                           'symbols_without_owner':[],
                                           'trades_unavailable':True}
            return {}
        attribution=getattr(self.admission,'attribution',None)
        resolver=getattr(attribution,'owner_for_trade',None)
        runtimes={instance_id:self.instance_supervisor.runtime(instance_id)
                  for instance_id in self.instance_supervisor.instance_ids()}
        claimed={};unresolved=[]
        for trade_id,trade in reader()['trades'].items():
            if trade.get('phase')=='FLAT_VERIFIED':continue
            owner=resolver(trade_id) if callable(resolver) else None
            plan=trade.get('plan') or {}
            symbol=plan.get('symbol')
            if owner is None or symbol is None or owner['instance_id'] not in runtimes:
                unresolved.append(trade_id)
                if symbol is not None:claimed[symbol]=None
                continue
            prior=claimed.get(symbol,'__unset__')
            if prior=='__unset__':
                claimed[symbol]=owner['instance_id']
            elif prior!=owner['instance_id']:
                # Two trades claim one symbol from different instances. Picking
                # either would feed one instance's features to the other, so the
                # symbol gets none and the conflict is reported.
                claimed[symbol]=None
                conflicts=locals().get('conflicts')
                if conflicts is None:conflicts=[]
                conflicts.append({'symbol':symbol,'instance_ids':sorted(
                    {prior,owner['instance_id']})})
                self._exit_feature_conflicts=conflicts
        features={}
        for symbol,instance_id in claimed.items():
            if instance_id is None:continue
            runtime=runtimes[instance_id]
            plugin_id=runtime.instance.manifest.plugin_id
            keys=self.feature_keys_for(plugin_id)
            state=runtime.host.state()
            value={}
            for key in keys:
                stored=state.get(key)
                from collections.abc import Mapping
                if isinstance(stored,Mapping) and symbol in stored:
                    value[key]=stored[symbol]
                elif stored is not None and not isinstance(stored,Mapping):
                    value[key]=stored
            if value:features[symbol]=value
        self.exit_feature_diagnostics={'unresolved_trades':unresolved,
            'symbols_without_owner':sorted(s for s,v in claimed.items() if v is None),
            'ownership_conflicts':getattr(self,'_exit_feature_conflicts',[])}
        return features

    def _entry_reference_bound(self,action):
        try:payload=json.loads(action.get('payload') or '{}')
        except ValueError:return False
        ref=payload.get('ref_id')
        if not isinstance(ref,str) or not ref:return False
        attribution=getattr(self.admission,'attribution',None)
        owner=getattr(attribution,'owner',None)
        if not callable(owner):
            return False
        owner_row=owner(ref)
        if owner_row is None:
            return False
        registry=getattr(self.instance_supervisor,'registry',None)
        if registry is None:
            return False
        current=registry.find(owner_row.get('instance_id'))
        if current is None or not current.enabled or current.mode!='shadow':
            return False
        return True

    def _exit_advice_views(self,snapshot,quotes):
        """Build one read-only position view per owned, open trade."""
        views=[]
        try:
            state=self.book.state()
        except (ValueError,KeyError,TypeError,OSError,sqlite3.Error):
            return views
        attribution=getattr(self.admission,'attribution',None)
        resolver=getattr(attribution,'owner_for_trade',None)
        registry=getattr(self.instance_supervisor,'registry',None)
        for trade_id,trade in (state.get('trades') or {}).items():
            if trade.get('phase')=='FLAT_VERIFIED':continue
            symbol=(trade.get('plan') or {}).get('symbol')
            quote=quotes.get(symbol)
            if not symbol or quote is None:continue
            quantity=str(trade.get('quantity') or '0')
            if number(quantity)==0:continue
            owner=resolver(trade_id) if callable(resolver) else None
            if owner is None:continue
            instance_id=owner['instance_id']
            instance=None
            if registry is not None and hasattr(registry,'instance'):
                instance=registry.instance(instance_id)
            plugin_id=instance.manifest.plugin_id if instance is not None else None
            if not plugin_id:continue
            keys=self.feature_keys_for(plugin_id)
            owner_state={}
            try:
                if instance is not None and self.instance_supervisor_enabled:
                    owner_state=self.instance_supervisor.runtime(instance_id).host.state()
            except (ValueError,KeyError,TypeError,OSError,sqlite3.Error):
                owner_state={}
            features={}
            for key in keys:
                stored=owner_state.get(key)
                from collections.abc import Mapping
                if isinstance(stored,Mapping) and symbol in stored:
                    features[key]=stored[symbol]
                elif stored is not None and not isinstance(stored,Mapping):
                    features[key]=stored
            views.append({'instance_id':instance_id,'plugin_id':plugin_id,
                'trade_id':trade_id,'symbol':symbol,'quantity':quantity,
                'entry_price':str(trade.get('entry_price') or ''),
                'stop':str(trade.get('stop') or ''),
                'initial_stop':str(trade.get('initial_stop') or ''),
                'quote':dict(quote),'features':features,
                'as_of':(quote.get('observed_at') or snapshot.get('observed_at')),
                'position_version':self.book.exit_position_version(trade)})
        return views

    def _exit_advices(self,snapshot,quotes):
        """Harvest finished advice, then submit this tick's views.

        Nothing here waits for a plugin: the owner reconciles with whatever
        arrived and protective management continues on the same tick. A slow
        advisor's result is used on a later tick and re-validated there.
        """
        advisor=getattr(self,'exit_advisor',None)
        if advisor is None:return None
        self._exit_advice_errors=[]
        advices={}
        try:
            arrived=advisor.collect() if callable(getattr(advisor,'collect',None)) else {}
        except (ValueError,KeyError,TypeError,OSError,RuntimeError):
            arrived={}
        for trade_id,advice in (arrived or {}).items():
            if isinstance(advice,dict) and advice.get('error'):
                error={'trade_id':trade_id,'reason':advice['error']}
                self._exit_advice_errors.append(error)
                self._exit_advice_history=(self._exit_advice_history+[error])[-256:]
                continue
            advices[trade_id]=advice
        views=self._exit_advice_views(snapshot,quotes)
        try:
            if callable(getattr(advisor,'submit',None)):
                advisor.submit(views)
        except (ValueError,KeyError,TypeError,OSError,RuntimeError):
            self._exit_advice_errors.append({'reason':'EXIT_ADVISOR_SUBMIT_FAILED'})
        return advices

    def _reconcile(self,snapshot,quotes,errors):
        """Reconcile, turning a known execution-identity conflict into evidence.

        ``LiveRiskGuard`` compares execution ids before the importer runs and
        raises when one id reappears with different content. That is exactly the
        conflict the receipt ledger must record, so it is captured here instead
        of escaping as an uncaught daemon exception.
        """
        advices=self._exit_advices(snapshot,quotes)
        try:
            self.book.reconcile(snapshot,quotes=quotes,features=self.exit_features(),
                                exit_advices=advices)
        except ValueError as exc:
            reason=str(exc)
            if reason!='LIVE_RISK_EXECUTION_CONFLICT':
                raise
            self._receipts_incomplete=True
            self._conflict_seen=True
            # The durable fault is written before this tick returns, whether or
            # not a receipt importer exists. Without it the gate would be memory
            # only and a restart with a clean snapshot would reopen risk.
            fault=self._record_integrity_fault(snapshot)
            try:
                recorded=self._record_risk_conflict(snapshot)
            except (ValueError,KeyError,TypeError,OSError,sqlite3.Error):
                # The audit could not be persisted. That is reported as its own
                # explicit fault and the tick still fails closed; a conflict
                # that cannot be recorded must never look like a clean tick.
                errors.append({'reason':'RECEIPT_CONFLICT_AUDIT_FAILED',
                               'scope':'STRATEGY_EXECUTION','source':'risk_guard'})
                return {'reconciled':False,'needs_reconciliation':True,
                        'reason':reason,'conflicts':0,'audit_failed':True,
                        'integrity_fault':fault}
            errors.append({'reason':'RECEIPT_EXECUTION_IDENTITY_CONFLICT',
                           'scope':'STRATEGY_EXECUTION','source':'risk_guard',
                           'conflicts':recorded})
            return {'reconciled':False,'needs_reconciliation':True,
                    'reason':reason,'conflicts':recorded,'integrity_fault':fault}
        except (sqlite3.Error,OSError):
            # The book could not reconcile at all: no safe management decision
            # can be made from this snapshot, and the tick must say so rather
            # than let the daemon crash with unknown protection state.
            self._receipts_incomplete=True
            # The account's execution facts could not be proven on this tick.
            # Persist that as a fault, not merely a tick label: a restart with a
            # clean snapshot must not be able to call the account healthy.
            fault=self._record_integrity_fault(
                snapshot,source='reconciliation_storage_failed')
            errors.append({'reason':'RECONCILIATION_STORAGE_FAILED',
                           'scope':'STRATEGY_EXECUTION','source':'execution_book',
                           'integrity_fault':fault})
            return {'reconciled':False,'needs_reconciliation':True,
                    'reason':'RECONCILIATION_STORAGE_FAILED','conflicts':0,
                    'integrity_fault':fault}
        return {'reconciled':True}

    def _integrity_state(self):
        """Read the one authority: ledger rows plus the sticky side-car.

        A fault that could not be written to the database still exists as
        ``EXECUTION_INTEGRITY_FAULT_UNRECORDED`` in ``<db>.entry-inhibit.json``.
        It marks unknown execution facts, not an ordinary operator hold, so a
        later session reads it as an unresolved integrity block rather than
        looking healthy because the ledger itself is now writable.
        """
        from trading_infra.integrity_gate import read_integrity
        state=read_integrity(self.book.db,
            db_path=getattr(self.book,'db_path',None),
            clock=getattr(self.book,'clock',None))
        if not state['known']:
            self.integrity_gate_unavailable=True
        return state

    def _fault_count(self):
        state=self._integrity_state()
        return len(state['faults']) if state['known'] else None

    def _integrity_fields(self):
        state=self._integrity_state()
        return {'integrity_faults_unresolved':(len(state['faults'])
                    if state['known'] else None),
                'integrity_gate_unavailable':self.integrity_gate_unavailable,
                'integrity_reasons':state['reasons'],
                'integrity_observed_at':state['observed_at'],
                'inhibit_other_issues':state['other_inhibit_issues']}

    def _record_integrity_fault(self,snapshot,*,source='execution_identity_conflict'):
        """Persist the conflict as an account-level fault, or fail closed.

        The typed ledger is the first writer. If it fails, a minimal row is
        still written in the same account database: the point of the record is
        that a later session sees it, and a convenience wrapper's failure is not
        evidence that the conflict did not happen. Only when neither write
        succeeds does this session declare itself unable to know the account's
        state, which is a fatal manual handoff rather than an ordinary conflict.
        """
        state=snapshot if isinstance(snapshot,dict) else {}
        account_key=state.get('account_key')
        execution_id=self._first_conflicting_execution(state)
        evidence={'account_mask':state.get('account_mask'),
                  'observed_at':state.get('observed_at'),
                  'orders':sorted(state.get('orders') or {})}
        try:
            if self.integrity_faults is None:
                raise ValueError('INTEGRITY_FAULT_LEDGER_UNAVAILABLE')
            return self.integrity_faults.record(
                source=source,provider='robinhood',account_key=account_key,
                execution_id=execution_id,evidence=evidence,
                observed_at=state.get('observed_at'))
        except (ValueError,KeyError,TypeError,OSError,sqlite3.Error):
            return self._fallback_integrity_fault(
                source=source,account_key=account_key,
                execution_id=execution_id,evidence=evidence,
                observed_at=state.get('observed_at'))

    def _fallback_integrity_fault(self,*,source,account_key,execution_id,evidence,
                                  observed_at=None):
        """Write the same authoritative row with the fewest dependencies."""
        try:
            import json as _json
            from trading_infra.integrity_faults import ExecutionIntegrityFaults,SCHEMA
            from trading_infra.transactions import atomic
            fault_id=ExecutionIntegrityFaults.fault_id(
                account_key=account_key,provider='robinhood',source=source,
                execution_id=execution_id)
            with atomic(self.book.db):
                self.book.db.execute(SCHEMA.strip().rstrip(';'))
                self.book.db.execute(
                    'INSERT OR IGNORE INTO execution_integrity_faults(fault_id,'
                    ' account_key,provider,source,execution_id,evidence,occurred_at,'
                    ' observed_at,first_seen_at,state) VALUES(?,?,?,?,?,?,?,?,?,?)',
                    (fault_id,account_key,'robinhood',source,execution_id,
                     _json.dumps(evidence,default=str,sort_keys=True,ensure_ascii=False),
                     observed_at,observed_at or self.book.clock().isoformat(),
                     self.book.clock().isoformat(),'UNRESOLVED'))
            return fault_id
        except (ValueError,KeyError,TypeError,OSError,sqlite3.Error):
            # Nothing durable could be written. Say so and persist the sticky
            # entry inhibit, so a restart cannot treat the account as healthy.
            self.integrity_gate_unavailable=True
            self._persist_gate_inhibit()
            return None

    def _persist_gate_inhibit(self):
        """Best-effort sticky entry inhibit when the fault ledger is unusable."""
        try:
            from martin_agentic.watchdog import inhibit_new_entries
            # Prefer the path the book was opened with: a broken connection can
            # make PRAGMA fail, and the side-car gate must still be written.
            path=getattr(self.book,'db_path',None)
            if not path:
                path=self.book.db.execute('PRAGMA database_list').fetchone()[2]
            if not path:
                return False
            inhibit_new_entries(path,['EXECUTION_INTEGRITY_FAULT_UNRECORDED'],
                                clock=self.book.clock)
            self._gate_inhibit_persisted=True
            return True
        except (ValueError,KeyError,TypeError,OSError,sqlite3.Error):
            self._gate_inhibit_persisted=False
            return False

    @staticmethod
    def _first_conflicting_execution(snapshot):
        seen={}
        for order in (snapshot.get('orders') or {}).values():
            for fill in order.get('executions') or ():
                fingerprint=digest(order.get('id'))
                prior=seen.get(fill.get('id'))
                if prior is not None:
                    return fill.get('id')
                seen[fill.get('id')]=fingerprint
        return None

    def _durable_integrity_blocked(self):
        # Ledger rows and the unrecorded-fault side-car are one authority. An
        # unreadable witness closes the gate rather than opening it.
        return self._integrity_state()['blocked']

    def _record_risk_conflict(self,snapshot):
        importer=self.receipt_importer
        recorder=getattr(importer,'record_risk_conflict',None)
        if not callable(recorder):return 0
        # The risk ledger knows the disputed execution only as a hash; passing
        # those ids is what lets the importer record a dispute even when no
        # receipt binding exists yet.
        prior=set()
        try:
            prior={str(key) for key in (self.book.risk_guard.state() or {}).get('executions',{})}
        except (ValueError,KeyError,TypeError,OSError,sqlite3.Error):
            prior=set()
        return len(recorder(snapshot,prior_execution_ids=prior))

    def _unreconciled_step(self,snapshot,quotes,errors,reconciled,*,dispatched=False):
        """Report an unreconciled tick from facts, never from a promise.

        Reconciliation aborted, so this tick placed no management action. What
        the book still holds — open actions, the last snapshot it accepted, the
        positions the broker reported — is reported as it is; nothing claims
        that management continued, and nothing claims the account is flat.
        """
        evidence_available=True
        try:
            state=self.book.state() or {}
            actions=[{'kind':action['kind'],'state':action['state']}
                     for action in self.book.actions()
                     if action.get('kind')!='entry']
        except (ValueError,KeyError,TypeError,OSError,sqlite3.Error):
            # The book itself cannot be read. Report that the evidence is
            # unavailable; never present it as zero positions or no actions.
            state={};actions=[];evidence_available=False
        active=[action for action in actions
                if action['state'] in {'READY','RUNNING','UNKNOWN','WAITING_USER','REPORTED'}]
        positions=len(snapshot.get('positions') or {}) if isinstance(snapshot,dict) else None
        result={'cycle':None,'reconciled':False,
                'needs_reconciliation':True,'reconciliation_reason':reconciled.get('reason'),
                'conflict_audit_failed':bool(reconciled.get('audit_failed')),
                'strategy_receipts':{'enabled':self.receipt_importer is not None},
                'strategy_bindings':{'enabled':False},'errors':errors,
                'new_entries_blocked':True,
                'new_entries_blockers':['EXECUTION_IDENTITY_CONFLICT'],
                'management_dispatched':bool(dispatched),
                'management_continues':False,
                'management_evidence_available':evidence_available,
                'management_state':{'protection_actions':len(actions) if evidence_available else None,
                                    'active_protection_actions':len(active) if evidence_available else None,
                                    'actions':actions if evidence_available else None,
                                    'positions_reported':positions,
                                    'last_accepted_snapshot':state.get('last_snapshot')},
                'protection_status':('EVIDENCE_UNAVAILABLE' if not evidence_available else
                                     'ACTIVE_UNVERIFIED' if active else
                                     'NONE_DISPATCHED_THIS_TICK'),
                'settlement_blocked':'RECEIPT_IMPORT_INCOMPLETE',
                'integrity_inhibit_persisted':self._gate_inhibit_persisted,
                **self._integrity_fields(),
                'strategy_exit_advice_errors':list(self._exit_advice_errors),
                'strategy_exit_advice_history':list(self._exit_advice_history),
                'flat_claimed':False,'healthy':False,'attention_required':True}
        if self.integrity_gate_unavailable:
            # No durable fault and no working ledger: this process cannot prove
            # the account is safe, and a restart must not silently resume.
            result['stop_disposition']=('EXECUTION_INTEGRITY_FAULT_LEDGER_UNAVAILABLE_'
                                        'MANUAL_HANDOFF_REQUIRED')
        purpose=self._account_facts_purpose()
        self._account_facts_cancel('CURRENT_RECONCILIATION_FAILED')
        try:
            self.book.clear_current_account_facts(purpose=purpose)
        except Exception:
            pass
        status=self._account_facts_missing(
            'CURRENT_RECONCILIATION_FAILED',purpose=purpose,
            context=self._account_facts_context(),
            now=self._account_facts_now(),status='observation_blocked',
            worker=self._account_facts_worker_snapshot())
        self.account_facts_status=status
        result['account_facts']=status
        return result

    def _bind_management(self,snapshot,errors):
        """Attach protect/cancel/exit identities to their instance, and report.

        Runs after reconciliation so the actions and broker orders it reads are
        the ones the book just accepted. A storage failure is an execution
        integrity fault: it blocks new entries and settlement, and is retried on
        the next tick rather than being reported as success.
        """
        if self.admission is None or self.receipt_importer is None:
            self._bindings_incomplete=False
            return {'enabled':False}
        attribution=getattr(self.admission,'attribution',None)
        account=(self.book.state() or {}).get('account_key')
        observed=(self.book.state() or {}).get('last_snapshot')
        try:
            from martin_agentic.management_binding import bind_management_actions, record_lifecycle_feedback
            bound=bind_management_actions(self.book,attribution=attribution,
                importer=self.receipt_importer,account_key=account)
            feedback={'recorded':[]}
            if isinstance(observed,str) and observed:
                feedback=record_lifecycle_feedback(self.book,attribution=attribution,
                    account_key=account,observed_at=observed)
        except (ValueError,KeyError,TypeError,OSError,sqlite3.Error) as exc:
            reason=(str(exc) if isinstance(exc,(ValueError,KeyError,TypeError))
                    else 'MANAGEMENT_BINDING_FAILED')
            errors.append({'reason':reason,'scope':'STRATEGY_EXECUTION'})
            self._bindings_incomplete=True
            return {'enabled':True,'failed':True,'reason':reason}
        self._bindings_incomplete=bool(bound['failures'])
        if bound['failures']:
            errors.append({'reason':'MANAGEMENT_REFERENCE_CONFLICT',
                           'scope':'STRATEGY_EXECUTION','count':len(bound['failures'])})
        return {'enabled':True,'bound':len(bound['bound']),'aliases':len(bound['aliases']),
                'unowned_actions':len(bound['unowned_actions']),
                'failures':len(bound['failures']),
                'lifecycle':len(feedback['recorded'])}

    def _import_receipts(self,snapshot,errors):
        """Route the reconciled broker facts to their owning instance.

        Runs after reconciliation so it sees the same snapshot the book used. A
        failure here is an execution-integrity fault: it is reported and blocks
        new entries, while protective management of existing positions continues
        independently.
        """
        if self.receipt_importer is None:
            if not self._conflict_seen:
                self._receipts_incomplete=False
            return {'enabled':False}
        try:
            imported=self.receipt_importer.import_snapshot(snapshot)
        except (ValueError,KeyError,TypeError,OSError,sqlite3.Error) as exc:
            reason=(str(exc) if isinstance(exc,(ValueError,KeyError,TypeError))
                    else 'RECEIPT_IMPORT_FAILED')
            errors.append({'reason':reason,'scope':'STRATEGY_EXECUTION'})
            # A failed import means the account's execution facts are not known
            # for this tick, so nothing may be released on the strength of it.
            self._receipts_incomplete=True
            return {'enabled':True,'failed':True,'reason':reason}
        # Anything still unresolved keeps the account's execution facts
        # incomplete: a quarantined order, a sale still waiting for its basis,
        # or anything left pending from an earlier snapshot.
        deferred=len(imported.get('deferred') or ())
        pending=self._pending_receipts()
        # A quarantined execution identity from the legacy migration is just as
        # unresolved as a live one: the account's execution facts are not known.
        identity=len(self._identity_conflicts())
        unresolved=(len(imported.get('quarantined') or ())+deferred+pending+identity)
        self._receipts_incomplete=bool(unresolved)
        if imported['quarantined']:
            reasons={}
            for item in imported['quarantined']:
                reason=item.get('reason') if isinstance(item,dict) else None
                if not isinstance(reason,str) or not reason:
                    reason='RECEIPT_OWNER_UNKNOWN'
                reasons[reason]=reasons.get(reason,0)+1
            for reason,count in sorted(reasons.items()):
                errors.append({'reason':reason,'scope':'STRATEGY_EXECUTION',
                               'count':count})
        if identity:
            errors.append({'reason':'RECEIPT_IDENTITY_CONFLICT',
                           'scope':'STRATEGY_EXECUTION','count':identity})
        if deferred or pending:
            # An execution whose basis has not arrived is unresolved, not
            # finished: the account's cash and position facts are incomplete, so
            # new risk must wait even though protective management continues.
            errors.append({'reason':'RECEIPT_AWAITING_BASIS',
                           'scope':'STRATEGY_EXECUTION','count':deferred+pending})
        return {'enabled':True,**{key:len(value) for key,value in imported.items()
                                  if isinstance(value,list)}}

    def _identity_conflicts(self):
        conflicts=getattr(self.receipt_importer,'identity_conflicts',None)
        return conflicts() if callable(conflicts) else []

    def _receipt_incomplete_reason(self):
        """Distinguish an actual identity conflict from other incomplete facts."""
        return ('RECEIPT_IDENTITY_CONFLICT' if self._identity_conflicts()
                else 'RECEIPT_FACTS_INCOMPLETE')

    def _pending_receipts(self):
        counter=getattr(self.receipt_importer,'pending_execution_count',None)
        return counter() if callable(counter) else 0

    def _entry_action_for(self,trade_id):
        for action in self.book.actions():
            if action.get('kind')!='entry' or action.get('trade_id')!=trade_id:continue
            return action
        return None

    def _recover_dispatching(self):
        """Resolve intents left mid-dispatch by a crash or a rejection.

        The book is the authority: no entry action for the trade means the
        propose never landed, so the intent can be terminated and released. An
        entry action means it did land, so the intent is adopted, its real
        reference bound, and no second order is ever proposed.
        """
        lister=getattr(self.admission,'dispatching_intents',None)
        if not callable(lister):return []
        outcomes=[]
        for intent in lister():
            try:
                action=self._entry_action_for(intent['trade_id'])
            except (ValueError,KeyError,TypeError,OSError,sqlite3.Error):
                # "Cannot read the book" is not "no action exists". Abandoning
                # here would release money and a symbol while an order may be
                # live. Keep the intent, the reservation and the lease, and
                # report the isolation instead of guessing.
                outcomes.append({**intent,'recovered':'ACTION_AUTHORITY_UNAVAILABLE',
                                 'dispatched':False,'quarantined':True,
                                 'error':'RECOVERY_ACTION_AUTHORITY_UNAVAILABLE'})
                self._strategy_pending_errors.append(
                    {'reason':'RECOVERY_ACTION_AUTHORITY_UNAVAILABLE',
                     'scope':'STRATEGY_EXECUTION','intent_id':intent['intent_id']})
                continue
            if action is None:
                result=self.admission.abandon_intent(intent['intent_id'])
                outcomes.append({**intent,**result,'recovered':'ABANDONED'})
                continue
            # Bind before declaring anything dispatched. A failed bind keeps the
            # intent in DISPATCHING so this recovery runs again next tick.
            bound=self._bind_execution_reference(intent,action['id'])
            if not bound.get('execution_ref_bound'):
                outcomes.append({**intent,**bound,'dispatched':False,
                                 'action_id':action['id'],'recovered':'BIND_FAILED',
                                 'error':'EXECUTION_REFERENCE_UNBOUND'})
                continue
            mark=getattr(self.admission,'mark_intent',None)
            if callable(mark):mark(intent['intent_id'],'DISPATCHED')
            outcomes.append({**intent,**bound,'dispatched':True,
                             'action_id':action['id'],'recovered':'ADOPTED'})
        return outcomes

    def _drain_pending_intents(self,allow_dispatch,snapshot,quotes):
        """Dispatch intents admitted on an earlier tick.

        Admission and dispatch are separate steps because a session may be
        allowed to reserve now and to send later. Without this drain, an intent
        admitted while dispatch was held would sit PENDING forever and its
        reservation would be invisible to every later tick.
        """
        if not allow_dispatch:return []
        outcomes=[]
        for intent in self.admission.pending_intents():
            # The registry is re-read here too: an instance disabled after its
            # intent was admitted must not create new risk from the queue.
            current=None
            if hasattr(self.instance_supervisor,'registry') and self.instance_supervisor.registry is not None:
                current=self.instance_supervisor.registry.find(intent['instance_id'])
            if current is None or not current.enabled or current.mode=='disabled':
                outcomes.append({**intent,'dispatched':False,
                                 'reason':'ADMISSION_INSTANCE_DISABLED'})
                continue
            if current.mode=='read_only_shadow':
                outcomes.append({**intent,'dispatched':False,
                                 'reason':'READ_ONLY_EXECUTION_CAPABILITY_NONE'})
                continue
            outcome=dict(intent,admitted=True,
                cost_buffer_per_share=intent['plan'].get('cost_buffer_per_share','0'))
            mark=dict(self._dispatch_intent(outcome,snapshot,quotes))
            outcomes.append({**outcome,**mark})
        return outcomes

    def _expire_intents(self):
        expire=getattr(self.admission,'expire_intents',None)
        return expire() if callable(expire) else []

    def _settle_flat_trades(self):
        """Release money and symbols for trades the book has verified closed.

        Evidence comes from the execution authority, not from this loop's
        opinion: the book's own phase, its remaining quantity and the absence of
        open orders. Without that, a symbol could be handed to another instance
        while the account still held it.
        """
        if self.admission is None:return {'enabled':False,'settled':[]}
        if self._receipts_incomplete:return {'enabled':True,'settled':[],
                                             'blocked':'RECEIPT_IMPORT_INCOMPLETE'}
        if self._bindings_incomplete:return {'enabled':True,'settled':[],
                                             'blocked':'MANAGEMENT_BINDING_INCOMPLETE'}
        if self._durable_integrity_blocked():return {'enabled':True,'settled':[],
                                                     'blocked':'EXECUTION_INTEGRITY_FAULT_UNRESOLVED'}
        read=getattr(self.book,'state',None)
        if not callable(read):return {'enabled':False,'settled':[]}
        state=read();settled=[]
        for trade_id,trade in state['trades'].items():
            if trade.get('phase')!='FLAT_VERIFIED':continue
            instance_id=trade.get('instance_id')
            if instance_id is None:
                attribution=getattr(self.admission,'attribution',None)
                owner=attribution.owner_for_trade(trade_id) if hasattr(attribution,'owner_for_trade') else None
                instance_id=owner['instance_id'] if owner else None
            if instance_id is None:continue
            # Facts, not declarations: the book's own phase, the orders it still
            # tracks and the last snapshot it accepted. A caller cannot assert
            # a flat account on the loop's behalf.
            outcome=self.admission.settle(instance_id,trade_id,authority=self.book)
            settled.append({'trade_id':trade_id,'instance_id':instance_id,**outcome})
        return {'enabled':True,'settled':settled}

    def _dispatch_intent(self,intent,snapshot,quotes):
        """Hand one admitted intent to the execution authority.

        Admission reserved the money and the symbol; this is the step that makes
        the trade exist in the book. It is separate so that a session which may
        not dispatch still records a durable, recoverable intent instead of
        losing the advice.
        """
        quote=(quotes or {}).get(intent['symbol'])
        if quote is None or snapshot is None:
            return {'dispatched':False,'reason':'INTENT_QUOTE_OR_SNAPSHOT_MISSING'}
        plan={'symbol':intent['symbol'],'quantity':intent['quantity'],
              'entry_ceiling':intent['entry_ceiling'],'stop':intent['stop_price'],
              'cost_buffer_per_share':intent.get('cost_buffer_per_share')
                  or intent.get('plan',{}).get('cost_buffer_per_share','0')}
        evidence={'source_sha256':intent['intent_id'],
                  'synthetic':self.book.simulation,
                  'qualified_strategy_entry':bool(intent.get('qualified_strategy_entry')
                      or (intent.get('plan') or {}).get('qualified_strategy_entry')),
                  # The original envelope travels with the intent so the book
                  # can enforce the advice's own validity instead of granting a
                  # fresh window on arrival.
                  'strategy_proposal_id':intent.get('proposal_id'),
                  'strategy_generated_at':intent.get('generated_at'),
                  'strategy_valid_until':intent.get('valid_until')}
        evidence={key:value for key,value in evidence.items() if value is not None}
        mark=getattr(self.admission,'mark_intent',None)
        if callable(mark) and mark(intent['intent_id'],'DISPATCHING')!=1:
            # Another tick already started this intent; do not propose twice.
            return {'dispatched':False,'reason':'INTENT_NOT_PENDING'}
        try:
            result=self.book.propose(intent['trade_id'],plan,quote,snapshot,
                evidence=evidence)
        except (ValueError,KeyError,TypeError,OSError,sqlite3.Error) as exc:
            # A rejection that created no action is provably unsent: terminate
            # it now instead of holding the reservation until someone notices.
            if self._entry_action_for(intent['trade_id']) is None:
                abandon=getattr(self.admission,'abandon_intent',None)
                if callable(abandon):abandon(intent['intent_id'])
                return {'dispatched':False,'released':True,
                        'reason':str(exc) if isinstance(exc,(ValueError,KeyError,TypeError)) else 'INTENT_DISPATCH_FAILED'}
            return {'dispatched':False,'released':False,'reason':'INTENT_DISPATCH_UNRESOLVED'}
        # Bind the identity the book actually submitted with, before the intent
        # is declared dispatched. If binding fails the intent stays DISPATCHING,
        # so the next recovery pass retries it against the existing action
        # instead of leaving a READY order with no owner.
        bound=self._bind_execution_reference(intent,result.get('action_id'))
        if not bound.get('execution_ref_bound'):
            return {'dispatched':False,'action_id':result.get('action_id'),
                    'execution_ref_bound':False,'reason':'EXECUTION_REFERENCE_UNBOUND'}
        delivery=None
        if (self.host_port is not None
                and not getattr(self.dispatcher,'handles_intent_delivery',False)):
            try:
                delivery=self.host_port.deliver({'intent_id':intent['intent_id'],
                    'action_id':result.get('action_id'),'trade_id':intent['trade_id'],
                    'ref_id':bound.get('execution_ref'),'symbol':intent['symbol']})
            except (ValueError,KeyError,TypeError,OSError,RuntimeError) as exc:
                # The intent stays DISPATCHING: the action exists and is bound,
                # so recovery adopts it; nothing here re-sends the delivery.
                return {'dispatched':False,'action_id':result.get('action_id'),
                        **bound,'host_delivery':False,
                        'reason':'HOST_DELIVERY_FAILED:'+type(exc).__name__}
        if callable(mark):mark(intent['intent_id'],'DISPATCHED')
        return {'dispatched':True,'action_id':result.get('action_id'),**bound,
                **({'host_delivery':delivery} if delivery is not None else {})}

    def _execution_reference(self,action_id):
        for action in self.book.actions():
            if action.get('id')!=action_id:continue
            try:payload=json.loads(action.get('payload') or '{}')
            except ValueError:return None
            ref=payload.get('ref_id')
            return ref if isinstance(ref,str) and ref else None
        return None

    def _bind_execution_reference(self,intent,action_id):
        attribution=getattr(self.admission,'attribution',None)
        bind=getattr(attribution,'bind',None)
        if not callable(bind) or not isinstance(action_id,str) or not action_id:
            return {'execution_ref_bound':False,'reason':'EXECUTION_REFERENCE_UNAVAILABLE'}
        ref=self._execution_reference(action_id)
        if ref is None:
            return {'execution_ref_bound':False,'reason':'EXECUTION_REFERENCE_UNAVAILABLE'}
        try:
            bind(ref,intent['instance_id'],intent['trade_id'])
        except (ValueError,KeyError,TypeError,OSError,sqlite3.Error) as exc:
            return {'execution_ref_bound':False,
                    'reason':str(exc) if isinstance(exc,(ValueError,KeyError,TypeError)) else 'EXECUTION_REFERENCE_BIND_FAILED'}
        return {'execution_ref_bound':True,'execution_ref':ref}

    def _step_async(self,snapshot,events,*,allow_entries,allow_dispatch):
        errors=[]
        quotes=self._management_quotes(events,errors)
        reconciled=self._reconcile(snapshot,quotes,errors)
        if not reconciled['reconciled']:
            # Reconciliation aborted, so nothing may be released or admitted on
            # this snapshot. Protection state is reported, never assumed.
            return self._unreconciled_step(snapshot,quotes,errors,reconciled)
        receipts=self._import_receipts(snapshot,errors)
        bindings=self._bind_management(snapshot,errors)
        management=any(a['kind']!='entry' and a['state'] in
            {'READY','RUNNING','UNKNOWN','WAITING_USER','REPORTED'} for a in self.book.actions())
        early=self._dispatch(snapshot,quotes,allow_entries=False,allow_dispatch=allow_dispatch) if management else None
        for event in events:
            try:self.strategy_queue.enqueue(event)
            except (ValueError,KeyError,TypeError,OSError,sqlite3.Error) as exc:
                reason=str(exc) if isinstance(exc,(ValueError,KeyError,TypeError)) else 'STRATEGY_QUEUE_ENQUEUE_FAILED'
                errors.append({'rejected':True,'reason':reason,'scope':'STRATEGY_INPUT'})
                if isinstance(event,dict):
                    try:record_capture_disposition(self.book,event,reason=reason)
                    except (ValueError,KeyError,TypeError,OSError,sqlite3.Error):
                        errors.append({'rejected':True,'reason':'REJECTION_ARCHIVE_FAILED','scope':'STRATEGY_INPUT'})
        progress=self.advance_strategy();errors.extend(self._strategy_pending_errors)
        self._strategy_pending_errors=[]
        # Integrity diagnostics retain the durable global entry inhibit.
        if any(e.get('reason') in {'CONSUMED_BAR_REVISED','STRATEGY_EVENT_ID_CONFLICT',
                'STRATEGY_EVENT_PROVENANCE_REQUIRED'} for e in errors):
            from martin_agentic.watchdog import inhibit_new_entries
            db=self.book.db.execute('PRAGMA database_list').fetchone()[2]
            try:inhibit_new_entries(db,['STRATEGY_INPUT_INTEGRITY'],clock=self.book.clock)
            except OSError:errors.append({'rejected':True,'reason':'ENTRY_INHIBIT_PERSISTENCE_FAILED','scope':'STRATEGY_INPUT'})
        durable_blocked=self._durable_integrity_blocked()
        if (durable_blocked or self._receipts_incomplete or self._bindings_incomplete
                or self.integrity_gate_unavailable):
            # The gate is a durable account fact, not a session flag: a clean
            # snapshot or a restart does not clear it.
            reason=('EXECUTION_INTEGRITY_FAULT_UNRESOLVED'
                    if durable_blocked else
                    self._receipt_incomplete_reason()
                    if self._receipts_incomplete else
                    'MANAGEMENT_BINDING_INCOMPLETE')
            errors.append({'reason':reason,
                           'scope':'STRATEGY_EXECUTION','source':'durable_gate'})
        result=self._finish_step(snapshot,quotes,errors,allow_entries=allow_entries,
            allow_dispatch=allow_dispatch,reconciled=True,early_dispatch=early)
        result['strategy_queue']=progress
        if (durable_blocked or self._receipts_incomplete or self._bindings_incomplete
                or self.integrity_gate_unavailable):
            result['reconciliation_pending']=True
            result['settlement_blocked']='RECEIPT_IMPORT_INCOMPLETE'
            result['new_entries_blocked']=True
            result['integrity_inhibit_persisted']=self._gate_inhibit_persisted
            result.update(self._integrity_fields())
            if self.integrity_gate_unavailable:
                result['stop_disposition']=('EXECUTION_INTEGRITY_FAULT_LEDGER_UNAVAILABLE_'
                                            'MANUAL_HANDOFF_REQUIRED')
        return result

    def _finish_step(self,snapshot,quotes,errors,*,allow_entries,allow_dispatch,reconciled=False,early_dispatch=None):
        # Strategy rejection never suppresses broker-based protective management.
        allow_entries=allow_entries and not errors
        if not reconciled:
            self.book.reconcile(snapshot,quotes=quotes,features=self.exit_features(),
                                exit_advices=self._exit_advices(snapshot,quotes))
        hook=getattr(self,'post_reconcile_publication',None)
        if hook is not None:
            self.capture_publication_result=hook(snapshot)
        if not capture_publication_ok(self.capture_publication_result):
            # A failed current publication cannot admit new risk from this or
            # a later stale context; protection/management may still proceed.
            allow_entries=False
        production=self.production.assess(snapshot)
        strategy_entries=(allow_entries and self.book.policy.get('engineering_only') is not True
            and (not production['required_for_current_dispatch']
                 or production['automatic_strategy_entry_allowed']))
        for signal in self.signals.pending() if strategy_entries else []:
            try:result=self.book.propose(signal['id'],signal['plan'],signal['quote'],snapshot,evidence=signal['evidence'])
            except (ValueError,KeyError,TypeError) as exc:
                result={'rejected':True,'reason':str(exc)};errors.append(result)
            self.signals.complete(signal['id'],result=result)
        dispatch_entries=self.production.permits_entry_dispatch(admission=production,allow_entries=allow_entries)
        verification=self.book.engineering_verification_gate(purpose='engineering')
        if not isinstance(verification,dict) or type(verification.get('blocked')) is not bool:
            verification={'blocked':True,'reason':'LIVE_VERIFICATION_CONTROL_DIAGNOSTIC_UNAVAILABLE',
                'execution_authorized':False}
        verification_requests_allowed=(not verification['blocked']
            or verification.get('reason')=='NEW_ENGINEERING_AUTHORIZATION_REQUIRED')
        dispatch=(early_dispatch if early_dispatch is not None else
            self._dispatch(snapshot,quotes,allow_entries=dispatch_entries,allow_dispatch=allow_dispatch))
        result={'lifecycle':self.book.summary(),'signals':len(self.signals.state()['machines']),
            'errors':errors,'strategy_exit_advice_errors':list(self._exit_advice_errors),
                'strategy_exit_advice_history':list(self._exit_advice_history),
            'host_delivery_status':dict(self.host_delivery_status),
            'execution_targeted_operations':(
                'AVAILABLE' if getattr(getattr(self.ports,'execution',None),'operate',None)
                else 'UNAVAILABLE'),'dispatch':dispatch,'live_orders_by_script':0,
            'new_entries_blocked':(not allow_entries or (production['required_for_current_dispatch']
                and not production['automatic_strategy_entry_allowed'])),
            'live_verification_control':verification,
            'explicit_engineering_requests_allowed':bool(dispatch_entries and verification_requests_allowed
                and self.production.agent_mode in {'execute','supervised-host'}),
            'production_admission':production,'automatic_strategy_entries_blocked':not production['automatic_strategy_entry_allowed'],
            'review_only_strategy_proposals_allowed':strategy_entries and not production['required_for_current_dispatch'],
            'entry_dispatch_scope':'ENGINEERING_ONLY' if production['required_for_current_dispatch']
                and not production['automatic_strategy_entry_allowed'] else 'REVIEW_OR_SIMULATION'}
        # Management/entry dispatch above retains priority.  Facts refresh is
        # diagnostic only and runs after the owner publication and management
        # path, so a slow/failed current-context read cannot delay protection.
        result['account_facts']=self._refresh_current_account_facts()
        return result


class AsyncDispatcher:
    """Claim/report on the owner thread; one external worker at a time."""
    def __init__(self,book,worker,output_dir,budget):
        self.book=book;self.worker=worker;self.directory=output_dir;self.budget=budget
        self.task=BackgroundTask('native-order-worker');self.action_id=None;self.action_hash=None;self.binding=None

    def __call__(self,*,allow_entries=True,allow_dispatch=True):
        completed=self.task.take()
        if completed:
            ok,value=completed
            if not ok:
                value={'action_id':self.action_id,'report':{'status':'unknown','message':'WORKER_RECEIPT_UNAVAILABLE_CHECK_BROKER'}}
            if ok:
                try:
                    receipt=json.loads(Path(value['receipt_file']).read_text())
                    if receipt.get('binding')!=self.binding or receipt.get('action_sha256')!=self.action_hash or receipt.get('action_id')!=self.action_id or receipt.get('report')!=value['report']:
                        raise ValueError('WORKER_RECEIPT_BINDING_INVALID')
                except (OSError,ValueError,KeyError,TypeError):
                    value={'action_id':self.action_id,'report':{'status':'unknown','message':'WORKER_RECEIPT_BINDING_INVALID_CHECK_BROKER'}}
            current=next((a for a in self.book.actions() if a['id']==self.action_id),None)
            if current and current['state']=='RUNNING':
                try:self.book.report(value['action_id'],value['report'])
                except (ValueError,TypeError,KeyError):
                    self.book.report(value['action_id'],{'status':'unknown','message':'INVALID_WORKER_RESULT_RECONCILE_BEFORE_RETRY'})
                value['report_applied']=True
            else:
                # Broker reconciliation can advance an action before the model
                # returns. Keep the durable receipt, never regress authoritative state.
                value['report_applied']=False
                value['reason']='ACTION_ALREADY_ADVANCED_RECEIPT_RETAINED'
            self.action_id=None
            return {'agent_called':False,'completed':value,'worker':self.task.status()}
        if self.task.thread is not None:
            return {'agent_called':False,'reason':'WORKER_IN_FLIGHT','worker':self.task.status()}
        if not allow_dispatch:
            return {'agent_called':False,'reason':'FRESH_BROKER_OBSERVATION_REQUIRED'}
        budget=self.budget.status()
        if not budget['management_available']:
            return {'agent_called':False,'reason':'MODEL_BUDGET_EXHAUSTED_MANAGEMENT_PENDING','budget':budget,'attention_required':True}
        db_path=self.budget.db.execute('PRAGMA database_list').fetchone()[2]
        lease=worker_lease(db_path)
        if lease is None:return {'agent_called':False,'reason':'WORKER_OWNER_STILL_ACTIVE','attention_required':True}
        try:action=self.book.claim(allow_entries=allow_entries and budget['entries_available'])
        except BaseException:lease.close();raise
        if not action:
            lease.close();return {'agent_called':False,'reason':'NO_DISPATCHABLE_ACTION','budget':budget}
        if not self.budget.reserve(action):
            lease.close()
            # A failed durable reservation must never reach a model or write tool.
            self.book.report(action['action_id'],{'status':'unknown','message':'BUDGET_RESERVATION_FAILED_NOT_DISPATCHED'})
            return {'agent_called':False,'reason':'BUDGET_RESERVATION_FAILED','attention_required':True}
        self.action_id=action['action_id'];self.action_hash=hashlib.sha256(json.dumps(action,sort_keys=True,separators=(',',':')).encode()).hexdigest()
        state=self.book.state()
        self.binding={'db':str(Path(db_path).resolve()),'account_key':state.get('account_key') if isinstance(state,dict) else None}
        try:self.task.start(run_leased_worker,lease,self.directory,action,self.worker,self.binding)
        except BaseException:lease.close();raise
        return {'agent_called':True,'action_id':self.action_id,'worker':self.task.status(),'budget':self.budget.status()}

    def close(self):
        # Do not cancel broker protection or infer a timed-out model did not write.
        # The durable RUNNING action is preserved by recover() as UNKNOWN.
        if self.task.thread is not None:
            stopper=getattr(self.worker,'stop',None)
            if callable(stopper):stopper()
            self.task.thread.join(timeout=.25)
        return self.task.status()


def collect_live_events(signals,*,symbols,clock=utc_now,capture_context=None,cancellation=None,
                        requirements=None):
    if len(symbols)!=len(set(symbols)) or not 10<=len(symbols)<=30:raise ValueError('EXPLICIT_UNIQUE_10_TO_30_STOCK_POOL_REQUIRED')
    now=clock();market=session(now.isoformat())
    if not market or not is_regular(now.isoformat()):raise ValueError('LIVE_INPUT_REQUIRES_REGULAR_SESSION')
    # Requirements come from the plugin manifests that will consume the
    # events. A legacy caller gets the Martin profile; a quotes-only family
    # must not be dragged through Martin's daily universe bootstrap.
    supported={'quotes','5minute-bars','universe-screen'}
    if requirements is None:
        required={'quotes','5minute-bars','universe-screen'}
    elif isinstance(requirements,(str,bytes)) or not isinstance(requirements,(tuple,list,set)):
        raise ValueError('MARKET_REQUIREMENTS_INVALID')
    else:
        required=set(requirements)
        unknown=required-supported
        if unknown:raise ValueError('MARKET_REQUIREMENT_UNSUPPORTED:'+','.join(sorted(unknown)))
    events=[];context=capture_context or {}
    directory=Path('runtime/live-captures/sessions')/(market['date']+'-'+digest(context)[:16])
    def preserve(record):
        persist_raw_record(directory,canonical(record).encode(),observed_at=record['observed_at'],
            metadata={'kind':'provider_response','binding':context})
    from martin_agentic.control_tasks import owned_read_transport
    with owned_read_transport(CodexMarketTransport,cancellation) as transport:
        reader=RobinhoodReader(transport,clock=clock,record_observer=preserve)
        # Metadata readiness is answered per consumer, not by one shared state:
        # a fully prepared instance group must not bootstrap again, and an
        # instance that is behind or a new session day must.
        projection=signals.collection_state() if hasattr(signals,'collection_state') else None
        state=projection.state() if projection is not None else signals.state()
        needs_daily=False
        if 'universe-screen' in required:
            needs_daily=(projection.needs_daily(market['date']) if projection is not None
                         else (state.get('universe_date')!=market['date']
                               or 'screening_summary' not in state
                               or 'indicator_values' not in state))
        if needs_daily:
            daily=collect_daily(reader,symbols=symbols,start_time=(now-timedelta(days=150)).replace(hour=0,minute=0,second=0,microsecond=0).isoformat(),
                end_time=market['open'].replace(hour=0,minute=0,second=0,microsecond=0).isoformat())
            persist_raw_record(directory,json.dumps(daily,sort_keys=True).encode(),observed_at=clock().isoformat(),
                metadata={'kind':'daily_normalization_batch','binding':context})
            at=clock().isoformat();e=universe_event(daily,as_of=at,universe_id='configured-live-pool-'+digest(symbols)[:12])
            # Collection is read-only; only the control owner consumes this event.
            events.append(e)
        records=[]
        # The first regular-session 5-minute bar is not complete until the
        # exchange-calendar availability instant.  Asking the upstream for a
        # range ending before that instant returns an invalid-argument error,
        # not an empty successful series.  Keep quotes flowing and wait for the
        # first completed bar instead of making a failing historicals call.
        first_bar_available=bar_available_at(market['open'].isoformat(),'5minute')
        bars_ready=now>=first_bar_available
        if '5minute-bars' in required and bars_ready:
            for offset in range(0,len(symbols),10):
                records.append(reader.fetch('get_equity_historicals',{'symbols':symbols[offset:offset+10],
                    'interval':'5minute','bounds':'regular','adjustment_type':'split',
                    'start_time':market['open'].isoformat(),'end_time':clock().isoformat()}))
        # Quotes last so history requests do not age executable prices unnecessarily.
        for offset in range(0,len(symbols),10):records.append(reader.fetch('get_equity_quotes',{'symbols':symbols[offset:offset+10]}))
    recording={'schema_version':1,'provider':'robinhood','synthetic':False,'records':records}
    persist_raw_record(directory,canonical(recording).encode(),observed_at=clock().isoformat(),
        metadata={'kind':'intraday_normalization_batch','binding':context})
    at=clock().isoformat();normalized=inspect_recording(recording,as_of=at)
    if not normalized['data_usable']:raise ValueError('LIVE_MARKET_INPUT_REJECTED:'+canonical(normalized['errors']+normalized['freshness_errors']))
    source=digest(recording)
    view=signals.collection_view() if hasattr(signals,'collection_view') else state
    new_bars,unchanged_bars=filter_unchanged_bars(normalized['bars'],view)
    bar_objects_to_emit={id(data) for data in new_bars}
    for kind,key in (('bar','bars'),('quote','quotes')):
        # Retain the original normalized index in event IDs even when filtering.
        for index,data in enumerate(normalized[key]):
            if kind=='bar' and id(data) not in bar_objects_to_emit:continue
            events.append({'id':source+'-'+kind+str(index),'at':at,'kind':kind,'data':data,
                'synthetic_input':False,'source_sha256':source})
    return events,{'raw_capture_directory':str(directory.resolve()),'at':at,'bars':len(normalized['bars']),'quotes':len(normalized['quotes']),
        'unchanged_bar_observations_omitted':unchanged_bars,'new_or_changed_bars':len(new_bars),
        'universe_complete':state.get('universe_complete',False),'source_sha256':source,'candidate_count':len(state.get('machines') or {}),'screening':state.get('screening_summary',{}),
        'daily_bootstrap_performed':bool(needs_daily),
        'bars_waiting_for_first_completion':bool('5minute-bars' in required and not bars_ready),
        'first_bar_available_at':first_bar_available.isoformat(),
        'collection_state':(projection.describe() if projection is not None else {'source':'legacy'})}


def _cancel_bounded(cancellation, deadline):
    """Invoke one cancellation within the shared deadline.

    Cancellation is always started in one named, tracked thread.  A
    cancellation contract that accepts ``deadline_monotonic`` passes it to
    the production transport; an older contract is still bounded by the same
    deadline.  If the thread outlives the budget the drain fails and keeps
    its identity, rather than running blocking work outside the budget.
    """
    if cancellation is None:
        return {'done': True, 'contract': 'NONE'}
    try:
        parameters = inspect.signature(cancellation.cancel).parameters
    except (TypeError, ValueError):
        parameters = {}
    contract=('DEADLINE' if 'deadline_monotonic' in parameters
              else 'COMPATIBILITY_THREAD')
    holder = {}
    def run_cancel():
        try:
            holder['result'] = (cancellation.cancel(deadline_monotonic=deadline)
                                if contract=='DEADLINE' else cancellation.cancel())
        except BaseException as exc:
            holder['error'] = type(exc).__name__
        finally:
            holder['done'] = True
    thread = threading.Thread(target=run_cancel, name='owned-read-cancel',
                              daemon=True)
    thread.start()
    thread.join(max(0.0, deadline - time.monotonic()))
    if thread.is_alive():
        return {'done': False, 'contract': contract,
                'cancel_thread_alive': True, 'cancel_thread_id': thread.ident}
    if holder.get('error'):
        return {'done': False, 'contract': contract,
                'error': holder['error']}
    result=holder.get('result')
    rpc_cleanup = result.get('rpc_cleanup') if isinstance(result, dict) else None
    if isinstance(rpc_cleanup, dict) and rpc_cleanup.get('complete') is False:
        return {'done': False, 'contract': contract,
                'rpc_cleanup': rpc_cleanup}
    return {'done': True, 'contract': contract,
            'rpc_cleanup': rpc_cleanup}

def _drain_read_task(task,cancellation,grace=READ_SHUTDOWN_GRACE_SECONDS,
                     deadline_monotonic=None):
    """Cancel, join and take one owned read task within one shared deadline.

    No blocking call starts before the deadline exists.  The cancellation
    owns RPC process/pipe/reader cleanup; this function owns the read thread
    and its result mailbox.  A late registration or a still-live reader is a
    failed drain, never a successful one.
    """
    if task.thread is None:
        return {'thread_alive':False,'running':False,'drained':True,
                'result':None,'status':task.status(),
                'drain_grace_seconds':grace,
                'resource':{'task':task.name,'thread_id':None},
                'cancel':{'done':True,'contract':'NO_THREAD'}}
    deadline = (deadline_monotonic if deadline_monotonic is not None
                else time.monotonic() + grace)
    thread_id=task.thread.ident
    cancel = _cancel_bounded(cancellation, deadline)
    cleanup=_cancel_cleanup(cancellation)
    if not cancel.get('done'):
        # The cancellation thread may still be between "started" and
        # "recorded cleanup". Report a structured not-yet-cleanup phase instead
        # of None; this is not a successful drain and never claims resources
        # were reclaimed.
        placeholder = {
            'complete': False,
            'not_started': True,
            'budget_exhausted': bool(
                deadline_monotonic is not None
                and time.monotonic() >= deadline_monotonic),
            'cancel_thread_alive': bool(cancel.get('cancel_thread_alive')),
            'cancel_contract': cancel.get('contract'),
        }
        if cleanup is None:
            cleanup = {'rpc_cleanup': placeholder,
                       'late_registration': None}
        elif isinstance(cleanup, dict) and cleanup.get('rpc_cleanup') is None:
            cleanup = dict(cleanup)
            cleanup['rpc_cleanup'] = placeholder
        return {'thread_alive':task.thread.is_alive(),'running':True,
                'drained':False,'result':None,'status':task.status(),
                'drain_grace_seconds':grace,
                'resource':{'task':task.name,'thread_id':thread_id},
                'cancel':cancel,'cleanup':cleanup}
    while task.thread is not None and task.thread.is_alive() \
            and time.monotonic()<deadline:
        remaining=deadline-time.monotonic()
        task.thread.join(timeout=min(.05,max(0.0,remaining)))
    if task.thread is not None and task.thread.is_alive():
        return {'thread_alive':True,'running':True,'drained':False,
                'result':None,'status':task.status(),
                'drain_grace_seconds':grace,
                'resource':{'task':task.name,'thread_id':thread_id},
                'cancel':cancel,'cleanup':cleanup}
    result=task.take()
    cleanup=_cancel_cleanup(cancellation)
    if cleanup is not None and not _cleanup_complete(cleanup):
        return {'thread_alive':False,'running':task.thread is not None,
                'drained':False,'result':result,'status':task.status(),
                'drain_grace_seconds':grace,
                'resource':{'task':task.name,'thread_id':thread_id},
                'cancel':cancel,'cleanup':cleanup}
    return {'thread_alive':False,'running':task.thread is not None,
            'drained':True,'result':result,'status':task.status(),
            'drain_grace_seconds':grace,
            'resource':{'task':task.name,'thread_id':thread_id},
            'cancel':cancel,'cleanup':cleanup}


def _cancel_cleanup(cancellation):
    getter=getattr(cancellation,'cleanup_status',None)
    if not callable(getter):
        return None
    try:return getter()
    except (ValueError,TypeError,AttributeError):
        return {'cleanup_status_error':True,'complete':False}


def _cleanup_complete(status):
    if not isinstance(status,dict):
        return False
    if status.get('cleanup_status_error'):return False
    if 'complete' in status and status.get('complete') is not True:return False
    rpc=status.get('rpc_cleanup')
    late=status.get('late_registration')
    for item in (rpc,late):
        if item is None:continue
        if not isinstance(item,dict) or item.get('complete') is not True:
            return False
    return True


def _current_observation_fresh(now, account_at, market_at, events,
                               market_optional=False):
    """Re-check the last consumed account/market evidence at stop time."""
    if account_at is None:
        return False
    try:
        account_age=(now-timestamp(account_at)).total_seconds()
        if not 0<=account_age<=30:
            return False
        if market_at is not None:
            market_age=(now-timestamp(market_at)).total_seconds()
            if not 0<=market_age<=30:
                return False
        elif not market_optional:
            # No market observation was ever consumed in a regular session.
            return False
        for event in events or ():
            if not isinstance(event,dict):
                return False
            data=event.get('data')
            observed=event.get('at')
            if observed is None and event.get('kind')=='quote' \
                    and isinstance(data,dict):
                observed=data.get('observed_at')
            if observed is None or not 0<=(now-timestamp(observed)).total_seconds()<=30:
                return False
            if event.get('kind')=='quote':
                if not isinstance(data,dict) or not all(
                        0<=(now-timestamp(data[key])).total_seconds()<=15
                        for key in ('bid_at','ask_at','observed_at')):
                    return False
        return True
    except (ValueError,KeyError,TypeError,AttributeError):
        return False

def _wait_with_facts(task,dispatcher,seconds,*,strategy_supervisor=None,
                     account_facts_supervisor=None):
    """Call the wait hook with facts support, tolerating legacy test doubles.

    Production uses the full signature.  Older tests/harnesses replace the
    wait hook with a positional-only callable; those never receive owner
    progress and keep their original bounded behavior.
    """
    full={}
    if strategy_supervisor is not None:
        full['strategy_supervisor']=strategy_supervisor
    if account_facts_supervisor is not None:
        full['account_facts_supervisor']=account_facts_supervisor
    attempts=[full,
              {key:value for key,value in full.items()
               if key!='account_facts_supervisor'},
              {key:value for key,value in full.items()
               if key!='strategy_supervisor'},
              {}]
    last=None
    seen=set()
    for kwargs in attempts:
        marker=tuple(sorted(kwargs))
        if marker in seen:
            continue
        seen.add(marker)
        try:
            return wait_for_result_or_tick(task,dispatcher,seconds,**kwargs)
        except TypeError as exc:
            last=exc
            text=str(exc)
            if not any(name in text for name in
                       ('account_facts_supervisor','strategy_supervisor')):
                raise
    raise last


def wait_for_result_or_tick(market_task,dispatcher,poll_seconds,*,
                            strategy_supervisor=None,
                            account_facts_supervisor=None):
    """Wake on durable task completion or bounded owner progress.

    Strategy progress is advisory-only and optional.  Account-facts progress is
    always served, even when no strategy scheduler exists: the owner polls the
    single read-only child, validates a candidate against its current epoch,
    and returns early on completion/deadline.  It never proposes, dispatches or
    treats a historical observation as a fresh broker collection.
    """
    tasks=[]
    if market_task is not None:tasks.append(market_task)
    if dispatcher is not None:tasks.append(dispatcher.task)
    wake=getattr(dispatcher,'wake_dispatcher',None) if dispatcher is not None else None
    if wake is not None:tasks.append(wake.task)
    def deadline_budget():
        deadline=getattr(wake,'next_deadline_monotonic',None) if wake is not None else None
        return max(0,deadline-time.monotonic()) if type(deadline) in (int,float) and math.isfinite(deadline) else poll_seconds
    strategy_enabled=getattr(strategy_supervisor,'strategy_schedule_enabled',False) is True
    probe=getattr(strategy_supervisor,'strategy_busy',None)
    def strategy_busy():
        return bool(strategy_enabled and callable(probe) and probe())
    facts_get_state=getattr(
        account_facts_supervisor,'account_facts_wait_state',None)
    facts_progress=getattr(
        account_facts_supervisor,'account_facts_progress',None)
    def facts_state():
        if not callable(facts_get_state):
            return {'inflight':False,'deadline_remaining_seconds':None}
        try:
            value=facts_get_state()
        except Exception:
            return {'inflight':False,'deadline_remaining_seconds':None}
        if not isinstance(value,dict):
            return {'inflight':False,'deadline_remaining_seconds':None}
        remaining=value.get('deadline_remaining_seconds')
        if (type(remaining) not in (int,float)
                or not math.isfinite(remaining)):
            remaining=None
        return {'inflight':value.get('inflight') is True,
                'deadline_remaining_seconds':remaining}
    def facts_inflight():
        return facts_state().get('inflight') is True
    def facts_budget():
        state=facts_state()
        return (state.get('deadline_remaining_seconds')
                if state.get('inflight') is True else poll_seconds)
    remaining=min(poll_seconds,deadline_budget(),facts_budget())
    if (not strategy_busy() and not facts_inflight()
            and not any(task.thread is not None for task in tasks)):
        time.sleep(remaining);return
    if facts_inflight() and remaining<=0 and callable(facts_progress):
        try: facts_progress()
        except Exception: pass
        return
    end=time.monotonic()+remaining
    while remaining>0:
        if strategy_enabled:strategy_supervisor.advance_strategy()
        if callable(facts_progress):
            try:
                progress=facts_progress()
            except Exception:
                progress=None
            if (isinstance(progress,dict)
                    and progress.get('changed') is True
                    and progress.get('inflight') is not True):
                return
        task_ready=any(task.thread is not None
                       and not task.mailbox.empty() for task in tasks)
        if task_ready:
            # A ready broker/market task must not make us abandon the child
            # before the next cycle's journal ingest changes its epoch.  Keep
            # the task queued and service facts until completion/deadline.
            if not facts_inflight():
                return
        remaining=min(remaining,deadline_budget(),facts_budget())
        if remaining<=0:return
        pause=min(.01 if strategy_busy() else .1,remaining)
        time.sleep(pause)
        if strategy_enabled or facts_inflight():
            remaining=min(max(0.0,end-time.monotonic()),deadline_budget(),
                          facts_budget())
        else:
            # Preserve the original bounded logical tick when callers replace
            # sleep with a virtual/no-op scheduler.
            remaining=remaining-pause
    if facts_inflight() and callable(facts_progress):
        # The sleep may consume the last fraction of the child deadline.
        # Poll once after the loop so the timeout is recorded at that deadline,
        # not only at the next fresh broker step.
        try: facts_progress()
        except Exception: pass


def record_capture_disposition(book,event,*,reason):
    """Retain rejected data/control diagnostics without consuming strategy state."""
    now=book.clock().isoformat();book._begin()
    try:
        archive_event(book.db,event,consumed_at=now)
        archive_decision(book.db,event_id=event['id'],result_id='admission',recorded_at=now,
            context={'status':'NOT_CONSUMED','reason':reason,'origin':'control_loop',
                'market_observation':event.get('source_type')!='local_control_diagnostic'})
        book.db.commit()
    except BaseException:book.db.rollback();raise


READ_ERRORS=(ValueError,KeyError,TypeError,OSError,RuntimeError)


def _broker_read(policy,clock,*,approval_ids=None,cancellation=None):
    """One saved observation under one absolute 10-second RPC budget.

    The detached read owns only collector/private-file work.  The saved bundle
    is converted to a supported-coverage snapshot here; SQLite journal, risk,
    execution reconciliation and publication remain owner-thread work.  A
    missing bridge configuration is a structured failure, never a fallback to
    the former parallel collect_broker path.
    """
    from martin_agentic.live_capture_bridge import SavedBundleObservationBridge
    deadline=time.monotonic()+10
    for attempt in range(2):
        if cancellation is not None and cancellation.stopped:
            raise RuntimeError('READ_ONLY_COLLECTION_STOPPED')
        if time.monotonic()>=deadline:
            raise TransportError('APP_SERVER_TOTAL_BUDGET_EXCEEDED')
        try:
            from martin_agentic.control_tasks import owned_read_transport
            with owned_read_transport(CodexBrokerReadTransport,cancellation,deadline_monotonic=deadline) as transport:
                bridge=SavedBundleObservationBridge(policy,clock)
                return bridge.read(transport,deadline_monotonic=deadline,
                    approval_ids=approval_ids,timeout_seconds=10)
        except READ_ERRORS as exc:
            if attempt or not safe_read_failure(exc)['retryable']:raise


def broker_approval_ids(book):
    own=set(book.pending_approval_ids())
    if not book.simulation and book.policy.get('capture_only') is not True:
        from martin_agentic.session_lineage import lineage_approval_ids
        try:own.update(lineage_approval_ids(book))
        except (ValueError,KeyError,TypeError,OSError,RuntimeError,sqlite3.Error):
            # Missing predecessor proof blocks entry at the separate gate; it
            # must not prevent fresh account reads and existing management.
            pass
    return sorted(own)


def publish_owner_observation(book,journal,policy,clock,snapshot):
    """Owner-thread publication of the exact saved observation just ingested.

    The journal write happened first. Publication never rolls back or rewrites
    broker facts. Association is built from the durable current journal row,
    not a caller self-report; failure leaves the last registry generation
    (which then ages out) and reports a structured assessment failure.
    """
    if not isinstance(snapshot,dict) or not isinstance(snapshot.get('live_capture'),dict):
        return {'status':'NOT_A_SAVED_BUNDLE_OBSERVATION','committed':False,
            'entry_allowed':False}
    try:
        from martin_agentic.live_capture_bridge import (
            SavedBundleObservationBridge,journal_association_projection)
        payload=journal.current_observation()
        if payload is None:
            raise ValueError('CURRENT_JOURNAL_OBSERVATION_MISSING')
        projection=journal_association_projection(payload)
        result=SavedBundleObservationBridge(policy,clock).publish(
            snapshot,book=book,journal_snapshot=projection)
        status=result.get('status')
        committed=bool(result.get('committed')
            or status in {'PUBLISHED','RECOVERED','ALREADY_PUBLISHED'})
        current=bool(result.get('current_evidence_valid'))
        return {'status':status,'committed':committed,
            'current_evidence_valid':current,
            'entry_allowed':False,
            'generation':(snapshot.get('live_capture') or {}).get('generation'),
            'association':result.get('association'),
            'current_context':result.get('current_context'),
            'reasons':result.get('current_reasons')}
    except (ValueError,KeyError,TypeError,OSError,RuntimeError,sqlite3.Error) as exc:
        code=getattr(exc,'code',None) or str(exc)
        return {'status':'FAILED','committed':False,'current_evidence_valid':False,
            'entry_allowed':False,'reason':code,
            'generation':(snapshot.get('live_capture') or {}).get('generation'),
            'current_context':{'status':'FAILED','reason':code}}


def capture_publication_ok(publication):
    if not isinstance(publication,dict):
        return True
    if publication.get('status')=='NOT_A_SAVED_BUNDLE_OBSERVATION':
        return True
    return bool(publication.get('committed')
        and publication.get('current_evidence_valid'))


def full_account_flat_authority(snapshot):
    """Whether a snapshot proves whole-account flatness, not just equity scope.

    Legacy collect_broker snapshots predate the explicit scope fields; their
    supported-equity meaning remains the compatibility contract. A bridge
    snapshot always carries explicit fields and must prove every one of them.
    """
    if not isinstance(snapshot,dict):
        return False
    explicit=any(key in snapshot for key in (
        'account_flat_proven','full_baseline_complete',
        'settlement_ledger_complete','unsupported_scopes'))
    if not explicit:
        # A legacy supported-equity snapshot is a read fact, not proof of
        # whole-account flatness. It may still report supported_scope_flat.
        return False
    return bool(snapshot.get('account_flat_proven') is True
        and snapshot.get('full_baseline_complete') is True
        and snapshot.get('settlement_ledger_complete') is True
        and snapshot.get('unsupported_scopes') == [])


def _closing_check(book,journal,policy,clock,*,observation=None):
    result={'final_broker_check_performed':False,'final_flat_verified':False,
        'close_disposition':'MANUAL_INTERVENTION_REQUIRED','attention_required':True}
    try:
        if observation is None:
            snapshot=_broker_read(policy,clock,approval_ids=broker_approval_ids(book))
        else:
            ok,snapshot=observation
            if not ok:raise snapshot
        journal.ingest(snapshot)
        broker=journal.summary(as_of=clock().isoformat())
        close_conflict=False;close_fault=None
        try:
            book.reconcile(snapshot,quotes={},features={})
        except ValueError as exc:
            if str(exc)!='LIVE_RISK_EXECUTION_CONFLICT':raise
            # A conflict discovered at the close is still a durable account
            # fact. Record it before reporting anything, so a restart cannot
            # read a clean ledger.
            close_conflict=True
            from trading_infra.integrity_faults import ExecutionIntegrityFaults
            try:
                close_fault=ExecutionIntegrityFaults(book.db,
                    clock=getattr(book,'clock',None)).record(
                        source='execution_identity_conflict_at_close',
                        account_key=snapshot.get('account_key'),
                        evidence={'observed_at':snapshot.get('observed_at'),
                                  'orders':sorted(snapshot.get('orders') or {})})
            except (ValueError,KeyError,TypeError,OSError,sqlite3.Error):
                close_fault='RECORD_FAILED'
        capture_publication=publish_owner_observation(
            book,journal,policy,clock,snapshot)
        result['capture_publication']=capture_publication
        # Broker absence is not proof that an in-flight submission failed.
        # Preserve unknown/native-review refs for explicit reconciliation.
        recovery=book.recovery_status(snapshot)
        unsettled=[a['id'] for a in book.actions()
            if a['state'] in {'READY','RUNNING','UNKNOWN','WAITING_USER','REPORTED'} and a['id'] not in recovery['acknowledged_action_ids']]
        active_trades=[tid for tid,t in book.summary().get('state',{}).get('trades',{}).items()
            if t.get('phase')!='FLAT_VERIFIED' and tid not in recovery['acknowledged_trade_ids']]
        from martin_agentic.recovery import unresolved_write_intents
        external_unsettled=unresolved_write_intents(book.db,snapshot,
            account_mask='••••'+policy['account_suffix'],simulation=book.simulation)
        result.update(unresolved_intent_ids=unsettled,nonterminal_trade_ids=active_trades,
            unresolved_external_write_ids=external_unsettled,
            recovery=recovery,historical_manual_recovery=bool(recovery['acknowledged_trade_ids']))
        physically_flat=bool(not unsettled and not external_unsettled and not active_trades and not book.active_issues(snapshot) and broker.get('read_healthy') is True and not broker.get('blockers')
            and not broker['open_equity_orders'] and not broker['other_asset_open_orders']
            and not broker.get('pending_approvals',0)
            and all(number(p['quantity'])==0 for p in broker['positions'].values()))
        supported_scope_flat=physically_flat
        account_flat_authority=full_account_flat_authority(snapshot)
        broker_physically_flat=bool(supported_scope_flat and account_flat_authority)
        # Physical broker flatness is one fact. Releasing, and calling the
        # session healthy, needs the durable execution-integrity authority to
        # be readable and clear; a close never washes a fault away.
        from trading_infra.integrity_gate import read_integrity
        integrity=read_integrity(book.db,db_path=getattr(book,'db_path',None),
                                 clock=getattr(book,'clock',None))
        integrity_blocked=bool(integrity['blocked'])
        integrity_unknown=not integrity['known']
        flat=bool(broker_physically_flat and not integrity_blocked and not integrity_unknown)
        publication_valid=capture_publication_ok(capture_publication)
        if integrity_unknown:
            disposition='INTEGRITY_UNAVAILABLE_MANUAL_HANDOFF'
        elif integrity_blocked and supported_scope_flat:
            disposition='BROKER_FLAT_INTEGRITY_UNRESOLVED'
        elif integrity_blocked:
            disposition='INTEGRITY_FAULT_UNRESOLVED'
        elif not account_flat_authority:
            disposition=('SUPPORTED_SCOPE_FLAT_FULL_ACCOUNT_UNPROVEN'
                if supported_scope_flat else 'MANUAL_INTERVENTION_REQUIRED')
        elif not publication_valid:
            disposition='CURRENT_CAPTURE_PUBLICATION_INVALID_MANUAL_HANDOFF'
        else:
            disposition='SAFE_FLAT' if flat else 'MANUAL_INTERVENTION_REQUIRED'
        result.update(final_broker_check_performed=True,
            final_flat_verified=(flat and publication_valid),
            broker_physically_flat=broker_physically_flat,
            supported_scope_flat=supported_scope_flat,
            full_account_flat_proven=account_flat_authority,
            flat_scope=('WHOLE_ACCOUNT' if account_flat_authority
                        else 'SUPPORTED_SCOPE_ONLY'),
            broker=broker,observed_at=snapshot['observed_at'],
            integrity_blocked=integrity_blocked,
            integrity_gate_unavailable=integrity_unknown,
            integrity_reasons=integrity['reasons'],
            integrity_observed_at=integrity['observed_at'],
            close_integrity_conflict=close_conflict,
            close_integrity_fault=close_fault,
            close_disposition=disposition,
            current_capture_valid=publication_valid,
            attention_required=(not flat or not publication_valid))
    except READ_ERRORS as exc:
        result['closing_failure']=safe_read_failure(exc)
    return result


def merge_step_result(into, step):
    """Merge a step result without letting either side overwrite the other.

    Health is an AND (a step failure must not be masked by a healthy base read,
    and a step success must not relax the base gates) and attention is an OR.
    Every other field is taken from the step, which is the only side that knows
    about reconciliation, protection and settlement state.
    """
    if not isinstance(step, dict):
        raise ValueError('STEP_RESULT_REQUIRED')
    step_healthy = step.get('healthy', True)
    step_attention = bool(step.get('attention_required'))
    into.update({key: value for key, value in step.items()
                 if key not in ('healthy', 'attention_required', 'cycle')})
    into['healthy'] = bool(into.get('healthy', True)) and bool(step_healthy)
    into['attention_required'] = bool(into.get('attention_required')) or step_attention
    return into


def build_strategy_services(book,policy,*,source_fingerprint,clock,instance_supervisor=None,
                            admission=None,composition=None):
    """Resolve which strategy shape this session runs, and its account authority.

    Returns ``(instance_supervisor, admission)``. Either may be ``None``: no
    instances declared keeps the single legacy loop, and a session that never
    admits allocates no account authority. A caller may inject either one; the
    policy only fills in what was not supplied. Declared instances require an
    application composition; the generic core never guesses a concrete plugin.
    """
    if instance_supervisor is None and policy.get('strategy_instances') is not None:
        builder=getattr(composition,'build_instance_group',None)
        if not callable(builder):
            raise ValueError('STRATEGY_COMPOSITION_REQUIRED')
        instance_supervisor=builder(book,policy,
            source_fingerprint=source_fingerprint,clock=clock)
        if instance_supervisor is not None and admission is None:
            admission_builder=getattr(composition,'build_admission_stack',None)
            if not callable(admission_builder):
                raise ValueError('STRATEGY_COMPOSITION_REQUIRED')
            admission=admission_builder(book,policy,clock=clock)
    return instance_supervisor,admission


class _PortAdvanceDispatcher:
    """The queue-advance power of the execution port, as the loop's dispatcher."""

    def __init__(self,port):
        if not callable(getattr(port,'advance_queue',None)):
            raise ValueError('ADAPTER_QUEUE_ADVANCE_UNAVAILABLE')
        self.port=port

    def __call__(self,*,allow_entries=True,allow_dispatch=True):
        if not allow_dispatch:
            return {'agent_called':False,'reason':'FRESH_BROKER_OBSERVATION_REQUIRED'}
        return self.port.advance_queue(allow_entries=allow_entries)


class _DefaultPortBundle:
    def __init__(self, *, broker, market, execution=None, host=None,
                 worker_adapter=None, host_delivery_status=None):
        self.broker=broker;self.market=market;self.execution=execution;self.host=host
        self.worker_adapter=worker_adapter
        self.host_delivery_status=host_delivery_status


def default_live_ports(book,policy,clock,*,dispatcher=None,host_surface=None,
                       broker_surface=None,market_surface=None,worker_adapter=None):
    """Application composition: the concrete adapters a default session uses.

    This is the only place that knows the provider read/collection callables;
    the control loop itself only knows the port. A simulation session supplies
    its own marked callables here, so the production closures below are never
    built for it.
    """
    from martin_agentic.adapters import (BlockedHostDeliveryPort, BridgeHostDeliveryPort,
                           ProviderBrokerReadPort, ProviderExecutionPort,
                           ProviderMarketDataPort)
    if broker_surface is None:
        broker_surface=(lambda approval_ids=():
                        _broker_read(policy,clock,approval_ids=list(approval_ids)))
    if market_surface is None:
        # Resolve through the module global at call time so tests and future
        # composition can substitute the collector without touching the port.
        market_surface=lambda *args,**kwargs: collect_live_events(*args,**kwargs)
    if not callable(broker_surface) or not callable(market_surface):
        raise ValueError('PORT_SURFACE_CALLABLE_REQUIRED')
    broker=ProviderBrokerReadPort(broker_surface)
    market=ProviderMarketDataPort(market_surface)
    observation_bound=getattr(dispatcher,'requires_observation',False) is True
    host=None
    operate=None
    host_delivery_status=None
    if getattr(host_surface,'local_worker_event',False) is True:
        # A local event worker is not a targeted review/place Host delivery
        # port. Do not wrap it as one or silently replace it with Blocked.
        host_delivery_status={'status':'LOCAL_WORKER_EVENT_NOT_TARGETED_HOST',
            'available':False,'local_worker_event':True,
            'formal_adapter_qualified':False,
            'reason':'LOCAL_WORKER_EVENT_HAS_NO_TARGETED_HOST_OPERATION'}
    elif host_surface is not None:
        try:
            host=BridgeHostDeliveryPort(host_surface)
            if callable(host_surface) and hasattr(host_surface, 'book'):
                # The host bridge exposes target-aware request/receipt surfaces;
                # use its dispatcher only for the explicit named operation, never
                # for queue advancement or autonomous action selection.
                operate=HostDeliveryDispatcher(book, host, host_surface).operate
            host_delivery_status={'status':'HOST_TARGETED_DELIVERY_AVAILABLE',
                'available':True,'formal_adapter_qualified':False}
        except (AttributeError, ValueError, KeyError, TypeError, OSError, sqlite3.Error):
            host=BlockedHostDeliveryPort('HOST_TARGETED_DELIVERY_UNAVAILABLE')
            operate=None
            host_delivery_status={'status':'HOST_TARGETED_DELIVERY_UNAVAILABLE',
                'available':False,'formal_adapter_qualified':False,
                'reason':'HOST_TARGETED_DELIVERY_UNAVAILABLE'}
    execution=ProviderExecutionPort(
        advance=(lambda allow_entries=True: dispatcher(allow_entries=allow_entries))
        if callable(dispatcher) and not observation_bound else None,
        operate=operate)
    bundle=_DefaultPortBundle(broker=broker,market=market,execution=execution,
                              host=host,worker_adapter=worker_adapter,
                              host_delivery_status=host_delivery_status)
    bundle.default=True
    return bundle


class HostPortDispatcher:
    """Targeted host delivery for one claimed action, receipt by intent identity."""

    def __init__(self,book,host):
        if not callable(getattr(host,'deliver',None)) or not callable(getattr(host,'receipt',None)):
            raise ValueError('HOST_DELIVERY_PORT_REQUIRED')
        self.book=book;self.host=host;self.reported=[]

    def _intent_id(self,action):
        # An admitted entry owns the execution intent. Protective, exit and
        # cancel actions are distinct broker operations on the same trade, so
        # they get a stable action identity; reusing the entry intent would
        # collide in the Host delivery index and hide the management action.
        if action['kind'] != 'entry':
            return 'action-'+action['id']
        try:
            row=self.book.db.execute(
                'SELECT intent_id FROM execution_intents WHERE trade_id=?'
                ' ORDER BY created_at DESC LIMIT 1',(action['trade_id'],)).fetchone()
        except sqlite3.OperationalError:
            row=None
        return row[0] if row else 'action-'+action['id']

    def __call__(self,*,allow_entries=True,allow_dispatch=True):
        if not allow_dispatch:
            return {'agent_called':False,'reason':'FRESH_BROKER_OBSERVATION_REQUIRED'}
        action=self.book.claim(allow_entries=allow_entries)
        if not action:
            return {'agent_called':False,'reason':'NO_DISPATCHABLE_ACTION'}
        action_id=action.get('action_id') or action.get('id')
        intent_id=self._intent_id(action)
        delivery={'intent_id':intent_id,'action_id':action_id,
                  'trade_id':action['trade_id'],'kind':action['kind'],
                  'operation':'cancel' if action['kind']=='cancel' else 'place'}
        self.host.deliver(delivery)
        receipt=self.host.receipt(intent_id)
        if receipt is None:
            # Waiting is not acceptance. The action stays RUNNING and nothing is
            # reported as submitted or executed.
            self.reported.append({'action_id':action_id,'intent_id':intent_id,
                                  'status':'HOST_TOOL_RESULT_PENDING'})
            return {'agent_called':True,'action_id':action_id,
                    'intent_id':intent_id,'status':'HOST_TOOL_RESULT_PENDING',
                    'host_verified':False}
        if not isinstance(receipt,dict) or receipt.get('verified') is not True:
            # Unverified, mismatched, pending or UNKNOWN receipts never move the
            # action to a successful state.
            self.reported.append({'action_id':action_id,'intent_id':intent_id,
                                  'status':'HOST_RECEIPT_UNVERIFIED',
                                  'reason':(receipt or {}).get('reason')})
            return {'agent_called':True,'action_id':action_id,
                    'intent_id':intent_id,'status':'HOST_RECEIPT_UNVERIFIED',
                    'reason':(receipt or {}).get('reason'),
                    'host_verified':False}
        native=(receipt.get('native_result') or {}).get('report') or {}
        status=native.get('status')
        if status in {'submitted','rejected','pending_approval','unknown',
                      'needs_confirmation','review_only'}:
            self.book.report(action_id,{'status':status})
        self.reported.append({'action_id':action_id,'intent_id':intent_id,
                              'status':status,'host_verified':True})
        return {'agent_called':True,'action_id':action_id,
                'intent_id':intent_id,'status':status,'host_verified':True}


class LocalWorkerDispatcher:
    """Explicit public dispatcher for a claimed action to the local worker wake.

    It uses the existing ExecutionBook.claim/admission/lease path and the
    existing HostExecutionBridge/outbox; no action state is fabricated.
    """
    requires_observation = True
    local_worker_event = True

    def __init__(self, book, bridge, budget=None):
        if not callable(bridge) or not hasattr(bridge, 'enqueue_local_worker_action'):
            raise ValueError('LOCAL_WORKER_BRIDGE_REQUIRED')
        self.book = book
        self.bridge = bridge
        self.budget = budget

    @property
    def task(self):
        return self.bridge.task

    @property
    def pending(self):
        return getattr(self.bridge, 'pending', None)

    def close(self):
        return self.bridge.close()

    def __call__(self, *, snapshot=None, quotes=None, allow_entries=False,
                 allow_dispatch=True):
        # Consume prior results and settle the exact wake receipt without
        # claiming new work here. No new outbox event is reserved by settle.
        consumed = {'local_worker_results':
                    self.bridge.consume_local_worker_results(),
                    'local_worker_wake_settled':
                    self.bridge.settle_local_worker_wake()}
        if not allow_dispatch:
            return {**consumed, 'agent_called': False,
                    'reason': 'LOCAL_WORKER_DISPATCH_NOT_ALLOWED'}
        budget = None
        claim_entries = allow_entries
        entry_budget_blocked = False
        if self.budget is not None:
            budget = self.budget.status()
            if not budget['management_available']:
                return {**consumed, 'agent_called': False,
                        'reason': 'MODEL_BUDGET_EXHAUSTED_MANAGEMENT_PENDING',
                        'budget': budget, 'entry_budget_blocked': True,
                        'attention_required': False}
            # Entry limits gate the claim itself. Claiming first and then
            # failing reserve would create a false UNKNOWN with no dispatch.
            claim_entries = bool(allow_entries and budget['entries_available'])
            entry_budget_blocked = bool(allow_entries and not claim_entries)
        action = self.book.claim(allow_entries=claim_entries)
        if not action:
            return {**consumed, 'agent_called': False,
                    'reason': 'NO_DISPATCHABLE_ACTION',
                    'entry_budget_blocked': entry_budget_blocked,
                    'budget': budget}
        action_id = action.get('action_id') or action.get('id')
        current = next((a for a in self.book.actions()
                        if a.get('id') == action_id), None)
        if current is None or current.get('state') != 'RUNNING':
            return {**consumed, 'agent_called': False,
                    'reason': 'CLAIMED_ACTION_NOT_RUNNING'}
        if self.budget is not None:
            reservation_error = None
            try:
                reserved = self.budget.reserve(action)
            except (sqlite3.Error, TimeoutError, OSError) as exc:
                # A raise is not proof of rollback. Ask the durable budget
                # whether this exact action holds a reservation; only an
                # explicit false lets the local owner withdraw the claim.
                reserved = False
                reservation_error = type(exc).__name__
            if not reserved:
                lookup = getattr(self.budget, 'has_reservation', None)
                try:
                    has_reservation = (bool(lookup(action_id))
                                       if callable(lookup) else None)
                except (sqlite3.Error, OSError):
                    has_reservation = None
                if has_reservation is False:
                    proof = ('RESERVATION_ERROR_ROLLED_BACK'
                             if reservation_error and has_reservation is False
                             else 'RESERVATION_REJECTED_WITHOUT_RECORD')
                    reason = ('BUDGET_RESERVATION_ERROR_ROLLED_BACK'
                              if reservation_error and has_reservation is False
                              else 'BUDGET_RESERVATION_FAILED_NOT_DISPATCHED')
                    try:
                        settled = self.book.abandon_not_dispatched(
                            action_id, reason=reason, proof=proof)
                    except (ValueError, KeyError, TypeError, OSError, sqlite3.Error) as exc:
                        # The Book refused to withdraw on its own evidence.
                        # Do not leave a claimed action RUNNING and do not call
                        # it completed: fall back to the existing reconcile
                        # terminal and surface attention.
                        self.book.report(action_id, {
                            'status': 'unknown',
                            'message': 'NOT_DISPATCHED_TERMINAL_REJECTED:' + type(exc).__name__})
                        return {**consumed, 'agent_called': False,
                                'reason': 'NOT_DISPATCHED_TERMINAL_REJECTED',
                                'budget': self.budget.status(),
                                'attention_required': True}
                    return {**consumed, 'agent_called': False,
                            'reason': reason,
                            'not_dispatched': settled,
                            'budget': self.budget.status(),
                            'attention_required': False}
                # An exact reservation, or an unavailable answer, may mean a
                # prior owner already dispatched. Preserve UNKNOWN; never replay.
                message = ('BUDGET_RESERVATION_RACE_RECONCILE'
                           if has_reservation is True
                           else 'BUDGET_RESERVATION_OUTCOME_UNCERTAIN_RECONCILE')
                self.book.report(action_id, {'status': 'unknown', 'message': message})
                return {**consumed, 'agent_called': False,
                        'reason': 'BUDGET_RESERVATION_RACE_OR_FAILURE',
                        'budget': self.budget.status(),
                        'attention_required': True}
        wake = self.bridge.enqueue_local_worker_action(current)
        return {**consumed, 'agent_called': True, 'action_id': action_id,
                'local_worker_wake': wake,
                'budget': self.budget.status() if self.budget is not None else None,
                'execution_authorized': False, 'broker_effect_verified': False}


class HostDeliveryDispatcher:
    """Default supervised-host dispatcher through the HostDeliveryPort.

    The bridge remains the single native-operation claim authority and the
    single receipt importer. This dispatcher only selects an already-READY (or
    reviewed-awaiting-place) action without claiming it, asks the HostDeliveryPort
    to deliver the exact operation, ticks the bridge to prepare/import, and asks
    the port to verify the imported receipt by the same identity before it
    reports a verified host outcome. It never claims a second action while the
    bridge has an operation in flight.
    """

    requires_observation = True
    handles_intent_delivery = True
    _PRIORITY = {'protect': 0, 'cancel': 1, 'exit': 2, 'entry': 3}

    def __init__(self, book, host, bridge):
        if not callable(getattr(host, 'deliver', None)) or not callable(getattr(host, 'receipt', None)):
            raise ValueError('HOST_DELIVERY_PORT_REQUIRED')
        if not callable(bridge) or not hasattr(bridge, 'book'):
            raise ValueError('HOST_BRIDGE_REQUIRED')
        self.book = book
        self.host = host
        self.bridge = bridge
        self.delivery = None
        self.deliveries = []
        self.receipts = []
        # Old management requests used the entry intent as their identity.
        # A missing optional capability is distinct from a scan that failed:
        # the latter is unknown, never "no legacy work".
        self.legacy_deliveries = []
        self.legacy_error = None
        scan = getattr(host, 'legacy_management_deliveries', None)
        if callable(scan):
            try:
                self.legacy_deliveries = scan()
            except Exception as exc:
                self.legacy_error = (str(exc)
                                     if str(exc).startswith('LEGACY_HOST_IDENTITY_')
                                     else 'LEGACY_HOST_IDENTITY_SCAN_FAILED')
                self.legacy_deliveries = []
        self.legacy_block = bool(self.legacy_deliveries or self.legacy_error)

    @property
    def task(self):
        return self.bridge.task

    @property
    def pending(self):
        return getattr(self.bridge, 'pending', None)

    def close(self):
        return self.bridge.close()

    def _intent_id(self, action):
        # Entry owns the admitted execution intent; management operations keep
        # a distinct stable action identity so they cannot collide with it.
        if action['kind'] != 'entry':
            if action['state'] == 'WAITING_USER' and self._abandoned_read(action):
                # A restart with a new Host epoch cannot reuse the old binding
                # or request. The new read is still tied to the same action and
                # original abandoned operation for audit, without touching it.
                row = self.book.db.execute(
                    "SELECT id FROM native_operation_records WHERE action_id=?"
                    " AND operation='review' ORDER BY rowid DESC LIMIT 1",
                    (action['id'],)).fetchone()
                return 'action-' + action['id'] + '-review-refresh-' + row[0]
            return 'action-' + action['id']
        try:
            row = self.book.db.execute(
                'SELECT intent_id FROM execution_intents WHERE trade_id=?'
                ' ORDER BY created_at DESC LIMIT 1',
                (action['trade_id'],)).fetchone()
        except sqlite3.OperationalError:
            row = None
        return row[0] if row else 'action-' + action['id']

    def _status(self, value):
        return {**value, 'delivery_port': 'HostDeliveryPort',
                'legacy_deliveries': list(self.legacy_deliveries),
                'legacy_error': self.legacy_error,
                'deliveries': list(self.deliveries),
                'receipts': list(self.receipts)}

    def _tick(self, snapshot, quotes, allow_entries):
        # Request-file processing stays enabled; autonomous bridge review does
        # not, so only a HostDeliveryPort action can claim new work.
        return self.bridge(snapshot=snapshot, quotes=quotes,
                           allow_entries=allow_entries, allow_dispatch=True,
                           allow_autonomous_review=False)

    def _abandoned_read(self, action):
        # A review that was proven local-only may be refreshed after restart.
        # A place/cancel attempt, an UNKNOWN write, or any later write record
        # means the operation may have reached the broker and must stay blocked.
        try:
            if self.book.db.execute(
                    "SELECT 1 FROM native_operation_records WHERE action_id=?"
                    " AND operation IN ('place','cancel')", (action['id'],)).fetchone():
                return False
            if self.book.db.execute(
                    "SELECT 1 FROM external_operations WHERE action_id=?"
                    " AND operation IN ('place','cancel')", (action['id'],)).fetchone():
                return False
            row = self.book.db.execute(
                "SELECT id,state,result FROM native_operation_records"
                " WHERE action_id=? AND operation='review' ORDER BY rowid DESC LIMIT 1",
                (action['id'],)).fetchone()
        except sqlite3.OperationalError:
            return False
        if not row or row[1] != 'ABANDONED_READ':
            return False
        try:
            result = json.loads(row[2])
        except (TypeError, ValueError):
            return False
        return bool(result.get('source') == 'local_control'
                    and result.get('review_verified') is False
                    and result.get('requires_fresh_review') is True
                    and result.get('no_broker_response') is True)

    def _place_ready(self, action):
        if action['state'] != 'WAITING_USER':
            return False
        if self.book.db.execute(
                "SELECT 1 FROM native_operation_records WHERE action_id=?"
                " AND operation IN ('place','cancel')", (action['id'],)).fetchone():
            return False
        row = self.book.db.execute(
            "SELECT state,result FROM native_operation_records WHERE action_id=?"
            " AND operation='review' ORDER BY rowid DESC LIMIT 1",
            (action['id'],)).fetchone()
        if not row or row[0] != 'RECORDED':
            return False
        try:
            return json.loads(row[1]).get('review_verified') is True
        except (TypeError,ValueError):
            return False

    def _target_action(self, action_id):
        if not isinstance(action_id, str) or not action_id:
            raise ValueError('ADAPTER_ACTION_ID_REQUIRED')
        action = next((item for item in self.book.actions()
                       if item.get('id') == action_id), None)
        if action is None:
            raise ValueError('ADAPTER_TARGET_ACTION_UNKNOWN')
        retired = self.book.recovery_status().get('acknowledged_action_ids', [])
        if action_id in retired:
            raise ValueError('ADAPTER_TARGET_ACTION_RETIRED')
        return action

    def _resolve_cancel_action(self, ref_id):
        if not isinstance(ref_id, str) or not ref_id:
            raise ValueError('ADAPTER_CANCEL_REF_REQUIRED')
        exact = next((item for item in self.book.actions()
                      if item.get('id') == ref_id and item.get('kind') == 'cancel'), None)
        if exact is not None:
            if ref_id in self.book.recovery_status().get('acknowledged_action_ids', []):
                raise ValueError('ADAPTER_TARGET_ACTION_RETIRED')
            return exact
        matches = []
        for action in self.book.actions():
            if action.get('kind') != 'cancel':
                continue
            try:
                payload = json.loads(action.get('payload') or '{}')
            except (TypeError, ValueError):
                continue
            parameters = payload.get('parameters') or {}
            if ref_id in {payload.get('target_order_id'),
                          parameters.get('order_id'),
                          payload.get('ref_id')}:
                matches.append(action)
        if not matches:
            raise ValueError('ADAPTER_CANCEL_TARGET_UNKNOWN')
        if len(matches) != 1:
            raise ValueError('ADAPTER_CANCEL_TARGET_AMBIGUOUS')
        if matches[0].get('id') in self.book.recovery_status().get(
                'acknowledged_action_ids', []):
            raise ValueError('ADAPTER_TARGET_ACTION_RETIRED')
        return matches[0]

    def operate(self, action_id, operation, intent=None):
        """Perform one named review/place/cancel and never claim queue-next.

        The target must already exist in the owner Book. Review is limited to a
        READY action (or a proven local-only abandoned review), place to the
        exact reviewed WAITING_USER action, and cancel to one uniquely resolved
        cancel action. A snapshot and quote are required for any execution
        attempt; without them this only records a target delivery.
        """
        if operation not in {'review', 'place', 'cancel'}:
            raise ValueError('ADAPTER_OPERATION_INVALID')
        if self.legacy_block:
            raise ValueError(self.legacy_error or
                             'LEGACY_HOST_IDENTITY_REQUIRES_RECONCILIATION')
        intent = intent if isinstance(intent, dict) else {}
        if operation == 'cancel':
            ref_id = intent.get('ref_id') or action_id
            action = self._resolve_cancel_action(ref_id)
        else:
            action = self._target_action(action_id)
        state = action.get('state')
        if operation == 'review':
            if state != 'READY' and not (state == 'WAITING_USER'
                                         and self._abandoned_read(action)):
                raise ValueError('ADAPTER_TARGET_REVIEW_STATE_INVALID')
        elif operation == 'place':
            if not self._place_ready(action):
                raise ValueError('ADAPTER_TARGET_PLACE_REQUIRES_REVIEW')
            if action.get('kind') == 'entry' and intent.get('allow_entries') is not True:
                raise ValueError('ADAPTER_ENTRY_INTENT_REQUIRED')
        else:
            if action.get('kind') != 'cancel' or state != 'READY':
                raise ValueError('ADAPTER_TARGET_CANCEL_NOT_READY')
        if self.bridge.pending is not None:
            raise ValueError('HOST_OPERATION_IN_FLIGHT')
        intent_id = self._intent_id(action)
        delivery = {'intent_id': intent_id, 'action_id': action['id'],
                    'trade_id': action['trade_id'], 'kind': action['kind'],
                    'operation': operation}
        recorded = self.host.deliver(delivery)
        result = {'status': 'HOST_DELIVERY_RECORDED', 'targeted': True,
                  'action_id': action['id'], 'intent_id': intent_id,
                  'operation': operation, 'request_id': recorded.get('request_id'),
                  'execution_authorized': False, 'broker_effect_verified': False}
        snapshot = intent.get('snapshot')
        quotes = intent.get('quotes')
        if snapshot is None:
            return {**result, 'host_status': 'HOST_TOOL_RESULT_PENDING',
                    'snapshot_supplied': False,
                    'fresh_snapshot_verified': False}
        if not isinstance(quotes, dict):
            quotes = {}
        # Process only the request this operation just published. A general
        # bridge tick could advance an unrelated queued action B.
        import inspect
        try:
            signature = inspect.signature(self.bridge)
            parameters = signature.parameters.values()
            supports_target = ('target_request_id' in signature.parameters
                               or any(param.kind == inspect.Parameter.VAR_KEYWORD
                                      for param in parameters))
        except (TypeError, ValueError):
            supports_target = False
        if not supports_target:
            raise ValueError('ADAPTER_TARGETED_BRIDGE_REQUIRED')
        processed = self.bridge(
            snapshot=snapshot, quotes=quotes,
            allow_entries=bool(intent.get('allow_entries', False)),
            allow_dispatch=True, allow_autonomous_review=False,
            target_request_id=result['request_id'])
        bridge_status = (processed or {}).get('status') if isinstance(processed, dict) else None
        bridge_reason = (processed or {}).get('reason') if isinstance(processed, dict) else None
        receipt = self.host.receipt(intent_id)
        base = {**result, 'snapshot_supplied': True,
                'fresh_snapshot_verified': False,
                'bridge_status': bridge_status,
                'bridge_reason': bridge_reason}
        if receipt is None:
            if bridge_status in {'HOST_TOOL_RESULT_PENDING', 'HOST_PACKET_PREPARED',
                                 'HOST_HANDOFF_EXPIRED_RECONCILE',
                                 'HOST_REVIEW_ABANDONED'}:
                return {**base, 'host_status': 'HOST_TOOL_RESULT_PENDING',
                        'receipt_status': 'HOST_RECEIPT_PENDING'}
            return {**base, 'host_status': bridge_status or
                    'HOST_TARGET_EXECUTION_PENDING',
                    'receipt_status': 'HOST_RECEIPT_UNAVAILABLE',
                    'reason': bridge_reason or 'HOST_TOOL_RESULT_PENDING'}
        if not isinstance(receipt, dict):
            return {**base, 'host_status': 'HOST_RECEIPT_INVALID',
                    'receipt_status': 'HOST_RECEIPT_INVALID',
                    'verified': False, 'reason': 'HOST_RECEIPT_NOT_OBJECT'}
        reason = receipt.get('reason')
        receipt_status = receipt.get('status')
        if receipt.get('verified') is not True:
            return {**base, 'host_status': 'HOST_RECEIPT_UNVERIFIED',
                    'receipt_status': receipt_status or 'HOST_RECEIPT_UNVERIFIED',
                    'verified': False,
                    'reason': reason or 'HOST_RECEIPT_UNVERIFIED',
                    'receipt': receipt}
        if (receipt_status != 'HOST_RECEIPT_IMPORTED'
                or receipt.get('request_id') != result['request_id']
                or receipt.get('action_id') != action['id']
                or receipt.get('trade_id') != action.get('trade_id')):
            return {**base, 'host_status': 'HOST_RECEIPT_UNVERIFIED',
                    'receipt_status': receipt_status or 'HOST_RECEIPT_UNVERIFIED',
                    'verified': False,
                    'reason': reason or 'HOST_RECEIPT_IDENTITY_MISMATCH',
                    'receipt': receipt}
        return {**base, 'host_status': 'HOST_RECEIPT_IMPORTED',
                'receipt_status': receipt_status, 'verified': True,
                'receipt': receipt}

    def _select(self, snapshot, quotes, allow_entries):
        if self.legacy_block:
            return None, None, (self.legacy_error or
                                'LEGACY_HOST_IDENTITY_REQUIRES_RECONCILIATION')
        active = [a for a in self.book.actions()
                  if a['id'] not in self.book.recovery_status()['acknowledged_action_ids']]
        if any(a['state'] in {'RUNNING', 'UNKNOWN'} for a in active):
            return None, None, 'UNRESOLVED_ACTION'
        now = self.book.clock()
        for action in sorted(active, key=lambda a: (
                self._PRIORITY.get(a['kind'], 4), a['created_at'])):
            if action['kind'] == 'entry' and not allow_entries:
                continue
            try:
                payload = json.loads(action['payload'])
                if now >= timestamp(payload['expires_at']):
                    continue
            except (KeyError, TypeError, ValueError):
                continue
            # Native-tool operations are only review/place/cancel. A Book
            # entry/protect/exit action is reviewed first and then placed; only
            # a cancel action maps directly to a cancel operation.
            if action['state'] == 'READY':
                operation = 'cancel' if action['kind'] == 'cancel' else 'review'
            elif (action['state'] == 'WAITING_USER'
                  and self._abandoned_read(action)):
                operation = 'review'
            elif self._place_ready(action):
                operation = 'place'
            else:
                continue
            quote = (quotes or {}).get(payload.get('plan', {}).get('symbol'))
            try:
                self.book.validate_external_action(action['id'], snapshot, quote=quote)
            except (ValueError, KeyError, TypeError, OSError, sqlite3.Error):
                continue
            return action, operation, None
        return None, None, 'NO_DELIVERABLE_ACTION'

    def _verify(self, snapshot, quotes, allow_entries):
        delivery = self.delivery
        tick = self._tick(snapshot, quotes, allow_entries)
        receipt = self.host.receipt(delivery['intent_id'])
        if receipt is None:
            status = tick.get('status')
            if status in {'HOST_RECEIPT_INVALID_RECONCILE',
                          'HOST_RECEIPT_IDENTITY_UNVERIFIED',
                          'HOST_HANDOFF_EXPIRED_RECONCILE',
                          'HOST_REVIEW_ABANDONED'}:
                self.receipts.append({**delivery, 'verified': False,
                                      'reason': status,
                                      'bridge_status': status})
                self.delivery = None
                return self._status({**tick, 'host_verified': False,
                                     'host_status': status,
                                     'receipt_status': 'HOST_RECEIPT_UNAVAILABLE'})
            return self._status({**tick, 'host_verified': False,
                                 'host_status': 'HOST_TOOL_RESULT_PENDING',
                                 'receipt_status': 'HOST_TOOL_RESULT_PENDING'})
        if not isinstance(receipt, dict) or receipt.get('verified') is not True:
            reason = (receipt or {}).get('reason') if isinstance(receipt, dict) else None
            self.receipts.append({**delivery, 'verified': False,
                                  'reason': reason or 'HOST_RECEIPT_UNVERIFIED'})
            self.delivery = None
            return self._status({**tick, 'host_verified': False,
                                 'host_status': 'HOST_RECEIPT_UNVERIFIED',
                                 'receipt_status': 'HOST_RECEIPT_UNVERIFIED',
                                 'reason': reason or 'HOST_RECEIPT_UNVERIFIED'})
        native = (receipt.get('native_result') or {}).get('report') or {}
        action = next((a for a in self.book.actions()
                       if a['id'] == delivery['action_id']), None)
        self.receipts.append({**delivery, 'verified': True,
                              'native_status': native.get('status'),
                              'book_state': action['state'] if action else None})
        self.delivery = None
        return self._status({**tick, 'host_verified': True,
                             'host_status': native.get('status'),
                             'receipt_status': receipt.get('status'),
                             'action_id': delivery['action_id'],
                             'intent_id': delivery['intent_id'],
                             'operation': delivery['operation'],
                             'book_state': action['state'] if action else None})

    def __call__(self, *, snapshot=None, quotes=None, allow_entries=True,
                 allow_dispatch=True):
        if not allow_dispatch:
            return self._status({'agent_called': False,
                                 'reason': 'FRESH_BROKER_OBSERVATION_REQUIRED'})
        if self.delivery is not None:
            return self._verify(snapshot, quotes, allow_entries)
        first = self._tick(snapshot, quotes, allow_entries)
        if self.bridge.pending is not None:
            # A request already in flight (including a recovery request) keeps
            # the single claim; this dispatcher must not claim a second action.
            return self._status({**first, 'host_status': 'HOST_OPERATION_IN_FLIGHT'})
        action, operation, reason = self._select(snapshot, quotes, allow_entries)
        if action is None:
            return self._status({**first, 'agent_called': False, 'reason': reason})
        intent_id = self._intent_id(action)
        delivery = {'intent_id': intent_id, 'action_id': action['id'],
                    'trade_id': action['trade_id'], 'kind': action['kind'],
                    'operation': operation}
        recorded = self.host.deliver(delivery)
        record = {**delivery, 'request_id': recorded.get('request_id')}
        self.delivery = record
        self.deliveries.append(record)
        prepared = self._tick(snapshot, quotes, allow_entries)
        receipt = self.host.receipt(intent_id)
        if receipt is not None:
            return self._verify(snapshot, quotes, allow_entries)
        return self._status({**prepared, 'agent_called': True,
                             'host_verified': False,
                             'host_status': 'HOST_TOOL_RESULT_PENDING',
                             'receipt_status': 'HOST_TOOL_RESULT_PENDING',
                             **{k: delivery[k] for k in
                                ('intent_id', 'action_id', 'operation')}})


MAX_OBSERVATION_WAIT_SECONDS=20.0


def run_live_session(db_path,policy,*,symbols,cycles=1,poll_seconds=15,agent_mode='disabled',
                     output_dir='runtime/live-agent',status_file=None,clock=utc_now,
                     budget_entry_limit=20,budget_total_limit=40,entry_inhibit_file=None,
                     require_watchdog=False,watchdog_policy_path=None,until_close=False,
                     instance_supervisor=None,admission=None,accounting=None,ports=None,
                     simulation=False,simulation_sources=None,simulation_workspace=None,
                     source_mode=None,current_preflight=None,host_wake_adapter=None,
                     host_wake_adapter_factory=None,composition=None,
                     supervisor_class=None,cycle_observer=None,
                     observation_wait_seconds=0.0):
    if type(simulation) is not bool:
        raise ValueError('SIMULATION_FLAG_INVALID')
    if (type(observation_wait_seconds) not in (int, float)
            or observation_wait_seconds < 0
            or observation_wait_seconds != observation_wait_seconds):
        raise ValueError('INVALID_OBSERVATION_WAIT')
    if source_mode is not None and source_mode not in SOURCE_MODES:
        raise ValueError('SOURCE_MODE_INVALID')
    if source_mode=='real_read_only' and agent_mode in {'supervised-host','execute'}:
        raise ValueError('REAL_READ_ONLY_REQUIRES_DISABLED_AGENT')
    if simulation and policy.get('live_enabled') is True:
        raise ValueError('SIMULATION_CANNOT_ENABLE_LIVE')
    if not simulation and (simulation_sources is not None or simulation_workspace is not None):
        raise ValueError('SIMULATION_SURFACES_REQUIRE_SIMULATION')
    if simulation and agent_mode not in {'disabled','script-review','supervised-host'}:
        raise ValueError('SIMULATION_AGENT_MODE_NOT_SUPPORTED')
    if simulation and ports is not None:
        # A caller-supplied port bundle is the production seam; simulation gets
        # its ports only from the explicitly marked synthetic surfaces below.
        raise ValueError('SIMULATION_CUSTOM_PORTS_FORBIDDEN')
    if simulation and (host_wake_adapter is not None
                       or host_wake_adapter_factory is not None):
        raise ValueError('HOST_WAKE_ADAPTER_REQUIRES_REAL_SESSION')
    if host_wake_adapter is not None and host_wake_adapter_factory is not None:
        raise ValueError('HOST_WAKE_ADAPTER_AND_FACTORY_EXCLUSIVE')
    if (host_wake_adapter is not None or host_wake_adapter_factory is not None) \
            and agent_mode != 'supervised-host':
        raise ValueError('HOST_WAKE_ADAPTER_REQUIRES_SUPERVISED_HOST')
    if type(until_close) is not bool or not 1<=cycles<=10000 or not 5<=poll_seconds<=60 or agent_mode not in {'disabled','review','script-review','supervised-host','execute'}:
        raise ValueError('INVALID_LIVE_SESSION_PARAMETERS')
    if len(symbols)!=len(set(symbols)) or not 10<=len(symbols)<=30:
        raise ValueError('EXPLICIT_UNIQUE_10_TO_30_STOCK_POOL_REQUIRED')
    capture_only=policy.get('capture_only') is True
    if (observation_wait_seconds and not capture_only):
        raise ValueError('OBSERVATION_WAIT_REQUIRES_CAPTURE_ONLY')
    if observation_wait_seconds>MAX_OBSERVATION_WAIT_SECONDS:
        raise ValueError('OBSERVATION_WAIT_TOO_LARGE')
    if capture_only and agent_mode!='disabled':
        raise ValueError('CAPTURE_ONLY_REQUIRES_DISABLED_AGENT')
    from martin_agentic.engineering_profile import validate_profile_mode
    validate_profile_mode(policy,agent_mode,capture_only=capture_only)
    if agent_mode=='execute' and policy.get('live_enabled') is not True:
        raise ValueError('LIVE_WRITE_MODE_REQUIRES_ENABLED_POLICY_AND_NATIVE_CONFIRMATION')
    if require_watchdog and (not status_file or not watchdog_policy_path):
        raise ValueError('REQUIRED_WATCHDOG_NEEDS_BOUND_POLICY_AND_STATUS_PATH')
    wake_report=worker_adapter_report(None)
    simulation_paths=None;simulation_broker=None;simulation_market=None
    simulation_review=None;simulation_wake=None
    if simulation:
        from martin_agentic.simulation import (broker_surface, market_surface,
                                 validate_simulation_sources,
                                 validate_simulation_workspace)
        sources=validate_simulation_sources(simulation_sources)
        simulation_paths=validate_simulation_workspace(
            simulation_workspace,db_path=db_path,output_dir=output_dir,
            status_file=status_file,entry_inhibit_file=entry_inhibit_file,
            watchdog_policy_path=watchdog_policy_path)
        simulation_broker=broker_surface(sources)
        simulation_market=market_surface(sources)
        simulation_review=sources.review
        simulation_wake=sources.wake
        # No execution book, journal, lock, packet or status file is created
        # before every source and path above has passed validation.
        db_path=str(simulation_paths.db_path)
        output_dir=str(simulation_paths.output_dir)
        status_file=(str(simulation_paths.status_file)
                     if simulation_paths.status_file is not None else None)
        entry_inhibit_file=str(simulation_paths.entry_inhibit_file)
        if simulation_paths.watchdog_policy_path is not None:
            watchdog_policy_path=str(simulation_paths.watchdog_policy_path)
        from martin_agentic.simulation import create_simulation_workspace
        create_simulation_workspace(simulation_paths)
    with single_instance(db_path):
        book=ExecutionBook(db_path,policy,simulation=simulation,clock=clock,
                           current_preflight=current_preflight);journal=BrokerJournal(db_path,allow_synthetic=simulation)
        if current_preflight is None:
            from martin_agentic.current_context import ensure_default_preflight
            ensure_default_preflight(book,clock=clock)
        if source_mode is None:
            source_mode='synthetic' if (simulation or book.simulation) else 'real'
        if source_mode not in SOURCE_MODES:
            raise ValueError('SOURCE_MODE_INVALID')
        if source_mode=='real_read_only' and agent_mode in {'supervised-host','execute'}:
            # A real-source read-only shadow cannot gain a write-capable agent path.
            raise ValueError('REAL_READ_ONLY_REQUIRES_DISABLED_AGENT')
        worker=None if agent_mode in {'disabled','script-review','supervised-host'} else CodexOrderWorker(Path.cwd(),output_dir,review_only=agent_mode!='execute')
        budget=WorkerBudget(db_path,policy['session_date'],entry_limit=budget_entry_limit,total_limit=budget_total_limit)
        dispatcher=AsyncDispatcher(book,worker,output_dir,budget) if worker else None
        if agent_mode=='script-review':
            from martin_agentic.async_native_review import AsyncNativeReviewDispatcher
            dispatcher=AsyncNativeReviewDispatcher(
                book,Path(output_dir)/'native-reviews',
                transport_factory=simulation_review)
        if agent_mode=='supervised-host':
            from martin_agentic.host_execution import HostExecutionBridge
            selected_wake = (simulation_wake if simulation else host_wake_adapter)
            if selected_wake is None and host_wake_adapter_factory is not None:
                selected_wake = host_wake_adapter_factory(
                    book, output_dir=Path(output_dir), policy=policy)
            wake_report=worker_adapter_report(selected_wake)
            dispatcher=HostExecutionBridge(
                book,Path(output_dir)/'host-execution',
                review_factory=simulation_review,
                wake_adapter=selected_wake)
            if (getattr(selected_wake, 'capability', {}) or {}).get(
                    'local_worker_event') is True:
                dispatcher=LocalWorkerDispatcher(book, dispatcher, budget=budget)
        build=source_fingerprint()
        # A supplied instance group, or one declared by policy, replaces the
        # single legacy strategy loop for this session only. With neither, the
        # default daemon path is unchanged.
        instance_supervisor,admission=build_strategy_services(
            book,policy,source_fingerprint=build,clock=clock,
            instance_supervisor=instance_supervisor,admission=admission,
            composition=composition)
        exit_advisor=None
        if instance_supervisor is not None:
            specs_for=getattr(composition,'exit_specs_for',None)
            if not callable(specs_for):
                raise ValueError('STRATEGY_COMPOSITION_REQUIRED')
            plugin_ids=set()
            for instance_id in instance_supervisor.instance_ids():
                runtime=instance_supervisor.runtime(instance_id)
                plugin_ids.add(runtime.instance.manifest.plugin_id)
            specs=specs_for(plugin_ids)
            if specs:
                from trading_infra.exit_advisory import ExitAdvisoryService
                exit_advisor=ExitAdvisoryService(specs,timeout_seconds=1.5)
        if ports is None:
            ports=default_live_ports(book,policy,clock,dispatcher=dispatcher,
                host_surface=(dispatcher if agent_mode=='supervised-host' else None),
                broker_surface=simulation_broker,market_surface=simulation_market,
                worker_adapter=wake_report)
        if getattr(ports,'broker',None) is None:
            raise ValueError('BROKER_READ_PORT_REQUIRED')
        if getattr(ports,'market',None) is None:
            raise ValueError('MARKET_DATA_PORT_REQUIRED')
        requirements=None
        if instance_supervisor is not None:
            requirements=set()
            for instance_id in instance_supervisor.instance_ids():
                runtime=instance_supervisor.runtime(instance_id)
                requirements.update(runtime.instance.manifest.data_requirements)
            requirements=tuple(sorted(requirements))
        # The admission stack creates the one per-session accounting ledger on
        # the owner connection. The default runner must hand that same ledger
        # to the receipt importer; a missing required importer would let fills
        # settle in the Book without any per-instance accounting.
        if accounting is None and admission is not None:
            accounting=getattr(admission,'accounting',None)
        if instance_supervisor is not None and admission is not None and accounting is None:
            raise ValueError('INSTANCE_ACCOUNTING_REQUIRED')
        if agent_mode=='supervised-host' and dispatcher is not None:
            from martin_agentic.host_execution import HostExecutionBridge
            host_port=getattr(ports,'host',None)
            if (isinstance(dispatcher,HostExecutionBridge) and host_port is not None
                    and callable(getattr(host_port,'validate_raw_receipt',None))):
                # The port is the delivery authority; the bridge remains the
                # only native-operation claim and receipt importer. Its own
                # receipt gate must pass before NativeOperations can report.
                dispatcher.receipt_gate=host_port.validate_raw_receipt
                dispatcher=HostDeliveryDispatcher(book,host_port,dispatcher)
        worker_budget_entries=bool(worker is not None or
            getattr(dispatcher,'local_worker_event',False) is True)
        signals_factory=(getattr(composition,'signals',None)
            or getattr(composition,'signals_factory',None))
        process_strategy=(instance_supervisor is None and composition is not None
            and callable(getattr(composition,'build_strategy_queue',None)))
        supervisor_type=supervisor_class or LiveSupervisor
        supervisor=supervisor_type(book,worker=worker,dispatcher=dispatcher,agent_mode=agent_mode,owner_source=build,
            process_strategy=process_strategy,instance_supervisor=instance_supervisor,
            admission=admission,accounting=accounting,exit_advisor=exit_advisor,
            ports=ports,source_mode=source_mode,worker_budget=budget,
            worker_budget_entries=worker_budget_entries,
            signals_factory=signals_factory,
            exit_features=getattr(composition,'exit_features',None),
            feature_keys_for=getattr(composition,'feature_keys_for',None),
            strategy_composition=composition);result={};announced_issues=set()
        def owner_publish_after_reconcile(snapshot):
            return publish_owner_observation(book,journal,policy,clock,snapshot)
        supervisor.post_reconcile_publication=owner_publish_after_reconcile
        if dispatcher is None and getattr(ports,'host',None) is not None:
            dispatcher=HostPortDispatcher(book,ports.host)
        from martin_agentic.control_tasks import ReadTransportOwner
        market_task=BackgroundTask('market-read-only');broker_task=BackgroundTask('broker-read-only')
        broker_cancellation=ReadTransportOwner()
        market_cancellation=ReadTransportOwner()
        entry_inhibit_file=entry_inhibit_file or str(db_path)+'.entry-inhibit.json'
        broker_failures=0;market_paused=None;broker_retry_at=None;last_broker_failure=None
        def failed_broker_read(exc):
            nonlocal broker_failures,broker_retry_at,last_broker_failure
            broker_failures+=1;failure=safe_read_failure(exc);last_broker_failure=failure
            # A day-bound supervisor remains alive through classified transient
            # read outages. Retry only reads, with capped backoff; no dispatcher
            # consumes old facts. Auth/schema/unknown failures still hand off.
            # Bounded diagnostic callers retain their original three attempts.
            if until_close and failure['retryable'] and broker_failures>=3:
                delay=min(30,5*2**min(broker_failures-3,3))
                broker_retry_at=clock()+timedelta(seconds=delay)
            journal.failure(at=clock().isoformat(),reason=failure['code'])
            return failure,not failure['retryable'] or (broker_failures>=3 and not until_close)
        def wait_cycle(task,seconds):
            options={'strategy_supervisor':supervisor} if getattr(supervisor,'strategy_schedule_enabled',False) is True else {}
            _wait_with_facts(task,dispatcher,seconds,
                account_facts_supervisor=supervisor,**options)
        def wait_progress(seconds):
            _wait_with_facts(None,None,seconds,
                account_facts_supervisor=supervisor)
        def start_market_read():
            # Build the frontier in the SQLite owner. The background read gets a
            # detached, immutable view and cannot consume, dispatch or write.
            from martin_agentic.collection_view import build_collection_state, build_collection_view
            view=build_collection_view(supervisor)
            collection_state=build_collection_state(supervisor)
            market_task.start(ports.market.collect_events,
                FrozenSignals(supervisor.signals.state(),view=view,
                              state_view=collection_state),
                symbols=symbols,requirements=requirements,clock=clock,
                cancellation=market_cancellation,
                capture_context={'db':str(Path(db_path).resolve()),'session_date':policy['session_date'],
                    'policy_sha256':digest(policy),'source_fingerprint':build,'symbols':symbols})
        def publish():
            result.update(health_scope=health_scope(result),
                operational_healthy=observation_healthy(result))
            if status_file:
                try:
                    final=Path(status_file);final.parent.mkdir(parents=True,exist_ok=True)
                    temp=final.with_suffix('.tmp')
                    temp.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
                    temp.replace(final)
                    # The runner never reads its own status back: observers do.
                    # Verifying the bytes parse catches a truncated/corrupt write
                    # here instead of letting a later failure be blamed on it.
                    json.loads(final.read_text())
                except (OSError,ValueError,sqlite3.Error):
                    # A status-file failure must never mask the reason the
                    # session is stopping, and must never look healthy.
                    result['status_persistence_failed']=True
                    result['status_persistence_reason']='STATUS_FILE_WRITE_OR_READBACK_FAILED'
                    result['healthy']=False
                    result['attention_required']=True
                    result.setdefault('stop_disposition',
                        'STATUS_PERSISTENCE_FAILED_MANUAL_HANDOFF_REQUIRED')
        def finish_tick():
            # A cycle that never reached a fresh owner step still needs a
            # diagnostic, but it must be missing rather than a prior green
            # status carried across an in-flight/backoff/error cycle.
            if 'account_facts' not in result:
                unrefreshed=getattr(
                    supervisor,'account_facts_unrefreshed_status',None)
                try:
                    value=(unrefreshed() if callable(unrefreshed) else None)
                except Exception:
                    value=None
                if isinstance(value,dict):
                    result['account_facts']=value
            # This pure advisory progress also runs while broker reads/backoff
            # are in flight. It cannot dispatch using an old account snapshot.
            if getattr(supervisor,'strategy_schedule_enabled',False) is True:
                progress=supervisor.advance_strategy();result['strategy_queue']=progress
                if progress.get('errors'):
                    result.setdefault('errors',[]).extend(progress['errors'])
                    result.update(healthy=False,attention_required=True,new_entries_blocked=True)
            result['control_heartbeat_at']=clock().isoformat()
            if dispatcher:
                result['worker_task']=dispatcher.task.status()
                if result.get('dispatch',{}).get('attention_required'):
                    result.update(attention_required=True,healthy=False,new_entries_blocked=True)
            try:
                current_issues=set(book.active_issues())
            except (ValueError,KeyError,TypeError,OSError,sqlite3.Error):
                # The book cannot be read. Say the evidence is unknown instead
                # of raising over the failure that got us here.
                current_issues=set()
                result['book_evidence_available']=False
                result.update(healthy=False,attention_required=True)
            if current_issues:result.update(healthy=False,new_entries_blocked=True)
            attention=bool(current_issues or result.get('attention_required') or result.get('market_failure'))
            result['attention_required']=attention
            dispatch_status=result.get('dispatch',{})
            waiting=dispatch_status.get('status') in {'WAITING_USER','CANCEL_REQUIRES_NATIVE_CONFIRMATION',
                'HOST_PACKET_PREPARED','HOST_TOOL_RESULT_PENDING','HOST_HANDOFF_EXPIRED_RECONCILE'}
            notice_key=('NATIVE_CONFIRMATION:'+str(dispatch_status.get('action_id'))+':'+str(dispatch_status.get('operation_id'))
                if waiting else result.get('reason'))
            issue_keys=current_issues|({notice_key} if attention else set())
            if issue_keys-announced_issues:
                notify_local('有交易动作需要按券商规则正式确认，请查看参数、预览披露与状态；尚未由脚本提交或撤单。'
                    if waiting else '实时会话存在数据或持仓管理异常，请检查状态；未继续新增风险。')
                announced_issues.update(issue_keys)
            publish();print(json.dumps(result,ensure_ascii=False),flush=True)
        def close_handoff():
            result.update(stopped=True,reason='CLOSE_BROKER_CONFIRMATION_DEADLINE_EXCEEDED',
                healthy=False,attention_required=True,final_flat_verified=False,
                final_broker_check_performed=False,close_disposition='MANUAL_INTERVENTION_REQUIRED',
                new_entries_blocked=True)
            notify_local('收盘后仍未取得新鲜完整账户核对，未宣称空仓；保留独立监护，需要人工检查。')
            publish()
        last_completed_observation_at=None
        last_completed_observation_healthy=None
        last_consumed_account_at=None
        last_consumed_market_at=None
        last_consumed_events=()
        last_consumed_market_optional=False
        latest_observation_state='NOT_ATTEMPTED'
        latest_observation_error=None
        latest_market_state='NOT_ATTEMPTED'
        latest_market_error=None
        cleanup_ok=True
        try:
            recovery=book.recover()
            recovery['worker_receipts']=inspect_worker_receipts(book,output_dir)
            startup_capture_recovery=None
            if isinstance(policy.get('current_capture'),dict):
                try:
                    from martin_agentic.live_capture_bridge import SavedBundleObservationBridge
                    startup_capture_recovery=SavedBundleObservationBridge(
                        policy,clock).recover(journal,book=book)
                except (ValueError,KeyError,TypeError,OSError,RuntimeError,sqlite3.Error) as exc:
                    startup_capture_recovery={
                        'status':'FAILED',
                        'reason':getattr(exc,'code',None) or str(exc),
                        'entry_allowed':False}
            completed_observation_cycles=0
            for cycle in (count() if until_close else range(cycles)):
                now=clock();market=session(now.isoformat());fatal=False
                cycle_had_observation=False
                cycle_observation_completed=False
                previous_consumed_account_at=last_consumed_account_at
                next_cycle=until_close or cycle+1<cycles
                result={'healthy':False,'cycle':cycle+1,'running':True,'pid':os.getpid(),
                    'agent_mode':agent_mode,'source_fingerprint':build,'new_entries_blocked':True,'db':str(Path(db_path).resolve()),
                    'watchdog_required':require_watchdog,
                    'read_only_capture':capture_only,'synthetic_session':bool(simulation),
                    'capture_recovery':startup_capture_recovery,
                    'source_mode':source_mode,'production_authorized':False,
                    'session_run_mode':'until_authorized_close' if until_close else 'bounded_cycles',
                    'close_confirmation_grace_seconds':30 if until_close else None,
                    'capture_idle_seconds':min(poll_seconds,5) if capture_only else None,
                    'execution_gate':'REVIEW_ONLY_UNTIL_NATIVE_CONFIRMATION_AND_PROTECTION_PATH_VERIFIED',
                    'worker_adapter':wake_report,
                    'worker_budget_entry_scope':('local_model_worker' if worker_budget_entries else 'not_consumed_by_selected_delivery'),
                    'startup_recovery':recovery}
                latest_observation_state='NOT_ATTEMPTED'
                latest_observation_error=None
                latest_market_state='NOT_ATTEMPTED'
                latest_market_error=None
                if not market or market['date']!=policy['session_date']:
                    result.update(stopped=True,reason='OUTSIDE_AUTHORIZED_SESSION',attention_required=True)
                    publish();break
                if broker_retry_at is not None and now<broker_retry_at and now<market['close']:
                    result.update(reason='BROKER_RECONNECT_BACKOFF',failure=last_broker_failure,
                        consecutive_read_failures=broker_failures,retry_at=broker_retry_at.isoformat(),
                        attention_required=True,agent_called=False,broker_snapshot_reused_for_dispatch=False)
                    finish_tick()
                    if next_cycle:_wait_with_facts(broker_task,None,min(poll_seconds,5),
                        account_facts_supervisor=supervisor,
                        **({'strategy_supervisor':supervisor} if getattr(supervisor,'strategy_schedule_enabled',False) is True else {}))
                    continue
                if broker_task.thread is None:
                    # Only detached JSON and approval IDs cross this boundary.
                    # No book, journal, SQLite connection or live strategy owner.
                    broker_task.start(ports.broker.snapshot,
                        approval_ids=list(broker_approval_ids(book)))
                broker_task.thread.join(timeout=.01)
                broker_observation=broker_task.take()
                if broker_observation is None and observation_wait_seconds and capture_only:
                    # Bounded read-only runs need each configured cycle to
                    # contain a real observation. A fresh process has no
                    # prefetched read, so wait for this cycle's owned read
                    # instead of spending the cycle on an empty in-flight tick.
                    if broker_task.thread is not None:
                        broker_task.thread.join(timeout=observation_wait_seconds)
                    broker_observation=broker_task.take()
                result['broker_task']=broker_task.status()
                if broker_observation is None:
                    latest_observation_state='IN_FLIGHT'
                    latest_observation_error={'code':'BROKER_COLLECTION_IN_FLIGHT','retryable':True}
                    if until_close and now>=market['close']+timedelta(seconds=30):
                        close_handoff();break
                    broker=journal.summary(as_of=clock().isoformat())
                    result.update(reason='BROKER_COLLECTION_IN_FLIGHT',broker=broker,
                        observed_at=broker.get('last_observed_at'),
                        attention_required=broker.get('read_healthy') is not True,
                        read_only_capture_healthy=False if capture_only else None,
                        broker_snapshot_reused_for_dispatch=False,model_budget=budget.status())
                    if dispatcher:
                        result['dispatch']=dispatcher(allow_entries=False,allow_dispatch=False)
                    finish_tick()
                    if next_cycle:
                        # Market results are consumed only after a new account
                        # observation. A ready market mailbox must not spin this
                        # branch or exhaust bounded cycles while broker is slow.
                        wait_cycle(broker_task,min(poll_seconds,5))
                    continue
                if now>=market['close']:
                    if broker_observation[0] and timestamp(broker_observation[1]['observed_at'])<market['close']:
                        latest_observation_state='STALE_BEFORE_CLOSE'
                        latest_observation_error={'code':'BROKER_SNAPSHOT_BEFORE_CLOSE','retryable':True}
                        if until_close and now>=market['close']+timedelta(seconds=30):
                            close_handoff();break
                        result.update(reason='CLOSE_REQUIRES_FRESH_BROKER_OBSERVATION',attention_required=True)
                        finish_tick()
                        if next_cycle:wait_progress(min(poll_seconds,5))
                        continue
                    if broker_observation[0]:
                        last_consumed_account_at=broker_observation[1].get('observed_at')
                        latest_observation_state='SUCCESS'
                    else:
                        latest_observation_state='ERROR'
                        latest_observation_error=safe_read_failure(broker_observation[1])
                    latest_market_state='OPTIONAL_CLOSE'
                    last_consumed_market_optional=True
                    closing=_closing_check(book,journal,policy,clock,observation=broker_observation)
                    result.update(closing,stopped=True,reason='AUTHORIZED_SESSION_CLOSED',healthy=closing['final_flat_verified'])
                    if closing['attention_required']:notify_local('收盘核对未证实空仓且无残留订单，需要人工处理。')
                    publish();break
                if now<market['open']:
                    try:
                        # Authenticate and reconcile before the bell. Waiting for
                        # the regular session must not mean waiting to discover
                        # stale credentials, unknown writes or overnight exposure.
                        ok,snapshot=broker_observation
                        if not ok:
                            latest_observation_state='ERROR'
                            latest_observation_error=safe_read_failure(snapshot)
                            raise snapshot
                        latest_observation_state='SUCCESS'
                        latest_observation_error=None
                        latest_market_state='OPTIONAL_PREOPEN'
                        journal.ingest(snapshot);broker_failures=0;broker_retry_at=None
                        managed=supervisor.step(snapshot,[],allow_entries=False,allow_dispatch=False)
                        capture_publication=getattr(supervisor,'capture_publication_result',None)
                        capture_publication=(capture_publication
                            if isinstance(capture_publication,dict) else None)
                        publication_ok=capture_publication_ok(capture_publication)
                        last_consumed_account_at=snapshot.get('observed_at')
                        last_consumed_events=()
                        last_consumed_market_optional=True
                        broker=journal.summary(as_of=clock().isoformat())
                        if broker['reconciliation_issues']:raise ValueError('BROKER_RECONCILIATION_ISSUES')
                        retirement=book.recovery_status(snapshot)
                        unsettled=[a['id'] for a in book.actions()
                            if a['state'] in {'READY','RUNNING','UNKNOWN','WAITING_USER','REPORTED'}
                            and a['id'] not in retirement['acknowledged_action_ids']]
                        reasons=[]
                        from martin_agentic.recovery import unresolved_write_intents
                        external_unsettled=unresolved_write_intents(book.db,snapshot,
                            account_mask='••••'+policy['account_suffix'],simulation=book.simulation)
                        if external_unsettled:reasons.append('PREOPEN_UNRESOLVED_EXTERNAL_WRITE_REQUIRES_ATTENTION')
                        if broker.get('read_healthy') is not True:reasons.append('BROKER_READ_UNHEALTHY')
                        if broker.get('blockers') or book.active_issues(snapshot):reasons.append('RECONCILIATION_REQUIRES_ATTENTION')
                        if any(number(p['quantity'])!=0 for p in broker.get('positions',{}).values()):reasons.append('PREOPEN_POSITION_REQUIRES_ATTENTION')
                        if broker.get('open_equity_orders',0) or broker.get('other_asset_open_orders',0):reasons.append('PREOPEN_OPEN_ORDER_REQUIRES_ATTENTION')
                        if broker.get('pending_approvals',0):reasons.append('PREOPEN_APPROVAL_REQUIRES_ATTENTION')
                        if not publication_ok:reasons.append('CURRENT_CAPTURE_PUBLICATION_REQUIRES_ATTENTION')
                        if unsettled:reasons.append('PREOPEN_UNRESOLVED_INTENT_REQUIRES_ATTENTION')
                        result.update(observed_at=snapshot['observed_at'],broker=broker,
                            capture_publication=capture_publication,
                            healthy=not reasons,attention_required=bool(reasons),
                            reason='WAITING_FOR_REGULAR_SESSION')
                        merge_step_result(result,managed)
                        if managed.get('integrity_gate_unavailable'):
                            fatal=True
                            result['stop_disposition']=managed.get('stop_disposition') or (
                                'EXECUTION_INTEGRITY_FAULT_LEDGER_UNAVAILABLE_MANUAL_HANDOFF_REQUIRED')
                        result['capture_observation_healthy']=not reasons
                        if managed.get('reconciled') is False:
                            # Management did not continue, so the label must not
                            # claim it did; it names the actual blocker.
                            result['reason']='EXECUTION_RECONCILIATION_REQUIRED_DURING_PREOPEN'
                            # The observation can be fine while the session as a
                            # whole is not: capture health is the conjunction.
                            result['capture_healthy']=False
                        else:
                            result['capture_healthy']=not reasons
                        result.update(
                            preopen_attention_reasons=reasons,unresolved_intent_ids=unsettled,
                            unresolved_external_write_ids=external_unsettled,
                            read_only_capture_healthy=(not reasons
                                and managed.get('reconciled') is not False)
                                if capture_only else None,
                            new_entries_blocked=True,
                            watchdog_guard={'required':require_watchdog,'ready':None,'scope':'NO_PREOPEN_ENTRY_ADMISSION'})
                    except READ_ERRORS as exc:
                        failure,fatal=failed_broker_read(exc)
                        latest_observation_state='ERROR'
                        latest_observation_error=failure
                        latest_market_state='OPTIONAL_PREOPEN'
                        try:
                            execution=book.summary()
                        except (ValueError,KeyError,TypeError,OSError,sqlite3.Error):
                            execution=None
                            result['book_evidence_available']=False
                        result.update(reason='PREOPEN_BROKER_OR_LIFECYCLE_UNAVAILABLE',failure=failure,
                            execution=execution,agent_called=False,attention_required=True,healthy=False)
                else:
                    # Once the regular session is active, market evidence is
                    # required; a preopen optional-market label cannot carry
                    # over into a market-read failure.
                    last_consumed_market_optional=False
                    try:
                        # Account observation remains available when the market-data pool fails.
                        ok,snapshot=broker_observation
                        if not ok:
                            latest_observation_state='ERROR'
                            latest_observation_error=safe_read_failure(snapshot)
                            raise snapshot
                        latest_observation_state='SUCCESS'
                        latest_observation_error=None
                        journal.ingest(snapshot);broker_failures=0;broker_retry_at=None
                        events=[];data={};market_failure=market_paused
                        market_consumed_ok=False
                        if market_paused is not None:
                            latest_market_state='ERROR';latest_market_error=market_paused
                        if market_paused is None:
                            if market_task.thread is None:
                                start_market_read()
                            # A tiny bounded scheduling grace is independent of collection duration.
                            market_task.thread.join(timeout=.01)
                            completed=market_task.take()
                            if (completed is None and capture_only
                                    and observation_wait_seconds):
                                # The market result belongs to this cycle too.
                                # Wait for the already-started bounded task
                                # rather than recording an empty/in-flight market
                                # observation as healthy.
                                if market_task.thread is not None:
                                    market_task.thread.join(
                                        timeout=observation_wait_seconds)
                                completed=market_task.take()
                            if completed:
                                ok,value=completed
                                if ok:
                                    events,data=value;market_consumed_ok=True
                                    latest_market_state='SUCCESS';latest_market_error=None
                                else:
                                    market_failure=safe_read_failure(value)
                                    latest_market_state='ERROR';latest_market_error=market_failure
                                    if market_failure['code']=='INTERACTIVE_AUTH_OR_APPROVAL_REQUIRED':market_paused=market_failure
                            else:
                                market_failure={'code':'MARKET_COLLECTION_IN_FLIGHT','retryable':True}
                                latest_market_state='IN_FLIGHT';latest_market_error=market_failure
                        result['market_task']=market_task.status()
                        if market_failure and market_failure.get('code')!='MARKET_COLLECTION_IN_FLIGHT':
                            observed=clock().isoformat()
                            diagnostic={'id':'capture-failure-'+digest([observed,market_failure,build]),'at':observed,
                                'kind':'collection_status','data':market_failure,'synthetic_input':book.simulation,
                                'source_sha256':digest(market_failure),'source_type':'local_control_diagnostic'}
                            record_capture_disposition(book,diagnostic,reason='MARKET_COLLECTION_FAILED')
                        # Long history calls cannot turn an old account read into execution evidence.
                        if (clock()-timestamp(snapshot['observed_at'])).total_seconds()>20:
                            for event in events:record_capture_disposition(book,event,reason='BROKER_SNAPSHOT_TOO_OLD')
                            raise TransportError('BROKER_SNAPSHOT_SUPERSEDED')
                        # A slow read may cross the close; never dispatch on the old boundary.
                        if clock()>=market['close']:
                            if timestamp(snapshot['observed_at'])<market['close']:
                                latest_observation_state='STALE_BEFORE_CLOSE'
                                latest_observation_error={'code':'BROKER_SNAPSHOT_BEFORE_CLOSE','retryable':True}
                                for event in events:record_capture_disposition(book,event,reason='CROSS_CLOSE_REQUIRES_FRESH_BROKER')
                                result.update(reason='CLOSE_REQUIRES_FRESH_BROKER_OBSERVATION',attention_required=True)
                                finish_tick()
                                if next_cycle:wait_progress(min(poll_seconds,5))
                                continue
                            last_consumed_account_at=snapshot.get('observed_at')
                            latest_observation_state='SUCCESS';latest_observation_error=None
                            latest_market_state='OPTIONAL_CLOSE'
                            last_consumed_market_optional=True
                            closing=_closing_check(book,journal,policy,clock,observation=(True,snapshot))
                            result.update(closing,stopped=True,reason='AUTHORIZED_SESSION_CLOSED',healthy=closing['final_flat_verified'])
                            if closing['attention_required']:notify_local('收盘核对未证实空仓且无残留订单，需要人工处理。')
                            publish();break
                        # Executable quotes can expire while the account is refreshed. Keep
                        # clock-based/protective management, without handing stale prices to it.
                        fresh_events=[]
                        for event in events:
                            if event['kind']=='quote' and not all(0<=(clock()-timestamp(event['data'][key])).total_seconds()<=15
                                    for key in ('bid_at','ask_at','observed_at')):
                                market_failure={'code':'MARKET_QUOTE_STALE','retryable':True}
                                latest_market_state='ERROR';latest_market_error=market_failure
                                record_capture_disposition(book,event,reason='MARKET_QUOTE_STALE')
                                continue
                            fresh_events.append(event)
                        events=fresh_events
                        broker=journal.summary(as_of=clock().isoformat())
                        if broker['reconciliation_issues']:raise ValueError('BROKER_RECONCILIATION_ISSUES')
                        from martin_agentic.watchdog import entry_inhibit,watchdog_entry_admission,inhibit_new_entries
                        guardian={'required':False,'ready':None,'scope':'UNMANAGED_DIRECT_DIAGNOSTIC'}
                        if require_watchdog:
                            guardian=watchdog_entry_admission(db_path,status_file,policy,
                                policy_path=watchdog_policy_path,build_fingerprint=build,
                                control_pid=os.getpid(),clock=clock)
                            if not guardian['ready']:
                                try:inhibit_new_entries(db_path,guardian['issues'],inhibit_file=entry_inhibit_file,clock=clock)
                                except OSError:
                                    guardian['issues'].append('WATCHDOG_ENTRY_INHIBIT_WRITE_FAILED')
                                    # Complete the current protective/reconciliation
                                    # step, then require operator handoff. A later
                                    # healthy observer must not erase a failure to
                                    # persist the sticky risk inhibit.
                                    fatal=True
                                    result['stop_disposition']='WATCHDOG_INHIBIT_PERSISTENCE_FAILED_MANUAL_HANDOFF_REQUIRED'
                        inhibit=entry_inhibit(entry_inhibit_file,db_path)
                        model_budget=budget.status()
                        admission=book.entry_gate() if not book.simulation and not capture_only else {'blocked':False,'reason':'NON_LIVE_ENTRY_SCOPE'}
                        guardian_ready=not require_watchdog or guardian['ready']
                        observer_healthy=guardian_ready and market_failure is None and broker.get('read_healthy') is True and not book.active_issues(snapshot)
                        allow_entries=not capture_only and observer_healthy and not inhibit['blocked'] and not admission['blocked'] and (agent_mode in {'script-review','supervised-host'} or model_budget['entries_available'])
                        result.update(entry_inhibit=inhibit,entry_admission=admission,watchdog_guard=guardian,model_budget=model_budget,
                            # Capture-only never admits risk. Keep its entry
                            # inhibit intact without treating that separate
                            # control as a failed read-only observation.
                            attention_required=(not guardian_ready or (not capture_only
                                and (inhibit['blocked'] or admission['blocked']))),
                            read_only_capture_healthy=observer_healthy if capture_only else None)
                        last_consumed_account_at=snapshot.get('observed_at')
                        if market_consumed_ok:
                            # A failed/in-flight latest market read does not
                            # erase the last successfully consumed evidence;
                            # it makes current observation health false while
                            # evidence freshness may remain true.
                            last_consumed_market_at=clock().isoformat()
                            last_consumed_events=tuple(events)
                            last_consumed_market_optional=False
                        step_result=supervisor.step(snapshot,events,allow_entries=allow_entries)
                        capture_publication=getattr(supervisor,'capture_publication_result',None)
                        capture_publication=(capture_publication
                            if isinstance(capture_publication,dict) else None)
                        publication_ok=capture_publication_ok(capture_publication)
                        base_reason=('WATCHDOG_UNAVAILABLE_MANAGING_EXISTING' if not guardian_ready
                            else 'MARKET_DATA_DEGRADED_MANAGING_EXISTING' if market_failure
                            else 'PREDECESSOR_LINEAGE_BLOCKED_MANAGING_EXISTING' if admission['blocked']
                            else 'READ_ONLY_CAPTURE_CYCLE_COMPLETED' if capture_only else 'LIVE_CYCLE_COMPLETED')
                        result.update(observed_at=snapshot['observed_at'],data=data,broker=broker,
                            capture_publication=capture_publication,
                            healthy=allow_entries,market_failure=market_failure,reason=base_reason)
                        merge_step_result(result,step_result)
                        if not publication_ok:
                            result.update(healthy=False,attention_required=True,
                                new_entries_blocked=True,
                                capture_publication_failed=True)
                        if step_result.get('integrity_gate_unavailable'):
                            # Without a durable fault the next process would read
                            # a clean ledger and resume. Stop and hand off.
                            fatal=True
                            result['stop_disposition']=step_result.get('stop_disposition') or (
                                'EXECUTION_INTEGRITY_FAULT_LEDGER_UNAVAILABLE_MANUAL_HANDOFF_REQUIRED')
                        if step_result.get('reconciled') is False:
                            # Reconciliation is the authoritative failure here; a
                            # read-level label would hide it, and management did
                            # not continue so the label must not say it did.
                            result['reason']='EXECUTION_RECONCILIATION_REQUIRED_ENTRIES_BLOCKED'
                            result['capture_observation_healthy']=bool(observer_healthy)
                            result['capture_healthy']=False
                        if result.get('errors'):
                            result.update(healthy=False,attention_required=True,
                                read_only_capture_healthy=False if capture_only else None)
                            if not step_result.get('reconciled') is False:
                                result['reason']='STRATEGY_INPUT_REJECTED_MANAGING_EXISTING'
                            if any(error.get('reason')=='ENTRY_INHIBIT_PERSISTENCE_FAILED' for error in result['errors']):
                                fatal=True
                                result['stop_disposition']='ENTRY_INHIBIT_PERSISTENCE_FAILED_MANUAL_HANDOFF_REQUIRED'
                        cycle_market_ok=bool(market_consumed_ok
                            and market_failure is None
                            and latest_market_state=='SUCCESS')
                        cycle_new_broker=True
                        if not book.simulation and previous_consumed_account_at:
                            try:
                                cycle_new_broker=bool(
                                    timestamp(snapshot.get('observed_at'))
                                    > timestamp(previous_consumed_account_at))
                            except (TypeError, ValueError, OverflowError):
                                cycle_new_broker=False
                        cycle_observation_completed=bool(
                            cycle_market_ok and cycle_new_broker
                            and publication_ok and not result.get('errors'))
                        result['cycle_observation_completed']=cycle_observation_completed
                        result['cycle_market_state']=latest_market_state
                        result['cycle_market_consumed']=market_consumed_ok
                        result['cycle_new_broker_observation']=cycle_new_broker
                        if cycle_observation_completed:
                            cycle_had_observation=True
                        last_completed_observation_at=snapshot.get('observed_at')
                        last_completed_observation_healthy=(result.get('read_only_capture_healthy')
                            if capture_only else result.get('healthy'))
                        # Overlap the next read-only market request with the
                        # next account observation. Consumption still occurs
                        # only below a new successful account read, followed by
                        # the original quote-age, guardian and entry gates.
                        # Never start unused bounded work, retry auth here, or
                        # begin a new request after crossing the close.
                        if (next_cycle and market_task.thread is None
                                and market_failure is None and not result.get('errors')
                                and (capture_only or agent_mode in {'script-review','supervised-host'})
                                and market['open']<=clock()<market['close']):
                            start_market_read()
                            result.update(market_prefetch_started=True,market_task=market_task.status())
                    except READ_ERRORS as exc:
                        failure,fatal=failed_broker_read(exc)
                        latest_observation_state='ERROR'
                        latest_observation_error=failure
                        try:
                            execution=book.summary()
                        except (ValueError,KeyError,TypeError,OSError,sqlite3.Error):
                            execution=None
                            result['book_evidence_available']=False
                        result.update(reason='BROKER_OR_LIFECYCLE_UNAVAILABLE',failure=failure,
                            execution=execution,agent_called=False,attention_required=True,healthy=False)
                if cycle_observation_completed:
                    completed_observation_cycles+=1
                finish_tick()
                if cycle_observer is not None:
                    # The observer receives a detached snapshot of the cycle
                    # that just ended. A failure to record the observation is
                    # itself an unhealthy cycle: the run may not report a
                    # completed cycle it could not journal.
                    try:
                        cycle_observer({'cycle':cycle+1,'configured_cycles':cycles,
                            'until_close':bool(until_close),'at':clock().isoformat(),
                            'result':dict(result),'fatal':bool(fatal)})
                    except (ValueError,OSError,TypeError,RuntimeError) as exc:
                        result.update(healthy=False,attention_required=True,
                            cycle_observation_failed=True,
                            cycle_observation_error=type(exc).__name__)
                        fatal=True
                if fatal:break
                if next_cycle:wait_cycle(market_task,min(poll_seconds,5) if capture_only else poll_seconds)
        finally:
            shutdown_deadline=time.monotonic()+READ_SHUTDOWN_TOTAL_SECONDS
            result['read_shutdown_budget_seconds']=READ_SHUTDOWN_TOTAL_SECONDS
            try:
                result['broker_read_shutdown']=broker_task.status()
                try:
                    broker_drain=_drain_read_task(broker_task,broker_cancellation,
                        deadline_monotonic=shutdown_deadline)
                    result['broker_read_cleanup']=dict(broker_drain['status'])
                    result['broker_read_cleanup'].update(
                        drained=broker_drain['drained'],
                        drain_grace_seconds=broker_drain['drain_grace_seconds'],
                        cancel=broker_drain.get('cancel'),
                        cleanup=broker_drain.get('cleanup'),
                        resource=broker_drain.get('resource'))
                    if not broker_drain['drained']:
                        cleanup_ok=False
                        result.update(attention_required=True,
                            stop_disposition='READ_ONLY_BROKER_READ_IN_FLIGHT_NO_CLOSE_PROOF')
                    else:
                        broker_result=broker_drain.get('result')
                        if broker_result and not broker_result[0]:
                            if str(broker_result[1])=='READ_ONLY_COLLECTION_STOPPED':
                                result['broker_read_cleanup']['result_error']={
                                    'code':'READ_ONLY_COLLECTION_STOPPED','retryable':False}
                            else:
                                failure=safe_read_failure(broker_result[1])
                                result['broker_read_cleanup']['result_error']=failure
                                cleanup_ok=False
                                result.update(attention_required=True,
                                    stop_disposition='READ_ONLY_BROKER_READ_CLEANUP_ERROR_NO_CLOSE_PROOF')
                except READ_ERRORS:
                    cleanup_ok=False
                    result.update(attention_required=True,broker_read_cleanup={'failed':True},
                        stop_disposition='READ_ONLY_BROKER_CLEANUP_FAILED_NO_CLOSE_PROOF')
                result['market_read_shutdown']=market_task.status()
                market_result_ready_before_stop=not market_task.mailbox.empty()
                try:
                    market_drain=_drain_read_task(market_task,market_cancellation,
                        deadline_monotonic=shutdown_deadline)
                    result['market_read_cleanup']=dict(market_drain['status'])
                    result['market_read_cleanup'].update(
                        drained=market_drain['drained'],
                        drain_grace_seconds=market_drain['drain_grace_seconds'],
                        cancel=market_drain.get('cancel'),
                        cleanup=market_drain.get('cleanup'),
                        resource=market_drain.get('resource'))
                    if not market_drain['drained']:
                        cleanup_ok=False
                        result.update(healthy=False,attention_required=True,
                            market_read_cleanup_incomplete=True,
                            stop_disposition='READ_ONLY_MARKET_READ_IN_FLIGHT_NO_CLOSE_PROOF')
                    else:
                        unconsumed=market_drain.get('result')
                        if unconsumed and unconsumed[0]:
                            abandoned_events,_=unconsumed[1]
                            for event in abandoned_events:
                                record_capture_disposition(book,event,reason='SESSION_STOPPED_BEFORE_CONSUMPTION')
                            result['market_shutdown_events_archived']=len(abandoned_events)
                        elif unconsumed:
                            failure=({'code':'READ_ONLY_COLLECTION_STOPPED','retryable':False}
                                if str(unconsumed[1])=='READ_ONLY_COLLECTION_STOPPED'
                                else safe_read_failure(unconsumed[1]))
                            observed=clock().isoformat()
                            diagnostic={'id':'market-shutdown-'+digest([observed,failure,build]),'at':observed,
                                'kind':'collection_status','data':{**failure,
                                    'result_ready_before_stop':market_result_ready_before_stop,'shutdown_requested':True},
                                'synthetic_input':book.simulation,'source_sha256':digest(failure),
                                'source_type':'local_control_diagnostic'}
                            record_capture_disposition(book,diagnostic,reason='MARKET_SHUTDOWN_RESULT')
                            result['market_shutdown_failure']=diagnostic['data']
                            if market_result_ready_before_stop or failure['code']!='READ_ONLY_COLLECTION_STOPPED':
                                cleanup_ok=False
                                result.update(healthy=False,attention_required=True)
                except READ_ERRORS:
                    cleanup_ok=False
                    result.update(healthy=False,attention_required=True,market_read_cleanup={'failed':True})
                if getattr(supervisor,'instance_supervisor_enabled',False) is True:
                    try:result['strategy_shutdown']=supervisor.instance_supervisor.close()
                    except (ValueError,OSError,sqlite3.Error):
                        result.update(healthy=False,attention_required=True,strategy_shutdown={'failed':True})
                elif getattr(supervisor,'strategy_process_enabled',False) is True:
                    try:result['strategy_shutdown']=supervisor.strategy_queue.close()
                    except (ValueError,OSError,sqlite3.Error):
                        result.update(healthy=False,attention_required=True,strategy_shutdown={'failed':True})
                account_facts_close=getattr(supervisor,'close',None)
                if callable(account_facts_close):
                    try:
                        shutdown=account_facts_close()
                    except (ValueError,OSError,RuntimeError,sqlite3.Error):
                        shutdown={'failed':True}
                    if isinstance(shutdown,dict):
                        result['account_facts_shutdown']=shutdown
                if dispatcher:
                    result['worker_shutdown']=dispatcher.close()
                    if result['worker_shutdown'].get('attention_required'):
                        result.update(healthy=False,attention_required=True)
                journal.failure(at=clock().isoformat(),reason='LIVE_SUPERVISOR_STOPPED')
                result['last_completed_observation_at']=last_completed_observation_at
                result['last_completed_observation_healthy']=last_completed_observation_healthy
                if 'broker_close_or_flat_proof' not in result:
                    result['broker_close_or_flat_proof']=bool(
                        result.get('final_flat_verified') is True
                        and result.get('final_broker_check_performed') is True
                        and result.get('close_disposition')=='SAFE_FLAT')
                close_proof=bool(result.get('broker_close_or_flat_proof'))
                # Evidence freshness and observation health are distinct:
                # old evidence can still be inside its age limit after the
                # latest read failed, but the current observation is not
                # healthy.  Only a successful latest account/market
                # observation plus fresh consumed evidence and completed
                # cleanup qualifies.  Entry-baseline/watchdog attention is
                # deliberately not part of this read-level conjunction.
                stopped_now=clock()
                current_fresh=_current_observation_fresh(
                    stopped_now,last_consumed_account_at,last_consumed_market_at,
                    last_consumed_events,
                    market_optional=bool(last_consumed_market_optional or close_proof))
                observation_ok=latest_observation_state=='SUCCESS'
                market_ok=latest_market_state in {
                    'SUCCESS','OPTIONAL_PREOPEN','OPTIONAL_CLOSE'}
                current_healthy=bool(current_fresh and observation_ok
                                     and market_ok and cleanup_ok)
                result['current_observation_evidence_fresh']=current_fresh
                result['current_observation_healthy']=current_healthy
                result['current_observation_evidence']={
                    'account_at':last_consumed_account_at,
                    'market_at':last_consumed_market_at,
                    'event_count':len(last_consumed_events),
                    'market_optional':bool(last_consumed_market_optional or close_proof),
                    'market_close_proof':close_proof,
                    'latest_observation_state':latest_observation_state,
                    'latest_observation_error':latest_observation_error,
                    'latest_market_state':latest_market_state,
                    'latest_market_error':latest_market_error,
                    'cleanup_ok':cleanup_ok,
                }
                if capture_only:
                    result['read_only_capture_healthy']=bool(
                        result.get('read_only_capture_healthy')) and current_healthy
                if not current_healthy:
                    if not current_fresh:
                        current_reason='CURRENT_OBSERVATION_MISSING_OR_STALE'
                    elif not cleanup_ok:
                        current_reason='CURRENT_OBSERVATION_CLEANUP_FAILED'
                    elif not observation_ok:
                        current_reason=('CURRENT_OBSERVATION_READ_ERROR'
                            if latest_observation_state in {'ERROR','STALE_BEFORE_CLOSE'}
                            else 'CURRENT_OBSERVATION_NOT_COMPLETED')
                    elif not market_ok:
                        current_reason=('CURRENT_OBSERVATION_MARKET_ERROR'
                            if latest_market_state=='ERROR'
                            else 'CURRENT_OBSERVATION_MARKET_NOT_COMPLETED')
                    else:
                        current_reason='CURRENT_OBSERVATION_UNHEALTHY'
                    result['current_observation_reason']=current_reason
                if not current_healthy and not close_proof:
                    result.update(healthy=False,attention_required=True)
                result['completed_observation_cycles']=completed_observation_cycles
                result['configured_cycles']=cycles
                result['bounded_capture_completed']=bool(
                    not until_close and result.get('cycle')==cycles and cleanup_ok
                    and completed_observation_cycles==cycles)
                if 'close_disposition' not in result:
                    state=book.summary().get('state',{})
                    recovery=book.recovery_status()
                    has_active=any(t.get('phase')!='FLAT_VERIFIED' and tid not in recovery['acknowledged_trade_ids'] for tid,t in state.get('trades',{}).items())
                    result['historical_manual_recovery']=bool(recovery['acknowledged_trade_ids'])
                    broker=result.get('broker',{})
                    exposure=any(number(p['quantity'])!=0 for p in broker.get('positions',{}).values())
                    if has_active or exposure or broker.get('open_equity_orders',0) or broker.get('pending_approvals',0):
                        result.update(attention_required=True,stop_disposition='MANAGEMENT_STOPPED_MANUAL_HANDOFF_REQUIRED')
                        notify_local('有未结束持仓或订单的管理会话已停止，需要人工接管。')
                result.update(running=False,stopped_at=clock().isoformat(),bounded_session_stopped=True)
                publish()
            finally:
                capture_config=policy.get('current_capture') if isinstance(policy,dict) else None
                if isinstance(capture_config,dict) and isinstance(capture_config.get('root'),str):
                    try:
                        from martin_agentic.live_capture_bridge import release_staging_owner
                        release_staging_owner(capture_config['root'])
                    except (ValueError,OSError,TypeError,RuntimeError):
                        result.update(attention_required=True,
                            staging_owner_release='FAILED')
                budget.close();journal.close();book.close()
        return result|{'running':False,'bounded_session_stopped':True,'continuous_live_execution_qualified':False}
