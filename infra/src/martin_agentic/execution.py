"""Durable event outbox and real-observation-driven, single-position lifecycle.

No broker write endpoint is called by this module. Agent reports are advisory;
only complete fresh broker observations can establish fills/protection/flatness.
"""
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import json
from pathlib import Path
import re
import sqlite3
import uuid

from .audit import AuditTrail, canonical, digest
from .broker import TERMINAL, valid_session_order, APPROVAL_NO_ORDER_TERMINAL
from .calendar import is_regular, session
from .ledger import timestamp
from .risk import number
from .live_risk import LiveRiskGuard
from .reconciliation import matches_order_identity,check_order_terms
from .trial import trial_proposal
from .transport import TransportError
from .entry_authority import DurableEntryAuthority
from .engineering_preflight import NoCurrentPreflight
from .assessment_receipt import (
    receipt_of, read_book_authority, verify_receipt_binding)


def utc_now():return datetime.now(timezone.utc)


def _safe_payload(action):
    try:
        value=json.loads(action.get('payload') or '{}')
    except ValueError:
        return {}
    return value if isinstance(value,dict) else {}


class ExecutionBook:
    def __init__(self,path,policy,*,simulation=False,clock=None,exit_decider=None,
                 current_preflight=None,formal_authority=None,
                 supervised_engineering_authority=None):
        if 'capture_only' in policy and type(policy['capture_only']) is not bool:
            raise ValueError('INVALID_CAPTURE_ONLY_POLICY')
        self.clock=clock or utc_now
        self.exit_decider=exit_decider
        self.policy=policy
        self.simulation=simulation
        self._current_preflight=current_preflight or NoCurrentPreflight()
        self._current_account_facts = {}
        from .formal_authority import FormalAuthority
        if formal_authority is not None and not isinstance(
                formal_authority, FormalAuthority):
            raise ValueError('FORMAL_AUTHORITY_INVALID')
        if formal_authority is None:
            formal_authority = getattr(
                self._current_preflight, 'formal_authority', None)
        self.formal_authority = formal_authority
        if (formal_authority is not None
                and getattr(self._current_preflight, 'formal_authority', None)
                is None and hasattr(self._current_preflight, 'formal_authority')):
            self._current_preflight.formal_authority = formal_authority
        if supervised_engineering_authority is not None:
            from .supervised_engineering import SupervisedEngineeringAuthority
            if not isinstance(supervised_engineering_authority,
                              SupervisedEngineeringAuthority):
                raise ValueError('SUPERVISED_ENGINEERING_AUTHORITY_INVALID')
        self.supervised_engineering_authority = supervised_engineering_authority
        self._supervised_entry_context = None
        self._entry_operation_contexts = {}
        # Kept independently of the SQLite handle: a failing database must not
        # also hide where the side-car risk gates live.
        self.db_path=str(Path(path))
        self.db=sqlite3.connect(Path(path),timeout=10)
        self._entry_authority=DurableEntryAuthority(
            self.db,clock=self.clock,allow_synthetic_no_namespace=simulation,
            current_facts_reader=self._read_current_account_facts,
            current_facts_validator=self._validate_current_account_facts,
            supervised_permit_validator=self._supervised_entry_permit,
            supervised_baseline_required=(
                supervised_engineering_authority is not None))
        self.db.executescript('''CREATE TABLE IF NOT EXISTS live_state(id INTEGER PRIMARY KEY CHECK(id=1),payload TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS live_actions(id TEXT PRIMARY KEY,trade_id TEXT NOT NULL,kind TEXT NOT NULL,
        state TEXT NOT NULL,payload TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL,report TEXT);
        CREATE TABLE IF NOT EXISTS live_signals(id TEXT PRIMARY KEY,fingerprint TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS external_operations(operation_id TEXT PRIMARY KEY,action_id TEXT NOT NULL,
            operation TEXT NOT NULL,created_at TEXT NOT NULL,report TEXT,broker_ref_id TEXT);
        CREATE TABLE IF NOT EXISTS external_broker_refs(ref_id TEXT PRIMARY KEY,action_id TEXT NOT NULL,
            operation_id TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS external_broker_order_ids(order_id TEXT PRIMARY KEY,action_id TEXT NOT NULL,
            operation_id TEXT NOT NULL,account_key TEXT NOT NULL);''')
        # The generic execution owner, not a strategy initializer, owns the
        # capture archive schema. A fresh read-only book with no strategy must
        # still be able to record rejected/unconsumed capture dispositions.
        from .capture_archive import ensure_archive_schema
        ensure_archive_schema(self.db)
        self.db.commit()
        self.audit=AuditTrail(connection=self.db,clock=self.clock)
        if not self.db.execute('SELECT 1 FROM live_state').fetchone():
            self.db.execute('INSERT INTO live_state VALUES(1,?)',(canonical({'policy_hash':digest(policy),
                'simulation':simulation,'trades':{},'issues':[],'account_key':None,'last_snapshot':None}),));self.db.commit()
        state=self.state()
        if state['policy_hash']!=digest(policy) or state['simulation']!=simulation:
            self.db.close();raise ValueError('EXECUTION_POLICY_OR_MODE_CHANGED')
        self.risk_guard=LiveRiskGuard(Path(path).with_name('account-risk-'+policy['account_suffix']+'.sqlite')
            if not simulation else ':memory:',policy,simulation=simulation)

    @property
    def entry_authority(self):return self._entry_authority

    @property
    def current_preflight(self):return self._current_preflight

    def current_account_assessment(self):
        """Return the last owner-produced facts assessment; never authorizes."""
        if None in self._current_account_facts:
            return self._current_account_facts[None]
        if len(self._current_account_facts) == 1:
            return next(iter(self._current_account_facts.values()))
        return None

    def _read_current_account_facts(self, *, purpose=None):
        value = self._current_account_facts.get(purpose)
        if value is None and purpose is not None:
            value = self._current_account_facts.get(None)
        return value

    def current_account_facts(self, *, purpose=None):
        """Owner-cached current facts; never a caller-supplied assessment."""
        return self._read_current_account_facts(purpose=purpose)

    def clear_current_account_facts(self, *, purpose=None):
        """Discard one purpose's facts so a failed current refresh cannot reuse it."""
        if purpose is None:
            self._current_account_facts.clear()
        else:
            self._current_account_facts[purpose] = None

    def _validate_current_account_facts(self, assessment, *, purpose=None):
        validator = getattr(self._current_preflight,
                            'validate_account_assessment', None)
        if not callable(validator):
            return False, 'CURRENT_ACCOUNT_FACTS_VALIDATOR_MISSING'
        try:
            return validator(assessment, book=self, purpose=purpose)
        except Exception:
            return False, 'CURRENT_ACCOUNT_FACTS_VALIDATOR_FAILED'

    def _begin_supervised_entry_prevalidation(self, action):
        """Create one process-local, one-shot promotion context.

        It is deliberately not a permit ledger: it lives only for the current
        ``promote_supervised_entry`` call, is cleared in a finally block, and
        cannot be read by claim or a later operation.
        """
        self._clear_supervised_entry_context()
        now = self.clock()
        context = {
            'book_id': id(self), 'db_path': self.db_path,
            'account_key': self.state().get('account_key'),
            'policy_sha256': digest(self.policy),
            'action_id': action.get('id'),
            'payload_sha256': digest(action.get('payload')),
            'authority_id': id(self.supervised_engineering_authority),
            'purpose': 'engineering',
            'created_at': now,
            'not_before': now,
            'validated_at': None,
            'last_seen_at': now,
            'expires_at': now + timedelta(seconds=10),
        }
        self._supervised_entry_context = context
        return context

    def _clear_supervised_entry_context(self):
        self._supervised_entry_context = None

    def _begin_entry_operation_context(self, action, operation):
        now = self.clock()
        self._entry_operation_contexts.setdefault(action.get('id'), {})[
            operation] = {
            'book_id': id(self), 'db_path': self.db_path,
            'account_key': self.state().get('account_key'),
            'policy_sha256': digest(self.policy),
            'action_id': action.get('id'),
            'payload_sha256': digest(action.get('payload')),
            'authority_id': id(self.supervised_engineering_authority),
            'operation': operation, 'purpose': 'engineering',
            'created_at': now,
            'not_before': now,
            'validated_at': None,
            'last_seen_at': now,
            'expires_at': now + timedelta(seconds=10),
        }
        return self._entry_operation_contexts[action.get('id')][operation]

    def _clear_entry_operation_contexts(self):
        self._entry_operation_contexts = {}

    @staticmethod
    def _entry_context_moment(value):
        if isinstance(value, datetime):
            moment = value
        elif isinstance(value, str):
            try:
                moment = datetime.fromisoformat(value)
            except (TypeError, ValueError, OverflowError):
                return None
        else:
            return None
        if moment.tzinfo is None:
            return None
        return moment.astimezone(timezone.utc)

    def _entry_context_window(self, result, action, created_at):
        """Return the shortest already-validated right window, never a new one.

        The process-local context may only cache rights the authority just
        verified.  It never extends a permit/request/facts/review/source/
        receipt/action deadline, and never refreshes after a slow read.  Any
        present but malformed window fails the context closed.
        """
        created = self._entry_context_moment(created_at)
        if created is None:
            return None
        deadline = created + timedelta(seconds=10)
        not_before = created
        bad = False
        try:
            payload = json.loads(action.get('payload'))
        except (TypeError, ValueError):
            return None
        if not isinstance(payload, dict):
            return None

        def add_end(value):
            nonlocal deadline, bad
            moment = self._entry_context_moment(value)
            if moment is None:
                bad = True
                return
            deadline = min(deadline, moment)

        def add_start(value):
            nonlocal not_before, bad
            if value is None:
                return
            moment = self._entry_context_moment(value)
            if moment is None:
                bad = True
                return
            not_before = max(not_before, moment)

        if 'expires_at' in payload:
            add_end(payload.get('expires_at'))
        permit = result.get('permit') or {}
        request = result.get('request') or {}
        facts = result.get('facts') or {}
        review = result.get('review') or {}
        evidence = result.get('real_source_evidence') or {}
        receipt = result.get('assessment_receipt') or result.get('receipt') or {}
        if not isinstance(permit, dict) or not isinstance(request, dict) \
                or not isinstance(facts, dict) or not isinstance(review, dict) \
                or not isinstance(evidence, dict) or not isinstance(receipt, dict):
            return None
        for value in (permit.get('valid_until'), request.get('valid_until'),
                      facts.get('observation_valid_until'),
                      evidence.get('valid_until'),
                      evidence.get('risk_valid_until'),
                      evidence.get('expires_at'),
                      receipt.get('valid_until'), receipt.get('expires_at'),
                      receipt.get('observation_valid_until')):
            if value is not None:
                add_end(value)
        recorded = review.get('recorded_at')
        if recorded is not None:
            moment = self._entry_context_moment(recorded)
            if moment is None:
                return None
            add_end(moment + timedelta(seconds=30))
            add_start(moment)
        for value in (permit.get('not_before'), request.get('not_before'),
                      facts.get('observed_at'), evidence.get('observed_at'),
                      receipt.get('observed_at')):
            add_start(value)
        if bad or deadline <= not_before:
            return None
        return {'not_before': not_before, 'expires_at': deadline}

    def _touch_entry_context_seen(self, context):
        now = self._entry_context_moment(self.clock())
        if now is None:
            return
        prior = self._entry_context_moment(context.get('last_seen_at'))
        if prior is None or now >= prior:
            context['last_seen_at'] = now

    def _entry_context_problem(self, context, action, payload_sha, *,
                               operation=None):
        now = self._entry_context_moment(self.clock())
        created = self._entry_context_moment(context.get('created_at'))
        validated = self._entry_context_moment(context.get('validated_at'))
        not_before = self._entry_context_moment(context.get('not_before'))
        expires = self._entry_context_moment(context.get('expires_at'))
        last_seen = self._entry_context_moment(context.get('last_seen_at'))
        if any(value is None for value in
               (now, created, validated, not_before, expires, last_seen)):
            return 'ENTRY_AUTHORITY_SUPERVISED_PERMIT_CONTEXT_INVALID'
        horizon = created + timedelta(seconds=10)
        if (validated < created or now < created or now < validated
                or now < not_before or now < last_seen
                or not_before > validated or expires <= validated
                or expires <= not_before or expires > horizon
                or now >= expires
                or context.get('book_id') != id(self)
                or context.get('db_path') != self.db_path
                or context.get('account_key') != self.state().get('account_key')
                or context.get('policy_sha256') != digest(self.policy)
                or context.get('action_id') != action.get('id')
                or context.get('payload_sha256') != payload_sha
                or context.get('authority_id') !=
                id(self.supervised_engineering_authority)
                or context.get('purpose') != 'engineering'
                or (operation is not None
                    and context.get('operation') != operation)
                or context.get('operation') not in
                {None, 'claim', 'review', 'place'}
                or not isinstance(context.get('permit_id'), str)
                or not isinstance(context.get('binding'), str)
                or not isinstance(context.get('request_sha256'), str)
                or len(context.get('request_sha256')) != 64
                or not isinstance(context.get('source_fingerprint'), str)
                or not isinstance(context.get('facts_assessment_id'), str)):
            return 'ENTRY_AUTHORITY_SUPERVISED_PERMIT_CONTEXT_INVALID'
        return None

    def _use_entry_operation_context(self, action, payload_sha, operation):
        context = self._entry_operation_contexts.get(action.get('id'), {}).get(
            operation)
        if not isinstance(context, dict):
            return {'allowed': False,
                    'reason': 'ENTRY_AUTHORITY_SUPERVISED_PERMIT_NOT_PREVALIDATED'}
        self._touch_entry_context_seen(context)
        problem = self._entry_context_problem(
            context, action, payload_sha, operation=operation)
        if problem is not None:
            return {'allowed': False, 'reason': problem}
        return {'allowed': True,
                'reason': 'SUPERVISED_ENGINEERING_PERMIT_VERIFIED',
                'permit_id': context['permit_id'],
                'binding': context['binding'],
                'permit_verified': True, 'cached': True,
                'entry_authorized': False, 'production_ready': False}

    def _supervised_entry_permit(self, *, action, purpose, source=None):
        """Connect the exact supervised permit to the outer entry gate.

        The entry authority supplies its already-checked facts decision; this
        callback only re-validates the same owner/source/permit binding through
        the configured authority.  It creates no grant, consumes nothing and
        cannot authorize a place.  Real books require the explicit local
        promotion stage; simulation remains the isolated test path.
        """
        if purpose != 'engineering':
            return {'allowed': False, 'reason': None}
        authority = self.supervised_engineering_authority
        if authority is None:
            return {'allowed': False, 'reason': None}
        if (self.simulation is False
                and getattr(authority, 'real_activation_enabled', False)
                is not True):
            return {'allowed': False,
                    'reason': 'SUPERVISED_ENGINEERING_REAL_ACTIVATION_DISABLED'}
        if not isinstance(action, dict) or action.get('kind') != 'entry':
            return {'allowed': False,
                    'reason': 'ENTRY_AUTHORITY_SUPERVISED_ACTION_REQUIRED'}
        try:
            payload = json.loads(action.get('payload'))
        except (TypeError, ValueError):
            return {'allowed': False,
                    'reason': 'ENTRY_AUTHORITY_SUPERVISED_ACTION_INVALID'}
        if not isinstance(payload, dict) or payload.get('purpose') != 'engineering':
            return {'allowed': False,
                    'reason': 'ENTRY_AUTHORITY_SUPERVISED_PURPOSE_REQUIRED'}
        state = action.get('state')
        if state not in {'REVIEW_PENDING', 'WAITING_USER', 'READY'}:
            return {'allowed': False,
                    'reason': 'ENTRY_AUTHORITY_SUPERVISED_ACTION_STATE'}
        payload_sha = digest(action.get('payload'))
        in_txn = bool(getattr(self.db, 'in_transaction', False))
        operation = {
            'ExecutionBook.claim.prevalidate': 'claim',
            'ExecutionBook.claim': 'claim',
            'ExecutionBook.prepare_external_action.review': 'review',
            'ExecutionBook.prepare_external_action.place': 'place',
        }.get(source)
        if source == 'ExecutionBook.promote_supervised_entry.final':
            if isinstance(self._supervised_entry_context, dict):
                return self._use_supervised_entry_context(action, payload_sha)
            return {'allowed': False,
                    'reason': 'ENTRY_AUTHORITY_SUPERVISED_PERMIT_NOT_PREVALIDATED'}
        if operation is not None:
            ctx = self._entry_operation_contexts.get(action.get('id'), {}).get(
                operation)
            if in_txn or (isinstance(ctx, dict) and ctx.get('permit_id')):
                return self._use_entry_operation_context(
                    action, payload_sha, operation)
        if in_txn:
            return {'allowed': False,
                    'reason': 'ENTRY_AUTHORITY_SUPERVISED_PERMIT_NOT_PREVALIDATED'}
        try:
            result = authority.validate_current_permit(
                self, action=action, purpose='engineering',
                allow_consumed=(state == 'READY'))
        except Exception as exc:
            return {'allowed': False,
                    'reason': getattr(exc, 'code', None) or type(exc).__name__}
        raw = (result.get('permit') or {}).get('permit')
        if not isinstance(raw, dict) or not isinstance(raw.get('permit_id'), str):
            return {'allowed': False,
                    'reason': 'SUPERVISED_ENGINEERING_PERMIT_INVALID'}
        decision = {'allowed': True,
                    'reason': 'SUPERVISED_ENGINEERING_PERMIT_VERIFIED',
                    'permit_id': raw.get('permit_id'),
                    'binding': result.get('binding'),
                    'permit_verified': True,
                    'entry_authorized': False,
                    'production_ready': False}
        def stamp_context(context):
            window = self._entry_context_window(
                result, action, context.get('created_at'))
            if window is None:
                return False
            context.update({
                'permit_id': raw.get('permit_id'),
                'binding': result.get('binding'),
                'request_sha256': (result.get('request') or {}).get(
                    'request_sha256'),
                'source_fingerprint': (raw.get('source') or {}).get(
                    'source_fingerprint'),
                'facts_assessment_id': (result.get('facts') or {}).get(
                    'assessment_id'),
                'purpose': 'engineering',
                'validated_at': self.clock(),
                'last_seen_at': self.clock(),
                'not_before': window['not_before'],
                'expires_at': window['expires_at'],
            })
            return True

        context = self._supervised_entry_context
        if (source == 'ExecutionBook.promote_supervised_entry'
                and isinstance(context, dict)
                and context.get('action_id') == action.get('id')
                and context.get('payload_sha256') == payload_sha):
            if not stamp_context(context):
                return {'allowed': False,
                        'reason': 'ENTRY_AUTHORITY_SUPERVISED_PERMIT_CONTEXT_INVALID'}
        if operation is not None:
            ctx = self._entry_operation_contexts.get(action.get('id'), {}).get(
                operation)
            if (isinstance(ctx, dict)
                    and ctx.get('action_id') == action.get('id')
                    and ctx.get('payload_sha256') == payload_sha):
                if not stamp_context(ctx):
                    return {'allowed': False,
                            'reason': 'ENTRY_AUTHORITY_SUPERVISED_PERMIT_CONTEXT_INVALID'}
        return decision

    def _use_supervised_entry_context(self, action, payload_sha):
        context = self._supervised_entry_context
        if not isinstance(context, dict):
            return {'allowed': False,
                    'reason': 'ENTRY_AUTHORITY_SUPERVISED_PERMIT_NOT_PREVALIDATED'}
        self._touch_entry_context_seen(context)
        problem = self._entry_context_problem(context, action, payload_sha)
        if problem is not None:
            return {'allowed': False, 'reason': problem}
        return {'allowed': True,
                'reason': 'SUPERVISED_ENGINEERING_PERMIT_VERIFIED',
                'permit_id': context['permit_id'],
                'binding': context['binding'],
                'permit_verified': True,
                'cached': True,
                'entry_authorized': False,
                'production_ready': False}

    def refresh_current_account_assessment(self, *, purpose=None):
        """Recompile current facts outside the write transaction and cache them."""
        preflight = self._current_preflight
        assess = getattr(preflight, 'current_account_assessment', None)
        if not callable(assess):
            self._current_account_facts[purpose] = None
            return {'status': 'facts_unavailable',
                    'reason': 'CURRENT_ACCOUNT_FACTS_READER_MISSING',
                    'entry_authorized': False, 'production_ready': False}
        try:
            value = assess(self, purpose=purpose)
        except Exception:
            value = None
        if not isinstance(value, dict):
            value = None
        self._current_account_facts[purpose] = value
        return value or {'status': 'facts_unavailable',
                         'reason': 'CURRENT_ACCOUNT_FACTS_READER_FAILED',
                         'entry_authorized': False, 'production_ready': False}

    def check_current_entry_action(self, action):
        return self._current_preflight_decision(action=action)

    def protection_evidence(self, trade_id, action_id=None):
        """Read-only actual protection evidence; never authorizes or writes."""
        from .protection_evidence import ProtectionEvidenceReader
        return ProtectionEvidenceReader(
            self, clock=self.clock).assess_actual(
                trade_id=trade_id, action_id=action_id)

    def _current_preflight_decision(self, *, proposal=False, snapshot=None,
                                     quote=None, plan=None, purpose=None,
                                     evidence=None, action=None,
                                     candidate_action_id=None):
        if proposal:
            decision = self._current_preflight.check_proposal(
                self, snapshot, quote, plan, purpose=purpose, evidence=evidence,
                candidate_action_id=candidate_action_id)
        else:
            decision = self._current_preflight.check_claim(self, action)
        result = dict(decision or {})
        reasons = list(result.get('reasons') or [])
        if self.simulation is not True:
            formal = (result.get('components') or {}).get('formal_channel') or {}
            if not isinstance(formal, dict) or formal.get('status') != 'allowed':
                reasons.append('FORMAL_ENGINEERING_CHANNEL_UNPROVEN')
                result['allowed'] = False
                result['entry_authorized'] = False
        result['reasons'] = sorted(set(reasons))
        return result

    def close(self):self.db.close();self.risk_guard.close()
    def state(self):return json.loads(self.db.execute('SELECT payload FROM live_state WHERE id=1').fetchone()[0])
    def actions(self):
        return [dict(zip(('id','trade_id','kind','state','payload','created_at','updated_at','report'),r)) for r in self.db.execute('SELECT * FROM live_actions ORDER BY created_at,id')]
    def _save(self,state):self.db.execute('UPDATE live_state SET payload=? WHERE id=1',(canonical(state),))
    def _begin(self):self.db.execute('BEGIN IMMEDIATE');self.audit.verify()
    def recovery_status(self,snapshot=None):
        """Read acknowledged retirement separately from immutable incident state."""
        empty={'acknowledged_trade_ids':[],'acknowledged_action_ids':[],
            'covered_issue_ids':[],'blockers':[],'automatic_execution_enabled':False}
        if not self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='recovery_cases'").fetchone():return empty
        if not self.db.execute("SELECT 1 FROM recovery_cases WHERE status='ACKNOWLEDGED'").fetchone():return empty
        if snapshot is None:
            if self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='broker_current'").fetchone():
                row=self.db.execute('SELECT payload FROM broker_current WHERE id=1').fetchone()
                if row:snapshot=json.loads(row[0])
        if snapshot is None:return {**empty,'blockers':['RECOVERY_CURRENT_OBSERVATION_MISSING']}
        from .recovery import recovery_disposition
        return recovery_disposition(self,snapshot)

    def active_issues(self,snapshot=None):
        recovery=self.recovery_status(snapshot)
        return sorted((set(self.state()['issues'])-set(recovery['covered_issue_ids']))|set(recovery['blockers']))

    def integrity_block(self):
        """The durable account-level execution-integrity gate.

        This is the same authority the supervisor reports, read at the book's
        own entry and settlement boundaries: a session flag alone would let a
        caller that skips the supervisor place risk on an unreconciled account.
        An unreadable ledger is closed, never optimistically clear.
        """
        if getattr(self,'_integrity_gate_unknown',False):return True
        try:
            from trading_infra.integrity_gate import read_integrity
            state=read_integrity(self.db,db_path=getattr(self,'db_path',None),
                                 clock=self.clock)
        except (ValueError,KeyError,TypeError,OSError,sqlite3.Error):
            self._integrity_gate_unknown=True
            return True
        if not state['known']:
            self._integrity_gate_unknown=True
        return bool(state['blocked'])

    def entry_gate(self):
        if self.policy.get('capture_only') is True:return {'blocked':True,'reason':'CAPTURE_ONLY_POLICY_READ_ONLY'}
        if self.integrity_block():
            return {'blocked':True,'reason':('EXECUTION_INTEGRITY_GATE_UNAVAILABLE'
                if getattr(self,'_integrity_gate_unknown',False)
                else 'EXECUTION_INTEGRITY_FAULT_UNRESOLVED')}
        from .watchdog import entry_inhibit
        path=self.db.execute('PRAGMA database_list').fetchone()[2]
        gate=entry_inhibit(path+'.entry-inhibit.json',path) if path else {'blocked':False,'reason':'IN_MEMORY_BOOK'}
        if gate['blocked'] or self.simulation:return gate
        from .session_lineage import lineage_entry_gate
        return lineage_entry_gate(self)

    def _check_entry_gate(self):
        gate=self.entry_gate()
        if gate['blocked']:raise ValueError('ENTRY_CONTROL_INHIBITED:'+gate['reason'])

    def engineering_verification_gate(self, *, purpose, evidence=None):
        from .live_verification_control import engineering_verification_gate
        path=self.db.execute('PRAGMA database_list').fetchone()[2]
        return engineering_verification_gate(path,simulation=self.simulation,purpose=purpose,
            evidence=evidence,clock=self.clock)

    def _check_engineering_verification(self, *, purpose, evidence=None):
        gate=self.engineering_verification_gate(purpose=purpose,evidence=evidence)
        if gate['blocked']:raise ValueError('ENTRY_CONTROL_INHIBITED:'+gate['reason'])
    def _review_only_action(self,action):
        """A completed native preview has no broker-side effect to retain."""
        if action['state']!='WAITING_USER':return False
        if self.db.execute('SELECT 1 FROM audit_events WHERE event_id=?',(action['id']+'-intent',)).fetchone():return False
        operations=self.db.execute('SELECT operation,report FROM external_operations WHERE action_id=?',(action['id'],)).fetchall()
        return bool(operations) and all(op=='review' and report is not None for op,report in operations)

    def _dispatch_evidence_for_action(self,action_id):
        """Return a persisted reason this action may have been dispatched.

        The caller's proof string is never enough. Wake reservations, worker
        budget reservations, external operations and broker-order identities
        are durable facts written by the dispatch path; their presence means a
        local withdrawal would overwrite possible delivery.
        """
        if not isinstance(action_id,str) or not action_id:
            return 'ACTION_ID_INVALID'
        if self.db.execute('SELECT 1 FROM external_operations WHERE action_id=?',
                           (action_id,)).fetchone():
            return 'EXTERNAL_OPERATION_PRESENT'
        if self.db.execute('SELECT 1 FROM external_broker_order_ids WHERE action_id=?',
                           (action_id,)).fetchone():
            return 'EXTERNAL_BROKER_ORDER_PRESENT'
        tables={r[0] for r in self.db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        if 'host_wake_events' in tables:
            for (raw,) in self.db.execute('SELECT payload FROM host_wake_events'):
                try:value=json.loads(raw)
                except (TypeError,ValueError):continue
                if isinstance(value,dict) and value.get('action_id')==action_id:
                    return 'HOST_WAKE_EVENT_PRESENT'
        if 'worker_call_reservations' in tables:
            if self.db.execute('SELECT 1 FROM worker_call_reservations WHERE action_id=?',
                               (action_id,)).fetchone():
                return 'WORKER_RESERVATION_PRESENT'
        return None

    def _known_not_dispatched_action(self,action_id,payload,report):
        try:
            status=json.loads(report or '{}').get('status')
            generation=int(payload.get('not_dispatched_generation',1))
        except (TypeError,ValueError,KeyError):
            return False
        if status!='not_dispatched' or generation<1:
            return False
        if self._dispatch_evidence_for_action(action_id) is not None:
            return False
        return self.db.execute('SELECT 1 FROM audit_events WHERE event_id=?',
                               (action_id+'-not-dispatched',)).fetchone() is not None

    def _supersede_undispatched(self,trade_id,kinds,now,*,except_id=None):
        # Only actions with no durable dispatch intent can be withdrawn locally.
        # In-flight, unknown and user-review refs retain their immutable identity.
        for action in self.actions():
            if (action['trade_id']!=trade_id or action['kind'] not in kinds
                    or action['id']==except_id or (action['state'] not in {'READY','EXPIRED'}
                    and not self._review_only_action(action))):continue
            if self.db.execute('SELECT 1 FROM audit_events WHERE event_id=?',(action['id']+'-intent',)).fetchone():continue
            self.db.execute("UPDATE live_actions SET state='SUPERSEDED',updated_at=? WHERE id=?",
                (now.isoformat(),action['id']))
    @staticmethod
    def _entry_action_id(signal_id, plan):
        params={'symbol':plan['symbol'],'side':'buy','type':'limit',
            'quantity':str(plan['quantity']),'limit_price':plan['entry_ceiling'],
            'market_hours':'regular_hours','time_in_force':'gfd'}
        return 'live-'+digest([signal_id,'entry',params,None])[:32]

    def _queue(self,trade,kind,params,now,reason,*,target=None,state='READY',intent_only=False):
        # Identical logical action always retains its UUID ref_id, including restart.
        identity=digest([trade['id'],kind,params,target])
        action_id='live-'+identity[:32]
        cancel_attempt=1
        exit_attempt=1
        not_dispatched_generation=1
        if kind=='cancel':
            # A definitive native rejection is not an ambiguous write. A fresh
            # reconciliation may prepare a NEW supervised attempt, never replay
            # an UNKNOWN cancellation or reuse an operation/ref identity.
            previous=[a for a in self.actions() if a['trade_id']==trade['id'] and a['kind']=='cancel'
                and json.loads(a['payload'])['target_order_id']==target]
            if previous:
                latest=max(previous,key=lambda a:json.loads(a['payload']).get('cancel_attempt',1))
                attempts=json.loads(latest['payload']).get('cancel_attempt',1)
                rejected=(latest['state']=='REPORTED' and latest['report']
                    and json.loads(latest['report'])['status']=='rejected'
                    and self.db.execute("SELECT 1 FROM external_operations WHERE action_id=? AND operation='cancel' AND json_extract(report,'$.status')='rejected'",(latest['id'],)).fetchone())
                if not rejected:return latest['id']
                if attempts>=3:
                    trade['manual_intervention_required']='CANCEL_RETRY_LIMIT_EXCEEDED:'+str(target)
                    return latest['id']
                cancel_attempt=attempts+1
                action_id='live-'+digest([trade['id'],kind,params,target,cancel_attempt])[:32]
                self.db.execute("UPDATE live_actions SET state='REJECTED',updated_at=? WHERE id=?",(now.isoformat(),latest['id']))
        if kind=='exit':
            # A definitive broker refusal is not an ambiguous write: a later
            # reconciliation may prepare a NEW supervised exit attempt with its
            # own ref and operation history. An UNKNOWN exit is never replayed,
            # and the rejected attempt stays in the ledger with its identity.
            previous=[a for a in self.actions() if a['trade_id']==trade['id']
                      and a['kind']=='exit'
                      and json.loads(a['payload']).get('parameters')==params]
            if previous:
                # Both failure classes share one monotonic retry identity.
                # The generation survives a native attempt and the native
                # attempt survives a not-dispatched generation; neither counter
                # is reset when the other class advances.
                latest=max(previous,key=lambda a:(
                    int(json.loads(a['payload']).get('not_dispatched_generation',1)),
                    int(json.loads(a['payload']).get('exit_attempt',1)),
                    a['created_at']))
                latest_payload=json.loads(latest['payload'])
                generation=int(latest_payload.get('not_dispatched_generation',1))
                attempts=int(latest_payload.get('exit_attempt',1))
                if self._known_not_dispatched_action(
                        latest['id'],latest_payload,latest['report']):
                    if generation>=3:
                        trade['manual_intervention_required']=(
                            'EXIT_NOT_DISPATCHED_RETRY_LIMIT_EXCEEDED')
                        return latest['id']
                    not_dispatched_generation=generation+1
                    exit_attempt=attempts
                    action_id='live-'+digest([trade['id'],kind,params,target,
                        'retry',not_dispatched_generation,exit_attempt])[:32]
                else:
                    rejected=(latest['state'] in {'REPORTED','OBSERVED'}
                              and self._definitive_exit_refusal(latest))
                    if not rejected:return latest['id']
                    if attempts>=3:
                        trade['manual_intervention_required']='EXIT_RETRY_LIMIT_EXCEEDED'
                        return latest['id']
                    not_dispatched_generation=generation
                    exit_attempt=attempts+1
                    action_id='live-'+digest([trade['id'],kind,params,target,
                        'retry',not_dispatched_generation,exit_attempt])[:32]
                    self.db.execute("UPDATE live_actions SET state='REJECTED',updated_at=? WHERE id=?",
                        (now.isoformat(),latest['id']))
        if kind=='protect':
            previous=[a for a in self.actions() if a['trade_id']==trade['id']
                      and a['kind']=='protect'
                      and json.loads(a['payload']).get('parameters')==params]
            if previous:
                latest=max(previous,key=lambda a:(json.loads(a['payload']).get('not_dispatched_generation',1),a['created_at']))
                latest_payload=json.loads(latest['payload'])
                if self._known_not_dispatched_action(
                        latest['id'],latest_payload,latest['report']):
                    generation=int(latest_payload.get('not_dispatched_generation',1))
                    if generation>=3:
                        trade['manual_intervention_required']=(
                            'PROTECT_NOT_DISPATCHED_RETRY_LIMIT_EXCEEDED')
                        return latest['id']
                    not_dispatched_generation=generation+1
                    action_id='live-'+digest([trade['id'],kind,params,target,
                        'not-dispatched',not_dispatched_generation])[:32]
                else:
                    action_id=latest['id']
        if kind in {'protect','exit'}:
            self._supersede_undispatched(trade['id'],{kind},now,except_id=action_id)
        existing=self.db.execute('SELECT state,payload FROM live_actions WHERE id=?',(action_id,)).fetchone()
        if existing:
            # A never-dispatched protective action can refresh its delivery deadline.
            # Reuse the same ref_id. Submitted/unknown/confirmed actions never rearm.
            dispatched=self.db.execute('SELECT 1 FROM audit_events WHERE event_id=?',(action_id+'-intent',)).fetchone()
            if existing[0] in {'EXPIRED','SUPERSEDED'} and not dispatched and kind!='entry':
                payload=json.loads(existing[1]);payload['expires_at']=(now+timedelta(seconds=120)).isoformat()
                payload['broker_snapshot_at']=trade.get('snapshot_at')
                self.db.execute("UPDATE live_actions SET state='READY',payload=?,updated_at=? WHERE id=?",
                    (canonical(payload),now.isoformat(),action_id))
            return action_id
        # One in-flight action per (trade, kind) is the rule, except for
        # cancels: a cancel is about one broker order, so an entry-remainder
        # cancel must not block a stop-target cancel. Same target was already
        # resolved above, which keeps exact idempotency and never re-sends an
        # UNKNOWN cancellation.
        blocking=self.db.execute(
            "SELECT payload FROM live_actions WHERE trade_id=? AND kind=? AND state IN"
            " ('READY','RUNNING','UNKNOWN','WAITING_USER','REPORTED')",(trade['id'],kind)).fetchall()
        for (payload,) in blocking:
            if kind=='cancel' and json.loads(payload).get('target_order_id')!=target:
                continue
            # An unresolved exit of any other size also blocks a new one: a
            # second sell for the changed quantity could oversell the account.
            return None
        ref=str(uuid.uuid5(uuid.NAMESPACE_URL,action_id))
        expires=now+timedelta(seconds=15 if kind=='entry' else 120)
        deadline=trade.get('evidence',{}).get('parameters_valid_until')
        if kind=='entry' and trade.get('purpose')=='engineering' and deadline:
            # The explicitly authorized engineering intent survives human
            # confirmation latency up to its date-bound grant. Executable
            # quotes, native reviews and prepared packets keep their independent
            # short deadlines. Existing/in-flight identities are never rearmed.
            expires=min(timestamp(deadline),session(now.isoformat())['close'])
        strategy_deadline=trade.get('evidence',{}).get('strategy_valid_until')
        if kind=='entry' and strategy_deadline:expires=min(expires,timestamp(strategy_deadline))
        payload={'action_id':action_id,'trade_id':trade['id'],'kind':kind,'account':'••••'+self.policy['account_suffix'],
            'synthetic':self.simulation,'parameters':params,'ref_id':ref,'target_order_id':target,
            'reason':reason,'created_at':now.isoformat(),'expires_at':expires.isoformat(),
            'plan':trade['plan'],'purpose':trade.get('purpose','unclassified'),'policy':self.policy,
            'broker_snapshot_at':trade.get('snapshot_at'),
            'not_dispatched_generation':not_dispatched_generation}
        if kind=='entry':payload['preflight_quote']=trade.get('preflight_quote')
        if kind=='cancel':payload['cancel_attempt']=cancel_attempt
        if kind=='exit':payload['exit_attempt']=exit_attempt
        if intent_only:payload['supervised_intent']=True
        self.db.execute('INSERT INTO live_actions VALUES(?,?,?,?,?,?,?,NULL)',
            (action_id,trade['id'],kind,state,canonical(payload),now.isoformat(),now.isoformat()))
        return action_id

    def _snapshot_check(self,snapshot,now):
        if (snapshot.get('synthetic') is not self.simulation or snapshot.get('collection_complete') is not True
            or snapshot.get('account_mask')!='••••'+self.policy['account_suffix']):
            raise ValueError('EXECUTION_REQUIRES_FRESH_COMPLETE_BOUND_ACCOUNT')
        state=self.state()
        if state['account_key'] and state['account_key']!=snapshot['account_key']:
            raise ValueError('EXECUTION_ACCOUNT_IDENTITY_CHANGED')
        if state['last_snapshot'] and timestamp(snapshot['observed_at'])<timestamp(state['last_snapshot']):
            raise TransportError('BROKER_SNAPSHOT_SUPERSEDED')
        if not 0<=(now-timestamp(snapshot['observed_at'])).total_seconds()<=30:
            raise ValueError('EXECUTION_REQUIRES_FRESH_COMPLETE_BOUND_ACCOUNT')
        if snapshot.get('other_asset_open_orders') or any(number(v)!=0 for v in snapshot.get('other_asset_values',{}).values()):
            raise ValueError('UNSUPPORTED_ACCOUNT_EXPOSURE')

    def _check_strategy_validity(self,evidence,*,now=None):
        """Optional V1 proposal envelope; legacy inputs keep their old gate.

        New process-host suggestions cannot gain another15seconds merely by
        reaching a durable execution outbox. Missing partial envelopes fail.
        """
        fields={'strategy_proposal_id','strategy_generated_at','strategy_valid_until'}
        if not isinstance(evidence,dict) or not fields.intersection(evidence):return
        try:
            if not fields.issubset(evidence) or not isinstance(evidence['strategy_proposal_id'],str) or not evidence['strategy_proposal_id']:
                raise ValueError()
            start=timestamp(evidence['strategy_generated_at']);until=timestamp(evidence['strategy_valid_until'])
            current=now if now is not None else self.clock()
            if until<=start or current<start:raise ValueError()
        except (ValueError,KeyError,TypeError,AttributeError):
            raise ValueError('STRATEGY_PROPOSAL_VALIDITY_INVALID') from None
        if current>=until:raise ValueError('STRATEGY_PROPOSAL_EXPIRED')

    def propose(self,signal_id,plan,quote,snapshot,*,evidence,purpose='strategy'):
        self._check_engineering_verification(purpose=purpose,evidence=evidence)
        self._check_strategy_validity(evidence)
        if self.policy.get('engineering_only') is True and purpose!='engineering':
            raise ValueError('ENGINEERING_ACCEPTANCE_ONLY')
        if (purpose=='engineering' and isinstance(evidence,dict) and evidence.get('parameters_valid_until')
                and self.clock()>=timestamp(evidence['parameters_valid_until'])):
            raise ValueError('ENGINEERING_PARAMETERS_EXPIRED')
        self.refresh_current_account_assessment(purpose=purpose)
        self.entry_authority.require_entry(
            source='ExecutionBook.propose', snapshot=snapshot, purpose=purpose)
        self._check_entry_gate()
        now=self.clock();self._snapshot_check(snapshot,now)
        account_risk=self.risk_guard.observe(snapshot)
        if account_risk['blocked']:raise ValueError('LIVE_ACCOUNT_RISK_BLOCKED:'+','.join(account_risk['reasons']))
        if purpose not in {'strategy','engineering'}:raise ValueError('EXPLICIT_TRADE_PURPOSE_REQUIRED')
        if purpose=='engineering' and (not isinstance(evidence,dict) or evidence.get('engineering_scope_authorized') is not True
                or evidence.get('parameters_user_specified') is not True or not signal_id.startswith('engineering-')):
            raise ValueError('ENGINEERING_SCOPE_AND_EXACT_PARAMETERS_REQUIRED')
        if isinstance(evidence,dict) and evidence.get('purpose',purpose)!=purpose:raise ValueError('TRADE_PURPOSE_CONFLICT')
        if not isinstance(evidence,dict) or not evidence.get('source_sha256') or evidence.get('synthetic') is not self.simulation:
            raise ValueError('STRATEGY_SOURCE_EVIDENCE_REQUIRED')
        if not is_regular(now.isoformat()) or now>=session(now.isoformat())['close']-timedelta(minutes=10):raise ValueError('ENTRY_CUTOFF')
        broker={'account':snapshot['account_mask'],'read_healthy':True,'new_risk_blocked':False,
            'last_observed_at':snapshot['observed_at'],'positions':snapshot['positions'],
            'open_equity_orders':sum(o['state'] not in TERMINAL for o in snapshot['orders'].values()),
            'other_asset_open_orders':snapshot['other_asset_open_orders'],'portfolio':snapshot['portfolio'],
            'risk_state':self.risk_guard.state(),
            'buying_power':snapshot['buying_power'],'unleveraged_buying_power':snapshot['unleveraged_buying_power']}
        check=trial_proposal(self.policy,broker,quote,plan,as_of=now.isoformat())
        if not check['within_trial_limits']:raise ValueError('ENTRY_GATES:'+','.join(check['blockers']))
        if snapshot['pending_approvals']:raise ValueError('PENDING_APPROVAL_BLOCKS_ENTRY')
        candidate_action_id=self._entry_action_id(signal_id,plan)
        preflight=self._current_preflight_decision(
            proposal=True,snapshot=snapshot,quote=quote,plan=plan,
            purpose=purpose,evidence=evidence,
            candidate_action_id=candidate_action_id)
        if preflight.get('allowed') is not True:
            raise ValueError('CURRENT_ENGINEERING_PREFLIGHT_BLOCKED:'+
                ','.join(preflight.get('reasons') or ['UNKNOWN']))
        fingerprint=digest([plan,evidence,purpose]);self._begin()
        try:
            self._check_engineering_verification(purpose=purpose,evidence=evidence)
            now=self.clock();self._check_strategy_validity(evidence,now=now)
            self._snapshot_check(snapshot,now)
            old=self.db.execute('SELECT fingerprint FROM live_signals WHERE id=?',(signal_id,)).fetchone()
            if old:
                if old[0]!=fingerprint:raise ValueError('SIGNAL_ID_CONFLICT')
                self.db.rollback();return {'duplicate':True}
            state=self.state()
            recovery=self.recovery_status(snapshot);retired=set(recovery['acknowledged_trade_ids'])
            active_trades=[t for t in state['trades'].values()
                           if t['phase']!='FLAT_VERIFIED' and t['id'] not in retired]
            # Build unresolved scope from the actions themselves.  The old
            # same_trade_ids set was derived from active trades that had
            # already been rejected above, so UNKNOWN orphan/FLAT actions could
            # silently pass.  UNKNOWN or unknown identity is always global
            # fail-closed; known different-symbol in-flight work is considered
            # separately below.
            unresolved=[]
            for action in self.actions():
                if action['state'] not in {'RUNNING','UNKNOWN','WAITING_USER',
                                           'REPORTED'}:
                    continue
                if action['id'] in recovery['acknowledged_action_ids']:
                    continue
                action_payload=_safe_payload(action)
                trade=state['trades'].get(action.get('trade_id'))
                trade_plan=(trade or {}).get('plan') if isinstance(trade,dict) else None
                action_plan=action_payload.get('plan') if isinstance(
                    action_payload.get('plan'),dict) else None
                symbol=(trade_plan or {}).get('symbol') if isinstance(
                    trade_plan,dict) else None
                if (not isinstance(trade,dict) or not isinstance(symbol,str)
                        or not symbol or not isinstance(action_plan,dict)
                        or action_plan.get('symbol')!=symbol
                        or not isinstance(action_payload.get('ref_id'),str)
                        or not action_payload.get('ref_id')):
                    raise ValueError('UNRESOLVED_ACTION_BLOCKS_ENTRY')
                if action['state']=='UNKNOWN':
                    raise ValueError('UNRESOLVED_ACTION_BLOCKS_ENTRY')
                if symbol==plan['symbol']:
                    raise ValueError('UNRESOLVED_ACTION_BLOCKS_ENTRY')
                if action['state'] not in {'RUNNING','REPORTED','WAITING_USER'}:
                    raise ValueError('UNRESOLVED_ACTION_BLOCKS_ENTRY')
                unresolved.append(action)
            if self.active_issues(snapshot) or any(
                    (t.get('plan') or {}).get('symbol')==plan['symbol']
                    for t in active_trades):
                raise ValueError('EXECUTION_HAS_ACTIVE_TRADE_OR_ISSUE')
            trade={'id':signal_id,'purpose':purpose,'plan':plan,'phase':'ENTRY_PLANNED','stop':plan['stop'],
                'initial_stop':plan['stop'],'quantity':'0','entry_price':None,'snapshot_at':snapshot['observed_at'],
                'exit_requested':False,'cancel_for_replace':None,'evidence':evidence,'preflight_quote':quote}
            action=self._queue(trade,'entry',{'symbol':plan['symbol'],'side':'buy','type':'limit',
                'quantity':str(plan['quantity']),'limit_price':plan['entry_ceiling'],
                'market_hours':'regular_hours','time_in_force':'gfd'},now,'ENGINEERING_ACCEPTANCE_TEST' if purpose=='engineering' else 'QUALIFIED_STRATEGY_SIGNAL')
            trade['entry_action']=action;state['trades'][signal_id]=trade;state['account_key']=snapshot['account_key']
            self.db.execute('INSERT INTO live_signals VALUES(?,?)',(signal_id,fingerprint));self._save(state)
            if not self._preflight_binding_ok(
                    preflight, action=None, require_journal=False,
                    purpose=purpose, evidence=evidence,
                    expected_account_key=snapshot['account_key'],
                    snapshot=snapshot):
                raise ValueError('CURRENT_ENGINEERING_PREFLIGHT_BINDING_CHANGED')
            self._consume_formal_authorization(preflight, action, now)
            self._check_strategy_validity(evidence);self.db.commit()
            return {'duplicate':False,'action_id':action,'budget_check':check}
        except BaseException:self.db.rollback();raise

    def record_supervised_engineering_intent(self,signal_id,plan,quote,snapshot,*,evidence):
        """Record an auditable review intent; never an entry/claim/write permit."""
        authority=self.supervised_engineering_authority
        if authority is None:
            raise ValueError('SUPERVISED_ENGINEERING_AUTHORITY_UNAVAILABLE')
        now=self.clock()
        if not isinstance(signal_id,str) or not signal_id.startswith('engineering-') or len(signal_id)>256:
            raise ValueError('ENGINEERING_SCOPE_AND_EXACT_PARAMETERS_REQUIRED')
        if (not isinstance(evidence,dict) or evidence.get('engineering_scope_authorized') is not True
                or evidence.get('parameters_user_specified') is not True
                or evidence.get('synthetic') is not self.simulation
                or not evidence.get('source_sha256')):
            raise ValueError('ENGINEERING_SCOPE_AND_EXACT_PARAMETERS_REQUIRED')
        if evidence.get('purpose','engineering')!='engineering':
            raise ValueError('TRADE_PURPOSE_CONFLICT')
        if (evidence.get('parameters_valid_until') is not None
                and now>=timestamp(evidence['parameters_valid_until'])):
            raise ValueError('ENGINEERING_PARAMETERS_EXPIRED')
        if not isinstance(quote,dict) or quote.get('symbol')!=plan.get('symbol'):
            raise ValueError('SUPERVISED_ENGINEERING_QUOTE_REQUIRED')
        try:
            for key in ('bid_at','ask_at','observed_at'):
                if not 0<=(now-timestamp(quote[key])).total_seconds()<=15:
                    raise ValueError('SUPERVISED_ENGINEERING_QUOTE_STALE')
        except (KeyError,TypeError,ValueError):
            raise ValueError('SUPERVISED_ENGINEERING_QUOTE_REQUIRED') from None
        request=authority.validate_intent(self,plan=plan,snapshot=snapshot,evidence=evidence,signal_id=signal_id)
        self._snapshot_check(snapshot,now)
        if not is_regular(now.isoformat()):
            raise ValueError('EXTERNAL_ACTION_OUTSIDE_AUTHORIZED_SESSION')
        fingerprint=digest([plan,evidence,'engineering-intent'])
        self._begin()
        try:
            self._snapshot_check(snapshot,now)
            old=self.db.execute('SELECT fingerprint FROM live_signals WHERE id=?',(signal_id,)).fetchone()
            if old:
                if old[0]!=fingerprint:raise ValueError('SIGNAL_ID_CONFLICT')
                existing=next((a for a in self.actions()
                               if a['trade_id']==signal_id and a['kind']=='entry'),None)
                if existing is None:
                    self.db.rollback()
                    raise ValueError(
                        'SUPERVISED_ENGINEERING_CORE_INTENT_LEGACY_UNVERIFIABLE')
                anchor=self.db.execute(
                    'SELECT payload FROM audit_events WHERE event_id=?',
                    ('supervised-intent-'+existing['id']+'-recorded',)).fetchone()
                try:
                    details=json.loads(anchor[0])['details']
                    anchor_id=details['request_id']
                    anchor_sha=details['request_sha256']
                except (TypeError, ValueError, KeyError):
                    self.db.rollback()
                    raise ValueError(
                        'SUPERVISED_ENGINEERING_CORE_INTENT_LEGACY_UNVERIFIABLE') from None
                self.db.rollback()
                return {'duplicate':True,'action_id':existing['id'],
                    'intent_only':True,'entry_authorized':False,
                    'request_id':anchor_id,'request_sha256':anchor_sha}
            if any(a['state'] in {'REVIEW_PENDING','READY','RUNNING','UNKNOWN','WAITING_USER','REPORTED'}
                   and a['kind']=='entry' for a in self.actions()):
                raise ValueError('SUPERVISED_ENGINEERING_INTENT_ALREADY_OPEN')
            state=self.state()
            trade={'id':signal_id,'purpose':'engineering','plan':plan,
                'phase':'ENTRY_PLANNED','stop':plan['stop'],'initial_stop':plan['stop'],
                'quantity':'0','entry_price':None,'snapshot_at':snapshot['observed_at'],
                'exit_requested':False,'cancel_for_replace':None,'evidence':dict(evidence),
                'preflight_quote':quote}
            action=self._queue(trade,'entry',{'symbol':plan['symbol'],'side':'buy','type':'limit',
                'quantity':str(plan['quantity']),'limit_price':plan['entry_ceiling'],
                'market_hours':'regular_hours','time_in_force':'gfd'},now,
                'SUPERVISED_ENGINEERING_INTENT',state='REVIEW_PENDING',intent_only=True)
            if action is None:raise ValueError('SUPERVISED_ENGINEERING_INTENT_BLOCKED')
            trade['entry_action']=action;state['trades'][signal_id]=trade
            state['account_key']=snapshot['account_key'];self._save(state)
            self.db.execute('INSERT INTO live_signals VALUES(?,?)',(signal_id,fingerprint))
            self.audit.append_in_transaction({
                'event_id':'supervised-intent-'+action+'-recorded',
                'occurred_at':now.isoformat(),'event_type':'RECONCILIATION_ISSUE',
                'actor':'script','synthetic':self.simulation,'trade_id':signal_id,
                'details':{'status':'SUPERVISED_ENGINEERING_INTENT_RECORDED',
                    'action_id':action,'request_id':request['request_id'],
                    'request_sha256':request['request_sha256'],
                    'entry_authorized':False,'intent_only':True}})
            self.db.commit()
            return {'duplicate':False,'action_id':action,'intent_only':True,
                'entry_authorized':False,'request_id':request['request_id'],
                'request_sha256':request['request_sha256']}
        except BaseException:self.db.rollback();raise

    def configure_engineering_source(self,root,*,provenance_reader=None,
                                      risk_lineage_reader=None,
                                      allow_synthetic=None,
                                      owner_source_root=None,
                                      native_receipt_root=None,
                                      native_ingress_root=None,
                                      real_activation_enabled=False):
        """Owner composition entry for the real-source evidence adapter.

        The activation gate defaults closed.  Missing readers stay named
        blocks; this never creates a fake provenance/risk record.
        """
        from .engineering_source import build_owner_engineering_authority
        if owner_source_root is not None:
            from .owner_engineering_source import EngineeringOwnerSource
            source=EngineeringOwnerSource(owner_source_root,clock=self.clock)
            provenance_reader=(provenance_reader or
                               source.provenance_reader)
            risk_lineage_reader=(risk_lineage_reader or
                                 source.risk_lineage_reader)
        native_reader=None
        if native_receipt_root is not None:
            from .native_receipt_source import NativeReceiptEvidenceReader
            native_reader=NativeReceiptEvidenceReader(
                native_receipt_root, ingress_root=native_ingress_root,
                clock=self.clock)
        authority=build_owner_engineering_authority(
            root,clock=self.clock,
            allow_synthetic=(self.simulation if allow_synthetic is None
                             else allow_synthetic),
            provenance_reader=provenance_reader,
            risk_lineage_reader=risk_lineage_reader,
            native_receipt_reader=native_reader,
            real_activation_enabled=real_activation_enabled)
        self.supervised_engineering_authority=authority
        if getattr(self, '_entry_authority', None) is not None:
            self._entry_authority.supervised_baseline_required = True
        return authority

    def dry_assess_supervised_engineering(self,action_id):
        """Read-only owner dry assessment; never consume/claim/place."""
        authority=self.supervised_engineering_authority
        if authority is None:
            raise ValueError('SUPERVISED_ENGINEERING_AUTHORITY_UNAVAILABLE')
        action=next((a for a in self.actions() if a['id']==action_id),None)
        if action is None or action['kind']!='entry':
            raise ValueError('SUPERVISED_ENGINEERING_ACTION_REQUIRED')
        decision=authority.dry_assess(book=self,action=action,purpose='engineering')
        if not isinstance(decision,dict):
            raise ValueError('SUPERVISED_ENGINEERING_DRY_DECISION_INVALID')
        # The dry result is evidence only; it never mutates an action, permit,
        # risk row, audit trail or consumption table.
        return decision

    def promote_supervised_entry(self,action_id,*,expected_permit_id=None):
        self._clear_supervised_entry_context()
        try:
            return self._promote_supervised_entry_impl(action_id,expected_permit_id=expected_permit_id)
        finally:
            self._clear_supervised_entry_context()

    def _promote_supervised_entry_impl(self,action_id,*,expected_permit_id=None):
        """Promote one reviewed intent to READY only after exact permit checks.

        The permit is consumed in the same transaction as the READY transition.
        Claim, native place and formal-channel fences still run independently.
        """
        authority=self.supervised_engineering_authority
        if authority is None:
            raise ValueError('SUPERVISED_ENGINEERING_AUTHORITY_UNAVAILABLE')
        now=self.clock();self.refresh_current_account_assessment(purpose='engineering')
        action=next((a for a in self.actions() if a['id']==action_id),None)
        if action is None or action['kind']!='entry':
            raise ValueError('SUPERVISED_ENGINEERING_ACTION_REQUIRED')
        payload=json.loads(action['payload'])
        if payload.get('purpose')!='engineering' or action['state'] not in {'REVIEW_PENDING','WAITING_USER'}:
            raise ValueError('SUPERVISED_ENGINEERING_PROMOTION_STATE')
        plan=payload.get('plan');evidence=self.state()['trades'][action['trade_id']]['evidence']
        decision=authority.assess(book=self,mode='claim',action=action,plan=plan,
            purpose='engineering',evidence=evidence,snapshot=self.state().get('last_snapshot'))
        if decision.get('allowed') is not True:
            raise ValueError('SUPERVISED_ENGINEERING_PERMIT_BLOCKED:'+
                ','.join(decision.get('reasons') or ['UNKNOWN']))
        self._begin_supervised_entry_prevalidation(action)
        entry=self.entry_authority.check(source='ExecutionBook.promote_supervised_entry',
            action=action,purpose='engineering')
        if entry.get('allowed') is not True:
            raise ValueError('ENTRY_AUTHORITY_BLOCKED:'+str(entry.get('reason') or 'UNKNOWN'))
        preflight=self._current_preflight_decision(action=action)
        if preflight.get('allowed') is not True:
            raise ValueError('CURRENT_ENGINEERING_PREFLIGHT_BLOCKED:'+
                ','.join(preflight.get('reasons') or ['UNKNOWN']))
        if not self._preflight_binding_ok(preflight,action=action,require_journal=True,
                purpose='engineering',evidence=evidence,
                expected_account_key=self.state().get('account_key')):
            raise ValueError('CURRENT_ENGINEERING_PREFLIGHT_BINDING_CHANGED')
        self._check_engineering_verification(purpose='engineering',evidence=evidence)
        component=decision.get('component') or decision.get('sanitized')
        if not isinstance(component,dict):
            raise ValueError('SUPERVISED_ENGINEERING_COMPONENT_INVALID')
        if expected_permit_id is not None:
            if (not isinstance(expected_permit_id,str)
                    or not re.fullmatch('[0-9a-f]{64}',expected_permit_id)
                    or component.get('permit_id')!=expected_permit_id):
                raise ValueError('SUPERVISED_ENGINEERING_PERMIT_REFERENCE_MISMATCH')
        control=self.engineering_verification_gate(purpose='engineering',evidence=evidence)
        preflight_receipt=receipt_of(preflight)
        promotion={
            'action_payload_sha256':digest(action['payload']),
            'entry_reason':entry.get('reason'),
            'control_gate_sha256':digest(control),
            'risk_state_sha256':digest(self.risk_guard.state()),
            'preflight_receipt_sha256':(digest(preflight_receipt)
                if preflight_receipt is not None else None),
        }
        self._begin()
        try:
            final_now=self.clock()
            current=next((a for a in self.actions() if a['id']==action_id),None)
            if (current is None
                    or current['state'] not in {'REVIEW_PENDING','WAITING_USER'}):
                raise ValueError('SUPERVISED_ENGINEERING_PROMOTION_CHANGED')
            if digest(current['payload'])!=promotion['action_payload_sha256']:
                raise ValueError('SUPERVISED_ENGINEERING_ACTION_CHANGED')
            entry_final=self.entry_authority.check(
                source='ExecutionBook.promote_supervised_entry.final',
                action=current,purpose='engineering')
            if (entry_final.get('allowed') is not True
                    or entry_final.get('reason')!=promotion['entry_reason']):
                raise ValueError('SUPERVISED_ENGINEERING_ENTRY_CHANGED')
            control_final=self.engineering_verification_gate(
                purpose='engineering',evidence=evidence)
            if digest(control_final)!=promotion['control_gate_sha256']:
                raise ValueError('SUPERVISED_ENGINEERING_CONTROL_CHANGED')
            if digest(self.risk_guard.state())!=promotion['risk_state_sha256']:
                raise ValueError('SUPERVISED_ENGINEERING_RISK_CHANGED')
            if preflight_receipt is not None:
                recheck=verify_receipt_binding(
                    preflight_receipt,book=self,action=current,now=final_now,
                    require_journal=True,
                    owner_binding_id=getattr(self._current_preflight,'binding_id',None),
                    expected_source_fingerprint=getattr(
                        self._current_preflight,'source_fingerprint_value',None),
                    expected_account_key=self.state().get('account_key'),
                    purpose='engineering',evidence=evidence,
                    snapshot=self.state().get('last_snapshot'),
                    authority_reader=getattr(self._current_preflight,
                                             'authority_reader',read_book_authority),
                    formal_authority=None)
                if recheck.get('bound') is not True:
                    raise ValueError('CURRENT_ENGINEERING_PREFLIGHT_BINDING_CHANGED')
            elif self.simulation is not True:
                raise ValueError('CURRENT_ENGINEERING_PREFLIGHT_RECEIPT_MISSING')
            finalized=authority.finalize_promotion(
                self,component,action_id,final_now,
                grant=decision.get('finalization_grant'))
            consume_now=self.clock()
            finalized=authority.consume(
                self,finalized['component'],action_id,consume_now,
                finalized=finalized)
            consumed_at=finalized.get('consumed_at',consume_now)
            self.db.execute("UPDATE live_actions SET state='READY',updated_at=? WHERE id=?",
                (consumed_at.isoformat(),action_id))
            self.audit.append_in_transaction({
                'event_id':'supervised-intent-'+action_id+'-promoted',
                'occurred_at':consumed_at.isoformat(),'event_type':'RECONCILIATION_ISSUE',
                'actor':'script','synthetic':component.get('supervised_synthetic'),
                'trade_id':action['trade_id'],
                'details':{'status':'SUPERVISED_ENGINEERING_INTENT_PROMOTED',
                    'action_id':action_id,'entry_authorized':False,
                    'execution_authorized':False,'permit_consumed':True,
                    'synthetic':component.get('supervised_synthetic'),
                    'synthetic_only':component.get('supervised_synthetic') is True,
                    'permit_id':component.get('permit_id'),
                    'one_shot_id':component.get('one_shot_id'),
                    'binding':component.get('binding'),
                    'account_key':self.state().get('account_key'),
                    'policy_sha256':digest(self.policy),
                    'source_fingerprint':component.get('source_fingerprint'),
                    'promotion_receipt_sha256':digest(promotion)}})
            # Final bounded owner-clock check after all audits and immediately
            # before commit.  Failure rolls back the one-shot and READY row.
            authority.final_consume_check(finalized,finalized['component'])
            self.db.commit()
            return {'action_id':action_id,'state':'READY','entry_authorized':False,
                'execution_authorized':False,'intent_only':False}
        except BaseException:self.db.rollback();raise

    @staticmethod
    def exit_position_version(trade):
        """Version binding one advice to the exact position it was computed for."""
        return digest(['exit-position-1', trade['id'], str(trade.get('quantity') or '0'),
                       str(trade.get('entry_price') or ''), str(trade.get('first_fill_at') or '')])

    def _decision_from_advice(self,trade,advice,now):
        """Translate one bounded advice into the book's own decision.

        Unknown, expired, mis-bound or unsupported advice is refused with a
        diagnostic; it never silently becomes an exit, and it never stops the
        infrastructure's own protective management.
        """
        if not isinstance(advice,dict) or advice.get('error'):
            trade['exit_advice_diagnostic']=str((advice or {}).get('error') or 'EXIT_ADVICE_MISSING')
            return {'action':'HOLD','reason':'EXIT_ADVICE_UNAVAILABLE'}
        expected=self.exit_position_version(trade)
        try:
            valid_until=timestamp(advice['valid_until'])
            as_of=timestamp(advice['as_of'])
        except (ValueError,KeyError,TypeError):
            trade['exit_advice_diagnostic']='EXIT_ADVICE_TIME_INVALID'
            return {'action':'HOLD','reason':'EXIT_ADVICE_REFUSED'}
        if advice.get('position_version')!=expected:
            trade['exit_advice_diagnostic']='EXIT_ADVICE_POSITION_MISMATCH'
            return {'action':'HOLD','reason':'EXIT_ADVICE_REFUSED'}
        if now>valid_until or not -1<= (now-as_of).total_seconds() <=30:
            trade['exit_advice_diagnostic']='EXIT_ADVICE_STALE'
            return {'action':'HOLD','reason':'EXIT_ADVICE_REFUSED'}
        action=advice.get('action')
        trade['exit_advice_diagnostic']=None
        trade['last_exit_advice']={'action':action,'reason':advice.get('reason'),
            'as_of':advice['as_of'],'valid_until':advice['valid_until'],
            'position_version':expected,'advisor_pid':advice.get('advisor_pid')}
        if action=='EXIT':
            return {'action':'EXIT_ALL','reason':advice.get('reason') or 'STRATEGY_EXIT'}
        if action=='TIGHTEN_PROTECTION':
            stop=advice.get('suggested_stop')
            if not isinstance(stop,str) or not stop:
                return {'action':'HOLD','reason':'EXIT_ADVICE_REFUSED'}
            return {'action':'RAISE_STOP','stop':stop,'reason':advice.get('reason') or 'STRATEGY_TIGHTEN'}
        if action=='HOLD':
            return {'action':'HOLD','reason':advice.get('reason') or 'STRATEGY_HOLD'}
        # REDUCE has no safe native partial-sell channel yet: refuse it
        # explicitly instead of ignoring it or pretending it was applied.
        trade['manual_intervention_required']='EXIT_ADVICE_UNSUPPORTED:'+str(action)
        return {'action':'HOLD','reason':'EXIT_ADVICE_UNSUPPORTED'}

    def _definitive_exit_refusal(self,action):
        """True only for broker/native evidence that the exit has no effect.

        An agent's own ``rejected`` string is not proof that no order exists.
        Evidence must be a prepared native ``place`` operation whose recorded
        broker response is ``rejected``, or an agent rejection backed by an
        exact broker order for this reference that the broker itself marked
        terminal.
        """
        row=self.db.execute("SELECT report FROM external_operations WHERE action_id=?"
            " AND operation='place'",(action['id'],)).fetchone()
        if row and row[0]:
            try:
                if json.loads(row[0]).get('status')=='rejected':return True
            except ValueError:pass
        # A broker's own terminal refusal is equally definitive, whether the
        # worker reported ``rejected`` or the order was later observed to die
        # with no execution. The match must use this action's exact identities.
        refs=set(self._order_refs(action))
        order_ids={row[0] for row in self.db.execute(
            'SELECT order_id FROM external_broker_order_ids WHERE action_id=?',
            (action['id'],))}
        definitive={'rejected','failed','voided','expired'}
        for order in (self.state().get('broker_orders') or {}).values():
            if order.get('state') not in definitive:
                continue
            if (order.get('ref_id') in refs or order.get('id') in refs
                    or order.get('id') in order_ids
                    or order.get('external_broker_order_id') in order_ids):
                # These terminal states mean the order never became a working
                # sell, whatever quantity it still lists as unfilled.
                return True
        return False

    def _entry_claim_ready(self,action):
        """An entry may only be claimed once its execution identity is bound.

        An action whose reference has no owner cannot be routed when a broker
        receipt arrives, so letting it run would create a position the
        infrastructure cannot attribute. The guard is injected by the
        composition layer; without one, behaviour is unchanged.
        """
        guard=getattr(self,'entry_claim_guard',None)
        if guard is None:return True
        return bool(guard(action))

    def current_preflight_binding(self, decision, *, action=None,
                                  require_journal=None, purpose=None,
                                  evidence=None, expected_account_key=None,
                                  snapshot=None):
        """Owner recheck of the versioned assessment receipt.

        The full capture/evidence compile happened before the write lock. This
        bounded recheck reads the journal/action rows and stat pins only; it
        never scans the whole project source under ``BEGIN IMMEDIATE``.
        """
        receipt = receipt_of(decision)
        if action is None and receipt is not None and receipt.get('mode') == 'claim':
            action_id = (receipt.get('action') or {}).get('id')
            if isinstance(action_id, str):
                action = next((item for item in self.actions()
                               if item['id'] == action_id), None)
        if purpose is None and action is not None:
            try:
                trade = self.state().get('trades', {}).get(action['trade_id'], {})
                purpose = trade.get('purpose')
                evidence = trade.get('evidence')
            except (KeyError, TypeError):
                pass
        if require_journal is None:
            require_journal = bool(receipt and receipt.get('mode') == 'claim')
        if expected_account_key is None and require_journal:
            expected_account_key = self.state().get('account_key')
        preflight = self._current_preflight
        result = verify_receipt_binding(
            receipt, book=self, action=action, now=self.clock(),
            require_journal=bool(require_journal),
            owner_binding_id=getattr(preflight, 'binding_id', None),
            expected_source_fingerprint=getattr(
                preflight, 'source_fingerprint_value', None),
            expected_account_key=expected_account_key,
            purpose=purpose, evidence=evidence, snapshot=snapshot,
            authority_reader=getattr(
                preflight, 'authority_reader', read_book_authority),
            formal_authority=getattr(preflight, 'formal_authority', None))
        pointer_reasons = getattr(preflight, 'registry_pointer_reasons', None)
        if callable(pointer_reasons):
            extra = pointer_reasons()
            if extra:
                result['reasons'] = sorted(
                    set(result.get('reasons') or []) | set(extra))
                result['bound'] = False
        return result

    def _preflight_binding_ok(self, decision, **kwargs):
        if isinstance(self._current_preflight, NoCurrentPreflight):
            return self.simulation is True
        return self.current_preflight_binding(
            decision, **kwargs).get('bound') is True

    def _consume_formal_authorization(self, decision, action_id, now):
        """Consume one owner order authorization inside the proposal txn."""
        component = (decision.get('components') or {}).get(
            'formal_channel') if isinstance(decision, dict) else None
        if not isinstance(component, dict) or component.get('status') != 'allowed':
            return None
        if component.get('duplicate_existing') is True:
            return None
        authority = getattr(self._current_preflight, 'formal_authority', None)
        if authority is None:
            raise ValueError('FORMAL_AUTHORITY_MISSING')
        authority.consume(self, component, action_id, now)
        return None

    def claim(self,*,allow_entries=True):
        self._clear_entry_operation_contexts()
        try:
            return self._claim_impl(allow_entries=allow_entries)
        finally:
            self._clear_entry_operation_contexts()

    def _claim_impl(self,*,allow_entries=True):
        # Current-evidence compilation/file reads happen before the write lock.
        preflight_decisions = {}
        entry_purposes = {}
        if allow_entries:
            refreshed = set()
            state = self.state()
            for candidate in self.actions():
                if candidate['state'] == 'READY' and candidate['kind'] == 'entry':
                    trade = (state.get('trades') or {}).get(
                        candidate.get('trade_id'), {})
                    purpose = trade.get('purpose') if isinstance(trade, dict) else None
                    entry_purposes[candidate['id']] = purpose
                    if purpose not in refreshed:
                        self.refresh_current_account_assessment(purpose=purpose)
                        refreshed.add(purpose)
                    preflight_decisions[candidate['id']] = (
                        self._current_preflight_decision(action=candidate))
                    if purpose == 'engineering':
                        self._begin_entry_operation_context(candidate, 'claim')
                        self.entry_authority.check(
                            source='ExecutionBook.claim.prevalidate',
                            action=candidate, purpose=purpose)
        now=self.clock();self._begin()
        try:
            now=self.clock()
            retired=self.recovery_status()['acknowledged_action_ids']
            # Never auto-reclaim interrupted execution; reconcile first.
            if self.db.execute("SELECT 1 FROM live_actions WHERE state IN ('RUNNING','UNKNOWN')").fetchone():
                self.db.rollback();return None
            for action in sorted(self.actions(),key=lambda a:({'protect':0,'cancel':1,'exit':2,'entry':3}.get(a['kind'],4),a['created_at'])):
                if action['id'] in retired:continue
                if action['state']!='READY':continue
                # An entry is new risk: an unreconciled account must not claim
                # one, whoever the caller is. Protection and exits are not
                # entries and continue from broker facts.
                if action['kind']=='entry' and self.integrity_block():continue
                if action['kind']=='entry' and not allow_entries:continue
                if action['kind']=='entry':
                    decision=self.entry_authority.check(
                        source='ExecutionBook.claim', action=action,
                        purpose=entry_purposes.get(action['id']))
                    if decision.get('allowed') is not True:continue
                    preflight=preflight_decisions.get(action['id'])
                    if preflight is None or preflight.get('allowed') is not True \
                            or not self._preflight_binding_ok(
                                preflight, action=action,
                                require_journal=True):continue
                if action['kind']=='entry' and not self._entry_claim_ready(action):continue
                payload=json.loads(action['payload'])
                if now>=timestamp(payload['expires_at']) or not is_regular(now.isoformat()):
                    self.db.execute("UPDATE live_actions SET state='EXPIRED',updated_at=? WHERE id=?",(now.isoformat(),action['id']));continue
                if action['kind']=='entry' and (self.active_issues() or self.entry_gate()['blocked'] or (self.risk_guard.state() or {}).get('blocked',True)):continue
                if action['kind']=='entry':
                    trade=self.state()['trades'][action['trade_id']]
                    if self.engineering_verification_gate(purpose=trade.get('purpose',payload.get('purpose')),
                            evidence=trade.get('evidence'))['blocked']:continue
                self.db.execute("UPDATE live_actions SET state='RUNNING',updated_at=? WHERE id=?",(now.isoformat(),action['id']))
                kind={'protect':'stop_loss','raise_stop':'raise_stop','cancel':'cancel','exit':'exit','entry':'entry'}[action['kind']]
                self.audit.append_in_transaction({'event_id':action['id']+'-intent','occurred_at':now.isoformat(),
                    'event_type':'AGENT_ACTION_INTENT','actor':'agent','synthetic':self.simulation,
                    'account':payload['account'],'intent_id':action['id'],'trade_id':payload['trade_id'],
                    'ref_id':payload['ref_id'],'symbol':payload['plan']['symbol'],
                    'details':{'action':kind,'parameters':payload['parameters'],'reason':payload['reason']}})
                self.db.commit();return payload
            self.db.commit();return None
        except BaseException:self.db.rollback();raise

    def report(self,action_id,report):
        allowed={'needs_confirmation','review_only','pending_approval','submitted','cancel_requested','rejected','unknown'}
        if not isinstance(report,dict) or report.get('status') not in allowed or set(report)-{'status','message','order_id','market_data_disclosure'}:
            raise ValueError('INVALID_AGENT_REPORT')
        # A model message never serves as a broker fill or approval receipt.
        now=self.clock();self._begin()
        try:
            action=next((a for a in self.actions() if a['id']==action_id),None)
            if not action or action['state']!='RUNNING':raise ValueError('ACTION_NOT_RUNNING')
            payload=json.loads(action['payload'])
            status=report['status'];state='UNKNOWN' if status=='unknown' else 'WAITING_USER' if status in {'needs_confirmation','review_only'} else 'REPORTED'
            self.db.execute('UPDATE live_actions SET state=?,updated_at=?,report=? WHERE id=?',
                (state,now.isoformat(),canonical(report),action_id))
            event={'event_id':action_id+'-report','occurred_at':now.isoformat(),'actor':'agent',
                'synthetic':self.simulation,'intent_id':action_id,'trade_id':action['trade_id'],
                'event_type':'AGENT_ACTION_UNKNOWN' if status=='unknown' else 'AGENT_ACTION_RESULT'}
            if status=='unknown':event['reason']='AGENT_OR_TRANSPORT_OUTCOME_UNKNOWN'
            else:event['details']={'outcome':{'needs_confirmation':'pending_approval','review_only':'pending_approval',
                'cancel_requested':'pending_cancel','rejected':'rejected','submitted':'submitted','pending_approval':'pending_approval'}[status],
                'agent_report':report,'broker_verified':False}
            self.audit.append_in_transaction(event);self.db.commit()
        except BaseException:self.db.rollback();raise

    _NOT_DISPATCHED_PROOFS=frozenset({'RESERVATION_REJECTED_WITHOUT_RECORD',
        'RESERVATION_ERROR_ROLLED_BACK'})

    def abandon_not_dispatched(self,action_id,*,reason,proof):
        """Withdraw a claimed action proven never to have reached a broker.

        This is the local infrastructure's terminal counterpart to a model
        report. It records a distinct audit result, never a broker rejection or
        fill, and is legal only for a claimed action with no durable dispatch
        evidence: no external operation/order identity, no Host wake event and
        no worker reservation. The caller's proof string is recorded, never
        trusted as the proof. ``SUPERSEDED`` is the existing terminal
        "withdrawn before dispatch" state; the trade remains unprotected in
        broker facts, so reconciliation cannot infer safety from this row.
        """
        if (not isinstance(action_id,str) or not action_id
                or not isinstance(reason,str) or not reason or len(reason)>256
                or proof not in self._NOT_DISPATCHED_PROOFS):
            raise ValueError('NOT_DISPATCHED_ARGUMENT_INVALID')
        now=self.clock();self._begin()
        try:
            action=next((a for a in self.actions() if a['id']==action_id),None)
            if action is None or action['state']!='RUNNING':
                raise ValueError('ACTION_NOT_RUNNING')
            evidence=self._dispatch_evidence_for_action(action_id)
            if evidence is not None:
                raise ValueError('NOT_DISPATCHED_DISPATCH_EVIDENCE_PRESENT:'+evidence)
            report={'status':'not_dispatched','message':reason}
            cursor=self.db.execute(
                "UPDATE live_actions SET state='SUPERSEDED',updated_at=?,report=?"
                ' WHERE id=? AND state=? ',(now.isoformat(),canonical(report),
                                           action_id,'RUNNING'))
            if cursor.rowcount!=1:raise ValueError('ACTION_NOT_RUNNING')
            self.audit.append_in_transaction({
                'event_id':action_id+'-not-dispatched','occurred_at':now.isoformat(),
                'event_type':'AGENT_ACTION_RESULT','actor':'agent',
                'synthetic':self.simulation,'intent_id':action_id,
                'trade_id':action['trade_id'],'reason':reason,
                'details':{'outcome':'failed','failure_reason':reason,
                    'dispatch_proof':proof,'broker_verified':False,
                    'agent_report':None}})
            self.db.commit()
            return {'action_id':action_id,'state':'SUPERSEDED','report':report,
                'dispatch_proof':proof,'broker_verified':False}
        except BaseException:
            self.db.rollback();raise

    def _order_refs(self,action):
        return {json.loads(action['payload'])['ref_id']}|{r[0] for r in self.db.execute(
            'SELECT ref_id FROM external_broker_refs WHERE action_id=?',(action['id'],))}

    def _order_matches(self,action,order):
        # A placement response can return a broker-rewritten ref. Its exact
        # order ID is a separate identity, never an approval ID or inferred ref.
        by_id=self.db.execute('SELECT action_id,account_key FROM external_broker_order_ids WHERE order_id=?',
            (order['id'],)).fetchone()
        return matches_order_identity(action['id'],order,logical_and_approval_refs=self._order_refs(action),
            exact_order_owner=by_id,account_key=self.state()['account_key'])

    def pending_approval_ids(self):
        """Exact ids from native placement receipts, never inferred by terms."""
        return [r[0] for r in self.db.execute("""SELECT DISTINCT r.ref_id FROM external_broker_refs r
            JOIN live_actions a ON a.id=r.action_id WHERE a.state IN
            ('RUNNING','UNKNOWN','WAITING_USER','REPORTED') ORDER BY r.ref_id""")]

    def _unresolved_sell_place(self,symbol,snapshot,*,except_id=None):
        for action in self.actions():
            if action['id']==except_id or action['kind'] not in {'protect','exit'}:continue
            payload=json.loads(action['payload'])
            if payload['plan']['symbol']!=symbol or action['state'] in {'APPROVAL_DECLINED','APPROVAL_EXPIRED'}:continue
            native=self.db.execute("SELECT report FROM external_operations WHERE action_id=? AND operation='place'",(action['id'],)).fetchone()
            # A native placement the broker formally refused cannot still
            # become an order. Without this, one refused sell would block every
            # later protective or exit attempt on the symbol forever.
            if native and native[0]:
                try: refused=json.loads(native[0]).get('status')=='rejected'
                except ValueError: refused=False
                if refused:continue
            legacy=(action['report'] and json.loads(action['report']).get('status') in {'pending_approval','submitted','unknown'})
            if not native and not legacy:continue
            observed=[o for o in snapshot['orders'].values() if self._order_matches(action,o)]
            if observed:
                for order in observed:self._check_order_terms(order,payload)
                if all(o['state'] in TERMINAL for o in observed):continue
            # Neither a vanished pending-list row nor a model/native receipt
            # proves that a sell can no longer execute in the future.
            return True
        return False

    def _approval_terminal(self,action,snapshot,trade):
        aliases=[r[0] for r in self.db.execute('SELECT ref_id FROM external_broker_refs WHERE action_id=?',(action['id'],))]
        if not aliases or action['state'] in {'APPROVAL_DECLINED','APPROVAL_EXPIRED','OBSERVED'}:return False
        requested=snapshot.get('requested_approval_ids',[]);states=snapshot.get('approval_states',{})
        evidence=[]
        for identity in aliases:
            value=states.get(identity,{})
            if (identity not in requested or identity in snapshot.get('missing_approval_ids',[])
                    or value.get('approval_id')!=identity or value.get('asset_class')!='ASSET_CLASS_EQUITY'
                    or value.get('is_edited') is not False or value.get('state') not in APPROVAL_NO_ORDER_TERMINAL):
                return False
            evidence.append(value)
        payload=json.loads(action['payload'])
        if any(self._order_matches(action,o) for o in snapshot['orders'].values()):
            raise ValueError('BROKER_APPROVAL_TERMINAL_ORDER_CONTRADICTION')
        formal='APPROVAL_DECLINED' if any(v['state']=='APPROVAL_STATE_DECLINED' for v in evidence) else 'APPROVAL_EXPIRED'
        report={'status':'rejected','message':'FORMAL_'+formal+'_NO_BROKER_ORDER_OBSERVED'}
        self.db.execute('UPDATE live_actions SET state=?,updated_at=?,report=? WHERE id=?',
            (formal,snapshot['observed_at'],canonical(report),action['id']))
        for operation_id, in self.db.execute("SELECT operation_id FROM external_operations WHERE action_id=? AND operation='place'",(action['id'],)).fetchall():
            self.audit.append_in_transaction({'event_id':operation_id+'-approval-terminal',
                'occurred_at':snapshot['observed_at'],'event_type':'AGENT_ACTION_RESULT','actor':'agent',
                'synthetic':self.simulation,'intent_id':operation_id,'trade_id':trade['id'],
                'details':{'outcome':'rejected','approval_states':evidence,'broker_verified':True,
                    'evidence':'fresh_bound_exact_approval_ids_and_complete_order_snapshot'}})
        state='EXIT_'+formal+':'+payload['plan']['symbol']
        if action['kind']=='exit':trade['manual_intervention_required']=state
        action['state']=formal;action['report']=canonical(report)
        return True

    def _planned_exit_cancel(self,trade,order):
        if (not trade.get('exit_requested') or order['state'] not in {'cancelled','canceled'}
                or number(order['filled'])!=0):return False
        for action in self.actions():
            if action['trade_id']!=trade['id'] or action['kind']!='cancel':continue
            payload=json.loads(action['payload'])
            if payload['target_order_id']!=order['id'] or payload['parameters']!={'order_id':order['id']}:continue
            if self.db.execute("SELECT 1 FROM external_operations WHERE action_id=? AND operation='cancel' AND json_extract(report,'$.status')='cancel_requested'",(action['id'],)).fetchone():
                return True
        return False

    def _planned_replacement_cancel(self,trade,order):
        """A terminal old stop replaced by a newly sized stop is not a failure."""
        if order['state'] not in {'cancelled','canceled'} or number(order['filled'])!=0:
            return False
        for action in self.actions():
            if action['trade_id']!=trade['id'] or action['kind']!='cancel':
                continue
            payload=json.loads(action['payload'])
            if (payload.get('target_order_id')==order['id']
                    and payload.get('parameters')=={'order_id':order['id']}
                    and payload.get('reason')=='REPLACE_PROTECTION_AFTER_TERMINAL_CANCEL'):
                return True
        return False

    @staticmethod
    def _check_order_terms(order,payload):
        return check_order_terms(order,payload)

    def validate_external_action(self,action_id,snapshot,*,quote=None):
        """Validate a concrete native-tool handoff; never approve or submit it."""
        now=self.clock();self._snapshot_check(snapshot,now)
        if action_id in self.recovery_status(snapshot)['acknowledged_action_ids']:
            raise ValueError('RECOVERED_ACTION_IS_HISTORICAL_ONLY')
        action=next((a for a in self.actions() if a['id']==action_id),None)
        if not action:raise ValueError('EXTERNAL_ACTION_NOT_FOUND')
        payload=json.loads(action['payload']);kind=action['kind'];params=payload['parameters']
        if kind not in {'entry','protect','exit','cancel'}:raise ValueError('UNSUPPORTED_EXTERNAL_ACTION')
        if action['state'] not in {'READY','WAITING_USER','REVIEW_PENDING'}:raise ValueError('EXTERNAL_ACTION_NOT_AVAILABLE')
        if self.db.execute("SELECT 1 FROM live_actions WHERE state IN ('RUNNING','UNKNOWN')").fetchone():
            raise ValueError('UNRESOLVED_ACTION_BLOCKS_EXTERNAL_DISPATCH')
        if not is_regular(now.isoformat()) or session(now.isoformat())['date']!=self.policy['session_date']:
            raise ValueError('EXTERNAL_ACTION_OUTSIDE_AUTHORIZED_SESSION')
        if now>=timestamp(payload['expires_at']):raise ValueError('EXTERNAL_ACTION_EXPIRED')
        for order in snapshot['orders'].values():
            if self._order_matches(action,order):
                if kind!='cancel':self._check_order_terms(order,payload)
                raise ValueError('EXTERNAL_ACTION_ALREADY_AT_BROKER_RECONCILE')
        symbol=payload['plan']['symbol'];trade=self.state()['trades'][action['trade_id']]
        if kind in {'protect','exit'} and self._unresolved_sell_place(symbol,snapshot,except_id=action_id):
            raise ValueError('UNRESOLVED_SELL_PLACE_BLOCKS_EXTERNAL_DISPATCH')
        if kind=='entry':
            if payload.get('supervised_intent') is True and action['state']=='WAITING_USER':
                raise ValueError('SUPERVISED_ENGINEERING_PROMOTION_REQUIRED')
            if action['state']!='REVIEW_PENDING':
                self._check_entry_gate()
            evidence=trade.get('evidence',{});plan=trade['plan']
            purpose=trade.get('purpose')
            if purpose=='engineering':
                if (not trade['id'].startswith('engineering-')
                        or evidence.get('engineering_scope_authorized') is not True
                        or evidence.get('parameters_user_specified') is not True):
                    raise ValueError('ENTRY_REMAINS_REVIEW_ONLY')
            elif purpose=='strategy':
                # A strategy entry may use a native channel only when the
                # infrastructure qualified it at admission. The qualification
                # is a durable plan fact, not a caller-supplied purpose or ID.
                if evidence.get('qualified_strategy_entry') is not True:
                    raise ValueError('STRATEGY_ENTRY_NOT_QUALIFIED')
            else:
                raise ValueError('ENTRY_REMAINS_REVIEW_ONLY')
            if evidence.get('synthetic') is not self.simulation or not evidence.get('source_sha256'):
                raise ValueError('ENTRY_REMAINS_REVIEW_ONLY')
            if evidence.get('parameters_valid_until') and now>=timestamp(evidence['parameters_valid_until']):
                raise ValueError('ENGINEERING_PARAMETERS_EXPIRED')
            if now>=session(now.isoformat())['close']-timedelta(minutes=10):raise ValueError('ENTRY_CUTOFF')
            expected={'symbol':plan['symbol'],'side':'buy','type':'limit','quantity':str(plan['quantity']),
                'limit_price':plan['entry_ceiling'],'market_hours':'regular_hours','time_in_force':'gfd'}
            if params!=expected:raise ValueError('ENGINEERING_ENTRY_TERMS_CHANGED')
            if action['state']!='REVIEW_PENDING':
                state=self.state()
                recovery=self.recovery_status(snapshot)
                if self.active_issues(snapshot) or trade.get('exit_requested') or any(t['id']!=trade['id'] and t['phase']!='FLAT_VERIFIED' and t['id'] not in recovery['acknowledged_trade_ids'] for t in state['trades'].values()):
                    raise ValueError('EXECUTION_HAS_ACTIVE_TRADE_OR_ISSUE')
                if any(a['id']!=action_id and a['state'] in {'READY','RUNNING','UNKNOWN','WAITING_USER','REPORTED'} and a['id'] not in recovery['acknowledged_action_ids'] for a in self.actions()):
                    raise ValueError('UNRESOLVED_ACTION_BLOCKS_ENTRY')
                account_risk=self.risk_guard.observe(snapshot)
                if account_risk['blocked']:raise ValueError('LIVE_ACCOUNT_RISK_BLOCKED:'+','.join(account_risk['reasons']))
                broker={'account':snapshot['account_mask'],'read_healthy':True,'new_risk_blocked':False,
                    'last_observed_at':snapshot['observed_at'],'positions':snapshot['positions'],
                    'open_equity_orders':sum(o['state'] not in TERMINAL for o in snapshot['orders'].values()),
                    'other_asset_open_orders':snapshot['other_asset_open_orders'],'portfolio':snapshot['portfolio'],
                    'risk_state':account_risk,'buying_power':snapshot['buying_power'],
                    'unleveraged_buying_power':snapshot['unleveraged_buying_power']}
                if not quote:raise ValueError('EXTERNAL_FRESH_QUOTE_REQUIRED')
                check=trial_proposal(self.policy,broker,quote,plan,as_of=now.isoformat())
                if not check['within_trial_limits']:raise ValueError('ENTRY_GATES:'+','.join(check['blockers']))
                if snapshot['pending_approvals']:raise ValueError('PENDING_APPROVAL_BLOCKS_ENTRY')
        elif kind=='cancel':
            target=snapshot['orders'].get(payload['target_order_id'])
            if not target or target['symbol']!=symbol or params!={'order_id':target['id']}:
                raise ValueError('EXTERNAL_CANCEL_TARGET_MISMATCH')
            if target['state'] in TERMINAL:raise ValueError('EXTERNAL_CANCEL_TARGET_ALREADY_TERMINAL')
        else:
            position=snapshot['positions'].get(symbol,{})
            qty=number(position.get('quantity','0'));expected=number(params['quantity'])
            if qty<=0 or qty!=qty.to_integral_value() or qty!=expected or number(position.get('available_for_sells','0'))!=qty:
                raise ValueError('EXTERNAL_ACTION_QUANTITY_NO_LONGER_VALID')
            active=[o for o in snapshot['orders'].values() if o['state'] not in TERMINAL]
            if any(o['symbol']==symbol and o['side']=='sell' for o in active):
                raise ValueError('EXTERNAL_ACTION_OPEN_SELL_ORDER')
            if snapshot['pending_approvals']:raise ValueError('EXTERNAL_ACTION_PENDING_APPROVAL')
            if kind=='exit' and any(o['symbol']==symbol and o['side']=='buy' for o in active):
                raise ValueError('EXTERNAL_EXIT_OPEN_BUY_ORDER')
            if kind=='protect' and (trade['exit_requested'] or number(params['stop_price'])!=number(trade['stop'])):
                raise ValueError('EXTERNAL_PROTECTION_NO_LONGER_CURRENT')
            if not quote or quote.get('symbol')!=symbol:raise ValueError('EXTERNAL_FRESH_QUOTE_REQUIRED')
            for key in ('bid_at','ask_at','observed_at'):
                if not 0<=(now-timestamp(quote[key])).total_seconds()<=15:raise ValueError('EXTERNAL_QUOTE_STALE_OR_FUTURE')
            if not 0<number(quote['bid'])<=number(quote['ask']):raise ValueError('EXTERNAL_QUOTE_INVALID')
            if kind=='protect' and number(params['stop_price'])>=number(quote['bid']):
                raise ValueError('EXTERNAL_STOP_NOT_BELOW_BID')
        return payload

    def _native_entry_authorization(self, action, snapshot, operation):
        """Full owner checks for a native entry review/place packet.

        This runs before the short reservation transaction. The final
        transaction re-reads the durable decision and receipt binding; it never
        recompiles the current bundle while holding the SQLite write lock.
        Internal callers cannot skip this by calling prepare_external_action
        directly: the check lives in this shared preparation boundary.
        """
        if not isinstance(action, dict) or action.get('kind') != 'entry' \
                or operation not in {'review', 'place'}:
            return None
        try:
            trade = self.state().get('trades', {}).get(action.get('trade_id'), {})
        except (ValueError, KeyError, TypeError, OSError, sqlite3.Error):
            raise ValueError('ENTRY_NATIVE_TRADE_STATE_UNAVAILABLE') from None
        purpose = trade.get('purpose')
        evidence = trade.get('evidence')
        self.refresh_current_account_assessment(purpose=purpose)
        decision = self.entry_authority.check(
            source='ExecutionBook.prepare_external_action.' + operation,
            snapshot=snapshot, action=action, purpose=purpose)
        if decision.get('allowed') is not True:
            raise ValueError('ENTRY_AUTHORITY_BLOCKED:' +
                str(decision.get('reason') or 'ENTRY_AUTHORITY_BLOCKED'))
        preflight = self._current_preflight_decision(action=action)
        if preflight.get('allowed') is not True:
            raise ValueError('CURRENT_ENGINEERING_PREFLIGHT_BLOCKED:' +
                ','.join(preflight.get('reasons') or ['UNKNOWN']))
        if not self._preflight_binding_ok(
                preflight, action=action, require_journal=True,
                purpose=purpose, evidence=evidence):
            raise ValueError('CURRENT_ENGINEERING_PREFLIGHT_BINDING_CHANGED')
        return {'preflight': preflight, 'purpose': purpose,
                'evidence': evidence}

    def prepare_external_action(self,action_id,snapshot,*,quote=None,operation_id,operation,reservation_writer=None):
        """Reserve one native operation and record intent, without authorizing a write."""
        self._clear_entry_operation_contexts()
        try:
            return self._prepare_external_action_impl(
                action_id, snapshot, quote=quote, operation_id=operation_id,
                operation=operation, reservation_writer=reservation_writer)
        finally:
            self._clear_entry_operation_contexts()

    def _prepare_external_action_impl(self,action_id,snapshot,*,quote=None,operation_id,operation,reservation_writer=None):
        """Implementation of one native reservation request."""
        if not isinstance(operation_id,str) or not operation_id or len(operation_id)>180:
            raise ValueError('INVALID_EXTERNAL_OPERATION_ID')
        if operation not in {'review','place','cancel'}:raise ValueError('INVALID_EXTERNAL_OPERATION')
        if self.policy.get('capture_only') is True and operation in {'place','cancel'}:
            raise ValueError('CAPTURE_ONLY_POLICY_READ_ONLY')
        # Existing non-authority validation stays first so its specific
        # rejection (expired/stale/active issue/quantity) is preserved.
        action_pre = next((a for a in self.actions() if a['id']==action_id), None)
        if operation=='place' and action_pre is not None \
                and action_pre.get('state')=='REVIEW_PENDING':
            raise ValueError('SUPERVISED_ENGINEERING_PROMOTION_REQUIRED')
        entry_guard = None
        if operation in {'review','place'} and action_pre is not None \
                and action_pre.get('kind') == 'entry':
            self.validate_external_action(action_id, snapshot, quote=quote)
            if not (operation=='review' and action_pre.get('state')=='REVIEW_PENDING'):
                self._begin_entry_operation_context(action_pre, operation)
                entry_guard = self._native_entry_authorization(
                    action_pre, snapshot, operation)
        now=self.clock();self._begin()
        try:
            if self.db.execute('SELECT 1 FROM external_operations WHERE operation_id=?',(operation_id,)).fetchone():
                raise ValueError('EXTERNAL_OPERATION_ALREADY_EXISTS')
            payload=self.validate_external_action(action_id,snapshot,quote=quote)
            if (payload['kind']=='cancel')!=(operation=='cancel'):raise ValueError('EXTERNAL_OPERATION_KIND_MISMATCH')
            if entry_guard is not None:
                current=next((a for a in self.actions() if a['id']==action_id),None)
                if current is None or current.get('kind')!='entry':
                    raise ValueError('ENTRY_NATIVE_ACTION_CHANGED')
                trade=self.state().get('trades',{}).get(current.get('trade_id'),{})
                purpose=trade.get('purpose');evidence=trade.get('evidence')
                final=self.entry_authority.check(
                    source='ExecutionBook.prepare_external_action.' + operation,
                    snapshot=snapshot, action=current, purpose=purpose)
                if final.get('allowed') is not True:
                    raise ValueError('ENTRY_AUTHORITY_BLOCKED:' +
                        str(final.get('reason') or 'ENTRY_AUTHORITY_BLOCKED'))
                if not self._preflight_binding_ok(
                        entry_guard['preflight'], action=current,
                        require_journal=True, purpose=purpose, evidence=evidence,
                        expected_account_key=snapshot.get('account_key')):
                    raise ValueError('CURRENT_ENGINEERING_PREFLIGHT_BINDING_CHANGED')
                if operation == 'place':
                    self._check_engineering_verification(purpose=purpose, evidence=evidence)
            elif operation=='place' and payload['kind']=='entry':
                trade=self.state()['trades'][payload['trade_id']]
                self._check_engineering_verification(purpose=trade.get('purpose'),evidence=trade.get('evidence'))
            self.db.execute('INSERT INTO external_operations VALUES(?,?,?,?,NULL,NULL)',
                (operation_id,action_id,operation,now.isoformat()))
            self.db.execute("UPDATE live_actions SET state='RUNNING',updated_at=? WHERE id=?",(now.isoformat(),action_id))
            self.audit.append_in_transaction({'event_id':operation_id+'-intent','occurred_at':now.isoformat(),
                'event_type':'AGENT_ACTION_INTENT','actor':'agent','synthetic':self.simulation,
                'account':payload['account'],'intent_id':operation_id,'trade_id':payload['trade_id'],
                'ref_id':payload['ref_id'],'symbol':payload['plan']['symbol'],
                'details':{'action':'review' if operation=='review' else 'cancel' if operation=='cancel' else
                    'stop_loss' if payload['kind']=='protect' else payload['kind'],'parameters':payload['parameters'],
                    'origin':'supervised_native_tool_handoff','local_record_is_not_order_authorization':True}})
            if reservation_writer is not None:reservation_writer(payload)
            self.db.commit();return payload
        except BaseException:self.db.rollback();raise

    def record_external_report(self,action_id,report,*,operation_id,broker_ref_id=None,broker_order_id=None,broker_account_key=None,result_writer=None):
        """Store an advisory native-tool report. Only broker observations prove effects."""
        allowed={'needs_confirmation','review_only','pending_approval','submitted','cancel_requested','rejected','unknown'}
        if not isinstance(report,dict) or report.get('status') not in allowed or set(report)-{'status','message','order_id','market_data_disclosure'}:
            raise ValueError('INVALID_EXTERNAL_REPORT')
        now=self.clock();self._begin()
        try:
            row=self.db.execute('SELECT action_id,operation,report,broker_ref_id FROM external_operations WHERE operation_id=?',(operation_id,)).fetchone()
            if not row or row[0]!=action_id:raise ValueError('EXTERNAL_OPERATION_NOT_PREPARED')
            encoded=canonical(report)
            if row[2] is not None:
                if row[2]!=encoded or row[3]!=broker_ref_id:raise ValueError('EXTERNAL_REPORT_CONFLICT')
                observed=self.db.execute('SELECT order_id,account_key FROM external_broker_order_ids WHERE operation_id=?',(operation_id,)).fetchone()
                if (observed and observed!=(broker_order_id,broker_account_key)
                        or not observed and broker_order_id is not None):raise ValueError('EXTERNAL_ORDER_ID_REPORT_CONFLICT')
                if result_writer is not None:
                    result_writer();self.db.commit()
                else:self.db.rollback()
                return {'duplicate':True}
            statuses={'review':{'needs_confirmation','review_only','rejected','unknown'},
                'place':{'pending_approval','submitted','rejected','unknown'},'cancel':{'cancel_requested','rejected','unknown'}}
            if report['status'] not in statuses[row[1]]:raise ValueError('EXTERNAL_REPORT_OPERATION_MISMATCH')
            action=next(a for a in self.actions() if a['id']==action_id)
            late_host_receipt=False
            if action['state']=='UNKNOWN' and result_writer is not None and row[1] in {'place','cancel'}:
                # Only an exact still-open native intent marked uncertain by
                # this Host bridge may receive its original late tool result.
                # This grants no retry/dispatch permission to arbitrary UNKNOWN.
                native=(self.db.execute("SELECT 1 FROM native_operation_records WHERE id=? AND action_id=? AND operation=? AND state='STARTED'",(operation_id,action_id,row[1])).fetchone()
                    if self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='native_operation_records'").fetchone() else None)
                marker=self.db.execute('SELECT payload FROM audit_events WHERE event_id=?',(operation_id+'-host-unknown',)).fetchone()
                event=json.loads(marker[0]) if marker else {}
                late_host_receipt=bool(native and event.get('intent_id')==operation_id
                    and event.get('event_type')=='AGENT_ACTION_UNKNOWN'
                    and event.get('reason') in {'HOST_HANDOFF_EXPIRED','HOST_CONTROLLER_STOPPED','HOST_PACKET_PUBLICATION_FAILED','HOST_RECEIPT_INVALID'})
            if action['state'] not in {'RUNNING','OBSERVED'} and not (
                    action['state']=='REVIEW_PENDING' and row[1]=='review') \
                    and not late_host_receipt:raise ValueError('EXTERNAL_OPERATION_NOT_RUNNING')
            if broker_order_id is not None:
                if (row[1]!='place' or report['status'] not in {'submitted','rejected'}
                        or not isinstance(broker_order_id,str) or not 0<len(broker_order_id)<=200
                        or report.get('order_id')!=broker_order_id or broker_account_key!=self.state()['account_key']):
                    raise ValueError('INVALID_EXPLICIT_BROKER_ORDER_ID')
                owner=self.db.execute('SELECT action_id,account_key FROM external_broker_order_ids WHERE order_id=?',(broker_order_id,)).fetchone()
                if owner and owner!=(action_id,broker_account_key):raise ValueError('BROKER_ORDER_ID_OWNER_CONFLICT')
                self.db.execute('INSERT OR IGNORE INTO external_broker_order_ids VALUES(?,?,?,?)',
                    (broker_order_id,action_id,operation_id,broker_account_key))
            if broker_ref_id is not None:
                if (row[1]!='place' or report['status'] not in {'pending_approval','submitted','rejected','unknown'}
                        or not isinstance(broker_ref_id,str) or not broker_ref_id or len(broker_ref_id)>200):
                    raise ValueError('INVALID_EXPLICIT_BROKER_REF_ALIAS')
                owner=self.db.execute('SELECT action_id FROM external_broker_refs WHERE ref_id=?',(broker_ref_id,)).fetchone()
                if owner and owner[0]!=action_id:raise ValueError('BROKER_REF_ALIAS_CONFLICT')
                if any(broker_ref_id==json.loads(a['payload'])['ref_id'] and a['id']!=action_id for a in self.actions()):
                    raise ValueError('BROKER_REF_ALIAS_CONFLICT')
                self.db.execute('INSERT OR IGNORE INTO external_broker_refs VALUES(?,?,?)',(broker_ref_id,action_id,operation_id))
            status=report['status'];read_failure=row[1]=='review' and status=='unknown'
            state='WAITING_USER' if read_failure or status in {'needs_confirmation','review_only'} else 'UNKNOWN' if status=='unknown' else 'REPORTED'
            if action['state']!='OBSERVED':
                self.db.execute('UPDATE live_actions SET state=?,updated_at=?,report=? WHERE id=?',(state,now.isoformat(),encoded,action_id))
            self.db.execute('UPDATE external_operations SET report=?,broker_ref_id=? WHERE operation_id=?',(encoded,broker_ref_id,operation_id))
            event={'event_id':operation_id+'-report','occurred_at':now.isoformat(),'actor':'agent',
                'synthetic':self.simulation,'intent_id':operation_id,'trade_id':action['trade_id'],
                'event_type':'AGENT_ACTION_UNKNOWN' if status=='unknown' and not read_failure else 'AGENT_ACTION_RESULT'}
            if read_failure:event['details']={'outcome':'failed','external_report':report,'broker_verified':False,'read_only_operation':True}
            elif status=='unknown':event['reason']='NATIVE_TOOL_OUTCOME_UNKNOWN_RECONCILE_BEFORE_RETRY'
            else:event['details']={'outcome':{'needs_confirmation':'pending_approval','review_only':'pending_approval',
                'pending_approval':'pending_approval','submitted':'submitted','cancel_requested':'pending_cancel','rejected':'rejected'}[status],
                'external_report':report,'broker_verified':False,'origin':'supervised_native_tool_handoff'}
            self.audit.append_in_transaction(event)
            if result_writer is not None:result_writer()
            self.db.commit();return {'duplicate':False,'broker_verified':False}
        except BaseException:self.db.rollback();raise

    def _resolve_external_intents(self,action,order,snapshot):
        for operation_id,report in self.db.execute("SELECT operation_id,report FROM external_operations WHERE action_id=? AND operation!='review'",(action['id'],)).fetchall():
            if report is not None and json.loads(report)['status']!='unknown':continue
            self.audit.append_in_transaction({'event_id':operation_id+'-broker-resolved',
                'occurred_at':snapshot['observed_at'],'event_type':'AGENT_ACTION_RESULT','actor':'agent',
                'synthetic':self.simulation,'intent_id':operation_id,'trade_id':action['trade_id'],
                'broker_order_id':order['id'],'details':{'outcome':'observed_fill' if number(order['filled'])>0 else
                    'cancelled' if order['state'] in {'cancelled','canceled'} else 'rejected' if order['state']=='rejected'
                    else 'failed' if order['state'] in TERMINAL else 'submitted',
                    'target_state':order['state'],'evidence':'fresh_complete_broker_snapshot','reconciled_by':'script'}})

    def recover(self):
        # The supervisor lock excludes legacy child workers, but a root-chat
        # native call deliberately spans short CLI processes without that lock.
        # Its unfinished reservation is not evidence that its owner died. Keep
        # the original operation open for its genuine receipt or broker proof;
        # RUNNING already blocks every competing submission.
        pending_native=[];legacy_unknown=[]
        for a in self.actions():
            if a['state']=='RUNNING':
                pending=self.db.execute('SELECT operation_id,operation FROM external_operations WHERE action_id=? AND report IS NULL',(a['id'],)).fetchall()
                if pending:
                    pending_native.extend({'operation_id':identity,'action_id':a['id'],'operation':operation,
                        'state':'RUNNING','required_action':'RECONCILE_BROKER_OR_RECORD_ORIGINAL_NATIVE_RECEIPT',
                        'owner_liveness':'NOT_INFERRED_FROM_SUPERVISOR_LOCK','resubmission_permitted':False}
                        for identity,operation in pending)
                else:
                    self.report(a['id'],{'status':'unknown','message':'WORKER_INTERRUPTED_RECONCILE_BEFORE_RETRY'})
                    legacy_unknown.append(a['id'])
        return {'recovery_disposition':'NATIVE_HANDOFF_REQUIRES_RECONCILIATION_OR_ORIGINAL_RECEIPT' if pending_native
                else 'LEGACY_WORKER_RECONCILIATION_REQUIRED' if legacy_unknown else 'NO_INTERRUPTED_ACTION',
            'native_operations_pending':pending_native,'legacy_unknown_action_ids':legacy_unknown,
            'native_operations_changed':0,'automatic_resubmission_permitted':False}

    def request_engineering_exit(self,trade_id,snapshot):
        """Request the authorized engineering round-trip's normal close path.

        Records a local management decision only. It neither confirms a native
        cancellation/order nor bypasses unresolved writes or terminal checks.
        """
        self.reconcile(snapshot)
        now=self.clock();self._begin()
        try:
            self._snapshot_check(snapshot,now)
            state=self.state();trade=state['trades'].get(trade_id)
            if not trade or trade.get('purpose')!='engineering':raise ValueError('ENGINEERING_TRADE_REQUIRED')
            if self.db.execute("SELECT 1 FROM live_actions WHERE state IN ('RUNNING','UNKNOWN')").fetchone():
                raise ValueError('UNRESOLVED_ACTION_BLOCKS_ENGINEERING_EXIT_REQUEST')
            position=snapshot['positions'].get(trade['plan']['symbol'],{})
            quantity=number(position.get('quantity','0'))
            if (quantity<=0 or quantity!=number(trade.get('quantity','0')) or not trade.get('first_fill_at')
                    or trade.get('phase')=='FLAT_VERIFIED'):
                raise ValueError('ENGINEERING_EXIT_REQUIRES_OBSERVED_POSITION_AND_FILL')
            trade['exit_requested']=True
            self._save(state)
            event_id='engineering-exit-'+digest(trade_id)[:32]
            duplicate=bool(self.db.execute('SELECT 1 FROM audit_events WHERE event_id=?',(event_id,)).fetchone())
            if not duplicate:self.audit.append_in_transaction({'event_id':event_id,
                'occurred_at':now.isoformat(),'event_type':'ENGINEERING_EXIT_REQUESTED',
                'actor':'script','synthetic':self.simulation,'account':snapshot['account_mask'],
                'trade_id':trade_id,'symbol':trade['plan']['symbol'],
                'details':{'reason':'AUTHORIZED_ENGINEERING_ROUND_TRIP_COMPLETION',
                    'quantity':str(quantity),'broker_snapshot_at':snapshot['observed_at'],
                    'local_record_is_not_order_authorization':True}})
            self.db.commit()
        except BaseException:self.db.rollback();raise
        return {'requested':True,'duplicate':duplicate,'trade_id':trade_id,'broker_effect_verified':False,
            'summary':self.reconcile(snapshot)}

    def flat_evidence(self,trade_id):
        """Authoritative facts about one trade, for whoever may release it.

        The point of this method is that the numbers come from the book, not
        from the caller. A settlement claim built anywhere else cannot prove the
        account is flat, so the admission service accepts evidence only from an
        object that exposes this method.
        """
        state=self.state();trade=state['trades'].get(trade_id)
        if trade is None:raise ValueError('FLAT_EVIDENCE_UNKNOWN_TRADE')
        # Order facts and their observation time must come from a snapshot that
        # was actually accepted for this account. Absence is refused, not read
        # as "no open orders", and the clock is never substituted for it.
        observed=state.get('last_snapshot')
        account=state.get('account_key')
        orders=state.get('broker_orders')
        if not isinstance(observed,str) or not observed:
            raise ValueError('FLAT_EVIDENCE_SNAPSHOT_REQUIRED')
        if not isinstance(account,str) or not account:
            raise ValueError('FLAT_EVIDENCE_ACCOUNT_REQUIRED')
        if not isinstance(orders,dict):
            raise ValueError('FLAT_EVIDENCE_ORDERS_REQUIRED')
        # Every identity this trade has ever been submitted under: the
        # action's own ref, any approval alias, and any external broker order
        # id. Matching on the ref alone would miss a protection or exit whose
        # ref was rewritten by the broker.
        # Every exact identity this trade's actions were submitted under. The
        # book keeps broker-rewritten refs and exact order ids in their own
        # tables; reading an attribute name would silently find nothing.
        refs,order_ids=set(),set()
        for action in self.actions():
            if action.get('trade_id')!=trade_id:continue
            payload=_safe_payload(action)
            if payload.get('ref_id'):refs.add(payload['ref_id'])
            member=self._order_refs(action)
            refs.update(member if isinstance(member,(set,list,tuple)) else ())
            for row in self.db.execute(
                    'SELECT order_id FROM external_broker_order_ids WHERE action_id=?',
                    (action['id'],)):
                order_ids.add(row[0])
        open_orders=[order for order in orders.values()
                     if (order.get('ref_id') in refs
                         or order.get('id') in refs
                         or order.get('id') in order_ids
                         or order.get('external_broker_order_id') in order_ids)
                     and order.get('state') not in
                     {'filled','cancelled','canceled','rejected','expired'}]
        active_actions=[a for a in self.actions() if a.get('trade_id')==trade_id
                        and a['state'] in {'READY','RUNNING','REPORTED','WAITING_USER'}]
        unknown=[a for a in self.actions() if a['state']=='UNKNOWN']
        # Account-wide approvals come from the snapshot, not from the locally
        # tracked actions: an approval for another trade still blocks release.
        approvals=state.get('broker_approvals')
        if not isinstance(approvals,dict):
            raise ValueError('FLAT_EVIDENCE_APPROVALS_REQUIRED')
        count,ids,states=approvals.get('count'),approvals.get('ids'),approvals.get('states')
        if (not isinstance(count,int) or not isinstance(ids,list)
                or not isinstance(states,dict) or count<0):
            raise ValueError('FLAT_EVIDENCE_APPROVALS_REQUIRED')
        # The provider reports a pending count, the ids behind that count, and
        # separately the states of every tracked approval. A count that
        # disagrees with its own list, or a pending id with no reported state,
        # is not evidence that nothing is awaiting a decision.
        missing=approvals.get('missing') or []
        # ``approval_states`` only covers *tracked* ids, so a pending id is not
        # required to appear there; ``missing`` is the provider's own statement
        # that a tracked approval's state could not be read.
        if count!=len(ids) or missing:
            raise ValueError('FLAT_EVIDENCE_APPROVALS_INCONSISTENT')
        return {'source':'execution_book','account_key':account,
            'trade_id':trade_id,'phase':trade.get('phase'),
            'remaining_quantity':str(trade.get('quantity') or '0'),
            'open_orders':len(open_orders),'active_actions':len(active_actions),
            'pending_approvals':count+len(self.pending_approval_ids()),
            'unknown_intents':len(unknown),'observed_at':observed}

    def reconcile(self,snapshot,*,quotes=None,features=None,exit_advices=None):
        """Reconcile broker facts and manage open trades.

        ``features`` is an opaque per-symbol mapping handed to the exit strategy.
        The book does not know or name any indicator: a strategy that wants an
        EMA receives one under whatever key it declares, and a strategy that
        wants nothing receives an empty mapping.
        """
        now=self.clock();self._snapshot_check(snapshot,now);quotes=quotes or {};features=features or {}
        advices=None if exit_advices is None else dict(exit_advices)
        self.risk_guard.observe(snapshot)
        self._begin()
        try:
            self._snapshot_check(snapshot,now)
            state=self.state();state['account_key']=snapshot['account_key'];state['last_snapshot']=snapshot['observed_at']
            # Persist the authoritative order facts the snapshot carried. They
            # are what release evidence is derived from; without them a settled
            # trade could not be shown to have had no open orders.
            state['broker_orders']={key:{'ref_id':order.get('ref_id'),'state':order.get('state'),
                'remaining':order.get('remaining'),'id':order.get('id'),
                'external_broker_order_id':order.get('external_broker_order_id')}
                for key,order in snapshot['orders'].items()}
            # The provider shape is count:int, pending_approval_ids:list and
            # approval_states:dict. Iterating the count is a TypeError the
            # moment a real approval exists, which would stop management.
            approvals=snapshot.get('pending_approvals')
            state['broker_approvals']={'count':int(approvals) if isinstance(approvals,int) else None,
                'ids':[item for item in (snapshot.get('pending_approval_ids') or [])],
                'states':dict(snapshot.get('approval_states') or {}),
                'missing':[item for item in (snapshot.get('missing_approval_ids') or [])]}
            previous_issues=set(state['issues']);current_explanation=set();mismatch_symbols=set()
            orders=snapshot['orders'];known=set();active_symbols=set()
            retirement=self.recovery_status(snapshot)
            for trade in state['trades'].values():
                if trade['id'] in retirement['acknowledged_trade_ids']:
                    # Preserve the failed trade/action state exactly as pinned by
                    # acknowledgement; current broker facts remain independently checked.
                    continue
                symbol=trade['plan']['symbol'];trade['snapshot_at']=snapshot['observed_at']
                own=[]
                for a in self.actions():
                    if a['trade_id']!=trade['id']:continue
                    payload=json.loads(a['payload'])
                    matched=[o for o in orders.values() if self._order_matches(a,o)]
                    self._approval_terminal(a,snapshot,trade)
                    for alias, in self.db.execute('SELECT ref_id FROM external_broker_refs WHERE action_id=?',(a['id'],)):
                        value=snapshot.get('approval_states',{}).get(alias,{})
                        if value.get('is_edited') is True:
                            state['issues']=sorted(set(state['issues']+['APPROVAL_EDITED_REQUIRES_MANUAL_RECONCILIATION:'+symbol]))
                        elif value and value.get('state') not in {'APPROVAL_STATE_PENDING','APPROVAL_STATE_APPROVED','APPROVAL_STATE_AUTO_APPROVED'}|APPROVAL_NO_ORDER_TERMINAL:
                            state['issues']=sorted(set(state['issues']+['APPROVAL_STATE_UNKNOWN:'+symbol]))
                    if (not matched and a['kind']=='entry' and now>=timestamp(payload['expires_at'])
                            and (a['state']=='READY' or self._review_only_action(a))):
                        # A never-placed entry has no latent broker effect. A
                        # submitted/pending/UNKNOWN operation must instead be
                        # resolved from the broker, regardless of local expiry.
                        self.db.execute("UPDATE live_actions SET state='EXPIRED',updated_at=? WHERE id=?",(now.isoformat(),a['id']))
                        a['state']='EXPIRED'
                    if len(matched)>1:raise ValueError('DUPLICATE_BROKER_REF_ID')
                    if matched:
                        if a['state'] in {'APPROVAL_DECLINED','APPROVAL_EXPIRED'}:
                            raise ValueError('BROKER_APPROVAL_TERMINAL_ORDER_CONTRADICTION')
                        order=matched[0]
                        self._check_order_terms(order,payload)
                        known.add(order['id']);own.append((a,order))
                        if a['state'] in {'RUNNING','UNKNOWN'}:
                            self._resolve_external_intents(a,order,snapshot)
                            if self.db.execute('SELECT 1 FROM audit_events WHERE event_id=?',(a['id']+'-intent',)).fetchone():self.audit.append_in_transaction({'event_id':a['id']+'-broker-resolved',
                                'occurred_at':snapshot['observed_at'],'event_type':'AGENT_ACTION_RESULT',
                                'actor':'agent','synthetic':self.simulation,'intent_id':a['id'],
                                'trade_id':trade['id'],'broker_order_id':order['id'],
                                'details':{'outcome':'observed_fill' if number(order['filled'])>0 else 'submitted',
                                    'reconciled_by':'script','evidence':'fresh_complete_broker_snapshot'}})
                        self.db.execute("UPDATE live_actions SET state='OBSERVED',updated_at=? WHERE id=?",(now.isoformat(),a['id']))
                    if a['kind']=='cancel' and payload['target_order_id'] in orders and orders[payload['target_order_id']]['state'] in TERMINAL:
                        if a['state'] in {'RUNNING','UNKNOWN'}:
                            target_order=orders[payload['target_order_id']]
                            self._resolve_external_intents(a,target_order,snapshot)
                            outcome=('observed_fill' if number(target_order['filled'])>0 else 'cancelled'
                                if target_order['state'] in {'cancelled','canceled','partially_filled_rest_cancelled'} else 'failed')
                            if self.db.execute('SELECT 1 FROM audit_events WHERE event_id=?',(a['id']+'-intent',)).fetchone():self.audit.append_in_transaction({'event_id':a['id']+'-broker-resolved',
                                'occurred_at':snapshot['observed_at'],'event_type':'AGENT_ACTION_RESULT',
                                'actor':'agent','synthetic':self.simulation,'intent_id':a['id'],
                                'trade_id':trade['id'],'broker_order_id':target_order['id'],
                                'details':{'outcome':outcome,'target_terminal_state':target_order['state'],
                                    'reconciled_by':'script','evidence':'fresh_complete_broker_snapshot'}})
                        self.db.execute("UPDATE live_actions SET state='OBSERVED',updated_at=? WHERE id=?",(now.isoformat(),a['id']))
                if trade['phase']=='FLAT_VERIFIED':
                    if any(o['state'] not in TERMINAL for _,o in own):
                        raise ValueError('CLOSED_TRADE_ORDER_BECAME_ACTIVE')
                    self._supersede_undispatched(trade['id'],{'protect','exit','cancel'},now)
                    continue
                entry=[o for a,o in own if a['kind']=='entry']
                position=snapshot['positions'].get(symbol)
                qty=number(position['quantity']) if position else Decimal(0)
                if qty<0 or qty!=qty.to_integral_value():raise ValueError('UNSUPPORTED_SHORT_OR_FRACTIONAL_POSITION')
                fills=sum((number(o['filled'])*(1 if o['side']=='buy' else -1) for _,o in own),Decimal(0))
                if qty!=fills:
                    issue='POSITION_NOT_EXPLAINED_BY_OWN_EXECUTIONS:'+symbol
                    current_explanation.add(issue);mismatch_symbols.add(symbol)
                    state['issues']=sorted(set(state['issues']+[issue]));continue
                trade['quantity']=str(qty)
                active=[o for a,o in own if o['state'] not in TERMINAL]
                # Cancel intents may only be superseded after their exact target reaches terminal.
                if qty==0:
                    if active:
                        trade['phase']='ENTRY_OPEN' if any(o['side']=='buy' for o in active) else 'RESIDUAL_EXIT_ORDER'
                        for o in active:
                            if o['side']=='buy':
                                if (self.risk_guard.state() or {}).get('blocked',True):
                                    self._cancel(trade,o,now,'ACCOUNT_RISK_LOCK_CANCEL_ENTRY')
                                elif (now-timestamp(o['created_at'])).total_seconds()>=60:
                                    self._cancel(trade,o,now,'UNFILLED_ENTRY_TIMEOUT')
                        if trade['phase']=='RESIDUAL_EXIT_ORDER':
                            for o in active:self._cancel(trade,o,now,'ZERO_POSITION_RESIDUAL_ORDER')
                    elif entry and any(number(o['filled'])>0 for o in entry):trade['phase']='FLAT_VERIFIED'
                    elif entry:trade['phase']='FLAT_VERIFIED'
                    else:
                        a=next(a for a in self.actions() if a['id']==trade['entry_action'])
                        trade['phase']='FLAT_VERIFIED' if a['state'] in {'EXPIRED','APPROVAL_DECLINED','APPROVAL_EXPIRED'} else 'ENTRY_AWAITING_BROKER'
                    if trade['phase']=='FLAT_VERIFIED':
                        self._supersede_undispatched(trade['id'],{'protect','exit','cancel'},now)
                    continue
                active_symbols.add(symbol)
                if not entry:raise ValueError('POSITION_WITHOUT_OWN_ENTRY')
                avg=sum(number(o['average_fill_price'])*number(o['filled']) for o in entry if number(o['filled'])>0)/sum(number(o['filled']) for o in entry)
                trade['entry_price']=str(avg)
                if not trade.get('first_fill_at'):
                    trade['first_fill_at']=min(e['executed_at'] for o in entry for e in o['executions'])
                # Partial entry first protects observed quantity; remaining buy cancellation is separate.
                for o in active:
                    if o['side']=='buy':self._cancel(trade,o,now,'PARTIAL_ENTRY_CANCEL_REMAINDER')
                sell_orders=[o for o in active if o['side']=='sell']
                reserved=sum((number(o['remaining']) for o in sell_orders),Decimal(0))
                if reserved>qty:raise ValueError('EXIT_ORDERS_EXCEED_ACTUAL_POSITION')
                quote=quotes.get(symbol);decision={'action':'HOLD','reason':'NO_STRATEGY_EXIT_ADVICE'}
                if quote:
                    # The hard stop is infrastructure, not strategy: it holds
                    # even when the plugin is stopped, hung or wrong.
                    if number(quote['bid'])<=number(trade['stop']):
                        decision={'action':'EXIT_ALL','reason':'HARD_STOP_BREACH'}
                    elif advices is not None:
                        decision=self._decision_from_advice(trade,advices.get(trade['id']),now)
                    elif getattr(self,'exit_decider',None) is not None:
                        decision=self.exit_decider(
                            {'stop':trade['stop'],'initial_stop':trade['initial_stop'],'entry_price':trade['entry_price']},
                            quote,as_of=now.isoformat(),features=features.get(symbol),allow_overnight=False)
                diagnostic=decision.get('strategy_diagnostic')
                if quote and diagnostic != trade.get('strategy_exit_diagnostic'):
                    if diagnostic is not None or 'strategy_exit_diagnostic' in trade:
                        trade['strategy_exit_diagnostic']=diagnostic
                        self.audit.append_in_transaction({'event_id':'strategy-advisory-'+digest([trade['id'],now.isoformat(),diagnostic])[:32],
                            'occurred_at':now.isoformat(),'event_type':'RECONCILIATION_ISSUE','actor':'script',
                            'synthetic':self.simulation,'account':snapshot['account_mask'],'trade_id':trade['id'],
                            'details':{'status':'STRATEGY_ADVISORY_UNAVAILABLE' if diagnostic else 'STRATEGY_ADVISORY_RECOVERED',
                                'diagnostic':diagnostic,'management_continues':True}})
                if decision['action']=='EXIT_ALL':trade['exit_requested']=True
                elif decision['action']=='RAISE_STOP':
                    # A tighter recommendation is not an adopted/live stop.
                    # No safe native amend/replace channel has been qualified.
                    # Preserve effective protection instead of making it look
                    # insufficient and cancelling it before a replacement exists.
                    suggested=decision['stop']
                    if number(suggested)>number(trade.get('suggested_stop',trade['stop'])):
                        trade['suggested_stop']=suggested
                        trade['stop_adjustment']={'status':'DEFERRED',
                            'current_stop':trade['stop'],'suggested_stop':suggested,
                            'reason':decision['reason'],
                            'deferred_reason':'SAFE_NATIVE_REPLACEMENT_NOT_VERIFIED',
                            'suggested_at':now.isoformat()}
                        self.audit.append_in_transaction({
                            'event_id':'stop-adjustment-deferred-'+digest([trade['id'],trade['stop'],suggested])[:32],
                            'occurred_at':now.isoformat(),'event_type':'SHADOW_TRANSITION',
                            'actor':'script','synthetic':self.simulation,
                            'account':snapshot['account_mask'],'trade_id':trade['id'],'symbol':symbol,
                            'details':{**trade['stop_adjustment'],'broker_effect_verified':False,
                                'effective_stop_unchanged':True,'quote_sha256':digest(quote)}})
                market=session(now.isoformat())
                if market and market['close']-timedelta(minutes=5)<=now<market['close']:
                    trade['exit_requested']=True;decision={'action':'EXIT_ALL','reason':'INTRADAY_CLOSE_POLICY'}
                rejected_protection=any(a['trade_id']==trade['id'] and a['kind']=='protect'
                    and a['report'] and json.loads(a['report']).get('status')=='rejected' for a in self.actions())
                failed_stops=[o for a,o in own if a['kind']=='protect' and o['state'] in TERMINAL
                    and number(o['filled'])==0 and o['stop'] is not None and number(o['stop'])>=number(trade['stop'])
                    and not self._planned_exit_cancel(trade,o)
                    and not self._planned_replacement_cancel(trade,o)]
                if rejected_protection or failed_stops:
                    reason='PROTECTION_REJECTED' if rejected_protection else 'PROTECTION_TERMINATED_UNCOVERED'
                    state['issues']=sorted(set(state['issues']+[reason+':'+symbol]))
                    trade['exit_requested']=True;decision={'action':'EXIT_ALL','reason':reason}
                good=[o for o in sell_orders if is_regular(now.isoformat()) and o['type']=='stop_market' and o['stop'] is not None and number(o['stop'])>=number(trade['stop'])
                    and o['state'] in {'confirmed','partially_filled'} and valid_session_order(o,now)]
                protected=sum((number(o['remaining']) for o in good),Decimal(0))
                if protected==qty:
                    trade.pop('unprotected_since',None)
                    trade.update(last_full_coverage_observed_at=snapshot['observed_at'],
                        last_full_coverage_observed_quantity=str(qty))
                    if not trade.get('first_protection_observed_at'):
                        trade.update(first_protection_observed_at=snapshot['observed_at'],
                            first_protection_observed_quantity=str(qty),
                            protection_observation_delay_seconds=str((timestamp(snapshot['observed_at'])-timestamp(trade['first_fill_at'])).total_seconds()),
                            protection_timing_basis='first_fill_to_first_full_coverage_observation',
                            protection_timing_synthetic=self.simulation)
                else:
                    # A late observation must not restart the naked-position
                    # budget. First exposure is dated by actual executions. For
                    # renewed exposure the last complete coverage observation
                    # is a conservative bound, NOT the exact stop cancellation
                    # time. New partial-fill tranches retain execution timing.
                    previous_coverage=trade.get('last_full_coverage_observed_at')
                    anchor=timestamp(previous_coverage or trade['first_fill_at'])
                    basis=('last_full_coverage_observation_conservative_bound' if previous_coverage
                        else 'first_actual_entry_execution')
                    if previous_coverage:
                        cumulative=Decimal(0);new_fill_at=None
                        entry_fills=sorted((e for o in entry for e in o['executions']),
                            key=lambda e:timestamp(e['executed_at']))
                        for execution in entry_fills:
                            cumulative+=number(execution['quantity'])
                            if cumulative>number(trade['last_full_coverage_observed_quantity']):
                                new_fill_at=timestamp(execution['executed_at']);break
                        if new_fill_at is not None:
                            trade['unprotected_new_entry_execution_at']=new_fill_at.isoformat()
                            # If the old tranche is still demonstrably covered,
                            # only the newly executed shares are naked. Their
                            # actual execution date is the exposure boundary.
                            old_quantity=number(trade['last_full_coverage_observed_quantity'])
                            if qty>old_quantity and protected>=old_quantity:
                                anchor=new_fill_at;basis='additional_actual_entry_execution_old_tranche_still_covered'
                            elif new_fill_at<anchor:
                                anchor=new_fill_at;basis='additional_actual_entry_execution'
                    if not trade.get('unprotected_since'):
                        trade.update(unprotected_first_observed_at=snapshot['observed_at'],
                            unprotected_detection_delay_seconds=str((timestamp(snapshot['observed_at'])-anchor).total_seconds()),
                            unprotected_timing_synthetic=self.simulation)
                    elif timestamp(trade['unprotected_since'])<anchor:
                        anchor=timestamp(trade['unprotected_since'])
                        basis=trade.get('unprotected_timing_basis','prior_unprotected_observation_conservative_bound')
                    trade.update(unprotected_since=anchor.isoformat(),unprotected_timing_basis=basis)
                    if (now-anchor).total_seconds()>=30:
                        issue='PROTECTION_WAIT_EXCEEDED:'+symbol
                        state['issues']=sorted(set(state['issues']+[issue]))
                        incident_id='protection-deadline-'+digest([trade['id'],anchor.isoformat()])[:32]
                        if not self.db.execute('SELECT 1 FROM audit_events WHERE event_id=?',(incident_id,)).fetchone():
                            self.audit.append_in_transaction({'event_id':incident_id,
                                'occurred_at':now.isoformat(),'event_type':'RECONCILIATION_ISSUE','actor':'script',
                                'synthetic':self.simulation,'account':snapshot['account_mask'],'trade_id':trade['id'],'symbol':symbol,
                                'details':{'issues':[issue],'unprotected_since':anchor.isoformat(),
                                    'broker_snapshot_at':snapshot['observed_at'],'normal_lifecycle_accepted':False}})
                        trade['exit_requested']=True
                        if decision['action']!='EXIT_ALL':decision={'action':'EXIT_ALL','reason':'PROTECTION_WAIT_EXCEEDED'}
                if not is_regular(now.isoformat()):trade['phase']='OUTSIDE_PROTECTION_SESSION';continue
                if trade['exit_requested']:
                    trade['phase']='EXIT_REQUESTED'
                    self._supersede_undispatched(trade['id'],{'protect'},now)
                    # A sell sized before an outstanding buy finishes can leave
                    # a new unprotected position when that buy fills afterwards.
                    if any(o['side']=='buy' for o in active):continue
                    for o in sell_orders:
                        if o['type']=='stop_market':self._cancel(trade,o,now,'CANCEL_BEFORE_EXIT')
                    if (not sell_orders and not snapshot['pending_approvals'] and number(position['available_for_sells'])==qty
                            and not self._unresolved_sell_place(symbol,snapshot)):
                        self._queue(trade,'exit',{'symbol':symbol,'side':'sell','type':'market','quantity':str(qty),
                            'market_hours':'regular_hours','time_in_force':'gfd'},now,decision.get('reason','EXIT_POLICY'))
                    continue
                trade['phase']='PROTECTED' if protected==qty else 'NEEDS_PROTECTION'
                if protected!=qty:
                    # A submitted stop has not failed and has not been
                    # acknowledged by the broker yet. Cancelling it just because
                    # confirmation is one observation late creates a
                    # cancel/stop-fill race and can leave the position naked.
                    # Wait for the submitted order to become confirmed or
                    # terminal; only replace orders that are already terminal,
                    # or whose terms are wrong/insufficient. Numeric quantities
                    # are compared by value, and split submitted stops must
                    # sum to the actual position (not each equal it).
                    pending_stop = self._submitted_stop_covers(
                        sell_orders, trade, qty, now)
                    if sell_orders and not pending_stop:
                        for o in sell_orders:self._cancel(trade,o,now,'REPLACE_PROTECTION_AFTER_TERMINAL_CANCEL')
                    elif not sell_orders and number(position['available_for_sells'])==qty and not self._unresolved_sell_place(symbol,snapshot):
                        if quote and number(quote['bid'])<=number(trade['stop']):
                            trade['exit_requested']=True
                        else:self._queue(trade,'protect',{'symbol':symbol,'side':'sell','type':'stop_market',
                            'quantity':str(qty),'stop_price':trade['stop'],'market_hours':'regular_hours','time_in_force':'gtc'},now,'PROTECT_ACTUAL_FILLED_QUANTITY')
            unmanaged=[s for s,p in snapshot['positions'].items() if number(p['quantity'])!=0 and s not in active_symbols]
            foreign=[o['id'] for o in orders.values() if o['state'] not in TERMINAL and o['id'] not in known]
            if unmanaged or foreign:
                current_explanation.add('UNMANAGED_POSITION_OR_ORDER')
                state['issues']=sorted(set(state['issues']+['UNMANAGED_POSITION_OR_ORDER']))
            if current_explanation:
                self.audit.append_in_transaction({'event_id':'execution-explanation-'+digest([snapshot['observed_at'],sorted(current_explanation)])[:32],
                    'occurred_at':snapshot['observed_at'],'event_type':'RECONCILIATION_ISSUE','actor':'script',
                    'synthetic':self.simulation,'account':snapshot['account_mask'],
                    'details':{'status':'observed','issues':sorted(current_explanation),
                        'broker_snapshot_sha256':digest(snapshot),'historical_issues_retained':True}})
            # A late official approval alias can explain a transient accounting
            # gap. Keep provenance from the FIRST occurrence so a later matching
            # order cannot silently erase a historical or unrelated exception.
            origins=state.setdefault('pending_alias_explanation_issues',{})
            pending_symbols={json.loads(a['payload'])['plan']['symbol'] for a in self.actions()
                if self.db.execute("SELECT 1 FROM external_operations WHERE action_id=? AND operation='place' AND (report IS NULL OR json_extract(report,'$.status')='unknown')",(a['id'],)).fetchone()}
            affected=mismatch_symbols|set(unmanaged)
            implicated=[o for o in orders.values() if o['id'] not in known and
                (o['id'] in foreign or o['symbol'] in affected and number(o['filled'])>0)]
            for issue in current_explanation:
                candidates=[o for o in implicated if issue=='UNMANAGED_POSITION_OR_ORDER'
                    or o['symbol']==issue.split(':',1)[1]]
                eligible=bool(candidates) and all(o.get('ref_id') and o['symbol'] in pending_symbols for o in candidates)
                if issue not in previous_issues and eligible:
                    origins[issue]={'observed_at':snapshot['observed_at'],
                        'orders':{o['id']:o['ref_id'] for o in candidates}}
                elif issue in origins:
                    # Newly unexplained exposure without a pending native place
                    # loses automatic resolution eligibility permanently.
                    added=[o for o in candidates if o['id'] not in origins[issue]['orders']]
                    if added and not all(o.get('ref_id') and o['symbol'] in pending_symbols for o in added):
                        origins.pop(issue)
                    else:origins[issue]['orders'].update({o['id']:o['ref_id'] for o in added})
            unresolved=any(a['state'] in {'RUNNING','UNKNOWN'} for a in self.actions())
            for issue,origin in list(origins.items()):
                if issue in current_explanation or unresolved:continue
                proof=origin['orders']
                if not proof or not all(oid in known and oid in orders and orders[oid].get('ref_id')==ref
                        and (self.db.execute('SELECT 1 FROM external_broker_refs WHERE ref_id=?',(ref,)).fetchone()
                            or self.db.execute('SELECT 1 FROM external_broker_order_ids WHERE order_id=? AND account_key=?',
                                (oid,snapshot['account_key'])).fetchone())
                        for oid,ref in proof.items()):continue
                state['issues']=[i for i in state['issues'] if i!=issue]
                self.audit.append_in_transaction({'event_id':'alias-issue-resolved-'+digest([issue,origin])[:32],
                    'occurred_at':snapshot['observed_at'],'event_type':'RECONCILIATION_ISSUE','actor':'script',
                    'synthetic':self.simulation,'account':snapshot['account_mask'],
                    'details':{'status':'resolved','issue':issue,'broker_verified':True,
                        'evidence':'explicit_native_alias_exact_order_terms_and_execution_reconciliation',
                        'first_gap_observed_at':origin['observed_at'],'broker_orders':proof}})
                origins.pop(issue)
            state['issues']=sorted(set(state['issues']+[t['manual_intervention_required']
                for t in state['trades'].values() if t.get('manual_intervention_required')]))
            self._save(state);self.db.commit();return self.summary()
        except BaseException:self.db.rollback();raise

    def _cancel(self,trade,order,now,reason):
        return self._queue(trade,'cancel',{'order_id':order['id']},now,reason,target=order['id'])

    def _submitted_stop_covers(self, sell_orders, trade, qty, now):
        """Whether submitted stop orders are a potentially exact protection.

        This is intentionally only a wait decision: submitted orders are never
        counted as active coverage. Numeric strings are compared by value, and
        split orders must sum to the actual position. Any wrong term, missing
        numeric quantity, or total mismatch returns False so existing
        replacement/terminal rules remain in force.
        """
        if not sell_orders:
            return False
        total=Decimal(0)
        for order in sell_orders:
            if (order.get('type')!='stop_market'
                    or order.get('state')!='submitted'
                    or order.get('stop') is None
                    or number(order['stop'])<number(trade['stop'])
                    or not valid_session_order(order,now)):
                return False
            remaining=order.get('remaining')
            if remaining is None:remaining=order.get('quantity')
            try:value=number(remaining)
            except (ValueError,TypeError):
                return False
            if value<=0:return False
            total+=value
        return total==qty

    def summary(self):
        actions=self.actions();recovery=self.recovery_status();return {'mode':'execution_lifecycle','synthetic':self.simulation,
            'state':self.state(),'risk':self.risk_guard.state(),'actions':[{k:v for k,v in a.items() if k!='payload'} for a in actions],
            'active_issues':self.active_issues(),'recovery':recovery,
            'effective_ready_actions':sum(a['state']=='READY' and a['id'] not in recovery['acknowledged_action_ids'] for a in actions),
            'effective_unresolved_actions':sum(a['state'] in {'RUNNING','UNKNOWN','WAITING_USER','REPORTED'} and a['id'] not in recovery['acknowledged_action_ids'] for a in actions),
            'ready_actions':sum(a['state']=='READY' for a in actions),
            'unresolved_actions':sum(a['state'] in {'RUNNING','UNKNOWN','WAITING_USER','REPORTED'} for a in actions),
            'broker_execution_verified':False,'live_orders_by_this_script':0}
