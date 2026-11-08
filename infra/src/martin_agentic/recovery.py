"""Audited manual recovery. This module has no broker write capability.

Acknowledgement establishes eligibility for a *new* supervised admission check;
it neither rewrites failed lifecycle evidence nor enables execution. Historical
issues, actions, risk locks and account high-water marks remain untouched.
"""
import json
import hashlib
import uuid
import sqlite3
from pathlib import Path
from datetime import datetime, timezone
from decimal import Decimal

from .audit import AuditTrail, canonical, digest
from .broker import TERMINAL, APPROVAL_NO_ORDER_TERMINAL
from .ledger import timestamp
from .risk import number
from .transport import TransportError


def _result_order_id(event):
    """Read only explicit legacy order-ID slots, never generic IDs or refs."""
    details = event.get('details', {})
    external = details.get('external_report', {})
    identities = {v for v in (event.get('broker_order_id'),details.get('broker_order_id'),
                              details.get('order_id'),external.get('order_id')) if v is not None}
    if len(identities) > 1 or any(not isinstance(v,str) or not v for v in identities):
        raise ValueError('RECOVERY_RECEIPT_ORDER_ID_CONFLICT')
    return next(iter(identities),None)


def unresolved_write_intents(connection, snapshot, *, account_mask, simulation=False):
    """Read exact terminal-write proof, including writes outside live_actions.

    A flat position does not disprove a delayed placement. Reuse recovery's
    terminal order / individually queried terminal approval evidence; previews
    and module-level intents are handled by their existing lifecycle checks.
    This helper never changes intent, audit, or broker state.
    """
    tables={r[0] for r in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    events=([json.loads(r[0]) for r in connection.execute('SELECT payload FROM audit_events ORDER BY sequence')]
            if 'audit_events' in tables else [])
    intents={e['intent_id']:e for e in events if e['event_type']=='AGENT_ACTION_INTENT'}
    results={}
    for event in events:
        if event['event_type']!='AGENT_ACTION_RESULT':continue
        intent=intents.get(event.get('intent_id'),{})
        if (event.get('synthetic') is simulation and intent.get('synthetic') is simulation
                and intent.get('account')==account_mask and event.get('account') in {None,account_mask}):
            identity=_result_order_id(event)
            if identity:results.setdefault(event['intent_id'],set()).add(identity)
    orders=snapshot.get('orders',{})
    def terminal(identity):
        return identity in orders and orders[identity]['state'] in TERMINAL
    def no_order(operation):
        if not {'external_operations','external_broker_refs','external_broker_order_ids'}<=tables:return False
        row=connection.execute('SELECT operation FROM external_operations WHERE operation_id=?',(operation,)).fetchone()
        if not row or row[0]!='place':return False
        aliases=[r[0] for r in connection.execute('SELECT ref_id FROM external_broker_refs WHERE operation_id=?',(operation,))]
        if not aliases or not set(aliases)<=set(snapshot.get('requested_approval_ids',[])):return False
        if connection.execute('SELECT 1 FROM external_broker_order_ids WHERE operation_id=?',(operation,)).fetchone():return False
        for alias in aliases:
            approval=snapshot.get('approval_states',{}).get(alias,{})
            if (alias in snapshot.get('missing_approval_ids',[]) or approval.get('approval_id')!=alias
                    or approval.get('asset_class')!='ASSET_CLASS_EQUITY' or approval.get('is_edited') is not False
                    or approval.get('state') not in APPROVAL_NO_ORDER_TERMINAL
                    or any(o.get('ref_id')==alias for o in orders.values())):return False
        return True
    unresolved=set()
    if 'external_operations' in tables:
        for operation,kind in connection.execute('SELECT operation_id,operation FROM external_operations'):
            if kind=='review':continue
            identities=results.get(operation,set())
            if not (identities and all(terminal(i) for i in identities)) and not no_order(operation):unresolved.add(operation)
    module_ids={r[0] for r in connection.execute('SELECT id FROM live_actions')} if 'live_actions' in tables else set()
    for identity,event in intents.items():
        if event['details']['action']=='review' or identity in module_ids:continue
        if event.get('synthetic') is not simulation or event.get('account')!=account_mask:
            unresolved.add(identity);continue
        identities=results.get(identity,set())
        if not identities and no_order(identity):continue
        if not identities or not all(terminal(i) for i in identities):unresolved.add(identity);continue
        if event['details']['action']=='cancel' and identities!={event['details']['parameters'].get('order_id')}:
            unresolved.add(identity)
    return sorted(unresolved)


class RecoveryCases:
    def __init__(self, book):
        self.book = book
        self.db = book.db
        self.db.execute('''CREATE TABLE IF NOT EXISTS recovery_cases(
            id TEXT PRIMARY KEY,trade_id TEXT NOT NULL,status TEXT NOT NULL,payload TEXT NOT NULL)''')
        self.db.execute('''CREATE TABLE IF NOT EXISTS protection_incident_dispositions(
            case_id TEXT PRIMARY KEY,payload TEXT NOT NULL)''')
        self.db.commit()

    def get(self, case_id):
        row = self.db.execute('SELECT payload FROM recovery_cases WHERE id=?', (case_id,)).fetchone()
        if not row:
            raise ValueError('RECOVERY_CASE_NOT_FOUND')
        case = json.loads(row[0])
        matching = [e for e in self._events() if e.get('details',{}).get('recovery_case_id') == case_id]
        if not matching or matching[-1]['details'].get('case_sha256') != digest(case):
            raise ValueError('RECOVERY_CASE_AUDIT_BINDING_INVALID')
        return case

    def _events(self):
        return [json.loads(r[0]) for r in self.db.execute('SELECT payload FROM audit_events ORDER BY sequence')]

    def required_approval_ids(self):
        """Recovery rechecks terminal aliases too, unlike normal pending polls."""
        return [r[0] for r in self.db.execute('SELECT ref_id FROM external_broker_refs ORDER BY ref_id')]

    def _save(self, case):
        self.db.execute('UPDATE recovery_cases SET status=?,payload=? WHERE id=?',
                        (case['status'], canonical(case), case['id']))
        self.book.audit.append_in_transaction({
            'event_id':case['id']+'-'+case['status'], 'occurred_at':self.book.clock().isoformat(),
            'event_type':'RECONCILIATION_ISSUE', 'actor':'script', 'synthetic':self.book.simulation,
            'account':case['account_mask'], 'trade_id':case['trade_id'],
            'details':{'recovery_case_id':case['id'], 'status':case['status'],
                       'manual_takeover':True, 'case_sha256':digest(case),
                       'historical_module_failure_preserved':True,
                       'automatic_execution_enabled':False}})

    def open(self, trade_id, *, intent_ids, incident_order_ids, manual_order_ids,
             execution_ids, cancelled_order_ids, reason):
        """Pin exact known identities; symbols and flatness cannot replace them."""
        self.book._begin()
        try:
            state = self.book.state()
            if trade_id not in state['trades'] or not state.get('account_key'):
                raise ValueError('RECOVERY_ORIGINAL_TRADE_OR_ACCOUNT_MISSING')
            if not isinstance(reason, str) or not reason.strip():
                raise ValueError('RECOVERY_REASON_REQUIRED')
            fields = dict(intent_ids=intent_ids, incident_order_ids=incident_order_ids,
                          manual_order_ids=manual_order_ids, execution_ids=execution_ids,
                          cancelled_order_ids=cancelled_order_ids)
            for name, values in fields.items():
                if (not isinstance(values, list) or len(values) != len(set(values))
                        or any(not isinstance(v, str) or not 0 < len(v) <= 256 for v in values)):
                    raise ValueError('RECOVERY_EXACT_IDENTITIES_REQUIRED:'+name)
            if not intent_ids or not incident_order_ids or not manual_order_ids or not execution_ids:
                raise ValueError('RECOVERY_EXACT_IDENTITIES_REQUIRED')
            if set(incident_order_ids) & set(manual_order_ids):
                raise ValueError('RECOVERY_MANUAL_MODULE_OWNERSHIP_OVERLAP')
            orders = set(incident_order_ids + manual_order_ids)
            if not set(cancelled_order_ids) <= orders:
                raise ValueError('RECOVERY_CANCEL_TARGET_NOT_LINKED')
            events = self._events()
            intents = {e['intent_id']:e for e in events if e['event_type']=='AGENT_ACTION_INTENT'}
            if any(i not in intents for i in intent_ids):
                raise ValueError('RECOVERY_INTENT_NOT_DURABLE')
            if not any(intents[i].get('trade_id') == trade_id for i in intent_ids):
                raise ValueError('RECOVERY_ORIGINAL_INTENT_NOT_LINKED')
            if any(intents[i].get('synthetic') is not self.book.simulation
                   or intents[i].get('account') != '••••'+self.book.policy['account_suffix']
                   for i in intent_ids):
                raise ValueError('RECOVERY_INTENT_ACCOUNT_OR_MODE_MISMATCH')
            if self.db.execute('SELECT 1 FROM recovery_cases WHERE trade_id=?', (trade_id,)).fetchone():
                raise ValueError('RECOVERY_ALREADY_OPEN')
            case = {'id':'recovery-'+str(uuid.uuid4()), 'trade_id':trade_id, 'status':'OPEN',
                    'account_key':state['account_key'], 'account_mask':'••••'+self.book.policy['account_suffix'],
                    'synthetic':self.book.simulation, 'manual_takeover':True,
                    'reason':reason, 'opened_at':self.book.clock().isoformat(),
                    'historical_issues':list(state['issues']), **fields}
            self.db.execute('INSERT INTO recovery_cases VALUES(?,?,?,?)',
                            (case['id'],trade_id,case['status'],canonical(case)))
            self._save(case)
            self.db.commit()
            return case
        except BaseException:
            self.db.rollback()
            raise

    def _validate_order_linkage(self, case, orders, events, intents, results):
        """Require original pre-dispatch ownership; retrospective imports override explicitly."""
        expected = set(case['incident_order_ids'] + case['manual_order_ids'])
        linked = {e.get('broker_order_id') for e in results if e.get('intent_id') in case['intent_ids']}
        if not expected <= linked:
            raise ValueError('RECOVERY_ORDER_NOT_LINKED_TO_DURABLE_INTENT')
        for identity in expected:
            linked_intents = [intents[e['intent_id']] for e in results
                              if e.get('broker_order_id') == identity and e['intent_id'] in case['intent_ids']]
            write_intents = [e for e in linked_intents if e['details']['action'] not in {'review','cancel'}]
            if not write_intents:
                raise ValueError('RECOVERY_ORDER_PLACEMENT_INTENT_MISSING')
            if identity in case['incident_order_ids'] and not any(e.get('trade_id') == case['trade_id'] for e in write_intents):
                raise ValueError('RECOVERY_INCIDENT_TRADE_MISMATCH')
            order = orders[identity]
            for event in write_intents:
                params = event['details']['parameters']
                for parameter,field in [('symbol','symbol'),('side','side'),('type','type'),
                                        ('time_in_force','time_in_force'),('market_hours','market_hours')]:
                    if params.get(parameter) != order.get(field):
                        raise ValueError('RECOVERY_ORDER_INTENT_TERMS_MISMATCH')
                for parameter,field in [('quantity','quantity'),('stop_price','stop'),('limit_price','limit_price')]:
                    actual, requested = order.get(field), params.get(parameter)
                    if (actual is None) != (requested is None) or actual is not None and number(actual) != number(requested):
                        raise ValueError('RECOVERY_ORDER_INTENT_TERMS_MISMATCH')

    def _safe_evidence(self, case, snapshot):
        self.book._snapshot_check(snapshot, self.book.clock())
        if self.book.simulation or snapshot.get('provider') != 'robinhood':
            raise ValueError('RECOVERY_REQUIRES_REAL_BROKER_OBSERVATION')
        if snapshot.get('account_key') != case['account_key']:
            raise ValueError('RECOVERY_ACCOUNT_MISMATCH')
        # A caller-supplied dictionary is insufficient: it must be the latest
        # complete collected observation, already persisted by BrokerJournal.
        try:
            current_row = self.db.execute('SELECT payload FROM broker_current WHERE id=1').fetchone()
            health_row = self.db.execute('SELECT payload FROM broker_health WHERE id=1').fetchone()
            source_digest = hashlib.sha256(json.dumps(snapshot,sort_keys=True).encode()).hexdigest()
            observation = self.db.execute('SELECT payload FROM broker_observations WHERE digest=?', (source_digest,)).fetchone()
        except Exception as exc:
            raise ValueError('RECOVERY_PERSISTED_BROKER_EVIDENCE_MISSING') from exc
        current = json.loads(current_row[0]) if current_row else {}
        health = json.loads(health_row[0]) if health_row else {}
        if (health.get('healthy') is not True or health.get('at') != snapshot['observed_at']
                or current.get('observed_at') != snapshot['observed_at']
                or current.get('account_key') != case['account_key']):
            raise ValueError('RECOVERY_LATEST_HEALTHY_SNAPSHOT_REQUIRED')
        journal_only = {'reconciliation_issues','new_executions_seen','baseline_established'}
        if canonical({k:v for k,v in current.items() if k not in journal_only}) != canonical(snapshot):
            raise ValueError('RECOVERY_SNAPSHOT_DIFFERS_FROM_JOURNAL')
        if not observation or canonical(json.loads(observation[0])) != canonical(snapshot):
            raise ValueError('RECOVERY_PERSISTED_BROKER_EVIDENCE_MISSING')
        if current.get('reconciliation_issues'):
            raise ValueError('RECOVERY_BROKER_RECONCILIATION_UNRESOLVED')
        if any(number(p['quantity']) != 0 or number(p.get('held_for_sells','0')) != 0
               for p in snapshot['positions'].values()):
            raise ValueError('RECOVERY_ACCOUNT_NOT_FLAT')
        orders = snapshot['orders']
        if any(o['state'] not in TERMINAL for o in orders.values()):
            raise ValueError('RECOVERY_OPEN_OR_UNKNOWN_ORDER')
        if any(g.get('state') not in TERMINAL for g in snapshot.get('groups', [])):
            raise ValueError('RECOVERY_OPEN_OR_UNKNOWN_GROUP')
        if snapshot['pending_approvals'] or snapshot.get('pending_approval_ids') or snapshot.get('missing_approval_ids'):
            raise ValueError('RECOVERY_APPROVAL_UNRESOLVED')
        tracked = set(self.required_approval_ids())
        if not tracked <= set(snapshot.get('requested_approval_ids', [])):
            raise ValueError('RECOVERY_TRACKED_APPROVAL_NOT_QUERIED')
        for identity in tracked:
            approval = snapshot.get('approval_states', {}).get(identity, {})
            if (approval.get('is_edited') is not False
                    or approval.get('state') not in APPROVAL_NO_ORDER_TERMINAL | {'APPROVAL_STATE_APPROVED','APPROVAL_STATE_AUTO_APPROVED'}):
                raise ValueError('RECOVERY_APPROVAL_UNRESOLVED')
            if approval['state'] not in APPROVAL_NO_ORDER_TERMINAL and not any(o.get('ref_id') == identity for o in orders.values()):
                raise ValueError('RECOVERY_APPROVED_WRITE_NOT_OBSERVED')
        expected = set(case['incident_order_ids'] + case['manual_order_ids'])
        if not expected <= set(orders):
            raise ValueError('RECOVERY_EXACT_ORDER_MISSING')
        if any(orders[i]['state'] not in {'cancelled','canceled'} for i in case['cancelled_order_ids']):
            raise ValueError('RECOVERY_CANCEL_NOT_TERMINAL')
        all_executions = [e for i in expected for e in orders[i]['executions']]
        executions = {e['id']:e for e in all_executions}
        if len(executions) != len(all_executions):
            raise ValueError('RECOVERY_DUPLICATE_EXECUTION_ID')
        if set(case['execution_ids']) != set(executions):
            raise ValueError('RECOVERY_EXECUTION_IDENTITY_MISMATCH')
        net = {}
        for identity in expected:
            order = orders[identity]
            if order.get('id') != identity:
                raise ValueError('RECOVERY_ORDER_IDENTITY_MISMATCH')
            total = Decimal(0)
            for execution in order['executions']:
                if (execution.get('order_id') != identity or execution['symbol'] != order['symbol']
                        or execution['side'] != order['side'] or number(execution['quantity']) <= 0
                        or timestamp(execution['executed_at']) > timestamp(snapshot['observed_at'])):
                    raise ValueError('RECOVERY_EXECUTION_INVALID')
                quantity = number(execution['quantity'])
                total += quantity
                net[order['symbol']] = net.get(order['symbol'], Decimal(0)) + quantity * (1 if order['side']=='buy' else -1)
            if total != number(order['filled']):
                raise ValueError('RECOVERY_EXECUTION_QUANTITY_MISMATCH')
        if any(v != 0 for v in net.values()):
            raise ValueError('RECOVERY_LINKED_EXECUTIONS_NOT_FLAT')
        events = self._events()
        intents = {e['intent_id']:e for e in events if e['event_type']=='AGENT_ACTION_INTENT'}
        results = [{**e,'broker_order_id':_result_order_id(e)} for e in events if e['event_type']=='AGENT_ACTION_RESULT'
                   and e.get('synthetic') is False and e.get('account') in {None,case['account_mask']}
                   and intents.get(e['intent_id'],{}).get('account') == case['account_mask']
                   and intents.get(e['intent_id'],{}).get('synthetic') is False]
        self._validate_order_linkage(case, orders, events, intents, results)
        def proven_no_order(operation):
            row = self.db.execute('SELECT operation FROM external_operations WHERE operation_id=?', (operation,)).fetchone()
            if not row or row[0] != 'place':
                return False
            aliases = [r[0] for r in self.db.execute('SELECT ref_id FROM external_broker_refs WHERE operation_id=?', (operation,))]
            if not aliases or not set(aliases) <= set(snapshot.get('requested_approval_ids', [])):
                return False
            for alias in aliases:
                approval = snapshot.get('approval_states',{}).get(alias,{})
                if (alias in snapshot.get('missing_approval_ids', []) or approval.get('approval_id') != alias
                        or approval.get('asset_class') != 'ASSET_CLASS_EQUITY' or approval.get('is_edited') is not False
                        or approval.get('state') not in APPROVAL_NO_ORDER_TERMINAL
                        or any(o.get('ref_id') == alias for o in orders.values())):
                    return False
            if self.db.execute('SELECT 1 FROM external_broker_order_ids WHERE operation_id=?', (operation,)).fetchone():
                return False
            return True

        # No unrelated pending write can be hidden by a flat account snapshot.
        for operation, action_id, kind, report in self.db.execute('SELECT operation_id,action_id,operation,report FROM external_operations'):
            if kind == 'review':
                continue
            linked_orders = [e.get('broker_order_id') for e in results if e.get('intent_id') == operation]
            if linked_orders and all(i in orders and orders[i]['state'] in TERMINAL for i in linked_orders):
                continue
            if proven_no_order(operation):
                continue
            raise ValueError('RECOVERY_UNKNOWN_OR_UNRESOLVED_WRITE:'+operation)
        for action in self.book.actions():
            if action['state'] in {'RUNNING','UNKNOWN'}:
                raise ValueError('RECOVERY_ACTION_RECONCILIATION_REQUIRED:'+action['id'])
        for event in events:
            if event['event_type'] != 'AGENT_ACTION_INTENT' or event['details']['action'] == 'review':
                continue
            if event.get('synthetic') is not False or event.get('account') != case['account_mask']:
                raise ValueError('RECOVERY_UNBOUND_WRITE_INTENT')
            matching = [e for e in results if e['intent_id'] == event['intent_id']]
            identities = [e.get('broker_order_id') for e in matching if e.get('broker_order_id')]
            if not identities and proven_no_order(event['intent_id']):
                continue
            if not identities or any(i not in orders or orders[i]['state'] not in TERMINAL for i in identities):
                raise ValueError('RECOVERY_MANUAL_WRITE_UNRESOLVED:'+event['intent_id'])
            if event['details']['action'] == 'cancel' and (
                    not event['details']['parameters'].get('order_id')
                    or set(identities) != {event['details']['parameters']['order_id']}):
                raise ValueError('RECOVERY_CANCEL_INTENT_TARGET_MISMATCH')
        return {'snapshot_sha256':digest(snapshot), 'observed_at':snapshot['observed_at'],
                'order_ids':sorted(expected), 'execution_ids':sorted(executions),
                'all_account_assets_flat':True, 'all_orders_terminal':True,
                'pending_approvals':0, 'unknown_writes':0, 'broker_journal_issues':current.get('reconciliation_issues', [])}

    def verify(self, case_id, snapshot):
        self.book._begin()
        try:
            case = self.get(case_id)
            if case['status'] != 'OPEN':
                raise ValueError('RECOVERY_NOT_OPEN')
            evidence = self._safe_evidence(case, snapshot)
            case.update(status='BROKER_SAFE_VERIFIED', verified_at=self.book.clock().isoformat(), evidence=evidence)
            self._save(case)
            self.db.commit()
            return case
        except BaseException:
            self.db.rollback()
            raise

    def acknowledge(self, case_id, snapshot, *, reason):
        """Recheck safety at acknowledgement; never inherit execution authority."""
        self.book._begin()
        try:
            case = self.get(case_id)
            if case['status'] != 'BROKER_SAFE_VERIFIED' or not isinstance(reason, str) or not reason.strip():
                raise ValueError('RECOVERY_VERIFICATION_AND_ACK_REASON_REQUIRED')
            evidence = self._safe_evidence(case, snapshot)
            actions = [a for a in self.book.actions() if a['trade_id'] == case['trade_id']]
            state = self.book.state()
            symbol = state['trades'][case['trade_id']]['plan']['symbol']
            permitted = {'UNMANAGED_POSITION_OR_ORDER', 'POSITION_NOT_EXPLAINED_BY_OWN_EXECUTIONS:'+symbol}
            case.update(status='ACKNOWLEDGED', acknowledged_at=self.book.clock().isoformat(),
                        acknowledgement_reason=reason, acknowledgement_evidence=evidence,
                        supervised_admission_recheck_allowed=True, automatic_execution_enabled=False,
                        retired_actions={a['id']:digest(a) for a in actions},
                        covered_issue_ids=sorted(set(case['historical_issues']) & set(state['issues']) & permitted),
                        terminal_order_sha256={i:digest(snapshot['orders'][i]) for i in evidence['order_ids']},
                        audit_sequence=self.db.execute('SELECT coalesce(max(sequence),0) FROM audit_events').fetchone()[0])
            self._save(case)
            self.db.commit()
            return case
        except BaseException:
            self.db.rollback()
            raise

    def dispose_protection_incident(self, case_id, snapshot, *, reason):
        """Explicitly dispose one historical incident; never accept its lifecycle.

        The original ACK/case, failed trade, actions and risk state stay intact.
        A separate audit-bound addendum permits only a new admission evaluation.
        """
        self.book._begin()
        try:
            case=self.get(case_id);state=self.book.state()
            if case['status']!='ACKNOWLEDGED' or not isinstance(reason,str) or not reason.strip():
                raise ValueError('PROTECTION_DISPOSITION_ACK_AND_REASON_REQUIRED')
            issue='PROTECTION_WAIT_EXCEEDED:'+state['trades'][case['trade_id']]['plan']['symbol']
            if issue not in case.get('historical_issues',[]) or issue not in state['issues']:
                raise ValueError('PROTECTION_DISPOSITION_ORIGINAL_HISTORICAL_ISSUE_REQUIRED')
            proof=recovery_disposition(self.book,snapshot)
            if proof['blockers'] or case['trade_id'] not in proof['acknowledged_trade_ids']:
                raise ValueError('PROTECTION_DISPOSITION_VALID_ACK_REQUIRED')
            for seq,raw in self.db.execute('SELECT sequence,payload FROM audit_events WHERE sequence>?',(case['audit_sequence'],)):
                event=json.loads(raw)
                if (event['event_type']=='RECONCILIATION_ISSUE'
                        and issue in event.get('details',{}).get('issues',[])):
                    raise ValueError('PROTECTION_DISPOSITION_ISSUE_RECURRED')
            evidence=self._safe_evidence(case,snapshot)
            risk=self.book.risk_guard.state()
            if not risk or risk.get('account_key')!=case['account_key']:
                raise ValueError('PROTECTION_DISPOSITION_BOUND_RISK_REQUIRED')
            prior=self.db.execute('SELECT payload FROM protection_incident_dispositions WHERE case_id=?',(case_id,)).fetchone()
            if prior:
                result=json.loads(prior[0]);self.db.commit();return result
            disposition={'version':1,'id':'protection-disposition-'+digest(case_id)[:32],
                'case_id':case_id,'case_sha256':digest(case),'trade_id':case['trade_id'],
                'trade_sha256':digest(state['trades'][case['trade_id']]),'issue_id':issue,
                'account_key':case['account_key'],'account_mask':case['account_mask'],
                'retired_actions':case['retired_actions'],'terminal_order_sha256':case['terminal_order_sha256'],
                'ack_audit_sequence':case['audit_sequence'],'evidence':evidence,
                'risk_state_sha256':digest(risk),'reason':reason,'disposed_at':self.book.clock().isoformat(),
                'historical_module_failure_preserved':True,'normal_lifecycle_accepted':False,
                'production_ready':False,'automatic_execution_enabled':False,'risk_locks_unchanged':True}
            self.db.execute('INSERT INTO protection_incident_dispositions VALUES(?,?)',(case_id,canonical(disposition)))
            self.book.audit.append_in_transaction({'event_id':disposition['id'],'occurred_at':disposition['disposed_at'],
                'event_type':'RECONCILIATION_ISSUE','actor':'script','synthetic':False,
                'account':case['account_mask'],'trade_id':case['trade_id'],
                'details':{'status':'HISTORICAL_PROTECTION_INCIDENT_DISPOSED',
                    'protection_disposition_id':disposition['id'],'disposition_sha256':digest(disposition),
                    'historical_module_failure_preserved':True,'normal_lifecycle_accepted':False,
                    'automatic_execution_enabled':False,'risk_locks_unchanged':True}})
            if digest(self.book.risk_guard.state())!=digest(risk):raise ValueError('PROTECTION_DISPOSITION_RISK_CHANGED')
            self.db.commit();return disposition
        except BaseException:self.db.rollback();raise


def recovery_disposition(book, snapshot):
    """Read-only retirement proof for admission; never forgives new failures.

    Current positions may belong to later trades. The proof instead requires
    unchanged terminal identities for the recovered trade and its original
    actions, plus a nonregressing fresh account-bound broker observation.
    Caller still performs ordinary account, exposure and risk reconciliation.
    """
    result = {'acknowledged_trade_ids':[], 'acknowledged_action_ids':[],
              'covered_issue_ids':[], 'blockers':[], 'automatic_execution_enabled':False}
    if not book.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='recovery_cases'").fetchone():
        return result
    try:
        book.audit.verify()
        book._snapshot_check(snapshot, book.clock())
    except (ValueError, KeyError, TransportError):
        result['blockers'].append('RECOVERY_CURRENT_OBSERVATION_INVALID')
        return result
    rows = book.db.execute("SELECT payload FROM recovery_cases WHERE status='ACKNOWLEDGED'").fetchall()
    actions = {a['id']:a for a in book.actions()}
    events = [(seq,json.loads(payload)) for seq,payload in book.db.execute('SELECT sequence,payload FROM audit_events')]
    for row in rows:
        case = json.loads(row[0])
        case_events = [event for _,event in events if event.get('details',{}).get('recovery_case_id') == case['id']]
        if (not case_events or case_events[-1]['details'].get('status') != 'ACKNOWLEDGED'
                or case_events[-1]['details'].get('case_sha256') != digest(case)):
            result['blockers'].append('RECOVERY_CASE_AUDIT_BINDING_INVALID:'+case['id'])
            continue
        evidence = case.get('acknowledgement_evidence', {})
        if (case.get('account_key') != snapshot.get('account_key') or case.get('synthetic') is not book.simulation
                or not case.get('manual_takeover') or not evidence.get('all_account_assets_flat')
                or not evidence.get('all_orders_terminal') or evidence.get('unknown_writes') != 0
                or timestamp(snapshot['observed_at']) < timestamp(evidence['observed_at'])):
            result['blockers'].append('RECOVERY_ACK_BINDING_INVALID:'+case['id'])
            continue
        pinned = case.get('terminal_order_sha256', {})
        retired = case.get('retired_actions', {})
        if (not pinned or any(i not in snapshot['orders'] or digest(snapshot['orders'][i]) != fp for i,fp in pinned.items())
                or any(i not in actions or digest(actions[i]) != fp for i,fp in retired.items())
                or any(a['trade_id']==case['trade_id'] and a['id'] not in retired for a in actions.values())):
            result['blockers'].append('RECOVERY_TERMINAL_EVIDENCE_CHANGED:'+case['id'])
            continue
        issues = set(case.get('covered_issue_ids', []))
        if book.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='protection_incident_dispositions'").fetchone():
            addition=book.db.execute('SELECT payload FROM protection_incident_dispositions WHERE case_id=?',(case['id'],)).fetchone()
            if addition:
                addition=json.loads(addition[0]);issue='PROTECTION_WAIT_EXCEEDED:'+book.state()['trades'][case['trade_id']]['plan']['symbol']
                audit=[event for _,event in events if event.get('event_id')==addition.get('id')]
                if (type(addition.get('version')) is not int or addition['version']!=1
                        or addition.get('case_id')!=case['id'] or addition.get('case_sha256')!=digest(case)
                        or addition.get('trade_sha256')!=digest(book.state()['trades'][case['trade_id']])
                        or addition.get('account_key')!=case['account_key'] or addition.get('issue_id')!=issue
                        or issue not in case.get('historical_issues',[])
                        or addition.get('retired_actions')!=retired or addition.get('terminal_order_sha256')!=pinned
                        or addition.get('ack_audit_sequence')!=case['audit_sequence']
                        or addition.get('normal_lifecycle_accepted') is not False
                        or addition.get('automatic_execution_enabled') is not False
                        or len(audit)!=1 or audit[0].get('details',{}).get('disposition_sha256')!=digest(addition)):
                    result['blockers'].append('PROTECTION_DISPOSITION_AUDIT_BINDING_INVALID:'+case['id']);continue
                issues.add(issue)
        repeated = set()
        for seq,event in events:
            if seq <= case.get('audit_sequence', -1) or event.get('event_type') != 'RECONCILIATION_ISSUE':
                continue
            details = event.get('details', {})
            if details.get('recovery_case_id'):
                continue
            repeated.update(set(details.get('issues', [])) & issues)
        result['acknowledged_trade_ids'].append(case['trade_id'])
        result['acknowledged_action_ids'].extend(retired)
        result['covered_issue_ids'].extend(sorted(issues-repeated))
        if repeated:
            result['blockers'].append('RECOVERY_ISSUE_RECURRED:'+case['id'])
    for name in ('acknowledged_trade_ids','acknowledged_action_ids','covered_issue_ids'):
        result[name] = sorted(set(result[name]))
    return result


def readonly_recovery_view(path, policy, *, historical_as_of=None, clock=None):
    """Inspect retirement through a SQLite read-only connection.

    ``historical_as_of`` is exclusively a closed-session historical check.
    It is explicitly labelled and cannot authorize live admission. Omitting it
    uses the actual clock and enforces the normal 30-second freshness limit.
    No ExecutionBook, schema initialization or risk-sidecar write is performed.
    """
    result = {'acknowledged_trade_ids':[], 'acknowledged_action_ids':[],
              'covered_issue_ids':[], 'blockers':[], 'automatic_execution_enabled':False,
              'historical_only':historical_as_of is not None,
              'live_admission_authorized':False}
    try:
        at = timestamp(historical_as_of) if historical_as_of is not None else (clock or (lambda:datetime.now(timezone.utc)))()
    except (ValueError,TypeError):
        result['blockers'] = ['RECOVERY_READONLY_CLOCK_INVALID']
        return result

    class ReadBook:
        def state(self):
            row = self.db.execute('SELECT payload FROM live_state WHERE id=1').fetchone()
            if not row:
                raise ValueError('RECOVERY_STATE_MISSING')
            return json.loads(row[0])

        def actions(self):
            names = ('id','trade_id','kind','state','payload','created_at','updated_at','report')
            return [dict(zip(names,r)) for r in self.db.execute('SELECT * FROM live_actions ORDER BY created_at,id')]

        def _snapshot_check(self, snapshot, now):
            state = self.state()
            if (state['policy_hash'] != digest(policy) or state['simulation'] is not False
                    or snapshot.get('synthetic') is not False or snapshot.get('provider') != 'robinhood'
                    or snapshot.get('collection_complete') is not True
                    or snapshot.get('account_mask') != '••••'+policy['account_suffix']
                    or snapshot.get('account_key') != state.get('account_key')
                    or not 0 <= (now-timestamp(snapshot['observed_at'])).total_seconds() <= 30
                    or state.get('last_snapshot') and timestamp(snapshot['observed_at']) < timestamp(state['last_snapshot'])
                    or snapshot.get('other_asset_open_orders')
                    or any(number(v) != 0 for v in snapshot.get('other_asset_values',{}).values())):
                raise ValueError('RECOVERY_READONLY_OBSERVATION_INVALID')

    try:
        db = sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro', uri=True, timeout=1)
        try:
            db.execute('BEGIN')
            facade = ReadBook()
            facade.db = db
            facade.simulation = False
            facade.policy = policy
            facade.clock = lambda:at
            # verify() only reads; constructing AuditTrail would initialize DDL.
            facade.audit = object.__new__(AuditTrail)
            facade.audit.db = db
            row = db.execute('SELECT payload FROM broker_current WHERE id=1').fetchone()
            health_row = db.execute('SELECT payload FROM broker_health WHERE id=1').fetchone()
            if not row or not health_row:
                raise ValueError('RECOVERY_BROKER_EVIDENCE_MISSING')
            snapshot = json.loads(row[0])
            health = json.loads(health_row[0])
            facade.audit.verify()
            healthy = health.get('healthy') is True and health.get('at') == snapshot['observed_at']
            stopped = False
            if not healthy and historical_as_of is not None:
                sources = [json.loads(r[0]) for r in db.execute('SELECT payload FROM audit_events ORDER BY sequence')
                           if json.loads(r[0]).get('event_type') == 'BROKER_SOURCE_STATE']
                event = sources[-1] if sources else {}
                stop_reason = health.get('reason')
                delta = (timestamp(health['at'])-timestamp(snapshot['observed_at'])).total_seconds()
                raw_snapshot = {k:v for k,v in snapshot.items() if k not in {
                    'reconciliation_issues','new_executions_seen','baseline_established'}}
                source_digest = hashlib.sha256(json.dumps(raw_snapshot,sort_keys=True).encode()).hexdigest()
                persisted = db.execute('SELECT payload FROM broker_observations WHERE digest=?', (source_digest,)).fetchone()
                stopped = (health.get('healthy') is False and stop_reason in {
                    'BOUNDED_OBSERVER_STOPPED','LIVE_SUPERVISOR_STOPPED'} and 0 <= delta <= 30
                    and event.get('actor') == 'script' and event.get('synthetic') is False
                    and event.get('occurred_at') == health['at'] and event.get('reason') == stop_reason
                    and event.get('details',{}).get('healthy') is False
                    and persisted is not None and canonical(json.loads(persisted[0])) == canonical(raw_snapshot))
            if not healthy and not stopped:
                raise ValueError('RECOVERY_BROKER_SOURCE_UNHEALTHY')
            facade._snapshot_check(snapshot,at)
            result.update(recovery_disposition(facade,snapshot))
            result['source_stopped_after_completed_snapshot'] = stopped
            result['observed_at'] = snapshot['observed_at']
            result['checked_as_of'] = at.isoformat()
            return result
        finally:
            db.rollback()
            db.close()
    except (OSError,sqlite3.Error,ValueError,KeyError,TypeError,TransportError):
        result['blockers'] = ['RECOVERY_READONLY_EVIDENCE_UNAVAILABLE_OR_INVALID']
        return result
