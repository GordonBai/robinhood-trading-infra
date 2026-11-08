"""Exclusive offline-owner recovery control; no broker write operations.

Acquire the same lock as the running supervisor before opening an ExecutionBook
or its risk sidecar. Keep it through formal observation, audit and final close.
A caller cannot recover a live owner's database through a second writer.
"""
from pathlib import Path
import json
import sqlite3

from .audit import digest

from .broker import BrokerJournal, collect_broker
from .execution import ExecutionBook
from .recovery import RecoveryCases
from .shadow import single_instance
from .transport import CodexBrokerReadTransport


def _validate_existing_risk(source, policy):
    """A standalone audit restore must never create a blank trading risk history."""
    suffix = policy.get('account_suffix')
    if not isinstance(suffix, str) or len(suffix) != 4 or not suffix.isdigit():
        raise ValueError('RECOVERY_RISK_ACCOUNT_SUFFIX_INVALID')
    risk_path = source.with_name('account-risk-' + suffix + '.sqlite')
    if (not risk_path.is_file() or risk_path.is_symlink()
            or risk_path.stat().st_nlink != 1):
        raise ValueError('RECOVERY_EXISTING_RISK_SIDECAR_REQUIRED')
    def state(path, table):
        connection = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=.25)
        try:
            row = connection.execute('SELECT payload FROM ' + table + ' WHERE id=1').fetchone()
            if not row:
                raise ValueError('RECOVERY_EXISTING_RISK_STATE_REQUIRED')
            return json.loads(row[0])
        finally:
            connection.close()
    original = state(source, 'live_state')
    risk = state(risk_path, 'account_risk')
    terms = digest({k:policy[k] for k in ('account_suffix', 'account_capital_usd',
        'stop_new_entries_at_daily_equity_loss_usd', 'project_max_drawdown_fraction')})
    if (original.get('simulation') is not False or risk.get('simulation') is not False
            or original.get('policy_hash') != digest(policy)
            or not original.get('account_key')
            or risk.get('account_key') != original['account_key'] or risk.get('terms') != terms):
        raise ValueError('RECOVERY_EXISTING_RISK_BINDING_INVALID')


def run_recovery_case(mode, db_path, policy, *, case_id=None, trade_id=None,
                      intent_ids=(), incident_order_ids=(), manual_order_ids=(),
                      execution_ids=(), cancelled_order_ids=(), reason='',
                      manual_takeover=False, response=None, account_number=None,
                      tool_call_id=None, origin_reference=None):
    if mode not in {'open', 'status', 'verify', 'acknowledge', 'import-response','dispose-protection-incident'}:
        raise ValueError('INVALID_RECOVERY_CONTROL_MODE')
    try:
        source = Path(db_path).expanduser().resolve(strict=True)
    except FileNotFoundError as exc:
        raise FileNotFoundError('Execution database does not exist') from exc
    if not source.is_file():
        raise ValueError('RECOVERY_EXISTING_DATABASE_REQUIRED')
    # Even status may initialize recovery schema; never bypass owner exclusion.
    with single_instance(source):
        _validate_existing_risk(source, policy)
        book = ExecutionBook(source, policy)
        try:
            if manual_takeover:
                from .manual_takeover import ManualTakeoverCases
                cases = ManualTakeoverCases(book)
            else:
                cases = RecoveryCases(book)
            if mode == 'import-response':
                if not manual_takeover:
                    raise ValueError('RECOVERY_MANUAL_RECEIPT_MODE_REQUIRED')
                return cases.import_response(trade_id, response,
                    account_number=account_number, tool_call_id=tool_call_id,
                    origin_reference=origin_reference, reason=reason)
            if mode == 'open':
                return cases.open(trade_id, intent_ids=list(intent_ids),
                    incident_order_ids=list(incident_order_ids), manual_order_ids=list(manual_order_ids),
                    execution_ids=list(execution_ids), cancelled_order_ids=list(cancelled_order_ids), reason=reason)
            if mode == 'status':
                return cases.get(case_id)
            # Safety cannot be established with a saved/caller supplied snapshot.
            tracked = sorted(set(book.pending_approval_ids()) | set(cases.required_approval_ids()))
            with CodexBrokerReadTransport() as transport:
                snapshot = collect_broker(transport, account_suffix=book.policy['account_suffix'],
                                          approval_ids=tracked)
            journal = BrokerJournal(source)
            try:
                journal.ingest(snapshot)
            finally:
                journal.close()
            return (cases.verify(case_id, snapshot) if mode == 'verify'
                    else cases.dispose_protection_incident(case_id,snapshot,reason=reason) if mode=='dispose-protection-incident'
                    else cases.acknowledge(case_id, snapshot, reason=reason))
        finally:
            book.close()
