"""Non-mutating confirmation/protection gates; never submit a broker order."""

from decimal import Decimal
from .calendar import is_regular
from .ledger import timestamp
from .risk import number


def confirmation_path(review, *, advanced=False):
    warning = review.get("alert") if advanced else review.get("order_checks")
    if warning:
        return "SHOW_WARNING_AND_CONFIRM"
    if review.get("customer_approval_required") is True:
        return "SUBMIT_FOR_APP_APPROVAL_ONLY"
    return "EXPLICIT_CHAT_CONFIRMATION_REQUIRED"


def check_oco(plan, quote, *, held_quantity, as_of):
    now=timestamp(as_of)
    errors=[]
    qty=plan.get("quantity")
    if type(qty) is not int or not 0 < qty <= held_quantity:
        errors.append("QUANTITY_EXCEEDS_ACTUAL_POSITION")
    if plan.get("symbol")!=quote.get("symbol") or plan.get("side")!="sell":
        errors.append("SYMBOL_OR_SIDE_MISMATCH")
    if not is_regular(as_of) or plan.get("market_hours")!="regular_hours":
        errors.append("OCO_REQUIRES_REGULAR_SESSION")
    for key in ("bid_at","ask_at","observed_at"):
        if not 0 <= (now-timestamp(quote[key])).total_seconds() <= 15:
            errors.append("STALE_OR_FUTURE_QUOTE")
    bid,ask=number(quote['bid']),number(quote['ask'])
    stop,take=number(plan['stop']),number(plan['take_profit'])
    if not 0 < stop < bid <= ask < take:
        errors.append("INVALID_SELL_OCO_GEOMETRY")
    if bid>0 and (bid-stop)/bid < Decimal('0.0025') or ask>0 and (take-ask)/ask < Decimal('0.0025'):
        errors.append("OCO_TOO_CLOSE_TO_MARKET")
    if take-stop < Decimal('0.10'):
        errors.append("OCO_LEGS_TOO_CLOSE")
    return {"locally_valid":not errors,"errors":sorted(set(errors)),
            "broker_accepted":False,"atomic_entry_protection":False}


"""Describe evidenced tool capabilities; never grants account or order authority."""
from datetime import datetime, timezone
from .audit import digest
from .ledger import timestamp


def capability_report(inventory, *, clock=None):
    if (inventory.get('schema_version') != 1
            or inventory.get('source') != 'current_conversation_official_tool_metadata'):
        raise ValueError('OFFICIAL_TOOL_METADATA_REQUIRED')
    timestamp(inventory['captured_at'])
    rows=inventory.get('tools')
    if not isinstance(rows,list) or not rows:raise ValueError('TOOL_METADATA_EMPTY')
    tools={}
    for row in rows:
        if (not isinstance(row,dict) or not isinstance(row.get('name'),str)
                or not isinstance(row.get('description'),str) or not row['description']
                or row['name'] in tools):raise ValueError('TOOL_METADATA_INVALID')
        tools[row['name']]=row['description']
    now=(clock or (lambda:datetime.now(timezone.utc)))()
    if timestamp(inventory['captured_at'])>now:
        raise ValueError('CAPABILITY_METADATA_FUTURE')
    advanced_names=('review_advanced_order','place_advanced_order',
                    'get_advanced_orders','cancel_advanced_order')
    oco=all(n in tools for n in advanced_names)
    oco=oco and all('OCO' in tools[n] for n in advanced_names[:2])
    advanced_review=tools.get('review_advanced_order')
    advanced_review_status=('REQUIRED' if advanced_review is not None
                            and 'get explicit confirmation before calling place_advanced_order'
                            in advanced_review.lower() else 'UNKNOWN')
    advanced_cancel=tools.get('cancel_advanced_order')
    if advanced_cancel is None:
        advanced_cancel_statement='UNKNOWN'
    elif 'always confirm with the user before calling' in advanced_cancel.lower():
        advanced_cancel_statement='STATED'
    else:
        advanced_cancel_statement='NOT_STATED'
    equity_review=tools.get('review_equity_order')
    place_status=('REQUIRED' if equity_review is not None
                  and 'get explicit confirmation before calling place_equity_order'
                  in equity_review.lower() else 'UNKNOWN')
    equity_cancel=tools.get('cancel_equity_order')
    cancel_status=('REQUIRED' if equity_cancel is not None
                   and 'always confirm with the user before calling'
                   in equity_cancel.lower() else 'UNKNOWN')
    # Compatibility booleans are True only for a positively evidenced
    # requirement. Unknown/absent metadata is None, never False-as-permitted.
    explicit_preview = True if place_status == 'REQUIRED' else None
    cancel_confirm = True if cancel_status == 'REQUIRED' else None
    return {'schema_version':1,'as_of':now.isoformat(),'metadata_captured_at':inventory['captured_at'],
        'metadata_sha256':digest(inventory),
        'evidence_scope':'tool_contract_only_not_account_or_live_execution',
        'account_trade_approval_setting':{'state':'NOT_READ','must_read_before_claim':True,
            'get_trade_approval_setting_present':'get_trade_approval_setting' in tools,
            'get_trade_approvals_present':'get_trade_approvals' in tools},
        'tools':sorted(tools),
        'equity_preview_requires_explicit_confirmation_when_no_app_approval':explicit_preview,
        'equity_cancel_requires_specific_confirmation':cancel_confirm,
        'equity_place_confirmation_requirement':place_status,
        'equity_cancel_confirmation_requirement':cancel_status,
        'app_approval_is_not_unattended':True,
        'exit_oco':{'documented':bool(oco),'adapter_live_validated':False,
            'entry_atomic_bracket_proven':False,'account_eligibility_validated':False,
            'review_present':'review_advanced_order' in tools,
            'place_present':'place_advanced_order' in tools,
            'get_present':'get_advanced_orders' in tools,
            'cancel_present':'cancel_advanced_order' in tools,
            'advanced_review_explicit_confirmation_when_no_app_approval':
                True if advanced_review_status == 'REQUIRED' else None,
            'advanced_review_confirmation_requirement':advanced_review_status,
            'advanced_cancel_confirmation_statement':advanced_cancel_statement,
            'advanced_cancel_unattended_qualified':False,
            'evidence':'review_advanced_order_and_place_advanced_order_contract' if oco else 'NOT_PROVEN'},
        'write_boundary':{'main_chat_raw_write_tools_visible':any(n.startswith(('place_','cancel_')) for n in tools),
            'project_gateway_globally_enforced':False,'worker_allowlist_scope':'worker_process_only',
            'gateway_feasibility':'NOT_PROVEN','local_auto_setting_waives_native_confirmation':False},
        'unattended_recovery':{'auth_expiry':'NOT_VALIDATED','host_sleep_resume':'NOT_VALIDATED',
            'host_restart':'NOT_VALIDATED','credential_refresh_owner':'official_connector_no_private_token_extraction'},
        'unattended_ready':False,'live_orders':0,'report_authorizes_trading':False}


def admission_for_capabilities(report, requirements):
    """Fail closed for capabilities lacking evidence, without changing a policy."""
    if not isinstance(requirements,dict):raise ValueError('CAPABILITY_REQUIREMENTS_REQUIRED')
    supported={'supervised_equity': 'review_equity_order' in report.get('tools',[]),
        'exit_oco_live':report.get('exit_oco',{}).get('adapter_live_validated') is True,
        'atomic_entry_bracket':report.get('exit_oco',{}).get('entry_atomic_bracket_proven') is True,
        'unattended_execution':report.get('unattended_ready') is True,
        'global_enforcement_gateway':report.get('write_boundary',{}).get('project_gateway_globally_enforced') is True}
    if any(type(v) is not bool for v in requirements.values()):raise ValueError('BOOLEAN_CAPABILITY_REQUIREMENT_REQUIRED')
    blockers=['CAPABILITY_NOT_PROVEN:'+name for name,needed in sorted(requirements.items()) if needed and not supported.get(name,False)]
    return {'eligible':not blockers,'blockers':blockers,'live_orders':0,'authorization_granted':False}


#: Neutral capability tokens this channel can advertise, derived only from the
#: evidenced tool contract above. A token is present exactly when the evidence
#: supports it; absence is a refusal, never a fallback.
_TOOL_CAPABILITY = {
    'get_equity_quotes': 'quotes',
    'get_equity_historicals': 'bars',
    'get_equity_technical_indicators': 'indicators',
    'review_equity_order': 'equity_review',
    'place_equity_order': 'equity_place',
    'cancel_equity_order': 'equity_cancel',
}


def capability_blockers(report, *, unattended_code='NO_FORMALLY_QUALIFIED_UNATTENDED_ADAPTER'):
    """Capability reasons shared by the legacy admission and read-only preflight.

    This is a projection of the existing capability report, not a second
    capability matrix or an authorization object. Absence, unknown or an
    unmatched confirmation description is a blocker, never an inferred
    permission.
    """
    if not isinstance(report, dict):
        return sorted({'CAPABILITY_EVIDENCE_UNAVAILABLE', unattended_code,
                       'CURRENT_NATIVE_PLACE_CONFIRMATION_UNKNOWN',
                       'CURRENT_NATIVE_CANCEL_CONFIRMATION_UNKNOWN',
                       'CURRENT_ADVANCED_CANCEL_CONFIRMATION_UNKNOWN',
                       'ACCOUNT_TRADE_APPROVAL_SETTING_UNKNOWN'})
    blockers=set()
    # Reuse the existing requirement view rather than reading a second
    # unattended flag in each consumer.
    unattended = admission_for_capabilities(
        report, {'unattended_execution': True})
    if not unattended.get('eligible'):
        blockers.add(unattended_code)
    place = report.get('equity_place_confirmation_requirement')
    if place == 'REQUIRED':
        blockers.add('CURRENT_NATIVE_PLACE_REQUIRES_SPECIFIC_CONFIRMATION')
    elif place != 'NOT_REQUIRED':
        blockers.add('CURRENT_NATIVE_PLACE_CONFIRMATION_UNKNOWN')
    cancel = report.get('equity_cancel_confirmation_requirement')
    if cancel == 'REQUIRED':
        blockers.add('CURRENT_NATIVE_CANCEL_REQUIRES_SPECIFIC_CONFIRMATION')
    elif cancel != 'NOT_REQUIRED':
        blockers.add('CURRENT_NATIVE_CANCEL_CONFIRMATION_UNKNOWN')
    if (report.get('account_trade_approval_setting') or {}).get('state') != 'READ':
        blockers.add('ACCOUNT_TRADE_APPROVAL_SETTING_UNKNOWN')
    advanced = report.get('exit_oco') or {}
    statement = advanced.get('advanced_cancel_confirmation_statement')
    if statement == 'STATED':
        blockers.add('CURRENT_ADVANCED_CANCEL_REQUIRES_SPECIFIC_CONFIRMATION')
    elif statement == 'NOT_STATED':
        blockers.add('CURRENT_ADVANCED_CANCEL_UNATTENDED_NOT_PROVEN')
    else:
        blockers.add('CURRENT_ADVANCED_CANCEL_CONFIRMATION_UNKNOWN')
    if advanced.get('advanced_cancel_unattended_qualified') is not True:
        blockers.add('CURRENT_ADVANCED_CANCEL_UNATTENDED_NOT_PROVEN')
    return sorted(blockers)


def channel_capabilities(inventory, *, adapter_id='robinhood-agentic', clock=None):
    """Describe the channel as a neutral :class:`CapabilityMatrix`.

    Only evidenced tool metadata is consulted. Continuous authorisation is
    advertised only when the report actually supports unattended execution, so a
    caller that requires it is refused at negotiation instead of discovering the
    gap halfway through a live trade.
    """
    from trading_contracts.ports import CapabilityMatrix

    report = capability_report(inventory, clock=clock)
    tools = set(report.get('tools') or ())
    provides = sorted({token for name, token in _TOOL_CAPABILITY.items()
                       if name in tools and (name != 'place_equity_order'
                                             or 'review_equity_order' in tools)})
    if report.get('exit_oco', {}).get('documented') is True:
        provides.append('exit_oco_documented')
    if report.get('exit_oco', {}).get('adapter_live_validated') is True:
        provides.append('exit_oco_live')
    if report.get('exit_oco', {}).get('entry_atomic_bracket_proven') is True:
        provides.append('atomic_entry_bracket')
    if report.get('unattended_ready') is True:
        provides.append('continuous_authorisation')
    if report.get('write_boundary', {}).get('project_gateway_globally_enforced') is True:
        provides.append('global_enforcement_gateway')
    return CapabilityMatrix(adapter_id=adapter_id, provides=tuple(sorted(set(provides))),
                            confirmation='required', continuous_authorisation=False,
                            synthetic=False)
