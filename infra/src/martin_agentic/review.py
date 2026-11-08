"""Once-per-session audit export and optional official post-close reconciliation."""

from datetime import datetime, timezone
import json
from pathlib import Path
from zoneinfo import ZoneInfo

from .audit import AuditTrail
from .broker import observe_broker
from .calendar import session
from .ledger import timestamp
from .risk import number
from .transport import safe_read_failure


def session_review(db_path, *, session_date, output_dir, account_suffix=None,
                   verify_broker=False, observer=None, clock=None):
    clock=clock or (lambda:datetime.now(timezone.utc))
    now=clock()
    market=session(now.isoformat())
    if verify_broker and (now.astimezone(ZoneInfo('America/New_York')).date().isoformat()!=session_date
                          or not market or now<market['close']):
        raise ValueError('Official close reconciliation requires this session to have closed')
    if not Path(db_path).is_file() and not verify_broker:
        raise ValueError('No audit database; absence of records does not prove no trading')
    broker=None
    failure=None
    failure_detail=None
    attempts=0
    if verify_broker and (not isinstance(account_suffix,str) or not (account_suffix.isdigit() and len(account_suffix)==4)):
        raise ValueError('ACCOUNT_SUFFIX_REQUIRED')
    if verify_broker:
        for attempts in range(1,3):
            try:
                result=(observer or observe_broker)(db_path,account_suffix=account_suffix,cycles=1)
                broker=result['observations'][-1]
                age=(clock()-timestamp(broker['last_observed_at'])).total_seconds()
                if broker.get('read_healthy') is not True or broker.get('account')!='••••'+account_suffix or not 0<=age<=30:
                    failure='CLOSING_BROKER_OBSERVATION_NOT_VERIFIED'
                break
            except (ValueError,KeyError,TypeError,OSError,RuntimeError) as exc:
                failure_detail=safe_read_failure(exc)
                if failure_detail['retryable'] and attempts<2:
                    continue
                failure='CLOSING_BROKER_READ_FAILED'
                break
    audit=AuditTrail(db_path)
    try:
        output=audit.export(session_date,output_dir)
        checked=verify_broker and broker is not None and failure is None
        flat=bool(checked and not broker.get('blockers') and not broker['open_equity_orders']
                  and not broker['other_asset_open_orders'] and not broker.get('pending_approvals',0)
                  and all(number(p['quantity'])==0 for p in broker['positions'].values()))
        addition={'review_scope':'post_close_broker_and_audit' if verify_broker else 'offline_audit_only',
            'broker_end_of_day_reconciliation_performed':checked,
            'broker_end_of_day_flat_verified':flat,'closing_broker_observation':broker,
            'closing_check_failure':failure,'healthy':failure is None and (flat if verify_broker else True),
            'closing_read_attempts':attempts,'closing_read_failure_detail':failure_detail if failure else None,
            'live_orders':0}
        report=audit.report(session_date)|addition
        path=Path(output['artifacts']['json'])
        temp=path.with_suffix('.json.tmp');temp.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n');temp.replace(path)
        path=Path(output['artifacts']['markdown'])
        text=path.read_text()+'\n## 收盘券商核对\n\n'
        text+=f'正式收盘核对：{checked}；已核实空仓且无残留订单：{flat}。\n\n'
        if not verify_broker:text+='当前为离线审计导出，没有核对券商实际持仓。\n'
        if failure:text+='核对失败：'+failure+'。不能据此宣布交易日已安全结束。\n'
        if failure_detail and failure:text+='安全诊断：'+failure_detail['code']+'；读取次数：'+str(attempts)+'。\n'
        if broker:text+='实际查询时间：'+broker['last_observed_at']+'；券商阻断：'+str(broker.get('blockers',[]))+'。\n'
        temp=path.with_suffix('.md.tmp');temp.write_text(text);temp.replace(path)
        return output|addition
    finally:
        audit.close()
