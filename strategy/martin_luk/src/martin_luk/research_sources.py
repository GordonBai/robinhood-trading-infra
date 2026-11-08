"""Audit historical provider captures without inventing point-in-time observations.

A successful query proves data access, never a complete research dataset. Provider
market timestamps and this machine's acquisition time remain separate.
"""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

from martin_agentic.ledger import timestamp
from martin_agentic.risk import number


def inspect_provider_capture(path, *, acquired_at):
    path=Path(path);raw=path.read_bytes();document=json.loads(raw,parse_float=str)
    acquired=timestamp(acquired_at)
    kind={'get_stock_quotes':'quotes','get_stock_bars':'bars'}.get(document.get('tool'))
    if kind is None:raise ValueError('UNSUPPORTED_HISTORICAL_PROVIDER_TOOL')
    request=document['request'];start=timestamp(request['start']);end=timestamp(request['end'])
    if not start<end<=acquired:raise ValueError('INVALID_HISTORICAL_CAPTURE_RANGE')
    symbols=request['symbols'];limit=request.get('limit');rows=document[kind]
    if not symbols or len(symbols)!=len(set(symbols)):raise ValueError('INVALID_CAPTURE_SYMBOLS')
    if set(rows)-set(symbols):raise ValueError('UNREQUESTED_CAPTURE_SYMBOL')
    gaps=[];summary={};total=0
    for symbol in symbols:
        records=rows.get(symbol,[]);total+=len(records);times=[];invalid=0;seen={};conflicts=0;duplicates=0
        for row in records:
            at=timestamp(row['timestamp']);times.append(at)
            if row.get('symbol')!=symbol or not start<=at<=end:invalid+=1
            key=row['timestamp'];value=json.dumps(row,sort_keys=True,separators=(',',':'))
            if key in seen:
                if seen[key]!=value:conflicts+=1
                else:duplicates+=1
            seen[key]=value
            if kind=='quotes':
                bid=number(row['bid_price']);ask=number(row['ask_price'])
                if bid<=0 or ask<bid or number(row['bid_size'])<=0 or number(row['ask_size'])<=0:invalid+=1
            else:
                lo=number(row['low']);hi=number(row['high'])
                if lo<=0 or not lo<=number(row['open'])<=hi or not lo<=number(row['close'])<=hi or number(row['volume'])<0:invalid+=1
        if not records:gaps.append('SYMBOL_DATA_MISSING:'+symbol)
        if invalid:gaps.append('INVALID_MARKET_RECORDS:'+symbol)
        if conflicts:gaps.append('CONFLICTING_MARKET_TIMESTAMPS:'+symbol)
        if times!=sorted(times):gaps.append('RECORDS_NOT_ASCENDING:'+symbol)
        summary[symbol]={'records':len(records),'first_market_at':min(times).isoformat() if times else None,
            'last_market_at':max(times).isoformat() if times else None,'invalid_records':invalid,
            'exact_duplicates':duplicates,'conflicting_timestamps':conflicts}
    counts=document.get('counts',{})
    if counts.get('records')!=total or any(counts.get('per_symbol',{}).get(s)!=summary[s]['records'] for s in symbols):
        gaps.append('PROVIDER_COUNTS_MISMATCH')
    if type(limit) is not int or limit<=0:gaps.append('PROVIDER_LIMIT_UNVERIFIED')
    elif total>=limit:gaps.append('POSSIBLE_LIMIT_TRUNCATION')
    feed=request.get('feed')
    if kind=='quotes' and feed!='sip':gaps.append('NOT_CONSOLIDATED_SIP_QUOTES')
    # Absence of records at the endpoint cannot prove completeness: markets can
    # legitimately have no event there. Require independent pagination/coverage.
    gaps.extend(['PROVIDER_PAGINATION_COMPLETENESS_NOT_PROVEN','POINT_IN_TIME_UNIVERSE_NOT_PRESENT',
        'ORIGINAL_RECEIPT_LATENCY_NOT_RECORDED','MULTIREGIME_RESEARCH_DATASET_NOT_ASSEMBLED'])
    return {'schema_version':1,'provider':'Alpaca MCP','path':str(path),'sha256':hashlib.sha256(raw).hexdigest(),
        'acquisition_upper_bound_at':acquired.isoformat(),'request':request,'feed':feed,'kind':kind,'symbols':summary,
        'data_access_proven':bool(total),'market_records':total,'gaps':sorted(set(gaps)),
        'provider_capture_complete':False,'ready_for_strategy_research':False,
        'historical_market_time_is_acquisition_time':False,'provenance_cryptographically_authenticated':False}
