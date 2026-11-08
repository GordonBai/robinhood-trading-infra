"""Offline compiler of frozen provider prices; never an original receipt archive.

The input specification is hash frozen before compilation, which is not proof
of preregistration before market-data acquisition. Historical
receipt times and locally seeded EMAs are assumptions, prominently carried into
all artifacts. No network, broker writes, data interpolation, or strategy edits.
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import re
from pathlib import Path

from martin_agentic.audit import digest
from martin_agentic.calendar import NY, calendar, prior_daily_date, session
from martin_agentic.ledger import timestamp
from .research import strategy_freeze
from martin_agentic.qualification import candidate_root
from martin_agentic.risk import number

UNIVERSE = ('AAPL','HOOD','NVDA','AMD','TSLA','META','PLTR','SOFI','IONQ','RKLB')


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _ns(value):
    match=re.fullmatch(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})',value)
    if not match:raise ValueError('INVALID_EXCHANGE_TIMESTAMP')
    base=datetime.fromisoformat(match[1]+match[3].replace('Z','+00:00'))
    return int(base.timestamp())*1_000_000_000+int((match[2] or '').ljust(9,'0'))


def _stamp(ns):
    sec,fraction=divmod(ns,1_000_000_000)
    return datetime.fromtimestamp(sec,timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')+f'.{fraction:09d}Z'


def _write(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True)+'\n')
    return sha(path)


def _ema(prices, period):
    # Explicit first-close seed, unlike an undocumented provider's EMA seed.
    value=prices[0];alpha=Decimal(2)/Decimal(period+1)
    for price in prices[1:]:value=alpha*price+(1-alpha)*value
    return str(value)


def _ohlcv(row):
    values={key:str(number(str(row[key]))) for key in ('open','high','low','close')}
    lo,hi,op,cl=(number(values[k]) for k in ('low','high','open','close'))
    volume=row['volume']
    if isinstance(volume,bool) or int(volume)!=volume or volume<=0 or not 0<lo<=min(op,cl)<=max(op,cl)<=hi:
        raise ValueError('INVALID_OHLCV')
    return dict(values,volume=int(volume))


def compile_historical(spec_path, output_dir, *, clock=None):
    """Return DATA_GAP on incomplete imports; a draft is never strict acceptance.

    A complete price import can be SIMULATION_INPUT_READY while PIT and split
    adjustment remain assumed. Consumers MUST honor input_mode and false proof
    flags; the legacy strict real-source runner is intentionally not authorized.
    """
    spec_path=Path(spec_path);out=Path(output_dir);out.mkdir(parents=True,exist_ok=True)
    if any(out.iterdir()):raise ValueError('OUTPUT_DIRECTORY_MUST_BE_EMPTY')
    now=(clock or (lambda:datetime.now(timezone.utc)))().isoformat()
    result={'schema_version':1,'compiled_at':now,'status':'DATA_GAP','input_mode':'historical_simulation',
        'synthetic_market_prices':False,'actual_point_in_time_verified':False,
        'actual_adjustment_verified':False,'strict_real_coverage_authorized':False,
        'strategy_profitability_validated':False,'quote_overlap_deduplication':'exact record plus within-capture occurrence when provider ID absent; identical repeated updates across captures remain indistinguishable','gaps':[],'raw_sources':[]}
    gaps=result['gaps']
    try:
        spec=json.loads(spec_path.read_bytes());result['spec_sha256']=sha(spec_path)
        result['compiler_sha256']=sha(candidate_root()/'strategy/martin_luk/src/martin_luk/historical_compiler.py');result['strategy_freeze']=strategy_freeze()
        _write(out/'frozen-spec.json',spec)
        if spec.get('schema_version')!=1 or spec.get('universe_symbols')!=list(UNIVERSE):raise ValueError('FIXED_TEN_SYMBOL_UNIVERSE_REQUIRED')
        assumptions=spec['assumptions'];bar_lag=assumptions['bar_publish_lag_seconds'];quote_lag=assumptions['quote_receipt_lag_seconds']
        if type(bar_lag) is not int or not 0<=bar_lag<=30 or type(quote_lag) is not int or not 0<=quote_lag<=15:raise ValueError('INVALID_MODELED_RECEIPT_LAG')
        if assumptions.get('adjustment')!='assume_split' or assumptions.get('ema_method')!='first_close_seed_all_available_daily':raise ValueError('EXPLICIT_ADJUSTMENT_AND_EMA_ASSUMPTIONS_REQUIRED')
        folds=spec['folds'];previous=None
        if len(folds)<2 or len({f['id'] for f in folds})!=len(folds):raise ValueError('PREDECLARED_MULTIPLE_FOLDS_REQUIRED')
        for f in folds:
            a,b,c,d=(timestamp(f[k]) for k in ('train_start','train_end','test_start','test_end'))
            if not a<=b<c<d or previous is not None and c<=previous:raise ValueError('INVALID_PREDECLARED_FOLDS')
            previous=d
        threshold=number(spec['regime_classifier']['absolute_return_threshold'])
        if spec['regime_classifier'].get('method')!='endpoint_return_v1' or not 0<threshold<1:raise ValueError('INVALID_REGIME_CLASSIFIER')
        gaps.append('ORIGINAL_POINT_IN_TIME_RECEIPTS_NOT_ARCHIVED')
        raw={'daily':{},'bar':{},'quote':{}};seen={}
        for i,item in enumerate(spec['sources']):
            path=spec_path.parent/item['path']
            if not path.is_file():gaps.append('RAW_SOURCE_MISSING:'+item['path']);continue
            if sha(path)!=item['sha256']:raise ValueError('RAW_SOURCE_HASH_MISMATCH:'+item['path'])
            acquired=timestamp(item['acquired_at']);document=json.loads(path.read_bytes(),parse_float=str);kind=item['kind'];request=document['request']
            if acquired>timestamp(now):raise ValueError('ACQUISITION_FROM_FUTURE')
            if kind not in raw:raise ValueError('UNSUPPORTED_RAW_KIND')
            rh=document.get('tool')=='get_equity_historicals'
            if rh:
                if document.get('observed_at')!=item['acquired_at']:raise ValueError('RH_ACQUISITION_METADATA_MISMATCH')
                if kind=='quote' or request.get('bounds')!='regular' or request.get('adjustment_type')!='split' or request.get('interval')!=('day' if kind=='daily' else '5minute'):
                    raise ValueError('ROBINHOOD_REGULAR_SPLIT_REQUEST_REQUIRED')
            elif request.get('feed')!='sip':gaps.append('SIP_REQUIRED:'+item['path']);continue
            if item.get('pagination_exhausted') is not True or item.get('truncated') is not False:gaps.append('PAGINATION_NOT_PROVEN:'+item['path'])
            if not rh and kind!='quote' and request.get('timeframe')!=('1Day' if kind=='daily' else '5Min'):raise ValueError('RAW_TIMEFRAME_MISMATCH')
            copy=out/('raw-'+str(i)+'.json');copy.write_bytes(path.read_bytes())
            result['raw_sources'].append(dict(item,frozen_path=copy.name))
            if rh:
                requested=request.get('symbols')
                if not isinstance(requested,list) or not requested or len(set(requested))!=len(requested):raise ValueError('RH_REQUEST_SYMBOLS_REQUIRED')
                request_start,request_end=_ns(request['start_time']),_ns(request['end_time'])
                if request_start>=request_end:raise ValueError('RH_REQUEST_WINDOW_INVALID')
                records={}
                for group in document['response']['data']['results']:
                    if group['symbol'] not in requested:raise ValueError('RH_RESULT_SYMBOL_NOT_REQUESTED')
                    if group['symbol'] in records:raise ValueError('RH_DUPLICATE_SYMBOL_GROUP')
                    if any(not request_start<=_ns(row['begins_at'])<=request_end for row in group['bars']):raise ValueError('RH_ROW_OUTSIDE_REQUEST_WINDOW')
                    if group.get('bounds')!='regular' or group.get('interval')!=request['interval']:raise ValueError('ROBINHOOD_RESULT_CONTRACT_MISMATCH')
                    if any(row.get('interpolated',False) is not False for row in group['bars']):raise ValueError('INTERPOLATED_RH_BAR')
                    records[group['symbol']]=[dict(row,timestamp=row['begins_at'],
                        open=row['open_price'],high=row['high_price'],low=row['low_price'],close=row['close_price'],
                        _adjustment='split',_daily_label=row['begins_at'][:10] if kind=='daily' else None)
                        for row in group['bars']]
            else:records=document['quotes' if kind=='quote' else 'bars']
            if rh and set(records)!=set(requested):raise ValueError('RH_RESULT_SYMBOL_COVERAGE_MISSING')
            for symbol,rows in records.items():
                occurrences={}
                for source_index,row in enumerate(rows):
                    if row.get('symbol',symbol)!=symbol:raise ValueError('RAW_SYMBOL_MISMATCH')
                    t=timestamp(row['timestamp'])
                    if _ns(row['timestamp'])>_ns(item['acquired_at']):raise ValueError('EXCHANGE_TIMESTAMP_AFTER_ACQUISITION')
                    if kind=='bar':
                        regular=session(t.isoformat())
                        if not regular or not regular['open']<=t or t+timedelta(minutes=5)>regular['close']:raise ValueError('REGULAR_5MIN_BAR_OUTSIDE_SESSION')
                        if (_ns(row['timestamp'])-_ns(regular['open'].isoformat()))%(300*1_000_000_000):raise ValueError('UNALIGNED_5MIN_BAR')
                    key=(kind,symbol,_ns(row['timestamp']))
                    if kind=='quote':
                        identity=row.get('quote_id',row.get('id',row.get('sequence')))
                        if identity is not None:key=(kind,symbol,'provider_id',digest(identity))
                        else:
                            fingerprint=digest(row);occurrence=occurrences.get(fingerprint,0);occurrences[fingerprint]=occurrence+1
                            key+=('exact_record_overlap_assumption',fingerprint,occurrence)

                    if key in seen and seen[key]!=digest(row):raise ValueError('CONFLICTING_RAW_EVENT')
                    if key in seen:continue
                    seen[key]=digest(row);stored=dict(row,source_row_index=source_index) if kind=='quote' else row
                    raw[kind].setdefault(symbol,[]).append((t,stored,item['sha256'],item['acquired_at']))
        daily_rows=[r for rows in raw['daily'].values() for r in rows]
        result['historical_adjustment_as_of_verified']=False
        gaps.append('HISTORICAL_SPLIT_ADJUSTMENT_AS_OF_NOT_VERIFIED')
        result['daily_adjustment_verified']=bool(daily_rows) and all(r[1].get('_adjustment')=='split' for r in daily_rows)
        all_price_rows=daily_rows+[r for rows in raw['bar'].values() for r in rows]
        result['actual_adjustment_verified']=bool(daily_rows) and all(r[1].get('_adjustment')=='split' for r in all_price_rows)
        if not result['actual_adjustment_verified']:gaps.append('SPLIT_ADJUSTMENT_NOT_VERIFIED:provider_default_raw')
        for group in raw.values():
            for rows in group.values():rows.sort(key=lambda x:_ns(x[1]['timestamp']))
        events=[];regime_evidence={};coverage={}
        def emit(kind,at,data,**extra):
            at=at if isinstance(at,str) else at.isoformat()
            events.append(dict(id=digest([kind,at,data]),kind=kind,at=at,data=data,
                historical_receipt_mode='MODELED',**extra))
        for fold in folds:
            start,end=timestamp(fold['test_start']),timestamp(fold['test_end'])
            regular_overlaps=0;fold_event_start=len(events)
            for day in calendar().sessions_in_range(start.astimezone(NY).date().isoformat(),end.astimezone(NY).date().isoformat()):
                opened=calendar().session_open(day).to_pydatetime();closed=calendar().session_close(day).to_pydatetime()
                if end<=opened or start>=closed:continue
                regular_overlaps+=1
                at=max(start,opened);histories={};indicators={}
                for symbol in UNIVERSE:
                    rows=[r for r in raw['daily'].get(symbol,[]) if (r[1].get('_daily_label') or r[0].astimezone(NY).date().isoformat())<opened.astimezone(NY).date().isoformat()]
                    bars=[]
                    for t,row,source,acquired in rows:
                        label=row.get('_daily_label') or t.astimezone(NY).date().isoformat()
                        if not calendar().is_session(label):raise ValueError('DAILY_NON_SESSION')
                        available=datetime.combine(datetime.fromisoformat(label).date()+timedelta(days=1),datetime.min.time(),NY)
                        bars.append(dict(symbol=symbol,begins_at=label+'T00:00:00+00:00',interval='day',bounds='regular',
                            adjustment=row.get('_adjustment','raw'),adjustment_evidence='EXPLICIT_FROZEN_RH_SPLIT_REQUEST' if row.get('_adjustment')=='split' else 'PROVIDER_DEFAULT_RAW_NOT_SPLIT_VERIFIED',available_at=available.isoformat(),
                            observed_at=available.isoformat(),actual_acquired_at=acquired,raw_exchange_timestamp=row['timestamp'],raw_source_sha256=source,**_ohlcv(row)))
                    histories[symbol]=bars
                    if len(bars)<64 or not bars or bars[-1]['begins_at'][:10]!=prior_daily_date(at.isoformat()):gaps.append('DAILY_HISTORY_INCOMPLETE:'+fold['id']+':'+symbol);continue
                    dates=[b['begins_at'][:10] for b in bars]
                    expected=[d.date().isoformat() for d in calendar().sessions_in_range(dates[0],dates[-1])]
                    if dates!=expected:gaps.append('DAILY_SESSION_GAPS:'+fold['id']+':'+symbol)
                    prices=[number(b['close']) for b in bars];last=bars[-1]
                    indicators[symbol]={str(p):dict(type='ema',period=p,interval='day',bounds='regular',adjustment=last['adjustment'],
                        begins_at=last['begins_at'],observed_at=last['observed_at'],available_at=last['available_at'],
                        value=_ema(prices,p),computation_method='first_close_seed_all_available_daily',seed_count=len(prices)) for p in (9,21,50)}
                emit('universe',at,dict(universe_id='frozen-ten-'+day.date().isoformat(),histories=histories,indicators=indicators))
                for symbol in UNIVERSE:
                    present=set()
                    for t,row,source,acquired in raw['bar'].get(symbol,[]):
                        if not opened<=t or t+timedelta(minutes=5)>closed or not start<=t or t+timedelta(minutes=5)>end:continue
                        if (t-opened).total_seconds()%300:raise ValueError('UNALIGNED_5MIN_BAR')
                        present.add(t)
                        emit('bar',t+timedelta(minutes=5,seconds=bar_lag),dict(symbol=symbol,begins_at=t.isoformat(),
                            interval='5minute',bounds='regular',available_at=(t+timedelta(minutes=5,seconds=bar_lag)).isoformat(),
                            observed_at=(t+timedelta(minutes=5,seconds=bar_lag)).isoformat(),actual_acquired_at=acquired,
                            raw_exchange_timestamp=row['timestamp'],raw_source_sha256=source,**_ohlcv(row)))
                    t=opened
                    while t+timedelta(minutes=5)<=min(end,closed):
                        if t>=start and t not in present:gaps.append('FIVE_MINUTE_BAR_MISSING:'+fold['id']+':'+symbol+':'+t.isoformat())
                        t+=timedelta(minutes=5)
                    times=[]
                    for t,row,source,acquired in raw['quote'].get(symbol,[]):
                        observed=t+timedelta(seconds=quote_lag)
                        if not opened<=t<closed or not start<=observed<=end:continue
                        bid,ask=number(str(row['bid_price'])),number(str(row['ask_price']))
                        if not 0<bid<=ask or row['bid_size']<=0 or row['ask_size']<=0:raise ValueError('INVALID_SIP_QUOTE')
                        times.append(observed)
                        emit('quote',_stamp(_ns(row['timestamp'])+quote_lag*1_000_000_000),dict(symbol=symbol,bid=str(bid),ask=str(ask),bid_size=row['bid_size'],ask_size=row['ask_size'],
                            bid_at=row['timestamp'],ask_at=row['timestamp'],observed_at=_stamp(_ns(row['timestamp'])+quote_lag*1_000_000_000),source_row_index=row['source_row_index'],actual_acquired_at=acquired,raw_exchange_timestamp=row['timestamp'],raw_source_sha256=source))
                    boundaries=[max(start,opened),*times,min(end,closed)]
                    if not times or any((b-a).total_seconds()>15 for a,b in zip(boundaries,boundaries[1:])):gaps.append('SIP_QUOTE_COVERAGE_GAP:'+fold['id']+':'+symbol)
            if not regular_overlaps:gaps.append('FOLD_NO_REGULAR_SESSION_OVERLAP:'+fold['id'])
            if len(events)==fold_event_start:gaps.append('FOLD_NO_NORMALIZED_EVENTS:'+fold['id'])
            benchmark=spec['regime_classifier']['benchmark'];points=[]
            for t,row,_,_ in raw['bar'].get(benchmark,[]):
                close=t+timedelta(minutes=5)
                regular=session(t.isoformat())
                if regular and regular['open']<=t<close<=regular['close'] and start<=t<close<=end:points.append(dict(at=close.isoformat(),close=str(number(str(row['close'])))))
            if len(points)<2:gaps.append('BENCHMARK_REGIME_DATA_MISSING:'+fold['id'])
            else:
                change=number(points[-1]['close'])/number(points[0]['close'])-1
                label='TREND_UP' if change>=threshold else 'ADVERSE' if change<=-threshold else 'RANGE'
                if label!=fold['market_regime']:gaps.append('PREDECLARED_REGIME_MISMATCH:'+fold['id'])
                regime_evidence[fold['id']]=dict(method='endpoint_return_v1',benchmark=benchmark,observations=points,
                    absolute_return_threshold=str(threshold),classification=label)
            coverage[fold['id']]=dict(universe_symbols=list(UNIVERSE),quote_max_gap_seconds=15,
                bar_max_publish_lag_seconds=max(1,bar_lag),source_completeness={'id':fold['id']},regime_evidence={'id':fold['id']})
        events.sort(key=lambda e:(_ns(e['at']),{'universe':0,'bar':1,'quote':2}[e['kind']]))
        # Derived source freezes normalized events and raw hashes together.
        source={'events':events,'raw_sources':result['raw_sources'],'provenance':dict(input_mode='historical_simulation',
            actual_point_in_time_verified=False,historical_adjustment_as_of_verified=False,actual_adjustment_verified=result['actual_adjustment_verified'],assumptions=assumptions),
            'coverage_evidence':{f['id']:dict(symbols=list(UNIVERSE),start=f['test_start'],end=f['test_end'],
                pagination_exhausted=not bool(gaps),truncated=None) for f in folds},
            'regime_evidence':regime_evidence}
        source_sha=_write(out/'source-normalized-events.json',source)
        dataset=[dict(e,source_sha256=source_sha,synthetic_input=False) for e in events]
        dataset_sha=_write(out/'events.json',dataset)
        for contract in coverage.values():
            contract['source_completeness']['source_sha256']=source_sha;contract['regime_evidence']['source_sha256']=source_sha
        manifest=dict(schema_version=1,strategy_freeze=result['strategy_freeze'],synthetic_input=False,
            input_mode='historical_simulation',actual_point_in_time_verified=False,historical_adjustment_as_of_verified=False,actual_adjustment_verified=result['actual_adjustment_verified'],
            assumptions=assumptions,policy=spec['policy'],folds=folds,costs=spec['costs'],scenarios=spec['scenarios'],
            sources=[dict(path='source-normalized-events.json',sha256=source_sha,provider='frozen_provider_historical_compiler',synthetic=False)],
            dataset=dict(path='events.json',sha256=dataset_sha),coverage=dict(schema_version=1,regime_classifier=spec['regime_classifier'],folds=coverage))
        _write(out/'historical-simulation-manifest.json',manifest)
        result.update(event_count=len(events),manifest_path=str(out/'historical-simulation-manifest.json'),
            legacy_strict_runner_authorized=False,assumptions=assumptions)
        result['gaps']=sorted(set(gaps));result['status']='DATA_GAP' if any(g not in {'ORIGINAL_POINT_IN_TIME_RECEIPTS_NOT_ARCHIVED','HISTORICAL_SPLIT_ADJUSTMENT_AS_OF_NOT_VERIFIED'} for g in gaps) else 'SIMULATION_INPUT_READY'
    except (ValueError,KeyError,TypeError,OSError) as exc:
        result['gaps'].append(type(exc).__name__+':'+str(exc))
    result['report_path']=str(out/'compile-report.json');_write(out/'compile-report.json',result)
    return result
