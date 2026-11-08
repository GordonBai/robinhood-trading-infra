"""Registered, point-in-time replay of the existing Martin strategy.

This is a conservative quote-driven simulation, never Robinhood execution or
profit evidence. Every invocation, including missing-data and invalid inputs,
is registered before evaluation. Scenarios are assumptions, not fitted delays.
"""
from collections import Counter
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_FLOOR
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import uuid

from martin_agentic.audit import canonical, digest
from martin_agentic.calendar import calendar, NY
from .features import build_candidates
from martin_agentic.execution import ExecutionBook
from .exits import exit_decision
from martin_agentic.ledger import timestamp
from .live_signals import LiveSignals
from martin_agentic.paper import PaperDesk
from martin_agentic.qualification import candidate_root
from martin_agentic.risk import number
from .strategy import PullbackMachine


def _required_hash(path, root):
    path = Path(path)
    if not path.is_file() or path.is_symlink():
        raise ValueError('STRATEGY_FREEZE_SOURCE_MISSING:' + path.as_posix())
    return path.relative_to(root).as_posix(), hashlib.sha256(path.read_bytes()).hexdigest()


def strategy_freeze():
    """Pin the real split strategy and its generic dependencies.

    The original list named paper/risk/ledger/calendar/market beside the
    strategy files. After the infra split those live in martin_agentic; hashing
    the old locations silently read missing files. Every path below is
    required, and the generic package globs are non-empty, so a wrapper or an
    empty glob cannot pass as a complete freeze.
    """
    root = candidate_root()
    martin_root = root / 'strategy/martin_luk/src/martin_luk'
    infra_root = root / 'infra/src/martin_agentic'
    files = {}
    for name in ('strategy.py', 'features.py', 'exits.py', 'live_signals.py',
                 'research.py', 'research_sources.py', 'research_verification.py',
                 'historical_compiler.py', 'live_session.py', 'service.py',
                 'instance_composition.py', 'exit_composition.py'):
        key, value = _required_hash(martin_root / name, root)
        files[key] = value
    for name in ('audit.py', 'calendar.py', 'execution.py', 'ledger.py',
                 'market.py', 'paper.py', 'qualification.py', 'risk.py'):
        key, value = _required_hash(infra_root / name, root)
        files[key] = value
    packages = {
        'trading_contracts': root / 'infra/src/trading_contracts',
        'trading_infra': root / 'infra/src/trading_infra',
        'trading_strategies': root / 'strategy/martin_luk/src/trading_strategies',
        'trading_example_plugins': root / 'strategy/examples/src/trading_example_plugins',
        'trading_example_app': root / 'strategy/examples/src/trading_example_app',
    }
    for package, package_root in packages.items():
        paths = sorted(package_root.rglob('*.py'))
        if not paths:
            raise ValueError('STRATEGY_FREEZE_PACKAGE_EMPTY:' + package)
        for path in paths:
            key, value = _required_hash(path, root)
            files[key] = value
    return {'version': PullbackMachine.VERSION,
            'dependency_lock_sha256': hashlib.sha256(
                (root / 'uv.lock').read_bytes()).hexdigest(),
            'files': files}


def _point_in_time(value,at):
    if isinstance(value,dict):
        for key in ('available_at','observed_at','known_at','begins_at','bid_at','ask_at'):
            if key in value and timestamp(value[key])>at:raise ValueError('FUTURE_DATA:'+key)
        for child in value.values():_point_in_time(child,at)
    elif isinstance(value,list):
        for child in value:_point_in_time(child,at)


def _real_coverage(events,manifest,sources):
    """Prove declared coverage against frozen events, not provider authenticity.

    Completeness assertions are frozen provider/importer metadata; arithmetic,
    schedule and event membership are checked here. No receipt times invented.
    Regime evidence is descriptive and never consumed by the strategy.
    """
    gaps=[];contract=manifest.get('coverage')
    if not isinstance(contract,dict) or type(contract.get('schema_version')) is not int or contract.get('schema_version')!=1:
        return ['REAL_COVERAGE_CONTRACT_REQUIRED']
    classifier=contract.get('regime_classifier',{})
    if not isinstance(classifier,dict) or classifier.get('method')!='endpoint_return_v1':return ['REGIME_CLASSIFIER_CONTRACT_REQUIRED']
    try:global_threshold=number(classifier['absolute_return_threshold'])
    except (KeyError,ValueError,TypeError):return ['REGIME_CLASSIFIER_CONTRACT_REQUIRED']
    if not 0<global_threshold<1:return ['REGIME_CLASSIFIER_CONTRACT_REQUIRED']
    folds=contract.get('folds',{})
    if not isinstance(folds,dict):return ['REAL_COVERAGE_FOLDS_REQUIRED']
    for fold in manifest['folds']:
        fid=fold['id'];spec=folds.get(fid,{})
        if not isinstance(spec,dict):gaps.append('COVERAGE_FOLD_CONTRACT_REQUIRED:'+fid);continue
        symbols=spec.get('universe_symbols',[])
        if not isinstance(symbols,list) or not symbols or any(not isinstance(s,str) or not s for s in symbols) or len(set(symbols))!=len(symbols):
            gaps.append('COVERAGE_SYMBOL_SET_REQUIRED:'+fid);continue
        expected=set(symbols);start=timestamp(fold['test_start']);end=timestamp(fold['test_end'])
        bar_lag=spec.get('bar_max_publish_lag_seconds')
        if type(bar_lag) is not int or not 0<bar_lag<=30:
            gaps.append('BAR_PUBLICATION_CONTRACT_REQUIRED:'+fid);continue
        gap=spec.get('quote_max_gap_seconds')
        if type(gap) is not int or not 0<gap<=15:
            gaps.append('QUOTE_GAP_CONTRACT_REQUIRED:'+fid);continue
        proof=spec.get('source_completeness',{})
        if not isinstance(proof,dict):proof={}
        source=sources.get(proof.get('source_sha256'),{})
        frozen=source.get('coverage_evidence',{}).get(proof.get('id'))
        if (not isinstance(frozen,dict) or frozen.get('pagination_exhausted') is not True
                or frozen.get('truncated') is not False or set(frozen.get('symbols',[]))!=expected
                or frozen.get('start')!=fold['test_start'] or frozen.get('end')!=fold['test_end']):
            gaps.append('SOURCE_COMPLETENESS_NOT_PROVEN:'+fid)
        sample=[e for e in events if start<=timestamp(e['at'])<=end]
        days=calendar().sessions_in_range(start.astimezone(NY).date().isoformat(),end.astimezone(NY).date().isoformat())
        if not len(days):gaps.append('FOLD_NO_REGULAR_SESSION:'+fid)
        for day in days:
            opened=calendar().session_open(day).to_pydatetime();closed=calendar().session_close(day).to_pydatetime()
            a=max(start,opened);b=min(end,closed)
            if a>=b:continue
            label=day.date().isoformat()
            universes=[e for e in events if e['kind']=='universe' and timestamp(e['at'])<=a
                and timestamp(e['at']).astimezone(NY).date()==opened.astimezone(NY).date()]
            if not universes:
                gaps.append('COVERAGE_UNIVERSE_MISSING:'+fid+':'+label)
            else:
                data=universes[-1]['data']
                if (set(data.get('histories',{}))!=expected or set(data.get('indicators',{}))!=expected
                        or any(not data['histories'].get(symbol) or not all(p in data['indicators'].get(symbol,{}) for p in ('9','21','50')) for symbol in symbols)):
                    gaps.append('COVERAGE_UNIVERSE_SYMBOL_MISSING:'+fid+':'+label)
                try:
                    screen=build_candidates(data['histories'],data['indicators'],as_of=universes[-1]['at'],universe_id=data['universe_id'])
                    if screen['universe_complete'] is not True:gaps.append('COVERAGE_UNIVERSE_HISTORY_INCOMPLETE:'+fid+':'+label)
                except (KeyError,ValueError,TypeError):gaps.append('COVERAGE_UNIVERSE_HISTORY_INCOMPLETE:'+fid+':'+label)
            for symbol in symbols:
                bars={timestamp(e['data']['begins_at']):e for e in events
                    if e['kind']=='bar' and e['data'].get('symbol')==symbol and start<=timestamp(e['at'])<=end+timedelta(seconds=bar_lag)}
                t=opened
                while t+timedelta(minutes=5)<=b:
                    if t>=a:
                        event=bars.get(t)
                        if (event is None or event['data'].get('interval')!='5minute'
                                or event['data'].get('bounds')!='regular'
                                or not 0<=(timestamp(event['at'])-t-timedelta(minutes=5)).total_seconds()<=bar_lag):
                            gaps.append('COVERAGE_5MIN_BAR_MISSING:'+fid+':'+symbol+':'+t.isoformat())
                    t+=timedelta(minutes=5)
                quotes=sorted(timestamp(e['at']) for e in sample if e['kind']=='quote'
                    and e['data'].get('symbol')==symbol and a<=timestamp(e['at'])<=b)
                boundaries=[a,*quotes,b]
                if not quotes or any((right-left).total_seconds()>gap for left,right in zip(boundaries,boundaries[1:])):
                    gaps.append('COVERAGE_QUOTE_GAP:'+fid+':'+symbol+':'+label)
        reference=spec.get('regime_evidence',{})
        if not isinstance(reference,dict):reference={}
        record=sources.get(reference.get('source_sha256'),{}).get('regime_evidence',{}).get(reference.get('id'))
        valid=False
        if isinstance(record,dict) and record.get('method')=='endpoint_return_v1' and record.get('benchmark'):
            try:
                points=record['observations'];first,last=points[0],points[-1]
                threshold=number(record['absolute_return_threshold'])
                prices=[number(point['close']) for point in points]
                times=[timestamp(point['at']) for point in points]
                valid=(len(points)>=2 and threshold==global_threshold and all(p>0 for p in prices)
                    and all(x<y for x,y in zip(times,times[1:]))
                    and start<=times[0]<times[-1]<=end)
                change=prices[-1]/prices[0]-1
                label='TREND_UP' if change>=threshold else 'ADVERSE' if change<=-threshold else 'RANGE'
                valid=valid and label==fold['market_regime'] and record.get('classification')==label
            except (KeyError,ValueError,TypeError,IndexError):valid=False
        if not valid:gaps.append('MARKET_REGIME_NOT_PROVEN:'+fid)
    return sorted(set(gaps))


def _recorded_provenance_gaps(value,scope):
    """Typed recorded-data declarations, never inferred from historical prices.

    These declarations bind importer assertions to frozen bytes; they do not
    independently authenticate provider provenance or fabricate receipt times.
    """
    if not isinstance(value,dict):return ['RECORDED_PROVENANCE_DECLARATIONS_REQUIRED:'+scope]
    gaps=[]
    if value.get('input_mode')!='recorded_point_in_time':gaps.append('RECORDED_POINT_IN_TIME_MODE_REQUIRED:'+scope)
    for key in ('actual_receipt_timestamps_verified','actual_point_in_time_verified','historical_adjustment_as_of_verified'):
        if value.get(key) is not True:gaps.append('RECORDED_PROOF_DECLARATION_REQUIRED:'+scope+':'+key)
    return gaps


def _load(manifest,base,*,path_resolver=None):
    resolve=path_resolver or (lambda name:base/name)
    if type(manifest.get('synthetic_input')) is not bool:raise ValueError('EXPLICIT_RESEARCH_PROVENANCE_REQUIRED')
    if type(manifest.get('schema_version')) is not int or manifest.get('schema_version')!=1:raise ValueError('RESEARCH_SCHEMA_UNSUPPORTED')
    if manifest.get('strategy_freeze')!=strategy_freeze():raise ValueError('STRATEGY_FREEZE_MISMATCH')
    policy=manifest['policy']
    required={'account_capital_usd':'500','max_planned_loss_per_trade_usd':'2.50',
        'max_position_notional_usd':'100','stop_new_entries_at_daily_equity_loss_usd':'10',
        'project_max_drawdown_fraction':'0.30','leverage_allowed':False,'overnight_allowed':False}
    if any(policy.get(k)!=v for k,v in required.items()):raise ValueError('RESEARCH_RISK_POLICY_MISMATCH')
    folds=manifest['folds']
    if len(folds)<2 or len({f['market_regime'] for f in folds})<2:raise ValueError('MULTIPLE_FOLDS_AND_REGIMES_REQUIRED')
    if len({f['id'] for f in folds})!=len(folds):raise ValueError('DUPLICATE_FOLD_ID')
    prior=None
    for fold in folds:
        a,b,c,d=[timestamp(fold[k]) for k in ('train_start','train_end','test_start','test_end')]
        if not a<=b<c<=d or prior is not None and c<=prior:raise ValueError('INVALID_WALK_FORWARD_BOUNDARIES')
        prior=d
    costs=manifest['costs']
    if costs.get('fee_basis')!='roundtrip_buffer_charged_on_entry':raise ValueError('EXPLICIT_FEE_BASIS_REQUIRED')
    for key in ('per_share_fee','entry_slippage_per_share','exit_slippage_per_share','model_cost_usd','data_cost_usd'):
        if number(costs[key])<0:raise ValueError('NEGATIVE_RESEARCH_COST')
    for scenario in manifest['scenarios']:
        if (type(scenario['protection_delay_seconds']) is not int or scenario['protection_delay_seconds']<0
                or not 0<number(scenario['fill_fraction_per_quote'])<=1
                or number(scenario['stop_stress_slippage_per_share'])<0):raise ValueError('INVALID_RESEARCH_SCENARIO')
    if len({s['id'] for s in manifest['scenarios']})!=len(manifest['scenarios']):raise ValueError('DUPLICATE_SCENARIO_ID')
    if not manifest['scenarios']:raise ValueError('RESEARCH_SCENARIOS_REQUIRED')
    gaps=[];sources={}
    if not manifest['synthetic_input']:gaps.extend(_recorded_provenance_gaps(manifest,'manifest'))
    for item in manifest['sources']:
        path=resolve(item['path'])
        if not path.is_file():gaps.append('SOURCE_MISSING:'+item['path']);continue
        actual=hashlib.sha256(path.read_bytes()).hexdigest()
        if actual!=item['sha256']:raise ValueError('SOURCE_HASH_MISMATCH:'+item['path'])
        if not item.get('provider'):raise ValueError('SOURCE_PROVIDER_REQUIRED')
        sources[actual]=dict(item)
        if manifest.get('synthetic_input') is False:
            document=json.loads(path.read_bytes())
            gaps.extend(_recorded_provenance_gaps(document.get('provenance') if isinstance(document,dict) else None,'source:'+item['path']))
            records=document if isinstance(document,list) else document.get('events',[])
            sources[actual]['event_fingerprints']={digest({k:v for k,v in event.items()
                if k not in {'source_sha256','synthetic_input'}}) for event in records}
            if isinstance(document,dict):
                for key in ('coverage_evidence','regime_evidence'):
                    value=document.get(key,{})
                    if not isinstance(value,dict):raise ValueError('INVALID_SOURCE_EVIDENCE:'+key)
                    sources[actual][key]=value
            if not records:gaps.append('REAL_SOURCE_HAS_NO_NORMALIZED_EVENTS:'+item['path'])
    path=resolve(manifest['dataset']['path'])
    if not path.is_file():gaps.append('DATASET_MISSING:'+manifest['dataset']['path']);return [],gaps
    raw=path.read_bytes()
    if hashlib.sha256(raw).hexdigest()!=manifest['dataset']['sha256']:raise ValueError('DATASET_HASH_MISMATCH')
    events=json.loads(raw)
    if not isinstance(events,list) or not events:raise ValueError('RESEARCH_EVENTS_REQUIRED')
    seen={};previous=None;unique_events=[]
    for event in events:
        at=timestamp(event['at']);fp=digest(event)
        if previous is not None and at<previous:raise ValueError('RESEARCH_EVENTS_NOT_CHRONOLOGICAL')
        previous=at
        if event['id'] in seen and seen[event['id']]!=fp:raise ValueError('RESEARCH_EVENT_ID_CONFLICT')
        if event['id'] in seen:continue
        seen[event['id']]=fp;unique_events.append(event)
        if event.get('source_sha256') not in sources:gaps.append('EVENT_SOURCE_NOT_VERIFIED:'+event['id'])
        elif manifest.get('synthetic_input') is False:
            frozen=digest({k:v for k,v in event.items() if k not in {'source_sha256','synthetic_input'}})
            if frozen not in sources[event['source_sha256']]['event_fingerprints']:
                gaps.append('EVENT_NOT_IN_FROZEN_SOURCE:'+event['id'])
        if event.get('synthetic_input') is not manifest['synthetic_input']:raise ValueError('RESEARCH_PROVENANCE_MISMATCH')
        if event['kind'] not in {'universe','bar','quote','data_status'}:raise ValueError('UNSUPPORTED_RESEARCH_EVENT')
        _point_in_time(event.get('data',{}),at)
    for fold in folds:
        sample=[e for e in events if timestamp(fold['test_start'])<=timestamp(e['at'])<=timestamp(fold['test_end'])]
        if not sample:gaps.append('FOLD_NO_EVENTS:'+fold['id'])
        if manifest['synthetic_input'] and not any(e['kind']=='universe' for e in sample):gaps.append('FOLD_NO_POINT_IN_TIME_UNIVERSE:'+fold['id'])
        if not any(e['kind']=='quote' for e in sample):gaps.append('FOLD_NO_EXECUTABLE_QUOTES:'+fold['id'])
    if not sources:gaps.append('NO_VERIFIED_MARKET_SOURCE')
    if not manifest['synthetic_input'] and any(s.get('synthetic') is not False for s in sources.values()):
        gaps.append('REAL_SOURCE_PROVENANCE_NOT_DECLARED')
    if not manifest['synthetic_input']:gaps.extend(_real_coverage(unique_events,manifest,sources))
    return unique_events,sorted(set(gaps))


def _replay(events,fold,scenario,manifest,carry=None):
    """Run frozen LiveSignals; PaperDesk owns synthetic accounting and risk."""
    costs=manifest['costs'];clock=[timestamp(fold['train_start'])];counter=[0]
    with tempfile.TemporaryDirectory(prefix='martin-research-') as tmp:
        book=ExecutionBook(Path(tmp)/'signals.sqlite',manifest['policy'],simulation=True,clock=lambda:clock[0])
        signals=LiveSignals(book,cost_buffer_per_share=str(number(costs['per_share_fee'])+number(costs['entry_slippage_per_share'])))
        desk=PaperDesk(Path(tmp)/'paper.sqlite');journal=[];orders={};trades={};completed=[];day=None;day_equity=Decimal('500');high=Decimal('500');maxdd=Decimal(0);exposures=[]
        opening_cash=number(carry['paper']['cash']) if carry else Decimal('500')
        def apply(kind,at,**kwargs):
            counter[0]+=1
            return desk.apply({'id':'research-'+str(counter[0]),'kind':kind,'at':at,**kwargs})
        initial_policy={
            'risk_fraction':'0.005','max_position_fraction':'0.20','portfolio_risk_fraction':'0.02',
            'daily_loss_fraction':'0.02','max_daily_entries':5,'cost_buffer_per_share':costs['per_share_fee']}
        if carry:
            # Exact private simulation checkpoint, not a reset or account flow.
            desk.db.execute('INSERT INTO paper_state VALUES(1,?)',(canonical(carry['paper']),));desk.db.commit()
            day=carry['day'];day_equity=number(carry['day_start_equity'])
            high=number(carry['equity_high']);maxdd=number(carry['max_drawdown_fraction'])
        else:apply('init',fold['train_start'],synthetic=True,capital='500',policy=initial_policy)
        try:
            for raw in events:
                at=timestamp(raw['at'])
                if at<timestamp(fold['train_start']) or at>timestamp(fold['test_end']):continue
                clock[0]=at;out_of_sample=at>=timestamp(fold['test_start'])
                event={**raw,'synthetic_input':True};signal=signals.consume(event)
                record={'event_id':raw['id'],'at':raw['at'],'phase':'test' if out_of_sample else 'train_warmup',
                    'kind':raw['kind'],'outcome':'NO_SIGNAL','strategy_state':signals.state()['machines'],
                    'signal':signal,'execution_events':[]}
                if raw['kind']=='universe':record['screening']=signals.state().get('screening_summary',{})
                if not out_of_sample:
                    if signal:signals.complete(signal['id'],result={'outcome':'TRAIN_ONLY_NOT_TRADED'})
                    journal.append(record);continue
                state=desk.snapshot();equity=number(state['nav']['equity'])
                label=raw['at'][:10]
                if day!=label:day=label;day_equity=equity
                for oid in list(orders):
                    if state['orders'][oid]['remaining'] and (state['nav']['halted'] or state['orders'][oid].get('cancel_requested')
                            or raw['kind']=='data_status' and not raw.get('healthy')):
                        apply('cancel',raw['at'],order_id=oid);record['execution_events'].append({'outcome':'RISK_OR_SOURCE_CANCEL','order_id':oid});continue
                    if state['orders'][oid]['remaining'] and (at-orders[oid]['at']).total_seconds()>=60:
                        apply('cancel',raw['at'],order_id=oid);record['execution_events'].append({'outcome':'LIMIT_EXPIRED','order_id':oid})
                if raw['kind']=='quote':
                    quote=raw['data'];symbol=quote['symbol'];bid=number(quote['bid']);ask=number(quote['ask']);mid=(bid+ask)/2
                    for k in ('bid_at','ask_at','observed_at'):
                        if not 0<=(at-timestamp(quote[k])).total_seconds()<=15:raise ValueError('RESEARCH_STALE_QUOTE')
                    if not 0<bid<=ask:raise ValueError('RESEARCH_INVALID_QUOTE')
                    for oid,meta in orders.items():
                        order=desk.snapshot()['orders'][oid]
                        if order['symbol']!=symbol or not order['remaining'] or at<=meta['at']:continue
                        px=ask+number(costs['entry_slippage_per_share'])
                        if px>number(order['limit']):
                            record['execution_events'].append({'outcome':'LIMIT_NOT_MARKETABLE','order_id':oid});continue
                        qty=max(1,int((Decimal(order['remaining'])*number(scenario['fill_fraction_per_quote'])).to_integral_value(rounding=ROUND_FLOOR)))
                        apply('fill',raw['at'],order_id=oid,quantity=qty,price=str(px))
                        t=trades.setdefault(symbol,{'order_id':oid,'quantity':0,'entry_notional':Decimal(0),
                            'entry_mid_notional':Decimal(0),'fees':Decimal(0),'spread_cost':Decimal(0),'slippage_cost':Decimal(0),'tranches':[]})
                        t['quantity']+=qty;t['entry_notional']+=px*qty;t['entry_mid_notional']+=mid*qty
                        t['fees']+=number(costs['per_share_fee'])*qty;t['spread_cost']+=(ask-mid)*qty
                        t['slippage_cost']+=(px-ask)*qty;t['tranches'].append({'at':at,'quantity':qty})
                        record['execution_events'].append({'outcome':'SIMULATED_FILL','quantity':qty,'price':str(px),'order_id':oid})
                    state=desk.snapshot()
                    if symbol in state['positions']:
                        t=trades[symbol];protected=sum(x['quantity'] for x in t['tranches'] if (at-x['at']).total_seconds()>=scenario['protection_delay_seconds'])
                        apply('protect',raw['at'],symbol=symbol,quantity=protected)
                        apply('mark',raw['at'],symbol=symbol,price=str(bid))
                        pos=desk.snapshot()['positions'][symbol]
                        decision=exit_decision(pos,quote,as_of=raw['at'],daily_ema9=signals.state()['ema9'].get(symbol),allow_overnight=False)
                        record['exit_decision']=decision
                        if decision['action']=='RAISE_STOP':
                            # Match the current execution core: no verified safe
                            # native replacement exists. A suggested higher stop
                            # cannot silently become effective in an OOS replay.
                            record['execution_events'].append({'outcome':'DEFERRED_STOP_ADJUSTMENT',
                                'reason':'SAFE_NATIVE_REPLACEMENT_NOT_VERIFIED',
                                'suggested_stop':decision['stop'],'effective_stop':pos['stop'],
                                'effective_stop_unchanged':True})
                        elif decision['action']=='EXIT_ALL':
                            if decision['reason']=='STRUCTURAL_STOP' and protected<pos['quantity']:
                                record['execution_events'].append({'outcome':'UNPROTECTED_STOP_DELAY','unprotected_quantity':pos['quantity']-protected})
                            else:
                                oid=t['order_id']
                                if desk.snapshot()['orders'][oid]['remaining']:apply('cancel',raw['at'],order_id=oid)
                                slip=number(costs['exit_slippage_per_share'])
                                if decision['reason']=='STRUCTURAL_STOP':slip+=number(scenario['stop_stress_slippage_per_share'])
                                px=bid-slip
                                if px<=0:raise ValueError('STRESS_EXIT_PRICE_NONPOSITIVE')
                                qty=pos['quantity'];apply('exit',raw['at'],symbol=symbol,quantity=qty,price=str(px))
                                gross=mid*qty-t['entry_mid_notional'];spread=t['spread_cost']+(mid-bid)*qty
                                slippage=t['slippage_cost']+slip*qty;net=gross-spread-slippage-t['fees']
                                completed.append({'symbol':symbol,'order_id':oid,'at':raw['at'],'quantity':qty,
                                    'entry_notional_usd':str(t['entry_notional']),'exit_notional_usd':str(px*qty),
                                    'gross_pnl_usd':str(gross),'net_pnl_usd':str(net),'spread_cost_usd':str(spread),
                                    'slippage_cost_usd':str(slippage),'fees_usd':str(t['fees']),
                                    'exit_reason':decision['reason'],'protection_delays_assumed_seconds':scenario['protection_delay_seconds']})
                                del trades[symbol]
                if signal:
                    symbol=signal['plan']['symbol'];state=desk.snapshot();equity=number(state['nav']['equity'])
                    blocked=(day_equity-equity>=Decimal('10') or symbol in state['positions']
                        or any(o['remaining'] and o['symbol']==symbol for o in state['orders'].values()))
                    if blocked:record['outcome']='PORTFOLIO_OR_DAILY_RISK_REJECTED'
                    else:
                        result=apply('submit',raw['at'],order_id=signal['id'],symbol=symbol,
                            entry_ceiling=signal['plan']['entry_ceiling'],stop=signal['plan']['stop'],cost_buffer_per_share=costs['per_share_fee'])
                        if signal['id'] in result['state']['orders']:
                            order=result['state']['orders'][signal['id']]
                            planned=(number(order['limit'])-number(order['stop'])+number(order['cost']))*order['quantity']
                            if number(order['limit'])*order['quantity']>100 or planned>Decimal('2.50'):
                                apply('cancel',raw['at'],order_id=signal['id']);record['outcome']='ABSOLUTE_RISK_CAP_REJECTED'
                            else:orders[signal['id']]={'at':at};record['outcome']='LIMIT_SUBMITTED'
                        else:record['outcome']='RISK_SIZING_REJECTED';record['risk_result']=result['outcome']
                    signals.complete(signal['id'],result={'outcome':record['outcome']})
                state=desk.snapshot();equity=number(state['nav']['equity']);high=max(high,equity);maxdd=max(maxdd,1-equity/high)
                pending_risk=sum((number(o['limit'])-number(o['stop'])+number(o['cost']))*o['remaining'] for o in state['orders'].values())
                # Match PaperDesk's admission basis: filled position stop risk
                # continues occupying budget after pending entry risk disappears.
                open_risk=sum(max(number(p['mark'])-number(p['stop']),Decimal(0))*p['quantity'] for p in state['positions'].values())
                risk_budget=equity*number(initial_policy['portfolio_risk_fraction'])
                occupied=pending_risk+open_risk
                exposures.append({'at':raw['at'],'equity_usd':str(equity),'reserved_risk_usd':str(pending_risk),
                    'open_position_risk_usd':str(open_risk),'occupied_risk_usd':str(occupied),
                    'portfolio_risk_budget_usd':str(risk_budget),
                    'risk_budget_utilization_fraction':str(occupied/risk_budget) if risk_budget>0 else None,
                    'remaining_risk_budget_usd':str(max(Decimal(0),risk_budget-occupied)),
                    'unprotected_quantity':sum(p['quantity']-p['protected'] for p in state['positions'].values()),
                    'unprotected_notional_usd':str(sum(number(p['mark'])*(p['quantity']-p['protected']) for p in state['positions'].values())),
                    'risk_basis':'pending limit-to-stop plus filled mark-to-stop, matching PaperDesk; not a guaranteed loss cap',
                    'open_notional_usd':str(sum(number(p['mark'])*p['quantity'] for p in state['positions'].values()))})
                journal.append(record)
            state=desk.snapshot();gross=sum((number(t['gross_pnl_usd']) for t in completed),Decimal(0));net=sum((number(t['net_pnl_usd']) for t in completed),Decimal(0))
            losses=[number(t['net_pnl_usd']) for t in completed if number(t['net_pnl_usd'])<0]
            streak=int(carry.get('loss_streak_at_end',0)) if carry else 0
            best=int(carry.get('max_consecutive_losses',0)) if carry else 0
            fold_streak=fold_best=0
            for t in completed:
                lost=number(t['net_pnl_usd'])<0
                streak=streak+1 if lost else 0;best=max(best,streak)
                fold_streak=fold_streak+1 if lost else 0;fold_best=max(fold_best,fold_streak)
            tail_candidates=losses[:]
            if carry and carry.get('scenario_loss_tail_min_usd') is not None:
                tail_candidates.append(number(carry['scenario_loss_tail_min_usd']))
            scenario_tail=str(min(tail_candidates)) if tail_candidates else None
            cash_delta=number(state['cash'])-opening_cash
            if not state['positions'] and cash_delta!=net:raise ValueError('RESEARCH_CASH_PNL_RECONCILIATION_FAILED')
            return {'fold_id':fold['id'],'market_regime':fold['market_regime'],'scenario':scenario,
                'journal':journal,'trades':completed,'exposures':exposures,'gross_pnl_usd':str(gross),
                'opening_cash_usd':str(opening_cash),'closing_cash_usd':state['cash'],
                'cash_pnl_reconciled':not state['positions'] and cash_delta==net,
                'fee_basis':'roundtrip_buffer_charged_on_entry',
                'training_mode':'frozen_rules_warmup_only_no_parameter_fit',
                'stop_adjustment_execution':'deferred_safe_native_replacement_not_verified',
                'risk_state_carried_across_folds':bool(carry),
                'risk_checkpoint':{'paper':state,'day':day,'day_start_equity':str(day_equity),
                    'equity_high':str(high),'max_drawdown_fraction':str(maxdd),
                    'loss_streak_at_end':streak,'max_consecutive_losses':best,
                    'scenario_loss_tail_min_usd':scenario_tail},
                'net_trading_pnl_usd':str(net),'net_after_run_cost_usd':str(net-number(costs['model_cost_usd'])-number(costs['data_cost_usd'])),
                'model_cost_usd':str(number(costs['model_cost_usd'])),
                'data_cost_usd':str(number(costs['data_cost_usd'])),
                'run_cost_basis':'manifest assumptions; not observed invoices or model usage',
                'run_cost_allocation':'model/data costs charged once per fold/scenario; never add scenario totals together',
                'turnover_usd':str(sum((number(t['entry_notional_usd'])+number(t['exit_notional_usd']) for t in completed),Decimal(0))),
                'turnover_shares':sum(t['quantity']*2 for t in completed),
                'loss_tail_min_usd':str(min(losses)) if losses else None,
                'scenario_loss_tail_min_usd':scenario_tail,'fold_max_consecutive_losses':fold_best,
                'max_consecutive_losses':best,'loss_streak_at_end':streak,
                'max_drawdown_fraction':str(maxdd),'no_signal_events':sum(r['outcome']=='NO_SIGNAL' for r in journal if r['phase']=='test'),
                'unfilled_orders':[oid for oid,o in state['orders'].items() if o['filled']==0],
                'partial_orders':[oid for oid,o in state['orders'].items() if 0<o['filled']<o['quantity']],
                'open_positions':state['positions'],'complete_intraday_sample':not state['positions'],
                'pnl_includes_open_positions':False,'simulated':True,'broker_validation':False}
        finally:book.close();desk.close()


def run_research(manifest_path,output_dir):
    path=Path(manifest_path);out=Path(output_dir);out.mkdir(parents=True,exist_ok=True)
    missing_manifest=not path.is_file();read_failure=None
    try:raw=path.read_bytes() if not missing_manifest else b''
    except OSError as exc:raw=b'';read_failure=type(exc).__name__+':'+str(exc)
    run_id=str(uuid.uuid4());registry=sqlite3.connect(out/'research-runs.sqlite')
    registry.execute('CREATE TABLE IF NOT EXISTS runs(id TEXT PRIMARY KEY,started_at TEXT,manifest_sha256 TEXT,manifest TEXT,status TEXT,result TEXT)')
    registry.execute('INSERT INTO runs VALUES(?,?,?,?,?,NULL)',(run_id,datetime.now(timezone.utc).isoformat(),hashlib.sha256(raw).hexdigest(),raw.decode(errors='replace'),'STARTED'));registry.commit()
    report={'run_id':run_id,'status':'FAILED','data_gaps':[],'results':[],
        'strategy_profitability_validated':False,'execution_is_simulated':True,'robinhood_execution_validated':False,
        'provenance_cryptographically_authenticated':False}
    try:
        if read_failure:raise ValueError('RESEARCH_MANIFEST_UNREADABLE:'+read_failure)
        if missing_manifest:raise ValueError('RESEARCH_MANIFEST_MISSING:'+str(path))
        manifest=json.loads(raw);report['manifest']=manifest
        events,gaps=_load(manifest,path.parent);report['data_gaps']=gaps
        if gaps:report['status']='DATA_GAP'
        else:
            for scenario in manifest['scenarios']:
                carry=None;blocked=False
                for fold in manifest['folds']:
                    if blocked:
                        report['results'].append({'fold_id':fold['id'],'scenario':scenario,'complete_intraday_sample':False,
                            'status':'BLOCKED_PREVIOUS_FOLD_OPEN_EXPOSURE','risk_state_not_reset':True});continue
                    result=_replay(events,fold,scenario,manifest,carry=carry)
                    report['results'].append(result);carry=result['risk_checkpoint']
                    blocked=not result['complete_intraday_sample']
            report['status']='SIMULATED_COMPLETE' if all(r['complete_intraday_sample'] for r in report['results']) else 'INCOMPLETE_OPEN_EXPOSURE'
            report['synthetic_market_input']=manifest['synthetic_input']
            report['note']='No selection or ranking of scenarios; all frozen folds, no-signal, rejected and unfilled cases retained. Real market input still uses assumed execution.'
    except (ValueError,KeyError,TypeError,OSError) as exc:report['error']=type(exc).__name__+':'+str(exc)
    finally:
        encoded=json.dumps(report,ensure_ascii=False,indent=2);target=out/(run_id+'.json');target.write_text(encoded+'\n')
        registry.execute('UPDATE runs SET status=?,result=? WHERE id=?',(report['status'],encoded,run_id));registry.commit();registry.close()
    return {**report,'report_path':str(target.resolve()),'registry_path':str((out/'research-runs.sqlite').resolve())}
