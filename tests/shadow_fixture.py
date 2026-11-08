"""Deterministic synthetic prices, not source evidence or estimated returns."""

from copy import deepcopy
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal

from martin_agentic.calendar import calendar, NY
from martin_luk.features import build_candidates


def universe():
    sessions=calendar().sessions_window('2026-10-14',-64)
    histories={}
    indicators={}
    observed='2026-10-15T09:00:00-04:00'
    for s in range(10):
        symbol='DEMO' if s==0 else f'S{s}'
        bars=[]
        for i,label in enumerate(sessions):
            close=Decimal('36')+Decimal(i)*Decimal('0.07') if s==0 else Decimal('40')-Decimal(i)*Decimal('0.01')-Decimal(s)/100
            low,high=close*Decimal('.96'),close*Decimal('1.04')
            if s==0 and i>=56:
                low,high,close=Decimal('39.7'),Decimal('41.7'),Decimal('40.5')
                if i==59:
                    low,high,close=Decimal('39.5'),Decimal('41.6'),Decimal('40.0')
                if i>=62:
                    low,high,close=Decimal('39.7'),Decimal('40.8'),Decimal('40.3')
            dt=label.to_pydatetime().replace(tzinfo=timezone.utc)
            available=datetime.combine(dt.date()+timedelta(days=1),time(0),NY)
            bars.append({'symbol':symbol,'begins_at':dt.isoformat(),'available_at':available.isoformat(),
                         'observed_at':observed,'interval':'day','bounds':'regular','adjustment':'split',
                         'open':str(close),'high':str(high),'low':str(low),'close':str(close),'volume':3_000_000})
        histories[symbol]=bars
        indicators[symbol]={str(p):{'type':'ema','period':p,'value':v,'interval':'day','bounds':'regular',
                                   'adjustment':'split','begins_at':bars[-1]['begins_at'],
                                   'available_at':bars[-1]['available_at'],'observed_at':observed}
                            for p,v in ((9,'40.0'),(21,'39.9'),(50,'38.0'))}
    return {'universe_id':'synthetic-ten-stock-universe','histories':histories,'indicators':indicators}


def events():
    data=universe()
    setups=build_candidates(data['histories'],data['indicators'],as_of='2026-10-15T09:25:00-04:00',universe_id=data['universe_id'])
    if not setups['candidates']:
        raise AssertionError(setups)
    setup=setups['candidates'][0]
    lower,upper=Decimal(setup['support_low']),Decimal(setup['support_high'])
    flush=lower-Decimal('.29')
    reclaim_high=upper+Decimal('.20')
    trigger=(reclaim_high+Decimal('.01')).quantize(Decimal('.01'))
    # Use the same legal tick rounding as the strategy.
    from decimal import ROUND_CEILING
    trigger=( (reclaim_high+Decimal('.01'))/Decimal('.01')).to_integral_value(rounding=ROUND_CEILING)*Decimal('.01')
    result=[{'id':'init','kind':'init','at':'2026-10-15T09:00:00-04:00','mode':'shadow','synthetic':True,
             'capital':'500','policy':{'risk_fraction':'0.005','max_position_fraction':'0.33',
             'portfolio_risk_fraction':'0.02','daily_loss_fraction':'0.03','max_daily_entries':5,'cost_buffer_per_share':'0.05'}},
            {'id':'universe','kind':'universe','at':'2026-10-15T09:25:00-04:00','data':data}]
    for index,(op,hi,lo,cl) in enumerate(((upper,upper+Decimal('.1'),flush,lower),
                                        (lower,reclaim_high,flush+Decimal('.1'),upper+Decimal('.1')))):
        start=datetime(2026,10,15,9,30,tzinfo=NY)+timedelta(minutes=5*index)
        at=start+timedelta(minutes=5,seconds=2)
        result.append({'id':f'bar-{index}','kind':'bar','at':at.isoformat(),'data':{
            'symbol':'DEMO','begins_at':start.isoformat(),'available_at':at.isoformat(),'observed_at':at.isoformat(),
            'interval':'5minute','bounds':'regular','adjustment':'split','open':str(op),'high':str(hi),
            'low':str(lo),'close':str(cl),'volume':20000}})
    for ident,at,bid in [('trigger','2026-10-15T09:40:05-04:00',trigger),
                         ('fill','2026-10-15T09:40:06-04:00',trigger),
                         ('profit','2026-10-15T10:00:00-04:00',trigger+Decimal('5')),
                         ('close','2026-10-15T15:55:00-04:00',trigger+Decimal('6'))]:
        result.append({'id':ident,'kind':'quote','at':at,'data':{'symbol':'DEMO','observed_at':at,
                       'bid_at':at,'ask_at':at,'bid':str(bid),'ask':str(bid+Decimal('.01'))}})
    return result
