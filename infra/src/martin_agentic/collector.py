"""Read-only collection and recorded-data bridge, independent of credentials.

The caller supplies a formally authenticated RobinhoodReader transport. No
auth token is copied, and writes are impossible through its tool allowlist.
"""

import hashlib
import json
from .market import inspect_recording


def universe_event(recording, *, as_of, universe_id):
    normalized=inspect_recording(recording,as_of=as_of)
    if not normalized['data_usable']:
        raise ValueError('Recording rejected: '+json.dumps(normalized['errors']+normalized['freshness_errors']))
    histories={}
    indicators={}
    for b in normalized['bars']:
        if b['interval']=='day':
            histories.setdefault(b['symbol'],[]).append(b)
    for p in normalized['indicators']:
        if p['interval']=='day':
            group=indicators.setdefault(p['symbol'],{})
            key=str(p['period'])
            if key not in group or p['begins_at']>group[key]['begins_at']:
                group[key]=p
    if set(histories)!=set(indicators):
        raise ValueError('History and indicator universe mismatch')
    digest=hashlib.sha256(json.dumps(recording,sort_keys=True).encode()).hexdigest()
    return {'id':'universe-'+digest[:24],'kind':'universe','at':as_of,
            'data':{'histories':histories,'indicators':indicators,'universe_id':universe_id},
            'source_sha256':digest,'synthetic_input':recording['synthetic']}


def collect_daily(reader, *, symbols, start_time, end_time):
    if len(symbols)<10 or len(set(symbols))!=len(symbols):
        raise ValueError('Collector requires an explicit unique universe of at least ten symbols')
    records=[]
    for offset in range(0,len(symbols),10):
        base={'symbols':symbols[offset:offset+10],'interval':'day','bounds':'regular','adjustment_type':'split',
              'start_time':start_time,'end_time':end_time}
        records.append(reader.fetch('get_equity_historicals',base))
        for period in (9,21,50):
            records.append(reader.fetch('get_equity_technical_indicators',
                                        {**base,'type':'ema','period':period,'output':'last:3'}))
    return {'schema_version':1,'provider':'robinhood','synthetic':False,'records':records}
