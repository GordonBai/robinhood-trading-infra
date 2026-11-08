"""End-to-end shadow event-inbox runner. Never dispatches real broker writes."""

from datetime import datetime, timezone
import json
from pathlib import Path
import time

from .engine import ShadowEngine
from martin_agentic.ledger import timestamp
from martin_agentic.shadow import single_instance


def run_shadow(inbox,db_path,*,once=False,poll_seconds=5):
    if not 0<poll_seconds<=60 or not Path(inbox).is_dir():
        raise ValueError('Require an event inbox and a poll interval in (0,60]')
    with single_instance(db_path):
        engine=ShadowEngine(db_path)
        engine.db.execute('''CREATE TABLE IF NOT EXISTS service_health(
            id INTEGER PRIMARY KEY CHECK(id=1),payload TEXT NOT NULL)''')
        engine.db.commit()
        try:
            while True:
                events=[]
                errors=[]
                for path in sorted(Path(inbox).glob('*.json')):
                    if path.is_symlink():
                        continue
                    try:
                        payload=json.loads(path.read_text())
                        if not isinstance(payload,dict) or not isinstance(payload.get('events'),list):
                            raise ValueError('Expected an object with events array')
                        for e in payload['events']:
                            if not isinstance(e,dict) or not isinstance(e.get('id'),str):
                                raise ValueError('Invalid event envelope')
                            timestamp(e['at'])
                        events.extend(payload['events'])
                    except (ValueError,KeyError,TypeError,OSError) as exc:
                        errors.append({'file':path.name,'reason':str(exc)})
                # Block the entire batch on malformed input, not just one stream.
                applied=0
                if not errors:
                    for event in sorted(events,key=lambda e:timestamp(e['at'])):
                        try:
                            engine.apply(event)
                            applied+=1
                        except (ValueError,KeyError,TypeError) as exc:
                            errors.append({'event_id':event['id'],'reason':str(exc)})
                            break
                health={'mode':'shadow','healthy':bool(events) and not errors,
                        'checked_at':datetime.now(timezone.utc).isoformat(),'events_checked':applied,
                        'errors':errors,'live_orders':0,'source':'local-event-inbox',
                        'continuous_authenticated_source':False}
                health['current_market_ready']=False
                health['replay_only']=True
                engine.db.execute('INSERT INTO service_health VALUES(1,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload',
                                  (json.dumps(health,sort_keys=True),))
                engine.db.commit()
                if once:
                    return health
                print(json.dumps(health,ensure_ascii=False,sort_keys=True),flush=True)
                time.sleep(poll_seconds)
        finally:
            engine.close()
