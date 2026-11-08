"""Official, account-bound broker observations and a durable execution inbox.

No placement/cancellation tools are available here. First observation establishes
a position baseline; later position deltas must agree with newly seen executions.
"""

from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import time
from zoneinfo import ZoneInfo

from .calendar import is_regular
from .ledger import timestamp
from .market import unwrap_response
from .risk import number
from .transport import CodexBrokerReadTransport, TransportError
from .audit import AuditTrail, digest as audit_digest


TERMINAL = frozenset({'filled', 'cancelled', 'canceled', 'rejected', 'failed', 'voided',
                      'partially_filled_rest_cancelled', 'locate_failed'})
KNOWN = TERMINAL | {'new', 'queued', 'confirmed', 'unconfirmed', 'partially_filled',
                    'pending_cancelled', 'locating'}
NY = ZoneInfo('America/New_York')
APPROVAL_NO_ORDER_TERMINAL = frozenset({'APPROVAL_STATE_DECLINED','APPROVAL_STATE_EXPIRED'})
APPROVAL_STATES = APPROVAL_NO_ORDER_TERMINAL | {'APPROVAL_STATE_PENDING','APPROVAL_STATE_APPROVED','APPROVAL_STATE_AUTO_APPROVED'}


def normalize_approval(raw,account_number):
    """Keep exact formal approval evidence without account identifiers/text."""
    if (not isinstance(raw,dict) or raw.get('account_number')!=account_number
            or not isinstance(raw.get('approval_id'),str) or not 0<len(raw['approval_id'])<=200
            or raw.get('asset_class') not in {'ASSET_CLASS_EQUITY','ASSET_CLASS_OPTION','ASSET_CLASS_CRYPTO'}
            or not isinstance(raw.get('state'),str) or not 0<len(raw['state'])<=100
            or type(raw.get('is_edited')) is not bool):
        raise ValueError('BROKER_APPROVAL_IDENTITY_OR_SCHEMA_INVALID')
    return {k:raw[k] for k in ('approval_id','asset_class','state','is_edited')}


def valid_session_order(order, at):
    return order['market_hours'] == 'regular_hours' and (order['time_in_force'] == 'gtc'
        or order['time_in_force'] == 'gfd' and timestamp(order['created_at']).astimezone(NY).date() == at.astimezone(NY).date())


def utc_now():
    return datetime.now(timezone.utc)


def rows(data, key):
    if not isinstance(data, dict) or not isinstance(data.get(key), list) or any(not isinstance(r, dict) for r in data[key]):
        raise ValueError('BROKER_COLLECTION_SCHEMA_INVALID')
    return data[key]


def fetch_pages(transport, tool, args, key):
    result, cursor, seen = [], None, set()
    for _ in range(100):
        data = unwrap_response(transport(tool, {**args, **({'cursor': cursor} if cursor else {})}))
        result.extend(rows(data, key))
        cursor = data.get('next')
        if not cursor:
            return result
        if not isinstance(cursor, str) or cursor in seen:
            raise ValueError('BROKER_PAGINATION_INVALID')
        seen.add(cursor)
    raise ValueError('BROKER_PAGINATION_LIMIT')


def normalize_positions(source):
    result = {}
    for p in source:
        symbol = p['symbol']
        if not isinstance(symbol, str) or not re.fullmatch(r'[A-Z][A-Z0-9.\-]{0,14}', symbol) or symbol in result:
            raise ValueError('BROKER_POSITION_IDENTITY_INVALID')
        qty, available, reserved = (number(p[k]) for k in ('quantity', 'shares_available_for_sells', 'shares_held_for_sells'))
        if available < 0 or reserved < 0 or qty >= 0 and available + reserved > qty:
            raise ValueError('BROKER_POSITION_HOLDS_INVALID')
        result[symbol] = {'quantity': str(qty), 'available_for_sells': str(available),
                          'held_for_sells': str(reserved),
                          'average_price': str(number(p['average_buy_price'])) if p.get('average_buy_price') is not None else None}
    return result


def normalize_order(raw, observed_at):
    symbol, side = raw['symbol'], raw['side']
    if not isinstance(symbol, str) or not re.fullmatch(r'[A-Z][A-Z0-9.\-]{0,14}', symbol) or side not in {'buy', 'sell'}:
        raise ValueError('BROKER_ORDER_IDENTITY_INVALID')
    order_id = raw['id']
    if not isinstance(order_id, str) or not order_id:
        raise ValueError('BROKER_ORDER_IDENTITY_INVALID')
    qty = number(raw['quantity']) if raw.get('quantity') is not None else None
    cumulative = number(raw['cumulative_quantity'])
    if cumulative < 0 or qty is not None and (qty <= 0 or cumulative > qty):
        raise ValueError('BROKER_ORDER_QUANTITY_INVALID')
    executions = []
    seen = set()
    source = raw.get('executions')
    if source is None and cumulative == 0:
        source = []
    if not isinstance(source, list):
        raise ValueError('BROKER_EXECUTION_DETAILS_MISSING')
    for e in source:
        eid = e['id']
        price, q, fees = number(e['price']), number(e['quantity']), number(e['fees'])
        if not isinstance(eid, str) or not eid or eid in seen or price <= 0 or q <= 0 or fees < 0 or timestamp(e['timestamp']) > timestamp(observed_at):
            raise ValueError('BROKER_EXECUTION_INVALID')
        seen.add(eid)
        executions.append({'id': eid, 'order_id': order_id, 'symbol': symbol, 'side': side,
                           'quantity': format(q.normalize(),'f'), 'price': format(price.normalize(),'f'),
                           'fees': format(fees.normalize(),'f'),
                           'executed_at': timestamp(e['timestamp']).astimezone(timezone.utc).isoformat()})
    if sum((number(e['quantity']) for e in executions), Decimal(0)) != cumulative:
        raise ValueError('BROKER_CUMULATIVE_EXECUTION_MISMATCH')
    kind = raw['type']
    trigger = raw['trigger']
    if kind not in {'market', 'limit'} or trigger not in {'immediate', 'stop'}:
        raise ValueError('BROKER_ORDER_TYPE_UNSUPPORTED')
    stop = number(raw['stop_price']) if raw.get('stop_price') is not None else None
    if trigger == 'stop' and (stop is None or stop <= 0):
        raise ValueError('BROKER_STOP_INVALID')
    created = timestamp(raw['created_at'])
    if created > timestamp(observed_at):
        raise ValueError('BROKER_FUTURE_ORDER')
    return {'id': order_id, 'symbol': symbol, 'side': side, 'state': raw['state'],
            'type': 'stop_' + kind if trigger == 'stop' else kind, 'quantity': str(qty) if qty is not None else None,
            'filled': str(cumulative), 'stop': str(stop) if stop is not None else None,
            'remaining': str(qty-cumulative) if qty is not None else None,
            'limit_price': str(number(raw['price'])) if raw.get('price') is not None else None,
            'market_hours': raw['market_hours'], 'time_in_force': raw['time_in_force'],
            'average_fill_price': str(sum((number(e['price'])*number(e['quantity']) for e in executions),Decimal(0))/cumulative) if cumulative else None,
            'created_at': created.isoformat(), 'ref_id': raw.get('ref_id'), 'executions': executions}


def collect_broker(transport, *, account_suffix, clock=utc_now, approval_ids=None):
    if not isinstance(account_suffix, str) or not re.fullmatch(r'\d{4}', account_suffix):
        raise ValueError('Require explicitly selected four-digit account suffix')
    tracked=[] if approval_ids is None else approval_ids
    if (not isinstance(tracked,list) or len(tracked)>1000
            or any(not isinstance(v,str) or not 0<len(v)<=200 for v in tracked)
            or len(set(tracked))!=len(tracked)):
        raise ValueError('INVALID_TRACKED_APPROVAL_IDS')
    started = clock().isoformat()
    accounts = rows(unwrap_response(transport('get_accounts', {})), 'accounts')
    matches = [a for a in accounts if a['account_number'].endswith(account_suffix)]
    if len(matches) != 1 or matches[0].get('agentic_allowed') is not True or matches[0].get('deactivated') is not False:
        raise ValueError('TARGET_ACCOUNT_NOT_UNIQUELY_ACCESSIBLE')
    account = matches[0]
    if account.get('state') != 'active' or account.get('permanently_deactivated') is not False:
        raise ValueError('TARGET_ACCOUNT_NOT_ACTIVE')
    transport.bind(account['account_number'], account.get('rhs_account_number'))
    args = {'account_number': account['account_number']}
    before = normalize_positions(fetch_pages(transport, 'get_equity_positions', args, 'positions'))
    raw_orders = fetch_pages(transport, 'get_equity_orders', args, 'orders')
    groups = fetch_pages(transport, 'get_advanced_orders', args, 'advanced_orders')
    for group in groups:
        if group.get('account_number') != account['account_number']:
            raise ValueError('ADVANCED_ORDER_ACCOUNT_MISMATCH')
        links = group.get('advanced_order_links')
        if not isinstance(links, list) or not links:
            raise ValueError('ADVANCED_ORDER_LEGS_MISSING')
        for link in links:
            leg = link.get('order')
            if leg is None:
                oid = link.get('order_id')
                if not oid:
                    raise ValueError('ADVANCED_ORDER_LEG_ID_MISSING')
                detail = rows(unwrap_response(transport('get_equity_orders', {**args, 'order_id': oid})), 'orders')
                if len(detail) != 1 or detail[0]['id'] != oid:
                    raise ValueError('ADVANCED_ORDER_LEG_UNRESOLVED')
                leg = detail[0]
            if link.get('order_id') and leg['id'] != link['order_id']:
                raise ValueError('ADVANCED_ORDER_LEG_ID_MISMATCH')
            raw_orders.append(leg)
    options = fetch_pages(transport, 'get_option_orders', args, 'orders')
    if not account.get('rhs_account_number'):
        raise ValueError('CRYPTO_ACCOUNT_SCOPE_UNAVAILABLE')
    crypto = fetch_pages(transport, 'get_crypto_orders', {'rhs_account_number': account['rhs_account_number']}, 'results')
    portfolio = unwrap_response(transport('get_portfolio', args))
    approval = unwrap_response(transport('get_trade_approval_setting', args))['setting']
    pending_approvals = fetch_pages(transport, 'get_trade_approvals', args, 'approvals')
    pending_ids=[]
    for raw in pending_approvals:
        item=normalize_approval(raw,account['account_number'])
        if item['state']!='APPROVAL_STATE_PENDING' or item['approval_id'] in pending_ids:
            raise ValueError('BROKER_PENDING_APPROVAL_LIST_INVALID')
        pending_ids.append(item['approval_id'])
    approval_states={}
    for offset in range(0,len(tracked),100):
        batch=tracked[offset:offset+100]
        data=unwrap_response(transport('get_trade_approvals',{**args,'approval_ids':batch}))
        if data.get('next'):raise ValueError('BROKER_EXACT_APPROVAL_QUERY_PAGINATED')
        for raw in rows(data,'approvals'):
            item=normalize_approval(raw,account['account_number']);identity=item['approval_id']
            if identity not in batch or identity in approval_states:
                raise ValueError('BROKER_EXACT_APPROVAL_RESPONSE_MISMATCH')
            approval_states[identity]=item
    if approval.get('account_number') != account['account_number'] or type(approval.get('human_must_approve_trades')) is not bool:
        raise ValueError('BROKER_APPROVAL_SCOPE_INVALID')
    after = normalize_positions(fetch_pages(transport, 'get_equity_positions', args, 'positions'))
    if before != after:
        raise ValueError('UNSTABLE_ACCOUNT_SNAPSHOT')
    at = clock().isoformat()
    if not 0 <= (timestamp(at)-timestamp(started)).total_seconds() <= 30:
        raise ValueError('BROKER_COLLECTION_WINDOW_EXCEEDED')
    orders = {}
    for raw in raw_orders:
        order = normalize_order(raw, at)
        if order['id'] in orders and orders[order['id']] != order:
            raise ValueError('UNSTABLE_ORDER_SNAPSHOT')
        orders[order['id']] = order
    if portfolio.get('currency') != 'USD' or portfolio['buying_power'].get('display_currency') != 'USD':
        raise ValueError('BROKER_CURRENCY_UNSUPPORTED')
    other_values = {k: str(number(portfolio[k])) for k in ('options_value', 'crypto_value', 'futures_value',
                   'event_contracts_value', 'fixed_income_value', 'mutual_funds_value')}
    return {'schema_version': 1, 'provider': 'robinhood', 'synthetic': False,
            'account_key': hashlib.sha256(account['account_number'].encode()).hexdigest(),
            'account_mask': '••••' + account_suffix, 'started_at': started, 'observed_at': at,
            'positions': after, 'orders': orders,
            'groups': [{'id':g['id'], 'state':g['state'], 'kind':g['contingency_type'],
                        'leg_ids':[l['order']['id'] if l.get('order') else l['order_id'] for l in g['advanced_order_links']]} for g in groups],
            'other_asset_open_orders': sum(o['state'] not in TERMINAL for o in options + crypto),
            'other_asset_values': other_values,
            'portfolio': {k:str(number(portfolio[k])) for k in ('cash','total_value','pending_deposits')},
            'buying_power': str(number(portfolio['buying_power']['buying_power'])),
            'unleveraged_buying_power': str(number(portfolio['buying_power']['unleveraged_buying_power'])),
            'human_must_approve_trades': approval['human_must_approve_trades'],
            'pending_approvals': len(pending_approvals),
            'pending_approval_ids':pending_ids,'approval_states':approval_states,
            'requested_approval_ids':tracked,'missing_approval_ids':[v for v in tracked if v not in approval_states],
            'collection_complete': True}


class BrokerJournal:
    def __init__(self, path, allow_synthetic=False):
        # A synthetic session still needs durable observation evidence, but it
        # must never be usable as real broker proof. The allowance is explicit
        # and only the simulation composition turns it on; the default journal
        # refuses a synthetic payload exactly as before.
        self.allow_synthetic = allow_synthetic is True
        self.db = sqlite3.connect(Path(path), timeout=10)
        self.db.executescript('''CREATE TABLE IF NOT EXISTS broker_observations(
            digest TEXT PRIMARY KEY, payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS broker_executions(id TEXT PRIMARY KEY, payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS broker_current(id INTEGER PRIMARY KEY CHECK(id=1), payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS broker_health(id INTEGER PRIMARY KEY CHECK(id=1), payload TEXT NOT NULL);''')
        self.audit=AuditTrail(connection=self.db)

    def close(self):
        self.db.close()

    def ingest(self, snapshot):
        complete = (snapshot.get('provider') == 'robinhood'
                    and snapshot.get('collection_complete') is True)
        synthetic = snapshot.get('synthetic') is True
        if not complete or not (snapshot.get('synthetic') is False
                                or (self.allow_synthetic and synthetic)):
            raise ValueError('Require complete explicit real broker observation')
        self.db.execute('BEGIN IMMEDIATE')
        try:
            self.audit.verify()
            old = self.db.execute('SELECT payload FROM broker_current WHERE id=1').fetchone()
            prior = json.loads(old[0]) if old else None
            if prior and prior['account_key'] != snapshot['account_key']:
                raise ValueError('Broker account binding changed')
            if prior and timestamp(snapshot['observed_at']) < timestamp(prior['observed_at']):
                raise TransportError('BROKER_SNAPSHOT_SUPERSEDED')
            issues = list(prior.get('reconciliation_issues', [])) if prior else []
            delta = {}
            cash_delta = Decimal(0)
            new_count = 0
            for order in snapshot['orders'].values():
                if not prior or prior['orders'].get(order['id'])!=order:
                    self.audit.append_in_transaction({'event_id':'order-'+audit_digest([snapshot['observed_at'],order]),
                        'occurred_at':snapshot['observed_at'],'event_type':'BROKER_ORDER_OBSERVED',
                        'actor':'broker','synthetic':False,'account':snapshot['account_mask'],
                        'broker_order_id':order['id'],'symbol':order['symbol'],'side':order['side'],
                        'details':order})
                if prior and order['id'] in prior['orders']:
                    prev = prior['orders'][order['id']]
                    if number(order['filled']) < number(prev['filled']) or prev['state'] in TERMINAL and order['state'] not in TERMINAL:
                        issues.append('BROKER_ORDER_REGRESSION')
                for fill in order['executions']:
                    encoded = json.dumps(fill, sort_keys=True)
                    stored = self.db.execute('SELECT payload FROM broker_executions WHERE id=?', (fill['id'],)).fetchone()
                    if stored:
                        if stored[0] != encoded:
                            issues.append('EXECUTION_ID_CONFLICT')
                        continue
                    self.db.execute('INSERT INTO broker_executions VALUES(?,?)', (fill['id'], encoded))
                    self.audit.append_in_transaction({'event_id':'execution-'+fill['id'],
                        'occurred_at':fill['executed_at'],'event_type':'BROKER_EXECUTION_OBSERVED',
                        'actor':'broker','synthetic':False,'account':snapshot['account_mask'],
                        'broker_order_id':order['id'],'symbol':fill['symbol'],'side':fill['side'],
                        'quantity':fill['quantity'],'price':fill['price'],'details':fill})
                    new_count += 1
                    delta[fill['symbol']] = delta.get(fill['symbol'], Decimal(0)) + number(fill['quantity']) * (1 if fill['side']=='buy' else -1)
                    cash_delta += number(fill['quantity'])*number(fill['price'])*(1 if fill['side']=='sell' else -1)-number(fill['fees'])
            if prior:
                if abs(number(snapshot['portfolio']['cash'])-number(prior['portfolio']['cash'])-cash_delta) > Decimal('0.01'):
                    issues.append('UNEXPLAINED_CASH_FLOW')
                for oid, order in prior['orders'].items():
                    if order['state'] not in TERMINAL and oid not in snapshot['orders']:
                        issues.append('OPEN_ORDER_DISAPPEARED')
                for symbol in set(prior['positions']) | set(snapshot['positions']) | set(delta):
                    before = number(prior['positions'].get(symbol, {}).get('quantity','0'))
                    after = number(snapshot['positions'].get(symbol, {}).get('quantity','0'))
                    if after - before != delta.get(symbol, Decimal(0)):
                        issues.append('POSITION_EXECUTION_DELTA_MISMATCH')
            payload = {**snapshot, 'reconciliation_issues': sorted(set(issues)),
                       'new_executions_seen': new_count, 'baseline_established': prior is None}
            encoded = json.dumps(snapshot, sort_keys=True)
            digest = hashlib.sha256(encoded.encode()).hexdigest()
            self.audit.append_in_transaction({'event_id':'account-'+digest,'occurred_at':snapshot['observed_at'],
                'event_type':'ACCOUNT_OBSERVED','actor':'broker','synthetic':False,'account':snapshot['account_mask'],
                'details':{'portfolio':snapshot['portfolio'],'positions':snapshot['positions'],
                           'open_orders':[o['id'] for o in snapshot['orders'].values() if o['state'] not in TERMINAL],
                           'reconciliation_issues':payload['reconciliation_issues'],'source_sha256':digest}})
            if set(issues)-set(prior.get('reconciliation_issues',[]) if prior else []):
                self.audit.append_in_transaction({'event_id':'reconciliation-'+digest,
                    'occurred_at':snapshot['observed_at'],'event_type':'RECONCILIATION_ISSUE',
                    'actor':'script','synthetic':False,'account':snapshot['account_mask'],
                    'details':{'issues':sorted(set(issues))}})
            self.db.execute('INSERT OR IGNORE INTO broker_observations VALUES(?,?)', (digest,encoded))
            self.db.execute('INSERT INTO broker_current VALUES(1,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload', (json.dumps(payload,sort_keys=True),))
            self.db.execute('INSERT INTO broker_health VALUES(1,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload',
                            (json.dumps({'healthy':True,'at':snapshot['observed_at']}),))
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return self.summary(as_of=snapshot['observed_at'])

    def failure(self, *, at, reason):
        with self.db:
            self.audit.verify()
            self.db.execute('INSERT INTO broker_health VALUES(1,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload',
                            (json.dumps({'healthy':False,'at':at,'reason':reason}),))
            self.audit.append_in_transaction({'event_id':'source-'+audit_digest([at,reason]),
                'occurred_at':at,'event_type':'BROKER_SOURCE_STATE','actor':'script','synthetic':False,
                'reason':reason,'details':{'healthy':False}})

    def current_observation(self):
        """Owner-read current payload for publication association.

        This is the actual durable journal row, not a caller self-report.  It
        remains a raw observation payload; it is not an entry authority.
        """
        self.audit.verify()
        row = self.db.execute('SELECT payload FROM broker_current WHERE id=1').fetchone()
        if row is None:
            return None
        value = json.loads(row[0])
        if not isinstance(value, dict):
            raise ValueError('BROKER_CURRENT_OBSERVATION_INVALID')
        return value

    def summary(self, *, as_of, max_age_seconds=30):
        self.audit.verify()
        at = timestamp(as_of)
        row = self.db.execute('SELECT payload FROM broker_current WHERE id=1').fetchone()
        health_row = self.db.execute('SELECT payload FROM broker_health WHERE id=1').fetchone()
        health = json.loads(health_row[0]) if health_row else {'healthy':False}
        if not row:
            return {'mode':'broker_observation','read_healthy':False,'new_risk_blocked':True,
                    'blockers':['NO_VERIFIED_BROKER_OBSERVATION'], 'execution_ready':False,'live_orders':0}
        state = json.loads(row[0])
        blockers = list(state['reconciliation_issues'])
        age = (at-timestamp(state['observed_at'])).total_seconds()
        if not 0 <= age <= max_age_seconds or health['healthy'] is not True:
            blockers.append('BROKER_OBSERVATION_STALE_OR_STOPPED')
        if state['other_asset_open_orders'] or any(number(v)!=0 for v in state['other_asset_values'].values()):
            blockers.append('OTHER_ASSET_EXPOSURE_OR_ORDERS')
        if state.get('pending_approvals',0):
            blockers.append('PENDING_APPROVAL_REQUIRES_RECONCILIATION')
        if any(number(state[k])<0 for k in ('buying_power','unleveraged_buying_power')):
            blockers.append('NEGATIVE_BUYING_POWER')
        if number(state['portfolio']['total_value']) <= 0:
            blockers.append('INVALID_ACCOUNT_VALUE')
        coverage = {}
        open_orders = [o for o in state['orders'].values() if o['state'] not in TERMINAL]
        excluded_legs = set()
        for group in state['groups']:
            if group['state'] not in TERMINAL and (group['kind'] != 'oco' or group['state'] != 'confirmed'):
                blockers.append('ADVANCED_GROUP_REQUIRES_RECONCILIATION')
                excluded_legs.update(group['leg_ids'])
        if any(o['state'] not in KNOWN for o in open_orders):
            blockers.append('UNKNOWN_BROKER_ORDER_STATE')
        for symbol, p in state['positions'].items():
            qty = number(p['quantity'])
            if qty < 0 or qty != qty.to_integral_value():
                blockers.append('UNSUPPORTED_POSITION:' + symbol)
            protected = sum((number(o['remaining']) for o in open_orders if o['symbol']==symbol and o['side']=='sell'
                and o['type']=='stop_market' and o['state']=='confirmed' and number(o['filled'])==0
                and o['remaining'] is not None and valid_session_order(o,at)
                and o['id'] not in excluded_legs),Decimal(0)) if is_regular(as_of) and 0 <= age <= max_age_seconds and health['healthy'] else Decimal(0)
            if protected > max(qty,0):
                blockers.append('EXCESS_PROTECTION_QUANTITY:' + symbol)
            uncovered = max(qty-protected,0)
            if uncovered:
                blockers.append('UNPROTECTED_POSITION:' + symbol)
            coverage[symbol] = {'quantity':str(qty),'observed_stop_coverage':str(min(max(qty,0),protected)),
                                'unverified_protection_quantity':str(uncovered)}
        if any(o['side']=='buy' for o in open_orders):
            blockers.append('OPEN_ENTRY_ORDER_REQUIRES_RECONCILIATION')
        for o in open_orders:
            if o['side']=='sell' and (o['remaining'] is None or number(o['remaining']) > max(number(state['positions'].get(o['symbol'],{}).get('quantity','0')),0)):
                blockers.append('EXIT_ORDER_POSITION_MISMATCH')
        return {'mode':'broker_observation','synthetic':False,'account':state['account_mask'],'as_of':as_of,
                'last_observed_at':state['observed_at'],'read_healthy':health['healthy'] and 0 <= age <= max_age_seconds,
                'portfolio':state['portfolio'],'buying_power':state['buying_power'],
                'unleveraged_buying_power':state['unleveraged_buying_power'],
                'human_must_approve_trades_at_observation':state['human_must_approve_trades'],
                'positions':coverage,'equity_orders_seen':len(state['orders']), 'open_equity_orders':len(open_orders),
                'advanced_groups_seen':len(state['groups']),'other_asset_open_orders':state['other_asset_open_orders'],
                'pending_approvals':state.get('pending_approvals',0),
                'executions_seen':self.db.execute('SELECT COUNT(*) FROM broker_executions').fetchone()[0],
                'new_executions_seen':state['new_executions_seen'],'reconciliation_issues':state['reconciliation_issues'],
                'blockers':sorted(set(blockers)),'new_risk_blocked':bool(blockers),'execution_ready':False,
                'actual_fill_protection_latency_verified':False,'live_orders':0}


def observe_broker(db_path, *, account_suffix, cycles=1, poll_seconds=5, transport_factory=None, clock=utc_now, sleeper=time.sleep):
    if type(cycles) is not int or not 1 <= cycles <= 1000 or not 1 <= poll_seconds <= 60:
        raise ValueError('Invalid broker observation cycle limits')
    journal = BrokerJournal(db_path)
    summaries = []
    try:
        with (transport_factory or CodexBrokerReadTransport)() as transport:
            for i in range(cycles):
                summaries.append(journal.ingest(collect_broker(transport, account_suffix=account_suffix, clock=clock)))
                if i+1 < cycles:
                    sleeper(poll_seconds)
        journal.failure(at=clock().isoformat(),reason='BOUNDED_OBSERVER_STOPPED')
        return {'mode':'broker_observation','cycles_completed':len(summaries),'observations':summaries,
                'current':journal.summary(as_of=clock().isoformat()),'continuous_monitor_running':False,
                'execution_ready':False,'live_orders':0}
    except (Exception, KeyboardInterrupt):
        journal.failure(at=clock().isoformat(),reason='BROKER_READ_FAILED_OR_INTERRUPTED')
        raise
    finally:
        journal.close()
