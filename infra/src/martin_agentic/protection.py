"""Persistent protection rehearsal from explicit simulated broker facts.

This journal never calls a broker. Approval, submission, acceptance and fills
are separate facts; no local intent is evidence of an effective live stop.
"""

from datetime import timedelta
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import sqlite3
from uuid import UUID, uuid5, NAMESPACE_URL

from .calendar import is_regular
from .ledger import timestamp
from .risk import number
from .audit import AuditTrail


RESERVED = frozenset({'AWAITING_APPROVAL', 'APPROVED', 'SUBMITTED', 'UNKNOWN',
                      'ACTIVE', 'PENDING_CANCEL', 'TRIGGERED'})
BROKER_STATES = frozenset({'ACTIVE', 'PENDING_CANCEL', 'TRIGGERED', 'CANCELLED',
                           'REJECTED', 'EXPIRED', 'FILLED', 'UNKNOWN'})
TERMINAL = frozenset({'CANCELLED', 'REJECTED', 'EXPIRED', 'FILLED'})


def whole(value):
    if type(value) is not int or value < 0:
        raise ValueError('Require nonnegative whole-share quantity')
    return value


class ProtectionJournal:
    def __init__(self, path):
        self.db = sqlite3.connect(Path(path), timeout=10)
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS protection_state(id INTEGER PRIMARY KEY CHECK(id=1), payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS protection_events(sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT UNIQUE NOT NULL, fingerprint TEXT NOT NULL, input TEXT NOT NULL, result TEXT NOT NULL);
        ''')
        self.audit=AuditTrail(connection=self.db)

    def close(self):
        self.db.close()

    def snapshot(self, *, as_of=None):
        row = self.db.execute('SELECT payload FROM protection_state WHERE id=1').fetchone()
        if row is None:
            raise ValueError('Protection journal is not initialized')
        state = json.loads(row[0])
        # This is a replay view unless the caller supplies an observation clock.
        return self._view(state, timestamp(as_of or state['at']))

    def apply(self, event):
        if not isinstance(event, dict) or not isinstance(event.get('id'), str) or not event['id']:
            raise ValueError('Require nonempty event id')
        at = timestamp(event['at'])
        raw = json.dumps(event, sort_keys=True, separators=(',', ':'))
        fingerprint = hashlib.sha256(raw.encode()).hexdigest()
        self.db.execute('BEGIN IMMEDIATE')
        try:
            self.audit.verify()
            prior = self.db.execute('SELECT fingerprint,result FROM protection_events WHERE event_id=?',
                                    (event['id'],)).fetchone()
            if prior:
                if prior[0] != fingerprint:
                    raise ValueError('Protection event id conflict')
                self.db.rollback()
                return json.loads(prior[1])
            row = self.db.execute('SELECT payload FROM protection_state WHERE id=1').fetchone()
            state = json.loads(row[0]) if row else None
            if state and at < timestamp(state['at']):
                raise ValueError('Out-of-order protection event')
            state = self._transition(state, event, at)
            state['at'] = at.isoformat()
            result = self._view(state, at)
            self.db.execute('INSERT INTO protection_state VALUES(1,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload',
                            (json.dumps(state, sort_keys=True),))
            self.db.execute('INSERT INTO protection_events(event_id,fingerprint,input,result) VALUES(?,?,?,?)',
                            (event['id'], fingerprint, raw, json.dumps(result, sort_keys=True)))
            self.audit.append_in_transaction({'event_id':'protection-'+event['id'],'occurred_at':event['at'],
                'event_type':'PROTECTION_REHEARSAL','actor':'simulation','synthetic':True,
                'intent_id':event.get('intent_id'),'details':{'input':event,'blockers':result['blockers']}})
            self.db.commit()
            return result
        except BaseException:
            self.db.rollback()
            raise

    @staticmethod
    def _transition(state, event, at):
        kind = event['kind']
        if state is None:
            if kind != 'init' or event.get('mode') != 'protection_rehearsal' or event.get('synthetic') is not True:
                raise ValueError('Explicit synthetic protection rehearsal initialization required')
            account = event.get('account_ref')
            if not isinstance(account, str) or not re.fullmatch(r'[A-Za-z][A-Za-z0-9_-]{0,63}', account):
                raise ValueError('Require a local account alias, never a full brokerage account number')
            wait, fresh = event['max_unprotected_seconds'], event['max_broker_age_seconds']
            if type(wait) is not int or type(fresh) is not int or not 0 < wait <= 3600 or not 0 < fresh <= 3600:
                raise ValueError('Explicit positive rehearsal latency limits required')
            return {'account_ref': account, 'mode': 'protection_rehearsal', 'synthetic': True,
                    'positions': {}, 'orders': {}, 'executions': {}, 'issues': [], 'position_checked_at': None,
                    'execution_revision': 0, 'checked_execution_revision': -1,
                    'max_unprotected_seconds': wait, 'max_broker_age_seconds': fresh}
        if event.get('account_ref') != state['account_ref']:
            raise ValueError('Account alias mismatch')
        if kind == 'init':
            raise ValueError('Cannot reset protection journal')

        def issue(reason):
            if reason not in state['issues']:
                state['issues'].append(reason)

        if kind == 'fill':
            execution_id = event['execution_id']
            if not isinstance(execution_id, str) or not execution_id:
                raise ValueError('Require execution identity')
            symbol, side, qty = event['symbol'], event['side'], whole(event['quantity'])
            price = number(event['price'])
            if not re.fullmatch(r'[A-Z][A-Z0-9.\-]{0,14}', symbol) or side not in {'buy', 'sell'} or qty == 0 or price <= 0:
                raise ValueError('Invalid execution')
            # Different observation events can contain the same execution. Dedup
            # by broker execution identity, independent of local event identity.
            fact = {key: event.get(key) for key in ('symbol', 'side', 'quantity', 'price', 'intent_id', 'initial_stop')}
            if execution_id in state['executions']:
                if state['executions'][execution_id] != fact:
                    issue('CONFLICTING_EXECUTION_IDENTITY')
                return state
            position = state['positions'].get(symbol)
            if side == 'buy':
                stop = number(event['initial_stop'])
                if not 0 < stop < price:
                    raise ValueError('Invalid initial stop for fill')
                if position and number(position['initial_stop']) != stop:
                    raise ValueError('Initial structure cannot change within this position')
                if not position:
                    position = {'quantity': 0, 'entry_price': '0', 'initial_stop': str(stop),
                                'first_fill_at': at.isoformat()}
                    state['positions'][symbol] = position
                prior = position['quantity']
                if prior < 0:
                    issue('UNEXPECTED_REVERSE_POSITION')
                total = prior + qty
                position['entry_price'] = str((number(position['entry_price']) * max(prior, 0) + price * qty) / total) if total > 0 else '0'
                position['quantity'] = total
            else:
                if position is None:
                    position = {'quantity': 0, 'entry_price': '0', 'initial_stop': '0',
                                'first_fill_at': at.isoformat()}
                    state['positions'][symbol] = position
                position['quantity'] -= qty
                intent_id = event.get('intent_id')
                order = state['orders'].get(intent_id)
                if not order or order['symbol'] != symbol:
                    issue('UNMATCHED_SELL_EXECUTION')
                else:
                    order['filled'] += qty
                    if order['filled'] > order['quantity']:
                        issue('ORDER_OVERFILLED')
                    # A fill invalidates the last acknowledged remaining quantity;
                    # require a fresh broker observation before trusting coverage.
                    order['status'] = 'FILLED' if order['filled'] >= order['quantity'] else 'UNKNOWN'
                if position['quantity'] < 0:
                    issue('UNEXPECTED_REVERSE_POSITION')
            state['executions'][execution_id] = fact
            state['execution_revision'] += 1
        elif kind == 'protect_intent':
            intent = str(UUID(event['intent_id']))
            if intent != event['intent_id'] or intent in state['orders']:
                raise ValueError('Require new canonical intent UUID')
            symbol, qty, stop = event['symbol'], whole(event['quantity']), number(event['stop'])
            p = state['positions'][symbol]
            reserved = sum(max(o['quantity'] - o['filled'], 0) for o in state['orders'].values()
                           if o['symbol'] == symbol and o['status'] in RESERVED)
            if qty <= 0 or qty > p['quantity'] - reserved or stop <= 0 or stop < number(p['initial_stop']):
                raise ValueError('Protection quantity reserved/excessive or stop widened')
            if state['issues']:
                raise ValueError('Reconciliation required before new intent')
            until = timestamp(event['valid_until'])
            if until <= at:
                raise ValueError('Expired protection intent')
            state['orders'][intent] = {'symbol': symbol, 'quantity': qty, 'filled': 0,
                'stop': str(stop), 'status': 'AWAITING_APPROVAL', 'broker_id': None,
                'broker_observed_at': None, 'valid_until': until.isoformat(),
                'intent_at': at.isoformat(), 'approved_at': None, 'submitted_at': None,
                'first_active_observed_at': None, 'cancel_requested_at': None,
                'ref_id': str(uuid5(NAMESPACE_URL, state['account_ref'] + '/' + intent))}
        elif kind in {'approve', 'submit', 'submit_unknown', 'cancel_request', 'broker_order'}:
            order = state['orders'][event['intent_id']]
            if kind == 'approve':
                if order['status'] != 'AWAITING_APPROVAL' or event.get('approved') is not True:
                    raise ValueError('Require explicit approval for pending intent')
                if at >= timestamp(order['valid_until']):
                    raise ValueError('Approval expired')
                order.update(status='APPROVED', approved_at=at.isoformat())
            elif kind == 'submit':
                if order['status'] != 'APPROVED':
                    raise ValueError('Submission requires approval')
                if at >= timestamp(order['valid_until']) or not is_regular(at.isoformat()):
                    raise ValueError('Submission outside allowed time window')
                order.update(status='SUBMITTED', submitted_at=at.isoformat())
            elif kind == 'submit_unknown':
                if order['status'] != 'SUBMITTED':
                    raise ValueError('No submitted request to mark unknown')
                order['status'] = 'UNKNOWN'
            elif kind == 'cancel_request':
                if order['status'] not in {'ACTIVE', 'UNKNOWN', 'TRIGGERED'}:
                    raise ValueError('Cannot cancel this order state')
                order.update(status='PENDING_CANCEL', cancel_requested_at=at.isoformat())
            else:
                status = event['status']
                if status not in BROKER_STATES or order['status'] in {'AWAITING_APPROVAL', 'APPROVED'}:
                    raise ValueError('Invalid broker order transition')
                if order['status'] in TERMINAL and status != order['status']:
                    issue('TERMINAL_ORDER_REGRESSION')
                broker_id = event['broker_id']
                if not isinstance(broker_id, str) or not broker_id:
                    raise ValueError('Require broker order identity')
                if order['broker_id'] and order['broker_id'] != broker_id:
                    issue('BROKER_ORDER_ID_CHANGED')
                if any(o is not order and o['broker_id'] == broker_id for o in state['orders'].values()):
                    issue('BROKER_ORDER_ID_REUSED')
                if whole(event['filled_quantity']) != order['filled']:
                    issue('MISSING_OR_CONFLICTING_EXECUTIONS')
                    status = 'UNKNOWN'
                if status == 'FILLED' and order['filled'] != order['quantity']:
                    issue('MISSING_OR_CONFLICTING_EXECUTIONS')
                    status = 'UNKNOWN'
                if number(event['stop']) != number(order['stop']) or whole(event['quantity']) != order['quantity']:
                    issue('BROKER_ORDER_TERMS_CHANGED')
                    status = 'UNKNOWN'
                if (event.get('symbol') != order['symbol'] or event.get('side') != 'sell'
                        or event.get('order_type') != 'stop_market' or event.get('market_hours') != 'regular_hours'
                        or timestamp(event['valid_until']) != timestamp(order['valid_until'])):
                    issue('BROKER_ORDER_TERMS_CHANGED')
                    status = 'UNKNOWN'
                order.update(status=status, broker_id=broker_id, broker_observed_at=at.isoformat())
                if status == 'ACTIVE' and order['first_active_observed_at'] is None:
                    order['first_active_observed_at'] = at.isoformat()
        elif kind == 'position_check':
            # Snapshot discrepancies latch a halt; never overwrite execution facts.
            observed = event['positions']
            if not isinstance(observed, dict) or any(type(v) is not int for v in observed.values()):
                raise ValueError('Require signed whole-share position snapshot')
            expected = {s: p['quantity'] for s, p in state['positions'].items() if p['quantity']}
            if {s: q for s, q in observed.items() if q} != expected:
                issue('POSITION_SNAPSHOT_MISMATCH')
            else:
                state['position_checked_at'] = at.isoformat()
                state['checked_execution_revision'] = state['execution_revision']
        elif kind != 'timer':
            raise ValueError('Unsupported protection rehearsal event')
        return state

    @staticmethod
    def _view(state, at):
        if at < timestamp(state['at']):
            raise ValueError('View clock cannot precede last observation')
        positions = {}
        blockers = list(state['issues'])
        checked = state.get('position_checked_at')
        account_checked = bool(checked and state['checked_execution_revision'] == state['execution_revision']
            and (at - timestamp(checked)).total_seconds() <= state['max_broker_age_seconds'])
        if not account_checked:
            blockers.append('ACCOUNT_POSITION_CHECK_REQUIRED')
        active_residual = False
        for symbol, p in state['positions'].items():
            qty, verified, reserved = p['quantity'], 0, 0
            for o in state['orders'].values():
                if o['symbol'] != symbol:
                    continue
                remaining = max(o['quantity'] - o['filled'], 0)
                if o['status'] in RESERVED:
                    reserved += remaining
                if o['status'] in {'ACTIVE', 'PENDING_CANCEL'} and o['broker_observed_at']:
                    age = (at - timestamp(o['broker_observed_at'])).total_seconds()
                    if 0 <= age <= state['max_broker_age_seconds'] and at < timestamp(o['valid_until']) and is_regular(at.isoformat()):
                        verified += remaining
            uncovered = max(qty - verified, 0)
            if qty <= 0 and reserved:
                active_residual = True
            if verified > max(qty, 0) or reserved > max(qty, 0):
                blockers.append('EXCESS_EXIT_QUANTITY:' + symbol)
            if uncovered:
                blockers.append('UNPROTECTED_QUANTITY:' + symbol)
                # Upper-bound exposure since first fill; later protection gaps do
                # not reset this clock. Precise gap-interval metrics are separate.
                if (at - timestamp(p['first_fill_at'])).total_seconds() > state['max_unprotected_seconds']:
                    blockers.append('PROTECTION_DEADLINE_EXCEEDED:' + symbol)
            label = 'RECONCILIATION_REQUIRED' if state['issues'] or qty < 0 else (
                'NEEDS_PROTECTION' if uncovered else 'PROTECTED' if qty else
                'EXIT_ORDERS_REMAIN' if reserved else 'FLAT_VERIFIED' if account_checked else 'FLAT_UNVERIFIED')
            positions[symbol] = {**p, 'verified_protected_quantity': min(verified, max(qty, 0)),
                'initial_r_per_share': str(number(p['entry_price']) - number(p['initial_stop'])) if qty > 0 else None,
                'unprotected_quantity': uncovered, 'reserved_exit_quantity': reserved, 'protection_state': label}
        if active_residual:
            blockers.append('EXIT_ORDERS_REMAIN')
        return {'mode': 'protection_rehearsal', 'synthetic': True, 'as_of': at.isoformat(),
                'account_ref': state['account_ref'], 'positions': positions, 'orders': state['orders'],
                'reconciliation_issues': state['issues'], 'blockers': sorted(set(blockers)),
                'new_risk_blocked': bool(blockers), 'execution_ready': False, 'live_orders': 0,
                'clock_basis': 'explicit-observation-or-replay', 'broker_protection_verified': False}
