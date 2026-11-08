"""Read-only actual-protection evidence from owner execution and broker records.

This module never calls a broker tool and never changes an action, trade, risk
state, or broker order.  It distinguishes structural source records from actual
protection coverage and timing.  A submitted native place is not a fill; an app
approval is not an active broker order; a local elapsed/declared limit is not
proof of coverage.  Only fresh owner broker observations establish quantity and
timing.
"""
from datetime import datetime, timezone
from decimal import Decimal
import json
import sqlite3

from .audit import digest
from .broker import TERMINAL
from .ledger import timestamp
from .risk import number


SCHEMA_VERSION = 1
MAX_OBSERVATION_SECONDS = 30
PROTECTION_TIMING_BUDGET_SECONDS = 30
ACTIVE_PROTECTION_STATES = frozenset({'confirmed', 'partially_filled'})
ACTIVE_ACTION_STATES = frozenset({'READY', 'RUNNING', 'UNKNOWN', 'WAITING_USER',
                                  'REPORTED', 'OBSERVED'})


class ProtectionEvidenceReader:
    def __init__(self, book, *, clock=None, max_observation_seconds=30):
        self.book = book
        self.clock = clock or getattr(book, 'clock', None) or (
            lambda: datetime.now(timezone.utc))
        if (type(max_observation_seconds) is not int
                or max_observation_seconds <= 0):
            raise ValueError('PROTECTION_OBSERVATION_LIMIT_INVALID')
        if max_observation_seconds > MAX_OBSERVATION_SECONDS:
            raise ValueError('PROTECTION_OBSERVATION_LIMIT_EXCEEDED')
        self.max_observation_seconds = max_observation_seconds

    @staticmethod
    def _json(value):
        if isinstance(value, dict):
            return value
        if not isinstance(value, str):
            return None
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return None
        return parsed if isinstance(parsed, dict) else None

    @staticmethod
    def _layer(status, reason=None, **extra):
        result = {'status': status, 'reason': reason}
        result.update(extra)
        return result

    def _result(self, *, schema_valid, reasons, layers, observed, binding,
                evidence_proven=False, capability_proven=False,
                qualification=None):
        return {
            'schema_version': SCHEMA_VERSION,
            'kind': 'protection_actual_evidence_v1',
            'schema_valid': schema_valid,
            'observed': observed,
            'evidence_proven': evidence_proven,
            'capability_proven': capability_proven,
            'qualification': qualification,
            'reasons': sorted(set(reasons)),
            'limits': {
                'observation_seconds': self.max_observation_seconds,
                'protection_timing_seconds': PROTECTION_TIMING_BUDGET_SECONDS,
            },
            'layers': layers,
            'binding': binding,
            'no_broker_call': True,
            'no_state_change': True,
        }

    def _book_state(self):
        try:
            state = self.book.state()
        except Exception:
            return None
        return state if isinstance(state, dict) else None

    def _action(self, action_id):
        try:
            return next((item for item in self.book.actions()
                         if item.get('id') == action_id), None)
        except Exception:
            return None

    def _native(self, action_id):
        try:
            rows = self.book.db.execute(
                'SELECT id,action_id,operation,state,payload,result,created_at '
                'FROM native_operation_records WHERE action_id=? ORDER BY rowid',
                (action_id,)).fetchall()
        except sqlite3.Error:
            return None
        return [dict(zip(('id', 'action_id', 'operation', 'state', 'payload',
                          'result', 'created_at'), row)) for row in rows]

    def _broker_current(self):
        try:
            row = self.book.db.execute(
                'SELECT payload FROM broker_current WHERE id=1').fetchone()
        except sqlite3.Error:
            return None
        return self._json(row[0]) if row else None

    def _entry_first_fill(self, trade, symbol, first_fill, current):
        """Bind first_fill to this trade's entry action/order executions."""
        entry_action = self._action(trade.get('entry_action'))
        if (not entry_action or entry_action.get('kind') != 'entry'
                or entry_action.get('trade_id') != trade.get('id')):
            return {'proven': False, 'reason': 'PROTECTION_ENTRY_ACTION_UNPROVEN'}
        payload = self._json(entry_action.get('payload')) or {}
        ref_id = payload.get('ref_id')
        params = payload.get('parameters')
        if (not isinstance(ref_id, str) or not ref_id
                or not isinstance(params, dict)
                or params.get('symbol') != symbol
                or params.get('side') != 'buy'):
            return {'proven': False, 'reason': 'PROTECTION_ENTRY_ACTION_UNPROVEN'}
        account_key = self._book_state().get('account_key')
        order_ids = set()
        try:
            owners = self.book.db.execute(
                'SELECT order_id,account_key FROM external_broker_order_ids '
                'WHERE action_id=?', (entry_action['id'],)).fetchall()
        except sqlite3.Error:
            owners = []
        for order_id, owner_account in owners:
            if owner_account != account_key:
                return {'proven': False,
                        'reason': 'PROTECTION_ENTRY_ORDER_ACCOUNT_MISMATCH'}
            if isinstance(order_id, str) and order_id:
                order_ids.add(order_id)
        if not order_ids and isinstance(current, dict):
            for order_id, order in (current.get('orders') or {}).items():
                if (isinstance(order, dict)
                        and order.get('ref_id') == ref_id
                        and order.get('symbol') == symbol
                        and order.get('side') == 'buy'
                        and str(order.get('quantity')) == str(params.get('quantity'))):
                    order_ids.add(order_id)
        if not order_ids:
            return {'proven': False, 'reason': 'PROTECTION_ENTRY_ORDER_OWNERSHIP_UNPROVEN'}
        try:
            rows = self.book.db.execute(
                "SELECT payload FROM broker_executions "
                "WHERE json_extract(payload,'$.executed_at')=? "
                "AND json_extract(payload,'$.symbol')=? "
                "AND json_extract(payload,'$.side')='buy'",
                (first_fill, symbol)).fetchall()
        except sqlite3.Error:
            rows = []
        selected = []
        for row in rows:
            fill = self._json(row[0])
            if isinstance(fill, dict) and fill.get('order_id') in order_ids:
                selected.append(fill)
        if not selected:
            return {'proven': False, 'reason': 'PROTECTION_FIRST_FILL_UNPROVEN'}
        observations = [current] if isinstance(current, dict) else []
        for observed_at in (first_fill, trade.get('first_protection_observed_at')):
            if isinstance(observed_at, str):
                item = self._observation_at(observed_at)
                if isinstance(item, dict):
                    observations.append(item)
        for fill in selected:
            order = None
            for observation in observations:
                candidate = (observation.get('orders') or {}).get(fill['order_id'])
                if isinstance(candidate, dict):
                    order = candidate
                    break
            if order is None:
                return {'proven': False,
                        'reason': 'PROTECTION_ENTRY_ORDER_OBSERVATION_MISSING'}
            if (order.get('symbol') != symbol or order.get('side') != 'buy'
                    or str(order.get('quantity')) != str(params.get('quantity'))):
                return {'proven': False,
                        'reason': 'PROTECTION_ENTRY_ORDER_TERMS_MISMATCH'}
            executions = order.get('executions')
            if not isinstance(executions, list):
                return {'proven': False,
                        'reason': 'PROTECTION_ENTRY_EXECUTIONS_MISSING'}
            matched = [execution for execution in executions
                       if isinstance(execution, dict)
                       and execution.get('id') == fill.get('id')
                       and execution.get('order_id') == fill.get('order_id')
                       and execution.get('symbol') == symbol
                       and execution.get('side') == 'buy'
                       and execution.get('quantity') == fill.get('quantity')
                       and execution.get('executed_at') == fill.get('executed_at')]
            if len(matched) != 1:
                return {'proven': False,
                        'reason': 'PROTECTION_FIRST_FILL_EXECUTION_MISMATCH'}
        return {'proven': True, 'execution_ids': sorted(
                    str(fill.get('id')) for fill in selected),
                'order_ids': sorted(order_ids),
                'quantity': str(sum(number(fill.get('quantity'))
                                    for fill in selected))}

    def _observation_at(self, observed_at):
        try:
            rows = self.book.db.execute(
                "SELECT payload FROM broker_observations "
                "WHERE json_extract(payload,'$.observed_at')=? LIMIT 2",
                (observed_at,)).fetchall()
        except sqlite3.Error:
            return None
        values = [self._json(row[0]) for row in rows]
        values = [value for value in values if isinstance(value, dict)]
        if len(values) != 1:
            return None
        return values[0] if values[0].get('observed_at') == observed_at else None

    def _cancel_layers(self, trade_id, order_id):
        reasons = []
        layers = []
        try:
            actions = [a for a in self.book.actions()
                       if a.get('trade_id') == trade_id and a.get('kind') == 'cancel']
        except Exception:
            actions = []
        for action in actions:
            payload = self._json(action.get('payload')) or {}
            if payload.get('target_order_id') != order_id:
                continue
            try:
                ops = self.book.db.execute(
                    'SELECT operation_id,report FROM external_operations WHERE action_id=?',
                    (action['id'],)).fetchall()
            except sqlite3.Error:
                ops = []
            reports = [self._json(row[1]) for row in ops]
            status = action.get('state')
            if status in ACTIVE_ACTION_STATES and not reports:
                reasons.append('PROTECTION_CANCEL_PENDING')
            elif any((report or {}).get('status') in {
                    'cancel_requested', 'pending_approval', 'submitted', 'unknown'}
                     for report in reports):
                reasons.append('PROTECTION_CANCEL_PENDING')
            layers.append({'action_id': action['id'], 'state': status,
                           'reports': [r for r in reports if isinstance(r, dict)]})
        return layers, sorted(set(reasons))

    def assess_capability(self, *, plan=None, channel=None, protection=None):
        """Pre-entry capability is distinct from post-fill actual evidence."""
        layers = {}
        reasons = []
        tools = getattr(protection, 'protection_tool_identity', None)
        if tools is None:
            tools = getattr(channel, 'tool_identity', None)
        if tools is not None:
            tools = set(tools)
            if not {'review_advanced_order', 'place_advanced_order'}.issubset(tools):
                reasons.append('PROTECTION_TOOLS_UNPROVEN')
                layers['tool_contract'] = self._layer(
                    'unproven', 'PROTECTION_TOOLS_UNPROVEN')
            else:
                layers['tool_contract'] = self._layer(
                    'descriptive_only', 'OCO_DESCRIPTION_IS_NOT_ENTRY_PROOF',
                    tool_identity=sorted(tools))
        else:
            layers['tool_contract'] = self._layer(
                'unproven', 'PROTECTION_TOOLS_UNPROVEN')
            reasons.append('PROTECTION_TOOLS_UNPROVEN')
        # No current official tool/receipt proves a pre-entry protection path
        # for this exact future fill.  A past successful protect does not grant
        # the next order.
        layers['pre_entry'] = self._layer(
            'unproven', 'CHANNEL_PROTECTION_PATH_UNPROVEN',
            reason_codes=['NO_PRE_ENTRY_PROTECTION_RECEIPT',
                          'OCO_IS_EXIT_PROTECTION_NOT_ENTRY_BRACKET'])
        reasons.append('CHANNEL_PROTECTION_PATH_UNPROVEN')
        return self._result(
            schema_valid=True, reasons=reasons, layers=layers,
            observed={'plan': self._json(plan) if isinstance(plan, dict) else None},
            binding={}, evidence_proven=False, capability_proven=False,
            qualification='CHANNEL_PROTECTION_PATH_UNPROVEN')

    def assess_actual(self, *, trade_id, action_id=None):
        state = self._book_state()
        if state is None:
            return self._result(schema_valid=False,
                                reasons=['PROTECTION_BOOK_STATE_UNAVAILABLE'],
                                layers={}, observed={}, binding={})
        trade = (state.get('trades') or {}).get(trade_id)
        if not isinstance(trade, dict):
            return self._result(schema_valid=False,
                                reasons=['PROTECTION_TRADE_MISSING'],
                                layers={}, observed={}, binding={})
        try:
            actions = [a for a in self.book.actions()
                       if a.get('trade_id') == trade_id and a.get('kind') == 'protect']
        except Exception:
            actions = []
        if action_id is not None:
            action = self._action(action_id)
            if not action or action.get('kind') != 'protect':
                return self._result(schema_valid=False,
                                    reasons=['PROTECTION_ACTION_MISSING'],
                                    layers={}, observed={}, binding={})
        else:
            action = max(actions, key=lambda a: a.get('created_at') or '') if actions else None
        if action is None:
            return self._result(schema_valid=False,
                                reasons=['PROTECTION_ACTION_MISSING'],
                                layers={}, observed={}, binding={})
        payload = self._json(action.get('payload')) or {}
        params = payload.get('parameters')
        if action.get('trade_id') != trade_id or payload.get('trade_id') != trade_id:
            return self._result(schema_valid=False,
                                reasons=['PROTECTION_ACTION_TRADE_MISMATCH'],
                                layers={}, observed={}, binding={})
        if not isinstance(params, dict):
            return self._result(schema_valid=False,
                                reasons=['PROTECTION_PARAMETERS_INVALID'],
                                layers={}, observed={}, binding={})
        plan = trade.get('plan') if isinstance(trade.get('plan'), dict) else {}
        symbol = plan.get('symbol')
        stop_price = trade.get('stop')
        if (params.get('symbol') != symbol or params.get('side') != 'sell'
                or params.get('type') != 'stop_market'
                or not isinstance(params.get('quantity'), str)
                or not isinstance(params.get('stop_price'), str)):
            return self._result(schema_valid=False,
                                reasons=['PROTECTION_ACTION_TERMS_INVALID'],
                                layers={}, observed={}, binding={})
        try:
            action_qty = number(params['quantity'])
            action_stop = number(params['stop_price'])
            trade_stop = number(stop_price)
        except (TypeError, ValueError):
            return self._result(schema_valid=False,
                                reasons=['PROTECTION_CURRENT_STOP_INVALID'],
                                layers={}, observed={}, binding={})
        if action_qty <= 0 or action_stop <= 0 or trade_stop <= 0:
            return self._result(schema_valid=False,
                                reasons=['PROTECTION_ACTION_TERMS_INVALID'],
                                layers={}, observed={}, binding={})
        if action_stop < trade_stop:
            return self._result(
                schema_valid=True,
                reasons=['PROTECTION_ACTION_STOP_NOT_CURRENT'],
                layers={'action': self._layer(
                    'unproven', 'PROTECTION_ACTION_STOP_NOT_CURRENT',
                    action_stop=str(action_stop),
                    current_trade_stop=str(trade_stop))},
                observed={'trade_id': trade_id, 'action_id': action['id']},
                binding={'trade_id': trade_id, 'action_id': action['id'],
                         'current_trade_stop': str(trade_stop),
                         'action_stop': str(action_stop)})
        account_key = state.get('account_key')
        if not isinstance(account_key, str) or not account_key:
            return self._result(schema_valid=False,
                                reasons=['PROTECTION_ACCOUNT_UNBOUND'],
                                layers={}, observed={}, binding={})
        if payload.get('account') != '••••' + str(self.book.policy.get('account_suffix')):
            return self._result(schema_valid=False,
                                reasons=['PROTECTION_ACCOUNT_MISMATCH'],
                                layers={}, observed={}, binding={})
        rows = self._native(action['id'])
        if rows is None:
            return self._result(schema_valid=False,
                                reasons=['PROTECTION_NATIVE_LEDGER_UNAVAILABLE'],
                                layers={}, observed={}, binding={})
        review_rows = [r for r in rows if r['operation'] == 'review']
        place_rows = [r for r in rows if r['operation'] == 'place']
        review = review_rows[-1] if review_rows else None
        place = place_rows[-1] if place_rows else None
        reasons = []
        layers = {}
        if action.get('state') == 'UNKNOWN':
            reasons.append('PROTECTION_ACTION_UNKNOWN')
            layers['action'] = self._layer('unproven', 'PROTECTION_ACTION_UNKNOWN')
        elif action.get('state') not in ACTIVE_ACTION_STATES:
            reasons.append('PROTECTION_ACTION_STATE_INVALID')
            layers['action'] = self._layer(
                'unproven', 'PROTECTION_ACTION_STATE_INVALID',
                action_state=action.get('state'))
        else:
            layers['action'] = self._layer(
                'observed', None, action_state=action.get('state'))
        review_at = place_at = None
        binding = {
            'trade_id': trade_id, 'action_id': action['id'],
            'account_key': account_key, 'symbol': symbol,
            'quantity': str(action_qty), 'stop_price': str(action_stop),
        }
        if review is None or review['state'] != 'RECORDED':
            reasons.append('PROTECTION_REVIEW_NOT_VERIFIED')
            layers['review'] = self._layer('unproven', 'PROTECTION_REVIEW_NOT_VERIFIED')
        else:
            r_result = self._json(review['result']) or {}
            r_packet = self._json(review['payload']) or {}
            if (r_result.get('review_verified') is not True
                    or r_result.get('response_usable') is not True):
                reasons.append('PROTECTION_REVIEW_NOT_VERIFIED')
                layers['review'] = self._layer('unproven', 'PROTECTION_REVIEW_NOT_VERIFIED')
            elif (r_packet.get('action_id') != action['id']
                    or r_packet.get('operation') != 'review'
                    or r_packet.get('tool') != 'review_equity_order'
                    or r_packet.get('account_key') != account_key
                    or r_packet.get('parameters') != params):
                reasons.append('PROTECTION_REVIEW_BINDING_MISMATCH')
                layers['review'] = self._layer(
                    'unproven', 'PROTECTION_REVIEW_BINDING_MISMATCH')
            else:
                layers['review'] = self._layer(
                    'verified', None, operation_id=review['id'],
                    response_hash=r_result.get('response_hash'),
                    recorded_at=r_result.get('recorded_at'),
                    review_verified=True, order_checks=r_result.get('order_checks'),
                    customer_approval_required=r_result.get(
                        'customer_approval_required'))
                binding['review_operation_id'] = review['id']
                binding['review_response_hash'] = r_result.get('response_hash')
                review_at = r_result.get('recorded_at')
        order_id = None
        if place is None or place['state'] != 'RECORDED':
            reasons.append('PROTECTION_PLACE_NOT_RECORDED')
            layers['place'] = self._layer('unproven', 'PROTECTION_PLACE_NOT_RECORDED')
        else:
            p_result = self._json(place['result']) or {}
            p_packet = self._json(place['payload']) or {}
            report = p_result.get('report') if isinstance(p_result.get('report'), dict) else {}
            approval_state = p_result.get('approval_state')
            if (p_result.get('response_usable') is not True
                    or p_result.get('broker_effect_verified') is not False):
                reasons.append('PROTECTION_PLACE_RESPONSE_INVALID')
                layers['place'] = self._layer('unproven', 'PROTECTION_PLACE_RESPONSE_INVALID')
            elif approval_state is not None:
                if approval_state == 'APPROVAL_STATE_PENDING':
                    reasons.append('PROTECTION_APPROVAL_PENDING_NOT_ACTIVE')
                elif approval_state in {'APPROVAL_STATE_APPROVED',
                                        'APPROVAL_STATE_AUTO_APPROVED'}:
                    reasons.append('PROTECTION_APPROVAL_NOT_BROKER_ORDER')
                else:
                    reasons.append('PROTECTION_APPROVAL_TERMINAL')
                layers['place'] = self._layer(
                    'unproven', reasons[-1], approval_state=approval_state)
            elif (report.get('status') != 'submitted'
                  or not isinstance(p_result.get('order_id'), str)
                  or not p_result.get('order_id')):
                reasons.append('PROTECTION_PLACE_NOT_BROKER_CONFIRMED')
                layers['place'] = self._layer(
                    'unproven', 'PROTECTION_PLACE_NOT_BROKER_CONFIRMED',
                    report=report)
            else:
                order_id = p_result['order_id']
                if (p_packet.get('action_id') != action['id']
                        or p_packet.get('operation') != 'place'
                        or p_packet.get('tool') != 'place_equity_order'
                        or p_packet.get('account_key') != account_key
                        or p_packet.get('parameters', {}).get('symbol') != symbol
                        or p_packet.get('parameters', {}).get('quantity') != str(action_qty)
                        or p_packet.get('parameters', {}).get('stop_price') != str(action_stop)
                        or p_packet.get('parameters', {}).get('ref_id') != payload.get('ref_id')):
                    reasons.append('PROTECTION_PLACE_BINDING_MISMATCH')
                    layers['place'] = self._layer(
                        'unproven', 'PROTECTION_PLACE_BINDING_MISMATCH')
                else:
                    try:
                        owner = self.book.db.execute(
                            'SELECT action_id,operation_id,account_key '
                            'FROM external_broker_order_ids WHERE order_id=?',
                            (order_id,)).fetchone()
                    except sqlite3.Error:
                        owner = None
                    try:
                        operation = self.book.db.execute(
                            'SELECT operation,report FROM external_operations '
                            'WHERE operation_id=? AND action_id=?',
                            (place['id'], action['id'])).fetchone()
                    except sqlite3.Error:
                        operation = None
                    if (operation is None or operation[0] != 'place'
                            or not isinstance(self._json(operation[1]), dict)
                            or self._json(operation[1]).get('status') != 'submitted'
                            or report.get('order_id') != order_id):
                        reasons.append('PROTECTION_PLACE_EXTERNAL_REPORT_UNPROVEN')
                        layers['place'] = self._layer(
                            'unproven',
                            'PROTECTION_PLACE_EXTERNAL_REPORT_UNPROVEN')
                    elif (owner is None or owner[0] != action['id']
                            or owner[1] != place['id'] or owner[2] != account_key):
                        reasons.append('PROTECTION_ORDER_OWNERSHIP_UNPROVEN')
                        layers['place'] = self._layer(
                            'unproven', 'PROTECTION_ORDER_OWNERSHIP_UNPROVEN')
                    else:
                        layers['place'] = self._layer(
                            'submitted_verified', None, operation_id=place['id'],
                            order_id=order_id,
                            response_hash=p_result.get('response_hash'),
                            recorded_at=p_result.get('recorded_at'),
                            broker_effect_verified=False,
                            report_status='submitted')
                        binding['place_operation_id'] = place['id']
                        binding['place_response_hash'] = p_result.get(
                            'response_hash')
                        binding['broker_order_id'] = order_id
                        place_at = p_result.get('recorded_at')
        if order_id is None:
            return self._result(
                schema_valid=True, reasons=reasons, layers=layers,
                observed={'trade_id': trade_id, 'action_id': action['id']},
                binding=binding, evidence_proven=False,
                qualification='PROTECTION_ACTUAL_EVIDENCE_UNPROVEN')
        cancel_layers, cancel_reasons = self._cancel_layers(trade_id, order_id)
        reasons.extend(cancel_reasons)
        layers['cancel'] = self._layer(
            'blocked' if cancel_reasons else 'clear',
            cancel_reasons[0] if cancel_reasons else None,
            actions=cancel_layers)
        broker_position_qty = None
        current = self._broker_current()
        if not isinstance(current, dict):
            reasons.append('BROKER_OBSERVATION_MISSING')
            layers['broker'] = self._layer('unproven', 'BROKER_OBSERVATION_MISSING')
        else:
            if (current.get('provider') != 'robinhood'
                    or current.get('collection_complete') is not True
                    or current.get('synthetic') is not self.book.simulation):
                reasons.append('BROKER_OBSERVATION_BINDING_INVALID')
                layers['broker'] = self._layer(
                    'unproven', 'BROKER_OBSERVATION_BINDING_INVALID')
            elif (current.get('account_key') != account_key
                  or current.get('account_mask') != '••••' +
                  str(self.book.policy.get('account_suffix'))):
                reasons.append('BROKER_OBSERVATION_ACCOUNT_MISMATCH')
                layers['broker'] = self._layer(
                    'unproven', 'BROKER_OBSERVATION_ACCOUNT_MISMATCH')
            else:
                try:
                    observed_at = timestamp(current.get('observed_at'))
                    age = (self.clock() - observed_at).total_seconds()
                except (TypeError, ValueError, OverflowError):
                    age = None
                if age is None or age < 0 or age > self.max_observation_seconds:
                    reasons.append('BROKER_OBSERVATION_STALE_OR_FUTURE')
                    layers['broker'] = self._layer(
                        'unproven', 'BROKER_OBSERVATION_STALE_OR_FUTURE',
                        age_seconds=age)
                elif current.get('pending_approvals') not in {0, None}:
                    reasons.append('PENDING_APPROVAL_BLOCKS_PROTECTION_ACTUAL')
                    layers['broker'] = self._layer(
                        'unproven', 'PENDING_APPROVAL_BLOCKS_PROTECTION_ACTUAL',
                        pending_approvals=current.get('pending_approvals'))
                else:
                    order = (current.get('orders') or {}).get(order_id)
                    position = (current.get('positions') or {}).get(symbol)
                    try:
                        position_qty = number((position or {}).get('quantity', '0'))
                    except (TypeError, ValueError):
                        position_qty = Decimal(0)
                    broker_position_qty = position_qty
                    if not isinstance(position, dict) or position_qty <= 0:
                        reasons.append('BROKER_POSITION_MISSING')
                        layers['broker'] = self._layer(
                            'unproven', 'BROKER_POSITION_MISSING')
                    elif not isinstance(order, dict):
                        reasons.append('BROKER_PROTECTION_ORDER_MISSING')
                        layers['broker'] = self._layer(
                            'unproven', 'BROKER_PROTECTION_ORDER_MISSING')
                    elif order.get('state') in TERMINAL:
                        reasons.append('BROKER_PROTECTION_ORDER_TERMINAL')
                        layers['broker'] = self._layer(
                            'unproven', 'BROKER_PROTECTION_ORDER_TERMINAL',
                            order_state=order.get('state'))
                    elif (order.get('side') != 'sell'
                          or order.get('type') != 'stop_market'
                          or order.get('symbol') != symbol
                          or order.get('state') not in ACTIVE_PROTECTION_STATES):
                        reasons.append('BROKER_PROTECTION_ORDER_NOT_ACTIVE')
                        layers['broker'] = self._layer(
                            'unproven', 'BROKER_PROTECTION_ORDER_NOT_ACTIVE')
                    else:
                        try:
                            remaining = number(order.get('remaining', '0'))
                            order_stop = number(order.get('stop'))
                        except (TypeError, ValueError):
                            remaining = Decimal(0)
                            order_stop = Decimal(0)
                        if order_stop < trade_stop:
                            reasons.append('BROKER_PROTECTION_STOP_NOT_CURRENT')
                            layers['broker'] = self._layer(
                                'unproven', 'BROKER_PROTECTION_STOP_NOT_CURRENT')
                        elif remaining != position_qty:
                            reasons.append('PROTECTION_COVERAGE_PARTIAL')
                            layers['broker'] = self._layer(
                                'unproven', 'PROTECTION_COVERAGE_PARTIAL',
                                covered=str(remaining), position=str(position_qty))
                        else:
                            layers['broker'] = self._layer(
                                'covered', None, order_id=order_id,
                                order_state=order.get('state'),
                                position_quantity=str(position_qty),
                                covered_quantity=str(remaining),
                                broker_observed_at=current.get('observed_at'),
                                broker_snapshot_sha256=digest(current))
                            binding['broker_observed_at'] = current.get('observed_at')
                            binding['broker_snapshot_sha256'] = digest(current)
        handoff_ok = False
        if not isinstance(review_at, str) or not isinstance(place_at, str):
            reasons.append('PROTECTION_NATIVE_HANDOFF_TIME_UNPROVEN')
            layers['native_handoff'] = self._layer(
                'unproven', 'PROTECTION_NATIVE_HANDOFF_TIME_UNPROVEN')
        else:
            try:
                review_time = timestamp(review_at)
                place_time = timestamp(place_at)
                now = self.clock()
                handoff_seconds = (place_time - review_time).total_seconds()
            except (TypeError, ValueError, OverflowError):
                handoff_seconds = None
            if (handoff_seconds is None or handoff_seconds < 0
                    or handoff_seconds > 30 or place_time > now):
                reasons.append('PROTECTION_NATIVE_HANDOFF_TIME_UNPROVEN')
                layers['native_handoff'] = self._layer(
                    'unproven', 'PROTECTION_NATIVE_HANDOFF_TIME_UNPROVEN',
                    seconds=handoff_seconds)
            else:
                handoff_ok = True
                layers['native_handoff'] = self._layer(
                    'verified', None, seconds=handoff_seconds,
                    source='local_native_review_to_place_receipt_not_broker_fill')
        coverage_ok = (layers.get('broker', {}).get('status') == 'covered')
        first_fill = trade.get('first_fill_at')
        first_coverage = trade.get('first_protection_observed_at')
        latest_coverage = trade.get('last_full_coverage_observed_at')
        first_qty_raw = trade.get('first_protection_observed_quantity')
        latest_qty_raw = trade.get('last_full_coverage_observed_quantity')
        timing_basis = trade.get('protection_timing_basis')
        timing_synthetic = trade.get('protection_timing_synthetic')
        recovery_seen = trade.get('unprotected_first_observed_at')
        recovery_after_first = False
        timing_seconds = None
        timing_ok = False
        timing_failed = False

        def timing_fail(reason, **extra):
            nonlocal timing_failed
            timing_failed = True
            reasons.append(reason)
            layers['timing'] = self._layer(
                'unproven', reason, seconds=timing_seconds,
                basis=timing_basis, synthetic=timing_synthetic, **extra)

        if (not isinstance(first_fill, str)
                or not isinstance(first_coverage, str)
                or not isinstance(latest_coverage, str)):
            timing_fail('PROTECTION_TIMING_UNPROVEN')
        else:
            try:
                first_fill_at = timestamp(first_fill)
                first_coverage_at = timestamp(first_coverage)
                latest_coverage_at = timestamp(latest_coverage)
                now = self.clock()
                timing_seconds = (first_coverage_at - first_fill_at).total_seconds()
            except (TypeError, ValueError, OverflowError):
                timing_fail('PROTECTION_TIMING_UNPROVEN')
            else:
                if (first_fill_at > now or first_coverage_at > now
                        or latest_coverage_at > now):
                    timing_fail('PROTECTION_TIMING_FUTURE')
                elif (first_coverage_at < first_fill_at
                      or latest_coverage_at < first_coverage_at):
                    timing_fail('PROTECTION_TIMING_REVERSED')
                elif timing_basis != 'first_fill_to_first_full_coverage_observation':
                    timing_fail('PROTECTION_TIMING_BASIS_UNPROVEN')
                elif timing_synthetic is not self.book.simulation:
                    timing_fail('PROTECTION_TIMING_SOURCE_MISMATCH')
                elif recovery_seen is not None and first_coverage is not None:
                    try:
                        recovery_after_first = (
                            timestamp(recovery_seen) > timestamp(first_coverage))
                    except (TypeError, ValueError, OverflowError):
                        recovery_after_first = True
                    if recovery_after_first:
                        timing_fail('PROTECTION_RECOVERY_TIMING_UNPROVEN')
                if timing_failed:
                    pass
                elif broker_position_qty is None:
                    timing_fail('PROTECTION_TIMING_POSITION_UNPROVEN')
                else:
                    try:
                        first_qty = number(first_qty_raw)
                        latest_qty = number(latest_qty_raw)
                    except (TypeError, ValueError):
                        first_qty = latest_qty = None
                    if (first_qty is None
                            or first_qty != broker_position_qty
                            or latest_qty != broker_position_qty):
                        timing_fail('PROTECTION_TIMING_QUANTITY_MISMATCH')
                    elif (not isinstance(current, dict)
                          or current.get('observed_at') != latest_coverage):
                        timing_fail('PROTECTION_CURRENT_COVERAGE_UNPROVEN')
                    else:
                        fill_proof = self._entry_first_fill(
                            trade, symbol, first_fill, current)
                        if fill_proof.get('proven') is not True:
                            timing_fail(fill_proof.get('reason') or
                                        'PROTECTION_FIRST_FILL_UNPROVEN')
                            fill_proof = None
                    if timing_failed:
                        pass
                    elif fill_proof is None:
                        pass
                    else:
                        first_observation = self._observation_at(first_coverage)
                        if not isinstance(first_observation, dict):
                            timing_fail('PROTECTION_INITIAL_COVERAGE_OBSERVATION_MISSING')
                        elif (first_observation.get('provider') != 'robinhood'
                              or first_observation.get('collection_complete') is not True
                              or first_observation.get('synthetic') is not self.book.simulation
                              or first_observation.get('account_key') != account_key
                              or first_observation.get('account_mask') != '••••' +
                              str(self.book.policy.get('account_suffix'))):
                            timing_fail('PROTECTION_INITIAL_COVERAGE_BINDING_INVALID')
                        else:
                            first_position = (first_observation.get('positions') or {}).get(symbol)
                            first_order = (first_observation.get('orders') or {}).get(order_id)
                            try:
                                first_position_qty = number(
                                    (first_position or {}).get('quantity', '0'))
                                first_remaining = number(
                                    (first_order or {}).get('remaining', '0'))
                                first_stop = number((first_order or {}).get('stop'))
                            except (TypeError, ValueError):
                                first_position_qty = first_remaining = Decimal(0)
                                first_stop = Decimal(0)
                            if (not isinstance(first_position, dict)
                                    or not isinstance(first_order, dict)
                                    or first_position_qty != first_qty
                                    or first_order.get('state') not in ACTIVE_PROTECTION_STATES
                                    or first_order.get('side') != 'sell'
                                    or first_order.get('type') != 'stop_market'
                                    or first_remaining != first_qty
                                    or first_stop < trade_stop):
                                timing_fail('PROTECTION_INITIAL_COVERAGE_UNPROVEN')
                            elif timing_seconds > PROTECTION_TIMING_BUDGET_SECONDS:
                                reasons.append('PROTECTION_COVERAGE_LATE')
                                layers['timing'] = self._layer(
                                    'late', 'PROTECTION_COVERAGE_LATE',
                                    seconds=timing_seconds, basis=timing_basis,
                                    synthetic=timing_synthetic,
                                    first_coverage_at=first_coverage,
                                    latest_coverage_at=latest_coverage)
                            else:
                                timing_ok = True
                                layers['timing'] = self._layer(
                                    'proven', None, seconds=timing_seconds,
                                    basis=timing_basis, synthetic=timing_synthetic,
                                    first_coverage_at=first_coverage,
                                    latest_coverage_at=latest_coverage,
                                    current_coverage_at=latest_coverage,
                                    execution_ids=fill_proof['execution_ids'],
                                    execution_order_ids=fill_proof['order_ids'],
                                    execution_quantity=fill_proof['quantity'],
                                    source='trade_entry_execution_to_first_full_coverage_observation',
                                    latest_coverage_is_not_first_latency=True)

        evidence_proven = coverage_ok and timing_ok and handoff_ok and not reasons
        qualification = ('SYNTHETIC_TEST_ONLY' if self.book.simulation
                         else 'REAL_ACTUAL_PROTECTION_EVIDENCE')
        layers['overall'] = self._layer(
            'proven' if evidence_proven else 'unproven',
            None if evidence_proven else (reasons[0] if reasons else
                                          'PROTECTION_ACTUAL_EVIDENCE_UNPROVEN'),
            coverage=coverage_ok, timing=timing_ok,
            place_submitted=order_id is not None,
            review_verified=layers.get('review', {}).get('status') == 'verified',
            synthetic_scope=self.book.simulation)
        return self._result(
            schema_valid=True, reasons=reasons, layers=layers,
            observed={'trade_id': trade_id, 'action_id': action['id'],
                      'order_id': order_id, 'broker_observed_at':
                      current.get('observed_at') if isinstance(current, dict) else None},
            binding=binding, evidence_proven=evidence_proven,
            capability_proven=False,
            qualification=qualification if evidence_proven else
            'PROTECTION_ACTUAL_EVIDENCE_UNPROVEN')
