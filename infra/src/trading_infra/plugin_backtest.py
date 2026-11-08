"""Deterministic synthetic replay for one installed plugin.

Semantics are deliberately explicit: a decision at event *i* may create an
order, and that order is eligible only from event *i+1*. No quote is rewritten,
no fill can beat the proposal's entry ceiling, and every position is bound to
one symbol. This is research arithmetic, not a market simulator or execution
authorization.
"""
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path

from trading_contracts.instance import config_version
from trading_contracts.strategy import ProposalAction, StrategyIdentity, StrategyInput
from trading_infra.plugin_protocol import (
    PluginDescriptor, distribution_identity, entry_point_for, validate_config,
)
from trading_infra.plugin_probe import decision_probe, probe
from trading_infra.sizing import size_entry


def _dec(value):
    try:
        value = Decimal(str(value))
    except Exception:
        raise ValueError('PLUGIN_BACKTEST_VALUE_INVALID') from None
    if not value.is_finite() or value < 0:
        raise ValueError('PLUGIN_BACKTEST_VALUE_INVALID')
    return value


def _aware(value, error):
    try:
        result = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    except (TypeError, ValueError):
        raise ValueError(error) from None
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError(error)
    return result.astimezone(timezone.utc)


def _events(dataset):
    if not isinstance(dataset, dict) or dataset.get('schema_version') != 1:
        raise ValueError('PLUGIN_BACKTEST_SCHEMA_REQUIRED')
    if dataset.get('synthetic') is not True:
        raise ValueError('PLUGIN_BACKTEST_REQUIRES_SYNTHETIC_DATA')
    events = dataset.get('events')
    if not isinstance(events, list) or not 1 <= len(events) <= 100000:
        raise ValueError('PLUGIN_BACKTEST_EVENTS_REQUIRED')
    as_of = dataset.get('as_of')
    as_of = (_aware(as_of, 'PLUGIN_BACKTEST_AS_OF_INVALID')
             if as_of is not None else None)
    max_age = dataset.get('max_quote_age_seconds', 15)
    if type(max_age) is not int or not 0 <= max_age <= 60:
        raise ValueError('PLUGIN_BACKTEST_QUOTE_MAX_AGE_INVALID')
    previous = None
    previous_quote_times = {}
    ids = set()
    symbols = set()
    derived = 0
    time_fields = ('bid_at', 'ask_at', 'observed_at')
    for event in events:
        if (not isinstance(event, dict) or not isinstance(event.get('id'), str)
                or not event.get('id') or event.get('kind') != 'quote'
                or not isinstance(event.get('data'), dict)):
            raise ValueError('PLUGIN_BACKTEST_EVENT_INVALID')
        if event['id'] in ids:
            raise ValueError('PLUGIN_BACKTEST_DUPLICATE_EVENT')
        ids.add(event['id'])
        at = _aware(event.get('at'), 'PLUGIN_BACKTEST_EVENT_TIME_INVALID')
        if previous is not None and at < previous:
            raise ValueError('PLUGIN_BACKTEST_OUT_OF_ORDER')
        if as_of is not None and at > as_of:
            raise ValueError('PLUGIN_BACKTEST_FUTURE_EVENT')
        previous = at
        data = event['data']
        symbol = data.get('symbol')
        if not isinstance(symbol, str) or not symbol:
            raise ValueError('PLUGIN_BACKTEST_SYMBOL_REQUIRED')
        symbols.add(symbol)
        if len(symbols) > 1:
            raise ValueError('PLUGIN_BACKTEST_MULTI_SYMBOL_UNSUPPORTED')
        for key in ('bid', 'ask'):
            if _dec(data.get(key)) <= 0:
                raise ValueError('PLUGIN_BACKTEST_QUOTE_INVALID')
        if _dec(data['bid']) > _dec(data['ask']):
            raise ValueError('PLUGIN_BACKTEST_BID_ASK_INVALID')
        for field in time_fields:
            if field not in data or data[field] is None:
                derived += 1
                continue
            observed = _aware(data[field], 'PLUGIN_BACKTEST_QUOTE_TIME_INVALID')
            if observed > at:
                raise ValueError('PLUGIN_BACKTEST_QUOTE_TIME_FUTURE')
            if (at - observed).total_seconds() > max_age:
                raise ValueError('PLUGIN_BACKTEST_QUOTE_TIME_STALE')
            if (field in previous_quote_times
                    and observed < previous_quote_times[field]):
                raise ValueError('PLUGIN_BACKTEST_QUOTE_TIMES_OUT_OF_ORDER')
            previous_quote_times[field] = observed
    return events, derived, max_age

def _exit_rule(plugin_id, config, view):
    value = probe(plugin_id, config, 'exit', view=view)
    if value.get('ok') is not True:
        raise ValueError('PLUGIN_BACKTEST_EXIT_RULE_FAILED:' + str(value.get('error')))
    result = value.get('result')
    if not isinstance(result, dict) or result.get('action') not in {
            'EXIT', 'REDUCE', 'TIGHTEN_PROTECTION', 'HOLD'}:
        raise ValueError('PLUGIN_BACKTEST_EXIT_RULE_OUTPUT_INVALID')
    if set(result) - {'action', 'reason', 'quantity', 'stop'}:
        raise ValueError('PLUGIN_BACKTEST_EXIT_RULE_OUTPUT_INVALID')
    if result['action'] == 'REDUCE' and type(result.get('quantity')) is not int:
        raise ValueError('PLUGIN_BACKTEST_REDUCE_QUANTITY_REQUIRED')
    if result['action'] == 'TIGHTEN_PROTECTION' and result.get('stop') is None:
        raise ValueError('PLUGIN_BACKTEST_TIGHTEN_STOP_INVALID')
    return result

def run_backtest(plugin_id, config, dataset, *, fee_per_share='0.01',
                 slippage_per_share='0.01', max_notional='100',
                 max_planned_loss='2.50', initial_equity='10000',
                 exit_advice_ttl_seconds=20,
                 end_of_data_policy='LIQUIDATE_AT_LAST_BID'):
    entry = entry_point_for(plugin_id)
    identity = distribution_identity(entry).digest
    described = probe(plugin_id, config, 'describe')
    if described.get('ok') is not True:
        raise ValueError('PLUGIN_BACKTEST_PLUGIN_FAILED:' + str(described.get('error')))
    descriptor = PluginDescriptor(**described['descriptor'])
    if described.get('distribution_digest') != identity:
        raise ValueError('PLUGIN_BACKTEST_IDENTITY_MISMATCH')
    if set(descriptor.data_requirements) != {'quotes'}:
        raise ValueError('PLUGIN_BACKTEST_DATA_REQUIREMENTS_UNSUPPORTED')
    if descriptor.warmup_bars or descriptor.history_bars:
        raise ValueError('PLUGIN_BACKTEST_WARMUP_UNSUPPORTED')
    validate_config(descriptor.config_schema, config)
    created = probe(plugin_id, config, 'create')
    if created.get('ok') is not True:
        raise ValueError('PLUGIN_BACKTEST_CREATE_FAILED:' + str(created.get('error')))
    events, derived_times, max_quote_age = _events(dataset)
    fee, slip = _dec(fee_per_share), _dec(slippage_per_share)
    initial = _dec(initial_equity)
    if initial <= 0:
        raise ValueError('PLUGIN_BACKTEST_INITIAL_EQUITY_INVALID')
    if type(exit_advice_ttl_seconds) is not int or exit_advice_ttl_seconds <= 0:
        raise ValueError('PLUGIN_BACKTEST_EXIT_TTL_INVALID')
    if end_of_data_policy not in {'LIQUIDATE_AT_LAST_BID', 'NONE'}:
        raise ValueError('PLUGIN_BACKTEST_END_OF_DATA_POLICY_INVALID')
    checkpoint = {}
    cash = initial
    position = None
    pending_entry = None
    pending_exit = None
    trades = []
    unfilled = []
    fees = Decimal(0)
    slippage = Decimal(0)
    gross = Decimal(0)
    symbol = events[0]['data']['symbol']

    def close_position(at, bid, reason, quantity=None):
        nonlocal cash, position, fees, slippage, gross
        if position is None:
            return
        quantity = min(position['quantity'], quantity or position['quantity'])
        price = bid - slip
        if price <= 0:
            return
        exit_fees = fee * quantity
        exit_slip = slip * quantity
        cash += price * quantity - exit_fees
        # A trade row accounts for both sides, while the aggregate counters are
        # updated at each actual cash event (entry and exit) exactly once.
        trade_fees = exit_fees + fee * quantity
        trade_slip = exit_slip + slip * quantity
        trade_gross = (bid - position['entry_raw']) * quantity
        fees += exit_fees
        slippage += exit_slip
        gross += trade_gross
        trades.append({'symbol': symbol, 'quantity': quantity,
                       'entry': str(position['entry_fill']),
                       'raw_entry': str(position['entry_raw']),
                       'exit': str(price), 'raw_exit': str(bid),
                       'reason': reason,
                       'gross_pnl': str(trade_gross),
                       'fees': str(trade_fees), 'slippage': str(trade_slip),
                       'pnl': str((trade_gross - trade_fees - trade_slip
                                   ).quantize(Decimal('0.01')))})
        position['quantity'] -= quantity
        if position['quantity'] == 0:
            position = None

    def execute_exit(at, bid, reason):
        nonlocal pending_exit
        if position is None or pending_exit is None:
            return
        close_position(at, bid, reason, pending_exit['quantity'])
        pending_exit = None

    def queue_exit(action, reason, valid_until, quantity=None, stop=None):
        nonlocal pending_exit, position
        if action == 'EXIT':
            pending_exit = {'quantity': position['quantity'], 'reason': reason,
                            'valid_until': valid_until, 'structural': False}
        elif action == 'REDUCE':
            try:
                reduced = int(quantity)
            except (TypeError, ValueError):
                reduced = 0
            if reduced <= 0:
                raise ValueError('PLUGIN_BACKTEST_REDUCE_QUANTITY_REQUIRED')
            pending_exit = {'quantity': min(reduced, position['quantity']),
                            'reason': reason, 'valid_until': valid_until,
                            'structural': False}
        elif action == 'TIGHTEN_PROTECTION':
            if stop is None or _dec(stop) <= position['stop']:
                raise ValueError('PLUGIN_BACKTEST_TIGHTEN_STOP_INVALID')
            position['stop'] = _dec(stop)

    for index, event in enumerate(events):
        at = datetime.fromisoformat(str(event['at']).replace('Z', '+00:00'))
        data = event['data']
        if data['symbol'] != symbol:
            raise ValueError('PLUGIN_BACKTEST_MULTI_SYMBOL_UNSUPPORTED')
        bid, ask = _dec(data['bid']), _dec(data['ask'])
        # Orders created by event i execute no earlier than event i+1.
        if pending_entry is not None:
            if at <= pending_entry['valid_until'] and ask + slip <= pending_entry['ceiling']:
                unit_cost = ask + slip + fee
                max_by_cash = int(cash / unit_cost) if cash > 0 else 0
                quantity = min(size_entry(
                    entry_ceiling=pending_entry['ceiling'], stop=pending_entry['stop'],
                    cost_buffer_per_share=fee + slip,
                    max_position_notional=max_notional,
                    max_planned_loss=max_planned_loss,
                    quantity_limit=pending_entry['quantity_limit']), max_by_cash)
                if quantity > 0:
                    price = ask + slip
                    cash -= price * quantity + fee * quantity
                    fees += fee * quantity
                    slippage += slip * quantity
                    position = {'symbol': symbol, 'quantity': quantity,
                                'entry': price, 'entry_fill': price,
                                'entry_raw': ask, 'stop': pending_entry['stop']}
                    pending_entry = None
            elif at > pending_entry['valid_until']:
                unfilled.append({'reason': 'LIMIT_EXPIRED', 'at': at.isoformat(),
                                 'ceiling': str(pending_entry['ceiling'])})
                pending_entry = None
        if position is not None and bid <= position['stop'] and pending_exit is not None:
            # An already eligible exit is relabelled as the structural stop;
            # it does not wait for a strategy TTL.
            pending_exit = {'quantity': position['quantity'],
                            'reason': 'STRUCTURAL_STOP',
                            'valid_until': None, 'structural': True}
        if pending_exit is not None and position is not None:
            if pending_exit['structural'] or (
                    pending_exit['valid_until'] is not None
                    and at <= pending_exit['valid_until']):
                execute_exit(at, bid, pending_exit['reason'])
            else:
                unfilled.append({'reason': 'EXIT_EXPIRED', 'at': at.isoformat()})
                pending_exit = None
        if position is not None and bid <= position['stop']:
            # A stop observed at this event is eligible from the next event;
            # it has no TTL and cannot be cancelled by HOLD.
            pending_exit = {'quantity': position['quantity'],
                            'reason': 'STRUCTURAL_STOP',
                            'valid_until': None, 'structural': True}
        event_data = dict(data)
        for field in ('bid_at', 'ask_at', 'observed_at'):
            if event_data.get(field) is None:
                event_data[field] = at.isoformat()
        if position is not None:
            execution_lifecycle = ('exit_pending' if pending_exit is not None
                                   else 'partial')
            execution_quantity = str(position['quantity'])
        elif pending_entry is not None:
            execution_lifecycle = 'pending'
            execution_quantity = '0'
        elif trades:
            execution_lifecycle = 'flat_verified'
            execution_quantity = '0'
        else:
            execution_lifecycle = 'not_executed'
            execution_quantity = '0'
        request = StrategyInput(
            identity=StrategyIdentity('backtest', plugin_id, descriptor.version),
            as_of=at, cursor=str(event['id']),
            data_snapshot_id='backtest-' + str(event['id']),
            source_sha256='b' * 64,
            config_version=config_version(plugin_id, descriptor.version, config, identity),
            synthetic=True,
            allocation={'instance_id': 'backtest', 'execution': {
                'instance_id': 'backtest',
                'lifecycle': execution_lifecycle,
                'confirmed_quantity': execution_quantity,
                'available_quantity': execution_quantity,
                'available_quantity_state': 'MODEL',
                'feedback_version': index + 1,
                'observed_at': at.isoformat(),
                'fact_fresh': True,
                'observation_at': at.isoformat(),
                'observation_fresh': True,
                'owner_state': None,
                'owner_action_at': None,
                'owner_action_kind': None,
                'owner_trade_id': None,
                'owner_activity_source': 'backtest_model',
                'source': 'backtest_model',
                'accounting_state': 'MODEL'}},
            market={'event': {'id': event['id'], 'kind': 'quote',
                              'at': at.isoformat(), 'synthetic_input': True,
                              'source_sha256': 'b' * 64,
                              'data': event_data}},
            checkpoint=checkpoint)
        decision, decision_error = decision_probe(plugin_id, config, request)
        if decision is None:
            raise ValueError('PLUGIN_BACKTEST_DECIDE_FAILED:' + str(decision_error))
        checkpoint = dict(decision.next_checkpoint)
        proposed = list(decision.proposals)
        if position is not None and pending_exit is None:
            features = {}
            for key in descriptor.feature_keys:
                stored = checkpoint.get(key)
                if isinstance(stored, Mapping) and symbol in stored:
                    features[key] = stored[symbol]
                elif stored is not None and not isinstance(stored, Mapping):
                    features[key] = stored
            view = {'instance_id': 'backtest', 'plugin_id': plugin_id,
                    'trade_id': 'backtest-trade', 'symbol': symbol,
                    'quantity': str(position['quantity']),
                    'entry_price': str(position['entry']), 'stop': str(position['stop']),
                    'initial_stop': str(position['stop']),
                    'quote': {'symbol': symbol, 'bid': str(bid), 'ask': str(ask),
                              'bid_at': data.get('bid_at', at.isoformat()),
                              'ask_at': data.get('ask_at', at.isoformat()),
                              'observed_at': data.get('observed_at', at.isoformat())},
                    'features': features, 'as_of': at.isoformat(),
                    'position_version': 1}
            rule = (_exit_rule(plugin_id, config, view)
                    if descriptor.exit_rule is not None else None)
            if rule is not None:
                proposed.append(dict(rule, valid_until=at + timedelta(
                    seconds=exit_advice_ttl_seconds)))
        for proposal in proposed:
            if isinstance(proposal, dict):
                action = proposal.get('action')
                reason = proposal.get('reason', 'EXIT_RULE')
                valid_until = proposal.get('valid_until')
                if set(proposal) - {'action', 'reason', 'valid_until',
                                    'quantity', 'stop'}:
                    raise ValueError('PLUGIN_BACKTEST_ACTION_FIELDS_UNSUPPORTED')
                if not isinstance(valid_until, datetime):
                    raise ValueError('PLUGIN_BACKTEST_PROPOSAL_VALIDITY_INVALID')
                if (valid_until.tzinfo is None or valid_until.utcoffset() is None
                        or valid_until <= at):
                    raise ValueError('PLUGIN_BACKTEST_PROPOSAL_VALIDITY_INVALID')
                quantity_limit = proposal.get('quantity')
                stop = proposal.get('stop')
                ceiling = None
            else:
                action = proposal.action.value
                reason = proposal.reason
                valid_until = proposal.valid_until
                quantity_limit = proposal.quantity_limit
                stop = proposal.stop_price
                ceiling = proposal.entry_ceiling
                if proposal.symbol != symbol:
                    raise ValueError('PLUGIN_BACKTEST_PROPOSAL_SYMBOL_MISMATCH')
            if action == 'ENTER':
                if position is not None or pending_entry is not None:
                    continue
                if ceiling is None or stop is None:
                    raise ValueError('PLUGIN_BACKTEST_ENTRY_FIELDS_REQUIRED')
                pending_entry = {'ceiling': ceiling, 'stop': stop,
                                 'quantity_limit': quantity_limit,
                                 'valid_until': valid_until}
            elif action in {'EXIT', 'REDUCE', 'TIGHTEN_PROTECTION'}:
                if position is None:
                    continue
                # Structural stop already owns the exit; HOLD/strategy advice
                # cannot overwrite it.
                if pending_exit is not None and pending_exit['structural']:
                    continue
                queue_exit(action, reason, valid_until, quantity_limit, stop)
            elif action == 'HOLD':
                continue
            else:
                raise ValueError('PLUGIN_BACKTEST_ACTION_UNSUPPORTED:' + str(action))
    if pending_entry is not None:
        unfilled.append({'reason': 'LIMIT_NOT_MARKETABLE_AT_END',
                         'ceiling': str(pending_entry['ceiling'])})
    if pending_exit is not None:
        # An exit created at the final event has no next event in which it can
        # execute. It is reported, never executed as though a next event existed.
        unfilled.append({'reason': 'EXIT_NOT_EXECUTED_AT_END',
                         'pending_reason': pending_exit['reason'],
                         'structural': pending_exit['structural']})
        pending_exit = None
    if position is not None and end_of_data_policy == 'LIQUIDATE_AT_LAST_BID':
        last = events[-1]
        last_at = _aware(last['at'], 'PLUGIN_BACKTEST_EVENT_TIME_INVALID')
        close_position(last_at, _dec(last['data']['bid']),
                       'END_OF_DATA_LIQUIDATION')
    final_equity = cash
    open_position = (None if position is None else
                     {'symbol': symbol, 'quantity': position['quantity'],
                      'entry': str(position['entry_fill']),
                      'stop': str(position['stop'])})
    dataset_bytes = json.dumps(dataset, sort_keys=True, separators=(',', ':')).encode()
    return {'schema_version': 1, 'plugin_id': plugin_id,
            'version': descriptor.version,
            'distribution': described.get('distribution'),
            'distribution_version': described.get('distribution_version'),
            'distribution_digest': identity,
            'config': dict(config), 'synthetic': True,
            'dataset_sha256': hashlib.sha256(dataset_bytes).hexdigest(),
            'time_model': {'event_time': 'aware UTC from event.at',
                           'quote_times': 'existing bid_at/ask_at/observed_at preserved; missing values derived as event.at',
                           'derived_quote_fields': derived_times,
                           'max_quote_age_seconds': max_quote_age},
            'events': len(events), 'trades': trades, 'trade_count': len(trades),
            'unfilled_orders': unfilled, 'initial_equity': str(initial),
            'final_equity': str(final_equity),
            'gross_pnl': str(gross), 'fees': str(fees), 'slippage': str(slippage),
            'fees_and_slippage': str(fees + slippage),
            'net_pnl': str((final_equity - initial).quantize(Decimal('0.01'))),
            'accounting': {
                'gross_pnl': 'sum((raw_exit_bid - raw_entry_ask) * quantity)',
                'fees': 'sum(fee_per_share * quantity) on entry and exit',
                'slippage': 'sum(slippage_per_share * quantity) on entry and exit',
                'net_pnl': 'final_equity - initial_equity = gross_pnl - fees - slippage',
                'reconciliation_ok': gross - fees - slippage == final_equity - initial},
            'open_position': open_position,
            'end_of_data_policy': end_of_data_policy,
            'end_of_data_liquidation': any(
                trade['reason'] == 'END_OF_DATA_LIQUIDATION'
                for trade in trades),
            'end_of_data_liquidation_policy': end_of_data_policy,
            'end_of_data_liquidation_note': (
                'a true value means at least one actual END_OF_DATA_LIQUIDATION '
                'trade exists; this is a policy/event, not a broker fill model'),
            'fill_model': {'decision': 'orders created at event i are eligible at i+1',
                           'entry': 'ask + slippage; no fill above entry ceiling; cash-capped',
                           'exit': 'bid - slippage',
                           'stop': 'structural stop wins over strategy exits and has no TTL; executes next event',
                           'expiry': 'strategy proposal/rule valid_until; expired orders are unfilled',
                           'single_symbol': True},
            'limitations': ['synthetic quotes only', 'no queue position, impact or partial fills',
                            'no short/borrow model', 'no profitability or live-execution claim'],
            'production_ready': False}


def run_backtest_file(plugin_id, config, dataset_path, **kwargs):
    dataset = json.loads(Path(dataset_path).read_text())
    return run_backtest(plugin_id, config, dataset, **kwargs)
