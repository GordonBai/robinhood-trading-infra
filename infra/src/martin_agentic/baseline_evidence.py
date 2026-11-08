"""Pure offline normalization of saved official read-only tool responses.

This module consumes a private bundle of captured MCP tool-result envelopes. It
never opens a broker, market, MCP, Host, SQLite book, risk sidecar, or network
connection. The raw files are local captured response envelopes, not broker wire
bytes and not a signature. Hashes prove only that the local bundle did not change
between capture and normalization.

The output is deliberately diagnostic: unsupported or missing official getters
remain blockers. A partial observation or candidate snapshot is never a
production-usable historical baseline.
"""
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path
import re
import stat

from trading_contracts.values import canonical, digest, timestamp

MAX_MANIFEST_BYTES = 4 * 1024 * 1024
MAX_RAW_BYTES = 16 * 1024 * 1024
MAX_TOTAL_RAW_BYTES = 64 * 1024 * 1024
MAX_PAGES_PER_TOOL = 100
MAX_COLLECTION_SECONDS = 30
SHA_RE = re.compile(r'^[0-9a-f]{64}$')
SUFFIX_RE = re.compile(r'^\d{4}$')

CROSS_ASSET_SCOPES = ('options', 'crypto', 'futures', 'event_contracts',
                      'fixed_income', 'mutual_funds')
SUPPORTED_SCOPES = ('options', 'crypto')
UNSUPPORTED_SCOPES = ('futures', 'event_contracts', 'fixed_income',
                      'mutual_funds')

# Tools whose complete collection requires two independent local passes. The
# second pass is required for positions, orders, and approvals; it is not
# invented when absent.
TWO_PASS_TOOLS = frozenset({
    'get_equity_positions', 'get_equity_orders', 'get_advanced_orders',
    'get_option_positions', 'get_option_orders', 'get_crypto_positions',
    'get_crypto_orders', 'get_trade_approvals',
})
SINGLE_PASS_TOOLS = frozenset({
    'get_accounts', 'get_portfolio', 'get_trade_approval_setting',
})
ALLOWED_ARGS = {
    'get_accounts': frozenset(),
    'get_equity_positions': frozenset({'account_number', 'cursor'}),
    'get_equity_orders': frozenset({
        'account_number', 'agent_initiated', 'created_at_gte', 'cursor',
        'order_id', 'placed_agent', 'state', 'symbol'}),
    'get_advanced_orders': frozenset({
        'account_number', 'contingency_type', 'created_at_gte', 'cursor',
        'order_id'}),
    'get_option_positions': frozenset({
        'account_number', 'chain_ids', 'cursor', 'expiration_date',
        'expiration_date_gte', 'expiration_date_lte', 'nonzero', 'option_ids',
        'option_type', 'type'}),
    'get_option_orders': frozenset({
        'account_number', 'agent_initiated', 'chain_ids', 'created_at_gte',
        'cursor', 'order_id', 'placed_agent', 'state', 'underlying_type'}),
    'get_crypto_positions': frozenset({'rhs_account_number', 'cursor'}),
    'get_crypto_orders': frozenset({
        'created_at_gte', 'cursor', 'order_id', 'rhs_account_number', 'side',
        'state', 'state_group', 'symbol', 'updated_at_gte'}),
    'get_portfolio': frozenset({'account_number'}),
    'get_trade_approval_setting': frozenset({'account_number'}),
    'get_trade_approvals': frozenset({
        'account_number', 'approval_ids', 'asset_class', 'cursor'}),
}
PAGE_KEYS = frozenset({
    'tool', 'args', 'pass', 'page_index', 'cursor_requested',
    'cursor_returned', 'started_at', 'completed_at', 'raw_path',
    'raw_sha256', 'raw_bytes', 'request_args_sha256', 'normalized_count',
    'normalized_sha256',
})
TOOL_ROW_KEYS = {
    'get_accounts': 'accounts',
    'get_equity_positions': 'positions',
    'get_equity_orders': 'orders',
    'get_advanced_orders': 'advanced_orders',
    'get_option_positions': 'positions',
    'get_option_orders': 'orders',
    'get_crypto_positions': 'results',
    'get_crypto_orders': 'results',
    'get_trade_approvals': 'approvals',
}
IDENTITY_BASELINE_SCOPE = 'terminal_equity_order_identity_v1'
IDENTITY_KIND = 'terminal_equity_order_identity'
IDENTITY_CLAIMS = {
    'order_identity_only': True,
    'full_account_flat': False,
    'other_asset_complete': False,
    'settlement_ledger_complete': False,
}
IDENTITY_TERMINAL_STATES = frozenset({
    'filled', 'cancelled', 'canceled', 'rejected', 'failed', 'voided',
    'expired'})

CAPABILITY_MATRIX = {
    'get_accounts': 'typed',
    'get_equity_positions': 'typed',
    'get_equity_orders': 'typed',
    'get_advanced_orders': 'typed',
    'get_option_positions': 'typed',
    'get_option_orders': 'typed',
    'get_crypto_positions': 'typed',
    'get_crypto_orders': 'empty_only',
    'get_portfolio': 'typed',
    'get_trade_approval_setting': 'typed',
    'get_trade_approvals': 'empty_only',
    'futures_positions': 'getter_missing',
    'futures_orders': 'getter_missing',
    'event_contracts_positions': 'getter_missing',
    'event_contracts_orders': 'getter_missing',
    'fixed_income_positions': 'getter_missing',
    'fixed_income_orders': 'getter_missing',
    'mutual_funds_positions': 'getter_missing',
    'mutual_funds_orders': 'getter_missing',
    'settlements': 'getter_missing',
}


class BundleError(ValueError):
    """Structural/integrity rejection for a saved response bundle."""

    def __init__(self, code, detail=None):
        self.code = code
        self.detail = detail
        super().__init__(code)

    def __str__(self):
        return self.code if self.detail is None else f'{self.code}:{self.detail}'


def _fail(code, detail=None):
    raise BundleError(code, detail)


def _sha(value):
    return isinstance(value, str) and SHA_RE.fullmatch(value) is not None


def _time(value):
    try:
        return timestamp(value)
    except (TypeError, ValueError, OverflowError):
        _fail('BUNDLE_TIMESTAMP_INVALID', str(value))


def _decimal(value, code='BUNDLE_NUMBER_INVALID'):
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        _fail(code)
    if not result.is_finite():
        _fail(code)
    return result


def _text(value, code):
    if not isinstance(value, str) or not value:
        _fail(code)
    return value


def _private_dir(path):
    path = Path(path)
    if path.is_symlink() or not path.is_dir():
        _fail('BUNDLE_ROOT_INVALID')
    if stat.S_IMODE(path.stat().st_mode) & 0o077:
        _fail('BUNDLE_PRIVATE_DIRECTORY_REQUIRED')
    return path.resolve()


def _read_private(path, limit, code):
    path = Path(path)
    if path.is_symlink() or not path.is_file():
        _fail(code, str(path))
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError:
        _fail(code, str(path))
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size > limit:
            _fail(code, str(path))
        data = os.read(fd, limit + 1)
        if len(data) != info.st_size or len(data) > limit:
            _fail(code, str(path))
        return data
    finally:
        os.close(fd)


def _unwrap(value):
    if not isinstance(value, dict) or value.get('isError'):
        _fail('BUNDLE_TOOL_RESULT_ERROR')
    if 'data' in value:
        return value['data']
    structured = value.get('structuredContent')
    if isinstance(structured, dict) and 'data' in structured:
        return structured['data']
    for block in value.get('content') or ():
        if isinstance(block, dict) and block.get('type') == 'text':
            try:
                parsed = json.loads(block.get('text'), parse_float=Decimal)
            except (TypeError, ValueError):
                _fail('BUNDLE_TOOL_CONTENT_INVALID')
            if isinstance(parsed, dict) and 'data' in parsed:
                return parsed['data']
    _fail('BUNDLE_TOOL_RESULT_INVALID')


def _rows(data, key):
    if not isinstance(data, dict) or not isinstance(data.get(key), list):
        _fail('BUNDLE_TOOL_ROWS_INVALID', key)
    if any(not isinstance(row, dict) for row in data[key]):
        _fail('BUNDLE_TOOL_ROW_INVALID', key)
    return data[key]


def _next(data):
    value = data.get('next') if isinstance(data, dict) else None
    if value in (None, ''):
        return None
    if not isinstance(value, str):
        _fail('BUNDLE_CURSOR_INVALID')
    return value


def _account_args(tool, args, account):
    if not isinstance(args, dict) or set(args) - ALLOWED_ARGS[tool]:
        _fail('BUNDLE_REQUEST_ARGS_INVALID', tool)
    if tool == 'get_accounts':
        if args:
            _fail('BUNDLE_REQUEST_ARGS_INVALID', tool)
        return
    if tool in {'get_crypto_positions', 'get_crypto_orders'}:
        if args.get('rhs_account_number') != account['rhs_account_number']:
            _fail('BUNDLE_CRYPTO_ACCOUNT_MISMATCH', tool)
        if 'rhc_account_number' in args or 'account_number' in args:
            _fail('BUNDLE_CRYPTO_IDENTIFIER_MIXED', tool)
    else:
        if args.get('account_number') != account['account_number']:
            _fail('BUNDLE_ACCOUNT_MISMATCH', tool)
        if 'rhs_account_number' in args or 'rhc_account_number' in args:
            _fail('BUNDLE_CRYPTO_IDENTIFIER_MIXED', tool)




def _query_scope(tool, args):
    """Classify whether a page is an account-wide listing or a subset query."""
    if tool in {'get_accounts', 'get_portfolio', 'get_trade_approval_setting'}:
        return 'full'
    if tool == 'get_equity_orders':
        if 'order_id' in args:
            return 'detail'
        if set(args) & {'agent_initiated', 'created_at_gte', 'placed_agent',
                        'state', 'symbol'}:
            return 'filtered'
        return 'full'
    if tool == 'get_advanced_orders':
        if 'order_id' in args:
            return 'detail'
        if set(args) & {'contingency_type', 'created_at_gte'}:
            return 'filtered'
        return 'full'
    if tool == 'get_option_positions':
        if 'nonzero' in args and args.get('nonzero') is True:
            return 'filtered'
        if set(args) & {'chain_ids', 'expiration_date', 'expiration_date_gte',
                        'expiration_date_lte', 'option_ids',
                        'option_type', 'type'}:
            return 'filtered'
        return 'full'
    if tool == 'get_option_orders':
        if 'order_id' in args:
            return 'detail'
        if set(args) & {'agent_initiated', 'chain_ids', 'created_at_gte',
                        'placed_agent', 'state', 'underlying_type'}:
            return 'filtered'
        return 'full'
    if tool == 'get_crypto_positions':
        return 'full'
    if tool == 'get_crypto_orders':
        if 'order_id' in args:
            return 'detail'
        if set(args) & {'created_at_gte', 'side', 'state', 'state_group',
                        'symbol', 'updated_at_gte'}:
            return 'filtered'
        return 'full'
    if tool == 'get_trade_approvals':
        if 'approval_ids' in args:
            return 'detail'
        if 'asset_class' in args:
            return 'filtered'
        return 'full'
    return 'full'


def _page_data(page, raw):
    try:
        value = json.loads(raw, parse_float=Decimal)
    except (TypeError, ValueError):
        _fail('BUNDLE_RAW_JSON_INVALID', page['raw_path'])
    return _unwrap(value)


def _collection_pages(loaded, tool, pass_number):
    return sorted((page for page in loaded['pages']
                   if page['tool'] == tool and page['pass'] == pass_number),
                  key=lambda page: page['page_index'])


def _full_pages(loaded, tool, pass_number):
    return [page for page in _collection_pages(loaded, tool, pass_number)
            if page['query_scope'] == 'full']


def _validate_page_chain(loaded, tool, pass_number):
    return _validate_page_chain_pages(
        loaded, tool, pass_number, _collection_pages(loaded, tool, pass_number))


def _validate_page_chain_pages(loaded, tool, pass_number, pages):
    if not pages:
        return []
    if tool in SINGLE_PASS_TOOLS and pass_number != 1:
        _fail('BUNDLE_PASS_INVALID', tool)
    if len(pages) > MAX_PAGES_PER_TOOL:
        _fail('BUNDLE_PAGE_LIMIT', tool)
    if [page['page_index'] for page in pages] != list(range(len(pages))):
        _fail('BUNDLE_PAGE_ORDER', tool)
    previous = None
    seen_requested = set()
    for page in pages:
        args_cursor = page['args'].get('cursor')
        if args_cursor in (None, '') and page['cursor_requested'] is not None:
            _fail('BUNDLE_CURSOR_CHAIN', tool)
        if args_cursor not in (None, '') and args_cursor != page['cursor_requested']:
            _fail('BUNDLE_CURSOR_CHAIN', tool)
        if page['cursor_requested'] != previous:
            _fail('BUNDLE_CURSOR_CHAIN', tool)
        seen_requested.add(page['cursor_requested'])
        if page['cursor_returned'] is not None \
                and page['cursor_returned'] in seen_requested:
            _fail('BUNDLE_CURSOR_LOOP', tool)
        previous = page['cursor_returned']
        if page['page_index'] + 1 < len(pages) and page['cursor_returned'] is None:
            _fail('BUNDLE_MISSING_PAGE', tool)
        if page['page_index'] + 1 == len(pages) and page['cursor_returned'] is not None:
            _fail('BUNDLE_MISSING_PAGE', tool)
    return pages


def _normalize_account_page(page):
    data = page['data']
    accounts = _rows(data, 'accounts')
    return accounts


def _normalize_equity_positions(page):
    data = page['data']
    records = []
    for raw in _rows(data, 'positions'):
        symbol = _text(raw.get('symbol'), 'BUNDLE_POSITION_IDENTITY')
        if not re.fullmatch(r'[A-Z][A-Z0-9.\-]{0,14}', symbol):
            _fail('BUNDLE_POSITION_IDENTITY')
        required = ('quantity', 'shares_available_for_sells',
                    'shares_held_for_sells', 'shares_held_for_asset_transfer',
                    'shares_held_for_options_events',
                    'shares_held_for_stock_grants',
                    'shares_pending_from_options_events')
        if any(key not in raw for key in required):
            _fail('BUNDLE_EQUITY_POSITION_SCHEMA', symbol)
        values = {key: _decimal(raw[key]) for key in required}
        if any(value < 0 for key, value in values.items()
               if key != 'quantity'):
            _fail('BUNDLE_EQUITY_POSITION_HOLDS', symbol)
        record = {
            'symbol': symbol,
            'quantity': str(values['quantity']),
            'available_for_sells': str(values['shares_available_for_sells']),
            'held_for_sells': str(values['shares_held_for_sells']),
            'shares_held_for_asset_transfer': str(
                values['shares_held_for_asset_transfer']),
            'shares_held_for_options_events': str(
                values['shares_held_for_options_events']),
            'shares_held_for_stock_grants': str(
                values['shares_held_for_stock_grants']),
            'shares_pending_from_options_events': str(
                values['shares_pending_from_options_events']),
        }
        avg = raw.get('average_buy_price')
        if avg is not None:
            record['average_price'] = str(_decimal(avg))
        records.append(record)
    return records


def _normalize_equity_order(raw, observed):
    from .broker import normalize_order
    observed_text = observed.isoformat() if isinstance(observed, datetime) else observed
    try:
        return normalize_order(raw, observed_text)
    except (KeyError, TypeError, ValueError) as exc:
        _fail('BUNDLE_EQUITY_ORDER_INVALID', str(exc))


def _normalize_advanced_page(page, observed, detail_by_id):
    groups = []
    orders = []
    for raw in _rows(page['data'], 'advanced_orders'):
        group_id = _text(raw.get('id'), 'BUNDLE_ADVANCED_GROUP_ID')
        account = _text(raw.get('account_number'), 'BUNDLE_ADVANCED_ACCOUNT')
        if account != page['account']['account_number']:
            _fail('BUNDLE_ADVANCED_ACCOUNT_MISMATCH', group_id)
        links = raw.get('advanced_order_links')
        if not isinstance(links, list) or not links:
            _fail('BUNDLE_ADVANCED_LEGS_MISSING', group_id)
        leg_ids = []
        for link in links:
            if not isinstance(link, dict):
                _fail('BUNDLE_ADVANCED_LINK_INVALID', group_id)
            leg = link.get('order')
            if leg is None:
                leg_id = link.get('order_id')
                leg = detail_by_id.get(leg_id)
                if leg is None:
                    _fail('BUNDLE_ADVANCED_LEG_UNRESOLVED', leg_id or group_id)
            normalized = _normalize_equity_order(leg, observed)
            if link.get('order_id') not in (None, normalized['id']):
                _fail('BUNDLE_ADVANCED_LEG_ID_MISMATCH', group_id)
            leg_ids.append(normalized['id'])
            orders.append(normalized)
        groups.append({
            'id': group_id,
            'state': _text(raw.get('state'), 'BUNDLE_ADVANCED_STATE'),
            'kind': _text(raw.get('contingency_type'), 'BUNDLE_ADVANCED_KIND'),
            'leg_ids': leg_ids,
        })
    return groups, orders


def _normalize_option_position(raw):
    for key in ('quantity', 'pending_assignment_quantity',
                'pending_buy_quantity', 'pending_exercise_quantity',
                'pending_expiration_quantity', 'pending_sell_quantity'):
        if key not in raw:
            _fail('BUNDLE_OPTION_POSITION_SCHEMA', key)
    option_id = _text(raw.get('option_id'), 'BUNDLE_OPTION_POSITION_ID')
    quantity = _decimal(raw['quantity'])
    pending = {key: str(_decimal(raw[key])) for key in (
        'pending_assignment_quantity', 'pending_buy_quantity',
        'pending_exercise_quantity', 'pending_expiration_quantity',
        'pending_sell_quantity')}
    if any(_decimal(value) < 0 for value in pending.values()):
        _fail('BUNDLE_OPTION_POSITION_PENDING', option_id)
    return {
        'asset': 'options',
        'option_id': option_id,
        'chain_id': raw.get('chain_id'),
        'chain_symbol': raw.get('chain_symbol'),
        'expiration_date': raw.get('expiration_date'),
        'type': raw.get('type'),
        'quantity': str(quantity),
        'held': str(sum(_decimal(value) for value in pending.values())),
        'pending': pending,
    }


def _normalize_option_order(raw, observed):
    order_id = _text(raw.get('id'), 'BUNDLE_OPTION_ORDER_ID')
    state = _text(raw.get('state'), 'BUNDLE_OPTION_ORDER_STATE')
    if any(key not in raw for key in ('quantity', 'processed_quantity',
                                      'pending_quantity')):
        _fail('BUNDLE_OPTION_ORDER_SCHEMA', order_id)
    quantity = _decimal(raw['quantity'])
    filled = _decimal(raw['processed_quantity'])
    pending = _decimal(raw['pending_quantity'])
    if quantity < 0 or filled < 0 or pending < 0 or filled + pending > quantity:
        _fail('BUNDLE_OPTION_ORDER_QUANTITY', order_id)
    legs = raw.get('legs')
    if not isinstance(legs, list) or not legs:
        _fail('BUNDLE_OPTION_ORDER_LEGS', order_id)
    normalized_legs = []
    leg_ids = set()
    execution_ids = set()
    for leg in legs:
        if not isinstance(leg, dict):
            _fail('BUNDLE_OPTION_ORDER_LEG', order_id)
        leg_id = _text(leg.get('id'), 'BUNDLE_OPTION_LEG_ID')
        if leg_id in leg_ids:
            _fail('BUNDLE_OPTION_LEG_ID_DUPLICATE', order_id)
        leg_ids.add(leg_id)
        side = _text(leg.get('side'), 'BUNDLE_OPTION_LEG_SIDE')
        if side not in {'buy', 'sell'}:
            _fail('BUNDLE_OPTION_LEG_SIDE', order_id)
        effect = _text(leg.get('position_effect'), 'BUNDLE_OPTION_LEG_EFFECT')
        if effect not in {'open', 'close'}:
            _fail('BUNDLE_OPTION_LEG_EFFECT', order_id)
        if 'ratio_quantity' not in leg:
            _fail('BUNDLE_OPTION_LEG_RATIO', order_id)
        ratio = _decimal(leg['ratio_quantity'])
        if ratio <= 0 or ratio != ratio.to_integral_value():
            _fail('BUNDLE_OPTION_LEG_RATIO', order_id)
        source_executions = leg.get('executions')
        if source_executions is None:
            source_executions = []
        if not isinstance(source_executions, list):
            _fail('BUNDLE_OPTION_ORDER_EXECUTIONS', order_id)
        executions = []
        leg_total = Decimal(0)
        for execution in source_executions:
            if not isinstance(execution, dict):
                _fail('BUNDLE_OPTION_ORDER_EXECUTION', order_id)
            execution_id = _text(execution.get('id'),
                                 'BUNDLE_OPTION_EXECUTION_ID')
            if execution_id in execution_ids:
                _fail('BUNDLE_OPTION_EXECUTION_DUPLICATE', order_id)
            execution_ids.add(execution_id)
            execution_quantity = _decimal(execution.get('quantity'))
            execution_price = _decimal(execution.get('price'))
            if execution_quantity <= 0 or execution_price <= 0:
                _fail('BUNDLE_OPTION_EXECUTION_INVALID', order_id)
            when = _time(execution.get('timestamp'))
            if when > observed:
                _fail('BUNDLE_BROKER_TIME_FUTURE', order_id)
            leg_total += execution_quantity
            executions.append({
                'id': execution_id,
                'quantity': str(execution_quantity),
                'price': str(execution_price),
                'executed_at': when.astimezone(timezone.utc).isoformat(),
            })
        # processed_quantity is in order units; each leg's execution quantity
        # is per-unit quantity multiplied by the leg ratio.
        if leg_total != filled * ratio:
            _fail('BUNDLE_OPTION_ORDER_EXECUTIONS_INCOMPLETE', order_id)
        normalized_legs.append({
            'id': leg_id,
            'option_id': leg.get('option_id'),
            'side': side,
            'position_effect': effect,
            'ratio_quantity': str(ratio),
            'executions': executions,
        })
    return {'id': order_id, 'asset': 'options', 'state': state,
            'quantity': str(quantity), 'filled': str(filled),
            'pending': str(pending), 'legs': normalized_legs}


def _normalize_crypto_position(raw):
    currency = raw.get('currency')
    if not isinstance(currency, dict) or not _text(
            currency.get('code'), 'BUNDLE_CRYPTO_CURRENCY'):
        _fail('BUNDLE_CRYPTO_POSITION_SCHEMA')
    for key in ('quantity', 'quantity_held_for_sell',
                'quantity_transferable'):
        if key not in raw:
            _fail('BUNDLE_CRYPTO_POSITION_SCHEMA', key)
    code = _text(currency['code'], 'BUNDLE_CRYPTO_CURRENCY')
    return {
        'asset': 'crypto', 'symbol': code,
        'currency': code, 'quantity': str(_decimal(raw['quantity'])),
        'held': str(_decimal(raw['quantity_held_for_sell'])),
        'quantity_transferable': str(_decimal(raw['quantity_transferable'])),
    }


def _normalize_option_orders_page(page, observed):
    return [_normalize_option_order(raw, observed)
            for raw in _rows(page['data'], 'orders')]


def _normalize_equity_orders_page(page, observed):
    return [_normalize_equity_order(raw, observed)
            for raw in _rows(page['data'], 'orders')]


def _normalize_crypto_positions_page(page):
    return [_normalize_crypto_position(raw)
            for raw in _rows(page['data'], 'results')]


def _empty_crypto_orders(page):
    data = page['data']
    if not isinstance(data, dict) or 'results' not in data:
        _fail('BUNDLE_SCHEMA_UNVERIFIED:get_crypto_orders')
    results = data.get('results')
    if results != [] or _next(data) is not None:
        _fail('BUNDLE_SCHEMA_UNVERIFIED:get_crypto_orders')
    keys = set(data)
    legacy = {'results', 'next'}
    observed = {'crypto_account_number', 'rhs_account_number',
                'results', 'next'}
    if keys <= legacy:
        return []
    if keys - observed or not {
            'crypto_account_number', 'rhs_account_number'} <= keys:
        _fail('BUNDLE_SCHEMA_UNVERIFIED:get_crypto_orders')
    account = page.get('account') or {}
    discovery_rhs = account.get('rhs_account_number')
    discovery_rhc = account.get('rhc_account_number')
    if not isinstance(discovery_rhs, str) or not discovery_rhs \
            or not isinstance(discovery_rhc, str) or not discovery_rhc:
        _fail('BUNDLE_CRYPTO_IDENTITY_MISSING')
    if (data['rhs_account_number'] != discovery_rhs
            or data['crypto_account_number'] != discovery_rhc):
        _fail('BUNDLE_CRYPTO_IDENTITY_MISMATCH')
    return []


def _empty_approvals(page):
    data = page['data']
    if not isinstance(data, dict) or set(data) - {'approvals', 'next'}:
        _fail('BUNDLE_SCHEMA_UNVERIFIED:get_trade_approvals')
    approvals = data.get('approvals')
    if approvals != [] or _next(data) is not None:
        _fail('BUNDLE_SCHEMA_UNVERIFIED:get_trade_approvals')
    return []


def _normalize_portfolio(page):
    data = page['data']
    if not isinstance(data, dict):
        _fail('BUNDLE_PORTFOLIO_SCHEMA')
    required = ('cash', 'total_value', 'pending_deposits', 'currency',
                'options_value', 'crypto_value', 'futures_value',
                'event_contracts_value', 'fixed_income_value',
                'mutual_funds_value')
    if any(key not in data for key in required):
        _fail('BUNDLE_PORTFOLIO_SCHEMA')
    if data['currency'] != 'USD':
        _fail('BUNDLE_CURRENCY_UNSUPPORTED')
    result = {key: str(_decimal(data[key])) for key in required
              if key not in {'currency'}}
    return result


def _normalize_setting(page):
    data = page['data']
    setting = data.get('setting') if isinstance(data, dict) else None
    if not isinstance(setting, dict):
        _fail('BUNDLE_APPROVAL_SETTING_SCHEMA')
    return {'account_number': setting.get('account_number'),
            'human_must_approve_trades': setting.get(
                'human_must_approve_trades')}


def _normalize_tool_page(page):
    tool = page['tool']
    observed = page['observed_at']
    if tool == 'get_accounts':
        return _normalize_account_page(page)
    if tool == 'get_equity_positions':
        return _normalize_equity_positions(page)
    if tool == 'get_equity_orders':
        return _normalize_equity_orders_page(page, observed)
    if tool == 'get_advanced_orders':
        raise AssertionError('advanced handled with detail context')
    if tool == 'get_option_positions':
        if page['args'].get('nonzero') is True:
            _fail('BUNDLE_NONZERO_FILTER_UNSUPPORTED')
        return [_normalize_option_position(raw)
                for raw in _rows(page['data'], 'positions')]
    if tool == 'get_option_orders':
        return _normalize_option_orders_page(page, observed)
    if tool == 'get_crypto_positions':
        return _normalize_crypto_positions_page(page)
    if tool == 'get_crypto_orders':
        return _empty_crypto_orders(page)
    if tool == 'get_trade_approvals':
        return _empty_approvals(page)
    if tool == 'get_portfolio':
        return _normalize_portfolio(page)
    if tool == 'get_trade_approval_setting':
        return _normalize_setting(page)
    _fail('BUNDLE_TOOL_UNSUPPORTED', tool)


def _load_bundle(root):
    root = _private_dir(root)
    manifest_path = root / 'manifest.json'
    raw_dir = root / 'raw'
    raw_dir_resolved = raw_dir.resolve()
    if raw_dir.is_symlink() or not raw_dir.is_dir():
        _fail('BUNDLE_RAW_DIRECTORY_INVALID')
    if stat.S_IMODE(raw_dir.stat().st_mode) & 0o077:
        _fail('BUNDLE_PRIVATE_DIRECTORY_REQUIRED')
    raw_bytes = _read_private(manifest_path, MAX_MANIFEST_BYTES,
                              'BUNDLE_MANIFEST_INVALID')
    try:
        manifest = json.loads(raw_bytes)
    except (TypeError, ValueError):
        _fail('BUNDLE_MANIFEST_JSON_INVALID')
    if not isinstance(manifest, dict):
        _fail('BUNDLE_MANIFEST_SCHEMA')
    if set(manifest) != {'schema_version', 'provider', 'account_suffix', 'pages'} \
            or manifest.get('schema_version') != 1 \
            or manifest.get('provider') != 'robinhood' \
            or not isinstance(manifest.get('account_suffix'), str) \
            or not SUFFIX_RE.fullmatch(manifest['account_suffix']) \
            or not isinstance(manifest.get('pages'), list):
        _fail('BUNDLE_MANIFEST_SCHEMA')
    raw_files = {}
    total = 0
    try:
        entries = list(os.scandir(raw_dir))
    except OSError:
        _fail('BUNDLE_RAW_DIRECTORY_INVALID')
    for entry in entries:
        path = Path(entry.path)
        if entry.is_symlink() or not entry.is_file() or not entry.name.endswith('.raw'):
            _fail('BUNDLE_RAW_EXTRA', entry.name)
        data = _read_private(path, MAX_RAW_BYTES, 'BUNDLE_RAW_INVALID')
        total += len(data)
        if total > MAX_TOTAL_RAW_BYTES:
            _fail('BUNDLE_RAW_TOTAL_LIMIT')
        digest_value = hashlib.sha256(data).hexdigest()
        if digest_value in raw_files:
            _fail('BUNDLE_RAW_DUPLICATE', entry.name)
        raw_files[entry.name] = {'bytes': data, 'sha256': digest_value}
    pages = []
    referenced = set()
    seen_page = set()
    for index, raw_page in enumerate(manifest['pages']):
        if not isinstance(raw_page, dict) or set(raw_page) - PAGE_KEYS:
            _fail('BUNDLE_PAGE_SCHEMA', str(index))
        tool = raw_page.get('tool')
        if tool not in ALLOWED_ARGS:
            _fail('BUNDLE_TOOL_UNSUPPORTED', str(tool))
        args = raw_page.get('args')
        if not isinstance(args, dict):
            _fail('BUNDLE_REQUEST_ARGS_INVALID', tool)
        pass_number = raw_page.get('pass')
        if pass_number not in (1, 2):
            _fail('BUNDLE_PASS_INVALID', tool)
        page_index = raw_page.get('page_index')
        if type(page_index) is not int or page_index < 0:
            _fail('BUNDLE_PAGE_INDEX', tool)
        identity = (tool, pass_number, page_index)
        if identity in seen_page:
            _fail('BUNDLE_PAGE_DUPLICATE', str(identity))
        seen_page.add(identity)
        cursor_requested = raw_page.get('cursor_requested')
        cursor_returned = raw_page.get('cursor_returned')
        if cursor_requested not in (None, '') and not isinstance(cursor_requested, str):
            _fail('BUNDLE_CURSOR_INVALID')
        if cursor_returned is not None and not isinstance(cursor_returned, str):
            _fail('BUNDLE_CURSOR_INVALID')
        started = _time(raw_page.get('started_at'))
        completed = _time(raw_page.get('completed_at'))
        if completed < started:
            _fail('BUNDLE_PAGE_TIME_INVALID', tool)
        raw_path = raw_page.get('raw_path')
        if not isinstance(raw_path, str) or not raw_path.startswith('raw/'):
            _fail('BUNDLE_RAW_PATH_ESCAPE')
        name = raw_path[len('raw/'):]
        if not name or '/' in name or '..' in name or Path(name).name != name:
            _fail('BUNDLE_RAW_PATH_ESCAPE')
        if name not in raw_files:
            _fail('BUNDLE_RAW_MISSING', raw_path)
        referenced.add(name)
        raw_info = raw_files[name]
        if raw_page.get('raw_bytes') != len(raw_info['bytes']) \
                or raw_page.get('raw_sha256') != raw_info['sha256']:
            _fail('BUNDLE_RAW_HASH_MISMATCH', raw_path)
        page = dict(raw_page)
        page['page_index'] = page_index
        page['pass'] = pass_number
        page['query_scope'] = _query_scope(tool, args)
        page['cursor_requested'] = cursor_requested or None
        page['cursor_returned'] = cursor_returned
        page['started_at_value'] = started
        page['completed_at_value'] = completed
        page['data'] = _page_data(page, raw_info['bytes'])
        page['raw_sha256_actual'] = raw_info['sha256']
        page['raw_bytes_actual'] = len(raw_info['bytes'])
        page['args_sha256'] = digest(args)
        if raw_page.get('request_args_sha256') not in (None, page['args_sha256']):
            _fail('BUNDLE_ARGS_HASH_MISMATCH', tool)
        page['normalized_count_expected'] = raw_page.get('normalized_count')
        page['normalized_sha256_expected'] = raw_page.get('normalized_sha256')
        pages.append(page)
    extra = set(raw_files) - referenced
    if extra:
        _fail('BUNDLE_RAW_EXTRA', ','.join(sorted(extra)))
    loaded = {'root': root, 'manifest': manifest, 'pages': pages,
              'raw_files': raw_files,
              'manifest_sha256': hashlib.sha256(raw_bytes).hexdigest()}
    return loaded


def _validate_page_global(loaded):
    pages = loaded['pages']
    if not pages:
        _fail('BUNDLE_PAGES_REQUIRED')
    starts = [page['started_at_value'] for page in pages]
    ends = [page['completed_at_value'] for page in pages]
    started, observed = min(starts), max(ends)
    if any(page['completed_at_value'] < page['started_at_value']
           for page in pages):
        _fail('BUNDLE_PAGE_TIME_INVALID')
    if (observed - started).total_seconds() > MAX_COLLECTION_SECONDS:
        _fail('BUNDLE_COLLECTION_WINDOW_EXCEEDED')
    # For each paged tool pass, verify cursor chain and raw response next.
    for tool in ALLOWED_ARGS:
        for pass_number in (1, 2):
            pages_for = _collection_pages(loaded, tool, pass_number)
            if not pages_for:
                continue
            chain_pages = (_full_pages(loaded, tool, pass_number)
                           if tool == 'get_equity_orders' else pages_for)
            if chain_pages:
                _validate_page_chain_pages(
                    loaded, tool, pass_number, chain_pages)
            for page in pages_for:
                data_next = _next(page['data'])
                if data_next != page['cursor_returned']:
                    _fail('BUNDLE_CURSOR_CHAIN', tool)
    # Per-tool page timing must follow cursor order; the second pass must start
    # only after the first pass has completely finished.
    for tool in ALLOWED_ARGS:
        for pass_number in (1, 2):
            pages_for = _collection_pages(loaded, tool, pass_number)
            for previous, current in zip(pages_for, pages_for[1:]):
                if current['started_at_value'] < previous['completed_at_value']:
                    _fail('BUNDLE_PAGE_TIME_ORDER', tool)
    pass1 = [page for page in pages if page['pass'] == 1]
    pass2 = [page for page in pages if page['pass'] == 2]
    if pass2:
        if not pass1:
            _fail('BUNDLE_PASS_TIME_ORDER')
        if min(page['started_at_value'] for page in pass2) \
                < max(page['completed_at_value'] for page in pass1):
            _fail('BUNDLE_PASS_TIME_OVERLAP')
    for page in pages:
        page['observed_at'] = observed
        page['global_started_at'] = started
    return started, observed


def _set_page_normalized(page, value):
    page['normalized'] = value
    page['normalized_count'] = len(value) if isinstance(value, list) else 1
    page['normalized_sha256'] = digest(value)
    if page.get('normalized_count_expected') is not None \
            and page['normalized_count_expected'] != page['normalized_count']:
        _fail('BUNDLE_NORMALIZED_MISMATCH', page['tool'])
    if page.get('normalized_sha256_expected') is not None \
            and page['normalized_sha256_expected'] != page['normalized_sha256']:
        _fail('BUNDLE_NORMALIZED_MISMATCH', page['tool'])
    return value


def _page_records(loaded, tool, pass_number, observed, pages=None):
    pages = _collection_pages(loaded, tool, pass_number) if pages is None else pages
    records = []
    for page in pages:
        page['observed_at'] = observed
        try:
            value = _normalize_tool_page(page)
        except BundleError as exc:
            if exc.code in {
                    'BUNDLE_SCHEMA_UNVERIFIED:get_crypto_orders',
                    'BUNDLE_SCHEMA_UNVERIFIED:get_trade_approvals',
                    'BUNDLE_CRYPTO_IDENTITY_MISSING',
                    'BUNDLE_CRYPTO_IDENTITY_MISMATCH'}:
                page['normalized'] = []
                page['normalized_error'] = exc.code
                return [], exc.code
            raise
        page['normalized'] = value
        _set_page_normalized(page, value)
        records.extend(value)
    return records, None


def _coverage(loaded, started, observed):
    coverage = {}
    for tool in ALLOWED_ARGS:
        entry = {}
        for pass_number in (1, 2):
            pages = _collection_pages(loaded, tool, pass_number)
            entry[f'pass{pass_number}'] = {
                'pages': len(pages),
                'page_indexes': [page['page_index'] for page in pages],
                'query_scope': sorted({page['query_scope'] for page in pages}),
                'records': sum(page.get('normalized_count', 0) for page in pages),
                'raw_sha256': [page['raw_sha256_actual'] for page in pages],
            }
        coverage[tool] = entry
    return coverage


def _merge_records(records, identity, label, blockers):
    merged = {}
    for record in records:
        key = identity(record)
        if not isinstance(key, str) or not key:
            _fail('BUNDLE_IDENTITY_INVALID', label)
        prior = merged.get(key)
        if prior is None:
            merged[key] = record
        elif canonical(prior) != canonical(record):
            blockers.append('IDENTITY_CONFLICT:' + label + ':' + key)
    return list(merged.values())


def _identity_for_tool(tool):
    if tool == 'get_equity_positions':
        return lambda record: record.get('symbol')
    if tool == 'get_option_positions':
        return lambda record: record.get('option_id')
    if tool == 'get_crypto_positions':
        return lambda record: record.get('symbol')
    if tool == 'get_equity_orders':
        return lambda record: record.get('id')
    if tool == 'get_option_orders':
        return lambda record: record.get('id')
    return None


def _normalize_loaded(loaded):
    started, observed = _validate_page_global(loaded)
    blockers = []
    account_pages = _collection_pages(loaded, 'get_accounts', 1)
    if len(account_pages) != 1:
        _fail('BUNDLE_ACCOUNT_PAGE_REQUIRED')
    accounts = _normalize_account_page(account_pages[0])
    _set_page_normalized(account_pages[0], accounts)
    suffix = loaded['manifest']['account_suffix']
    matches = [account for account in accounts
               if isinstance(account, dict)
               and isinstance(account.get('account_number'), str)
               and account['account_number'].endswith(suffix)]
    if len(matches) != 1:
        _fail('BUNDLE_ACCOUNT_UNBOUND')
    raw_account = matches[0]
    if raw_account.get('agentic_allowed') is not True \
            or raw_account.get('deactivated') is not False \
            or raw_account.get('permanently_deactivated') is not False \
            or raw_account.get('state') != 'active':
        _fail('BUNDLE_ACCOUNT_INACTIVE_OR_UNAUTHORIZED')
    account_number = _text(raw_account.get('account_number'),
                           'BUNDLE_ACCOUNT_IDENTIFIER')
    rhs_raw = raw_account.get('rhs_account_number')
    if rhs_raw is None or rhs_raw == '':
        rhs = None
        blockers.append('CRYPTO_RHS_MISSING')
    else:
        rhs = _text(rhs_raw, 'BUNDLE_CRYPTO_ACCOUNT_IDENTIFIER')
    rhc_raw = raw_account.get('rhc_account_number')
    rhc = None if rhc_raw is None or rhc_raw == '' else _text(
        rhc_raw, 'BUNDLE_CRYPTO_ACCOUNT_IDENTIFIER')
    account = {'account_number': account_number,
               'rhs_account_number': rhs,
               'rhc_account_number': rhc,
               'account_key': hashlib.sha256(account_number.encode()).hexdigest(),
               'account_mask': '••••' + suffix}
    for page in loaded['pages']:
        page['account'] = account
        _account_args(page['tool'], page['args'], account)
    # Portfolio and approval setting are single-pass typed tools.
    portfolio_pages = _collection_pages(loaded, 'get_portfolio', 1)
    portfolio = None
    if len(portfolio_pages) != 1:
        blockers.append('MISSING_TOOL:get_portfolio')
    else:
        portfolio = _normalize_portfolio(portfolio_pages[0])
        _set_page_normalized(portfolio_pages[0], portfolio)
    setting_pages = _collection_pages(loaded, 'get_trade_approval_setting', 1)
    setting = None
    if len(setting_pages) != 1:
        blockers.append('MISSING_TOOL:get_trade_approval_setting')
    else:
        setting = _normalize_setting(setting_pages[0])
        _set_page_normalized(setting_pages[0], setting)
        if setting.get('account_number') != account_number:
            _fail('BUNDLE_APPROVAL_SETTING_ACCOUNT')
        if type(setting.get('human_must_approve_trades')) is not bool:
            _fail('BUNDLE_APPROVAL_SETTING_SCHEMA')
    # Paged typed tools.
    normalized = {}
    tool_status = {}
    for tool in TWO_PASS_TOOLS:
        if tool == 'get_advanced_orders':
            continue
        pass_values = []
        for pass_number in (1, 2):
            pages = _collection_pages(loaded, tool, pass_number)
            if not pages:
                blockers.append('TWO_PASS_REQUIRED:' + tool)
                pass_values.append(None)
                continue
            scopes = {page['query_scope'] for page in pages}
            if tool == 'get_equity_orders':
                list_pages = [page for page in pages
                              if page['query_scope'] == 'full']
                if any(page['query_scope'] == 'filtered' for page in pages):
                    blockers.append('QUERY_SCOPE_FILTERED:' + tool +
                                    ':pass' + str(pass_number))
                    pass_values.append(None)
                    continue
            else:
                list_pages = pages
                if scopes != {'full'}:
                    blockers.append('QUERY_SCOPE_PARTIAL:' + tool +
                                    ':pass' + str(pass_number))
                    pass_values.append(None)
                    continue
            if not list_pages:
                blockers.append('QUERY_SCOPE_PARTIAL:' + tool +
                                ':pass' + str(pass_number))
                pass_values.append(None)
                continue
            try:
                if tool in ('get_crypto_orders', 'get_trade_approvals'):
                    values, schema_error = _page_records(
                        loaded, tool, pass_number, observed, pages=list_pages)
                    if schema_error:
                        blockers.append(schema_error + ':pass' + str(pass_number))
                        pass_values.append(None)
                        continue
                else:
                    values, _ = _page_records(
                        loaded, tool, pass_number, observed, pages=list_pages)
            except BundleError as exc:
                blockers.append(exc.code + ':pass' + str(pass_number))
                pass_values.append(None)
                continue
            identity = _identity_for_tool(tool)
            if identity is not None:
                values = _merge_records(values, identity, tool, blockers)
            pass_values.append(values)
        if pass_values[0] is not None and pass_values[1] is not None \
                and canonical(pass_values[0]) != canonical(pass_values[1]):
            blockers.append('TWO_PASS_MISMATCH:' + tool)
        normalized[tool] = pass_values[0]
        tool_status[tool] = ('complete' if pass_values[0] is not None
                             and pass_values[1] is not None
                             and canonical(pass_values[0]) == canonical(pass_values[1])
                             else 'incomplete')
    # Advanced orders need leg details from equity order pages.
    equity_detail = {}
    for pass_number in (1, 2):
        for page in _collection_pages(loaded, 'get_equity_orders', pass_number):
            if page['args'].get('order_id'):
                for raw in _rows(page['data'], 'orders'):
                    equity_detail[pass_number, raw.get('id')] = raw
    advanced = {}
    for pass_number in (1, 2):
        groups = []
        orders = []
        pages = _collection_pages(loaded, 'get_advanced_orders', pass_number)
        if not pages:
            advanced[pass_number] = None
            continue
        if {page['query_scope'] for page in pages} != {'full'}:
            blockers.append('QUERY_SCOPE_PARTIAL:get_advanced_orders:pass' +
                            str(pass_number))
            advanced[pass_number] = None
            continue
        detail = {key[1]: value for key, value in equity_detail.items()
                  if key[0] == pass_number}
        try:
            for page in pages:
                page['observed_at'] = observed
                page_groups, page_orders = _normalize_advanced_page(
                    page, observed, detail)
                groups.extend(page_groups)
                orders.extend(page_orders)
                _set_page_normalized(page, {'groups': page_groups,
                                            'orders': page_orders})
        except BundleError as exc:
            blockers.append(exc.code + ':pass' + str(pass_number))
            advanced[pass_number] = None
            continue
        groups = _merge_records(groups, lambda group: group.get('id'),
                                'get_advanced_orders:groups', blockers)
        orders = _merge_records(orders, lambda order: order.get('id'),
                                'get_advanced_orders:orders', blockers)
        advanced[pass_number] = {'groups': groups, 'orders': orders}
    if advanced.get(1) is None or advanced.get(2) is None:
        blockers.append('TWO_PASS_REQUIRED:get_advanced_orders')
    if advanced.get(1) is not None and advanced.get(2) is not None \
            and canonical(advanced[1]) != canonical(advanced[2]):
        blockers.append('TWO_PASS_MISMATCH:get_advanced_orders')
    normalized['get_advanced_orders'] = advanced.get(1)
    tool_status['get_advanced_orders'] = (
        'complete' if advanced.get(1) is not None and advanced.get(2) is not None
        and canonical(advanced[1]) == canonical(advanced[2]) else 'incomplete')
    # Account/portfolio/setting coverage.
    coverage = _coverage(loaded, started, observed)
    for tool in ('get_accounts', 'get_portfolio', 'get_trade_approval_setting'):
        pages = _collection_pages(loaded, tool, 1)
        coverage[tool]['status'] = ('complete' if len(pages) == 1 else 'missing')
    for tool in TWO_PASS_TOOLS:
        coverage[tool]['status'] = tool_status.get(tool, 'missing')
    # Required missing capabilities are blockers, never [] or INAPPLICABLE.
    for scope in UNSUPPORTED_SCOPES:
        blockers.append('GETTER_MISSING:' + scope + '_positions')
        blockers.append('GETTER_MISSING:' + scope + '_orders')
        blockers.append('CONDITIONS_MISSING:' + scope + '_positions')
        blockers.append('CONDITIONS_MISSING:' + scope + '_orders')
    blockers.append('SETTLEMENT_SOURCE_MISSING')
    blockers.append('CONDITIONS_MISSING:SETTLEMENT_SOURCE_MISSING')
    if normalized.get('get_crypto_orders') is None:
        blockers.append('SCHEMA_UNVERIFIED:get_crypto_orders')
    if normalized.get('get_trade_approvals') is None:
        blockers.append('SCHEMA_UNVERIFIED:get_trade_approvals')
    # Actual positions/orders that are not flat or complete.
    equity_positions_observed = normalized.get('get_equity_positions')
    equity_positions = equity_positions_observed or []
    for position in equity_positions:
        values = [position.get('quantity', '0'),
                  position.get('available_for_sells', '0'),
                  position.get('held_for_sells', '0'),
                  position.get('shares_held_for_asset_transfer', '0'),
                  position.get('shares_held_for_options_events', '0'),
                  position.get('shares_held_for_stock_grants', '0'),
                  position.get('shares_pending_from_options_events', '0')]
        if any(_decimal(value) != 0 for value in values):
            blockers.append('NONFLAT_POSITION:equity:' + position['symbol'])
    option_positions_observed = normalized.get('get_option_positions')
    option_positions = option_positions_observed or []
    for position in option_positions:
        if _decimal(position['quantity']) != 0 \
                or any(_decimal(value) != 0 for value in position['pending'].values()):
            blockers.append('NONFLAT_POSITION:options:' + position['option_id'])
    crypto_positions_observed = normalized.get('get_crypto_positions')
    crypto_positions = crypto_positions_observed or []
    for position in crypto_positions:
        if _decimal(position['quantity']) != 0 \
                or _decimal(position['held']) != 0:
            blockers.append('NONFLAT_POSITION:crypto:' + position['symbol'])
    option_orders_observed = normalized.get('get_option_orders')
    option_orders = option_orders_observed or []
    equity_orders_observed = normalized.get('get_equity_orders')
    equity_orders = equity_orders_observed or []
    advanced_observed = normalized.get('get_advanced_orders')
    advanced_value = advanced_observed or {'groups': [], 'orders': []}
    for order in equity_orders + (advanced_value.get('orders') or []):
        if order.get('quantity') is None:
            blockers.append('UNPROVEN_ORDER_QUANTITY:' + order['id'])
        if str(order.get('state')).lower() not in {'filled', 'cancelled',
                                                   'canceled', 'rejected',
                                                   'expired'}:
            blockers.append('OPEN_ORDER:equity:' + order['id'])
    for group in advanced_value.get('groups') or []:
        if str(group.get('state')).lower() not in {'filled', 'cancelled',
                                                   'canceled', 'rejected',
                                                   'expired'}:
            blockers.append('OPEN_ORDER:advanced_group:' + group['id'])
    all_equity_orders = list(equity_orders) + list(advanced_value.get('orders') or [])
    seen_orders = {}
    for order in all_equity_orders:
        prior = seen_orders.get(order['id'])
        if prior is None:
            seen_orders[order['id']] = order
        elif canonical(prior) != canonical(order):
            blockers.append('IDENTITY_CONFLICT:equity_order:' + order['id'])
    for order in option_orders:
        if str(order['state']).lower() not in {'filled', 'cancelled', 'canceled',
                                               'rejected', 'expired'}:
            blockers.append('OPEN_ORDER:options:' + order['id'])
    if normalized.get('get_trade_approvals') is not None \
            and normalized['get_trade_approvals']:
        blockers.append('APPROVALS_UNVERIFIED')
    # Portfolio aggregate consistency is auxiliary only.
    if portfolio is not None:
        for key, value in portfolio.items():
            if key.startswith(('options_', 'crypto_', 'futures_',
                               'event_contracts_', 'fixed_income_',
                               'mutual_funds_')) and _decimal(value) != 0:
                blockers.append('AGGREGATE_NONZERO:' + key)
    # Desired scope completeness; missing data never becomes empty lists.
    available_scopes = set()
    if tool_status.get('get_option_positions') == 'complete' \
            and tool_status.get('get_option_orders') == 'complete':
        available_scopes.add('options')
    if tool_status.get('get_crypto_positions') == 'complete' \
            and tool_status.get('get_crypto_orders') == 'complete':
        available_scopes.add('crypto')
    for scope in UNSUPPORTED_SCOPES:
        if scope not in available_scopes:
            blockers.append('SCOPE_UNAVAILABLE:' + scope)
    candidate = None
    required_supported = (
        'get_equity_positions', 'get_equity_orders', 'get_advanced_orders',
        'get_option_positions', 'get_option_orders', 'get_crypto_positions',
        'get_crypto_orders', 'get_trade_approvals')
    if account_pages and portfolio is not None \
            and all(tool_status.get(tool) == 'complete'
                    for tool in required_supported) \
            and normalized.get('get_equity_positions') is not None \
            and normalized.get('get_equity_orders') is not None \
            and normalized.get('get_trade_approvals') == []:
        candidate = _candidate_snapshot(
            account, loaded, started, observed, portfolio, setting,
            normalized, available_scopes)
    raw_chain = _raw_chain(loaded)
    result = {
        'schema_version': 1, 'kind': 'saved_readonly_bundle_adaptation',
        'provider': 'robinhood', 'account_suffix': suffix,
        'account': account, 'started_at': started.isoformat(),
        'observed_at': observed.isoformat(),
        'collection_window_seconds': (observed - started).total_seconds(),
        'raw_chain_sha256': raw_chain, 'raw_chain': raw_chain,
        'pages': [_page_receipt(page) for page in loaded['pages']],
        'coverage': coverage, 'capability_matrix': dict(CAPABILITY_MATRIX),
        'observations': {
            'equity_positions': equity_positions_observed,
            'equity_orders': equity_orders_observed,
            'advanced': advanced_observed,
            'option_positions': option_positions_observed,
            'option_orders': option_orders_observed,
            'crypto_positions': crypto_positions_observed,
            'crypto_orders': normalized.get('get_crypto_orders'),
            'approvals': normalized.get('get_trade_approvals'),
            'portfolio': portfolio,
            'approval_setting': setting,
        },
        'blockers': sorted(set(blockers)),
        'candidate_snapshot': candidate,
        'baseline_snapshot': None,
        'ready': False,
        'production_ready': False,
    }
    return result


def _identity_account(loaded):
    """Bind a bundle to exactly one active/agentic account without exposing it."""
    pages = _collection_pages(loaded, 'get_accounts', 1)
    if len(pages) != 1:
        _fail('BUNDLE_ACCOUNT_PAGE_REQUIRED')
    accounts = _normalize_account_page(pages[0])
    _set_page_normalized(pages[0], accounts)
    suffix = loaded['manifest']['account_suffix']
    matches = [account for account in accounts
               if isinstance(account, dict)
               and isinstance(account.get('account_number'), str)
               and account['account_number'].endswith(suffix)]
    if len(matches) != 1:
        _fail('BUNDLE_ACCOUNT_UNBOUND')
    raw_account = matches[0]
    if raw_account.get('agentic_allowed') is not True \
            or raw_account.get('deactivated') is not False \
            or raw_account.get('permanently_deactivated') is not False \
            or raw_account.get('state') != 'active':
        _fail('BUNDLE_ACCOUNT_INACTIVE_OR_UNAUTHORIZED')
    account_number = _text(raw_account.get('account_number'),
                           'BUNDLE_ACCOUNT_IDENTIFIER')
    rhs_raw = raw_account.get('rhs_account_number')
    rhc_raw = raw_account.get('rhc_account_number')
    account = {
        'account_number': account_number,
        'rhs_account_number': (None if rhs_raw in (None, '') else
                               _text(rhs_raw, 'BUNDLE_CRYPTO_ACCOUNT_IDENTIFIER')),
        'rhc_account_number': (None if rhc_raw in (None, '') else
                               _text(rhc_raw, 'BUNDLE_CRYPTO_ACCOUNT_IDENTIFIER')),
        'account_key': hashlib.sha256(account_number.encode()).hexdigest(),
        'account_mask': '••••' + suffix,
        'affiliate': raw_account.get('affiliate'),
        'type': raw_account.get('type'),
        'brokerage_account_type': raw_account.get('brokerage_account_type'),
        'agentic_allowed': raw_account.get('agentic_allowed'),
        'is_default': raw_account.get('is_default'),
        'state': raw_account.get('state'),
        'option_level': raw_account.get('option_level'),
        'unsettled_funds': raw_account.get('unsettled_funds'),
    }
    for page in loaded['pages']:
        page['account'] = account
        _account_args(page['tool'], page['args'], account)
    return account


def _identity_orders(loaded, observed, blockers):
    """Return exact terminal orders from two complete, full list-mode passes."""
    by_pass = []
    for pass_number in (1, 2):
        pages = _collection_pages(loaded, 'get_equity_orders', pass_number)
        if not pages:
            blockers.append('TWO_PASS_REQUIRED:get_equity_orders')
            by_pass.append(None)
            continue
        if any(page['query_scope'] != 'full' for page in pages):
            blockers.append('QUERY_SCOPE_PARTIAL:get_equity_orders:pass' +
                            str(pass_number))
            by_pass.append(None)
            continue
        values, schema_error = _page_records(
            loaded, 'get_equity_orders', pass_number, observed, pages=pages)
        if schema_error:
            blockers.append(schema_error + ':pass' + str(pass_number))
            by_pass.append(None)
            continue
        values = _merge_records(
            values, _identity_for_tool('get_equity_orders'),
            'get_equity_orders', blockers)
        by_pass.append(values)
    if by_pass[0] is None or by_pass[1] is None:
        return None
    if canonical(by_pass[0]) != canonical(by_pass[1]):
        blockers.append('TWO_PASS_MISMATCH:get_equity_orders')
        return None
    return by_pass[0]


def normalize_order_identity(bundle, *, now=None):
    """Normalize only exact terminal equity order/execution identity.

    This is a deliberately narrow historical view. It never builds a full
    account snapshot, portfolio balance, settlement list, fill/budget/risk
    fact, or production-ready baseline. Missing/unsupported scopes and any
    nonterminal or inconsistent identity remain blockers, never empty lists.
    """
    if not isinstance(bundle, dict) or 'pages' not in bundle:
        bundle = _load_bundle(bundle)
    elif 'root' in bundle:
        bundle = _load_bundle(bundle['root'])
    else:
        _fail('BUNDLE_LOADED_UNSEALED')
    started, observed = _validate_page_global(bundle)
    account = _identity_account(bundle)
    blockers = []
    orders = _identity_orders(bundle, observed, blockers)
    if orders is None:
        return {
            'schema_version': 1, 'kind': IDENTITY_KIND,
            'baseline_scope': IDENTITY_BASELINE_SCOPE,
            'provider': 'robinhood',
            'account': {'account_key': account['account_key'],
                        'account_mask': account['account_mask']},
            'account_key': account['account_key'],
            'account_mask': account['account_mask'],
            'observed_at': observed.isoformat(),
            'collection_window_seconds': (observed - started).total_seconds(),
            'source': {
                'bundle_manifest_sha256': bundle['manifest_sha256'],
                'raw_chain_sha256': _raw_chain(bundle),
                'tool': 'get_equity_orders',
                'passes': [1, 2],
                'query_scope': 'full',
            },
            'orders': [], 'executions': [],
            'claims': dict(IDENTITY_CLAIMS),
            'blockers': sorted(set(blockers)),
            'ready': False, 'production_ready': False,
        }
    if not orders:
        blockers.append('TERMINAL_ORDERS_REQUIRED')
    refs = set()
    execution_ids = set()
    executions = []
    for order in orders:
        state = str(order.get('state') or '').lower()
        if state not in IDENTITY_TERMINAL_STATES:
            blockers.append('ORDER_NOT_TERMINAL:' + str(order.get('id')))
        if order.get('quantity') is None:
            blockers.append('ORDER_QUANTITY_MISSING:' + str(order.get('id')))
        ref_id = order.get('ref_id')
        if ref_id is not None:
            if not isinstance(ref_id, str) or not ref_id:
                blockers.append('ORDER_REF_INVALID:' + str(order.get('id')))
            elif ref_id in refs:
                blockers.append('ORDER_REF_DUPLICATE:' + str(order.get('id')))
            else:
                refs.add(ref_id)
        for execution in order.get('executions') or ():
            execution_id = execution.get('id')
            if not isinstance(execution_id, str) or not execution_id:
                blockers.append('EXECUTION_ID_INVALID:' + str(order.get('id')))
            elif execution_id in execution_ids:
                blockers.append('EXECUTION_ID_DUPLICATE:' + str(execution_id))
            else:
                execution_ids.add(execution_id)
            executions.append(execution)
    result = {
        'schema_version': 1, 'kind': IDENTITY_KIND,
        'baseline_scope': IDENTITY_BASELINE_SCOPE,
        'provider': 'robinhood',
        'account': {'account_key': account['account_key'],
                    'account_mask': account['account_mask']},
        'account_key': account['account_key'],
        'account_mask': account['account_mask'],
        'observed_at': observed.isoformat(),
        'collection_window_seconds': (observed - started).total_seconds(),
        'source': {
            'bundle_manifest_sha256': bundle['manifest_sha256'],
            'raw_chain_sha256': _raw_chain(bundle),
            'tool': 'get_equity_orders',
            'passes': [1, 2],
            'query_scope': 'full',
        },
        'orders': list(orders),
        'executions': executions,
        'claims': dict(IDENTITY_CLAIMS),
        'blockers': sorted(set(blockers)),
        'ready': False, 'production_ready': False,
    }
    return result


def _page_receipt(page):
    return {
        'tool': page['tool'], 'pass': page['pass'],
        'page_index': page['page_index'], 'args_sha256': page['args_sha256'],
        'account_binding': (None if page['tool'] == 'get_accounts' else
                            ('rhs_account_number'
                             if page['tool'] in {'get_crypto_positions',
                                                 'get_crypto_orders'}
                             else 'account_number')),
        'cursor_requested': page['cursor_requested'],
        'cursor_returned': page['cursor_returned'],
        'started_at': page['started_at_value'].isoformat(),
        'completed_at': page['completed_at_value'].isoformat(),
        'raw_path': page['raw_path'],
        'raw_sha256': page['raw_sha256_actual'],
        'raw_bytes': page['raw_bytes_actual'],
        'normalized_count': page.get('normalized_count', 0),
        'normalized_sha256': page.get('normalized_sha256'),
    }


def _raw_chain(loaded):
    receipts = [_page_receipt(page) for page in sorted(
        loaded['pages'], key=lambda p: (p['tool'], p['pass'], p['page_index']))]
    return digest(receipts)


def _candidate_snapshot(account, loaded, started, observed, portfolio, setting,
                        normalized, available_scopes):
    """Build a diagnostic snapshot; never a production-usable one.

    Unsupported scopes are omitted rather than filled with empty lists. The
    baseline core therefore rejects it with CONDITIONS_MISSING.
    """
    positions = {position['symbol']: position
                 for position in (normalized.get('get_equity_positions') or [])}
    orders = {order['id']: order
              for order in (normalized.get('get_equity_orders') or [])}
    advanced = normalized.get('get_advanced_orders') or {'groups': [], 'orders': []}
    for order in advanced.get('orders') or []:
        orders.setdefault(order['id'], order)
    other_positions = {}
    other_orders = {}
    if 'options' in available_scopes:
        other_positions['options'] = normalized.get('get_option_positions') or []
        other_orders['options'] = normalized.get('get_option_orders') or []
    if 'crypto' in available_scopes:
        other_positions['crypto'] = normalized.get('get_crypto_positions') or []
        other_orders['crypto'] = []
    asset_scopes = {}
    position_tool = {'options': 'get_option_positions',
                     'crypto': 'get_crypto_positions'}
    order_tool = {'options': 'get_option_orders',
                  'crypto': 'get_crypto_orders'}
    for scope in available_scopes:
        asset_scopes[scope] = {
            'positions': {'pagination': {
                'complete': True,
                'pages': len(_collection_pages(
                    loaded, position_tool[scope], 1)),
                'source': 'saved_official_bundle',
                'observed_at': observed.isoformat()},
                'record_sha256': digest(other_positions.get(scope, [])),
                'record_count': len(other_positions.get(scope, []))},
            'orders': {'pagination': {
                'complete': True,
                'pages': len(_collection_pages(
                    loaded, order_tool[scope], 1)),
                'source': 'saved_official_bundle',
                'observed_at': observed.isoformat()},
                'record_sha256': digest(other_orders.get(scope, [])),
                'record_count': len(other_orders.get(scope, []))},
        }
    provenance = {
        'collection_source': 'saved_official_tool_response_bundle_v1',
        'positions_complete': True, 'positions_pagination_complete': True,
        'orders_complete': True, 'executions_complete': True,
        'approvals_complete': normalized.get('get_trade_approvals') is not None,
        'asset_scopes': asset_scopes,
        'snapshot_time_source': 'collector_completed_at_utc',
        'execution_time_source': 'broker_execution_timestamp_utc',
    }
    return {
        'schema_version': 1, 'provider': 'robinhood', 'synthetic': False,
        'account_key': account['account_key'], 'account_mask': account['account_mask'],
        'started_at': started.isoformat(), 'observed_at': observed.isoformat(),
        'collection_complete': False,
        'positions': positions, 'orders': orders,
        'groups': advanced.get('groups') or [],
        'other_asset_open_orders': sum(
            1 for order in (normalized.get('get_option_orders') or [])
            if str(order.get('state')).lower() not in {'filled', 'cancelled',
                                                       'canceled', 'rejected',
                                                       'expired'}),
        'other_asset_values': {
            key: portfolio[key] for key in (
                'options_value', 'crypto_value', 'futures_value',
                'event_contracts_value', 'fixed_income_value',
                'mutual_funds_value')} if portfolio else {},
        'other_asset_positions': other_positions,
        'other_asset_orders': other_orders,
        # No settlement getter exists; do not fabricate six empty lists.
        'other_asset_pending_settlements': {},
        'portfolio': {key: portfolio[key] for key in
                      ('cash', 'total_value', 'pending_deposits')}
                     if portfolio else {},
        'pending_approvals': 0,
        'pending_approval_ids': [],
        'human_must_approve_trades': setting.get('human_must_approve_trades')
                                     if setting else None,
        'provenance': provenance,
        'diagnostic_only': True,
        'production_ready': False,
    }


def load_saved_bundle(root):
    """Load/validate raw bundle bytes without writing or querying a database."""
    return _load_bundle(root)


def normalize_bundle(bundle, *, now=None):
    """Normalize a loaded bundle. Raises BundleError on structural corruption.

    A loaded mapping is never trusted for derived page data: if it came from
    load_saved_bundle it is re-read from the controlled root, including
    manifest/args/cursors/raw bytes.  A detached mapping has no trusted raw
    backing and is rejected.
    """
    if not isinstance(bundle, dict) or 'pages' not in bundle:
        bundle = _load_bundle(bundle)
    elif 'root' in bundle:
        bundle = _load_bundle(bundle['root'])
    else:
        _fail('BUNDLE_LOADED_UNSEALED')
    result = _normalize_loaded(bundle)
    now_value = now or datetime.now(timezone.utc)
    if not isinstance(now_value, datetime) or now_value.tzinfo is None:
        _fail('BUNDLE_CLOCK_INVALID')
    now_value = now_value.astimezone(timezone.utc)
    observed = timestamp(result['observed_at'])
    if observed > now_value:
        _fail('BUNDLE_FUTURE_OBSERVATION')
    if (now_value - observed).total_seconds() > MAX_COLLECTION_SECONDS:
        _fail('BUNDLE_STALE_OBSERVATION')
    for page in result['pages']:
        if timestamp(page['completed_at']) > now_value:
            _fail('BUNDLE_FUTURE_PAGE')
    return result


def adapt_saved_bundle(root, *, now=None):
    """Load and normalize, converting structural failures to diagnostics."""
    try:
        loaded = load_saved_bundle(root)
        return normalize_bundle(loaded, now=now)
    except BundleError as exc:
        return {
            'schema_version': 1, 'kind': 'saved_readonly_bundle_adaptation',
            'provider': None, 'account': None, 'pages': [],
            'coverage': {}, 'capability_matrix': dict(CAPABILITY_MATRIX),
            'blockers': [exc.code], 'blocker_detail': exc.detail,
            'observations': {}, 'candidate_snapshot': None,
            'baseline_snapshot': None, 'ready': False,
            'production_ready': False,
        }
