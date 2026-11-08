"""Pure read-only current account/product evidence compiler.

This module compiles two independent observations from a saved official
response bundle and a pinned public-product source set:

* ``AccountScopeApplicability v1`` — what the current observation actually
  proves, per asset scope, without promoting unknown scopes to empty/N/A;
* ``FundsAvailability v1`` — current portfolio/account money facts, without
  claiming a settlement ledger or spendability authority.

It never opens a broker, market, MCP, Host, risk sidecar, lineage or execution
DB. It never authorizes an entry, applies a baseline, clears quarantine, or
turns public product documentation into account-specific historical facts.
"""
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path
import stat

from trading_contracts.values import canonical, digest, timestamp

from .baseline_evidence import (
    BundleError, _account_args, _collection_pages, _identity_account,
    _merge_records, _normalize_account_page, _normalize_advanced_page,
    _normalize_portfolio, _normalize_setting, _page_records,
    _set_page_normalized, _validate_page_global, _identity_for_tool,
    _raw_chain, load_saved_bundle)

SCHEMA_VERSION = 1
ACCOUNT_SCOPE_KIND = 'account_scope_applicability'
FUNDS_KIND = 'funds_availability'
PUBLIC_POLICY_VERSION = 'public-product-scope-v1'
MAX_PUBLIC_SOURCE_BYTES = 4 * 1024 * 1024
MAX_CURRENT_EVIDENCE_AGE_SECONDS = 30
PUBLIC_SOURCE_MAX_AGE = timedelta(days=30)
TERMINAL_ORDER_STATES = frozenset({
    'filled', 'cancelled', 'canceled', 'rejected', 'failed', 'voided',
    'expired'})

# Fixed, reviewed official source set for this policy version. A different
# URL/hash must be a new reviewed policy version, not caller-supplied trust.
PUBLIC_SOURCES = {
    'investment-products': {
        'url': 'https://robinhood.com/us/en/support/articles/investments-you-can-make-on-robinhood/',
        'sha256': '3b7d9a1de39680cc4e31a03542a9d563c706349f822db34b474a363000eec573',
        'conclusions': {
            'rhf_current_equity_options_adr': 'supported',
            'rhf_current_mutual_funds': 'not_supported',
            'rhf_current_fixed_income': 'not_supported',
            'futures': 'rhd_separate_account',
        },
    },
    'multiple-accounts': {
        'url': 'https://robinhood.com/us/en/support/articles/multiple-investing-accounts-faq/',
        'sha256': '892fd1eb36241d4a5e5da90ce50139151e0a076e907d112d5d230bebe0209f06',
        'conclusions': {
            'futures': 'rhd_primary_only',
            'event_contracts': 'rhd_primary_only',
        },
    },
    'agent-account-setup': {
        'url': 'https://robinhood.com/us/en/support/articles/setting-up-an-agent/',
        'sha256': 'b15293114fab5b1191c6a05ed14b480ba9479e7f36195fcf5270dafe5f6a75c8',
        'conclusions': {
            'agentic_account_requires_primary': 'documented',
        },
    },
}


class CurrentEvidenceError(ValueError):
    def __init__(self, code, detail=None):
        self.code = code
        self.detail = detail
        super().__init__(code)

    def __str__(self):
        return self.code if self.detail is None else f'{self.code}:{self.detail}'


def _fail(code, detail=None):
    raise CurrentEvidenceError(code, detail)


def _money(value, code):
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        _fail(code)
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        _fail(code)
    if not result.is_finite():
        _fail(code)
    return result


class _FloatToken:
    pass


def _reject_float_values(value):
    if isinstance(value, _FloatToken):
        return True
    if isinstance(value, dict):
        return any(_reject_float_values(v) for v in value.values())
    if isinstance(value, list):
        return any(_reject_float_values(v) for v in value)
    return False


def _portfolio_raw_has_float(page, loaded):
    name = Path(page['raw_path']).name
    raw = loaded.get('raw_files', {}).get(name, {}).get('bytes')
    if not isinstance(raw, (bytes, bytearray)):
        return False
    try:
        parsed = json.loads(raw, parse_float=lambda value: _FloatToken())
    except (TypeError, ValueError):
        return False
    return _reject_float_values(parsed)


def _read_public(path, limit=MAX_PUBLIC_SOURCE_BYTES):
    path = Path(path)
    if path.is_symlink() or not path.is_file():
        _fail('PUBLIC_SOURCE_MISSING', str(path))
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError:
        _fail('PUBLIC_SOURCE_MISSING', str(path))
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size > limit:
            _fail('PUBLIC_SOURCE_INVALID', str(path))
        chunks = []
        size = 0
        while True:
            block = os.read(fd, 1024 * 1024)
            if not block:
                break
            chunks.append(block)
            size += len(block)
            if size > limit:
                _fail('PUBLIC_SOURCE_INVALID', str(path))
        data = b''.join(chunks)
        if size != info.st_size:
            _fail('PUBLIC_SOURCE_CHANGED', str(path))
        return data
    finally:
        os.close(fd)


def load_public_product_evidence(manifest_path, *, now):
    """Validate the pinned public docs. Missing/change/expiry is a blocker."""
    result = {
        'schema_version': SCHEMA_VERSION, 'policy_version': PUBLIC_POLICY_VERSION,
        'status': 'invalid', 'manifest_sha256': None, 'manifest_path': None,
        'sources': [], 'rules': {}, 'blockers': [],
    }
    try:
        manifest_bytes = _read_public(manifest_path)
        result['manifest_sha256'] = hashlib.sha256(manifest_bytes).hexdigest()
        result['manifest_path'] = str(Path(manifest_path).resolve())
        manifest_info = os.stat(Path(manifest_path), follow_symlinks=False)
        result['manifest_size'] = manifest_info.st_size
        result['manifest_mtime_ns'] = manifest_info.st_mtime_ns
        manifest = json.loads(manifest_bytes)
    except (CurrentEvidenceError, TypeError, ValueError) as exc:
        result['blockers'].append(str(exc))
        return result
    if not isinstance(manifest, dict) or not isinstance(manifest.get('sources'), list):
        result['blockers'].append('PUBLIC_SOURCE_MANIFEST_INVALID')
        return result
    base = Path(manifest_path).resolve().parent
    seen = set()
    for expected_name, expected in PUBLIC_SOURCES.items():
        matches = [item for item in manifest['sources']
                   if isinstance(item, dict) and item.get('name') == expected_name]
        if len(matches) != 1:
            result['blockers'].append('PUBLIC_SOURCE_MISSING:' + expected_name)
            continue
        item = matches[0]
        seen.add(expected_name)
        try:
            url = item.get('url')
            final_url = item.get('final_url', url)
            if url != expected['url'] or final_url != expected['url']:
                _fail('PUBLIC_SOURCE_URL_UNAPPROVED', expected_name)
            if item.get('sha256') != expected['sha256']:
                _fail('PUBLIC_SOURCE_HASH_UNAPPROVED', expected_name)
            retrieved = timestamp(item.get('retrieved_at'))
            if retrieved > now:
                _fail('PUBLIC_SOURCE_FUTURE', expected_name)
            if now - retrieved > PUBLIC_SOURCE_MAX_AGE:
                _fail('PUBLIC_SOURCE_EXPIRED', expected_name)
            raw_path = item.get('path')
            if not isinstance(raw_path, str) or not raw_path:
                _fail('PUBLIC_SOURCE_PATH_INVALID', expected_name)
            candidate = Path(raw_path)
            if candidate.is_absolute():
                _fail('PUBLIC_SOURCE_PATH_INVALID', expected_name)
            choices = [base / candidate]
            if candidate.name != raw_path:
                choices.append(base / candidate.name)
            resolved = None
            for choice in choices:
                if choice.exists() and not choice.is_symlink():
                    resolved = choice.resolve()
                    break
            if resolved is None or (base != resolved.parent
                                    and base not in resolved.parents):
                _fail('PUBLIC_SOURCE_PATH_INVALID', expected_name)
            data = _read_public(resolved)
            actual = hashlib.sha256(data).hexdigest()
            if actual != expected['sha256'] or item.get('sha256') != actual:
                _fail('PUBLIC_SOURCE_HASH_MISMATCH', expected_name)
            if item.get('bytes') != len(data):
                _fail('PUBLIC_SOURCE_BYTES_MISMATCH', expected_name)
            source_info = os.stat(resolved, follow_symlinks=False)
            if source_info.st_size != len(data):
                _fail('PUBLIC_SOURCE_CHANGED', expected_name)
            result['sources'].append({
                'name': expected_name, 'url': url, 'final_url': final_url,
                'retrieved_at': retrieved.isoformat(),
                'sha256': actual, 'bytes': len(data),
                'path': str(resolved),
                'size': source_info.st_size,
                'mtime_ns': source_info.st_mtime_ns,
                'expires_at': (retrieved + PUBLIC_SOURCE_MAX_AGE).isoformat(),
                'conclusions': dict(expected['conclusions']),
            })
        except (CurrentEvidenceError, TypeError, ValueError) as exc:
            result['blockers'].append(str(exc))
    if seen != set(PUBLIC_SOURCES) or len(seen) != len(result['sources']):
        result['blockers'].append('PUBLIC_SOURCE_SET_INCOMPLETE')
    if result['blockers']:
        return result
    result['status'] = 'valid'
    result['rules'] = {
        'rhf_current_equity_options_adr': 'supported_by_public_product_page',
        'rhf_current_mutual_funds': 'not_supported_by_public_product_page',
        'rhf_current_fixed_income': 'not_supported_by_public_product_page',
        'futures': 'RHD_separate_account_public_statement',
        'event_contracts': 'RHD_primary_account_public_statement',
        'account_scope_conclusion': 'unknown_linked_rhd_or_primary_role_unproven',
        'is_default_implies_primary': False,
        'public_product_does_not_prove_historical_orders': True,
    }
    return result


def _sort_records(tool, records):
    identity = _identity_for_tool(tool)
    if identity is None:
        return sorted(records, key=canonical)
    return sorted(records, key=lambda record: (
        identity(record) or '', canonical(record)))


def _two_pass(loaded, tool, observed, blockers):
    result = {'status': 'complete', 'records': None, 'reasons': []}
    values = []
    for pass_number in (1, 2):
        pages = _collection_pages(loaded, tool, pass_number)
        if not pages:
            blockers.append('TWO_PASS_REQUIRED:' + tool)
            result['status'] = 'missing'
            result['reasons'].append('TWO_PASS_REQUIRED')
            values.append(None)
            continue
        if any(page['query_scope'] != 'full' for page in pages):
            blockers.append('QUERY_SCOPE_PARTIAL:' + tool)
            result['status'] = 'incomplete'
            result['reasons'].append('QUERY_SCOPE_PARTIAL')
            values.append(None)
            continue
        try:
            records, schema_error = _page_records(
                loaded, tool, pass_number, observed, pages=pages)
        except (BundleError, AssertionError) as exc:
            code = getattr(exc, 'code', 'BUNDLE_SCHEMA_UNVERIFIED:' + tool)
            blockers.append(code)
            result['status'] = 'schema_unverified'
            result['reasons'].append(code)
            values.append(None)
            continue
        if schema_error:
            blockers.append(schema_error)
            result['status'] = 'schema_unverified'
            result['reasons'].append(schema_error)
            values.append(None)
            continue
        if tool in {'get_equity_positions', 'get_equity_orders',
                    'get_option_positions', 'get_option_orders',
                    'get_crypto_positions'}:
            before = len(blockers)
            records = _merge_records(
                records, _identity_for_tool(tool), tool, blockers)
            if len(blockers) != before:
                result['status'] = 'conflict'
                result['reasons'].append('IDENTITY_CONFLICT')
            records = _sort_records(tool, records)
        values.append(records)
    if result['status'] != 'complete':
        return result
    if values[0] is None or values[1] is None:
        result['status'] = 'missing'
        result['reasons'].append('PASS_MISSING')
        return result
    if canonical(values[0]) != canonical(values[1]):
        blockers.append('TWO_PASS_MISMATCH:' + tool)
        result['status'] = 'unstable'
        result['reasons'].append('TWO_PASS_MISMATCH')
        return result
    result['records'] = values[0]
    return result


def _advanced_two_pass(loaded, observed, blockers):
    result = {'status': 'complete', 'groups': None, 'orders': None,
              'reasons': []}
    values = []
    for pass_number in (1, 2):
        pages = _collection_pages(loaded, 'get_advanced_orders', pass_number)
        if not pages:
            blockers.append('TWO_PASS_REQUIRED:get_advanced_orders')
            result['status'] = 'missing'
            result['reasons'].append('TWO_PASS_REQUIRED')
            values.append(None)
            continue
        if any(page['query_scope'] != 'full' for page in pages):
            blockers.append('QUERY_SCOPE_PARTIAL:get_advanced_orders')
            result['status'] = 'incomplete'
            result['reasons'].append('QUERY_SCOPE_PARTIAL')
            values.append(None)
            continue
        detail = {}
        for page in _collection_pages(loaded, 'get_equity_orders', pass_number):
            order_id = page['args'].get('order_id')
            if order_id:
                for raw in page['data'].get('orders', []) if isinstance(page['data'], dict) else []:
                    detail[raw.get('id')] = raw
        groups, orders = [], []
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
            blockers.append(exc.code)
            result['status'] = 'schema_unverified'
            result['reasons'].append(exc.code)
            values.append(None)
            continue
        before = len(blockers)
        groups = _merge_records(groups, lambda group: group.get('id'),
                                'get_advanced_orders:groups', blockers)
        orders = _merge_records(orders, lambda order: order.get('id'),
                                'get_advanced_orders:orders', blockers)
        if len(blockers) != before:
            result['status'] = 'conflict'
            result['reasons'].append('IDENTITY_CONFLICT')
        groups = sorted(groups, key=lambda group: group.get('id') or '')
        orders = sorted(orders, key=lambda order: order.get('id') or '')
        values.append({'groups': groups, 'orders': orders})
    if result['status'] != 'complete':
        return result
    if values[0] is None or values[1] is None:
        result['status'] = 'missing'
        result['reasons'].append('PASS_MISSING')
        return result
    if canonical(values[0]) != canonical(values[1]):
        blockers.append('TWO_PASS_MISMATCH:get_advanced_orders')
        result['status'] = 'unstable'
        result['reasons'].append('TWO_PASS_MISMATCH')
        return result
    result['groups'] = values[0]['groups']
    result['orders'] = values[0]['orders']
    return result


def _order_summary(records):
    open_orders = [item for item in records
                   if str(item.get('state') or '').lower()
                   not in TERMINAL_ORDER_STATES]
    return {'status': 'observed_complete', 'count': len(records),
            'terminal_count': len(records) - len(open_orders),
            'open_count': len(open_orders),
            'records': records}


def _position_summary(records):
    return {'status': 'observed_complete', 'count': len(records),
            'records': records}


def _portfolio_page(loaded):
    pages = _collection_pages(loaded, 'get_portfolio', 1)
    if len(pages) != 1:
        return None, 'MISSING_TOOL:get_portfolio'
    try:
        normalized = _normalize_portfolio(pages[0])
        _set_page_normalized(pages[0], normalized)
        return pages[0], None
    except BundleError as exc:
        return None, exc.code


def _approval_setting(loaded, account):
    pages = _collection_pages(loaded, 'get_trade_approval_setting', 1)
    if len(pages) != 1:
        return None, 'MISSING_TOOL:get_trade_approval_setting'
    try:
        setting = _normalize_setting(pages[0])
        _set_page_normalized(pages[0], setting)
    except BundleError as exc:
        return None, exc.code
    if setting.get('account_number') != account['account_number']:
        return None, 'BUNDLE_APPROVAL_SETTING_ACCOUNT'
    value = setting.get('human_must_approve_trades')
    if type(value) is not bool:
        return None, 'BUNDLE_APPROVAL_SETTING_TYPE_INVALID'
    raw = pages[0]['data'].get('setting') if isinstance(
        pages[0]['data'], dict) else None
    updated_at = None
    updated_at_present = isinstance(raw, dict) and 'updated_at' in raw
    if updated_at_present:
        try:
            updated_at = timestamp(raw.get('updated_at')).isoformat()
        except (TypeError, ValueError, OverflowError):
            return None, 'BUNDLE_APPROVAL_SETTING_UPDATED_AT_INVALID'
    setting = dict(setting)
    setting['updated_at'] = updated_at
    setting['updated_at_present'] = updated_at_present
    return setting, None


def _portfolio_money(page, code, loaded=None):
    if loaded is not None and _portfolio_raw_has_float(page, loaded):
        _fail('FUNDS_FLOAT_VALUE_NOT_ACCEPTED')
    data = page['data']
    bp = data.get('buying_power') if isinstance(data, dict) else None
    if not isinstance(bp, dict):
        _fail('FUNDS_BUYING_POWER_MISSING')
    currency = data.get('currency')
    if currency != 'USD' or bp.get('display_currency') != 'USD':
        _fail('FUNDS_CURRENCY_UNSUPPORTED')
    cash = _money(data.get('cash'), code)
    pending = _money(data.get('pending_deposits'), code)
    if pending < 0:
        _fail('FUNDS_PENDING_DEPOSITS_NEGATIVE')
    total = _money(data.get('total_value'), code)
    buying = _money(bp.get('buying_power'), code)
    unleveraged = _money(bp.get('unleveraged_buying_power'), code)
    asset_keys = ('options_value', 'crypto_value',
                  'futures_value', 'event_contracts_value',
                  'fixed_income_value', 'mutual_funds_value')
    assets = {key: str(_money(data.get(key), code)) for key in asset_keys}
    return {
        'cash': str(cash), 'pending_deposits': str(pending),
        'total_value': str(total),
        'buying_power': str(buying),
        'unleveraged_buying_power': str(unleveraged),
        'asset_values': assets,
        'currency': 'USD',
        'buying_power_equals_unleveraged': buying == unleveraged,
        'cash_equals_buying_power': cash == buying,
        'buying_power_negative': buying < 0,
        'unleveraged_buying_power_negative': unleveraged < 0,
        'buying_power_source': 'get_portfolio.buying_power.buying_power',
        'unleveraged_buying_power_source': (
            'get_portfolio.buying_power.unleveraged_buying_power'),
        'broker_buying_power_authoritative_for_spendability': True,
        'cash_is_not_buying_power': cash != buying,
        'source': 'get_portfolio_single_pass',
    }


def _public_product_not_applicable(public, account, product, value, observed):
    """Return a bounded N/A receipt only for the reviewed RHF product rule.

    The public product page states that current RHF accounts do not support
    fixed income or mutual funds.  That is a product-scope fact, not an order
    or history observation.  A non-zero current value conflicts with the rule,
    and a missing/other affiliate cannot borrow the RHF conclusion.
    """
    if product not in {'fixed_income', 'mutual_funds'}:
        return None, 'PRODUCT_NOT_IN_SCOPE'
    if (public.get('status') != 'valid' or public.get('blockers')
            or public.get('policy_version') != PUBLIC_POLICY_VERSION):
        return None, 'PUBLIC_PRODUCT_SOURCE_NOT_VALID'
    affiliate = account.get('affiliate') if isinstance(account, dict) else None
    if not isinstance(affiliate, str) or affiliate.strip().lower() != 'rhf':
        return None, 'ACCOUNT_AFFILIATE_UNPROVEN'
    if value is None:
        return None, 'PORTFOLIO_VALUE_MISSING'
    try:
        if _money(value, 'FUNDS_VALUE_INVALID') != 0:
            return None, 'CURRENT_NONZERO_VALUE_REQUIRES_ORDER_SOURCE'
    except CurrentEvidenceError:
        return None, 'PORTFOLIO_VALUE_INVALID'
    source = next((item for item in (public.get('sources') or [])
                   if isinstance(item, dict)
                   and item.get('name') == 'investment-products'), None)
    if source is None:
        return None, 'PUBLIC_PRODUCT_SOURCE_MISSING'
    conclusion_key = {
        'fixed_income': 'rhf_current_fixed_income',
        'mutual_funds': 'rhf_current_mutual_funds',
    }[product]
    conclusions = source.get('conclusions')
    conclusion = (conclusions.get(conclusion_key)
                  if isinstance(conclusions, dict) else None)
    if conclusion != 'not_supported':
        return None, 'PUBLIC_CONCLUSION_NOT_APPLICABLE'
    return {
        'schema_version': SCHEMA_VERSION,
        'kind': 'public_product_scope_v1',
        'policy_version': public.get('policy_version'),
        'manifest_sha256': public.get('manifest_sha256'),
        'manifest_path': public.get('manifest_path'),
        'source_name': 'investment-products',
        'source_url': source.get('url'),
        'source_sha256': source.get('sha256'),
        'source_bytes': source.get('bytes'),
        'retrieved_at': source.get('retrieved_at'),
        'expires_at': source.get('expires_at'),
        'observed_at': observed.isoformat(),
        'conclusion_key': conclusion_key,
        'conclusion': conclusion,
        'product': product,
        'account_key_hash': account.get('account_key'),
        'account_mask': account.get('account_mask'),
        'account_affiliate': affiliate.strip().lower(),
        'account_brokerage_type': account.get('brokerage_account_type'),
        'basis': 'pinned_public_product_scope_for_current_rhf_account',
        'not_an_order_observation': True,
    }, 'PRODUCT_NOT_SUPPORTED_BY_PINNED_PUBLIC_SOURCE'



def _capture_pin(loaded, started, observed):
    """Immutable-generation summary for one validated local capture bundle.

    The stat list is a bounded ordinary-race detector used inside a short
    write transaction. It is not a broker signature and cannot prove that an
    attacker did not rewrite bytes while preserving size/mtime.
    """
    root = Path(loaded['root']).resolve()
    files = []
    manifest_path = root / 'manifest.json'
    manifest_info = os.stat(manifest_path, follow_symlinks=False)
    if manifest_info.st_size < 1:
        _fail('CAPTURE_GENERATION_INVALID')
    files.append({'path': 'manifest.json',
                  'size': manifest_info.st_size,
                  'mtime_ns': manifest_info.st_mtime_ns})
    raw_dir = root / 'raw'
    for name in sorted(loaded['raw_files']):
        path = raw_dir / name
        info = os.stat(path, follow_symlinks=False)
        if info.st_size != len(loaded['raw_files'][name]['bytes']):
            _fail('CAPTURE_GENERATION_CHANGED', name)
        files.append({'path': 'raw/' + name,
                      'size': info.st_size,
                      'mtime_ns': info.st_mtime_ns})
    manifest_sha = loaded['manifest_sha256']
    raw_chain = _raw_chain(loaded)
    generation = digest({
        'schema_version': 1, 'kind': 'capture_generation_v1',
        'manifest_sha256': manifest_sha,
        'raw_chain_sha256': raw_chain,
        'observed_at': observed.isoformat(),
        'file_count': len(files),
    })
    return {
        'schema_version': 1, 'kind': 'capture_generation_v1',
        'root': str(root),
        'manifest_sha256': manifest_sha,
        'raw_chain_sha256': raw_chain,
        'observed_at': observed.isoformat(),
        'collection_started_at': started.isoformat(),
        'collection_window_seconds': (observed - started).total_seconds(),
        'file_count': len(files),
        'file_stats': files,
        'generation': generation,
        'local_hash_is_not_broker_signature': True,
    }


def capture_generation(bundle_root):
    """Read-only validated capture generation; no database/network access."""
    loaded = load_saved_bundle(bundle_root)
    started, observed = _validate_page_global(loaded)
    return _capture_pin(loaded, started, observed)


def compile_current_evidence(bundle_root, public_sources, *, now=None):
    """Compile the two independent current observations; never authorize."""
    now = now or datetime.now(timezone.utc)
    if not isinstance(now, datetime) or now.tzinfo is None:
        _fail('CURRENT_EVIDENCE_CLOCK_INVALID')
    now = now.astimezone(timezone.utc)
    loaded = load_saved_bundle(bundle_root)
    started, observed = _validate_page_global(loaded)
    capture = _capture_pin(loaded, started, observed)
    stale = (now - observed).total_seconds() > MAX_CURRENT_EVIDENCE_AGE_SECONDS
    future = observed > now
    account = _identity_account(loaded)
    blockers = []
    if future:
        blockers.append('CURRENT_EVIDENCE_FUTURE')
    if stale:
        blockers.append('CURRENT_EVIDENCE_STALE')
    public = load_public_product_evidence(public_sources, now=now)
    blockers.extend(public['blockers'])

    portfolio_page, portfolio_error = _portfolio_page(loaded)
    if portfolio_error:
        blockers.append(portfolio_error)
    setting, setting_error = _approval_setting(loaded, account)
    if setting_error:
        blockers.append(setting_error)

    results = {
        'equity_positions': _two_pass(loaded, 'get_equity_positions', observed, blockers),
        'equity_orders': _two_pass(loaded, 'get_equity_orders', observed, blockers),
        'option_positions': _two_pass(loaded, 'get_option_positions', observed, blockers),
        'option_orders': _two_pass(loaded, 'get_option_orders', observed, blockers),
        'crypto_positions': _two_pass(loaded, 'get_crypto_positions', observed, blockers),
        'crypto_orders': _two_pass(loaded, 'get_crypto_orders', observed, blockers),
        'advanced': _advanced_two_pass(loaded, observed, blockers),
        'approvals': _two_pass(loaded, 'get_trade_approvals', observed, blockers),
    }
    required_complete = all(item['status'] == 'complete'
                            for item in results.values())
    portfolio = portfolio_page['normalized'] if portfolio_page else None
    scope = {key: {} for key in (
        'equity', 'options', 'crypto', 'advanced', 'futures',
        'event_contracts', 'fixed_income', 'mutual_funds')}
    def unknown(result):
        return {'status': 'unknown_' + result['status'],
                'reason': result['reasons'][0] if result['reasons'] else None}

    def known_positions(result):
        return (_position_summary(result['records'])
                if result['status'] == 'complete' else unknown(result))

    def known_orders(result):
        return (_order_summary(result['records'])
                if result['status'] == 'complete' else unknown(result))

    for key, pos_key, order_key in (
            ('equity', 'equity_positions', 'equity_orders'),
            ('options', 'option_positions', 'option_orders'),
            ('crypto', 'crypto_positions', 'crypto_orders')):
        scope[key] = {
            'positions': known_positions(results[pos_key]),
            'orders': known_orders(results[order_key]),
            'historical_orders_complete': False,
        }
        scope[key]['scope_status'] = (
            'observed_supported'
            if (scope[key]['positions'].get('status') == 'observed_complete'
                and scope[key]['orders'].get('status') == 'observed_complete')
            else 'unknown')
        scope[key]['scope_status_reason'] = (
            'OBSERVED_SUPPORTED' if scope[key]['scope_status'] ==
            'observed_supported' else 'SUPPORTED_SCOPE_NOT_COMPLETE')
    adv = results['advanced']
    if adv['status'] == 'complete':
        scope['advanced'] = {
            'groups': {'status': 'observed_complete',
                       'count': len(adv['groups'])},
            'orders': _order_summary(adv['orders']),
            'historical_orders_complete': False,
            'scope_status': 'observed_supported',
            'scope_status_reason': 'OBSERVED_SUPPORTED',
        }
    else:
        scope['advanced'] = {
            'groups': unknown(adv),
            'orders': unknown(adv),
            'historical_orders_complete': False,
            'scope_status': 'unknown',
            'scope_status_reason': 'SUPPORTED_SCOPE_NOT_COMPLETE',
        }
    for key in ('futures', 'event_contracts'):
        scope[key] = {
            'positions': {'status': 'unknown_account_scope_unproven',
                          'reason': 'linked_rhd_or_primary_role_unproven'},
            'orders': {'status': 'unknown_account_scope_unproven',
                       'reason': 'linked_rhd_or_primary_role_unproven'},
            'historical_orders_complete': False,
            'product_capability': public['rules'].get(key, 'unverified'),
            'scope_status': 'unsupported_channel',
            'scope_status_reason': (
                'NO_CURRENT_GETTER_AND_LINKED_ACCOUNT_ROLE_UNPROVEN'),
            'channel': {
                'status': 'unsupported_channel',
                'reason': 'NO_FUTURES_OR_EVENT_CONTRACT_GETTER',
            },
        }
    for key, value_key in (('fixed_income', 'fixed_income_value'),
                           ('mutual_funds', 'mutual_funds_value')):
        value = portfolio.get(value_key) if portfolio else None
        applicability, scope_reason = _public_product_not_applicable(
            public, account, key, value, observed)
        if applicability is not None:
            scope[key] = {
                'positions': {
                    'status': 'not_applicable_by_public_product_source',
                    'current_value': str(_money(value, 'FUNDS_VALUE_INVALID')),
                    'source': 'get_portfolio_single_pass',
                    'single_pass_only': True,
                    'not_an_order_observation': True,
                },
                'orders': {
                    'status': 'not_applicable_by_public_product_source',
                    'reason': 'PRODUCT_NOT_SUPPORTED_NO_ORDER_GETTER_REQUIRED',
                    'not_an_order_observation': True,
                },
                'historical_orders_complete': False,
                'product_capability': public['rules'].get(
                    {'fixed_income': 'rhf_current_fixed_income',
                     'mutual_funds': 'rhf_current_mutual_funds'}[key],
                    'unverified'),
                'scope_status': 'verified_not_applicable',
                'scope_status_reason': scope_reason,
                'applicability': applicability,
                'channel': {
                    'status': 'unsupported_channel',
                    'reason': 'PRODUCT_NOT_SUPPORTED_BY_PUBLIC_SOURCE',
                },
            }
            continue
        if value is None:
            value_status = 'unknown_portfolio_value'
        else:
            try:
                value_status = ('current_zero_by_portfolio'
                                if _money(value, 'FUNDS_VALUE_INVALID') == 0
                                else 'current_nonzero_by_portfolio')
            except CurrentEvidenceError:
                value_status = 'unknown_portfolio_value'
        scope[key] = {
            'positions': {
                'status': value_status,
                'current_value': (str(value) if value is not None else None),
                'source': 'get_portfolio_single_pass',
                'single_pass_only': True,
            },
            'orders': {'status': 'unknown_getter_missing'},
            'historical_orders_complete': False,
            'product_capability': public['rules'].get(
                {'fixed_income': 'rhf_current_fixed_income',
                 'mutual_funds': 'rhf_current_mutual_funds'}[key],
                'unverified'),
            'scope_status': 'unsupported_channel',
            'scope_status_reason': scope_reason,
            'channel': {
                'status': 'unsupported_channel',
                'reason': 'NO_FIXED_INCOME_OR_MUTUAL_FUND_ORDER_GETTER',
            },
        }

    approval_result = results['approvals']
    if approval_result['status'] == 'complete':
        approvals_summary = {
            'status': 'observed_complete',
            'count': len(approval_result['records']),
        }
    else:
        approvals_summary = {
            'status': 'unknown_' + approval_result['status'],
            'count': None,
            'reason': (approval_result['reasons'][0]
                       if approval_result['reasons'] else None),
        }
    if setting:
        setting_summary = {
            'status': 'observed_single_pass',
            'human_must_approve_trades': setting.get('human_must_approve_trades'),
            'updated_at': setting.get('updated_at'),
            'updated_at_present': setting.get('updated_at_present'),
            'single_pass_only': True,
        }
    elif setting_error:
        setting_summary = {'status': 'unknown_invalid',
                           'reason': setting_error}
    else:
        setting_summary = {'status': 'unknown_incomplete'}

    funds = {
        'schema_version': SCHEMA_VERSION, 'kind': FUNDS_KIND,
        'account_mask': account['account_mask'],
        'observed_at': observed.isoformat(),
        'status': 'unknown_incomplete',
        'money_facts_valid': False,
        'informational_valid': None,
        'values': None, 'unsettled_funds': None,
        'settlement_ledger_complete': False,
        'settlement_ledger_status': 'unproven_no_source',
        'entry_authorized': False,
        'policy_limits_evaluated': False,
        'buying_power_authority': {
            'source': 'get_portfolio.buying_power',
            'buying_power_is_broker_spendable': True,
            'unleveraged_buying_power_ignores_margin_leverage': True,
            'cash_is_not_buying_power': True,
            'local_policy_may_not_replace': True,
        },
        'blockers': [],
    }
    if portfolio_page is not None:
        try:
            values = _portfolio_money(portfolio_page, 'FUNDS_VALUE_INVALID', loaded)
            funds['values'] = values
            funds['money_facts_valid'] = True
            funds['status'] = 'observed_current_money_facts'
        except CurrentEvidenceError as exc:
            funds['blockers'].append(exc.code)
            blockers.append(exc.code)
    else:
        funds['blockers'].append('MISSING_TOOL:get_portfolio')
    raw_unsettled = account.get('unsettled_funds')
    if raw_unsettled in (None, ''):
        funds['unsettled_funds'] = {
            'status': 'absent_unknown_not_zero',
            'authoritative_for_spendability': False,
            'may_lag': True,
        }
        funds['informational_valid'] = None
    else:
        try:
            funds['unsettled_funds'] = {
                'status': 'present_informational',
                'raw': str(raw_unsettled),
                'value': str(_money(raw_unsettled, 'FUNDS_UNSETTLED_INVALID')),
                'authoritative_for_spendability': False,
                'may_lag': True,
                'component_ledger': False,
            }
            funds['informational_valid'] = True
        except CurrentEvidenceError as exc:
            funds['unsettled_funds'] = {'status': 'invalid',
                                        'reason': exc.code,
                                        'authoritative_for_spendability': False}
            funds['informational_valid'] = False
            funds['blockers'].append(exc.code)
            blockers.append(exc.code)
    if funds['status'] == 'observed_current_money_facts' \
            and funds['informational_valid'] is False:
        funds['status'] = 'money_facts_valid_informational_invalid'
    elif funds['status'] != 'observed_current_money_facts':
        funds['status'] = 'blocked'

    account_scope = {
        'schema_version': SCHEMA_VERSION, 'kind': ACCOUNT_SCOPE_KIND,
        'account_mask': account['account_mask'],
        'account_key_hash': account['account_key'],
        'observed_at': observed.isoformat(),
        'collection_window_seconds': (observed - started).total_seconds(),
        'bundle_is_current': not stale and not future,
        'bundle_manifest_sha256': loaded['manifest_sha256'],
        'raw_chain_sha256': capture['raw_chain_sha256'],
        'scopes': scope,
        'approvals': approvals_summary,
        'approval_setting': setting_summary,
        'product_evidence': {
            'policy_version': public['policy_version'],
            'status': public['status'],
            'manifest_sha256': public.get('manifest_sha256'),
            'manifest_path': public.get('manifest_path'),
            'manifest_size': public.get('manifest_size'),
            'manifest_mtime_ns': public.get('manifest_mtime_ns'),
            'sources': public['sources'],
            'rules': public['rules'],
        },
        'claims': {
            'historical_orders_complete': False,
            'settlement_ledger_complete': False,
            'all_asset_flat_proven': False,
            'entry_authorized': False,
            'not_applicable_scopes': sorted(
                name for name, item in scope.items()
                if item.get('scope_status') == 'verified_not_applicable'),
            'not_applicable_is_not_order_history': True,
        },
        'blockers': sorted(set(blockers)),
        'ready': False,
        'production_ready': False,
    }
    return {
        'schema_version': SCHEMA_VERSION, 'kind': 'current_evidence_v1',
        'compiled_at': now.isoformat(),
        'observed_at': observed.isoformat(),
        'collection_window_seconds': (observed - started).total_seconds(),
        'bundle_is_current': not stale and not future,
        'account': {
            'account_mask': account['account_mask'],
            'account_key_hash': account['account_key'],
            'affiliate': account.get('affiliate'),
            'type': account.get('type'),
            'brokerage_account_type': account.get('brokerage_account_type'),
            'agentic_allowed': account.get('agentic_allowed'),
            'is_default': account.get('is_default'),
            'state': account.get('state'),
            'option_level': account.get('option_level'),
        },
        'account_scope_applicability': account_scope,
        'funds_availability': funds,
        'capture': capture,
        'historical_orders_complete': False,
        'settlement_ledger_complete': False,
        'entry_authorized': False,
        'observation_valid': (
            not future and not stale and not public['blockers']
            and required_complete and portfolio_error is None
            and setting_error is None
            and funds['status'] == 'observed_current_money_facts'),
        'ready': False,
        'production_ready': False,
        'blockers': sorted(set(blockers) | set(funds['blockers'])),
    }


def _parse_args(argv=None):
    import argparse
    parser = argparse.ArgumentParser(
        prog='martin-agentic current-evidence',
        description='Pure read-only current account/product evidence; never authorizes')
    parser.add_argument('--bundle-root', required=True)
    parser.add_argument('--public-sources', required=True)
    return parser.parse_args(argv)


def run_cli(argv=None):
    import sys
    args = _parse_args(argv)
    try:
        value = compile_current_evidence(args.bundle_root, args.public_sources)
    except (BundleError, CurrentEvidenceError, OSError, ValueError) as exc:
        value = {'schema_version': SCHEMA_VERSION,
                 'kind': 'current_evidence_v1', 'status': 'BLOCKED',
                 'blockers': [getattr(exc, 'code', str(exc))],
                 'entry_authorized': False, 'ready': False,
                 'production_ready': False}
        print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))
        return 3
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if value.get('observation_valid') is True else 3
