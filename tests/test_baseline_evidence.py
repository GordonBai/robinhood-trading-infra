"""Pure saved-response bundle adapter tests. No broker, market, or Host calls."""
import hashlib
import json
import os
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
import shutil
import sqlite3
import tempfile
from unittest import TestCase

from martin_agentic.baseline_evidence import (
    BundleError, adapt_saved_bundle, load_saved_bundle, normalize_bundle)
from test_historical_baseline import AT, NOW, evidence
from trading_contracts.values import digest
from trading_infra.historical_baseline import BaselineError, plan_baseline


ACCOUNT = 'SIM-0000'
RHS = 'RHS-0000'
RHC = 'RHC-0000'
AT_DT = datetime.fromisoformat(AT)
OBSERVED = (AT_DT + timedelta(seconds=3)).isoformat()
NOW_LATER = datetime.fromisoformat(OBSERVED) + timedelta(seconds=1)


def envelope(data):
    return json.dumps({'data': data}, separators=(',', ':')).encode()


def page_data(bundle, page):
    return json.loads(bundle.files[Path(page['raw_path']).name])['data']


def account_data():
    return {'accounts': [{
        'account_number': ACCOUNT,
        'rhs_account_number': RHS,
        'rhc_account_number': RHC,
        'agentic_allowed': True,
        'deactivated': False,
        'permanently_deactivated': False,
        'state': 'active',
        'brokerage_account_type': 'individual',
        'affiliate': 'rhf',
        'type': 'cash',
        'option_level': 'option_level_0',
    }]}


def portfolio_data():
    return {
        'cash': '500', 'total_value': '500', 'pending_deposits': '0',
        'currency': 'USD', 'options_value': '0', 'crypto_value': '0',
        'futures_value': '0', 'event_contracts_value': '0',
        'fixed_income_value': '0', 'mutual_funds_value': '0',
        'buying_power': {'buying_power': '500', 'display_currency': 'USD',
                         'unleveraged_buying_power': '500'},
    }


class BundleFixture:
    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.root.chmod(0o700)
        self.raw = self.root / 'raw'
        self.raw.mkdir(mode=0o700)
        self.pages = []
        self.files = {}
        self._seq = 0
        self.add('get_accounts', 1, account_data(), args={})
        self.add('get_portfolio', 1, portfolio_data(), args={'account_number': ACCOUNT})
        self.add('get_trade_approval_setting', 1,
                 {'setting': {'account_number': ACCOUNT,
                              'human_must_approve_trades': False}},
                 args={'account_number': ACCOUNT})
        for tool, key, data, cr in (
                ('get_equity_positions', 'positions', [], False),
                ('get_equity_orders', 'orders', [], True),
                ('get_advanced_orders', 'advanced_orders', [], True),
                ('get_option_positions', 'positions', [], False),
                ('get_option_orders', 'orders', [], True),
                ('get_crypto_positions', 'results', [], False),
                ('get_crypto_orders', 'results', [], True),
                ('get_trade_approvals', 'approvals', [], True)):
            for pass_number in (1, 2):
                args = {'rhs_account_number': RHS} if tool in {
                    'get_crypto_positions', 'get_crypto_orders'} \
                    else {'account_number': ACCOUNT}
                self.add(tool, pass_number, {key: data}, args=args)
        self.write_manifest()

    def add(self, tool, pass_number, data, *, args, cursor_requested=None,
            cursor_returned=None, started_at=None, completed_at=None,
            page_index=None, raw=None):
        self._seq += 1
        name = f'{self._seq:03d}-{tool}-p{pass_number}.raw'
        raw = envelope(data) if raw is None else raw
        path = self.raw / name
        path.write_bytes(raw)
        os.chmod(path, 0o600)
        if page_index is None:
            page_index = 0
        if started_at is None:
            started_at = (AT_DT + timedelta(seconds=pass_number)).isoformat()
        if completed_at is None:
            completed_at = (AT_DT + timedelta(seconds=pass_number + 1)).isoformat()
        self.files[name] = raw
        page = {
            'tool': tool, 'args': args, 'pass': pass_number,
            'page_index': page_index, 'cursor_requested': cursor_requested,
            'cursor_returned': cursor_returned,
            'started_at': started_at, 'completed_at': completed_at,
            'raw_path': 'raw/' + name, 'raw_sha256': hashlib.sha256(raw).hexdigest(),
            'raw_bytes': len(raw),
        }
        self.pages.append(page)
        return page

    def write_manifest(self):
        (self.root / 'manifest.json').write_text(json.dumps({
            'schema_version': 1, 'provider': 'robinhood',
            'account_suffix': '0000', 'pages': self.pages,
        }, sort_keys=True))
        os.chmod(self.root / 'manifest.json', 0o600)

    def rewrite_raw(self, page, data):
        name = Path(page['raw_path']).name
        raw = envelope(data)
        self.files[name] = raw
        (self.raw / name).write_bytes(raw)
        page['raw_sha256'] = hashlib.sha256(raw).hexdigest()
        page['raw_bytes'] = len(raw)
        self.write_manifest()

    def set_crypto_orders(self, data, passes=(1, 2)):
        for page in self.pages:
            if page['tool'] == 'get_crypto_orders' and page['pass'] in passes:
                self.rewrite_raw(page, data)


class BaselineEvidenceTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.fixture = BundleFixture(self.temp.name)

    def adapt(self, **kwargs):
        return adapt_saved_bundle(self.temp.name, now=kwargs.pop('now', NOW_LATER))

    def test_supported_scopes_normalize_and_never_ready(self):
        result = self.adapt()
        self.assertFalse(result['ready'])
        self.assertFalse(result['production_ready'])
        self.assertIsNone(result['baseline_snapshot'])
        self.assertIsNotNone(result['candidate_snapshot'])
        self.assertEqual(result['account']['account_number'], ACCOUNT)
        self.assertIn('GETTER_MISSING:futures_positions', result['blockers'])
        self.assertIn('SETTLEMENT_SOURCE_MISSING', result['blockers'])
        self.assertEqual(result['observations']['equity_positions'], [])
        self.assertEqual(result['coverage']['get_equity_positions']['status'],
                         'complete')
        self.assertEqual(result['raw_chain_sha256'],
                         result['raw_chain_sha256'].lower())

        # The candidate is diagnostic only. The real baseline core must reject
        # it because the four unsupported scopes are absent, not empty.
        ev = deepcopy(evidence(ACCOUNT))
        account_key = result['account']['account_key']
        for section in ('source', 'risk', 'host', 'lineage'):
            ev[section]['account_key'] = account_key
        with self.assertRaises(BaselineError) as raised:
            plan_baseline(
                result['candidate_snapshot'], account_key=account_key,
                cutoff_utc=AT, policy_sha256='b' * 64,
                source_fingerprint='c' * 64, evidence=ev,
                expires_at=(NOW_LATER + timedelta(minutes=1)).isoformat(),
                now=NOW_LATER)
        self.assertEqual(raised.exception.code, 'CONDITIONS_MISSING')

    def test_negative_account_and_crypto_identifier_rejected(self):
        page = next(page for page in self.fixture.pages
                    if page['tool'] == 'get_equity_positions')
        page['args']['account_number'] = 'SIM-OTHER'
        self.fixture.write_manifest()
        result = self.adapt()
        self.assertIn('BUNDLE_ACCOUNT_MISMATCH', result['blockers'])

        fixture = BundleFixture(Path(self.temp.name + '-crypto'))
        page = next(page for page in fixture.pages
                    if page['tool'] == 'get_crypto_positions')
        page['args'] = {'rhs_account_number': 'RHS-OTHER'}
        fixture.write_manifest()
        result = adapt_saved_bundle(fixture.root, now=NOW_LATER)
        self.assertIn('BUNDLE_CRYPTO_ACCOUNT_MISMATCH', result['blockers'])

    def test_nonflat_option_and_crypto_positions_are_blockers(self):
        page = next(page for page in self.fixture.pages
                    if page['tool'] == 'get_option_positions'
                    and page['pass'] == 1)
        self.fixture.rewrite_raw(page, {'positions': [{
            'option_id': 'option-1', 'chain_id': 'chain',
            'chain_symbol': 'SPY', 'expiration_date': '2026-10-16',
            'type': 'call', 'quantity': '1',
            'pending_assignment_quantity': '0', 'pending_buy_quantity': '0',
            'pending_exercise_quantity': '0', 'pending_expiration_quantity': '0',
            'pending_sell_quantity': '0'}]})
        result = self.adapt()
        self.assertIn('NONFLAT_POSITION:options:option-1', result['blockers'])

        fixture = BundleFixture(Path(self.temp.name + '-crypto-position'))
        page = next(page for page in fixture.pages
                    if page['tool'] == 'get_crypto_positions'
                    and page['pass'] == 1)
        fixture.rewrite_raw(page, {'results': [{
            'currency': {'code': 'BTC'}, 'quantity': '0',
            'quantity_held_for_sell': '1', 'quantity_transferable': '0'}]})
        result = adapt_saved_bundle(fixture.root, now=NOW_LATER)
        self.assertIn('NONFLAT_POSITION:crypto:BTC', result['blockers'])

    def test_missing_page_cursor_loop_and_conflicting_duplicate(self):
        page = next(page for page in self.fixture.pages
                    if page['tool'] == 'get_equity_positions'
                    and page['pass'] == 1)
        page['cursor_returned'] = 'next-1'
        self.fixture.write_manifest()
        result = self.adapt()
        self.assertIn('BUNDLE_MISSING_PAGE', result['blockers'])

        fixture = BundleFixture(Path(self.temp.name + '-cursor'))
        page = next(page for page in fixture.pages
                    if page['tool'] == 'get_equity_positions'
                    and page['pass'] == 1)
        page['cursor_returned'] = 'loop'
        fixture.add('get_equity_positions', 1, {'positions': []},
                    args={'account_number': ACCOUNT}, cursor_requested='loop',
                    cursor_returned='loop', page_index=1)
        fixture.write_manifest()
        result = adapt_saved_bundle(fixture.root, now=NOW_LATER)
        self.assertTrue(any(code.startswith('BUNDLE_')
                            for code in result['blockers']))

        fixture = BundleFixture(Path(self.temp.name + '-duplicate'))
        page = next(page for page in fixture.pages
                    if page['tool'] == 'get_equity_positions'
                    and page['pass'] == 1)
        fixture.rewrite_raw(page, {'positions': [{
            'symbol': 'DEMO', 'quantity': '0',
            'shares_available_for_sells': '0', 'shares_held_for_sells': '0',
            'shares_held_for_asset_transfer': '0',
            'shares_held_for_options_events': '0',
            'shares_held_for_stock_grants': '0',
            'shares_pending_from_options_events': '0'}], 'next': 'c1'})
        page['cursor_returned'] = 'c1'
        fixture.add('get_equity_positions', 1, {'positions': [{
            'symbol': 'DEMO', 'quantity': '1',
            'shares_available_for_sells': '1', 'shares_held_for_sells': '0',
            'shares_held_for_asset_transfer': '0',
            'shares_held_for_options_events': '0',
            'shares_held_for_stock_grants': '0',
            'shares_pending_from_options_events': '0'}]},
            args={'account_number': ACCOUNT, 'cursor': 'c1'},
            cursor_requested='c1', page_index=1,
            started_at=(AT_DT + timedelta(seconds=2)).isoformat(),
            completed_at=(AT_DT + timedelta(seconds=2)).isoformat())
        fixture.write_manifest()
        result = adapt_saved_bundle(fixture.root, now=NOW_LATER)
        self.assertTrue(any('IDENTITY_CONFLICT' in code
                            for code in result['blockers']))

    def test_raw_mutation_symlink_bounds_and_time_rejected(self):
        result = adapt_saved_bundle(
            self.temp.name,
            now=datetime.fromisoformat(OBSERVED) + timedelta(seconds=31))
        self.assertIn('BUNDLE_STALE_OBSERVATION', result['blockers'])
        page = next(page for page in self.fixture.pages
                    if page['tool'] == 'get_equity_positions')
        path = self.fixture.raw / Path(page['raw_path']).name
        path.write_bytes(b'{"data":{"positions":[],"extra":1}}')
        result = self.adapt()
        self.assertIn('BUNDLE_RAW_HASH_MISMATCH', result['blockers'])

        fixture = BundleFixture(Path(self.temp.name + '-symlink'))
        page = next(page for page in fixture.pages
                    if page['tool'] == 'get_equity_positions')
        path = fixture.raw / Path(page['raw_path']).name
        target = Path(self.temp.name) / 'outside.raw'
        target.write_bytes(b'x')
        path.unlink()
        path.symlink_to(target)
        result = adapt_saved_bundle(fixture.root, now=NOW_LATER)
        self.assertIn('BUNDLE_RAW_EXTRA', result['blockers'])


    def test_two_pass_change_and_unverified_nonempty_schemas(self):
        fixture = BundleFixture(Path(self.temp.name + '-pass'))
        page = next(page for page in fixture.pages
                    if page['tool'] == 'get_equity_positions'
                    and page['pass'] == 2)
        fixture.rewrite_raw(page, {'positions': [{
            'symbol': 'DEMO', 'quantity': '1',
            'shares_available_for_sells': '1', 'shares_held_for_sells': '0',
            'shares_held_for_asset_transfer': '0',
            'shares_held_for_options_events': '0',
            'shares_held_for_stock_grants': '0',
            'shares_pending_from_options_events': '0'}]})
        result = adapt_saved_bundle(fixture.root, now=NOW_LATER)
        self.assertIn('TWO_PASS_MISMATCH:get_equity_positions',
                      result['blockers'])

        fixture = BundleFixture(Path(self.temp.name + '-crypto-orders'))
        page = next(page for page in fixture.pages
                    if page['tool'] == 'get_crypto_orders'
                    and page['pass'] == 1)
        fixture.rewrite_raw(page, {'results': [{'id': 'unverified'}]})
        result = adapt_saved_bundle(fixture.root, now=NOW_LATER)
        self.assertTrue(any('BUNDLE_SCHEMA_UNVERIFIED' in code
                            for code in result['blockers']))

    def test_future_unverified_approvals_dollar_and_option_filter(self):
        page = next(page for page in self.fixture.pages
                    if page['tool'] == 'get_equity_positions'
                    and page['pass'] == 2)
        page['completed_at'] = (datetime.fromisoformat(OBSERVED)
                                + timedelta(seconds=2)).isoformat()
        self.fixture.write_manifest()
        result = self.adapt()
        self.assertTrue(any(code.startswith('BUNDLE_FUTURE')
                            for code in result['blockers']))

        fixture = BundleFixture(Path(self.temp.name + '-approval'))
        page = next(page for page in fixture.pages
                    if page['tool'] == 'get_trade_approvals'
                    and page['pass'] == 1)
        fixture.rewrite_raw(page, {'approvals': [{
            'approval_id': 'approval-1', 'account_number': ACCOUNT,
            'asset_class': 'ASSET_CLASS_EQUITY',
            'state': 'APPROVAL_STATE_PENDING', 'is_edited': False}]})
        result = adapt_saved_bundle(fixture.root, now=NOW_LATER)
        self.assertTrue(any('BUNDLE_SCHEMA_UNVERIFIED' in code
                            for code in result['blockers']))

        fixture = BundleFixture(Path(self.temp.name + '-dollar'))
        page = next(page for page in fixture.pages
                    if page['tool'] == 'get_equity_orders'
                    and page['pass'] == 1)
        fixture.rewrite_raw(page, {'orders': [{
            'id': 'dollar-order', 'symbol': 'DEMO', 'side': 'buy',
            'type': 'market', 'trigger': 'immediate', 'state': 'confirmed',
            'quantity': None, 'cumulative_quantity': '0', 'executions': [],
            'created_at': AT, 'market_hours': 'regular_hours',
            'time_in_force': 'gfd'}]})
        result = adapt_saved_bundle(fixture.root, now=NOW_LATER)
        self.assertTrue(any('UNPROVEN_ORDER_QUANTITY' in code
                            for code in result['blockers']))

        fixture = BundleFixture(Path(self.temp.name + '-option-filter'))
        page = next(page for page in fixture.pages
                    if page['tool'] == 'get_option_positions'
                    and page['pass'] == 1)
        page['args']['nonzero'] = True
        fixture.write_manifest()
        result = adapt_saved_bundle(fixture.root, now=NOW_LATER)
        self.assertTrue(any('QUERY_SCOPE_PARTIAL' in code
                            for code in result['blockers']))

    def test_future_fill_is_rejected(self):
        fixture = BundleFixture(Path(self.temp.name + '-future-fill'))
        page = next(page for page in fixture.pages
                    if page['tool'] == 'get_equity_orders'
                    and page['pass'] == 1)
        fixture.rewrite_raw(page, {'orders': [{
            'id': 'future-order', 'symbol': 'DEMO', 'side': 'buy',
            'type': 'market', 'trigger': 'immediate', 'state': 'filled',
            'quantity': '1', 'cumulative_quantity': '1',
            'executions': [{
                'id': 'future-exec', 'price': '40', 'quantity': '1',
                'fees': '0',
                'timestamp': (AT_DT + timedelta(seconds=100)).isoformat()}],
            'created_at': AT, 'market_hours': 'regular_hours',
            'time_in_force': 'gfd'}]})
        result = adapt_saved_bundle(fixture.root, now=NOW_LATER)
        self.assertTrue(any(code.startswith('BUNDLE_')
                            for code in result['blockers']))

    def test_missing_supported_tool_is_condition_blocker(self):
        removed = [page for page in self.fixture.pages
                  if page['tool'] == 'get_option_positions']
        self.fixture.pages = [page for page in self.fixture.pages
                              if page['tool'] != 'get_option_positions']
        for page in removed:
            (self.fixture.root / page['raw_path']).unlink()
        self.fixture.write_manifest()
        result = self.adapt()
        self.assertIn('TWO_PASS_REQUIRED:get_option_positions',
                      result['blockers'])
        self.assertFalse(result['ready'])

    def test_filtered_query_never_marks_account_scope_complete(self):
        for page in self.fixture.pages:
            if page['tool'] == 'get_equity_orders':
                page['args']['state'] = 'filled'
        self.fixture.write_manifest()
        result = self.adapt()
        self.assertTrue(any('QUERY_SCOPE_FILTERED:get_equity_orders' in b
                            for b in result['blockers']))
        self.assertNotEqual(
            result['coverage']['get_equity_orders']['status'], 'complete')
        self.assertIsNone(result['candidate_snapshot'])

    def test_second_pass_must_start_after_first_pass_completes(self):
        page = next(page for page in self.fixture.pages
                    if page['tool'] == 'get_equity_positions'
                    and page['pass'] == 2)
        page['started_at'] = (AT_DT + timedelta(seconds=1)).isoformat()
        page['completed_at'] = (AT_DT + timedelta(seconds=2)).isoformat()
        self.fixture.write_manifest()
        result = self.adapt()
        self.assertIn('BUNDLE_PASS_TIME_OVERLAP', result['blockers'])

    def test_filled_option_order_requires_execution_quantities(self):
        fixture = BundleFixture(Path(self.temp.name + '-option-fill'))
        for page in fixture.pages:
            if page['tool'] == 'get_option_orders':
                fixture.rewrite_raw(page, {'orders': [{
                    'id': 'option-order', 'state': 'filled',
                    'quantity': '1', 'processed_quantity': '1',
                    'pending_quantity': '0',
                    'legs': [{
                        'id': 'leg-1', 'option_id': 'option-1',
                        'side': 'buy', 'position_effect': 'open',
                        'ratio_quantity': 1, 'executions': []}]}]})
        result = adapt_saved_bundle(fixture.root, now=NOW_LATER)
        self.assertTrue(any('BUNDLE_OPTION_ORDER_EXECUTIONS_INCOMPLETE' in b
                            for b in result['blockers']))
        self.assertNotEqual(
            result['coverage']['get_option_orders']['status'], 'complete')

    def test_loaded_mutable_data_is_revalidated_from_raw(self):
        fixture = BundleFixture(Path(self.temp.name + '-loaded'))
        page = next(page for page in fixture.pages
                    if page['tool'] == 'get_equity_positions'
                    and page['pass'] == 1)
        fixture.rewrite_raw(page, {'positions': [{
            'symbol': 'DEMO', 'quantity': '1',
            'shares_available_for_sells': '1', 'shares_held_for_sells': '0',
            'shares_held_for_asset_transfer': '0',
            'shares_held_for_options_events': '0',
            'shares_held_for_stock_grants': '0',
            'shares_pending_from_options_events': '0'}]})
        loaded = load_saved_bundle(fixture.root)
        target = next(page for page in loaded['pages']
                      if page['tool'] == 'get_equity_positions'
                      and page['pass'] == 1)
        target['data']['positions'] = []
        target['args']['account_number'] = 'MUTATED'
        target['cursor_returned'] = 'MUTATED'
        result = normalize_bundle(loaded, now=NOW_LATER)
        self.assertTrue(any('NONFLAT_POSITION:equity:DEMO' in b
                            for b in result['blockers']))
        self.assertEqual(result['observations']['equity_positions'][0]['quantity'],
                         '1')

    def test_unknown_observations_remain_none(self):
        fixture = BundleFixture(Path(self.temp.name + '-unknown'))
        removed = [page for page in fixture.pages
                   if page['tool'] in {'get_option_positions',
                                       'get_option_orders'}]
        fixture.pages = [page for page in fixture.pages
                         if page['tool'] not in {'get_option_positions',
                                                 'get_option_orders'}]
        for page in removed:
            (fixture.root / page['raw_path']).unlink()
        fixture.write_manifest()
        result = adapt_saved_bundle(fixture.root, now=NOW_LATER)
        self.assertIsNone(result['observations']['option_positions'])
        self.assertIsNone(result['observations']['option_orders'])
        self.assertFalse(result['ready'])
        self.assertIsNone(result['candidate_snapshot'])

    def test_observed_crypto_empty_shape_is_bound_and_complete(self):
        data = {'crypto_account_number': RHC, 'rhs_account_number': RHS,
                'results': []}
        self.fixture.set_crypto_orders(data)
        result = self.adapt()
        self.assertNotIn('SCHEMA_UNVERIFIED:get_crypto_orders',
                         result['blockers'])
        self.assertNotIn('BUNDLE_CRYPTO_IDENTITY_MISSING', result['blockers'])
        self.assertNotIn('BUNDLE_CRYPTO_IDENTITY_MISMATCH', result['blockers'])
        self.assertEqual(result['coverage']['get_crypto_orders']['status'],
                         'complete')
        self.assertEqual(result['observations']['crypto_orders'], [])
        self.assertIn('GETTER_MISSING:futures_positions', result['blockers'])
        self.assertIn('SETTLEMENT_SOURCE_MISSING', result['blockers'])
        self.assertIsNone(result['baseline_snapshot'])
        self.assertFalse(result['ready'])
        self.assertFalse(result['production_ready'])

    def test_legacy_crypto_empty_shape_still_accepted(self):
        data = {'results': [], 'next': ''}
        self.fixture.set_crypto_orders(data)
        result = self.adapt()
        self.assertEqual(result['coverage']['get_crypto_orders']['status'],
                         'complete')
        self.assertNotIn('SCHEMA_UNVERIFIED:get_crypto_orders',
                         result['blockers'])

    def test_crypto_empty_shape_identity_negatives(self):
        base = {'crypto_account_number': RHC, 'rhs_account_number': RHS,
                'results': []}
        cases = {
            'wrong-rhs': ({**base, 'rhs_account_number': 'RHS-OTHER'},
                          'BUNDLE_CRYPTO_IDENTITY_MISMATCH:pass1'),
            'wrong-rhc': ({**base, 'crypto_account_number': 'RHC-OTHER'},
                          'BUNDLE_CRYPTO_IDENTITY_MISMATCH:pass1'),
            'swapped': ({**base, 'rhs_account_number': RHC,
                         'crypto_account_number': RHS},
                        'BUNDLE_CRYPTO_IDENTITY_MISMATCH:pass1'),
            'nonstring-rhs': ({**base, 'rhs_account_number': 123},
                              'BUNDLE_CRYPTO_IDENTITY_MISMATCH:pass1'),
            'missing-rhs': ({'crypto_account_number': RHC, 'results': []},
                            'BUNDLE_SCHEMA_UNVERIFIED:get_crypto_orders:pass1'),
            'missing-rhc': ({'rhs_account_number': RHS, 'results': []},
                            'BUNDLE_SCHEMA_UNVERIFIED:get_crypto_orders:pass1'),
            'extra-field': ({**base, 'surprise': 'x'},
                            'BUNDLE_SCHEMA_UNVERIFIED:get_crypto_orders:pass1'),
            'null-results': ({**base, 'results': None},
                             'BUNDLE_SCHEMA_UNVERIFIED:get_crypto_orders:pass1'),
            'nonempty-results': ({**base, 'results': [{'id': 'unverified'}]},
                                 'BUNDLE_SCHEMA_UNVERIFIED:get_crypto_orders:pass1'),
            'pagination': ({**base, 'next': 'more'},
                           'BUNDLE_CURSOR_CHAIN'),
        }
        for name, (data, blocker) in cases.items():
            fixture = BundleFixture(Path(self.temp.name + '-crypto-' + name))
            fixture.set_crypto_orders(data)
            result = adapt_saved_bundle(fixture.root, now=NOW_LATER)
            self.assertIn(blocker, result['blockers'], name)
            self.assertNotEqual(
                result['coverage'].get('get_crypto_orders', {})
                .get('status'), 'complete', name)

    def test_crypto_identity_missing_discovery_alias_and_pass2_change(self):
        fixture = BundleFixture(Path(self.temp.name + '-crypto-no-rhc'))
        page = next(page for page in fixture.pages
                    if page['tool'] == 'get_accounts')
        data = account_data()
        data['accounts'][0].pop('rhc_account_number')
        fixture.rewrite_raw(page, data)
        fixture.set_crypto_orders({'crypto_account_number': 'RHC-ANY',
                                   'rhs_account_number': RHS,
                                   'results': []})
        result = adapt_saved_bundle(fixture.root, now=NOW_LATER)
        self.assertIn('BUNDLE_CRYPTO_IDENTITY_MISSING:pass1',
                      result['blockers'])
        self.assertNotEqual(
            result['coverage']['get_crypto_orders']['status'], 'complete')

        fixture = BundleFixture(Path(self.temp.name + '-crypto-pass2'))
        fixture.set_crypto_orders({'crypto_account_number': RHC,
                                   'rhs_account_number': RHS,
                                   'results': []})
        fixture.set_crypto_orders({'crypto_account_number': 'RHC-OTHER',
                                   'rhs_account_number': RHS,
                                   'results': []}, passes=(2,))
        result = adapt_saved_bundle(fixture.root, now=NOW_LATER)
        self.assertIn('BUNDLE_CRYPTO_IDENTITY_MISMATCH:pass2',
                      result['blockers'])
        self.assertNotEqual(
            result['coverage']['get_crypto_orders']['status'], 'complete')

    def test_crypto_request_args_never_use_rhc(self):
        fixture = BundleFixture(Path(self.temp.name + '-crypto-args'))
        for page in fixture.pages:
            if page['tool'] == 'get_crypto_orders':
                page['args']['rhc_account_number'] = RHC
        fixture.write_manifest()
        result = adapt_saved_bundle(fixture.root, now=NOW_LATER)
        self.assertIn('BUNDLE_REQUEST_ARGS_INVALID', result['blockers'])

    def test_crypto_raw_rewrite_requires_new_hash_and_manifest(self):
        fixture = BundleFixture(Path(self.temp.name + '-crypto-tamper'))
        fixture.set_crypto_orders({'crypto_account_number': RHC,
                                   'rhs_account_number': RHS,
                                   'results': []})
        page = next(page for page in fixture.pages
                    if page['tool'] == 'get_crypto_orders'
                    and page['pass'] == 1)
        raw = envelope({'crypto_account_number': 'RHC-OTHER',
                        'rhs_account_number': RHS, 'results': []})
        (fixture.root / page['raw_path']).write_bytes(raw)
        result = adapt_saved_bundle(fixture.root, now=NOW_LATER)
        self.assertIn('BUNDLE_RAW_HASH_MISMATCH', result['blockers'])

        fixture.rewrite_raw(page, {'crypto_account_number': 'RHC-OTHER',
                                   'rhs_account_number': RHS,
                                   'results': []})
        result = adapt_saved_bundle(fixture.root, now=NOW_LATER)
        self.assertIn('BUNDLE_CRYPTO_IDENTITY_MISMATCH:pass1',
                      result['blockers'])

    def test_same_suffix_ambiguous_account_never_inferred(self):
        fixture = BundleFixture(Path(self.temp.name + '-crypto-ambiguous'))
        page = next(page for page in fixture.pages
                    if page['tool'] == 'get_accounts')
        data = account_data()
        duplicate = deepcopy(data['accounts'][0])
        duplicate['account_number'] = 'SIM-10000'
        duplicate['rhs_account_number'] = 'RHS-OTHER'
        duplicate['rhc_account_number'] = 'RHC-OTHER'
        data['accounts'].append(duplicate)
        fixture.rewrite_raw(page, data)
        result = adapt_saved_bundle(fixture.root, now=NOW_LATER)
        self.assertIn('BUNDLE_ACCOUNT_UNBOUND', result['blockers'])

