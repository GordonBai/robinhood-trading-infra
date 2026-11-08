"""Current evidence compiler: pure read-only account scope and funds facts."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
from unittest import TestCase
from unittest.mock import patch

from martin_agentic.baseline_evidence import BundleError
from martin_agentic.current_evidence import (
    CurrentEvidenceError, compile_current_evidence, run_cli)
from test_baseline_evidence import (
    ACCOUNT, AT, NOW_LATER, OBSERVED, BundleFixture, account_data, envelope,
    page_data)
from test_broker import order


URLS = {
    'investment-products': 'https://robinhood.com/us/en/support/articles/investments-you-can-make-on-robinhood/',
    'multiple-accounts': 'https://robinhood.com/us/en/support/articles/multiple-investing-accounts-faq/',
    'agent-account-setup': 'https://robinhood.com/us/en/support/articles/setting-up-an-agent/',
}


class PublicFixture:
    def __init__(self, root, *, retrieved_at=AT):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.root.chmod(0o700)
        self.files = {}
        self.sources = []
        for name, url in URLS.items():
            body = (name + ' pinned public product source\n').encode()
            path = self.root / (name + '.html')
            path.write_bytes(body)
            os.chmod(path, 0o600)
            digest = hashlib.sha256(body).hexdigest()
            self.files[name] = path
            self.sources.append({
                'name': name, 'url': url, 'final_url': url,
                'retrieved_at': retrieved_at, 'bytes': len(body),
                'sha256': digest, 'path': path.name,
            })
        self.manifest = self.root / 'manifest.json'
        self.write()
        self.policy = {
            name: {'url': url,
                   'sha256': next(item['sha256'] for item in self.sources
                                  if item['name'] == name),
                   'conclusions': {}}
            for name, url in URLS.items()
        }

    def write(self):
        self.manifest.write_text(json.dumps({
            'kind': 'public official product documentation',
            'sources': self.sources}, sort_keys=True))
        os.chmod(self.manifest, 0o600)

    def enable_rhf_product_not_applicable(self):
        self.policy['investment-products']['conclusions'] = {
            'rhf_current_equity_options_adr': 'supported',
            'rhf_current_mutual_funds': 'not_supported',
            'rhf_current_fixed_income': 'not_supported',
            'futures': 'rhd_separate_account',
        }
        return self

    def compile(self, root, *, now=NOW_LATER):
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES', self.policy):
            return compile_current_evidence(root, self.manifest, now=now)


class CurrentEvidenceTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.bundle = BundleFixture(Path(self.temp.name) / 'bundle')
        self.public = PublicFixture(Path(self.temp.name) / 'public')

    def evidence(self, **kwargs):
        return self.public.compile(self.bundle.root, **kwargs)

    def test_observed_scope_and_funds_are_facts_not_authority(self):
        result = self.evidence()
        self.assertTrue(result['observation_valid'])
        self.assertTrue(result['bundle_is_current'])
        self.assertFalse(result['entry_authorized'])
        self.assertFalse(result['historical_orders_complete'])
        self.assertFalse(result['settlement_ledger_complete'])
        self.assertNotIn(ACCOUNT, json.dumps(result, ensure_ascii=False))
        self.assertEqual(result['account']['account_mask'], '••••0000')
        scope = result['account_scope_applicability']['scopes']
        self.assertEqual(scope['equity']['positions']['count'], 0)
        self.assertEqual(scope['equity']['orders']['count'], 0)
        self.assertEqual(scope['fixed_income']['orders']['status'],
                         'unknown_getter_missing')
        self.assertEqual(scope['fixed_income']['scope_status'],
                         'unsupported_channel')
        self.assertEqual(scope['mutual_funds']['orders']['status'],
                         'unknown_getter_missing')
        self.assertEqual(scope['mutual_funds']['scope_status'],
                         'unsupported_channel')
        self.assertEqual(scope['futures']['positions']['status'],
                         'unknown_account_scope_unproven')
        self.assertEqual(scope['futures']['scope_status'],
                         'unsupported_channel')
        self.assertEqual(scope['event_contracts']['orders']['status'],
                         'unknown_account_scope_unproven')
        self.assertNotEqual(scope['futures']['positions']['status'], 'not_applicable')
        self.assertFalse(result['account_scope_applicability']['claims'][
            'all_asset_flat_proven'])
        funds = result['funds_availability']
        self.assertEqual(funds['status'], 'observed_current_money_facts')
        self.assertEqual(funds['values']['cash'], '500')
        self.assertEqual(funds['values']['total_value'], '500')
        self.assertEqual(funds['values']['buying_power'], '500')
        self.assertIn('fixed_income_value', funds['values']['asset_values'])
        self.assertFalse(funds['values']['buying_power_equals_unleveraged'] is None)
        self.assertFalse(funds['unsettled_funds']['authoritative_for_spendability'])
        self.assertTrue(funds['unsettled_funds']['may_lag'])
        self.assertFalse(funds['settlement_ledger_complete'])
        self.assertFalse(funds['policy_limits_evaluated'])
        self.assertNotIn('five_calendar', json.dumps(funds))

    def test_verified_public_na_is_account_bound_and_not_order_history(self):
        self.public.enable_rhf_product_not_applicable()
        result = self.evidence()
        self.assertTrue(result['observation_valid'])
        self.assertFalse(result['entry_authorized'])
        scope = result['account_scope_applicability']['scopes']
        for name, conclusion_key in (
                ('fixed_income', 'rhf_current_fixed_income'),
                ('mutual_funds', 'rhf_current_mutual_funds')):
            with self.subTest(name):
                item = scope[name]
                self.assertEqual(item['scope_status'], 'verified_not_applicable')
                self.assertEqual(item['positions']['status'],
                                 'not_applicable_by_public_product_source')
                self.assertEqual(item['orders']['status'],
                                 'not_applicable_by_public_product_source')
                self.assertTrue(item['positions']['not_an_order_observation'])
                self.assertTrue(item['orders']['not_an_order_observation'])
                app = item['applicability']
                self.assertEqual(app['kind'], 'public_product_scope_v1')
                self.assertEqual(app['conclusion_key'], conclusion_key)
                self.assertEqual(app['conclusion'], 'not_supported')
                self.assertEqual(app['account_affiliate'], 'rhf')
                self.assertEqual(app['account_key_hash'],
                                 result['account']['account_key_hash'])
                self.assertEqual(app['source_name'], 'investment-products')
                self.assertEqual(app['manifest_sha256'],
                                 result['account_scope_applicability'][
                                     'product_evidence']['manifest_sha256'])
                self.assertEqual(app['source_sha256'],
                                 next(source['sha256'] for source in
                                      result['account_scope_applicability'][
                                          'product_evidence']['sources']
                                      if source['name'] == 'investment-products'))
                self.assertTrue(item['applicability']['not_an_order_observation'])
        self.assertEqual(
            result['account_scope_applicability']['claims'][
                'not_applicable_scopes'],
            ['fixed_income', 'mutual_funds'])
        for name in ('futures', 'event_contracts'):
            item = scope[name]
            self.assertEqual(item['scope_status'], 'unsupported_channel')
            self.assertEqual(item['positions']['status'],
                             'unknown_account_scope_unproven')
            self.assertEqual(item['orders']['status'],
                             'unknown_account_scope_unproven')
        self.assertFalse(result['account_scope_applicability']['claims'][
            'all_asset_flat_proven'])
        self.assertFalse(result['historical_orders_complete'])

    def test_public_na_rejects_nonzero_other_affiliate_and_stale_source(self):
        self.public.enable_rhf_product_not_applicable()
        page = next(p for p in self.bundle.pages
                    if p['tool'] == 'get_portfolio')
        data = page_data(self.bundle, page)
        data['fixed_income_value'] = '10'
        self.bundle.rewrite_raw(page, data)
        result = self.evidence()
        fixed = result['account_scope_applicability']['scopes']['fixed_income']
        self.assertNotEqual(fixed['scope_status'], 'verified_not_applicable')
        self.assertEqual(fixed['scope_status'], 'unsupported_channel')
        self.assertEqual(fixed['scope_status_reason'],
                         'CURRENT_NONZERO_VALUE_REQUIRES_ORDER_SOURCE')
        self.assertNotIn('applicability', fixed)

        account_page = next(p for p in self.bundle.pages
                            if p['tool'] == 'get_accounts')
        account = account_data()
        account['accounts'][0]['affiliate'] = 'rhx'
        self.bundle.rewrite_raw(account_page, account)
        portfolio = deepcopy(page_data(self.bundle, page))
        portfolio['fixed_income_value'] = '0'
        self.bundle.rewrite_raw(page, portfolio)
        result = self.evidence()
        fixed = result['account_scope_applicability']['scopes']['fixed_income']
        self.assertEqual(fixed['scope_status'], 'unsupported_channel')
        self.assertEqual(fixed['scope_status_reason'],
                         'ACCOUNT_AFFILIATE_UNPROVEN')

        stale = PublicFixture(Path(self.temp.name) / 'public-stale-na',
                              retrieved_at='2025-01-01T00:00:00+00:00')
        stale.enable_rhf_product_not_applicable()
        result = stale.compile(self.bundle.root)
        self.assertFalse(result['observation_valid'])
        fixed = result['account_scope_applicability']['scopes']['fixed_income']
        self.assertEqual(fixed['scope_status'], 'unsupported_channel')
        self.assertEqual(fixed['scope_status_reason'],
                         'PUBLIC_PRODUCT_SOURCE_NOT_VALID')
        self.assertNotIn('applicability', fixed)

    def test_buying_power_semantics_cash_and_negative_pending(self):
        page = next(p for p in self.bundle.pages
                    if p['tool'] == 'get_portfolio')
        data = page_data(self.bundle, page)
        data.update({'cash': '400', 'total_value': '500',
                     'pending_deposits': '0'})
        data['buying_power'].update({'buying_power': '500',
                                     'unleveraged_buying_power': '500'})
        self.bundle.rewrite_raw(page, data)
        result = self.evidence()
        funds = result['funds_availability']
        self.assertEqual(funds['status'], 'observed_current_money_facts')
        self.assertEqual(funds['values']['cash'], '400')
        self.assertEqual(funds['values']['buying_power'], '500')
        self.assertFalse(funds['values']['cash_equals_buying_power'])
        self.assertTrue(funds['values']['cash_is_not_buying_power'])
        self.assertTrue(funds['values'][
            'broker_buying_power_authoritative_for_spendability'])
        self.assertEqual(funds['values']['buying_power_source'],
                         'get_portfolio.buying_power.buying_power')
        self.assertTrue(funds['buying_power_authority'][
            'cash_is_not_buying_power'])

        data['pending_deposits'] = '-1'
        self.bundle.rewrite_raw(page, data)
        result = self.evidence()
        self.assertFalse(result['observation_valid'])
        self.assertEqual(result['funds_availability']['status'], 'blocked')
        self.assertTrue(any('FUNDS_PENDING_DEPOSITS_NEGATIVE' in b
                            for b in result['blockers']))

    def test_stale_and_future_bundle_are_not_current(self):
        stale = self.evidence(now=datetime_from(OBSERVED) + timedelta(seconds=31))
        self.assertFalse(stale['bundle_is_current'])
        self.assertIn('CURRENT_EVIDENCE_STALE', stale['blockers'])
        future = self.evidence(now=datetime_from(OBSERVED) - timedelta(seconds=1))
        self.assertFalse(future['observation_valid'])
        self.assertIn('CURRENT_EVIDENCE_FUTURE', future['blockers'])

    def test_money_float_bool_nonfinite_currency_and_asset_values_are_blocked(self):
        cases = {
            'total_value_nonfinite': ('total_value', 'NaN'),
            'total_value_float': ('total_value', 1.5),
            'cash_bool': ('cash', True),
            'currency': ('currency', 'EUR'),
            'fixed_income_nonfinite': ('fixed_income_value', 'Infinity'),
            'buying_power_float': ('buying_power', 1.5),
            'buying_power_bool': ('buying_power', True),
        }
        for label, (key, value) in cases.items():
            with self.subTest(label):
                root = Path(self.temp.name) / label
                bundle = BundleFixture(root / 'bundle')
                public = PublicFixture(root / 'public')
                page = next(p for p in bundle.pages
                            if p['tool'] == 'get_portfolio')
                data = {
                    'cash': '500', 'total_value': '500',
                    'pending_deposits': '0', 'currency': 'USD',
                    'options_value': '0', 'crypto_value': '0',
                    'futures_value': '0', 'event_contracts_value': '0',
                    'fixed_income_value': '0', 'mutual_funds_value': '0',
                    'buying_power': {'buying_power': '500',
                                     'unleveraged_buying_power': '500',
                                     'display_currency': 'USD'},
                }
                if key == 'buying_power':
                    data['buying_power']['buying_power'] = value
                else:
                    data[key] = value
                bundle.rewrite_raw(page, data)
                result = public.compile(bundle.root)
                self.assertFalse(result['observation_valid'], label)
                self.assertFalse(result['entry_authorized'], label)

    def test_scope_events_are_independent_and_duplicate_conflicts_are_unknown(self):
        # Empty positions plus a real order: the order count is not inferred
        # from the empty position list.
        self.bundle.rewrite_raw(
            next(p for p in self.bundle.pages
                 if p['tool'] == 'get_equity_orders' and p['pass'] == 1),
            {'orders': [order('one', 'buy', '1', '0', 'confirmed')]})
        self.bundle.rewrite_raw(
            next(p for p in self.bundle.pages
                 if p['tool'] == 'get_equity_orders' and p['pass'] == 2),
            {'orders': [order('one', 'buy', '1', '0', 'confirmed')]})
        result = self.evidence()
        scope = result['account_scope_applicability']['scopes']['equity']
        self.assertEqual(scope['positions']['count'], 0)
        self.assertEqual(scope['orders']['count'], 1)
        self.assertTrue(result['observation_valid'])

        # Same identity/same content across pages dedupes; conflict is not a
        # complete range and does not retain an arbitrary first record.
        a = order('dup', 'buy', '1', '0', 'confirmed')
        b = dict(a)
        b['state'] = 'cancelled'
        self.bundle.rewrite_raw(
            next(p for p in self.bundle.pages
                 if p['tool'] == 'get_equity_orders' and p['pass'] == 1),
            {'orders': [a, a, b]})
        self.bundle.rewrite_raw(
            next(p for p in self.bundle.pages
                 if p['tool'] == 'get_equity_orders' and p['pass'] == 2),
            {'orders': [a, a, b]})
        result = self.evidence()
        scope = result['account_scope_applicability']['scopes']['equity']
        self.assertFalse(result['observation_valid'])
        self.assertEqual(scope['orders']['status'], 'unknown_conflict')
        self.assertNotIn('open_count', scope['orders'])

    def test_partial_scope_failure_does_not_erase_other_side(self):
        page = next(p for p in self.bundle.pages
                    if p['tool'] == 'get_equity_positions' and p['pass'] == 1)
        self.bundle.rewrite_raw(page, {'positions': [{'symbol': 'DEMO',
                                                       'quantity': '1'}]})
        result = self.evidence()
        scope = result['account_scope_applicability']['scopes']['equity']
        self.assertFalse(result['observation_valid'])
        self.assertEqual(scope['positions']['status'], 'unknown_schema_unverified')
        self.assertEqual(scope['orders']['status'], 'observed_complete')
        self.assertEqual(scope['orders']['count'], 0)

    def test_two_pass_mismatch_is_unstable(self):
        first = next(p for p in self.bundle.pages
                     if p['tool'] == 'get_equity_orders' and p['pass'] == 1)
        second = next(p for p in self.bundle.pages
                      if p['tool'] == 'get_equity_orders' and p['pass'] == 2)
        self.bundle.rewrite_raw(first, {'orders': [order('x', 'buy', '1', '0',
                                                         'confirmed')]})
        self.bundle.rewrite_raw(second, {'orders': [order('x', 'buy', '1', '0',
                                                          'cancelled')]})
        result = self.evidence()
        scope = result['account_scope_applicability']['scopes']['equity']
        self.assertFalse(result['observation_valid'])
        self.assertEqual(scope['orders']['status'], 'unknown_unstable')
        self.assertNotIn('open_count', scope['orders'])

    def test_approvals_missing_and_nonempty_are_not_count_zero(self):
        pages = [p for p in self.bundle.pages
                 if p['tool'] == 'get_trade_approvals']
        for page in pages:
            self.bundle.pages.remove(page)
            (self.bundle.raw / Path(page['raw_path']).name).unlink()
        self.bundle.write_manifest()
        result = self.evidence()
        self.assertFalse(result['observation_valid'])
        self.assertEqual(result['account_scope_applicability']['approvals']['status'],
                         'unknown_missing')
        self.assertIsNone(result['account_scope_applicability']['approvals']['count'])

    def test_position_identity_conflict_and_duplicate_are_consistent(self):
        full = {'symbol': 'DEMO', 'quantity': '1',
                'shares_available_for_sells': '1', 'shares_held_for_sells': '0',
                'shares_held_for_asset_transfer': '0',
                'shares_held_for_options_events': '0',
                'shares_held_for_stock_grants': '0',
                'shares_pending_from_options_events': '0'}
        # Same value twice dedupes; conflicting quantity is not complete.
        for page in self.bundle.pages:
            if page['tool'] == 'get_equity_positions':
                self.bundle.rewrite_raw(page, {'positions': [full, full]})
        result = self.evidence()
        scope = result['account_scope_applicability']['scopes']['equity']
        self.assertTrue(result['observation_valid'])
        self.assertEqual(scope['positions']['count'], 1)

        other = {**full, 'quantity': '2', 'shares_available_for_sells': '2'}
        for page in self.bundle.pages:
            if page['tool'] == 'get_equity_positions':
                self.bundle.rewrite_raw(page, {'positions': [full, other]})
        result = self.evidence()
        scope = result['account_scope_applicability']['scopes']['equity']
        self.assertFalse(result['observation_valid'])
        self.assertEqual(scope['positions']['status'], 'unknown_conflict')
        self.assertNotIn('count', scope['positions'])

    def test_unsettled_invalid_is_informational_not_money_fact(self):
        page = next(p for p in self.bundle.pages
                    if p['tool'] == 'get_accounts')
        data = account_data()
        data['accounts'][0]['unsettled_funds'] = 'NaN'
        self.bundle.rewrite_raw(page, data)
        result = self.evidence()
        funds = result['funds_availability']
        self.assertFalse(result['observation_valid'])
        self.assertEqual(funds['status'], 'money_facts_valid_informational_invalid')
        self.assertTrue(funds['money_facts_valid'])
        self.assertFalse(funds['informational_valid'])
        self.assertFalse(funds['unsettled_funds']['authoritative_for_spendability'])
        self.assertIn('FUNDS_UNSETTLED_INVALID', result['blockers'])

    def test_approval_setting_requires_real_bool_and_preserves_updated_at(self):
        page = next(p for p in self.bundle.pages
                    if p['tool'] == 'get_trade_approval_setting')
        self.bundle.rewrite_raw(page, {'setting': {
            'account_number': ACCOUNT,
            'human_must_approve_trades': 'false',
            'updated_at': AT}})
        result = self.evidence()
        self.assertFalse(result['observation_valid'])
        self.assertEqual(result['account_scope_applicability']['approval_setting']['status'],
                         'unknown_invalid')
        self.assertIn('BUNDLE_APPROVAL_SETTING_TYPE_INVALID', result['blockers'])

        self.bundle.rewrite_raw(page, {'setting': {
            'account_number': ACCOUNT,
            'human_must_approve_trades': False,
            'updated_at': AT}})
        result = self.evidence()
        setting = result['account_scope_applicability']['approval_setting']
        self.assertTrue(result['observation_valid'])
        self.assertEqual(setting['status'], 'observed_single_pass')
        self.assertIs(setting['human_must_approve_trades'], False)
        self.assertTrue(setting['updated_at_present'])
        self.assertIsNotNone(setting['updated_at'])

    def test_public_hash_url_or_expiry_change_is_blocked(self):
        self.public.files['investment-products'].write_bytes(b'changed')
        self.public.write()
        result = self.evidence()
        self.assertFalse(result['observation_valid'])
        self.assertTrue(any('PUBLIC_SOURCE_HASH' in b
                            for b in result['blockers']))

        self.public = PublicFixture(Path(self.temp.name) / 'public-url')
        self.public.sources[0]['url'] = 'https://example.com/fake'
        self.public.write()
        result = self.evidence()
        self.assertFalse(result['observation_valid'])
        self.assertTrue(any('PUBLIC_SOURCE_URL' in b
                            for b in result['blockers']))

        self.public = PublicFixture(Path(self.temp.name) / 'public-expired',
                                    retrieved_at='2025-01-01T00:00:00+00:00')
        result = self.evidence()
        self.assertFalse(result['observation_valid'])
        self.assertTrue(any('PUBLIC_SOURCE_EXPIRED' in b
                            for b in result['blockers']))

    def test_missing_pass_and_bundle_hash_are_fail_closed(self):
        page = next(p for p in self.bundle.pages
                    if p['tool'] == 'get_equity_positions' and p['pass'] == 2)
        self.bundle.pages.remove(page)
        (self.bundle.raw / Path(page['raw_path']).name).unlink()
        self.bundle.write_manifest()
        result = self.evidence()
        self.assertFalse(result['observation_valid'])
        self.assertTrue(any('TWO_PASS_REQUIRED:get_equity_positions' in b
                            for b in result['blockers']))

        page1 = next(p for p in self.bundle.pages
                     if p['tool'] == 'get_equity_positions' and p['pass'] == 1)
        raw = self.bundle.raw / Path(page1['raw_path']).name
        raw.write_bytes(envelope({'positions': []}) + b'x')
        with self.assertRaises(BundleError) as raised:
            self.evidence()
        self.assertEqual(raised.exception.code, 'BUNDLE_RAW_HASH_MISMATCH')

    def test_cross_account_binding_is_rejected(self):
        page = next(p for p in self.bundle.pages
                    if p['tool'] == 'get_accounts')
        self.bundle.rewrite_raw(page, {'accounts': [{
            'account_number': 'OTHER-9999',
            'rhs_account_number': 'RHS-9999', 'rhc_account_number': 'RHC-9999',
            'agentic_allowed': True, 'deactivated': False,
            'permanently_deactivated': False, 'state': 'active'}]})
        with self.assertRaises(BundleError) as raised:
            self.evidence()
        self.assertEqual(raised.exception.code, 'BUNDLE_ACCOUNT_UNBOUND')

        fixture = BundleFixture(Path(self.temp.name) / 'bundle-arg')
        page = next(p for p in fixture.pages
                    if p['tool'] == 'get_equity_positions' and p['pass'] == 1)
        page['args']['account_number'] = 'OTHER-9999'
        fixture.write_manifest()
        public = PublicFixture(Path(self.temp.name) / 'public-arg')
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   public.policy):
            with self.assertRaises(BundleError) as raised:
                compile_current_evidence(fixture.root, public.manifest,
                                         now=NOW_LATER)
        self.assertEqual(raised.exception.code, 'BUNDLE_ACCOUNT_MISMATCH')

    def test_account_mismatch_and_invalid_money_are_blocked(self):
        page = next(p for p in self.bundle.pages
                    if p['tool'] == 'get_portfolio')
        page_data = {'cash': 'NaN', 'total_value': '500',
                     'pending_deposits': '0', 'currency': 'USD',
                     'options_value': '0', 'crypto_value': '0',
                     'futures_value': '0', 'event_contracts_value': '0',
                     'fixed_income_value': '0', 'mutual_funds_value': '0',
                     'buying_power': {'buying_power': '500',
                                      'unleveraged_buying_power': '500',
                                      'display_currency': 'USD'}}
        self.bundle.rewrite_raw(page, page_data)
        result = self.evidence()
        self.assertEqual(result['funds_availability']['status'], 'blocked')
        self.assertFalse(result['entry_authorized'])

    def _make_current(self, bundle):
        now = datetime.now(timezone.utc)
        base = now - timedelta(seconds=10)
        for page in bundle.pages:
            if page['pass'] == 1:
                page['started_at'] = base.isoformat()
                page['completed_at'] = (base + timedelta(seconds=1)).isoformat()
            else:
                page['started_at'] = (base + timedelta(seconds=2)).isoformat()
                page['completed_at'] = (base + timedelta(seconds=3)).isoformat()
        bundle.write_manifest()
        return now

    def test_cli_returns_3_for_structural_and_conflict_negatives(self):
        cases = [
            ('get_equity_positions', {'positions': [{'symbol': 'DEMO', 'quantity': '1'}]}),
            ('get_crypto_orders', {'results': [{'id': 'unparsed-open-order'}]}),
            ('get_trade_approvals', {'approvals': [{'id': 'pending-1'}]}),
        ]
        a = order('duplicate', 'buy', '1', '0', 'confirmed')
        b = dict(a)
        b['state'] = 'cancelled'
        cases.append(('get_equity_orders', {'orders': [a, b]}))
        bad_account = account_data()
        bad_account['accounts'][0]['unsettled_funds'] = 'NaN'
        cases.append(('get_accounts', bad_account))
        cases.append(('get_trade_approval_setting', {'setting': {
            'account_number': ACCOUNT,
            'human_must_approve_trades': 'false', 'updated_at': AT}}))
        for index, (tool, data) in enumerate(cases):
            with self.subTest(tool):
                root = Path(self.temp.name) / ('cli-negative-' + str(index))
                bundle = BundleFixture(root / 'bundle')
                for page in bundle.pages:
                    if page['tool'] == tool:
                        bundle.rewrite_raw(page, data)
                now = self._make_current(bundle)
                public = PublicFixture(root / 'public',
                                       retrieved_at=(now - timedelta(seconds=1)).isoformat())
                out = io.StringIO()
                with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                           public.policy), patch('sys.stdout', out):
                    code = run_cli(['--bundle-root', str(bundle.root),
                                    '--public-sources', str(public.manifest)])
                self.assertEqual(code, 3)
                value = json.loads(out.getvalue())
                self.assertFalse(value['observation_valid'])
                self.assertFalse(value['entry_authorized'])

    def test_cli_is_read_only_and_never_authorizes(self):
        now = datetime.now(timezone.utc)
        base = now - timedelta(seconds=10)
        for page in self.bundle.pages:
            if page['pass'] == 1:
                page['started_at'] = base.isoformat()
                page['completed_at'] = (base + timedelta(seconds=1)).isoformat()
            else:
                page['started_at'] = (base + timedelta(seconds=2)).isoformat()
                page['completed_at'] = (base + timedelta(seconds=3)).isoformat()
        self.bundle.write_manifest()
        self.public = PublicFixture(Path(self.temp.name) / 'public-current',
                                    retrieved_at=(now - timedelta(seconds=1)).isoformat())
        out = io.StringIO()
        with patch('martin_agentic.current_evidence.PUBLIC_SOURCES',
                   self.public.policy), \
                patch('sys.stdout', out):
            code = run_cli(['--bundle-root', str(self.bundle.root),
                            '--public-sources', str(self.public.manifest)])
        self.assertEqual(code, 0)
        value = json.loads(out.getvalue())
        self.assertFalse(value['entry_authorized'])
        self.assertFalse(value['ready'])
        self.assertNotIn(ACCOUNT, out.getvalue())


def datetime_from(value):
    from datetime import datetime
    return datetime.fromisoformat(value)
