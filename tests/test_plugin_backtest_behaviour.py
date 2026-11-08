"""Default-discovered external-plugin backtest semantics and negative controls.

These run against a freshly built, non-editable installation of the public
example distribution and synthetic probe fixtures. No provider, broker, ledger
or model is contacted.
"""
import json
from unittest import TestCase

from external_environment import ExternalEnvironment


SCRIPT = r'''
import copy
import json
from pathlib import Path

from trading_infra.plugin_backtest import run_backtest

BASE = Path('examples/plugins/example_momentum')
CONFIG = json.loads((BASE / 'config.json').read_text())
DATASETS = {name: json.loads((BASE / name).read_text()) for name in (
    'events-momentum.json', 'events-mean-reversion.json', 'events-expired.json')}


def run(plugin='example-momentum', dataset=None, config=None, **kwargs):
    return run_backtest(plugin, CONFIG if config is None else config,
                        copy.deepcopy(dataset), **kwargs)


def captured(call):
    try:
        return {'ok': True, 'report': call()}
    except ValueError as exc:
        return {'ok': False, 'error': str(exc)}


def quote_dataset(*quotes, start=0):
    rows = []
    base = 14 * 3600
    for index, (bid, ask, offset) in enumerate(quotes):
        total = base + offset
        stamp = '2026-10-16T%02d:%02d:%02d+00:00' % (
            total // 3600, (total % 3600) // 60, total % 60)
        rows.append({'id': 'q%d' % (index + 1), 'at': stamp, 'kind': 'quote',
                     'data': {'symbol': 'DEMO', 'bid': bid, 'ask': ask}})
    return {'schema_version': 1, 'synthetic': True,
            'max_quote_age_seconds': 15, 'events': rows}


def main():
    out = {}
    momentum = run(dataset=DATASETS['events-momentum.json'])
    mean = run(plugin='example-mean-reversion',
               dataset=DATASETS['events-mean-reversion.json'])
    two = {
        'schema_version': 1, 'synthetic': True, 'max_quote_age_seconds': 15,
        'events': [
            {'id': 't1', 'at': '2026-10-16T14:00:00+00:00', 'kind': 'quote',
             'data': {'symbol': 'DEMO', 'bid': '40.00', 'ask': '40.01'}},
            {'id': 't2', 'at': '2026-10-16T14:00:10+00:00', 'kind': 'quote',
             'data': {'symbol': 'DEMO', 'bid': '39.90', 'ask': '39.91'}},
            {'id': 't3', 'at': '2026-10-16T14:00:20+00:00', 'kind': 'quote',
             'data': {'symbol': 'DEMO', 'bid': '41.00', 'ask': '41.01'}},
            {'id': 't4', 'at': '2026-10-16T14:00:30+00:00', 'kind': 'quote',
             'data': {'symbol': 'DEMO', 'bid': '41.20', 'ask': '41.21'}},
            {'id': 't5', 'at': '2026-10-16T14:00:40+00:00', 'kind': 'quote',
             'data': {'symbol': 'DEMO', 'bid': '39.50', 'ask': '39.51'}},
            {'id': 't6', 'at': '2026-10-16T14:00:50+00:00', 'kind': 'quote',
             'data': {'symbol': 'DEMO', 'bid': '39.40', 'ask': '39.41'}},
            {'id': 't7', 'at': '2026-10-16T14:01:00+00:00', 'kind': 'quote',
             'data': {'symbol': 'DEMO', 'bid': '40.50', 'ask': '40.51'}},
            {'id': 't8', 'at': '2026-10-16T14:01:10+00:00', 'kind': 'quote',
             'data': {'symbol': 'DEMO', 'bid': '41.00', 'ask': '41.01'}}]}
    two_report = run(plugin='example-mean-reversion', dataset=two)
    expired = run(dataset=DATASETS['events-expired.json'])
    out['roundtrips'] = {
        'momentum': {key: momentum[key] for key in (
            'trade_count', 'trades', 'unfilled_orders', 'initial_equity',
            'final_equity', 'gross_pnl', 'fees', 'slippage', 'net_pnl',
            'accounting', 'time_model', 'end_of_data_liquidation',
            'end_of_data_liquidation_policy')},
        'mean_reversion': {key: mean[key] for key in (
            'trade_count', 'trades', 'unfilled_orders', 'initial_equity',
            'final_equity', 'gross_pnl', 'fees', 'slippage', 'net_pnl',
            'accounting', 'time_model', 'end_of_data_liquidation',
            'end_of_data_liquidation_policy')},
        'expired': {key: expired[key] for key in (
            'trade_count', 'trades', 'unfilled_orders', 'final_equity')},
        'two_breakouts': {key: two_report[key] for key in (
            'trade_count', 'trades', 'unfilled_orders', 'final_equity',
            'net_pnl')}}

    future_quote = copy.deepcopy(DATASETS['events-momentum.json'])
    future_quote['events'][1]['data']['observed_at'] = '2099-01-01T14:00:00Z'
    before = json.dumps(future_quote, sort_keys=True)
    out['future_quote'] = captured(lambda: run(dataset=future_quote))
    out['future_quote_input_unchanged'] = (json.dumps(future_quote, sort_keys=True)
                                           == before)
    for field in ('bid_at', 'ask_at', 'observed_at'):
        case = copy.deepcopy(DATASETS['events-momentum.json'])
        case['events'][1]['data'][field] = '2099-01-01T14:00:00Z'
        out['future_' + field] = captured(lambda case=case: run(dataset=case))

    naive = copy.deepcopy(DATASETS['events-momentum.json'])
    naive['events'][1]['data']['bid_at'] = '2026-10-16T14:00:10'
    out['naive_quote'] = captured(lambda: run(dataset=naive))

    quote_order = copy.deepcopy(DATASETS['events-momentum.json'])
    quote_order['events'][0]['data']['observed_at'] = '2026-10-16T14:00:00+00:00'
    quote_order['events'][1]['data']['observed_at'] = '2026-10-16T13:59:55+00:00'
    out['out_of_order_quote'] = captured(lambda: run(dataset=quote_order))

    stale = copy.deepcopy(DATASETS['events-momentum.json'])
    stale['events'][2]['data']['ask_at'] = '2026-10-16T13:59:00+00:00'
    out['stale_quote'] = captured(lambda: run(dataset=stale))

    naive_event = copy.deepcopy(DATASETS['events-momentum.json'])
    naive_event['events'][1]['at'] = '2026-10-16T14:00:10'
    out['naive_event'] = captured(lambda: run(dataset=naive_event))

    future_event = copy.deepcopy(DATASETS['events-momentum.json'])
    future_event['as_of'] = '2026-10-16T14:00:05+00:00'
    out['future_event'] = captured(lambda: run(dataset=future_event))

    out_of_order = copy.deepcopy(DATASETS['events-momentum.json'])
    out_of_order['events'][2]['at'] = '2026-10-16T14:00:05+00:00'
    out['out_of_order'] = captured(lambda: run(dataset=out_of_order))

    cross_symbol = copy.deepcopy(DATASETS['events-momentum.json'])
    cross_symbol['events'][1]['data']['symbol'] = 'OTHER'
    out['cross_symbol'] = captured(lambda: run(dataset=cross_symbol))

    unmarketable = quote_dataset(('50.00', '50.01', 0), ('50.00', '50.01', 10))
    unmarketable['events'][1]['data']['ask'] = '60.00'
    unmarketable['events'][1]['data']['bid'] = '59.00'
    out['unmarketable'] = captured(
        lambda: run(plugin='test-stop-only', config={}, dataset=unmarketable))

    no_exit_rule = quote_dataset(
        ('50.00', '50.01', 0), ('50.00', '50.01', 10),
        ('30.00', '30.01', 20), ('29.00', '29.01', 30),
        ('60.00', '60.01', 40))
    out['structural_stop'] = captured(
        lambda: run(plugin='test-stop-only', config={}, dataset=no_exit_rule))

    final_event_stop = quote_dataset(
        ('50.00', '50.01', 0), ('50.00', '50.01', 10),
        ('30.00', '30.01', 20))
    out['final_event_stop'] = captured(
        lambda: run(plugin='test-stop-only', config={}, dataset=final_event_stop))
    out['final_event_no_liquidation'] = captured(
        lambda: run(plugin='test-stop-only', config={}, dataset=final_event_stop,
                    end_of_data_policy='NONE'))

    expired_exit = copy.deepcopy(DATASETS['events-mean-reversion.json'])
    expired_exit['events'] = expired_exit['events'][:4]
    expired_exit['events'][1]['data'].update({'bid': '39.90', 'ask': '39.91'})
    expired_exit['events'][3]['at'] = '2026-10-16T14:00:41+00:00'
    out['exit_ttl_expired'] = captured(
        lambda: run(plugin='example-mean-reversion', dataset=expired_exit,
                    end_of_data_policy='NONE'))

    reduce = quote_dataset(('50.00', '50.01', 0), ('50.00', '50.01', 10),
                           ('50.00', '50.01', 20), ('50.00', '50.01', 30),
                           ('50.00', '50.01', 40))
    out['reduce'] = captured(lambda: run(
        plugin='test-actions', config={'mode': 'reduce'}, dataset=reduce))

    tighten = quote_dataset(('50.00', '50.01', 0), ('50.00', '50.01', 10),
                            ('50.00', '50.01', 20), ('49.20', '49.21', 30),
                            ('49.00', '49.01', 40))
    out['tighten'] = captured(lambda: run(
        plugin='test-actions', config={'mode': 'tighten'}, dataset=tighten))

    always_exit = quote_dataset(('50.00', '50.01', 0), ('50.00', '50.01', 10),
                                ('50.00', '50.01', 20), ('50.00', '50.01', 30))
    out['always_exit'] = captured(lambda: run(
        plugin='test-always-exit', config={}, dataset=always_exit))

    out['bad_action'] = captured(lambda: run(
        plugin='test-bad-action', config={},
        dataset=quote_dataset(('50.00', '50.01', 0), ('50.00', '50.01', 10),
                              ('50.00', '50.01', 20))))

    out['bars'] = captured(lambda: run(plugin='test-bars', config={},
                                       dataset=no_exit_rule))
    out['warmup'] = captured(lambda: run(plugin='test-warmup', config={},
                                         dataset=no_exit_rule))

    out['exact_one_share_cash'] = captured(lambda: run(
        plugin='test-stop-only', config={},
        dataset=quote_dataset(('49.99', '50.00', 0), ('49.99', '50.00', 10)),
        initial_equity='50.01', slippage_per_share='0', fee_per_share='0.01'))

    print(json.dumps(out, sort_keys=True, default=str))


main()
'''


class ExternalBacktestBehaviourTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.environment = ExternalEnvironment.shared()

    def setUp(self):
        self.result = self.environment.run_script(SCRIPT)

    def assert_error(self, name, code):
        item = self.result[name]
        self.assertFalse(item['ok'], (name, item))
        self.assertEqual(item['error'], code, name)

    def test_public_template_roundtrips_and_expiry(self):
        rounds = self.result['roundtrips']
        momentum = rounds['momentum']
        self.assertEqual(momentum['trade_count'], 1, momentum)
        self.assertEqual(momentum['trades'][0]['reason'],
                         'MOMENTUM_LOST_AVERAGE', momentum['trades'])
        self.assertEqual(momentum['final_equity'], '10000.65', momentum)
        self.assertEqual(momentum['gross_pnl'], '0.69', momentum)
        self.assertEqual(momentum['fees'], '0.02', momentum)
        self.assertEqual(momentum['slippage'], '0.02', momentum)
        self.assertEqual(momentum['net_pnl'], '0.65', momentum)
        self.assertEqual(momentum['trades'][0]['fees'], '0.02', momentum)
        self.assertEqual(momentum['trades'][0]['slippage'], '0.02', momentum)
        self.assertEqual(momentum['trades'][0]['pnl'], '0.65', momentum)
        self.assertTrue(momentum['accounting']['reconciliation_ok'], momentum)
        self.assertIs(momentum['end_of_data_liquidation'], False, momentum)
        self.assertEqual(momentum['end_of_data_liquidation_policy'],
                         'LIQUIDATE_AT_LAST_BID', momentum)
        self.assertGreater(momentum['time_model']['derived_quote_fields'], 0)
        mean = rounds['mean_reversion']
        self.assertEqual(mean['trade_count'], 1, mean)
        self.assertEqual(mean['trades'][0]['reason'],
                         'MEAN_REVERSION_REACHED_AVERAGE', mean['trades'])
        self.assertEqual(mean['final_equity'], '10000.75', mean)
        self.assertEqual(mean['gross_pnl'], '0.79', mean)
        self.assertEqual(mean['net_pnl'], '0.75', mean)
        self.assertEqual(mean['trades'][0]['fees'], '0.02', mean)
        self.assertEqual(mean['trades'][0]['slippage'], '0.02', mean)
        self.assertEqual(mean['trades'][0]['pnl'], '0.75', mean)
        self.assertTrue(mean['accounting']['reconciliation_ok'], mean)
        two = rounds['two_breakouts']
        self.assertEqual(two['trade_count'], 2, two)
        self.assertEqual([trade['reason'] for trade in two['trades']],
                         ['MEAN_REVERSION_REACHED_AVERAGE'] * 2, two)
        expired = rounds['expired']
        self.assertEqual(expired['trade_count'], 0, expired)
        self.assertIn('LIMIT_EXPIRED',
                      [item['reason'] for item in expired['unfilled_orders']],
                      expired)

    def test_time_semantics_are_unified_and_existing_times_preserved(self):
        self.assert_error('future_quote', 'PLUGIN_BACKTEST_QUOTE_TIME_FUTURE')
        self.assertTrue(self.result['future_quote_input_unchanged'])
        for field in ('bid_at', 'ask_at', 'observed_at'):
            self.assert_error('future_' + field,
                              'PLUGIN_BACKTEST_QUOTE_TIME_FUTURE')
        self.assert_error('naive_quote', 'PLUGIN_BACKTEST_QUOTE_TIME_INVALID')
        self.assert_error('stale_quote', 'PLUGIN_BACKTEST_QUOTE_TIME_STALE')
        self.assert_error('naive_event', 'PLUGIN_BACKTEST_EVENT_TIME_INVALID')
        self.assert_error('future_event', 'PLUGIN_BACKTEST_FUTURE_EVENT')
        self.assert_error('out_of_order', 'PLUGIN_BACKTEST_OUT_OF_ORDER')
        self.assert_error('out_of_order_quote',
                          'PLUGIN_BACKTEST_QUOTE_TIMES_OUT_OF_ORDER')
        self.assert_error('cross_symbol',
                          'PLUGIN_BACKTEST_MULTI_SYMBOL_UNSUPPORTED')

    def test_structural_stop_and_unmarketable_limit(self):
        stop = self.result['structural_stop']
        self.assertTrue(stop['ok'], stop)
        report = stop['report']
        self.assertEqual(report['trade_count'], 1, report)
        self.assertEqual(report['trades'][0]['reason'], 'STRUCTURAL_STOP', report)
        self.assertEqual(report['trades'][0]['exit'], '28.99', report)
        limit = self.result['unmarketable']
        self.assertTrue(limit['ok'], limit)
        self.assertEqual(limit['report']['trade_count'], 0, limit)
        self.assertEqual(limit['report']['unfilled_orders'][0]['reason'],
                         'LIMIT_NOT_MARKETABLE_AT_END', limit)

    def test_final_event_exit_is_not_falsely_executed(self):
        final = self.result['final_event_stop']
        self.assertTrue(final['ok'], final)
        report = final['report']
        self.assertEqual(report['trade_count'], 1, report)
        self.assertEqual(report['trades'][0]['reason'],
                         'END_OF_DATA_LIQUIDATION', report)
        pending = [row for row in report['unfilled_orders']
                   if row['reason'] == 'EXIT_NOT_EXECUTED_AT_END']
        self.assertEqual(len(pending), 1, report)
        self.assertEqual(pending[0]['pending_reason'], 'STRUCTURAL_STOP', report)
        self.assertTrue(pending[0]['structural'], report)
        none = self.result['final_event_no_liquidation']
        self.assertTrue(none['ok'], none)
        self.assertEqual(none['report']['trade_count'], 0, none)
        self.assertEqual(none['report']['open_position']['quantity'], 1, none)
        expired = self.result['exit_ttl_expired']
        self.assertTrue(expired['ok'], expired)
        self.assertIn('EXIT_EXPIRED', [row['reason'] for row in
                                       expired['report']['unfilled_orders']],
                      expired)

    def test_reduce_and_tighten_are_supported(self):
        reduce = self.result['reduce']
        self.assertTrue(reduce['ok'], reduce)
        self.assertEqual(reduce['report']['trade_count'], 1, reduce)
        self.assertEqual(reduce['report']['trades'][0]['reason'],
                         'ACTION_REDUCE', reduce)
        always = self.result['always_exit']
        self.assertTrue(always['ok'], always)
        self.assertEqual(always['report']['trade_count'], 1, always)
        self.assertEqual(always['report']['trades'][0]['reason'],
                         'ALWAYS_EXIT', always)
        self.assertNotIn('EXIT_EXPIRED', [row['reason'] for row in
                                          always['report']['unfilled_orders']],
                         always)
        tight = self.result['tighten']
        self.assertTrue(tight['ok'], tight)
        self.assertEqual(tight['report']['trade_count'], 1, tight)
        self.assertEqual(tight['report']['trades'][0]['reason'],
                         'STRUCTURAL_STOP', tight)
        self.assertEqual(tight['report']['trades'][0]['exit'], '48.99', tight)

    def test_unsupported_data_requirements_and_warmup_are_rejected(self):
        self.assert_error('bad_action',
                          'PLUGIN_BACKTEST_EXIT_RULE_OUTPUT_INVALID')
        self.assert_error('bars', 'PLUGIN_BACKTEST_DATA_REQUIREMENTS_UNSUPPORTED')
        self.assert_error('warmup', 'PLUGIN_BACKTEST_WARMUP_UNSUPPORTED')

    def test_exact_one_share_cash_includes_the_fee_once(self):
        exact = self.result['exact_one_share_cash']
        self.assertTrue(exact['ok'], exact)
        self.assertEqual(exact['report']['trade_count'], 1, exact)
        self.assertEqual(exact['report']['open_position'], None, exact)
