"""Public tutorial rehearsal in a fresh non-editable install layout.

The release is extracted, the example template is copied outside the qualified
candidate, the example wheel is built from that copy, and every documented CLI
step is executed. Source correctness is checked before and after; build or
report outputs must not enter the qualified source set.
"""
import json
import shutil
from pathlib import Path
from unittest import TestCase

from martin_agentic.qualification import (
    _source_pin_scan, source_files, source_fingerprint, source_pin,
)
from external_environment import ExternalEnvironment


class PublicTutorialTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.environment = ExternalEnvironment.shared()

    def cli_json(self, *args, cwd):
        result = self.environment.run_cli(*args, cwd=cwd)
        self.assertEqual(result.returncode, 0,
                         (args, result.stdout[-2000:], result.stderr[-4000:]))
        return json.loads(result.stdout)

    def test_documented_copy_build_and_cli_steps_leave_source_identity_intact(self):
        env = self.environment
        candidate = env.candidate
        work = env.base / 'tutorial-work'
        output = env.base / 'tutorial-output'
        shutil.copytree(candidate / 'examples/plugins/example_momentum', work)
        output.mkdir()

        before_fingerprint = source_fingerprint(candidate)
        before_pin = source_pin(candidate)
        before_files = {path.relative_to(candidate).as_posix()
                        for path in source_files(candidate)}
        self.assertFalse(work.is_relative_to(candidate))

        env.build_public_example(work, work / 'dist')
        self.assertTrue(any((work / 'dist').glob('trading_example_momentum-*.whl')))

        listed = self.cli_json('plugin', 'list', cwd=work)
        ids = {row.get('plugin_id') for row in listed['plugins']
               if row.get('distribution') == 'trading-example-momentum'}
        self.assertEqual(ids, {'example-momentum', 'example-mean-reversion'},
                         listed)

        checked = self.cli_json(
            'plugin', 'check', '--plugin', 'example-momentum',
            '--config', work / 'config.json', cwd=work)
        self.assertTrue(checked['passed'], checked)

        reports = {}
        for name, plugin in (('momentum', 'example-momentum'),
                             ('mean-reversion', 'example-mean-reversion')):
            path = output / (name + '.json')
            self.cli_json(
                'plugin', 'backtest', '--plugin', plugin,
                '--dataset', work / ('events-' + name + '.json'),
                '--config', work / 'config.json',
                '--output', path, cwd=work)
            reports[name] = json.loads(path.read_text())
        self.assertEqual(reports['momentum']['trade_count'], 1)
        self.assertEqual(reports['momentum']['trades'][0]['reason'],
                         'MOMENTUM_LOST_AVERAGE')
        self.assertEqual(reports['momentum']['final_equity'], '10000.65')
        self.assertEqual(reports['mean-reversion']['trade_count'], 1)
        self.assertEqual(reports['mean-reversion']['trades'][0]['reason'],
                         'MEAN_REVERSION_REACHED_AVERAGE')
        self.assertEqual(reports['mean-reversion']['final_equity'], '10000.75')

        expired = self.cli_json(
            'plugin', 'backtest', '--plugin', 'example-momentum',
            '--dataset', work / 'events-expired.json',
            '--config', work / 'config.json',
            '--output', output / 'expired.json', cwd=work)
        self.assertEqual(expired['trade_count'], 0)
        self.assertEqual([row['reason'] for row in expired['unfilled_orders']],
                         ['LIMIT_EXPIRED'])

        shadow = self.cli_json(
            'plugin', 'shadow', '--plugin', 'example-momentum',
            '--dataset', work / 'events-momentum.json',
            '--config', work / 'config.json', cwd=work)
        self.assertTrue(shadow['advisory_only'])
        self.assertEqual(shadow['book_actions'], 0)

        self.assertEqual(source_fingerprint(candidate), before_fingerprint)
        self.assertEqual(source_pin(candidate)['digest'], before_pin['digest'])
        after_files = {path.relative_to(candidate).as_posix()
                       for path in source_files(candidate)}
        self.assertEqual(after_files, before_files)
        self.assertEqual(set(_source_pin_scan(candidate)), after_files)
        self.assertNotIn('examples/plugins/example_momentum/build',
                         '\n'.join(after_files))
