"""Runtime evidence that generic infrastructure does not need Martin.

These checks go beyond importing the CLI: they call the generic approval
helper and build the example plugin group with Martin imports blocked.
"""
import ast
import builtins
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
INFRA = ROOT / 'infra/src'
STRATEGY_ROOTS = (ROOT / 'strategy/martin_luk/src', ROOT / 'strategy/examples/src')


def _blocked_imports():
    original = builtins.__import__

    def guarded(name, *args, **kwargs):
        if name.split('.')[0] in {'martin_luk', 'trading_strategies'}:
            raise ImportError('strategy package blocked: ' + name)
        return original(name, *args, **kwargs)

    return original, guarded


class RuntimeDecouplingTests(unittest.TestCase):
    def test_generic_approval_helper_runs_with_strategy_imports_blocked(self):
        policy = json.loads((ROOT / 'tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        policy['capture_only'] = True
        with tempfile.TemporaryDirectory() as directory:
            script = (
                'import builtins, json, sys\n'
                'from pathlib import Path\n'
                'real=builtins.__import__\n'
                'def guard(name,*a,**k):\n'
                '    if name.split(".")[0] in {"martin_luk","trading_strategies"}:\n'
                '        raise ImportError("strategy package blocked: "+name)\n'
                '    return real(name,*a,**k)\n'
                'builtins.__import__=guard\n'
                'from martin_agentic.execution import ExecutionBook\n'
                'from martin_agentic.live_session import broker_approval_ids\n'
                'book=ExecutionBook(Path(sys.argv[1]),json.loads(sys.argv[2]),simulation=True)\n'
                'try:\n'
                '    print(json.dumps({"ids":broker_approval_ids(book),'
                '"martin_loaded":any(m=="martin_luk" or m.startswith("martin_luk.")'
                ' for m in sys.modules)}))\n'
                'finally:\n'
                '    book.close()\n')
            env = dict(os.environ)
            env['PYTHONPATH'] = os.pathsep.join(
                [str(INFRA), str(ROOT / 'strategy/martin_luk/src'),
                 str(ROOT / 'strategy/examples/src'), str(ROOT / 'tests')])
            value = subprocess.run(
                [sys.executable, '-c', script, str(Path(directory) / 'book.sqlite'),
                 json.dumps(policy)], capture_output=True, text=True, env=env)
            self.assertEqual(value.returncode, 0, value.stderr)
            result = json.loads(value.stdout)
            self.assertEqual(result['ids'], [])
            self.assertFalse(result['martin_loaded'])

    def test_example_plugin_group_builds_with_martin_blocked(self):
        original, guarded = _blocked_imports()
        builtins.__import__ = guarded
        try:
            from martin_agentic.execution import ExecutionBook
            from trading_example_app.composition import COMPOSITION
            from trading_example_app.catalog import contract
        finally:
            builtins.__import__ = original
        policy = json.loads((ROOT / 'tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
        policy['strategy_instances'] = [
            {'instance_id': 'example', 'plugin': 'vwap-fixture'}]
        with tempfile.TemporaryDirectory() as directory:
            book = ExecutionBook(Path(directory) / 'book.sqlite', policy, simulation=True)
            try:
                group = COMPOSITION.build_instance_group(
                    book, policy, source_fingerprint='a' * 64,
                    runner_factory=lambda cost, pin, plugin_id=None: _Runner())
                try:
                    self.assertEqual(group.instance_ids(), ['example'])
                    runtime = group.runtime('example')
                    self.assertEqual(runtime.instance.manifest.plugin_id,
                                     'vwap-fixture')
                    self.assertEqual(runtime.instance.manifest.data_requirements,
                                     ('quotes',))
                    self.assertEqual(contract('vwap-fixture')['worker_module'],
                                     'trading_example_app.stream_worker')
                finally:
                    group.close()
            finally:
                book.close()

    def test_generic_runtime_modules_have_no_concrete_strategy_import(self):
        offenders = []
        for path in sorted(INFRA.rglob('*.py')):
            tree = ast.parse(path.read_text(), filename=str(path))
            for node in ast.walk(tree):
                names = []
                if isinstance(node, ast.Import):
                    names = [alias.name for alias in node.names]
                elif isinstance(node, ast.ImportFrom) and node.module:
                    names = [node.module]
                for name in names:
                    if name.split('.')[0] in {'martin_luk', 'trading_strategies'}:
                        relative = path.relative_to(INFRA).as_posix()
                        offenders.append(f'{relative}:{node.lineno}:{name}')
        # Only the CLI may lazily select an application composition by name.
        self.assertTrue(
            all(item.startswith('martin_agentic/cli.py:') for item in offenders),
            '; '.join(offenders))


class _Runner:
    @property
    def next_deadline_monotonic(self):
        return None

    def start(self, request, *, payload=None):
        pass

    def poll(self):
        return None

    def close(self):
        pass


if __name__ == '__main__':
    unittest.main()
