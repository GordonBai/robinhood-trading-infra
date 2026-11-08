"""Structural dependency direction for the in-place infra/strategy split.

Generic infra may use contracts and stdlib, never a concrete strategy package.
Martin strategy/application may import generic infra. CLI strategy commands are
allowed to lazy-import Martin only when that command is selected; module import
of generic infra must not require Martin.
"""
import ast
from pathlib import Path
from unittest import TestCase

ROOT = Path(__file__).resolve().parents[1]
INFRA = ROOT / 'infra/src'
STRATEGY = ROOT / 'strategy/martin_luk/src'
EXAMPLES = ROOT / 'strategy/examples/src'


def modules(root, package):
    return sorted(path for path in (root / package).rglob('*.py'))


def imports(path, *, module_level_only=False):
    tree = ast.parse(path.read_text(), filename=str(path))
    nodes = tree.body if module_level_only else list(ast.walk(tree))
    found = []
    for node in nodes:
        if isinstance(node, ast.Import):
            found.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                continue
            if node.module:
                found.append(node.module)
                found.extend(node.module + '.' + a.name for a in node.names)
    return found


class DependencyDirectionTests(TestCase):
    def test_packages_exist_in_expected_roots(self):
        for package in ('trading_contracts', 'trading_infra', 'martin_agentic'):
            self.assertTrue((INFRA / package / '__init__.py').is_file(), package)
        for package in ('martin_luk', 'trading_strategies'):
            self.assertTrue((STRATEGY / package / '__init__.py').is_file(), package)
        for package in ('trading_example_plugins', 'trading_example_app'):
            self.assertTrue((EXAMPLES / package / '__init__.py').is_file(), package)

    def test_infra_has_no_top_level_strategy_import(self):
        failures = []
        forbidden = ('trading_strategies', 'martin_luk')
        for package in ('trading_contracts', 'trading_infra', 'martin_agentic'):
            for path in modules(INFRA, package):
                for name in imports(path, module_level_only=True):
                    if name.split('.')[0] in forbidden:
                        failures.append(f'{path.relative_to(INFRA)} imports {name}')
        self.assertEqual(failures, [], '; '.join(failures))

    def test_contracts_do_not_import_infra_or_strategy(self):
        failures = []
        forbidden = ('trading_infra', 'trading_strategies', 'martin_agentic', 'martin_luk')
        for path in modules(INFRA, 'trading_contracts'):
            for name in imports(path):
                if name.split('.')[0] in forbidden:
                    failures.append(f'{path.relative_to(INFRA)} imports {name}')
        self.assertEqual(failures, [], '; '.join(failures))

    def test_trading_infra_does_not_import_concrete_strategy(self):
        failures = []
        forbidden = ('trading_strategies', 'martin_agentic', 'martin_luk')
        for path in modules(INFRA, 'trading_infra'):
            for name in imports(path):
                if name.split('.')[0] in forbidden:
                    failures.append(f'{path.relative_to(INFRA)} imports {name}')
        self.assertEqual(failures, [], '; '.join(failures))

    def test_infra_imports_with_strategy_packages_blocked(self):
        import builtins
        blocked = ('martin_luk', 'trading_strategies')
        original = builtins.__import__
        def guarded(name, *args, **kwargs):
            if name.split('.')[0] in blocked:
                raise ImportError('strategy package blocked: ' + name)
            return original(name, *args, **kwargs)
        builtins.__import__ = guarded
        try:
            for name in ('trading_contracts', 'trading_infra',
                         'martin_agentic.execution', 'martin_agentic.cli'):
                __import__(name)
        finally:
            builtins.__import__ = original

    def test_nested_infra_imports_only_allow_explicit_cli_selection(self):
        failures = []
        for package in ('trading_contracts', 'trading_infra', 'martin_agentic'):
            for path in modules(INFRA, package):
                relative = path.relative_to(INFRA).as_posix()
                for name in imports(path):
                    if name.split('.')[0] in ('trading_strategies', 'martin_luk'):
                        if relative != 'martin_agentic/cli.py':
                            failures.append(f'{relative} imports {name}')
        self.assertEqual(failures, [], '; '.join(failures))

    def test_example_application_does_not_import_martin(self):
        failures = []
        for path in modules(EXAMPLES, 'trading_example_plugins') + modules(EXAMPLES, 'trading_example_app'):
            for name in imports(path):
                if name.split('.')[0] in ('trading_strategies', 'martin_luk'):
                    failures.append(f'{path.relative_to(EXAMPLES)} imports {name}')
        self.assertEqual(failures, [], '; '.join(failures))

    def test_strategy_package_may_import_infra(self):
        # Positive structural assertion: at least one moved strategy module uses
        # the public generic package rather than a copied implementation.
        text = (STRATEGY / 'martin_luk/engine.py').read_text()
        self.assertIn('from martin_agentic.ledger import timestamp', text)
