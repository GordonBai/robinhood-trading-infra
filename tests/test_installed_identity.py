"""Installed candidate identity remains authoritative under sys.path shadows."""
import json
import tempfile
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch
from martin_agentic import qualification

from external_environment import ExternalEnvironment


SCRIPT = r'''
import json
import sys
import unittest
from importlib import metadata
from pathlib import Path

candidate = Path(%(candidate)r)
sys.path.insert(0, str(candidate))
# The build-time egg-info at the extracted root shadows the real dist-info when
# metadata is searched through sys.path. It must not be the identity authority.
shadow = metadata.distribution('martin-agentic')
shadow_path = str(getattr(shadow, '_path', ''))

from martin_agentic.qualification import candidate_root
from trading_infra.plugin_protocol import entry_point_for, load_plugin

before = str(candidate_root())
plugin_entry = entry_point_for('example-momentum')
plugin_dist = str(getattr(plugin_entry.dist, '_path', ''))
loaded_plugin = load_plugin('example-momentum')
loader = unittest.TestLoader()
suite = loader.discover(str(candidate / 'tests'),
                        top_level_dir=str(candidate / 'tests'))
failed = [test.id() for test in loader.errors] if getattr(loader, 'errors', None) else []
def walk(node):
    if isinstance(node, unittest.TestSuite):
        for child in node:
            walk(child)
    elif '_FailedTest' in node.id():
        failed.append(node.id())
walk(suite)
after = str(candidate_root())
print(json.dumps({'shadow_path': shadow_path, 'before': before,
                  'after': after,
                  'candidate': str(candidate),
                  'failed': failed,
                  'loaded': suite.countTestCases(),
                  'plugin_dist': plugin_dist,
                  'plugin_id': loaded_plugin.descriptor.plugin_id}, sort_keys=True))
'''


class DistributionResolverTests(TestCase):
    class Fake:
        metadata = {'Name': 'martin-agentic'}

        def __init__(self, module, files=('martin_agentic/qualification.py',)):
            self.module = module
            self.files = files

        def locate_file(self, name):
            return self.module if name == 'martin_agentic/qualification.py' else self.module.parent / name

    def resolve(self, module, distributions):
        with patch.object(qualification, '__file__', str(module)), \
                patch.object(qualification.importlib.metadata, 'distributions',
                             return_value=distributions):
            return qualification._installed_distribution()

    def test_unique_record_owner_is_selected_and_ambiguity_fails_closed(self):
        with tempfile.TemporaryDirectory() as temp:
            module = Path(temp) / 'site-packages/martin_agentic/qualification.py'
            module.parent.mkdir(parents=True)
            module.write_text('')
            owner = self.Fake(module)
            self.assertIs(self.resolve(module, [owner]), owner)
            with self.assertRaisesRegex(
                    ValueError, 'INSTALLED_DISTRIBUTION_IDENTITY_AMBIGUOUS'):
                self.resolve(module, [owner, self.Fake(module)])
            with self.assertRaisesRegex(
                    ValueError, 'INSTALLED_DISTRIBUTION_IDENTITY_UNAVAILABLE'):
                self.resolve(module, [self.Fake(module, files=('other.py',))])
            with self.assertRaisesRegex(
                    ValueError, 'INSTALLED_DISTRIBUTION_IDENTITY_UNAVAILABLE'):
                self.resolve(module, [])


class CandidateRootRejectionTests(TestCase):
    class Dist:
        _path = '/tmp/fake.dist-info'

        def read_text(self, name):
            return None

    def test_missing_direct_url_still_fails_closed(self):
        with tempfile.TemporaryDirectory() as temp:
            module = Path(temp) / 'site-packages/martin_agentic/qualification.py'
            module.parent.mkdir(parents=True)
            module.write_text('')
            with patch.object(qualification, '__file__', str(module)), \
                    patch.object(qualification, '_installed_distribution',
                                 return_value=self.Dist()):
                with self.assertRaisesRegex(
                        ValueError,
                        'INSTALLED_CANDIDATE_SOURCE_IDENTITY_UNAVAILABLE'):
                    qualification._candidate_root()


class InstalledIdentityShadowTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.environment = ExternalEnvironment.shared()
        cls.result = cls.environment.run_script(
            SCRIPT % {'candidate': str(cls.environment.candidate)})

    def test_candidate_root_uses_record_ownership_not_first_sys_path_metadata(self):
        result = self.result
        self.assertIn('martin_agentic.egg-info', result['shadow_path'])
        self.assertGreater(result['loaded'], 2000)
        from pathlib import Path
        expected = str(Path(result['candidate']).resolve())
        self.assertEqual(result['before'], expected)
        self.assertEqual(result['after'], expected)
        self.assertGreater(result['loaded'], 2000)
        self.assertEqual(result['failed'], [])
        self.assertIn('trading_example_momentum-1.0.0.dist-info',
                      result['plugin_dist'])
        self.assertEqual(result['plugin_id'], 'example-momentum')
