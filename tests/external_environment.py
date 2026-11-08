"""One temporary, fresh, non-editable installation for external plugin tests.

The fixture builds the candidate release from the repository under test, extracts
the published source inventory, builds the core wheel and the public example
wheel from that extraction, and installs both plus the synthetic probe fixture
into a new venv. It never imports the example distribution into this process.
"""
import atexit
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

from martin_agentic.release_bundle import build_release_bundle, verify_release_bundle


class ExternalEnvironment:
    _instance = None

    def __init__(self):
        self.root = Path(__file__).resolve().parents[1]
        self._temp = tempfile.TemporaryDirectory(prefix='martin-external-test-')
        base = Path(self._temp.name)
        uv = shutil.which('uv')
        if uv is None:
            raise RuntimeError('UV_REQUIRED_FOR_EXTERNAL_INTEGRATION_TEST')
        self.uv = uv
        self.base = base
        self._scripts = base / 'scripts'
        self._scripts.mkdir()
        self.example_source = None
        bundle = base / 'release'
        candidate = base / 'candidate'
        built = build_release_bundle(self.root, bundle)
        verify_release_bundle(
            bundle, expected_source=built['source_fingerprint'],
            expected_input_set_sha256=built['input_set_sha256'],
            extract_to=candidate)
        (candidate / 'dist').mkdir()
        uv = self.uv
        self._run_uv(uv, ['build', '--wheel', '--offline', '--out-dir',
                          str(candidate / 'dist'), str(candidate)])
        example_source = base / 'example-plugin-src'
        shutil.copytree(candidate / 'examples/plugins/example_momentum',
                        example_source)
        self.example_source = example_source
        self._run_uv(uv, ['build', '--wheel', '--offline', '--out-dir',
                          str(candidate / 'dist'), str(example_source)])
        probe_source = base / 'probe-plugin-src'
        shutil.copytree(candidate / 'tests/fixtures/probe_plugins', probe_source)
        self._run_uv(uv, ['build', '--wheel', '--offline', '--out-dir',
                          str(candidate / 'dist'), str(probe_source)])
        self.venv = base / 'venv'
        self._run_uv(uv, ['venv', '--python', sys.executable, str(self.venv)])
        self.python = self.venv / 'bin' / 'python'
        self._run_uv(uv, ['pip', 'install', '--offline', '--python',
                          str(self.python), *map(str, (candidate / 'dist').glob('*.whl'))])
        self.candidate = candidate
        self.env = {
            'PATH': os.environ.get('PATH', '/usr/bin:/bin:/usr/sbin:/sbin'),
            'HOME': str(base / 'home'), 'TMPDIR': str(base / 'tmp'),
            'PYTHONDONTWRITEBYTECODE': '1', 'LANG': 'C', 'LC_ALL': 'C',
        }
        (base / 'home').mkdir()
        (base / 'tmp').mkdir()
        atexit.register(self._temp.cleanup)

    @staticmethod
    def _run_uv(uv, args):
        result = subprocess.run([uv, *args], capture_output=True, text=True,
                                timeout=180)
        if result.returncode != 0:
            raise RuntimeError('EXTERNAL_ENV_BUILD_FAILED: '
                               + (result.stderr or result.stdout)[-2000:])

    @classmethod
    def shared(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def build_public_example(self, source, out_dir):
        Path(out_dir).mkdir(parents=True, exist_ok=True)
        self._run_uv(self.uv, ['build', '--wheel', '--offline', '--out-dir',
                               str(out_dir), str(source)])

    def run_cli(self, *args, cwd=None, timeout=120):
        result = subprocess.run(
            [str(self.python), '-I', '-m', 'martin_agentic', *map(str, args)],
            cwd=str(cwd or self.candidate), env=self.env,
            capture_output=True, text=True, timeout=timeout)
        return result

    def run_script(self, source, *, timeout=120):
        path = self._scripts / ('script-%d.py' % len(list(self._scripts.glob('*.py'))))
        path.write_text(source)
        result = subprocess.run(
            [str(self.python), '-I', str(path)], cwd=self.root, env=self.env,
            capture_output=True, text=True, timeout=timeout)
        if result.returncode != 0:
            raise AssertionError(
                'external test script failed rc=%s\nstdout:\n%s\nstderr:\n%s'
                % (result.returncode, result.stdout[-4000:], result.stderr[-4000:]))
        lines = [line for line in result.stdout.splitlines() if line.strip()]
        try:
            return json.loads(lines[-1])
        except (IndexError, ValueError):
            raise AssertionError('external test script emitted no JSON: '
                                 + result.stdout[-4000:]) from None


def load_report(path):
    return json.loads(Path(path).read_text())
