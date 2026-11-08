"""Simulation source and workspace isolation, fail-closed before any I/O."""
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest import TestCase

from martin_luk.live_session import default_live_ports, run_live_session
from martin_agentic.native_review import EquityReviewTransport
from martin_agentic.simulation import (SimulationSources,
                                      validate_simulation_workspace)
from test_supervised_host_default_loop import (SimulationBroker, SimulationMarket,
                                               SimulationReviewTransport,
                                               SimulationWake)
from test_live_session import full_snapshot

NOW = datetime(2026, 10, 16, 14, 0, tzinfo=timezone.utc)
SYMBOLS = ['DEMO'] + [str(index) for index in range(9)]
_MISSING = object()


def policy(**changes):
    value = json.loads(Path('tests/fixtures/policies/live_trial_2026-10-16.json').read_text())
    value.update(capture_only=False, live_enabled=False)
    value.update(changes)
    return value


class UnmarkedBroker:
    def __call__(self, approval_ids=()):
        return full_snapshot(NOW.isoformat())


class MarkedWrong:
    simulation_only = True


class SimulationIsolationTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.workspace = self.root / 'workspace'
        self.db = self.workspace / 'book.sqlite'
        self.output = self.workspace / 'output'
        self.status = self.workspace / 'status.json'
        self.broker = SimulationBroker()
        self.market = SimulationMarket()
        self.wake = SimulationWake()

    def sources(self, **overrides):
        values = {'broker': self.broker, 'market': self.market,
                  'review': SimulationReviewTransport, 'wake': self.wake}
        values.update(overrides)
        return SimulationSources(**values)

    def run_sim(self, *, sources=_MISSING, workspace=_MISSING, db=None,
                output=None, status=None, entry_inhibit=None, watchdog=None,
                ports=None, agent_mode='supervised-host', policy_value=None):
        return run_live_session(
            self.db if db is None else db,
            policy_value or policy(),
            symbols=SYMBOLS, cycles=1, agent_mode=agent_mode,
            simulation=True,
            simulation_workspace=(self.workspace if workspace is _MISSING
                                  else workspace),
            simulation_sources=(self.sources() if sources is _MISSING
                                else sources),
            output_dir=self.output if output is None else output,
            status_file=self.status if status is None else status,
            entry_inhibit_file=entry_inhibit,
            watchdog_policy_path=watchdog,
            ports=ports, clock=lambda: NOW,
            budget_entry_limit=0, budget_total_limit=0)

    def assert_no_workspace_or_db(self):
        self.assertFalse(self.workspace.exists())
        self.assertFalse(self.db.exists())

    def test_missing_sources_or_workspace_is_refused_before_io(self):
        with self.assertRaisesRegex(ValueError, 'SIMULATION_SOURCES_REQUIRED'):
            self.run_sim(sources=None)
        self.assert_no_workspace_or_db()
        self.broker.calls = 0
        with self.assertRaisesRegex(ValueError, 'SIMULATION_WORKSPACE_REQUIRED'):
            run_live_session(
                self.db, policy(), symbols=SYMBOLS, cycles=1,
                agent_mode='supervised-host', simulation=True,
                simulation_sources=self.sources(), output_dir=self.output,
                status_file=self.status, clock=lambda: NOW,
                budget_entry_limit=0, budget_total_limit=0)
        self.assert_no_workspace_or_db()
        self.assertEqual((self.broker.calls, self.market.calls), (0, 0))

    def test_sources_must_be_marked_and_typed(self):
        cases = [
            (dict(sources={'broker': self.broker}), 'SIMULATION_SOURCES_TYPE_INVALID'),
            (dict(sources=SimulationSources(
                broker=self.broker, market=self.market,
                review=SimulationReviewTransport, wake=self.wake,
                simulation_only=False)), 'SIMULATION_SOURCES_NOT_MARKED'),
            (dict(sources=self.sources(broker=UnmarkedBroker())),
             'SIMULATION_SOURCE_NOT_MARKED:broker'),
            (dict(sources=self.sources(review=EquityReviewTransport)),
             'SIMULATION_SOURCE_NOT_MARKED:review'),
            (dict(sources=self.sources(market=MarkedWrong())),
             'SIMULATION_SOURCE_TYPE_MISMATCH:market'),
            (dict(sources=self.sources(broker=MarkedWrong())),
             'SIMULATION_SOURCE_TYPE_MISMATCH:broker'),
            (dict(sources=self.sources(review=MarkedWrong())),
             'SIMULATION_SOURCE_TYPE_MISMATCH:review'),
            (dict(sources=self.sources(wake=MarkedWrong())),
             'SIMULATION_SOURCE_TYPE_MISMATCH:wake'),
            (dict(sources=self.sources(broker=None)),
             'SIMULATION_SOURCE_MISSING:broker'),
        ]
        for kwargs, reason in cases:
            with self.subTest(reason=reason):
                with self.assertRaisesRegex(ValueError, reason):
                    self.run_sim(**kwargs)
                self.assert_no_workspace_or_db()
        self.assertEqual((self.broker.calls, self.market.calls), (0, 0))

    def test_escape_paths_are_refused_before_workspace_creation(self):
        outside = self.root / 'outside'
        cases = [
            (dict(db=outside / 'book.sqlite'),
             'SIMULATION_PATH_OUTSIDE_WORKSPACE:db_path'),
            (dict(output=outside / 'run'),
             'SIMULATION_PATH_OUTSIDE_WORKSPACE:output_dir'),
            (dict(status=outside / 'status.json'),
             'SIMULATION_PATH_OUTSIDE_WORKSPACE:status_file'),
            (dict(entry_inhibit=outside / 'inhibit.json'),
             'SIMULATION_PATH_OUTSIDE_WORKSPACE:entry_inhibit_file'),
            (dict(watchdog=outside / 'watchdog.json'),
             'SIMULATION_PATH_OUTSIDE_WORKSPACE:watchdog_policy_path'),
            (dict(db=self.workspace / 'live-0000-2026-10-28.sqlite'),
             'SIMULATION_PATH_PROTECTED:db_path'),
        ]
        for kwargs, reason in cases:
            with self.subTest(reason=reason):
                with self.assertRaisesRegex(ValueError, reason):
                    self.run_sim(**kwargs)
                self.assert_no_workspace_or_db()
                self.assertFalse(outside.exists())

    def test_workspace_symlink_is_refused_before_resolution_or_creation(self):
        target = self.root / 'empty-target'
        target.mkdir()
        link = self.root / 'workspace-link'
        link.symlink_to(target, target_is_directory=True)
        self.assertTrue(link.is_symlink())
        with self.assertRaisesRegex(ValueError,
                                    'SIMULATION_WORKSPACE_SYMLINK'):
            validate_simulation_workspace(
                link, db_path=link / 'book.sqlite',
                output_dir=link / 'output', status_file=link / 'status.json')
        self.assertEqual(list(target.iterdir()), [])
        self.assertFalse((target / 'book.sqlite').exists())
        self.assertFalse((target / 'output').exists())
        self.assertFalse((target / 'status.json').exists())

        with self.assertRaisesRegex(ValueError,
                                    'SIMULATION_WORKSPACE_SYMLINK'):
            run_live_session(
                link / 'book.sqlite', policy(), symbols=SYMBOLS, cycles=1,
                agent_mode='supervised-host', simulation=True,
                simulation_workspace=link, simulation_sources=self.sources(),
                output_dir=link / 'output', status_file=link / 'status.json',
                clock=lambda: NOW, budget_entry_limit=0, budget_total_limit=0)
        self.assertEqual(list(target.iterdir()), [])
        self.assertFalse((target / 'book.sqlite').exists())
        self.assertFalse((target / 'output').exists())
        self.assertFalse((target / 'status.json').exists())

    def test_system_style_symlinked_ancestor_is_not_mistaken_for_workspace_link(self):
        real = self.root / 'real'
        real.mkdir()
        alias = self.root / 'alias'
        alias.symlink_to(real, target_is_directory=True)
        workspace = alias / 'new-workspace'
        paths = validate_simulation_workspace(
            workspace, db_path=workspace / 'book.sqlite',
            output_dir=workspace / 'output')
        self.assertEqual(paths.root, (real / 'new-workspace').resolve())
        self.assertFalse(workspace.exists())

    def test_repository_runtime_default_output_and_nonempty_workspace_are_refused(self):
        repo_runtime = Path(__file__).resolve().parents[1] / 'runtime'
        with self.assertRaisesRegex(
                ValueError, 'SIMULATION_WORKSPACE_OVERLAPS_LIVE_RUNTIME'):
            self.run_sim(workspace=repo_runtime / 'synthetic-isolation-test')
        self.assertFalse((repo_runtime / 'synthetic-isolation-test').exists())
        with self.assertRaisesRegex(
                ValueError, 'SIMULATION_PATH_OUTSIDE_WORKSPACE:output_dir'):
            run_live_session(
                self.db, policy(), symbols=SYMBOLS, cycles=1,
                agent_mode='supervised-host', simulation=True,
                simulation_workspace=self.workspace,
                simulation_sources=self.sources(), clock=lambda: NOW,
                budget_entry_limit=0, budget_total_limit=0)
        self.assert_no_workspace_or_db()
        self.workspace.mkdir()
        (self.workspace / 'prior.json').write_text('{}')
        with self.assertRaisesRegex(ValueError,
                                    'SIMULATION_WORKSPACE_NOT_EMPTY'):
            self.run_sim()
        self.assertFalse(self.db.exists())

    def test_custom_ports_live_policy_and_unsupported_agent_mode_are_refused(self):
        with self.assertRaisesRegex(ValueError,
                                    'SIMULATION_CUSTOM_PORTS_FORBIDDEN'):
            self.run_sim(ports=object())
        self.assert_no_workspace_or_db()
        with self.assertRaisesRegex(ValueError, 'SIMULATION_CANNOT_ENABLE_LIVE'):
            self.run_sim(policy_value=policy(live_enabled=True))
        self.assert_no_workspace_or_db()
        for mode in ('review', 'execute'):
            with self.subTest(mode=mode):
                with self.assertRaisesRegex(
                        ValueError, 'SIMULATION_AGENT_MODE_NOT_SUPPORTED'):
                    self.run_sim(agent_mode=mode)
                self.assert_no_workspace_or_db()

    def test_simulation_arguments_require_simulation_flag(self):
        with self.assertRaisesRegex(ValueError,
                                    'SIMULATION_SURFACES_REQUIRE_SIMULATION'):
            run_live_session(
                self.root / 'book.sqlite', policy(), symbols=SYMBOLS, cycles=1,
                simulation=False, simulation_sources=self.sources(),
                output_dir=self.root / 'out', clock=lambda: NOW)
        self.assertFalse((self.root / 'book.sqlite').exists())

    def test_derived_control_sidecar_stays_inside_workspace(self):
        paths = validate_simulation_workspace(
            self.workspace, db_path=self.db, output_dir=self.output)
        self.assertEqual(paths.entry_inhibit_file,
                         self.db.with_name(self.db.name + '.entry-inhibit.json').resolve())
        self.assertTrue(paths.entry_inhibit_file.is_relative_to(paths.root))
        self.assertFalse(self.workspace.exists())

    def test_default_ports_still_build_production_surfaces_without_simulation(self):
        from unittest.mock import MagicMock
        book = MagicMock()
        book.clock = lambda: NOW
        policy_value = policy()
        ports = default_live_ports(book, policy_value, lambda: NOW)
        self.assertTrue(ports.default)
        self.assertIsNotNone(ports.broker)
        self.assertIsNotNone(ports.market)
        self.assertFalse(getattr(ports.broker, 'simulation_only', False))
        self.assertFalse(getattr(ports.market, 'simulation_only', False))
