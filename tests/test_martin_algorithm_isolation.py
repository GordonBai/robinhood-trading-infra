"""Frozen pre-migration parity and calendar-free algorithm dependency tests."""
import ast
from copy import deepcopy
from datetime import timedelta
import importlib.util
import json
from pathlib import Path
from unittest import TestCase

from martin_luk.strategy import PullbackMachine as LegacyMachine, replay_setup as legacy_replay
from martin_luk.features import build_candidates as legacy_screen
from trading_infra.market_calendar import ExchangeCalendarView
from trading_strategies.martin.pullback import PullbackMachine, atomic_transition, replay_setup
from trading_strategies.martin.screening import build_candidates
from trading_contracts.values import timestamp
from shadow_fixture import universe


def oracle(name):
    spec = importlib.util.spec_from_file_location(name, Path('tests/fixtures')/(name+'.py'))
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    return module


class MartinAlgorithmIsolationTests(TestCase):
    def setUp(self):
        self.calendar = ExchangeCalendarView()
        self.scenario = json.loads(Path('examples/pullback_setup.json').read_text())
        self.old = oracle('legacy_pullback_v1')

    def machines(self):
        return (self.old.PullbackMachine(deepcopy(self.scenario['setup'])),
                PullbackMachine(deepcopy(self.scenario['setup']), calendar_view=self.calendar))

    def apply(self, machine, event):
        return getattr(machine, 'on_'+event['kind'])(event['data'], as_of=event['at'])

    def test_trace_checkpoint_and_restart_exactly_match_frozen_algorithm(self):
        old, pure = self.machines()
        self.assertEqual(pure.VERSION, old.VERSION)
        for event in self.scenario['events']:
            self.assertEqual(self.apply(pure, event), self.apply(old, event))
            self.assertEqual(pure.checkpoint(), old.checkpoint())
            pure = PullbackMachine.restore(json.loads(json.dumps(pure.checkpoint())), calendar_view=self.calendar)
            old = self.old.PullbackMachine.restore(json.loads(json.dumps(old.checkpoint())))
        self.assertEqual(replay_setup(self.scenario, calendar_view=self.calendar), self.old.replay_setup(self.scenario))
        self.assertEqual(legacy_replay(self.scenario), self.old.replay_setup(self.scenario))
        restored = LegacyMachine.restore(pure.checkpoint())
        self.assertIsInstance(restored, LegacyMachine)
        self.assertEqual(restored.checkpoint(), pure.checkpoint())

    def test_invalid_event_errors_and_atomic_rollback_match(self):
        base = self.scenario['events'][0]
        bad = [
            {**base, 'data': {**base['data'], 'symbol': 'WRONG'}},
            {**base, 'data': {**base['data'], 'volume': 0}},
            {**base, 'data': {**base['data'], 'high': '1'}},
            {**base, 'data': {**base['data'], 'available_at': base['data']['begins_at']}},
            {**base, 'at': '2026-10-15T09:00:00-04:00'},
        ]
        for event in bad:
            old, pure = self.machines(); before = pure.checkpoint()
            errors = []
            for machine in (old, pure):
                with self.assertRaises(Exception) as error:
                    self.apply(machine, event)
                errors.append((type(error.exception), str(error.exception)))
            self.assertEqual(errors[0], errors[1])
            self.assertEqual(pure.checkpoint(), before)
            self.assertEqual(pure.checkpoint(), old.checkpoint())

    def test_duplicate_revision_gap_expiry_and_quote_rejections_match(self):
        old, pure = self.machines()
        for event in self.scenario['events'][:2]:
            self.assertEqual(self.apply(pure, event), self.apply(old, event))
        duplicate = self.scenario['events'][1]
        self.assertEqual(self.apply(pure, duplicate), self.apply(old, duplicate))
        baseline = pure.checkpoint()
        quote = self.scenario['events'][-1]
        variants = [
            {**duplicate, 'data': {**duplicate['data'], 'close': '39.85'}},
            {**quote, 'at': '2026-10-15T09:41:00-04:00'},
            {**quote, 'data': {**quote['data'], 'bid': '40.09', 'ask': '40.10'}},
            {**quote, 'at': '2026-10-15T09:55:02-04:00'},
        ]
        gap = deepcopy(duplicate)
        start = timestamp(gap['data']['begins_at'])+timedelta(minutes=10)
        gap['data'].update(begins_at=start.isoformat(),
            available_at=(start+timedelta(minutes=5, seconds=2)).isoformat(),
            observed_at=(start+timedelta(minutes=5, seconds=2)).isoformat())
        gap['at'] = gap['data']['available_at']; variants.append(gap)
        for event in variants:
            old = self.old.PullbackMachine.restore(deepcopy(baseline))
            pure = PullbackMachine.restore(deepcopy(baseline), calendar_view=self.calendar)
            result = []
            for machine in (old, pure):
                try: result.append(('ok', self.apply(machine, event)))
                except Exception as exc: result.append((type(exc).__name__, str(exc)))
            self.assertEqual(result[0], result[1]); self.assertEqual(old.checkpoint(), pure.checkpoint())

    def test_calendar_service_is_not_copied_or_serialized_with_algorithm_state(self):
        calendar = self.calendar
        class NonCopyable:
            def __init__(self): self.calls = 0
            def __deepcopy__(self, memo): raise AssertionError('Calendar service must not be copied')
            def bar_available_at(self, begins_at, interval):
                self.calls += 1
                return calendar.bar_available_at(begins_at, interval)
        service = NonCopyable()
        machine = PullbackMachine(self.scenario['setup'], calendar_view=service)
        before = machine.checkpoint(); event = deepcopy(self.scenario['events'][0]); event['data']['high'] = '1'
        with self.assertRaises(ValueError): self.apply(machine, event)
        self.assertEqual(machine.checkpoint(), before); self.assertIs(machine._calendar_view, service)
        self.assertEqual(service.calls, 1)
        self.assertNotIn('_calendar_view', machine.checkpoint())
        class OtherObject:
            value = 0
            @atomic_transition
            def fail(self): self.value = 1; raise ValueError('rollback')
        other = OtherObject()
        with self.assertRaises(ValueError): other.fail()
        self.assertEqual(other.value, 0)

    def test_screen_output_errors_history_gaps_and_provenance_match(self):
        old = oracle('legacy_screening_v1')
        baseline = universe(); variants = [deepcopy(baseline)]
        missing = deepcopy(baseline); missing['histories']['S1'].pop(); variants.append(missing)
        gap = deepcopy(baseline); gap['histories']['DEMO'].pop(20); variants.append(gap)
        future = deepcopy(baseline); future['indicators']['DEMO']['9']['observed_at'] = '2026-10-16T00:00:00Z'; variants.append(future)
        wrong = deepcopy(baseline); wrong['indicators']['DEMO']['9']['period'] = 10; variants.append(wrong)
        for data in variants:
            for at in ('2026-10-15T09:25:00-04:00', '2026-10-15T15:55:00-04:00'):
                args = (data['histories'], data['indicators'])
                kwargs = dict(as_of=at, universe_id=data['universe_id'])
                expected = old.build_candidates(*args, **kwargs)
                self.assertEqual(build_candidates(*args, **kwargs, calendar_view=self.calendar), expected)
                self.assertEqual(legacy_screen(*args, **kwargs), expected)
        with self.assertRaisesRegex(ValueError, 'at least ten'):
            build_candidates({}, {}, as_of='2026-10-15T09:25:00-04:00', universe_id='small', calendar_view=self.calendar)

    def test_pure_algorithm_imports_have_no_infra_or_legacy_dependency(self):
        for path in [Path('strategy/martin_luk/src/trading_strategies/martin/pullback.py'), Path('strategy/martin_luk/src/trading_strategies/martin/screening.py')]:
            for node in ast.walk(ast.parse(path.read_text())):
                imports = ([node.module or ''] if isinstance(node, ast.ImportFrom)
                    else [n.name for n in node.names] if isinstance(node, ast.Import) else [])
                for name in imports:
                    self.assertFalse(name.startswith(('martin_agentic', 'trading_infra')), (path, name))
