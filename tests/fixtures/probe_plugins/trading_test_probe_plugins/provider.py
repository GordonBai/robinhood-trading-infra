"""Synthetic providers for bounded-probe and backtest negative controls."""
from datetime import timedelta
from decimal import Decimal
import os
import random
import subprocess
import sys
import time

from trading_contracts.strategy import (
    ProposalAction, StrategyDecision, StrategyProposal,
)
from trading_infra.plugin_protocol import PluginDescriptor

_SCHEMA = {
    'type': 'object', 'additionalProperties': False, 'required': [],
    'properties': {
        'sleep_seconds': {'type': 'integer', 'minimum': 0, 'maximum': 30},
        'create_sleep': {'type': 'integer', 'minimum': 0, 'maximum': 30},
        'decide_sleep': {'type': 'integer', 'minimum': 0, 'maximum': 30},
        'pid_file': {'type': 'string', 'minLength': 1, 'maxLength': 512},
        'spawn_child': {'type': 'boolean'},
        'noise_bytes': {'type': 'integer', 'minimum': 0, 'maximum': 4000000},
    },
}
_ACTION_SCHEMA = {
    'type': 'object', 'additionalProperties': False, 'required': ['mode'],
    'properties': {'mode': {'type': 'string', 'enum': ['reduce', 'tighten']}},
}
_STOP_SCHEMA = {
    'type': 'object', 'additionalProperties': False, 'required': [],
    'properties': {'lookback': {'type': 'integer', 'minimum': 1, 'maximum': 20}},
}


def _decision(request, proposals=(), checkpoint=None, diagnostics=None):
    return StrategyDecision(
        identity=request.identity,
        decision_id='probe-decision-' + request.cursor,
        as_of=request.as_of,
        data_snapshot_id=request.data_snapshot_id,
        consumed_cursor=request.cursor,
        synthetic=request.synthetic,
        proposals=tuple(proposals),
        next_checkpoint=checkpoint if checkpoint is not None else dict(request.checkpoint),
        diagnostics=diagnostics or {})


class _StopOnly:
    VERSION = '1.0.0'

    def decide(self, request):
        checkpoint = dict(request.checkpoint)
        if checkpoint.get('in_position'):
            return _decision(request, (), checkpoint)
        data = request.market['event']['data']
        bid, ask = Decimal(str(data['bid'])), Decimal(str(data['ask']))
        proposal = StrategyProposal(
            identity=request.identity, proposal_id='probe-entry-' + request.cursor,
            decision_id='probe-decision-' + request.cursor,
            setup_id='probe-setup-' + request.cursor,
            data_snapshot_id=request.data_snapshot_id,
            source_sha256=request.source_sha256,
            generated_at=request.as_of,
            valid_until=request.as_of + timedelta(seconds=20),
            action=ProposalAction.ENTER, symbol=data['symbol'],
            reason='PROBE_ENTRY', synthetic=request.synthetic,
            quantity_limit=1, entry_ceiling=(ask * Decimal('1.01')).quantize(Decimal('0.01')),
            stop_price=(bid * Decimal('0.98')).quantize(Decimal('0.01')))
        return _decision(request, (proposal,),
                         {**checkpoint, 'in_position': True})


class _Actions:
    VERSION = '1.0.0'

    def __init__(self, config):
        self.mode = config['mode']

    def decide(self, request):
        checkpoint = dict(request.checkpoint)
        data = request.market['event']['data']
        if not checkpoint.get('in_position'):
            bid, ask = Decimal(str(data['bid'])), Decimal(str(data['ask']))
            proposal = StrategyProposal(
                identity=request.identity,
                proposal_id='action-entry-' + request.cursor,
                decision_id='probe-decision-' + request.cursor,
                setup_id='action-setup-' + request.cursor,
                data_snapshot_id=request.data_snapshot_id,
                source_sha256=request.source_sha256,
                generated_at=request.as_of,
                valid_until=request.as_of + timedelta(seconds=20),
                action=ProposalAction.ENTER, symbol=data['symbol'],
                reason='ACTION_ENTRY', synthetic=request.synthetic,
                quantity_limit=1,
                entry_ceiling=(ask * Decimal('1.01')).quantize(Decimal('0.01')),
                stop_price=(bid * Decimal('0.98')).quantize(Decimal('0.01')))
            return _decision(request, (proposal,),
                             {**checkpoint, 'in_position': True})
        if checkpoint.get('acted'):
            return _decision(request, (), checkpoint)
        if self.mode == 'reduce':
            proposal = StrategyProposal(
                identity=request.identity,
                proposal_id='action-reduce-' + request.cursor,
                decision_id='probe-decision-' + request.cursor,
                setup_id='action-setup-' + request.cursor,
                data_snapshot_id=request.data_snapshot_id,
                source_sha256=request.source_sha256,
                generated_at=request.as_of,
                valid_until=request.as_of + timedelta(seconds=20),
                action=ProposalAction.REDUCE, symbol=data['symbol'],
                reason='ACTION_REDUCE', synthetic=request.synthetic,
                quantity_limit=1)
        else:
            proposal = StrategyProposal(
                identity=request.identity,
                proposal_id='action-tighten-' + request.cursor,
                decision_id='probe-decision-' + request.cursor,
                setup_id='action-setup-' + request.cursor,
                data_snapshot_id=request.data_snapshot_id,
                source_sha256=request.source_sha256,
                generated_at=request.as_of,
                valid_until=request.as_of + timedelta(seconds=20),
                action=ProposalAction.TIGHTEN_PROTECTION, symbol=data['symbol'],
                reason='ACTION_TIGHTEN', synthetic=request.synthetic,
                stop_price=Decimal('49.50'))
        return _decision(request, (proposal,), {**checkpoint, 'acted': True})


class _Nondeterministic:
    VERSION = '1.0.0'

    def decide(self, request):
        return _decision(request, (), {'nonce': str(random.random())},
                         {'nonce': str(random.random())})


class _Slow:
    VERSION = '1.0.0'

    def __init__(self, config):
        self.config = dict(config)

    def decide(self, request):
        _record_owner(self.config.get('pid_file'))
        time.sleep(int(self.config.get('decide_sleep',
                                       self.config.get('sleep_seconds', 0))))
        return _decision(request)


class _Noisy:
    VERSION = '1.0.0'

    def decide(self, request):
        return _decision(request)


class _Leaky:
    VERSION = '1.0.0'

    def __init__(self, config):
        self.config = dict(config)

    def decide(self, request):
        return _decision(request)


def _record_owner(path):
    if path:
        with open(path, 'w', encoding='ascii') as handle:
            handle.write('%d %d' % (os.getpid(), os.getpgrp()))


def _create_slow(config):
    _record_owner(config.get('pid_file'))
    time.sleep(int(config.get('create_sleep',
                              config.get('sleep_seconds', 0))))
    return _Slow(config)


def _create_leaky(config):
    path = config.get('pid_file')
    if config.get('spawn_child') is True:
        child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'],
                                  stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                  stderr=subprocess.DEVNULL)
        if path:
            with open(path, 'w', encoding='ascii') as handle:
                handle.write('%d %d' % (child.pid, os.getpgrp()))
    return _Leaky(config)


def _describe_noisy():
    sys.stdout.write('x' * int(1200000))
    sys.stdout.flush()
    return PluginDescriptor(
        plugin_id='test-probe-noisy', version='1.0.0',
        state_schema='test-probe-noisy-1', config_schema=_SCHEMA,
        data_requirements=('quotes',))


class StopOnlyProvider:
    @staticmethod
    def describe():
        return PluginDescriptor(
            plugin_id='test-stop-only', version='1.0.0',
            state_schema='test-stop-only-1', config_schema=_STOP_SCHEMA,
            data_requirements=('quotes',), capabilities=('advisory-entry',))

    @staticmethod
    def create(config):
        return _StopOnly()


class ActionsProvider:
    @staticmethod
    def describe():
        return PluginDescriptor(
            plugin_id='test-actions', version='1.0.0',
            state_schema='test-actions-1', config_schema=_ACTION_SCHEMA,
            data_requirements=('quotes',), capabilities=('advisory-entry',))

    @staticmethod
    def create(config):
        return _Actions(config)


class NondeterministicProvider:
    @staticmethod
    def describe():
        return PluginDescriptor(
            plugin_id='test-nondeterministic', version='1.0.0',
            state_schema='test-nondeterministic-1', config_schema=_SCHEMA,
            data_requirements=('quotes',))

    @staticmethod
    def create(config):
        return _Nondeterministic()


class SlowProvider:
    @staticmethod
    def describe():
        return PluginDescriptor(
            plugin_id='test-probe-slow', version='1.0.0',
            state_schema='test-probe-slow-1', config_schema=_SCHEMA,
            data_requirements=('quotes',), feature_keys=('slow_feature',),
            exit_rule='trading_test_probe_plugins.rules:slow_exit')

    @staticmethod
    def create(config):
        return _create_slow(config)


class AlwaysExitProvider:
    @staticmethod
    def describe():
        return PluginDescriptor(
            plugin_id='test-always-exit', version='1.0.0',
            state_schema='test-always-exit-1', config_schema=_STOP_SCHEMA,
            data_requirements=('quotes',), feature_keys=('anchor',),
            exit_rule='trading_test_probe_plugins.rules:always_exit')

    @staticmethod
    def create(config):
        return _StopOnly()


class BadActionProvider:
    @staticmethod
    def describe():
        return PluginDescriptor(
            plugin_id='test-bad-action', version='1.0.0',
            state_schema='test-bad-action-1', config_schema=_STOP_SCHEMA,
            data_requirements=('quotes',), feature_keys=('anchor',),
            exit_rule='trading_test_probe_plugins.rules:bad_action')

    @staticmethod
    def create(config):
        return _StopOnly()


class BarsProvider:
    @staticmethod
    def describe():
        return PluginDescriptor(
            plugin_id='test-bars', version='1.0.0',
            state_schema='test-bars-1', config_schema=_STOP_SCHEMA,
            data_requirements=('5minute-bars',))

    @staticmethod
    def create(config):
        return _StopOnly()


class WarmupProvider:
    @staticmethod
    def describe():
        return PluginDescriptor(
            plugin_id='test-warmup', version='1.0.0',
            state_schema='test-warmup-1', config_schema=_STOP_SCHEMA,
            data_requirements=('quotes',), warmup_bars=2)

    @staticmethod
    def create(config):
        return _StopOnly()


class BadJsonProvider:
    @staticmethod
    def describe():
        return PluginDescriptor(
            plugin_id='test-probe-badjson', version='1.0.0',
            state_schema='test-probe-badjson-1', config_schema=_SCHEMA,
            data_requirements=('quotes',))

    @staticmethod
    def create(config):
        sys.stdout.write('not-json\n')
        sys.stdout.flush()
        return _Noisy()


class NoisyProvider:
    @staticmethod
    def describe():
        return _describe_noisy()

    @staticmethod
    def create(config):
        return _Noisy()


class LeakyProvider:
    @staticmethod
    def describe():
        return PluginDescriptor(
            plugin_id='test-probe-leaky', version='1.0.0',
            state_schema='test-probe-leaky-1', config_schema=_SCHEMA,
            data_requirements=('quotes',))

    @staticmethod
    def create(config):
        return _create_leaky(config)
