"""Bounded, falsifiable conformance checks for one installed external plugin.

Every plugin operation (descriptor, create, decide, exit rule) runs in a fresh
bounded child process. The checks use the real durable instance host for
duplicate, future/out-of-order and checkpoint-recovery semantics. Python
plugins remain trusted code; the timeout is fault isolation, not a sandbox.
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import base64
from pathlib import Path
import sqlite3
import tempfile

from trading_contracts.codec import decode_decision, encode_decision, encode_input
from trading_contracts.instance import InstanceBudget
from trading_contracts.strategy import StrategyIdentity, StrategyInput
from trading_infra.instance_host import InstanceStrategyHost
from trading_infra.instance_registry import InstanceRegistry
from trading_infra.plugin_probe import (
    PROBE_TIMEOUT_SECONDS, probe,
)
from trading_infra.plugin_protocol import (
    PluginDescriptor, distribution_identity, entry_point_for, validate_config,
)


def _probe(plugin_id, config, operation, *, request=None, view=None):
    return probe(plugin_id, config, operation, request=request, view=view,
                 timeout_seconds=PROBE_TIMEOUT_SECONDS)

def _request(plugin_id, version, identity, config, *, cursor, at, symbol,
             bid, ask, checkpoint=None):
    return StrategyInput(
        identity=StrategyIdentity('conformance', plugin_id, version),
        as_of=at, cursor=cursor, data_snapshot_id='conformance-' + cursor,
        source_sha256='a' * 64,
        config_version=__import__('trading_contracts.instance', fromlist=['config_version']).config_version(
            plugin_id, version, config, identity),
        synthetic=True,
        allocation={'instance_id': 'conformance', 'execution': {
            'instance_id': 'conformance', 'lifecycle': 'not_executed',
            'confirmed_quantity': '0', 'available_quantity': '0',
            'available_quantity_state': 'MODEL',
            'feedback_version': 0, 'observed_at': None, 'fact_fresh': False,
            'observation_at': None, 'observation_fresh': False,
            'owner_state': None, 'owner_action_at': None,
            'owner_action_kind': None, 'owner_trade_id': None,
            'owner_activity_source': 'conformance_model',
            'source': 'conformance_model', 'accounting_state': 'MODEL'}},
        market={'event': {'id': cursor, 'kind': 'quote', 'at': at.isoformat(),
                          'synthetic_input': True, 'source_sha256': 'a' * 64,
                          'data': {'symbol': symbol, 'bid': str(bid),
                                   'ask': str(ask), 'bid_at': at.isoformat(),
                                   'ask_at': at.isoformat(),
                                   'observed_at': at.isoformat()}}},
        checkpoint=checkpoint or {})


def _decision(plugin_id, config, request):
    value = _probe(plugin_id, config, 'decide', request=request)
    if value.get('ok') is not True:
        return None, value.get('error', 'PLUGIN_DECIDE_FAILED')
    try:
        return decode_decision(base64.b64decode(value['decision_b64'])), None
    except Exception:
        return None, 'PLUGIN_CONFORMANCE_DECISION_DECODE_FAILED'


def _exit_check(plugin_id, config, descriptor, identity):
    if descriptor.exit_rule is None:
        return {'name': 'exit_rule', 'passed': True, 'skipped': 'not_declared'}
    now = datetime(2026, 10, 16, 14, 0, tzinfo=timezone.utc).isoformat()
    view = {'instance_id': 'conformance', 'plugin_id': plugin_id,
            'trade_id': 'trade', 'symbol': 'DEMO', 'quantity': '1',
            'entry_price': '40', 'stop': '39', 'initial_stop': '39',
            'quote': {'symbol': 'DEMO', 'bid': '39.50', 'ask': '39.51',
                      'bid_at': now, 'ask_at': now, 'observed_at': now},
            'features': {key: '40' for key in descriptor.feature_keys},
            'as_of': now, 'position_version': 1}
    value = _probe(plugin_id, config, 'exit', view=view)
    if value.get('ok') is not True:
        return {'name': 'exit_rule', 'passed': False,
                'error': value.get('error', 'PLUGIN_EXIT_RULE_FAILED')}
    result = value.get('result')
    if not isinstance(result, dict) or result.get('action') not in {
            'EXIT', 'REDUCE', 'TIGHTEN_PROTECTION', 'HOLD'}:
        return {'name': 'exit_rule', 'passed': False,
                'error': 'PLUGIN_EXIT_RULE_OUTPUT_INVALID'}
    return {'name': 'exit_rule', 'passed': True, 'action': result['action'],
            'reason': result.get('reason')}


def _host_event(identity, event_id, at, symbol='DEMO', bid='40.00', ask='40.01'):
    return {'id': event_id, 'kind': 'quote', 'at': at.isoformat(),
            'synthetic_input': True, 'source_sha256': 'c' * 64,
            'data': {'symbol': symbol, 'bid': str(bid), 'ask': str(ask),
                     'bid_at': at.isoformat(), 'ask_at': at.isoformat(),
                     'observed_at': at.isoformat()}}


def run_conformance(plugin_id, config, *, timeout_seconds=PROBE_TIMEOUT_SECONDS):
    global PROBE_TIMEOUT_SECONDS
    PROBE_TIMEOUT_SECONDS = float(timeout_seconds)
    entry = entry_point_for(plugin_id)
    identity = distribution_identity(entry).digest
    checks = []
    described = _probe(plugin_id, config, 'describe')
    if described.get('ok') is not True:
        return {'schema_version': 1, 'plugin_id': plugin_id, 'passed': False,
                'checks': [{'name': 'descriptor', 'passed': False,
                            'error': described.get('error')}],
                'errors': [described.get('error')], 'production_ready': False}
    descriptor = PluginDescriptor(**described['descriptor'])
    if described.get('distribution_digest') != identity:
        return {'schema_version': 1, 'plugin_id': plugin_id, 'passed': False,
                'checks': [{'name': 'identity', 'passed': False,
                            'error': 'PLUGIN_IDENTITY_MISMATCH'}],
                'errors': ['PLUGIN_IDENTITY_MISMATCH'], 'production_ready': False}
    validate_config(descriptor.config_schema, config)
    checks.append({'name': 'descriptor', 'passed': True,
                   'protocol_version': descriptor.protocol_version,
                   'distribution': described.get('distribution'),
                   'distribution_version': described.get('distribution_version'),
                   'distribution_digest': identity})
    created = _probe(plugin_id, config, 'create')
    checks.append({'name': 'create', 'passed': created.get('ok') is True,
                   **({'error': created.get('error')} if not created.get('ok') else {})})
    at = datetime(2026, 10, 16, 14, 0, tzinfo=timezone.utc)
    request1 = _request(plugin_id, descriptor.version, identity, config,
                        cursor='conformance-1', at=at, symbol='DEMO',
                        bid='40.00', ask='40.01')
    first, error = _decision(plugin_id, config, request1)
    if first is None:
        checks.append({'name': 'decision_contract', 'passed': False,
                       'error': error})
        return {'schema_version': 1, 'plugin_id': plugin_id, 'passed': False,
                'version': descriptor.version, 'distribution': described.get('distribution'),
                'distribution_version': described.get('distribution_version'),
                'distribution_digest': identity, 'config': dict(config),
                'checks': checks, 'errors': [error], 'production_ready': False}
    checks.append({'name': 'decision_contract', 'passed': True,
                   'proposals': len(first.proposals)})
    second, error = _decision(plugin_id, config, request1)
    deterministic = (second is not None
                     and encode_decision(first) == encode_decision(second))
    checks.append({'name': 'deterministic_replay',
                   'passed': deterministic,
                   **({'error': error or 'PLUGIN_DECISION_NOT_DETERMINISTIC'}
                      if not deterministic else {})})
    if not deterministic:
        return {'schema_version': 1, 'plugin_id': plugin_id, 'passed': False,
                'version': descriptor.version, 'distribution': described.get('distribution'),
                'distribution_version': described.get('distribution_version'),
                'distribution_digest': identity, 'config': dict(config),
                'checks': checks, 'errors': ['PLUGIN_DECISION_NOT_DETERMINISTIC'],
                'production_ready': False}
    # Durable host semantics: duplicate event, changed content, future event,
    # checkpoint recovery after reopening the database, and two-instance state.
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / 'conformance.sqlite'
        db = sqlite3.connect(path, isolation_level=None)
        registry = InstanceRegistry(db, clock=lambda: at)
        manifest = descriptor.manifest(config, plugin_identity=identity)
        budget = InstanceBudget(max_cash=Decimal('100'),
                                max_planned_loss=Decimal('2.50'),
                                max_notional=Decimal('100'), max_concurrent=1)
        registry.register('conformance', manifest, budget, mode='shadow', enabled=True)
        host = InstanceStrategyHost(registry, registry.instance('conformance'), clock=lambda: at)
        event1 = _host_event(identity, 'host-1', at)
        prepared = host.prepare(event1)
        decision1, error = _decision(plugin_id, config, prepared.request)
        if decision1 is None:
            checks.append({'name': 'duplicate_event_idempotent', 'passed': False, 'error': error})
        else:
            host.commit(prepared, decision1)
            duplicate = host.prepare(event1)
            changed = dict(event1, data=dict(event1['data'], bid='40.02'))
            conflict = False
            try:
                host.prepare(changed)
            except ValueError as exc:
                conflict = str(exc) == 'STRATEGY_EVENT_ID_CONFLICT'
            checks.append({'name': 'duplicate_event_idempotent',
                           'passed': duplicate is None and conflict,
                           **({'error': 'PLUGIN_DUPLICATE_OR_CHANGED_CONFLICT_NOT_REJECTED'}
                              if duplicate is not None or not conflict else {})})
            future = _host_event(identity, 'host-future', at + timedelta(seconds=31))
            future_rejected = False
            try:
                host.prepare(future)
            except ValueError as exc:
                future_rejected = str(exc) in {'STALE_STRATEGY_EVENT'}
            checks.append({'name': 'future_event_rejected',
                           'passed': future_rejected,
                           **({'error': 'PLUGIN_FUTURE_EVENT_ACCEPTED'} if not future_rejected else {})})
        if decision1 is None:
            checks.append({'name': 'checkpoint_recovery_after_reopen',
                           'passed': False, 'error': 'PLUGIN_DECISION_UNAVAILABLE'})
            checks.append({'name': 'instance_isolation', 'passed': False,
                           'error': 'PLUGIN_DECISION_UNAVAILABLE'})
            db.close()
            errors = [check.get('error') for check in checks if check.get('passed') is not True]
            return {'schema_version': 1, 'plugin_id': plugin_id, 'version': descriptor.version,
                    'distribution': described.get('distribution'),
                    'distribution_version': described.get('distribution_version'),
                    'distribution_digest': identity, 'config': dict(config),
                    'checks': checks, 'passed': False, 'errors': errors,
                    'production_ready': False}
        # A revised bar with the same economic key is refused by the host
        # before another plugin computation, not silently replayed. Plugins
        # that do not declare bars are not forced through an unrelated shape.
        if '5minute-bars' not in descriptor.data_requirements:
            checks.append({'name': 'out_of_order_bar_revision_rejected',
                           'passed': True, 'skipped': 'plugin_declares_quotes_only'})
            bar = None
        else:
            bar = {'id': 'bar-1', 'kind': 'bar', 'at': at.isoformat(),
               'synthetic_input': True, 'source_sha256': 'd' * 64,
               'data': {'symbol': 'DEMO', 'begins_at': at.isoformat(),
                        'open': '40', 'high': '40.5', 'low': '39.9',
                        'close': '40.1', 'volume': 100, 'interval': '5minute',
                        'bounds': 'regular', 'adjustment': 'split'}}
        if bar is not None:
            prepared_bar = host.prepare(bar)
            bar_decision, error = _decision(plugin_id, config, prepared_bar.request)
            if bar_decision is not None:
                host.commit(prepared_bar, bar_decision)
            revised = dict(bar, id='bar-2', data=dict(bar['data'], close='40.2'))
            revision_rejected = False
            try:
                host.prepare(revised)
            except ValueError as exc:
                revision_rejected = str(exc) == 'CONSUMED_BAR_REVISED'
            checks.append({'name': 'out_of_order_bar_revision_rejected',
                           'passed': revision_rejected,
                           **({'error': 'PLUGIN_BAR_REVISION_ACCEPTED'}
                              if not revision_rejected else {})})
        # Close/reopen the durable registry and recover the exact checkpoint.
        db.close()
        db = sqlite3.connect(path, isolation_level=None)
        registry2 = InstanceRegistry(db, clock=lambda: at)
        host2 = InstanceStrategyHost(registry2, registry2.instance('conformance'), clock=lambda: at)
        event2 = _host_event(identity, 'host-2', at)
        prepared2 = host2.prepare(event2)
        recovered = prepared2.request.checkpoint == decision1.next_checkpoint
        decision2, error = _decision(plugin_id, config, prepared2.request)
        if decision2 is not None:
            host2.commit(prepared2, decision2)
        checks.append({'name': 'checkpoint_recovery_after_reopen',
                       'passed': recovered and decision2 is not None,
                       **({'error': error or 'PLUGIN_CHECKPOINT_RECOVERY_MISMATCH'}
                          if not recovered or decision2 is None else {})})
        # A second instance has an independent checkpoint namespace.
        registry2.register('conformance-2', manifest, budget, mode='shadow', enabled=True)
        host_second = InstanceStrategyHost(registry2, registry2.instance('conformance-2'),
                                           clock=lambda: at)
        prepared3 = host_second.prepare(_host_event(identity, 'host-2', at))
        checks.append({'name': 'instance_isolation',
                       'passed': prepared3.request.checkpoint == {}
                       and registry2.state('conformance') != registry2.state('conformance-2')})
        db.close()
    checks.append(_exit_check(plugin_id, config, descriptor, identity))
    errors = [check.get('error') for check in checks if check.get('passed') is not True]
    return {'schema_version': 1, 'plugin_id': plugin_id,
            'version': descriptor.version,
            'distribution': described.get('distribution'),
            'distribution_version': described.get('distribution_version'),
            'distribution_digest': identity, 'config': dict(config),
            'checks': checks, 'passed': not errors, 'errors': errors,
            'production_ready': False}
