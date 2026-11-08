"""Offline advisory shadow replay for an installed plugin.

This uses the real instance host and bounded external worker, but intentionally
does not create an ExecutionBook action. It is an advisory replay for tutorial
and conformance checks, not a substitute for the Book/exit-advisor session.
"""
from datetime import datetime
from decimal import Decimal
import hashlib
import json
import sqlite3
import tempfile
import time

from trading_contracts.instance import InstanceBudget
from trading_infra.instance_registry import InstanceRegistry
from trading_infra.plugin_probe import probe
from trading_infra.plugin_protocol import PluginDescriptor
from trading_infra.instance_runtime import InstanceRuntime
from trading_infra.strategy_stream import WarmStrategyRunner


def run_shadow(plugin_id, config, dataset, *, timeout_seconds=30):
    described = probe(plugin_id, config, 'describe')
    if described.get('ok') is not True:
        raise ValueError('PLUGIN_SHADOW_PLUGIN_FAILED:' + str(described.get('error')))
    descriptor = PluginDescriptor(**described['descriptor'])
    identity = described.get('distribution_digest')
    if not isinstance(dataset, dict) or dataset.get('synthetic') is not True:
        raise ValueError('PLUGIN_SHADOW_REQUIRES_SYNTHETIC_DATA')
    events = dataset.get('events')
    if not isinstance(events, list) or not events:
        raise ValueError('PLUGIN_SHADOW_EVENTS_REQUIRED')
    first = datetime.fromisoformat(str(events[0]['at']).replace('Z', '+00:00'))
    now = [first]
    clock = lambda: now[0]
    config = dict(config)
    config['cost_buffer_per_share'] = '0.05'
    created = probe(plugin_id, config, 'create')
    if created.get('ok') is not True:
        raise ValueError('PLUGIN_SHADOW_CREATE_FAILED:' + str(created.get('error')))
    with tempfile.TemporaryDirectory() as directory:
        db = sqlite3.connect(directory + '/shadow.sqlite', isolation_level=None)
        try:
            registry = InstanceRegistry(db, clock=clock)
            manifest = descriptor.manifest(config, plugin_identity=identity)
            budget = InstanceBudget(max_cash=Decimal('100'),
                                    max_planned_loss=Decimal('2.50'),
                                    max_notional=Decimal('100'), max_concurrent=1)
            registry.register('shadow', manifest, budget, mode='shadow', enabled=True)
            runtime = InstanceRuntime(
                registry, registry.instance('shadow'),
                runner=WarmStrategyRunner([
                    __import__('sys').executable, '-I', '-m',
                    'trading_infra.external_worker', '--plugin', plugin_id,
                    '--plugin-digest', identity, '--plugin-identity', identity,
                    '--config', json.dumps(config, sort_keys=True),
                    '--source-fingerprint', hashlib.sha256(
                        json.dumps(dataset, sort_keys=True).encode()).hexdigest()],
                    timeout_seconds=5),
                source_fingerprint=hashlib.sha256(
                    json.dumps(dataset, sort_keys=True).encode()).hexdigest(),
                clock=clock)
            results = []
            try:
                for event in events:
                    now[0] = datetime.fromisoformat(
                        str(event['at']).replace('Z', '+00:00'))
                    runtime.enqueue(dict(event, synthetic_input=True,
                        source_sha256=event.get('source_sha256', 'd' * 64)))
                    deadline = time.monotonic() + timeout_seconds
                    while time.monotonic() < deadline:
                        report = runtime.advance()
                        if not report.get('pending') and not report.get('busy'):
                            break
                        time.sleep(.01)
                    else:
                        raise ValueError('PLUGIN_SHADOW_TIMEOUT')
                    if report.get('errors'):
                        raise ValueError('PLUGIN_SHADOW_WORKER_FAILED:' + str(
                            report['errors'][0].get('reason')))
                    results.append(runtime.status())
            finally:
                runtime.close()
            rows = []
            for row in db.execute(
                    'SELECT proposal_id,event_id,trade_id,action,symbol,'
                    'valid_until,state,payload,recorded_at '
                    'FROM strategy_instance_proposals ORDER BY recorded_at'):
                item = dict(zip(('proposal_id', 'event_id', 'trade_id', 'action',
                                 'symbol', 'valid_until', 'state', 'payload',
                                 'recorded_at'), row))
                if isinstance(item['payload'], bytes):
                    item['payload'] = __import__('base64').b64encode(
                        item['payload']).decode('ascii')
                rows.append(item)
            state = registry.state('shadow')
            return {'schema_version': 1, 'plugin_id': plugin_id,
                    'version': descriptor.version,
                    'distribution': described.get('distribution'),
                    'distribution_version': described.get('distribution_version'),
                    'distribution_digest': identity,
                    'config': dict(config), 'synthetic': True,
                    'events': len(events), 'checkpoint': state,
                    'proposals': rows, 'proposal_count': len(rows),
                    'worker_results': results, 'book_actions': 0,
                    'advisory_only': True, 'limitations': [
                        'advisory instance replay only',
                        'no ExecutionBook action, broker order or exit fill',
                        'use the runtime Book/exit-advisor session for management'],
                    'production_ready': False}
        finally:
            db.close()
