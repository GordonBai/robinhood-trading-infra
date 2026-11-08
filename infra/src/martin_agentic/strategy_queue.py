"""Compatibility re-export. The durable advisory queue now lives in ``trading_infra``.

The queue never contained strategy knowledge; it only needed the owner's
connection, a prepared request and a bounded runner.
"""
from trading_contracts.instance import LEGACY_INSTANCE_ID
from trading_infra.strategy_queue import DurableStrategyQueue, ensure_strategy_task_schema

__all__ = ['DurableStrategyQueue', 'LEGACY_INSTANCE_ID', 'ensure_strategy_task_schema']
