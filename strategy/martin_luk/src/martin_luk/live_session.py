"""Martin compatibility entry over the generic live supervisor.

All control flow lives in ``martin_agentic.live_session``. This module only
supplies Martin's signal source, feature mapping and plugin/exit catalog. A
generic caller never imports it; an explicit Martin command may.
"""
import sys
import time

from martin_agentic.live_session import *  # noqa: F401,F403
from martin_agentic.live_session import (  # noqa: F401
    BackgroundTask,
    CodexBrokerReadTransport,
    CodexMarketTransport,
    CodexOrderWorker,
    ExecutionBook,
    RobinhoodReader,
    collect_broker,
    collect_daily,
    dispatch_one,
    inspect_recording,
    notify_local,
    persist_raw_record,
    single_instance,
    source_pin,
    universe_event,
    ACCOUNT_FACTS_REASON_LIMIT,
    ACCOUNT_FACTS_REASON_MAX_LENGTH,
    ACCOUNT_FACTS_REUSE_TTL_SECONDS,
    ACCOUNT_FACTS_WORKER_TIMEOUT_SECONDS,
    READ_SHUTDOWN_GRACE_SECONDS,
    READ_SHUTDOWN_TOTAL_SECONDS,
    SOURCE_MODES,
    AsyncDispatcher,
    HostDeliveryDispatcher,
    HostPortDispatcher,
    LocalWorkerDispatcher,
    LiveSupervisor as _GenericLiveSupervisor,
    _PortAdvanceDispatcher,
    _broker_read,
    _cancel_bounded,
    _cancel_cleanup,
    _cleanup_complete,
    _closing_check,
    _current_observation_fresh,
    _drain_read_task,
    _wait_with_facts,
    broker_approval_ids,
    build_strategy_services as _build_strategy_services,
    capture_publication_ok,
    collect_live_events,
    default_live_ports,
    filter_unchanged_bars,
    full_account_flat_authority,
    merge_step_result,
    publish_owner_observation,
    record_capture_disposition,
    run_live_session as _run_live_session,
    wait_for_result_or_tick,
)
from trading_infra.instance_composition import (
    build_admission_stack as _build_admission_stack,
    build_instance_group as _build_instance_group,
)
from trading_infra.strategy_queue import DurableStrategyQueue
from trading_infra.strategy_stream import WarmStrategyRunner

from . import exit_composition
from .live_signals import LiveSignals


def _exit_features(state):
    """Martin's own per-symbol feature mapping; the core stays indicator-free."""
    ema9 = state.get('ema9') or {}
    return {symbol: {'ema9': value} for symbol, value in ema9.items()}


def _features_from_state(state):
    return _exit_features(state)


def _signals(book, *, persist_contract_metadata=False):
    return LiveSignals(book, persist_contract_metadata=persist_contract_metadata)


class MartinComposition:
    """Application-owned catalog and signal source for Martin sessions."""

    signals = staticmethod(_signals)
    signals_factory = staticmethod(_signals)
    exit_features = staticmethod(_exit_features)
    feature_keys_for = staticmethod(exit_composition.feature_keys_for)
    exit_specs_for = staticmethod(exit_composition.specs_for)

    @staticmethod
    def build_instance_group(book, policy, *, source_fingerprint, clock=None,
                             runner_factory=None, mode='shadow', enabled=True):
        from .instance_composition import MartinCatalog
        return _build_instance_group(
            book, policy, source_fingerprint=source_fingerprint,
            catalog=MartinCatalog(), clock=clock, runner_factory=runner_factory,
            mode=mode, enabled=enabled)

    @staticmethod
    def build_admission_stack(book, policy, *, clock=None, limits=None):
        return _build_admission_stack(book, policy, clock=clock, limits=limits)

    @staticmethod
    def build_strategy_queue(book, *, owner_source, source_fingerprint):
        signals = LiveSignals(book, persist_contract_metadata=True)
        runner = WarmStrategyRunner(
            [sys.executable, '-I', '-m', 'martin_luk.strategy_stream_worker',
             '--cost-buffer-per-share', str(signals.cost),
             '--source-fingerprint', str(source_fingerprint)], timeout_seconds=5)
        return DurableStrategyQueue(
            signals, runner=runner, source_fingerprint=source_fingerprint)


MARTIN_COMPOSITION = MartinComposition()


class LiveSupervisor(_GenericLiveSupervisor):
    """Martin-default supervisor; explicit injections still win."""

    def __init__(self, *args, **kwargs):
        kwargs.setdefault('signals_factory', _signals)
        kwargs.setdefault('exit_features', _exit_features)
        kwargs.setdefault('feature_keys_for', exit_composition.feature_keys_for)
        kwargs.setdefault('strategy_composition', MARTIN_COMPOSITION)
        super().__init__(*args, **kwargs)


def build_strategy_services(book, policy, *, source_fingerprint, clock,
                            instance_supervisor=None, admission=None,
                            composition=None):
    return _build_strategy_services(
        book, policy, source_fingerprint=source_fingerprint, clock=clock,
        instance_supervisor=instance_supervisor, admission=admission,
        composition=composition or MARTIN_COMPOSITION)


def run_live_session(*args, **kwargs):
    kwargs.setdefault('composition', MARTIN_COMPOSITION)
    kwargs.setdefault('supervisor_class', LiveSupervisor)
    return _run_live_session(*args, **kwargs)
