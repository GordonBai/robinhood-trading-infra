"""Explicit example application composition for a generic live session.

A caller passes this object to ``martin_agentic.live_session.run_live_session``.
It supplies only the example catalog and exit rules; the generic session owns
the control loop, admission, Book, ports and recovery.
"""
from trading_infra.instance_composition import (
    build_admission_stack as _build_admission_stack,
    build_instance_group as _build_instance_group,
)

from . import catalog


class ExampleComposition:
    catalog = catalog

    @staticmethod
    def build_instance_group(book, policy, *, source_fingerprint, clock=None,
                             runner_factory=None, mode='shadow', enabled=True):
        return _build_instance_group(
            book, policy, source_fingerprint=source_fingerprint,
            catalog=catalog, clock=clock, runner_factory=runner_factory,
            mode=mode, enabled=enabled)

    @staticmethod
    def build_admission_stack(book, policy, *, clock=None, limits=None):
        return _build_admission_stack(book, policy, clock=clock, limits=limits)

    @staticmethod
    def feature_keys_for(plugin_id):
        return catalog.EXIT_REGISTRY.feature_keys_for(plugin_id)

    @staticmethod
    def exit_specs_for(plugin_ids):
        return catalog.EXIT_REGISTRY.specs_for(plugin_ids)


COMPOSITION = ExampleComposition()
