"""Martin application composition over the generic instance builder.

The group/registry/budget/lease machinery lives in ``trading_infra``; this
module only supplies the Martin plugin catalog and the compatibility names that
existing callers import. Generic sessions never import this module.
"""
from trading_infra.instance_composition import (  # noqa: F401
    DEFAULT_ACCOUNT_LIMITS,
    build_admission_stack,
    build_instance_group as _build_instance_group,
)
from trading_strategies.registry import contract as _contract
from trading_strategies.registry import worker_command as _worker_command


class MartinCatalog:
    """The concrete plugin family map owned by the Martin application."""

    contract = staticmethod(_contract)
    worker_command = staticmethod(_worker_command)


def build_instance_group(book, policy, *, source_fingerprint, clock=None,
                         runner_factory=None, mode='shadow', enabled=True):
    """Build the Martin-declared instance group with the Martin catalog."""
    return _build_instance_group(
        book, policy, source_fingerprint=source_fingerprint,
        catalog=MartinCatalog(), clock=clock, runner_factory=runner_factory,
        mode=mode, enabled=enabled)
