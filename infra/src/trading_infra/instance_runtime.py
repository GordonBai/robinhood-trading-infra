"""Run several strategy instances on one account without letting them mix.

One runtime owns one instance: its host (prepare/commit over that instance's
checkpoint) and its durable advisory queue, which dispatches to one bounded
worker process. Instances share the owner connection, the account budget
authority and the symbol-lease table; they never share state, tasks or money.

This is the composition point. Nothing here decides an entry, sizes an order or
touches a broker.
"""
from trading_infra.instance_host import InstanceStrategyHost
from trading_infra.strategy_queue import DurableStrategyQueue
from trading_contracts.values import digest


class _OwnerView:
    """The minimal ``book``-shaped view the durable queue needs from its owner."""

    def __init__(self, db, clock):
        self.db = db
        self.clock = clock
        self.policy = {}

    def _begin(self):
        self.db.execute('BEGIN IMMEDIATE')


class _InstanceSignals:
    """Adapter presenting one instance host as a queue collaborator."""

    def __init__(self, host, *, db, clock):
        self.host = host
        self.book = _OwnerView(db, clock)
        self.identity = host.identity
        self.cost = 0

    def config_fingerprint(self):
        return self.host.config_fingerprint()

    def consumed_hash(self, event_id):
        return self.host.consumed_hash(event_id)

    def prepare(self, event):
        return self.host.prepare(event)

    def commit(self, prepared, decision):
        return self.host.commit(prepared, decision)

    def restore(self, event, row):
        return self.host.restore(event, row)


class InstanceRuntime:
    """One registered strategy instance: state host plus durable advisory queue."""

    def __init__(self, registry, instance, *, runner, source_fingerprint,
                 clock=None, max_pending=2048):
        self.instance = instance
        self.source_fingerprint = source_fingerprint
        self.host = InstanceStrategyHost(registry, instance, clock=clock)
        self.signals = _InstanceSignals(self.host, db=registry.db, clock=self.host.clock)
        self.queue = DurableStrategyQueue(
            self.signals, runner=runner, source_fingerprint=source_fingerprint,
            max_pending=max_pending, instance_id=instance.instance_id)

    @property
    def instance_id(self):
        return self.instance.instance_id

    def enqueue(self, event):
        return self.queue.enqueue(event)

    def advance(self):
        return self.queue.advance()

    def status(self):
        return self.queue.status()

    @property
    def next_deadline_monotonic(self):
        return getattr(self.queue.runner, 'next_deadline_monotonic', None)

    def state(self):
        return self.host.state()

    def close(self):
        return self.queue.close()


def instance_bundle_digest(registry, instance_ids):
    """Stable digest of the exact set of registered instances and their contracts."""
    parts = []
    for instance_id in sorted(instance_ids):
        instance = registry.instance(instance_id)
        parts.append([instance.instance_id, instance.manifest.digest(),
                      instance.budget.as_dict(), instance.mode, instance.enabled])
    return digest(['strategy-instance-bundle-1', parts])
