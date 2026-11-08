"""Advance several strategy instances from one control loop.

The control loop owns the account. It must not stop managing instance A because
instance B raised, hung or was disabled, and it must not let one instance's
backlog starve another's advisory progress. This supervisor gives each instance
exactly one advance per tick, in a fixed order, and records what happened per
instance instead of merging the outcomes into one health number.

It never places an order, never sizes one and never reads a broker object.
"""
from trading_contracts.values import digest


class InstanceSupervisor:
    """Deterministic per-tick advance over a fixed set of instance runtimes."""

    def __init__(self, runtimes, *, clock=None):
        runtimes = tuple(runtimes)
        if not runtimes:
            raise ValueError('INSTANCE_SUPERVISOR_REQUIRES_INSTANCES')
        identifiers = [runtime.instance_id for runtime in runtimes]
        if len(set(identifiers)) != len(identifiers):
            raise ValueError('INSTANCE_SUPERVISOR_DUPLICATE_INSTANCE')
        for runtime in runtimes:
            if not callable(getattr(runtime, 'advance', None)):
                raise ValueError('INSTANCE_SUPERVISOR_RUNTIME_INVALID')
        # Sorted by instance id so a tick order is reproducible across restarts
        # and does not depend on dictionary or registration order.
        self.runtimes = {runtime.instance_id: runtime
                         for runtime in sorted(runtimes, key=lambda item: item.instance_id)}
        self.order = sorted(self.runtimes)
        self.clock = clock
        self._closed = False
        # Exposed so the control loop can re-read registered state instead of
        # trusting a runtime snapshot taken at start-up.
        host = getattr(runtimes[0], 'host', None)
        self.registry = getattr(host, 'registry', None)

    def instance_ids(self):
        return list(self.order)

    def runtime(self, instance_id):
        try:
            return self.runtimes[instance_id]
        except KeyError:
            raise ValueError('INSTANCE_SUPERVISOR_UNKNOWN_INSTANCE') from None

    def advance(self):
        """One advance per instance. A failing instance never stops the others."""
        if self._closed:
            raise ValueError('INSTANCE_SUPERVISOR_CLOSED')
        report = {'instances': {}, 'errors': [], 'pending': 0, 'busy': False}
        for instance_id in self.order:
            try:
                progress = self.runtimes[instance_id].advance()
            except Exception as exc:
                # Bounded, structural: the caller only needs to know which
                # instance faulted and that its own progress did not advance.
                report['instances'][instance_id] = {'advanced': False,
                                                    'reason': type(exc).__name__}
                report['errors'].append({'instance_id': instance_id,
                                         'reason': type(exc).__name__})
                continue
            published = self._publishable(progress)
            report['instances'][instance_id] = published
            report['pending'] += published.get('pending', 0) or 0
            report['busy'] = report['busy'] or bool(published.get('busy'))
            # An instance that returns normally can still have rejected work.
            # Leaving those only in the nested report meant the control loop,
            # which reads the top level, never saw a timeout or an integrity
            # rejection and could not inhibit new risk because of it.
            for item in published.get('errors') or ():
                if isinstance(item, dict) and isinstance(item.get('reason'), str):
                    report['errors'].append({'instance_id': instance_id,
                                             'reason': item['reason'],
                                             'scope': 'STRATEGY_INSTANCE'})
        return report

    def strategy_busy(self):
        """True when any instance still has advisory work outstanding."""
        return any(self.runtimes[instance_id].status().get('busy')
                   for instance_id in self.order)

    def next_deadline_monotonic(self):
        """The earliest worker deadline, so a waiting loop can bound its sleep."""
        deadlines = []
        for instance_id in self.order:
            deadline = getattr(self.runtimes[instance_id], 'next_deadline_monotonic', None)
            if isinstance(deadline, (int, float)):
                deadlines.append(deadline)
        return min(deadlines) if deadlines else None

    @staticmethod
    def _publishable(progress):
        """Keep only stable, structural fields out of an instance's own report."""
        if not isinstance(progress, dict):
            return {'advanced': False, 'reason': 'INSTANCE_PROGRESS_INVALID'}
        published = {'advanced': True,
                     'completed': len(progress.get('completed') or ()),
                     'rejected': len(progress.get('rejected') or ()),
                     'started': progress.get('started'),
                     'pending': progress.get('pending', 0),
                     'busy': bool(progress.get('busy')),
                     'errors': [{'reason': item['reason']}
                                for item in (progress.get('errors') or ())
                                if isinstance(item, dict)
                                and isinstance(item.get('reason'), str)]}
        return published

    def status(self):
        return {instance_id: self.runtimes[instance_id].status()
                for instance_id in self.order}

    def enqueue(self, instance_id, event):
        return self.runtime(instance_id).enqueue(event)

    def state_digest(self):
        """Identity of the exact set of instances this loop is running."""
        return digest(['instance-supervisor-1',
                       [[instance_id,
                         self.runtimes[instance_id].status().get('config_fingerprint'),
                         self.runtimes[instance_id].status().get('source_fingerprint')]
                        for instance_id in self.order]])

    def close(self):
        if self._closed:
            return {'closed': True}
        outcomes = {}
        for instance_id in self.order:
            try:
                self.runtimes[instance_id].close()
            except Exception as exc:
                outcomes[instance_id] = {'closed': False, 'reason': type(exc).__name__}
            else:
                outcomes[instance_id] = {'closed': True}
        self._closed = True
        return outcomes
