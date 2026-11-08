"""Detached frontier set for one background collection pass.

Collection used to ask a single strategy state whether a bar was already known.
With more than one instance that question has no single answer: instance A may
have consumed a bar that instance B has not, and omitting it because A had seen
it silently starved B.

This view holds **one frontier per consumer** and answers the only question that
is safe for all of them: emit a bar unless every consumer already recorded the
identical economic content. A slow instance therefore still receives its input,
a repeat is still idempotent at each consumer, and a revised bar reaches every
consumer so the existing integrity gate can reject it.

The view is built on the SQLite owner thread and is pure data: the background
collection task holds no connection, performs no write and runs no strategy.
"""
from copy import deepcopy

from trading_contracts.market import bar_observation_identity


class CollectionView:
    """Immutable per-consumer frontiers plus the legacy single-instance one."""

    def __init__(self, *, frontiers, legacy=None, instance_ids=(), source=None,
                 bar_frontiers=None):
        self.conflicts = []
        self._frontiers = tuple(dict(frontier) for frontier in frontiers)
        # Only instances that consume bars participate in bar de-duplication.
        # A quotes-only peer must not make every bar look new just because its
        # own (irrelevant) frontier is empty. Direct callers that do not pass
        # this keep the original all-frontiers behavior.
        self._bar_frontiers = (self._frontiers if bar_frontiers is None
                               else tuple(dict(frontier) for frontier in bar_frontiers))
        self._legacy = dict(legacy) if legacy is not None else None
        self.instance_ids = tuple(instance_ids)
        self.source = source or ('legacy' if legacy is not None else 'instances')

    # -- construction ----------------------------------------------------
    @classmethod
    def legacy_view(cls, state):
        """The single-instance frontier, taken from the legacy strategy state."""
        return cls(frontiers=(), legacy=(state or {}).get('bar_observations') or {},
                   source='legacy')

    @classmethod
    def from_instances(cls, runtimes):
        """One bar frontier per active consumer that requires 5-minute bars.

        A consumer whose two frontier sources disagree contributes **no**
        knowledge: the bar is emitted for it rather than filtered by a guess.
        The conflict itself is reported for reconciliation.
        """
        runtimes = tuple(runtimes)
        frontiers, bar_frontiers, instance_ids, conflicts = [], [], [], []
        for runtime in runtimes:
            requirements = set(getattr(runtime.instance.manifest,
                                       'data_requirements', ()) or ())
            try:
                frontier = runtime.host.observations()
            except ValueError:
                frontier = {}
                conflicts.append(runtime.instance_id)
            frontiers.append(frontier)
            if '5minute-bars' in requirements:
                bar_frontiers.append(frontier)
            instance_ids.append(runtime.instance_id)
        view = cls(frontiers=frontiers, bar_frontiers=bar_frontiers,
                   instance_ids=instance_ids, source='instances')
        view.conflicts = conflicts
        return view

    def state(self):
        """Compatibility accessor: a detached mapping, never a live object."""
        return {'bar_observations': dict(self._legacy or {})}

    # -- queries ---------------------------------------------------------
    def known_everywhere(self, key, fingerprint):
        """True only when every consumer already holds this exact content.

        Any consumer that is missing the key, or holds a different fingerprint
        for it, makes the bar new for that consumer. A different fingerprint is
        a revision, and revisions must reach the owner's integrity gate rather
        than being filtered out here.
        """
        if self._legacy is not None:
            return self._legacy.get(key) == fingerprint
        if not self._bar_frontiers:
            return False
        return all(frontier.get(key) == fingerprint for frontier in self._bar_frontiers)

    def should_emit(self, data):
        key, fingerprint = bar_observation_identity(data)
        return not self.known_everywhere(key, fingerprint)

    def describe(self):
        return {'source': self.source, 'consumers': len(self._frontiers),
                'instance_ids': list(self.instance_ids),
                'frontier_sizes': [len(frontier) for frontier in self._frontiers],
                'bar_frontier_sizes': [len(frontier) for frontier in self._bar_frontiers],
                'frontier_conflicts': list(self.conflicts)}


def filter_bars_for_view(bars, view):
    """Split bars into the ones still needed and the count safely omitted."""
    if not isinstance(view, CollectionView):
        raise ValueError('COLLECTION_VIEW_REQUIRED')
    changed, omitted = [], 0
    for data in bars:
        if view.should_emit(data):
            changed.append(data)
        else:
            omitted += 1
    return changed, omitted


def active_runtimes(supervisor):
    """The consumers that are enabled *now*, not the ones present at start-up.

    A disabled instance cannot consume, so letting it hold readiness back would
    make the group bootstrap forever, and letting it hold the dedupe frontier
    back would defeat the frontier entirely. The registry is read on every
    build; a re-enabled instance is picked up on the next pass and then catches
    up from its own checkpoint.
    """
    instance_supervisor = getattr(supervisor, 'instance_supervisor', None)
    if instance_supervisor is None:
        return []
    registry = getattr(instance_supervisor, 'registry', None)
    active = []
    for instance_id in instance_supervisor.instance_ids():
        if registry is not None:
            current = registry.find(instance_id)
            if current is None or not current.enabled or current.mode == 'disabled':
                continue
        active.append(instance_supervisor.runtime(instance_id))
    return active


def build_collection_view(supervisor, *, book=None):
    """Choose the frontier shape this session actually runs.

    An instance group gets one frontier per *active* instance; a session without
    one keeps the legacy single frontier. Choosing here keeps the choice in one
    place instead of letting the collector guess from a flag.
    """
    if getattr(supervisor, 'instance_supervisor_enabled', False) is not True:
        return CollectionView.legacy_view(supervisor.signals.state())
    runtimes = active_runtimes(supervisor)
    if not runtimes:
        view = CollectionView(frontiers=[], instance_ids=[], source='instances')
        view.conflicts = []
        return view
    return CollectionView.from_instances(runtimes)


class CollectionState:
    """Detached collection metadata for whichever consumers this session runs.

    The collector decides whether to bootstrap the daily universe by looking at
    the day's prepared metadata: ``universe_date``, ``screening_summary`` and
    ``indicator_values``. With more than one instance that question is answered
    per consumer, exactly like the bar frontier: bootstrap unless *every*
    consumer already has the current day prepared. A single shared legacy state
    made a fully prepared group bootstrap again on every pass.
    """

    _REQUIRED = ('screening_summary', 'indicator_values')

    def __init__(self, *, states, instance_ids=(), legacy=None, source=None,
                 universe_instance_ids=None):
        # Deep, because these mappings are handed to a background thread and a
        # shallow copy would still share the owner's nested values.
        self._states = tuple(deepcopy(dict(state or {})) for state in states)
        self._legacy = deepcopy(dict(legacy)) if legacy is not None else None
        self.instance_ids = tuple(instance_ids)
        if self.instance_ids and len(self.instance_ids) != len(self._states):
            raise ValueError('COLLECTION_STATE_IDENTITY_MISMATCH')
        # None is the legacy direct-constructor contract: readiness covers all
        # supplied states even when the caller did not provide optional ids.
        # An explicit empty tuple means there is no universe consumer. A
        # non-empty subset must be addressable by matching instance ids; never
        # let zip silently drop a state and report a false ready.
        if universe_instance_ids is None:
            self.universe_instance_ids = self.instance_ids
            self._universe_indices = tuple(range(len(self._states)))
        else:
            subset = tuple(universe_instance_ids)
            self.universe_instance_ids = subset
            if not subset:
                self._universe_indices = ()
            else:
                if not self.instance_ids or len(self.instance_ids) != len(self._states):
                    raise ValueError('COLLECTION_STATE_IDENTITY_MISMATCH')
                index = {instance_id: position
                         for position, instance_id in enumerate(self.instance_ids)}
                try:
                    self._universe_indices = tuple(index[instance_id]
                                                   for instance_id in subset)
                except KeyError:
                    raise ValueError('COLLECTION_STATE_UNKNOWN_UNIVERSE_INSTANCE') from None
        self.source = source or ('legacy' if legacy is not None else 'instances')

    @classmethod
    def legacy_state_view(cls, state):
        return cls(states=(), legacy=state or {}, source='legacy')

    @classmethod
    def from_instances(cls, runtimes):
        runtimes = tuple(runtimes)
        universe_instance_ids = []
        for runtime in runtimes:
            requirements = set(getattr(runtime.instance.manifest,
                                       'data_requirements', ()) or ())
            if 'universe-screen' in requirements:
                universe_instance_ids.append(runtime.instance_id)
        return cls(states=[runtime.host.state() for runtime in runtimes],
                   instance_ids=[runtime.instance_id for runtime in runtimes],
                   universe_instance_ids=universe_instance_ids,
                   source='instances')

    def _consumers(self):
        if self._legacy is not None:
            return (self._legacy,)
        return self._states

    def _universe_consumers(self):
        if self._legacy is not None:
            return (self._legacy,)
        return tuple(self._states[position] for position in self._universe_indices)

    @staticmethod
    def _prepared(state, date):
        if date is None:
            return False
        if state.get('universe_date') != date:
            return False
        return all(key in state for key in CollectionState._REQUIRED)

    def ready_on(self, date):
        """True when every active universe consumer has this session day prepared."""
        consumers = self._universe_consumers()
        return not consumers or all(self._prepared(state, date)
                                   for state in consumers)

    @property
    def no_active_consumers(self):
        return self._legacy is None and not self._states

    def needs_daily(self, date):
        """Bootstrap unless the whole active group is ready for ``date``.

        A consumer that is behind, or a new session day, requires one bootstrap;
        that event is offered to every instance, so being behind once does not
        leave an instance permanently stale.

        With no active consumer there is nothing to prepare: fetching and
        publishing a universe event for nobody would be work with no consumer,
        so it is skipped and the situation is reported instead.
        """
        if self.no_active_consumers:
            return False
        return not self.ready_on(date)

    def lagging(self, date):
        if not self.instance_ids:
            # With no labels there is no honest per-instance name to return.
            # Keep the historical empty result; readiness still covers every
            # state through _universe_indices above.
            return []
        return [self.instance_ids[position] for position in self._universe_indices
                if not self._prepared(self._states[position], date)]

    def state(self):
        """A legacy-shaped mapping for the collector's summary fields."""
        lead = self._legacy
        if lead is None:
            lead = next((state for state in self._states if state), {})
        # Copied, not shared: the collector runs in another thread and must not
        # be able to mutate the owner's mapping.
        machines = dict(lead.get('machines') or {})
        return {'universe_date': lead.get('universe_date'),
                'universe_complete': lead.get('universe_complete', False),
                'screening_summary': dict(lead.get('screening_summary') or {}),
                'indicator_values': dict(lead.get('indicator_values') or {}),
                'machines': machines}

    def describe(self):
        return {'source': self.source, 'consumers': len(self._consumers()),
                'instance_ids': list(self.instance_ids),
                'universe_instance_ids': list(self.universe_instance_ids),
                'no_active_consumers': self.no_active_consumers}


def build_collection_state(supervisor):
    """Choose the metadata projection this session actually runs."""
    if getattr(supervisor, 'instance_supervisor_enabled', False) is not True:
        return CollectionState.legacy_state_view(supervisor.signals.state())
    return CollectionState.from_instances(active_runtimes(supervisor))
