"""Per-instance advisory host over shared infrastructure state.

One account, many strategy instances. This host answers a single question for
one instance: given one market event, what does that instance's plugin advise,
and what is its next checkpoint? It owns prepare/commit for that instance only.

What it deliberately does not do: size an order, reserve account money, take a
symbol lease, place or cancel anything, or read the execution ledger. Those are
infrastructure decisions made after, and independently of, an advisory result.

The instance's checkpoint lives in its own registry namespace and is committed
with compare-and-set, so two instances can never inherit each other's state and
a stale computation can never overwrite newer state.
"""
from dataclasses import dataclass
from decimal import Decimal
import hashlib
import json
from datetime import datetime, timezone

from trading_contracts.instance import StrategyInstance, config_version
from trading_contracts.strategy import StrategyDecision, StrategyInput
from trading_contracts.market import bar_observation_identity
from trading_contracts.values import canonical, digest, thaw, timestamp
from .transactions import atomic

STALE_EVENT_SECONDS = 30
FEEDBACK_STALE_SECONDS = 30

_SCHEMA = '''
CREATE TABLE IF NOT EXISTS strategy_instance_proposals(
    instance_id TEXT NOT NULL, proposal_id TEXT NOT NULL, event_id TEXT NOT NULL,
    trade_id TEXT NOT NULL, action TEXT NOT NULL, symbol TEXT NOT NULL,
    valid_until TEXT NOT NULL, state TEXT NOT NULL
        CHECK(state IN ('PENDING','CONSUMED','EXPIRED')),
    payload BLOB NOT NULL, recorded_at TEXT NOT NULL,
    PRIMARY KEY(instance_id,proposal_id));
CREATE INDEX IF NOT EXISTS strategy_instance_proposals_pending
    ON strategy_instance_proposals(instance_id,state,valid_until);
CREATE TABLE IF NOT EXISTS strategy_instance_consumed(
    instance_id TEXT NOT NULL, event_id TEXT NOT NULL, event_hash TEXT NOT NULL,
    consumed_at TEXT NOT NULL, PRIMARY KEY(instance_id,event_id));
CREATE TABLE IF NOT EXISTS strategy_instance_observations(
    instance_id TEXT NOT NULL, observation_key TEXT NOT NULL,
    fingerprint TEXT NOT NULL, observed_at TEXT NOT NULL,
    PRIMARY KEY(instance_id,observation_key));
'''


@dataclass(frozen=True)
class PreparedInstanceInput:
    """A pinned request plus the exact checkpoint text it was computed from."""
    request: StrategyInput
    checkpoint: dict
    checkpoint_digest: str
    # None means "this instance has no stored checkpoint yet". Distinguishing
    # absent from empty is what makes the compare-and-set exact.
    expected_state: dict | None = None

    @property
    def checkpoint_fingerprint(self):
        """Queue-facing name for the checkpoint identity this input was built on."""
        return self.checkpoint_digest


class InstanceStrategyHost:
    """Prepare and commit one instance's advisory evaluation. No execution authority."""

    def __init__(self, registry, instance, *, clock=None):
        if not isinstance(instance, StrategyInstance):
            raise ValueError('INSTANCE_REQUIRED')
        if registry.find(instance.instance_id) is None:
            raise ValueError('INSTANCE_NOT_REGISTERED')
        self.registry = registry
        self.instance = instance
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.db = registry.db
        self.db.executescript(_SCHEMA)
        self.identity = instance.strategy_identity()
        self.config_version = config_version(
            instance.manifest.plugin_id, instance.manifest.version,
            instance.manifest.config, instance.manifest.plugin_identity)
        # Mode and source provenance are separate versioned facts. Only the
        # synthetic shadow mode accepts replay input; a read-only real-source
        # shadow carries real provenance but still has no execution capability.
        from trading_contracts.instance import (MODE_EXECUTION_CAPABILITY,
                                                MODE_PROVENANCE)
        self.source_mode = MODE_PROVENANCE[instance.mode]
        self.synthetic = self.source_mode == 'synthetic'
        self.execution_capability = MODE_EXECUTION_CAPABILITY[instance.mode]

    # -- reads -----------------------------------------------------------
    def state(self):
        return self.registry.state(self.instance.instance_id) or {}

    def config_fingerprint(self):
        """Identity of everything that would change this instance's decisions."""
        from trading_contracts.values import digest
        return digest(['strategy-instance-config-1', self.instance.instance_id,
                       self.instance.manifest.digest(), self.instance.budget.as_dict()])

    def observation(self, observation_key):
        """What this instance last recorded for one observation key, if anything."""
        row = self.db.execute(
            'SELECT fingerprint FROM strategy_instance_observations'
            ' WHERE instance_id=? AND observation_key=?',
            (self.instance.instance_id, observation_key)).fetchone()
        recorded = self._checkpoint_observations().get(observation_key)
        if row is not None:
            if recorded is not None and recorded != row[0]:
                # Two sources describe the same consumed key differently. Picking
                # one would silently discard the other, so this fails closed.
                raise ValueError('INSTANCE_OBSERVATION_SOURCE_CONFLICT')
            return row[0]
        # A checkpoint written before this index existed still records what the
        # instance consumed. Ignoring it would let a revised bar through and
        # then let the next checkpoint overwrite that older evidence.
        return recorded

    def adopt_checkpoint_observations(self, *, at):
        """Persist the checkpoint's own frontier into the durable index.

        Reading a legacy checkpoint on demand is not enough: the next ordinary
        commit may return a checkpoint without ``bar_observations`` at all, and
        every historical key would then disappear with no trace. Writing them
        into the index inside the commit transaction makes them survive that.

        A key already recorded differently is a conflict, not a silent choice.
        """
        adopted = 0
        for key, fingerprint in self._checkpoint_observations().items():
            row = self.db.execute(
                'SELECT fingerprint FROM strategy_instance_observations'
                ' WHERE instance_id=? AND observation_key=?',
                (self.instance.instance_id, key)).fetchone()
            if row is not None:
                if row[0] != fingerprint:
                    raise ValueError('INSTANCE_OBSERVATION_SOURCE_CONFLICT')
                continue
            self.db.execute(
                'INSERT INTO strategy_instance_observations(instance_id,'
                ' observation_key,fingerprint,observed_at) VALUES(?,?,?,?)',
                (self.instance.instance_id, key, fingerprint,
                 at.astimezone(timezone.utc).isoformat()))
            adopted += 1
        return adopted

    def _checkpoint_observations(self):
        stored = self.registry.state(self.instance.instance_id) or {}
        recorded = stored.get('bar_observations')
        return dict(recorded) if isinstance(recorded, dict) else {}

    def observations(self):
        """One detached frontier per instance, new index and checkpoint combined.

        Both sources are consumed facts. They may agree, one may be silent, or
        they may disagree; disagreement is reported instead of resolved, because
        silently preferring either side would hide a revision.
        """
        merged = self._checkpoint_observations()
        for row in self.db.execute(
                'SELECT observation_key,fingerprint FROM strategy_instance_observations'
                ' WHERE instance_id=?', (self.instance.instance_id,)):
            prior = merged.get(row[0])
            if prior is not None and prior != row[1]:
                raise ValueError('INSTANCE_OBSERVATION_SOURCE_CONFLICT')
            merged[row[0]] = row[1]
        return merged

    def record_observation(self, observation_key, fingerprint, *, at):
        """Record one consumed observation inside the caller's transaction.

        The instance, not the process, owns its frontier: two instances with
        different configurations must be able to disagree about whether they
        have seen a bar. Collection therefore cannot use one shared frontier.

        An already-consumed key may not change its economic content. Updating in
        place would let a silently revised bar rewrite the frontier that the
        integrity gate is supposed to protect, so a revision is a hard error.
        """
        if not isinstance(observation_key, str) or not observation_key:
            raise ValueError('INSTANCE_OBSERVATION_KEY_REQUIRED')
        if not isinstance(fingerprint, str) or not fingerprint:
            raise ValueError('INSTANCE_OBSERVATION_FINGERPRINT_REQUIRED')
        prior = self.observation(observation_key)
        if prior is not None and prior != fingerprint:
            raise ValueError('CONSUMED_BAR_REVISED')
        self.db.execute(
            'INSERT INTO strategy_instance_observations(instance_id,observation_key,'
            ' fingerprint,observed_at) VALUES(?,?,?,?)'
            ' ON CONFLICT(instance_id,observation_key)'
            ' DO UPDATE SET fingerprint=excluded.fingerprint,'
            ' observed_at=excluded.observed_at',
            (self.instance.instance_id, observation_key, fingerprint,
             at.astimezone(timezone.utc).isoformat()))

    def check_observation(self, event):
        """Refuse an event whose economic content revises a consumed bar.

        Called before computation and again inside the commit transaction, so a
        revision cannot slip in through a lock wait, and a rejection leaves the
        checkpoint, the proposal outbox and the consumption marker untouched.
        """
        pair = self._bar_identity(event)
        if pair is None:
            return
        key, fingerprint = pair
        stored = self.observation(key)
        if stored is not None and stored != fingerprint:
            raise ValueError('CONSUMED_BAR_REVISED')

    @staticmethod
    def _bar_identity(event):
        if not isinstance(event, dict) or event.get('kind') != 'bar':
            return None
        data = event.get('data')
        if not isinstance(data, dict):
            return None
        return bar_observation_identity(data)

    def consumed_hash(self, event_id):
        row = self.db.execute('SELECT event_hash FROM strategy_instance_consumed'
                              ' WHERE instance_id=? AND event_id=?',
                              (self.instance.instance_id, event_id)).fetchone()
        return row[0] if row else None

    def allocation_view(self):
        """What this instance may assume about its own slice, never the account's."""
        budget = self.instance.budget
        return {'instance_id': self.instance.instance_id,
                'max_cash': str(budget.max_cash),
                'max_planned_loss': str(budget.max_planned_loss),
                'max_notional': str(budget.max_notional),
                'max_concurrent': budget.max_concurrent}

    def _has_table(self, name):
        return self.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (name,)).fetchone() is not None

    def _broker_observation_at(self):
        """The execution owner's own latest complete broker observation time.

        This is deliberately separate from feedback occurred_at: a position may
        be idle for a long time while the broker is watched continuously. Only
        ``last_snapshot`` describes whether the owner currently SEES the
        account. It is never inferred from a feedback event.
        """
        if not self._has_table('live_state'):
            return None
        row = self.db.execute(
            'SELECT payload FROM live_state WHERE id=1').fetchone()
        if row is None:
            return None
        try:
            state = json.loads(row[0])
        except (TypeError, ValueError):
            return None
        if not isinstance(state, dict):
            return None
        observed = state.get('last_snapshot')
        return observed if isinstance(observed, str) and observed else None

    def _current_owner_activity(self, open_quantity):
        """Current live-action state for trades this instance owns.

        Ownership comes from the execution attribution table, never from a
        symbol. Missing tables mean the answer is unknown, not absent.
        """
        result = {'state': None, 'at': None, 'kind': None, 'trade_id': None,
                  'owned_trades': 0, 'source': 'live_actions'}
        if not (self._has_table('live_actions')
                and self._has_table('execution_attribution')):
            result['source'] = 'unavailable'
            return result
        owned = {row[0] for row in self.db.execute(
            'SELECT DISTINCT trade_id FROM execution_attribution'
            ' WHERE instance_id=?', (self.instance.instance_id,))}
        result['owned_trades'] = len(owned)
        if not owned:
            return result
        rows = [row for row in self.db.execute(
            'SELECT trade_id,kind,state,updated_at,payload FROM live_actions'
            ' ORDER BY updated_at,id')
            if row[0] in owned and row[2] in
            {'READY', 'RUNNING', 'WAITING_USER', 'UNKNOWN'}]
        if not rows:
            return result
        latest = max(rows, key=lambda row: (row[3] or '', row[0], row[1]))
        result.update(state=latest[2], at=latest[3], kind=latest[1],
                      trade_id=latest[0])
        # Every currently live sell action reserves its own quantity. Using only
        # the latest action would overstate available quantity when several
        # exits are live; an unresolvable quantity keeps the answer unknown.
        # No live sell reservation at all is also unknown: WAITING_USER/UNKNOWN
        # actions are not evidence that the whole position is still sellable.
        reserved = Decimal(0)
        reservations = 0
        for row in rows:
            if row[1] != 'exit' or row[2] not in {'READY', 'RUNNING'}:
                continue
            try:
                payload = json.loads(row[4] or '{}')
            except (TypeError, ValueError):
                reserved = None
                break
            parameters = payload.get('parameters') if isinstance(payload, dict) else None
            quantity = parameters.get('quantity') if isinstance(parameters, dict) else None
            try:
                amount = Decimal(str(quantity))
            except (ArithmeticError, TypeError, ValueError):
                reserved = None
                break
            if amount <= 0:
                reserved = None
                break
            reserved += amount
            reservations += 1
        if reserved is not None and reservations:
            result['reserved_quantity'] = reserved
        return result

    def execution_view(self):
        """Instance-owned execution facts from current owner state and feedback.

        The view aggregates, in one place: execution attribution ownership,
        the instance's fills/accounting, current management-action state from
        the execution owner, and a fresh broker-observation timestamp. It never
        contains a broker object, account, order/approval/ref identity or
        another instance's data.

        Two clocks are kept distinct. ``observed_at``/``fact_fresh`` describe
        the latest activity fact; ``observation_at``/``observation_fresh``
        describe whether the owner currently sees the broker account. A flat
        recorded long ago remains verified when the account is freshly observed
        flat, and no observation ever fabricates a new activity fact.
        """
        rows = []
        if self._has_table('execution_feedback'):
            rows = [{'kind': row[0], 'occurred_at': row[1],
                     'sequence': row[2]}
                    for row in self.db.execute(
                        'SELECT kind,occurred_at,sequence FROM execution_feedback'
                        ' WHERE instance_id=? ORDER BY sequence',
                        (self.instance.instance_id,))]
        fills = []
        if self._has_table('instance_fills'):
            fills = [{'side': row[0], 'quantity': row[1]}
                     for row in self.db.execute(
                         'SELECT side,quantity FROM instance_fills'
                         ' WHERE instance_id=? ORDER BY occurred_at,fill_id',
                         (self.instance.instance_id,))]
        try:
            open_quantity = sum((Decimal(row['quantity']) *
                                 (Decimal(1) if row['side'] == 'buy' else Decimal(-1))
                                 for row in fills), Decimal(0))
        except (ArithmeticError, TypeError, ValueError):
            open_quantity = Decimal(-1)
        fill_facts = {'filled', 'partial_fill', 'protection_covered',
                      'exit', 'cancel_terminal'}
        if open_quantity < 0:
            open_quantity = Decimal(0)
            accounting_state = 'UNKNOWN'
        elif not fills and any(row['kind'] in fill_facts for row in rows):
            accounting_state = 'UNKNOWN'
        else:
            accounting_state = 'CONFIRMED'
        latest = max(rows, key=lambda item: (item['occurred_at'],
                                             item['sequence'])) if rows else None
        observed = latest['occurred_at'] if latest else None
        version = max((row['sequence'] for row in rows), default=0)

        def fresh_at(value):
            if value is None:
                return False
            try:
                age = (self.clock() - timestamp(value)).total_seconds()
                return 0 <= age <= FEEDBACK_STALE_SECONDS
            except (TypeError, ValueError, OverflowError):
                return False

        fresh = fresh_at(observed)
        observation_at = self._broker_observation_at()
        observation_fresh = fresh_at(observation_at)
        activity = self._current_owner_activity(open_quantity)
        activity_after = False
        try:
            activity_after = bool(activity['at'] and observed
                                  and timestamp(activity['at']) > timestamp(observed))
        except (TypeError, ValueError, OverflowError):
            activity_after = False
        kind = latest['kind'] if latest else None
        owner_state = activity['state'] if activity_after else None
        owner_kind = activity['kind'] if activity_after else None
        lifecycle = None
        if accounting_state != 'CONFIRMED':
            lifecycle = 'unknown'
        elif open_quantity > 0:
            if owner_state in {'WAITING_USER', 'UNKNOWN'}:
                lifecycle = 'unknown'
            elif owner_state is not None:
                lifecycle = ('protected' if owner_kind == 'protect'
                             else 'exit_pending' if owner_kind == 'exit'
                             else 'pending')
            elif kind == 'unknown':
                lifecycle = 'unknown'
            elif kind == 'protection_covered':
                lifecycle = 'protected'
            elif kind in {'exit', 'cancel_terminal'}:
                lifecycle = 'exit_pending'
            elif kind in {'pending_confirmation', 'order_accepted'}:
                lifecycle = 'pending'
            elif kind in {'partial_fill', 'filled'}:
                lifecycle = 'partial'
            else:
                lifecycle = 'unknown'
        else:
            if owner_state in {'WAITING_USER', 'UNKNOWN'}:
                lifecycle = 'unknown'
            elif owner_state is not None:
                lifecycle = 'pending'
            elif kind == 'flat':
                lifecycle = 'flat_verified' if observation_fresh else 'unknown'
            elif kind == 'rejected':
                lifecycle = 'not_executed'
            elif kind in {'pending_confirmation', 'order_accepted'}:
                lifecycle = 'pending'
            elif not rows and not fills:
                lifecycle = 'not_executed'
            else:
                lifecycle = 'unknown'
        available = Decimal(0)
        available_state = 'unknown'
        if open_quantity == 0:
            if lifecycle == 'flat_verified':
                available_state = 'confirmed'
        elif accounting_state == 'CONFIRMED' and activity_after:
            reserved = activity.get('reserved_quantity')
            if reserved is not None:
                available = max(Decimal(0), open_quantity - reserved)
                available_state = 'confirmed'
        if available_state != 'confirmed':
            available_text = 'unknown'
        else:
            available_text = str(available)
        return {'instance_id': self.instance.instance_id,
                'lifecycle': lifecycle,
                'confirmed_quantity': str(open_quantity),
                'available_quantity': available_text,
                'available_quantity_state': available_state,
                'feedback_version': version,
                'observed_at': observed,
                'fact_fresh': fresh,
                'observation_at': observation_at,
                'observation_fresh': observation_fresh,
                'owner_state': activity['state'],
                'owner_action_at': activity['at'],
                'owner_action_kind': activity['kind'],
                'owner_trade_id': activity['trade_id'],
                'owner_activity_source': activity['source'],
                'source': 'infra_execution_feedback',
                'accounting_state': accounting_state}

    # -- prepare ---------------------------------------------------------
    def prepare(self, event):
        """Pin one event for this instance without holding a write transaction."""
        if not isinstance(event, dict):
            raise ValueError('STRATEGY_EVENT_OBJECT_REQUIRED')
        current = self.registry.instance(self.instance.instance_id)
        if not current.enabled or current.mode == 'disabled':
            raise ValueError('INSTANCE_DISABLED')
        from trading_contracts.instance import MODE_PROVENANCE
        expected_synthetic = MODE_PROVENANCE[current.mode] == 'synthetic'
        if event.get('synthetic_input') is not expected_synthetic:
            raise ValueError('STRATEGY_EVENT_PROVENANCE_REQUIRED')
        if not event.get('source_sha256'):
            raise ValueError('STRATEGY_EVENT_PROVENANCE_REQUIRED')
        now = self.clock()
        age = (now - timestamp(event['at'])).total_seconds()
        if not 0 <= age <= STALE_EVENT_SECONDS:
            raise ValueError('STALE_STRATEGY_EVENT')
        event_id = event.get('id')
        if not isinstance(event_id, str) or not event_id:
            raise ValueError('STRATEGY_EVENT_OBJECT_REQUIRED')
        event_hash = digest(event)
        prior = self.consumed_hash(event_id)
        if prior is not None:
            if prior != event_hash:
                # The same identity carrying different content is an identity
                # conflict; a different identity revising a consumed bar is a
                # revision. Reporting the precise one matters because the
                # control loop treats them differently.
                raise ValueError('STRATEGY_EVENT_ID_CONFLICT')
            return None
        # A revision of an already-consumed bar is refused before any
        # computation is requested.
        self.check_observation(event)
        stored = self.registry.state(self.instance.instance_id)
        checkpoint = stored or {}
        request = StrategyInput(
            identity=self.identity, as_of=now, cursor=event_id,
            data_snapshot_id='event-' + event_hash[:32],
            source_sha256=event_hash, config_version=self.config_version,
            synthetic=expected_synthetic,
            market={'event': event},
            allocation={**self.allocation_view(), 'execution': self.execution_view()},
            budget={'max_concurrent': self.instance.budget.max_concurrent},
            checkpoint=checkpoint)
        return PreparedInstanceInput(request=request, checkpoint=checkpoint,
                                     checkpoint_digest=self._checkpoint_digest(checkpoint),
                                     expected_state=stored)

    def restore(self, event, row):
        """Rebuild the exact prepared input a persisted task was issued with.

        A restart must reuse the original request, its original ``as_of`` and
        its original checkpoint. It must not re-prepare, which would refresh
        both the clock and the checkpoint and could turn an expired request into
        a fresh one. The absent-versus-empty checkpoint distinction is preserved
        because it changes what the compare-and-set accepts.
        """
        from trading_contracts.codec import decode_input
        encoded = row['request_payload']
        request = decode_input(encoded)
        if (request.cursor != row['event_id'] or request.source_sha256 != row['event_hash']
                or request.identity != self.identity
                or request.config_version != self.config_version
                or digest(thaw(request.market.get('event') or {})) != row['event_hash']):
            raise ValueError('STRATEGY_QUEUE_REQUEST_CORRUPT')
        checkpoint = thaw(request.checkpoint)
        if self._checkpoint_digest(checkpoint) != row['checkpoint_hash']:
            raise ValueError('STRATEGY_QUEUE_REQUEST_CORRUPT')
        expected = None if row.get('checkpoint_absent') else checkpoint
        now = self.clock()
        if not 0 <= (now - request.as_of).total_seconds() <= STALE_EVENT_SECONDS:
            raise ValueError('STALE_STRATEGY_EVENT')
        if request.as_of > now:
            raise ValueError('STRATEGY_DECISION_EXPIRED')
        return PreparedInstanceInput(request=request, checkpoint=checkpoint,
                                     checkpoint_digest=row['checkpoint_hash'],
                                     expected_state=expected)

    def _checkpoint_digest(self, checkpoint):
        return digest(['strategy-instance-state-1', self.instance.instance_id,
                       'checkpoint', canonical(checkpoint)])

    # -- commit ----------------------------------------------------------
    def commit(self, prepared, decision):
        """Compare-and-set this instance's checkpoint and consume the event once."""
        if not isinstance(prepared, PreparedInstanceInput):
            raise ValueError('INSTANCE_PREPARED_INPUT_REQUIRED')
        if not isinstance(decision, StrategyDecision):
            raise ValueError('STRATEGY_HOST_DECISION_CONTRACT_REQUIRED')
        request = prepared.request
        decision.validate_for(request)
        if self.registry.find(self.instance.instance_id) is None:
            raise ValueError('INSTANCE_NOT_REGISTERED')
        current = self.registry.instance(self.instance.instance_id)
        if (current.manifest.digest() != self.instance.manifest.digest()
                or self.config_version != config_version(
                    current.manifest.plugin_id, current.manifest.version,
                    current.manifest.config, current.manifest.plugin_identity)
                or (current.mode, current.enabled) != (self.instance.mode, self.instance.enabled)):
            raise ValueError('STRATEGY_INSTANCE_CONTRACT_CHANGED')
        event_hash = request.source_sha256
        prior = self.consumed_hash(request.cursor)
        if prior is not None:
            if prior != event_hash:
                raise ValueError('STRATEGY_EVENT_ID_CONFLICT')
            return {'event_id': request.cursor, 'duplicate': True,
                    'checkpoint_digest': prepared.checkpoint_digest, 'proposals': []}
        event = thaw(request.market.get('event') or {})
        self._check_response_window(request, decision, event, self.clock())
        self.db.execute('BEGIN IMMEDIATE')
        try:
            # A lock wait can outlast the response window; the checks above ran
            # before it. Re-read the clock inside the transaction, immediately
            # before the first durable write, exactly as the legacy host does.
            self._check_response_window(request, decision, event, self.clock())
            # Historical frontier facts are adopted BEFORE the checkpoint is
            # replaced. A plugin that returns a checkpoint without
            # ``bar_observations`` would otherwise erase the only remaining
            # record of bars consumed under the previous shape.
            self.adopt_checkpoint_observations(at=self.clock())
            outcome = self.registry.put_state(
                self.instance.instance_id, thaw(decision.next_checkpoint),
                expected=prepared.expected_state)
            self.db.execute('INSERT INTO strategy_instance_consumed'
                            '(instance_id,event_id,event_hash,consumed_at) VALUES(?,?,?,?)',
                            (self.instance.instance_id, request.cursor, event_hash,
                             self.clock().isoformat()))
            # A consumed bar becomes part of this instance's own frontier, in
            # the same transaction that consumes it. Collection reads these
            # per-instance rather than a single shared map. A revised bar raises
            # here, which rolls the whole commit back.
            if event.get('kind') == 'bar':
                self.check_observation(event)
                key, fingerprint = bar_observation_identity(event['data'])
                self.record_observation(key, fingerprint, at=self.clock())
            published = self._publish_proposals(request, decision)
            # Publishing encodes and writes the whole decision, which can take
            # long enough for the advice to expire. Re-check at the final commit
            # boundary, not only at entry and before the first write.
            self._check_response_window(request, decision, event, self.clock())
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return {'event_id': request.cursor, 'duplicate': False,
                'checkpoint_digest': outcome, 'proposals': published}

    def _check_response_window(self, request, decision, event, now):
        """The request, its event and every proposal must still be live.

        Three separate clocks matter here and the legacy path only ever checked
        one of them: the event's own age, the time the request was issued, and
        the validity the strategy itself attached to each proposal.
        """
        issued = now - request.as_of
        if issued.total_seconds() < 0 or issued.total_seconds() > STALE_EVENT_SECONDS:
            raise ValueError('STALE_STRATEGY_EVENT')
        age = (now - timestamp(event.get('at'))).total_seconds()             if event.get('at') is not None else None
        if age is not None and not 0 <= age <= STALE_EVENT_SECONDS:
            raise ValueError('STALE_STRATEGY_EVENT')
        for proposal in decision.proposals:
            if now >= proposal.valid_until:
                raise ValueError('STRATEGY_DECISION_EXPIRED')

    def _publish_proposals(self, request, decision):
        """Persist every advisory proposal inside the commit transaction.

        The queue's acknowledgement is a summary; it is not the only place a
        proposal exists. Storing the full decision payload alongside the
        checkpoint is what makes a restart able to recover the advice instead of
        reporting a duplicate and dropping it.
        """
        from trading_contracts.codec import encode_decision
        published = []
        payload = encode_decision(decision)
        for proposal in decision.proposals:
            self.db.execute(
                'INSERT INTO strategy_instance_proposals(instance_id,proposal_id,event_id,'
                ' trade_id,action,symbol,valid_until,state,payload,recorded_at)'
                ' VALUES(?,?,?,?,?,?,?,?,?,?)'
                ' ON CONFLICT(instance_id,proposal_id) DO NOTHING',
                (self.instance.instance_id, proposal.proposal_id, request.cursor,
                 request.cursor, proposal.action.value, proposal.symbol,
                 proposal.valid_until.isoformat(), 'PENDING', payload,
                 self.clock().isoformat()))
            published.append({'proposal_id': proposal.proposal_id,
                              'action': proposal.action.value,
                              'symbol': proposal.symbol,
                              'valid_until': proposal.valid_until.isoformat()})
        return published

    # -- advisory outbox -------------------------------------------------
    def pending_proposals(self, *, now=None):
        """Proposals this instance produced that no downstream stage consumed yet."""
        moment = (now or self.clock()).astimezone(timezone.utc).isoformat()
        return [{'proposal_id': row[0], 'event_id': row[1], 'trade_id': row[2],
                 'action': row[3], 'symbol': row[4], 'valid_until': row[5]}
                for row in self.db.execute(
                    'SELECT proposal_id,event_id,trade_id,action,symbol,valid_until'
                    ' FROM strategy_instance_proposals'
                    ' WHERE instance_id=? AND state=? AND valid_until>?'
                    ' ORDER BY recorded_at,proposal_id',
                    (self.instance.instance_id, 'PENDING', moment))]

    def proposal_detail(self, proposal_id):
        """The full advisory proposal, decoded from the durable outbox."""
        from trading_contracts.codec import decode_decision
        row = self.db.execute(
            'SELECT payload,state,trade_id FROM strategy_instance_proposals'
            ' WHERE instance_id=? AND proposal_id=?',
            (self.instance.instance_id, proposal_id)).fetchone()
        if row is None:
            raise ValueError('INSTANCE_PROPOSAL_UNKNOWN')
        decision = decode_decision(row[0])
        for proposal in decision.proposals:
            if proposal.proposal_id == proposal_id:
                return {'proposal': proposal, 'state': row[1], 'trade_id': row[2]}
        raise ValueError('INSTANCE_PROPOSAL_UNKNOWN')

    def expire_proposals(self, *, now=None):
        """Mark proposals whose own validity elapsed. Never deletes them."""
        moment = (now or self.clock()).astimezone(timezone.utc).isoformat()
        cursor = self.db.execute(
            "UPDATE strategy_instance_proposals SET state='EXPIRED'"
            " WHERE instance_id=? AND state='PENDING' AND valid_until<=?",
            (self.instance.instance_id, moment))
        return cursor.rowcount

    def claim_proposal(self, proposal_id):
        """Consume one proposal exactly once. A repeat reports it as already taken."""
        with atomic(self.db):
            cursor = self.db.execute(
                "UPDATE strategy_instance_proposals SET state='CONSUMED'"
                " WHERE instance_id=? AND proposal_id=? AND state='PENDING'",
                (self.instance.instance_id, proposal_id))
        if cursor.rowcount == 1:
            return {'proposal_id': proposal_id, 'claim': 'CONSUMED'}
        row = self.db.execute(
            'SELECT state FROM strategy_instance_proposals'
            ' WHERE instance_id=? AND proposal_id=?',
            (self.instance.instance_id, proposal_id)).fetchone()
        return {'proposal_id': proposal_id,
                'claim': row[0] if row else 'UNKNOWN'}

    # -- diagnostics -----------------------------------------------------
    def record(self, record_kind, record_id, detail):
        self.registry.record(self.instance.instance_id, record_kind, record_id, detail)

    def events(self):
        return self.registry.events(self.instance.instance_id)


def event_source_hash(event):
    """Stable identity of one raw event, shared by every instance that sees it."""
    return hashlib.sha256(canonical(event).encode('utf-8')).hexdigest()
