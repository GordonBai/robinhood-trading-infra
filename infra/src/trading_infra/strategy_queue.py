"""Durable FIFO for pure advisory work, on the owner's connection.

The caller must already own the book's controller lock. Children receive only
contract bytes, never this connection. Restart recomputes the exact persisted
request; it neither extends deadlines nor retries broker operations.
"""
from contextlib import contextmanager
import hashlib
import json
import re
import threading

from trading_contracts.codec import MAX_BYTES, encode_input, decode_input, encode_decision
from trading_contracts.instance import LEGACY_INSTANCE_ID
from trading_contracts.strategy import PreparedSignalInput
from trading_contracts.values import canonical, digest, thaw, timestamp


_COLUMNS = ('sequence', 'event_id', 'event_hash', 'event_payload', 'state', 'queued_at',
    'source_fingerprint', 'config_fingerprint', 'request_payload', 'request_hash',
    'checkpoint_hash', 'started_at', 'finished_at', 'reason', 'result_json',
    'decision_payload', 'decision_hash', 'attempts', 'recoveries')
# Polling reads the current task twice per advance while a child is computing.
# The three payload columns hold the whole event/request/decision (hundreds of
# KiB); they are never needed to poll, reject or acknowledge a task.
_LIGHT_COLUMNS = tuple(name for name in _COLUMNS
    if name not in ('event_payload', 'request_payload', 'decision_payload'))
_SAFE_ERRORS = frozenset({
    'STALE_STRATEGY_EVENT', 'STRATEGY_EVENT_ID_CONFLICT', 'STRATEGY_EVENT_PROVENANCE_REQUIRED',
    'STRATEGY_CHECKPOINT_CHANGED_RETRY_REQUIRED', 'STRATEGY_DECISION_EXPIRED',
    'STRATEGY_PREPARED_CONFIG_CHANGED', 'STRATEGY_PREPARED_INPUT_CHANGED',
    'CONSUMED_BAR_REVISED', 'UNSUPPORTED_LIVE_STRATEGY_EVENT',
    'STRATEGY_PROCESS_TIMEOUT', 'STRATEGY_PROCESS_CRASHED', 'STRATEGY_PROCESS_INVALID_OUTPUT',
    'STRATEGY_PROCESS_STDOUT_LIMIT', 'STRATEGY_PROCESS_STDERR_LIMIT',
    'STRATEGY_PROCESS_INPUT_NOT_DELIVERED', 'STRATEGY_PROCESS_IO_FAILED',
    'STRATEGY_PROCESS_START_FAILED', 'STRATEGY_PROCESS_REAP_PENDING',
    'STRATEGY_QUEUE_EVENT_CORRUPT', 'STRATEGY_QUEUE_REQUEST_CORRUPT',
})


def _reason(exc, fallback):
    value = str(exc)
    return value if value in _SAFE_ERRORS else fallback


def _event_bytes(event):
    if (not isinstance(event, dict) or not isinstance(event.get('id'), str)
            or not 0 < len(event['id']) <= 256):
        raise ValueError('STRATEGY_QUEUE_EVENT_INVALID')
    chunks = []; size = 0
    try:
        encoder = json.JSONEncoder(sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)
        for piece in encoder.iterencode(event):
            raw = piece.encode('utf-8'); size += len(raw)
            if size > MAX_BYTES:
                raise ValueError()
            chunks.append(raw)
    except (ValueError, TypeError, RecursionError, UnicodeError):
        raise ValueError('STRATEGY_QUEUE_EVENT_INVALID') from None
    return b''.join(chunks)


_TASK_COLUMNS_SQL = '''
    instance_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    event_hash TEXT NOT NULL,event_payload BLOB NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('QUEUED','RUNNING','COMMITTED','REJECTED')),
    queued_at TEXT NOT NULL,source_fingerprint TEXT NOT NULL,config_fingerprint TEXT NOT NULL,
    request_payload BLOB,request_hash TEXT,checkpoint_hash TEXT,started_at TEXT,finished_at TEXT,
    reason TEXT,result_json TEXT,decision_payload BLOB,decision_hash TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,recoveries INTEGER NOT NULL DEFAULT 0,
    checkpoint_absent INTEGER NOT NULL DEFAULT 0,
    UNIQUE(instance_id,event_id)'''


def ensure_strategy_task_schema(db, *, instance_id=LEGACY_INSTANCE_ID):
    """Create or migrate ``strategy_tasks`` to the instance-namespaced shape.

    V1 kept one table-wide unique ``event_id``, which made it impossible for two
    instances to consume the same market event. The migration rebuilds the table
    and stamps every existing row with the legacy instance id, so no historical
    task silently changes owner. It is idempotent and never deletes rows.
    """
    columns = {row[1] for row in db.execute('PRAGMA table_info(strategy_tasks)')}
    if columns and 'instance_id' not in columns:
        legacy = [row[1] for row in db.execute('PRAGMA table_info(strategy_tasks)')]
        kept = [name for name in legacy if name != 'sequence']
        db.execute('ALTER TABLE strategy_tasks RENAME TO strategy_tasks_legacy_v1')
        db.execute('CREATE TABLE strategy_tasks(sequence INTEGER PRIMARY KEY AUTOINCREMENT,'
                   + _TASK_COLUMNS_SQL + ')')
        db.execute('INSERT INTO strategy_tasks(instance_id,' + ','.join(kept) + ')'
                   ' SELECT ?, ' + ','.join(kept) + ' FROM strategy_tasks_legacy_v1',
                   (instance_id,))
        db.execute('DROP TABLE strategy_tasks_legacy_v1')
    else:
        db.execute('CREATE TABLE IF NOT EXISTS strategy_tasks('
                   'sequence INTEGER PRIMARY KEY AUTOINCREMENT,' + _TASK_COLUMNS_SQL + ')')
        present = {row[1] for row in db.execute('PRAGMA table_info(strategy_tasks)')}
        if 'checkpoint_absent' not in present:
            db.execute('ALTER TABLE strategy_tasks'
                       ' ADD COLUMN checkpoint_absent INTEGER NOT NULL DEFAULT 0')
    # Ordered FIFO selection and per-tick counting both scan by state.
    # Without this index every control tick walked rows whose payload
    # columns hold the entire event, request and decision.
    db.execute('CREATE INDEX IF NOT EXISTS strategy_tasks_state'
               ' ON strategy_tasks(instance_id,state,sequence)')


class DurableStrategyQueue:
    def __init__(self, signals, *, runner, source_fingerprint, max_pending=2048,
                 instance_id=LEGACY_INSTANCE_ID):
        if (not isinstance(source_fingerprint, str)
                or not re.fullmatch('[0-9a-f]{64}', source_fingerprint)):
            raise ValueError('STRATEGY_QUEUE_SOURCE_FINGERPRINT_REQUIRED')
        if type(max_pending) is not int or not 1 <= max_pending <= 10000:
            raise ValueError('STRATEGY_QUEUE_BOUND_INVALID')
        if (not isinstance(instance_id, str) or not instance_id
                or len(instance_id) > 128):
            raise ValueError('STRATEGY_QUEUE_INSTANCE_INVALID')
        if any(not callable(getattr(runner, method, None)) for method in ('start', 'poll', 'close')):
            raise ValueError('STRATEGY_QUEUE_RUNNER_REQUIRED')
        self.signals = signals; self.runner = runner; self.source_fingerprint = source_fingerprint
        self.instance_id = instance_id
        self.max_pending = max_pending; self.db = signals.book.db
        self.owner = threading.get_ident(); self._active = None; self._closed = False; self._closing = False
        self._config = self._current_config()
        with self._transaction():
            ensure_strategy_task_schema(self.db)
            self.recovered_running = self.db.execute(
                "UPDATE strategy_tasks SET state='QUEUED',recoveries=recoveries+1"
                " WHERE state='RUNNING' AND instance_id=?", (self.instance_id,)).rowcount

    def _owner(self):
        if threading.get_ident() != self.owner:
            raise ValueError('STRATEGY_QUEUE_OWNER_REQUIRED')

    @contextmanager
    def _transaction(self):
        self._owner()
        if self.db.in_transaction:
            raise ValueError('STRATEGY_QUEUE_REQUIRES_IDLE_TRANSACTION')
        self.signals.book._begin()
        try:
            yield
            self.db.commit()
        except BaseException:
            self.db.rollback(); raise

    def _now(self):
        return self.signals.book.clock().isoformat()

    def _current_config(self):
        # A generic host owns its own configuration identity; the legacy
        # composition layer has none and keeps the previous computation.
        fingerprint = getattr(self.signals, 'config_fingerprint', None)
        if callable(fingerprint):
            return fingerprint()
        identity = self.signals.identity
        return digest([identity.instance_id, identity.plugin_id, identity.version,
            str(self.signals.cost), self.signals.book.policy])

    def _row(self, event_id, *, columns=_COLUMNS):
        row = self.db.execute('SELECT '+','.join(columns)+' FROM strategy_tasks WHERE instance_id=? AND event_id=?', (self.instance_id,event_id)).fetchone()
        return dict(zip(columns, row)) if row else None

    def _light_row(self, event_id):
        return self._row(event_id, columns=_LIGHT_COLUMNS)

    def _runner_busy(self):
        # A warm child remains alive between requests. A pending deadline, not
        # process existence, identifies an outstanding advisory computation.
        return getattr(self.runner, 'next_deadline_monotonic', None) is not None

    def _consumed_fingerprint(self, event_id):
        """Durable proof that this instance already consumed ``event_id``."""
        lookup = getattr(self.signals, 'consumed_hash', None)
        if callable(lookup):
            return lookup(event_id)
        prior = self.db.execute('SELECT fingerprint FROM live_inputs WHERE id=?',
                                (event_id,)).fetchone()
        return prior[0] if prior else None

    def _consumed(self, row):
        prior = self._consumed_fingerprint(row['event_id'])
        if prior is not None and prior != row['event_hash']:
            raise ValueError('STRATEGY_EVENT_ID_CONFLICT')
        return prior is not None

    def _version_reason(self, row):
        if row['source_fingerprint'] != self.source_fingerprint:
            return 'STRATEGY_QUEUE_SOURCE_CHANGED'
        if row['config_fingerprint'] != self._config or self._current_config() != self._config:
            return 'STRATEGY_QUEUE_CONFIG_CHANGED'
        return None

    def enqueue(self, event):
        self._owner()
        if self._closed or self._closing:
            raise ValueError('STRATEGY_QUEUE_CLOSED')
        if self._current_config() != self._config:
            raise ValueError('STRATEGY_QUEUE_CONFIG_CHANGED')
        raw = _event_bytes(event); fingerprint = hashlib.sha256(raw).hexdigest(); identity = event['id']
        with self._transaction():
            previous = self._row(identity)
            if previous:
                if previous['event_hash'] != fingerprint:
                    raise ValueError('STRATEGY_EVENT_ID_CONFLICT')
                return {'event_id': identity, 'state': previous['state'], 'duplicate': True, 'enqueued': False}
            prior_hash = self._consumed_fingerprint(identity)
            if prior_hash is not None and prior_hash != fingerprint:
                raise ValueError('STRATEGY_EVENT_ID_CONFLICT')
            consumed = prior_hash is not None
            count = self.db.execute("SELECT count(*) FROM strategy_tasks"
                " WHERE instance_id=? AND state IN ('QUEUED','RUNNING')",
                (self.instance_id,)).fetchone()[0]
            if not consumed and count >= self.max_pending:
                raise ValueError('STRATEGY_QUEUE_CAPACITY_EXCEEDED')
            state = 'COMMITTED' if consumed else 'QUEUED'
            self.db.execute('''INSERT INTO strategy_tasks(instance_id,event_id,event_hash,event_payload,state,queued_at,
                source_fingerprint,config_fingerprint,finished_at,reason) VALUES(?,?,?,?,?,?,?,?,?,?)''',
                (self.instance_id, identity, fingerprint, raw, state, self._now(),
                 self.source_fingerprint, self._config,
                 self._now() if consumed else None, 'ALREADY_CONSUMED' if consumed else None))
        return {'event_id': identity, 'state': state, 'duplicate': bool(consumed), 'enqueued': not consumed}

    def _reject(self, row, reason):
        with self._transaction():
            self.db.execute("UPDATE strategy_tasks SET state='REJECTED',finished_at=?,reason=?"
                " WHERE instance_id=? AND event_id=?",
                (self._now(), reason, self.instance_id, row['event_id']))
        return {'event_id': row['event_id'], 'reason': reason}

    def _ack(self, row, signal=None, *, duplicate=False):
        result = {'event_id': row['event_id'], 'signal': signal, 'duplicate': duplicate}
        with self._transaction():
            self.db.execute("UPDATE strategy_tasks SET state='COMMITTED',finished_at=?,reason=?,result_json=?"
                " WHERE instance_id=? AND event_id=?",
                (self._now(), 'ALREADY_CONSUMED' if duplicate else 'COMMITTED', canonical(result),
                 self.instance_id, row['event_id']))
        return result

    def _prepare(self, row):
        raw = row['event_payload']
        if type(raw) is not bytes or len(raw) > MAX_BYTES or hashlib.sha256(raw).hexdigest() != row['event_hash']:
            raise ValueError('STRATEGY_QUEUE_EVENT_CORRUPT')
        event = json.loads(raw)
        if _event_bytes(event) != raw or event['id'] != row['event_id']:
            raise ValueError('STRATEGY_QUEUE_EVENT_CORRUPT')
        if row['request_payload'] is not None:
            encoded = row['request_payload']
            if (type(encoded) is not bytes or len(encoded) > MAX_BYTES
                    or hashlib.sha256(encoded).hexdigest() != row['request_hash']):
                raise ValueError('STRATEGY_QUEUE_REQUEST_CORRUPT')
            restore = getattr(self.signals, 'restore', None)
            if callable(restore):
                # A host that owns its own checkpoint identity and prepared type
                # must rebuild both; guessing them here is what made a valid
                # restart look like corruption.
                return restore(event, row), encoded
            request = decode_input(encoded)
            if (request.cursor != row['event_id'] or request.source_sha256 != row['event_hash']
                    or request.identity != self.signals.identity
                    or digest(thaw(request.checkpoint)) != row['checkpoint_hash']
                    or digest(thaw(request.market['event'])) != row['event_hash']):
                raise ValueError('STRATEGY_QUEUE_REQUEST_CORRUPT')
            now = self.signals.book.clock()
            if not 0 <= (now-timestamp(event['at'])).total_seconds() <= 30:
                raise ValueError('STALE_STRATEGY_EVENT')
            if request.as_of > now:
                raise ValueError('STRATEGY_DECISION_EXPIRED')
            return PreparedSignalInput(request, row['checkpoint_hash']), encoded
        prepared = self.signals.prepare(event)
        if prepared is None:
            return None, None
        return prepared, encode_input(prepared.request)

    @staticmethod
    def _append_rejection(output, rejected):
        output['rejected'].append(rejected)
        output['errors'].append(rejected)

    def _finish(self, row, decision, output):
        reason = self._version_reason(row)
        if reason:
            self._append_rejection(output, self._reject(row, reason))
            self._active = None
            return
        try:
            encoded = encode_decision(decision)
            with self._transaction():
                self.db.execute('UPDATE strategy_tasks SET decision_payload=?,decision_hash=?'
                    ' WHERE instance_id=? AND event_id=?',
                    (encoded, hashlib.sha256(encoded).hexdigest(), self.instance_id, row['event_id']))
            signal = self.signals.commit(self._active['prepared'], decision)
        except Exception as exc:
            # Inbox proof distinguishes commit/return failure from rejection.
            # Do not put ACK in this exception region: storage failure after a
            # successful commit must leave RUNNING for exact inbox repair.
            try:
                consumed = self._consumed(row)
            except ValueError:
                self._append_rejection(output, self._reject(row, 'STRATEGY_EVENT_ID_CONFLICT'))
            else:
                if consumed:
                    output['completed'].append(self._ack(row, duplicate=True))
                else:
                    self._append_rejection(output, self._reject(row,
                        _reason(exc, 'STRATEGY_TASK_COMMIT_REJECTED')))
        else:
            output['completed'].append(self._ack(row, signal))
        self._active = None

    def _start_next(self, output):
        raw = self.db.execute("SELECT event_id FROM strategy_tasks"
            " WHERE instance_id=? AND state='QUEUED' ORDER BY sequence LIMIT 1",
            (self.instance_id,)).fetchone()
        if not raw:
            return
        row = self._row(raw[0])
        try:
            consumed = self._consumed(row)
        except ValueError:
            self._append_rejection(output, self._reject(row, 'STRATEGY_EVENT_ID_CONFLICT'))
            return
        if consumed:
            output['completed'].append(self._ack(row, duplicate=True))
            return
        reason = self._version_reason(row)
        if reason:
            self._append_rejection(output, self._reject(row, reason))
            return
        try:
            prepared, encoded = self._prepare(row)
        except Exception as exc:
            self._append_rejection(output, self._reject(row,
                _reason(exc, 'STRATEGY_TASK_PREPARE_REJECTED')))
            return
        if prepared is None:
            output['completed'].append(self._ack(row, duplicate=True))
            return
        with self._transaction():
            self.db.execute("""UPDATE strategy_tasks SET state='RUNNING',request_payload=?,request_hash=?,
                checkpoint_hash=?,started_at=?,attempts=attempts+1,checkpoint_absent=?
                WHERE instance_id=? AND event_id=?""",
                (encoded, hashlib.sha256(encoded).hexdigest(), prepared.checkpoint_fingerprint,
                 self._now(),
                 1 if getattr(prepared, 'expected_state', 0) is None else 0,
                 self.instance_id, row['event_id']))
        self._active = {'event_id': row['event_id'], 'prepared': prepared}
        try:
            # Reuse the payload that was just persisted; the wire form of this
            # exact request already exists and re-encoding it doubled the most
            # expensive per-event operation in the boundary.
            self.runner.start(prepared.request, payload=encoded)
        except Exception as exc:
            self._append_rejection(output, self._reject(row, _reason(exc, 'STRATEGY_TASK_RUNNER_FAILED')))
            self._active = None
        else:
            output['started'] = row['event_id']

    def advance(self):
        self._owner()
        result = {'completed': [], 'rejected': [], 'errors': [], 'started': None, 'busy': False}
        if self._closed or self._closing:
            return {**result, 'closed': True, 'pending': self.status()['pending']}
        # One row fetch per advance: polling happens many times per task while a
        # child computes, so repeated lookups were pure per-tick overhead.
        row = self._light_row(self._active['event_id']) if self._active is not None else None
        if row is not None:
            try:
                consumed = self._consumed(row)
            except ValueError:
                self._append_rejection(result, self._reject(row, 'STRATEGY_EVENT_ID_CONFLICT'))
                self._active = None  # Remaining pure output will be drained only.
            else:
                if consumed:
                    result['completed'].append(self._ack(row, duplicate=True))
                    self._active = None
        # Exactly one runner.poll per advance. A retained obsolete result can
        # only be drained; it cannot reach the host commit path.
        if self._active is not None:
            try:
                decision = self.runner.poll()
            except Exception as exc:
                self._append_rejection(result, self._reject(row, _reason(exc, 'STRATEGY_TASK_RUNNER_FAILED')))
                self._active = None
            else:
                if decision is not None:
                    self._finish(row, decision, result)
        elif self._runner_busy():
            try:
                self.runner.poll()
            except Exception:
                result['errors'].append({'event_id': None, 'reason': 'STRATEGY_QUEUE_RUNNER_REAP_PENDING'})
        if self._active is None and not self._runner_busy():
            self._start_next(result)
        # Polling needs only the pending count and busy flag. Full status()
        # additionally digests the live configuration; that is not needed on
        # every control tick and was measurable per-poll overhead.
        pending = self._pending_count()
        result.update(busy=bool(pending) or self._active is not None or self._runner_busy(),
                      pending=pending)
        return result

    def _pending_count(self):
        row = self.db.execute("SELECT count(*) FROM strategy_tasks"
                              " WHERE instance_id=? AND state IN ('QUEUED','RUNNING')",
                              (self.instance_id,)).fetchone()
        return int(row[0]) if row else 0

    def status(self):
        self._owner()
        counts = dict(self.db.execute('SELECT state,count(*) FROM strategy_tasks'
                                      ' WHERE instance_id=? GROUP BY state', (self.instance_id,)))
        return {'pending': counts.get('QUEUED', 0)+counts.get('RUNNING', 0),
            'queued': counts.get('QUEUED', 0), 'running': counts.get('RUNNING', 0),
            'committed': counts.get('COMMITTED', 0), 'rejected': counts.get('REJECTED', 0),
            'busy': bool(counts.get('QUEUED', 0)+counts.get('RUNNING', 0)) or self._active is not None or self._runner_busy(), 'closed': self._closed,
            'max_pending': self.max_pending, 'active_event_id': self._active['event_id'] if self._active else None,
            'source_fingerprint': self.source_fingerprint, 'config_fingerprint': self._config,
            'config_changed': self._current_config() != self._config,
            'next_deadline_monotonic': getattr(self.runner, 'next_deadline_monotonic', None)}

    def close(self):
        self._owner()
        if self._closed:
            return self.status()
        self._closing = True
        self.runner.close()
        if self._active is not None:
            with self._transaction():
                self.db.execute("UPDATE strategy_tasks SET state='QUEUED'"
                    " WHERE instance_id=? AND event_id=? AND state='RUNNING'",
                    (self.instance_id, self._active['event_id']))
            self._active = None
        self._closed = True
        return self.status()
