"""Bounded deterministic execution driver, currently synthetic fixtures ONLY.

No real broker write transport is implemented or accepted here. In particular,
``simulation_only`` is a fixture contract, never a real-money authorization.
NativeOperations retains its original confirmation metadata unchanged.
"""
import json

from .audit import canonical, digest
from .control_tasks import worker_lease
from .native_operations import NativeOperations, sanitized


class ScriptExecutionDriver:
    """Consume existing core actions without a model or parallel risk engine.

    ``transport.execute_simulated(packet)`` must synchronously return one native
    response envelope and must set ``simulation_only = True``. Each step invokes
    at most one such operation. Callers supply a NEW observed snapshot on every
    step; receipts never substitute for broker observation. Transport timeouts
    must be bounded by the fixture implementation; this is not a live daemon.
    """

    def __init__(self, book, transport):
        self.book = book
        self.transport = transport
        # Deliberately do not instantiate NativeOperations, create tables, take
        # leases or reconcile anything until the strict synthetic gate passes.

    def _gate(self, snapshot):
        if (self.book.simulation is not True or self.book.state().get('simulation') is not True
                or not isinstance(snapshot, dict)
                or snapshot.get('synthetic') is not True):
            return {'status': 'BLOCKED_CAPABILITY', 'reason': 'REAL_SCRIPT_EXECUTION_CHANNEL_UNVERIFIED',
                    'transport_called': False, 'broker_effect_verified': False}
        if (getattr(self.transport, 'simulation_only', None) is not True
                or not callable(getattr(self.transport, 'execute_simulated', None))):
            return {'status': 'BLOCKED_CAPABILITY', 'reason': 'SYNTHETIC_FIXTURE_TRANSPORT_REQUIRED',
                    'transport_called': False, 'broker_effect_verified': False}
        return None

    def step(self, snapshot, quotes=None, *, allow_entries=False):
        prepared = self.prepare(snapshot, quotes, allow_entries=allow_entries)
        if prepared['status'] != 'PREPARED':
            return prepared
        try:
            try:
                response = self.transport.execute_simulated(prepared['packet'])
            except Exception:
                response = {'isError': True, 'content': [{'type': 'text', 'text': 'SYNTHETIC_TRANSPORT_OUTCOME_UNKNOWN'}]}
            return self.complete(prepared['packet'], response)
        finally:
            prepared['_lease'].close()

    def prepare(self, snapshot, quotes=None, *, allow_entries=False):
        """Owner-only reservation; successful caller MUST retain/release _lease."""
        blocked = self._gate(snapshot)
        if blocked:
            return blocked
        path = self.book.db.execute('PRAGMA database_list').fetchone()[2]
        if not path:
            return {'status': 'BLOCKED_OWNERSHIP', 'reason': 'FILE_BACKED_WORKER_LEASE_REQUIRED', 'transport_called': False}
        lease = worker_lease(path)
        if lease is None:
            return {'status': 'BLOCKED_OWNERSHIP', 'reason': 'WORKER_LEASE_HELD', 'transport_called': False}
        try:
            result = self._prepare(snapshot, quotes or {}, allow_entries=allow_entries)
            if result['status'] == 'PREPARED':
                result['_lease'] = lease
                return result
        except BaseException:
            lease.close()
            raise
        lease.close()
        return result

    def _prepare(self, snapshot, quotes, *, allow_entries):
        try:
            self.book.reconcile(snapshot, quotes=quotes)
        except (ValueError, KeyError, TypeError) as error:
            return {'status': 'BLOCKED_VALIDATION', 'reason': str(error), 'transport_called': False}
        actions = self.book.actions()
        if any(a['state'] in {'RUNNING', 'UNKNOWN'} for a in actions):
            return {'status': 'AWAITING_RECONCILIATION', 'transport_called': False}
        retired = self.book.recovery_status(snapshot)['acknowledged_action_ids']
        candidates = [a for a in actions if a['state'] in {'READY', 'WAITING_USER'}
                      and a['id'] not in retired and (allow_entries or a['kind'] != 'entry')]
        candidates.sort(key=lambda a: ({'protect': 0, 'cancel': 1, 'exit': 2, 'entry': 3}.get(a['kind'], 4),
                                      a['created_at'], a['id']))
        if not candidates:
            return {'status': 'IDLE', 'transport_called': False}
        action = candidates[0]
        payload = json.loads(action['payload'])
        if (payload.get('synthetic') is not True
                or payload.get('account') != '••••' + self.book.policy['account_suffix']
                or digest(payload.get('policy')) != digest(self.book.policy)
                or payload.get('action_id') != action['id']
                or payload.get('trade_id') != action['trade_id']
                or payload.get('kind') != action['kind']):
            return {'status': 'BLOCKED_VALIDATION', 'reason': 'SCRIPT_ACTION_BINDING_MISMATCH',
                    'action_id': action['id'], 'transport_called': False}
        quote = quotes.get(payload['plan']['symbol'])
        ops = NativeOperations(self.book)
        operation = 'cancel' if action['kind'] == 'cancel' else 'review'
        if action['state'] == 'WAITING_USER':
            row = self.book.db.execute("SELECT result FROM native_operation_records WHERE action_id=? AND operation='review' AND state='RECORDED' ORDER BY rowid DESC LIMIT 1", (action['id'],)).fetchone()
            review = json.loads(row[0]) if row else {}
            # These checks model an explicitly synthetic no-confirmation channel.
            # They do NOT override requires_native_confirmation in NativeOperations
            # and are unreachable for real books, even with a fake transport flag.
            if (not review.get('review_verified') or review.get('order_checks')
                    or review.get('customer_approval_required') is not False):
                return {'status': 'WAITING_USER', 'action_id': action['id'], 'transport_called': False}
            operation = 'place'
        try:
            packet = ops.begin(action['id'], operation, snapshot, quote=quote)
        except ValueError as error:
            return {'status': 'BLOCKED_VALIDATION', 'reason': str(error),
                    'action_id': action['id'], 'transport_called': False}
        return {'status': 'PREPARED', 'packet': packet, 'transport_called': False}

    def complete(self, packet, response):
        """Owner-only receipt import; tool return is still not a broker effect."""
        if self.book.simulation is not True or self.book.state().get('simulation') is not True:
            return {'status': 'BLOCKED_CAPABILITY', 'transport_called': False}
        if not self.book.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='native_operation_records'").fetchone():
            raise ValueError('SCRIPT_PACKET_NOT_PREPARED')
        stored = self.book.db.execute('SELECT payload FROM native_operation_records WHERE id=?',
                                      (packet['operation_id'],)).fetchone()
        if not stored or digest(json.loads(stored[0])) != digest(packet):
            raise ValueError('SCRIPT_PACKET_BINDING_MISMATCH')
        ops = NativeOperations(self.book)
        operation = packet['operation']
        redacted = sanitized(response)
        self.book.db.execute('''CREATE TABLE IF NOT EXISTS script_execution_receipts(
            operation_id TEXT PRIMARY KEY, received_at TEXT NOT NULL,
            response_sha256 TEXT NOT NULL, response TEXT NOT NULL)''')
        previous = self.book.db.execute('SELECT response_sha256 FROM script_execution_receipts WHERE operation_id=?',
                                        (packet['operation_id'],)).fetchone()
        if previous and previous[0] != digest(redacted):
            raise ValueError('SCRIPT_RECEIPT_CONFLICT')
        if not previous:
            self.book.db.execute('INSERT INTO script_execution_receipts VALUES(?,?,?,?)',
                                 (packet['operation_id'], self.book.clock().isoformat(), digest(redacted), canonical(redacted)))
        self.book.db.commit()
        result = ops.record(packet['operation_id'], response)
        status = result['report']['status']
        if operation == 'review':
            status = 'REVIEWED_SYNTHETIC' if (result.get('review_verified')
                and not result.get('order_checks') and result.get('customer_approval_required') is False) else 'WAITING_USER'
        return {'status': status, 'operation': operation, 'operation_id': packet['operation_id'],
                'action_id': packet['action_id'], 'transport_called': True,
                'broker_effect_verified': False, 'synthetic': True, 'result': result,
                'receipt_duplicate': bool(previous)}
