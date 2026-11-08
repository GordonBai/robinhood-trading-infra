"""Non-model synthetic dispatcher: SQLite owner and transport I/O stay separate.

This is a simulation fixture facility, not an authenticated execution channel.
Stopped or slow work is never evidence that an intended write did not happen.
"""
from copy import deepcopy
from datetime import datetime, timezone
import os
from pathlib import Path
import threading

from .audit import canonical, digest
from .control_tasks import BackgroundTask
from .native_operations import sanitized
from .script_execution import ScriptExecutionDriver


def _execute_and_preserve(transport, packet, binding, directory, lease):
    """Background-only I/O; receives detached data and no SQLite/book handle."""
    try:
        try:
            response = transport.execute_simulated(deepcopy(packet))
        except Exception:
            response = {'isError': True, 'content': [{'type': 'text', 'text': 'SYNTHETIC_TRANSPORT_OUTCOME_UNKNOWN'}]}
        receipt = {'binding': binding, 'packet': packet,
                   'packet_sha256': digest(packet), 'response': sanitized(response),
                   'received_at': datetime.now(timezone.utc).isoformat(), 'synthetic': True}
        folder = Path(directory)
        folder.mkdir(parents=True, exist_ok=True)
        folder.chmod(0o700)
        path = folder / (packet['operation_id'] + '.json')
        temporary = path.with_suffix('.tmp')
        with temporary.open('x', encoding='utf-8') as handle:
            os.chmod(temporary, 0o600)
            handle.write(canonical(receipt))
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
        descriptor = os.open(folder, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        return {'packet': packet, 'response': response, 'receipt_file': str(path),
                'binding': binding, 'packet_sha256': digest(packet)}
    finally:
        lease.close()


class AsyncScriptExecutionDriver:
    """Owner calls step repeatedly, supplying a fresh broker snapshot each time.

    At most one operation is in flight. Transport latency does not stop owner
    reconciliation. ``stop`` prevents new submissions but does not kill Python
    threads, release another worker's lease or erase possible write effects.
    Persisted receipts permit explicit forensic recovery after owner failure;
    they are never auto-replayed as authorization for another write.
    """
    def __init__(self, book, transport, receipt_dir):
        self.driver = ScriptExecutionDriver(book, transport)
        self.book = book
        self.transport = transport
        self.receipt_dir = receipt_dir
        self.owner = threading.get_ident()
        self.worker = BackgroundTask('synthetic-script-execution')
        self.stopped = False
        self.pending = None

    def _owner(self):
        if threading.get_ident() != self.owner:
            raise ValueError('SCRIPT_EXECUTION_OWNER_THREAD_REQUIRED')

    def status(self):
        return {**self.worker.status(), 'stopped': self.stopped,
                'operation_id': self.pending['operation_id'] if self.pending else None,
                'automatic_resubmission_permitted': False, 'synthetic_only': True}

    def stop(self):
        self._owner()
        self.stopped = True
        return {'status': 'STOPPING_AWAITING_RECEIPT' if self.pending else 'STOPPED', **self.status()}

    def step(self, snapshot, quotes=None, *, allow_entries=False):
        # Real books/snapshots/transports never acquire a lease, create receipts,
        # reconcile a book, or instantiate NativeOperations.
        self._owner()
        blocked = self.driver._gate(snapshot)
        if blocked:
            return blocked
        quotes = quotes or {}
        if self.pending:
            ready = self.worker.take()
            if ready is not None:
                packet = self.pending
                self.pending = None
                ok, value = ready
                if not ok:
                    # Even a local fsync failure after I/O must record UNKNOWN.
                    value = {'packet': packet, 'response': {'isError': True, 'content': [
                        {'type': 'text', 'text': 'SYNTHETIC_RECEIPT_PERSISTENCE_FAILED_OUTCOME_UNKNOWN'}]}}
                result = self.driver.complete(value['packet'], value['response'])
                try:
                    self.book.reconcile(snapshot, quotes=quotes)
                except (ValueError, KeyError, TypeError) as error:
                    result['reconciliation_error'] = str(error)
                if ok:
                    result['receipt_file'] = value['receipt_file']
                return result
            try:
                self.book.reconcile(snapshot, quotes=quotes)
            except (ValueError, KeyError, TypeError) as error:
                return {'status': 'BLOCKED_VALIDATION', 'reason': str(error),
                        'inflight': True, 'transport_called': False}
            return {'status': 'IN_FLIGHT', 'inflight': True, 'transport_called': False}
        if self.stopped:
            try:
                self.book.reconcile(snapshot, quotes=quotes)
            except (ValueError, KeyError, TypeError) as error:
                return {'status': 'BLOCKED_VALIDATION', 'reason': str(error), 'transport_called': False}
            return {'status': 'STOPPED', 'transport_called': False}
        prepared = self.driver.prepare(snapshot, quotes, allow_entries=allow_entries)
        if prepared['status'] != 'PREPARED':
            return prepared
        packet = deepcopy(prepared['packet'])
        binding = {'db': str(Path(self.book.db.execute('PRAGMA database_list').fetchone()[2]).resolve()),
                   'account_key': self.book.state()['account_key'], 'policy_sha256': digest(self.book.policy)}
        self.pending = packet
        try:
            self.worker.start(_execute_and_preserve, self.transport, packet, binding,
                              self.receipt_dir, prepared['_lease'])
        except BaseException:
            prepared['_lease'].close()
            self.pending = None
            self.driver.complete(packet, {'isError': True, 'content': [
                {'type': 'text', 'text': 'SYNTHETIC_WORKER_START_OUTCOME_UNKNOWN'}]})
            raise
        return {'status': 'STARTED_SYNTHETIC', 'operation_id': packet['operation_id'],
                'action_id': packet['action_id'], 'operation': packet['operation'],
                'transport_called': False, 'inflight': True, 'broker_effect_verified': False}
