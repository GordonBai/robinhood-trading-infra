"""Owner-audited provenance and risk-lineage readers for engineering dry checks.

This module reads private owner records, the local Host audit trail and the
existing session-lineage risk registry.  It does not consume permits, grant
entry, write risk state, or replace the official execution channel.  A receipt
proves local owner intake integrity; it is not a broker or user authentication.
"""
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import sqlite3

from trading_contracts.values import digest

from .host_mailbox import read_private_json, write_private_json
from .qualification import source_fingerprint
from .supervised_engineering import SupervisedEngineeringError, _time

RECEIPT_ROOT = 'engineering-intent-receipts'
RECEIPT_KIND = 'engineering_owner_intent_receipt_v1'
REQUEST_FILE = 'supervised-engineering-request.json'
RISK_SOURCE = 'owner-risk-lineage-v1'
RISK_MAX_AGE_SECONDS = 30
RECEIPT_KEYS = frozenset({
    'schema_version', 'kind', 'request_id', 'host_request_id',
    'request_sha256', 'host_request_sha256', 'signal_id', 'action_id', 'ref_id',
    'account_key', 'account_suffix', 'purpose', 'policy_sha256',
    'source_fingerprint', 'received_at', 'audit_event_id', 'host_binding'})
_HOST_BINDING_KEYS = frozenset({
    'db', 'account', 'account_key', 'policy_sha256', 'source_fingerprint',
    'epoch'})
_ID = re.compile(r'[A-Za-z0-9][A-Za-z0-9._:-]{0,199}\Z')
_HEX64 = re.compile(r'[0-9a-f]{64}\Z')


class OwnerSourceError(ValueError):
    def __init__(self, code):
        super().__init__(code)
        self.code = code


def _fail(code):
    raise OwnerSourceError(code)


def _check_id(value, code):
    if not isinstance(value, str) or not _ID.fullmatch(value):
        _fail(code)
    return value


class EngineeringOwnerSource:
    """Private receipt writer/reader and risk-lineage reader."""

    def __init__(self, root, *, clock=None):
        self.root = Path(root)
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def _receipt_path(self, request_id):
        return self.root / RECEIPT_ROOT / (_check_id(
            request_id, 'OWNER_RECEIPT_REQUEST_ID') + '.json')

    def build_receipt(self, request, *, host_request_id, host_request_sha256,
                      signal_id, action, host_binding, audit_event_id,
                      received_at):
        """Build the immutable receipt from an already-committed request snapshot.

        The caller must pass the exact request dictionary that was validated
        when the Host core intent was committed.  This method never rereads the
        owner file, so a later file edit cannot renew an old audit.
        """
        if not isinstance(request, dict):
            _fail('OWNER_RECEIPT_REQUEST_INVALID')
        request_id = request.get('request_id')
        if (not isinstance(request_id, str) or not _ID.fullmatch(request_id)
                or request.get('signal_id') != signal_id):
            _fail('OWNER_RECEIPT_REQUEST_BINDING')
        _check_id(host_request_id, 'OWNER_RECEIPT_HOST_REQUEST_ID')
        request_sha = digest(request)
        try:
            payload = json.loads(action['payload'])
        except (KeyError, TypeError, ValueError):
            _fail('OWNER_RECEIPT_ACTION_INVALID')
        if (not isinstance(host_binding, dict)
                or set(host_binding) != _HOST_BINDING_KEYS):
            _fail('OWNER_RECEIPT_HOST_BINDING')
        receipt = {
            'schema_version': 1, 'kind': RECEIPT_KIND,
            'request_id': request_id, 'host_request_id': host_request_id,
            'request_sha256': request_sha,
            'host_request_sha256': host_request_sha256,
            'signal_id': signal_id, 'action_id': action['id'],
            'ref_id': payload.get('ref_id'),
            'account_key': request.get('account_key'),
            'account_suffix': request.get('account_suffix'),
            'purpose': request.get('purpose'),
            'policy_sha256': (request.get('source') or {}).get('policy_sha256'),
            'source_fingerprint':
                (request.get('source') or {}).get('source_fingerprint'),
            'received_at': received_at.isoformat(),
            'audit_event_id': audit_event_id,
            'host_binding': dict(host_binding)}
        for key in ('request_sha256', 'host_request_sha256', 'policy_sha256',
                    'source_fingerprint'):
            if not isinstance(receipt.get(key), str) or not _HEX64.fullmatch(
                    receipt[key]):
                _fail('OWNER_RECEIPT_HASH_INVALID:' + key)
        if not isinstance(receipt.get('ref_id'), str) or not receipt['ref_id']:
            _fail('OWNER_RECEIPT_REF_INVALID')
        return receipt

    def existing_receipt(self, request):
        """Return an existing receipt for this owner request, if any."""
        if not isinstance(request, dict):
            _fail('OWNER_RECEIPT_REQUEST_INVALID')
        path = self._receipt_path(request.get('request_id'))
        try:
            receipt = read_private_json(path)
        except FileNotFoundError:
            return None
        except (OSError, ValueError, json.JSONDecodeError):
            _fail('OWNER_RECEIPT_UNAVAILABLE')
        if not isinstance(receipt, dict) or set(receipt) != RECEIPT_KEYS:
            _fail('OWNER_RECEIPT_SCHEMA')
        if receipt.get('request_sha256') != digest(request):
            _fail('OWNER_RECEIPT_CONFLICT')
        return receipt

    def write_receipt(self, receipt):
        """Idempotently publish one immutable receipt; conflicts are rejected."""
        if not isinstance(receipt, dict) or set(receipt) != RECEIPT_KEYS:
            _fail('OWNER_RECEIPT_SCHEMA')
        request_id = receipt.get('request_id')
        _check_id(request_id, 'OWNER_RECEIPT_REQUEST_ID')
        path = self._receipt_path(request_id)
        try:
            existing = read_private_json(path)
        except FileNotFoundError:
            existing = None
        except (OSError, ValueError, json.JSONDecodeError):
            _fail('OWNER_RECEIPT_UNAVAILABLE')
        if existing is not None:
            if existing != receipt:
                _fail('OWNER_RECEIPT_CONFLICT')
            return existing
        write_private_json(path, receipt)
        return receipt

    def record_intent(self, book, *, host_request_id, host_request_sha256,
                      signal_id, action, host_binding, audit_event_id,
                      received_at=None):
        """Build from the current owner file, for explicit owner-side calls.

        Host recovery must use ``build_receipt`` with the original committed
        request snapshot and ``write_receipt``; it must not call this method.
        """
        request = read_private_json(self.root / REQUEST_FILE)
        receipt = self.build_receipt(
            request, host_request_id=host_request_id,
            host_request_sha256=host_request_sha256, signal_id=signal_id,
            action=action, host_binding=host_binding,
            audit_event_id=audit_event_id,
            received_at=received_at or self.clock())
        row = book.db.execute(
            'SELECT payload FROM audit_events WHERE event_id=?',
            (audit_event_id,)).fetchone()
        if row:
            try:
                original = json.loads(row[0])['details'].get('owner_receipt')
            except (TypeError, ValueError, KeyError):
                original = None
            if not isinstance(original, dict) or original != receipt:
                _fail('OWNER_RECEIPT_AUDIT_BINDING')
        return self.write_receipt(receipt)

    def provenance_reader(self, book, result):
        """Read the owner receipt and bind it to the exact permit/action/audit."""
        permit_wrapper = (result or {}).get('permit') or {}
        permit = (permit_wrapper.get('permit')
                  if isinstance(permit_wrapper.get('permit'), dict)
                  else permit_wrapper)
        request = (result or {}).get('request') or {}
        request_id = request.get('request_id')
        permit_sha = permit.get('request_sha256') or request.get('request_sha256')
        path = self._receipt_path(request_id)
        try:
            info = path.stat()
            if info.st_mode & 0o077 or info.st_nlink != 1:
                _fail('OWNER_RECEIPT_FILE_INVALID')
        except FileNotFoundError:
            pass
        try:
            receipt = read_private_json(path)
        except FileNotFoundError:
            _fail('OWNER_RECEIPT_MISSING')
        except (OSError, ValueError, json.JSONDecodeError):
            _fail('OWNER_RECEIPT_UNAVAILABLE')
        if not isinstance(receipt, dict) or set(receipt) != RECEIPT_KEYS:
            _fail('OWNER_RECEIPT_SCHEMA')
        if (receipt.get('schema_version') != 1
                or receipt.get('kind') != RECEIPT_KIND
                or receipt.get('request_id') != request_id
                or receipt.get('request_sha256') != permit_sha):
            _fail('OWNER_RECEIPT_BINDING')
        for key in ('request_sha256', 'host_request_sha256',
                    'policy_sha256', 'source_fingerprint'):
            if (not isinstance(receipt.get(key), str)
                    or not _HEX64.fullmatch(receipt[key])):
                _fail('OWNER_RECEIPT_SCHEMA')
        for key in ('host_request_id', 'signal_id', 'action_id',
                    'audit_event_id'):
            _check_id(receipt.get(key), 'OWNER_RECEIPT_SCHEMA')
        if (receipt.get('account_suffix') != permit.get('account_suffix')
                or receipt.get('purpose') != 'engineering'):
            _fail('OWNER_RECEIPT_SCHEMA')
        action_id = permit.get('action_id')
        action = next((item for item in book.actions()
                       if item.get('id') == action_id), None)
        if action is None:
            _fail('OWNER_RECEIPT_ACTION_MISSING')
        try:
            payload = json.loads(action.get('payload'))
        except (TypeError, ValueError):
            _fail('OWNER_RECEIPT_ACTION_INVALID')
        source = permit.get('source') or {}
        if (receipt.get('action_id') != action_id
                or receipt.get('ref_id') != payload.get('ref_id')
                or receipt.get('account_key') != permit.get('account_key')
                or receipt.get('account_suffix') != permit.get('account_suffix')
                or receipt.get('purpose') != permit.get('purpose')
                or receipt.get('policy_sha256') != source.get('policy_sha256')
                or receipt.get('source_fingerprint') !=
                source.get('source_fingerprint')):
            _fail('OWNER_RECEIPT_BINDING')
        binding = receipt.get('host_binding')
        if not isinstance(binding, dict) or set(binding) != _HOST_BINDING_KEYS:
            _fail('OWNER_RECEIPT_HOST_BINDING')
        try:
            current_db = Path(book.db.execute(
                'PRAGMA database_list').fetchone()[2]).resolve()
            bound_db = Path(binding.get('db', '')).resolve()
        except (OSError, TypeError, ValueError):
            _fail('OWNER_RECEIPT_HOST_BINDING')
        if (not Path(binding.get('db', '')).is_absolute()
                or bound_db != current_db
                or binding.get('account') !=
                '••••' + str(permit.get('account_suffix'))
                or binding.get('account_key') != permit.get('account_key')
                or binding.get('policy_sha256') !=
                source.get('policy_sha256')
                or binding.get('source_fingerprint') !=
                source.get('source_fingerprint')
                or not isinstance(binding.get('epoch'), str)
                or not binding.get('epoch')):
            _fail('OWNER_RECEIPT_HOST_BINDING')
        row = book.db.execute(
            'SELECT payload FROM audit_events WHERE event_id=?',
            (receipt.get('audit_event_id'),)).fetchone()
        if not row:
            _fail('OWNER_RECEIPT_AUDIT_MISSING')
        try:
            event = json.loads(row[0])
            details = event['details']
            host_result = details['host_result']
        except (TypeError, ValueError, KeyError):
            _fail('OWNER_RECEIPT_AUDIT_INVALID')
        original = details.get('owner_receipt')
        if not isinstance(original, dict):
            _fail('OWNER_RECEIPT_AUDIT_LEGACY_UNVERIFIABLE')
        if (original != receipt
                or receipt.get('audit_event_id') !=
                'host-engineering-intent-' + receipt.get('host_request_id')):
            _fail('OWNER_RECEIPT_AUDIT_BINDING')
        if (details.get('host_request_sha256') !=
                receipt.get('host_request_sha256')
                or host_result.get('action_id') != action_id
                or host_result.get('signal_id') != receipt.get('signal_id')
                or event.get('event_type') != 'RECONCILIATION_ISSUE'
                or event.get('synthetic') is not book.simulation):
            _fail('OWNER_RECEIPT_AUDIT_BINDING')
        try:
            received = _time(receipt.get('received_at'),
                             'OWNER_RECEIPT_TIME')
            event_at = _time(event.get('occurred_at'),
                             'OWNER_RECEIPT_TIME')
            requested = _time((request.get('request') or {}).get('requested_at'),
                              'OWNER_RECEIPT_TIME')
            valid_until = _time((request.get('request') or {}).get('valid_until'),
                                'OWNER_RECEIPT_TIME')
        except (SupervisedEngineeringError, AttributeError):
            _fail('OWNER_RECEIPT_TIME_INVALID')
        now = self.clock().astimezone(timezone.utc)
        if (received > now or received != event_at
                or received < requested or received > valid_until):
            _fail('OWNER_RECEIPT_TIME_INVALID')
        return {'kind': 'host_owner_request', 'record': receipt,
                'record_sha256': digest(receipt)}

    def risk_lineage_reader(self, book, result):
        """Read registered lineage/head and the current live risk state."""
        from . import session_lineage
        if book.simulation:
            _fail('OWNER_RISK_REAL_BOOK_REQUIRED')
        policy = book.policy or {}
        if policy.get('capture_only') is True:
            _fail('CAPTURE_ONLY_POLICY_READ_ONLY')
        path = session_lineage._book_paths(book)
        risk_path = session_lineage._risk_path(policy)
        try:
            with session_lineage._readonly(risk_path) as db:
                records, risk, head = session_lineage._read_registry(
                    db, policy)
        except ValueError as exc:
            _fail(str(exc))
        except sqlite3.OperationalError as exc:
            if 'no such table' in str(exc):
                _fail('LINEAGE_REGISTRY_UNINITIALIZED')
            _fail('OWNER_RISK_REGISTRY_UNAVAILABLE')
        except (OSError, sqlite3.Error, KeyError, TypeError):
            _fail('OWNER_RISK_REGISTRY_UNAVAILABLE')
        gate = session_lineage.lineage_entry_gate(book)
        if gate.get('blocked') is not False:
            _fail(str(gate.get('reason') or 'LINEAGE_GATE_BLOCKED'))
        if (gate.get('registry_head_hash') != head[1]
                or gate.get('registry_sequence') != head[0]
                or gate.get('registered_sessions') != len(records)):
            _fail('OWNER_RISK_REGISTRY_CHANGED')
        current = next((record for record in records
                        if record['db'] == str(path)), None)
        if current is None or records[-1]['db'] != current['db']:
            _fail('OWNER_RISK_CURRENT_SESSION_NOT_REGISTERED')
        state = book.risk_guard.state()
        if not isinstance(state, dict) or not state:
            _fail('OWNER_RISK_STATE_MISSING')
        permit_wrapper = (result or {}).get('permit') or {}
        permit = (permit_wrapper.get('permit')
                  if isinstance(permit_wrapper.get('permit'), dict)
                  else permit_wrapper)
        if (state.get('account_key') != permit.get('account_key')
                or state.get('terms') != risk.get('terms')
                or state.get('simulation') is not False):
            _fail('OWNER_RISK_BINDING')
        if (state.get('blocked') is True or state.get('reasons')
                or state.get('project_halted') is True
                or state.get('daily_halted') is True):
            _fail('OWNER_RISK_LOCKED')
        try:
            observed = _time(state.get('at'), 'OWNER_RISK_TIME')
        except SupervisedEngineeringError:
            _fail('OWNER_RISK_TIME_INVALID')
        now = self.clock().astimezone(timezone.utc)
        if observed > now or (now - observed).total_seconds() > \
                RISK_MAX_AGE_SECONDS:
            _fail('OWNER_RISK_STALE')
        return {
            'status': 'bound', 'lineage_id': head[1],
            'risk_state_sha256': digest(state),
            'account_key': state['account_key'], 'purpose': 'engineering',
            'source': RISK_SOURCE,
            'observed_at': observed.isoformat(),
            'valid_until': (observed + timedelta(
                seconds=RISK_MAX_AGE_SECONDS)).isoformat()}
