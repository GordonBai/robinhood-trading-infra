"""Read-only evidence linking a raw Host-submitted native response to its packet,
request, action and NativeOperations review record.

The returned origin is ``host_submitted_response``: this machine holds the
response body delivered by the Host.  It is not a broker signature, a user
confirmation, a fill, or proof of an active protection order.  Missing or
tampered raw receipts are rejected; a SQLite summary alone is never accepted.
"""
from datetime import datetime, timezone
import json
from pathlib import Path
import re

from trading_contracts.values import digest

from .host_mailbox import read_private_json
from .native_operations import native_data, response_identity
from .supervised_engineering import _time

RECEIPT_KEYS = frozenset({
    'schema_version', 'request_id', 'action_id', 'trade_id', 'operation_id',
    'binding', 'packet_sha256', 'response', 'response_sha256', 'received_at',
    'execution_authorized', 'broker_effect_verified'})
PACKET_KEYS = frozenset({
    'schema_version', 'status', 'request_id', 'binding', 'packet',
    'packet_sha256', 'prepared_at', 'expires_at', 'execution_authorized',
    'broker_effect_verified'})
BINDING_KEYS = frozenset({
    'db', 'account', 'account_key', 'policy_sha256', 'source_fingerprint',
    'epoch'})
TOOLS = {'review': 'review_equity_order', 'place': 'place_equity_order',
         'cancel': 'cancel_equity_order'}
TOOL = 'review_equity_order'
_HEX64 = re.compile(r'[0-9a-f]{64}\Z')
_SAFE_ID = re.compile(r'[A-Za-z0-9][A-Za-z0-9._:-]{0,199}\Z')


class NativeReceiptError(ValueError):
    def __init__(self, code):
        super().__init__(code)
        self.code = code


def _fail(code):
    raise NativeReceiptError(code)


def _private_json(path):
    try:
        info = path.stat()
        if info.st_mode & 0o077 or info.st_nlink != 1:
            _fail('NATIVE_RECEIPT_FILE_INVALID')
    except FileNotFoundError:
        _fail('NATIVE_RECEIPT_MISSING')
    except OSError:
        _fail('NATIVE_RECEIPT_UNAVAILABLE')
    try:
        value = read_private_json(path)
    except (OSError, ValueError, json.JSONDecodeError):
        _fail('NATIVE_RECEIPT_UNAVAILABLE')
    if not isinstance(value, dict):
        _fail('NATIVE_RECEIPT_SCHEMA')
    return value


class NativeReceiptEvidenceReader:
    """Fixed private Host directory reader; never writes or consumes permits."""

    def __init__(self, root, *, ingress_root=None, clock=None):
        self.root = Path(root).resolve()
        self.ingress_root = (Path(ingress_root).resolve()
                             if ingress_root is not None else None)
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def read(self, book, result):
        try:
            return self._read(book, result)
        except NativeReceiptError:
            raise
        except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
            _fail('NATIVE_RECEIPT_INVALID')

    def _file(self, folder, name):
        if (not isinstance(name, str) or not _SAFE_ID.fullmatch(name)
                or Path(name).name != name or '/' in name or '\\' in name
                or name in {'.', '..'}):
            _fail('NATIVE_RECEIPT_ID_INVALID')
        base = self.root / folder
        try:
            if base.is_symlink() or base.resolve().parent != self.root:
                _fail('NATIVE_RECEIPT_PATH_ESCAPE')
        except OSError:
            _fail('NATIVE_RECEIPT_PATH_ESCAPE')
        return _private_json(base / (name + '.json'))

    def _ingress(self, operation_id):
        if not isinstance(operation_id, str) or not _SAFE_ID.fullmatch(operation_id):
            _fail('NATIVE_INGRESS_ID_INVALID')
        try:
            if (self.ingress_root.is_symlink()
                    or self.ingress_root.resolve() != self.ingress_root):
                _fail('NATIVE_INGRESS_PATH_ESCAPE')
        except OSError:
            _fail('NATIVE_INGRESS_PATH_ESCAPE')
        return _private_json(self.ingress_root / (operation_id + '.json'))

    def _read(self, book, result):
        permit_wrapper = (result or {}).get('permit') or {}
        permit = (permit_wrapper.get('permit')
                  if isinstance(permit_wrapper.get('permit'), dict)
                  else permit_wrapper)
        action_id = permit.get('action_id')
        review = (result or {}).get('review') or {}
        operation_id = review.get('operation_id') or (
            permit.get('formal') or {}).get('review_operation_id')
        if (not isinstance(operation_id, str)
                or not _SAFE_ID.fullmatch(operation_id)):
            _fail('NATIVE_OPERATION_ID_MISSING')
        row = book.db.execute(
            'SELECT action_id,operation,payload,result,state,created_at '
            'FROM native_operation_records WHERE id=?', (operation_id,)).fetchone()
        if not row:
            _fail('NATIVE_OPERATION_RECORD_MISSING')
        row_action, operation, raw_packet, raw_result, state, created = row
        if (row_action != action_id or operation != 'review'
                or state != 'RECORDED'):
            _fail('NATIVE_OPERATION_RECORD_INVALID')
        receipt = self._file('receipts', operation_id)
        if set(receipt) != RECEIPT_KEYS:
            _fail('NATIVE_RECEIPT_SCHEMA')
        if (receipt.get('schema_version') != 1
                or receipt.get('operation_id') != operation_id
                or receipt.get('action_id') != action_id
                or receipt.get('execution_authorized') is not False
                or receipt.get('broker_effect_verified') is not False):
            _fail('NATIVE_RECEIPT_SCHEMA')
        response = receipt.get('response')
        if not isinstance(response, dict):
            _fail('NATIVE_RESPONSE_MISSING')
        if receipt.get('response_sha256') != digest(response):
            _fail('NATIVE_RESPONSE_DIGEST')
        request_id = receipt.get('request_id')
        if (not isinstance(request_id, str)
                or not _SAFE_ID.fullmatch(request_id)):
            _fail('NATIVE_RECEIPT_ID_INVALID')
        packet_envelope = self._file('packets', request_id)
        request_record = self._file('requests', request_id)
        if set(packet_envelope) != PACKET_KEYS:
            _fail('NATIVE_PACKET_SCHEMA')
        packet = packet_envelope.get('packet')
        binding = receipt.get('binding')
        if (not isinstance(binding, dict) or set(binding) != BINDING_KEYS
                or not isinstance(packet, dict)
                or packet_envelope.get('schema_version') != 1
                or packet_envelope.get('status') != 'PREPARED'
                or packet_envelope.get('request_id') != request_id
                or packet_envelope.get('binding') != binding
                or packet_envelope.get('packet_sha256') != digest(packet)
                or packet_envelope.get('execution_authorized') is not False
                or packet_envelope.get('broker_effect_verified') is not False
                or receipt.get('packet_sha256') != digest(packet)):
            _fail('NATIVE_PACKET_BINDING')
        if (packet.get('operation_id') != operation_id
                or packet.get('action_id') != action_id
                or packet.get('operation') != 'review'
                or packet.get('tool') != TOOLS['review']
                or packet.get('account') != binding.get('account')
                or packet.get('account_key') != binding.get('account_key')
                or request_record.get('operation') != 'review'):
            _fail('NATIVE_PACKET_OPERATION')
        parameters = packet.get('parameters')
        if not isinstance(parameters, dict):
            _fail('NATIVE_PACKET_ARGUMENTS')
        if (request_record.get('request_id') != request_id
                or request_record.get('action_id') != action_id
                or request_record.get('binding') != binding):
            _fail('NATIVE_REQUEST_BINDING')
        if (binding.get('account_key') != permit.get('account_key')
                or binding.get('account') !=
                '••••' + str(permit.get('account_suffix'))
                or binding.get('policy_sha256') !=
                (permit.get('source') or {}).get('policy_sha256')
                or binding.get('source_fingerprint') !=
                (permit.get('source') or {}).get('source_fingerprint')
                or Path(binding.get('db', '')).resolve() !=
                Path(book.db.execute(
                    'PRAGMA database_list').fetchone()[2]).resolve()):
            _fail('NATIVE_RECEIPT_ACCOUNT_BINDING')
        redacted, identity_hash = response_identity(response)
        try:
            stored_result = json.loads(raw_result)
            stored_packet = json.loads(raw_packet)
        except (TypeError, ValueError):
            _fail('NATIVE_OPERATION_RECORD_INVALID')
        if (not isinstance(stored_result, dict)
                or stored_result.get('operation_id') != operation_id
                or stored_result.get('action_id') != action_id
                or stored_result.get('response_hash') != identity_hash):
            _fail('NATIVE_RESULT_RESPONSE_MISMATCH')
        if (stored_result.get('review_verified') is not True
                or stored_result.get('response_usable') is not True
                or stored_packet != packet
                or stored_packet.get('operation') != 'review'
                or stored_packet.get('tool') != TOOLS['review']
                or stored_packet.get('operation_id') != operation_id
                or stored_packet.get('action_id') != action_id):
            _fail('NATIVE_REVIEW_RESULT_MISMATCH')
        try:
            data = native_data(response)
        except (ValueError, KeyError, TypeError):
            _fail('NATIVE_RESPONSE_SCHEMA')
        if (data.get('order_checks') != stored_result.get('order_checks')
                or data.get('market_data_disclosure') !=
                stored_result.get('market_data_disclosure')
                or data.get('customer_approval_required') !=
                stored_result.get('customer_approval_required')):
            _fail('NATIVE_REVIEW_RESULT_MISMATCH')
        try:
            received = _time(receipt.get('received_at'), 'NATIVE_RECEIPT_TIME')
            prepared = _time(packet_envelope.get('prepared_at'),
                             'NATIVE_RECEIPT_TIME')
            expires = _time(packet_envelope.get('expires_at'),
                            'NATIVE_RECEIPT_TIME')
            created_at = _time(created, 'NATIVE_RECEIPT_TIME')
            row_result_at = _time(stored_result.get('recorded_at'),
                                  'NATIVE_RECEIPT_TIME')
        except Exception:
            _fail('NATIVE_RECEIPT_TIME_INVALID')
        now = self.clock().astimezone(timezone.utc)
        if (created_at > prepared or prepared > received
                or received > row_result_at or row_result_at > now
                or received > expires
                or digest(parameters) != (permit.get('formal') or {}).get(
                    'review_parameters_sha256')):
            _fail('NATIVE_RECEIPT_TIME_OR_ARGUMENTS')
        origin = 'host_submitted_response'
        if self.ingress_root is not None:
            ingress = self._ingress(operation_id)
            if (ingress.get('schema_version') != 1
                    or ingress.get('kind') != 'formal_review_ingress_v1'
                    or ingress.get('status') != 'received'
                    or ingress.get('origin') not in {
                        'formal_runtime_transport',
                        'test_scripted_transport'}
                    or ingress.get('operation_id') != operation_id
                    or ingress.get('action_id') != action_id
                    or ingress.get('tool') != TOOL
                    or ingress.get('account_key') != binding.get('account_key')
                    or ingress.get('account') != binding.get('account')
                    or ingress.get('ref_id') != packet.get('logical_ref_id')
                    or ingress.get('packet_sha256') != digest(packet)
                    or ingress.get('parameters_sha256') != digest(parameters)
                    or ingress.get('response_sha256') !=
                    receipt.get('response_sha256')
                    or ingress.get('response_identity_sha256') != identity_hash
                    or ingress.get('host_receipt_sha256') != digest(receipt)
                    or ingress.get('received_at') != receipt.get('received_at')
                    or not isinstance(ingress.get('call_id'), str)
                    or not _SAFE_ID.fullmatch(ingress['call_id'])
                    or not _HEX64.fullmatch(str(ingress.get('descriptor_sha256')))):
                _fail('NATIVE_INGRESS_BINDING')
            origin = ingress['origin']
            if origin == 'formal_runtime_transport':
                call_path = self.ingress_root / (
                    operation_id + '.call.json')
                try:
                    call = _private_json(call_path)
                except NativeReceiptError:
                    _fail('NATIVE_INGRESS_CALL_RECORD_MISSING')
                if (ingress.get('call_record_sha256') != digest(call)
                        or call.get('status') != 'received'
                        or call.get('tool') != TOOL
                        or call.get('descriptor_sha256') !=
                        ingress.get('descriptor_sha256')
                        or call.get('rpc_request_id') != ingress.get('call_id')
                        or call.get('parameters_sha256') !=
                        ingress.get('parameters_sha256')
                        or call.get('response_sha256') !=
                        ingress.get('response_sha256')
                        or call.get('response_identity_sha256') !=
                        ingress.get('response_identity_sha256')
                        or call.get('started_at') !=
                        ingress.get('call_started_at')
                        or call.get('ended_at') !=
                        ingress.get('call_ended_at')
                        or call.get('account_scope_sha256') !=
                        binding.get('account_key')
                        or not isinstance(call.get('server'), str)
                        or not call.get('server')
                        or not isinstance(call.get('thread_id'), str)
                        or not call.get('thread_id')
                        or not isinstance(call.get('connection_id'), str)
                        or not call.get('connection_id')
                        or not _HEX64.fullmatch(str(call.get('arguments_sha256')))
                        or not _HEX64.fullmatch(str(call.get('parameters_sha256')))):
                    _fail('NATIVE_INGRESS_CALL_RECORD_MISMATCH')
                try:
                    call_started = _time(call.get('started_at'),
                                         'NATIVE_INGRESS_TIME')
                    call_ended = _time(call.get('ended_at'),
                                       'NATIVE_INGRESS_TIME')
                except Exception:
                    _fail('NATIVE_INGRESS_TIME_INVALID')
                if (call_started < prepared or call_ended < call_started
                        or call_ended > received
                        or call_ended > row_result_at
                        or call_ended > now):
                    _fail('NATIVE_INGRESS_TIME_INVALID')
        return {
            'status': 'bound',
            'origin': origin,
            'receipt_sha256': digest(receipt),
            'response_sha256': receipt['response_sha256'],
            'response_identity_sha256': identity_hash,
            'operation_id': operation_id,
            'request_id': request_id,
            'action_id': action_id,
            'parameters_sha256': digest(parameters),
            'record_state': state,
            'native_status': stored_result.get('report', {}).get('status'),
            'native_confirmation_required': stored_result.get(
                'requires_native_confirmation') is True,
            'broker_effect_verified': False,
            'raw_receipt_present': True,
        }
