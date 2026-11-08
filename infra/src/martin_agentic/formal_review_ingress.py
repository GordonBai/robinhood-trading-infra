"""Default-disabled formal review transport ingress.

Only ``review_equity_order`` crosses this boundary.  Formal origin is accepted
only from the controlled ``EquityReviewTransport`` class with a real
``StdioRpc`` app-server process and a received call record produced by that
transport boundary.  Caller objects, injected RPCs and scripted transports
remain explicitly non-formal.

An ingress/call record is not a broker signature, user confirmation, fill,
protection or flat proof.  No place/cancel/approval/auth path exists here.
"""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re

from trading_contracts.values import digest

from .host_mailbox import read_private_json, submit_receipt, write_private_json
from .native_operations import response_identity
from .native_review import EquityReviewTransport
from .transport import StdioRpc, safe_read_failure

INGRESS_KIND = 'formal_review_ingress_v1'
TOOL = 'review_equity_order'
_SAFE_ID = re.compile(r'[A-Za-z0-9][A-Za-z0-9._:-]{0,199}\Z')


class FormalReviewIngressError(ValueError):
    def __init__(self, code):
        super().__init__(code)
        self.code = code


def _fail(code):
    raise FormalReviewIngressError(code)


class FormalReviewIngress:
    def __init__(self, root, *, clock=None):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.root.chmod(0o700)
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def _path(self, operation_id):
        if (not isinstance(operation_id, str)
                or not _SAFE_ID.fullmatch(operation_id)):
            _fail('FORMAL_INGRESS_OPERATION_ID_INVALID')
        return self.root / (operation_id + '.json')

    def _claim(self, packet):
        path = self.root / (packet['operation_id'] + '.claim')
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL
                         | os.O_NOFOLLOW, 0o600)
        except FileExistsError:
            _fail('FORMAL_INGRESS_ALREADY_CLAIMED')
        try:
            os.write(fd, json.dumps({
                'operation_id': packet['operation_id'],
                'packet_sha256': digest(packet),
                'claimed_at': self.clock().astimezone(
                    timezone.utc).isoformat()}, sort_keys=True).encode())
            os.fsync(fd)
        finally:
            os.close(fd)

    def _formal(self, transport):
        if getattr(transport, 'simulation_only', None) is True:
            return None
        if type(transport) is not EquityReviewTransport:
            _fail('FORMAL_INGRESS_TRANSPORT_NOT_FORMAL')
        if type(getattr(transport, 'rpc', None)) is not StdioRpc:
            _fail('FORMAL_INGRESS_RPC_NOT_FORMAL')
        if (not isinstance(transport.thread_id, str) or not transport.thread_id
                or TOOL not in transport.tools
                or TOOL not in transport.tool_descriptors):
            _fail('FORMAL_INGRESS_TOOL_DESCRIPTOR_UNAVAILABLE')
        return digest(transport.tool_descriptors[TOOL])

    def _aware(self, value):
        try:
            parsed = datetime.fromisoformat(value)
        except (TypeError, ValueError):
            raise
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError('NAIVE_TIME')
        return parsed

    def _host_packet(self, packet, packet_envelope, *, check_time=True):
        if (not isinstance(packet_envelope, dict)
                or packet_envelope.get('schema_version') != 1
                or packet_envelope.get('status') != 'PREPARED'
                or packet_envelope.get('packet_sha256') != digest(packet)
                or packet_envelope.get('execution_authorized') is not False
                or packet_envelope.get('broker_effect_verified') is not False):
            _fail('FORMAL_INGRESS_HOST_PACKET_INVALID')
        try:
            prepared = self._aware(packet_envelope['prepared_at'])
            expires = self._aware(packet_envelope['expires_at'])
        except (KeyError, TypeError, ValueError):
            _fail('FORMAL_INGRESS_HOST_PACKET_TIME')
        if check_time:
            now = self.clock().astimezone(timezone.utc)
            if not prepared <= now < expires:
                _fail('FORMAL_INGRESS_HOST_PACKET_EXPIRED')
        return prepared, expires

    def receive(self, *, transport, packet, request_id, host_directory,
                binding):
        if (not isinstance(packet, dict)
                or packet.get('operation') != 'review'
                or packet.get('tool') != TOOL):
            _fail('FORMAL_INGRESS_REVIEW_ONLY')
        if (not isinstance(binding, dict)
                or binding.get('account_key') != packet.get('account_key')
                or binding.get('account') != packet.get('account')):
            _fail('FORMAL_INGRESS_BINDING')
        host_directory = Path(host_directory)
        try:
            packet_envelope = read_private_json(
                host_directory / 'packets' / (request_id + '.json'))
            request_record = read_private_json(
                host_directory / 'requests' / (request_id + '.json'))
        except (OSError, ValueError, json.JSONDecodeError):
            _fail('FORMAL_INGRESS_HOST_PACKET_MISSING')
        self._host_packet(packet, packet_envelope, check_time=False)
        if (packet_envelope.get('request_id') != request_id
                or packet_envelope.get('binding') != binding
                or request_record.get('request_id') != request_id
                or request_record.get('action_id') != packet.get('action_id')
                or request_record.get('operation') != 'review'
                or request_record.get('binding') != binding):
            _fail('FORMAL_INGRESS_REQUEST_BINDING')
        operation_id = packet['operation_id']
        path = self._path(operation_id)
        try:
            existing = json.loads(path.read_text())
        except FileNotFoundError:
            existing = None
        except (OSError, ValueError, json.JSONDecodeError):
            _fail('FORMAL_INGRESS_RECORD_UNAVAILABLE')
        if existing is not None:
            self._validate_ingress(existing, packet, request_id,
                                    packet_envelope, request_record, binding,
                                    host_directory)
            return existing
        journal_path = self.root / (operation_id + '.journal.json')
        try:
            journal = json.loads(journal_path.read_text())
        except FileNotFoundError:
            journal = None
        except (OSError, ValueError, json.JSONDecodeError):
            _fail('FORMAL_INGRESS_JOURNAL_UNAVAILABLE')
        if journal is not None:
            expires, received = self._validate_journal(
                journal, packet, request_id, packet_envelope, request_record,
                binding)
            return self._persist_recovery(
                journal, packet, request_id, host_directory, expires, received)
        if transport is None:
            _fail('FORMAL_INGRESS_TRANSPORT_UNAVAILABLE')
        descriptor_sha = self._formal(transport)
        self._host_packet(packet, packet_envelope, check_time=True)
        self._claim(packet)
        execute = getattr(transport, 'execute_review', None)
        if not callable(execute):
            _fail('FORMAL_INGRESS_TRANSPORT_UNAVAILABLE')
        call_started = self.clock().astimezone(timezone.utc)
        try:
            response = execute(packet)
        except Exception as exc:
            failure_code = safe_read_failure(exc)['code']
            self._record_failure(
                packet, request_id, descriptor_sha, call_started,
                failure_code,
                call=getattr(transport, 'last_review_call', None))
            _fail('FORMAL_INGRESS_TRANSPORT_FAILED:' + failure_code)
        if not isinstance(response, dict) or response.get('isError') is True:
            self._record_failure(packet, request_id, descriptor_sha,
                                 call_started, 'RESPONSE_ERROR')
            _fail('FORMAL_INGRESS_RESPONSE_INVALID:RESPONSE_ERROR')
        response_received_at = self.clock().astimezone(timezone.utc)
        call = getattr(transport, 'last_review_call', None)
        if call is not None and descriptor_sha is not None:
            if (not isinstance(call, dict)
                    or call.get('status') != 'received'
                    or call.get('tool') != TOOL
                    or call.get('parameters_sha256') !=
                    digest(packet.get('parameters'))
                    or call.get('response_sha256') != digest(response)
                    or call.get('response_identity_sha256') !=
                    response_identity(response)[1]
                    or call.get('descriptor_sha256') != descriptor_sha):
                _fail('FORMAL_INGRESS_CALL_RECORD_MISMATCH')
            call_id = call.get('rpc_request_id')
            call_started = datetime.fromisoformat(call['started_at'])
            call_ended = datetime.fromisoformat(call['ended_at'])
            origin = 'formal_runtime_transport'
        else:
            if descriptor_sha is not None:
                _fail('FORMAL_INGRESS_CALL_RECORD_MISSING')
            call_id = getattr(transport, 'last_call_id', None)
            call_ended = self.clock().astimezone(timezone.utc)
            origin = 'test_scripted_transport'
        if not isinstance(call_id, str) or not _SAFE_ID.fullmatch(call_id):
            _fail('FORMAL_INGRESS_CALL_ID_MISSING')
        call_record_sha256 = None
        if origin == 'formal_runtime_transport':
            call_record_sha256 = self._persist_call_record(operation_id, call)
        journal = {
            'schema_version': 1, 'kind': 'formal_review_call_journal_v1',
            'status': 'response_received', 'operation_id': operation_id,
            'response_received_at': response_received_at.isoformat(),
            'request_id': request_id, 'action_id': packet['action_id'],
            'account': packet['account'],
            'account_key': packet['account_key'],
            'ref_id': packet.get('logical_ref_id'),
            'packet_sha256': digest(packet),
            'expires_at': packet_envelope['expires_at'],
            'tool': TOOL, 'execution_authorized': False,
            'broker_effect_verified': False,
            'native_confirmation_required': True,
            'parameters_sha256': digest(packet.get('parameters')),
            'descriptor_sha256': descriptor_sha, 'origin': origin,
            'call_id': call_id,
            'call_started_at': call_started.isoformat(),
            'call_ended_at': call_ended.isoformat(),
            'call': call, 'response': response,
            'response_sha256': digest(response),
            'response_identity_sha256': response_identity(response)[1]}
        try:
            write_private_json(journal_path, journal)
        except ValueError:
            _fail('FORMAL_INGRESS_JOURNAL_CONFLICT')
        try:
            receipt = submit_receipt(
                host_directory, request_id, response,
                clock=lambda: response_received_at)
        except (OSError, ValueError, TypeError) as exc:
            _fail('FORMAL_INGRESS_RECEIPT_PERSISTENCE_FAILED:'
                  + str(getattr(exc, 'code', None) or type(exc).__name__))
        _redacted, identity_hash = response_identity(response)
        value = {
            'schema_version': 1, 'kind': INGRESS_KIND,
            'operation_id': operation_id,
            'request_id': request_id, 'action_id': packet['action_id'],
            'account': packet['account'],
            'account_key': packet['account_key'],
            'ref_id': packet.get('logical_ref_id'),
            'tool': TOOL, 'descriptor_sha256': descriptor_sha,
            'origin': origin, 'call_id': call_id,
            'call_started_at': call_started.isoformat(),
            'call_ended_at': call_ended.isoformat(),
            'packet_sha256': digest(packet),
            'parameters_sha256': digest(packet.get('parameters')),
            'response_sha256': digest(response),
            'response_identity_sha256': identity_hash,
            'received_at': receipt['received_at'],
            'host_receipt_sha256': digest(receipt),
            'call_record_sha256': call_record_sha256,
            'historical_recovery': False, 'recovery_status': 'live',
            'status': 'received',
            'execution_authorized': False,
            'broker_effect_verified': False,
            'native_confirmation_required': True}
        try:
            write_private_json(path, value)
        except ValueError as exc:
            if str(exc) != 'HOST_FILE_CONFLICT':
                raise
            _fail('FORMAL_INGRESS_REPLAY_CONFLICT')
        return value

    def _validate_ingress(self, ingress, packet, request_id, packet_envelope,
                          request_record, binding, host_directory):
        if (not isinstance(ingress, dict)
                or ingress.get('schema_version') != 1
                or ingress.get('kind') != INGRESS_KIND
                or ingress.get('status') != 'received'
                or ingress.get('origin') not in {
                    'formal_runtime_transport', 'test_scripted_transport'}
                or ingress.get('operation_id') != packet.get('operation_id')
                or ingress.get('request_id') != request_id
                or ingress.get('action_id') != packet.get('action_id')
                or ingress.get('account') != packet.get('account')
                or ingress.get('account_key') != packet.get('account_key')
                or ingress.get('ref_id') != packet.get('logical_ref_id')
                or ingress.get('tool') != TOOL
                or ingress.get('execution_authorized') is not False
                or ingress.get('broker_effect_verified') is not False
                or ingress.get('native_confirmation_required') is not True
                or ingress.get('packet_sha256') != digest(packet)
                or ingress.get('parameters_sha256') !=
                digest(packet.get('parameters'))):
            _fail('FORMAL_INGRESS_CACHE_INVALID')
        try:
            prepared = self._aware(packet_envelope['prepared_at'])
            expires = self._aware(packet_envelope['expires_at'])
            received = self._aware(ingress['received_at'])
            started = self._aware(ingress['call_started_at'])
            ended = self._aware(ingress['call_ended_at'])
        except (KeyError, TypeError, ValueError):
            _fail('FORMAL_INGRESS_CACHE_TIME_INVALID')
        now = self.clock().astimezone(timezone.utc)
        if (started < prepared or ended < started or received < ended
                or received > expires or received > now):
            _fail('FORMAL_INGRESS_CACHE_TIME_ORDER')
        receipt_path = (__import__('pathlib').Path(host_directory) /
                        'receipts' / (packet['operation_id'] + '.json'))
        try:
            receipt = json.loads(receipt_path.read_text())
        except (OSError, ValueError, json.JSONDecodeError):
            _fail('FORMAL_INGRESS_CACHE_RECEIPT_MISSING')
        if (ingress.get('host_receipt_sha256') != digest(receipt)
                or receipt.get('request_id') != request_id
                or receipt.get('action_id') != packet.get('action_id')
                or receipt.get('binding') != binding
                or receipt.get('response_sha256') !=
                ingress.get('response_sha256')
                or digest(receipt.get('response')) !=
                ingress.get('response_sha256')
                or response_identity(receipt.get('response'))[1] !=
                ingress.get('response_identity_sha256')
                or receipt.get('received_at') != ingress.get('received_at')):
            _fail('FORMAL_INGRESS_CACHE_RECEIPT_MISMATCH')
        journal_path = self.root / (packet['operation_id'] + '.journal.json')
        try:
            journal = json.loads(journal_path.read_text())
        except (OSError, ValueError, json.JSONDecodeError):
            _fail('FORMAL_INGRESS_CACHE_JOURNAL_MISSING')
        self._validate_journal(journal, packet, request_id, packet_envelope,
                               request_record, binding)
        if (journal.get('response_sha256') != ingress.get('response_sha256')
                or journal.get('response_identity_sha256') !=
                ingress.get('response_identity_sha256')
                or journal.get('call_id') != ingress.get('call_id')
                or journal.get('descriptor_sha256') !=
                ingress.get('descriptor_sha256')):
            _fail('FORMAL_INGRESS_CACHE_JOURNAL_MISMATCH')
        if ingress.get('origin') == 'formal_runtime_transport':
            self._load_call_record(packet['operation_id'], ingress, binding)

    def _persist_call_record(self, operation_id, call):
        call_record_sha256 = digest(call)
        call_path = self.root / (operation_id + '.call.json')
        try:
            write_private_json(call_path, call)
        except ValueError:
            _fail('FORMAL_INGRESS_CALL_RECORD_CONFLICT')
        return call_record_sha256

    def _load_call_record(self, operation_id, ingress, binding):
        call_path = self.root / (operation_id + '.call.json')
        try:
            call = json.loads(call_path.read_text())
        except FileNotFoundError:
            _fail('FORMAL_INGRESS_CACHE_CALL_MISSING')
        except (OSError, ValueError, json.JSONDecodeError):
            _fail('FORMAL_INGRESS_CACHE_CALL_UNAVAILABLE')
        if ingress.get('call_record_sha256') != digest(call):
            _fail('FORMAL_INGRESS_CACHE_CALL_CONFLICT')
        self._validate_call_record(call, ingress, binding)

    def _validate_call_record(self, call, ingress, binding):
        if (not isinstance(call, dict)
                or call.get('schema_version') != 1
                or call.get('status') != 'received'
                or call.get('tool') != TOOL
                or not isinstance(call.get('server'), str) or not call.get('server')
                or not isinstance(call.get('thread_id'), str) or not call.get('thread_id')
                or not isinstance(call.get('connection_id'), str) or not call.get('connection_id')
                or call.get('rpc_request_id') != ingress.get('call_id')
                or call.get('descriptor_sha256') != ingress.get('descriptor_sha256')
                or call.get('account_scope_sha256') != binding.get('account_key')
                or call.get('parameters_sha256') != ingress.get('parameters_sha256')
                or call.get('response_sha256') != ingress.get('response_sha256')
                or call.get('response_identity_sha256') !=
                ingress.get('response_identity_sha256')
                or call.get('started_at') != ingress.get('call_started_at')
                or call.get('ended_at') != ingress.get('call_ended_at')):
            _fail('FORMAL_INGRESS_CALL_RECORD_INVALID')
        try:
            started = self._aware(call['started_at'])
            ended = self._aware(call['ended_at'])
        except (KeyError, TypeError, ValueError):
            _fail('FORMAL_INGRESS_CALL_TIME_INVALID')
        if ended < started:
            _fail('FORMAL_INGRESS_CALL_TIME_INVALID')

    def _validate_journal(self, journal, packet, request_id, packet_envelope,
                          request_record, binding):
        if (not isinstance(request_record, dict)
                or request_record.get('request_id') != request_id
                or request_record.get('action_id') != packet.get('action_id')
                or request_record.get('operation') != 'review'
                or request_record.get('binding') != binding
                or not isinstance(journal, dict)
                or journal.get('schema_version') != 1
                or journal.get('kind') != 'formal_review_call_journal_v1'
                or journal.get('status') != 'response_received'
                or journal.get('operation_id') != packet.get('operation_id')
                or journal.get('request_id') != request_id
                or journal.get('action_id') != packet.get('action_id')
                or journal.get('account') != packet.get('account')
                or journal.get('account_key') != packet.get('account_key')
                or journal.get('ref_id') != packet.get('logical_ref_id')
                or journal.get('packet_sha256') != digest(packet)
                or journal.get('expires_at') != packet_envelope.get('expires_at')
                or journal.get('parameters_sha256') !=
                digest(packet.get('parameters'))
                or journal.get('origin') not in {
                    'formal_runtime_transport', 'test_scripted_transport'}
                or journal.get('tool') != TOOL
                or journal.get('execution_authorized') is not False
                or journal.get('broker_effect_verified') is not False
                or journal.get('native_confirmation_required') is not True
                or not isinstance(journal.get('response'), dict)
                or journal.get('response_sha256') !=
                digest(journal.get('response'))
                or journal.get('response_identity_sha256') !=
                response_identity(journal['response'])[1]):
            _fail('FORMAL_INGRESS_JOURNAL_INVALID')
        try:
            prepared = self._aware(packet_envelope['prepared_at'])
            expires = self._aware(packet_envelope['expires_at'])
            journal_expires = self._aware(journal['expires_at'])
            received = self._aware(journal['response_received_at'])
            started = self._aware(journal['call_started_at'])
            ended = self._aware(journal['call_ended_at'])
        except (KeyError, TypeError, ValueError):
            _fail('FORMAL_INGRESS_JOURNAL_TIME_INVALID')
        now = self.clock().astimezone(timezone.utc)
        if (journal_expires != expires or started < prepared
                or ended < started or received < ended
                or received > now or received > expires):
            _fail('FORMAL_INGRESS_JOURNAL_TIME_ORDER')
        call = journal.get('call')
        if journal.get('origin') == 'formal_runtime_transport':
            if (not isinstance(call, dict)
                    or call.get('status') != 'received'
                    or call.get('tool') != TOOL
                    or call.get('parameters_sha256') !=
                    journal.get('parameters_sha256')
                    or call.get('response_sha256') !=
                    journal.get('response_sha256')
                    or call.get('response_identity_sha256') !=
                    journal.get('response_identity_sha256')
                    or call.get('descriptor_sha256') !=
                    journal.get('descriptor_sha256')
                    or call.get('account_scope_sha256') != binding.get('account_key')
                    or call.get('rpc_request_id') != journal.get('call_id')
                    or call.get('started_at') != journal.get('call_started_at')
                    or call.get('ended_at') != journal.get('call_ended_at')):
                _fail('FORMAL_INGRESS_JOURNAL_CALL_INVALID')
        elif call is not None:
            _fail('FORMAL_INGRESS_JOURNAL_CALL_INVALID')
        return expires, received

    def _persist_recovery(self, journal, packet, request_id, host_directory,
                          expires, received):
        response = journal.get('response')
        received_at = received
        try:
            receipt = submit_receipt(
                host_directory, request_id, response,
                clock=lambda: received_at)
        except (OSError, ValueError, TypeError) as exc:
            _fail('FORMAL_INGRESS_RECOVERY_FAILED:'
                  + str(getattr(exc, 'code', None) or str(exc)))
        _redacted, identity_hash = response_identity(response)
        expired = self.clock().astimezone(timezone.utc) >= expires
        value = {
            'schema_version': 1, 'kind': INGRESS_KIND,
            'operation_id': journal['operation_id'],
            'historical_recovery': expired,
            'recovery_status': ('historical_expired' if expired
                                else 'recovered'),
            'request_id': request_id, 'action_id': journal['action_id'],
            'account': journal['account'],
            'account_key': journal['account_key'],
            'ref_id': journal.get('ref_id'),
            'tool': TOOL, 'descriptor_sha256': journal.get('descriptor_sha256'),
            'origin': journal.get('origin'), 'call_id': journal.get('call_id'),
            'call_started_at': journal['call_started_at'],
            'call_ended_at': journal['call_ended_at'],
            'packet_sha256': journal['packet_sha256'],
            'parameters_sha256': journal['parameters_sha256'],
            'response_sha256': digest(response),
            'response_identity_sha256': identity_hash,
            'received_at': receipt['received_at'],
            'host_receipt_sha256': digest(receipt),
            'call_record_sha256': (digest(journal['call'])
                                   if journal.get('call') is not None
                                   else None),
            'status': 'received', 'execution_authorized': False,
            'broker_effect_verified': False,
            'native_confirmation_required': True}
        try:
            write_private_json(self._path(journal['operation_id']), value)
        except ValueError:
            _fail('FORMAL_INGRESS_REPLAY_CONFLICT')
        return value

    def _record_failure(self, packet, request_id, descriptor_sha,
                        call_started, reason, *, call=None):
        call=call if isinstance(call,dict) else None
        value = {
            'schema_version': 1, 'kind': INGRESS_KIND,
            'operation_id': packet.get('operation_id'),
            'request_id': request_id, 'action_id': packet.get('action_id'),
            'account': packet.get('account'),
            'account_key': packet.get('account_key'),
            'ref_id': packet.get('logical_ref_id'),
            'tool': TOOL, 'descriptor_sha256': descriptor_sha,
            'origin': ('formal_runtime_transport'
                       if descriptor_sha is not None
                       else 'test_scripted_transport'),
            'call_id': (call or {}).get('rpc_request_id'),
            'call_started_at': ((call or {}).get('started_at')
                                or call_started.isoformat()),
            'call_ended_at': ((call or {}).get('ended_at')
                              or call_started.isoformat()),
            'packet_sha256': digest(packet),
            'parameters_sha256': digest(packet.get('parameters')),
            'response_sha256': None, 'response_identity_sha256': None,
            'received_at': call_started.isoformat(),
            'host_receipt_sha256': None,
            'call_record_sha256': digest(call) if call is not None else None,
            'status': 'failed', 'failure': reason, 'execution_authorized': False,
            'broker_effect_verified': False,
            'native_confirmation_required': True}
        try:
            write_private_json(self._path(packet['operation_id']), value)
        except ValueError:
            pass
