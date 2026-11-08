"""Owner-bound real-source evidence for supervised engineering.

This module does not grant entry, consume a permit, or open a broker path.  It
turns the owner request/permit, native review record, account-facts assessment,
current-capture registry and optional risk-lineage/provenance records into one
bounded, digest-addressed evidence object for dry assessment.

A caller-supplied boolean, arbitrary JSON, or ``lambda: True`` is never a trust
root.  Missing trusted owner provenance or risk lineage is an explicit block.
"""
from datetime import datetime, timedelta, timezone
import json
import sqlite3

from trading_contracts.values import digest

from .current_context import CurrentContextRegistry, CurrentContextError
from .qualification import source_fingerprint
from .supervised_engineering import (
    MAX_REVIEW_AGE_SECONDS, SupervisedEngineeringError, _review_record, _time)

EVIDENCE_KIND = 'engineering_source_evidence_v1'
PROVENANCE_KINDS = frozenset({'host_owner_request', 'owner_mailbox_request'})
RISK_LINEAGE_KEYS = frozenset({
    'status', 'lineage_id', 'risk_state_sha256', 'account_key', 'purpose',
    'source', 'observed_at', 'valid_until'})
RISK_LINEAGE_SOURCE = 'owner-risk-lineage-v1'


def _blocked(code, detail=None):
    return {'kind': EVIDENCE_KIND, 'verified': False, 'status': 'blocked',
            'reasons': [code] + ([str(detail)] if detail else []),
            'source_sha256': None, 'execution_authorized': False,
            'entry_authorized': False}


def _bound(value, keys, code):
    if not isinstance(value, dict) or set(value) != set(keys):
        raise ValueError(code)
    return value


class EngineeringSourceVerifier:
    """Read authoritative owner records; never accept a caller boolean.

    ``provenance_reader`` and ``risk_lineage_reader`` are explicit owner
    interfaces.  Without them the verifier blocks named reasons; they are not
    silently replaced by synthetic fixtures.
    """

    def __init__(self, *, provenance_reader=None, risk_lineage_reader=None,
                 native_receipt_reader=None, clock=None):
        self.provenance_reader = provenance_reader
        self.risk_lineage_reader = risk_lineage_reader
        self.native_receipt_reader = native_receipt_reader
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def verify(self, book, result, *, allow_ready=False):
        try:
            return self._verify(book, result, allow_ready=allow_ready)
        except (SupervisedEngineeringError, ValueError, KeyError, TypeError,
                OSError, sqlite3.Error) as exc:
            return _blocked(getattr(exc, 'code', None) or str(exc))

    def _verify(self, book, result, *, allow_ready=False):
        if not isinstance(result, dict):
            return _blocked('ENGINEERING_SOURCE_RESULT_MISSING')
        permit_wrapper = result.get('permit') or {}
        permit = (permit_wrapper.get('permit')
                  if isinstance(permit_wrapper.get('permit'), dict)
                  else permit_wrapper)
        request = result.get('request') or {}
        action = result.get('action') or {}
        action_id = action.get('id') or permit.get('action_id')
        if not action_id:
            return _blocked('ENGINEERING_SOURCE_ACTION_MISSING')
        action_row = next((item for item in book.actions()
                           if item.get('id') == action_id), None)
        if action_row is None:
            return _blocked('ENGINEERING_SOURCE_ACTION_MISSING')
        allowed_states = {'REVIEW_PENDING', 'WAITING_USER'}
        if allow_ready is True:
            allowed_states.add('READY')
        if action_row.get('state') not in allowed_states:
            return _blocked('ENGINEERING_SOURCE_ACTION_STATE')
        action_payload_sha256 = digest(action_row.get('payload'))
        if permit.get('action_id') != action_id:
            return _blocked('ENGINEERING_SOURCE_ACTION_MISMATCH')

        review = _review_record(book, action_id)
        formal = permit.get('formal') or {}
        if (review.get('operation_id') != formal.get('review_operation_id')
                or review.get('response_hash') != formal.get('review_response_hash')
                or review.get('parameters_sha256') !=
                formal.get('review_parameters_sha256')
                or review.get('order_checks_sha256') !=
                formal.get('order_checks_sha256')):
            return _blocked('ENGINEERING_SOURCE_REVIEW_MISMATCH')
        if review.get('recorded_at') > self.clock().astimezone(timezone.utc):
            return _blocked('ENGINEERING_SOURCE_REVIEW_FUTURE')

        facts = book.current_account_facts(purpose='engineering') \
            if callable(getattr(book, 'current_account_facts', None)) else None
        if not isinstance(facts, dict):
            return _blocked('ENGINEERING_SOURCE_FACTS_MISSING')
        if (facts.get('assessment_id') != (permit.get('facts') or {}).get(
                'assessment_id')
                or facts.get('observation_valid') is not True):
            return _blocked('ENGINEERING_SOURCE_FACTS_MISMATCH')

        generation = None
        config = (book.policy or {}).get('current_capture')
        if isinstance(config, dict) and config.get('root') \
                and config.get('public_manifest'):
            try:
                from pathlib import Path
                root = Path(config['root'])
                registry = CurrentContextRegistry(
                    root / 'registry.json',
                    allowed_capture_roots=(root,),
                    allowed_public_roots=(
                        Path(config['public_manifest']).parent,),
                    clock=self.clock)
                record = registry.read()
                generation = record.get('generation_id')
                permit_generation = (permit.get('facts') or {}).get(
                    'capture_generation')
                facts_generation = (facts.get('binding') or {}).get(
                    'capture_generation')
                source_generation = (permit.get('source') or {}).get(
                    'capture_generation')
                if (record.get('source_fingerprint') != source_fingerprint()
                        or record.get('account_key_hash') !=
                        permit.get('account_key')
                        or record.get('policy_sha256') !=
                        (permit.get('source') or {}).get('policy_sha256')):
                    return _blocked('ENGINEERING_SOURCE_CAPTURE_BINDING')
                if (not isinstance(permit_generation, str)
                        or not permit_generation
                        or facts_generation != permit_generation
                        or (source_generation is not None
                            and source_generation != permit_generation)
                        or generation != permit_generation):
                    return _blocked('ENGINEERING_SOURCE_CAPTURE_GENERATION')
            except (CurrentContextError, OSError, ValueError, TypeError) as exc:
                return _blocked('ENGINEERING_SOURCE_CAPTURE_UNAVAILABLE',
                                getattr(exc, 'code', None) or str(exc))
        else:
            return _blocked('ENGINEERING_SOURCE_CAPTURE_CONFIG_MISSING')

        provenance = self._provenance(book, result)
        if provenance.get('verified') is not True:
            return _blocked('ENGINEERING_SOURCE_OWNER_PROVENANCE_MISSING')
        risk = self._risk_lineage(book, result)
        if risk.get('verified') is not True:
            return _blocked('ENGINEERING_SOURCE_RISK_LINEAGE_MISSING',
                            risk.get('reason'))
        provenance_after = self._provenance(book, result)
        if (provenance_after.get('verified') is not True
                or provenance_after.get('record_sha256') !=
                provenance.get('record_sha256')
                or provenance_after.get('request_sha256') !=
                provenance.get('request_sha256')):
            return _blocked('ENGINEERING_SOURCE_OWNER_PROVENANCE_CHANGED')
        risk_after = self._risk_lineage(book, result)
        if (risk_after.get('verified') is not True
                or risk_after.get('lineage_id') != risk.get('lineage_id')
                or risk_after.get('risk_state_sha256') !=
                risk.get('risk_state_sha256')):
            return _blocked('ENGINEERING_SOURCE_RISK_LINEAGE_CHANGED')
        native = {'status': 'not_configured', 'origin': None,
                  'receipt_sha256': None, 'response_sha256': None,
                  'response_identity_sha256': None, 'operation_id': None,
                  'request_id': None, 'parameters_sha256': None,
                  'record_state': None, 'native_status': None,
                  'native_confirmation_required': None}
        if self.native_receipt_reader is not None:
            try:
                reader = self.native_receipt_reader
                native = (reader.read(book, result)
                          if callable(getattr(reader, 'read', None))
                          else reader(book, result))
            except Exception as exc:
                return _blocked('ENGINEERING_SOURCE_NATIVE_RECEIPT:'
                                + str(getattr(exc, 'code', None)
                                      or type(exc).__name__))
            if (not isinstance(native, dict)
                    or native.get('status') != 'bound'
                    or native.get('origin') not in {
                        'host_submitted_response', 'formal_runtime_transport'}
                    or native.get('broker_effect_verified') is not False):
                return _blocked('ENGINEERING_SOURCE_NATIVE_RECEIPT_UNBOUND')

        try:
            record_after = registry.read()
        except (CurrentContextError, OSError, ValueError, TypeError) as exc:
            return _blocked('ENGINEERING_SOURCE_CAPTURE_UNAVAILABLE',
                            getattr(exc, 'code', None) or str(exc))
        if (record.get('_registry_sha256') !=
                record_after.get('_registry_sha256')
                or record.get('generation_id') !=
                record_after.get('generation_id')
                or record.get('account_key_hash') !=
                record_after.get('account_key_hash')
                or record.get('policy_sha256') !=
                record_after.get('policy_sha256')
                or record.get('source_fingerprint') !=
                record_after.get('source_fingerprint')):
            return _blocked('ENGINEERING_SOURCE_CAPTURE_CHANGED')
        end_now = self.clock().astimezone(timezone.utc)
        try:
            facts_valid = _time(facts.get('observation_valid_until'),
                                'ENGINEERING_SOURCE_TIME')
            permit_valid = _time(permit.get('valid_until'),
                                 'ENGINEERING_SOURCE_TIME')
        except SupervisedEngineeringError:
            return _blocked('ENGINEERING_SOURCE_TIME_INVALID')
        if (end_now > facts_valid or end_now > permit_valid
                or review['recorded_at'] > end_now
                or (end_now - review['recorded_at']).total_seconds()
                > MAX_REVIEW_AGE_SECONDS
                or end_now > risk['valid_until_dt']
                or end_now < risk['observed_at_dt']):
            return _blocked('ENGINEERING_SOURCE_WINDOW_EXPIRED')
        body = {
            'kind': EVIDENCE_KIND,
            'request_id': request.get('request_id'),
            'request_sha256': result.get('request', {}).get('request_sha256'),
            'permit_id': permit.get('permit_id'),
            'action_id': action_id,
            'ref_id': permit.get('ref_id'),
            'purpose': permit.get('purpose'),
            'account_key': permit.get('account_key'),
            'account_suffix': permit.get('account_suffix'),
            'policy_sha256': (permit.get('source') or {}).get('policy_sha256'),
            'source_fingerprint': (permit.get('source') or {}).get(
                'source_fingerprint'),
            'capture_generation': generation,
            'facts_assessment_id': facts.get('assessment_id'),
            'review_operation_id': review.get('operation_id'),
            'review_response_hash': review.get('response_hash'),
            'review_parameters_sha256': review.get('parameters_sha256'),
            'review_recorded_at': review.get('recorded_at').isoformat(),
            'action_payload_sha256': action_payload_sha256,
            'risk_lineage_id': risk['lineage_id'],
            'risk_state_sha256': risk['risk_state_sha256'],
            'risk_source': risk['source'],
            'risk_observed_at': risk['observed_at'],
            'risk_valid_until': risk['valid_until'],
            'provenance_kind': provenance['kind'],
            'provenance_sha256': provenance['record_sha256'],
            'provenance_request_sha256': provenance['request_sha256'],
            'native_receipt_status': native['status'],
            'native_receipt_sha256': native.get('receipt_sha256'),
            'native_response_sha256': native.get('response_sha256'),
            'native_response_identity_sha256': native.get(
                'response_identity_sha256'),
            'native_operation_id': native.get('operation_id'),
            'native_request_id': native.get('request_id'),
            'native_origin': native.get('origin'),
            'native_parameters_sha256': native.get('parameters_sha256'),
            'native_record_state': native.get('record_state'),
            'native_status': native.get('native_status'),
            'native_confirmation_required': native.get(
                'native_confirmation_required'),
            'native_broker_effect_verified': False,
            'one_shot_id': permit.get('one_shot_id'),
            'observed_at': end_now.isoformat(),
            'valid_until': min(
                _time(permit.get('valid_until'), 'ENGINEERING_SOURCE_TIME'),
                _time(facts.get('observation_valid_until'),
                      'ENGINEERING_SOURCE_TIME'),
                review['recorded_at'] + timedelta(
                    seconds=MAX_REVIEW_AGE_SECONDS),
                risk['valid_until_dt']).isoformat(),
            'execution_authorized': False,
            'entry_authorized': False,
            'production_ready': False,
        }
        body['source_sha256'] = digest(body)
        return {**body, 'verified': True, 'status': 'verified',
                'reasons': []}

    def _provenance(self, book, result):
        if self.provenance_reader is None:
            return {'verified': False, 'reason': 'READER_NOT_CONFIGURED'}
        try:
            value = self.provenance_reader(book, result)
        except Exception as exc:
            return {'verified': False, 'reason': getattr(exc, 'code', None)
                    or type(exc).__name__}
        if not isinstance(value, dict) or value.get('kind') not in PROVENANCE_KINDS:
            return {'verified': False, 'reason': 'KIND_INVALID'}
        record = value.get('record')
        if not isinstance(record, dict):
            return {'verified': False, 'reason': 'RECORD_MISSING'}
        request = result.get('request') or {}
        permit_wrapper = result.get('permit') or {}
        permit = (permit_wrapper.get('permit')
                  if isinstance(permit_wrapper.get('permit'), dict)
                  else permit_wrapper)
        request_sha = (permit.get('request_sha256')
                       or request.get('request_sha256'))
        if (not isinstance(record.get('request_sha256'), str)
                or record.get('request_sha256') != request_sha
                or record.get('request_id') != request.get('request_id')
                or record.get('action_id') != permit.get('action_id')):
            return {'verified': False, 'reason': 'BINDING_MISMATCH'}
        record_sha = digest(record)
        if value.get('record_sha256') != record_sha:
            return {'verified': False, 'reason': 'HASH_MISMATCH'}
        return {'verified': True, 'kind': value['kind'],
                'record_sha256': record_sha,
                'request_sha256': request_sha}

    def _risk_lineage(self, book, result):
        if self.risk_lineage_reader is None:
            return {'verified': False, 'reason': 'READER_NOT_CONFIGURED'}
        try:
            value = self.risk_lineage_reader(book, result)
        except Exception as exc:
            return {'verified': False, 'reason': getattr(exc, 'code', None)
                    or type(exc).__name__}
        if not isinstance(value, dict) or set(value) != RISK_LINEAGE_KEYS:
            return {'verified': False, 'reason': 'SCHEMA'}
        if value.get('status') != 'bound':
            return {'verified': False, 'reason': 'STATUS_NOT_BOUND'}
        if value.get('source') != RISK_LINEAGE_SOURCE:
            return {'verified': False, 'reason': 'SOURCE_CONTRACT'}
        permit_wrapper = result.get('permit') or {}
        permit = (permit_wrapper.get('permit')
                  if isinstance(permit_wrapper.get('permit'), dict)
                  else permit_wrapper)
        if (not isinstance(value.get('lineage_id'), str)
                or not value.get('lineage_id')
                or value.get('risk_state_sha256') !=
                digest(book.risk_guard.state())
                or value.get('account_key') != permit.get('account_key')
                or value.get('purpose') != 'engineering'):
            return {'verified': False, 'reason': 'BINDING_MISMATCH'}
        try:
            observed = _time(value.get('observed_at'), 'RISK_TIME')
            expires = _time(value.get('valid_until'), 'RISK_TIME')
        except SupervisedEngineeringError:
            return {'verified': False, 'reason': 'TIME_INVALID'}
        now = self.clock().astimezone(timezone.utc)
        if observed > now or now > expires or expires <= observed:
            return {'verified': False, 'reason': 'WINDOW'}
        return {'verified': True, **value,
                'observed_at_dt': observed, 'valid_until_dt': expires}


def build_owner_engineering_authority(root, *, clock=None,
                                      allow_synthetic=False,
                                      provenance_reader=None,
                                      risk_lineage_reader=None,
                                      native_receipt_reader=None,
                                      real_activation_enabled=False):
    """Compose the owner authority with the real-source evidence adapter.

    The activation gate is explicit and defaults closed.  Supplying readers is
    an owner integration decision; missing readers produce named blocks rather
    than a synthetic or boolean substitute.
    """
    from .supervised_engineering import SupervisedEngineeringAuthority
    verifier = EngineeringSourceVerifier(
        provenance_reader=provenance_reader,
        risk_lineage_reader=risk_lineage_reader,
        native_receipt_reader=native_receipt_reader, clock=clock)
    return SupervisedEngineeringAuthority(
        root, clock=clock, allow_synthetic=allow_synthetic,
        real_source_verifier=verifier,
        real_activation_enabled=real_activation_enabled)

