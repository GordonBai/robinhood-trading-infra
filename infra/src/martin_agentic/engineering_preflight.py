"""Current engineering preflight composition.

The current evidence compiler is the observation source for this gate; the
execution book remains the risk/lineage/control authority.  This module only
adds a current-observation consumer.  It never submits, approves, or grants a
formal write channel.  A real ``simulation=false`` book is always blocked here
until a formal execution/protection channel is separately proven.

Every assessment returns a versioned owner-bound receipt.  Binding truth is
independent of that final formal-channel denial: a receipt can be internally
consistent (``bound=true``) while the decision remains ``allowed=false`` for
source gaps or the unproven formal channel.
"""
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation
import hashlib
import json
import sqlite3

from trading_contracts.values import digest, timestamp

from .assessment_receipt import (
    AUTHORITY_NAMES, authorization_context, build_receipt, capture_recheck_reasons,
    journal_components, normalize_authority_component, read_book_authority,
    verify_receipt_binding, _public_recheck_reasons)
from .current_evidence import (
    MAX_CURRENT_EVIDENCE_AGE_SECONDS, compile_current_evidence)
from .formal_authority import FormalAuthority, component_of
from .qualification import source_fingerprint, source_pin


def _sha256_text(value):
    return hashlib.sha256(str(value or '').encode()).hexdigest()


class CurrentEngineeringPreflight:
    """Bound capture root + policy/source/account identity."""

    def __init__(self, capture_root, public_sources, *, account_key,
                 account_suffix, policy_sha256, source_fingerprint_value,
                 clock=None, authority_reader=None,
                 expected_capture_generation=None, registry_sha256=None,
                 registry_path=None, formal_authority=None):
        self.capture_root = str(capture_root)
        self.public_sources = str(public_sources)
        self.account_key = account_key
        self.account_suffix = account_suffix
        self.policy_sha256 = policy_sha256
        self.source_fingerprint_value = source_fingerprint_value
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.authority_reader = authority_reader or read_book_authority
        if formal_authority is not None and not isinstance(
                formal_authority, FormalAuthority):
            raise ValueError('FORMAL_AUTHORITY_INVALID')
        self.formal_authority = formal_authority
        self.expected_capture_generation = expected_capture_generation
        self.registry_sha256 = registry_sha256
        self.registry_path = (str(registry_path) if registry_path is not None
                              else None)
        self.factory_owned = False
        self.context_capture = None
        self.context_observed_at = None
        self.context_published_at = None
        self.context_source_pin = None
        self.binding_id = digest({
            'schema_version': 1,
            'kind': 'current_engineering_preflight_binding_v1',
            'account_key': self.account_key,
            'account_suffix': self.account_suffix,
            'policy_sha256': self.policy_sha256,
            'source_fingerprint': self.source_fingerprint_value,
            'capture_root': str(self.capture_root),
            'public_sources': str(self.public_sources),
            'expected_capture_generation': self.expected_capture_generation,
            'registry_sha256': self.registry_sha256,
            'registry_path': self.registry_path,
        })

    def registry_pointer_reasons(self):
        """Cheap bounded check that no newer publication replaced this context."""
        if not self.registry_path:
            return []
        from .current_context import CurrentContextError, _read_registry_bytes
        try:
            raw = _read_registry_bytes(self.registry_path)
        except (CurrentContextError, OSError, ValueError):
            return ['CURRENT_REGISTRY_POINTER_UNAVAILABLE']
        current = hashlib.sha256(raw).hexdigest()
        if self.registry_sha256 is None or current != self.registry_sha256:
            return ['CURRENT_REGISTRY_POINTER_CHANGED']
        return []

    def _block(self, reason, evidence=None):
        return {'allowed': False, 'reasons': [reason],
                'evidence': evidence or {}, 'entry_authorized': False,
                'components': {name: {'status': 'not_evaluated',
                                      'reason': reason}
                               for name in ('current_evidence', 'capture_generation',
                                            'journal_current', 'journal_health',
                                            'journal_capture_consistency', 'action',
                                            'quote', 'risk', 'lineage', 'control',
                                            'formal_channel')}}

    def _verified_not_applicable(self, name, item, evidence):
        """Consume only a complete, source-bound public product N/A receipt."""
        if item.get('scope_status') != 'verified_not_applicable':
            return False
        positions = item.get('positions') or {}
        orders = item.get('orders') or {}
        if (positions.get('status') != 'not_applicable_by_public_product_source'
                or orders.get('status') !=
                'not_applicable_by_public_product_source'):
            return False
        app = item.get('applicability')
        scope = evidence.get('account_scope_applicability') or {}
        account = evidence.get('account') or {}
        product = scope.get('product_evidence') or {}
        if not isinstance(app, dict) or not isinstance(account, dict):
            return False
        if (app.get('schema_version') != 1
                or app.get('kind') != 'public_product_scope_v1'
                or app.get('product') != name
                or app.get('conclusion') != 'not_supported'
                or app.get('account_affiliate') != 'rhf'
                or app.get('account_key_hash') != account.get('account_key_hash')
                or app.get('manifest_sha256') != product.get('manifest_sha256')
                or app.get('policy_version') != product.get('policy_version')
                or app.get('not_an_order_observation') is not True):
            return False
        expected_key = {
            'fixed_income': 'rhf_current_fixed_income',
            'mutual_funds': 'rhf_current_mutual_funds',
        }.get(name)
        if expected_key is None or app.get('conclusion_key') != expected_key:
            return False
        source = next((entry for entry in (product.get('sources') or [])
                       if isinstance(entry, dict)
                       and entry.get('name') == app.get('source_name')), None)
        if source is None:
            return False
        if (source.get('sha256') != app.get('source_sha256')
                or source.get('url') != app.get('source_url')
                or source.get('retrieved_at') != app.get('retrieved_at')
                or source.get('expires_at') != app.get('expires_at')
                or (source.get('conclusions') or {}).get(expected_key)
                != 'not_supported'):
            return False
        try:
            observed = timestamp(app.get('observed_at'))
            retrieved = timestamp(app.get('retrieved_at'))
            expires = timestamp(app.get('expires_at'))
        except (TypeError, ValueError):
            return False
        return retrieved <= observed <= expires

    def _evidence_base(self, book):
        reasons = []
        evidence = compile_current_evidence(
            self.capture_root, self.public_sources, now=book.clock())
        observed_source = source_fingerprint()
        if evidence.get('observation_valid') is not True:
            reasons.extend(evidence.get('blockers') or
                           ['CURRENT_EVIDENCE_NOT_VALID'])
        if evidence.get('bundle_is_current') is not True:
            reasons.append('CURRENT_EVIDENCE_NOT_CURRENT')
        account = evidence.get('account') or {}
        if account.get('account_key_hash') != self.account_key:
            reasons.append('CURRENT_EVIDENCE_ACCOUNT_MISMATCH')
        if account.get('account_mask') != '••••' + self.account_suffix:
            reasons.append('CURRENT_EVIDENCE_ACCOUNT_MASK_MISMATCH')
        if digest(book.policy) != self.policy_sha256:
            reasons.append('CURRENT_EVIDENCE_POLICY_MISMATCH')
        if observed_source != self.source_fingerprint_value:
            reasons.append('CURRENT_EVIDENCE_SOURCE_CHANGED')
        return evidence, observed_source, sorted(set(reasons))

    def _scope_funds_checks(self, evidence, plan=None):
        """Separate fact-reading gaps from entry-readiness blockers."""
        facts = set()
        readiness = set()
        scopes = (evidence.get('account_scope_applicability') or {}).get('scopes') or {}
        for name in ('equity', 'options', 'crypto'):
            item = scopes.get(name) or {}
            if item.get('scope_status') != 'observed_supported':
                facts.add('CURRENT_SCOPE_UNPROVEN:' + name)
            positions = item.get('positions') or {}
            orders = item.get('orders') or {}
            if positions.get('status') != 'observed_complete':
                facts.add('CURRENT_POSITION_UNPROVEN:' + name)
            elif positions.get('count') != 0:
                readiness.add('CURRENT_POSITION_NOT_FLAT:' + name)
            if orders.get('status') != 'observed_complete':
                facts.add('CURRENT_OPEN_ORDER_UNPROVEN:' + name)
            elif orders.get('open_count') != 0:
                readiness.add('CURRENT_OPEN_ORDER_PRESENT:' + name)
        advanced = scopes.get('advanced') or {}
        groups = advanced.get('groups') or {}
        orders = advanced.get('orders') or {}
        if advanced.get('scope_status') != 'observed_supported':
            facts.add('CURRENT_SCOPE_UNPROVEN:advanced')
        if groups.get('status') != 'observed_complete':
            facts.add('CURRENT_ADVANCED_GROUP_UNPROVEN')
        if orders.get('status') != 'observed_complete':
            facts.add('CURRENT_ADVANCED_ORDER_UNPROVEN')
        elif orders.get('open_count') != 0:
            readiness.add('CURRENT_ADVANCED_ORDER_PRESENT')
        approvals = (evidence.get('account_scope_applicability') or {}).get('approvals') or {}
        if approvals.get('status') != 'observed_complete':
            facts.add('CURRENT_APPROVALS_UNPROVEN')
        elif approvals.get('count') != 0:
            readiness.add('CURRENT_APPROVALS_PRESENT')
        for name in ('fixed_income', 'mutual_funds'):
            item = scopes.get(name) or {}
            if self._verified_not_applicable(name, item, evidence):
                continue
            if item.get('scope_status') == 'verified_not_applicable':
                facts.add('CURRENT_SCOPE_SOURCE_UNPROVEN:' + name)
            if item.get('scope_status') not in {
                    'observed_supported', 'verified_not_applicable'}:
                facts.add('CURRENT_SCOPE_UNPROVEN:' + name)
            position_status = (item.get('positions') or {}).get('status')
            if position_status == 'current_nonzero_by_portfolio':
                readiness.add('CURRENT_POSITION_NOT_FLAT:' + name)
            elif position_status != 'current_zero_by_portfolio':
                facts.add('CURRENT_POSITION_UNPROVEN:' + name)
            if (item.get('orders') or {}).get('status') != 'unknown_getter_missing':
                facts.add('CURRENT_ORDERS_SCOPE_UNEXPECTED:' + name)
            facts.add('BLOCKED_SOURCE:' + name + '_orders')
        for name in ('futures', 'event_contracts'):
            item = scopes.get(name) or {}
            if item.get('scope_status') != 'observed_supported':
                facts.add('CURRENT_SCOPE_UNPROVEN:' + name)
            if (item.get('positions') or {}).get('status') != 'unknown_account_scope_unproven':
                facts.add('CURRENT_SCOPE_UNEXPECTED:' + name)
            facts.add('BLOCKED_SOURCE:' + name)
        funds = evidence.get('funds_availability') or {}
        if funds.get('status') != 'observed_current_money_facts':
            facts.add('CURRENT_FUNDS_UNPROVEN')
        if funds.get('money_facts_valid') is not True:
            facts.add('CURRENT_FUNDS_UNPROVEN')
        values = funds.get('values')
        if not isinstance(values, dict) or not values:
            facts.add('CURRENT_FUNDS_FIELDS_INVALID')
        else:
            try:
                pending = Decimal(str(values['pending_deposits']))
                if pending < 0:
                    facts.add('CURRENT_PENDING_DEPOSITS_NEGATIVE')
                elif pending != 0:
                    readiness.add('PENDING_DEPOSIT_REQUIRES_REVIEW')
                buying = Decimal(str(values['buying_power']))
                unleveraged = Decimal(str(values['unleveraged_buying_power']))
                if buying < 0 or unleveraged < 0:
                    readiness.add('CURRENT_BUYING_POWER_NEGATIVE')
                if (isinstance(plan, dict)
                        and 'quantity' in plan and 'entry_ceiling' in plan):
                    qty = Decimal(str(plan['quantity']))
                    entry = Decimal(str(plan['entry_ceiling']))
                    cost = Decimal(str(plan.get('cost_buffer_per_share', '0')))
                    need = qty * (entry + cost)
                    for key in ('buying_power', 'unleveraged_buying_power'):
                        if Decimal(str(values[key])) < need:
                            readiness.add('CURRENT_UNLEVERAGED_FUNDS_INSUFFICIENT')
                            break
            except (KeyError, TypeError, ValueError, InvalidOperation):
                facts.add('CURRENT_FUNDS_FIELDS_INVALID')
        return sorted(facts), sorted(readiness)

    def _base(self, book, snapshot, quote, plan):
        evidence, observed_source, reasons = self._evidence_base(book)
        if snapshot.get('account_key') != self.account_key:
            reasons.append('BOOK_SNAPSHOT_ACCOUNT_MISMATCH')
        if snapshot.get('account_mask') != '••••' + self.account_suffix:
            reasons.append('BOOK_SNAPSHOT_MASK_MISMATCH')
        if quote.get('symbol') != plan.get('symbol'):
            reasons.append('QUOTE_SYMBOL_MISMATCH')
        try:
            if not 0 <= (book.clock() - _ts(quote['observed_at'])).total_seconds() <= 15:
                reasons.append('QUOTE_STALE_OR_FUTURE')
        except (KeyError, TypeError, ValueError):
            reasons.append('QUOTE_TIME_INVALID')
        facts, readiness = self._scope_funds_checks(evidence, plan)
        reasons.extend(facts)
        reasons.extend(readiness)
        return evidence, observed_source, sorted(set(reasons))

    def current_account_assessment(self, book, *, purpose=None):
        """Read-only account facts; never an entry authorization or permit.

        ``observation_valid`` means the owner-bound capture/journal/account
        facts were read consistently.  ``facts_qualified`` additionally means
        every material scope is observed or verified not applicable and the
        risk/lineage/control components were evaluable.  ``account_entry_readiness``
        additionally requires the purpose/policy entry conditions (flat,
        no open orders/approvals, no pending deposit/negative funds, no risk
        lock) to hold.  A complete non-zero position is a valid observation,
        but makes readiness false.
        """
        try:
            evidence, observed_source, observation_reasons = self._evidence_base(book)
            snapshot, health, snapshot_reasons = self._current_snapshot(book)
            observation_reasons.extend(snapshot_reasons)
            capture = self._capture_component(evidence)
            if capture.get('status') != 'evaluated':
                observation_reasons.append(
                    capture.get('reason') or
                    'CURRENT_CAPTURE_GENERATION_MISSING')
            elif (self.expected_capture_generation is not None
                  and capture.get('generation') !=
                  self.expected_capture_generation):
                observation_reasons.append(
                    'CURRENT_CAPTURE_GENERATION_REPLACED')
            source = self._source_component(evidence, observed_source)
            if snapshot is None:
                journal = {'status': 'not_evaluated',
                           'reason': 'CURRENT_PREFLIGHT_SNAPSHOT_MISSING'}
                journal_health = {'status': 'not_evaluated',
                                  'reason': 'CURRENT_PREFLIGHT_HEALTH_MISSING'}
                consistency = {'status': 'not_evaluated',
                               'reason': 'CURRENT_PREFLIGHT_SNAPSHOT_MISSING'}
            else:
                journal, journal_health, consistency, consistency_reasons = \
                    journal_components(book, evidence)
                observation_reasons.extend(consistency_reasons)
                if consistency.get('status') != 'evaluated':
                    observation_reasons.append(
                        consistency.get('reason') or
                        'CURRENT_JOURNAL_CAPTURE_COMPARISON_FAILED')
            authority, _ = self._authority_components(
                book, purpose=purpose, evidence=None, snapshot=snapshot)
            authority_facts = set()
            authority_readiness = set()
            for name in AUTHORITY_NAMES:
                component = authority.get(name) or {}
                if component.get('status') != 'evaluated':
                    authority_facts.add(
                        'CURRENT_' + name.upper() + '_NOT_EVALUATED:' +
                        str(component.get('reason') or 'UNKNOWN'))
                elif component.get('blocked'):
                    authority_readiness.add(
                        'CURRENT_' + name.upper() + '_BLOCKED:' +
                        str(component.get('reason') or 'UNKNOWN'))
            facts, readiness = self._scope_funds_checks(evidence)
            facts = sorted(set(facts) | authority_facts)
            readiness = sorted(set(readiness) | authority_readiness)
            check_now = self.clock()
            observation_reasons.extend(
                _public_recheck_reasons(source, check_now))
            observation_reasons.extend(capture_recheck_reasons(capture))
            try:
                current_pin = source_pin()
                recorded_pin = source.get('pin') or {}
                if (current_pin.get('status') != 'bounded'
                        or current_pin.get('truncated') is not False
                        or recorded_pin.get('status') != 'bounded'
                        or recorded_pin.get('truncated') is not False
                        or current_pin.get('digest') != recorded_pin.get('digest')
                        or current_pin.get('count') != recorded_pin.get('count')
                        or current_pin.get('root') != recorded_pin.get('root')):
                    observation_reasons.append(
                        'CURRENT_ACCOUNT_FACTS_SOURCE_PIN_CHANGED')
            except (TypeError, ValueError):
                observation_reasons.append(
                    'CURRENT_ACCOUNT_FACTS_SOURCE_PIN_INVALID')
            try:
                current_row = book.db.execute(
                    'SELECT payload FROM broker_current WHERE id=1').fetchone()
                health_row = book.db.execute(
                    'SELECT payload FROM broker_health WHERE id=1').fetchone()
                if (isinstance(journal, dict)
                        and journal.get('status') == 'evaluated'
                        and (current_row is None
                             or _sha256_text(current_row[0]) !=
                             journal.get('sha256'))):
                    observation_reasons.append(
                        'CURRENT_ACCOUNT_FACTS_JOURNAL_CHANGED')
                if (isinstance(journal_health, dict)
                        and journal_health.get('status') == 'evaluated'
                        and (health_row is None
                             or _sha256_text(health_row[0]) !=
                             journal_health.get('sha256'))):
                    observation_reasons.append(
                        'CURRENT_ACCOUNT_FACTS_HEALTH_CHANGED')
            except (sqlite3.Error, TypeError, ValueError):
                observation_reasons.append(
                    'CURRENT_ACCOUNT_FACTS_JOURNAL_UNVERIFIABLE')
            time_values = [('EVIDENCE', evidence.get('observed_at'))]
            if isinstance(capture, dict):
                time_values.append(('CAPTURE', capture.get('observed_at')))
            if isinstance(journal, dict) and journal.get('observed_at') is not None:
                time_values.append(('JOURNAL', journal.get('observed_at')))
            if isinstance(health, dict) and health.get('at') is not None:
                time_values.append(('HEALTH', health.get('at')))
            final_now = self.clock()
            original_times = []
            for label, value in time_values:
                if value is None:
                    # A missing journal/health component already carries its
                    # own reason.  It must not turn the whole assessment into
                    # an unhandled AttributeError that hides that reason.
                    continue
                try:
                    at = timestamp(value)
                except (TypeError, ValueError, OverflowError, AttributeError):
                    observation_reasons.append(
                        'CURRENT_ACCOUNT_FACTS_' + label + '_TIME_INVALID')
                    continue
                original_times.append(at)
                age = (final_now - at).total_seconds()
                if age < 0:
                    observation_reasons.append(
                        'CURRENT_ACCOUNT_FACTS_' + label + '_FUTURE')
                elif age > MAX_CURRENT_EVIDENCE_AGE_SECONDS:
                    observation_reasons.append(
                        'CURRENT_ACCOUNT_FACTS_' + label + '_STALE')
            if original_times:
                observed_value = min(original_times)
                observation_valid_from = observed_value.isoformat()
                observation_valid_until = (
                    observed_value + timedelta(
                        seconds=MAX_CURRENT_EVIDENCE_AGE_SECONDS)).isoformat()
            else:
                observation_valid_from = None
                observation_valid_until = None
                observation_reasons.append(
                    'CURRENT_ACCOUNT_FACTS_OBSERVATION_TIME_INVALID')
            observation_reasons = sorted(set(observation_reasons))
            observation_valid = not observation_reasons
            facts_qualified = observation_valid and not facts
            account_entry_readiness = facts_qualified and not readiness
            if not observation_valid:
                status = 'observation_blocked'
            elif not facts_qualified:
                status = 'facts_blocked'
            elif not account_entry_readiness:
                status = 'readiness_blocked'
            else:
                status = 'facts_ready'
            reasons = sorted(set(observation_reasons) | set(facts) |
                             set(readiness))
            evidence_account = evidence.get('account') or {}
            account = {
                'key_hash': (evidence_account.get('account_key_hash')
                             or self.account_key),
                'mask': (evidence_account.get('account_mask')
                         or '••••' + self.account_suffix),
                'suffix': self.account_suffix,
                'configured_key_hash': self.account_key,
            }
            capture_generation = capture.get('generation')
            binding = {
                'kind': 'current_account_facts_binding_v1',
                'owner_binding_id': self.binding_id,
                'account_key': self.account_key,
                'account_suffix': self.account_suffix,
                'policy_sha256': self.policy_sha256,
                'source_fingerprint': self.source_fingerprint_value,
                'capture_generation': capture_generation,
                'expected_capture_generation': self.expected_capture_generation,
                'purpose': purpose,
            }
            body = {
                'schema_version': 1,
                'kind': 'current_account_facts_assessment_v1',
                'status': status,
                'assessed_at': final_now.isoformat(),
                'observed_at': evidence.get('observed_at'),
                'observation_valid_from': observation_valid_from,
                'observation_valid_until': observation_valid_until,
                'purpose': purpose,
                'binding': binding,
                'account': account,
                'observation_valid': observation_valid,
                'facts_qualified': facts_qualified,
                'account_entry_readiness': account_entry_readiness,
                'entry_authorized': False,
                'production_ready': False,
                'facts_are_not_entry_authorization': True,
                'observation_reasons': observation_reasons,
                'facts_reasons': facts,
                'readiness_reasons': readiness,
                'reasons': reasons,
                'components': {
                    'current_evidence': {
                        'status': 'evaluated',
                        'observation_valid': evidence.get('observation_valid'),
                        'bundle_is_current': evidence.get('bundle_is_current'),
                        'blockers': list(evidence.get('blockers') or []),
                    },
                    'capture_generation': capture,
                    'source': source,
                    'journal_current': journal,
                    'journal_health': journal_health,
                    'journal_capture_consistency': consistency,
                    'risk': authority['risk'],
                    'lineage': authority['lineage'],
                    'control': authority['control'],
                    'scope_funds': {
                        'status': 'evaluated',
                        'facts_reasons': facts,
                        'readiness_reasons': readiness,
                    },
                    'formal_channel': {
                        'status': 'not_evaluated',
                        'reason': 'FACT_ASSESSMENT_EXCLUDES_FORMAL_CHANNEL',
                    },
                },
            }
            body['assessment_id'] = digest(body)
            return body
        except Exception:
            return {
                'schema_version': 1,
                'kind': 'current_account_facts_assessment_v1',
                'status': 'facts_unavailable',
                'observed_at': None,
                'purpose': purpose,
                'observation_valid': False,
                'facts_qualified': False,
                'account_entry_readiness': False,
                'entry_authorized': False,
                'production_ready': False,
                'facts_are_not_entry_authorization': True,
                'reasons': ['CURRENT_ACCOUNT_FACTS_COMPILE_FAILED'],
                'observation_reasons': ['CURRENT_ACCOUNT_FACTS_COMPILE_FAILED'],
                'facts_reasons': [],
                'readiness_reasons': [],
            }

    def validate_account_assessment(self, assessment, *, book, purpose=None):
        """Owner recheck of a cached assessment against current context.

        This uses only current binding values, bounded pin/source rechecks,
        current broker journal rows and the existing risk/lineage/control
        reader.  It never recompiles the capture/source and never treats a
        self-consistent assessment digest as current authority.
        """
        if (not isinstance(assessment, dict)
                or assessment.get('kind') !=
                'current_account_facts_assessment_v1'
                or assessment.get('schema_version') != 1
                or assessment.get('facts_are_not_entry_authorization') is not True
                or assessment.get('entry_authorized') is not False
                or assessment.get('production_ready') is not False):
            return False, 'CURRENT_ACCOUNT_FACTS_INVALID'
        supplied_id = assessment.get('assessment_id')
        if not isinstance(supplied_id, str) or digest({
                key: value for key, value in assessment.items()
                if key != 'assessment_id'}) != supplied_id:
            return False, 'CURRENT_ACCOUNT_FACTS_TAMPERED'
        binding = assessment.get('binding')
        if not isinstance(binding, dict):
            return False, 'CURRENT_ACCOUNT_FACTS_BINDING_MISSING'
        if (binding.get('kind') != 'current_account_facts_binding_v1'
                or binding.get('owner_binding_id') != self.binding_id
                or binding.get('account_key') != self.account_key
                or binding.get('account_suffix') != self.account_suffix
                or binding.get('policy_sha256') != self.policy_sha256
                or binding.get('source_fingerprint') !=
                self.source_fingerprint_value
                or (purpose is not None and binding.get('purpose') != purpose)):
            return False, 'CURRENT_ACCOUNT_FACTS_OWNER_BINDING_CHANGED'
        expected_generation = self.expected_capture_generation
        if expected_generation is not None:
            if binding.get('capture_generation') != expected_generation:
                return False, 'CURRENT_ACCOUNT_FACTS_CAPTURE_GENERATION_CHANGED'
        elif binding.get('expected_capture_generation') is not None:
            return False, 'CURRENT_ACCOUNT_FACTS_CAPTURE_GENERATION_CHANGED'
        components = assessment.get('components')
        if not isinstance(components, dict):
            return False, 'CURRENT_ACCOUNT_FACTS_COMPONENTS_MISSING'
        source = components.get('source')
        if not isinstance(source, dict):
            return False, 'CURRENT_ACCOUNT_FACTS_SOURCE_MISSING'
        recorded_pin = source.get('pin') or {}
        current_pin = source_pin()
        if (current_pin.get('status') != 'bounded'
                or current_pin.get('truncated') is not False
                or recorded_pin.get('status') != 'bounded'
                or recorded_pin.get('truncated') is not False
                or current_pin.get('digest') != recorded_pin.get('digest')
                or current_pin.get('count') != recorded_pin.get('count')
                or current_pin.get('root') != recorded_pin.get('root')):
            return False, 'CURRENT_ACCOUNT_FACTS_SOURCE_CHANGED'
        now = self.clock()
        public_reasons = _public_recheck_reasons(source, now)
        if public_reasons:
            return False, public_reasons[0]
        capture = components.get('capture_generation')
        if not isinstance(capture, dict):
            return False, 'CURRENT_ACCOUNT_FACTS_CAPTURE_MISSING'
        capture_reasons = capture_recheck_reasons(capture)
        if capture_reasons:
            return False, capture_reasons[0]
        try:
            current_row = book.db.execute(
                'SELECT payload FROM broker_current WHERE id=1').fetchone()
            health_row = book.db.execute(
                'SELECT payload FROM broker_health WHERE id=1').fetchone()
            journal = components.get('journal_current') or {}
            if (journal.get('status') == 'evaluated'
                    and (current_row is None
                         or _sha256_text(current_row[0]) !=
                         journal.get('sha256'))):
                return False, 'CURRENT_ACCOUNT_FACTS_JOURNAL_CHANGED'
            health_component = components.get('journal_health') or {}
            if (health_component.get('status') == 'evaluated'
                    and (health_row is None
                         or _sha256_text(health_row[0]) !=
                         health_component.get('sha256'))):
                return False, 'CURRENT_ACCOUNT_FACTS_HEALTH_CHANGED'
            snapshot, _, snapshot_reasons = self._current_snapshot(book)
            if snapshot_reasons:
                return False, snapshot_reasons[0]
            authority, _ = self._authority_components(
                book, purpose=purpose, evidence=None, snapshot=snapshot)
            for name in AUTHORITY_NAMES:
                if authority.get(name) != components.get(name):
                    return False, 'CURRENT_ACCOUNT_FACTS_AUTHORITY_CHANGED:' + name
        except (sqlite3.Error, KeyError, TypeError, ValueError):
            return False, 'CURRENT_ACCOUNT_FACTS_CURRENT_STATE_INVALID'
        return True, None

    def _current_snapshot(self, book):
        reasons = []
        try:
            row = book.db.execute(
                "SELECT payload FROM broker_current WHERE id=1").fetchone()
        except Exception:
            return None, None, ['CURRENT_PREFLIGHT_BROKER_CURRENT_UNAVAILABLE']
        if row is None:
            return None, None, ['CURRENT_PREFLIGHT_SNAPSHOT_MISSING']
        try:
            snapshot = json.loads(row[0])
        except (TypeError, ValueError):
            return None, None, ['CURRENT_PREFLIGHT_SNAPSHOT_INVALID']
        if not isinstance(snapshot, dict):
            return None, None, ['CURRENT_PREFLIGHT_SNAPSHOT_INVALID']
        try:
            observation_age = (book.clock() - _ts(snapshot['observed_at'])).total_seconds()
        except (KeyError, TypeError, ValueError):
            return None, None, ['CURRENT_PREFLIGHT_SNAPSHOT_TIME_INVALID']
        if observation_age < 0:
            reasons.append('CURRENT_PREFLIGHT_SNAPSHOT_FUTURE')
        elif observation_age > 30:
            reasons.append('CURRENT_PREFLIGHT_SNAPSHOT_STALE')
        if snapshot.get('collection_complete') is not True:
            reasons.append('CURRENT_PREFLIGHT_SNAPSHOT_INCOMPLETE')
        if snapshot.get('provider') != 'robinhood':
            reasons.append('CURRENT_PREFLIGHT_SNAPSHOT_PROVENANCE_INVALID')
        if snapshot.get('synthetic') is not book.simulation:
            reasons.append('CURRENT_PREFLIGHT_SNAPSHOT_SYNTHETIC_MISMATCH')
        if snapshot.get('account_key') != self.account_key:
            reasons.append('CURRENT_PREFLIGHT_SNAPSHOT_ACCOUNT_MISMATCH')
        if snapshot.get('account_mask') != '••••' + self.account_suffix:
            reasons.append('CURRENT_PREFLIGHT_SNAPSHOT_MASK_MISMATCH')
        try:
            health_row = book.db.execute(
                "SELECT payload FROM broker_health WHERE id=1").fetchone()
            health = json.loads(health_row[0]) if health_row else None
        except Exception:
            return snapshot, None, sorted(set(reasons + [
                'CURRENT_PREFLIGHT_HEALTH_UNAVAILABLE']))
        if not isinstance(health, dict):
            reasons.append('CURRENT_PREFLIGHT_HEALTH_INVALID')
            return snapshot, None, sorted(set(reasons))
        if health.get('healthy') is not True:
            reasons.append('CURRENT_PREFLIGHT_BROKER_UNHEALTHY')
        try:
            health_at = _ts(health['at'])
            health_age = (book.clock() - health_at).total_seconds()
        except (KeyError, TypeError, ValueError):
            reasons.append('CURRENT_PREFLIGHT_HEALTH_TIME_INVALID')
        else:
            if health_age < 0:
                reasons.append('CURRENT_PREFLIGHT_HEALTH_FUTURE')
            elif health_age > 30:
                reasons.append('CURRENT_PREFLIGHT_HEALTH_STALE')
            if health.get('at') != snapshot.get('observed_at'):
                reasons.append('CURRENT_PREFLIGHT_HEALTH_SNAPSHOT_MISMATCH')
            for key in ('account_key', 'account_mask'):
                if key in health and health.get(key) != snapshot.get(key):
                    reasons.append('CURRENT_PREFLIGHT_HEALTH_' + key.upper() + '_MISMATCH')
        return snapshot, health, sorted(set(reasons))

    def _source_component(self, evidence, observed_source):
        public = ((evidence.get('account_scope_applicability') or {})
                  .get('product_evidence') or {})
        return {
            'expected_fingerprint': self.source_fingerprint_value,
            'observed_fingerprint': observed_source,
            'pin': source_pin(),
            'public_manifest_sha256': public.get('manifest_sha256'),
            'public_manifest_path': public.get('manifest_path'),
            'manifest_size': public.get('manifest_size'),
            'manifest_mtime_ns': public.get('manifest_mtime_ns'),
            'public_files': [
                {'name': item.get('name'), 'path': item.get('path'),
                 'size': item.get('size'), 'mtime_ns': item.get('mtime_ns'),
                 'sha256': item.get('sha256'),
                 'retrieved_at': item.get('retrieved_at'),
                 'expires_at': item.get('expires_at')}
                for item in (public.get('sources') or [])
                if isinstance(item, dict)],
        }

    def _capture_component(self, evidence):
        capture = evidence.get('capture')
        if not isinstance(capture, dict):
            return {'status': 'not_evaluated',
                    'reason': 'CURRENT_CAPTURE_GENERATION_MISSING'}
        component = {'status': 'evaluated', **capture}
        component['expected_generation'] = self.expected_capture_generation
        return component

    def _quote_component(self, quote, plan):
        reasons = []
        if not isinstance(quote, dict):
            return {'status': 'not_evaluated',
                    'reason': 'CURRENT_PREFLIGHT_QUOTE_MISSING'}, \
                   ['CURRENT_PREFLIGHT_QUOTE_MISSING']
        if quote.get('symbol') != plan.get('symbol'):
            reasons.append('QUOTE_SYMBOL_MISMATCH')
        for key, label in (('bid_at', 'BID'), ('ask_at', 'ASK'),
                           ('observed_at', 'OBSERVED')):
            try:
                age = (self.clock() - _ts(quote[key])).total_seconds()
                if age < 0:
                    reasons.append('QUOTE_' + label + '_FUTURE')
                elif age > 15:
                    reasons.append('QUOTE_' + label + '_STALE')
            except (KeyError, TypeError, ValueError):
                reasons.append('QUOTE_' + label + '_TIME_INVALID')
        return {'status': 'evaluated', 'symbol': quote.get('symbol'),
                'observed_at': quote.get('observed_at'),
                'bid_at': quote.get('bid_at'),
                'ask_at': quote.get('ask_at'),
                'digest': digest(quote),
                'max_age_seconds': 15}, sorted(set(reasons))

    def _action_component(self, book, action):
        if not isinstance(action, dict) or not isinstance(action.get('id'), str):
            return ({'status': 'not_evaluated',
                     'reason': 'CURRENT_PREFLIGHT_ACTION_MISSING'}, None, None,
                    ['CURRENT_PREFLIGHT_ACTION_MISSING'])
        try:
            row = book.db.execute(
                'SELECT id,trade_id,kind,state,payload FROM live_actions WHERE id=?',
                (action['id'],)).fetchone()
        except Exception:
            row = None
        if row is None:
            return ({'status': 'not_evaluated',
                     'reason': 'CURRENT_PREFLIGHT_ACTION_NOT_FOUND'}, None, None,
                    ['CURRENT_PREFLIGHT_ACTION_NOT_FOUND'])
        payload = _json_object(row[4])
        if payload is None:
            return ({'status': 'not_evaluated',
                     'reason': 'CURRENT_PREFLIGHT_ACTION_PAYLOAD_INVALID'}, None, None,
                    ['CURRENT_PREFLIGHT_ACTION_PAYLOAD_INVALID'])
        plan = payload.get('plan')
        quote = payload.get('preflight_quote')
        reasons = []
        if not isinstance(plan, dict):
            reasons.append('CURRENT_PREFLIGHT_PLAN_INVALID')
        if not isinstance(quote, dict):
            reasons.append('CURRENT_PREFLIGHT_QUOTE_MISSING')
        component = {
            'status': 'evaluated',
            'id': row[0], 'trade_id': row[1], 'kind': row[2], 'state': row[3],
            'payload_sha256': _sha256_text(row[4]),
            'plan_sha256': digest(plan) if isinstance(plan, dict) else None,
            'quote_sha256': digest(quote) if isinstance(quote, dict) else None,
        }
        return component, plan, quote, sorted(set(reasons))

    def _authority_components(self, book, *, purpose, evidence, snapshot):
        reasons = []
        try:
            raw = self.authority_reader(
                book, purpose=purpose, evidence=evidence, snapshot=snapshot)
        except Exception:
            raw = {}
        components = {}
        for name in AUTHORITY_NAMES:
            component = normalize_authority_component((raw or {}).get(name))
            components[name] = component
            if component.get('status') != 'evaluated':
                reasons.append('CURRENT_' + name.upper() + '_NOT_EVALUATED:' +
                               str(component.get('reason') or 'UNKNOWN'))
            elif component.get('blocked'):
                reasons.append('CURRENT_' + name.upper() + '_BLOCKED:' +
                               str(component.get('reason') or 'UNKNOWN'))
        return components, sorted(set(reasons))

    def _assess(self, book, *, mode, snapshot, quote, plan, purpose=None,
                evidence=None, action=None, candidate_action_id=None):
        journal_snapshot = None
        health = None
        journal_parse_reasons = []
        action_component = {'status': 'not_evaluated',
                            'reason': 'ACTION_NOT_BOUND_FOR_PROPOSAL'}
        action_reasons = []
        if mode == 'claim':
            journal_snapshot, health, journal_parse_reasons = \
                self._current_snapshot(book)
            action_component, action_plan, action_quote, action_reasons = \
                self._action_component(book, action)
            if isinstance(action_plan, dict):
                plan = action_plan
            if isinstance(action_quote, dict):
                quote = action_quote
            try:
                trade = (book.state().get('trades', {})
                         .get(action_component.get('trade_id'), {}))
            except (AttributeError, KeyError, TypeError):
                trade = {}
            if purpose is None:
                purpose = trade.get('purpose')
            if evidence is None:
                evidence = trade.get('evidence')
            if not isinstance(snapshot, dict):
                snapshot = journal_snapshot or {}
        if not isinstance(snapshot, dict):
            snapshot = {}
        if not isinstance(quote, dict):
            quote = {}
        if not isinstance(plan, dict):
            plan = {}
        current, observed_source, reasons = self._base(
            book, snapshot, quote, plan)
        source = self._source_component(current, observed_source)
        capture = self._capture_component(current)
        if capture.get('status') != 'evaluated':
            reasons.append(capture.get('reason') or
                           'CURRENT_CAPTURE_GENERATION_MISSING')
        elif (self.expected_capture_generation is not None
              and capture.get('generation') != self.expected_capture_generation):
            reasons.append('CURRENT_CAPTURE_GENERATION_REPLACED')
        if mode == 'claim':
            reasons.extend(journal_parse_reasons)
            reasons.extend(action_reasons)
            if journal_snapshot is None:
                journal = {'status': 'not_evaluated',
                           'reason': (journal_parse_reasons[0]
                                      if journal_parse_reasons else
                                      'CURRENT_PREFLIGHT_SNAPSHOT_MISSING')}
                journal_health = {'status': 'not_evaluated',
                                  'reason': (journal_parse_reasons[0]
                                             if journal_parse_reasons else
                                             'CURRENT_PREFLIGHT_HEALTH_MISSING')}
                consistency = {'status': 'not_evaluated',
                               'reason': (journal_parse_reasons[0]
                                          if journal_parse_reasons else
                                          'CURRENT_PREFLIGHT_SNAPSHOT_MISSING')}
            else:
                journal, journal_health, consistency, consistency_reasons = \
                    journal_components(book, current)
                reasons.extend(consistency_reasons)
                if consistency.get('status') != 'evaluated':
                    reasons.append(consistency.get('reason') or
                                   'CURRENT_JOURNAL_CAPTURE_COMPARISON_FAILED')
            quote_component, quote_reasons = self._quote_component(quote, plan)
            reasons.extend(quote_reasons)
            if action_component.get('status') != 'evaluated':
                reasons.append(action_component.get('reason') or
                               'CURRENT_PREFLIGHT_ACTION_NOT_EVALUATED')
            snapshot_for_authority = journal_snapshot
        else:
            journal = {'status': 'not_evaluated',
                       'reason': 'BROKER_JOURNAL_NOT_BOUND_FOR_PROPOSAL'}
            journal_health = {'status': 'not_evaluated',
                              'reason': 'BROKER_JOURNAL_NOT_BOUND_FOR_PROPOSAL'}
            consistency = {'status': 'not_evaluated',
                           'reason': 'BROKER_JOURNAL_NOT_BOUND_FOR_PROPOSAL'}
            quote_component, quote_reasons = self._quote_component(quote, plan)
            reasons.extend(quote_reasons)
            snapshot_for_authority = snapshot
        authority, authority_reasons = self._authority_components(
            book, purpose=purpose, evidence=evidence,
            snapshot=snapshot_for_authority)
        reasons.extend(authority_reasons)
        authorization = authorization_context(
            book, mode=mode, action=action, purpose=purpose,
            evidence=evidence)
        if authorization.get('status') != 'evaluated':
            reasons.append('CURRENT_AUTHORIZATION_NOT_EVALUATED:' +
                           str(authorization.get('reason') or 'UNKNOWN'))
        if self.formal_authority is None:
            if book.simulation is True:
                formal_component = {
                    'status': 'not_applicable',
                    'reason': 'SYNTHETIC_FORMAL_CHANNEL_NOT_REQUIRED',
                }
            else:
                formal_component = {
                    'status': 'blocked',
                    'reason': 'FORMAL_ENGINEERING_CHANNEL_UNPROVEN',
                    'reasons': ['FORMAL_ENGINEERING_CHANNEL_UNPROVEN'],
                }
                reasons.append('FORMAL_ENGINEERING_CHANNEL_UNPROVEN')
        else:
            try:
                formal_decision = self.formal_authority.assess(
                    book=book, mode=mode, action=action,
                    snapshot=snapshot_for_authority, quote=quote, plan=plan,
                    purpose=purpose, evidence=evidence, current=current,
                    candidate_action_id=candidate_action_id)
            except Exception:
                formal_decision = {
                    'allowed': False, 'status': 'blocked',
                    'reasons': ['FORMAL_AUTHORITY_EVALUATION_FAILED'],
                }
            formal_component = component_of(formal_decision)
            if formal_decision.get('allowed') is not True:
                reasons.extend(formal_decision.get('reasons') or
                               ['FORMAL_ENGINEERING_CHANNEL_UNPROVEN'])
        components = {
            'current_evidence': {
                'status': 'evaluated',
                'observation_valid': current.get('observation_valid'),
                'bundle_is_current': current.get('bundle_is_current'),
                'blockers': list(current.get('blockers') or []),
            },
            'capture_generation': capture,
            'journal_current': journal,
            'journal_health': journal_health,
            'journal_capture_consistency': consistency,
            'action': action_component,
            'quote': quote_component,
            'risk': authority['risk'],
            'lineage': authority['lineage'],
            'control': authority['control'],
            'formal_channel': formal_component,
        }
        reasons = sorted(set(reasons))
        evidence_account = current.get('account') or {}
        receipt_account = {
            'key_hash': (evidence_account.get('account_key_hash')
                         or self.account_key),
            'mask': (evidence_account.get('account_mask')
                     or '••••' + self.account_suffix),
            'suffix': self.account_suffix,
            'configured_key_hash': self.account_key,
        }
        if receipt_account['configured_key_hash'] != receipt_account['key_hash']:
            reasons = sorted(set(reasons + ['RECEIPT_ACCOUNT_BINDING_MISMATCH']))
        components['current_evidence']['account_key_hash'] = (
            evidence_account.get('account_key_hash'))
        components['current_evidence']['account_mask'] = (
            evidence_account.get('account_mask'))
        receipt = build_receipt(
            mode=mode, simulation=book.simulation,
            owner_binding_id=self.binding_id,
            account=receipt_account,
            policy_sha256=self.policy_sha256,
            source=source, capture=capture, journal=journal,
            journal_health=journal_health,
            action=action_component, quote=quote_component,
            components=components, reasons=reasons,
            assessed_at=self.clock().isoformat(),
            authorization=authorization)
        return {'allowed': not reasons, 'reasons': reasons,
                'evidence': current, 'receipt': receipt,
                'entry_authorized': False, 'components': components}

    def check_proposal(self, book, snapshot, quote, plan, *, purpose, evidence,
                       candidate_action_id=None):
        if purpose != 'engineering':
            return self._block('ENGINEERING_ONLY_PREFLIGHT')
        try:
            return self._assess(book, mode='proposal', snapshot=snapshot,
                                quote=quote, plan=plan, purpose=purpose,
                                evidence=evidence,
                                candidate_action_id=candidate_action_id)
        except Exception:
            return self._block('CURRENT_EVIDENCE_COMPILE_FAILED')

    def check_claim(self, book, action):
        try:
            return self._assess(book, mode='claim', snapshot=None,
                                quote=None, plan={}, action=action)
        except Exception:
            return self._block('CURRENT_PREFLIGHT_COMPILE_FAILED')


class NoCurrentPreflight:
    """Synthetic compatibility only; real books are always blocked."""

    def _current_snapshot(self, book):
        try:
            row = book.db.execute(
                "SELECT payload FROM broker_current WHERE id=1").fetchone()
        except Exception:
            return None, 'CURRENT_PREFLIGHT_BROKER_CURRENT_UNAVAILABLE'
        if row is None:
            return None, 'CURRENT_PREFLIGHT_SNAPSHOT_MISSING'
        try:
            snapshot = json.loads(row[0])
        except (TypeError, ValueError):
            return None, 'CURRENT_PREFLIGHT_SNAPSHOT_INVALID'
        if not isinstance(snapshot, dict):
            return None, 'CURRENT_PREFLIGHT_SNAPSHOT_INVALID'
        try:
            observation_age = (book.clock() - _ts(snapshot['observed_at'])).total_seconds()
        except (KeyError, TypeError, ValueError):
            return None, 'CURRENT_PREFLIGHT_SNAPSHOT_TIME_INVALID'
        if not 0 <= observation_age <= 30:
            return None, 'CURRENT_PREFLIGHT_SNAPSHOT_STALE'
        if snapshot.get('collection_complete') is not True:
            return None, 'CURRENT_PREFLIGHT_SNAPSHOT_INCOMPLETE'
        try:
            health_row = book.db.execute(
                "SELECT payload FROM broker_health WHERE id=1").fetchone()
            health = json.loads(health_row[0]) if health_row else None
        except Exception:
            return None, 'CURRENT_PREFLIGHT_HEALTH_UNAVAILABLE'
        if not isinstance(health, dict) or health.get('healthy') is not True:
            return None, 'CURRENT_PREFLIGHT_BROKER_UNHEALTHY'
        return snapshot, None

    def check_proposal(self, book, snapshot, quote, plan, *, purpose, evidence,
                       candidate_action_id=None):
        if book.simulation is True:
            return {'allowed': True, 'reasons': [],
                    'reason': 'SYNTHETIC_TEST_PREFLIGHT_ONLY',
                    'entry_authorized': False,
                    'components': {'current_evidence':
                                   {'status': 'not_evaluated',
                                    'reason': 'SYNTHETIC_TEST_PREFLIGHT_ONLY'}}}
        return {'allowed': False,
                'reasons': ['CURRENT_PREFLIGHT_CONTEXT_MISSING'],
                'entry_authorized': False,
                'components': {'current_evidence':
                               {'status': 'not_evaluated',
                                'reason': 'CURRENT_PREFLIGHT_CONTEXT_MISSING'}}}

    def check_claim(self, book, action):
        if book.simulation is True:
            return {'allowed': True, 'reasons': [],
                    'reason': 'SYNTHETIC_TEST_PREFLIGHT_ONLY',
                    'entry_authorized': False,
                    'components': {'current_evidence':
                                   {'status': 'not_evaluated',
                                    'reason': 'SYNTHETIC_TEST_PREFLIGHT_ONLY'}}}
        return {'allowed': False,
                'reasons': ['CURRENT_PREFLIGHT_CONTEXT_MISSING'],
                'entry_authorized': False,
                'components': {'current_evidence':
                               {'status': 'not_evaluated',
                                'reason': 'CURRENT_PREFLIGHT_CONTEXT_MISSING'}}}


def _ts(value):
    from datetime import datetime as dt
    return dt.fromisoformat(value)


def _json_object(value):
    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        return None
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else None
