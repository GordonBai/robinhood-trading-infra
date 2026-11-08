"""Scope-aware, durable entry authority.

The historical baseline matcher proves only order identity.  It is not an
account-wide flat/settlement proof and must never authorize a new entry.  This
module reads the *durable* baseline namespace in the execution database and
returns a fail-closed decision for every new entry path.  Revoking an
identity-only baseline does not remove that durable fact.

Production `simulation=false` has no local boolean/dict/policy override.  A
current full baseline is not an entry permit: this authority reads an
owner-produced, versioned current-account facts assessment when available and
distinguishes facts-missing, facts-blocked, and facts-ready-but-permit-missing.
It remains fail-closed and never creates a production permit.  The existing
`simulation=true` synthetic mode and the tests-only fixture are isolated test
paths; they cannot authorize a real write.  Reads, reconciliation, native
protection, cancellation of protection, exits and management of an existing
position do not call this gate.
"""
from datetime import datetime, timezone
import json
import sqlite3

from trading_contracts.values import digest

IDENTITY_SCOPE = 'terminal_equity_order_identity_v1'
FULL_SCOPE = 'full_account_baseline_v1'


class EntryAuthority:
    """Interface used by ExecutionBook and Host entry paths."""

    def _supervised_baseline_problem(self, marker=None):
        """Check a durable APPLIED full baseline before a supervised permit.

        A marker is an inhibit anchor, never a grant.  When the supervised
        permit bridge is enabled, the full-marker/full-baseline path must find
        the exact APPLIED manifest, validate its hash/scope and bind its
        account/policy to the current book.  Revoked/unknown/orphan markers,
        missing or corrupt manifest rows and expired manifests stay blocked.
        """
        current = self._book_binding()
        if current is None:
            return 'ENTRY_AUTHORITY_BASELINE_BOOK_BINDING'
        rows, error, has_schema = self._baseline_rows()
        if error:
            return error
        if not has_schema:
            return 'ENTRY_AUTHORITY_BASELINE_MISSING'
        target = marker.get('baseline_id') if isinstance(marker, dict) else None
        if marker is not None:
            reason = marker.get('reason')
            if reason == 'BASELINE_REVOKED':
                return 'ENTRY_AUTHORITY_BASELINE_REVOKED'
            if reason != 'BASELINE_APPLIED':
                return 'ENTRY_AUTHORITY_BASELINE_MARKER_UNKNOWN'
            if not isinstance(target, str) or not target:
                return 'ENTRY_AUTHORITY_BASELINE_MARKER_ORPHAN'
        saw_target = False
        for baseline_id, status, pin, encoded in rows or ():
            if target is not None and baseline_id != target:
                continue
            if target is not None:
                saw_target = True
            if status != 'APPLIED':
                if target is not None:
                    return 'ENTRY_AUTHORITY_BASELINE_NOT_APPLIED'
                continue
            try:
                manifest = json.loads(encoded)
            except (TypeError, ValueError):
                return 'ENTRY_AUTHORITY_BASELINE_TAMPERED'
            if (not isinstance(manifest, dict)
                    or manifest.get('kind') != 'historical_baseline_manifest'
                    or manifest.get('baseline_id') != baseline_id
                    or manifest.get('manifest_sha256') != pin
                    or digest({key: value for key, value in manifest.items()
                               if key != 'manifest_sha256'}) != pin):
                return 'ENTRY_AUTHORITY_BASELINE_TAMPERED'
            if manifest.get('baseline_scope') != FULL_SCOPE:
                if target is not None:
                    return 'ENTRY_AUTHORITY_BASELINE_SCOPE'
                continue
            if (manifest.get('account_key') != current['account_key']
                    or manifest.get('policy_sha256') != current['policy_hash']):
                return 'ENTRY_AUTHORITY_BASELINE_BINDING'
            try:
                expires = datetime.fromisoformat(manifest.get('expires_at'))
                if expires.tzinfo is None:
                    return 'ENTRY_AUTHORITY_BASELINE_EXPIRED'
                expires = expires.astimezone(timezone.utc)
                if self.clock().astimezone(timezone.utc) > expires:
                    return 'ENTRY_AUTHORITY_BASELINE_EXPIRED'
            except (TypeError, ValueError, OverflowError):
                return 'ENTRY_AUTHORITY_BASELINE_EXPIRED'
            return None
        if target is not None:
            return ('ENTRY_AUTHORITY_BASELINE_MARKER_ORPHAN'
                    if not saw_target else 'ENTRY_AUTHORITY_BASELINE_MISSING')
        return 'ENTRY_AUTHORITY_BASELINE_MISSING'

    def check(self, *, source, snapshot=None, action=None, purpose=None):
        raise NotImplementedError

    def require_entry(self, *, source, snapshot=None, action=None, purpose=None):
        decision = self.check(source=source, snapshot=snapshot, action=action,
                              purpose=purpose)
        if decision.get('allowed') is not True:
            reason = decision.get('reason') or 'ENTRY_AUTHORITY_BLOCKED'
            raise ValueError('ENTRY_AUTHORITY_BLOCKED:' + reason)
        return decision


class DurableEntryAuthority(EntryAuthority):
    """Fail-closed authority backed by the execution DB's baseline history.

    A missing/empty/partial baseline namespace is never an authorization.  A
    durable marker outside the baseline schema also remembers that a baseline
    was once applied, so dropping/recovering the baseline tables cannot reopen
    entries.  Every read error is converted into a structured blocked reason.
    """

    MARKER_TABLE = 'entry_authority_marker'
    MAX_FACTS_AGE_SECONDS = 30

    def __init__(self, db, *, clock=None, allow_synthetic_no_namespace=False,
                 current_facts_reader=None, current_facts_validator=None,
                 supervised_permit_validator=None,
                 supervised_baseline_required=False):
        self.db = db
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.allow_synthetic_no_namespace = allow_synthetic_no_namespace
        self.current_facts_reader = current_facts_reader
        self.current_facts_validator = current_facts_validator
        self.supervised_permit_validator = supervised_permit_validator
        self.supervised_baseline_required = supervised_baseline_required is True

    def _book_binding(self):
        try:
            row = self.db.execute(
                'SELECT payload FROM live_state WHERE id=1').fetchone()
            state = json.loads(row[0]) if row else None
        except (sqlite3.Error, TypeError, ValueError):
            return None
        if not isinstance(state, dict):
            return None
        account_key = state.get('account_key')
        policy_hash = state.get('policy_hash')
        if not isinstance(account_key, str) or not account_key:
            return None
        return {'account_key': account_key, 'policy_hash': policy_hash}

    def _facts_for(self, purpose=None):
        if not callable(self.current_facts_reader):
            return None, 'CURRENT_FULL_PREFLIGHT_MISSING'
        try:
            value = self.current_facts_reader(purpose=purpose)
        except Exception:
            return None, 'CURRENT_ACCOUNT_FACTS_READER_FAILED'
        if value is None:
            return None, 'ENTRY_AUTHORITY_CURRENT_FACTS_MISSING'
        return value, None

    def _facts_denial(self, purpose=None, action=None, source=None):
        assessment, error = self._facts_for(purpose)
        if error:
            return {'allowed': False, 'reason': error, 'source': 'current_facts',
                    'facts_reason': ('ENTRY_AUTHORITY_CURRENT_FACTS_MISSING'
                                     if error == 'CURRENT_FULL_PREFLIGHT_MISSING'
                                     else error),
                    'facts_ready': False, 'permit_missing': False}
        if (not isinstance(assessment, dict)
                or assessment.get('kind') !=
                'current_account_facts_assessment_v1'
                or assessment.get('schema_version') != 1
                or assessment.get('facts_are_not_entry_authorization') is not True
                or assessment.get('entry_authorized') is not False
                or assessment.get('production_ready') is not False
                or not isinstance(assessment.get('assessment_id'), str)):
            return {'allowed': False,
                    'reason': 'ENTRY_AUTHORITY_CURRENT_FACTS_UNTRUSTED',
                    'source': 'current_facts', 'facts_ready': False,
                    'permit_missing': False}
        supplied_id = assessment.get('assessment_id')
        if digest({key: value for key, value in assessment.items()
                   if key != 'assessment_id'}) != supplied_id:
            return {'allowed': False,
                    'reason': 'ENTRY_AUTHORITY_CURRENT_FACTS_UNTRUSTED',
                    'source': 'current_facts', 'facts_ready': False,
                    'permit_missing': False}
        binding = assessment.get('binding')
        current = self._book_binding()
        if (not isinstance(binding, dict)
                or current is None
                or binding.get('account_key') != current['account_key']
                or binding.get('policy_sha256') != current['policy_hash']
                or (purpose is not None
                    and binding.get('purpose') != purpose)):
            return {'allowed': False,
                    'reason': 'ENTRY_AUTHORITY_CURRENT_FACTS_UNTRUSTED',
                    'source': 'current_facts', 'facts_ready': False,
                    'permit_missing': False}
        try:
            assessed = datetime.fromisoformat(assessment.get('assessed_at'))
            observed = datetime.fromisoformat(assessment.get('observed_at'))
            valid_until = datetime.fromisoformat(
                assessment.get('observation_valid_until'))
            if any(item.tzinfo is None for item in (assessed, observed, valid_until)):
                raise ValueError('naive')
            assessed = assessed.astimezone(timezone.utc)
            observed = observed.astimezone(timezone.utc)
            valid_until = valid_until.astimezone(timezone.utc)
            now = self.clock().astimezone(timezone.utc)
        except (TypeError, ValueError, OverflowError):
            return {'allowed': False,
                    'reason': 'ENTRY_AUTHORITY_CURRENT_FACTS_STALE',
                    'source': 'current_facts', 'facts_ready': False,
                    'permit_missing': False}
        if (observed > valid_until
                or observed > assessed
                or assessed > valid_until
                or assessed > now
                or now < observed
                or now > valid_until):
            return {'allowed': False,
                    'reason': 'ENTRY_AUTHORITY_CURRENT_FACTS_STALE',
                    'source': 'current_facts', 'facts_ready': False,
                    'permit_missing': False}
        if not callable(self.current_facts_validator):
            return {'allowed': False,
                    'reason': 'ENTRY_AUTHORITY_CURRENT_FACTS_UNTRUSTED',
                    'source': 'current_facts', 'facts_ready': False,
                    'permit_missing': False,
                    'validator_reason': 'CURRENT_ACCOUNT_FACTS_VALIDATOR_MISSING'}
        try:
            valid, validator_reason = self.current_facts_validator(
                assessment, purpose=purpose)
        except Exception:
            valid, validator_reason = False, 'CURRENT_ACCOUNT_FACTS_VALIDATOR_FAILED'
        if valid is not True:
            return {'allowed': False,
                    'reason': 'ENTRY_AUTHORITY_CURRENT_FACTS_UNTRUSTED',
                    'source': 'current_facts', 'facts_ready': False,
                    'permit_missing': False,
                    'validator_reason': validator_reason}
        # The owner validator may itself perform bounded authority reads.  It
        # can consume the original 30s window, so re-read the same trusted
        # clock and reject stale/future/reversed state after it returns.
        try:
            assessed_after = datetime.fromisoformat(
                assessment.get('assessed_at'))
            observed_after = datetime.fromisoformat(
                assessment.get('observed_at'))
            valid_after = datetime.fromisoformat(
                assessment.get('observation_valid_until'))
            if any(item.tzinfo is None for item in
                   (assessed_after, observed_after, valid_after)):
                raise ValueError('naive')
            assessed_after = assessed_after.astimezone(timezone.utc)
            observed_after = observed_after.astimezone(timezone.utc)
            valid_after = valid_after.astimezone(timezone.utc)
            now_after = self.clock().astimezone(timezone.utc)
        except (TypeError, ValueError, OverflowError):
            return {'allowed': False,
                    'reason': 'ENTRY_AUTHORITY_CURRENT_FACTS_STALE',
                    'source': 'current_facts', 'facts_ready': False,
                    'permit_missing': False}
        if (observed_after > valid_after
                or observed_after > assessed_after
                or assessed_after > valid_after
                or assessed_after > now_after
                or now_after < observed_after
                or now_after > valid_after):
            return {'allowed': False,
                    'reason': 'ENTRY_AUTHORITY_CURRENT_FACTS_STALE',
                    'source': 'current_facts', 'facts_ready': False,
                    'permit_missing': False}
        observation_valid = assessment.get('observation_valid')
        facts_qualified = assessment.get('facts_qualified')
        readiness = assessment.get('account_entry_readiness')
        if (type(observation_valid) is not bool
                or type(facts_qualified) is not bool
                or type(readiness) is not bool
                or (facts_qualified and not observation_valid)
                or (readiness and not facts_qualified)):
            return {'allowed': False,
                    'reason': 'ENTRY_AUTHORITY_CURRENT_FACTS_UNTRUSTED',
                    'source': 'current_facts', 'facts_ready': False,
                    'permit_missing': False}
        base = {
            'allowed': False, 'source': 'current_facts',
            'facts_ready': False, 'permit_missing': False,
            'entry_authorized': False, 'production_ready': False,
            'observation_valid': observation_valid,
            'facts_qualified': facts_qualified,
            'account_entry_readiness': readiness,
            'assessment_id': supplied_id,
        }
        if not observation_valid:
            base['reason'] = 'ENTRY_AUTHORITY_CURRENT_FACTS_BLOCKED'
            base['reasons'] = list(assessment.get('observation_reasons') or
                                   assessment.get('reasons') or [])
            return base
        if not facts_qualified:
            base['reason'] = 'ENTRY_AUTHORITY_CURRENT_FACTS_BLOCKED'
            base['reasons'] = list(assessment.get('facts_reasons') or
                                   assessment.get('reasons') or [])
            return base
        if not readiness:
            base['reason'] = 'ENTRY_AUTHORITY_CURRENT_FACTS_BLOCKED'
            base['reasons'] = list(assessment.get('readiness_reasons') or
                                   assessment.get('reasons') or [])
            return base
        base.update({'reason': 'ENTRY_AUTHORITY_CURRENT_FACTS_READY_PERMIT_MISSING',
                     'facts_ready': True, 'permit_missing': True})
        if action is None or not callable(self.supervised_permit_validator):
            return base
        try:
            permit = self.supervised_permit_validator(
                action=action, purpose=purpose, source=source)
        except Exception as exc:
            base.update({'reason': 'ENTRY_AUTHORITY_SUPERVISED_PERMIT_UNAVAILABLE',
                         'supervised_permit_reason': getattr(exc, 'code', None)
                         or type(exc).__name__})
            return base
        if not isinstance(permit, dict) or permit.get('allowed') is not True:
            base.update({'reason': (permit or {}).get(
                'reason') or 'ENTRY_AUTHORITY_SUPERVISED_PERMIT_BLOCKED',
                'supervised_permit_reason': (permit or {}).get('reason')})
            return base
        base.update({'allowed': True,
                     'reason': permit.get('reason') or
                     'SUPERVISED_ENGINEERING_PERMIT_VERIFIED',
                     'permit_missing': False, 'permit_verified': True,
                     'permit_id': permit.get('permit_id'),
                     'permit_binding': permit.get('binding'),
                     'permit_cached': permit.get('cached') is True,
                     'supervised_engineering': True,
                     'entry_authorized': False, 'production_ready': False})
        return base

    def _table_exists(self, name):
        return self.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (name,)).fetchone() is not None

    def _marker(self):
        try:
            exists = self._table_exists(self.MARKER_TABLE)
        except sqlite3.Error:
            return None, 'ENTRY_AUTHORITY_LEDGER_UNAVAILABLE'
        if not exists:
            return None, 'ABSENT'
        try:
            rows = self.db.execute(
                'SELECT required,reason,baseline_id,baseline_scope,created_at '
                'FROM entry_authority_marker').fetchall()
        except sqlite3.Error:
            return None, 'ENTRY_AUTHORITY_MARKER_INVALID'
        if len(rows) != 1:
            return None, 'ENTRY_AUTHORITY_MARKER_INVALID'
        required, reason, baseline_id, scope, created = rows[0]
        if required != 1 or not isinstance(reason, str) or not reason \
                or not isinstance(created, str):
            return None, 'ENTRY_AUTHORITY_MARKER_INVALID'
        return {'required': required, 'reason': reason,
                'baseline_id': baseline_id, 'baseline_scope': scope,
                'created_at': created}, 'PRESENT'

    def _baseline_rows(self):
        try:
            exists = self._table_exists('historical_baseline_manifest')
        except sqlite3.Error:
            return None, 'ENTRY_AUTHORITY_LEDGER_UNAVAILABLE', False
        if not exists:
            return [], None, False
        try:
            rows = self.db.execute(
                'SELECT baseline_id,status,manifest_sha256,manifest_json '
                'FROM historical_baseline_manifest ORDER BY sequence').fetchall()
        except sqlite3.Error:
            return None, 'ENTRY_AUTHORITY_LEDGER_INVALID', True
        return rows, None, True

    def _supervised_baseline_problem(self, marker=None):
        """Check a durable APPLIED full baseline before a supervised permit.

        A marker is an inhibit anchor, never a grant.  When the supervised
        permit bridge is enabled, the full-marker/full-baseline path must find
        the exact APPLIED manifest, validate its hash/scope and bind its
        account/policy to the current book.  Revoked/unknown/orphan markers,
        missing or corrupt manifest rows and expired manifests stay blocked.
        """
        current = self._book_binding()
        if current is None:
            return 'ENTRY_AUTHORITY_BASELINE_BOOK_BINDING'
        rows, error, has_schema = self._baseline_rows()
        if error:
            return error
        if not has_schema:
            return 'ENTRY_AUTHORITY_BASELINE_MISSING'
        target = marker.get('baseline_id') if isinstance(marker, dict) else None
        if marker is not None:
            reason = marker.get('reason')
            if reason == 'BASELINE_REVOKED':
                return 'ENTRY_AUTHORITY_BASELINE_REVOKED'
            if reason != 'BASELINE_APPLIED':
                return 'ENTRY_AUTHORITY_BASELINE_MARKER_UNKNOWN'
            if not isinstance(target, str) or not target:
                return 'ENTRY_AUTHORITY_BASELINE_MARKER_ORPHAN'
        saw_target = False
        for baseline_id, status, pin, encoded in rows or ():
            if target is not None and baseline_id != target:
                continue
            if target is not None:
                saw_target = True
            if status != 'APPLIED':
                if target is not None:
                    return 'ENTRY_AUTHORITY_BASELINE_NOT_APPLIED'
                continue
            try:
                manifest = json.loads(encoded)
            except (TypeError, ValueError):
                return 'ENTRY_AUTHORITY_BASELINE_TAMPERED'
            if (not isinstance(manifest, dict)
                    or manifest.get('kind') != 'historical_baseline_manifest'
                    or manifest.get('baseline_id') != baseline_id
                    or manifest.get('manifest_sha256') != pin
                    or digest({key: value for key, value in manifest.items()
                               if key != 'manifest_sha256'}) != pin):
                return 'ENTRY_AUTHORITY_BASELINE_TAMPERED'
            if manifest.get('baseline_scope') != FULL_SCOPE:
                if target is not None:
                    return 'ENTRY_AUTHORITY_BASELINE_SCOPE'
                continue
            if (manifest.get('account_key') != current['account_key']
                    or manifest.get('policy_sha256') != current['policy_hash']):
                return 'ENTRY_AUTHORITY_BASELINE_BINDING'
            try:
                expires = datetime.fromisoformat(manifest.get('expires_at'))
                if expires.tzinfo is None:
                    return 'ENTRY_AUTHORITY_BASELINE_EXPIRED'
                expires = expires.astimezone(timezone.utc)
                if self.clock().astimezone(timezone.utc) > expires:
                    return 'ENTRY_AUTHORITY_BASELINE_EXPIRED'
            except (TypeError, ValueError, OverflowError):
                return 'ENTRY_AUTHORITY_BASELINE_EXPIRED'
            return None
        if target is not None:
            return ('ENTRY_AUTHORITY_BASELINE_MARKER_ORPHAN'
                    if not saw_target else 'ENTRY_AUTHORITY_BASELINE_MISSING')
        return 'ENTRY_AUTHORITY_BASELINE_MISSING'

    def check(self, *, source, snapshot=None, action=None, purpose=None):
        marker, marker_state = self._marker()
        if marker_state not in {'ABSENT', 'PRESENT'}:
            return {'allowed': False, 'reason': marker_state, 'source': source}
        if marker_state == 'PRESENT':
            scope = marker.get('baseline_scope')
            reason = marker.get('reason') or ''
            if scope == IDENTITY_SCOPE:
                return {'allowed': False,
                        'reason': ('ENTRY_AUTHORITY_IDENTITY_ONLY_REVOKED'
                                   if reason == 'BASELINE_REVOKED'
                                   else 'ENTRY_AUTHORITY_IDENTITY_ONLY'),
                        'source': source, 'marker': marker}
            if scope == FULL_SCOPE:
                if self.supervised_baseline_required:
                    problem = self._supervised_baseline_problem(marker)
                    if problem is not None:
                        return {'allowed': False, 'reason': problem,
                                'source': source, 'marker': marker}
                result = self._facts_denial(purpose=purpose, action=action, source=source)
                result.update({'source': source, 'marker': marker})
                return result
            if scope is None:
                return {'allowed': False,
                        'reason': 'ENTRY_AUTHORITY_LEGACY_FULL_PREFLIGHT_MISSING',
                        'source': source, 'marker': marker}
            return {'allowed': False,
                    'reason': 'ENTRY_AUTHORITY_MARKER_PRESENT',
                    'source': source, 'marker': marker}
        rows, error, has_schema = self._baseline_rows()
        if error:
            return {'allowed': False, 'reason': error, 'source': source}
        identity = []
        full = []
        legacy = False
        unknown = False
        tampered = False
        for baseline_id, status, pin, encoded in rows or ():
            try:
                manifest = json.loads(encoded)
            except (TypeError, ValueError):
                tampered = True
                continue
            if not isinstance(manifest, dict):
                tampered = True
                continue
            if manifest.get('manifest_sha256') != pin \
                    or digest({key: value for key, value in manifest.items()
                               if key != 'manifest_sha256'}) != pin \
                    or manifest.get('baseline_id') != baseline_id:
                tampered = True
                continue
            scope = manifest.get('baseline_scope')
            if scope == IDENTITY_SCOPE:
                identity.append((baseline_id, status, manifest))
            elif scope == FULL_SCOPE:
                if status == 'APPLIED':
                    full.append((baseline_id, status, manifest))
            elif scope is None:
                legacy = True
            else:
                unknown = True
        if tampered:
            return {'allowed': False,
                    'reason': 'ENTRY_AUTHORITY_BASELINE_TAMPERED',
                    'source': source}
        if unknown:
            return {'allowed': False,
                    'reason': 'ENTRY_AUTHORITY_SCOPE_UNKNOWN',
                    'source': source}
        if identity:
            return {'allowed': False,
                    'reason': ('ENTRY_AUTHORITY_IDENTITY_ONLY_REVOKED'
                               if all(status == 'REVOKED'
                                      for _, status, _ in identity)
                               else 'ENTRY_AUTHORITY_IDENTITY_ONLY'),
                    'source': source,
                    'identity_baselines': [item[0] for item in identity]}
        if full:
            if self.supervised_baseline_required:
                problem = self._supervised_baseline_problem(None)
                if problem is not None:
                    return {'allowed': False, 'reason': problem, 'source': source,
                            'full_baselines': [item[0] for item in full]}
            result = self._facts_denial(purpose=purpose, action=action, source=source)
            result.update({'source': source,
                           'full_baselines': [item[0] for item in full]})
            return result
        if legacy:
            return {'allowed': False,
                    'reason': 'ENTRY_AUTHORITY_LEGACY_FULL_PREFLIGHT_MISSING',
                    'source': source}
        if not has_schema:
            if self.allow_synthetic_no_namespace:
                return {'allowed': True,
                        'reason': 'SYNTHETIC_TEST_ENTRY_AUTHORITY_ONLY',
                        'source': source}
            return {'allowed': False,
                    'reason': 'ENTRY_AUTHORITY_NAMESPACE_ABSENT',
                    'source': source}
        return {'allowed': False, 'reason': 'NO_FULL_ENTRY_AUTHORITY',
                'source': source}
