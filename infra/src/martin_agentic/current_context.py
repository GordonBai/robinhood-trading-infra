"""Owner-side current-capture receiver/factory for production composition.

The capture producer is external and is not implemented in this repository.
What is implemented here is the minimal, reviewable receiver: a private
registry file names one immutable capture generation and its public-product
manifest.  Only that owner-selected generation is bound; strategy payloads
cannot choose an arbitrary capture root or execution database.

If the registry is absent/invalid, the default real path returns a structured
``MissingCurrentContext`` and the real entry remains denied.  This is not a
claim that the production producer/full chain exists.
"""
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat

from trading_contracts.values import digest, timestamp

from .current_evidence import (
    MAX_CURRENT_EVIDENCE_AGE_SECONDS, capture_generation,
    load_public_product_evidence)
from .assessment_receipt import capture_recheck_reasons
from .engineering_preflight import (
    CurrentEngineeringPreflight, NoCurrentPreflight)
from .qualification import candidate_root, source_fingerprint, source_pin

ROOT = candidate_root()
DEFAULT_CAPTURE_ROOT = ROOT / 'runtime' / 'current-capture'
DEFAULT_PUBLIC_ROOT = ROOT / 'runtime' / 'public-product-sources'
DEFAULT_REGISTRY_PATH = DEFAULT_CAPTURE_ROOT / 'registry.json'
REGISTRY_SCHEMA_VERSION = 1
REGISTRY_KIND = 'current_capture_registry_v1'
MAX_REGISTRY_BYTES = 64 * 1024
SHA_RE = re.compile(r'^[0-9a-f]{64}$')
SUFFIX_RE = re.compile(r'^\d{4}$')


class CurrentContextError(ValueError):
    def __init__(self, code, detail=None):
        self.code = code
        self.detail = detail
        super().__init__(code)

    def __str__(self):
        return self.code if self.detail is None else f'{self.code}:{self.detail}'


def _fail(code, detail=None):
    raise CurrentContextError(code, detail)


def _read_registry_bytes(path, limit=MAX_REGISTRY_BYTES):
    path = Path(path)
    if path.is_symlink() or not path.is_file():
        _fail('CURRENT_CONTEXT_REGISTRY_MISSING')
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError:
        _fail('CURRENT_CONTEXT_REGISTRY_MISSING')
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size > limit:
            _fail('CURRENT_CONTEXT_REGISTRY_INVALID')
        if stat.S_IMODE(info.st_mode) & 0o077:
            _fail('CURRENT_CONTEXT_REGISTRY_PRIVATE_REQUIRED')
        data = os.read(fd, limit + 1)
        if len(data) != info.st_size or len(data) > limit:
            _fail('CURRENT_CONTEXT_REGISTRY_CHANGED')
        return data
    finally:
        os.close(fd)


def _under(path, roots, code):
    try:
        resolved = Path(path).resolve()
    except OSError:
        _fail(code)
    for root in roots:
        root = Path(root).resolve()
        if resolved == root or root in resolved.parents:
            return resolved
    _fail(code)


class CurrentContextRegistry:
    """Private owner registry; one immutable capture generation per publication."""

    REQUIRED = frozenset({
        'schema_version', 'kind', 'generation_id', 'capture_root',
        'capture_manifest_sha256', 'raw_chain_sha256', 'public_manifest',
        'public_manifest_sha256', 'account_key_hash', 'account_suffix',
        'policy_sha256', 'source_fingerprint', 'published_at', 'producer',
    })

    def __init__(self, path=DEFAULT_REGISTRY_PATH, *, allowed_capture_roots=None,
                 allowed_public_roots=None, clock=None):
        self.path = Path(path)
        self.allowed_capture_roots = tuple(
            allowed_capture_roots or (DEFAULT_CAPTURE_ROOT,))
        self.allowed_public_roots = tuple(
            allowed_public_roots or (DEFAULT_PUBLIC_ROOT,))
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def read(self):
        data = _read_registry_bytes(self.path)
        registry_sha = hashlib.sha256(data).hexdigest()
        try:
            record = json.loads(data)
        except (TypeError, ValueError):
            _fail('CURRENT_CONTEXT_REGISTRY_INVALID')
        if not isinstance(record, dict) or set(record) != self.REQUIRED:
            _fail('CURRENT_CONTEXT_REGISTRY_SCHEMA')
        if record.get('schema_version') != REGISTRY_SCHEMA_VERSION \
                or record.get('kind') != REGISTRY_KIND:
            _fail('CURRENT_CONTEXT_REGISTRY_SCHEMA')
        for key in ('generation_id', 'capture_manifest_sha256', 'raw_chain_sha256',
                    'public_manifest_sha256', 'account_key_hash', 'policy_sha256',
                    'source_fingerprint'):
            if not isinstance(record.get(key), str) or not SHA_RE.fullmatch(record[key]):
                _fail('CURRENT_CONTEXT_REGISTRY_HASH_INVALID', key)
        if not isinstance(record.get('account_suffix'), str) \
                or not SUFFIX_RE.fullmatch(record['account_suffix']):
            _fail('CURRENT_CONTEXT_REGISTRY_ACCOUNT_SUFFIX')
        if not isinstance(record.get('producer'), str) or not record['producer']:
            _fail('CURRENT_CONTEXT_REGISTRY_PRODUCER')
        capture_root = record.get('capture_root')
        public_manifest = record.get('public_manifest')
        if not isinstance(capture_root, str) or not Path(capture_root).is_absolute():
            _fail('CURRENT_CONTEXT_CAPTURE_ROOT_INVALID')
        if not isinstance(public_manifest, str) or not Path(public_manifest).is_absolute():
            _fail('CURRENT_CONTEXT_PUBLIC_MANIFEST_INVALID')
        resolved_capture = _under(
            capture_root, self.allowed_capture_roots,
            'CURRENT_CONTEXT_CAPTURE_ROOT_DENIED')
        if not resolved_capture.is_dir() or resolved_capture.is_symlink():
            _fail('CURRENT_CONTEXT_CAPTURE_ROOT_INVALID')
        resolved_public = _under(
            public_manifest, self.allowed_public_roots,
            'CURRENT_CONTEXT_PUBLIC_MANIFEST_DENIED')
        if not resolved_public.is_file() or resolved_public.is_symlink():
            _fail('CURRENT_CONTEXT_PUBLIC_MANIFEST_INVALID')
        try:
            published = timestamp(record['published_at'])
            now = self.clock()
            age = (now - published).total_seconds()
        except (TypeError, ValueError):
            _fail('CURRENT_CONTEXT_PUBLISHED_AT_INVALID')
        if age < 0:
            _fail('CURRENT_CONTEXT_REGISTRY_FUTURE')
        if age > MAX_CURRENT_EVIDENCE_AGE_SECONDS:
            _fail('CURRENT_CONTEXT_REGISTRY_STALE')
        result = dict(record)
        result['capture_root'] = str(resolved_capture)
        result['public_manifest'] = str(resolved_public)
        result['_registry_sha256'] = registry_sha
        return result


class MissingCurrentContext:
    """Structured NOT_EVALUATED boundary; never grants entry."""

    binding_id = None
    authority_reader = None
    factory_owned = True

    def __init__(self, reason):
        self.reason = reason or 'CURRENT_CONTEXT_MISSING'

    def _decision(self):
        reason = self.reason
        components = {name: {'status': 'not_evaluated', 'reason': reason}
                      for name in ('current_evidence', 'capture_generation',
                                   'journal_current', 'journal_health',
                                   'journal_capture_consistency', 'action',
                                   'quote', 'risk', 'lineage', 'control',
                                   'formal_channel')}
        return {'allowed': False,
                'reasons': ['CURRENT_PREFLIGHT_CONTEXT_MISSING'],
                'detail': reason,
                'components': components,
                'entry_authorized': False, 'receipt': None}

    def check_proposal(self, book, snapshot, quote, plan, *, purpose, evidence,
                       candidate_action_id=None):
        return self._decision()

    def check_claim(self, book, action):
        return self._decision()


def factory_from_current_capture_config(policy, *, clock=None,
                                        formal_authority=None):
    """Build the owner receiver for the project-local capture configuration.

    The producer and receiver share one root and one local public manifest.
    A missing entry keeps the legacy default receiver; malformed entries fail
    closed through that receiver instead of silently naming another source.
    """
    config = policy.get('current_capture') if isinstance(policy, dict) else None
    if not isinstance(config, dict):
        return None
    root = config.get('root')
    public_manifest = config.get('public_manifest')
    if not isinstance(root, str) or not root \
            or not isinstance(public_manifest, str) or not public_manifest:
        return None
    root_path = Path(root)
    public_path = Path(public_manifest)
    registry = CurrentContextRegistry(
        root_path / 'registry.json',
        allowed_capture_roots=(root_path,),
        allowed_public_roots=(public_path.parent,),
        clock=clock)
    return CurrentContextFactory(registry=registry, clock=clock,
                                 formal_authority=formal_authority)


class CurrentContextFactory:
    """Calls the private registry once and builds the owner-bound preflight."""

    def __init__(self, registry=None, *, clock=None, authority_reader=None,
                 formal_authority=None):
        self.registry = registry or CurrentContextRegistry(clock=clock)
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.authority_reader = authority_reader
        self.formal_authority = formal_authority
        self.calls = 0
        self.last_error = None
        self.last_evidence = None
        self.last_context = None

    def __call__(self, book):
        self.calls += 1
        self.last_error = None
        self.last_evidence = None
        self.last_context = None
        try:
            record = self.registry.read()
            capture = capture_generation(record['capture_root'])
            if capture.get('generation') != record['generation_id']:
                _fail('CURRENT_CONTEXT_GENERATION_MISMATCH')
            if capture.get('manifest_sha256') != record['capture_manifest_sha256']:
                _fail('CURRENT_CONTEXT_CAPTURE_MANIFEST_MISMATCH')
            if capture.get('raw_chain_sha256') != record['raw_chain_sha256']:
                _fail('CURRENT_CONTEXT_RAW_CHAIN_MISMATCH')
            try:
                observed = timestamp(capture['observed_at'])
                now = self.clock()
                age = (now - observed).total_seconds()
            except (KeyError, TypeError, ValueError):
                _fail('CURRENT_CONTEXT_CAPTURE_TIME_INVALID')
            if age < 0:
                _fail('CURRENT_CONTEXT_CAPTURE_FUTURE')
            if age > MAX_CURRENT_EVIDENCE_AGE_SECONDS:
                _fail('CURRENT_CONTEXT_CAPTURE_STALE')
            public = load_public_product_evidence(
                record['public_manifest'], now=now)
            if public.get('status') != 'valid':
                _fail('CURRENT_CONTEXT_PUBLIC_INVALID',
                      ','.join(public.get('blockers') or ['UNKNOWN']))
            if public.get('manifest_sha256') != record['public_manifest_sha256']:
                _fail('CURRENT_CONTEXT_PUBLIC_MANIFEST_MISMATCH')
            if source_fingerprint() != record['source_fingerprint']:
                _fail('CURRENT_CONTEXT_SOURCE_CHANGED')
            if digest(book.policy) != record['policy_sha256']:
                _fail('CURRENT_CONTEXT_POLICY_MISMATCH')
            if book.policy.get('account_suffix') != record['account_suffix']:
                _fail('CURRENT_CONTEXT_ACCOUNT_SUFFIX_MISMATCH')
            known_account = book.state().get('account_key')
            if known_account is not None and known_account != record['account_key_hash']:
                _fail('CURRENT_CONTEXT_ACCOUNT_MISMATCH')
            source_pin_value = source_pin()
            if source_pin_value.get('status') != 'bounded' \
                    or source_pin_value.get('truncated') is not False:
                _fail('CURRENT_CONTEXT_SOURCE_PIN_LIMIT',
                      str(source_pin_value.get('reason') or 'NOT_COMPLETE'))
            preflight = CurrentEngineeringPreflight(
                record['capture_root'], record['public_manifest'],
                account_key=record['account_key_hash'],
                account_suffix=record['account_suffix'],
                policy_sha256=record['policy_sha256'],
                source_fingerprint_value=record['source_fingerprint'],
                clock=self.clock, authority_reader=self.authority_reader,
                expected_capture_generation=record['generation_id'],
                registry_sha256=record['_registry_sha256'],
                registry_path=self.registry.path,
                formal_authority=self.formal_authority)
            preflight.factory_owned = True
            preflight.context_capture = dict(capture)
            preflight.context_capture['expected_generation'] = record['generation_id']
            preflight.context_observed_at = capture['observed_at']
            preflight.context_published_at = record['published_at']
            preflight.context_source_pin = source_pin_value
            self.last_evidence = {
                'registry_sha256': record['_registry_sha256'],
                'generation_id': record['generation_id'],
                'capture_generation': capture['generation'],
                'capture_manifest_sha256': capture['manifest_sha256'],
                'raw_chain_sha256': capture['raw_chain_sha256'],
                'public_manifest_sha256': public['manifest_sha256'],
                'account_key_hash': record['account_key_hash'],
                'account_suffix': record['account_suffix'],
                'policy_sha256': record['policy_sha256'],
                'source_fingerprint': record['source_fingerprint'],
                'producer': record['producer'],
            }
            self.last_context = preflight
            return preflight
        except (CurrentContextError, OSError, KeyError, TypeError, ValueError,
                sqlite3.Error) as exc:
            self.last_error = getattr(exc, 'code', None) or str(exc)
            context = MissingCurrentContext(self.last_error)
            self.last_context = context
            return context


class CurrentContextRefresher:
    """Factory-owned rolling refresh; never replaces an explicitly injected context."""

    def __init__(self, book, *, factory, clock=None):
        self.book = book
        self.factory = factory
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.last_pointer_sha = None
        self.last_status = None

    def _status(self, status, *, pointer_sha, **extra):
        value = {
            'status': status,
            'generation_id': getattr(self.book._current_preflight,
                                    'expected_capture_generation', None),
            'registry_sha256': pointer_sha,
            'factory_calls': self.factory.calls,
            'assessment': 'deferred_to_entry_preflight',
            'producer': (self.factory.last_evidence or {}).get('producer'),
        }
        value.update(extra)
        self.last_status = value
        return value

    def _missing(self, reason, *, pointer_sha):
        context = MissingCurrentContext(reason)
        context.factory_owned = True
        self.book._current_preflight = context
        self.last_pointer_sha = pointer_sha
        return self._status('missing', pointer_sha=pointer_sha,
                            reason=reason, error=self.factory.last_error)

    def _same_pointer_reasons(self, current):
        reasons = []
        now = self.clock()
        for label, value in (
                ('CURRENT_CONTEXT_CAPTURE', current.context_observed_at),
                ('CURRENT_CONTEXT_PUBLICATION', current.context_published_at)):
            try:
                at = timestamp(value)
                age = (now - at).total_seconds()
            except (TypeError, ValueError):
                reasons.append(label + '_TIME_INVALID')
                continue
            if age < 0:
                reasons.append(label + '_FUTURE')
            elif age > MAX_CURRENT_EVIDENCE_AGE_SECONDS:
                reasons.append(label + '_STALE')
        if digest(self.book.policy) != current.policy_sha256:
            reasons.append('CURRENT_CONTEXT_POLICY_CHANGED')
        try:
            known = self.book.state().get('account_key')
        except Exception:
            known = None
        if known is not None and known != current.account_key:
            reasons.append('CURRENT_CONTEXT_ACCOUNT_CHANGED')
        current_pin = source_pin()
        recorded_pin = current.context_source_pin or {}
        if (current_pin.get('status') != 'bounded'
                or current_pin.get('truncated') is not False
                or recorded_pin.get('status') != 'bounded'
                or recorded_pin.get('truncated') is not False):
            reasons.append('CURRENT_CONTEXT_SOURCE_PIN_LIMIT:' +
                           str(current_pin.get('reason')
                               or recorded_pin.get('reason') or 'NOT_COMPLETE'))
        elif (current_pin.get('digest') != recorded_pin.get('digest')
              or current_pin.get('count') != recorded_pin.get('count')
              or current_pin.get('root') != recorded_pin.get('root')):
            reasons.append('CURRENT_CONTEXT_SOURCE_CHANGED')
        reasons.extend(capture_recheck_reasons(current.context_capture))
        return sorted(set(reasons))

    def refresh(self):
        current = getattr(self.book, '_current_preflight', None)
        if current is None or not getattr(current, 'factory_owned', False):
            return self._status('explicit_context', pointer_sha=None)
        try:
            raw = _read_registry_bytes(self.factory.registry.path)
            pointer_sha = hashlib.sha256(raw).hexdigest()
        except (CurrentContextError, OSError, ValueError) as exc:
            return self._missing(
                getattr(exc, 'code', None) or str(exc), pointer_sha=None)
        if (isinstance(current, CurrentEngineeringPreflight)
                and current.registry_sha256 == pointer_sha):
            cached_missing = getattr(self, '_cached_missing_sha', None)
            if cached_missing == pointer_sha:
                return self._status(
                    'missing', pointer_sha=pointer_sha,
                    reason=getattr(self.book._current_preflight, 'reason',
                                   'CURRENT_CONTEXT_CACHE_INVALID'))
            reasons = self._same_pointer_reasons(current)
            if reasons:
                self._cached_missing_sha = pointer_sha
                return self._missing(','.join(reasons),
                                     pointer_sha=pointer_sha)
            return self._status('bound', pointer_sha=pointer_sha,
                                cached=True)
        if (isinstance(current, MissingCurrentContext)
                and self.last_pointer_sha == pointer_sha):
            return self._status('missing', pointer_sha=pointer_sha,
                                reason=getattr(current, 'reason',
                                               'CURRENT_CONTEXT_MISSING'))
        context = self.factory(self.book)
        self.book._current_preflight = context
        self.last_pointer_sha = pointer_sha
        self._cached_missing_sha = (pointer_sha
                                    if isinstance(context, MissingCurrentContext)
                                    else None)
        return self._status(
            'bound' if isinstance(context, CurrentEngineeringPreflight)
            else 'missing',
            pointer_sha=pointer_sha,
            reason=None if isinstance(context, CurrentEngineeringPreflight)
            else self.factory.last_error,
            evidence=self.factory.last_evidence)


def default_current_preflight(book, *, factory=None, clock=None,
                              formal_authority=None):
    """Default owner path used when no preflight was injected.

    Synthetic simulation keeps the legacy compatibility object.  A real book
    gets the private registry receiver; missing producer/publication remains a
    structured missing context and never an allowed entry.
    """
    if book.simulation is True:
        return NoCurrentPreflight(), {
            'status': 'synthetic_default',
            'factory_calls': 0,
            'reason': 'SYNTHETIC_COMPATIBILITY_ONLY',
        }
    factory = factory or factory_from_current_capture_config(
        book.policy, clock=clock, formal_authority=formal_authority) \
        or CurrentContextFactory(clock=clock,
                                 formal_authority=formal_authority)
    context = factory(book)
    status = {
        'status': ('bound' if isinstance(context, CurrentEngineeringPreflight)
                   else 'missing'),
        'factory_calls': factory.calls,
        'evidence': factory.last_evidence,
        'error': factory.last_error,
    }
    return context, status


def ensure_default_preflight(book, *, factory=None, clock=None,
                             formal_authority=None):
    """Attach/refresh the default receiver only where no explicit context was injected."""
    formal_authority = (formal_authority
                        or getattr(book, 'formal_authority', None))
    current = getattr(book, '_current_preflight', None)
    if current is not None and not isinstance(current, NoCurrentPreflight):
        if not getattr(current, 'factory_owned', False):
            book._current_context_refresher = None
            book._current_context_factory = None
            if (formal_authority is not None
                    and getattr(current, 'formal_authority', None) is None):
                current.formal_authority = formal_authority
            return current, {'status': 'injected', 'factory_calls': 0}
        refresher = getattr(book, '_current_context_refresher', None)
        if refresher is None:
            factory = factory or getattr(book, '_current_context_factory', None) \
                or factory_from_current_capture_config(
                    book.policy, clock=clock,
                    formal_authority=formal_authority) \
                or CurrentContextFactory(
                    clock=clock, formal_authority=formal_authority)
            refresher = CurrentContextRefresher(
                book, factory=factory, clock=clock)
            book._current_context_refresher = refresher
            book._current_context_factory = factory
        status = refresher.refresh()
        return book._current_preflight, status
    if book.simulation is True:
        context = NoCurrentPreflight()
        book._current_preflight = context
        return context, {'status': 'synthetic_default', 'factory_calls': 0}
    factory = factory or factory_from_current_capture_config(
        book.policy, clock=clock, formal_authority=formal_authority) \
        or CurrentContextFactory(clock=clock,
                                 formal_authority=formal_authority)
    context = factory(book)
    book._current_preflight = context
    book._current_context_factory = factory
    if getattr(context, 'factory_owned', False):
        refresher = CurrentContextRefresher(book, factory=factory, clock=clock)
        book._current_context_refresher = refresher
    status = {
        'status': ('bound' if isinstance(context, CurrentEngineeringPreflight)
                   else 'missing'),
        'factory_calls': factory.calls,
        'evidence': factory.last_evidence,
        'error': factory.last_error,
        'generation_id': getattr(context, 'expected_capture_generation', None),
    }
    return context, status
