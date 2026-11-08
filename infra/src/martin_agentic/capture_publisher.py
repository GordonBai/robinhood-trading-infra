"""Owner-side current-capture producer and atomic publication.

The producer consumes the existing bounded read-only collector
(``collect_saved_bundle``) or an already completed collector output, validates
the same raw bundle/public evidence through the existing current-evidence
compiler, copies it into an immutable private generation, and atomically
replaces the owner registry pointer.

It never opens a broker, market, Host, execution journal, risk sidecar, or
network.  ``producer`` is local metadata, never a broker signature.  A valid
supported-coverage observation may be publishable even when the strict full
baseline remains unresolved; missing full/settlement evidence is never
reported as a complete flat account.
"""
from datetime import datetime, timezone
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import stat
import threading
import time
import uuid

from trading_contracts.values import canonical, digest, timestamp

from .assessment_receipt import _journal_consistency, capture_recheck_reasons
from .broker_bundle import CollectorError, collect_saved_bundle
from .current_context import (
    REGISTRY_KIND, REGISTRY_SCHEMA_VERSION, _read_registry_bytes)
from .current_evidence import (
    MAX_CURRENT_EVIDENCE_AGE_SECONDS, PUBLIC_SOURCE_MAX_AGE,
    capture_generation, compile_current_evidence,
    load_public_product_evidence)
from .qualification import source_fingerprint, source_pin


MAX_PUBLISH_FILES = 10000
MAX_PUBLISH_BYTES = 96 * 1024 * 1024
MAX_FAILURE_EVIDENCE = 50
PUBLICATION_LOCK_SECONDS = 5.0
PRUNE_MAX_ENTRIES = 10000
PRUNE_MAX_SECONDS = 1.0

_ROOT_LOCKS = {}
_ROOT_LOCKS_GUARD = threading.Lock()


def _root_lock(root):
    key = str(Path(root).resolve())
    with _ROOT_LOCKS_GUARD:
        return _ROOT_LOCKS.setdefault(key, threading.Lock())


@contextmanager
def _writer_lock(root, timeout=PUBLICATION_LOCK_SECONDS):
    """Finite single-writer constraint for commit/prune.

    The thread lock covers same-process publishers; ``flock`` covers separate
    processes. The lock is released automatically on process exit. A busy lock
    returns PUBLICATION_WRITER_BUSY rather than waiting indefinitely.
    """
    root = Path(root)
    lock_path = root / '.publish.lock'
    if lock_path.is_symlink():
        _fail('PUBLICATION_LOCK_INVALID')
    try:
        descriptor = os.open(
            lock_path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    except OSError:
        _fail('PUBLICATION_LOCK_UNAVAILABLE')
    deadline = time.monotonic() + timeout
    thread_lock = _root_lock(root)
    acquired = False
    try:
        remaining = max(0.0, deadline - time.monotonic())
        if not thread_lock.acquire(timeout=remaining):
            _fail('PUBLICATION_WRITER_BUSY')
        acquired = True
        while True:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    _fail('PUBLICATION_WRITER_BUSY')
                time.sleep(0.01)
        yield
    finally:
        if acquired:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            except OSError:
                pass
        try:
            os.close(descriptor)
        finally:
            if acquired:
                thread_lock.release()


class PublicationError(ValueError):
    def __init__(self, code, detail=None):
        self.code = code
        self.detail = detail
        super().__init__(code)

    def __str__(self):
        return self.code if self.detail is None else f'{self.code}:{self.detail}'


def _fail(code, detail=None):
    raise PublicationError(code, detail)


def _now_utc(value):
    if not isinstance(value, datetime) or value.tzinfo is None \
            or value.utcoffset() is None:
        _fail('PUBLICATION_CLOCK_INVALID')
    return value.astimezone(timezone.utc)


def _private_dir(path):
    path = Path(path)
    if path.is_symlink():
        _fail('PUBLICATION_PRIVATE_DIRECTORY_REQUIRED')
    if not path.exists():
        path.mkdir(mode=0o700)
    if not path.is_dir() or stat.S_IMODE(path.stat().st_mode) & 0o077:
        _fail('PUBLICATION_PRIVATE_DIRECTORY_REQUIRED')
    return path


def _fsync_dir(path):
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_private(path, data):
    path = Path(path)
    descriptor = os.open(
        path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        view = memoryview(data)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                _fail('PUBLICATION_WRITE_FAILED', str(path))
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _read_private_file(path, limit):
    path = Path(path)
    if path.is_symlink() or not path.is_file():
        _fail('PUBLICATION_SOURCE_INVALID', str(path))
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_size > limit:
            _fail('PUBLICATION_SOURCE_INVALID', str(path))
        data = os.read(descriptor, limit + 1)
        if len(data) != info.st_size or len(data) > limit:
            _fail('PUBLICATION_SOURCE_CHANGED', str(path))
        return data
    finally:
        os.close(descriptor)


class CapturePublisher:
    """Validate and publish one immutable current-capture generation."""

    def __init__(self, root, public_manifest, *, policy, account_key,
                 account_suffix, source_fingerprint_value, clock=None,
                 retention=3):
        self.root = Path(root)
        self.public_manifest = Path(public_manifest)
        self.policy = policy
        self.policy_sha256 = digest(policy)
        self.account_key = account_key
        self.account_suffix = account_suffix
        self.source_fingerprint_value = source_fingerprint_value
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.retention = max(1, int(retention))
        if not isinstance(account_suffix, str) or len(account_suffix) != 4 \
                or not account_suffix.isdigit():
            _fail('PUBLICATION_ACCOUNT_SUFFIX_INVALID')
        if not isinstance(source_fingerprint_value, str) \
                or len(source_fingerprint_value) != 64:
            _fail('PUBLICATION_SOURCE_FINGERPRINT_INVALID')

    @property
    def registry_path(self):
        return self.root / 'registry.json'

    def _ensure_layout(self):
        self.root = _private_dir(self.root)
        return {
            'generations': _private_dir(self.root / 'generations'),
            'staging': _private_dir(self.root / 'staging'),
            'failures': _private_dir(self.root / 'failures'),
        }

    def _public(self, now):
        public = load_public_product_evidence(self.public_manifest, now=now)
        if public.get('status') != 'valid':
            _fail('PUBLICATION_PUBLIC_SOURCE_INVALID',
                  ','.join(public.get('blockers') or ['UNKNOWN']))
        return public

    def _validate_bundle(self, bundle_root, now, public, journal_snapshot=None):
        capture = capture_generation(bundle_root)
        try:
            observed = timestamp(capture['observed_at'])
        except (KeyError, TypeError, ValueError):
            _fail('PUBLICATION_CAPTURE_TIME_INVALID')
        age = (now - observed).total_seconds()
        if age < 0:
            _fail('PUBLICATION_CAPTURE_FUTURE')
        if age > MAX_CURRENT_EVIDENCE_AGE_SECONDS:
            _fail('PUBLICATION_CAPTURE_STALE')
        evidence = compile_current_evidence(
            bundle_root, self.public_manifest, now=now)
        if evidence.get('observation_valid') is not True:
            _fail('PUBLICATION_OBSERVATION_INVALID',
                  ','.join(evidence.get('blockers') or ['UNKNOWN']))
        if evidence.get('bundle_is_current') is not True:
            _fail('PUBLICATION_OBSERVATION_NOT_CURRENT')
        account = evidence.get('account') or {}
        if account.get('account_key_hash') != self.account_key:
            _fail('PUBLICATION_ACCOUNT_MISMATCH')
        if account.get('account_mask') != '••••' + self.account_suffix:
            _fail('PUBLICATION_ACCOUNT_MASK_MISMATCH')
        if self._policy_reasons():
            _fail('PUBLICATION_POLICY_CHANGED')
        if source_fingerprint() != self.source_fingerprint_value:
            _fail('PUBLICATION_SOURCE_CHANGED')
        scope = (evidence.get('account_scope_applicability') or {})
        if (scope.get('product_evidence') or {}).get('manifest_sha256') != \
                public['manifest_sha256']:
            _fail('PUBLICATION_PUBLIC_MANIFEST_MISMATCH')
        association = None
        if journal_snapshot is not None:
            if not isinstance(journal_snapshot, dict):
                _fail('PUBLICATION_JOURNAL_SNAPSHOT_INVALID')
            if journal_snapshot.get('observed_at') != capture['observed_at']:
                _fail('PUBLICATION_JOURNAL_OBSERVATION_MISMATCH')
            health = {'healthy': True, 'at': journal_snapshot.get('observed_at')}
            reasons, compared = _journal_consistency(
                evidence, journal_snapshot, health)
            if reasons:
                _fail('PUBLICATION_JOURNAL_MISMATCH', ','.join(reasons))
            association = {
                'schema_version': 1,
                'kind': 'capture_journal_generation_association_v1',
                'generation_id': capture['generation'],
                'journal_observed_at': journal_snapshot.get('observed_at'),
                'journal_sha256': digest(journal_snapshot),
                'compared': compared,
                'local_semantic_association_only': True,
                'broker_signature': False,
            }
        return capture, evidence, association

    def _policy_sha256(self):
        return self.policy_sha256

    def _policy_reasons(self):
        return ([] if digest(self.policy) == self.policy_sha256
                else ['PUBLICATION_POLICY_CHANGED'])

    def _persisted_association(self, root, expected, generation_id):
        path = Path(root) / 'journal-generation.json'
        if not path.exists():
            if expected is not None:
                _fail('PUBLICATION_ASSOCIATION_MISSING')
            return None
        try:
            data = _read_private_file(path, 64 * 1024)
            value = json.loads(data)
        except (OSError, TypeError, ValueError):
            _fail('PUBLICATION_ASSOCIATION_INVALID')
        if not isinstance(value, dict) or value.get('generation_id') != generation_id:
            _fail('PUBLICATION_ASSOCIATION_INVALID')
        if expected is not None and canonical(value) != canonical(expected):
            _fail('PUBLICATION_ASSOCIATION_CONFLICT')
        return value

    def _validate_commit(self, root, expected_capture, *, now,
                         expected_association, expected_public_sha256=None):
        public = self._public(now)
        if (expected_public_sha256 is not None
                and public.get('manifest_sha256') != expected_public_sha256):
            _fail('PUBLICATION_PUBLIC_MANIFEST_CHANGED')
        capture = capture_generation(root)
        if (capture.get('generation') != expected_capture.get('generation')
                or capture.get('manifest_sha256')
                != expected_capture.get('manifest_sha256')
                or capture.get('raw_chain_sha256')
                != expected_capture.get('raw_chain_sha256')):
            _fail('PUBLICATION_GENERATION_CHANGED')
        try:
            observed = timestamp(capture['observed_at'])
        except (KeyError, TypeError, ValueError):
            _fail('PUBLICATION_CAPTURE_TIME_INVALID')
        age = (now - observed).total_seconds()
        if age < 0:
            _fail('PUBLICATION_CAPTURE_FUTURE')
        if age > MAX_CURRENT_EVIDENCE_AGE_SECONDS:
            _fail('PUBLICATION_COMMIT_STALE')
        if self._policy_reasons():
            _fail('PUBLICATION_POLICY_CHANGED')
        if source_fingerprint() != self.source_fingerprint_value:
            _fail('PUBLICATION_SOURCE_CHANGED')
        evidence = compile_current_evidence(root, self.public_manifest, now=now)
        if evidence.get('observation_valid') is not True:
            _fail('PUBLICATION_OBSERVATION_INVALID',
                  ','.join(evidence.get('blockers') or ['UNKNOWN']))
        if evidence.get('bundle_is_current') is not True:
            _fail('PUBLICATION_OBSERVATION_NOT_CURRENT')
        account = evidence.get('account') or {}
        if account.get('account_key_hash') != self.account_key:
            _fail('PUBLICATION_ACCOUNT_MISMATCH')
        if account.get('account_mask') != '••••' + self.account_suffix:
            _fail('PUBLICATION_ACCOUNT_MASK_MISMATCH')
        product = ((evidence.get('account_scope_applicability') or {})
                   .get('product_evidence') or {})
        if product.get('manifest_sha256') != public.get('manifest_sha256'):
            _fail('PUBLICATION_PUBLIC_MANIFEST_MISMATCH')
        pin = source_pin()
        if pin.get('status') != 'bounded' or pin.get('truncated') is not False:
            _fail('PUBLICATION_SOURCE_PIN_LIMIT',
                  str(pin.get('reason') or 'NOT_COMPLETE'))
        association = self._persisted_association(
            root, expected_association, capture['generation'])
        return capture, public, evidence, association, pin


    def _public_manifest_sha256(self, now):
        return self._public(now)['manifest_sha256']

    def _copy_bundle(self, source, destination):
        source = Path(source)
        destination = Path(destination)
        if destination.exists() or destination.is_symlink():
            _fail('PUBLICATION_STAGING_EXISTS')
        destination.mkdir(mode=0o700)
        files = 0
        total = 0
        try:
            for entry in os.scandir(source):
                if entry.is_symlink():
                    _fail('PUBLICATION_BUNDLE_SYMLINK')
                if entry.is_dir(follow_symlinks=False):
                    if entry.name != 'raw':
                        _fail('PUBLICATION_BUNDLE_EXTRA', entry.name)
                    target = destination / 'raw'
                    target.mkdir(mode=0o700)
                    for raw in os.scandir(entry.path):
                        files += 1
                        if files > MAX_PUBLISH_FILES:
                            _fail('PUBLICATION_FILE_LIMIT')
                        if raw.is_symlink() or not raw.is_file(follow_symlinks=False) \
                                or not raw.name.endswith('.raw'):
                            _fail('PUBLICATION_BUNDLE_EXTRA', raw.name)
                        data = _read_private_file(raw.path, MAX_PUBLISH_BYTES)
                        total += len(data)
                        if total > MAX_PUBLISH_BYTES:
                            _fail('PUBLICATION_BYTES_LIMIT')
                        _write_private(target / raw.name, data)
                    _fsync_dir(target)
                elif entry.is_file(follow_symlinks=False):
                    if entry.name not in {'manifest.json', 'capture-provenance.json'}:
                        _fail('PUBLICATION_BUNDLE_EXTRA', entry.name)
                    data = _read_private_file(entry.path, MAX_PUBLISH_BYTES)
                    total += len(data)
                    if total > MAX_PUBLISH_BYTES:
                        _fail('PUBLICATION_BYTES_LIMIT')
                    _write_private(destination / entry.name, data)
                else:
                    _fail('PUBLICATION_BUNDLE_EXTRA', entry.name)
            _fsync_dir(destination)
            return files, total
        except BaseException:
            # The caller keeps the original collector output as evidence.
            raise

    def _existing_registry(self):
        try:
            raw = _read_registry_bytes(self.registry_path)
        except Exception:
            return None
        try:
            record = json.loads(raw)
        except (TypeError, ValueError):
            return None
        return record if isinstance(record, dict) else None

    def _record(self, capture, public, now, producer, final_dir, association):
        return {
            'schema_version': REGISTRY_SCHEMA_VERSION,
            'kind': REGISTRY_KIND,
            'generation_id': capture['generation'],
            'capture_root': str(final_dir.resolve()),
            'capture_manifest_sha256': capture['manifest_sha256'],
            'raw_chain_sha256': capture['raw_chain_sha256'],
            'public_manifest': str(self.public_manifest.resolve()),
            'public_manifest_sha256': public['manifest_sha256'],
            'account_key_hash': self.account_key,
            'account_suffix': self.account_suffix,
            'policy_sha256': self._policy_sha256(),
            'source_fingerprint': self.source_fingerprint_value,
            'published_at': now.isoformat(),
            'producer': str(producer),
        }

    def _public_cheap_reasons(self, public, now):
        reasons = []
        sources = public.get('sources') or []
        if not sources:
            return ['PUBLICATION_PUBLIC_SOURCE_MISSING']
        for item in sources:
            name = str(item.get('name'))
            path = item.get('path')
            if not isinstance(path, str) or not path:
                reasons.append('PUBLICATION_PUBLIC_SOURCE_MISSING:' + name)
                continue
            try:
                info = os.stat(path, follow_symlinks=False)
            except OSError:
                reasons.append('PUBLICATION_PUBLIC_SOURCE_MISSING:' + name)
                continue
            if (info.st_size != item.get('size')
                    or info.st_mtime_ns != item.get('mtime_ns')):
                reasons.append('PUBLICATION_PUBLIC_SOURCE_CHANGED:' + name)
            try:
                retrieved = timestamp(item['retrieved_at'])
                expires = timestamp(item['expires_at'])
            except (KeyError, TypeError, ValueError):
                reasons.append('PUBLICATION_PUBLIC_SOURCE_TIME_INVALID:' + name)
                continue
            if now < retrieved:
                reasons.append('PUBLICATION_PUBLIC_SOURCE_FUTURE:' + name)
            if now - retrieved > PUBLIC_SOURCE_MAX_AGE:
                reasons.append('PUBLICATION_PUBLIC_SOURCE_EXPIRED:' + name)
            if now > expires or expires < retrieved:
                reasons.append('PUBLICATION_PUBLIC_SOURCE_EXPIRED:' + name)
        return sorted(set(reasons))

    def _final_validity(self, capture, public, expected_source_pin, now):
        """Cheap last owner-clock/pin check after all heavy preparation."""
        reasons = []
        try:
            observed = timestamp(capture['observed_at'])
            age = (now - observed).total_seconds()
        except (KeyError, TypeError, ValueError):
            reasons.append('PUBLICATION_CAPTURE_TIME_INVALID')
        else:
            if age < 0:
                reasons.append('PUBLICATION_CAPTURE_FUTURE')
            elif age > MAX_CURRENT_EVIDENCE_AGE_SECONDS:
                reasons.append('PUBLICATION_CURRENT_STALE')
        reasons.extend(capture_recheck_reasons(capture))
        reasons.extend(self._public_cheap_reasons(public, now))
        current_pin = source_pin()
        if (current_pin.get('status') != 'bounded'
                or current_pin.get('truncated') is not False
                or expected_source_pin.get('status') != 'bounded'
                or expected_source_pin.get('truncated') is not False):
            reasons.append('PUBLICATION_SOURCE_PIN_LIMIT')
        elif (current_pin.get('digest') != expected_source_pin.get('digest')
              or current_pin.get('count') != expected_source_pin.get('count')
              or current_pin.get('root') != expected_source_pin.get('root')):
            reasons.append('PUBLICATION_SOURCE_CHANGED')
        if self._policy_reasons():
            reasons.append('PUBLICATION_POLICY_CHANGED')
        return sorted(set(reasons))

    def _validity(self, reasons, *, checked_at, committed_at_valid):
        return {
            'observation_valid': not reasons,
            'current_evidence_valid': not reasons,
            'committed_at_valid': bool(committed_at_valid),
            'reasons': list(reasons),
            'checked_at': checked_at.isoformat(),
        }

    def _publish_record(self, commit):
        """Commit one prepared generation under the root writer lock.

        ``commit`` is one positional mapping so the existing bounded probe can
        pause the call without changing its public interface. Same-generation
        retry, orphan recovery and a new commit share one final cheap
        owner-clock/pin check after all compile/fsync work and immediately
        before the pointer replace.
        """
        record = commit['record']
        final_dir = commit['final_dir']
        expected_capture = commit['expected_capture']
        expected_association = commit['expected_association']
        recovered = commit.get('recovered', False)
        producer = commit.get('producer', 'owner-collector-v1')
        expected_public_sha256 = commit.get('expected_public_sha256')
        layout = self._ensure_layout()
        with _writer_lock(self.root):
            existing = self._existing_registry()
            if existing is not None:
                if existing.get('generation_id') == record.get('generation_id'):
                    comparable = ('capture_root', 'capture_manifest_sha256',
                                  'raw_chain_sha256', 'public_manifest',
                                  'public_manifest_sha256', 'account_key_hash',
                                  'account_suffix', 'policy_sha256',
                                  'source_fingerprint')
                    if any(existing.get(key) != record.get(key)
                           for key in comparable):
                        _fail('PUBLICATION_GENERATION_CONFLICT')
                    if not final_dir.is_dir() or final_dir.is_symlink():
                        _fail('PUBLICATION_REGISTRY_GENERATION_CORRUPT')
                    try:
                        current = capture_generation(final_dir)
                    except Exception:
                        _fail('PUBLICATION_REGISTRY_GENERATION_CORRUPT')
                    if (current.get('generation') != record['generation_id']
                            or current.get('manifest_sha256')
                            != record['capture_manifest_sha256']
                            or current.get('raw_chain_sha256')
                            != record['raw_chain_sha256']):
                        _fail('PUBLICATION_REGISTRY_GENERATION_CORRUPT')
                    persisted = self._persisted_association(
                        final_dir, expected_association,
                        record['generation_id'])
                    existing_sha = self._existing_registry_sha()
                    try:
                        capture, public, _evidence, persisted, pin = \
                            self._validate_commit(
                                final_dir, expected_capture,
                                now=_now_utc(self.clock()),
                                expected_association=expected_association,
                                expected_public_sha256=expected_public_sha256)
                    except PublicationError as exc:
                        if str(getattr(exc, 'code', '')).startswith(
                                'PUBLICATION_ASSOCIATION'):
                            raise
                        checked = _now_utc(self.clock())
                        return {
                            'status': 'ALREADY_PUBLISHED',
                            'registry_sha256': existing_sha,
                            'record': existing,
                            'association': persisted,
                            'validity': self._validity(
                                [getattr(exc, 'code', None) or str(exc)],
                                checked_at=checked, committed_at_valid=True),
                            'context': {'capture': current, 'public': None,
                                        'source_pin': None},
                        }
                    checked = _now_utc(self.clock())
                    reasons = self._final_validity(capture, public, pin, checked)
                    return {
                        'status': 'ALREADY_PUBLISHED',
                        'registry_sha256': existing_sha,
                        'record': existing,
                        'association': persisted,
                        'validity': self._validity(
                            reasons, checked_at=checked,
                            committed_at_valid=True),
                        'context': {'capture': capture, 'public': public,
                                    'source_pin': pin},
                    }
                try:
                    current_capture = capture_generation(existing['capture_root'])
                    current_observed = timestamp(current_capture['observed_at'])
                except Exception:
                    _fail('PUBLICATION_CURRENT_GENERATION_INVALID')
                if current_observed > timestamp(expected_capture['observed_at']):
                    _fail('PUBLICATION_STALE_ORPHAN')
            capture, public, _evidence, persisted, pin = self._validate_commit(
                final_dir, expected_capture, now=_now_utc(self.clock()),
                expected_association=expected_association,
                expected_public_sha256=expected_public_sha256)
            committed = self._record(
                capture, public, _now_utc(self.clock()), producer, final_dir,
                persisted)
            temporary = layout['staging'] / (
                'registry-' + uuid.uuid4().hex + '.tmp')
            _write_private(temporary, (canonical(committed) + '\n').encode())
            checked = _now_utc(self.clock())
            reasons = self._final_validity(capture, public, pin, checked)
            if reasons:
                try:
                    temporary.unlink()
                except OSError:
                    pass
                return {
                    'status': 'REJECTED',
                    'registry_sha256': self._existing_registry_sha(),
                    'record': committed,
                    'association': persisted,
                    'validity': self._validity(
                        reasons, checked_at=checked,
                        committed_at_valid=False),
                    'context': {'capture': capture, 'public': public,
                                'source_pin': pin},
                }
            try:
                os.replace(temporary, self.registry_path)
                _fsync_dir(self.root)
            except BaseException:
                try:
                    temporary.unlink()
                except OSError:
                    pass
                raise
            data = _read_private_file(self.registry_path, 64 * 1024)
            sha = hashlib.sha256(data).hexdigest()
            status = 'RECOVERED' if recovered else 'PUBLISHED'
            return {
                'status': status,
                'registry_sha256': sha,
                'record': committed,
                'association': persisted,
                'validity': self._validity(
                    [], checked_at=checked, committed_at_valid=True),
                'context': {'capture': capture, 'public': public,
                            'source_pin': pin},
            }

    def _prune(self, referenced):
        """Prune under the writer lock using the actual committed pointer."""
        layout = self._ensure_layout()
        with _writer_lock(self.root):
            if not self.registry_path.is_file():
                return {'status': 'pending', 'reason': 'REGISTRY_MISSING',
                        'deleted': [], 'kept_current': None}
            try:
                record = json.loads(_read_registry_bytes(self.registry_path))
                current_path = Path(record['capture_root'])
                current_name = current_path.name
                if not current_path.exists() or current_path.is_symlink():
                    return {'status': 'pending',
                            'reason': 'CURRENT_GENERATION_MISSING',
                            'deleted': [], 'kept_current': current_name}
            except Exception:
                return {'status': 'pending', 'reason': 'REGISTRY_INVALID',
                        'deleted': [], 'kept_current': None}
            keep = {current_name} | set(referenced or ())
            started = time.monotonic()
            entries = []
            with os.scandir(layout['generations']) as iterator:
                for entry in iterator:
                    if len(entries) >= PRUNE_MAX_ENTRIES:
                        return {'status': 'pending', 'reason': 'ENTRY_LIMIT',
                                'deleted': [], 'kept_current': current_name}
                    if time.monotonic() - started > PRUNE_MAX_SECONDS:
                        return {'status': 'pending', 'reason': 'TIME_LIMIT',
                                'deleted': [], 'kept_current': current_name}
                    if entry.is_symlink() or not entry.is_dir(follow_symlinks=False):
                        continue
                    if not entry.name.startswith('capture-'):
                        continue
                    info = entry.stat(follow_symlinks=False)
                    entries.append((info.st_mtime_ns, entry.name, entry.path))
            entries.sort(reverse=True)
            kept_names = {name for _, name, _ in entries[:self.retention]}
            deleted = []
            for _, name, path in entries:
                if name in keep or name in kept_names:
                    continue
                if time.monotonic() - started > PRUNE_MAX_SECONDS:
                    return {'status': 'pending', 'reason': 'TIME_LIMIT',
                            'deleted': sorted(deleted), 'kept_current': current_name}
                try:
                    import shutil
                    shutil.rmtree(path)
                    deleted.append(name)
                except OSError:
                    return {'status': 'pending', 'reason': 'DELETE_FAILED',
                            'deleted': sorted(deleted), 'kept_current': current_name}
            return {'status': 'complete', 'reason': None,
                    'deleted': sorted(deleted), 'kept_current': current_name}

    def _already_or_rejected(self, bundle_root, exc, *, journal_requested=False):
        """Distinguish an already-committed generation from a new rejection."""
        code = getattr(exc, 'code', None) or str(exc)
        if journal_requested:
            return self._rejected(exc, None)
        try:
            candidate = capture_generation(bundle_root)
            existing = self._existing_registry()
        except Exception:
            return self._rejected(exc, None)
        if (existing is not None
                and existing.get('generation_id') == candidate.get('generation')):
            final_dir = Path(existing.get('capture_root', ''))
            try:
                persisted = self._persisted_association(
                    final_dir, None, candidate['generation'])
            except Exception:
                persisted = None
            checked = _now_utc(self.clock())
            return {
                'status': 'ALREADY_PUBLISHED', 'published': False,
                'committed': True,
                'error': code, 'reason': str(exc),
                'generation_id': candidate['generation'],
                'capture_root': str(final_dir),
                'registry_path': str(self.registry_path),
                'registry_sha256': self._existing_registry_sha(),
                'published_at': existing.get('published_at'),
                'renewed': False,
                'observation_valid': False,
                'current_evidence_valid': False,
                'committed_at_valid': True,
                'validation': self._validity(
                    [code], checked_at=checked, committed_at_valid=True),
                'current_reasons': [code],
                'historical_orders_complete': False,
                'settlement_ledger_complete': False,
                'all_asset_flat_proven': False,
                'association': persisted,
            }
        return self._rejected(exc, None)

    def _rejected(self, exc, final_dir):
        return {
            'status': 'REJECTED',
            'published': False,
            'committed': False,
            'error': getattr(exc, 'code', None) or str(exc),
            'reason': str(exc),
            'generation_id': None,
            'capture_root': str(final_dir) if final_dir is not None else '',
            'registry_path': str(self.registry_path),
            'renewed': False,
            'observation_valid': False,
            'current_evidence_valid': False,
            'committed_at_valid': False,
            'historical_orders_complete': False,
            'settlement_ledger_complete': False,
            'all_asset_flat_proven': False,
            'association': None,
        }

    def publish_bundle(self, bundle_root, *, producer='owner-collector-v1',
                       journal_snapshot=None, referenced_generations=()):
        """Validate an existing collector output and commit/recover it atomically."""
        now = _now_utc(self.clock())
        try:
            public = self._public(now)
            capture, evidence, association = self._validate_bundle(
                bundle_root, now, public, journal_snapshot=journal_snapshot)
        except PublicationError as exc:
            return self._already_or_rejected(
                bundle_root, exc,
                journal_requested=journal_snapshot is not None)
        layout = self._ensure_layout()
        final_dir = layout['generations'] / ('capture-' + capture['generation'])
        record = self._record(
            capture, public, now, producer, final_dir, association)
        recovered = final_dir.exists() or final_dir.is_symlink()
        if not recovered:
            stage = layout['staging'] / ('publish-' + uuid.uuid4().hex)
            try:
                self._copy_bundle(bundle_root, stage)
                copied = capture_generation(stage)
                if copied.get('generation') != capture['generation']:
                    _fail('PUBLICATION_COPY_GENERATION_MISMATCH')
                if association is not None:
                    _write_private(
                        stage / 'journal-generation.json',
                        (canonical(association) + '\n').encode())
                    _fsync_dir(stage)
                # Revalidate the prepared files at the post-copy owner clock
                # before making them visible as an immutable generation.
                self._validate_commit(
                    stage, capture, now=_now_utc(self.clock()),
                    expected_association=association,
                    expected_public_sha256=public['manifest_sha256'])
                try:
                    os.rename(stage, final_dir)
                except OSError:
                    if not final_dir.exists():
                        raise
                    recovered = True
                _fsync_dir(layout['generations'])
            except PublicationError as exc:
                return self._rejected(exc, final_dir)
            finally:
                if stage.exists():
                    try:
                        import shutil
                        shutil.rmtree(stage)
                    except OSError:
                        pass
        if recovered:
            record.setdefault('published_at', now.isoformat())
        commit = {
            'record': record, 'final_dir': final_dir,
            'expected_capture': capture,
            'expected_association': association,
            'recovered': recovered, 'producer': producer,
            'expected_public_sha256': public['manifest_sha256'],
        }
        try:
            published = self._publish_record(commit)
        except PublicationError as exc:
            return self._rejected(exc, final_dir)
        except OSError:
            # A post-replace fsync/return failure may have committed already.
            # Report the committed state, not a fake publication failure.
            existing = self._existing_registry()
            if (existing is not None
                    and existing.get('generation_id') == capture['generation']
                    and final_dir.is_dir()):
                final_capture = capture_generation(final_dir)
                final_pin = source_pin()
                checked = _now_utc(self.clock())
                reasons = self._final_validity(
                    final_capture, public, final_pin, checked)
                published = {
                    'status': 'RECOVERED' if recovered else 'PUBLISHED',
                    'registry_sha256': self._existing_registry_sha(),
                    'record': existing,
                    'association': self._persisted_association(
                        final_dir, association, capture['generation']),
                    'validity': self._validity(
                        reasons, checked_at=checked, committed_at_valid=True),
                    'context': {'capture': final_capture, 'public': public,
                                'source_pin': final_pin},
                }
            else:
                raise
        status = published['status']
        validity = published['validity']
        if status == 'REJECTED':
            reasons = validity.get('reasons') or ['PUBLICATION_FINAL_CHECK_FAILED']
            return {
                'status': 'REJECTED', 'published': False,
                'error': 'PUBLICATION_FINAL_CHECK_FAILED:' + ','.join(reasons),
                'reason': ','.join(reasons),
                'generation_id': published['record'].get('generation_id'),
                'capture_root': str(final_dir),
                'registry_path': str(self.registry_path),
                'renewed': False, 'committed': False,
                'observation_valid': False,
                'current_evidence_valid': False,
                'committed_at_valid': False,
                'validation': validity,
                'current_reasons': reasons,
                'historical_orders_complete': False,
                'settlement_ledger_complete': False,
                'all_asset_flat_proven': False,
                'association': published.get('association'),
            }
        registry_sha = published['registry_sha256']
        used_record = published['record']
        persisted = published['association']
        context = published.get('context') or {}
        if context.get('capture') is not None and context.get('public') is not None \
                and context.get('source_pin') is not None:
            try:
                cleanup = self._prune(referenced_generations)
            except Exception as exc:
                cleanup = {'status': 'pending',
                           'reason': 'CLEANUP_FAILED:' + type(exc).__name__,
                           'deleted': [], 'kept_current': None}
            post_now = _now_utc(self.clock())
            post_reasons = self._final_validity(
                context['capture'], context['public'], context['source_pin'],
                post_now)
            if post_reasons:
                validity = self._validity(
                    post_reasons, checked_at=post_now,
                    committed_at_valid=bool(validity.get('committed_at_valid')))
        else:
            cleanup = {'status': 'pending', 'reason': 'VALIDATION_CONTEXT_MISSING',
                       'deleted': [], 'kept_current': None}
        if cleanup['status'] != 'complete':
            return {
                'status': status,
                'generation_id': used_record['generation_id'],
                'registry_path': str(self.registry_path),
                'registry_sha256': registry_sha,
                'capture_root': str(final_dir) if final_dir is not None else '',
                'published_at': used_record.get('published_at'),
                'renewed': False,
                'observation_valid': validity.get('observation_valid', False),
                'current_evidence_valid': validity.get('current_evidence_valid', False),
                'committed_at_valid': validity.get('committed_at_valid', False),
                'validation': validity,
                'current_reasons': validity.get('reasons', []),
                'committed': True,
                'coverage': evidence.get('account_scope_applicability'),
                'historical_orders_complete': False,
                'settlement_ledger_complete': False,
                'all_asset_flat_proven': False,
                'association': persisted,
                'cleanup': cleanup,
                'pruned_generations': cleanup.get('deleted') or [],
            }
        return {
            'status': status,
            'generation_id': used_record['generation_id'],
            'registry_path': str(self.registry_path),
            'registry_sha256': registry_sha,
            'capture_root': str(final_dir) if final_dir is not None else '',
            'published_at': used_record.get('published_at'),
            'renewed': False,
            'observation_valid': validity.get('observation_valid', False),
            'current_evidence_valid': validity.get('current_evidence_valid', False),
            'committed_at_valid': validity.get('committed_at_valid', False),
            'validation': validity,
            'current_reasons': validity.get('reasons', []),
            'committed': True,
            'coverage': evidence.get('account_scope_applicability'),
            'historical_orders_complete': False,
            'settlement_ledger_complete': False,
            'all_asset_flat_proven': False,
            'association': persisted,
            'cleanup': cleanup,
            'pruned_generations': cleanup.get('deleted') or [],
        }

    def publication_status(self):
        """Explicit local status for retry/resume (never a broker replay)."""
        try:
            record = json.loads(_read_registry_bytes(self.registry_path))
        except Exception:
            return {'status': 'NO_CURRENT', 'registry_path': str(self.registry_path)}
        root = Path(record.get('capture_root', ''))
        valid = root.is_dir() and not root.is_symlink()
        return {'status': 'COMMITTED' if valid else 'CURRENT_INVALID',
                'registry_path': str(self.registry_path),
                'generation_id': record.get('generation_id'),
                'published_at': record.get('published_at'),
                'capture_root': str(root), 'generation_exists': valid}

    def retry(self, bundle_root, **kwargs):
        return self.publish_bundle(bundle_root, **kwargs)

    recover = retry

    def _existing_registry_sha(self):
        try:
            return hashlib.sha256(
                _read_registry_bytes(self.registry_path)).hexdigest()
        except Exception:
            return None

    def produce_and_publish(self, transport, *, timeout_seconds=30,
                            producer='owner-collector-v1',
                            journal_snapshot=None,
                            referenced_generations=()):
        """Run the owner collector in private staging, then publish or retain failure."""
        layout = self._ensure_layout()
        output = layout['staging'] / ('collect-' + uuid.uuid4().hex)
        try:
            result = collect_saved_bundle(
                output, account_suffix=self.account_suffix,
                transport=transport, timeout_seconds=timeout_seconds,
                clock=self.clock)
        except (CollectorError, OSError, ValueError, TypeError, KeyError) as exc:
            return {'status': 'FAILED',
                    'error': getattr(exc, 'code', None) or str(exc),
                    'collector_result': None, 'published': False}
        if result.get('status') == 'FAILED':
            self._retain_failure(output, result)
            return {'status': 'FAILED', 'error': result.get('blockers'),
                    'collector_result': result, 'published': False}
        try:
            published = self.publish_bundle(
                output, producer=producer, journal_snapshot=journal_snapshot,
                referenced_generations=referenced_generations)
        except (PublicationError, OSError, ValueError, TypeError, KeyError) as exc:
            self._retain_failure(output, result, error=getattr(exc, 'code', None)
                                 or str(exc))
            return {'status': 'FAILED',
                    'error': getattr(exc, 'code', None) or str(exc),
                    'collector_result': result, 'published': False}
        if published.get('status') == 'REJECTED':
            self._retain_failure(output, result, error=published.get('error'))
            return {'status': 'FAILED', 'error': published.get('error'),
                    'collector_result': result, 'published': False,
                    'publication': published}
        try:
            import shutil
            shutil.rmtree(output)
        except OSError:
            pass
        published['collector_result'] = result
        return published

    def _retain_failure(self, output, result, *, error=None):
        if not Path(output).exists():
            return
        layout = self._ensure_layout()
        target = layout['failures'] / (
            time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())
            + '-' + uuid.uuid4().hex[:12])
        try:
            os.rename(output, target)
            _write_private(target / 'publication-failure.json',
                           (canonical({'error': error,
                                       'collector_result': result}) + '\n').encode())
        except OSError:
            pass
        # Failure evidence is retained; only cap the directory count by
        # removing oldest entries, never overwriting a newer failure.
        try:
            entries = sorted(
                (entry.stat(follow_symlinks=False).st_mtime_ns, entry.path)
                for entry in os.scandir(layout['failures'])
                if entry.is_dir(follow_symlinks=False))
            for _, path in entries[:-MAX_FAILURE_EVIDENCE]:
                import shutil
                shutil.rmtree(path)
        except OSError:
            pass
