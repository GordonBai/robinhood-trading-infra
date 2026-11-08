"""Historical-baseline CLI: read-only plan/validate/status and local apply/revoke.

Plan/validate/status never construct an ExecutionBook/BrokerJournal/
LiveRiskGuard, never write a database, Host mailbox, risk sidecar or control
file. Apply and revoke are explicit local writes over an existing stopped
source: they require external pins, a held owner scope and a caller-owned
transaction. They never call a broker, market, MCP or Host, never reset risk,
and never create a strategy owner/fill/PnL. Source evidence is measured from
the stopped files; a caller-supplied evidence dictionary is never accepted.
"""
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat

from .baseline_evidence import (
    BundleError, load_saved_bundle, normalize_bundle,
    normalize_order_identity)
from .live_verification_control import CANONICAL_RUNTIME
from .source_evidence import (
    EXPECTED_FIELDS, SourceEvidenceError, SourceEvidenceProvider, _safe_path)
from .sqlite_readonly import SQLiteReadOnlyError, readonly_uri
from trading_contracts.values import canonical, digest
from trading_infra.historical_baseline import (
    BASELINE_SCOPE_IDENTITY, BASELINE_SCOPE_FULL, PROVIDER, BaselineError,
    _manifest_checked, _utc, _validate_order_identity_snapshot,
    _validate_snapshot, apply_baseline, plan_baseline,
    plan_order_identity_baseline, revoke_baseline, status_baseline,
    validate_baseline)


EXIT_OK = 0
EXIT_CONDITIONS = 3
EXIT_VALIDATION = 4
EXIT_INPUT = 5
MAX_INPUT_BYTES = 8 * 1024 * 1024
MAX_DB_BYTES = 512 * 1024 * 1024


class CliError(ValueError):
    def __init__(self, code, detail=None):
        self.code = code
        self.detail = detail
        super().__init__(code)

    def __str__(self):
        return self.code if self.detail is None else f'{self.code}:{self.detail}'


def _fail(code, detail=None):
    raise CliError(code, detail)


def _flags(status):
    return {
        'production_ready': False, 'live_enabled': False,
        'execution_authorized': False,
    }


def _result(kind, status):
    return {
        'schema_version': 1, 'kind': kind, 'status': status,
        'blockers': [], 'blocker_detail': None, **_flags(status),
    }


def _error(kind, status, exc):
    result = _result(kind, status)
    result['blockers'] = [exc.code]
    result['blocker_detail'] = exc.detail
    return result


class _Clock:
    """Sampled internal clock; default wall UTC, never a public CLI option."""

    def __init__(self, clock=None):
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self.last = None

    def __call__(self):
        try:
            value = self._clock()
        except Exception:
            _fail('CLI_CLOCK_INVALID')
        if not isinstance(value, datetime) or value.tzinfo is None \
                or value.utcoffset() is None:
            _fail('CLI_CLOCK_INVALID')
        value = value.astimezone(timezone.utc)
        if self.last is not None and value < self.last:
            _fail('CLI_CLOCK_WENT_BACKWARDS')
        self.last = value
        return value


def _read_json_input(path, code):
    target = Path(path)
    if target.is_symlink() or not target.is_file():
        _fail(code, str(target))
    try:
        fd = os.open(target, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError:
        _fail(code, str(target))
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_INPUT_BYTES \
                or info.st_mode & 0o022:
            _fail(code, str(target))
        chunks = []
        size = 0
        while True:
            block = os.read(fd, 1024 * 1024)
            if not block:
                break
            chunks.append(block)
            size += len(block)
            if size > MAX_INPUT_BYTES:
                _fail(code, str(target))
        raw = b''.join(chunks)
        if size != info.st_size:
            _fail(code, str(target))
    except OSError:
        _fail(code, str(target))
    finally:
        os.close(fd)

    def reject_float(value):
        raise ValueError('JSON_NUMBER_NOT_ACCEPTED')

    def reject_constant(value):
        raise ValueError('JSON_CONSTANT_NOT_ACCEPTED')

    try:
        value = json.loads(raw.decode('utf-8'), parse_float=reject_float,
                           parse_constant=reject_constant)
    except (UnicodeDecodeError, TypeError, ValueError):
        _fail(code + '_JSON', str(target))
    if not isinstance(value, dict):
        _fail(code + '_SCHEMA', str(target))
    return value


def _read_policy(path):
    policy = _read_json_input(path, 'CLI_POLICY_INVALID')
    if not policy:
        _fail('CLI_POLICY_SCHEMA')
    return policy


def _read_pins(path):
    pins = _read_json_input(path, 'CLI_PINS_INVALID')
    if set(pins) != set(EXPECTED_FIELDS):
        _fail('CLI_PINS_SCHEMA')
    return pins


def _read_manifest(path):
    return _read_json_input(path, 'CLI_MANIFEST_INVALID')


def _plan_time_checked(snapshot, *, account_key, cutoff, expires, now):
    observed = _utc(snapshot.get('observed_at'),
                    'BASELINE_SNAPSHOT_TIME_INVALID')
    expiry = _utc(expires, 'BASELINE_EXPIRY_INVALID')
    if expiry <= now:
        raise BaselineError('BASELINE_PLAN_EXPIRED')
    if expiry < observed:
        raise BaselineError('BASELINE_EXPIRY_BEFORE_SNAPSHOT')
    _validate_snapshot(
        snapshot, account_key=account_key,
        provider=snapshot.get('provider', 'robinhood'),
        cutoff=_utc(cutoff, 'BASELINE_CUTOFF_INVALID'),
        observed_at=observed, now=now)


def _resolve_root(root, isolated):
    value = Path(root) if root is not None else CANONICAL_RUNTIME
    if value.is_symlink() or not value.is_dir():
        _fail('CLI_ROOT_INVALID')
    resolved = value.resolve()
    if resolved != CANONICAL_RUNTIME.resolve() and not isolated:
        _fail('CLI_ISOLATED_ROOT_REQUIRED')
    return value


def _controlled_db_path(root, value, *, allow_missing):
    target = Path(value)
    absolute = target if target.is_absolute() else Path(root) / target
    try:
        return _safe_path(root, absolute, kind='file')
    except SourceEvidenceError:
        if not allow_missing or absolute.exists() or absolute.is_symlink():
            raise
        parent = _safe_path(root, absolute.parent, kind='directory')
        if absolute.name in {'', '.', '..'}:
            _fail('CLI_DB_PATH_INVALID')
        return parent / absolute.name


def _source_provider(args, policy, pins, clock):
    def provider_clock():
        try:
            return clock()
        except CliError as exc:
            raise SourceEvidenceError(exc.code, exc.detail) from None

    return SourceEvidenceProvider(
        execution_db=args.execution_db, risk_db=args.risk_db,
        host_dir=args.host_dir, expected=pins, policy=policy,
        root=args.root, isolated=args.isolated_root, clock=provider_clock)


def _source_summary(result):
    evidence = result.get('evidence')
    provenance = result.get('provenance') or {}
    return {
        'status': result.get('status'),
        'blockers': list(result.get('blockers') or []),
        'blocker_detail': result.get('blocker_detail'),
        'evidence': evidence,
        'provenance': {
            key: provenance.get(key) for key in (
                'owner_lock_stopped', 'owner_lock_held_across_read',
                'wal_converged', 'file_hashes', 'state_generation',
                'lineage_sequence')
        },
    }


def _bundle_summary(bundle):
    account = bundle.get('account') or {}
    return {
        'raw_chain_sha256': bundle.get('raw_chain_sha256'),
        'account_key': account.get('account_key'),
        'account_mask': account.get('account_mask'),
        'observed_at': bundle.get('observed_at'),
        'collection_window_seconds': bundle.get('collection_window_seconds'),
        'blockers': list(bundle.get('blockers') or []),
        'candidate_snapshot_present': isinstance(
            bundle.get('candidate_snapshot'), dict),
        'ready': bundle.get('ready') is True,
    }


def _bundle_identity(bundle):
    account = bundle.get('account') or {}
    return canonical({
        'raw_chain_sha256': bundle.get('raw_chain_sha256'),
        'account_key': account.get('account_key'),
        'account_mask': account.get('account_mask'),
        'observed_at': bundle.get('observed_at'),
        'candidate_snapshot': bundle.get('candidate_snapshot'),
    })


def _identity_summary(identity):
    return {
        'kind': identity.get('kind'),
        'baseline_scope': identity.get('baseline_scope'),
        'raw_chain_sha256': (identity.get('source') or {}).get(
            'raw_chain_sha256'),
        'account_key': (identity.get('account') or {}).get('account_key'),
        'account_mask': (identity.get('account') or {}).get('account_mask'),
        'observed_at': identity.get('observed_at'),
        'collection_window_seconds': identity.get('collection_window_seconds'),
        'blockers': list(identity.get('blockers') or []),
        'identity_snapshot_present': True,
        'order_count': len(identity.get('orders') or []),
        'execution_count': len(identity.get('executions') or []),
        'ready': False,
    }


def _identity_identity(identity):
    return canonical(identity)


def _open_readonly(db_path):
    db = None
    try:
        uri = readonly_uri(db_path)
    except SQLiteReadOnlyError as exc:
        code = ('CLI_WAL_NOT_CONVERGED'
                if exc.code == 'SQLITE_SIDECAR_NOT_CONVERGED'
                else 'CLI_SQLITE_SIDECAR_INVALID')
        _fail(code, exc.detail)
    try:
        db = sqlite3.connect(uri, uri=True, timeout=.5)
        db.execute('PRAGMA query_only=ON')
        if db.execute('PRAGMA integrity_check').fetchall() != [('ok',)]:
            _fail('CLI_SQLITE_INTEGRITY_FAILED', str(db_path))
        return db
    except sqlite3.Error:
        if db is not None:
            db.close()
        _fail('CLI_SQLITE_INVALID', str(db_path))
    except BaseException:
        if db is not None:
            db.close()
        raise


def _file_state(path):
    try:
        before = Path(path).stat()
    except OSError:
        _fail('CLI_DB_READ_FAILED', str(path))
    h = hashlib.sha256()
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError:
        _fail('CLI_DB_READ_FAILED', str(path))
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_DB_BYTES:
            _fail('CLI_DB_TOO_LARGE_OR_INVALID', str(path))
        size = 0
        while True:
            block = os.read(fd, 1024 * 1024)
            if not block:
                break
            size += len(block)
            h.update(block)
        if size != info.st_size:
            _fail('CLI_DB_CHANGED_DURING_STATUS', str(path))
    except OSError:
        _fail('CLI_DB_READ_FAILED', str(path))
    finally:
        os.close(fd)
    try:
        after = Path(path).stat()
    except OSError:
        _fail('CLI_DB_CHANGED_DURING_STATUS', str(path))
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
            after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
        _fail('CLI_DB_CHANGED_DURING_STATUS', str(path))
    return h.hexdigest()


def _open_writable(db_path):
    try:
        uri = Path(db_path).as_uri() + '?mode=rw'
    except ValueError:
        _fail('CLI_DB_PATH_INVALID', str(db_path))
    db = None
    try:
        db = sqlite3.connect(uri, uri=True, timeout=5.0)
        if db.execute('PRAGMA integrity_check').fetchall() != [('ok',)]:
            _fail('CLI_SQLITE_INTEGRITY_FAILED', str(db_path))
        db.execute('PRAGMA query_only=OFF')
        return db
    except sqlite3.Error:
        if db is not None:
            db.close()
        _fail('CLI_SQLITE_INVALID', str(db_path))
    except BaseException:
        if db is not None:
            db.close()
        raise


def _try_exact_applied(*, db_path, manifest, manifest_pin, approval_scope,
                       clock):
    """Use the core duplicate branch without requiring source/expiry.

    The read-only connection and owner scope are supplied by the caller.
    ``BASELINE_CURRENT_INPUTS_REQUIRED`` means there is no exact applied row;
    every other error is a real integrity/pin failure, not a new apply.
    """
    db = _open_readonly(db_path)
    try:
        try:
            value = apply_baseline(
                db, manifest, manifest_pin=manifest_pin,
                approval_scope=approval_scope, now=clock(),
                evidence=None, snapshot=None)
        except BaselineError as exc:
            if exc.code == 'BASELINE_CURRENT_INPUTS_REQUIRED':
                return None
            raise
    finally:
        db.close()
    if value.get('status') == 'APPLIED' and value.get('duplicate') is True:
        return value
    if value.get('status') == 'REVOKED' and value.get('active') is False:
        return value
    _fail('CLI_APPLY_REPLAY_INVALID')


def _post_apply_status(db_path, manifest):
    db = _open_readonly(db_path)
    try:
        status = status_baseline(db)
    finally:
        db.close()
    if status.get('status') != 'PRESENT':
        _fail('CLI_APPLY_POSTCHECK_STATUS', status.get('reason'))
    active = status.get('active') or []
    if not any(item.get('baseline_id') == manifest.get('baseline_id')
               and item.get('manifest_sha256') == manifest.get('manifest_sha256')
               for item in active if isinstance(item, dict)):
        _fail('CLI_APPLY_POSTCHECK_MISMATCH')
    return status


def _apply_unknown(kind, manifest, pin, exc, *, applied):
    status = 'APPLIED_POSTCHECK_FAILED' if applied else 'APPLY_OUTCOME_UNKNOWN'
    output = _result(kind, status)
    output['applied'] = True if applied else None
    output['baseline'] = {
        'status': 'APPLIED' if applied else 'UNKNOWN',
        'baseline_id': manifest.get('baseline_id'),
        'manifest_sha256': manifest.get('manifest_sha256'),
        'manifest_pin': pin,
    }
    code = getattr(exc, 'code', None) or type(exc).__name__
    output['blockers'] = [code]
    output['blocker_detail'] = getattr(exc, 'detail', None)
    return output


def _run_apply(args, *, clock=None):
    kind = 'historical_baseline_cli_apply'
    sampled = _Clock(clock)
    try:
        sampled()
        approval_scope = getattr(args, 'approval_scope', None)
        if not isinstance(approval_scope, str) or not approval_scope.strip():
            _fail('CLI_APPROVAL_SCOPE_REQUIRED')
        root = _resolve_root(args.root, args.isolated_root)
        try:
            db_path = _controlled_db_path(root, args.db, allow_missing=False)
        except SourceEvidenceError as exc:
            raise CliError(exc.code, exc.detail) from None
        policy = _read_policy(args.policy)
        pins = _read_pins(args.pins)
        manifest = _read_manifest(args.manifest)
        if not isinstance(manifest, dict):
            _fail('CLI_MANIFEST_INVALID')
        manifest_scope = manifest.get('baseline_scope')
        requested_scope = getattr(args, 'scope', None)
        if requested_scope == BASELINE_SCOPE_IDENTITY \
                and manifest_scope != BASELINE_SCOPE_IDENTITY:
            _fail('CLI_SCOPE_MANIFEST_MISMATCH')
        if requested_scope == BASELINE_SCOPE_FULL \
                and manifest_scope not in (None, BASELINE_SCOPE_FULL):
            _fail('CLI_SCOPE_MANIFEST_MISMATCH')
        if requested_scope is None:
            requested_scope = manifest_scope or BASELINE_SCOPE_FULL
        _manifest_checked(
            manifest, args.manifest_pin, now=sampled(), require_expiry=False)
        if digest(policy) != manifest.get('policy_sha256'):
            _fail('CLI_MANIFEST_POLICY_MISMATCH')
        suffix = pins.get('account_suffix')
        if (pins.get('account_key') != manifest.get('account_key')
                or pins.get('policy_sha256') != manifest.get('policy_sha256')
                or pins.get('source_fingerprint')
                != manifest.get('source_fingerprint')
                or not isinstance(suffix, str)
                or not str(manifest.get('account_mask') or '').endswith(suffix)):
            _fail('CLI_MANIFEST_PINS_MISMATCH')
        provider = _source_provider(args, policy, pins, sampled)
        if db_path.resolve() != Path(provider.execution_db).resolve():
            _fail('CLI_DB_SOURCE_MISMATCH')
        with provider.exclusive_scope() as owner:
            replay = _try_exact_applied(
                db_path=db_path, manifest=manifest,
                manifest_pin=args.manifest_pin,
                approval_scope=approval_scope, clock=sampled)
            if replay is not None:
                try:
                    provider.verify_scope(owner)
                except SourceEvidenceError as exc:
                    output = _error(kind, 'BLOCKED', exc)
                    output['source'] = {'status': 'BLOCKED',
                                        'blockers': [exc.code],
                                        'blocker_detail': exc.detail}
                    return output, EXIT_CONDITIONS
                if replay.get('status') == 'REVOKED':
                    output = _result(kind, 'REVOKED')
                    output.update({
                        'applied': False, 'already_applied': False,
                        'active': False,
                        'baseline': {
                            'status': 'REVOKED',
                            'baseline_id': replay.get('baseline_id'),
                            'manifest_sha256': replay.get('manifest_sha256'),
                            'manifest_pin': args.manifest_pin,
                            'duplicate': False,
                        },
                        'post_apply_status': None,
                    })
                    return output, EXIT_VALIDATION
                output = _result(kind, 'ALREADY_APPLIED')
                output.update({
                    'applied': False, 'already_applied': True,
                    'baseline': {
                        'status': 'ALREADY_APPLIED',
                        'baseline_id': replay.get('baseline_id'),
                        'manifest_sha256': replay.get('manifest_sha256'),
                        'manifest_pin': args.manifest_pin,
                        'duplicate': True,
                    },
                    'post_apply_status': None,
                })
                return output, EXIT_OK
            if manifest.get('status') not in {'PLANNED', 'VALIDATED'}:
                _fail('CLI_APPLY_MANIFEST_STATUS')
            _manifest_checked(
                manifest, args.manifest_pin, now=sampled(), require_expiry=True)
            source = provider.read()
            if source.get('status') != 'READY':
                output = _result(kind, 'BLOCKED')
                output['source'] = _source_summary(source)
                output['baseline'] = {'status': 'NOT_ATTEMPTED'}
                return output, EXIT_CONDITIONS
            source_block = (source.get('evidence') or {}).get('source') or {}
            if source_block.get('account_key') != manifest.get('account_key'):
                _fail('CLI_ACCOUNT_MISMATCH')
            try:
                loaded = load_saved_bundle(args.bundle_root)
                if requested_scope == BASELINE_SCOPE_IDENTITY:
                    bundle = normalize_order_identity(loaded, now=sampled())
                    bundle_summary = _identity_summary(bundle)
                    snapshot = bundle
                else:
                    bundle = normalize_bundle(loaded, now=sampled())
                    bundle_summary = _bundle_summary(bundle)
                    snapshot = bundle.get('candidate_snapshot')
            except BundleError as exc:
                return _error(kind, 'INVALID_INPUT',
                              CliError(exc.code, exc.detail)), EXIT_INPUT
            output = _result(kind, 'CONDITIONS_MISSING')
            output['source'] = _source_summary(source)
            output['bundle'] = bundle_summary
            output['baseline'] = {'status': 'NOT_ATTEMPTED'}
            blockers = set(output['bundle'].get('blockers') or ())
            if blockers or not isinstance(snapshot, dict):
                output['blockers'] = sorted(blockers)
                return output, EXIT_CONDITIONS
            bundle_account = output['bundle'].get('account_key')
            if bundle_account != manifest.get('account_key'):
                _fail('CLI_ACCOUNT_MISMATCH')
            db = None
            applied = None
            close_error = None
            locked_source = None
            transaction_error = None
            try:
                try:
                    db = _open_writable(db_path)
                    db.execute('BEGIN IMMEDIATE')
                    try:
                        # The write lock is held now. Validate the actual
                        # owner boundary and re-read every external source
                        # pin before any baseline DDL/DML; pre-lock evidence
                        # is not reused.
                        provider.verify_scope(owner)
                        _manifest_checked(
                            manifest, args.manifest_pin, now=sampled(),
                            require_expiry=True)
                        locked_source = provider.read()
                        if locked_source.get('status') != 'READY':
                            db.rollback()
                            output = _result(kind, 'BLOCKED')
                            output['source'] = _source_summary(locked_source)
                            output['baseline'] = {'status': 'NOT_ATTEMPTED'}
                            return output, EXIT_CONDITIONS
                        locked_block = (
                            (locked_source.get('evidence') or {}).get('source')
                            or {})
                        if locked_block.get('account_key') \
                                != manifest.get('account_key'):
                            _fail('CLI_ACCOUNT_MISMATCH')
                        # Verify immediately before core DDL/DML as well; the
                        # source read itself must not have lost the owner.
                        provider.verify_scope(owner)

                        def prewrite_check():
                            provider.verify_scope(owner)
                            current = provider.read()
                            if current.get('status') != 'READY':
                                blockers = current.get('blockers') or [
                                    'SOURCE_BLOCKED']
                                raise SourceEvidenceError(
                                    blockers[0],
                                    current.get('blocker_detail'))
                            if canonical(current.get('evidence')) \
                                    != canonical(locked_source.get('evidence')):
                                raise SourceEvidenceError(
                                    'CLI_SOURCE_CHANGED_BEFORE_APPLY')

                        applied = apply_baseline(
                            db, manifest, manifest_pin=args.manifest_pin,
                            approval_scope=approval_scope, clock=sampled,
                            evidence=locked_source['evidence'],
                            snapshot=snapshot, prewrite=prewrite_check)
                        if applied.get('duplicate') is True:
                            db.rollback()
                        else:
                            db.commit()
                    except BaseException as exc:
                        try:
                            if db.in_transaction:
                                db.rollback()
                        except sqlite3.Error:
                            pass
                        transaction_error = exc
                finally:
                    if db is not None:
                        try:
                            db.close()
                        except sqlite3.Error as exc:
                            close_error = exc
            except BaseException:
                raise
            if transaction_error is not None:
                if isinstance(transaction_error, (sqlite3.Error, OSError)):
                    try:
                        replay = _try_exact_applied(
                            db_path=db_path, manifest=manifest,
                            manifest_pin=args.manifest_pin,
                            approval_scope=approval_scope, clock=sampled)
                    except (CliError, SourceEvidenceError, BaselineError,
                            sqlite3.Error, OSError):
                        replay = None
                    if replay is not None:
                        if replay.get('status') == 'REVOKED':
                            output = _result(kind, 'REVOKED')
                            output.update({
                                'applied': False, 'already_applied': False,
                                'active': False,
                                'baseline': {
                                    'status': 'REVOKED',
                                    'baseline_id': replay.get('baseline_id'),
                                    'manifest_sha256':
                                        replay.get('manifest_sha256'),
                                    'manifest_pin': args.manifest_pin,
                                    'duplicate': False,
                                },
                            })
                            return output, EXIT_VALIDATION
                        output = _result(kind, 'ALREADY_APPLIED')
                        output.update({
                            'applied': True, 'already_applied': True,
                            'baseline': {
                                'status': 'ALREADY_APPLIED',
                                'baseline_id': replay.get('baseline_id'),
                                'manifest_sha256':
                                    replay.get('manifest_sha256'),
                                'manifest_pin': args.manifest_pin,
                                'duplicate': True,
                            },
                        })
                        return output, EXIT_OK
                    return _apply_unknown(
                        kind, manifest, args.manifest_pin,
                        transaction_error, applied=False), EXIT_VALIDATION
                raise transaction_error
            if close_error is not None:
                return _apply_unknown(
                    kind, manifest, args.manifest_pin, close_error,
                    applied=True), EXIT_VALIDATION
            if applied is None or applied.get('status') != 'APPLIED':
                return _apply_unknown(
                    kind, manifest, args.manifest_pin,
                    CliError('CLI_APPLY_STATUS_INVALID',
                             str(None if applied is None
                                 else applied.get('status'))),
                    applied=False), EXIT_VALIDATION
            if applied.get('duplicate') is True:
                output = _result(kind, 'ALREADY_APPLIED')
                output.update({
                    'applied': False, 'already_applied': True,
                    'baseline': {
                        'status': 'ALREADY_APPLIED',
                        'baseline_id': applied.get('baseline_id'),
                        'manifest_sha256': applied.get('manifest_sha256'),
                        'manifest_pin': args.manifest_pin,
                        'duplicate': True,
                    },
                })
                return output, EXIT_OK
            try:
                post_status = _post_apply_status(db_path, manifest)
            except (CliError, SourceEvidenceError, BaselineError,
                    sqlite3.Error, OSError) as exc:
                return _apply_unknown(
                    kind, manifest, args.manifest_pin, exc,
                    applied=True), EXIT_VALIDATION
            source = locked_source
            output = _result(kind, 'APPLIED')
            output.update({
                'applied': True, 'already_applied': False,
                'source': _source_summary(source),
                'bundle': bundle_summary,
                'baseline': {
                    'status': 'APPLIED',
                    'baseline_id': applied.get('baseline_id'),
                    'manifest_sha256': applied.get('manifest_sha256'),
                    'manifest_pin': args.manifest_pin,
                    'duplicate': False,
                    'counts': applied.get('counts'),
                },
                'post_apply_status': post_status,
            })
            return output, EXIT_OK
    except CliError as exc:
        return _error(kind, 'INVALID_INPUT', exc), EXIT_INPUT
    except SourceEvidenceError as exc:
        output = _error(kind, 'BLOCKED', exc)
        output['source'] = {'status': 'BLOCKED', 'blockers': [exc.code],
                            'blocker_detail': exc.detail}
        return output, EXIT_CONDITIONS
    except BaselineError as exc:
        return _error(kind, 'VALIDATION_FAILED',
                      CliError(exc.code, exc.detail)), EXIT_VALIDATION
    except (sqlite3.Error, OSError) as exc:
        return _error(kind, 'INVALID_INPUT',
                      CliError('CLI_SQLITE_INVALID', str(exc))), EXIT_INPUT
    except BundleError as exc:
        return _error(kind, 'INVALID_INPUT',
                      CliError(exc.code, exc.detail)), EXIT_INPUT


def _baseline_entry(db, baseline_id, manifest_pin):
    status = status_baseline(db)
    if status.get('status') != 'PRESENT':
        _fail('CLI_BASELINE_STATUS_INVALID', status.get('reason'))
    for entry in status.get('history') or []:
        if entry.get('baseline_id') == baseline_id:
            if entry.get('manifest_sha256') != manifest_pin:
                _fail('BASELINE_REVOKE_PIN_MISMATCH')
            return entry
    _fail('BASELINE_REVOKE_TARGET_MISSING')


def _revoke_terminal_output(kind, entry, manifest_pin, status,
                            *, already_revoked):
    output = _result(kind, status)
    output.update({
        'revoked': True,
        'applied': False,
        'already_revoked': already_revoked,
        'baseline': {
            'status': 'ALREADY_REVOKED' if already_revoked else 'REVOKED',
            'baseline_id': entry.get('baseline_id'),
            'manifest_sha256': entry.get('manifest_sha256'),
            'manifest_pin': manifest_pin,
            'revoked_at': entry.get('revoked_at'),
            'revoke_approval_scope': entry.get('revoke_approval_scope'),
            'revoke_reason': entry.get('revoke_reason'),
        },
    })
    return output


def _revoke_unknown(kind, entry, manifest_pin, exc, *, revoked):
    status = 'REVOKE_POSTCHECK_FAILED' if revoked else 'REVOKE_OUTCOME_UNKNOWN'
    output = _result(kind, status)
    output.update({
        'revoked': True if revoked else None,
        'applied': False,
        'already_revoked': False,
        'baseline': {
            'status': 'REVOKED' if revoked else 'UNKNOWN',
            'baseline_id': entry.get('baseline_id'),
            'manifest_sha256': entry.get('manifest_sha256'),
            'manifest_pin': manifest_pin,
        },
        'blockers': [getattr(exc, 'code', None) or type(exc).__name__],
        'blocker_detail': getattr(exc, 'detail', None),
    })
    return output


def _revoke_rejected(kind, entry, manifest_pin, exc):
    output = _error(kind, 'REVOKE_REJECTED',
                    CliError(getattr(exc, 'code', None)
                             or type(exc).__name__, str(exc)))
    output.update({
        'revoked': False,
        'applied': False,
        'already_revoked': False,
        'baseline': {
            'status': 'REJECTED',
            'baseline_id': (entry or {}).get('baseline_id'),
            'manifest_sha256': (entry or {}).get('manifest_sha256')
            or manifest_pin,
            'manifest_pin': manifest_pin,
        },
    })
    return output


def _run_revoke(args, *, clock=None):
    kind = 'historical_baseline_cli_revoke'
    sampled = _Clock(clock)
    try:
        sampled()
        approval_scope = getattr(args, 'approval_scope', None)
        reason = getattr(args, 'reason', None)
        if not isinstance(approval_scope, str) or not approval_scope.strip():
            _fail('CLI_REVOKE_APPROVAL_SCOPE_REQUIRED')
        if not isinstance(reason, str) or not reason.strip():
            _fail('CLI_REVOKE_REASON_REQUIRED')
        root = _resolve_root(args.root, args.isolated_root)
        try:
            db_path = _controlled_db_path(root, args.db, allow_missing=False)
        except SourceEvidenceError as exc:
            raise CliError(exc.code, exc.detail) from None
        policy = _read_policy(args.policy)
        pins = _read_pins(args.pins)
        provider = _source_provider(args, policy, pins, sampled)
        if db_path.resolve() != Path(provider.execution_db).resolve():
            _fail('CLI_DB_SOURCE_MISMATCH')
        manifest_pin = args.manifest_pin
        if not isinstance(manifest_pin, str) or len(manifest_pin) != 64 \
                or any(ch not in '0123456789abcdef' for ch in manifest_pin):
            _fail('BASELINE_REVOKE_PIN_INVALID')
        with provider.exclusive_scope() as owner:
            # Exact already-REVOKED is a read-only terminal confirmation.  It
            # still requires a valid schema/target/event chain and owner.
            db = _open_readonly(db_path)
            try:
                entry = _baseline_entry(db, args.baseline_id, manifest_pin)
            finally:
                db.close()
            if entry.get('status') == 'REVOKED':
                provider.verify_scope(owner)
                return _revoke_terminal_output(
                    kind, entry, manifest_pin, 'ALREADY_REVOKED',
                    already_revoked=True), EXIT_OK
            if entry.get('status') != 'APPLIED':
                _fail('BASELINE_REVOKE_NOT_ACTIVE')

            db = None
            revoked = None
            close_error = None
            transaction_error = None
            commit_attempted = False
            rollback_ok = False
            began = False
            try:
                try:
                    db = _open_writable(db_path)
                    db.execute('BEGIN IMMEDIATE')
                    began = True
                    try:
                        provider.verify_scope(owner)
                        entry = _baseline_entry(
                            db, args.baseline_id, manifest_pin)
                        if entry.get('status') == 'REVOKED':
                            db.rollback()
                            provider.verify_scope(owner)
                            return _revoke_terminal_output(
                                kind, entry, manifest_pin,
                                'ALREADY_REVOKED',
                                already_revoked=True), EXIT_OK
                        if entry.get('status') != 'APPLIED':
                            _fail('BASELINE_REVOKE_NOT_ACTIVE')
                        source = provider.read()
                        if source.get('status') != 'READY':
                            db.rollback()
                            output = _result(kind, 'BLOCKED')
                            output['source'] = _source_summary(source)
                            output['baseline'] = {
                                'status': 'NOT_ATTEMPTED',
                                'baseline_id': args.baseline_id,
                                'manifest_pin': manifest_pin,
                            }
                            return output, EXIT_CONDITIONS
                        source_block = ((source.get('evidence') or {}).get(
                            'source') or {})
                        if source_block.get('account_key') \
                                != entry.get('account_key'):
                            _fail('CLI_REVOKE_ACCOUNT_MISMATCH')
                        if entry.get('provider') != PROVIDER:
                            _fail('CLI_REVOKE_PROVIDER_MISMATCH')

                        def prewrite_check():
                            provider.verify_scope(owner)
                            current = provider.read()
                            if current.get('status') != 'READY':
                                blockers = current.get('blockers') or [
                                    'SOURCE_BLOCKED']
                                raise SourceEvidenceError(
                                    blockers[0],
                                    current.get('blocker_detail'))
                            if canonical(current.get('evidence')) \
                                    != canonical(source.get('evidence')):
                                raise SourceEvidenceError(
                                    'CLI_SOURCE_CHANGED_BEFORE_REVOKE')

                        revoked = revoke_baseline(
                            db, args.baseline_id, manifest_pin,
                            approval_scope=approval_scope, reason=reason,
                            clock=sampled, prewrite=prewrite_check)
                        if revoked.get('status') == 'ALREADY_REVOKED':
                            db.rollback()
                        else:
                            commit_attempted = True
                            db.commit()
                    except BaseException as exc:
                        try:
                            if db.in_transaction:
                                db.rollback()
                            if began and not commit_attempted:
                                rollback_ok = True
                        except sqlite3.Error:
                            rollback_ok = False
                        transaction_error = exc
                finally:
                    if db is not None:
                        try:
                            db.close()
                        except sqlite3.Error as exc:
                            close_error = exc
            except BaseException:
                raise
            if transaction_error is not None:
                if isinstance(transaction_error, (sqlite3.Error, OSError)):
                    try:
                        current = _open_readonly(db_path)
                        try:
                            entry = _baseline_entry(
                                current, args.baseline_id, manifest_pin)
                        finally:
                            current.close()
                    except (CliError, BaselineError, sqlite3.Error, OSError):
                        entry = {'baseline_id': args.baseline_id,
                                 'manifest_sha256': manifest_pin}
                    if entry.get('status') == 'REVOKED':
                        return _revoke_terminal_output(
                            kind, entry, manifest_pin,
                            'REVOKED', already_revoked=False), EXIT_OK
                    if rollback_ok and not commit_attempted:
                        return _revoke_rejected(
                            kind, entry, manifest_pin,
                            transaction_error), EXIT_VALIDATION
                    return _revoke_unknown(
                        kind, entry, manifest_pin,
                        transaction_error, revoked=False), EXIT_VALIDATION
                raise transaction_error
            if close_error is not None:
                return _revoke_unknown(
                    kind, entry, manifest_pin, close_error,
                    revoked=True), EXIT_VALIDATION
            if revoked is None or revoked.get('status') not in {
                    'REVOKED', 'ALREADY_REVOKED'}:
                return _revoke_unknown(
                    kind, entry, manifest_pin,
                    CliError('CLI_REVOKE_STATUS_INVALID',
                             str(None if revoked is None
                                 else revoked.get('status'))),
                    revoked=False), EXIT_VALIDATION
            if revoked.get('status') == 'ALREADY_REVOKED':
                return _revoke_terminal_output(
                    kind, entry, manifest_pin, 'ALREADY_REVOKED',
                    already_revoked=True), EXIT_OK
            try:
                current = _open_readonly(db_path)
                try:
                    final = _baseline_entry(
                        current, args.baseline_id, manifest_pin)
                finally:
                    current.close()
            except (CliError, BaselineError, sqlite3.Error, OSError) as exc:
                return _revoke_unknown(
                    kind, entry, manifest_pin, exc,
                    revoked=True), EXIT_VALIDATION
            if final.get('status') != 'REVOKED':
                return _revoke_unknown(
                    kind, final, manifest_pin,
                    CliError('CLI_REVOKE_POSTCHECK_MISMATCH'),
                    revoked=True), EXIT_VALIDATION
            return _revoke_terminal_output(
                kind, final, manifest_pin, 'REVOKED',
                already_revoked=False), EXIT_OK
    except CliError as exc:
        if exc.code.startswith('BASELINE_') or exc.code in {
                'CLI_BASELINE_STATUS_INVALID',
                'CLI_REVOKE_ACCOUNT_MISMATCH',
                'CLI_REVOKE_PROVIDER_MISMATCH'}:
            return _revoke_rejected(
                kind, {'baseline_id': getattr(args, 'baseline_id', None)},
                getattr(args, 'manifest_pin', None), exc), EXIT_VALIDATION
        output = _error(kind, 'INVALID_INPUT', exc)
        output['baseline'] = {
            'status': 'REJECTED',
            'baseline_id': getattr(args, 'baseline_id', None),
            'manifest_pin': getattr(args, 'manifest_pin', None),
        }
        return output, EXIT_INPUT
    except SourceEvidenceError as exc:
        output = _error(kind, 'BLOCKED', exc)
        output['source'] = {'status': 'BLOCKED', 'blockers': [exc.code],
                            'blocker_detail': exc.detail}
        output['baseline'] = {
            'status': 'NOT_ATTEMPTED',
            'baseline_id': getattr(args, 'baseline_id', None),
            'manifest_pin': getattr(args, 'manifest_pin', None),
        }
        return output, EXIT_CONDITIONS
    except BaselineError as exc:
        return _revoke_rejected(
            kind, {'baseline_id': getattr(args, 'baseline_id', None)},
            getattr(args, 'manifest_pin', None), exc), EXIT_VALIDATION
    except (sqlite3.Error, OSError) as exc:
        output = _error(kind, 'INVALID_INPUT',
                        CliError('CLI_SQLITE_INVALID', str(exc)))
        output['baseline'] = {
            'status': 'UNKNOWN',
            'baseline_id': getattr(args, 'baseline_id', None),
            'manifest_pin': getattr(args, 'manifest_pin', None),
        }
        return output, EXIT_INPUT


def _run_identity_plan(args, *, clock=None):
    kind = 'historical_baseline_cli_identity_plan'
    sampled = _Clock(clock)
    try:
        sampled()
        policy = _read_policy(args.policy)
        pins = _read_pins(args.pins)
        provider = _source_provider(args, policy, pins, sampled)
        loaded = load_saved_bundle(args.bundle_root)
        identity = normalize_order_identity(loaded, now=sampled())
        source = provider.read()
    except CliError as exc:
        return _error(kind, 'INVALID_INPUT', exc), EXIT_INPUT
    except SourceEvidenceError as exc:
        return _error(kind, 'BLOCKED', exc), EXIT_CONDITIONS
    except BaselineError as exc:
        return _error(kind, 'INVALID_INPUT',
                      CliError(exc.code, exc.detail)), EXIT_INPUT
    except BundleError as exc:
        return _error(kind, 'INVALID_INPUT',
                      CliError(exc.code, exc.detail)), EXIT_INPUT
    result = _result(kind, 'CONDITIONS_MISSING')
    result['bundle'] = _identity_summary(identity)
    result['source'] = _source_summary(source)
    result['conditions_missing'] = True
    result['baseline'] = {'status': 'NOT_ATTEMPTED'}
    blockers = set(identity.get('blockers') or ())
    blockers.update(result['source'].get('blockers') or ())
    if source.get('status') != 'READY':
        result['status'] = 'BLOCKED'
        result['blockers'] = sorted(blockers)
        return result, EXIT_CONDITIONS
    source_account = ((source.get('evidence') or {}).get('source') or {}).get(
        'account_key')
    account = identity.get('account') or {}
    if source_account != account.get('account_key'):
        blockers.add('CLI_ACCOUNT_MISMATCH')
        result['status'] = 'VALIDATION_FAILED'
        result['conditions_missing'] = False
        result['blockers'] = sorted(blockers)
        return result, EXIT_VALIDATION
    if blockers:
        result['blockers'] = sorted(blockers)
        return result, EXIT_CONDITIONS
    try:
        manifest = plan_order_identity_baseline(
            identity, account_key=source_account, cutoff_utc=args.cutoff,
            policy_sha256=digest(policy),
            source_fingerprint=pins['source_fingerprint'],
            evidence=source['evidence'], expires_at=args.expires_at,
            now=sampled())
    except BaselineError as exc:
        result['blockers'] = sorted(set(blockers) | {exc.code})
        result['blocker_detail'] = exc.detail
        result['status'] = ('CONDITIONS_MISSING'
                            if exc.code == 'CONDITIONS_MISSING'
                            else 'VALIDATION_FAILED')
        return result, (EXIT_CONDITIONS
                        if result['status'] == 'CONDITIONS_MISSING'
                        else EXIT_VALIDATION)
    result['status'] = 'PLANNED'
    result['conditions_missing'] = False
    result['blockers'] = []
    result['blocker_detail'] = None
    result['baseline'] = {'status': 'PLANNED', 'plan': manifest}
    return result, EXIT_OK


def _run_plan(args, *, clock=None):
    if getattr(args, 'scope', None) == BASELINE_SCOPE_IDENTITY:
        return _run_identity_plan(args, clock=clock)
    kind = 'historical_baseline_cli_plan'
    sampled = _Clock(clock)
    try:
        sampled()
        policy = _read_policy(args.policy)
        pins = _read_pins(args.pins)
        provider = _source_provider(args, policy, pins, sampled)
        loaded = load_saved_bundle(args.bundle_root)
        bundle = normalize_bundle(loaded, now=sampled())
        source = provider.read()
    except CliError as exc:
        return _error(kind, 'INVALID_INPUT', exc), EXIT_INPUT
    except SourceEvidenceError as exc:
        result = _error(kind, 'BLOCKED', exc)
        result['source'] = {'status': 'BLOCKED',
                            'blockers': [exc.code],
                            'blocker_detail': exc.detail}
        return result, EXIT_CONDITIONS
    except BaselineError as exc:
        return _error(kind, 'INVALID_INPUT',
                      CliError(exc.code, exc.detail)), EXIT_INPUT
    except BundleError as exc:
        return _error(kind, 'INVALID_INPUT',
                      CliError(exc.code, exc.detail)), EXIT_INPUT
    result = _result(kind, 'CONDITIONS_MISSING')
    result['bundle'] = _bundle_summary(bundle)
    result['source'] = _source_summary(source)
    result['conditions_missing'] = True
    result['baseline'] = {'status': 'NOT_ATTEMPTED'}
    blockers = set(result['bundle']['blockers'])
    blockers.update(result['source']['blockers'])
    if source.get('status') != 'READY':
        result['status'] = 'BLOCKED'
        result['blockers'] = sorted(blockers)
        return result, EXIT_CONDITIONS
    source_account = ((source.get('evidence') or {}).get('source') or {}).get(
        'account_key')
    bundle_account = result['bundle'].get('account_key')
    if source_account != bundle_account:
        blockers.add('CLI_ACCOUNT_MISMATCH')
        result['status'] = 'VALIDATION_FAILED'
        result['conditions_missing'] = False
        result['blockers'] = sorted(blockers)
        return result, EXIT_VALIDATION
    snapshot = bundle.get('candidate_snapshot')
    if not blockers and isinstance(snapshot, dict):
        try:
            manifest = plan_baseline(
                snapshot, account_key=source_account,
                cutoff_utc=args.cutoff, policy_sha256=digest(policy),
                source_fingerprint=pins['source_fingerprint'],
                evidence=source['evidence'], expires_at=args.expires_at,
                now=sampled())
            _plan_time_checked(
                snapshot, account_key=source_account, cutoff=args.cutoff,
                expires=args.expires_at, now=sampled())
        except CliError as exc:
            return _error(kind, 'INVALID_INPUT', exc), EXIT_INPUT
        except BaselineError as exc:
            result['blockers'] = sorted(set(blockers) | {exc.code})
            result['blocker_detail'] = exc.detail
            result['status'] = ('CONDITIONS_MISSING'
                                if exc.code == 'CONDITIONS_MISSING'
                                else 'VALIDATION_FAILED')
            return result, (EXIT_CONDITIONS
                            if result['status'] == 'CONDITIONS_MISSING'
                            else EXIT_VALIDATION)
        result['status'] = 'PLANNED'
        result['conditions_missing'] = False
        result['blockers'] = []
        result['blocker_detail'] = None
        result['baseline'] = {'status': 'PLANNED', 'plan': manifest}
        return result, EXIT_OK
    result['blockers'] = sorted(blockers)
    return result, EXIT_CONDITIONS


def _run_validate(args, *, clock=None):
    kind = 'historical_baseline_cli_validate'
    sampled = _Clock(clock)
    try:
        sampled()
        policy = _read_policy(args.policy)
        pins = _read_pins(args.pins)
        manifest = _read_manifest(args.manifest)
        manifest_scope = manifest.get('baseline_scope')
        requested_scope = getattr(args, 'scope', None)
        if requested_scope == BASELINE_SCOPE_IDENTITY \
                and manifest_scope != BASELINE_SCOPE_IDENTITY:
            _fail('CLI_SCOPE_MANIFEST_MISMATCH')
        if requested_scope == BASELINE_SCOPE_FULL \
                and manifest_scope not in (None, BASELINE_SCOPE_FULL):
            _fail('CLI_SCOPE_MANIFEST_MISMATCH')
        if requested_scope is None:
            requested_scope = manifest_scope or BASELINE_SCOPE_FULL
        _manifest_checked(manifest, args.manifest_pin, now=sampled())
        provider = _source_provider(args, policy, pins, sampled)
        if Path(args.db).resolve() != Path(provider.execution_db).resolve():
            _fail('CLI_DB_SOURCE_MISMATCH')
    except CliError as exc:
        return _error(kind, 'INVALID_INPUT', exc), EXIT_INPUT
    except SourceEvidenceError as exc:
        return _error(kind, 'BLOCKED', exc), EXIT_CONDITIONS
    except BaselineError as exc:
        return _error(kind, 'VALIDATION_FAILED',
                      CliError(exc.code, exc.detail)), EXIT_VALIDATION
    except (KeyError, TypeError) as exc:
        return _error(kind, 'VALIDATION_FAILED',
                      CliError('CLI_MANIFEST_INVALID', str(exc))), EXIT_VALIDATION
    try:
        with provider.exclusive_scope() as owner:
            source = provider.read()
            if source.get('status') != 'READY':
                provider.verify_scope(owner)
                output = _error(kind, 'BLOCKED', CliError(
                    (source.get('blockers') or ['SOURCE_BLOCKED'])[0],
                    source.get('blocker_detail')))
                output['source'] = _source_summary(source)
                return output, EXIT_CONDITIONS
            loaded = load_saved_bundle(args.bundle_root)
            if requested_scope == BASELINE_SCOPE_IDENTITY:
                bundle = normalize_order_identity(loaded, now=sampled())
                bundle_summary = _identity_summary(bundle)
                snapshot = bundle
            else:
                bundle = normalize_bundle(loaded, now=sampled())
                bundle_summary = _bundle_summary(bundle)
                snapshot = bundle.get('candidate_snapshot')
            output = _result(kind, 'CONDITIONS_MISSING')
            output['source'] = _source_summary(source)
            output['bundle'] = bundle_summary
            output['manifest_sha256'] = manifest.get('manifest_sha256')
            output['conditions_missing'] = True
            output['baseline'] = {'status': 'NOT_ATTEMPTED'}
            blockers = set(output['bundle']['blockers'])
            if blockers or not isinstance(snapshot, dict):
                output['blockers'] = sorted(blockers)
                provider.verify_scope(owner)
                return output, EXIT_CONDITIONS
            source_account = ((source.get('evidence') or {}).get(
                'source') or {}).get('account_key')
            if source_account != output['bundle'].get('account_key'):
                _fail('CLI_ACCOUNT_MISMATCH')
            db = _open_readonly(provider.execution_db)
            try:
                validation = validate_baseline(
                    db, manifest, manifest_pin=args.manifest_pin,
                    now=sampled(), evidence=source['evidence'],
                    snapshot=snapshot)
            finally:
                db.close()
            # Re-read every moving input under the same held owner boundary.
            current_source = provider.read()
            if current_source.get('status') != 'READY':
                output['status'] = 'BLOCKED'
                output['blockers'] = list(
                    current_source.get('blockers') or ['SOURCE_BLOCKED'])
                output['blocker_detail'] = current_source.get('blocker_detail')
                output['source'] = _source_summary(current_source)
                output['baseline'] = {'status': 'NOT_VALIDATED'}
                return output, EXIT_CONDITIONS
            if canonical(current_source.get('evidence')) \
                    != canonical(source.get('evidence')):
                _fail('CLI_SOURCE_CHANGED_DURING_VALIDATION')
            current_loaded = load_saved_bundle(args.bundle_root)
            if requested_scope == BASELINE_SCOPE_IDENTITY:
                current_bundle = normalize_order_identity(
                    current_loaded, now=sampled())
                current_snapshot = current_bundle
                same_bundle = (_identity_identity(current_bundle)
                               == _identity_identity(bundle))
            else:
                current_bundle = normalize_bundle(current_loaded, now=sampled())
                current_snapshot = current_bundle.get('candidate_snapshot')
                same_bundle = (_bundle_identity(current_bundle)
                               == _bundle_identity(bundle))
            if not same_bundle:
                _fail('CLI_BUNDLE_CHANGED_DURING_VALIDATION')
            current_manifest = _read_manifest(args.manifest)
            if canonical(current_manifest) != canonical(manifest):
                _fail('CLI_MANIFEST_CHANGED_DURING_VALIDATION')
            current_policy = _read_policy(args.policy)
            current_pins = _read_pins(args.pins)
            if canonical(current_policy) != canonical(policy) \
                    or canonical(current_pins) != canonical(pins):
                _fail('CLI_POLICY_OR_PINS_CHANGED_DURING_VALIDATION')
            final_now = sampled()
            _manifest_checked(
                current_manifest, args.manifest_pin, now=final_now)
            if requested_scope == BASELINE_SCOPE_IDENTITY:
                _validate_order_identity_snapshot(
                    current_snapshot,
                    account_key=current_manifest['account_key'],
                    provider=current_manifest['provider'],
                    cutoff=_utc(current_manifest['cutoff_utc'],
                                'BASELINE_CUTOFF_INVALID'),
                    observed_at=_utc(current_manifest['snapshot_observed_at'],
                                     'BASELINE_SNAPSHOT_TIME_INVALID'),
                    now=final_now)
            else:
                _validate_snapshot(
                    current_snapshot,
                    account_key=current_manifest['account_key'],
                    provider=current_manifest['provider'],
                    cutoff=_utc(current_manifest['cutoff_utc'],
                                'BASELINE_CUTOFF_INVALID'),
                    observed_at=_utc(current_manifest['snapshot_observed_at'],
                                     'BASELINE_SNAPSHOT_TIME_INVALID'),
                    now=final_now)
            provider.verify_scope(owner)
            output['status'] = 'VALIDATED'
            output['conditions_missing'] = False
            output['blockers'] = []
            output['baseline'] = validation
            return output, EXIT_OK
    except CliError as exc:
        return _error(kind, 'VALIDATION_FAILED', exc), EXIT_VALIDATION
    except SourceEvidenceError as exc:
        return _error(kind, 'BLOCKED', exc), EXIT_CONDITIONS
    except BaselineError as exc:
        return _error(kind, 'VALIDATION_FAILED',
                      CliError(exc.code, exc.detail)), EXIT_VALIDATION
    except BundleError as exc:
        return _error(kind, 'INVALID_INPUT',
                      CliError(exc.code, exc.detail)), EXIT_INPUT
    except sqlite3.Error as exc:
        return _error(kind, 'VALIDATION_FAILED',
                      CliError('CLI_SQLITE_INVALID', str(exc))), EXIT_VALIDATION


def _run_status(args, *, now=None):
    kind = 'historical_baseline_cli_status'
    try:
        root = _resolve_root(args.root, args.isolated_root)
        db_path = _controlled_db_path(root, args.db, allow_missing=True)
    except (CliError, SourceEvidenceError) as exc:
        if isinstance(exc, SourceEvidenceError):
            exc = CliError(exc.code, exc.detail)
        return _error(kind, 'INVALID_INPUT', exc), EXIT_INPUT
    if not db_path.exists() and not db_path.is_symlink():
        output = _result(kind, 'MISSING')
        output.update({
            'db': {'path': str(db_path), 'exists': False,
                   'sha256': None},
            'baseline': None, 'source_diagnostics': None,
            'source_verified': False,
        })
        output['blockers'] = ['CLI_DB_MISSING']
        return output, EXIT_VALIDATION
    try:
        before = _file_state(db_path)
        db = _open_readonly(db_path)
        try:
            baseline = status_baseline(db)
        finally:
            db.close()
        after = _file_state(db_path)
        if before != after:
            _fail('CLI_DB_CHANGED_DURING_STATUS')
    except (CliError, sqlite3.Error) as exc:
        if not isinstance(exc, CliError):
            exc = CliError('CLI_SQLITE_INVALID', str(exc))
        output = _error(kind, 'INVALID', exc)
        output.update({'db': {'path': str(db_path), 'exists': True,
                              'sha256': None},
                       'baseline': None, 'source_diagnostics': None,
                       'source_verified': False})
        return output, EXIT_VALIDATION
    status = baseline.get('status')
    code = EXIT_OK if status in {'PRESENT', 'ABSENT'} else EXIT_VALIDATION
    output = _result(kind, status if status in {'PRESENT', 'ABSENT'}
                     else 'INVALID')
    output.update({
        'db': {'path': str(db_path), 'exists': True,
               'sha256': after},
        'baseline': baseline, 'source_diagnostics': None,
        'source_verified': False,
    })
    return output, code


def add_parser(subparsers):
    parser = subparsers.add_parser(
        'historical-baseline',
        help='Baseline plan/validate/status plus explicit local apply/revoke')
    commands = parser.add_subparsers(dest='baseline_mode', required=True)

    def source_arguments(command):
        command.add_argument('--bundle-root', required=True,
                             help='Controlled saved official-response bundle')
        command.add_argument('--policy', required=True)
        command.add_argument('--pins', required=True,
                             help='External expected source pins; never derived from target files')
        command.add_argument('--execution-db', required=True)
        command.add_argument('--risk-db', required=True)
        command.add_argument('--host-dir', required=True)
        command.add_argument('--root', help='Controlled source root; canonical by default')
        command.add_argument('--isolated-root', action='store_true',
                             help='Explicit temporary root selection only; does not relax gates')

    plan = commands.add_parser(
        'plan', help='Read saved bundle/source files and print a plan; never writes')
    source_arguments(plan)
    plan.add_argument('--cutoff', required=True)
    plan.add_argument('--expires-at', required=True)
    plan.add_argument('--scope', choices=(BASELINE_SCOPE_FULL,
                                          BASELINE_SCOPE_IDENTITY),
                      default=None,
                      help='Explicit baseline scope; identity is narrow and '
                           'never authorizes entry')

    validate = commands.add_parser(
        'validate', help='Re-read bundle/source and validate an externally pinned manifest')
    source_arguments(validate)
    validate.add_argument('--db', required=True)
    validate.add_argument('--manifest', required=True)
    validate.add_argument('--manifest-pin', required=True)
    validate.add_argument('--scope', choices=(BASELINE_SCOPE_FULL,
                                              BASELINE_SCOPE_IDENTITY),
                          default=None)

    status = commands.add_parser(
        'status', help='Read baseline status from one existing database')
    status.add_argument('--db', required=True)
    status.add_argument('--root', help='Controlled root; canonical by default')
    status.add_argument('--isolated-root', action='store_true',
                        help='Explicit temporary root selection only; does not relax gates')

    apply_command = commands.add_parser(
        'apply',
        help='Explicit local write of one externally pinned baseline; never calls broker')
    source_arguments(apply_command)
    apply_command.add_argument('--db', required=True,
                               help='Existing candidate execution SQLite; must equal the source DB')
    apply_command.add_argument('--manifest', required=True)
    apply_command.add_argument('--manifest-pin', required=True)
    apply_command.add_argument('--scope', choices=(BASELINE_SCOPE_FULL,
                                                   BASELINE_SCOPE_IDENTITY),
                               default=None)
    apply_command.add_argument('--approval-scope', required=True,
                               help='Nonempty local audit scope; not broker approval')

    revoke_command = commands.add_parser(
        'revoke',
        help='Append-only local revoke of the exact active baseline; no broker calls')
    revoke_command.add_argument('--db', required=True,
                                help='Existing execution SQLite; must equal the source DB')
    revoke_command.add_argument('--baseline-id', required=True)
    revoke_command.add_argument('--manifest-pin', required=True)
    revoke_command.add_argument('--reason', required=True)
    revoke_command.add_argument('--approval-scope', required=True,
                                help='Nonempty local audit scope; not broker approval')
    revoke_command.add_argument('--policy', required=True)
    revoke_command.add_argument('--pins', required=True)
    revoke_command.add_argument('--execution-db', required=True)
    revoke_command.add_argument('--risk-db', required=True)
    revoke_command.add_argument('--host-dir', required=True)
    revoke_command.add_argument('--root',
                                help='Controlled source root; canonical by default')
    revoke_command.add_argument('--isolated-root', action='store_true',
                                help='Explicit temporary root selection only; does not relax gates')
    return parser


def run(args):
    try:
        if args.baseline_mode == 'plan':
            output, code = _run_plan(args)
        elif args.baseline_mode == 'validate':
            output, code = _run_validate(args)
        elif args.baseline_mode == 'status':
            output, code = _run_status(args)
        elif args.baseline_mode == 'apply':
            output, code = _run_apply(args)
        elif args.baseline_mode == 'revoke':
            output, code = _run_revoke(args)
        else:
            raise CliError('CLI_MODE_INVALID')
    except CliError as exc:
        output = _error('historical_baseline_cli', 'INVALID_INPUT', exc)
        code = EXIT_INPUT
    except Exception as exc:
        output = _error('historical_baseline_cli', 'INTERNAL_ERROR',
                        CliError('CLI_INTERNAL_ERROR', type(exc).__name__))
        code = EXIT_INPUT
    print(json.dumps(output, ensure_ascii=False, sort_keys=True, indent=2))
    return code
