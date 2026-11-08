"""Local, stopped-owner runtime checkpoint and preflight.

This is deliberately separate from the audit-only backup profile. A runtime
checkpoint includes the execution SQLite snapshot, the account-risk sidecar,
and the complete Host mailbox tree, then binds them to one generation and
source. It is a local consistency boundary, not a claim of cross-database
atomicity, production restore qualification, or authority to trade.

The caller must stop the owner before creating a checkpoint. This module never
opens a broker, market feed, Host, MCP server, or a real runtime directory.
"""
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import sqlite3
import stat
import tempfile

from .audit import canonical, digest
from .capture_archive import _private_snapshot
from .qualification import source_fingerprint as current_source_fingerprint
from .sqlite_readonly import SQLiteReadOnlyError, readonly_uri
from trading_contracts.values import number

SCHEMA_VERSION = 1
PROFILE = 'local_runtime_checkpoint_v1'
_RUNTIME_TABLES = ('live_state', 'live_actions', 'execution_intents',
                   'native_operation_records', 'external_operations',
                   'instance_fills', 'execution_fill_identity')
_HOST_REQUIRED = ('manifest.json',)
_COMPLETE = 'COMPLETE'


def _sync_dir(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _read(path, limit=8 * 1024 * 1024):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as handle:
        info = os.fstat(handle.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > limit:
            raise ValueError('RUNTIME_CHECKPOINT_FILE_INVALID')
        body = handle.read(limit + 1)
    if len(body) > limit:
        raise ValueError('RUNTIME_CHECKPOINT_FILE_TOO_LARGE')
    return body


def _hash(path):
    h = hashlib.sha256()
    size = 0
    with path.open('rb') as handle:
        while True:
            block = handle.read(1024 * 1024)
            if not block:
                break
            h.update(block)
            size += len(block)
    return {'sha256': h.hexdigest(), 'bytes': size}


def _load_json(path):
    try:
        value = json.loads(_read(path))
    except (OSError, ValueError):
        raise ValueError('RUNTIME_CHECKPOINT_JSON_INVALID') from None
    if not isinstance(value, dict):
        raise ValueError('RUNTIME_CHECKPOINT_JSON_INVALID')
    return value


def _db(path, *, tables=()):
    try:
        uri = readonly_uri(path)
    except SQLiteReadOnlyError as exc:
        code = ('RUNTIME_CHECKPOINT_WAL_NOT_CONVERGED'
                if exc.code == 'SQLITE_SIDECAR_NOT_CONVERGED'
                else 'RUNTIME_CHECKPOINT_SQLITE_SIDECAR_INVALID')
        raise ValueError(code + (':' + str(exc.detail)
                                if exc.detail is not None else '')) from None
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=.5)
        connection.execute('PRAGMA query_only=ON')
        if connection.execute('PRAGMA integrity_check').fetchall() != [('ok',)]:
            raise ValueError('RUNTIME_CHECKPOINT_SQLITE_INTEGRITY_FAILED')
        for table in tables:
            if not connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                    (table,)).fetchone():
                raise ValueError('RUNTIME_CHECKPOINT_TABLE_MISSING:' + table)
        return connection
    except sqlite3.Error:
        raise ValueError('RUNTIME_CHECKPOINT_SQLITE_INVALID') from None


def _reject_unconverged_wal(path):
    for suffix in ('-wal', '-shm', '-journal'):
        sidecar = Path(str(path) + suffix)
        if sidecar.is_symlink() or (sidecar.exists() and not sidecar.is_file()):
            raise ValueError('RUNTIME_CHECKPOINT_SQLITE_SIDECAR_INVALID:' + suffix)
        if sidecar.exists() and sidecar.stat().st_size:
            raise ValueError('RUNTIME_CHECKPOINT_WAL_NOT_CONVERGED:' + suffix)


def _snapshot_db(source, destination, timeout_seconds):
    source = Path(source).absolute()
    if source.is_symlink() or not source.is_file():
        raise ValueError('RUNTIME_CHECKPOINT_SQLITE_SOURCE_INVALID')
    connection = None
    try:
        connection, snapshot, metadata = _private_snapshot(
            source, destination.parent, timeout_seconds)
        connection.execute('PRAGMA query_only=OFF')
        if connection.execute('PRAGMA journal_mode=DELETE').fetchone()[0] != 'delete':
            raise ValueError('RUNTIME_CHECKPOINT_STANDALONE_SQLITE_REQUIRED')
        connection.close()
        connection = None
        snapshot.replace(destination)
        os.chmod(destination, 0o600)
        _db(destination)
        return metadata
    finally:
        if connection is not None:
            connection.close()


def _risk_state(path, account_suffix):
    connection = _db(path, tables=('account_risk',))
    try:
        row = connection.execute('SELECT payload FROM account_risk WHERE id=1').fetchone()
    finally:
        connection.close()
    if not row:
        raise ValueError('RUNTIME_CHECKPOINT_RISK_STATE_REQUIRED')
    try:
        state = json.loads(row[0])
    except (TypeError, ValueError):
        raise ValueError('RUNTIME_CHECKPOINT_RISK_STATE_INVALID') from None
    required = ('account_key', 'terms', 'blocked', 'day', 'day_start_equity',
                'nav_high', 'daily_halted', 'project_halted', 'equity', 'cash')
    if (not isinstance(state, dict)
            or any(key not in state for key in required)
            or not isinstance(state.get('account_key'), str)
            or not isinstance(state.get('terms'), str)
            or type(state.get('blocked')) is not bool
            or type(state.get('daily_halted')) is not bool
            or type(state.get('project_halted')) is not bool
            or not isinstance(state.get('day'), str)
            or not isinstance(state.get('nav_high'), str)
            or not isinstance(state.get('day_start_equity'), str)
            or not isinstance(state.get('equity'), str)
            or not isinstance(state.get('cash'), str)):
        raise ValueError('RUNTIME_CHECKPOINT_RISK_STATE_INVALID')
    values = []
    for key in ('nav_high', 'day_start_equity', 'equity', 'cash'):
        try:
            values.append(number(state[key]))
        except (ValueError, TypeError):
            raise ValueError('RUNTIME_CHECKPOINT_RISK_STATE_INVALID') from None
    if any(value < 0 for value in values) or values[0] <= 0:
        raise ValueError('RUNTIME_CHECKPOINT_RISK_STATE_INVALID')
    return state


def _host_inventory(root):
    root = Path(root)
    if root.is_symlink() or not root.is_dir():
        raise ValueError('RUNTIME_CHECKPOINT_HOST_DIRECTORY_INVALID')
    files = {}
    total = 0
    for path in sorted(root.rglob('*')):
        if path.is_symlink():
            raise ValueError('RUNTIME_CHECKPOINT_HOST_SYMLINK')
        if path.is_dir():
            continue
        if not path.is_file():
            raise ValueError('RUNTIME_CHECKPOINT_HOST_FILE_INVALID')
        record = _hash(path)
        total += record['bytes']
        if record['bytes'] > 64 * 1024 * 1024 or total > 512 * 1024 * 1024:
            raise ValueError('RUNTIME_CHECKPOINT_HOST_TOO_LARGE')
        files[path.relative_to(root).as_posix()] = record
    if not files:
        raise ValueError('RUNTIME_CHECKPOINT_HOST_EMPTY')
    for name in _HOST_REQUIRED:
        if name not in files:
            raise ValueError('RUNTIME_CHECKPOINT_HOST_MANIFEST_REQUIRED')
    for name in ('requests', 'deliveries'):
        folder = root / name
        if (not folder.is_dir() or folder.is_symlink()
                or not any(folder.glob('*.json'))):
            raise ValueError('RUNTIME_CHECKPOINT_HOST_MAILBOX_EMPTY')
    return files


def _sql_value(value):
    if isinstance(value, bytes):
        return {'$bytes': value.hex()}
    if isinstance(value, memoryview):
        return {'$bytes': bytes(value).hex()}
    if isinstance(value, float) and not math.isfinite(value):
        return {'$float': repr(value)}
    return value


def _table_rows(db_path, tables=()):
    connection = _db(db_path, tables=tables)
    try:
        names = [row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name").fetchall()]
        parts = {}
        for table in names:
            rows = [[_sql_value(value) for value in row]
                    for row in connection.execute('SELECT * FROM ' + table).fetchall()]
            rows.sort(key=lambda row: canonical(row))
            parts[table] = rows
        return parts
    finally:
        connection.close()


def _full_generation(db_path, risk_path, host_dir, *, account_suffix,
                     source_fingerprint, policy_sha256):
    binding = _host_binding(host_dir)
    if binding.get('policy_sha256') != policy_sha256:
        raise ValueError('RUNTIME_CHECKPOINT_POLICY_MISMATCH')
    if (binding.get('account') not in (None, '••••' + account_suffix)
            or binding.get('source_fingerprint') != source_fingerprint):
        raise ValueError('RUNTIME_CHECKPOINT_HOST_BINDING_MISMATCH')
    risk = _risk_state(risk_path, account_suffix)
    return digest({
        'schema_version': SCHEMA_VERSION, 'profile': PROFILE,
        'account_suffix': account_suffix,
        'source_fingerprint': source_fingerprint,
        'policy_sha256': policy_sha256,
        'execution_tables': _table_rows(db_path, _RUNTIME_TABLES),
        'risk_tables': _table_rows(risk_path),
        'risk_state': risk,
        'host_binding': binding,
        'host_files': _host_inventory(host_dir),
    })


def _host_binding(host_dir):
    manifest_path = host_dir / 'manifest.json'
    manifest = _load_json(manifest_path)
    binding = manifest.get('binding')
    if (not isinstance(binding, dict) or not binding.get('account_key')
            or not isinstance(binding.get('db'), str)
            or not binding.get('account')
            or not isinstance(binding.get('policy_sha256'), str)
            or not isinstance(binding.get('source_fingerprint'), str)
            or not isinstance(binding.get('epoch'), str)):
        raise ValueError('RUNTIME_CHECKPOINT_HOST_BINDING_INVALID')
    return binding


def create_runtime_checkpoint(db_path, host_dir, risk_path, output_dir, *,
                              account_suffix, timeout_seconds=30):
    """Create a verified local checkpoint from stopped owner artifacts."""
    if not isinstance(account_suffix, str) or not re.fullmatch('[0-9]{4}', account_suffix):
        raise ValueError('RUNTIME_CHECKPOINT_ACCOUNT_SUFFIX_INVALID')
    db = Path(db_path).absolute()
    host = Path(host_dir).absolute()
    risk = Path(risk_path).absolute()
    output = Path(output_dir).absolute()
    if output.exists() or output.is_symlink():
        raise FileExistsError(output)
    if not risk.is_file() or risk.is_symlink():
        raise ValueError('RUNTIME_CHECKPOINT_RISK_SIDECAR_REQUIRED')
    if risk.name != 'account-risk-' + account_suffix + '.sqlite':
        raise ValueError('RUNTIME_CHECKPOINT_RISK_SIDECAR_NAME_INVALID')
    _reject_unconverged_wal(db)
    _reject_unconverged_wal(risk)
    _host_inventory(host)
    risk_state = _risk_state(risk, account_suffix)
    binding = _host_binding(host)
    if binding['source_fingerprint'] != current_source_fingerprint():
        raise ValueError('RUNTIME_CHECKPOINT_SOURCE_MISMATCH')
    if binding.get('account') not in (None, '••••' + account_suffix):
        raise ValueError('RUNTIME_CHECKPOINT_ACCOUNT_MISMATCH')
    if Path(binding['db']).resolve() != db.resolve():
        raise ValueError('RUNTIME_CHECKPOINT_HOST_DB_MISMATCH')
    if risk_state['account_key'] != binding['account_key']:
        raise ValueError('RUNTIME_CHECKPOINT_RISK_ACCOUNT_MISMATCH')
    output.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix='.runtime-checkpoint-', dir=output.parent))
    stage.chmod(0o700)
    try:
        execution_name = 'execution.sqlite'
        risk_name = 'account-risk-' + account_suffix + '.sqlite'
        _snapshot_db(db, stage / execution_name, timeout_seconds)
        _snapshot_db(risk, stage / risk_name, timeout_seconds)
        shutil.copytree(host, stage / 'host', symlinks=False)
        _host_inventory(stage / 'host')
        files = {}
        for path in sorted(stage.rglob('*')):
            if path.is_file() and path.name not in {'manifest.json', _COMPLETE}:
                files[path.relative_to(stage).as_posix()] = _hash(path)
        # The checkpoint manifest is itself outside the file inventory; COMPLETE
        # commits its hash after all payload files exist.
        manifest = {
            'schema_version': SCHEMA_VERSION, 'profile': PROFILE,
            'created_at': datetime.now(timezone.utc).isoformat(),
            'account_suffix': account_suffix,
            'source_fingerprint': binding['source_fingerprint'],
            'host_binding': binding,
            'host_files': _host_inventory(stage / 'host'),
            'files': files,
            'source_execution_path': str(db),
            'state_generation': _full_generation(
                stage / execution_name, stage / risk_name, stage / 'host',
                account_suffix=account_suffix,
                source_fingerprint=binding['source_fingerprint'],
                policy_sha256=binding['policy_sha256']),
            'risk': {'state_sha256': digest(risk_state),
                     'account_key': risk_state['account_key'],
                     'blocked': risk_state['blocked'],
                     'daily_halted': bool(risk_state.get('daily_halted')),
                     'project_halted': bool(risk_state.get('project_halted'))},
            'execution_sqlite': execution_name,
            'account_risk_sidecar': risk_name,
            'host_mailbox': 'host',
            'local_only': True,
            'trading_runtime_restore_qualified': True,
            'formal_production_restore_qualified': False,
            'automatic_execution_authorized': False,
            'stale_after_newer_generation': True,
        }
        raw = (canonical(manifest) + '\n').encode()
        manifest_sha = hashlib.sha256(raw).hexdigest()
        (stage / 'manifest.json').write_bytes(raw)
        os.chmod(stage / 'manifest.json', 0o600)
        (stage / _COMPLETE).write_text(
            canonical({'schema_version': 1, 'manifest_sha256': manifest_sha}) + '\n')
        os.chmod(stage / _COMPLETE, 0o600)
        _sync_dir(stage)
        verify_runtime_checkpoint(stage, expected_manifest_sha256=manifest_sha,
                                  expected_state_generation=manifest['state_generation'])
        stage.rename(output)
        _sync_dir(output.parent)
        return {'status': 'RUNTIME_CHECKPOINT_CREATED', 'directory': str(output),
                'manifest_sha256': manifest_sha,
                'state_generation': manifest['state_generation'],
                'risk_blocked': manifest['risk']['blocked'],
                'trading_runtime_restore_qualified': True,
                'formal_production_restore_qualified': False,
                'automatic_execution_authorized': False}
    finally:
        if stage.exists():
            shutil.rmtree(stage)


def runtime_generation(db_path, host_dir, risk_path, *, account_suffix):
    """Read-only full generation of one stopped source boundary."""
    binding = _host_binding(Path(host_dir))
    if Path(binding['db']).resolve() != Path(db_path).resolve():
        raise ValueError('RUNTIME_CHECKPOINT_HOST_DB_MISMATCH')
    return _full_generation(
        Path(db_path), Path(risk_path), Path(host_dir),
        account_suffix=account_suffix,
        source_fingerprint=binding['source_fingerprint'],
        policy_sha256=binding['policy_sha256'])


def _verify_checkpoint(directory, *, expected_manifest_sha256=None,
                       expected_source_fingerprint=None,
                       expected_state_generation=None, require_pin=True):
    if require_pin and (not isinstance(expected_manifest_sha256, str)
                        or not re.fullmatch('[0-9a-f]{64}', expected_manifest_sha256)):
        raise ValueError('RUNTIME_CHECKPOINT_TRUSTED_MANIFEST_REQUIRED')
    if require_pin and (not isinstance(expected_state_generation, str)
                        or not expected_state_generation):
        raise ValueError('RUNTIME_CHECKPOINT_LATEST_HEAD_REQUIRED')
    directory = Path(directory).absolute()
    if directory.is_symlink() or not directory.is_dir():
        raise ValueError('RUNTIME_CHECKPOINT_DIRECTORY_INVALID')
    if any((directory / name).is_symlink() for name in
           ('manifest.json', _COMPLETE, 'execution.sqlite')):
        raise ValueError('RUNTIME_CHECKPOINT_SYMLINK')
    manifest = _load_json(directory / 'manifest.json')
    complete = _load_json(directory / _COMPLETE)
    if (manifest.get('schema_version') != SCHEMA_VERSION
            or manifest.get('profile') != PROFILE
            or manifest.get('local_only') is not True
            or manifest.get('trading_runtime_restore_qualified') is not True
            or manifest.get('formal_production_restore_qualified') is not False
            or manifest.get('automatic_execution_authorized') is not False):
        raise ValueError('RUNTIME_CHECKPOINT_MANIFEST_INVALID')
    manifest_sha = hashlib.sha256((directory / 'manifest.json').read_bytes()).hexdigest()
    if complete != {'schema_version': 1, 'manifest_sha256': manifest_sha}:
        raise ValueError('RUNTIME_CHECKPOINT_COMPLETE_INVALID')
    if (expected_manifest_sha256 is not None
            and manifest_sha != expected_manifest_sha256):
        raise ValueError('RUNTIME_CHECKPOINT_TRUSTED_MANIFEST_MISMATCH')
    if (expected_source_fingerprint is not None
            and manifest.get('source_fingerprint') != expected_source_fingerprint):
        raise ValueError('RUNTIME_CHECKPOINT_SOURCE_MISMATCH')
    files = manifest.get('files')
    if (not isinstance(files, dict) or not files
            or manifest.get('execution_sqlite') != 'execution.sqlite'
            or manifest.get('account_risk_sidecar') !=
            'account-risk-' + str(manifest.get('account_suffix')) + '.sqlite'
            or manifest.get('host_mailbox') != 'host'
            or not isinstance(manifest.get('risk'), dict)
            or set(manifest['risk']) != {'state_sha256', 'account_key',
                                         'blocked', 'daily_halted',
                                         'project_halted'}):
        raise ValueError('RUNTIME_CHECKPOINT_FILE_INVENTORY_INVALID')
    for name in files:
        if (not isinstance(name, str) or not name or Path(name).is_absolute()
                or '..' in Path(name).parts):
            raise ValueError('RUNTIME_CHECKPOINT_FILE_INVENTORY_INVALID')
    actual = {path.relative_to(directory).as_posix() for path in directory.rglob('*')
              if path.is_file() and path.name not in {'manifest.json', _COMPLETE}}
    if actual != set(files):
        raise ValueError('RUNTIME_CHECKPOINT_FILE_SET_MISMATCH')
    for name, record in files.items():
        path = directory / name
        if path.is_symlink() or not path.is_file() or _hash(path) != record:
            raise ValueError('RUNTIME_CHECKPOINT_FILE_HASH_MISMATCH:' + name)
    execution = directory / manifest['execution_sqlite']
    risk = directory / manifest['account_risk_sidecar']
    _db(execution, tables=_RUNTIME_TABLES)
    risk_state = _risk_state(risk, manifest['account_suffix'])
    if (digest(risk_state) != manifest['risk']['state_sha256']
            or risk_state['account_key'] != manifest['risk']['account_key']
            or bool(risk_state.get('blocked')) != manifest['risk']['blocked']
            or bool(risk_state.get('daily_halted')) !=
            manifest['risk']['daily_halted']
            or bool(risk_state.get('project_halted')) !=
            manifest['risk']['project_halted']):
        raise ValueError('RUNTIME_CHECKPOINT_RISK_STATE_MISMATCH')
    host = directory / manifest['host_mailbox']
    host_binding = _host_binding(host)
    if host_binding != manifest['host_binding']:
        raise ValueError('RUNTIME_CHECKPOINT_HOST_BINDING_MISMATCH')
    if (Path(str(manifest.get('source_execution_path'))).resolve() !=
            Path(str(host_binding.get('db'))).resolve()
            or manifest.get('source_fingerprint') !=
            host_binding.get('source_fingerprint')
            or host_binding.get('account') not in
            (None, '••••' + str(manifest.get('account_suffix')))
            or risk_state['account_key'] != host_binding.get('account_key')):
        raise ValueError('RUNTIME_CHECKPOINT_HOST_BINDING_MISMATCH')
    host_inventory = _host_inventory(host)
    if host_inventory != manifest['host_files']:
        raise ValueError('RUNTIME_CHECKPOINT_HOST_INVENTORY_MISMATCH')
    generation = _full_generation(
        execution, risk, host, account_suffix=manifest['account_suffix'],
        source_fingerprint=manifest['source_fingerprint'],
        policy_sha256=host_binding['policy_sha256'])
    if generation != manifest['state_generation']:
        raise ValueError('RUNTIME_CHECKPOINT_STATE_GENERATION_MISMATCH')
    if (expected_state_generation is not None
            and generation != expected_state_generation):
        raise ValueError('RUNTIME_CHECKPOINT_NEWER_GENERATION_REQUIRED')
    return {'status': 'RUNTIME_CHECKPOINT_VERIFIED', 'directory': str(directory),
            'manifest_sha256': manifest_sha, 'state_generation': generation,
            'source_fingerprint': manifest['source_fingerprint'],
            'risk_blocked': manifest['risk']['blocked'],
            'trading_runtime_restore_qualified': bool(require_pin),
            'formal_production_restore_qualified': False,
            'automatic_execution_authorized': False}


def verify_runtime_checkpoint(directory, *, expected_manifest_sha256=None,
                              expected_source_fingerprint=None,
                              expected_state_generation=None):
    """Qualified verification requires an independently retained manifest pin."""
    return _verify_checkpoint(
        directory, expected_manifest_sha256=expected_manifest_sha256,
        expected_source_fingerprint=expected_source_fingerprint,
        expected_state_generation=expected_state_generation, require_pin=True)


def inspect_runtime_checkpoint(directory, *, expected_source_fingerprint=None):
    """Offline inspection: never claims restore qualification or write authority."""
    result = _verify_checkpoint(
        directory, expected_source_fingerprint=expected_source_fingerprint,
        require_pin=False)
    return {**result, 'status': 'RUNTIME_CHECKPOINT_INSPECTED',
            'offline_inspection_only': True,
            'trading_runtime_restore_qualified': False,
            'requires_reconciliation': True, 'writes_authorized': False,
            'automatic_execution_authorized': False}


def preflight_runtime_checkpoint(directory, *, expected_manifest_sha256=None,
                                 expected_state_generation=None,
                                 expected_source_fingerprint=None,
                                 artifact_directory=None,
                                 expected_artifact_manifest_sha256=None,
                                 expected_artifact_input_set_sha256=None):
    """Read-only qualified preflight with independent pin and latest head."""
    if (not isinstance(expected_state_generation, str)
            or not expected_state_generation):
        raise ValueError('RUNTIME_CHECKPOINT_LATEST_HEAD_REQUIRED')
    checked = verify_runtime_checkpoint(
        directory, expected_manifest_sha256=expected_manifest_sha256,
        expected_source_fingerprint=expected_source_fingerprint,
        expected_state_generation=expected_state_generation)
    result = {**checked, 'restore_requires_stopped_owner': True,
              'requires_reconciliation': True, 'writes_authorized': False}
    if artifact_directory is None:
        return result
    artifact = Path(artifact_directory)
    manifest_path = artifact / 'manifest.json'
    if not manifest_path.is_file() or manifest_path.is_symlink():
        raise ValueError('RUNTIME_CHECKPOINT_ARTIFACT_MANIFEST_REQUIRED')
    payload = _load_json(manifest_path)
    old_source = payload.get('source_fingerprint')
    if not isinstance(old_source, str) or not old_source:
        raise ValueError('RUNTIME_CHECKPOINT_ARTIFACT_SOURCE_REQUIRED')
    if old_source != checked['source_fingerprint']:
        raise ValueError('RUNTIME_CHECKPOINT_ARTIFACT_SOURCE_MISMATCH')
    if payload.get('admission_schema_version') != 2:
        raise ValueError('RUNTIME_CHECKPOINT_ARTIFACT_SCHEMA_UNKNOWN')
    if (not isinstance(expected_artifact_manifest_sha256, str)
            or not re.fullmatch('[0-9a-f]{64}', expected_artifact_manifest_sha256)):
        raise ValueError('RUNTIME_CHECKPOINT_ARTIFACT_TRUSTED_PIN_REQUIRED')
    manifest_sha = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    if manifest_sha != expected_artifact_manifest_sha256:
        raise ValueError('RUNTIME_CHECKPOINT_ARTIFACT_TRUSTED_PIN_MISMATCH')
    if (not isinstance(expected_artifact_input_set_sha256, str)
            or not re.fullmatch('[0-9a-f]{64}', expected_artifact_input_set_sha256)
            or payload.get('input_set_sha256') != expected_artifact_input_set_sha256):
        raise ValueError('RUNTIME_CHECKPOINT_ARTIFACT_INPUT_PIN_REQUIRED')
    return {**result, 'artifact_compatible': True,
            'artifact_source_fingerprint': old_source,
            'artifact_manifest_sha256': manifest_sha,
            'artifact_input_set_sha256': expected_artifact_input_set_sha256}


def restore_runtime_checkpoint(directory, destination, *,
                               expected_manifest_sha256=None,
                               expected_state_generation=None,
                               expected_source_fingerprint=None):
    """Restore to a new directory without enabling execution."""
    verified = preflight_runtime_checkpoint(
        directory, expected_manifest_sha256=expected_manifest_sha256,
        expected_source_fingerprint=expected_source_fingerprint,
        expected_state_generation=expected_state_generation)
    source = Path(directory).absolute()
    target = Path(destination).absolute()
    if target.exists() or target.is_symlink():
        raise FileExistsError(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix='.runtime-restore-', dir=target.parent))
    try:
        manifest = _load_json(source / 'manifest.json')
        for name in (manifest['execution_sqlite'],
                     manifest['account_risk_sidecar']):
            shutil.copy2(source / name, stage / name)
        shutil.copytree(source / manifest['host_mailbox'], stage / 'host',
                        symlinks=False)
        receipt = {'schema_version': 1, 'status': 'RUNTIME_CHECKPOINT_RESTORED',
                   'checkpoint_manifest_sha256': verified['manifest_sha256'],
                   'state_generation': verified['state_generation'],
                   'source_fingerprint': verified['source_fingerprint'],
                   'requires_reconciliation': True,
                   'writes_authorized': False,
                   'automatic_execution_authorized': False}
        (stage / 'restore-receipt.json').write_text(
            canonical(receipt) + '\n')
        os.chmod(stage / 'restore-receipt.json', 0o600)
        _sync_dir(stage)
        stage.rename(target)
        _sync_dir(target.parent)
        return {**receipt, 'destination': str(target),
                'host_mailbox': str(target / 'host'),
                'execution_sqlite': str(target / manifest['execution_sqlite']),
                'account_risk_sidecar': str(
                    target / manifest['account_risk_sidecar'])}
    finally:
        if stage.exists():
            shutil.rmtree(stage)
