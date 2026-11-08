"""Local capture evidence, separate from provider authentication and PIT claims.

SQLite writes belong to the caller's transaction. File records are immutable by
convention and verified on reuse; local owners can still rewrite all evidence.
"""
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import shutil
import tempfile
import time
import math

from .audit import canonical, digest
from .ledger import timestamp


def _time(value):
    if not isinstance(value, str):
        raise ValueError('CAPTURE_TIMESTAMP_REQUIRED')
    timestamp(value)
    return value


def _identity(value):
    if not isinstance(value, str) or not value or len(value) > 512:
        raise ValueError('CAPTURE_ID_REQUIRED')
    return value


def _fsync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _immutable_write(path, content):
    """Publish a fully fsynced inode without replacing an existing final file."""
    path = Path(path)
    fd, temporary = tempfile.mkstemp(prefix='.capture-', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.is_symlink() or path.read_bytes() != content:
                raise ValueError('CAPTURE_IMMUTABLE_FILE_CONFLICT')
        _fsync_dir(path.parent)
    finally:
        Path(temporary).unlink(missing_ok=True)


def persist_raw_record(root, payload, *, observed_at, metadata=None):
    """Keep raw bytes and a separate immutable local observation receipt.

    observed_at must be the actual caller observation time. It is not inferred
    from timestamps inside the response, nor called original receipt provenance.
    """
    if not isinstance(payload, bytes):
        raise TypeError('CAPTURE_RAW_BYTES_REQUIRED')
    _time(observed_at)
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    root.chmod(0o700)
    raw_hash = hashlib.sha256(payload).hexdigest()
    raw_name = raw_hash + '.raw'
    _immutable_write(root / raw_name, payload)
    receipt = {
        'version': 1, 'raw_sha256': raw_hash, 'raw_path': raw_name,
        'raw_bytes': len(payload), 'observed_at': observed_at,
        'original_receipt_verified': False, 'scope': 'local_observation',
        'metadata': {} if metadata is None else metadata,
    }
    receipt_bytes = canonical(receipt).encode()
    receipt_hash = hashlib.sha256(receipt_bytes).hexdigest()
    receipt_name = receipt_hash + '.receipt.json'
    _immutable_write(root / receipt_name, receipt_bytes)
    return {**receipt, 'receipt_sha256': receipt_hash, 'receipt_path': receipt_name}


def ensure_archive_schema(db):
    """Create tables without committing or disrupting an existing transaction."""
    db.execute('''CREATE TABLE IF NOT EXISTS live_event_archive(
        id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL,
        payload TEXT NOT NULL, consumed_at TEXT NOT NULL)''')
    db.execute('''CREATE TABLE IF NOT EXISTS live_decision_archive(
        event_id TEXT NOT NULL, result_id TEXT NOT NULL,
        fingerprint TEXT NOT NULL, payload TEXT NOT NULL,
        recorded_at TEXT NOT NULL, PRIMARY KEY(event_id,result_id))''')
    db.execute('''CREATE TABLE IF NOT EXISTS live_state_snapshots(
        fingerprint TEXT PRIMARY KEY, payload TEXT NOT NULL)''')
    db.execute('''CREATE TABLE IF NOT EXISTS live_decision_state_refs(
        event_id TEXT NOT NULL, result_id TEXT NOT NULL, field TEXT NOT NULL,
        fingerprint TEXT NOT NULL, PRIMARY KEY(event_id,result_id,field))''')


def _transaction(db):
    if not db.in_transaction:
        raise ValueError('CAPTURE_OWNER_TRANSACTION_REQUIRED')


def archive_event(db, event, *, consumed_at):
    """Append the complete accepted event inside the consuming transaction."""
    _transaction(db)
    event_id = _identity(event.get('id'))
    _time(consumed_at)
    payload = canonical(event)
    fingerprint = digest(event)
    old = db.execute('SELECT fingerprint,payload FROM live_event_archive WHERE id=?', (event_id,)).fetchone()
    if old:
        if old[0] != fingerprint or old[1] != payload:
            raise ValueError('CAPTURE_EVENT_ID_CONFLICT')
        return fingerprint
    db.execute('INSERT INTO live_event_archive VALUES(?,?,?,?)',
               (event_id, fingerprint, payload, consumed_at))
    return fingerprint


STATE_FIELDS = frozenset({'state_before', 'state_after'})
PREVIEW_LIMIT = 1000


def _tables(db):
    return {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def _snapshot_value(db, fingerprint):
    row = db.execute('SELECT payload FROM live_state_snapshots WHERE fingerprint=?', (fingerprint,)).fetchone()
    if row is None:
        raise ValueError('CAPTURE_STATE_SNAPSHOT_MISSING')
    value = json.loads(row[0])
    if canonical(value) != row[0] or digest(value) != fingerprint:
        raise ValueError('CAPTURE_STATE_SNAPSHOT_INTEGRITY_FAILURE')
    return value


def _decision_context(db, event_id, result_id, fingerprint, encoded, tables):
    context = json.loads(encoded)
    if not isinstance(context, dict) or canonical(context) != encoded:
        raise ValueError('CAPTURE_DECISION_INTEGRITY_FAILURE')
    if 'live_decision_state_refs' in tables:
        for field, state_hash in db.execute(
                'SELECT field,fingerprint FROM live_decision_state_refs WHERE event_id=? AND result_id=? ORDER BY field',
                (event_id, result_id)):
            if field not in STATE_FIELDS or field in context or 'live_state_snapshots' not in tables:
                raise ValueError('CAPTURE_STATE_REFERENCE_INTEGRITY_FAILURE')
            context[field] = _snapshot_value(db, state_hash)
    if digest({'event_id': event_id, 'result_id': result_id, 'context': context}) != fingerprint:
        raise ValueError('CAPTURE_DECISION_INTEGRITY_FAILURE')
    return context


def read_decision_context(db, event_id, result_id):
    """Return the verified original logical context for compact or legacy rows."""
    row = db.execute('SELECT fingerprint,payload FROM live_decision_archive WHERE event_id=? AND result_id=?',
                     (event_id, result_id)).fetchone()
    if row is None:
        raise ValueError('CAPTURE_DECISION_MISSING')
    return _decision_context(db, event_id, result_id, row[0], row[1], _tables(db))


def archive_decision(db, *, event_id, result_id, context, recorded_at):
    """Append a logical decision, deduplicating full before/after state snapshots.

    The decision fingerprint still covers the original hydrated context. Legacy
    inline decisions are read unchanged. No existing decision is rewritten.
    All inserts remain inside the consuming owner's transaction.
    """
    _transaction(db)
    _identity(event_id)
    _identity(result_id)
    _time(recorded_at)
    if not isinstance(context, dict):
        raise ValueError('CAPTURE_DECISION_CONTEXT_REQUIRED')
    if not db.execute('SELECT 1 FROM live_event_archive WHERE id=?', (event_id,)).fetchone():
        raise ValueError('CAPTURE_DECISION_EVENT_MISSING')
    fingerprint = digest({'event_id': event_id, 'result_id': result_id, 'context': context})
    old = db.execute('SELECT fingerprint,payload FROM live_decision_archive WHERE event_id=? AND result_id=?',
                     (event_id, result_id)).fetchone()
    if old:
        if old[0] != fingerprint:
            raise ValueError('CAPTURE_DECISION_ID_CONFLICT')
        if canonical(_decision_context(db, event_id, result_id, old[0], old[1], _tables(db))) != canonical(context):
            raise ValueError('CAPTURE_DECISION_ID_CONFLICT')
        return fingerprint
    compact = {key: value for key, value in context.items() if key not in STATE_FIELDS}
    refs = []
    for field in sorted(STATE_FIELDS.intersection(context)):
        encoded = canonical(context[field])
        state_hash = hashlib.sha256(encoded.encode()).hexdigest()
        prior = db.execute('SELECT payload FROM live_state_snapshots WHERE fingerprint=?', (state_hash,)).fetchone()
        if prior:
            if prior[0] != encoded:
                raise ValueError('CAPTURE_STATE_SNAPSHOT_INTEGRITY_FAILURE')
        else:
            db.execute('INSERT INTO live_state_snapshots VALUES(?,?)', (state_hash, encoded))
        refs.append((event_id, result_id, field, state_hash))
    db.execute('INSERT INTO live_decision_archive VALUES(?,?,?,?,?)',
               (event_id, result_id, fingerprint, canonical(compact), recorded_at))
    db.executemany('INSERT INTO live_decision_state_refs VALUES(?,?,?,?)', refs)
    return fingerprint


def _iter_events(db, tables):
    if 'live_event_archive' not in tables:
        return
    for sequence, row in enumerate(db.execute(
            'SELECT id,fingerprint,payload,consumed_at FROM live_event_archive ORDER BY rowid'), 1):
        event_id, fingerprint, encoded, consumed_at = row
        payload = json.loads(encoded)
        _identity(event_id)
        _time(consumed_at)
        if not isinstance(payload, dict) or payload.get('id') != event_id or digest(payload) != fingerprint or canonical(payload) != encoded:
            raise ValueError('CAPTURE_EVENT_INTEGRITY_FAILURE')
        yield {'capture_sequence': sequence, 'id': event_id, 'fingerprint': fingerprint,
               'payload': payload, 'consumed_at': consumed_at}


def _iter_decisions(db, tables):
    if 'live_decision_archive' not in tables:
        return
    if 'live_event_archive' not in tables and db.execute('SELECT 1 FROM live_decision_archive LIMIT 1').fetchone():
        raise ValueError('CAPTURE_DECISION_INTEGRITY_FAILURE')
    for sequence, row in enumerate(db.execute(
            'SELECT event_id,result_id,fingerprint,payload,recorded_at FROM live_decision_archive ORDER BY rowid'), 1):
        event_id, result_id, fingerprint, encoded, recorded_at = row
        _identity(event_id)
        _identity(result_id)
        _time(recorded_at)
        if not db.execute('SELECT 1 FROM live_event_archive WHERE id=?', (event_id,)).fetchone():
            raise ValueError('CAPTURE_DECISION_INTEGRITY_FAILURE')
        context = _decision_context(db, event_id, result_id, fingerprint, encoded, tables)
        yield {'decision_sequence': sequence, 'event_id': event_id, 'result_id': result_id,
               'fingerprint': fingerprint, 'context': context, 'recorded_at': recorded_at}


def _iter_gaps(db, tables):
    if 'live_inputs' in tables:
        if 'live_event_archive' not in tables:
            for event_id, fingerprint in db.execute('SELECT id,fingerprint FROM live_inputs ORDER BY id'):
                yield {'code': 'LEGACY_EVENT_PAYLOAD_MISSING', 'event_id': event_id, 'fingerprint': fingerprint}
        else:
            for event_id, fingerprint, archived in db.execute(
                    'SELECT i.id,i.fingerprint,e.fingerprint FROM live_inputs i '
                    'LEFT JOIN live_event_archive e ON e.id=i.id ORDER BY i.id'):
                if archived is None:
                    yield {'code': 'LEGACY_EVENT_PAYLOAD_MISSING', 'event_id': event_id, 'fingerprint': fingerprint}
                elif archived != fingerprint:
                    raise ValueError('CAPTURE_LEGACY_FINGERPRINT_CONFLICT')
    if 'live_event_archive' not in tables:
        yield {'code': 'EVENT_ARCHIVE_TABLE_ABSENT'}
    if 'live_decision_archive' not in tables:
        yield {'code': 'DECISION_ARCHIVE_TABLE_ABSENT'}
    if 'live_event_archive' in tables:
        sql = ('SELECT e.id FROM live_event_archive e WHERE NOT EXISTS '
               '(SELECT 1 FROM live_decision_archive d WHERE d.event_id=e.id) ORDER BY e.id'
               if 'live_decision_archive' in tables else 'SELECT id FROM live_event_archive ORDER BY id')
        for (event_id,) in db.execute(sql):
            yield {'code': 'EVENT_DECISION_CONTEXT_MISSING', 'event_id': event_id}


def _iter_snapshots(db, tables):
    if 'live_state_snapshots' not in tables:
        return
    for fingerprint, encoded in db.execute('SELECT fingerprint,payload FROM live_state_snapshots ORDER BY rowid'):
        value = json.loads(encoded)
        if canonical(value) != encoded or digest(value) != fingerprint:
            raise ValueError('CAPTURE_STATE_SNAPSHOT_INTEGRITY_FAILURE')
        yield {'fingerprint': fingerprint, 'payload': value}


def _validate_refs(db, tables):
    if 'live_decision_state_refs' not in tables:
        return
    count = db.execute('SELECT count(*) FROM live_decision_state_refs').fetchone()[0]
    if count and not {'live_decision_archive', 'live_state_snapshots'} <= tables:
        raise ValueError('CAPTURE_STATE_REFERENCE_INTEGRITY_FAILURE')
    if count and db.execute(
            'SELECT 1 FROM live_decision_state_refs r LEFT JOIN live_decision_archive d '
            'ON d.event_id=r.event_id AND d.result_id=r.result_id '
            'LEFT JOIN live_state_snapshots s ON s.fingerprint=r.fingerprint '
            'WHERE d.event_id IS NULL OR s.fingerprint IS NULL OR r.field NOT IN (?,?) LIMIT 1',
            ('state_before', 'state_after')).fetchone():
        raise ValueError('CAPTURE_STATE_REFERENCE_INTEGRITY_FAILURE')


def _stream_rows(path, rows):
    """Publish an NDJSON stream with bounded per-record memory."""
    path = Path(path)
    fd, temporary = tempfile.mkstemp(prefix='.capture-', dir=path.parent)
    hasher = hashlib.sha256()
    size = count = 0
    try:
        with os.fdopen(fd, 'wb') as handle:
            for row in rows:
                content = (canonical(row) + '\n').encode()
                handle.write(content)
                hasher.update(content)
                size += len(content)
                count += 1
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, path)
        _fsync_dir(path.parent)
    finally:
        Path(temporary).unlink(missing_ok=True)
    return {'sha256': hasher.hexdigest(), 'bytes': size, 'records': count}


def _copy_raw_file(source, destination):
    """Verify and copy a raw file without reading its entire payload into RAM."""
    if source.is_symlink() or not source.is_file():
        raise ValueError('CAPTURE_RAW_FILE_INVALID')
    fd, temporary = tempfile.mkstemp(prefix='.capture-', dir=destination.parent)
    hasher = hashlib.sha256()
    size = 0
    try:
        with source.open('rb') as reader, os.fdopen(fd, 'wb') as writer:
            for block in iter(lambda: reader.read(1024 * 1024), b''):
                writer.write(block)
                hasher.update(block)
                size += len(block)
            writer.flush()
            os.fsync(writer.fileno())
        if hasher.hexdigest() != source.name.split('.')[0]:
            raise ValueError('CAPTURE_RAW_INTEGRITY_FAILURE')
        os.link(temporary, destination)
    finally:
        Path(temporary).unlink(missing_ok=True)
    return {'sha256': hasher.hexdigest(), 'bytes': size}


def _private_snapshot(source, stage, timeout_seconds):
    """Copy in short SQLite backup steps, never pinning a live read transaction."""
    if type(timeout_seconds) not in (int, float) or not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise ValueError('CAPTURE_BACKUP_TIMEOUT_REQUIRED')
    fd, filename = tempfile.mkstemp(prefix='.capture-snapshot-', suffix='.sqlite', dir=stage)
    os.close(fd)  # mkstemp's 0600 file stays private, including during backup.
    readonly = target = None
    started = datetime.now(timezone.utc).isoformat()
    deadline = time.monotonic() + timeout_seconds
    def progress(status, remaining, total):
        if time.monotonic() >= deadline:
            raise TimeoutError('CAPTURE_SNAPSHOT_BACKUP_TIMEOUT')
    try:
        readonly = sqlite3.connect(source.as_uri() + '?mode=ro', uri=True, timeout=.1)
        readonly.execute('PRAGMA query_only=ON')
        target = sqlite3.connect(filename)
        readonly.backup(target, pages=256, progress=progress, sleep=.01)
        readonly.close()
        readonly = None
        target.execute('PRAGMA query_only=ON')
        return target, Path(filename), {'backup_started_at': started,
            'backup_completed_at': datetime.now(timezone.utc).isoformat(),
            'backup_timeout_seconds': timeout_seconds, 'backup_step_pages': 256,
            'source_transaction_held_during_export': False}
    except BaseException:
        if target is not None:
            target.close()
        Path(filename).unlink(missing_ok=True)
        raise
    finally:
        if readonly is not None:
            readonly.close()


def export_capture_archive(db_path, output_dir, *, raw_dir=None, backup_timeout_seconds=30):
    """Stream one consistent SQLite snapshot to a never-reused directory.

    A private SQLite backup avoids holding a read transaction against the live
    owner while streaming. Full logical contexts are hydrated one at a time,
    including legacy multi-GB archives. Raw inventory is copied after the DB
    backup and is explicitly a different observation boundary. Hashes establish
    local consistency, never provider or PIT authentication.
    """
    source = Path(db_path).absolute()
    output = Path(output_dir)
    if output.exists():
        raise FileExistsError(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix='.capture-export-', dir=output.parent))
    db = None
    published = False
    try:
        db, snapshot_path, snapshot_metadata = _private_snapshot(source, stage, backup_timeout_seconds)
        db.execute('BEGIN')
        tables = _tables(db)

        _validate_refs(db, tables)
        files = {}
        files['events.ndjson'] = _stream_rows(stage / 'events.ndjson', _iter_events(db, tables))
        files['decisions.ndjson'] = _stream_rows(stage / 'decisions.ndjson', _iter_decisions(db, tables))
        files['gaps.ndjson'] = _stream_rows(stage / 'gaps.ndjson', _iter_gaps(db, tables))
        files['state-snapshots.ndjson'] = _stream_rows(stage / 'state-snapshots.ndjson', _iter_snapshots(db, tables))
        raw_inventory = {}
        raw_inventory_count = 0
        raw_root = None if raw_dir is None else Path(raw_dir)
        if raw_root is not None:
            if not raw_root.is_dir():
                raise ValueError('CAPTURE_RAW_DIRECTORY_MISSING')
            (stage / 'raw').mkdir()

        def raw_rows():
            nonlocal raw_inventory_count
            if raw_root is None:
                return
            # scandir is streamed; no response contents or path list accumulate.
            with os.scandir(raw_root) as paths:
                for entry in paths:
                    if not (entry.name.endswith('.raw') or entry.name.endswith('.receipt.json')):
                        continue
                    info = _copy_raw_file(Path(entry.path), stage / 'raw' / entry.name)
                    raw_inventory_count += 1
                    if len(raw_inventory) < PREVIEW_LIMIT:
                        raw_inventory['raw/' + entry.name] = info
                    yield {'path': 'raw/' + entry.name, **info}
            _fsync_dir(stage / 'raw')

        files['raw-files.ndjson'] = _stream_rows(stage / 'raw-files.ndjson', raw_rows())
        if raw_root is not None:
            # Receipt validation uses the copied verified inventory, avoiding a
            # second read of mutable source files. Each receipt is small local metadata.
            with os.scandir(stage / 'raw') as paths:
                for entry in paths:
                    if not entry.name.endswith('.receipt.json'):
                        continue
                    receipt = json.loads(Path(entry.path).read_bytes())
                    raw_hash = receipt.get('raw_sha256')
                    if not isinstance(raw_hash, str) or len(raw_hash) != 64 or any(c not in '0123456789abcdef' for c in raw_hash):
                        raise ValueError('CAPTURE_RAW_RECEIPT_INVALID')
                    raw_name = raw_hash + '.raw'
                    raw_path = stage / 'raw' / raw_name
                    if receipt.get('raw_path') != raw_name or not raw_path.is_file() or raw_path.stat().st_size != receipt.get('raw_bytes'):
                        raise ValueError('CAPTURE_RAW_RECEIPT_INVALID')
                    _time(receipt['observed_at'])
        raw_gaps = []

        def raw_gap_rows():
            if raw_root is None:
                row = {'code': 'RAW_RESPONSE_PACKAGE_NOT_EMBEDDED'}
                raw_gaps.append(row)
                yield row
                return
            if 'live_event_archive' not in tables:
                return
            for event_id, encoded in db.execute('SELECT id,payload FROM live_event_archive ORDER BY rowid'):
                payload = json.loads(encoded)
                if payload.get('source_type') == 'local_control_diagnostic':
                    continue
                source_hash = payload.get('source_sha256', '')
                present = (isinstance(source_hash, str) and len(source_hash) == 64
                           and all(c in '0123456789abcdef' for c in source_hash)
                           and (stage / 'raw' / (source_hash + '.raw')).is_file())
                if not present:
                    row = {'code': 'EVENT_RAW_SOURCE_MISSING', 'event_id': event_id}
                    if len(raw_gaps) < PREVIEW_LIMIT:
                        raw_gaps.append(row)
                    yield row

        files['raw-gaps.ndjson'] = _stream_rows(stage / 'raw-gaps.ndjson', raw_gap_rows())
        db.rollback()
        db.close()
        db = None
        snapshot_path.unlink()
        manifest = {
            'version': 2, 'scope': 'local_sqlite_capture_snapshot', 'source_db': str(source),
            'exported_at': datetime.now(timezone.utc).isoformat(), 'snapshot': snapshot_metadata,
            'event_count': files['events.ndjson']['records'],
            'decision_count': files['decisions.ndjson']['records'],
            'gap_count': files['gaps.ndjson']['records'],
            'state_snapshot_count': files['state-snapshots.ndjson']['records'],
            'files': files, 'provider_authenticated': False, 'global_feed_complete': False,
            'actual_point_in_time_verified': False, 'original_receipt_timestamps_verified': False,
            'production_ready': False, 'external_immutable_custody': False,
            'raw_responses_embedded': raw_dir is not None, 'raw_files': raw_inventory,
            'raw_file_count': raw_inventory_count,
            'raw_files_truncated': raw_inventory_count > PREVIEW_LIMIT,
            'raw_gaps': raw_gaps, 'raw_gap_count': files['raw-gaps.ndjson']['records'],
            'raw_gaps_truncated': files['raw-gaps.ndjson']['records'] > PREVIEW_LIMIT,
            'inline_raw_preview_limit': PREVIEW_LIMIT,
            'raw_inventory_scope': 'Streamed inventory observed after SQLite snapshot; may contain unconsumed or later responses',
            'coverage_gaps': ['EXTERNAL_FEED_COVERAGE_UNVERIFIED', 'LOCAL_ARCHIVE_IS_NOT_GLOBAL_PIT_PROOF'],
        }
        _immutable_write(stage / 'manifest.json', (canonical(manifest) + '\n').encode())
        _fsync_dir(stage)
        # Only reserve the destination after all validation passes. mkdir is
        # exclusive; no existing destination or evidence package is replaced.
        output.mkdir(mode=0o700, exist_ok=False)
        published = True
        for child in stage.iterdir():
            if child.name != 'manifest.json':
                child.rename(output / child.name)
        (stage / 'manifest.json').rename(output / 'manifest.json')
        _fsync_dir(output)
        _fsync_dir(output.parent)
        return manifest
    except BaseException:
        if published:
            shutil.rmtree(output, ignore_errors=True)
        raise
    finally:
        if db is not None:
            db.close()
        shutil.rmtree(stage, ignore_errors=True)
