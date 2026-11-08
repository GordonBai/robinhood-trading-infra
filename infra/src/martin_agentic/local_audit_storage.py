"""Private local audit database backups; not independent immutable custody.

The COMPLETE marker is the atomic commit point. An incomplete directory is not
a backup and is never restored. A local filesystem owner can rewrite a backup
and its manifest; a trusted manifest hash must be retained separately to detect
that. No broker, risk sidecar, credentials or runtime process is opened here.
"""
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import stat
import tempfile
import time
import uuid

from .audit import AuditTrail,canonical
from .capture_archive import _private_snapshot
from .ledger import timestamp

FILES={'audit.sqlite','manifest.json','COMPLETE'}
FLAGS={'local_only':True,'independent_immutable_storage_verified':False,
       'trading_runtime_restore_qualified':False,'account_risk_sidecar_included':False,
       'automatic_execution_authorized':False}


def _sync_directory(path):
    fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    try:os.fsync(fd)
    finally:os.close(fd)


def _write(path,body):
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,'wb') as f:
        f.write(body);f.flush();os.fsync(f.fileno())


def _read(path,max_bytes):
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(fd,'rb') as f:
        info=os.fstat(f.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size>max_bytes:
            raise ValueError('LOCAL_AUDIT_BACKUP_FILE_INVALID')
        body=f.read(max_bytes+1)
    if len(body)>max_bytes:raise ValueError('LOCAL_AUDIT_BACKUP_FILE_TOO_LARGE')
    return body


def _hash(path,*,deadline=None):
    h=hashlib.sha256();size=0
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(fd,'rb') as f:
        if not stat.S_ISREG(os.fstat(f.fileno()).st_mode):
            raise ValueError('LOCAL_AUDIT_BACKUP_FILE_INVALID')
        while True:
            if deadline is not None and time.monotonic()>=deadline:
                raise TimeoutError('LOCAL_AUDIT_BACKUP_TIMEOUT')
            block=f.read(1024*1024)
            if not block:break
            h.update(block);size+=len(block)
    return {'sha256':h.hexdigest(),'bytes':size}


def _check_db(path,*,timeout_seconds=30):
    deadline=time.monotonic()+timeout_seconds
    connection=sqlite3.connect(path.absolute().as_uri()+'?mode=ro&immutable=1',uri=True,timeout=.1)
    try:
        connection.execute('PRAGMA query_only=ON')
        connection.set_progress_handler(lambda:int(time.monotonic()>=deadline),1000)
        if connection.execute('PRAGMA integrity_check').fetchall()!=[('ok',)]:
            raise ValueError('LOCAL_AUDIT_SQLITE_INTEGRITY_FAILED')
        audit=object.__new__(AuditTrail);audit.db=connection
        verified=audit.verify()
        last=connection.execute('SELECT sequence FROM audit_events ORDER BY sequence DESC LIMIT 1').fetchone()
        return {**verified,'head_sequence':last[0] if last else 0}
    except sqlite3.Error:
        raise ValueError('LOCAL_AUDIT_DATABASE_INVALID') from None
    finally:connection.close()


def _metadata(directory,expected_manifest_sha256):
    directory=Path(directory).absolute()
    if directory.is_symlink() or not directory.is_dir():
        raise ValueError('LOCAL_AUDIT_BACKUP_DIRECTORY_INVALID')
    if {p.name for p in directory.iterdir()}!=FILES:
        raise ValueError('LOCAL_AUDIT_BACKUP_INCOMPLETE_OR_EXTRA_FILES')
    if any((directory/name).is_symlink() for name in FILES):
        raise ValueError('LOCAL_AUDIT_BACKUP_SYMLINK')
    raw=_read(directory/'manifest.json',128*1024)
    manifest_sha=hashlib.sha256(raw).hexdigest()
    if expected_manifest_sha256 is not None:
        if (not isinstance(expected_manifest_sha256,str) or not re.fullmatch('[0-9a-f]{64}',expected_manifest_sha256)
                or manifest_sha!=expected_manifest_sha256):
            raise ValueError('LOCAL_AUDIT_TRUSTED_MANIFEST_MISMATCH')
    manifest=json.loads(raw);commit=json.loads(_read(directory/'COMPLETE',1024))
    expected={'schema_version','backup_id','created_at','scope','database','audit','snapshot',*FLAGS}
    if (not isinstance(manifest,dict) or set(manifest)!=expected or manifest.get('schema_version')!=1
            or manifest.get('scope')!='local_audit_database_snapshot'
            or any(manifest.get(k) is not v for k,v in FLAGS.items())
            or commit!={'schema_version':1,'manifest_sha256':manifest_sha}):
        raise ValueError('LOCAL_AUDIT_MANIFEST_INVALID')
    try:
        if str(uuid.UUID(manifest['backup_id']))!=manifest['backup_id']:
            raise ValueError('LOCAL_AUDIT_MANIFEST_INVALID')
        timestamp(manifest['created_at'])
    except (ValueError,TypeError,AttributeError):
        raise ValueError('LOCAL_AUDIT_MANIFEST_INVALID') from None
    database=manifest['database']
    if (not isinstance(database,dict) or set(database)!={'path','bytes','sha256'}
            or database['path']!='audit.sqlite' or type(database['bytes']) is not int or database['bytes']<=0
            or not isinstance(database['sha256'],str) or not re.fullmatch('[0-9a-f]{64}',database['sha256'])):
        raise ValueError('LOCAL_AUDIT_DATABASE_METADATA_INVALID')
    return directory,manifest,manifest_sha


def create_local_backup(db_path,output_dir,*,timeout_seconds=30):
    source=Path(db_path).absolute();output=Path(output_dir).absolute()
    if source.is_symlink() or not source.is_file():raise ValueError('LOCAL_AUDIT_SOURCE_INVALID')
    if output.exists() or output.is_symlink():raise FileExistsError(output)
    output.parent.mkdir(parents=True,exist_ok=True)
    stage=Path(tempfile.mkdtemp(prefix='.local-audit-',dir=output.parent));stage.chmod(0o700)
    connection=None
    try:
        connection,snapshot,metadata=_private_snapshot(source,stage,timeout_seconds)
        # The SQLite backup may retain a WAL-mode header. Normalize only this
        # private target into a standalone DELETE-journal database so the
        # package never depends on unarchived WAL/SHM files.
        connection.execute('PRAGMA query_only=OFF')
        if connection.execute('PRAGMA journal_mode=DELETE').fetchone()[0]!='delete':
            raise ValueError('LOCAL_AUDIT_STANDALONE_SNAPSHOT_REQUIRED')
        metadata['standalone_journal_mode']='delete'
        connection.close();connection=None
        snapshot.rename(stage/'audit.sqlite')
        path=stage/'audit.sqlite';os.chmod(path,0o600)
        fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
        try:os.fsync(fd)
        finally:os.close(fd)
        audit=_check_db(path,timeout_seconds=timeout_seconds)
        inventory=_hash(path,deadline=time.monotonic()+timeout_seconds)
        manifest={'schema_version':1,'backup_id':str(uuid.uuid4()),
            'created_at':datetime.now(timezone.utc).isoformat(),'scope':'local_audit_database_snapshot',
            'database':{'path':'audit.sqlite',**inventory},'audit':audit,'snapshot':metadata,**FLAGS}
        raw=(canonical(manifest)+'\n').encode();manifest_sha=hashlib.sha256(raw).hexdigest()
        _write(stage/'manifest.json',raw)
        _write(stage/'COMPLETE',(canonical({'schema_version':1,'manifest_sha256':manifest_sha})+'\n').encode())
        _sync_directory(stage)
        verify_local_backup(stage,expected_manifest_sha256=manifest_sha)
        # Never replace a directory, even an existing empty backup directory.
        # Publish COMPLETE only after the data/manifest and their links persist.
        output.mkdir(mode=0o700,exist_ok=False)
        for name in ('audit.sqlite','manifest.json'):os.link(stage/name,output/name,follow_symlinks=False)
        _sync_directory(output)
        os.link(stage/'COMPLETE',output/'COMPLETE',follow_symlinks=False)
        _sync_directory(output);_sync_directory(output.parent)
        return {'status':'LOCAL_AUDIT_BACKUP_CREATED','directory':str(output),
            'manifest_sha256':manifest_sha,'database_sha256':inventory['sha256'],'audit':audit,
            'commit_marker':'COMPLETE','manifest_requires_trusted_provenance':True,**FLAGS}
    finally:
        if connection is not None:connection.close()
        shutil.rmtree(stage)


def verify_local_backup(directory,*,expected_manifest_sha256=None):
    directory,manifest,manifest_sha=_metadata(directory,expected_manifest_sha256)
    inventory=_hash(directory/'audit.sqlite')
    if inventory!={k:manifest['database'][k] for k in ('bytes','sha256')}:
        raise ValueError('LOCAL_AUDIT_DATABASE_HASH_MISMATCH')
    audit=_check_db(directory/'audit.sqlite')
    if audit!=manifest['audit'] or _hash(directory/'audit.sqlite')!=inventory:
        raise ValueError('LOCAL_AUDIT_BACKUP_CONTENT_CHANGED')
    return {'status':'LOCAL_AUDIT_BACKUP_VERIFIED','directory':str(directory),
        'manifest_sha256':manifest_sha,'database_sha256':inventory['sha256'],'audit':audit,
        'trusted_manifest_hash_supplied':expected_manifest_sha256 is not None,
        'manifest_requires_trusted_provenance':True,**FLAGS}


def restore_local_backup(directory,destination_db,*,expected_manifest_sha256=None):
    verified=verify_local_backup(directory,expected_manifest_sha256=expected_manifest_sha256)
    directory,manifest,manifest_sha=_metadata(directory,verified['manifest_sha256'])
    destination=Path(destination_db).absolute()
    if any(p.exists() or p.is_symlink() for p in [destination,*[Path(str(destination)+s) for s in ('-wal','-shm','-journal','.lock')]]):
        raise FileExistsError(destination)
    destination.parent.mkdir(parents=True,exist_ok=True)
    fd,temporary=tempfile.mkstemp(prefix='.local-audit-restore-',suffix='.sqlite',dir=destination.parent)
    temporary=Path(temporary)
    try:
        with os.fdopen(fd,'wb') as target:
            source_fd=os.open(directory/'audit.sqlite',os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
            with os.fdopen(source_fd,'rb') as source:
                if not stat.S_ISREG(os.fstat(source.fileno()).st_mode):
                    raise ValueError('LOCAL_AUDIT_BACKUP_FILE_INVALID')
                remaining=manifest['database']['bytes']
                while remaining:
                    block=source.read(min(1024*1024,remaining))
                    if not block:raise ValueError('LOCAL_AUDIT_RESTORE_COPY_MISMATCH')
                    target.write(block);remaining-=len(block)
                if source.read(1):raise ValueError('LOCAL_AUDIT_RESTORE_COPY_MISMATCH')
            target.flush();os.fsync(target.fileno())
        if (_hash(temporary)!={k:manifest['database'][k] for k in ('bytes','sha256')}
                or _check_db(temporary)!=manifest['audit']):
            raise ValueError('LOCAL_AUDIT_RESTORE_COPY_MISMATCH')
        # Recheck sidecars before publication; link is the exclusive commit for DB.
        if any(Path(str(destination)+s).exists() or Path(str(destination)+s).is_symlink()
                for s in ('-wal','-shm','-journal','.lock')):
            raise FileExistsError(destination)
        os.link(temporary,destination,follow_symlinks=False);_sync_directory(destination.parent)
        return {'status':'LOCAL_AUDIT_BACKUP_RESTORED','database':str(destination),
            'manifest_sha256':manifest_sha,'database_sha256':manifest['database']['sha256'],
            'audit':manifest['audit'],'existing_runtime_modified':False,**FLAGS}
    finally:temporary.unlink(missing_ok=True)
