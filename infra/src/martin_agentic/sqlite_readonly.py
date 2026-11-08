"""Safe read-only SQLite URI selection without creating sidecars.

A plain ``mode=ro`` connection to a WAL database can create or update
``-shm``.  For a WAL-mode database with no unresolved sidecar content the main
file is the checkpointed view, so ``immutable=1`` reads it without touching
SQLite sidecars.  Any non-empty WAL/shm/journal sidecar is refused: ignoring it
would either hide committed facts or require a write/checkpoint, both outside
this module's authority.
"""
import os
from pathlib import Path
import stat


MAGIC = b'SQLite format 3\x00'


class SQLiteReadOnlyError(ValueError):
    def __init__(self, code, detail=None):
        self.code = code
        self.detail = detail
        super().__init__(code)

    def __str__(self):
        return self.code if self.detail is None else f'{self.code}:{self.detail}'


def _fail(code, detail=None):
    raise SQLiteReadOnlyError(code, detail)


def _header(path):
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError:
        _fail('SQLITE_FILE_INVALID', str(path))
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o022:
            _fail('SQLITE_FILE_INVALID', str(path))
        chunks = []
        size = 0
        while size < 100:
            block = os.read(fd, 100 - size)
            if not block:
                break
            chunks.append(block)
            size += len(block)
        body = b''.join(chunks)
    except OSError:
        _fail('SQLITE_FILE_INVALID', str(path))
    finally:
        os.close(fd)
    if len(body) < 100 or body[:16] != MAGIC:
        return None
    return body


def _sidecars(path):
    for suffix in ('-wal', '-shm', '-journal'):
        sidecar = Path(str(path) + suffix)
        if sidecar.is_symlink():
            _fail('SQLITE_SIDECAR_INVALID', suffix)
        if not sidecar.exists():
            continue
        try:
            info = sidecar.stat()
        except OSError:
            _fail('SQLITE_SIDECAR_INVALID', suffix)
        if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o022:
            _fail('SQLITE_SIDECAR_INVALID', suffix)
        if info.st_size:
            _fail('SQLITE_SIDECAR_NOT_CONVERGED', suffix)


def readonly_uri(path):
    """Return a URI that cannot create/update a sidecar for this file.

    Non-WAL databases use ordinary read-only mode.  WAL-mode databases use
    immutable mode only after proving every WAL/shm/journal sidecar is absent
    or empty; a non-empty WAL is refused rather than ignored.
    """
    path = Path(path)
    if path.is_symlink() or not path.is_file():
        _fail('SQLITE_FILE_INVALID', str(path))
    _sidecars(path)
    header = _header(path)
    wal_mode = header is not None and (header[18] == 2 or header[19] == 2)
    uri = path.absolute().as_uri()
    return uri + ('?mode=ro&immutable=1' if wal_mode else '?mode=ro')
