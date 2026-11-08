"""Explicit synthetic-source and workspace contract for simulation sessions.

A simulation run must not inherit the production broker, market, review or host
wake surfaces. The caller supplies a :class:`SimulationSources` whose four
surfaces each carry ``simulation_only = True``; validation happens before the
session creates any lock, database, journal, packet or status file.

Storage is a new, empty workspace outside the repository ``runtime`` tree. The
workspace path supplied by the caller is checked for a final-component symlink
before any resolution, so a link may not redirect the workspace elsewhere. All
session paths are resolved and checked to be inside it before anything is
written, so a caller cannot accidentally reuse an account book or live output
directory.

``simulation_only`` is a caller declaration, not a sandbox: a deliberately
malicious marked object could still wrap external effects. The contract only
guarantees the default composition never selects the production adapters.
"""
from dataclasses import dataclass
from pathlib import Path

from .qualification import candidate_root

SIMULATION_SURFACES = ('broker', 'market', 'review', 'wake')
_REPO_ROOT = candidate_root()
_CANONICAL_RUNTIME = _REPO_ROOT / 'runtime'
_PROTECTED_NAMES = frozenset({
    'live-verification-control.json',
})
_PROTECTED_PREFIXES = ('live-', 'account-risk-')


@dataclass(frozen=True)
class SimulationSources:
    """The only data/review/wake surfaces a simulation session may use.

    Each value must itself be marked ``simulation_only = True``. The marker is
    checked on the object supplied, not inferred from its module or name.
    """

    broker: object
    market: object
    review: object
    wake: object
    simulation_only: bool = True


@dataclass(frozen=True)
class SimulationWorkspace:
    root: Path
    db_path: Path
    output_dir: Path
    status_file: Path | None
    entry_inhibit_file: Path
    watchdog_policy_path: Path | None


def _marked(surface):
    return getattr(surface, 'simulation_only', None) is True


def _surface_error(name, code):
    return ValueError('SIMULATION_SOURCE_' + code + ':' + name)


def validate_simulation_sources(sources):
    """Return ``sources`` only when every surface is explicitly synthetic."""
    if sources is None:
        raise ValueError('SIMULATION_SOURCES_REQUIRED')
    if not isinstance(sources, SimulationSources):
        raise ValueError('SIMULATION_SOURCES_TYPE_INVALID')
    if not _marked(sources):
        raise ValueError('SIMULATION_SOURCES_NOT_MARKED')
    for name in SIMULATION_SURFACES:
        surface = getattr(sources, name, None)
        if surface is None:
            raise _surface_error(name, 'MISSING')
        if not _marked(surface):
            raise _surface_error(name, 'NOT_MARKED')
    broker = sources.broker
    if not (callable(broker) or callable(getattr(broker, 'snapshot', None))):
        raise _surface_error('broker', 'TYPE_MISMATCH')
    market = sources.market
    if not (callable(market) or callable(getattr(market, 'collect_events', None))):
        raise _surface_error('market', 'TYPE_MISMATCH')
    review = sources.review
    if not callable(review) or not callable(getattr(review, 'execute_review', None)):
        raise _surface_error('review', 'TYPE_MISMATCH')
    wake = sources.wake
    if not callable(getattr(wake, 'enqueue', None)):
        raise _surface_error('wake', 'TYPE_MISMATCH')
    return sources


def _resolved(path):
    return Path(path).expanduser().resolve()


def _inside(path, root):
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _safe_name(path):
    return path.name.lower()


def _check_workspace_root(workspace, db_path=None):
    # Inspect the caller's final workspace component before resolve() follows a
    # link. Only the supplied component matters: macOS system ancestors such as
    # /var -> /private/var remain valid, because the workspace itself is not a
    # symlink. A broken symlink is rejected here too (`is_symlink()` is lstat).
    supplied = Path(workspace).expanduser()
    if supplied.is_symlink():
        raise ValueError('SIMULATION_WORKSPACE_SYMLINK')
    root = supplied.resolve()
    if root == _REPO_ROOT or root == _CANONICAL_RUNTIME:
        raise ValueError('SIMULATION_WORKSPACE_OVERLAPS_LIVE_RUNTIME')
    if _inside(root, _CANONICAL_RUNTIME):
        raise ValueError('SIMULATION_WORKSPACE_OVERLAPS_LIVE_RUNTIME')
    if _inside(_REPO_ROOT, root):
        # The repository itself cannot be an empty synthetic workspace.
        raise ValueError('SIMULATION_WORKSPACE_CONTAINS_REPOSITORY')
    if root.exists():
        if not root.is_dir():
            raise ValueError('SIMULATION_WORKSPACE_NOT_A_DIRECTORY')
        # A controller owns the per-database process lock before it can hand
        # this synthetic workspace to the child. Empty lock files are control
        # identity, not session state; every other entry still makes the
        # workspace non-empty. This does not permit reuse of a prior book or
        # output directory.
        db_name = Path(db_path).name if db_path is not None else ''
        controller_names = {
            db_name,
            db_name + '.lock', db_name + '.control.lock',
            db_name + '.watchdog.lock', db_name + '.worker.lock',
            db_name + '.entry-inhibit.json', db_name + '.watchdog.json',
        } if db_name else set()
        for entry in root.iterdir():
            if not entry.is_file() or entry.is_symlink():
                raise ValueError('SIMULATION_WORKSPACE_NOT_EMPTY')
            if entry.name == db_name:
                # The controller may have created the empty SQLite file before
                # handing the workspace to the child. A nonempty prior book is
                # still state and is refused.
                if entry.stat().st_size != 0:
                    raise ValueError('SIMULATION_WORKSPACE_NOT_EMPTY')
                continue
            if entry.name in controller_names:
                continue
            if not entry.name.endswith('.lock'):
                raise ValueError('SIMULATION_WORKSPACE_NOT_EMPTY')
    return root


def _confined(path, root, *, name, allow_none=False):
    if path is None:
        if allow_none:
            return None
        raise ValueError('SIMULATION_PATH_REQUIRED:' + name)
    resolved = _resolved(path)
    if not _inside(resolved, root):
        raise ValueError('SIMULATION_PATH_OUTSIDE_WORKSPACE:' + name)
    if (_safe_name(resolved) in _PROTECTED_NAMES
            or _safe_name(resolved).startswith(_PROTECTED_PREFIXES)):
        raise ValueError('SIMULATION_PATH_PROTECTED:' + name)
    return resolved


def validate_simulation_workspace(workspace, *, db_path, output_dir,
                                  status_file=None, entry_inhibit_file=None,
                                  watchdog_policy_path=None):
    """Resolve and confine every session path, before any filesystem write.

    The workspace must be a new empty directory (or a non-existent path that
    this function will not create). Caller-supplied paths must already live
    inside it; nothing falls back to the production ``runtime/live-agent``.
    """
    if workspace is None:
        raise ValueError('SIMULATION_WORKSPACE_REQUIRED')
    root = _check_workspace_root(workspace, db_path)
    db = _confined(db_path, root, name='db_path')
    output = _confined(output_dir, root, name='output_dir')
    status = _confined(status_file, root, name='status_file', allow_none=True)
    inhibit = _confined(entry_inhibit_file, root,
                        name='entry_inhibit_file', allow_none=True)
    watchdog = _confined(watchdog_policy_path, root,
                         name='watchdog_policy_path', allow_none=True)
    if inhibit is None:
        inhibit = db.with_name(db.name + '.entry-inhibit.json')
    if not _inside(inhibit, root):
        raise ValueError('SIMULATION_PATH_OUTSIDE_WORKSPACE:entry_inhibit_file')
    return SimulationWorkspace(root=root, db_path=db, output_dir=output,
                               status_file=status, entry_inhibit_file=inhibit,
                               watchdog_policy_path=watchdog)


def create_simulation_workspace(paths):
    """Create the validated workspace and its parent directories.

    Called only after :func:`validate_simulation_workspace`; existing non-empty
    directories were already refused, so this cannot overwrite prior evidence.
    """
    paths.root.mkdir(parents=True, exist_ok=True)
    paths.root.chmod(0o700)
    paths.db_path.parent.mkdir(parents=True, exist_ok=True)
    paths.output_dir.mkdir(parents=True, exist_ok=True)
    if paths.status_file is not None:
        paths.status_file.parent.mkdir(parents=True, exist_ok=True)
    paths.entry_inhibit_file.parent.mkdir(parents=True, exist_ok=True)
    return paths


def broker_surface(sources):
    """Return a callable ``(approval_ids=()) -> snapshot`` for the port."""
    broker = sources.broker
    if callable(broker) and not hasattr(broker, 'snapshot'):
        return broker
    snapshot = getattr(broker, 'snapshot', None)
    if not callable(snapshot):
        raise _surface_error('broker', 'TYPE_MISMATCH')
    return lambda approval_ids=(): snapshot(approval_ids=list(approval_ids))


def market_surface(sources):
    """Return a callable ``(events..., **kwargs) -> collection`` for the port."""
    market = sources.market
    if callable(market) and not hasattr(market, 'collect_events'):
        return market
    collect = getattr(market, 'collect_events', None)
    if not callable(collect):
        raise _surface_error('market', 'TYPE_MISMATCH')
    return lambda *args, **kwargs: collect(*args, **kwargs)
