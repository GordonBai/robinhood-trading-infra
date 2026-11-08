"""Evidence-based release diagnostics. This report never authorizes orders."""
from datetime import datetime,timezone
from decimal import Decimal
import hashlib
import importlib.metadata
import json
import re
import stat
import zipfile
import os
from pathlib import Path, PurePosixPath
import time
from urllib.parse import unquote, urlparse

from .audit import AuditTrail, digest
from .broker import TERMINAL, valid_session_order
from .calendar import is_regular
from .ledger import timestamp
from .risk import number


_MODULE = 'martin_agentic/qualification.py'


def _installed_distribution():
    """Return the distribution whose RECORD owns this executing module.

    ``importlib.metadata.distribution(name)`` searches sys.path in order. A
    test that puts an extracted candidate on sys.path can expose a build-time
    ``.egg-info`` for the same project before the real installed ``.dist-info``.
    Resolve from the executing file's actual site-packages instead, and require
    exactly one distribution whose RECORD owns that file.
    """
    module = Path(__file__).resolve()
    if module.parent.name != 'martin_agentic':
        raise ValueError('INSTALLED_DISTRIBUTION_IDENTITY_UNAVAILABLE')
    site_packages = module.parents[1]
    try:
        distributions = list(importlib.metadata.distributions(
            path=[str(site_packages)]))
    except (OSError, TypeError, ValueError):
        raise ValueError('INSTALLED_DISTRIBUTION_IDENTITY_UNAVAILABLE') from None
    matches = []
    for distribution in distributions:
        name = (distribution.metadata.get('Name') or '').lower().replace('_', '-')
        if name != 'martin-agentic':
            continue
        try:
            owned = any(str(item).replace('\\', '/') == _MODULE
                        for item in (distribution.files or ()))
            located = Path(distribution.locate_file(_MODULE)).resolve()
        except (OSError, TypeError, ValueError):
            continue
        if owned and located == module:
            matches.append(distribution)
    if not matches:
        raise ValueError('INSTALLED_DISTRIBUTION_IDENTITY_UNAVAILABLE')
    if len(matches) > 1:
        raise ValueError('INSTALLED_DISTRIBUTION_IDENTITY_AMBIGUOUS')
    return matches[0]


def _candidate_root():
    """Resolve and bind the running installed wheel to a verified release.

    Source-tree runs use the repository root. A wheel-installed run must have
    an existing PEP 610 wheel, installed site-packages bytes equal to that
    wheel, and a sibling release manifest whose qualified source inventory,
    source fingerprint, input-set digest, archive and file hashes all match the
    extracted candidate. Missing/empty/mismatched evidence is refused; cwd,
    PYTHONPATH and unverified environment paths are never authorities.
    """
    package_root = Path(__file__).resolve().parents[2]
    repo_root = package_root.parent
    if ((repo_root / 'infra').is_dir() and (repo_root / 'strategy').is_dir()
            and (repo_root / 'pyproject.toml').is_file()):
        return repo_root
    try:
        distribution = _installed_distribution()
        raw = distribution.read_text('direct_url.json')
        data = json.loads(raw) if raw else {}
        url = data.get('url') if isinstance(data, dict) else None
        if not isinstance(url, str):
            raise ValueError
        wheel = Path(unquote(urlparse(url).path)).resolve()
        if (not wheel.is_file() or wheel.is_symlink()
                or wheel.suffix != '.whl'):
            raise ValueError
        candidate = wheel.parent.parent
        if (wheel.parent.name != 'dist' or not (candidate / 'infra').is_dir()
                or not (candidate / 'strategy').is_dir()
                or not (candidate / 'pyproject.toml').is_file()):
            raise ValueError
        # The direct_url archive hash is optional in PEP 610, but if present it
        # must bind the exact wheel bytes.
        archive = data.get('archive_info') if isinstance(data, dict) else None
        if isinstance(archive, dict):
            declared = archive.get('hash')
            if isinstance(declared, str) and declared.startswith('sha256='):
                if hashlib.sha256(wheel.read_bytes()).hexdigest() != declared[7:]:
                    raise ValueError
            elif isinstance(declared, dict) and isinstance(declared.get('sha256'), str):
                if hashlib.sha256(wheel.read_bytes()).hexdigest() != declared['sha256']:
                    raise ValueError
        expected = _source_fingerprint_at(candidate)
        if not source_files(candidate):
            raise ValueError
        # Verify the installed package bytes against the wheel itself, and bind
        # every wheel package member to the candidate source file it was built
        # from. Installed↔wheel and candidate↔manifest are separate chains; a
        # stale wheel copied beside a freshly rebuilt source release must fail
        # here. The mapping is the existing package layout, not a new inventory.
        packages = {'martin_agentic', 'martin_luk', 'trading_contracts',
                    'trading_infra', 'trading_strategies',
                    'trading_example_plugins', 'trading_example_app'}
        source_roots = (candidate / 'infra/src', candidate / 'strategy/martin_luk/src',
                        candidate / 'strategy/examples/src')
        package_sources = set()
        for source_root in source_roots:
            package_sources.update(
                path.relative_to(source_root).as_posix()
                for path in source_files(candidate)
                if path.is_relative_to(source_root)
                and path.relative_to(source_root).parts[0] in packages)
        source_root = candidate
        site_packages = Path(__file__).resolve().parents[1]
        with zipfile.ZipFile(wheel) as zipped:
            members = [name for name in zipped.namelist()
                       if name.split('/', 1)[0] in packages
                       and not name.endswith('/')]
            if not members or any(
                    part in {'', '.', '..'} for name in members
                    for part in PurePosixPath(name).parts):
                raise ValueError
            if set(members) != package_sources:
                raise ValueError
            for name in members:
                member_digest = hashlib.sha256(zipped.read(name)).hexdigest()
                source = next((root / name for root in source_roots
                               if (root / name).is_file()), None)
                if source is None or source.is_symlink():
                    raise ValueError
                if hashlib.sha256(source.read_bytes()).hexdigest() != member_digest:
                    raise ValueError
                installed = site_packages / name
                if not installed.is_file() or installed.is_symlink():
                    raise ValueError
                if hashlib.sha256(installed.read_bytes()).hexdigest() != \
                        member_digest:
                    raise ValueError
        # Verify the sibling release manifest and archive, including every
        # qualified source file. A manifest that merely repeats a hash is not
        # enough.
        for manifest_path in sorted(candidate.parent.glob('*/manifest.json')):
            try:
                manifest = json.loads(manifest_path.read_text())
            except (OSError, ValueError):
                continue
            if not isinstance(manifest, dict):
                continue
            rows = manifest.get('files')
            if (manifest.get('scope') !=
                    'local_source_and_test_inputs_not_deployment_authority'
                    or manifest.get('source_fingerprint') != expected
                    or not isinstance(rows, list) or not rows
                    or manifest.get('file_count') != len(rows)
                    or manifest.get('input_set_sha256') != digest(rows)
                    or manifest.get('archive') != 'source-and-tests.tar.gz'):
                continue
            archive_path = manifest_path.parent / manifest['archive']
            if (not archive_path.is_file() or archive_path.is_symlink()
                    or hashlib.sha256(archive_path.read_bytes()).hexdigest()
                    != manifest.get('archive_sha256')):
                continue
            qualified = {row.get('path') for row in rows
                         if isinstance(row, dict) and row.get('qualified_source')}
            actual = {path.relative_to(candidate).as_posix()
                      for path in source_files(candidate)}
            if not qualified or qualified != actual:
                continue
            for row in rows:
                if not isinstance(row, dict) or not isinstance(row.get('path'), str):
                    raise ValueError
                path = candidate / row['path']
                if (not path.is_file() or path.is_symlink()
                        or path.stat().st_size != row.get('bytes')
                        or hashlib.sha256(path.read_bytes()).hexdigest()
                        != row.get('sha256')):
                    raise ValueError
                if row.get('qualified_source') and stat.S_IMODE(
                        path.stat().st_mode) not in {0o644, 0o755}:
                    raise ValueError
            return candidate
        raise ValueError
    except (ImportError, KeyError, OSError, ValueError, TypeError,
            zipfile.BadZipFile) as exc:
        if str(exc).startswith('INSTALLED_DISTRIBUTION_'):
            raise
        raise ValueError(
            'INSTALLED_CANDIDATE_SOURCE_IDENTITY_UNAVAILABLE') from None


candidate_root = _candidate_root


def _source_fingerprint_at(root):
    h=hashlib.sha256(b'martin-source-v2\0')
    for path in source_files(root):
        h.update(path.relative_to(root).as_posix().encode());h.update(b'\0');h.update(path.read_bytes());h.update(b'\0')
    return h.hexdigest()


def source_files(root=None):
    root=Path(root) if root is not None else candidate_root()
    patterns=('infra/src/martin_agentic/**/*.py','infra/src/martin_agentic/web/*',
        'infra/src/trading_contracts/**/*.py','infra/src/trading_infra/**/*.py',
        'strategy/martin_luk/src/**/*.py','strategy/examples/src/**/*.py',
        'examples/plugins/**/*.py','examples/plugins/**/*.toml','examples/plugins/**/*.json',
        'examples/plugins/**/*.md','examples/readonly/**/*.json','examples/readonly/**/*.md','config/project.json','config/*.example.json','config/synthetic_policy.json',
        'tests/**/*.py','scripts/c3_qualification.py','scripts/c4_qualification.py',
        'scripts/rehearse_local_process_faults.py','scripts/rehearse_live_session_blocking.py',
        'docs/**/*.md','docs/plugins/schema/*.json','strategy-spec/*.md',
        'pyproject.toml','uv.lock','README.md','LICENSE')
    paths={p for pattern in patterns for p in root.glob(pattern) if p.is_file() and '__pycache__' not in p.parts}
    return sorted(paths, key=lambda p: p.relative_to(root).as_posix())


def source_fingerprint(root=None):
    root=Path(root) if root is not None else candidate_root()
    return _source_fingerprint_at(root)


SOURCE_PIN_MAX_FILES = 5000
SOURCE_PIN_MAX_NODES = 50000
SOURCE_PIN_MAX_SECONDS = 1.0

# Explicit finite traversal rules mirroring source_files().  The pin never
# asks Path.glob to build an unlimited tree first.
_SOURCE_PIN_DIR_RULES = (
    ('infra/src/martin_agentic', True, '.py'),
    ('infra/src/martin_agentic/web', False, None),
    ('infra/src/trading_contracts', True, '.py'),
    ('infra/src/trading_infra', True, '.py'),
    ('strategy/martin_luk/src', True, '.py'),
    ('strategy/examples/src', True, '.py'),
    ('examples/plugins', True, ('.py', '.toml', '.json', '.md')),
    ('examples/readonly', True, ('.json', '.md')),
    ('tests', True, '.py'),
    ('docs', True, '.md'),
    ('strategy-spec', False, '.md'),
)
_SOURCE_PIN_EXACT = ('pyproject.toml','uv.lock','README.md','LICENSE','config/project.json',
    'config/current_capture.example.json','config/synthetic_policy.json',
    'scripts/c3_qualification.py','scripts/c4_qualification.py',
    'scripts/rehearse_local_process_faults.py','scripts/rehearse_live_session_blocking.py',
    'docs/plugins/schema/descriptor.schema.json',
    'docs/plugins/schema/manifest.schema.json')


class _SourcePinLimit(Exception):
    def __init__(self, reason):
        self.reason = reason


def _source_pin_scan(root):
    files = {}
    nodes = 0
    started = time.monotonic()
    def tick():
        nonlocal nodes
        nodes += 1
        if nodes > SOURCE_PIN_MAX_NODES:
            raise _SourcePinLimit('SOURCE_PIN_NODE_LIMIT')
        if time.monotonic() - started > SOURCE_PIN_MAX_SECONDS:
            raise _SourcePinLimit('SOURCE_PIN_TIME_LIMIT')
    def add(path):
        relative = path.relative_to(root).as_posix()
        if relative in files:
            return
        try:
            info = os.stat(path, follow_symlinks=False)
        except OSError:
            raise _SourcePinLimit('SOURCE_PIN_FILE_UNAVAILABLE')
        if not os.path.isfile(path) or os.path.islink(path):
            raise _SourcePinLimit('SOURCE_PIN_NON_REGULAR_FILE')
        files[relative] = [relative, info.st_size, info.st_mtime_ns]
        if len(files) > SOURCE_PIN_MAX_FILES:
            raise _SourcePinLimit('SOURCE_PIN_FILE_LIMIT')
    for relative, recursive, suffix in _SOURCE_PIN_DIR_RULES:
        suffixes = ((suffix,) if isinstance(suffix, str)
                    else tuple(suffix) if suffix is not None else ())
        base = root / relative
        if base.is_symlink():
            raise _SourcePinLimit('SOURCE_PIN_SYMLINK')
        if not base.is_dir():
            continue
        stack = [base]
        while stack:
            current = stack.pop()
            try:
                iterator = os.scandir(current)
            except OSError:
                raise _SourcePinLimit('SOURCE_PIN_DIRECTORY_UNAVAILABLE')
            with iterator:
                for entry in iterator:
                    tick()
                    try:
                        if entry.is_symlink():
                            raise _SourcePinLimit('SOURCE_PIN_SYMLINK')
                        if entry.is_dir(follow_symlinks=False):
                            if recursive and entry.name != '__pycache__':
                                stack.append(Path(entry.path))
                            continue
                        if not entry.is_file(follow_symlinks=False):
                            continue
                        if suffix is not None and not entry.name.endswith(suffixes):
                            continue
                        add(Path(entry.path))
                    except OSError:
                        raise _SourcePinLimit('SOURCE_PIN_FILE_UNAVAILABLE')
    for relative in _SOURCE_PIN_EXACT:
        path = root / relative
        if path.is_symlink():
            raise _SourcePinLimit('SOURCE_PIN_SYMLINK')
        if path.exists():
            add(path)
    return files


def source_pin(root=None):
    """Bounded complete stat-only source version for short-lock rechecks.

    Enumeration is finite: explicit rules, a node/time budget, and at most
    MAX_FILES+1 files. Exceeding any bound returns a non-bounded status, so a
    receipt must fail closed instead of treating a truncated prefix as safe.
    Normal small source sets cover the same files as source_fingerprint().
    This never reads source bytes and cannot defeat an attacker who rewrites
    content while preserving size/mtime; that trust boundary is explicit.
    """
    root=Path(root) if root is not None else candidate_root()
    root=root.resolve()
    try:
        files=_source_pin_scan(root)
    except _SourcePinLimit as exc:
        status=('limit_exceeded' if 'LIMIT' in exc.reason
                else 'unavailable')
        return {'schema_version':1,'status':status,
                'reason':exc.reason,'root':str(root),'truncated':True,
                'limit_files':SOURCE_PIN_MAX_FILES,
                'limit_nodes':SOURCE_PIN_MAX_NODES,
                'limit_seconds':SOURCE_PIN_MAX_SECONDS}
    entries=sorted(files.values())
    return {'schema_version':1,'status':'bounded','root':str(root),
            'count':len(entries),'truncated':False,
            'digest':hashlib.sha256(json.dumps(entries,separators=(',',':')).encode()).hexdigest()}


def protection_references(book):
    return {ref:json.loads(a['payload']) for a in book.actions() if a['kind']=='protect' for ref in book._order_refs(a)}


def _execution_fills(order,observed_at):
    """Only normalized, positive broker executions establish a fill."""
    fills=order['executions'];seen=set();total=Decimal(0)
    for fill in fills:
        if (not fill['id'] or fill['id'] in seen or fill['order_id']!=order['id']
                or fill['symbol']!=order['symbol'] or fill['side']!=order['side']
                or number(fill['quantity'])<=0 or number(fill['price'])<=0
                or number(fill['fees'])<0 or timestamp(fill['executed_at'])>observed_at
                or timestamp(fill['executed_at'])<timestamp(order['created_at'])):
            raise ValueError('INVALID_RELEASE_EXECUTION')
        seen.add(fill['id']);total+=number(fill['quantity'])
    if total!=number(order['filled']) or total>number(order['quantity']):
        raise ValueError('INVALID_RELEASE_EXECUTION_TOTAL')
    return fills


def _manual_takeover(book,trade_id,trade):
    """Positive, permanent intervention markers; ordinary confirmation is not one."""
    sources=[]
    if trade.get('manual_takeover') is True:sources.append('trade.manual_takeover')
    for (raw,) in book.db.execute('SELECT payload FROM audit_events ORDER BY sequence'):
        event=json.loads(raw)
        if (event.get('trade_id')==trade_id and event.get('synthetic') is False
                and event.get('account')=='••••'+book.policy['account_suffix']
                and event.get('details',{}).get('manual_takeover') is True):
            sources.append('audit:'+event['event_id'])
    if book.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='recovery_cases'").fetchone():
        for case_id,raw in book.db.execute('SELECT id,payload FROM recovery_cases WHERE trade_id=?',(trade_id,)):
            if json.loads(raw).get('manual_takeover') is True:sources.append('recovery_case:'+case_id)
    return sources


def _takeover_boundary(book,state,now):
    """Latest incident/recovery marker across the account, or explicit unknown.

    A later success must start with new actual executions after this boundary.
    A missing incident time cannot be filled in from an observation or guessed
    from another trade. Historical failures remain permanently visible.
    """
    times=[];unknown=False;present=False
    def collect(value):
        nonlocal unknown
        try:
            at=timestamp(value)
            if at>now:unknown=True
            else:times.append(at)
        except (ValueError,TypeError,AttributeError):unknown=True
    for trade in state['trades'].values():
        if trade.get('manual_takeover') is True:
            present=True;collect(trade.get('manual_takeover_at'))
    for (raw,) in book.db.execute('SELECT payload FROM audit_events ORDER BY sequence'):
        event=json.loads(raw)
        if (event.get('synthetic') is False and event.get('account')=='••••'+book.policy['account_suffix']
                and event.get('details',{}).get('manual_takeover') is True):
            present=True;collect(event.get('occurred_at'))
    if book.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='recovery_cases'").fetchone():
        for (raw,) in book.db.execute('SELECT payload FROM recovery_cases'):
            case=json.loads(raw)
            if case.get('manual_takeover') is not True:continue
            present=True
            values=[case[k] for k in ('opened_at','verified_at','acknowledged_at') if case.get(k)]
            if not values:unknown=True
            for value in values:collect(value)
    return {'present':present,'known':present and not unknown and bool(times),
        'latest_at':max(times).isoformat() if times else None,
        'timing_incomplete':unknown}


def _action_timeline(book,actions,now):
    """Reconstruct only recorded milestones. A null is not a zero latency.

    Local intent and result times are diagnostics, never acceptance evidence.
    The broker has no exact acceptance timestamp contract: use the first complete
    confirmed observation separately as a conservative upper bound.
    """
    audit=[json.loads(r[0]) for r in book.db.execute('SELECT payload FROM audit_events ORDER BY sequence')]
    timelines=[]
    for action in actions:
        if action['kind']!='protect':continue
        refs=book._order_refs(action)
        intents=[e for e in audit if e.get('event_type')=='AGENT_ACTION_INTENT'
            and e.get('synthetic') is False and e.get('trade_id')==action['trade_id']
            and (e.get('intent_id')==action['id'] or e.get('ref_id') in refs)
            and timestamp(e['occurred_at'])<=now]
        def first(events):return min((e['occurred_at'] for e in events),key=timestamp,default=None)
        def intent_time(kind):return first([e for e in intents if e.get('details',{}).get('action')==kind])
        review_ids={e.get('intent_id') for e in intents if e.get('details',{}).get('action')=='review'}
        results=[e for e in audit if e.get('event_type')=='AGENT_ACTION_RESULT'
            and e.get('synthetic') is False and e.get('intent_id') in review_ids
            and timestamp(e['occurred_at'])<=now]
        # Review-result time starts a known confirmation wait; there is no
        # durable explicit user-response event yet, so its endpoint stays null.
        created=action.get('created_at')
        if created and timestamp(created)>now:created=None
        timelines.append({'action_id':action['id'],'protection_intent_at':created,
            'claim_at':first([e for e in intents if e.get('intent_id')==action['id']]),
            'review_started_at':intent_time('review'),'review_result_at':first(results),
            'confirmation_wait_started_at':first([e for e in results
                if e.get('details',{}).get('outcome')=='pending_approval']),
            'user_confirmation_at':None,'place_started_at':first([e for e in intents
                if e.get('intent_id')!=action['id']
                and e.get('details',{}).get('action')=='stop_loss'
                and e.get('details',{}).get('origin')=='supervised_native_tool_handoff']),
            'broker_accepted_at':None,'source':'local_action_and_hash_linked_audit',
            'local_events_prove_broker_acceptance':False})
    return timelines


def live_lifecycle_evidence(book,state,now):
    """Prove a single trade's ordered lifecycle from complete broker snapshots.

    Trade state and Agent reports alone never prove a fill or a protected round
    trip. Logical refs and explicitly recorded approval aliases are both matched
    against exact order terms. A full smoke chain must cover the final entry
    quantity before its actual sell executions and finish with terminal orders
    and a completely flat account, rather than combining unrelated trades.
    """
    if book.simulation or not state.get('account_key'):return []
    if not book.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='broker_observations'").fetchone():return []
    snapshots=[]
    for (encoded,) in book.db.execute('SELECT payload FROM broker_observations'):
        try:
            snap=json.loads(encoded);at=timestamp(snap['observed_at'])
            if (snap.get('synthetic') is False and snap.get('collection_complete') is True
                    and snap.get('provider')=='robinhood'
                    and snap.get('account_key')==state['account_key']
                    and snap.get('account_mask')=='••••'+book.policy['account_suffix']
                    and at<=now):snapshots.append((at,snap))
        except (ValueError,KeyError,TypeError):continue
    snapshots.sort(key=lambda item:item[0])
    results=[]
    for trade_id,trade in state['trades'].items():
        actions=[a for a in book.actions() if a['trade_id']==trade_id and a['kind'] in {'entry','protect','exit'}]
        evidence={'trade_id':trade_id,'entry_fill_validated':False,
            'protection_acceptance_validated':False,'exit_flat_validated':False,
            'complete_protected_round_trip':False,'protection_deadline_seconds':30,
            'protection_timing_basis':'first_formal_confirmed_observation_upper_bound',
            'protection_tranches':[]}
        protections=[];tranches={}
        manual_sources=_manual_takeover(book,trade_id,trade)
        evidence.update(manual_takeover_detected=bool(manual_sources),manual_takeover_sources=manual_sources,
            protection_action_timeline=_action_timeline(book,actions,now))
        for at,snap in snapshots:
            try:
                own=[];execution_ids=set()
                for order in snap['orders'].values():
                    matches=[a for a in actions if book._order_matches(a,order)]
                    if not matches:continue
                    if len(matches)!=1:raise ValueError('AMBIGUOUS_RELEASE_ORDER_OWNER')
                    action=matches[0]
                    book._check_order_terms(order,json.loads(action['payload']))
                    fills=_execution_fills(order,at)
                    if any(fill['id'] in execution_ids for fill in fills):raise ValueError('DUPLICATE_RELEASE_EXECUTION')
                    execution_ids.update(fill['id'] for fill in fills)
                    own.append((action,order,fills))
                if len({a['id'] for a,_,_ in own})!=len(own):raise ValueError('DUPLICATE_RELEASE_REFERENCE')
                entries=[(o,f) for a,o,f in own if a['kind']=='entry' and o['side']=='buy']
                exits=[(o,f) for a,o,f in own if a['kind'] in {'protect','exit'} and o['side']=='sell']
                entry_fills={f['id']:f for _,fills in entries for f in fills}
                sell_fills={f['id']:f for _,fills in exits for f in fills}
                bought=sum((number(f['quantity']) for f in entry_fills.values()),Decimal(0))
                sold=sum((number(f['quantity']) for f in sell_fills.values()),Decimal(0))
                qty=number(snap['positions'].get(trade['plan']['symbol'],{}).get('quantity','0'))
                if bought<=0 or qty!=bought-sold:continue
                evidence['entry_fill_validated']=True
                for fill_id,fill in entry_fills.items():
                    tranches.setdefault(fill_id,{'execution_id':fill_id,'quantity':fill['quantity'],
                        'executed_at':fill['executed_at'],'first_observed_at':snap['observed_at'],
                        'protection_confirmed_observed_at':None,'protection_order_id':None,'protection_action_id':None,
                        'unprotected_seconds_upper_bound':None,'within_30_seconds':False})
                evidence.setdefault('entry_observed_at',snap['observed_at'])
                # The recorded stop must cover the entire observed entry before
                # any exit fill; later added entry quantity needs new coverage.
                stops=[o for a,o,_ in own if a['kind']=='protect' and o['side']=='sell'
                    and o['type']=='stop_market' and o['state']=='confirmed'
                    and number(o['remaining'])==qty and valid_session_order(o,at)]
                if qty>0 and sold==0 and is_regular(snap['observed_at']) and stops:
                    evidence['protection_acceptance_validated']=True
                    protections.append((at,entry_fills.copy(),stops[0]['id']))
                    evidence.setdefault('protection_observed_at',snap['observed_at'])
                    for fill_id in entry_fills:
                        tranche=tranches[fill_id]
                        if tranche['protection_confirmed_observed_at'] is None:
                            delay=(at-timestamp(tranche['executed_at'])).total_seconds()
                            tranche.update(protection_confirmed_observed_at=snap['observed_at'],
                                protection_order_id=stops[0]['id'],
                                protection_action_id=next(a['id'] for a,o,_ in own if o['id']==stops[0]['id']),
                                unprotected_seconds_upper_bound=delay,
                                within_30_seconds=0<=delay<=30)
                flat=(qty==0 and sold==bought and sold>0 and sell_fills
                    and all(o['state'] in TERMINAL for _,o,_ in own)
                    and all(number(p['quantity'])==0 for p in snap['positions'].values())
                    and all(o['state'] in TERMINAL for o in snap['orders'].values())
                    and all(g['state'] in TERMINAL for g in snap.get('groups',[]))
                    and snap['pending_approvals']==0 and snap['other_asset_open_orders']==0
                    and all(number(v)==0 for v in snap['other_asset_values'].values())
                    and trade.get('phase')=='FLAT_VERIFIED')
                if flat:
                    evidence['exit_flat_validated']=True
                    evidence['flat_observed_at']=snap['observed_at']
                    for protected_at,protected_entries,stop_id in protections:
                        if (protected_at<at and protected_entries==entry_fills
                                and all(timestamp(f['executed_at'])>=protected_at for f in sell_fills.values())):
                            evidence.update(complete_protected_round_trip=True,
                                protected_order_id=stop_id,protection_observed_at=protected_at.isoformat(),
                                entry_execution_ids=sorted(entry_fills),exit_execution_ids=sorted(sell_fills))
                            break
            except (ValueError,KeyError,TypeError):continue
        timelines={t['action_id']:t for t in evidence['protection_action_timeline']}
        for tranche in tranches.values():
            local=timelines.get(tranche['protection_action_id'],{})
            milestones={'execution':tranche['executed_at'],'observation':tranche['first_observed_at'],
                'confirmed_observation':tranche['protection_confirmed_observed_at'],**local}
            pairs=[('fill_to_observation','execution','observation'),
                ('observation_to_protection_intent','observation','protection_intent_at'),
                ('intent_to_claim','protection_intent_at','claim_at'),
                ('claim_to_review','claim_at','review_started_at'),
                ('review_roundtrip','review_started_at','review_result_at'),
                ('confirmation_wait','confirmation_wait_started_at','user_confirmation_at'),
                ('confirmation_to_place','user_confirmation_at','place_started_at'),
                ('place_to_confirmed_observation','place_started_at','confirmed_observation')]
            tranche['latency_segments']={}
            for name,start,end in pairs:
                a,b=milestones.get(start),milestones.get(end)
                seconds=(timestamp(b)-timestamp(a)).total_seconds() if a and b else None
                tranche['latency_segments'][name]={'started_at':a,'ended_at':b,
                    'seconds':seconds if seconds is not None and seconds>=0 else None}
        evidence['protection_tranches']=sorted(tranches.values(),key=lambda t:(timestamp(t['executed_at']),t['execution_id']))
        evidence['protection_timeliness_passed']=bool(tranches) and all(t['within_30_seconds'] for t in tranches.values())
        evidence['module_functional_closed_loop']=evidence['complete_protected_round_trip'] and not evidence['manual_takeover_detected']
        evidence['supervised_module_acceptance_passed']=(evidence['module_functional_closed_loop']
            and evidence['protection_timeliness_passed'])
        # Identity-bound observations establish facts, not cryptographic source authentication.
        evidence['provenance_cryptographically_authenticated']=False
        results.append(evidence)
    return results


def release_check(book,status,test_evidence,*,clock=None):
    now=(clock or book.clock)();fingerprint=source_fingerprint();blockers=[]
    if not test_evidence.get('passed') or test_evidence.get('source_fingerprint')!=fingerprint:
        blockers.append('CURRENT_SOURCE_TEST_EVIDENCE_MISSING')
    if status.get('source_fingerprint')!=fingerprint:blockers.append('RUNNING_PROCESS_BUILD_NOT_VERIFIED')
    if not status.get('running') or not status.get('healthy'):blockers.append('LIVE_PROCESS_NOT_HEALTHY')
    try:fresh=0<=(now-timestamp(status['observed_at'])).total_seconds()<=30
    except (KeyError,ValueError,TypeError):fresh=False
    if not fresh:blockers.append('BROKER_OBSERVATION_NOT_CURRENT')
    if status.get('broker',{}).get('account')!='••••'+book.policy['account_suffix']:
        blockers.append('TARGET_ACCOUNT_NOT_VERIFIED')
    # A healthy flag in an old status file cannot prove that its PID still owns
    # this database. Keep this current operational gate separate from historical
    # lifecycle evidence, which remains useful after a successful session ends.
    from .session_control import process_matches
    db_path=book.db.execute('PRAGMA database_list').fetchone()[2]
    process_verified=process_matches(status.get('pid'),db_path)
    if not process_verified:blockers.append('LIVE_PROCESS_IDENTITY_NOT_VERIFIED')
    try:audit=book.audit.verify()
    except (ValueError,KeyError,TypeError):audit={'integrity_valid':False}
    if audit.get('integrity_valid') is not True:blockers.append('AUDIT_INTEGRITY_NOT_VERIFIED')
    state=book.state()
    blockers+=book.active_issues()
    if book.entry_gate()['blocked']:blockers.append('WATCHDOG_ENTRY_INHIBITED')
    if any(a['state'] in {'RUNNING','UNKNOWN'} for a in book.actions()):blockers.append('UNRESOLVED_SUBMISSION')
    if book.simulation:blockers.append('SYNTHETIC_BOOK_NOT_LIVE_EVIDENCE')
    current_operational_healthy=not blockers
    lifecycle=live_lifecycle_evidence(book,state,now) if audit.get('integrity_valid') is True else []
    real_fills=any(t['entry_fill_validated'] for t in lifecycle)
    accepted_protection=any(t['protection_acceptance_validated'] for t in lifecycle)
    real_round_trip=any(t['exit_flat_validated'] for t in lifecycle)
    smoke_passed=any(t['supervised_module_acceptance_passed'] for t in lifecycle)
    functional=any(t['module_functional_closed_loop'] for t in lifecycle)
    timely=any(t['complete_protected_round_trip'] and t['protection_timeliness_passed'] for t in lifecycle)
    manual=any(t['manual_takeover_detected'] for t in lifecycle)
    if not functional:blockers.append('MODULE_FUNCTIONAL_CLOSED_LOOP_NOT_VALIDATED')
    if not timely:blockers.append('PROTECTION_30_SECOND_DEADLINE_NOT_VALIDATED')
    boundary=_takeover_boundary(book,state,now)
    later_acceptance=bool(boundary['known']) and any(
        t['supervised_module_acceptance_passed'] and t['protection_tranches']
        and min(timestamp(x['executed_at']) for x in t['protection_tranches'])>timestamp(boundary['latest_at'])
        and timestamp(t['flat_observed_at'])>timestamp(boundary['latest_at']) for t in lifecycle)
    manual_current_blocker=(manual or boundary['present']) and not later_acceptance
    if manual_current_blocker:
        blockers.append('MANUAL_TAKEOVER_REQUIRES_NEW_MODULE_ACCEPTANCE')
        smoke_passed=False
    if not real_fills:blockers.append('NO_REAL_ENTRY_FILL_VALIDATED')
    if not accepted_protection:blockers.append('NO_REAL_PROTECTION_ACCEPTANCE_VALIDATED')
    if not real_round_trip:blockers.append('NO_REAL_EXIT_AND_FLAT_RECONCILIATION_VALIDATED')
    # A local setting/file cannot waive the native confirmation contract or prove
    # recovery under logout/sleep/auth expiry. No enable-prod boolean exists here.
    blockers+=['NATIVE_SPECIFIC_ORDER_CONFIRMATION_STILL_REQUIRED','UNATTENDED_RESTART_AND_AUTH_RECOVERY_NOT_VALIDATED']
    return {'as_of':now.isoformat(),'source_fingerprint':fingerprint,'audit':audit,
        'process_identity_verified':process_verified,'current_operational_healthy':current_operational_healthy,
        'historical_execution_issues':state['issues'],'recovery':book.recovery_status(),
        'software_test_evidence_current':test_evidence.get('passed') is True and test_evidence.get('source_fingerprint')==fingerprint,
        'actual_entry_fill_validated':real_fills,'actual_protection_acceptance_validated':accepted_protection,
        'actual_exit_flat_validated':real_round_trip,'module_functional_closed_loop_validated':functional,
        'protection_timeliness_validated':timely,'manual_takeover_detected':manual or boundary['present'],
        'historical_manual_takeover_detected':manual or boundary['present'],
        'manual_takeover_current_blocker':manual_current_blocker,'manual_takeover_boundary':boundary,
        'post_takeover_supervised_acceptance_validated':later_acceptance,
        'unattended_recovery_validated':False,'strategy_research_validated':False,
        'supervised_live_smoke_passed':smoke_passed,'live_lifecycle_evidence':lifecycle,
        'production_ready':False,'unattended_execution_authorized_by_report':False,'blockers':sorted(set(blockers))}
