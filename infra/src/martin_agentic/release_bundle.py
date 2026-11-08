"""Local source and test-input bundles. No deployment, credentials or execution.

Archive hashes detect changes relative to a trusted manifest, not authenticity
against a local owner who can rewrite both. Verification never runs bundled code.
"""
import gzip
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import tarfile
import tempfile
import zlib

from .audit import canonical, digest
from .qualification import source_files

MAX_BYTES=64*1024*1024
MAX_FILES=5000
CONTRACT_FIXTURE='reports/community_tool_contract_evidence.json'
PUBLIC_SCRIPTS = frozenset({
    'scripts/c3_qualification.py',
    'scripts/c4_qualification.py',
    'scripts/rehearse_local_process_faults.py',
    'scripts/rehearse_live_session_blocking.py',
})
LEGACY_ARTIFACT_FIXTURE='tests/fixtures/legacy-artifact-579e/manifest.json'
TEST_FIXTURE_ROOT='tests/fixtures'
# Explicit, reviewable non-Python test inputs. Python fixtures are already
# qualified source files via qualification.source_files.
REQUIRED_TEST_INPUTS=frozenset({
    LEGACY_ARTIFACT_FIXTURE,
    'tests/fixtures/robinhood_historicals_before_first_bar.json',
    'tests/fixtures/formal-tool-contract.json',
    'tests/fixtures/synthetic_recording.json',
    'tests/fixtures/probe_plugins/pyproject.toml',
    *{'tests/fixtures/policies/' + name for name in (
        'live_trial_2026-10-16.json','live_trial_2026-10-19.json',
        'live_trial_2026-10-21.json','live_trial_2026-10-22.json',
        'live_trial_2026-10-26.json','read_only_capture_2026-10-20.json',
        'read_only_capture_2026-10-21.json','read_only_capture_2026-10-27.json')},
})


def _fixture_inventory(root):
    directory=Path(root)/TEST_FIXTURE_ROOT
    if not directory.is_dir():raise ValueError('RELEASE_TEST_FIXTURE_ROOT_MISSING')
    files=set()
    for path in directory.rglob('*'):
        if not path.is_file() or '__pycache__' in path.parts or path.suffix=='.py':
            continue
        files.add(path.relative_to(root).as_posix())
    return files


def _safe_name(name):
    if (not isinstance(name,str) or not name or '\\' in name
            or PurePosixPath(name).is_absolute() or any(p in {'','..','.'} for p in name.split('/'))):
        raise ValueError('RELEASE_UNSAFE_PATH')
    return name


def _forbidden_name(name):
    parts=[p.lower() for p in PurePosixPath(name).parts]
    base=parts[-1]
    return (any(p=='.env' or p.startswith('.env.') or p in {'.aws','.ssh','.gnupg'} for p in parts)
            or base in {'auth.json','credentials','credentials.json','secrets.json','tokens.json','token.json',
                'id_rsa','id_dsa','id_ecdsa','id_ed25519'}
            or base.endswith(('.pem','.key','.p12','.pfx')))


def _allowed(name):
    if _forbidden_name(name):
        return False
    return (name in {'pyproject.toml','uv.lock','README.md',CONTRACT_FIXTURE,
                     'reports/community_tool_contract_evidence.json',
                     *PUBLIC_SCRIPTS}
        or name.startswith(('infra/src/martin_agentic/','infra/src/trading_contracts/',
            'infra/src/trading_infra/','strategy/martin_luk/src/',
            'strategy/examples/src/','tests/','config/',
            'examples/','docs/','strategy-spec/')))


def _bounded_file(path,limit):
    """Reject links/non-regular/oversized files before reading bounded bytes."""
    try:fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    except OSError as exc:raise ValueError('RELEASE_INPUT_UNREADABLE_OR_SYMLINK') from None
    with os.fdopen(fd,'rb') as handle:
        info=os.fstat(handle.fileno())
        if not stat.S_ISREG(info.st_mode):raise ValueError('RELEASE_INPUT_NOT_REGULAR')
        if info.st_size>limit:raise ValueError('RELEASE_INPUT_TOO_LARGE')
        body=handle.read(limit+1)
        if len(body)>limit:raise ValueError('RELEASE_INPUT_TOO_LARGE')
    return body,info.st_mode


def _files(root):
    qualified=set(source_files(root));extra={root/CONTRACT_FIXTURE}
    for relative in ('infra/src','strategy','tests','config','examples','scripts','skills','sites'):
        base=Path(root)/relative
        if base.is_dir():
            for path in base.rglob('*'):
                if path.is_file() and '__pycache__' not in path.parts and _forbidden_name(
                        path.relative_to(root).as_posix()):
                    raise ValueError('RELEASE_FORBIDDEN_INPUT')
    for required in ('pyproject.toml','uv.lock',CONTRACT_FIXTURE):
        if not (root/required).is_file():raise ValueError('RELEASE_REQUIRED_INPUT_MISSING:'+required)
    missing=sorted(path for path in REQUIRED_TEST_INPUTS
                   if not (root/path).is_file())
    if missing:raise ValueError('RELEASE_REQUIRED_INPUT_MISSING:'+missing[0])
    discovered=_fixture_inventory(root)
    required_fixtures=set(REQUIRED_TEST_INPUTS)
    if discovered!=required_fixtures:
        missing_discovered=sorted(required_fixtures-discovered)
        extra_discovered=sorted(discovered-required_fixtures)
        detail=(missing_discovered+extra_discovered)[0] if (missing_discovered or extra_discovered) else 'unknown'
        raise ValueError('RELEASE_TEST_FIXTURE_INVENTORY_MISMATCH:'+detail)
    extra.update(root/path for path in REQUIRED_TEST_INPUTS)
    if not (root/'examples').is_dir():raise ValueError('RELEASE_EXAMPLES_REQUIRED')
    extra.update(p for p in (root/'examples').rglob('*') if p.is_file() and p.suffix in {'.json','.md'})
    report_fixture=root/'reports/community_tool_contract_evidence.json'
    if report_fixture.is_file():extra.add(report_fixture)
    if (root/'README.md').exists():extra.add(root/'README.md')
    paths=sorted((qualified|extra), key=lambda p: p.relative_to(root).as_posix())
    if len(paths)>MAX_FILES:raise ValueError('RELEASE_TOO_MANY_FILES')
    payloads={};rows=[];total=0
    for path in paths:
        relative=_safe_name(path.relative_to(root).as_posix())
        if not _allowed(relative) or any(p in {'__pycache__','.git','.venv','.env','node_modules'} for p in path.relative_to(root).parts):
            raise ValueError('RELEASE_FORBIDDEN_INPUT')
        # Do not follow a source file or a source-directory symlink outside the tree.
        if path.is_symlink() or any(p.is_symlink() for p in path.parents if p!=root and root in p.parents):
            raise ValueError('RELEASE_SYMLINK_INPUT')
        body,mode=_bounded_file(path,MAX_BYTES-total);total+=len(body)
        payloads[relative]=body
        rows.append({'path':relative,'bytes':len(body),'sha256':hashlib.sha256(body).hexdigest(),
            'qualified_source':path in qualified,'mode':0o755 if mode&0o111 else 0o644})
    return rows,payloads


def _source_hash(rows,payloads):
    h=hashlib.sha256(b'martin-source-v2\0')
    for row in rows:
        if row['qualified_source']:
            h.update(row['path'].encode());h.update(b'\0');h.update(payloads[row['path']]);h.update(b'\0')
    return h.hexdigest()


def build_release_bundle(root,output_dir):
    root=Path(root).resolve(strict=True);output=Path(output_dir).absolute()
    if output.exists() or output.is_symlink():raise ValueError('RELEASE_OUTPUT_EXISTS')
    rows,payloads=_files(root);before=_source_hash(rows,payloads);input_hash=digest(rows)
    _same_snapshot(root,before,input_hash,'DURING_SNAPSHOT')
    output.parent.mkdir(parents=True,exist_ok=True)
    stage=Path(tempfile.mkdtemp(prefix='.release-',dir=output.parent));stage.chmod(0o700)
    try:
        archive=stage/'source-and-tests.tar.gz'
        with archive.open('xb') as raw:
            os.chmod(archive,0o600)
            with gzip.GzipFile(filename='',mode='wb',fileobj=raw,mtime=0) as zipped:
                with tarfile.open(fileobj=zipped,mode='w',format=tarfile.PAX_FORMAT) as tar:
                    for row in rows:
                        info=tarfile.TarInfo(row['path']);info.size=row['bytes'];info.mode=row['mode'];info.mtime=0
                        tar.addfile(info,io.BytesIO(payloads[row['path']]))
            raw.flush();os.fsync(raw.fileno())
        manifest={'schema_version':1,'scope':'local_source_and_test_inputs_not_deployment_authority',
            'source_fingerprint':before,'files':rows,'file_count':len(rows),
            'input_set_sha256':digest(rows),'archive_sha256':hashlib.sha256(_bounded_file(archive,MAX_BYTES)[0]).hexdigest(),
            'production_ready':False,'independent_immutable_custody':False,
            'tests_executed_by_builder':False,'archive':'source-and-tests.tar.gz'}
        with (stage/'manifest.json').open('x') as f:
            os.chmod(f.name,0o600);f.write(canonical(manifest)+'\n');f.flush();os.fsync(f.fileno())
        verify_release_bundle(stage,expected_source=before)
        _same_snapshot(root,before,input_hash,'BEFORE_PUBLICATION')
        # mkdir is exclusive; do not overwrite a prior release or an existing link.
        output.mkdir(mode=0o700,exist_ok=False)
        for name in ('manifest.json','source-and-tests.tar.gz'):(stage/name).replace(output/name)
        for directory in (output,output.parent):
            fd=os.open(directory,os.O_RDONLY)
            try:os.fsync(fd)
            finally:os.close(fd)
        return {'status':'RELEASE_BUNDLE_BUILT','directory':str(output),'source_fingerprint':before,
            'input_set_sha256':manifest['input_set_sha256'],'archive_sha256':manifest['archive_sha256'],
            'files':len(rows),'tests_executed':False,'production_ready':False}
    finally:shutil.rmtree(stage)


def _same_snapshot(root,source_hash,input_hash,phase):
    rows,payloads=_files(root)
    if _source_hash(rows,payloads)!=source_hash:
        raise ValueError('RELEASE_SOURCE_CHANGED_'+phase)
    if digest(rows)!=input_hash:raise ValueError('RELEASE_INPUT_SET_CHANGED_'+phase)


def _read_archive(archive_bytes,rows):
    try:
        payloads={}
        # Bound decompression before tarfile interprets extension/PAX headers.
        with gzip.GzipFile(fileobj=io.BytesIO(archive_bytes),mode='rb') as zipped:
            decompressed=zipped.read(MAX_BYTES+MAX_FILES*4096+1)
        if len(decompressed)>MAX_BYTES+MAX_FILES*4096:raise ValueError('RELEASE_DECOMPRESSED_ARCHIVE_TOO_LARGE')
        with tarfile.open(fileobj=io.BytesIO(decompressed),mode='r:') as tar:
            for index,member in enumerate(tar):
                if index>=len(rows):raise ValueError('RELEASE_EXTRA_ARCHIVE_MEMBER')
                row=rows[index]
                if (not member.isfile() or member.name!=row['path'] or member.size!=row['bytes']
                        or member.mode!=row['mode']):raise ValueError('RELEASE_ARCHIVE_MEMBER_INVALID')
                stream=tar.extractfile(member)
                if stream is None:raise ValueError('RELEASE_ARCHIVE_MEMBER_INVALID')
                with stream:body=stream.read(member.size+1)
                if len(body)!=row['bytes'] or hashlib.sha256(body).hexdigest()!=row['sha256']:
                    raise ValueError('RELEASE_FILE_HASH_MISMATCH')
                payloads[member.name]=body
        if len(payloads)!=len(rows):raise ValueError('RELEASE_MISSING_ARCHIVE_MEMBER')
    except (OSError,EOFError,tarfile.TarError,zlib.error) as exc:
        raise ValueError("RELEASE_ARCHIVE_FORMAT_INVALID") from None
    return payloads


def verify_release_bundle(directory,*,expected_source=None,expected_input_set_sha256=None,extract_to=None):
    if extract_to is not None and (expected_source is None or expected_input_set_sha256 is None):
        raise ValueError('RELEASE_EXTRACTION_TRUSTED_PINS_REQUIRED')
    for value in (expected_source,expected_input_set_sha256):
        if value is not None and (not isinstance(value,str) or not re.fullmatch('[0-9a-f]{64}',value)):
            raise ValueError('RELEASE_TRUSTED_HASH_INVALID')
    directory=Path(directory);manifest_path=directory/'manifest.json';archive=directory/'source-and-tests.tar.gz'
    if manifest_path.is_symlink() or archive.is_symlink():raise ValueError('RELEASE_SYMLINK_ARCHIVE')
    manifest=json.loads(_bounded_file(manifest_path,4*1024*1024)[0])
    if (not isinstance(manifest,dict) or type(manifest.get('schema_version')) is not int
            or manifest.get('schema_version')!=1 or manifest.get('archive')!='source-and-tests.tar.gz'
            or manifest.get('scope')!='local_source_and_test_inputs_not_deployment_authority'
            or manifest.get('tests_executed_by_builder') is not False
            or manifest.get('production_ready') is not False or manifest.get('independent_immutable_custody') is not False
            or type(manifest.get('file_count')) is not int):
        raise ValueError('RELEASE_MANIFEST_INVALID')
    for key in ('source_fingerprint','input_set_sha256','archive_sha256'):
        if not isinstance(manifest.get(key),str) or not re.fullmatch('[0-9a-f]{64}',manifest[key]):
            raise ValueError('RELEASE_MANIFEST_HASH_INVALID')
    rows=manifest.get('files')
    if (not isinstance(rows,list) or not rows or len(rows)>MAX_FILES or len(rows)!=manifest.get('file_count')
            or digest(rows)!=manifest.get('input_set_sha256')):raise ValueError('RELEASE_INPUT_SET_INVALID')
    names=[]
    for row in rows:
        if not isinstance(row,dict) or set(row)!={'path','bytes','sha256','qualified_source','mode'}:
            raise ValueError('RELEASE_FILE_METADATA_INVALID')
        name=_safe_name(row['path'])
        if (not _allowed(name) or type(row['bytes']) is not int or not 0<=row['bytes']<=MAX_BYTES
                or type(row['qualified_source']) is not bool or type(row['mode']) is not int
                or row['mode'] not in {0o644,0o755} or not isinstance(row['sha256'],str)
                or not re.fullmatch('[0-9a-f]{64}',row['sha256'])):
            raise ValueError('RELEASE_FILE_METADATA_INVALID')
        names.append(name)
    if names!=sorted(set(names)) or sum(r['bytes'] for r in rows)>MAX_BYTES:
        raise ValueError('RELEASE_DUPLICATE_OR_OVERSIZED_INPUTS')
    if expected_source is not None and expected_source!=manifest['source_fingerprint']:
        raise ValueError('RELEASE_EXPECTED_SOURCE_MISMATCH')
    if expected_input_set_sha256 is not None and expected_input_set_sha256!=manifest['input_set_sha256']:
        raise ValueError('RELEASE_EXPECTED_INPUT_SET_MISMATCH')
    archive_bytes=_bounded_file(archive,MAX_BYTES)[0]
    if hashlib.sha256(archive_bytes).hexdigest()!=manifest.get('archive_sha256'):
        raise ValueError('RELEASE_ARCHIVE_HASH_MISMATCH')
    payloads=_read_archive(archive_bytes,rows)
    # Private extraction is manual after all archive paths and hashes pass.
    # No bundled code is imported or run while verifying its source inventory.
    with tempfile.TemporaryDirectory(prefix='martin-release-verify-') as temporary:
        root=Path(temporary)
        for row in rows:
            path=root/row['path'];path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(payloads[row['path']])
        if any(not (root/p).is_file() for p in ('pyproject.toml','uv.lock',CONTRACT_FIXTURE,LEGACY_ARTIFACT_FIXTURE)) or not (root/'examples').is_dir():
            raise ValueError('RELEASE_REQUIRED_INPUT_MISSING')
        actual={p.relative_to(root).as_posix() for p in source_files(root)}
        claimed={r['path'] for r in rows if r['qualified_source']}
        if actual!=claimed or _source_hash(rows,payloads)!=manifest['source_fingerprint']:
            raise ValueError('RELEASE_SOURCE_FINGERPRINT_MISMATCH')
    if extract_to is not None:
        destination=Path(extract_to)
        destination.mkdir(mode=0o700,parents=True,exist_ok=False)
        for row in rows:
            path=destination/row['path'];path.parent.mkdir(parents=True,exist_ok=True)
            with path.open('xb') as f:f.write(payloads[row['path']]);os.chmod(path,row['mode'])
    return {'status':'RELEASE_BUNDLE_VERIFIED','source_fingerprint':manifest['source_fingerprint'],
        'input_set_sha256':manifest['input_set_sha256'],'archive_sha256':manifest['archive_sha256'],
        'files':len(rows),'extracted':extract_to is not None,'tests_executed':False,
        'manifest_requires_trusted_provenance':True,'production_ready':False}
