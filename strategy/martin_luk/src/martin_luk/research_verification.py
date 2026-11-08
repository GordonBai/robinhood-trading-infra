"""Read-only registered research replay verification, never strategy admission.

All registered result fields are compared, including journals, execution IDs,
scenario/fold order and risk checkpoints. The original run ID is retained for
comparison; no metric-only projection or replacement run is registered. Inputs
are frozen in a private temporary directory before the existing replay engine
runs, then checked again against their original files and registry record.
"""
from copy import deepcopy
import hashlib
import json
import os
import re
from pathlib import Path
import sqlite3
import stat
import tempfile
import time

from martin_agentic.audit import canonical, digest
from .research import _load, _replay, strategy_freeze
from martin_agentic.ledger import timestamp


DEFAULT_LIMITS = {
    'max_registry_bytes':256_000_000,
    'max_document_bytes':64_000_000,
    'max_input_bytes':256_000_000,
    'max_input_files':256,
    'max_events':250_000,
    'max_fold_scenarios':200,
    'max_replay_event_visits':5_000_000,
    'max_coverage_work_units':5_000_000,
}


class _VerificationFailure(ValueError):
    pass


def _file(path, limit, *, missing_ok=False):
    path = Path(path).expanduser()
    if path.is_symlink():
        raise _VerificationFailure('VERIFICATION_SYMLINK_INPUT_UNSUPPORTED')
    try:
        fd = os.open(path,os.O_RDONLY|os.O_NONBLOCK|os.O_NOFOLLOW)
        with os.fdopen(fd,'rb') as handle:
            before = os.fstat(handle.fileno())
            if not stat.S_ISREG(before.st_mode):
                raise _VerificationFailure('VERIFICATION_REGULAR_FILE_REQUIRED')
            if before.st_size > limit:
                raise _VerificationFailure('VERIFICATION_DOCUMENT_LIMIT_EXCEEDED')
            raw = handle.read(limit + 1)
            after = os.fstat(handle.fileno())
    except FileNotFoundError:
        if missing_ok:
            return None
        raise _VerificationFailure('VERIFICATION_INPUT_MISSING') from None
    if len(raw) > limit:
        raise _VerificationFailure('VERIFICATION_DOCUMENT_LIMIT_EXCEEDED')
    identity = lambda value:(value.st_dev,value.st_ino,value.st_size,value.st_mtime_ns,value.st_ctime_ns)
    if identity(before) != identity(after) or identity(after) != identity(path.stat()):
        raise _VerificationFailure('VERIFICATION_INPUT_CHANGED_DURING_READ')
    return {'raw':raw,'sha256':hashlib.sha256(raw).hexdigest(),
            'inode':(after.st_dev,after.st_ino)}


def _registry(path, run_id, limits):
    path = Path(path).expanduser()
    if path.is_symlink() or not path.is_file():
        raise _VerificationFailure('VERIFICATION_EXISTING_REGISTRY_REQUIRED')
    metadata = path.stat()
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > limits['max_registry_bytes']:
        raise _VerificationFailure('VERIFICATION_REGISTRY_LIMIT_EXCEEDED')
    # mode=ro is insufficient to promise zero sidecar changes for WAL input.
    # Require a settled rollback-journal registry; do not ignore committed WAL
    # contents through immutable=1 or blindly copy an active database.
    fd = os.open(path,os.O_RDONLY|os.O_NONBLOCK|os.O_NOFOLLOW)
    with os.fdopen(fd,'rb') as handle:
        header = handle.read(100)
    if len(header)<100 or header[:16]!=b'SQLite format 3\x00':
        raise _VerificationFailure('VERIFICATION_REGISTRY_HEADER_INVALID')
    if header[18]!=1 or header[19]!=1 or any(Path(str(path)+suffix).exists()
            or Path(str(path)+suffix).is_symlink() for suffix in ('-wal','-shm','-journal')):
        raise _VerificationFailure('VERIFICATION_REGISTRY_CONSISTENT_SNAPSHOT_REQUIRED')
    connection = sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=.25)
    deadline = time.monotonic()+5
    connection.set_progress_handler(lambda:int(time.monotonic()>deadline),1000)
    try:
        connection.execute('PRAGMA query_only=ON');connection.execute('BEGIN')
        if connection.execute('SELECT count(*) FROM runs WHERE id=?',(run_id,)).fetchone()[0] != 1:
            raise _VerificationFailure('VERIFICATION_EXACTLY_ONE_REGISTERED_RUN_REQUIRED')
        row = connection.execute('''SELECT length(CAST(manifest AS BLOB)),
            length(CAST(result AS BLOB)),manifest_sha256,status,started_at FROM runs WHERE id=?''',(run_id,)).fetchone()
        if row is None:
            raise _VerificationFailure('VERIFICATION_RUN_NOT_REGISTERED')
        if row[3] == 'STARTED' or row[1] is None:
            raise _VerificationFailure('VERIFICATION_REGISTERED_RUN_NOT_TERMINAL')
        if any(type(n) is not int or n > limits['max_document_bytes'] for n in row[:2]):
            raise _VerificationFailure('VERIFICATION_REGISTERED_DOCUMENT_LIMIT_EXCEEDED')
        payload = connection.execute('SELECT manifest,result FROM runs WHERE id=?',(run_id,)).fetchone()
        value = {'id':run_id,'manifest':payload[0],'result':payload[1],
                 'manifest_sha256':row[2],'status':row[3],'started_at':row[4]}
        if not all(isinstance(v,str) for v in value.values()):
            raise _VerificationFailure('VERIFICATION_REGISTRY_RECORD_INVALID')
        if hashlib.sha256(value['manifest'].encode()).hexdigest() != value['manifest_sha256']:
            raise _VerificationFailure('VERIFICATION_REGISTERED_MANIFEST_HASH_INVALID')
        if (path.stat().st_dev,path.stat().st_ino) != (metadata.st_dev,metadata.st_ino):
            raise _VerificationFailure('VERIFICATION_REGISTRY_REPLACED')
        return value,(metadata.st_dev,metadata.st_ino)
    finally:
        connection.close()


def _reference_name(value):
    # Match the existing loader's relative/absolute/parent-reference semantics.
    # Only snapshot *destinations* are constrained to private flat names.
    if not isinstance(value,str) or not value or '\0' in value:
        raise _VerificationFailure('VERIFICATION_INPUT_REFERENCE_INVALID')
    return value


def _reproduce(manifest, base, run_id, limits, *, path_resolver=None):
    """Existing evaluator workflow without run registration or persistent files."""
    report = {'run_id':run_id,'status':'FAILED','data_gaps':[],'results':[],
        'strategy_profitability_validated':False,'execution_is_simulated':True,
        'robinhood_execution_validated':False,'provenance_cryptographically_authenticated':False}
    try:
        report['manifest'] = manifest
        events,gaps = _load(manifest,base,path_resolver=path_resolver);report['data_gaps'] = gaps
        if gaps:
            report['status'] = 'DATA_GAP'
        else:
            products = len(manifest['folds'])*len(manifest['scenarios'])
            if len(events)>limits['max_events'] or products>limits['max_fold_scenarios'] or len(events)*products>limits['max_replay_event_visits']:
                raise _VerificationFailure('VERIFICATION_REPLAY_WORK_LIMIT_EXCEEDED')
            for scenario in manifest['scenarios']:
                carry = None;blocked = False
                for fold in manifest['folds']:
                    if blocked:
                        report['results'].append({'fold_id':fold['id'],'scenario':scenario,
                            'complete_intraday_sample':False,'status':'BLOCKED_PREVIOUS_FOLD_OPEN_EXPOSURE',
                            'risk_state_not_reset':True})
                        continue
                    result = _replay(events,fold,scenario,manifest,carry=carry)
                    report['results'].append(result);carry = result['risk_checkpoint']
                    blocked = not result['complete_intraday_sample']
            report['status'] = 'SIMULATED_COMPLETE' if all(r['complete_intraday_sample'] for r in report['results']) else 'INCOMPLETE_OPEN_EXPOSURE'
            report['synthetic_market_input'] = manifest['synthetic_input']
            report['note'] = 'No selection or ranking of scenarios; all frozen folds, no-signal, rejected and unfilled cases retained. Real market input still uses assumed execution.'
    except _VerificationFailure:
        raise
    except (ValueError,KeyError,TypeError,OSError) as exc:
        report['error'] = type(exc).__name__+':'+str(exc)
    return report


def verify_research_run(registry_path, run_id, manifest_path, report_path=None, *, limits=None):
    """Verify one exact registered result; source/report/registry remain untouched.

    ``reproducible=True`` can describe a faithfully reproduced DATA_GAP or
    failure. ``data_contract_qualified`` separately denotes an internally
    consistent recorded input contract, never authenticated provider provenance,
    validated profitability, Robinhood execution or permission to trade.
    """
    bounds = dict(DEFAULT_LIMITS)
    if limits is not None:
        if not isinstance(limits,dict) or set(limits)-set(bounds):
            raise ValueError('VERIFICATION_LIMITS_INVALID')
        bounds.update(limits)
    if any(type(value) is not int or value<=0 for value in bounds.values()):
        raise ValueError('VERIFICATION_LIMITS_INVALID')
    if not isinstance(run_id,str) or not 0<len(run_id)<=256 or any(c in run_id for c in '/\\\0'):
        raise ValueError('VERIFICATION_RUN_ID_INVALID')
    result = {'schema_version':1,'scope':'registered_research_reproducibility',
        'run_id':run_id,'status':'VERIFICATION_FAILED','reproducible':False,
        'data_contract_qualified':False,'research_admitted':False,'production_ready':False,
        'strategy_profitability_validated':False,'robinhood_execution_validated':False,
        'provenance_cryptographically_authenticated':False,'broker_calls':0,
        'new_runs_registered':0,'original_inputs_mutated':False,
        'registry_input_scope':'settled_rollback_journal_sqlite_only','limits':bounds,'blockers':[]}
    try:
        registry = Path(registry_path).expanduser().resolve(strict=True)
        if Path(registry_path).expanduser().is_symlink():
            raise _VerificationFailure('VERIFICATION_SYMLINK_INPUT_UNSUPPORTED')
        registered,registry_inode = _registry(registry,run_id,bounds)
        stored = json.loads(registered['result'])
        if (not isinstance(stored,dict) or stored.get('run_id')!=run_id
                or stored.get('status')!=registered['status']):
            raise _VerificationFailure('VERIFICATION_REGISTERED_REPORT_IDENTITY_MISMATCH')
        report = Path(report_path) if report_path is not None else registry.parent/(run_id+'.json')
        report_input = _file(report,bounds['max_document_bytes'])
        if report_input['raw'] != (registered['result']+'\n').encode():
            raise _VerificationFailure('VERIFICATION_REPORT_REGISTRY_BYTES_MISMATCH')
        manifest_path = Path(manifest_path).expanduser()
        manifest_input = _file(manifest_path,bounds['max_document_bytes'])
        if (manifest_input['sha256']!=registered['manifest_sha256']
                or manifest_input['raw']!=registered['manifest'].encode()):
            raise _VerificationFailure('VERIFICATION_MANIFEST_REGISTRY_MISMATCH')
        manifest = json.loads(manifest_input['raw'])
        if not isinstance(manifest,dict) or stored.get('manifest')!=manifest:
            raise _VerificationFailure('VERIFICATION_MANIFEST_REPORT_MISMATCH')
        freeze = strategy_freeze()
        if manifest.get('strategy_freeze')!=freeze:
            raise _VerificationFailure('VERIFICATION_STRATEGY_FREEZE_MISMATCH')
        folds,scenarios = manifest.get('folds'),manifest.get('scenarios')
        if not isinstance(folds,list) or not isinstance(scenarios,list):
            raise _VerificationFailure('VERIFICATION_FROZEN_FOLDS_SCENARIOS_REQUIRED')
        if len(folds)*len(scenarios)>bounds['max_fold_scenarios']:
            raise _VerificationFailure('VERIFICATION_REPLAY_WORK_LIMIT_EXCEEDED')
        declarations = [*manifest.get('sources',[]),manifest.get('dataset',{})]
        # The loader visits declarations, not just distinct paths. Repeated
        # references must not multiply parsing work behind a unique-file cap.
        if len(declarations)>bounds['max_input_files']:
            raise _VerificationFailure('VERIFICATION_INPUT_FILE_LIMIT_EXCEEDED')
        references = {}
        for reference in declarations:
            if not isinstance(reference,dict):
                raise _VerificationFailure('VERIFICATION_INPUT_REFERENCE_INVALID')
            name = _reference_name(reference.get('path'));expected = reference.get('sha256')
            if not isinstance(expected,str) or re.fullmatch('[0-9a-f]{64}',expected) is None:
                raise _VerificationFailure('VERIFICATION_INPUT_HASH_INVALID')
            if name in references and references[name]!=expected:
                raise _VerificationFailure('VERIFICATION_INPUT_HASH_CONFLICT')
            references[name] = expected
        if len(references)>bounds['max_input_files']:
            raise _VerificationFailure('VERIFICATION_INPUT_FILE_LIMIT_EXCEEDED')
        base = manifest_path.resolve().parent;inputs = {};total = 0
        with tempfile.TemporaryDirectory(prefix='martin-research-verification-') as folder:
            frozen = Path(folder)
            frozen_paths = {}
            for index,(name,expected) in enumerate(references.items()):
                source = base/name
                target = frozen/('input-'+str(index)+'.json')
                frozen_paths[name] = target
                captured = _file(source,min(bounds['max_document_bytes'],bounds['max_input_bytes']-total),missing_ok=True)
                inputs[name] = captured
                if captured is None:
                    continue
                total += len(captured['raw'])
                if total>bounds['max_input_bytes']:
                    raise _VerificationFailure('VERIFICATION_TOTAL_INPUT_BYTES_LIMIT_EXCEEDED')
                if captured['sha256']!=expected:
                    raise _VerificationFailure('VERIFICATION_FROZEN_SOURCE_HASH_MISMATCH')
                target.write_bytes(captured['raw']);target.chmod(0o600)
            declared_bytes=sum(len(inputs[r['path']]['raw']) for r in declarations
                               if inputs[r['path']] is not None)
            if declared_bytes>bounds['max_input_bytes']:
                raise _VerificationFailure('VERIFICATION_DECLARED_INPUT_WORK_BYTES_LIMIT_EXCEEDED')
            dataset_input = inputs[manifest['dataset']['path']]
            if dataset_input is not None:
                events_preview = json.loads(dataset_input['raw'])
                if isinstance(events_preview,list):
                    if (len(events_preview)>bounds['max_events']
                            or len(events_preview)*len(folds)*len(scenarios)>bounds['max_replay_event_visits']):
                        raise _VerificationFailure('VERIFICATION_REPLAY_WORK_LIMIT_EXCEEDED')
                    # Coverage validation precedes replay, so bound its declared
                    # symbol/day/event work before invoking _load as well.
                    coverage = manifest.get('coverage',{})
                    declared = coverage.get('folds',{}) if isinstance(coverage,dict) else {}
                    coverage_work = 0
                    for fold in folds:
                        if not isinstance(fold,dict):
                            continue  # Existing validator reproduces the error.
                        spec = declared.get(fold.get('id'),{}) if isinstance(declared,dict) else {}
                        symbols = spec.get('universe_symbols',[]) if isinstance(spec,dict) else []
                        try:
                            seconds=max(0,(timestamp(fold['test_end'])-timestamp(fold['test_start'])).total_seconds())
                            days=int(seconds//86400)+2
                        except (KeyError,ValueError,TypeError):
                            continue
                        coverage_work += len(symbols)*days*(len(events_preview)+1)
                    if coverage_work>bounds['max_coverage_work_units']:
                        raise _VerificationFailure('VERIFICATION_COVERAGE_WORK_LIMIT_EXCEEDED')
            reproduced = _reproduce(deepcopy(manifest),frozen,run_id,bounds,
                path_resolver=lambda name:frozen_paths[name])
            # Compare every field, not just status or profitable aggregate rows.
            result['registered_result_sha256'] = digest(stored)
            result['reproduced_result_sha256'] = digest(reproduced)
            if canonical(reproduced)!=canonical(stored):
                raise _VerificationFailure('VERIFICATION_EXACT_REPLAY_RESULT_MISMATCH')
            for name,captured in inputs.items():
                after = _file(base/name,bounds['max_document_bytes'],missing_ok=True)
                if (captured is None)!=(after is None) or captured is not None and (
                        after['sha256']!=captured['sha256'] or after['inode']!=captured['inode']):
                    raise _VerificationFailure('VERIFICATION_SOURCE_CHANGED_DURING_REPLAY')
        for path,captured in ((manifest_path,manifest_input),(report,report_input)):
            after = _file(path,bounds['max_document_bytes'])
            if after['sha256']!=captured['sha256'] or after['inode']!=captured['inode']:
                raise _VerificationFailure('VERIFICATION_DOCUMENT_CHANGED_DURING_REPLAY')
        after,after_inode = _registry(registry,run_id,bounds)
        if after_inode!=registry_inode or canonical(after)!=canonical(registered):
            raise _VerificationFailure('VERIFICATION_REGISTRY_CHANGED_DURING_REPLAY')
        if strategy_freeze()!=freeze:
            raise _VerificationFailure('VERIFICATION_STRATEGY_CHANGED_DURING_REPLAY')
        synthetic = manifest.get('synthetic_input') is not False
        data_status = ('DATA_GAP' if reproduced['status']=='DATA_GAP' else 'INVALID_RESEARCH_INPUT'
            if reproduced['status']=='FAILED' else 'SYNTHETIC_ONLY' if synthetic else
            'INCOMPLETE_OPEN_EXPOSURE' if reproduced['status']=='INCOMPLETE_OPEN_EXPOSURE'
            else 'FROZEN_RECORDED_CONTRACT_VALIDATED')
        result.update(status='REPRODUCIBLE',reproducible=True,
            registered_status=reproduced['status'],input_data_status=data_status,
            data_gaps=reproduced.get('data_gaps',[]),
            data_contract_qualified=not synthetic and reproduced['status']=='SIMULATED_COMPLETE',
            manifest_sha256=manifest_input['sha256'],report_sha256=report_input['sha256'],
            input_file_count=len(inputs),present_input_bytes=total,
            exact_result_fields_compared=sorted(stored),
            note='Exact replay verification only; no predeclared profitability admission criteria exist.')
    except (ValueError,KeyError,TypeError,OSError,sqlite3.Error) as exc:
        result['blockers'] = [str(exc) if isinstance(exc,_VerificationFailure) else 'VERIFICATION_INPUT_OR_REGISTRY_INVALID']
    return result
