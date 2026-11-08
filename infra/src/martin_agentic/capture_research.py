"""Bridge frozen Robinhood capture archives to research source/dataset files.

Preserve received events and timestamps. This never grants research admission,
repairs missing data, sorts consumption order, or upgrades provenance assertions.
"""
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import tempfile

from .audit import canonical, digest
from .ledger import timestamp


def _file_hash(path):
    value = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            value.update(chunk)
    return value.hexdigest()


def _consumption_index(root, stage, manifest):
    """Verify streamed decisions and index dispositions in private scratch space."""
    info = manifest.get('files', {}).get('decisions.ndjson')
    if not isinstance(info, dict) or 'decision_count' not in manifest:
        raise ValueError('CAPTURE_CONSUMPTION_DECISIONS_REQUIRED')
    path = stage / '.consumption.sqlite'
    db = sqlite3.connect(path)
    os.chmod(path, 0o600)
    try:
        db.execute('PRAGMA cache_size=-1024')
        db.execute('PRAGMA journal_mode=OFF')
        db.execute('CREATE TABLE decisions(event_id TEXT, result_id TEXT, status TEXT, reason TEXT, recorded_at TEXT, seen INTEGER DEFAULT 0, PRIMARY KEY(event_id,result_id))')
        checksum = hashlib.sha256(); size = count = 0
        with (root / 'decisions.ndjson').open('rb') as stream:
            for line in stream:
                checksum.update(line); size += len(line); count += 1
                row = json.loads(line); context = row.get('context')
                if (row.get('decision_sequence') != count or not isinstance(context, dict)
                        or any(not isinstance(row.get(key), str) or not row[key] for key in ('event_id', 'result_id'))
                        or row.get('fingerprint') != digest({'event_id': row['event_id'],
                            'result_id': row['result_id'], 'context': context})):
                    raise ValueError('CAPTURE_DECISION_INTEGRITY_FAILURE')
                timestamp(row['recorded_at'])
                status = context.get('status')
                if status is not None and not isinstance(status, str):
                    raise ValueError('CAPTURE_DECISION_INTEGRITY_FAILURE')
                db.execute('INSERT INTO decisions(event_id,result_id,status,reason,recorded_at) VALUES(?,?,?,?,?)',
                    (row['event_id'], row['result_id'], status,
                     str(context.get('reason', 'UNSPECIFIED')), row['recorded_at']))
                if count % 512 == 0:
                    db.commit()
        db.commit()
        if (checksum.hexdigest() != info['sha256'] or size != info['bytes']
                or count != info['records'] or count != manifest['decision_count']):
            raise ValueError('CAPTURE_DECISION_FILE_INTEGRITY_FAILURE')
        return db
    except BaseException:
        db.close()
        path.unlink(missing_ok=True)
        raise


def compile_capture(archive_dir, output_dir):
    """Validate a frozen event export and stream a derived JSON source/dataset.

    Output has no folds or cost assumptions: callers must predeclare those in
    their research manifest. Missing original receipts/feed/adjustment evidence
    remain blockers in the output provenance. No live databases are opened.
    """
    root = Path(archive_dir)
    target = Path(output_dir)
    if target.exists():
        raise FileExistsError(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix='.capture-research-', dir=target.parent))
    published = False
    decisions = None
    try:
        manifest_bytes = (root / 'manifest.json').read_bytes()
        manifest = json.loads(manifest_bytes)
        if manifest.get('version') != 2 or manifest.get('scope') != 'local_sqlite_capture_snapshot':
            raise ValueError('CAPTURE_EXPORT_VERSION_REQUIRED')
        info = manifest['files']['events.ndjson']
        decisions = _consumption_index(root, stage, manifest)
        provenance = {'input_mode': 'recorded_point_in_time',
                      'actual_receipt_timestamps_verified': False,
                      'actual_point_in_time_verified': False,
                      'historical_adjustment_as_of_verified': False}
        report = {'schema_version': 1, 'status': 'RESEARCH_NOT_ADMITTED',
                  'provider': 'Robinhood local capture archive',
                  'archive_manifest_sha256': hashlib.sha256(manifest_bytes).hexdigest(),
                  'archive_events_sha256': info['sha256'],
                  'archive_decisions_sha256': manifest['files']['decisions.ndjson']['sha256'],
                  'events': 0, 'excluded_control_diagnostics': 0, 'kinds': {},
                  'excluded_not_consumed_events': 0, 'not_consumed_reasons': {},
                  'consumption_decisions_verified': True,
                  'first_at': None, 'last_at': None,
                  'production_ready': False, 'strategy_validated': False,
                  'original_event_times_preserved': True,
                  'replay_clock': 'recorded_owner_consumed_at',
                  'coverage_complete': False, 'provenance': provenance,
                  'gaps': ['RESEARCH_FOLDS_COSTS_AND_COVERAGE_NOT_ASSEMBLED',
                           'ORIGINAL_RECEIPT_PROOF_NOT_ESTABLISHED',
                           'HISTORICAL_ADJUSTMENT_ASOF_NOT_VERIFIED',
                           'MULTIREGIME_OUT_OF_SAMPLE_NOT_VALIDATED']}
        if manifest.get('raw_responses_embedded') is not True:
            report['gaps'].append('RAW_RESPONSE_PACKAGE_NOT_EMBEDDED')
        if manifest.get('global_feed_complete') is not True:
            report['gaps'].append('GLOBAL_FEED_COVERAGE_UNVERIFIED')
        events_path = stage / 'normalized.ndjson'
        checksum = hashlib.sha256()
        size = count = 0
        previous = None
        with (root / 'events.ndjson').open('rb') as incoming, events_path.open('xb') as output:
            for line in incoming:
                checksum.update(line)
                size += len(line)
                count += 1
                row = json.loads(line)
                event = row['payload']
                if (row['capture_sequence'] != count or row['id'] != event.get('id')
                        or digest(event) != row['fingerprint']):
                    raise ValueError('CAPTURE_EVENT_INTEGRITY_FAILURE')
                at = timestamp(event['at'])
                consumed = timestamp(row['consumed_at'])
                if consumed < at:
                    raise ValueError('CONSUMPTION_BEFORE_EVENT')
                dispositions = decisions.execute('SELECT status,reason,recorded_at,seen FROM decisions WHERE event_id=?',
                                                  (event['id'],)).fetchall()
                if any(item[3] for item in dispositions):
                    raise ValueError('CAPTURE_EVENT_ID_CONFLICT')
                decisions.execute('UPDATE decisions SET seen=1 WHERE event_id=?', (event['id'],))
                if event.get('source_type') == 'local_control_diagnostic':
                    report['excluded_control_diagnostics'] += 1
                    continue
                if event.get('synthetic_input') is not False:
                    raise ValueError('REAL_CAPTURE_PROVENANCE_REQUIRED')
                if event.get('kind') not in ('universe', 'bar', 'quote', 'data_status'):
                    raise ValueError('UNSUPPORTED_CAPTURE_RESEARCH_EVENT')
                accepted = [item for item in dispositions if item[0] == 'CONSUMED']
                rejected = [item for item in dispositions if item[0] == 'NOT_CONSUMED']
                if (accepted and rejected) or len(accepted) > 1:
                    raise ValueError('CAPTURE_CONSUMPTION_DISPOSITION_CONFLICT')
                if rejected:
                    report['excluded_not_consumed_events'] += 1
                    for reason in {item[1] for item in rejected}:
                        report['not_consumed_reasons'][reason] = report['not_consumed_reasons'].get(reason, 0) + 1
                    continue
                if not accepted:
                    raise ValueError('CAPTURE_CONSUMPTION_DECISION_REQUIRED')
                if timestamp(accepted[0][2]) != consumed:
                    raise ValueError('CAPTURE_CONSUMPTION_TIME_MISMATCH')
                if previous is not None and consumed < previous:
                    raise ValueError('CAPTURE_CONSUMPTION_CLOCK_NOT_CHRONOLOGICAL')
                previous = consumed
                normalized = {k: v for k, v in event.items()
                              if k not in ('source_sha256', 'synthetic_input')}
                normalized['raw_source_sha256'] = event.get('source_sha256')
                normalized['captured_event_at'] = event['at']
                normalized['at'] = row['consumed_at']
                normalized['archive_consumed_at'] = row['consumed_at']
                normalized['archive_consumption_status'] = 'CONSUMED'
                output.write((canonical(normalized) + '\n').encode())
                report['events'] += 1
                kind = event['kind']
                report['kinds'][kind] = report['kinds'].get(kind, 0) + 1
                if report['first_at'] is None:
                    report['first_at'] = row['consumed_at']
                report['last_at'] = row['consumed_at']
            output.flush()
            os.fsync(output.fileno())
        if (checksum.hexdigest() != info['sha256'] or size != info['bytes']
                or count != info['records'] or count != manifest['event_count']):
            raise ValueError('CAPTURE_EXPORT_FILE_INTEGRITY_FAILURE')
        if decisions.execute('SELECT 1 FROM decisions WHERE seen=0 LIMIT 1').fetchone():
            raise ValueError('CAPTURE_DECISION_EVENT_MISSING')
        decisions.close(); decisions = None
        (stage / '.consumption.sqlite').unlink()
        # The source binds normalized events to their original raw hashes and
        # exported manifest, without claiming the raw responses were embedded.
        source_path = stage / 'source-normalized-events.json'
        with source_path.open('xb') as output, events_path.open() as events:
            prefix = {'provenance': provenance,
                      'capture_manifest_sha256': report['archive_manifest_sha256'],
                      'capture_events_sha256': info['sha256']}
            output.write((canonical(prefix)[:-1] + ',"events":[').encode())
            first = True
            for line in events:
                if not first:
                    output.write(b',')
                output.write(line.rstrip().encode())
                first = False
            output.write(b']}\n')
            output.flush()
            os.fsync(output.fileno())
        source_hash = _file_hash(source_path)
        dataset_path = stage / 'events.json'
        with dataset_path.open('xb') as output, events_path.open() as events:
            output.write(b'[')
            first = True
            for line in events:
                event = dict(json.loads(line), source_sha256=source_hash, synthetic_input=False)
                if not first:
                    output.write(b',')
                output.write(canonical(event).encode())
                first = False
            output.write(b']\n')
            output.flush()
            os.fsync(output.fileno())
        events_path.unlink()
        report['source'] = {'path': source_path.name, 'sha256': source_hash,
                            'provider': report['provider'], 'synthetic': False}
        report['dataset'] = {'path': dataset_path.name, 'sha256': _file_hash(dataset_path)}
        if not report['kinds'].get('universe'):
            report['gaps'].append('POINT_IN_TIME_UNIVERSE_MISSING')
        if not report['kinds'].get('bar'):
            report['gaps'].append('FIVE_MINUTE_BARS_MISSING')
        with (stage / 'compile-report.json').open('x') as output:
            output.write(json.dumps(report, indent=2) + '\n')
            output.flush()
            os.fsync(output.fileno())
        # Copy exact metadata bytes, rather than rewriting its provenance flags.
        (stage / 'capture-manifest.json').write_bytes(manifest_bytes)
        target.mkdir(mode=0o700, exist_ok=False)
        published = True
        for child in stage.iterdir():
            child.rename(target / child.name)
        return report
    except BaseException:
        if published:
            shutil.rmtree(target)
        raise
    finally:
        if decisions is not None:
            decisions.close()
        shutil.rmtree(stage, ignore_errors=True)
