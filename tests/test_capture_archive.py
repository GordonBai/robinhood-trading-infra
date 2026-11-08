from hashlib import sha256
import json
from pathlib import Path
import sqlite3
import tempfile
from unittest import TestCase

from martin_agentic.capture_archive import (archive_decision, archive_event,
    ensure_archive_schema, export_capture_archive, persist_raw_record, read_decision_context)

AT = '2026-10-20T13:30:01+00:00'
EVENT = {'id': 'event-1', 'kind': 'quote', 'at': AT,
         'data': {'symbol': 'SOFI', 'bid': '14.20'}, 'source_sha256': 'raw-hash'}


class CaptureArchiveTests(TestCase):
    def database(self, path=':memory:'):
        db = sqlite3.connect(path)
        self.addCleanup(db.close)
        ensure_archive_schema(db)
        db.execute('CREATE TABLE live_inputs(id TEXT PRIMARY KEY,fingerprint TEXT NOT NULL)')
        db.commit()
        return db

    def add_event(self, db):
        db.execute('BEGIN')
        fingerprint = archive_event(db, EVENT, consumed_at=AT)
        db.execute('INSERT INTO live_inputs VALUES(?,?)', (EVENT['id'], fingerprint))
        return fingerprint

    def test_raw_record_is_content_addressed_with_exact_observation_receipt(self):
        with tempfile.TemporaryDirectory() as folder:
            raw = b'{"timestamp":"1999-01-01T00:00:00Z"}'
            record = persist_raw_record(folder, raw, observed_at=AT, metadata={'endpoint': 'quotes'})
            self.assertEqual(record['raw_sha256'], sha256(raw).hexdigest())
            self.assertEqual(record['observed_at'], AT)
            self.assertFalse(record['original_receipt_verified'])
            self.assertEqual((Path(folder) / record['raw_path']).read_bytes(), raw)
            same = persist_raw_record(folder, raw, observed_at=AT, metadata={'endpoint': 'quotes'})
            self.assertEqual(same, record)
            later = persist_raw_record(folder, raw, observed_at='2026-10-20T13:30:02Z')
            self.assertEqual(later['raw_path'], record['raw_path'])
            self.assertNotEqual(later['receipt_path'], record['receipt_path'])
            self.assertFalse(list(Path(folder).glob('.capture-*')))

    def test_raw_existing_file_tampering_and_symlinks_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            record = persist_raw_record(folder, b'original', observed_at=AT)
            path = Path(folder) / record['raw_path']
            path.write_bytes(b'tampered')
            with self.assertRaisesRegex(ValueError, 'IMMUTABLE_FILE_CONFLICT'):
                persist_raw_record(folder, b'original', observed_at=AT)
            path.unlink()
            target = Path(folder) / 'target'; target.write_bytes(b'original')
            path.symlink_to(target)
            with self.assertRaisesRegex(ValueError, 'IMMUTABLE_FILE_CONFLICT'):
                persist_raw_record(folder, b'original', observed_at=AT)

    def test_naive_or_missing_observation_times_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            for value in (None, '2026-10-20T13:30:00', 'bad'):
                with self.subTest(value=value), self.assertRaises((ValueError, TypeError)):
                    persist_raw_record(folder, b'raw', observed_at=value)
            self.assertEqual(list(Path(folder).iterdir()), [])

    def test_archive_requires_owner_transaction(self):
        db = self.database()
        with self.assertRaisesRegex(ValueError, 'OWNER_TRANSACTION_REQUIRED'):
            archive_event(db, EVENT, consumed_at=AT)
        with self.assertRaisesRegex(ValueError, 'OWNER_TRANSACTION_REQUIRED'):
            archive_decision(db, event_id='event-1', result_id='consumed', context={}, recorded_at=AT)

    def test_event_decisions_and_owner_state_roll_back_together(self):
        db = self.database()
        self.add_event(db)
        context = {'screening': {'candidates': []}, 'before': {'state': 'WAITING'},
                   'after': {'state': 'WAITING'}, 'signal': None, 'decline': 'NO_SETUP'}
        archive_decision(db, event_id='event-1', result_id='consumed', context=context, recorded_at=AT)
        self.assertTrue(db.in_transaction)
        db.rollback()
        for table in ('live_inputs', 'live_event_archive', 'live_decision_archive'):
            self.assertEqual(db.execute('SELECT count(*) FROM ' + table).fetchone()[0], 0)

    def test_schema_creation_does_not_commit_existing_transaction(self):
        db = sqlite3.connect(':memory:'); self.addCleanup(db.close)
        db.execute('CREATE TABLE owner_state(value TEXT)')
        db.execute('INSERT INTO owner_state VALUES(?)', ('uncommitted',))
        ensure_archive_schema(db)
        self.assertTrue(db.in_transaction)
        db.rollback()
        self.assertEqual(db.execute('SELECT count(*) FROM owner_state').fetchone()[0], 0)
        self.assertFalse(db.execute("SELECT 1 FROM sqlite_master WHERE name='live_event_archive'").fetchone())

    def test_event_and_result_identity_conflicts_are_detected(self):
        db = self.database(); fingerprint = self.add_event(db)
        self.assertEqual(archive_event(db, EVENT, consumed_at='2026-10-20T13:31:00Z'), fingerprint)
        with self.assertRaisesRegex(ValueError, 'EVENT_ID_CONFLICT'):
            archive_event(db, {**EVENT, 'kind': 'bar'}, consumed_at=AT)
        first = archive_decision(db, event_id='event-1', result_id='consumed', context={'signal': None}, recorded_at=AT)
        self.assertEqual(archive_decision(db, event_id='event-1', result_id='consumed', context={'signal': None},
                                         recorded_at='2026-10-20T13:31:00Z'), first)
        with self.assertRaisesRegex(ValueError, 'DECISION_ID_CONFLICT'):
            archive_decision(db, event_id='event-1', result_id='consumed', context={'signal': 'new'}, recorded_at=AT)
        with self.assertRaisesRegex(ValueError, 'DECISION_EVENT_MISSING'):
            archive_decision(db, event_id='missing', result_id='x', context={}, recorded_at=AT)
        self.assertEqual(db.execute('SELECT consumed_at FROM live_event_archive').fetchone()[0], AT)

    def test_export_complete_payloads_hashes_and_legacy_gaps_without_source_mutation(self):
        with tempfile.TemporaryDirectory() as folder:
            source = Path(folder) / 'book.sqlite'; output = Path(folder) / 'export'
            db = self.database(source); self.add_event(db)
            context = {'screening': {'candidates': []}, 'transition': ['READY', 'DECLINED'], 'decline': 'LIMIT'}
            archive_decision(db, event_id='event-1', result_id='consumed', context=context, recorded_at=AT)
            db.execute('INSERT INTO live_inputs VALUES(?,?)', ('old-event', 'unrecoverable'))
            db.commit(); before = source.read_bytes(); changes = db.total_changes
            manifest = export_capture_archive(source, output)
            self.assertEqual(source.read_bytes(), before)
            self.assertEqual(db.total_changes, changes)
            self.assertEqual(manifest['event_count'], 1)
            self.assertEqual(manifest['decision_count'], 1)
            self.assertEqual(manifest['gap_count'], 1)
            for flag in ('provider_authenticated', 'global_feed_complete', 'actual_point_in_time_verified',
                         'original_receipt_timestamps_verified', 'production_ready', 'external_immutable_custody'):
                self.assertIs(manifest[flag], False)
            self.assertEqual(json.loads((output / 'events.ndjson').read_text())['payload'], EVENT)
            self.assertEqual(json.loads((output / 'decisions.ndjson').read_text())['context'], context)
            self.assertEqual(json.loads((output / 'gaps.ndjson').read_text())['event_id'], 'old-event')
            for name, evidence in manifest['files'].items():
                self.assertEqual(evidence['sha256'], sha256((output / name).read_bytes()).hexdigest())
            with self.assertRaises(FileExistsError): export_capture_archive(source, output)

    def test_export_detects_event_decision_or_input_tampering(self):
        for table, update in (
                ('event', "UPDATE live_event_archive SET payload='{}'"),
                ('decision', "UPDATE live_decision_archive SET payload='{}'"),
                ('input', "UPDATE live_inputs SET fingerprint='rewritten'")):
            with self.subTest(table=table), tempfile.TemporaryDirectory() as folder:
                source = Path(folder) / 'book.sqlite'; db = self.database(source); self.add_event(db)
                archive_decision(db, event_id='event-1', result_id='consumed', context={'signal': None}, recorded_at=AT)
                db.execute(update); db.commit()
                with self.assertRaisesRegex(ValueError, 'INTEGRITY_FAILURE|FINGERPRINT_CONFLICT'):
                    export_capture_archive(source, Path(folder) / 'export')
                self.assertFalse((Path(folder) / 'export').exists())

    def test_export_reads_committed_snapshot_and_reports_missing_context(self):
        with tempfile.TemporaryDirectory() as folder:
            source = Path(folder) / 'book.sqlite'; db = self.database(source)
            db.execute('PRAGMA journal_mode=WAL')
            self.add_event(db); db.commit()
            db.execute('BEGIN'); db.execute('DELETE FROM live_event_archive')
            result = export_capture_archive(source, Path(folder) / 'export')
            self.assertEqual(result['event_count'], 1)
            gaps = [json.loads(line) for line in (Path(folder) / 'export/gaps.ndjson').read_text().splitlines()]
            self.assertEqual(gaps, [{'code': 'EVENT_DECISION_CONTEXT_MISSING', 'event_id': 'event-1'}])
            db.rollback()

    def test_legacy_only_book_is_explicitly_incomplete(self):
        with tempfile.TemporaryDirectory() as folder:
            source = Path(folder) / 'book.sqlite'; db = sqlite3.connect(source); self.addCleanup(db.close)
            db.execute('CREATE TABLE live_inputs(id TEXT PRIMARY KEY,fingerprint TEXT)')
            db.execute('INSERT INTO live_inputs VALUES(?,?)', ('old', 'hash')); db.commit()
            result = export_capture_archive(source, Path(folder) / 'export')
            self.assertEqual(result['event_count'], 0)
            self.assertEqual(result['gap_count'], 3)

    def test_export_preserves_actual_insertion_order_instead_of_lexical_ids(self):
        with tempfile.TemporaryDirectory() as folder:
            source=Path(folder)/'book.sqlite';db=self.database(source)
            db.execute('BEGIN')
            for identity in ['z-first','a-second']:
                archive_event(db,{**EVENT,'id':identity},consumed_at=AT)
                archive_decision(db,event_id=identity,result_id='consumed',context={},recorded_at=AT)
            db.commit();output=Path(folder)/'export'
            export_capture_archive(source,output)
            rows=[json.loads(line) for line in (output/'events.ndjson').read_text().splitlines()]
            self.assertEqual([r['id'] for r in rows],['z-first','a-second'])
            self.assertEqual([r['capture_sequence'] for r in rows],[1,2])

    def test_export_embeds_verified_raw_receipts_and_flags_missing_source(self):
        with tempfile.TemporaryDirectory() as folder:
            source=Path(folder)/'book.sqlite';db=self.database(source)
            self.add_event(db);db.commit()
            raw=Path(folder)/'raw';persist_raw_record(raw,b'provider response',observed_at=AT)
            output=Path(folder)/'export';report=export_capture_archive(source,output,raw_dir=raw)
            self.assertTrue(report['raw_responses_embedded'])
            self.assertEqual(len(report['raw_files']),2)
            self.assertEqual(report['raw_gaps'],[{'code':'EVENT_RAW_SOURCE_MISSING','event_id':EVENT['id']}])
            self.assertFalse(report['actual_point_in_time_verified'])
            for path,entry in report['raw_files'].items():
                self.assertEqual(sha256((output/path).read_bytes()).hexdigest(),entry['sha256'])

    def test_export_refuses_tampered_raw_without_publishing_manifest(self):
        with tempfile.TemporaryDirectory() as folder:
            source=Path(folder)/'book.sqlite';db=self.database(source);db.commit()
            raw=Path(folder)/'raw';receipt=persist_raw_record(raw,b'provider response',observed_at=AT)
            (raw/receipt['raw_path']).write_bytes(b'changed')
            output=Path(folder)/'export'
            with self.assertRaisesRegex(ValueError,'RAW_INTEGRITY_FAILURE'):
                export_capture_archive(source,output,raw_dir=raw)
            self.assertFalse((output/'manifest.json').exists())

    def test_large_repeated_states_are_losslessly_stored_once(self):
        from martin_agentic.audit import canonical, digest
        db = self.database()
        state = {'indicators': {str(i): {'ema': '123.456', 'payload': 'x' * 100} for i in range(200)}}
        context = {'state_before': state, 'state_after': state, 'screening': None, 'reason': 'NO_TRIGGER'}
        db.execute('BEGIN')
        for i in range(100):
            event_id = 'event-' + str(i)
            archive_event(db, {**EVENT, 'id': event_id}, consumed_at=AT)
            fingerprint = archive_decision(db, event_id=event_id, result_id='consumed', context=context, recorded_at=AT)
            self.assertEqual(fingerprint, digest({'event_id': event_id, 'result_id': 'consumed', 'context': context}))
        db.commit()
        self.assertEqual(db.execute('SELECT count(*) FROM live_state_snapshots').fetchone()[0], 1)
        self.assertEqual(db.execute('SELECT count(*) FROM live_decision_state_refs').fetchone()[0], 200)
        stored = db.execute('SELECT sum(length(payload)) FROM live_decision_archive').fetchone()[0]
        snapshot_bytes = db.execute('SELECT sum(length(payload)) FROM live_state_snapshots').fetchone()[0]
        self.assertLess(stored + snapshot_bytes, len(canonical(context)) * 2)
        self.assertNotIn('state_before', json.loads(db.execute('SELECT payload FROM live_decision_archive LIMIT 1').fetchone()[0]))
        self.assertEqual(read_decision_context(db, 'event-99', 'consumed'), context)
        self.assertEqual(context['state_before'], state)  # Input has not been mutated.

    def test_state_snapshots_refs_and_decisions_share_owner_rollback(self):
        db = self.database(); self.add_event(db)
        archive_decision(db, event_id=EVENT['id'], result_id='consumed',
                         context={'state_before': {'value': 1}, 'state_after': {'value': 2}}, recorded_at=AT)
        self.assertEqual(db.execute('SELECT count(*) FROM live_state_snapshots').fetchone()[0], 2)
        self.assertTrue(db.in_transaction)
        db.rollback()
        for table in ('live_event_archive', 'live_decision_archive', 'live_state_snapshots', 'live_decision_state_refs'):
            self.assertEqual(db.execute('SELECT count(*) FROM ' + table).fetchone()[0], 0)

    def test_snapshot_conflict_fails_without_rewriting_existing_evidence(self):
        from martin_agentic.audit import digest
        db = self.database(); self.add_event(db)
        state = {'value': 1}; state_hash = digest(state)
        db.execute('INSERT INTO live_state_snapshots VALUES(?,?)', (state_hash, '{"value":2}'))
        db.commit(); db.execute('BEGIN')
        with self.assertRaisesRegex(ValueError, 'STATE_SNAPSHOT_INTEGRITY_FAILURE'):
            archive_decision(db, event_id=EVENT['id'], result_id='consumed', context={'state_before': state}, recorded_at=AT)
        db.rollback()
        self.assertEqual(db.execute('SELECT payload FROM live_state_snapshots').fetchone()[0], '{"value":2}')
        self.assertEqual(db.execute('SELECT count(*) FROM live_decision_archive').fetchone()[0], 0)

    def test_missing_tampered_or_orphan_state_refs_fail_closed(self):
        for mutation in (
                "UPDATE live_state_snapshots SET payload='{}'",
                'DELETE FROM live_state_snapshots',
                'DELETE FROM live_decision_state_refs',
                "UPDATE live_decision_state_refs SET fingerprint='absent'",
                "UPDATE live_decision_state_refs SET field='unknown' WHERE field='state_before'",
                "DELETE FROM live_decision_archive"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as folder:
                source = Path(folder) / 'book.sqlite'; db = self.database(source); self.add_event(db)
                archive_decision(db, event_id=EVENT['id'], result_id='consumed',
                                 context={'state_before': {'v': 1}, 'state_after': {'v': 2}}, recorded_at=AT)
                db.execute(mutation); db.commit()
                with self.assertRaisesRegex(ValueError, 'INTEGRITY_FAILURE|SNAPSHOT_MISSING'):
                    export_capture_archive(source, Path(folder) / 'export')
                self.assertFalse((Path(folder) / 'export').exists())

    def test_legacy_inline_state_and_new_refs_export_identical_logical_contexts(self):
        from martin_agentic.audit import canonical, digest
        with tempfile.TemporaryDirectory() as folder:
            source = Path(folder) / 'book.sqlite'; db = self.database(source); self.add_event(db)
            context = {'state_before': {'v': 1}, 'state_after': {'v': 2}, 'reason': 'NO_TRIGGER'}
            legacy = canonical(context)
            fingerprint = digest({'event_id': EVENT['id'], 'result_id': 'legacy', 'context': context})
            db.execute('INSERT INTO live_decision_archive VALUES(?,?,?,?,?)',
                       (EVENT['id'], 'legacy', fingerprint, legacy, AT))
            archive_decision(db, event_id=EVENT['id'], result_id='compact', context=context, recorded_at=AT)
            self.assertEqual(archive_decision(db, event_id=EVENT['id'], result_id='legacy', context=context, recorded_at=AT), fingerprint)
            db.commit()
            self.assertEqual(db.execute("SELECT payload FROM live_decision_archive WHERE result_id='legacy'").fetchone()[0], legacy)
            self.assertEqual(read_decision_context(db, EVENT['id'], 'legacy'), context)
            self.assertEqual(read_decision_context(db, EVENT['id'], 'compact'), context)
            before = source.read_bytes()
            report = export_capture_archive(source, Path(folder) / 'export')
            rows = [json.loads(line) for line in (Path(folder) / 'export/decisions.ndjson').read_text().splitlines()]
            self.assertEqual([r['context'] for r in rows], [context, context])
            self.assertEqual(report['state_snapshot_count'], 2)
            self.assertEqual(source.read_bytes(), before)
            second = export_capture_archive(source, Path(folder) / 'export-again')
            for name in report['files']:
                self.assertEqual(report['files'][name], second['files'][name])

    def test_export_hydration_memory_is_bounded_by_record_not_whole_dataset(self):
        import tracemalloc
        with tempfile.TemporaryDirectory() as folder:
            source = Path(folder) / 'book.sqlite'; db = self.database(source); db.execute('BEGIN')
            state = {'large': 'x' * 65536}
            for i in range(100):
                event_id = 'large-' + str(i)
                archive_event(db, {**EVENT, 'id': event_id}, consumed_at=AT)
                archive_decision(db, event_id=event_id, result_id='consumed',
                                 context={'state_before': state, 'state_after': state}, recorded_at=AT)
            db.commit()
            if tracemalloc.is_tracing():
                self.skipTest('A caller already owns tracemalloc')
            tracemalloc.start()
            try:
                report = export_capture_archive(source, Path(folder) / 'export')
                peak = tracemalloc.get_traced_memory()[1]
            finally:
                tracemalloc.stop()
            self.assertGreater(report['files']['decisions.ndjson']['bytes'], 12_000_000)
            self.assertLess(peak, 5_000_000)
            self.assertEqual(report['decision_count'], 100)

    def test_raw_metadata_previews_are_capped_with_full_streamed_gap_evidence(self):
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as folder:
            source = Path(folder) / 'book.sqlite'; db = self.database(source); db.execute('BEGIN')
            for i in range(5): archive_event(db, {**EVENT, 'id': str(i)}, consumed_at=AT)
            db.commit(); raw = Path(folder) / 'raw'; raw.mkdir()
            with patch('martin_agentic.capture_archive.PREVIEW_LIMIT', 2):
                report = export_capture_archive(source, Path(folder) / 'export', raw_dir=raw)
            self.assertEqual(len(report['raw_gaps']), 2)
            self.assertTrue(report['raw_gaps_truncated'])
            self.assertEqual(report['raw_gap_count'], 5)
            self.assertEqual(len((Path(folder) / 'export/raw-gaps.ndjson').read_text().splitlines()), 5)

    def test_private_snapshot_allows_live_rollback_journal_writer_during_slow_export(self):
        import threading
        from unittest.mock import patch
        from martin_agentic import capture_archive
        with tempfile.TemporaryDirectory() as folder:
            source = Path(folder) / 'book.sqlite'; db = self.database(source); self.add_event(db); db.commit()
            self.assertEqual(db.execute('PRAGMA journal_mode').fetchone()[0], 'delete')
            start_writer = threading.Event(); writer_done = threading.Event(); errors = []
            def write():
                try:
                    if not start_writer.wait(5): raise RuntimeError('Export did not begin streaming')
                    writer = sqlite3.connect(source, timeout=.2)
                    try:
                        writer.execute('INSERT INTO live_inputs VALUES(?,?)', ('concurrent', 'later'))
                        writer.commit()
                    finally:
                        writer.close()
                except BaseException as exc: errors.append(exc)
                finally: writer_done.set()
            thread = threading.Thread(target=write); thread.start()
            original = capture_archive._stream_rows
            def slow_stream(path, rows):
                if Path(path).name == 'events.ndjson':
                    start_writer.set()
                    self.assertTrue(writer_done.wait(2), 'Live writer blocked by export snapshot')
                    self.assertFalse(errors)
                return original(path, rows)
            try:
                with patch('martin_agentic.capture_archive._stream_rows', side_effect=slow_stream):
                    result = export_capture_archive(source, Path(folder) / 'export')
            finally:
                start_writer.set(); thread.join(timeout=5)
            self.assertFalse(thread.is_alive()); self.assertFalse(errors)
            self.assertEqual(db.execute('SELECT count(*) FROM live_inputs').fetchone()[0], 2)
            self.assertEqual(result['gap_count'], 1)  # Only pre-backup event lacks a decision.
            self.assertFalse(result['snapshot']['source_transaction_held_during_export'])
            self.assertEqual(result['snapshot']['backup_step_pages'], 256)
            self.assertFalse(list((Path(folder) / 'export').glob('*.sqlite*')))

    def test_backup_deadline_failure_cleans_private_copy_without_publication(self):
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as folder:
            source = Path(folder) / 'book.sqlite'; db = self.database(source); db.commit()
            with patch('martin_agentic.capture_archive.time.monotonic', side_effect=[0, 31]):
                with self.assertRaisesRegex(TimeoutError, 'SNAPSHOT_BACKUP_TIMEOUT'):
                    export_capture_archive(source, Path(folder) / 'export', backup_timeout_seconds=30)
            self.assertFalse((Path(folder) / 'export').exists())
            self.assertFalse(list(Path(folder).glob('.capture-export-*')))
            for invalid in (0, -1, float('inf'), True):
                with self.subTest(invalid=invalid), self.assertRaisesRegex(ValueError, 'BACKUP_TIMEOUT_REQUIRED'):
                    export_capture_archive(source, Path(folder) / 'export', backup_timeout_seconds=invalid)
