import contextlib
import gzip
import hashlib
import io
import json
from pathlib import Path
import shutil
import sqlite3
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import zipfile

from insider_pipeline.audit_batches import file_hash, readonly, run as audit
from insider_pipeline.baseline import build as build_baseline
from insider_pipeline.discovery import discover
from insider_pipeline.discovery_refresh import (MANIFEST, index_url, materialize, prepare,
                                                requested_quarters, source_files)
from insider_pipeline.http import atomic_write
from insider_pipeline.increment import build as build_increment, restore as restore_increment
from insider_pipeline.inventory import canonical, connect
from insider_pipeline.inventory_archive import archive, capture_snapshot
from insider_pipeline.inventory_delta import schema, state
from insider_pipeline.locking import writer_lock
from insider_pipeline.package import package
from insider_pipeline.runner import Shards, finish, prepare as prepare_document, run as collect
from test_parser import ACCESSION, FOOTNOTES, filing, transaction

REMOVED = '0001234567-26-000002'
LEGACY = '0001234567-26-000003'
PENDING = '0001234567-26-000004'
NEW = '0001234567-26-000005'


def source_body(rows):
    return ('CIK|Company Name|Form Type|Date Filed|Filename\n' + ''.join(
        f'{cik}|Fixture|{form}|{filed}|edgar/data/{cik}/{accession}.txt\n'
        for accession, form, filed, cik in rows)).encode()


class SourceClient:
    def __init__(self, bodies, *, delay=0):
        self.bodies, self.calls = bodies, []
        self.delay = delay
        self.lock = threading.Lock()

    def cached(self, url, directory, refresh=False):
        with self.lock:
            self.calls.append((url, refresh))
        time.sleep(self.delay)
        body = self.bodies[url]
        meta = {'url': url, 'bytes': len(body), 'sha256': hashlib.sha256(body).hexdigest(),
                'retrieved_at_utc': '2026-09-09T00:00:00Z'}
        stem = hashlib.sha256(url.encode()).hexdigest()
        atomic_write(Path(directory) / (stem + '.body'), body)
        atomic_write(Path(directory) / (stem + '.json'), canonical(meta))
        return body, meta


class DiscoveryRefreshTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)
        self.root = self.base / 'parent'
        db = connect(self.root)
        db.executemany('INSERT INTO settings VALUES(?,?)',
                       [('window_start', '2026-01-01'), ('window_end', '2026-09-03'), ('schema_version', '1')])
        db.commit(); db.close()
        self.initial = {
            index_url('2026Q1'): source_body([(LEGACY, '4', '2026-01-03', 64040)]),
            index_url('2026Q2'): source_body([(PENDING, '4', '2026-06-03', 64040)]),
            index_url('2026Q3'): source_body([(ACCESSION, '4', '2026-09-03', 64040),
                                             (REMOVED, '4', '2026-09-03', 64040)])}
        with contextlib.redirect_stdout(io.StringIO()):
            discover(self.root, SourceClient(self.initial), workers=2)
        db = connect(self.root)
        shards = Shards(self.root, db)
        for accession in (ACCESSION, REMOVED, LEGACY):
            row = dict(db.execute('SELECT * FROM filings WHERE accession=?', (accession,)).fetchone())
            record = prepare_document(self.document(accession), row)
            self.assertEqual(record['status'], 'verified')
            relative = shards.choose(row, record)
            shards.save(relative, record); finish(db, row, record, relative)
        shards.close()
        db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        db.execute('PRAGMA journal_mode=DELETE')
        db.close()
        self.inventory = self.root / 'inventory.sqlite3'
        self.changed = {
            index_url('2026Q2'): source_body([(PENDING, '4', '2026-06-03', 42)]),
            index_url('2026Q3'): source_body([(ACCESSION, '4', '2026-09-04', 64040),
                                             (NEW, '4', '2026-09-04', 64040)])}

    @staticmethod
    def document(accession):
        xml = filing('<nonDerivativeTable>' + transaction() + '</nonDerivativeTable>' + FOOTNOTES)
        return b'ACCESSION NUMBER: ' + accession.encode() + b'\n<DOCUMENT>\n<TYPE>4\n<XML>\n' + xml + b'\n</XML>\n</DOCUMENT>'

    def plan(self, name='plan', *, bodies=None, **kwargs):
        client = SourceClient(self.changed if bodies is None else bodies)
        result = prepare(self.inventory, self.base / name, '2026-09-04', client=client, **kwargs)
        return self.base / name, result, client

    def rows(self, path):
        db = readonly(path)
        try:
            return {table: [tuple(row) for row in db.execute('SELECT * FROM ' + table + ' ORDER BY ' +
                    ','.join(shape['columns'][table]))] for shape in [schema(db)] for table in shape['columns']}
        finally:
            db.close()

    def record(self, root, accession):
        db = readonly(root / 'inventory.sqlite3')
        try:
            row = dict(db.execute('SELECT * FROM filings WHERE accession=?', (accession,)).fetchone())
            shards = Shards(root, db)
            try:
                return row, shards.existing(row)
            finally:
                shards.close()
        finally:
            db.close()

    def repin(self, directory, **changes):
        manifest = json.loads((directory / MANIFEST).read_text())
        manifest.update(changes)
        (directory / MANIFEST).write_bytes(canonical(manifest))
        return file_hash(directory / MANIFEST)

    def test_selected_refresh_extends_cutoff_preserves_history_and_reports_all_changed_originals(self):
        before = self.rows(self.inventory); parent_hash = file_hash(self.inventory)
        directory, result, client = self.plan()
        self.assertEqual(sorted(client.calls), [(index_url('2026Q2'), True), (index_url('2026Q3'), True)])
        self.assertEqual(result['committed_documents_to_restore'], 2)
        self.assertEqual(result['new_filings'], 1)
        self.assertEqual(json.loads((directory / 'committed-accessions.json').read_text()), [ACCESSION, REMOVED])
        db = readonly(directory / 'inventory.sqlite3')
        try:
            rows = {row['accession']: dict(row) for row in db.execute('SELECT * FROM filings')}
            self.assertEqual(set(rows), {ACCESSION, REMOVED, LEGACY, PENDING, NEW})
            self.assertEqual(rows[ACCESSION]['filing_date'], '2026-09-03')
            self.assertEqual(rows[NEW]['filing_date'], '2026-09-04')
            self.assertEqual(rows[NEW]['status'], 'pending')
            self.assertIn('/data/42/', rows[PENDING]['source_url'])
            self.assertIn('BULK_INDEX_IDENTITY_DISAGREEMENT:2026Q3:4:2026-09-04', rows[ACCESSION]['discovery_issues'])
            self.assertIn('INDEX_MEMBERSHIP_REMOVED:2026Q3', rows[REMOVED]['discovery_issues'])
            self.assertEqual(db.execute('SELECT filing_date FROM index_observations WHERE accession=?', (ACCESSION,)).fetchone()[0], '2026-09-04')
            history = [json.loads(row[0]) for row in db.execute("SELECT value FROM settings WHERE key LIKE 'index_observation_history:%'")]
            self.assertEqual(len(history), 3)
            self.assertEqual({row['observation']['accession'] for row in history}, {ACCESSION, REMOVED, PENDING})
            self.assertEqual({row['observation']['filing_date'] for row in history}, {'2026-09-03', '2026-06-03'})
            self.assertEqual(db.execute("SELECT value FROM settings WHERE key='window_end'").fetchone()[0], '2026-09-04')
            self.assertFalse(json.loads((directory / MANIFEST).read_text())['current_cutoff_completeness_verified'])
        finally:
            db.close()
        self.assertEqual(file_hash(self.inventory), parent_hash)
        self.assertEqual(self.rows(self.inventory), before)
        self.assertFalse((directory / 'shards').exists())
        self.assertFalse(source_files(directory / 'sources/indexes', '2026Q1')[0].exists())

    def test_materialization_retains_exact_blobs_in_new_shards_with_unselected_history_absent(self):
        directory, result, _ = self.plan()
        before = self.rows(self.inventory)
        originals = {accession: self.record(self.root, accession) for accession in (ACCESSION, REMOVED)}
        legacy_row, _ = self.record(self.root, LEGACY)
        (self.root / legacy_row['shard']).unlink()
        original_files = {path: file_hash(path) for path in (self.root / 'shards').glob('*.sqlite3')}
        destination = self.base / 'candidate'
        report = materialize(directory, result['plan_sha256'], self.root, destination)
        self.assertEqual(report['rechecked_committed_documents'], 2)
        self.assertEqual(report['rechecked_documents_in_review'], 2)
        self.assertTrue(report['parent_inventory_unchanged'])
        self.assertFalse(report['source_audit_performed'])
        self.assertFalse((destination / legacy_row['shard']).exists())
        for accession, (old_row, old) in originals.items():
            new_row, new = self.record(destination, accession)
            for key in ('source_gzip', 'source_sha256', 'source_bytes', 'parsed_gzip', 'parsed_sha256', 'parsed_bytes', 'fetched_at'):
                self.assertEqual(old[key], new[key], key)
            self.assertEqual(new['status'], 'review')
            self.assertNotEqual(new_row['shard'], old_row['shard'])
            self.assertTrue(new_row['shard'].endswith('-0002.sqlite3'))
            self.assertEqual(json.loads(gzip.decompress(new['parsed_gzip'])), json.loads(gzip.decompress(old['parsed_gzip'])))
        self.assertEqual(self.rows(self.inventory), before)
        self.assertEqual({path: file_hash(path) for path in original_files}, original_files)

    def test_serial_and_parallel_preparation_are_byte_and_row_equivalent(self):
        directory1, result1, _ = self.plan('serial', workers=1)
        directory4, result4, _ = self.plan('parallel', workers=4)
        self.assertEqual(result1, result4)
        for path in directory1.rglob('*'):
            if path.is_file():
                self.assertEqual(path.read_bytes(), (directory4 / path.relative_to(directory1)).read_bytes(), path.name)
        self.assertEqual(self.rows(directory1 / 'inventory.sqlite3'), self.rows(directory4 / 'inventory.sqlite3'))

    def test_repeated_observation_preserves_review_without_loading_originals_again(self):
        directory, result, _ = self.plan()
        candidate = self.base / 'candidate'
        materialize(directory, result['plan_sha256'], self.root, candidate)
        prior = readonly(candidate / 'inventory.sqlite3')
        try:
            history = [tuple(row) for row in prior.execute(
                "SELECT * FROM settings WHERE key LIKE 'index_observation_history:%' ORDER BY key")]
        finally:
            prior.close()
        next_plan = self.base / 'next-plan'
        next_result = prepare(candidate / 'inventory.sqlite3', next_plan, '2026-09-05',
                              client=SourceClient(self.changed), workers=2)
        self.assertEqual(next_result['committed_documents_to_restore'], 0)
        shutil.rmtree(candidate / 'shards')
        next_root = self.base / 'next-candidate'
        report = materialize(next_plan, next_result['plan_sha256'], candidate, next_root)
        self.assertEqual(report['rechecked_committed_documents'], 0)
        db = readonly(next_root / 'inventory.sqlite3')
        try:
            self.assertEqual([tuple(row) for row in db.execute(
                "SELECT * FROM settings WHERE key LIKE 'index_observation_history:%' ORDER BY key")], history)
            self.assertEqual(db.execute("SELECT count(*) FROM filings WHERE status='review'").fetchone()[0], 2)
        finally:
            db.close()

    def test_unchanged_sources_do_not_reopen_resolved_identity_findings(self):
        db = sqlite3.connect(self.inventory)
        db.execute('UPDATE filings SET filing_date=? WHERE accession=?', ('2026-09-02', ACCESSION))
        db.commit(); db.close()
        directory, result, _ = self.plan(bodies=self.initial)
        self.assertEqual(result['committed_documents_to_restore'], 0)
        candidate = self.base / 'candidate'
        shutil.rmtree(self.root / 'shards')
        report = materialize(directory, result['plan_sha256'], self.root, candidate)
        self.assertEqual(report['rechecked_committed_documents'], 0)
        self.assertFalse((candidate / 'shards').exists())
        row = readonly(candidate / 'inventory.sqlite3')
        try:
            self.assertEqual(row.execute('SELECT discovery_issues FROM filings WHERE accession=?', (ACCESSION,)).fetchone()[0], '[]')
        finally:
            row.close()

    def test_new_empty_quarter_can_be_recorded_without_erasing_populated_history(self):
        unchanged = self.initial.copy()
        unchanged[index_url('2026Q4')] = b'CIK|Company Name|Form Type|Date Filed|Filename\n'
        client = SourceClient(unchanged)
        result = prepare(self.inventory, self.base / 'quarter-roll', '2026-10-01', client=client)
        self.assertEqual(result['source_quarters'], ['2026Q3', '2026Q4'])
        db = readonly(self.base / 'quarter-roll/inventory.sqlite3')
        try:
            self.assertEqual(db.execute("SELECT filing_count FROM index_sources WHERE source_key='2026Q4'").fetchone()[0], 0)
            self.assertEqual(db.execute('SELECT count(*) FROM filings').fetchone()[0], 4)
        finally:
            db.close()
        with self.assertRaisesRegex(ValueError, 'became empty'):
            self.plan('empty-old', bodies={**self.changed, index_url('2026Q3'): unchanged[index_url('2026Q4')]})
        self.assertFalse((self.base / 'empty-old').exists())

    def test_missing_coverage_invalid_cutoff_and_omitted_crossed_quarters_fail_before_fetch(self):
        scope = {'window_start': '2026-01-01', 'window_end': '2026-09-03'}
        for end, selected in [('2026-09-02', None), ('2026-9-04', None), ('2026-13-01', None),
                              ('2026-10-01', '2026Q4'), ('2026-09-04', '2025Q4 2026Q3')]:
            with self.assertRaises(ValueError):
                requested_quarters(scope, end, selected)
        db = sqlite3.connect(self.inventory)
        db.execute("DELETE FROM index_membership WHERE source_key='2026Q1'")
        db.commit(); db.close()
        client = SourceClient(self.changed)
        with self.assertRaisesRegex(ValueError, 'membership differs'):
            prepare(self.inventory, self.base / 'bad', '2026-09-04', client=client)
        self.assertEqual(client.calls, [])

    def test_corrupt_cached_sources_and_failed_fetches_leave_parent_and_destination_untouched(self):
        before = file_hash(self.inventory)
        cache = self.root / 'sources/indexes'
        path, _ = source_files(cache, '2026Q3')
        path.write_bytes(b'corrupt')
        with self.assertRaisesRegex(ValueError, 'bytes or metadata'):
            prepare(self.inventory, self.base / 'bad-cache', '2026-09-04', cache=cache)
        with self.assertRaises(KeyError):
            self.plan('failed-fetch', bodies={})
        self.assertFalse((self.base / 'bad-cache').exists())
        self.assertFalse((self.base / 'failed-fetch').exists())
        self.assertEqual(file_hash(self.inventory), before)

    def test_missing_changed_original_stops_without_creating_a_candidate(self):
        directory, result, _ = self.plan()
        row, _ = self.record(self.root, ACCESSION)
        (self.root / row['shard']).unlink()
        before = file_hash(self.inventory)
        with self.assertRaisesRegex(ValueError, 'must be restored'):
            materialize(directory, result['plan_sha256'], self.root, self.base / 'missing')
        self.assertFalse((self.base / 'missing').exists())
        self.assertEqual(file_hash(self.inventory), before)

    def test_missing_required_bulk_source_stops_before_materialization(self):
        db = sqlite3.connect(self.inventory)
        db.execute('INSERT INTO sources VALUES(?,?,?,?,?,?,?)',
                   ('2026Q3', 'https://www.sec.gov/files/2026q3_form345.zip', '0' * 64,
                    'sources/quarterly/2026Q3.zip', 4, '2026-09-09', 1))
        db.execute('UPDATE filings SET bulk_source=? WHERE accession=?', ('2026Q3', ACCESSION))
        db.commit(); db.close()
        directory, result, _ = self.plan()
        self.assertEqual(result['required_bulk_quarters'], ['2026Q3'])
        with self.assertRaisesRegex(ValueError, 'bulk source must be restored'):
            materialize(directory, result['plan_sha256'], self.root, self.base / 'missing-bulk')
        self.assertFalse((self.base / 'missing-bulk').exists())

    def test_required_bulk_source_is_checked_copied_and_cannot_be_omitted_from_plan(self):
        source = self.root / 'sources/quarterly/2026Q3.zip'
        source.parent.mkdir(parents=True)
        with zipfile.ZipFile(source, 'w') as archive_file:
            archive_file.writestr('SUBMISSION.tsv', 'ACCESSION_NUMBER\tISSUERCIK\n' + ACCESSION + '\t64040\n')
        db = sqlite3.connect(self.inventory)
        db.execute('INSERT INTO sources VALUES(?,?,?,?,?,?,?)',
                   ('2026Q3', 'https://www.sec.gov/files/2026q3_form345.zip', file_hash(source),
                    'sources/quarterly/2026Q3.zip', source.stat().st_size, '2026-09-09', 1))
        db.execute('UPDATE filings SET bulk_source=? WHERE accession=?', ('2026Q3', ACCESSION))
        db.commit(); db.close()
        directory, result, _ = self.plan()
        destination = self.base / 'with-bulk'
        materialize(directory, result['plan_sha256'], self.root, destination)
        self.assertEqual((destination / source.relative_to(self.root)).read_bytes(), source.read_bytes())
        pin = self.repin(directory, required_bulk_quarters=[])
        with self.assertRaisesRegex(ValueError, 'omitted or added a required original bulk'):
            materialize(directory, pin, self.root, self.base / 'omitted-bulk')

    def test_target_cutoff_and_source_catalog_are_checked_even_after_repinning(self):
        directory, _, _ = self.plan()
        target_path = directory / 'inventory.sqlite3'
        db = sqlite3.connect(target_path)
        db.execute("UPDATE settings SET value='2026-09-05' WHERE key='window_end'")
        db.commit(); db.close()
        db = readonly(target_path)
        target_state = state(db, schema(db)); db.close()
        pin = self.repin(directory, target_inventory_state=target_state,
                         target_inventory_file_sha256=file_hash(target_path))
        with self.assertRaisesRegex(ValueError, 'target cutoff differs'):
            materialize(directory, pin, self.root, self.base / 'wrong-cutoff')
        db = sqlite3.connect(target_path)
        db.execute("UPDATE settings SET value='2026-09-04' WHERE key='window_end'")
        db.execute("UPDATE index_sources SET retrieved_at='2026-09-10T00:00:00Z' WHERE source_key='2026Q3'")
        db.commit(); db.close()
        db = readonly(target_path)
        target_state = state(db, schema(db)); db.close()
        pin = self.repin(directory, target_inventory_state=target_state,
                         target_inventory_file_sha256=file_hash(target_path))
        with self.assertRaisesRegex(ValueError, 'source catalog differs'):
            materialize(directory, pin, self.root, self.base / 'wrong-source')

    def test_malformed_ownership_rows_are_not_silently_treated_as_index_removals(self):
        malformed = [
            '64040|Fixture|4|2026-09-04\n',
            'x|Fixture|4|2026-09-04|edgar/data/64040/' + NEW + '.txt\n',
            '64040|Fixture|4|20260904|edgar/data/64040/' + NEW + '.txt\n',
            '64040|Fixture|4|2026-09-99|edgar/data/64040/' + NEW + '.txt\n',
        ]
        before = file_hash(self.inventory)
        for number, row in enumerate(malformed):
            with self.subTest(row=row):
                with self.assertRaises(ValueError):
                    self.plan('bad-row-' + str(number),
                              bodies={**self.changed, index_url('2026Q3'): self.changed[index_url('2026Q3')] + row.encode()})
                self.assertFalse((self.base / ('bad-row-' + str(number))).exists())
        self.assertEqual(file_hash(self.inventory), before)

    def test_omitted_committed_selection_is_rejected_even_when_re_pinned(self):
        directory, _, _ = self.plan()
        body = canonical([ACCESSION])
        (directory / 'committed-accessions.json').write_bytes(body)
        pin = self.repin(directory, committed_documents=1, committed_accessions_sha256=hashlib.sha256(body).hexdigest())
        with self.assertRaisesRegex(ValueError, 'omitted or added'):
            materialize(directory, pin, self.root, self.base / 'omitted')
        self.assertFalse((self.base / 'omitted').exists())

    def test_wrong_parent_pin_corrupt_plan_and_active_writer_cannot_mutate_state(self):
        directory, result, _ = self.plan()
        with self.assertRaisesRegex(ValueError, 'plan pin'):
            materialize(directory, '0' * 64, self.root, self.base / 'bad-pin')
        with writer_lock(self.root):
            with self.assertRaisesRegex(RuntimeError, 'Another backfill writer'):
                materialize(directory, result['plan_sha256'], self.root, self.base / 'busy')
        db = sqlite3.connect(self.inventory)
        db.execute('UPDATE filings SET attempts=7 WHERE accession=?', (PENDING,))
        db.commit(); db.close()
        with self.assertRaisesRegex(ValueError, 'Restored parent differs'):
            materialize(directory, result['plan_sha256'], self.root, self.base / 'wrong-parent')
        source_files(directory / 'sources/indexes', '2026Q3')[0].write_bytes(b'corrupt')
        with self.assertRaisesRegex(ValueError, 'source checksum'):
            materialize(directory, result['plan_sha256'], self.root, self.base / 'bad-source')

    def test_inventory_mutation_during_preparation_is_detected(self):
        original = SourceClient.cached

        def change(client, url, directory, refresh=False):
            value = original(client, url, directory, refresh)
            db = sqlite3.connect(self.inventory)
            db.execute('UPDATE filings SET attempts=attempts+1 WHERE accession=?', (PENDING,))
            db.commit(); db.close()
            return value

        with patch.object(SourceClient, 'cached', change):
            with self.assertRaisesRegex(ValueError, 'Frozen parent inventory changed'):
                self.plan('mutated', workers=1)
        self.assertFalse((self.base / 'mutated').exists())

    def test_refresh_collect_increment_and_full_restore_match_every_row_and_document(self):
        with contextlib.redirect_stdout(io.StringIO()):
            archive(self.root, self.base / 'inventory-archive')
            snapshot = self.base / 'inventory-archive/inventory-snapshot.sqlite3'
            audit(self.root, self.base / 'parent-audit', workers=1, inventory_snapshot=snapshot)
            package(self.root, self.base / 'parent-audit', self.base / 'parent-documents')
            baseline = build_baseline(self.base / 'inventory-archive', self.base / 'parent-documents', self.base / 'baseline')
        # The baseline inventory is pinned byte for byte; use its snapshot as the
        # refresh parent so the subsequent increment checks the same checkpoint.
        self.inventory = snapshot
        directory, result, _ = self.plan()
        parent_copy = self.base / 'partial-parent'
        shutil.copytree(self.root, parent_copy)
        shutil.copyfile(snapshot, parent_copy / 'inventory.sqlite3')
        legacy_row, _ = self.record(parent_copy, LEGACY)
        (parent_copy / legacy_row['shard']).unlink()
        candidate = self.base / 'candidate'
        materialize(directory, result['plan_sha256'], parent_copy, candidate)
        document = self.document(NEW)

        class Client:
            requests = download_bytes = 0
            blocked = threading.Event()

            def get(inner, url):
                self.assertTrue(url.endswith(NEW + '.txt'))
                inner.requests += 1
                inner.download_bytes += len(document)
                return document

        client = Client()
        with contextlib.redirect_stdout(io.StringIO()):
            collected = collect(candidate, client, workers=1, start='2026-09-04', end='2026-09-04')
            capture_snapshot(candidate, self.base / 'target.sqlite3')
            increment = build_increment(candidate, self.base / 'baseline/baseline.json', baseline['baseline_sha256'],
                                        snapshot, self.base / 'target.sqlite3', self.base / 'increment', workers=1)
            restored = restore_increment(self.base / 'increment', self.base / 'baseline/baseline.json',
                                         self.base / 'restored', increment['increment_manifest_sha256'])
        self.assertEqual(client.requests, 1)
        self.assertEqual(collected['completed_this_run'], 1)
        self.assertEqual(increment['changed_committed_documents'], 3)
        self.assertEqual(restored['restored_documents'], 4)
        self.assertEqual(self.rows(self.base / 'target.sqlite3'), self.rows(self.base / 'restored/inventory.sqlite3'))
        for accession in (ACCESSION, REMOVED, LEGACY, NEW):
            reference = self.root if accession == LEGACY else candidate
            self.assertEqual(self.record(reference, accession), self.record(self.base / 'restored', accession))
