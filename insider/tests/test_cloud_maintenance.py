from collections import Counter
import contextlib
from datetime import datetime, timezone
import gzip
import hashlib
import io
import json
from pathlib import Path
import signal
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch

from insider_pipeline import cloud_maintenance as cloud, github_documents, runner
from insider_pipeline.audit_batches import file_hash, readonly, run as audit
from insider_pipeline.inventory import canonical, connect
from insider_pipeline.maintenance_refresh import CATALOG_URL
from test_bulk_refresh import quarter_body
import test_discovery_refresh as fixtures
import test_github_session as session_fixtures
from test_maintenance_refresh import catalog


class SEC(fixtures.SourceClient):
    def __init__(self, bodies, *, fail=False, malformed=False):
        super().__init__(bodies)
        self.requests = self.download_bytes = 0
        self.blocked = threading.Event()
        self.fail, self.malformed, self.documents = fail, malformed, []

    def cached(self, url, directory, refresh=False):
        body, meta = super().cached(url, directory, refresh)
        with self.lock:
            self.requests += 1; self.download_bytes += len(body)
        return body, meta

    def get(self, url):
        accession = url.rsplit('/', 1)[1].removesuffix('.txt')
        with self.lock:
            self.requests += 1; self.documents.append(accession)
        if self.fail:
            raise ConnectionError('synthetic unavailable source')
        body = b'not original XML' if self.malformed else fixtures.DiscoveryRefreshTests.document(accession)
        with self.lock:
            self.download_bytes += len(body)
        return body


class CloudMaintenanceTests(unittest.TestCase):
    def setUp(self):
        self.fixture = session_fixtures.GitHubSessionRefreshTests(); self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.base, self.github = self.fixture.base, self.fixture.github
        from insider_pipeline.bulk_refresh import bulk_url
        self.urls = {key: bulk_url(key) for key in ('2026Q2', '2026Q3')}
        self.bodies = {**self.fixture.fixture.changed, CATALOG_URL: catalog(self.urls),
                       self.urls['2026Q2']: quarter_body([(fixtures.PENDING, 64040, '2026-06-03', '4')]),
                       self.urls['2026Q3']: quarter_body([(fixtures.ACCESSION, 64040, '2026-09-03', '4'),
                                                         (fixtures.REMOVED, 64040, '2026-09-03', '4')])}

    def prepare(self, name='cloud', **kwargs):
        client = kwargs.pop('client', SEC(self.bodies))
        with self.github.patches(), contextlib.redirect_stdout(io.StringIO()):
            result = cloud.prepare(self.base / name, self.fixture.bucket, self.fixture.staged['transport_sha256'],
                                   self.fixture.published['document_index_sha256'], client=client,
                                   through='2026-09-04', max_filings=1, workers=2, **kwargs)
        return self.base / name, result, client

    def repin(self, work, **changes):
        path = work / cloud.MANIFEST; value = json.loads(path.read_text()); value.update(changes)
        path.write_bytes(canonical(value)); return file_hash(path)

    def test_complete_cloud_sequence_downloads_once_publishes_and_restores_all_originals(self):
        work, prepared, client = self.prepare()
        self.assertEqual(client.documents, [fixtures.PENDING])
        self.assertEqual(prepared['collection']['attempted_this_run'], 1)
        self.assertEqual(prepared['collection']['http_requests_this_run'], 1)
        self.assertEqual(prepared['collection']['completed_this_run'], 1)
        self.assertEqual(prepared['changed_documents_archived'], 3)
        self.assertEqual(prepared['indexed_documents'], 4)
        self.assertGreater(prepared['original_source_checks'], 0)
        self.assertEqual(prepared['archive_recovery']['inventory_archive_replay_count'], 1)
        from test_github_documents import GitHubDocumentTests
        calls = Counter(GitHubDocumentTests().downloads(self.github)); self.assertTrue(all(count == 1 for count in calls.values()))
        self.assertFalse(any(kind == 'command' and args[:2] != ['release', 'download'] for kind, args in self.github.calls))
        self.github.push = True
        before = {tag: dict(values) for tag, values in self.github.bodies.items()}
        with self.github.patches(), contextlib.redirect_stdout(io.StringIO()):
            published = cloud.publish(work, prepared['prepared_manifest_sha256'], self.fixture.bucket)
        self.assertTrue(published['maintenance_checkpoint_archived'])
        self.assertEqual(published['changed_documents_archived'], 3)
        self.assertTrue(published['latest_release_unchanged'])
        self.assertFalse(published['bucket_draft'] or published['complete_backfill'] or published['cloud_daily_maintenance_active'])
        for tag, values in before.items():
            for name, body in values.items():
                self.assertEqual(self.github.bodies[tag][name], body)
        from insider_pipeline.increment import restore
        from test_increment import IncrementTests
        with contextlib.redirect_stdout(io.StringIO()):
            restored = restore(work / 'increment', self.base / 'increment/increment.json', self.base / 'restored-new',
                               prepared['increment_manifest_sha256'], ancestor_manifests=[self.base / 'baseline/baseline.json'])
            checked = audit(self.base / 'restored-new', self.base / 'full-audit-new', workers=2)
        self.assertEqual(restored['restored_documents'], 4)
        IncrementTests().assert_inventory_equal(work / 'target-inventory.sqlite3', self.base / 'restored-new/inventory.sqlite3')
        for accession in (fixtures.ACCESSION, fixtures.REMOVED, fixtures.LEGACY, fixtures.PENDING):
            expected = self.fixture.root if accession == fixtures.LEGACY else work / 'candidate'
            self.assertEqual(self.fixture.fixture.record(expected, accession), self.fixture.fixture.record(self.base / 'restored-new', accession))
        self.assertEqual(checked['selected_documents'], 4)
        self.assertEqual(checked['counts'].get('document_failure', 0), 0)
        self.assertEqual(checked['counts'].get('original_xml_field_failure', 0), 0)
        self.assertNotIn(fixtures.ACCESSION, json.dumps(published))
        # Reusing the exact prepared run does not replace or duplicate assets.
        uploaded = sum(len(values) for values in self.github.bodies.values())
        with self.github.patches(), contextlib.redirect_stdout(io.StringIO()):
            repeated = cloud.publish(work, prepared['prepared_manifest_sha256'], self.fixture.bucket)
        self.assertEqual(repeated['increment_assets_uploaded'] + repeated['index_assets_uploaded'], 0)
        self.assertEqual(sum(len(values) for values in self.github.bodies.values()), uploaded)

    def test_no_changed_originals_still_loads_complete_index_without_historical_chunks(self):
        work, first, _ = self.prepare('first')
        self.github.push = True
        with self.github.patches(), contextlib.redirect_stdout(io.StringIO()):
            published = cloud.publish(work, first['prepared_manifest_sha256'], self.fixture.bucket)
        self.github.push = False; self.github.calls.clear()
        client = SEC(self.bodies)
        with self.github.patches(), contextlib.redirect_stdout(io.StringIO()):
            second = cloud.prepare(self.base / 'second', published['locator']['tag'], published['locator']['sha256'],
                                   published['document_index_sha256'], client=client, through='2026-09-04', max_filings=1)
        self.assertEqual(second['originals_rechecked'], 0)
        self.assertEqual(second['indexed_documents'], 5)
        self.assertEqual(client.documents, [fixtures.NEW])
        self.assertEqual(second['archive_recovery']['selected_documents_verified'], 0)
        for kind, args in self.github.calls:
            if kind == 'command':
                self.assertEqual(args[:2], ['release', 'download'])
                name = args[args.index('--pattern') + 1]
                self.assertFalse(self.github.bodies[args[2]][name].startswith(b'PK'))

    def test_cached_complete_index_corruption_rejects_reuse_before_original_chunks(self):
        from insider_pipeline.github_session import open_inventory
        with self.github.patches(), contextlib.redirect_stdout(io.StringIO()):
            session = open_inventory(self.fixture.bucket, self.fixture.staged['transport_sha256'], self.base / 'index-only')
            cached = github_documents.cache_index(session.chain, session.downloader, self.fixture.published['document_index_sha256'])
            self.assertTrue(cached['index_path'].is_file())
            cached['index_path'].write_bytes(b'corrupt')
            before = list(self.github.calls)
            with self.assertRaisesRegex(ValueError, 'Retained document-index cache'):
                session.recover([fixtures.ACCESSION], document_index_pin=self.fixture.published['document_index_sha256'])
        self.assertFalse(any(kind == 'command' for kind, args in self.github.calls[len(before):]))
        self.assertFalse((session.root / 'shards').exists())

    def test_failed_download_is_bounded_and_retry_state_survives_checkpoint(self):
        work, result, client = self.prepare('retry', client=SEC(self.bodies, fail=True))
        self.assertEqual(client.documents, [fixtures.PENDING])
        self.assertEqual(result['collection']['attempted_this_run'], 1)
        self.assertEqual(result['collection']['completed_this_run'], 0)
        self.assertEqual(result['collection']['network_errors_this_run'], 1)
        self.assertEqual(result['queue_counts']['retry'], 1)
        db = readonly(work / 'target-inventory.sqlite3')
        row = db.execute('SELECT attempts,status,retry_after FROM filings WHERE accession=?', (fixtures.PENDING,)).fetchone()
        self.assertEqual((row[0], row[1]), (1, 'retry')); self.assertGreater(row[2], 0); db.close()
        self.assertEqual(result['changed_documents_archived'], 2)

    def test_historical_pending_selection_recovers_its_older_bulk_zip_for_source_audit(self):
        from insider_pipeline import bulk_refresh, discovery
        older = '0001234567-26-000006'
        original_setup = fixtures.DiscoveryRefreshTests.setUp

        def seed(instance):
            original_setup(instance)
            bodies = dict(instance.initial)
            bodies[fixtures.index_url('2026Q1')] = fixtures.source_body([(fixtures.LEGACY, '4', '2026-01-03', 64040),
                                                                       (older, '4', '2026-01-05', 64040)])
            discovery.discover(instance.root, fixtures.SourceClient(bodies), workers=1)
            body = quarter_body([(older, 64040, '2026-01-05', '4')]); path = instance.root / 'quarter.zip'; path.write_bytes(body)
            meta = {'url': bulk_refresh.bulk_url('2026Q1'), 'bytes': len(body), 'sha256': hashlib.sha256(body).hexdigest(),
                    'retrieved_at_utc': '2026-09-09T00:00:00Z'}
            parsed = bulk_refresh.parse_source(('2026Q1', meta, str(path), {'window_start':'2026-01-01','window_end':'2026-09-03'},
                                                str(instance.root / 'quarter-records.sqlite3')))
            db = sqlite3.connect(instance.inventory); db.row_factory = sqlite3.Row
            with db:
                bulk_refresh.import_sources(db, [parsed])
            destination = instance.root / db.execute("SELECT path FROM sources WHERE source_key='2026Q1'").fetchone()[0]
            destination.parent.mkdir(parents=True); destination.write_bytes(body); db.close()

        fixture = session_fixtures.GitHubSessionRefreshTests()
        with patch.object(fixtures.DiscoveryRefreshTests, 'setUp', seed), contextlib.redirect_stdout(io.StringIO()):
            fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        with fixture.github.patches(), contextlib.redirect_stdout(io.StringIO()):
            result = cloud.prepare(fixture.base / 'cloud-extra', fixture.bucket, fixture.staged['transport_sha256'],
                                   fixture.published['document_index_sha256'], client=SEC(self.bodies),
                                   through='2026-09-04', max_filings=1)
        self.assertEqual(result['archive_recovery']['source_quarters'], ['2026Q1'])
        self.assertGreater(result['archive_recovery']['selected_source_files_verified'], 0)
        self.assertEqual(json.loads((fixture.base / 'cloud-extra/collection-accessions.json').read_text()), [older])
        self.assertEqual(result['source_audit_counts'].get('document_failure', 0), 0)
        self.assertEqual(result['source_audit_counts'].get('original_xml_field_failure', 0), 0)
        self.assertGreater(result['original_source_checks'], 0)

    def test_independent_field_failure_blocks_publication_even_with_valid_storage_hashes(self):
        original_prepare = runner.prepare

        def altered(body, row):
            record = original_prepare(body, row)
            parsed = json.loads(gzip.decompress(record['parsed_gzip']))
            parsed['transactions'][0]['transaction_value'] = '999.99'
            serialized = canonical(parsed)
            record.update(parsed_gzip=gzip.compress(serialized, mtime=0), parsed_sha256=hashlib.sha256(serialized).hexdigest(),
                          parsed_bytes=len(serialized))
            return record

        with patch('insider_pipeline.runner.prepare', side_effect=altered), self.assertRaisesRegex(ValueError, 'Source audit failures'):
            self.prepare('bad-original')
        self.assertFalse((self.base / 'bad-original' / cloud.MANIFEST).exists())
        failure = json.loads((self.base / 'bad-original/failure.json').read_text())
        self.assertEqual(failure['stage'], 'audit-and-build-increment')
        self.assertFalse(any(kind == 'command' and args[:2] != ['release', 'download'] for kind, args in self.github.calls))

    def test_unparsed_responses_remain_archived_as_review_with_explicit_audit_counts(self):
        work, result, _ = self.prepare('unparsed', client=SEC(self.bodies, malformed=True))
        self.assertEqual(result['source_audit_counts']['unparsed_documents'], 1)
        row, document = self.fixture.fixture.record(work / 'candidate', fixtures.PENDING)
        self.assertEqual(row['status'], 'review')
        self.assertEqual(gzip.decompress(document['source_gzip']), b'not original XML')
        self.assertFalse(result['complete_backfill'])

    def test_repinning_changed_pending_inventory_rejects_before_archive_upload(self):
        work, result, _ = self.prepare('mutated')
        db = sqlite3.connect(work / 'target-inventory.sqlite3')
        db.execute('UPDATE filings SET attempts=77 WHERE accession=?', (fixtures.NEW,)); db.commit(); db.close()
        pin = self.repin(work, target_inventory_file_sha256=file_hash(work / 'target-inventory.sqlite3'))
        before = list(self.github.calls)
        with self.github.patches(), self.assertRaisesRegex(ValueError, 'every archived table row'):
            cloud.publish(work, pin, self.fixture.bucket)
        self.assertEqual(self.github.calls, before)
        self.assertFalse((work / 'maintenance-publication.json').exists())

    def test_time_scope_and_work_limits_reject_before_remote_reads(self):
        self.assertEqual(cloud.completed_calendar_date(datetime(2026, 9, 10, 2, tzinfo=timezone.utc)), '2026-09-08')
        client = SEC(self.bodies)
        for change in ({'max_filings': 1001}, {'seconds': 0}, {'workers': 8}, {'through': '2999-01-01'},
                       {'through': '2026-09-04', 'collection_start': '2026-09-05'}):
            options = {'through': '2026-09-04', **change}
            with self.github.patches(), self.assertRaises(ValueError):
                cloud.prepare(self.base / 'invalid', self.fixture.bucket, self.fixture.staged['transport_sha256'],
                              self.fixture.published['document_index_sha256'], client=client, **options)
        self.assertEqual(client.requests, 0); self.assertEqual(self.github.calls, [])
        self.assertFalse((self.base / 'invalid').exists())


class BoundedRunnerTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(); self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name) / 'state'; db = connect(self.root)
        self.accessions = [f'0001234567-26-{number:06d}' for number in range(1, 7)]
        db.executemany('INSERT INTO filings(accession,form,filing_date,source_url) VALUES(?,?,?,?)',
                       [(a, '4', '2026-09-03', 'https://www.sec.gov/' + a + '.txt') for a in self.accessions])
        db.commit(); db.close()

    def test_attempt_limit_counts_failures_and_preserves_every_unselected_row(self):
        before = readonly(self.root / 'inventory.sqlite3')
        original = {r['accession']: tuple(r) for r in before.execute('SELECT * FROM filings')}; before.close()
        client = SEC({}, fail=True); client.requests = 9; client.download_bytes = 111
        with contextlib.redirect_stdout(io.StringIO()):
            result = runner.run(self.root, client, workers=4, limit=2, attempt_limit=2, accessions=self.accessions[2:])
        self.assertEqual(result['attempted_this_run'], 2)
        self.assertEqual(result['network_errors_this_run'], 2)
        self.assertEqual(result['http_requests_this_run'], 2)
        self.assertEqual(result['download_bytes_this_run'], 0)
        self.assertEqual(set(client.documents), set(self.accessions[2:4]))
        after = readonly(self.root / 'inventory.sqlite3')
        for row in after.execute('SELECT * FROM filings'):
            if row['accession'] not in client.documents:
                self.assertEqual(tuple(row), original[row['accession']])
        after.close()

    def test_empty_selection_makes_no_requests_and_signals_are_restored_on_success_and_failure(self):
        original = {name: signal.getsignal(name) for name in (signal.SIGTERM, signal.SIGINT)}
        client = SEC({})
        with contextlib.redirect_stdout(io.StringIO()):
            result = runner.run(self.root, client, accessions=[], attempt_limit=1)
        self.assertEqual(result['attempted_this_run'], 0); self.assertEqual(client.requests, 0)
        self.assertEqual(original, {name: signal.getsignal(name) for name in original})
        from types import SimpleNamespace
        with patch('insider_pipeline.runner.shutil.disk_usage', return_value=SimpleNamespace(free=0)), contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(RuntimeError, '10 GiB'):
                runner.run(self.root, client, accessions=self.accessions[:1], attempt_limit=1)
        self.assertEqual(original, {name: signal.getsignal(name) for name in original})

    def test_invalid_selection_or_limits_leave_inventory_unchanged(self):
        before = file_hash(self.root / 'inventory.sqlite3')
        for values in ({'accessions': [self.accessions[0]] * 2}, {'accessions': ['../escape']},
                       {'accessions': self.accessions, 'sample_each_form': 1}, {'attempt_limit': -1}, {'workers': 8}):
            with self.assertRaises(ValueError):
                runner.run(self.root, SEC({}), **values)
        self.assertEqual(file_hash(self.root / 'inventory.sqlite3'), before)


if __name__ == '__main__':
    unittest.main()
