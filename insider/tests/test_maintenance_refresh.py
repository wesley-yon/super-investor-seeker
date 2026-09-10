from collections import Counter
import contextlib
import io
import json
import shutil
import sqlite3
import unittest
from unittest.mock import patch

from insider_pipeline import bulk_refresh as bulk, maintenance_refresh as maintenance
from insider_pipeline.audit_batches import file_hash, readonly, run as audit
from insider_pipeline.discovery_refresh import index_url
from insider_pipeline.http import atomic_write
from insider_pipeline.inventory import canonical
from insider_pipeline.inventory_delta import schema, state
import test_discovery_refresh as fixtures
from test_bulk_refresh import quarter_body


def catalog(urls):
    return ('<html>' + ''.join(f'<a href="{url}">{key}</a>' for key, url in urls.items()) + '</html>').encode()


class MaintenanceTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.DiscoveryRefreshTests(); self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.base, self.root, self.inventory = self.fixture.base, self.fixture.root, self.fixture.inventory
        self.urls = {key: bulk.bulk_url(key) for key in ('2026Q2', '2026Q3')}
        self.urls['2026Q2'] = self.urls['2026Q2'].replace('datastandardsinnovation', 'structureddata')
        self.rows = [(fixtures.ACCESSION, 64040, '2026-09-03', '4'), (fixtures.REMOVED, 64040, '2026-09-03', '4')]
        self.bodies = {**self.fixture.changed, maintenance.CATALOG_URL: catalog(self.urls),
                       self.urls['2026Q2']: quarter_body([(fixtures.PENDING, 64040, '2026-06-03', '4')]),
                       self.urls['2026Q3']: quarter_body(self.rows + [(fixtures.NEW, 64040, '2026-09-04', '4')])}

    def prepare(self, name='plan', *, bodies=None, inventory=None, through='2026-09-04', workers=1, **kwargs):
        client = fixtures.SourceClient(self.bodies if bodies is None else bodies)
        result = maintenance.prepare(inventory or self.inventory, self.base / name, through,
                                     client=client, workers=workers, **kwargs)
        return self.base / name, result, client

    def repin(self, directory, **changes):
        path = directory / maintenance.MANIFEST
        value = json.loads(path.read_text()); value.update(changes); path.write_bytes(canonical(value))
        return file_hash(path)

    def test_combined_changes_select_original_union_once_and_retain_both_sources(self):
        before = self.fixture.rows(self.inventory); original = file_hash(self.inventory)
        directory, result, client = self.prepare()
        self.assertEqual(result['new_filings'], 1)  # Same accession in both sources counts once.
        self.assertEqual(result['committed_documents_to_restore'], 2)
        self.assertEqual(json.loads((directory / 'committed-accessions.json').read_text()), [fixtures.ACCESSION, fixtures.REMOVED])
        self.assertEqual(Counter(url for url, fresh in client.calls), Counter(self.bodies.keys()))
        self.assertTrue(all(fresh for _, fresh in client.calls))
        output = self.base / 'candidate'
        from insider_pipeline.discovery_refresh import prepare_document
        with patch('insider_pipeline.discovery_refresh.prepare_document', wraps=prepare_document) as recheck:
            report = maintenance.materialize(directory, result['plan_sha256'], self.root, output)
        self.assertEqual(recheck.call_count, 2)
        self.assertTrue(report['combined_metadata_refresh_verified'])
        self.assertEqual(report['target_scope']['window_end'], '2026-09-04')
        for accession in (fixtures.ACCESSION, fixtures.REMOVED):
            old_row, old = self.fixture.record(self.root, accession); row, new = self.fixture.record(output, accession)
            self.assertNotEqual(row['shard'], old_row['shard'])
            for field in ('source_gzip', 'source_sha256', 'source_bytes', 'parsed_gzip', 'parsed_sha256', 'parsed_bytes', 'fetched_at'):
                self.assertEqual(old[field], new[field], field)
        plan = json.loads((directory / maintenance.MANIFEST).read_text())
        for asset in plan['source_files']:
            self.assertEqual((directory / asset['file']).read_bytes(), (output / asset['file']).read_bytes())
        db = readonly(output / 'inventory.sqlite3')
        row = dict(db.execute('SELECT * FROM filings WHERE accession=?', (fixtures.NEW,)).fetchone())
        self.assertEqual((row['status'], row['bulk_source']), ('pending', '2026Q3'))
        self.assertIsNotNone(row['source_url']); db.close()
        self.assertEqual(self.fixture.rows(self.inventory), before); self.assertEqual(file_hash(self.inventory), original)
        self.assertFalse(report['source_audit_performed'] or report['cloud_daily_maintenance_active'] or report['complete_backfill'])

    def test_exact_published_urls_relative_links_and_rejected_conflicts_or_foreign_sources(self):
        relative = self.urls['2026Q2'].removeprefix('https://www.sec.gov')
        self.assertEqual(maintenance.published_sources(catalog({'2026Q2': relative})), {'2026Q2': self.urls['2026Q2']})
        for body in (b'<html>unavailable</html>', catalog({'q': 'https://example.com/2026q2_form345.zip'}),
                     catalog({'a': self.urls['2026Q2'], 'b': bulk.bulk_url('2026Q2')})):
            with self.assertRaises(ValueError):
                maintenance.published_sources(body)
        db = sqlite3.connect(self.inventory); db.execute("DELETE FROM index_sources WHERE source_key='2026Q1'"); db.commit(); db.close()
        client = fixtures.SourceClient(self.bodies)
        with self.assertRaisesRegex(ValueError, 'missing or out-of-scope'):
            maintenance.prepare(self.inventory, self.base / 'incomplete', '2026-09-04', client=client)
        self.assertEqual(client.calls, [])

    def test_new_published_quarters_cannot_be_omitted_or_silently_truncated(self):
        scope = {'window_start': '2025-01-01', 'window_end': '2026-09-04'}
        sources = {key: bulk.bulk_url(key) for key in ('2025Q1', '2025Q2', '2025Q3', '2025Q4', '2026Q1', '2026Q2')}
        with self.assertRaisesRegex(ValueError, 'one to four'):
            maintenance.selected_bulk(scope, [], sources)
        with self.assertRaisesRegex(ValueError, 'every newly published'):
            maintenance.selected_bulk(scope, ['2025Q1', '2025Q2'], sources, ['2026Q1', '2026Q2'])
        self.assertEqual(maintenance.selected_bulk(scope, list(sources), sources), ['2026Q1', '2026Q2'])
        with self.assertRaisesRegex(ValueError, 'published URLs'):
            maintenance.selected_bulk(scope, list(sources), sources, ['2026Q3'])

    def test_cutoff_crosses_quarter_and_zip_carryover_uses_full_filing_scope(self):
        bodies = {**self.bodies, index_url('2026Q4'): fixtures.source_body([(fixtures.NEW, '4', '2026-10-01', 64040)])}
        bodies[index_url('2026Q3')] = self.fixture.initial[index_url('2026Q3')]
        # A newly published Q3 ZIP can carry a Q2 filing; it is not filtered by Q3 dates.
        bodies[self.urls['2026Q3']] = quarter_body(self.rows + [(fixtures.PENDING, 64040, '2026-06-03', '4')])
        bodies[self.urls['2026Q2']] = quarter_body([])
        directory, result, _ = self.prepare(bodies=bodies, through='2026-10-02')
        self.assertEqual(result['index_quarters'], ['2026Q3', '2026Q4'])
        maintenance.materialize(directory, result['plan_sha256'], self.root, self.base / 'rollover')
        db = readonly(self.base / 'rollover/inventory.sqlite3')
        self.assertEqual(db.execute('SELECT bulk_source FROM filings WHERE accession=?', (fixtures.PENDING,)).fetchone()[0], '2026Q3')
        self.assertEqual(db.execute('SELECT count(*) FROM index_sources').fetchone()[0], 4); db.close()

    def test_unchanged_zip_imports_newly_in_scope_filings_when_cutoff_advances(self):
        initial = {**self.bodies, **self.fixture.initial}
        directory, first, _ = self.prepare('initial', bodies=initial, through='2026-09-03')
        root = self.base / 'attached'; maintenance.materialize(directory, first['plan_sha256'], self.root, root)
        directory, second, _ = self.prepare('advance', inventory=root / 'inventory.sqlite3')
        self.assertEqual(second['changed_bulk_quarters'], ['2026Q3'])
        self.assertEqual(second['new_filings'], 1)
        db = readonly(directory / 'inventory.sqlite3')
        row = db.execute('SELECT bulk_source,expected_counts FROM filings WHERE accession=?', (fixtures.NEW,)).fetchone()
        self.assertEqual(row[0], '2026Q3'); self.assertEqual(json.loads(row[1])['NONDERIV_TRANS'], 1); db.close()
        maintenance.materialize(directory, second['plan_sha256'], root, self.base / 'advanced')

    def test_value_only_bulk_change_and_index_change_form_one_union(self):
        directory, first, _ = self.prepare('first')
        root = self.base / 'attached'; maintenance.materialize(directory, first['plan_sha256'], self.root, root)
        bodies = dict(self.bodies)
        bodies[self.urls['2026Q3']] = quarter_body(self.rows + [(fixtures.NEW, 64040, '2026-09-04', '4')], price='777.123')
        bodies[index_url('2026Q1')] = fixtures.source_body([(fixtures.LEGACY, '4', '2026-01-04', 64040)])
        directory, result, _ = self.prepare('values', inventory=root / 'inventory.sqlite3', bodies=bodies,
                                          index_quarters=['2026Q1', '2026Q2', '2026Q3'])
        self.assertEqual(result['committed_documents_to_restore'], 3)
        # Materialize requires the inherited untouched legacy shard as well.
        row, _ = self.fixture.record(self.root, fixtures.LEGACY)
        (root / row['shard']).parent.mkdir(exist_ok=True); shutil.copyfile(self.root / row['shard'], root / row['shard'])
        candidate = self.base / 'values-candidate'
        maintenance.materialize(directory, result['plan_sha256'], root, candidate)
        with contextlib.redirect_stdout(io.StringIO()):
            result = audit(candidate, self.base / 'values-audit', workers=1,
                           inventory_snapshot=candidate / 'inventory.sqlite3', parent_inventory=root / 'inventory.sqlite3')
        self.assertEqual(result['selected_documents'], 3)
        self.assertEqual(result['counts'].get('original_xml_field_failure', 0), 0)
        self.assertEqual(result['counts'].get('document_failure', 0), 0)
        self.assertEqual(result['financial_table_comparisons']['review'], 2)

    def test_index_only_revision_recovers_its_inherited_unselected_bulk_source(self):
        directory, first, _ = self.prepare('attached-plan')
        root = self.base / 'attached'; maintenance.materialize(directory, first['plan_sha256'], self.root, root)
        bodies = dict(self.bodies)
        bodies[index_url('2026Q3')] = fixtures.source_body([(fixtures.ACCESSION, '4', '2026-09-05', 64040),
                                                           (fixtures.NEW, '4', '2026-09-04', 64040)])
        directory, result, _ = self.prepare('index-revision', inventory=root / 'inventory.sqlite3',
                                          bodies=bodies, through='2026-09-05', bulk_quarters=['2026Q2'])
        self.assertEqual(result['changed_bulk_quarters'], [])
        self.assertEqual(result['committed_documents_to_restore'], 1)
        self.assertEqual(result['required_bulk_quarters'], ['2026Q3'])
        candidate = self.base / 'index-revised'
        maintenance.materialize(directory, result['plan_sha256'], root, candidate)
        db = readonly(root / 'inventory.sqlite3')
        path = db.execute("SELECT path FROM sources WHERE source_key='2026Q3'").fetchone()[0]; db.close()
        self.assertEqual((candidate / path).read_bytes(), (root / path).read_bytes())

    def test_serial_parallel_and_cached_preparations_preserve_every_row_and_source_byte(self):
        first, one, _ = self.prepare('serial', workers=1)
        second, two, _ = self.prepare('parallel', workers=2)
        self.assertEqual(one, two)
        for path in first.rglob('*'):
            if path.is_file():
                self.assertEqual(path.read_bytes(), (second / path.relative_to(first)).read_bytes(), path.name)
        cache = self.base / 'cache'
        for url, body in self.bodies.items():
            kind = 'catalog' if url == maintenance.CATALOG_URL else 'indexes' if url.endswith('master.idx') else 'quarterly'
            fixtures.SourceClient({url: body}).cached(url, cache / kind)
        result = maintenance.prepare(self.inventory, self.base / 'cached', '2026-09-04', cache=cache, workers=2)
        self.assertEqual(result['committed_documents_to_restore'], one['committed_documents_to_restore'])
        maintenance.materialize(self.base / 'cached', result['plan_sha256'], self.root, self.base / 'cached-candidate')
        self.assertEqual(result['retrieval_mode'], 'verified_cache')
        for row in json.loads((first / maintenance.MANIFEST).read_text())['source_files']:
            self.assertEqual((first / row['file']).read_bytes(), (self.base / 'cached' / row['file']).read_bytes())

    def test_repinned_omissions_invented_metadata_and_unpublished_urls_are_rejected(self):
        directory, _, _ = self.prepare('omitted')
        selection = directory / 'committed-accessions.json'; atomic_write(selection, canonical([fixtures.ACCESSION]))
        pin = self.repin(directory, committed_documents=1, committed_accessions_sha256=file_hash(selection))
        with self.assertRaisesRegex(ValueError, 'omitted or added an affected'):
            maintenance.materialize(directory, pin, self.root, self.base / 'bad-omitted')
        directory, _, _ = self.prepare('invented')
        db = sqlite3.connect(directory / 'inventory.sqlite3'); db.row_factory = sqlite3.Row
        db.execute('UPDATE filings SET attempts=99 WHERE accession=?', (fixtures.PENDING,)); db.commit()
        target_state = state(db, schema(db)); db.close()
        pin = self.repin(directory, target_inventory_state=target_state, target_inventory_file_sha256=file_hash(directory / 'inventory.sqlite3'))
        with self.assertRaisesRegex(ValueError, 'independently replayed'):
            maintenance.materialize(directory, pin, self.root, self.base / 'bad-invented')
        directory, _, _ = self.prepare('urls')
        plan = json.loads((directory / maintenance.MANIFEST).read_text())
        plan['source_catalog'][0]['url'] = bulk.bulk_url('2026Q2')
        pin = self.repin(directory, source_catalog=plan['source_catalog'])
        with self.assertRaisesRegex(ValueError, 'retained SEC publication'):
            maintenance.read_plan(directory, pin)
        self.assertFalse((self.base / 'bad-omitted').exists() or (self.base / 'bad-invented').exists())

    def test_missing_original_and_corrupt_publication_leave_parent_and_output_untouched(self):
        before = file_hash(self.inventory)
        directory, result, _ = self.prepare()
        row, _ = self.fixture.record(self.root, fixtures.ACCESSION)
        path = self.root / row['shard']; path.rename(path.with_suffix('.hidden'))
        with self.assertRaisesRegex(ValueError, 'must be restored'):
            maintenance.materialize(directory, result['plan_sha256'], self.root, self.base / 'missing')
        path.with_suffix('.hidden').rename(path)
        page, _ = maintenance.publication_files(directory); page.write_bytes(b'bad page')
        with self.assertRaisesRegex(ValueError, 'source checksum'):
            maintenance.materialize(directory, result['plan_sha256'], self.root, self.base / 'corrupt')
        self.assertFalse((self.base / 'missing').exists() or (self.base / 'corrupt').exists())
        self.assertEqual(file_hash(self.inventory), before)


class MaintenanceChainTests(unittest.TestCase):
    def test_one_session_combined_candidate_increment_and_full_restore_retain_every_row(self):
        from insider_pipeline.github_session import open_inventory
        from insider_pipeline.increment import build as build_increment, restore as restore_increment
        from insider_pipeline.inventory_archive import capture_snapshot
        from test_github_session import GitHubSessionRefreshTests
        from test_github_documents import GitHubDocumentTests
        from test_increment import IncrementTests
        fixture = GitHubSessionRefreshTests(); fixture.setUp(); self.addCleanup(fixture.doCleanups)
        base = fixture.base
        url = bulk.bulk_url('2026Q3')
        bodies = {**fixture.fixture.changed, maintenance.CATALOG_URL: catalog({'2026Q3': url}),
                  url: quarter_body([(fixtures.ACCESSION, 64040, '2026-09-03', '4'), (fixtures.REMOVED, 64040, '2026-09-03', '4')])}
        with fixture.github.patches():
            session = open_inventory(fixture.bucket, fixture.staged['transport_sha256'], base / 'maintenance-cloud')
            prepared = maintenance.prepare(session.root / 'inventory.sqlite3', base / 'maintenance-plan', '2026-09-04',
                                           client=fixtures.SourceClient(bodies), workers=2)
            recovered = session.recover_maintenance(base / 'maintenance-plan', prepared['plan_sha256'],
                                                    document_index_pin=fixture.published['document_index_sha256'])
            self.assertEqual(recovered['selected_documents_verified'], 2)
            self.assertEqual(recovered['filing_selection_source'], 'local_maintenance_plan')
            self.assertTrue(recovered['maintenance_plan_parent_verified'])
            maintenance.materialize(base / 'maintenance-plan', prepared['plan_sha256'], session.root, base / 'maintenance-candidate')
        calls = Counter(GitHubDocumentTests().downloads(fixture.github)); self.assertTrue(all(count == 1 for count in calls.values()))
        self.assertFalse(any(kind == 'command' and args[:2] != ['release', 'download'] for kind, args in fixture.github.calls))
        target = base / 'maintenance-target.sqlite3'; capture_snapshot(base / 'maintenance-candidate', target)
        parent_manifest = base / 'increment/increment.json'
        with contextlib.redirect_stdout(io.StringIO()):
            built = build_increment(base / 'maintenance-candidate', parent_manifest, file_hash(parent_manifest),
                                    fixture.target, target, base / 'maintenance-increment', workers=1)
            restored = restore_increment(base / 'maintenance-increment', parent_manifest, base / 'maintenance-restored',
                                         built['increment_manifest_sha256'], ancestor_manifests=[base / 'baseline/baseline.json'])
            checked = audit(base / 'maintenance-restored', base / 'maintenance-full-audit', workers=1)
        self.assertEqual(restored['restored_documents'], 3); self.assertEqual(built['changed_committed_documents'], 2)
        IncrementTests().assert_inventory_equal(target, base / 'maintenance-restored/inventory.sqlite3')
        for accession in (fixtures.ACCESSION, fixtures.REMOVED, fixtures.LEGACY):
            expected = fixture.root if accession == fixtures.LEGACY else base / 'maintenance-candidate'
            self.assertEqual(fixture.fixture.record(expected, accession), fixture.fixture.record(base / 'maintenance-restored', accession))
        self.assertEqual(checked['selected_documents'], 3)
        self.assertEqual(checked['counts'].get('original_xml_field_failure', 0), 0)
        self.assertEqual(checked['counts'].get('document_failure', 0), 0)
        self.assertFalse(checked['complete_backfill'])


if __name__ == '__main__':
    unittest.main()
