import hashlib
import json
from pathlib import Path
import shutil
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import zipfile

from insider_pipeline.audit_batches import file_hash, readonly
from insider_pipeline.github_chain import Downloader, blob_name, descriptor_name, metadata, read_chain
from insider_pipeline.github_increment_stage import stage_increment
from insider_pipeline import github_sources
from insider_pipeline.increment import MANIFEST, build
from insider_pipeline.inventory import canonical, connect
from insider_pipeline.inventory_archive import capture_snapshot
import test_github_chain as fixtures
import test_github_inventory_chain as inventory_fixtures
import test_increment as increment_fixtures


class GitHubSourceTests(unittest.TestCase):
    bucket = fixtures.GitHubChainTests.bucket

    def seed(self, base):
        root, target, bundle, pin, parent, github = fixtures.GitHubChainTests().seed(base)
        with github.patches():
            staged = stage_increment(bundle, base / 'stage', pin, parent, self.bucket, 1)
            github.publish(self.bucket)
        github.calls.clear(); github.push = False
        return root, target, bundle, pin, staged, github

    def requested(self, github):
        return inventory_fixtures.GitHubInventoryChainTests().downloads(github)

    def inventory_parts(self, github):
        names = set()
        for tag, bodies in github.bodies.items():
            for name, body in bodies.items():
                if name.endswith('.gz.part') or (name.startswith('blob-') and body[:2] == b'\x1f\x8b'):
                    names.add((tag, name))
        return names

    def test_loads_exact_current_source_versions_without_historical_documents(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); root, target, bundle, _, staged, github = self.seed(base)
            expected = inventory_fixtures.GitHubInventoryChainTests().inventory_pairs(github, staged['transport_sha256'])
            baseline = json.loads(github.bodies['insider-baseline']['manifest.json'])
            current = json.loads((bundle / 'manifest.json').read_text())
            increment = json.loads((bundle / MANIFEST).read_text())
            expected.update(('insider-baseline', name) for name in ('manifest.json', 'collection-catalog.json'))
            expected.update((self.bucket, blob_name(file_hash(bundle / name))) for name in ('manifest.json', 'collection-catalog.json'))
            quarter = next(row for row in baseline['assets'] if row.get('url', '').endswith('2026Q1.zip'))
            changed_index = next(row for row in increment['source_assets'] if row['source_table'] == 'index_sources')
            expected.update({('insider-baseline', quarter['file']), (self.bucket, blob_name(changed_index['sha256']))})
            github.corrupt_download = baseline['chunks'][0]['file']
            with github.patches():
                report = read_chain(self.bucket, staged['transport_sha256'], base / 'cloud', inventory_only=True, source_quarters=['2026Q1'])
            self.assertEqual(set(self.requested(github)), expected)
            self.assertEqual(len(self.requested(github)), len(expected))
            self.assertEqual(report['asset_bytes'], sum(len(github.bodies[tag][name]) for tag, name in expected))
            self.assertEqual(report['selected_source_files_verified'], 2)
            self.assertEqual(report['quarter_zip_keys'], ['2026Q1'])
            self.assertEqual(report['index_source_keys'], ['2026Q1'])
            self.assertTrue(report['source_cache_verified'] and report['source_catalog_matches_inventory'])
            self.assertFalse(report['full_restore_verified'] or report['source_audit_performed'] or report['collection_resume_ready'])
            self.assertFalse(report['includes_original_documents'] or report['cloud_daily_maintenance_active'])
            self.assertFalse((base / 'cloud/restored/shards').exists())
            increment_fixtures.IncrementTests().assert_inventory_equal(target, base / 'cloud/restored/inventory.sqlite3')
            db = readonly(target)
            index = dict(db.execute('SELECT * FROM index_sources').fetchone()); db.close()
            stem = hashlib.sha256(index['url'].encode()).hexdigest()
            copied = base / 'cloud/restored/sources/indexes' / (stem + '.body')
            self.assertEqual(copied.read_bytes(), (root / 'sources/indexes' / (stem + '.body')).read_bytes())
            self.assertEqual(json.loads(copied.with_suffix('.json').read_text())['retrieved_at_utc'], index['retrieved_at'])
            self.assertEqual((base / 'cloud/restored/2026Q1.zip').read_bytes(), (root / '2026Q1.zip').read_bytes())
            self.assertFalse((base / 'cloud/restored/2018Q1.zip').exists())
            self.assertFalse((base / 'cloud/restored/2026Q2.zip').exists())
            self.assertEqual(current['include_sources'], False)

    def extend(self, base, root, target, bundle, pin, staged, github, modify):
        db = connect(root); modify(db); db.commit(); db.close()
        next_target = base / 'next.sqlite3'; capture_snapshot(root, next_target)
        next_bundle = base / 'next'
        result = build(root, bundle / MANIFEST, pin, target, next_target, next_bundle, workers=1)
        github.push = True
        with github.patches():
            next_staged = stage_increment(next_bundle, base / 'next-stage', result['increment_manifest_sha256'], staged['locator'], self.bucket, 1)
        github.calls.clear(); github.push = False
        return next_target, next_bundle, next_staged

    def test_revised_zip_and_current_index_only_quarter_follow_newest_checkpoint(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); root, target, bundle, pin, staged, github = self.seed(base)
            revised = root / '2026Q2-revised.zip'
            with zipfile.ZipFile(revised, 'w') as archive:
                archive.writestr('SUBMISSION.tsv', 'A later source revision\n')
            url = 'https://www.sec.gov/Archives/edgar/full-index/2026/QTR3/master.idx'
            raw = b'CIK|Company Name|Form Type|Date Filed|Filename\n64040|Fixture|4|2026-07-01|edgar/data/64040/0001234567-26-000004.txt\n'
            cache = root / 'sources/indexes' / (hashlib.sha256(url.encode()).hexdigest() + '.body'); cache.write_bytes(raw)

            def change(db):
                db.execute("UPDATE sources SET path=?,sha256=?,bytes=? WHERE source_key='2026Q2'", (revised.name, file_hash(revised), revised.stat().st_size))
                db.execute('INSERT INTO index_sources VALUES(?,?,?,?,?,?)', ('2026Q3', url, file_hash(cache), len(raw), '2026-09-10', 1))

            next_target, _, next_staged = self.extend(base, root, target, bundle, pin, staged, github, change)
            with github.patches():
                report = read_chain(self.bucket, next_staged['transport_sha256'], base / 'cloud', inventory_only=True, source_quarters=['2026Q3', '2026Q2'])
            self.assertEqual(report['source_quarters'], ['2026Q2', '2026Q3'])
            self.assertEqual(report['quarter_zip_keys'], ['2026Q2'])
            self.assertEqual(report['index_source_keys'], ['2026Q3'])
            self.assertEqual(report['selected_source_files_verified'], 2)
            self.assertEqual((base / 'cloud/restored' / revised.name).read_bytes(), revised.read_bytes())
            self.assertFalse((base / 'cloud/restored/2026Q2.zip').exists())
            self.assertEqual((base / 'cloud/restored/sources/indexes' / cache.name).read_bytes(), raw)
            increment_fixtures.IncrementTests().assert_inventory_equal(next_target, base / 'cloud/restored/inventory.sqlite3')

    def test_metadata_only_source_changes_retain_bytes_from_original_ancestor(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); root, target, bundle, pin, staged, github = self.seed(base)
            def change(db):
                db.execute("UPDATE sources SET path='renamed-quarter.zip',url='https://www.sec.gov/new-alias.zip',imported_at='later' WHERE source_key='2026Q1'")
            next_target, next_bundle, next_staged = self.extend(base, root, target, bundle, pin, staged, github, change)
            self.assertEqual(json.loads((next_bundle / MANIFEST).read_text())['source_assets'], [])
            with github.patches():
                report = read_chain(self.bucket, next_staged['transport_sha256'], base / 'cloud', inventory_only=True, source_quarters=['2026Q1'])
            self.assertTrue(report['source_cache_verified'])
            self.assertEqual((base / 'cloud/restored/renamed-quarter.zip').read_bytes(), (root / '2026Q1.zip').read_bytes())
            self.assertFalse((base / 'cloud/restored/2026Q1.zip').exists())
            increment_fixtures.IncrementTests().assert_inventory_equal(next_target, base / 'cloud/restored/inventory.sqlite3')

    def rewrite_catalog(self, github, pin, change):
        bodies = github.bodies[self.bucket]
        transport = json.loads(bodies[descriptor_name(pin)])
        manifest = json.loads(bodies[blob_name(transport['checkpoint']['manifest_sha256'])])
        documents = json.loads(bodies[blob_name(manifest['document_manifest_sha256'])])
        old = next(row for row in documents['assets'] if row['kind'] == 'collection_catalog')
        catalog = json.loads(bodies[blob_name(old['sha256'])]); change(catalog)
        raw = canonical(catalog); digest = hashlib.sha256(raw).hexdigest(); bodies[blob_name(digest)] = raw
        for rows in (documents['assets'], manifest['files']):
            row = next(item for item in rows if item['file'] == old['file'])
            row.update(sha256=digest, bytes=len(raw))
        raw = canonical(documents); digest = hashlib.sha256(raw).hexdigest(); bodies[blob_name(digest)] = raw
        manifest['document_manifest_sha256'] = digest
        next(row for row in manifest['files'] if row['file'] == 'manifest.json').update(sha256=digest, bytes=len(raw))
        raw = canonical(manifest); digest = hashlib.sha256(raw).hexdigest(); bodies[blob_name(digest)] = raw
        transport['checkpoint']['manifest_sha256'] = digest
        raw = canonical(transport); digest = hashlib.sha256(raw).hexdigest(); bodies[descriptor_name(digest)] = raw
        return digest

    def test_catalog_row_must_match_inventory_even_when_every_outer_hash_is_repinned(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); _, _, _, _, staged, github = self.seed(base)
            pin = self.rewrite_catalog(github, staged['transport_sha256'], lambda catalog: catalog['sources']['2026Q1'].update(imported_at='unrelated time'))
            baseline = json.loads(github.bodies['insider-baseline']['manifest.json'])
            source = next(row['file'] for row in baseline['assets'] if row.get('url', '').endswith('2026Q1.zip'))
            with github.patches(), self.assertRaisesRegex(ValueError, 'catalog rows differ'):
                read_chain(self.bucket, pin, base / 'bad', inventory_only=True, source_quarters=['2026Q1'])
            self.assertNotIn(('insider-baseline', source), self.requested(github))
            self.assertFalse((base / 'bad/restored').exists())
            self.assertFalse((base / 'bad/cloud-verification.json').exists())

    def test_unsafe_or_missing_selection_fails_before_inventory_parts(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); _, _, _, _, staged, github = self.seed(base)
            for index, (quarters, inventory_only) in enumerate([([], True), (['../2026Q1'], True), (['2026Q1'] * 2, True), (['2026Q1'], False), (['2027Q1'], True)]):
                github.calls.clear()
                with github.patches(), self.assertRaises(ValueError):
                    read_chain(self.bucket, staged['transport_sha256'], base / str(index), inventory_only=inventory_only, source_quarters=quarters)
                self.assertFalse(set(self.requested(github)) & self.inventory_parts(github))
                self.assertFalse((base / str(index) / 'restored').exists())
            pin = self.rewrite_catalog(github, staged['transport_sha256'], lambda catalog: catalog['sources']['2026Q1'].update(path='../escape.zip'))
            github.calls.clear()
            with github.patches(), self.assertRaises(ValueError):
                read_chain(self.bucket, pin, base / 'path', inventory_only=True, source_quarters=['2026Q1'])
            self.assertFalse(set(self.requested(github)) & self.inventory_parts(github))
            self.assertFalse((base / 'path/restored').exists())

    def test_selected_source_corruption_never_exposes_completed_root(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); _, _, _, _, staged, github = self.seed(base)
            baseline = json.loads(github.bodies['insider-baseline']['manifest.json'])
            github.corrupt_download = next(row['file'] for row in baseline['assets'] if row.get('url', '').endswith('2026Q1.zip'))
            with github.patches(), self.assertRaisesRegex(ValueError, 'Downloaded checkpoint bytes'):
                read_chain(self.bucket, staged['transport_sha256'], base / 'bad', inventory_only=True, source_quarters=['2026Q1'])
            self.assertFalse((base / 'bad/restored').exists())
            self.assertFalse((base / 'bad/cloud-verification.json').exists())

    def test_index_original_length_and_digest_are_checked_after_valid_compressed_download(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); _, target, _, _, staged, github = self.seed(base)
            for field, value, message in (('bytes', 1, 'exceeds its declared original'), ('sha256', '0' * 64, 'differ from the restored inventory')):
                with self.subTest(field=field):
                    root = base / field; root.mkdir(); shutil.copyfile(target, root / 'inventory.sqlite3')
                    downloader = Downloader(base / (field + '-cache'))
                    with github.patches():
                        chain = metadata(staged['locator'], base / (field + '-metadata'), downloader)
                        selection = github_sources.plan(chain, downloader, ['2026Q1'])
                        row = selection['catalog_rows'][('index_sources', '2026Q1')]
                        row[field] = value
                        db = connect(root); db.execute('UPDATE index_sources SET ' + field + '=?', (value,)); db.commit(); db.close()
                        with self.assertRaisesRegex(ValueError, message):
                            github_sources.restore(selection, downloader, root)
                    self.assertFalse((root / 'selected-source-report.json').exists())

    def test_combined_source_download_and_disk_budgets_fail_before_large_parts(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); _, _, bundle, _, staged, github = self.seed(base)
            with github.patches():
                report = read_chain(self.bucket, staged['transport_sha256'], base / 'good', inventory_only=True, source_quarters=['2026Q1'])
            github.calls.clear()
            with github.patches(), patch('insider_pipeline.github_chain.MAX_DOWNLOAD_BYTES', report['asset_bytes'] - 1), self.assertRaisesRegex(ValueError, 'download budget'):
                read_chain(self.bucket, staged['transport_sha256'], base / 'small', inventory_only=True, source_quarters=['2026Q1'])
            self.assertFalse(set(self.requested(github)) & self.inventory_parts(github))
            base_inventory = json.loads(github.bodies['insider-baseline']['inventory-manifest.json'])
            delta = json.loads((bundle / 'inventory-delta.json').read_text())
            required = 3 * (base_inventory['raw_bytes'] + 4 * delta['raw_bytes']) + 2 * report['asset_bytes'] + report['selected_source_raw_bytes'] + 2 * 1024**3
            github.calls.clear()
            with github.patches(), patch('insider_pipeline.github_chain.MIN_FREE_BYTES', 0), patch('insider_pipeline.github_chain.shutil.disk_usage', return_value=SimpleNamespace(free=required - 1)), self.assertRaisesRegex(ValueError, 'selected inventory and source cache'):
                read_chain(self.bucket, staged['transport_sha256'], base / 'disk', inventory_only=True, source_quarters=['2026Q1'])
            self.assertFalse(set(self.requested(github)) & self.inventory_parts(github))


if __name__ == '__main__':
    unittest.main()
