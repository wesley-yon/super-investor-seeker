import contextlib
import gzip
import hashlib
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import zipfile

from insider_pipeline import document_index as index
from insider_pipeline.audit_batches import readonly
from insider_pipeline.github_chain import blob_name, read_chain
from insider_pipeline.github_document_index_stage import stage_index
from insider_pipeline.github_increment_stage import stage_increment
from insider_pipeline.inventory import canonical
from insider_pipeline.restore import update_digest
import test_document_index as index_fixtures
import test_github_inventory_chain as inventory_fixtures
import test_increment as increment_fixtures


class GitHubDocumentTests(unittest.TestCase):
    bucket = 'insider-archives-202609-001'
    accession = '0001234567-18-000001'

    def seed(self, base, publish_index=True, accessions=None):
        root, target, bundle, pin, result, parent, github = index_fixtures.DocumentIndexTests().seed(base)
        with github.patches(), contextlib.redirect_stdout(io.StringIO()):
            staged = stage_increment(bundle, base / 'stage', pin, parent, self.bucket, 1)
            github.publish(self.bucket)
            if publish_index:
                published = stage_index(base / 'index', base / 'index-stage', result['document_index_manifest_sha256'],
                                        accessions or [self.accession], target, self.bucket, staged['transport_sha256'], 1)
            else:
                published = None
        github.calls.clear()
        return root, target, bundle, result, staged, published, github

    def downloads(self, github):
        return inventory_fixtures.GitHubInventoryChainTests().downloads(github)

    def inventory_parts(self, github):
        result = set()
        for tag, bodies in github.bodies.items():
            for body in list(bodies.values()):
                if not body.startswith(b'{'):
                    continue
                value = json.loads(body)
                if value.get('inventory_archive_schema') != 1 and value.get('inventory_delta_schema') != 1:
                    continue
                for part in value['parts']:
                    result.add((tag, part['file'] if tag == 'insider-baseline' else blob_name(part['sha256'])))
        return result

    def read(self, base, staged, published, github, **kwargs):
        with github.patches():
            return read_chain(self.bucket, staged['transport_sha256'], base, inventory_only=True,
                              document_index_pin=published['document_index_sha256'],
                              filing_selection_pin=published['filing_selection_sha256'], **kwargs)

    def test_staging_is_verified_idempotent_preserves_assets_and_advertises_index_last(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); _, target, _, result, staged, _, github = self.seed(base, publish_index=False)
            before = dict(github.bodies[self.bucket])
            with github.patches():
                report = stage_index(base / 'index', base / 'index-stage', result['document_index_manifest_sha256'],
                                     [self.accession], target, self.bucket, staged['transport_sha256'], 1)
                again = stage_index(base / 'index', base / 'index-stage', result['document_index_manifest_sha256'],
                                    [self.accession], target, self.bucket, staged['transport_sha256'], 1)
            self.assertEqual((report['new_assets_uploaded'], again['new_assets_uploaded']), (3, 0))
            self.assertTrue(report['independently_downloaded_and_verified'] and report['existing_assets_preserved'])
            uploads = [args for kind, args in github.calls if kind == 'command' and args[:2] == ['release', 'upload']]
            self.assertEqual(Path(uploads[-1][3]).name, blob_name(result['document_index_manifest_sha256']))
            self.assertEqual(report['indexed_documents'], 3)
            for name, raw in before.items():
                self.assertEqual(github.bodies[self.bucket][name], raw)
            self.assertNotIn(self.accession, json.dumps(report))

    def test_selective_read_downloads_only_needed_chunks_and_materializes_exact_requested_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); root, target, bundle, result, staged, published, github = self.seed(base)
            github.push = False
            baseline = json.loads(github.bodies['insider-baseline']['manifest.json'])
            github.corrupt_download = baseline['chunks'][0]['file']
            expected = inventory_fixtures.GitHubInventoryChainTests().inventory_pairs(github, staged['transport_sha256'])
            expected.update((self.bucket, blob_name(pin)) for pin in
                            (published['document_index_sha256'], published['filing_selection_sha256'], result['index_file']['sha256']))
            latest = json.loads((bundle / 'manifest.json').read_text())
            expected.add((self.bucket, blob_name(latest['chunks'][0]['sha256'])))
            report = self.read(base / 'cloud', staged, published, github)
            self.assertEqual(set(self.downloads(github)), expected)
            self.assertEqual(len(self.downloads(github)), len(expected))
            self.assertEqual(report['asset_bytes'], sum(len(github.bodies[tag][name]) for tag, name in expected))
            self.assertEqual((report['documents'], report['selected_documents_verified'], report['selected_document_chunks']), (3, 1, 1))
            self.assertTrue(report['document_index_complete_membership_verified'] and report['includes_original_documents'])
            self.assertFalse(report['full_restore_verified'] or report['source_audit_performed'] or report['collection_resume_ready'])
            self.assertNotIn(self.accession, json.dumps(report))
            increment_fixtures.IncrementTests().assert_inventory_equal(target, base / 'cloud/restored/inventory.sqlite3')
            target_db = readonly(target)
            row = dict(target_db.execute('SELECT * FROM filings WHERE accession=?', (self.accession,)).fetchone()); target_db.close()
            original = readonly(root / row['shard'])
            expected_document = dict(original.execute('SELECT * FROM documents WHERE accession=?', (self.accession,)).fetchone()); original.close()
            recovered = readonly(base / 'cloud/restored' / row['shard'])
            self.assertEqual([dict(doc) for doc in recovered.execute('SELECT * FROM documents')], [expected_document]); recovered.close()
            self.assertEqual(len(list((base / 'cloud/restored/shards').glob('*.sqlite3'))), 1)
            digest = hashlib.sha256(); update_digest(digest, row, expected_document)
            self.assertEqual(report['selected_documents_rows_sha256'], digest.hexdigest())

    def test_selected_inherited_document_and_source_cache_can_be_recovered_together(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            inherited = '0001234567-26-000001'
            root, target, bundle, _, staged, published, github = self.seed(base, accessions=[inherited])
            latest = json.loads((bundle / 'manifest.json').read_text())
            github.corrupt_download = blob_name(latest['chunks'][0]['sha256'])
            report = self.read(base / 'cloud', staged, published, github, source_quarters=['2026Q1'])
            self.assertTrue(report['source_cache_verified'] and report['document_index_complete_membership_verified'])
            self.assertEqual(report['selected_source_files_verified'], 2)
            self.assertEqual(report['selected_documents_verified'], 1)
            self.assertEqual((base / 'cloud/restored/2026Q1.zip').read_bytes(), (root / '2026Q1.zip').read_bytes())
            increment_fixtures.IncrementTests().assert_inventory_equal(target, base / 'cloud/restored/inventory.sqlite3')
            self.assertNotIn((self.bucket, blob_name(latest['chunks'][0]['sha256'])), self.downloads(github))

    def rewrite_index(self, github, published, change_rows=None, change_manifest=None, accessions=None):
        bodies = github.bodies[self.bucket]
        value = json.loads(bodies[blob_name(published['document_index_sha256'])])
        rows = [json.loads(line) for line in gzip.decompress(bodies[blob_name(value['index_file']['sha256'])]).splitlines()]
        if change_rows:
            change_rows(rows, value)
        raw = b''.join(canonical(row) + b'\n' for row in rows)
        body = gzip.compress(raw, mtime=0); pin = hashlib.sha256(body).hexdigest()
        raw_pin = hashlib.sha256(raw).hexdigest(); bodies[blob_name(pin)] = body
        value['index_file'] = {'file': 'document-index-' + raw_pin[:16] + '.jsonl.gz', 'bytes': len(body),
                               'sha256': pin, 'raw_bytes': len(raw), 'raw_sha256': raw_pin}
        if change_manifest:
            change_manifest(value)
        body = canonical(value); index_pin = hashlib.sha256(body).hexdigest(); bodies[blob_name(index_pin)] = body
        body = canonical(index.selection(index_pin, accessions or [self.accession]))
        selection_pin = hashlib.sha256(body).hexdigest(); bodies[blob_name(selection_pin)] = body
        return {**published, 'document_index_sha256': index_pin, 'filing_selection_sha256': selection_pin}

    def test_every_index_row_must_match_inventory_before_any_selected_chunk_download(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); _, _, bundle, _, staged, published, github = self.seed(base)
            changed = self.rewrite_index(github, published, change_rows=lambda rows, _: rows[-1].__setitem__(1, '0' * 64))
            with self.assertRaisesRegex(ValueError, 'complete committed inventory rows'):
                self.read(base / 'bad', staged, changed, github)
            latest = json.loads((bundle / 'manifest.json').read_text())
            self.assertNotIn((self.bucket, blob_name(latest['chunks'][0]['sha256'])), self.downloads(github))
            self.assertFalse((base / 'bad/restored').exists())
            self.assertFalse((base / 'bad/cloud-verification.json').exists())

    def test_older_envelope_cannot_replace_current_metadata_even_with_a_repinned_lookup(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); _, _, _, _, staged, published, github = self.seed(base)
            def old_chunk(rows, value):
                first = next(i for i, chunk in enumerate(value['chunks']) if chunk['checkpoint_sha256'] != value['checkpoint']['manifest_sha256'])
                row = next(row for row in rows if row[0] == self.accession)
                row[2] = first
                with zipfile.ZipFile(io.BytesIO(github.bodies['insider-baseline'][value['chunks'][first]['file']])) as archive:
                    raw = archive.read('filings/' + self.accession + '.json.gz')
                row[3], row[4] = len(raw), len(gzip.decompress(raw))
            changed = self.rewrite_index(github, published, change_rows=old_chunk)
            with self.assertRaisesRegex(ValueError, 'differs from its complete inventory row'):
                self.read(base / 'bad', staged, changed, github)
            self.assertFalse((base / 'bad/restored').exists())
            self.assertFalse((base / 'bad/cloud-verification.json').exists())

    def test_index_publication_checks_latest_dataset_and_bucket_capacity_before_upload(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); _, target, _, result, staged, _, github = self.seed(base, publish_index=False)
            before = dict(github.bodies[self.bucket])
            with github.patches(), self.assertRaisesRegex(ValueError, 'dataset pointer changed'):
                stage_index(base / 'index', base / 'wrong-latest', result['document_index_manifest_sha256'],
                            [self.accession], target, self.bucket, staged['transport_sha256'], 2)
            with github.patches(), patch('insider_pipeline.github_increment_stage.MAX_BUCKET_ASSETS', len(before) + 2), self.assertRaisesRegex(ValueError, 'bucket is full'):
                stage_index(base / 'index', base / 'full-bucket', result['document_index_manifest_sha256'],
                            [self.accession], target, self.bucket, staged['transport_sha256'], 1)
            self.assertEqual(github.bodies[self.bucket], before)
            self.assertFalse(any(kind == 'command' and args[:2] == ['release', 'upload'] for kind, args in github.calls))

    def test_wrong_binding_unknown_filing_and_incomplete_pins_fail_before_inventory_parts(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); _, _, _, _, staged, published, github = self.seed(base)
            changed = self.rewrite_index(github, published, change_manifest=lambda value: value.update(inventory_state_sha256='0' * 64))
            with self.assertRaisesRegex(ValueError, 'complete target checkpoint'):
                self.read(base / 'binding', staged, changed, github)
            self.assertFalse(set(self.downloads(github)) & self.inventory_parts(github))
            github.calls.clear()
            changed = self.rewrite_index(github, published, accessions=['0001234567-26-000003'])
            with self.assertRaisesRegex(ValueError, 'absent from the complete committed'):
                self.read(base / 'pending', staged, changed, github)
            self.assertFalse(set(self.downloads(github)) & self.inventory_parts(github))
            with github.patches(), self.assertRaisesRegex(ValueError, 'both index and private selection'):
                read_chain(self.bucket, staged['transport_sha256'], base / 'incomplete', inventory_only=True,
                           document_index_pin=published['document_index_sha256'])
            self.assertFalse((base / 'incomplete').exists())

    def test_download_corruption_does_not_publish_index_or_expose_restored_documents(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); _, target, bundle, result, staged, _, github = self.seed(base, publish_index=False)
            github.corrupt_download = blob_name(result['index_file']['sha256'])
            with github.patches(), self.assertRaisesRegex(ValueError, 'Downloaded checkpoint bytes'):
                stage_index(base / 'index', base / 'index-stage', result['document_index_manifest_sha256'],
                            [self.accession], target, self.bucket, staged['transport_sha256'], 1)
            self.assertNotIn(blob_name(result['document_index_manifest_sha256']), github.bodies[self.bucket])
            self.assertFalse((base / 'index-stage/document-index-stage-report.json').exists())
            github.corrupt_download = None
            with github.patches():
                published = stage_index(base / 'index', base / 'index-stage', result['document_index_manifest_sha256'],
                                        [self.accession], target, self.bucket, staged['transport_sha256'], 1)
            self.assertEqual(published['new_assets_uploaded'], 1)
            latest = json.loads((bundle / 'manifest.json').read_text())
            github.corrupt_download = blob_name(latest['chunks'][0]['sha256'])
            with self.assertRaisesRegex(ValueError, 'Downloaded checkpoint bytes'):
                self.read(base / 'bad', staged, published, github)
            self.assertFalse((base / 'bad/restored').exists())
            self.assertFalse((base / 'bad/cloud-verification.json').exists())

    def test_combined_download_and_disk_budgets_stop_before_large_inventory_parts(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); _, _, bundle, result, staged, published, github = self.seed(base)
            report = self.read(base / 'good', staged, published, github, source_quarters=['2026Q1'])
            github.calls.clear()
            with patch('insider_pipeline.github_chain.MAX_DOWNLOAD_BYTES', report['asset_bytes'] - 1), self.assertRaisesRegex(ValueError, 'download budget'):
                self.read(base / 'small', staged, published, github, source_quarters=['2026Q1'])
            self.assertFalse(set(self.downloads(github)) & self.inventory_parts(github))
            base_inventory = json.loads(github.bodies['insider-baseline']['inventory-manifest.json'])
            delta = json.loads((bundle / 'inventory-delta.json').read_text())
            entry = next(row for row in index.records(base / 'index' / result['index_file']['file'], result) if row[0] == self.accession)
            required = (3 * (base_inventory['raw_bytes'] + 4 * delta['raw_bytes']) + 2 * report['asset_bytes']
                        + report['selected_source_raw_bytes'] + 4 * entry[4] + 2 * 1024**3)
            github.calls.clear()
            with patch('insider_pipeline.github_chain.MIN_FREE_BYTES', 0), patch('insider_pipeline.github_chain.shutil.disk_usage', return_value=SimpleNamespace(free=required - 1)), self.assertRaisesRegex(ValueError, 'selected inventory, sources, and documents'):
                self.read(base / 'disk', staged, published, github, source_quarters=['2026Q1'])
            self.assertFalse(set(self.downloads(github)) & self.inventory_parts(github))


if __name__ == '__main__':
    unittest.main()
