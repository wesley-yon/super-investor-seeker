import contextlib
import gzip
import io
import json
from pathlib import Path
import shutil
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from insider_pipeline import document_index as index
from insider_pipeline.audit_batches import readonly
from insider_pipeline.increment import MANIFEST, build as increment
from insider_pipeline.inventory import canonical, connect
from insider_pipeline.inventory_archive import capture_snapshot
import test_github_chain as fixtures


class DocumentIndexTests(unittest.TestCase):
    def seed(self, base):
        with contextlib.redirect_stdout(io.StringIO()):
            root, target, bundle, pin, parent, github = fixtures.GitHubChainTests().seed(base)
            result = index.build(bundle / MANIFEST, pin, target, base / 'index', [base / 'baseline/baseline.json'])
        return root, target, bundle, pin, result, parent, github

    def test_complete_index_resolves_latest_and_inherited_full_document_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); root, target, bundle, pin, result, _, _ = self.seed(base)
            chain = index.local_chain(bundle / MANIFEST, pin, [base / 'baseline/baseline.json'])
            value = index.validate_manifest(index.pinned_json(base / 'index' / index.MANIFEST,
                                                              result['document_index_manifest_sha256']), chain)
            self.assertEqual(value['committed_documents'], 3)
            self.assertFalse(value['complete_backfill'])
            index.verify_inventory(base / 'index' / value['index_file']['file'], value, target)
            nodes = {node['reference']['manifest_sha256']: node for node in chain}
            db = readonly(target)
            try:
                for accession, row_hash, number, compressed, decoded in index.records(base / 'index' / value['index_file']['file'], value):
                    chunk = value['chunks'][number]
                    if accession == '0001234567-18-000001':
                        self.assertEqual(chunk['checkpoint_sha256'], pin)
                    if accession == '0001234567-26-000001':
                        self.assertNotEqual(chunk['checkpoint_sha256'], pin)
                    with zipfile.ZipFile(nodes[chunk['checkpoint_sha256']]['directory'] / chunk['file']) as archive:
                        inventory, document, _, _ = index.member(archive, accession, compressed, decoded)
                    expected = dict(db.execute('SELECT * FROM filings WHERE accession=?', (accession,)).fetchone())
                    self.assertEqual(inventory, expected)
                    self.assertEqual(index.row_hash(inventory), row_hash)
                    original = readonly(root / expected['shard'])
                    self.assertEqual(document, dict(original.execute('SELECT * FROM documents WHERE accession=?', (accession,)).fetchone()))
                    original.close()
            finally:
                db.close()

    def test_extension_reads_no_old_chunks_and_matches_a_fresh_full_index_byte_for_byte(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); root, target, bundle, pin, first, _, _ = self.seed(base)
            db = connect(root)
            db.execute("UPDATE filings SET attempts=9 WHERE accession='0001234567-26-000001'")
            db.execute("DELETE FROM filings WHERE accession='0001234567-18-000001'")
            db.commit(); db.close()
            next_target = base / 'next.sqlite3'; capture_snapshot(root, next_target)
            with contextlib.redirect_stdout(io.StringIO()):
                next_manifest = increment(root, bundle / MANIFEST, pin, target, next_target, base / 'next', workers=1)
            ancestors = [base / 'baseline/baseline.json', bundle / MANIFEST]
            hidden = []
            for folder in (base / 'baseline', bundle):
                for chunk in folder.glob('filings-*.zip'):
                    hidden.append(chunk); chunk.rename(chunk.with_suffix('.unavailable'))
            extended = index.build(base / 'next' / MANIFEST, next_manifest['increment_manifest_sha256'],
                                   next_target, base / 'extended', ancestors,
                                   base / 'index' / index.MANIFEST, first['document_index_manifest_sha256'])
            self.assertEqual(extended['committed_documents'], 2)
            self.assertTrue(extended['indexed_from_previous_checkpoint'])
            self.assertEqual(extended['new_checkpoint_chunks_read'], 1)
            for chunk in hidden:
                chunk.with_suffix('.unavailable').rename(chunk)
            full = index.build(base / 'next' / MANIFEST, next_manifest['increment_manifest_sha256'],
                               next_target, base / 'full', ancestors)
            self.assertEqual(full['document_index_manifest_sha256'], extended['document_index_manifest_sha256'])
            for name in (index.MANIFEST, full['index_file']['file']):
                self.assertEqual((base / 'extended' / name).read_bytes(), (base / 'full' / name).read_bytes())

    def test_wrong_scope_missing_ancestor_and_missing_document_never_publish_index(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); _, target, bundle, pin, _, _, _ = self.seed(base)
            changed = base / 'changed.sqlite3'; shutil.copyfile(target, changed)
            db = sqlite3.connect(changed)
            db.execute("UPDATE filings SET attempts=17 WHERE status='pending'"); db.commit(); db.close()
            with self.assertRaisesRegex(ValueError, 'Frozen index inventory'):
                index.build(bundle / MANIFEST, pin, changed, base / 'bad-state', [base / 'baseline/baseline.json'])
            with self.assertRaisesRegex(ValueError, 'complete acyclic checkpoint chain'):
                index.build(bundle / MANIFEST, pin, target, base / 'no-parent')
            chunk = next(bundle.glob('filings-*.zip')); chunk.write_bytes(b'corrupt')
            with self.assertRaisesRegex(ValueError, 'source chunk bytes differ'):
                index.build(bundle / MANIFEST, pin, target, base / 'bad-document', [base / 'baseline/baseline.json'])
            for name in ('bad-state', 'no-parent', 'bad-document'):
                self.assertFalse((base / name).exists())

    def test_complete_membership_raw_hash_lengths_and_asset_bounds_are_enforced(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); _, target, bundle, pin, result, _, _ = self.seed(base)
            value = json.loads((base / 'index' / index.MANIFEST).read_text())
            path = base / 'index' / value['index_file']['file']
            for field, replacement in [('raw_sha256', '0' * 64), ('raw_bytes', 1), ('bytes', 1)]:
                damaged = json.loads(canonical(value)); damaged['index_file'][field] = replacement
                with self.subTest(field=field), self.assertRaises(ValueError):
                    list(index.records(path, damaged))
            damaged = json.loads(canonical(value)); damaged['committed_documents'] -= 1
            with self.assertRaises(ValueError):
                index.verify_inventory(path, damaged, target)
            with patch('insider_pipeline.document_index.MAX_INDEX_BYTES', 1), self.assertRaisesRegex(ValueError, 'index file has invalid'):
                index.build(bundle / MANIFEST, pin, target, base / 'too-small', [base / 'baseline/baseline.json'])
            self.assertFalse((base / 'too-small').exists())
            self.assertEqual(index.pinned_json(base / 'index' / index.MANIFEST, result['document_index_manifest_sha256']), value)

    def test_private_selection_and_decompression_require_bounded_exact_values(self):
        pin = 'a' * 64
        first, second = '0001234567-18-000001', '0001234567-26-000001'
        self.assertEqual(index.selection(pin, [second, first])['accessions'], [first, second])
        for values in ([], [first, first], ['../bad'], [first] * 1001):
            with self.assertRaises(ValueError):
                index.selection(pin, values)
        with self.assertRaises(ValueError):
            index.selection('not-a-pin', [first])
        for maximum, expected in ((3, None), (10, 3), (3, 4)):
            with self.assertRaises(ValueError):
                index.bounded_gzip(gzip.compress(b'abcd'), maximum, expected)
        self.assertEqual(index.bounded_gzip(gzip.compress(b'abcd'), 10, 4), b'abcd')

    def test_inventory_mutation_during_index_construction_prevents_publication(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); _, target, bundle, pin, _, _, _ = self.seed(base)
            original = index.verify_inventory
            def change_after_verification(*args):
                original(*args)
                db = sqlite3.connect(target)
                db.execute("UPDATE filings SET attempts=attempts+1 WHERE status='pending'")
                db.commit(); db.close()
            with patch('insider_pipeline.document_index.verify_inventory', side_effect=change_after_verification), self.assertRaisesRegex(ValueError, 'inventory changed during construction'):
                index.build(bundle / MANIFEST, pin, target, base / 'mutating', [base / 'baseline/baseline.json'])
            self.assertFalse((base / 'mutating').exists())


if __name__ == '__main__':
    unittest.main()
