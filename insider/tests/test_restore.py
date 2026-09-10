import gzip
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
import zipfile

from insider_pipeline.audit_batches import SELECTION_COLUMNS, file_hash, readonly, run
from insider_pipeline.inventory import canonical
from insider_pipeline.package import package
from insider_pipeline.restore import records, restore
import test_package as package_fixtures


class RestoreTests(unittest.TestCase):
    def test_fresh_restore_reproduces_every_row_and_original_audit(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            root, audit, output, result = package_fixtures.PackageTests().setup_package(base)
            target = base / 'restored'
            report = restore(output, target, result['manifest_sha256'])
            self.assertEqual(report['restored_documents'], 2)
            self.assertTrue(report['database_readback_verified'])
            self.assertEqual(report['archive_scope'], 'selection_only')
            original, restored = readonly(root / 'inventory.sqlite3'), readonly(target / 'inventory.sqlite3')
            try:
                self.assertEqual([dict(r) for r in original.execute('SELECT * FROM filings ORDER BY accession')],
                                 [dict(r) for r in restored.execute('SELECT * FROM filings ORDER BY accession')])
                self.assertEqual(restored.execute("SELECT value FROM settings WHERE key='complete_backfill'").fetchone()[0], 'false')
            finally:
                original.close(); restored.close()
            checked = run(target, base / 'restored-audit', 1)
            self.assertEqual(checked['semantic_sha256'], json.loads((audit / 'report.json').read_text())['semantic_sha256'])

    def test_pin_and_existing_destination_protect_other_files(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            root, audit, output, result = package_fixtures.PackageTests().setup_package(base)
            target = base / 'restored'; target.mkdir(); (target / 'keep.txt').write_text('keep')
            with self.assertRaisesRegex(ValueError, 'already exist'):
                restore(output, target, result['manifest_sha256'])
            self.assertEqual((target / 'keep.txt').read_text(), 'keep')
            with self.assertRaisesRegex(ValueError, 'pinned checksum'):
                restore(output, base / 'wrong-pin', '0' * 64)
            self.assertFalse((base / 'wrong-pin').exists())

    def test_discovery_exception_is_preserved_without_fake_restored_document(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            root, audit, output, result = package_fixtures.PackageTests().setup_package(base)
            from insider_pipeline.inventory import connect
            db = connect(root)
            db.execute("INSERT INTO filings(accession,form,filing_date,discovery_issues) VALUES(?,?,?,?)",
                       ('0001234567-26-000002', '4', '2026-01-03', '["missing original"]'))
            db.commit(); db.close()
            output = base / 'with-exception'
            result = package(root, audit, output)
            target = base / 'restored'
            report = restore(output, target, result['manifest_sha256'])
            self.assertEqual(report['discovery_exceptions_preserved'], 1)
            self.assertEqual(report['restored_documents'], 2)
            catalog = json.loads((target / 'archive-catalog.json').read_text())
            self.assertEqual(catalog['discovery_exceptions'][0]['inventory']['status'], 'pending')

    def test_unsafe_shard_cannot_write_outside_fresh_destination(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            root, audit, output, result = package_fixtures.PackageTests().setup_package(base, max_documents=2)
            manifest = json.loads((output / 'manifest.json').read_text())
            chunk = manifest['chunks'][0]; path = output / chunk['file']
            with zipfile.ZipFile(path) as archive:
                payloads = [(info, archive.read(info.filename)) for info in archive.infolist()]
            value = json.loads(gzip.decompress(payloads[0][1]))
            value['inventory']['shard'] = '../outside.sqlite3'
            payloads[0] = (payloads[0][0], gzip.compress(canonical(value), mtime=0))
            with zipfile.ZipFile(path, 'w') as archive:
                for info, body in payloads:
                    archive.writestr(info, body)
            chunk['sha256'], chunk['bytes'] = file_hash(path), path.stat().st_size
            digest = hashlib.sha256()
            for inventory, document in records(output, manifest):
                digest.update(canonical({k: inventory[k] for k in SELECTION_COLUMNS}) + b'\n')
            manifest['selection_sha256'] = digest.hexdigest()
            catalog_path = output / 'collection-catalog.json'
            catalog = json.loads(catalog_path.read_text()); catalog['selection_sha256'] = digest.hexdigest()
            catalog_path.write_bytes(canonical(catalog))
            for asset in manifest['assets']:
                if asset['kind'] == 'collection_catalog':
                    asset['sha256'], asset['bytes'] = file_hash(catalog_path), catalog_path.stat().st_size
            (output / 'manifest.json').write_bytes(canonical(manifest))
            with self.assertRaisesRegex(ValueError, 'Unsafe restored shard'):
                restore(output, base / 'restored', file_hash(output / 'manifest.json'))
            self.assertFalse((base / 'outside.sqlite3').exists())
            self.assertFalse((base / 'restored').exists())


if __name__ == '__main__':
    unittest.main()
