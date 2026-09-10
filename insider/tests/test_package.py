import gzip
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
import zipfile

from insider_pipeline.audit_batches import run
from insider_pipeline.inventory import canonical
from insider_pipeline.package import decode_row, encode_row, package, verify
import test_audit_batches as audit_fixtures


class PackageTests(unittest.TestCase):
    def setup_package(self, base, max_documents=1):
        root, audit, output = base / 'state', base / 'audit', base / 'package'
        root.mkdir()
        audit_fixtures.AuditBatchTests().seed(root)
        run(root, audit, workers=1)
        result = package(root, audit, output, max_documents=max_documents)
        return root, audit, output, result

    def test_complete_round_trip_chunking_resume_and_manifest_pin(self):
        with tempfile.TemporaryDirectory() as directory:
            root, audit, output, result = self.setup_package(Path(directory))
            self.assertEqual(result['documents'], 2)
            self.assertEqual(result['chunks'], 2)
            self.assertTrue(result['verified'])
            self.assertFalse(result['complete_backfill'])
            resumed = package(root, audit, output, max_documents=1)
            self.assertEqual(result, resumed)
            independent = package(root, audit, Path(directory) / 'second-package', max_documents=1)
            self.assertEqual(result['manifest_sha256'], independent['manifest_sha256'])
            self.assertEqual(verify(output, result['manifest_sha256']), result)
            with self.assertRaises(ValueError):
                verify(output, '0' * 64)
            with self.assertRaises(ValueError):
                package(root, audit, output, max_documents=2)
            self.assertEqual(decode_row(encode_row({'blob': b'\x00\xff\n', 'text': '', 'null': None})),
                             {'blob': b'\x00\xff\n', 'text': '', 'null': None})

    def test_single_document_cannot_overrun_asset_budget(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); root = base / 'state'; root.mkdir()
            audit_fixtures.AuditBatchTests().seed(root)
            run(root, base / 'audit', workers=1)
            with self.assertRaisesRegex(ValueError, 'One filing exceeds'):
                package(root, base / 'audit', base / 'package', max_bytes=500)
            self.assertFalse((base / 'package' / 'manifest.json').exists())

    def test_inner_source_corruption_caught_even_with_matching_chunk_hash(self):
        with tempfile.TemporaryDirectory() as directory:
            root, audit, output, result = self.setup_package(Path(directory), max_documents=2)
            manifest = json.loads((output / 'manifest.json').read_text())
            chunk = manifest['chunks'][0]; path = output / chunk['file']
            with zipfile.ZipFile(path) as archive:
                records = [(info, archive.read(info.filename)) for info in archive.infolist()]
            envelope = json.loads(gzip.decompress(records[0][1]))
            document = decode_row(envelope['document'])
            document['source_gzip'] = gzip.compress(b'changed source', mtime=0)
            envelope['document'] = encode_row(document)
            records[0] = (records[0][0], gzip.compress(canonical(envelope), mtime=0))
            with zipfile.ZipFile(path, 'w') as archive:
                for info, body in records:
                    archive.writestr(info, body)
            chunk['sha256'] = hashlib.sha256(path.read_bytes()).hexdigest(); chunk['bytes'] = path.stat().st_size
            (output / 'manifest.json').write_bytes(canonical(manifest))
            with self.assertRaisesRegex(ValueError, 'Archived source'):
                verify(output)

    def test_unsafe_asset_path_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root, audit, output, result = self.setup_package(Path(directory))
            manifest = json.loads((output / 'manifest.json').read_text())
            manifest['assets'][0]['file'] = '../outside.zip'
            (output / 'manifest.json').write_bytes(canonical(manifest))
            with self.assertRaisesRegex(ValueError, 'unsafe'):
                verify(output)
