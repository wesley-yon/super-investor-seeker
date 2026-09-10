import json
from pathlib import Path
import tempfile
import threading
import unittest

from insider_pipeline.audit_batches import file_hash, run as audit
from insider_pipeline.baseline import build, restore
from insider_pipeline.inventory import connect
from insider_pipeline.inventory_archive import archive
from insider_pipeline.package import package
from insider_pipeline.runner import run as collect
import test_audit_batches as fixtures
import test_backfill as backfill_fixtures


class BaselineTests(unittest.TestCase):
    def setup(self, base, audit_limit=0, change_live=False):
        root, inventory, documents = base / 'state', base / 'inventory', base / 'documents'
        root.mkdir(); fixtures.AuditBatchTests().seed(root)
        db = connect(root)
        db.execute('INSERT INTO filings(accession,form,filing_date,source_url,status,attempts) VALUES(?,?,?,?,?,?)',
                   ('0001234567-26-000002', '4', '2026-01-03', 'https://www.sec.gov/0001234567-26-000002.txt', 'inflight', 1))
        db.commit(); db.close()
        archive(root, inventory)
        snapshot = inventory / 'inventory-snapshot.sqlite3'
        if change_live:
            db = connect(root)
            db.execute("UPDATE filings SET attempts=99 WHERE status='verified'"); db.commit(); db.close()
        audit(root, base / 'audit', workers=1, limit=audit_limit, inventory_snapshot=snapshot)
        package(root, base / 'audit', documents)
        return root, inventory, documents

    def test_matching_checkpoint_restores_and_resumes_only_uncollected_filing(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            root, inventory, documents = self.setup(base, change_live=True)
            output, target = base / 'baseline', base / 'restored'
            report = build(inventory, documents, output)
            restored = restore(output, target, report['baseline_sha256'])
            self.assertTrue(restored['collection_resume_ready'])
            self.assertFalse(restored['complete_backfill'])
            self.assertEqual(restored['queue_counts_at_checkpoint'], {'inflight': 1, 'verified': 2})
            self.assertEqual(file_hash(target / 'inventory.sqlite3'), file_hash(inventory / 'inventory-snapshot.sqlite3'))
            db = connect(target)
            self.assertEqual(db.execute("SELECT max(attempts) FROM filings WHERE status='verified'").fetchone()[0], 0)
            db.close()
            source = backfill_fixtures.BackfillTests().source()
            class Client:
                requests = 0
                download_bytes = 0
                blocked = threading.Event()
                def get(self, url):
                    self.requests += 1
                    result = source.replace(backfill_fixtures.ACCESSION.encode(), Path(url).stem.encode())
                    self.download_bytes += len(result)
                    return result
            client = Client()
            resumed = collect(target, client, workers=1)
            self.assertEqual(resumed['counts'], {'verified': 3})
            self.assertEqual(client.requests, 1)
            self.assertEqual(build(inventory, documents, output), report)

    def test_partial_document_selection_cannot_become_full_checkpoint(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            root, inventory, documents = self.setup(base, audit_limit=1)
            with self.assertRaisesRegex(ValueError, 'same complete collected checkpoint'):
                build(inventory, documents, base / 'baseline')
            self.assertFalse((base / 'baseline' / 'baseline.json').exists())

    def test_changed_frozen_inventory_cannot_reuse_audit(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            root, inventory, documents = self.setup(base)
            snapshot = inventory / 'inventory-snapshot.sqlite3'
            import sqlite3
            db = sqlite3.connect(snapshot)
            db.execute("UPDATE filings SET attempts=88 WHERE status='verified'"); db.commit(); db.close()
            with self.assertRaisesRegex(ValueError, 'Frozen inventory changed'):
                package(root, base / 'audit', base / 'changed-package')
            with self.assertRaisesRegex(ValueError, 'different frozen inventory'):
                audit(root, base / 'audit', workers=1, inventory_snapshot=snapshot)

    def test_existing_destination_or_missing_asset_cannot_pass_restoration(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            root, inventory, documents = self.setup(base)
            output = base / 'baseline'; report = build(inventory, documents, output)
            target = base / 'restored'; target.mkdir(); (target / 'keep').write_text('keep')
            with self.assertRaisesRegex(ValueError, 'must not exist'):
                restore(output, target, report['baseline_sha256'])
            self.assertEqual((target / 'keep').read_text(), 'keep')
            manifest = json.loads((output / 'manifest.json').read_text())
            (output / manifest['chunks'][0]['file']).unlink()
            with self.assertRaises((ValueError, FileNotFoundError)):
                restore(output, base / 'incomplete', report['baseline_sha256'])
            self.assertFalse((base / 'incomplete').exists())


if __name__ == '__main__':
    unittest.main()
