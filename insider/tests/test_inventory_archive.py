from pathlib import Path
import tempfile
import unittest

from insider_pipeline.audit_batches import file_hash, readonly
from insider_pipeline.inventory import canonical, connect
from insider_pipeline.inventory_archive import archive, capture_snapshot, describe, restore, verify_parts
import test_audit_batches as fixtures


class InventoryArchiveTests(unittest.TestCase):
    def seed(self, root):
        root.mkdir()
        fixtures.AuditBatchTests().seed(root)
        db = connect(root)
        db.executescript('''CREATE TABLE index_sources(source_key TEXT PRIMARY KEY,url TEXT,sha256 TEXT,bytes INTEGER,retrieved_at TEXT,filing_count INTEGER);
            CREATE TABLE index_membership(source_key TEXT,accession TEXT,PRIMARY KEY(source_key,accession));
            CREATE TABLE index_observations(source_key TEXT,accession TEXT,form TEXT,filing_date TEXT,source_url TEXT,PRIMARY KEY(source_key,accession));''')
        db.execute("INSERT INTO filings(accession,form,filing_date,status,attempts,retry_after,last_error,discovery_issues) VALUES(?,?,?,?,?,?,?,?)",
                   ('0001234567-26-000002', '4', '2026-01-03', 'retry', 3, 1789009999.5, 'original unavailable', '["index disagreement"]'))
        db.execute("INSERT INTO index_sources VALUES('2026Q1','https://www.sec.gov/index','abc',42,'2026-09-10',1)")
        db.execute("INSERT INTO index_membership VALUES('2026Q1','0001234567-26-000002')")
        db.execute("INSERT INTO index_observations VALUES('2026Q1','0001234567-26-000002','4','2026-01-04','https://www.sec.gov/example')")
        db.commit(); db.close()

    def test_chunked_roundtrip_preserves_full_queue_and_discovery_state(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); root, output = base / 'state', base / 'archive'
            self.seed(root)
            manifest = archive(root, output, max_bytes=2000)
            self.assertGreater(len(manifest['parts']), 1)
            self.assertTrue(all(p['bytes'] <= 2000 for p in manifest['parts']))
            self.assertEqual(manifest['queue_counts'], {'retry': 1, 'verified': 2})
            self.assertEqual(manifest['tables']['index_observations'], 1)
            target = base / 'restored'
            restored = restore(output, target, file_hash(output / 'inventory-manifest.json'))
            self.assertTrue(restored['byte_identical'])
            self.assertFalse(restored['includes_original_documents'])
            self.assertEqual(file_hash(output / 'inventory-snapshot.sqlite3'), file_hash(target / 'inventory.sqlite3'))
            a, b = readonly(root / 'inventory.sqlite3'), readonly(target / 'inventory.sqlite3')
            try:
                for table in manifest['tables']:
                    self.assertEqual([tuple(r) for r in a.execute('SELECT * FROM ' + table)],
                                     [tuple(r) for r in b.execute('SELECT * FROM ' + table)])
            finally:
                a.close(); b.close()
            self.assertEqual(archive(root, output, max_bytes=2000), manifest)
            with self.assertRaisesRegex(ValueError, 'different inventory root'):
                archive(base / 'other-state', output, max_bytes=2000)

    def test_live_writes_do_not_change_in_progress_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); root = base / 'state'; self.seed(root)
            db = connect(root)
            db.execute('INSERT INTO settings VALUES(?,?)', ('large_fixture', 'x' * 12_000_000))
            db.commit()
            changed = []
            def on_pages(status, remaining, total):
                if not changed:
                    self.assertGreater(remaining, 0)
                    db.execute("UPDATE filings SET status='pending' WHERE status='retry'")
                    db.commit(); changed.append(True)
            capture_snapshot(root, base / 'snapshot.sqlite3', progress=on_pages)
            self.assertTrue(changed)
            self.assertEqual(describe(base / 'snapshot.sqlite3')['queue_counts'], {'retry': 1, 'verified': 2})
            self.assertEqual(dict(db.execute('SELECT status,count(*) FROM filings GROUP BY status')), {'pending': 1, 'verified': 2})
            db.close()

    def test_corrupt_part_or_wrong_pin_leaves_no_restore(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); root, output = base / 'state', base / 'archive'; self.seed(root)
            manifest = archive(root, output)
            target = base / 'restored'
            with self.assertRaisesRegex(ValueError, 'pinned checksum'):
                restore(output, target, '0' * 64)
            part = output / manifest['parts'][0]['file']
            body = bytearray(part.read_bytes()); body[len(body) // 2] ^= 1; part.write_bytes(body)
            with self.assertRaisesRegex(ValueError, 'checksum'):
                restore(output, target, file_hash(output / 'inventory-manifest.json'))
            self.assertFalse(target.exists())

    def test_outside_part_path_and_existing_destination_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); root, output = base / 'state', base / 'archive'; self.seed(root)
            manifest = archive(root, output)
            target = base / 'restored'; target.mkdir(); (target / 'keep').write_text('keep')
            with self.assertRaisesRegex(ValueError, 'must not exist'):
                restore(output, target, file_hash(output / 'inventory-manifest.json'))
            self.assertEqual((target / 'keep').read_text(), 'keep')
            manifest['parts'][0]['file'] = '../outside'
            (output / 'inventory-manifest.json').write_bytes(canonical(manifest))
            with self.assertRaisesRegex(ValueError, 'Unsafe'):
                verify_parts(output)


if __name__ == '__main__':
    unittest.main()
