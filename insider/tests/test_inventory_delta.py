import gzip
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from insider_pipeline.audit_batches import file_hash, readonly
from insider_pipeline.inventory import canonical, connect
from insider_pipeline.inventory_archive import capture_snapshot
from insider_pipeline.inventory_delta import MANIFEST, PRIMARY_KEYS, build, finish_state, restore, verify
import test_inventory_archive as fixtures


class InventoryDeltaTests(unittest.TestCase):
    def setup(self, base):
        root = base / 'state'
        fixtures.InventoryArchiveTests().seed(root)
        parent = base / 'parent.sqlite3'
        capture_snapshot(root, parent)
        return root, parent

    def mutate(self, root):
        db = connect(root)
        db.execute("UPDATE settings SET value='2026-09-11' WHERE key='window_end'")
        db.execute('INSERT INTO settings VALUES(?,?)', ('unicode_note', 'Amendment – 修正'))
        db.execute("UPDATE sources SET filing_count=99")
        db.execute("UPDATE filings SET attempts=7,retry_after=1789009999.125,last_error=?,bulk_metadata=? WHERE filing_date='2018-01-02'",
                   ('A retained discrepancy', b'\x00\xff\x01binary\x00'))
        db.execute("DELETE FROM filings WHERE status='retry'")
        for index in range(30):
            accession = f'0001234567-26-{100 + index:06}'
            db.execute('INSERT INTO filings(accession,form,filing_date,bulk_metadata,source_url) VALUES(?,?,?,?,?)',
                       (accession, '4/A', '2026-09-10', bytes(range(256)), 'https://www.sec.gov/' + accession + '.txt'))
        db.execute("UPDATE index_sources SET bytes=100,retrieved_at='2026-09-11'")
        db.execute("DELETE FROM index_membership")
        db.execute("INSERT INTO index_membership VALUES('2026Q3','0001234567-26-000100')")
        db.execute("UPDATE index_observations SET filing_date='2026-01-05'")
        db.execute("INSERT INTO index_observations VALUES('2026Q3','0001234567-26-000100','4/A','2026-09-10','https://www.sec.gov/new')")
        db.commit(); db.close()

    def assert_rows_equal(self, first, second):
        a, b = readonly(first), readonly(second)
        try:
            for table, keys in PRIMARY_KEYS.items():
                query = 'SELECT * FROM ' + table + ' ORDER BY ' + ','.join(keys)
                self.assertEqual([dict(row) for row in a.execute(query)], [dict(row) for row in b.execute(query)], table)
        finally:
            a.close(); b.close()

    def forge_operations(self, output, change):
        manifest = json.loads((output / MANIFEST).read_text())
        operations = [json.loads(line) for line in gzip.decompress(b''.join((output / part['file']).read_bytes() for part in manifest['parts'])).splitlines()]
        change(operations)
        raw = b''.join(canonical(operation) + b'\n' for operation in operations)
        compressed = gzip.compress(raw, mtime=0)
        digest = hashlib.sha256(compressed).hexdigest()
        name = f'inventory-delta-00001-{digest[:16]}.gz.part'
        for part in manifest['parts']:
            (output / part['file']).unlink()
        (output / name).write_bytes(compressed)
        manifest['parts'] = [{'file': name, 'bytes': len(compressed), 'sha256': digest}]
        manifest['raw_bytes'] = len(raw); manifest['raw_sha256'] = hashlib.sha256(raw).hexdigest()
        (output / MANIFEST).write_bytes(canonical(manifest))
        return file_hash(output / MANIFEST)

    def test_chunked_roundtrip_preserves_every_table_and_binary_value(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); root, parent = self.setup(base); parent_hash = file_hash(parent)
            self.mutate(root)
            target = base / 'target.sqlite3'; capture_snapshot(root, target)
            output = base / 'delta'
            manifest = build(parent, target, output, max_bytes=500)
            self.assertGreater(len(manifest['parts']), 1)
            self.assertTrue(all(part['bytes'] <= 500 for part in manifest['parts']))
            self.assertEqual(manifest['operations']['filings'], {'insert': 30, 'update': 1, 'delete': 1})
            self.assertTrue(all(sum(counts.values()) > 0 for counts in manifest['operations'].values()))
            restored = base / 'restored'
            report = restore(parent, output, restored, file_hash(output / MANIFEST))
            self.assertTrue(report['every_inventory_row_verified'])
            self.assertFalse(report['includes_original_documents'])
            self.assertFalse(report['collection_resume_ready'])
            self.assertFalse(report['complete_backfill'])
            self.assert_rows_equal(target, restored / 'inventory.sqlite3')
            self.assertEqual(file_hash(parent), parent_hash)
            db = readonly(restored / 'inventory.sqlite3')
            try:
                row = db.execute("SELECT bulk_metadata,retry_after FROM filings WHERE attempts=7").fetchone()
                self.assertEqual(row['bulk_metadata'], b'\x00\xff\x01binary\x00')
                self.assertEqual(row['retry_after'], 1789009999.125)
            finally:
                db.close()

    def test_no_change_and_chained_replay_use_logical_parent_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); root, parent = self.setup(base)
            empty = base / 'empty'; manifest = build(parent, parent, empty)
            self.assertEqual(manifest['raw_bytes'], 0)
            self.assertEqual(manifest['parent'], manifest['target'])
            self.mutate(root)
            first = base / 'first'; build(parent, root / 'inventory.sqlite3', first)
            one = base / 'one'; restore(parent, first, one, file_hash(first / MANIFEST))
            capture_snapshot(root, base / 'middle.sqlite3')
            db = connect(root); db.execute("INSERT INTO settings VALUES('second_day','ready')"); db.commit(); db.close()
            second = base / 'second'; build(base / 'middle.sqlite3', root / 'inventory.sqlite3', second)
            two = base / 'two'; restore(one / 'inventory.sqlite3', second, two, file_hash(second / MANIFEST))
            self.assert_rows_equal(root / 'inventory.sqlite3', two / 'inventory.sqlite3')
            with self.assertRaisesRegex(ValueError, 'parent state'):
                restore(parent, second, base / 'skipped-day', file_hash(second / MANIFEST))
            with self.assertRaisesRegex(ValueError, 'parent state'):
                restore(one / 'inventory.sqlite3', first, base / 'duplicate-day', file_hash(first / MANIFEST))

    def test_snapshot_transactions_ignore_later_live_writes(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); root, parent = self.setup(base)
            writer = connect(root); changes = []
            def progress(item):
                if not changes:
                    writer.execute("UPDATE filings SET last_error='later live change'")
                    writer.commit(); changes.append(True)
            output = base / 'delta'
            manifest = build(parent, root / 'inventory.sqlite3', output, progress=progress)
            writer.close()
            self.assertTrue(changes)
            self.assertEqual(manifest['parent'], manifest['target'])
            restored = base / 'restored'; restore(parent, output, restored, file_hash(output / MANIFEST))
            self.assert_rows_equal(parent, restored / 'inventory.sqlite3')

    def test_bad_pin_corrupt_asset_or_wrong_parent_leaves_no_destination(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); root, parent = self.setup(base); self.mutate(root)
            output = base / 'delta'; manifest = build(parent, root / 'inventory.sqlite3', output)
            target = base / 'restored'; pin = file_hash(output / MANIFEST)
            with self.assertRaisesRegex(ValueError, 'pinned checksum'):
                restore(parent, output, target, '0' * 64)
            with self.assertRaisesRegex(ValueError, 'parent state'):
                restore(root / 'inventory.sqlite3', output, target, pin)
            part = output / manifest['parts'][0]['file']
            part.write_bytes(part.read_bytes() + b'bad')
            with self.assertRaisesRegex(ValueError, 'checksum'):
                restore(parent, output, target, pin)
            self.assertFalse(target.exists())

    def test_duplicate_missing_and_incorrect_operations_cannot_pass_replay(self):
        cases = {
            'duplicate': lambda ops: ops.insert(0, ops[0]),
            'missing': lambda ops: ops.pop(),
            'wrong old row': lambda ops: ops[0].update(before_sha256='0' * 64),
            'wrong value': lambda ops: next(op for op in ops if op['table'] == 'settings' and op['after'])['after'].update(value='altered'),
            'wrong key': lambda ops: ops[0].update(key=['absent']),
        }
        for name, change in cases.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                base = Path(directory); root, parent = self.setup(base); self.mutate(root)
                output = base / 'delta'; build(parent, root / 'inventory.sqlite3', output)
                pin = self.forge_operations(output, change)
                with self.assertRaises(ValueError):
                    restore(parent, output, base / 'failed', pin)
                self.assertFalse((base / 'failed').exists())

    def test_wrong_target_hash_and_unsafe_part_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); root, parent = self.setup(base); self.mutate(root)
            output = base / 'delta'; manifest = build(parent, root / 'inventory.sqlite3', output)
            manifest['target']['tables']['settings']['sha256'] = '0' * 64
            manifest['target'] = finish_state(manifest['target']['schema_sha256'], manifest['target']['tables'])
            (output / MANIFEST).write_bytes(canonical(manifest))
            with self.assertRaisesRegex(ValueError, 'complete target state'):
                restore(parent, output, base / 'failed', file_hash(output / MANIFEST))
            self.assertFalse((base / 'failed').exists())
            manifest['parts'][0]['file'] = '../outside'
            (output / MANIFEST).write_bytes(canonical(manifest))
            with self.assertRaisesRegex(ValueError, 'Unsafe'):
                verify(output, file_hash(output / MANIFEST))

    def test_schema_changes_require_full_baseline_and_existing_outputs_are_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); root, parent = self.setup(base)
            db = connect(root); db.execute('ALTER TABLE filings ADD COLUMN extra TEXT'); db.commit(); db.close()
            with self.assertRaisesRegex(ValueError, 'schema changed'):
                build(parent, root / 'inventory.sqlite3', base / 'changed')
            self.assertFalse((base / 'changed').exists())
            db = sqlite3.connect(parent); db.execute('CREATE TABLE unhandled(key TEXT PRIMARY KEY)'); db.commit(); db.close()
            with self.assertRaisesRegex(ValueError, 'supported tables'):
                build(parent, parent, base / 'unknown')
            target = base / 'existing'; target.mkdir(); (target / 'keep').write_text('retained')
            with self.assertRaisesRegex(ValueError, 'must not exist'):
                build(parent, parent, target)
            with self.assertRaisesRegex(ValueError, 'must not exist'):
                restore(parent, base / 'missing', target, '0' * 64)
            self.assertEqual((target / 'keep').read_text(), 'retained')


if __name__ == '__main__':
    unittest.main()
