import contextlib
import io
from pathlib import Path
import shutil
import tempfile
import threading
import unittest
from unittest.mock import patch

from insider_pipeline.audit_batches import file_hash, readonly
from insider_pipeline.increment import build, restore
from insider_pipeline.inventory import connect
from insider_pipeline.inventory_archive import capture_snapshot
from insider_pipeline.runner import Shards, prepare, run
import test_backfill as collection_fixture
import test_increment as archive_fixture


class PartialCollectionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / 'state'
        self.db = connect(self.root)
        self.addCleanup(self.db.close)
        self.fixture = collection_fixture.BackfillTests()
        self.row = self.fixture.row()
        self.record = prepare(self.fixture.source(), self.row)

    def seed(self, relative, **values):
        row = {**self.row, 'shard': relative, **values}
        columns = list(row)
        self.db.execute('INSERT INTO filings(' + ','.join(columns) + ') VALUES(' +
                        ','.join('?' for _ in columns) + ')', tuple(row.values()))
        self.db.commit()
        return dict(self.db.execute('SELECT * FROM filings WHERE accession=?', (row['accession'],)).fetchone())

    def shards(self):
        shards = Shards(self.root, self.db)
        self.addCleanup(shards.close)
        return shards

    def test_inventory_files_and_orphan_sidecars_all_reserve_slots(self):
        self.seed('shards/2026-09-0012.sqlite3')
        directory = self.root / 'shards'; directory.mkdir()
        old = directory / '2026-09-0007.sqlite3'; old.write_bytes(b'archived bytes')
        orphan = directory / '2026-09-0020.sqlite3-wal'; orphan.write_bytes(b'orphan bytes')
        shards = self.shards()
        chosen = shards.choose(self.row, self.record)
        self.assertEqual(chosen, 'shards/2026-09-0021.sqlite3')
        shards.save(chosen, self.record)
        self.assertEqual(old.read_bytes(), b'archived bytes')
        self.assertEqual(orphan.read_bytes(), b'orphan bytes')
        self.assertFalse((directory / '2026-09-0012.sqlite3').exists())

    def test_fresh_run_keeps_existing_files_and_rotates_only_its_own_writes(self):
        first = Shards(self.root)
        initial = first.choose(self.row, self.record)
        first.save(initial, self.record); first.close()
        original = (self.root / initial).read_bytes()
        second = self.shards()
        chosen = second.choose(self.row, self.record)
        self.assertEqual(chosen, 'shards/2026-09-0002.sqlite3')
        second.save(chosen, self.record)
        self.assertEqual(second.choose(self.row, self.record), chosen)
        with patch('insider_pipeline.runner.SHARD_LIMIT', 1):
            self.assertEqual(second.choose(self.row, self.record), 'shards/2026-09-0003.sqlite3')
        self.assertEqual((self.root / initial).read_bytes(), original)
        with self.assertRaisesRegex(ValueError, 'allocated in this collection run'):
            second.save(initial, self.record)

    def test_missing_uncommitted_intended_shard_does_not_create_empty_database(self):
        row = self.seed('shards/2026-09-0008.sqlite3', status='inflight')
        shards = self.shards()
        self.assertIsNone(shards.existing(row))
        self.assertFalse((self.root / row['shard']).exists())
        self.assertEqual(shards.choose(row, self.record), 'shards/2026-09-0009.sqlite3')

    def test_missing_committed_document_stops_before_network_or_empty_file(self):
        row = self.seed('shards/2026-09-0008.sqlite3', status='pending',
                        source_sha256=self.record['source_sha256'], parsed_sha256=self.record['parsed_sha256'])
        client = self.client()
        with contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(ValueError, 'must be restored before retry'):
                run(self.root, client, workers=1)
        self.assertEqual(client.requests, 0)
        self.assertFalse((self.root / row['shard']).exists())
        current = dict(self.db.execute('SELECT * FROM filings').fetchone())
        self.assertEqual(current, row)
        with self.assertRaisesRegex(ValueError, 'missing its inventory shard'):
            self.shards().existing({**row, 'shard': None})

    def test_partial_existing_shard_cannot_hide_a_missing_committed_document(self):
        shards = self.shards()
        relative = shards.choose(self.row, self.record)
        shards.save(relative, self.record)
        other = {**self.row, 'accession': '0001234567-26-999998', 'shard': relative,
                 'source_sha256': self.record['source_sha256']}
        with self.assertRaisesRegex(ValueError, 'must be restored before retry'):
            shards.existing(other)
        with self.assertRaisesRegex(ValueError, 'differs from inventory'):
            shards.existing({**self.row, 'shard': relative, 'source_sha256': '0' * 64})

    def test_crash_recovery_reads_durable_document_without_refetching_or_rewriting(self):
        row = self.seed(None, status='inflight')
        shards = Shards(self.root, self.db)
        relative = shards.choose(row, self.record)
        self.db.execute('UPDATE filings SET shard=?', (relative,)); self.db.commit()
        shards.save(relative, self.record); shards.close()
        before = (self.root / relative).read_bytes()
        client = self.client()
        with contextlib.redirect_stdout(io.StringIO()):
            result = run(self.root, client, workers=1)
        self.assertEqual(result['counts'], {'verified': 1})
        self.assertEqual(client.requests, 0)
        self.assertEqual((self.root / relative).read_bytes(), before)
        self.assertEqual(list((self.root / 'shards').glob('*.sqlite3')), [self.root / relative])

    def test_invalid_paths_slot_exhaustion_and_late_collision_fail_before_writes(self):
        shards = self.shards()
        for relative in ('../escape.sqlite3', 'shards/2026-13-0001.sqlite3',
                         'shards/2026-09-0000.sqlite3', 'shards/2026-09-10000.sqlite3', 7):
            with self.assertRaisesRegex(ValueError, 'Invalid collection shard path'):
                shards.reserve(relative)
        shards.reserve('shards/2026-09-9999.sqlite3')
        with self.assertRaisesRegex(ValueError, 'slots exhausted'):
            shards.choose(self.row, self.record)
        other = {**self.row, 'filing_date': '2026-10-01'}
        path = self.root / 'shards/2026-10-0001.sqlite3'
        path.write_bytes(b'another writer')
        with self.assertRaisesRegex(ValueError, 'another writer'):
            shards.choose(other, self.record)
        self.assertEqual(path.read_bytes(), b'another writer')

    def test_inventory_only_constructor_and_symlink_boundaries(self):
        self.seed('shards/2026-09-0027.sqlite3')
        shards = Shards(self.root)
        self.assertEqual(shards.choose(self.row, self.record), 'shards/2026-09-0028.sqlite3')
        shards.close()
        target = self.root / 'outside.sqlite3'; target.write_bytes(b'untouched')
        (self.root / 'shards/2026-09-0030.sqlite3').symlink_to(target)
        with self.assertRaisesRegex(ValueError, 'symlink'):
            self.shards().existing({**self.row, 'shard': 'shards/2026-09-0030.sqlite3'})
        self.assertEqual(target.read_bytes(), b'untouched')

    def client(self):
        source = self.fixture.source()
        original_accession = self.row['accession']

        class Client:
            requests = 0
            download_bytes = 0
            blocked = threading.Event()

            def get(self, url):
                self.requests += 1
                result = source.replace(original_accession.encode(), Path(url).stem.encode())
                self.download_bytes += len(result)
                return result

        return Client()

    def test_partial_inventory_collection_packages_and_restores_with_original_parent(self):
        base = Path(self.temporary.name) / 'roundtrip'; base.mkdir()
        helper = archive_fixture.IncrementTests()
        with contextlib.redirect_stdout(io.StringIO()):
            original, parent, parent_archive, parent_pin = helper.seed(base)
        db = readonly(parent)
        old_rows = [dict(row) for row in db.execute("SELECT * FROM filings WHERE status='verified'")]
        db.close()
        old_documents, old_hashes = {}, {}
        for old_row in old_rows:
            old_shard = original / old_row['shard']
            old_hashes[old_row['shard']] = file_hash(old_shard)
            old_db = readonly(old_shard)
            old_documents[old_row['accession']] = tuple(old_db.execute(
                'SELECT * FROM documents WHERE accession=?', (old_row['accession'],)).fetchone())
            old_db.close()
        partial = base / 'partial'
        shutil.copytree(original, partial, ignore=shutil.ignore_patterns('shards', 'inventory.sqlite3*'))
        shutil.copyfile(parent, partial / 'inventory.sqlite3')
        db = connect(partial)
        accession = '0001234567-18-000004'
        db.execute('INSERT INTO filings(accession,issuer_cik,form,filing_date,source_url) VALUES(?,?,?,?,?)',
                   (accession, 64040, '4', '2018-01-04', 'https://www.sec.gov/' + accession + '.txt'))
        db.commit(); db.close()
        client = self.client()
        with contextlib.redirect_stdout(io.StringIO()):
            collected = run(partial, client, workers=1, limit=1)
        self.assertEqual(collected['completed_this_run'], 1)
        self.assertEqual(client.requests, 1)
        for old_row in old_rows:
            self.assertFalse((partial / old_row['shard']).exists())
        db = readonly(partial / 'inventory.sqlite3')
        new_row = dict(db.execute('SELECT * FROM filings WHERE accession=?', (accession,)).fetchone())
        self.assertEqual(new_row['status'], 'verified')
        self.assertEqual(new_row['shard'], 'shards/2018-01-0002.sqlite3')
        db.close()
        new_db = readonly(partial / new_row['shard'])
        new_document = tuple(new_db.execute('SELECT * FROM documents').fetchone()); new_db.close()
        target = base / 'target.sqlite3'; capture_snapshot(partial, target)
        with contextlib.redirect_stdout(io.StringIO()):
            increment = build(partial, parent_archive / 'baseline.json', parent_pin,
                              parent, target, base / 'increment', workers=1)
            result = restore(base / 'increment', parent_archive / 'baseline.json',
                             base / 'restored', increment['increment_manifest_sha256'])
        self.assertEqual(increment['changed_committed_documents'], 1)
        self.assertEqual(result['restored_documents'], 3)
        helper.assert_inventory_equal(target, base / 'restored/inventory.sqlite3')
        expected = [(row, old_documents[row['accession']]) for row in old_rows] + [(new_row, new_document)]
        for row, document in expected:
            restored = readonly(base / 'restored' / row['shard'])
            self.assertEqual(tuple(restored.execute('SELECT * FROM documents WHERE accession=?',
                                                   (row['accession'],)).fetchone()), document)
            restored.close()
        for relative, expected_hash in old_hashes.items():
            self.assertEqual(file_hash(original / relative), expected_hash)


if __name__ == '__main__':
    unittest.main()
