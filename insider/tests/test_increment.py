from collections import Counter
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from insider_pipeline.audit_batches import SELECTION_COLUMNS, file_hash, readonly, run as audit, snapshot
from insider_pipeline.baseline import build as baseline
from insider_pipeline.increment import MANIFEST, build, delta_document_selection, restore, verify
from insider_pipeline.inventory import canonical, connect
from insider_pipeline.inventory_archive import archive, capture_snapshot
from insider_pipeline.inventory_delta import build as make_delta
from insider_pipeline.package import package
from insider_pipeline.runner import Shards, finish, prepare
import test_audit_batches as fixtures
from test_parser import filing, transaction, FOOTNOTES


class IncrementTests(unittest.TestCase):
    def seed(self, base):
        root = base / 'state'; root.mkdir(); fixtures.AuditBatchTests().seed(root)
        db = connect(root)
        db.executescript('''CREATE TABLE index_sources(source_key TEXT PRIMARY KEY,url TEXT,sha256 TEXT,bytes INTEGER,retrieved_at TEXT,filing_count INTEGER);
            CREATE TABLE index_membership(source_key TEXT,accession TEXT,PRIMARY KEY(source_key,accession));
            CREATE TABLE index_observations(source_key TEXT,accession TEXT,form TEXT,filing_date TEXT,source_url TEXT,PRIMARY KEY(source_key,accession));''')
        accession = '0001234567-26-000002'
        db.execute('INSERT INTO filings(accession,form,filing_date,source_url,status,attempts) VALUES(?,?,?,?,?,?)',
                   (accession, '4', '2026-01-03', 'https://www.sec.gov/' + accession + '.txt', 'inflight', 1))
        url = 'https://www.sec.gov/Archives/edgar/full-index/2026/QTR1/master.idx'
        body = ('CIK|Company Name|Form Type|Date Filed|Filename\n64040|Fixture|4|2026-01-03|edgar/data/64040/' + accession + '.txt\n').encode()
        path = root / 'sources/indexes' / (hashlib.sha256(url.encode()).hexdigest() + '.body')
        path.parent.mkdir(parents=True); path.write_bytes(body)
        db.execute('INSERT INTO index_sources VALUES(?,?,?,?,?,?)', ('2026Q1', url, hashlib.sha256(body).hexdigest(), len(body), '2026-09-09', 1))
        db.execute('INSERT INTO index_membership VALUES(?,?)', ('2026Q1', accession))
        db.execute('INSERT INTO index_observations VALUES(?,?,?,?,?)', ('2026Q1', accession, '4', '2026-01-03', 'https://www.sec.gov/' + accession + '.txt'))
        db.commit(); db.close()
        inventory, documents, output = base / 'inventory', base / 'documents', base / 'baseline'
        archive(root, inventory)
        parent = inventory / 'inventory-snapshot.sqlite3'
        audit(root, base / 'baseline-audit', workers=1, inventory_snapshot=parent)
        package(root, base / 'baseline-audit', documents)
        result = baseline(inventory, documents, output)
        return root, parent, output, result['baseline_sha256']

    def advance(self, root, target):
        db = connect(root)
        row = dict(db.execute("SELECT * FROM filings WHERE status='inflight'").fetchone())
        body = filing('<nonDerivativeTable>' + transaction() + '</nonDerivativeTable>' + FOOTNOTES)
        record = prepare(body, row)
        self.assertEqual(record['status'], 'verified')
        shards = Shards(root); relative = shards.choose(row, record)
        shards.save(relative, record); finish(db, row, record, relative); shards.close()
        db.execute("UPDATE filings SET attempts=7 WHERE accession='0001234567-18-000001'")
        db.execute("INSERT INTO filings(accession,form,filing_date,status) VALUES('0001234567-26-000003','4','2026-01-04','pending')")
        old = db.execute('SELECT * FROM index_sources').fetchone()
        path = root / 'sources/indexes' / (hashlib.sha256(old['url'].encode()).hexdigest() + '.body')
        body = path.read_bytes() + b'64040|Fixture|4|2026-01-04|edgar/data/64040/0001234567-26-000003.txt\n'
        path.write_bytes(body)
        db.execute('UPDATE index_sources SET sha256=?,bytes=?,retrieved_at=?,filing_count=2',
                   (hashlib.sha256(body).hexdigest(), len(body), '2026-09-10'))
        db.execute("INSERT INTO index_membership VALUES('2026Q1','0001234567-26-000003')")
        db.execute("INSERT INTO index_observations VALUES('2026Q1','0001234567-26-000003','4','2026-01-04','https://www.sec.gov/new.txt')")
        quarter = root / '2026Q2.zip'
        with zipfile.ZipFile(quarter, 'w') as packed:
            packed.writestr('SUBMISSION.tsv', 'ACCESSION_NUMBER\tISSUERCIK\tFILING_DATE\tDOCUMENT_TYPE\n')
        db.execute('INSERT INTO sources VALUES(?,?,?,?,?,?,?)',
                   ('2026Q2', 'https://www.sec.gov/2026Q2.zip', file_hash(quarter), quarter.name, quarter.stat().st_size, '2026-09-10', 0))
        db.commit(); db.close()
        capture_snapshot(root, target)

    def assert_inventory_equal(self, first, second):
        a, b = readonly(first), readonly(second)
        try:
            for table in ('settings', 'sources', 'filings', 'index_sources', 'index_membership', 'index_observations'):
                self.assertEqual(sorted(tuple(row) for row in a.execute('SELECT * FROM ' + table)),
                                 sorted(tuple(row) for row in b.execute('SELECT * FROM ' + table)), table)
        finally:
            a.close(); b.close()

    def test_complete_increment_restores_changed_inherited_and_pending_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); root, parent, parent_archive, pin = self.seed(base)
            parent_hash = file_hash(parent); target = base / 'target.sqlite3'; self.advance(root, target)
            # Live queue changes after the snapshot must not enter the package.
            db = connect(root); db.execute("UPDATE filings SET attempts=99 WHERE accession='0001234567-18-000001'"); db.commit(); db.close()
            output = base / 'increment'
            manifest = build(root, parent_archive / 'baseline.json', pin, parent, target, output, workers=1)
            self.assertEqual(manifest['changed_committed_documents'], 2)
            self.assertEqual(manifest['target_inventory']['committed_documents'], 3)
            self.assertEqual(manifest['target_inventory']['queue_counts'], {'pending': 1, 'verified': 3})
            self.assertEqual(len(manifest['source_assets']), 2)
            self.assertFalse(manifest['standalone_restore_ready'])
            result = restore(output, parent_archive / 'baseline.json', base / 'restored', manifest['increment_manifest_sha256'])
            self.assertEqual((result['changed_documents'], result['inherited_documents'], result['restored_documents']), (2, 1, 3))
            self.assertEqual(result['source_files_verified'], 4)
            self.assertTrue(result['collection_resume_ready'])
            self.assertFalse(result['complete_backfill'])
            self.assert_inventory_equal(target, base / 'restored/inventory.sqlite3')
            self.assertEqual(file_hash(parent), parent_hash)
            db = readonly(target)
            try:
                for row in db.execute("SELECT * FROM filings WHERE status='verified'"):
                    source, restored = readonly(root / row['shard']), readonly(base / 'restored' / row['shard'])
                    try:
                        self.assertEqual(dict(source.execute('SELECT * FROM documents WHERE accession=?', (row['accession'],)).fetchone()),
                                         dict(restored.execute('SELECT * FROM documents WHERE accession=?', (row['accession'],)).fetchone()))
                    finally:
                        source.close(); restored.close()
            finally:
                db.close()
            audited = audit(base / 'restored', base / 'changed-audit', workers=1,
                            inventory_snapshot=base / 'restored/inventory.sqlite3', parent_inventory=parent)
            self.assertEqual(audited['semantic_sha256'], manifest['changed_source_audit_sha256'])
            all_documents = audit(base / 'restored', base / 'full-audit', workers=1)
            self.assertEqual(all_documents['counts']['checksum_documents_passed'], 3)
            self.assertEqual(all_documents['counts'].get('original_xml_field_failure', 0), 0)

    def test_chained_pending_only_increment_has_no_document_reuploads(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); root, parent, parent_archive, pin = self.seed(base)
            first_target = base / 'first.sqlite3'; self.advance(root, first_target)
            one = base / 'one'; first = build(root, parent_archive / 'baseline.json', pin, parent, first_target, one, workers=1)
            db = connect(root)
            db.execute("UPDATE filings SET attempts=3,retry_after=1789009999.25,status='retry' WHERE status='pending'")
            db.execute("UPDATE settings SET value='2026-09-10' WHERE key='window_end'")
            db.commit(); db.close()
            second_target = base / 'second.sqlite3'; capture_snapshot(root, second_target)
            two = base / 'two'; second = build(root, one / MANIFEST, first['increment_manifest_sha256'], first_target, second_target, two, workers=1)
            self.assertEqual(second['changed_committed_documents'], 0)
            self.assertEqual(second['source_assets'], [])
            self.assertFalse(any(row['file'].startswith('filings-') for row in second['files']))
            with self.assertRaisesRegex(ValueError, 'ancestor manifest'):
                restore(two, one / MANIFEST, base / 'missing-parent', second['increment_manifest_sha256'])
            self.assertFalse((base / 'missing-parent').exists())
            result = restore(two, one / MANIFEST, base / 'restored', second['increment_manifest_sha256'],
                             ancestor_manifests=[parent_archive / 'baseline.json'])
            self.assertEqual(result['changed_documents'], 0)
            self.assertEqual(result['inherited_documents'], 3)
            self.assert_inventory_equal(second_target, base / 'restored/inventory.sqlite3')

    def test_narrow_document_sample_cannot_be_published_as_a_complete_increment(self):
        def narrowed(root, output, **kwargs):
            full = output.with_name('full-audit')
            audit(root, full, **kwargs)
            selected = output.parent / 'narrow.sqlite3'
            shutil.copyfile(full / 'selection.sqlite3', selected)
            db = sqlite3.connect(selected); db.row_factory = sqlite3.Row
            metadata = json.loads(db.execute('SELECT value FROM metadata').fetchone()[0])
            db.execute('DELETE FROM selected WHERE accession NOT IN (SELECT accession FROM selected ORDER BY filing_date,accession LIMIT 1)')
            rows = list(db.execute('SELECT * FROM selected ORDER BY filing_date,accession'))
            digest = hashlib.sha256()
            for row in rows:
                digest.update(canonical({name: row[name] for name in SELECTION_COLUMNS}) + b'\n')
            metadata.update(selected_documents=len(rows), groups=dict(Counter(row['audit_group'] for row in rows)), selection_sha256=digest.hexdigest())
            db.execute('UPDATE metadata SET value=?', (canonical(metadata).decode(),)); db.commit(); db.close()
            return audit(root, output, workers=1, selection=selected)
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); root, parent, parent_archive, pin = self.seed(base)
            target = base / 'target.sqlite3'; self.advance(root, target)
            with patch('insider_pipeline.increment.audit', side_effect=narrowed):
                with self.assertRaisesRegex(ValueError, 'checkpoint bindings differ'):
                    build(root, parent_archive / 'baseline.json', pin, parent, target, base / 'incomplete', workers=1)
            self.assertFalse((base / 'incomplete').exists())
            failures = list(base.glob('.incomplete.building-*/failure.json'))
            self.assertEqual(len(failures), 1)
            self.assertFalse(json.loads(failures[0].read_text())['checkpoint_published'])

    def test_wrong_pin_missing_parent_archive_and_corrupt_increment_preserve_destination(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); root, parent, parent_archive, pin = self.seed(base)
            target = base / 'target.sqlite3'; self.advance(root, target)
            output = base / 'increment'; manifest = build(root, parent_archive / 'baseline.json', pin, parent, target, output, workers=1)
            destination = base / 'restored'
            with self.assertRaisesRegex(ValueError, 'pinned checksum'):
                restore(output, parent_archive / 'baseline.json', destination, '0' * 64)
            parent_files = json.loads((parent_archive / 'manifest.json').read_text())
            inherited_chunk = parent_archive / parent_files['chunks'][0]['file']
            backup = inherited_chunk.read_bytes(); inherited_chunk.unlink()
            with self.assertRaises(FileNotFoundError):
                restore(output, parent_archive / 'baseline.json', destination, manifest['increment_manifest_sha256'])
            self.assertFalse(destination.exists())
            inherited_chunk.write_bytes(backup)
            changed_chunk = next(output / row['file'] for row in manifest['files'] if row['file'].startswith('filings-'))
            changed_chunk.write_bytes(changed_chunk.read_bytes() + b'corrupt')
            with self.assertRaisesRegex(ValueError, 'asset bytes differ'):
                verify(output, manifest['increment_manifest_sha256'])
            destination.mkdir(); (destination / 'keep').write_text('retained')
            with self.assertRaisesRegex(ValueError, 'must not exist'):
                restore(output, parent_archive / 'baseline.json', destination, manifest['increment_manifest_sha256'])
            self.assertEqual((destination / 'keep').read_text(), 'retained')

    def test_delta_selection_streams_across_tiny_compressed_parts(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); root, parent, parent_archive, pin = self.seed(base)
            db = connect(root); db.execute("UPDATE filings SET attempts=4 WHERE status='verified'"); db.commit(); db.close()
            target = base / 'target.sqlite3'; capture_snapshot(root, target)
            output = base / 'delta'; delta = make_delta(parent, target, output, max_bytes=2)
            self.assertGreater(len(delta['parts']), 2)
            count, digest = delta_document_selection(output, delta)
            selected = snapshot(root, base / 'selection.sqlite3', inventory_snapshot=target, parent_inventory=parent)
            self.assertEqual(count, 2)
            self.assertEqual((count, digest), (selected['selected_documents'], selected['selection_sha256']))
            with self.assertRaisesRegex(ValueError, 'incremental scope'):
                snapshot(root, base / 'selection.sqlite3', inventory_snapshot=target)
            with self.assertRaisesRegex(ValueError, 'sample limit'):
                snapshot(root, base / 'sample.sqlite3', limit=1, inventory_snapshot=target, parent_inventory=parent)


if __name__ == '__main__':
    unittest.main()
