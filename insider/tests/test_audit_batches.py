import csv
import hashlib
import io
from pathlib import Path
import sqlite3
import tempfile
import unittest
import zipfile

from insider_pipeline.audit_batches import run, snapshot
from insider_pipeline.inventory import connect
from insider_pipeline.runner import Shards, finish, prepare
from test_parser import filing, transaction, FOOTNOTES


class AuditBatchTests(unittest.TestCase):
    def seed(self, root):
        db = connect(root)
        db.executemany('INSERT INTO settings VALUES(?,?)', [('window_start', '2018-01-01'), ('window_end', '2026-09-09')])
        shards = Shards(root)
        for year in [2018, 2026]:
            accession = f'0001234567-{year % 100:02d}-000001'
            quarter = f'{year}Q1'
            row = {'accession': accession, 'source_url': 'https://www.sec.gov/' + accession + '.txt',
                   'form': '4', 'filing_date': f'{year}-01-02', 'issuer_cik': 64040, 'bulk_source': quarter}
            xml = filing('<nonDerivativeTable>' + transaction() + '</nonDerivativeTable>' + FOOTNOTES)
            record = prepare(xml, row)
            self.assertEqual(record['status'], 'verified')
            relative = shards.choose(row, record)
            shards.save(relative, record)
            db.execute('INSERT INTO filings(accession,form,filing_date,issuer_cik,source_url,bulk_source) VALUES(?,?,?,?,?,?)',
                       tuple(row[k] for k in ['accession', 'form', 'filing_date', 'issuer_cik', 'source_url', 'bulk_source']))
            db.commit()
            finish(db, row, record, relative)
            source = root / (quarter + '.zip')
            with zipfile.ZipFile(source, 'w') as archive:
                for table in ['NONDERIV_TRANS', 'DERIV_TRANS', 'NONDERIV_HOLDING', 'DERIV_HOLDING']:
                    rows = io.StringIO()
                    writer = csv.writer(rows, delimiter='\t')
                    writer.writerow(['ACCESSION_NUMBER', 'TRANS_SHARES', 'TRANS_PRICEPERSHARE'])
                    if table == 'NONDERIV_TRANS':
                        writer.writerow([accession, '2.5', '10.13'])
                    archive.writestr(table + '.tsv', rows.getvalue())
            db.execute('INSERT INTO sources VALUES(?,?,?,?,?,?,?)',
                       (quarter, 'https://www.sec.gov/' + quarter + '.zip', hashlib.sha256(source.read_bytes()).hexdigest(), source.name,
                        source.stat().st_size, '2026-09-10T00:00:00Z', 1))
            db.commit()
        shards.close(); db.close()

    def test_resume_and_parallel_have_identical_evidence(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder) / 'state'; root.mkdir()
            self.seed(root)
            one, four = Path(folder) / 'one', Path(folder) / 'four'
            serial = run(root, one, 1)
            parallel = run(root, four, 4, selection=one / 'selection.sqlite3')
            resumed = run(root, one, 1)
            self.assertEqual(serial['semantic_sha256'], parallel['semantic_sha256'])
            self.assertEqual(serial['semantic_sha256'], resumed['semantic_sha256'])
            self.assertEqual(serial['counts']['checksum_documents_passed'], 2)
            self.assertEqual(serial['financial_table_comparisons']['bulk_rounding'], 2)
            self.assertFalse(serial['complete_backfill'])
            (one / '2018Q1.jsonl.gz').write_bytes(b'corrupt')
            with self.assertRaises(ValueError):
                run(root, one, 1)

    def test_snapshot_is_fixed_when_collection_changes(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder) / 'state'; root.mkdir()
            self.seed(root)
            output = Path(folder) / 'audit'
            metadata = snapshot(root, output / 'selection.sqlite3')
            db = connect(root)
            db.execute("UPDATE filings SET status='pending'"); db.commit(); db.close()
            reused = snapshot(root, output / 'selection.sqlite3')
            self.assertEqual(metadata, reused)
            self.assertEqual(reused['selected_documents'], 2)

    def test_changed_selection_cannot_reuse_audit_evidence(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder) / 'state'; root.mkdir()
            self.seed(root)
            output = Path(folder) / 'audit'
            snapshot(root, output / 'selection.sqlite3')
            db = sqlite3.connect(output / 'selection.sqlite3')
            db.execute("UPDATE selected SET source_sha256='changed'")
            db.commit(); db.close()
            with self.assertRaisesRegex(ValueError, 'checksum'):
                run(root, output)
