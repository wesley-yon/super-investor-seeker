import hashlib
import json
from pathlib import Path
import tempfile
import unittest
import zipfile

from insider_pipeline.inventory import TABLES, connect, filed_date, import_cached


class InventoryTests(unittest.TestCase):
    def test_date_formats(self):
        self.assertEqual(filed_date('02-JAN-2018'), '2018-01-02')
        self.assertEqual(filed_date('2026-09-09'), '2026-09-09')
        with self.assertRaises(ValueError):
            filed_date('31-FEB-2020')

    def fixture(self, cache):
        body = cache / 'source.body'
        with zipfile.ZipFile(body, 'w') as z:
            z.writestr('SUBMISSION.tsv',
                'ACCESSION_NUMBER\tISSUERCIK\tFILING_DATE\tDOCUMENT_TYPE\n'
                '0000000001-18-000001\t64040\t02-JAN-2018\t4\n'
                '0000000001-17-000001\t64040\t29-DEC-2017\t4\n')
            for name in TABLES:
                rows = '0000000001-18-000001\tvalue\n' if name in ('NONDERIV_TRANS', 'REPORTINGOWNER') else ''
                z.writestr(name + '.tsv', 'ACCESSION_NUMBER\tOTHER\n' + rows)
        metadata = {'url': 'https://www.sec.gov/files/2018q1_form345.zip',
                    'bytes': body.stat().st_size, 'sha256': hashlib.sha256(body.read_bytes()).hexdigest()}
        (cache / 'source.json').write_text(json.dumps(metadata))

    def test_import_resume_and_original_source_hash(self):
        with tempfile.TemporaryDirectory() as d:
            root, cache = Path(d) / 'state', Path(d) / 'cache'
            cache.mkdir()
            self.fixture(cache)
            first = import_cached(root, cache, '2018-01-01', '2018-03-31', 1)
            second = import_cached(root, cache, '2018-01-01', '2018-03-31', 2)
            self.assertEqual(first, second)
            self.assertEqual(first['filings_by_status'], {'pending': 1})
            db = connect(root)
            row = db.execute('SELECT * FROM filings').fetchone()
            self.assertEqual(json.loads(row['expected_counts'])['NONDERIV_TRANS'], 1)
            self.assertEqual(row['status'], 'pending')
            source = db.execute('SELECT * FROM sources').fetchone()
            self.assertEqual(hashlib.sha256((root / source['path']).read_bytes()).hexdigest(), source['sha256'])
            db.close()
            with self.assertRaises(ValueError):
                import_cached(root, cache, '2017-01-01', '2018-03-31', 1)

    def test_corrupt_source_does_not_enter_queue(self):
        with tempfile.TemporaryDirectory() as d:
            root, cache = Path(d) / 'state', Path(d) / 'cache'
            cache.mkdir()
            self.fixture(cache)
            (cache / 'source.body').write_bytes(b'changed')
            with self.assertRaises(ValueError):
                import_cached(root, cache, '2018-01-01', '2018-03-31', 1)
            db = connect(root)
            self.assertEqual(db.execute('SELECT count(*) FROM filings').fetchone()[0], 0)
            db.close()
