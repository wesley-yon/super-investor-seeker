import gzip
import hashlib
import json
from pathlib import Path
import tempfile
import threading
import unittest

from insider_pipeline.discovery import parse_index, quarters
from insider_pipeline.http import check_url
from insider_pipeline.inventory import TABLES, connect
from insider_pipeline.locking import writer_lock
from insider_pipeline.runner import Shards, extract_xml, finish, prepare, run, verified_document
from test_parser import ACCESSION, URL, filing, transaction, FOOTNOTES


class BackfillTests(unittest.TestCase):
    def row(self):
        counts = dict.fromkeys(TABLES, 0)
        counts.update(NONDERIV_TRANS=1, REPORTINGOWNER=1, OWNER_SIGNATURE=1, FOOTNOTES=2)
        return {'accession': ACCESSION, 'source_url': URL, 'issuer_cik': 64040, 'form': '4',
                'filing_date': '2026-09-03', 'expected_counts': json.dumps(counts), 'discovery_issues': '[]'}

    def source(self):
        xml = filing('<nonDerivativeTable>' + transaction() + '</nonDerivativeTable>' + FOOTNOTES)
        return b'ACCESSION NUMBER: ' + ACCESSION.encode() + b'\n<DOCUMENT>\n<TYPE>4\n<XML>\n' + xml + b'\n</XML>\n</DOCUMENT>'

    def test_index_joint_filers_and_quarters(self):
        body = ('CIK|Company Name|Form Type|Date Filed|Filename\n'
                '64040|Issuer|4|2018-01-02|edgar/data/64040/0000000001-18-000001.txt\n'
                '2|Owner|4|2018-01-02|edgar/data/2/0000000001-18-000001.txt\n'
                '2|Owner|10-K|2018-01-02|edgar/data/2/0000000001-18-000002.txt\n').encode()
        self.assertEqual(len(parse_index(body, '2018-01-01', '2026-09-09')), 1)
        self.assertEqual(len(list(quarters('2018-01-01', '2026-09-09'))), 35)
        with self.assertRaises(ValueError):
            parse_index(b'<html>Access denied</html>', '2018-01-01', '2026-09-09')

    def test_sec_url_boundary(self):
        check_url(URL)
        for url in ['http://www.sec.gov/a', 'https://example.com/a', 'https://www.sec.gov:8000/a', 'https://u@www.sec.gov/a']:
            with self.assertRaises(ValueError):
                check_url(url)

    def test_complete_submission_identity_and_count_audit(self):
        body = self.source()
        record = prepare(body, self.row())
        self.assertEqual(record['status'], 'verified')
        self.assertEqual(gzip.decompress(record['source_gzip']), body)
        parsed = json.loads(gzip.decompress(record['parsed_gzip']))
        self.assertEqual(parsed['transactions'][0]['transaction_value'], '25.312500')
        row = self.row(); row['issuer_cik'] = 999
        self.assertIn('ISSUER_CIK_MISMATCH', prepare(body, row)['audit_json'])
        row = self.row(); row['expected_counts'] = json.dumps(dict.fromkeys(TABLES, 0))
        self.assertEqual(prepare(body, row)['status'], 'review')
        with self.assertRaises(ValueError):
            extract_xml(body.replace(ACCESSION.encode(), b'0000000000-26-000000'), ACCESSION)
        with self.assertRaises(ValueError):
            extract_xml(body + body, ACCESSION)

    def test_malformed_source_retained_for_review(self):
        record = prepare(b'<html>Not XML</html>', self.row())
        self.assertEqual(record['status'], 'review')
        self.assertEqual(gzip.decompress(record['source_gzip']), b'<html>Not XML</html>')

    def test_crash_between_shard_and_queue_recovers_exact_bytes(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            db = connect(root)
            row = self.row()
            db.execute('INSERT INTO filings(accession,issuer_cik,form,filing_date,source_url) VALUES(?,?,?,?,?)',
                       tuple(row[k] for k in ['accession', 'issuer_cik', 'form', 'filing_date', 'source_url']))
            db.commit()
            shards = Shards(root)
            record = prepare(self.source(), row)
            relative = shards.choose(row, record)
            with db:
                db.execute('UPDATE filings SET shard=? WHERE accession=?', (relative, ACCESSION))
            shards.save(relative, record)
            shards.close()
            shards = Shards(root)
            recovered_row = dict(db.execute('SELECT * FROM filings').fetchone())
            recovered = shards.existing(recovered_row)
            self.assertEqual(recovered['source_sha256'], hashlib.sha256(self.source()).hexdigest())
            finish(db, recovered_row, recovered, relative)
            self.assertEqual(db.execute('SELECT status FROM filings').fetchone()[0], 'verified')
            shard_db = shards.database(relative)
            shard_db.execute('UPDATE documents SET source_sha256=?', ('0' * 64,)); shard_db.commit()
            with self.assertRaises(ValueError):
                verified_document(shard_db, ACCESSION)
            shards.close(); db.close()

    def test_duplicate_writer_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            with writer_lock(folder):
                with self.assertRaises(RuntimeError):
                    with writer_lock(folder):
                        pass

    def test_one_and_four_workers_equivalent_and_resume(self):
        source = self.source()

        class Client:
            requests = 0
            download_bytes = 0
            blocked = threading.Event()

            def get(self, url):
                self.requests += 1
                result = source.replace(ACCESSION.encode(), Path(url).stem.encode())
                self.download_bytes += len(result)
                return result

        def seed(root):
            db = connect(root)
            for i in range(1, 7):
                accession = f'0001234567-26-{i:06d}'
                db.execute('INSERT INTO filings(accession,issuer_cik,form,filing_date,source_url) VALUES(?,?,?,?,?)',
                           (accession, 64040, '4', '2026-09-03', 'https://www.sec.gov/' + accession + '.txt'))
            db.commit(); db.close()

        with tempfile.TemporaryDirectory() as folder:
            first, second = Path(folder) / 'serial', Path(folder) / 'parallel'
            seed(first); seed(second)
            run(first, Client(), workers=1, limit=2)
            resumed = run(first, Client(), workers=1)
            parallel = run(second, Client(), workers=4)
            self.assertEqual(resumed['counts'], {'verified': 6})
            self.assertEqual(resumed['completed_this_run'], 4)
            self.assertEqual(parallel['counts'], {'verified': 6})
            one, four = connect(first), connect(second)
            sql = 'SELECT accession,source_sha256,parsed_sha256 FROM filings ORDER BY accession'
            self.assertEqual([tuple(r) for r in one.execute(sql)], [tuple(r) for r in four.execute(sql)])
            one.close(); four.close()


if __name__ == '__main__':
    unittest.main()
