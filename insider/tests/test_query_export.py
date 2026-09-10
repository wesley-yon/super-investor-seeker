import gzip
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from insider_pipeline import query_export as export
from insider_pipeline.audit_batches import file_hash, readonly
from insider_pipeline.inventory import canonical, connect
from insider_pipeline.inventory_archive import capture_snapshot
from insider_pipeline.runner import Shards, finish, prepare
from test_parser import filing, transaction, FOOTNOTES


class QueryExportTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(); self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name); self.root = self.base / 'state'
        db = connect(self.root)
        db.executemany('INSERT INTO settings VALUES(?,?)', [('window_start', '2026-09-01'), ('window_end', '2026-09-09')])
        second_owner = '''<reportingOwner><reportingOwnerId><rptOwnerCik>0001000002</rptOwnerCik>
            <rptOwnerName>Example Two</rptOwnerName></reportingOwnerId></reportingOwner>'''
        holding = '''<nonDerivativeHolding><securityTitle><value>Common Stock</value></securityTitle>
            <postTransactionAmounts><sharesOwnedFollowingTransaction><value>001234.5000</value>
            </sharesOwnedFollowingTransaction></postTransactionAmounts></nonDerivativeHolding>'''
        notes = FOOTNOTES.replace('</footnotes>', '<footnote id="F1">A second distinct F1.</footnote></footnotes>')
        body = filing(second_owner + '<nonDerivativeTable>'
                      + transaction(shares='9007199254740993.00001', price='0.00000000001') + holding
                      + '</nonDerivativeTable><derivativeTable>' + transaction(code='M', derivative=True)
                      + '</derivativeTable>' + notes)
        amendment = filing('<nonDerivativeTable>' + transaction(code='S') + '</nonDerivativeTable>' + FOOTNOTES, form='4/A')
        amendment = amendment.replace(b'<transactionDate><value>2026-09-01', b'<transactionDate><value>0024-02-01')
        sources = [(body, '4', 64040), (amendment, '4/A', 64040), (b'unparseable retained response', '4', 99999)]
        self.documents = {}; self.shards = {}; shards = Shards(self.root)
        for number, (raw, form, cik) in enumerate(sources, 1):
            accession = f'0001234567-26-{number:06d}'
            db.execute('INSERT INTO filings(accession,issuer_cik,form,filing_date,source_url,status) VALUES(?,?,?,?,?,?)',
                       (accession, cik, form, f'2026-09-{number:02d}', 'https://www.sec.gov/' + accession + '.txt', 'inflight'))
            row = dict(db.execute('SELECT * FROM filings WHERE accession=?', (accession,)).fetchone())
            record = prepare(raw, row); relative = shards.choose(row, record)
            shards.save(relative, record); finish(db, row, record, relative)
            self.documents[accession] = json.loads(gzip.decompress(record['parsed_gzip']))
            self.shards[accession] = relative
        shards.close()
        db.execute("INSERT INTO filings(accession,issuer_cik,form,filing_date,status) VALUES('0001234567-26-000004',64040,'4','2026-09-04','pending')")
        db.commit(); db.close()
        self.inventory = self.base / 'snapshot.sqlite3'; capture_snapshot(self.root, self.inventory)
        self.pin = file_hash(self.inventory)

    def build(self, name='export', **kwargs):
        return export.build(self.root, self.inventory, self.pin, self.base / name, **kwargs)

    def test_every_record_round_trips_and_views_preserve_exact_values_and_as_filed_semantics(self):
        result = self.build()
        self.assertEqual((result['selected_documents'], result['parsed_documents'], result['unparsed_documents']), (3, 2, 1))
        self.assertEqual(result['entity_counts'], {'filing': 2, 'owners': 3, 'transactions': 3,
                                                  'holdings': 1, 'footnotes': 5, 'signatures': 2})
        self.assertEqual(result['matching_inventory_status_counts']['pending'], 1)
        db = readonly(self.base / 'export' / export.DATABASE)
        try:
            for accession, original in self.documents.items():
                if original is None:
                    self.assertEqual(db.execute('SELECT is_parsed FROM documents WHERE accession=?', (accession,)).fetchone()[0], 0)
                    continue
                for entity in export.ENTITIES:
                    rows = [json.loads(row[0]) for row in db.execute(
                        'SELECT record_json FROM records WHERE entity=? AND accession=? ORDER BY ordinal', (entity, accession))]
                    self.assertEqual(rows, [original[entity]] if entity == 'filing' else original[entity])
            row = db.execute("SELECT transaction_shares,typeof(transaction_shares),transaction_price_per_share,transaction_value,shares_owned_following FROM transactions WHERE row_id='0001234567-26-000001:ND-T:1'").fetchone()
            self.assertEqual(tuple(row), ('9007199254740993.00001', 'text', '0.00000000001', '90071.9925474099300001', '001234.5000'))
            self.assertEqual(db.execute("SELECT count(*) FROM transactions WHERE accession='0001234567-26-000001'").fetchone()[0], 2)
            self.assertEqual(db.execute('SELECT count(*) FROM filings WHERE is_amendment=1').fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT transaction_date FROM transactions WHERE accession='0001234567-26-000002'").fetchone()[0], '0024-02-01')
            self.assertEqual(db.execute("SELECT count(*) FROM footnotes WHERE accession='0001234567-26-000001' AND footnote_id='F1'").fetchone()[0], 2)
            self.assertEqual(db.execute('SELECT owner_cik FROM owners ORDER BY accession,ordinal LIMIT 1').fetchone()[0], '0001000001')
            self.assertEqual(db.execute("SELECT market_scope FROM transactions WHERE transaction_code='P'").fetchone()[0], 'open_or_private')
        finally:
            db.close()
        self.assertFalse(result['private_archive_uploaded'] or result['complete_backfill'])

    def test_issuer_and_filing_date_filters_exclude_other_source_reads_and_keep_empty_scope_explicit(self):
        accession = '0001234567-26-000003'
        db = sqlite3.connect(self.root / self.shards[accession]); db.execute('UPDATE documents SET parsed_gzip=? WHERE accession=?', (b'corrupt', accession)); db.commit(); db.close()
        result = self.build(issuer_cik=64040, filed_from='2026-09-02', filed_through='2026-09-04')
        self.assertEqual((result['selected_documents'], result['parsed_documents']), (1, 1))
        self.assertEqual(result['matching_inventory_status_counts']['pending'], 1)
        empty = self.build('empty', issuer_cik=77777)
        self.assertEqual(empty['selected_documents'], 0)
        self.assertEqual(sum(empty['entity_counts'].values()), 0)

    def test_corrupted_or_missing_selected_document_never_publishes_an_export(self):
        accession = '0001234567-26-000001'; path = self.root / self.shards[accession]
        db = sqlite3.connect(path); db.execute('UPDATE documents SET source_sha256=? WHERE accession=?', ('0' * 64, accession)); db.commit(); db.close()
        with self.assertRaisesRegex(ValueError, 'checksum differs'):
            self.build()
        self.assertFalse((self.base / 'export').exists())
        db = sqlite3.connect(path); db.execute('DELETE FROM documents WHERE accession=?', (accession,)); db.commit(); db.close()
        with self.assertRaisesRegex(ValueError, 'identity or collection status differs'):
            self.build()
        self.assertFalse((self.base / 'export').exists())

    def test_invalid_dates_pin_cik_and_existing_output_are_rejected_without_overwrite(self):
        for changes in ({'issuer_cik': 0}, {'issuer_cik': True}, {'filed_from': '2026-08-31'},
                        {'filed_through': '2026-09-10'}, {'filed_from': '2026-09-07', 'filed_through': '2026-09-01'}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.build(**changes)
        with self.assertRaisesRegex(ValueError, 'pinned frozen inventory'):
            export.build(self.root, self.inventory, '0' * 64, self.base / 'bad-pin')
        output = self.base / 'existing'; output.mkdir(); (output / 'keep.txt').write_text('keep')
        with self.assertRaisesRegex(ValueError, 'new output directory'):
            self.build('existing')
        self.assertEqual((output / 'keep.txt').read_text(), 'keep')

    def test_disk_shortage_and_inventory_change_during_export_do_not_publish(self):
        with patch('insider_pipeline.query_export.shutil.disk_usage') as usage:
            usage.return_value.free = 1
            with self.assertRaisesRegex(ValueError, 'free disk'):
                self.build('disk')
        original = export.readback
        def mutate(*args):
            original(*args)
            db = sqlite3.connect(self.inventory); db.execute("UPDATE filings SET attempts=7 WHERE status='pending'"); db.commit(); db.close()
        with patch('insider_pipeline.query_export.readback', side_effect=mutate), self.assertRaisesRegex(ValueError, 'inventory changed'):
            self.build('changed')
        self.assertFalse((self.base / 'disk').exists() or (self.base / 'changed').exists())

    def test_persisted_financial_row_or_document_provenance_tampering_is_detected(self):
        original = export.readback
        def change_row(path, *args):
            db = sqlite3.connect(path)
            payload = json.loads(db.execute("SELECT record_json FROM records WHERE entity='transactions' LIMIT 1").fetchone()[0])
            payload['transaction_price_per_share'] = '999'
            db.execute("UPDATE records SET record_json=? WHERE entity='transactions' AND document_order=1 AND ordinal=1", (canonical(payload).decode(),))
            db.commit(); db.close(); return original(path, *args)
        with patch('insider_pipeline.query_export.readback', side_effect=change_row), self.assertRaisesRegex(ValueError, 'rows differ'):
            self.build('row-tamper')
        def change_source(path, *args):
            db = sqlite3.connect(path); db.execute("UPDATE documents SET form='5' WHERE sequence=1"); db.commit(); db.close()
            return original(path, *args)
        with patch('insider_pipeline.query_export.readback', side_effect=change_source), self.assertRaisesRegex(ValueError, 'provenance differs'):
            self.build('source-tamper')
        self.assertFalse((self.base / 'row-tamper').exists() or (self.base / 'source-tamper').exists())

    def test_malformed_normalized_shape_or_numeric_financial_value_is_not_silently_exported(self):
        accession = '0001234567-26-000001'; db = readonly(self.root / self.shards[accession])
        record = dict(db.execute('SELECT * FROM documents WHERE accession=?', (accession,)).fetchone()); db.close()
        inv = readonly(self.inventory); item = dict(inv.execute('SELECT * FROM filings WHERE accession=?', (accession,)).fetchone()); inv.close()
        original = self.documents[accession]
        for mode in ('count', 'numeric', 'identity', 'extra-entity'):
            value = json.loads(canonical(original))
            if mode == 'count': value['filing']['transaction_count'] = 99
            if mode == 'numeric': value['transactions'][0]['transaction_shares'] = 1.5
            if mode == 'identity': value['owners'][0]['accession'] = 'wrong'
            if mode == 'extra-entity': value['unhandled'] = []
            raw = canonical(value); changed = {**record, 'parsed_gzip': gzip.compress(raw), 'parsed_bytes': len(raw), 'parsed_sha256': hashlib.sha256(raw).hexdigest()}
            with self.subTest(mode=mode), self.assertRaises(ValueError):
                export.decoded_document({**item, 'parsed_sha256': changed['parsed_sha256']}, changed)


if __name__ == '__main__':
    unittest.main()
