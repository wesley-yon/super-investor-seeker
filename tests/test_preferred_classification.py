from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pipeline
import preferred_classification as p
from reviewed_ticker_map import public_display_mappings
from scripts.repair_note_classifications import repair_fund, repair_directory, quarter_hash


def row(cusip='595017302', kind='EQUITY', cls='DEP SHS REPSTG', **extra):
    return {'cusip': cusip, 'reported_cusip': cusip, 'holding_type': kind,
            'class': cls, 'reported_class': cls, 'put_call': None,
            'issuer': 'MICROCHIP TECHNOLOGY INC', 'reported_issuer': 'MICROCHIP TECHNOLOGY INC',
            'shares': 123, 'value': 456, **extra}


def fund():
    q = {'report_date': '2026-06-30', 'total_value': 1824,
         'composition_version': 2, 'composition_hash_version': 3,
         'security_identity_version': 1, 'source_filings': [{'accession':'0000000001-26-000001','source_hash':'a'*64}], 'base_accession': '0000000001-26-000001',
         'applied_accessions': ['0000000001-26-000001'], 'holdings': [row(), row('26922A198', 'PREF', 'PFD INC ETF'),
          row('65339F655', 'PREF', 'UNIT 02/15/2029'), row('03769M304', 'EQUITY', 'SER A MAND CNV')]}
    q['composition_hash'] = quarter_hash(q)
    return {'cik': 1, 'quarters': [q]}


class PreferredClassificationTests(unittest.TestCase):
    def test_primary_review_and_separate_series(self):
        review = p.classification_review()
        self.assertEqual(51, len(review))
        self.assertIn('Series A', review['02079K404']['name'])
        self.assertIn('Series B', review['02079K602']['name'])
        self.assertTrue(review['03769M304']['historical_retired'])
        for entry in review.values():
            self.assertTrue(entry['proofs'])
            self.assertTrue(any(x['url'].startswith('https://www.sec.gov/') for x in entry['proofs']))
            self.assertIn(entry['to_type'], {'EQUITY', 'PREF'})

    def test_classifies_exact_preferred_funds_and_units(self):
        for c, cls, expected in [('595017302','DEP SHS REPSTG','PREF'),
                ('02079K404','DEP SHS RP1/20 A','PREF'), ('42824C208','7.625 MAND CONV','PREF'),
                ('48251W500','6.25 CON SER D','PREF'), ('03769M304','SER A MAND CNV','PREF'),
                ('26922A198','PFD INCOME','EQUITY'), ('72201R619','PREF CAP SEC','EQUITY'),
                ('65339F655','PFD UNIT','EQUITY'), ('059460402','SP ADR PFD NEW','EQUITY')]:
            with self.subTest(cusip=c):
                self.assertEqual(expected, pipeline._classify_holding(row(c, cls=cls)))
                self.assertEqual(expected, pipeline.classify_saved_holding(row(c, 'PREF', cls)))

    def test_preserves_options_conflicting_identifiers_and_unreviewed_debt(self):
        for h in [row(kind='CALL'), row(kind='PUT'), row(kind='OPT'), row(put_call='PUT'),
                  row(cls='OPTION'), row(reported_class='CALL'), row(reported_cusip='595017104'),
                  row('26210CAD6', 'NOTE', 'NOTE 0.500% 2/0')]:
            self.assertIsNone(p.reviewed_preferred_type(h, h['holding_type']))
        self.assertEqual('EQUITY', pipeline._classify_holding(row('595017104', cls='COM')))
        for kind in ('CALL','PUT','OPT'):
            self.assertEqual(kind, pipeline.classify_saved_holding(row(kind=kind)))

    def test_migration_preserves_all_filing_fields_and_is_idempotent(self):
        old = fund(); frozen = deepcopy(old)
        new, report = repair_fund(old, review='preferred')
        self.assertEqual(frozen, old)
        self.assertEqual(4, report['rows'])
        q = new['quarters'][0]
        self.assertEqual(['PREF','EQUITY','EQUITY','PREF'], [h['holding_type'] for h in q['holdings']])
        self.assertEqual(quarter_hash(q), q['composition_hash'])
        for a,b in zip(old['quarters'][0]['holdings'],q['holdings']):
            self.assertEqual({k:v for k,v in a.items() if k!='holding_type'},
                             {k:v for k,v in b.items() if k!='holding_type'})
        same, report = repair_fund(new, review='preferred')
        self.assertEqual(new, same)
        self.assertEqual(0, report['rows'])
        old['quarters'][0]['holdings'][0]['shares'] += 1
        with self.assertRaisesRegex(ValueError, 'hash mismatch'):
            repair_fund(old, review='preferred')

    def test_serial_parallel_equivalence_and_failed_batch(self):
        with tempfile.TemporaryDirectory() as t:
            dirs = [Path(t)/'serial', Path(t)/'parallel']
            for directory in dirs:
                directory.mkdir()
                for i in range(2): (directory/f'{i}.json').write_text(json.dumps(fund()))
            a = repair_directory(dirs[0], apply=True, workers=1, review='preferred')
            b = repair_directory(dirs[1], apply=True, workers=2, review='preferred')
            self.assertEqual(a,b)
            for i in range(2): self.assertEqual((dirs[0]/f'{i}.json').read_bytes(), (dirs[1]/f'{i}.json').read_bytes())
            (dirs[0]/'0.json').write_text(json.dumps(fund()))
            before = (dirs[0]/'0.json').read_bytes()
            (dirs[0]/'1.json').write_text('broken')
            with self.assertRaises(ValueError):
                repair_directory(dirs[0], apply=True, workers=1, review='preferred')
            self.assertEqual(before, (dirs[0]/'0.json').read_bytes())

    def test_names_routes_wait_for_corpus_and_displays_keep_exact_symbols(self):
        meta = p.public_preferred_metadata({'595017302': {'type':'EQUITY'}})
        self.assertFalse(meta['preferred_type_corrections'])
        meta = p.public_preferred_metadata({'595017302': {'type':'PREF'}})
        self.assertEqual('PREF', meta['preferred_type_corrections']['595017302|EQUITY'])
        self.assertNotIn('595017302|CALL', meta['preferred_type_corrections'])
        self.assertIn('Series A', meta['instrument_names']['595017302'])
        display = {'ticker':'MCHPP','match_kind':'exact_cusip'}
        result = public_display_mappings('595017302', {'display_mappings': {'595017302|EQUITY':display}})
        self.assertEqual('MCHPP', result['PREF']['ticker'])
        with self.assertRaisesRegex(ValueError, 'conflict'):
            public_display_mappings('595017302', {'display_mappings': {'595017302|EQUITY':{**display,'ticker':'MCHP'}}})

    def test_retired_preferred_blocks_quotes_without_mutating_raw_evidence(self):
        import reviewed_ticker_map as r
        raw = {'records': {'03769M304|PREF': {'ticker':'APOPRA','price_lookup_allowed':True},
                           '03769M304|EQUITY': {'ticker':'OTHER'}}}
        frozen = deepcopy(raw)
        with patch.object(r, 'load_review', return_value=None):
            projected = r.apply_review(raw, Path('.'))
        self.assertEqual(raw, frozen)
        self.assertFalse(projected['records']['03769M304|PREF']['price_lookup_allowed'])
        self.assertEqual(projected['records']['03769M304|EQUITY'], frozen['records']['03769M304|EQUITY'])

    def test_completion_marker_survives_state_saves(self):
        with tempfile.TemporaryDirectory() as t:
            path = Path(t)/'state.json'
            with patch.object(pipeline, 'STATE_PATH', path):
                pipeline.save_state({'preferred_classification_review_sha256':p.REVIEW_SHA256})
                pipeline.save_state(pipeline.load_state())
            self.assertEqual(p.REVIEW_SHA256, json.loads(path.read_text())['preferred_classification_review_sha256'])

    def test_completion_marker_requires_successful_rebuild(self):
        import scripts.repair_preferred_classifications as script
        with tempfile.TemporaryDirectory() as t, patch('sys.argv', ['repair', '--apply','--rebuild','--pending-only','--report',str(Path(t)/'report.json')]), \
                patch.object(pipeline,'_serialize_pipeline_maintenance',side_effect=lambda fn: fn), \
                patch.object(pipeline,'load_state',return_value={}), \
                patch.object(pipeline,'save_state') as save, \
                patch.object(script,'repair_directory',return_value={'rows':0}), \
                patch.object(pipeline,'rebuild_registry_backed_outputs',side_effect=RuntimeError('rebuild failed')):
            with self.assertRaisesRegex(RuntimeError,'rebuild failed'): script.main()
            save.assert_not_called()
