from collections import Counter
import copy
import gzip
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from insider_pipeline.audit import FIELDS
from insider_pipeline.audit_batches import file_hash, run as audit
from insider_pipeline.bulk_review import calendar_projection, classification, run
from insider_pipeline.inventory import canonical
import test_audit_batches as fixtures
import insider_pipeline.bulk_review as review_module


class BulkReviewTests(unittest.TestCase):
    def test_calendar_projection_validates_dates_and_offsets_without_utc_conversion(self):
        self.assertEqual(calendar_projection('2018-01-01-05:00'), ('2018-01-01', '-05:00'))
        self.assertEqual(calendar_projection('2024-02-29+14:00'), ('2024-02-29', '+14:00'))
        self.assertEqual(calendar_projection('2024-02-29Z'), ('2024-02-29', 'Z'))
        for text in ('2018-02-29-05:00', '2018-01-01+14:01', '2018-01-01-15:00',
                     '2018-01-01+02:60', '2018-01-01T00:00:00Z', '2018-01-01',
                     'INVALID:2018-01-01-05:00', '2018-01-01-05:00 suffix', '0000-01-01Z'):
            self.assertIsNone(calendar_projection(text), text)

    def test_date_offset_loss_and_rounding_remain_explicit_and_preserve_inputs(self):
        fields = {name: FIELDS[name] for name in ('transaction_date', 'transaction_shares', 'transaction_price_per_share')}
        xml = [{'transaction_date': '2018-01-01-05:00', 'transaction_shares': '1', 'transaction_price_per_share': '10.125'}]
        bulk = [{'TRANS_DATE': '01-JAN-2018', 'TRANS_SHARES': '1', 'TRANS_PRICEPERSHARE': '10.13'}]
        before = copy.deepcopy((xml, bulk))
        result = classification(xml, bulk, fields)
        self.assertEqual(result['category'], 'rounding_and_date_timezone_loss')
        self.assertEqual(result['timezone_date_fields'][0]['original'], '2018-01-01-05:00')
        self.assertFalse(result['bulk_values_applied'])
        self.assertEqual((xml, bulk), before)
        bulk[0]['TRANS_DATE'] = '2017-12-31'
        self.assertEqual(classification(xml, bulk, fields)['category'], 'unexplained_source_difference')

    def test_matching_column_totals_cannot_hide_wrong_row_associations(self):
        fields = {name: FIELDS[name] for name in ('transaction_shares', 'transaction_price_per_share', 'nature_of_ownership')}
        xml = [{'transaction_shares': '1', 'transaction_price_per_share': '10', 'nature_of_ownership': 'Trust'},
               {'transaction_shares': '2', 'transaction_price_per_share': '12', 'nature_of_ownership': ''}]
        bulk = [{'TRANS_SHARES': '1', 'TRANS_PRICEPERSHARE': '10', 'NATURE_OF_OWNERSHIP': ''},
                {'TRANS_SHARES': '2', 'TRANS_PRICEPERSHARE': '12', 'NATURE_OF_OWNERSHIP': 'Trust'}]
        result = classification(xml, bulk, fields)
        self.assertEqual(result['category'], 'row_association_difference')
        self.assertEqual(result['single_field_omissions_that_match'], ['nature_of_ownership'])
        self.assertTrue(result['requires_source_review'])
        xml[0]['transaction_price_per_share'] = '10.004'
        self.assertEqual(classification(xml, bulk, fields)['category'], 'rounding_and_row_association_difference')
        self.assertEqual(classification(xml, bulk[:1], fields)['category'], 'row_count_difference')
        self.assertEqual(classification([{'transaction_shares': ''}], [{'TRANS_SHARES': '0'}],
                                        {'transaction_shares': FIELDS['transaction_shares']})['category'], 'unexplained_source_difference')

    def setup(self, base):
        root = base / 'state'; root.mkdir(); fixtures.AuditBatchTests().seed(root)
        evidence = base / 'audit'; report = audit(root, evidence, workers=1)
        return root, evidence, report['semantic_sha256']

    def test_all_findings_reproduce_and_serial_parallel_evidence_matches(self):
        with tempfile.TemporaryDirectory() as folder:
            base = Path(folder); root, evidence, pin = self.setup(base)
            old = {path.name: file_hash(path) for path in evidence.iterdir() if path.is_file()}
            first = run(root, evidence, base / 'one', pin, workers=1)
            second = run(root, evidence, base / 'two', pin, workers=2)
            self.assertEqual(first['semantic_sha256'], second['semantic_sha256'])
            self.assertEqual(first['findings_reproduced'], 2)
            self.assertEqual(first['original_documents_rechecked'], 2)
            self.assertEqual(first['classification_counts'], {'rounding': 2})
            self.assertGreater(first['original_source_checks'], 0)
            self.assertEqual(first['unexplained_differences'], 0)
            self.assertFalse(first['original_values_changed'])
            self.assertFalse(first['complete_backfill'])
            self.assertEqual(old, {path.name: file_hash(path) for path in evidence.iterdir() if path.is_file()})
            row = json.loads(gzip.decompress((base / 'one/2018Q1.jsonl.gz').read_bytes()))
            self.assertEqual(row['original_rows'][0]['fields']['transaction_price_per_share'], '10.1250')
            self.assertEqual(row['bulk_rows'][0]['fields']['TRANS_PRICEPERSHARE'], '10.13')
            self.assertEqual(row['bulk_rows'][0]['record_ordinal'], 1)

    def test_wrong_pin_corrupt_details_or_original_source_failure_cannot_publish_ledger(self):
        with tempfile.TemporaryDirectory() as folder:
            base = Path(folder); root, evidence, pin = self.setup(base)
            with self.assertRaisesRegex(ValueError, 'pinned source audit'):
                run(root, evidence, base / 'wrong', 'f' * 64, workers=1)
            with patch('insider_pipeline.bulk_review.compare_source', return_value=(Counter(financial_fields=1), [{'field': 'wrong_row'}])):
                with self.assertRaisesRegex(ValueError, 'Original-source verification failed'):
                    run(root, evidence, base / 'failed-source', pin, workers=1)
            self.assertFalse((base / 'failed-source').exists())
            self.assertTrue(list(base.glob('.failed-source.reviewing-*/failure.json')))
            path = evidence / '2018Q1.jsonl.gz'; path.write_bytes(path.read_bytes() + b'corrupt')
            with self.assertRaisesRegex(ValueError, 'details differ'):
                run(root, evidence, base / 'corrupt', pin, workers=1)
            destination = base / 'keep'; destination.mkdir(); (destination / 'keep').write_text('preserved')
            with self.assertRaisesRegex(ValueError, 'fresh directory'):
                run(root, evidence, destination, pin, workers=1)
            self.assertEqual((destination / 'keep').read_text(), 'preserved')

    def test_repinned_false_finding_still_must_reproduce_from_original_sources(self):
        with tempfile.TemporaryDirectory() as folder:
            base = Path(folder); root, evidence, pin = self.setup(base)
            path = evidence / '2018Q1.jsonl.gz'
            finding = json.loads(gzip.decompress(path.read_bytes()))
            finding['unmatched_bulk_rows'][0]['values']['transaction_price_per_share'] = '999'
            path.write_bytes(gzip.compress(canonical(finding) + b'\n', mtime=0))
            report = json.loads((evidence / 'report.json').read_text())
            group = next(item for item in report['groups'] if item['group'] == '2018Q1')
            group.update(details_sha256=file_hash(path), details_bytes=path.stat().st_size)
            semantic = {key: report[key] for key in ('selection_sha256', 'audit_version', 'counts', 'financial_table_comparisons', 'groups')}
            pin = hashlib.sha256(canonical(semantic)).hexdigest(); report['semantic_sha256'] = pin
            (evidence / 'report.json').write_bytes(canonical(report))
            with self.assertRaisesRegex(ValueError, 'could not be reproduced'):
                run(root, evidence, base / 'forged', pin, workers=1)
            self.assertFalse((base / 'forged').exists())

    def test_changed_audit_during_review_cannot_publish_a_stale_pin(self):
        with tempfile.TemporaryDirectory() as folder:
            base = Path(folder); root, evidence, pin = self.setup(base)
            original = review_module.review_group
            def changed(task):
                result = original(task)
                path = evidence / task[3]['details_file']
                path.write_bytes(path.read_bytes() + b'changed after review')
                return result
            with patch('insider_pipeline.bulk_review.review_group', side_effect=changed):
                with self.assertRaisesRegex(ValueError, 'details differ'):
                    run(root, evidence, base / 'changed', pin, workers=1)
            self.assertFalse((base / 'changed').exists())


if __name__ == '__main__':
    unittest.main()
