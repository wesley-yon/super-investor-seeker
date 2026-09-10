import copy
import unittest
from insider_pipeline.parser import parse_ownership_xml
from insider_pipeline.source_audit import compare_source
from test_parser import ACCESSION, URL, filing, transaction, FOOTNOTES


class CompleteSourceAuditTests(unittest.TestCase):
    def fixture(self):
        extra = '<futureBlock unit="units"><item>first</item><item>second</item></futureBlock>'
        xml = filing('<nonDerivativeTable>' + transaction(extra=extra) + '</nonDerivativeTable>' + FOOTNOTES)
        return xml, parse_ownership_xml(xml, ACCESSION, URL)

    def test_all_entities_raw_unknown_fields_and_exact_calculation(self):
        xml, parsed = self.fixture()
        counts, failures = compare_source(xml, parsed)
        self.assertEqual(failures, [])
        for category in ['owner_fields', 'footnotes_fields', 'signatures_fields', 'raw_paths', 'footnote_links', 'calculated_values', 'owner_joins']:
            self.assertGreater(counts[category], 0)

    def test_corrupt_owner_note_signature_raw_path_join_and_value_detected(self):
        xml, parsed = self.fixture()
        broken = copy.deepcopy(parsed)
        broken['owners'][0]['officer_title'] = 'CFO'
        broken['footnotes'][0]['text'] = 'Altered text'
        broken['signatures'][0]['signature_date'] = '2026-01-01'
        broken['filing']['raw_fields']['nonDerivativeTable/nonDerivativeTransaction/futureBlock/item[1]'] = 'changed'
        broken['transactions'][0]['normalized_footnote_refs'] = {}
        broken['transactions'][0]['transaction_value'] = '25.31'
        counts, failures = compare_source(xml, broken)
        categories = {r['category'] for r in failures}
        self.assertTrue({'owner_fields', 'footnotes_fields', 'signatures_fields', 'raw_paths', 'footnote_links', 'calculated_values', 'owner_joins'}.issubset(categories))

    def test_zero_and_missing_price_do_not_become_the_same_value(self):
        for price in ['0', None]:
            xml = filing('<nonDerivativeTable>' + transaction(price=price) + '</nonDerivativeTable>' + FOOTNOTES)
            parsed = parse_ownership_xml(xml, ACCESSION, URL)
            counts, failures = compare_source(xml, parsed)
            self.assertEqual(failures, [])

    def test_filing_date_must_match_sec_submission_header(self):
        xml, parsed = self.fixture()
        parsed = parse_ownership_xml(xml, ACCESSION, URL, '2026-09-03')
        body = (b'<SEC-HEADER>\nACCESSION NUMBER: ' + ACCESSION.encode() +
                b'\nFILED AS OF DATE: 20260903\nCONFORMED SUBMISSION TYPE: 4\n</SEC-HEADER>\n<XML>' + xml + b'</XML>')
        counts, failures = compare_source(body, parsed)
        self.assertEqual(counts['submission_header_fields'], 3)
        self.assertEqual(failures, [])
        changed_header = body.replace(b'FILED AS OF DATE: 20260903', b'FILED AS OF DATE: 20260904')
        counts, failures = compare_source(changed_header, parsed)
        self.assertEqual(failures[0]['field'], 'filing_date')
        parsed['footnotes'][0]['accession'] = 'wrong'
        counts, failures = compare_source(body, parsed)
        self.assertTrue(any(r.get('category') == 'filing_joins' for r in failures))
