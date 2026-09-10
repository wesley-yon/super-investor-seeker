import unittest
from insider_pipeline.audit import compare, compare_original, value
from insider_pipeline.parser import parse_ownership_xml
from test_parser import ACCESSION, URL, filing, transaction, FOOTNOTES


class AuditTests(unittest.TestCase):
    def test_missing_zero_precision_and_wrong_row_association(self):
        self.assertNotEqual(value('', 'number'), value('0.0', 'number'))
        self.assertEqual(value('123.4500', 'number'), '123.45')
        fields = ['TRANS_SHARES', 'TRANS_PRICEPERSHARE', 'NATURE_OF_OWNERSHIP']
        xml = [{'transaction_shares': '1', 'transaction_price_per_share': '10.125', 'nature_of_ownership': 'Trust'},
               {'transaction_shares': '2', 'transaction_price_per_share': '12', 'nature_of_ownership': ''}]
        bulk = [{'TRANS_SHARES': '1.0', 'TRANS_PRICEPERSHARE': '10.13', 'NATURE_OF_OWNERSHIP': 'Trust'},
                {'TRANS_SHARES': '2.0', 'TRANS_PRICEPERSHARE': '12.0', 'NATURE_OF_OWNERSHIP': ''}]
        self.assertEqual(compare(xml, bulk, fields)['status'], 'bulk_rounding')
        bulk[0]['NATURE_OF_OWNERSHIP'], bulk[1]['NATURE_OF_OWNERSHIP'] = '', 'Trust'
        result = compare(xml, bulk, fields)
        self.assertEqual(result['status'], 'review')
        self.assertEqual(len(result['unmatched_bulk_rows']), 2)
        self.assertEqual(xml[0]['transaction_price_per_share'], '10.125')

    def test_original_xml_exposes_value_assigned_to_wrong_row(self):
        xml = filing('<nonDerivativeTable>' + transaction() + transaction(shares='17') + '</nonDerivativeTable>' + FOOTNOTES)
        parsed = parse_ownership_xml(xml, ACCESSION, URL)
        checked, failures = compare_original(xml, parsed)
        self.assertGreater(checked, 0)
        self.assertEqual(failures, [])
        parsed['transactions'][0]['transaction_shares'] = '17'
        checked, failures = compare_original(xml, parsed)
        self.assertEqual(failures[0]['field'], 'transaction_shares')


if __name__ == '__main__':
    unittest.main()
