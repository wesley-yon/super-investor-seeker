"""Protect reviewed identities without weakening the raw SEC evidence contract."""
from collections import Counter
from datetime import date
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import reviewed_ticker_map as r
from scripts.validate_reviewed_coverage import check_coverage


class ReviewedIdentityTests(unittest.TestCase):
    def setUp(self):
        self.review = {'mappings': {'931142103|EQUITY': {
            'ticker': 'WMT', 'ticker_as_of': '2026-09-07',
        }}}
        self.master = {'records': {'931142103|EQUITY': {
            'cusip': '931142103', 'instrument_type': 'EQUITY',
            'mapping_status': 'unresolved', 'ticker': None,
            'official_13f': {'status': 'active'},
        }}}

    def apply(self):
        with patch.object(r, 'load_review', return_value=self.review):
            return r.apply_review(self.master, Path('.'))

    def test_exact_key_only_and_no_raw_master_mutation(self):
        resolved = self.apply()
        self.assertEqual(resolved['records']['931142103|EQUITY']['ticker'], 'WMT')
        self.assertEqual(resolved['records']['931142103|EQUITY']['ticker_source'], r.REVIEW_SOURCE)
        self.assertIsNone(self.master['records']['931142103|EQUITY']['ticker'])
        self.assertNotIn('931142103|NOTE', resolved['records'])
        self.assertNotIn('931142103|CALL', resolved['records'])

    def test_same_ticker_keeps_current_sec_provenance(self):
        self.master['records']['931142103|EQUITY'].update(
            mapping_status='resolved', ticker='WMT', ticker_source='sec_ixbrl')
        self.assertEqual(self.apply()['records']['931142103|EQUITY']['ticker_source'], 'sec_ixbrl')

    def test_conflicting_sec_symbol_blocks_instead_of_overwriting(self):
        self.master['records']['931142103|EQUITY'].update(
            mapping_status='resolved', ticker='OTHER')
        with self.assertRaisesRegex(r.SecurityMasterError, 'conflict'):
            self.apply()
        self.assertEqual(self.master['records']['931142103|EQUITY']['ticker'], 'OTHER')

    def test_missing_sec_key_can_use_exact_review(self):
        self.master['records'] = {}
        self.assertEqual(self.apply()['records']['931142103|EQUITY']['ticker'], 'WMT')

    def test_refresh_disappearance_preserves_reviewed_identity(self):
        self.master['records']['931142103|EQUITY']['mapping_status'] = 'no_listed_symbol'
        self.assertEqual(self.apply()['records']['931142103|EQUITY']['ticker'], 'WMT')

    def test_retired_identity_is_never_quote_eligible(self):
        self.review['mappings']['931142103|EQUITY']['price_lookup_allowed'] = False
        row = self.apply()['records']['931142103|EQUITY']
        self.assertFalse(row['price_lookup_allowed'])
        self.assertEqual(row['trading_status'], 'historical_retired_identity')

    def test_unapproved_bytes_are_rejected(self):
        with self.assertRaisesRegex(r.SecurityMasterError, 'checksum'):
            r.validate_review_bytes(b'{"mappings":{}}')

    def test_missing_required_map_blocks(self):
        with tempfile.TemporaryDirectory() as root:
            with self.assertRaisesRegex(r.SecurityMasterError, 'missing'):
                r.load_review(Path(root), required=True)
            self.assertIsNone(r.load_review(Path(root)))

    def test_expired_review_blocks_instead_of_dropping_tickers(self):
        import hashlib
        raw = b'{"as_of":"2026-09-07","mappings":{}}'
        with patch.object(r, 'REVIEW_SHA256', hashlib.sha256(raw).hexdigest()):
            with self.assertRaisesRegex(r.SecurityMasterError, 'revalidation'):
                r.validate_review_bytes(raw, as_of=date(2026, 12, 7))

    def test_coverage_uses_integer_boundary(self):
        totals = Counter({('2026-06-30', 'rows'): 100, ('2026-06-30', 'resolved'): 98})
        check_coverage(totals, '2026-06-30', '2026-06-30')
        totals[('2026-06-30', 'resolved')] = 97
        with self.assertRaisesRegex(ValueError, 'below 98'):
            check_coverage(totals, '2026-06-30', '2026-06-30')

    def test_new_quarter_cannot_hide_behind_frozen_coverage(self):
        totals = Counter({('2026-06-30', 'rows'): 100, ('2026-06-30', 'resolved'): 99,
                          ('2026-09-30', 'rows'): 100, ('2026-09-30', 'resolved'): 97})
        with self.assertRaisesRegex(ValueError, '2026-09-30'):
            check_coverage(totals, '2026-06-30', '2026-09-30')


class InstrumentProjectionTests(unittest.TestCase):
    def test_exact_secondary_type_survives_aggregate_classification(self):
        import pipeline as p
        import validate_data as v
        records = {'931142103|EQUITY': {
            'reviewed_identity': True, 'mapping_status': 'resolved',
            'ticker': 'WMT', 'ticker_source': r.REVIEW_SOURCE,
            'ticker_as_of': '2026-09-07',
        }}
        typed = r.public_instrument_mappings('931142103', 'NOTE', records)
        entry = {'type': 'NOTE', 'ticker': None, 'mapping_status': 'unresolved',
                 'instrument_mappings': typed}
        self.assertEqual(p._registry_position_ticker(entry, 'EQUITY'), 'WMT')
        self.assertEqual(v.expected_registry_position_ticker(entry, 'EQUITY'), 'WMT')
        self.assertIsNone(p._registry_position_ticker(entry, 'NOTE'))
        self.assertIsNone(p._registry_position_ticker(entry, 'WARRANT'))
        self.assertIsNone(p._registry_position_ticker(entry, 'CALL'))

    def test_secondary_type_requires_its_own_reviewed_key(self):
        records = {'931142103|EQUITY': {
            'mapping_status': 'resolved', 'ticker': 'WMT',
            'ticker_source': 'sec_ftd', 'ticker_as_of': '2026-09-07',
        }}
        self.assertEqual(r.public_instrument_mappings('931142103', 'NOTE', records), {})
        records['931142103|EQUITY']['reviewed_identity'] = True
        self.assertEqual(r.public_instrument_mappings('931142103', 'EQUITY', records), {})

    def test_secondary_mapping_carries_retirement_restriction(self):
        record = {'reviewed_identity': True, 'mapping_status': 'resolved',
                  'ticker': 'WMT', 'ticker_source': r.REVIEW_SOURCE,
                  'ticker_as_of': '2026-09-07', 'price_lookup_allowed': False,
                  'trading_status': 'historical_retired_identity', 'private_proofs': []}
        typed = r.public_instrument_mappings('931142103', 'PREF', {'931142103|EQUITY': record})
        self.assertFalse(typed['EQUITY']['price_lookup_allowed'])
        self.assertNotIn('private_proofs', typed['EQUITY'])
