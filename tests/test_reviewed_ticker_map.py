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

    def test_display_review_rejects_conflicts_and_incomplete_option_proof(self):
        import hashlib
        import json
        base = {'as_of':'2026-09-07', 'mappings':{}, 'display_mappings':{
            '78462F953|PUT': {'ticker':'SPY', 'confidence_tier':'A',
                'match_kind':'underlying_only', 'underlying_cusip':'78462F103',
                'proofs':[{'url':'https://www.sec.gov/example'}]},
        }}
        for field, value in [('confidence_tier','C'), ('match_kind','exact_cusip'),
                             ('underlying_cusip',None), ('proofs',[]), ('ticker','PCGpG')]:
            with self.subTest(field=field):
                document = json.loads(json.dumps(base))
                document['display_mappings']['78462F953|PUT'][field] = value
                raw = json.dumps(document).encode()
                with patch.multiple(r, REVIEW_SHA256=hashlib.sha256(raw).hexdigest(),
                                    REVIEW_MAPPING_COUNT=0, DISPLAY_MAPPING_COUNT=1):
                    with self.assertRaises(r.SecurityMasterError):
                        r.validate_review_bytes(raw, as_of=date(2026,9,7))

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
        self.assertEqual(check_coverage(totals, '2026-06-30', '2026-06-30'), [])
        totals[('2026-06-30', 'resolved')] = 97
        warnings = check_coverage(totals, '2026-06-30', '2026-06-30')
        self.assertEqual(len(warnings), 1)
        self.assertIn('below 98', warnings[0])
        self.assertIn('publication is allowed', warnings[0])

    def test_new_quarter_cannot_hide_behind_frozen_coverage(self):
        totals = Counter({('2026-06-30', 'rows'): 100, ('2026-06-30', 'resolved'): 99,
                          ('2026-09-30', 'rows'): 100, ('2026-09-30', 'resolved'): 97})
        warnings = check_coverage(totals, '2026-06-30', '2026-09-30')
        self.assertEqual(len(warnings), 1)
        self.assertIn('2026-09-30', warnings[0])

    def test_missing_population_still_fails_integrity_check(self):
        with self.assertRaisesRegex(ValueError, 'no EQUITY holding population'):
            check_coverage(Counter(), '2026-06-30', '2026-06-30')


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


class ListedFundSearchTests(unittest.TestCase):
    def test_reviewed_displays_add_search_without_mutating_filing_identity(self):
        import json
        import pipeline as p
        import validate_data as v
        from scripts.incremental_pipeline import registry_identity
        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp) / 'data'
            funds, stocks = data / 'funds', data / 'stocks'
            funds.mkdir(parents=True)
            entries = {}
            holdings = []
            for cusip, kind, ticker, match in [
                ('921937827', 'NOTE', 'BSV', 'exact_cusip'),
                ('78462F953', 'PUT', 'SPY', 'underlying_only'),
            ]:
                display = {'ticker':ticker, 'match_kind':match, 'confidence_tier':'A'}
                if kind == 'PUT':
                    display['underlying_cusip'] = '78462F103'
                entries[cusip] = {'type':kind, 'ticker':None, 'mapping_status':'unresolved',
                    'name':'Example security', 'display_mappings':{kind:display}}
                holdings.append({'cusip':cusip, 'holding_type':kind, 'ticker':None,
                    'issuer':'Example security', 'value':100, 'shares':10})
            fund = {'cik':123456, 'name':'Example manager', 'quarters':[{
                'report_date':'2026-06-30', 'filing_date':'2026-08-14',
                'total_value':200, 'num_holdings':2, 'holdings':holdings}]}
            path = funds / '123456.json'
            path.write_text(json.dumps(fund)); original = path.read_bytes()
            with patch.multiple(p, DATA_DIR=data, FUNDS_DIR=funds, STOCKS_DIR=stocks,
                    INDEX_PATH=data/'index.json', FUNDS_INDEX_PATH=data/'funds-index.json',
                    load_cusip_registry=lambda:entries):
                p.regenerate_stock_files_and_index(state={})
            index = json.loads((data/'index.json').read_text())
            self.assertEqual({x['ticker'] for x in index['tickers']}, {'BSV','SPY'})
            self.assertEqual(path.read_bytes(), original)
            self.assertEqual(json.loads((stocks/'921937827__NOTE.json').read_text())['ticker'], '921937827')
            self.assertIsNone(p._registry_position_ticker(entries['921937827'], 'NOTE'))
            self.assertIsNone(v.expected_registry_position_ticker(entries['921937827'], 'NOTE'))
            self.assertIsNone(p._registry_search_ticker(entries['78462F953'], 'CALL'))
            errors = []
            v.validate_index(index, {'123456':path},
                {x.stem:x for x in stocks.glob('*.json')}, entries, errors, [])
            self.assertEqual(errors, [])
            self.assertIn('display_mappings', registry_identity(entries)['921937827'])

    def test_display_projection_omits_proofs_and_blocks_resolved_conflicts(self):
        review = {'display_mappings': {'78462F953|PUT': {
            'ticker':'SPY', 'match_kind':'underlying_only', 'underlying_cusip':'78462F103',
            'proofs':[{'url':'https://www.sec.gov/example'}], 'notes':['private review'],
        }}}
        projected = r.public_display_mappings('78462F953', review)
        self.assertEqual(set(projected), {'PUT'})
        self.assertNotIn('proofs', projected['PUT'])
        self.assertNotIn('notes', projected['PUT'])
        with self.assertRaisesRegex(r.SecurityMasterError, 'conflict'):
            r.assert_display_compatibility({'display_mappings':projected, 'underlying_ticker':'OTHER'})

    def test_historical_preferred_row_keeps_symbol_without_duplicate_fund_search(self):
        import json
        import pipeline as p
        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp) / 'data'
            funds = data / 'funds'
            stocks = data / 'stocks'
            funds.mkdir(parents=True)
            entry = {'type': 'EQUITY', 'security_kind': 'ETF', 'name': 'Example fund',
                     'ticker': 'FUND', 'mapping_status': 'resolved',
                     'ticker_source': r.REVIEW_SOURCE, 'ticker_as_of': '2026-09-07',
                     'instrument_mappings': {'PREF': {'ticker': 'FUND',
                         'ticker_source': r.REVIEW_SOURCE, 'ticker_as_of': '2026-09-07'}}}
            fund = {'cik': 123456, 'name': 'Example manager', 'quarters': [{
                'report_date': '2026-06-30', 'filing_date': '2026-08-14',
                'total_value': 200, 'num_holdings': 2,
                'holdings': [{'cusip': '123456789', 'holding_type': kind,
                    'issuer': 'Example fund', 'ticker': 'FUND', 'value': 100, 'shares': 10}
                    for kind in ('EQUITY', 'PREF')]}]}
            path = funds / '123456.json'
            path.write_text(json.dumps(fund))
            original = path.read_bytes()
            with patch.multiple(p, DATA_DIR=data, FUNDS_DIR=funds, STOCKS_DIR=stocks,
                    INDEX_PATH=data / 'index.json', FUNDS_INDEX_PATH=data / 'funds-index.json',
                    load_cusip_registry=lambda: {'123456789': entry}):
                p.regenerate_stock_files_and_index(state={})
            index = json.loads((data / 'index.json').read_text())
            self.assertEqual([row['stock_id'] for row in index['tickers']], ['123456789'])
            self.assertEqual(json.loads((stocks / '123456789__PREF.json').read_text())['ticker'], 'FUND')
            self.assertEqual(path.read_bytes(), original)

class ReviewedFundNameTests(unittest.TestCase):
    def setUp(self):
        import sec_security_master as sm
        self.state = sm.empty_source_state()
        self.state['updated_at'] = '2026-09-08T12:00:00Z'
        self.state['sources'][sm.SEC_FUND_TICKERS_URL] = {
            'url': sm.SEC_FUND_TICKERS_URL, 'kind': 'sec_fund_tickers',
            'sha256': 'd' * 64, 'accepted_at': '2026-09-08T12:00:00Z',
            'symbols': ['DAPR'], 'symbol_titles': {}, 'symbol_exchanges': {},
            'symbol_count': 1, 'fund_records': [{'symbol': 'DAPR', 'cik': '0001000000',
                'series_id': 'S000002745', 'class_id': 'C000007635'}],
        }
        url = sm.sec_fund_series_url('0001000000')
        self.state['sources'][url] = {
            'url': url, 'kind': 'sec_fund_series', 'sha256': 'e' * 64,
            'accepted_at': '2026-09-08T12:00:00Z',
            'last_successful_check_at': '2026-09-08T12:00:00Z', 'cik': '0001000000',
            'series_names': {'S000002745': 'FT Vest U.S. Equity Deep Buffer ETF - April'},
            'class_names': {'C000007635': 'ETF Shares'},
        }
        self.key = '33740U802|EQUITY'
        self.master = {'source_state_sha256': sm.source_state_sha256(self.state),
            'records': {self.key: {'cusip': '33740U802', 'instrument_type': 'EQUITY',
                'mapping_status': 'ambiguous', 'ticker': None}}}
        self.accepted = {'ticker': 'DAPR', 'ticker_as_of': '2026-09-07',
            'assertion': {'cusip': '33740U802', 'symbol': 'DAPR', 'instrument': 'FUND_SHARE'}}
        self.review = {'mappings': {self.key: self.accepted}}

    def apply(self, root=Path('.'), **kwargs):
        with patch.object(r, 'load_review', return_value=self.review):
            return r.apply_review(self.master, root, **kwargs)['records'][self.key]

    def test_reviewed_fund_gets_exact_sec_name_without_changing_raw_record(self):
        row = self.apply(source_state=self.state)
        self.assertEqual('FT Vest U.S. Equity Deep Buffer ETF - April — ETF Shares',
                         row['fund_series_name'])
        self.assertEqual('e' * 64, row['fund_series_evidence']['sha256'])
        self.assertEqual('ambiguous', self.master['records'][self.key]['mapping_status'])
        self.assertNotIn('fund_series_name', self.master['records'][self.key])

    def test_source_binding_rejects_changed_names(self):
        import sec_security_master as sm
        url = sm.sec_fund_series_url('0001000000')
        self.state['sources'][url]['series_names']['S000002745'] = 'Wrong Fund'
        with self.assertRaisesRegex(r.SecurityMasterError, 'bound SEC'):
            self.apply(source_state=self.state)

    def test_retired_or_nonfund_review_cannot_name_a_current_fund(self):
        for field, value in [('instrument', 'COMMON_SHARE'), ('cusip', '33740U999'),
                             ('symbol', 'OTHER')]:
            with self.subTest(field=field):
                prior = self.accepted['assertion'][field]
                self.accepted['assertion'][field] = value
                self.assertNotIn('fund_series_name', self.apply(source_state=self.state))
                self.accepted['assertion'][field] = prior
        self.accepted['price_lookup_allowed'] = False
        self.assertNotIn('fund_series_name', self.apply(source_state=self.state))

    def test_disk_source_and_supplied_source_produce_same_projection(self):
        import sec_security_master as sm
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / '.cache').mkdir()
            sm.save_source_state(self.state, root / '.cache/sec_source_state.json')
            self.assertEqual(self.apply(source_state=self.state), self.apply(root))
            r._cached_fund_names.cache_clear()

    def test_ambiguous_sec_fund_symbol_withholds_product_name(self):
        import sec_security_master as sm
        source = self.state['sources'][sm.SEC_FUND_TICKERS_URL]
        source['fund_records'].append({'symbol':'DAPR','cik':'0001000001',
                                      'series_id':'S000002746','class_id':'C000007636'})
        self.master['source_state_sha256'] = sm.source_state_sha256(self.state)
        self.assertNotIn('fund_series_name', self.apply(source_state=self.state))

    def test_cache_cannot_reuse_names_for_a_different_source_digest(self):
        import sec_security_master as sm
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / '.cache').mkdir()
            sm.save_source_state(self.state, root / '.cache/sec_source_state.json')
            self.assertIn('fund_series_name', self.apply(root))
            self.master['source_state_sha256'] = '0' * 64
            with self.assertRaisesRegex(r.SecurityMasterError, 'bound SEC'):
                self.apply(root)
            r._cached_fund_names.cache_clear()
