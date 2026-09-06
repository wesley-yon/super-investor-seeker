from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import quantity_estimation as q
from scripts.quantity_policy import import_exports, prepare_requests

DAY = '2025-12-31'
CUSIP = '037833100'
SOURCE = {'accession': 'a', 'url': 'https://www.sec.gov/Archives/a', 'sha256': 'a' * 64}


def peer(cik, price=100, kind='EQUITY', unit='SH'):
    return {'cik': str(cik), 'report_date': DAY, 'cusip': CUSIP, 'instrument_type': kind, 'unit': unit, 'value': price * 10, 'quantity': 10, 'source': SOURCE}


def market_fixture():
    identity = {'cusip': CUSIP, 'instrument_type': 'EQUITY', 'ticker': 'AAPL', 'ticker_source': 'sec_ftd', 'interval': {'symbol': 'AAPL', 'first_seen': '2020-01-01', 'last_seen': '2026-06-30', 'sources': [SOURCE]}}
    company = {'companyKey': 'NASDAQ_AAPL', 'ticker': 'AAPL', 'micCode': 'XNAS', 'tradingCurrency': 'USD'}
    listing = {'ticker': 'AAPL', 'operatingMic': 'XNAS', 'tradingCurrency': 'USD', 'exchangeCode': 'NASDAQ', 'listingFiscalIdentifier': 'f123'}
    export = {'companyKey': company['companyKey'], 'listing': listing, 'seriesThrough': '2026-06-30', 'prices': [{'date': DAY, 'closePrice': 25.0, 'volume': 1000}], 'splits': [{'exDate': '2026-02-01', 'splitType': 'Stock Split', 'rate': 4}], 'fetchedAt': '2026-09-06T00:00:00Z'}
    request = {'cusip': CUSIP, 'report_date': DAY, 'sec_identity': identity}
    return company, export, request


class QuantityPolicyTests(unittest.TestCase):
    def test_quarter_sessions(self):
        self.assertEqual('2024-03-28', q.quarter_close_date('2024-03-31'))
        self.assertEqual('2023-12-29', q.quarter_close_date('2023-12-31'))
        self.assertEqual(DAY, q.quarter_close_date(DAY))
        for day in ('2009-12-31', '2025-02-28', '2025-03-30'):
            with self.assertRaises(ValueError):
                q.quarter_close_date(day)

    def test_peers_require_three_other_filers_and_agreement(self):
        key = (DAY, CUSIP, 'EQUITY')
        self.assertIsNone(q.peer_reference(key, [peer(1), peer(2), peer(3)], exclude_cik='1'))
        self.assertIsNone(q.peer_reference(key, [peer(1), peer(2), peer(3, 1000)], exclude_cik='4'))
        ref = q.peer_reference(key, [peer(i, 100 + i / 100) for i in range(10)], exclude_cik='99')
        self.assertEqual([], q.validate_reference(ref))
        self.assertEqual(6, ref['peer_count'])
        self.assertEqual(10, ref['screened_inlier_count'])
        self.assertAlmostEqual(100.045, ref['price'])
        changed = deepcopy(ref); changed['peers'][0]['observations'][0]['quantity'] = 0
        self.assertTrue(q.validate_reference(changed))

    def test_market_split_basis_and_rejections(self):
        company, export, request = market_fixture()
        ref = q.fiscal_reference(export, request, company)
        self.assertEqual(100, ref['price'])
        self.assertEqual([], q.validate_reference(ref))
        mutations = [
            lambda e: e['listing'].update(ticker='OTHER'),
            lambda e: e['listing'].update(tradingCurrency='CAD'),
            lambda e: e['prices'][0].update(volume=0),
            lambda e: e['prices'][0].update(closePrice=float('nan')),
            lambda e: e['prices'][0].update(date='2025-12-30'),
            lambda e: e['splits'][0].update(splitType='Reverse Stock Split'),
            lambda e: e.update(seriesThrough='2025-12-01'),
            lambda e: e['prices'].append(deepcopy(e['prices'][0])),
        ]
        for mutate in mutations:
            invalid = deepcopy(export); mutate(invalid)
            with self.subTest(export=invalid), self.assertRaises(ValueError):
                q.fiscal_reference(invalid, request, company)
        wrong = deepcopy(request); wrong['cusip'] = 'BAD'
        with self.assertRaises(ValueError):
            q.fiscal_reference(export, wrong, company)
        for malformed in (None, [], {'peers': [None], 'method': 'sec_same_quarter_median'}):
            self.assertTrue(q.validate_reference(malformed))

    def write_funds(self, root, value=250):
        funds = root / 'data/funds'; funds.mkdir(parents=True)
        for cik in range(1, 5):
            holding = {'cusip': CUSIP, 'class': 'COM', 'holding_type': 'EQUITY', 'shares': 10 if cik < 4 else 0, 'value': 1000 if cik < 4 else value, 'share_amount_type': 'SH', 'accession': 'a'}
            q.atomic_json(funds / f'{cik}.json', {'cik': cik, 'quarters': [{'report_date': DAY, 'holdings': [holding], 'reported_identity_sources': [SOURCE]}]})
        return funds

    def build(self, root, funds):
        return q.build_plan(funds, evidence_path=root / '.cache/evidence.json', market_path=root / '.cache/market.json')

    def apply(self, root, funds, plan):
        return q.apply_plan(plan, funds, evidence_path=root / '.cache/evidence.json', request_path=root / '.cache/requests.json')

    def test_end_to_end_idempotent_frozen_estimate_and_exclusion(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); funds = self.write_funds(root)
            plan = self.build(root, funds)
            result = self.apply(root, funds, plan)
            self.assertEqual(1, result['estimated_rows'])
            row = json.loads((funds / '4.json').read_text())['quarters'][0]['holdings'][0]
            self.assertEqual(2.5, row['shares'])
            self.assertEqual(0, self.apply(root, funds, self.build(root, funds))['files_changed'])
            # Estimated target never becomes a price voter for another target.
            observations = q.collect_peer_observations(funds, {(DAY, CUSIP, 'EQUITY')})
            self.assertEqual({'1', '2', '3'}, {r['cik'] for r in observations[(DAY, CUSIP, 'EQUITY')]})
            (funds / '1.json').unlink()
            self.assertEqual([], q.validate_quantity_annotation(row, DAY, plan['evidence'], cik='4'))

    def test_zero_value_zero_quantity_and_imputed_peers_are_excluded(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); funds = self.write_funds(root)
            for cik, change in [(1, {'shares': 0}), (2, {'value': 0}), (3, {'shares_imputed': True})]:
                path = funds / f'{cik}.json'; fund = json.loads(path.read_text()); fund['quarters'][0]['holdings'][0].update(change); q.atomic_json(path, fund)
            self.assertEqual({}, q.collect_peer_observations(funds, {(DAY, CUSIP, 'EQUITY')}))

    def test_subunit_target_unknown_even_when_rounding_would_reach_one(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); funds = self.write_funds(root, value=99.9999999)
            row = self.build(root, funds)['targets'][0]['revised']
            self.assertEqual('below_one_reported_unit', row['quantity_unknown_reason'])
            self.assertEqual(99.9999999, row['value'])
            self.assertEqual(0, row['shares'])

    def test_market_preferred_and_options_remain_separate(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); funds = self.write_funds(root)
            company, export, request = market_fixture(); export['prices'][0]['closePrice'] = 50
            ref = q.fiscal_reference(export, request, company)
            q.atomic_json(root / '.cache/market.json', {'schema_version': 1, 'references': {q.canonical_json_hash(ref): ref}})
            path = funds / '4.json'; fund = json.loads(path.read_text()); option = deepcopy(fund['quarters'][0]['holdings'][0]); option.update(holding_type='CALL', put_call='CALL'); fund['quarters'][0]['holdings'].append(option); q.atomic_json(path, fund)
            plan = self.build(root, funds)
            self.assertEqual([1.25, 1.25], [r['revised']['shares'] for r in plan['targets']])
            self.assertEqual({'EQUITY', 'CALL'}, {r['instrument_type'] for r in plan['evidence']['references'].values()})
            self.apply(root, funds, plan)

    def test_entire_plan_preflight_prevents_partial_mutations(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); funds = self.write_funds(root)
            q.atomic_json(funds / '5.json', {'cik': 5, 'quarters': [{'report_date': DAY, 'holdings': [{'cusip': CUSIP, 'shares': 0, 'value': 500, 'share_amount_type': 'SH', 'holding_type': 'EQUITY'}]}]})
            plan = self.build(root, funds); before = (funds / '4.json').read_bytes()
            (funds / '5.json').write_text('{}')
            with self.assertRaises(q.QuantityEstimationError):
                self.apply(root, funds, plan)
            self.assertEqual(before, (funds / '4.json').read_bytes())
            self.assertFalse((root / '.cache/evidence.json').exists())

    def test_plan_cannot_modify_reported_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); funds = self.write_funds(root)
            plan = self.build(root, funds); plan['targets'][0]['revised']['reported_issuer'] = 'tampered'
            with self.assertRaises(q.QuantityEstimationError):
                self.apply(root, funds, plan)

    def test_parallel_and_serial_observations_and_targets_match(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); funds = self.write_funds(root)
            for cik in range(5, 36):
                q.atomic_json(funds / f'{cik}.json', {'cik': cik, 'quarters': []})
            paths = sorted(funds.glob('*.json'))
            self.assertEqual([r for path in paths for r in q._read_targets(path)], q.collect_targets(funds))
            keys = {(DAY, CUSIP, 'EQUITY')}
            with mock.patch.object(q.os, 'cpu_count', return_value=1):
                serial = q.collect_peer_observations(funds, keys)
            self.assertEqual(serial, q.collect_peer_observations(funds, keys))

    def test_import_reports_rejections_and_keeps_valid_references(self):
        company, export, request = market_fixture()
        with tempfile.TemporaryDirectory() as tmp:
            market = Path(tmp) / 'market.json'
            result = import_exports([{'company': company, 'requests': [request]}], [export], market)
            self.assertEqual(1, result['accepted'])
            export['prices'] = []
            result = import_exports([{'company': company, 'requests': [request]}], [export], market)
            self.assertEqual(1, len(result['rejected']))
            self.assertEqual(1, len(q.load_book(market)['references']))

    def test_request_preparation_fails_closed_without_sec_identity(self):
        company, _, request = market_fixture(); company.update(name='Apple Inc.', cik=320193, availableDatasets=['stock_prices'])
        result = prepare_requests([request], [company], {'records': {}}, {})
        self.assertEqual([], result['groups'])
        self.assertEqual(1, result['skipped']['no_exact_SEC_equity_mapping'])


if __name__ == '__main__':
    unittest.main()
