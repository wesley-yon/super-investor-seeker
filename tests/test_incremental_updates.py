from __future__ import annotations

import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pipeline as p
import validate_data as v
from incremental_validation import ValidationCache
from scripts import incremental_pipeline as inc
from scripts import refresh_recent_13f_filings as feed


REGISTRY = {cusip: {'ticker': None, 'name': issuer, 'type': 'EQUITY', 'mapping_status': 'unresolved'}
            for cusip, issuer in [('037833100', 'APPLE INC'), ('594918104', 'MICROSOFT CORP')]}


def fund(cik, cusip='037833100', date='2026-06-30', kind='EQUITY'):
    holding = {'cusip': cusip, 'ticker': None, 'issuer': REGISTRY[cusip]['name'],
               'reported_issuer': REGISTRY[cusip]['name'], 'class': 'COM',
               'holding_type': kind, 'shares': 10, 'value': 1000}
    if kind in {'CALL', 'PUT'}:
        holding['put_call'] = kind
    return {'cik': cik, 'name': f'Fund {cik}', 'quarters': [{
        'report_date': date, 'filing_date': '2026-08-14', 'num_holdings': 1,
        'total_value': 1000, 'holdings': [holding]}]}


class IncrementalGenerationTests(unittest.TestCase):
    def test_changed_dependency_outputs_equal_full_rebuild(self):
        for scenario in ('amendment_removal', 'new_option', 'deleted_fund', 'calendar_rollover', 'withheld', 'registry_change'):
            with self.subTest(scenario=scenario), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                data = root / 'data'
                funds = data / 'funds'
                funds.mkdir(parents=True)
                original = {1: fund(1), 2: fund(2, '594918104'), 3: fund(3, '594918104')}
                for cik, obj in original.items():
                    (funds / f'{cik}.json').write_text(json.dumps(obj))
                registry = copy.deepcopy(REGISTRY)
                with mock.patch.multiple(p, DATA_DIR=data, FUNDS_DIR=funds, STOCKS_DIR=data / 'stocks',
                                         INDEX_PATH=data / 'index.json', FUNDS_INDEX_PATH=data / 'funds-index.json',
                                         load_cusip_registry=mock.Mock(side_effect=lambda: registry)):
                    p.regenerate_stock_files_and_index(state={})
                    before = inc.inventory(registry)
                    state = {}
                    changed_cusips, changed_ciks = set(), set()
                    if scenario == 'amendment_removal':
                        original[1]['quarters'][0].update(holdings=[], total_value=0, num_holdings=0)
                    elif scenario == 'new_option':
                        original[1] = fund(1, kind='CALL')
                    elif scenario == 'deleted_fund':
                        (funds / '1.json').unlink()
                        del original[1]
                    elif scenario == 'calendar_rollover':
                        original[2] = fund(2, '594918104', '2026-09-30')
                        original[3] = fund(3, '594918104', '2026-09-30')
                    elif scenario == 'withheld':
                        state = {'quarter_health_pending': {'1:2026-06-30': {'cik': 1, 'report_date': '2026-06-30'}}}
                        changed_ciks = {'1'}
                    elif scenario == 'registry_change':
                        registry['037833100']['name'] = 'APPLE NEW DISPLAY NAME'
                        changed_cusips = {'037833100'}
                    for cik, obj in original.items():
                        (funds / f'{cik}.json').write_text(json.dumps(obj))
                    _, ids = inc.affected(before, inc.inventory(registry), changed_cusips, changed_ciks)
                    unchanged = data / 'stocks/594918104.json'
                    old_inode = unchanged.stat().st_ino
                    p.regenerate_stock_files_and_index(state=state, stock_ids=ids)
                    actual = {str(path.relative_to(data)): path.read_bytes() for path in data.rglob('*.json')}
                    if scenario in {'amendment_removal', 'new_option', 'deleted_fund', 'withheld', 'registry_change'}:
                        self.assertEqual(old_inode, unchanged.stat().st_ino)
                    p.regenerate_stock_files_and_index(state=state)
                    expected = {str(path.relative_to(data)): path.read_bytes() for path in data.rglob('*.json')}
                    self.assertEqual(expected, actual)

    def test_capture_serializes_public_state_and_detects_changed_status(self):
        state = {'quarter_health_pending': {'1:x': {'cik': 1, 'report_date': '2026-06-30'}}}
        normalized = inc.public_state(state)
        self.assertEqual(normalized, json.loads(json.dumps(normalized)))
        self.assertIn('1', normalized['withheld'])


class IncrementalValidationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.funds = self.root / 'data/funds'
        self.stocks = self.root / 'data/stocks'
        self.funds.mkdir(parents=True)
        self.stocks.mkdir()
        self.cache_path = self.root / '.cache/validation_cache.sqlite3'
        self.registry = copy.deepcopy(REGISTRY)
        for cik in range(1, 6):
            (self.funds / f'{cik}.json').write_text(json.dumps(fund(cik)))
        (self.funds / '6.json').write_text(json.dumps(fund(6, '594918104')))
        self.patch = mock.patch.multiple(v, ROOT=self.root, FUNDS_DIR=self.funds, STOCKS_DIR=self.stocks)
        self.patch.start()
        self.addCleanup(self.patch.stop)
        with mock.patch.multiple(p, DATA_DIR=self.root / 'data', FUNDS_DIR=self.funds, STOCKS_DIR=self.stocks,
                                 INDEX_PATH=self.root / 'data/index.json', FUNDS_INDEX_PATH=self.root / 'data/funds-index.json',
                                 load_cusip_registry=mock.Mock(return_value=self.registry)):
            p.regenerate_stock_files_and_index(state={})

    def compare(self, *, expected_valid=True, reuse=True):
        full_errors, full_quality, full_splits = [], {}, {}
        full_funds = v.validate_funds(full_errors, self.registry, full_quality)
        full_stocks = v.validate_stocks(full_errors, full_funds[3], full_funds[4], full_splits, registry=self.registry)
        cache = ValidationCache(v, self.cache_path, reuse=reuse)
        errors, quality, splits = [], {}, {}
        funds = cache.validate_funds(errors, self.registry, quality)
        stocks = cache.validate_stocks(errors, funds[3], funds[4], splits, registry=self.registry)
        cache.finish(success=not errors)
        self.assertEqual(full_funds, funds)
        self.assertEqual(full_stocks, stocks)
        self.assertEqual(full_quality, quality)
        self.assertEqual(full_splits, splits)
        self.assertEqual(sorted(full_errors), sorted(errors))
        if expected_valid:
            self.assertEqual([], errors)
        else:
            self.assertTrue(errors)
        return cache.counts

    def test_warm_reuse_and_explicit_full_refresh(self):
        self.assertEqual(6, self.compare()['fund_checked'])
        counts = self.compare()
        self.assertEqual(6, counts['fund_reused'])
        self.assertEqual(6, counts['peer_reused'])
        self.assertEqual(2, counts['stock_reused'])
        self.assertEqual(6, self.compare(reuse=False)['fund_checked'])

    def test_changed_fund_invalidates_peers_and_dependent_stock_checks(self):
        self.compare()
        path = self.funds / '1.json'
        obj = json.loads(path.read_bytes())
        obj['quarters'][0]['holdings'][0]['shares'] = 12
        path.write_text(json.dumps(obj))
        counts = self.compare(expected_valid=False)
        self.assertEqual(1, counts['fund_checked'])
        self.assertEqual(6, counts['peer_checked'])
        self.assertEqual(1, counts['stock_checked'])
        self.assertEqual(1, counts['stock_reused'])

    def test_registry_and_checker_code_invalidate_cached_results(self):
        self.compare()
        self.registry['037833100']['name'] = 'CHANGED NAME'
        counts = self.compare(expected_valid=False)
        self.assertEqual(5, counts['fund_checked'])
        self.registry = copy.deepcopy(REGISTRY)
        (self.root / 'validator_dependency.py').write_text('POLICY = 2\n')
        self.assertEqual(6, self.compare()['fund_checked'])

    def test_corrupt_cache_and_payload_fall_back_to_original_checks(self):
        self.compare()
        cache = ValidationCache(v, self.cache_path)
        cache.connection.execute("UPDATE checks SET payload=x'00' WHERE kind='fund_corpus' OR (kind='fund' AND name='1.json')")
        cache.finish(success=True)
        self.assertEqual(1, self.compare()['fund_checked'])
        self.cache_path.write_bytes(b'not a SQLite database')
        self.assertEqual(6, self.compare()['fund_checked'])

    def test_stock_corruption_and_missing_file_are_rejected_after_warm_cache(self):
        self.compare()
        path = self.stocks / '037833100.json'
        obj = json.loads(path.read_bytes())
        obj['holders'][0]['history'][0]['shares'] += 1
        path.write_text(json.dumps(obj))
        self.compare(expected_valid=False)
        path.unlink()
        # The global missing-identity gate runs even if every remaining file is cached.
        cache = ValidationCache(v, self.cache_path)
        errors = []
        result = cache.validate_funds(errors, self.registry, {})
        cache.validate_stocks(errors, result[3], result[4], {}, registry=self.registry)
        cache.finish(success=False)
        self.assertTrue(any('no generated stock file' in error for error in errors))

    def test_quantity_evidence_change_invalidates_fund_checks(self):
        self.compare()
        evidence = self.root / '.cache/quantity_estimation_evidence.json'
        evidence.write_text(json.dumps({'schema_version': 1, 'references': {}, 'reported_rows': {}, 'review': 'changed'}))
        self.assertEqual(6, self.compare()['fund_checked'])

    def test_unchanged_stock_is_rechecked_when_holder_calendar_changes(self):
        self.compare()
        path = self.funds / '1.json'
        obj = json.loads(path.read_bytes())
        obj['quarters'].insert(0, {'report_date': '2026-09-30', 'filing_date': '2026-11-13',
                                   'num_holdings': 0, 'total_value': 0, 'holdings': []})
        path.write_text(json.dumps(obj))
        # Its actual historical observations are unchanged, but the holder's
        # report calendar changes current and transition reconciliation.
        full_errors = []
        full = v.validate_funds(full_errors, self.registry)
        cache = ValidationCache(v, self.cache_path)
        errors = []
        result = cache.validate_funds(errors, self.registry, {})
        cache.validate_stocks(errors, result[3], result[4], {}, registry=self.registry)
        cache.finish(success=not errors)
        self.assertEqual(full, result)
        self.assertEqual(1, cache.counts['stock_checked'])
        self.assertEqual(1, cache.counts['stock_reused'])

    def test_failed_global_gate_does_not_commit_cache_entries(self):
        cache = ValidationCache(v, self.cache_path)
        cache.write('test', 'file', 'key', {}, {'checked': True})
        cache.finish(success=False)
        cache = ValidationCache(v, self.cache_path)
        self.assertIsNone(cache.read('test', 'file', 'key'))
        cache.finish(success=False)


class DurableRecentFeedTests(unittest.TestCase):
    def test_old_discovered_backlog_survives_batch_bound_and_processes_oldest_first(self):
        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp)
            older = {'cik': 9, 'accession': '0000000009-26-000001', 'accepted_at': '2026-08-01T12:00:00Z'}
            newer = {'cik': 1, 'accession': '0000000001-26-000001', 'accepted_at': '2026-09-06T12:00:00Z'}
            state = {'_processed_set': set(), '_quarantined': {}, 'recent_feed_pending': {older['accession']: older}}
            calls = []
            def replay(cik, triggers, _mapping, _quarters, state, **kwargs):
                calls.append(cik)
                state['_processed_set'].update(f['accession'] for f in triggers)
                return len(triggers)
            with mock.patch.multiple(p, DATA_DIR=data, FUNDS_DIR=data / 'funds', STOCKS_DIR=data / 'stocks',
                                     load_state=mock.Mock(return_value=state), load_cusip_map=mock.Mock(return_value={}),
                                     save_state=mock.Mock(), save_cusip_map=mock.Mock(), replay_quarters_for_cik=mock.Mock(side_effect=replay)), \
                 mock.patch.object(feed, 'fetch_recent_feed_filings', return_value=[newer]), \
                 mock.patch.dict('os.environ', {'RECENT_13F_MAX_CIKS': '1'}):
                self.assertEqual(0, feed.main())
                self.assertEqual([9], calls)
                self.assertEqual({newer['accession']: newer}, state['recent_feed_pending'])
                self.assertEqual(0, feed.main())
                self.assertEqual([9, 1], calls)
                self.assertEqual({}, state['recent_feed_pending'])


if __name__ == '__main__':
    unittest.main()
