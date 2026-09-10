from __future__ import annotations

from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest import mock

import pipeline as p
import selective_rebuild as cache_module
from scripts import incremental_pipeline as inc


APPLE = '037833100'
MICROSOFT = '594918104'


class FixedClock(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 9, 9, 12, tzinfo=timezone.utc)


class SelectiveRebuildTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name) / 'actual'
        (self.root / 'data/funds').mkdir(parents=True)
        (self.root / '.cache').mkdir()
        self.master = {'records': {}}
        for cusip, issuer, ticker in [(APPLE, 'APPLE INC', 'AAPL'), (MICROSOFT, 'MICROSOFT CORP', 'MSFT')]:
            for kind in ['EQUITY', 'CALL', 'PUT', 'NOTE']:
                self.master['records'][f'{cusip}|{kind}'] = {
                    'cusip': cusip, 'instrument_type': kind, 'reported_issuer': issuer,
                    'mapping_status': 'resolved' if kind == 'EQUITY' else 'unresolved',
                    'ticker': ticker if kind == 'EQUITY' else None,
                    'ticker_source': 'sec_ftd' if kind == 'EQUITY' else None,
                    'ticker_as_of': '2026-09-01' if kind == 'EQUITY' else None,
                    'symbol_evidence': [],
                }
        self.write_master()
        (self.root / '.cache/sec_source_state.json').write_text('{"fixture":1}')
        for cik in range(1, 7):
            self.write_fund(cik, APPLE if cik <= 4 else MICROSOFT)
        self.context = self.use_root(self.root)
        self.context.__enter__()
        self.addCleanup(self.context.__exit__, None, None, None)
        # Source authenticity has its own complete suite and production-corpus
        # gate. These small dependency fixtures omit the mandatory safety-case
        # seeds, but use the real registry, health, quantity, and stock builders.
        patches = ExitStack()
        self.addCleanup(patches.close)
        patches.enter_context(mock.patch.object(p, 'load_security_master', side_effect=lambda path: json.loads(path.read_bytes())))
        patches.enter_context(mock.patch.object(p, 'validate_cusip_registry', return_value=[]))
        patches.enter_context(mock.patch.object(inc, 'upgrade_master_resolution_rules', return_value=False))
        patches.enter_context(mock.patch.object(inc, 'extend_master_for_changed_funds'))
        patches.enter_context(mock.patch.object(p, 'datetime', FixedClock))
        self.run_update()
        self.run_update()  # First quantity pass creates its empty evidence book.

    @contextmanager
    def use_root(self, root):
        data, cache = root / 'data', root / '.cache'
        with mock.patch.object(inc, 'ROOT', root), mock.patch.multiple(
            p, ROOT=root, DATA_DIR=data, CACHE_DIR=cache, FUNDS_DIR=data / 'funds',
            STOCKS_DIR=data / 'stocks', INDEX_PATH=data / 'index.json',
            FUNDS_INDEX_PATH=data / 'funds-index.json', STATE_PATH=data / 'pipeline_state.json',
            LEGACY_STATE_PATH=cache / 'pipeline_state.json',
            CUSIP_REGISTRY_PATH=cache / 'cusip_registry.json',
            LEGACY_CUSIP_REGISTRY_PATH=data / 'cusip_registry.json',
            SECURITY_LABELS_PATH=data / 'security_labels.json', TICKER_HEALTH_PATH=data / 'ticker_health.json',
            SEC_SECURITY_MASTER_PATH=cache / 'sec_security_master.json',
            SEC_SOURCE_STATE_PATH=cache / 'sec_source_state.json',
        ):
            yield

    def write_master(self):
        (self.root / '.cache/sec_security_master.json').write_text(json.dumps(self.master))

    def write_fund(self, cik, cusip, *, value=1000, shares=10, kind='EQUITY', date='2026-06-30'):
        issuer = 'APPLE INC' if cusip == APPLE else 'MICROSOFT CORP'
        holding = {'cusip': cusip, 'reported_cusip': cusip, 'reported_issuer': issuer,
                   'issuer': issuer, 'class': 'COM', 'reported_class': 'COM',
                   'holding_type': kind, 'value': value, 'shares': shares}
        if kind in {'CALL', 'PUT'}:
            holding['put_call'] = kind
        fund = {'cik': cik, 'name': f'Fund {cik}', 'quarters': [{
            'report_date': date, 'filing_date': '2026-08-14', 'num_holdings': 1,
            'total_value': value, 'holdings': [holding],
        }]}
        (self.root / f'data/funds/{cik}.json').write_text(json.dumps(fund))

    def run_update(self, *, full=False):
        baseline = inc.ROOT / '.cache/incremental_update_baseline.json'
        inc.capture(baseline)
        return inc.regenerate(baseline, full_rebuild=full)

    def outputs(self, root):
        return {str(path.relative_to(root)): path.read_bytes()
                for path in sorted((root / 'data').rglob('*.json'))}

    def compare_with_full(self):
        expected_root = self.root.parent / 'expected'
        shutil.copytree(self.root, expected_root)
        result = self.run_update()
        actual = self.outputs(self.root)
        with self.use_root(expected_root):
            self.run_update(full=True)
        self.assertEqual(self.outputs(expected_root), actual)
        return result

    def test_unchanged_inputs_skip_expensive_phases_and_preserve_bytes(self):
        before = self.outputs(self.root)
        with ExitStack() as patches:
            spies = {name: patches.enter_context(mock.patch.object(p, name, wraps=getattr(p, name)))
                     for name in ['inventory_published_quarter_health_issues', 'build_cusip_registry',
                                  'repair_zero_share_holdings_in_place', 'aggregate_ticker_health',
                                  'regenerate_stock_files_and_index']}
            result = self.run_update()
        for spy in spies.values():
            spy.assert_not_called()
        self.assertFalse(result['full_rebuild'])
        self.assertEqual(0, result['rebuilt_stock_ids'])
        self.assertEqual(before, self.outputs(self.root))

    def test_amended_holding_rebuilds_all_holders_of_only_affected_security(self):
        self.write_fund(1, APPLE, value=1700)
        unrelated = self.root / f'data/stocks/{MICROSOFT}.json'
        inode = unrelated.stat().st_ino
        result = self.compare_with_full()
        self.assertEqual(1, result['registry_cusips_rebuilt'])
        self.assertEqual(1, result['rebuilt_stock_ids'])
        self.assertEqual(inode, unrelated.stat().st_ino)
        holders = json.loads((self.root / f'data/stocks/{APPLE}.json').read_bytes())['holders']
        self.assertEqual(4, len(holders))
        self.assertEqual(1700, next(h for h in holders if h['cik'] == 1)['history'][0]['value'])

    def test_deleted_security_removes_registry_stock_and_health_dependencies(self):
        for cik in [5, 6]:
            (self.root / f'data/funds/{cik}.json').unlink()
        self.compare_with_full()
        self.assertFalse((self.root / f'data/stocks/{MICROSOFT}.json').exists())
        self.assertNotIn(MICROSOFT, p.load_cusip_registry())

    def test_new_option_keeps_equity_and_option_identities_separate(self):
        self.write_fund(1, APPLE, kind='CALL')
        self.compare_with_full()
        self.assertTrue((self.root / 'data/stocks' / p.stock_filename(APPLE, 'CALL')).exists())
        equity = json.loads((self.root / f'data/stocks/{APPLE}.json').read_bytes())
        self.assertNotIn(1, [h['cik'] for h in equity['holders']])

    def test_source_revision_rebuilds_unchanged_fund_display_dependencies(self):
        self.master['records'][f'{APPLE}|EQUITY']['ticker'] = 'NEW'
        self.write_master()
        result = self.compare_with_full()
        self.assertEqual(2, result['registry_cusips_rebuilt'])
        self.assertEqual('NEW', json.loads((self.root / 'data/funds/2.json').read_bytes())['quarters'][0]['holdings'][0]['ticker'])

    def test_metadata_change_before_capture_is_not_lost(self):
        path = self.root / 'data/funds/1.json'
        fund = json.loads(path.read_bytes())
        fund['name'] = 'RENAMED FUND'
        path.write_text(json.dumps(fund))
        self.compare_with_full()

    def test_reporting_calendar_rollover_recomputes_unmodified_security_counts(self):
        for cik in [1, 2, 3, 4]:
            self.write_fund(cik, APPLE, date='2026-09-30')
        self.compare_with_full()

    def test_withheld_state_change_updates_unchanged_stock_views(self):
        state = p.load_state()
        state['quarter_health_pending']['1:2026-09-30'] = {
            'cik': 1, 'report_date': '2026-09-30', 'reason': 'awaiting_replay',
        }
        p.save_state(state)
        self.compare_with_full()

    def test_bad_quarter_still_withheld_and_durable_retry_queue_preserved(self):
        path = self.root / 'data/funds/1.json'
        fund = json.loads(path.read_bytes())
        fund['quarters'][0]['num_holdings'] = 9
        path.write_text(json.dumps(fund))
        self.compare_with_full()
        self.assertEqual([], json.loads(path.read_bytes())['quarters'])
        self.assertIn('1:2026-06-30', p.load_state()['quarter_health_pending'])

    def test_company_title_change_rerenders_health_using_all_cached_records(self):
        (self.root / 'data/company_tickers.json').write_text(json.dumps({'0': {'ticker': 'AAPL', 'title': 'APPLE INC'}}))
        self.compare_with_full()

    def test_registry_corruption_is_rebuilt_from_source(self):
        registry = p.load_cusip_registry()
        registry[APPLE]['name'] = 'CORRUPT LABEL'
        p.save_cusip_registry(registry)
        self.compare_with_full()

    def test_missing_or_corrupt_stock_forces_complete_stock_rebuild(self):
        (self.root / f'data/stocks/{MICROSOFT}.json').write_text('{broken')
        result = self.compare_with_full()
        self.assertEqual(2, result['rebuilt_stock_ids'])

    def test_missing_cache_falls_back_to_full_build(self):
        (self.root / cache_module.CACHE_RELATIVE_PATH).unlink()
        self.assertTrue(self.compare_with_full()['full_rebuild'])

    def test_corrupt_cache_falls_back_to_full_build(self):
        path = self.root / cache_module.CACHE_RELATIVE_PATH
        envelope = json.loads(path.read_bytes())
        envelope['payload']['registry_keys'][APPLE] = 'changed without updating digest'
        path.write_text(json.dumps(envelope))
        self.assertTrue(self.compare_with_full()['full_rebuild'])

    def test_changed_checker_code_falls_back_to_full_build(self):
        (self.root / 'new_policy.py').write_text('VERSION = 2\n')
        self.assertTrue(self.compare_with_full()['full_rebuild'])

    def test_price_evidence_change_reruns_quantity_policy(self):
        (self.root / '.cache/quarter_close_prices.json').write_text('{"schema_version":1,"references":{}}')
        with mock.patch.object(p, 'repair_zero_share_holdings_in_place', wraps=p.repair_zero_share_holdings_in_place) as repair:
            self.compare_with_full()
        self.assertEqual(2, repair.call_count)

    def test_failed_generation_does_not_advance_cache(self):
        self.write_fund(1, APPLE, value=1700)
        path = self.root / cache_module.CACHE_RELATIVE_PATH
        before = path.read_bytes()
        with mock.patch.object(p, 'write_ticker_health_report', side_effect=RuntimeError('render failed')):
            with self.assertRaisesRegex(RuntimeError, 'render failed'):
                self.run_update()
        self.assertEqual(before, path.read_bytes())
        self.compare_with_full()

    def test_cache_symlink_is_rejected(self):
        path = self.root / cache_module.CACHE_RELATIVE_PATH
        path.unlink()
        path.symlink_to(self.root / '.cache/sec_security_master.json')
        with self.assertRaisesRegex(ValueError, 'symlink'):
            self.run_update()
