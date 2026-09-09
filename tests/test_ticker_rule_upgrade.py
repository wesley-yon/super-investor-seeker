from __future__ import annotations

import copy
import json
import tempfile
import unittest
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

import pipeline as p
import sec_security_master as sec
from scripts import incremental_pipeline as inc
from test_sec_security_master import FTD_URL, LIST_URL, ftd_record, numbered_cusip, official_record, source_state


NOW = datetime(2026, 8, 20, 12, tzinfo=timezone.utc)
CUSIP = '615369105'
OLD_ONLY_CUSIP = numbered_cusip(99881)
SOURCE = {
    'accession': '0001067983-26-000001',
    'report_date': '2026-06-30',
    'url': 'https://www.sec.gov/Archives/edgar/data/1067983/000106798326000001/infotable.xml',
    'sha256': 'e' * 64,
}


class TickerRuleUpgradeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.funds = self.root / 'data/funds'
        self.funds.mkdir(parents=True)
        self.master_path = self.root / '.cache/sec_security_master.json'
        self.source_path = self.root / '.cache/sec_source_state.json'
        self.patches = ExitStack()
        self.addCleanup(self.patches.close)
        self.patches.enter_context(mock.patch.multiple(
            p, DATA_DIR=self.root / 'data', FUNDS_DIR=self.funds,
            SEC_SECURITY_MASTER_PATH=self.master_path, SEC_SOURCE_STATE_PATH=self.source_path))
        clock = self.patches.enter_context(mock.patch.object(inc, 'datetime'))
        clock.now.return_value = NOW
        for name in ('make_sec_fetcher', 'refresh_security_master'):
            self.patches.enter_context(mock.patch.object(
                p, name, side_effect=AssertionError('rule upgrade must remain offline')))
        self.write_fixture()

    def holding(self, *, cusip=CUSIP, issuer='MOODYS CORP', kind='EQUITY', security_class='COM'):
        return {
            'cusip': cusip, 'reported_cusip': cusip,
            'issuer': 'DISPLAY LABEL MUST NOT BE USED', 'reported_issuer': issuer,
            'class': 'DISPLAY CLASS MUST NOT BE USED', 'reported_class': security_class,
            'holding_type': kind, 'ticker': None, 'shares': 10, 'value': 1000,
            'accession': SOURCE['accession'], 'report_date': SOURCE['report_date'],
        }

    def write_fund(self, path, holdings):
        path.write_text(json.dumps({'cik': int(path.stem), 'quarters': [{
            'report_date': SOURCE['report_date'], 'reported_identity_sources': [SOURCE],
            'holdings': holdings,
        }]}))

    def write_fixture(self, version=None):
        self.write_fund(self.funds / '1.json', [
            self.holding(),
            self.holding(kind='NOTE', security_class='SENIOR NOTES 2030'),
            self.holding(kind='CALL'),
        ])
        state = source_state(
            rows=[ftd_record(day, symbol='MCO', cusip=CUSIP, description='MOODYS CORP /DE/')
                  for day in ('2026-08-01', '2026-08-04')],
            symbols=['MCO'], symbol_titles={'MCO': ['MOODYS CORP']},
            official_rows=[official_record(cusip=CUSIP, issuer='MOODYS CORP')])
        for url, kind in (
            (sec.SEC_COMPANY_EXCHANGE_TICKERS_URL, 'sec_company_exchange_tickers'),
            (sec.SEC_FUND_TICKERS_URL, 'sec_fund_tickers'),
        ):
            state['sources'][url] = {
                **copy.deepcopy(state['sources'][sec.SEC_COMPANY_TICKERS_URL]),
                'url': url, 'kind': kind,
            }
        for url, kind, discovered in (
            (sec.FTD_PAGE_URL, 'sec_ftd_index', FTD_URL),
            (sec.OFFICIAL_13F_LIST_PAGE_URL, 'sec_13f_list_index', LIST_URL),
        ):
            state['sources'][url] = {
                'url': url, 'kind': kind, 'sha256': 'd' * 64,
                'accepted_at': '2026-08-20T12:00:00Z', 'discovered_urls': [discovered],
            }
        for source in state['sources'].values():
            if source['kind'] != 'sec_ftd_archive':
                source['last_successful_check_at'] = '2026-08-20T12:00:00Z'
        universe = p.collect_security_master_universe()
        # This historical identity is retained only in the saved master, not
        # the current fund corpus or active official list.
        historical = {'cusip': OLD_ONLY_CUSIP, 'instrument_type': 'NOTE',
                      'reported_issuer': 'LEGACY CORP', 'reported_class': 'SENIOR NOTES'}
        historical['reported_identity_evidence'] = [{
            **SOURCE, 'reported_cusip': OLD_ONLY_CUSIP,
            'reported_issuer': historical['reported_issuer'],
            'reported_class': historical['reported_class'],
        }]
        universe.append(historical)
        # Reproduce the prior source-formatting rejection without editing the
        # resolver or forging any resolved proof fields.
        with mock.patch.object(sec, '_normalize_issuer_presentation', side_effect=lambda value: value):
            old_master = sec.rebuild_security_master(
                state, universe, recent_window_days=21, max_evidence_age_days=180,
                min_confirmation_dates=2)
        self.assertIsNone(old_master['records'][f'{CUSIP}|EQUITY']['ticker'])
        if version is None:
            old_master['policy'].pop('resolution_rules_version', None)
        else:
            old_master['policy']['resolution_rules_version'] = version
        sec.save_security_master_pair(
            old_master, state, master_path=self.master_path, source_state_path=self.source_path)
        self.old_master, self.old_source = sec.load_security_master_pair(
            master_path=self.master_path, source_state_path=self.source_path)

    def pair_bytes(self):
        return self.master_path.read_bytes(), self.source_path.read_bytes()

    @contextmanager
    def fixture_populations(self):
        # Scale population floors to this one-security corpus. Every real
        # audit gate still runs, including coverage, freshness, regression,
        # current official period, and immutable reported-identity evidence.
        with mock.patch.multiple(
            p, PRODUCTION_MIN_CURRENT_SYMBOL_POPULATION_BY_KIND={
                'sec_company_tickers': 1, 'sec_company_exchange_tickers': 1,
                'sec_fund_tickers': 1,
            }, PRODUCTION_MIN_ACTIVE_OFFICIAL_CUSIP_COUNT=1,
        ):
            yield

    def test_missing_and_older_markers_upgrade_saved_pair_once(self):
        for version in (None, 1):
            with self.subTest(version=version):
                self.write_fixture(version)
                fund_before = (self.funds / '1.json').read_bytes()
                source_before = self.source_path.read_bytes()
                with self.fixture_populations(), \
                     mock.patch.object(p, 'collect_security_master_universe', wraps=p.collect_security_master_universe) as collect, \
                     mock.patch.object(p, '_audit_security_master', wraps=p._audit_security_master) as audit:
                    self.assertTrue(inc.upgrade_master_resolution_rules())
                collect.assert_called_once_with()
                audit_kwargs = audit.call_args.kwargs
                self.assertTrue(audit_kwargs['enforce_reported_identity_evidence'])
                self.assertTrue(audit_kwargs['enforce_latest_completed_official_period'])
                self.assertEqual(self.old_master, audit_kwargs['prior_master'])
                self.assertEqual(NOW, audit_kwargs['as_of'])
                current, source = sec.load_security_master_pair(
                    master_path=self.master_path, source_state_path=self.source_path)
                self.assertEqual(sec.TICKER_RESOLUTION_RULES_VERSION, current['policy']['resolution_rules_version'])
                for name in ('recent_window_days', 'max_evidence_age_days', 'min_confirmation_dates'):
                    self.assertEqual(self.old_master['policy'][name], current['policy'][name])
                self.assertEqual('MCO', current['records'][f'{CUSIP}|EQUITY']['ticker'])
                self.assertEqual(set(self.old_master['records']), set(current['records']))
                for key in (f'{CUSIP}|NOTE', f'{CUSIP}|CALL', f'{OLD_ONLY_CUSIP}|NOTE'):
                    self.assertIsNone(current['records'][key]['ticker'])
                    self.assertEqual(self.old_master['records'][key]['instrument_type'], current['records'][key]['instrument_type'])
                self.assertEqual(self.old_source, source)
                self.assertEqual(source_before, self.source_path.read_bytes())
                self.assertEqual(fund_before, (self.funds / '1.json').read_bytes())
                before = self.pair_bytes()
                with mock.patch.object(p, 'load_security_master_pair', side_effect=AssertionError('warm source load')), \
                     mock.patch.object(p, 'collect_security_master_universe', side_effect=AssertionError('warm full scan')), \
                     mock.patch.object(p, 'save_security_master_pair', side_effect=AssertionError('warm pair save')):
                    self.assertFalse(inc.upgrade_master_resolution_rules())
                self.assertEqual(before, self.pair_bytes())

    def test_newer_rule_version_fails_without_loading_source_or_mutating_pair(self):
        self.write_fixture(sec.TICKER_RESOLUTION_RULES_VERSION + 1)
        before = self.pair_bytes()
        with mock.patch.object(p, 'load_security_master_pair', side_effect=AssertionError('future source load')), \
             mock.patch.object(p, 'collect_security_master_universe', side_effect=AssertionError('future full scan')):
            with self.assertRaisesRegex(p.FundDataError, 'newer.*refusing downgrade'):
                inc.upgrade_master_resolution_rules()
        self.assertEqual(before, self.pair_bytes())

    def test_recovered_pair_is_rechecked_before_replaying_rules(self):
        future = copy.deepcopy(self.old_master)
        future['policy']['resolution_rules_version'] = sec.TICKER_RESOLUTION_RULES_VERSION + 1
        with mock.patch.object(p, 'load_security_master_pair', return_value=(future, self.old_source)), \
             mock.patch.object(p, 'collect_security_master_universe', side_effect=AssertionError('future full scan')):
            with self.assertRaisesRegex(p.FundDataError, 'refusing downgrade'):
                inc.upgrade_master_resolution_rules()

    def test_rebuild_failure_preserves_exact_prior_pair(self):
        before = self.pair_bytes()
        with mock.patch.object(p, 'rebuild_sec_security_master', side_effect=sec.SecurityMasterError('bad source proof')), \
             mock.patch.object(p, 'save_security_master_pair', side_effect=AssertionError('rejected pair save')):
            with self.assertRaisesRegex(sec.SecurityMasterError, 'bad source proof'):
                inc.upgrade_master_resolution_rules()
        self.assertEqual(before, self.pair_bytes())

    def test_unscaled_production_population_gate_rejects_small_source_and_preserves_pair(self):
        before = self.pair_bytes()
        with mock.patch.object(p, '_audit_security_master', wraps=p._audit_security_master) as audit:
            with self.assertRaisesRegex(p.FundDataError, 'publication gate'):
                inc.upgrade_master_resolution_rules()
        self.assertEqual(p.PRODUCTION_MIN_CURRENT_SYMBOL_POPULATION_BY_KIND,
                         audit.call_args.kwargs['minimum_current_symbol_population_by_kind'])
        self.assertEqual(p.PRODUCTION_MIN_ACTIVE_OFFICIAL_CUSIP_COUNT,
                         audit.call_args.kwargs['minimum_active_official_cusip_count'])
        self.assertEqual(p.PRODUCTION_MIN_CURRENT_SYMBOL_TITLE_RATIO,
                         audit.call_args.kwargs['minimum_current_symbol_title_ratio'])
        self.assertEqual(before, self.pair_bytes())

    def test_stale_source_gate_preserves_prior_pair(self):
        before = self.pair_bytes()
        inc.datetime.now.return_value = datetime(2026, 10, 30, tzinfo=timezone.utc)
        with self.fixture_populations():
            with self.assertRaisesRegex(p.FundDataError, 'stale'):
                inc.upgrade_master_resolution_rules()
        self.assertEqual(before, self.pair_bytes())

    def test_missing_reported_identity_evidence_cannot_be_hidden_by_rebuild(self):
        fund = json.loads((self.funds / '1.json').read_bytes())
        fund['quarters'][0]['reported_identity_sources'] = []
        fund['quarters'][0]['holdings'].append(self.holding(
            cusip=numbered_cusip(99882), issuer='NEW CORP'))
        (self.funds / '1.json').write_text(json.dumps(fund))
        before = self.pair_bytes()
        with self.fixture_populations():
            with self.assertRaisesRegex(p.FundDataError, 'reported_identity'):
                inc.upgrade_master_resolution_rules()
        self.assertEqual(before, self.pair_bytes())

    def test_legacy_class_conflict_can_load_but_new_rules_withdraw_its_symbol(self):
        for version in (None, 1):
            with self.subTest(version=version):
                self.write_fund(self.funds / '1.json', [self.holding(security_class='CL A')])
                state = source_state(rows=[
                    ftd_record(day, symbol='MCO', cusip=CUSIP, description='MOODYS CORP CL B')
                    for day in ('2026-08-01', '2026-08-04')
                ])
                state['sources'].update({
                    url: copy.deepcopy(source) for url, source in self.old_source['sources'].items()
                    if source['kind'] != 'sec_ftd_archive'
                })
                state['sources'][LIST_URL]['records'][0]['description'] = 'CL A'
                # Reproduce a legacy admitted mapping under its former guard,
                # then restore the current guard before save/load and replay.
                with mock.patch.object(sec, '_ftd_class_conflict_reason', return_value=None):
                    legacy = sec.rebuild_security_master(state, p.collect_security_master_universe())
                self.assertEqual('MCO', legacy['records'][f'{CUSIP}|EQUITY']['ticker'])
                if version is None:
                    legacy['policy'].pop('resolution_rules_version')
                else:
                    legacy['policy']['resolution_rules_version'] = version
                sec.save_security_master_pair(
                    legacy, state, master_path=self.master_path, source_state_path=self.source_path)
                loaded, _ = sec.load_security_master_pair(
                    master_path=self.master_path, source_state_path=self.source_path)
                self.assertEqual('MCO', loaded['records'][f'{CUSIP}|EQUITY']['ticker'])
                forged_current = copy.deepcopy(loaded)
                forged_current['policy']['resolution_rules_version'] = sec.TICKER_RESOLUTION_RULES_VERSION
                with self.assertRaisesRegex(sec.SecurityMasterError, 'official class'):
                    sec.validate_security_master(forged_current)
                before = self.pair_bytes()
                with self.fixture_populations(), \
                     mock.patch.object(p, '_audit_security_master', wraps=p._audit_security_master) as audit:
                    with self.assertRaisesRegex(p.FundDataError, 'resolved_mapping_population_regressed'):
                        inc.upgrade_master_resolution_rules()
                candidate = audit.call_args.args[0]
                self.assertEqual(sec.TICKER_RESOLUTION_RULES_VERSION, candidate['policy']['resolution_rules_version'])
                record = candidate['records'][f'{CUSIP}|EQUITY']
                self.assertIsNone(record['ticker'])
                self.assertEqual('ftd_class_designator_conflicts_with_official_13f_identity', record['resolution_reason'])
                self.assertEqual(before, self.pair_bytes())

    def test_regenerate_propagates_rule_change_to_unchanged_dependent_fund(self):
        other_cusip = numbered_cusip(99883)
        other_path = self.funds / '2.json'
        self.write_fund(other_path, [self.holding(cusip=other_cusip, issuer='UNRELATED INC')])
        other_before = other_path.read_bytes()

        class Registry(dict):
            @property
            def observed_cusips(self):
                return set(self)

        prior_registry = Registry({
            cusip: {'ticker': None, 'name': issuer, 'type': 'EQUITY', 'mapping_status': 'unresolved'}
            for cusip, issuer in ((CUSIP, 'MOODYS CORP'), (other_cusip, 'UNRELATED INC'))
        })
        baseline = self.root / 'baseline.json'
        baseline.write_text(json.dumps({
            'version': 1, 'code': 'fixture', 'funds': inc.inventory(prior_registry),
            'registry': inc.registry_identity(prior_registry), 'state': inc.public_state({}),
        }))

        def current_registry():
            result = copy.deepcopy(prior_registry)
            record = sec.load_security_master(self.master_path)['records'][f'{CUSIP}|EQUITY']
            result[CUSIP].update(ticker=record['ticker'], mapping_status=record['mapping_status'])
            return result

        def canonicalize(*, preserve_position_identity, fund_paths):
            self.assertTrue(preserve_position_identity)
            for path in fund_paths:
                fund = json.loads(path.read_bytes())
                for quarter in fund['quarters']:
                    for holding in quarter['holdings']:
                        if holding['holding_type'] == 'EQUITY' and holding['cusip'] == CUSIP:
                            holding['ticker'] = current_registry()[CUSIP]['ticker']
                path.write_text(json.dumps(fund))

        with self.fixture_populations(), ExitStack() as patches:
            patches.enter_context(mock.patch.object(inc, 'checker_fingerprint', return_value='fixture'))
            patches.enter_context(mock.patch.object(p, 'load_state', return_value={}))
            patches.enter_context(mock.patch.object(p, 'load_cusip_registry', return_value=prior_registry))
            patches.enter_context(mock.patch.object(p, 'build_cusip_registry', side_effect=current_registry))
            patches.enter_context(mock.patch.object(p, 'validate_cusip_registry', return_value=[]))
            canonical = patches.enter_context(mock.patch.object(p, 'canonicalize_fund_files', side_effect=canonicalize))
            stock_rebuild = patches.enter_context(mock.patch.object(p, 'regenerate_stock_files_and_index'))
            for name in ('enforce_published_quarter_health', 'save_state', 'write_security_labels',
                         'repair_zero_share_holdings_in_place', 'upgrade_composition_hashes_in_place',
                         'write_ticker_health_report'):
                patches.enter_context(mock.patch.object(p, name))
            patches.enter_context(mock.patch.object(
                inc, 'extend_master_for_changed_funds', side_effect=AssertionError('full upgrade already includes all funds')))
            summary = inc.regenerate.__wrapped__(baseline)
        canonical.assert_called_once_with(preserve_position_identity=True, fund_paths=[self.funds / '1.json'])
        self.assertEqual(1, summary['changed_funds'])
        self.assertEqual(1, summary['registry_identity_changes'])
        self.assertIn(CUSIP, stock_rebuild.call_args.kwargs['stock_ids'])
        self.assertEqual(other_before, other_path.read_bytes())
        holdings = json.loads((self.funds / '1.json').read_bytes())['quarters'][0]['holdings']
        self.assertEqual(['EQUITY', 'NOTE', 'CALL'], [row['holding_type'] for row in holdings])
        self.assertEqual([10, 10, 10], [row['shares'] for row in holdings])
        self.assertEqual([1000, 1000, 1000], [row['value'] for row in holdings])
        self.assertEqual(['MCO', None, None], [row['ticker'] for row in holdings])


if __name__ == '__main__':
    unittest.main()
