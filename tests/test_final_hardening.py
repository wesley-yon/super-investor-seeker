import copy
import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

import pipeline
import validate_data
from scripts import refresh_recent_13f_filings


class SplitProofHardeningTests(unittest.TestCase):
    def test_malformed_numeric_rows_cannot_crash_split_inference(self) -> None:
        malformed = [
            None,
            "100",
            True,
            float("nan"),
            float("inf"),
            [],
            10**10000,
        ]
        holders = []
        for index, value in enumerate(malformed, start=1):
            holders.append({
                "cik": index,
                "history": [
                    {
                        "date": "2025-06-30",
                        "shares": value,
                        "value": 1_000,
                    },
                    {
                        "date": "2025-03-31",
                        "shares": 10,
                        "value": 1_000,
                    },
                ],
            })
        holders.append({
            "cik": len(holders) + 1,
            "history": [
                {
                    "date": "2025-06-30",
                    "shares": 1.0,
                    "value": 1.0,
                },
                {
                    "date": "2025-03-31",
                    "shares": 1e308,
                    "value": 5e-324,
                },
            ],
        })

        self.assertEqual([], pipeline.infer_proven_split_adjustments(holders))

        with tempfile.TemporaryDirectory() as tmpdir:
            stocks_dir = Path(tmpdir)
            (stocks_dir / "037833100.json").write_text(json.dumps({
                "stock_id": "037833100",
                "cusip": "037833100",
                "instrument_type": "EQUITY",
                "holders": [{
                    "cik": 1,
                    "history": [
                        {
                            "date": "2025-06-30",
                            "shares": 10**1000,
                            "value": 1_000,
                            "pct_of_fund": 100,
                        },
                        {
                            "date": "2025-03-31",
                            "shares": 10,
                            "value": 1_000,
                            "pct_of_fund": 100,
                        },
                    ],
                }],
            }))
            errors: list[str] = []
            with mock.patch.object(validate_data, "STOCKS_DIR", stocks_dir):
                validate_data.validate_stocks(errors)

        self.assertTrue(any(
            "has non-numeric position data" in error for error in errors
        ))

    def test_generator_and_validator_share_exact_bootstrap_split_map(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir) / "data"
            funds_dir = data_dir / "funds"
            stocks_dir = data_dir / "stocks"
            funds_dir.mkdir(parents=True)
            for cik in range(1, 21):
                (funds_dir / f"{cik}.json").write_text(json.dumps({
                    "cik": cik,
                    "name": f"Fund {cik}",
                    "quarters": [
                        {
                            "report_date": "2025-06-30",
                            "total_value": 1_000,
                            "holdings": [{
                                "cusip": "037833100",
                                "ticker": "AAPL",
                                "issuer": "APPLE INC",
                                "holding_type": "EQUITY",
                                "shares": 20,
                                "value": 1_000,
                            }],
                        },
                        {
                            "report_date": "2025-03-31",
                            "total_value": 1_000,
                            "holdings": [{
                                "cusip": "037833100",
                                "ticker": "AAPL",
                                "issuer": "APPLE INC",
                                "holding_type": "EQUITY",
                                "shares": 10,
                                "value": 1_000,
                            }],
                        },
                    ],
                }))

            index_path = data_dir / "index.json"
            funds_index_path = data_dir / "funds-index.json"
            registry_path = data_dir / "cusip_registry.json"
            with mock.patch.multiple(
                pipeline,
                DATA_DIR=data_dir,
                FUNDS_DIR=funds_dir,
                STOCKS_DIR=stocks_dir,
                INDEX_PATH=index_path,
                FUNDS_INDEX_PATH=funds_index_path,
                CUSIP_REGISTRY_PATH=registry_path,
                LEGACY_CUSIP_REGISTRY_PATH=registry_path,
            ):
                pipeline.regenerate_stock_files_and_index(state={})

            index = json.loads(index_path.read_text())
            funds_index = json.loads(funds_index_path.read_text())
            published = index["proven_split_adjustments"]
            self.assertEqual(
                {"037833100"},
                set(published),
            )
            self.assertEqual(
                published,
                funds_index["proven_split_adjustments"],
            )

            errors: list[str] = []
            expected: dict[str, list[dict]] = {}
            with mock.patch.object(validate_data, "STOCKS_DIR", stocks_dir):
                validate_data.validate_stocks(
                    errors,
                    expected_split_adjustments=expected,
                )
            fund_files = {
                path.stem: path for path in funds_dir.glob("*.json")
            }
            with mock.patch.object(
                validate_data,
                "FUNDS_INDEX_PATH",
                funds_index_path,
            ):
                validate_data.validate_funds_index(
                    funds_index,
                    index,
                    errors,
                    fund_files,
                    expected,
                )
            self.assertEqual([], errors)
            self.assertEqual(published, expected)

            missing = copy.deepcopy(funds_index)
            missing.pop("proven_split_adjustments")
            errors = []
            with mock.patch.object(
                validate_data,
                "FUNDS_INDEX_PATH",
                funds_index_path,
            ):
                validate_data.validate_funds_index(
                    missing,
                    index,
                    errors,
                    fund_files,
                    expected,
                )
            self.assertTrue(any(
                "proven_split_adjustments must be an object" in error
                for error in errors
            ))

            tampered_funds = copy.deepcopy(funds_index)
            tampered_index = copy.deepcopy(index)
            tampered_funds["proven_split_adjustments"] = {}
            tampered_index["proven_split_adjustments"] = {}
            errors = []
            with mock.patch.object(
                validate_data,
                "FUNDS_INDEX_PATH",
                funds_index_path,
            ):
                validate_data.validate_funds_index(
                    tampered_funds,
                    tampered_index,
                    errors,
                    fund_files,
                    expected,
                )
            self.assertTrue(any(
                "do not match independently recomputed stock proof" in error
                for error in errors
            ))


class CompositionHashHardeningTests(unittest.TestCase):
    @staticmethod
    def _current_identity_quarter() -> dict:
        accession = "0000000001-25-000001"
        source = {
            "accession": accession,
            "source_hash": "0" * 64,
            "applied": True,
            "form_type": "13F-HR",
            "amendment_kind": "ORIGINAL",
            "composition_action": "BASE",
            "security_identity_version":
                pipeline.SECURITY_IDENTITY_VERSION,
            "filing_date": "2025-05-15",
            "reported_entry_total": 1,
            "reported_value_total": 100,
        }
        quarter = {
            "composition_version": pipeline.AMENDMENT_REDUCER_VERSION,
            "composition_hash_version": pipeline.COMPOSITION_HASH_VERSION,
            "security_identity_version":
                pipeline.SECURITY_IDENTITY_VERSION,
            "report_date": "2025-03-31",
            "is_complete": True,
            "num_holdings": 1,
            "total_value": 100,
            "holdings": [{
                "cusip": "037833100",
                "class": "COM",
                "holding_type": "EQUITY",
                "value": 100,
                "shares": 1,
                "put_call": None,
            }],
            "base_accession": accession,
            "applied_accessions": [accession],
            "source_filings": [source],
            "accession": accession,
            "filing_date": source["filing_date"],
        }
        quarter["composition_hash"] = (
            validate_data.calculate_composition_hash(quarter)
        )
        return quarter

    def test_invalid_hash_versions_report_errors_without_crashing(self) -> None:
        accession = "0000000001-25-000001"
        source = {
            "accession": accession,
            "source_hash": "0" * 64,
            "applied": True,
            "form_type": "13F-HR",
            "amendment_kind": "ORIGINAL",
            "filing_date": "2025-05-15",
            "reported_entry_total": 0,
            "reported_value_total": 0,
        }
        for invalid in ("2", None, True, 3):
            with self.subTest(invalid=invalid):
                quarter = {
                    "composition_version": 1,
                    "composition_hash_version": invalid,
                    "report_date": "2025-03-31",
                    "is_complete": True,
                    "num_holdings": 0,
                    "total_value": 0,
                    "holdings": [],
                    "base_accession": accession,
                    "applied_accessions": [accession],
                    "source_filings": [source],
                    "accession": accession,
                    "filing_date": source["filing_date"],
                }
                quarter["composition_hash"] = (
                    validate_data.calculate_composition_hash(quarter)
                )
                errors: list[str] = []
                validate_data.validate_amendment_composition(
                    quarter,
                    "fixture",
                    errors,
                )
                self.assertTrue(any(
                    "unsupported composition_hash_version" in error
                    for error in errors
                ))

    def test_malformed_composition_fields_report_without_crashing(self) -> None:
        cases = {
            "applied_accessions": lambda quarter: quarter.update({
                "applied_accessions": [
                    quarter["base_accession"],
                    {},
                ],
            }),
            "holdings": lambda quarter: quarter.update({
                "holdings": [None],
            }),
            "form_type": lambda quarter: quarter["source_filings"][0].update({
                "form_type": {},
            }),
            "amendment_kind": (
                lambda quarter: quarter["source_filings"][0].update({
                    "amendment_kind": [],
                })
            ),
            "composition_action": (
                lambda quarter: quarter["source_filings"][0].update({
                    "composition_action": {},
                })
            ),
            "cover_status": (
                lambda quarter: quarter["source_filings"][0].update({
                    "cover_reported_entry_total": 1,
                    "cover_reported_value_total": 100,
                    "cover_reconciliation_status": {},
                })
            ),
            "value_confidence": (
                lambda quarter: quarter["source_filings"][0].update({
                    "value_unit_policy_version":
                        validate_data.VALUE_UNIT_POLICY_VERSION,
                    "value_multiplier": 1,
                    "normalized_value_total": 100,
                    "value_unit_method": "fixture",
                    "value_unit_confidence": {},
                    "value_unit_evidence": {},
                })
            ),
        }
        for name, mutate in cases.items():
            with self.subTest(name=name):
                quarter = copy.deepcopy(self._current_identity_quarter())
                mutate(quarter)
                errors: list[str] = []
                validate_data.validate_amendment_composition(
                    quarter,
                    "malformed fixture",
                    errors,
                )
                self.assertTrue(errors)

    def test_parser_proof_legacy_hash_upgrades_once_after_verification(
        self,
    ) -> None:
        quarter = self._current_identity_quarter()
        quarter["composition_hash_version"] = 1
        quarter["composition_hash"] = (
            validate_data.calculate_composition_hash(quarter)
        )
        legacy_errors: list[str] = []
        validate_data.validate_amendment_composition(
            quarter,
            "legacy parser-proof fixture",
            legacy_errors,
        )
        self.assertTrue(any(
            "security identity proof requires composition hash v2" in error
            for error in legacy_errors
        ))
        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir)
            fund_path = funds_dir / "1.json"
            fund_path.write_text(json.dumps({
                "cik": 1,
                "name": "Fixture",
                "quarters": [quarter],
            }))
            with mock.patch.object(pipeline, "FUNDS_DIR", funds_dir):
                self.assertEqual(
                    1,
                    pipeline.upgrade_composition_hashes_in_place(),
                )
                self.assertEqual(
                    0,
                    pipeline.upgrade_composition_hashes_in_place(),
                )

            upgraded = json.loads(fund_path.read_text())["quarters"][0]
            self.assertEqual(
                pipeline.COMPOSITION_HASH_VERSION,
                upgraded["composition_hash_version"],
            )
            self.assertEqual(
                validate_data.calculate_composition_hash(upgraded),
                upgraded["composition_hash"],
            )
            errors: list[str] = []
            validate_data.validate_amendment_composition(
                upgraded,
                "upgraded fixture",
                errors,
            )
            self.assertEqual([], errors)

            upgraded["composition_hash_version"] = 1
            upgraded["composition_hash"] = "f" * 64
            fund_path.write_text(json.dumps({
                "cik": 1,
                "name": "Fixture",
                "quarters": [upgraded],
            }))
            with mock.patch.object(pipeline, "FUNDS_DIR", funds_dir):
                self.assertEqual(
                    0,
                    pipeline.upgrade_composition_hashes_in_place(),
                )


class ReplayCheckpointHardeningTests(unittest.TestCase):
    def test_run_all_checkpoints_zero_success_completed_groups(self) -> None:
        filings = [
            {
                "cik": cik,
                "accession": f"{cik:010d}-25-000001",
            }
            for cik in range(1, 27)
        ]
        state = {
            "_processed_set": set(),
            "_quarantined": {},
            "amendment_reducer_version": pipeline.AMENDMENT_REDUCER_VERSION,
            "amendment_migration_pending": {},
            "security_identity_migration_version":
                pipeline.SECURITY_IDENTITY_VERSION,
            "security_identity_migration_pending": {},
            "quarter_health_pending": {},
        }
        last_started = threading.Event()
        last_finished = threading.Event()
        release_last = threading.Event()
        checkpoint_during_last = threading.Event()
        calls = 0
        calls_lock = threading.Lock()

        def replay(*_args, **_kwargs) -> int:
            nonlocal calls
            with calls_lock:
                calls += 1
                current = calls
            if current == 26:
                last_started.set()
                release_last.wait(1)
                last_finished.set()
            return 0

        def save_state(_state: dict) -> None:
            if last_started.is_set() and not last_finished.is_set():
                checkpoint_during_last.set()
                release_last.set()

        def short_wait(_seconds: float) -> None:
            threading.Event().wait(0.001)

        with (
            mock.patch.object(pipeline, "WORKER_COUNT", 1),
            mock.patch.object(pipeline, "load_state", return_value=state),
            mock.patch.object(pipeline, "load_cusip_map", return_value={}),
            mock.patch.object(
                pipeline,
                "get_recent_filing_quarters",
                return_value=[(2025, 2)],
            ),
            mock.patch.object(
                pipeline,
                "download_company_idx",
                return_value=filings,
            ),
            mock.patch.object(
                pipeline,
                "replay_quarters_for_cik",
                side_effect=replay,
            ),
            mock.patch.object(
                pipeline.time,
                "sleep",
                side_effect=short_wait,
            ),
            mock.patch.object(
                pipeline,
                "save_state",
                side_effect=save_state,
            ),
            mock.patch.object(pipeline, "save_cusip_map"),
        ):
            self.assertTrue(pipeline.run_all(4, rebuild_outputs=False))

        self.assertTrue(checkpoint_during_last.is_set())

    def test_interrupt_checkpoints_before_alive_worker_return(self) -> None:
        filing = {
            "cik": 1,
            "accession": "0000000001-25-000001",
        }
        state = {
            "_processed_set": set(),
            "_quarantined": {},
            "amendment_reducer_version": pipeline.AMENDMENT_REDUCER_VERSION,
            "amendment_migration_pending": {},
            "security_identity_migration_version":
                pipeline.SECURITY_IDENTITY_VERSION,
            "security_identity_migration_pending": {},
            "quarter_health_pending": {},
        }
        cusip_map: dict[str, str] = {}
        mutated = threading.Event()
        release_worker = threading.Event()
        worker_finished = threading.Event()
        saved_states: list[dict] = []
        saved_maps: list[dict] = []

        def replay(*_args, **_kwargs) -> int:
            state["_quarantined"]["checkpointed"] = {
                "reason": "FilingFetchError",
            }
            cusip_map["037833100"] = "AAPL"
            mutated.set()
            release_worker.wait(2)
            worker_finished.set()
            return 0

        def interrupt_after_mutation(_seconds: float) -> None:
            self.assertTrue(mutated.wait(1))
            raise KeyboardInterrupt

        try:
            with (
                mock.patch.object(pipeline, "WORKER_COUNT", 1),
                mock.patch.object(pipeline, "load_state", return_value=state),
                mock.patch.object(
                    pipeline,
                    "load_cusip_map",
                    return_value=cusip_map,
                ),
                mock.patch.object(
                    pipeline,
                    "get_recent_filing_quarters",
                    return_value=[(2025, 2)],
                ),
                mock.patch.object(
                    pipeline,
                    "download_company_idx",
                    return_value=[filing],
                ),
                mock.patch.object(
                    pipeline,
                    "replay_quarters_for_cik",
                    side_effect=replay,
                ),
                mock.patch.object(
                    pipeline.time,
                    "sleep",
                    side_effect=interrupt_after_mutation,
                ),
                mock.patch.object(
                    pipeline.threading.Thread,
                    "join",
                    return_value=None,
                ),
                mock.patch.object(
                    pipeline,
                    "save_state",
                    side_effect=lambda value: saved_states.append(
                        copy.deepcopy(value)
                    ),
                ),
                mock.patch.object(
                    pipeline,
                    "save_cusip_map",
                    side_effect=lambda value: saved_maps.append(
                        copy.deepcopy(value)
                    ),
                ),
            ):
                self.assertFalse(
                    pipeline.run_all(4, rebuild_outputs=False)
                )
        finally:
            release_worker.set()
            self.assertTrue(worker_finished.wait(1))

        self.assertTrue(any(
            "checkpointed" in snapshot["_quarantined"]
            for snapshot in saved_states
        ))
        self.assertTrue(any(
            snapshot.get("037833100") == "AAPL"
            for snapshot in saved_maps
        ))


class RecentFeedCheckpointHardeningTests(unittest.TestCase):
    def test_replay_interrupt_and_error_checkpoint_mutations(self) -> None:
        trigger = {
            "cik": 1,
            "accession": "0000000001-25-000001",
        }
        for failure in (KeyboardInterrupt(), RuntimeError("boom")):
            with self.subTest(failure=type(failure).__name__):
                state = {
                    "_processed_set": set(),
                    "_quarantined": {},
                }
                cusip_map: dict[str, str] = {}
                saved_states: list[dict] = []
                saved_maps: list[dict] = []

                def replay(
                    _cik: int,
                    _triggers: list[dict],
                    replay_map: dict[str, str],
                    _quarters_n: int,
                    replay_state: dict,
                    **_kwargs,
                ) -> int:
                    replay_state["_quarantined"]["checkpointed"] = {
                        "reason": "test",
                    }
                    replay_map["037833100"] = "AAPL"
                    raise failure

                with tempfile.TemporaryDirectory() as tmpdir:
                    data_dir = Path(tmpdir)
                    with (
                        mock.patch.object(pipeline, "DATA_DIR", data_dir),
                        mock.patch.object(
                            pipeline,
                            "FUNDS_DIR",
                            data_dir / "funds",
                        ),
                        mock.patch.object(
                            pipeline,
                            "STOCKS_DIR",
                            data_dir / "stocks",
                        ),
                        mock.patch.object(
                            pipeline,
                            "load_state",
                            return_value=state,
                        ),
                        mock.patch.object(
                            pipeline,
                            "load_cusip_map",
                            return_value=cusip_map,
                        ),
                        mock.patch.object(
                            refresh_recent_13f_filings,
                            "fetch_recent_feed_filings",
                            return_value=[trigger],
                        ),
                        mock.patch.object(
                            pipeline,
                            "replay_quarters_for_cik",
                            side_effect=replay,
                        ),
                        mock.patch.object(
                            pipeline,
                            "save_state",
                            side_effect=lambda value: saved_states.append(
                                copy.deepcopy(value)
                            ),
                        ),
                        mock.patch.object(
                            pipeline,
                            "save_cusip_map",
                            side_effect=lambda value: saved_maps.append(
                                copy.deepcopy(value)
                            ),
                        ),
                    ):
                        self.assertEqual(
                            1,
                            refresh_recent_13f_filings.main(),
                        )

                self.assertIn(
                    "checkpointed",
                    saved_states[-1]["_quarantined"],
                )
                self.assertEqual("AAPL", saved_maps[-1]["037833100"])

    def test_discovery_failure_does_not_write_an_unmutated_checkpoint(
        self,
    ) -> None:
        state = {"_processed_set": set(), "_quarantined": {}}
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir)
            with (
                mock.patch.object(pipeline, "DATA_DIR", data_dir),
                mock.patch.object(
                    pipeline,
                    "FUNDS_DIR",
                    data_dir / "funds",
                ),
                mock.patch.object(
                    pipeline,
                    "STOCKS_DIR",
                    data_dir / "stocks",
                ),
                mock.patch.object(
                    pipeline,
                    "load_state",
                    return_value=state,
                ),
                mock.patch.object(
                    pipeline,
                    "load_cusip_map",
                    return_value={},
                ),
                mock.patch.object(
                    refresh_recent_13f_filings,
                    "fetch_recent_feed_filings",
                    side_effect=RuntimeError("feed unavailable"),
                ),
                mock.patch.object(pipeline, "save_state") as save_state,
                mock.patch.object(pipeline, "save_cusip_map") as save_map,
            ):
                self.assertEqual(1, refresh_recent_13f_filings.main())

        save_state.assert_not_called()
        save_map.assert_not_called()


if __name__ == "__main__":
    unittest.main()
