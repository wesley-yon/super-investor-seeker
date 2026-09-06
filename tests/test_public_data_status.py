from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pipeline
import validate_data


class WithheldFundStatusTests(unittest.TestCase):
    @staticmethod
    def pipeline_paths(root: Path):
        data_dir = root / "data"
        funds_dir = data_dir / "funds"
        funds_dir.mkdir(parents=True)
        return (
            funds_dir,
            data_dir / "pipeline_state.json",
            root / "missing-state.json",
        )

    def test_newest_target_and_reasons_are_deterministic(self) -> None:
        state = {
            "_quarantined": {
                "old": {
                    "cik": 123,
                    "report_date": "2025-12-31",
                },
                "latest": {
                    "cik": 123,
                    "report_date": "2026-03-31",
                },
            },
            "security_identity_migration_pending": {
                "123:2026-03-31": {
                    "cik": 123,
                    "report_date": "2026-03-31",
                },
            },
        }

        targets = pipeline._active_withheld_targets_by_cik(state)

        self.assertEqual("2026-03-31", targets[123]["report_date"])
        self.assertEqual(
            {
                "SEC filing verification pending",
                "security identity verification pending",
            },
            targets[123]["reasons"],
        )

    def test_invalid_target_is_not_published_as_a_status(self) -> None:
        state = {
            "quarantined": {
                "bad-date": {
                    "cik": 123,
                    "report_date": "2026-04-01",
                },
                "bad-cik": {
                    "cik": "not-a-cik",
                    "report_date": "2026-03-31",
                },
            }
        }

        self.assertEqual(
            {},
            pipeline._active_withheld_targets_by_cik(state),
        )

    def test_index_validator_requires_exact_active_withheld_status(self) -> None:
        calendar = {
            "123": {
                "report_dates": ("2025-12-31",),
                "q": (20254,),
            }
        }
        state = {
            "quarantined": {
                "new": {
                    "cik": 123,
                    "report_date": "2026-03-31",
                }
            }
        }
        fund_entry = {
            "cik": 123,
            "name": "Withheld Fund",
            "q": [20254],
            "status": "WITHHELD",
            "latest_withheld_report_date": "2026-03-31",
            "withheld_reason": "SEC filing verification pending",
        }
        index = {
            "funds": [fund_entry],
            "tickers": [],
            "total_filers": 1,
            "total_tickers": 0,
        }
        errors: list[str] = []

        validate_data.validate_index(
            index,
            {"123": Path("123.json")},
            {},
            {},
            errors,
            [],
            calendar,
            state,
        )
        self.assertEqual([], errors)

        del fund_entry["status"]
        errors.clear()
        validate_data.validate_index(
            index,
            {"123": Path("123.json")},
            {},
            {},
            errors,
            [],
            calendar,
            state,
        )
        self.assertTrue(any(
            "must publish status WITHHELD" in error for error in errors
        ))

    def test_unpublished_first_filing_remains_retry_only(self) -> None:
        state = {
            "quarantined": {
                "first-filing": {
                    "cik": 456,
                    "report_date": "2026-03-31",
                }
            }
        }

        self.assertEqual(
            {},
            validate_data.expected_index_withheld_statuses(state, {}),
        )

    def test_existing_empty_fund_still_requires_withheld_status(self) -> None:
        state = {
            "quarantined": {
                "withheld-empty-fund": {
                    "cik": 456,
                    "report_date": "2026-03-31",
                }
            }
        }

        self.assertEqual(
            {"456": "2026-03-31"},
            validate_data.expected_index_withheld_statuses(
                state,
                {
                    "456": {
                        "report_dates": (),
                        "q": (),
                    }
                },
            ),
        )

    def test_historical_quarantine_is_an_exact_comparison_gap(self) -> None:
        calendar = {
            "123": {
                "report_dates": ("2026-03-31", "2025-12-31"),
                "q": (20261, 20254),
            }
        }
        state = {
            "quarantined": {
                "historical": {
                    "cik": 123,
                    "report_date": "2025-12-31",
                }
            }
        }
        fund_entry = {
            "cik": 123,
            "name": "Current Fund With Historical Gap",
            "q": [20261, 20254],
            "unverified_report_dates": ["2025-12-31"],
        }
        index = {
            "funds": [fund_entry],
            "tickers": [],
            "total_filers": 1,
            "total_tickers": 0,
        }
        errors: list[str] = []

        self.assertEqual(
            {
                123: {
                    "2025-12-31": {
                        "SEC filing verification pending",
                    }
                }
            },
            pipeline._active_unverified_targets_by_cik({
                "_quarantined": state["quarantined"],
            }),
        )
        validate_data.validate_index(
            index,
            {"123": Path("123.json")},
            {},
            {},
            errors,
            [],
            calendar,
            state,
        )
        self.assertEqual([], errors)

        del fund_entry["unverified_report_dates"]
        validate_data.validate_index(
            index,
            {"123": Path("123.json")},
            {},
            {},
            errors,
            [],
            calendar,
            state,
        )
        self.assertTrue(any(
            "unverified_report_dates" in error for error in errors
        ))

    def test_health_enforcement_queues_before_withholding(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            funds_dir, state_path, legacy_state_path = self.pipeline_paths(root)
            fund_path = funds_dir / "123.json"
            fund_path.write_text(json.dumps({
                "cik": 123,
                "name": "Malformed Fund",
                "quarters": [{
                    "report_date": "2026-03-31",
                    "accession": "source-accession",
                    "num_holdings": 1,
                    "holdings": [],
                    "total_value": 0,
                }],
            }))
            state = {
                "_processed_set": {"source-accession"},
                "_quarantined": {},
                "quarter_health_pending": {},
            }

            with mock.patch.multiple(
                pipeline,
                FUNDS_DIR=funds_dir,
                STATE_PATH=state_path,
                LEGACY_STATE_PATH=legacy_state_path,
            ):
                withheld = pipeline.enforce_published_quarter_health(state)

            self.assertEqual(1, withheld)
            self.assertEqual(
                [],
                json.loads(fund_path.read_text())["quarters"],
            )
            key = "123:2026-03-31"
            self.assertIn(key, state["quarter_health_pending"])
            self.assertNotIn("source-accession", state["_processed_set"])
            self.assertEqual(
                "QuarterHealthError",
                state["_quarantined"]["source-accession"]["reason"],
            )
            persisted = json.loads(state_path.read_text())
            self.assertIn(key, persisted["quarter_health_pending"])

    def test_health_enforcement_refuses_to_delete_without_queue_readback(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            funds_dir, state_path, legacy_state_path = self.pipeline_paths(root)
            fund_path = funds_dir / "123.json"
            original = {
                "cik": 123,
                "quarters": [{
                    "report_date": "2026-03-31",
                    "accession": "source-accession",
                    "num_holdings": 1,
                    "holdings": [],
                    "total_value": 0,
                }],
            }
            fund_path.write_text(json.dumps(original))
            state = {
                "_processed_set": {"source-accession"},
                "_quarantined": {},
                "quarter_health_pending": {},
            }

            with (
                mock.patch.multiple(
                    pipeline,
                    FUNDS_DIR=funds_dir,
                    STATE_PATH=state_path,
                    LEGACY_STATE_PATH=legacy_state_path,
                ),
                mock.patch.object(pipeline, "save_state"),
            ):
                with self.assertRaises(pipeline.FundDataError):
                    pipeline.enforce_published_quarter_health(state)

            self.assertEqual(
                original["quarters"],
                json.loads(fund_path.read_text())["quarters"],
            )

    def test_healthy_replay_clears_pending_status_and_diagnostic(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            funds_dir, state_path, legacy_state_path = self.pipeline_paths(root)
            (funds_dir / "123.json").write_text(json.dumps({
                "cik": 123,
                "name": "Recovered Fund",
                "quarters": [{
                    "report_date": "2026-03-31",
                    "accession": "source-accession",
                    "num_holdings": 0,
                    "holdings": [],
                    "total_value": 0,
                }],
            }))
            key = "123:2026-03-31"
            state = {
                "_processed_set": set(),
                "_quarantined": {
                    "source-accession": {
                        "reason": "QuarterHealthError",
                    }
                },
                "quarter_health_pending": {
                    key: {
                        "cik": 123,
                        "report_date": "2026-03-31",
                        "source_accessions": ["source-accession"],
                    }
                },
            }

            with mock.patch.multiple(
                pipeline,
                FUNDS_DIR=funds_dir,
                STATE_PATH=state_path,
                LEGACY_STATE_PATH=legacy_state_path,
            ):
                withheld = pipeline.enforce_published_quarter_health(state)

            self.assertEqual(0, withheld)
            self.assertNotIn(key, state["quarter_health_pending"])
            self.assertNotIn("source-accession", state["_quarantined"])
            self.assertIn("source-accession", state["_processed_set"])

    def test_failed_health_retry_keeps_health_diagnostic_identity(self) -> None:
        state = {
            "_processed_set": set(),
            "_quarantined": {},
        }
        pipeline.quarantine_replay_failure(
            state,
            123,
            [{
                "accession": "source-accession",
                "report_date": "2026-03-31",
            }],
            pipeline.FilingParseError("source is still malformed"),
            reason_override="QuarterHealthError",
        )

        self.assertEqual(
            "QuarterHealthError",
            state["_quarantined"]["source-accession"]["reason"],
        )

    def test_successful_health_replay_defers_state_cleanup_to_health_scan(
        self,
    ) -> None:
        report_date = "2026-03-31"
        trigger = {
            "cik": 123,
            "accession": "source-accession",
            "report_date": report_date,
            "form_type": "13F-HR",
        }
        quarter = {
            "report_date": report_date,
            "holdings": [],
            "source_filings": [{"accession": "source-accession"}],
        }
        state = {
            "_processed_set": set(),
            "_quarantined": {
                "source-accession": {
                    "reason": "QuarterHealthError",
                }
            },
            "amendment_migration_pending": {},
            "security_identity_migration_pending": {},
            "quarter_health_pending": {
                "123:2026-03-31": {
                    "cik": 123,
                    "report_date": report_date,
                    "source_accessions": ["source-accession"],
                }
            },
        }

        with mock.patch.multiple(
            pipeline,
            _compose_replay_targets=mock.Mock(return_value=[quarter]),
            update_holding_tickers=mock.Mock(),
            merge_composed_quarters_into_fund=mock.Mock(return_value={
                "cik": 123,
                "quarters": [quarter],
            }),
            save_fund=mock.Mock(),
            _security_identity_target_is_resolved=mock.Mock(
                return_value=False
            ),
        ):
            replayed = pipeline.replay_quarters_for_cik(
                123,
                [trigger],
                1,
                state,
                force=True,
                replace_only=True,
                discovered_submission=([trigger], "Recovered Fund"),
            )

        self.assertEqual(1, replayed)
        self.assertNotIn("source-accession", state["_processed_set"])
        self.assertIn("source-accession", state["_quarantined"])
        self.assertIn(
            "123:2026-03-31",
            state["quarter_health_pending"],
        )

    def test_unhealthy_replacement_releases_obsolete_health_source(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            funds_dir, state_path, legacy_state_path = self.pipeline_paths(root)
            (funds_dir / "123.json").write_text(json.dumps({
                "cik": 123,
                "quarters": [{
                    "report_date": "2026-03-31",
                    "accession": "replacement-source",
                    "num_holdings": 1,
                    "holdings": [],
                    "total_value": 0,
                }],
            }))
            key = "123:2026-03-31"
            state = {
                "_processed_set": set(),
                "_quarantined": {
                    "obsolete-source": {
                        "reason": "QuarterHealthError",
                    }
                },
                "quarter_health_pending": {
                    key: {
                        "cik": 123,
                        "report_date": "2026-03-31",
                        "source_accessions": ["obsolete-source"],
                    }
                },
            }

            with mock.patch.multiple(
                pipeline,
                FUNDS_DIR=funds_dir,
                STATE_PATH=state_path,
                LEGACY_STATE_PATH=legacy_state_path,
            ):
                self.assertEqual(
                    1,
                    pipeline.enforce_published_quarter_health(state),
                )

            target = state["quarter_health_pending"][key]
            self.assertEqual(
                ["replacement-source"],
                target["source_accessions"],
            )
            self.assertIn("obsolete-source", state["_processed_set"])
            self.assertNotIn("obsolete-source", state["_quarantined"])
            self.assertNotIn("replacement-source", state["_processed_set"])
            self.assertEqual(
                "QuarterHealthError",
                state["_quarantined"]["replacement-source"]["reason"],
            )

    def test_peer_distortion_is_withheld_without_harming_clean_peers(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            funds_dir, state_path, legacy_state_path = self.pipeline_paths(root)

            def rows(*, distorted: bool) -> list[dict]:
                holdings = []
                for position in range(12):
                    shares = 100_000 + position
                    price = 100 + position
                    holdings.append({
                        "cusip": f"{position:09d}",
                        "holding_type": "EQUITY",
                        "value": (
                            shares if distorted else shares * price
                        ),
                        "shares": (
                            shares * price if distorted else shares
                        ),
                    })
                return holdings

            for cik in range(1, 6):
                holdings = rows(distorted=False)
                (funds_dir / f"{cik}.json").write_text(json.dumps({
                    "cik": cik,
                    "quarters": [{
                        "report_date": "2026-03-31",
                        "num_holdings": len(holdings),
                        "total_value": sum(row["value"] for row in holdings),
                        "holdings": holdings,
                    }],
                }))
            distorted_holdings = rows(distorted=True)
            (funds_dir / "999.json").write_text(json.dumps({
                "cik": 999,
                "quarters": [{
                    "report_date": "2026-03-31",
                    "accession": "bad-source",
                    "num_holdings": len(distorted_holdings),
                    "total_value": sum(
                        row["value"] for row in distorted_holdings
                    ),
                    "holdings": distorted_holdings,
                }],
            }))
            state = {
                "_processed_set": {"bad-source"},
                "_quarantined": {},
                "quarter_health_pending": {},
            }

            with mock.patch.multiple(
                pipeline,
                FUNDS_DIR=funds_dir,
                STATE_PATH=state_path,
                LEGACY_STATE_PATH=legacy_state_path,
            ):
                self.assertEqual(
                    1,
                    pipeline.enforce_published_quarter_health(state),
                )

            self.assertEqual(
                [],
                json.loads((funds_dir / "999.json").read_text())["quarters"],
            )
            for cik in range(1, 6):
                self.assertEqual(
                    1,
                    len(json.loads(
                        (funds_dir / f"{cik}.json").read_text()
                    )["quarters"]),
                )


class SplitAdjustmentTests(unittest.TestCase):
    @staticmethod
    def holders(
        *,
        count: int = 25,
        share_factor: float = 10,
        price_factor: float = 0.1,
        imputed: bool = False,
    ) -> list[dict]:
        rows = []
        for cik in range(1, count + 1):
            prior_shares = 100 + cik
            prior_price = 100.0
            current_shares = prior_shares * share_factor
            current_price = prior_price * price_factor
            rows.append({
                "cik": cik,
                "history": [
                    {
                        "date": "2025-12-31",
                        "shares": current_shares,
                        "value": current_shares * current_price,
                        **({"shares_imputed": True} if imputed else {}),
                    },
                    {
                        "date": "2025-09-30",
                        "shares": prior_shares,
                        "value": prior_shares * prior_price,
                    },
                ],
            })
        return rows

    def test_consensus_share_and_inverse_price_move_proves_split(self) -> None:
        adjustments = pipeline.infer_proven_split_adjustments(self.holders())

        self.assertEqual(1, len(adjustments))
        self.assertEqual(
            {
                "from_report_date": "2025-09-30",
                "to_report_date": "2025-12-31",
                "factor": 10,
                "proven": True,
                "support": 25,
                "observations": 25,
            },
            adjustments[0],
        )

    def test_price_confirmation_is_required(self) -> None:
        self.assertEqual(
            [],
            pipeline.infer_proven_split_adjustments(
                self.holders(price_factor=0.8)
            ),
        )

    def test_imputed_shares_and_small_samples_fail_closed(self) -> None:
        self.assertEqual(
            [],
            pipeline.infer_proven_split_adjustments(
                self.holders(imputed=True)
            ),
        )
        self.assertEqual(
            [],
            pipeline.infer_proven_split_adjustments(
                self.holders(count=19)
            ),
        )

    def test_stock_validator_recomputes_split_proof(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            stocks_dir = Path(temporary) / "stocks"
            stocks_dir.mkdir()
            holders = json.loads(json.dumps(self.holders()))
            for holder in holders:
                for row in holder["history"]:
                    row["pct_of_fund"] = 1.0
            adjustments = pipeline.infer_proven_split_adjustments(holders)
            stock_path = stocks_dir / "123456789.json"
            stock = {
                "stock_id": "123456789",
                "cusip": "123456789",
                "ticker": "TEST",
                "issuer": "TEST ISSUER",
                "instrument_type": "EQUITY",
                "holders": holders,
                "split_adjustments": adjustments,
            }
            stock_path.write_text(json.dumps(stock))

            errors: list[str] = []
            with mock.patch.object(
                validate_data,
                "STOCKS_DIR",
                stocks_dir,
            ):
                validate_data.validate_stocks(errors)
            self.assertEqual([], errors)

            stock["split_adjustments"][0]["factor"] = 5
            stock_path.write_text(json.dumps(stock))
            errors.clear()
            with mock.patch.object(
                validate_data,
                "STOCKS_DIR",
                stocks_dir,
            ):
                validate_data.validate_stocks(errors)
            self.assertTrue(any(
                "split_adjustments do not match" in error
                for error in errors
            ))


if __name__ == "__main__":
    unittest.main()
