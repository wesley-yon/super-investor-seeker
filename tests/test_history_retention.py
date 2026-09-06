from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import pipeline
from scripts import refresh_recent_13f_filings


CIK = 123456
TRIGGER = {
    "cik": CIK,
    "name": "Example Manager",
    "form_type": "13F-HR",
    "accession": "0000123456-26-000001",
    "date_filed": "2026-05-15",
    "accepted_at": "2026-05-15T12:00:00Z",
    "report_date": "2026-03-31",
    "filename": "",
}


def composed_quarter(report_date: str, suffix: str) -> dict:
    accession = f"0000123456-26-{suffix}"
    return {
        "report_date": report_date,
        "filing_date": "2026-05-15",
        "composition_version": pipeline.AMENDMENT_REDUCER_VERSION,
        "is_complete": True,
        "base_accession": accession,
        "source_filings": [{"accession": accession}],
        "holdings": [],
        "num_holdings": 0,
        "total_value": 0,
    }


class HistoryRetentionTests(unittest.TestCase):
    def test_quarantined_accession_retry_cooldown_is_weekly(self) -> None:
        now = datetime(2026, 7, 27, 12, tzinfo=timezone.utc)
        for reason in (
            pipeline.FilingParseError.__name__,
            "missing_base",
        ):
            with self.subTest(reason=reason):
                state = {
                    "_quarantined": {
                        TRIGGER["accession"]: {
                            "reason": reason,
                            "last_attempt_at": (
                                now - timedelta(days=6)
                            ).strftime("%Y-%m-%dT%H:%M:%SZ"),
                        }
                    }
                }
                self.assertFalse(
                    pipeline.accession_retry_due(
                        state,
                        TRIGGER["accession"],
                        now=now,
                    )
                )
                state["_quarantined"][TRIGGER["accession"]][
                    "last_attempt_at"
                ] = (now - timedelta(days=7)).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                )
                self.assertTrue(
                    pipeline.accession_retry_due(
                        state,
                        TRIGGER["accession"],
                        now=now,
                    )
                )
        self.assertTrue(
            pipeline.accession_retry_due({}, "new-accession", now=now)
        )

    def test_fetch_quarantine_retries_daily_and_preserves_error_type(
        self,
    ) -> None:
        now = datetime(2026, 7, 27, 12, tzinfo=timezone.utc)
        state = {"_processed_set": {TRIGGER["accession"]}}
        pipeline.quarantine_replay_failure(
            state,
            CIK,
            [TRIGGER],
            pipeline.FilingFetchError("SEC resource remained unavailable"),
        )
        diagnostic = state["_quarantined"][TRIGGER["accession"]]

        self.assertEqual(
            pipeline.FilingFetchError.__name__,
            diagnostic["reason"],
        )
        self.assertNotIn(TRIGGER["accession"], state["_processed_set"])

        diagnostic["last_attempt_at"] = (
            now - timedelta(hours=23)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.assertFalse(
            pipeline.accession_retry_due(
                state,
                TRIGGER["accession"],
                now=now,
            )
        )
        diagnostic["last_attempt_at"] = (
            now - timedelta(days=1)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.assertTrue(
            pipeline.accession_retry_due(
                state,
                TRIGGER["accession"],
                now=now,
            )
        )

    def test_merge_never_prunes_existing_history(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            funds_dir = Path(temporary) / "funds"
            funds_dir.mkdir()
            existing_dates = [
                "2025-12-31",
                "2025-09-30",
                "2025-06-30",
                "2025-03-31",
                "2024-12-31",
            ]
            (funds_dir / f"{CIK}.json").write_text(json.dumps({
                "cik": CIK,
                "name": "Example Manager",
                "quarters": [
                    composed_quarter(report_date, f"{index:06d}")
                    for index, report_date in enumerate(existing_dates)
                ],
            }))

            with mock.patch.object(pipeline, "FUNDS_DIR", funds_dir):
                merged = pipeline.merge_composed_quarters_into_fund(
                    CIK,
                    "Example Manager",
                    [composed_quarter("2026-03-31", "999999")],
                    1,
                    preserve_history=False,
                )

        self.assertEqual(
            ["2026-03-31", *existing_dates],
            [quarter["report_date"] for quarter in merged["quarters"]],
        )

    def test_single_fund_update_uses_quarters_only_for_discovery(self) -> None:
        with (
            mock.patch.object(
                pipeline, "load_state", return_value={"_processed_set": set()}
            ),
            mock.patch.object(pipeline, "load_cusip_map", return_value={}),
            mock.patch.object(
                pipeline,
                "get_13f_filings_for_cik",
                return_value=([TRIGGER], "Example Manager"),
            ) as discovery,
            mock.patch.object(
                pipeline, "replay_quarters_for_cik", return_value=1
            ) as replay,
            mock.patch.object(pipeline, "save_state"),
        ):
            self.assertTrue(pipeline.run_for_cik(CIK, 2, rebuild_outputs=False))

        discovery.assert_called_once_with(CIK, 2)
        self.assertIs(replay.call_args.kwargs["preserve_history"], True)
        self.assertIs(replay.call_args.kwargs["force"], True)

    def test_single_fund_failure_checkpoints_partial_replay_progress(self) -> None:
        state = {"_processed_set": set()}
        cusip_map = {}

        def fail_after_progress(*_args, **_kwargs) -> int:
            state["partial_retry"] = {"cik": CIK}
            cusip_map["123456789"] = "XYZ"
            raise pipeline.FilingParseError("later filing failed")

        with (
            mock.patch.object(pipeline, "load_state", return_value=state),
            mock.patch.object(
                pipeline, "load_cusip_map", return_value=cusip_map
            ),
            mock.patch.object(
                pipeline,
                "get_13f_filings_for_cik",
                return_value=([TRIGGER], "Example Manager"),
            ),
            mock.patch.object(
                pipeline,
                "replay_quarters_for_cik",
                side_effect=fail_after_progress,
            ),
            mock.patch.object(pipeline, "save_state") as save_state,
        ):
            self.assertFalse(
                pipeline.run_for_cik(CIK, 2, rebuild_outputs=False)
            )

        save_state.assert_called_once_with(state)
        self.assertEqual({"cik": CIK}, state["partial_retry"])
        self.assertEqual("XYZ", cusip_map["123456789"])

    def test_normal_ingestion_preserves_history(self) -> None:
        state = {
            "_processed_set": set(),
            "amendment_reducer_version": pipeline.AMENDMENT_REDUCER_VERSION,
            "amendment_migration_pending": {},
            "security_identity_migration_version":
                pipeline.SECURITY_IDENTITY_VERSION,
            "security_identity_migration_pending": {},
        }
        with (
            mock.patch.object(pipeline, "WORKER_COUNT", 1),
            mock.patch.object(pipeline.time, "sleep"),
            mock.patch.object(pipeline, "load_state", return_value=state),
            mock.patch.object(pipeline, "load_cusip_map", return_value={}),
            mock.patch.object(pipeline, "retry_pending_amendment_migrations"),
            mock.patch.object(
                pipeline, "get_recent_filing_quarters", return_value=[(2026, 2)]
            ),
            mock.patch.object(
                pipeline, "download_company_idx", return_value=[TRIGGER]
            ),
            mock.patch.object(
                pipeline, "replay_quarters_for_cik", return_value=1
            ) as replay,
            mock.patch.object(pipeline, "save_state"),
        ):
            self.assertTrue(pipeline.run_all(3, rebuild_outputs=False))

        self.assertIs(replay.call_args.kwargs["preserve_history"], True)

    def test_normal_ingestion_skips_recent_quarantine_retry(self) -> None:
        state = {
            "_processed_set": set(),
            "_quarantined": {
                TRIGGER["accession"]: {
                    "last_attempt_at": "2099-01-01T00:00:00Z",
                }
            },
            "amendment_reducer_version": pipeline.AMENDMENT_REDUCER_VERSION,
            "amendment_migration_pending": {},
            "security_identity_migration_version":
                pipeline.SECURITY_IDENTITY_VERSION,
            "security_identity_migration_pending": {},
        }
        with (
            mock.patch.object(pipeline, "WORKER_COUNT", 1),
            mock.patch.object(pipeline.time, "sleep"),
            mock.patch.object(pipeline, "load_state", return_value=state),
            mock.patch.object(pipeline, "load_cusip_map", return_value={}),
            mock.patch.object(pipeline, "retry_pending_amendment_migrations"),
            mock.patch.object(
                pipeline, "get_recent_filing_quarters", return_value=[(2026, 2)]
            ),
            mock.patch.object(
                pipeline, "download_company_idx", return_value=[TRIGGER]
            ),
            mock.patch.object(
                pipeline, "replay_quarters_for_cik", return_value=1
            ) as replay,
            mock.patch.object(pipeline, "save_state"),
        ):
            self.assertTrue(pipeline.run_all(3, rebuild_outputs=False))

        replay.assert_not_called()

    def test_recent_filing_ingestion_preserves_history(self) -> None:
        state = {"_processed_set": set(), "_quarantined": {}}
        with tempfile.TemporaryDirectory() as temporary:
            data_dir = Path(temporary)
            with (
                mock.patch.object(pipeline, "DATA_DIR", data_dir),
                mock.patch.object(pipeline, "FUNDS_DIR", data_dir / "funds"),
                mock.patch.object(pipeline, "STOCKS_DIR", data_dir / "stocks"),
                mock.patch.object(pipeline, "load_state", return_value=state),
                mock.patch.object(pipeline, "load_cusip_map", return_value={}),
                mock.patch.object(
                    refresh_recent_13f_filings,
                    "fetch_recent_feed_filings",
                    return_value=[TRIGGER],
                ),
                mock.patch.object(
                    pipeline, "replay_quarters_for_cik", return_value=1
                ) as replay,
                mock.patch.object(pipeline, "save_state"),
            ):
                self.assertEqual(0, refresh_recent_13f_filings.main())

        self.assertIs(replay.call_args.kwargs["preserve_history"], True)

    def test_recent_filing_ingestion_skips_recent_quarantine_retry(self) -> None:
        state = {
            "_processed_set": set(),
            "_quarantined": {
                TRIGGER["accession"]: {
                    "last_attempt_at": "2099-01-01T00:00:00Z",
                }
            },
        }
        with tempfile.TemporaryDirectory() as temporary:
            data_dir = Path(temporary)
            with (
                mock.patch.object(pipeline, "DATA_DIR", data_dir),
                mock.patch.object(pipeline, "FUNDS_DIR", data_dir / "funds"),
                mock.patch.object(pipeline, "STOCKS_DIR", data_dir / "stocks"),
                mock.patch.object(pipeline, "load_state", return_value=state),
                mock.patch.object(pipeline, "load_cusip_map", return_value={}),
                mock.patch.object(
                    refresh_recent_13f_filings,
                    "fetch_recent_feed_filings",
                    return_value=[TRIGGER],
                ),
                mock.patch.object(
                    pipeline, "replay_quarters_for_cik", return_value=1
                ) as replay,
                mock.patch.object(pipeline, "save_state"),
            ):
                self.assertEqual(0, refresh_recent_13f_filings.main())

        replay.assert_not_called()

    def test_amendment_repair_preserves_history(self) -> None:
        amendment = {**TRIGGER, "form_type": "13F-HR/A"}
        with (
            mock.patch.object(
                pipeline, "load_state", return_value={"_processed_set": set()}
            ),
            mock.patch.object(pipeline, "load_cusip_map", return_value={}),
            mock.patch.object(
                pipeline, "get_recent_filing_quarters", return_value=[(2026, 2)]
            ),
            mock.patch.object(
                pipeline, "download_company_idx", return_value=[amendment]
            ),
            mock.patch.object(
                pipeline, "replay_quarters_for_cik", return_value=1
            ) as replay,
            mock.patch.object(pipeline, "save_state"),
        ):
            self.assertTrue(
                pipeline.repair_amendments(4, rebuild_outputs=False)
            )

        self.assertIs(replay.call_args.kwargs["preserve_history"], True)


if __name__ == "__main__":
    unittest.main()
