"""Focused coverage for the SEC-backed security identity migration."""

from __future__ import annotations

import copy
import hashlib
import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

import pipeline
import validate_data


CIK = 123456
REPORT_DATE = "2025-12-31"
ACCESSION = "0000123456-26-000001"


def unsafe_holding() -> dict:
    return {
        "ticker": "XYZ",
        "issuer": "EXAMPLE ETF",
        "cusip": "123456789",
        "class": "ETF",
        "value": 100,
        "shares": 10,
        "holding_type": "PUT",
    }


def current_holding() -> dict:
    return {
        **unsafe_holding(),
        "holding_type": "EQUITY",
    }


def filing_row(report_date: str = REPORT_DATE) -> dict:
    return {
        "cik": CIK,
        "name": "Example Manager",
        "form_type": "13F-HR",
        "accession": ACCESSION,
        "date_filed": "2026-02-14",
        "accepted_at": "2026-02-14T12:00:00Z",
        "report_date": report_date,
        "filename": "",
    }


def current_component(report_date: str = REPORT_DATE) -> dict:
    row = filing_row(report_date)
    return {
        "cik": CIK,
        "report_date": report_date,
        "filing_date": row["date_filed"],
        "accepted_at": row["accepted_at"],
        "accession": row["accession"],
        "form_type": row["form_type"],
        "amendment_number": None,
        "amendment_kind": "ORIGINAL",
        "reported_entry_total": 1,
        "reported_value_total": 100,
        "normalized_value_total": 100,
        "value_unit_policy_version": 1,
        "value_multiplier": 1,
        "value_unit_method": "fixture",
        "value_unit_confidence": "high",
        "value_unit_evidence": {},
        "security_identity_version": pipeline.SECURITY_IDENTITY_VERSION,
        "source_hash": hashlib.sha256(ACCESSION.encode()).hexdigest(),
        "holdings": [current_holding()],
    }


def fund_payload(*, include_neighbor: bool = False) -> dict:
    quarters = [
        {
            "report_date": REPORT_DATE,
            "filing_date": "2026-02-14",
            "total_value": 100,
            "num_holdings": 1,
            "holdings": [unsafe_holding()],
        }
    ]
    if include_neighbor:
        quarters.append({
            "report_date": "2025-09-30",
            "filing_date": "2025-11-14",
            "total_value": 50,
            "num_holdings": 1,
            "holdings": [{
                **current_holding(),
                "value": 50,
                "shares": 5,
            }],
        })
    return {
        "cik": CIK,
        "name": "Example Manager",
        "quarters": quarters,
    }


class IdentityInventoryTests(unittest.TestCase):
    def test_inventory_targets_only_rows_that_current_proof_would_change(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir) / "funds"
            funds_dir.mkdir()
            payload = fund_payload()
            payload["quarters"].append({
                "report_date": "2025-09-30",
                "holdings": [{
                    **unsafe_holding(),
                    "class": "PUT",
                    "holding_type": "PUT",
                }],
            })
            payload["quarters"].append({
                "report_date": "2025-06-30",
                "holdings": [{
                    **unsafe_holding(),
                    "class": "OPTION",
                    "holding_type": "OPT",
                }],
            })
            (funds_dir / f"{CIK}.json").write_text(json.dumps(payload))

            with mock.patch.object(pipeline, "FUNDS_DIR", funds_dir):
                targets = (
                    pipeline.retained_security_identity_migration_targets()
                )

        self.assertEqual(
            [{"cik": CIK, "report_date": REPORT_DATE}],
            targets,
        )
        self.assertTrue(
            pipeline.has_unsafe_legacy_option_identity(unsafe_holding())
        )
        self.assertFalse(
            pipeline.has_unsafe_legacy_option_identity(
                {
                    **unsafe_holding(),
                    "class": "PUT",
                    "holding_type": "PUT",
                }
            )
        )

    def test_inventory_skips_proof_backed_unsafe_saved_row(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir) / "funds"
            funds_dir.mkdir()
            proof_backed = pipeline.compose_quarter_filings(
                [current_component()]
            )
            proof_backed["holdings"] = [unsafe_holding()]
            payload = fund_payload()
            payload["quarters"] = [proof_backed]
            (funds_dir / f"{CIK}.json").write_text(json.dumps(payload))

            self.assertTrue(
                pipeline._quarter_retains_raw_put_call(proof_backed)
            )
            self.assertTrue(
                pipeline.has_unsafe_legacy_option_identity(
                    proof_backed["holdings"][0]
                )
            )
            self.assertFalse(
                pipeline.quarter_has_unsafe_legacy_option_identity(
                    proof_backed
                )
            )
            with mock.patch.object(pipeline, "FUNDS_DIR", funds_dir):
                targets = (
                    pipeline.retained_security_identity_migration_targets()
                )

        self.assertEqual([], targets)


class IdentityMarkerTests(unittest.TestCase):
    def test_current_hash_version_binds_public_holding_type(self) -> None:
        quarter = pipeline.compose_quarter_filings([current_component()])
        self.assertEqual(
            pipeline.COMPOSITION_HASH_VERSION,
            quarter["composition_hash_version"],
        )

        tampered = copy.deepcopy(quarter)
        tampered["holdings"][0]["holding_type"] = "NOTE"
        self.assertNotEqual(
            quarter["composition_hash"],
            validate_data.calculate_composition_hash(tampered),
        )
        errors: list[str] = []
        validate_data.validate_amendment_composition(
            tampered,
            "tampered holding type",
            errors,
        )
        self.assertTrue(any(
            "composition_hash does not match" in error
            for error in errors
        ))

    def test_composed_identity_proof_requires_all_applied_source_markers(
        self,
    ) -> None:
        quarter = pipeline.compose_quarter_filings([current_component()])

        self.assertEqual(
            pipeline.SECURITY_IDENTITY_VERSION,
            quarter["security_identity_version"],
        )
        self.assertEqual(
            pipeline.SECURITY_IDENTITY_VERSION,
            quarter["source_filings"][0]["security_identity_version"],
        )
        self.assertTrue(pipeline._quarter_retains_raw_put_call(quarter))

        tampered = copy.deepcopy(quarter)
        del tampered["source_filings"][0]["security_identity_version"]
        self.assertFalse(pipeline._quarter_retains_raw_put_call(tampered))
        self.assertNotEqual(
            quarter["composition_hash"],
            validate_data.calculate_composition_hash(tampered),
        )
        errors: list[str] = []
        validate_data.validate_amendment_composition(
            tampered, "tampered quarter", errors
        )
        self.assertTrue(any(
            "top-level security identity proof is not backed" in error
            for error in errors
        ))
        self.assertTrue(any(
            "composition_hash does not match" in error
            for error in errors
        ))

        top_level_tamper = copy.deepcopy(quarter)
        top_level_tamper["source_filings"][0]["filing_date"] = "2026-05-01"
        del top_level_tamper["security_identity_version"]
        self.assertFalse(
            pipeline._quarter_retains_raw_put_call(top_level_tamper)
        )

    def test_legacy_v2_hash_stays_stable_when_marker_is_absent(self) -> None:
        component = current_component()
        del component["security_identity_version"]
        quarter = pipeline.compose_quarter_filings([component])
        self.assertNotIn("security_identity_version", quarter)
        self.assertNotIn(
            "security_identity_version", quarter["source_filings"][0]
        )
        self.assertEqual(
            quarter["composition_hash"],
            validate_data.calculate_composition_hash(quarter),
        )


class IdentityReplayTests(unittest.TestCase):
    def test_interrupt_checkpoints_identity_replay_state(self) -> None:
        state = {}
        cusip_map = {}

        class InterruptibleThread:
            def __init__(self, *args, **kwargs) -> None:
                self.name = kwargs["name"]
                self._checks = 0

            def start(self) -> None:
                return None

            def is_alive(self) -> bool:
                self._checks += 1
                return self._checks == 1

            def join(self, timeout=None) -> None:
                return None

        with (
            mock.patch.object(
                pipeline.threading, "Thread", InterruptibleThread
            ),
            mock.patch.object(
                pipeline.time, "sleep", side_effect=KeyboardInterrupt
            ),
            mock.patch.object(pipeline, "save_state") as save_state,
        ):
            succeeded, resolved = pipeline._run_security_identity_replays(
                [{"cik": CIK, "report_date": REPORT_DATE}],
                state,
                cusip_map,
            )

        self.assertFalse(succeeded)
        self.assertEqual(0, resolved)
        save_state.assert_called_once_with(state)

    def test_alive_identity_worker_gets_final_checkpoint(self) -> None:
        state = {}
        cusip_map = {}

        class LateAliveThread:
            def __init__(self, *args, **kwargs) -> None:
                self.name = kwargs["name"]
                self._checks = 0

            def start(self) -> None:
                return None

            def is_alive(self) -> bool:
                self._checks += 1
                return self._checks > 1

            def join(self, timeout=None) -> None:
                return None

        with (
            mock.patch.object(pipeline.threading, "Thread", LateAliveThread),
            mock.patch.object(pipeline, "save_state") as save_state,
        ):
            succeeded, resolved = pipeline._run_security_identity_replays(
                [{"cik": CIK, "report_date": REPORT_DATE}],
                state,
                cusip_map,
            )

        self.assertFalse(succeeded)
        self.assertEqual(0, resolved)
        save_state.assert_called_once_with(state)

    def _write_fund(self, funds_dir: Path, *, neighbor: bool = False) -> Path:
        path = funds_dir / f"{CIK}.json"
        path.write_text(json.dumps(fund_payload(include_neighbor=neighbor)))
        return path

    def test_no_accession_target_replays_only_its_report_date(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir) / "funds"
            funds_dir.mkdir()
            fund_path = self._write_fund(funds_dir, neighbor=True)
            before_neighbor = json.loads(fund_path.read_text())["quarters"][1]
            target = {"cik": CIK, "report_date": REPORT_DATE}
            key = pipeline.security_identity_migration_key(
                CIK, REPORT_DATE
            )
            state = {
                "_processed_set": set(),
                "_quarantined": {},
                "amendment_migration_pending": {},
                "security_identity_migration_pending": {
                    key: {
                        **target,
                        "reason": "awaiting_replay",
                        "message": "",
                        "last_attempt_at": None,
                    }
                },
            }

            with (
                mock.patch.object(pipeline, "FUNDS_DIR", funds_dir),
                mock.patch.object(
                    pipeline,
                    "_discover_submission_filings",
                    return_value=([filing_row()], "Example Manager"),
                ) as discover,
                mock.patch.object(
                    pipeline,
                    "fetch_filing_holdings",
                    return_value=current_component(),
                ),
            ):
                resolved = (
                    pipeline._replay_security_identity_target_group(
                        CIK,
                        [target],
                        state,
                        {"123456789": "XYZ"},
                        threading.Lock(),
                    )
                )

            self.assertEqual(1, resolved)
            self.assertNotIn(key, state["security_identity_migration_pending"])
            discover.assert_called_once_with(CIK, include_archives=False)
            repaired = json.loads(fund_path.read_text())
            by_date = {
                quarter["report_date"]: quarter
                for quarter in repaired["quarters"]
            }
            self.assertEqual(
                pipeline.SECURITY_IDENTITY_VERSION,
                by_date[REPORT_DATE]["security_identity_version"],
            )
            self.assertEqual(
                before_neighbor,
                by_date["2025-09-30"],
            )

    def test_normal_replay_clears_restored_identity_pending_key(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir) / "funds"
            funds_dir.mkdir()
            target = {"cik": CIK, "report_date": REPORT_DATE}
            key = pipeline.security_identity_migration_key(
                CIK, REPORT_DATE
            )
            state = {
                "_processed_set": set(),
                "_quarantined": {},
                "amendment_migration_pending": {},
                "security_identity_migration_pending": {
                    key: {
                        **target,
                        "reason": "replay_incomplete",
                        "message": "prior SEC failure",
                        "last_attempt_at": "2026-07-25T12:00:00Z",
                    }
                },
            }

            with (
                mock.patch.object(pipeline, "FUNDS_DIR", funds_dir),
                mock.patch.object(
                    pipeline,
                    "_discover_submission_filings",
                    return_value=([filing_row()], "Example Manager"),
                ),
                mock.patch.object(
                    pipeline,
                    "fetch_filing_holdings",
                    return_value=current_component(),
                ),
            ):
                processed = pipeline.replay_quarters_for_cik(
                    CIK,
                    [filing_row()],
                    {"123456789": "XYZ"},
                    4,
                    state,
                    preserve_history=True,
                )

            published = json.loads(
                (funds_dir / f"{CIK}.json").read_text()
            )

        self.assertEqual(1, processed)
        self.assertNotIn(
            key, state["security_identity_migration_pending"]
        )
        self.assertEqual(
            pipeline.SECURITY_IDENTITY_VERSION,
            published["quarters"][0]["security_identity_version"],
        )

    def test_failed_target_is_withheld_then_restored_from_pending_date(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir) / "funds"
            funds_dir.mkdir()
            fund_path = self._write_fund(funds_dir)
            target = {"cik": CIK, "report_date": REPORT_DATE}
            key = pipeline.security_identity_migration_key(
                CIK, REPORT_DATE
            )
            state = {
                "_processed_set": set(),
                "_quarantined": {},
                "amendment_migration_pending": {},
                "security_identity_migration_pending": {
                    key: {
                        **target,
                        "reason": "awaiting_replay",
                        "message": "",
                        "last_attempt_at": None,
                    }
                },
            }

            with (
                mock.patch.object(pipeline, "FUNDS_DIR", funds_dir),
                mock.patch.object(
                    pipeline,
                    "_discover_submission_filings",
                    side_effect=pipeline.FilingDiscoveryError("offline"),
                ),
            ):
                self.assertEqual(
                    0,
                    pipeline._replay_security_identity_target_group(
                        CIK,
                        [target],
                        state,
                        {"123456789": "XYZ"},
                        threading.Lock(),
                    ),
                )
                self.assertEqual(
                    1,
                    pipeline.withhold_pending_security_identity_quarters(
                        state
                    ),
                )

            self.assertEqual(
                [], json.loads(fund_path.read_text())["quarters"]
            )
            self.assertEqual(
                "discovery_failed",
                state["security_identity_migration_pending"][key]["reason"],
            )

            with (
                mock.patch.object(pipeline, "FUNDS_DIR", funds_dir),
                mock.patch.object(
                    pipeline,
                    "_discover_submission_filings",
                    return_value=([filing_row()], "Example Manager"),
                ),
                mock.patch.object(
                    pipeline,
                    "fetch_filing_holdings",
                    return_value=current_component(),
                ),
            ):
                self.assertEqual(
                    1,
                    pipeline._replay_security_identity_target_group(
                        CIK,
                        [target],
                        state,
                        {"123456789": "XYZ"},
                        threading.Lock(),
                    ),
                )

            restored = json.loads(fund_path.read_text())["quarters"]
            self.assertEqual([REPORT_DATE], [
                quarter["report_date"] for quarter in restored
            ])
            self.assertNotIn(key, state["security_identity_migration_pending"])

    def test_missing_recent_date_fetches_archives_only_as_fallback(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir) / "funds"
            funds_dir.mkdir()
            self._write_fund(funds_dir)
            target = {"cik": CIK, "report_date": REPORT_DATE}
            key = pipeline.security_identity_migration_key(
                CIK, REPORT_DATE
            )
            state = {
                "_processed_set": set(),
                "_quarantined": {},
                "amendment_migration_pending": {},
                "security_identity_migration_pending": {
                    key: {
                        **target,
                        "reason": "awaiting_replay",
                        "message": "",
                        "last_attempt_at": None,
                    }
                },
            }
            unrelated = filing_row("2025-09-30")

            with (
                mock.patch.object(pipeline, "FUNDS_DIR", funds_dir),
                mock.patch.object(
                    pipeline,
                    "_discover_submission_filings",
                    side_effect=[
                        ([unrelated], "Example Manager"),
                        ([unrelated, filing_row()], "Example Manager"),
                    ],
                ) as discover,
                mock.patch.object(
                    pipeline,
                    "fetch_filing_holdings",
                    return_value=current_component(),
                ),
            ):
                self.assertEqual(
                    1,
                    pipeline._replay_security_identity_target_group(
                        CIK,
                        [target],
                        state,
                        {"123456789": "XYZ"},
                        threading.Lock(),
                    ),
                )

            self.assertEqual(
                [
                    mock.call(CIK, include_archives=False),
                    mock.call(CIK, include_archives=True),
                ],
                discover.call_args_list,
            )


class IdentityMigrationOrchestrationTests(unittest.TestCase):
    def test_failure_limit_requires_ninety_percent_success_at_corpus_scale(
        self,
    ) -> None:
        self.assertEqual(
            0, pipeline.security_identity_migration_failure_limit(0)
        )
        self.assertEqual(
            1, pipeline.security_identity_migration_failure_limit(9)
        )
        self.assertEqual(
            1, pipeline.security_identity_migration_failure_limit(19)
        )
        self.assertEqual(
            2, pipeline.security_identity_migration_failure_limit(20)
        )
        self.assertEqual(
            9, pipeline.security_identity_migration_failure_limit(99)
        )
        self.assertEqual(
            250, pipeline.security_identity_migration_failure_limit(2_500)
        )
        self.assertEqual(
            1_000,
            pipeline.security_identity_migration_failure_limit(10_000),
        )
        self.assertEqual(
            1_844,
            pipeline.security_identity_migration_failure_limit(18_441),
        )

    def test_live_corpus_quarantine_is_inside_identity_success_floor(
        self,
    ) -> None:
        failure_limit = pipeline.security_identity_migration_failure_limit(
            18_441
        )
        self.assertIsNone(
            pipeline.security_identity_migration_health_error(
                total=18_441,
                resolved=17_559,
                unresolved=882,
            )
        )
        self.assertIsNotNone(
            pipeline.security_identity_migration_health_error(
                total=18_441,
                resolved=18_441 - failure_limit - 1,
                unresolved=failure_limit + 1,
            )
        )
        self.assertIsNotNone(
            pipeline.security_identity_migration_health_error(
                total=18_441,
                resolved=0,
                unresolved=18_441,
            )
        )
        self.assertIsNotNone(
            pipeline.security_identity_migration_health_error(
                total=18_441,
                resolved=17_559,
                unresolved=881,
            )
        )

    def test_pending_target_without_an_attempt_cannot_complete_migration(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir) / "funds"
            funds_dir.mkdir()
            (funds_dir / f"{CIK}.json").write_text(json.dumps({
                "cik": CIK,
                "name": "Example Manager",
                "quarters": [],
            }))
            target = {"cik": CIK, "report_date": REPORT_DATE}
            key = pipeline.security_identity_migration_key(
                CIK, REPORT_DATE
            )
            state = {
                "security_identity_migration_pending": {
                    key: {
                        **target,
                        "reason": "awaiting_replay",
                        "message": "awaiting authoritative SEC replay",
                        "last_attempt_at": None,
                    }
                }
            }

            with mock.patch.object(pipeline, "FUNDS_DIR", funds_dir):
                errors = (
                    pipeline.security_identity_migration_outcome_errors(
                        [target], state
                    )
                )

        self.assertTrue(any(
            "without completed-attempt diagnostics" in error
            for error in errors
        ))

    def test_systemic_failure_keeps_every_quarter_published_and_unadvanced(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            funds_dir = root / "funds"
            funds_dir.mkdir()
            fund_path = funds_dir / f"{CIK}.json"
            original_fund = fund_payload(include_neighbor=True)
            fund_path.write_text(json.dumps(original_fund))
            state_path = root / "pipeline_state.json"
            state_path.write_text(json.dumps({
                "processed": [],
                "quarantined": {},
                "amendment_reducer_version": 2,
                "amendment_migration_pending": {},
            }))
            target = {"cik": CIK, "report_date": REPORT_DATE}

            def record_systemic_failure(
                targets: list[dict],
                state: dict,
                _cusip_map: dict[str, str],
            ) -> tuple[bool, int]:
                pipeline._set_security_identity_pending(
                    state,
                    targets[0],
                    reason="discovery_failed",
                    message="offline",
                )
                return True, 0

            with (
                mock.patch.object(pipeline, "FUNDS_DIR", funds_dir),
                mock.patch.object(pipeline, "STATE_PATH", state_path),
                mock.patch.object(
                    pipeline, "LEGACY_STATE_PATH", root / "missing-state.json"
                ),
                mock.patch.object(pipeline, "load_cusip_map", return_value={}),
                mock.patch.object(
                    pipeline,
                    "retained_security_identity_migration_targets",
                    return_value=[target],
                ),
                mock.patch.object(
                    pipeline,
                    "_run_security_identity_replays",
                    side_effect=record_systemic_failure,
                ),
            ):
                self.assertFalse(
                    pipeline.repair_security_identity_migration(
                        rebuild_outputs=False
                    )
                )

            persisted = json.loads(state_path.read_text())
            self.assertNotEqual(
                pipeline.SECURITY_IDENTITY_VERSION,
                persisted.get("security_identity_migration_version"),
            )
            self.assertIn(
                pipeline.security_identity_migration_key(CIK, REPORT_DATE),
                persisted["security_identity_migration_pending"],
            )
            self.assertEqual(
                original_fund["quarters"],
                json.loads(fund_path.read_text())["quarters"],
            )

    def test_bounded_isolated_failure_may_withhold_after_one_resolution(
        self,
    ) -> None:
        successful_date = "2025-09-30"
        failed_target = {"cik": CIK, "report_date": REPORT_DATE}
        successful_target = {
            "cik": CIK,
            "report_date": successful_date,
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            funds_dir = root / "funds"
            funds_dir.mkdir()
            fund_path = funds_dir / f"{CIK}.json"
            payload = fund_payload()
            payload["quarters"].append(
                pipeline.compose_quarter_filings(
                    [current_component(successful_date)]
                )
            )
            fund_path.write_text(json.dumps(payload))
            state_path = root / "pipeline_state.json"
            state_path.write_text(json.dumps({
                "processed": [],
                "quarantined": {},
                "amendment_reducer_version": 2,
                "amendment_migration_pending": {},
            }))

            def record_isolated_failure(
                targets: list[dict],
                state: dict,
                _cusip_map: dict[str, str],
            ) -> tuple[bool, int]:
                pipeline._set_security_identity_pending(
                    state,
                    failed_target,
                    reason="discovery_failed",
                    message="isolated failure",
                )
                state["security_identity_migration_pending"].pop(
                    pipeline.security_identity_migration_key(
                        CIK, successful_date
                    )
                )
                return True, 1

            with (
                mock.patch.object(pipeline, "FUNDS_DIR", funds_dir),
                mock.patch.object(pipeline, "STATE_PATH", state_path),
                mock.patch.object(
                    pipeline, "LEGACY_STATE_PATH", root / "missing-state.json"
                ),
                mock.patch.object(pipeline, "load_cusip_map", return_value={}),
                mock.patch.object(
                    pipeline,
                    "retained_security_identity_migration_targets",
                    return_value=[failed_target, successful_target],
                ),
                mock.patch.object(
                    pipeline,
                    "_run_security_identity_replays",
                    side_effect=record_isolated_failure,
                ),
            ):
                self.assertTrue(
                    pipeline.repair_security_identity_migration(
                        rebuild_outputs=False
                    )
                )

            persisted = json.loads(state_path.read_text())
            self.assertEqual(
                pipeline.SECURITY_IDENTITY_VERSION,
                persisted["security_identity_migration_version"],
            )
            self.assertEqual(
                [successful_date],
                [
                    quarter["report_date"]
                    for quarter in json.loads(
                        fund_path.read_text()
                    )["quarters"]
                ],
            )

    def test_run_all_migrates_identity_then_defers_routine_ingest(
        self,
    ) -> None:
        before = {
            "processed": [],
            "_processed_set": set(),
            "amendment_reducer_version":
                pipeline.AMENDMENT_REDUCER_VERSION,
            "amendment_migration_pending": {},
            "security_identity_migration_version": 0,
        }
        after = {
            **before,
            "security_identity_migration_version":
                pipeline.SECURITY_IDENTITY_VERSION,
            "security_identity_migration_pending": {},
        }
        with (
            mock.patch.object(
                pipeline, "load_state", side_effect=[before, after]
            ),
            mock.patch.object(
                pipeline,
                "repair_security_identity_migration",
                return_value=True,
            ) as repair,
            mock.patch.object(
                pipeline, "get_recent_filing_quarters", return_value=[]
            ) as recent_quarters,
            mock.patch.object(pipeline, "save_state"),
        ):
            self.assertTrue(pipeline.run_all(
                4,
                rebuild_outputs=False,
                migrations_only=True,
            ))

        repair.assert_called_once_with(rebuild_outputs=False)
        recent_quarters.assert_not_called()

    def test_migrations_only_cli_defers_regeneration_to_workflow(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir) / "data"
            with (
                mock.patch(
                    "sys.argv",
                    [
                        "pipeline.py",
                        "--migrations-only",
                        "--quarters",
                        "4",
                        "--defer-regeneration",
                    ],
                ),
                mock.patch.multiple(
                    pipeline,
                    DATA_DIR=data_dir,
                    FUNDS_DIR=data_dir / "funds",
                    STOCKS_DIR=data_dir / "stocks",
                ),
                mock.patch.object(
                    pipeline,
                    "USER_AGENT",
                    "Super Investor Seeker ops@example.org",
                ),
                mock.patch.object(
                    pipeline,
                    "run_all",
                    return_value=True,
                ) as run,
            ):
                self.assertEqual(0, pipeline.main())

        run.assert_called_once_with(
            4,
            rebuild_outputs=False,
            migrations_only=True,
        )


class IdentityStateValidationTests(unittest.TestCase):
    def _state(self, pending: dict) -> dict:
        return {
            "processed": [],
            "quarantined": {},
            "amendment_reducer_version": 2,
            "amendment_migration_pending": {},
            "security_identity_migration_version": 1,
            "security_identity_migration_pending": pending,
        }

    def test_validator_rejects_pending_quarter_that_is_still_published(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            funds_dir = root / "funds"
            funds_dir.mkdir()
            fund_path = funds_dir / f"{CIK}.json"
            fund_path.write_text(json.dumps(fund_payload()))
            key = pipeline.security_identity_migration_key(
                CIK, REPORT_DATE
            )
            state_path = root / "pipeline_state.json"
            state_path.write_text(json.dumps(self._state({
                key: {
                    "cik": CIK,
                    "report_date": REPORT_DATE,
                    "reason": "replay_incomplete",
                    "message": "fixture",
                    "last_attempt_at": "2026-07-25T12:00:00Z",
                }
            })))
            errors: list[str] = []
            warnings: list[str] = []

            with mock.patch.object(
                validate_data, "STATE_PATH", state_path
            ):
                validate_data.validate_pipeline_state(
                    {str(CIK): fund_path},
                    errors,
                    warnings,
                )

        self.assertTrue(any(
            "remains published instead of fail-closed" in error
            for error in errors
        ))
        self.assertTrue(any(
            "unsafe legacy option identity row" in error
            for error in errors
        ))

    def test_validator_accepts_current_parser_identity_after_issuer_rewrite(
        self,
    ) -> None:
        component = current_component()
        component["holdings"] = [{
            **unsafe_holding(),
            "issuer": "CALL 100 META PLATFORMS INC",
            "class": "Listed Options",
            "holding_type": "CALL",
        }]
        proof_backed = pipeline.compose_quarter_filings([component])

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            funds_dir = root / "funds"
            funds_dir.mkdir()
            fund_path = funds_dir / f"{CIK}.json"
            fund_path.write_text(json.dumps({
                "cik": CIK,
                "name": "Example Manager",
                "quarters": [proof_backed],
            }))
            registry_path = root / "cusip_registry.json"
            registry_path.write_text(json.dumps({
                "123456789": {
                    "ticker": "META",
                    "name": "Meta Platforms, Inc.",
                    "type": "CALL",
                }
            }))
            state_path = root / "pipeline_state.json"
            state_path.write_text(json.dumps(self._state({})))
            errors: list[str] = []
            warnings: list[str] = []

            with mock.patch.multiple(
                pipeline,
                FUNDS_DIR=funds_dir,
                CUSIP_REGISTRY_PATH=registry_path,
                LEGACY_CUSIP_REGISTRY_PATH=registry_path,
            ):
                pipeline.canonicalize_fund_files()
                pipeline.canonicalize_fund_files()

            rebuilt = json.loads(fund_path.read_text())
            rebuilt_quarter = rebuilt["quarters"][0]
            rebuilt_holding = rebuilt_quarter["holdings"][0]
            self.assertEqual("Meta Platforms, Inc.", rebuilt_holding["issuer"])
            self.assertEqual("CALL", rebuilt_holding["holding_type"])
            self.assertTrue(
                pipeline._quarter_retains_raw_put_call(rebuilt_quarter)
            )
            self.assertTrue(
                pipeline.has_unsafe_legacy_option_identity(rebuilt_holding)
            )
            self.assertFalse(
                pipeline.quarter_has_unsafe_legacy_option_identity(
                    rebuilt_quarter
                )
            )

            with mock.patch.object(validate_data, "STATE_PATH", state_path):
                validate_data.validate_pipeline_state(
                    {str(CIK): fund_path},
                    errors,
                    warnings,
                )

        self.assertFalse(any(
            "unsafe legacy option identity row" in error
            for error in errors
        ))

    def test_validator_accepts_well_formed_withheld_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fund_path = root / f"{CIK}.json"
            fund_path.write_text(json.dumps({
                "cik": CIK,
                "name": "Example Manager",
                "quarters": [],
            }))
            key = pipeline.security_identity_migration_key(
                CIK, REPORT_DATE
            )
            state_path = root / "pipeline_state.json"
            state_path.write_text(json.dumps(self._state({
                key: {
                    "cik": CIK,
                    "report_date": REPORT_DATE,
                    "reason": "discovery_failed",
                    "message": "offline",
                    "last_attempt_at": "2026-07-25T12:00:00Z",
                }
            })))
            errors: list[str] = []
            warnings: list[str] = []

            with mock.patch.object(
                validate_data, "STATE_PATH", state_path
            ):
                validate_data.validate_pipeline_state(
                    {str(CIK): fund_path},
                    errors,
                    warnings,
                )

        self.assertEqual([], errors)
        self.assertTrue(any(
            "security identity migration target" in warning
            for warning in warnings
        ))


if __name__ == "__main__":
    unittest.main()
