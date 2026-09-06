from __future__ import annotations

import contextlib
import copy
import io
import json
import tempfile
import unittest
from pathlib import Path

import sec_security_master as master
from verify_security_master_reproducibility import (
    ReproducibilityVerificationError,
    main,
    verify_security_master_reproducibility,
)


class SecurityMasterReproducibilityTests(unittest.TestCase):
    def make_state(
        self,
        clock: str,
        *,
        source_sha256: str = "a" * 64,
        discovery_sha256: str = "b" * 64,
    ) -> dict:
        state = master.empty_source_state()
        state["updated_at"] = clock
        state["sources"][master.SEC_COMPANY_TICKERS_URL] = {
            "url": master.SEC_COMPANY_TICKERS_URL,
            "kind": "sec_company_tickers",
            "sha256": source_sha256,
            "accepted_at": clock,
            "last_successful_check_at": clock,
            "symbols": ["AAPL"],
            "symbol_titles": {"AAPL": ["Apple Inc."]},
            "symbol_exchanges": {"AAPL": ["Nasdaq"]},
            "symbol_count": 1,
        }
        state["edgar_evidence"] = {
            "schema_version": 1,
            "generated_at": clock,
            "sources": [],
            "schedule_evidence": [],
            "ixbrl_evidence": [],
            "records": {},
            "unresolved": {},
            "summary": {
                "source_count": 0,
                "schedule_record_count": 0,
                "ixbrl_record_count": 0,
                "resolved_count": 0,
                "unresolved_count": 0,
            },
        }
        state["edgar_discovery"] = {
            "schema_version": 2,
            "records": {
                "037833100": {
                    "cusip": "037833100",
                    "status": "no_evidence",
                    "terminal": True,
                    "reason": "no_exact_schedule_13dg_cusip_evidence",
                    "issuer_cik": None,
                    "security_class": None,
                    "schedule_candidate_count": 0,
                    "exact_schedule_count": 0,
                    "periodic_candidate_count": 0,
                    "source_accessions": [],
                    "record_sha256": discovery_sha256,
                    "checked_at": clock,
                    "last_successful_check_at": clock,
                }
            },
            "fetched_sources": {},
        }
        return state

    @staticmethod
    def universe(issuer: str = "APPLE INC") -> list[dict[str, str]]:
        return [
            {
                "cusip": "037833100",
                "instrument_type": "EQUITY",
                "reported_issuer": issuer,
                "reported_class": "COM",
            }
        ]

    def write_pair(
        self,
        root: Path,
        name: str,
        state: dict,
        *,
        issuer: str = "APPLE INC",
    ) -> tuple[Path, Path]:
        pair_root = root / name
        pair_root.mkdir()
        source_path = pair_root / "sec_source_state.json"
        master_path = pair_root / "sec_security_master.json"
        built = master.rebuild_security_master(state, self.universe(issuer))
        master.save_source_state(state, source_path)
        master.save_security_master(built, master_path)
        return master_path, source_path

    @staticmethod
    def verify(paths: tuple[Path, Path, Path, Path]) -> dict:
        return verify_security_master_reproducibility(
            first_master_path=paths[0],
            first_source_state_path=paths[1],
            second_master_path=paths[2],
            second_source_state_path=paths[3],
        )

    def test_independent_pairs_ignore_only_validated_fetch_clocks(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            first = self.write_pair(
                root,
                "first",
                self.make_state("2026-08-20T12:00:00Z"),
            )
            second = self.write_pair(
                root,
                "second",
                self.make_state("2026-08-21T13:14:15Z"),
            )

            summary = self.verify((*first, *second))

            self.assertTrue(summary["ok"])
            self.assertEqual(1, summary["record_count"])
            self.assertEqual(1, summary["source_count"])
            self.assertRegex(summary["normalized_master_sha256"], r"^[0-9a-f]{64}$")
            self.assertRegex(summary["sec_evidence_sha256"], r"^[0-9a-f]{64}$")

    def test_non_clock_sec_evidence_difference_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            first = self.write_pair(
                root,
                "first",
                self.make_state("2026-08-20T12:00:00Z"),
            )
            second_state = self.make_state(
                "2026-08-21T12:00:00Z",
                discovery_sha256="c" * 64,
            )
            second = self.write_pair(root, "second", second_state)

            with self.assertRaisesRegex(
                ReproducibilityVerificationError,
                "SEC input/evidence identity differs",
            ):
                self.verify((*first, *second))

    def test_source_checksum_difference_reports_both_projections(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            first = self.write_pair(
                root,
                "first",
                self.make_state("2026-08-20T12:00:00Z"),
            )
            second = self.write_pair(
                root,
                "second",
                self.make_state(
                    "2026-08-21T12:00:00Z",
                    source_sha256="d" * 64,
                ),
            )

            with self.assertRaises(ReproducibilityVerificationError) as raised:
                self.verify((*first, *second))
            self.assertIn("SEC input/evidence identity differs", str(raised.exception))
            self.assertIn("normalized security-master output differs", str(raised.exception))

    def test_master_difference_fails_when_sec_evidence_is_equal(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            first = self.write_pair(
                root,
                "first",
                self.make_state("2026-08-20T12:00:00Z"),
            )
            second = self.write_pair(
                root,
                "second",
                self.make_state("2026-08-21T12:00:00Z"),
                issuer="APPLE COMPUTER INC",
            )

            with self.assertRaisesRegex(
                ReproducibilityVerificationError,
                "normalized security-master output differs",
            ):
                self.verify((*first, *second))

    def test_each_master_must_bind_to_its_exact_companion_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            first = self.write_pair(
                root,
                "first",
                self.make_state("2026-08-20T12:00:00Z"),
            )
            second = self.write_pair(
                root,
                "second",
                self.make_state("2026-08-21T12:00:00Z"),
            )
            changed_state = master.load_source_state(second[1])
            changed_state["sources"][master.SEC_COMPANY_TICKERS_URL][
                "accepted_at"
            ] = "2026-08-21T12:00:01Z"
            master.save_source_state(changed_state, second[1])

            with self.assertRaisesRegex(
                ReproducibilityVerificationError,
                "not bound to its companion source state",
            ):
                self.verify((*first, *second))

    def test_invalid_value_in_ignored_clock_path_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            first = self.write_pair(
                root,
                "first",
                self.make_state("2026-08-20T12:00:00Z"),
            )
            second = self.write_pair(
                root,
                "second",
                self.make_state("2026-08-21T12:00:00Z"),
            )
            malformed = master.load_source_state(second[1])
            malformed["sources"][master.SEC_COMPANY_TICKERS_URL][
                "accepted_at"
            ] = "not-a-timestamp"
            second[1].write_text(json.dumps(malformed), encoding="utf-8")

            with self.assertRaisesRegex(
                master.SecurityMasterError,
                "invalid accepted_at",
            ):
                self.verify((*first, *second))

    def test_same_files_cannot_stand_in_for_two_builds(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            pair = self.write_pair(
                root,
                "only",
                self.make_state("2026-08-20T12:00:00Z"),
            )

            with self.assertRaisesRegex(
                ReproducibilityVerificationError,
                "all four inputs must be distinct",
            ):
                self.verify((*pair, *pair))

    def test_cli_prints_machine_readable_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            first = self.write_pair(
                root,
                "first",
                self.make_state("2026-08-20T12:00:00Z"),
            )
            second = self.write_pair(
                root,
                "second",
                self.make_state("2026-08-21T12:00:00Z"),
            )
            output = io.StringIO()

            with contextlib.redirect_stdout(output):
                status = main(
                    [
                        "--first-master",
                        str(first[0]),
                        "--first-source-state",
                        str(first[1]),
                        "--second-master",
                        str(second[0]),
                        "--second-source-state",
                        str(second[1]),
                    ]
                )

            self.assertEqual(0, status)
            self.assertTrue(json.loads(output.getvalue())["ok"])

    def test_malformed_pair_returns_cli_failure_without_traceback(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            first = self.write_pair(
                root,
                "first",
                self.make_state("2026-08-20T12:00:00Z"),
            )
            second = self.write_pair(
                root,
                "second",
                self.make_state("2026-08-21T12:00:00Z"),
            )
            malformed = copy.deepcopy(master.load_source_state(second[1]))
            malformed["schema_version"] = 999
            second[1].write_text(json.dumps(malformed), encoding="utf-8")
            errors = io.StringIO()

            with contextlib.redirect_stderr(errors):
                status = main(
                    [
                        "--first-master",
                        str(first[0]),
                        "--first-source-state",
                        str(first[1]),
                        "--second-master",
                        str(second[0]),
                        "--second-source-state",
                        str(second[1]),
                    ]
                )

            self.assertEqual(1, status)
            self.assertIn("verification failed", errors.getvalue())
            self.assertNotIn("Traceback", errors.getvalue())


if __name__ == "__main__":
    unittest.main()
