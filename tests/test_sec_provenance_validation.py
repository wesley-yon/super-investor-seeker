from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pipeline
import validate_data
from sec_edgar_evidence import FilingSource, apply_sec_edgar_evidence
from sec_security_master import RefreshResult, source_state_sha256


class SecProvenanceValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self._stage_tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._stage_tmp.cleanup)
        self._stage_patch = mock.patch.object(
            pipeline,
            "SEC_SECURITY_MASTER_REBUILD_WORK_ROOT",
            Path(self._stage_tmp.name) / "sec-security-master-rebuild-work",
        )
        self._stage_patch.start()
        self.addCleanup(self._stage_patch.stop)

    @staticmethod
    def safety_anchor_state() -> tuple[dict[str, dict], dict]:
        registry: dict[str, dict] = {}
        records: dict[str, dict] = {}
        for cusip, ticker in validate_data.SEC_EQUITY_SAFETY_ANCHORS.items():
            registry[cusip] = {
                "type": "EQUITY",
                "mapping_status": "resolved",
                "ticker": ticker,
                "ticker_source": "sec_ftd",
                "ticker_as_of": "2026-08-14",
            }
            records[f"{cusip}|EQUITY"] = {
                "cusip": cusip,
                "instrument_type": "EQUITY",
                "mapping_status": "resolved",
                "ticker": ticker,
                "ticker_source": "sec_ftd",
                "ticker_as_of": "2026-08-14",
            }
        for cusip in validate_data.SEC_TICKERLESS_NOTE_SAFETY_ANCHORS:
            registry[cusip] = {
                "type": "NOTE",
                "security_kind": "BOND",
                "mapping_status": "no_listed_symbol",
                "ticker": None,
                "ticker_source": None,
                "ticker_as_of": None,
            }
            records[f"{cusip}|NOTE"] = {
                "cusip": cusip,
                "instrument_type": "NOTE",
                "mapping_status": "no_listed_symbol",
                "ticker": None,
                "ticker_source": None,
                "ticker_as_of": None,
            }
        return registry, {"records": records}

    @staticmethod
    def write_private_state(root: Path) -> tuple[Path, Path]:
        master_path = root / "sec_security_master.json"
        source_path = root / "sec_source_state.json"
        ftd_url = (
            "https://www.sec.gov/files/data/fails-deliver-data/"
            "cnsfails202608a.zip"
        )
        ftd_sha256 = "a" * 64
        company_url = "https://www.sec.gov/files/company_tickers.json"
        company_sha256 = "b" * 64
        source_state = {
            "schema_version": 2,
            "updated_at": "2026-08-31T00:00:00Z",
            "current_filter_universe_sha256": None,
            "current_filter_universe_count": 0,
            "filter_universes": {},
            "required_filter_coverage_urls": [],
            "edgar_evidence": {},
            "edgar_discovery": {},
            "sources": {
                ftd_url: {
                    "url": ftd_url,
                    "kind": "sec_ftd_archive",
                    "sha256": ftd_sha256,
                    "accepted_at": "2026-08-31T00:00:00Z",
                    "records": [{
                        "record_schema_version": 1,
                        "cusip": "037833100",
                        "symbol": "AAPL",
                        "description": "APPLE INC",
                        "first_settlement_date": "2026-08-13",
                        "last_settlement_date": "2026-08-14",
                        "observation_dates": ["2026-08-13", "2026-08-14"],
                        "distinct_settlement_date_count": 2,
                        "row_count": 2,
                    }],
                    "record_count": 1,
                    "raw_record_count": 2,
                    "filter_all_cusips": True,
                },
                company_url: {
                    "url": company_url,
                    "kind": "sec_company_tickers",
                    "sha256": company_sha256,
                    "accepted_at": "2026-08-31T00:00:00Z",
                    "symbols": ["AAPL"],
                    "symbol_titles": {"AAPL": ["Apple Inc."]},
                    "symbol_exchanges": {"AAPL": ["Nasdaq"]},
                    "symbol_count": 1,
                },
            },
        }
        source_path.write_text(json.dumps(source_state), encoding="utf-8")
        master_path.write_text(json.dumps({
            "schema_version": 1,
            "source_state_schema_version": 2,
            "generated_at": "2026-08-31T00:00:00Z",
            "source_state_sha256": source_state_sha256(source_state),
            "universe_sha256": "1" * 64,
            "policy": {"min_confirmation_dates": 2},
            "sources": [
                {
                    "url": company_url,
                    "sha256": company_sha256,
                    "kind": "sec_company_tickers",
                    "schema_sha256": "c" * 64,
                },
                {
                    "url": ftd_url,
                    "sha256": ftd_sha256,
                    "kind": "sec_ftd_archive",
                    "schema_sha256": "d" * 64,
                },
            ],
            "audit": {
                "schema_version": 1,
                "as_of": "2026-08-31",
                "official_13f_period": "2026Q2",
                "active_non_option_official_cusip_count": 2,
                "active_non_option_official_cusips_sha256": "2" * 64,
                "malformed_active_official_cusip_count": 0,
                "ftd_evidenced_official_cusip_count": 2,
                "ftd_coverage_ratio": 1.0,
                "latest_ftd_settlement_date": "2026-08-14",
                "ftd_source_age_days": 17,
                "source_staleness_threshold_days": 45,
                "source_stale": False,
                "source_schema_sha256_by_kind": {},
            },
            "records": {
                "037833100|EQUITY": {
                    "cusip": "037833100",
                    "instrument_type": "EQUITY",
                    "mapping_status": "resolved",
                    "ticker": "AAPL",
                    "ticker_source": "sec_ftd",
                    "ticker_as_of": "2026-08-14",
                    "last_verification_date": "2026-08-14",
                    "mapping_method": (
                        "exact_ftd_symbol_with_sec_metadata_validation"
                    ),
                    "effective_from": "2026-08-13",
                    "effective_to": None,
                    "candidate_ticker": "AAPL",
                    "candidate_as_of": "2026-08-14",
                    "confirmation_dates": ["2026-08-13", "2026-08-14"],
                    "symbol_validation_sources": ["sec_company_tickers"],
                    "symbol_validation_titles": ["Apple Inc."],
                    "symbol_validation_exchanges": ["Nasdaq"],
                    "resolution_reason": "recent_repeated_ftd_symbol",
                    "symbol_evidence": [
                        {
                            "settlement_date": observed_at,
                            "symbol": "AAPL",
                            "observation_count": 1,
                            "descriptions": ["APPLE INC"],
                            "sources": [{
                                "url": ftd_url,
                                "sha256": ftd_sha256,
                            }],
                        }
                        for observed_at in ("2026-08-13", "2026-08-14")
                    ],
                    "symbol_intervals": [{
                        "symbol": "AAPL",
                        "first_seen": "2026-08-13",
                        "last_seen": "2026-08-14",
                        "observation_dates": ["2026-08-13", "2026-08-14"],
                        "observation_date_count": 2,
                        "observation_count": 2,
                        "sources": [{"url": ftd_url, "sha256": ftd_sha256}],
                        "descriptions": ["APPLE INC"],
                    }],
                },
                "378331004|EQUITY": {
                    "cusip": "378331004",
                    "instrument_type": "EQUITY",
                    "mapping_status": "malformed_as_filed",
                    "ticker": None,
                    "ticker_source": None,
                    "ticker_as_of": None,
                    "resolution_reason": "check_digit_mismatch",
                    "symbol_evidence": [],
                },
            },
            "quarantine": {
                "378331004|EQUITY": {
                    "cusip": "378331004",
                    "instrument_type": "EQUITY",
                    "reason": "check_digit_mismatch",
                },
            },
            "summary": {
                "ambiguous": 0,
                "malformed_as_filed": 1,
                "no_listed_symbol": 0,
                "resolved": 1,
                "unresolved": 0,
            },
        }), encoding="utf-8")
        return master_path, source_path

    @classmethod
    def write_edgar_private_state(cls, root: Path) -> tuple[Path, Path]:
        """Add one internally consistent Schedule 13G/iXBRL bridge fixture."""

        master_path, source_path = cls.write_private_state(root)
        schedule_url = (
            "https://www.sec.gov/Archives/edgar/data/123/"
            "000000012326000001/primary_doc.xml"
        )
        ixbrl_url = (
            "https://www.sec.gov/Archives/edgar/data/1652044/"
            "000165204426000002/goog-20260630.htm"
        )
        schedule_accession = "0000000123-26-000001"
        ixbrl_accession = "0001652044-26-000002"
        schedule_record = {
            "accession": schedule_accession,
            "as_of": "2026-06-30",
            "cusip": "02079K305",
            "filing_type": "SCHEDULE 13G",
            "issuer_cik": "0001652044",
            "issuer_name": "Alphabet Inc.",
            "kind": "schedule_13dg",
            "security_class": "Class A Common Stock",
            "security_class_key": "class a common stock",
            "url": schedule_url,
        }
        ixbrl_record = {
            "accession": ixbrl_accession,
            "as_of": "2026-06-30",
            "context_id": "class-a",
            "dimensions": [],
            "exchange": "NASDAQ",
            "filing_type": "10-K",
            "issuer_cik": "0001652044",
            "kind": "periodic_ixbrl",
            "registration": "12b",
            "security_class": "Class A Common Stock",
            "security_class_key": "class a common stock",
            "ticker": "GOOGL",
            "url": ixbrl_url,
        }
        edgar_cache = {
            "schema_version": 1,
            "generated_at": "2026-07-01T00:00:00Z",
            "sources": [
                {
                    "accession": ixbrl_accession,
                    "as_of": "2026-06-30",
                    "kind": "periodic_ixbrl",
                    "record_count": 1,
                    "sha256": "c" * 64,
                    "url": ixbrl_url,
                },
                {
                    "accession": schedule_accession,
                    "as_of": "2026-06-30",
                    "kind": "schedule_13dg",
                    "record_count": 1,
                    "sha256": "d" * 64,
                    "url": schedule_url,
                },
            ],
            "schedule_evidence": [schedule_record],
            "ixbrl_evidence": [ixbrl_record],
            "records": {
                "02079K305": {
                    "cusip": "02079K305",
                    "cusip_source": "sec_schedule_13dg",
                    "exchange": "NASDAQ",
                    "exchanges": ["NASDAQ"],
                    "issuer_cik": "0001652044",
                    "issuer_name": "Alphabet Inc.",
                    "ixbrl_accession": ixbrl_accession,
                    "ixbrl_as_of": "2026-06-30",
                    "ixbrl_context_ids": ["class-a"],
                    "ixbrl_url": ixbrl_url,
                    "mapping_status": "resolved",
                    "schedule_13dg_accession": schedule_accession,
                    "schedule_13dg_as_of": "2026-06-30",
                    "schedule_13dg_url": schedule_url,
                    "security_class": "Class A Common Stock",
                    "ticker": "GOOGL",
                    "ticker_as_of": "2026-06-30",
                    "ticker_source": "sec_ixbrl",
                }
            },
            "unresolved": {},
            "summary": {
                "source_count": 2,
                "schedule_record_count": 1,
                "ixbrl_record_count": 1,
                "resolved_count": 1,
                "unresolved_count": 0,
            },
        }
        source_state = json.loads(source_path.read_text(encoding="utf-8"))
        source_state["edgar_evidence"] = edgar_cache
        master = json.loads(master_path.read_text(encoding="utf-8"))
        master["records"]["02079K305|EQUITY"] = {
            "cusip": "02079K305",
            "instrument_type": "EQUITY",
            "issuer": "Alphabet Inc.",
            "security_class": "Class A Common Stock",
            "reported_issuer": "Alphabet Inc.",
            "reported_issuers": ["Alphabet Inc."],
            "reported_class": "Class A Common Stock",
            "reported_classes": ["Class A Common Stock"],
            "mapping_status": "unresolved",
            "ticker": None,
            "ticker_source": None,
            "ticker_as_of": None,
            "last_verification_date": None,
            "resolution_reason": "no_ftd_symbol_evidence",
            "symbol_evidence": [],
        }
        master["summary"]["unresolved"] += 1
        master = apply_sec_edgar_evidence(master, edgar_cache)
        master["source_state_sha256"] = source_state_sha256(source_state)
        source_path.write_text(json.dumps(source_state), encoding="utf-8")
        master_path.write_text(json.dumps(master), encoding="utf-8")
        return master_path, source_path

    def test_accepts_every_sec_ticker_source_and_fail_closed_status(self) -> None:
        registry = {
            f"0000000{index:02d}": {
                "mapping_status": "resolved",
                "ticker": f"T{index}",
                "ticker_source": source,
                "ticker_as_of": "2026-08-14",
            }
            for index, source in enumerate(
                sorted(validate_data.SEC_TICKER_SOURCES),
                start=1,
            )
        }
        for index, status in enumerate(
            sorted(validate_data.SEC_MAPPING_STATUSES - {"resolved"}),
            start=20,
        ):
            registry[f"0000000{index:02d}"] = {
                "mapping_status": status,
                "ticker": None,
                "ticker_source": None,
                "ticker_as_of": None,
            }

        errors: list[str] = []
        validate_data.validate_sec_mapping_provenance(registry, errors)

        self.assertEqual([], errors)

    def test_rejects_non_sec_or_noncanonical_mapping_provenance(self) -> None:
        registry = {
            "000000001": {
                "mapping_status": "resolved",
                "ticker": "lower",
                "ticker_source": "unverified_feed",
                "ticker_as_of": "2026-08-14T00:00:00Z",
            },
            "000000002": {
                "mapping_status": "unresolved",
                "ticker": "LEAK",
                "ticker_source": "sec_ftd",
                "ticker_as_of": "2026-08-14",
            },
            "000000003": {
                "mapping_status": "quarantined",
                "ticker": None,
                "ticker_source": None,
                "ticker_as_of": None,
            },
            "000000004": {
                "mapping_status": "unresolved",
            },
        }

        errors: list[str] = []
        validate_data.validate_sec_mapping_provenance(registry, errors)

        self.assertEqual(3, len(errors))
        self.assertTrue(any("resolved mappings" in error for error in errors))
        self.assertTrue(any("non-resolved mappings" in error for error in errors))
        self.assertTrue(any("invalid mapping_status" in error for error in errors))

    def test_public_registry_provenance_is_sec_filer_or_synthetic_only(
        self,
    ) -> None:
        registry = {
            f"label-{index}": {
                "label_source": label_source,
                "sources": ["sec_13f_filer_consensus"],
            }
            for index, label_source in enumerate(
                sorted(validate_data.PUBLIC_REGISTRY_LABEL_SOURCES),
                start=1,
            )
        }
        registry.update({
            f"source-{index}": {
                "label_source": "synthetic_identifier",
                "sources": [source],
            }
            for index, source in enumerate(
                sorted(validate_data.PUBLIC_REGISTRY_EVIDENCE_SOURCES),
                start=1,
            )
        })
        errors: list[str] = []

        validate_data.validate_public_registry_provenance(registry, errors)

        self.assertEqual([], errors)

        registry.update({
            "unverified-label": {
                "label_source": "unverified_source",
                "sources": ["sec_ftd"],
            },
            "unverified-source": {
                "label_source": "sec_ftd",
                "sources": ["unverified_source"],
            },
            "malformed-sources": {
                "label_source": "sec_13f_list",
                "sources": "sec_13f_list",
            },
        })
        errors.clear()

        validate_data.validate_public_registry_provenance(registry, errors)

        self.assertEqual(2, len(errors))
        self.assertTrue(any("security labels" in error for error in errors))
        self.assertTrue(any("entries with sources" in error for error in errors))
        self.assertTrue(any("unverified-label" in error for error in errors))
        self.assertTrue(any("unverified-source" in error for error in errors))
        self.assertTrue(any("malformed-sources" in error for error in errors))

    def test_sec_safety_anchors_accept_exact_registry_and_master_state(
        self,
    ) -> None:
        registry, master = self.safety_anchor_state()
        errors: list[str] = []

        validate_data.validate_sec_safety_anchors(registry, master, errors)

        self.assertEqual([], errors)

    def test_sec_safety_anchors_reject_equity_drift_on_either_side(
        self,
    ) -> None:
        mutations = (
            (
                "missing-public-aapl",
                lambda registry, master: registry.pop("037833100"),
                "037833100",
            ),
            (
                "wrong-private-xom",
                lambda registry, master: master["records"][
                    "30231G102|EQUITY"
                ].update({"ticker": "CVX"}),
                "30231G102",
            ),
            (
                "unapproved-public-rivn-source",
                lambda registry, master: registry["76954A103"].update(
                    {"ticker_source": "sec_company_tickers"}
                ),
                "76954A103",
            ),
        )
        for name, mutate, expected_cusip in mutations:
            with self.subTest(name=name):
                registry, master = self.safety_anchor_state()
                mutate(registry, master)
                errors: list[str] = []

                validate_data.validate_sec_safety_anchors(
                    registry,
                    master,
                    errors,
                )

                self.assertEqual(1, len(errors), errors)
                self.assertIn(expected_cusip, errors[0])
                self.assertIn("safety-case equity", errors[0])

    def test_sec_safety_anchors_reject_note_common_stock_inheritance(
        self,
    ) -> None:
        for cusip in sorted(validate_data.SEC_TICKERLESS_NOTE_SAFETY_ANCHORS):
            with self.subTest(cusip=cusip):
                registry, master = self.safety_anchor_state()
                registry[cusip].update({
                    "mapping_status": "resolved",
                    "ticker": "COMMON",
                    "ticker_source": "sec_ftd",
                    "ticker_as_of": "2026-08-14",
                })
                errors: list[str] = []

                validate_data.validate_sec_safety_anchors(
                    registry,
                    master,
                    errors,
                )

                self.assertEqual(1, len(errors), errors)
                self.assertIn(cusip, errors[0])
                self.assertIn("tickerless NOTE/BOND", errors[0])

    def test_sec_safety_anchors_reject_note_underlying_ticker_escape(
        self,
    ) -> None:
        registry, master = self.safety_anchor_state()
        registry["76954AAD5"].update({
            "underlying_ticker": "RIVN",
            "underlying_ticker_source": "sec_ftd",
            "underlying_ticker_as_of": "2026-08-14",
        })
        errors: list[str] = []

        validate_data.validate_sec_safety_anchors(registry, master, errors)

        self.assertEqual(1, len(errors), errors)
        self.assertIn("76954AAD5", errors[0])
        self.assertIn("tickerless NOTE/BOND", errors[0])

    def test_sec_safety_anchors_reject_resolved_note_in_private_master(
        self,
    ) -> None:
        registry, master = self.safety_anchor_state()
        master["records"]["090043AF7|NOTE"].update({
            "mapping_status": "resolved",
            "ticker": "BILL",
            "ticker_source": "sec_ftd",
            "ticker_as_of": "2026-08-14",
        })
        errors: list[str] = []

        validate_data.validate_sec_safety_anchors(registry, master, errors)

        self.assertEqual(1, len(errors), errors)
        self.assertIn("090043AF7", errors[0])
        self.assertIn("tickerless NOTE/BOND", errors[0])

    def test_private_cache_and_public_registry_must_match_exactly(self) -> None:
        registry = {
            "037833100": {
                "type": "EQUITY",
                "mapping_status": "resolved",
                "ticker": "AAPL",
                "ticker_source": "sec_ftd",
                "ticker_as_of": "2026-08-14",
            },
            "378331004": {
                "type": "EQUITY",
                "mapping_status": "malformed_as_filed",
                "ticker": None,
                "ticker_source": None,
                "ticker_as_of": None,
            },
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            master_path, source_path = self.write_private_state(Path(tmpdir))
            errors: list[str] = []
            validate_data.validate_private_sec_security_state(
                registry,
                errors,
                master_path=master_path,
                source_state_path=source_path,
                enforce_production_source_gates=False,
            )
            self.assertEqual([], errors)

            registry["037833100"]["ticker_as_of"] = "2026-08-13"
            registry["378331004"]["candidate_ticker"] = "PRIVATE"
            registry["594918104"] = {
                "type": "EQUITY",
                "mapping_status": "unresolved",
                "ticker": None,
                "ticker_source": None,
                "ticker_as_of": None,
            }
            errors.clear()
            validate_data.validate_private_sec_security_state(
                registry,
                errors,
                master_path=master_path,
                source_state_path=source_path,
                enforce_production_source_gates=False,
            )
            self.assertEqual(3, len(errors))
            self.assertTrue(any("absent" in error for error in errors))
            self.assertTrue(any("differ" in error for error in errors))
            self.assertTrue(any("private SEC evidence" in error for error in errors))

    def test_option_underlying_requires_exact_equity_master_provenance(
        self,
    ) -> None:
        registry = {
            "037833100": {
                "type": "EQUITY",
                "mapping_status": "resolved",
                "ticker": "AAPL",
                "ticker_source": "sec_ftd",
                "ticker_as_of": "2026-08-14",
                "underlying_ticker": "AAPL",
                "underlying_ticker_source": "sec_ftd",
                "underlying_ticker_as_of": "2026-08-14",
            }
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            master_path, source_path = self.write_private_state(Path(tmpdir))
            errors: list[str] = []
            validate_data.validate_private_sec_security_state(
                registry,
                errors,
                master_path=master_path,
                source_state_path=source_path,
                enforce_production_source_gates=False,
            )
            self.assertEqual([], errors)

            registry["037833100"]["underlying_ticker_source"] = (
                "unverified_source"
            )
            errors.clear()
            validate_data.validate_private_sec_security_state(
                registry,
                errors,
                master_path=master_path,
                source_state_path=source_path,
                enforce_production_source_gates=False,
            )

        self.assertTrue(
            any("option-underlying mappings" in error for error in errors),
            errors,
        )

    def test_private_validation_enforces_production_source_floors_by_default(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            master_path, source_path = self.write_private_state(Path(tmpdir))
            errors: list[str] = []
            validate_data.validate_private_sec_security_state(
                {},
                errors,
                master_path=master_path,
                source_state_path=source_path,
            )

        self.assertTrue(
            any(
                "current_symbol_source_population_regressed" in error
                and "official_13f_population_regressed" in error
                for error in errors
            ),
            errors,
        )
        self.assertTrue(
            any("current production schemas" in error for error in errors),
            errors,
        )
        self.assertTrue(
            any("audit claims do not match" in error for error in errors),
            errors,
        )
        self.assertTrue(
            any("required SEC safety-case equity" in error for error in errors),
            errors,
        )

    def test_production_validation_rejects_raw_legacy_source_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            master_path, source_path = self.write_private_state(Path(tmpdir))
            normalized_state = pipeline.load_source_state(source_path)
            current_master = pipeline.rebuild_sec_security_master(
                normalized_state,
                [{
                    "cusip": "037833100",
                    "instrument_type": "EQUITY",
                }],
            )
            pipeline.save_security_master(current_master, master_path)

            errors: list[str] = []
            validate_data.validate_private_sec_security_state(
                {},
                errors,
                master_path=master_path,
                source_state_path=source_path,
            )

        schema_errors = [
            error for error in errors if "current production schemas" in error
        ]
        self.assertEqual(1, len(schema_errors), errors)
        self.assertIn("source state", schema_errors[0])
        self.assertNotIn("security master", schema_errors[0])
        self.assertNotIn("master audit", schema_errors[0])

    def test_private_cache_files_are_required(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            errors: list[str] = []
            validate_data.validate_private_sec_security_state(
                {},
                errors,
                master_path=root / "missing-master.json",
                source_state_path=root / "missing-state.json",
                enforce_production_source_gates=False,
            )
            self.assertEqual(2, len(errors))
            self.assertTrue(all("missing or invalid" in error for error in errors))

    def test_private_master_must_match_exact_source_state_digest(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            master_path, source_path = self.write_private_state(Path(tmpdir))
            changed = json.loads(source_path.read_text(encoding="utf-8"))
            changed["updated_at"] = "2026-08-30T00:00:00Z"
            source_path.write_text(json.dumps(changed), encoding="utf-8")

            errors: list[str] = []
            validate_data.validate_private_sec_security_state(
                {},
                errors,
                master_path=master_path,
                source_state_path=source_path,
                enforce_production_source_gates=False,
            )

            self.assertTrue(
                any(
                    "not bound to the current source-state digest" in error
                    for error in errors
                ),
                errors,
            )

    def test_private_master_source_checksums_are_bound_to_source_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            master_path, source_path = self.write_private_state(Path(tmpdir))
            payload = json.loads(master_path.read_text(encoding="utf-8"))
            company_source = next(
                source
                for source in payload["sources"]
                if source["kind"] == "sec_company_tickers"
            )
            company_source["sha256"] = "f" * 64
            master_path.write_text(json.dumps(payload), encoding="utf-8")

            errors: list[str] = []
            validate_data.validate_private_sec_security_state(
                {},
                errors,
                master_path=master_path,
                source_state_path=source_path,
                enforce_production_source_gates=False,
            )

            self.assertTrue(
                any(
                    "source checksums do not match" in error
                    for error in errors
                ),
                errors,
            )

    def test_private_master_cannot_borrow_real_ftd_hash_for_forged_cusip(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            master_path, source_path = self.write_private_state(Path(tmpdir))
            payload = json.loads(master_path.read_text(encoding="utf-8"))
            forged = dict(payload["records"]["037833100|EQUITY"])
            forged["cusip"] = "594918104"
            payload["records"]["594918104|EQUITY"] = forged
            payload["summary"]["resolved"] += 1
            master_path.write_text(json.dumps(payload), encoding="utf-8")

            errors: list[str] = []
            validate_data.validate_private_sec_security_state(
                {},
                errors,
                master_path=master_path,
                source_state_path=source_path,
                enforce_production_source_gates=False,
            )

            self.assertTrue(
                any("FTD records do not match" in error for error in errors),
                errors,
            )

    def test_private_master_cannot_forge_sec_validation_title(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            master_path, source_path = self.write_private_state(Path(tmpdir))
            payload = json.loads(master_path.read_text(encoding="utf-8"))
            payload["records"]["037833100|EQUITY"][
                "symbol_validation_titles"
            ] = ["Invented Compatible Issuer"]
            master_path.write_text(json.dumps(payload), encoding="utf-8")

            errors: list[str] = []
            validate_data.validate_private_sec_security_state(
                {},
                errors,
                master_path=master_path,
                source_state_path=source_path,
                enforce_production_source_gates=False,
            )

            self.assertTrue(
                any(
                    "resolved FTD proof conflicts" in error
                    or "symbol validation metadata" in error
                    for error in errors
                ),
                errors,
            )

    def test_private_master_edgar_proof_matches_cached_cik_and_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            master_path, source_path = self.write_edgar_private_state(
                Path(tmpdir)
            )
            baseline_errors: list[str] = []
            validate_data.validate_private_sec_security_state(
                {},
                baseline_errors,
                master_path=master_path,
                source_state_path=source_path,
                enforce_production_source_gates=False,
            )
            self.assertEqual([], baseline_errors)

            for field, forged_value in (
                ("issuer_cik", "9999999999"),
                ("context_ids", ["invented-context"]),
            ):
                with self.subTest(field=field):
                    master = json.loads(master_path.read_text(encoding="utf-8"))
                    proof = master["records"]["02079K305|EQUITY"][
                        "sec_edgar_evidence"
                    ]
                    if field == "issuer_cik":
                        proof[field] = forged_value
                    else:
                        proof["ixbrl"][field] = forged_value
                    forged_path = Path(tmpdir) / f"forged-{field}.json"
                    forged_path.write_text(json.dumps(master), encoding="utf-8")
                    errors: list[str] = []
                    validate_data.validate_private_sec_security_state(
                        {},
                        errors,
                        master_path=forged_path,
                        source_state_path=source_path,
                        enforce_production_source_gates=False,
                    )
                    self.assertTrue(
                        any("iXBRL records do not match" in error for error in errors),
                        errors,
                    )

    def test_registry_keeps_only_structural_non_ticker_sanity_checks(self) -> None:
        registry = {
            "000000001": {
                "type": "EQUITY",
                "mapping_status": "resolved",
                "ticker": "AAA",
                "ticker_source": "sec_ftd",
                "ticker_as_of": "2026-08-14",
                "security_kind": "BOND",
            },
            "000000002": {
                "type": "NOTE",
                "mapping_status": "no_listed_symbol",
                "ticker": None,
                "ticker_source": None,
                "ticker_as_of": None,
                "security_kind": "ETF",
            },
            "000000003": {
                "type": "EQUITY",
                "mapping_status": "resolved",
                "ticker": "IBB",
                "ticker_source": "sec_ftd",
                "ticker_as_of": "2026-08-14",
                "name": "ISHARES TR",
                "dominant_issuer": "ISHARES TR",
            },
        }

        errors: list[str] = []
        validate_data.validate_registry(
            set(registry) | {"000000004"},
            errors,
            registry,
        )

        self.assertTrue(any("missing 1 fund CUSIPs" in error for error in errors))
        self.assertTrue(any("bonds as non-NOTE" in error for error in errors))
        self.assertTrue(any("listed funds as non-EQUITY" in error for error in errors))
        self.assertTrue(any("deterministic filer fund kinds" in error for error in errors))

    def test_registry_requires_every_named_safety_case_to_exist(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            snapshot_path = Path(tmpdir) / "cusip_registry.json"
            snapshot_path.write_text("{}\n", encoding="utf-8")
            with (
                mock.patch.object(
                    pipeline,
                    "load_cusip_registry",
                    return_value={},
                ),
                mock.patch.object(
                    pipeline,
                    "load_security_master",
                    return_value={"records": {}},
                ),
                mock.patch.object(
                    pipeline,
                    "LEGACY_CUSIP_REGISTRY_PATH",
                    snapshot_path,
                ),
            ):
                issues = pipeline.validate_cusip_registry(current_cusips=set())

        for cusip in (
            "037833100",
            "30231G102",
            "76954A103",
            "76954AAD5",
            "090043AF7",
            "26210CAC8",
            "26210CAD6",
        ):
            self.assertTrue(
                any(cusip in issue and "missing" in issue for issue in issues),
                (cusip, issues),
            )

    def test_terminal_edgar_result_retries_only_after_evidence_change(
        self,
    ) -> None:
        master = {
            "records": {
                "111111118|EQUITY": {
                    "cusip": "111111118",
                    "instrument_type": "EQUITY",
                    "mapping_status": "unresolved",
                    "resolution_reason": "no_ftd_symbol_evidence",
                    "official_13f_status": "active",
                    "reported_issuer": "EXAMPLE INC",
                    "reported_class": "COM",
                },
                "222222226|NOTE": {
                    "cusip": "222222226",
                    "instrument_type": "NOTE",
                    "mapping_status": "unresolved",
                },
            }
        }
        candidates, fingerprints = pipeline._sec_edgar_discovery_candidates(
            master,
            {},
            as_of=datetime(2026, 9, 1, tzinfo=timezone.utc),
        )
        self.assertEqual(["111111118"], candidates)
        state = {
            "edgar_discovery": {
                "records": {
                    "111111118": {
                        "record_sha256": fingerprints["111111118"],
                        "terminal": True,
                        "status": "no_evidence",
                        "checked_at": "2026-08-31T00:00:00Z",
                    }
                }
            }
        }
        candidates, _ = pipeline._sec_edgar_discovery_candidates(
            master,
            state,
            as_of=datetime(2026, 9, 1, tzinfo=timezone.utc),
        )
        self.assertEqual([], candidates)

        candidates, _ = pipeline._sec_edgar_discovery_candidates(
            master,
            state,
            as_of=datetime(2026, 12, 1, tzinfo=timezone.utc),
        )
        self.assertEqual([], candidates)

        master["records"]["111111118|EQUITY"]["resolution_reason"] = (
            "conflicting_recent_ftd_symbols"
        )
        candidates, _ = pipeline._sec_edgar_discovery_candidates(
            master,
            state,
            as_of=datetime(2026, 9, 1, tzinfo=timezone.utc),
        )
        self.assertEqual(["111111118"], candidates)

        state["edgar_discovery"]["records"]["111111118"]["terminal"] = False
        candidates, _ = pipeline._sec_edgar_discovery_candidates(
            master,
            state,
            as_of=datetime(2026, 9, 1, tzinfo=timezone.utc),
        )
        self.assertEqual(["111111118"], candidates)

    def test_edgar_queue_excludes_permanent_historical_corpus_gaps(self) -> None:
        def unresolved(cusip: str, **fields) -> dict:
            return {
                "cusip": cusip,
                "instrument_type": "EQUITY",
                "mapping_status": "unresolved",
                "resolution_reason": "no_ftd_symbol_evidence",
                **fields,
            }

        master = {
            "policy": {"recent_window_days": 31},
            "audit": {"latest_ftd_settlement_date": "2026-08-29"},
            "records": {
                "111111118|EQUITY": unresolved(
                    "111111118",
                    official_13f_status="active",
                ),
                "222222226|EQUITY": unresolved(
                    "222222226",
                    reported_identity_evidence=[{"report_date": "2020-06-30"}],
                ),
                "333333334|EQUITY": unresolved(
                    "333333334",
                    reported_identity_evidence=[{"report_date": "2026-06-30"}],
                ),
                "444444442|EQUITY": unresolved(
                    "444444442",
                    candidate_as_of="2026-08-20",
                ),
                "555555550|EQUITY": unresolved(
                    "555555550",
                    candidate_as_of="2025-08-20",
                ),
                "666666667|EQUITY": {
                    **unresolved(
                        "666666667",
                        official_13f_status="active",
                    ),
                    "mapping_status": "ambiguous",
                    "resolution_reason": "conflicting_recent_ftd_symbols",
                },
                "777777774|EQUITY": {
                    **unresolved(
                        "777777774",
                        candidate_as_of="2026-08-21",
                    ),
                    "mapping_status": "ambiguous",
                    "resolution_reason": "conflicting_recent_ftd_symbols",
                },
            },
        }

        candidates, fingerprints = pipeline._sec_edgar_discovery_candidates(
            master,
            {},
            as_of=datetime(2026, 9, 1, tzinfo=timezone.utc),
        )

        self.assertEqual(
            [
                "666666667",
                "111111118",
                "777777774",
                "444444442",
                "333333334",
            ],
            candidates,
        )
        # Fingerprints cover every structurally eligible record so a caller can
        # still bind diagnostics precisely; queue admission is the bounded step.
        self.assertEqual(7, len(fingerprints))

    def test_quarterly_list_container_churn_does_not_reopen_terminal_result(
        self,
    ) -> None:
        exact_row = {
            "cusip": "111111118",
            "option_indicator": "",
            "issuer": "EXAMPLE INC",
            "description": "COM",
            "status": "",
        }
        record = {
            "cusip": "111111118",
            "instrument_type": "EQUITY",
            "mapping_status": "unresolved",
            "resolution_reason": "no_ftd_symbol_evidence",
            "official_13f_status": "active",
            "official_13f_as_of": "2026Q2",
            "official_13f": {
                "url": "https://www.sec.gov/files/investment/13flist2026q2.pdf",
                "sha256": "a" * 64,
                "period": "2026Q2",
                "status": "active",
                "records": [exact_row],
            },
        }
        master = {"records": {"111111118|EQUITY": record}}
        _candidates, fingerprints = pipeline._sec_edgar_discovery_candidates(
            master,
            {},
            as_of=datetime(2026, 9, 1, tzinfo=timezone.utc),
        )
        state = {
            "edgar_discovery": {
                "records": {
                    "111111118": {
                        "record_sha256": fingerprints["111111118"],
                        "terminal": True,
                        "status": "no_evidence",
                        "checked_at": "2026-08-31T00:00:00Z",
                    }
                }
            }
        }

        record["official_13f_as_of"] = "2026Q3"
        record["official_13f"].update({
            "url": "https://www.sec.gov/files/investment/13flist2026q3.pdf",
            "sha256": "b" * 64,
            "period": "2026Q3",
        })
        candidates, refreshed = pipeline._sec_edgar_discovery_candidates(
            master,
            state,
            as_of=datetime(2026, 9, 1, tzinfo=timezone.utc),
        )
        self.assertEqual([], candidates)
        self.assertEqual(fingerprints, refreshed)

        record["official_13f"]["records"][0]["issuer"] = "EXAMPLE HOLDINGS"
        candidates, _ = pipeline._sec_edgar_discovery_candidates(
            master,
            state,
            as_of=datetime(2026, 9, 1, tzinfo=timezone.utc),
        )
        self.assertEqual(["111111118"], candidates)

    def test_edgar_queue_cap_drains_terminal_results_deterministically(self) -> None:
        records = {
            f"{cusip}|EQUITY": {
                "cusip": cusip,
                "instrument_type": "EQUITY",
                "mapping_status": "unresolved",
                "resolution_reason": "no_ftd_symbol_evidence",
                "official_13f_status": "active",
            }
            for cusip in ("111111118", "222222226", "333333334")
        }
        master = {"records": records}
        candidates, fingerprints = pipeline._sec_edgar_discovery_candidates(
            master,
            {},
            as_of=datetime(2026, 9, 1, tzinfo=timezone.utc),
            max_candidates=2,
        )
        self.assertEqual(["111111118", "222222226"], candidates)
        state = {
            "edgar_discovery": {
                "records": {
                    cusip: {
                        "record_sha256": fingerprints[cusip],
                        "terminal": True,
                        "status": "no_evidence",
                        "checked_at": "2026-09-01T00:00:00Z",
                    }
                    for cusip in candidates
                }
            }
        }
        candidates, _ = pipeline._sec_edgar_discovery_candidates(
            master,
            state,
            as_of=datetime(2026, 9, 2, tzinfo=timezone.utc),
            max_candidates=2,
        )
        self.assertEqual(["333333334"], candidates)

    def test_transient_results_rotate_behind_unattempted_backlog(self) -> None:
        master = {
            "records": {
                f"{cusip}|EQUITY": {
                    "cusip": cusip,
                    "instrument_type": "EQUITY",
                    "mapping_status": "unresolved",
                    "resolution_reason": "no_ftd_symbol_evidence",
                    "official_13f_status": "active",
                }
                for cusip in ("111111118", "222222226", "333333334")
            }
        }
        first, fingerprints = pipeline._sec_edgar_discovery_candidates(
            master,
            {},
            as_of=datetime(2026, 9, 1, tzinfo=timezone.utc),
            max_candidates=2,
        )
        self.assertEqual(["111111118", "222222226"], first)
        state = {
            "edgar_discovery": {
                "records": {
                    cusip: {
                        "record_sha256": fingerprints[cusip],
                        "terminal": False,
                        "status": "transient_error",
                        "checked_at": "2026-09-01T00:00:00Z",
                    }
                    for cusip in first
                }
            }
        }

        second, _ = pipeline._sec_edgar_discovery_candidates(
            master,
            state,
            as_of=datetime(2026, 9, 2, tzinfo=timezone.utc),
            max_candidates=2,
        )

        self.assertEqual(["333333334", "111111118"], second)

    def test_transient_higher_lane_cannot_starve_unattempted_lower_lane(
        self,
    ) -> None:
        records = {
            f"{cusip}|EQUITY": {
                "cusip": cusip,
                "instrument_type": "EQUITY",
                "mapping_status": "ambiguous",
                "resolution_reason": "conflicting_recent_ftd_symbols",
                "official_13f_status": "active",
            }
            for cusip in ("111111118", "222222226", "333333334")
        }
        records["999999999|EQUITY"] = {
            "cusip": "999999999",
            "instrument_type": "EQUITY",
            "mapping_status": "unresolved",
            "resolution_reason": "no_ftd_symbol_evidence",
            "official_13f_status": "active",
        }
        master = {"records": records}
        first, fingerprints = pipeline._sec_edgar_discovery_candidates(
            master,
            {},
            as_of=datetime(2026, 9, 1, tzinfo=timezone.utc),
            max_candidates=2,
        )
        self.assertEqual(["111111118", "222222226"], first)
        state = {
            "edgar_discovery": {
                "records": {
                    cusip: {
                        "record_sha256": fingerprints[cusip],
                        "terminal": False,
                        "status": "transient_error",
                        "checked_at": "2026-09-01T00:00:00Z",
                    }
                    for cusip in first
                }
            }
        }

        second, _ = pipeline._sec_edgar_discovery_candidates(
            master,
            state,
            as_of=datetime(2026, 9, 2, tzinfo=timezone.utc),
            max_candidates=2,
        )

        self.assertEqual(["333333334", "999999999"], second)

    def test_changed_fingerprint_precedes_untouched_current_backlog(self) -> None:
        master = {
            "records": {
                f"{cusip}|EQUITY": {
                    "cusip": cusip,
                    "instrument_type": "EQUITY",
                    "mapping_status": "ambiguous",
                    "resolution_reason": "conflicting_recent_ftd_symbols",
                    "official_13f_status": "active",
                }
                for cusip in ("111111118", "999999999")
            }
        }
        state = {
            "edgar_discovery": {
                "records": {
                    "999999999": {
                        "record_sha256": "a" * 64,
                        "terminal": True,
                        "status": "no_evidence",
                        "checked_at": "2026-08-31T00:00:00Z",
                    }
                }
            }
        }

        candidates, _ = pipeline._sec_edgar_discovery_candidates(
            master,
            state,
            as_of=datetime(2026, 9, 1, tzinfo=timezone.utc),
            max_candidates=1,
        )

        self.assertEqual(["999999999"], candidates)

    def test_edgar_refresh_uses_incremental_and_clean_queue_limits(self) -> None:
        result = RefreshResult(
            master={"records": {}},
            state={},
            changed=False,
            refreshed_urls=(),
            retained_urls=(),
            errors=(),
            acceptance={"ok": True},
        )
        with mock.patch.object(
            pipeline,
            "_sec_edgar_discovery_candidates",
            return_value=([], {}),
        ) as select:
            pipeline._refresh_sec_edgar_exceptions(
                result,
                [],
                refreshed_at=datetime(2026, 9, 1, tzinfo=timezone.utc),
                checkpoint_batches=False,
            )
            pipeline._refresh_sec_edgar_exceptions(
                result,
                [],
                refreshed_at=datetime(2026, 9, 1, tzinfo=timezone.utc),
                checkpoint_batches=True,
            )

        self.assertEqual(
            [
                pipeline._SEC_EDGAR_INCREMENTAL_CANDIDATE_LIMIT,
                pipeline._SEC_EDGAR_CLEAN_CANDIDATE_LIMIT,
            ],
            [call.kwargs["max_candidates"] for call in select.call_args_list],
        )

    def test_resolved_ixbrl_mapping_is_revalidated_every_30_days(self) -> None:
        resolved_master = {
            "records": {
                "111111118|EQUITY": {
                    "cusip": "111111118",
                    "instrument_type": "EQUITY",
                    "mapping_status": "resolved",
                    "ticker": "TEST",
                    "ticker_source": "sec_ixbrl",
                    "ticker_as_of": "2026-06-30",
                    "last_verification_date": "2026-06-30",
                    "resolution_reason": (
                        "exact_sec_schedule_13dg_ixbrl_class_bridge"
                    ),
                    "reported_issuer": "EXAMPLE INC",
                    "reported_class": "COM",
                }
            }
        }
        candidates, fingerprints = pipeline._sec_edgar_discovery_candidates(
            resolved_master,
            {},
            as_of=datetime(2026, 9, 1, tzinfo=timezone.utc),
        )
        self.assertEqual(["111111118"], candidates)
        state = {
            "edgar_discovery": {
                "records": {
                    "111111118": {
                        "record_sha256": fingerprints["111111118"],
                        "terminal": True,
                        "status": "sources_found",
                        "checked_at": "2026-08-31T00:00:00Z",
                    }
                }
            }
        }
        candidates, _ = pipeline._sec_edgar_discovery_candidates(
            resolved_master,
            state,
            as_of=datetime(2026, 9, 1, tzinfo=timezone.utc),
        )
        self.assertEqual([], candidates)
        candidates, _ = pipeline._sec_edgar_discovery_candidates(
            resolved_master,
            state,
            as_of=datetime(2026, 9, 29, tzinfo=timezone.utc),
        )
        self.assertEqual([], candidates)
        candidates, _ = pipeline._sec_edgar_discovery_candidates(
            resolved_master,
            state,
            as_of=datetime(2026, 9, 30, tzinfo=timezone.utc),
        )
        self.assertEqual(["111111118"], candidates)

    def test_edgar_discovery_is_embedded_and_rebuilt_without_third_cache(self) -> None:
        master = {
            "policy": {
                "recent_window_days": 31,
                "max_evidence_age_days": 395,
                "min_confirmation_dates": 2,
            },
            "records": {
                "111111118|EQUITY": {
                    "cusip": "111111118",
                    "instrument_type": "EQUITY",
                    "mapping_status": "unresolved",
                    "resolution_reason": "no_ftd_symbol_evidence",
                    "official_13f_status": "active",
                }
            },
            "summary": {"resolved": 0},
        }
        result = RefreshResult(
            master=master,
            state={
                "updated_at": "2026-08-01T00:00:00Z",
                "edgar_discovery": {},
                "edgar_evidence": {},
            },
            changed=False,
            refreshed_urls=(),
            retained_urls=(),
            errors=(),
            acceptance={"ok": True},
        )
        source_pair = (
            FilingSource(
                "schedule_13dg",
                "https://www.sec.gov/Archives/edgar/data/1/"
                "000000000124000001/schedule.xml",
                "0000000001-24-000001",
            ),
            FilingSource(
                "periodic_ixbrl",
                "https://www.sec.gov/Archives/edgar/data/1/"
                "000000000124000002/report.htm",
                "0000000001-24-000002",
            ),
        )
        discovery = SimpleNamespace(
            sources=source_pair,
            to_dict=lambda: {
                "sources": [],
                "diagnostics": [
                    {
                        "cusip": "111111118",
                        "status": "sources_found",
                        "terminal": True,
                        "reason": "exact_schedule_cusip_and_ixbrl_class_bridge",
                    }
                ],
                "fetched_sources": [
                    {
                        "kind": "sec_cusip_search",
                        "url": "https://efts.sec.gov/LATEST/search-index?q=111111118",
                        "outcome": "fetched",
                        "sha256": "a" * 64,
                    }
                ],
            },
        )
        evidence_cache = {
            "schema_version": 1,
            "generated_at": "2026-08-31T00:00:00Z",
            "sources": [],
            "schedule_evidence": [],
            "ixbrl_evidence": [],
            "records": {},
            "unresolved": {},
            "summary": {},
        }
        rebuilt = {
            **master,
            "summary": {"resolved": 1},
        }
        fetcher = object()
        with (
            mock.patch.object(
                pipeline,
                "discover_sec_edgar_sources",
                return_value=discovery,
            ) as discover,
            mock.patch.object(
                pipeline,
                "refresh_sec_edgar_evidence",
                return_value=evidence_cache,
            ) as refresh_evidence,
            mock.patch.object(
                pipeline,
                "merge_sec_edgar_evidence_caches",
                return_value=evidence_cache,
            ),
            mock.patch.object(
                pipeline,
                "rebuild_sec_security_master",
                return_value=rebuilt,
            ),
            mock.patch.object(
                pipeline,
                "audit_security_master",
                return_value={"ok": True, "issues": []},
            ),
            mock.patch.object(
                pipeline,
                "save_security_master_pair",
            ) as save_pair,
        ):
            refreshed = pipeline._refresh_sec_edgar_exceptions(
                result,
                [{"cusip": "111111118", "instrument_type": "EQUITY"}],
                refreshed_at=datetime(
                    2026,
                    8,
                    31,
                    tzinfo=timezone.utc,
                ),
                fetcher=fetcher,
            )

        discover.assert_called_once_with(["111111118"], fetcher=fetcher)
        self.assertIsNone(refresh_evidence.call_args.kwargs["cache_path"])
        self.assertIs(fetcher, refresh_evidence.call_args.kwargs["fetcher"])
        save_pair.assert_called_once_with(
            rebuilt,
            refreshed.state,
            master_path=pipeline.SEC_SECURITY_MASTER_PATH,
            source_state_path=pipeline.SEC_SOURCE_STATE_PATH,
        )
        self.assertEqual(1, refreshed.master["summary"]["resolved"])
        diagnostic = refreshed.state["edgar_discovery"]["records"]["111111118"]
        self.assertTrue(diagnostic["terminal"])
        self.assertEqual("2026-08-31T00:00:00Z", diagnostic["checked_at"])
        self.assertEqual(
            "2026-08-31T00:00:00Z",
            diagnostic["last_successful_check_at"],
        )
        self.assertEqual(
            2,
            refreshed.state["edgar_discovery"]["schema_version"],
        )
        self.assertRegex(diagnostic["record_sha256"], r"^[0-9a-f]{64}$")
        self.assertEqual(evidence_cache, refreshed.state["edgar_evidence"])

    def test_clean_edgar_batches_fetch_each_new_filing_pair_only_once(self) -> None:
        cusips = ("111111118", "222222226")
        current_master = {
            "policy": {
                "recent_window_days": 31,
                "max_evidence_age_days": 395,
                "min_confirmation_dates": 2,
            },
            "records": {
                f"{cusip}|EQUITY": {
                    "cusip": cusip,
                    "instrument_type": "EQUITY",
                    "mapping_status": "unresolved",
                    "resolution_reason": "no_ftd_symbol_evidence",
                    "official_13f_status": "active",
                }
                for cusip in cusips
            },
            "summary": {"unresolved": 2, "resolved": 0},
        }
        result = RefreshResult(
            master=current_master,
            state={
                "updated_at": "2026-08-01T00:00:00Z",
                "edgar_discovery": {},
                "edgar_evidence": {},
            },
            changed=False,
            refreshed_urls=(),
            retained_urls=(),
            errors=(),
            acceptance={"ok": True},
        )

        def discover(batch, *, fetcher):
            del fetcher
            cusip = batch[0]
            serial = cusips.index(cusip) + 1
            pair = (
                FilingSource(
                    "schedule_13dg",
                    "https://www.sec.gov/Archives/edgar/data/1/"
                    f"00000000012400000{serial}/schedule.xml",
                    f"0000000001-24-00000{serial}",
                ),
                FilingSource(
                    "periodic_ixbrl",
                    "https://www.sec.gov/Archives/edgar/data/1/"
                    f"00000000012400001{serial}/report.htm",
                    f"0000000001-24-00001{serial}",
                ),
            )
            return SimpleNamespace(
                sources=pair,
                to_dict=lambda: {
                    "sources": [],
                    "diagnostics": [{
                        "cusip": cusip,
                        "status": "sources_found",
                        "terminal": True,
                        "reason": "exact_schedule_cusip_and_ixbrl_class_bridge",
                    }],
                    "fetched_sources": [],
                },
            )

        fetched_pairs = []

        def refresh_pair(sources, **_kwargs):
            fetched_pairs.append(tuple(source.url for source in sources))
            return {}

        with (
            mock.patch.object(pipeline, "_SEC_EDGAR_CLEAN_CHUNK_SIZE", 1),
            mock.patch.object(
                pipeline,
                "discover_sec_edgar_sources",
                side_effect=discover,
            ),
            mock.patch.object(
                pipeline,
                "refresh_sec_edgar_evidence",
                side_effect=refresh_pair,
            ),
            mock.patch.object(
                pipeline,
                "merge_sec_edgar_evidence_caches",
                return_value={},
            ),
            mock.patch.object(
                pipeline,
                "rebuild_sec_security_master",
                return_value=current_master,
            ),
            mock.patch.object(
                pipeline,
                "audit_security_master",
                return_value={"ok": True, "issues": []},
            ),
            mock.patch.object(
                pipeline,
                "save_security_master_pair",
            ) as save_pair,
        ):
            pipeline._refresh_sec_edgar_exceptions(
                result,
                [
                    {"cusip": cusip, "instrument_type": "EQUITY"}
                    for cusip in cusips
                ],
                refreshed_at=datetime(2026, 8, 31, tzinfo=timezone.utc),
                fetcher=object(),
                checkpoint_batches=True,
            )

        self.assertEqual(2, len(fetched_pairs))
        self.assertTrue(set(fetched_pairs[0]).isdisjoint(fetched_pairs[1]))
        # Batch payloads are compact checkpoints; the large bound state/master
        # pair is rebuilt and persisted exactly once after every batch succeeds.
        self.assertEqual(1, save_pair.call_count)

    def test_rebuild_workspace_refuses_to_delete_unmanaged_files(self) -> None:
        universe = [{"cusip": "111111118", "instrument_type": "EQUITY"}]
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "sec-security-master-rebuild-work"
            production_master = pipeline.load_security_master(
                Path(tmpdir) / "missing-production-master.json"
            )
            production_state = pipeline.load_source_state(
                Path(tmpdir) / "missing-production-state.json"
            )
            root.mkdir()
            (root / "manifest.json").write_text("{}\n", encoding="utf-8")
            unrelated = root / "keep-me.txt"
            unrelated.write_text("private\n", encoding="utf-8")
            with mock.patch.object(
                pipeline,
                "SEC_SECURITY_MASTER_REBUILD_WORK_ROOT",
                root,
            ):
                with self.assertRaisesRegex(
                    pipeline.SecurityMasterRefreshError,
                    "unmanaged entries.*keep-me.txt",
                ):
                    pipeline._prepare_security_master_rebuild_work(
                        universe,
                        production_master=production_master,
                        production_state=production_state,
                    )
            self.assertEqual("private\n", unrelated.read_text(encoding="utf-8"))

    def test_rebuild_workspace_rejects_symlinked_managed_file(self) -> None:
        universe = [{"cusip": "111111118", "instrument_type": "EQUITY"}]
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "sec-security-master-rebuild-work"
            production_master = pipeline.load_security_master(
                Path(tmpdir) / "missing-production-master.json"
            )
            production_state = pipeline.load_source_state(
                Path(tmpdir) / "missing-production-state.json"
            )
            root.mkdir()
            outside = Path(tmpdir) / "outside.json"
            outside.write_text("{}\n", encoding="utf-8")
            managed = root / "sec_source_state.json"
            managed.symlink_to(outside)
            with mock.patch.object(
                pipeline,
                "SEC_SECURITY_MASTER_REBUILD_WORK_ROOT",
                root,
            ):
                with self.assertRaisesRegex(
                    pipeline.SecurityMasterRefreshError,
                    "managed path must be a regular file: sec_source_state.json",
                ):
                    pipeline._prepare_security_master_rebuild_work(
                        universe,
                        production_master=production_master,
                        production_state=production_state,
                    )
            self.assertTrue(managed.is_symlink())
            self.assertEqual("{}\n", outside.read_text(encoding="utf-8"))

    def test_parser_drift_resets_only_ftd_stage_and_preserves_13f_receipt(
        self,
    ) -> None:
        universe = [{"cusip": "111111118", "instrument_type": "EQUITY"}]
        current = datetime(2026, 8, 31, tzinfo=timezone.utc)
        missing_root = Path(self._stage_tmp.name)
        production_master = pipeline.load_security_master(
            missing_root / "missing-production-master.json"
        )
        production_state = pipeline.load_source_state(
            missing_root / "missing-production-state.json"
        )
        root, complete = pipeline._prepare_security_master_rebuild_work(
            universe,
            production_master=production_master,
            production_state=production_state,
            current=current,
        )
        self.assertFalse(complete)
        receipt = {
            "generated_at": "2026-08-30T00:00:00Z",
            "receipt_sha256": "a" * 64,
        }
        pipeline._record_reported_identity_rebuild_receipt(root, receipt)
        manifest_path = root / "manifest.json"
        stale_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        stale_manifest["plan"]["parser_sha256"][
            "sec_security_master.py"
        ] = "0" * 64
        stale_manifest["plan_sha256"] = hashlib.sha256(
            json.dumps(
                stale_manifest["plan"],
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        pipeline._atomic_write_json(
            manifest_path,
            stale_manifest,
            sort_keys=True,
        )
        (root / "sec_source_state.json").write_text(
            '{"stale":"ftd-state"}\n',
            encoding="utf-8",
        )
        (root / "sec_security_master.json").write_text(
            '{"stale":"master"}\n',
            encoding="utf-8",
        )

        with (
            mock.patch.object(
                pipeline,
                "load_security_master_pair",
                return_value=(production_master, production_state),
            ),
        ):
            resumed_receipt = (
                pipeline._reported_identity_resume_receipt_for_in_progress_rebuild(
                    universe
                )
            )
        self.assertEqual(receipt, resumed_receipt)

        reset_root, complete = pipeline._prepare_security_master_rebuild_work(
            universe,
            production_master=production_master,
            production_state=production_state,
            current=current,
        )
        self.assertEqual(root, reset_root)
        self.assertFalse(complete)
        self.assertFalse((root / "sec_source_state.json").exists())
        self.assertFalse((root / "sec_security_master.json").exists())
        current_manifest = json.loads(
            manifest_path.read_text(encoding="utf-8")
        )
        self.assertNotEqual(
            stale_manifest["plan_sha256"],
            current_manifest["plan_sha256"],
        )
        self.assertNotIn("reported_identity_rebuild_receipt", current_manifest)

        pipeline._record_reported_identity_rebuild_receipt(
            reset_root,
            resumed_receipt,
        )
        rerecorded = json.loads(manifest_path.read_text(encoding="utf-8"))
        self.assertEqual(
            receipt,
            rerecorded["reported_identity_rebuild_receipt"],
        )

    def test_rebuild_workspace_resumes_compatible_complete_stage(self) -> None:
        universe = [
            {
                "cusip": "111111118",
                "instrument_type": "EQUITY",
                "issuer": "Example A",
                "security_class": "Class A",
            },
            {
                "cusip": "111111118",
                "instrument_type": "EQUITY",
                "issuer": "Example B",
                "security_class": "Class B",
            },
        ]
        current = datetime(2026, 8, 31, tzinfo=timezone.utc)
        production_master = pipeline.load_security_master(
            Path(self._stage_tmp.name) / "missing-production-master.json"
        )
        production_state = pipeline.load_source_state(
            Path(self._stage_tmp.name) / "missing-production-state.json"
        )
        desired_manifest = pipeline._security_master_rebuild_manifest(
            universe,
            production_master=production_master,
            production_state=production_state,
        )
        self.assertIn(
            "pipeline.py",
            desired_manifest["plan"]["parser_sha256"],
        )
        self.assertIn(
            "security_identity.py",
            desired_manifest["plan"]["parser_sha256"],
        )
        root, complete = pipeline._prepare_security_master_rebuild_work(
            universe,
            production_master=production_master,
            production_state=production_state,
            current=current,
        )
        self.assertFalse(complete)
        state = pipeline.load_source_state(root / "missing-state.json")
        staged_master = pipeline.rebuild_sec_security_master(state, universe)
        pipeline.save_source_state(state, root / "sec_source_state.json")
        pipeline.save_security_master(
            staged_master,
            root / "sec_security_master.json",
        )
        pipeline._mark_security_master_rebuild_complete(root, current=current)
        state_bytes = (root / "sec_source_state.json").read_bytes()
        master_bytes = (root / "sec_security_master.json").read_bytes()
        completed_manifest = json.loads(
            (root / "manifest.json").read_text(encoding="utf-8")
        )
        self.assertEqual(
            pipeline._security_master_pair_fingerprint(
                staged_master,
                state,
            ),
            completed_manifest["completed_stage"],
        )

        with mock.patch.object(
            pipeline,
            "audit_security_master",
            return_value={"ok": True, "issues": []},
        ) as audit:
            resumed_root, complete = (
                pipeline._prepare_security_master_rebuild_work(
                    universe,
                    production_master=staged_master,
                    production_state=state,
                    current=current,
                )
            )

        self.assertTrue(complete)
        audit.assert_called_once()
        self.assertEqual(root, resumed_root)
        self.assertEqual(state_bytes, (root / "sec_source_state.json").read_bytes())
        self.assertEqual(master_bytes, (root / "sec_security_master.json").read_bytes())

        fresh_root, complete = pipeline._prepare_security_master_rebuild_work(
            universe,
            production_master=staged_master,
            production_state=state,
            current=current,
            force_fresh_completed=True,
        )
        self.assertEqual(root, fresh_root)
        self.assertFalse(complete)
        self.assertFalse((root / "sec_source_state.json").exists())
        self.assertFalse((root / "sec_security_master.json").exists())
        fresh_manifest = json.loads(
            (root / "manifest.json").read_text(encoding="utf-8")
        )
        self.assertEqual("in_progress", fresh_manifest["status"])
        self.assertEqual(
            pipeline._security_master_pair_fingerprint(staged_master, state),
            fresh_manifest["base_production"],
        )

    def test_completed_stage_survives_crash_before_production_promotion(
        self,
    ) -> None:
        universe = [{"cusip": "111111118", "instrument_type": "EQUITY"}]
        current = datetime(2026, 8, 31, tzinfo=timezone.utc)
        missing_root = Path(self._stage_tmp.name)
        base_master = pipeline.load_security_master(
            missing_root / "missing-base-master.json"
        )
        base_state = pipeline.load_source_state(
            missing_root / "missing-base-state.json"
        )
        root, complete = pipeline._prepare_security_master_rebuild_work(
            universe,
            production_master=base_master,
            production_state=base_state,
            current=current,
        )
        self.assertFalse(complete)
        receipt = {
            "generated_at": "2026-08-30T00:00:00Z",
            "receipt_sha256": "a" * 64,
        }
        pipeline._record_reported_identity_rebuild_receipt(root, receipt)

        staged_master = pipeline.rebuild_sec_security_master(
            base_state,
            universe,
        )
        pipeline.save_security_master_pair(
            staged_master,
            base_state,
            master_path=root / "sec_security_master.json",
            source_state_path=root / "sec_source_state.json",
        )
        pipeline._mark_security_master_rebuild_complete(root, current=current)
        state_bytes = (root / "sec_source_state.json").read_bytes()
        master_bytes = (root / "sec_security_master.json").read_bytes()

        with mock.patch.object(
            pipeline,
            "load_security_master_pair",
            side_effect=[
                (base_master, base_state),
                (staged_master, base_state),
            ],
        ):
            self.assertEqual(
                receipt,
                pipeline._reported_identity_resume_receipt_for_in_progress_rebuild(
                    universe
                ),
            )
        with mock.patch.object(
            pipeline,
            "load_security_master_pair",
            return_value=(staged_master, base_state),
        ):
            self.assertIsNone(
                pipeline._reported_identity_resume_receipt_for_in_progress_rebuild(
                    universe
                )
            )

        # Simulate process death after the stage was committed but before the
        # production pair changed. Even an established installation passes
        # force_fresh_completed=True; the unfinished attempt must still resume
        # this exact stage instead of selecting a different EDGAR batch.
        with mock.patch.object(
            pipeline,
            "audit_security_master",
            return_value={"ok": True, "issues": []},
        ):
            resumed_root, complete = (
                pipeline._prepare_security_master_rebuild_work(
                    universe,
                    production_master=base_master,
                    production_state=base_state,
                    current=current,
                    force_fresh_completed=True,
                )
            )

        self.assertTrue(complete)
        self.assertEqual(root, resumed_root)
        self.assertEqual(
            state_bytes,
            (root / "sec_source_state.json").read_bytes(),
        )
        self.assertEqual(
            master_bytes,
            (root / "sec_security_master.json").read_bytes(),
        )
        manifest = json.loads(
            (root / "manifest.json").read_text(encoding="utf-8")
        )
        self.assertEqual("complete", manifest["status"])
        self.assertEqual(
            pipeline._security_master_pair_fingerprint(
                base_master,
                base_state,
            ),
            manifest["base_production"],
        )

    def test_rebuild_workspace_rejects_complete_stage_after_production_advances(
        self,
    ) -> None:
        universe = [{"cusip": "111111118", "instrument_type": "EQUITY"}]
        current = datetime(2026, 8, 31, tzinfo=timezone.utc)
        missing_root = Path(self._stage_tmp.name)
        base_master = pipeline.load_security_master(
            missing_root / "missing-base-master.json"
        )
        base_state = pipeline.load_source_state(
            missing_root / "missing-base-state.json"
        )
        root, complete = pipeline._prepare_security_master_rebuild_work(
            universe,
            production_master=base_master,
            production_state=base_state,
            current=current,
        )
        self.assertFalse(complete)
        staged_master = pipeline.rebuild_sec_security_master(
            base_state,
            universe,
        )
        pipeline.save_source_state(
            base_state,
            root / "sec_source_state.json",
        )
        pipeline.save_security_master(
            staged_master,
            root / "sec_security_master.json",
        )
        pipeline._mark_security_master_rebuild_complete(root, current=current)

        advanced_state = json.loads(json.dumps(base_state))
        advanced_state["updated_at"] = "2026-09-01T00:00:00Z"
        advanced_master = pipeline.rebuild_sec_security_master(
            advanced_state,
            universe,
        )
        _resumed_root, complete = pipeline._prepare_security_master_rebuild_work(
            universe,
            production_master=advanced_master,
            production_state=advanced_state,
            current=current,
        )

        self.assertFalse(complete)
        self.assertFalse((root / "sec_security_master.json").exists())
        self.assertFalse((root / "sec_source_state.json").exists())
        manifest = json.loads(
            (root / "manifest.json").read_text(encoding="utf-8")
        )
        self.assertEqual("in_progress", manifest["status"])
        self.assertEqual(
            pipeline._security_master_pair_fingerprint(
                advanced_master,
                advanced_state,
            ),
            manifest["base_production"],
        )
        self.assertIsNone(manifest["completed_stage"])

    def test_rebuild_workspace_reopens_complete_stage_that_fails_current_audit(
        self,
    ) -> None:
        universe = [{"cusip": "111111118", "instrument_type": "EQUITY"}]
        current = datetime(2026, 8, 31, tzinfo=timezone.utc)
        missing_root = Path(self._stage_tmp.name)
        base_master = pipeline.load_security_master(
            missing_root / "missing-base-master.json"
        )
        base_state = pipeline.load_source_state(
            missing_root / "missing-base-state.json"
        )
        root, complete = pipeline._prepare_security_master_rebuild_work(
            universe,
            production_master=base_master,
            production_state=base_state,
            current=current,
        )
        self.assertFalse(complete)
        staged_master = pipeline.rebuild_sec_security_master(
            base_state,
            universe,
        )
        pipeline.save_source_state(
            base_state,
            root / "sec_source_state.json",
        )
        pipeline.save_security_master(
            staged_master,
            root / "sec_security_master.json",
        )
        pipeline._mark_security_master_rebuild_complete(root, current=current)
        state_bytes = (root / "sec_source_state.json").read_bytes()
        master_bytes = (root / "sec_security_master.json").read_bytes()

        with mock.patch.object(
            pipeline,
            "audit_security_master",
            return_value={"ok": False, "issues": ["official_13f_period_is_stale"]},
        ) as audit:
            _resumed_root, complete = (
                pipeline._prepare_security_master_rebuild_work(
                    universe,
                    production_master=staged_master,
                    production_state=base_state,
                    current=current,
                )
            )

        self.assertFalse(complete)
        audit.assert_called_once()
        self.assertEqual(state_bytes, (root / "sec_source_state.json").read_bytes())
        self.assertEqual(master_bytes, (root / "sec_security_master.json").read_bytes())
        manifest = json.loads(
            (root / "manifest.json").read_text(encoding="utf-8")
        )
        self.assertEqual("in_progress", manifest["status"])
        self.assertEqual(
            pipeline._security_master_pair_fingerprint(
                staged_master,
                base_state,
            ),
            manifest["base_production"],
        )
        self.assertIsNone(manifest["completed_stage"])

        # A crash after reopening must resume the same staged work rather than
        # discard it due to the old pre-promotion base fingerprint.
        _resumed_root, complete = pipeline._prepare_security_master_rebuild_work(
            universe,
            production_master=staged_master,
            production_state=base_state,
            current=current,
        )
        self.assertFalse(complete)
        self.assertEqual(state_bytes, (root / "sec_source_state.json").read_bytes())
        self.assertEqual(master_bytes, (root / "sec_security_master.json").read_bytes())

    def test_reported_identity_receipt_requires_matching_in_progress_workspace(
        self,
    ) -> None:
        universe = [{"cusip": "111111118", "instrument_type": "EQUITY"}]
        production_master = pipeline.load_security_master(
            Path(self._stage_tmp.name) / "missing-production-master.json"
        )
        production_state = pipeline.load_source_state(
            Path(self._stage_tmp.name) / "missing-production-state.json"
        )
        root, complete = pipeline._prepare_security_master_rebuild_work(
            universe,
            production_master=production_master,
            production_state=production_state,
            current=datetime(2026, 8, 31, tzinfo=timezone.utc),
        )
        self.assertFalse(complete)
        receipt = {
            "generated_at": "2026-08-30T00:00:00Z",
            "receipt_sha256": "a" * 64,
        }
        pipeline._record_reported_identity_rebuild_receipt(root, receipt)

        with (
            mock.patch.object(
                pipeline,
                "load_security_master_pair",
                return_value=(production_master, production_state),
            ),
        ):
            self.assertEqual(
                receipt,
                pipeline._reported_identity_resume_receipt_for_in_progress_rebuild(
                    universe
                ),
            )

        manifest_path = root / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["status"] = "complete"
        manifest["completed_at"] = "2026-08-31T00:00:00Z"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        self.assertIsNone(
            pipeline._reported_identity_resume_receipt_for_in_progress_rebuild(
                universe
            )
        )

    def test_completed_identity_receipt_is_only_reused_for_unpublished_cutover(
        self,
    ) -> None:
        receipt = {
            "schema_version": 1,
            "receipt_sha256": "a" * 64,
        }
        with mock.patch.object(
            pipeline,
            "load_completed_clean_rebuild_receipt",
            return_value=receipt,
        ) as load_receipt:
            self.assertEqual(
                receipt,
                pipeline._legacy_cutover_completed_identity_receipt(
                    published_sec_security_state=False,
                ),
            )
            self.assertIsNone(
                pipeline._legacy_cutover_completed_identity_receipt(
                    published_sec_security_state=True,
                )
            )

        load_receipt.assert_called_once_with()

    def test_preplan_index_adoption_is_never_prepared_for_published_state(
        self,
    ) -> None:
        receipt = {
            "schema_version": 2,
            "receipt_scope": pipeline.LEGACY_INDEX_ADOPTION_RECEIPT_SCOPE,
            "receipt_sha256": "a" * 64,
        }
        with (
            mock.patch.object(
                pipeline,
                "load_completed_clean_rebuild_receipt",
                return_value=None,
            ) as load_receipt,
            mock.patch.object(
                pipeline,
                "prepare_unpublished_legacy_index_adoption",
                return_value=receipt,
            ) as prepare_adoption,
        ):
            self.assertIsNone(
                pipeline._legacy_cutover_completed_identity_receipt(
                    published_sec_security_state=True,
                )
            )
            prepare_adoption.assert_not_called()
            self.assertEqual(
                receipt,
                pipeline._legacy_cutover_completed_identity_receipt(
                    published_sec_security_state=False,
                ),
            )

        load_receipt.assert_called_once_with()
        prepare_adoption.assert_called_once_with(
            pipeline.FUNDS_DIR,
            published_sec_security_state=False,
        )

    def test_full_rebuild_passes_and_records_owned_reported_identity_receipt(
        self,
    ) -> None:
        universe = [{"cusip": "111111118", "instrument_type": "EQUITY"}]
        old_receipt = {
            "generated_at": "2026-08-30T00:00:00Z",
            "receipt_sha256": "a" * 64,
        }
        new_receipt = {
            "generated_at": "2026-08-30T00:00:00Z",
            "receipt_sha256": "b" * 64,
        }
        rebuild_root = pipeline.SEC_SECURITY_MASTER_REBUILD_WORK_ROOT
        rebuild_root.mkdir(parents=True)
        (rebuild_root / "manifest.json").write_text(
            json.dumps({"status": "in_progress"}),
            encoding="utf-8",
        )
        refresh_result = RefreshResult(
            master={
                "records": {"111111118|EQUITY": {}},
                "summary": {},
                "audit": {},
            },
            state={
                "schema_version": 2,
                "sources": {},
                "edgar_discovery": {"records": {}},
            },
            changed=True,
            refreshed_urls=(),
            retained_urls=(),
            errors=(),
            acceptance={"ok": True, "issues": []},
        )
        backfill_result = SimpleNamespace(
            backfill=SimpleNamespace(holdings_changed=0, files_changed=0),
            archive_fallback=None,
            completed_rebuild_receipt=new_receipt,
        )
        acceptance = {
            "ok": True,
            "issues": [],
            "ftd_coverage_ratio": 1.0,
            "ftd_evidenced_official_cusip_count": 1,
            "active_non_option_official_cusip_count": 1,
        }
        publication_events: list[str] = []

        with (
            mock.patch.object(
                pipeline,
                "reported_identity_backfill_audit",
                side_effect=[
                    {
                        "holdings_scanned": 1,
                        "incomplete_holdings": 0,
                        "needed": False,
                    },
                    {"incomplete_holdings": 0, "needed": False},
                ],
            ),
            mock.patch.object(
                pipeline,
                "collect_security_master_universe",
                return_value=universe,
            ) as collect_universe,
            mock.patch.object(
                pipeline,
                "_reported_identity_resume_receipt_for_in_progress_rebuild",
                return_value=old_receipt,
            ) as load_receipt,
            mock.patch.object(
                pipeline,
                "rebuild_reported_identity_from_sec",
                return_value=backfill_result,
            ) as backfill,
            mock.patch.object(
                pipeline,
                "load_security_master_pair",
                return_value=({"audit": {}}, {}),
            ),
            mock.patch.object(
                pipeline,
                "_prepare_security_master_rebuild_work",
                return_value=(rebuild_root, False),
            ) as prepare_rebuild,
            mock.patch.object(
                pipeline,
                "_record_reported_identity_rebuild_receipt",
            ) as record_receipt,
            mock.patch.object(
                pipeline,
                "refresh_security_master",
                return_value=refresh_result,
            ),
            mock.patch.object(
                pipeline,
                "_refresh_sec_fund_series_evidence",
                side_effect=lambda result, _universe, **_kwargs: result,
            ),
            mock.patch.object(
                pipeline,
                "_refresh_sec_edgar_exceptions",
                side_effect=lambda result, _universe, **_kwargs: result,
            ),
            mock.patch.object(
                pipeline,
                "audit_security_master",
                return_value=acceptance,
            ),
            mock.patch.object(
                pipeline,
                "save_security_master_pair",
                side_effect=lambda *_args, **_kwargs: publication_events.append(
                    "promote"
                ),
            ) as save_pair,
            mock.patch.object(
                pipeline,
                "_mark_security_master_rebuild_complete",
                side_effect=lambda *_args, **_kwargs: publication_events.append(
                    "complete-stage"
                ),
            ) as mark_complete,
        ):
            pipeline.refresh_sec_security_master_from_funds(full_rebuild=True)

        collect_universe.assert_called_once_with(None)
        load_receipt.assert_called_once_with(universe)
        backfill.assert_called_once_with(
            pipeline.FUNDS_DIR,
            user_agent=pipeline.USER_AGENT,
            completed_rebuild_receipt=old_receipt,
        )
        record_receipt.assert_called_once_with(rebuild_root, new_receipt)
        prepare_rebuild.assert_called_once_with(
            universe,
            production_master={"audit": {}},
            production_state={},
            force_fresh_completed=True,
        )
        save_pair.assert_called_once_with(
            refresh_result.master,
            refresh_result.state,
            master_path=pipeline.SEC_SECURITY_MASTER_PATH,
            source_state_path=pipeline.SEC_SOURCE_STATE_PATH,
        )
        mark_complete.assert_called_once_with(rebuild_root)
        self.assertEqual(["complete-stage", "promote"], publication_events)

    def test_legacy_cutover_retry_can_reuse_completed_clean_stage(self) -> None:
        missing_root = Path(self._stage_tmp.name)
        legacy_master = pipeline.load_security_master(
            missing_root / "missing-legacy-master.json"
        )
        legacy_state = pipeline.load_source_state(
            missing_root / "missing-legacy-state.json"
        )

        self.assertFalse(
            pipeline._has_published_sec_security_state(
                legacy_master,
                legacy_state,
            )
        )
        self.assertTrue(
            pipeline._has_published_sec_security_state(
                {**legacy_master, "audit": {}},
                legacy_state,
            )
        )

    def test_transient_edgar_attempt_preserves_last_successful_check(self) -> None:
        current_master = {
            "policy": {
                "recent_window_days": 31,
                "max_evidence_age_days": 395,
                "min_confirmation_dates": 2,
            },
            "records": {
                "111111118|EQUITY": {
                    "cusip": "111111118",
                    "instrument_type": "EQUITY",
                    "mapping_status": "resolved",
                    "ticker": "TEST",
                    "ticker_source": "sec_ixbrl",
                    "ticker_as_of": "2026-06-30",
                    "last_verification_date": "2026-06-30",
                    "resolution_reason": (
                        "exact_sec_schedule_13dg_ixbrl_class_bridge"
                    ),
                }
            },
            "summary": {"resolved": 1},
        }
        _, fingerprints = pipeline._sec_edgar_discovery_candidates(
            current_master,
            {},
            as_of=datetime(2026, 8, 1, tzinfo=timezone.utc),
        )
        prior_success = "2026-07-01T00:00:00Z"
        result = RefreshResult(
            master=current_master,
            state={
                "updated_at": prior_success,
                "edgar_evidence": {},
                "edgar_discovery": {
                    "schema_version": 2,
                    "records": {
                        "111111118": {
                            "record_sha256": fingerprints["111111118"],
                            "terminal": True,
                            "status": "sources_found",
                            "checked_at": prior_success,
                            "last_successful_check_at": prior_success,
                        }
                    },
                    "fetched_sources": {},
                },
            },
            changed=False,
            refreshed_urls=(),
            retained_urls=(),
            errors=(),
            acceptance={"ok": True},
        )
        discovery = SimpleNamespace(
            sources=(),
            to_dict=lambda: {
                "sources": [],
                "diagnostics": [{
                    "cusip": "111111118",
                    "status": "transient_error",
                    "terminal": False,
                    "reason": "search_fetch_failed",
                }],
                "fetched_sources": [],
            },
        )
        with (
            mock.patch.object(
                pipeline,
                "discover_sec_edgar_sources",
                return_value=discovery,
            ),
            mock.patch.object(
                pipeline,
                "rebuild_sec_security_master",
                return_value=current_master,
            ),
            mock.patch.object(
                pipeline,
                "audit_security_master",
                return_value={"ok": True, "issues": []},
            ),
            mock.patch.object(pipeline, "save_security_master_pair"),
        ):
            refreshed = pipeline._refresh_sec_edgar_exceptions(
                result,
                [{"cusip": "111111118", "instrument_type": "EQUITY"}],
                refreshed_at=datetime(2026, 8, 31, tzinfo=timezone.utc),
                fetcher=object(),
            )

        diagnostic = refreshed.state["edgar_discovery"]["records"]["111111118"]
        self.assertEqual("2026-08-31T00:00:00Z", diagnostic["checked_at"])
        self.assertEqual(prior_success, diagnostic["last_successful_check_at"])

    def test_edgar_schema_change_is_fatal_before_persistence(self) -> None:
        current_master = {
            "policy": {
                "recent_window_days": 31,
                "max_evidence_age_days": 395,
                "min_confirmation_dates": 2,
            },
            "records": {
                "111111118|EQUITY": {
                    "cusip": "111111118",
                    "instrument_type": "EQUITY",
                    "mapping_status": "unresolved",
                    "resolution_reason": "no_ftd_symbol_evidence",
                    "official_13f_status": "active",
                }
            },
            "summary": {"unresolved": 1},
        }
        result = RefreshResult(
            master=current_master,
            state={
                "updated_at": "2026-08-01T00:00:00Z",
                "edgar_discovery": {},
                "edgar_evidence": {},
            },
            changed=False,
            refreshed_urls=(),
            retained_urls=(),
            errors=(),
            acceptance={"ok": True},
        )
        with (
            mock.patch.object(
                pipeline,
                "discover_sec_edgar_sources",
                side_effect=pipeline.EvidenceSchemaError(
                    "SEC search response has no hits array"
                ),
            ),
            mock.patch.object(
                pipeline,
                "save_security_master_pair",
            ) as save_pair,
        ):
            with self.assertRaises(pipeline.SourceSchemaChangeError):
                pipeline._refresh_sec_edgar_exceptions(
                    result,
                    [{"cusip": "111111118", "instrument_type": "EQUITY"}],
                    refreshed_at=datetime(2026, 8, 31, tzinfo=timezone.utc),
                    fetcher=object(),
                )

        save_pair.assert_not_called()

    def test_terminal_edgar_recheck_withdraws_superseded_bridge(self) -> None:
        schedule_url = (
            "https://www.sec.gov/Archives/edgar/data/1/"
            "000000000124000001/schedule.xml"
        )
        ixbrl_url = (
            "https://www.sec.gov/Archives/edgar/data/1/"
            "000000000124000002/report.htm"
        )
        resolved_record = {
            "cusip": "111111118",
            "instrument_type": "EQUITY",
            "mapping_status": "resolved",
            "ticker": "TEST",
            "ticker_source": "sec_ixbrl",
            "ticker_as_of": "2026-06-30",
            "last_verification_date": "2026-06-30",
            "resolution_reason": "exact_sec_schedule_13dg_ixbrl_class_bridge",
        }
        current_master = {
            "policy": {
                "recent_window_days": 31,
                "max_evidence_age_days": 395,
                "min_confirmation_dates": 2,
            },
            "records": {"111111118|EQUITY": resolved_record},
            "summary": {"resolved": 1},
        }
        result = RefreshResult(
            master=current_master,
            state={
                "updated_at": "2026-08-01T00:00:00Z",
                "edgar_discovery": {},
                "edgar_evidence": {
                    "sources": [
                        {
                            "kind": "schedule_13dg",
                            "url": schedule_url,
                            "accession": "0000000001-24-000001",
                        },
                        {
                            "kind": "periodic_ixbrl",
                            "url": ixbrl_url,
                            "accession": "0000000001-24-000002",
                        },
                    ],
                    "records": {
                        "111111118": {
                            "schedule_13dg_url": schedule_url,
                            "ixbrl_url": ixbrl_url,
                        }
                    },
                },
            },
            changed=False,
            refreshed_urls=(),
            retained_urls=(),
            errors=(),
            acceptance={"ok": True},
        )
        discovery = SimpleNamespace(
            sources=(),
            to_dict=lambda: {
                "sources": [],
                "diagnostics": [
                    {
                        "cusip": "111111118",
                        "status": "no_evidence",
                        "terminal": True,
                        "reason": "no_compatible_class_in_latest_periodic_filings",
                    }
                ],
                "fetched_sources": [],
            },
        )
        rebuilt = {
            **current_master,
            "records": {
                "111111118|EQUITY": {
                    **resolved_record,
                    "mapping_status": "unresolved",
                    "ticker": None,
                    "ticker_source": None,
                    "ticker_as_of": None,
                    "last_verification_date": None,
                    "resolution_reason": "no_ftd_symbol_evidence",
                }
            },
            "summary": {"resolved": 0, "unresolved": 1},
        }
        with (
            mock.patch.object(
                pipeline,
                "discover_sec_edgar_sources",
                return_value=discovery,
            ),
            mock.patch.object(
                pipeline,
                "refresh_sec_edgar_evidence",
            ) as refresh_evidence,
            mock.patch.object(
                pipeline,
                "merge_sec_edgar_evidence_caches",
                return_value={},
            ),
            mock.patch.object(
                pipeline,
                "rebuild_sec_security_master",
                return_value=rebuilt,
            ),
            mock.patch.object(
                pipeline,
                "audit_security_master",
                return_value={"ok": True, "issues": []},
            ),
            mock.patch.object(pipeline, "save_security_master_pair"),
        ):
            refreshed = pipeline._refresh_sec_edgar_exceptions(
                result,
                [{"cusip": "111111118", "instrument_type": "EQUITY"}],
                refreshed_at=datetime(2026, 12, 1, tzinfo=timezone.utc),
                fetcher=object(),
            )

        refresh_evidence.assert_not_called()
        self.assertEqual({}, refreshed.state["edgar_evidence"])
        withdrawn = refreshed.master["records"]["111111118|EQUITY"]
        self.assertEqual("unresolved", withdrawn["mapping_status"])
        self.assertIsNone(withdrawn["ticker"])

    def test_full_rebuild_verifies_complete_reported_identity_before_master(
        self,
    ) -> None:
        refresh_result = RefreshResult(
            master={
                "records": {"111111118|EQUITY": {}},
                "summary": {},
                "audit": {},
            },
            state={},
            changed=True,
            refreshed_urls=(),
            retained_urls=(),
            errors=(),
            acceptance={"ok": True, "issues": []},
        )
        backfill_result = SimpleNamespace(
            backfill=SimpleNamespace(holdings_changed=1, files_changed=1),
            archive_fallback=None,
        )

        def clean_refresh(_universe, **kwargs):
            self.assertFalse(kwargs["master_path"].exists())
            self.assertFalse(kwargs["source_state_path"].exists())
            return refresh_result

        with (
            mock.patch.object(
                pipeline,
                "reported_identity_backfill_audit",
                side_effect=[
                    {
                        "holdings_scanned": 1,
                        "incomplete_holdings": 0,
                        "needed": False,
                    },
                    {"incomplete_holdings": 0, "needed": False},
                ],
            ),
            mock.patch.object(
                pipeline,
                "rebuild_reported_identity_from_sec",
                return_value=backfill_result,
            ) as backfill,
            mock.patch.object(
                pipeline,
                "collect_security_master_universe",
                return_value=[
                    {"cusip": "111111118", "instrument_type": "EQUITY"}
                ],
            ),
            mock.patch.object(
                pipeline,
                "load_security_master_pair",
                return_value=({"audit": {}}, {}),
            ),
            mock.patch.object(
                pipeline,
                "refresh_security_master",
                side_effect=clean_refresh,
            ) as refresh_master,
            mock.patch.object(
                pipeline,
                "_refresh_sec_edgar_exceptions",
                side_effect=lambda result, _universe, **_kwargs: result,
            ),
            mock.patch.object(
                pipeline,
                "audit_security_master",
                return_value={
                    "ok": True,
                    "issues": [],
                    "ftd_coverage_ratio": 1.0,
                    "ftd_evidenced_official_cusip_count": 1,
                    "active_non_option_official_cusip_count": 1,
                },
            ),
            mock.patch.object(
                pipeline,
                "_mark_security_master_rebuild_complete",
            ) as mark_complete,
            mock.patch.object(
                pipeline,
                "save_security_master_pair",
            ) as save_pair,
        ):
            pipeline.refresh_sec_security_master_from_funds(full_rebuild=True)

        backfill.assert_called_once_with(
            pipeline.FUNDS_DIR,
            user_agent=pipeline.USER_AGENT,
        )
        refresh_kwargs = refresh_master.call_args.kwargs
        self.assertIsNone(refresh_kwargs["lookback_months"])
        self.assertNotEqual(
            pipeline.SEC_SECURITY_MASTER_PATH,
            refresh_kwargs["master_path"],
        )
        self.assertNotEqual(
            pipeline.SEC_SOURCE_STATE_PATH,
            refresh_kwargs["source_state_path"],
        )
        self.assertEqual(
            refresh_kwargs["master_path"].parent,
            refresh_kwargs["source_state_path"].parent,
        )
        save_pair.assert_called_once_with(
            refresh_result.master,
            refresh_result.state,
            master_path=pipeline.SEC_SECURITY_MASTER_PATH,
            source_state_path=pipeline.SEC_SOURCE_STATE_PATH,
        )
        mark_complete.assert_called_once_with(
            refresh_kwargs["master_path"].parent,
        )

    def test_clean_rebuild_does_not_promote_partial_supplemental_state(
        self,
    ) -> None:
        refresh_result = RefreshResult(
            master={
                "records": {"111111118|EQUITY": {}},
                "summary": {},
                "audit": {},
            },
            state={"schema_version": 2},
            changed=True,
            refreshed_urls=(),
            retained_urls=(),
            errors=(),
            acceptance={"ok": True, "issues": []},
        )
        incomplete = RefreshResult(
            **{
                **refresh_result.__dict__,
                "errors": ("SEC fund-series page unavailable",),
            }
        )
        backfill_result = SimpleNamespace(
            backfill=SimpleNamespace(holdings_changed=0, files_changed=0),
            archive_fallback=None,
        )
        with (
            mock.patch.object(
                pipeline,
                "reported_identity_backfill_audit",
                side_effect=[
                    {"holdings_scanned": 1, "incomplete_holdings": 0},
                    {"needed": False, "incomplete_holdings": 0},
                ],
            ),
            mock.patch.object(
                pipeline,
                "rebuild_reported_identity_from_sec",
                return_value=backfill_result,
            ),
            mock.patch.object(
                pipeline,
                "collect_security_master_universe",
                return_value=[
                    {"cusip": "111111118", "instrument_type": "EQUITY"}
                ],
            ),
            mock.patch.object(
                pipeline,
                "load_security_master_pair",
                return_value=(
                    {"audit": {"ftd_coverage_ratio": 1.0}},
                    {"schema_version": 2},
                ),
            ),
            mock.patch.object(
                pipeline,
                "refresh_security_master",
                return_value=refresh_result,
            ),
            mock.patch.object(
                pipeline,
                "_refresh_sec_fund_series_evidence",
                return_value=incomplete,
            ),
            mock.patch.object(
                pipeline,
                "_refresh_sec_edgar_exceptions",
            ) as refresh_edgar,
            mock.patch.object(
                pipeline,
                "save_security_master_pair",
            ) as save_pair,
        ):
            with self.assertRaisesRegex(
                pipeline.SecurityMasterRefreshError,
                "supplemental last-good state",
            ):
                pipeline.refresh_sec_security_master_from_funds(
                    full_rebuild=True
                )

        refresh_edgar.assert_not_called()
        save_pair.assert_not_called()

    def test_clean_rebuild_promotes_with_new_transient_edgar_unresolved(
        self,
    ) -> None:
        refresh_result = RefreshResult(
            master={
                "records": {"111111118|EQUITY": {}},
                "summary": {},
                "audit": {},
            },
            state={"schema_version": 2},
            changed=True,
            refreshed_urls=(),
            retained_urls=(),
            errors=(),
            acceptance={"ok": True, "issues": []},
        )
        transient = RefreshResult(
            **{
                **refresh_result.__dict__,
                "state": {
                    "schema_version": 2,
                    "edgar_discovery": {
                        "records": {
                            "111111118": {
                                "status": "transient_error",
                                "terminal": False,
                                "reason": "search_fetch_failed",
                            }
                        }
                    },
                },
            }
        )
        backfill_result = SimpleNamespace(
            backfill=SimpleNamespace(holdings_changed=0, files_changed=0),
            archive_fallback=None,
        )
        accepted_audit = {
            "ok": True,
            "issues": [],
            "ftd_coverage_ratio": 1.0,
            "ftd_evidenced_official_cusip_count": 1,
            "active_non_option_official_cusip_count": 1,
        }
        with (
            mock.patch.object(
                pipeline,
                "reported_identity_backfill_audit",
                side_effect=[
                    {"holdings_scanned": 1, "incomplete_holdings": 0},
                    {"needed": False, "incomplete_holdings": 0},
                ],
            ),
            mock.patch.object(
                pipeline,
                "rebuild_reported_identity_from_sec",
                return_value=backfill_result,
            ),
            mock.patch.object(
                pipeline,
                "collect_security_master_universe",
                return_value=[
                    {"cusip": "111111118", "instrument_type": "EQUITY"}
                ],
            ),
            mock.patch.object(
                pipeline,
                "load_security_master_pair",
                return_value=(
                    {"audit": {"ftd_coverage_ratio": 1.0}},
                    {"schema_version": 2},
                ),
            ),
            mock.patch.object(
                pipeline,
                "refresh_security_master",
                return_value=refresh_result,
            ),
            mock.patch.object(
                pipeline,
                "_refresh_sec_fund_series_evidence",
                side_effect=lambda result, _universe, **_kwargs: result,
            ),
            mock.patch.object(
                pipeline,
                "_refresh_sec_edgar_exceptions",
                return_value=transient,
            ),
            mock.patch.object(
                pipeline,
                "audit_security_master",
                return_value=accepted_audit,
            ),
            mock.patch.object(
                pipeline,
                "_mark_security_master_rebuild_complete",
            ) as mark_complete,
            mock.patch.object(
                pipeline,
                "save_security_master_pair",
            ) as save_pair,
        ):
            result = pipeline.refresh_sec_security_master_from_funds(
                full_rebuild=True
            )

        self.assertEqual(transient.master, result.master)
        self.assertEqual(transient.state, result.state)
        self.assertEqual(accepted_audit, result.acceptance)
        self.assertIsNone(
            result.master["records"]["111111118|EQUITY"].get("ticker")
        )
        save_pair.assert_called_once_with(
            transient.master,
            transient.state,
            master_path=pipeline.SEC_SECURITY_MASTER_PATH,
            source_state_path=pipeline.SEC_SOURCE_STATE_PATH,
        )
        mark_complete.assert_called_once()

    def test_incremental_ftd_gap_keeps_verified_master_and_new_id_unresolved(
        self,
    ) -> None:
        prior_state = {
            "schema_version": 2,
            "updated_at": "2026-08-31T00:00:00Z",
            "sources": {},
        }
        prior_master = {
            "policy": {
                "recent_window_days": 31,
                "max_evidence_age_days": 395,
                "min_confirmation_dates": 2,
            },
            "audit": {"ftd_coverage_ratio": 1.0},
            "records": {
                "037833100|EQUITY": {"mapping_status": "resolved"},
            },
        }
        fallback_master = {
            **prior_master,
            "records": {
                **prior_master["records"],
                "111111118|EQUITY": {
                    "mapping_status": "unresolved",
                    "ticker": None,
                },
            },
            "summary": {
                "resolved": 1,
                "ambiguous": 0,
                "malformed_as_filed": 0,
            },
        }
        acceptance = {
            "ok": True,
            "issues": [],
            "ftd_coverage_ratio": 1.0,
            "ftd_evidenced_official_cusip_count": 1,
            "active_non_option_official_cusip_count": 1,
        }
        gap = pipeline.SecurityMasterAcceptanceError({
            "ok": False,
            "issues": ["ftd_filter_universe_incomplete"],
        })
        universe = [{
            "cusip": "111111118",
            "instrument_type": "EQUITY",
            "issuer": "EXAMPLE INC",
            "security_class": "COM",
        }]

        with (
            mock.patch.object(
                pipeline,
                "collect_security_master_universe",
                return_value=universe,
            ),
            mock.patch.object(
                pipeline,
                "load_security_master_pair",
                return_value=(prior_master, prior_state),
            ),
            mock.patch.object(
                pipeline,
                "refresh_security_master",
                side_effect=gap,
            ),
            mock.patch.object(
                pipeline,
                "rebuild_sec_security_master",
                return_value=fallback_master,
            ) as rebuild,
            mock.patch.object(
                pipeline,
                "audit_security_master",
                return_value=acceptance,
            ),
            mock.patch.object(
                pipeline,
                "save_security_master_pair",
            ) as save_pair,
            mock.patch.object(
                pipeline,
                "_refresh_sec_fund_series_evidence",
                side_effect=lambda result, _universe: result,
            ) as refresh_fund_series,
            mock.patch.object(
                pipeline,
                "_refresh_sec_edgar_exceptions",
                side_effect=lambda result, _universe: result,
            ) as refresh_edgar,
        ):
            result = pipeline.refresh_sec_security_master_from_funds(
                full_rebuild=False,
            )

        self.assertIsNotNone(result)
        self.assertEqual(
            "unresolved",
            result.master["records"]["111111118|EQUITY"]["mapping_status"],
        )
        self.assertIn("remain unresolved", result.errors[0])
        save_pair.assert_called_once_with(
            fallback_master,
            prior_state,
            master_path=pipeline.SEC_SECURITY_MASTER_PATH,
            source_state_path=pipeline.SEC_SOURCE_STATE_PATH,
        )
        rebuild.assert_called_once()
        refresh_fund_series.assert_not_called()
        refresh_edgar.assert_not_called()

    def test_fund_series_batch_failure_keeps_exact_prior_state(self) -> None:
        first_cik = "0000000001"
        second_cik = "0000000002"
        source_state = {
            "sources": {
                "https://www.sec.gov/files/company_tickers_mf.json": {
                    "url": "https://www.sec.gov/files/company_tickers_mf.json",
                    "kind": "sec_fund_tickers",
                    "fund_records": [
                        {
                            "symbol": "AAA",
                            "cik": first_cik,
                            "series_id": "S1",
                            "class_id": "C1",
                        },
                        {
                            "symbol": "BBB",
                            "cik": second_cik,
                            "series_id": "S2",
                            "class_id": "C2",
                        },
                    ],
                },
            }
        }
        prior_master = {
            "records": {
                "111111118|EQUITY": {
                    "mapping_status": "resolved",
                    "ticker": "AAA",
                },
                "222222226|EQUITY": {
                    "mapping_status": "resolved",
                    "ticker": "BBB",
                },
            },
            "policy": {},
        }
        result = RefreshResult(
            master=prior_master,
            state=source_state,
            changed=False,
            refreshed_urls=(),
            retained_urls=(),
            errors=(),
            acceptance={"ok": True, "issues": []},
        )
        first_url = pipeline.sec_fund_series_url(first_cik)
        second_url = pipeline.sec_fund_series_url(second_cik)
        page = b"""
            <table>
              <tr><th>ID</th><th>Name</th></tr>
              <tr><td><a href='?CIK=S1'>S1</a></td><td>Fund One</td></tr>
              <tr><td><a href='?CIK=C1'>C1</a></td><td>Class One</td></tr>
            </table>
        """

        def fetch(url: str) -> bytes:
            if url == first_url:
                return page
            self.assertEqual(second_url, url)
            raise OSError("temporary SEC page failure")

        with (
            mock.patch.object(
                pipeline,
                "save_security_master_pair",
            ) as save_pair,
            mock.patch.object(pipeline, "rebuild_sec_security_master") as rebuild,
        ):
            refreshed = pipeline._refresh_sec_fund_series_evidence(
                result,
                [],
                refreshed_at=datetime(2026, 9, 1, tzinfo=timezone.utc),
                fetcher=fetch,
            )

        self.assertIs(source_state, refreshed.state)
        self.assertEqual(prior_master, refreshed.master)
        self.assertTrue(any("temporary SEC page failure" in error for error in refreshed.errors))
        save_pair.assert_not_called()
        rebuild.assert_not_called()

    def test_fund_series_pair_interrupt_preserves_primary_error(self) -> None:
        cik = "0000000001"
        fund_tickers_url = (
            "https://www.sec.gov/files/company_tickers_mf.json"
        )
        source_state = {
            "updated_at": "2026-08-01T00:00:00Z",
            "sources": {
                fund_tickers_url: {
                    "url": fund_tickers_url,
                    "kind": "sec_fund_tickers",
                    "fund_records": [
                        {
                            "symbol": "AAA",
                            "cik": cik,
                            "series_id": "S1",
                            "class_id": "C1",
                        },
                    ],
                },
            },
        }
        prior_master = {
            "records": {
                "111111118|EQUITY": {
                    "mapping_status": "resolved",
                    "ticker": "AAA",
                },
            },
            "policy": {},
        }
        rebuilt_master = {
            **prior_master,
            "generation": "candidate",
        }
        result = RefreshResult(
            master=prior_master,
            state=source_state,
            changed=False,
            refreshed_urls=(),
            retained_urls=(),
            errors=(),
            acceptance={"ok": True, "issues": []},
        )
        page = b"""
            <table>
              <tr><th>ID</th><th>Name</th></tr>
              <tr><td><a href='?CIK=S1'>S1</a></td><td>Fund One</td></tr>
              <tr><td><a href='?CIK=C1'>C1</a></td><td>Class One</td></tr>
            </table>
        """
        interruption = KeyboardInterrupt("master write interrupted")

        with (
            mock.patch.object(
                pipeline,
                "rebuild_sec_security_master",
                return_value=rebuilt_master,
            ),
            mock.patch.object(
                pipeline,
                "audit_security_master",
                return_value={"ok": True, "issues": []},
            ),
            mock.patch.object(
                pipeline,
                "save_security_master_pair",
                side_effect=interruption,
            ) as save_pair,
            self.assertRaises(KeyboardInterrupt) as raised,
        ):
            pipeline._refresh_sec_fund_series_evidence(
                result,
                [],
                refreshed_at=datetime(2026, 9, 1, tzinfo=timezone.utc),
                fetcher=lambda _url: page,
            )

        self.assertIs(interruption, raised.exception)
        save_pair.assert_called_once()
        self.assertIs(rebuilt_master, save_pair.call_args.args[0])
        candidate_state = save_pair.call_args.args[1]
        self.assertIsNot(source_state, candidate_state)
        self.assertEqual(
            "2026-09-01T00:00:00Z",
            candidate_state["updated_at"],
        )
        self.assertEqual(
            pipeline.SEC_SECURITY_MASTER_PATH,
            save_pair.call_args.kwargs["master_path"],
        )
        self.assertEqual(
            pipeline.SEC_SOURCE_STATE_PATH,
            save_pair.call_args.kwargs["source_state_path"],
        )


if __name__ == "__main__":
    unittest.main()
