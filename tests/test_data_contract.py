import json
import re
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import data_contract
import pipeline
import validate_data
from scripts import annotate_ticker_health


ROOT = Path(__file__).resolve().parents[1]


class GeneratedDataContractTests(unittest.TestCase):
    def test_strict_iso_date_rejects_noncanonical_and_invalid_dates(self) -> None:
        for value in ("2024-02-29", "2026-09-05", "2000-01-01", "9999-12-31"):
            self.assertTrue(validate_data.is_strict_iso_date(value), value)
        for value in (
            None, 20260905, {}, "", "2026-9-5", "20260905", "2026-W36-6",
            "2026-02-29", "2026-13-01", "2026-00-01", "2026-09-00",
            "2026-09-05 ", " 2026-09-05", "2026-09-05T00:00:00Z",
        ):
            self.assertFalse(validate_data.is_strict_iso_date(value), repr(value))

    def test_registry_loader_prefers_snapshot_data_over_stale_cache(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            data_registry = root / "data/cusip_registry.json"
            cache_registry = root / ".cache/cusip_registry.json"
            data_registry.parent.mkdir()
            cache_registry.parent.mkdir()
            data_registry.write_text(
                '{"037833100":{"ticker":"AAPL"}}\n',
                encoding="utf-8",
            )
            cache_registry.write_text(
                '{"037833100":{"ticker":"STALE"}}\n',
                encoding="utf-8",
            )

            with mock.patch.multiple(
                pipeline,
                CUSIP_REGISTRY_PATH=cache_registry,
                LEGACY_CUSIP_REGISTRY_PATH=data_registry,
            ):
                registry = pipeline.load_cusip_registry()

            self.assertEqual("AAPL", registry["037833100"]["ticker"])

    def test_ticker_health_annotations_separate_backlog_from_action(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = Path(tmpdir) / "ticker_health.json"
            report_path.write_text(json.dumps({
                "label_coverage": {
                    "total": 5,
                    "labeled": 5,
                    "unlabeled": 0,
                },
                "summary": {
                    "unresolved": 2,
                    "synthetic_identifier": 3,
                    "option_family_artifact": 1,
                },
                "buckets": {
                    "unresolved": [
                        {
                            "cusip": "111111111",
                            "instrument_type": "NOTE",
                            "last_seen": "2026-03-31",
                        },
                        {
                            "cusip": "222222222",
                            "instrument_type": "EQUITY",
                            "last_seen": "2026-06-30",
                        },
                    ],
                    "synthetic_identifier": [
                        {
                            "cusip": "000000NAN",
                            "max_value": 100,
                            "last_seen": "2026-06-30",
                        },
                        {
                            "cusip": "0LOOKITUP",
                            "max_value": 0,
                            "last_seen": "2026-06-30",
                        },
                        {
                            "cusip": "MONEYMRKT",
                            "max_value": 100,
                            "last_seen": "2026-06-30",
                        },
                    ],
                    "option_family_artifact": [{
                        "cusip": "333333901",
                        "last_seen": "2026-06-30",
                    }],
                },
            }))
            with (
                mock.patch.object(
                    annotate_ticker_health.sys,
                    "argv",
                    ["annotate_ticker_health.py", str(report_path)],
                ),
                mock.patch("builtins.print") as print_mock,
            ):
                self.assertEqual(0, annotate_ticker_health.main())

        lines = [call.args[0] for call in print_mock.call_args_list]
        self.assertTrue(any(
            line.startswith(
                "::notice title=ticker_health_backlog::1 stable debt/option"
            )
            for line in lines
        ), lines)
        self.assertTrue(any(
            line.startswith(
                "::notice title=ticker_health::1 current unresolved"
            )
            and "222222222=∅" in line
            for line in lines
        ), lines)
        self.assertTrue(any(
            "::notice title=ticker_health::1 current nonzero synthetic"
            in line
            for line in lines
        ), lines)
        self.assertFalse(any(
            "::warning" in line and "MONEYMRKT" in line
            for line in lines
        ), lines)
        self.assertTrue(any(
            "::notice title=ticker_health_backlog::2 stale, zero-value, or cash"
            in line
            for line in lines
        ), lines)
        self.assertTrue(any(
            "::notice title=ticker_health::1 option_family_artifact"
            in line
            for line in lines
        ), lines)

    def test_current_contract_is_explicitly_version_five(self) -> None:
        self.assertEqual(5, data_contract.DATA_CONTRACT_VERSION)

    def test_registry_consensus_does_not_replace_exact_blank_issuer(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir) / "funds"
            funds_dir.mkdir()
            (funds_dir / "1643792.json").write_text(json.dumps({
                "cik": 1643792,
                "quarters": [{
                    "report_date": "2026-06-30",
                    "holdings": [{
                        "cusip": "M46528101",
                        "reported_cusip": "M46528101",
                        "issuer": "Frontline plc",
                        "reported_issuer": "",
                        "class": "COM",
                        "reported_class": "COM",
                        "holding_type": "EQUITY",
                        "value": 0,
                    }],
                }],
            }))
            with (
                mock.patch.object(pipeline, "FUNDS_DIR", funds_dir),
                mock.patch.object(
                    pipeline,
                    "load_security_master",
                    return_value={"records": {}},
                ),
                mock.patch.object(pipeline, "save_cusip_registry"),
            ):
                registry = pipeline.build_cusip_registry()

        entry = registry["M46528101"]
        self.assertEqual("", entry["dominant_issuer"])
        self.assertEqual("COM", entry["dominant_class"])
        self.assertEqual("UNIDENTIFIED EQUITY SECURITY", entry["name"])
        self.assertNotIn("Frontline", json.dumps(entry))

    def test_registry_issuer_skips_identifiers_in_exact_evidence(self) -> None:
        identifier = "436CVR021"
        record = {
            "cusip": identifier,
            "official_13f": {"records": [{"issuer": identifier}]},
            "reported_issuer": identifier,
            "reported_issuers": [identifier, "HOLOGIC INC"],
        }
        self.assertEqual(
            ("HOLOGIC INC", "sec_13f_filer_consensus"),
            pipeline._master_record_issuer(record, identifier),
        )
        record.update({
            "instrument_type": "EQUITY",
            "mapping_status": "ambiguous",
            "ticker": None,
            "reported_classes": ["13F EXEMPT"],
            "security_label": f"{identifier} — 13F EXEMPT",
        })
        evidence = {identifier: {
            "issuer_value": {"HOLOGIC INC": 1},
            "class_value": {"RIGHT": 1},
            "instrument_type_count": {"EQUITY": 1},
            "instrument_type_value": {"EQUITY": 1},
        }}
        with tempfile.TemporaryDirectory() as temporary:
            with (
                mock.patch.object(pipeline, "FUNDS_DIR", Path(temporary)),
                mock.patch.object(pipeline, "_aggregate_cusip_evidence", return_value=evidence),
                mock.patch.object(pipeline, "load_security_master", return_value={
                    "records": {f"{identifier}|EQUITY": record},
                }),
                mock.patch.object(pipeline, "save_cusip_registry"),
            ):
                entry = pipeline.build_cusip_registry()[identifier]
        self.assertEqual("HOLOGIC INC", entry["name"])
        self.assertEqual(
            "HOLOGIC INC — 13F EXEMPT — 436CVR021", entry["security_label"]
        )
        self.assertIsNone(entry["ticker"])
        record["reported_issuers"] = [identifier]
        self.assertEqual(("", ""), pipeline._master_record_issuer(record, identifier))

    def test_fund_share_markers_exclude_explicit_options_but_not_bad_debt(self) -> None:
        registry = {
            f"00000000{number}": {
                "type": kind,
                "name": "EXAMPLE FUND",
                "security_label": "EXAMPLE FUND — ETF",
                "label_source": "sec_13f_filer_consensus",
                "security_kind": "ETF",
                "security_kind_source": "filer_metadata",
                "dominant_class": "ETF",
            }
            for number, kind in enumerate(("EQUITY", "CALL", "PUT", "OPT"), 1)
        }
        with tempfile.TemporaryDirectory() as temporary:
            labels_path = Path(temporary) / "labels.json"
            with (
                mock.patch.object(pipeline, "SECURITY_LABELS_PATH", labels_path),
                mock.patch.object(validate_data, "SECURITY_LABELS_PATH", labels_path),
            ):
                pipeline.write_security_labels(registry)
                self.assertEqual(
                    ["000000001"], json.loads(labels_path.read_text())["fund_identities"]
                )
                errors = []
                validate_data.validate_security_labels(registry, errors)
                self.assertEqual([], errors)
                registry["000000002"]["type"] = "NOTE"
                pipeline.write_security_labels(registry)
                validate_data.validate_security_labels(registry, errors)
                self.assertTrue(any("fund_identities differ" in error for error in errors))

    def test_filer_fund_kind_does_not_reclassify_saved_debt_from_issuer_alone(self) -> None:
        entry = {"name": "ISHARES TR", "dominant_class": "IBONDS DEC 2029",
                 "security_kind": "BOND", "ticker": None}
        for kind in ("NOTE", "PREF", "WARRANT"):
            self.assertIsNone(validate_data.expected_filer_fund_kind({**entry, "type": kind}))
        for kind in ("EQUITY", "CALL", "PUT"):
            self.assertEqual("ETF", validate_data.expected_filer_fund_kind({**entry, "type": kind}))

    def test_validator_requires_exact_current_integer_version(self) -> None:
        errors: list[str] = []
        validate_data.validate_data_contract(
            {"data_contract_version": data_contract.DATA_CONTRACT_VERSION},
            "fixture.json",
            errors,
        )
        self.assertEqual([], errors)

        for invalid in (
            {},
            {"data_contract_version": None},
            {"data_contract_version": True},
            {"data_contract_version": "1"},
            {"data_contract_version": 1},
            {"data_contract_version": data_contract.DATA_CONTRACT_VERSION + 1},
        ):
            with self.subTest(invalid=invalid):
                errors = []
                validate_data.validate_data_contract(
                    invalid,
                    "fixture.json",
                    errors,
                )
                self.assertEqual(1, len(errors))
                self.assertIn("unsupported data_contract_version", errors[0])

    def test_generator_versions_both_bootstrap_indexes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir) / "data"
            funds_dir = data_dir / "funds"
            funds_dir.mkdir(parents=True)
            stocks_dir = data_dir / "stocks"
            registry_path = data_dir / "cusip_registry.json"

            with mock.patch.multiple(
                pipeline,
                DATA_DIR=data_dir,
                FUNDS_DIR=funds_dir,
                STOCKS_DIR=stocks_dir,
                INDEX_PATH=data_dir / "index.json",
                FUNDS_INDEX_PATH=data_dir / "funds-index.json",
                CUSIP_REGISTRY_PATH=registry_path,
                LEGACY_CUSIP_REGISTRY_PATH=registry_path,
            ):
                pipeline.regenerate_stock_files_and_index()
                index = json.loads((data_dir / "index.json").read_text())
                funds_index = json.loads(
                    (data_dir / "funds-index.json").read_text()
                )
                marker = "2000-01-01T00:00:00Z"
                index["last_updated"] = marker
                funds_index["last_updated"] = marker
                (data_dir / "index.json").write_text(json.dumps(index))
                (data_dir / "funds-index.json").write_text(
                    json.dumps(funds_index)
                )
                pipeline.regenerate_stock_files_and_index()

            index = json.loads((data_dir / "index.json").read_text())
            funds_index = json.loads(
                (data_dir / "funds-index.json").read_text()
            )
            for artifact in (index, funds_index):
                self.assertEqual(
                    data_contract.DATA_CONTRACT_VERSION,
                    artifact["data_contract_version"],
                )
                self.assertRegex(
                    artifact["fund_data_revision"],
                    r"^[0-9a-f]{64}$",
                )
                self.assertEqual(marker, artifact["last_updated"])

            errors: list[str] = []
            validate_data.validate_funds_index(
                funds_index,
                index,
                errors,
                {},
            )
            self.assertEqual([], errors)

            changed_fund = funds_dir / "1.json"
            changed_fund.write_text('{"cik":1,"quarters":[]}')
            errors.clear()
            validate_data.validate_funds_index(
                funds_index,
                index,
                errors,
                {"1": changed_fund},
            )
            self.assertTrue(any(
                "does not match the checked-in fund payloads" in error
                for error in errors
            ))

            funds_index["last_updated"] = "not-a-timestamp"
            index["last_updated"] = "not-a-timestamp"
            errors.clear()
            validate_data.validate_funds_index(
                funds_index,
                index,
                errors,
                {},
            )
            self.assertTrue(any(
                "must use strict UTC" in error for error in errors
            ))

    def test_noop_reports_preserve_semantic_timestamps(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir) / "data"
            funds_dir = data_dir / "funds"
            funds_dir.mkdir(parents=True)
            state_path = data_dir / "pipeline_state.json"
            health_path = data_dir / "ticker_health.json"
            marker = "2000-01-01T00:00:00Z"
            (funds_dir / "123.json").write_text(json.dumps({
                "cik": 123,
                "name": "Example",
                "quarters": [{
                    "report_date": "2026-03-31",
                    "holdings": [{
                        "cusip": "037833100",
                        "ticker": "AAPL",
                        "issuer": "APPLE INC",
                        "value": 100,
                    }],
                }],
            }))

            state = {
                "_processed_set": {"0000000123-26-000001"},
                "_quarantined": {},
            }
            with mock.patch.multiple(
                pipeline,
                STATE_PATH=state_path,
                LEGACY_STATE_PATH=data_dir / "missing-legacy-state.json",
                FUNDS_DIR=funds_dir,
                TICKER_HEALTH_PATH=health_path,
                load_cusip_registry=mock.Mock(return_value={
                    "037833100": {
                        "ticker": "AAPL",
                        "type": "EQUITY",
                        "security_label": "AAPL",
                    },
                }),
            ):
                pipeline.save_state(state)
                saved_state = json.loads(state_path.read_text())
                saved_state["last_run"] = marker
                state_path.write_text(json.dumps(saved_state))
                pipeline.save_state(pipeline.load_state())

                pipeline.write_ticker_health_report()
                health = json.loads(health_path.read_text())
                health["generated_at"] = marker
                health_path.write_text(json.dumps(health))
                pipeline.write_ticker_health_report()

            self.assertEqual(
                marker,
                json.loads(state_path.read_text())["last_run"],
            )
            self.assertEqual(
                marker,
                json.loads(health_path.read_text())["generated_at"],
            )
            self.assertEqual(
                {
                    "total": 1,
                    "labeled": 1,
                    "unlabeled": 0,
                    "unlabeled_samples": [],
                },
                json.loads(health_path.read_text())["label_coverage"],
            )

    def test_ticker_health_separates_likely_option_family_artifacts(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir) / "data"
            funds_dir = data_dir / "funds"
            funds_dir.mkdir(parents=True)
            health_path = data_dir / "ticker_health.json"
            (funds_dir / "123.json").write_text(json.dumps({
                "cik": 123,
                "quarters": [{
                    "report_date": "2026-03-31",
                    "holdings": [
                        {
                            "cusip": "123456101",
                            "ticker": "EXM",
                            "issuer": "EXAMPLE INC",
                            "class": "COM",
                            "value": 100,
                            "holding_type": "EQUITY",
                        },
                        {
                            "cusip": "123456901",
                            "ticker": None,
                            "issuer": "EXAMPLE INC",
                            "class": "COM",
                            "value": 50,
                            "holding_type": "EQUITY",
                        },
                    ],
                }],
            }))
            registry = {
                "123456101": {
                    "ticker": "EXM",
                    "type": "EQUITY",
                    "security_label": "EXM",
                },
                "123456901": {
                    "ticker": None,
                    "type": "EQUITY",
                    "security_label": "EXAMPLE INC",
                },
            }
            with mock.patch.multiple(
                pipeline,
                DATA_DIR=data_dir,
                FUNDS_DIR=funds_dir,
                TICKER_HEALTH_PATH=health_path,
                load_cusip_registry=mock.Mock(return_value=registry),
            ):
                report = pipeline.write_ticker_health_report()

        self.assertEqual(1, report["summary"]["option_family_artifact"])
        self.assertNotIn("unresolved", report["summary"])
        self.assertEqual(
            "123456901",
            report["buckets"]["option_family_artifact"][0]["cusip"],
        )

    def test_ticker_health_ignores_unproven_holding_ticker(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir) / "data"
            funds_dir = data_dir / "funds"
            funds_dir.mkdir(parents=True)
            health_path = data_dir / "ticker_health.json"
            (funds_dir / "123.json").write_text(json.dumps({
                "cik": 123,
                "quarters": [{
                    "report_date": "2026-03-31",
                    "holdings": [{
                        "cusip": "037833100",
                        "ticker": "UNPROVEN",
                        "issuer": "Unproven Vendor Label",
                        "reported_issuer": "APPLE INC",
                        "class": "COM",
                        "value": 100,
                        "holding_type": "EQUITY",
                    }],
                }],
            }))
            with mock.patch.multiple(
                pipeline,
                DATA_DIR=data_dir,
                FUNDS_DIR=funds_dir,
                TICKER_HEALTH_PATH=health_path,
                load_cusip_registry=mock.Mock(return_value={}),
            ):
                report = pipeline.write_ticker_health_report()

        row = report["buckets"]["unresolved"][0]
        self.assertIsNone(row["ticker"])
        self.assertEqual("APPLE INC", row["issuer"])
        self.assertNotIn("ambiguous", report["buckets"])

    def test_ticker_health_accepts_sec_verified_reserved_word_ticker(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir) / "data"
            funds_dir = data_dir / "funds"
            funds_dir.mkdir(parents=True)
            health_path = data_dir / "ticker_health.json"
            (data_dir / "company_tickers.json").write_text(json.dumps({
                "0": {
                    "ticker": "NOTE",
                    "title": "FiscalNote Holdings, Inc.",
                },
            }))
            (funds_dir / "123.json").write_text(json.dumps({
                "cik": 123,
                "quarters": [{
                    "report_date": "2026-03-31",
                    "holdings": [{
                        "cusip": "337655302",
                        "ticker": None,
                        "issuer": "FISCALNOTE HOLDINGS, INC.",
                        "class": "CL A NEW",
                        "value": 100,
                    }],
                }],
            }))
            with mock.patch.multiple(
                pipeline,
                DATA_DIR=data_dir,
                FUNDS_DIR=funds_dir,
                TICKER_HEALTH_PATH=health_path,
                load_cusip_registry=mock.Mock(return_value={
                    "337655302": {
                        "ticker": "NOTE",
                        "name": "FiscalNote Holdings, Inc.",
                        "dominant_issuer": "FISCALNOTE HOLDINGS, INC.",
                        "type": "EQUITY",
                        "security_label": "NOTE",
                    },
                }),
            ):
                report = pipeline.write_ticker_health_report()

            self.assertNotIn("suspicious_symbol", report["buckets"])
            self.assertNotIn("unresolved", report["buckets"])

    def test_ticker_health_accepts_proven_sec_ftd_reserved_word_ticker(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir) / "data"
            funds_dir = data_dir / "funds"
            funds_dir.mkdir(parents=True)
            health_path = data_dir / "ticker_health.json"
            (data_dir / "company_tickers.json").write_text(json.dumps({
                "0": {
                    "ticker": "PFD",
                    "title": (
                        "FLAHERTY & CRUMRINE PREFERRED & INCOME FUND INC"
                    ),
                },
            }))
            (funds_dir / "123.json").write_text(json.dumps({
                "cik": 123,
                "quarters": [{
                    "report_date": "2026-06-30",
                    "holdings": [{
                        "cusip": "338480106",
                        "ticker": None,
                        "issuer": "FLAHERTY & CRUMRINE PFD INCO",
                        "class": "COM",
                        "value": 100,
                    }],
                }],
            }))
            registry_entry = {
                "ticker": "PFD",
                "ticker_source": "sec_ftd",
                "ticker_as_of": "2026-08-06",
                "mapping_status": "resolved",
                "name": "FLAHERTY & CRUMRINE PFD INCO",
                "dominant_issuer": "FLAHERTY & CRUMRINE PFD INCO",
                "type": "EQUITY",
                "security_label": "FLAHERTY & CRUMRINE PFD INCO — COM",
                "security_kind": "COMMON",
                "security_kind_source": "sec_13f_list",
                "sources": [
                    "sec_13f_list",
                    "sec_company_tickers",
                    "sec_ftd",
                ],
            }
            with mock.patch.multiple(
                pipeline,
                DATA_DIR=data_dir,
                FUNDS_DIR=funds_dir,
                TICKER_HEALTH_PATH=health_path,
                load_cusip_registry=mock.Mock(return_value={
                    "338480106": registry_entry,
                }),
            ):
                report = pipeline.write_ticker_health_report()

            self.assertNotIn("suspicious_symbol", report["buckets"])
            self.assertNotIn("unresolved", report["buckets"])

    def test_ticker_health_keeps_unproven_reserved_word_ticker_suspicious(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir) / "data"
            funds_dir = data_dir / "funds"
            funds_dir.mkdir(parents=True)
            health_path = data_dir / "ticker_health.json"
            (data_dir / "company_tickers.json").write_text(json.dumps({
                "0": {
                    "ticker": "PFD",
                    "title": (
                        "FLAHERTY & CRUMRINE PREFERRED & INCOME FUND INC"
                    ),
                },
            }))
            (funds_dir / "123.json").write_text(json.dumps({
                "cik": 123,
                "quarters": [{
                    "report_date": "2026-06-30",
                    "holdings": [{
                        "cusip": "338480106",
                        "ticker": None,
                        "issuer": "FLAHERTY & CRUMRINE PFD INCO",
                        "class": "COM",
                        "value": 100,
                    }],
                }],
            }))
            registry_entry = {
                "ticker": "PFD",
                "ticker_source": "sec_ftd",
                "ticker_as_of": "2026-08-06",
                "mapping_status": "resolved",
                "name": "FLAHERTY & CRUMRINE PFD INCO",
                "dominant_issuer": "FLAHERTY & CRUMRINE PFD INCO",
                "type": "EQUITY",
                "security_label": "FLAHERTY & CRUMRINE PFD INCO — COM",
                "security_kind": "COMMON",
                "security_kind_source": "filer_metadata",
                "sources": ["sec_company_tickers", "sec_ftd"],
            }
            with mock.patch.multiple(
                pipeline,
                DATA_DIR=data_dir,
                FUNDS_DIR=funds_dir,
                TICKER_HEALTH_PATH=health_path,
                load_cusip_registry=mock.Mock(return_value={
                    "338480106": registry_entry,
                }),
            ):
                report = pipeline.write_ticker_health_report()

            self.assertEqual(1, report["summary"]["suspicious_symbol"])
            self.assertEqual(
                "338480106",
                report["buckets"]["suspicious_symbol"][0]["cusip"],
            )

    def test_frontend_version_and_fail_closed_guard_match_python(self) -> None:
        html = (ROOT / "index.html").read_text()
        version = re.search(
            r"const DATA_CONTRACT_VERSION\s*=\s*(\d+);",
            html,
        )
        self.assertIsNotNone(version)
        self.assertEqual(
            data_contract.DATA_CONTRACT_VERSION,
            int(version.group(1)),
        )
        self.assertIn("class DataContractMismatchError", html)
        self.assertIn("assertCompatibleDataContract(data", html)
        self.assertIn("showDataMaintenance()", html)
        self.assertIn(
            'fetchJson("data/funds-index.json")',
            html,
        )
        self.assertIn(
            'fetchJson("data/index.json")',
            html,
        )
        self.assertIn('fetch(url, { cache: "no-cache" })', html)

    def test_security_label_artifact_covers_registry_without_raw_cusips(
        self,
    ) -> None:
        registry = {
            "037833100": {
                "ticker": "AAPL",
                "name": "Apple Inc.",
                "security_label": "AAPL",
                "label_source": "canonical_ticker",
                "dominant_class": "COM",
            },
            "65339F655": {
                "ticker": None,
                "name": "NEXTERA ENERGY INC",
                "security_label": "NEE 7.375 02/15/29",
                "label_source": "sec_13f_list",
                "security_kind": "PREFERRED",
                "security_kind_source": "sec_13f_list",
                "dominant_class": "UNIT 02/15/2029",
            },
            "46222L116": {
                "ticker": None,
                "name": "IONQ INC",
                "security_label": "IONQ/WS — WARRANT EXP 10/01/26",
                "label_source": "sec_13f_list",
                "security_kind": "WARRANT",
                "security_kind_source": "sec_13f_list",
                "dominant_class": "0",
            },
            "464286772": {
                "ticker": "EWY",
                "name": "ISHARES INC",
                "security_label": "EWY",
                "label_source": "sec_ftd",
                "security_kind": "ETF",
                "security_kind_source": "sec_fund_series",
                "product_name": "ISHARES MSCI SOUTH KOREA ETF",
                "product_name_source": "sec_fund_series",
                "dominant_class": "MSCI STH KOR ETF",
            },
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            labels_path = Path(tmpdir) / "security_labels.json"
            labels_path.write_text(json.dumps({
                "data_contract_version": data_contract.DATA_CONTRACT_VERSION,
                "labels": {
                    cusip: entry["security_label"]
                    for cusip, entry in registry.items()
                },
                "kinds": {
                    "65339F655": "PREFERRED",
                    "46222L116": "WARRANT",
                    "464286772": "ETF",
                },
                "fund_identities": ["464286772"],
                "product_names": {
                    "464286772": "ISHARES MSCI SOUTH KOREA ETF",
                },
            }))
            with mock.patch.object(
                validate_data,
                "SECURITY_LABELS_PATH",
                labels_path,
            ):
                errors: list[str] = []
                labels = validate_data.validate_security_labels(
                    registry,
                    errors,
                )
                self.assertEqual([], errors)
                self.assertEqual(
                    "NEE 7.375 02/15/29",
                    labels["65339F655"],
                )

                payload = json.loads(labels_path.read_text())
                payload["labels"]["65339F655"] = "65339F655"
                labels_path.write_text(json.dumps(payload))
                errors.clear()
                validate_data.validate_security_labels(registry, errors)
                self.assertTrue(
                    any("non-canonical labels" in error for error in errors),
                    errors,
                )

                payload["labels"]["65339F655"] = "NEE 7.375 02/15/29"
                payload["kinds"]["65339F655"] = "PREF"
                labels_path.write_text(json.dumps(payload))
                errors.clear()
                validate_data.validate_security_labels(registry, errors)
                self.assertTrue(
                    any("invalid security kinds" in error for error in errors),
                    errors,
                )

                payload["kinds"]["65339F655"] = "PREFERRED"
                labels_path.write_text(json.dumps(payload))
                payload["product_names"]["464286772"] = "464286772"
                labels_path.write_text(json.dumps(payload))
                errors.clear()
                validate_data.validate_security_labels(registry, errors)
                self.assertTrue(
                    any("invalid product names" in error for error in errors),
                    errors,
                )

                payload["product_names"]["464286772"] = (
                    "ISHARES MSCI SOUTH KOREA ETF"
                )
                registry["464287655"] = {
                    "ticker": "IWM",
                    "name": "ISHARES TR",
                    "security_label": "IWM",
                    "label_source": "sec_ftd",
                    "security_kind": "ETF",
                    "security_kind_source": "sec_fund_series",
                    "product_name": "ISHARES MSCI SOUTH KOREA ETF",
                    "product_name_source": "sec_fund_series",
                    "dominant_class": "RUSSELL 2000 ETF",
                }
                payload["labels"]["464287655"] = "IWM"
                payload["kinds"]["464287655"] = "ETF"
                payload["product_names"]["464287655"] = (
                    "ISHARES MSCI SOUTH KOREA ETF"
                )
                payload["fund_identities"].append("464287655")
                labels_path.write_text(json.dumps(payload))
                errors.clear()
                validate_data.validate_security_labels(registry, errors)
                self.assertTrue(
                    any(
                        "ambiguous fund product names" in error
                        for error in errors
                    ),
                    errors,
                )
                registry["464287655"]["name"] = (
                    "GENERIC ISSUER SPONSORED ADR"
                )
                registry["464287655"]["security_kind"] = "COMMON"
                registry["464287655"]["security_kind_source"] = "sec_13f_list"
                payload["kinds"]["464287655"] = "COMMON"
                labels_path.write_text(json.dumps(payload))
                errors.clear()
                validate_data.validate_security_labels(registry, errors)
                self.assertTrue(
                    any(
                        "depositary receipts as COMMON" in error
                        for error in errors
                    ),
                    errors,
                )
                registry.pop("464287655")
                payload["labels"].pop("464287655")
                payload["kinds"].pop("464287655")
                payload["product_names"].pop("464287655")
                payload["fund_identities"].remove("464287655")
                labels_path.write_text(json.dumps(payload))
                registry["65339F655"]["name"] = "65339F655"
                errors.clear()
                validate_data.validate_security_labels(registry, errors)
                self.assertTrue(
                    any("raw identifiers as issuer names" in error for error in errors),
                    errors,
                )

    def test_etn_product_name_provenance_is_narrow_and_reproducible(
        self,
    ) -> None:
        identifier = "22542D449"
        product_name = "UBS ETRACS SILVER SHARES COVERED CALL ETNS"
        for product_source, sources in (
            ("filer_issuer", ["filer_dominant"]),
            ("sec_title", ["sec_title"]),
        ):
            with self.subTest(product_source=product_source):
                registry = {
                    identifier: {
                        "name": product_name,
                        "dominant_issuer": product_name,
                        "dominant_class": "ETN",
                        "type": "NOTE",
                        "sources": sources,
                        "security_label": "SLVO",
                        "label_source": "sec_ftd",
                        "security_kind": "ETN",
                        "security_kind_source": "filer_metadata",
                        "product_name": product_name,
                        "product_name_source": product_source,
                    },
                }
                payload = {
                    "data_contract_version": (
                        data_contract.DATA_CONTRACT_VERSION
                    ),
                    "labels": {identifier: "SLVO"},
                    "kinds": {identifier: "ETN"},
                    "fund_identities": [],
                    "product_names": {identifier: product_name},
                }
                with tempfile.TemporaryDirectory() as tmpdir:
                    labels_path = Path(tmpdir) / "security_labels.json"
                    labels_path.write_text(json.dumps(payload))
                    with mock.patch.object(
                        validate_data,
                        "SECURITY_LABELS_PATH",
                        labels_path,
                    ):
                        errors: list[str] = []
                        validate_data.validate_security_labels(
                            registry,
                            errors,
                        )
                        self.assertEqual([], errors)

                        registry[identifier]["security_kind"] = "ETF"
                        registry[identifier][
                            "security_kind_source"
                        ] = "sec_13f_list"
                        payload["kinds"][identifier] = "ETF"
                        labels_path.write_text(json.dumps(payload))
                        errors.clear()
                        validate_data.validate_security_labels(
                            registry,
                            errors,
                        )
                        self.assertTrue(
                            any(
                                "product names with invalid kind or provenance"
                                in error
                                for error in errors
                            ),
                            errors,
                        )

    def test_frontend_rejects_prior_integer_contract_before_routing(
        self,
    ) -> None:
        html = (ROOT / "index.html").read_text()
        start = html.index("const DATA_CONTRACT_VERSION")
        end = html.index("// ---------- formatters ----------", start)
        contract_logic = html[start:end]
        completed = subprocess.run(
            [
                "node",
                "-e",
                (
                    f"{contract_logic}\n"
                    "let rejected = false;\n"
                    "try {\n"
                    "  assertCompatibleDataContract("
                    "{data_contract_version: 1}, 'old-index.json');\n"
                    "} catch (error) {\n"
                    "  rejected = error instanceof DataContractMismatchError;\n"
                    "}\n"
                    "console.log(JSON.stringify({rejected}));\n"
                ),
            ],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual({"rejected": True}, json.loads(completed.stdout))

    @unittest.skipUnless(
        (ROOT / "data/index.json").is_file(),
        "private dataset is not bootstrapped",
    )
    def test_bootstrapped_indexes_use_current_contract(self) -> None:
        for relative_path in ("data/index.json", "data/funds-index.json"):
            with self.subTest(path=relative_path):
                artifact = json.loads((ROOT / relative_path).read_text())
                self.assertEqual(
                    data_contract.DATA_CONTRACT_VERSION,
                    artifact.get("data_contract_version"),
                )
                self.assertIn("proven_split_adjustments", artifact)
        labels = json.loads(
            (ROOT / "data/security_labels.json").read_text()
        )
        self.assertEqual(
            data_contract.DATA_CONTRACT_VERSION,
            labels.get("data_contract_version"),
        )
        self.assertIsInstance(labels.get("labels"), dict)
        self.assertTrue(labels["labels"])
        self.assertIsInstance(labels.get("kinds"), dict)
        self.assertIsInstance(labels.get("product_names"), dict)
        self.assertIsInstance(labels.get("fund_identities"), list)
        registry = json.loads(
            (ROOT / "data/cusip_registry.json").read_text()
        )
        if any("mapping_status" in entry for entry in registry.values()):
            # The legacy snapshot below used manual/vendor-era display
            # overrides. An SEC cutover must instead reconcile every public
            # identity and mapping to its exact provenance-bearing master.
            master = pipeline.load_security_master(
                pipeline.SEC_SECURITY_MASTER_PATH
            )
            errors: list[str] = []
            validate_data.validate_sec_mapping_provenance(registry, errors)
            validate_data.validate_public_registry_provenance(registry, errors)
            validate_data.validate_security_labels(registry, errors)
            validate_data.validate_sec_safety_anchors(registry, master, errors)
            self.assertEqual([], errors)
            for cusip, entry in registry.items():
                with self.subTest(cusip=cusip):
                    key = f"{cusip}|{entry['type']}"
                    self.assertIn(key, master["records"])
                    exact = master["records"][key]
                    for field in (
                        "mapping_status", "ticker", "ticker_source", "ticker_as_of"
                    ):
                        self.assertEqual(exact.get(field), entry.get(field))
                    self.assertEqual(
                        entry["security_label"], labels["labels"][cusip]
                    )
            return

        prudential_note = registry["744320888"]
        self.assertEqual("NOTE", prudential_note["type"])
        self.assertEqual("BOND", prudential_note["security_kind"])
        self.assertEqual(
            "manual_verified",
            prudential_note["security_kind_source"],
        )
        self.assertIsNone(prudential_note["ticker"])
        self.assertEqual(
            "PFH — 4.125% JUNIOR SUBORDINATED NOTES DUE 2060",
            prudential_note["security_label"],
        )
        prudential_stock = json.loads(
            (ROOT / "data/stocks/744320888__NOTE.json").read_text()
        )
        self.assertEqual("744320888|NOTE", prudential_stock["stock_id"])
        self.assertEqual("NOTE", prudential_stock["instrument_type"])
        self.assertFalse((ROOT / "data/stocks/744320888.json").exists())
        self.assertFalse(
            (ROOT / "data/stocks/744320888__PREF.json").exists()
        )
        health = json.loads(
            (ROOT / "data/ticker_health.json").read_text()
        )
        prudential_health = next(
            row
            for row in health["buckets"]["unresolved"]
            if row["cusip"] == "744320888"
        )
        self.assertEqual("NOTE", prudential_health["instrument_type"])

        def assert_product_terms(
            identifier: str,
            *required_terms: str,
        ) -> None:
            product_name = labels["product_names"].get(identifier)
            self.assertIsInstance(product_name, str, identifier)
            normalized = set(re.findall(r"[A-Z0-9]+", product_name.upper()))
            for term in required_terms:
                self.assertIn(term, normalized, (identifier, product_name))

        def assert_official_product_name(
            identifier: str,
            official_name: str,
            symbol: str,
        ) -> None:
            registry_entry = registry[identifier]
            self.assertEqual(
                symbol,
                registry_entry.get("ticker"),
                identifier,
            )
            actual = (
                labels["product_names"].get(identifier),
                registry_entry.get("product_name_source"),
            )
            expected = {
                (official_name, "sec_fund_series"),
                (
                    f"{official_name} — {symbol}",
                    "sec_fund_series_ticker",
                ),
            }
            self.assertIn(actual, expected, identifier)
            self.assertEqual(
                actual[0],
                registry_entry.get("product_name"),
                identifier,
            )

        assert_product_terms("464286772", "KOREA", "ETF")
        assert_official_product_name(
            "46435GAA0",
            "iShares iBonds Dec 2026 Term Corporate ETF",
            "IBDR",
        )
        assert_official_product_name(
            "46435U259",
            "iShares iBonds Dec 2026 Term Muni Bond ETF",
            "IBMO",
        )
        assert_official_product_name(
            "46436E858",
            "iShares iBonds Dec 2026 Term Treasury ETF",
            "IBTG",
        )
        self.assertEqual(
            "SCHWAB GOVERNMENT MONEY FUND - SWEEP SHARES",
            labels["product_names"].get("808515209"),
        )
        self.assertEqual(
            "MA-COM TECH — 0 12/15/29",
            labels["labels"].get("55405YAC4"),
        )
        self.assertEqual(
            "MUTUAL FUND",
            labels["kinds"].get("027681824"),
        )
        assert_product_terms(
            "027681824",
            "AMERICAN",
            "MUTUAL",
            "FUND",
        )
        self.assertEqual(
            "TURTLE BEACH CORP",
            labels["labels"].get("87252P106"),
        )
        self.assertNotIn("87252P106", labels["kinds"])
        self.assertNotIn("87252P106", labels["product_names"])
        for identifier in (
            "00143W701",
            "552981706",
            "552981805",
            "74441K107",
        ):
            self.assertEqual(
                "MUTUAL FUND",
                labels["kinds"].get(identifier),
            )
        for identifier in (
            "G12808104",
            "G29361113",
            "G3156P103",
            "G8766R134",
            "G8827C100",
        ):
            self.assertEqual(
                "CLOSED-END FUND",
                labels["kinds"].get(identifier),
            )
        for identifier in (
            "02157G309",
            "39531G308",
            "63902N106",
            "G4036C106",
        ):
            self.assertEqual(
                "COMMON",
                labels["kinds"].get(identifier),
            )
        expected_product_names = {
            "00768Y271": "ADVISORSHARES TR — HVAC INDUS ETF",
            "69384J307": "PACER FDS TR — SWAN SOS MAY ETF",
            "88340F795": "LEVERAGE SHARES 2X UPS",
            "88340W541": "LEVERAGE SHARES 2X ORLY",
            "88340W624": "LEVERAGE SHARES 2X DNN",
            "88340W731": "LEVERAGE SHARES 2X AXP",
            "921910840": "VANGUARD MEGA CAP VALUE ETF",
        }
        for identifier, expected_name in expected_product_names.items():
            self.assertEqual(
                expected_name,
                labels["product_names"].get(identifier),
            )
        assert_product_terms(
            "37960A230",
            "AI",
            "SEMICONDUCTOR",
            "QUANTUM",
            "ETF",
        )
        assert_product_terms(
            "922908611",
            "VANGUARD",
            "SMALL",
            "CAP",
            "VALUE",
        )
        for identifier in ("06738C786", "06740C527", "06748M188"):
            self.assertEqual("ETN", labels["kinds"].get(identifier))
        for identifier, required_term in {
            "06747C322": "CARBON",
            "06748M188": "VIX",
            "48133Q408": "VIX",
            "90269A278": "MLP",
            "90274E174": "PREFER",
        }.items():
            self.assertEqual("ETN", labels["kinds"].get(identifier))
            assert_product_terms(identifier, required_term)

        for identifier in (
            "027681824",
            "464286772",
            "37960A230",
            "922908611",
        ):
            self.assertEqual(
                "sec_fund_series",
                registry[identifier].get("product_name_source"),
            )
        product_symbols: dict[str, set[str]] = {}
        for identifier, entry in registry.items():
            product_name = str(entry.get("product_name") or "").strip()
            if not product_name:
                continue
            symbol = str(entry.get("ticker") or "").strip().upper() or None
            if symbol:
                product_symbols.setdefault(
                    product_name.casefold(),
                    set(),
                ).add(symbol)
        self.assertFalse(
            {
                product_name: symbols
                for product_name, symbols in product_symbols.items()
                if len(symbols) > 1
            }
        )

    def test_frontend_maintenance_remains_sticky_across_detail_races(
        self,
    ) -> None:
        html = (ROOT / "index.html").read_text()

        def section(start: str, end: str) -> str:
            start_pos = html.index(start)
            return html[start_pos:html.index(end, start_pos)]

        enter_detail = section(
            "function enterDetailView(",
            "async function loadCachedJson(",
        )
        self.assertLess(
            enter_detail.index("if (dataContractBlocked)"),
            enter_detail.index("clearAllSearchInputs()"),
        )
        self.assertIn("return false;", enter_detail)

        route = section("function routeFromHash(", "function setUrl(")
        self.assertLess(
            route.index("if (dataContractBlocked)"),
            route.index("const hash ="),
        )

        for start, end in (
            ("function showLoadingMessage(", "function showLoadError("),
            ("function showLoadError(", "function showDataMaintenance("),
        ):
            display = section(start, end)
            self.assertLess(
                display.index("if (dataContractBlocked)"),
                display.index("app().innerHTML"),
            )

        for loader, renderer in (
            ("loadFund", "renderFund"),
            ("loadStock", "renderStock"),
        ):
            detail_load = section(
                f"async function {loader}(",
                f"function {renderer}(",
            )
            await_pos = detail_load.rindex("await ")
            blocked_pos = detail_load.index(
                "if (dataContractBlocked)",
                await_pos,
            )
            render_pos = detail_load.index(f"{renderer}(")
            self.assertLess(await_pos, blocked_pos)
            self.assertLess(blocked_pos, render_pos)

    def test_update_workflow_publishes_private_snapshot_without_git_data(
        self,
    ) -> None:
        workflow = (ROOT / ".github/workflows/update-data.yml").read_text()
        publisher = (ROOT / "scripts/publish_private_snapshot.sh").read_text()
        self.assertRegex(
            workflow,
            r"(?m)^  push:\n    branches: \[main\]",
        )
        self.assertNotIn("paths-ignore:", workflow)
        self.assertRegex(workflow, r"(?m)^          ref: main$")
        self.assertRegex(
            workflow,
            r"(?s)- name: Run pipeline.*?timeout-minutes: 240",
        )
        self.assertIn("mode=(--migrations-only)", workflow)
        self.assertIn(
            "python scripts/repair_value_units.py --migrate-policy",
            workflow,
        )
        self.assertIn(
            "pipeline.VALUE_UNIT_MIGRATION_VERSION",
            workflow,
        )
        self.assertIn(
            'echo "migration_only=$migration_only" >> "$GITHUB_OUTPUT"',
            workflow,
        )
        self.assertIn("actions/create-github-app-token@v3", workflow)
        self.assertIn("python scripts/data_snapshot.py pull", workflow)
        self.assertIn("bash scripts/publish_private_snapshot.sh", workflow)
        self.assertIn("python scripts/data_snapshot.py pack", publisher)
        self.assertIn("main moved during generation", publisher)
        self.assertNotIn("git add data/", publisher)
        self.assertNotIn("git commit -m", publisher)
        self.assertNotIn("git push origin", publisher)
        self.assertRegex(
            workflow,
            r"(?s)- name: Refresh recently accepted 13F filings.*?"
            r"if: >-\n\s+"
            r"steps\.restore_snapshot\.outputs\.legacy_snapshot != 'true' &&\n\s+"
            r"steps\.pipeline\.outputs\.migration_only != 'true' &&\n\s+"
            r"steps\.pipeline\.outputs\.targeted_cik != 'true'",
        )
        for required_step in (
            "Regenerate registry-backed site data",
            "Validate generated data",
            "Publish validated private snapshot",
        ):
            section = workflow[
                workflow.index(f"- name: {required_step}"):]
            section = section[:section.find("\n      - name: ", 1)]
            self.assertNotIn(
                "steps.pipeline.outputs.migration_only",
                section,
            )

    def test_data_publishers_deploy_exact_validated_pages_artifact(
        self,
    ) -> None:
        publisher = (ROOT / "scripts/publish_private_snapshot.sh").read_text()
        for relative_path in (
            ".github/workflows/update-data.yml",
            ".github/workflows/refresh-cusip-registry.yml",
        ):
            with self.subTest(workflow=relative_path):
                workflow = (ROOT / relative_path).read_text()
                self.assertIn("id: publish_snapshot", workflow)
                self.assertIn("bash scripts/publish_private_snapshot.sh", workflow)
                self.assertIn('echo "code_sha=', publisher)
                self.assertIn('echo "release_tag=', publisher)
                self.assertIn('echo "dataset_id=', publisher)
                self.assertIn(
                    "site_changed: "
                    "${{ steps.publish_snapshot.outputs.site_changed }}",
                    workflow,
                )
                self.assertIn('echo "site_changed=', publisher)
                self.assertIn(
                    "outputs.site_changed == 'true'",
                    workflow,
                )
                self.assertIn(
                    "bash scripts/pages_deploy_needed.sh",
                    publisher,
                )
                self.assertIn(
                    "code_sha: "
                    "${{ steps.publish_snapshot.outputs.code_sha }}",
                    workflow,
                )
                self.assertIn("python scripts/data_snapshot.py pull", workflow)
                self.assertIn("python scripts/data_snapshot.py pack", publisher)
                self.assertIn("gh_mutate_once release create", publisher)
                self.assertIn("gh_read_retry release download", publisher)
                self.assertIn("gh_mutate_once release edit", publisher)
                self.assertNotIn("git add data/", publisher)
                self.assertIn(
                    "uses: ./.github/workflows/deploy-pages.yml",
                    workflow,
                )
                self.assertIn(
                    "release_tag: ${{ needs.",
                    workflow,
                )
                self.assertIn("secrets: inherit", workflow)
                self.assertNotIn("/pages/builds/latest", workflow)
                self.assertNotIn(
                    '"/repos/$GITHUB_REPOSITORY/pages/builds"',
                    workflow,
                )

        deploy_check = (
            ROOT / "scripts/pages_deploy_needed.sh"
        ).read_text()
        for required_path in (
            "index.html",
            "site-data-loader.js",
            "scripts/build_pages_artifact.py",
            "scripts/data_snapshot.py",
            "scripts/github_cli_retry.py",
            ".github/workflows/deploy-pages.yml",
        ):
            self.assertIn(required_path, deploy_check)
        self.assertIn("deployment-manifest.json", deploy_check)

        deployment = (
            ROOT / ".github/workflows/deploy-pages.yml"
        ).read_text()
        self.assertIn(
            '--between "$EXPECTED_CODE_SHA" "$current_sha"',
            deployment,
        )
        self.assertIn(
            "ref: ${{ inputs.code_sha }}",
            deployment,
        )
        self.assertIn(
            'if [ "$checked_out_sha" != "$EXPECTED_CODE_SHA" ]; then',
            deployment,
        )
        self.assertIn(
            "python scripts/build_pages_artifact.py",
            deployment,
        )
        self.assertIn("--max-archive-bytes 1000000000", deployment)
        self.assertIn('--dataset-id "$EXPECTED_DATASET_ID"', deployment)
        self.assertIn("Audit the public artifact allowlist", deployment)
        self.assertIn("-f build_type=workflow", deployment)
        self.assertIn("actions/configure-pages@v6", deployment)
        self.assertIn("actions/upload-pages-artifact@v5", deployment)
        self.assertIn("actions/deploy-pages@v5", deployment)
        self.assertIn("id-token: write", deployment)
        self.assertIn(
            '[ "$observed_code_sha" = "$EXPECTED_CODE_SHA" ]',
            deployment,
        )
        self.assertIn(
            '[ "$observed_dataset_id" = "$EXPECTED_DATASET_ID" ]',
            deployment,
        )


if __name__ == "__main__":
    unittest.main()
