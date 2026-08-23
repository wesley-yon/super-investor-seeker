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
                "::warning title=ticker_health::1 current unresolved"
            )
            and "222222222=∅" in line
            for line in lines
        ), lines)
        self.assertTrue(any(
            "::warning title=ticker_health::1 current nonzero synthetic"
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
            "::warning title=ticker_health::1 option_family_artifact"
            in line
            for line in lines
        ), lines)

    def test_current_contract_is_explicitly_version_five(self) -> None:
        self.assertEqual(5, data_contract.DATA_CONTRACT_VERSION)

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

    def test_committed_registry_metadata_survives_stale_private_cache(
        self,
    ) -> None:
        common_cases = {
            "02157G309": ("ALCE", "COM"),
            "39531G308": ("VIP", "CLASS A COM"),
            "63902N106": ("SHMP", "COMMON"),
            "G4036C106": ("TONT", "ORD SHS CL A"),
        }
        fund_kind_cases = {
            "00143W701": ("ODMAX", "MUTUAL FUND"),
            "552981706": ("MRFIX", "MUTUAL FUND"),
            "552981805": ("MTRIX", "MUTUAL FUND"),
            "74441K107": ("PGNAX", "MUTUAL FUND"),
            "G12808104": ("BOPCF", "CLOSED-END FUND"),
            "G29361113": ("EWIIF", "CLOSED-END FUND"),
            "G3156P103": ("ASA", "CLOSED-END FUND"),
            "G8766R134": ("TGONF", "CLOSED-END FUND"),
            "G8827C100": ("TPNTF", "CLOSED-END FUND"),
        }
        product_cases = {
            "00768Y271": {
                "symbol": "HVAC",
                "name": "ADVISORSHARES TR",
                "class": "HVAC INDUS ETF",
                "expected": "ADVISORSHARES TR — HVAC INDUS ETF",
                "expected_source": "filer_issuer_class",
                "stale": "ADVISORSHARES HVAC INDUS",
                "stale_openfigi": True,
            },
            "37960A230": {
                "symbol": "CHPX",
                "name": "GLOBAL X FDS AI SEMIC",
                "class": "COMMON",
                "expected": "GLO X AI SEMICON & QUANT ETF",
                "expected_source": "openfigi",
                "stale": None,
                "stale_openfigi": False,
            },
            "69384J307": {
                "symbol": "MMAY",
                "name": "PACER FDS TR",
                "class": "SWAN SOS MAY ETF",
                "expected": "PACER FDS TR — SWAN SOS MAY ETF",
                "expected_source": "filer_issuer_class",
                "stale": "PCR SWN SOS MRTE MAY ETF",
                "stale_openfigi": True,
            },
            "88340F795": {
                "symbol": "UPSG",
                "name": "THEMES ETF TR",
                "class": "LEVERAGE SHS 2X",
                "expected": "LEVERAGE SHARES 2X UPS",
                "expected_source": "openfigi",
                "stale": "THEMES ETF TR — LEVERAGE SHS 2X — UPSG",
                "stale_openfigi": False,
            },
            "88340W541": {
                "symbol": "ORLG",
                "name": "THEMES ETF TR",
                "class": "LEVERAGE SHS 2X",
                "expected": "LEVERAGE SHARES 2X ORLY",
                "expected_source": "openfigi",
                "stale": "THEMES ETF TR — LEVERAGE SHS 2X — ORLG",
                "stale_openfigi": False,
            },
            "88340W624": {
                "symbol": "DNNG",
                "name": "THEMES ETF TR",
                "class": "LEVERAGE SHS 2X",
                "expected": "LEVERAGE SHARES 2X DNN",
                "expected_source": "openfigi",
                "stale": "THEMES ETF TR — LEVERAGE SHS 2X — DNNG",
                "stale_openfigi": False,
            },
            "88340W731": {
                "symbol": "AXPG",
                "name": "THEMES ETF TR",
                "class": "LEVERAGE SHS 2X",
                "expected": "LEVERAGE SHARES 2X AXP",
                "expected_source": "openfigi",
                "stale": "THEMES ETF TR — LEVERAGE SHS 2X — AXPG",
                "stale_openfigi": False,
            },
            "921910840": {
                "symbol": "MGV",
                "name": "VANGUARD WORLD FD",
                "class": "MEGA CAP VAL ETF",
                "expected": "VANGUARD MEGA CAP VALUE ETF",
                "expected_source": "openfigi",
                "stale": "VNGRD MRGSTR MG CP VL ETF-UI",
                "stale_openfigi": True,
            },
            "922908611": {
                "symbol": "VBR",
                "name": "VANGUARD INDEX FDS",
                "class": "SM CP VAL ETF",
                "expected": "VANGUARD SMALL-CAP VALUE ETF",
                "expected_source": "openfigi",
                "stale": "VNGRD MRNGST SL-CP VAL ETF-U",
                "stale_openfigi": True,
            },
        }

        private_registry: dict[str, dict] = {}
        committed_registry: dict[str, dict] = {}
        for identifier, (symbol, dominant_class) in common_cases.items():
            private_entry = {
                "ticker": symbol,
                "security_label": symbol,
                "type": "EQUITY",
                "dominant_class": dominant_class,
                "sources": ["filer_dominant", "cusip_map_vetted"],
            }
            private_registry[identifier] = private_entry
            committed_registry[identifier] = {
                **private_entry,
                "security_kind": "COMMON",
                "security_kind_source": "filer_metadata",
            }
        for identifier, (symbol, kind) in fund_kind_cases.items():
            private_entry = {
                "ticker": symbol,
                "security_label": symbol,
                "type": "EQUITY",
                "dominant_class": "COM",
                "sources": ["filer_dominant", "cusip_map_vetted"],
            }
            private_registry[identifier] = private_entry
            committed_registry[identifier] = {
                **private_entry,
                "security_kind": kind,
                "security_kind_source": "openfigi",
            }
        for identifier, case in product_cases.items():
            private_entry = {
                "ticker": case["symbol"],
                "security_label": case["symbol"],
                "security_kind": "ETF",
                "security_kind_source": "openfigi",
                "type": "EQUITY",
                "name": case["name"],
                "dominant_issuer": case["name"],
                "dominant_class": case["class"],
                "sources": ["filer_dominant"],
            }
            if case["stale"]:
                private_entry["product_name"] = case["stale"]
                private_entry["product_name_source"] = (
                    "openfigi"
                    if case["stale_openfigi"]
                    else "filer_issuer_class_ticker"
                )
            private_registry[identifier] = private_entry
            committed_registry[identifier] = {
                **private_entry,
                "product_name": case["expected"],
                "product_name_source": case["expected_source"],
            }

        private_registry["123456789"] = {
            "ticker": "NEWF",
            "security_label": "NEWF",
            "security_kind": "MUTUAL FUND",
            "security_kind_source": "openfigi",
            "product_name": "NEW SHORT VALUE ETF",
            "product_name_source": "openfigi",
        }
        committed_registry["123456789"] = {
            "ticker": "OLDF",
            "security_label": "OLDF",
            "security_kind": "ETF",
            "security_kind_source": "openfigi",
            "product_name": "OLD SPONSOR LONG STRATEGY ETF",
            "product_name_source": "openfigi",
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            private_path = root / "cache" / "cusip_registry.json"
            public_path = root / "data" / "cusip_registry.json"
            private_path.parent.mkdir()
            public_path.parent.mkdir()
            private_path.write_text(json.dumps(private_registry))
            public_path.write_text(json.dumps(committed_registry))
            with mock.patch.multiple(
                pipeline,
                CUSIP_REGISTRY_PATH=private_path,
                LEGACY_CUSIP_REGISTRY_PATH=public_path,
            ):
                prior_registry = pipeline.load_cusip_registry()

        for identifier in common_cases:
            with self.subTest(identifier=identifier):
                self.assertEqual(
                    "COMMON",
                    prior_registry[identifier]["security_kind"],
                )
                self.assertEqual(
                    ("COMMON", "filer_metadata"),
                    pipeline._registry_security_kind(
                        identifier=identifier,
                        openfigi_detail=None,
                        prior_entry=prior_registry[identifier],
                        entry=private_registry[identifier],
                    ),
                )
        for identifier, (_symbol, kind) in fund_kind_cases.items():
            with self.subTest(identifier=identifier):
                self.assertEqual(
                    kind,
                    prior_registry[identifier]["security_kind"],
                )
                self.assertEqual(
                    (kind, "openfigi_prior_registry"),
                    pipeline._registry_security_kind(
                        identifier=identifier,
                        openfigi_detail=None,
                        prior_entry=prior_registry[identifier],
                        entry=private_registry[identifier],
                    ),
                )
        for identifier, case in product_cases.items():
            with self.subTest(identifier=identifier):
                self.assertEqual(
                    case["expected"],
                    prior_registry[identifier]["product_name"],
                )
                openfigi_detail = (
                    {
                        "status": "matched",
                        "ticker": case["symbol"],
                        "securityDescription": case["symbol"],
                        "name": case["stale"],
                    }
                    if case["stale_openfigi"]
                    else None
                )
                product_name, _source = (
                    pipeline._registry_fund_product_name(
                        identifier=identifier,
                        entry=private_registry[identifier],
                        openfigi_detail=openfigi_detail,
                        prior_entry=prior_registry[identifier],
                    )
                )
                self.assertEqual(case["expected"], product_name)

        self.assertEqual(
            "MUTUAL FUND",
            prior_registry["123456789"]["security_kind"],
        )
        self.assertEqual(
            "NEW SHORT VALUE ETF",
            prior_registry["123456789"]["product_name"],
        )

    def test_committed_sec_fund_provenance_survives_case_only_cache_drift(
        self,
    ) -> None:
        identifier = "464286772"
        committed_entry = {
            "ticker": "EWY",
            "security_label": "EWY",
            "security_kind": "ETF",
            "product_name": "iShares MSCI South Korea ETF",
            "product_name_source": "sec_fund_series",
        }
        private_entry = {
            "ticker": "EWY",
            "security_label": "EWY",
            "security_kind": "ETF",
            "product_name": "ISHARES MSCI SOUTH KOREA ETF",
            "product_name_source": "openfigi",
        }

        for private_source in ("openfigi", ""):
            with self.subTest(private_source=private_source):
                candidate = {
                    **private_entry,
                    "product_name_source": private_source,
                }
                merged = pipeline._merge_committed_registry_display_metadata(
                    {identifier: candidate},
                    {identifier: committed_entry},
                )
                self.assertEqual(
                    committed_entry["product_name"],
                    merged[identifier]["product_name"],
                )
                self.assertEqual(
                    "sec_fund_series",
                    merged[identifier]["product_name_source"],
                )

        changed_symbol = {
            **private_entry,
            "ticker": "NEWF",
            "security_label": "NEWF",
        }
        changed = pipeline._merge_committed_registry_display_metadata(
            {identifier: changed_symbol},
            {identifier: committed_entry},
        )
        self.assertEqual(
            private_entry["product_name"],
            changed[identifier]["product_name"],
        )
        self.assertEqual(
            "openfigi",
            changed[identifier]["product_name_source"],
        )

    def test_swgxx_manual_name_override_keeps_filer_issuer_provenance(
        self,
    ) -> None:
        evidence = {
            "808515209": {
                "total_value": 7_850_874,
                "holder_ciks": {1, 2, 3, 4},
                "issuer_value": {"APPLE INC": 7_850_874},
                "class_value": {"MONEY MARKET FUND": 7_850_874},
                "put_call_value": {},
                "first_seen": "2019-06-30",
                "last_seen": "2026-03-31",
            },
        }
        openfigi_details = {
            "808515209": {
                "status": "matched",
                "ticker": "SWGXX",
                "name": "SCHWAB GOVT MNY FND-SWP",
                "securityDescription": "SWGXX",
                "securityType": "Open-End Fund",
                "securityType2": "Mutual Fund",
                "marketSector": "Equity",
                "exchCode": "US",
            },
        }
        with mock.patch.multiple(
            pipeline,
            FUNDS_DIR=mock.MagicMock(exists=mock.Mock(return_value=True)),
            _aggregate_cusip_evidence=mock.Mock(return_value=evidence),
            load_cusip_map=mock.Mock(return_value={}),
            load_cusip_registry=mock.Mock(return_value={}),
            load_openfigi_details=mock.Mock(
                return_value=openfigi_details
            ),
            load_sec_fund_name_cache=mock.Mock(return_value={}),
            save_cusip_registry=mock.Mock(),
        ):
            registry = pipeline.build_cusip_registry(
                company_ticker_data=[],
            )

        swgxx = registry["808515209"]
        self.assertEqual(
            "SCHWAB GOVERNMENT MONEY FUND - SWEEP SHARES",
            swgxx["name"],
        )
        self.assertEqual("APPLE INC", swgxx["dominant_issuer"])
        self.assertIn("manual_name_override", swgxx["sources"])
        self.assertEqual("SWGXX", swgxx["ticker"])
        self.assertEqual("MUTUAL FUND", swgxx["security_kind"])
        self.assertEqual(
            "SCHWAB GOVERNMENT MONEY FUND - SWEEP SHARES",
            swgxx["product_name"],
        )
        self.assertEqual(
            "manual_name_override",
            swgxx["product_name_source"],
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
            'fetch("data/funds-index.json", { cache: "no-cache" })',
            html,
        )
        self.assertIn(
            'fetch("data/index.json", { cache: "no-cache" })',
            html,
        )

    def test_security_label_artifact_covers_registry_without_raw_cusips(
        self,
    ) -> None:
        registry = {
            "037833100": {
                "name": "Apple Inc.",
                "security_label": "AAPL",
                "label_source": "canonical_ticker",
                "dominant_class": "COM",
            },
            "65339F655": {
                "name": "NEXTERA ENERGY INC",
                "security_label": "NEE 7.375 02/15/29",
                "label_source": "openfigi",
                "security_kind": "PREFERRED",
                "security_kind_source": "openfigi",
                "dominant_class": "UNIT 02/15/2029",
            },
            "46222L116": {
                "name": "IONQ INC",
                "security_label": "IONQ/WS — WARRANT EXP 10/01/26",
                "label_source": "manual_verified",
                "security_kind": "WARRANT",
                "security_kind_source": "manual_verified",
                "dominant_class": "0",
            },
            "464286772": {
                "name": "ISHARES INC",
                "security_label": "EWY",
                "label_source": "openfigi",
                "security_kind": "ETF",
                "security_kind_source": "openfigi",
                "product_name": "ISHARES MSCI SOUTH KOREA ETF",
                "product_name_source": "openfigi",
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
                    "name": "ISHARES TR",
                    "security_label": "IWM",
                    "label_source": "openfigi",
                    "security_kind": "ETF",
                    "security_kind_source": "openfigi",
                    "product_name": "ISHARES MSCI SOUTH KOREA ETF",
                    "product_name_source": "openfigi",
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
                registry["464287655"]["security_kind_source"] = "openfigi"
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
                        "label_source": "openfigi",
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
                        ] = "openfigi"
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
                pipeline._registry_fund_symbol(
                    identifier=identifier,
                    entry=registry_entry,
                ),
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
            symbol = pipeline._registry_fund_symbol(
                identifier=identifier,
                entry=entry,
            )
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
        self.assertIn(
            "actions/create-github-app-token@"
            "bcd2ba49218906704ab6c1aa796996da409d3eb1 # v3",
            workflow,
        )
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
        self.assertIn(
            "actions/configure-pages@"
            "45bfe0192ca1faeb007ade9deae92b16b8254a0d # v6",
            deployment,
        )
        self.assertIn(
            "actions/upload-pages-artifact@"
            "fc324d3547104276b827a68afc52ff2a11cc49c9 # v5",
            deployment,
        )
        self.assertIn(
            "actions/deploy-pages@"
            "cd2ce8fcbc39b97be8ca5fce6e763baed58fa128 # v5",
            deployment,
        )
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
