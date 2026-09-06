"""SEC-master tests for option and non-option identity separation."""

from __future__ import annotations

import json
import tempfile
import unittest
from collections import defaultdict
from pathlib import Path
from unittest import mock

import pipeline
import validate_data


INFO_TABLE_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
  <infoTable>
    <nameOfIssuer>APPLE INC</nameOfIssuer>
    <titleOfClass>COM</titleOfClass>
    <cusip>037833100</cusip>
    <value>100</value>
    <shrsOrPrnAmt><sshPrnamt>10</sshPrnamt></shrsOrPrnAmt>
  </infoTable>
  <infoTable>
    <nameOfIssuer>APPLE INC</nameOfIssuer>
    <titleOfClass>COM</titleOfClass>
    <cusip>037833100</cusip>
    <value>50</value>
    <shrsOrPrnAmt><sshPrnamt>5</sshPrnamt></shrsOrPrnAmt>
    <putCall>CALL</putCall>
  </infoTable>
</informationTable>
"""


DECIMAL_SHARES_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
  <infoTable>
    <nameOfIssuer>APPLE INC</nameOfIssuer>
    <titleOfClass>COM</titleOfClass>
    <cusip>037833100</cusip>
    <value>100</value>
    <shrsOrPrnAmt><sshPrnamt>10.5</sshPrnamt></shrsOrPrnAmt>
  </infoTable>
</informationTable>
"""


def aggregate_row(
    *,
    issuer: str,
    security_class: str,
    instrument_type: str,
    value: int = 100,
) -> dict:
    return {
        "total_value": value,
        "holder_ciks": {1},
        "issuer_value": {issuer: value},
        "class_value": {security_class: value},
        "put_call_value": defaultdict(int),
        "instrument_type_value": {instrument_type: value},
        "instrument_type_count": {instrument_type: 1},
        "non_option_issuer_value": (
            {} if instrument_type in {"CALL", "PUT", "OPT"} else {issuer: value}
        ),
        "non_option_issuer_count": (
            {} if instrument_type in {"CALL", "PUT", "OPT"} else {issuer: 1}
        ),
        "non_option_class_value": (
            {}
            if instrument_type in {"CALL", "PUT", "OPT"}
            else {security_class: value}
        ),
        "non_option_class_count": (
            {} if instrument_type in {"CALL", "PUT", "OPT"} else {security_class: 1}
        ),
        "first_seen": "2026-03-31",
        "last_seen": "2026-06-30",
    }


class ExactPositionIdentityTests(unittest.TestCase):
    def test_master_universe_preserves_saved_types_despite_class_text(self) -> None:
        holdings = [
            {"cusip": "00217DAG5", "holding_type": "EQUITY",
             "class": "CONVERTIBLE BOND"},
            {"cusip": "03843E904", "holding_type": "NOTE", "class": "COM"},
            {"cusip": "037833100", "holding_type": "PREF", "class": "COM"},
            {"cusip": "037833100", "holding_type": "WARRANT", "class": "COM"},
            {"cusip": "BAD-CUSIP", "holding_type": "NOTE", "class": "COM"},
        ]
        before = json.loads(json.dumps(holdings))
        universe = pipeline._security_universe_from_holdings(holdings)
        self.assertEqual(
            {(row["cusip"], row["holding_type"]) for row in holdings},
            {(row["cusip"], row["instrument_type"]) for row in universe},
        )
        self.assertEqual(before, holdings)

    def test_live_ticker_update_cannot_reclassify_saved_note_to_equity(self) -> None:
        master = {"records": {
            "037833100|EQUITY": {
                "cusip": "037833100", "instrument_type": "EQUITY",
                "mapping_status": "resolved", "ticker": "AAPL",
            },
        }}
        holdings = [{
            "cusip": "037833100", "holding_type": "NOTE",
            "class": "COM", "ticker": "OLD",
        }]
        cusip_map = {"037833100": "OLD"}
        with mock.patch.object(pipeline, "load_security_master", return_value=master):
            pipeline.update_cusip_map(cusip_map, holdings)
        self.assertEqual({}, cusip_map)
        self.assertIsNone(holdings[0]["ticker"])
        self.assertEqual("NOTE", holdings[0]["holding_type"])

    def test_call_put_and_equity_are_distinct_public_ids(self) -> None:
        self.assertEqual("037833100", pipeline.stock_lookup_id("037833100", "EQUITY"))
        self.assertEqual("037833100|CALL", pipeline.stock_lookup_id("037833100", "CALL"))
        self.assertEqual("037833100|PUT", pipeline.stock_lookup_id("037833100", "PUT"))
        self.assertEqual(
            "037833100__CALL.json",
            pipeline.stock_filename("037833100", "CALL"),
        )
        self.assertEqual(
            "037833100__PUT.json",
            pipeline.stock_filename("037833100", "PUT"),
        )

    def test_security_universe_adds_explicit_option_underlying(self) -> None:
        universe = pipeline._security_universe_from_holdings(
            [
                {
                    "reported_cusip": "037833100",
                    "reported_issuer": "APPLE INC",
                    "reported_class": "COM",
                    "put_call": "CALL",
                },
                {
                    "reported_cusip": "76954AAD5",
                    "reported_issuer": "RIVIAN AUTOMOTIVE INC",
                    "reported_class": "NOTE 3.625% 10/15/30",
                    "holding_type": "NOTE",
                },
            ]
        )
        identities = {
            (entry["cusip"], entry["instrument_type"]) for entry in universe
        }
        self.assertEqual(
            {
                ("037833100", "CALL"),
                ("037833100", "EQUITY"),
                ("76954AAD5", "NOTE"),
            },
            identities,
        )

    def test_registry_position_ticker_uses_only_proven_underlying(self) -> None:
        entry = {
            "type": "EQUITY",
            "ticker": None,
            "underlying_ticker": "AAPL",
            "underlying_ticker_source": "sec_ftd",
            "underlying_ticker_as_of": "2026-06-30",
        }
        self.assertEqual("AAPL", pipeline._registry_position_ticker(entry, "CALL"))
        self.assertEqual("AAPL", pipeline._registry_position_ticker(entry, "PUT"))
        self.assertIsNone(pipeline._registry_position_ticker(entry, "NOTE"))
        self.assertIsNone(
            pipeline._registry_position_ticker({"ticker": None}, "CALL")
        )

    def test_universe_binds_quarter_source_to_exact_reported_identity(self) -> None:
        source = {
            "accession": "0001067983-26-000001",
            "report_date": "2026-06-30",
            "url": "https://www.sec.gov/files/structureddata/data/"
            "form-13f-data-sets/2026q2_form13f.zip",
            "sha256": "a" * 64,
        }
        universe = pipeline._security_universe_from_holdings(
            [{
                "reported_cusip": "037833100",
                "reported_issuer": "APPLE INC",
                "reported_class": "COM",
                "accession": source["accession"],
                "report_date": source["report_date"],
                "holding_type": "EQUITY",
            }],
            [source],
        )
        evidence = universe[0]["reported_identity_evidence"]
        self.assertEqual("037833100", evidence[0]["reported_cusip"])
        self.assertEqual("APPLE INC", evidence[0]["reported_issuer"])
        self.assertEqual(source["sha256"], evidence[0]["sha256"])

    def test_universe_keeps_case_variants_and_normalizes_only_whitespace(
        self,
    ) -> None:
        source = {
            "accession": "0001067983-26-000001",
            "report_date": "2026-06-30",
            "url": "https://www.sec.gov/files/structureddata/data/"
            "form-13f-data-sets/2026q2_form13f.zip",
            "sha256": "a" * 64,
        }
        universe = pipeline._security_universe_from_holdings(
            [
                {
                    "reported_cusip": "037833100",
                    "reported_issuer": "APPLE   INC",
                    "reported_class": "COMMON   STOCK",
                    "accession": source["accession"],
                    "report_date": source["report_date"],
                    "holding_type": "EQUITY",
                },
                {
                    "reported_cusip": "037833100",
                    "reported_issuer": "Apple Inc",
                    "reported_class": "Common Stock",
                    "accession": source["accession"],
                    "report_date": source["report_date"],
                    "holding_type": "EQUITY",
                },
            ],
            [source],
        )

        self.assertEqual(2, len(universe))
        self.assertEqual(
            {("APPLE INC", "COMMON STOCK"), ("Apple Inc", "Common Stock")},
            {
                (record["issuer"], record["security_class"])
                for record in universe
            },
        )
        for record in universe:
            evidence = record["reported_identity_evidence"]
            self.assertEqual(1, len(evidence))
            self.assertEqual(record["issuer"], evidence[0]["reported_issuer"])
            self.assertEqual(
                record["security_class"],
                evidence[0]["reported_class"],
            )

        with tempfile.TemporaryDirectory() as tmpdir:
            state = pipeline.load_source_state(Path(tmpdir) / "missing.json")
        built = pipeline.rebuild_sec_security_master(state, universe)
        record = built["records"]["037833100|EQUITY"]
        self.assertEqual(2, len(record["reported_identities"]))
        self.assertEqual(2, len(record["reported_identity_evidence"]))

    def test_collected_universe_does_not_cross_attach_case_variant_evidence(
        self,
    ) -> None:
        rows = [
            (
                "0001067983-26-000001",
                "APPLE INC",
                "COMMON STOCK",
                "a" * 64,
            ),
            (
                "0001067983-26-000002",
                "Apple Inc",
                "Common Stock",
                "b" * 64,
            ),
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir)
            for index, (accession, issuer, security_class, digest) in enumerate(
                rows
            ):
                fund = {
                    "quarters": [{
                        "holdings": [{
                            "reported_cusip": "037833100",
                            "reported_issuer": issuer,
                            "reported_class": security_class,
                            "accession": accession,
                            "report_date": "2026-06-30",
                            "holding_type": "EQUITY",
                        }],
                        "reported_identity_sources": [{
                            "accession": accession,
                            "report_date": "2026-06-30",
                            "url": "https://www.sec.gov/files/structureddata/data/"
                            "form-13f-data-sets/2026q2_form13f.zip",
                            "sha256": digest,
                        }],
                    }],
                }
                (funds_dir / f"{index}.json").write_text(json.dumps(fund))
            with mock.patch.object(pipeline, "FUNDS_DIR", funds_dir):
                universe = pipeline.collect_security_master_universe()

        self.assertEqual(2, len(universe))
        for record in universe:
            evidence = record["reported_identity_evidence"]
            self.assertEqual(1, len(evidence))
            self.assertEqual(record["issuer"], evidence[0]["reported_issuer"])
            self.assertEqual(
                record["security_class"],
                evidence[0]["reported_class"],
            )

    def test_universe_ignores_untrusted_holding_local_identity_evidence(
        self,
    ) -> None:
        universe = pipeline._security_universe_from_holdings([{
            "reported_cusip": "037833100",
            "reported_issuer": "APPLE INC",
            "reported_class": "COM",
            "accession": "0001067983-26-000001",
            "report_date": "2026-06-30",
            "holding_type": "EQUITY",
            "reported_identity_evidence": [{
                "accession": "0001067983-26-999999",
                "report_date": "2025-12-31",
                "url": "https://www.sec.gov/Archives/edgar/data/1067983/"
                "000106798326999999/informationtable.xml",
                "sha256": "a" * 64,
            }],
        }])

        self.assertEqual(1, len(universe))
        self.assertNotIn("reported_identity_evidence", universe[0])

    def test_universe_preserves_exact_blank_reported_issuer(self) -> None:
        source = {
            "accession": "0001643792-26-000009",
            "report_date": "2026-06-30",
            "url": "https://www.sec.gov/Archives/edgar/data/1643792/"
            "000164379226000009/Rapportering_13F_20260804.xml",
            "sha256": "a" * 64,
        }
        universe = pipeline._security_universe_from_holdings(
            [{
                "reported_cusip": "M46528101",
                "reported_issuer": "",
                "reported_class": "COM",
                "issuer": "Frontline plc",
                "class": "COM",
                "accession": source["accession"],
                "report_date": source["report_date"],
                "holding_type": "EQUITY",
            }],
            [source],
        )

        self.assertEqual(1, len(universe))
        self.assertEqual("M46528101", universe[0]["cusip"])
        self.assertEqual("EQUITY", universe[0]["instrument_type"])
        self.assertEqual("", universe[0]["issuer"])
        self.assertEqual("COM", universe[0]["security_class"])
        evidence = universe[0]["reported_identity_evidence"]
        self.assertEqual("", evidence[0]["reported_issuer"])
        self.assertEqual("COM", evidence[0]["reported_class"])
        self.assertEqual(source["url"], evidence[0]["url"])
        self.assertNotIn("Frontline", json.dumps(universe))
        with tempfile.TemporaryDirectory() as tmpdir:
            state = pipeline.load_source_state(Path(tmpdir) / "missing.json")
        master = pipeline.rebuild_sec_security_master(state, universe)
        record = master["records"]["M46528101|EQUITY"]
        self.assertEqual(
            [{
                "reported_cusip": "M46528101",
                "reported_issuer": "",
                "reported_class": "COM",
            }],
            record["reported_identities"],
        )
        self.assertEqual("", record["reported_identity_evidence"][0][
            "reported_issuer"
        ])
        self.assertNotIn("Frontline", json.dumps(record))

        missing_reported_key = pipeline._security_universe_from_holdings(
            [{
                "reported_cusip": "M46528101",
                "reported_class": "COM",
                "issuer": "Frontline plc",
                "class": "COM",
                "accession": source["accession"],
                "report_date": source["report_date"],
                "holding_type": "EQUITY",
            }],
            [source],
        )
        self.assertEqual("", missing_reported_key[0]["issuer"])
        self.assertNotIn(
            "reported_identity_evidence",
            missing_reported_key[0],
        )
        self.assertNotIn("Frontline", json.dumps(missing_reported_key))

    def test_registry_ticker_never_crosses_non_option_sibling_types(self) -> None:
        equity = {"type": "EQUITY", "ticker": "ABC"}
        preferred = {"type": "PREF", "ticker": "ABC/PA"}
        warrant = {"type": "WARRANT", "ticker": "ABC/WS"}
        self.assertEqual(
            "ABC",
            pipeline._registry_position_ticker(equity, "EQUITY"),
        )
        self.assertIsNone(pipeline._registry_position_ticker(equity, "PREF"))
        self.assertIsNone(pipeline._registry_position_ticker(equity, "WARRANT"))
        self.assertEqual(
            "ABC/PA",
            pipeline._registry_position_ticker(preferred, "PREF"),
        )
        self.assertIsNone(
            pipeline._registry_position_ticker(preferred, "EQUITY")
        )
        self.assertEqual(
            "ABC/WS",
            pipeline._registry_position_ticker(warrant, "WARRANT"),
        )
        self.assertIsNone(
            pipeline._registry_position_ticker({"ticker": "ABC"}, "EQUITY")
        )

    def test_update_cusip_map_resolves_each_non_option_type_exactly(self) -> None:
        master = {
            "records": {
                "037833100|EQUITY": {
                    "cusip": "037833100",
                    "instrument_type": "EQUITY",
                    "mapping_status": "resolved",
                    "ticker": "AAPL",
                },
                "037833100|PREF": {
                    "cusip": "037833100",
                    "instrument_type": "PREF",
                    "mapping_status": "unresolved",
                    "ticker": None,
                },
                "037833100|WARRANT": {
                    "cusip": "037833100",
                    "instrument_type": "WARRANT",
                    "mapping_status": "resolved",
                    "ticker": "AAPL/WS",
                },
            }
        }
        holdings = [
            {
                "cusip": "037833100",
                "class": "COM",
                "holding_type": "EQUITY",
                "ticker": "OLD",
            },
            {
                "cusip": "037833100",
                "class": "PFD",
                "holding_type": "PREF",
                "ticker": "OLD",
            },
            {
                "cusip": "037833100",
                "class": "WARRANT",
                "holding_type": "WARRANT",
                "ticker": "OLD",
            },
        ]
        cusip_map = {"037833100": "OLD"}
        with mock.patch.object(
            pipeline,
            "load_security_master",
            return_value=master,
        ):
            pipeline.update_cusip_map(cusip_map, holdings)

        self.assertNotIn("037833100", cusip_map)
        self.assertEqual("AAPL", holdings[0]["ticker"])
        self.assertIsNone(holdings[1]["ticker"])
        self.assertEqual("AAPL/WS", holdings[2]["ticker"])

    def test_compatibility_map_checks_siblings_absent_from_current_quarter(
        self,
    ) -> None:
        master = {
            "records": {
                "037833100|EQUITY": {
                    "cusip": "037833100",
                    "instrument_type": "EQUITY",
                    "mapping_status": "resolved",
                    "ticker": "AAPL",
                },
                "037833100|PREF": {
                    "cusip": "037833100",
                    "instrument_type": "PREF",
                    "mapping_status": "unresolved",
                    "ticker": None,
                },
            }
        }
        current_quarter = [{
            "cusip": "037833100",
            "class": "COM",
            "holding_type": "EQUITY",
        }]

        resolved = pipeline.resolve_cusips_via_sec_security_master(
            ["037833100"],
            holdings=current_quarter,
            master=master,
        )

        self.assertEqual({}, resolved)


class SecOnlyRegistryBuildTests(unittest.TestCase):
    def test_loaded_master_lookup_does_not_revalidate_whole_master(self) -> None:
        key = "037833100|EQUITY"
        master = {
            "records": {
                key: {
                    "cusip": "037833100",
                    "instrument_type": "EQUITY",
                    "mapping_status": "resolved",
                    "ticker": "AAPL",
                    "ticker_source": "sec_ftd",
                    "ticker_as_of": "2026-06-30",
                }
            }
        }
        with mock.patch.object(
            pipeline,
            "resolve_security",
            side_effect=AssertionError("whole-master resolver was called"),
        ):
            resolved = pipeline._resolve_loaded_security(
                master,
                "037833100",
                "EQUITY",
            )
        self.assertEqual("AAPL", resolved["ticker"])

    def test_security_universe_preserves_conflicting_as_filed_descriptors(self) -> None:
        holdings = [
            {
                "reported_cusip": "037833100",
                "reported_issuer": "APPLE INC",
                "reported_class": "COM",
            },
            {
                "reported_cusip": "037833100",
                "reported_issuer": "WRONG ISSUER",
                "reported_class": "COM",
            },
        ]
        universe = pipeline._security_universe_from_holdings(holdings)
        self.assertEqual(2, len(universe))
        self.assertEqual(
            {"APPLE INC", "WRONG ISSUER"},
            {record["issuer"] for record in universe},
        )

    def test_debt_never_inherits_issuer_common_ticker(self) -> None:
        evidence = {
            "76954AAD5": aggregate_row(
                issuer="RIVIAN AUTOMOTIVE INC",
                security_class="NOTE 3.625% 10/15/30",
                instrument_type="NOTE",
            )
        }

        def resolve(_master, cusip, instrument_type):
            self.assertEqual(("76954AAD5", "NOTE"), (cusip, instrument_type))
            return {
                "cusip": cusip,
                "instrument_type": instrument_type,
                "mapping_status": "no_listed_symbol",
                "ticker": None,
                "ticker_source": None,
                "ticker_as_of": None,
                "security_label": "RIVIAN AUTOMOTIVE INC — NOTE 3.625% 10/15/30",
                "security_label_source": "sec_13f_list",
            }

        with (
            mock.patch.object(pipeline, "FUNDS_DIR", pipeline.ROOT),
            mock.patch.object(pipeline, "_aggregate_cusip_evidence", return_value=evidence),
            mock.patch.object(pipeline, "load_security_master", return_value={}),
            mock.patch.object(pipeline, "resolve_security", side_effect=resolve),
            mock.patch.object(pipeline, "save_cusip_registry") as save,
        ):
            registry = pipeline.build_cusip_registry(company_ticker_data={})

        entry = registry["76954AAD5"]
        self.assertEqual("NOTE", entry["type"])
        self.assertEqual("no_listed_symbol", entry["mapping_status"])
        self.assertIsNone(entry["ticker"])
        self.assertEqual("BOND", entry["security_kind"])
        save.assert_called_once()

    def test_synthetic_and_bad_checksum_ids_remain_tickerless_with_labels(
        self,
    ) -> None:
        holdings = [
            {
                "reported_cusip": "000000000",
                "reported_issuer": "",
                "reported_class": "",
                "cusip": "000000000",
                "issuer": "",
                "class": "",
                "holding_type": "EQUITY",
            },
            {
                "reported_cusip": "037833101",
                "reported_issuer": "APPLE INC",
                "reported_class": "COM",
                "cusip": "037833101",
                "issuer": "APPLE INC",
                "class": "COM",
                "holding_type": "EQUITY",
            },
        ]
        universe = pipeline._security_universe_from_holdings(holdings)

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = pipeline.load_source_state(root / "missing-source-state.json")
            built = pipeline.rebuild_sec_security_master(state, universe)

            expected_reasons = {
                "000000000": "synthetic_or_placeholder_identifier",
                "037833101": "check_digit_mismatch",
            }
            for cusip, reason in expected_reasons.items():
                master_key = f"{cusip}|EQUITY"
                record = built["records"][master_key]
                self.assertEqual("malformed_as_filed", record["mapping_status"])
                self.assertEqual(reason, record["resolution_reason"])
                self.assertIsNone(record["ticker"])
                self.assertIsNone(record["ticker_source"])
                self.assertIsNone(record["ticker_as_of"])
                self.assertEqual(
                    cusip,
                    record["reported_identities"][0]["reported_cusip"],
                )
                self.assertEqual(reason, built["quarantine"][master_key]["reason"])

            evidence = {
                "000000000": aggregate_row(
                    issuer="",
                    security_class="",
                    instrument_type="EQUITY",
                ),
                "037833101": aggregate_row(
                    issuer="APPLE INC",
                    security_class="COM",
                    instrument_type="EQUITY",
                ),
            }
            with (
                mock.patch.object(pipeline, "FUNDS_DIR", root),
                mock.patch.object(
                    pipeline,
                    "_aggregate_cusip_evidence",
                    return_value=evidence,
                ),
                mock.patch.object(
                    pipeline,
                    "load_security_master",
                    return_value=built,
                ),
                mock.patch.object(pipeline, "save_cusip_registry"),
            ):
                registry = pipeline.build_cusip_registry(company_ticker_data={})

            expected_labels = {
                "000000000": "UNIDENTIFIED EQUITY SECURITY — 000000000",
                "037833101": "APPLE INC — COM",
            }
            expected_label_sources = {
                "000000000": "synthetic_identifier",
                "037833101": "sec_13f_filer_consensus",
            }
            self.assertEqual(set(expected_reasons), set(registry))
            for cusip, label in expected_labels.items():
                entry = registry[cusip]
                self.assertEqual("malformed_as_filed", entry["mapping_status"])
                self.assertIsNone(entry["ticker"])
                self.assertIsNone(entry["ticker_source"])
                self.assertIsNone(entry["ticker_as_of"])
                self.assertEqual(label, entry["security_label"])
                self.assertEqual(
                    expected_label_sources[cusip],
                    entry["label_source"],
                )

            labels_path = root / "security_labels.json"
            with mock.patch.object(
                pipeline,
                "SECURITY_LABELS_PATH",
                labels_path,
            ):
                pipeline.write_security_labels(registry)

            payload = json.loads(labels_path.read_text(encoding="utf-8"))
            self.assertEqual(expected_labels, payload["labels"])

            errors: list[str] = []
            with mock.patch.object(
                validate_data,
                "SECURITY_LABELS_PATH",
                labels_path,
            ):
                labels = validate_data.validate_security_labels(registry, errors)
            self.assertEqual([], errors)
            self.assertEqual(expected_labels, labels)

    def test_official_class_corrects_broad_public_type_to_exact_master_key(
        self,
    ) -> None:
        evidence = {
            "037833100": aggregate_row(
                issuer="APPLE INC",
                security_class="PFD",
                instrument_type="EQUITY",
            )
        }
        official = {
            "status": "active",
            "period": "2026Q2",
            "records": [{
                "cusip": "037833100",
                "issuer": "APPLE INC",
                "description": "PFD",
                "status": "",
            }],
        }
        master = {
            "records": {
                "037833100|EQUITY": {
                    "cusip": "037833100",
                    "instrument_type": "EQUITY",
                    "mapping_status": "no_listed_symbol",
                    "ticker": None,
                    "ticker_source": None,
                    "ticker_as_of": None,
                    "official_13f": official,
                },
                "037833100|PREF": {
                    "cusip": "037833100",
                    "instrument_type": "PREF",
                    "mapping_status": "resolved",
                    "ticker": "AAPLPR",
                    "ticker_source": "sec_ftd",
                    "ticker_as_of": "2026-06-30",
                    "official_13f": official,
                    "security_label": "APPLE INC — PFD",
                    "security_label_source": "sec_13f_list",
                },
            }
        }

        with (
            mock.patch.object(pipeline, "FUNDS_DIR", pipeline.ROOT),
            mock.patch.object(
                pipeline,
                "_aggregate_cusip_evidence",
                return_value=evidence,
            ),
            mock.patch.object(
                pipeline,
                "load_security_master",
                return_value=master,
            ),
            mock.patch.object(pipeline, "save_cusip_registry"),
        ):
            registry = pipeline.build_cusip_registry(company_ticker_data={})

        entry = registry["037833100"]
        self.assertEqual("PREF", entry["type"])
        self.assertEqual("PREFERRED", entry["security_kind"])
        self.assertEqual("sec_13f_list", entry["security_kind_source"])
        self.assertEqual("AAPLPR", entry["ticker"])
        self.assertEqual("sec_ftd", entry["ticker_source"])

    def test_option_mapping_stays_null_while_proven_underlying_is_displayable(self) -> None:
        evidence = {
            "037833100": aggregate_row(
                issuer="APPLE INC",
                security_class="COM",
                instrument_type="CALL",
            )
        }

        def resolve(_master, cusip, instrument_type):
            if instrument_type == "CALL":
                return {
                    "cusip": cusip,
                    "instrument_type": "CALL",
                    "mapping_status": "no_listed_symbol",
                    "ticker": None,
                    "ticker_source": None,
                    "ticker_as_of": None,
                    "security_label": "APPLE INC — COM",
                    "security_label_source": "sec_13f_list",
                }
            self.assertEqual("EQUITY", instrument_type)
            return {
                "cusip": cusip,
                "instrument_type": "EQUITY",
                "mapping_status": "resolved",
                "ticker": "AAPL",
                "ticker_source": "sec_ftd",
                "ticker_as_of": "2026-06-30",
            }

        with (
            mock.patch.object(pipeline, "FUNDS_DIR", pipeline.ROOT),
            mock.patch.object(pipeline, "_aggregate_cusip_evidence", return_value=evidence),
            mock.patch.object(pipeline, "load_security_master", return_value={}),
            mock.patch.object(pipeline, "resolve_security", side_effect=resolve),
            mock.patch.object(pipeline, "save_cusip_registry"),
        ):
            registry = pipeline.build_cusip_registry(company_ticker_data={})

        entry = registry["037833100"]
        self.assertEqual("CALL", entry["type"])
        self.assertIsNone(entry["ticker"])
        self.assertEqual("AAPL", entry["underlying_ticker"])
        self.assertEqual("sec_ftd", entry["underlying_ticker_source"])
        self.assertEqual("AAPL", pipeline._registry_position_ticker(entry, "CALL"))

    def test_mixed_equity_registry_still_carries_proven_option_underlying(self) -> None:
        evidence = {
            "037833100": aggregate_row(
                issuer="APPLE INC",
                security_class="COM",
                instrument_type="EQUITY",
            )
        }
        evidence["037833100"]["instrument_type_count"]["CALL"] = 1
        evidence["037833100"]["instrument_type_value"]["CALL"] = 100
        master = {
            "records": {
                "037833100|EQUITY": {
                    "cusip": "037833100",
                    "instrument_type": "EQUITY",
                    "mapping_status": "resolved",
                    "ticker": "AAPL",
                    "ticker_source": "sec_ftd",
                    "ticker_as_of": "2026-06-30",
                }
            }
        }
        with (
            mock.patch.object(pipeline, "FUNDS_DIR", pipeline.ROOT),
            mock.patch.object(
                pipeline,
                "_aggregate_cusip_evidence",
                return_value=evidence,
            ),
            mock.patch.object(
                pipeline,
                "load_security_master",
                return_value=master,
            ),
            mock.patch.object(pipeline, "save_cusip_registry"),
        ):
            registry = pipeline.build_cusip_registry(company_ticker_data={})

        entry = registry["037833100"]
        self.assertEqual("EQUITY", entry["type"])
        self.assertEqual("AAPL", entry["ticker"])
        self.assertEqual("AAPL", entry["underlying_ticker"])
        self.assertEqual("AAPL", pipeline._registry_position_ticker(entry, "CALL"))


class ProviderNeutralRegressionTests(unittest.TestCase):
    def test_fund_validation_rejects_tickers_without_exact_registry_proof(
        self,
    ) -> None:
        holdings = [
            {
                "ticker": "MSFT",
                "issuer": "APPLE INC",
                "cusip": "037833100",
                "class": "COM",
                "value": 100,
                "shares": 10,
                "holding_type": "EQUITY",
            },
            {
                "ticker": "RIVN",
                "issuer": "RIVIAN AUTOMOTIVE INC",
                "cusip": "76954AAD5",
                "class": "NOTE",
                "value": 50,
                "shares": 5,
                "holding_type": "NOTE",
            },
            {
                "ticker": "MSFT",
                "issuer": "APPLE INC",
                "cusip": "037833100",
                "class": "COM",
                "value": 25,
                "shares": 2,
                "holding_type": "CALL",
                "put_call": "CALL",
            },
        ]
        registry = {
            "037833100": {
                "type": "EQUITY",
                "name": "APPLE INC",
                "ticker": "AAPL",
                "mapping_status": "resolved",
                "ticker_source": "sec_ftd",
                "ticker_as_of": "2026-06-30",
                "underlying_ticker": "AAPL",
                "underlying_ticker_source": "sec_ftd",
                "underlying_ticker_as_of": "2026-06-30",
            },
            "76954AAD5": {
                "type": "NOTE",
                "name": "RIVIAN AUTOMOTIVE INC",
                "ticker": None,
                "mapping_status": "unresolved",
                "ticker_source": None,
                "ticker_as_of": None,
            },
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir)
            (funds_dir / "1.json").write_text(json.dumps({
                "cik": 1,
                "name": "Fixture Fund",
                "quarters": [{
                    "report_date": "2026-06-30",
                    "filing_date": "2026-08-14",
                    "total_value": 175,
                    "num_holdings": 3,
                    "holdings": holdings,
                }],
            }))
            errors: list[str] = []
            with mock.patch.object(validate_data, "FUNDS_DIR", funds_dir):
                validate_data.validate_funds(errors, registry)

        ticker_errors = [
            error for error in errors
            if "does not match its exact SEC registry proof" in error
        ]
        self.assertEqual(3, len(ticker_errors), ticker_errors)
        self.assertTrue(any("'MSFT'" in error and "'AAPL'" in error for error in ticker_errors))
        self.assertTrue(any("'RIVN'" in error and "None" in error for error in ticker_errors))

    def test_parse_and_consolidate_keep_equity_and_call_separate(self) -> None:
        holdings = pipeline.parse_information_table(
            INFO_TABLE_XML,
            accession="0000320193-26-000001",
            report_date="2026-06-30",
        )

        self.assertIsNotNone(holdings)
        self.assertEqual(
            ["EQUITY", "CALL"],
            [holding["holding_type"] for holding in holdings],
        )
        for holding in holdings:
            self.assertEqual("APPLE INC", holding["reported_issuer"])
            self.assertEqual("COM", holding["reported_class"])
            self.assertEqual("037833100", holding["reported_cusip"])
            self.assertEqual("0000320193-26-000001", holding["accession"])
            self.assertEqual("2026-06-30", holding["report_date"])

        consolidated = pipeline.consolidate_holdings(holdings)
        self.assertEqual(2, len(consolidated))
        by_type = {holding["holding_type"]: holding for holding in consolidated}
        self.assertEqual((100, 10), (
            by_type["EQUITY"]["value"],
            by_type["EQUITY"]["shares"],
        ))
        self.assertEqual((50, 5), (
            by_type["CALL"]["value"],
            by_type["CALL"]["shares"],
        ))
        self.assertEqual("CALL", by_type["CALL"]["put_call"])

    def test_classify_saved_holding_preserves_existing_option_type(self) -> None:
        holding = {
            "issuer": "APPLE INC",
            "cusip": "037833100",
            "class": "COM",
            "holding_type": "CALL",
            "value": 50,
            "shares": 5,
        }

        self.assertEqual("CALL", pipeline.classify_saved_holding(holding))

    def test_parse_information_table_keeps_decimal_share_counts(self) -> None:
        holdings = pipeline.parse_information_table(DECIMAL_SHARES_XML)

        self.assertIsNotNone(holdings)
        self.assertEqual(10.5, holdings[0]["shares"])

    def test_stock_files_are_cusip_keyed_and_split_by_instrument_type(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir) / "data"
            funds_dir = data_dir / "funds"
            stocks_dir = data_dir / "stocks"
            funds_dir.mkdir(parents=True)
            fund = {
                "cik": 123456,
                "name": "Example Fund",
                "quarters": [{
                    "report_date": "2026-06-30",
                    "filing_date": "2026-08-14",
                    "total_value": 250,
                    "num_holdings": 4,
                    "holdings": [
                        {
                            "ticker": None,
                            "issuer": "APPLE INC",
                            "cusip": "037833100",
                            "class": "COM",
                            "value": 100,
                            "shares": 10,
                            "holding_type": "EQUITY",
                        },
                        {
                            "ticker": None,
                            "issuer": "APPLE INC",
                            "cusip": "037833100",
                            "class": "COM",
                            "value": 50,
                            "shares": 5,
                            "holding_type": "CALL",
                            "put_call": "CALL",
                        },
                        {
                            "ticker": None,
                            "issuer": "BLACKROCK INC",
                            "cusip": "09290D101",
                            "class": "COM",
                            "value": 75,
                            "shares": 3,
                            "holding_type": "EQUITY",
                        },
                        {
                            "ticker": None,
                            "issuer": "ISHARES INC",
                            "cusip": "46434G772",
                            "class": "ETF",
                            "value": 25,
                            "shares": 1,
                            "holding_type": "EQUITY",
                        },
                    ],
                }],
            }
            (funds_dir / "123456.json").write_text(json.dumps(fund))
            registry = {
                "037833100": {
                    "ticker": "AAPL",
                    "mapping_status": "resolved",
                    "ticker_source": "sec_ftd",
                    "ticker_as_of": "2026-06-30",
                    "name": "APPLE INC",
                    "type": "EQUITY",
                    "security_kind": "COMMON",
                    "underlying_ticker": "AAPL",
                    "underlying_ticker_source": "sec_ftd",
                    "underlying_ticker_as_of": "2026-06-30",
                },
                "09290D101": {
                    "ticker": "BLK",
                    "mapping_status": "resolved",
                    "ticker_source": "sec_ftd",
                    "ticker_as_of": "2026-06-30",
                    "name": "BLACKROCK INC",
                    "type": "EQUITY",
                    "security_kind": "COMMON",
                },
                "46434G772": {
                    "ticker": "BLK",
                    "mapping_status": "resolved",
                    "ticker_source": "sec_ftd",
                    "ticker_as_of": "2026-06-30",
                    "name": "ISHARES INC",
                    "type": "EQUITY",
                    "security_kind": "ETF",
                },
            }

            with mock.patch.multiple(
                pipeline,
                DATA_DIR=data_dir,
                FUNDS_DIR=funds_dir,
                STOCKS_DIR=stocks_dir,
                INDEX_PATH=data_dir / "index.json",
                FUNDS_INDEX_PATH=data_dir / "funds-index.json",
                load_cusip_registry=mock.Mock(return_value=registry),
            ):
                pipeline.regenerate_stock_files_and_index(state={})

            equity = json.loads((stocks_dir / "037833100.json").read_text())
            call = json.loads(
                (stocks_dir / "037833100__CALL.json").read_text()
            )
            blackrock = json.loads(
                (stocks_dir / "09290D101.json").read_text()
            )
            ishares = json.loads(
                (stocks_dir / "46434G772.json").read_text()
            )
            index = json.loads((data_dir / "index.json").read_text())

            self.assertEqual("037833100", equity["stock_id"])
            self.assertEqual("037833100|CALL", call["stock_id"])
            self.assertEqual("AAPL", equity["ticker"])
            self.assertEqual("AAPL", call["ticker"])
            self.assertEqual(100, equity["holders"][0]["history"][0]["value"])
            self.assertEqual(50, call["holders"][0]["history"][0]["value"])
            self.assertEqual("09290D101", blackrock["stock_id"])
            self.assertEqual("46434G772", ishares["stock_id"])
            self.assertEqual("BLACKROCK INC", blackrock["issuer"])
            self.assertEqual("ISHARES INC", ishares["issuer"])
            self.assertEqual(
                {
                    "037833100",
                    "037833100|CALL",
                    "09290D101",
                    "46434G772",
                },
                {entry["stock_id"] for entry in index["tickers"]},
            )

            errors: list[str] = []
            with mock.patch.object(validate_data, "STOCKS_DIR", stocks_dir):
                validate_data.validate_stocks(errors, registry=registry)
            self.assertEqual([], errors)

            # Coordinated edits to a stock payload and its search row used to
            # agree with each other and evade the cross-file checks.  The SEC
            # registry must remain the independent publication authority.
            equity["ticker"] = "VENDOR"
            equity["issuer"] = "VENDOR ISSUER"
            (stocks_dir / "037833100.json").write_text(json.dumps(equity))
            apple_index = next(
                entry
                for entry in index["tickers"]
                if entry["stock_id"] == "037833100"
            )
            apple_index["ticker"] = "VENDOR"
            apple_index["issuer"] = "VENDOR ISSUER"

            errors.clear()
            with mock.patch.object(validate_data, "STOCKS_DIR", stocks_dir):
                stock_files = validate_data.validate_stocks(
                    errors,
                    registry=registry,
                )
                validate_data.validate_index(
                    index,
                    {"123456": funds_dir / "123456.json"},
                    stock_files,
                    registry,
                    errors,
                    [],
                )
            self.assertTrue(
                any(
                    "stock file 037833100.json ticker 'VENDOR'" in error
                    for error in errors
                ),
                errors,
            )
            self.assertTrue(
                any(
                    "index.json ticker entry 037833100 publishes ticker "
                    "'VENDOR'" in error
                    for error in errors
                ),
                errors,
            )
            self.assertTrue(
                any("canonical SEC registry issuer" in error for error in errors),
                errors,
            )

    def test_current_holder_baseline_prefers_newer_tie_and_skips_withheld(
        self,
    ) -> None:
        funds = [
            {"cik": 1, "q": [20261, 20254]},
            {"cik": 2, "q": [20261, 20254]},
            {"cik": 3, "q": [20262, 20261]},
            {"cik": 4, "q": [20262, 20261]},
            {"cik": 5, "q": [20263, 20262], "status": "WITHHELD"},
        ]

        self.assertEqual(20262, pipeline._modal_latest_reporting_quarter(funds))
        self.assertEqual(
            {3: 20262, 4: 20262},
            pipeline._current_fund_quarters(funds, 20262),
        )
        self.assertEqual(
            20262,
            validate_data._index_modal_latest_reporting_quarter(funds),
        )

    def test_state_migrates_from_legacy_snapshot_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            current_path = root / "data" / "pipeline_state.json"
            legacy_path = root / ".cache" / "pipeline_state.json"
            legacy_path.parent.mkdir(parents=True)
            legacy_path.write_text(json.dumps({
                "processed": ["0000000000-00-000001"],
                "last_run": "2026-04-17T00:00:00Z",
            }))

            with (
                mock.patch.object(pipeline, "STATE_PATH", current_path),
                mock.patch.object(pipeline, "LEGACY_STATE_PATH", legacy_path),
            ):
                state = pipeline.load_state()

            self.assertEqual(
                {"0000000000-00-000001"},
                state["_processed_set"],
            )
            self.assertEqual(
                ["0000000000-00-000001"],
                json.loads(current_path.read_text())["processed"],
            )

    @staticmethod
    def _write_zero_share_funds(
        funds_dir: Path,
        *,
        missing_value: int,
    ) -> None:
        reference = {
            "cik": 1,
            "name": "Reference Fund",
            "quarters": [{
                "report_date": "2025-12-31",
                "filing_date": "2026-02-14",
                "total_value": 1_000,
                "num_holdings": 1,
                "holdings": [{
                    "ticker": None,
                    "issuer": "APPLE INC",
                    "cusip": "037833100",
                    "class": "COM",
                    "value": 1_000,
                    "shares": 10,
                    "holding_type": "EQUITY",
                }],
            }],
        }
        missing = {
            "cik": 2,
            "name": "Missing Shares Fund",
            "quarters": [{
                "report_date": "2025-12-31",
                "filing_date": "2026-02-14",
                "total_value": missing_value,
                "num_holdings": 1,
                "holdings": [{
                    "ticker": None,
                    "issuer": "APPLE INC",
                    "cusip": "037833100",
                    "class": "COM",
                    "value": missing_value,
                    "shares": 0,
                    "holding_type": "EQUITY",
                }],
            }],
        }
        for fund in (reference, missing):
            quarter = fund["quarters"][0]
            quarter["reported_identity_sources"] = [{"accession": "a", "url": "https://www.sec.gov/Archives/a", "sha256": "a" * 64}]
            for holding in quarter["holdings"]:
                holding.update({"share_amount_type": "SH", "accession": "a"})
        for cik in (1, 3, 4):
            reference["cik"] = cik
            (funds_dir / f"{cik}.json").write_text(json.dumps(reference))
        (funds_dir / "2.json").write_text(json.dumps(missing))

    def test_zero_share_repair_imputes_when_value_exceeds_one_share(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir) / "funds"
            funds_dir.mkdir()
            self._write_zero_share_funds(funds_dir, missing_value=250)

            with mock.patch.object(pipeline, "FUNDS_DIR", funds_dir):
                updated = pipeline.repair_zero_share_holdings_in_place()

            holding = json.loads((funds_dir / "2.json").read_text())[
                "quarters"
            ][0]["holdings"][0]
            self.assertEqual(1, updated)
            self.assertEqual(2.5, holding["shares"])
            self.assertIs(holding["shares_imputed"], True)
            self.assertEqual(0, holding["reported_shares"])

    def test_zero_share_repair_keeps_plausible_sub_share_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir) / "funds"
            funds_dir.mkdir()
            self._write_zero_share_funds(funds_dir, missing_value=90)

            with mock.patch.object(pipeline, "FUNDS_DIR", funds_dir):
                updated = pipeline.repair_zero_share_holdings_in_place()

            holding = json.loads((funds_dir / "2.json").read_text())[
                "quarters"
            ][0]["holdings"][0]
            self.assertEqual(0, updated)
            self.assertEqual(0, holding["shares"])
            self.assertNotIn("shares_imputed", holding)

    def test_zero_share_repair_rebuilds_prior_imputations_idempotently(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir) / "funds"
            funds_dir.mkdir()
            self._write_zero_share_funds(funds_dir, missing_value=250)
            target_path = funds_dir / "2.json"
            target = json.loads(target_path.read_text())
            equity = target["quarters"][0]["holdings"][0]
            equity.update({
                "shares": 999,
                "reported_shares": 0,
                "shares_imputed": True,
            })
            target["quarters"][0]["holdings"].append({
                **equity,
                "value": 250,
                "shares": 999,
                "holding_type": "CALL",
                "put_call": "CALL",
            })
            target_path.write_text(json.dumps(target))
            report_date = "2025-12-31"
            original_hash = pipeline.calculate_composition_hash(
                report_date,
                "base-accession",
                ["base-accession"],
                ["a" * 64],
                target["quarters"][0]["holdings"],
                composition_version=1,
            )

            with mock.patch.object(pipeline, "FUNDS_DIR", funds_dir):
                self.assertEqual(1, pipeline.repair_zero_share_holdings_in_place())

            repaired = json.loads(target_path.read_text())
            equity, call = repaired["quarters"][0]["holdings"]
            self.assertEqual(2.5, equity["shares"])
            self.assertIs(equity["shares_imputed"], True)
            self.assertEqual(0, call["shares"])
            self.assertEqual(0, call["reported_shares"])
            self.assertNotIn("shares_imputed", call)
            self.assertEqual(
                original_hash,
                pipeline.calculate_composition_hash(
                    report_date,
                    "base-accession",
                    ["base-accession"],
                    ["a" * 64],
                    repaired["quarters"][0]["holdings"],
                    composition_version=1,
                ),
            )

            first_result = target_path.read_text()
            with mock.patch.object(pipeline, "FUNDS_DIR", funds_dir):
                self.assertEqual(1, pipeline.repair_zero_share_holdings_in_place())
            self.assertEqual(first_result, target_path.read_text())

    def test_validation_preserves_unestimated_reported_zero_share_row(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir) / "funds"
            funds_dir.mkdir()
            self._write_zero_share_funds(funds_dir, missing_value=250)
            errors: list[str] = []

            with mock.patch.object(validate_data, "FUNDS_DIR", funds_dir):
                validate_data.validate_funds(errors)

            self.assertEqual([], errors)


if __name__ == "__main__":
    unittest.main()
