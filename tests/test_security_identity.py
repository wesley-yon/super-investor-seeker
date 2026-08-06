from __future__ import annotations

import json
import re
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from unittest import mock

import pipeline
import security_identity
import validate_data


class InstrumentTypeTests(unittest.TestCase):
    def test_normalize_instrument_type_accepts_clean_strings(self) -> None:
        cases = {
            "CALL": "CALL",
            "put": "PUT",
            "  pref  ": "PREF",
            "WARRANT": "WARRANT",
        }
        for raw, expected in cases.items():
            with self.subTest(raw=raw):
                self.assertEqual(
                    expected,
                    security_identity.normalize_instrument_type(raw),
                )

    def test_normalize_instrument_type_defaults_unknown_values_to_equity(
        self,
    ) -> None:
        for raw in (None, "", "stock", "unknown", 123):
            with self.subTest(raw=raw):
                self.assertEqual(
                    "EQUITY",
                    security_identity.normalize_instrument_type(raw),
                )

    def test_supported_type_constants_are_stable(self) -> None:
        expected = (
            "EQUITY",
            "PREF",
            "NOTE",
            "WARRANT",
            "CALL",
            "PUT",
            "OPT",
        )
        self.assertEqual(expected, security_identity.INSTRUMENT_TYPES)
        self.assertEqual(frozenset(expected), security_identity.VALID_INSTRUMENT_TYPES)

    def test_explicit_put_call_wins_over_saved_or_legacy_type(self) -> None:
        self.assertEqual(
            "CALL",
            security_identity.holding_instrument_type(
                {
                    "put_call": "call",
                    "holding_type": "PUT",
                    "option_type": "PREF",
                }
            ),
        )
        self.assertEqual(
            "PUT",
            security_identity.holding_instrument_type(
                {"put_call": "PUT", "holding_type": "WARRANT"}
            ),
        )

    def test_saved_type_is_used_when_explicit_option_side_is_absent(self) -> None:
        self.assertEqual(
            "NOTE",
            security_identity.holding_instrument_type({"holding_type": "note"}),
        )
        self.assertEqual(
            "CALL",
            security_identity.holding_instrument_type({"option_type": "call"}),
        )
        self.assertEqual(
            "EQUITY",
            security_identity.holding_instrument_type(None),
        )


class NoteSecurityLabelTests(unittest.TestCase):
    def test_normalizes_supported_openfigi_note_labels(self) -> None:
        cases = {
            "RIVN 3.625 10/15/30": "RIVN 3.625 10/15/30",
            "  rivn   3 5/8   10/15/30  ": "RIVN 3 5/8 10/15/30",
            "BILL 0 04/01/30": "BILL 0 04/01/30",
            "BAC 4.3 PERP L": "BAC 4.3 PERP L",
            "UBER 0.875 12/01/28 2028": "UBER 0.875 12/01/28",
            "EXAMPLE FLT 06/01/31": "EXAMPLE FLT 06/01/31",
            "EXAMPLE VAR 06/01/31": "EXAMPLE VAR 06/01/31",
            "ADT 4.125 08/01/29 144A": "ADT 4.125 08/01/29 144A",
            "AES V7.6 01/15/55": "AES V7.6 01/15/55",
            "MN ADRSCD 4 02/01/2041": "MN ADRSCD 4 02/01/2041",
            "AMCX 4.25 02/15/29 *": "AMCX 4.25 02/15/29 *",
            "TNDM 1.5 03/15/29 2024": "TNDM 1.5 03/15/29 2024",
            "BNP 0 04/07/25 0018": "BNP 0 04/07/25 0018",
            "C 0 09/03/27 0M!N": "C 0 09/03/27 0M!N",
            "IONPLA 8.75 05/01/29 144@": "IONPLA 8.75 05/01/29 144@",
            "MT FORPOL 03/01/2031": "MT FORPOL 03/01/2031",
        }
        for raw, expected in cases.items():
            with self.subTest(raw=raw):
                self.assertEqual(
                    expected,
                    security_identity.normalize_note_security_label(raw),
                )

    def test_rejects_plain_or_malformed_symbols_and_bad_year_suffixes(
        self,
    ) -> None:
        for raw in (
            None,
            "",
            "RIVN",
            "76954AAD5",
            "RIVN 3.625",
            "RIVN NOTE 10/15/30",
        ):
            with self.subTest(raw=raw):
                self.assertIsNone(
                    security_identity.normalize_note_security_label(raw)
                )

    def test_type_gate_keeps_note_labels_off_other_instruments(self) -> None:
        label = "RIVN 3.625 10/15/30"
        self.assertEqual(
            label,
            pipeline.display_ticker_for_holding_type(label, "NOTE"),
        )
        self.assertIsNone(
            pipeline.display_ticker_for_holding_type(label, "EQUITY")
        )
        self.assertIsNone(
            pipeline.display_ticker_for_holding_type("RIVN", "NOTE")
        )
        self.assertEqual(
            "RIVN",
            pipeline.display_ticker_for_holding_type("RIVN", "EQUITY"),
        )
        self.assertIsNone(
            pipeline._classify_ticker_health(
                "76954AAD5",
                label,
                "NOTE",
            )
        )
        self.assertEqual(
            "suspicious_symbol",
            pipeline._classify_ticker_health(
                "76954AAD5",
                label,
                "EQUITY",
            ),
        )
        self.assertEqual(
            "suspicious_symbol",
            pipeline._classify_ticker_health(
                "76954AAD5",
                "RIVN",
                "NOTE",
            ),
        )


class GenericSecurityLabelTests(unittest.TestCase):
    def test_normalizes_only_supported_display_kinds(self) -> None:
        self.assertEqual(
            "CLOSED-END FUND",
            security_identity.normalize_security_kind(
                "  closed-end   fund "
            ),
        )
        self.assertIsNone(
            security_identity.normalize_security_kind("OPTION")
        )
        self.assertEqual(
            "RIGHT",
            security_identity.normalize_security_kind(" right "),
        )
        self.assertEqual(
            "UNIT",
            security_identity.normalize_security_kind("unit"),
        )
        self.assertEqual(
            "ETN",
            security_identity.normalize_security_kind("etn"),
        )

    def test_normalizes_useful_openfigi_labels_and_plain_symbols(self) -> None:
        cases = {
            "  NEE   7.375   02/15/29  ": "NEE 7.375 02/15/29",
            "BSV": "BSV",
            " IWM ": "IWM",
            "BAC 7.25 PERP L": "BAC 7.25 PERP L",
            "AMCAR 2017-3 D": "AMCAR 2017-3 D",
        }
        for raw, expected in cases.items():
            with self.subTest(raw=raw):
                self.assertEqual(
                    expected,
                    security_identity.normalize_security_label(raw),
                )

    def test_explicit_etn_metadata_outranks_openfigi_etp_taxonomy(
        self,
    ) -> None:
        kind, source = pipeline._registry_security_kind(
            identifier="90274E174",
            openfigi_detail={
                "status": "matched",
                "securityType": "ETP",
                "securityType2": "Mutual Fund",
            },
            prior_entry=None,
            entry={
                "name": "UBS AG",
                "dominant_issuer": "UBS AG",
                "dominant_class": "ETRACS 2XM ETN",
                "type": "EQUITY",
            },
        )
        self.assertEqual("ETN", kind)
        self.assertEqual("filer_metadata", source)

    def test_explicit_sec_common_class_fills_missing_openfigi_kind(
        self,
    ) -> None:
        kind, source = pipeline._registry_security_kind(
            identifier="04626A103",
            openfigi_detail=None,
            prior_entry=None,
            entry={
                "ticker": "ALAB",
                "name": "Astera Labs, Inc.",
                "dominant_issuer": "ASTERA LABS INC",
                "dominant_class": "COM",
                "type": "EQUITY",
                "sources": ["sec_title", "cusip_map_vetted"],
            },
        )
        self.assertEqual("COMMON", kind)
        self.assertEqual("filer_metadata", source)

    def test_stale_filer_fund_kind_is_not_preserved_without_current_evidence(
        self,
    ) -> None:
        entry = {
            "ticker": "LISSX",
            "name": "LIONESS CAPITAL",
            "dominant_issuer": "LIONESS CAPITAL",
            "dominant_class": "MFF",
            "type": "EQUITY",
            "sources": ["sec_title", "cusip_map_vetted"],
        }
        self.assertIsNone(pipeline._filer_security_kind(entry))
        self.assertEqual(
            (None, None),
            pipeline._registry_security_kind(
                identifier="53625T101",
                openfigi_detail=None,
                prior_entry={
                    **entry,
                    "security_kind": "MUTUAL FUND",
                    "security_kind_source": "filer_metadata",
                },
                entry=entry,
            ),
        )

    def test_current_filer_kind_replaces_stale_filer_prior(self) -> None:
        entry = {
            "ticker": "ITOT",
            "name": "ISHARES TR",
            "dominant_issuer": "ISHARES TR",
            "dominant_class": "CORE S&P TTL STK",
            "type": "EQUITY",
            "sources": ["sec_title", "cusip_map_vetted"],
        }
        self.assertEqual(
            ("ETF", "filer_metadata"),
            pipeline._registry_security_kind(
                identifier="464287150",
                openfigi_detail=None,
                prior_entry={
                    **entry,
                    "security_kind": "MUTUAL FUND",
                    "security_kind_source": "filer_metadata",
                },
                entry=entry,
            ),
        )

    def test_specific_filer_kind_outranks_openfigi_common(self) -> None:
        openfigi_common = {
            "status": "matched",
            "securityType": "Common Stock",
            "securityType2": "Common Stock",
            "marketSector": "Equity",
        }
        cases = (
            (
                {
                    "dominant_issuer": "GENERIC ISSUER",
                    "dominant_class": "UNIT SER 1",
                    "type": "EQUITY",
                },
                "UNIT",
            ),
            (
                {
                    "dominant_issuer": "GENERIC ISSUER",
                    "dominant_class": "PREFERRED SHARES",
                    "type": "EQUITY",
                },
                "PREFERRED",
            ),
            (
                {
                    "dominant_issuer": "GENERIC ISSUER",
                    "dominant_class": "WARRANT",
                    "type": "EQUITY",
                },
                "WARRANT",
            ),
            (
                {
                    "dominant_issuer": "ISHARES TRUST",
                    "dominant_class": "CORE INDEX ETF",
                    "type": "EQUITY",
                },
                "ETF",
            ),
            (
                {
                    "dominant_issuer": "GENERIC CLOSED-END FUND",
                    "dominant_class": "COMMON SHARES",
                    "type": "EQUITY",
                },
                "CLOSED-END FUND",
            ),
        )
        for entry, expected in cases:
            with self.subTest(expected=expected):
                self.assertEqual(
                    (expected, "filer_metadata"),
                    pipeline._registry_security_kind(
                        identifier="123456789",
                        openfigi_detail=openfigi_common,
                        prior_entry=None,
                        entry=entry,
                    ),
                )

    def test_depositary_receipt_is_not_promoted_to_common_stock(
        self,
    ) -> None:
        depositary_detail = {
            "status": "matched",
            "securityType": "Depositary Receipt",
            "securityType2": "Depositary Receipt",
            "marketSector": "Equity",
        }
        common_entry = {
            "ticker": "ASML",
            "dominant_issuer": "ASML HOLDING NV",
            "dominant_class": "COM",
            "type": "EQUITY",
            "sources": ["sec_title"],
        }
        self.assertIsNone(
            pipeline._openfigi_security_kind(depositary_detail)
        )
        self.assertEqual(
            (None, None),
            pipeline._registry_security_kind(
                identifier="N07059210",
                openfigi_detail=depositary_detail,
                prior_entry={
                    "security_kind": "COMMON",
                    "security_kind_source": "openfigi",
                },
                entry=common_entry,
            ),
        )
        preferred_entry = {
            **common_entry,
            "dominant_class": "PREFERRED SHARES",
        }
        self.assertEqual(
            ("PREFERRED", "filer_metadata"),
            pipeline._registry_security_kind(
                identifier="123456789",
                openfigi_detail=depositary_detail,
                prior_entry=None,
                entry=preferred_entry,
            ),
        )
        generic_common_detail = {
            "status": "matched",
            "securityType": "Common Stock",
            "securityType2": "Common Stock",
            "marketSector": "Equity",
        }
        for filer_fields in (
            {
                "dominant_issuer": "GENERIC ISSUER SPONSORED ADR",
                "dominant_class": "COM",
            },
            {
                "dominant_issuer": "GENERIC ISSUER",
                "dominant_class": "SP ADR REP COM",
            },
            {
                "dominant_issuer": "GENERIC ISSUER DEPOSITARY SHS",
                "dominant_class": "COM",
            },
        ):
            with self.subTest(filer_fields=filer_fields):
                self.assertEqual(
                    (None, None),
                    pipeline._registry_security_kind(
                        identifier="123456789",
                        openfigi_detail=generic_common_detail,
                        prior_entry={
                            "security_kind": "COMMON",
                            "security_kind_source": "openfigi",
                        },
                        entry={
                            "ticker": "GEN",
                            "type": "EQUITY",
                            "sources": ["sec_title"],
                            **filer_fields,
                        },
                    ),
                )

    def test_verified_us_warrant_venue_codes_keep_exact_figi_tickers(
        self,
    ) -> None:
        for exchange_code, ticker in {
            "UM": "CIFRW",
            "UX": "EVGOW",
            "UD": "HTOOW",
            "UB": "IMTXW",
        }.items():
            detail = {
                "status": "matched",
                "ticker": ticker,
                "securityDescription": ticker,
                "marketSector": "Equity",
                "securityType": "Equity WRT",
                "securityType2": "Warrant",
                "exchCode": exchange_code,
            }
            with self.subTest(exchange_code=exchange_code, ticker=ticker):
                self.assertEqual(
                    ticker,
                    pipeline._openfigi_canonical_ticker(
                        detail,
                        identifier="123456789",
                        instrument_type="WARRANT",
                        dominant_class="WARRANT",
                    ),
                )
                self.assertEqual(
                    ticker,
                    pipeline._openfigi_security_label(
                        detail,
                        "123456789",
                    ),
                )

    def test_verified_special_security_overrides_outrank_noisy_common_class(
        self,
    ) -> None:
        for identifier, (ticker, expected) in {
            "09032H113": ("BCGWW", "WARRANT"),
            "128745114": ("ATCHW", "WARRANT"),
            "459867123": ("IMAQR", "RIGHT"),
            "590188108": ("IPB", "BOND"),
            "74319X116": ("NVACW", "WARRANT"),
            "97655B125": ("WINVR", "RIGHT"),
        }.items():
            with self.subTest(identifier=identifier):
                kind, source = pipeline._registry_security_kind(
                    identifier=identifier,
                    openfigi_detail=None,
                    prior_entry=None,
                    entry={
                        "ticker": ticker,
                        "name": "SEC-TITLED ISSUER",
                        "dominant_class": "COM",
                        "type": "EQUITY",
                        "sources": ["sec_title", "cusip_map_vetted"],
                    },
                )
                self.assertEqual(expected, kind)
                self.assertEqual("manual_verified", source)

    def test_rejects_blank_control_identifier_and_overlong_labels(
        self,
    ) -> None:
        for raw in (
            None,
            "",
            "   ",
            "#N/A",
            "N/A",
            "NONE",
            "INVALID",
            "LOOK IT UP",
            "#N/A INVALID SECURITY",
            "ACME INVALID SECURITY",
            "0",
            "698",
            "NEE\n7.375 02/15/29",
            "NEE\x00 7.375 02/15/29",
            "\u200bBSV",
            "INNOVIZ TECHNOLOGIES LTD W EXP 04/05/202",
            "X" * 161,
        ):
            with self.subTest(raw=raw):
                self.assertIsNone(
                    security_identity.normalize_security_label(raw)
                )
        self.assertIsNone(
            security_identity.normalize_security_label(
                "65339F655",
                "65339f655",
            )
        )
        self.assertEqual(
            "3M CO",
            security_identity.normalize_security_label("3M CO"),
        )

    def test_composes_issuer_with_informative_security_class(self) -> None:
        self.assertEqual(
            "NEXTERA ENERGY INC — UNIT 02/15/2029",
            security_identity.compose_security_label(
                "NEXTERA ENERGY INC",
                "UNIT 02/15/2029",
                "PREF",
                "65339F655",
            ),
        )
        self.assertEqual(
            "MA-COM TECH — 0 12/15/29",
            security_identity.compose_security_label(
                "MA-COM TECH",
                "0 15/12/2029",
                "NOTE",
                "55405YAC4",
            ),
        )
        self.assertEqual(
            "EXAMPLE ISSUER — 0 11/12/2029",
            security_identity.compose_security_label(
                "EXAMPLE ISSUER",
                "0 11/12/2029",
                "NOTE",
            ),
        )

    def test_ignores_generic_classes_and_placeholder_dates(self) -> None:
        self.assertEqual(
            "VANGUARD BD INDEX FDS",
            security_identity.compose_security_label(
                "VANGUARD BD INDEX FDS",
                "COMMON STOCK",
                "EQUITY",
                "921937108",
            ),
        )
        self.assertEqual(
            "ISHARES TR",
            security_identity.compose_security_label(
                "ISHARES TR",
                "UNIT 00/00/0000",
                "NOTE",
                "464287655",
            ),
        )
        self.assertEqual(
            "NOTE SECURITY",
            security_identity.compose_security_label(
                None,
                "NOTE",
                "NOTE",
                "76954AAD5",
            ),
        )
        self.assertEqual(
            "SER A",
            security_identity.compose_security_label(
                None,
                "SER A 99/99/9999",
                "PREF",
                "65339F655",
            ),
        )
        self.assertEqual(
            "EXAMPLE ISSUER",
            security_identity.compose_security_label(
                "EXAMPLE ISSUER",
                "*W EXP 10/30/202",
                "WARRANT",
            ),
        )
        self.assertEqual(
            "EXAMPLE ISSUER",
            security_identity.compose_security_label(
                "EXAMPLE ISSUER",
                "PFD #N/A",
                "PREF",
            ),
        )
        self.assertEqual(
            "BANC ONE CORP",
            security_identity.compose_security_label(
                "BANC ONE CORP",
                "5552357",
                "EQUITY",
            ),
        )
        self.assertEqual(
            "RIGHT",
            security_identity.compose_security_label(
                "#N/A INVALID SECURITY",
                "RIGHT",
                "EQUITY",
            ),
        )
        self.assertEqual(
            "EQUITY SECURITY",
            security_identity.compose_security_label(
                "0",
                "0",
                "EQUITY",
            ),
        )
        self.assertEqual(
            "EQUITY SECURITY",
            security_identity.compose_security_label(
                "LOOK IT UP",
                "LOOK IT UP",
                "EQUITY",
            ),
        )
        self.assertEqual(
            "EXAMPLE ISSUER — 0 01/03/2030",
            security_identity.compose_security_label(
                "EXAMPLE ISSUER",
                "0 01/03/2030",
                "NOTE",
            ),
        )

    def test_cusip_issuer_uses_class_or_type_fallback(self) -> None:
        self.assertEqual(
            "UNIT 02/15/2029",
            security_identity.compose_security_label(
                "65339F655",
                "UNIT 02/15/2029",
                "PREF",
                "65339F655",
            ),
        )
        self.assertEqual(
            "PREF SECURITY",
            security_identity.compose_security_label(
                "65339F655",
                "PREF",
                "PREF",
                "65339F655",
            ),
        )


class StockLookupIdTests(unittest.TestCase):
    def test_canonical_identifier_rejects_delimiter_and_normalization_drift(
        self,
    ) -> None:
        self.assertTrue(
            security_identity.is_canonical_security_identifier("037833100")
        )
        for raw in (None, "", " 037833100 ", "abc123", "ABC|CALL", 123):
            with self.subTest(raw=raw):
                self.assertFalse(
                    security_identity.is_canonical_security_identifier(raw)
                )

    def test_stock_lookup_id_normalizes_identifier_and_suffix(self) -> None:
        self.assertEqual(
            "037833100",
            security_identity.stock_lookup_id(" 037833100 ", "equity"),
        )
        self.assertEqual(
            "037833100|CALL",
            security_identity.stock_lookup_id(
                "037833100",
                "CALL",
            ),
        )
        self.assertEqual(
            "BRK-B|PREF",
            security_identity.stock_lookup_id("brk-b", "pref"),
        )

    def test_stock_lookup_id_returns_empty_for_a_blank_identifier(self) -> None:
        self.assertEqual("", security_identity.stock_lookup_id(None, "CALL"))
        self.assertEqual("", security_identity.stock_lookup_id("  ", "PUT"))

    def test_compatibility_builder_names_are_aliases(self) -> None:
        self.assertIs(
            security_identity.stock_lookup_id,
            security_identity.make_stock_lookup_id,
        )

    def test_parse_stock_lookup_id_normalizes_valid_and_invalid_suffixes(
        self,
    ) -> None:
        self.assertEqual(
            ("037833100", "EQUITY"),
            security_identity.parse_stock_lookup_id(" 037833100 "),
        )
        self.assertEqual(
            ("037833100", "CALL"),
            security_identity.parse_stock_lookup_id("037833100|call"),
        )
        self.assertEqual(
            ("037833100", "EQUITY"),
            security_identity.parse_stock_lookup_id("037833100|unknown"),
        )

    def test_lookup_id_round_trip_covers_every_supported_type(self) -> None:
        for instrument_type in security_identity.INSTRUMENT_TYPES:
            with self.subTest(instrument_type=instrument_type):
                stock_id = security_identity.stock_lookup_id(
                    "abc123",
                    instrument_type,
                )
                self.assertEqual(
                    ("ABC123", instrument_type),
                    security_identity.parse_stock_lookup_id(stock_id),
                )


class StockFilenameTests(unittest.TestCase):
    def test_safe_ticker_preserves_supported_characters_and_replaces_others(
        self,
    ) -> None:
        self.assertEqual(
            "BRK-B._X",
            security_identity.safe_ticker(" brk-b./x "),
        )

    def test_stock_file_stem_matches_existing_equity_and_option_layout(
        self,
    ) -> None:
        self.assertEqual(
            "037833100",
            security_identity.stock_file_stem("037833100"),
        )
        self.assertEqual(
            "037833100__CALL",
            security_identity.stock_file_stem("037833100|CALL"),
        )
        self.assertEqual(
            "BRK_B__PUT",
            security_identity.stock_file_stem("brk/b|put"),
        )

    def test_stock_filename_matches_pipeline_naming(self) -> None:
        self.assertEqual(
            "037833100.json",
            security_identity.stock_filename("037833100", "EQUITY"),
        )
        self.assertEqual(
            "037833100__CALL.json",
            security_identity.stock_filename("037833100", "CALL"),
        )
        self.assertEqual("", security_identity.stock_filename("", "CALL"))


class FrontendIdentityContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.html = (Path(__file__).resolve().parents[1] / "index.html").read_text()

    def test_frontend_supported_types_match_shared_contract(self) -> None:
        match = re.search(
            r"function normalizeInstrumentType\(type\).*?"
            r"return \[(.*?)\]\.includes",
            self.html,
            re.DOTALL,
        )
        self.assertIsNotNone(match)
        self.assertEqual(
            security_identity.INSTRUMENT_TYPES,
            tuple(re.findall(r'"([A-Z]+)"', match.group(1))),
        )

    def test_frontend_normalizes_instrument_type_whitespace_and_case(
        self,
    ) -> None:
        self.assertRegex(
            self.html,
            r"function normalizeInstrumentType\(type\)\s*\{\s*"
            r'const t = String\(type \|\| "EQUITY"\)'
            r"\.trim\(\)\.toUpperCase\(\);",
        )

    def test_frontend_history_and_ticker_metadata_use_parsed_stock_identity(
        self,
    ) -> None:
        self.assertRegex(
            self.html,
            r"(?s)function holdingInstrumentType\(holding\).*?"
            r"holding\?\.put_call.*?\[\"CALL\", \"PUT\"\]\.includes",
        )
        self.assertRegex(
            self.html,
            r"(?s)function holdingHistoryKey\(h\)\s*\{\s*"
            r"return stockLookupId\(.*?holdingPublishedInstrumentType\(h\)",
        )
        for field in (
            "cusip: parsed.id_base",
            "instrument_type: parsed.instrument_type",
            "stock_id: parsed.stock_id",
        ):
            self.assertIn(field, self.html)


class SecValidatedTickerAliasTests(unittest.TestCase):
    def test_company_ticker_maps_require_unique_structural_preference(
        self,
    ) -> None:
        company_tickers = [
            {"ticker": "PFH", "title": "PRUDENTIAL FINANCIAL INC"},
            {"ticker": "PRU", "title": "PRUDENTIAL FINANCIAL INC"},
            {"ticker": "PRU", "title": "PRUDENTIAL FINANCIAL INC"},
            {"ticker": "AAPL", "title": "APPLE INC"},
            {"ticker": "BAC", "title": "BANK OF AMERICA CORP"},
            {"ticker": "BAC-PB", "title": "BANK OF AMERICA CORP"},
        ]

        with mock.patch.object(
            pipeline,
            "_load_company_tickers_data",
            return_value=company_tickers,
        ):
            name_to_ticker, ticker_to_norms = (
                pipeline.fetch_company_ticker_maps()
            )

        issuer_key = pipeline.normalize_name("PRUDENTIAL FINANCIAL INC")
        self.assertNotIn(issuer_key, name_to_ticker)
        apple_key = pipeline.normalize_name("APPLE INC")
        self.assertEqual("AAPL", name_to_ticker[apple_key])
        bank_key = pipeline.normalize_name("BANK OF AMERICA CORP")
        self.assertEqual("BAC", name_to_ticker[bank_key])
        self.assertEqual({issuer_key}, ticker_to_norms["PRU"])
        self.assertEqual({issuer_key}, ticker_to_norms["PFH"])
        self.assertEqual({apple_key}, ticker_to_norms["AAPL"])
        self.assertEqual({bank_key}, ticker_to_norms["BAC"])
        self.assertEqual({bank_key}, ticker_to_norms["BAC-PB"])

    def test_only_narrow_existing_ticker_edits_are_accepted(self) -> None:
        sec_titles = {
            "TRI": "THOMSON REUTERS CORP /CAN/",
            "HON": "HONEYWELL INTERNATIONAL INC",
            "BF-B": "BROWN FORMAN INC",
            "SCO": "PROSHARES ULTRASHORT BLOOMBERG CRUDE OIL",
            "WEAT": "TEUCRIUM WHEAT FUND",
            "WT": "WISDOMTREE, INC.",
            "AAPL": "APPLE INC.",
        }
        cases = (
            ("TRI4EUR", "THOMSON REUTERS CORP", "TRI"),
            ("HONGBP", "HONEYWELL INTL INC", "HON"),
            ("BF/B", "BROWN FORMAN INC", "BF-B"),
            (
                "SCOUSD",
                "PROSHARES ULTRASHORT BLOOMBERG CRUDE OIL",
                "SCO",
            ),
            ("WEATUSD", "TEUCRIUM WHEAT FUND", "WEAT"),
            ("DGRW", "WISDOMTREE INC", None),
            ("SWGXX", "APPLE INC", None),
        )
        for source_ticker, issuer, expected in cases:
            with self.subTest(source_ticker=source_ticker):
                self.assertEqual(
                    expected,
                    pipeline._validated_sec_ticker_alias(
                        source_ticker,
                        issuer,
                        sec_titles,
                    ),
                )
        self.assertIsNone(
            pipeline._validated_sec_ticker_alias(
                "ABCUSD",
                "EXAMPLE ISSUER INC",
                {
                    "ABCUSD": "EXAMPLE ISSUER INC",
                    "ABC": "EXAMPLE ISSUER INC",
                },
            )
        )

    def test_registry_build_preserves_alias_proof_and_does_not_name_remap(
        self,
    ) -> None:
        rows = (
            ("000000001", "TRI4EUR", "THOMSON REUTERS CORP", "TRI"),
            ("000000002", "HONGBP", "HONEYWELL INTL INC", "HON"),
            ("000000003", "BF/B", "BROWN FORMAN INC", "BF-B"),
            (
                "000000004",
                "SCOUSD",
                "PROSHARES ULTRASHORT BLOOMBERG CRUDE OIL",
                "SCO",
            ),
            ("000000005", "WEATUSD", "TEUCRIUM WHEAT FUND", "WEAT"),
            ("000000006", "DGRW", "WISDOMTREE INC", "DGRW"),
            ("000000007", "SWGXX", "APPLE INC", "SWGXX"),
        )
        evidence = {}
        cusip_map = {}
        for cusip, source_ticker, issuer, _expected in rows:
            evidence[cusip] = {
                "total_value": 100,
                "holder_ciks": {1},
                "issuer_value": {issuer: 100},
                "class_value": {"COM": 100},
                "put_call_value": {},
                "first_seen": "2025-12-31",
                "last_seen": "2025-12-31",
            }
            cusip_map[cusip] = source_ticker

        company_tickers = [
            {"ticker": "TRI", "title": "THOMSON REUTERS CORP /CAN/"},
            {"ticker": "HON", "title": "HONEYWELL INTERNATIONAL INC"},
            {"ticker": "BF-B", "title": "BROWN FORMAN INC"},
            {
                "ticker": "SCO",
                "title": "PROSHARES ULTRASHORT BLOOMBERG CRUDE OIL",
            },
            {"ticker": "WEAT", "title": "TEUCRIUM WHEAT FUND"},
            {"ticker": "WT", "title": "WISDOMTREE, INC."},
            {"ticker": "AAPL", "title": "APPLE INC."},
        ]

        with mock.patch.multiple(
            pipeline,
            FUNDS_DIR=mock.MagicMock(exists=mock.Mock(return_value=True)),
            _aggregate_cusip_evidence=mock.Mock(return_value=evidence),
            load_cusip_map=mock.Mock(return_value=cusip_map),
            _load_company_tickers_data=mock.Mock(
                return_value=company_tickers
            ),
            save_cusip_registry=mock.Mock(),
        ):
            registry = pipeline.build_cusip_registry()

        for cusip, source_ticker, _issuer, expected in rows:
            with self.subTest(cusip=cusip):
                entry = registry[cusip]
                self.assertEqual(expected, entry["ticker"])
                if expected != source_ticker:
                    self.assertEqual(source_ticker, entry["source_ticker"])
                    self.assertIn(
                        "sec_validated_ticker_alias",
                        entry["sources"],
                    )
                else:
                    self.assertNotIn("source_ticker", entry)
                    self.assertNotIn(
                        "sec_validated_ticker_alias",
                        entry["sources"],
                    )

        self.assertNotEqual("WT", registry["000000006"]["ticker"])
        self.assertNotEqual("AAPL", registry["000000007"]["ticker"])

    def test_collision_validator_only_exempts_same_issuer_proven_aliases(
        self,
    ) -> None:
        alias_entry = {
            "ticker": "TRI",
            "source_ticker": "TRI4EUR",
            "name": "THOMSON REUTERS CORP /CAN/",
            "dominant_issuer": "THOMSON REUTERS CORP",
            "type": "EQUITY",
            "sources": ["sec_title", "sec_validated_ticker_alias"],
        }
        canonical_entry = {
            "ticker": "TRI",
            "name": "THOMSON REUTERS CORP /CAN/",
            "dominant_issuer": "THOMSON REUTERS CORP",
            "type": "EQUITY",
            "sources": ["manual_override", "sec_title"],
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            public_path = Path(tmpdir) / "cusip_registry.json"

            def validate(registry: dict) -> list[str]:
                public_path.write_text(json.dumps(registry))
                with mock.patch.multiple(
                    pipeline,
                    LEGACY_CUSIP_REGISTRY_PATH=public_path,
                    load_cusip_registry=mock.Mock(return_value=registry),
                    _aggregate_cusip_evidence=mock.Mock(
                        return_value={cusip: {} for cusip in registry}
                    ),
                ):
                    return pipeline.validate_cusip_registry()

            valid_registry = {
                "884903808": alias_entry,
                "884903881": canonical_entry,
            }
            valid_issues = validate(valid_registry)
            self.assertFalse(
                any("claimed by multiple CUSIPs" in issue for issue in valid_issues),
                valid_issues,
            )

            unrelated_entry = {
                **canonical_entry,
                "dominant_issuer": "APPLE INC",
                "name": "APPLE INC",
            }
            invalid_issues = validate(
                {
                    "884903808": alias_entry,
                    "037833100": unrelated_entry,
                }
            )
            self.assertTrue(
                any(
                    "claimed by multiple CUSIPs" in issue
                    for issue in invalid_issues
                ),
                invalid_issues,
            )

    def test_alias_is_idempotent_across_incremental_and_full_rebuilds(
        self,
    ) -> None:
        company_tickers = [
            {"ticker": "TRI", "title": "THOMSON REUTERS CORP /CAN/"},
        ]
        for full_refresh in (False, True):
            with self.subTest(full_refresh=full_refresh):
                with tempfile.TemporaryDirectory() as tmpdir:
                    root = Path(tmpdir)
                    data_dir = root / "data"
                    funds_dir = data_dir / "funds"
                    cache_dir = root / ".cache"
                    funds_dir.mkdir(parents=True)
                    cache_dir.mkdir()
                    fund_path = funds_dir / "123.json"
                    fund_path.write_text(json.dumps({
                        "cik": 123,
                        "name": "Alias Test Fund",
                        "quarters": [{
                            "report_date": "2026-03-31",
                            "total_value": 100100,
                            "num_holdings": 2,
                            "holdings": [
                                {
                                    "cusip": "884903808",
                                    "ticker": "TRI",
                                    "issuer": "THOMSON REUTERS CORP /CAN/",
                                    "class": "COM",
                                    "holding_type": "EQUITY",
                                    "value": 100000,
                                    "shares": 1000,
                                },
                                {
                                    "cusip": "884903881",
                                    "ticker": "TRI",
                                    "issuer": "THOMSON REUTERS CORP /CAN/",
                                    "class": "COM",
                                    "holding_type": "EQUITY",
                                    "value": 100,
                                    "shares": 1,
                                },
                            ],
                        }],
                    }))
                    map_path = cache_dir / "cusip_map.json"
                    map_path.write_text(json.dumps({
                        "884903808": "TRI4EUR",
                        "884903881": "TRI",
                    }))
                    registry_path = cache_dir / "cusip_registry.json"
                    public_registry_path = data_dir / "cusip_registry.json"
                    initial_registry = {
                        "884903808": {
                            "ticker": "TRI",
                            "source_ticker": "TRI4EUR",
                            "name": "THOMSON REUTERS CORP /CAN/",
                            "dominant_issuer": (
                                "THOMSON REUTERS CORP /CAN/"
                            ),
                            "type": "EQUITY",
                            "sources": [
                                "sec_title",
                                "sec_validated_ticker_alias",
                            ],
                        },
                        "884903881": {
                            "ticker": "TRI",
                            "name": "THOMSON REUTERS CORP /CAN/",
                            "dominant_issuer": (
                                "THOMSON REUTERS CORP /CAN/"
                            ),
                            "type": "EQUITY",
                            "sources": ["sec_title", "cusip_map_vetted"],
                        },
                    }
                    registry_path.write_text(json.dumps(initial_registry))
                    public_registry_path.write_text(
                        json.dumps(initial_registry)
                    )

                    with (
                        mock.patch.multiple(
                            pipeline,
                            FUNDS_DIR=funds_dir,
                            CUSIP_MAP_PATH=map_path,
                            LEGACY_CUSIP_MAP_PATH=(
                                data_dir / "legacy-cusip-map.json"
                            ),
                            CUSIP_REGISTRY_PATH=registry_path,
                            LEGACY_CUSIP_REGISTRY_PATH=public_registry_path,
                        ),
                        mock.patch.object(
                            pipeline,
                            "resolve_cusips_via_openfigi",
                            return_value={"884903881": "TRI"},
                        ),
                        mock.patch.object(
                            pipeline,
                            "get_openfigi_api_key",
                            return_value="test-key",
                        ),
                    ):
                        snapshots = []
                        for _iteration in range(2):
                            pipeline.rebuild_tickers_in_place(
                                full_refresh=full_refresh,
                                company_ticker_data=company_tickers,
                            )
                            pipeline.build_cusip_registry(
                                full_refresh=full_refresh,
                                company_ticker_data=company_tickers,
                            )
                            pipeline.canonicalize_fund_files()
                            snapshots.append((
                                json.loads(map_path.read_text()),
                                json.loads(registry_path.read_text()),
                                json.loads(fund_path.read_text()),
                            ))

                    self.assertEqual(snapshots[0], snapshots[1])
                    self.assertEqual(
                        "TRI4EUR",
                        snapshots[1][0]["884903808"],
                    )
                    self.assertEqual(
                        "TRI",
                        snapshots[1][1]["884903808"]["ticker"],
                    )
                    self.assertEqual(
                        "TRI4EUR",
                        snapshots[1][1]["884903808"]["source_ticker"],
                    )
                    self.assertEqual(
                        ["TRI", "TRI"],
                        [
                            holding["ticker"]
                            for holding in snapshots[1][2]["quarters"][0][
                                "holdings"
                            ]
                        ],
                    )


class IndependentRegistryAliasValidationTests(unittest.TestCase):
    company_tickers = {
        "0": {
            "cik_str": 1075124,
            "ticker": "TRI",
            "title": "THOMSON REUTERS CORP /CAN/",
        },
    }
    alias_entry = {
        "ticker": "TRI",
        "source_ticker": "TRI4EUR",
        "name": "THOMSON REUTERS CORP /CAN/",
        "dominant_issuer": "THOMSON REUTERS CORP",
        "type": "EQUITY",
        "sources": ["sec_title", "sec_validated_ticker_alias"],
    }
    canonical_entry = {
        "ticker": "TRI",
        "name": "THOMSON REUTERS CORP /CAN/",
        "dominant_issuer": "THOMSON REUTERS CORP",
        "type": "EQUITY",
        "sources": ["manual_override", "sec_title"],
    }

    def validate(self, registry: dict) -> list[str]:
        errors: list[str] = []
        validate_data.validate_registry(
            set(registry),
            errors,
            registry,
            self.company_tickers,
        )
        return errors

    def test_same_issuer_historical_alias_group_has_independent_sec_proof(
        self,
    ) -> None:
        errors = self.validate({
            "884903808": self.alias_entry,
            "884903881": self.canonical_entry,
        })
        self.assertEqual([], errors)

    def test_malformed_alias_marker_is_rejected(self) -> None:
        errors = self.validate({
            "884903808": {
                **self.alias_entry,
                "source_ticker": "UNRELATEDUSD",
            },
        })
        self.assertTrue(
            any("fail independent" in error for error in errors),
            errors,
        )

    def test_unrelated_equity_collision_is_rejected(self) -> None:
        errors = self.validate({
            "884903808": self.alias_entry,
            "000000001": {
                **self.canonical_entry,
                "name": "APPLE INC",
                "dominant_issuer": "APPLE INC",
            },
        })
        self.assertTrue(
            any("EQUITY ticker collision" in error for error in errors),
            errors,
        )


class PipelinePositionIdentityTests(unittest.TestCase):
    def test_axiom_unit_regeneration_preserves_composition_hash(self) -> None:
        accession = "0001475597-26-000004"
        source = {
            "accession": accession,
            "source_hash": "4" * 64,
            "form_type": "13F-HR",
            "accepted_at": "2026-02-12T21:00:56.000Z",
            "amendment_number": None,
            "amendment_kind": "ORIGINAL",
            "composition_action": "BASE",
            "applied": True,
            "security_identity_version": pipeline.SECURITY_IDENTITY_VERSION,
        }
        quarter = {
            "report_date": "2025-12-31",
            "filing_date": "2026-02-12",
            "accession": accession,
            "total_value": 256_000,
            "num_holdings": 1,
            "holdings": [{
                "ticker": "AXINU",
                "issuer": "Axiom Intelligence Acquisition Corp 1",
                "cusip": "G0750N120",
                "class": "06/10/2030",
                "value": 256_000,
                "shares": 25_040,
                "holding_type": "EQUITY",
                "share_amount_type": "SH",
            }],
            "composition_version": pipeline.AMENDMENT_REDUCER_VERSION,
            "composition_hash_version": pipeline.COMPOSITION_HASH_VERSION,
            "security_identity_version": pipeline.SECURITY_IDENTITY_VERSION,
            "is_complete": True,
            "base_accession": accession,
            "applied_accessions": [accession],
            "source_filings": [source],
        }
        quarter["composition_hash"] = validate_data.calculate_composition_hash(
            quarter
        )
        fund = {
            "cik": 1475597,
            "name": "HRT FINANCIAL LP",
            "quarters": [quarter],
        }
        registry = {
            "G0750N120": {
                "ticker": "AXINU",
                "name": "Axiom Intelligence Acquisition Corp 1",
                "dominant_issuer": (
                    "AXIOM INTELLIGENCE ACQUISITION CORP 1"
                ),
                "dominant_class": "UNIT 06/10/2030",
                "type": "EQUITY",
                "security_kind": "UNIT",
                "security_kind_source": "openfigi",
                "sources": ["sec_title", "openfigi_plain_ticker"],
            }
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir) / "funds"
            funds_dir.mkdir()
            fund_path = funds_dir / "1475597.json"
            transforms = (
                (
                    "ticker refresh",
                    lambda: pipeline.rebuild_tickers_in_place(
                        company_ticker_data=[]
                    ),
                ),
                ("registry canonicalization", pipeline.canonicalize_fund_files),
            )
            with mock.patch.multiple(
                pipeline,
                FUNDS_DIR=funds_dir,
                load_cusip_map=mock.Mock(
                    return_value={"G0750N120": "AXINU"}
                ),
                load_cusip_registry=mock.Mock(return_value=registry),
                load_openfigi_details=mock.Mock(return_value={
                    "G0750N120": {
                        "status": "matched",
                        "ticker": "AXINU",
                    }
                }),
                resolve_cusips_via_openfigi=mock.Mock(return_value={}),
                save_cusip_map=mock.Mock(),
            ):
                for name, transform in transforms:
                    with self.subTest(transform=name):
                        fund_path.write_text(json.dumps(fund))
                        transform()
                        first = json.loads(fund_path.read_text())
                        transform()
                        second = json.loads(fund_path.read_text())
                        self.assertEqual(first, second)
                        rebuilt_quarter = second["quarters"][0]
                        self.assertEqual(
                            "EQUITY",
                            rebuilt_quarter["holdings"][0]["holding_type"],
                        )
                        self.assertEqual(
                            rebuilt_quarter["composition_hash"],
                            validate_data.calculate_composition_hash(
                                rebuilt_quarter
                            ),
                        )

    def test_regenerate_cli_refreshes_fund_names_only_in_full_mode(
        self,
    ) -> None:
        company_ticker_data = [{"ticker": "EWY"}]
        modes = (
            (["--regenerate-only", "--retry-unresolved"], False),
            (["--regenerate-only", "--full-cusip-refresh"], True),
        )

        for cli_args, full_refresh in modes:
            with (
                self.subTest(cli_args=cli_args),
                tempfile.TemporaryDirectory() as tmpdir,
            ):
                data_dir = Path(tmpdir) / "data"
                retry_unresolved = mock.Mock()
                rebuild_tickers = mock.Mock()
                rebuild_outputs = mock.Mock()
                with (
                    mock.patch("sys.argv", ["pipeline.py", *cli_args]),
                    mock.patch.multiple(
                        pipeline,
                        DATA_DIR=data_dir,
                        FUNDS_DIR=data_dir / "funds",
                        STOCKS_DIR=data_dir / "stocks",
                        retry_unresolved_cusips=retry_unresolved,
                        load_state=mock.Mock(return_value={}),
                        enforce_published_quarter_health=mock.Mock(),
                        save_state=mock.Mock(),
                        _load_company_tickers_data=mock.Mock(
                            return_value=company_ticker_data
                        ),
                        rebuild_tickers_in_place=rebuild_tickers,
                        rebuild_registry_backed_outputs=rebuild_outputs,
                    ),
                ):
                    self.assertEqual(0, pipeline.main())

                rebuild_tickers.assert_called_once_with(
                    full_refresh=full_refresh,
                    company_ticker_data=company_ticker_data,
                )
                rebuild_outputs.assert_called_once_with(
                    full_refresh=full_refresh,
                    company_ticker_data=company_ticker_data,
                    refresh_official_fund_names=full_refresh,
                )
                if full_refresh:
                    retry_unresolved.assert_not_called()
                else:
                    retry_unresolved.assert_called_once_with()

    def test_registry_identity_warnings_block_derived_publication(self) -> None:
        canonicalize = mock.Mock()
        with mock.patch.multiple(
            pipeline,
            build_cusip_registry=mock.Mock(),
            validate_cusip_registry=mock.Mock(return_value=[
                "1 equity tickers still claimed by multiple CUSIPs",
            ]),
            canonicalize_fund_files=canonicalize,
        ):
            with self.assertRaisesRegex(
                pipeline.FundDataError,
                "registry identity gate failed",
            ):
                pipeline.rebuild_registry_backed_outputs()

        canonicalize.assert_not_called()

    def test_registry_rebuild_normalizes_identity_before_share_repair(
        self,
    ) -> None:
        calls: list[str] = []
        builder = mock.Mock(
            side_effect=lambda **_kwargs: calls.append("registry")
        )
        company_ticker_data = [{"ticker": "EWY"}]
        with mock.patch.multiple(
            pipeline,
            build_cusip_registry=builder,
            validate_cusip_registry=mock.Mock(return_value=[]),
            canonicalize_fund_files=mock.Mock(
                side_effect=lambda: calls.append("canonicalize")
            ),
            repair_zero_share_holdings_in_place=mock.Mock(
                side_effect=lambda: calls.append("zero_share")
            ),
            regenerate_stock_files_and_index=mock.Mock(
                side_effect=lambda: calls.append("stocks")
            ),
            write_ticker_health_report=mock.Mock(
                side_effect=lambda: calls.append("health")
            ),
        ):
            pipeline.rebuild_registry_backed_outputs(
                company_ticker_data=company_ticker_data
            )

        self.assertEqual(
            ["registry", "canonicalize", "zero_share", "stocks", "health"],
            calls,
        )
        builder.assert_called_once_with(
            full_refresh=False,
            company_ticker_data=company_ticker_data,
            refresh_official_fund_names=True,
        )

    def test_canonicalization_normalizes_identity_without_registry(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            funds_dir = root / "funds"
            funds_dir.mkdir()
            fund_path = funds_dir / "123456.json"
            fund_path.write_text(
                json.dumps(
                    {
                        "cik": 123456,
                        "quarters": [
                            {
                                "report_date": "2025-12-31",
                                "holdings": [
                                    {
                                        "cusip": "46090E103",
                                        "ticker": "QQQ",
                                        "issuer": "INVESCO QQQ",
                                        "put_call": "CALL",
                                        "holding_type": "PUT",
                                        "option_type": "PUT",
                                        "value": 100,
                                        "shares": 10,
                                    }
                                ],
                            }
                        ],
                    }
                )
            )
            missing_registry = root / "missing_registry.json"

            with mock.patch.multiple(
                pipeline,
                FUNDS_DIR=funds_dir,
                CUSIP_REGISTRY_PATH=missing_registry,
                LEGACY_CUSIP_REGISTRY_PATH=missing_registry,
            ):
                self.assertEqual(1, pipeline.canonicalize_fund_files())

            holding = json.loads(fund_path.read_text())["quarters"][0][
                "holdings"
            ][0]
            self.assertEqual("CALL", holding["holding_type"])
            self.assertNotIn("option_type", holding)
            self.assertEqual("QQQ", holding["ticker"])
            self.assertEqual("INVESCO QQQ", holding["issuer"])

    def test_canonicalization_uses_put_call_persistence_boundary(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            funds_dir = root / "funds"
            funds_dir.mkdir()
            fund_path = funds_dir / "123456.json"
            fund_path.write_text(
                json.dumps(
                    {
                        "cik": 123456,
                        "quarters": [
                            {
                                "report_date": "2026-03-31",
                                "filing_date": "2026-05",
                                "applied_accessions": ["new-filing"],
                                "source_filings": [
                                    {
                                        "accession": "new-filing",
                                        "filing_date": "2026-05",
                                        "applied": True,
                                    }
                                ],
                                "holdings": [
                                    {
                                        "cusip": "88688T209",
                                        "class": "COM",
                                        "holding_type": "CALL",
                                        "value": 100,
                                        "shares": 10,
                                    }
                                ],
                            },
                            {
                                "report_date": "2025-12-31",
                                "filing_date": "2026-02-13",
                                "holdings": [
                                    {
                                        "cusip": "123456789",
                                        "class": "COM",
                                        "holding_type": "CALL",
                                        "value": 50,
                                        "shares": 5,
                                    }
                                ],
                            },
                        ],
                    }
                )
            )
            missing_registry = root / "missing_registry.json"

            with mock.patch.multiple(
                pipeline,
                FUNDS_DIR=funds_dir,
                CUSIP_REGISTRY_PATH=missing_registry,
                LEGACY_CUSIP_REGISTRY_PATH=missing_registry,
            ):
                pipeline.canonicalize_fund_files()

            quarters = json.loads(fund_path.read_text())["quarters"]
            self.assertEqual(
                ["EQUITY", "CALL"],
                [
                    quarter["holdings"][0]["holding_type"]
                    for quarter in quarters
                ],
            )

    def test_option_reclassification_requires_complete_source_proof(
        self,
    ) -> None:
        self.assertFalse(
            pipeline._filing_retains_raw_put_call("2026-04-16")
        )
        self.assertTrue(
            pipeline._filing_retains_raw_put_call("2026-04-17")
        )

        mixed_sources = {
            "report_date": "2025-12-31",
            "filing_date": "2026-05-01",
            "applied_accessions": ["old", "new"],
            "source_filings": [
                {
                    "accession": "old",
                    "filing_date": "2026-02-13",
                    "applied": True,
                },
                {
                    "accession": "new",
                    "filing_date": "2026-05-01",
                    "applied": True,
                },
            ],
        }
        self.assertFalse(
            pipeline._quarter_retains_raw_put_call(mixed_sources)
        )

        all_new_sources = json.loads(json.dumps(mixed_sources))
        all_new_sources["source_filings"][0]["filing_date"] = "2026-04-17"
        self.assertTrue(
            pipeline._quarter_retains_raw_put_call(all_new_sources)
        )

        mismatched_sources = json.loads(json.dumps(all_new_sources))
        mismatched_sources["applied_accessions"] = ["new"]
        self.assertFalse(
            pipeline._quarter_retains_raw_put_call(mismatched_sources)
        )

        no_source_provenance = {
            "report_date": "2025-12-31",
            "filing_date": "2026-05-01",
        }
        self.assertFalse(
            pipeline._quarter_retains_raw_put_call(no_source_provenance)
        )
        no_source_provenance["report_date"] = "2026-06-30"
        self.assertTrue(
            pipeline._quarter_retains_raw_put_call(no_source_provenance)
        )

    def test_explicit_conflict_does_not_guess_other_funds_legacy_side(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            funds_dir = root / "funds"
            funds_dir.mkdir()
            (funds_dir / "100.json").write_text(
                json.dumps(
                    {
                        "cik": 100,
                        "quarters": [
                            {
                                "report_date": "2025-12-31",
                                "holdings": [
                                    {
                                        "cusip": "46090E103",
                                        "class": "ETF",
                                        "put_call": "CALL",
                                        "holding_type": "PUT",
                                        "value": 100,
                                        "shares": 10,
                                    }
                                ],
                            }
                        ],
                    }
                )
            )
            equity_path = funds_dir / "200.json"
            equity_path.write_text(
                json.dumps(
                    {
                        "cik": 200,
                        "quarters": [
                            {
                                "report_date": "2025-09-30",
                                "filing_date": "2025-11-14",
                                "holdings": [
                                    {
                                        "cusip": "46090E103",
                                        "class": "ETF",
                                        "holding_type": "PUT",
                                        "value": 40,
                                        "shares": 4,
                                    }
                                ],
                            }
                        ],
                    }
                )
            )
            missing_registry = root / "missing_registry.json"

            with mock.patch.multiple(
                pipeline,
                FUNDS_DIR=funds_dir,
                CUSIP_REGISTRY_PATH=missing_registry,
                LEGACY_CUSIP_REGISTRY_PATH=missing_registry,
            ):
                pipeline.canonicalize_fund_files()

            holding = json.loads(equity_path.read_text())["quarters"][0][
                "holdings"
            ][0]
            self.assertEqual("PUT", holding["holding_type"])

    def test_canonicalization_uses_row_type_and_preserves_source_grain(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            funds_dir = root / "funds"
            funds_dir.mkdir()
            registry_path = root / "cusip_registry.json"
            fund_path = funds_dir / "123456.json"
            fund_path.write_text(
                json.dumps(
                    {
                        "cik": 123456,
                        "name": "Example Fund",
                        "quarters": [
                            {
                                "report_date": "2025-12-31",
                                "total_value": 100,
                                "num_holdings": 2,
                                "composition_hash": "unchanged",
                                "holdings": [
                                    {
                                        "cusip": "46090E103",
                                        "issuer": "INVESCO QQQ",
                                        "class": "UNIT SER 1",
                                        "value": 60,
                                        "shares": 6,
                                        "put_call": "CALL",
                                        "holding_type": "PUT",
                                    },
                                    {
                                        "cusip": "46090E103",
                                        "issuer": "INVESCO QQQ",
                                        "class": "CALL",
                                        "value": 40,
                                        "shares": 4,
                                        "option_type": "CALL",
                                    },
                                ],
                            }
                        ],
                    }
                )
            )
            registry_path.write_text(
                json.dumps(
                    {
                        "46090E103": {
                            "ticker": "QQQ",
                            "name": "INVESCO QQQ TRUST",
                            "type": "PUT",
                            "underlying_cusip": "631100104",
                        }
                    }
                )
            )

            with mock.patch.multiple(
                pipeline,
                FUNDS_DIR=funds_dir,
                CUSIP_REGISTRY_PATH=registry_path,
                LEGACY_CUSIP_REGISTRY_PATH=registry_path,
            ):
                self.assertEqual(1, pipeline.canonicalize_fund_files())

            rebuilt = json.loads(fund_path.read_text())
            quarter = rebuilt["quarters"][0]
            self.assertEqual("unchanged", quarter["composition_hash"])
            self.assertEqual(2, quarter["num_holdings"])
            self.assertEqual(2, len(quarter["holdings"]))
            self.assertEqual(
                ["CALL", "CALL"],
                [holding["holding_type"] for holding in quarter["holdings"]],
            )
            self.assertTrue(
                all("option_type" not in holding for holding in quarter["holdings"])
            )

    def test_canonicalization_splits_equity_from_same_cusip_put_with_source_proof(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            data_dir = root / "data"
            funds_dir = data_dir / "funds"
            stocks_dir = data_dir / "stocks"
            funds_dir.mkdir(parents=True)
            stocks_dir.mkdir()
            registry_path = root / "cusip_registry.json"
            fund_path = funds_dir / "123456.json"
            fund_path.write_text(
                json.dumps(
                    {
                        "cik": 123456,
                        "name": "Example Fund",
                        "quarters": [
                            {
                                "report_date": "2025-12-31",
                                "filing_date": "2026-05-01",
                                "applied_accessions": ["source-1"],
                                "source_filings": [
                                    {
                                        "accession": "source-1",
                                        "filing_date": "2026-05-01",
                                        "applied": True,
                                    }
                                ],
                                "total_value": 100,
                                "holdings": [
                                    {
                                        "cusip": "46090E103",
                                        "ticker": "QQQ",
                                        "issuer": "INVESCO QQQ TRUST",
                                        "class": "ETF",
                                        "value": 60,
                                        "shares": 6,
                                        "put_call": "PUT",
                                        "holding_type": "PUT",
                                    },
                                    {
                                        "cusip": "46090E103",
                                        "ticker": "QQQ",
                                        "issuer": "INVESCO QQQ TRUST",
                                        "class": "ETF",
                                        "value": 40,
                                        "shares": 4,
                                        "holding_type": "PUT",
                                    },
                                ],
                            }
                        ],
                    }
                )
            )
            registry_path.write_text(
                json.dumps(
                    {
                        "46090E103": {
                            "ticker": "QQQ",
                            "name": "INVESCO QQQ TRUST",
                            "type": "PUT",
                        }
                    }
                )
            )

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
                pipeline.canonicalize_fund_files()
                pipeline.regenerate_stock_files_and_index()

            holdings = json.loads(fund_path.read_text())["quarters"][0][
                "holdings"
            ]
            self.assertEqual(
                ["PUT", "EQUITY"],
                [holding["holding_type"] for holding in holdings],
            )
            put_stock = json.loads(
                (stocks_dir / "46090E103__PUT.json").read_text()
            )
            equity_stock = json.loads(
                (stocks_dir / "46090E103.json").read_text()
            )
            self.assertEqual(60, put_stock["holders"][0]["history"][0]["value"])
            self.assertEqual(
                40,
                equity_stock["holders"][0]["history"][0]["value"],
            )

    def test_legacy_option_without_raw_side_requires_migration_evidence(
        self,
    ) -> None:
        holding = {
            "cusip": "46090E103",
            "issuer": "INVESCO QQQ TRUST, SERIES 1",
            "class": "ETF",
            "holding_type": "PUT",
        }
        self.assertEqual("PUT", pipeline.classify_saved_holding(holding))
        self.assertEqual(
            "EQUITY",
            pipeline.classify_saved_holding(
                holding,
                allow_missing_option_side_reclassification=True,
            ),
        )
        self.assertEqual(
            "CALL",
            pipeline.classify_saved_holding(
                {
                    "cusip": "46090E103",
                    "class": "CALL",
                    "holding_type": "PUT",
                }
            ),
        )

    def test_legacy_option_side_is_not_downgraded_to_generic_option(self) -> None:
        holding = {
            "cusip": "037833100",
            "issuer": "APPLE INC",
            "class": "OPTION",
            "holding_type": "CALL",
        }
        self.assertEqual("CALL", pipeline.classify_saved_holding(holding))
        self.assertEqual(
            "OPT",
            pipeline.classify_saved_holding(
                holding,
                allow_missing_option_side_reclassification=True,
            ),
        )

    def test_proven_registry_option_contamination_does_not_trust_debt_words(
        self,
    ) -> None:
        holding = {
            "cusip": "46090E103",
            "issuer": "INVESCO QQQ TRUST",
            "class": "CONV BONDS",
            "holding_type": "PUT",
        }
        self.assertEqual("PUT", pipeline.classify_saved_holding(holding))
        self.assertEqual(
            "EQUITY",
            pipeline.classify_saved_holding(
                holding,
                allow_missing_option_side_reclassification=True,
            ),
        )

    def test_clear_legacy_non_equity_classes_are_reclassified(self) -> None:
        cases = (
            ("CNV", "037833100", "NOTE"),
            ("US TREASURY", "91282CJL6", "NOTE"),
            ("*W EXP 06/01/2030", "037833100", "WARRANT"),
        )
        for security_class, cusip, expected in cases:
            with self.subTest(security_class=security_class, cusip=cusip):
                self.assertEqual(
                    expected,
                    pipeline.classify_saved_holding(
                        {
                            "issuer": "EXAMPLE ISSUER",
                            "class": security_class,
                            "cusip": cusip,
                            "holding_type": "EQUITY",
                        }
                    ),
                )

    def test_note_terms_cannot_be_invented_across_source_fields(self) -> None:
        axiom_unit = {
            "issuer": "Axiom Intelligence Acquisition Corp 1",
            "class": "06/10/2030",
            "cusip": "G0750N120",
            "holding_type": "EQUITY",
        }
        self.assertEqual("EQUITY", pipeline._classify_holding(axiom_unit))
        self.assertEqual(
            "EQUITY",
            pipeline.classify_saved_holding(axiom_unit),
        )

        for debt_row in (
            {"issuer": "EXAMPLE ISSUER 1 06/10/2030", "class": "SECURITY"},
            {"issuer": "EXAMPLE ISSUER", "class": "1 06/10/2030"},
        ):
            with self.subTest(debt_row=debt_row):
                self.assertEqual("NOTE", pipeline._classify_holding(debt_row))

    def test_hash_bound_type_is_not_reclassified_during_regeneration(
        self,
    ) -> None:
        holding = {
            "issuer": "EXAMPLE ISSUER",
            "class": "NOTE",
            "cusip": "123456789",
            "holding_type": "EQUITY",
        }
        self.assertEqual(
            "EQUITY",
            pipeline._canonical_holding_type_for_quarter(
                {"composition_hash_version": pipeline.COMPOSITION_HASH_VERSION},
                holding,
            ),
        )
        self.assertEqual(
            "NOTE",
            pipeline._canonical_holding_type_for_quarter(
                {"composition_hash_version": 1},
                holding,
            ),
        )

    def test_fund_strategy_words_do_not_override_equity_class(self) -> None:
        cases = (
            "CONV",
            "CONVERTIBLE EQTY",
            "CONVERTIBLE TOTAL RETURN FUND",
            "CONVERTIBLE BOND ETF",
            "US TREASURY ETF",
        )
        for security_class in cases:
            with self.subTest(security_class=security_class):
                self.assertEqual(
                    "EQUITY",
                    pipeline.classify_saved_holding(
                        {
                            "issuer": "EXAMPLE FUND",
                            "class": security_class,
                            "cusip": "922040845",
                            "holding_type": "EQUITY",
                        }
                    ),
                )

    def test_equity_tokens_do_not_override_preferred_or_warrant_classes(self) -> None:
        cases = (
            ("PREFERRED STOCK", "PREF"),
            ("PREFERRED SHARES", "PREF"),
            ("CONVERTIBLE PREFERRED STOCK", "PREF"),
            ("COMMON STOCK WARRANTS", "WARRANT"),
        )
        for security_class, expected in cases:
            with self.subTest(security_class=security_class):
                self.assertEqual(
                    expected,
                    pipeline.classify_saved_holding(
                        {
                            "issuer": "EXAMPLE ISSUER",
                            "class": security_class,
                            "cusip": "037833100",
                            "holding_type": "EQUITY",
                        }
                    ),
                )

    def test_ambiguous_saved_non_equity_type_is_preserved(self) -> None:
        self.assertEqual(
            "NOTE",
            pipeline.classify_saved_holding(
                {
                    "class": "SECURITY",
                    "holding_type": "NOTE",
                }
            ),
        )
        self.assertEqual(
            "PREF",
            pipeline.classify_saved_holding(
                {
                    "class": "ADR",
                    "holding_type": "PREF",
                }
            ),
        )
        self.assertEqual(
            "NOTE",
            pipeline.classify_saved_holding(
                {
                    "class": "7.25%         DEP  SHS  A",
                    "holding_type": "NOTE",
                }
            ),
        )
        self.assertEqual(
            "NOTE",
            pipeline.classify_saved_holding(
                {
                    "class": "COM NOTE  0.250% 9/1",
                    "holding_type": "NOTE",
                }
            ),
        )
        self.assertEqual(
            "NOTE",
            pipeline.classify_saved_holding(
                {
                    "issuer": "CONV PFD 5.2500 03/06/2032",
                    "class": "EQUITY",
                    "holding_type": "NOTE",
                }
            ),
        )
        self.assertEqual(
            "EQUITY",
            pipeline.classify_saved_holding(
                {
                    "class": "ETF",
                    "holding_type": "NOTE",
                }
            ),
        )

    def test_stock_regeneration_aggregates_class_variants_without_registry_type(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            data_dir = root / "data"
            funds_dir = data_dir / "funds"
            stocks_dir = data_dir / "stocks"
            funds_dir.mkdir(parents=True)
            stocks_dir.mkdir()
            registry_path = root / "cusip_registry.json"
            (funds_dir / "123456.json").write_text(
                json.dumps(
                    {
                        "cik": 123456,
                        "name": "Example Fund",
                        "quarters": [
                            {
                                "report_date": "2025-12-31",
                                "filing_date": "2026-02-13",
                                "total_value": 100,
                                "num_holdings": 2,
                                "holdings": [
                                    {
                                        "cusip": "46090E103",
                                        "ticker": "QQQ",
                                        "issuer": "INVESCO QQQ",
                                        "class": "UNIT SER 1",
                                        "value": 60,
                                        "shares": 6,
                                        "put_call": "CALL",
                                        "holding_type": "PUT",
                                    },
                                    {
                                        "cusip": "46090E103",
                                        "ticker": "QQQ",
                                        "issuer": "INVESCO QQQ",
                                        "class": "COM",
                                        "value": 40,
                                        "shares": 4,
                                        "put_call": "CALL",
                                        "holding_type": "CALL",
                                    },
                                ],
                            }
                        ],
                    }
                )
            )
            registry_path.write_text(
                json.dumps(
                    {
                        "46090E103": {
                            "ticker": "QQQ",
                            "name": "INVESCO QQQ TRUST",
                            "type": "PUT",
                            "underlying_cusip": "631100104",
                        }
                    }
                )
            )

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
                pipeline.canonicalize_fund_files()
                pipeline.regenerate_stock_files_and_index()

            call_path = stocks_dir / "46090E103__CALL.json"
            self.assertTrue(call_path.exists())
            self.assertFalse((stocks_dir / "46090E103__PUT.json").exists())
            call_stock = json.loads(call_path.read_text())
            self.assertEqual("46090E103|CALL", call_stock["stock_id"])
            self.assertEqual("CALL", call_stock["instrument_type"])
            self.assertEqual(
                100,
                call_stock["holders"][0]["history"][0]["value"],
            )
            self.assertEqual(
                10,
                call_stock["holders"][0]["history"][0]["shares"],
            )

            errors: list[str] = []
            warnings: list[str] = []
            with mock.patch.object(validate_data, "FUNDS_DIR", funds_dir):
                (
                    fund_files,
                    _stock_groups,
                    _fund_cusips,
                    calendars,
                    expected,
                ) = validate_data.validate_funds(errors, {})
            with mock.patch.object(validate_data, "STOCKS_DIR", stocks_dir):
                stock_files = validate_data.validate_stocks(
                    errors,
                    calendars,
                    expected,
                )
            index = json.loads((data_dir / "index.json").read_text())
            funds_index = json.loads((data_dir / "funds-index.json").read_text())
            validate_data.validate_index(
                index,
                fund_files,
                stock_files,
                {},
                errors,
                warnings,
                calendars,
            )
            validate_data.validate_funds_index(
                funds_index,
                index,
                errors,
                fund_files,
            )
            self.assertEqual([], errors)
            self.assertEqual([], warnings)

    def test_zero_share_repair_does_not_mix_same_cusip_option_prices(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir) / "funds"
            funds_dir.mkdir()
            report_date = "2025-12-31"
            (funds_dir / "100.json").write_text(
                json.dumps(
                    {
                        "cik": 100,
                        "quarters": [
                            {
                                "report_date": report_date,
                                "holdings": [
                                    {
                                        "cusip": "037833100",
                                        "class": "COM",
                                        "value": 100,
                                        "shares": 10,
                                        "holding_type": "EQUITY",
                                    },
                                    {
                                        "cusip": "037833100",
                                        "class": "COM",
                                        "value": 1000,
                                        "shares": 10,
                                        "put_call": "CALL",
                                        "holding_type": "CALL",
                                    },
                                ],
                            }
                        ],
                    }
                )
            )
            target_path = funds_dir / "200.json"
            target_path.write_text(
                json.dumps(
                    {
                        "cik": 200,
                        "quarters": [
                            {
                                "report_date": report_date,
                                "holdings": [
                                    {
                                        "cusip": "037833100",
                                        "class": "COM",
                                        "value": 50,
                                        "shares": 0,
                                        "holding_type": "EQUITY",
                                    }
                                ],
                            }
                        ],
                    }
                )
            )

            with mock.patch.object(pipeline, "FUNDS_DIR", funds_dir):
                self.assertEqual(1, pipeline.repair_zero_share_holdings_in_place())

            repaired = json.loads(target_path.read_text())
            holding = repaired["quarters"][0]["holdings"][0]
            self.assertEqual(5, holding["shares"])
            self.assertTrue(holding["shares_imputed"])


class IdentityValidationTests(unittest.TestCase):
    def test_zero_share_validation_does_not_mix_same_cusip_instruments(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir) / "funds"
            funds_dir.mkdir()
            (funds_dir / "100.json").write_text(
                json.dumps(
                    {
                        "cik": 100,
                        "name": "Example Fund",
                        "quarters": [
                            {
                                "report_date": "2025-12-31",
                                "filing_date": "2026-02-13",
                                "total_value": 150,
                                "num_holdings": 2,
                                "holdings": [
                                    {
                                        "cusip": "037833100",
                                        "holding_type": "EQUITY",
                                        "value": 100,
                                        "shares": 10,
                                    },
                                    {
                                        "cusip": "037833100",
                                        "holding_type": "CALL",
                                        "put_call": "CALL",
                                        "value": 50,
                                        "shares": 0,
                                    },
                                ],
                            }
                        ],
                    }
                )
            )

            errors: list[str] = []
            with mock.patch.object(validate_data, "FUNDS_DIR", funds_dir):
                validate_data.validate_funds(errors, {})

            self.assertEqual([], errors)

    def test_fund_validator_requires_reproducible_imputed_shares(self) -> None:
        def validate_target(target_holding: dict) -> list[str]:
            with tempfile.TemporaryDirectory() as tmpdir:
                funds_dir = Path(tmpdir) / "funds"
                funds_dir.mkdir()
                report_date = "2025-12-31"
                (funds_dir / "100.json").write_text(
                    json.dumps(
                        {
                            "cik": 100,
                            "name": "Reference Fund",
                            "quarters": [
                                {
                                    "report_date": report_date,
                                    "filing_date": "2026-02-13",
                                    "total_value": 1000,
                                    "num_holdings": 1,
                                    "holdings": [
                                        {
                                            "cusip": "037833100",
                                            "holding_type": "EQUITY",
                                            "value": 1000,
                                            "shares": 10,
                                        }
                                    ],
                                }
                            ],
                        }
                    )
                )
                (funds_dir / "200.json").write_text(
                    json.dumps(
                        {
                            "cik": 200,
                            "name": "Target Fund",
                            "quarters": [
                                {
                                    "report_date": report_date,
                                    "filing_date": "2026-02-13",
                                    "total_value": 250,
                                    "num_holdings": 1,
                                    "holdings": [target_holding],
                                }
                            ],
                        }
                    )
                )
                errors: list[str] = []
                with mock.patch.object(validate_data, "FUNDS_DIR", funds_dir):
                    validate_data.validate_funds(errors, {})
                return errors

        valid = {
            "cusip": "037833100",
            "holding_type": "EQUITY",
            "value": 250,
            "shares": 2.5,
            "reported_shares": 0,
            "shares_imputed": True,
        }
        self.assertEqual([], validate_target(valid))

        invalid_marker = dict(valid, shares_imputed=False)
        self.assertTrue(any(
            "expected literal true" in error
            for error in validate_target(invalid_marker)
        ))

        invalid_reported = dict(valid, reported_shares=1)
        self.assertTrue(any(
            "must preserve reported zero" in error
            for error in validate_target(invalid_reported)
        ))

        stale_estimate = dict(valid, shares=2.500001)
        self.assertTrue(any(
            "expected exactly 2.5" in error
            for error in validate_target(stale_estimate)
        ))

        no_peer = dict(
            valid,
            holding_type="CALL",
            put_call="CALL",
        )
        self.assertTrue(any(
            "no qualifying peer price" in error
            for error in validate_target(no_peer)
        ))

    def test_fund_validator_rejects_uncanonicalized_option_identity(self) -> None:
        errors: list[str] = []
        lookup_id = validate_data.validate_fund_holding_identity(
            {
                "cusip": "46090E103",
                "holding_type": "PUT",
                "put_call": "CALL",
                "option_type": "PUT",
            },
            "fixture holding",
            errors,
        )

        self.assertEqual("46090E103|CALL", lookup_id)
        self.assertTrue(any("obsolete option_type" in error for error in errors))
        self.assertTrue(any(
            "put_call CALL inconsistent with holding_type PUT" in error
            for error in errors
        ))

    def test_fund_validator_requires_cusip_even_when_ticker_is_present(self) -> None:
        errors: list[str] = []
        validate_data.validate_fund_holding_identity(
            {
                "ticker": "AAPL",
                "holding_type": "EQUITY",
            },
            "fixture holding",
            errors,
        )
        self.assertTrue(any(
            "invalid canonical cusip None" in error
            for error in errors
        ))

    def test_stock_validator_rejects_suffix_payload_type_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            stocks_dir = Path(tmpdir) / "stocks"
            stocks_dir.mkdir()
            (stocks_dir / "46090E103__CALL.json").write_text(
                json.dumps(
                    {
                        "stock_id": "46090E103|CALL",
                        "cusip": "46090E103",
                        "ticker": "QQQ",
                        "issuer": "INVESCO QQQ TRUST",
                        "instrument_type": "PUT",
                        "holders": [],
                    }
                )
            )
            errors: list[str] = []
            with mock.patch.object(validate_data, "STOCKS_DIR", stocks_dir):
                validate_data.validate_stocks(errors)

        self.assertTrue(any(
            "exact cusip/type identity is 46090E103|PUT" in error
            for error in errors
        ))
        self.assertTrue(any(
            "exact cusip/type filename 46090E103__PUT.json" in error
            for error in errors
        ))

    def test_stock_validator_rejects_orphan_stock_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            stocks_dir = Path(tmpdir)
            (stocks_dir / "037833100.json").write_text(
                json.dumps(
                    {
                        "stock_id": "037833100",
                        "cusip": "037833100",
                        "instrument_type": "EQUITY",
                        "holders": [],
                    }
                )
            )
            errors: list[str] = []
            with mock.patch.object(validate_data, "STOCKS_DIR", stocks_dir):
                validate_data.validate_stocks(errors, {}, {})

        self.assertTrue(any(
            "has no retained fund position" in error
            for error in errors
        ))


class NoteLabelPipelineTests(unittest.TestCase):
    @staticmethod
    def note_evidence(issuer: str = "RIVIAN AUTOMOTIVE INC") -> dict:
        return {
            "total_value": 100,
            "holder_ciks": {1},
            "issuer_value": {issuer: 100},
            "class_value": {"NOTE 3.625% 10/15/30": 100},
            "put_call_value": {},
            "first_seen": "2025-12-31",
            "last_seen": "2026-03-31",
        }

    def test_prudential_manual_bond_override_outranks_mixed_equity_data(
        self,
    ) -> None:
        evidence = self.note_evidence("PRUDENTIAL FINL INC")
        evidence.update({
            "total_value": 41_625,
            "holder_ciks": {713676, 791540, 1727336},
            "issuer_value": {
                "PRUDENTIAL FINL INC": 35_617,
                "PRUDENTIAL FINL 4.125 09/01/60 '25": 6_008,
            },
            "class_value": {
                "COMMON STOCK": 35_617,
                "PREFERRED STOCK": 6_008,
            },
            "instrument_type_value": {
                "EQUITY": 35_617,
                "PREF": 6_008,
            },
            "instrument_type_count": {"EQUITY": 4, "PREF": 3},
            "non_option_class_value": {
                "COMMON STOCK": 35_617,
                "PREFERRED STOCK": 6_008,
            },
            "non_option_class_count": {
                "COMMON STOCK": 4,
                "PREFERRED STOCK": 3,
            },
        })
        openfigi_detail = {
            "status": "matched",
            "ticker": "PRU 4.125 09/01/60",
            "name": "PRUDENTIAL FINANCIAL INC",
            "securityDescription": "PRU 4.125 09/01/60",
            "marketSector": "Pfd",
            "securityType": "PUBLIC",
            "securityType2": "Preferred Stock",
            "exchCode": "NEW YORK",
        }
        prior_entry = {
            "ticker": None,
            "security_label": "PRU 4.125 09/01/60",
            "label_source": "openfigi",
            "security_kind": "PREFERRED",
            "security_kind_source": "openfigi",
            "type": "PREF",
            "sources": ["filer_dominant"],
        }

        with mock.patch.multiple(
            pipeline,
            FUNDS_DIR=mock.MagicMock(exists=mock.Mock(return_value=True)),
            _aggregate_cusip_evidence=mock.Mock(return_value={
                "744320888": evidence,
            }),
            load_cusip_map=mock.Mock(return_value={}),
            load_cusip_registry=mock.Mock(return_value={
                "744320888": prior_entry,
            }),
            load_openfigi_details=mock.Mock(return_value={
                "744320888": openfigi_detail,
            }),
            save_cusip_registry=mock.Mock(),
        ):
            registry = pipeline.build_cusip_registry(
                company_ticker_data=[]
            )

        prudential_note = registry["744320888"]
        self.assertEqual("NOTE", prudential_note["type"])
        self.assertIsNone(prudential_note["ticker"])
        self.assertEqual("BOND", prudential_note["security_kind"])
        self.assertEqual(
            "manual_verified",
            prudential_note["security_kind_source"],
        )
        self.assertEqual(
            "PFH — 4.125% JUNIOR SUBORDINATED NOTES DUE 2060",
            prudential_note["security_label"],
        )
        self.assertEqual(
            "manual_verified",
            prudential_note["label_source"],
        )

    def test_registry_separates_note_labels_from_canonical_tickers(
        self,
    ) -> None:
        evidence = {
            "76954AAD5": self.note_evidence(),
            "090043AF7": self.note_evidence("BILL HOLDINGS INC"),
        }
        with mock.patch.multiple(
            pipeline,
            FUNDS_DIR=mock.MagicMock(exists=mock.Mock(return_value=True)),
            _aggregate_cusip_evidence=mock.Mock(return_value=evidence),
            load_cusip_map=mock.Mock(return_value={
                "76954AAD5": "RIVN 3.625 10/15/30",
                "090043AF7": "BILL",
            }),
            load_cusip_registry=mock.Mock(return_value={}),
            load_openfigi_details=mock.Mock(return_value={}),
            save_cusip_registry=mock.Mock(),
        ):
            registry = pipeline.build_cusip_registry(
                company_ticker_data=[]
            )

        rivian = registry["76954AAD5"]
        self.assertEqual("NOTE", rivian["type"])
        self.assertIsNone(rivian["ticker"])
        self.assertEqual(
            "RIVN 3.625 10/15/30",
            rivian["security_label"],
        )
        self.assertEqual(
            "openfigi_legacy_ticker",
            rivian["label_source"],
        )
        self.assertNotIn("note_label_vetted", rivian["sources"])
        self.assertIsNone(registry["090043AF7"]["ticker"])
        self.assertEqual(
            "BILL HOLDINGS INC — NOTE 3.625% 10/15/30",
            registry["090043AF7"]["security_label"],
        )
        self.assertEqual(
            "filer_issuer_class",
            registry["090043AF7"]["label_source"],
        )
        self.assertNotIn(
            "note_label_vetted",
            registry["090043AF7"]["sources"],
        )

    def test_prior_structured_note_label_outranks_generic_current_name(
        self,
    ) -> None:
        label, source = pipeline._registry_security_label(
            identifier="76954AAD5",
            entry={
                "ticker": None,
                "dominant_issuer": "RIVIAN AUTOMOTIVE INC",
                "dominant_class": "NOTE 3.625% 10/15/30",
                "type": "NOTE",
            },
            openfigi_detail={
                "status": "matched",
                "ticker": None,
                "securityDescription": None,
                "name": "RIVIAN AUTOMOTIVE INC",
            },
            prior_entry={
                "security_label": "RIVN 3.625 10/15/30 2030",
                "label_source": "openfigi",
                "sources": [],
            },
            legacy_openfigi_label=None,
        )

        self.assertEqual("RIVN 3.625 10/15/30", label)
        self.assertEqual("openfigi_prior_registry", source)

        current_label, current_source = pipeline._registry_security_label(
            identifier="691543607",
            entry={
                "ticker": None,
                "dominant_issuer": "OXFORD LANE CAPITAL CORP",
                "dominant_class": "NOTE",
                "type": "NOTE",
            },
            openfigi_detail={
                "status": "matched",
                "ticker": "OXLC 6.25 02/28/27 2027",
                "securityDescription": "OXLC 6.25 02/28/27",
                "name": "OXFORD LANE CAPITAL CORP",
                "marketSector": "Corp",
                "securityType": "Corp",
                "securityType2": "Corp",
                "exchCode": "TRACE",
            },
            prior_entry=None,
            legacy_openfigi_label=None,
        )
        self.assertEqual("OXLC 6.25 02/28/27", current_label)
        self.assertEqual("openfigi", current_source)

    def test_registry_and_artifact_cover_openfigi_and_fallback_labels(
        self,
    ) -> None:
        def evidence(issuer: str = "", security_class: str = "") -> dict:
            return {
                "total_value": 100,
                "holder_ciks": {1},
                "issuer_value": {issuer: 100} if issuer else {},
                "class_value": (
                    {security_class: 100} if security_class else {}
                ),
                "put_call_value": {},
                "first_seen": "2025-12-31",
                "last_seen": "2026-03-31",
            }

        evidence_by_cusip = {
            "65339F655": evidence(
                "NEXTERA ENERGY INC",
                "UNIT 02/15/2029",
            ),
            "921937827": evidence(
                "VANGUARD BD INDEX FDS",
                "SHORT TRM BOND",
            ),
            "464288687": evidence(
                "ISHARES TR",
                "PFD AND INCM SEC",
            ),
            "464287226": evidence(
                "ISHARES TR",
                "CORE US AGGBD ET",
            ),
            "81369Y803": evidence(
                "SELECT SECTOR SPDR TR",
                "STATE STREET TEC",
            ),
            "26923G822": evidence(
                "ETFIS SER TR I",
                "VIRTUS INFRCAP",
            ),
            "47103U852": evidence(
                "JANUS DETROIT STR TR",
                "HENDERSON MTG",
            ),
            "74933W452": evidence(
                "RBB FD INC",
                "US TREAS 3 MNTH",
            ),
            "808524862": evidence(
                "SCHWAB STRATEGIC TR",
                "SHT TM US TRES",
            ),
            "09260B630": evidence(
                "BLACKROCK HIGH YIELD INSTL",
                "MFF",
            ),
            "21874A114": evidence(
                "CORE SCIENTIFIC, INC./TX",
                "*W EXP 01/23/202",
            ),
            "02072L359": evidence(
                "EA SERIES TRUST",
                "ARK 21SHARES ACT",
            ),
            "00437K108": evidence(
                "ACCELERATE ARBITRAGE FUND",
                "ETF",
            ),
            "464286772": evidence(
                "ISHARES INC",
                "MSCI STH KOR ETF",
            ),
            "123456789": evidence(),
            "12345ABC6": evidence("KEEPER INC", "NOTE"),
            "45669R701": evidence("45669R701", "STOCK"),
            "46222L116": evidence(),
            "M5R635116": evidence(
                "INNOVIZ TECHNOLOGIES LTD W EXP 04/05/202",
                "WARRANT",
            ),
        }
        openfigi_details = {
            "65339F655": {
                "status": "matched",
                "ticker": "NEE 7.375 02/15/29",
                "name": "NEXTERA ENERGY INC",
                "securityDescription": "NEE 7 3/8 02/15/29",
                "marketSector": "Pfd",
                "securityType": "PUBLIC",
                "securityType2": "Preferred Stock",
                "exchCode": "NEW YORK",
            },
            "921937827": {
                "status": "matched",
                "ticker": "BSV",
                "name": "VANGUARD SHORT-TERM BOND ETF",
                "securityDescription": "BSV",
                "marketSector": "Equity",
                "securityType": "ETP",
                "securityType2": "Mutual Fund",
                "exchCode": "US",
            },
            "21874A114": {
                "status": "matched",
                "ticker": "CORZW",
                "name": "CORE SCIENTIFIC INC - 27",
                "securityDescription": "CORZW",
                "marketSector": "Equity",
                "securityType": "Equity WRT",
                "securityType2": "Warrant",
                "exchCode": "US",
            },
            "02072L359": {
                "status": "matched",
                "ticker": "81K0",
                "name": "ARK 21SHRS ACTBITC FUT STRGY",
                "securityDescription": "81K0",
                "marketSector": "Equity",
                "securityType": "ETP",
                "securityType2": "Mutual Fund",
                "exchCode": "GR",
            },
            "00437K108": {
                "status": "matched",
                "ticker": "ARB",
                "name": "ACCELERATE ARBITRAGE FUND",
                "securityDescription": "ARB",
                "marketSector": "Equity",
                "securityType": "ETP",
                "securityType2": "Mutual Fund",
                "exchCode": "CN",
            },
            "464286772": {
                "status": "matched",
                "ticker": "EWY",
                "name": "ISHARES MSCI SOUTH KOREA ETF",
                "securityDescription": "EWY",
                "marketSector": "Equity",
                "securityType": "ETP",
                "securityType2": "Mutual Fund",
                "exchCode": "US",
            },
            "12345ABC6": {"status": "no_match"},
        }
        prior_registry = {
            "464288687": {
                "ticker": "PFF",
                "security_label": "PFF",
                "label_source": "canonical_ticker",
                "security_kind": "PREFERRED",
                "security_kind_source": "filer_metadata",
                "type": "EQUITY",
                "sources": ["filer_dominant", "cusip_map_vetted"],
            },
            "12345ABC6": {
                "ticker": None,
                "security_label": "KEEP 4.25 01/01/30",
                "label_source": "openfigi",
                "security_kind": "BOND",
                "security_kind_source": "openfigi",
                "type": "NOTE",
                "sources": ["filer_dominant"],
            },
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            labels_path = Path(tmpdir) / "security_labels.json"
            with mock.patch.multiple(
                pipeline,
                FUNDS_DIR=mock.MagicMock(exists=mock.Mock(return_value=True)),
                SECURITY_LABELS_PATH=labels_path,
                _aggregate_cusip_evidence=mock.Mock(
                    return_value=evidence_by_cusip
                ),
                load_cusip_map=mock.Mock(return_value={
                    "00437K108": "ARB",
                    "464288687": "PFF",
                    "464287226": "AGG",
                    "81369Y803": "XLK",
                    "26923G822": "PFFA",
                    "47103U852": "JMBS",
                    "74933W452": "TBIL",
                    "808524862": "SCHO",
                    "09260B630": "BHYIX",
                }),
                load_cusip_registry=mock.Mock(
                    return_value=prior_registry
                ),
                load_openfigi_details=mock.Mock(
                    return_value=openfigi_details
                ),
                load_sec_fund_name_cache=mock.Mock(return_value={}),
                save_cusip_registry=mock.Mock(),
            ):
                registry = pipeline.build_cusip_registry(
                    company_ticker_data=[]
                )
                pipeline.write_security_labels(registry)

            labels_payload = json.loads(labels_path.read_text())

        next_era = registry["65339F655"]
        self.assertEqual("EQUITY", next_era["type"])
        self.assertIsNone(next_era["ticker"])
        self.assertEqual(
            "NEE 7.375 02/15/29",
            next_era["security_label"],
        )
        self.assertEqual("openfigi", next_era["label_source"])
        self.assertEqual("PREFERRED", next_era["security_kind"])

        bsv = registry["921937827"]
        self.assertEqual("EQUITY", bsv["type"])
        self.assertEqual("BSV", bsv["ticker"])
        self.assertEqual("BSV", bsv["security_label"])
        self.assertEqual("openfigi", bsv["label_source"])
        self.assertEqual("ETF", bsv["security_kind"])

        pff = registry["464288687"]
        self.assertEqual("EQUITY", pff["type"])
        self.assertEqual("PFF", pff["ticker"])
        self.assertEqual("PFF", pff["security_label"])
        self.assertEqual("ETF", pff["security_kind"])
        self.assertEqual("filer_metadata", pff["security_kind_source"])

        for identifier, ticker in (
            ("464287226", "AGG"),
            ("81369Y803", "XLK"),
            ("26923G822", "PFFA"),
            ("47103U852", "JMBS"),
            ("74933W452", "TBIL"),
            ("808524862", "SCHO"),
        ):
            with self.subTest(structural_etf=ticker):
                entry = registry[identifier]
                self.assertEqual("EQUITY", entry["type"])
                self.assertEqual(ticker, entry["ticker"])
                self.assertEqual("ETF", entry["security_kind"])
                self.assertEqual(
                    "filer_metadata",
                    entry["security_kind_source"],
                )

        bhyix = registry["09260B630"]
        self.assertEqual("EQUITY", bhyix["type"])
        self.assertEqual("BHYIX", bhyix["ticker"])
        self.assertNotIn("security_kind", bhyix)
        self.assertTrue(
            pipeline._registry_entry_has_equity_fund_identity(bhyix)
        )

        corzw = registry["21874A114"]
        self.assertEqual("WARRANT", corzw["type"])
        self.assertEqual("CORZW", corzw["ticker"])
        self.assertEqual("CORZW", corzw["security_label"])
        self.assertIn("openfigi_plain_ticker", corzw["sources"])
        self.assertEqual("WARRANT", corzw["security_kind"])

        opaque_fund = registry["02072L359"]
        self.assertIsNone(opaque_fund["ticker"])
        self.assertEqual(
            "ARK 21SHRS ACTBITC FUT STRGY",
            opaque_fund["security_label"],
        )
        self.assertEqual("ETF", opaque_fund["security_kind"])

        foreign_alpha = registry["00437K108"]
        self.assertIsNone(foreign_alpha["ticker"])
        self.assertNotIn("cusip_map_vetted", foreign_alpha["sources"])
        self.assertEqual(
            "ACCELERATE ARBITRAGE FUND",
            foreign_alpha["security_label"],
        )
        self.assertEqual("ETF", foreign_alpha["security_kind"])

        ewy = registry["464286772"]
        self.assertEqual("EWY", ewy["ticker"])
        self.assertEqual("ISHARES INC", ewy["name"])
        self.assertEqual("EWY", ewy["security_label"])
        self.assertEqual("ETF", ewy["security_kind"])
        self.assertEqual(
            "ISHARES MSCI SOUTH KOREA ETF",
            ewy["product_name"],
        )
        self.assertEqual("openfigi", ewy["product_name_source"])

        fallback = registry["123456789"]
        self.assertIsNone(fallback["ticker"])
        self.assertEqual("EQUITY SECURITY", fallback["security_label"])
        self.assertEqual("instrument_type", fallback["label_source"])

        synthetic_label, synthetic_source = (
            pipeline._registry_security_label(
                identifier="0LOOKITUP",
                entry={"ticker": None, "type": "EQUITY"},
                openfigi_detail=None,
                prior_entry=None,
                legacy_openfigi_label=None,
            )
        )
        self.assertEqual(
            "UNIDENTIFIED EQUITY SECURITY",
            synthetic_label,
        )
        self.assertEqual("synthetic_identifier", synthetic_source)

        informative_synthetic_label, informative_synthetic_source = (
            pipeline._registry_security_label(
                identifier="00000CASH",
                entry={
                    "ticker": None,
                    "dominant_issuer": "CASH SWEEP",
                    "dominant_class": "CASH",
                    "type": "EQUITY",
                },
                openfigi_detail=None,
                prior_entry=None,
                legacy_openfigi_label=None,
            )
        )
        self.assertEqual(
            "CASH SWEEP — CASH",
            informative_synthetic_label,
        )
        self.assertEqual(
            "filer_issuer_class",
            informative_synthetic_source,
        )

        right_label, right_source = pipeline._registry_security_label(
            identifier="714920113",
            entry={
                "ticker": None,
                "name": "PERSHING SQUARE SPARC HOLDINGS, LTD.",
                "dominant_issuer": "#N/A INVALID SECURITY",
                "dominant_class": "RIGHT",
                "type": "EQUITY",
                "sources": ["manual_name_override"],
            },
            openfigi_detail=None,
            prior_entry=None,
            legacy_openfigi_label=None,
        )
        self.assertEqual(
            "PERSHING SQUARE SPARC HOLDINGS, LTD. — RIGHT",
            right_label,
        )
        self.assertEqual("manual_name_class", right_source)

        malformed_labels = {
            identifier: pipeline._registry_security_label(
                identifier=identifier,
                entry={"ticker": None, "type": instrument_type},
                openfigi_detail=None,
                prior_entry=None,
                legacy_openfigi_label=None,
            )
            for identifier, instrument_type in {
                "056517388": "EQUITY",
                "056517389": "EQUITY",
                "464287294": "PUT",
                "MONEYMRKT": "EQUITY",
                "OOOOOOOOO": "EQUITY",
            }.items()
        }
        self.assertEqual(
            ("UNIDENTIFIED EQUITY SECURITY", "historical_invalid_identifier"),
            malformed_labels["056517388"],
        )
        self.assertEqual(
            ("UNIDENTIFIED PUT SECURITY", "historical_invalid_identifier"),
            malformed_labels["464287294"],
        )
        self.assertEqual(
            ("MONEY MARKET FUND", "synthetic_identifier"),
            malformed_labels["MONEYMRKT"],
        )
        self.assertEqual(
            ("UNIDENTIFIED EQUITY SECURITY", "synthetic_identifier"),
            malformed_labels["OOOOOOOOO"],
        )

        self.assertEqual(
            "IACH",
            pipeline.MANUAL_CUSIP_TICKER_OVERRIDES["45669R701"],
        )
        self.assertEqual(
            "INFORMATION ARCHITECTS CORP",
            pipeline.MANUAL_CUSIP_NAME_OVERRIDES["45669R701"],
        )
        historical = registry["45669R701"]
        self.assertEqual("IACH", historical["ticker"])
        self.assertEqual(
            "INFORMATION ARCHITECTS CORP",
            historical["name"],
        )
        self.assertEqual("IACH", historical["security_label"])
        self.assertIn("manual_override", historical["sources"])
        self.assertIn("manual_name_override", historical["sources"])

        ionq_warrant = registry["46222L116"]
        self.assertIsNone(ionq_warrant["ticker"])
        self.assertEqual("IONQ INC", ionq_warrant["name"])
        self.assertEqual(
            "IONQ/WS — WARRANT EXP 10/01/26",
            ionq_warrant["security_label"],
        )
        self.assertEqual("manual_verified", ionq_warrant["label_source"])
        self.assertEqual("WARRANT", ionq_warrant["security_kind"])
        self.assertEqual(
            "manual_verified",
            ionq_warrant["security_kind_source"],
        )
        self.assertIn("manual_name_override", ionq_warrant["sources"])

        innoviz_warrant = registry["M5R635116"]
        self.assertIsNone(innoviz_warrant["ticker"])
        self.assertEqual(
            "INNOVIZ TECHNOLOGIES LTD",
            innoviz_warrant["name"],
        )
        self.assertEqual(
            "INVZW — WARRANT EXP 04/05/26",
            innoviz_warrant["security_label"],
        )
        self.assertEqual(
            "manual_verified",
            innoviz_warrant["label_source"],
        )
        self.assertEqual("WARRANT", innoviz_warrant["security_kind"])
        self.assertEqual(
            "filer_metadata",
            innoviz_warrant["security_kind_source"],
        )
        self.assertIn(
            "manual_name_override",
            innoviz_warrant["sources"],
        )

        retained = registry["12345ABC6"]
        self.assertEqual(
            "KEEP 4.25 01/01/30",
            retained["security_label"],
        )
        self.assertEqual(
            "openfigi_prior_registry",
            retained["label_source"],
        )
        self.assertEqual("BOND", retained["security_kind"])
        self.assertEqual(
            "openfigi_prior_registry",
            retained["security_kind_source"],
        )

        self.assertEqual(
            set(registry),
            set(labels_payload["labels"]),
        )
        self.assertEqual(
            {
                cusip: entry["security_label"]
                for cusip, entry in registry.items()
            },
            labels_payload["labels"],
        )
        self.assertEqual(
            {
                cusip: entry["security_kind"]
                for cusip, entry in registry.items()
                if entry.get("security_kind")
            },
            labels_payload["kinds"],
        )
        self.assertEqual(
            sorted(
                cusip
                for cusip, entry in registry.items()
                if pipeline._registry_entry_has_equity_fund_identity(entry)
            ),
            labels_payload["fund_identities"],
        )
        self.assertEqual(
            {
                cusip: entry["product_name"]
                for cusip, entry in registry.items()
                if entry.get("product_name")
            },
            labels_payload["product_names"],
        )
        self.assertFalse(any(
            cusip == label
            for cusip, label in labels_payload["labels"].items()
        ))

    def test_cold_cache_retains_registry_confirmed_fund_ticker(self) -> None:
        evidence = {
            "921937827": {
                "total_value": 100,
                "holder_ciks": {1},
                "issuer_value": {"VANGUARD BD INDEX FDS": 100},
                "class_value": {"SHORT TRM BOND": 100},
                "put_call_value": {},
                "first_seen": "2025-12-31",
                "last_seen": "2026-03-31",
            },
        }
        prior_registry = {
            "921937827": {
                "ticker": "BSV",
                "name": "VANGUARD BD INDEX FDS",
                "dominant_issuer": "VANGUARD BD INDEX FDS",
                "dominant_class": "SHORT TRM BOND",
                "security_label": "BSV",
                "label_source": "openfigi",
                "security_kind": "ETF",
                "security_kind_source": "openfigi",
                "type": "NOTE",
                "sources": ["filer_dominant"],
            },
        }
        with mock.patch.multiple(
            pipeline,
            FUNDS_DIR=mock.MagicMock(exists=mock.Mock(return_value=True)),
            _aggregate_cusip_evidence=mock.Mock(return_value=evidence),
            load_cusip_map=mock.Mock(return_value={}),
            load_cusip_registry=mock.Mock(return_value=prior_registry),
            load_openfigi_details=mock.Mock(return_value={}),
            load_sec_fund_name_cache=mock.Mock(return_value={}),
            save_cusip_registry=mock.Mock(),
        ):
            registry = pipeline.build_cusip_registry(
                company_ticker_data=[],
            )

        bsv = registry["921937827"]
        self.assertEqual("EQUITY", bsv["type"])
        self.assertEqual("ETF", bsv["security_kind"])
        self.assertEqual("BSV", bsv["ticker"])
        self.assertIn("openfigi_prior_registry_ticker", bsv["sources"])

    def test_cold_cache_retains_structurally_confirmed_etf_ticker(self) -> None:
        evidence = {
            "464287150": {
                "total_value": 100,
                "holder_ciks": {1},
                "issuer_value": {"ISHARES TR": 100},
                "class_value": {"CORE S&P TTL STK": 100},
                "put_call_value": {},
                "first_seen": "2025-12-31",
                "last_seen": "2026-03-31",
            },
        }
        prior_registry = {
            "464287150": {
                "ticker": "ITOT",
                "name": "ISHARES TR",
                "dominant_issuer": "ISHARES TR",
                "dominant_class": "CORE S&P TTL STK",
                "security_label": "ITOT",
                "label_source": "canonical_ticker",
                "security_kind": "ETF",
                "security_kind_source": "filer_metadata",
                "type": "EQUITY",
                "sources": ["filer_dominant", "cusip_map_vetted"],
            },
        }
        with mock.patch.multiple(
            pipeline,
            FUNDS_DIR=mock.MagicMock(exists=mock.Mock(return_value=True)),
            _aggregate_cusip_evidence=mock.Mock(return_value=evidence),
            load_cusip_map=mock.Mock(return_value={}),
            load_cusip_registry=mock.Mock(return_value=prior_registry),
            load_openfigi_details=mock.Mock(return_value={}),
            load_sec_fund_name_cache=mock.Mock(return_value={}),
            save_cusip_registry=mock.Mock(),
        ):
            registry = pipeline.build_cusip_registry(
                company_ticker_data=[],
            )

        itot = registry["464287150"]
        self.assertEqual("EQUITY", itot["type"])
        self.assertEqual("ETF", itot["security_kind"])
        self.assertEqual("ITOT", itot["ticker"])
        self.assertIn("openfigi_prior_registry_ticker", itot["sources"])

    def test_cold_cache_retains_untyped_fund_identity_from_five_letter_symbol(
        self,
    ) -> None:
        evidence = {
            "09260B630": {
                "total_value": 100,
                "holder_ciks": {1},
                "issuer_value": {"BLACKROCK HIGH YIELD INSTL": 100},
                "class_value": {"MFF": 100},
                "non_option_issuer_value": {
                    "BLACKROCK HIGH YIELD INSTL": 100,
                },
                "non_option_issuer_count": {
                    "BLACKROCK HIGH YIELD INSTL": 1,
                },
                "non_option_class_value": {"MFF": 100},
                "non_option_class_count": {"MFF": 1},
                "instrument_type_value": {"NOTE": 100},
                "instrument_type_count": {"NOTE": 1},
                "put_call_value": {},
                "first_seen": "2025-12-31",
                "last_seen": "2026-03-31",
            },
        }
        prior_registry = {
            "09260B630": {
                "ticker": "BHYIX",
                "name": "BLACKROCK HIGH YIELD INSTL",
                "dominant_issuer": "BLACKROCK HIGH YIELD INSTL",
                "dominant_class": "MFF",
                "security_label": "BHYIX",
                "label_source": "canonical_ticker",
                "type": "EQUITY",
                "sources": ["filer_dominant", "cusip_map_vetted"],
            },
        }
        with mock.patch.multiple(
            pipeline,
            FUNDS_DIR=mock.MagicMock(exists=mock.Mock(return_value=True)),
            _aggregate_cusip_evidence=mock.Mock(return_value=evidence),
            load_cusip_map=mock.Mock(return_value={}),
            load_cusip_registry=mock.Mock(return_value=prior_registry),
            load_openfigi_details=mock.Mock(return_value={}),
            load_sec_fund_name_cache=mock.Mock(return_value={}),
            save_cusip_registry=mock.Mock(),
        ):
            registry = pipeline.build_cusip_registry(
                company_ticker_data=[],
            )

        bhyix = registry["09260B630"]
        self.assertEqual("EQUITY", bhyix["type"])
        self.assertEqual("BHYIX", bhyix["ticker"])
        self.assertNotIn("security_kind", bhyix)
        self.assertIn("openfigi_prior_registry_ticker", bhyix["sources"])
        self.assertTrue(
            pipeline._registry_entry_has_equity_fund_identity(bhyix)
        )

    def test_cold_cache_guarded_etfs_outrank_raw_note_and_preferred_types(
        self,
    ) -> None:
        cases = {
            "74933W452": (
                "TBIL",
                "RBB FD INC",
                "US TREAS 3 MNTH",
                "NOTE",
            ),
            "808524862": (
                "SCHO",
                "SCHWAB STRATEGIC TR",
                "SHT TM US TRES",
                "PREF",
            ),
        }
        evidence = {}
        prior_registry = {}
        for identifier, (ticker, issuer, security_class, raw_type) in cases.items():
            evidence[identifier] = {
                "total_value": 100,
                "holder_ciks": {1},
                "issuer_value": {issuer: 100},
                "class_value": {security_class: 100},
                "non_option_issuer_value": {issuer: 100},
                "non_option_issuer_count": {issuer: 1},
                "non_option_class_value": {security_class: 100},
                "non_option_class_count": {security_class: 1},
                "instrument_type_value": {raw_type: 100},
                "instrument_type_count": {raw_type: 1},
                "put_call_value": {},
                "first_seen": "2025-12-31",
                "last_seen": "2026-03-31",
            }
            prior_registry[identifier] = {
                "ticker": ticker,
                "name": issuer,
                "dominant_issuer": issuer,
                "dominant_class": security_class,
                "security_label": ticker,
                "label_source": "canonical_ticker",
                "security_kind": "ETF",
                "security_kind_source": "filer_metadata",
                "type": "EQUITY",
                "sources": ["filer_dominant", "cusip_map_vetted"],
            }
        with mock.patch.multiple(
            pipeline,
            FUNDS_DIR=mock.MagicMock(exists=mock.Mock(return_value=True)),
            _aggregate_cusip_evidence=mock.Mock(return_value=evidence),
            load_cusip_map=mock.Mock(return_value={}),
            load_cusip_registry=mock.Mock(return_value=prior_registry),
            load_openfigi_details=mock.Mock(return_value={}),
            load_sec_fund_name_cache=mock.Mock(return_value={}),
            save_cusip_registry=mock.Mock(),
        ):
            registry = pipeline.build_cusip_registry(
                company_ticker_data=[],
            )

        for identifier, (ticker, _issuer, _class, _raw_type) in cases.items():
            with self.subTest(ticker=ticker):
                entry = registry[identifier]
                self.assertEqual("EQUITY", entry["type"])
                self.assertEqual("ETF", entry["security_kind"])
                self.assertEqual(ticker, entry["ticker"])
                self.assertIn(
                    "openfigi_prior_registry_ticker",
                    entry["sources"],
                )

    def test_current_vetted_map_proves_new_fund_identity_before_type_inference(
        self,
    ) -> None:
        cases = {
            "09260B630": (
                "BHYIX",
                "BLACKROCK HIGH YIELD INSTL",
                "MFF",
                "NOTE",
                None,
            ),
            "05569M608": (
                "BCOIX",
                "BROWN ADVISORY TOTAL RETURN",
                "FD",
                "PREF",
                None,
            ),
            "74933W452": (
                "TBIL",
                "RBB FD INC",
                "US TREAS 3 MNTH",
                "NOTE",
                "ETF",
            ),
            "808524862": (
                "SCHO",
                "SCHWAB STRATEGIC TR",
                "SHT TM US TRES",
                "PREF",
                "ETF",
            ),
            "808515605": (
                "SWRSX",
                "SCHWAB STRATEGIC TR",
                "FD",
                "NOTE",
                None,
            ),
        }
        evidence = {}
        cusip_map = {}
        for identifier, (
            ticker,
            issuer,
            security_class,
            raw_type,
            _expected_kind,
        ) in cases.items():
            evidence[identifier] = {
                "total_value": 100,
                "holder_ciks": {1},
                "issuer_value": {issuer: 100},
                "class_value": {security_class: 100},
                "non_option_issuer_value": {issuer: 100},
                "non_option_issuer_count": {issuer: 1},
                "non_option_class_value": {security_class: 100},
                "non_option_class_count": {security_class: 1},
                "instrument_type_value": {raw_type: 100},
                "instrument_type_count": {raw_type: 1},
                "put_call_value": {},
                "first_seen": "2025-12-31",
                "last_seen": "2026-03-31",
            }
            cusip_map[identifier] = ticker
        with mock.patch.multiple(
            pipeline,
            FUNDS_DIR=mock.MagicMock(exists=mock.Mock(return_value=True)),
            _aggregate_cusip_evidence=mock.Mock(return_value=evidence),
            load_cusip_map=mock.Mock(return_value=cusip_map),
            load_cusip_registry=mock.Mock(return_value={}),
            load_openfigi_details=mock.Mock(return_value={}),
            load_sec_fund_name_cache=mock.Mock(return_value={}),
            save_cusip_registry=mock.Mock(),
        ):
            registry = pipeline.build_cusip_registry(
                company_ticker_data=[],
            )

        for identifier, (
            ticker,
            _issuer,
            _class,
            _raw_type,
            expected_kind,
        ) in cases.items():
            with self.subTest(ticker=ticker):
                entry = registry[identifier]
                self.assertEqual("EQUITY", entry["type"])
                self.assertEqual(ticker, entry["ticker"])
                self.assertEqual(expected_kind, entry.get("security_kind"))
                self.assertTrue(
                    pipeline._registry_entry_has_equity_fund_identity(entry)
                )

    def test_pre_type_fund_evidence_rejects_duplicate_or_label_only_symbols(
        self,
    ) -> None:
        prior_label_only = {
            "ticker": None,
            "security_label": "BHYIX",
            "label_source": "openfigi",
            "type": "EQUITY",
            "sources": ["cusip_map_vetted"],
        }
        self.assertIsNone(
            pipeline._prior_registry_fund_ticker(
                prior_label_only,
                identifier="09260B630",
                instrument_type="EQUITY",
            )
        )
        self.assertEqual(
            (None, None),
            pipeline._fund_identity_ticker_candidate(
                identifier="09260B630",
                dominant_class="MFF",
                legacy_ticker="BHYIX",
                legacy_ticker_claims=Counter({"BHYIX": 2}),
                openfigi_detail=None,
                prior_entry=None,
                filer_kind=None,
            ),
        )
        self.assertEqual(
            "NOTE",
            pipeline._registry_type_from_evidence(
                {
                    "total_value": 100,
                    "issuer_value": {"GENERIC ISSUER": 100},
                    "class_value": {"SR NOTE": 100},
                    "instrument_type_value": {"NOTE": 100},
                    "instrument_type_count": {"NOTE": 1},
                    "put_call_value": {},
                },
                identifier="09260B630",
                prior_entry={
                    "ticker": "BHYIX",
                    "security_kind": "PREFERRED",
                    "security_kind_source": "openfigi",
                    "type": "EQUITY",
                    "sources": ["cusip_map_vetted"],
                },
                filer_fund_identity=True,
            ),
        )
        self.assertEqual(
            "EQUITY",
            pipeline._registry_type_from_evidence(
                {
                    "total_value": 100,
                    "issuer_value": {"GENERIC FUND": 100},
                    "class_value": {"SR NOTE": 100},
                    "instrument_type_value": {"NOTE": 100},
                    "instrument_type_count": {"NOTE": 1},
                    "put_call_value": {},
                },
                identifier="09260B630",
                prior_entry={
                    "ticker": "BHYIX",
                    "security_kind": "PREFERRED",
                    "security_kind_source": "filer_metadata",
                    "type": "EQUITY",
                    "sources": ["cusip_map_vetted"],
                },
                filer_fund_identity=True,
            ),
        )

    def test_stale_filer_etf_prior_cannot_override_changed_current_issuer(
        self,
    ) -> None:
        issuer = "GENERIC DEBT ISSUER"
        evidence = {
            "74933W452": {
                "total_value": 100,
                "holder_ciks": {1},
                "issuer_value": {issuer: 100},
                "class_value": {"SR NOTE": 100},
                "non_option_issuer_value": {issuer: 100},
                "non_option_issuer_count": {issuer: 1},
                "non_option_class_value": {"SR NOTE": 100},
                "non_option_class_count": {"SR NOTE": 1},
                "instrument_type_value": {"NOTE": 100},
                "instrument_type_count": {"NOTE": 1},
                "put_call_value": {},
                "first_seen": "2025-12-31",
                "last_seen": "2026-03-31",
            },
        }
        prior_registry = {
            "74933W452": {
                "ticker": "TBIL",
                "name": "RBB FD INC",
                "dominant_issuer": "RBB FD INC",
                "dominant_class": "US TREAS 3 MNTH",
                "security_label": "TBIL",
                "label_source": "canonical_ticker",
                "security_kind": "ETF",
                "security_kind_source": "filer_metadata",
                "type": "EQUITY",
                "sources": ["filer_dominant", "cusip_map_vetted"],
            },
        }
        with mock.patch.multiple(
            pipeline,
            FUNDS_DIR=mock.MagicMock(exists=mock.Mock(return_value=True)),
            _aggregate_cusip_evidence=mock.Mock(return_value=evidence),
            load_cusip_map=mock.Mock(return_value={}),
            load_cusip_registry=mock.Mock(return_value=prior_registry),
            load_openfigi_details=mock.Mock(return_value={}),
            load_sec_fund_name_cache=mock.Mock(return_value={}),
            save_cusip_registry=mock.Mock(),
        ):
            registry = pipeline.build_cusip_registry(
                company_ticker_data=[],
            )

        entry = registry["74933W452"]
        self.assertEqual("NOTE", entry["type"])
        self.assertIsNone(entry["ticker"])
        self.assertNotIn("security_kind", entry)

    def test_fund_product_names_preserve_full_names_and_use_safe_fallbacks(
        self,
    ) -> None:
        verbose_mutual = {
            "name": (
                "INVESCO INTERMEDIATE TERM MUNI INCOME FUND CLASS Y"
            ),
            "dominant_issuer": (
                "INVESCO INTERMEDIATE TERM MUNI INCOME FUND CLASS Y"
            ),
            "dominant_class": "MUTUAL FUND",
            "security_kind": "MUTUAL FUND",
            "sources": ["filer_dominant"],
        }
        self.assertEqual(
            (None, None),
            pipeline._registry_fund_product_name(
                identifier="001419563",
                entry=verbose_mutual,
                openfigi_detail={
                    "status": "matched",
                    "ticker": "VKLIX",
                    "securityDescription": "VKLIX",
                    "name": "INVESCO INTM TRM MUNI INC-Y",
                },
                prior_entry=None,
            ),
        )

        self.assertEqual(
            (None, None),
            pipeline._registry_fund_product_name(
                identifier="911717106",
                entry={
                    "ticker": "USCI",
                    "security_label": "USCI",
                    "name": "United States Commodity Index Funds Trust",
                    "dominant_issuer": (
                        "United States Commodity Index Funds Trust"
                    ),
                    "dominant_class": "COMM IDX FND",
                    "security_kind": "ETF",
                    "sources": ["sec_title"],
                },
                openfigi_detail={
                    "status": "matched",
                    "ticker": "USCI",
                    "securityDescription": "USCI",
                    "name": "UNITED STATES COMMODITY INDE",
                },
                prior_entry=None,
            ),
        )

        for identifier, existing, dominant_class, kind, ticker, candidate in (
            (
                "893509224",
                "TRANSAMERICA INTERNATIONAL EQUITY FUND CLASS I",
                "MUTUAL FUND",
                "MUTUAL FUND",
                "TSWIX",
                "TRANSAM INTL EQTY-I",
            ),
            (
                "921937504",
                "VANGUARD TOTAL BOND MARKET INDEX FUND",
                "MUTUAL FUND",
                "MUTUAL FUND",
                "VBTIX",
                "VANGUARD TTL BD MKT IDX-INST",
            ),
            (
                "09248X100",
                "BlackRock Taxable Municipal Bond Trust",
                "SHS",
                "CLOSED-END FUND",
                "BBN",
                "BLACKROCK TAXABLE MUNI BND",
            ),
            (
                "153436100",
                "CENTRAL & EASTERN EUROPE FUND, INC.",
                "COM",
                "CLOSED-END FUND",
                "CEE",
                "CENTRAL AND EASTERN EUROPE F",
            ),
            (
                "46438M106",
                "iShares Staked Ethereum Trust ETF",
                "SHARES OF FRACTI",
                "ETF",
                "ETHB",
                "ISHR STAKED ETHER TRST ETF",
            ),
            (
                "027681105",
                "AMERICAN MUTUAL FUND CLASS F2",
                "MUTUAL FUND",
                "MUTUAL FUND",
                "AMRFX",
                "AMER FNDS MUTUAL FND-F2",
            ),
        ):
            with self.subTest(identifier=identifier):
                self.assertEqual(
                    (None, None),
                    pipeline._registry_fund_product_name(
                        identifier=identifier,
                        entry={
                            "ticker": ticker,
                            "security_label": ticker,
                            "name": existing,
                            "dominant_issuer": existing,
                            "dominant_class": dominant_class,
                            "security_kind": kind,
                            "sources": ["sec_title"],
                        },
                        openfigi_detail={
                            "status": "matched",
                            "ticker": ticker,
                            "securityDescription": ticker,
                            "name": candidate,
                        },
                        prior_entry=None,
                    ),
                )

        ark_name, ark_source = pipeline._registry_fund_product_name(
            identifier="00214Q904",
            entry={
                "name": "ARK ETF TR",
                "dominant_issuer": "ARK ETF TR",
                "dominant_class": "INNOVATION ETF",
                "security_kind": "ETF",
                "sources": ["filer_dominant"],
            },
            openfigi_detail=None,
            prior_entry=None,
        )
        self.assertEqual("ARK ETF TR — INNOVATION ETF", ark_name)
        self.assertEqual("filer_issuer_class", ark_source)

        retained_name, retained_source = (
            pipeline._registry_fund_product_name(
                identifier="46429B598",
                entry={
                    "name": "ISHARES TR",
                    "dominant_issuer": "ISHARES TR",
                    "dominant_class": "MSCI INDIA ETF",
                    "security_kind": "ETF",
                    "sources": ["filer_dominant"],
                },
                openfigi_detail=None,
                prior_entry={
                    "product_name": "ISHARES MSCI INDIA ETF",
                    "product_name_source": "openfigi",
                },
            )
        )
        self.assertEqual("ISHARES MSCI INDIA ETF", retained_name)
        self.assertEqual("openfigi_prior_registry", retained_source)

        concatenated_name, concatenated_source = (
            pipeline._registry_fund_product_name(
                identifier="46438G653",
                entry={
                    "name": "ISHARES TR",
                    "dominant_issuer": "ISHARES TR",
                    "dominant_class": "IBONDS DEC 2034",
                    "security_kind": "ETF",
                    "sources": ["filer_dominant"],
                },
                openfigi_detail={
                    "status": "matched",
                    "ticker": "IBDZ",
                    "securityDescription": "IBDZ",
                    "name": "ISHARESIBONDSDEC2034TMCORP",
                },
                prior_entry=None,
            )
        )
        self.assertEqual(
            "ISHARES TR — IBONDS DEC 2034",
            concatenated_name,
        )
        self.assertEqual("filer_issuer_class", concatenated_source)

        self.assertEqual(
            (None, None),
            pipeline._registry_fund_product_name(
                identifier="137221107",
                entry={
                    "name": "CANARY LITECOIN ETF",
                    "dominant_issuer": "CANARY LITECOIN ETF",
                    "dominant_class": "LITECOIN ETF",
                    "security_kind": "ETF",
                    "sources": ["filer_dominant"],
                },
                openfigi_detail={
                    "status": "matched",
                    "ticker": "LTCC",
                    "securityDescription": "LTCC",
                    "name": "CNRY LTECN ETF",
                },
                prior_entry=None,
            ),
        )

        prior_name, prior_source = pipeline._registry_fund_product_name(
            identifier="38964T206",
            entry={
                "name": "GRAYSCALE INVESTMENTS LLC",
                "dominant_issuer": "GRAYSCALE INVESTMENTS LLC",
                "dominant_class": "ETHEREUM STAKING ETF",
                "security_kind": "ETF",
                "sources": ["filer_dominant"],
            },
            openfigi_detail={
                "status": "matched",
                "ticker": "ETHE",
                "securityDescription": "ETHE",
                "name": "GRAYSCALE ETHEREUM STAKING E",
            },
            prior_entry={
                "product_name": "GRAYSCALE ETHEREUM STAKING ETF",
                "product_name_source": "openfigi",
            },
        )
        self.assertEqual(
            "GRAYSCALE ETHEREUM STAKING ETF",
            prior_name,
        )
        self.assertEqual("openfigi_prior_registry", prior_source)

        renamed_name, renamed_source = pipeline._registry_fund_product_name(
            identifier="123456789",
            entry={
                "name": "SPONSOR ETF TR",
                "dominant_issuer": "SPONSOR ETF TR",
                "dominant_class": "ETF",
                "security_kind": "ETF",
                "sources": ["filer_dominant"],
            },
            openfigi_detail={
                "status": "matched",
                "ticker": "NEWF",
                "securityDescription": "NEWF",
                "name": "NEW SHORT VALUE ETF",
            },
            prior_entry={
                "product_name": "OLD SPONSOR LONG STRATEGY ETF",
                "product_name_source": "openfigi",
            },
        )
        self.assertEqual("NEW SHORT VALUE ETF", renamed_name)
        self.assertEqual("openfigi", renamed_source)

        for identifier, existing, dominant_class, ticker, candidate in (
            (
                "00039J731",
                "AB ACTIVE ETFS INC",
                "US EQUITY ETF",
                "XCHG",
                "AB US EQUITY ETF",
            ),
            (
                "92647N550",
                "VICTORY PORTFOLIOS II",
                "ETF",
                "UIVM",
                "VICTORYSHARES INTERNATIONAL",
            ),
            (
                "381430180",
                "GOLDMAN SACHS ETF TR",
                "CMN",
                "GSID",
                "GOLDMAN SACHS MARKETBETA INT",
            ),
            (
                "33735J101",
                "FIRST TR EXCHANGE-TRADED ALP",
                "COM SHS",
                "FTA",
                "FIRST TRUST L C VAL ALP",
            ),
            (
                "69344A750",
                "PGIM ETF TR",
                "PGIM CORP BD 0",
                "PCS",
                "PGIM CORP BOND 0-5 YEAR ETF",
            ),
        ):
            with self.subTest(identifier=identifier):
                self.assertEqual(
                    (candidate, "openfigi"),
                    pipeline._registry_fund_product_name(
                        identifier=identifier,
                        entry={
                            "name": existing,
                            "dominant_issuer": existing,
                            "dominant_class": dominant_class,
                            "security_kind": "ETF",
                            "sources": ["filer_dominant"],
                        },
                        openfigi_detail={
                            "status": "matched",
                            "ticker": ticker,
                            "securityDescription": ticker,
                            "name": candidate,
                        },
                        prior_entry=None,
                    ),
                )

        for identifier, existing, dominant_class, ticker, candidate in (
            (
                "78462F103",
                "SPDR S&P 500 ETF TRUST",
                "TR UNIT",
                "SPY",
                "SS SPDR S&P 500 ETF TRUST-US",
            ),
            (
                "78467X109",
                "SPDR DOW JONES INDUSTRIAL AVERAGE ETF TRUST",
                "UT SER 1",
                "DIA",
                "SS SPDR DOW JONES INDUS AVG",
            ),
            (
                "78463V107",
                "SPDR GOLD TRUST",
                "GOLD SHS",
                "GLD",
                "SPDR GOLD SHARES",
            ),
        ):
            with self.subTest(identifier=identifier):
                self.assertEqual(
                    (None, None),
                    pipeline._registry_fund_product_name(
                        identifier=identifier,
                        entry={
                            "name": existing,
                            "dominant_issuer": existing,
                            "dominant_class": dominant_class,
                            "security_kind": "ETF",
                            "sources": ["filer_dominant"],
                        },
                        openfigi_detail={
                            "status": "matched",
                            "ticker": ticker,
                            "securityDescription": ticker,
                            "name": candidate,
                        },
                        prior_entry=None,
                    ),
                )

        for identifier, existing, dominant_class in (
            (
                "00301W105",
                "ABRDN ASIA-PACIFIC INCOME FUND, INC.",
                "COM NEW",
            ),
            (
                "09174C104",
                "Bitwise 10 Crypto Index ETF",
                "UNIT BENEFICIAL",
            ),
            (
                "92189K105",
                "VanEck Avalanche ETF",
                "COM SHS BENF INT",
            ),
            (
                "091948109",
                "Bitwise Solana Staking ETF",
                "COM SHS OF BENEF",
            ),
            (
                "09257D102",
                "Blackstone Long-Short Credit Income Fund",
                "COM SHS BN INT",
            ),
            (
                "G40705108",
                "Grayscale CoinDesk Crypto 5 ETF",
                "USD SHS",
            ),
            (
                "78464A375",
                "SPDR SERIES TRUST",
                "STATE STREET SPD",
            ),
            (
                "14020G101",
                "CAPITAL GROUP GROWTH ETF",
                "SHS CREATION UNI",
            ),
            (
                "23306X506",
                "DBX ETF TR XTRACKERS S&P ESG DIVIDEND ARISTOCRATS ETF",
                "SNPD",
            ),
            (
                "0075W0825",
                "ADVISORS INNER CIRCLE FUND CAM",
                "ADR",
            ),
            (
                "04314H881",
                "ARTISAN INTERNATIONAL VALUE FUND",
                "EQUITIES",
            ),
            (
                "72202D106",
                "PIMCO DYNAMIC CREDIT AND MORTGAGE INCOME FUND",
                "CEF",
            ),
            (
                "471023564",
                "JANUS HENDERSON SMALL CAP VALUE FUND",
                "MUT",
            ),
        ):
            with self.subTest(identifier=identifier):
                self.assertEqual(
                    (None, None),
                    pipeline._registry_fund_product_name(
                        identifier=identifier,
                        entry={
                            "name": existing,
                            "dominant_issuer": existing,
                            "dominant_class": dominant_class,
                            "ticker": (
                                dominant_class
                                if dominant_class == "SNPD"
                                else None
                            ),
                            "security_label": (
                                dominant_class
                                if dominant_class == "SNPD"
                                else None
                            ),
                            "security_kind": "ETF",
                            "sources": ["sec_title"],
                        },
                        openfigi_detail=None,
                        prior_entry=None,
                    ),
                )

        turtle_entry = {
            "name": "TURTLE BEACH CORP",
            "dominant_issuer": "TURTLE BEACH CORP",
            "dominant_class": "COM",
            "security_kind": "ETF",
            "sources": ["filer_dominant"],
            "type": "EQUITY",
        }
        turtle_detail = {
            "status": "matched",
            "ticker": "TBCH",
            "securityDescription": "TBCH",
            "name": "TD TGT 2028 INV GR BD",
            "securityType": "ETP",
            "securityType2": "Mutual Fund",
            "marketSector": "Equity",
            "exchCode": "CN",
        }
        self.assertEqual(
            (None, None),
            pipeline._registry_fund_product_name(
                identifier="87252P106",
                entry=turtle_entry,
                openfigi_detail=turtle_detail,
                prior_entry=None,
            ),
        )
        self.assertEqual(
            (None, None),
            pipeline._registry_security_kind(
                identifier="87252P106",
                entry=turtle_entry,
                openfigi_detail=turtle_detail,
                prior_entry={
                    "security_kind": "ETF",
                    "security_kind_source": "openfigi",
                },
            ),
        )
        self.assertEqual(
            ("TURTLE BEACH CORP", "filer_issuer"),
            pipeline._registry_security_label(
                identifier="87252P106",
                entry=turtle_entry,
                openfigi_detail=turtle_detail,
                prior_entry={
                    "security_label": "TD TGT 2028 INV GR BD",
                    "label_source": "openfigi",
                },
                legacy_openfigi_label="TD TGT 2028 INV GR BD",
            ),
        )

        for kind in ("COMMON", "PREFERRED", "BOND"):
            with self.subTest(kind=kind):
                self.assertEqual(
                    (None, None),
                    pipeline._registry_fund_product_name(
                        identifier="037833100",
                        entry={
                            "name": "APPLE INC",
                            "dominant_issuer": "APPLE INC",
                            "dominant_class": "COM",
                            "security_kind": kind,
                            "sources": ["sec_title"],
                        },
                        openfigi_detail={
                            "status": "matched",
                            "ticker": "AAPL",
                            "securityDescription": "AAPL",
                            "name": "APPLE INC",
                        },
                        prior_entry=None,
                    ),
                )

    def test_sec_fund_series_parser_and_name_composition(self) -> None:
        page = """
        <table>
          <tr><td colspan="3"><b>CIK</b></td><td></td><td></td></tr>
          <tr><td></td><td colspan="2"><b>Series</b></td>
            <td></td><td><b>Ticker</b></td></tr>
          <tr><td></td><td></td><td><b>Class/Contract</b></td>
            <td><b>Name</b></td><td><b>Symbol</b></td></tr>
          <tr><td><td colspan="2">
            <a href="/cgi-bin/browse-edgar?action=getcompany&amp;CIK=S000055059">
              S000055059
            </a></td><td>
            <a>iShares iBonds Dec 2026 Term Corporate ETF</a>
          </td><td></td></tr>
          <tr><td><td><td>
            <a href="/cgi-bin/browse-edgar?action=getcompany&amp;CIK=C000173141">
              C000173141
            </a></td><td>iShares iBonds Dec 2026 Term Corporate ETF</td>
            <td>IBDR</td>
          </tr>
          <tr><td><td colspan="2">
            <a href="/cgi-bin/browse-edgar?action=getcompany&amp;CIK=S000008999">
              S000008999
            </a></td><td><a>AMERICAN MUTUAL FUND</a></td>
            <td></td>
          </tr>
          <tr><td><td><td>
            <a href="/cgi-bin/browse-edgar?action=getcompany&amp;CIK=C000068556">
              C000068556
            </a></td><td>Class F-2</td>
            <td>AMRFX</td>
          </tr>
        </table>
        """
        series_names, class_names = (
            pipeline._parse_sec_fund_series_page(page)
        )
        self.assertEqual(
            "iShares iBonds Dec 2026 Term Corporate ETF",
            series_names["S000055059"],
        )
        self.assertEqual(
            "iShares iBonds Dec 2026 Term Corporate ETF",
            class_names["C000173141"],
        )
        self.assertEqual(
            "iShares iBonds Dec 2026 Term Corporate ETF",
            pipeline._sec_official_fund_name(
                series_names["S000055059"],
                class_names["C000173141"],
            ),
        )
        self.assertEqual(
            "AMERICAN MUTUAL FUND — Class F-2",
            pipeline._sec_official_fund_name(
                series_names["S000008999"],
                class_names["C000068556"],
            ),
        )

    def test_sec_fund_series_parser_fails_closed_on_bad_headers(self) -> None:
        missing_header = """
        <table>
          <tr><td></td><td colspan="2"><a href="?CIK=S000055059">
            S000055059</a></td><td>Fund Name</td><td>IBDR</td></tr>
        </table>
        """
        ambiguous_header = """
        <table>
          <tr><td></td><td></td><td><b>Name</b></td>
            <td><b>Name</b></td><td><b>Symbol</b></td></tr>
          <tr><td></td><td colspan="2"><a href="?CIK=S000055059">
            S000055059</a></td><td>Fund Name</td><td>IBDR</td></tr>
        </table>
        """
        data_ticker_named_name = """
        <table>
          <tr><td colspan="3"><b>CIK</b></td><td></td><td></td></tr>
          <tr><td></td><td colspan="2"><b>Series</b></td>
            <td></td><td><b>Ticker</b></td></tr>
          <tr><td></td><td></td><td><b>Class/Contract</b></td>
            <td><b>Name</b></td><td><b>Symbol</b></td></tr>
          <tr><td></td><td colspan="2"><a href="?CIK=S000000001">
            S000000001</a></td><td>Example Fund</td></tr>
          <tr><td></td><td></td><td><a href="?CIK=C000000001">
            C000000001</a></td><td>Example Fund</td><td>NAME</td></tr>
        </table>
        """
        conflicting_names = """
        <table>
          <tr><td colspan="3"><b>CIK</b></td><td></td><td></td></tr>
          <tr><td></td><td colspan="2"><b>Series</b></td>
            <td></td><td><b>Ticker</b></td></tr>
          <tr><td></td><td></td><td><b>Class/Contract</b></td>
            <td><b>Name</b></td><td><b>Symbol</b></td></tr>
          <tr><td></td><td colspan="2"><a href="?CIK=S000000001">
            S000000001</a></td><td>First Name</td></tr>
          <tr><td></td><td colspan="2"><a href="?CIK=S000000001">
            S000000001</a></td><td>Conflicting Name</td></tr>
        </table>
        """
        self.assertEqual(
            ({}, {}), pipeline._parse_sec_fund_series_page(missing_header)
        )
        self.assertEqual(
            ({}, {}), pipeline._parse_sec_fund_series_page(ambiguous_header)
        )
        self.assertEqual(
            (
                {"S000000001": "Example Fund"},
                {"C000000001": "Example Fund"},
            ),
            pipeline._parse_sec_fund_series_page(data_ticker_named_name),
        )
        self.assertEqual(
            ({}, {}),
            pipeline._parse_sec_fund_series_page(conflicting_names),
        )

    def test_registry_build_preserves_daily_name_and_refreshes_full_mode(
        self,
    ) -> None:
        identifier = "464286772"
        official_name = "iShares MSCI South Korea ETF — EWY"
        renamed_official_name = "iShares MSCI Korea ETF — EWY"
        evidence = {
            identifier: {
                "total_value": 100,
                "holder_ciks": {1},
                "issuer_value": {"ISHARES INC": 100},
                "class_value": {"MSCI STH KOR ETF": 100},
                "put_call_value": {},
                "first_seen": "2025-12-31",
                "last_seen": "2026-03-31",
            },
        }
        prior_registry = {
            identifier: {
                "ticker": "EWY",
                "name": "ISHARES INC",
                "type": "EQUITY",
                "dominant_issuer": "ISHARES INC",
                "dominant_class": "MSCI STH KOR ETF",
                "security_label": "EWY",
                "label_source": "openfigi",
                "security_kind": "ETF",
                "security_kind_source": "openfigi",
                "product_name": official_name,
                "product_name_source": "sec_fund_series",
                "sources": ["filer_dominant", "openfigi_plain_ticker"],
            },
        }
        for refresh_official_names, expected_name in (
            (False, official_name),
            (True, renamed_official_name),
        ):
            with self.subTest(
                refresh_official_names=refresh_official_names
            ):
                refresh_names = mock.Mock(return_value={
                    "EWY": renamed_official_name,
                })
                with mock.patch.multiple(
                    pipeline,
                    FUNDS_DIR=mock.MagicMock(
                        exists=mock.Mock(return_value=True)
                    ),
                    _aggregate_cusip_evidence=mock.Mock(
                        return_value=evidence
                    ),
                    load_cusip_map=mock.Mock(
                        return_value={identifier: "EWY"}
                    ),
                    load_cusip_registry=mock.Mock(
                        return_value=prior_registry
                    ),
                    load_openfigi_details=mock.Mock(return_value={}),
                    load_sec_fund_name_cache=mock.Mock(return_value={}),
                    refresh_sec_fund_names=refresh_names,
                    save_cusip_registry=mock.Mock(),
                ):
                    registry = pipeline.build_cusip_registry(
                        company_ticker_data=[],
                        refresh_official_fund_names=(
                            refresh_official_names
                        ),
                    )

                if refresh_official_names:
                    refresh_names.assert_called_once_with({"EWY"})
                else:
                    refresh_names.assert_not_called()
                self.assertEqual("EWY", registry[identifier]["ticker"])
                self.assertEqual(
                    "ETF",
                    registry[identifier]["security_kind"],
                )
                self.assertEqual(
                    expected_name,
                    registry[identifier]["product_name"],
                )
                self.assertEqual(
                    "sec_fund_series",
                    registry[identifier]["product_name_source"],
                )

    def test_sec_fund_name_refresh_joins_dynamic_fields_once_per_cik(
        self,
    ) -> None:
        ticker_payload = {
            "fields": ["symbol", "classId", "cik", "seriesId"],
            "data": [
                ["IBDR", "C000173141", 1100663, "S000055059"],
                ["IBMO", "C000204676", 1100663, "S000063115"],
            ],
        }
        page = """
        <table>
          <tr><td colspan="3"><b>CIK</b></td><td></td><td></td></tr>
          <tr><td></td><td colspan="2"><b>Series</b></td>
            <td></td><td><b>Ticker</b></td></tr>
          <tr><td></td><td></td><td><b>Class/Contract</b></td>
            <td><b>Name</b></td><td><b>Symbol</b></td></tr>
          <tr><td><td colspan="2"><a href="?CIK=S000055059">
            S000055059</a></td><td><a>
            iShares iBonds Dec 2026 Term Corporate ETF
          </a></td><td></td></tr>
          <tr><td><td><td><a href="?CIK=C000173141">
            C000173141</a></td><td>
            iShares iBonds Dec 2026 Term Corporate ETF
          </td><td>IBDR</td></tr>
          <tr><td><td colspan="2"><a href="?CIK=S000063115">
            S000063115</a></td><td><a>
            iShares iBonds Dec 2026 Term Muni Bond ETF
          </a></td><td></td></tr>
          <tr><td><td><td><a href="?CIK=C000204676">
            C000204676</a></td><td>
            iShares iBonds Dec 2026 Term Muni Bond ETF
          </td><td>IBMO</td></tr>
        </table>
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_path = Path(tmpdir) / "sec-fund-names.json"
            response = mock.Mock(text=page)
            with (
                mock.patch.object(
                    pipeline,
                    "SEC_FUND_NAMES_PATH",
                    cache_path,
                ),
                mock.patch.object(
                    pipeline,
                    "_load_sec_fund_tickers_data",
                    return_value=ticker_payload,
                ),
                mock.patch.object(
                    pipeline.HTTP,
                    "get",
                    return_value=response,
                ) as get_mock,
            ):
                names = pipeline.refresh_sec_fund_names({
                    "ibdr",
                    "IBMO",
                    "",
                })
                cached_names = pipeline.refresh_sec_fund_names({
                    "IBDR",
                    "IBMO",
                })

            self.assertEqual(names, cached_names)
            self.assertEqual(
                "iShares iBonds Dec 2026 Term Corporate ETF",
                names["IBDR"],
            )
            self.assertEqual(
                "iShares iBonds Dec 2026 Term Muni Bond ETF",
                names["IBMO"],
            )
            self.assertEqual(2, get_mock.call_count)
            self.assertTrue(cache_path.exists())

    def test_sec_fund_name_refresh_revalidates_cached_identity_tuple(
        self,
    ) -> None:
        ticker_payload = {
            "fields": ["cik", "seriesId", "classId", "symbol"],
            "data": [
                [1100663, "S000055059", "C000173141", "IBDR"],
            ],
        }
        page = """
        <table>
          <tr><td colspan="3"><b>CIK</b></td><td></td><td></td></tr>
          <tr><td></td><td colspan="2"><b>Series</b></td>
            <td></td><td><b>Ticker</b></td></tr>
          <tr><td></td><td></td><td><b>Class/Contract</b></td>
            <td><b>Name</b></td><td><b>Symbol</b></td></tr>
          <tr><td><td colspan="2"><a href="?CIK=S000055059">
            S000055059</a></td><td><a>
            iShares iBonds Dec 2026 Term Corporate ETF
          </a></td><td></td></tr>
          <tr><td><td><td><a href="?CIK=C000173141">
            C000173141</a></td><td>
            iShares iBonds Dec 2026 Term Corporate ETF
          </td><td>IBDR</td></tr>
        </table>
        """
        stale_cache = {
            "IBDR": {
                "cik": "0000000001",
                "series_id": "S000000001",
                "class_id": "C000000001",
                "name": "Stale Reused Symbol Fund",
            },
            "GONE": {
                "cik": "0000000002",
                "series_id": "S000000002",
                "class_id": "C000000002",
                "name": "Liquidated Fund",
            },
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_path = Path(tmpdir) / "sec-fund-names.json"
            cache_path.write_text(json.dumps(stale_cache))
            response = mock.Mock(text=page)
            with (
                mock.patch.object(
                    pipeline,
                    "SEC_FUND_NAMES_PATH",
                    cache_path,
                ),
                mock.patch.object(
                    pipeline,
                    "_load_sec_fund_tickers_data",
                    return_value=ticker_payload,
                ),
                mock.patch.object(
                    pipeline.HTTP,
                    "get",
                    return_value=response,
                ) as get_mock,
            ):
                names = pipeline.refresh_sec_fund_names({
                    "IBDR",
                    "GONE",
                })

            self.assertEqual(
                "iShares iBonds Dec 2026 Term Corporate ETF",
                names["IBDR"],
            )
            self.assertNotIn("GONE", names)
            self.assertEqual(1, get_mock.call_count)
            cache = json.loads(cache_path.read_text())
            self.assertEqual("0001100663", cache["IBDR"]["cik"])
            self.assertNotIn("GONE", cache)

    def test_sec_fund_name_refresh_observes_same_series_rename(
        self,
    ) -> None:
        ticker_payload = {
            "fields": ["cik", "seriesId", "classId", "symbol"],
            "data": [
                [1100663, "S000055059", "C000173141", "IBDR"],
            ],
        }
        renamed_page = """
        <table>
          <tr><td colspan="3"><b>CIK</b></td><td></td><td></td></tr>
          <tr><td></td><td colspan="2"><b>Series</b></td>
            <td></td><td><b>Ticker</b></td></tr>
          <tr><td></td><td></td><td><b>Class/Contract</b></td>
            <td><b>Name</b></td><td><b>Symbol</b></td></tr>
          <tr><td><td colspan="2"><a href="?CIK=S000055059">
            S000055059</a></td><td><a>
            iShares iBonds Dec 2026 Corporate ETF
          </a></td><td></td></tr>
          <tr><td><td><td><a href="?CIK=C000173141">
            C000173141</a></td><td>
            iShares iBonds Dec 2026 Corporate ETF
          </td><td>IBDR</td></tr>
        </table>
        """
        cached_entry = {
            "IBDR": {
                "cik": "0001100663",
                "series_id": "S000055059",
                "class_id": "C000173141",
                "name": "Old Fund Name",
            },
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_path = Path(tmpdir) / "sec-fund-names.json"
            cache_path.write_text(json.dumps(cached_entry))
            with (
                mock.patch.object(
                    pipeline,
                    "SEC_FUND_NAMES_PATH",
                    cache_path,
                ),
                mock.patch.object(
                    pipeline,
                    "_load_sec_fund_tickers_data",
                    return_value=ticker_payload,
                ),
                mock.patch.object(
                    pipeline.HTTP,
                    "get",
                    return_value=mock.Mock(text=renamed_page),
                ) as get_mock,
            ):
                names = pipeline.refresh_sec_fund_names({"IBDR"})

            self.assertEqual(
                "iShares iBonds Dec 2026 Corporate ETF",
                names["IBDR"],
            )
            self.assertEqual(1, get_mock.call_count)
            cache = json.loads(cache_path.read_text())
            self.assertEqual(
                "iShares iBonds Dec 2026 Corporate ETF",
                cache["IBDR"]["name"],
            )

    def test_duplicate_fund_names_prefer_sec_then_ticker_fallback(
        self,
    ) -> None:
        registry = {
            "46435GAA0": {
                "ticker": None,
                "security_label": "IBDR",
                "name": "ISHARES TR",
                "dominant_issuer": "ISHARES TR",
                "dominant_class": "IBONDS DEC2026",
                "security_kind": "ETF",
                "sources": ["filer_dominant"],
            },
            "46435U259": {
                "ticker": None,
                "security_label": "IBMO",
                "name": "ISHARES TR",
                "dominant_issuer": "ISHARES TR",
                "dominant_class": "IBONDS DEC 26",
                "security_kind": "ETF",
                "sources": ["filer_dominant"],
            },
            "45782C318": {
                "ticker": "PMAY",
                "security_label": "PMAY",
                "name": "INNOVATOR ETFS TRUST",
                "dominant_issuer": "INNOVATOR ETFS TRUST",
                "dominant_class": "US EQTY PWR BUF",
                "security_kind": "ETF",
                "sources": ["filer_dominant"],
            },
            "45782C383": {
                "ticker": "PMAR",
                "security_label": "PMAR",
                "name": "INNOVATOR ETFS TRUST",
                "dominant_issuer": "INNOVATOR ETFS TRUST",
                "dominant_class": "US EQTY PWR BUF",
                "security_kind": "ETF",
                "sources": ["filer_dominant"],
            },
        }
        details = {
            "46435GAA0": {
                "status": "matched",
                "ticker": "IBDR",
                "securityDescription": "IBDR",
                "name": "ISHARES IBONDS DEC 2026 TERM",
            },
            "46435U259": {
                "status": "matched",
                "ticker": "IBMO",
                "securityDescription": "IBMO",
                "name": "ISHARES IBONDS DEC 2026 TERM",
            },
            "45782C318": {
                "status": "matched",
                "ticker": "PMAY",
                "securityDescription": "PMAY",
                "name": "INNOVATOR U.S. EQUITY POWER",
            },
            "45782C383": {
                "status": "matched",
                "ticker": "PMAR",
                "securityDescription": "PMAR",
                "name": "INNOVATOR U.S. EQUITY POWER",
            },
        }
        pipeline._apply_registry_fund_product_names(
            registry,
            openfigi_details=details,
            prior_registry={},
            sec_fund_names={
                "IBDR": (
                    "iShares iBonds Dec 2026 Term Corporate ETF"
                ),
                "IBMO": (
                    "iShares iBonds Dec 2026 Term Muni Bond ETF"
                ),
            },
        )
        self.assertEqual(
            "iShares iBonds Dec 2026 Term Corporate ETF",
            registry["46435GAA0"]["product_name"],
        )
        self.assertEqual(
            "iShares iBonds Dec 2026 Term Muni Bond ETF",
            registry["46435U259"]["product_name"],
        )
        self.assertEqual(
            "sec_fund_series",
            registry["46435GAA0"]["product_name_source"],
        )
        self.assertEqual(
            "INNOVATOR U.S. EQUITY POWER — PMAY",
            registry["45782C318"]["product_name"],
        )
        self.assertEqual(
            "openfigi_ticker",
            registry["45782C318"]["product_name_source"],
        )
        duplicate_name = "Innovator U.S. Equity Power Buffer ETF"
        pipeline._apply_registry_fund_product_names(
            registry,
            openfigi_details=details,
            prior_registry={},
            sec_fund_names={
                "PMAY": duplicate_name,
                "PMAR": duplicate_name,
            },
        )
        for identifier, symbol in (
            ("45782C318", "PMAY"),
            ("45782C383", "PMAR"),
        ):
            self.assertEqual(
                f"{duplicate_name} — {symbol}",
                registry[identifier]["product_name"],
            )
            self.assertEqual(
                "sec_fund_series_ticker",
                registry[identifier]["product_name_source"],
            )

    def test_incomplete_fund_names_request_official_sec_names(
        self,
    ) -> None:
        registry = {
            "464287804": {
                "ticker": "IJR",
                "security_label": "IJR",
                "name": "ISHARES TR",
                "dominant_issuer": "ISHARES TR",
                "dominant_class": "CORE S&P SMALL-CAP",
                "security_kind": "ETF",
                "last_seen": "2026-06-30",
                "sources": ["filer_dominant"],
            },
            "78464A409": {
                "ticker": "SPIB",
                "security_label": "SPIB",
                "name": "SPDR SERIES TRUST",
                "dominant_issuer": "SPDR SERIES TRUST",
                "dominant_class": "SHARES",
                "security_kind": "ETF",
                "last_seen": "2026-06-30",
                "sources": ["filer_dominant"],
            },
            "46641Q332": {
                "ticker": "JEPI",
                "security_label": "JEPI",
                "name": "JPMORGAN ETF TRUST",
                "dominant_issuer": "JPMORGAN ETF TRUST",
                "dominant_class": "EQUITY PREMIUM",
                "security_kind": "ETF",
                "last_seen": "2025-12-31",
                "sources": ["filer_dominant"],
            },
        }
        details = {
            "464287804": {
                "status": "matched",
                "ticker": "IJR",
                "securityDescription": "IJR",
                "name": "ISHARES CORE S&P SMALL-CAP E",
            },
            "78464A409": {
                "status": "matched",
                "ticker": "SPIB",
                "securityDescription": "SPIB",
                "name": "SPDR SERIES TRUST",
            },
            "46641Q332": {
                "status": "matched",
                "ticker": "JEPI",
                "securityDescription": "JEPI",
                "name": "JPMORGAN EQUITY PREMIUM INCO",
            },
        }
        requested_symbols: set[str] = set()

        self.assertEqual(
            3,
            pipeline._apply_registry_fund_product_names(
                registry,
                openfigi_details=details,
                prior_registry={},
                sec_fund_names={
                    "IJR": "iShares Core S&P Small-Cap ETF",
                    "SPIB": (
                        "SPDR Portfolio Intermediate Term Corporate Bond ETF"
                    ),
                    "JEPI": "JPMorgan Equity Premium Income ETF",
                },
                ambiguous_symbols=requested_symbols,
            ),
        )

        self.assertEqual({"IJR", "JEPI", "SPIB"}, requested_symbols)
        self.assertEqual(
            "iShares Core S&P Small-Cap ETF",
            registry["464287804"]["product_name"],
        )
        self.assertEqual(
            "SPDR Portfolio Intermediate Term Corporate Bond ETF",
            registry["78464A409"]["product_name"],
        )
        self.assertEqual(
            "JPMorgan Equity Premium Income ETF",
            registry["46641Q332"]["product_name"],
        )
        self.assertEqual(
            "sec_fund_series",
            registry["464287804"]["product_name_source"],
        )

    def test_etn_uses_descriptive_vendor_name_without_sec_fund_lookup(
        self,
    ) -> None:
        registry = {
            "06747C322": {
                "ticker": "GRN",
                "security_label": "GRN",
                "name": "BARCLAYS BANK PLC",
                "dominant_issuer": "BARCLAYS BANK PLC",
                "dominant_class": "ETN",
                "security_kind": "ETN",
                "last_seen": "2026-06-30",
                "sources": ["filer_dominant"],
                "type": "NOTE",
            },
            "22542D449": {
                "ticker": None,
                "security_label": "UBS 2033 SILVER COVERED CALL ETN",
                "name": (
                    "UBS AG ETRACS SILVER SHARES COVERED CALL ETNS "
                    "DUE APRIL 21, 2033"
                ),
                "dominant_issuer": (
                    "UBS AG ETRACS SILVER SHARES COVERED CALL ETNS "
                    "DUE APRIL 21, 2033"
                ),
                "dominant_class": "ETN",
                "security_kind": "ETN",
                "last_seen": "2026-06-30",
                "sources": ["filer_dominant"],
                "type": "NOTE",
            },
        }
        requested_symbols: set[str] = set()

        self.assertEqual(
            2,
            pipeline._apply_registry_fund_product_names(
                registry,
                openfigi_details={
                    "06747C322": {
                        "status": "matched",
                        "ticker": "GRN",
                        "securityDescription": "GRN",
                        "name": "IPATH SERIES B CARBON ETN",
                        "securityType": "ETP",
                        "securityType2": "Mutual Fund",
                        "marketSector": "Equity",
                    },
                },
                prior_registry={},
                ambiguous_symbols=requested_symbols,
            ),
        )

        self.assertEqual(set(), requested_symbols)
        self.assertEqual(
            "IPATH SERIES B CARBON ETN",
            registry["06747C322"]["product_name"],
        )
        self.assertEqual(
            "openfigi",
            registry["06747C322"]["product_name_source"],
        )
        self.assertEqual(
            (
                "UBS AG ETRACS SILVER SHARES COVERED CALL ETNS "
                "DUE APRIL 21, 2033"
            ),
            registry["22542D449"]["product_name"],
        )
        self.assertEqual(
            "filer_issuer",
            registry["22542D449"]["product_name_source"],
        )

    def test_duplicate_openfigi_fund_names_restore_filer_class_detail(
        self,
    ) -> None:
        registry = {
            "45782C763": {
                "ticker": "BJAN",
                "name": "INNOVATOR ETFS TRUST",
                "dominant_issuer": "INNOVATOR ETFS TRUST",
                "dominant_class": "US EQUITY BUFFER ETF JAN",
                "security_kind": "ETF",
                "sources": ["filer_dominant"],
            },
            "45782C771": {
                "ticker": "BFEB",
                "name": "INNOVATOR ETFS TRUST",
                "dominant_issuer": "INNOVATOR ETFS TRUST",
                "dominant_class": "US EQUITY BUFFER ETF FEB",
                "security_kind": "ETF",
                "sources": ["filer_dominant"],
            },
        }
        details = {
            identifier: {
                "status": "matched",
                "ticker": ticker,
                "securityDescription": ticker,
                "name": "INNOVATOR U.S. EQUITY BUFFER",
            }
            for identifier, ticker in (
                ("45782C763", "BJAN"),
                ("45782C771", "BFEB"),
            )
        }

        ambiguous_symbols: set[str] = set()
        self.assertEqual(
            2,
            pipeline._apply_registry_fund_product_names(
                registry,
                openfigi_details=details,
                prior_registry={},
                ambiguous_symbols=ambiguous_symbols,
            ),
        )
        self.assertEqual({"BJAN", "BFEB"}, ambiguous_symbols)
        self.assertEqual(
            "INNOVATOR U.S. EQUITY BUFFER — US EQUITY BUFFER ETF JAN",
            registry["45782C763"]["product_name"],
        )
        self.assertEqual(
            "INNOVATOR U.S. EQUITY BUFFER — US EQUITY BUFFER ETF FEB",
            registry["45782C771"]["product_name"],
        )
        self.assertEqual(
            "openfigi_class",
            registry["45782C763"]["product_name_source"],
        )
        self.assertEqual(
            "openfigi_class",
            registry["45782C771"]["product_name_source"],
        )
        prior_registry = json.loads(json.dumps(registry))
        for entry in prior_registry.values():
            entry["product_name_source"] = "openfigi_prior_registry"
        warm_ambiguous_symbols: set[str] = set()
        pipeline._apply_registry_fund_product_names(
            registry,
            openfigi_details=details,
            prior_registry=prior_registry,
            ambiguous_symbols=warm_ambiguous_symbols,
        )
        self.assertEqual(
            {"BJAN", "BFEB"},
            warm_ambiguous_symbols,
        )

    def test_duplicate_openfigi_names_ignore_shared_class_boilerplate(
        self,
    ) -> None:
        registry = {
            identifier: {
                "name": "GOLDMAN SACHS ETF TRUST",
                "dominant_issuer": "GOLDMAN SACHS ETF TRUST",
                "dominant_class": "CMN",
                "security_kind": "ETF",
                "sources": ["filer_dominant"],
            }
            for identifier in ("38149W101", "38149W119")
        }
        details = {
            identifier: {
                "status": "matched",
                "ticker": ticker,
                "securityDescription": ticker,
                "name": "GOLDMAN SACHS MARKETBETA US",
            }
            for identifier, ticker in (
                ("38149W101", "GSUS"),
                ("38149W119", "GSUS.P"),
            )
        }

        pipeline._apply_registry_fund_product_names(
            registry,
            openfigi_details=details,
            prior_registry={},
        )
        for entry in registry.values():
            self.assertEqual(
                "GOLDMAN SACHS MARKETBETA US",
                entry["product_name"],
            )
            self.assertEqual("openfigi", entry["product_name_source"])

    def test_duplicate_openfigi_names_do_not_split_same_ticker_aliases(
        self,
    ) -> None:
        registry = {
            "833445109": {
                "ticker": "GLDM",
                "name": "WORLD GOLD TRUST",
                "dominant_issuer": "WORLD GOLD TRUST",
                "dominant_class": "SPDR GOLD MINISHARES",
                "security_kind": "ETF",
                "sources": ["filer_dominant"],
            },
            "833445901": {
                "ticker": "GLDM",
                "name": "WORLD GOLD TRUST",
                "dominant_issuer": "WORLD GOLD TRUST",
                "dominant_class": "SPRD GLD MINIS",
                "security_kind": "ETF",
                "sources": ["filer_dominant"],
            },
        }
        details = {
            identifier: {
                "status": "matched",
                "ticker": "GLDM",
                "securityDescription": "GLDM",
                "name": "SPDR GOLD MINISHARES TRUST",
            }
            for identifier in registry
        }

        pipeline._apply_registry_fund_product_names(
            registry,
            openfigi_details=details,
            prior_registry={},
        )
        for entry in registry.values():
            self.assertEqual(
                "SPDR GOLD MINISHARES TRUST",
                entry["product_name"],
            )
            self.assertEqual("openfigi", entry["product_name_source"])

    def test_duplicate_openfigi_name_can_add_one_sided_class_detail(
        self,
    ) -> None:
        registry = {
            "78463X772": {
                "ticker": "DWX",
                "security_label": "DWX",
                "name": "SPDR INDEX SHS FDS",
                "dominant_issuer": "SPDR INDEX SHS FDS",
                "dominant_class": "S&P INTL ETF",
                "security_kind": "ETF",
                "sources": ["filer_dominant"],
            },
            "78463X871": {
                "ticker": "GWX",
                "security_label": "GWX",
                "name": "SPDR INDEX SHS FDS",
                "dominant_issuer": "SPDR INDEX SHS FDS",
                "dominant_class": "S&P INTL SMLCP",
                "security_kind": "ETF",
                "sources": ["filer_dominant"],
            },
        }
        details = {
            identifier: {
                "status": "matched",
                "ticker": ticker,
                "securityDescription": ticker,
                "name": "STATE STREET SPDR S&P INTERN",
            }
            for identifier, ticker in (
                ("78463X772", "DWX"),
                ("78463X871", "GWX"),
            )
        }

        pipeline._apply_registry_fund_product_names(
            registry,
            openfigi_details=details,
            prior_registry={},
        )
        self.assertEqual(
            "STATE STREET SPDR S&P INTERN",
            registry["78463X772"]["product_name"],
        )
        self.assertEqual(
            "STATE STREET SPDR S&P INTERN — S&P INTL SMLCP",
            registry["78463X871"]["product_name"],
        )
        self.assertEqual(
            "openfigi_class",
            registry["78463X871"]["product_name_source"],
        )

    def test_openfigi_kind_maps_closed_end_and_debt_abbreviations(self) -> None:
        cases = (
            (
                {
                    "status": "matched",
                    "securityType": "Closed-End Fund",
                    "securityType2": "Mutual Fund",
                    "marketSector": "Equity",
                },
                "CLOSED-END FUND",
            ),
            (
                {
                    "status": "matched",
                    "securityType": "ABS",
                    "securityType2": "CMBS",
                    "marketSector": "Mtge",
                },
                "BOND",
            ),
            (
                {
                    "status": "matched",
                    "securityType": "Whole Loan",
                    "securityType2": "LL",
                },
                "BOND",
            ),
            (
                {
                    "status": "matched",
                    "name": "UBS ETRACS ETN",
                    "securityType": "ETP",
                    "securityType2": "Note",
                    "marketSector": "Corp",
                },
                "ETN",
            ),
            (
                {
                    "status": "matched",
                    "ticker": "ETN",
                    "name": "EATON CORP PLC",
                    "securityDescription": "ETN 4.15 11/02/42",
                    "securityType": "GLOBAL",
                    "securityType2": "Corp",
                    "marketSector": "Corp",
                },
                "BOND",
            ),
            (
                {
                    "status": "matched",
                    "securityType": "Preference",
                    "securityType2": "Preference",
                    "marketSector": "Equity",
                },
                "PREFERRED",
            ),
            (
                {
                    "status": "matched",
                    "securityType": "Right",
                    "securityType2": "Right",
                    "marketSector": "Equity",
                },
                "RIGHT",
            ),
            (
                {
                    "status": "matched",
                    "securityType": "Unit",
                    "securityType2": "Unit",
                    "marketSector": "Equity",
                },
                "UNIT",
            ),
            (
                {
                    "status": "matched",
                    "securityType": "Equity",
                    "securityType2": "Equity",
                    "marketSector": "Equity",
                },
                None,
            ),
        )
        for detail, expected in cases:
            with self.subTest(detail=detail):
                self.assertEqual(
                    expected,
                    pipeline._openfigi_security_kind(detail),
                )

        for identifier in ("06738C786", "06740C527", "06748M188"):
            with self.subTest(identifier=identifier):
                self.assertEqual(
                    ("ETN", "manual_verified"),
                    pipeline._registry_security_kind(
                        identifier=identifier,
                        openfigi_detail={
                            "status": "matched",
                            "name": "IPATH SERIES B",
                            "securityType": "ETP",
                            "securityType2": "Mutual Fund",
                            "marketSector": "Equity",
                        },
                        prior_entry=None,
                        entry={
                            "name": "BARCLAYS BANK PLC",
                            "dominant_class": "FUND",
                            "type": "EQUITY",
                        },
                    ),
                )

    def test_common_inference_does_not_scan_ordinary_issuer_words(
        self,
    ) -> None:
        cases = (
            ("ZIONS BANCORPORATION NA /UT/", "COM", "ZION"),
            ("OPTION CARE HEALTH INC", "COM NEW", "OPCH"),
            ("CVR ENERGY INC", "COM", "CVI"),
            ("PREFERRED BANK", "COM NEW", "PFBC"),
            ("OUR BOND INC", "COM SHS", "OBAI"),
            ("FATPIPE INC/UT", "COM", "FATN"),
        )
        for issuer, dominant_class, ticker in cases:
            with self.subTest(ticker=ticker):
                self.assertEqual(
                    "COMMON",
                    pipeline._filer_security_kind({
                        "name": issuer,
                        "dominant_issuer": issuer,
                        "dominant_class": dominant_class,
                        "type": "EQUITY",
                        "ticker": ticker,
                        "sources": ["sec_title", "cusip_map_vetted"],
                    }),
                )

    def test_common_inference_keeps_true_issuer_form_exclusions(
        self,
    ) -> None:
        for issuer in (
            "GENERIC ISSUER SPONSORED ADR",
            "GENERIC HOLDINGS LP",
            "GENERIC MUNICIPAL INCOME FUND",
            "GENERIC ISSUER WARRANT",
        ):
            with self.subTest(issuer=issuer):
                self.assertIsNone(
                    pipeline._filer_security_kind({
                        "name": issuer,
                        "dominant_issuer": issuer,
                        "dominant_class": "COM",
                        "type": "EQUITY",
                        "ticker": "GEN",
                        "sources": ["sec_title", "cusip_map_vetted"],
                    })
                )

    def test_filer_kind_fallback_is_explicit_and_fund_first(self) -> None:
        cases = (
            (
                {
                    "name": "SPDR S&P 500 ETF TRUST",
                    "dominant_class": "TR UNIT",
                    "type": "EQUITY",
                },
                "ETF",
            ),
            (
                {
                    "name": "ISHARES TR",
                    "dominant_class": "CORE S&P500 ETF",
                    "type": "EQUITY",
                },
                "ETF",
            ),
            (
                {
                    "name": "ISHARES TR",
                    "dominant_class": "CORE US AGGBD ET",
                    "type": "EQUITY",
                    "ticker": "AGG",
                },
                "ETF",
            ),
            (
                {
                    "name": "SELECT SECTOR SPDR TR",
                    "dominant_class": "STATE STREET TEC",
                    "type": "EQUITY",
                    "ticker": "XLK",
                },
                "ETF",
            ),
            (
                {
                    "name": "ETFIS SER TR I",
                    "dominant_class": "VIRTUS INFRCAP",
                    "type": "PREF",
                    "ticker": "PFFA",
                },
                "ETF",
            ),
            (
                {
                    "name": "JANUS DETROIT STR TR",
                    "dominant_class": "HENDERSON MTG",
                    "type": "EQUITY",
                    "ticker": "JMBS",
                },
                "ETF",
            ),
            (
                {
                    "name": "RBB FD INC",
                    "dominant_class": "US TREAS 3 MNTH",
                    "type": "EQUITY",
                    "ticker": "TBIL",
                    "sources": ["cusip_map_vetted"],
                },
                "ETF",
            ),
            (
                {
                    "name": "SCHWAB STRATEGIC TR",
                    "dominant_class": "SHT TM US TRES",
                    "type": "EQUITY",
                    "ticker": "SCHO",
                    "sources": ["cusip_map_vetted"],
                },
                "ETF",
            ),
            (
                {
                    "name": "RBB FD INC",
                    "dominant_class": "US TREAS 3 MNTH",
                    "type": "EQUITY",
                    "ticker": "TBIL",
                    "sources": [],
                },
                None,
            ),
            (
                {
                    "name": "RBB FD INC",
                    "dominant_class": "US TREAS 3 MNTH",
                    "type": "EQUITY",
                    "ticker": "TBIL",
                    "sources": [
                        "cusip_map_vetted",
                        "ticker_collision_demoted",
                    ],
                },
                None,
            ),
            (
                {
                    "name": "SCHWAB STRATEGIC TR",
                    "dominant_class": "FD",
                    "type": "EQUITY",
                    "ticker": "SWRSX",
                    "sources": ["cusip_map_vetted"],
                },
                None,
            ),
            (
                {
                    "name": "BLACKROCK HIGH YIELD INSTL",
                    "dominant_class": "MFF",
                    "type": "EQUITY",
                    "ticker": "BHYIX",
                },
                None,
            ),
            (
                {
                    "name": "EATON VANCE FLOATING RATE I",
                    "dominant_class": "EIBLX",
                    "type": "EQUITY",
                    "ticker": "EIBLX",
                },
                None,
            ),
            (
                {
                    "name": "ISHARES TR",
                    "dominant_class": "PFD AND INCM SEC",
                    "type": "PREF",
                },
                "ETF",
            ),
            (
                {
                    "name": "INVESCO QQQ TRUST, SERIES 1",
                    "dominant_class": "UNIT SER 1",
                    "type": "PUT",
                },
                "ETF",
            ),
            (
                {
                    "name": "SPDR GOLD TRUST",
                    "dominant_class": "GOLD SHS",
                    "type": "CALL",
                },
                "ETF",
            ),
            (
                {
                    "name": "ISHARES SILVER TRUST",
                    "dominant_class": "ISHARES",
                    "type": "CALL",
                },
                "ETF",
            ),
            (
                {
                    "name": "VANGUARD TOTAL BOND MARKET INDEX INV",
                    "dominant_class": "MUTUAL FUND",
                    "type": "EQUITY",
                },
                "MUTUAL FUND",
            ),
            (
                {
                    "name": "JPMORGAN EXCH TRADED F",
                    "dominant_class": "EQUITY PREMIUM INCOME",
                    "type": "EQUITY",
                },
                "ETF",
            ),
            (
                {
                    "name": "UBS AG ETRACS ETNS ETP 2022-2",
                    "dominant_class": "ETF",
                    "type": "EQUITY",
                },
                "ETN",
            ),
            (
                {
                    "name": "GENERIC CASH VEHICLE",
                    "dominant_class": "NON-SWEEP MMF",
                    "type": "EQUITY",
                },
                "MUTUAL FUND",
            ),
            (
                {
                    "name": "PERSHING SQUARE SPARC HOLDINGS LTD",
                    "dominant_class": "RIGHT",
                    "type": "EQUITY",
                },
                "RIGHT",
            ),
            (
                {
                    "name": "ASTERA LABS INC",
                    "dominant_class": "COM",
                    "type": "EQUITY",
                    "ticker": "ALAB",
                    "sources": ["sec_title", "cusip_map_vetted"],
                },
                "COMMON",
            ),
            (
                {
                    "name": "CIENA CORP",
                    "dominant_class": "COM NEW",
                    "type": "EQUITY",
                    "ticker": "CIEN",
                    "sources": ["sec_title", "cusip_map_vetted"],
                },
                "COMMON",
            ),
            (
                {
                    "name": "ALPHABET INC",
                    "dominant_class": "CAP STK CL A",
                    "type": "EQUITY",
                    "ticker": "GOOGL",
                    "sources": ["sec_title", "cusip_map_vetted"],
                },
                "COMMON",
            ),
            (
                {
                    "name": "TWILIO INC",
                    "dominant_class": "CL A",
                    "type": "EQUITY",
                    "ticker": "TWLO",
                    "sources": ["sec_title", "cusip_map_vetted"],
                },
                "COMMON",
            ),
            (
                {
                    "name": "GENERIC FOREIGN ISSUER",
                    "dominant_class": "ORDINARY SHARES",
                    "type": "EQUITY",
                    "ticker": "GFI",
                    "sources": ["sec_title", "cusip_map_vetted"],
                },
                "COMMON",
            ),
            (
                {
                    "name": "GENERIC ISSUER",
                    "dominant_class": "COM",
                    "type": "CALL",
                    "ticker": "GEN",
                    "sources": ["sec_title", "cusip_map_vetted"],
                },
                None,
            ),
            (
                {
                    "name": "ISHARES TREASURY SERVICES INC",
                    "dominant_class": "COM",
                    "type": "EQUITY",
                    "ticker": "ITS",
                },
                None,
            ),
            (
                {
                    "name": "ISHARES TRT HOLDINGS",
                    "dominant_class": "COM",
                    "type": "EQUITY",
                    "ticker": "ITRT",
                },
                None,
            ),
            (
                {
                    "name": "PEBBLEBROOK HOTEL TRUST",
                    "dominant_class": "6.375 PFD SER F",
                    "type": "PREF",
                },
                "PREFERRED",
            ),
            (
                {
                    "name": "GENERIC ISSUER",
                    "dominant_class": "BANK LOAN",
                    "type": "EQUITY",
                    "ticker": "GEN",
                    "sources": ["sec_title", "cusip_map_vetted"],
                },
                None,
            ),
            (
                {
                    "name": "GENERIC FUND OR PRIVATE HOLDING",
                    "dominant_class": "SHS",
                    "type": "EQUITY",
                    "ticker": "GEN",
                    "sources": ["sec_title", "cusip_map_vetted"],
                },
                None,
            ),
            (
                {
                    "name": "PIMCO MUNICIPAL INCOME FUND",
                    "dominant_class": "COM",
                    "type": "EQUITY",
                    "ticker": "PMF",
                    "sources": ["sec_title", "cusip_map_vetted"],
                },
                None,
            ),
            (
                {
                    "name": "GENERIC ISSUER SPONSORED ADR",
                    "dominant_class": "SP ADR REP COM",
                    "type": "EQUITY",
                    "ticker": "GEN",
                    "sources": ["sec_title", "cusip_map_vetted"],
                },
                None,
            ),
            (
                {
                    "name": "GENERIC ISSUER WARRANT",
                    "dominant_class": "COM",
                    "type": "EQUITY",
                    "ticker": "GEN",
                    "sources": ["sec_title", "cusip_map_vetted"],
                },
                None,
            ),
            (
                {
                    "name": (
                        "MERRILL LYNCH DEPOSITOR INC "
                        "INDEXPLUS TRUST SERIES 2003-1"
                    ),
                    "dominant_class": "COMMON STOCK",
                    "type": "EQUITY",
                    "ticker": "IPB",
                    "sources": ["sec_title", "cusip_map_vetted"],
                },
                None,
            ),
            (
                {
                    "name": "GENERIC ISSUER",
                    "dominant_class": "",
                    "type": "EQUITY",
                },
                None,
            ),
        )
        for entry, expected in cases:
            with self.subTest(entry=entry):
                self.assertEqual(
                    expected,
                    pipeline._filer_security_kind(entry),
                )

    def test_fund_kind_enrichment_discovers_plural_fund_metadata(self) -> None:
        for text in (
            "NTF EQUITY FUNDS",
            "AMERICAN FUNDS AMCAP R6",
            "BITWISE FUNDS TRUST ETFS",
            "MODEL PORTFOLIOS",
            "INVESCO QQQ TRUST UNIT SER 1",
        ):
            with self.subTest(text=text):
                self.assertIsNotNone(
                    pipeline._FUND_KIND_DISCOVERY_RE.search(text)
                )

    def test_tickered_plural_fund_is_targeted_for_kind_enrichment(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            funds_dir = Path(tmpdir) / "funds"
            funds_dir.mkdir()
            (funds_dir / "1.json").write_text(json.dumps({
                "cik": 1,
                "quarters": [{
                    "report_date": "2026-03-31",
                    "holdings": [{
                        "cusip": "023375819",
                        "ticker": "RAFGX",
                        "issuer": "AMERICAN FUNDS AMCAP R6",
                        "class": "NTF EQUITY FUNDS",
                        "holding_type": "EQUITY",
                        "value": 100,
                        "shares": 1,
                    }],
                }],
            }))
            prior_registry = {
                "023375819": {
                    "ticker": "RAFGX",
                    "name": "AMERICAN FUNDS AMCAP R6",
                    "dominant_issuer": "AMERICAN FUNDS AMCAP R6",
                    "dominant_class": "NTF EQUITY FUNDS",
                    "type": "EQUITY",
                    "sources": ["cusip_map_vetted"],
                }
            }
            resolver = mock.Mock(return_value={})
            with mock.patch.multiple(
                pipeline,
                FUNDS_DIR=funds_dir,
                load_cusip_map=mock.Mock(
                    return_value={"023375819": "RAFGX"}
                ),
                load_cusip_registry=mock.Mock(
                    return_value=prior_registry
                ),
                load_openfigi_details=mock.Mock(return_value={}),
                resolve_cusips_via_openfigi=resolver,
                save_cusip_map=mock.Mock(),
            ):
                pipeline.rebuild_tickers_in_place(
                    company_ticker_data=[]
                )

            resolver.assert_called_once()
            self.assertIn("023375819", resolver.call_args.args[0])

    def test_registry_ticker_dedup_prefers_current_cusip(self) -> None:
        registry = {
            "00258Y104": {
                "ticker": "ABX",
                "name": "Abacus Global Management, Inc.",
                "dominant_issuer": "ABACUS GLOBAL MANAGEMENT, INC.",
                "type": "EQUITY",
                "first_seen": "2024-12-31",
                "last_seen": "2026-06-30",
                "holder_count": 183,
                "total_value": 785_867_055,
                "sources": ["sec_title", "openfigi_plain_ticker"],
            },
            "067901108": {
                "ticker": "ABX",
                "name": "Abacus Global Management, Inc.",
                "dominant_issuer": "BARRICK GOLD CORP",
                "type": "EQUITY",
                "first_seen": "2017-09-30",
                "last_seen": "2026-06-30",
                "holder_count": 20,
                "total_value": 209_489_088,
                "sources": ["sec_title", "openfigi_plain_ticker"],
            },
        }
        self.assertEqual(
            1,
            pipeline._deduplicate_registry_equity_tickers(registry),
        )
        self.assertEqual("ABX", registry["00258Y104"]["ticker"])
        self.assertIsNone(registry["067901108"]["ticker"])
        self.assertEqual(
            "BARRICK GOLD CORP",
            registry["067901108"]["name"],
        )
        self.assertIn(
            "ticker_collision_demoted",
            registry["067901108"]["sources"],
        )
        label, source = pipeline._registry_security_label(
            identifier="067901108",
            entry=registry["067901108"],
            openfigi_detail={
                "status": "matched",
                "ticker": "ABX",
                "name": "BARRICK GOLD CORP",
                "securityDescription": "ABX",
                "marketSector": "Equity",
                "securityType": "Common Stock",
                "securityType2": "Common Stock",
                "exchCode": "IX",
            },
            prior_entry={
                "security_label": "ABX",
                "label_source": "openfigi",
            },
            legacy_openfigi_label=None,
        )
        self.assertEqual("BARRICK GOLD CORP", label)
        self.assertEqual("openfigi_collision_name", source)

    def test_independent_registry_validator_accepts_a_null_note_ticker(
        self,
    ) -> None:
        base = {
            "ticker": None,
            "name": "RIVIAN AUTOMOTIVE INC",
            "dominant_issuer": "RIVIAN AUTOMOTIVE INC",
            "dominant_class": "NOTE 3.625% 10/15/30",
            "security_label": "RIVN 3.625 10/15/30",
            "label_source": "openfigi",
            "type": "NOTE",
            "sources": ["filer_dominant"],
        }

        valid_errors: list[str] = []
        validate_data.validate_registry(
            {"76954AAD5"},
            valid_errors,
            {"76954AAD5": base},
            {},
        )
        self.assertEqual([], valid_errors)

        prudential_note = {
            **base,
            "name": "PRUDENTIAL FINL INC",
            "dominant_issuer": "PRUDENTIAL FINL INC",
            "dominant_class": "COMMON STOCK",
            "security_label": (
                "PFH — 4.125% JUNIOR SUBORDINATED NOTES DUE 2060"
            ),
            "label_source": "manual_verified",
            "security_kind": "BOND",
            "security_kind_source": "manual_verified",
        }
        errors = []
        validate_data.validate_registry(
            {"744320888"},
            errors,
            {"744320888": prudential_note},
            {},
        )
        self.assertEqual([], errors)

        errors = []
        validate_data.validate_registry(
            {"744320888"},
            errors,
            {
                "744320888": {
                    **prudential_note,
                    "type": "EQUITY",
                    "security_kind": "PREFERRED",
                    "security_kind_source": "openfigi",
                },
            },
            {},
        )
        self.assertTrue(
            any("manual security-kind proof" in error for error in errors),
            errors,
        )

        for invalid in (
            {**base, "ticker": "RIVN 3.625 10/15/30"},
            {**base, "ticker": "RIVN"},
            {
                **base,
                "ticker": "RIVN 3.625 10/15/30",
                "type": "EQUITY",
            },
        ):
            with self.subTest(invalid=invalid):
                errors: list[str] = []
                validate_data.validate_registry(
                    {"76954AAD5"},
                    errors,
                    {"76954AAD5": invalid},
                    {},
                )
                self.assertTrue(
                    any("NOTE labels" in error for error in errors),
                    errors,
                )

        for invalid_type in ("EQUITY", "PREF", "CALL", "PUT", "OPT"):
            with self.subTest(invalid_bond_type=invalid_type):
                non_note_bond = {
                    **base,
                    "type": invalid_type,
                    "security_kind": "BOND",
                    "security_kind_source": "openfigi",
                }
                errors = []
                validate_data.validate_registry(
                    {"76954AAD5"},
                    errors,
                    {"76954AAD5": non_note_bond},
                    {},
                )
                self.assertTrue(
                    any(
                        "bonds as non-NOTE instruments" in error
                        for error in errors
                    ),
                    errors,
                )

        for invalid_type in ("NOTE", "PREF", "WARRANT"):
            with self.subTest(invalid_fund_type=invalid_type):
                invalid_fund = {
                    "ticker": "BSV",
                    "name": "VANGUARD SHORT-TERM BOND ETF",
                    "dominant_issuer": "VANGUARD BD INDEX FDS",
                    "dominant_class": "SHORT TRM BOND",
                    "security_label": "BSV",
                    "label_source": "canonical_ticker",
                    "type": invalid_type,
                    "security_kind": "ETF",
                    "security_kind_source": "openfigi",
                    "sources": ["openfigi_plain_ticker"],
                }
                errors = []
                validate_data.validate_registry(
                    {"921937827"},
                    errors,
                    {"921937827": invalid_fund},
                    {},
                )
                self.assertTrue(
                    any(
                        "listed funds as non-EQUITY non-option" in error
                        for error in errors
                    ),
                    errors,
                )

        for identifier, invalid_fund in (
            (
                "464287226",
                {
                    "ticker": "AGG",
                    "security_label": "AGG",
                    "name": "ISHARES TR",
                    "dominant_issuer": "ISHARES TR",
                    "dominant_class": "CORE US AGGBD ET",
                    "type": "EQUITY",
                    "sources": ["cusip_map_vetted"],
                },
            ),
            (
                "74933W452",
                {
                    "ticker": "TBIL",
                    "security_label": "TBIL",
                    "name": "RBB FD INC",
                    "dominant_issuer": "RBB FD INC",
                    "dominant_class": "US TREAS 3 MNTH",
                    "type": "EQUITY",
                    "sources": ["cusip_map_vetted"],
                },
            ),
        ):
            with self.subTest(missing_structural_fund_kind=identifier):
                errors = []
                validate_data.validate_registry(
                    {identifier},
                    errors,
                    {identifier: invalid_fund},
                    {},
                )
                self.assertTrue(
                    any(
                        "misses deterministic filer fund kinds" in error
                        for error in errors
                    ),
                    errors,
                )

        ambiguous_fund = {
            "ticker": "BCOIX",
            "security_label": "BCOIX",
            "name": "BAIRD CORE PLUS BOND INS T",
            "dominant_issuer": "BAIRD CORE PLUS BOND INS T",
            "dominant_class": "BOND",
            "type": "NOTE",
            "sources": ["cusip_map_vetted"],
        }
        errors = []
        validate_data.validate_registry(
            {"057071870"},
            errors,
            {"057071870": ambiguous_fund},
            {},
        )
        self.assertTrue(
            any(
                "listed funds as non-EQUITY non-option" in error
                for error in errors
            ),
            errors,
        )

    def test_retry_accepts_and_normalizes_note_description(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            health_path = Path(tmpdir) / "ticker_health.json"
            health_path.write_text(json.dumps({
                "buckets": {
                    "unresolved": [{
                        "cusip": "90353TAM2",
                        "instrument_type": "NOTE",
                    }],
                },
            }))
            save_map = mock.Mock()
            with mock.patch.multiple(
                pipeline,
                TICKER_HEALTH_PATH=health_path,
                load_cusip_registry=mock.Mock(return_value={
                    "90353TAM2": {"type": "NOTE"},
                }),
                get_openfigi_api_key=mock.Mock(return_value="test-key"),
                resolve_cusips_via_openfigi=mock.Mock(return_value={
                    "90353TAM2": "UBER 0.875 12/01/28 2028",
                }),
                load_cusip_map=mock.Mock(return_value={}),
                save_cusip_map=save_map,
            ):
                self.assertEqual(1, pipeline.retry_unresolved_cusips())

        saved = save_map.call_args.args[0]
        self.assertEqual(
            "UBER 0.875 12/01/28",
            saved["90353TAM2"],
        )

    def test_daily_retry_defers_stable_debt_options_and_stale_rows(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            health_path = Path(tmpdir) / "ticker_health.json"
            health_path.write_text(json.dumps({
                "buckets": {
                    "unresolved": [
                        {
                            "cusip": "111111111",
                            "instrument_type": "EQUITY",
                            "last_seen": "2026-06-30",
                        },
                        {
                            "cusip": "222222222",
                            "instrument_type": "PREF",
                            "last_seen": "2026-03-31",
                        },
                        {
                            "cusip": "333333333",
                            "instrument_type": "WARRANT",
                            "last_seen": "2026-03-31",
                        },
                        {
                            "cusip": "444444444",
                            "instrument_type": "NOTE",
                            "last_seen": "2026-06-30",
                        },
                        {
                            "cusip": "555555555",
                            "instrument_type": "CALL",
                            "last_seen": "2026-06-30",
                        },
                        {
                            "cusip": "666666666",
                            "instrument_type": "EQUITY",
                            "last_seen": "2025-12-31",
                        },
                    ],
                    "suspicious_symbol": [{
                        "cusip": "777777777",
                        "instrument_type": "NOTE",
                        "last_seen": "2025-12-31",
                    }],
                    "option_family_artifact": [{
                        "cusip": "888888988",
                        "instrument_type": "EQUITY",
                        "last_seen": "2025-12-31",
                    }],
                },
            }))
            resolver = mock.Mock(return_value={})
            with mock.patch.multiple(
                pipeline,
                TICKER_HEALTH_PATH=health_path,
                load_cusip_registry=mock.Mock(return_value={}),
                resolve_cusips_via_openfigi=resolver,
                load_cusip_map=mock.Mock(return_value={}),
                save_cusip_map=mock.Mock(),
            ):
                self.assertEqual(0, pipeline.retry_unresolved_cusips())

        resolver.assert_called_once_with([
            "111111111",
            "222222222",
            "333333333",
            "777777777",
            "888888988",
        ])

    def test_retry_quarantines_option_family_common_ticker_collision(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            health_path = Path(tmpdir) / "ticker_health.json"
            health_path.write_text(json.dumps({
                "buckets": {
                    "option_family_artifact": [{
                        "cusip": "123456901",
                        "instrument_type": "EQUITY",
                        "last_seen": "2026-06-30",
                    }],
                },
            }))
            save_map = mock.Mock()
            with mock.patch.multiple(
                pipeline,
                TICKER_HEALTH_PATH=health_path,
                load_cusip_registry=mock.Mock(return_value={
                    "123456101": {
                        "ticker": "EXM",
                        "type": "EQUITY",
                    },
                    "123456901": {
                        "ticker": None,
                        "type": "EQUITY",
                    },
                }),
                resolve_cusips_via_openfigi=mock.Mock(return_value={
                    "123456901": "EXM",
                }),
                load_cusip_map=mock.Mock(return_value={
                    "123456901": "EXM",
                    "999999999": "KEEP",
                }),
                save_cusip_map=save_map,
            ):
                self.assertEqual(1, pipeline.retry_unresolved_cusips())

        save_map.assert_called_once_with({"999999999": "KEEP"})

    def test_note_security_label_stays_out_of_position_tickers(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            funds_dir = root / "funds"
            stocks_dir = root / "stocks"
            funds_dir.mkdir()
            stocks_dir.mkdir()
            fund_path = funds_dir / "123.json"
            fund_path.write_text(json.dumps({
                "cik": 123,
                "name": "TEST FUND",
                "quarters": [{
                    "report_date": "2026-03-31",
                    "total_value": 150,
                    "holdings": [
                        {
                            "cusip": "76954AAD5",
                            "ticker": None,
                            "issuer": "RIVIAN AUTOMOTIVE INC",
                            "class": "NOTE 3.625% 10/15/30",
                            "holding_type": "NOTE",
                            "value": 100,
                            "shares": 10,
                        },
                        {
                            "cusip": "76954AAD5",
                            "ticker": None,
                            "issuer": "RIVIAN AUTOMOTIVE INC",
                            "class": "COM",
                            "holding_type": "EQUITY",
                            "value": 50,
                            "shares": 5,
                        },
                    ],
                }],
            }))
            registry_path = root / "cusip_registry.json"
            registry_path.write_text(json.dumps({
                "76954AAD5": {
                    "ticker": None,
                    "name": "RIVIAN AUTOMOTIVE INC",
                    "dominant_issuer": "RIVIAN AUTOMOTIVE INC",
                    "dominant_class": "NOTE 3.625% 10/15/30",
                    "security_label": "RIVN 3.625 10/15/30",
                    "label_source": "openfigi",
                    "type": "NOTE",
                    "sources": ["filer_dominant"],
                },
            }))

            with mock.patch.multiple(
                pipeline,
                DATA_DIR=root,
                FUNDS_DIR=funds_dir,
                STOCKS_DIR=stocks_dir,
                INDEX_PATH=root / "index.json",
                FUNDS_INDEX_PATH=root / "funds-index.json",
                CUSIP_REGISTRY_PATH=registry_path,
                LEGACY_CUSIP_REGISTRY_PATH=registry_path,
            ):
                pipeline.canonicalize_fund_files()
                pipeline.regenerate_stock_files_and_index(state={})

            holdings = json.loads(fund_path.read_text())["quarters"][0][
                "holdings"
            ]
            by_type = {
                holding["holding_type"]: holding
                for holding in holdings
            }
            self.assertIsNone(by_type["NOTE"]["ticker"])
            self.assertIsNone(by_type["EQUITY"]["ticker"])

            note_stock = json.loads(
                (stocks_dir / "76954AAD5__NOTE.json").read_text()
            )
            equity_stock = json.loads(
                (stocks_dir / "76954AAD5.json").read_text()
            )
            self.assertEqual(
                "76954AAD5|NOTE",
                note_stock["stock_id"],
            )
            self.assertEqual(
                "76954AAD5",
                note_stock["ticker"],
            )
            self.assertEqual("76954AAD5", equity_stock["ticker"])

            ticker_rows = json.loads(
                (root / "index.json").read_text()
            )["tickers"]
            self.assertEqual([], ticker_rows)


if __name__ == "__main__":
    unittest.main()
