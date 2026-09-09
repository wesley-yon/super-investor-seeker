"""Issuer spelling may validate exact evidence; it must never invent that evidence."""

from __future__ import annotations

import copy
import unittest

import sec_security_master as master
from test_sec_security_master import (
    ftd_record,
    numbered_cusip,
    official_record,
    source_state,
)


class TickerNameResolutionTests(unittest.TestCase):
    # These names reproduce ordinary SEC issuer/FTD spelling differences. The
    # symbol is supplied by two dated, exact-CUSIP FTD observations, not a name
    # lookup or a production override table.
    CASES = (
        ("615369105", "MCO", "MOODYS CORP", "MOODY'S CORP /DE/", "Moody's Corp", "COM"),
        ("92343E102", "VRSN", "VERISIGN INC", "VERISIGN INC/CA", "VeriSign Inc", "COM"),
        ("526057104", "LEN", "LENNAR CORP", "LENNAR CORP /NEW/ CL A", "Lennar Corp /NEW/", "CL A"),
        ("62944T105", "NVR", "NVR INC", "NVR INC (NEW)", "NVR INC", "COM"),
        ("829933100", "SIRI", "SIRIUS XM HOLDINGS INC", "SIRIUS XM HOLDINGS INC, NEW", "Sirius XM Holdings Inc.", "COM"),
        ("23331A109", "DHI", "HORTON D R INC", "D R HORTON INC", "HORTON D R INC /DE/", "COM"),
        ("09075V102", "BNTX", "BIONTECH SE", "BIONTECH SE ADS (DEU)", "BioNTech SE", "SPONSORED ADS"),
    )

    def build_case(self, case, *, rows=None, title=None, instrument_type="EQUITY", reported_class=None):
        cusip, symbol, issuer, ftd_description, sec_title, security_class = case
        if rows is None:
            rows = [
                ftd_record(day, symbol=symbol, cusip=cusip, description=ftd_description)
                for day in ("2026-08-01", "2026-08-04")
            ]
        state = source_state(
            rows=rows,
            symbols=[symbol],
            symbol_titles={symbol: [title or sec_title]},
            official_rows=[official_record(cusip=cusip, issuer=issuer, description=security_class)],
        )
        built = master.rebuild_security_master(state, [{
            "cusip": cusip,
            "instrument_type": instrument_type,
            "reported_issuer": issuer,
            "reported_class": reported_class or security_class,
        }])
        master.validate_security_master(built)
        return built, built["records"][f"{cusip}|{instrument_type}"]

    def test_sec_spelling_variants_resolve_with_exact_cusip_evidence(self):
        for case in self.CASES:
            with self.subTest(symbol=case[1]):
                built, record = self.build_case(case)
                self.assertEqual("resolved", record["mapping_status"], record["resolution_reason"])
                self.assertEqual(case[1], record["ticker"])
                self.assertEqual("sec_ftd", record["ticker_source"])
                self.assertEqual(
                    "exact_ftd_symbol_with_sec_metadata_validation",
                    record["mapping_method"],
                )
                self.assertEqual(["2026-08-01", "2026-08-04"], record["confirmation_dates"])
                self.assertTrue(record["symbol_evidence"])
                master.validate_security_master(built)

    def test_validator_replays_normalized_names_and_rejects_unrelated_title(self):
        for case in self.CASES:
            with self.subTest(symbol=case[1]):
                built, record = self.build_case(case)
                self.assertEqual(case[1], record["ticker"], record["resolution_reason"])
                tampered = copy.deepcopy(built)
                tampered["records"][f"{case[0]}|EQUITY"]["symbol_validation_titles"] = [
                    "UNRELATED INDUSTRIAL SUPPLY INC"
                ]
                with self.assertRaises(master.SecurityMasterError):
                    master.validate_security_master(tampered)

    def test_unrelated_issuer_does_not_become_compatible_with_cleanup(self):
        for case in self.CASES:
            with self.subTest(symbol=case[1]):
                _, record = self.build_case(case, title="UNRELATED INDUSTRIAL SUPPLY INC /DE/")
                self.assertIsNone(record["ticker"])
                self.assertEqual("ambiguous", record["mapping_status"])

    def test_matching_name_without_ftd_evidence_stays_unresolved(self):
        for case in self.CASES:
            with self.subTest(symbol=case[1]):
                _, record = self.build_case(case, rows=[])
                self.assertIsNone(record["ticker"])
                self.assertEqual("unresolved", record["mapping_status"])

    def test_initial_name_reordering_preserves_initial_sequence_and_issuer(self):
        case = self.CASES[5]
        for description in ("R D HORTON INC", "D S HORTON INC", "D R HORTON BUILDERS INC"):
            with self.subTest(description=description):
                _, record = self.build_case(case, rows=[
                    ftd_record(day, symbol="DHI", cusip=case[0], description=description)
                    for day in ("2026-08-01", "2026-08-04")
                ])
                self.assertIsNone(record["ticker"])
                self.assertEqual("issuer_conflict_with_ftd_description", record["resolution_reason"])

    def test_class_b_cannot_inherit_class_a_symbol_from_same_issuer(self):
        class_a = self.CASES[2]
        class_b_cusip = "526057302"
        state = source_state(
            rows=[
                ftd_record(day, symbol="LEN", cusip=class_a[0], description="LENNAR CORP /NEW/ CL A")
                for day in ("2026-08-01", "2026-08-04")
            ],
            symbols=["LEN", "LEN-B"],
            symbol_titles={"LEN": ["Lennar Corp /NEW/"], "LEN-B": ["Lennar Corp /NEW/"]},
            official_rows=[
                official_record(cusip=class_a[0], issuer="LENNAR CORP", description="CL A"),
                official_record(cusip=class_b_cusip, issuer="LENNAR CORP", description="CL B"),
            ],
        )
        built = master.rebuild_security_master(state, [{
            "cusip": class_b_cusip, "instrument_type": "EQUITY",
            "reported_issuer": "LENNAR CORP", "reported_class": "CL B",
        }])
        self.assertEqual("LEN", built["records"][f"{class_a[0]}|EQUITY"]["ticker"])
        self.assertIsNone(built["records"][f"{class_b_cusip}|EQUITY"]["ticker"])
        master.validate_security_master(built)

    def test_reported_class_b_conflicts_with_exact_class_a_cusip(self):
        _, record = self.build_case(self.CASES[2], reported_class="CL B")
        self.assertIsNone(record["ticker"])
        self.assertEqual(
            "official_13f_class_designator_conflicts_with_reported_class",
            record["resolution_reason"],
        )

    def test_explicit_ftd_class_cannot_override_exact_official_class(self):
        # The CUSIP is present in both dated FTD observations. A matching
        # issuer and current ticker therefore cannot excuse an explicit
        # contradiction between the FTD class and the official CUSIP class.
        for cusip, symbol, official_class, ftd_class in (
            ("526057104", "LEN-B", "CL A", "CL-B"),
            ("526057302", "LEN", "CL B", "CL-A"),
        ):
            with self.subTest(official=official_class, ftd=ftd_class):
                _, record = self.build_case((
                    cusip, symbol, "LENNAR CORP",
                    f"LENNAR CORP {ftd_class} COMMON",
                    "LENNAR CORP /NEW/", official_class,
                ))
                self.assertIsNone(record["ticker"])
                self.assertNotEqual("resolved", record["mapping_status"])

    def test_validator_rejects_explicit_ftd_class_contradiction(self):
        for cusip, symbol, official_class, ftd_class in (
            ("526057104", "LEN", "CL A", "CL-B"),
            ("526057302", "LEN-B", "CL B", "CL-A"),
        ):
            with self.subTest(official=official_class, ftd=ftd_class):
                built, record = self.build_case((
                    cusip, symbol, "LENNAR CORP",
                    f"LENNAR CORP {official_class} COMMON",
                    "LENNAR CORP /NEW/", official_class,
                ))
                self.assertEqual(symbol, record["ticker"])
                altered = copy.deepcopy(built)
                evidence = altered["records"][f"{cusip}|EQUITY"]
                description = f"LENNAR CORP {ftd_class} COMMON"
                for observation in evidence["symbol_evidence"]:
                    observation["descriptions"] = [description]
                for interval in evidence["symbol_intervals"]:
                    interval["descriptions"] = [description]
                    interval["symbol_descriptions"] = {symbol: [description]}
                with self.assertRaises(master.SecurityMasterError):
                    master.validate_security_master(altered)

    def test_ordinary_share_cusip_cannot_inherit_adr_symbol_from_name(self):
        adr_cusip = "09075V102"
        # Deliberately synthetic ordinary-share identity: no claim that BioNTech
        # has this actual ordinary-share CUSIP or an independently listed symbol.
        ordinary_cusip = numbered_cusip(90123)
        state = source_state(
            rows=[
                ftd_record(day, symbol="BNTX", cusip=adr_cusip, description="BIONTECH SE ADS (DEU)")
                for day in ("2026-08-01", "2026-08-04")
            ],
            symbols=["BNTX"],
            symbol_titles={"BNTX": ["BioNTech SE"]},
            official_rows=[
                official_record(cusip=ordinary_cusip, issuer="BIONTECH SE", description="ORD SHS"),
                official_record(cusip=adr_cusip, issuer="BIONTECH SE", description="SPONSORED ADS"),
            ],
        )
        built = master.rebuild_security_master(state, [{
            "cusip": ordinary_cusip, "instrument_type": "EQUITY",
            "reported_issuer": "BIONTECH SE", "reported_class": "ORD SHS",
        }])
        self.assertEqual("BNTX", built["records"][f"{adr_cusip}|EQUITY"]["ticker"])
        self.assertIsNone(built["records"][f"{ordinary_cusip}|EQUITY"]["ticker"])
        master.validate_security_master(built)

    def fund_state(self, *, evidenced_cusip="464287655", symbol="IWM", description="ISHARES RUSSELL 2000 ETF"):
        state = source_state(
            rows=[
                ftd_record(day, symbol=symbol, cusip=evidenced_cusip, description=description)
                for day in ("2026-08-01", "2026-08-04")
            ],
            official_rows=[
                official_record(cusip="464287200", issuer="ISHARES TR", description="CORE S&P 500 ETF"),
                official_record(cusip="464287655", issuer="ISHARES TR", description="RUSSELL 2000 ETF"),
            ],
        )
        state["sources"][master.SEC_FUND_TICKERS_URL] = {
            "url": master.SEC_FUND_TICKERS_URL,
            "kind": "sec_fund_tickers",
            "sha256": "d" * 64,
            "accepted_at": "2026-08-20T12:00:00Z",
            "symbols": ["IVV", "IWM"],
            "symbol_titles": {},
            "symbol_exchanges": {},
            "symbol_count": 2,
            "fund_records": [
                {"symbol": "IVV", "cik": "0001100663", "series_id": "S000004310", "class_id": "C000012054"},
                {"symbol": "IWM", "cik": "0001100663", "series_id": "S000004319", "class_id": "C000012063"},
            ],
        }
        return state

    def test_same_etf_trust_does_not_supply_another_products_missing_evidence(self):
        built = master.rebuild_security_master(self.fund_state(), [{
            "cusip": "464287200", "instrument_type": "EQUITY",
            "reported_issuer": "ISHARES TR", "reported_class": "CORE S&P 500 ETF",
        }])
        self.assertEqual("IWM", built["records"]["464287655|EQUITY"]["ticker"])
        self.assertIsNone(built["records"]["464287200|EQUITY"]["ticker"])
        master.validate_security_master(built)

    def test_exact_etf_cusip_wins_over_another_products_reported_name(self):
        state = self.fund_state(
            evidenced_cusip="464287200", symbol="IVV", description="ISHARES CORE S&P 500 ETF",
        )
        built = master.rebuild_security_master(state, [{
            "cusip": "464287200", "instrument_type": "EQUITY",
            "reported_issuer": "ISHARES TR", "reported_class": "RUSSELL 2000 ETF",
        }])
        record = built["records"]["464287200|EQUITY"]
        self.assertEqual("IVV", record["ticker"], record["resolution_reason"])
        self.assertEqual("CORE S&P 500 ETF", record["security_class"])
        self.assertIsNone(built["records"]["464287655|EQUITY"]["ticker"])
        master.validate_security_master(built)

    def build_fund_brand(self, issuer, ftd_description, *, symbol="BRND"):
        # The source identifiers are synthetic, checksum-valid fixtures. They
        # bind two observations to one product without asserting a live filing.
        cusip = numbered_cusip(90234)
        state = source_state(
            rows=[
                ftd_record(day, symbol=symbol, cusip=cusip, description=ftd_description)
                for day in ("2026-08-01", "2026-08-04")
            ],
            official_rows=[official_record(cusip=cusip, issuer=issuer, description="ETF SHS")],
        )
        state["sources"][master.SEC_FUND_TICKERS_URL] = {
            "url": master.SEC_FUND_TICKERS_URL,
            "kind": "sec_fund_tickers",
            "sha256": "d" * 64,
            "accepted_at": "2026-08-20T12:00:00Z",
            "symbols": [symbol],
            "symbol_titles": {},
            "symbol_exchanges": {},
            "symbol_count": 1,
            "fund_records": [{
                "symbol": symbol, "cik": "0000000123",
                "series_id": "S000009234", "class_id": "C000009234",
            }],
        }
        built = master.rebuild_security_master(state, [{
            "cusip": cusip, "instrument_type": "EQUITY",
            "reported_issuer": issuer, "reported_class": "ETF SHS",
        }])
        master.validate_security_master(built)
        return built, built["records"][f"{cusip}|EQUITY"]

    def test_fund_trust_brand_variants_validate_exact_product_evidence(self):
        cases = (
            ("ARK ETF TR", "ARK INNOVATION ETF", "ARKK"),
            ("GLOBAL X FDS", "GLOBAL X ARGENTINA ETF", "ARGT"),
            ("DIREXION SHARES ETF TRUST", "DIREXION SHS DAILY SEMICONDUCTOR BULL 3X", "SOXL"),
        )
        for issuer, description, symbol in cases:
            with self.subTest(issuer=issuer):
                built, record = self.build_fund_brand(issuer, description, symbol=symbol)
                self.assertEqual(symbol, record["ticker"], record["resolution_reason"])
                self.assertEqual("sec_ftd", record["ticker_source"])
                self.assertEqual(["2026-08-01", "2026-08-04"], record["confirmation_dates"])
                self.assertEqual(symbol, record["symbol_validation_fund_identity"]["symbol"])
                master.validate_security_master(built)

    def test_generic_fund_word_variants_preserve_the_distinctive_brand(self):
        # Each generic token can appear in the official trust label without
        # appearing in the FTD product label; the distinctive brand must remain.
        for generic in (
            "ETF", "ETFS", "FUND", "FUNDS", "FD", "FDS", "EXCHANGE",
            "EXCHNG", "EXCH", "EX", "EXC", "TRADED", "TRAD", "TRD",
            "SHARES", "SHS", "INC",
        ):
            with self.subTest(generic=generic):
                _, record = self.build_fund_brand(
                    f"ALPINE {generic} TR", "ALPINE US QUALITY EQUITY ETF",
                )
                self.assertEqual("BRND", record["ticker"], record["resolution_reason"])

    def test_fund_trust_roman_numerals_remain_part_of_identity(self):
        for official_roman, ftd_roman in (("VI", "VIII"), ("VIII", "VI")):
            with self.subTest(official_roman=official_roman):
                issuer = f"FIRST TRUST EXCHANGE TRADED FUND {official_roman}"
                _, control = self.build_fund_brand(
                    issuer, f"FIRST TRUST {official_roman} US EQUITY ETF",
                )
                self.assertEqual("BRND", control["ticker"], control["resolution_reason"])
                _, record = self.build_fund_brand(
                    issuer, f"FIRST TRUST {ftd_roman} US EQUITY ETF",
                )
                self.assertIsNone(record["ticker"])
                self.assertEqual("issuer_conflict_with_ftd_description", record["resolution_reason"])

    def test_generic_fund_words_do_not_erase_unrelated_brands(self):
        for issuer, description in (
            ("ARK ETF TR", "ARROW INNOVATION ETF"),
            ("GLOBAL X FDS", "GLOBAL ALPHA ARGENTINA ETF"),
            ("DIREXION SHARES ETF TRUST", "PROSHARES DAILY SEMICONDUCTOR ETF"),
        ):
            with self.subTest(issuer=issuer):
                _, record = self.build_fund_brand(issuer, description)
                self.assertIsNone(record["ticker"])
                self.assertEqual("issuer_conflict_with_ftd_description", record["resolution_reason"])

    def test_generic_only_fund_trust_without_brand_is_rejected(self):
        for issuer in ("EXCHANGE TRADED FUND SERIES TRUST", "ETF SHARES TR", "FDS INC"):
            with self.subTest(issuer=issuer):
                _, record = self.build_fund_brand(issuer, "ALPINE US QUALITY ETF")
                self.assertIsNone(record["ticker"])
                self.assertEqual("fund_lacks_official_issuer_identity", record["resolution_reason"])

    def test_fund_generic_cleanup_requires_current_fund_identity(self):
        _, record = self.build_case((
            numbered_cusip(90234), "BRND", "GLOBAL X FDS",
            "GLOBAL X ARGENTINA ETF", "GLOBAL X FDS", "COM",
        ))
        self.assertIsNone(record["ticker"])
        self.assertEqual("issuer_conflict_with_ftd_description", record["resolution_reason"])

    def test_debt_does_not_inherit_cleaned_issuer_stock_symbol(self):
        for instrument_type in ("NOTE", "EQUITY"):
            with self.subTest(instrument_type=instrument_type):
                _, record = self.build_case(
                    self.CASES[0], instrument_type=instrument_type,
                    reported_class="SENIOR NOTES 3.625 2030",
                )
                self.assertIsNone(record["ticker"])
                self.assertEqual("no_listed_symbol", record["mapping_status"])


if __name__ == "__main__":
    unittest.main()
